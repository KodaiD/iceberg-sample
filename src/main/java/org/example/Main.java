package org.example;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class Main {
  public static final String MINIO_ENDPOINT = "http://localhost:9000";
  public static final String MINIO_USER = "minioadmin";
  public static final String MINIO_PASSWORD = "minioadmin";

  public static final String JDBC_URI = "jdbc:postgresql://localhost:5432/";
  public static final String JDBC_USER = "postgres";
  public static final String JDBC_PASSWORD = "postgres";

  public static final String NAMESPACE = "ns";
  public static final String TABLE = "test";

  public static void main(String[] args) throws IOException, SQLException {
    init();

    Map<String, String> properties = new HashMap<>();
    // Common properties
    properties.put(CatalogProperties.CATALOG_IMPL, JdbcCatalog.class.getName());
    properties.put(CatalogProperties.URI, JDBC_URI + "iceberg_metastore");
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, "s3a://iceberg-warehouse/warehouse");
    // JDBC-specific properties
    properties.put(JdbcCatalog.PROPERTY_PREFIX + "user", JDBC_USER);
    properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", JDBC_PASSWORD);
    properties.put(JdbcCatalog.PROPERTY_PREFIX + "schema-version", "V2");

    Configuration hadoopConf = new Configuration();
    hadoopConf.set("fs.s3a.endpoint", MINIO_ENDPOINT);
    hadoopConf.set("fs.s3a.access.key", MINIO_USER);
    hadoopConf.set("fs.s3a.secret.key", MINIO_PASSWORD);
    hadoopConf.set("fs.s3a.path.style.access", "true");
    Catalog catalog = CatalogUtil.buildIcebergCatalog("test_jdbc_catalog", properties, hadoopConf);

    Namespace namespace = Namespace.of(NAMESPACE);
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, TABLE);

    catalog.dropTable(tableIdentifier);

    Schema schema =
        new Schema(
            Types.NestedField.required(1, "c1", Types.IntegerType.get()),
            Types.NestedField.required(2, "c2", Types.LongType.get()),
            Types.NestedField.optional(3, "c3", Types.StringType.get()));

    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("write.delete.mode", "merge-on-read");
    tableProperties.put("write.update.mode", "merge-on-read");
    tableProperties.put("write.merge.mode", "merge-on-read");

    PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).identity("c1").build();
    SortOrder sortOrder = SortOrder.builderFor(schema).asc("c2").build();

    // Create a table if not exist
    Table table;
    try {
      table =
          catalog
              .buildTable(tableIdentifier, schema)
              .withPartitionSpec(partitionSpec)
              .withSortOrder(sortOrder)
              .withProperties(tableProperties)
              .create();
    } catch (AlreadyExistsException e) {
      table = catalog.loadTable(tableIdentifier);
    }

    // Insert records
    GenericRecord record = GenericRecord.create(schema);
    ImmutableList.Builder<GenericRecord> builder = ImmutableList.builder();
    builder.add(record.copy(ImmutableMap.of("c1", 1, "c2", 2L, "c3", "aaa")));
    builder.add(record.copy(ImmutableMap.of("c1", 2, "c2", 3L, "c3", "bbb")));
    builder.add(record.copy(ImmutableMap.of("c1", 3, "c2", 4L, "c3", "ccc")));
    ImmutableList<GenericRecord> records = builder.build();
    String filePath = table.location() + "/data" + UUID.randomUUID() + ".parquet";

    OutputFile outputFile = table.io().newOutputFile(filePath);
    DataWriter<GenericRecord> dataWriter =
        Parquet.writeData(outputFile)
            .schema(table.schema())
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .overwrite(false)
            .withSpec(PartitionSpec.unpartitioned())
            .build();
    for (GenericRecord recordToWrite : records) {
      dataWriter.write(recordToWrite);
    }
    dataWriter.close();
    DataFile dataFile = dataWriter.toDataFile();
    table.newAppend().appendFile(dataFile).commit();

    // List all tables in the namespace
    List<TableIdentifier> tables = catalog.listTables(namespace);
    System.out.println(tables);

    // Drop table
    catalog.dropTable(tableIdentifier);
  }

  private static void init() throws SQLException {
    S3Client client =
        S3Client.builder()
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(MINIO_USER, MINIO_PASSWORD)))
            .region(Region.AP_NORTHEAST_1)
            .endpointOverride(URI.create(MINIO_ENDPOINT))
            .forcePathStyle(true)
            .build();
    try {
      client.createBucket(b -> b.bucket("iceberg-warehouse"));
    } catch (S3Exception e) {
      if (e.statusCode() != 409) {
        throw e;
      }
    }

    Connection connection = DriverManager.getConnection(JDBC_URI, JDBC_USER, JDBC_PASSWORD);
    connection.createStatement().execute("CREATE DATABASE iceberg_metastore");
  }
}
