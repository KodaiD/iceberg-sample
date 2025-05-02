package org.example;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
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

    // Insert a record
    GenericRecord record = GenericRecord.create(schema);
    record.setField("c1", 0);
    record.setField("c2", 2L);
    record.setField("c3", "aaa");
    String filePath = table.location() + "/data" + UUID.randomUUID() + ".parquet";
    OutputFile outputFile = table.io().newOutputFile(filePath);
    GenericRecord partitionRecord = GenericRecord.create(table.spec().partitionType());
    partitionRecord.setField("c1", 0);
    DataWriter<GenericRecord> dataWriter =
        Parquet.writeData(outputFile)
            .schema(table.schema())
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .overwrite(false)
            .withSpec(table.spec())
            .withPartition(partitionRecord)
            .build();
    dataWriter.write(record);
    dataWriter.close();
    DataFile dataFile = dataWriter.toDataFile();
    table.newAppend().appendFile(dataFile).commit();

    // Insert records
    Transaction transaction = table.newTransaction();
    AppendFiles append = transaction.newAppend();
    GenericRecord record1 = GenericRecord.create(schema);
    record1.setField("c1", 1);
    record1.setField("c2", 2L);
    record1.setField("c3", "aaa");
    filePath = table.location() + "/data" + UUID.randomUUID() + ".parquet";
    outputFile = table.io().newOutputFile(filePath);
    partitionRecord = GenericRecord.create(table.spec().partitionType());
    partitionRecord.setField("c1", 1);
    dataWriter =
        Parquet.writeData(outputFile)
            .schema(table.schema())
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .overwrite(false)
            .withSpec(table.spec())
            .withPartition(partitionRecord)
            .build();
    dataWriter.write(record1);
    dataWriter.close();
    dataFile = dataWriter.toDataFile();
    append.appendFile(dataFile);

    GenericRecord record2 = GenericRecord.create(schema);
    record2.setField("c1", 2);
    record2.setField("c2", 2L);
    record2.setField("c3", "bbb");
    filePath = table.location() + "/data" + UUID.randomUUID() + ".parquet";
    outputFile = table.io().newOutputFile(filePath);
    partitionRecord = GenericRecord.create(table.spec().partitionType());
    partitionRecord.setField("c1", 2);
    dataWriter =
        Parquet.writeData(outputFile)
            .schema(table.schema())
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .overwrite(false)
            .withSpec(table.spec())
            .withPartition(partitionRecord)
            .build();
    dataWriter.write(record2);
    dataWriter.close();
    dataFile = dataWriter.toDataFile();
    append.appendFile(dataFile);

    GenericRecord record3 = GenericRecord.create(schema);
    record3.setField("c1", 3);
    record3.setField("c2", 2L);
    record3.setField("c3", "ccc");
    filePath = table.location() + "/data" + UUID.randomUUID() + ".parquet";
    outputFile = table.io().newOutputFile(filePath);
    partitionRecord = GenericRecord.create(table.spec().partitionType());
    partitionRecord.setField("c1", 3);
    dataWriter =
        Parquet.writeData(outputFile)
            .schema(table.schema())
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .overwrite(false)
            .withSpec(table.spec())
            .withPartition(partitionRecord)
            .build();
    dataWriter.write(record3);
    dataWriter.close();
    dataFile = dataWriter.toDataFile();
    append.appendFile(dataFile);

    append.commit();
    transaction.commitTransaction();

    // Read records
    CloseableIterable<Record> result =
        IcebergGenerics.read(table).where(Expressions.equal("c1", 1)).build();
    for (Record r : result) {
      System.out.println(r);
    }

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

    //    Connection connection = DriverManager.getConnection(JDBC_URI, JDBC_USER, JDBC_PASSWORD);
    //    connection.createStatement().execute("CREATE DATABASE iceberg_metastore");
  }
}
