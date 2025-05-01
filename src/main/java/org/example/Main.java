package org.example;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
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

public class Main {
  public static final String NAMESPACE = "ns";
  public static final String TABLE = "test";

  public static void main(String[] args) throws IOException {
    Map<String, String> properties = new HashMap<>();
    // Common properties
    properties.put(CatalogProperties.CATALOG_IMPL, JdbcCatalog.class.getName());
    properties.put(CatalogProperties.URI, "jdbc:postgresql://localhost:5432/iceberg_metastore");
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, "s3a://iceberg-warehouse/warehouse");
    // JDBC-specific properties
    properties.put(JdbcCatalog.PROPERTY_PREFIX + "user", "postgres");
    properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", "postgres");
    properties.put(JdbcCatalog.PROPERTY_PREFIX + "schema-version", "V2");

    Configuration hadoopConf = new Configuration();
    hadoopConf.set("fs.s3a.endpoint", "http://localhost:9000");
    hadoopConf.set("fs.s3a.access.key", "minioadmin");
    hadoopConf.set("fs.s3a.secret.key", "minioadmin");
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
}
