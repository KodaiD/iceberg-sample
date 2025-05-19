package org.example;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class Main {
  public static final String MINIO_ENDPOINT = "http://localhost:9000";
  public static final String MINIO_USER = "minioadmin";
  public static final String MINIO_PASSWORD = "minioadmin";
  public static final String JDBC_DRIVER = "org.postgresql.Driver";
  public static final String JDBC_BASE_URI = "jdbc:postgresql://localhost:5432/";
  public static final String JDBC_USER = "postgres";
  public static final String JDBC_PASSWORD = "postgres";
  public static final String JDBC_DB_NAME = "iceberg_metastore";
  public static final String JDBC_CATALOG_URI = JDBC_BASE_URI + JDBC_DB_NAME;
  public static final String CATALOG_NAME = "test_jdbc_catalog";
  public static final String NAMESPACE_NAME = "ns";
  public static final String TABLE_NAME = "test";
  public static final String WAREHOUSE_PATH_S3FILEIO = "s3://iceberg-warehouse/warehouse";

  public static void main(String[] args) throws IOException {
    initS3();
    initDB();

    Map<String, String> properties = getProperties();
    Catalog catalog = CatalogUtil.buildIcebergCatalog(CATALOG_NAME, properties, null);

    Namespace namespace = Namespace.of(NAMESPACE_NAME);
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, TABLE_NAME);

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
            .withSortOrder(table.sortOrder())
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
            .withSortOrder(table.sortOrder())
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
            .withSortOrder(table.sortOrder())
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
            .withSortOrder(table.sortOrder())
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

  @NotNull
  private static Map<String, String> getProperties() {
    Map<String, String> properties = new HashMap<>();
    // Common properties
    properties.put(CatalogProperties.CATALOG_IMPL, JdbcCatalog.class.getName());
    properties.put(CatalogProperties.URI, JDBC_CATALOG_URI);
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, WAREHOUSE_PATH_S3FILEIO);
    // JDBC-specific properties
    properties.put(JdbcCatalog.PROPERTY_PREFIX + "user", JDBC_USER);
    properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", JDBC_PASSWORD);
    properties.put(JdbcCatalog.PROPERTY_PREFIX + "schema-version", "V2");
    // S3FileIO-specific properties
    Map<String, String> s3Properties = new HashMap<>();
    s3Properties.put("s3.endpoint", MINIO_ENDPOINT);
    s3Properties.put("s3.access-key-id", MINIO_USER);
    s3Properties.put("s3.secret-access-key", MINIO_PASSWORD);
    s3Properties.put("s3.path-style-access", "true");
    properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
    properties.putAll(s3Properties);
    return properties;
  }

  private static void initS3() {
    try (S3Client client =
        S3Client.builder()
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(MINIO_USER, MINIO_PASSWORD)))
            .region(Region.AP_NORTHEAST_1)
            .endpointOverride(URI.create(MINIO_ENDPOINT))
            .forcePathStyle(true)
            .build()) {
      client.createBucket(b -> b.bucket("iceberg-warehouse"));
      System.out.println("S3 bucket 'iceberg-warehouse' ensured (created or already exists).");
    } catch (S3Exception e) {
      String errorCode = e.awsErrorDetails() != null ? e.awsErrorDetails().errorCode() : "";
      if (e.statusCode() == 409
          && (errorCode.equals("BucketAlreadyOwnedByYou")
              || errorCode.equals("BucketAlreadyExists"))) {
        System.out.println("S3 bucket 'iceberg-warehouse' already exists.");
      } else {
        System.err.println(
            "Failed to create or verify S3 bucket 'iceberg-warehouse': " + e.getMessage());
        throw e;
      }
    }
  }

  private static void initDB() {
    try {
      Class.forName(JDBC_DRIVER);
    } catch (ClassNotFoundException e) {
      System.err.println(
          "PostgreSQL JDBC Driver not found. Please ensure it's in the classpath. Error: "
              + e.getMessage());
      throw new RuntimeException("PostgreSQL JDBC Driver not found", e);
    }

    String checkDbUrl = JDBC_BASE_URI + "postgres";
    try (Connection conn = DriverManager.getConnection(checkDbUrl, JDBC_USER, JDBC_PASSWORD)) {
      boolean dbExists = false;
      try (Statement stmt = conn.createStatement();
          ResultSet rs =
              stmt.executeQuery(
                  "SELECT 1 FROM pg_database WHERE datname = '"
                      + JDBC_DB_NAME.toLowerCase()
                      + "'")) {
        if (rs.next()) {
          dbExists = true;
        }
      }

      if (!dbExists) {
        try (Statement stmt = conn.createStatement()) {
          stmt.executeUpdate("CREATE DATABASE " + JDBC_DB_NAME);
          System.out.println("Database '" + JDBC_DB_NAME + "' created successfully.");
        }
      } else {
        System.out.println("Database '" + JDBC_DB_NAME + "' already exists.");
      }
    } catch (SQLException e) {
      if ("42P04".equals(e.getSQLState())) { // DUPLICATE_DATABASE
        System.out.println(
            "Database '" + JDBC_DB_NAME + "' already exists (confirmed by SQLState 42P04).");
      } else if (e.getMessage().toLowerCase().contains("connection refused")) {
        System.err.println(
            "PostgreSQL connection refused. Ensure server at "
                + JDBC_BASE_URI
                + " is running and accessible.");
        throw new RuntimeException("PostgreSQL connection failed", e);
      } else {
        System.err.println(
            "Error during PostgreSQL DB initialization: "
                + e.getMessage()
                + " (SQLState: "
                + e.getSQLState()
                + ")");
        throw new RuntimeException(
            "Failed to initialize PostgreSQL database '" + JDBC_DB_NAME + "'", e);
      }
    }
  }
}
