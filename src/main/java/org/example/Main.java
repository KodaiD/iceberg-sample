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
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.jdbc.JdbcCatalog;
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
            Types.NestedField.required(1, IcebergWrapper.PARTITION_KEY, Types.StringType.get()),
            Types.NestedField.required(2, IcebergWrapper.CLUSTERING_KEY, Types.StringType.get()),
            Types.NestedField.optional(3, IcebergWrapper.VALUE, Types.StringType.get()));
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("write.delete.mode", "merge-on-read");
    tableProperties.put("write.update.mode", "merge-on-read");
    tableProperties.put("write.merge.mode", "merge-on-read");
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(schema).identity(IcebergWrapper.PARTITION_KEY).build();
    SortOrder sortOrder = SortOrder.builderFor(schema).asc(IcebergWrapper.CLUSTERING_KEY).build();
    Table table =
        createTableIfNotExists(
            catalog, tableIdentifier, schema, partitionSpec, sortOrder, tableProperties);

    IcebergWrapper wrapper = new IcebergWrapper(table);
    wrapper.insert("000", "111", "aaa");
    wrapper.insert("000", "222", "aaa");
    wrapper.insert("000", "333", "aaa");
    CloseableIterable<Record> iterable1 = wrapper.read("000", "111");
    if (iterable1.iterator().hasNext()) {
      System.out.println(iterable1.iterator().next().getField(IcebergWrapper.VALUE));
    }
    iterable1.close();
    wrapper.update("000", "111", "xxx");
    CloseableIterable<Record> iterable2 = wrapper.read("000", "111");
    if (iterable2.iterator().hasNext()) {
      System.out.println(iterable2.iterator().next().getField(IcebergWrapper.VALUE));
    }
    iterable2.close();

    // List all tables in the namespace
    List<TableIdentifier> tables = catalog.listTables(namespace);
    System.out.println(tables);

    // Drop table
    catalog.dropTable(tableIdentifier);
  }

  private static Table createTableIfNotExists(
      Catalog catalog,
      TableIdentifier tableId,
      Schema schema,
      PartitionSpec partitionSpec,
      SortOrder sortOrder,
      Map<String, String> tableProperties) {
    Table table;
    try {
      table =
          catalog
              .buildTable(tableId, schema)
              .withPartitionSpec(partitionSpec)
              .withSortOrder(sortOrder)
              .withProperties(tableProperties)
              .create();
    } catch (AlreadyExistsException e) {
      table = catalog.loadTable(tableId);
    }
    return table;
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
