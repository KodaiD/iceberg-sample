package org.example;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFile;

public class IcebergWrapper {
  public static final String PARTITION_KEY = "partition_key";
  public static final String CLUSTERING_KEY = "clustering_key";
  public static final String VALUE = "value";
  private final Table table;

  public IcebergWrapper(Table table) {
    this.table = table;
  }

  private static <T> Record getPartitionRecord(Table table, String partitionKey, T partitionValue) {
    GenericRecord partitionRecord = GenericRecord.create(table.spec().partitionType());
    partitionRecord.setField(partitionKey, partitionValue);
    return partitionRecord;
  }

  public CloseableIterable<Record> read(String partitionKey, String clusteringKey) {
    return IcebergGenerics.read(table)
        .where(Expressions.equal(PARTITION_KEY, partitionKey))
        .where(Expressions.equal(CLUSTERING_KEY, clusteringKey))
        .build();
  }

  public void insert(String partitionKey, String clusteringKey, String value) throws IOException {
    Transaction transaction = table.newTransaction();
    CloseableIterable<Record> iterable = read(partitionKey, clusteringKey);
    if (iterable.iterator().hasNext()) {
      System.out.println("Record already exists");
      iterable.close();
      return;
    }
    GenericRecord record = GenericRecord.create(table.schema());
    record.setField(PARTITION_KEY, partitionKey);
    record.setField(CLUSTERING_KEY, clusteringKey);
    record.setField(VALUE, value);
    List<Record> records = Collections.singletonList(record);
    transaction.newAppend().appendFile(newDataFile(partitionKey, records)).commit();
    transaction.commitTransaction();
    iterable.close();
  }

  public void update(String partitionKey, String clusteringKey, String value) throws IOException {
    Transaction transaction = table.newTransaction();
    CloseableIterable<Record> iterable = read(partitionKey, clusteringKey);
    if (!iterable.iterator().hasNext()) {
      System.out.println("Record does not exist");
      iterable.close();
      return;
    }
    Record record = iterable.iterator().next();
    DeleteFile deleteFile = newEqualityDeleteFile(partitionKey, Collections.singletonList(record));
    record.setField(VALUE, value);
    DataFile dataFile = newDataFile(partitionKey, Collections.singletonList(record));
    transaction.newRowDelta().addDeletes(deleteFile).addRows(dataFile).commit();
    transaction.commitTransaction();
    iterable.close();
  }

  private DataFile newDataFile(String partitionKey, List<Record> records) throws IOException {
    DataWriter<Record> writer = newDataWriter(partitionKey);
    try (Closeable ignored = writer) {
      records.forEach(writer::write);
    }
    return writer.toDataFile();
  }

  private DeleteFile newPositionDeleteFile(String partitionKey, List<Record> records)
      throws IOException {
    PositionDeleteWriter<Record> writer = newPositionDeleteWriter(partitionKey);
    PositionDelete<Record> positionDelete = PositionDelete.create();
    for (Record record : records) {
      positionDelete.set(
          record.getField("_file").toString(), (Long) record.getField("_pos"), record);
    }
    try (Closeable ignored = writer) {
      writer.write(positionDelete);
    }
    return writer.toDeleteFile();
  }

  private DeleteFile newEqualityDeleteFile(String partitionKey, List<Record> records)
      throws IOException {
    int[] equalityFieldIds = {
      table.schema().findField(PARTITION_KEY).fieldId(),
      table.schema().findField(CLUSTERING_KEY).fieldId()
    };
    EqualityDeleteWriter<Record> writer = newEqualityDeleteWriter(partitionKey, equalityFieldIds);
    try (Closeable ignored = writer) {
      records.forEach(writer::write);
    }
    return writer.toDeleteFile();
  }

  private DataWriter<Record> newDataWriter(String partitionKey) {
    Record partitionRecord = GenericRecord.create(table.spec().partitionType());
    partitionRecord.setField(PARTITION_KEY, partitionKey);
    FileWriterFactory<Record> factoryForDataFile =
        CustomFileWriterFactory.builderFor(table).build();
    return factoryForDataFile.newDataWriter(
        EncryptedFiles.encryptedOutput(getOutputFile(partitionRecord), EncryptionKeyMetadata.EMPTY),
        table.spec(),
        partitionRecord);
  }

  private PositionDeleteWriter<Record> newPositionDeleteWriter(String partitionKey) {
    Record partitionRecord = GenericRecord.create(table.spec().partitionType());
    partitionRecord.setField(PARTITION_KEY, partitionKey);
    FileWriterFactory<Record> factory =
        CustomFileWriterFactory.builderFor(table).positionDeleteRowSchema(table.schema()).build();
    return factory.newPositionDeleteWriter(
        EncryptedFiles.encryptedOutput(getOutputFile(partitionRecord), EncryptionKeyMetadata.EMPTY),
        table.spec(),
        partitionRecord);
  }

  private EqualityDeleteWriter<Record> newEqualityDeleteWriter(
      String partitionKey, int[] equalityFieldIds) {
    Record partitionRecord = GenericRecord.create(table.spec().partitionType());
    partitionRecord.setField(PARTITION_KEY, partitionKey);
    FileWriterFactory<Record> factory =
        CustomFileWriterFactory.builderFor(table)
            .equalityDeleteRowSchema(table.schema())
            .equalityFieldIds(equalityFieldIds)
            .build();
    return factory.newEqualityDeleteWriter(
        EncryptedFiles.encryptedOutput(getOutputFile(partitionRecord), EncryptionKeyMetadata.EMPTY),
        table.spec(),
        partitionRecord);
  }

  private OutputFile getOutputFile(Record partitionRecord) {
    String filePath =
        table
            .locationProvider()
            .newDataLocation(table.spec(), partitionRecord, UUID.randomUUID() + ".parquet");
    return table.io().newOutputFile(filePath);
  }
}
