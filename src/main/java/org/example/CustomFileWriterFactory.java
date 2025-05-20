package org.example;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.DELETE_DEFAULT_FILE_FORMAT;

import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.BaseFileWriterFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.parquet.Preconditions;

public class CustomFileWriterFactory extends BaseFileWriterFactory<Record> {

  public CustomFileWriterFactory(
      Table table,
      FileFormat dataFileFormat,
      Schema dataSchema,
      SortOrder dataSortOrder,
      FileFormat deleteFileFormat,
      int[] equalityFieldIds,
      Schema equalityDeleteRowSchema,
      SortOrder equalityDeleteSortOrder,
      Schema positionDeleteRowSchema) {
    super(
        table,
        dataFileFormat,
        dataSchema,
        dataSortOrder,
        deleteFileFormat,
        equalityFieldIds,
        equalityDeleteRowSchema,
        equalityDeleteSortOrder,
        positionDeleteRowSchema);
  }

  static Builder builderFor(Table table) {
    return new Builder(table);
  }

  @Override
  protected void configureDataWrite(Avro.DataWriteBuilder dataWriteBuilder) {
    dataWriteBuilder.createWriterFunc(DataWriter::create);
  }

  @Override
  protected void configureEqualityDelete(Avro.DeleteWriteBuilder deleteWriteBuilder) {
    deleteWriteBuilder.createWriterFunc(DataWriter::create);
  }

  @Override
  protected void configurePositionDelete(Avro.DeleteWriteBuilder deleteWriteBuilder) {
    deleteWriteBuilder.createWriterFunc(DataWriter::create);
  }

  @Override
  protected void configureDataWrite(Parquet.DataWriteBuilder dataWriteBuilder) {
    dataWriteBuilder.createWriterFunc(GenericParquetWriter::buildWriter);
  }

  @Override
  protected void configureEqualityDelete(Parquet.DeleteWriteBuilder deleteWriteBuilder) {
    deleteWriteBuilder.createWriterFunc(GenericParquetWriter::buildWriter);
  }

  @Override
  protected void configurePositionDelete(Parquet.DeleteWriteBuilder deleteWriteBuilder) {
    deleteWriteBuilder.createWriterFunc(GenericParquetWriter::buildWriter);
  }

  @Override
  protected void configureDataWrite(org.apache.iceberg.orc.ORC.DataWriteBuilder dataWriteBuilder) {
    dataWriteBuilder.createWriterFunc(GenericOrcWriter::buildWriter);
  }

  @Override
  protected void configureEqualityDelete(
      org.apache.iceberg.orc.ORC.DeleteWriteBuilder deleteWriteBuilder) {
    deleteWriteBuilder.createWriterFunc(GenericOrcWriter::buildWriter);
  }

  @Override
  protected void configurePositionDelete(
      org.apache.iceberg.orc.ORC.DeleteWriteBuilder deleteWriteBuilder) {
    deleteWriteBuilder.createWriterFunc(GenericOrcWriter::buildWriter);
  }

  static class Builder {
    private final Table table;
    private FileFormat dataFileFormat;
    private Schema dataSchema;
    private SortOrder dataSortOrder;
    private FileFormat deleteFileFormat;
    private int[] equalityFieldIds;
    private Schema equalityDeleteRowSchema;
    private SortOrder equalityDeleteSortOrder;
    private Schema positionDeleteRowSchema;

    Builder(Table table) {
      this.table = table;
      this.dataSchema = table.schema();
      this.dataSortOrder = table.sortOrder();

      Map<String, String> properties = table.properties();

      String dataFileFormatName =
          properties.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
      this.dataFileFormat = FileFormat.fromString(dataFileFormatName);

      String deleteFileFormatName =
          properties.getOrDefault(DELETE_DEFAULT_FILE_FORMAT, dataFileFormatName);
      this.deleteFileFormat = FileFormat.fromString(deleteFileFormatName);
    }

    Builder dataFileFormat(FileFormat newDataFileFormat) {
      this.dataFileFormat = newDataFileFormat;
      return this;
    }

    Builder dataSchema(Schema newDataSchema) {
      this.dataSchema = newDataSchema;
      return this;
    }

    Builder dataSortOrder(SortOrder newDataSortOrder) {
      this.dataSortOrder = newDataSortOrder;
      return this;
    }

    Builder deleteFileFormat(FileFormat newDeleteFileFormat) {
      this.deleteFileFormat = newDeleteFileFormat;
      return this;
    }

    Builder equalityFieldIds(int[] newEqualityFieldIds) {
      this.equalityFieldIds = newEqualityFieldIds;
      return this;
    }

    Builder equalityDeleteRowSchema(Schema newEqualityDeleteRowSchema) {
      this.equalityDeleteRowSchema = newEqualityDeleteRowSchema;
      return this;
    }

    Builder equalityDeleteSortOrder(SortOrder newEqualityDeleteSortOrder) {
      this.equalityDeleteSortOrder = newEqualityDeleteSortOrder;
      return this;
    }

    Builder positionDeleteRowSchema(Schema newPositionDeleteRowSchema) {
      this.positionDeleteRowSchema = newPositionDeleteRowSchema;
      return this;
    }

    CustomFileWriterFactory build() {
      boolean noEqualityDeleteConf = equalityFieldIds == null && equalityDeleteRowSchema == null;
      boolean fullEqualityDeleteConf = equalityFieldIds != null && equalityDeleteRowSchema != null;
      Preconditions.checkArgument(
          noEqualityDeleteConf || fullEqualityDeleteConf,
          "Equality field IDs and equality delete row schema must be set together");

      return new CustomFileWriterFactory(
          table,
          dataFileFormat,
          dataSchema,
          dataSortOrder,
          deleteFileFormat,
          equalityFieldIds,
          equalityDeleteRowSchema,
          equalityDeleteSortOrder,
          positionDeleteRowSchema);
    }
  }
}
