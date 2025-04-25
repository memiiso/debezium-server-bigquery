package io.debezium.server.bigquery;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;

import java.util.List;
import java.util.stream.Collectors;

public class StorageWriteSchemaConverter {

  public static TableSchema toStorageTableSchema(Schema schema, boolean doUpsert) {
    TableSchema.Builder builder = TableSchema.newBuilder();
    for (Field field : schema.getFields()) {
      builder.addFields(convertField(field));
    }
    if (doUpsert) {
      builder.addFields(TableFieldSchema.newBuilder()
          .setName("_CHANGE_TYPE")
          .setType(TableFieldSchema.Type.STRING)
          .setMode(TableFieldSchema.Mode.NULLABLE)
          .build());
    }
    return builder.build();
  }

  private static TableFieldSchema convertField(Field field) {
    TableFieldSchema.Builder fieldBuilder = TableFieldSchema.newBuilder()
        .setName(field.getName())
        .setType(toStorageType(field.getType().getStandardType()))
        .setMode(toStorageMode(field.getMode()));

    // Handle nested RECORD/STRUCT fields recursively
    if (field.getType().getStandardType() == StandardSQLTypeName.STRUCT && field.getSubFields() != null) {
      List<TableFieldSchema> subFields = field.getSubFields().stream()
          .map(StorageWriteSchemaConverter::convertField)
          .collect(Collectors.toList());
      fieldBuilder.addAllFields(subFields); // Add subfields recursively
    }

    return fieldBuilder.build();
  }

  private static TableFieldSchema.Type toStorageType(StandardSQLTypeName type) {
    if (type == StandardSQLTypeName.FLOAT64) {
      return TableFieldSchema.Type.DOUBLE; // special case
    }
    try {
      return TableFieldSchema.Type.valueOf(type.name());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unsupported type: " + type, e);
    }
  }

  private static TableFieldSchema.Mode toStorageMode(Field.Mode mode) {
    if (mode == null) {
      return TableFieldSchema.Mode.NULLABLE; // default fallback
    }
    try {
      return TableFieldSchema.Mode.valueOf(mode.name());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unsupported mode: " + mode, e);
    }
  }
}
