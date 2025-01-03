/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Abstract base class for Debezium event record conversion to BigQuery format.
 * <p>This class provides the foundation for converting Debezium event records into a format
 * suitable for writing to BigQuery tables. It handles common tasks like schema conversion,
 * table constraint generation, and clustering configuration. Concrete implementations of this
 * class can extend this functionality for specific use cases.
 *
 * @author Ismail Simsek
 */
public abstract class BaseRecordConverter implements RecordConverter {
  protected static final Logger LOGGER = LoggerFactory.getLogger(BaseRecordConverter.class);
  protected static final List<String> TS_MS_FIELDS = List.of("__ts_ms", "__source_ts_ms");
  protected static final List<String> BOOLEAN_FIELDS = List.of("__deleted");
  protected static final ObjectMapper mapper = new ObjectMapper();
  protected static final String CHANGE_TYPE_PSEUDO_COLUMN = "_CHANGE_TYPE";

  protected final String destination;
  protected final JsonNode value;
  protected final JsonNode key;
  protected final JsonNode valueSchema;
  protected final JsonNode keySchema;

  public BaseRecordConverter(String destination, JsonNode value, JsonNode key, JsonNode valueSchema, JsonNode keySchema) {
    this.destination = destination;
    // @TODO process values. ts_ms values etc...
    // TODO add field if exists backward compatible!
    this.value = value;
    this.key = key;
    this.valueSchema = valueSchema;
    this.keySchema = keySchema;
  }

  protected ArrayList<Field> schemaFields(JsonNode schemaNode) {

    ArrayList<Field> fields = new ArrayList<>();

    if (schemaNode == null) {
      return fields;
    }

    String schemaType = schemaNode.get("type").textValue();
    String schemaName = "root";
    if (schemaNode.has("field")) {
      schemaName = schemaNode.get("field").textValue();
    }
    LOGGER.trace("Converting Schema of: {}::{}", schemaName, schemaType);

    for (JsonNode jsonSchemaFieldNode : schemaNode.get("fields")) {
      String fieldName = jsonSchemaFieldNode.get("field").textValue();
      String fieldType = jsonSchemaFieldNode.get("type").textValue();
      String fieldSemanticType = "NO-SEMANTIC-TYPE";
      if (jsonSchemaFieldNode.has("name")) {
        fieldSemanticType = jsonSchemaFieldNode.get("name").textValue();
      }
      LOGGER.trace("Converting field: {}.{}::{}", schemaName, fieldName, fieldType);
      // for all the debezium data types please see org.apache.kafka.connect.data.Schema;
      switch (fieldType) {
        case "struct":
          // recursive call for nested fields
          ArrayList<Field> subFields = schemaFields(jsonSchemaFieldNode);
          fields.add(Field.newBuilder(fieldName, StandardSQLTypeName.STRUCT, FieldList.of(subFields)).build());
          break;
        default:
          // default to String type
          fields.add(schemaPrimitiveField(fieldType, fieldName, fieldSemanticType));
          break;
      }
    }

    return fields;
  }

  protected Field schemaPrimitiveField(String fieldType, String fieldName, String fieldSemanticType) {
    switch (fieldType) {
      case "int8":
      case "int16":
      case "int32":
      case "int64":
        if (TS_MS_FIELDS.contains(fieldName)) {
          return Field.of(fieldName, StandardSQLTypeName.TIMESTAMP);
        }
        switch (fieldSemanticType) {
          case "io.debezium.time.Date":
            return Field.of(fieldName, StandardSQLTypeName.DATE);
          case "io.debezium.time.Timestamp":
            // NOTE automatic conversion not supported by batch load! it expects string datetime value!
            // Caused by: io.grpc.StatusRuntimeException: INVALID_ARGUMENT: 
            // Cannot return an invalid datetime value of 1562639337000 microseconds relative to the Unix epoch. 
            // The range of valid datetime values is [0001-01-01 00:00:00, 9999-12-31 23:59:59.999999] on field c_timestamp0. 
            return Field.of(fieldName, StandardSQLTypeName.INT64);
          case "io.debezium.time.MicroTimestamp":
            // NOTE automatic conversion not supported by batch load! it expects string datetime value!
            return Field.of(fieldName, StandardSQLTypeName.INT64);
          case "io.debezium.time.NanoTimestamp":
            // NOTE automatic conversion not supported by batch load! it expects string datetime value!
            return Field.of(fieldName, StandardSQLTypeName.INT64);
          default:
            return Field.of(fieldName, StandardSQLTypeName.INT64);
        }
      case "float8":
      case "float16":
      case "float32":
      case "float64":
        return Field.of(fieldName, StandardSQLTypeName.FLOAT64);
      case "double":
        return Field.of(fieldName, StandardSQLTypeName.FLOAT64);
      case "boolean":
        return Field.of(fieldName, StandardSQLTypeName.BOOL);
      case "string":
        if (BOOLEAN_FIELDS.contains(fieldName)) {
          return Field.of(fieldName, StandardSQLTypeName.BOOL);
        }
        switch (fieldSemanticType) {
          case "io.debezium.time.ISODate":
            return Field.of(fieldName, StandardSQLTypeName.DATE);
          case "io.debezium.time.ISODateTime":
            return Field.of(fieldName, StandardSQLTypeName.DATETIME);
          case "io.debezium.time.ISOTime":
            return Field.of(fieldName, StandardSQLTypeName.TIME);
          case "io.debezium.data.Json":
            return Field.of(fieldName, StandardSQLTypeName.JSON);
          case "io.debezium.time.ZonedTimestamp":
            return Field.of(fieldName, StandardSQLTypeName.TIMESTAMP);
          case "io.debezium.time.ZonedTime":
            // Invalid time string "12:05:11Z" Field: c_time; Value: 12:05:11Z
            return Field.of(fieldName, StandardSQLTypeName.STRING);
          default:
            return Field.of(fieldName, StandardSQLTypeName.STRING);
        }
      case "bytes":
        return Field.of(fieldName, StandardSQLTypeName.BYTES);
      case "array":
        return Field.of(fieldName, StandardSQLTypeName.ARRAY);
      case "map":
        return Field.of(fieldName, StandardSQLTypeName.STRUCT);
      default:
        // default to String type
        return Field.of(fieldName, StandardSQLTypeName.STRING);
    }

  }

  protected ArrayList<String> keyFields() {

    ArrayList<String> keyFields = new ArrayList<>();
    for (JsonNode jsonSchemaFieldNode : this.keySchema().get("fields")) {
      keyFields.add(jsonSchemaFieldNode.get("field").textValue());
    }

    return keyFields;
  }

  @Override
  public String destination() {
    return destination;
  }

  @Override
  public JsonNode value() {
    return value;
  }

  @Override
  public JsonNode key() {
    return key;
  }

  @Override
  public JsonNode valueSchema() {
    return valueSchema;
  }

  @Override
  public JsonNode keySchema() {
    return keySchema;
  }


  @Override
  public TableConstraints tableConstraints() {
    return
        TableConstraints.newBuilder()
            .setPrimaryKey(PrimaryKey.newBuilder().setColumns(this.keyFields()).build())
            .build();
  }

  @Override
  public Clustering tableClustering(String clusteringField) {
    // special destinations like "heartbeat.topics"
    if (this.destination().startsWith("__debezium")) {
      return Clustering.newBuilder().build();
    }

    if (this.keySchema() == null) {
      return Clustering.newBuilder().setFields(List.of(clusteringField)).build();
    } else {
      ArrayList<String> keyFields = this.keyFields();
      // NOTE Limit clustering fields to 4. it's the limit of Bigquery
      List<String> clusteringFields = keyFields.stream().limit(3).collect(Collectors.toList());
      clusteringFields.add(clusteringField);
      return Clustering.newBuilder().setFields(clusteringFields).build();
    }
  }

  @Override
  public Schema tableSchema() {
    ArrayList<Field> fields = schemaFields(this.valueSchema());

    if (fields.isEmpty()) {
      return null;
    }

    return Schema.of(fields);
  }

}
