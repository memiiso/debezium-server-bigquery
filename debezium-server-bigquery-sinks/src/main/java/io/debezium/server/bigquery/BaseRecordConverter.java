/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.PrimaryKey;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableConstraints;
import io.debezium.DebeziumException;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
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

  protected static final String REGEX_TEMPORAL_VALUE_ENDS_WITH_Z = ".*\\d+Z$";

  protected final String destination;
  protected final JsonNode value;
  protected final JsonNode key;
  protected final JsonNode valueSchema;
  protected final JsonNode keySchema;
  protected final DebeziumConfig debeziumConfig;

  public BaseRecordConverter(String destination, JsonNode value, JsonNode key, JsonNode valueSchema, JsonNode keySchema, DebeziumConfig debeziumConfig) {
    this.destination = destination;
    this.value = value;
    this.key = key;
    this.valueSchema = valueSchema;
    this.keySchema = keySchema;
    this.debeziumConfig = debeziumConfig;
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
      String fieldTypeName = "NO-SEMANTIC-TYPE";
      if (jsonSchemaFieldNode.has("name")) {
        fieldTypeName = jsonSchemaFieldNode.get("name").textValue();
      }
      LOGGER.trace("Converting field: {}.{}::{}", schemaName, fieldName, fieldType);
      // for all the debezium data types please see org.apache.kafka.connect.data.Schema;
      switch (fieldType) {
        case "struct":
          switch (fieldTypeName) {
            case "io.debezium.data.geometry.Geometry":
              //  io.debezium.data.geometry.Geometry
              //  Contains a structure with two fields:
              //  srid (INT32: spatial reference system ID that defines the type of geometry object stored in the structure
              //  wkb (BYTES): binary representation of the geometry object encoded in the Well-Known-Binary (wkb) format. See the Open Geospatial Consortium for more details.
              List<Field> geometryFields = List.of(Field.of("srid", StandardSQLTypeName.INT64), Field.of("wkb", StandardSQLTypeName.GEOGRAPHY));
              fields.add(Field.newBuilder(fieldName, StandardSQLTypeName.STRUCT, FieldList.of(geometryFields)).build());
              break;
            default:
              fields.add(getStructField(jsonSchemaFieldNode, fieldName));
              break;
          }
          break;
        default:
          // default to String type
          fields.add(schemaPrimitiveField(jsonSchemaFieldNode, fieldType, fieldName, fieldTypeName));
          break;
      }
    }

    return fields;
  }

  protected Field getStructField(JsonNode jsonSchemaFieldNode, String fieldName) {
    if (debeziumConfig.common().nestedAsJson()) {
      return Field.of(fieldName, StandardSQLTypeName.JSON);
    }
    // recursive call for nested fields
    ArrayList<Field> subFields = schemaFields(jsonSchemaFieldNode);
    return Field.newBuilder(fieldName, StandardSQLTypeName.STRUCT, FieldList.of(subFields)).build();
  }

  public static String removeTemporalValueTrailingZ(String input) {
    if (input != null && !input.isEmpty() && input.strip().matches(REGEX_TEMPORAL_VALUE_ENDS_WITH_Z)) {
      return input.strip().substring(0, input.length() - 1);
    }
    return input;
  }

  protected ArrayList<String> keyFields() {
    ArrayList<String> keyFields = new ArrayList<>();

    if (this.keySchema() != null && this.keySchema().has("fields")) {
      for (JsonNode jsonSchemaFieldNode : this.keySchema().get("fields")) {
        keyFields.add(jsonSchemaFieldNode.get("field").textValue());
      }
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
    if (this.keyFields().isEmpty() || this.destination().startsWith("__debezium")) {
      return null;
    }

    return
        TableConstraints.newBuilder()
            .setPrimaryKey(PrimaryKey.newBuilder().setColumns(this.keyFields()).build())
            .build();
  }

  @Override
  public Clustering tableClustering(String clusteringField) {
    // special destinations like "heartbeat.topics"
    if (this.destination().startsWith("__debezium")) {
      return null;
    }

    ArrayList<String> keyFields = this.keyFields();
    if (keyFields.isEmpty()) {
      if (value().has(clusteringField)) {
      return Clustering.newBuilder().setFields(List.of(clusteringField)).build();
      } else {
        return null;
      }
    } else {
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

  protected static void handleFieldValue(ObjectNode parentNode, Field field, JsonNode value) {

    if (value.isNull()) {
      return;
    }

    final String fieldName = field.getName();

    switch (field.getType().getStandardType()) {
      case BYTES:
        if (value.isTextual()) {
          try {
            parentNode.replace(fieldName, BinaryNode.valueOf(value.binaryValue()));
          } catch (IOException e) {
            throw new DebeziumException("Failed to process BYTES field: " + fieldName + " value: " + value.textValue(), e);
          }
        }
        break;
      case GEOGRAPHY:
        if (value.isBinary()) {
          try {
            String hexString = Hex.encodeHexString(value.binaryValue());
            parentNode.replace(fieldName, TextNode.valueOf(hexString));
          } catch (IOException e) {
            throw new DebeziumException("Failed to process GEOGRAPHY field: " + fieldName + " value: " + value.textValue(), e);
          }
          break;
        }
        if (value.isTextual()) {
          String hexString = Hex.encodeHexString(Base64.getDecoder().decode(value.textValue()));
          parentNode.replace(fieldName, TextNode.valueOf(hexString));
          break;
        }
        break;
      case STRUCT:
        for (Field f : field.getSubFields()) {
          if (!value.has(f.getName())) {
            continue;
          }
          if (value.get(f.getName()) == null || value.get(f.getName()).isNull()) {
            continue;
          }
          handleFieldValue((ObjectNode) value, f, value.get(f.getName()));
        }
        break;
      case JSON:
        if (value.isTextual()) {
          try {
            parentNode.replace(fieldName, mapper.readTree(value.textValue()));
          } catch (JsonProcessingException e) {
            throw new DebeziumException("Failed to process JSON field: " + fieldName + " value: " + value.textValue(), e);
          }
          break;
        }
        if (value.isObject() || value.isArray()) {
          break;
        }
        throw new DebeziumException("Unexpected JSON value: " + fieldName + " value-type: " + value.getNodeType() + "value: " + value.textValue());
      case DATE:
      case DATETIME:
      case TIME:
        if (value.isTextual()) {
          parentNode.replace(fieldName, TextNode.valueOf(removeTemporalValueTrailingZ(value.textValue())));
        }
        break;
      case TIMESTAMP:
        if (value.isNumber()) {
          if (TS_MS_FIELDS.contains(fieldName)) {
            // Process DEBEZIUM TS_MS values
            parentNode.replace(fieldName, TextNode.valueOf(Instant.ofEpochMilli(value.longValue()).toString()));
            break;
          }
          break;
        }
        break;
      default:
        // Handle other cases or do nothing
        break;
    }
  }

  protected Field schemaPrimitiveField(JsonNode jsonSchemaFieldNode, String fieldType, String fieldName, String fieldTypeName) {
    switch (fieldType) {
      case "int8":
      case "int16":
      case "int32":
      case "int64":
        if (TS_MS_FIELDS.contains(fieldName)) {
          return Field.of(fieldName, StandardSQLTypeName.TIMESTAMP);
        }
        return switch (fieldTypeName) {
          case "io.debezium.time.Date" -> Field.of(fieldName, StandardSQLTypeName.DATE);
          case "io.debezium.time.Timestamp" -> Field.of(fieldName, StandardSQLTypeName.INT64);
          case "io.debezium.time.MicroTimestamp" -> Field.of(fieldName, StandardSQLTypeName.INT64);
          case "io.debezium.time.NanoTimestamp" -> Field.of(fieldName, StandardSQLTypeName.INT64);
          default -> Field.of(fieldName, StandardSQLTypeName.INT64);
        };
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
        return switch (fieldTypeName) {
          case "io.debezium.time.IsoDate" -> Field.of(fieldName, StandardSQLTypeName.DATE);
          case "io.debezium.time.IsoTimestamp" -> Field.of(fieldName, StandardSQLTypeName.DATETIME);
          case "io.debezium.time.IsoTime" -> Field.of(fieldName, StandardSQLTypeName.TIME);
          case "io.debezium.data.Json" -> Field.of(fieldName, StandardSQLTypeName.JSON);
          case "io.debezium.time.ZonedTimestamp" -> Field.of(fieldName, StandardSQLTypeName.TIMESTAMP);
          case "io.debezium.time.ZonedTime" -> Field.of(fieldName, StandardSQLTypeName.TIME);
          default -> Field.of(fieldName, StandardSQLTypeName.STRING);
        };
      case "bytes":
        return Field.of(fieldName, StandardSQLTypeName.BYTES);
      case "array":
        JsonNode itemsNode = jsonSchemaFieldNode.get("items");
        if (itemsNode == null) {
          return Field.of(fieldName, StandardSQLTypeName.JSON);
        }

        String itemType = itemsNode.get("type").textValue();
        String itemTypeName = itemsNode.has("name") ? itemsNode.get("name").textValue() : "NO-SEMANTIC-TYPE";
        Field elementField = schemaPrimitiveField(jsonSchemaFieldNode, itemType, fieldName + "_element", itemTypeName);

        return Field.newBuilder(fieldName, elementField.getType()).setMode(Field.Mode.REPEATED).build();
      case "map":
        return Field.of(fieldName, StandardSQLTypeName.STRUCT);
      default:
        // default to String type
        return Field.of(fieldName, StandardSQLTypeName.STRING);
    }

  }

}
