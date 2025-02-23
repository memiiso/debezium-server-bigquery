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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import io.debezium.DebeziumException;

import java.time.Instant;

/**
 * @author Ismail Simsek
 */
public class BatchRecordConverter extends BaseRecordConverter {

  public BatchRecordConverter(String destination, JsonNode value, JsonNode key, JsonNode valueSchema, JsonNode keySchema, DebeziumConfig debeziumConfig) {
    super(destination, value, key, valueSchema, keySchema, debeziumConfig);
  }

  public static String removeTrailingZ(String input) {
    if (input != null && input.endsWith("Z")) {
      return input.substring(0, input.length() - 1);
    }
    return input;
  }

  public static void convertFieldValue(ObjectNode parentNode, String fieldName, StandardSQLTypeName fieldType, JsonNode value) throws JsonProcessingException {

    if (value.isNull()) {
      return;
    }

    // Process DEBEZIUM TS_MS values
    if (TS_MS_FIELDS.contains(fieldName)) {
      parentNode.replace(fieldName, TextNode.valueOf(Instant.ofEpochMilli(value.longValue()).toString()));
      return;
    }

    switch (fieldType) {
      case JSON:
        parentNode.replace(fieldName, mapper.readTree(value.textValue()));
        break;
      case DATE:
      case DATETIME:
      case TIME:
        if (value.isTextual()) {
          parentNode.replace(fieldName, TextNode.valueOf(removeTrailingZ(value.textValue())));
        }
        break;
      default:
        // Handle other cases or do nothing
        break;
    }

  }

  /**
   * Used by `bigquerybatch` {@link BatchBigqueryChangeConsumer} consumer.
   *
   * @param schema Bigquery table schema
   * @return returns Debezium event as a single line json string
   * @throws JsonProcessingException
   */
  @Override
  public String convert(Schema schema, boolean upsert, boolean upsertKeepDeletes) throws DebeziumException {

    if (value == null) {
      return null;
    }

    try {
      // process JSON fields
      if (schema != null) {
        for (Field f : schema.getFields()) {
          if (!value.has(f.getName())) {
            continue;
          }
          convertFieldValue((ObjectNode) value, f.getName(), f.getType().getStandardType(), value.get(f.getName()));
        }
      }

      return mapper.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new DebeziumException(e);
    }
  }

}
