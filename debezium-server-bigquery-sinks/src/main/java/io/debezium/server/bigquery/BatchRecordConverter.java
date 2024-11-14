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
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import io.debezium.DebeziumException;

import java.time.Instant;
import java.time.LocalDate;

/**
 * @author Ismail Simsek
 */
public class BatchRecordConverter extends BaseRecordConverter {

  public BatchRecordConverter(String destination, JsonNode value, JsonNode key, JsonNode valueSchema, JsonNode keySchema) {
    super(destination, value, key, valueSchema, keySchema);
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
          if (f.getType() == LegacySQLTypeName.JSON && value.has(f.getName())) {
            ((ObjectNode) value).replace(f.getName(), mapper.readTree(value.get(f.getName()).asText("{}")));
          }
          // process DATE values
          if (f.getType() == LegacySQLTypeName.DATE && value.has(f.getName()) && !value.get(f.getName()).isNull()) {
            ((ObjectNode) value).put(f.getName(), LocalDate.ofEpochDay(value.get(f.getName()).longValue()).toString());
          }
        }
      }

      // Process DEBEZIUM TS_MS values
      TS_MS_FIELDS.forEach(tsf -> {
        if (value.has(tsf)) {
          ((ObjectNode) value).put(tsf, Instant.ofEpochMilli(value.get(tsf).longValue()).toString());
        }
      });

      // Process DEBEZIUM BOOLEAN values
      BOOLEAN_FIELDS.forEach(bf -> {
        if (value.has(bf)) {
          ((ObjectNode) value).put(bf, Boolean.valueOf(value.get(bf).asText()));
        }
      });

      return mapper.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new DebeziumException(e);
    }
  }

}
