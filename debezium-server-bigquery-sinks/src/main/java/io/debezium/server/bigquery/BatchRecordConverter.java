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
import com.google.cloud.bigquery.Schema;
import io.debezium.DebeziumException;

/**
 * @author Ismail Simsek
 */
public class BatchRecordConverter extends BaseRecordConverter {

  public BatchRecordConverter(String destination, JsonNode value, JsonNode key, JsonNode valueSchema, JsonNode keySchema, DebeziumConfig debeziumConfig) {
    super(destination, value, key, valueSchema, keySchema, debeziumConfig);
  }

  /**
   * Used by `bigquerybatch` {@link BatchBigqueryChangeConsumer} consumer to convert debezium event to json string.
   *
   * @param schema Bigquery table schema
   * @return returns Debezium event as a single line json string
   * @throws JsonProcessingException
   */
  @Override
  public String convert(Schema schema, boolean upsert, boolean upsertKeepDeletes) throws DebeziumException {

    if (value == null || value.isNull()) {
      return null;
    }

    try {
      // handle event values
      if (schema != null) {
        for (Field f : schema.getFields()) {
          // skip non-existing values
          if (!value.has(f.getName())) {
            continue;
          }
          handleFieldValue((ObjectNode) value, f, value.get(f.getName()));
        }
      }

      return mapper.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new DebeziumException(e);
    }
  }

}
