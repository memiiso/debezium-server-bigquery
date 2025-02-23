/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import io.debezium.DebeziumException;
import org.json.JSONObject;

import java.util.Map;

/**
 * @author Ismail Simsek
 */
public class StreamRecordConverter extends BaseRecordConverter {

  public StreamRecordConverter(String destination, JsonNode value, JsonNode key, JsonNode valueSchema, JsonNode keySchema, DebeziumConfig debeziumConfig) {
    super(destination, value, key, valueSchema, keySchema, debeziumConfig);
  }

  /**
   * Used by `bigquerystream` {@link StreamBigqueryChangeConsumer} consumer.
   * See https://cloud.google.com/bigquery/docs/write-api#data_type_conversions
   *
   * @param upsert            when set to true it adds change type column `_CHANGE_TYPE`. Otherwise, all events are considered as insert/append
   * @param upsertKeepDeletes when set to true it retains last deleted data row
   * @return returns Debezium events as {@link JSONObject}
   */
  @Override
  public JSONObject convert(Schema schema, boolean upsert, boolean upsertKeepDeletes) throws DebeziumException {
    Map<String, Object> jsonMap = mapper.convertValue(value, new TypeReference<>() {
    });
    // SET UPSERT meta field `_CHANGE_TYPE`! this additional field allows us to do deletes, updates in bigquery
    if (upsert) {
      // if its deleted row and upsertKeepDeletes = false, deleted records are deleted from target table
      if (!upsertKeepDeletes && jsonMap.get("__op").equals("d")) {
        jsonMap.put(CHANGE_TYPE_PSEUDO_COLUMN, "DELETE");
      } else {
        // if it's not deleted row or upsertKeepDeletes = true then add deleted record to target table
        jsonMap.put(CHANGE_TYPE_PSEUDO_COLUMN, "UPSERT");
      }
    }

    // fix the TS_MS fields.
    TS_MS_FIELDS.forEach(tsf -> {
      if (jsonMap.containsKey(tsf)) {
        // Convert millisecond to microseconds
        jsonMap.replace(tsf, ((Long) jsonMap.get(tsf) * 1000L));
      }
    });

    // Fix boolean fields, create this fields as BOOLEAN instead of STRING in BigQuery
    BOOLEAN_FIELDS.forEach(bf -> {
      if (jsonMap.containsKey(bf)) {
        jsonMap.replace(bf, Boolean.valueOf((String) jsonMap.get(bf)));
      }
    });

    return new JSONObject(jsonMap);
  }

  @Override
  protected Field schemaPrimitiveField(String fieldType, String fieldName, String fieldSemanticType) {
    switch (fieldType) {
      case "bytes":
        return Field.of(fieldName, StandardSQLTypeName.STRING);
      default:
        return super.schemaPrimitiveField(fieldType, fieldName, fieldSemanticType);
    }
  }
}