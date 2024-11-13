/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * @author Ismail Simsek
 */
public class BatchRecordConverter extends BaseRecordConverter {

  public BatchRecordConverter(String destination, JsonNode value, JsonNode key, JsonNode valueSchema, JsonNode keySchema) {
    super(destination, value, key, valueSchema, keySchema);
  }
}
