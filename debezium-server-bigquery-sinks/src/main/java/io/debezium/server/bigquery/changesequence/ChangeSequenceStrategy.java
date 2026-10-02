/*
 * Copyright memiiso Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.bigquery.changesequence;

import com.fasterxml.jackson.databind.JsonNode;
import io.debezium.DebeziumException;
import io.debezium.server.bigquery.ChangeSequenceNumber;

/**
 * Strategy for extracting database-specific CDC coordinates into a ChangeSequenceNumber.
 *
 * @author Ismail Simsek
 */
public interface ChangeSequenceStrategy {

  /**
   * Returns true if this strategy supports the given connector class or alias.
   *
   * @param connectorClass connector class name or database identifier
   * @return true if supported
   */
  boolean supports(String connectorClass);

  /**
   * Extracts CDC source coordinates from the Debezium payload value.
   *
   * @param value Debezium flattened record value JSON node
   * @return ChangeSequenceNumber instance
   * @throws DebeziumException if required coordinates cannot be extracted
   */
  ChangeSequenceNumber extract(JsonNode value) throws DebeziumException;

  /**
   * Human-readable name for logging.
   */
  default String name() {
    return getClass().getSimpleName();
  }
}
