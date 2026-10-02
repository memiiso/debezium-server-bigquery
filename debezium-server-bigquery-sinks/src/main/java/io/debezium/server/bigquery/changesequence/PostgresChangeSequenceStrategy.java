/*
 * Copyright memiiso Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.bigquery.changesequence;

import com.fasterxml.jackson.databind.JsonNode;
import io.debezium.DebeziumException;
import io.debezium.server.bigquery.ChangeSequenceNumber;

import java.math.BigInteger;
import java.util.List;

/**
 * ChangeSequenceStrategy implementation for PostgreSQL connectors.
 *
 * @author Ismail Simsek
 */
public class PostgresChangeSequenceStrategy implements ChangeSequenceStrategy {

  @Override
  public boolean supports(String connectorClass) {
    if (connectorClass == null || connectorClass.isBlank()) {
      return false;
    }
    String normalized = connectorClass.toLowerCase();
    return normalized.contains("postgres") || normalized.contains("postgresql");
  }

  @Override
  public ChangeSequenceNumber extract(JsonNode value) throws DebeziumException {
    BigInteger timestamp = ChangeSequenceNumber.numericField(value, "__source_ts_ns");
    BigInteger lsn = ChangeSequenceNumber.postgresLsnField(value, "__source_lsn");
    BigInteger transactionId = ChangeSequenceNumber.numericField(value, "__source_txId");
    BigInteger transactionOrder = ChangeSequenceNumber.numericField(value, "__transaction_total_order");
    return ChangeSequenceNumber.of(List.of(timestamp, lsn, transactionId, transactionOrder));
  }

  @Override
  public String name() {
    return "PostgreSQL";
  }
}
