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
 * Default strategy that delegates to MySQL or PostgreSQL strategy based on present source fields,
 * preserving baseline detection behavior.
 *
 * @author Ismail Simsek
 */
public class DefaultChangeSequenceStrategy implements ChangeSequenceStrategy {

  private final MySqlChangeSequenceStrategy mysql = new MySqlChangeSequenceStrategy();
  private final PostgresChangeSequenceStrategy postgres = new PostgresChangeSequenceStrategy();

  @Override
  public boolean supports(String connectorClass) {
    return mysql.supports(connectorClass) || postgres.supports(connectorClass);
  }

  @Override
  public ChangeSequenceNumber extract(JsonNode value) throws DebeziumException {
    if (ChangeSequenceNumber.hasValue(value, "__source_file")) {
      return mysql.extract(value);
    }
    if (ChangeSequenceNumber.hasValue(value, "__source_lsn")) {
      return postgres.extract(value);
    }
    throw new DebeziumException("Cannot construct BigQuery change sequence: no supported source coordinates found; "
        + "configure unwrap add.fields with source.ts_ns,source.file,source.pos,source.row for MySQL or "
        + "source.ts_ns,source.lsn,source.txId,transaction.total_order for PostgreSQL");
  }

  @Override
  public String name() {
    return "Default";
  }
}
