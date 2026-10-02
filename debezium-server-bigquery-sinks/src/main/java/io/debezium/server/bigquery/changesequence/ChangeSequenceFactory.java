/*
 * Copyright memiiso Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.bigquery.changesequence;

import io.debezium.DebeziumException;
import io.debezium.server.bigquery.ConsumerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Factory and resolver for ChangeSequenceStrategy instances.
 *
 * @author Ismail Simsek
 */
public final class ChangeSequenceFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChangeSequenceFactory.class);

  private ChangeSequenceFactory() {
  }

  public static ChangeSequenceStrategy getStrategy(
      Optional<String> configuredStrategy,
      Optional<String> connectorClass) {

    // 1. Explicitly configured strategy takes highest precedence
    if (configuredStrategy != null && configuredStrategy.isPresent() && !configuredStrategy.get().isBlank()) {
      String strategyName = configuredStrategy.get().trim();
      LOGGER.info("Configured ChangeSequenceStrategy: {}", strategyName);
      String lower = strategyName.toLowerCase();
      if (lower.equals("mysql") || lower.equals("mariadb")) {
        return new MySqlChangeSequenceStrategy();
      }
      if (lower.equals("postgres") || lower.equals("postgresql")) {
        return new PostgresChangeSequenceStrategy();
      }
      return ConsumerUtil.loadInstance(strategyName, ChangeSequenceStrategy.class);
    }

    // 2. Resolve from connector class if provided
    if (connectorClass != null && connectorClass.isPresent() && !connectorClass.get().isBlank()) {
      String connector = connectorClass.get().trim();
      String lower = connector.toLowerCase();
      if (lower.contains("mysql") || lower.contains("mariadb")) {
        LOGGER.info("Resolved ChangeSequenceStrategy 'MySQL' for connector: {}", connector);
        return new MySqlChangeSequenceStrategy();
      }
      if (lower.contains("postgres") || lower.contains("postgresql")) {
        LOGGER.info("Resolved ChangeSequenceStrategy 'PostgreSQL' for connector: {}", connector);
        return new PostgresChangeSequenceStrategy();
      }

      // Fail fast at startup if connector is unsupported
      throw new DebeziumException("BigQuery change sequence is not supported for connector class: '" + connector + "'");
    }

    // Fail fast if neither is configured
    throw new DebeziumException("BigQuery change sequence is enabled ('debezium.sink.bigquerystream.change-sequence.enabled=true'), "
        + "but neither 'debezium.sink.bigquerystream.change-sequence.strategy' nor 'debezium.source.connector.class' is configured.");
  }
}
