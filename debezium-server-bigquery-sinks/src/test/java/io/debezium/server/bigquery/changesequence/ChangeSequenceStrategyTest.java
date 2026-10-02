/*
 * Copyright memiiso Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.bigquery.changesequence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.DebeziumException;
import io.debezium.server.bigquery.ChangeSequenceNumber;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class ChangeSequenceStrategyTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void mysqlStrategySupportsConnectorClasses() {
    MySqlChangeSequenceStrategy strategy = new MySqlChangeSequenceStrategy();
    assertTrue(strategy.supports("io.debezium.connector.mysql.MySqlConnector"));
    assertTrue(strategy.supports("io.debezium.connector.mariadb.MariaDbConnector"));
    assertTrue(strategy.supports("mysql"));
    assertFalse(strategy.supports("io.debezium.connector.postgresql.PostgresConnector"));
    assertFalse(strategy.supports("io.debezium.connector.mongodb.MongoDbConnector"));
    assertFalse(strategy.supports(null));
    assertFalse(strategy.supports(""));
  }

  @Test
  void mysqlStrategyExtractsCoordinates() throws Exception {
    MySqlChangeSequenceStrategy strategy = new MySqlChangeSequenceStrategy();
    JsonNode value = MAPPER.readTree(
        "{\"__source_ts_ns\":123456789,\"__source_file\":\"mysql-bin.001234\",\"__source_pos\":255,\"__source_row\":10}");
    ChangeSequenceNumber sequence = strategy.extract(value);
    assertEquals("00000000075BCD15/00000000000004D2/00000000000000FF/000000000000000A", sequence.toString());
  }

  @Test
  void mysqlStrategyFailsOnNonNumericFile() throws Exception {
    MySqlChangeSequenceStrategy strategy = new MySqlChangeSequenceStrategy();
    JsonNode value = MAPPER.readTree(
        "{\"__source_ts_ns\":123456789,\"__source_file\":\"mysql-bin.invalid\",\"__source_pos\":255,\"__source_row\":10}");
    DebeziumException ex = assertThrows(DebeziumException.class, () -> strategy.extract(value));
    assertTrue(ex.getMessage().contains("must end in a numeric component"));
  }

  @Test
  void postgresStrategySupportsConnectorClasses() {
    PostgresChangeSequenceStrategy strategy = new PostgresChangeSequenceStrategy();
    assertTrue(strategy.supports("io.debezium.connector.postgresql.PostgresConnector"));
    assertTrue(strategy.supports("postgres"));
    assertTrue(strategy.supports("postgresql"));
    assertFalse(strategy.supports("io.debezium.connector.mysql.MySqlConnector"));
    assertFalse(strategy.supports("io.debezium.connector.oracle.OracleConnector"));
    assertFalse(strategy.supports(null));
  }

  @Test
  void postgresStrategyExtractsCoordinates() throws Exception {
    PostgresChangeSequenceStrategy strategy = new PostgresChangeSequenceStrategy();
    JsonNode value = MAPPER.readTree(
        "{\"__source_ts_ns\":10,\"__source_lsn\":\"0/16B3748\",\"__source_txId\":41,\"__transaction_total_order\":7}");
    ChangeSequenceNumber sequence = strategy.extract(value);
    assertEquals("000000000000000A/00000000016B3748/0000000000000029/0000000000000007", sequence.toString());
  }

  @Test
  void factoryAutoDetectsMysqlConnector() {
    ChangeSequenceStrategy strategy = ChangeSequenceFactory.getStrategy(
        Optional.empty(),
        Optional.of("io.debezium.connector.mysql.MySqlConnector")
    );
    assertNotNull(strategy);
    assertEquals("MySQL", strategy.name());
  }

  @Test
  void factoryAutoDetectsPostgresConnector() {
    ChangeSequenceStrategy strategy = ChangeSequenceFactory.getStrategy(
        Optional.empty(),
        Optional.of("io.debezium.connector.postgresql.PostgresConnector")
    );
    assertNotNull(strategy);
    assertEquals("PostgreSQL", strategy.name());
  }

  @Test
  void factoryRespectsExplicitConfiguration() {
    ChangeSequenceStrategy strategy = ChangeSequenceFactory.getStrategy(
        Optional.of("postgres"),
        Optional.of("io.debezium.connector.mysql.MySqlConnector") // config overrides connector
    );
    assertNotNull(strategy);
    assertEquals("PostgreSQL", strategy.name());
  }

  @Test
  void factoryFailsFastOnUnsupportedConnector() {
    DebeziumException ex = assertThrows(DebeziumException.class, () ->
        ChangeSequenceFactory.getStrategy(
            Optional.empty(),
            Optional.of("io.debezium.connector.mongodb.MongoDbConnector")
        )
    );
    assertTrue(ex.getMessage().contains("BigQuery change sequence is not supported for connector class: 'io.debezium.connector.mongodb.MongoDbConnector'"));
  }

  @Test
  void factoryFailsFastWhenNoStrategyOrConnectorConfigured() {
    DebeziumException ex = assertThrows(DebeziumException.class, () ->
        ChangeSequenceFactory.getStrategy(
            Optional.empty(),
            Optional.empty()
        )
    );
    assertTrue(ex.getMessage().contains("neither 'debezium.sink.bigquerystream.change-sequence.strategy' nor 'debezium.source.connector.class' is configured"));
  }
}
