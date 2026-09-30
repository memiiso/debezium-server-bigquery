/*
 * Copyright memiiso Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.bigquery.history;

import io.debezium.DebeziumException;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BigquerySchemaHistoryMigrationTest {

  @Test
  void loadFileSchemaHistoryThrowsOnNonExistentFile() {
    BigquerySchemaHistory history = new BigquerySchemaHistory();
    File nonExistent = new File("/nonexistent/path/to/schema_history.json");

    DebeziumException thrown = assertThrows(
        DebeziumException.class,
        () -> history.loadFileSchemaHistory(nonExistent)
    );

    assertTrue(thrown.getMessage().contains("Failed to migrate history record from history file at"));
  }
}
