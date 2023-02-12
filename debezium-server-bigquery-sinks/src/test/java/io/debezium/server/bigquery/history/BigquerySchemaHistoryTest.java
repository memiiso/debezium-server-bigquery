/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery.history;

import io.debezium.config.Configuration;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.DatabaseHistoryListener;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import io.debezium.util.Collect;

import java.sql.Types;
import java.util.Map;

import org.junit.jupiter.api.*;
import static io.debezium.server.bigquery.TestConfigSource.*;


@Disabled
class BigquerySchemaHistoryTest {

  protected BigquerySchemaHistory history;
  static String databaseName = "db";
  static String schemaName = "myschema";
  static String ddl = "CREATE TABLE foo ( first VARCHAR(22) NOT NULL );";
  static Map<String, Object> source;
  static Map<String, Object> position;
  static TableId tableId;
  static Table table;
  static TableChanges tableChanges;
  static HistoryRecord historyRecord;
  static Map<String, Object> position2;
  static TableId tableId2;
  static Table table2;
  static TableChanges tableChanges2;
  static HistoryRecord historyRecord2;
  static DdlParser ddlParser = new TestingAntlrDdlParser();

  @BeforeEach
  public void beforeEach() {
    history = getSchemaHist();
    Assertions.assertEquals(BQ_DATASET+".__debezium_database_history_storage_test_table",history.getTableFullName());
    history.start();
  }
  
  @BeforeAll
  public static void beforeClass() {
    source = Collect.linkMapOf("server", "abc");
    position = Collect.linkMapOf("file", "x.log", "positionInt", 100, "positionLong", Long.MAX_VALUE, "entry", 1);
    tableId = new TableId(databaseName, schemaName, "foo");
    table = Table.editor()
        .tableId(tableId)
        .addColumn(Column.editor()
            .name("first")
            .jdbcType(Types.VARCHAR)
            .type("VARCHAR")
            .length(22)
            .optional(false)
            .create())
        .setPrimaryKeyNames("first")
        .create();
    tableChanges = new TableChanges().create(table);
    historyRecord = new HistoryRecord(source, position, databaseName, schemaName, ddl, tableChanges);
    //
    position2 = Collect.linkMapOf("file", "x.log", "positionInt", 100, "positionLong", Long.MAX_VALUE, "entry", 2);
    tableId2 = new TableId(databaseName, schemaName, "bar");
    table2 = Table.editor()
        .tableId(tableId2)
        .addColumn(Column.editor()
            .name("first")
            .jdbcType(Types.VARCHAR)
            .type("VARCHAR")
            .length(22)
            .optional(false)
            .create())
        .setPrimaryKeyNames("first")
        .create();
    tableChanges2 = new TableChanges().create(table2);
    historyRecord2 = new HistoryRecord(source, position, databaseName, schemaName, ddl, tableChanges2);
  }

  @AfterEach
  public void afterEach() {
    if (history != null) {
      history.stop();
    }
  }

  @Test
  public void shouldNotFailMultipleInitializeStorage() {
    history.initializeStorage();
    history.initializeStorage();
    history.initializeStorage();
    Assertions.assertTrue(history.storageExists());
    Assertions.assertTrue(history.exists());
  }

  @Test
  public void shouldRecordChangesAndRecover() throws InterruptedException {
    history.record(source, position, databaseName, schemaName, ddl, tableChanges);
    history.record(source, position, databaseName, schemaName, ddl, tableChanges);
    Tables tables = new Tables();
    history.recover(source, position, tables, ddlParser);
    Assertions.assertEquals(tables.size(), 1);
    Assertions.assertEquals(tables.forTable(tableId), table);
    history.record(source, position2, databaseName, schemaName, ddl, tableChanges2);
    history.record(source, position2, databaseName, schemaName, ddl, tableChanges2);
    history.stop();
    // after restart, it should recover history correctly
    BigquerySchemaHistory history2 = getSchemaHist();
    history2.start();
    Assertions.assertTrue(history2.storageExists());
    Assertions.assertTrue(history2.exists());
    Tables tables2 = new Tables();
    history2.recover(source, position2, tables2, ddlParser);
    Assertions.assertEquals(tables2.size(), 2);
    Assertions.assertEquals(tables2.forTable(tableId2), table2);
  }
  
  private BigquerySchemaHistory getSchemaHist(){

    BigquerySchemaHistory history2 = new BigquerySchemaHistory();
    history2.configure(Configuration.create()
        .with("debezium.sink.type", "bigquerybatch")
        .with("debezium.source.database.history", "io.debezium.server.bigquery.history.BigquerySchemaHistory")
        .with("debezium.source.database.history.bigquery.table-name", "__debezium_database_history_storage_test_table")
        .with("debezium.source.database.history.bigquery.migrate-history-file", "src/test/resources/history.dat")
        .with("database.history", "io.debezium.server.bigquery.history.BigquerySchemaHistory")
        .with("database.history.bigquery.table-name", "__debezium_database_history_storage_test_table")
        .with("database.history.bigquery.migrate-history-file", "src/test/resources/dbhistory.txt")
        .with("debezium.sink.type", "bigquerybatch")
        .with("debezium.sink.bigquerybatch.project", "ppro-bi-gcp-dev")
        .with("debezium.sink.bigquerybatch.dataset", BQ_DATASET)
        .with("debezium.sink.bigquerybatch.location", BQ_LOCATION)
        .with("debezium.sink.bigquerybatch.credentials-file", BQ_CRED_FILE)
        .build(), null, DatabaseHistoryListener.NOOP, true);
    
    return history2;
  } 
}