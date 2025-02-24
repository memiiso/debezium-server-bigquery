/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import io.debezium.server.bigquery.shared.BigQueryTableResultPrinter;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static io.debezium.server.bigquery.shared.BigQueryDB.BQ_DATASET;

/**
 *
 * @author Ismail Simsek
 */

public class BaseBigqueryTest {
  public static final Logger LOGGER = LoggerFactory.getLogger(BaseBigqueryTest.class);
  public static BigQuery bqClient;

  public static Schema getTableSchema(String destination) throws InterruptedException {
    TableId tableId = getTableId(destination);
    return bqClient.getTable(tableId).getDefinition().getSchema();
  }

  public static TableResult simpleQuery(String query) throws InterruptedException {
    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
    try {
      return bqClient.query(queryConfig);
    } catch (Exception e) {
      return null;
    }
  }

  public static void truncateTable(String destination) {
    TableId tableId = getTableId(destination);
    try {
      simpleQuery("TRUNCATE TABLE " + tableId.getProject() + "." + tableId.getDataset() + "." + tableId.getTable());
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static void dropTable(String destination) {
    TableId tableId = getTableId(destination);
    LOGGER.warn("Dropping table {}", tableId);
    try {
      simpleQuery("DROP TABLE IF EXISTS " + tableId.getProject() + "." + tableId.getDataset() + "." + tableId.getTable());
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static void truncateTables() {
    try {
      TableResult result = simpleQuery("select \n" +
          "concat(\"DELETE FROM \",table_schema,\".\",   table_name, \" WHERE 1=1;\" ) AS TRUNCATE_TABLES_QUERY\n" +
          "from testdataset.INFORMATION_SCHEMA.TABLES\n" +
          "where table_schema = 'testdataset'\n");
      for (FieldValueList row : result.iterateAll()) {
        String sql = row.get("TRUNCATE_TABLES_QUERY").getStringValue();
        LOGGER.warn("Running: " + sql);
        simpleQuery(sql);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static void dropTables() {
    try {
      TableResult result = simpleQuery("select \n" +
          "concat(\"DROP TABLE \",table_schema,\".\",   table_name, \";\" ) AS DROP_TABLES_QUERY\n" +
          "from testdataset.INFORMATION_SCHEMA.TABLES\n" +
          "where table_schema = 'testdataset'\n");
      if (result == null) {
        return;
      }
      for (FieldValueList row : result.iterateAll()) {
        String sql = row.get("DROP_TABLES_QUERY").getStringValue();
        LOGGER.warn("Running: " + sql);
        simpleQuery(sql);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static TableResult getTableData(String destination, String where) throws InterruptedException {
    TableId tableId = getTableId(destination);
    return simpleQuery("SELECT * FROM " + tableId.getProject() + "." + tableId.getDataset() + "." + tableId.getTable()
        + " WHERE " + where
    );
  }

  public static Field getTableField(String destination, String fieldName) throws InterruptedException {
    Field field = null;
    TableId tableId = getTableId(destination);
    for (Field f : getTableSchema(destination).getFields()) {
      if (Objects.equals(f.getName(), fieldName)) {
        field = f;
        break;
      }
    }
    return field;
  }

  public static TableResult getTableData(String destination) throws InterruptedException {
    return getTableData(destination, "1=1");
  }

  public static TableId getTableId(String destination) {
    final String tableName = destination
        .replaceAll("", "")
        .replace(".", "_");
    return TableId.of(bqClient.getOptions().getProjectId(), BQ_DATASET, tableName);
  }

  public static void assertTableRowsMatch(String dest, long expectedRows) throws InterruptedException {
    assertTableRowsMatch(dest, expectedRows, "1=1");
  }

  public static void assertTableRowsMatch(String dest, long expectedRows, String filter) throws InterruptedException {
    TableResult tableResult = getTableData(dest, filter);
    Assert.assertEquals("Total rows didn't match! filter:" + filter, expectedRows, tableResult.getTotalRows());
  }

  public static void assertTableRowsAboveEqual(String dest, long expectedRows) throws InterruptedException {
    assertTableRowsAboveEqual(dest, expectedRows, "1=1");
  }

  public static void assertTableRowsAboveEqual(String dest, long expectedRows, String filter) throws InterruptedException {
    TableResult tableResult = getTableData(dest, filter);
    Assert.assertTrue("Total rows didn't match! filter:" + filter, tableResult.getTotalRows() >= expectedRows);
  }


  public static void prettyPrint(String destination) throws InterruptedException {
    prettyPrint(getTableData(destination));
  }

  public static void prettyPrint(TableResult tableResult) {
    BigQueryTableResultPrinter.prettyPrintTable(tableResult);
  }

}
