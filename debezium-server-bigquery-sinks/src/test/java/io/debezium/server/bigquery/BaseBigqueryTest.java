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
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
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
    //System.out.println(query);
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

  public void testSimpleUpload() {
    Awaitility.await().atMost(Duration.ofSeconds(180)).until(() -> {
      String dest = "testc.inventory.customers";
      try {
        TableResult result = BaseBigqueryTest.getTableData(dest);
        result.iterateAll().forEach(System.out::println);
        return getTableData(dest).getTotalRows() >= 4
            && getTableData(dest, "DATE(__source_ts_ms) = CURRENT_DATE").getTotalRows() >= 4
            && getTableData(dest, "DATE(__ts_ms) = CURRENT_DATE").getTotalRows() >= 4
            && getTableField(dest, "__source_ts_ms").getType() == LegacySQLTypeName.TIMESTAMP
            && getTableField(dest, "__ts_ms").getType() == LegacySQLTypeName.TIMESTAMP
            && getTableField(dest, "__deleted").getType() == LegacySQLTypeName.BOOLEAN
            ;
      } catch (Exception e) {
        return false;
      }
    });

    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        TableResult result = BaseBigqueryTest.getTableData("testc.inventory.geom");
        result.iterateAll().forEach(System.out::println);
        return result.getTotalRows() >= 3;
      } catch (Exception e) {
        return false;
      }
    });
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

}
