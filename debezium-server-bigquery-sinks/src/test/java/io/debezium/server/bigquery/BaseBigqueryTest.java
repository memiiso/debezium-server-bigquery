/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import io.debezium.DebeziumException;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;

import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.debezium.server.bigquery.TestConfigSource.*;

/**
 *
 * @author Ismail Simsek
 */

public class BaseBigqueryTest {
  public static final Logger LOGGER = LoggerFactory.getLogger(BaseBigqueryTest.class);
  public static BigQuery bqClient;

  static {
    GoogleCredentials credentials;
    try {
      if (BQ_CRED_FILE != null && !BQ_CRED_FILE.isEmpty()) {
        credentials = GoogleCredentials.fromStream(new FileInputStream(BQ_CRED_FILE));
      } else {
        credentials = GoogleCredentials.getApplicationDefault();
      }
    } catch (IOException e) {
      throw new DebeziumException("Failed to initialize google credentials", e);
    }

    bqClient = BigQueryOptions.newBuilder()
        .setCredentials(credentials)
        //.setProjectId(BQ_PROJECT) // use project from cred file!?
        .setLocation(BQ_LOCATION)
        .setRetrySettings(
            RetrySettings.newBuilder()
                // Set the max number of attempts
                .setMaxAttempts(5)
                // InitialRetryDelay controls the delay before the first retry. 
                // Subsequent retries will use this value adjusted according to the RetryDelayMultiplier. 
                .setInitialRetryDelay(org.threeten.bp.Duration.ofSeconds(5))
                .setMaxRetryDelay(org.threeten.bp.Duration.ofSeconds(60))
                // Set the backoff multiplier
                .setRetryDelayMultiplier(2.0)
                // Set the max duration of all attempts
                .setTotalTimeout(org.threeten.bp.Duration.ofMinutes(5))
                .build()
        )
        .build()
        .getService();
    LOGGER.warn("Using BQ project {}", bqClient.getOptions().getProjectId());
  }

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
        System.out.println("" + getTableField(dest, "__source_ts").getType());
        System.out.println("" + getTableField(dest, "__deleted").getType());
        return getTableData(dest).getTotalRows() >= 4
            && getTableData(dest, "DATE(__source_ts) = CURRENT_DATE").getTotalRows() >= 4
            && getTableField(dest, "__source_ts").getType() == LegacySQLTypeName.TIMESTAMP
            && getTableField(dest, "__deleted").getType() == LegacySQLTypeName.STRING
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

  public void loadVariousDataTypeConversion() {
    //SourcePostgresqlDB.runSQL("DELETE FROM  inventory.test_data_types WHERE c_id>0;");
    String dest = "testc.inventory.test_data_types";
    Awaitility.await().atMost(Duration.ofSeconds(320)).until(() -> {
      try {
        //getTableData(dest).iterateAll().forEach(System.out::println);
        return getTableData(dest).getTotalRows() >= 3
            // '2019-07-09 02:28:10.123456+01' --> hour is UTC in BQ
            && getTableData(dest, "c_timestamptz = TIMESTAMP('2019-07-09T01:28:10.123456Z')").getTotalRows() == 1
            // '2019-07-09 02:28:20.666666+01' --> hour is UTC in BQ
            && getTableData(dest, "c_timestamptz = TIMESTAMP('2019-07-09T01:28:20.666666Z')").getTotalRows() == 1
            && getTableData(dest, "DATE(c_timestamptz) = DATE('2019-07-09')").getTotalRows() >= 2
            && getTableField(dest, "c_timestamptz").getType() == LegacySQLTypeName.TIMESTAMP
            ;
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
