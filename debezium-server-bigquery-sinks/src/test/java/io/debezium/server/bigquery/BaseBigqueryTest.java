/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import io.debezium.DebeziumException;
import io.debezium.server.bigquery.shared.SourcePostgresqlDB;

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
import static io.debezium.server.bigquery.ConfigSource.*;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */

public class BaseBigqueryTest {
  public static final Logger LOGGER = LoggerFactory.getLogger(BaseBigqueryTest.class);
  public static BigQuery bqClient;
  public static String CREATE_TEST_TABLE = "" +
      "CREATE TABLE IF NOT EXISTS inventory.test_table (" +
      " c_id INTEGER ," +
      " c_id2 INTEGER ," +
      " c_data TEXT," +
      " c_text TEXT," +
      " c_varchar VARCHAR(1666) ," +
      " PRIMARY KEY (c_id, c_id2)" +
      " );";

  public static String CREATE_TEST_DATATYPES_TABLE = "\n" +
      "        DROP TABLE IF EXISTS inventory.test_datatypes;\n" +
      "        CREATE TABLE IF NOT EXISTS inventory.test_datatypes (\n" +
      "            c_id INTEGER ,\n" +
      "            c_json JSON,\n" +
      "            c_jsonb JSONB,\n" +
      "            c_date DATE,\n" +
      "            c_timestamp0 TIMESTAMP(0),\n" +
      "            c_timestamp1 TIMESTAMP(1),\n" +
      "            c_timestamp2 TIMESTAMP(2),\n" +
      "            c_timestamp3 TIMESTAMP(3),\n" +
      "            c_timestamp4 TIMESTAMP(4),\n" +
      "            c_timestamp5 TIMESTAMP(5),\n" +
      "            c_timestamp6 TIMESTAMP(6),\n" +
      "            c_timestamptz TIMESTAMPTZ,\n" +
      "            PRIMARY KEY (c_id)\n" +
      "          );" +
      "ALTER TABLE inventory.test_datatypes REPLICA IDENTITY FULL;";

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

  public void loadVariousDataTypeConversion() throws Exception {
    String sql = "INSERT INTO inventory.test_datatypes (" +
        "c_id, c_json, c_jsonb, c_date, " +
        "c_timestamp0, c_timestamp1, c_timestamp2, c_timestamp3, c_timestamp4, c_timestamp5, c_timestamp6, " +
        "c_timestamptz)" +
        "VALUES (1, null, null, null,null,null,null," +
        "null,null,null,null,null)," +
        "(2, '{\"reading\": 1123}'::json, '{\"reading\": 1123}'::jsonb, '2017-02-10'::DATE, " +
        "'2019-07-09 02:28:57+01', '2019-07-09 02:28:57.1+01', '2019-07-09 02:28:57.12+01', " +
        "'2019-07-09 02:28:57.123+01', '2019-07-09 02:28:57.1234+01','2019-07-09 02:28:57.12345+01', " +
        "'2019-07-09 02:28:57.123456+01', '2019-07-09 02:28:57.123456+01')," +
        "(3, '{\"reading\": 1123}'::json, '{\"reading\": 1123}'::jsonb, '2017-02-10'::DATE, " +
        "'2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', " +
        "'2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01','2019-07-09 02:28:57.666666+01', " +
        "'2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01')";

    SourcePostgresqlDB.runSQL("DELETE FROM  inventory.test_datatypes WHERE c_id>0;");
    SourcePostgresqlDB.runSQL(sql);
    String dest = "testc.inventory.test_datatypes";
    Awaitility.await().atMost(Duration.ofSeconds(320)).until(() -> {
      try {
        // @TODO validate resultset!!
        TableResult result = getTableData(dest);
        result.iterateAll().forEach(System.out::println);
        return result.getTotalRows() >= 6
            && getTableData(dest, "WHERE DATE(c_timestamptz) = DATE('2019-07-09')").getTotalRows() >= 6
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
