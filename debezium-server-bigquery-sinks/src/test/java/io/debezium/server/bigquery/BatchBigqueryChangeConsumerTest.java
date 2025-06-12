/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import com.google.cloud.bigquery.LegacySQLTypeName;
import io.debezium.server.bigquery.shared.BigQueryGCP;
import io.debezium.server.bigquery.shared.SourcePostgresqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = BigQueryGCP.class, restrictToAnnotatedClass = true)
@TestProfile(BatchBigqueryChangeConsumerTest.TestProfile.class)
@DisabledIfEnvironmentVariable(named = "GITHUB_ACTIONS", matches = "true")
public class BatchBigqueryChangeConsumerTest extends BaseBigqueryTest {

  @BeforeAll
  public static void setup() throws InterruptedException {
    bqClient = BigQueryGCP.bigQueryClient();
    dropTables();
  }

  @Test
  public void testSimpleUpload() {
    Awaitility.await().atMost(Duration.ofSeconds(180)).until(() -> {
      String dest = "testc.inventory.customers";
      try {
        prettyPrint(dest);
        assertTableRowsAboveEqual(dest, 4);
        assertTableRowsAboveEqual(dest, 4, "DATE(__source_ts_ms) = CURRENT_DATE");
        assertTableRowsAboveEqual(dest, 4, "DATE(__ts_ms) = CURRENT_DATE");
        Assert.assertEquals(getTableField(dest, "__source_ts_ms").getType(), LegacySQLTypeName.TIMESTAMP);
        Assert.assertEquals(getTableField(dest, "__ts_ms").getType(), LegacySQLTypeName.TIMESTAMP);
        Assert.assertEquals(getTableField(dest, "__deleted").getType(), LegacySQLTypeName.BOOLEAN);
        return true;
      } catch (AssertionError | Exception e) {
        return false;
      }

    });

    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        String dest = "testc.inventory.geom";
        prettyPrint(dest);
        assertTableRowsMatch(dest, 3);
        return true;
      } catch (AssertionError | Exception e) {
        return false;
      }
    });
  }

  @Test
  public void testVariousDataTypeConversion() throws Exception {
    String dest = "testc.inventory.test_data_types";
    Awaitility.await().until(() -> {
      try {
        prettyPrint(dest);
        assertTableRowsAboveEqual(dest, 3);
        // '2019-07-09 02:28:10.123456+01' --> hour is UTC in BQ
        // TODO disabled because emulator has problem with TIMESTAMP values
//        assertTableRowsMatch(dest, 1, "c_timestamptz = TIMESTAMP('2019-07-09 02:28:10.123456+01')");
        // '2019-07-09 02:28:20.666666+01' --> hour is UTC in BQ
//        assertTableRowsMatch(dest, 1, "c_timestamptz = TIMESTAMP('2019-07-09 02:28:57.666666+01')");
        assertTableRowsAboveEqual(dest, 2, "DATE(c_timestamptz) = DATE('2019-07-09')");
        Assert.assertEquals(getTableField(dest, "c_timestamptz").getType(), LegacySQLTypeName.TIMESTAMP);
        Assert.assertEquals(getTableField(dest, "c_timestamp5").getType(), LegacySQLTypeName.DATETIME);
        Assert.assertEquals(getTableField(dest, "c_date").getType(), LegacySQLTypeName.DATE);
        Assert.assertEquals(getTableField(dest, "c_time").getType(), LegacySQLTypeName.TIME);
        Assert.assertEquals(getTableField(dest, "c_time_whtz").getType(), LegacySQLTypeName.TIME);
        // TODO disabled because emulator has problem with DATE values
//        assertTableRowsMatch(dest, 1, "c_date = DATE('2017-09-15')");
//        assertTableRowsMatch(dest, 1, "c_date = DATE('2017-02-10')");
//        assertTableRowsMatch(dest, 1, "int64(c_json.jfield) = 111 AND int64(c_jsonb.jfield) = 211");
//        assertTableRowsMatch(dest, 1, "int64(c_json.jfield) = 222 AND int64(c_jsonb.jfield) = 222");
        Assert.assertEquals(getTableField(dest, "c_json").getType(), LegacySQLTypeName.JSON);
        Assert.assertEquals(getTableField(dest, "c_jsonb").getType(), LegacySQLTypeName.JSON);
        Assert.assertEquals(getTableField(dest, "c_binary").getType(), LegacySQLTypeName.BYTES);
        return true;
      } catch (AssertionError | Exception e) {
        return false;
      }
    });

    SourcePostgresqlDB.runSQL("""
        INSERT INTO
           inventory.test_data_types
        VALUES
           (4 , '{"jfield": 222}'::json , '{"jfield": 222}'::jsonb , '2017-02-10'::DATE , '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:20.666666+01', '04:10:22 UTC', '04:05:22.789', INTERVAL '10' DAY, 'abcd'::bytea )
        ;""");
    Awaitility.await().until(() -> {
      try {
        prettyPrint(dest);
        assertTableRowsAboveEqual(dest, 4);
        return true;
      } catch (AssertionError | Exception e) {
        return false;
      }
    });
  }

  public static class TestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("debezium.sink.type", "bigquerybatch");
      return config;
    }
  }
}