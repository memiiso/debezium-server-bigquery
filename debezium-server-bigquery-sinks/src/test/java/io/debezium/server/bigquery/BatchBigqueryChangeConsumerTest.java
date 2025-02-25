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
    Awaitility.await().atMost(Duration.ofSeconds(320)).until(() -> {
      try {
        return getTableData(dest).getTotalRows() >= 3
            // '2019-07-09 02:28:10.123456+01' --> hour is UTC in BQ
            && getTableData(dest, "c_timestamptz = TIMESTAMP('2019-07-09T01:28:10.123456Z')").getTotalRows() == 1
            // '2019-07-09 02:28:20.666666+01' --> hour is UTC in BQ
            && getTableData(dest, "c_timestamptz = TIMESTAMP('2019-07-09T01:28:20.666666Z')").getTotalRows() == 1
            && getTableData(dest, "DATE(c_timestamptz) = DATE('2019-07-09')").getTotalRows() >= 2
            && getTableField(dest, "c_timestamptz").getType() == LegacySQLTypeName.TIMESTAMP
            && getTableData(dest, "c_date = DATE('2017-09-15')").getTotalRows() == 1
            && getTableData(dest, "c_date = DATE('2017-02-10')").getTotalRows() == 1
            ;
      } catch (Exception e) {
        LOGGER.error(e.getMessage());
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