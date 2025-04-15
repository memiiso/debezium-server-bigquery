/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import com.google.cloud.bigquery.LegacySQLTypeName;
import io.debezium.server.bigquery.shared.BigQueryDB;
import io.debezium.server.bigquery.shared.SourcePostgresqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@TestProfile(StreamBigqueryChangeConsumerNestedTest.TestProfile.class)
@QuarkusTestResource(value = BigQueryDB.class, restrictToAnnotatedClass = true)
public class StreamBigqueryChangeConsumerNestedTest extends BaseBigqueryTest {

  @BeforeAll
  public static void setup() throws InterruptedException {
    bqClient = BigQueryDB.bigQueryClient();
//    dropTables();
    Thread.sleep(5000);
  }

  @Test
  public void testSimpleUpload() {
    Awaitility.await().atMost(Duration.ofSeconds(180)).until(() -> {
      String dest = "testc.inventory.customers";
      try {
        prettyPrint(dest);
        assertTableRowsAboveEqual(dest, 4);
        Assert.assertEquals(getTableField(dest, "before").getType(), LegacySQLTypeName.JSON);
        Assert.assertEquals(getTableField(dest, "after").getType(), LegacySQLTypeName.JSON);
        Assert.assertEquals(getTableField(dest, "source").getType(), LegacySQLTypeName.JSON);
        Assert.assertEquals(getTableField(dest, "transaction").getType(), LegacySQLTypeName.JSON);
        Assert.assertEquals(getTableField(dest, "op").getType(), LegacySQLTypeName.STRING);
        Assert.assertEquals(getTableField(dest, "ts_ms").getType(), LegacySQLTypeName.INTEGER);
        Assert.assertEquals(getTableField(dest, "ts_ns").getType(), LegacySQLTypeName.INTEGER);
        return true;
      } catch (AssertionError | Exception e) {
        LOGGER.error("Error: {}", e.getMessage());
        return false;
      }
    });
  }


  public static class TestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("debezium.sink.type", "bigquerystream");
      config.put("debezium.transforms", ",");
      config.put("debezium.sink.batch.struct-as-json", "true");
      return config;
    }
  }
}
