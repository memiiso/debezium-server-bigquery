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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Ismail Simsek
 */
@QuarkusTest
@Tag("integration")
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@TestProfile(StreamBigqueryChangeConsumerNestedTest.TestProfile.class)
@QuarkusTestResource(value = BigQueryDB.class, restrictToAnnotatedClass = true)
public class StreamBigqueryChangeConsumerNestedTest extends BaseBigqueryTest {

  @BeforeAll
  public static void setup() throws InterruptedException {
    bqClient = BigQueryDB.bigQueryClient();
    Thread.sleep(5000);
  }

  @Test
  public void testSimpleUpload() {
    Awaitility.await().atMost(Duration.ofSeconds(180)).until(() -> {
      String dest = "testc.inventory.customers";
      try {
        prettyPrint(dest);
        assertTableRowsAboveEqual(dest, 4);
        Assertions.assertEquals(LegacySQLTypeName.JSON, getTableField(dest, "before").getType());
        Assertions.assertEquals(LegacySQLTypeName.JSON, getTableField(dest, "after").getType());
        Assertions.assertEquals(LegacySQLTypeName.JSON, getTableField(dest, "source").getType());
        Assertions.assertEquals(LegacySQLTypeName.JSON, getTableField(dest, "transaction").getType());
        Assertions.assertEquals(LegacySQLTypeName.STRING, getTableField(dest, "op").getType());
        Assertions.assertEquals(LegacySQLTypeName.INTEGER, getTableField(dest, "ts_ms").getType());
        Assertions.assertEquals(LegacySQLTypeName.INTEGER, getTableField(dest, "ts_ns").getType());
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
      config.put("debezium.sink.batch.nested-as-json", "true");
      return config;
    }
  }
}
