/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.TableResult;
import io.debezium.server.bigquery.shared.BigQueryGCP;
import io.debezium.server.bigquery.shared.SourcePostgresqlDB;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Ismail Simsek
 */
@QuarkusTest
@WithTestResource(value = SourcePostgresqlDB.class)
@WithTestResource(value = BigQueryGCP.class)
@TestProfile(BatchBigqueryChangeConsumerTest.TestProfile.class)
@DisabledIfEnvironmentVariable(named = "GITHUB_ACTIONS", matches = "true")
public class BatchBigqueryChangeConsumerTest extends BaseBigqueryTest {

  @BeforeAll
  public static void setup() {
    bqClient = BigQueryGCP.bigQueryClient();
  }
  public static final Logger LOGGER = LoggerFactory.getLogger(BaseBigqueryTest.class);

  @Test
  public void testSimpleUpload() {

    Awaitility.await().atMost(Duration.ofSeconds(180)).until(() -> {
      String dest = "testc.inventory.customers";
      try {
        TableResult result = getTableData(dest);
        result.iterateAll().forEach(System.out::println);
        return getTableData(dest).getTotalRows() >= 4
            && getTableData(dest, "DATE(__source_ts_ms) = CURRENT_DATE").getTotalRows() >= 4
            && getTableData(dest, "DATE(__ts_ms) = CURRENT_DATE").getTotalRows() >= 4
            && getTableField(dest, "__source_ts_ms").getType() == LegacySQLTypeName.TIMESTAMP
            && getTableField(dest, "__ts_ms").getType() == LegacySQLTypeName.TIMESTAMP
            && getTableField(dest, "__deleted").getType() == LegacySQLTypeName.BOOLEAN
            ;
      } catch (Exception e) {
//        e.printStackTrace();
        return false;
      }
    });

    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        TableResult result = getTableData("testc.inventory.geom");
        result.iterateAll().forEach(System.out::println);
        return result.getTotalRows() >= 3;
      } catch (Exception e) {
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