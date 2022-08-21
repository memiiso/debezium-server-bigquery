/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import io.debezium.server.bigquery.shared.SourcePostgresqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import com.google.cloud.bigquery.TableResult;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@TestProfile(BatchBigqueryChangeConsumerTest.BatchBigqueryChangeConsumerTestProfile.class)
@Disabled("manual run")
public class BatchBigqueryChangeConsumerTest extends BaseBigqueryTest {

  @Test
  @Disabled
  public void testPerformance() throws Exception {
    int maxBatchSize = 1500;
    int iteration = 1;
    for (int i = 0; i <= iteration; i++) {
      new Thread(() -> {
        try {
          SourcePostgresqlDB.PGLoadTestDataTable(maxBatchSize, false);
        } catch (Exception e) {
          e.printStackTrace();
          Thread.currentThread().interrupt();
        }
      }).start();
    }

    Awaitility.await().atMost(Duration.ofSeconds(1200)).until(() -> {
      try {
        TableResult result = getTableData("testc.inventory.test_table");
        return result.getTotalRows() >= (long) iteration * maxBatchSize;
      } catch (Exception e) {
        return false;
      }
    });

    TableResult result = getTableData("testc.inventory.test_table");
    System.out.println("Row Count=" + result.getTotalRows());
  }

  @Test
  public void testSimpleUpload() {
    super.testSimpleUpload();
  }

  @Test
  public void testVariousDataTypeConversion() throws Exception {
    this.loadVariousDataTypeConversion();
  }

  public static class BatchBigqueryChangeConsumerTestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("debezium.sink.type", "bigquerybatch");
      return config;
    }
  }
}