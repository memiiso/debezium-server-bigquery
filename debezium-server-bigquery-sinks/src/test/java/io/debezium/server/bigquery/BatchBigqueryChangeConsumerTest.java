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
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@TestProfile(BatchBigqueryChangeConsumerTest.BatchBigqueryChangeConsumerTestProfile.class)
@Disabled("manual run")
public class BatchBigqueryChangeConsumerTest extends BaseBigqueryTest {

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