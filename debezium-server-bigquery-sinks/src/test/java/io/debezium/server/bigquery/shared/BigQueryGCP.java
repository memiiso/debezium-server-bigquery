/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery.shared;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BigQueryGCP implements QuarkusTestResourceLifecycleManager {
  public static final Logger LOGGER = LoggerFactory.getLogger(BigQueryGCP.class);
  public static final String BQ_PROJECT = System.getenv().getOrDefault("GCP_BQ_TEST_PROJECT_ID", "eco-serenity-440218-q8");
  public static final String BQ_DATASET = System.getenv().getOrDefault("GCP_BQ_TEST_DATASET", "testdataset");
  public static String BQ_LOCATION = "EU";
  public static String BQ_CRED_FILE = "";
  private static BigQuery bqClient;

  public static BigQuery bigQueryClient() {
    BigQueryOptions options = BigQueryOptions
        .newBuilder()
        .setProjectId(BQ_PROJECT)
        .build();
    return options.getService();
  }

  @Override
  public void stop() {
    return;
  }

  @Override
  public Map<String, String> start() {
    bqClient = bigQueryClient();
    Map<String, String> config = new ConcurrentHashMap<>();
    // batch
    // src/test/resources/application.properties
    config.put("debezium.sink.bigquerybatch.project", BQ_PROJECT);
    config.put("debezium.sink.bigquerybatch.dataset", BQ_DATASET);
    config.put("debezium.sink.bigquerybatch.location", BQ_LOCATION);
    config.put("debezium.sink.bigquerybatch.credentials-file", BQ_CRED_FILE);
// stream
    config.put("debezium.sink.bigquerystream.project", BQ_PROJECT);
    config.put("debezium.sink.bigquerystream.dataset", BQ_DATASET);
    config.put("debezium.sink.bigquerystream.location", BQ_LOCATION);
    config.put("debezium.sink.bigquerystream.credentials-file", BQ_CRED_FILE);
    return config;
  }
}
