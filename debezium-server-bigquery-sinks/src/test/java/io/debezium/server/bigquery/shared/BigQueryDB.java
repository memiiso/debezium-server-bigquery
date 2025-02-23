/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery.shared;

import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BigQueryEmulatorContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BigQueryDB implements QuarkusTestResourceLifecycleManager {
  public static final Logger LOGGER = LoggerFactory.getLogger(BigQueryDB.class);
  public static final String BQ_PROJECT = "test-project";
  public static final String BQ_DATASET = "testdataset";
  public static final BigQueryEmulatorContainer container = new BigQueryEmulatorContainer("ghcr.io/goccy/bigquery-emulator:0.6")
      .withCommand(
          "--log-level=debug",
//          "--location=" + BQ_LOCATION,
          "--project=" + BQ_PROJECT,
          "--dataset=" + BQ_DATASET
      )
      .waitingFor(Wait.forLogMessage(".*listening.*0.0.0.0:9060.*", 1))
      .waitingFor(Wait.forLogMessage(".*listening.*0.0.0.0:9050.*", 1));
  public static String BQ_LOCATION = "EU";
  // "/path/to/application_credentials.json"
  public static String BQ_CRED_FILE = "bigquery-testing-emulator.json";
  public static BigQuery bqClient;

  public static BigQuery bigQueryClient() {
    String url = container.getEmulatorHttpEndpoint();
    BigQueryOptions options = BigQueryOptions
        .newBuilder()
        .setProjectId(container.getProjectId())
        .setHost(url)
        .setLocation(url)
        .setCredentials(NoCredentials.getInstance())
        .build();
    return options.getService();
  }

  @Override
  public void stop() {
    container.stop();
  }

  @Override
  public Map<String, String> start() {
    container.start();
    bqClient = bigQueryClient();
    LOGGER.warn("BIGQUERY EMULATOR HOST: " + bqClient.getOptions().getHost());
    Map<String, String> config = new ConcurrentHashMap<>();
    // batch
    // src/test/resources/application.properties
    config.put("debezium.sink.bigquerybatch.project", BQ_PROJECT);
    config.put("debezium.sink.bigquerybatch.dataset", BQ_DATASET);
    config.put("debezium.sink.bigquerybatch.location", BQ_LOCATION);
    config.put("debezium.sink.bigquerybatch.credentials-file", BQ_CRED_FILE);
    config.put("debezium.sink.bigquerybatch.bigquery-custom-host", container.getEmulatorHttpEndpoint());
    config.put("debezium.sink.bigquerybatch.bigquery-dev-emulator", "true");
    config.put("debezium.sink.bigquerybatch.bigquery-custom-grpc-host", String.format("%s:%d", container.getHost(), container.getMappedPort(9060)));
    // stream
    config.put("debezium.sink.bigquerystream.project", BQ_PROJECT);
    config.put("debezium.sink.bigquerystream.dataset", BQ_DATASET);
    config.put("debezium.sink.bigquerystream.location", BQ_LOCATION);
    config.put("debezium.sink.bigquerystream.credentials-file", BQ_CRED_FILE);
    config.put("debezium.sink.bigquerystream.bigquery-dev-emulator", "true");
    config.put("debezium.sink.bigquerystream.bigquery-custom-host", container.getEmulatorHttpEndpoint());
    config.put("debezium.sink.bigquerystream.bigquery-custom-grpc-host", String.format("%s:%d", container.getHost(), container.getMappedPort(9060)));
    return config;
  }
}
