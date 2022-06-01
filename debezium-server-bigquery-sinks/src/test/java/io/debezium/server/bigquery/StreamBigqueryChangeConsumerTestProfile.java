/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.HashMap;
import java.util.Map;

public class StreamBigqueryChangeConsumerTestProfile implements QuarkusTestProfile {

  //This method allows us to override configuration properties.
  @Override
  public Map<String, String> getConfigOverrides() {
    Map<String, String> config = new HashMap<>();

    config.put("debezium.sink.type", "bigquerystream");
    config.put("debezium.sink.bigquerystream.allowFieldAddition", "true");
    config.put("debezium.source.table.include.list", "inventory.test_date_table,inventory.customers,inventory.geom");
    //
    config.put("debezium.sink.bigquerystream.project", "test");
    config.put("debezium.sink.bigquerystream.dataset", "test");
    config.put("debezium.sink.bigquerystream.location", "EU");
    // logging
    config.put("quarkus.log.category.\"io.debezium.server.bigquery\".level", "INFO");
    config.put("quarkus.log.category.\"io.debezium.server.bigquery.bigquery.StreamBigqueryChangeConsumer\".level",
        "DEBUG");
    config.put("quarkus.log.category.\"com.google.cloud.bigquery\".level", "INFO");
    return config;
  }
}
