/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.microprofile.config.spi.ConfigSource;

public class TestConfigSource implements ConfigSource {
  public static String BQ_LOCATION = "EU";
  // overriden by user src/test/resources/application.properties
  public static String BQ_PROJECT = "test";
  public static String BQ_DATASET = "stage";
  public static String BQ_CRED_FILE = ""; // "/path/to/application_credentials.json"
  public static List<String> TABLES = List.of("customers", "geom", "orders", "products", "products_on_hand",
      "test_data_types", "test_table");
  protected Map<String, String> config = new HashMap<>();

  public TestConfigSource() {
    config.put("debezium.sink.type", "bigquerybatch");
    config.put("debezium.source.include.schema.changes", "false");
    config.put("debezium.source.decimal.handling.mode", "double");
    config.put("debezium.source.max.batch.size", "100");
    config.put("debezium.source.poll.interval.ms", "5000");
    //config.put("debezium.source.database.server.name", "testc");
    config.put("debezium.source.database.server.id", "1234");
    config.put("debezium.source.topic.prefix", "testc");
    //
    //config.put("debezium.source.offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore");
    config.put("debezium.source.offset.storage", "io.debezium.server.bigquery.offset.BigqueryOffsetBackingStore");
    config.put("debezium.source.offset.storage.bigquery.table-name", "__debezium_offset_storage_test_table");
    config.put("debezium.source.offset.flush.interval.ms", "60000");
    config.put("debezium.source.schema.history.internal", "io.debezium.server.bigquery.history.BigquerySchemaHistory");
    config.put("debezium.source.schema.history.internal.bigquery.table-name", 
        "__debezium_database_history_storage_test_table");
    //config.put("debezium.source.schema.history.internal", "io.debezium.relational.history.MemoryDatabaseHistory");
    //
    config.put("debezium.source.table.include.list", "inventory.*");
    config.put("debezium.source.snapshot.select.statement.overrides.inventory.products_on_hand", "SELECT * FROM products_on_hand WHERE 1>2");
    // enable disable schema
    config.put("debezium.format.value.schemas.enable", "true");
    // batch
    // src/test/resources/application.properties
    config.put("debezium.sink.bigquerybatch.project", BQ_PROJECT);
    config.put("debezium.sink.bigquerybatch.dataset", BQ_DATASET);
    config.put("debezium.sink.bigquerybatch.location", BQ_LOCATION);
    config.put("debezium.sink.bigquerybatch.credentialsFile", BQ_CRED_FILE);
    // stream
    config.put("debezium.sink.bigquerystream.project", BQ_PROJECT);
    config.put("debezium.sink.bigquerystream.dataset", BQ_DATASET);
    config.put("debezium.sink.bigquerystream.location", BQ_LOCATION);
    config.put("debezium.sink.bigquerystream.credentialsFile", BQ_CRED_FILE);
    config.put("debezium.sink.batch.objectkey-prefix", "debezium-cdc-");
    config.put("debezium.sink.batch.objectkey-partition", "true");

    // debezium unwrap message
    config.put("debezium.transforms", "unwrap");
    config.put("debezium.transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
    config.put("debezium.transforms.unwrap.delete.handling.mode", "rewrite");
    config.put("debezium.transforms.unwrap.drop.tombstones", "true");

    // logging levels
    config.put("quarkus.log.level", "INFO");
    config.put("quarkus.log.category.\"io.debezium.server.bigquery\".level", "DEBUG");
    config.put("quarkus.log.category.\"com.google.cloud.bigquery\".level", "INFO");
  }


  @Override
  public Map<String, String> getProperties() {
    return config;
  }

  @Override
  public String getValue(String propertyName) {
    return config.get(propertyName);
  }

  @Override
  public String getName() {
    return "test";
  }

  @Override
  public Set<String> getPropertyNames() {
    return config.keySet();
  }
}
