/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import org.eclipse.microprofile.config.spi.ConfigSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestConfigSource implements ConfigSource {
  public static String OFFSET_TABLE = "__debezium_offset_storage_test_table";
  public static String HISTORY_TABLE = "__debezium_database_history_storage_test_table";
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
    config.put("debezium.source.offset.storage.bigquery.table-name", OFFSET_TABLE);
    config.put("debezium.source.offset.flush.interval.ms", "60000");
    config.put("debezium.source.schema.history.internal", "io.debezium.server.bigquery.history.BigquerySchemaHistory");
    config.put("debezium.source.schema.history.internal.bigquery.table-name", HISTORY_TABLE);
    //config.put("debezium.source.schema.history.internal", "io.debezium.relational.history.MemoryDatabaseHistory");
    //
    config.put("debezium.source.table.include.list", "inventory.*");
    config.put("debezium.source.snapshot.select.statement.overrides.inventory.products_on_hand", "SELECT * FROM products_on_hand WHERE 1>2");
    // enable disable schema
    config.put("debezium.format.value.schemas.enable", "true");
    config.put("debezium.sink.batch.objectkey-prefix", "debezium-cdc-");
    config.put("debezium.sink.batch.objectkey-partition", "true");

    // debezium unwrap message
    config.put("debezium.transforms", "unwrap");
    config.put("debezium.transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
    config.put("debezium.transforms.unwrap.delete.handling.mode", "rewrite");
    config.put("debezium.transforms.unwrap.drop.tombstones", "true");

    // logging levels

    config.put("quarkus.devservices.enabled", "false");
    config.put("quarkus.log.level", "WARN");
    config.put("quarkus.log.category.\"io.debezium.server.bigquery\".level", "INFO");
    config.put("quarkus.log.category.\"io.debezium.server.bigquery.StreamBigqueryChangeConsumer\".level", "DEBUG");
    config.put("quarkus.log.category.\"io.debezium.server.bigquery.BatchBigqueryChangeConsumer\".level", "DEBUG");
    config.put("quarkus.log.category.\"io.debezium.server.bigquery.StreamDataWriter\".level", "DEBUG");
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
