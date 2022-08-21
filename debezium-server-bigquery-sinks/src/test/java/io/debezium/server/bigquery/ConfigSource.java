/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import io.debezium.server.TestConfigSource;

public class ConfigSource extends TestConfigSource {

  public ConfigSource() {
    config.put("debezium.source.include.schema.changes", "false");
    config.put("debezium.source.decimal.handling.mode", "double");
    config.put("debezium.source.max.batch.size", "100");
    config.put("debezium.source.poll.interval.ms", "5000");
    config.put("debezium.source.database.server.name", "testc");
    //
    config.put("debezium.source.offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore");
    config.put("debezium.source.offset.flush.interval.ms", "60000");
    config.put("debezium.source.database.history.kafka.bootstrap.servers", "kafka:9092");
    config.put("debezium.source.database.history.kafka.topic", "dbhistory.fullfillment");
    config.put("debezium.source.database.history", "io.debezium.relational.history.MemoryDatabaseHistory");
    //
    config.put("debezium.source.snapshot.select.statement.overrides.inventory.products_on_hand", "SELECT * FROM products_on_hand WHERE 1>2");
    // enable disable schema
    config.put("debezium.format.value.schemas.enable", "true");
    // batch
    config.put("debezium.sink.bigquerybatch.project", "test");
    config.put("debezium.sink.bigquerybatch.dataset", "stage");
    config.put("debezium.sink.bigquerybatch.location", "EU");
    // stream
    config.put("debezium.sink.bigquerystream.project", "test");
    config.put("debezium.sink.bigquerystream.dataset", "stage");
    config.put("debezium.sink.bigquerystream.location", "EU");
    //config.put("debezium.sink.bigquerybatch.credentialsFile", "/path/to/application_credentials.json");
    config.put("debezium.sink.batch.objectkey-prefix", "debezium-cdc-");
    config.put("debezium.sink.batch.objectkey-partition", "true");

    // debezium unwrap message
    config.put("debezium.transforms", "unwrap");
    config.put("debezium.transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
    config.put("debezium.transforms.unwrap.drop.tombstones", "true");
    config.put("debezium.transforms.unwrap.delete.handling.mode", "rewrite");

    // logging levels
    config.put("quarkus.log.level", "INFO");
    config.put("quarkus.log.category.\"io.debezium.server.bigquery\".level", "DEBUG");
    config.put("quarkus.log.category.\"com.google.cloud.bigquery\".level", "INFO");
  }

  @Override
  public int getOrdinal() {
    // Configuration property precedence is based on ordinal values and since we override the
    // properties in TestConfigSource, we should give this a higher priority.
    return super.getOrdinal() + 1;
  }
}
