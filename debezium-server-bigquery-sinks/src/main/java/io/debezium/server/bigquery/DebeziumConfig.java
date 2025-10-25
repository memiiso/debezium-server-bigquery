package io.debezium.server.bigquery;

import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import io.smallrye.config.WithParentName;

@ConfigRoot
@ConfigMapping
public interface DebeziumConfig {

  @WithParentName
  CommonConfig common();

  @WithName("debezium.format.value")
  @WithDefault("json")
  String valueFormat();

  @WithName("debezium.format.key")
  @WithDefault("json")
  String keyFormat();

  @WithName(value = "debezium.source.time.precision.mode")
  @WithDefault(value = "isostring")
  TemporalPrecisionMode temporalPrecisionMode();

  @WithName("debezium.source.decimal.handling.mode")
  @WithDefault("double")
  RelationalDatabaseConnectorConfig.DecimalHandlingMode decimalHandlingMode();

  @WithName("debezium.format.value.schemas.enable")
  @WithDefault("true")
  boolean eventSchemaEnabled();

  @WithName("debezium.format.key.schemas.enable")
  @WithDefault("true")
  boolean eventKeySchemaEnabled();

  // SET RECOMMENDED DEFAULT VALUES FOR DEBEZIUM CONFIGS
  //# Save debezium offset state to destination, bigquery table
  @WithName("debezium.source.offset.storage")
  @WithDefault("io.debezium.server.bigquery.offset.BigqueryOffsetBackingStore")
  String offsetStorage();

  @WithName("debezium.source.offset.storage.bigquery.table-name")
  @WithDefault("_debezium_offset_storage")
  String offsetStorageTable();

  // Save schema history to iceberg table
  @WithName("debezium.source.schema.history.internal")
  @WithDefault("io.debezium.server.bigquery.history.BigquerySchemaHistory")
  String schemaHistoryStorage();

  @WithName("debezium.source.schema.history.internal.bigquery.table-name")
  @WithDefault("_debezium_database_history_storage")
  String schemaHistoryStorageTable();

  //  Event flattening. unwrap message!
  @WithName("debezium.transforms")
  @WithDefault("unwrap")
  String transforms();

  @WithName("debezium.transforms.unwrap.type")
  @WithDefault("io.debezium.transforms.ExtractNewRecordState")
  String unwrapType();

  @WithName("debezium.transforms.unwrap.add.fields")
  @WithDefault("op,table,source.ts_ms,db,ts_ms,ts_ns,source.ts_ns")
  String unwrapAddFields();

  @WithName("debezium.transforms.unwrap.delete.tombstone.handling.mode")
  @WithDefault("rewrite")
  String unwrapDeleteHandlingMode();

  @WithName("debezium.transforms.unwrap.drop.tombstones")
  @WithDefault("true")
  String unwrapDeleteTombstoneHandlingMode();

  @WithName("debezium.source.topic.heartbeat.prefix")
  @WithDefault("__debezium-heartbeat")
  String topicHeartbeatPrefix();

  @WithName("debezium.source.topic.heartbeat.skip-consuming")
  @WithDefault("ture")
  boolean topicHeartbeatSkipConsuming();

  @WithName("debezium.source.include.schema.changes")
  @WithDefault("false")
  boolean includeSchemaChanges();

  default boolean isIsoStringTemporalMode() {
    return temporalPrecisionMode() == TemporalPrecisionMode.ISOSTRING;
  }

  default boolean isAdaptiveTemporalMode() {
    return temporalPrecisionMode() == TemporalPrecisionMode.ADAPTIVE ||
        temporalPrecisionMode() == TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS;
  }
}