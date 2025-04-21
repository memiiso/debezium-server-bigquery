package io.debezium.server.bigquery;

import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import io.smallrye.config.WithParentName;

import java.util.Optional;

@ConfigRoot
@ConfigMapping
public interface StreamConsumerConfig {
  @WithParentName
  CommonConfig common();

  @WithParentName
  DebeziumConfig debezium();

  @WithName("debezium.sink.bigquerystream.dataset")
  Optional<String> bqDataset();

  @WithName("debezium.sink.bigquerystream.project")
  Optional<String> gcpProject();

  @WithName("debezium.sink.bigquerystream.location")
  @WithDefault("US")
  String bqLocation();

  @WithName("debezium.sink.bigquerystream.ignore-unknown-fields")
  @WithDefault("true")
  Boolean ignoreUnknownFields();

  @WithName("debezium.sink.bigquerystream.create-if-needed")
  @WithDefault("true")
  Boolean createIfNeeded();

  @WithName("debezium.sink.bigquerystream.partition-field")
  @WithDefault("__ts_ms")
  String partitionField();

  @WithName("debezium.sink.bigquerystream.clustering-field")
  @WithDefault("__source_ts_ms")
  String clusteringField();

  @WithName("debezium.sink.bigquerystream.partition-type")
  @WithDefault("MONTH")
  String partitionType();

  @WithName("debezium.sink.bigquerystream.allow-field-addition")
  @WithDefault("false")
  Boolean allowFieldAddition();

  @WithName("debezium.sink.bigquerystream.credentials-file")
  Optional<String> credentialsFile();

  @WithName("debezium.sink.bigquerystream.bigquery-custom-host")
  Optional<String> bigQueryCustomHost();

  @WithName("debezium.sink.bigquerystream.bigquery-custom-grpc-host")
  Optional<String> bigQueryCustomGRPCHost();

  @WithName("debezium.sink.bigquerystream.bigquery-dev-emulator")
  @WithDefault("false")
  Boolean isBigqueryDevEmulator();

  @WithName("debezium.sink.bigquerystream.upsert")
  @WithDefault("false")
  boolean upsert();

  @WithName("debezium.sink.bigquerystream.upsert-keep-deletes")
  @WithDefault("true")
  boolean upsertKeepDeletes();

  @WithName("debezium.sink.bigquerystream.upsert-dedup-column")
  @WithDefault("__source_ts_ns")
  String sourceTsColumn();

  @WithName("debezium.sink.bigquerystream.upsert-op-column")
  @WithDefault("__op")
  String opColumn();

}