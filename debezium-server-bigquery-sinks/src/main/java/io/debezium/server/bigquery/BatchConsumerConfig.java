package io.debezium.server.bigquery;

import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import io.smallrye.config.WithParentName;

import java.util.Optional;

@ConfigRoot
@ConfigMapping
public interface BatchConsumerConfig {
  @WithParentName
  CommonConfig common();

  @WithParentName
  DebeziumConfig debezium();

  @WithName("debezium.sink.bigquerybatch.dataset")
//  @WithDefault("")
  Optional<String> bqDataset();

  @WithName("debezium.sink.bigquerybatch.location")
  @WithDefault("US")
  String bqLocation();

  @WithName("debezium.sink.bigquerybatch.project")
//  @WithDefault("")
  Optional<String> gcpProject();

  @WithName("debezium.sink.bigquerybatch.create-disposition")
  @WithDefault("CREATE_IF_NEEDED")
  String createDisposition();

  @WithName("debezium.sink.bigquerybatch.writeDisposition")
  @WithDefault("WRITE_APPEND")
  String writeDisposition();

  @WithName("debezium.sink.bigquerybatch.partition-field")
  @WithDefault("__ts_ms")
  String partitionField();

  @WithName("debezium.sink.bigquerybatch.clustering-field")
  @WithDefault("__source_ts_ms")
  String clusteringField();

  @WithName("debezium.sink.bigquerybatch.partition-type")
  @WithDefault("MONTH")
  String partitionType();

  @WithName("debezium.sink.bigquerybatch.allow-field-addition")
  @WithDefault("true")
  Boolean allowFieldAddition();

  @WithName("debezium.sink.bigquerybatch.allow-field-relaxation")
  @WithDefault("true")
  Boolean allowFieldRelaxation();

  @WithName("debezium.sink.bigquerybatch.credentials-file")
  @WithDefault("")
  Optional<String> credentialsFile();

  @WithName("debezium.sink.bigquerybatch.bigquery-custom-host")
  @WithDefault("")
  Optional<String> bigQueryCustomHost();

  @WithName("debezium.sink.bigquerybatch.bigquery-dev-emulator")
  @WithDefault("false")
  Boolean isBigqueryDevEmulator();

  @WithName("debezium.sink.bigquerybatch.cast-deleted-field")
  @WithDefault("false")
  Boolean castDeletedField();
}