package io.debezium.server.bigquery.batchsizewait;

import io.debezium.config.CommonConnectorConfig;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

import static io.debezium.config.CommonConnectorConfig.DEFAULT_MAX_BATCH_SIZE;

@ConfigRoot
@ConfigMapping
public interface BatchSizeWaitConfig {

  @WithName("debezium.source.max.batch.size")
  @WithDefault(DEFAULT_MAX_BATCH_SIZE + "")
  Integer maxBatchSize();

  @WithName("debezium.sink.batch.batch-size-wait.max-wait-ms")
  @WithDefault("300000")
  Integer maxWaitMs();

  @WithName("debezium.source.max.batch.size")
  @WithDefault(CommonConnectorConfig.DEFAULT_MAX_BATCH_SIZE + "")
  int maxBatchSizeInt();

  @WithName("debezium.sink.batch.batch-size-wait.max-wait-ms")
  @WithDefault("300000")
  int maxWaitMsInt();

  @WithName("debezium.sink.batch.batch-size-wait.wait-interval-ms")
  @WithDefault("10000")
  int waitIntervalMs();
}