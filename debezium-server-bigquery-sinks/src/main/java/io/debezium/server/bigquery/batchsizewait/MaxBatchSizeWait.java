/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery.batchsizewait;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.server.DebeziumMetrics;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Optimizes batch size around 85%-90% of max,batch.size using dynamically calculated sleep(ms)
 *
 * @author Ismail Simsek
 */
@Dependent
@Named("MaxBatchSizeWait")
public class MaxBatchSizeWait implements InterfaceBatchSizeWait {
  protected static final Logger LOGGER = LoggerFactory.getLogger(MaxBatchSizeWait.class);
  @ConfigProperty(name = "debezium.source.max.batch.size", defaultValue = CommonConnectorConfig.DEFAULT_MAX_BATCH_SIZE + "")
  int maxBatchSize;
  @ConfigProperty(name = "debezium.sink.batch.batch-size-wait.max-wait-ms", defaultValue = "300000")
  int maxWaitMs;
  @ConfigProperty(name = "debezium.sink.batch.batch-size-wait.wait-interval-ms", defaultValue = "10000")
  int waitIntervalMs;
  @Inject
  DebeziumMetrics debeziumMetrics;

  @Override
  public void initizalize() throws DebeziumException {
    assert waitIntervalMs < maxWaitMs : "`wait-interval-ms` cannot be bigger than `max-wait-ms`";
  }

  @Override
  public void waitMs(long numRecordsProcessed, Integer processingTimeMs) throws InterruptedException {

    if (debeziumMetrics.snapshotRunning()) {
      return;
    }

    LOGGER.info("Processed {}, " +
            "QueueCurrentSize:{}, " +
            "QueueTotalCapacity:{}, " +
            "QueueCurrentUtilization:{}%, " +
            "MilliSecondsBehindSource:{}, " +
            "SnapshotCompleted:{}, " +
            "snapshotRunning:{}",
        numRecordsProcessed,
        debeziumMetrics.streamingQueueCurrentSize(), debeziumMetrics.maxQueueSize(),
        (debeziumMetrics.streamingQueueCurrentSize() / debeziumMetrics.maxQueueSize()) * 100,
        debeziumMetrics.streamingMilliSecondsBehindSource(),
        debeziumMetrics.snapshotCompleted(), debeziumMetrics.snapshotRunning()
    );

    int totalWaitMs = 0;
    while (totalWaitMs < maxWaitMs && debeziumMetrics.streamingQueueCurrentSize() < maxBatchSize) {
      totalWaitMs += waitIntervalMs;
      LOGGER.trace("Sleeping {} Milliseconds, QueueCurrentSize:{} < maxBatchSize:{}, Total wait {}",
          waitIntervalMs, debeziumMetrics.streamingQueueCurrentSize(), maxBatchSize, totalWaitMs);

      Thread.sleep(waitIntervalMs);
    }

    LOGGER.debug("Total wait {} Milliseconds, QueueCurrentSize:{}, maxBatchSize:{}",
        totalWaitMs, debeziumMetrics.streamingQueueCurrentSize(), maxBatchSize);

  }

}
