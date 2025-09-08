/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.Schema;
import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.serde.DebeziumSerdes;
import io.debezium.server.bigquery.batchsizewait.BatchSizeWait;
import io.debezium.util.Clock;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Abstract base class for Debezium change consumers that deliver messages to a BigQuery destination.
 * <p>This class provides a foundation for building Debezium change consumers that handle
 * incoming change events and deliver them to a BigQuery destination. It implements the
 * `DebeziumEngine.ChangeConsumer` interface, defining the `handleBatch` method for processing
 * batches of change events. Concrete implementations of this class need to provide specific logic
 * for uploading or persisting the converted data to the BigQuery destination.
 *
 * @author Ismail Simsek
 */
public abstract class BaseChangeConsumer extends io.debezium.server.BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

  protected static final Duration LOG_INTERVAL = Duration.ofMinutes(15);
  protected static final ConcurrentHashMap<String, Object> uploadLock = new ConcurrentHashMap<>();
  protected static final Serde<JsonNode> valSerde = DebeziumSerdes.payloadJson(JsonNode.class);
  protected static final Serde<JsonNode> keySerde = DebeziumSerdes.payloadJson(JsonNode.class);
  protected static final ObjectMapper mapper = new ObjectMapper();
  static Deserializer<JsonNode> keyDeserializer;
  protected final Logger LOGGER = LoggerFactory.getLogger(getClass());
  protected final Clock clock = Clock.system();
  protected Deserializer<JsonNode> valDeserializer;
  protected long consumerStart = clock.currentTimeInMillis();
  protected long numConsumedEvents = 0;
  protected Threads.Timer logTimer = Threads.timer(clock, LOG_INTERVAL);
  @Inject
  @Any
  Instance<BatchSizeWait> batchSizeWaitInstances;
  BatchSizeWait batchSizeWait;
  @Inject
  CommonConfig commonConfig;
  @Inject
  DebeziumConfig debeziumConfig;
  ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
  private int numConcurrentUploads;
  private int concurrentUploadsTimeoutMinutes;
  private Semaphore concurrencyLimiter;

  public void initialize() throws InterruptedException {
    // configure and set 
    valSerde.configure(Collections.emptyMap(), false);
    valDeserializer = valSerde.deserializer();
    // configure and set 
    keySerde.configure(Collections.emptyMap(), true);
    keyDeserializer = keySerde.deserializer();

    if (!debeziumConfig.valueFormat().equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new InterruptedException("debezium.format.value={" + debeziumConfig.valueFormat() + "} not supported! Supported (debezium.format.value=*) formats are {json,}!");
    }

    if (!debeziumConfig.keyFormat().equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new InterruptedException("debezium.format.key={" + debeziumConfig.valueFormat() + "} not supported! Supported (debezium.format.key=*) formats are {json,}!");
    }

    batchSizeWait = ConsumerUtil.selectInstance(batchSizeWaitInstances, commonConfig.batchSizeWaitName());
    LOGGER.info("Using {} to optimize batch size", batchSizeWait.getClass().getSimpleName());
    batchSizeWait.initizalize();

    this.numConcurrentUploads = commonConfig.concurrentUploads();
    this.concurrentUploadsTimeoutMinutes = commonConfig.concurrentUploadsTimeoutMinutes();

    // Initialize the semaphore for parallel processing
    if (numConcurrentUploads > 1) {
      this.concurrencyLimiter = new Semaphore(numConcurrentUploads);
      LOGGER.info("Parallel uploads enabled with concurrency limit: {}", numConcurrentUploads);
    }
  }

  @PreDestroy
  void close() {
    shutdownExecutors();
  }

  protected void shutdownExecutors() {
    try {
      LOGGER.info("Shutting down executor service.");
      executor.shutdown();
      if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
        LOGGER.warn("Executor service did not terminate in 30 seconds. Forcing shutdown.");
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      LOGGER.warn("Interrupted while shutting down executor service.");
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
      throws InterruptedException {
    LOGGER.trace("Received {} events", records.size());

    Instant start = Instant.now();
    Map<String, List<ChangeEvent<Object, Object>>> events = records.stream()
        .collect(Collectors.groupingBy(ChangeEvent<Object, Object>::destination));

    // consume list of events for each destination table
    if (numConcurrentUploads > 1) {
      this.processTablesInParallel(events);
    } else {
      this.processTablesSequentially(events);
    }

    // workaround! somehow offset is not saved to file unless we call committer.markProcessed
    // even it's should be saved to file periodically
    for (ChangeEvent<Object, Object> record : records) {
      LOGGER.trace("Processed event '{}'", record);
      committer.markProcessed(record);
    }
    committer.markBatchFinished();
    long numUploadedEvents = records.size();
    this.logConsumerProgress(numUploadedEvents);
    LOGGER.debug("Received:{} Processed:{} events", records.size(), numUploadedEvents);

    batchSizeWait.waitMs(numUploadedEvents, (int) Duration.between(start, Instant.now()).toMillis());

  }

  private void processTablesSequentially(Map<String, List<ChangeEvent<Object, Object>>> events) {
    for (Map.Entry<String, List<ChangeEvent<Object, Object>>> destinationEvents : events.entrySet()) {
      if (destinationEvents.getKey().startsWith(debeziumConfig.topicHeartbeatPrefix()) && debeziumConfig.topicHeartbeatSkipConsuming()) {
        continue;
      }
      this.addToTable(destinationEvents);
    }
  }

  private void addToTable(Map.Entry<String, List<ChangeEvent<Object, Object>>> destinationEvents) {
    // group list of events by their schema, if in the batch we have schema change events grouped by their schema
    // so with this uniform schema is guaranteed for each batch
    Map<JsonNode, List<RecordConverter>> eventsGroupedBySchema =
        destinationEvents.getValue().stream()
            .map((ChangeEvent<Object, Object> e) -> {
              try {
                return this.eventAsRecordConverter(e);
              } catch (IOException ex) {
                throw new DebeziumException(ex);
              }
            })
            .collect(Collectors.groupingBy(RecordConverter::valueSchema));
    LOGGER.debug("Destination {} got {} records with {} different schema!!", destinationEvents.getKey(),
        destinationEvents.getValue().size(),
        eventsGroupedBySchema.keySet().size());

    for (List<RecordConverter> schemaEvents : eventsGroupedBySchema.values()) {
      this.uploadDestination(destinationEvents.getKey(), schemaEvents);
    }
  }

  private void processTablesInParallel(Map<String, List<ChangeEvent<Object, Object>>> events) {
    List<Callable<Void>> tasks = new ArrayList<>();
    for (Map.Entry<String, List<ChangeEvent<Object, Object>>> destinationEvents : events.entrySet()) {
      if (destinationEvents.getKey().startsWith(debeziumConfig.topicHeartbeatPrefix()) && debeziumConfig.topicHeartbeatSkipConsuming()) {
        continue;
      }

      tasks.add(() -> {
        try {
          // Acquire a permit from the Semaphore to enforce the concurrency limit.
          LOGGER.trace("Task for destination '{}' waiting for permit. Available: {}", destinationEvents.getKey(), concurrencyLimiter.availablePermits());
          concurrencyLimiter.acquire();
          LOGGER.debug("Task for destination '{}' acquired permit. Starting processing.", destinationEvents.getKey());

          this.addToTable(destinationEvents);
          return null; // Callable must return a value
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt(); // Restore interrupted status
          LOGGER.warn("Task for destination '{}' was interrupted.", destinationEvents.getKey(), e);
          return null;
        } catch (Exception e) {
          // Log and rethrow. This will be wrapped in an ExecutionException by the Future.
          LOGGER.error("Task for destination '{}' failed.", destinationEvents.getKey(), e);
          throw e;
        } finally {
          // Always release the permit, even if an exception occurred.
          concurrencyLimiter.release();
          LOGGER.trace("Task for destination '{}' released permit. Available: {}", destinationEvents.getKey(), concurrencyLimiter.availablePermits());
        }
      });
    }
    // process
    LOGGER.debug("Invoking {} parallel tasks and waiting for completion...", tasks.size());
    try {
      // Invoke all tasks and wait for them to complete, with a timeout.
      List<Future<Void>> futures = executor.invokeAll(tasks, concurrentUploadsTimeoutMinutes, TimeUnit.MINUTES);

      // Check the status of each task to log any exceptions
      for (Future<Void> future : futures) {
        try {
          // future.get() will throw an exception if the task failed or was cancelled.
          future.get();
        } catch (CancellationException e) {
          LOGGER.error("A task was cancelled, likely due to timeout.", e);
        } catch (ExecutionException e) {
          // The original exception from the Callable is wrapped in ExecutionException
          LOGGER.error("A task failed with an exception: {}", e.getCause().getMessage(), e.getCause());
        }
      }
      LOGGER.debug("All parallel tasks have been processed.");

    } catch (InterruptedException e) {
      LOGGER.warn("Main thread interrupted while waiting for tasks to complete.", e);
      Thread.currentThread().interrupt();
    }
  }

  protected void logConsumerProgress(long numUploadedEvents) {
    numConsumedEvents += numUploadedEvents;
    if (logTimer.expired()) {
      LOGGER.info("Consumed {} records after {}", numConsumedEvents, Strings.duration(clock.currentTimeInMillis() - consumerStart));
      numConsumedEvents = 0;
      consumerStart = clock.currentTimeInMillis();
      logTimer = Threads.timer(clock, LOG_INTERVAL);
    }
  }

  protected static boolean schemaContainsField(Schema schema, String field) {
    try {
      if (schema.getFields() != null && schema.getFields().get(field) != null) {
        return true;
      }
    } catch (Exception e) {
      return false;
    }

    return false;
  }

  public abstract long uploadDestination(String destination, List<RecordConverter> data);

  public abstract RecordConverter eventAsRecordConverter(ChangeEvent<Object, Object> e) throws IOException;
}
