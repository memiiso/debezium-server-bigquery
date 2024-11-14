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
import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.serde.DebeziumSerdes;
import io.debezium.server.bigquery.batchsizewait.BatchSizeWait;
import io.debezium.util.Clock;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
  @ConfigProperty(name = "debezium.format.value", defaultValue = "json")
  String valueFormat;
  @ConfigProperty(name = "debezium.format.key", defaultValue = "json")
  String keyFormat;
  @ConfigProperty(name = "debezium.sink.batch.batch-size-wait", defaultValue = "NoBatchSizeWait")
  String batchSizeWaitName;
  @Inject
  @Any
  Instance<BatchSizeWait> batchSizeWaitInstances;
  BatchSizeWait batchSizeWait;

  public void initialize() throws InterruptedException {
    // configure and set 
    valSerde.configure(Collections.emptyMap(), false);
    valDeserializer = valSerde.deserializer();
    // configure and set 
    keySerde.configure(Collections.emptyMap(), true);
    keyDeserializer = keySerde.deserializer();

    if (!valueFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new InterruptedException("debezium.format.value={" + valueFormat + "} not supported! Supported (debezium.format.value=*) formats are {json,}!");
    }

    if (!keyFormat.equalsIgnoreCase(Json.class.getSimpleName().toLowerCase())) {
      throw new InterruptedException("debezium.format.key={" + valueFormat + "} not supported! Supported (debezium.format.key=*) formats are {json,}!");
    }

    batchSizeWait = ConsumerUtil.selectInstance(batchSizeWaitInstances, batchSizeWaitName);
    LOGGER.info("Using {} to optimize batch size", batchSizeWait.getClass().getSimpleName());
    batchSizeWait.initizalize();
  }

  @Override
  public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
      throws InterruptedException {
    LOGGER.trace("Received {} events", records.size());

    Instant start = Instant.now();
    Map<String, List<RecordConverter>> events = records.stream()
        .map((ChangeEvent<Object, Object> e)
            -> {
          try {
            return this.eventAsRecordConverter(e);
          } catch (IOException ex) {
            throw new DebeziumException(ex);
          }
        })
        .collect(Collectors.groupingBy(RecordConverter::destination));

    long numUploadedEvents = 0;
    for (Map.Entry<String, List<RecordConverter>> destinationEvents : events.entrySet()) {
      // group list of events by their schema, if in the batch we have schema change events grouped by their schema
      // so with this uniform schema is guaranteed for each batch
      Map<JsonNode, List<RecordConverter>> eventsGroupedBySchema =
          destinationEvents.getValue().stream()
              .collect(Collectors.groupingBy(RecordConverter::valueSchema));
      LOGGER.debug("Destination {} got {} records with {} different schema!!", destinationEvents.getKey(),
          destinationEvents.getValue().size(),
          eventsGroupedBySchema.keySet().size());

      for (List<RecordConverter> schemaEvents : eventsGroupedBySchema.values()) {
        numUploadedEvents += this.uploadDestination(destinationEvents.getKey(), schemaEvents);
      }
    }
    // workaround! somehow offset is not saved to file unless we call committer.markProcessed
    // even it's should be saved to file periodically
    for (ChangeEvent<Object, Object> record : records) {
      LOGGER.trace("Processed event '{}'", record);
      committer.markProcessed(record);
    }
    committer.markBatchFinished();
    this.logConsumerProgress(numUploadedEvents);
    LOGGER.debug("Received:{} Processed:{} events", records.size(), numUploadedEvents);

    batchSizeWait.waitMs(numUploadedEvents, (int) Duration.between(start, Instant.now()).toMillis());

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

  public abstract long uploadDestination(String destination, List<RecordConverter> data);

  public abstract RecordConverter eventAsRecordConverter(ChangeEvent<Object, Object> e) throws IOException;
}
