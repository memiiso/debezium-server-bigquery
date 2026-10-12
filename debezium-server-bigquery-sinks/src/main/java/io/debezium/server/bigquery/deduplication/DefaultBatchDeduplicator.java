/*
 * Copyright memiiso Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.bigquery.deduplication;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.debezium.DebeziumException;
import io.debezium.server.bigquery.RecordConverter;
import io.debezium.server.bigquery.StreamConsumerConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default deduplication strategy comparing timestamp and operation code.
 * When no timestamp column is specified, it preserves incoming event order (retaining the last event).
 *
 * @author Ismail Simsek
 */
public class DefaultBatchDeduplicator implements BatchDeduplicator {

  private static final ImmutableMap<String, Integer> CDC_OPERATIONS =
      ImmutableMap.of("c", 1, "r", 2, "u", 3, "d", 4);

  private final Optional<String> sourceTsColumn;
  private final String opColumn;

  public DefaultBatchDeduplicator(StreamConsumerConfig config) {
    this(config != null ? config.sourceTsColumn() : Optional.empty(),
        config != null ? config.opColumn() : "__op");
  }

  public DefaultBatchDeduplicator(Optional<String> sourceTsColumn, String opColumn) {
    this.sourceTsColumn = sourceTsColumn != null ? sourceTsColumn : Optional.empty();
    this.opColumn = (opColumn != null && !opColumn.isBlank()) ? opColumn : "__op";
  }

  public DefaultBatchDeduplicator() {
    this(Optional.empty(), "__op");
  }

  @Override
  public List<RecordConverter> deduplicate(List<RecordConverter> events) {
    if (events == null || events.isEmpty()) {
      return Collections.emptyList();
    }

    Map<JsonNode, RecordConverter> deduplicatedEvents = new ConcurrentHashMap<>();

    events.forEach(e -> {
      if (e.key() == null || e.key().isNull()) {
        throw new DebeziumException("Cannot deduplicate data with null key! destination:'"
            + e.destination() + "' event: '" + (e.value() != null ? e.value().toString() : "null") + "'");
      }
      deduplicatedEvents.merge(e.key(), e, (oldValue, newValue) -> {
        int comparison = compareByTsThenOp(oldValue.value(), newValue.value());
        if (comparison <= 0) {
          return newValue;
        } else {
          return oldValue;
        }
      });
    });

    return new ArrayList<>(deduplicatedEvents.values());
  }

  private int compareByTsThenOp(JsonNode lhs, JsonNode rhs) {
    if (sourceTsColumn.isEmpty() || sourceTsColumn.get().isBlank()) {
      return -1;
    }

    String tsCol = sourceTsColumn.get();
    long lhsTs = lhs != null && lhs.hasNonNull(tsCol) ? lhs.get(tsCol).asLong(0) : 0;
    long rhsTs = rhs != null && rhs.hasNonNull(tsCol) ? rhs.get(tsCol).asLong(0) : 0;

    int result = Long.compare(lhsTs, rhsTs);

    if (result == 0) {
      String lhsOp = lhs != null && lhs.hasNonNull(opColumn) ? lhs.get(opColumn).asText("c") : "c";
      String rhsOp = rhs != null && rhs.hasNonNull(opColumn) ? rhs.get(opColumn).asText("c") : "c";

      result = CDC_OPERATIONS.getOrDefault(lhsOp, -1)
          .compareTo(
              CDC_OPERATIONS.getOrDefault(rhsOp, -1)
          );
    }

    return result;
  }
}
