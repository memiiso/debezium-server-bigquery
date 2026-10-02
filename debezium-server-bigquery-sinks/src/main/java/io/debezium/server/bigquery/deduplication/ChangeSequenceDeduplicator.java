/*
 * Copyright memiiso Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.bigquery.deduplication;

import com.fasterxml.jackson.databind.JsonNode;
import io.debezium.DebeziumException;
import io.debezium.server.bigquery.RecordConverter;
import io.debezium.server.bigquery.changesequence.ChangeSequenceStrategy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Deduplication strategy that orders duplicate records using ChangeSequenceStrategy CDC coordinates.
 *
 * @author Ismail Simsek
 */
public class ChangeSequenceDeduplicator implements BatchDeduplicator {

  private final ChangeSequenceStrategy strategy;

  public ChangeSequenceDeduplicator(ChangeSequenceStrategy strategy) {
    this.strategy = Objects.requireNonNull(strategy, "ChangeSequenceStrategy must be configured when using ChangeSequenceDeduplicator");
  }

  public ChangeSequenceStrategy getStrategy() {
    return strategy;
  }

  @Override
  public List<RecordConverter> deduplicate(List<RecordConverter> events) {
    if (events == null || events.isEmpty()) {
      return Collections.emptyList();
    }

    ConcurrentHashMap<JsonNode, RecordConverter> deduplicatedEvents = new ConcurrentHashMap<>();

    events.forEach(e -> {
      if (e.key() == null || e.key().isNull()) {
        throw new DebeziumException("Cannot deduplicate data with null key! destination:'"
            + e.destination() + "' event: '" + (e.value() != null ? e.value().toString() : "null") + "'");
      }
      deduplicatedEvents.merge(e.key(), e, (oldValue, newValue) -> {
        int comparison = strategy.extract(oldValue.value()).compareTo(strategy.extract(newValue.value()));
        return comparison <= 0 ? newValue : oldValue;
      });
    });

    return new ArrayList<>(deduplicatedEvents.values());
  }

  @Override
  public String name() {
    return "ChangeSequence(" + strategy.name() + ")";
  }
}
