/*
 * Copyright memiiso Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.bigquery.deduplication;

import com.fasterxml.jackson.databind.JsonNode;
import io.debezium.DebeziumException;
import io.debezium.server.bigquery.ChangeSequenceNumber;
import io.debezium.server.bigquery.RecordConverter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Deduplication strategy using Debezium CDC coordinates via ChangeSequenceNumber.
 *
 * @author Ismail Simsek
 */
public class ChangeSequenceBatchDeduplicator implements BatchDeduplicator {

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
        int comparison = ChangeSequenceNumber.from(oldValue.value())
            .compareTo(ChangeSequenceNumber.from(newValue.value()));
        if (comparison <= 0) {
          return newValue;
        } else {
          return oldValue;
        }
      });
    });

    return new ArrayList<>(deduplicatedEvents.values());
  }
}
