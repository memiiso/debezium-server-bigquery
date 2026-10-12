/*
 * Copyright memiiso Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.bigquery.deduplication;

import com.fasterxml.jackson.databind.JsonNode;
import io.debezium.DebeziumException;
import io.debezium.server.bigquery.RecordConverter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Deduplication strategy that retains the last event for each primary key (PK).
 * Because Debezium delivers change events in chronological commit order, retaining the latest
 * event preserves accurate state without requiring timestamp or operation parsing.
 *
 * @author Ismail Simsek
 */
public class RetainLastBatchDeduplicator implements BatchDeduplicator {

  @Override
  public List<RecordConverter> deduplicate(List<RecordConverter> events) {
    if (events == null || events.isEmpty()) {
      return Collections.emptyList();
    }

    Map<JsonNode, RecordConverter> deduplicatedEvents = new LinkedHashMap<>(events.size());

    for (RecordConverter e : events) {
      if (e.key() == null || e.key().isNull()) {
        throw new DebeziumException("Cannot deduplicate data with null key! destination:'"
            + e.destination() + "' event: '" + (e.value() != null ? e.value().toString() : "null") + "'");
      }
      deduplicatedEvents.put(e.key(), e);
    }

    return new ArrayList<>(deduplicatedEvents.values());
  }
}
