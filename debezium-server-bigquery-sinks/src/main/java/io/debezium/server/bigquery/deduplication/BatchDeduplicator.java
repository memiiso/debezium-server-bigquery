/*
 * Copyright memiiso Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.bigquery.deduplication;

import io.debezium.server.bigquery.RecordConverter;

import java.util.List;

/**
 * Interface for batch deduplication strategies within a destination table.
 *
 * @author Ismail Simsek
 */
public interface BatchDeduplicator {

  /**
   * Deduplicates events within the given batch destined for a single table.
   *
   * @param events list of incoming converted events
   * @return list of deduplicated events
   */
  List<RecordConverter> deduplicate(List<RecordConverter> events);

  /**
   * Human-readable name for logging.
   */
  default String name() {
    return getClass().getSimpleName();
  }
}
