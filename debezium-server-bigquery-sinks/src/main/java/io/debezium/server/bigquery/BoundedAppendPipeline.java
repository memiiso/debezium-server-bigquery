/*
 * Copyright memiiso Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.bigquery;

import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import io.debezium.DebeziumException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/** Submits a bounded number of balanced append requests and observes every result. */
final class BoundedAppendPipeline {
  @FunctionalInterface
  interface Appender {
    Future<AppendRowsResponse> append(JSONArray rows) throws Exception;
  }

  private BoundedAppendPipeline() {
  }

  static void append(List<JSONObject> rows, int maxInFlight, Appender appender) {
    if (rows.isEmpty()) {
      return;
    }
    int appendCount = Math.min(maxInFlight, rows.size());
    List<Future<AppendRowsResponse>> futures = new ArrayList<>(appendCount);
    DebeziumException failure = null;

    int baseSize = rows.size() / appendCount;
    int largerChunks = rows.size() % appendCount;
    int offset = 0;
    for (int chunk = 0; chunk < appendCount; chunk++) {
      int chunkSize = baseSize + (chunk < largerChunks ? 1 : 0);
      JSONArray payload = new JSONArray();
      for (int i = 0; i < chunkSize; i++) {
        payload.put(rows.get(offset++));
      }
      try {
        futures.add(appender.append(payload));
      } catch (Exception e) {
        failure = combine(failure, new DebeziumException("Failed to submit BigQuery append request", e));
        break;
      }
    }

    boolean interrupted = false;
    for (Future<AppendRowsResponse> future : futures) {
      if (interrupted && !future.isDone()) {
        failure = combine(failure, new DebeziumException(
            "A BigQuery append request did not terminate after cancellation"));
        continue;
      }
      boolean observed = false;
      while (!observed) {
        try {
          AppendRowsResponse response = future.get();
          observed = true;
          if (response.hasError()) {
            failure = combine(failure, StreamDataWriter.createDebeziumExceptionFromResponseError(response));
          }
        } catch (InterruptedException e) {
          interrupted = true;
          failure = combine(failure, new DebeziumException("Interrupted while waiting for BigQuery append requests", e));
          cancelOutstanding(futures, failure);
          if (!future.isDone()) {
            failure = combine(failure, new DebeziumException(
                "A BigQuery append request did not terminate after cancellation"));
          }
          observed = true;
        } catch (CancellationException e) {
          observed = true;
          failure = combine(failure, new DebeziumException("A BigQuery append request was cancelled", e));
        } catch (ExecutionException | RuntimeException e) {
          observed = true;
          failure = combine(failure, StreamDataWriter.createDebeziumExceptionFromException(e));
        }
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    if (failure != null) {
      throw failure;
    }
  }

  private static void cancelOutstanding(List<? extends Future<?>> futures, DebeziumException failure) {
    for (Future<?> future : futures) {
      if (!future.isDone()) {
        try {
          future.cancel(true);
        } catch (RuntimeException e) {
          failure.addSuppressed(new DebeziumException("Failed to cancel a BigQuery append request", e));
        }
      }
    }
  }

  private static DebeziumException combine(DebeziumException existing, DebeziumException additional) {
    if (existing == null) {
      return additional;
    }
    existing.addSuppressed(additional);
    return existing;
  }
}
