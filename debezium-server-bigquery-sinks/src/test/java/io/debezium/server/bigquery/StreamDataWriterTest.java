/*
 * Copyright memiiso Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.bigquery;

import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.rpc.Status;
import io.debezium.DebeziumException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class StreamDataWriterTest {
  @Test
  void successfulPipelinedAppendResetsRecreationBudget() throws Exception {
    StreamDataWriter writer = writerReturning(AppendRowsResponse.getDefaultInstance());
    AtomicInteger recreateCount = recreateCount(writer);
    recreateCount.set(3);

    writer.appendPipelined(List.of(new JSONObject().put("id", 1)), 2);

    assertEquals(0, recreateCount.get());
  }

  @Test
  void failedPipelinedAppendPreservesRecreationBudget() throws Exception {
    AppendRowsResponse failure = AppendRowsResponse.newBuilder()
        .setError(Status.newBuilder().setCode(3).setMessage("bad append"))
        .build();
    StreamDataWriter writer = writerReturning(failure);
    AtomicInteger recreateCount = recreateCount(writer);
    recreateCount.set(3);

    assertThrows(DebeziumException.class,
        () -> writer.appendPipelined(List.of(new JSONObject().put("id", 1)), 2));

    assertEquals(3, recreateCount.get());
  }

  private static StreamDataWriter writerReturning(AppendRowsResponse response) throws Exception {
    return new StreamDataWriter("table", null, false, null, null) {
      @Override
      Future<AppendRowsResponse> appendAsync(JSONArray data) {
        return CompletableFuture.completedFuture(response);
      }
    };
  }

  private static AtomicInteger recreateCount(StreamDataWriter writer) throws Exception {
    Field field = StreamDataWriter.class.getDeclaredField("recreateCount");
    field.setAccessible(true);
    return (AtomicInteger) field.get(writer);
  }
}
