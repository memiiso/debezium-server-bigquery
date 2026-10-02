/*
 * Copyright memiiso Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.bigquery.deduplication;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.server.bigquery.RecordConverter;
import io.debezium.server.bigquery.StreamRecordConverter;
import io.debezium.server.bigquery.changesequence.MySqlChangeSequenceStrategy;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class BatchDeduplicatorTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void timestampAndOpDeduplicatorComparesTimestampAndOp() throws Exception {
    TimestampAndOpDeduplicator deduplicator = new TimestampAndOpDeduplicator(
        Optional.of("__source_ts_ms"), "__op");

    // Later timestamp wins
    RecordConverter older = record(1, "u", "v1", 100);
    RecordConverter newer = record(1, "u", "v2", 200);
    List<RecordConverter> result = deduplicator.deduplicate(List.of(older, newer));
    assertEquals(1, result.size());
    assertEquals("v2", result.get(0).value().get("text").asText());

    // Same timestamp: Delete (op priority 4) wins over Insert (op priority 1)
    RecordConverter insertSameTs = record(2, "c", "v1", 100);
    RecordConverter deleteSameTs = record(2, "d", "v1", 100);
    List<RecordConverter> tieResult = deduplicator.deduplicate(List.of(insertSameTs, deleteSameTs));
    assertEquals(1, tieResult.size());
    assertEquals("d", tieResult.get(0).value().get("__op").asText());
  }

  @Test
  void timestampAndOpDeduplicatorFallsBackToLastWhenTsColumnBlank() throws Exception {
    TimestampAndOpDeduplicator deduplicator = new TimestampAndOpDeduplicator(
        Optional.empty(), "__op");

    RecordConverter first = record(1, "d", "v1", 100);
    RecordConverter second = record(1, "c", "v2", 50);
    List<RecordConverter> result = deduplicator.deduplicate(List.of(first, second));
    assertEquals(1, result.size());
    assertEquals("v2", result.get(0).value().get("text").asText());
  }

  @Test
  void changeSequenceDeduplicatorUsesCdcCoordinates() throws Exception {
    ChangeSequenceDeduplicator deduplicator = new ChangeSequenceDeduplicator(new MySqlChangeSequenceStrategy());

    // Both have exact same timestamp (100 ns), but second has later row coordinate (2 vs 1)
    JsonNode k = MAPPER.readTree("{\"id\":1}");
    JsonNode v1 = MAPPER.readTree(
        "{\"id\":1,\"text\":\"earlier\",\"__op\":\"d\",\"__source_ts_ns\":100,\"__source_file\":\"mysql-bin.000007\",\"__source_pos\":20,\"__source_row\":1}");
    JsonNode v2 = MAPPER.readTree(
        "{\"id\":1,\"text\":\"later\",\"__op\":\"c\",\"__source_ts_ns\":100,\"__source_file\":\"mysql-bin.000007\",\"__source_pos\":20,\"__source_row\":2}");

    RecordConverter r1 = new StreamRecordConverter("test", v1, k, null, null, null);
    RecordConverter r2 = new StreamRecordConverter("test", v2, k, null, null, null);

    List<RecordConverter> result = deduplicator.deduplicate(List.of(r1, r2));
    assertEquals(1, result.size());
    assertEquals("later", result.get(0).value().get("text").asText());
    assertEquals(2, result.get(0).value().get("__source_row").asInt());
  }

  @Test
  void changeSequenceDeduplicatorRejectsNullStrategy() {
    assertThrows(NullPointerException.class, () -> new ChangeSequenceDeduplicator(null));
  }

  private static RecordConverter record(int id, String op, String text, long ts) throws Exception {
    JsonNode key = MAPPER.readTree("{\"id\":" + id + "}");
    JsonNode val = MAPPER.readTree(String.format(
        "{\"id\":%d,\"__op\":\"%s\",\"text\":\"%s\",\"__source_ts_ms\":%d,\"__source_ts_ns\":%d}",
        id, op, text, ts, ts * 1_000_000));
    return new StreamRecordConverter("test", val, key, null, null, null);
  }
}
