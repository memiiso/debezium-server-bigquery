/*
 * Copyright memiiso Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.bigquery.deduplication;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.DebeziumException;
import io.debezium.server.bigquery.RecordConverter;
import io.debezium.server.bigquery.StreamRecordConverter;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class BatchDeduplicatorTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static StreamRecordConverter createRecord(int id, String op, Long tsNs, String binlogFile, Long pos, Long row) throws Exception {
    StringBuilder json = new StringBuilder("{");
    json.append("\"id\":").append(id);
    if (op != null) {
      json.append(",\"__op\":\"").append(op).append("\"");
    }
    if (tsNs != null) {
      json.append(",\"__source_ts_ns\":").append(tsNs);
    }
    if (binlogFile != null) {
      json.append(",\"__source_file\":\"").append(binlogFile).append("\"");
    }
    if (pos != null) {
      json.append(",\"__source_pos\":").append(pos);
    }
    if (row != null) {
      json.append(",\"__source_row\":").append(row);
    }
    json.append("}");

    JsonNode key = MAPPER.readTree("{\"id\":" + id + "}");
    JsonNode value = MAPPER.readTree(json.toString());
    return new StreamRecordConverter("test_destination", value, key, null, null, null);
  }

  @Test
  void defaultDeduplicatorComparesTimestampAndOp() throws Exception {
    DefaultBatchDeduplicator deduplicator = new DefaultBatchDeduplicator(
        Optional.of("__source_ts_ns"),
        "__op"
    );

    StreamRecordConverter older = createRecord(1, "u", 100L, null, null, null);
    StreamRecordConverter newer = createRecord(1, "u", 200L, null, null, null);

    List<RecordConverter> result = deduplicator.deduplicate(List.of(older, newer));
    assertEquals(1, result.size());
    assertEquals(200L, result.get(0).value().get("__source_ts_ns").asLong());

    // Reversed input order
    List<RecordConverter> reversedResult = deduplicator.deduplicate(List.of(newer, older));
    assertEquals(1, reversedResult.size());
    assertEquals(200L, reversedResult.get(0).value().get("__source_ts_ns").asLong());

    // Tie-breaker by op code: 'd' > 'u' > 'r' > 'c'
    StreamRecordConverter insert = createRecord(1, "c", 100L, null, null, null);
    StreamRecordConverter delete = createRecord(1, "d", 100L, null, null, null);
    List<RecordConverter> tieResult = deduplicator.deduplicate(List.of(insert, delete));
    assertEquals(1, tieResult.size());
    assertEquals("d", tieResult.get(0).value().get("__op").asText());
  }

  @Test
  void defaultDeduplicatorFallsBackToLastWhenTsColumnBlank() throws Exception {
    DefaultBatchDeduplicator deduplicator = new DefaultBatchDeduplicator(
        Optional.empty(),
        "__op"
    );

    StreamRecordConverter first = createRecord(1, "c", 500L, null, null, null);
    StreamRecordConverter second = createRecord(1, "u", 100L, null, null, null);

    List<RecordConverter> result = deduplicator.deduplicate(List.of(first, second));
    assertEquals(1, result.size());
    assertEquals("u", result.get(0).value().get("__op").asText());
  }

  @Test
  void defaultDeduplicatorHandlesMultipleKeys() throws Exception {
    DefaultBatchDeduplicator deduplicator = new DefaultBatchDeduplicator(
        Optional.of("__source_ts_ns"),
        "__op"
    );

    StreamRecordConverter key1V1 = createRecord(1, "c", 100L, null, null, null);
    StreamRecordConverter key1V2 = createRecord(1, "u", 200L, null, null, null);
    StreamRecordConverter key2V1 = createRecord(2, "c", 300L, null, null, null);

    List<RecordConverter> result = deduplicator.deduplicate(List.of(key1V1, key2V1, key1V2));
    assertEquals(2, result.size());

    RecordConverter k1 = result.stream().filter(r -> r.key().get("id").asInt() == 1).findFirst().orElseThrow();
    assertEquals(200L, k1.value().get("__source_ts_ns").asLong());

    RecordConverter k2 = result.stream().filter(r -> r.key().get("id").asInt() == 2).findFirst().orElseThrow();
    assertEquals(300L, k2.value().get("__source_ts_ns").asLong());
  }

  @Test
  void changeSequenceDeduplicatorUsesCdcCoordinates() throws Exception {
    ChangeSequenceBatchDeduplicator deduplicator = new ChangeSequenceBatchDeduplicator();

    StreamRecordConverter earlier = createRecord(1, "d", 100L, "mysql-bin.000001", 1000L, 1L);
    StreamRecordConverter later = createRecord(1, "c", 100L, "mysql-bin.000001", 1000L, 2L);

    List<RecordConverter> result = deduplicator.deduplicate(List.of(earlier, later));
    assertEquals(1, result.size());
    assertEquals(2L, result.get(0).value().get("__source_row").asLong());

    // Reversed input order
    List<RecordConverter> reversedResult = deduplicator.deduplicate(List.of(later, earlier));
    assertEquals(1, reversedResult.size());
    assertEquals(2L, reversedResult.get(0).value().get("__source_row").asLong());
  }

  @Test
  void handlesEmptyAndNullEventsGracefully() {
    DefaultBatchDeduplicator defaultDedup = new DefaultBatchDeduplicator();
    ChangeSequenceBatchDeduplicator changeSeqDedup = new ChangeSequenceBatchDeduplicator();

    assertTrue(defaultDedup.deduplicate(null).isEmpty());
    assertTrue(defaultDedup.deduplicate(Collections.emptyList()).isEmpty());

    assertTrue(changeSeqDedup.deduplicate(null).isEmpty());
    assertTrue(changeSeqDedup.deduplicate(Collections.emptyList()).isEmpty());
  }

  @Test
  void rejectsNullKey() throws Exception {
    DefaultBatchDeduplicator defaultDedup = new DefaultBatchDeduplicator();
    ChangeSequenceBatchDeduplicator changeSeqDedup = new ChangeSequenceBatchDeduplicator();

    JsonNode value = MAPPER.readTree("{\"id\":1}");
    StreamRecordConverter nullKeyRecord = new StreamRecordConverter("dest", value, null, null, null, null);

    DebeziumException ex1 = assertThrows(DebeziumException.class, () ->
        defaultDedup.deduplicate(List.of(nullKeyRecord)));
    assertTrue(ex1.getMessage().contains("Cannot deduplicate data with null key!"));

    DebeziumException ex2 = assertThrows(DebeziumException.class, () ->
        changeSeqDedup.deduplicate(List.of(nullKeyRecord)));
    assertTrue(ex2.getMessage().contains("Cannot deduplicate data with null key!"));
  }
}
