package io.debezium.server.bigquery;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StreamCdcSequencingTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void disabledSequencingDoesNotAddPseudocolumn() throws Exception {
    JSONObject converted = converter("u", 1, 2, 3).convert(null, true, false, false);
    assertEquals("UPSERT", converted.getString("_CHANGE_TYPE"));
    assertFalse(converted.has(ChangeSequenceNumber.PSEUDO_COLUMN));
  }

  @Test
  void allCdcMutationTypesReceiveSequence() throws Exception {
    JSONObject upsert = converter("u", 1, 2, 3).convert(null, true, false, true);
    JSONObject retainedDelete = converter("d", 1, 2, 4).convert(null, true, true, true);
    JSONObject delete = converter("d", 1, 2, 5).convert(null, true, false, true);
    assertEquals("UPSERT", upsert.getString("_CHANGE_TYPE"));
    assertEquals("UPSERT", retainedDelete.getString("_CHANGE_TYPE"));
    assertEquals("DELETE", delete.getString("_CHANGE_TYPE"));
    assertNotNull(upsert.get(ChangeSequenceNumber.PSEUDO_COLUMN));
    assertNotNull(retainedDelete.get(ChangeSequenceNumber.PSEUDO_COLUMN));
    assertNotNull(delete.get(ChangeSequenceNumber.PSEUDO_COLUMN));
  }

  @Test
  void storageSchemaContainsSequenceOnlyForSequencedCdc() {
    Schema physical = Schema.of(Field.of("id", StandardSQLTypeName.INT64));
    TableSchema append = StorageWriteSchemaConverter.toStorageTableSchema(physical, false, true);
    TableSchema unsequencedCdc = StorageWriteSchemaConverter.toStorageTableSchema(physical, true, false);
    TableSchema sequencedCdc = StorageWriteSchemaConverter.toStorageTableSchema(physical, true, true);
    assertFalse(hasField(append, ChangeSequenceNumber.PSEUDO_COLUMN));
    assertFalse(hasField(unsequencedCdc, ChangeSequenceNumber.PSEUDO_COLUMN));
    assertTrue(hasField(sequencedCdc, ChangeSequenceNumber.PSEUDO_COLUMN));
    assertFalse(physical.getFields().stream().anyMatch(field -> field.getName().equals(ChangeSequenceNumber.PSEUDO_COLUMN)));
  }

  @Test
  void deduplicationUsesCompleteSourceCoordinate() throws Exception {
    StreamBigqueryChangeConsumer consumer = new StreamBigqueryChangeConsumer();
    consumer.config = mock(StreamConsumerConfig.class);
    when(consumer.config.changeSequenceEnabled()).thenReturn(true);
    StreamRecordConverter earlier = converter("d", 100, 20, 1);
    StreamRecordConverter later = converter("c", 100, 20, 2);
    List<RecordConverter> result = consumer.deduplicateBatch(List.of(earlier, later));
    assertEquals(1, result.size());
    assertEquals(2, result.get(0).value().get("__source_row").asInt());
  }

  @Test
  void replayProducesTheSameSequencedMutation() throws Exception {
    JSONObject firstAttempt = converter("u", 100, 20, 3).convert(null, true, false, true);
    JSONObject replay = converter("u", 100, 20, 3).convert(null, true, false, true);
    assertEquals(firstAttempt.getString(ChangeSequenceNumber.PSEUDO_COLUMN),
        replay.getString(ChangeSequenceNumber.PSEUDO_COLUMN));
    assertEquals(firstAttempt.getString("_CHANGE_TYPE"), replay.getString("_CHANGE_TYPE"));
  }

  @Test
  void postgresCdcMutationReceivesSequence() throws Exception {
    JsonNode value = MAPPER.readTree(
        "{\"id\":1,\"__op\":\"u\",\"__source_ts_ns\":100,\"__source_lsn\":200,\"__source_txId\":300}");
    JsonNode key = MAPPER.readTree("{\"id\":1}");
    JSONObject converted = new StreamRecordConverter("test", value, key, null, null, null)
        .convert(null, true, false, true);
    assertEquals("0000000000000064/00000000000000C8/000000000000012C/0000000000000000",
        converted.getString(ChangeSequenceNumber.PSEUDO_COLUMN));
  }

  private static boolean hasField(TableSchema schema, String name) {
    return schema.getFieldsList().stream().anyMatch(field -> field.getName().equals(name));
  }

  private static StreamRecordConverter converter(String operation, long timestamp, long position, long row) throws Exception {
    JsonNode value = MAPPER.readTree(String.format(
        "{\"id\":1,\"__op\":\"%s\",\"__source_ts_ns\":%d,\"__source_file\":\"mysql-bin.000007\",\"__source_pos\":%d,\"__source_row\":%d}",
        operation, timestamp, position, row));
    JsonNode key = MAPPER.readTree("{\"id\":1}");
    return new StreamRecordConverter("test", value, key, null, null, null);
  }
}
