package io.debezium.server.bigquery;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.DebeziumException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class ChangeSequenceNumberTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void formatsFourFixedWidthUppercaseHexSections() throws Exception {
    String sequence = ChangeSequenceNumber.from(value(123456789, "mysql-bin.001234", 255, 10)).toString();
    assertEquals("00000000075BCD15/00000000000004D2/00000000000000FF/000000000000000A", sequence);
    assertTrue(sequence.matches("[0-9A-F]{16}(?:/[0-9A-F]{16}){3}"));
  }

  @Test
  void binlogRotationChangesFileSection() throws Exception {
    String first = ChangeSequenceNumber.from(value(1, "mysql-bin.000009", 2, 3)).toString();
    String rotated = ChangeSequenceNumber.from(value(1, "mysql-bin.000010", 2, 3)).toString();
    assertEquals("0000000000000009", first.split("/")[1]);
    assertEquals("000000000000000A", rotated.split("/")[1]);
    assertTrue(first.compareTo(rotated) < 0);
  }

  @Test
  void positionAndRowBreakTimestampTies() throws Exception {
    ChangeSequenceNumber first = ChangeSequenceNumber.from(value(10, "bin.1", 20, 30));
    ChangeSequenceNumber laterPosition = ChangeSequenceNumber.from(value(10, "bin.1", 21, 0));
    ChangeSequenceNumber laterRow = ChangeSequenceNumber.from(value(10, "bin.1", 20, 31));
    assertTrue(first.compareTo(laterPosition) < 0);
    assertTrue(first.compareTo(laterRow) < 0);
  }

  @Test
  void postgresUsesLsnTransactionIdAndEventOrderCoordinates() throws Exception {
    ChangeSequenceNumber first = ChangeSequenceNumber.from(postgresValue(10, "255", 41, 7));
    ChangeSequenceNumber laterLsn = ChangeSequenceNumber.from(postgresValue(10, "256", 1, 1));
    ChangeSequenceNumber laterTransaction = ChangeSequenceNumber.from(postgresValue(10, "255", 42, 1));
    ChangeSequenceNumber laterInTransaction = ChangeSequenceNumber.from(postgresValue(10, "255", 41, 8));
    assertEquals("000000000000000A/00000000000000FF/0000000000000029/0000000000000007",
        first.toString());
    assertTrue(first.compareTo(laterLsn) < 0);
    assertTrue(first.compareTo(laterTransaction) < 0);
    assertTrue(first.compareTo(laterInTransaction) < 0);
  }

  @Test
  void postgresAcceptsFormattedHexadecimalLsn() throws Exception {
    ChangeSequenceNumber sequence = ChangeSequenceNumber.from(postgresValue(10, "\"0/16B3748\"", 41, 7));
    assertEquals("000000000000000A/00000000016B3748/0000000000000029/0000000000000007",
        sequence.toString());
  }

  @Test
  void postgresRequiresTransactionEventOrder() throws Exception {
    JsonNode missingOrder = MAPPER.readTree(
        "{\"__source_ts_ns\":10,\"__source_lsn\":255,\"__source_txId\":41}");
    assertTrue(assertThrows(DebeziumException.class, () -> ChangeSequenceNumber.from(missingOrder))
        .getMessage().contains("__transaction_total_order"));
  }

  @Test
  void missingAndMalformedFieldsFailFast() throws Exception {
    JsonNode missing = MAPPER.readTree("{\"__source_ts_ns\":1}");
    assertTrue(assertThrows(DebeziumException.class, () -> ChangeSequenceNumber.from(missing))
        .getMessage().contains("no supported source coordinates"));
    assertTrue(assertThrows(DebeziumException.class,
        () -> ChangeSequenceNumber.from(value(1, "mysql-bin.current", 2, 3))).getMessage().contains("numeric component"));
    JsonNode negative = MAPPER.readTree("{\"__source_ts_ns\":-1,\"__source_file\":\"bin.1\",\"__source_pos\":2,\"__source_row\":3}");
    assertTrue(assertThrows(DebeziumException.class, () -> ChangeSequenceNumber.from(negative))
        .getMessage().contains("__source_ts_ns"));
  }

  private static JsonNode value(long timestamp, String file, long position, long row) throws Exception {
    return MAPPER.readTree(String.format(
        "{\"__source_ts_ns\":%d,\"__source_file\":\"%s\",\"__source_pos\":%d,\"__source_row\":%d}",
        timestamp, file, position, row));
  }

  private static JsonNode postgresValue(long timestamp, String lsn, long transactionId, long transactionOrder)
      throws Exception {
    return MAPPER.readTree(String.format(
        "{\"__source_ts_ns\":%d,\"__source_lsn\":%s,\"__source_txId\":%d,\"__transaction_total_order\":%d}",
        timestamp, lsn, transactionId, transactionOrder));
  }
}
