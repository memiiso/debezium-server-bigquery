package io.debezium.server.bigquery;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.DebeziumException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
  void missingAndMalformedFieldsFailFast() throws Exception {
    JsonNode missing = MAPPER.readTree("{\"__source_ts_ns\":1}");
    assertTrue(assertThrows(DebeziumException.class, () -> ChangeSequenceNumber.from(missing))
        .getMessage().contains("__source_file"));
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
}
