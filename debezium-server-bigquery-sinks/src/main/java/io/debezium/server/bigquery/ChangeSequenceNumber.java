/*
 * Copyright memiiso Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.bigquery;

import com.fasterxml.jackson.databind.JsonNode;
import io.debezium.DebeziumException;

import java.math.BigInteger;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** A complete Debezium source coordinate formatted for BigQuery custom CDC ordering. */
public final class ChangeSequenceNumber implements Comparable<ChangeSequenceNumber> {
  public static final String PSEUDO_COLUMN = "_CHANGE_SEQUENCE_NUMBER";
  private static final Pattern TRAILING_NUMBER = Pattern.compile("(\\d+)$");
  private static final BigInteger MAX_SECTION_VALUE = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
  private static final BigInteger ZERO = BigInteger.ZERO;

  private final List<BigInteger> sections;

  private ChangeSequenceNumber(List<BigInteger> sections) {
    this.sections = List.copyOf(sections);
  }

  public static ChangeSequenceNumber from(JsonNode value) {
    BigInteger timestamp = numericField(value, "__source_ts_ns");
    if (hasValue(value, "__source_file")) {
      return fromMySql(value, timestamp);
    }
    if (hasValue(value, "__source_lsn")) {
      return fromPostgres(value, timestamp);
    }
    throw new DebeziumException("Cannot construct BigQuery change sequence: no supported source coordinates found; "
        + "configure unwrap add.fields with source.ts_ns,source.file,source.pos,source.row for MySQL or "
        + "source.ts_ns,source.lsn,source.txId for PostgreSQL");
  }

  private static ChangeSequenceNumber fromMySql(JsonNode value, BigInteger timestamp) {
    JsonNode fileNode = requiredField(value, "__source_file");
    if (!fileNode.isTextual() || fileNode.textValue().isBlank()) {
      throw invalid("__source_file", fileNode, "must be a non-blank string ending in a numeric component");
    }
    Matcher matcher = TRAILING_NUMBER.matcher(fileNode.textValue());
    if (!matcher.find()) {
      throw invalid("__source_file", fileNode, "must end in a numeric component (for example mysql-bin.001234)");
    }
    BigInteger fileIndex = bounded("__source_file", matcher.group(1), new BigInteger(matcher.group(1)));
    BigInteger position = numericField(value, "__source_pos");
    BigInteger row = numericField(value, "__source_row");
    return new ChangeSequenceNumber(List.of(timestamp, fileIndex, position, row));
  }

  private static ChangeSequenceNumber fromPostgres(JsonNode value, BigInteger timestamp) {
    BigInteger lsn = numericField(value, "__source_lsn");
    BigInteger transactionId = numericField(value, "__source_txId");
    return new ChangeSequenceNumber(List.of(timestamp, lsn, transactionId, ZERO));
  }

  private static boolean hasValue(JsonNode value, String field) {
    return value != null && !value.isNull() && value.hasNonNull(field);
  }

  private static BigInteger numericField(JsonNode value, String field) {
    JsonNode node = requiredField(value, field);
    if (!node.isIntegralNumber() && !node.isTextual()) {
      throw invalid(field, node, "must be a non-negative integer");
    }
    try {
      String raw = node.isIntegralNumber() ? node.bigIntegerValue().toString() : node.textValue();
      if (raw == null || !raw.matches("\\d+")) {
        throw new NumberFormatException();
      }
      return bounded(field, raw, new BigInteger(raw));
    } catch (NumberFormatException e) {
      throw invalid(field, node, "must be a non-negative integer");
    }
  }

  private static BigInteger bounded(String field, String raw, BigInteger number) {
    if (number.signum() < 0 || number.compareTo(MAX_SECTION_VALUE) > 0) {
      throw new DebeziumException("Cannot construct BigQuery change sequence: field '" + field
          + "' value '" + raw + "' is outside the unsigned 64-bit range");
    }
    return number;
  }

  private static JsonNode requiredField(JsonNode value, String field) {
    if (value == null || value.isNull() || !value.hasNonNull(field)) {
      throw new DebeziumException("Cannot construct BigQuery change sequence: required Debezium unwrap field '"
          + field + "' is missing; configure all sequence fields documented for the source connector");
    }
    return value.get(field);
  }

  private static DebeziumException invalid(String field, JsonNode value, String expectation) {
    return new DebeziumException("Cannot construct BigQuery change sequence: field '" + field + "' value "
        + String.valueOf(value) + " " + expectation);
  }

  @Override
  public int compareTo(ChangeSequenceNumber other) {
    for (int i = 0; i < sections.size(); i++) {
      int result = sections.get(i).compareTo(other.sections.get(i));
      if (result != 0) {
        return result;
      }
    }
    return 0;
  }

  @Override
  public String toString() {
    return String.format("%016X/%016X/%016X/%016X",
        sections.get(0), sections.get(1), sections.get(2), sections.get(3));
  }
}
