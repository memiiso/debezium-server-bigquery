/*
 * Copyright memiiso Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.bigquery.changesequence;

import com.fasterxml.jackson.databind.JsonNode;
import io.debezium.DebeziumException;
import io.debezium.server.bigquery.ChangeSequenceNumber;

import java.math.BigInteger;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ChangeSequenceStrategy implementation for MySQL / MariaDB connectors.
 *
 * @author Ismail Simsek
 */
public class MySqlChangeSequenceStrategy implements ChangeSequenceStrategy {

  private static final Pattern TRAILING_NUMBER = Pattern.compile("(\\d+)$");

  @Override
  public boolean supports(String connectorClass) {
    if (connectorClass == null || connectorClass.isBlank()) {
      return false;
    }
    String normalized = connectorClass.toLowerCase();
    return normalized.contains("mysql") || normalized.contains("mariadb");
  }

  @Override
  public ChangeSequenceNumber extract(JsonNode value) throws DebeziumException {
    BigInteger timestamp = ChangeSequenceNumber.numericField(value, "__source_ts_ns");
    JsonNode fileNode = ChangeSequenceNumber.requiredField(value, "__source_file");
    if (!fileNode.isTextual() || fileNode.textValue().isBlank()) {
      throw ChangeSequenceNumber.invalid("__source_file", fileNode, "must be a non-blank string ending in a numeric component");
    }
    Matcher matcher = TRAILING_NUMBER.matcher(fileNode.textValue());
    if (!matcher.find()) {
      throw ChangeSequenceNumber.invalid("__source_file", fileNode, "must end in a numeric component (for example mysql-bin.001234)");
    }
    BigInteger fileIndex = ChangeSequenceNumber.bounded("__source_file", matcher.group(1), new BigInteger(matcher.group(1)));
    BigInteger position = ChangeSequenceNumber.numericField(value, "__source_pos");
    BigInteger row = ChangeSequenceNumber.numericField(value, "__source_row");
    return ChangeSequenceNumber.of(List.of(timestamp, fileIndex, position, row));
  }

  @Override
  public String name() {
    return "MySQL";
  }
}
