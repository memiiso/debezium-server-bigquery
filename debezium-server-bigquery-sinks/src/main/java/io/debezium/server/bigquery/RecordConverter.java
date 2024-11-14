package io.debezium.server.bigquery;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableConstraints;
import io.debezium.DebeziumException;

public interface RecordConverter {
  String destination();

  JsonNode value();

  default <T> T convert(Schema schema) throws DebeziumException {
    return convert(schema, false, false);
  }

  <T> T convert(Schema schema, boolean upsert, boolean upsertKeepDeletes) throws DebeziumException;

  JsonNode key();

  JsonNode valueSchema();

  JsonNode keySchema();

  TableConstraints tableConstraints();

  Clustering tableClustering(String clusteringField);

  Schema tableSchema(Boolean binaryAsString);
}
