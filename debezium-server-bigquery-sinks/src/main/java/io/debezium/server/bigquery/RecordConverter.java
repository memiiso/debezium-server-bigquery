package io.debezium.server.bigquery;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableConstraints;
import org.json.JSONObject;

public interface RecordConverter {
  String destination();

  JsonNode value();

  String valueAsJsonLine(Schema schema) throws JsonProcessingException;

  JSONObject valueAsJsonObject(boolean upsert, boolean upsertKeepDeletes);

  JsonNode key();

  JsonNode valueSchema();

  JsonNode keySchema();

  TableConstraints tableConstraints();

  Clustering tableClustering(String clusteringField);

  Schema tableSchema(Boolean binaryAsString);
}
