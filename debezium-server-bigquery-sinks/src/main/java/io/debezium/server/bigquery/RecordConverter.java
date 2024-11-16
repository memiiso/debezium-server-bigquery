package io.debezium.server.bigquery;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableConstraints;
import io.debezium.DebeziumException;
import org.json.JSONObject;

public interface RecordConverter {
  String destination();

  JsonNode value();

  default <T> T convert(Schema schema) throws DebeziumException {
    return convert(schema, false, false);
  }

  /**
   * Converts a Debezium event for BigQuery ingestion.
   *
   * <p>This method transforms a Debezium event into a format suitable for writing to BigQuery.
   * It performs value conversions based on the specified BigQuery schema to ensure compatibility
   * with the target data types.
   *
   * @param schema            The BigQuery schema to use for data type conversions.
   * @param upsert            Indicates whether to enable upsert operations. When set to `true`, a
   *                          `_CHANGE_TYPE` column is added to track record changes (insert, update, delete).
   * @param upsertKeepDeletes When set to `true` in conjunction with `upsert`, the last deleted
   *                          row is retained.
   * @param <T>               The return type of the converted event. This can be a serialized JSON string
   *                          for batch operations or a {@link JSONObject} for stream operations, depending on the
   *                          specific implementation.
   * @return The converted Debezium event, ready for BigQuery ingestion.
   * @throws DebeziumException If an error occurs during the conversion process.
   */
  <T> T convert(Schema schema, boolean upsert, boolean upsertKeepDeletes) throws DebeziumException;

  JsonNode key();

  JsonNode valueSchema();

  JsonNode keySchema();


  /**
   * Extracts table constraints from the Debezium event's key schema.
   *
   * <p>This method analyzes the provided Debezium event's key schema and generates a
   * corresponding BigQuery table constraints configuration. The primary key constraint
   * is derived from the fields present in the key schema. This configuration can be
   * used to optimize table storage and query performance in BigQuery.
   *
   * @return The generated BigQuery table {@link TableConstraints}.
   */
  TableConstraints tableConstraints();


  /**
   * Determines the clustering fields for the BigQuery table.
   *
   * <p>This method extracts field names from the Debezium event's key schema and combines them
   * with a user-provided clustering field (defaulting to `__source_ts_ms`) to create a suitable
   * clustering configuration for the BigQuery table. Clustering the table on these fields
   * can significantly improve query performance, especially for time-series and analytical workloads.
   *
   * @param clusteringField The additional field to use for clustering, beyond the fields
   *                        extracted from the key schema.
   * @return The generated BigQuery {@link Clustering} configuration.
   */
  Clustering tableClustering(String clusteringField);


  /**
   * Transforms a Debezium event's value schema into a corresponding BigQuery schema.
   *
   * <p>This method analyzes the provided Debezium event's value schema and generates a
   * compatible BigQuery schema. It considers factors like field types and
   * the `binaryAsString` flag to accurately map the schema.
   *
   * @return The generated BigQuery {@link Schema}.
   */
  Schema tableSchema();
}
