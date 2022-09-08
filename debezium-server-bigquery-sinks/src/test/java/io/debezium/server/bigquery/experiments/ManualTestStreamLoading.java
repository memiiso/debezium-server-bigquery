/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery.experiments;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import org.json.JSONArray;
import org.json.JSONObject;

public class ManualTestStreamLoading {

  public static void main(String[] args) throws IOException, InterruptedException, Descriptors.DescriptorValidationException, ExecutionException {
    // [START ]
    // Table schema definition
    BigQuery bigquery = BigQueryOptions.newBuilder()
        .setCredentials(GoogleCredentials.getApplicationDefault())
        .setLocation("EU")
        .build()
        .getService();
    TableName tn = TableName.of(bigquery.getOptions().getProjectId(), "stage", "test_json_loading_stream");
    TableId tableId = TableId.of(tn.getDataset(), tn.getTable());
    Field[] fields =
        new Field[]{
            Field.of("c_id", LegacySQLTypeName.INTEGER),
            Field.of("c_date", LegacySQLTypeName.DATE),
            Field.of("c_datetime", LegacySQLTypeName.DATETIME),
            Field.of("c_ts", LegacySQLTypeName.TIMESTAMP)
        };
    // Table schema definition
    Schema schema = Schema.of(fields);
    TableInfo tableInfo = TableInfo.newBuilder(tableId, StandardTableDefinition.newBuilder()
        .setSchema(schema)
        .build()).build();
    //bigquery.delete(tableId);
    //bigquery.create(tableInfo);
    System.out.println("Created table!");

    BigQueryWriteSettings bigQueryWriteSettings = BigQueryWriteSettings
        .newBuilder()
        .setCredentialsProvider(FixedCredentialsProvider.create(GoogleCredentials.getApplicationDefault()))
        .build();
    BigQueryWriteClient bigQueryWriteClient = BigQueryWriteClient.create(bigQueryWriteSettings);

    Thread.sleep(5000);
    JsonStreamWriter streamWriter = JsonStreamWriter
        .newBuilder(tn.toString(), BqToBqStorageSchemaConverter.convertTableSchema(schema), bigQueryWriteClient)
        .build();

    JSONArray jsonArr = new JSONArray();
    JSONObject record = new JSONObject();
    record.put("c_id", 1);
    record.put("c_date", 3);
    record.put("c_datetime", 1662805921200L);
    record.put("c_ts", 1662805921200L);
    jsonArr.put(record);

    ApiFuture<AppendRowsResponse> request = streamWriter.append(jsonArr);
    //AppendRowsResponse waitComplete = request.get();
    System.out.println("DONE");
    //streamWriter.close();
    TableResult result =
        bigquery.query(QueryJobConfiguration.newBuilder("select * from " + tableId.getDataset() + "." + tableId.getTable()).build());
    result.getValues().forEach(r -> System.out.println(r.toString()));
    // [END ]
  }
}

/**
 * Converts structure from BigQuery client to BigQueryStorage client
 */
class BqToBqStorageSchemaConverter {
  private static ImmutableMap<Field.Mode, TableFieldSchema.Mode> BQTableSchemaModeMap =
      ImmutableMap.of(
          Field.Mode.NULLABLE, TableFieldSchema.Mode.NULLABLE,
          Field.Mode.REPEATED, TableFieldSchema.Mode.REPEATED,
          Field.Mode.REQUIRED, TableFieldSchema.Mode.REQUIRED);

  private static ImmutableMap<StandardSQLTypeName, TableFieldSchema.Type> BQTableSchemaTypeMap =
      new ImmutableMap.Builder<StandardSQLTypeName, TableFieldSchema.Type>()
          .put(StandardSQLTypeName.BOOL, TableFieldSchema.Type.BOOL)
          .put(StandardSQLTypeName.BYTES, TableFieldSchema.Type.BYTES)
          .put(StandardSQLTypeName.DATE, TableFieldSchema.Type.DATE)
          .put(StandardSQLTypeName.DATETIME, TableFieldSchema.Type.DATETIME)
          .put(StandardSQLTypeName.FLOAT64, TableFieldSchema.Type.DOUBLE)
          .put(StandardSQLTypeName.GEOGRAPHY, TableFieldSchema.Type.GEOGRAPHY)
          .put(StandardSQLTypeName.INT64, TableFieldSchema.Type.INT64)
          .put(StandardSQLTypeName.NUMERIC, TableFieldSchema.Type.NUMERIC)
          .put(StandardSQLTypeName.STRING, TableFieldSchema.Type.STRING)
          .put(StandardSQLTypeName.STRUCT, TableFieldSchema.Type.STRUCT)
          .put(StandardSQLTypeName.TIME, TableFieldSchema.Type.TIME)
          .put(StandardSQLTypeName.TIMESTAMP, TableFieldSchema.Type.TIMESTAMP)
          .build();

  /**
   * Converts from BigQuery client Table Schema to bigquery storage API Table Schema.
   *
   * @param schema the BigQuery client Table Schema
   * @return the bigquery storage API Table Schema
   */
  public static TableSchema convertTableSchema(Schema schema) {
    TableSchema.Builder result = TableSchema.newBuilder();
    for (int i = 0; i < schema.getFields().size(); i++) {
      result.addFields(i, convertFieldSchema(schema.getFields().get(i)));
    }
    return result.build();
  }

  /**
   * Converts from bigquery v2 Field Schema to bigquery storage API Field Schema.
   *
   * @param field the BigQuery client Field Schema
   * @return the bigquery storage API Field Schema
   */
  public static TableFieldSchema convertFieldSchema(Field field) {
    TableFieldSchema.Builder result = TableFieldSchema.newBuilder();
    if (field.getMode() == null) {
      field = field.toBuilder().setMode(Field.Mode.NULLABLE).build();
    }
    result.setMode(BQTableSchemaModeMap.get(field.getMode()));
    result.setName(field.getName());
    result.setType(BQTableSchemaTypeMap.get(field.getType().getStandardType()));
    if (field.getDescription() != null) {
      result.setDescription(field.getDescription());
    }
    if (field.getSubFields() != null) {
      for (int i = 0; i < field.getSubFields().size(); i++) {
        result.addFields(i, convertFieldSchema(field.getSubFields().get(i)));
      }
    }
    return result.build();
  }
}