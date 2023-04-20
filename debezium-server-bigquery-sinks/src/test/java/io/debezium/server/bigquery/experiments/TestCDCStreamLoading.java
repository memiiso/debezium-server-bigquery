/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery.experiments;

import io.debezium.server.bigquery.BqToBqStorageSchemaConverter;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.protobuf.Descriptors;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestCDCStreamLoading {

  protected static final Logger LOGGER = LoggerFactory.getLogger(TestCDCStreamLoading.class);

  public static void main(String[] args) throws IOException, InterruptedException, ExecutionException, Descriptors.DescriptorValidationException {
    // [START ]
    // Table schema definition
    BigQuery bqClient = BigQueryOptions.newBuilder()
        .setCredentials(GoogleCredentials.getApplicationDefault())
        .setLocation("EU")
        .build()
        .getService();
    TableName tableName = TableName.of(bqClient.getOptions().getProjectId(), "stage", "test_CDC_stream_data_loading");
    TableId tableId = TableId.of(tableName.getDataset(), tableName.getTable());
    // Table schema definition
    Schema schema = Schema.of(
        Field.of("c_id", LegacySQLTypeName.INTEGER),
        Field.of("c_string", LegacySQLTypeName.STRING)
    );
    TableInfo tableInfo = TableInfo.newBuilder(tableId, StandardTableDefinition.newBuilder()
        .setSchema(schema)
        .setClustering(Clustering.newBuilder().setFields(List.of("c_id", "c_string")).build())
        .build()).build();
    
//    bqClient.delete(tableId);
//    Table table = bqClient.create(tableInfo);
//    LOGGER.error("Created table " + table.getTableId());
//    String query = "ALTER TABLE " + tableName.getDataset() + "." + tableName.getTable() + " ADD PRIMARY KEY (c_id) NOT ENFORCED;";
//    bqClient.query(QueryJobConfiguration.newBuilder(query).build());
//    LOGGER.error("Created PK " + table.getTableId());
    String query2 = "ALTER table  " + tableName.getDataset() + "." + tableName.getTable() + " SET OPTIONS " +
        "(max_staleness = INTERVAL '0-0 0 0:0:2' YEAR TO SECOND);";
    bqClient.query(QueryJobConfiguration.newBuilder(query2).build());

    TableSchema tableSchema = BqToBqStorageSchemaConverter.convertTableSchema(schema);

    BigQueryWriteSettings bigQueryWriteSettings = BigQueryWriteSettings
        .newBuilder()
        .setCredentialsProvider(FixedCredentialsProvider.create(GoogleCredentials.getApplicationDefault()))
        .build();
    BigQueryWriteClient bigQueryWriteClient = BigQueryWriteClient.create(bigQueryWriteSettings);

    Thread.sleep(10000);
    JsonStreamWriter streamWriter = JsonStreamWriter.newBuilder(tableName.toString(),tableSchema,
        bigQueryWriteClient).build();
    LOGGER.error("Created streamWriter");

    JSONArray jsonArr = new JSONArray();
    jsonArr.put(new JSONObject().put("c_id", 1).put("c_string", "record-1-v2").put("_CHANGE_TYPE", "UPSERT"));
    jsonArr.put(new JSONObject().put("c_id", 1).put("c_string", "record-1-v1").put("_CHANGE_TYPE", "UPSERT"));
    jsonArr.put(new JSONObject().put("c_id", 1).put("c_string", "record-1-v3").put("_CHANGE_TYPE", "UPSERT"));
    AppendRowsResponse response = streamWriter.append(jsonArr).get();
    LOGGER.error("response {}", response.getError());
    Thread.sleep(15000);
    ///////////////////////////////////////
    jsonArr = new JSONArray();
    jsonArr.put(new JSONObject().put("c_id", 2).put("c_string", "record-2").put("_CHANGE_TYPE", "UPSERT"));
    streamWriter.append(jsonArr).get();
    Thread.sleep(15000);
    ///////////////////////////////////////
    jsonArr = new JSONArray();
    jsonArr.put(new JSONObject().put("c_id", 1).put("c_string", "record-1 V2-UPSERT").put("_CHANGE_TYPE", "UPSERT"));
    streamWriter.append(jsonArr).get();
    Thread.sleep(15000);
    ///////////////////////////////////////
    jsonArr = new JSONArray();
    jsonArr.put(new JSONObject().put("c_id", 2).put("c_string", "record-2-UPSERT-r3").put("_CHANGE_TYPE", "UPSERT"));
    jsonArr.put(new JSONObject().put("c_id", 2).put("c_string", "record-2-UPSERT-r1").put("_CHANGE_TYPE", "UPSERT"));
    jsonArr.put(new JSONObject().put("c_id", 2).put("c_string", "record-2-UPSERT-r2").put("_CHANGE_TYPE", "UPSERT"));
    streamWriter.append(jsonArr).get();
    Thread.sleep(15000);

    LOGGER.error("DONE");
    streamWriter.close();
    TableResult result =
        bqClient.query(QueryJobConfiguration.newBuilder("select * from " + tableId.getDataset() + "." + tableId.getTable()).build());
    result.getValues().forEach(r -> System.out.println(r.toString()));
    // [END ]
  }
}

