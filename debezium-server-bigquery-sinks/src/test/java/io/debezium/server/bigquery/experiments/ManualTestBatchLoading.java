/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery.experiments;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import com.google.common.base.Charsets;

public class ManualTestBatchLoading {

  public static void main(String[] args) throws IOException, InterruptedException {

    BigQuery bigquery = BigQueryOptions.newBuilder()
        .setCredentials(GoogleCredentials.getApplicationDefault())
        .setLocation("EU")
        .build()
        .getService();

    // [START ]
    TableId tableId = TableId.of("stage", "test_json_loading_batch");
    Field[] fields =
        new Field[]{
            Field.of("c_id", LegacySQLTypeName.INTEGER),
            Field.of("c_date", LegacySQLTypeName.INTEGER), // Could not convert non-string JSON value to DATETIME type.
            Field.of("c_datetime", LegacySQLTypeName.INTEGER), // Could not convert non-string JSON value to DATETIME 
            // type.
            Field.of("c_ts", LegacySQLTypeName.TIMESTAMP)// Fails
        };
    // Table schema definition
    Schema schema = Schema.of(fields);
    bigquery.delete(tableId);
    TableInfo tableInfo = TableInfo.newBuilder(tableId, StandardTableDefinition.newBuilder()
        .setSchema(schema)
        .build()).build();
    bigquery.create(tableInfo);
    System.out.println("Created table!");

    WriteChannelConfiguration writeChannelConfiguration =
        WriteChannelConfiguration.newBuilder(tableId)
            .setFormatOptions(FormatOptions.json())
            .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
            .setDestinationTable(tableId)
            .setSchema(schema)
            .build();
    // The location must be specified; other fields can be auto-detected.
    JobId jobId = JobId.newBuilder().setLocation("EU").build();
    TableDataWriteChannel writer = bigquery.writer(jobId, writeChannelConfiguration);
    // Write data to writer
    // FAILS
    String dataMS = "{\"c_id\":1,\"c_date\":3,\"c_datetime\":1662805921200,\"c_ts\":1662805921200}\n";
    //String dataMICROS = "{\"c_id\":1,\"c_date\":3,\"c_datetime\":1662805921200,\"c_ts\":1662805921200}";
    System.out.println("LOADING DATA:\n" + dataMS);
    try {
      writer.write(ByteBuffer.wrap(dataMS.getBytes(Charsets.UTF_8)));
    } finally {
      writer.close();
    }
    // Get load job
    Job job = writer.getJob();
    try {
      job = job.waitFor();
    } catch (BigQueryException be) {
      StringBuilder err = new StringBuilder("Failed to load data:");
      if (be.getErrors() != null) {
        for (BigQueryError ber : be.getErrors()) {
          err.append("\n").append(ber.getMessage());
        }
      }
      System.out.println(err);
    } catch (Exception e) {
      System.out.println(e.getMessage());
      e.printStackTrace();
    }
    JobStatistics.LoadStatistics stats = job.getStatistics();
    System.out.println("Loaded Rows " + stats.getOutputRows());
    TableResult result =
        bigquery.query(QueryJobConfiguration.newBuilder("select * from " + tableId.getDataset() + "." + tableId.getTable()).build());
    result.getValues().forEach(r -> System.out.println(r.toString()));
    // [END ]
    /*
      LOADING ---------
      Row 	c_id	c_json	
      1	"{\"jfield1\": 1111}"
      2	"{\"jfield2\": 22222}"
      SHOULD BE ---------
      Row 	c_id	c_json	
      1	"{"jfield1": 1111}"
      2	"{"jfield2": 22222}"
     */
  }


}
