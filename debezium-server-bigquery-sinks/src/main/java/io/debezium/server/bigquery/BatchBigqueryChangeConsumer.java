/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of a Debezium change consumer that delivers batches of events to BigQuery tables.
 * <p>This class extends the `BaseChangeConsumer` and provides functionality for uploading batches of
 * Debezium change events to BigQuery tables. It leverages the BigQuery Java client library
 * to perform data loading and table management tasks.
 *
 * @author Ismail Simsek
 */

@Named("bigquerybatch")
@Dependent
public class BatchBigqueryChangeConsumer<T> extends BaseChangeConsumer {

  BigQuery bqClient;
  TimePartitioning timePartitioning;
  final List<JobInfo.SchemaUpdateOption> schemaUpdateOptions = new ArrayList<>();

  @Inject
  BatchConsumerConfig config;

  @PostConstruct
  void connect() throws InterruptedException {
    this.initialize();
  }

  public void initialize() throws InterruptedException {
    super.initialize();
    bqClient = ConsumerUtil.bigqueryClient(config.isBigqueryDevEmulator(), config.gcpProject(), config.bqDataset(), config.credentialsFile(), config.bqLocation(), config.bigQueryCustomHost());
    timePartitioning =
        TimePartitioning.newBuilder(TimePartitioning.Type.valueOf(config.partitionType())).setField(config.partitionField()).build();

    if (config.allowFieldAddition()) {
      schemaUpdateOptions.add(JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION);
    }
    if (config.allowFieldRelaxation()) {
      schemaUpdateOptions.add(JobInfo.SchemaUpdateOption.ALLOW_FIELD_RELAXATION);
    }

  }

  @Override
  public long uploadDestination(String destination, List<RecordConverter> data) {

    try {
      Instant start = Instant.now();
      final long numRecords;
      TableId tableId = getTableId(destination);

      RecordConverter sampleEvent = data.get(0);
      Schema schema = sampleEvent.tableSchema();
      if (schema == null) {
        schema = bqClient.getTable(tableId).getDefinition().getSchema();
      }

      Clustering clustering = sampleEvent.tableClustering(config.clusteringField());

      // Google BigQuery Configuration for a load operation. A load configuration can be used to load data
      // into a table with a {@link com.google.cloud.WriteChannel}
      WriteChannelConfiguration.Builder wCCBuilder = WriteChannelConfiguration
          .newBuilder(tableId, FormatOptions.json())
          .setWriteDisposition((config.writeDisposition()))
          .setSchema(schema)
          .setSchemaUpdateOptions(schemaUpdateOptions)
          .setCreateDisposition(config.createDisposition())
          .setMaxBadRecords(0)
          .setCreateSession(true);

      if (schemaContainsField(schema, timePartitioning.getField())) {
        wCCBuilder.setTimePartitioning(timePartitioning);
      }

      if (clustering != null && clustering.getFields() != null && !clustering.getFields().isEmpty()) {
        wCCBuilder.setClustering(clustering);
      }

      //WriteChannel implementation to stream data into a BigQuery table. 
      try (TableDataWriteChannel writer = bqClient.writer(wCCBuilder.build())) {
        //Constructs a stream that writes bytes to the given channel.
        try (OutputStream stream = Channels.newOutputStream(writer)) {
          for (RecordConverter e : data) {

            final String val = e.convert(schema);

            if (val == null) {
              LOGGER.warn("Null Value received skipping the entry! destination:{} key:{}", destination, getString(e.key()));
              continue;
            }

            final String valData = val + System.lineSeparator();
            stream.write(valData.getBytes(StandardCharsets.UTF_8));
          }
        }
        Job job = writer.getJob().waitFor();
        JobStatistics.LoadStatistics jobStatistics = job.getStatistics();
        numRecords = jobStatistics.getOutputRows();

        if (job.isDone()) {
          LOGGER.debug("Data successfully loaded to {}. rows: {}, jobStatistics: {}", tableId, numRecords,
              jobStatistics);
        } else {
          throw new DebeziumException("Failed to load table: " + tableId + "!" +
              " Error:" + job.getStatus().getError() +
              ", JobStatistics:" + jobStatistics +
              ", BadRecords:" + jobStatistics.getBadRecords() +
              ", JobStatistics:" + jobStatistics);
        }
      }

      LOGGER.debug("Uploaded {} rows to:{}, upload time:{}, clusteredFields:{}",
          numRecords,
          tableId,
          Duration.between(start, Instant.now()).truncatedTo(ChronoUnit.SECONDS),
          clustering
      );

      return numRecords;

    } catch (BigQueryException be) {
      StringBuilder err = new StringBuilder("Failed to load data: ");
      if (be.getErrors() == null || be.getErrors().isEmpty()) {
        err.append(be.getMessage());
      } else {
        for (BigQueryError ber : be.getErrors()) {
          err.append("\n\tReason: ").append(ber.getReason());
          err.append("\n\tLocation: ").append(ber.getLocation());
          err.append("\n\tMessage: ").append(ber.getMessage());
        }
      }
      throw new DebeziumException(err.toString(), be);

    } catch (InterruptedException | IOException e) {
      throw new DebeziumException(e);
    }
  }

  TableId getTableId(String destination) {
    final String tableName = destination
        .replaceAll(this.config.common().destinationRegexp().orElse(""), this.config.common().destinationRegexpReplace().orElse(""))
        .replace(".", "_");
    return TableId.of(config.gcpProject().get(), config.bqDataset().get(), tableName);
  }

  public RecordConverter eventAsRecordConverter(ChangeEvent<Object, Object> e) throws IOException {
    return new BatchRecordConverter(e.destination(),
        valDeserializer.deserialize(e.destination(), getBytes(e.value())),
        e.key() == null ? null : keyDeserializer.deserialize(e.destination(), getBytes(e.key())),
        mapper.readTree(getBytes(e.value())).get("schema"),
        e.key() == null ? null : mapper.readTree(getBytes(e.key())).get("schema"),
        debeziumConfig
    );
  }

}