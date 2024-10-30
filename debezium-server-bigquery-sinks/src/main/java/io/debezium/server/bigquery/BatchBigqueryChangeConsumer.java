/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import com.google.cloud.bigquery.*;
import io.debezium.DebeziumException;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Implementation of the consumer that delivers the messages into Amazon S3 destination.
 *
 * @author Ismail Simsek
 */
@Named("bigquerybatch")
@Dependent
public class BatchBigqueryChangeConsumer<T> extends AbstractChangeConsumer {

  @ConfigProperty(name = "debezium.sink.batch.destination-regexp", defaultValue = "")
  protected Optional<String> destinationRegexp;
  @ConfigProperty(name = "debezium.sink.batch.destination-regexp-replace", defaultValue = "")
  protected Optional<String> destinationRegexpReplace;
  @Inject
  @ConfigProperty(name = "debezium.sink.bigquerybatch.dataset", defaultValue = "")
  Optional<String> bqDataset;
  @ConfigProperty(name = "debezium.sink.bigquerybatch.location", defaultValue = "US")
  String bqLocation;
  @ConfigProperty(name = "debezium.sink.bigquerybatch.project", defaultValue = "")
  Optional<String> gcpProject;
  @ConfigProperty(name = "debezium.sink.bigquerybatch.create-disposition", defaultValue = "CREATE_IF_NEEDED")
  String createDisposition;
  @ConfigProperty(name = "debezium.sink.bigquerybatch.writeDisposition", defaultValue = "WRITE_APPEND")
  String writeDisposition;
  @ConfigProperty(name = "debezium.sink.bigquerybatch.partition-field", defaultValue = "__ts_ms")
  String partitionField;
  @ConfigProperty(name = "debezium.sink.bigquerybatch.clustering-field", defaultValue = "__source_ts_ms")
  String clusteringField;
  @ConfigProperty(name = "debezium.sink.bigquerybatch.partition-type", defaultValue = "MONTH")
  String partitionType;
  @ConfigProperty(name = "debezium.sink.bigquerybatch.allow-field-addition", defaultValue = "true")
  Boolean allowFieldAddition;
  @ConfigProperty(name = "debezium.sink.bigquerybatch.allow-field-relaxation", defaultValue = "true")
  Boolean allowFieldRelaxation;
  @ConfigProperty(name = "debezium.sink.bigquerybatch.credentials-file", defaultValue = "")
  Optional<String> credentialsFile;
  @ConfigProperty(name = "debezium.sink.bigquerybatch.bigquery-custom-host", defaultValue = "")
  Optional<String> bigQueryCustomHost;
  @ConfigProperty(name = "debezium.sink.bigquerybatch.bigquery-dev-emulator", defaultValue = "false")
  Boolean isBigqueryDevEmulator;
  @ConfigProperty(name = "debezium.sink.bigquerybatch.cast-deleted-field", defaultValue = "false")
  Boolean castDeletedField;

  BigQuery bqClient;
  TimePartitioning timePartitioning;
  final List<JobInfo.SchemaUpdateOption> schemaUpdateOptions = new ArrayList<>();

  @PostConstruct
  void connect() throws InterruptedException {
    this.initizalize();
  }

  public void initizalize() throws InterruptedException {
    super.initizalize();
    bqClient = ConsumerUtil.bigqueryClient(isBigqueryDevEmulator, gcpProject, bqDataset, credentialsFile, bqLocation, bigQueryCustomHost);
    timePartitioning =
        TimePartitioning.newBuilder(TimePartitioning.Type.valueOf(partitionType)).setField(partitionField).build();

    if (allowFieldAddition) {
      schemaUpdateOptions.add(JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION);
    }
    if (allowFieldRelaxation) {
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
      Schema schema = sampleEvent.tableSchema(false);
      if (schema == null) {
        schema = bqClient.getTable(tableId).getDefinition().getSchema();
      }

      Clustering clustering = sampleEvent.tableClustering(clusteringField);

      // Google BigQuery Configuration for a load operation. A load configuration can be used to load data
      // into a table with a {@link com.google.cloud.WriteChannel}
      WriteChannelConfiguration.Builder wCCBuilder = WriteChannelConfiguration
          .newBuilder(tableId, FormatOptions.json())
          .setWriteDisposition(JobInfo.WriteDisposition.valueOf(writeDisposition))
          .setClustering(clustering)
          .setSchema(schema)
          .setTimePartitioning(timePartitioning)
          .setSchemaUpdateOptions(schemaUpdateOptions)
          .setCreateDisposition(JobInfo.CreateDisposition.valueOf(createDisposition))
          .setMaxBadRecords(0);

      //WriteChannel implementation to stream data into a BigQuery table. 
      try (TableDataWriteChannel writer = bqClient.writer(wCCBuilder.build())) {
        //Constructs a stream that writes bytes to the given channel.
        try (OutputStream stream = Channels.newOutputStream(writer)) {
          for (RecordConverter e : data) {

            final String val = e.valueAsJsonLine(schema);

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
      StringBuilder err = new StringBuilder("Failed to load data:");
      if (be.getErrors() != null) {
        for (BigQueryError ber : be.getErrors()) {
          err.append("\n").append(ber.getMessage());
        }
      }
      throw new DebeziumException(err.toString(), be);
    } catch (InterruptedException | IOException e) {
      e.printStackTrace();
      throw new DebeziumException(e);
    }
  }

  TableId getTableId(String destination) {
    final String tableName = destination
        .replaceAll(destinationRegexp.orElse(""), destinationRegexpReplace.orElse(""))
        .replace(".", "_");
    return TableId.of(gcpProject.get(), bqDataset.get(), tableName);
  }

}