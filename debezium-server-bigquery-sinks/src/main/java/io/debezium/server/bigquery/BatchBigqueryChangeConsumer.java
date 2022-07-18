/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import io.debezium.DebeziumException;

import java.io.FileInputStream;
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
import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Named;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import org.eclipse.microprofile.config.inject.ConfigProperty;

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
  @ConfigProperty(name = "debezium.sink.bigquerybatch.createDisposition", defaultValue = "CREATE_IF_NEEDED")
  String createDisposition;
  @ConfigProperty(name = "debezium.sink.bigquerybatch.writeDisposition", defaultValue = "WRITE_APPEND")
  String writeDisposition;
  @ConfigProperty(name = "debezium.sink.bigquerybatch.partitionField", defaultValue = "__source_ts")
  String partitionField;
  @ConfigProperty(name = "debezium.sink.bigquerybatch.partitionType", defaultValue = "MONTH")
  String partitionType;
  @ConfigProperty(name = "debezium.sink.bigquerybatch.allowFieldAddition", defaultValue = "true")
  Boolean allowFieldAddition;
  @ConfigProperty(name = "debezium.sink.bigquerybatch.allowFieldRelaxation", defaultValue = "true")
  Boolean allowFieldRelaxation;
  @ConfigProperty(name = "debezium.sink.bigquerybatch.credentialsFile", defaultValue = "")
  Optional<String> credentialsFile;
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

    if (gcpProject.isEmpty()) {
      throw new InterruptedException("Please provide a value for `debezium.sink.bigquerybatch.project`");
    }

    if (bqDataset.isEmpty()) {
      throw new InterruptedException("Please provide a value for `debezium.sink.bigquerybatch.dataset`");
    }

    GoogleCredentials credentials;
    try {
      if (credentialsFile.isPresent()) {
        credentials = GoogleCredentials.fromStream(new FileInputStream(credentialsFile.get()));
      } else {
        credentials = GoogleCredentials.getApplicationDefault();
      }
    } catch (IOException e) {
      throw new DebeziumException("Failed to initialize google credentials", e);
    }

    bqClient = BigQueryOptions.newBuilder()
        .setCredentials(credentials)
        .setProjectId(gcpProject.get())
        .setLocation(bqLocation)
        .setRetrySettings(
            RetrySettings.newBuilder()
                // Set the max number of attempts
                .setMaxAttempts(5)
                // InitialRetryDelay controls the delay before the first retry. 
                // Subsequent retries will use this value adjusted according to the RetryDelayMultiplier. 
                .setInitialRetryDelay(org.threeten.bp.Duration.ofSeconds(5))
                .setMaxRetryDelay(org.threeten.bp.Duration.ofSeconds(60))
                // Set the backoff multiplier
                .setRetryDelayMultiplier(2.0)
                // Set the max duration of all attempts
                .setTotalTimeout(org.threeten.bp.Duration.ofMinutes(5))
                .build()
        )
        .build()
        .getService();

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
  public long uploadDestination(String destination, List<DebeziumBigqueryEvent> data) {

    try {
      Instant start = Instant.now();
      final long numRecords;
      TableId tableId = getTableId(destination);

      DebeziumBigqueryEvent sampleEvent = data.get(0);
      Schema schema = sampleEvent.getBigQuerySchema(castDeletedField);
      Clustering clustering = sampleEvent.getBigQueryClustering();

      // Google BigQuery Configuration for a load operation. A load configuration can be used to load data
      // into a table with a {@link com.google.cloud.WriteChannel}
      WriteChannelConfiguration.Builder wCCBuilder = WriteChannelConfiguration
          .newBuilder(tableId, FormatOptions.json())
          .setWriteDisposition(JobInfo.WriteDisposition.valueOf(writeDisposition))
          .setClustering(clustering)
          .setTimePartitioning(timePartitioning)
          .setSchemaUpdateOptions(schemaUpdateOptions)
          .setCreateDisposition(JobInfo.CreateDisposition.valueOf(createDisposition))
          .setMaxBadRecords(0);

      if (schema != null) {
        LOGGER.trace("Setting schema to: {}", schema);
        wCCBuilder.setSchema(schema);
      }
//        else {
//          wCCBuilder.setAutodetect(true);
//        }

      //WriteChannel implementation to stream data into a BigQuery table. 
      try (TableDataWriteChannel writer = bqClient.writer(wCCBuilder.build())) {
        //Constructs a stream that writes bytes to the given channel.
        try (OutputStream stream = Channels.newOutputStream(writer)) {
          for (DebeziumBigqueryEvent e : data) {
            final JsonNode valNode = e.value();

            if (valNode == null) {
              LOGGER.warn("Null Value received skipping the entry! destination:{} key:{}", destination, getString(e.key()));
              continue;
            }

            final String valData = mapper.writeValueAsString(valNode) + System.lineSeparator();
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
          throw new DebeziumException("BigQuery was unable to load into the table:" + tableId + "." +
              "\nError:" + job.getStatus().getError() +
              "\nJobStatistics:" + jobStatistics +
              "\nBadRecords:" + jobStatistics.getBadRecords() +
              "\nJobStatistics:" + jobStatistics);
        }
      }

      LOGGER.debug("Uploaded {} rows to:{}, upload time:{}, clusteredFields:{}",
          numRecords,
          tableId,
          Duration.between(start, Instant.now()).truncatedTo(ChronoUnit.SECONDS),
          clustering
      );

      return numRecords;

    } catch (BigQueryException | InterruptedException | IOException e) {
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