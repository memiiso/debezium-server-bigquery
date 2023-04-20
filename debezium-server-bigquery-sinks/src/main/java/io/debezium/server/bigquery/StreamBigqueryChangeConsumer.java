/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import io.debezium.DebeziumException;
import io.grpc.Status;
import io.grpc.Status.Code;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.common.annotations.Beta;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.json.JSONArray;

/**
 * Implementation of the consumer that delivers the messages to Bigquery
 *
 * @author Ismail Simsek
 */
@Named("bigquerystream")
@Dependent
@Beta
public class StreamBigqueryChangeConsumer extends AbstractChangeConsumer {
  protected static final ConcurrentHashMap<String, DataWriter> jsonStreamWriters = new ConcurrentHashMap<>();
  private static final int MAX_RETRY_COUNT = 3;
  private static final ImmutableList<Code> RETRIABLE_ERROR_CODES =
      ImmutableList.of(
          Code.INTERNAL,
          Code.ABORTED,
          Code.CANCELLED,
          Code.FAILED_PRECONDITION,
          Code.DEADLINE_EXCEEDED,
          Code.UNAVAILABLE);

  public static BigQueryWriteClient bigQueryWriteClient;
  @ConfigProperty(name = "debezium.sink.batch.destination-regexp", defaultValue = "")
  protected Optional<String> destinationRegexp;
  @ConfigProperty(name = "debezium.sink.batch.destination-regexp-replace", defaultValue = "")
  protected Optional<String> destinationRegexpReplace;
  @Inject
  @ConfigProperty(name = "debezium.sink.bigquerystream.dataset", defaultValue = "")
  Optional<String> bqDataset;
  @ConfigProperty(name = "debezium.sink.bigquerystream.project", defaultValue = "")
  Optional<String> gcpProject;
  @ConfigProperty(name = "debezium.sink.bigquerystream.location", defaultValue = "US")
  String bqLocation;
  @ConfigProperty(name = "debezium.sink.bigquerystream.ignore-unknown-fields", defaultValue = "true")
  Boolean ignoreUnknownFields;
  @ConfigProperty(name = "debezium.sink.bigquerystream.create-if-needed", defaultValue = "true")
  Boolean createIfNeeded;
  @ConfigProperty(name = "debezium.sink.bigquerystream.partition-field", defaultValue = "__ts_ms")
  String partitionField;
  @ConfigProperty(name = "debezium.sink.bigquerystream.clustering-field", defaultValue = "__source_ts_ms")
  String clusteringField;
  @ConfigProperty(name = "debezium.sink.bigquerystream.partition-type", defaultValue = "MONTH")
  String partitionType;
  @ConfigProperty(name = "debezium.sink.bigquerystream.allow-field-addition", defaultValue = "false")
  Boolean allowFieldAddition;
  @ConfigProperty(name = "debezium.sink.bigquerystream.credentials-file", defaultValue = "")
  Optional<String> credentialsFile;
  TimePartitioning timePartitioning;
  BigQuery bqClient;

  @PostConstruct
  void connect() throws InterruptedException {
    this.initizalize();
  }

  @PreDestroy
  void closeStreams() {
    for (Map.Entry<String, DataWriter> sw : jsonStreamWriters.entrySet()) {
      try {
        sw.getValue().close(bigQueryWriteClient);
      } catch (Exception e) {
        e.printStackTrace();
        LOGGER.warn("Exception while closing bigquery stream, destination:" + sw.getKey(), e);
      }
    }
  }

  public void initizalize() throws InterruptedException {
    super.initizalize();

    bqClient = BatchUtil.getBQClient(gcpProject, bqDataset, credentialsFile, bqLocation);

    timePartitioning =
        TimePartitioning.newBuilder(TimePartitioning.Type.valueOf(partitionType)).setField(partitionField).build();

    try {
      BigQueryWriteSettings bigQueryWriteSettings = BigQueryWriteSettings
          .newBuilder()
          .setCredentialsProvider(FixedCredentialsProvider.create(bqClient.getOptions().getCredentials()))
          .build();
      bigQueryWriteClient = BigQueryWriteClient.create(bigQueryWriteSettings);
    } catch (IOException e) {
      throw new DebeziumException("Failed to create BigQuery Write Client", e);
    }
  }

  private DataWriter getDataWriter(Table table) {
    try {
      return new DataWriter(
          TableName.of(table.getTableId().getProject(), table.getTableId().getDataset(), table.getTableId().getTable()),
          bigQueryWriteClient,
          ignoreUnknownFields
      );
    } catch (DescriptorValidationException | IOException | InterruptedException e) {
      throw new DebeziumException("Failed to initialize stream writer for table " + table.getTableId(), e);
    }
  }

  @Override
  public long uploadDestination(String destination, List<DebeziumBigqueryEvent> data) {
    long numRecords = data.size();
    Table table = getTable(destination, data.get(0));
    // get stream writer create if not yet exists!
    DataWriter writer = jsonStreamWriters.computeIfAbsent(destination, k -> getDataWriter(table));
    try {
      JSONArray jsonArr = new JSONArray();
      data.forEach(e -> jsonArr.put(e.valueAsJSONObject()));
      writer.appendSync(jsonArr);
    } catch (DescriptorValidationException | IOException e) {
      throw new DebeziumException("Failed to append data to stream " + writer.streamWriter.getStreamName(), e);
    }
    LOGGER.debug("Appended {} records to {} successfully.", numRecords, destination);
    return numRecords;
  }

  public TableId getTableId(String destination) {
    final String tableName = destination
        .replaceAll(destinationRegexp.orElse(""), destinationRegexpReplace.orElse(""))
        .replace(".", "_");
    return TableId.of(gcpProject.get(), bqDataset.get(), tableName);
  }


  private Table createTable(TableId tableId, Schema schema, Clustering clustering) {

    StandardTableDefinition tableDefinition =
        StandardTableDefinition.newBuilder()
            .setSchema(schema)
            .setTimePartitioning(timePartitioning)
            .setClustering(clustering)
            .build();
    TableInfo tableInfo =
        TableInfo.newBuilder(tableId, tableDefinition).build();
    Table table = bqClient.create(tableInfo);
    LOGGER.warn("Created table {}", table.getTableId());

    // NOTE @TODO ideally we should wait here for streaming cache to update with the new table information 
    // but seems like there is no proper way to wait... 
    // Without wait consumer fails

    return table;
  }

  private Table getTable(String destination, DebeziumBigqueryEvent sampleBqEvent) {
    TableId tableId = getTableId(destination);
    Table table = bqClient.getTable(tableId);
    // create table if missing
    if (createIfNeeded && table == null) {
      table = this.createTable(tableId,
          sampleBqEvent.getBigQuerySchema(true, true),
          sampleBqEvent.getBigQueryClustering(clusteringField)
      );
    }

    // alter table schema add new fields
    if (allowFieldAddition && table != null) {
      table = this.updateTableSchema(table, sampleBqEvent.getBigQuerySchema(true, true), destination);
    }
    return table;
  }

  /**
   * add new fields to table, using event schema.
   *
   * @param table
   * @param updatedSchema
   * @param destination
   * @return Table
   */
  private Table updateTableSchema(Table table, Schema updatedSchema, String destination) {

    List<Field> tableFields = new ArrayList<>(table.getDefinition().getSchema().getFields());
    List<String> tableFieldNames = tableFields.stream().map(Field::getName).collect(Collectors.toList());

    boolean newFieldFound = false;
    StringBuilder newFields = new StringBuilder();
    for (Field field : updatedSchema.getFields()) {
      if (!tableFieldNames.contains(field.getName())) {
        tableFields.add(field);
        newFields.append(field);
        newFieldFound = true;
      }
    }

    if (newFieldFound) {
      LOGGER.warn("Updating table {} with the new fields", table.getTableId());
      Schema newSchema = Schema.of(tableFields);
      final Table updatedTable = table.toBuilder().setDefinition(
          StandardTableDefinition.newBuilder()
              .setSchema(newSchema)
              .build()
      ).build();
      table = updatedTable.update();
      LOGGER.info("New columns {} successfully added to {}, refreshing stream writer...", newFields, table.getTableId());
      jsonStreamWriters.get(destination).close(bigQueryWriteClient);
      jsonStreamWriters.replace(destination, getDataWriter(table));

      LOGGER.info("New columns {} added to {}", newFields, table.getTableId());
    }

    return table;
  }

  protected static class DataWriter {
    private final JsonStreamWriter streamWriter;

    public DataWriter(TableName parentTable, BigQueryWriteClient client, Boolean ignoreUnknownFields)
        throws DescriptorValidationException, IOException, InterruptedException {

      // Use the JSON stream writer to send records in JSON format. Specify the table name to write
      // to the default stream.
      // For more information about JsonStreamWriter, see:
      // https://googleapis.dev/java/google-cloud-bigquerystorage/latest/com/google/cloud/bigquery/storage/v1/JsonStreamWriter.html
      streamWriter = JsonStreamWriter
          .newBuilder(parentTable.toString(), client)
          .setIgnoreUnknownFields(ignoreUnknownFields)
          .build();
    }

    private void appendSync(JSONArray data, int retryCount) throws DescriptorValidationException,
        IOException {
      ApiFuture<AppendRowsResponse> future = streamWriter.append(data);
      try {
        AppendRowsResponse response = future.get();
        if (response.hasError()) {
          throw new DebeziumException("Failed to append data to stream. " + response.getError().getMessage());
        }
      } catch (InterruptedException | ExecutionException throwable) {
        // If the wrapped exception is a StatusRuntimeException, check the state of the operation.
        // If the state is INTERNAL, CANCELLED, or ABORTED, you can retry. For more information,
        // see: https://grpc.github.io/grpc-java/javadoc/io/grpc/StatusRuntimeException.html
        Status status = Status.fromThrowable(throwable);
        if (retryCount < MAX_RETRY_COUNT
            && RETRIABLE_ERROR_CODES.contains(status.getCode())) {
          // Since default stream appends are not ordered, we can simply retry the appends.
          // Retrying with exclusive streams requires more careful consideration.
          this.appendSync(data, ++retryCount);
          // Mark the existing attempt as done since it's being retried.
        } else {
          throw new DebeziumException("Failed to append data to stream " + streamWriter.getStreamName() + "\n" + throwable.getMessage(),
              throwable);
        }
      }

    }

    public void appendSync(JSONArray data) throws DescriptorValidationException, IOException {
      this.appendSync(data, 0);
    }

    public void close(BigQueryWriteClient client) {
      if (streamWriter != null) {
        streamWriter.close();
        client.finalizeWriteStream(streamWriter.getStreamName());
      }
    }
  }

}

