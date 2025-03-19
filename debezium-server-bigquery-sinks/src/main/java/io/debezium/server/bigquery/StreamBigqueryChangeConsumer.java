/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableConstraints;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.common.annotations.Beta;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of a Debezium change consumer that delivers events to BigQuery tables in a streaming manner.
 * <p>This class extends the `BaseChangeConsumer` and provides functionality for streaming batches of
 * Debezium change events to BigQuery tables using the BigQuery Write API. It offers features like
 * upsert, deduplication, and table schema management.
 *
 * @author Ismail Simsek
 */

@Named("bigquerystream")
@Dependent
@Beta
public class StreamBigqueryChangeConsumer extends BaseChangeConsumer {
  protected static final ConcurrentHashMap<String, StreamDataWriter> jsonStreamWriters = new ConcurrentHashMap<>();
  static final ImmutableMap<String, Integer> cdcOperations = ImmutableMap.of("c", 1, "r", 2, "u", 3, "d", 4);
  public static BigQueryWriteClient bigQueryWriteClient;
  TimePartitioning timePartitioning;
  BigQuery bqClient;

  @Inject
  StreamConsumerConfig config;

  @PostConstruct
  void connect() throws InterruptedException {
    this.initialize();
  }

  @PreDestroy
  void closeStreams() {
    for (Map.Entry<String, StreamDataWriter> sw : jsonStreamWriters.entrySet()) {
      closeStreamWriter(sw.getValue(), sw.getKey());
    }
    if (bigQueryWriteClient != null) {
      try {
        bigQueryWriteClient.close();
      } catch (Exception e) {
        e.printStackTrace();
        LOGGER.warn("Exception while closing BigQueryWriteClient", e);
      }
    }
  }

  public void initialize() throws InterruptedException {
    super.initialize();

    bqClient = ConsumerUtil.bigqueryClient(config.isBigqueryDevEmulator(), config.gcpProject(), config.bqDataset(), config.credentialsFile(), config.bqLocation(), config.bigQueryCustomHost());
    timePartitioning =
        TimePartitioning.newBuilder(TimePartitioning.Type.valueOf(config.partitionType())).setField(config.partitionField()).build();
    try {
      BigQueryWriteSettings bigQueryWriteSettings = ConsumerUtil.bigQueryWriteSettings(config.isBigqueryDevEmulator(), bqClient, config.bigQueryCustomGRPCHost());
      bigQueryWriteClient = BigQueryWriteClient.create(bigQueryWriteSettings);
    } catch (IOException e) {
      throw new DebeziumException("Failed to create BigQuery Write Client", e);
    }
  }

  private void closeStreamWriter(StreamDataWriter writer, String destination) {
    try {
      writer.close(bigQueryWriteClient);
    } catch (Exception e) {
      e.printStackTrace();
      LOGGER.warn("Exception while closing bigquery stream, destination:" + destination, e);
    }
  }

  private StreamDataWriter getDataWriter(Table table) {
    try {
      final String streamOrTableName;
      if (config.isBigqueryDevEmulator()) {
        // Workaround!! for emulator https://github.com/goccy/bigquery-emulator/issues/342#issuecomment-2581118253
        streamOrTableName = String.format("projects/%s/datasets/%s/tables/%s/streams/_default",
            table.getTableId().getProject(), table.getTableId().getDataset(), table.getTableId().getTable()
        );
      } else {
        TableName tableName = TableName.of(table.getTableId().getProject(), table.getTableId().getDataset(), table.getTableId().getTable());
        streamOrTableName = tableName.toString();
      }

      StreamDataWriter writer = new StreamDataWriter(
          streamOrTableName,
          bigQueryWriteClient,
          config.ignoreUnknownFields()
      );
      writer.initialize();
      return writer;
    } catch (DescriptorValidationException | IOException | InterruptedException e) {
      throw new DebeziumException("Failed to initialize stream writer for table " + table.getGeneratedId(), e);
    }
  }

  boolean doTableHasPrimaryKey(Table table) {
    if (table.getTableConstraints() == null) {
      return false;
    }

    return table.getTableConstraints().getPrimaryKey() != null;

  }

  @Override
  public long uploadDestination(String destination, List<RecordConverter> data) {
    long numRecords = data.size();
    Table table = getTable(destination, data.get(0));
    // get stream writer create if not yet exists!
    StreamDataWriter writer = jsonStreamWriters.computeIfAbsent(destination, k -> getDataWriter(table));
    try {
      // running with upsert mode deduplicate data! for the tables having Primary Key
      // for the tables without primary key run append mode
      // Otherwise it throws Exception
      // INVALID_ARGUMENT:Create UPSERT stream is not supported for primary key disabled table: xyz
      final boolean tableHasPrimaryKey = doTableHasPrimaryKey(table);
      final boolean doUpsert = config.upsert() && tableHasPrimaryKey;

      if (doUpsert) {
        data = deduplicateBatch(data);
      }
      // add data to JSONArray
      JSONArray jsonArr = new JSONArray();
      data.forEach(e -> {
        JSONObject val = e.convert(table.getDefinition().getSchema(), doUpsert, config.upsertKeepDeletes());
        jsonArr.put(val);
      });
      writer.appendSync(jsonArr);
    } catch (DescriptorValidationException | IOException e) {
      throw new DebeziumException("Failed to append data to stream " + writer.streamWriter.getStreamName(), e);
    }
    LOGGER.debug("Added {} records to {} successfully.", numRecords, destination);
    return numRecords;
  }


  protected List<RecordConverter> deduplicateBatch(List<RecordConverter> events) {

    ConcurrentHashMap<JsonNode, RecordConverter> deduplicatedEvents = new ConcurrentHashMap<>();

    events.forEach(e ->
        // deduplicate using key(PK)
        deduplicatedEvents.merge(e.key(), e, (oldValue, newValue) -> {
          if (this.compareByTsThenOp(oldValue.value(), newValue.value()) <= 0) {
            return newValue;
          } else {
            return oldValue;
          }
        })
    );

    return new ArrayList<>(deduplicatedEvents.values());
  }

  /**
   * This is used to deduplicate events within given batch.
   * <p>
   * Forex ample a record can be updated multiple times in the source. for example insert followed by update and
   * delete. for this case we need to only pick last change event for the row.
   * <p>
   * Its used when `upsert` feature enabled (when the consumer operating non append mode) which means it should not add
   * duplicate records to target table.
   *
   * @param lhs
   * @param rhs
   * @return
   */
  private int compareByTsThenOp(JsonNode lhs, JsonNode rhs) {

    int result = Long.compare(lhs.get(config.sourceTsMsColumn()).asLong(0), rhs.get(config.sourceTsMsColumn()).asLong(0));

    if (result == 0) {
      // return (x < y) ? -1 : ((x == y) ? 0 : 1);
      result = cdcOperations.getOrDefault(lhs.get(config.opColumn()).asText("c"), -1)
          .compareTo(
              cdcOperations.getOrDefault(rhs.get(config.opColumn()).asText("c"), -1)
          );
    }

    return result;
  }

  public TableId getTableId(String destination) {
    final String tableName = destination
        .replaceAll(config.common().destinationRegexp().orElse(""), config.common().destinationRegexpReplace().orElse(""))
        .replace(".", "_");
    return TableId.of(config.gcpProject().get(), config.bqDataset().get(), tableName);
  }

  // create table if not exists
  private Table createTable(TableId tableId, Schema schema, Clustering clustering, TableConstraints tableConstraints) {

    StandardTableDefinition tableDefinition =
        StandardTableDefinition.newBuilder()
            .setSchema(schema)
            .setTimePartitioning(timePartitioning)
            .setClustering(clustering)
            .setTableConstraints(tableConstraints)
            .build();
    TableInfo tableInfo =
        TableInfo.newBuilder(tableId, tableDefinition).build();
    Table table = bqClient.create(tableInfo);
    LOGGER.warn("Created table {} PK {}", table.getGeneratedId(), tableConstraints.getPrimaryKey());
    // NOTE @TODO ideally we should wait here for streaming cache to update with the new table information 
    // but seems like there is no proper way to wait... 
    // Without wait consumer fails

    return table;
  }

  // get table, create if not exists, update schema if needed
  private Table getTable(String destination, RecordConverter sampleBqEvent) {
    TableId tableId = getTableId(destination);
    Table table = bqClient.getTable(tableId);
    // create table if missing
    if (config.createIfNeeded() && table == null) {
      table = this.createTable(tableId,
          sampleBqEvent.tableSchema(),
          sampleBqEvent.tableClustering(config.clusteringField()),
          sampleBqEvent.tableConstraints()
      );
    }

    // alter table schema add new fields
    if (config.allowFieldAddition() && table != null) {
      table = this.updateTableSchema(table, sampleBqEvent.tableSchema(), destination);
    }
    return table;
  }

  /**
   * Updates the table schema by adding new fields from the updated schema.
   *
   * @param table       The existing BigQuery table.
   * @param updatedSchema The schema containing potential new fields.
   * @param destination The destination table name.
   * @return The updated BigQuery table.
   */
  private Table updateTableSchema(Table table, Schema updatedSchema, String destination) {
    Schema currentSchema = table.getDefinition().getSchema();
    List<Field> tableFields = new ArrayList<>(currentSchema.getFields());
    List<String> currentFieldNames = currentSchema.getFields().stream().map(Field::getName).toList();
    List<Field> newFields = new ArrayList<>();

    for (Field field : updatedSchema.getFields()) {
      if (!currentFieldNames.contains(field.getName())) {
        tableFields.add(field);
        newFields.add(field);
      }
    }

    if (!newFields.isEmpty()) {
      LOGGER.warn("Adding new fields to table {} fields:{}", table.getGeneratedId(), newFields);
      Schema newSchema = Schema.of(tableFields);
      TableDefinition newDefinition = table.getDefinition().toBuilder().setSchema(newSchema).build();
      table = table.toBuilder().setDefinition(newDefinition).build().update();
      LOGGER.info("New fields successfully added to table {}", table.getGeneratedId());
      closeStreamWriter(jsonStreamWriters.get(destination), destination);
      jsonStreamWriters.replace(destination, getDataWriter(table));
      LOGGER.info("Stream writer of the table is updated with the new schema");
    }

    return table;
  }


  public RecordConverter eventAsRecordConverter(ChangeEvent<Object, Object> e) throws IOException {
    return new StreamRecordConverter(e.destination(),
        valDeserializer.deserialize(e.destination(), getBytes(e.value())),
        e.key() == null ? null : keyDeserializer.deserialize(e.destination(), getBytes(e.key())),
        mapper.readTree(getBytes(e.value())).get("schema"),
        e.key() == null ? null : mapper.readTree(getBytes(e.key())).get("schema"),
        debeziumConfig
    );
  }

}

