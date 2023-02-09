/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery.offset;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.server.bigquery.BatchUtil;
import io.debezium.util.Strings;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.Future;

import autovalue.shaded.com.google.common.collect.ImmutableList;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.*;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.SafeObjectInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of OffsetBackingStore that saves data to database table.
 */
public class BigqueryOffsetBackingStore extends MemoryOffsetBackingStore implements OffsetBackingStore {

  public static final String OFFSET_STORAGE_TABLE_DDL = "CREATE TABLE %s " +
      "(id STRING NOT NULL, " +
      "offset_data STRING, " +
      "record_insert_ts TIMESTAMP NOT NULL " +
      ")";

  public static final String OFFSET_STORAGE_TABLE_SELECT = "SELECT id, offset_data FROM %s ";

  public static final String OFFSET_STORAGE_TABLE_INSERT = "INSERT INTO %s VALUES ( ?, ?, ? )";

  public static final String OFFSET_STORAGE_TABLE_DELETE = "DELETE FROM %s WHERE 1=1";

  private static final Logger LOG = LoggerFactory.getLogger(BigqueryOffsetBackingStore.class);
  BigQuery bqClient;
  private String tableFullName;
  private TableId tableId;
  BigqueryOffsetBackingStoreConfig config;
  protected static final ObjectMapper mapper = new ObjectMapper();
  protected Map<String, String> data = new HashMap<>();


  public BigqueryOffsetBackingStore() {
  }

  public String getTableFullName() {
    return tableFullName;
  }

  @Override
  public void configure(WorkerConfig config) {
    super.configure(config);
    this.config = new BigqueryOffsetBackingStoreConfig(Configuration.from(config.originalsStrings()));

    try {
      bqClient = BatchUtil.getBQClient(
          Optional.ofNullable(this.config.getBigqueryProject()),
          Optional.ofNullable(this.config.getBigqueryDataset()),
          Optional.ofNullable(this.config.getBigqueryCredentialsFile()),
          this.config.getBigqueryLocation()
      );
      tableFullName = String.format("%s.%s", this.config.getBigqueryDataset(), this.config.getBigqueryTable());
      tableId = TableId.of(this.config.getBigqueryDataset(), this.config.getBigqueryTable());
    } catch (Exception e) {
      throw new IllegalStateException("Failed to connect bigquery offset backing store", e);
    }
  }

  @Override
  public synchronized void start() {
    super.start();

    LOG.info("Starting BigqueryOffsetBackingStore table:{}", this.tableFullName);
    try {
      initializeTable();
    } catch (SQLException e) {
      e.printStackTrace();
      throw new IllegalStateException("Failed to create bigquery offset table:" + this.tableFullName, e);
    }
    load();
  }

  private void initializeTable() throws SQLException {
    Table table = bqClient.getTable(tableId);
    if (table == null) {
      LOG.debug("Creating table {} to store offset", tableFullName);
      BatchUtil.executeQuery(bqClient, String.format(OFFSET_STORAGE_TABLE_DDL, tableFullName));
      LOG.warn("Created offset storage table {} to store offset", tableFullName);
      
      if (!Strings.isNullOrEmpty(config.getMigrateOffsetFile().strip())){
        LOG.warn("Loading offset from file {}", config.getMigrateOffsetFile());
        this.loadFileOffset(new File(config.getMigrateOffsetFile()));
      }
    }
  }

  protected void save() {
    LOG.debug("Saving offset data to bigquery table...");
    try {
      BatchUtil.executeQuery(bqClient, String.format(OFFSET_STORAGE_TABLE_DELETE, tableFullName));
      String dataJson = mapper.writeValueAsString(data);
      LOG.debug("Saving offset data {}", dataJson);
      Timestamp currentTs = new Timestamp(System.currentTimeMillis());
      BatchUtil.executeQuery(bqClient,
          String.format(OFFSET_STORAGE_TABLE_INSERT, tableFullName),
          ImmutableList.of(
              QueryParameterValue.string(UUID.randomUUID().toString()),
              QueryParameterValue.string(dataJson),
              QueryParameterValue.timestamp(String.valueOf(currentTs))
          )
      );
      LOG.debug("Successfully saved offset data to bigquery table");

    } catch (SQLException | JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private void load() {
    try {
      String dataJsonString = null;
      TableResult rs = BatchUtil.executeQuery(bqClient, String.format(OFFSET_STORAGE_TABLE_SELECT, tableFullName));
      for (FieldValueList row : rs.getValues()) {
        dataJsonString = row.get("offset_data").getStringValue();
        break;
      }

      if (dataJsonString != null) {
        this.data = mapper.readValue(dataJsonString, new TypeReference<>() {
        });
        LOG.debug("Loaded offset data {}", dataJsonString);
      }
    } catch (SQLException | JsonProcessingException e) {
      e.printStackTrace();
      LOG.error("Failed load offset data from bigquery", e);
      throw new RuntimeException(e);
    }
  }

  private void loadFileOffset(File file) {
    try (SafeObjectInputStream is = new SafeObjectInputStream(Files.newInputStream(file.toPath()))) {
      Object obj = is.readObject();
      
      if (!(obj instanceof HashMap))
        throw new ConnectException("Expected HashMap but found " + obj.getClass());
      
      Map<byte[], byte[]> raw = (Map<byte[], byte[]>) obj;
      for (Map.Entry<byte[], byte[]> mapEntry : raw.entrySet()) {
        ByteBuffer key = (mapEntry.getKey() != null) ? ByteBuffer.wrap(mapEntry.getKey()) : null;
        ByteBuffer value = (mapEntry.getValue() != null) ? ByteBuffer.wrap(mapEntry.getValue()) : null;
        data.put(fromByteBuffer(key), fromByteBuffer(value));
      }
    } catch (IOException | ClassNotFoundException e) {
      throw new DebeziumException("Failed migrating offset from file", e);
    }
    
    LOG.warn("Loaded file offset, saving it to Bigquery offset storage");
    save();
  }
  @Override
  public Future<Void> set(final Map<ByteBuffer, ByteBuffer> values,
                          final Callback<Void> callback) {
    return executor.submit(() -> {
      for (Map.Entry<ByteBuffer, ByteBuffer> entry : values.entrySet()) {
        if (entry.getKey() == null) {
          continue;
        }
        data.put(fromByteBuffer(entry.getKey()), fromByteBuffer(entry.getValue()));
      }
      save();
      if (callback != null) {
        callback.onCompletion(null, null);
      }
      return null;
    });
  }

  @Override
  public Future<Map<ByteBuffer, ByteBuffer>> get(final Collection<ByteBuffer> keys) {
    return executor.submit(() -> {
      Map<ByteBuffer, ByteBuffer> result = new HashMap<>();
      for (ByteBuffer key : keys) {
        result.put(key, toByteBuffer(data.get(fromByteBuffer(key))));
      }
      return result;
    });
  }

  public String fromByteBuffer(ByteBuffer data) {
    return (data != null) ? String.valueOf(StandardCharsets.UTF_8.decode(data.asReadOnlyBuffer())) : null;
  }

  public ByteBuffer toByteBuffer(String data) {
    return (data != null) ? ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8)) : null;
  }

  public static class BigqueryOffsetBackingStoreConfig extends WorkerConfig {
    private final Configuration config;

    static final Field SINK_TYPE_FIELD = Field.create("debezium.sink.type").required();

    public BigqueryOffsetBackingStoreConfig(Configuration config) {
      super(new ConfigDef(), config.asMap());
      this.config = config;
    }

    public String sinkType() {
      String type = this.config.getString(SINK_TYPE_FIELD);
      if (type == null) {
        throw new DebeziumException("The config property debezium.sink.type is required " +
            "but it could not be found in any config source");
      }
      return type;
    }

    public String getBigqueryProject() {
      return this.config.getString(Field.create(String.format("debezium.sink.%s.project", this.sinkType())));
    }

    public String getBigqueryDataset() {
      return this.config.getString(Field.create(String.format("debezium.sink.%s.dataset", this.sinkType())));
    }

    public String getBigqueryTable() {
      return this.config.getString(Field.create("offset.storage.bigquery.table-name").withDefault("debezium_offset_storage"));
    }

    public String getMigrateOffsetFile() {
      return this.config.getString(Field.create("offset.storage.bigquery.migrate-offset-file").withDefault(""));
    }

    public String getBigqueryCredentialsFile() {
      return this.config.getString(Field.create(String.format("debezium.sink.%s.credentials-file", this.sinkType())).withDefault(""));
    }

    public String getBigqueryLocation() {
      return this.config.getString(Field.create(String.format("debezium.sink.%s.location", this.sinkType())).withDefault("US"));
    }
  }

}
