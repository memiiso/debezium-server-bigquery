/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery.history;

import io.debezium.DebeziumException;
import io.debezium.annotation.ThreadSafe;
import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.*;
import io.debezium.server.bigquery.BatchUtil;
import io.debezium.util.Collect;
import io.debezium.util.FunctionalReadWriteLock;
import io.debezium.util.Strings;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import autovalue.shaded.com.google.common.collect.ImmutableList;
import com.google.cloud.bigquery.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DatabaseHistory} implementation that stores the schema history to database table
 *
 * @author Ismail Simsek
 */
@ThreadSafe
@Incubating
public final class BigquerySchemaHistory extends AbstractDatabaseHistory {

  private static final Logger LOG = LoggerFactory.getLogger(BigquerySchemaHistory.class);

  public static final String DATABASE_HISTORY_TABLE_DDL = "CREATE TABLE IF NOT EXISTS %s " +
      "(id STRING NOT NULL, " +
      "history_data STRING, " +
      "record_insert_ts TIMESTAMP NOT NULL " +
      ")";

  public static final String DATABASE_HISTORY_STORAGE_TABLE_INSERT = "INSERT INTO %s VALUES ( ?, ?, ? )";
  public static final String DATABASE_HISTORY_STORAGE_TABLE_SELECT = "SELECT id, history_data, record_insert_ts FROM %s ORDER BY " +
      "record_insert_ts ASC";

  static final Field SINK_TYPE_FIELD = Field.create("debezium.sink.type").required();
  public static Collection<Field> ALL_FIELDS = Collect.arrayListOf(SINK_TYPE_FIELD);

  private final FunctionalReadWriteLock lock = FunctionalReadWriteLock.reentrant();
  private final DocumentWriter writer = DocumentWriter.defaultWriter();
  private final DocumentReader reader = DocumentReader.defaultReader();
  private final AtomicBoolean running = new AtomicBoolean();
  BigquerySchemaHistoryConfig config;
  BigQuery bqClient;
  private String tableFullName;
  private TableId tableId;

  @Override
  public void configure(Configuration config, HistoryRecordComparator comparator, DatabaseHistoryListener listener, boolean useCatalogBeforeSchema) {

    super.configure(config, comparator, listener, useCatalogBeforeSchema);
    this.config = new BigquerySchemaHistoryConfig(config);
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
      throw new DatabaseHistoryException("Failed to connect bigquery database history backing store", e);
    }

    if (running.get()) {
      throw new DatabaseHistoryException("Bigquery database history process already initialized table: " + tableFullName);
    }
  }

  @Override
  public void start() {
    super.start();
    lock.write(() -> {
      if (running.compareAndSet(false, true)) {
        try {
          if (!storageExists()) {
            initializeStorage();
          }
        } catch (Exception e) {
          throw new DatabaseHistoryException("Unable to create history table: " + tableFullName + " : " + e.getMessage(),
              e);
        }
      }
    });
  }

  public String getTableFullName() {
    return tableFullName;
  }

  @Override
  protected void storeRecord(HistoryRecord record) throws DatabaseHistoryException {
    if (record == null) {
      return;
    }
    lock.write(() -> {
      if (!running.get()) {
        throw new DebeziumException("The history has been stopped and will not accept more records");
      }
      try {
        String recordDocString = writer.write(record.document());
        LOG.trace("Saving history data {}", recordDocString);
        Timestamp currentTs = new Timestamp(System.currentTimeMillis());
        BatchUtil.executeQuery(bqClient,
            String.format(DATABASE_HISTORY_STORAGE_TABLE_INSERT, tableFullName),
            ImmutableList.of(
                QueryParameterValue.string(UUID.randomUUID().toString()),
                QueryParameterValue.string(recordDocString),
                QueryParameterValue.timestamp(String.valueOf(currentTs))
            )
        );
        LOG.trace("Successfully saved history data to bigquery table");
      } catch (IOException | SQLException e) {
        throw new DatabaseHistoryException("Failed to store record: " + record, e);
      }
    });
  }

  @Override
  public void stop() {
    running.set(false);
    super.stop();
  }

  @Override
  protected synchronized void recoverRecords(Consumer<HistoryRecord> records) {
    lock.write(() -> {
      try {
        if (exists()) {
          TableResult rs = BatchUtil.executeQuery(bqClient, String.format(DATABASE_HISTORY_STORAGE_TABLE_SELECT, tableFullName));
          for (FieldValueList row : rs.getValues()) {
            String line = row.get("history_data").getStringValue();
            if (line == null) {
              break;
            }
            if (!line.isEmpty()) {
              records.accept(new HistoryRecord(reader.read(line)));
            }
          }
        }
      } catch (IOException | SQLException e) {
        throw new DatabaseHistoryException("Failed to recover records", e);
      }
    });
  }

  @Override
  public boolean storageExists() {
    Table table = bqClient.getTable(tableId);
    return table != null;
  }

  @Override
  public boolean exists() {

    if (!storageExists()) {
      return false;
    }

    int numRows = 0;
    try {
      TableResult rs = BatchUtil.executeQuery(bqClient, "SELECT COUNT(*) as row_count FROM " + tableFullName);
      for (FieldValueList row : rs.getValues()) {
        numRows = row.get("row_count").getNumericValue().intValue();
        break;
      }
    } catch (SQLException e) {
      throw new DatabaseHistoryException("Failed to check database history storage", e);
    }
    return numRows > 0;
  }

  @Override
  public String toString() {
    return "Bigquery database history storage: " + (tableFullName != null ? tableFullName : "(unstarted)");
  }

  @Override
  public void initializeStorage() {
    if (!storageExists()) {
      try {
        LOG.debug("Creating table {} to store database history", tableFullName);
        BatchUtil.executeQuery(bqClient, String.format(DATABASE_HISTORY_TABLE_DDL, tableFullName));
        LOG.warn("Created database history storage table {} to store history", tableFullName);

        if (!Strings.isNullOrEmpty(config.getMigrateHistoryFile().strip())) {
          LOG.warn("Migrating history from file {}", config.getMigrateHistoryFile());
          this.loadFileDatabaseHistory(new File(config.getMigrateHistoryFile()));
        }
      } catch (Exception e) {
        throw new DatabaseHistoryException("Creation of database history topic failed, please create the topic manually", e);
      }
    } else {
      LOG.debug("Storage is exists, skipping initialization");
    }
  }

  private void loadFileDatabaseHistory(File file) {
    LOG.warn(String.format("Migrating file database history from:'%s' to Bigquery database history storage: %s",
        file.toPath(), tableFullName));
    AtomicInteger numRecords = new AtomicInteger();
    lock.write(() -> {
      try (BufferedReader historyReader = Files.newBufferedReader(file.toPath())) {
        while (true) {
          String line = historyReader.readLine();
          if (line == null) {
            break;
          }
          if (!line.isEmpty()) {
            this.storeRecord(new HistoryRecord(reader.read(line)));
            numRecords.getAndIncrement();
          }
        }
      } catch (IOException e) {
        logger.error("Failed to migrate history record from history file at {}", file.toPath(), e);
      }
    });
    LOG.warn("Migrated {} database history record. " +
        "Migrating file database history to Bigquery database history storage successfully completed", numRecords.get());
  }

  public static class BigquerySchemaHistoryConfig {
    private final Configuration config;

    public BigquerySchemaHistoryConfig(Configuration config) {

      if (!config.validateAndRecord(ALL_FIELDS, LOG::error)) {
        throw new DatabaseHistoryException(
            "Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
      }
      config.validateAndRecord(ALL_FIELDS, LOG::error);

      this.config = config;
    }

    public String sinkType() {
      String type = this.config.getString(SINK_TYPE_FIELD);
      if (type == null) {
        throw new DatabaseHistoryException("The config property debezium.sink.type is required " +
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
      return this.config.getString(Field.create("database.history.bigquery.table-name").withDefault(
          "debezium_database_history_storage"));
    }

    public String getMigrateHistoryFile() {
      return this.config.getString(Field.create("database.history.bigquery.migrate-history-file").withDefault(""));
    }

    public String getBigqueryCredentialsFile() {
      return this.config.getString(Field.create(String.format("debezium.sink.%s.credentials-file", this.sinkType())).withDefault(""));
    }

    public String getBigqueryLocation() {
      return this.config.getString(Field.create(String.format("debezium.sink.%s.location", this.sinkType())).withDefault("US"));
    }
  }

}
