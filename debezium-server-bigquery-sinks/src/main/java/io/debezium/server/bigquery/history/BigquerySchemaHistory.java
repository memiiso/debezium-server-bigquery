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
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.*;
import io.debezium.server.bigquery.BatchUtil;
import io.debezium.util.FunctionalReadWriteLock;
import io.debezium.util.Strings;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.SQLException;
import java.sql.Timestamp;
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
 * A {@link SchemaHistory} implementation that stores the schema history to database table
 *
 * @author Ismail Simsek
 */
@ThreadSafe
@Incubating
public final class BigquerySchemaHistory extends AbstractSchemaHistory {

  private static final Logger LOG = LoggerFactory.getLogger(BigquerySchemaHistory.class);

  public static final String DATABASE_HISTORY_TABLE_DDL = "CREATE TABLE IF NOT EXISTS %s " +
      "(id STRING NOT NULL, " +
      "history_data STRING, " +
      "record_insert_ts TIMESTAMP NOT NULL " +
      ")";

  public static final String DATABASE_HISTORY_STORAGE_TABLE_INSERT = "INSERT INTO %s VALUES ( ?, ?, ? )";
  public static final String DATABASE_HISTORY_STORAGE_TABLE_SELECT = "SELECT id, history_data, record_insert_ts FROM %s ORDER BY " +
      "record_insert_ts ASC";
  private final FunctionalReadWriteLock lock = FunctionalReadWriteLock.reentrant();
  private final DocumentWriter writer = DocumentWriter.defaultWriter();
  private final DocumentReader reader = DocumentReader.defaultReader();
  private final AtomicBoolean running = new AtomicBoolean();
  BigquerySchemaHistoryConfig historyConfig;
  BigQuery bqClient;
  private String tableFullName;
  private TableId tableId;

  @Override
  public void configure(Configuration config, HistoryRecordComparator comparator, SchemaHistoryListener listener, boolean useCatalogBeforeSchema) {

    super.configure(config, comparator, listener, useCatalogBeforeSchema);
    this.historyConfig = new BigquerySchemaHistoryConfig(config);
    try {
      bqClient = BatchUtil.getBQClient(
          Optional.ofNullable(this.historyConfig.getBigqueryProject()),
          Optional.ofNullable(this.historyConfig.getBigqueryDataset()),
          Optional.ofNullable(this.historyConfig.getBigqueryCredentialsFile()),
          this.historyConfig.getBigqueryLocation()
      );
      tableFullName = String.format("%s.%s", this.historyConfig.getBigqueryDataset(), this.historyConfig.getBigqueryTable());
      tableId = TableId.of(this.historyConfig.getBigqueryDataset(), this.historyConfig.getBigqueryTable());
    } catch (Exception e) {
      throw new SchemaHistoryException("Failed to connect bigquery database history backing store", e);
    }

    if (running.get()) {
      throw new SchemaHistoryException("Bigquery database history process already initialized table: " + tableFullName);
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
          throw new SchemaHistoryException("Unable to create history table: " + tableFullName + " : " + e.getMessage(),
              e);
        }
      }
    });
  }

  public String getTableFullName() {
    return tableFullName;
  }

  @Override
  protected void storeRecord(HistoryRecord record) throws SchemaHistoryException {
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
        throw new SchemaHistoryException("Failed to store record: " + record, e);
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
        throw new SchemaHistoryException("Failed to recover records", e);
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
      throw new SchemaHistoryException("Failed to check database history storage", e);
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

        if (!Strings.isNullOrEmpty(historyConfig.getMigrateHistoryFile().strip())) {
          LOG.warn("Migrating history from file {}", historyConfig.getMigrateHistoryFile());
          this.loadFileSchemaHistory(new File(historyConfig.getMigrateHistoryFile()));
        }
      } catch (Exception e) {
        throw new SchemaHistoryException("Creation of database history topic failed, please create the topic manually", e);
      }
    } else {
      LOG.debug("Storage is exists, skipping initialization");
    }
  }

  private void loadFileSchemaHistory(File file) {
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
      this.config = config;
    }

    private String getConfig(String configName, String fallbackConfigName, String defaultValue) {
      return this.config.getString(configName, this.config.getString(fallbackConfigName, defaultValue));
    }

    public String getBigqueryProject() {
      return getConfig(CONFIGURATION_FIELD_PREFIX_STRING + "bigquerybatch.project",
          CONFIGURATION_FIELD_PREFIX_STRING + "bigquerystream.project", null);
    }

    public String getBigqueryDataset() {
      return getConfig(CONFIGURATION_FIELD_PREFIX_STRING + "bigquerybatch.dataset",
          CONFIGURATION_FIELD_PREFIX_STRING + "bigquerystream.dataset", null);
    }

    public String getBigqueryTable() {
      return getConfig(CONFIGURATION_FIELD_PREFIX_STRING + "bigquery.table-name",
          CONFIGURATION_FIELD_PREFIX_STRING + "bigquerystream.table-name", "debezium_database_history_storage"
      );
    }

    public String getMigrateHistoryFile() {
      return this.getConfig(CONFIGURATION_FIELD_PREFIX_STRING + "bigquery.migrate-history-file",
          CONFIGURATION_FIELD_PREFIX_STRING + "bigquerystream.migrate-history-file", "");
    }

    public String getBigqueryCredentialsFile() {
      return this.getConfig(CONFIGURATION_FIELD_PREFIX_STRING + "bigquerybatch.credentialsFile",
          CONFIGURATION_FIELD_PREFIX_STRING + "bigquerystream.credentialsFile", "");
    }

    public String getBigqueryLocation() {
      return this.getConfig(CONFIGURATION_FIELD_PREFIX_STRING + "bigquerybatch.location",
          CONFIGURATION_FIELD_PREFIX_STRING + "bigquerystream.location", "US");
    }
  }

}
