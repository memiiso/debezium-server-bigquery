package io.debezium.server.bigquery.history;

import io.debezium.config.Configuration;
import io.debezium.server.bigquery.AbstractBigqueryStorageConfig;
import io.debezium.server.bigquery.ConsumerUtil;


public class BigquerySchemaHistoryConfig extends AbstractBigqueryStorageConfig {

  public BigquerySchemaHistoryConfig(Configuration config, String configFieldPrefix) {
    super(config);
    String sinkType = ConsumerUtil.sinkType(config);
    config.subset("debezium.sink." + sinkType + ".", true).forEach(configCombined::put);
    config.subset("debezium.source." + configFieldPrefix, true).forEach(configCombined::put);
    config.subset(configFieldPrefix, true).forEach(configCombined::put);
  }

  @Override
  public String getBigqueryTable() {
    return (String) configCombined.getOrDefault("bigquery.table-name", "debezium_database_history_storage");
  }

  @Override
  public String getMigrationFile() {
    return (String) configCombined.getOrDefault("bigquery.migrate-history-file", "");
  }
}
