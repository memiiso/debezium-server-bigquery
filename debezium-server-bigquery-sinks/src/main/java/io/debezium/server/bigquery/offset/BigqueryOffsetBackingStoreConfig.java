package io.debezium.server.bigquery.offset;

import io.debezium.config.Configuration;
import io.debezium.server.bigquery.BaseBigqueryStorageConfig;

public class BigqueryOffsetBackingStoreConfig extends BaseBigqueryStorageConfig {

  public BigqueryOffsetBackingStoreConfig(Configuration config, String configFieldPrefix) {
    super(config);
    config.subset("debezium.source." + configFieldPrefix, true).forEach(configCombined::put);
    config.subset(configFieldPrefix, true).forEach(configCombined::put);
  }

  public String getBigqueryTable() {
    return (String) configCombined.getOrDefault("bigquery.table-name", "debezium_offset_storage");
  }

  @Override
  public String getMigrationFile() {
    return (String) configCombined.getOrDefault("bigquery.migrate-offset-file", "");
  }

}
