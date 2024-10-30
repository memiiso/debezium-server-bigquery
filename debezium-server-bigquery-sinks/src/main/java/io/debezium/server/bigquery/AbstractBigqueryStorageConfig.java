package io.debezium.server.bigquery;

import com.google.cloud.bigquery.BigQuery;
import io.debezium.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;


public abstract class AbstractBigqueryStorageConfig {
  protected static final Logger LOG = LoggerFactory.getLogger(AbstractBigqueryStorageConfig.class);
  protected Properties configCombined = new Properties();

  public AbstractBigqueryStorageConfig(Configuration config) {
    String sinkType = ConsumerUtil.sinkType(config);
    // debezium is doing config filtering before passing it down to this class! so we are taking unfiltered configs!
    Map<String, String> confIcebergSubset2 = ConsumerUtil.getConfigSubset("debezium.sink." + sinkType + ".");
    confIcebergSubset2.forEach(configCombined::putIfAbsent);
  }


  public Boolean getIsBigqueryDevEmulator() {
    return Boolean.parseBoolean((String) configCombined.getOrDefault("bigquery-dev-emulator", "false"));
  }

  public String getBigqueryProject() {
    return (String) configCombined.getOrDefault("project", null);
  }

  public String getBigqueryDataset() {
    return (String) configCombined.getOrDefault("dataset", null);
  }

  public abstract String getBigqueryTable();

  public abstract String getMigrationFile();

  public String getBigqueryCredentialsFile() {
    return (String) configCombined.getOrDefault("credentials-file", "");
  }

  public String getBigQueryCustomHost() {
    return (String) configCombined.getOrDefault("bigquery-custom-host", "");
  }

  public String getBigqueryLocation() {
    return (String) configCombined.getOrDefault("location", "US");
  }

  public BigQuery bigqueryClient() throws InterruptedException {
    return ConsumerUtil.bigqueryClient(
        getIsBigqueryDevEmulator(),
        Optional.ofNullable(this.getBigqueryProject()),
        Optional.ofNullable(this.getBigqueryDataset()),
        Optional.ofNullable(this.getBigqueryCredentialsFile()),
        this.getBigqueryLocation(),
        Optional.ofNullable(this.getBigQueryCustomHost())
    );
  }

}
