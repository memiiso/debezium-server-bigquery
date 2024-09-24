/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.literal.NamedLiteral;
import org.eclipse.microprofile.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 *
 * @author Ismail Simsek
 */
public class BatchUtil {
  protected static final Logger LOGGER = LoggerFactory.getLogger(BatchUtil.class);

  static final io.debezium.config.Field SINK_TYPE_FIELD = io.debezium.config.Field.create("debezium.sink.type").optional();
  static final io.debezium.config.Field SINK_TYPE_FIELD_FALLBACK = Field.create("name").optional();

  public static Map<String, String> getConfigSubset(Config config, String prefix) {
    final Map<String, String> ret = new HashMap<>();

    for (String propName : config.getPropertyNames()) {
      if (propName.startsWith(prefix)) {
        final String newPropName = propName.substring(prefix.length());
        ret.put(newPropName, config.getValue(propName, String.class));
      }
    }

    return ret;
  }


  public static String sinkType(Configuration config) {
    String type = config.getString(SINK_TYPE_FIELD, config.getString(SINK_TYPE_FIELD_FALLBACK));
    if (type == null) {
      throw new DebeziumException("The config property debezium.sink.type is required " + "but it could not be found in any config source");
    }
    return type;
  }

  public static <T> T selectInstance(Instance<T> instances, String name) {

    Instance<T> instance = instances.select(NamedLiteral.of(name));
    if (instance.isAmbiguous()) {
      throw new DebeziumException("Multiple batch size wait class named '" + name + "' were found");
    } else if (instance.isUnsatisfied()) {
      throw new DebeziumException("No batch size wait class named '" + name + "' is available");
    }

    LOGGER.info("Using {}", instance.getClass().getName());
    return instance.get();
  }

  public static BigQuery getBQClient(Optional<String> gcpProject, Optional<String> bqDataset, Optional<String> credentialsFile, String bqLocation) throws InterruptedException {

    if (gcpProject.isEmpty()) {
      throw new InterruptedException("Please provide a value for `debezium.sink.{bigquerybatch|bigquerystream}.project`");
    }

    if (bqDataset.isEmpty()) {
      throw new InterruptedException("Please provide a value for `debezium.sink.{bigquerybatch|bigquerystream}.dataset`");
    }

    GoogleCredentials credentials;
    try {
      if (credentialsFile.isPresent() && !credentialsFile.orElse("").isEmpty()) {
        credentials = GoogleCredentials.fromStream(new FileInputStream(credentialsFile.get()));
      } else {
        credentials = GoogleCredentials.getApplicationDefault();
      }
    } catch (IOException e) {
      throw new DebeziumException("Failed to initialize google credentials", e);
    }

    return BigQueryOptions.newBuilder()
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
    
  }

  public static TableResult executeQuery(BigQuery bqClient, String query, List<QueryParameterValue> parameters) throws SQLException {
    try {
      QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query)
          .setPositionalParameters(parameters)
          .build();
      return bqClient.query(queryConfig);
    } catch (BigQueryException | InterruptedException e) {
      throw new SQLException(e);
    }
  }

  public static TableResult executeQuery(BigQuery bqClient, String query) throws SQLException {
    return BatchUtil.executeQuery(bqClient, query, null);
  }
  
}
