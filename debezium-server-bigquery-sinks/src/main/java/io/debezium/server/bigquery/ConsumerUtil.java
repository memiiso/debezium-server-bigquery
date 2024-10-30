/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.grpc.ManagedChannelBuilder;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.literal.NamedLiteral;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author Ismail Simsek
 */
public class ConsumerUtil {
  protected static final Logger LOGGER = LoggerFactory.getLogger(ConsumerUtil.class);

  static final io.debezium.config.Field SINK_TYPE_FIELD = io.debezium.config.Field.create("debezium.sink.type").optional();
  static final io.debezium.config.Field SINK_TYPE_FIELD_FALLBACK = Field.create("name").optional();

  public static Map<String, String> getConfigSubset(String prefix) {
    Config config = ConfigProvider.getConfig();
    final Map<String, String> ret = new HashMap<>();

    for (String propName : config.getPropertyNames()) {
      if (propName.startsWith(prefix)) {
        final String newPropName = propName.substring(prefix.length());
        ret.put(newPropName, config.getOptionalValue(propName, String.class).orElse(""));
      }
    }

    return ret;
  }


  public static String sinkType(Configuration config) {
    String type = config.getString(SINK_TYPE_FIELD, config.getString(SINK_TYPE_FIELD_FALLBACK));

    if (type == null) {
      type = ConfigProvider.getConfig().getOptionalValue(SINK_TYPE_FIELD.name(), String.class).orElse(null);
    }

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

  public static BigQuery bigqueryClient(Boolean isBigqueryDevEmulator, Optional<String> gcpProject, Optional<String> bqDataset, Optional<String> credentialsFile, String bqLocation) throws InterruptedException {
    return bigqueryClient(isBigqueryDevEmulator, gcpProject, bqDataset, credentialsFile, bqLocation, Optional.empty());
  }

  public static BigQuery bigqueryClient(Boolean isBigqueryDevEmulator, Optional<String> gcpProject, Optional<String> bqDataset, Optional<String> credentialsFile, String bqLocation, Optional<String> hostUrl) throws InterruptedException {

    if (gcpProject.isEmpty()) {
      throw new InterruptedException("Please provide a value for `debezium.sink.{bigquerybatch|bigquerystream}.project`");
    }

    if (bqDataset.isEmpty()) {
      throw new InterruptedException("Please provide a value for `debezium.sink.{bigquerybatch|bigquerystream}.dataset`");
    }

    Credentials credentials;
    try {
      // testing only
      if (isBigqueryDevEmulator) {
        credentials = NoCredentials.getInstance();
      } else if (credentialsFile.isPresent() && !credentialsFile.orElse("").isEmpty()) {
        credentials = GoogleCredentials.fromStream(new FileInputStream(credentialsFile.get()));
      } else {
        credentials = GoogleCredentials.getApplicationDefault();
      }
    } catch (IOException e) {
      throw new DebeziumException("Failed to initialize google credentials", e);
    }

    BigQueryOptions.Builder builder = BigQueryOptions.newBuilder()
        .setCredentials(credentials)
        .setProjectId(gcpProject.get())
        .setLocation(bqLocation)
        .setRetrySettings(
            RetrySettings.newBuilder()
                // Set the max number of attempts
                .setMaxAttempts(5)
                // InitialRetryDelay controls the delay before the first retry. 
                // Subsequent retries will use this value adjusted according to the RetryDelayMultiplier. 
                .setInitialRetryDelay(Duration.ofSeconds(5))
                .setMaxRetryDelay(Duration.ofSeconds(60))
                // Set the backoff multiplier
                .setRetryDelayMultiplier(2.0)
                // Set the max duration of all attempts
                .setTotalTimeout(Duration.ofMinutes(5))
                .build()
        );

    if (!hostUrl.orElse("").isEmpty()) {
      builder
          .setHost(hostUrl.get()).setLocation(hostUrl.get());
    }
    return builder.build().getService();
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
    return ConsumerUtil.executeQuery(bqClient, query, null);
  }

  public static BigQueryWriteSettings bigQueryWriteSettings(Boolean isBigqueryDevEmulator, BigQuery bqClient, Optional<String> bigQueryCustomGRPCHost) throws IOException {
    BigQueryWriteSettings.Builder builder = BigQueryWriteSettings.newBuilder();

    if (isBigqueryDevEmulator) {
      // it is bigquery emulator
      builder.setCredentialsProvider(NoCredentialsProvider.create())
          .setTransportChannelProvider(
              BigQueryWriteSettings.defaultGrpcTransportProviderBuilder()
                  .setChannelConfigurator(ManagedChannelBuilder::usePlaintext)
                  .build()
          );
    } else {
      builder.setCredentialsProvider(FixedCredentialsProvider.create(bqClient.getOptions().getCredentials()));
    }
    if (!bigQueryCustomGRPCHost.orElse("").isEmpty()) {
      builder.setEndpoint(bigQueryCustomGRPCHost.get());
    }
    return builder.build();
  }
}
