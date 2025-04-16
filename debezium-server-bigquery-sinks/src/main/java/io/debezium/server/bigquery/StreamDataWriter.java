package io.debezium.server.bigquery;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.FixedExecutorProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.storage.v1.AppendRowsRequest;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.RowError;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.rpc.Status;
import io.debezium.DebeziumException;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class responsible for writing data to BigQuery in a streaming manner using the BigQuery Write API.
 * This class creates and manages a {@link JsonStreamWriter} writer for a specific BigQuery table. It offers functionality
 * for adding data in JSON format and handling potential errors during the write process.
 */
public class StreamDataWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamDataWriter.class);
  private static final int MAX_RECREATE_COUNT = 3;
  private final BigQueryWriteClient client;
  private final Boolean ignoreUnknownFields;
  private final String streamOrTableName;
  private final Object lock = new Object();
  JsonStreamWriter streamWriter;
  private AtomicInteger recreateCount = new AtomicInteger(0);
  private final TableSchema tableSchema;


  public StreamDataWriter(String streamOrTableName,
                          BigQueryWriteClient client,
                          Boolean ignoreUnknownFields,
                          Schema tableSchema)
      throws DescriptorValidationException, IOException, InterruptedException {
    this.client = client;
    this.ignoreUnknownFields = ignoreUnknownFields;
    this.streamOrTableName = streamOrTableName;
    this.tableSchema = StorageWriteSchemaConverter.toStorageTableSchema(tableSchema);
  }

  public void initialize()
      throws DescriptorValidationException, IOException, InterruptedException {
    streamWriter = createStreamWriter();
  }

  private JsonStreamWriter createStreamWriter()
      throws DescriptorValidationException, IOException, InterruptedException {
    // https://cloud.google.com/bigquery/docs/write-api-streaming
    // Configure in-stream automatic retry settings.
    // Error codes that are immediately retried:
    // * ABORTED, UNAVAILABLE, CANCELLED, INTERNAL, DEADLINE_EXCEEDED
    // Error codes that are retried with exponential backoff:
    // * RESOURCE_EXHAUSTED
    RetrySettings retrySettings =
        RetrySettings.newBuilder()
            .setInitialRetryDelay(Duration.ofMillis(500))
            .setRetryDelayMultiplier(1.1)
            .setMaxAttempts(5)
            .setMaxRetryDelay(Duration.ofMinutes(1))
            .build();
    // Use the JSON stream writer to send records in JSON format. Specify the table name to write
    // to the default stream.
    // For more information about JsonStreamWriter, see:
    // https://googleapis.dev/java/google-cloud-bigquerystorage/latest/com/google/cloud/bigquery/storage/v1/JsonStreamWriter.html

    JsonStreamWriter streamWriter = JsonStreamWriter.newBuilder(this.streamOrTableName, this.tableSchema, client)
        .setIgnoreUnknownFields(ignoreUnknownFields)
        .setExecutorProvider(FixedExecutorProvider.create(Executors.newScheduledThreadPool(100)))
        //.setEnableConnectionPool(true)
        // If value is missing in json and there is a default value configured on bigquery
        // column, apply the default value to the missing value field.
        .setDefaultMissingValueInterpretation(AppendRowsRequest.MissingValueInterpretation.DEFAULT_VALUE)
        .setRetrySettings(retrySettings)
        //
        .setEndpoint(client.getSettings().getEndpoint())
        .setCredentialsProvider(client.getSettings().getCredentialsProvider())
        .setChannelProvider(client.getSettings().getTransportChannelProvider())
        .build();
    ensureStreamExists(streamWriter.getStreamName());
    return streamWriter;
  }


  public void appendSync(JSONArray data) throws DescriptorValidationException, IOException {
    try {
      synchronized (this.lock) {
        if (!streamWriter.isUserClosed() && streamWriter.isClosed() && recreateCount.getAndIncrement() < MAX_RECREATE_COUNT) {
          streamWriter = createStreamWriter();
        }
      }

      ApiFuture<AppendRowsResponse> future = streamWriter.append(data);
      AppendRowsResponse response = future.get();
      if (response.hasError()) {
        throw createDebeziumExceptionFromResponseError(response);
      }
    } catch (InterruptedException | ExecutionException | Exceptions.AppendSerializationError e) {
      throw createDebeziumExceptionFromException(e);
    }
  }

  private DebeziumException createDebeziumExceptionFromResponseError(AppendRowsResponse response) {
    StringBuilder errorMessageBuilder = new StringBuilder("Failed to append data to stream.\n");

    Status error = response.getError();
    errorMessageBuilder.append("Error Code: ").append(error.getCode()).append("\n");
    errorMessageBuilder.append("Error Message: ").append(error.getMessage()).append("\n");

    List<RowError> rowErrors = response.getRowErrorsList();
    errorMessageBuilder.append("Row Errors:\n");
    for (RowError rowError : rowErrors) {
      errorMessageBuilder
          .append("  Row: " + rowError.getIndex())
          .append(", Code: " + rowError.getCode())
          .append(", Message: " + rowError.getMessage())
          .append("\n");
    }
    return new DebeziumException(errorMessageBuilder.toString());
  }

  private DebeziumException createDebeziumExceptionFromException(Exception e) {
    StringBuilder exceptionMessage = new StringBuilder("Failed to append data to stream due to an exception.\n");
    exceptionMessage.append("Exception: ").append(e.getClass().getName()).append("\n");
    exceptionMessage.append("Message: ").append(e.getMessage()).append("\n");

    if (e instanceof ExecutionException && e.getCause() instanceof Exceptions.AppendSerializationError) {
      Exceptions.AppendSerializationError cause = (Exceptions.AppendSerializationError) ((ExecutionException) e).getCause();
      cause.getRowIndexToErrorMessage().entrySet().forEach(se -> {
        exceptionMessage.append("Row:" + se.getKey() + ": " + se.getValue() + "\n");
      });
    } else if (e instanceof Exceptions.AppendSerializationError) {
      ((Exceptions.AppendSerializationError) e).getRowIndexToErrorMessage().entrySet().forEach(se -> {
        exceptionMessage.append("Row:" + se.getKey() + ": " + se.getValue() + "\n");
      });
    }

    return new DebeziumException(exceptionMessage.toString(), e);
  }

  public void close(BigQueryWriteClient client) {
    if (streamWriter != null) {
      streamWriter.close();
      client.finalizeWriteStream(streamWriter.getStreamName());
    }
  }

  private void ensureStreamExists(String streamName) {
    long retryIntervalSeconds = 2;
    long startTime = System.currentTimeMillis();
    long endTime = startTime + TimeUnit.SECONDS.toMillis(15);

    while (System.currentTimeMillis() < endTime) {
      try {
        TimeUnit.SECONDS.sleep(retryIntervalSeconds);
        client.getWriteStream(streamName);
        LOGGER.debug("Stream {} exists.", streamName);
        return;
      } catch (BigQueryException e) {
        if (e.getCode() == 404) {
          LOGGER.warn("Stream {} does not exist", streamName);
        } else {
          LOGGER.warn("Error checking if stream exists for {}: {}", streamName, e.getMessage());
        }
      } catch (Exception e) {
        LOGGER.warn("Error checking if stream exists for {}: {}", streamName, e.getMessage());
      }
      LOGGER.warn("Waiting {} seconds before checking again.", retryIntervalSeconds);
    }

    throw new DebeziumException("Timed out waiting for stream " + streamName + " to exist.");
  }
}