package io.debezium.server.bigquery;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.FixedExecutorProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import io.debezium.DebeziumException;
import org.json.JSONArray;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;


public class StreamDataWriter {
  private static final int MAX_RECREATE_COUNT = 3;
  private final BigQueryWriteClient client;
  private final Boolean ignoreUnknownFields;
  private final InstantiatingGrpcChannelProvider instantiatingGrpcChannelProvider;
  private final TableName parentTable;
  private final Object lock = new Object();
  JsonStreamWriter streamWriter;
  private AtomicInteger recreateCount = new AtomicInteger(0);


  public StreamDataWriter(TableName parentTable, BigQueryWriteClient client,
                          Boolean ignoreUnknownFields, InstantiatingGrpcChannelProvider instantiatingGrpcChannelProvider)
      throws DescriptorValidationException, IOException, InterruptedException {
    this.client = client;
    this.ignoreUnknownFields = ignoreUnknownFields;
    this.instantiatingGrpcChannelProvider = instantiatingGrpcChannelProvider;
    this.parentTable = parentTable;
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

    // Initialize a write stream for the specified table.
    // For more information on WriteStream.Type, see:
    // https://googleapis.dev/java/google-cloud-bigquerystorage/latest/com/google/cloud/bigquery/storage/v1/WriteStream.Type.html
    WriteStream stream = WriteStream.newBuilder().setType(WriteStream.Type.COMMITTED).build();

    CreateWriteStreamRequest createWriteStreamRequest =
        CreateWriteStreamRequest.newBuilder()
            .setParent(parentTable.toString())
            .setWriteStream(stream)
            .build();

    WriteStream writeStream = client.createWriteStream(createWriteStreamRequest);
    // Use the JSON stream writer to send records in JSON format. Specify the table name to write
    // to the default stream.
    // For more information about JsonStreamWriter, see:
    // https://googleapis.dev/java/google-cloud-bigquerystorage/latest/com/google/cloud/bigquery/storage/v1/JsonStreamWriter.html
    return JsonStreamWriter.newBuilder(writeStream.getName(), writeStream.getTableSchema())
        .setIgnoreUnknownFields(ignoreUnknownFields)
        .setExecutorProvider(FixedExecutorProvider.create(Executors.newScheduledThreadPool(100)))
        .setChannelProvider(instantiatingGrpcChannelProvider)
        //.setEnableConnectionPool(true)
        // If value is missing in json and there is a default value configured on bigquery
        // column, apply the default value to the missing value field.
        .setDefaultMissingValueInterpretation(AppendRowsRequest.MissingValueInterpretation.DEFAULT_VALUE)
        .setRetrySettings(retrySettings)
        .build();
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
        throw new DebeziumException("Failed to append data to stream. Error Code:" + response.getError().getCode() + " Error Message:" + response.getError().getMessage());
      }
    } catch (Exception throwable) {
      throw new DebeziumException("Failed to append data to stream " + streamWriter.getStreamName() + "\n" + throwable.getMessage(),
          throwable);
    }
  }

  public void close(BigQueryWriteClient client) {
    if (streamWriter != null) {
      streamWriter.close();
      client.finalizeWriteStream(streamWriter.getStreamName());
    }
  }
}
