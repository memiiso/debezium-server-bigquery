package io.debezium.server.bigquery;

import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

import java.util.Optional;

@ConfigRoot
@ConfigMapping
public interface CommonConfig {

  @WithName("debezium.sink.batch.destination-regexp")
  Optional<String> destinationRegexp();

  @WithName("debezium.sink.batch.destination-regexp-replace")
  Optional<String> destinationRegexpReplace();

  @WithName("debezium.sink.batch.batch-size-wait")
  @WithDefault("NoBatchSizeWait")
  String batchSizeWaitName();

  @WithName("debezium.sink.batch.nested-as-json")
  @WithDefault("false")
  boolean nestedAsJson();

  @WithName("debezium.sink.batch.concurrent-uploads")
  @WithDefault("1")
  int concurrentUploads();

  @WithName("debezium.sink.batch.concurrent-uploads.timeout-minutes")
  @WithDefault("60")
  int concurrentUploadsTimeoutMinutes();

}