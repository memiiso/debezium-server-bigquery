package io.debezium.server.bigquery;

import io.debezium.jdbc.TemporalPrecisionMode;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

@ConfigRoot
@ConfigMapping
public interface DebeziumConfig {

  @WithName("debezium.format.value")
  @WithDefault("json")
  String valueFormat();

  @WithName("debezium.format.key")
  @WithDefault("json")
  String keyFormat();

  @WithName(value = "debezium.source.time.precision.mode")
  @WithDefault(value = "isostring")
  public TemporalPrecisionMode temporalPrecisionMode();

  default boolean isIsoStringTemporalMode() {
    return temporalPrecisionMode() == TemporalPrecisionMode.ISOSTRING;
  }

  default boolean isAdaptiveTemporalMode() {
    return temporalPrecisionMode() == TemporalPrecisionMode.ADAPTIVE ||
        temporalPrecisionMode() == TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS;
  }

}