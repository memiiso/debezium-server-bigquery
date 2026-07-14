package io.debezium.server.bigquery;

import io.debezium.DebeziumException;
import io.smallrye.config.WithDefault;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StreamConsumerConfigTest {
  @Test
  void sequencingAndAppendDefaultsAreBackwardCompatible() throws Exception {
    WithDefault sequenceDefault = StreamConsumerConfig.class.getMethod("changeSequenceEnabled").getAnnotation(WithDefault.class);
    WithDefault inFlightDefault = StreamConsumerConfig.class.getMethod("maxInFlightAppends").getAnnotation(WithDefault.class);
    assertEquals("false", sequenceDefault.value());
    assertEquals("1", inFlightDefault.value());
  }

  @Test
  void zeroInFlightAppendsIsRejectedBeforeConnecting() {
    StreamBigqueryChangeConsumer consumer = new StreamBigqueryChangeConsumer();
    consumer.config = mock(StreamConsumerConfig.class);
    when(consumer.config.maxInFlightAppends()).thenReturn(0);
    DebeziumException error = assertThrows(DebeziumException.class, consumer::initialize);
    assertEquals("debezium.sink.bigquerystream.max-in-flight-appends must be at least 1, but was 0", error.getMessage());
  }
}
