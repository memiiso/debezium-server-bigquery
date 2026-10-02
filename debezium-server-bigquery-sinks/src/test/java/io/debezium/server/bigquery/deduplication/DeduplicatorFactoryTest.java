/*
 * Copyright memiiso Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.bigquery.deduplication;

import io.debezium.DebeziumException;
import io.debezium.server.bigquery.RecordConverter;
import io.debezium.server.bigquery.StreamConsumerConfig;
import io.debezium.server.bigquery.changesequence.ChangeSequenceFactory;
import io.debezium.server.bigquery.changesequence.ChangeSequenceStrategy;
import io.debezium.server.bigquery.changesequence.MySqlChangeSequenceStrategy;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Tag("unit")
class DeduplicatorFactoryTest {

  public static class CustomTestDeduplicator implements BatchDeduplicator {
    @Override
    public List<RecordConverter> deduplicate(List<RecordConverter> events) {
      return events;
    }

    @Override
    public String name() {
      return "CustomTest";
    }
  }

  @Test
  void defaultsToRetainLastWhenNotConfigured() {
    BatchDeduplicator dedup1 = DeduplicatorFactory.getDeduplicator((String) null, null, null);
    assertInstanceOf(RetainLastDeduplicator.class, dedup1);

    BatchDeduplicator dedup2 = DeduplicatorFactory.getDeduplicator(Optional.empty(), null, null);
    assertInstanceOf(RetainLastDeduplicator.class, dedup2);

    BatchDeduplicator dedup3 = DeduplicatorFactory.getDeduplicator("   ", null, null);
    assertInstanceOf(RetainLastDeduplicator.class, dedup3);
  }

  @Test
  void resolvesExplicitDeduplicators() {
    StreamConsumerConfig config = mock(StreamConsumerConfig.class);
    ChangeSequenceStrategy strategy = new MySqlChangeSequenceStrategy();

    assertInstanceOf(RetainLastDeduplicator.class,
        DeduplicatorFactory.getDeduplicator("RetainLast", config, strategy));

    assertInstanceOf(TimestampAndOpDeduplicator.class,
        DeduplicatorFactory.getDeduplicator("TimestampAndOp", config, strategy));

    assertInstanceOf(ChangeSequenceDeduplicator.class,
        DeduplicatorFactory.getDeduplicator("ChangeSequence", config, strategy));
  }

  @Test
  void resolvesKebabCaseAndCaseInsensitiveNames() {
    StreamConsumerConfig config = mock(StreamConsumerConfig.class);
    ChangeSequenceStrategy strategy = new MySqlChangeSequenceStrategy();

    assertInstanceOf(RetainLastDeduplicator.class,
        DeduplicatorFactory.getDeduplicator("retainlast", config, null));
    assertInstanceOf(RetainLastDeduplicator.class,
        DeduplicatorFactory.getDeduplicator("retain-last", config, null));

    assertInstanceOf(TimestampAndOpDeduplicator.class,
        DeduplicatorFactory.getDeduplicator("timestampandop", config, null));
    assertInstanceOf(TimestampAndOpDeduplicator.class,
        DeduplicatorFactory.getDeduplicator("timestamp-and-op", config, null));

    assertInstanceOf(ChangeSequenceDeduplicator.class,
        DeduplicatorFactory.getDeduplicator("changesequence", config, strategy));
    assertInstanceOf(ChangeSequenceDeduplicator.class,
        DeduplicatorFactory.getDeduplicator("change-sequence", config, strategy));
  }

  @Test
  void resolvesConfiguredClassViaReflection() {
    BatchDeduplicator deduplicator = DeduplicatorFactory.getDeduplicator(
        Optional.of(CustomTestDeduplicator.class.getName()),
        null,
        null
    );

    assertNotNull(deduplicator);
    assertInstanceOf(CustomTestDeduplicator.class, deduplicator);
    assertEquals("CustomTest", deduplicator.name());
  }

  @Test
  void failsFastOnUnknownDeduplicator() {
    DebeziumException ex = assertThrows(DebeziumException.class, () ->
        DeduplicatorFactory.getDeduplicator(
            Optional.of("non.existent.DeduplicatorClass"),
            null,
            null
        )
    );

    assertTrue(ex.getMessage().contains("Could not resolve BatchDeduplicator for name or class: 'non.existent.DeduplicatorClass'"));
  }

  @Test
  void failsFastWhenChangeSequenceRequestedWithoutStrategy() {
    DebeziumException ex = assertThrows(DebeziumException.class, () ->
        DeduplicatorFactory.getDeduplicator(
            "ChangeSequence",
            null,
            null
        )
    );
    assertTrue(ex.getMessage().contains("ChangeSequenceStrategy must be configured when using ChangeSequence deduplicator"));
  }
}
