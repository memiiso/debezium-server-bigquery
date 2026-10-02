/*
 * Copyright memiiso Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.bigquery.deduplication;

import io.debezium.DebeziumException;
import io.debezium.server.bigquery.ConsumerUtil;
import io.debezium.server.bigquery.StreamConsumerConfig;
import io.debezium.server.bigquery.changesequence.ChangeSequenceStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Factory and resolver for BatchDeduplicator instances.
 *
 * @author Ismail Simsek
 */
public final class DeduplicatorFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeduplicatorFactory.class);

  public static final String DEFAULT_DEDUPLICATOR = RetainLastDeduplicator.NAME;

  private DeduplicatorFactory() {
  }

  public static BatchDeduplicator getDeduplicator(
      Optional<String> configuredDeduplicator,
      StreamConsumerConfig config,
      ChangeSequenceStrategy sequenceStrategy) {
    return getDeduplicator(
        (configuredDeduplicator != null && configuredDeduplicator.isPresent()) ? configuredDeduplicator.get() : null,
        config,
        sequenceStrategy
    );
  }

  public static BatchDeduplicator getDeduplicator(
      String configuredDeduplicator,
      StreamConsumerConfig config,
      ChangeSequenceStrategy sequenceStrategy) {

    String requested = (configuredDeduplicator != null && !configuredDeduplicator.isBlank())
        ? configuredDeduplicator.trim()
        : DEFAULT_DEDUPLICATOR;

    String lower = requested.toLowerCase();
    if (lower.equals("retainlast") || lower.equals("retain-last")) {
      return new RetainLastDeduplicator();
    }
    if (lower.equals("timestampandop") || lower.equals("timestamp-and-op")) {
      return new TimestampAndOpDeduplicator(config);
    }
    if (lower.equals("changesequence") || lower.equals("change-sequence")) {
      if (sequenceStrategy == null) {
        throw new DebeziumException("ChangeSequenceStrategy must be configured when using ChangeSequence deduplicator");
      }
      return new ChangeSequenceDeduplicator(sequenceStrategy);
    }

    // Resolve custom deduplicator via ConsumerUtil (CDI @Named bean or reflection FQCN)
    BatchDeduplicator deduplicator = ConsumerUtil.loadInstance(requested, BatchDeduplicator.class);
    LOGGER.info("Resolved custom deduplicator: {}", deduplicator.getClass().getName());
    return deduplicator;
  }
}
