/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery.batchsizewait;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

import javax.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(DynamicBatchSizeWaitTestProfile.class)
@Disabled
class DynamicBatchSizeWaitTest {

  @Inject
  DynamicBatchSizeWait waitBatchSize;

  @ConfigProperty(name = "debezium.source.poll.interval.ms", defaultValue = "1000")
  long pollIntervalMs;

  @Test
  void shouldIncreaseSleepMs() {
    DynamicBatchSizeWait dynamicSleep = waitBatchSize;
    // if its consuming small batch sizes, the sleep delay should increase to adjust batch size
    // sleep size should increase and stay at max (pollIntervalMs)
    long sleep = 0;
    sleep = dynamicSleep.getWaitMs(3L);
    Assertions.assertTrue(sleep < pollIntervalMs);
    sleep = dynamicSleep.getWaitMs(2L);
    Assertions.assertTrue(sleep <= pollIntervalMs);
    sleep = dynamicSleep.getWaitMs(1L);
    Assertions.assertEquals(sleep, pollIntervalMs);
    sleep = dynamicSleep.getWaitMs(1L);
    Assertions.assertEquals(sleep, pollIntervalMs);
    sleep = dynamicSleep.getWaitMs(1L);
    Assertions.assertEquals(sleep, pollIntervalMs);
  }

  @Test
  void shouldDecreaseSleepMs() {
    DynamicBatchSizeWait dynamicSleep = waitBatchSize;
    // if its consuming large batch sizes, the sleep delay should decrease
    dynamicSleep.getWaitMs(3L);
    dynamicSleep.getWaitMs(2L);
    dynamicSleep.getWaitMs(1L);
    // start test
    // max batch size = debezium.source.max.batch.size = 100
    long sleep1 = dynamicSleep.getWaitMs(120L);
    long sleep2 = dynamicSleep.getWaitMs(120L);
    Assertions.assertTrue(sleep2 <= sleep1);
    long sleep3 = dynamicSleep.getWaitMs(120L);
    Assertions.assertTrue(sleep3 <= sleep2);
    long sleep4 = dynamicSleep.getWaitMs(120L);
    Assertions.assertTrue(sleep4 <= sleep3);
    dynamicSleep.getWaitMs(120L);
    dynamicSleep.getWaitMs(120L);
    dynamicSleep.getWaitMs(120L);
    dynamicSleep.getWaitMs(120L);
    Assertions.assertTrue(dynamicSleep.getWaitMs(120L) <= 100L);
  }

}