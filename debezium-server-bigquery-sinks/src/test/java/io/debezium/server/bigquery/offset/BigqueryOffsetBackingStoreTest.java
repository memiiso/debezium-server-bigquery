/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery.offset;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.util.Callback;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import static io.debezium.server.bigquery.TestConfigSource.BQ_DATASET;
import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
@TestProfile(BigqueryOffsetBackingStoreTest.TestProfile.class)
@Disabled
public class BigqueryOffsetBackingStoreTest {

  private static final Map<ByteBuffer, ByteBuffer> firstSet = new HashMap<>();
  private static final Map<ByteBuffer, ByteBuffer> secondSet = new HashMap<>();

  public static ByteBuffer toByteBuffer(String data) {
    return (data != null) ? ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_16)) : null;
  }

  @BeforeAll
  public static void setup() {
    firstSet.put(toByteBuffer("key"), toByteBuffer("value"));
    firstSet.put(toByteBuffer("key2"), null);
    secondSet.put(toByteBuffer("key1secondSet"), toByteBuffer("value1secondSet"));
    secondSet.put(toByteBuffer("key2secondSet"), toByteBuffer("value2secondSet"));
  }

  public Map<String, String> config() {
    Map<String, String> conf = new HashMap<>();
    for (String propName : ConfigProvider.getConfig().getPropertyNames()) {
      if (propName.startsWith("debezium")) {
        try {
          conf.put(propName, ConfigProvider.getConfig().getValue(propName, String.class));
        } catch (Exception e) {
          conf.put(propName, "");
        }
      }
    }
    return conf;
  }

  @Test
  public void testInitialize() throws ExecutionException, InterruptedException {
    // multiple initialization should not fail
    // first one should create the table and following ones should use the created table
    BigqueryOffsetBackingStore store = new BigqueryOffsetBackingStore();
    store.configure(new TestWorkerConfig(config()));
    store.start();
    store.start();
    store.start();
    assertEquals(store.getTableFullName(), BQ_DATASET + ".__debezium_offset_storage_test_table");
    store.stop();

    BigqueryOffsetBackingStore restore = new BigqueryOffsetBackingStore();
    restore.configure(new TestWorkerConfig(config()));
    restore.start();
    restore.get(Collections.singletonList(toByteBuffer("payload"))).get();
    restore.stop();
  }

  @Test
  public void testGetSet() throws Exception {
    Callback<Void> cb = (error, result) -> {
    };

    BigqueryOffsetBackingStore store = new BigqueryOffsetBackingStore();
    store.configure(new TestWorkerConfig(config()));
    store.start();
    store.set(firstSet, cb).get();

    Map<ByteBuffer, ByteBuffer> values = store.get(Arrays.asList(toByteBuffer("key"), toByteBuffer("bad"))).get();
    assertEquals(toByteBuffer("value"), values.get(toByteBuffer("key")));
    Assertions.assertNull(values.get(toByteBuffer("bad")));
  }

  @Test
  public void testSaveRestore() throws Exception {
    Callback<Void> cb = (error, result) -> {
    };

    BigqueryOffsetBackingStore store = new BigqueryOffsetBackingStore();
    store.configure(new TestWorkerConfig(config()));
    store.start();
    store.set(firstSet, cb).get();
    store.set(secondSet, cb).get();
    store.stop();
    // Restore into a new store mand make sure its correctly reload
    BigqueryOffsetBackingStore restore = new BigqueryOffsetBackingStore();
    restore.configure(new TestWorkerConfig(config()));
    restore.start();
    Map<ByteBuffer, ByteBuffer> values = restore.get(Collections.singletonList(toByteBuffer("key"))).get();
    Map<ByteBuffer, ByteBuffer> values2 = restore.get(Collections.singletonList(toByteBuffer("key1secondSet"))).get();
    Map<ByteBuffer, ByteBuffer> values3 = restore.get(Collections.singletonList(toByteBuffer("key2secondSet"))).get();
    assertEquals(toByteBuffer("value"), values.get(toByteBuffer("key")));
    assertEquals(toByteBuffer("value1secondSet"), values2.get(toByteBuffer("key1secondSet")));
    assertEquals(toByteBuffer("value2secondSet"), values3.get(toByteBuffer("key2secondSet")));
  }

  public static class TestWorkerConfig extends WorkerConfig {
    public TestWorkerConfig(Map<String, String> props) {
      super(new ConfigDef(), props);
    }
  }

  public static class TestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("debezium.sink.type", "bigquerybatch");
      config.put("debezium.source.offset.storage", "io.debezium.server.bigquery.offset.BigqueryOffsetBackingStore");
      config.put("debezium.source.offset.flush.interval.ms", "60010");
      config.put("debezium.source.offset.storage.bigquery.table-name", "__debezium_offset_storage_test_table");
      config.put("debezium.source.offset.storage.bigquery.migrate-offset-file", "src/test/resources/offsets.dat");
      return config;
    }
  }
}