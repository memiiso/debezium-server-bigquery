/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *  
 */

package io.debezium.server.bigquery.offset;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.util.Callback;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Disabled;
import static io.debezium.server.bigquery.ConfigSource.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
public class BigqueryOffsetBackingStoreTest {

  private final Map<ByteBuffer, ByteBuffer> firstSet = new HashMap<>();
  private final Map<ByteBuffer, ByteBuffer> secondSet = new HashMap<>();
  Map<String, String> props = new HashMap<>();

  public String fromByteBuffer(ByteBuffer data) {
    return (data != null) ? String.valueOf(StandardCharsets.UTF_16.decode(data.asReadOnlyBuffer())) : null;
  }

  public ByteBuffer toByteBuffer(String data) {
    return (data != null) ? ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_16)) : null;
  }

  @Before
  public void setup() {
    props.put("debezium.sink.type", "bigquerybatch");
    props.put("debezium.sink.bigquerybatch.project", "test");
    props.put("debezium.sink.bigquerybatch.dataset", BQ_DATASET);
    props.put("debezium.sink.bigquerybatch.location", BQ_LOCATION);
    props.put("debezium.sink.bigquerybatch.credentialsFile", BQ_CRED_FILE);
    props.put("debezium.source.offset.storage", "io.debezium.server.bigquery.offset.BigqueryOffsetBackingStore");
    props.put("debezium.source.offset.flush.interval.ms", "60000");
    props.put("debezium.source.offset.storage.bigquery.table-name", "debezium_offset_storage_custom_table");
    firstSet.put(this.toByteBuffer("key"), this.toByteBuffer("value"));
    firstSet.put(this.toByteBuffer("key2"), null);
    secondSet.put(this.toByteBuffer("key1secondSet"), this.toByteBuffer("value1secondSet"));
    secondSet.put(this.toByteBuffer("key2secondSet"), this.toByteBuffer("value2secondSet"));
  }

  @Test
  public void testInitialize() {
    // multiple initialization should not fail
    // first one should create the table and following ones should use the created table
    BigqueryOffsetBackingStore store = new BigqueryOffsetBackingStore();
    store.configure(new TestWorkerConfig(props));
    store.start();
    store.start();
    store.start();
    store.stop();
  }

  @Test
  public void testGetSet() throws Exception {
    Callback<Void> cb = (error, result) -> {
    };

    BigqueryOffsetBackingStore store = new BigqueryOffsetBackingStore();
    store.configure(new TestWorkerConfig(props));
    store.start();
    store.set(firstSet, cb).get();

    Map<ByteBuffer, ByteBuffer> values = store.get(Arrays.asList(this.toByteBuffer("key"), this.toByteBuffer("bad"))).get();
    assertEquals(this.toByteBuffer("value"), values.get(this.toByteBuffer("key")));
    Assert.assertNull(values.get(this.toByteBuffer("bad")));
  }

  @Test
  public void testSaveRestore() throws Exception {
    Callback<Void> cb = (error, result) -> {
    };

    BigqueryOffsetBackingStore store = new BigqueryOffsetBackingStore();
    store.configure(new TestWorkerConfig(props));
    store.start();
    store.set(firstSet, cb).get();
    store.set(secondSet, cb).get();
    store.stop();
    // Restore into a new store mand make sure its correctly reload
    BigqueryOffsetBackingStore restore = new BigqueryOffsetBackingStore();
    restore.configure(new TestWorkerConfig(props));
    restore.start();
    Map<ByteBuffer, ByteBuffer> values = restore.get(Collections.singletonList(this.toByteBuffer("key"))).get();
    Map<ByteBuffer, ByteBuffer> values2 = restore.get(Collections.singletonList(this.toByteBuffer("key1secondSet"))).get();
    Map<ByteBuffer, ByteBuffer> values3 = restore.get(Collections.singletonList(this.toByteBuffer("key2secondSet"))).get();
    assertEquals(this.toByteBuffer("value"), values.get(this.toByteBuffer("key")));
    assertEquals(this.toByteBuffer("value1secondSet"), values2.get(this.toByteBuffer("key1secondSet")));
    assertEquals(this.toByteBuffer("value2secondSet"), values3.get(this.toByteBuffer("key2secondSet")));
  }

  public static class TestWorkerConfig extends WorkerConfig {
    public TestWorkerConfig(Map<String, String> props) {
      super(new ConfigDef(), props);
    }
  }
}