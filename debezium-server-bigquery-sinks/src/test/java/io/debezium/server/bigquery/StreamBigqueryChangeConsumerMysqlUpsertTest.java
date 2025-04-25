/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import io.debezium.server.bigquery.shared.BigQueryDB;
import io.debezium.server.bigquery.shared.SourceMysqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(value = SourceMysqlDB.class, restrictToAnnotatedClass = true)
@QuarkusTestResource(value = BigQueryDB.class, restrictToAnnotatedClass = true)
@TestProfile(StreamBigqueryChangeConsumerMysqlUpsertTest.TestProfile.class)
public class StreamBigqueryChangeConsumerMysqlUpsertTest extends BaseBigqueryTest {

  @BeforeAll
  public static void setup() throws InterruptedException {
    bqClient = BigQueryDB.bigQueryClient();
    Thread.sleep(5000);
  }

  @Test
  public void testMysqlSimpleUploadWithDelete() throws Exception {
    // only runs for real bigquery tests
    if (streamConsumer.config.isBigqueryDevEmulator()) {
      return;
    }

    String createTable = "CREATE TABLE IF NOT EXISTS inventory.test_table (" +
        " c_id INTEGER ," +
        " c_id2 INTEGER ," +
        " c_data TEXT," +
        " c_text TEXT," +
        " c_varchar VARCHAR(1666) ," +
        " PRIMARY KEY (c_id, c_id2)" +
        " );";
    SourceMysqlDB.runSQL(createTable);
    String sqlInsert =
        "INSERT INTO inventory.test_table (c_id, c_id2, c_data ) " +
            "VALUES  (1,1,'data'),(1,2,'data'),(1,3,'data'),(1,4,'data') ;";
    String sqlDelete = "DELETE FROM inventory.test_table where c_id = 1 ;";
    String dest = "testc.inventory.test_table";
    TableId tableId = getTableId(dest);
    SourceMysqlDB.runSQL(sqlInsert);

    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        String query2 = "ALTER table  " + tableId.getDataset() + "." + tableId.getTable() + " SET OPTIONS " +
            "(max_staleness = INTERVAL '0-0 0 0:0:2' YEAR TO SECOND);";
        bqClient.query(QueryJobConfiguration.newBuilder(query2).build());
        return true;
      } catch (Exception e) {
        LOGGER.error("Error: {}", e.getMessage());
        return false;
      }
    });

    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        prettyPrint(dest);
        assertTableRowsMatch(dest, 4);
        assertTableRowsMatch(dest, 4, "__deleted = false");
        return true;
      } catch (AssertionError | Exception e) {
        LOGGER.error("Error: {}", e.getMessage());
        return false;
      }
    });

    SourceMysqlDB.runSQL(sqlDelete);

    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        prettyPrint(dest);
        assertTableRowsMatch(dest, 4);
        assertTableRowsMatch(dest, 4, "__deleted = true");
        assertTableRowsMatch(dest, 4, "__op = 'd'");
        return true;
      } catch (AssertionError | Exception e) {
        LOGGER.error("Error: {}", e.getMessage());
        return false;
      }
    });

    SourceMysqlDB.runSQL(sqlInsert);
    Thread.sleep(3000);
    SourceMysqlDB.runSQL(sqlDelete);
    Thread.sleep(3000);
    SourceMysqlDB.runSQL(sqlInsert);
    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        prettyPrint(dest);
        assertTableRowsMatch(dest, 4);
        assertTableRowsMatch(dest, 4, "__deleted = false");
        assertTableRowsMatch(dest, 4, "__op = 'c'");
        return true;
      } catch (AssertionError | Exception e) {
        LOGGER.error("Error: {}", e.getMessage());
        return false;
      }
    });
  }


  @Test
  public void testDeduplicateBatch() throws Exception {
    RecordConverter e1 = builder
        .destination("destination")
        .addKeyField("id", 1)
        .addKeyField("first_name", "row1")
        .addField("__op", "r")
        .addField("__source_ts_ns", 3L)
        .build();
    RecordConverter e2 = builder
        .destination("destination")
        .addKeyField("id", 1)
        .addKeyField("first_name", "row1")
        .addField("__op", "u")
        .addField("__source_ts_ns", 1L)
        .build();

    List<RecordConverter> records = List.of(e1, e2);
    List<RecordConverter> dedups = streamConsumer.deduplicateBatch(records);
    Assertions.assertEquals(1, dedups.size());
    Assertions.assertEquals(3L, dedups.get(0).value().get("__source_ts_ns").asLong(0L));

    RecordConverter e21 = builder
        .destination("destination")
        .addKeyField("id", 1)
        .addField("__op", "r")
        .addField("__source_ts_ns", 1L)
        .build();
    RecordConverter e22 = builder
        .destination("destination")
        .addKeyField("id", 1)
        .addField("__op", "u")
        .addField("__source_ts_ns", 1L)
        .build();

    List<RecordConverter> records2 = List.of(e21, e22);
    List<RecordConverter> dedups2 = streamConsumer.deduplicateBatch(records2);
    Assertions.assertEquals(1, dedups2.size());
    Assertions.assertEquals("u", dedups2.get(0).value().get("__op").asText("x"));
  }

  public static class TestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("debezium.sink.type", "bigquerystream");
      config.put("debezium.sink.bigquerystream.allow-field-addition", "true");
      config.put("debezium.source.table.include.list", "inventory.test_table");
      config.put("debezium.sink.bigquerystream.upsert", "true");
      config.put("debezium.sink.bigquerystream.upsert-keep-deletes", "true");
      config.put("debezium.source.offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore");
      config.put("debezium.source.schema.history.internal", "io.debezium.relational.history.MemorySchemaHistory");
      return config;
    }
  }
}
