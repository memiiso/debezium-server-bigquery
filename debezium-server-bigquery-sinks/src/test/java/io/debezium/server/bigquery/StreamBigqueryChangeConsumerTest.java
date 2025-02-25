/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.TableResult;
import io.debezium.server.bigquery.shared.BigQueryDB;
import io.debezium.server.bigquery.shared.SourcePostgresqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@TestProfile(StreamBigqueryChangeConsumerTest.TestProfile.class)
@QuarkusTestResource(value = BigQueryDB.class, restrictToAnnotatedClass = true)
public class StreamBigqueryChangeConsumerTest extends BaseBigqueryTest {

  @BeforeAll
  public static void setup() throws InterruptedException {
    bqClient = BigQueryDB.bigQueryClient();
  }

  @Test
  public void testSimpleUpload() {
    Awaitility.await().atMost(Duration.ofSeconds(180)).until(() -> {
      String dest = "testc.inventory.customers";
      try {
        prettyPrint(dest);
        assertTableRowsAboveEqual(dest, 4);
        assertTableRowsAboveEqual(dest, 4, "DATE(__source_ts_ms) = CURRENT_DATE");
        Assert.assertEquals(getTableField(dest, "__source_ts_ms").getType(), LegacySQLTypeName.TIMESTAMP);
        Assert.assertEquals(getTableField(dest, "__ts_ms").getType(), LegacySQLTypeName.TIMESTAMP);
        Assert.assertEquals(getTableField(dest, "__deleted").getType(), LegacySQLTypeName.BOOLEAN);
        return true;
      } catch (AssertionError | Exception e) {
        return false;
      }
    });

    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        TableResult result = getTableData("testc.inventory.geom");
        result.iterateAll().forEach(System.out::println);
        assertTableRowsAboveEqual("testc.inventory.geom", 3);
        return true;
      } catch (AssertionError | Exception e) {
        return false;
      }
    });
  }

  @Test
  public void testVariousDataTypeConversion() {
    String dest = "testc.inventory.test_data_types";
    Awaitility.await().until(() -> {
      try {
        prettyPrint(dest);
        assertTableRowsAboveEqual(dest, 3);
        // '2019-07-09 02:28:10.123456+01' --> hour is UTC in BQ
        // TODO disabled because emulator has problem with TIMESTAMP values
//        assertTableRowsMatch(dest, 1, "c_timestamptz = TIMESTAMP('2019-07-09 02:28:10.123456+01')");
        // '2019-07-09 02:28:20.666666+01' --> hour is UTC in BQ
//        assertTableRowsMatch(dest, 1, "c_timestamptz = TIMESTAMP('2019-07-09 02:28:57.666666+01')");
        assertTableRowsAboveEqual(dest, 2, "DATE(c_timestamptz) = DATE('2019-07-09')");
        Assert.assertEquals(getTableField(dest, "c_timestamptz").getType(), LegacySQLTypeName.TIMESTAMP);
        Assert.assertEquals(getTableField(dest, "c_timestamp5").getType(), LegacySQLTypeName.DATETIME);
        Assert.assertEquals(getTableField(dest, "c_date").getType(), LegacySQLTypeName.DATE);
        // TODO disabled because emulator has problem with DATE values
//        assertTableRowsMatch(dest, 1, "c_date = DATE('2017-09-15')");
//        assertTableRowsMatch(dest, 1, "c_date = DATE('2017-02-10')");
//        assertTableRowsMatch(dest, 1, "int64(c_json.jfield) = 111 AND int64(c_jsonb.jfield) = 211");
//        assertTableRowsMatch(dest, 1, "int64(c_json.jfield) = 222 AND int64(c_jsonb.jfield) = 222");
        Assert.assertEquals(getTableField(dest, "c_json").getType(), LegacySQLTypeName.JSON);
        Assert.assertEquals(getTableField(dest, "c_jsonb").getType(), LegacySQLTypeName.JSON);
        return true;
      } catch (AssertionError | Exception e) {
        LOGGER.error("Error: {}", e.getMessage());
        return false;
      }
    });
  }

  @Test
  @Disabled
  public void testSchemaChanges() throws Exception {
    String dest = "testc.inventory.customers";
    Awaitility.await().atMost(Duration.ofSeconds(180)).until(() -> {
      try {
        assertTableRowsAboveEqual(dest, 4);
        return true;
      } catch (AssertionError | Exception e) {
        LOGGER.error("Error: {}", e.getMessage());
        return false;
      }
    });

    SourcePostgresqlDB.runSQL("UPDATE inventory.customers SET first_name='George__UPDATE1' WHERE ID = 1002 ;");
    SourcePostgresqlDB.runSQL("ALTER TABLE inventory.customers ADD test_varchar_column varchar(255);");
    SourcePostgresqlDB.runSQL("ALTER TABLE inventory.customers ADD test_boolean_column boolean;");
    SourcePostgresqlDB.runSQL("ALTER TABLE inventory.customers ADD test_date_column date;");
    SourcePostgresqlDB.runSQL("UPDATE inventory.customers SET first_name='George__UPDATE1'  WHERE id = 1002 ;");
    SourcePostgresqlDB.runSQL("ALTER TABLE inventory.customers ALTER COLUMN email DROP NOT NULL;");
    SourcePostgresqlDB.runSQL("INSERT INTO inventory.customers VALUES " +
        "(default,'SallyUSer2','Thomas',null,'value1',false, '2020-01-01');");
    SourcePostgresqlDB.runSQL("ALTER TABLE inventory.customers ALTER COLUMN last_name DROP NOT NULL;");
    SourcePostgresqlDB.runSQL("UPDATE inventory.customers SET last_name = NULL  WHERE id = 1002 ;");
    SourcePostgresqlDB.runSQL("DELETE FROM inventory.customers WHERE id = 1004 ;");

    Awaitility.await().atMost(Duration.ofSeconds(180)).until(() -> {
      try {
        prettyPrint(dest);
        assertTableRowsAboveEqual(dest, 9);
        assertTableRowsMatch(dest, 3, "first_name = 'George__UPDATE1'");
        assertTableRowsMatch(dest, 1, "first_name = 'SallyUSer2'");
        assertTableRowsMatch(dest, 1, "last_name is null");
        assertTableRowsMatch(dest, 1, "id = 1004 AND __op = 'd'");
        assertTableRowsMatch(dest, 1, "test_varchar_column = 'value1'");
        return true;
      } catch (AssertionError | Exception e) {
        LOGGER.error("Error: {}", e.getMessage());
        return false;
      }
    });

    SourcePostgresqlDB.runSQL("ALTER TABLE inventory.customers DROP COLUMN email;");
    SourcePostgresqlDB.runSQL("INSERT INTO inventory.customers VALUES " +
        "(default,'User3','lastname_value3','test_varchar_value3',true, '2020-01-01'::DATE);");

    Awaitility.await().atMost(Duration.ofSeconds(180)).until(() -> {
      try {
        prettyPrint(dest);
        assertTableRowsAboveEqual(dest, 10);
        assertTableRowsMatch(dest, 1, "first_name = 'User3'");
        assertTableRowsMatch(dest, 1, "test_varchar_column = 'test_varchar_value3'");
        return true;
      } catch (AssertionError | Exception e) {
        LOGGER.error("Error: {}", e.getMessage());
        return false;
      }
    });
  }

  public static class TestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("debezium.sink.type", "bigquerystream");
      config.put("debezium.sink.bigquerystream.allow-field-addition", "false");
      return config;
    }
  }
}
