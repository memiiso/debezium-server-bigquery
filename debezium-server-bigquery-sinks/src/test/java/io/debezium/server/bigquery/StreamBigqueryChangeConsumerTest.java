/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import io.debezium.server.bigquery.shared.SourcePostgresqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.TableResult;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@TestProfile(StreamBigqueryChangeConsumerTest.TestProfile.class)
@Disabled("manual run")
public class StreamBigqueryChangeConsumerTest extends BaseBigqueryTest {

  @Test
  public void testSimpleUpload() {
    super.testSimpleUpload();
  }

  @Test
  @Disabled
  public void testVariousDataTypeConversion() throws Exception {
    this.loadVariousDataTypeConversion();
    String dest = "testc.inventory.test_data_types";
    Awaitility.await().atMost(Duration.ofSeconds(320)).until(() -> {
      try {
        return getTableData(dest, "int64(c_json.jfield) = 111 AND int64(c_jsonb.jfield) = 211").getTotalRows() == 1
            && getTableData(dest, "int64(c_json.jfield) = 222 AND int64(c_jsonb.jfield) = 222").getTotalRows() == 1
            && getTableField(dest, "c_json").getType() == LegacySQLTypeName.JSON
            && getTableField(dest, "c_jsonb").getType() == LegacySQLTypeName.JSON
            && getTableData(dest, "c_date = DATE('2017-02-10')").getTotalRows() == 1
            && getTableData(dest, "c_date = DATE('2017-09-15')").getTotalRows() == 1
            && getTableField(dest, "c_date").getType() == LegacySQLTypeName.DATE
            ;
      } catch (Exception e) {
        return false;
      }
    });
  }

  @Test
  @Disabled
  public void testPerformance() throws Exception {
    int maxBatchSize = 1500;
    int iteration = 1;
    for (int i = 0; i <= iteration; i++) {
      new Thread(() -> {
        try {
          SourcePostgresqlDB.PGLoadTestDataTable(maxBatchSize, false);
        } catch (Exception e) {
          e.printStackTrace();
          Thread.currentThread().interrupt();
        }
      }).start();
    }

    Awaitility.await().atMost(Duration.ofSeconds(1200)).until(() -> {
      try {
        TableResult result = getTableData("testc.inventory.test_table");
        return result.getTotalRows() >= (long) iteration * maxBatchSize;
      } catch (Exception e) {
        return false;
      }
    });

    TableResult result = getTableData("testc.inventory.test_table");
    System.out.println("Row Count=" + result.getTotalRows());
  }

  @Test
  @Disabled("WIP")
  public void testSchemaChanges() throws Exception {
    String dest = "testc.inventory.customers";
    Awaitility.await().atMost(Duration.ofSeconds(180)).until(() -> {
      try {
        return getTableData(dest).getTotalRows() >= 4;
      } catch (Exception e) {
        e.printStackTrace();
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
        //getTableData(dest).getValues().forEach(System.out::println);
        return getTableData(dest).getTotalRows() >= 9
            && getTableData(dest, "first_name = 'George__UPDATE1'").getTotalRows() == 3
            && getTableData(dest, "first_name = 'SallyUSer2'").getTotalRows() == 1
            && getTableData(dest, "last_name is null").getTotalRows() == 1
            && getTableData(dest, "id = 1004 AND __op = 'd'").getTotalRows() == 1
            //&& getTableData(dest, "test_varchar_column = 'value1'").getTotalRows() == 1
            ;
      } catch (Exception e) {
        e.printStackTrace();
        return false;
      }
    });

    SourcePostgresqlDB.runSQL("ALTER TABLE inventory.customers DROP COLUMN email;");
    SourcePostgresqlDB.runSQL("INSERT INTO inventory.customers VALUES " +
        "(default,'User3','lastname_value3','test_varchar_value3',true, '2020-01-01'::DATE);");

    Awaitility.await().atMost(Duration.ofSeconds(180)).until(() -> {
      try {
        getTableData(dest).getValues().forEach(System.out::println);
        getTableData(dest).getSchema().getFields().forEach(System.out::println);
        return getTableData(dest).getTotalRows() >= 10
            && getTableData(dest, "first_name = 'User3'").getTotalRows() == 1
            && getTableData(dest, "test_varchar_column = 'test_varchar_value3'").getTotalRows() == 1
            ;
      } catch (Exception e) {
        e.printStackTrace();
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
