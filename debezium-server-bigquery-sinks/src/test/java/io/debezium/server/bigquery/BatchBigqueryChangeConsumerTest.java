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
import io.quarkus.test.junit.TestProfile;

import java.time.Duration;
import javax.inject.Inject;

import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import static io.debezium.server.bigquery.shared.BaseTest.PGCreateTestDataTable;
import static io.debezium.server.bigquery.shared.BaseTest.PGLoadTestDataTable;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(SourcePostgresqlDB.class)
@TestProfile(BatchBigqueryChangeConsumerTestProfile.class)
@Disabled("manual run")
public class BatchBigqueryChangeConsumerTest {

  @Inject
  BatchBigqueryChangeConsumer bqchangeConsumer;

  public TableResult simpleQuery(String query) throws InterruptedException {

    if (bqchangeConsumer.bqClient == null) {
      bqchangeConsumer.initizalize();
    }
    //System.out.println(query);
    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
    try {
      return bqchangeConsumer.bqClient.query(queryConfig);
    } catch (Exception e) {
      return null;
    }
  }

  public void truncateTable(String destination) throws InterruptedException {
    TableId tableId = bqchangeConsumer.getTableId(destination);
    this.simpleQuery("TRUNCATE TABLE " + tableId.getProject() + "." + tableId.getDataset() + "." + tableId.getTable());
  }

  public void dropTable(String destination) throws InterruptedException {
    TableId tableId = bqchangeConsumer.getTableId(destination);
    this.simpleQuery("DROP TABLE IF EXISTS " + tableId.getProject() + "." + tableId.getDataset() + "." + tableId.getTable());
  }

  public TableResult getTableData(String destination, String where) throws InterruptedException {
    TableId tableId = bqchangeConsumer.getTableId(destination);
    return this.simpleQuery("SELECT * FROM " + tableId.getProject() + "." + tableId.getDataset() + "." + tableId.getTable()
        + " WHERE " + where
    );
  }

  public TableResult getTableData(String destination) throws InterruptedException {
    return getTableData(destination, "1=1");
  }

  @Test
  public void testSimpleUpload() throws InterruptedException {
    truncateTable("testc.inventory.geom");
    truncateTable("testc.inventory.customers");
    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        TableResult result = this.getTableData("testc.inventory.geom");
        result.iterateAll().forEach(System.out::println);
        return result.getTotalRows() >= 3;
      } catch (Exception e) {
        return false;
      }
    });

    Awaitility.await().atMost(Duration.ofSeconds(180)).until(() -> {
      try {
        TableResult result = this.getTableData("testc.inventory.customers");
        result.iterateAll().forEach(System.out::println);
        return result.getTotalRows() >= 4;
      } catch (Exception e) {
        return false;
      }
    });
  }


  @Test
  public void testSchemaChanges() throws Exception {
    String dest = "testc.inventory.customers";
    dropTable(dest);
    Awaitility.await().atMost(Duration.ofSeconds(180)).until(() -> {
      try {
        return this.getTableData(dest).getTotalRows() >= 4;
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
        //this.getTableData(dest).getValues().forEach(System.out::println);
        return this.getTableData(dest).getTotalRows() >= 9
            && this.getTableData(dest, "first_name = 'George__UPDATE1'").getTotalRows() == 3
            && this.getTableData(dest, "first_name = 'SallyUSer2'").getTotalRows() == 1
            && this.getTableData(dest, "last_name is null").getTotalRows() == 1
            && this.getTableData(dest, "id = 1004 AND __op = 'd'").getTotalRows() == 1
            //&& this.getTableData(dest, "test_varchar_column = 'value1'").getTotalRows() == 1
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
        this.getTableData(dest).getValues().forEach(System.out::println);
        this.getTableData(dest).getSchema().getFields().forEach(System.out::println);
        return this.getTableData(dest).getTotalRows() >= 10
            && this.getTableData(dest, "first_name = 'User3'").getTotalRows() == 1
            && this.getTableData(dest, "test_varchar_column = 'test_varchar_value3'").getTotalRows() == 1
            ;
      } catch (Exception e) {
        e.printStackTrace();
        return false;
      }
    });
  }


  @Test
  public void testPerformance() throws Exception {
    truncateTable("testc.inventory.test_date_table");
    this.testPerformance(1500);
  }

  public void testPerformance(int maxBatchSize) throws Exception {
    int iteration = 1;
    PGCreateTestDataTable();
    for (int i = 0; i <= iteration; i++) {
      new Thread(() -> {
        try {
          PGLoadTestDataTable(maxBatchSize, false);
        } catch (Exception e) {
          e.printStackTrace();
          Thread.currentThread().interrupt();
        }
      }).start();
    }

    Awaitility.await().atMost(Duration.ofSeconds(1200)).until(() -> {
      try {
        TableResult result = this.getTableData("testc.inventory.test_date_table");
        return result.getTotalRows() >= (long) iteration * maxBatchSize;
      } catch (Exception e) {
        return false;
      }
    });

    TableResult result = this.getTableData("testc.inventory.test_date_table");
    System.out.println("Row Count=" + result.getTotalRows());
  }
}
