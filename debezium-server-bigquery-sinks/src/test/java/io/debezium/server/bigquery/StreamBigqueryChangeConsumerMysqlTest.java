/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import io.debezium.server.bigquery.shared.SourceMysqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

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
@QuarkusTestResource(value = SourceMysqlDB.class, restrictToAnnotatedClass = true)
@TestProfile(StreamBigqueryChangeConsumerMysqlTest.StreamBigqueryChangeConsumerMysqlTestProfile.class)
@Disabled("manual run")
public class StreamBigqueryChangeConsumerMysqlTest extends BaseBigqueryTest {

  @Test
  public void testMysqlSimpleUploadWithDelete() throws Exception {

    String sqlInsert =
        "INSERT INTO inventory.test_table (c_id, c_id2, c_data ) " +
            "VALUES  (1,1,'data'),(1,2,'data'),(1,3,'data'),(1,4,'data') ;";
    String sqlDelete = "DELETE FROM inventory.test_table where c_id = 1 ;";
    SourceMysqlDB.runSQL(sqlInsert);
    SourceMysqlDB.runSQL(sqlDelete);
    SourceMysqlDB.runSQL(sqlInsert);
    SourceMysqlDB.runSQL(sqlDelete);
    SourceMysqlDB.runSQL(sqlInsert);
    String dest = "testc.inventory.test_table";
    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        TableResult result = getTableData(dest);
        result.iterateAll().forEach(System.out::println);
        System.out.println(result.getTotalRows());
        return result.getTotalRows() >= 20
            && getTableData(dest, "__deleted = 'true'").getTotalRows() >= 8
            && getTableData(dest, "__op = 'd'").getTotalRows() >= 8;
      } catch (Exception e) {
        //e.printStackTrace();
        return false;
      }
    });
  }

  public static class StreamBigqueryChangeConsumerMysqlTestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("debezium.sink.type", "bigquerystream");
      config.put("debezium.sink.bigquerystream.allowFieldAddition", "true");
      return config;
    }
  }
}