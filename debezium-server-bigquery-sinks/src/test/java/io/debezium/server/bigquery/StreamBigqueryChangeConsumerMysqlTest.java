/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import com.google.cloud.bigquery.TableResult;
import io.debezium.server.bigquery.shared.BigQueryGCP;
import io.debezium.server.bigquery.shared.SourceMysqlDB;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@WithTestResource(value = SourceMysqlDB.class)
@WithTestResource(value = BigQueryGCP.class)
@TestProfile(StreamBigqueryChangeConsumerMysqlTest.TestProfile.class)
@DisabledIfEnvironmentVariable(named = "GITHUB_ACTIONS", matches = "true")
public class StreamBigqueryChangeConsumerMysqlTest extends BaseBigqueryTest {

  @BeforeAll
  public static void setup() {
    bqClient = BigQueryGCP.bigQueryClient();
  }

  @Test
  public void testMysqlSimpleUploadWithDelete() throws Exception {
    
    String createTable = "" +
        "CREATE TABLE IF NOT EXISTS inventory.test_table (" +
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
    SourceMysqlDB.runSQL(sqlInsert);
    SourceMysqlDB.runSQL(sqlDelete);
    SourceMysqlDB.runSQL(sqlInsert);
    SourceMysqlDB.runSQL(sqlDelete);
    SourceMysqlDB.runSQL(sqlInsert);
    String dest = "testc.inventory.test_table";
//    BaseBigqueryTest.truncateTable(dest);
    Awaitility.await().atMost(Duration.ofSeconds(120)).until(() -> {
      try {
        TableResult result = getTableData(dest);
        result.iterateAll().forEach(System.out::println);
//        System.out.println(result.getTotalRows());
        return result.getTotalRows() >= 4
            && getTableData(dest, "__deleted = true").getTotalRows() >= 2
            && getTableData(dest, "__op = 'd'").getTotalRows() >= 2;
      } catch (Exception e) {
        //e.printStackTrace();
        return false;
      }
    });
  }

  public static class TestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("debezium.sink.type", "bigquerystream");
      config.put("debezium.sink.bigquerystream.allow-field-addition", "true");
      config.put("debezium.source.table.include.list", "inventory.test_table");
      return config;
    }
  }
}