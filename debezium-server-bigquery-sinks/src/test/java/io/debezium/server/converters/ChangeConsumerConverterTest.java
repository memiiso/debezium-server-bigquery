/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.converters;

import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.TableResult;
import io.debezium.server.bigquery.BaseBigqueryTest;
import io.debezium.server.bigquery.shared.BigQueryGCP;
import io.debezium.server.bigquery.shared.SourcePostgresqlDB;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
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
@WithTestResource(value = SourcePostgresqlDB.class)
@TestProfile(ChangeConsumerConverterTest.ChangeConsumerConverterProfile.class)
@WithTestResource(value = BigQueryGCP.class)
@DisabledIfEnvironmentVariable(named = "GITHUB_ACTIONS", matches = "true")
@Disabled // enable and fix
public class ChangeConsumerConverterTest extends BaseBigqueryTest {

  @BeforeAll
  public static void setup() {
    bqClient = BigQueryGCP.bigQueryClient();
  }

  @Test
  public void testVariousDataTypeConversion() throws Exception {
    //SourcePostgresqlDB.runSQL("DELETE FROM  inventory.test_data_types WHERE c_id>0;");
    String dest = "testc.inventory.test_data_types";
    Awaitility.await().atMost(Duration.ofSeconds(320)).until(() -> {
      try {
        //getTableData(dest).iterateAll().forEach(System.out::println);
        return getTableData(dest).getTotalRows() >= 3
            // '2019-07-09 02:28:10.123456+01' --> hour is UTC in BQ
            && getTableData(dest, "c_timestamptz = TIMESTAMP('2019-07-09T01:28:10.123456Z')").getTotalRows() == 1
            // '2019-07-09 02:28:20.666666+01' --> hour is UTC in BQ
            && getTableData(dest, "c_timestamptz = TIMESTAMP('2019-07-09T01:28:20.666666Z')").getTotalRows() == 1
            && getTableData(dest, "DATE(c_timestamptz) = DATE('2019-07-09')").getTotalRows() >= 2
            && getTableField(dest, "c_timestamptz").getType() == LegacySQLTypeName.TIMESTAMP
            && getTableData(dest, "c_date = DATE('2017-09-15')").getTotalRows() == 1
            && getTableData(dest, "c_date = DATE('2017-02-10')").getTotalRows() == 1
            ;
      } catch (Exception e) {
        e.printStackTrace();
        return false;
      }
    });

    TableResult result = BaseBigqueryTest.getTableData(dest);
    result.iterateAll().forEach(System.out::println);
    Awaitility.await().atMost(Duration.ofSeconds(320)).until(() -> {
      try {
        return getTableData(dest, "STRING(c_timestamp5) = '2019-07-09 02:28:57.123450'").getTotalRows() == 1
            && getTableData(dest, "c_date = DATE('2017-02-10')").getTotalRows() == 1
            && getTableData(dest, "c_date = DATE('2017-09-15')").getTotalRows() == 1
            && getTableField(dest, "c_date").getType() == LegacySQLTypeName.DATE
            && getTableField(dest, "c_timestamp0").getType() == LegacySQLTypeName.DATETIME
            && getTableField(dest, "c_timestamp5").getType() == LegacySQLTypeName.DATETIME
            && getTableField(dest, "c_timestamp6").getType() == LegacySQLTypeName.DATETIME
            ;
      } catch (Exception e) {
        e.printStackTrace();
        return false;
      }
    });

  }

  public static class ChangeConsumerConverterProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>();
      config.put("debezium.sink.type", "bigquerybatch");
      config.put("debezium.source.table.include.list", "inventory.test_data_types");
      config.put("debezium.source.converters", "bqdatetime");
      config.put("debezium.source.bqdatetime.type", "io.debezium.server.converters.TemporalToISOStringConverter");
      //
      return config;
    }
  }
}