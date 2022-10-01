/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.converters;

import io.debezium.server.bigquery.BaseBigqueryTest;
import io.debezium.server.bigquery.shared.SourcePostgresqlDB;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import com.google.cloud.bigquery.LegacySQLTypeName;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 *
 * @author Ismail Simsek
 */
@QuarkusTest
@QuarkusTestResource(value = SourcePostgresqlDB.class, restrictToAnnotatedClass = true)
@TestProfile(ChangeConsumerConverterTest.ChangeConsumerConverterProfile.class)
@Disabled("manual run")
public class ChangeConsumerConverterTest extends BaseBigqueryTest {
  @Test
  public void testVariousDataTypeConversion() throws Exception {
    this.loadVariousDataTypeConversion();
    String dest = "testc.inventory.test_data_types";
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