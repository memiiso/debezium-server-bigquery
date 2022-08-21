/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery.shared;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class SourcePostgresqlDB implements QuarkusTestResourceLifecycleManager {

  public static final String POSTGRES_USER = "postgres";
  public static final String POSTGRES_PASSWORD = "postgres";
  public static final String POSTGRES_DBNAME = "postgres";
  public static final String POSTGRES_IMAGE = "debezium/example-postgres:1.9";
  public static final String POSTGRES_HOST = "localhost";
  public static final Integer POSTGRES_PORT_DEFAULT = 5432;
  private static final Logger LOGGER = LoggerFactory.getLogger(SourcePostgresqlDB.class);

  private static GenericContainer<?> container;
  private static Connection con = null;

  public static void runSQL(String query) throws SQLException, ClassNotFoundException {
    try {

      if (con == null) {
        String url = "jdbc:postgresql://" + POSTGRES_HOST + ":" + getMappedPort() + "/" + POSTGRES_DBNAME;
        Class.forName("org.postgresql.Driver");
        con = DriverManager.getConnection(url, POSTGRES_USER, POSTGRES_PASSWORD);
      }

      Statement st = con.createStatement();
      st.execute(query);
      LOGGER.debug("Successfully executed SQL query!");
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  public static Integer getMappedPort() {
    return container.getMappedPort(POSTGRES_PORT_DEFAULT);
  }

  public static void PGCreateTestDataTable() throws Exception {
    // create test table
    String sql = "" +
        "        CREATE TABLE IF NOT EXISTS inventory.test_date_table (\n" +
        "            c_id INTEGER ,\n" +
        "            c_text TEXT,\n" +
        "            c_varchar VARCHAR" +
        "          );";
    SourcePostgresqlDB.runSQL(sql);
  }

  @Override
  public void stop() {
    if (container != null) {
      container.stop();
    }

    try {
      if (con != null) {
        con.close();
      }
    } catch (SQLException e) {
      //
    }
  }

  public static int PGLoadTestDataTable(int numRows) throws Exception {
    return PGLoadTestDataTable(numRows, false);
  }

  public static int PGLoadTestDataTable(int numRows, boolean addRandomDelay) throws Exception {
    int numInsert = 0;
    do {

      new Thread(() -> {
        try {
          if (addRandomDelay) {
            Thread.sleep(TestUtil.randomInt(20000, 100000));
          }
          String sql = "INSERT INTO inventory.test_date_table (c_id, c_text, c_varchar ) " +
              "VALUES ";
          StringBuilder values = new StringBuilder("\n(" + TestUtil.randomInt(15, 32) + ", '" + TestUtil.randomString(524) + "', '" + TestUtil.randomString(524) + "')");
          for (int i = 0; i < 200; i++) {
            values.append("\n,(").append(TestUtil.randomInt(15, 32)).append(", '").append(TestUtil.randomString(524)).append("', '").append(TestUtil.randomString(524)).append("')");
          }
          SourcePostgresqlDB.runSQL(sql + values);
          SourcePostgresqlDB.runSQL("COMMIT;");
        } catch (Exception e) {
          e.printStackTrace();
          Thread.currentThread().interrupt();
        }
      }).start();

      numInsert += 200;
    } while (numInsert <= numRows);
    return numInsert;
  }

  public static void createTestDataTypesTable() {
    String sql = "\n" +
        "        DROP TABLE IF EXISTS inventory.test_datatypes;\n" +
        "        CREATE TABLE IF NOT EXISTS inventory.test_datatypes (\n" +
        "            c_id INTEGER ,\n" +
        "            c_json JSON,\n" +
        "            c_jsonb JSONB,\n" +
        "            c_date DATE,\n" +
        "            c_timestamp0 TIMESTAMP(0),\n" +
        "            c_timestamp1 TIMESTAMP(1),\n" +
        "            c_timestamp2 TIMESTAMP(2),\n" +
        "            c_timestamp3 TIMESTAMP(3),\n" +
        "            c_timestamp4 TIMESTAMP(4),\n" +
        "            c_timestamp5 TIMESTAMP(5),\n" +
        "            c_timestamp6 TIMESTAMP(6),\n" +
        "            c_timestamptz TIMESTAMPTZ,\n" +
        "            PRIMARY KEY (c_id)\n" +
        "          );";
    try {
      SourcePostgresqlDB.runSQL(sql);
    } catch (SQLException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<String, String> start() {
    container = new GenericContainer<>(POSTGRES_IMAGE)
        .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*", 2))
        .withEnv("POSTGRES_USER", POSTGRES_USER)
        .withEnv("POSTGRES_PASSWORD", POSTGRES_PASSWORD)
        .withEnv("POSTGRES_DB", POSTGRES_DBNAME)
        .withEnv("POSTGRES_INITDB_ARGS", "-E UTF8")
        .withEnv("LANG", "en_US.utf8")
        .withStartupTimeout(Duration.ofSeconds(30));
    container.start();

    SourcePostgresqlDB.createTestDataTypesTable();

    Map<String, String> params = new ConcurrentHashMap<>();
    params.put("debezium.source.database.hostname", POSTGRES_HOST);
    params.put("debezium.source.database.port", container.getMappedPort(POSTGRES_PORT_DEFAULT).toString());
    params.put("debezium.source.database.user", POSTGRES_USER);
    params.put("debezium.source.database.password", POSTGRES_PASSWORD);
    params.put("debezium.source.database.dbname", POSTGRES_DBNAME);
    params.put("debezium.source.schema.include.list", "inventory");
    params.put("debezium.source.table.include.list", "inventory.*");
    params.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
    params.put("debezium.transforms.unwrap.add.fields", "op,table,source.ts_ms,db,source.lsn,source.txId");
    return params;
  }

}
