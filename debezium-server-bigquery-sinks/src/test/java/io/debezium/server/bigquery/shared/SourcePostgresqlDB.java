/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery.shared;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SourcePostgresqlDB implements QuarkusTestResourceLifecycleManager {

  public static final String POSTGRES_USER = "postgres";
  public static final String POSTGRES_PASSWORD = "postgres";
  public static final String POSTGRES_DBNAME = "postgres";
  public static final String POSTGRES_IMAGE = "debezium/example-postgres:2.7.3.Final";
  public static final String POSTGRES_HOST = "localhost";
  public static final Integer POSTGRES_PORT_DEFAULT = 5432;
  private static final Logger LOGGER = LoggerFactory.getLogger(SourcePostgresqlDB.class);

  private static GenericContainer<?> container = new GenericContainer<>(POSTGRES_IMAGE)
      .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*", 2))
      .withEnv("POSTGRES_USER", POSTGRES_USER)
      .withEnv("POSTGRES_PASSWORD", POSTGRES_PASSWORD)
      .withEnv("POSTGRES_DB", POSTGRES_DBNAME)
      .withEnv("POSTGRES_INITDB_ARGS", "-E UTF8")
      .withEnv("LANG", "en_US.utf8")
      .withExposedPorts(POSTGRES_PORT_DEFAULT)
      .withStartupTimeout(Duration.ofSeconds(30));


  public static void runSQL(String query) throws SQLException, ClassNotFoundException {
    try {
      String url = "jdbc:postgresql://" + POSTGRES_HOST + ":" + container.getMappedPort(POSTGRES_PORT_DEFAULT) + "/" + POSTGRES_DBNAME;
      Class.forName("org.postgresql.Driver");
      Connection con = DriverManager.getConnection(url, POSTGRES_USER, POSTGRES_PASSWORD);
      Statement st = con.createStatement();
      st.execute(query);
      con.close();
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  public void createObjectsForTesting() {
    try {
      SourcePostgresqlDB.runSQL("CREATE EXTENSION hstore;");
      SourcePostgresqlDB.runSQL("""          
          CREATE TABLE IF NOT EXISTS inventory.test_data_types
              (
                  c_id INTEGER             ,
                  c_json JSON              ,
                  c_jsonb JSONB            ,
                  c_date DATE              ,
                  c_timestamp0 TIMESTAMP(0),
                  c_timestamp1 TIMESTAMP(1),
                  c_timestamp2 TIMESTAMP(2),
                  c_timestamp3 TIMESTAMP(3),
                  c_timestamp4 TIMESTAMP(4),
                  c_timestamp5 TIMESTAMP(5),
                  c_timestamp6 TIMESTAMP(6),
                  c_timestamptz TIMESTAMPTZ,
                  c_time TIME WITH TIME ZONE,
                  c_time_whtz TIME WITHOUT TIME ZONE,
                  c_interval INTERVAL,
                  c_binary BYTEA,
                  PRIMARY KEY(c_id)
              ) ;
          -- ALTER TABLE inventory.test_data_types REPLICA IDENTITY FULL;
          INSERT INTO
             inventory.test_data_types\s
          VALUES
             (1 , NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ),\s
             (2 , '{"jfield": 111}'::json , '{"jfield": 211}'::jsonb , '2017-09-15'::DATE , '2019-07-09 02:28:57+01' , '2019-07-09 02:28:57.1+01' , '2019-07-09 02:28:57.12+01' , '2019-07-09 02:28:57.123+01' , '2019-07-09 02:28:57.1234+01' , '2019-07-09 02:28:57.12345+01' , '2019-07-09 02:28:57.123456+01', '2019-07-09 02:28:10.123456+01', '04:05:11 PST', '04:05:11.789', INTERVAL '1' YEAR, '1234'::bytea ),\s
             (3 , '{"jfield": 222}'::json , '{"jfield": 222}'::jsonb , '2017-02-10'::DATE , '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:57.666666+01', '2019-07-09 02:28:20.666666+01', '04:10:22 UTC', '04:05:22.789', INTERVAL '10' DAY, 'abcd'::bytea )
          ;
          
          CREATE TABLE IF NOT EXISTS inventory.test_table
              (
                  c_id INTEGER           ,
                  c_id2 VARCHAR(64)      ,
                  c_data TEXT            ,
                  c_text TEXT            ,
                  c_varchar VARCHAR(1666),
                  PRIMARY KEY(c_id, c_id2)
              ) ;
          -- ALTER TABLE inventory.test_table REPLICA IDENTITY FULL;
          """
      );
    } catch (SQLException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<String, String> start() {
    container.start();
    this.createObjectsForTesting();
    Map<String, String> params = new ConcurrentHashMap<>();
    params.put("debezium.source.database.hostname", POSTGRES_HOST);
    params.put("debezium.source.database.port", container.getMappedPort(POSTGRES_PORT_DEFAULT).toString());
    params.put("debezium.source.database.user", POSTGRES_USER);
    params.put("debezium.source.database.password", POSTGRES_PASSWORD);
    params.put("debezium.source.database.dbname", POSTGRES_DBNAME);
    params.put("debezium.source.schema.include.list", "inventory");
    params.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
    params.put("debezium.transforms.unwrap.add.fields", "op,table,ts_ms,source.ts_ms,db,source.lsn,source.txId,source.ts_ns");
    return params;
  }

  @Override
  public void stop() {
    if (container != null) {
      container.stop();
    }
  }

}
