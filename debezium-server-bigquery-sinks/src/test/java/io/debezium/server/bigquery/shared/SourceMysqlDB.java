/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery.shared;

import io.debezium.server.bigquery.BaseBigqueryTest;
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
import static io.debezium.server.bigquery.TestConfigSource.*;

public class SourceMysqlDB implements QuarkusTestResourceLifecycleManager {

  public static final String MYSQL_ROOT_PASSWORD = "debezium";
  public static final String MYSQL_USER = "mysqluser";
  public static final String MYSQL_PASSWORD = "mysqlpw";
  public static final String MYSQL_DEBEZIUM_USER = "debezium";
  public static final String MYSQL_DEBEZIUM_PASSWORD = "dbz";
  public static final String MYSQL_IMAGE = "debezium/example-mysql:1.9";
  public static final String MYSQL_HOST = "127.0.0.1";
  public static final String MYSQL_DATABASE = "inventory";
  public static final Integer MYSQL_PORT_DEFAULT = 3306;
  private static final Logger LOGGER = LoggerFactory.getLogger(SourceMysqlDB.class);

  static private GenericContainer<?> container;

  public static void runSQL(String query) throws SQLException, ClassNotFoundException {
    try {
      String url = "jdbc:mysql://" + MYSQL_HOST + ":" + container.getMappedPort(MYSQL_PORT_DEFAULT) + "/" + MYSQL_DATABASE + "?useSSL=false";
      Class.forName("com.mysql.cj.jdbc.Driver");
      Connection con = DriverManager.getConnection(url, MYSQL_USER, MYSQL_PASSWORD);
      Statement st = con.createStatement();
      st.execute(query);
      con.close();
      LOGGER.debug("Successfully executed sql query");
    } catch (Exception e) {
      LOGGER.error(query);
      throw e;
    }
  }

  @Override
  public void stop() {
    if (container != null) {
      container.stop();
    }
  }

  @Override
  public Map<String, String> start() {
    container = new GenericContainer<>(MYSQL_IMAGE)
        .waitingFor(Wait.forLogMessage(".*mysqld: ready for connections.*", 2))
        .withEnv("MYSQL_USER", MYSQL_USER)
        .withEnv("MYSQL_PASSWORD", MYSQL_PASSWORD)
        .withEnv("MYSQL_ROOT_PASSWORD", MYSQL_ROOT_PASSWORD)
        .withExposedPorts(MYSQL_PORT_DEFAULT)
        .withStartupTimeout(Duration.ofSeconds(30));
    container.start();

    LOGGER.warn("Dropping all destination BQ tables");
    TABLES.forEach(t -> BaseBigqueryTest.dropTable("testc.inventory." + t));
    BaseBigqueryTest.dropTable(OFFSET_TABLE);
    BaseBigqueryTest.dropTable(HISTORY_TABLE);

    Map<String, String> params = new ConcurrentHashMap<>();
    params.put("debezium.source.database.hostname", MYSQL_HOST);
    params.put("debezium.source.database.port", container.getMappedPort(MYSQL_PORT_DEFAULT).toString());
    params.put("debezium.source.database.user", MYSQL_DEBEZIUM_USER);
    params.put("debezium.source.database.password", MYSQL_DEBEZIUM_PASSWORD);
    params.put("debezium.source.database.dbname", MYSQL_DATABASE);
    params.put("debezium.source.database.include.list", "inventory");
    params.put("debezium.source.connector.class", "io.debezium.connector.mysql.MySqlConnector");
    params.put("debezium.transforms.unwrap.add.fields", "op,table,source.ts_ms,db,source.file,source.pos,source.row,source.gtid");

    return params;
  }

}
