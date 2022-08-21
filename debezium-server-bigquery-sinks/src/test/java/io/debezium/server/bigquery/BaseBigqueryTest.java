/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import java.util.List;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3 destination.
 *
 * @author Ismail Simsek
 */

public abstract class BaseBigqueryTest {

  protected final Logger LOGGER = LoggerFactory.getLogger(getClass());
  BigQuery bqClient;

  protected static List<String> tables = List.of("customers", "geom", "orders", "products",
      "products_on_hand", "test_datatypes", "test_datatypes");

  public void setup(BigQuery bqClient) throws InterruptedException {
    this.bqClient = bqClient;
    // remove all the tables from BQ!
    LOGGER.warn("Truncating all destination tables");
    for (String t : tables) {
      truncateTable("testc.inventory." + t);
    }
  }

  public TableResult simpleQuery(String query) throws InterruptedException {
    //System.out.println(query);
    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
    try {
      return bqClient.query(queryConfig);
    } catch (Exception e) {
      return null;
    }
  }

  public void truncateTable(String destination) throws InterruptedException {
    TableId tableId = this.getTableId(destination);
    this.simpleQuery("TRUNCATE TABLE " + tableId.getProject() + "." + tableId.getDataset() + "." + tableId.getTable());
  }

  public void dropTable(String destination) throws InterruptedException {
    TableId tableId = this.getTableId(destination);
    LOGGER.warn("Dropping table {}", tableId);
    this.simpleQuery("DROP TABLE IF EXISTS " + tableId.getProject() + "." + tableId.getDataset() + "." + tableId.getTable());
  }

  public TableResult getTableData(String destination, String where) throws InterruptedException {
    TableId tableId = this.getTableId(destination);
    return this.simpleQuery("SELECT * FROM " + tableId.getProject() + "." + tableId.getDataset() + "." + tableId.getTable()
        + " WHERE " + where
    );
  }

  public TableResult getTableData(String destination) throws InterruptedException {
    return getTableData(destination, "1=1");
  }

  abstract TableId getTableId(String destination);

}
