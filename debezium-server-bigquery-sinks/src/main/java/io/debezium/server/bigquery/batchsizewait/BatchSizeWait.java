/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery.batchsizewait;

/**
 * Interface for wait strategies used to optimize and adjust batch sizes.
 *
 * @author Ismail Simsek
 */

public interface BatchSizeWait {

  default void initizalize() {
  }

  void waitMs(long numRecordsProcessed, Integer processingTimeMs) throws InterruptedException;

}
