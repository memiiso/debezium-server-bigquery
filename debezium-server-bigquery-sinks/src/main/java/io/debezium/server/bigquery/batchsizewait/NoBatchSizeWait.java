/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery.batchsizewait;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;

/**
 * A no-op implementation of BatchSizeWait that does not introduce any wait time.
 *
 * @author Ismail Simsek
 */
@Dependent
@Named("NoBatchSizeWait")
public class NoBatchSizeWait implements BatchSizeWait {

  public void waitMs(long numRecordsProcessed, Integer processingTimeMs) {
  }

}
