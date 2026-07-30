# `bigquerybatch` Consumer

Writes Debezium events to Google BigQuery using standard BigQuery Load Jobs.
It groups CDC events and appends them to the destination BigQuery table [using BigQuery Load Jobs](https://cloud.google.com/bigquery/docs/batch-loading-data#loading_data_from_local_files).

!!! note

    This consumer only supports append mode.

!!! info

    This consumer uses the BigQuery Load Jobs API to import data into BigQuery without incurring streaming ingestion costs.

## Configuration

### Consumer Configuration

| Config                                               | Default            | Description                                                                                                |
|------------------------------------------------------|--------------------|------------------------------------------------------------------------------------------------------------|
| `debezium.sink.bigquerybatch.project`                |                    | Destination GCP BigQuery project ID                                                                        |
| `debezium.sink.bigquerybatch.dataset`                |                    | Destination BigQuery dataset name                                                                          |
| `debezium.sink.bigquerybatch.location`               | `US`               | BigQuery dataset location (e.g. `US`, `EU`)                                                                |
| `debezium.sink.bigquerybatch.create-disposition`     | `CREATE_IF_NEEDED` | Create destination tables if needed (`CREATE_IF_NEEDED` or `CREATE_NEVER`)                                 |
| `debezium.sink.bigquerybatch.writeDisposition`       | `WRITE_APPEND`     | Action that occurs if destination table exists (`WRITE_APPEND`, `WRITE_TRUNCATE`, `WRITE_EMPTY`)           |
| `debezium.sink.bigquerybatch.partition-field`        | `__ts_ms`          | Field used to partition target BigQuery tables                                                             |
| `debezium.sink.bigquerybatch.partition-type`         | `MONTH`            | Table partitioning type (`DAY`, `MONTH`, `YEAR`, `HOUR`)                                                  |
| `debezium.sink.bigquerybatch.clustering-field`       | `__source_ts_ms`   | Field used to cluster target BigQuery tables (combined with primary key fields)                             |
| `debezium.sink.bigquerybatch.allow-field-addition`   | `true`             | Allow dynamic schema expansion (adding new columns to target table schema)                                 |
| `debezium.sink.bigquerybatch.allow-field-relaxation` | `true`             | Allow field relaxation in schema updates                                                                   |
| `debezium.sink.bigquerybatch.cast-deleted-field`     | `false`            | Cast `__deleted` field to boolean type (default is string type)                                           |
| `debezium.sink.bigquerybatch.credentials-file`       |                    | Path to GCP service account JSON credentials file                                                           |
| `debezium.sink.bigquerybatch.bigquery-custom-host`   |                    | Custom HTTP endpoint for BigQuery API. Useful for local testing with `bq-emulator`.                       |
| `debezium.sink.bigquerybatch.bigquery-dev-emulator`  | `false`            | Whether to connect to a local `bq-emulator` instance                                                      |

### Common Batch & Mapping Options

| Config                                                  | Default            | Description                                                                                                |
|---------------------------------------------------------|--------------------|------------------------------------------------------------------------------------------------------------|
| `debezium.sink.batch.destination-regexp`                |                    | Regexp pattern applied to event target table names for destination mapping                                |
| `debezium.sink.batch.destination-regexp-replace`        |                    | Replacement string for destination regexp mapping                                                          |
| `debezium.sink.batch.batch-size-wait`                   | `NoBatchSizeWait`  | Wait strategy to optimize batch sizes (`NoBatchSizeWait`, `MaxBatchSizeWait`, `DynamicBatchSizeWait`)      |
| `debezium.sink.batch.nested-as-json`                    | `false`            | Serializes complex nested structures/maps as raw JSON strings rather than expanded BigQuery STRUCTs        |
| `debezium.sink.batch.concurrent-uploads`                | `1`                | Number of parallel concurrent upload threads for destination tables                                       |
| `debezium.sink.batch.concurrent-uploads.timeout-minutes`| `60`               | Timeout (in minutes) for concurrent uploads                                                                |
