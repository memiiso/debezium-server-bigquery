# `bigquerybatch` Consumer

Writes debezium events to Bigquery using [BigQuery Storage Write API](https://cloud.google.com/bigquery/docs/write-api).
It groups CDC events and appends to destination BigQuery
table [using BigQuery Write API](https://cloud.google.com/bigquery/docs/batch-loading-data#loading_data_from_local_files)

**NOTE**: this consumer only supports append mode.

## Configuration

| Config                                               | Default            | Description                                                                                                |
|------------------------------------------------------|--------------------|------------------------------------------------------------------------------------------------------------|
| `debezium.sink.bigquerybatch.dataset`                |                    | Destination Bigquery dataset name                                                                          |
| `debezium.sink.bigquerybatch.location`               | `US`               | Bigquery table location                                                                                    |
| `debezium.sink.bigquerybatch.project`                |                    | Bigquery project                                                                                           |
| `debezium.sink.bigquerybatch.create-disposition`     | `CREATE_IF_NEEDED` | Create tables if needed                                                                                    |
| `debezium.sink.bigquerybatch.partition-field`        | `__ts_ms`          | Partition target tables by `__ts_ms` field                                                                 |
| `debezium.sink.bigquerybatch.clustering-field`       | `__source_ts_ms`   | Cluster target tables by `PK + __source_ts_ms` field                                                       |
| `debezium.sink.bigquerybatch.partition-type`         | `MONTH`            | Partitioning type                                                                                          |
| `debezium.sink.bigquerybatch.allow-field-addition`   | `true`             | Allow field addition to target tables                                                                      |
| `debezium.sink.bigquerybatch.allow-field-relaxation` | `true`             | Allow field relaxation                                                                                     |
| `debezium.sink.bigquerybatch.credentials-file`       |                    | GCP service account credentialsFile                                                                        |
| `debezium.sink.bigquerybatch.cast-deleted-field`     | `false`            | Cast deleted field to boolean type(by default it is string type)                                           |
| `debezium.sink.bigquerybatch.writeDisposition`       | `WRITE_APPEND`     | Specifies the action that occurs if the destination table or partition already exists.                     |
| `debezium.sink.bigquerybatch.bigquery-custom-host`   |                    | Custom endpoint for BigQuery API. Useful for testing against a local BigQuery emulator like `bq-emulator`. |
| `debezium.sink.bigquerybatch.bigquery-dev-emulator`  | `false`            | Whether or not Debezium should connect to `bq-emulator` instance.                                          |

