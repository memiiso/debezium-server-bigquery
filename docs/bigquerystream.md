# `bigquerystream` Consumer

Streams debezium events to Bigquery using
the [Storage Write API](https://cloud.google.com/bigquery/docs/write-api-streaming).

## Configuration

| Config                                                   | Default          | Description                                                                                                                            |
|----------------------------------------------------------|------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| `debezium.sink.bigquerystream.dataset`                   |                  | Destination Bigquery dataset name                                                                                                      |
| `debezium.sink.bigquerystream.location`                  | `US`             | Bigquery table location                                                                                                                |
| `debezium.sink.bigquerystream.project`                   |                  | Bigquery project                                                                                                                       |
| `debezium.sink.bigquerystream.ignore-unknown-fields`     | `true`           | if true, unknown Json fields to BigQuery will be ignored instead of error out.                                                         |
| `debezium.sink.bigquerystream.create-if-needed`          | `true`           | Creates Bigquery table if not found                                                                                                    |
| `debezium.sink.bigquerystream.partition-field`           | `__ts_ms`        | Partition target tables by `__ts_ms` field                                                                                             |
| `debezium.sink.bigquerystream.clustering-field`          | `__source_ts_ms` | Cluster target tables by `PK + __source_ts_ms` field                                                                                   |
| `debezium.sink.bigquerystream.partition-type`            | `MONTH`          | Partitioning type                                                                                                                      |
| `debezium.sink.bigquerystream.allow-field-addition`      | `false`          | Allow field addition to target tables                                                                                                  |
| `debezium.sink.bigquerystream.credentials-file`          |                  | GCP service account credentialsFile                                                                                                    |
| `debezium.sink.bigquerystream.bigquery-custom-host`      |                  | Custom endpoint for BigQuery API. Useful for testing against a local BigQuery emulator like `bq-emulator`.                             |
| `debezium.sink.bigquerystream.bigquery-custom-grpc-host` |                  | Custom endpoint for BigQuery GRPC API. Useful for testing against a local BigQuery emulator like `bq-emulator`.                        |
| `debezium.sink.bigquerystream.bigquery-dev-emulator`     | `false`          | Whether or not Debezium should connect to `bq-emulator` instance.                                                                      |
| `debezium.sink.bigquerystream.upsert`                    | `false`          | Running upsert mode overwriting updated rows. Using [Bigquery CDC feature](https://cloud.google.com/bigquery/docs/change-data-capture) |
| `debezium.sink.bigquerystream.upsert-keep-deletes`       | `true`           | With upsert mode, keeps deleted rows in bigquery table.                                                                                |
| `debezium.sink.bigquerystream.upsert-dedup-column`       | `__source_ts_ns` | With upsert mode used to deduplicate data. row with highest `__source_ts_ns` is kept.                                                  |
| `debezium.sink.bigquerystream.upsert-op-column`          | `__op`           | Used with upsert mode to deduplicate data when `__source_ts_ns` of rows are same.                                                      |
| `debezium.sink.bigquerystream.cast-deleted-field`        | `false`          | Cast deleted field to boolean type(by default it is string type)                                                                       |

### Upsert

By default, Bigquery Streaming consumer is running with append mode
`debezium.sink.bigquerystream.upsert=false`.
Upsert mode uses source Primary Key and does upsert on target table(delete followed by insert). For the tables without
Primary Key consumer falls back to append mode.

### Upsert Mode Data Deduplication

With upsert mode data deduplication is done. Deduplication is done based on `__source_ts_ns` value and event type `__op`
.
its is possible to change this field using `debezium.sink.bigquerystream.upsert-dedup-column=__source_ts_ns` (Currently
only
Long field type supported.)

Operation type priorities are `{"c":1, "r":2, "u":3, "d":4}`. When two records with same key and same `__source_ts_ns`
values received then the record with higher `__op` priority is kept and added to destination table and duplicate record
is dropped from stream.

