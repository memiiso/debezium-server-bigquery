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
| `debezium.sink.bigquerystream.change-sequence.enabled`   | `false`          | Add BigQuery `_CHANGE_SEQUENCE_NUMBER` custom CDC ordering values to every CDC mutation.                                              |
| `debezium.sink.bigquerystream.max-in-flight-appends`     | `1`              | Maximum append requests pipelined within one destination batch. Values below 1 are rejected.                                         |
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

When `debezium.sink.bigquerystream.change-sequence.enabled=true`, deduplication instead compares the complete source
coordinate described below. This resolves ties across binlog files, positions, and rows at the same nanosecond.

### Custom CDC ordering

Change sequencing is optional and only applies to tables that actually use BigQuery CDC/upsert mode. When enabled, every
UPSERT and DELETE mutation receives the Storage Write API pseudocolumn `_CHANGE_SEQUENCE_NUMBER`; retained delete rows
(`upsert-keep-deletes=true`) receive both an UPSERT change type and a sequence. The pseudocolumn is included in the
Storage Write protobuf schema but is never created as a physical BigQuery table column.

Configure the Debezium unwrap transform to expose all required source fields:

```properties
debezium.transforms.unwrap.add.fields=op,source.ts_ms,source.ts_ns,source.file,source.pos,source.row
debezium.sink.bigquerystream.change-sequence.enabled=true
```

The sequence format is four uppercase, zero-padded, 16-character hexadecimal sections:

```text
source.ts_ns/binlog-file-index/source.pos/source.row
%016X/%016X/%016X/%016X
```

The binlog file index is the trailing numeric component of `__source_file` (for example, `mysql-bin.001234` becomes
`1234`). `__source_ts_ns`, `__source_file`, `__source_pos`, and `__source_row` must be present and valid on every CDC
mutation. Missing, malformed, negative, or larger-than-64-bit components fail the complete Debezium batch; the connector
does not silently emit an unsequenced mutation.

### Append and destination concurrency

`debezium.sink.batch.concurrent-uploads` processes different destination tables concurrently.
`debezium.sink.bigquerystream.max-in-flight-appends` pipelines append requests within one destination. The default value
of `1` preserves the synchronous single-append behavior. Values above 1 split a destination batch into at most that many
balanced chunks, submit them to the BigQuery default stream, and wait for every response even when completions arrive out
of order.

An append response error, exception, cancellation, timeout, or interruption fails the whole batch. Already-submitted
futures are observed before failure is reported, and Debezium offsets are not marked processed or batch-finished unless
every destination and every append succeeds. Replaying a failed sequenced batch is safe because source coordinates produce
the same ordering values.

Benchmark values above 1 for the workload. For upsert destinations, use change sequencing when enabling append
pipelining so out-of-order append completion cannot change the final CDC state.
