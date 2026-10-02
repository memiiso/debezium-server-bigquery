# `bigquerystream` Consumer

Streams Debezium events to BigQuery using the [Storage Write API](https://cloud.google.com/bigquery/docs/write-api-streaming).

## Configuration

| Config                                                   | Default          | Description                                                                                                                            |
|----------------------------------------------------------|------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| `debezium.sink.bigquerystream.dataset`                   |                  | Destination BigQuery dataset name                                                                                                      |
| `debezium.sink.bigquerystream.location`                  | `US`             | BigQuery table location (e.g. `US`, `EU`)                                                                                              |
| `debezium.sink.bigquerystream.project`                   |                  | Destination GCP BigQuery project ID                                                                                                    |
| `debezium.sink.bigquerystream.ignore-unknown-fields`     | `true`           | If `true`, unknown JSON fields sent to BigQuery will be ignored instead of causing an error                                            |
| `debezium.sink.bigquerystream.create-if-needed`          | `true`           | Creates target BigQuery table automatically if not found                                                                               |
| `debezium.sink.bigquerystream.partition-field`           | `__ts_ms`        | Field used to partition target BigQuery tables                                                                                         |
| `debezium.sink.bigquerystream.clustering-field`          | `__source_ts_ms` | Field used to cluster target BigQuery tables (combined with primary key fields)                                                        |
| `debezium.sink.bigquerystream.partition-type`            | `MONTH`          | Table partitioning type (`DAY`, `MONTH`, `YEAR`, `HOUR`)                                                                               |
| `debezium.sink.bigquerystream.allow-field-addition`      | `false`          | Allow schema expansion (adding new columns to target BigQuery table)                                                                   |
| `debezium.sink.bigquerystream.credentials-file`          |                  | Path to GCP service account JSON credentials file                                                                                       |
| `debezium.sink.bigquerystream.bigquery-custom-host`      |                  | Custom HTTP endpoint for BigQuery API. Useful for testing against a local emulator like `bq-emulator`.                                 |
| `debezium.sink.bigquerystream.bigquery-custom-grpc-host` |                  | Custom gRPC endpoint for BigQuery API. Useful for testing against a local emulator like `bq-emulator`.                                 |
| `debezium.sink.bigquerystream.bigquery-dev-emulator`     | `false`          | Whether to connect to a local `bq-emulator` instance                                                                                   |
| `debezium.sink.bigquerystream.upsert`                    | `false`          | Enables UPSERT mode using [BigQuery CDC](https://cloud.google.com/bigquery/docs/change-data-capture)                                   |
| `debezium.sink.bigquerystream.upsert-keep-deletes`       | `true`           | Retains deleted rows in target table when UPSERT mode is active                                                                        |
| `debezium.sink.bigquerystream.deduplicator`              | `RetainLast`     | Deduplication strategy before streaming: `RetainLast`, `TimestampAndOp`, `ChangeSequence`, or custom FQCN class name           |
| `debezium.sink.bigquerystream.upsert-dedup-column`       | `__source_ts_ns` | Timestamp column used for UPSERT deduplication (row with highest value is retained when using `TimestampAndOp` deduplicator)          |
| `debezium.sink.bigquerystream.upsert-op-column`          | `__op`           | Operation type column used for tie-breaking when deduplication timestamp values are identical                                          |
| `debezium.sink.bigquerystream.change-sequence.enabled`   | `false`          | Enables fine-grained `_CHANGE_SEQUENCE_NUMBER` generation for precise BigQuery CDC ordering                                            |
| `debezium.sink.bigquerystream.change-sequence.strategy`  |                  | Strategy to extract CDC coordinates: `mysql`, `postgres`, or custom FQCN class name (inferred from connector class if omitted)          |
| `debezium.sink.bigquerystream.max-in-flight-appends`     | `1`              | Maximum concurrent append requests pipelined within one destination batch                                                              |

### Upsert

By default, the `bigquerystream` consumer runs in append-only mode (`debezium.sink.bigquerystream.upsert=false`).

When `debezium.sink.bigquerystream.upsert=true`, the consumer uses primary key fields to perform UPSERT (overwrite/delete followed by insert) on the destination table via BigQuery CDC. Tables without a primary key automatically fall back to append mode.

### Upsert Mode Data Deduplication

In UPSERT mode, in-memory event deduplication is performed prior to streaming to eliminate duplicate mutations within the same batch.

The deduplication strategy is configurable via `debezium.sink.bigquerystream.deduplicator` (default: `RetainLast`):

- **`RetainLast` (Default)**: Retains the latest event in arrival order for each primary key. Because Debezium delivers events in database commit order, this approach guarantees that subsequent updates or deletes cleanly overwrite prior operations without parsing overhead.
- **`TimestampAndOp`**: Retains the event with the highest timestamp (configured via `debezium.sink.bigquerystream.upsert-dedup-column`). Operation type priorities (`{"c": 1, "r": 2, "u": 3, "d": 4}`) break ties when timestamps match.
- **`ChangeSequence`**: Compares fine-grained source coordinates (LSN, binlog offset) extracted by the configured `ChangeSequenceStrategy`. Recommended when CDC change sequencing is enabled.
- **Custom Deduplicator**: You can provide a custom deduplicator implementing `io.debezium.server.bigquery.deduplication.BatchDeduplicator`. Configure either the fully qualified class name or `@Named` CDI bean name.

### Custom CDC Ordering

Change sequencing is optional and applies to tables utilizing BigQuery CDC/UPSERT mode. When enabled (`debezium.sink.bigquerystream.change-sequence.enabled=true`), every UPSERT and DELETE mutation receives the Storage Write API pseudocolumn `_CHANGE_SEQUENCE_NUMBER`. Retained delete records (`debezium.sink.bigquerystream.upsert-keep-deletes=true`) receive both an UPSERT change type and a sequence value. The pseudocolumn is embedded in the Storage Write Protobuf payload but is not written as a physical table column.

Configure the Debezium `unwrap` SMT to expose all required source metadata fields:

For MySQL:

```properties
debezium.transforms.unwrap.add.fields=op,source.ts_ms,source.ts_ns,source.file,source.pos,source.row
debezium.sink.bigquerystream.change-sequence.enabled=true
```

For PostgreSQL:

```properties
debezium.source.provide.transaction.metadata=true
debezium.transforms.unwrap.add.fields=op,source.ts_ms,source.ts_ns,source.lsn,source.txId,transaction.total_order
debezium.sink.bigquerystream.change-sequence.enabled=true
```

Sequences use four uppercase, zero-padded, 16-character hexadecimal sections formatted as:

```text
MySQL:      source.ts_ns/binlog-file-index/source.pos/source.row
PostgreSQL: source.ts_ns/source.lsn/source.txId/transaction.total_order
Format:     %016X/%016X/%016X/%016X
```

The binlog file index is parsed from the trailing numeric component of `__source_file` (e.g., `mysql-bin.001234` becomes `1234`). MySQL requires `__source_ts_ns`, `__source_file`, `__source_pos`, and `__source_row`. PostgreSQL requires `__source_ts_ns`, `__source_lsn`, `__source_txId`, and `__transaction_total_order`. PostgreSQL transaction metadata must be enabled (`debezium.source.provide.transaction.metadata=true`) to ensure distinct sequence numbers for multiple row updates within the same transaction/LSN.

#### Strategy Resolution & Extensibility

When `debezium.sink.bigquerystream.change-sequence.strategy` is omitted, the sequence extraction strategy is automatically inferred based on the configured source connector (`debezium.source.connector.class`):
- `io.debezium.connector.mysql.MySqlConnector` -> `mysql` strategy
- `io.debezium.connector.postgresql.PostgresConnector` -> `postgres` strategy

You can also explicitly specify the strategy or provide a custom strategy class:
```properties
# Explicitly choose built-in strategy
debezium.sink.bigquerystream.change-sequence.strategy=postgres

# Or use a custom strategy implementing ChangeSequenceStrategy
debezium.sink.bigquerystream.change-sequence.strategy=com.example.MyCustomSequenceStrategy
```

If an unsupported connector is configured when `change-sequence.enabled=true`, the consumer fails fast during startup with an informative configuration exception.

### Append and Destination Concurrency

`debezium.sink.batch.concurrent-uploads` processes multiple destination tables concurrently.
`debezium.sink.bigquerystream.max-in-flight-appends` pipelines append requests within a single destination table. The default value (`1`) maintains synchronous single-append execution. Values greater than `1` split destination batches into balanced chunks, sending them concurrently via the BigQuery Storage Write API and waiting for all completions.

If any append request fails, times out, or encounters an exception, the entire batch fails without marking offsets as committed. Retrying a failed sequenced batch is safe and idempotent because deterministic source coordinates generate identical sequence numbers.

When setting `max-in-flight-appends` above 1 for UPSERT tables, ensure `debezium.sink.bigquerystream.change-sequence.enabled=true` so out-of-order response completions do not alter final CDC state in BigQuery.

