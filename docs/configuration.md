# Configuration Guide

This page details shared configuration settings, data type mappings, batch optimization strategies, and internal state storage mechanisms for Debezium BigQuery consumers.

## Shared Consumer Configurations

| Config                                                        | Default                                                         | Description                                                                                                |
|---------------------------------------------------------------|-----------------------------------------------------------------|------------------------------------------------------------------------------------------------------------|
| `debezium.sink.batch.destination-regexp`                      |                                                                 | Regex pattern to modify target table names. Allows mapping tables like `table_p1`, `table_p2` to `table_combined`. |
| `debezium.sink.batch.destination-regexp-replace`              |                                                                 | Replacement string for `destination-regexp`.                                                               |
| `debezium.sink.batch.batch-size-wait`                         | `NoBatchSizeWait`                                               | Strategy to wait for bigger batches before committing (`NoBatchSizeWait`, `MaxBatchSizeWait`, `DynamicBatchSizeWait`). |
| `debezium.sink.batch.batch-size-wait.max-wait-ms`             | `300000`                                                        | Maximum wait duration (ms) before committing a batch when using batch size wait strategies.                |
| `debezium.sink.batch.batch-size-wait.wait-interval-ms`        | `10000`                                                         | Interval (ms) to check queue size during batch wait.                                                       |
| `debezium.sink.batch.nested-as-json`                          | `false`                                                         | Serialize nested struct/map fields as JSON strings rather than expanded BigQuery STRUCTs.                  |
| `debezium.sink.batch.concurrent-uploads`                      | `1`                                                             | Number of concurrent threads for uploading files/data to destination tables.                               |
| `debezium.sink.batch.concurrent-uploads.timeout-minutes`      | `60`                                                            | Timeout (minutes) for concurrent table uploads.                                                            |
| `debezium.source.max.batch.size`                              | `2048`                                                          | Maximum number of records consumed in a single batch.                                                      |
| `debezium.format.value`                                       | `json`                                                          | Value format produced by Debezium (must be `json`).                                                        |
| `debezium.format.key`                                         | `json`                                                          | Key format produced by Debezium (must be `json`).                                                          |
| `debezium.source.time.precision.mode`                         | `isostring`                                                     | Time precision mode for Debezium events (`isostring`, `adaptive`, `adaptive_time_microseconds`).            |
| `debezium.source.decimal.handling.mode`                       | `double`                                                        | Decimal handling mode (`double`, `string`, `precise`).                                                      |
| `debezium.format.value.schemas.enable`                        | `true`                                                          | Enables inline schemas in JSON event values.                                                               |
| `debezium.format.key.schemas.enable`                          | `true`                                                          | Enables inline schemas in JSON event keys.                                                                 |
| `debezium.source.offset.storage`                              | `io.debezium.server.bigquery.offset.BigqueryOffsetBackingStore` | Class for storing CDC offset state in BigQuery.                                                            |
| `debezium.source.offset.storage.bigquery.table-name`          | `_debezium_offset_storage`                                      | BigQuery table name used for offset storage.                                                               |
| `debezium.source.offset.storage.bigquery.migrate-offset-file` |                                                                 | Path to a legacy file-based offset store (e.g. `data/offsets.dat`) to migrate into BigQuery upon table creation. |
| `debezium.source.schema.history.internal`                     | `io.debezium.server.bigquery.history.BigquerySchemaHistory`     | Class for storing database schema history in BigQuery.                                                     |
| `debezium.source.schema.history.internal.bigquery.table-name` | `_debezium_database_history_storage`                            | BigQuery table name used for schema history storage.                                                       |
| `debezium.source.schema.history.internal.bigquery.migrate-history-file` |                                                       | Path to a legacy file-based schema history store (e.g. `data/dbhistory.txt`) to migrate into BigQuery upon table creation. |
| `debezium.source.topic.heartbeat.prefix`                      | `__debezium-heartbeat`                                          | Topic name prefix for Debezium heartbeat messages (automatically skipped by consumer).                     |
| `debezium.source.topic.heartbeat.skip-consuming`              | `true`                                                          | Controls whether to skip processing heartbeat topics.                                                      |
| `debezium.source.include.schema.changes`                      | `false`                                                         | Controls whether to include schema change (DDL) events.                                                     |
| `debezium.transforms`                                         | `unwrap`                                                        | Event transformation alias.                                                                                |
| `debezium.transforms.unwrap.type`                             | `io.debezium.transforms.ExtractNewRecordState`                  | Event flattening transform type.                                                                           |
| `debezium.transforms.unwrap.add.fields`                       | `op,table,source.ts_ms,db,ts_ms,ts_ns,source.ts_ns`             | Metadata fields added to flattened event payload.                                                          |
| `debezium.transforms.unwrap.delete.tombstone.handling.mode`   | `rewrite`                                                       | Handling mode for deleted records (`rewrite` adds `__deleted` field).                                      |
| `debezium.transforms.unwrap.drop.tombstones`                  | `true`                                                          | Whether to drop tombstone records.                                                                         |

---

## Data Type Mapping

The table below maps Debezium field types to target BigQuery column types:

| Debezium Semantic Type             | Debezium Field Type | BigQuery Batch                    | BigQuery Stream                   | Notes                           |
|------------------------------------|---------------------|-----------------------------------|-----------------------------------|---------------------------------|
|                                    | int8-int64          | INT64                             | INT64                             |                                 |
| io.debezium.time.Date              | int32               | DATE                              | DATE                              |                                 |
| io.debezium.time.Timestamp         | int64               | INT64                             | INT64                             |                                 |
| io.debezium.time.MicroTimestamp    | int64               | INT64                             | INT64                             |                                 |
| io.debezium.time.NanoTimestamp     | int64               | INT64                             | INT64                             |                                 |
| io.debezium.time.IsoDate           | string              | DATE                              | DATE                              |                                 |
| io.debezium.time.IsoTimestamp      | string              | DATETIME                          | DATETIME                          |                                 |
| io.debezium.time.IsoTime           | string              | TIME                              | TIME                              |                                 |
| io.debezium.time.ZonedTimestamp    | string              | TIMESTAMP                         | TIMESTAMP                         |                                 |
| io.debezium.time.ZonedTime         | string              | TIME                              | TIME                              |                                 |
| io.debezium.data.Json              | string              | JSON                              | JSON                              |                                 |
| io.debezium.data.geometry.Geometry | struct              | STRUCT(srid:INT64, wkb:GEOGRAPHY) | STRUCT(srid:INT64, wkb:GEOGRAPHY) | Supported in `0.8.0.Beta`+.     |
|                                    | string              | STRING                            | STRING                            |                                 |
|                                    | double              | FLOAT64                           | FLOAT64                           |                                 |
|                                    | float8-float64      | FLOAT64                           | FLOAT64                           |                                 |
|                                    | boolean             | BOOL                              | BOOL                              |                                 |
|                                    | bytes               | BYTES                             | BYTES                             |                                 |
|                                    | array               | ARRAY                             | ARRAY                             | Expanded unless `nested-as-json=true` |
|                                    | map                 | STRUCT                            | STRUCT                            | Expanded unless `nested-as-json=true` |
|                                    | struct              | STRUCT                            | STRUCT                            | Expanded unless `nested-as-json=true` |

### Special Field Mappings

| Field Name                  | Debezium Field Type | BigQuery Target Type | Description                                                    |
|-----------------------------|---------------------|----------------------|----------------------------------------------------------------|
| `__ts_ms`, `__source_ts_ms` | int64               | TIMESTAMP            | Event timestamp in milliseconds converted to BigQuery TIMESTAMP|
| `__deleted`                 | string / boolean    | BOOL                 | Indicates if record was soft-deleted (`true`/`false`)           |

---

## Required Baseline Configuration

### 1. Debezium Event Format & Schema

```properties
debezium.format.value=json
debezium.format.key=json
debezium.format.schemas.enable=true
debezium.format.key.schemas.enable=true
```

### 2. Flattening Event Data

Debezium BigQuery consumers require event flattening via `ExtractNewRecordState`:

```properties
debezium.transforms=unwrap
debezium.transforms.unwrap.type=io.debezium.transforms.ExtractNewRecordState
debezium.transforms.unwrap.add.fields=op,table,source.ts_ms,db,ts_ms,ts_ns,source.ts_ns
debezium.transforms.unwrap.delete.tombstone.handling.mode=rewrite
debezium.transforms.unwrap.drop.tombstones=true
```

---

## Optimizing Batch Size & Commit Intervals

When consuming real-time CDC events, frequent small commits can lead to rate limits or small data files. You can configure batch wait strategies to group events in memory before writing.

### 1. `NoBatchSizeWait` (Default)
Consumes events immediately as they arrive without introducing additional wait time.

### 2. `MaxBatchSizeWait`
Monitors internal queue metrics and delays commits until the batch size reaches `debezium.source.max.batch.size` or `debezium.sink.batch.batch-size-wait.max-wait-ms` expires.

```properties
debezium.sink.batch.batch-size-wait=MaxBatchSizeWait
debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector
debezium.source.max.batch.size=2048
debezium.source.max.queue.size=16000
debezium.sink.batch.batch-size-wait.max-wait-ms=30000
debezium.sink.batch.batch-size-wait.wait-interval-ms=5000
```

### 3. `DynamicBatchSizeWait`
Dynamically adapts sleep times based on historical batch sizes to target 85%-90% capacity of `debezium.source.max.batch.size`.

```properties
debezium.sink.batch.batch-size-wait=DynamicBatchSizeWait
debezium.source.max.batch.size=2048
debezium.sink.batch.batch-size-wait.max-wait-ms=300000
```

---

## BigQuery Offset Storage

`BigqueryOffsetBackingStore` persists Debezium offsets directly into a BigQuery table in your dataset, eliminating the need for external Kafka/Zookeeper state stores.

```properties
debezium.source.offset.storage=io.debezium.server.bigquery.offset.BigqueryOffsetBackingStore
debezium.source.offset.storage.bigquery.table-name=_debezium_offset_storage
```

### Migrating Existing Offsets

To migrate existing offset state from a local file store (e.g. `data/offsets.dat`) to BigQuery, configure:

```properties
debezium.source.offset.storage.bigquery.migrate-offset-file=data/offsets.dat
```

When the BigQuery offset storage table is created for the first time, existing offset records will automatically be read from the specified file and written into BigQuery.

---

## BigQuery Schema History Storage

`BigquerySchemaHistory` stores database schema history directly in BigQuery.

```properties
debezium.source.schema.history.internal=io.debezium.server.bigquery.history.BigquerySchemaHistory
debezium.source.schema.history.internal.bigquery.table-name=_debezium_database_history_storage
```

### Migrating Existing Schema History

To migrate existing schema history state from a local file store (e.g. `data/dbhistory.txt`) to BigQuery, configure:

```properties
debezium.source.schema.history.internal.bigquery.migrate-history-file=data/dbhistory.txt
```

When the BigQuery schema history storage table is created for the first time, existing schema history records will automatically be read from the specified file and populated into BigQuery.

---

## Logging Configuration

Log levels are configured via Quarkus application properties:

```properties
quarkus.log.level=INFO
quarkus.log.category."org.eclipse.jetty".level=WARN
quarkus.log.category."com.google.cloud.bigquery".level=INFO
```