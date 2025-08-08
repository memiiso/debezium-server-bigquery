# Shared Configs for both consumers

| Config                                                        | Default                                                         | Description                                                                                                |
|---------------------------------------------------------------|-----------------------------------------------------------------|------------------------------------------------------------------------------------------------------------|
| `debezium.sink.batch.destination-regexp`                      | ``                                                              | Regexp to modify destination. With this its possible to map `table_ptt1`,`table_ptt2` to `table_combined`. |
| `debezium.sink.batch.destination-regexp-replace`              | ``                                                              | Regexp Replace part to modify destination                                                                  |
| `debezium.sink.batch.batch-size-wait`                         | `NoBatchSizeWait`                                               | Batch size wait strategy to optimize data files and upload interval. explained below.                      |
| `debezium.sink.batch.batch-size-wait.max-wait-ms`             | `300000`                                                        |                                                                                                            |
| `debezium.sink.batch.batch-size-wait.wait-interval-ms`        | `10000`                                                         |                                                                                                            |
| `debezium.source.max.batch.size`                              | `2048`                                                          |                                                                                                            |
| `debezium.format.value`                                       | `json`                                                          |                                                                                                            |
| `debezium.format.key`                                         | `json`                                                          |                                                                                                            |
| `debezium.source.time.precision.mode`                         | `isostring`                                                     |                                                                                                            |
| `debezium.source.decimal.handling.mode`                       | `double`                                                        |                                                                                                            |
| `debezium.format.value.schemas.enable`                        | `true`                                                          |                                                                                                            |
| `debezium.format.key.schemas.enable`                          | `true`                                                          |                                                                                                            |
| `debezium.source.offset.storage`                              | `io.debezium.server.bigquery.offset.BigqueryOffsetBackingStore` |                                                                                                            |
| `debezium.source.offset.storage.bigquery.table-name`          | `_debezium_offset_storage`                                      |                                                                                                            |
| `debezium.source.schema.history.internal`                     | `io.debezium.server.bigquery.history.BigquerySchemaHistory`     |                                                                                                            |
| `debezium.source.schema.history.internal.bigquery.table-name` | `_debezium_database_history_storage`                            |                                                                                                            |
| `debezium.transforms`                                         | `unwrap`                                                        |                                                                                                            |
| `debezium.transforms.unwrap.type`                             | `io.debezium.transforms.ExtractNewRecordState`                  |                                                                                                            |
| `debezium.transforms.unwrap.add.fields`                       | `op,table,source.ts_ms,db,ts_ms,ts_ns,source.ts_ns`             |                                                                                                            |
| `debezium.transforms.unwrap.delete.handling.mode`             | `rewrite`                                                       |                                                                                                            |
| `debezium.transforms.unwrap.drop.tombstones`                  | `true`                                                          |                                                                                                            |

## Data type mapping

Data type mapping listed below.

| Debezium Semantic Type             | Debezium Field Type | Bigquery Batch                    | Bigquery Stream                   | Notes                           |
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
| io.debezium.time.ZonedTime         | string              | TIME (STRING before<0.8.0.Beta)   | TIME (STRING before <0.8.0.Beta)  |                                 |
| io.debezium.data.Json              | string              | JSON                              | JSON                              |                                 |
| io.debezium.data.geometry.Geometry | struct              | STRUCT(srid:INT64, wkb:GEOGRAPHY) | STRUCT(srid:INT64, wkb:GEOGRAPHY) | version `0.8.0.Beta` and above. |
|                                    | string              | STRING                            | STRING                            |                                 |
|                                    | double              | FLOAT64                           | FLOAT64                           |                                 |
|                                    | float8-float64      | FLOAT64                           | FLOAT64                           |                                 |
|                                    | boolean             | BOOL                              | BOOL                              |                                 |
|                                    | bytes               | BYTES                             | BYTES (STRING before <0.8.0.Beta) |                                 |
|                                    | array               | ARRAY                             | ARRAY                             |                                 |
|                                    | map                 | STRUCT                            | STRUCT                            |                                 |
|                                    | struct              | STRUCT                            | STRUCT                            |                                 |

Handling of special fields:

| Field Name                  | Debezium Semantic Type | Debezium Field Type | Bigquery Batch | Bigquery Stream | Notes |
|-----------------------------|------------------------|---------------------|----------------|-----------------|-------|
| `__ts_ms`, `__source_ts_ms` |                        | int64               | TIMESTAMP      | TIMESTAMP       |       |
| `__deleted`                 |                        | string              | BOOL           | BOOL            |       |

### Mandatory config

#### Debezium Event format and schema

```properties
debezium.format.value=json
debezium.format.key=json
debezium.format.schemas.enable=true
```

#### Flattening Event Data

Bigquery consumers requires event flattening, please
see [debezium feature](https://debezium.io/documentation/reference/configuration/event-flattening.html#_configuration)

```properties
debezium.transforms=unwrap
debezium.transforms.unwrap.type=io.debezium.transforms.ExtractNewRecordState
debezium.transforms.unwrap.add.fields=op,table,lsn,source.ts_ms,source.ts_ns
debezium.transforms.unwrap.add.headers=db
debezium.transforms.unwrap.delete.handling.mode=rewrite
```

### Optimizing batch size (or commit interval)

Debezium extracts database events in real time and this could cause too frequent commits or too many small files
which is not optimal for batch processing especially when near realtime data feed is sufficient.
To avoid this problem following batch-size-wait classes are used.

Batch size wait adds delay between consumer calls to increase total number of events received per call and meanwhile
events are collected in memory.
This setting should be configured together with `debezium.source.max.queue.size` and `debezium.source.max.batch.size`
debezium properties

#### NoBatchSizeWait

This is default configuration, by default consumer will not use any wait. All the events are consumed immediately.

#### MaxBatchSizeWait

MaxBatchSizeWait uses debezium metrics to optimize batch size.
MaxBatchSizeWait periodically reads streaming queue current size and waits until number of events reaches
to `max.batch.size` or until `debezium.sink.batch.batch-size-wait.max-wait-ms`.

Maximum wait and check intervals are controlled by `debezium.sink.batch.batch-size-wait.max-wait-ms`
, `debezium.sink.batch.batch-size-wait.wait-interval-ms` properties.

example setup to receive ~2048 events per commit. maximum wait is set to 30 seconds, streaming queue current size
checked every 5 seconds

```properties
debezium.sink.batch.batch-size-wait=MaxBatchSizeWait
debezium.sink.batch.metrics.snapshot-mbean=debezium.postgres:type=connector-metrics,context=snapshot,server=testc
debezium.sink.batch.metrics.streaming-mbean=debezium.postgres:type=connector-metrics,context=streaming,server=testc
debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector
debezium.source.max.batch.size=2048;
debezium.source.max.queue.size=16000";
debezium.sink.batch.batch-size-wait.max-wait-ms=30000
debezium.sink.batch.batch-size-wait.wait-interval-ms=5000
```

## Bigquery Offset Storage

This implementation saves CDC offset to a bigquery table, along the destination data. With this no additional dependency
required to manage the application.

```
debezium.source.offset.storage=io.debezium.server.bigquery.offset.BigqueryOffsetBackingStore
debezium.source.offset.storage.bigquery.table-name=debezium_offset_storage_custom_table
```

## Bigquery Database History Storage

This implementation saves database history to a bigquery table, along the destination data. With this no additional
dependency required to manage the application.

```properties
debezium.source.database.history=io.debezium.server.bigquery.history.BigquerySchemaHistory
debezium.source.database.history.bigquery.table-name=__debezium_database_history_storage_test_table
```

## Configuring log levels

```properties
quarkus.log.level=INFO
# Ignore messages below warning level from Jetty, because it's a bit verbose
quarkus.log.category."org.eclipse.jetty".level=WARN
#
```