# Data Type Mapping

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