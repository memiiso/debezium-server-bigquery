# Data Type Mapping

Data type mapping listed below.

| Debezium Semantic Type          | Debezium Field Type | Bigquery Batch | Bigquery Stream | Notes |
|---------------------------------|---------------------|----------------|-----------------|-------|
|                                 | int8-int64          | INT64          | INT64           |       |
| io.debezium.time.Date           | int32               | DATE           | DATE            |       |
| io.debezium.time.Timestamp      | int64               | INT64          | INT64           |       |
| io.debezium.time.MicroTimestamp | int64               | INT64          | INT64           |       |
| io.debezium.time.NanoTimestamp  | int64               | INT64          | INT64           |       |
| io.debezium.time.ISODate        | string              | DATE           | DATE            |       |
| io.debezium.time.ISODateTime    | string              | DATETIME       | DATETIME        |       |
| io.debezium.time.ISOTime        | string              | TIME           | TIME            |       |
| io.debezium.time.ZonedTimestamp | string              | TIMESTAMP      | TIMESTAMP       |       |
| io.debezium.time.ZonedTime      | string              | STRING         | STRING          |       |
| io.debezium.data.Json           | string              | JSON           | JSON            |       |
|                                 | string              | STRING         | STRING          |       |
|                                 | double              | FLOAT64        | FLOAT64         |       |
|                                 | float8-float64      | FLOAT64        | FLOAT64         |       |
|                                 | boolean             | BOOL           | BOOL            |       |
|                                 | bytes               | BYTES          | STRING          |       |
|                                 | array               | ARRAY          | ARRAY           |       |
|                                 | map                 | STRUCT         | STRUCT          |       |
|                                 | struct              | STRUCT         | STRUCT          |       |
|                                 |                     |                |                 |       |
|                                 |                     |                |                 |       |
|                                 |                     |                |                 |       |
|                                 |                     |                |                 |       |



