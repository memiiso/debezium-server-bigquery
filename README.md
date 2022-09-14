[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)
![Java CI with Maven](https://github.com/memiiso/debezium-server-bigquery/workflows/Java%20CI%20with%20Maven/badge.svg?branch=master)

# Table of contents

* [Debezium Bigquery Consumers](#debezium-bigquery-consumers)
    * [`bigquerybatch` Consumer](#bigquerybatch-consumer)
    * [`bigquerystream` Consumer](#bigquerystream-consumer)
* [Install from source](#install-from-source)

# Debezium Bigquery Consumers

This project adds Bigquery consumers
to [Debezium Server](https://debezium.io/documentation/reference/operations/debezium-server.html).
This consumer replicates RDBMS CDC events to Bigquery in real time.

![Debezium Bigquery Consumers](docs/images/debezium-batch.png)

## `bigquerybatch` Consumer

Writes debezium events to Bigquery
using [BigQuery Storage Write API](https://cloud.google.com/bigquery/docs/write-api).
It groups CDC events and appends to destination BigQuery
table [using BigQuery Write API](https://cloud.google.com/bigquery/docs/batch-loading-data#loading_data_from_local_files)
.

| Config                                             | Default            | Description                                                                                               |
|----------------------------------------------------|--------------------|-----------------------------------------------------------------------------------------------------------|
| `debezium.sink.bigquerybatch.dataset`              |                    | Destination Bigquery dataset name                                                                         |
| `debezium.sink.bigquerybatch.location`             | `US`               | Bigquery table location                                                                                   |
| `debezium.sink.bigquerybatch.project`              |                    | Bigquery project                                                                                          |
| `debezium.sink.bigquerybatch.createDisposition`    | `CREATE_IF_NEEDED` | Create tables if needed                                                                                   |
| `debezium.sink.bigquerybatch.partition-field`      | `__ts_ms`          | Partition target tables by `__ts_ms` field                                                                  |
| `debezium.sink.bigquerybatch.clustering-field`     | `__source_ts_ms`   | Cluster target tables by `PK + __source_ts_ms` field                                                        |
| `debezium.sink.bigquerybatch.partitionType`        | `MONTH`            | Partitioning type                                                                                         |
| `debezium.sink.bigquerybatch.allowFieldAddition`   | `true`             | Allow field addition to target tables                                                                     |
| `debezium.sink.bigquerybatch.allowFieldRelaxation` | `true`             | Allow field relaxation                                                                                    |
| `debezium.sink.bigquerybatch.credentialsFile`      |                    | GCP service account credentialsFile                                                                       |
| `debezium.sink.bigquerybatch.cast-deleted-field`   | `false`            | Cast deleted field to boolean type(by default it is string type)                                          |
| `debezium.sink.batch.destination-regexp`           | ``                 | Regexp to modify destination. With this its possible to map `table_ptt1`,`table_ptt2` to `table_combined`. |
| `debezium.sink.batch.destination-regexp-replace`   | ``                 | Regexp Replace part to modify destination                                                                 |
| `debezium.sink.batch.batch-size-wait`              | `NoBatchSizeWait`  | Batch size wait strategy to optimize data files and upload interval. explained below.                     |

## `bigquerystream` Consumer

Streams debezium events to Bigquery using
the [Storage Write API](https://cloud.google.com/bigquery/docs/write-api-streaming).

| Config                                             | Default           | Description                                                                                                |
|----------------------------------------------------|-------------------|------------------------------------------------------------------------------------------------------------|
| `debezium.sink.bigquerystream.dataset`             |                   | Destination Bigquery dataset name                                                                          |
| `debezium.sink.bigquerystream.location`            | `US`              | Bigquery table location                                                                                    |
| `debezium.sink.bigquerystream.project`             |                   | Bigquery project                                                                                           |
| `debezium.sink.bigquerystream.ignoreUnknownFields` |                   | if true, unknown Json fields to BigQuery will be ignored instead of error out.                             |
| `debezium.sink.bigquerystream.createIfNeeded`      | `true`            | Creates Bigquery table if not found                                                                        |
| `debezium.sink.bigquerystream.partition-field`     | `__ts_ms`         | Partition target tables by `__ts_ms` field                                                                  |
| `debezium.sink.bigquerystream.clustering-field`    | `__source_ts_ms`  | Cluster target tables by `PK + __source_ts_ms` field                                                        |
| `debezium.sink.bigquerystream.partitionType`       | `MONTH`           | Partitioning type                                                                                          |
| `debezium.sink.bigquerystream.allowFieldAddition`  | `false`           | Allow field addition to target tables                                                                      |
| `debezium.sink.bigquerystream.credentialsFile`     |                   | GCP service account credentialsFile                                                                        |
| `debezium.sink.bigquerystream.cast-deleted-field`  | `false`           | Cast deleted field to boolean type(by default it is string type)                                           |
| `debezium.sink.batch.destination-regexp`           | ``                | Regexp to modify destination. With this its possible to map `table_ptt1`,`table_ptt2` to `table_combined`. |
| `debezium.sink.batch.destination-regexp-replace`   | ``                | Regexp Replace part to modify destination                                                                  |
| `debezium.sink.batch.batch-size-wait`              | `NoBatchSizeWait` | Batch size wait strategy to optimize data files and upload interval. explained below.                      |

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
debezium.transforms.unwrap.add.fields=op,table,lsn,source.ts_ms
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

## Configuring log levels

```properties
quarkus.log.level=INFO
# Ignore messages below warning level from Jetty, because it's a bit verbose
quarkus.log.category."org.eclipse.jetty".level=WARN
#
```

# Install from source

- Requirements:
    - JDK 11
    - Maven
- Clone from repo: `git clone https://github.com/memiiso/debezium-server-bigquery.git`
- From the root of the project:
    - Build and package debezium server: `mvn -Passembly -Dmaven.test.skip package`
    - After building, unzip your server
      distribution: `unzip debezium-server-bigquery-dist/target/debezium-server-bigquery-dist*.zip -d appdist`
    - cd into unzipped folder: `cd appdist`
    - Create `application.properties` file and config it: `nano conf/application.properties`, you can check the example
      configuration
      in [application.properties.example](debezium-server-bigquery-sinks/src/main/resources/conf/application.properties.example)
    - Run the server using provided script: `bash run.sh`

# Contributing

The Memiiso community welcomes anyone that wants to help out in any way, whether that includes reporting problems,
helping with documentation, or contributing code changes to fix bugs, add tests, or implement new features.

### Contributors

<a href="https://github.com/memiiso/debezium-server-bigquery/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=memiiso/debezium-server-bigquery" />
</a>