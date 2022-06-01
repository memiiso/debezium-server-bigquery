[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)
![Java CI with Maven](https://github.com/memiiso/debezium-server-bigquery/workflows/Java%20CI%20with%20Maven/badge.svg?branch=master)

# Table of contents

* [Debezium Bigquery Consumers](#debezium-bigquery-consumers)
    * [`bigquerybatch` Consumer](#bigquerybatch-consumer)
    * [`bigquerystream` Consumer](#bigquerystream-consumer)
* [Install from source](#install-from-source)

# Debezium Batch Consumers

This project adds Bigquery consumers
to [Debezium Server](https://debezium.io/documentation/reference/operations/debezium-server.html). Using this consumers
its possible to consume CDC events to bigquery.

![Debezium Batch Consumers](docs/images/debezium-batch.png)

## `bigquerybatch` Consumer

Consumes debezium events to Bigquery tables
using [BigQuery Storage Write API](https://cloud.google.com/bigquery/docs/write-api).
It groups and writes events to jsonlines file per destination. Then appends jsonlines to destination BigQuery table
using BigQuery Write API.

| Config                                             | Default            | Description                                                                           |
|----------------------------------------------------|--------------------|---------------------------------------------------------------------------------------|
| `debezium.sink.bigquerybatch.dataset`              |                    | Destination Bigquery dataset name                                                     |
| `debezium.sink.bigquerybatch.location`             | `US`               | Bigquery table location                                                               |
| `debezium.sink.bigquerybatch.project`              |                    | Bigquery project                                                                      |
| `debezium.sink.bigquerybatch.createDisposition`    | `CREATE_IF_NEEDED` | Create tables if needed                                                               |
| `debezium.sink.bigquerybatch.partitionField`       | `source_ts`        | Partition target tables by field                                                      |
| `debezium.sink.bigquerybatch.partitionType`        | `MONTH`            | Partitioning type                                                                     |
| `debezium.sink.bigquerybatch.allowFieldAddition`   | `true`             | Allow field addition to target tables                                                 |
| `debezium.sink.bigquerybatch.allowFieldRelaxation` | `true`             | Allow field relaxation                                                                |
| `debezium.sink.bigquerybatch.credentialsFile`      |                    | GCP service account credentialsFile                                                   |
| `debezium.sink.bigquerybatch.cast-deleted-field`   | `false`            | Cast deleted field to bolean type(by default it is string mode)                       |
| `debezium.sink.batch.destination-regexp`           | ``                 | Regexp to modify destination                                                          |
| `debezium.sink.batch.destination-regexp-replace`   | ``                 | Regexp Replace part to modify destination                                             |
| `debezium.sink.batch.batch-size-wait`              | `NoBatchSizeWait`  | Batch size wait strategy to optimize data files and upload interval. explained below. |

## `bigquerystream` Consumer

WIP

### Mandatory config

#### Debezium Event format and schema

```properties
debezium.format.value=json
debezium.format.key=json
debezium.format.schemas.enable=true
```

#### Flattening Event Data

Batch consumer requires event flattening, please
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

#### DynamicBatchSizeWait

**Deprecated**
This wait strategy dynamically adds wait to increase batch size. Wait duration is calculated based on number of
processed events in
last 3 batches. if last batch sizes are lower than `max.batch.size` Wait duration will increase and if last batch sizes
are bigger than 90% of `max.batch.size` Wait duration will decrease

This strategy optimizes batch size between 85%-90% of the `max.batch.size`, it does not guarantee consistent batch size.

example setup to receive ~2048 events per commit. maximum wait is set to 5 seconds

```properties
debezium.source.max.queue.size=16000
debezium.source.max.batch.size=2048
debezium.sink.batch.batch-size-wait=DynamicBatchSizeWait
debezium.sink.batch.batch-size-wait.max-wait-ms=5000
```

#### MaxBatchSizeWait

MaxBatchSizeWait uses debezium metrics to optimize batch size, this strategy is more precise compared to
DynamicBatchSizeWait.
MaxBatchSizeWait periodically reads streaming queue current size and waits until it reaches to `max.batch.size`.
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
