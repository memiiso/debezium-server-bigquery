[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)
![Java CI with Maven](https://github.com/memiiso/debezium-server-bigquery/workflows/Java%20CI%20with%20Maven/badge.svg?branch=master)

# Debezium BigQuery Consumers

This project adds BigQuery sink consumers to [Debezium Server](https://debezium.io/documentation/reference/operations/debezium-server.html). These consumers replicate change data capture (CDC) events from databases to Google BigQuery in real time.

* **Debezium BigQuery Consumers:**
    * [`bigquerybatch` Consumer](https://memiiso.github.io/debezium-server-bigquery/bigquerybatch/) - Uses standard BigQuery Load Jobs (no streaming ingestion fees).
    * [`bigquerystream` Consumer](https://memiiso.github.io/debezium-server-bigquery/bigquerystream/) - Uses high-throughput BigQuery Storage Write API with real-time streaming and optional CDC Upsert support.

## Key Features

- **Batch & Streaming Modes:** Support for both standard BigQuery Load Jobs and the high-throughput BigQuery Storage Write API.
- **CDC Upsert & Deletion Handling:** Real-time deduplication and UPSERT mode using BigQuery CDC.
- **Embedded Storage Extensions:** Built-in BigQuery implementations for Debezium Offset Storage (`BigqueryOffsetBackingStore`) and Schema History (`BigquerySchemaHistory`).
- **Dynamic Batch Optimization:** Configurable batch size wait strategies (`MaxBatchSizeWait`, `DynamicBatchSizeWait`) to optimize file sizes and upload intervals.
- **Nested JSON Serialization:** Configurable handling of nested record structures as JSON strings (`debezium.sink.batch.nested-as-json`).

## Versioning Policy

`debezium-server-bigquery` follows an upstream-anchored versioning scheme:

```
<debezium-major>.<debezium-minor>.<debezium-patch>.<sink-revision>.<qualifier>
Example: 3.6.3.0.Final
```

- **Debezium Version** (`3.6.3`): Matches the exact upstream Debezium release bundled in the server distribution and container.
- **Sink Revision** (`0`, `1`, `2`): Incremented for BigQuery sink bug fixes, improvements, or features independent of Debezium version updates.
- **Qualifier** (`Beta`, `Final`): Indicates testing (`Beta`, `Beta2`) or production-ready general availability (`Final`).

### Version Compatibility Matrix

| Release | Upstream Debezium | Java Baseline | Quarkus Version | Notes |
| :--- | :--- | :--- | :--- | :--- |
| `3.6.3.0.Final` | `3.6.3.Final` | Java 21 | 3.15.x | Debezium 3.6 baseline, safe CDC sequencing & append pipelining |
| `0.12.0.Final` | `3.1.3.Final` | Java 21 | 3.15.x | BigQuery CDC sequencing & pipeline |
| `0.9.3.Final` | `3.1.3.Final` | Java 21 | 3.8.x | Debezium 3.1 upgrade |
| `0.6.0.Final` | `2.7.3.Final` | Java 17 | 3.2.x | Debezium 2.7 upgrade |

Container images are published to GitHub Container Registry:
```bash
docker pull ghcr.io/memiiso/debezium-server-bigquery:3.6.3.0.Final
# Floating convenience tags for the latest patch and minor versions:
docker pull ghcr.io/memiiso/debezium-server-bigquery:3.6.3
docker pull ghcr.io/memiiso/debezium-server-bigquery:3.6
docker pull ghcr.io/memiiso/debezium-server-bigquery:latest
```

## Build and Install from Source

### Prerequisites
- JDK 21 or later
- Apache Maven 3.6.3 or later

### Installation Steps

1. **Clone the repository:**
   ```bash
   git clone https://github.com/memiiso/debezium-server-bigquery.git
   cd debezium-server-bigquery
   ```

2. **Build and package:**
   ```bash
   mvn clean package -Passembly -DskipTests
   ```

3. **Unzip the distribution package:**
   ```bash
   unzip debezium-server-bigquery-dist/target/debezium-server-bigquery-dist*.zip -d appdist
   cd appdist
   ```

4. **Configure the application:**
   Edit `conf/application.properties` (refer to [application.properties.example](debezium-server-bigquery-sinks/src/main/resources/conf/application.properties.example) for baseline settings).

5. **Run the server:**
   ```bash
   bash run.sh
   ```

### Running via Docker

Build and run using the provided multi-stage `Dockerfile`:

```bash
# Build the container image
docker build -t debezium-server-bigquery .

# Run with custom configuration and data volumes
docker run -d --name debezium-bigquery \
  -v $(pwd)/conf:/app/conf \
  -v $(pwd)/data:/app/data \
  debezium-server-bigquery
```


## Contributing

We welcome contributions of any kind! Feel free to report issues, suggest improvements, or submit pull requests.

### Contributors

<a href="https://github.com/memiiso/debezium-server-bigquery/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=memiiso/debezium-server-bigquery" />
</a>
