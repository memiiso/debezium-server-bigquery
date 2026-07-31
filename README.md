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
