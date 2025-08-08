[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)
![Java CI with Maven](https://github.com/memiiso/debezium-server-bigquery/workflows/Java%20CI%20with%20Maven/badge.svg?branch=master)

# Debezium Bigquery Consumers

This project adds Bigquery consumers
to [Debezium Server](https://debezium.io/documentation/reference/operations/debezium-server.html).
These consumers replicate Given Database to Bigquery in real time.

* Debezium Bigquery Consumers
    * [`bigquerybatch` Consumer (Uses BQ Free API)](https://memiiso.github.io/debezium-server-bigquery/bigquerybatch/)
    * [`bigquerystream` Consumer](https://memiiso.github.io/debezium-server-bigquery/bigquerystream/)

# Install from source

- Requirements:
    - JDK 21
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
