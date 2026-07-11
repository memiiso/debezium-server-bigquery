FROM maven:3.9.9-eclipse-temurin-21 as builder
ARG RELEASE_VERSION=0.0.1-SNAPSHOT
RUN apt-get -qq update && apt-get -qq install unzip

# Copy parent and module pom.xml files to cache dependencies
COPY pom.xml /app/
COPY debezium-server-bigquery-sinks/pom.xml /app/debezium-server-bigquery-sinks/
COPY debezium-server-bigquery-dist/pom.xml /app/debezium-server-bigquery-dist/
WORKDIR /app
RUN mvn dependency:go-offline -B -Drevision=${RELEASE_VERSION}

# Copy source and build
COPY . /app
RUN mvn clean package -Passembly -Dmaven.test.skip --quiet -Drevision=${RELEASE_VERSION}
RUN unzip /app/debezium-server-bigquery-dist/target/debezium-server-bigquery-dist-${RELEASE_VERSION}.zip -d appdist

FROM eclipse-temurin:21-jre

# Create non-root user
RUN groupadd -r debezium && useradd -r -g debezium -d /app debezium

# Copy distribution files and set ownership
COPY --from=builder --chown=debezium:debezium /app/appdist/debezium-server-bigquery/ /app/

# Ensure run.sh is executable and create conf and data directories
RUN chmod +x /app/run.sh && \
    mkdir -p /app/conf /app/data && \
    chown -R debezium:debezium /app

USER debezium
WORKDIR /app
EXPOSE 8080 8083
VOLUME ["/app/conf", "/app/data"] 

ENTRYPOINT ["/app/run.sh"]