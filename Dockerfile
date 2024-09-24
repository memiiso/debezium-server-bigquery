FROM eclipse-temurin:17-jdk as builder
RUN apt-get -qq update && apt-get -qq install maven unzip
COPY . /app
WORKDIR /app
RUN mvn clean package -Passembly -Dmaven.test.skip --quiet
RUN unzip /app/debezium-server-bigquery-dist/target/debezium-server-bigquery-dist*.zip -d appdist

FROM eclipse-temurin:17-jre
COPY --from=builder /app/appdist/debezium-server-bigquery/ /app/

WORKDIR /app
EXPOSE 8080 8083
VOLUME ["/app/conf", "/app/data"] 

ENTRYPOINT ["/app/run.sh"]