FROM eclipse-temurin:21-jdk-jammy AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN mkdir -p lib classes \
    && curl -fL \
        https://repo.maven.apache.org/maven2/org/polypheny/polypheny-jdbc-driver/2.2/polypheny-jdbc-driver-2.2.jar \
        -o lib/polypheny-jdbc-driver.jar \
    && curl -fL \
        https://repo.maven.apache.org/maven2/org/duckdb/duckdb_jdbc/1.5.2.1/duckdb_jdbc-1.5.2.1.jar \
        -o lib/duckdb-jdbc.jar

COPY src/DuckDbJdbcBenchmark.java src/DuckDbJdbcBenchmark.java
COPY src/PolyphenyMqlBenchmark.java src/PolyphenyMqlBenchmark.java
COPY src/PolyphenyJdbcBenchmark.java src/PolyphenyJdbcBenchmark.java

RUN javac --release 21 \
    -cp "lib/*" \
    -d classes \
    src/PolyphenyMqlBenchmark.java \
    src/PolyphenyJdbcBenchmark.java \
    src/DuckDbJdbcBenchmark.java


FROM eclipse-temurin:21-jre-jammy AS java21

FROM apache/spark:3.5.3-java17-python3
USER root
ENV HOME=/tmp
ENV SPARK_SUBMIT_OPTS="-Duser.home=/tmp"
ENV SPARK_LOCAL_IP=127.0.0.1
WORKDIR /app

# Spark retains its Java 17 runtime; JDBC benchmarks use Java 21.
COPY --from=java21 /opt/java/openjdk/ /opt/java21/
COPY --from=builder /app/lib/ /app/lib/
COPY --from=builder /app/classes/ /app/classes/
COPY queries/ /app/queries/
COPY src/SparkSqlBenchmark.py /app/SparkSqlBenchmark.py
COPY entrypoint.sh /app/entrypoint.sh

ENTRYPOINT ["sh", "/app/entrypoint.sh"]
CMD ["PolyphenyMqlBenchmark"]
