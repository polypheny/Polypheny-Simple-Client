FROM eclipse-temurin:21-jdk-jammy AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN mkdir -p lib classes \
    && curl -fL \
        https://repo.maven.apache.org/maven2/org/polypheny/polypheny-jdbc-driver/2.2/polypheny-jdbc-driver-2.2.jar \
        -o lib/polypheny-jdbc-driver.jar

COPY src/PolyphenyMqlBenchmark.java src/PolyphenyMqlBenchmark.java
COPY src/PolyphenyJdbcBenchmark.java src/PolyphenyJdbcBenchmark.java

RUN javac --release 21 \
    -cp "lib/*" \
    -d classes \
    src/PolyphenyMqlBenchmark.java \
    src/PolyphenyJdbcBenchmark.java


FROM eclipse-temurin:21-jre-jammy

WORKDIR /app

COPY --from=builder /app/lib/ /app/lib/
COPY --from=builder /app/classes/ /app/classes/
COPY queries/ /app/queries/

ENTRYPOINT ["java", "-cp", "/app/classes:/app/lib/*"]
CMD ["PolyphenyMqlBenchmark"]
