FROM eclipse-temurin:21-jdk-jammy AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl ca-certificates tar gzip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /ycsbExternal

RUN curl -fL \
        https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz \
        -o /tmp/ycsb.tar.gz \
    && tar -xzf /tmp/ycsb.tar.gz \
        -C /ycsbExternal --strip-components=1 \
    && rm /tmp/ycsb.tar.gz

RUN curl -fL \
        https://repo.maven.apache.org/maven2/com/google/code/gson/gson/2.11.0/gson-2.11.0.jar \
        -o /ycsbExternal/lib/gson-2.11.0.jar

COPY libs/polyphenyJdbcDrivers/new-driver.jar \
    /ycsbExternal/lib/polypheny-jdbc-driver.jar

COPY docker/ycsb/binding/PolyphenyMongoBinding.java \
    /tmp/binding/PolyphenyMongoBinding.java

RUN mkdir -p /tmp/binding/classes \
    && javac --release 21 \
        -cp "/ycsbExternal/lib/*" \
        -d /tmp/binding/classes \
        /tmp/binding/PolyphenyMongoBinding.java \
    && jar --create \
        --file /ycsbExternal/lib/polypheny-ycsb-binding.jar \
        -C /tmp/binding/classes .


FROM eclipse-temurin:21-jre-jammy

WORKDIR /ycsbExternal

COPY --from=builder /ycsbExternal/ /ycsbExternal/
