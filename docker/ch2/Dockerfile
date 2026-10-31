FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl ca-certificates tar gzip build-essential cmake libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /ch2External

RUN curl -fL \
        https://github.com/couchbaselabs/ch2/archive/refs/heads/main.tar.gz \
        -o /tmp/ch2.tar.gz \
    && tar -xzf /tmp/ch2.tar.gz \
        -C /ch2External --strip-components=1 \
    && rm /tmp/ch2.tar.gz

WORKDIR /ch2External/ch2driver/pytpcc
