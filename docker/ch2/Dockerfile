
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

RUN pip install --no-cache-dir \
    numpy==2.0.2 \
    python-dateutil==2.9.0.post0 \
    ujson==5.11.0 \
    requests==2.32.5 \
    polypheny==1.0.0

# Polypheny 1.0.0 assumes one TCP read contains a whole response.
COPY docker/ch2/prism-read-exact.patch /tmp/prism-read-exact.patch
RUN patch "$(python -c 'import polypheny.rpc; print(polypheny.rpc.__file__)')" /tmp/prism-read-exact.patch \
    && rm /tmp/prism-read-exact.patch

WORKDIR /ch2External/ch2driver/pytpcc

COPY docker/ch2/polyphenydriver.py ./drivers/polyphenydriver.py

COPY docker/ch2/setup.py docker/ch2/run_ch2.py docker/ch2/measurements.py docker/ch2/analytical_queries.py ./
COPY docker/ch2/queries/ ./queries/
