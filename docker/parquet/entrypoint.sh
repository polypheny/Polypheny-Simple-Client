#!/bin/sh
set -eu
case "${1:-}" in
    spark)
        shift
        exec /opt/spark/bin/spark-submit --master 'local[2]' --driver-memory 4g /app/SparkSqlBenchmark.py "$@"
        ;;
    PolyphenyJdbcBenchmark|PolyphenyMqlBenchmark|DuckDbJdbcBenchmark)
        exec /opt/java21/bin/java -cp '/app/classes:/app/lib/*' "$@"
        ;;
    *)
        echo 'Expected PolyphenyJdbcBenchmark, PolyphenyMqlBenchmark, DuckDbJdbcBenchmark or spark' >&2
        exit 2
        ;;
esac
