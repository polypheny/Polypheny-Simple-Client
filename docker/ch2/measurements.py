"""Collect worker timings without writing to stdout for each transaction."""
import json
import math
import os
import time
from collections import Counter, defaultdict
from pathlib import Path


class Measurements:
    def __init__(self, client_id, kind):
        self.client_id = client_id
        self.kind = kind
        self.start = time.monotonic_ns()
        self.latencies = defaultdict(Counter)
        self.aborted = Counter()
        self.errors = Counter()

    def record(self, operation, start, status="ok"):
        if status == "ok":
            elapsed_micros = (time.monotonic_ns() - start) // 1000
            self.latencies[operation][max(1, elapsed_micros)] += 1
        elif status == "aborted":
            self.aborted[operation] += 1
        else:
            self.errors[operation] += 1

    def save(self):
        directory = os.environ.get("CH2_METRICS_DIRECTORY")
        if not directory:
            return
        data = dict(start=self.start, stop=time.monotonic_ns(), kind=self.kind,
                    latencies=self.latencies, aborted=self.aborted, errors=self.errors)
        Path(directory, f"{os.getpid()}-{self.client_id}.json").write_text(json.dumps(data))


def summarize(directory):
    workers = [json.loads(path.read_text()) for path in Path(directory).glob("*.json")]
    if not workers:
        raise RuntimeError("CH2 produced no worker measurements")
    runtime = (max(w["stop"] for w in workers) - min(w["start"] for w in workers)) / 1000
    histograms = defaultdict(Counter)
    aborted, errors = Counter(), Counter()
    for worker in workers:
        aborted.update(worker["aborted"])
        errors.update(worker["errors"])
        for operation, buckets in worker["latencies"].items():
            histograms[operation].update({int(latency): count for latency, count in buckets.items()})
    operations = {}
    for operation in sorted(histograms.keys() | aborted.keys() | errors.keys()):
        histogram = histograms[operation]
        count = sum(histogram.values())
        total = sum(latency * n for latency, n in histogram.items())
        percentiles = {}
        cumulative = 0
        for latency, n in sorted(histogram.items()):
            cumulative += n
            for percentile in (95, 99):
                if cumulative >= math.ceil(count * percentile / 100):
                    percentiles.setdefault(percentile, latency)
        operations[operation] = {
            "operations": count,
            "totalMicros": total,
            "averageMicros": total / count if count else 0,
            "p95Micros": percentiles.get(95, 0),
            "p99Micros": percentiles.get(99, 0),
            "aborted": aborted[operation],
            "errors": errors[operation],
        }
    return {
        "runtimeMicros": runtime,
        "operations": operations,
        "transactionClients": sum(worker["kind"] == "T" for worker in workers),
        "analyticalClients": sum(worker["kind"] == "A" for worker in workers),
    }
