"""Run upstream CH2, validate loads and export numeric measurements."""
import argparse
import json
import os
import runpy
import tempfile
import time

from measurements import summarize
from setup import validate_load
from drivers.polyphenydriver import load_queries


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--no-execute", action="store_true")
    parser.add_argument("--warehouses", type=int, default=1)
    parser.add_argument("--scalefactor", type=float, default=1)
    parser.add_argument("--aclients", type=int, default=0)
    args, _ = parser.parse_known_args()
    if args.aclients > 0:
        load_queries(os.environ.get("CH2_NAMESPACE", "ch2"))
    if args.no_execute:
        result = load_data(args)
    else:
        result = run_workload()
    print("[CH2] " + json.dumps(result), flush=True)


def load_data(args):
    validate_load(args.warehouses, args.scalefactor, empty=True)
    start = time.monotonic_ns()
    runpy.run_path("tpcc.py", run_name="__main__")
    elapsed = (time.monotonic_ns() - start) / 1000
    counts = validate_load(args.warehouses, args.scalefactor)
    return {
        "runtimeMicros": elapsed,
        "loadCounts": counts,
        "operations": {
            "INSERT": {"operations": sum(counts.values()), "errors": 0, "aborted": 0},
        },
    }


def run_workload():
    with tempfile.TemporaryDirectory(prefix="ch2-metrics-") as directory:
        os.environ["CH2_METRICS_DIRECTORY"] = directory
        runpy.run_path("tpcc.py", run_name="__main__")
        return summarize(directory)


if __name__ == "__main__":
    main()
