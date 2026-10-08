#!/usr/bin/env python3
"""Run the host-side recovery benchmark and retain every run's summary."""

import argparse
import csv
import io
import statistics
import subprocess
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("executable", type=Path)
    parser.add_argument("output_dir", type=Path)
    parser.add_argument("--runs", type=int, default=5)
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("--runs must be positive")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    for run in range(1, args.runs + 1):
        result = subprocess.run([str(args.executable.resolve())], check=True,
                                capture_output=True, text=True)
        for record in csv.DictReader(io.StringIO(result.stdout)):
            rows.append({"run": run, **record})
    metrics = ("restore_median_ms", "restore_p99_ms",
               "replay_median_ms", "replay_p99_ms")
    with (args.output_dir / "recovery_bench_runs.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=("run", "allocations", *metrics, "trials"))
        writer.writeheader()
        writer.writerows(rows)
    with (args.output_dir / "recovery_bench.csv").open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(("allocations", *metrics, "trials"))
        for count in (1, 64, 256, 1024):
            group = [row for row in rows if int(row["allocations"]) == count]
            writer.writerow((count, *(f"{statistics.median(float(row[key]) for row in group):.6f}"
                                      for key in metrics), group[0]["trials"]))


if __name__ == "__main__":
    main()
