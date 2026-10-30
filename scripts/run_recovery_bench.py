#!/usr/bin/env python3
"""Run host recovery benchmarks with validated CSV and preserved raw run logs."""
import argparse
import csv
import io
import json
import math
from pathlib import Path
import platform
import statistics
import subprocess

METRICS = ('restore_median_ms', 'restore_p99_ms', 'replay_median_ms', 'replay_p99_ms')
FIELDS = ('allocations', *METRICS, 'trials')
COUNTS = (1, 64, 256, 1024)


def parse_run(text):
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames != list(FIELDS):
        raise ValueError('Unexpected recovery CSV schema')
    rows = list(reader)
    if len(rows) != len(COUNTS) or sorted(int(r['allocations']) for r in rows) != list(COUNTS):
        raise ValueError('Expected exactly one row for each allocation count')
    for row in rows:
        if None in row or any(value is None for value in row.values()):
            raise ValueError('Malformed CSV row')
        if int(row['trials']) <= 0:
            raise ValueError('Trials must be positive')
        for key in METRICS:
            value = float(row[key])
            if not math.isfinite(value) or value < 0:
                raise ValueError('Timing must be finite and nonnegative')
        for prefix in ('restore', 'replay'):
            if float(row[prefix + '_p99_ms']) < float(row[prefix + '_median_ms']):
                raise ValueError('p99 cannot be lower than median')
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('executable', type=Path)
    parser.add_argument('output_dir', type=Path)
    parser.add_argument('--runs', type=int, default=5)
    parser.add_argument('--timeout', type=float, default=300)
    args = parser.parse_args()
    if args.runs < 1 or args.timeout <= 0 or not math.isfinite(args.timeout):
        parser.error('runs and timeout must be positive')
    args.output_dir.mkdir(parents=True, exist_ok=False)
    metadata = {'status': 'running', 'platform': platform.platform(), 'runs': args.runs,
                'executable': str(args.executable.resolve()),
                'aggregation': 'Median across per-run quantiles; NOT pooled p99',
                'scope': 'Host reconstruction/replay only; excludes checkpoint I/O and startup'}
    rows = []
    try:
        with (args.output_dir / 'recovery_bench_runs.csv').open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=('run', *FIELDS))
            writer.writeheader()
            for run in range(1, args.runs + 1):
                raw = args.output_dir / f'run-{run:03}.csv'
                with raw.open('w') as stdout, (args.output_dir / f'run-{run:03}.stderr').open('w') as stderr:
                    subprocess.run([str(args.executable.resolve())], check=True,
                                   stdout=stdout, stderr=stderr, timeout=args.timeout)
                parsed = parse_run(raw.read_text())
                current = [{'run': run, **record} for record in parsed]
                rows.extend(current)
                writer.writerows(current)
                stream.flush()
        with (args.output_dir / 'recovery_bench.csv').open('w', newline='') as stream:
            writer = csv.writer(stream)
            writer.writerow(FIELDS)
            for count in COUNTS:
                group = [r for r in rows if int(r['allocations']) == count]
                if len({r['trials'] for r in group}) != 1:
                    raise ValueError('Trial counts differ between runs')
                writer.writerow((count, *(f'{statistics.median(float(r[k]) for r in group):.6f}'
                                         for k in METRICS), group[0]['trials']))
        metadata['status'] = 'passed'
    except BaseException as exc:
        metadata.update(status='failed', error=str(exc))
        raise
    finally:
        (args.output_dir / 'metadata.json').write_text(json.dumps(metadata, indent=2) + '\n')


if __name__ == '__main__':
    main()
