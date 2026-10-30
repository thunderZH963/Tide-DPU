#!/usr/bin/env python3
"""Build and run named host experiments; preserve raw outputs and failure evidence."""
import argparse
import csv
import datetime as dt
import json
import math
import os
from pathlib import Path
import platform
import subprocess
import sys
import time


def probe(command, cwd):
    try:
        return subprocess.check_output(command, cwd=cwd, stderr=subprocess.STDOUT,
                                       text=True, timeout=15).strip()
    except (OSError, subprocess.SubprocessError) as exc:
        return 'unavailable: ' + str(exc)


def execute(command, cwd, output, label, timeout, events):
    start = time.monotonic()
    event = {'command': list(map(str, command)), 'cwd': str(cwd), 'label': label}
    with (output / (label + '.log')).open('w') as log:
        try:
            result = subprocess.run(event['command'], cwd=cwd, stdout=log,
                                    stderr=subprocess.STDOUT, timeout=timeout)
            event['returncode'] = result.returncode
        except subprocess.TimeoutExpired:
            event.update(returncode=124, error='timeout')
        except OSError as exc:
            event.update(returncode=127, error=str(exc))
            log.write(str(exc) + '\n')
    event['elapsed_seconds'] = time.monotonic() - start
    events.append(event)
    (output / 'commands.json').write_text(json.dumps(events, indent=2) + '\n')
    if event['returncode']:
        raise RuntimeError(f"{label} failed ({event['returncode']}); see {output / (label + '.log')}")
    return event['elapsed_seconds']


def main():
    repo = Path(__file__).resolve().parents[1]
    config = json.loads((repo / 'scripts/experiments.json').read_text())
    cases = config['experiments']
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('experiment', choices=['all', *cases])
    parser.add_argument('--list', action='store_true')
    parser.add_argument('--repeat', type=int, default=1)
    parser.add_argument('--jobs', type=int, default=2)
    parser.add_argument('--timeout', type=float, default=300, help='Per experiment timeout, seconds')
    parser.add_argument('--build-timeout', type=float, default=1800)
    parser.add_argument('--sanitize', action='store_true')
    parser.add_argument('--build-type', choices=['Debug', 'Release', 'RelWithDebInfo'], default='Release')
    parser.add_argument('--build-dir', type=Path)
    parser.add_argument('--output', type=Path, help='New directory; existing paths are never overwritten')
    args = parser.parse_args()
    if args.list:
        for name, case in cases.items():
            print(f"{name:24} {case['kind']:20} {case['description']}")
        return 0
    if min(args.repeat, args.jobs, args.timeout, args.build_timeout) <= 0 or not all(math.isfinite(x) for x in (args.timeout, args.build_timeout)):
        parser.error('repeat, jobs and timeouts must be positive')
    build = (args.build_dir or repo / 'build' / ('experiments-' + args.build_type + ('-san' if args.sanitize else ''))).resolve()
    stamp = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')
    output = (args.output or repo / 'experiment-results' / (stamp + '-' + args.experiment)).resolve()
    output.mkdir(parents=True, exist_ok=False)
    names = list(cases) if args.experiment == 'all' else [args.experiment]
    metadata = {'started_utc': stamp, 'git_commit': probe(['git', 'rev-parse', 'HEAD'], repo),
                'git_status_before_run': probe(['git', 'status', '--porcelain'], repo),
                'platform': platform.platform(), 'machine': platform.machine(),
                'python': sys.version, 'cmake': probe(['cmake', '--version'], repo),
                'requested_cxx': os.environ.get('CXX', 'CMake default'),
                'build_type': args.build_type, 'sanitize': args.sanitize,
                'experiments': {n: cases[n] for n in names}, 'repeat': args.repeat,
                'status': 'running', 'timing_scope': 'Harness wall time is NOT query throughput or kernel latency.'}
    events = []
    meta_path = output / 'metadata.json'
    meta_path.write_text(json.dumps(metadata, indent=2) + '\n')
    print('Results:', output, flush=True)
    try:
        execute(['cmake', '-S', str(repo / config['source']), '-B', str(build),
                 '-DCMAKE_BUILD_TYPE=' + args.build_type,
                 '-D' + config['sanitize_option'] + '=' + ('ON' if args.sanitize else 'OFF'),
                 *config['cmake_args']], repo, output, 'configure', args.build_timeout, events)
        cache = build / 'CMakeCache.txt'
        if cache.exists():
            (output / 'CMakeCache.txt').write_bytes(cache.read_bytes())
            for line in cache.read_text().splitlines():
                if line.startswith('CMAKE_CXX_COMPILER:FILEPATH='):
                    metadata['compiler'] = probe([line.split('=', 1)[1], '--version'], repo)
        targets = sorted({cases[n]['target'] for n in names})
        execute(['cmake', '--build', str(build), '--parallel', str(args.jobs), '--target', *targets],
                repo, output, 'build', args.build_timeout, events)
        with (output / 'runs.csv').open('w', newline='') as stream:
            writer = csv.writer(stream)
            writer.writerow(['experiment', 'repetition', 'kind', 'harness_seconds', 'status'])
            for name in names:
                case = cases[name]
                for repetition in range(1, args.repeat + 1):
                    label = f'{name}-{repetition:03}'
                    elapsed = execute([str(build / case['executable']), *case['args']], build,
                                      output, label, args.timeout, events)
                    writer.writerow([name, repetition, case['kind'], f'{elapsed:.6f}', 'passed'])
                    stream.flush()
                print(name + ': passed', flush=True)
        metadata['status'] = 'passed'
        return 0
    except (RuntimeError, KeyboardInterrupt) as exc:
        metadata.update(status='failed', error=str(exc))
        print(str(exc), file=sys.stderr)
        return 1
    finally:
        metadata['finished_utc'] = dt.datetime.now(dt.timezone.utc).isoformat()
        meta_path.write_text(json.dumps(metadata, indent=2) + '\n')


if __name__ == '__main__':
    sys.exit(main())
