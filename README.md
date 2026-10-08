# Tide Control Library

This repository contains Tide's C++ control-library prototype and tests. Tide adds tenant-aware scheduling, execution-aware admission, lifetime accounting, and coordinated Pushdown/Pullup placement to a DPU database runtime.

## Contents

- `fpdb-tide/`: dependency-free C++17 runtime core and unit tests.
- `data/`: raw five-run recovery timings and the corresponding aggregate used in the paper figure.
- `INTEGRATION.md`: mapping from the runtime API to TapDB actors, `TupleSet`, and HBTable.
- `scripts/run_tests.sh`: clean configure, build, and test command.
- `scripts/run_recovery_bench.py`: repeat the host-side recovery microbenchmark and save raw and aggregate CSV files.

## Quick start

Requirements: CMake 3.16 or newer and a C++17 compiler.

```bash
./scripts/run_tests.sh
```

The test executables cover memory reservation and materialization, execution-slot admission, finish/release lifetime separation, stale-generation rejection, weighted scheduling, joint Pushdown/Pullup budgets, and versioned placement commits. A controlled process-restart fixture reconstructs a saved account and checks retained memory, replayed final release, and monotonic allocation IDs.
After building, run `python3 scripts/run_recovery_bench.py /path/to/tide-recovery-bench /path/to/output --runs 5` to repeat the host-side reconstruction and replay measurements. Each run uses 100 trials after 10 warmups for 1--1024 live allocations. The benchmark excludes checkpoint I/O and process startup.

## Scope

The artifact isolates Tide's control-plane mechanisms from CAF, Arrow, and vendor libraries so they can be built and tested on a regular machine. `INTEGRATION.md` describes the adapter boundary used to connect these mechanisms to TapDB. Pushdown executes on the storage-side DPU; Pullup executes on the compute tier.

The library and tests build without TapDB dependencies. `INTEGRATION.md` describes the database hooks; it is an adapter plan, not a complete TapDB integration or a DPU hardware benchmark.
