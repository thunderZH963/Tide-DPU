# Tide Control Library

This repository contains Tide's C++ control-library prototype and tests. Tide adds tenant-aware scheduling, execution-aware admission, lifetime accounting, and coordinated Pushdown/Pullup placement to a DPU database runtime.

## Contents

- `fpdb-tide/`: dependency-free C++17 runtime core, tests, and executable database adapter example.
- `data/`: historical paper recovery timings; `data/recovery-2026-10-22/` contains a fresh five-run measurement of the updated code and its host/build metadata.
- `INTEGRATION.md`: TapDB mapping and required adapter boundaries for FlexPushdownDB, Taurus, openGauss NDPPlugin, and S3 Select.
- `scripts/run_tests.sh`: clean configure, build, and test command.
- `scripts/run_recovery_bench.py`: repeat the host-side recovery microbenchmark and save raw and aggregate CSV files.

## Quick start

Requirements: CMake 3.16 or newer and a C++17 compiler.

```bash
./scripts/run_tests.sh
```

The test executables cover weighted service and reconciliation, fragment fanout, debt windows, bounded progress, admission retries, shared device memory, incremental reservations, durable spill/reload, finish/release ordering, stale generations, versioned placement, and concurrent terminal callbacks. A controlled process-restart fixture reconstructs a saved account and checks retained memory, replayed final release, and monotonic allocation IDs. Three adapter-contract scenarios additionally check split/merge readers, a page response with a separately charged compute continuation, and shared-memory contention between frontends. Tests remain active in Release builds.
After building, run `python3 scripts/run_recovery_bench.py /path/to/tide-recovery-bench /path/to/output --runs 5` to repeat the host-side reconstruction and replay measurements. Each run uses 100 trials after 10 warmups for 1--1024 live allocations. The benchmark excludes checkpoint I/O and process startup.

To retain a build and run the example directly:

```bash
cmake -S fpdb-tide -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j 2
(cd build && ctest --output-on-failure)
./build/tide-adapter-demo
```

The example executes a filter and a grouped sum through the runtime interfaces, checks their results, writes and synchronizes an aggregation partition to a temporary file, reloads it on the same backend, and verifies that every allocation is released. It illustrates the hooks for native operators; it is not an SSB/TPC-H benchmark or a TapDB execution engine.

For memory and undefined-behavior checks, configure a separate build with `-DTIDE_ENABLE_SANITIZERS=ON -DCMAKE_BUILD_TYPE=Debug`, build it, and run CTest from its build directory.

## Runtime interfaces

- `TenantAccount::reserveMore` extends an existing batch allocation without another credit. Failed extensions leave all counters unchanged.
- `TransportManager::spillComplete` lowers occupied memory only after the database acknowledges a durable spill. Each allocation has unique spill IDs; replayed completions are ignored, including after account restoration.
- The manager's fifth constructor argument sets the actual shared DPU-memory limit. It checks all tenants' Pushdown allocations atomically and rebuilds retained output charges after restoration; Pullup memory still counts toward its tenant limit.
- `registerConsumer`, `sealOutput`, and `releaseConsumer` track multi-reader output in the core, including duplicate callbacks and checkpoint replay. See the registered-consumer contract in `INTEGRATION.md`.
- `finish` returns the execution slot and unused reservation. Occupied output and its allocation credit remain charged until final consumption.
- `onFinalConsumer` handles either order of completion and consumption. An early consumer event is remembered, and the allocation is released after producer completion.
- `HierarchicalScheduler::next` selects a tenant by weighted virtual service and then rotates its fragments. `finish` reconciles estimated service with measured service; `defer` rolls back a selection blocked by admission.
- `SchedulerPolicy{10, 1, true}` enables service-debt admission with a 10-ms base quantum and a 1-ms progress allowance. Call `advanceWindow()` on the runtime's window timer. Debt above the weighted quantum blocks ordinary work; explicitly marked release-enabling work can consume the bounded allowance.
- `PlacementBudgetLedger::release(fragment, generation)` returns exactly the named committed reservation. Duplicate or stale releases cannot create capacity.

See [INTEGRATION.md](INTEGRATION.md) for call ordering, identity rules, recovery, and the TapDB mapping.

## Scope

The artifact isolates Tide's control-plane mechanisms from CAF, Arrow, and vendor libraries so they can be built and tested on a regular machine. `INTEGRATION.md` describes the adapter boundary used to connect these mechanisms to TapDB. Pushdown executes on the storage-side DPU; Pullup executes on the compute tier.

The library and tests build without TapDB dependencies. The executable example exercises real control-library calls and small native operators. `INTEGRATION.md` describes the TapDB hook mapping; compiling these tests does not establish a complete TapDB integration or a DPU hardware benchmark.
