# Tide Artifact

This anonymous artifact contains the Tide runtime core used by the accompanying EDBT submission. Tide adds tenant-aware scheduling, execution-aware admission, lifetime accounting, and coordinated Pushdown/Pullup placement to a DPU database runtime.

## Contents

- `fpdb-tide/`: dependency-free C++17 runtime core and unit tests.
- `INTEGRATION.md`: mapping from the runtime API to TapDB actors, `TupleSet`, and HBTable.
- `scripts/run_tests.sh`: clean configure, build, and test command.

## Quick start

Requirements: CMake 3.16 or newer and a C++17 compiler.

```bash
./scripts/run_tests.sh
```

The test executable covers memory reservation and materialization, execution-slot admission, finish/release lifetime separation, stale-generation rejection, weighted scheduling, joint Pushdown/Pullup budgets, and versioned placement commits.

## Scope

The artifact isolates Tide's control-plane mechanisms from CAF, Arrow, and vendor libraries so they can be built and tested on a regular machine. `INTEGRATION.md` describes the adapter boundary used to connect these mechanisms to TapDB. Pushdown executes on the storage-side DPU; Pullup executes on the compute tier.

## Anonymity

The repository contains no author names, affiliations, submission identifiers, or machine-specific paths. Please report artifact questions through the anonymous submission channel during review.

