# TapDB Integration

Tide is inserted at TapDB's actor send path while retaining the existing operator DAG, CAF messages, `TupleSet`, serialization, and reliable transport.

## Process objects

Each executor and storage-server process owns:

- one `ResourceLedger`;
- one `HierarchicalScheduler`;
- one `TransportManager`;
- one `PlacementBudgetLedger` and `PlacementController` in the periodic controller.

## Lifecycle mapping

1. After physical planning assigns an operator identifier, call `open` with its tenant, fragment, legal Pushdown/Pullup implementations, and generation.
2. When dependencies become ready, enqueue the fragment's next bounded batch in the hierarchical scheduler.
3. Before sending its `TupleSet`, call `acquire`. `WouldBlock` leaves the batch ready until a slot, credit, or memory reservation becomes available.
4. Attach tenant, fragment, batch, allocation, and generation identifiers to the message envelope.
5. Call `push` as reserved bytes materialize.
6. Call `finish` when the operator handler completes. This reconciles service and returns the backend execution slot, while live output remains charged.
7. Call `release` after the final consumer drops the output. This returns occupied or reserved bytes and the allocation credit.
8. Call `close` to reject new batches for the fragment generation.

## Stateful work

HBTable partitions carry the same tenant, allocation, and generation identifiers. Spill reduces occupied memory only after the local-NVMe write completes. Running stateful fragments stay with Pushdown; only unstarted fragments or partitions may select Pullup.

## Placement publication

For each bounded cohort of unstarted fragments, provide predicted demand vectors for both Pushdown and Pullup. The Pullup vector includes network traffic. `placeAndCommit` computes against a versioned snapshot and commits the full cohort only if the budget version is unchanged. A stale pass changes no live reservation.

## Account checkpoint fixture

`TenantAccount::checkpoint()` exports the tenant policy, service totals, next allocation ID, and live allocation records. `ResourceLedger::restoreTenant()` validates those records before admitting new work. The `tide-recovery-test` fixture writes this image to a file, terminates the writer process, and restores it in another process. It verifies that retained bytes still block a new grant and that a replayed final release returns credit only once. Production persistence and coordination with query/transport recovery remain the embedding database's responsibility.
