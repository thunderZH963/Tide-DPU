# Database Integration

Tide controls dispatch and resource lifetime while the embedding database owns its physical DAG, operators, tuple representation, reliable transport, and SQL semantics. `fpdb-tide/examples/database_adapter.cpp` is a runnable integration example. The TapDB mapping below shows where to attach the same interfaces to CAF actors, `TupleSet`, and HBTable.

## Process objects and identity

Create one `ResourceLedger`, one `HierarchicalScheduler`, and one `TransportManager` per execution process. Register each tenant once. The controller owns a separate `PlacementBudgetLedger` and `PlacementController` for its shared CPU/memory/network budget.

Configure the actual shared DPU-memory cap through the manager's fifth constructor argument, for example `TransportManager(ledger, scheduler, 8, 8, 8ULL << 30)`. The default leaves this shared cap unbounded for source compatibility; a device deployment must set it. Every Pushdown reservation and extension checks both the tenant and shared limit under the manager lock. Materialization preserves the total charge; durable spill, unused-reservation refund, and final release return capacity. Pullup memory is charged to its tenant but does not consume this DPU pool. Use the manager's mutation APIs consistently while it is running; direct mutations of a managed account would bypass the device gate.

`FragmentId` must be unique within a transport manager and placement ledger; an adapter can map `(query_id, operator_id, partition_id)` to a generated ID. Batch IDs are unique within a fragment generation. Allocation IDs are tenant-local and monotonic, so every message carries the tenant and generation along with the allocation ID. Reusing a batch ID requires a new fragment generation.

`Grant` is an opaque admission token: preserve all its fields. The manager checks its tenant, allocation, generation, backend, fragment, batch, and original reservation. Changing its backend cannot return a different execution slot.

## Batch lifecycle

1. Call `open(FragmentSpec, placement)` after choosing an unstarted fragment's backend. `open` checks tenant registration and legal implementations.
2. When dependencies are satisfied, enqueue `ReadyBatch{tenant, fragment, batch, estimated_service_ms, generation}`. Only dependency-ready work belongs in the scheduler. For each backlogged tenant, the scheduler rotates among ready fragments.
3. `next()` returns the next weighted-service selection. Pass its identity to `acquire(BatchRequest{..., memory_bytes})`. If admission returns `WouldBlock`, call `scheduler.defer(selected)` and retry after a slot, memory, or credit event. Preserve the returned selection token when deferring: a stale retry cannot refund another selection.
4. `Granted` returns the canonical `Grant`. A repeated batch acquisition returns `DuplicateBatch` without a new allocation or slot. It must not execute the operator again.
5. Before allocating beyond the original reservation, call `extend(grant, additional_bytes)`. A failed extension changes no counters: yield without exposing partial logical output, and wait for a release or a spill completion. Each successful `push(grant, bytes)` transfers that allocation's bytes from reserved to occupied; publish the corresponding buffer only after this check succeeds.
6. `finish(tenant, grant, measured_cpu_ms, network_bytes)` seals production, charges service once, returns the execution slot, and refunds unused reserved bytes. Occupied bytes and the credit remain live.
7. Invoke `onFinalConsumer(tenant, allocation, generation)` after the last consumer releases the allocation. The callback is safe before or after `finish`; an early event remains pending until producer completion. Repeated and stale events return false without changing counters. An output with no consumers still needs this callback. The lower-level `release` API requires the allocation to have already finished.
8. `close(fragment, generation)` stops new admissions. Opening the next generation requires the old one to be closed and every old allocation released. An active hash table cannot change backend through `open`.

The scheduler precharges the selected batch's estimated CPU service divided by tenant weight. A successful manager completion replaces that estimate with measured service exactly once. `defer` removes the estimate for work that never acquired capacity. Empty tenants gain no historical entitlement while idle. Completed scheduler identities remain tombstones for the scheduler's lifetime; retire the scheduler with its query/executor context after all callbacks have drained, rather than recycling batch identities in place.

Enable the paper's debt gate with `HierarchicalScheduler(ledger, SchedulerPolicy{10, 1, true})` and call `advanceWindow()` at the runtime's accounting interval. The weighted quantum is the base quantum times tenant weight. Completion updates service debt by measured minus predicted work, bounded below by zero. Unused quantum repays debt at a window boundary. While debt exceeds the tenant's quantum, ordinary batches remain queued. A batch marked `progress = true` may use the progress allowance to release existing memory or complete a spill; this flag is a trusted database capability, not a tenant-supplied request. Selection reserves estimated service and completion reconciles actual service, so an overrun blocks later progress grants. The database must yield long-running work to enforce a hard execution-time bound. Default construction keeps debt gating off until a runtime supplies its window timer.

## Stateful work and spill

An allocation may cover a bounded state partition and its output. Chunk extensions use the same allocation credit. Calling `finish` prevents further materialization or extensions, so a later reload or new state partition needs an unfinished allocation or a new grant.

Do not free DRAM at spill submission. After the database has made the partition recoverable on SSD, call `spillComplete(grant, spill_id, bytes)`. Each `spill_id` is unique within the allocation. The ledger subtracts only occupied bytes, preserves the allocation's ownership and credit, and remembers the spill ID for replay. Failed writes produce no completion. A partial write must report only the recoverable, reclaimed portion. Before reloading, reserve and charge resident memory again. The example synchronizes a small partition with `fsync`, releases it, reserves reload space, and verifies its result after reading it back.

The database must account for state, output, decode scratch, and transport buffers that it manages. It also supplies a safe yield point and a bounded release-enabling step. A failed reservation authorizes neither uncharged allocation nor migration of active state.

## Placement publication and release

Build a bounded cohort of unstarted `PlacementCandidate`s, with both implementations' cost and CPU/memory/network demand vectors. Pullup demand includes data transfer. `placeAndCommit` orders candidates by expected benefit and commits the cohort only against the same budget version; a stale version changes no live reservations. A cohort without feasible choices is rejected before commit.

Every decision carries its fragment and generation. Release with `ledger.release(fragment, generation)` once its resource reservation is no longer needed. Do not release an unlabelled demand vector: it cannot distinguish a retransmitted completion from a different owner. A later reservation for the same fragment requires a larger generation. The ledger rejects duplicate fragment IDs in a cohort, invalid floating-point values, and attempts to commit an already-live owner.

The placement ledger bounds predicted cohort demand; the transport manager enforces actual memory and execution slots. The database manages the relationship between placement reservations and actual allocations, and supplies link/CPU telemetry and network backpressure. Summing independent tenant limits is insufficient: route all DPU allocations through the shared gate.

## Account recovery

`TenantAccount::checkpoint()` exports policy, CPU/network service totals, next allocation ID, and allocation records, including their backend and completed spill IDs. The embedding database owns serialization and durable publication of this image. Persist the backend and spill-ID list as well as byte counters; the small finished-output fixture uses only Pushdown and has no spill records.

`ResourceLedger::restoreTenant()` validates the image before admission. A finished record has zero reserved bytes; occupied bytes remain charged. Images from an older implementation that retained unused reservation at completion require an explicit format conversion. Duplicate allocations, invalid counters, and invalid spill IDs are rejected.

An account snapshot cannot reconstruct backend execution slots or query state. `TransportManager` therefore fences startup when restored unfinished allocations exist. The embedding database must first reconcile backend termination or finish its query recovery; only known-finished allocations can use the final-release replay path. The controlled process-restart test verifies this boundary, monotonic IDs, and that stale or duplicate releases do not return memory twice.

## TapDB hook mapping

| TapDB event | Tide action |
|---|---|
| Physical operator/partition created | Assign tenant and globally unique fragment ID; expose both legal implementations. |
| CAF actor dependencies satisfied | Enqueue its next bounded batch. |
| Actor dispatch or send path | Select, acquire a grant, attach the token identity to the message. |
| `TupleSet` allocation and publication | Reserve/extend; materialize bytes before publishing. |
| Actor handler completes at safe yield | Finish the batch with measured service. |
| Last `TupleSet` consumer releases its buffer | Invoke `onFinalConsumer`; retain this identity after the producer finishes. |
| HBTable partition SSD write becomes durable | Report that partition's unique spill completion. |
| HBTable partition reload | Reserve and materialize before consuming resident capacity. |

Scans and filters yield at bounded `TupleSet` boundaries. Join build and partial aggregation preserve their active state until a safe partition boundary; old state stays on its chosen backend. A future unstarted partition or downstream operator may choose Pullup. These hooks are the TapDB adaptation work; the standalone example does not supply CAF or HBTable operator implementations.
