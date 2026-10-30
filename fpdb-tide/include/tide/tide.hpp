#pragma once

#include <cstdint>
#include <deque>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace tide {

using TenantId = std::string;
using FragmentId = std::uint64_t;
using BatchId = std::uint64_t;
using AllocationId = std::uint64_t;
using Generation = std::uint64_t;
using SpillId = std::uint64_t;
using ConsumerId = std::uint64_t;

struct TenantPolicy {
  double weight{1.0};
  std::uint32_t batch_limit{4};
  std::uint64_t memory_limit_bytes{2ULL << 30U};
};

struct ResourceDemand {
  double dpu_cpu_ms{0};
  std::uint64_t dpu_memory_bytes{0};
  std::uint64_t network_bytes{0};
};

struct SharedBudget {
  double dpu_cpu_ms{0};
  std::uint64_t dpu_memory_bytes{0};
  std::uint64_t network_bytes{0};
};

enum class Placement { Pushdown, Pullup };
enum class AcquireStatus {
  Granted, WouldBlock, Closed, StaleGeneration, UnknownFragment, DuplicateBatch
};

struct FragmentSpec {
  TenantId tenant;
  FragmentId fragment{0};
  Generation generation{0};
  bool pushdown_legal{true};
  bool pullup_legal{true};
  bool stateful{false};
};

struct BatchRequest {
  TenantId tenant;
  FragmentId fragment{0};
  BatchId batch{0};
  Generation generation{0};
  std::uint64_t memory_bytes{0};
};

struct Grant {
  TenantId tenant;
  AllocationId allocation{0};
  Generation generation{0};
  Placement placement{Placement::Pushdown};
  std::uint64_t reserved_bytes{0};
  FragmentId fragment{0};
  BatchId batch{0};
};

struct AcquireResult {
  AcquireStatus status{AcquireStatus::WouldBlock};
  std::optional<Grant> grant;
};

struct TenantSnapshot {
  TenantPolicy policy;
  double cpu_service_ms{0};
  std::uint64_t occupied_bytes{0};
  std::uint64_t reserved_bytes{0};
  std::uint64_t network_bytes{0};
  std::uint32_t credits_in_use{0};
};

struct AllocationCheckpoint {
  AllocationId id{0};
  Generation generation{0};
  std::uint64_t reserved{0};
  std::uint64_t occupied{0};
  bool finished{false};
  // Durable-spill completions are replay-safe across an account restore.
  std::vector<std::pair<SpillId, std::uint64_t>> spills;
  Placement placement{Placement::Pushdown};
  bool tracked_output{false};
  bool sealed{false};
  // Released IDs are retained as tombstones until allocation collection.
  std::vector<std::pair<ConsumerId, bool>> consumers;
};

struct TenantCheckpoint {
  TenantPolicy policy;
  double cpu_service_ms{0};
  std::uint64_t network_bytes{0};
  AllocationId next_allocation{1};
  std::vector<AllocationCheckpoint> allocations;
};

class TenantAccount {
public:
  explicit TenantAccount(TenantPolicy policy);
  explicit TenantAccount(const TenantCheckpoint &checkpoint);

  std::optional<AllocationId> reserve(std::uint64_t bytes, Generation generation,
                                     Placement placement = Placement::Pushdown);
  // Extend this batch's reservation without consuming another allocation credit.
  bool reserveMore(AllocationId allocation, Generation generation, std::uint64_t bytes);
  bool materialize(AllocationId allocation, Generation generation, std::uint64_t bytes);
  // Call only after the SSD write is durable; duplicate spill IDs change nothing.
  bool spillComplete(AllocationId allocation, Generation generation,
                     SpillId spill, std::uint64_t bytes);
  bool finish(AllocationId allocation, Generation generation,
              double cpu_ms, std::uint64_t network_bytes);
  bool release(AllocationId allocation, Generation generation);
  bool registerConsumer(AllocationId, Generation, ConsumerId);
  bool sealOutput(AllocationId, Generation);
  bool releaseConsumer(AllocationId, Generation, ConsumerId);
  bool outputReady(AllocationId, Generation) const;
  TenantSnapshot snapshot() const;
  // Stable account image; the embedding database owns durable storage and replay.
  TenantCheckpoint checkpoint() const;

private:
  struct Allocation {
    Generation generation{0};
    std::uint64_t reserved{0};
    std::uint64_t occupied{0};
    bool finished{false};
    std::unordered_map<SpillId, std::uint64_t> spills;
    Placement placement{Placement::Pushdown};
    bool tracked_output{false};
    bool sealed{false};
    std::unordered_map<ConsumerId, bool> consumers;
  };

  mutable std::mutex mutex_;
  TenantPolicy policy_;
  double cpu_service_ms_{0};
  std::uint64_t occupied_bytes_{0};
  std::uint64_t reserved_bytes_{0};
  std::uint64_t network_bytes_{0};
  std::uint32_t credits_in_use_{0};
  AllocationId next_allocation_{1};
  std::unordered_map<AllocationId, Allocation> allocations_;
};

class ResourceLedger {
public:
  void registerTenant(const TenantId &tenant, const TenantPolicy &policy);
  // Restore into a fresh ledger; reject duplicate tenant IDs and invalid counters.
  void restoreTenant(const TenantId &tenant, const TenantCheckpoint &checkpoint);
  std::shared_ptr<TenantAccount> account(const TenantId &tenant) const;
  std::unordered_map<TenantId, TenantSnapshot> snapshot() const;

private:
  mutable std::mutex mutex_;
  std::unordered_map<TenantId, std::shared_ptr<TenantAccount>> accounts_;
};

struct SchedulerPolicy {
  double service_quantum_ms{10.0}; // Tenant quantum is this value times its weight.
  // Estimate-bounded exception grants; actual overshoot blocks future grants.
  double progress_allowance_ms{1.0};
  bool enforce_debt{false}; // Enable explicitly when the caller advances windows.
};

struct ReadyBatch {
  TenantId tenant;
  FragmentId fragment{0};
  BatchId batch{0};
  double estimated_service_ms{1.0};
  Generation generation{0};
  std::uint64_t selection_attempt{0}; // Assigned by next(), zero before selection.
  bool progress{false}; // Caller certifies this work can enable resource release.
};

class HierarchicalScheduler {
public:
  explicit HierarchicalScheduler(std::shared_ptr<ResourceLedger> ledger,
                                 SchedulerPolicy policy = {});
  // A (tenant, fragment, batch, generation) identifies work for this scheduler's
  // lifetime. Re-enqueueing queued, selected, or completed work throws.
  void enqueue(ReadyBatch batch);
  // Charge estimated service / tenant weight; round-robin fragments per tenant.
  std::optional<ReadyBatch> next();
  // Reconcile selected work once using actual service. Unknown, stale, queued,
  // and duplicate completions return false; invalid service throws. A nonzero
  // selection_attempt must match; zero permits canonical transport callbacks.
  bool complete(const ReadyBatch &batch, double actual_service_ms);
  // Undo a selection that could not acquire capacity, then requeue its work.
  // Requires the exact next() result; duplicate/stale attempts return false.
  // Retry after a capacity event rather than repeatedly polling blocked work.
  bool defer(const ReadyBatch &batch);
  // Unused weighted service repays debt. The embedding runtime owns the timer;
  // service is charged to the window in which complete() observes it.
  void advanceWindow();
  double serviceDebt(const TenantId &tenant) const;
  // Number of ready batches, excluding selected or completed work.
  std::size_t size() const;

private:
  struct BatchKey {
    TenantId tenant;
    FragmentId fragment;
    BatchId batch;
    Generation generation;
    bool operator==(const BatchKey &other) const;
  };
  struct BatchKeyHash {
    std::size_t operator()(const BatchKey &key) const;
  };
  enum class WorkStatus { Queued, Selected, Completed };
  struct WorkRecord {
    double estimate;
    double weight;
    WorkStatus status{WorkStatus::Queued};
    std::uint64_t selection_attempt{0};
    bool progress{false};
    bool used_progress_allowance{false};
    std::uint64_t progress_window{0};
  };
  struct QueueState {
    std::unordered_map<FragmentId, std::deque<ReadyBatch>> fragments;
    std::vector<FragmentId> fragment_order;
    std::size_t cursor{0};
    std::size_t ready_count{0};
    std::size_t in_flight{0};
    long double virtual_service{0};
    double weight{1.0};
    long double service_debt{0};
    long double actual_window_service{0};
    long double progress_used{0};
  };
  std::shared_ptr<ResourceLedger> ledger_;
  SchedulerPolicy policy_;
  mutable std::mutex mutex_;
  std::unordered_map<TenantId, QueueState> queues_;
  // Completed identities remain tombstones: recycled IDs require a new
  // generation, so delayed completions cannot change another batch's service.
  std::unordered_map<BatchKey, WorkRecord, BatchKeyHash> work_;
  long double virtual_time_{0};
  std::size_t ready_count_{0};
  std::uint64_t window_{0};
};

struct PlacementCandidate {
  FragmentId fragment{0};
  TenantId tenant;
  ResourceDemand pushdown;
  ResourceDemand pullup;
  double pushdown_cost_ms{0};
  double pullup_cost_ms{0};
  bool pushdown_legal{true};
  bool pullup_legal{true};
  Generation generation{0};
};

struct PlacementDecision {
  FragmentId fragment{0};
  Placement placement{Placement::Pullup};
  ResourceDemand reservation;
  Generation generation{0};
};

struct BudgetSnapshot {
  SharedBudget available;
  std::uint64_t version{0};
};

class PlacementBudgetLedger {
public:
  explicit PlacementBudgetLedger(SharedBudget capacity);
  BudgetSnapshot snapshot() const;
  bool tryCommit(std::uint64_t expected_version,
                 const std::vector<PlacementDecision> &decisions);
  // A release names the committed owner; repeated or stale events change nothing.
  bool release(FragmentId fragment, Generation generation);

private:
  mutable std::mutex mutex_;
  SharedBudget capacity_;
  SharedBudget available_;
  std::uint64_t version_{0};
  std::unordered_map<FragmentId, PlacementDecision> reservations_;
  std::unordered_map<FragmentId, Generation> generations_;
};

class PlacementController {
public:
  std::vector<PlacementDecision> place(std::vector<PlacementCandidate> candidates,
                                       const SharedBudget &available) const;
  std::optional<std::vector<PlacementDecision>> placeAndCommit(
      const std::vector<PlacementCandidate> &candidates,
      PlacementBudgetLedger &ledger,
      std::uint32_t retry_limit = 3) const;
};

class TransportManager {
public:
  TransportManager(std::shared_ptr<ResourceLedger> ledger,
                   std::shared_ptr<HierarchicalScheduler> scheduler,
                   std::uint32_t pushdown_slots = 1,
                   std::uint32_t pullup_slots = 1,
                   std::uint64_t pushdown_memory_limit_bytes =
                       std::numeric_limits<std::uint64_t>::max());

  void open(const FragmentSpec &fragment, Placement placement);
  AcquireResult acquire(const BatchRequest &request);
  bool push(const Grant &grant, std::uint64_t materialized_bytes);
  // Register before exposing output. sealOutput forbids new readers/bytes.
  bool registerConsumer(const Grant &, ConsumerId);
  bool sealOutput(const Grant &);
  bool sealOutput(const TenantId &, AllocationId, Generation);
  // Accepts replayed events for checkpoint-restored allocations.
  bool releaseConsumer(const TenantId &, AllocationId, Generation, ConsumerId);
  bool extend(const Grant &grant, std::uint64_t additional_bytes);
  bool spillComplete(const Grant &grant, SpillId spill, std::uint64_t durable_bytes);
  bool finish(const TenantId &tenant, const Grant &grant,
              double cpu_ms, std::uint64_t network_bytes);
  bool release(const TenantId &tenant, AllocationId allocation, Generation generation);
  // Remember a legitimate last-consumer event that races ahead of finish.
  // The allocation stays charged until both events have arrived.
  bool onFinalConsumer(const TenantId &tenant, AllocationId allocation, Generation generation);
  bool close(FragmentId fragment, Generation generation);
  std::uint32_t slotsInUse(Placement placement) const;
  std::uint64_t pushdownMemoryInUse() const;

private:
  struct FragmentState {
    FragmentSpec spec;
    Placement placement{Placement::Pushdown};
    bool open{true};
    std::unordered_map<BatchId, AllocationId> batches;
  };

  struct GrantState {
    Grant grant;
    bool finished{false};
    bool release_pending{false};
    std::uint64_t reserved{0};
    std::uint64_t occupied{0};
  };
  GrantState *findGrant(const Grant &grant);
  bool releaseLocked(const TenantId &tenant, AllocationId allocation, Generation generation);
  void collectOutputLocked(const TenantId &, AllocationId, Generation);

  std::shared_ptr<ResourceLedger> ledger_;
  std::shared_ptr<HierarchicalScheduler> scheduler_;
  mutable std::mutex mutex_;
  std::unordered_map<FragmentId, FragmentState> fragments_;
  std::unordered_map<TenantId, std::unordered_map<AllocationId, GrantState>> grants_;
  std::unordered_map<TenantId, std::unordered_map<AllocationId, AllocationCheckpoint>> restored_;
  std::uint32_t pushdown_slot_limit_{1};
  std::uint32_t pullup_slot_limit_{1};
  std::uint32_t pushdown_slots_in_use_{0};
  std::uint32_t pullup_slots_in_use_{0};
  std::uint64_t pushdown_memory_limit_bytes_;
  std::uint64_t pushdown_memory_in_use_{0};
};

// Adapter callbacks for a database engine. Native operator and tuple types stay
// outside the Tide core; the engine maps them to fragment/allocation IDs.
struct DatabaseHooks {
  std::function<void(FragmentId, BatchId, Placement)> dispatch;
  std::function<void(FragmentId)> pause_at_yield;
  std::function<void(FragmentId, AllocationId)> spill_state_partition;
  std::function<void(FragmentId, AllocationId)> publish_output;
  std::function<void(AllocationId, Generation)> on_final_consumer;
};

// TapDB's existing adapter may retain its HBTable-specific callback name.
struct TapDBHooks {
  std::function<void(FragmentId, BatchId, Placement)> dispatch;
  std::function<void(FragmentId)> pause_at_yield;
  std::function<void(FragmentId, AllocationId)> spill_hbtable_partition;
};

} // namespace tide
