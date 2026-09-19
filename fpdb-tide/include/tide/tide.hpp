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
enum class AcquireStatus { Granted, WouldBlock, Closed, StaleGeneration, UnknownFragment };

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

class TenantAccount {
public:
  explicit TenantAccount(TenantPolicy policy);

  std::optional<AllocationId> reserve(std::uint64_t bytes, Generation generation);
  bool materialize(AllocationId allocation, Generation generation, std::uint64_t bytes);
  bool finish(AllocationId allocation, Generation generation,
              double cpu_ms, std::uint64_t network_bytes);
  bool release(AllocationId allocation, Generation generation);
  TenantSnapshot snapshot() const;

private:
  struct Allocation {
    Generation generation{0};
    std::uint64_t reserved{0};
    std::uint64_t occupied{0};
    bool finished{false};
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
  std::shared_ptr<TenantAccount> account(const TenantId &tenant) const;
  std::unordered_map<TenantId, TenantSnapshot> snapshot() const;

private:
  mutable std::mutex mutex_;
  std::unordered_map<TenantId, std::shared_ptr<TenantAccount>> accounts_;
};

struct ReadyBatch {
  TenantId tenant;
  FragmentId fragment{0};
  BatchId batch{0};
  double estimated_service_ms{1.0};
};

class HierarchicalScheduler {
public:
  explicit HierarchicalScheduler(std::shared_ptr<ResourceLedger> ledger);
  void enqueue(ReadyBatch batch);
  std::optional<ReadyBatch> next();
  std::size_t size() const;

private:
  struct QueueState {
    std::deque<ReadyBatch> ready;
    double deficit{0};
  };
  std::shared_ptr<ResourceLedger> ledger_;
  mutable std::mutex mutex_;
  std::unordered_map<TenantId, QueueState> queues_;
  std::vector<TenantId> order_;
  std::size_t cursor_{0};
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
};

struct PlacementDecision {
  FragmentId fragment{0};
  Placement placement{Placement::Pullup};
  ResourceDemand reservation;
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
  void release(const ResourceDemand &reservation);

private:
  mutable std::mutex mutex_;
  SharedBudget capacity_;
  SharedBudget available_;
  std::uint64_t version_{0};
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
                   std::uint32_t pullup_slots = 1);

  void open(const FragmentSpec &fragment, Placement placement);
  AcquireResult acquire(const BatchRequest &request);
  bool push(const Grant &grant, std::uint64_t materialized_bytes);
  bool finish(const TenantId &tenant, const Grant &grant,
              double cpu_ms, std::uint64_t network_bytes);
  bool release(const TenantId &tenant, AllocationId allocation, Generation generation);
  bool close(FragmentId fragment, Generation generation);
  std::uint32_t slotsInUse(Placement placement) const;

private:
  struct FragmentState {
    FragmentSpec spec;
    Placement placement{Placement::Pushdown};
    bool open{true};
  };

  std::shared_ptr<ResourceLedger> ledger_;
  std::shared_ptr<HierarchicalScheduler> scheduler_;
  mutable std::mutex mutex_;
  std::unordered_map<FragmentId, FragmentState> fragments_;
  std::uint32_t pushdown_slot_limit_{1};
  std::uint32_t pullup_slot_limit_{1};
  std::uint32_t pushdown_slots_in_use_{0};
  std::uint32_t pullup_slots_in_use_{0};
};

// Thin hooks used by a TapDB adapter. TupleSet and CAF message types stay outside
// the Tide core so this module can be linked without Arrow or CAF.
struct TapDBHooks {
  std::function<void(FragmentId, BatchId, Placement)> dispatch;
  std::function<void(FragmentId)> pause_at_yield;
  std::function<void(FragmentId, AllocationId)> spill_hbtable_partition;
};

} // namespace tide
