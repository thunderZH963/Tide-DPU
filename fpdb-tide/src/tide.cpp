#include "tide/tide.hpp"

#include <algorithm>
#include <cmath>
#include <stdexcept>

namespace tide {

TenantAccount::TenantAccount(TenantPolicy policy) : policy_(policy) {
  if (policy_.weight <= 0 || policy_.batch_limit == 0 || policy_.memory_limit_bytes == 0)
    throw std::invalid_argument("invalid tenant policy");
}

std::optional<AllocationId> TenantAccount::reserve(std::uint64_t bytes, Generation generation) {
  std::lock_guard<std::mutex> guard(mutex_);
  if (credits_in_use_ >= policy_.batch_limit) return std::nullopt;
  if (occupied_bytes_ > policy_.memory_limit_bytes ||
      reserved_bytes_ > policy_.memory_limit_bytes - occupied_bytes_ ||
      bytes > policy_.memory_limit_bytes - occupied_bytes_ - reserved_bytes_)
    return std::nullopt;
  const auto id = next_allocation_++;
  allocations_.emplace(id, Allocation{generation, bytes, 0, false});
  reserved_bytes_ += bytes;
  ++credits_in_use_;
  return id;
}

bool TenantAccount::materialize(AllocationId id, Generation generation, std::uint64_t bytes) {
  std::lock_guard<std::mutex> guard(mutex_);
  auto it = allocations_.find(id);
  if (it == allocations_.end() || it->second.generation != generation || it->second.finished ||
      bytes > it->second.reserved)
    return false;
  it->second.reserved -= bytes;
  it->second.occupied += bytes;
  reserved_bytes_ -= bytes;
  occupied_bytes_ += bytes;
  return true;
}

bool TenantAccount::finish(AllocationId id, Generation generation,
                           double cpu_ms, std::uint64_t network_bytes) {
  std::lock_guard<std::mutex> guard(mutex_);
  auto it = allocations_.find(id);
  if (it == allocations_.end() || it->second.generation != generation || it->second.finished)
    return false;
  it->second.finished = true;
  cpu_service_ms_ += std::max(0.0, cpu_ms);
  network_bytes_ += network_bytes;
  return true;
}

bool TenantAccount::release(AllocationId id, Generation generation) {
  std::lock_guard<std::mutex> guard(mutex_);
  auto it = allocations_.find(id);
  if (it == allocations_.end() || it->second.generation != generation || !it->second.finished)
    return false;
  reserved_bytes_ -= it->second.reserved;
  occupied_bytes_ -= it->second.occupied;
  if (credits_in_use_ > 0) --credits_in_use_;
  allocations_.erase(it);
  return true;
}

TenantSnapshot TenantAccount::snapshot() const {
  std::lock_guard<std::mutex> guard(mutex_);
  return TenantSnapshot{policy_, cpu_service_ms_, occupied_bytes_, reserved_bytes_,
                        network_bytes_, credits_in_use_};
}

void ResourceLedger::registerTenant(const TenantId &tenant, const TenantPolicy &policy) {
  std::lock_guard<std::mutex> guard(mutex_);
  accounts_.try_emplace(tenant, std::make_shared<TenantAccount>(policy));
}

std::shared_ptr<TenantAccount> ResourceLedger::account(const TenantId &tenant) const {
  std::lock_guard<std::mutex> guard(mutex_);
  auto it = accounts_.find(tenant);
  return it == accounts_.end() ? nullptr : it->second;
}

std::unordered_map<TenantId, TenantSnapshot> ResourceLedger::snapshot() const {
  std::vector<std::pair<TenantId, std::shared_ptr<TenantAccount>>> copy;
  {
    std::lock_guard<std::mutex> guard(mutex_);
    copy.reserve(accounts_.size());
    for (const auto &entry : accounts_) copy.push_back(entry);
  }
  std::unordered_map<TenantId, TenantSnapshot> result;
  for (const auto &entry : copy) result.emplace(entry.first, entry.second->snapshot());
  return result;
}

HierarchicalScheduler::HierarchicalScheduler(std::shared_ptr<ResourceLedger> ledger)
    : ledger_(std::move(ledger)) {}

void HierarchicalScheduler::enqueue(ReadyBatch batch) {
  std::lock_guard<std::mutex> guard(mutex_);
  auto [it, inserted] = queues_.try_emplace(batch.tenant);
  if (inserted) order_.push_back(batch.tenant);
  it->second.ready.push_back(std::move(batch));
}

std::optional<ReadyBatch> HierarchicalScheduler::next() {
  std::lock_guard<std::mutex> guard(mutex_);
  if (order_.empty()) return std::nullopt;
  for (std::size_t attempt = 0; attempt < order_.size() * 3; ++attempt) {
    const auto &tenant = order_[cursor_++ % order_.size()];
    auto &state = queues_.at(tenant);
    if (state.ready.empty()) continue;
    auto account = ledger_->account(tenant);
    if (!account) continue;
    state.deficit += account->snapshot().policy.weight;
    const auto cost = std::max(0.001, state.ready.front().estimated_service_ms);
    if (state.deficit < cost) continue;
    state.deficit -= cost;
    auto result = state.ready.front();
    state.ready.pop_front();
    return result;
  }
  // Normalize large service estimates while preserving tenant weights.
  for (auto &entry : queues_) {
    if (auto account = ledger_->account(entry.first))
      entry.second.deficit += account->snapshot().policy.weight * 16.0;
  }
  return std::nullopt;
}

std::size_t HierarchicalScheduler::size() const {
  std::lock_guard<std::mutex> guard(mutex_);
  std::size_t total = 0;
  for (const auto &entry : queues_) total += entry.second.ready.size();
  return total;
}

PlacementBudgetLedger::PlacementBudgetLedger(SharedBudget capacity)
    : capacity_(capacity), available_(capacity) {
  if (capacity.dpu_cpu_ms < 0) throw std::invalid_argument("negative CPU budget");
}

BudgetSnapshot PlacementBudgetLedger::snapshot() const {
  std::lock_guard<std::mutex> guard(mutex_);
  return {available_, version_};
}

bool PlacementBudgetLedger::tryCommit(
    std::uint64_t expected_version, const std::vector<PlacementDecision> &decisions) {
  ResourceDemand total;
  for (const auto &decision : decisions) {
    if (decision.reservation.dpu_cpu_ms < 0) return false;
    total.dpu_cpu_ms += decision.reservation.dpu_cpu_ms;
    if (decision.reservation.dpu_memory_bytes >
            std::numeric_limits<std::uint64_t>::max() - total.dpu_memory_bytes ||
        decision.reservation.network_bytes >
            std::numeric_limits<std::uint64_t>::max() - total.network_bytes)
      return false;
    total.dpu_memory_bytes += decision.reservation.dpu_memory_bytes;
    total.network_bytes += decision.reservation.network_bytes;
  }
  std::lock_guard<std::mutex> guard(mutex_);
  if (version_ != expected_version || total.dpu_cpu_ms > available_.dpu_cpu_ms ||
      total.dpu_memory_bytes > available_.dpu_memory_bytes ||
      total.network_bytes > available_.network_bytes)
    return false;
  available_.dpu_cpu_ms -= total.dpu_cpu_ms;
  available_.dpu_memory_bytes -= total.dpu_memory_bytes;
  available_.network_bytes -= total.network_bytes;
  ++version_;
  return true;
}

void PlacementBudgetLedger::release(const ResourceDemand &reservation) {
  std::lock_guard<std::mutex> guard(mutex_);
  available_.dpu_cpu_ms = std::min(capacity_.dpu_cpu_ms,
                                   available_.dpu_cpu_ms + std::max(0.0, reservation.dpu_cpu_ms));
  available_.dpu_memory_bytes = std::min(
      capacity_.dpu_memory_bytes, available_.dpu_memory_bytes + reservation.dpu_memory_bytes);
  available_.network_bytes = std::min(
      capacity_.network_bytes, available_.network_bytes + reservation.network_bytes);
  ++version_;
}

std::vector<PlacementDecision> PlacementController::place(
    std::vector<PlacementCandidate> candidates, const SharedBudget &available) const {
  std::sort(candidates.begin(), candidates.end(), [](const auto &a, const auto &b) {
    return (a.pullup_cost_ms - a.pushdown_cost_ms) > (b.pullup_cost_ms - b.pushdown_cost_ms);
  });
  auto remaining = available;
  std::vector<PlacementDecision> decisions;
  decisions.reserve(candidates.size());
  const auto fits = [](const ResourceDemand &demand, const SharedBudget &budget) {
    return demand.dpu_cpu_ms <= budget.dpu_cpu_ms &&
           demand.dpu_memory_bytes <= budget.dpu_memory_bytes &&
           demand.network_bytes <= budget.network_bytes;
  };
  const auto reserve = [](SharedBudget &budget, const ResourceDemand &demand) {
    budget.dpu_cpu_ms -= demand.dpu_cpu_ms;
    budget.dpu_memory_bytes -= demand.dpu_memory_bytes;
    budget.network_bytes -= demand.network_bytes;
  };
  for (const auto &candidate : candidates) {
    const bool prefer_pushdown = candidate.pushdown_cost_ms <= candidate.pullup_cost_ms;
    const bool pushdown_fits = candidate.pushdown_legal && fits(candidate.pushdown, remaining);
    const bool pullup_fits = candidate.pullup_legal && fits(candidate.pullup, remaining);
    if ((prefer_pushdown && pushdown_fits) || (!pullup_fits && pushdown_fits)) {
      decisions.push_back({candidate.fragment, Placement::Pushdown, candidate.pushdown});
      reserve(remaining, candidate.pushdown);
    } else if (pullup_fits) {
      decisions.push_back({candidate.fragment, Placement::Pullup, candidate.pullup});
      reserve(remaining, candidate.pullup);
    } else {
      throw std::runtime_error("fragment has no feasible placement");
    }
  }
  return decisions;
}

std::optional<std::vector<PlacementDecision>> PlacementController::placeAndCommit(
    const std::vector<PlacementCandidate> &candidates,
    PlacementBudgetLedger &ledger,
    std::uint32_t retry_limit) const {
  for (std::uint32_t attempt = 0; attempt < retry_limit; ++attempt) {
    const auto view = ledger.snapshot();
    auto decisions = place(candidates, view.available);
    if (ledger.tryCommit(view.version, decisions)) return decisions;
  }
  return std::nullopt;
}

TransportManager::TransportManager(std::shared_ptr<ResourceLedger> ledger,
                                   std::shared_ptr<HierarchicalScheduler> scheduler,
                                   std::uint32_t pushdown_slots,
                                   std::uint32_t pullup_slots)
    : ledger_(std::move(ledger)), scheduler_(std::move(scheduler)),
      pushdown_slot_limit_(pushdown_slots), pullup_slot_limit_(pullup_slots) {
  if (!ledger_ || !scheduler_ || pushdown_slots == 0 || pullup_slots == 0)
    throw std::invalid_argument("invalid transport manager configuration");
}

void TransportManager::open(const FragmentSpec &fragment, Placement placement) {
  std::lock_guard<std::mutex> guard(mutex_);
  if ((placement == Placement::Pushdown && !fragment.pushdown_legal) ||
      (placement == Placement::Pullup && !fragment.pullup_legal))
    throw std::invalid_argument("illegal fragment placement");
  auto existing = fragments_.find(fragment.fragment);
  if (existing != fragments_.end() && existing->second.spec.generation >= fragment.generation)
    throw std::invalid_argument("fragment generation must increase");
  fragments_[fragment.fragment] = FragmentState{fragment, placement, true};
}

AcquireResult TransportManager::acquire(const BatchRequest &request) {
  Placement placement;
  {
    std::lock_guard<std::mutex> guard(mutex_);
    auto it = fragments_.find(request.fragment);
    if (it == fragments_.end()) return {AcquireStatus::UnknownFragment, std::nullopt};
    if (!it->second.open) return {AcquireStatus::Closed, std::nullopt};
    if (it->second.spec.generation != request.generation || it->second.spec.tenant != request.tenant)
      return {AcquireStatus::StaleGeneration, std::nullopt};
    placement = it->second.placement;
    auto &used = placement == Placement::Pushdown ? pushdown_slots_in_use_ : pullup_slots_in_use_;
    const auto limit = placement == Placement::Pushdown ? pushdown_slot_limit_ : pullup_slot_limit_;
    if (used >= limit) return {AcquireStatus::WouldBlock, std::nullopt};
    ++used;
  }
  auto account = ledger_->account(request.tenant);
  if (!account) {
    std::lock_guard<std::mutex> guard(mutex_);
    auto &used = placement == Placement::Pushdown ? pushdown_slots_in_use_ : pullup_slots_in_use_;
    --used;
    return {AcquireStatus::UnknownFragment, std::nullopt};
  }
  auto id = account->reserve(request.memory_bytes, request.generation);
  if (!id) {
    std::lock_guard<std::mutex> guard(mutex_);
    auto &used = placement == Placement::Pushdown ? pushdown_slots_in_use_ : pullup_slots_in_use_;
    --used;
    return {AcquireStatus::WouldBlock, std::nullopt};
  }
  return {AcquireStatus::Granted,
          Grant{request.tenant, *id, request.generation, placement, request.memory_bytes}};
}

bool TransportManager::push(const Grant &grant, std::uint64_t bytes) {
  auto account = ledger_->account(grant.tenant);
  return account && account->materialize(grant.allocation, grant.generation, bytes);
}

bool TransportManager::finish(const TenantId &tenant, const Grant &grant,
                              double cpu_ms, std::uint64_t network_bytes) {
  if (tenant != grant.tenant) return false;
  auto account = ledger_->account(tenant);
  if (!account || !account->finish(grant.allocation, grant.generation, cpu_ms, network_bytes))
    return false;
  std::lock_guard<std::mutex> guard(mutex_);
  auto &used = grant.placement == Placement::Pushdown ? pushdown_slots_in_use_ : pullup_slots_in_use_;
  if (used > 0) --used;
  return true;
}

bool TransportManager::release(const TenantId &tenant, AllocationId allocation,
                               Generation generation) {
  auto account = ledger_->account(tenant);
  return account && account->release(allocation, generation);
}

bool TransportManager::close(FragmentId fragment, Generation generation) {
  std::lock_guard<std::mutex> guard(mutex_);
  auto it = fragments_.find(fragment);
  if (it == fragments_.end() || it->second.spec.generation != generation) return false;
  it->second.open = false;
  return true;
}

std::uint32_t TransportManager::slotsInUse(Placement placement) const {
  std::lock_guard<std::mutex> guard(mutex_);
  return placement == Placement::Pushdown ? pushdown_slots_in_use_ : pullup_slots_in_use_;
}

} // namespace tide
