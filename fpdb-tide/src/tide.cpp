#include "tide/tide.hpp"

#include <algorithm>
#include <cmath>
#include <stdexcept>

namespace tide {

namespace {
bool validDemand(const ResourceDemand &demand) {
  return std::isfinite(demand.dpu_cpu_ms) && demand.dpu_cpu_ms >= 0;
}
bool validBudget(const SharedBudget &budget) {
  return std::isfinite(budget.dpu_cpu_ms) && budget.dpu_cpu_ms >= 0;
}
bool validPlacement(Placement placement) {
  return placement == Placement::Pushdown || placement == Placement::Pullup;
}
} // namespace

TenantAccount::TenantAccount(TenantPolicy policy) : policy_(policy) {
  if (!std::isfinite(policy_.weight) || policy_.weight <= 0 ||
      policy_.batch_limit == 0 || policy_.memory_limit_bytes == 0)
    throw std::invalid_argument("invalid tenant policy");
}

TenantAccount::TenantAccount(const TenantCheckpoint &checkpoint)
    : TenantAccount(checkpoint.policy) {
  if (checkpoint.next_allocation == 0 ||
      !std::isfinite(checkpoint.cpu_service_ms) || checkpoint.cpu_service_ms < 0)
    throw std::invalid_argument("invalid tenant checkpoint");
  cpu_service_ms_ = checkpoint.cpu_service_ms;
  network_bytes_ = checkpoint.network_bytes;
  next_allocation_ = checkpoint.next_allocation;
  for (const auto &entry : checkpoint.allocations) {
    if (entry.id == 0 || entry.id >= next_allocation_ || !validPlacement(entry.placement) ||
        (entry.finished && entry.reserved != 0) ||
        !allocations_.emplace(entry.id, Allocation{entry.generation, entry.reserved,
                                                 entry.occupied, entry.finished, {},
                                                 entry.placement}).second)
      throw std::invalid_argument("invalid allocation checkpoint");
    auto &spills = allocations_.at(entry.id).spills;
    for (const auto &spill : entry.spills) {
      if (spill.first == 0 || spill.second == 0 || !spills.emplace(spill).second)
        throw std::invalid_argument("invalid spill checkpoint");
    }
    if (entry.reserved > policy_.memory_limit_bytes - reserved_bytes_ ||
        entry.occupied > policy_.memory_limit_bytes - occupied_bytes_)
      throw std::invalid_argument("checkpoint exceeds tenant limit");
    reserved_bytes_ += entry.reserved;
    occupied_bytes_ += entry.occupied;
    ++credits_in_use_;
  }
  if (credits_in_use_ > policy_.batch_limit ||
      reserved_bytes_ > policy_.memory_limit_bytes - occupied_bytes_)
    throw std::invalid_argument("checkpoint exceeds tenant limit");
}

std::optional<AllocationId> TenantAccount::reserve(std::uint64_t bytes, Generation generation,
                                                  Placement placement) {
  std::lock_guard<std::mutex> guard(mutex_);
  if (!validPlacement(placement)) throw std::invalid_argument("invalid allocation placement");
  if (credits_in_use_ >= policy_.batch_limit) return std::nullopt;
  if (next_allocation_ == std::numeric_limits<AllocationId>::max())
    return std::nullopt;  // Preserve monotonic IDs; never wrap into a live identity.
  if (occupied_bytes_ > policy_.memory_limit_bytes ||
      reserved_bytes_ > policy_.memory_limit_bytes - occupied_bytes_ ||
      bytes > policy_.memory_limit_bytes - occupied_bytes_ - reserved_bytes_)
    return std::nullopt;
  const auto id = next_allocation_++;
  allocations_.emplace(id, Allocation{generation, bytes, 0, false, {}, placement});
  reserved_bytes_ += bytes;
  ++credits_in_use_;
  return id;
}

bool TenantAccount::reserveMore(AllocationId id, Generation generation, std::uint64_t bytes) {
  std::lock_guard<std::mutex> guard(mutex_);
  auto it = allocations_.find(id);
  if (it == allocations_.end() || it->second.generation != generation || it->second.finished ||
      occupied_bytes_ > policy_.memory_limit_bytes ||
      reserved_bytes_ > policy_.memory_limit_bytes - occupied_bytes_ ||
      bytes > policy_.memory_limit_bytes - occupied_bytes_ - reserved_bytes_)
    return false;
  it->second.reserved += bytes;
  reserved_bytes_ += bytes;
  return true;
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

bool TenantAccount::spillComplete(AllocationId id, Generation generation,
                                 SpillId spill, std::uint64_t bytes) {
  std::lock_guard<std::mutex> guard(mutex_);
  auto it = allocations_.find(id);
  if (it == allocations_.end() || it->second.generation != generation ||
      spill == 0 || bytes == 0 || bytes > it->second.occupied ||
      it->second.spills.count(spill) != 0)
    return false;
  it->second.spills.emplace(spill, bytes);
  it->second.occupied -= bytes;
  occupied_bytes_ -= bytes;
  return true;
}

bool TenantAccount::finish(AllocationId id, Generation generation,
                           double cpu_ms, std::uint64_t network_bytes) {
  std::lock_guard<std::mutex> guard(mutex_);
  auto it = allocations_.find(id);
  if (it == allocations_.end() || it->second.generation != generation || it->second.finished ||
      !std::isfinite(cpu_ms) || cpu_ms < 0 ||
      !std::isfinite(cpu_service_ms_ + cpu_ms) ||
      network_bytes > std::numeric_limits<std::uint64_t>::max() - network_bytes_)
    return false;
  it->second.finished = true;
  // No more output may materialize after completion. Only occupied bytes and
  // the allocation credit remain live until final-consumer release.
  reserved_bytes_ -= it->second.reserved;
  it->second.reserved = 0;
  cpu_service_ms_ += cpu_ms;
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

TenantCheckpoint TenantAccount::checkpoint() const {
  std::lock_guard<std::mutex> guard(mutex_);
  TenantCheckpoint copy{policy_, cpu_service_ms_, network_bytes_, next_allocation_, {}};
  copy.allocations.reserve(allocations_.size());
  for (const auto &entry : allocations_) {
    copy.allocations.push_back(AllocationCheckpoint{entry.first, entry.second.generation,
                                                      entry.second.reserved,
                                                      entry.second.occupied,
                                                      entry.second.finished, {},
                                                      entry.second.placement});
    auto &spills = copy.allocations.back().spills;
    for (const auto &spill : entry.second.spills) spills.push_back(spill);
    std::sort(spills.begin(), spills.end());
  }
  std::sort(copy.allocations.begin(), copy.allocations.end(),
            [](const auto &a, const auto &b) { return a.id < b.id; });
  return copy;
}

void ResourceLedger::registerTenant(const TenantId &tenant, const TenantPolicy &policy) {
  if (tenant.empty()) throw std::invalid_argument("empty tenant ID");
  std::lock_guard<std::mutex> guard(mutex_);
  if (!accounts_.try_emplace(tenant, std::make_shared<TenantAccount>(policy)).second)
    throw std::invalid_argument("tenant already exists");
}

void ResourceLedger::restoreTenant(const TenantId &tenant,
                                   const TenantCheckpoint &checkpoint) {
  if (tenant.empty()) throw std::invalid_argument("empty tenant ID");
  auto restored = std::make_shared<TenantAccount>(checkpoint);
  std::lock_guard<std::mutex> guard(mutex_);
  if (!accounts_.emplace(tenant, std::move(restored)).second)
    throw std::invalid_argument("tenant already exists");
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

PlacementBudgetLedger::PlacementBudgetLedger(SharedBudget capacity)
    : capacity_(capacity), available_(capacity) {
  if (!validBudget(capacity)) throw std::invalid_argument("invalid CPU budget");
}

BudgetSnapshot PlacementBudgetLedger::snapshot() const {
  std::lock_guard<std::mutex> guard(mutex_);
  return {available_, version_};
}

bool PlacementBudgetLedger::tryCommit(
    std::uint64_t expected_version, const std::vector<PlacementDecision> &decisions) {
  ResourceDemand total;
  std::unordered_map<FragmentId, bool> seen;
  for (const auto &decision : decisions) {
    if (!validDemand(decision.reservation) || !validPlacement(decision.placement) ||
        !seen.emplace(decision.fragment, true).second)
      return false;
    total.dpu_cpu_ms += decision.reservation.dpu_cpu_ms;
    if (!std::isfinite(total.dpu_cpu_ms)) return false;
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
  for (const auto &decision : decisions) {
    auto prior = generations_.find(decision.fragment);
    if (reservations_.count(decision.fragment) ||
        (prior != generations_.end() && decision.generation <= prior->second))
      return false;
  }
  for (const auto &decision : decisions) {
    reservations_.emplace(decision.fragment, decision);
    generations_[decision.fragment] = decision.generation;
  }
  available_.dpu_cpu_ms -= total.dpu_cpu_ms;
  available_.dpu_memory_bytes -= total.dpu_memory_bytes;
  available_.network_bytes -= total.network_bytes;
  ++version_;
  return true;
}

bool PlacementBudgetLedger::release(FragmentId fragment, Generation generation) {
  std::lock_guard<std::mutex> guard(mutex_);
  auto it = reservations_.find(fragment);
  if (it == reservations_.end() || it->second.generation != generation) return false;
  const auto &reservation = it->second.reservation;
  available_.dpu_cpu_ms = std::min(capacity_.dpu_cpu_ms,
                                   available_.dpu_cpu_ms + std::max(0.0, reservation.dpu_cpu_ms));
  available_.dpu_memory_bytes += reservation.dpu_memory_bytes;
  available_.network_bytes += reservation.network_bytes;
  reservations_.erase(it);
  ++version_;
  return true;
}

std::vector<PlacementDecision> PlacementController::place(
    std::vector<PlacementCandidate> candidates, const SharedBudget &available) const {
  if (!validBudget(available)) throw std::invalid_argument("invalid placement budget");
  std::unordered_map<FragmentId, bool> seen;
  for (const auto &candidate : candidates) {
    if (candidate.tenant.empty() || !validDemand(candidate.pushdown) ||
        !validDemand(candidate.pullup) || !std::isfinite(candidate.pushdown_cost_ms) ||
        !std::isfinite(candidate.pullup_cost_ms) || candidate.pushdown_cost_ms < 0 ||
        candidate.pullup_cost_ms < 0 || !seen.emplace(candidate.fragment, true).second)
      throw std::invalid_argument("invalid or duplicate placement candidate");
  }
  std::sort(candidates.begin(), candidates.end(), [](const auto &a, const auto &b) {
    const auto left = a.pullup_cost_ms - a.pushdown_cost_ms;
    const auto right = b.pullup_cost_ms - b.pushdown_cost_ms;
    return left != right ? left > right : a.fragment < b.fragment;
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
      decisions.push_back({candidate.fragment, Placement::Pushdown, candidate.pushdown,
                           candidate.generation});
      reserve(remaining, candidate.pushdown);
    } else if (pullup_fits) {
      decisions.push_back({candidate.fragment, Placement::Pullup, candidate.pullup,
                           candidate.generation});
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
                                   std::uint32_t pullup_slots,
                                   std::uint64_t pushdown_memory_limit_bytes)
    : ledger_(std::move(ledger)), scheduler_(std::move(scheduler)),
      pushdown_slot_limit_(pushdown_slots), pullup_slot_limit_(pullup_slots),
      pushdown_memory_limit_bytes_(pushdown_memory_limit_bytes) {
  if (!ledger_ || !scheduler_ || pushdown_slots == 0 || pullup_slots == 0)
    throw std::invalid_argument("invalid transport manager configuration");
  for (const auto &tenant : ledger_->snapshot()) {
    const auto image = ledger_->account(tenant.first)->checkpoint();
    for (const auto &allocation : image.allocations) {
      if (!allocation.finished)
        throw std::invalid_argument("reconcile unfinished backend work before transport startup");
      if (allocation.placement == Placement::Pushdown) {
        if (allocation.occupied > pushdown_memory_limit_bytes_ - pushdown_memory_in_use_)
          throw std::invalid_argument("restored allocations exceed shared DPU memory");
        pushdown_memory_in_use_ += allocation.occupied;
      }
      restored_[tenant.first].emplace(allocation.id, allocation);
    }
  }
}

void TransportManager::open(const FragmentSpec &fragment, Placement placement) {
  std::lock_guard<std::mutex> guard(mutex_);
  if (!validPlacement(placement)) throw std::invalid_argument("invalid fragment placement");
  if (!ledger_->account(fragment.tenant))
    throw std::invalid_argument("unknown fragment tenant");
  if ((placement == Placement::Pushdown && !fragment.pushdown_legal) ||
      (placement == Placement::Pullup && !fragment.pullup_legal))
    throw std::invalid_argument("illegal fragment placement");
  auto existing = fragments_.find(fragment.fragment);
  if (existing != fragments_.end()) {
    if (existing->second.spec.tenant != fragment.tenant || existing->second.open ||
        existing->second.spec.generation >= fragment.generation)
      throw std::invalid_argument("close and drain fragment before advancing generation");
    for (const auto &tenant : grants_)
      for (const auto &entry : tenant.second)
        if (entry.second.grant.fragment == fragment.fragment)
          throw std::invalid_argument("fragment still owns live allocations");
  }
  fragments_[fragment.fragment] = FragmentState{fragment, placement, true, {}};
}

AcquireResult TransportManager::acquire(const BatchRequest &request) {
  // Keep close/open, slot admission, and account reservation in one critical
  // section. No callback is executed while this lock is held.
  std::lock_guard<std::mutex> guard(mutex_);
  auto it = fragments_.find(request.fragment);
  if (it == fragments_.end()) return {AcquireStatus::UnknownFragment, std::nullopt};
  if (it->second.spec.generation != request.generation || it->second.spec.tenant != request.tenant)
    return {AcquireStatus::StaleGeneration, std::nullopt};
  if (!it->second.open) return {AcquireStatus::Closed, std::nullopt};
  if (it->second.batches.count(request.batch))
    return {AcquireStatus::DuplicateBatch, std::nullopt};
  const auto placement = it->second.placement;
  auto &used = placement == Placement::Pushdown ? pushdown_slots_in_use_ : pullup_slots_in_use_;
  const auto limit = placement == Placement::Pushdown ? pushdown_slot_limit_ : pullup_slot_limit_;
  if (used >= limit) return {AcquireStatus::WouldBlock, std::nullopt};
  if (placement == Placement::Pushdown &&
      request.memory_bytes > pushdown_memory_limit_bytes_ - pushdown_memory_in_use_)
    return {AcquireStatus::WouldBlock, std::nullopt};
  auto account = ledger_->account(request.tenant);
  if (!account) return {AcquireStatus::UnknownFragment, std::nullopt};
  auto id = account->reserve(request.memory_bytes, request.generation, placement);
  if (!id) return {AcquireStatus::WouldBlock, std::nullopt};
  Grant grant{request.tenant, *id, request.generation, placement, request.memory_bytes,
              request.fragment, request.batch};
  grants_[request.tenant].emplace(*id, GrantState{grant, false, false, request.memory_bytes, 0});
  it->second.batches.emplace(request.batch, *id);
  ++used;
  if (placement == Placement::Pushdown) pushdown_memory_in_use_ += request.memory_bytes;
  return {AcquireStatus::Granted, grant};
}

TransportManager::GrantState *TransportManager::findGrant(const Grant &grant) {
  auto tenant = grants_.find(grant.tenant);
  if (tenant == grants_.end()) return nullptr;
  auto it = tenant->second.find(grant.allocation);
  if (it == tenant->second.end()) return nullptr;
  const auto &original = it->second.grant;
  if (original.generation != grant.generation || original.placement != grant.placement ||
      original.fragment != grant.fragment || original.batch != grant.batch ||
      original.reserved_bytes != grant.reserved_bytes)
    return nullptr;
  return &it->second;
}

bool TransportManager::push(const Grant &grant, std::uint64_t bytes) {
  std::lock_guard<std::mutex> guard(mutex_);
  auto *state = findGrant(grant);
  if (!state || state->finished) return false;
  auto account = ledger_->account(grant.tenant);
  if (!account || !account->materialize(grant.allocation, grant.generation, bytes)) return false;
  state->reserved -= bytes;
  state->occupied += bytes;
  return true;
}

bool TransportManager::extend(const Grant &grant, std::uint64_t bytes) {
  std::lock_guard<std::mutex> guard(mutex_);
  auto *state = findGrant(grant);
  if (!state || state->finished) return false;
  if (grant.placement == Placement::Pushdown &&
      bytes > pushdown_memory_limit_bytes_ - pushdown_memory_in_use_) return false;
  auto account = ledger_->account(grant.tenant);
  if (!account || !account->reserveMore(grant.allocation, grant.generation, bytes)) return false;
  state->reserved += bytes;
  if (grant.placement == Placement::Pushdown) pushdown_memory_in_use_ += bytes;
  return true;
}

bool TransportManager::spillComplete(const Grant &grant, SpillId spill, std::uint64_t bytes) {
  std::lock_guard<std::mutex> guard(mutex_);
  auto *state = findGrant(grant);
  if (!state) return false;
  auto account = ledger_->account(grant.tenant);
  if (!account || !account->spillComplete(grant.allocation, grant.generation, spill, bytes))
    return false;
  state->occupied -= bytes;
  if (grant.placement == Placement::Pushdown) pushdown_memory_in_use_ -= bytes;
  return true;
}

bool TransportManager::finish(const TenantId &tenant, const Grant &grant,
                              double cpu_ms, std::uint64_t network_bytes) {
  if (tenant != grant.tenant) return false;
  std::lock_guard<std::mutex> guard(mutex_);
  auto *state = findGrant(grant);
  if (!state || state->finished) return false;
  auto account = ledger_->account(tenant);
  if (!account || !account->finish(grant.allocation, grant.generation, cpu_ms, network_bytes))
    return false;
  state->finished = true;
  if (grant.placement == Placement::Pushdown) pushdown_memory_in_use_ -= state->reserved;
  state->reserved = 0;
  auto &used = state->grant.placement == Placement::Pushdown ?
      pushdown_slots_in_use_ : pullup_slots_in_use_;
  --used;
  scheduler_->complete(ReadyBatch{tenant, grant.fragment, grant.batch, 1.0, grant.generation}, cpu_ms);
  if (state->release_pending) {
    releaseLocked(tenant, grant.allocation, grant.generation);
  }
  return true;
}

bool TransportManager::release(const TenantId &tenant, AllocationId allocation,
                               Generation generation) {
  std::lock_guard<std::mutex> guard(mutex_);
  return releaseLocked(tenant, allocation, generation);
}

bool TransportManager::releaseLocked(const TenantId &tenant, AllocationId allocation,
                                     Generation generation) {
  auto account = ledger_->account(tenant);
  if (!account) return false;
  auto owner = grants_.find(tenant);
  if (owner != grants_.end()) {
    auto live = owner->second.find(allocation);
    if (live != owner->second.end()) {
      if (!account->release(allocation, generation)) return false;
      if (live->second.grant.placement == Placement::Pushdown)
        pushdown_memory_in_use_ -= live->second.occupied;
      owner->second.erase(live);
      return true;
    }
  }
  auto old = restored_.find(tenant);
  if (old == restored_.end()) return false;
  auto restored = old->second.find(allocation);
  if (restored == old->second.end() || !account->release(allocation, generation)) return false;
  if (restored->second.placement == Placement::Pushdown)
    pushdown_memory_in_use_ -= restored->second.occupied;
  old->second.erase(restored);
  return true;
}

bool TransportManager::onFinalConsumer(const TenantId &tenant, AllocationId allocation,
                                       Generation generation) {
  std::lock_guard<std::mutex> guard(mutex_);
  auto account = ledger_->account(tenant);
  if (!account) return false;
  auto owner = grants_.find(tenant);
  if (owner == grants_.end() || !owner->second.count(allocation))
    return releaseLocked(tenant, allocation, generation);
  auto &state = owner->second.at(allocation);
  if (state.grant.generation != generation || state.release_pending) return false;
  if (!state.finished) {
    state.release_pending = true;
    return true;
  }
  return releaseLocked(tenant, allocation, generation);
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

std::uint64_t TransportManager::pushdownMemoryInUse() const {
  std::lock_guard<std::mutex> guard(mutex_);
  return pushdown_memory_in_use_;
}

} // namespace tide
