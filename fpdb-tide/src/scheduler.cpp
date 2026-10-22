#include "tide/tide.hpp"

#include <algorithm>
#include <cmath>
#include <stdexcept>

namespace tide {

bool HierarchicalScheduler::BatchKey::operator==(const BatchKey &other) const {
  return tenant == other.tenant && fragment == other.fragment &&
         batch == other.batch && generation == other.generation;
}

std::size_t HierarchicalScheduler::BatchKeyHash::operator()(const BatchKey &key) const {
  auto seed = std::hash<TenantId>{}(key.tenant);
  const auto mix = [&seed](std::uint64_t value) {
    seed ^= std::hash<std::uint64_t>{}(value) + 0x9e3779b9U + (seed << 6U) + (seed >> 2U);
  };
  mix(key.fragment);
  mix(key.batch);
  mix(key.generation);
  return seed;
}

HierarchicalScheduler::HierarchicalScheduler(std::shared_ptr<ResourceLedger> ledger,
                                           SchedulerPolicy policy)
    : ledger_(std::move(ledger)), policy_(policy) {
  if (!ledger_) throw std::invalid_argument("scheduler requires a resource ledger");
  if (!std::isfinite(policy_.service_quantum_ms) || policy_.service_quantum_ms <= 0 ||
      !std::isfinite(policy_.progress_allowance_ms) || policy_.progress_allowance_ms < 0)
    throw std::invalid_argument("scheduler quantum must be positive and allowance nonnegative");
}

void HierarchicalScheduler::enqueue(ReadyBatch batch) {
  if (batch.tenant.empty() || !std::isfinite(batch.estimated_service_ms) ||
      batch.estimated_service_ms <= 0 || batch.selection_attempt != 0)
    throw std::invalid_argument("ready batch requires a tenant and positive finite service");
  const auto account = ledger_->account(batch.tenant);
  if (!account) throw std::invalid_argument("unknown scheduler tenant");
  const auto weight = account->snapshot().policy.weight;
  if (!std::isfinite(weight) || weight <= 0)
    throw std::invalid_argument("scheduler tenant requires a positive finite weight");

  std::lock_guard<std::mutex> guard(mutex_);
  const BatchKey key{batch.tenant, batch.fragment, batch.batch, batch.generation};
  if (work_.find(key) != work_.end())
    throw std::invalid_argument("batch identity was already enqueued");
  auto &tenant = queues_[batch.tenant];
  tenant.weight = weight;
  // Idle tenants cannot bank service while other tenants consume the device.
  if (tenant.ready_count == 0 && tenant.in_flight == 0)
    tenant.virtual_service = std::max(tenant.virtual_service, virtual_time_);
  auto inserted = tenant.fragments.try_emplace(batch.fragment);
  if (inserted.second) tenant.fragment_order.push_back(batch.fragment);
  inserted.first->second.push_back(std::move(batch));
  WorkRecord record{inserted.first->second.back().estimated_service_ms, weight};
  record.progress = inserted.first->second.back().progress;
  work_.emplace(key, record);
  ++tenant.ready_count;
  ++ready_count_;
}

std::optional<ReadyBatch> HierarchicalScheduler::next() {
  std::lock_guard<std::mutex> guard(mutex_);
  auto best = queues_.end();
  std::size_t best_fragment = 0;
  std::size_t best_batch = 0;
  bool best_uses_progress = false;
  for (auto it = queues_.begin(); it != queues_.end(); ++it) {
    const auto &tenant = it->second;
    if (tenant.ready_count == 0) continue;
    const auto quantum = static_cast<long double>(policy_.service_quantum_ms) * tenant.weight;
    const bool debt_gated = policy_.enforce_debt && tenant.service_debt > quantum;
    const auto allowance = static_cast<long double>(policy_.progress_allowance_ms) -
                           tenant.progress_used;
    bool eligible = false;
    std::size_t fragment_index = tenant.cursor;
    std::size_t batch_index = 0;
    for (std::size_t offset = 0; offset < tenant.fragment_order.size(); ++offset) {
      fragment_index = (tenant.cursor + offset) % tenant.fragment_order.size();
      const auto &fragment = tenant.fragments.at(tenant.fragment_order[fragment_index]);
      if (!debt_gated) {
        eligible = true;
        break;
      }
      // Release-enabling work may bypass an ordinary head, but the ordinary
      // work itself remains queued and does not borrow the progress allowance.
      for (batch_index = 0; batch_index < fragment.size(); ++batch_index) {
        if (fragment[batch_index].progress && fragment[batch_index].estimated_service_ms <= allowance) {
          eligible = true;
          break;
        }
      }
      if (eligible) break;
    }
    if (!eligible) continue;
    if (best == queues_.end() ||
        it->second.virtual_service < best->second.virtual_service ||
        (it->second.virtual_service == best->second.virtual_service && it->first < best->first)) {
      best = it;
      best_fragment = fragment_index;
      best_batch = batch_index;
      best_uses_progress = debt_gated;
    }
  }
  if (best == queues_.end()) return std::nullopt;

  auto &tenant = best->second;
  auto &fragment = tenant.fragments.at(tenant.fragment_order[best_fragment]);
  const auto &front = fragment[best_batch];
  auto &record = work_.at(BatchKey{front.tenant, front.fragment, front.batch, front.generation});
  const auto charged = tenant.virtual_service +
      static_cast<long double>(record.estimate) / static_cast<long double>(record.weight);
  if (!std::isfinite(charged)) throw std::overflow_error("scheduler virtual service overflow");
  if (record.selection_attempt == std::numeric_limits<std::uint64_t>::max())
    throw std::overflow_error("scheduler selection attempt overflow");
  // Advance to the selected start tag, not the remaining queue's tag: otherwise
  // a tenant that submits its next batch after selection loses its fair share.
  virtual_time_ = std::max(virtual_time_, tenant.virtual_service);
  tenant.virtual_service = charged;
  auto selected = std::move(fragment[best_batch]);
  fragment.erase(fragment.begin() + best_batch);
  if (fragment.empty()) {
    tenant.fragments.erase(selected.fragment);
    tenant.fragment_order.erase(tenant.fragment_order.begin() + best_fragment);
    tenant.cursor = best_fragment == tenant.fragment_order.size() ? 0 : best_fragment;
  } else {
    tenant.cursor = (best_fragment + 1) % tenant.fragment_order.size();
  }
  --tenant.ready_count;
  --ready_count_;
  ++tenant.in_flight;
  record.status = WorkStatus::Selected;
  record.used_progress_allowance = best_uses_progress;
  record.progress_window = window_;
  if (best_uses_progress) tenant.progress_used += record.estimate;
  selected.selection_attempt = ++record.selection_attempt;
  return selected;
}

bool HierarchicalScheduler::complete(const ReadyBatch &batch, double actual_service_ms) {
  if (!std::isfinite(actual_service_ms) || actual_service_ms < 0)
    throw std::invalid_argument("actual scheduler service must be finite and nonnegative");
  std::lock_guard<std::mutex> guard(mutex_);
  const auto it = work_.find(BatchKey{batch.tenant, batch.fragment, batch.batch, batch.generation});
  if (it == work_.end() || it->second.status != WorkStatus::Selected ||
      (batch.selection_attempt != 0 && batch.selection_attempt != it->second.selection_attempt))
    return false;
  auto &record = it->second;
  auto &tenant = queues_.at(batch.tenant);
  const auto reconciled = tenant.virtual_service +
      (static_cast<long double>(actual_service_ms) - static_cast<long double>(record.estimate)) /
          static_cast<long double>(record.weight);
  const auto debt = std::max(0.0L, tenant.service_debt +
      static_cast<long double>(actual_service_ms) - record.estimate);
  const auto window_service = tenant.actual_window_service + actual_service_ms;
  auto progress_used = tenant.progress_used;
  if (record.used_progress_allowance) {
    // An old-window reservation was reset by advanceWindow; its completion
    // consumes actual service in this window instead of refunding a new grant.
    progress_used += static_cast<long double>(actual_service_ms) -
                     (record.progress_window == window_ ? record.estimate : 0.0);
    progress_used = std::max(0.0L, progress_used);
  }
  if (!std::isfinite(reconciled) || !std::isfinite(debt) ||
      !std::isfinite(window_service) || !std::isfinite(progress_used))
    throw std::overflow_error("scheduler service accounting overflow");
  tenant.virtual_service = reconciled;
  tenant.service_debt = debt;
  tenant.actual_window_service = window_service;
  tenant.progress_used = progress_used;
  --tenant.in_flight;
  record.status = WorkStatus::Completed;
  return true;
}

bool HierarchicalScheduler::defer(const ReadyBatch &batch) {
  std::lock_guard<std::mutex> guard(mutex_);
  const auto it = work_.find(BatchKey{batch.tenant, batch.fragment, batch.batch, batch.generation});
  if (it == work_.end() || it->second.status != WorkStatus::Selected ||
      batch.selection_attempt == 0 || batch.selection_attempt != it->second.selection_attempt)
    return false;
  auto &record = it->second;
  auto &tenant = queues_.at(batch.tenant);
  const auto refunded = tenant.virtual_service -
      static_cast<long double>(record.estimate) / static_cast<long double>(record.weight);
  if (!std::isfinite(refunded)) throw std::overflow_error("scheduler virtual service overflow");
  auto inserted = tenant.fragments.try_emplace(batch.fragment);
  if (inserted.second) tenant.fragment_order.push_back(batch.fragment);
  // Reconstruct from the canonical reservation, not caller-supplied estimates.
  ReadyBatch retry{batch.tenant, batch.fragment, batch.batch, record.estimate, batch.generation};
  retry.progress = record.progress;
  inserted.first->second.push_front(std::move(retry));
  tenant.virtual_service = refunded;
  if (record.used_progress_allowance && record.progress_window == window_)
    tenant.progress_used = std::max(0.0L, tenant.progress_used - record.estimate);
  --tenant.in_flight;
  ++tenant.ready_count;
  ++ready_count_;
  record.status = WorkStatus::Queued;
  record.used_progress_allowance = false;
  return true;
}

void HierarchicalScheduler::advanceWindow() {
  std::lock_guard<std::mutex> guard(mutex_);
  if (window_ == std::numeric_limits<std::uint64_t>::max())
    throw std::overflow_error("scheduler service window overflow");
  for (auto &entry : queues_) {
    auto &tenant = entry.second;
    const auto quantum = static_cast<long double>(policy_.service_quantum_ms) * tenant.weight;
    const auto unused = std::max(0.0L, quantum - tenant.actual_window_service);
    tenant.service_debt = std::max(0.0L, tenant.service_debt - unused);
    tenant.actual_window_service = 0;
    tenant.progress_used = 0;
  }
  ++window_;
}

double HierarchicalScheduler::serviceDebt(const TenantId &tenant) const {
  if (!ledger_->account(tenant)) throw std::invalid_argument("unknown scheduler tenant");
  std::lock_guard<std::mutex> guard(mutex_);
  const auto it = queues_.find(tenant);
  if (it == queues_.end()) return 0;
  if (it->second.service_debt > std::numeric_limits<double>::max())
    throw std::overflow_error("scheduler debt exceeds double range");
  return static_cast<double>(it->second.service_debt);
}

std::size_t HierarchicalScheduler::size() const {
  std::lock_guard<std::mutex> guard(mutex_);
  return ready_count_;
}

} // namespace tide
