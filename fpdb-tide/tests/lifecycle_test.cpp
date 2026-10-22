#include "tide/tide.hpp"

#include <atomic>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <thread>
#include <vector>

using namespace tide;

#define CHECK(condition)                                                     \
  do {                                                                       \
    if (!(condition)) {                                                       \
      std::cerr << __FILE__ << ':' << __LINE__ << ": CHECK failed: "           \
                << #condition << '\n';                                       \
      std::abort();                                                           \
    }                                                                         \
  } while (false)

namespace {

bool invalid(const std::function<void()> &operation) {
  try {
    operation();
  } catch (const std::invalid_argument &) {
    return true;
  }
  return false;
}

void sameAccount(const TenantSnapshot &a, const TenantSnapshot &b) {
  CHECK(a.cpu_service_ms == b.cpu_service_ms);
  CHECK(a.network_bytes == b.network_bytes);
  CHECK(a.occupied_bytes == b.occupied_bytes);
  CHECK(a.reserved_bytes == b.reserved_bytes);
  CHECK(a.credits_in_use == b.credits_in_use);
}

void sameBudget(const BudgetSnapshot &a, const BudgetSnapshot &b) {
  CHECK(a.version == b.version);
  CHECK(a.available.dpu_cpu_ms == b.available.dpu_cpu_ms);
  CHECK(a.available.dpu_memory_bytes == b.available.dpu_memory_bytes);
  CHECK(a.available.network_bytes == b.available.network_bytes);
}

struct Runtime {
  std::shared_ptr<ResourceLedger> ledger = std::make_shared<ResourceLedger>();
  std::shared_ptr<HierarchicalScheduler> scheduler;
  std::unique_ptr<TransportManager> transport;

  Runtime() {
    ledger->registerTenant("A", TenantPolicy{1.0, 4, 100});
    scheduler = std::make_shared<HierarchicalScheduler>(ledger);
    transport = std::make_unique<TransportManager>(ledger, scheduler, 2, 2);
  }

  Grant acquire(FragmentId fragment, BatchId batch, Generation generation,
                std::uint64_t bytes) {
    auto result = transport->acquire({"A", fragment, batch, generation, bytes});
    CHECK(result.status == AcquireStatus::Granted);
    CHECK(result.grant.has_value());
    return *result.grant;
  }
};

void incrementalReservationAndDurableSpill() {
  TenantAccount account(TenantPolicy{1.0, 2, 100});
  auto id = account.reserve(40, 7);
  CHECK(id.has_value());
  CHECK(account.materialize(*id, 7, 30));
  CHECK(account.reserveMore(*id, 7, 50));
  auto before = account.snapshot();
  CHECK(before.occupied_bytes == 30);
  CHECK(before.reserved_bytes == 60);
  CHECK(before.credits_in_use == 1);  // Chunks share one allocation credit.
  CHECK(!account.reserveMore(*id, 7, 11));
  CHECK(!account.reserveMore(*id, 6, 1));
  sameAccount(before, account.snapshot());
  CHECK(account.materialize(*id, 7, 20));
  CHECK(account.snapshot().occupied_bytes == 50);

  CHECK(!account.spillComplete(*id, 6, 100, 10));
  CHECK(!account.spillComplete(*id, 7, 0, 10));
  CHECK(!account.spillComplete(*id, 7, 100, 51));
  CHECK(account.spillComplete(*id, 7, 100, 10));
  before = account.snapshot();
  CHECK(before.occupied_bytes == 40 && before.reserved_bytes == 40);
  CHECK(!account.spillComplete(*id, 7, 100, 10));
  sameAccount(before, account.snapshot());

  // A durable spill replay after a restart must not subtract the bytes twice.
  const auto checkpoint = account.checkpoint();
  CHECK(checkpoint.allocations.size() == 1);
  CHECK(checkpoint.allocations.front().spills.size() == 1);
  TenantAccount restored(checkpoint);
  CHECK(!restored.spillComplete(*id, 7, 100, 10));
  sameAccount(before, restored.snapshot());
  CHECK(restored.finish(*id, 7, 2.5, 128));
  auto finished = restored.snapshot();
  CHECK(finished.reserved_bytes == 0);  // Completion refunds unused P.
  CHECK(finished.occupied_bytes == 40 && finished.credits_in_use == 1);
  CHECK(!restored.reserveMore(*id, 7, 1));
  CHECK(!restored.materialize(*id, 7, 1));
  CHECK(!restored.release(*id, 6));
  CHECK(restored.release(*id, 7));
  CHECK(!restored.release(*id, 7));
  CHECK(restored.snapshot().occupied_bytes == 0);
  CHECK(restored.snapshot().credits_in_use == 0);
}

void grantIdentityAndGeneration() {
  Runtime runtime;
  auto &transport = *runtime.transport;
  transport.open({"A", 10, 3, true, true, false}, Placement::Pushdown);
  const auto grant = runtime.acquire(10, 1, 3, 60);
  const auto duplicate = transport.acquire({"A", 10, 1, 3, 60});
  CHECK(duplicate.status == AcquireStatus::DuplicateBatch);
  CHECK(!duplicate.grant);
  CHECK(transport.slotsInUse(Placement::Pushdown) == 1);
  CHECK(runtime.ledger->account("A")->snapshot().credits_in_use == 1);

  auto forged = grant;
  forged.placement = Placement::Pullup;
  CHECK(!transport.finish("A", forged, 1.0, 10));
  CHECK(!transport.push(forged, 1));
  CHECK(transport.slotsInUse(Placement::Pushdown) == 1);
  CHECK(transport.slotsInUse(Placement::Pullup) == 0);
  forged = grant;
  ++forged.batch;
  CHECK(!transport.finish("A", forged, 1.0, 10));

  CHECK(invalid([&] {
    transport.open({"A", 10, 4, true, true, false}, Placement::Pullup);
  }));
  CHECK(transport.push(grant, 40));
  CHECK(transport.close(10, 3));
  CHECK(transport.acquire({"A", 10, 2, 3, 10}).status == AcquireStatus::Closed);
  CHECK(invalid([&] {
    transport.open({"A", 10, 4, true, true, false}, Placement::Pullup);
  }));
  CHECK(transport.finish("A", grant, 1.0, 10));
  CHECK(transport.slotsInUse(Placement::Pushdown) == 0);
  // Returning an execution slot does not drain a retained output.
  CHECK(invalid([&] {
    transport.open({"A", 10, 4, true, true, false}, Placement::Pullup);
  }));
  CHECK(transport.onFinalConsumer("A", grant.allocation, 3));
  transport.open({"A", 10, 4, true, true, false}, Placement::Pullup);
  CHECK(transport.acquire({"A", 10, 1, 3, 10}).status ==
        AcquireStatus::StaleGeneration);
  const auto next = runtime.acquire(10, 1, 4, 10);
  CHECK(transport.slotsInUse(Placement::Pullup) == 1);
  CHECK(transport.finish("A", next, 0, 0));
  CHECK(transport.release("A", next.allocation, 4));
}

void earlyConsumerAndConcurrentTerminalEvents() {
  Runtime runtime;
  auto &transport = *runtime.transport;
  transport.open({"A", 20, 1, true, true, false}, Placement::Pushdown);
  const auto grant = runtime.acquire(20, 1, 1, 70);
  CHECK(transport.push(grant, 50));
  CHECK(!transport.release("A", grant.allocation, 1));
  CHECK(!transport.onFinalConsumer("A", grant.allocation, 0));
  CHECK(transport.onFinalConsumer("A", grant.allocation, 1));
  CHECK(!transport.onFinalConsumer("A", grant.allocation, 1));
  auto held = runtime.ledger->account("A")->snapshot();
  CHECK(held.occupied_bytes == 50 && held.reserved_bytes == 20);
  CHECK(held.credits_in_use == 1);
  CHECK(transport.slotsInUse(Placement::Pushdown) == 1);
  CHECK(transport.finish("A", grant, 2.0, 30));
  const auto done = runtime.ledger->account("A")->snapshot();
  CHECK(done.occupied_bytes == 0 && done.reserved_bytes == 0);
  CHECK(done.credits_in_use == 0 && done.cpu_service_ms == 2.0);
  CHECK(!transport.finish("A", grant, 2.0, 30));
  CHECK(!transport.onFinalConsumer("A", grant.allocation, 1));

  // Every ordering of duplicate completion and final-reader callbacks is safe.
  for (unsigned round = 0; round < 16; ++round) {
    const auto next = runtime.acquire(20, 2 + round, 1, 30);
    CHECK(transport.push(next, 20));
    std::atomic<bool> start{false};
    std::atomic<unsigned> finishes{0}, releases{0};
    std::vector<std::thread> workers;
    for (unsigned i = 0; i < 8; ++i) {
      workers.emplace_back([&, i] {
        while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
        if (i % 2 == 0) {
          if (transport.finish("A", next, 1.0, 1)) ++finishes;
        } else if (transport.onFinalConsumer("A", next.allocation, 1)) {
          ++releases;
        }
      });
    }
    start.store(true, std::memory_order_release);
    for (auto &worker : workers) worker.join();
    CHECK(finishes == 1 && releases == 1);
    CHECK(transport.slotsInUse(Placement::Pushdown) == 0);
    const auto state = runtime.ledger->account("A")->snapshot();
    CHECK(state.credits_in_use == 0 && state.occupied_bytes == 0);
    CHECK(state.reserved_bytes == 0);
  }
}

void restoreRequiresBackendReconciliation() {
  TenantAccount source(TenantPolicy{1.0, 2, 100});
  auto id = source.reserve(70, 9);
  CHECK(id && source.materialize(*id, 9, 50));
  auto ledger = std::make_shared<ResourceLedger>();
  ledger->restoreTenant("A", source.checkpoint());
  auto scheduler = std::make_shared<HierarchicalScheduler>(ledger);
  CHECK(invalid([&] { TransportManager rejected(ledger, scheduler, 1, 1); }));
  // The database first reconciles the backend's terminal status.
  CHECK(ledger->account("A")->finish(*id, 9, 1.0, 1));
  TransportManager restored(ledger, scheduler, 1, 1);
  CHECK(ledger->account("A")->snapshot().occupied_bytes == 50);
  CHECK(restored.onFinalConsumer("A", *id, 9));
  CHECK(!restored.onFinalConsumer("A", *id, 9));
}

void numericAndCheckpointValidation() {
  const auto inf = std::numeric_limits<double>::infinity();
  const auto nan = std::numeric_limits<double>::quiet_NaN();
  const auto maximum = std::numeric_limits<std::uint64_t>::max();
  TenantAccount account(TenantPolicy{1.0, 2, 100});
  auto id = account.reserve(20, 1);
  CHECK(id.has_value());
  const auto before = account.snapshot();
  CHECK(!account.finish(*id, 1, inf, 0));
  CHECK(!account.finish(*id, 1, nan, 0));
  CHECK(!account.finish(*id, 1, -1, 0));
  sameAccount(before, account.snapshot());
  CHECK(account.finish(*id, 1, 0.5, 0));
  CHECK(account.release(*id, 1));

  TenantCheckpoint near_overflow;
  near_overflow.policy = {1.0, 2, 100};
  near_overflow.network_bytes = maximum - 3;
  TenantAccount network(near_overflow);
  auto network_id = network.reserve(10, 1);
  CHECK(network_id.has_value());
  const auto network_before = network.snapshot();
  CHECK(!network.finish(*network_id, 1, 1.0, 4));
  sameAccount(network_before, network.snapshot());
  CHECK(network.finish(*network_id, 1, 1.0, 3));
  CHECK(network.snapshot().network_bytes == maximum);

  TenantCheckpoint exhausted;
  exhausted.policy = {1.0, 2, 100};
  exhausted.next_allocation = maximum;
  TenantAccount exhausted_account(exhausted);
  CHECK(!exhausted_account.reserve(1, 1));
  CHECK(exhausted_account.checkpoint().next_allocation == maximum);
  CHECK(exhausted_account.snapshot().credits_in_use == 0);

  auto malformed = exhausted;
  malformed.next_allocation = 0;
  CHECK(invalid([&] { TenantAccount rejected(malformed); }));
  malformed = near_overflow;
  malformed.cpu_service_ms = inf;
  CHECK(invalid([&] { TenantAccount rejected(malformed); }));
  malformed.cpu_service_ms = nan;
  CHECK(invalid([&] { TenantAccount rejected(malformed); }));
  malformed = near_overflow;
  malformed.next_allocation = 3;
  malformed.allocations = {{1, 1, 1, 0, true, {}}};
  CHECK(invalid([&] { TenantAccount rejected(malformed); }));
  malformed.allocations = {{1, 1, 0, 40, true, {}},
                           {1, 1, 0, 40, true, {}}};
  CHECK(invalid([&] { TenantAccount rejected(malformed); }));
  malformed.allocations = {{1, 1, 60, 50, false, {}}};
  CHECK(invalid([&] { TenantAccount rejected(malformed); }));
}

void versionedPlacementOwnership() {
  PlacementBudgetLedger ledger({8, 100, 100});
  const PlacementDecision first{1, Placement::Pushdown, {2, 30, 20}, 7};
  const auto initial = ledger.snapshot();
  CHECK(!ledger.tryCommit(initial.version, {first, first}));
  sameBudget(initial, ledger.snapshot());
  auto invalid_decision = first;
  invalid_decision.fragment = 2;
  invalid_decision.reservation.dpu_cpu_ms =
      std::numeric_limits<double>::quiet_NaN();
  CHECK(!ledger.tryCommit(initial.version, {first, invalid_decision}));
  sameBudget(initial, ledger.snapshot());
  CHECK(ledger.tryCommit(initial.version, {first}));
  auto committed = ledger.snapshot();
  CHECK(committed.available.dpu_cpu_ms == 6);
  CHECK(committed.available.dpu_memory_bytes == 70);
  CHECK(committed.available.network_bytes == 80);
  CHECK(!ledger.release(1, 6));
  sameBudget(committed, ledger.snapshot());
  CHECK(!ledger.tryCommit(initial.version, {{2, Placement::Pullup, {0, 0, 10}, 1}}));
  sameBudget(committed, ledger.snapshot());
  CHECK(ledger.release(1, 7));
  auto released = ledger.snapshot();
  CHECK(released.available.dpu_cpu_ms == 8);
  CHECK(released.available.dpu_memory_bytes == 100);
  CHECK(released.available.network_bytes == 100);
  CHECK(!ledger.release(1, 7));
  sameBudget(released, ledger.snapshot());
  CHECK(!ledger.tryCommit(released.version, {first}));
  sameBudget(released, ledger.snapshot());
  auto next = first;
  next.generation = 8;
  CHECK(ledger.tryCommit(released.version, {next}));
  committed = ledger.snapshot();
  CHECK(!ledger.release(1, 7));
  sameBudget(committed, ledger.snapshot());
  CHECK(ledger.release(1, 8));

  const auto maximum = std::numeric_limits<std::uint64_t>::max();
  PlacementBudgetLedger overflow({8, maximum, maximum});
  const auto overflow_before = overflow.snapshot();
  CHECK(!overflow.tryCommit(overflow_before.version,
       {{1, Placement::Pushdown, {1, maximum, maximum}, 1},
        {2, Placement::Pushdown, {1, 1, 1}, 1}}));
  sameBudget(overflow_before, overflow.snapshot());
}

}  // namespace

int main() {
  incrementalReservationAndDurableSpill();
  grantIdentityAndGeneration();
  earlyConsumerAndConcurrentTerminalEvents();
  restoreRequiresBackendReconciliation();
  numericAndCheckpointValidation();
  versionedPlacementOwnership();
  std::cout << "tide_lifecycle_test: all checks passed\n";
}
