#include "tide/tide.hpp"

#include <atomic>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <thread>

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

struct Runtime {
  std::shared_ptr<ResourceLedger> ledger = std::make_shared<ResourceLedger>();
  std::shared_ptr<HierarchicalScheduler> scheduler;
  std::unique_ptr<TransportManager> transport;

  explicit Runtime(std::uint64_t shared_bytes = 100) {
    ledger->registerTenant("A", {1.0, 4, 200});
    ledger->registerTenant("B", {1.0, 4, 200});
    scheduler = std::make_shared<HierarchicalScheduler>(ledger);
    transport = std::make_unique<TransportManager>(ledger, scheduler, 4, 4,
                                                  shared_bytes);
  }

  Grant acquire(const TenantId &tenant, FragmentId fragment, BatchId batch,
                std::uint64_t bytes) {
    auto result = transport->acquire({tenant, fragment, batch, 1, bytes});
    CHECK(result.status == AcquireStatus::Granted);
    CHECK(result.grant.has_value());
    return *result.grant;
  }
};

void sharedCapAcrossTenantsAndDurableSpill() {
  Runtime runtime;
  auto &transport = *runtime.transport;
  transport.open({"A", 1, 1, true, true, true}, Placement::Pushdown);
  transport.open({"B", 2, 1, true, true, false}, Placement::Pushdown);
  const auto a = runtime.acquire("A", 1, 1, 60);
  CHECK(transport.pushdownMemoryInUse() == 60);
  const auto b_before = runtime.ledger->account("B")->snapshot();
  // Both requests fit their own 200-byte tenant limit, but not the shared 100.
  const auto rejected = transport.acquire({"B", 2, 1, 1, 50});
  CHECK(rejected.status == AcquireStatus::WouldBlock && !rejected.grant);
  CHECK(transport.pushdownMemoryInUse() == 60);
  CHECK(transport.slotsInUse(Placement::Pushdown) == 1);
  sameAccount(b_before, runtime.ledger->account("B")->snapshot());

  const auto a_before = runtime.ledger->account("A")->snapshot();
  CHECK(!transport.extend(a, 41));
  CHECK(transport.pushdownMemoryInUse() == 60);
  sameAccount(a_before, runtime.ledger->account("A")->snapshot());
  const auto b = runtime.acquire("B", 2, 1, 40);
  CHECK(transport.pushdownMemoryInUse() == 100);
  CHECK(transport.push(a, 40));
  CHECK(transport.pushdownMemoryInUse() == 100);  // P -> U keeps the total.
  CHECK(!transport.extend(a, 1));
  CHECK(transport.pushdownMemoryInUse() == 100);

  // Starting an SSD write does not change the gate; only its durable ack does.
  CHECK(runtime.ledger->account("A")->snapshot().occupied_bytes == 40);
  CHECK(transport.pushdownMemoryInUse() == 100);
  auto stale = a;
  ++stale.generation;
  CHECK(!transport.spillComplete(stale, 7, 20));
  CHECK(!transport.spillComplete(a, 0, 20));
  CHECK(!transport.spillComplete(a, 7, 41));
  CHECK(transport.pushdownMemoryInUse() == 100);
  CHECK(transport.spillComplete(a, 7, 20));
  CHECK(transport.pushdownMemoryInUse() == 80);
  CHECK(!transport.spillComplete(a, 7, 20));
  CHECK(transport.pushdownMemoryInUse() == 80);
  CHECK(runtime.ledger->account("A")->snapshot().occupied_bytes == 20);

  CHECK(transport.extend(a, 20));
  CHECK(transport.pushdownMemoryInUse() == 100);
  CHECK(transport.push(b, 30));
  CHECK(transport.pushdownMemoryInUse() == 100);
  CHECK(transport.finish("B", b, 1.0, 0));
  CHECK(transport.pushdownMemoryInUse() == 90);  // Refund B's unused 10.
  CHECK(runtime.ledger->account("B")->snapshot().reserved_bytes == 0);
  CHECK(transport.onFinalConsumer("B", b.allocation, 1));
  CHECK(transport.pushdownMemoryInUse() == 60);
  CHECK(!transport.onFinalConsumer("B", b.allocation, 1));
  CHECK(transport.pushdownMemoryInUse() == 60);
  CHECK(transport.finish("A", a, 1.0, 0));
  CHECK(transport.pushdownMemoryInUse() == 20);  // Refund A's unused 40.
  CHECK(!transport.release("A", a.allocation, 0));
  CHECK(transport.pushdownMemoryInUse() == 20);
  CHECK(transport.release("A", a.allocation, 1));
  CHECK(transport.pushdownMemoryInUse() == 0);
  CHECK(!transport.release("A", a.allocation, 1));
  CHECK(transport.pushdownMemoryInUse() == 0);
}

void pullupMemoryDoesNotConsumeDpuCapacity() {
  Runtime runtime;
  auto &transport = *runtime.transport;
  transport.open({"A", 1, 1, true, true, false}, Placement::Pushdown);
  transport.open({"B", 2, 1, true, true, false}, Placement::Pullup);
  const auto pushdown = runtime.acquire("A", 1, 1, 90);
  const auto pullup = runtime.acquire("B", 2, 1, 150);
  CHECK(transport.pushdownMemoryInUse() == 90);
  CHECK(transport.extend(pullup, 25));
  CHECK(transport.push(pullup, 100));
  CHECK(transport.pushdownMemoryInUse() == 90);
  CHECK(runtime.ledger->account("B")->snapshot().occupied_bytes == 100);
  CHECK(runtime.ledger->account("B")->snapshot().reserved_bytes == 75);
  CHECK(transport.spillComplete(pullup, 1, 20));
  CHECK(transport.pushdownMemoryInUse() == 90);
  CHECK(transport.finish("B", pullup, 1.0, 0));
  CHECK(transport.onFinalConsumer("B", pullup.allocation, 1));
  CHECK(transport.pushdownMemoryInUse() == 90);
  CHECK(transport.finish("A", pushdown, 1.0, 0));
  CHECK(transport.pushdownMemoryInUse() == 0);  // All A bytes were still reserved.
  CHECK(transport.onFinalConsumer("A", pushdown.allocation, 1));

  Runtime zero_capacity(0);
  zero_capacity.transport->open({"B", 2, 1, true, true, false}, Placement::Pullup);
  const auto zero_pullup = zero_capacity.acquire("B", 2, 1, 100);
  CHECK(zero_capacity.transport->pushdownMemoryInUse() == 0);
  CHECK(zero_capacity.transport->finish("B", zero_pullup, 0, 0));
  CHECK(zero_capacity.transport->release("B", zero_pullup.allocation, 1));
}

TenantCheckpoint completedOutput(std::uint64_t bytes, Placement placement) {
  TenantAccount account(TenantPolicy{1.0, 4, 200});
  const auto id = account.reserve(bytes, 7, placement);
  CHECK(id && account.materialize(*id, 7, bytes));
  CHECK(account.finish(*id, 7, 1.0, 0));
  const auto checkpoint = account.checkpoint();
  CHECK(checkpoint.allocations.size() == 1);
  CHECK(checkpoint.allocations.front().placement == placement);
  return checkpoint;
}

void restoredBackendMemoryIsIncluded() {
  const auto dpu = completedOutput(50, Placement::Pushdown);
  const auto host = completedOutput(90, Placement::Pullup);
  auto ledger = std::make_shared<ResourceLedger>();
  ledger->restoreTenant("A", dpu);
  ledger->restoreTenant("B", host);
  auto scheduler = std::make_shared<HierarchicalScheduler>(ledger);
  TransportManager restored(ledger, scheduler, 4, 4, 60);
  CHECK(restored.pushdownMemoryInUse() == 50);
  restored.open({"A", 1000, 1, true, true, false}, Placement::Pushdown);
  const auto blocked = restored.acquire({"A", 1000, 1, 1, 11});
  CHECK(blocked.status == AcquireStatus::WouldBlock && !blocked.grant);
  CHECK(restored.pushdownMemoryInUse() == 50);
  CHECK(!restored.release("A", dpu.allocations.front().id, 6));
  CHECK(restored.pushdownMemoryInUse() == 50);
  CHECK(restored.onFinalConsumer("B", host.allocations.front().id, 7));
  CHECK(restored.pushdownMemoryInUse() == 50);
  CHECK(restored.onFinalConsumer("A", dpu.allocations.front().id, 7));
  CHECK(restored.pushdownMemoryInUse() == 0);
  CHECK(!restored.onFinalConsumer("A", dpu.allocations.front().id, 7));
  CHECK(restored.pushdownMemoryInUse() == 0);

  auto overcap = std::make_shared<ResourceLedger>();
  overcap->restoreTenant("A", completedOutput(60, Placement::Pushdown));
  overcap->restoreTenant("B", completedOutput(50, Placement::Pushdown));
  auto overcap_scheduler = std::make_shared<HierarchicalScheduler>(overcap);
  CHECK(invalid([&] {
    TransportManager rejected(overcap, overcap_scheduler, 4, 4, 100);
  }));
  CHECK(overcap->account("A")->snapshot().occupied_bytes == 60);
  CHECK(overcap->account("B")->snapshot().occupied_bytes == 50);
}

void earlyFinalConsumerKeepsGlobalChargeUntilCompletion() {
  Runtime runtime;
  auto &transport = *runtime.transport;
  transport.open({"A", 1, 1, true, true, false}, Placement::Pushdown);
  transport.open({"B", 2, 1, true, true, false}, Placement::Pushdown);
  const auto a = runtime.acquire("A", 1, 1, 80);
  CHECK(transport.push(a, 60));
  CHECK(transport.onFinalConsumer("A", a.allocation, 1));
  CHECK(!transport.onFinalConsumer("A", a.allocation, 1));
  CHECK(transport.pushdownMemoryInUse() == 80);
  CHECK(transport.acquire({"B", 2, 1, 1, 30}).status == AcquireStatus::WouldBlock);
  CHECK(transport.finish("A", a, 1.0, 0));
  CHECK(transport.pushdownMemoryInUse() == 0);
  CHECK(runtime.ledger->account("A")->snapshot().credits_in_use == 0);
  const auto b = runtime.acquire("B", 2, 1, 30);
  CHECK(transport.pushdownMemoryInUse() == 30);
  CHECK(transport.finish("B", b, 0, 0));
  CHECK(transport.pushdownMemoryInUse() == 0);
  CHECK(transport.onFinalConsumer("B", b.allocation, 1));
}

void concurrentTenantAdmissionCannotOversubscribe() {
  for (unsigned round = 0; round < 16; ++round) {
    Runtime runtime;
    auto &transport = *runtime.transport;
    transport.open({"A", 1, 1, true, true, false}, Placement::Pushdown);
    transport.open({"B", 2, 1, true, true, false}, Placement::Pushdown);
    std::atomic<bool> start{false};
    AcquireResult a, b;
    std::thread first([&] {
      while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
      a = transport.acquire({"A", 1, 1, 1, 60});
    });
    std::thread second([&] {
      while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
      b = transport.acquire({"B", 2, 1, 1, 60});
    });
    start.store(true, std::memory_order_release);
    first.join();
    second.join();
    CHECK((a.status == AcquireStatus::Granted) != (b.status == AcquireStatus::Granted));
    CHECK(a.status == AcquireStatus::Granted || a.status == AcquireStatus::WouldBlock);
    CHECK(b.status == AcquireStatus::Granted || b.status == AcquireStatus::WouldBlock);
    CHECK(transport.pushdownMemoryInUse() == 60);
    CHECK(transport.slotsInUse(Placement::Pushdown) == 1);
    const auto winner = a.grant ? *a.grant : *b.grant;
    CHECK(transport.finish(winner.tenant, winner, 1.0, 0));
    CHECK(transport.onFinalConsumer(winner.tenant, winner.allocation, 1));
    CHECK(transport.pushdownMemoryInUse() == 0);
  }
}

}  // namespace

int main() {
  sharedCapAcrossTenantsAndDurableSpill();
  pullupMemoryDoesNotConsumeDpuCapacity();
  restoredBackendMemoryIsIncluded();
  earlyFinalConsumerKeepsGlobalChargeUntilCompletion();
  concurrentTenantAdmissionCannotOversubscribe();
  std::cout << "tide_shared_memory_test: all checks passed\n";
}
