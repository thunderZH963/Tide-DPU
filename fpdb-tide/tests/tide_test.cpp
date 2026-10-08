#include "tide/tide.hpp"

#include <cassert>
#include <iostream>

using namespace tide;

int main() {
  auto ledger = std::make_shared<ResourceLedger>();
  ledger->registerTenant("A", TenantPolicy{1.0, 2, 100});
  ledger->registerTenant("B", TenantPolicy{2.0, 2, 100});
  auto scheduler = std::make_shared<HierarchicalScheduler>(ledger);
  TransportManager transport(ledger, scheduler, 1, 1);

  transport.open(FragmentSpec{"A", 10, 3, true, true, false}, Placement::Pushdown);
  auto acquired = transport.acquire(BatchRequest{"A", 10, 1, 3, 60});
  assert(acquired.status == AcquireStatus::Granted && acquired.grant);
  assert(transport.slotsInUse(Placement::Pushdown) == 1);
  assert(!transport.release("A", acquired.grant->allocation, 3));
  auto blocked_on_slot = transport.acquire(BatchRequest{"A", 10, 2, 3, 10});
  assert(blocked_on_slot.status == AcquireStatus::WouldBlock);
  assert(transport.push(*acquired.grant, 40));
  auto snap = ledger->account("A")->snapshot();
  assert(snap.occupied_bytes == 40 && snap.reserved_bytes == 20);
  assert(transport.finish("A", *acquired.grant, 2.5, 4096));
  assert(transport.slotsInUse(Placement::Pushdown) == 0);
  assert(!transport.finish("A", *acquired.grant, 2.5, 4096));
  assert(!transport.push(*acquired.grant, 1));
  // finish returns the execution slot, not the live allocation.
  snap = ledger->account("A")->snapshot();
  assert(snap.occupied_bytes == 40 && snap.credits_in_use == 1);
  assert(!transport.release("A", acquired.grant->allocation, 2)); // stale
  assert(transport.release("A", acquired.grant->allocation, 3));
  assert(!transport.release("A", acquired.grant->allocation, 3)); // duplicate
  snap = ledger->account("A")->snapshot();
  assert(snap.occupied_bytes == 0 && snap.reserved_bytes == 0 && snap.credits_in_use == 0);

  // A retained output blocks a new reservation even after its slot returns.
  auto held = ledger->account("A")->reserve(80, 4);
  assert(held && ledger->account("A")->materialize(*held, 4, 70));
  assert(ledger->account("A")->finish(*held, 4, 1.0, 0));
  assert(!ledger->account("A")->reserve(40, 4));
  assert(ledger->account("A")->snapshot().occupied_bytes == 70);
  assert(ledger->account("A")->release(*held, 4));
  assert(!ledger->account("A")->release(*held, 4));
  auto after_release = ledger->account("A")->reserve(40, 4);
  assert(after_release);
  assert(ledger->account("A")->finish(*after_release, 4, 0, 0));
  assert(ledger->account("A")->release(*after_release, 4));

  // Simulate a control-process restart from a saved account checkpoint.
  auto recover_source = std::make_shared<TenantAccount>(TenantPolicy{1.0, 2, 100});
  auto live_id = recover_source->reserve(80, 7);
  assert(live_id && recover_source->materialize(*live_id, 7, 70));
  assert(recover_source->finish(*live_id, 7, 1.25, 128));
  auto saved = recover_source->checkpoint();
  recover_source.reset();
  auto recovered_ledger = std::make_shared<ResourceLedger>();
  recovered_ledger->restoreTenant("R", saved);
  auto recovered_scheduler = std::make_shared<HierarchicalScheduler>(recovered_ledger);
  TransportManager recovered(recovered_ledger, recovered_scheduler, 1, 1);
  auto recovered_account = recovered_ledger->account("R");
  assert(recovered_account->snapshot().occupied_bytes == 70);
  assert(recovered_account->snapshot().reserved_bytes == 10);
  assert(!recovered_account->reserve(40, 8)); // live output survived restart
  assert(!recovered.release("R", *live_id, 6)); // stale replay
  assert(recovered.release("R", *live_id, 7));  // final-consumer replay
  assert(!recovered.release("R", *live_id, 7)); // duplicate replay
  auto fresh_id = recovered_account->reserve(40, 8);
  assert(fresh_id && *fresh_id > *live_id);
  assert(recovered_account->finish(*fresh_id, 8, 0, 0));
  assert(recovered_account->release(*fresh_id, 8));
  assert(recovered_account->snapshot().credits_in_use == 0);

  PlacementController controller;
  std::vector<PlacementCandidate> candidates{
      {1, "A", {6, 60, 5}, {0, 0, 45}, 3, 12, true, true},
      {2, "B", {5, 50, 5}, {0, 0, 55}, 4, 11, true, true}};
  auto decisions = controller.place(candidates, SharedBudget{8, 80, 60});
  assert(decisions.size() == 2);
  assert(decisions[0].placement == Placement::Pushdown);
  assert(decisions[1].placement == Placement::Pullup);
  assert(decisions[1].reservation.network_bytes == 55);

  bool rejected = false;
  try {
    controller.place({{3, "A", {9, 90, 5}, {0, 0, 70}, 2, 8, true, true}},
                     SharedBudget{8, 80, 60});
  } catch (const std::runtime_error &) {
    rejected = true;
  }
  assert(rejected);

  PlacementBudgetLedger budget(SharedBudget{8, 80, 60});
  const auto before = budget.snapshot();
  auto committed = controller.placeAndCommit(candidates, budget);
  assert(committed && committed->size() == 2);
  const auto after = budget.snapshot();
  assert(after.version == before.version + 1);
  assert(after.available.dpu_cpu_ms == 2);
  assert(after.available.dpu_memory_bytes == 20);
  assert(after.available.network_bytes == 0);
  // A stale shadow version cannot partially reserve resources.
  assert(!budget.tryCommit(before.version, *committed));
  const auto after_stale = budget.snapshot();
  assert(after_stale.version == after.version);
  assert(after_stale.available.network_bytes == after.available.network_bytes);

  scheduler->enqueue(ReadyBatch{"A", 1, 1, 1});
  scheduler->enqueue(ReadyBatch{"B", 2, 1, 1});
  assert(scheduler->size() == 2);
  assert(scheduler->next().has_value());

  std::cout << "tide_test: all checks passed\n";
  return 0;
}
