#include "tide/tide.hpp"

#include <array>
#include <cmath>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>

using namespace tide;

namespace {

void check(bool condition, const char *expression, int line) {
  if (!condition)
    throw std::runtime_error(std::string("line ") + std::to_string(line) + ": " + expression);
}
#define CHECK(expression) check(static_cast<bool>(expression), #expression, __LINE__)

template <class F> void rejects(F operation) {
  bool rejected = false;
  try { operation(); } catch (const std::invalid_argument &) { rejected = true; }
  CHECK(rejected);
}

std::shared_ptr<ResourceLedger> ledgerWith(const std::array<double, 3> &weights) {
  auto ledger = std::make_shared<ResourceLedger>();
  for (std::size_t i = 0; i < weights.size(); ++i)
    ledger->registerTenant(std::string(1, static_cast<char>('A' + i)),
                           TenantPolicy{weights[i], 4, 1024});
  return ledger;
}

void weightedSaturation() {
  HierarchicalScheduler scheduler(ledgerWith({1, 2, 4}));
  std::array<BatchId, 3> ids{1, 1, 1};
  std::array<int, 3> count{};
  for (std::size_t i = 0; i < ids.size(); ++i)
    scheduler.enqueue({std::string(1, static_cast<char>('A' + i)), 1, ids[i]++, 1});
  // One batch at a time per tenant, as with a real completion-driven producer.
  for (int i = 0; i < 7000; ++i) {
    const auto selected = scheduler.next();
    CHECK(selected);
    const auto tenant = static_cast<std::size_t>(selected->tenant[0] - 'A');
    ++count[tenant];
    CHECK(scheduler.complete(*selected, 1));
    scheduler.enqueue({selected->tenant, 1, ids[tenant]++, 1});
  }
  CHECK(std::abs(count[0] - 1000) <= 1);
  CHECK(std::abs(count[1] - 2000) <= 1);
  CHECK(std::abs(count[2] - 4000) <= 1);
  CHECK(scheduler.size() == 3);
}

void unequalBatchCosts() {
  HierarchicalScheduler scheduler(ledgerWith({1, 2, 4}));
  const std::array<double, 3> costs{2, 7, 3};
  std::array<BatchId, 3> ids{1, 1, 1};
  std::array<double, 3> service{};
  for (std::size_t i = 0; i < ids.size(); ++i)
    scheduler.enqueue({std::string(1, static_cast<char>('A' + i)), 1, ids[i]++, costs[i]});
  for (int i = 0; i < 10000; ++i) {
    const auto selected = scheduler.next();
    CHECK(selected);
    const auto tenant = static_cast<std::size_t>(selected->tenant[0] - 'A');
    service[tenant] += costs[tenant];
    CHECK(scheduler.complete(*selected, costs[tenant]));
    scheduler.enqueue({selected->tenant, 1, ids[tenant]++, costs[tenant]});
  }
  // Weighted service, rather than the batch count, converges to configured shares.
  CHECK(std::abs(service[0] - service[1] / 2) <= 7);
  CHECK(std::abs(service[0] - service[2] / 4) <= 7);
}

void inaccurateEstimatesReconcileShares() {
  HierarchicalScheduler scheduler(ledgerWith({1, 2, 4}));
  const std::array<double, 3> actual_costs{2, 7, 3};
  const std::array<double, 3> estimates{9, 1, 5};
  std::array<BatchId, 3> ids{1, 1, 1};
  std::array<double, 3> service{};
  for (std::size_t i = 0; i < ids.size(); ++i)
    scheduler.enqueue({std::string(1, static_cast<char>('A' + i)), 1, ids[i]++, estimates[i]});
  for (int i = 0; i < 10000; ++i) {
    const auto selected = scheduler.next();
    CHECK(selected);
    const auto tenant = static_cast<std::size_t>(selected->tenant[0] - 'A');
    service[tenant] += actual_costs[tenant];
    CHECK(scheduler.complete(*selected, actual_costs[tenant]));
    scheduler.enqueue({selected->tenant, 1, ids[tenant]++, estimates[tenant]});
  }
  CHECK(std::abs(service[0] - service[1] / 2) <= 7);
  CHECK(std::abs(service[0] - service[2] / 4) <= 7);
}

void fragmentFairness() {
  HierarchicalScheduler scheduler(ledgerWith({1, 1, 1}));
  for (BatchId batch = 1; batch <= 150; ++batch) {
    for (FragmentId fragment = 1; fragment <= 10; ++fragment)
      scheduler.enqueue({"A", fragment, batch, 1});
    scheduler.enqueue({"B", 1, batch, 1});
  }
  std::array<int, 10> fragments{};
  int a_count = 0;
  int b_count = 0;
  for (int i = 0; i < 200; ++i) {
    auto selected = scheduler.next();
    CHECK(selected);
    if (selected->tenant == "A") {
      ++a_count;
      ++fragments[selected->fragment - 1];
    } else ++b_count;
    CHECK(scheduler.complete(*selected, 1));
  }
  CHECK(a_count == 100);
  CHECK(b_count == 100);
  for (const auto count : fragments) CHECK(count == 10);
}

void noIdleWindfall() {
  HierarchicalScheduler scheduler(ledgerWith({1, 1, 1}));
  scheduler.enqueue({"A", 1, 1, 1});
  for (BatchId batch = 1; batch <= 200; ++batch) scheduler.enqueue({"B", 1, batch, 1});
  auto first = scheduler.next();
  CHECK(first && first->tenant == "A");
  CHECK(scheduler.complete(*first, 1));
  for (int i = 0; i < 100; ++i) {
    const auto selected = scheduler.next();
    CHECK(selected && selected->tenant == "B");
    CHECK(scheduler.complete(*selected, 1));
  }
  for (BatchId batch = 2; batch <= 101; ++batch) scheduler.enqueue({"A", 1, batch, 1});
  int a_count = 0;
  for (int i = 0; i < 20; ++i) {
    const auto selected = scheduler.next();
    CHECK(selected);
    if (selected->tenant == "A") ++a_count;
    CHECK(scheduler.complete(*selected, 1));
  }
  CHECK(a_count >= 9 && a_count <= 11);
}

void reconciliationAndIdentity() {
  HierarchicalScheduler scheduler(ledgerWith({1, 1, 1}));
  const ReadyBatch a{"A", 1, 1, 1, 7};
  scheduler.enqueue(a);
  scheduler.enqueue({"A", 1, 2, 1, 7});
  for (BatchId batch = 1; batch <= 12; ++batch) scheduler.enqueue({"B", 1, batch, 1, 7});
  CHECK(!scheduler.complete(a, 10)); // queued work cannot be completed
  rejects([&] { scheduler.enqueue(a); });
  const auto selected = scheduler.next();
  CHECK(selected && selected->tenant == "A" && selected->generation == 7);
  rejects([&] { scheduler.enqueue(a); });
  auto stale = a;
  stale.generation = 6;
  CHECK(!scheduler.complete(stale, 100));
  CHECK(scheduler.complete(*selected, 10));
  CHECK(!scheduler.complete(*selected, 100)); // cannot charge the same service twice
  rejects([&] { scheduler.enqueue(a); });
  for (int i = 0; i < 10; ++i) {
    const auto other = scheduler.next();
    CHECK(other && other->tenant == "B");
    CHECK(scheduler.complete(*other, 1));
  }
  const auto a_again = scheduler.next();
  CHECK(a_again && a_again->tenant == "A" && a_again->batch == 2);
  CHECK(scheduler.complete(*a_again, 1));
  auto new_generation = a;
  new_generation.generation = 8;
  scheduler.enqueue(new_generation);
  CHECK(!scheduler.complete(a, 0)); // cannot refund the new generation's charge

  HierarchicalScheduler refunds(ledgerWith({1, 1, 1}));
  refunds.enqueue({"A", 1, 1, 10});
  refunds.enqueue({"A", 1, 2, 1});
  refunds.enqueue({"B", 1, 1, 1});
  auto overestimated = refunds.next();
  CHECK(overestimated && overestimated->tenant == "A");
  CHECK(refunds.complete(*overestimated, 1));
  auto b = refunds.next();
  CHECK(b && b->tenant == "B");
  CHECK(refunds.complete(*b, 1));
  CHECK(!refunds.complete(*overestimated, 0));
  auto refunded_a = refunds.next();
  CHECK(refunded_a && refunded_a->tenant == "A");
}

void capacityDeferral() {
  HierarchicalScheduler scheduler(ledgerWith({1, 1, 1}));
  scheduler.enqueue({"A", 1, 1, 10, 7});
  scheduler.enqueue({"B", 1, 1, 1, 7});
  const auto blocked = scheduler.next();
  CHECK(blocked && blocked->tenant == "A");
  CHECK(scheduler.size() == 1);
  CHECK(scheduler.defer(*blocked));
  CHECK(scheduler.size() == 2);
  CHECK(!scheduler.defer(*blocked));
  CHECK(!scheduler.complete(*blocked, 10));
  // No service ran: retry must remain ahead of B, despite A's 10-ms estimate.
  const auto retry = scheduler.next();
  CHECK(retry && retry->tenant == "A" && retry->batch == blocked->batch);
  CHECK(retry->selection_attempt > blocked->selection_attempt);
  CHECK(!scheduler.defer(*blocked)); // old attempt cannot refund the new attempt
  CHECK(!scheduler.complete(*blocked, 0));
  CHECK(scheduler.complete(*retry, 1));
  CHECK(!scheduler.defer(*retry));
  const auto other = scheduler.next();
  CHECK(other && other->tenant == "B");
  CHECK(scheduler.complete(*other, 1));
  CHECK(scheduler.size() == 0);

  // Deferral has no long-term service charge and cannot manufacture refunds.
  HierarchicalScheduler shares(ledgerWith({1, 2, 4}));
  std::array<BatchId, 3> ids{1, 1, 1};
  std::array<int, 3> counts{};
  for (std::size_t i = 0; i < ids.size(); ++i)
    shares.enqueue({std::string(1, static_cast<char>('A' + i)), 1, ids[i]++, 1});
  for (int i = 0; i < 7000; ++i) {
    auto selected = shares.next();
    CHECK(selected);
    if (i % 3 == 0) {
      CHECK(shares.defer(*selected));
      CHECK(!shares.defer(*selected));
      const auto retried = shares.next();
      CHECK(retried && retried->tenant == selected->tenant && retried->batch == selected->batch);
      selected = retried;
    }
    const auto tenant = static_cast<std::size_t>(selected->tenant[0] - 'A');
    ++counts[tenant];
    CHECK(shares.complete(*selected, 1));
    shares.enqueue({selected->tenant, 1, ids[tenant]++, 1});
  }
  CHECK(std::abs(counts[0] - 1000) <= 1);
  CHECK(std::abs(counts[1] - 2000) <= 1);
  CHECK(std::abs(counts[2] - 4000) <= 1);
}

void serviceDebtWindows() {
  const SchedulerPolicy policy{10, 1, true};
  HierarchicalScheduler scheduler(ledgerWith({1, 1, 1}), policy);
  CHECK(scheduler.serviceDebt("A") == 0);
  scheduler.enqueue({"A", 1, 1, 1});
  scheduler.enqueue({"A", 1, 2, 1});
  const auto underestimated = scheduler.next();
  CHECK(underestimated);
  CHECK(scheduler.complete(*underestimated, 22));
  CHECK(scheduler.serviceDebt("A") == 21);
  CHECK(!scheduler.complete(*underestimated, 100));
  CHECK(scheduler.serviceDebt("A") == 21);
  CHECK(!scheduler.next());
  CHECK(scheduler.size() == 1); // debt gates work without deleting it
  scheduler.advanceWindow();  // first window consumed 22 > its weighted quantum
  CHECK(scheduler.serviceDebt("A") == 21);
  CHECK(!scheduler.next());
  scheduler.advanceWindow();  // unused 10 ms repays debt
  CHECK(scheduler.serviceDebt("A") == 11);
  CHECK(!scheduler.next());
  scheduler.advanceWindow();
  CHECK(scheduler.serviceDebt("A") == 1);
  CHECK(scheduler.next());

  HierarchicalScheduler weighted(ledgerWith({2, 1, 1}), policy);
  weighted.enqueue({"A", 1, 1, 1});
  weighted.enqueue({"A", 1, 2, 1});
  auto a = weighted.next();
  CHECK(a && weighted.complete(*a, 16));
  CHECK(weighted.serviceDebt("A") == 15);
  CHECK(weighted.next()); // weight 2 has quantum 20, not 10
  weighted.advanceWindow(); // 16 consumed, so only unused 4 repays debt
  CHECK(weighted.serviceDebt("A") == 11);
}

void boundedReleaseProgress() {
  HierarchicalScheduler scheduler(ledgerWith({1, 1, 1}), SchedulerPolicy{10, 1, true});
  scheduler.enqueue({"A", 1, 1, 1});
  scheduler.enqueue({"A", 1, 2, 1});
  const auto first = scheduler.next();
  CHECK(first && scheduler.complete(*first, 22));
  // Progress is behind ordinary work in the same fragment, and must bypass it.
  scheduler.enqueue({"A", 1, 3, 0.4, 0, 0, true});
  const auto progress = scheduler.next();
  CHECK(progress && progress->batch == 3 && progress->progress);
  CHECK(scheduler.complete(*progress, 0.6)); // reconcile allowance to actual 0.6
  CHECK(std::abs(scheduler.serviceDebt("A") - 21.2) < 1e-9);
  scheduler.enqueue({"A", 1, 4, 0.5, 0, 0, true});
  scheduler.enqueue({"A", 1, 5, 0.4, 0, 0, true});
  const auto fits = scheduler.next();
  CHECK(fits && fits->batch == 5); // 0.5 does not fit the remaining 0.4
  CHECK(scheduler.complete(*fits, 0.4));
  CHECK(!scheduler.next()); // neither ordinary work nor excess progress is eligible
  scheduler.advanceWindow();
  const auto reset_allowance = scheduler.next();
  CHECK(reset_allowance && reset_allowance->batch == 4);
}

void debtDeferralAndWindowEpochs() {
  HierarchicalScheduler scheduler(ledgerWith({1, 1, 1}), SchedulerPolicy{10, 1, true});
  scheduler.enqueue({"A", 1, 1, 1});
  scheduler.enqueue({"A", 1, 2, 1});
  const auto first = scheduler.next();
  CHECK(first && scheduler.complete(*first, 30));
  scheduler.enqueue({"A", 1, 3, 0.8, 0, 0, true});
  const auto old_window = scheduler.next();
  CHECK(old_window && old_window->progress);
  CHECK(scheduler.defer(*old_window));
  CHECK(!scheduler.defer(*old_window));
  CHECK(scheduler.serviceDebt("A") == 29); // unexecuted work creates no debt
  const auto retried = scheduler.next();
  CHECK(retried && retried->batch == 3);
  CHECK(!scheduler.defer(*old_window));
  scheduler.advanceWindow();
  scheduler.enqueue({"A", 1, 4, 0.8, 0, 0, true});
  const auto new_window = scheduler.next();
  CHECK(new_window && new_window->batch == 4);
  CHECK(scheduler.defer(*retried)); // no old-window refund to the new allowance
  CHECK(!scheduler.defer(*retried));
  scheduler.enqueue({"A", 1, 5, 0.3, 0, 0, true});
  CHECK(!scheduler.next()); // new-window reservation still consumes 0.8
  CHECK(scheduler.complete(*new_window, 1.2)); // underestimation can exceed allowance
  CHECK(!scheduler.next()); // subsequent grants respect the observed overshoot
  CHECK(std::abs(scheduler.serviceDebt("A") - 29.4) < 1e-9);

  HierarchicalScheduler late_complete(ledgerWith({1, 1, 1}), SchedulerPolicy{10, 1, true});
  late_complete.enqueue({"A", 1, 1, 1});
  auto selected = late_complete.next();
  CHECK(selected && late_complete.complete(*selected, 30));
  late_complete.enqueue({"A", 1, 2, 0.8, 0, 0, true});
  selected = late_complete.next();
  CHECK(selected);
  late_complete.advanceWindow();
  CHECK(late_complete.complete(*selected, 0.8));
  late_complete.enqueue({"A", 1, 3, 0.3, 0, 0, true});
  CHECK(!late_complete.next()); // late actual service is counted in the current window
}

void invalidInputsAndEmptyQueue() {
  rejects([] { HierarchicalScheduler scheduler(nullptr); });
  for (const auto quantum : {0.0, -1.0, std::numeric_limits<double>::infinity(),
                             std::numeric_limits<double>::quiet_NaN()})
    rejects([&] { HierarchicalScheduler scheduler(ledgerWith({1, 1, 1}), {quantum, 1, true}); });
  for (const auto allowance : {-1.0, std::numeric_limits<double>::infinity(),
                               std::numeric_limits<double>::quiet_NaN()})
    rejects([&] { HierarchicalScheduler scheduler(ledgerWith({1, 1, 1}), {10, allowance, true}); });
  HierarchicalScheduler scheduler(ledgerWith({1, 1, 1}));
  CHECK(!scheduler.next());
  CHECK(scheduler.size() == 0);
  rejects([&] { scheduler.enqueue({"unknown", 1, 1, 1}); });
  rejects([&] { scheduler.serviceDebt("unknown"); });
  rejects([&] { scheduler.enqueue({"", 1, 1, 1}); });
  for (const auto cost : {0.0, -1.0, std::numeric_limits<double>::infinity(),
                          std::numeric_limits<double>::quiet_NaN()})
    rejects([&] { scheduler.enqueue({"A", 1, 1, cost}); });
  scheduler.enqueue({"A", 1, 1, 1});
  auto selected = scheduler.next();
  CHECK(selected);
  for (const auto actual : {-1.0, std::numeric_limits<double>::infinity(),
                            std::numeric_limits<double>::quiet_NaN()})
    rejects([&] { scheduler.complete(*selected, actual); });
  CHECK(scheduler.complete(*selected, 0));
  CHECK(!scheduler.next());
  CHECK(scheduler.size() == 0);
}

} // namespace

int main() {
  try {
    weightedSaturation();
    unequalBatchCosts();
    inaccurateEstimatesReconcileShares();
    fragmentFairness();
    noIdleWindfall();
    reconciliationAndIdentity();
    capacityDeferral();
    serviceDebtWindows();
    boundedReleaseProgress();
    debtDeferralAndWindowEpochs();
    invalidInputsAndEmptyQueue();
    std::cout << "scheduler_test: all checks passed\n";
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "scheduler_test: " << error.what() << '\n';
    return 1;
  }
}
