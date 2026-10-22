#include "tide/tide.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <memory>
#include <vector>

using namespace tide;
using Clock = std::chrono::steady_clock;

static double percentile(std::vector<double> values, double fraction) {
  std::sort(values.begin(), values.end());
  const auto index = static_cast<std::size_t>(fraction * (values.size() - 1));
  return values[index];
}

int main() {
  std::puts("allocations,restore_median_ms,restore_p99_ms,replay_median_ms,replay_p99_ms,trials");
  for (const std::uint32_t count : {1U, 64U, 256U, 1024U}) {
    TenantAccount source(TenantPolicy{1.0, count + 1, count * 100ULL + 100});
    std::vector<AllocationId> ids;
    ids.reserve(count);
    for (std::uint32_t i = 0; i < count; ++i) {
      auto id = source.reserve(80, 7);
      assert(id && source.materialize(*id, 7, 70));
      assert(source.finish(*id, 7, 1.0, 128));
      ids.push_back(*id);
    }
    const auto saved = source.checkpoint();
    std::vector<double> restore_ms, replay_ms;
    for (int trial = 0; trial < 110; ++trial) {
      auto ledger = std::make_shared<ResourceLedger>();
      const auto t0 = Clock::now();
      ledger->restoreTenant("A", saved);
      const auto t1 = Clock::now();
      auto account = ledger->account("A");
      assert(account->snapshot().occupied_bytes == count * 70ULL);
      assert(account->snapshot().reserved_bytes == 0);
      for (const auto id : ids) assert(account->release(id, 7));
      const auto t2 = Clock::now();
      assert(account->snapshot().credits_in_use == 0);
      if (trial >= 10) {
        restore_ms.push_back(std::chrono::duration<double, std::milli>(t1 - t0).count());
        replay_ms.push_back(std::chrono::duration<double, std::milli>(t2 - t1).count());
      }
    }
    std::printf("%u,%.6f,%.6f,%.6f,%.6f,%zu\n", count,
                percentile(restore_ms, .50), percentile(restore_ms, .99),
                percentile(replay_ms, .50), percentile(replay_ms, .99),
                restore_ms.size());
  }
}
