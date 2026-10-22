#include "tide/tide.hpp"

#include <array>
#include <cstdio>
#include <ctime>
#include <iostream>
#include <stdexcept>
#include <vector>
#include <unistd.h>

using namespace tide;

static void require(bool condition, const char *message) {
  if (!condition) throw std::runtime_error(message);
}

// The database must reserve a chunk before allocating/publishing beyond the
// existing reservation. No new allocation credit is needed for the extension.
static void scanFilter(TransportManager &transport, const Grant &grant) {
  std::vector<std::uint64_t> output;
  for (std::uint64_t value = 0; value < 32; ++value) {
    if (value % 2 != 0) continue;
    if (output.size() == 8)
      require(transport.extend(grant, 64), "scan extension blocked; yield and retry");
    require(transport.push(grant, sizeof(value)), "scan output lacks a reservation");
    output.push_back(value); // publish only after push succeeds
  }
  require(output.size() == 16 && output.back() == 30, "incorrect filter result");
  // Fast consumers can finish before the producer's completion event arrives.
  require(transport.onFinalConsumer(grant.tenant, grant.allocation, grant.generation),
          "last consumer event rejected");
}

static void aggregatePartition(TransportManager &transport, const Grant &grant) {
  struct Row { std::uint64_t key; std::uint64_t sum; };
  using Partition = std::array<Row, 4>;
  constexpr std::uint64_t state_bytes = sizeof(Partition);
  require(transport.push(grant, state_bytes), "aggregation state lacks a reservation");
  auto groups = std::make_unique<Partition>();
  for (std::uint64_t key = 0; key < 4; ++key) (*groups)[key] = Row{key, 0};
  for (std::uint64_t value = 0; value < 32; ++value) (*groups)[value % 4].sum += value;

  // A real HBTable adapter supplies its partition encoding and spill path.
  // This fixture uses a temporary file and fsync so that the completion below
  // is an actual durable write acknowledgement, rather than a copy callback.
  auto *file = std::tmpfile();
  require(file != nullptr, "cannot create spill file");
  const bool durable = std::fwrite(groups->data(), sizeof(Row), groups->size(), file) == groups->size()
      && std::fflush(file) == 0 && ::fsync(::fileno(file)) == 0;
  if (!durable) { std::fclose(file); throw std::runtime_error("spill write failed"); }
  require(transport.spillComplete(grant, 1, state_bytes), "durable spill rejected");
  require(!transport.spillComplete(grant, 1, state_bytes), "duplicate spill freed memory");
  groups.reset();

  // Reload on the same backend, reserving resident memory before the read.
  require(transport.extend(grant, state_bytes), "reload blocked; yield and retry");
  require(transport.push(grant, state_bytes), "reload lacks a reservation");
  std::rewind(file);
  groups = std::make_unique<Partition>();
  const bool read = std::fread(groups->data(), sizeof(Row), groups->size(), file) == groups->size();
  std::fclose(file);
  require(read, "cannot read restored aggregate");
  const std::array<std::uint64_t, 4> expected{112, 120, 128, 136};
  std::uint64_t sum = 0;
  for (std::size_t key = 0; key < groups->size(); ++key) {
    require((*groups)[key].key == key && (*groups)[key].sum == expected[key],
            "incorrect restored aggregate");
    sum += (*groups)[key].sum;
  }
  require(sum == 496, "incorrect aggregation result");
}

int main() {
  auto ledger = std::make_shared<ResourceLedger>();
  ledger->registerTenant("scan", TenantPolicy{1, 2, 256});
  ledger->registerTenant("aggregate", TenantPolicy{2, 2, 256});
  auto scheduler = std::make_shared<HierarchicalScheduler>(ledger, SchedulerPolicy{10, 1, true});
  TransportManager transport(ledger, scheduler, 1, 1, 256);
  transport.open({"scan", 1, 1, true, true, false}, Placement::Pushdown);
  transport.open({"aggregate", 2, 1, true, true, true}, Placement::Pushdown);
  scheduler->enqueue({"scan", 1, 1, 1, 1});
  scheduler->enqueue({"aggregate", 2, 1, 2, 1});
  while (const auto batch = scheduler->next()) {
    auto result = transport.acquire({batch->tenant, batch->fragment, batch->batch,
                                     batch->generation, 64});
    if (result.status == AcquireStatus::WouldBlock) {
      scheduler->defer(*batch); // retry after a capacity event in an async database
      throw std::runtime_error("unexpected blocking in serial fixture");
    }
    require(result.status == AcquireStatus::Granted && result.grant.has_value(), "grant failed");
    const auto grant = *result.grant;
    const auto start = std::clock();
    if (batch->fragment == 1) scanFilter(transport, grant);
    else aggregatePartition(transport, grant);
    const auto cpu_ms = 1000.0 * (std::clock() - start) / CLOCKS_PER_SEC;
    require(transport.finish(batch->tenant, grant, cpu_ms, 0), "completion rejected");
    if (batch->fragment == 2)
      require(transport.onFinalConsumer(batch->tenant, grant.allocation, grant.generation),
              "aggregate consumer event rejected");
    require(transport.close(batch->fragment, batch->generation), "close failed");
  }
  for (const auto &entry : ledger->snapshot()) {
    const auto &account = entry.second;
    require(account.occupied_bytes == 0 && account.reserved_bytes == 0 &&
            account.credits_in_use == 0, "live allocation leaked");
  }
  std::cout << "adapter demo: filter, aggregate, durable spill/reload, and final release passed\n";
}
