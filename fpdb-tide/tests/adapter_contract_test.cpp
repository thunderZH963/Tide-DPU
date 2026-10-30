#include "tide/tide.hpp"

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <iterator>
#include <memory>
#include <numeric>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

// These fixtures exercise different database adapter contracts using the real
// Tide library. They do not link or benchmark any native SQL engine binary.
namespace {
using namespace tide;
using Values = std::vector<std::uint64_t>;
std::size_t checks = 0;

void require(bool condition, const char *message) {
  ++checks;
  if (!condition) throw std::runtime_error(message);
}

std::uint64_t bytes(const Values &values) {
  return values.size() * sizeof(Values::value_type);
}

Values filterEven(const Values &input) {
  Values output;
  std::copy_if(input.begin(), input.end(), std::back_inserter(output),
               [](auto value) { return value % 2 == 0; });
  return output;
}

struct Fixture {
  std::shared_ptr<ResourceLedger> ledger = std::make_shared<ResourceLedger>();
  std::shared_ptr<HierarchicalScheduler> scheduler;
  std::unique_ptr<TransportManager> transport;

  explicit Fixture(std::uint64_t shared_limit) {
    ledger->registerTenant("columnar", {1.0, 8, 512});
    ledger->registerTenant("page", {2.0, 8, 512});
    scheduler = std::make_shared<HierarchicalScheduler>(ledger);
    transport = std::make_unique<TransportManager>(ledger, scheduler, 8, 8,
                                                   shared_limit);
  }

  Grant acquire(const TenantId &tenant, FragmentId fragment, BatchId batch,
                std::uint64_t reservation) {
    auto result = transport->acquire({tenant, fragment, batch, 1, reservation});
    require(result.status == AcquireStatus::Granted && result.grant.has_value(),
            "expected adapter request to acquire capacity");
    return *result.grant;
  }

  void checkSharedCharge() const {
    std::uint64_t sum = 0;
    for (const auto &entry : ledger->snapshot()) {
      const auto image = ledger->account(entry.first)->checkpoint();
      for (const auto &allocation : image.allocations)
        if (allocation.placement == Placement::Pushdown)
          sum += allocation.reserved + allocation.occupied;
    }
    require(sum == transport->pushdownMemoryInUse(),
            "shared device and tenant allocation charges diverged");
  }

  void checkDrained() const {
    checkSharedCharge();
    require(transport->pushdownMemoryInUse() == 0, "DPU bytes leaked");
    require(transport->slotsInUse(Placement::Pushdown) == 0, "DPU slot leaked");
    require(transport->slotsInUse(Placement::Pullup) == 0, "compute slot leaked");
    for (const auto &entry : ledger->snapshot()) {
      const auto &account = entry.second;
      require(account.occupied_bytes == 0 && account.reserved_bytes == 0 &&
                  account.credits_in_use == 0,
              "adapter allocation or credit leaked");
    }
  }
};

void sameAccount(const TenantSnapshot &before, const TenantSnapshot &after) {
  require(before.cpu_service_ms == after.cpu_service_ms &&
              before.network_bytes == after.network_bytes &&
              before.occupied_bytes == after.occupied_bytes &&
              before.reserved_bytes == after.reserved_bytes &&
              before.credits_in_use == after.credits_in_use,
          "failed operation changed tenant counters");
}

// The adapter assigns reader IDs; the core owns their lifetime and deduplication.
struct ColumnarOutput {
  Grant grant;
  Values values;
  bool readerDone(TransportManager &transport, unsigned reader) {
    return transport.releaseConsumer(grant.tenant, grant.allocation,
                                     grant.generation, reader + 1);
  }
};

void columnarSplitFanout() {
  Fixture fixture(128);
  auto &transport = *fixture.transport;
  transport.open({"columnar", 10, 1, true, true, false}, Placement::Pushdown);
  std::vector<ColumnarOutput> outputs;
  const std::vector<Values> splits{{1, 2, 3, 4, 5, 6, 7, 8},
                                   {9, 10, 11, 12, 13, 14, 15, 16}};
  for (std::size_t split = 0; split < splits.size(); ++split) {
    auto grant = fixture.acquire("columnar", 10, split + 1, 48);
    auto output = filterEven(splits[split]);
    require(transport.push(grant, bytes(output)), "columnar publication uncharged");
    require(transport.registerConsumer(grant, 1), "first reader registration failed");
    require(transport.registerConsumer(grant, 2), "second reader registration failed");
    require(transport.sealOutput(grant), "columnar seal failed");
    outputs.push_back({grant, std::move(output)});
    require(transport.finish("columnar", grant, 1.0, bytes(outputs.back().values)),
            "columnar producer completion rejected");
    fixture.checkSharedCharge();
  }
  const auto completed = fixture.ledger->account("columnar")->snapshot();
  require(completed.occupied_bytes == 64 && completed.reserved_bytes == 0 &&
              completed.credits_in_use == 2,
          "producer completion released live split output");
  require(transport.slotsInUse(Placement::Pushdown) == 0,
          "completed producers retained execution slots");

  Values merged;
  std::uint64_t sum = 0;
  for (auto &output : outputs) {
    merged.insert(merged.end(), output.values.begin(), output.values.end());
    sum += std::accumulate(output.values.begin(), output.values.end(), 0ULL);
    require(output.readerDone(transport, 0), "first reader event rejected");
    require(!output.readerDone(transport, 0), "duplicate reader counted twice");
  }
  require(merged == Values({2, 4, 6, 8, 10, 12, 14, 16}) && sum == 72,
          "split merge or shared-reader result incorrect");
  sameAccount(completed, fixture.ledger->account("columnar")->snapshot());
  require(transport.pushdownMemoryInUse() == 64,
          "first reader released a second reader's live bytes");
  for (auto &output : outputs) {
    require(output.readerDone(transport, 1), "second reader event rejected");
    require(!transport.onFinalConsumer("columnar", output.grant.allocation, 1),
            "duplicate final callback released another allocation");
    fixture.checkSharedCharge();
  }
  require(transport.close(10, 1), "columnar fragment close rejected");
  fixture.checkDrained();
}

bool changingActiveBackendFails(TransportManager &transport, FragmentId fragment) {
  try {
    transport.open({"page", fragment, 2, true, true, false}, Placement::Pullup);
  } catch (const std::invalid_argument &) {
    return true;
  }
  return false;
}

void pageRequestWithNativeContinuation() {
  Fixture fixture(128);
  auto &transport = *fixture.transport;
  const std::vector<Values> pages{{1, 2, 3, 4}, {5, 6, 7, 8}, {9, 10, 11, 12}};
  Values actual;
  Values input;
  std::set<std::size_t> published_pages;
  for (std::size_t page = 0; page < pages.size(); ++page) {
    input.insert(input.end(), pages[page].begin(), pages[page].end());
    const auto fragment = 100 + page;
    transport.open({"page", fragment, 1, true, true, false}, Placement::Pushdown);
    const auto storage = fixture.acquire("page", fragment, 1, bytes(pages[page]));
    // The fixture marks one page as requiring native visibility processing.
    // This is a bounded new compute operation, not migration of active state.
    const bool native_page = page == 1;
    const auto response = native_page ? pages[page] : filterEven(pages[page]);
    require(transport.push(storage, bytes(response)), "storage response uncharged");
    require(transport.finish("page", storage, 1.0, bytes(response)),
            "storage completion rejected");
    require(changingActiveBackendFails(transport, fragment),
            "retained response allowed active backend mutation");
    fixture.checkSharedCharge();

    const auto retry = transport.acquire({"page", fragment, 1, 1, bytes(pages[page])});
    require(retry.status == AcquireStatus::DuplicateBatch && !retry.grant,
            "duplicate storage response acquired another execution");
    Values output;
    if (native_page) {
      const auto host_fragment = 200 + page;
      transport.open({"page", host_fragment, 1, false, true, false}, Placement::Pullup);
      const auto host = fixture.acquire("page", host_fragment, 1, bytes(response));
      output = filterEven(response);
      require(transport.push(host, bytes(output)), "native output uncharged");
      require(transport.finish("page", host, 1.0, 0), "native completion rejected");
      require(transport.pushdownMemoryInUse() == bytes(response),
              "native grant changed storage response ownership");
      require(fixture.ledger->account("page")->snapshot().occupied_bytes ==
                  bytes(response) + bytes(output),
              "raw response and native result were not independently charged");
      require(transport.onFinalConsumer("page", storage.allocation, 1),
              "native reader did not release raw page");
      require(transport.onFinalConsumer("page", host.allocation, 1),
              "native result consumer release rejected");
      require(transport.close(host_fragment, 1), "native continuation close rejected");
    } else {
      output = response;
      require(transport.onFinalConsumer("page", storage.allocation, 1),
              "result reader did not release storage output");
    }
    require(published_pages.insert(page).second, "page result published twice");
    actual.insert(actual.end(), output.begin(), output.end());
    require(transport.close(fragment, 1), "storage request close rejected");
    fixture.checkSharedCharge();
  }
  require(actual == filterEven(input) && published_pages.size() == pages.size(),
          "mixed storage/native processing changed the query result");
  fixture.checkDrained();
}

void mixedFrontendsShareOneDevice() {
  Fixture fixture(80);
  auto &transport = *fixture.transport;
  transport.open({"columnar", 300, 1, true, true, false}, Placement::Pushdown);
  transport.open({"page", 301, 1, true, true, false}, Placement::Pushdown);
  const auto columnar = fixture.acquire("columnar", 300, 1, 48);
  const Values columnar_result{2, 4, 6, 8};
  require(transport.push(columnar, bytes(columnar_result)), "columnar result uncharged");
  require(transport.finish("columnar", columnar, 1.0, bytes(columnar_result)),
          "columnar completion rejected");
  const auto before = fixture.ledger->account("page")->snapshot();
  const auto blocked = transport.acquire({"page", 301, 1, 1, 64});
  require(blocked.status == AcquireStatus::WouldBlock && !blocked.grant,
          "individually feasible request exceeded shared memory");
  sameAccount(before, fixture.ledger->account("page")->snapshot());
  require(transport.pushdownMemoryInUse() == 32 &&
              transport.slotsInUse(Placement::Pushdown) == 0,
          "rejected page request changed shared counters or slots");
  fixture.checkSharedCharge();

  require(transport.onFinalConsumer("columnar", columnar.allocation, 1),
          "final columnar reader did not unblock device memory");
  const auto page = fixture.acquire("page", 301, 1, 64);
  const Values raw_page{1, 2, 3, 4};
  require(transport.push(page, bytes(raw_page)), "page response uncharged");
  require(transport.finish("page", page, 2.0, bytes(raw_page)),
          "page completion rejected after retry");
  require(transport.pushdownMemoryInUse() == bytes(raw_page),
          "retry failed to refund unused reservation");
  require(transport.onFinalConsumer("page", page.allocation, 1),
          "page reader release rejected");
  require(filterEven(raw_page) == Values({2, 4}), "page result incorrect after blocking");
  require(transport.close(300, 1) && transport.close(301, 1), "fragment close rejected");
  fixture.checkDrained();
}
} // namespace

int main() {
  try {
    columnarSplitFanout();
    pageRequestWithNativeContinuation();
    mixedFrontendsShareOneDevice();
    std::cout << "adapter-contract fixtures: 3 scenarios, " << checks
              << " checks passed (no native SQL engine integration claimed)\n";
    return 0;
  } catch (const std::exception &error) {
    std::cerr << "adapter-contract fixture failed: " << error.what() << '\n';
    return 1;
  }
}
