#include "tide/tide.hpp"

#include <cassert>
#include <cstdio>
#include <fstream>
#include <memory>
#include <sys/wait.h>
#include <unistd.h>

using namespace tide;

int main() {
  char path[] = "/tmp/tide-recovery-XXXXXX";
  const int fd = mkstemp(path);
  assert(fd >= 0);
  close(fd);

  const pid_t child = fork();
  assert(child >= 0);
  if (child == 0) {
    TenantAccount account(TenantPolicy{1.0, 2, 100});
    const auto id = account.reserve(80, 7);
    if (!id || !account.materialize(*id, 7, 70) ||
        !account.finish(*id, 7, 1.25, 128)) _exit(2);
    const auto saved = account.checkpoint();
    std::ofstream out(path, std::ios::trunc);
    out << saved.policy.weight << ' ' << saved.policy.batch_limit << ' '
        << saved.policy.memory_limit_bytes << ' ' << saved.cpu_service_ms << ' '
        << saved.network_bytes << ' ' << saved.next_allocation << ' '
        << saved.allocations.size() << '\n';
    for (const auto &a : saved.allocations)
      out << a.id << ' ' << a.generation << ' ' << a.reserved << ' '
          << a.occupied << ' ' << a.finished << '\n';
    out.flush();
    if (!out) _exit(3);
    out.close();
    _exit(77);  // abrupt control-process termination
  }

  int status = 0;
  assert(waitpid(child, &status, 0) == child);
  assert(WIFEXITED(status) && WEXITSTATUS(status) == 77);
  TenantCheckpoint saved;
  std::size_t count = 0;
  std::ifstream in(path);
  assert(in >> saved.policy.weight >> saved.policy.batch_limit >>
         saved.policy.memory_limit_bytes >> saved.cpu_service_ms >>
         saved.network_bytes >> saved.next_allocation >> count);
  for (std::size_t i = 0; i < count; ++i) {
    AllocationCheckpoint a;
    assert(in >> a.id >> a.generation >> a.reserved >> a.occupied >> a.finished);
    saved.allocations.push_back(a);
  }
  std::remove(path);

  auto ledger = std::make_shared<ResourceLedger>();
  ledger->restoreTenant("A", saved);
  auto account = ledger->account("A");
  assert(account->snapshot().occupied_bytes == 70);
  assert(account->snapshot().reserved_bytes == 0);
  assert(!account->reserve(40, 8));
  assert(!account->release(saved.allocations.front().id, 6));
  assert(account->release(saved.allocations.front().id, 7));
  assert(!account->release(saved.allocations.front().id, 7));
  const auto next = account->reserve(40, 8);
  assert(next && *next >= saved.next_allocation);
  assert(account->finish(*next, 8, 0, 0));
  assert(account->release(*next, 8));
  assert(account->snapshot().credits_in_use == 0);
}
