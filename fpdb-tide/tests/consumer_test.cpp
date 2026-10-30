#include "tide/tide.hpp"
#include <algorithm>
#include <array>
#include <atomic>
#include <iostream>
#include <stdexcept>
#include <thread>
using namespace tide;
void check(bool v){if(!v)throw std::runtime_error("consumer lifetime check failed");}
struct Runtime {
 std::shared_ptr<ResourceLedger> ledger=std::make_shared<ResourceLedger>();
 std::shared_ptr<HierarchicalScheduler> scheduler;
 std::unique_ptr<TransportManager> tm;
 Runtime(){ledger->registerTenant("a",{1,2,100});scheduler=std::make_shared<HierarchicalScheduler>(ledger);
 tm=std::make_unique<TransportManager>(ledger,scheduler,2,2,100);
 tm->open({"a",1,1,true,true,false},Placement::Pushdown);}
 Grant grant(){auto r=tm->acquire({"a",1,1,1,100});check(r.grant.has_value());check(tm->push(*r.grant,80));return *r.grant;}
};
int main(){
 // Every ordering of sealing, production completion and both reader callbacks.
 std::array<int,4> events{0,1,2,3};int orders=0;
 do{
  Runtime r;auto g=r.grant();auto &t=*r.tm;
  check(t.registerConsumer(g,11));check(t.registerConsumer(g,12));
  check(!t.registerConsumer(g,11));check(!t.releaseConsumer("a",g.allocation,2,11));
  check(!t.releaseConsumer("a",g.allocation,1,99));
  int n=0;
  for(int e:events){
   if(e==0)check(t.sealOutput(g));
   if(e==1)check(t.finish("a",g,1,0));
   if(e==2)check(t.releaseConsumer("a",g.allocation,1,11));
   if(e==3)check(t.releaseConsumer("a",g.allocation,1,12));
   ++n;
   if(n<4){check(r.ledger->account("a")->snapshot().occupied_bytes==80);
    check(!t.release("a",g.allocation,1));}
  }
  check(t.pushdownMemoryInUse()==0);check(t.slotsInUse(Placement::Pushdown)==0);
  check(r.ledger->account("a")->snapshot().credits_in_use==0);
  check(!t.releaseConsumer("a",g.allocation,1,11));++orders;
 }while(std::next_permutation(events.begin(),events.end()));
 // Released IDs cannot be re-registered, and sealed output cannot grow.
 {Runtime r;auto g=r.grant();auto&t=*r.tm;
 check(t.registerConsumer(g,5));check(t.releaseConsumer("a",g.allocation,1,5));
 check(!t.registerConsumer(g,5));check(t.registerConsumer(g,6));
 check(t.sealOutput(g));check(!t.extend(g,1));check(!t.push(g,1));check(!t.registerConsumer(g,7));
 check(t.onFinalConsumer("a",g.allocation,1)); // Cannot bypass the registered reader.
 check(t.finish("a",g,1,0));check(t.pushdownMemoryInUse()==80);
 check(t.releaseConsumer("a",g.allocation,1,6));check(t.pushdownMemoryInUse()==0);}
 // Empty publication set still waits for producer completion.
 {Runtime r;auto g=r.grant();check(r.tm->sealOutput(g));check(r.tm->pushdownMemoryInUse()==100);
 check(r.tm->finish("a",g,1,0));check(r.tm->pushdownMemoryInUse()==0);}
 // Recovery retains live reader and released-ID tombstone. Also allow sealing
 // a completed allocation after restore (crash occurred before seal callback).
 for(bool sealed:{false,true}){
 Runtime r;auto g=r.grant();check(r.tm->registerConsumer(g,11));check(r.tm->registerConsumer(g,12));
 check(r.tm->releaseConsumer("a",g.allocation,1,11));if(sealed)check(r.tm->sealOutput(g));
 check(r.tm->finish("a",g,1,0));auto image=r.ledger->account("a")->checkpoint();
 auto restored=std::make_shared<ResourceLedger>();restored->restoreTenant("a",image);
 auto sched=std::make_shared<HierarchicalScheduler>(restored);TransportManager t(restored,sched,2,2,100);
 check(t.pushdownMemoryInUse()==80);check(!t.release("a",g.allocation,1));
 check(!t.releaseConsumer("a",g.allocation,1,11));
 if(!sealed)check(t.sealOutput("a",g.allocation,1));
 check(t.releaseConsumer("a",g.allocation,1,12));check(t.pushdownMemoryInUse()==0);
 check(restored->account("a")->snapshot().credits_in_use==0);
 }
 // Snapshot after ACK persistence but before transport collection: duplicate
 // replay must collect the already-drained record without another decrement.
 {TenantAccount a(TenantPolicy{1,2,100});auto id=*a.reserve(50,1);check(a.materialize(id,1,50));
 check(a.registerConsumer(id,1,3));check(a.sealOutput(id,1));check(a.finish(id,1,0,0));
 check(a.releaseConsumer(id,1,3));auto l=std::make_shared<ResourceLedger>();l->restoreTenant("a",a.checkpoint());
 auto sched=std::make_shared<HierarchicalScheduler>(l);TransportManager t(l,sched,1,1,100);
 check(t.pushdownMemoryInUse()==50);check(!t.releaseConsumer("a",id,1,3));
 check(t.pushdownMemoryInUse()==0);check(l->account("a")->snapshot().credits_in_use==0);}
 // Duplicate readers and invalid mode must be rejected on recovery.
 {Runtime r;auto g=r.grant();check(r.tm->registerConsumer(g,1));auto c=r.ledger->account("a")->checkpoint();
 c.allocations[0].consumers.push_back({1,false});bool rejected=false;
 try{TenantAccount bad(c);}catch(const std::invalid_argument&){rejected=true;}check(rejected);}
 // Race the final reader with completion; shared memory returns exactly once.
 for(int i=0;i<100;++i){Runtime r;auto g=r.grant();check(r.tm->registerConsumer(g,1));check(r.tm->sealOutput(g));
 std::thread a([&]{check(r.tm->finish("a",g,1,0));});
 std::thread b([&]{check(r.tm->releaseConsumer("a",g.allocation,1,1));});a.join();b.join();
 check(r.tm->pushdownMemoryInUse()==0);check(r.tm->slotsInUse(Placement::Pushdown)==0);}
 std::cout<<orders<<" event permutations, restore, tombstone, sealing and 100 completion races passed\n";
}
