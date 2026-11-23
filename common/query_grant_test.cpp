/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "common/query_grant.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <limits>
#include <thread>
#include <vector>

#include "common/constants.hpp"
#include "common/memory_budget.hpp"

namespace tinylamb {

TEST(QueryGrantTest, PoolFloorsProtectGuaranteedMinimums) {
  ProcessMemoryBudget& b = ProcessMemoryBudget::Global();
  struct ResetGuard {
    ProcessMemoryBudget& budget;
    ~ResetGuard() { budget.ResetForTest(0); }
  } reset_guard{b};
  b.ResetForTest(/*limit_bytes=*/1000, /*storage_limit_bytes=*/0,
                 /*columnar_limit_bytes=*/0, /*check_rss_pressure=*/false);
  // M_t = 800. Floors: storage 200 (25%), query 200 (25%), tx 40 (5%).
  b.SetPoolFloorsForTest(200, 0, 200, 40);

  // Storage cannot consume more than M_t - (query_floor + tx_floor) = 560.
  EXPECT_FALSE(b.TryReserve(MemoryPoolKind::kStorage, 561));
  EXPECT_TRUE(b.TryReserve(MemoryPoolKind::kStorage, 560));

  // Query and Tx floors remain guaranteed!
  EXPECT_TRUE(b.TryReserve(MemoryPoolKind::kQuery, 200));
  EXPECT_TRUE(b.TryReserve(MemoryPoolKind::kTx, 40));
  EXPECT_EQ(b.TotalUsed(), 800U);

  b.Release(MemoryPoolKind::kStorage, 560);
  b.Release(MemoryPoolKind::kQuery, 200);
  b.Release(MemoryPoolKind::kTx, 40);
  b.ResetForTest(0);
}

TEST(QueryGrantTest, ReclaimHooksExecuteInOrderOnPressure) {
  ProcessMemoryBudget& b = ProcessMemoryBudget::Global();
  // ASSERT below must not strand the 800 bytes reserved on the global budget:
  // a later test would then start with an unexpectedly depleted allowance.
  struct ResetBudgetOnExit {
    ProcessMemoryBudget& budget;
    ~ResetBudgetOnExit() { budget.ResetForTest(0); }
  } reset{b};
  b.ResetForTest(/*limit_bytes=*/1000, /*storage_limit_bytes=*/0,
                 /*columnar_limit_bytes=*/0, /*check_rss_pressure=*/false);
  ASSERT_TRUE(b.TryReserve(MemoryPoolKind::kColumnar, 400));
  ASSERT_TRUE(b.TryReserve(MemoryPoolKind::kStorage, 400));

  std::vector<ReclaimStage> order;
  ScopedReclaimHook col_hook(ReclaimStage::kColumnar, [&](size_t need) {
    order.push_back(ReclaimStage::kColumnar);
    const size_t freed = std::min<size_t>(400, need);
    b.Release(MemoryPoolKind::kColumnar, freed);
    return freed;
  });
  ScopedReclaimHook st_hook(ReclaimStage::kCleanStorage, [&](size_t need) {
    order.push_back(ReclaimStage::kCleanStorage);
    const size_t freed = std::min<size_t>(400, need);
    b.Release(MemoryPoolKind::kStorage, freed);
    return freed;
  });

  // Need 600 bytes -> reclaims 400 from columnar first, then 200 from storage.
  Status st;
  EXPECT_TRUE(b.TryReserveWithReclaim(MemoryPoolKind::kQuery, 600, &st));
  ASSERT_EQ(order.size(), 2U);
  EXPECT_EQ(order[0], ReclaimStage::kColumnar);
  EXPECT_EQ(order[1], ReclaimStage::kCleanStorage);

  // Insufficient even after exhausting everything -> kMemoryLimitExceeded.
  EXPECT_FALSE(b.TryReserveWithReclaim(MemoryPoolKind::kQuery, 900, &st));
  EXPECT_EQ(st.GetCode(), StatusCode::kMemoryLimitExceeded);
  EXPECT_NE(st.GetMessage().find("query"), std::string::npos);

  col_hook.Reset();
  st_hook.Reset();
  b.Release(MemoryPoolKind::kStorage, b.StorageUsed());
  b.Release(MemoryPoolKind::kQuery, b.QueryUsed());
  b.ResetForTest(0);
}

TEST(QueryGrantTest, CoordinatorAdmitsWaitsWakesAndTimesOut) {
  ProcessMemoryBudget& b = ProcessMemoryBudget::Global();
  b.ResetForTest(/*limit_bytes=*/1000, /*storage_limit_bytes=*/0,
                 /*columnar_limit_bytes=*/0, /*check_rss_pressure=*/false);
  QueryMemoryCoordinator coord(&b);

  // Zero-preferred point query never blocks.
  auto point = coord.AcquireGrant(0);
  ASSERT_TRUE(point.HasValue());
  EXPECT_EQ(point.Value().GrantedBytes(), 0U);

  // Hold 600 bytes out of 800 soft cap.
  auto g1 = coord.AcquireGrant(600, 600, /*wait_timeout_ms=*/50);
  ASSERT_TRUE(g1.HasValue());
  EXPECT_EQ(g1.Value().GrantedBytes(), 600U);

  // Requesting 500 min when only 200 free must time out with
  // kMemoryLimitExceeded.
  auto g_timeout = coord.AcquireGrant(500, 500, /*wait_timeout_ms=*/30);
  EXPECT_FALSE(g_timeout.HasValue());
  EXPECT_EQ(g_timeout.GetStatus().GetCode(), StatusCode::kMemoryLimitExceeded);

  // Releasing g1 asynchronously unblocks g2 waiting for 500 bytes.
  std::thread releaser([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(15));
    g1.Value().ReleaseGrant();
  });
  // Bounded wait: uint64_t::max would hang past the 60s ctest limit if the
  // releaser thread never runs; 30s is ample headroom over the 15ms release.
  auto g2 = coord.AcquireGrant(500, 500, /*wait_timeout_ms=*/30000);
  releaser.join();
  ASSERT_TRUE(g2.HasValue());
  EXPECT_EQ(g2.Value().GrantedBytes(), 500U);

  g2.Value().ReleaseGrant();
  b.ResetForTest(0);
}

// P1842 (stress/TSan pin): ~QueryMemoryCoordinator must stay safe against
// an in-flight release listener holding the stale registration copy. The
// listener now captures a weak_ptr to a heap-owned condition_variable, so a
// late notify locks nothing instead of touching the freed object. With the
// old raw [this] capture TSan builds report the race/UAF on this loop; plain
// builds exercise it as a never-crash guard.
TEST(QueryGrantTest, CoordinatorDestroyedWhileReleaseListenersFire) {
  ProcessMemoryBudget& b = ProcessMemoryBudget::Global();
  b.ResetForTest(/*limit_bytes=*/1U << 20, /*storage_limit_bytes=*/0,
                 /*columnar_limit_bytes=*/0, /*check_rss_pressure=*/false);
  std::atomic<bool> stop{false};
  std::thread churn([&] {
    // Come and go repeatedly while registered on the shared budget: each
    // death unregisters, but a concurrent release may already hold a copy of
    // the previous listener view.
    while (!stop.load(std::memory_order_relaxed)) {
      QueryMemoryCoordinator coord(&b);
      (void)coord.AcquireGrant(0);
    }
  });
  // Join on every exit path: an ASSERT failure inside the loop returns early,
  // and destroying a joinable std::thread would std::terminate before gtest
  // can report the failure.
  const std::unique_ptr<std::thread, void (*)(std::thread*)> join_churn(
      &churn, [](std::thread* t) { t->join(); });
  QueryMemoryCoordinator keeper(&b);
  for (int i = 0; i < 20000; ++i) {
    auto grant = keeper.AcquireGrant(1024, 1024, /*wait_timeout_ms=*/5000);
    ASSERT_TRUE(grant.HasValue()) << "iteration " << i;
    grant.Value().ReleaseGrant();  // runs NotifyReleaseListeners()
  }
  stop.store(true, std::memory_order_relaxed);
  b.ResetForTest(0);
}

TEST(QueryGrantTest, LocalBatchCacheKeepsReservationsRepresentable) {
  QueryMemoryGrant grant;
  QueryMemoryGrant::LocalBatchCache cache(&grant);
  constexpr size_t max_bytes = std::numeric_limits<size_t>::max();
  ASSERT_TRUE(cache.Reserve(max_bytes - 1));
  cache.Release(1);
  ASSERT_TRUE(cache.Reserve(1));
  EXPECT_FALSE(cache.Reserve(2));
  EXPECT_EQ(cache.HeldBytes(), max_bytes - 1);
  cache.ReleaseAll();
  EXPECT_EQ(grant.UsedBytes(), 0U);
}

TEST(QueryGrantTest, DynamicSharingLocalBatchCacheAndEffectiveDop) {
  ProcessMemoryBudget& b = ProcessMemoryBudget::Global();
  b.ResetForTest(/*limit_bytes=*/10000000, /*storage_limit_bytes=*/0,
                 /*columnar_limit_bytes=*/0, /*check_rss_pressure=*/false);
  QueryMemoryCoordinator coord(&b);
  auto g_or = coord.AcquireGrant(size_t{4} << 20, size_t{1} << 20, 50);
  ASSERT_TRUE(g_or.HasValue());
  QueryMemoryGrant grant = g_or.MoveValue();

  {
    auto c1 = grant.TrackActiveConsumer();
    auto c2 = grant.TrackActiveConsumer();
    EXPECT_EQ(grant.ActiveConsumerCount(), 2U);
    EXPECT_EQ(grant.OperatorBudgetBytes(size_t{256} << 10), size_t{2} << 20);
  }
  EXPECT_EQ(grant.ActiveConsumerCount(), 0U);
  EXPECT_EQ(grant.OperatorBudgetBytes(size_t{256} << 10), size_t{4} << 20);

  // Local batch cache caches refills and returns unused excess.
  {
    QueryMemoryGrant::LocalBatchCache cache(&grant, size_t{64} << 10);
    EXPECT_TRUE(cache.Reserve(1024));
    EXPECT_EQ(grant.UsedBytes(), size_t{64} << 10);
    EXPECT_EQ(cache.HeldBytes(), 1024U);
    cache.Release(1024);
  }
  EXPECT_EQ(grant.UsedBytes(), 0U);

  // DOP scales down cleanly when B_op / W < B_min.
  EXPECT_EQ(QueryMemoryGrant::ComputeEffectiveDop(32, size_t{1} << 20,
                                                  size_t{256} << 10),
            4U);
  EXPECT_EQ(QueryMemoryGrant::ComputeEffectiveDop(8, size_t{64} << 10,
                                                  size_t{256} << 10),
            1U);

  grant.ReleaseGrant();
  b.ResetForTest(0);
}

}  // namespace tinylamb
