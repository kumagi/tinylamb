/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "common/memory_budget.hpp"

#include <atomic>
#include <filesystem>
#include <thread>
#include <vector>

#if defined(__linux__) && defined(__GLIBC__)
#include <malloc.h>
#define TINYLAMB_MEMORY_TEST_CAN_MALLOPT 1
#endif

#include "common/incarnation.hpp"
#include "common/platform/process.hpp"
#include "gtest/gtest.h"

namespace tinylamb {

TEST(MemoryBudgetTest,
     DefaultColumnarBudgetRetainsLargeProjectionAndOtherFloors) {
  auto& budget = ProcessMemoryBudget::Global();
  for (const size_t limit : {size_t{1} << 30, size_t{16} << 30}) {
    budget.ResetForTest(limit, limit / 4, 0, false);
    budget.SetColumnarPoolLimitForTest(0);
    const size_t soft = budget.SoftLimit();
    const size_t storage_floor = soft / 4;
    const size_t query_floor = soft / 4;
    const size_t tx_floor = soft / 20;
    budget.SetPoolFloorsForTest(storage_floor, 0, query_floor, tx_floor);
    {
      // Account a projection, without allocating a multi-GiB test buffer.
      // The former M/8 cap rejected this although the other pools' floors
      // and the process-wide soft limit leave room for it.
      ProcessMemoryCharge columns(MemoryPoolKind::kColumnar);
      const size_t projection = (limit / 16) * 3;
      EXPECT_TRUE(columns.TryAdd(projection));
      ProcessMemoryCharge query(MemoryPoolKind::kQuery);
      ProcessMemoryCharge storage(MemoryPoolKind::kStorage);
      ProcessMemoryCharge tx(MemoryPoolKind::kTx);
      EXPECT_TRUE(query.TryAdd(query_floor));
      EXPECT_TRUE(storage.TryAdd(storage_floor));
      EXPECT_TRUE(tx.TryAdd(tx_floor));
      const size_t cap =
          budget.EffectivePoolMaxBytes(MemoryPoolKind::kColumnar);
      ASSERT_GE(cap, columns.Bytes());
      EXPECT_TRUE(columns.TryAdd(cap - columns.Bytes()));
      EXPECT_FALSE(columns.TryAdd(1));
      EXPECT_LE(budget.TotalUsed(), soft);
    }
    EXPECT_EQ(budget.TotalUsed(), 0U);
  }
  budget.ResetForTest(0);
}

TEST(MemoryBudgetTest, ColumnarReclaimEvictsLeastRecentlyUsedUnpinnedStore) {
  auto& budget = ProcessMemoryBudget::Global();
  budget.ResetForTest(0, 0, 300, false);
  {
    TableIncarnation oldest;
    TableIncarnation recently_used;
    TableIncarnation newest;
    auto store = [] {
      return std::make_shared<ProcessMemoryCharge>(MemoryPoolKind::kColumnar,
                                                   100);
    };
    oldest.SetLSTableStoreRaw(store());
    recently_used.SetProjectedLSTableStoreRaw(store());
    newest.SetLSTableStoreRaw(store());
    ASSERT_EQ(budget.ColumnarUsed(), 300U);
    // Hold the oldest cache entry through pressure: it cannot release bytes.
    auto pinned = oldest.GetLSTableStoreRaw();
    // Read a projected cache entry too; recency must apply to both forms.
    ASSERT_NE(recently_used.GetProjectedLSTableStoreRaw(), nullptr);
    ASSERT_TRUE(budget.TryReserveWithReclaim(MemoryPoolKind::kColumnar, 100));
    EXPECT_EQ(newest.GetLSTableStoreRaw(), nullptr);
    EXPECT_NE(recently_used.GetProjectedLSTableStoreRaw(), nullptr);
    EXPECT_EQ(oldest.GetLSTableStoreRaw(), pinned);
    budget.Release(MemoryPoolKind::kColumnar, 100);
    // Fill the freed slot and make the pinned entry oldest again.
    newest.SetLSTableStoreRaw(store());
    ASSERT_NE(recently_used.GetProjectedLSTableStoreRaw(), nullptr);
    ASSERT_TRUE(budget.TryReserveWithReclaim(MemoryPoolKind::kColumnar, 100));
    EXPECT_EQ(oldest.GetLSTableStoreRaw(), pinned);
    EXPECT_EQ(newest.GetLSTableStoreRaw(), nullptr);
    budget.Release(MemoryPoolKind::kColumnar, 100);
    pinned.reset();
  }
  EXPECT_EQ(budget.ColumnarUsed(), 0U);
  budget.ResetForTest(0);
}

TEST(MemoryBudgetTest, AtomicPoolReservationsRespectLimitsAndRelease) {
  ProcessMemoryBudget& b = ProcessMemoryBudget::Global();
  b.ResetForTest(/*limit_bytes=*/1000, /*storage_limit_bytes=*/400,
                 /*columnar_limit_bytes=*/200, /*check_rss_pressure=*/false);
  EXPECT_EQ(b.Limit(), 1000U);
  EXPECT_EQ(b.SoftLimit(), 800U);

  EXPECT_TRUE(b.TryReserve(MemoryPoolKind::kStorage, 400));
  EXPECT_FALSE(b.TryReserve(MemoryPoolKind::kStorage, 1));
  EXPECT_EQ(b.StorageUsed(), 400U);
  EXPECT_EQ(b.TotalUsed(), 400U);

  // Soft limit is 800 total, 400 already used -> query can reserve at most 400.
  EXPECT_FALSE(b.TryReserve(MemoryPoolKind::kQuery, 401));
  EXPECT_TRUE(b.TryReserve(MemoryPoolKind::kQuery, 400));
  EXPECT_EQ(b.TotalUsed(), 800U);

  b.Release(MemoryPoolKind::kStorage, 150);
  EXPECT_EQ(b.StorageUsed(), 250U);
  EXPECT_EQ(b.TotalUsed(), 650U);
  b.ResetForTest(0);
}

TEST(MemoryBudgetTest, ConcurrentTryReserveNeverExceedsSoftCap) {
  ProcessMemoryBudget& b = ProcessMemoryBudget::Global();
  b.ResetForTest(/*limit_bytes=*/1000, 0, 0, /*check_rss_pressure=*/false);
  std::atomic<size_t> successes{0};
  std::vector<std::jthread> workers;
  workers.reserve(16);
  for (int i = 0; i < 16; ++i) {
    workers.emplace_back([&] {
      for (int iter = 0; iter < 100; ++iter) {
        if (b.TryReserve(MemoryPoolKind::kQuery, 10)) {
          successes.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }
  workers.clear();
  EXPECT_EQ(successes.load() * 10, b.QueryUsed());
  EXPECT_LE(b.TotalUsed(), 800U);
  EXPECT_EQ(b.TotalUsed(), 800U);
  b.ResetForTest(0);
}

TEST(MemoryBudgetTest, ProcessRssReportingReturnsPositiveOnLinux) {
  EXPECT_GT(platform::CurrentProcessRssBytes(), 0U);
  EXPECT_GT(platform::PeakProcessRssBytes(), 0U);
}

TEST(MemoryBudgetTest, TempDirectoryRespectsRegisteredDefaultWhenEnvUnset) {
  if (std::getenv("TINYLAMB_TEMP") != nullptr) {
    GTEST_SKIP() << "TINYLAMB_TEMP explicitly set in caller environment";
  }
  // Unique per-process/per-run path: a fixed name in the shared temp dir is
  // not parallel-safe and a stale directory must never be removed.
  static std::atomic<uint64_t> unique_counter{0};
  const auto tmp =
      std::filesystem::temp_directory_path() /
      ("tl_budget_tmp_test_" + std::to_string(platform::GetPid()) + "_" +
       std::to_string(unique_counter.fetch_add(1, std::memory_order_relaxed)));
  // The default temp directory is process-global: restore it (and clean up
  // the path) on every exit path so a fatal assertion or exception cannot
  // leak the override into later tests.
  struct DefaultTempDirReset {
    ~DefaultTempDirReset() {
      ProcessMemoryBudget::SetDefaultTempDirectory({});
      std::error_code ec;
      std::filesystem::remove_all(path, ec);
    }
    std::filesystem::path path;
  } reset{tmp};
  ProcessMemoryBudget::SetDefaultTempDirectory(tmp);
  EXPECT_EQ(ProcessMemoryBudget::ResolveTempDirectory(), tmp);
}

TEST(MemoryBudgetTest, TracksPerPoolPeaksAndTxPool) {
  ProcessMemoryBudget& b = ProcessMemoryBudget::Global();
  b.ResetForTest(/*limit_bytes=*/2000, /*storage_limit_bytes=*/1000,
                 /*columnar_limit_bytes=*/500, /*check_rss_pressure=*/false);
  b.ResetPeaks();

  EXPECT_TRUE(b.TryReserve(MemoryPoolKind::kStorage, 300));
  EXPECT_TRUE(b.TryReserve(MemoryPoolKind::kColumnar, 200));
  EXPECT_TRUE(b.TryReserve(MemoryPoolKind::kQuery, 250));
  EXPECT_TRUE(b.TryReserve(MemoryPoolKind::kTx, 150));
  EXPECT_EQ(b.TxUsed(), 150U);
  EXPECT_EQ(b.TotalUsed(), 900U);

  b.Release(MemoryPoolKind::kTx, 100);
  b.Release(MemoryPoolKind::kQuery, 200);
  EXPECT_EQ(b.TxPeak(), 150U);
  EXPECT_EQ(b.QueryPeak(), 250U);
  EXPECT_EQ(b.StoragePeak(), 300U);
  EXPECT_EQ(b.ColumnarPeak(), 200U);
  EXPECT_EQ(b.TotalPeak(), 900U);
  // RSS bookkeeping must stay meaningful: the process is resident (a 0
  // reading means sampling silently broke) and "unaccounted" RSS is
  // rss - tracked clamped at zero, so it can never exceed the host's
  // physical memory (an underflow would produce a huge value here).
  EXPECT_GT(platform::CurrentProcessRssBytes(), 0U);
  EXPECT_LE(b.CurrentUnaccountedRssBytes(), platform::PhysicalMemoryBytes());
  b.ResetForTest(0);
}

TEST(MemoryBudgetTest, FaultInjectionAndQueryTelemetryScope) {
  ProcessMemoryBudget& b = ProcessMemoryBudget::Global();
  b.ResetForTest(/*limit_bytes=*/4096, 0, 0, /*check_rss_pressure=*/false);
  b.ResetTestKnobs();

  b.SetTestMemoryFailCountdown(2);
  EXPECT_TRUE(b.TryReserve(MemoryPoolKind::kQuery, 64));
  EXPECT_FALSE(
      b.TryReserve(MemoryPoolKind::kQuery, 64));  // countdown == 1 -> fail
  EXPECT_TRUE(b.TryReserve(MemoryPoolKind::kQuery, 64));  // recovered

  b.SetTestSpillIoFailCountdown(1);
  EXPECT_TRUE(b.ConsumeInjectedSpillIoFailure());
  EXPECT_FALSE(b.ConsumeInjectedSpillIoFailure());

  b.SetTestOpMemoryBytes(size_t{1} << 20);
  b.SetTestAlwaysSpill(true);
  b.SetTestTempLimitBytes(size_t{64} << 20);
  EXPECT_EQ(b.OpMemoryOverrideBytes(), size_t{1} << 20);
  EXPECT_TRUE(b.AlwaysSpillEnabled());
  EXPECT_EQ(b.TempLimitBytes(), size_t{64} << 20);

  OocQueryMetrics qm;
  {
    OocQueryScope scope(&qm);
    b.RecordOperatorPeakBytes(4096);
    b.RecordSpillBytes(8192);
    b.RecordPartitionDepth(3);
    b.RecordSkewFallback();
    b.RecordReclaim(1024);
  }
  const OocTelemetrySnapshot snap = qm.Snapshot();
  EXPECT_EQ(snap.op_peak_bytes, 4096U);
  EXPECT_EQ(snap.spill_bytes, 8192U);
  EXPECT_EQ(snap.spill_ops, 1U);
  EXPECT_EQ(snap.max_partition_depth, 3U);
  EXPECT_EQ(snap.skew_fallbacks, 1U);
  EXPECT_EQ(snap.reclaim_calls, 1U);
  EXPECT_EQ(snap.reclaim_bytes, 1024U);

  // Per-query metrics must keep receiving updates even after the global
  // telemetry peak is already higher (the per-row hot path skips the global
  // CAS unless a new global peak is reached).
  OocQueryMetrics qm1;
  OocQueryMetrics qm2;
  b.RecordOperatorPeakBytes(1 << 20);
  {
    OocQueryScope s1(&qm1);
    b.RecordOperatorPeakBytes(4096);
  }
  {
    OocQueryScope s2(&qm2);
    b.RecordOperatorPeakBytes(8192);
  }
  EXPECT_EQ(qm1.Snapshot().op_peak_bytes, 4096U);
  EXPECT_EQ(qm2.Snapshot().op_peak_bytes, 8192U);
  EXPECT_GE(b.TelemetrySnapshot().op_peak_bytes, size_t{1} << 20);

  b.ResetTestKnobs();
  b.ResetForTest(0);
}

TEST(MemoryBudgetTest, OverReleaseClampsToPoolWithoutCorruptingTotalUsed) {
  ProcessMemoryBudget& b = ProcessMemoryBudget::Global();
  b.ResetForTest(/*limit_bytes=*/4096, /*storage_limit_bytes=*/0,
                 /*columnar_limit_bytes=*/0, /*check_rss_pressure=*/false);
  ASSERT_TRUE(b.TryReserve(MemoryPoolKind::kStorage, 1000));
  ASSERT_TRUE(b.TryReserve(MemoryPoolKind::kQuery, 200));
  EXPECT_EQ(b.TotalUsed(), 1200U);

  // Releasing more than kQuery currently holds must clamp to kQuery's 200 and
  // leave kStorage's 1000 intact in TotalUsed().
  b.Release(MemoryPoolKind::kQuery, 900);
  EXPECT_EQ(b.QueryUsed(), 0U);
  EXPECT_EQ(b.StorageUsed(), 1000U);
  EXPECT_EQ(b.TotalUsed(), 1000U);

  b.Release(MemoryPoolKind::kQuery, 100);
  EXPECT_EQ(b.TotalUsed(), 1000U);

  b.ResetForTest(0);
}

// #982: the RSS gate counts heap the allocator retains after free().  A
// TryReserveWithReclaim that is refused only by that gate must purge the
// retained heap (ReleaseFreeHeapToOs) and re-sample before giving up, or the
// freed-but-resident pages starve every later grant forever (TPC-H system
// allocator: Q9 degradation, then all later queries time out).
TEST(MemoryBudgetTest, RssGatedReclaimPurgesRetainedHeapBeforeRefusing) {
  ProcessMemoryBudget& b = ProcessMemoryBudget::Global();
  const uint64_t base = platform::CurrentProcessRssBytes();
  if (base == 0) {
    GTEST_SKIP() << "no process RSS sampling on this platform";
  }
#ifdef TINYLAMB_MEMORY_TEST_CAN_MALLOPT
  const int saved_mmap = mallopt(M_MMAP_THRESHOLD, 1 << 30);
  const int saved_trim = mallopt(M_TRIM_THRESHOLD, 1 << 30);
#endif
  auto restore_mallopt = [&] {
#ifdef TINYLAMB_MEMORY_TEST_CAN_MALLOPT
    mallopt(M_MMAP_THRESHOLD, saved_mmap);
    mallopt(M_TRIM_THRESHOLD, saved_trim);
#endif
  };
  constexpr size_t kChunk = size_t{384} << 20;
  std::vector<char> retained(kChunk);
  for (size_t i = 0; i < kChunk; i += 4096) {
    retained[i] = 2;
  }
  retained.clear();
  retained.shrink_to_fit();
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  const uint64_t rss_retained = platform::CurrentProcessRssBytes();
  if (rss_retained < base + (size_t{256} << 20)) {
    restore_mallopt();
    GTEST_SKIP() << "allocator returned the pages on free; nothing to purge";
  }
  // Place the RSS cap strictly between the retained-heap reading and the
  // (approximately base) reading a successful purge produces.
  const uint64_t gate = (rss_retained + base) / 2;
  b.ResetForTest(static_cast<size_t>((gate * 8 / 7) + 8), 0, 0,
                 /*check_rss_pressure=*/true);
  const bool refused_before =
      !b.CanReserve(MemoryPoolKind::kQuery, size_t{64} << 10);
  std::this_thread::sleep_for(std::chrono::milliseconds(25));
  Status status;
  const bool reclaimed = b.TryReserveWithReclaim(MemoryPoolKind::kQuery,
                                                 size_t{64} << 10, &status);
  if (reclaimed) {
    b.Release(MemoryPoolKind::kQuery, size_t{64} << 10);
    // The gate was closed moments ago (refused_before), so the reservation
    // could only pass if the reclaim path actually purged the retained heap
    // and the forced re-sample landed below the gate.  A grant admitted on a
    // stale/low RSS sample without the purge would leave RSS at the
    // retained level, which sits >128 MiB above the gate; allow half that
    // as noise margin.
    const uint64_t rss_after = platform::CurrentProcessRssBytes();
    EXPECT_LT(rss_after, gate + (size_t{64} << 20))
        << "TryReserveWithReclaim succeeded without lowering RSS below the "
           "gate: the retained-heap purge never ran (#982)";
  }
  b.ResetForTest(0);
  if (!refused_before) {
    restore_mallopt();
    GTEST_SKIP() << "RSS gate never closed (noise in RSS readings)";
  }
  if (!reclaimed) {
    // Diagnostic: if a purge could have opened the gate but the reclaim path
    // did not run one, this is the #982 starvation itself, not environment
    // noise.  Only a purge that fails to lower RSS at all is an
    // unsupported-allocator situation.
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
    const bool manual_purge_ran = platform::ReleaseFreeHeapToOs();
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    const uint64_t rss_after_manual_purge = platform::CurrentProcessRssBytes();
    restore_mallopt();
    if (manual_purge_ran && rss_after_manual_purge < gate) {
      GTEST_FAIL() << "TryReserveWithReclaim refused a grant that a heap purge "
                      "would have admitted (#982 retained-heap starvation): "
                   << status.GetMessage();
    }
    GTEST_SKIP() << "purge left RSS above the cap on this allocator: "
                 << status.GetMessage();
  }
  restore_mallopt();
}

// #1054: Release() is the OOC free hot path; the notification must not take
// hooks_mu_ or allocate, and a lost try_lock must not silently drop a wake-up
// (a dropped notification costs every grant waiter a full poll slice).
TEST(MemoryBudgetTest, ReleaseNotifiesEveryListenerWithoutDrops) {
  ProcessMemoryBudget& b = ProcessMemoryBudget::Global();
  b.ResetForTest(0);
  std::atomic<uint64_t> hits1{0};
  std::atomic<uint64_t> hits2{0};
  std::atomic<uint64_t> hits3{0};
  const uint64_t id1 = b.RegisterReleaseListener(
      [&] { hits1.fetch_add(1, std::memory_order_relaxed); });
  const uint64_t id2 = b.RegisterReleaseListener(
      [&] { hits2.fetch_add(1, std::memory_order_relaxed); });
  constexpr int kThreads = 8;
  constexpr int kReleases = 500;
  std::vector<std::thread> workers;
  workers.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    workers.emplace_back([&] {
      for (int i = 0; i < kReleases; ++i) {
        b.Release(MemoryPoolKind::kQuery, 1);
      }
    });
  }
  for (std::thread& w : workers) {
    w.join();
  }
  EXPECT_EQ(hits1.load(), static_cast<uint64_t>(kThreads * kReleases));
  EXPECT_EQ(hits2.load(), static_cast<uint64_t>(kThreads * kReleases));

  b.UnregisterReleaseListener(id2);
  b.Release(MemoryPoolKind::kQuery, 1);
  EXPECT_EQ(hits1.load(), static_cast<uint64_t>(kThreads * kReleases) + 1);
  EXPECT_EQ(hits2.load(), static_cast<uint64_t>(kThreads * kReleases));

  // A throwing listener must not suppress the listeners registered after it.
  const uint64_t id_throw = b.RegisterReleaseListener([] { throw 1; });
  const uint64_t id3 = b.RegisterReleaseListener(
      [&] { hits3.fetch_add(1, std::memory_order_relaxed); });
  b.Release(MemoryPoolKind::kQuery, 1);
  EXPECT_EQ(hits3.load(), 1U);

  b.UnregisterReleaseListener(id1);
  b.UnregisterReleaseListener(id_throw);
  b.UnregisterReleaseListener(id3);
  b.Release(MemoryPoolKind::kQuery, 1);
  EXPECT_EQ(hits1.load(), static_cast<uint64_t>(kThreads * kReleases) + 2);
  EXPECT_EQ(hits3.load(), 1U);
  b.ResetForTest(0);
}

}  // namespace tinylamb
