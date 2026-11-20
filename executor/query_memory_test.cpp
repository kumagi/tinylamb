/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/query_memory.hpp"

#include <atomic>
#include <cstdlib>
#include <thread>
// setenv/unsetenv below are POSIX APIs that <cstdlib> does not declare.
// NOLINTNEXTLINE(modernize-deprecated-headers)
#include <stdlib.h>

#include <string>
#include <utility>

#include "executor/detail/relation.hpp"
#include "gtest/gtest.h"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

class QueryMemoryTest : public ::testing::Test {
 protected:
  void TearDown() override { QueryMemoryBudget::Global().ResetForTest(0); }
};

TEST_F(QueryMemoryTest, QueryMemoryBudget_ReserveAndRelease_TracksUsage) {
  QueryMemoryBudget& budget = QueryMemoryBudget::Global();
  budget.ResetForTest(1000);
  EXPECT_FALSE(budget.Unlimited());
  EXPECT_EQ(budget.Limit(), 1000U);
  EXPECT_EQ(budget.Used(), 0U);
  EXPECT_TRUE(budget.CanReserve(100));
  budget.ReserveForced(100);
  EXPECT_EQ(budget.Used(), 100U);
  EXPECT_EQ(budget.Remaining(), 900U);
  budget.Release(40);
  EXPECT_EQ(budget.Used(), 60U);
}

TEST_F(QueryMemoryTest, CanReserve_PastSoftLimit_RejectsReservation) {
  QueryMemoryBudget& budget = QueryMemoryBudget::Global();
  budget.ResetForTest(1000);
  // Soft limit is 80% (800): reserving up to and including 800 is allowed,
  // but anything past the soft limit is rejected.
  EXPECT_TRUE(budget.CanReserve(700));
  EXPECT_TRUE(budget.CanReserve(800));
  EXPECT_FALSE(budget.CanReserve(801));
  budget.ReserveForced(800);
  EXPECT_FALSE(budget.CanReserve(1));
  budget.ResetForTest(0);
  EXPECT_TRUE(budget.Unlimited());
}

TEST_F(QueryMemoryTest,
     EstimateRowBytes_SampleRowAndValues_ReturnsExpectedEstimates) {
  Row row({Value(1), Value("hello")});
  EXPECT_GT(EstimateRowBytes(row), 0U);
  EXPECT_GE(EstimateValueBytes(Value("hello")), 5U);
  EXPECT_GT(EstimateValueBytes(Value(1)), 0U);
}

TEST_F(QueryMemoryTest, RowEstimateIncludesNestedArraysAndScalarPayloads) {
  const Value nested = Value::Array(
      {Value(1), Value(), Value::Array({Value("abc")}, "STRING")}, "JSON");
  const size_t nested_bytes = 32 + 4 + 40 + 32 + 32 + 6 + 35;
  EXPECT_EQ(EstimateValueBytes(nested), nested_bytes);
  const Row row({Value(), Value(1), Value(2.0), Value::DateFromDays(3),
                 Value("hello"), nested});
  EXPECT_EQ(EstimateRowBytes(row), 64 + 32 + 40 + 40 + 40 + 37 + nested_bytes);
}

TEST_F(QueryMemoryTest, Global_MultipleCalls_ReturnsSameInstance) {
  QueryMemoryBudget& a = QueryMemoryBudget::Global();
  QueryMemoryBudget& b = QueryMemoryBudget::Global();
  EXPECT_EQ(&a, &b);
  a.ResetForTest(0);
}

TEST_F(QueryMemoryTest, ResetForTest_WithEnvVar_MaintainsCorrectLimit) {
  const char* previous = std::getenv("TINYLAMB_QUERY_MEMORY_BYTES");
  const std::string saved = previous != nullptr ? previous : "";
  QueryMemoryBudget& budget = QueryMemoryBudget::Global();
  setenv("TINYLAMB_QUERY_MEMORY_BYTES", "424242", 1);
  budget.ResetForTest(1024);
  EXPECT_EQ(budget.Limit(), 1024U);
  setenv("TINYLAMB_QUERY_MEMORY_BYTES", "", 1);
  budget.ResetForTest(1024);
  EXPECT_EQ(budget.Limit(), 1024U);
  setenv("TINYLAMB_QUERY_MEMORY_BYTES", "0", 1);
  budget.ResetForTest(1024);
  EXPECT_EQ(budget.Limit(), 1024U);
  if (previous != nullptr) {
    setenv("TINYLAMB_QUERY_MEMORY_BYTES", saved.c_str(), 1);
  } else {
    unsetenv("TINYLAMB_QUERY_MEMORY_BYTES");
  }
  budget.ResetForTest(0);
}

TEST_F(QueryMemoryTest, Remaining_WhenUnlimited_ReturnsMaxSize) {
  QueryMemoryBudget& budget = QueryMemoryBudget::Global();
  budget.ResetForTest(0);
  EXPECT_TRUE(budget.Unlimited());
  EXPECT_EQ(budget.Remaining(), static_cast<size_t>(-1));
  EXPECT_TRUE(budget.CanReserve(1 << 20));
  budget.ReserveForced(100);
  EXPECT_TRUE(budget.Unlimited());
  EXPECT_EQ(budget.Used(), 100U);
  EXPECT_EQ(budget.Remaining(), static_cast<size_t>(-1));
}

TEST_F(QueryMemoryTest, Remaining_WhenExceedingLimit_ClampsAtZero) {
  QueryMemoryBudget& budget = QueryMemoryBudget::Global();
  budget.ResetForTest(100);
  EXPECT_EQ(budget.Remaining(), 100U);
  budget.ReserveForced(100);
  EXPECT_EQ(budget.Remaining(), 0U);
  budget.ReserveForced(50);
  EXPECT_EQ(budget.Used(), 150U);
  EXPECT_EQ(budget.Remaining(), 0U);
}

TEST_F(QueryMemoryTest, CanReserve_AtBoundaries_ReturnsExpectedBoolean) {
  QueryMemoryBudget& budget = QueryMemoryBudget::Global();
  budget.ResetForTest(1000);
  EXPECT_TRUE(budget.CanReserve(0));
  budget.ReserveForced(799);
  EXPECT_TRUE(budget.CanReserve(1));
  EXPECT_FALSE(budget.CanReserve(2));
  budget.ReserveForced(1);
  EXPECT_FALSE(budget.CanReserve(1));
  EXPECT_TRUE(budget.CanReserve(0));  // bytes==0 is always allowed
}

TEST_F(QueryMemoryTest,
     ReserveForcedAndRelease_VariousAmounts_ClampsUsageCorrectly) {
  QueryMemoryBudget& budget = QueryMemoryBudget::Global();
  budget.ResetForTest(1000);
  budget.ReserveForced(0);
  EXPECT_EQ(budget.Used(), 0U);
  budget.ReserveForced(250);
  budget.Release(0);
  EXPECT_EQ(budget.Used(), 250U);
  budget.Release(100);
  EXPECT_EQ(budget.Used(), 150U);
  budget.Release(1000);
  EXPECT_EQ(budget.Used(), 0U);
}

TEST_F(QueryMemoryTest, EstimateValueBytes_AllTypes_ReturnsExpectedByteSizes) {
  EXPECT_EQ(EstimateValueBytes(Value()), 32U);
  EXPECT_EQ(EstimateValueBytes(Value(1)), 40U);
  EXPECT_EQ(EstimateValueBytes(Value(2.5)), 40U);
  EXPECT_EQ(EstimateValueBytes(Value::Date("2024-01-01")), 40U);
  EXPECT_EQ(EstimateValueBytes(Value(std::string("hello"))), 37U);
  EXPECT_EQ(EstimateRowBytes(Row()), 64U);
  EXPECT_EQ(EstimateRowBytes(Row({Value(1), Value(std::string("hi"))})),
            64U + 40U + 34U);
}

TEST_F(QueryMemoryTest, QueryMemoryCharge_RaiiScope_AcquiresAndReleasesMemory) {
  QueryMemoryBudget& budget = QueryMemoryBudget::Global();
  budget.ResetForTest(0);
  EXPECT_EQ(budget.Used(), 0U);
  {
    QueryMemoryCharge charge(100);
    EXPECT_EQ(charge.Bytes(), 100U);
    EXPECT_EQ(budget.Used(), 100U);
    charge.Add(50);
    EXPECT_EQ(charge.Bytes(), 150U);
    EXPECT_EQ(budget.Used(), 150U);
    charge.Add(0);
    EXPECT_EQ(charge.Bytes(), 150U);
  }
  EXPECT_EQ(budget.Used(), 0U);
  QueryMemoryCharge zero(0);
  EXPECT_EQ(zero.Bytes(), 0U);
  EXPECT_EQ(budget.Used(), 0U);
  QueryMemoryCharge default_ctor;
  EXPECT_EQ(default_ctor.Bytes(), 0U);
}

TEST_F(QueryMemoryTest, QueryMemoryCharge_MoveSemantics_TransfersOwnership) {
  QueryMemoryBudget& budget = QueryMemoryBudget::Global();
  budget.ResetForTest(0);
  QueryMemoryCharge a(100);
  QueryMemoryCharge c(std::move(a));
  // The moved-from charge must report zero bytes; verifying that contract is
  // the point of this test (QueryMemoryCharge zeroes its source on move).
  // clang-format off
  // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
  EXPECT_EQ(a.Bytes(), 0U);
  // clang-format on
  EXPECT_EQ(c.Bytes(), 100U);
  EXPECT_EQ(budget.Used(), 100U);
  QueryMemoryCharge d(30);
  d = std::move(c);
  // clang-format off
  // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
  EXPECT_EQ(c.Bytes(), 0U);
  // clang-format on
  EXPECT_EQ(d.Bytes(), 100U);
  EXPECT_EQ(budget.Used(), 100U);
  // Self-move must be a no-op; go through a reference so the compiler cannot
  // prove the self-move and warn about it.
  QueryMemoryCharge& d_alias = d;
  d = std::move(d_alias);
  EXPECT_EQ(d.Bytes(), 100U);
  EXPECT_EQ(budget.Used(), 100U);
  d.ReleaseAll();
  EXPECT_EQ(d.Bytes(), 0U);
  EXPECT_EQ(budget.Used(), 0U);
  d.ReleaseAll();
  EXPECT_EQ(budget.Used(), 0U);
}

TEST_F(
    QueryMemoryTest,
    ResetForTest_PreservesProcessLinkageByDefaultAndSupportsExplicitIsolation) {
  ProcessMemoryBudget& proc = ProcessMemoryBudget::Global();
  QueryMemoryBudget& budget = QueryMemoryBudget::Global();
  proc.ResetForTest(/*limit_bytes=*/8192, /*storage_limit_bytes=*/0,
                    /*columnar_limit_bytes=*/0, /*check_rss_pressure=*/false);
  budget.ResetForTest(8192);

  ASSERT_TRUE(budget.TryReserve(512));
  EXPECT_EQ(budget.Used(), 512U);
  EXPECT_EQ(proc.QueryUsed(), 512U);
  budget.Release(256);
  EXPECT_EQ(proc.QueryUsed(), 256U);

  // Explicit isolation detaches from ProcessMemoryBudget and releases prior
  // usage cleanly.
  budget.ResetForTest(4096, /*isolate_from_process=*/true);
  EXPECT_EQ(proc.QueryUsed(), 0U);
  budget.ReserveForced(1024);
  EXPECT_EQ(budget.Used(), 1024U);
  EXPECT_EQ(proc.QueryUsed(), 0U);

  // Subsequent default ResetForTest() re-attaches linkage instead of leaving it
  // permanently disabled across subsequent tests.
  budget.ResetForTest(8192);
  ASSERT_TRUE(budget.TryReserve(512));
  EXPECT_EQ(proc.QueryUsed(), 512U);

  budget.ResetForTest(0);
  proc.ResetForTest(0);
}

TEST_F(QueryMemoryTest, TryReserveQueryMemory_IsAtomicUnderContention) {
  // T3724: two concurrent 500-byte charges against an 800-byte soft limit
  // must yield exactly one winner.  The old implementation checked
  // CanReserve() and then force-reserved inside the QueryMemoryCharge
  // constructor, so both threads could pass the pre-check and each push
  // used_ past the soft limit.
  auto& budget = QueryMemoryBudget::Global();
  budget.ResetForTest(1000, /*isolate_from_process=*/true);
  int total_winners = 0;
  for (int round = 0; round < 64; ++round) {
    std::atomic<int> winners{0};
    std::atomic<int> ready{0};
    std::atomic<int> attempted{0};
    std::atomic<bool> go{false};
    const auto worker = [&] {
      ready.fetch_add(1);
      while (!go.load()) {
        std::this_thread::yield();
      }
      auto charge = relational_detail::TryReserveQueryMemory(500);
      if (charge) {
        winners.fetch_add(1);
      }
      // Hold the charge until BOTH threads finished their attempt: only
      // then do the two reservations genuinely coexist, and an early
      // release would let the loser reserve into the freed headroom.
      attempted.fetch_add(1);
      while (attempted.load() < 2) {
        std::this_thread::yield();
      }
    };
    std::thread a(worker);
    std::thread b(worker);
    while (ready.load() < 2) {
      std::this_thread::yield();
    }
    go.store(true);
    a.join();
    b.join();
    total_winners += winners.load();
    // Every charge is RAII-released by the worker scope, so each round must
    // hand the budget back fully even when both threads raced to reserve.
    EXPECT_EQ(budget.Used(), 0U);
  }
  // Fixed code: exactly one winner per round.  The racy pre-check produced
  // two winners whenever the threads overlapped.
  EXPECT_EQ(total_winners, 64);
  budget.ResetForTest(0);
}

}  // namespace tinylamb
