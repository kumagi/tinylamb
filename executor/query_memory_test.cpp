/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/query_memory.hpp"

#include <cstdlib>
// setenv/unsetenv below are POSIX APIs that <cstdlib> does not declare.
// NOLINTNEXTLINE(modernize-deprecated-headers)
#include <stdlib.h>
#include <string>
#include <utility>

#include "gtest/gtest.h"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

TEST(QueryMemoryTest, QueryMemoryBudget_ReserveAndRelease_TracksUsage) {
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

TEST(QueryMemoryTest, CanReserve_PastSoftLimit_RejectsReservation) {
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

TEST(QueryMemoryTest, EstimateRowBytes_SampleRowAndValues_ReturnsExpectedEstimates) {
  Row row({Value(1), Value("hello")});
  EXPECT_GT(EstimateRowBytes(row), 0U);
  EXPECT_GE(EstimateValueBytes(Value("hello")), 5U);
  EXPECT_GE(EstimateValueBytes(Value(1)), 0U);
}

TEST(QueryMemoryTest, Global_MultipleCalls_ReturnsSameInstance) {
  QueryMemoryBudget& a = QueryMemoryBudget::Global();
  QueryMemoryBudget& b = QueryMemoryBudget::Global();
  EXPECT_EQ(&a, &b);
  a.ResetForTest(0);
}

TEST(QueryMemoryTest, ResetForTest_WithEnvVar_MaintainsCorrectLimit) {
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
  unsetenv("TINYLAMB_QUERY_MEMORY_BYTES");
  budget.ResetForTest(0);
}

TEST(QueryMemoryTest, Remaining_WhenUnlimited_ReturnsMaxSize) {
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

TEST(QueryMemoryTest, Remaining_WhenExceedingLimit_ClampsAtZero) {
  QueryMemoryBudget& budget = QueryMemoryBudget::Global();
  budget.ResetForTest(100);
  EXPECT_EQ(budget.Remaining(), 100U);
  budget.ReserveForced(100);
  EXPECT_EQ(budget.Remaining(), 0U);
  budget.ReserveForced(50);
  EXPECT_EQ(budget.Used(), 150U);
  EXPECT_EQ(budget.Remaining(), 0U);
}

TEST(QueryMemoryTest, CanReserve_AtBoundaries_ReturnsExpectedBoolean) {
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

TEST(QueryMemoryTest, ReserveForcedAndRelease_VariousAmounts_ClampsUsageCorrectly) {
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

TEST(QueryMemoryTest, EstimateValueBytes_AllTypes_ReturnsExpectedByteSizes) {
  EXPECT_EQ(EstimateValueBytes(Value()), 32U);
  EXPECT_EQ(EstimateValueBytes(Value(1)), 40U);
  EXPECT_EQ(EstimateValueBytes(Value(2.5)), 40U);
  EXPECT_EQ(EstimateValueBytes(Value::Date("2024-01-01")), 40U);
  EXPECT_EQ(EstimateValueBytes(Value(std::string("hello"))), 37U);
  EXPECT_EQ(EstimateRowBytes(Row()), 64U);
  EXPECT_EQ(EstimateRowBytes(Row({Value(1), Value(std::string("hi"))})),
            64U + 40U + 34U);
}

TEST(QueryMemoryTest, QueryMemoryCharge_RaiiScope_AcquiresAndReleasesMemory) {
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

TEST(QueryMemoryTest, QueryMemoryCharge_MoveSemantics_TransfersOwnership) {
  QueryMemoryBudget& budget = QueryMemoryBudget::Global();
  budget.ResetForTest(0);
  QueryMemoryCharge a(100);
  QueryMemoryCharge c(std::move(a));
  // The moved-from charge must report zero bytes; verifying that contract is
  // the point of this test (QueryMemoryCharge zeroes its source on move).
  EXPECT_EQ(a.Bytes(), 0U);  // NOLINT(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
  EXPECT_EQ(c.Bytes(), 100U);
  EXPECT_EQ(budget.Used(), 100U);
  QueryMemoryCharge d(30);
  d = std::move(c);
  EXPECT_EQ(c.Bytes(), 0U);  // NOLINT(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
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

}  // namespace tinylamb
