/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/query_scheduler.hpp"

#include <chrono>
#include <future>
#include <memory>
#include <sstream>
#include <utility>
#include <string>
#include <vector>

#include "executor/constant_executor.hpp"
#include "executor/executor_base.hpp"
#include "executor/data_chunk.hpp"
#include "gtest/gtest.h"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

TEST(QuerySchedulerTest, EnforcesCpuAndMemoryBudgets) {
  QueryScheduler scheduler(2, 100);
  auto first = scheduler.Acquire(2, 80);
  EXPECT_EQ(scheduler.UsedCpuSlots(), 2U);
  EXPECT_EQ(scheduler.UsedMemoryBytes(), 80U);

  std::promise<void> acquired;
  std::future<void> acquired_future = acquired.get_future();
  std::future<void> waiter = std::async(std::launch::async, [&] {
    auto second = scheduler.Acquire(1, 30);
    acquired.set_value();
  });
  EXPECT_EQ(acquired_future.wait_for(std::chrono::milliseconds(20)),
            std::future_status::timeout);
  first.Release();
  EXPECT_EQ(acquired_future.wait_for(std::chrono::seconds(1)),
            std::future_status::ready);
  waiter.get();
  EXPECT_EQ(scheduler.UsedCpuSlots(), 0U);
  EXPECT_EQ(scheduler.UsedMemoryBytes(), 0U);
}

TEST(QuerySchedulerTest, ScheduledExecutorReleasesLeaseAtEndOfQuery) {
  QueryScheduler scheduler(1, 128);
  Executor values = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(1)}), Row({Value(2)})});
  ScheduledExecutor executor(std::move(values), scheduler, 1, 64);

  Row row;
  ASSERT_TRUE(executor.Next(&row, nullptr));
  EXPECT_EQ(scheduler.UsedCpuSlots(), 1U);
  EXPECT_EQ(scheduler.UsedMemoryBytes(), 64U);
  ASSERT_TRUE(executor.Next(&row, nullptr));
  ASSERT_FALSE(executor.Next(&row, nullptr));
  EXPECT_EQ(scheduler.UsedCpuSlots(), 0U);
  EXPECT_EQ(scheduler.UsedMemoryBytes(), 0U);
}

TEST(QuerySchedulerTest, ZeroCapacityIsClampedToOne) {
  QueryScheduler scheduler(0, 0);
  EXPECT_EQ(scheduler.CpuCapacity(), 1U);
  EXPECT_EQ(scheduler.MemoryCapacity(), 1U);
}

TEST(QuerySchedulerTest, AcquireClampsRequestsToCapacity) {
  QueryScheduler scheduler(2, 100);
  auto lease = scheduler.Acquire(100, 1000);
  EXPECT_EQ(scheduler.CpuCapacity(), 2U);
  EXPECT_EQ(scheduler.MemoryCapacity(), 100U);
  EXPECT_EQ(scheduler.UsedCpuSlots(), 2U);
  EXPECT_EQ(scheduler.UsedMemoryBytes(), 100U);
  lease.Release();
  EXPECT_EQ(scheduler.UsedCpuSlots(), 0U);
  EXPECT_EQ(scheduler.UsedMemoryBytes(), 0U);
}

TEST(QuerySchedulerTest, LeaseMoveAssignmentReleasesPreviousLease) {
  QueryScheduler scheduler(2, 100);
  auto first = scheduler.Acquire(1, 30);
  auto second = scheduler.Acquire(1, 30);
  EXPECT_EQ(scheduler.UsedCpuSlots(), 2U);
  first = std::move(second);
  EXPECT_EQ(scheduler.UsedCpuSlots(), 1U);
  EXPECT_EQ(scheduler.UsedMemoryBytes(), 30U);
  first.Release();
  EXPECT_EQ(scheduler.UsedCpuSlots(), 0U);
  EXPECT_EQ(scheduler.UsedMemoryBytes(), 0U);
}

TEST(QuerySchedulerTest, MovedFromLeaseReleaseIsNoOp) {
  QueryScheduler scheduler(1, 64);
  auto lease = scheduler.Acquire(1, 64);
  QueryScheduler::Lease moved(std::move(lease));
  EXPECT_EQ(scheduler.UsedCpuSlots(), 1U);
  // Release on a moved-from lease must be a no-op; that contract is what
  // this test verifies.
  lease.Release();  // NOLINT(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
  EXPECT_EQ(scheduler.UsedCpuSlots(), 1U);
  moved.Release();
  EXPECT_EQ(scheduler.UsedCpuSlots(), 0U);
}

TEST(QuerySchedulerTest, ScheduledExecutorReleasesLeaseOnDestruction) {
  QueryScheduler scheduler(1, 128);
  {
    ScheduledExecutor executor(
        std::make_shared<ConstantExecutor>(
            std::vector<Row>{Row({Value(1)}), Row({Value(2)})}),
        scheduler, 1, 64);
    Row row;
    ASSERT_TRUE(executor.Next(&row, nullptr));
    EXPECT_EQ(scheduler.UsedCpuSlots(), 1U);
    EXPECT_EQ(scheduler.UsedMemoryBytes(), 64U);
  }
  EXPECT_EQ(scheduler.UsedCpuSlots(), 0U);
  EXPECT_EQ(scheduler.UsedMemoryBytes(), 0U);
}

TEST(QuerySchedulerTest, ScheduledExecutorNextBatchKeepsLeaseUntilExhausted) {
  QueryScheduler scheduler(1, 128);
  Executor values = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(1)}), Row({Value(2)}), Row({Value(3)})});
  ScheduledExecutor executor(std::move(values), scheduler, 1, 64);
  DataChunk chunk;
  EXPECT_EQ(executor.NextBatch(&chunk, 2), 2U);
  EXPECT_EQ(scheduler.UsedCpuSlots(), 1U);
  EXPECT_EQ(scheduler.UsedMemoryBytes(), 64U);
  EXPECT_EQ(executor.NextBatch(&chunk, 2), 1U);
  EXPECT_EQ(executor.NextBatch(&chunk, 2), 0U);
  EXPECT_EQ(scheduler.UsedCpuSlots(), 0U);
  EXPECT_EQ(scheduler.UsedMemoryBytes(), 0U);
}

TEST(QuerySchedulerTest, ScheduledExecutorDumpAndExplain) {
  QueryScheduler scheduler(1, 128);
  Executor values = std::make_shared<ConstantExecutor>(
      std::vector<Row>{Row({Value(1)})});
  ScheduledExecutor executor(std::move(values), scheduler, 1, 64);
  std::stringstream dump;
  executor.Dump(dump, 0);
  EXPECT_NE(dump.str().find("ScheduledQuery"), std::string::npos);
  std::stringstream explain;
  executor.Explain(explain, 0);
  EXPECT_NE(explain.str().find("ScheduledQuery"), std::string::npos);
}

TEST(QuerySchedulerTest, GlobalReturnsSameSingleton) {
  QueryScheduler& a = QueryScheduler::Global();
  QueryScheduler& b = QueryScheduler::Global();
  EXPECT_EQ(&a, &b);
  EXPECT_GT(a.CpuCapacity(), 0U);
  EXPECT_GT(a.MemoryCapacity(), 0U);
}

}  // namespace tinylamb
