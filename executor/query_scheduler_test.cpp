/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/query_scheduler.hpp"

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <vector>

#include "executor/constant_executor.hpp"
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

}  // namespace tinylamb
