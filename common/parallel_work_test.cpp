/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "common/parallel_work.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <stdexcept>
#include <thread>
#include <vector>

#include "common/platform/cpu_affinity.hpp"
#include "common/query_parallelism.hpp"
#include "gtest/gtest.h"

namespace tinylamb {
TEST(ParallelWorkTest, TeamSizeRespectsWorkAndConcurrentAdmission) {
  EXPECT_EQ(ParallelWorkerCount(0), 1U);
  EXPECT_EQ(ParallelWorkerCount(1), 1U);
  std::atomic<size_t> active{3};
  ScopedQueryParallelismBudget budget(&active, 7);
  EXPECT_GE(ParallelWorkerCount(1000), 1U);
  EXPECT_LE(ParallelWorkerCount(1000), 2U);
  active.store(8);
  EXPECT_EQ(ParallelWorkerCount(1000), 1U);
}

TEST(ParallelWorkTest, ReusesHelpersAndExecutesEveryIndex) {
  std::array<std::thread::id, 2> first{};
  std::array<std::thread::id, 2> second{};
  ASSERT_TRUE(RunParallelTasks(2, [&](size_t i) {
                first[i] = std::this_thread::get_id();
              }).ok());
  ASSERT_TRUE(RunParallelTasks(2, [&](size_t i) {
                second[i] = std::this_thread::get_id();
              }).ok());
  EXPECT_EQ(first, second);
  EXPECT_EQ(first[0], std::this_thread::get_id());
  EXPECT_NE(first[0], first[1]);
  std::array<std::atomic<int>, 17> counts{};
  ASSERT_TRUE(
      RunParallelTasks(counts.size(), [&](size_t i) { ++counts[i]; }).ok());
  for (const auto& count : counts) {
    EXPECT_EQ(count.load(), 1);
  }
}

TEST(ParallelWorkTest, NestedAndConcurrentTeamsCompleteWithoutLosingTasks) {
  std::atomic<size_t> calls{0};
  std::vector<std::jthread> coordinators;
  coordinators.reserve(4);
  for (size_t caller = 0; caller < 4; ++caller) {
    coordinators.emplace_back([&] {
      for (int repeat = 0; repeat < 8; ++repeat) {
        EXPECT_TRUE(RunParallelTasks(4, [&](size_t) {
                      EXPECT_TRUE(
                          RunParallelTasks(3, [&](size_t) { ++calls; }).ok());
                    }).ok());
      }
    });
  }
  coordinators.clear();
  EXPECT_EQ(calls.load(), 4U * 8U * 4U * 3U);
}

TEST(ParallelWorkTest, ExceptionsWaitForAllTasksAndDoNotPoisonHelpers) {
  std::array<std::atomic<int>, 8> calls{};
  const Status status = RunParallelTasks(calls.size(), [&](size_t i) {
    ++calls[i];
    if (i == 2) {
      throw std::runtime_error("task error");
    }
  });
  EXPECT_FALSE(status.ok());
  for (const auto& count : calls) {
    EXPECT_EQ(count.load(), 1);
  }
  EXPECT_TRUE(RunParallelTasks(2, [&](size_t i) { ++calls[i]; }).ok());
  EXPECT_EQ(calls[0].load(), 2);
  EXPECT_EQ(calls[1].load(), 2);
  EXPECT_TRUE(
      RunParallelTasks(0, [](size_t) { FAIL() << "empty team ran"; }).ok());
  EXPECT_FALSE(RunParallelTasks(1, [](size_t) {
                 throw std::runtime_error("caller");
               }).ok());
}

TEST(ParallelWorkTest, HelpersInheritEachCoordinatorsBudgetAndAffinity) {
  std::atomic<size_t> active{2};
  {
    ScopedQueryParallelismBudget budget(&active, 6);
    const auto affinity = platform::CpuSet::CurrentThread();
    ASSERT_TRUE(RunParallelTasks(3, [&](size_t) {
                  EXPECT_EQ(QueryParallelWorkerLimit(100), 3U);
                  EXPECT_EQ(platform::CpuSet::CurrentThread(), affinity);
                }).ok());
  }
  EXPECT_TRUE(RunParallelTasks(3, [](size_t) {
                EXPECT_EQ(QueryParallelWorkerLimit(100), 100U);
              }).ok());
}
TEST(ParallelWorkTest, ReusedHelpersFollowChangedCpuAffinity) {
  const auto original = platform::CpuSet::CurrentThread();
  if (!original || original->Count() < 2) {
    GTEST_SKIP() << "requires at least two selectable CPUs";
  }
  ASSERT_TRUE(RunParallelTasks(2, [](size_t) {}).ok());
  struct
      RestoreAffinity {  // NOLINT(cppcoreguidelines-special-member-functions)
    platform::CpuSet cpus;
    ~RestoreAffinity() { static_cast<void>(cpus.ApplyToCurrentThread()); }
  } restore{*original};
  const auto single = platform::CpuSet::Single(original->First());
  ASSERT_TRUE(single.ApplyToCurrentThread());
  EXPECT_EQ(ParallelWorkerCount(1000), 1U);
  ASSERT_TRUE(RunParallelTasks(2, [&](size_t) {
                EXPECT_EQ(platform::CpuSet::CurrentThread(), single);
              }).ok());
  ASSERT_TRUE(original->ApplyToCurrentThread());
  EXPECT_TRUE(RunParallelTasks(2, [&](size_t) {
                EXPECT_EQ(platform::CpuSet::CurrentThread(), original);
              }).ok());
}

}  // namespace tinylamb
