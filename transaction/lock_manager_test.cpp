/**
 * Copyright 2026 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "transaction/lock_manager.hpp"

#include <chrono>
#include <thread>

#include "gtest/gtest.h"
#include "page/row_position.hpp"

namespace tinylamb {

TEST(LockManagerTest, ExclusiveWaitTimesOut) {
  LockManager lm;
  const RowPosition row{1, 0};
  ASSERT_TRUE(lm.GetExclusiveLock(row, 1, false));

  std::thread waiter([&] {
    // Contended exclusive wait should fail after a short timeout instead of
    // hanging forever.
    EXPECT_FALSE(
        lm.GetExclusiveLock(row, 2, std::chrono::milliseconds(50)));
  });
  waiter.join();
  ASSERT_TRUE(lm.ReleaseExclusiveLock(row, 1));
}

TEST(LockManagerTest, ExclusiveWaitSucceedsAfterRelease) {
  LockManager lm;
  const RowPosition row{2, 0};
  ASSERT_TRUE(lm.GetExclusiveLock(row, 1, false));

  std::thread releaser([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    ASSERT_TRUE(lm.ReleaseExclusiveLock(row, 1));
  });
  EXPECT_TRUE(lm.GetExclusiveLock(row, 2, std::chrono::milliseconds(500)));
  releaser.join();
  ASSERT_TRUE(lm.ReleaseExclusiveLock(row, 2));
}

TEST(LockManagerTest, DurabilityWaitRaisesEffectiveTimeout) {
  LockManager lm;
  EXPECT_EQ(lm.ExclusiveWaitTimeout(), std::chrono::milliseconds(5000));
  {
    LockManager::DurabilityWaitGuard guard(lm);
    EXPECT_EQ(lm.ExclusiveWaitTimeout(), std::chrono::milliseconds(60000));
    {
      // Nested durability waits (concurrent commits) must not restore the
      // base timeout while any of them is still in flight.
      LockManager::DurabilityWaitGuard nested(lm);
      EXPECT_EQ(lm.ExclusiveWaitTimeout(), std::chrono::milliseconds(60000));
    }
    EXPECT_EQ(lm.ExclusiveWaitTimeout(), std::chrono::milliseconds(60000));
  }
  EXPECT_EQ(lm.ExclusiveWaitTimeout(), std::chrono::milliseconds(5000));
}

TEST(LockManagerTest, ContendedWaitSurvivesDurabilityStall) {
  LockManager lm;
  const RowPosition row{9, 9};
  ASSERT_TRUE(lm.GetExclusiveLock(row, 1, false));

  bool granted = false;
  std::thread waiter([&] {
    // Simulates a concurrent commit parked in WaitForDurable: the contended
    // waiter below must keep waiting past the base 5 s timeout instead of
    // failing spuriously.
    LockManager::DurabilityWaitGuard guard(lm);
    granted = lm.GetExclusiveLock(row, 2);
  });
  // Well under the base 5 s timeout: the grant proves the wait stayed alive.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  ASSERT_TRUE(lm.ReleaseExclusiveLock(row, 1));
  waiter.join();
  EXPECT_TRUE(granted);
  ASSERT_TRUE(lm.ReleaseExclusiveLock(row, 2));
}

TEST(LockManagerTest, ExclusiveWaitTimesOutWithoutDurabilityWait) {
  LockManager lm;
  const RowPosition row{10, 0};
  ASSERT_TRUE(lm.GetExclusiveLock(row, 1, false));

  std::thread waiter([&] {
    EXPECT_FALSE(lm.GetExclusiveLock(
        row, 2, std::chrono::milliseconds(50)));
  });
  waiter.join();
  ASSERT_TRUE(lm.ReleaseExclusiveLock(row, 1));
}

TEST(LockManagerTest, SharedLocksTrackOwners) {
  LockManager lm;
  const RowPosition row{3, 0};
  ASSERT_TRUE(lm.GetSharedLock(row, 1));
  ASSERT_TRUE(lm.GetSharedLock(row, 2));

  // A foreign owner cannot release someone else's shared lock.
  EXPECT_FALSE(lm.ReleaseSharedLock(row, 3));
  EXPECT_TRUE(lm.ReleaseSharedLock(row, 1));
  EXPECT_FALSE(lm.ReleaseSharedLock(row, 1));
  EXPECT_TRUE(lm.ReleaseSharedLock(row, 2));
  EXPECT_FALSE(lm.ReleaseSharedLock(row, 2));
}

}  // namespace tinylamb
