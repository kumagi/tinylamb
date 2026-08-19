/**
 * Copyright 2024 KUMAZAKI Hiroki
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

#include "index/lsm_tree.hpp"

#include <chrono>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <thread>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "gtest/gtest.h"
#include "lsm_detail/lsm_view.hpp"

namespace tinylamb {
class LSMTreeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_path_ = "lsm_tree_test-" + RandomString();
    t_ = std::make_unique<LSMTree>(dir_path_);
  }

  void TearDown() override {
    t_.reset();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    std::filesystem::remove_all(dir_path_);
  }

  std::unique_ptr<LSMTree> t_;
  std::string dir_path_;
};

// TEST_F(LSMTreeTest, Construct) {}

TEST_F(LSMTreeTest, WriteOne) {
  // Arrange -- nothing more than fixture setup

  // Act -- write single key-value pair, then sleep to let background flush
  t_->Write("foo", "bar");
  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  // Assert -- implicit; no crash, no explicit assertions; gtest green on pass
}

TEST_F(LSMTreeTest, WriteMany) {
  // Arrange -- nothing more than fixture setup

  // Act -- write 1000 key-value pairs, then sleep to let background flush
  for (int i = 0; i < 1000; ++i) {
    t_->Write(std::to_string(i), std::to_string(i * 2));
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  // Assert -- implicit; no crash, no explicit assertions; gtest green on pass
}

TEST_F(LSMTreeTest, ReadOne) {
  // Arrange -- write one key-value pair, wait for flush
  t_->Write("foo", "bar");
  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  // Act -- read back the key
  auto result = t_->Read("foo");

  // Assert -- read returns the value written
  ASSERT_SUCCESS_AND_EQ(result, "bar");
}

TEST_F(LSMTreeTest, ReadManyMemory) {
  // Arrange -- write 100 key-value pairs, wait for flush
  for (int i = 0; i < 100; ++i) {
    t_->Write(std::to_string(i), std::to_string(i * 2));
  }

  // Act -- read each key back
  for (int i = 0; i < 100; ++i) {
    ASSERT_SUCCESS_AND_EQ(t_->Read(std::to_string(i)), std::to_string(i * 2));
  }

  // Assert -- implicit; all 100 reads returned expected values; gtest green
}

TEST_F(LSMTreeTest, RangeScan) {
  // Arrange -- write 1000 key-value pairs, wait for sync, build expected map
  std::map<std::string, std::string> expected;
  for (int i = 0; i < 1000; ++i) {
    t_->Write(std::to_string(i), std::to_string(i * 2));
    expected.emplace(std::to_string(i), std::to_string(i * 2));
  }

  // Act -- sync LSMTree, then scan via iterator
  t_->Sync();
  LSMView v = t_->GetView();
  LSMView::Iterator it = v.Begin();
  auto expected_iter = expected.begin();

  // Assert -- iterator yields every key-value pair in sorted order, matching expected
  while (it.IsValid()) {
    ASSERT_EQ(expected_iter->first, it.Key());
    ASSERT_EQ(expected_iter->second, it.Value());
    ++it;
    ++expected_iter;
  }
  ASSERT_EQ(expected_iter, expected.end());
}

TEST_F(LSMTreeTest, PointQuery) {
  // Arrange -- write 1000 key-value pairs, wait for sync
  for (int i = 0; i < 1000; ++i) {
    t_->Write(std::to_string(i), std::to_string(i * 2));
  }

  // Act -- sync LSMTree, then point-query each key via view
  t_->Sync();
  LSMView v = t_->GetView();
  for (int i = 0; i < 1000; ++i) {
    auto ret = v.Find(std::to_string(i));

    // Assert -- each point-query returns the expected value
    ASSERT_SUCCESS_AND_EQ(ret, std::to_string(i * 2));
  }
}

TEST_F(LSMTreeTest, OverwrittenRangeScan) {
  // Arrange -- write 1000 key-value pairs, sync, take view v1
  for (int i = 0; i < 1000; ++i) {
    t_->Write(std::to_string(i), std::to_string(i * 2));
  }
  t_->Sync();
  LSMView v1 = t_->GetView();

  // Act 1 -- overwrite even-indexed keys with i*i, sync, take view v2
  for (int i = 0; i < 1000; i += 2) {
    t_->Write(std::to_string(i), std::to_string(i * i));
  }
  t_->Sync();
  LSMView v2 = t_->GetView();

  // Assert 1 -- v1 sees original values for all 1000 keys (snapshot semantics)
  for (int i = 0; i < 1000; ++i) {
    auto ret = v1.Find(std::to_string(i));
    ASSERT_SUCCESS_AND_EQ(ret, std::to_string(i * 2));
  }

  // Assert 2 -- v2 sees i*i for even keys, i*2 for odd keys (overwrite visible only in v2)
  for (int i = 0; i < 1000; ++i) {
    if (i % 2 == 0) {
      auto ret = v2.Find(std::to_string(i));
      ASSERT_SUCCESS_AND_EQ(ret, std::to_string(i * i));
    } else {
      auto ret = v2.Find(std::to_string(i));
      ASSERT_SUCCESS_AND_EQ(ret, std::to_string(i * 2));
    }
  }
}

TEST_F(LSMTreeTest, LongKeyRangeScan) {
  // Arrange -- write 300 key-value pairs with long keys (i*i+1 bytes of 'x'), wait for sync
  std::map<std::string, std::string> expected;
  for (int i = 0; i < 300; ++i) {
    std::string key(i * i + 1, 'x');
    t_->Write(key, std::to_string(i * 2));
    expected.emplace(key, std::to_string(i * 2));
  }

  // Act -- sync LSMTree, then scan via iterator
  t_->Sync();
  LSMView v = t_->GetView();
  LSMView::Iterator it = v.Begin();
  auto expected_iter = expected.begin();

  // Assert -- iterator yields every key-value pair in sorted order, matching expected
  while (it.IsValid()) {
    ASSERT_EQ(expected_iter->first, it.Key());
    ASSERT_EQ(expected_iter->second, it.Value());
    ++it;
    ++expected_iter;
  }
  ASSERT_EQ(expected_iter, expected.end());
}

TEST_F(LSMTreeTest, DeleteSingle) {
  // Arrange -- write 1000 key-value pairs, sync, delete odd-indexed keys
  for (int i = 0; i < 1000; ++i) {
    t_->Write(std::to_string(i), std::to_string(i * 2));
  }
  t_->Sync();
  for (int i = 1; i < 1000; i += 2) {
    t_->Delete(std::to_string(i));
  }
  t_->Sync();

  // Act -- read each key back; odd keys should be absent, even keys present
  for (int i = 0; i < 1000; ++i) {
    StatusOr<std::string> ret = t_->Read(std::to_string(i));

    // Assert -- even keys retain original value, odd keys return kNotExists
    if (i % 2 == 0) {
      ASSERT_SUCCESS_AND_EQ(ret, std::to_string(i * 2));
    } else {
      ASSERT_EQ(ret.GetStatus(), Status::kNotExists);
    }
  }
}

TEST_F(LSMTreeTest, DeleteRangeScan) {
  // Arrange -- write 1000 key-value pairs, sync, delete odd-indexed keys
  for (int i = 0; i < 1000; ++i) {
    t_->Write(std::to_string(i), std::to_string(i * 2));
  }
  t_->Sync();
  for (int i = 1; i < 1000; i += 2) {
    t_->Delete(std::to_string(i));
  }
  t_->Sync();

  // Act -- scan via iterator after deletion
  LSMView v = t_->GetView();
  LSMView::Iterator iter = v.Begin();

  // Assert -- iterator yields only even-indexed keys with their original values
  while (iter.IsValid()) {
    int key = std::stoi(iter.Key());
    ASSERT_EQ(key % 2, 0);
    ASSERT_EQ(iter.Value(), std::to_string(key * 2));
    ++iter;
  }
}

// Regression test for a crash found by the lsm_tree fuzzer (seed
// "\x00\x01a\x01b").  The background merger thread wakes every 20ms and calls
// MergeAll(); whenever the tree has no sorted run yet -- a fresh tree that was
// never flushed, or the race where Sync() has built a run file but has not yet
// registered it under file_tree_lock_ -- the LSMView built by GetViewImpl()
// contains ZERO runs.  LSMView::Iterator's constructor then reads iters_[0]
// from the empty vector (index/lsm_detail/lsm_view.cpp:115), which is a null
// reference binding (UBSan) and a SEGV under ASan.  The crashing merger thread
// died while holding file_tree_lock_, deadlocking every later Sync/Read/Write.
// Correct behavior: merging an empty index is a no-op, and Begin() on an empty
// view is an already-invalid iterator.
TEST_F(LSMTreeTest, MergeAllOnEmptyIndex) {
  // Arrange -- an empty tree: nothing was ever written, so the flusher's Sync()
  // always returns early and index_ holds no sorted run.

  // Act -- merge the (empty) index, exactly what the background Merger does.
  t_->MergeAll();

  // Assert -- should be a no-op on an empty tree.  Currently crashes in
  // LSMView::CreateSingleRun() -> Begin() -> iters_[0] on an empty vector.
  ASSERT_TRUE(true);
}

// Same root cause, reachable through the public view API on an empty tree.
TEST_F(LSMTreeTest, EmptyViewBegin) {
  // Arrange -- an empty tree, so GetView() builds an LSMView with no runs.
  LSMView v = t_->GetView();

  // Act -- begin iteration over the empty view.
  LSMView::Iterator it = v.Begin();

  // Assert -- an iterator over an empty view is invalid, not a null deref.
  ASSERT_FALSE(it.IsValid());
}
}  // namespace tinylamb
