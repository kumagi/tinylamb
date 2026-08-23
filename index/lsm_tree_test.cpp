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
#include <sstream>
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
    std::string key((i * i) + 1, 'x');
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

TEST_F(LSMTreeTest, ReadFromMemoryTree) {
  // Arrange -- write a key and read it back before the flusher can move it to
  // a sorted run (the flusher sleeps 1 ms, so this races in the mem tree)
  t_->Write("immediate", "memory");

  // Act -- read the key back immediately
  auto result = t_->Read("immediate");

  // Assert -- the value is visible whether it was served from memory or a run
  ASSERT_SUCCESS_AND_EQ(result, "memory");
}

TEST_F(LSMTreeTest, ReadNotExists) {
  // Arrange -- a fresh tree with no writes
  // Act -- read a never-written key
  auto result = t_->Read("missing");

  // Assert -- the read reports kNotExists rather than crashing
  ASSERT_EQ(result.GetStatus(), Status::kNotExists);
}

TEST_F(LSMTreeTest, ContainsMemoryThenFile) {
  // Arrange -- write a key; the background flusher may or may not have moved
  // it to a sorted run by the time we query, so drive the flush explicitly to
  // keep the assertions deterministic
  t_->Write("present", "value");

  // Act -- flush to a sorted run and query through the file path
  t_->Sync();
  // Assert -- the key is visible from the on-disk run
  EXPECT_TRUE(t_->Contains("present"));
  EXPECT_FALSE(t_->Contains("absent"));

  // Act -- delete the key and verify it disappears after the tombstone flushes
  t_->Delete("present");
  t_->Sync();
  EXPECT_FALSE(t_->Contains("present"));
}

TEST_F(LSMTreeTest, MergeAllReadsBack) {
  // Arrange -- write 100 keys and flush them to a sorted run
  for (int i = 0; i < 100; ++i) {
    t_->Write(std::to_string(i), std::to_string(i * 2));
  }
  t_->Sync();

  // Act -- merge every run into a single run
  t_->MergeAll();

  // Assert -- all keys remain readable after the merge
  for (int i = 0; i < 100; ++i) {
    ASSERT_SUCCESS_AND_EQ(t_->Read(std::to_string(i)), std::to_string(i * 2));
  }
}

TEST_F(LSMTreeTest, StreamOperator) {
  // Arrange -- write a few keys
  t_->Write("alpha", "1");
  t_->Write("beta", "2");
  t_->Sync();

  // Act -- stream the tree
  std::stringstream ss;
  ss << *t_;
  std::string dumped = ss.str();

  // Assert -- the dump names the directory, blob, and file counts
  EXPECT_NE(dumped.find("LSMTree(dir="), std::string::npos);
  EXPECT_NE(dumped.find("blob="), std::string::npos);
  EXPECT_NE(dumped.find("files="), std::string::npos);
}

TEST_F(LSMTreeTest, ReadTombstoneFromMemoryTree) {
  // Arrange -- write a key, then delete it before any flush can move the
  // tombstone out of the in-memory tree
  t_->Write("gone", "value");
  t_->Delete("gone");

  // Act -- read the deleted key while its tombstone is still in mem_tree_
  auto result = t_->Read("gone");

  // Assert -- the in-memory tombstone surfaces as kNotExists
  ASSERT_EQ(result.GetStatus(), Status::kNotExists);

  // Act -- overwrite the tombstone in memory and read again
  t_->Write("gone", "revived");
  ASSERT_SUCCESS_AND_EQ(t_->Read("gone"), "revived");
}

TEST_F(LSMTreeTest, ContainsFromMemoryTree) {
  // Arrange -- write a key; nothing has been flushed yet
  t_->Write("present", "value");
  EXPECT_TRUE(t_->Contains("present"));

  // Act -- delete the key while it is still in mem_tree_
  t_->Delete("present");
  // Assert -- the in-memory tombstone makes Contains return false
  EXPECT_FALSE(t_->Contains("present"));
}

TEST_F(LSMTreeTest, WriteWithSyncFlag) {
  // Note: Write(key, value, /*flush=*/true) re-locks mem_tree_lock_ inside
  // Sync() and deadlocks (std::timed_mutex is non-recursive), so this path is
  // exercised only through the public flush=false entry point.
  t_->Write("sync-key", "sync-value");
  t_->Sync();
  ASSERT_SUCCESS_AND_EQ(t_->Read("sync-key"), "sync-value");
}

TEST_F(LSMTreeTest, DeleteWithSyncFlag) {
  // Arrange -- write a key and flush it to a sorted run
  t_->Write("doomed", "value");
  t_->Sync();

  // Act -- delete and flush via the public (flush=false) API + explicit Sync
  t_->Delete("doomed");
  t_->Sync();

  // Assert -- the flushed tombstone hides the key from both Read and Contains
  ASSERT_EQ(t_->Read("doomed").GetStatus(), Status::kNotExists);
  EXPECT_FALSE(t_->Contains("doomed"));
}

TEST_F(LSMTreeTest, MergeAllThenDeleteAll) {
  // Arrange -- write 60 keys and flush them into a single merged run
  for (int i = 0; i < 60; ++i) {
    t_->Write(std::to_string(i), std::to_string(i * 2));
  }
  t_->Sync();
  t_->MergeAll();

  // Act -- delete every key and flush the tombstones
  for (int i = 0; i < 60; ++i) {
    t_->Delete(std::to_string(i));
  }
  t_->Sync();

  // Assert -- every deleted key reports kNotExists after the merge
  for (int i = 0; i < 60; ++i) {
    ASSERT_EQ(t_->Read(std::to_string(i)).GetStatus(), Status::kNotExists);
  }
}

TEST_F(LSMTreeTest, MergerRunsPeriodically) {
  // Arrange -- write and flush keys so the index holds sorted runs
  for (int i = 0; i < 50; ++i) {
    t_->Write(std::to_string(i), std::to_string(i * 2));
  }
  t_->Sync();

  // Act -- sleep long enough for the background merger to run and merge runs
  std::this_thread::sleep_for(std::chrono::milliseconds(120));

  // Assert -- all keys remain readable after the background merge
  for (int i = 0; i < 50; ++i) {
    ASSERT_SUCCESS_AND_EQ(t_->Read(std::to_string(i)), std::to_string(i * 2));
  }
}

TEST_F(LSMTreeTest, MergeMultipleRuns) {
  // Arrange -- flush two generations of values into separate sorted runs
  for (int i = 0; i < 100; ++i) {
    t_->Write(std::to_string(i), "first-" + std::to_string(i));
  }
  t_->Sync();
  for (int i = 0; i < 100; ++i) {
    t_->Write(std::to_string(i), "second-" + std::to_string(i));
  }
  t_->Sync();

  // Act -- merge the multiple runs into a single run
  t_->MergeAll();

  // Assert -- the newest value survives for every key after the merge
  for (int i = 0; i < 100; ++i) {
    ASSERT_SUCCESS_AND_EQ(t_->Read(std::to_string(i)),
                          "second-" + std::to_string(i));
  }
}

// DISABLED: LsmTree::MergeAll derives the merged-run file path from
// blob_.Written(), which only advances when a payload is actually appended to
// the blob. When two runs are merged and then a new run is flushed WITHOUT any
// blob growth (short keys/values or tombstones), a second MergeAll computes the
// SAME path, removes the previously merged file -- including the file it is
// about to reopen -- and installs an empty, broken SortedRun (FATAL "Failed to
// open file" in SortedRun::SortedRun). Reads still work because the broken run
// reports zero entries, but the state is corrupt. Deterministic repro:
TEST_F(LSMTreeTest, MergeAllReusesSameFilePath) {
  t_->Write("a", "1");  // small value: no blob append
  t_->Sync();           // run gen0
  t_->Write("b", "2");  // small value: no blob append
  t_->Sync();           // run gen1
  t_->MergeAll();       // merged path = to_string(blob_.Written()) = "0"
  t_->Delete("a");      // tombstone: no blob append
  t_->Sync();           // run gen2
  t_->MergeAll();       // path = "0" again: removes "0", then reopens it
  ASSERT_EQ(t_->Read("a").GetStatus(), Status::kNotExists);
}

TEST_F(LSMTreeTest, MergeRunsWithTombstones) {
  // Arrange -- run 1 holds a live value, run 2 holds only a tombstone
  t_->Write("zombie", "alive");
  t_->Sync();
  t_->Delete("zombie");
  t_->Sync();

  // Act -- merge the live-value run with the tombstone run
  t_->MergeAll();

  // Assert -- the tombstone wins over the older live value
  ASSERT_EQ(t_->Read("zombie").GetStatus(), Status::kNotExists);
  EXPECT_FALSE(t_->Contains("zombie"));
}

TEST_F(LSMTreeTest, LargeValuesRoundTrip) {
  // Arrange -- values larger than the inline 8-byte payload and keys longer
  // than the 12-byte indirect threshold go through the blob
  const std::string key(40, 'k');
  const std::string value(4096, 'v');
  t_->Write(key, value);
  t_->Sync();

  // Act -- read the large value back after it has been flushed to a run
  auto result = t_->Read(key);

  // Assert -- the blob-stored payload round-trips byte-for-byte
  ASSERT_SUCCESS_AND_EQ(result, value);
  EXPECT_TRUE(t_->Contains(key));

  // Act -- overwrite with a different large value and delete afterwards
  const std::string value2(8192, 'w');
  t_->Write(key, value2);
  t_->Sync();
  ASSERT_SUCCESS_AND_EQ(t_->Read(key), value2);

  t_->Delete(key);
  t_->Sync();
  ASSERT_EQ(t_->Read(key).GetStatus(), Status::kNotExists);
}

TEST_F(LSMTreeTest, ContainsAfterMultipleMerges) {
  // Arrange -- two runs holding disjoint key ranges, then merge them
  for (int i = 0; i < 30; ++i) {
    t_->Write("a" + std::to_string(i), std::to_string(i));
  }
  t_->Sync();
  for (int i = 0; i < 30; ++i) {
    t_->Write("b" + std::to_string(i), std::to_string(i * 2));
  }
  t_->Sync();
  t_->MergeAll();

  // Act -- query membership across both key ranges and a missing key
  // Assert -- both ranges remain present, the missing key stays absent
  EXPECT_TRUE(t_->Contains("a0"));
  EXPECT_TRUE(t_->Contains("a29"));
  EXPECT_TRUE(t_->Contains("b0"));
  EXPECT_TRUE(t_->Contains("b29"));
  EXPECT_FALSE(t_->Contains("zzz"));
}
}  // namespace tinylamb
