/**
 * Copyright 2023 KUMAZAKI Hiroki
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

#include "b_plus_tree_iterator.hpp"

#include <cstddef>
#include <cstdio>
#include <iostream>
#include <memory>
#include <string>
#include <tuple>

#include "b_plus_tree.hpp"
#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "page/page.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "page/page_type.hpp"
#include "recovery/checkpoint_manager.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {
class BPlusTreeIteratorTest : public ::testing::Test {
 public:
  void SetUp() override {
    std::string prefix = "b_plus_tree_iterator_test-" + RandomString();
    db_name_ = prefix + ".db";
    log_name_ = prefix + ".log";
    Recover();
    auto txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kLeafPage);
    ASSERT_EQ(page->PageID(), 1);
    EXPECT_SUCCESS(txn.PreCommit());
  }

  void Insert(Transaction& txn, const char c, size_t key_len,
              size_t value_len) const {
    ASSERT_SUCCESS(
        bpt_->Insert(txn, std::string(key_len, c), std::string(value_len, c)));
  }

  void Flush(page_id_t pid) const { p_->GetPool()->FlushPageForTest(pid); }

  void Recover() {
    page_id_t root = bpt_ ? bpt_->Root() : 1;
    if (p_) {
      p_->GetPool()->DropAllPages();
    }
    bpt_.reset();
    tm_.reset();
    r_.reset();
    lm_.reset();
    l_.reset();
    p_.reset();
    p_ = std::make_unique<PageManager>(db_name_, 10);
    l_ = std::make_unique<Logger>(log_name_);
    lm_ = std::make_unique<LockManager>();
    r_ = std::make_unique<RecoveryManager>(log_name_, p_->GetPool());
    tm_ = std::make_unique<TransactionManager>(lm_.get(), p_.get(), l_.get(),
                                               r_.get());
    bpt_ = std::make_unique<BPlusTree>(root);
  }

  void TearDown() override {
    bpt_.reset();
    tm_.reset();
    r_.reset();
    lm_.reset();
    l_.reset();
    p_.reset();
    std::ignore = std::remove(db_name_.c_str());
    std::ignore = std::remove(log_name_.c_str());
  }

  std::string db_name_;
  std::string log_name_;
  std::unique_ptr<LockManager> lm_;
  std::unique_ptr<PageManager> p_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<RecoveryManager> r_;
  std::unique_ptr<TransactionManager> tm_;
  std::unique_ptr<BPlusTree> bpt_;
};

TEST_F(BPlusTreeIteratorTest, Construct) {
  // Arrange -- nothing to set up; default BPlusTreeIterator constructed by SetUp()
  // Act -- nothing to execute; default constructed via SetUp()
  // Assert -- nothing to verify; gtest green on pass, death on crash
}

TEST_F(BPlusTreeIteratorTest, FullScan) {
  // Arrange -- begin transaction, insert 7 keys a..g with 1000-byte key and 100-byte value
  auto txn = tm_->Begin();
  for (const auto& c : {'a', 'b', 'c', 'd', 'e', 'f', 'g'}) {
    Insert(txn, c, 1000, 100);
  }

  // Act -- begin a full scan iterator and walk through all 7 keys
  BPlusTreeIterator it = bpt_->Begin(txn);
  for (const auto& c : {'a', 'b', 'c', 'd', 'e', 'f', 'g'}) {
    EXPECT_TRUE(it.IsValid());
    EXPECT_EQ(it.Value(), std::string(100, c));
    ++it;
  }

  // Assert -- iterator is exhausted after the 7th key
  EXPECT_FALSE(it.IsValid());
}

TEST_F(BPlusTreeIteratorTest, RangeAcending) {
  // Arrange -- begin transaction, insert 7 keys a..g with 1000-byte key and 100-byte value
  auto txn = tm_->Begin();
  for (const auto& c : {'a', 'b', 'c', 'd', 'e', 'f', 'g'}) {
    Insert(txn, c, 1000, 100);
  }

  // Act -- begin a range scan iterator [b, d] and walk forward
  BPlusTreeIterator it = bpt_->Begin(txn, "b", "d");
  EXPECT_TRUE(it.IsValid());
  EXPECT_EQ(it.Value(), std::string(100, 'b'));
  ++it;
  EXPECT_TRUE(it.IsValid());
  EXPECT_EQ(it.Value(), std::string(100, 'c'));
  ++it;

  // Assert -- iterator reaches the upper bound "d" and then exhausts
  EXPECT_FALSE(it.IsValid());
}

TEST_F(BPlusTreeIteratorTest, RangeDescending) {
  // Arrange -- begin transaction, insert 7 keys a..g with 1000-byte key and 100-byte value
  auto txn = tm_->Begin();
  for (const auto& c : {'a', 'b', 'c', 'd', 'e', 'f', 'g'}) {
    Insert(txn, c, 1000, 100);
  }

  // Act -- begin a descending range scan iterator ["", "d"] and walk backward
  BPlusTreeIterator it = bpt_->Begin(txn, "", "d", false);
  EXPECT_EQ(it.Value(), std::string(100, 'd'));
  --it;
  EXPECT_TRUE(it.IsValid());
  EXPECT_EQ(it.Value(), std::string(100, 'c'));
  --it;
  EXPECT_TRUE(it.IsValid());
  EXPECT_EQ(it.Value(), std::string(100, 'b'));
  --it;
  EXPECT_TRUE(it.IsValid());
  EXPECT_EQ(it.Value(), std::string(100, 'a'));
  --it;

  // Assert -- iterator reaches the lower bound "a" and then exhausts
  EXPECT_FALSE(it.IsValid());
}

TEST_F(BPlusTreeIteratorTest, RangeDescendingRightOpen) {
  // Arrange -- begin transaction, insert 7 keys a..g with 1000-byte key and 100-byte value
  auto txn = tm_->Begin();
  for (const auto& c : {'a', 'b', 'c', 'd', 'e', 'f', 'g'}) {
    Insert(txn, c, 1000, 100);
  }

  // Act -- begin a descending full scan iterator ["", ""] (right-open) and walk backward
  BPlusTreeIterator it = bpt_->Begin(txn, "", "", false);
  EXPECT_EQ(it.Value(), std::string(100, 'g'));
  --it;
  EXPECT_TRUE(it.IsValid());
  EXPECT_EQ(it.Value(), std::string(100, 'f'));
  --it;
  EXPECT_TRUE(it.IsValid());
  EXPECT_EQ(it.Value(), std::string(100, 'e'));
  --it;
  EXPECT_TRUE(it.IsValid());
  EXPECT_EQ(it.Value(), std::string(100, 'd'));
  --it;
  EXPECT_TRUE(it.IsValid());
  EXPECT_EQ(it.Value(), std::string(100, 'c'));
  --it;
  EXPECT_TRUE(it.IsValid());
  EXPECT_EQ(it.Value(), std::string(100, 'b'));
  --it;
  EXPECT_TRUE(it.IsValid());
  EXPECT_EQ(it.Value(), std::string(100, 'a'));
  --it;

  // Assert -- iterator exhausts after visiting all 7 keys in reverse
  EXPECT_FALSE(it.IsValid());
}

TEST_F(BPlusTreeIteratorTest, FullScanMultiLeaf) {
  // Arrange -- begin transaction, insert 9 keys '1'..'9' with 2723-byte key and 2723-byte value
  constexpr int kSize = 2723;
  auto txn = tm_->Begin();
  for (const auto& c : {'1', '2', '3', '4', '5', '6', '7', '8', '9'}) {
    Insert(txn, c, kSize, kSize);
  }
  DumpLogTxn(*bpt_, txn);

  // Act -- begin a full scan iterator and walk through all 9 keys
  BPlusTreeIterator it = bpt_->Begin(txn);
  for (const auto& c : {'1', '2', '3', '4', '5', '6', '7', '8', '9'}) {
    SCOPED_TRACE(c);
    DumpLogTxn(*bpt_, txn);
    ASSERT_TRUE(it.IsValid());
    ASSERT_EQ(it.Value(), std::string(kSize, c));
    ++it;
  }

  // Assert -- iterator is exhausted after the 9th key
  EXPECT_FALSE(it.IsValid());
}

TEST_F(BPlusTreeIteratorTest, FullScanMultiLeafRecovery) {
  // Arrange -- begin transaction, insert 9 keys '1'..'9' with 2000-byte key and 2000-byte value, precommit
  constexpr int kSize = 2000;
  {
    auto txn = tm_->Begin();
    for (const auto& c : {'1', '2', '3', '4', '5', '6', '7', '8', '9'}) {
      Insert(txn, c, kSize, kSize);
    }
    txn.PreCommit();
  }
  LOG(ERROR) << "before recovery";
  {
    auto txn = tm_->Begin();
    DumpLogTxn(*bpt_, txn);
  }

  // Act -- emulate crash + recovery, then begin a full scan iterator
  Recover();
  r_->RecoverFrom(0, tm_.get());
  LOG(ERROR) << "after recovery";
  {
    auto txn = tm_->Begin();
    BPlusTreeIterator it = bpt_->Begin(txn);
    for (const auto& c : {'1', '2', '3', '4', '5', '6', '7', '8', '9'}) {
      SCOPED_TRACE(c);
      ASSERT_TRUE(it.IsValid());
      ASSERT_EQ(it.Value(), std::string(kSize, c));
      ++it;
    }

    // Assert -- iterator is exhausted after the 9th key
    EXPECT_FALSE(it.IsValid());
  }
}

TEST_F(BPlusTreeIteratorTest, FullScanReverse) {
  // Arrange -- begin transaction, insert 11 keys 'k'..'a' (reverse order) with 2000-byte key and 2000-byte value
  constexpr int kSize = 2000;
  auto txn = tm_->Begin();
  for (const auto& c :
       {'k', 'j', 'i', 'h', 'g', 'f', 'e', 'd', 'c', 'b', 'a'}) {
    Insert(txn, c, kSize, kSize);
  }

  // Act -- begin a full scan iterator (forward) and walk through all 11 keys in alphabetical order
  BPlusTreeIterator it = bpt_->Begin(txn);
  for (const auto& c :
       {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k'}) {
    SCOPED_TRACE(c);
    EXPECT_TRUE(it.IsValid());
    ASSERT_EQ(it.Value(), std::string(kSize, c));
    ++it;
  }

  // Assert -- iterator is exhausted after the 11th key
  EXPECT_FALSE(it.IsValid());
}

TEST_F(BPlusTreeIteratorTest, EndOpenFullScanReverse) {
  // Arrange -- begin transaction, insert 11 keys 'a'..'k' (forward order) with 2000-byte key and 2000-byte value
  constexpr int kSize = 2000;
  auto txn = tm_->Begin();
  for (const auto& c :
       {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k'}) {
    Insert(txn, c, kSize, kSize);
  }

  // Act -- begin a descending full scan iterator ["", ""] (both bounds open) and walk backward
  BPlusTreeIterator it = bpt_->Begin(txn, "", "", false);
  DumpLogTxn(*bpt_, txn);
  for (const auto& c :
       {'k', 'j', 'i', 'h', 'g', 'f', 'e', 'd', 'c', 'b', 'a'}) {
    SCOPED_TRACE(c);
    EXPECT_TRUE(it.IsValid());
    ASSERT_EQ(it.Value(), std::string(kSize, c));
    --it;
  }

  // Assert -- iterator is exhausted after visiting all 11 keys in reverse
  EXPECT_FALSE(it.IsValid());
}

// PRODUCTION BUG: BPlusTreeIterator's invalid-range guard never throws. The
// constructor computes `std::runtime_error("invalid begin & end")` without a
// `throw` (index/b_plus_tree_iterator.cpp:34-36), so it only constructs and
// discards an exception object. Both EXPECT_THROWs below fail.
TEST_F(BPlusTreeIteratorTest, ThrowsOnInvalidRange) {
  // Arrange -- begin transaction, no inserts needed
  auto txn = tm_->Begin();

  // Act/Assert -- a range whose begin sorts after end SHOULD be rejected in
  // both directions, but the missing `throw` lets it pass silently
  EXPECT_THROW(bpt_->Begin(txn, "z", "a"), std::runtime_error);
  EXPECT_THROW(bpt_->Begin(txn, "z", "a", false), std::runtime_error);

  // A well-ordered range is accepted for both directions.
  EXPECT_NO_THROW(bpt_->Begin(txn, "a", "z"));
  EXPECT_NO_THROW(bpt_->Begin(txn, "a", "z", false));
}

TEST_F(BPlusTreeIteratorTest, EmptyRangeIsInvalidFromConstruction) {
  // Arrange -- a single-page tree with small keys
  auto txn = tm_->Begin();
  for (const auto& c : {'a', 'b', 'c', 'd', 'e', 'f', 'g'}) {
    Insert(txn, c, 10, 10);
  }

  // Act -- request a range whose begin lands after the end; the first in-range
  // key is already past the upper bound ("d" sorts before the stored "dddd...")
  BPlusTreeIterator it = bpt_->Begin(txn, "d", "d");

  // Assert -- the iterator reports invalid without yielding any key
  EXPECT_FALSE(it.IsValid());
}

TEST_F(BPlusTreeIteratorTest, AscendingRangeStopsAtUpperBoundAcrossLeaves) {
  // Arrange -- one exclusive-size key+value per page splits the nine keys
  // into leaves ['1','2','3'] | ['4','5','6'] + foster ['7','8','9']
  constexpr int kSize = 2723;
  auto txn = tm_->Begin();
  for (const auto& c : {'1', '2', '3', '4', '5', '6', '7', '8', '9'}) {
    Insert(txn, c, kSize, kSize);
  }

  // Act 1 -- scan [1, "3\x7f"); '3' is the last key of the fosterless left
  // leaf, so crossing its high fence must stop at the upper bound
  const std::string fence_end = std::string("3") + static_cast<char>(0x7f);
  BPlusTreeIterator it = bpt_->Begin(txn, "1", fence_end);
  for (const auto& c : {'1', '2', '3'}) {
    SCOPED_TRACE(c);
    ASSERT_TRUE(it.IsValid());
    ASSERT_EQ(it.Value(), std::string(kSize, c));
    ++it;
  }
  EXPECT_FALSE(it.IsValid());

  // Act 2 -- scan [1, "6\x7f"); '6' is the last key of the middle leaf, whose
  // next page is reached through a foster pointer
  const std::string foster_end = std::string("6") + static_cast<char>(0x7f);
  it = bpt_->Begin(txn, "1", foster_end);
  for (const auto& c : {'1', '2', '3', '4', '5', '6'}) {
    SCOPED_TRACE(c);
    ASSERT_TRUE(it.IsValid());
    ASSERT_EQ(it.Value(), std::string(kSize, c));
    ++it;
  }

  // Assert -- both crossings stop before the first key past their upper bound
  EXPECT_FALSE(it.IsValid());
}

TEST_F(BPlusTreeIteratorTest, DescendingRangeStopsAtLowerBoundWithinPage) {
  // Arrange -- small keys so every key shares one leaf page
  auto txn = tm_->Begin();
  for (const auto& c : {'a', 'b', 'c', 'd', 'e', 'f', 'g'}) {
    Insert(txn, c, 10, 10);
  }

  // Act -- descend from the rightmost key and stop before "c"
  BPlusTreeIterator it = bpt_->Begin(txn, "c", "", false);
  ASSERT_TRUE(it.IsValid());
  ASSERT_EQ(it.Value(), std::string(10, 'g'));
  --it;
  ASSERT_TRUE(it.IsValid());
  ASSERT_EQ(it.Value(), std::string(10, 'f'));
  --it;
  ASSERT_TRUE(it.IsValid());
  ASSERT_EQ(it.Value(), std::string(10, 'e'));
  --it;
  ASSERT_TRUE(it.IsValid());
  ASSERT_EQ(it.Value(), std::string(10, 'd'));
  --it;
  ASSERT_TRUE(it.IsValid());
  ASSERT_EQ(it.Value(), std::string(10, 'c'));
  --it;

  // Assert -- 'b' < "c" lower bound, so the scan is exhausted
  EXPECT_FALSE(it.IsValid());
}

TEST_F(BPlusTreeIteratorTest, DescendingRangeStopsAtLowerBoundAcrossLeaves) {
  // Arrange -- one exclusive-size key+value per page splits the seven keys
  // into leaves ['a','b','c'] | ['d','e','f'] + foster ['g']
  constexpr int kSize = 2723;
  auto txn = tm_->Begin();
  for (const auto& c : {'a', 'b', 'c', 'd', 'e', 'f', 'g'}) {
    Insert(txn, c, kSize, kSize);
  }

  // Act -- descend from the rightmost key and stop before "d"; 'd' is the
  // first key of the middle leaf, so the final step crosses a leaf boundary
  BPlusTreeIterator it = bpt_->Begin(txn, "d", "", false);
  ASSERT_TRUE(it.IsValid());
  ASSERT_EQ(it.Value(), std::string(kSize, 'g'));
  --it;
  ASSERT_TRUE(it.IsValid());
  ASSERT_EQ(it.Value(), std::string(kSize, 'f'));
  --it;
  ASSERT_TRUE(it.IsValid());
  ASSERT_EQ(it.Value(), std::string(kSize, 'e'));
  --it;
  ASSERT_TRUE(it.IsValid());
  ASSERT_EQ(it.Value(), std::string(kSize, 'd'));
  --it;

  // Assert -- crossing from the 'd' page to the 'c' page stops at the bound
  EXPECT_FALSE(it.IsValid());
}

TEST_F(BPlusTreeIteratorTest, ReverseScanAfterDescendingInserts) {
  // Arrange -- insert 11 keys 'k'..'a' (reverse order), one exclusive-size
  // pair per page; this produces a foster topology distinct from ascending
  // inserts
  constexpr int kSize = 2723;
  auto txn = tm_->Begin();
  for (const auto& c :
       {'k', 'j', 'i', 'h', 'g', 'f', 'e', 'd', 'c', 'b', 'a'}) {
    Insert(txn, c, kSize, kSize);
  }

  // Act -- scan every key in reverse order
  BPlusTreeIterator it = bpt_->Begin(txn, "", "", false);
  for (const auto& c :
       {'k', 'j', 'i', 'h', 'g', 'f', 'e', 'd', 'c', 'b', 'a'}) {
    SCOPED_TRACE(c);
    ASSERT_TRUE(it.IsValid());
    ASSERT_EQ(it.Value(), std::string(kSize, c));
    --it;
  }

  // Assert -- the scan is exhausted after the smallest key
  EXPECT_FALSE(it.IsValid());
}

TEST_F(BPlusTreeIteratorTest, ForwardScanCrossesIntoFosterPage) {
  // Shared rows fill the first leaf; exclusive rows grow a foster chain that
  // is absorbed into branch pages.  Crossing the shared-leaf high fence must
  // descend through the foster chain in the read-only leaf lookup.
  constexpr static int kExclusiveValue = 6000;
  auto txn = tm_->Begin();
  Insert(txn, 'a', 10, 10);
  Insert(txn, 'b', 10, 10);
  Insert(txn, 'c', 10, 10);
  for (const auto& c : {'d', 'e', 'f', 'g'}) {
    Insert(txn, c, 1, kExclusiveValue);
  }

  const auto ExpectedKey = [](char c) {
    return std::string(c <= 'c' ? 10 : 1, c);
  };

  BPlusTreeIterator it = bpt_->Begin(txn);
  for (const auto& c : {'a', 'b', 'c', 'd', 'e', 'f', 'g'}) {
    SCOPED_TRACE(c);
    ASSERT_TRUE(it.IsValid());
    ASSERT_EQ(it.Key(), ExpectedKey(c));
    ++it;
  }
  EXPECT_FALSE(it.IsValid());

  it = bpt_->Begin(txn, "b", "f");
  for (const auto& c : {'b', 'c', 'd', 'e', 'f'}) {
    SCOPED_TRACE(c);
    ASSERT_TRUE(it.IsValid());
    ++it;
  }
  EXPECT_FALSE(it.IsValid());
}

TEST_F(BPlusTreeIteratorTest, ReverseScanOverExclusiveFosterChain) {
  // A pure exclusive foster chain: every page holds a single exclusive row and
  // is linked to its successor by a foster pointer.  A full descending scan
  // must hop between foster pages while honoring the lower bound.
  constexpr static int kExclusiveValue = 6000;
  auto txn = tm_->Begin();
  for (const auto& c : {'a', 'b', 'c', 'd', 'e', 'f', 'g'}) {
    Insert(txn, c, 1, kExclusiveValue);
  }

  BPlusTreeIterator it = bpt_->Begin(txn, "", "", false);
  for (const auto& c : {'g', 'f', 'e', 'd', 'c', 'b', 'a'}) {
    SCOPED_TRACE(c);
    ASSERT_TRUE(it.IsValid());
    ASSERT_EQ(it.Value(), std::string(kExclusiveValue, c));
    --it;
  }
  EXPECT_FALSE(it.IsValid());
}

TEST_F(BPlusTreeIteratorTest, BoundedReverseOverFosterChain) {
  // A descending range scan over an exclusive foster chain: stepping below the
  // lower bound crosses foster links and must stop exactly at the boundary.
  constexpr static int kExclusiveValue = 6000;
  auto txn = tm_->Begin();
  for (const auto& c : {'a', 'b', 'c', 'd', 'e', 'f', 'g'}) {
    Insert(txn, c, 1, kExclusiveValue);
  }

  BPlusTreeIterator it = bpt_->Begin(txn, "c", "", false);
  for (const auto& c : {'g', 'f', 'e', 'd', 'c'}) {
    SCOPED_TRACE(c);
    ASSERT_TRUE(it.IsValid());
    ASSERT_EQ(it.Value(), std::string(kExclusiveValue, c));
    --it;
  }
  EXPECT_FALSE(it.IsValid());
}

TEST_F(BPlusTreeIteratorTest, HeavyExclusiveFullScanBothDirections) {
  // A large exclusive tree stresses the iterator's leaf and foster crossings
  // in both directions with no explicit bounds.
  constexpr static int kCount = 40;
  constexpr static int kExclusiveValue = 6000;
  auto txn = tm_->Begin();
  for (int i = 0; i < kCount; ++i) {
    char c = static_cast<char>('a' + i);
    Insert(txn, c, 1, kExclusiveValue);
  }

  {
    BPlusTreeIterator it = bpt_->Begin(txn);
    for (int i = 0; i < kCount; ++i) {
      SCOPED_TRACE(i);
      ASSERT_TRUE(it.IsValid());
      ++it;
    }
    EXPECT_FALSE(it.IsValid());
  }
  {
    BPlusTreeIterator it = bpt_->Begin(txn, "", "", false);
    for (int i = 0; i < kCount; ++i) {
      SCOPED_TRACE(i);
      ASSERT_TRUE(it.IsValid());
      --it;
    }
    EXPECT_FALSE(it.IsValid());
  }
}

TEST_F(BPlusTreeIteratorTest, ForwardScanDescendsEmptyLeafWithFoster) {
  // root (branch) -> [leaf "a", empty leaf B (foster -> leaf "b")].  Crossing
  // leaf A's high fence performs a read-only leaf lookup that lands on the
  // empty foster parent B and must follow B's foster pointer to the row.
  {
    auto txn = tm_->Begin();
    PageRef root = p_->GetPage(bpt_->Root());
    root->PageTypeChange(txn, PageType::kBranchPage);
    PageRef a = p_->AllocateNewPage(txn, PageType::kLeafPage);
    a->InsertLeaf(txn, "a", "1");
    COERCE(a->SetHighFence(txn, IndexKey("b")));
    root->SetLowestValue(txn, a->PageID());
    PageRef b = p_->AllocateNewPage(txn, PageType::kLeafPage);
    COERCE(b->SetLowFence(txn, IndexKey("b")));
    root->InsertBranch(txn, "b", b->PageID());
    PageRef c = p_->AllocateNewPage(txn, PageType::kLeafPage);
    c->InsertLeaf(txn, "b", "2");
    COERCE(c->SetLowFence(txn, IndexKey("b")));
    ASSERT_SUCCESS(b->SetFoster(txn, FosterPair("b", c->PageID())));
    txn.PreCommit();
  }

  auto txn = tm_->Begin();
  BPlusTreeIterator it = bpt_->Begin(txn);
  ASSERT_TRUE(it.IsValid());
  ASSERT_EQ(it.Key(), "a");
  ++it;
  ASSERT_TRUE(it.IsValid());
  ASSERT_EQ(it.Key(), "b");
  ++it;
  EXPECT_FALSE(it.IsValid());

  // The reverse walk from the foster child's only row steps back onto the
  // empty foster parent and must end the scan (no lower rows exist).
  it = bpt_->Begin(txn, "", "", false);
  ASSERT_TRUE(it.IsValid());
  ASSERT_EQ(it.Key(), "b");
  --it;
  EXPECT_FALSE(it.IsValid());
}

TEST_F(BPlusTreeIteratorTest, ReverseScanKeyAndValueAccess) {
  // Arrange -- a single-page tree with small keys
  auto txn = tm_->Begin();
  Insert(txn, 'a', 8, 6);
  Insert(txn, 'b', 8, 6);
  Insert(txn, 'c', 8, 6);

  // Act -- walk the iterator backward while inspecting both payloads
  BPlusTreeIterator it = bpt_->Begin(txn, "", "", false);
  ASSERT_TRUE(it.IsValid());
  EXPECT_EQ(it.Key(), std::string(8, 'c'));
  EXPECT_EQ(it.Value(), std::string(6, 'c'));
  --it;
  EXPECT_TRUE(it.IsValid());
  EXPECT_EQ(it.Key(), std::string(8, 'b'));
  EXPECT_EQ(it.Value(), std::string(6, 'b'));
  --it;
  EXPECT_TRUE(it.IsValid());
  EXPECT_EQ(it.Key(), std::string(8, 'a'));
  EXPECT_EQ(it.Value(), std::string(6, 'a'));
  --it;
  EXPECT_FALSE(it.IsValid());

  // Assert -- operator<< renders both the valid and exhausted states
  std::ostringstream oss;
  it = bpt_->Begin(txn);
  ASSERT_TRUE(it.IsValid());
  oss << it;
  EXPECT_NE(oss.str().find("BPlusTreeIterator(key="), std::string::npos);
  EXPECT_NE(oss.str().find("value="), std::string::npos);
}

TEST_F(BPlusTreeIteratorTest, EmptyTreeForwardScanExhaustsImmediately) {
  // Arrange -- the tree created by SetUp() has an empty single leaf root
  auto txn = tm_->Begin();

  // Act -- begin a forward full scan on the empty tree
  BPlusTreeIterator it = bpt_->Begin(txn);

  // Assert -- stepping past the empty leaf invalidates the iterator
  EXPECT_TRUE(it.IsValid());
  ++it;
  EXPECT_FALSE(it.IsValid());
}

TEST_F(BPlusTreeIteratorTest, EmptyTreeReverseScanStartsInvalid) {
  // Arrange -- the tree created by SetUp() has an empty single leaf root
  auto txn = tm_->Begin();

  // Act -- begin a descending full scan on the empty tree
  BPlusTreeIterator it = bpt_->Begin(txn, "", "", false);

  // Assert -- no rows exist, so the descending iterator is born invalid
  EXPECT_FALSE(it.IsValid());
}

TEST_F(BPlusTreeIteratorTest, KeyAndValueAccessAcrossFosterBoundary) {
  // Arrange -- three exclusive rows that each own a leaf; the forward scan
  // hops a->b->c across two foster links
  constexpr static int kExclusiveValue = 6000;
  auto txn = tm_->Begin();
  Insert(txn, 'a', 1, kExclusiveValue);
  Insert(txn, 'b', 1, kExclusiveValue);
  Insert(txn, 'c', 1, kExclusiveValue);

  // Act -- walk forward while reading both payloads on every page
  BPlusTreeIterator it = bpt_->Begin(txn);
  for (const auto& c : {'a', 'b', 'c'}) {
    SCOPED_TRACE(c);
    ASSERT_TRUE(it.IsValid());
    ASSERT_EQ(it.Key(), std::string(1, c));
    ASSERT_EQ(it.Value(), std::string(kExclusiveValue, c));
    ++it;
  }

  // Assert -- the scan is exhausted after the last foster page
  EXPECT_FALSE(it.IsValid());
}

TEST_F(BPlusTreeIteratorTest, DescendingScanStartsAtNonLastIndex) {
  // Arrange -- one leaf holds two shared rows so the rightmost page is not a
  // single-row page (idx_ starts at row_count_-1, not 0)
  auto txn = tm_->Begin();
  Insert(txn, 'a', 1000, 100);
  Insert(txn, 'b', 1000, 100);
  Insert(txn, 'c', 1000, 100);
  Insert(txn, 'd', 1000, 100);

  // Act -- walk backwards over the shared rightmost leaf
  BPlusTreeIterator it = bpt_->Begin(txn, "", "", false);
  ASSERT_TRUE(it.IsValid());
  ASSERT_EQ(it.Value(), std::string(100, 'd'));
  --it;
  ASSERT_TRUE(it.IsValid());
  ASSERT_EQ(it.Value(), std::string(100, 'c'));
  --it;
  ASSERT_TRUE(it.IsValid());
  ASSERT_EQ(it.Value(), std::string(100, 'b'));
  --it;
  ASSERT_TRUE(it.IsValid());
  ASSERT_EQ(it.Value(), std::string(100, 'a'));
  --it;

  // Assert -- the scan ends at the smallest key
  EXPECT_FALSE(it.IsValid());
}

TEST_F(BPlusTreeIteratorTest, SeekStartsAtFirstKeyGreaterOrEqual) {
  // Arrange -- insert a..g; the leaf holds keys with 1000-byte prefix
  auto txn = tm_->Begin();
  for (const auto& c : {'a', 'b', 'c', 'd', 'e', 'f', 'g'}) {
    Insert(txn, c, 1000, 100);
  }

  // Act -- a range whose begin key is absent between "b" and "c" must start
  // at the first stored key that sorts at or after the bound
  const std::string between = std::string(999, 'b') + "~";
  BPlusTreeIterator it = bpt_->Begin(txn, between, "");
  ASSERT_TRUE(it.IsValid());
  ASSERT_EQ(it.Value(), std::string(100, 'c'));
  ++it;
  ASSERT_TRUE(it.IsValid());
  ASSERT_EQ(it.Value(), std::string(100, 'd'));

  // A begin key past every stored key yields an exhausted iterator
  BPlusTreeIterator past = bpt_->Begin(txn, std::string(1000, 'z'), "");
  EXPECT_FALSE(past.IsValid());
}

TEST_F(BPlusTreeIteratorTest, BoundedAscendingStopsBeforeMissingUpperBound) {
  // Arrange -- insert a..g
  auto txn = tm_->Begin();
  for (const auto& c : {'a', 'b', 'c', 'd', 'e', 'f', 'g'}) {
    Insert(txn, c, 1000, 100);
  }

  // Act -- scan [a, x) where "x" is not stored; the scan must stop once the
  // next stored key sorts after the upper bound
  const std::string upper = std::string(999, 'f') + "~";
  BPlusTreeIterator it = bpt_->Begin(txn, "a", upper);
  for (const auto& c : {'a', 'b', 'c', 'd', 'e', 'f'}) {
    SCOPED_TRACE(c);
    ASSERT_TRUE(it.IsValid());
    ASSERT_EQ(it.Value(), std::string(100, c));
    ++it;
  }
  EXPECT_FALSE(it.IsValid());
}

}  // namespace tinylamb
