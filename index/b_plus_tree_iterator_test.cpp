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

#include <filesystem>

#include "b_plus_tree.hpp"
#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "page/page.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
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
      p_->GetPool()->LostAllPageForTest();
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
    std::remove(db_name_.c_str());
    std::remove(log_name_.c_str());
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

TEST_F(BPlusTreeIteratorTest, Construct) {}
TEST_F(BPlusTreeIteratorTest, FullScan) {
  auto txn = tm_->Begin();
  for (const auto& c : {'a', 'b', 'c', 'd', 'e', 'f', 'g'}) {
    Insert(txn, c, 1000, 100);
  }
  BPlusTreeIterator it = bpt_->Begin(txn);
  for (const auto& c : {'a', 'b', 'c', 'd', 'e', 'f', 'g'}) {
    EXPECT_TRUE(it.IsValid());
    EXPECT_EQ(it.Value(), std::string(100, c));
    ++it;
  }

  EXPECT_FALSE(it.IsValid());
}

TEST_F(BPlusTreeIteratorTest, RangeAcending) {
  auto txn = tm_->Begin();
  for (const auto& c : {'a', 'b', 'c', 'd', 'e', 'f', 'g'}) {
    Insert(txn, c, 1000, 100);
  }
  BPlusTreeIterator it = bpt_->Begin(txn, "b", "d");
  EXPECT_TRUE(it.IsValid());

  EXPECT_EQ(it.Value(), std::string(100, 'b'));
  ++it;
  EXPECT_TRUE(it.IsValid());

  EXPECT_EQ(it.Value(), std::string(100, 'c'));
  ++it;
  EXPECT_FALSE(it.IsValid());
}

TEST_F(BPlusTreeIteratorTest, RangeDescending) {
  auto txn = tm_->Begin();
  for (const auto& c : {'a', 'b', 'c', 'd', 'e', 'f', 'g'}) {
    Insert(txn, c, 1000, 100);
  }
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
  EXPECT_FALSE(it.IsValid());
}

TEST_F(BPlusTreeIteratorTest, RangeDescendingRightOpen) {
  auto txn = tm_->Begin();
  for (const auto& c : {'a', 'b', 'c', 'd', 'e', 'f', 'g'}) {
    Insert(txn, c, 1000, 100);
  }
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
  EXPECT_FALSE(it.IsValid());
}

TEST_F(BPlusTreeIteratorTest, FullScanMultiLeaf) {
  constexpr int kSize = 2723;
  auto txn = tm_->Begin();
  for (const auto& c : {'1', '2', '3', '4', '5', '6', '7', '8', '9'}) {
    Insert(txn, c, kSize, kSize);
  }
  bpt_->Dump(txn, std::cerr);
  BPlusTreeIterator it = bpt_->Begin(txn);
  for (const auto& c : {'1', '2', '3', '4', '5', '6', '7', '8', '9'}) {
    SCOPED_TRACE(c);
    bpt_->Dump(txn, std::cerr);
    ASSERT_TRUE(it.IsValid());
    ASSERT_EQ(it.Value(), std::string(kSize, c));
    ++it;
  }

  EXPECT_FALSE(it.IsValid());
}

TEST_F(BPlusTreeIteratorTest, FullScanMultiLeafRecovery) {
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
    bpt_->Dump(txn, std::cerr);
  }
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
    EXPECT_FALSE(it.IsValid());
  }
}

TEST_F(BPlusTreeIteratorTest, FullScanReverse) {
  constexpr int kSize = 2000;
  auto txn = tm_->Begin();
  for (const auto& c :
       {'k', 'j', 'i', 'h', 'g', 'f', 'e', 'd', 'c', 'b', 'a'}) {
    Insert(txn, c, kSize, kSize);
  }
  BPlusTreeIterator it = bpt_->Begin(txn);
  for (const auto& c :
       {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k'}) {
    SCOPED_TRACE(c);
    EXPECT_TRUE(it.IsValid());
    ASSERT_EQ(it.Value(), std::string(kSize, c));
    ++it;
  }
  EXPECT_FALSE(it.IsValid());
}

TEST_F(BPlusTreeIteratorTest, EndOpenFullScanReverse) {
  constexpr int kSize = 2000;
  auto txn = tm_->Begin();
  for (const auto& c :
       {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k'}) {
    Insert(txn, c, kSize, kSize);
  }
  BPlusTreeIterator it = bpt_->Begin(txn, "", "", false);
  bpt_->Dump(txn, std::cerr);
  for (const auto& c :
       {'k', 'j', 'i', 'h', 'g', 'f', 'e', 'd', 'c', 'b', 'a'}) {
    SCOPED_TRACE(c);
    EXPECT_TRUE(it.IsValid());
    ASSERT_EQ(it.Value(), std::string(kSize, c));
    --it;
  }
  EXPECT_FALSE(it.IsValid());
}

}  // namespace tinylamb