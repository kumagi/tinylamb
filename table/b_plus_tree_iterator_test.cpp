//
// Created by kumagi on 2022/02/10.
//

#include "b_plus_tree_iterator.hpp"

#include "b_plus_tree.hpp"
#include "common/constants.hpp"
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
 protected:
  static constexpr char kDBFileName[] = "b_plus_tree_test.db";
  static constexpr char kLogName[] = "b_plus_tree_test.log";
  static constexpr char kMasterRecordName[] = "b_plus_tree_master.log";

 public:
  void SetUp() override {
    Recover();
    auto txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kLeafPage);
    ASSERT_EQ(page->PageID(), 1);
    EXPECT_SUCCESS(txn.PreCommit());
  }

  void Init(Transaction& txn, const char c, size_t key_len,
            size_t value_len) const {
    ASSERT_SUCCESS(
        bpt_->Insert(txn, std::string(key_len, c), std::string(value_len, c)));
  }

  void Flush(page_id_t pid) { p_->GetPool()->FlushPageForTest(pid); }

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
    p_ = std::make_unique<PageManager>(kDBFileName, 10);
    l_ = std::make_unique<Logger>(kLogName);
    lm_ = std::make_unique<LockManager>();
    r_ = std::make_unique<RecoveryManager>(kLogName, p_->GetPool());
    tm_ = std::make_unique<TransactionManager>(lm_.get(), l_.get(), r_.get());
    cm_ = std::make_unique<CheckpointManager>(kMasterRecordName, tm_.get(),
                                              p_->GetPool(), 1);
    bpt_ = std::make_unique<BPlusTree>(root, p_.get());
  }

  void TearDown() override {
    bpt_.reset();
    tm_.reset();
    r_.reset();
    lm_.reset();
    l_.reset();
    p_.reset();
    std::remove(kDBFileName);
    std::remove(kLogName);
  }

 public:
  std::unique_ptr<LockManager> lm_;
  std::unique_ptr<PageManager> p_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<RecoveryManager> r_;
  std::unique_ptr<TransactionManager> tm_;
  std::unique_ptr<CheckpointManager> cm_;
  std::unique_ptr<BPlusTree> bpt_;
};

TEST_F(BPlusTreeIteratorTest, Construct) {}
TEST_F(BPlusTreeIteratorTest, FullScan) {
  auto txn = tm_->Begin();
  for (const auto& c : {'a', 'b', 'c', 'd', 'e', 'f', 'g'}) {
    Init(txn, c, 1000, 100);
  }
  BPlusTreeIterator it = bpt_->Begin(txn);
  for (const auto& c : {'a', 'b', 'c', 'd', 'e', 'f', 'g'}) {
    EXPECT_TRUE(it.IsValid());
    EXPECT_EQ(*it, std::string(100, c));
    ++it;
  }

  EXPECT_FALSE(it.IsValid());
}

TEST_F(BPlusTreeIteratorTest, RangeAcending) {
  auto txn = tm_->Begin();
  for (const auto& c : {'a', 'b', 'c', 'd', 'e', 'f', 'g'}) {
    Init(txn, c, 1000, 100);
  }
  BPlusTreeIterator it = bpt_->Begin(txn, "b", "d");
  EXPECT_TRUE(it.IsValid());

  EXPECT_EQ(*it, std::string(100, 'b'));
  ++it;
  EXPECT_TRUE(it.IsValid());

  EXPECT_EQ(*it, std::string(100, 'c'));
  ++it;
  EXPECT_FALSE(it.IsValid());
}

TEST_F(BPlusTreeIteratorTest, RangeDescending) {
  auto txn = tm_->Begin();
  for (const auto& c : {'a', 'b', 'c', 'd', 'e', 'f', 'g'}) {
    Init(txn, c, 1000, 100);
  }
  BPlusTreeIterator it = bpt_->Begin(txn, "", "d", false);
  EXPECT_EQ(*it, std::string(100, 'd'));
  --it;
  EXPECT_TRUE(it.IsValid());

  EXPECT_EQ(*it, std::string(100, 'c'));
  --it;
  EXPECT_TRUE(it.IsValid());

  EXPECT_EQ(*it, std::string(100, 'b'));
  --it;
  EXPECT_TRUE(it.IsValid());

  EXPECT_EQ(*it, std::string(100, 'a'));
  --it;
  EXPECT_FALSE(it.IsValid());
}

TEST_F(BPlusTreeIteratorTest, FullScanMultiLeaf) {
  auto txn = tm_->Begin();
  for (const auto& c : {'1', '2', '3', '4', '5', '6', '7'}) {
    Init(txn, c, 1000, 10000);
  }
  BPlusTreeIterator it = bpt_->Begin(txn);
  bpt_->Dump(txn, std::cerr);
  for (const auto& c : {'1', '2', '3', '4', '5', '6', '7'}) {
    SCOPED_TRACE(c);
    ASSERT_TRUE(it.IsValid());
    EXPECT_EQ(*it, std::string(10000, c));
    ++it;
  }
  EXPECT_FALSE(it.IsValid());
}

TEST_F(BPlusTreeIteratorTest, FullScanMultiInternal) {
  auto txn = tm_->Begin();
  for (const auto& c :
       {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k'}) {
    Init(txn, c, 10000, 2000);
  }
  BPlusTreeIterator it = bpt_->Begin(txn);
  bpt_->Dump(txn, std::cerr);
  for (const auto& c :
       {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k'}) {
    EXPECT_TRUE(it.IsValid());
    EXPECT_EQ(*it, std::string(2000, c));
    ++it;
  }
  EXPECT_FALSE(it.IsValid());
}

}  // namespace tinylamb