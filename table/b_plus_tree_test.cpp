//
// Created by kumagi on 2022/02/02.
//

#include "b_plus_tree.hpp"

#include <memory>
#include <string>

#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

class BPlusTreeTest : public ::testing::Test {
 protected:
  static constexpr char kDBFileName[] = "b_plus_tree_test.db";
  static constexpr char kLogName[] = "b_plus_tree_test.log";

 public:
  void SetUp() override {
    Recover();
    auto txn = tm_->Begin();
    PageRef page_ = p_->AllocateNewPage(txn, PageType::kLeafPage);
    root_page_id_ = page_->PageID();
    EXPECT_SUCCESS(txn.PreCommit());
  }

  void Recover() {
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
  std::unique_ptr<BPlusTree> bpt_;
  page_id_t root_page_id_;
};

TEST_F(BPlusTreeTest, Construct) {}

TEST_F(BPlusTreeTest, ConstructBPlusTree) {
  bpt_ = std::make_unique<BPlusTree>(root_page_id_, p_.get());
}

TEST_F(BPlusTreeTest, InsertLeaf) {
  bpt_ = std::make_unique<BPlusTree>(root_page_id_, p_.get());
  auto txn = tm_->Begin();
  ASSERT_SUCCESS(bpt_->Insert(txn, "hello", "world"));
  ASSERT_SUCCESS(bpt_->Insert(txn, "this", "is a pen"));
  ASSERT_SUCCESS(bpt_->Insert(txn, "lorem", "ipsum"));
  ASSERT_SUCCESS(bpt_->Insert(txn, "foo", "bar"));
  ASSERT_SUCCESS(bpt_->Insert(txn, "key", "blar"));
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(BPlusTreeTest, SplitLeaf) {
  bpt_ = std::make_unique<BPlusTree>(root_page_id_, p_.get());
  auto txn = tm_->Begin();
  std::string key_prefix("key");
  std::string long_value(8000, 'v');
  for (int i = 0; i < 100; ++i) {
    ASSERT_SUCCESS(
        bpt_->Insert(txn, key_prefix + std::to_string(i), long_value));
  }
  bpt_->Dump(txn, std::cout);
}

}  // namespace tinylamb