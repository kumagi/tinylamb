//
// Created by kumagi on 2022/01/26.
//

#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "page/leaf_page.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "page/page_type.hpp"
#include "recovery/logger.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

class InternalPageTest : public ::testing::Test {
 protected:
  static constexpr char kDBFileName[] = "internal_page_test.db";
  static constexpr char kLogName[] = "internal_page_test.log";

  void SetUp() override {
    Recover();
    auto txn = tm_->Begin();
    PageRef page_ = p_->AllocateNewPage(txn, PageType::kInternalPage);
    internal_page_id_ = page_->PageID();
    EXPECT_TRUE(txn.PreCommit());
  }

  void Flush() { p_->GetPool()->FlushPageForTest(internal_page_id_); }

  virtual void Recover() {
    if (p_) {
      p_->GetPool()->LostAllPageForTest();
    }
    tm_.reset();
    lm_.reset();
    l_.reset();
    p_.reset();
    p_ = std::make_unique<PageManager>(kDBFileName, 10);
    l_ = std::make_unique<Logger>(kLogName);
    lm_ = std::make_unique<LockManager>();
    tm_ = std::make_unique<TransactionManager>(lm_.get(), l_.get(), nullptr);
  }

  void TearDown() override {
    std::remove(kDBFileName);
    std::remove(kLogName);
  }

  std::unique_ptr<LockManager> lm_;
  std::unique_ptr<PageManager> p_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<TransactionManager> tm_;
  page_id_t internal_page_id_;
};

TEST_F(InternalPageTest, Construct) {}
TEST_F(InternalPageTest, SetTree) {
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(internal_page_id_);
  page->SetTree(txn, "b", 100, 200);
}

TEST_F(InternalPageTest, GetPageForKeyMinimum) {
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(internal_page_id_);
  page->SetTree(txn, "b", 100, 200);
  page_id_t pid;
  ASSERT_TRUE(page->GetPageForKey(txn, "alpha", &pid));
  ASSERT_EQ(pid, 100);
  ASSERT_TRUE(page->GetPageForKey(txn, "b", &pid));
  ASSERT_EQ(pid, 200);
  ASSERT_TRUE(page->GetPageForKey(txn, "delta", &pid));
  ASSERT_EQ(pid, 200);
}

TEST_F(InternalPageTest, InsertKey) {
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(internal_page_id_);
  page->SetTree(txn, "d", 100, 200);
  ASSERT_TRUE(page->Insert(txn, "a", 10));
  ASSERT_TRUE(page->Insert(txn, "b", 20));
  ASSERT_TRUE(page->Insert(txn, "e", 40));
  ASSERT_TRUE(page->Insert(txn, "f", 50));
  ASSERT_TRUE(page->Insert(txn, "g", 60));
  ASSERT_TRUE(page->Insert(txn, "c", 30));
}

TEST_F(InternalPageTest, GetPageForKey) {
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(internal_page_id_);
  page->SetTree(txn, "c", 2, 23);
  ASSERT_TRUE(page->Insert(txn, "b", 20));
  ASSERT_TRUE(page->Insert(txn, "e", 40));

  page_id_t pid;
  ASSERT_TRUE(page->GetPageForKey(txn, "alpha", &pid));
  ASSERT_EQ(pid, 2);
  ASSERT_TRUE(page->GetPageForKey(txn, "b", &pid));
  EXPECT_EQ(pid, 20);
  ASSERT_TRUE(page->GetPageForKey(txn, "c", &pid));
  EXPECT_EQ(pid, 23);
  ASSERT_TRUE(page->GetPageForKey(txn, "zeta", &pid));
  EXPECT_EQ(pid, 40);
}

}  // namespace tinylamb