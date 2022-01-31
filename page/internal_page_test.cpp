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

  void AssertPIDForKey(page_id_t pid, std::string_view key,
                       page_id_t expected) {
    auto txn = tm_->Begin();
    PageRef p = p_->GetPage(pid);

    page_id_t value;
    ASSERT_TRUE(p->GetPageForKey(txn, key, &value));
    ASSERT_EQ(value, expected);
    EXPECT_TRUE(txn.PreCommit());
  }

  virtual void Recover() {
    if (p_) {
      p_->GetPool()->LostAllPageForTest();
    }
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
    // r_->RecoverFrom(0, tm_.get());
  }

  void TearDown() override {
    std::remove(kDBFileName);
    std::remove(kLogName);
  }

  std::unique_ptr<LockManager> lm_;
  std::unique_ptr<PageManager> p_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<RecoveryManager> r_;
  std::unique_ptr<TransactionManager> tm_;
  page_id_t internal_page_id_;
};

TEST_F(InternalPageTest, Construct) {}
TEST_F(InternalPageTest, SetMinimumTree) {
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(internal_page_id_);
  page->SetLowestValue(txn, 100);
  ASSERT_TRUE(page->Insert(txn, "b", 200));
}

TEST_F(InternalPageTest, GetPageForKeyMinimum) {
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(internal_page_id_);
    page->SetLowestValue(txn, 100);
    ASSERT_TRUE(page->Insert(txn, "b", 200));
    txn.PreCommit();
  }
  AssertPIDForKey(internal_page_id_, "alpha", 100);
  AssertPIDForKey(internal_page_id_, "b", 200);
  AssertPIDForKey(internal_page_id_, "delta", 200);
}

TEST_F(InternalPageTest, InsertKey) {
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(internal_page_id_);
  page->SetLowestValue(txn, 100);
  ASSERT_TRUE(page->Insert(txn, "d", 200));
  ASSERT_TRUE(page->Insert(txn, "a", 10));
  ASSERT_TRUE(page->Insert(txn, "b", 20));
  ASSERT_TRUE(page->Insert(txn, "e", 40));
  ASSERT_TRUE(page->Insert(txn, "f", 50));
  ASSERT_TRUE(page->Insert(txn, "g", 60));
  ASSERT_TRUE(page->Insert(txn, "c", 30));
}

TEST_F(InternalPageTest, GetPageForKey) {
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(internal_page_id_);
    page->SetLowestValue(txn, 2);

    ASSERT_TRUE(page->Insert(txn, "c", 23));
    ASSERT_TRUE(page->Insert(txn, "b", 20));
    ASSERT_TRUE(page->Insert(txn, "e", 40));
    ASSERT_TRUE(txn.PreCommit());
  }

  AssertPIDForKey(internal_page_id_, "alpha", 2);
  AssertPIDForKey(internal_page_id_, "b", 20);
  AssertPIDForKey(internal_page_id_, "c", 23);
  AssertPIDForKey(internal_page_id_, "zeta", 40);
}

TEST_F(InternalPageTest, InsertAndGetKey) {
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(internal_page_id_);
  page->SetLowestValue(txn, 100);

  page_id_t pid;
  ASSERT_TRUE(page->Insert(txn, "c", 200));
  ASSERT_TRUE(page->Insert(txn, "a", 10));
  ASSERT_TRUE(page->GetPageForKey(txn, "a", &pid));
  ASSERT_TRUE(page->GetPageForKey(txn, "alpha", &pid));
  ASSERT_EQ(pid, 10);

  ASSERT_TRUE(page->Insert(txn, "g", 60));
  ASSERT_TRUE(page->GetPageForKey(txn, "g", &pid));
  ASSERT_EQ(pid, 60);
  ASSERT_TRUE(page->GetPageForKey(txn, "guide", &pid));
  ASSERT_EQ(pid, 60);

  ASSERT_TRUE(page->Insert(txn, "e", 40));
  ASSERT_TRUE(page->GetPageForKey(txn, "e", &pid));
  ASSERT_EQ(pid, 40);
  ASSERT_TRUE(page->GetPageForKey(txn, "error", &pid));
  ASSERT_EQ(pid, 40);

  ASSERT_TRUE(page->Insert(txn, "f", 50));
  ASSERT_TRUE(page->GetPageForKey(txn, "f", &pid));
  ASSERT_EQ(pid, 50);
  ASSERT_TRUE(page->GetPageForKey(txn, "flight", &pid));
  ASSERT_EQ(pid, 50);

  ASSERT_TRUE(page->Insert(txn, "b", 20));
  ASSERT_TRUE(page->GetPageForKey(txn, "b", &pid));
  ASSERT_EQ(pid, 20);
  ASSERT_TRUE(page->GetPageForKey(txn, "battle", &pid));
  ASSERT_EQ(pid, 20);

  ASSERT_TRUE(page->Insert(txn, "c", 30));
  ASSERT_TRUE(page->GetPageForKey(txn, "c", &pid));
  ASSERT_EQ(pid, 30);
  ASSERT_TRUE(page->GetPageForKey(txn, "cut", &pid));
  ASSERT_EQ(pid, 30);
}

TEST_F(InternalPageTest, UpdateKey) {
  auto txn = tm_->Begin();
  {
    PageRef page = p_->GetPage(internal_page_id_);
    page->SetLowestValue(txn, 100);
    ASSERT_TRUE(page->Insert(txn, "a", 1));
    ASSERT_TRUE(page->Insert(txn, "b", 2));
    ASSERT_TRUE(page->Insert(txn, "c", 3));
    ASSERT_TRUE(page->Insert(txn, "d", 4));
    ASSERT_TRUE(page->Update(txn, "a", 5));
    ASSERT_TRUE(page->Update(txn, "b", 6));
    ASSERT_TRUE(page->Update(txn, "c", 7));
    ASSERT_TRUE(page->Update(txn, "d", 8));
    ASSERT_FALSE(page->Update(txn, "e", 60));
    ASSERT_FALSE(page->Update(txn, "f", 30));
    txn.PreCommit();
  }

  AssertPIDForKey(internal_page_id_, "a", 5);
  AssertPIDForKey(internal_page_id_, "b", 6);
  AssertPIDForKey(internal_page_id_, "c", 7);
  AssertPIDForKey(internal_page_id_, "d", 8);
}

TEST_F(InternalPageTest, DeleteKey) {
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(internal_page_id_);
  page->SetLowestValue(txn, 2);
  ASSERT_TRUE(page->Insert(txn, "c", 23));
  ASSERT_TRUE(page->Insert(txn, "b", 20));
  ASSERT_TRUE(page->Insert(txn, "e", 40));

  page_id_t pid;
  ASSERT_TRUE(page->GetPageForKey(txn, "alpha", &pid));
  ASSERT_EQ(pid, 2);
  ASSERT_TRUE(page->Delete(txn, "b"));
  ASSERT_TRUE(page->GetPageForKey(txn, "b", &pid));
  EXPECT_EQ(pid, 2);
  ASSERT_TRUE(page->Delete(txn, "e"));
  ASSERT_TRUE(page->GetPageForKey(txn, "e", &pid));
  EXPECT_EQ(pid, 23);
}

TEST_F(InternalPageTest, SplitInto) {
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(internal_page_id_);
  page->SetLowestValue(txn, 1);
  ASSERT_TRUE(page->Insert(txn, "a", 2));
  ASSERT_TRUE(page->Insert(txn, "b", 3));
  ASSERT_TRUE(page->Insert(txn, "c", 4));
  ASSERT_TRUE(page->Insert(txn, "d", 5));
  ASSERT_TRUE(page->Insert(txn, "e", 6));
  ASSERT_TRUE(page->Insert(txn, "f", 7));
  ASSERT_TRUE(page->Insert(txn, "g", 8));
  ASSERT_TRUE(page->Insert(txn, "h", 9));
  ASSERT_TRUE(page->Insert(txn, "i", 10));
  PageRef right = p_->AllocateNewPage(txn, PageType::kInternalPage);
  std::string_view mid;

  page->SplitInto(txn, right.get(), &mid);
  ASSERT_EQ("e", mid);

  page_id_t pid;
  ASSERT_TRUE(page->GetPageForKey(txn, "a", &pid));
  ASSERT_EQ(pid, 2);
  ASSERT_TRUE(page->GetPageForKey(txn, "b", &pid));
  EXPECT_EQ(pid, 3);
  ASSERT_TRUE(page->GetPageForKey(txn, "c", &pid));
  EXPECT_EQ(pid, 4);
  ASSERT_TRUE(page->GetPageForKey(txn, "d", &pid));
  EXPECT_EQ(pid, 5);
  ASSERT_TRUE(page->GetPageForKey(txn, "e", &pid));
  ASSERT_EQ(pid, 5);  // Migrated into right node.
  ASSERT_TRUE(page->GetPageForKey(txn, "f", &pid));
  EXPECT_EQ(pid, 5);
  ASSERT_TRUE(page->GetPageForKey(txn, "g", &pid));
  EXPECT_EQ(pid, 5);

  ASSERT_TRUE(right->GetPageForKey(txn, "a", &pid));
  ASSERT_EQ(pid, 6);  // Minimum value of this node.
  ASSERT_TRUE(right->GetPageForKey(txn, "e", &pid));
  ASSERT_EQ(pid, 6);
  ASSERT_TRUE(right->GetPageForKey(txn, "f", &pid));
  EXPECT_EQ(pid, 7);
  ASSERT_TRUE(right->GetPageForKey(txn, "g", &pid));
  EXPECT_EQ(pid, 8);
  ASSERT_TRUE(right->GetPageForKey(txn, "h", &pid));
  EXPECT_EQ(pid, 9);
  ASSERT_TRUE(right->GetPageForKey(txn, "i", &pid));
  EXPECT_EQ(pid, 10);
}

TEST_F(InternalPageTest, Recovery) {
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(internal_page_id_);
    page->SetLowestValue(txn, 2);

    ASSERT_TRUE(page->Insert(txn, "c", 23));
    ASSERT_TRUE(page->Insert(txn, "b", 20));
    ASSERT_TRUE(page->Insert(txn, "e", 40));
    txn.PreCommit();
  }

  Recover();  // Expect redo happen.
  r_->RecoverFrom(0, tm_.get());

  AssertPIDForKey(internal_page_id_, "alpha", 2);
  AssertPIDForKey(internal_page_id_, "b", 20);
  AssertPIDForKey(internal_page_id_, "c", 23);
  AssertPIDForKey(internal_page_id_, "zeta", 40);
}

TEST_F(InternalPageTest, InsertCrash) {
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(internal_page_id_);
    page->SetLowestValue(txn, 2);

    Flush();
    ASSERT_TRUE(page->Insert(txn, "c", 23));
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(internal_page_id_);
    ASSERT_TRUE(page->Insert(txn, "b", 20));
    ASSERT_TRUE(page->Insert(txn, "e", 40));
  }

  Recover();  // Expect redo happen.
  r_->RecoverFrom(0, tm_.get());

  AssertPIDForKey(internal_page_id_, "alpha", 2);
  AssertPIDForKey(internal_page_id_, "b", 2);
  AssertPIDForKey(internal_page_id_, "c", 23);
  AssertPIDForKey(internal_page_id_, "zeta", 23);
}

TEST_F(InternalPageTest, InsertAbort) {
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(internal_page_id_);
    page->SetLowestValue(txn, 2);

    ASSERT_TRUE(page->Insert(txn, "c", 23));
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    {
      PageRef page = p_->GetPage(internal_page_id_);
      ASSERT_TRUE(page->Insert(txn, "b", 20));
      ASSERT_TRUE(page->Insert(txn, "e", 40));
    }
    Flush();
    txn.Abort();
  }

  Recover();  // Expect redo happen.
  r_->RecoverFrom(0, tm_.get());

  AssertPIDForKey(internal_page_id_, "alpha", 2);
  AssertPIDForKey(internal_page_id_, "b", 2);
  AssertPIDForKey(internal_page_id_, "c", 23);
  AssertPIDForKey(internal_page_id_, "zeta", 23);
}

TEST_F(InternalPageTest, UpdateCrash) {
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(internal_page_id_);
    page->SetLowestValue(txn, 2);
    ASSERT_TRUE(page->Insert(txn, "c", 23));
    ASSERT_TRUE(page->Insert(txn, "b", 20));
    ASSERT_TRUE(page->Insert(txn, "e", 40));
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(internal_page_id_);
    ASSERT_TRUE(page->Update(txn, "b", 200));
    ASSERT_TRUE(page->Update(txn, "e", 400));
    txn.PreCommit();
    Flush();
  }

  Recover();  // Expect redo happen.
  r_->RecoverFrom(0, tm_.get());

  AssertPIDForKey(internal_page_id_, "alpha", 2);
  AssertPIDForKey(internal_page_id_, "b", 200);
  AssertPIDForKey(internal_page_id_, "c", 23);
  AssertPIDForKey(internal_page_id_, "zeta", 400);
}

TEST_F(InternalPageTest, UpdateAbort) {
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(internal_page_id_);
    page->SetLowestValue(txn, 2);
    ASSERT_TRUE(page->Insert(txn, "c", 23));
    ASSERT_TRUE(page->Insert(txn, "b", 20));
    ASSERT_TRUE(page->Insert(txn, "e", 40));
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    {
      PageRef page = p_->GetPage(internal_page_id_);
      ASSERT_TRUE(page->Update(txn, "b", 2000));
      ASSERT_TRUE(page->Update(txn, "e", 4000));
    }
    txn.Abort();
  }

  Recover();  // Expect redo happen.
  r_->RecoverFrom(0, tm_.get());

  AssertPIDForKey(internal_page_id_, "alpha", 2);
  AssertPIDForKey(internal_page_id_, "b", 20);
  AssertPIDForKey(internal_page_id_, "c", 23);
  AssertPIDForKey(internal_page_id_, "zeta", 40);
}

TEST_F(InternalPageTest, DeleteCrash) {
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(internal_page_id_);
    page->SetLowestValue(txn, 2);
    ASSERT_TRUE(page->Insert(txn, "b", 20));
    ASSERT_TRUE(page->Insert(txn, "e", 40));
    ASSERT_TRUE(page->Insert(txn, "c", 23));
    Flush();
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(internal_page_id_);
    ASSERT_TRUE(page->Delete(txn, "b"));
    ASSERT_TRUE(page->Delete(txn, "e"));
  }

  Recover();  // Expect redo happen.
  r_->RecoverFrom(0, tm_.get());

  AssertPIDForKey(internal_page_id_, "alpha", 2);
  AssertPIDForKey(internal_page_id_, "b", 20);
  AssertPIDForKey(internal_page_id_, "c", 23);
  AssertPIDForKey(internal_page_id_, "zeta", 40);
}

TEST_F(InternalPageTest, DeleteAbort) {
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(internal_page_id_);
    page->SetLowestValue(txn, 2);
    ASSERT_TRUE(page->Insert(txn, "b", 20));
    ASSERT_TRUE(page->Insert(txn, "e", 40));
    ASSERT_TRUE(page->Insert(txn, "c", 23));
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    {
      PageRef page = p_->GetPage(internal_page_id_);
      ASSERT_TRUE(page->Delete(txn, "b"));
      ASSERT_TRUE(page->Delete(txn, "e"));
    }
    txn.Abort();
    Flush();
  }

  Recover();  // Expect redo happen.
  r_->RecoverFrom(0, tm_.get());

  AssertPIDForKey(internal_page_id_, "alpha", 2);
  AssertPIDForKey(internal_page_id_, "b", 20);
  AssertPIDForKey(internal_page_id_, "c", 23);
  AssertPIDForKey(internal_page_id_, "zeta", 40);
}

}  // namespace tinylamb