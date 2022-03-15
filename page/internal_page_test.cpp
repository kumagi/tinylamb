//
// Created by kumagi on 2022/01/26.
//
#include <memory>
#include <string>

#include "common/random_string.hpp"
#include "common/test_util.hpp"
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
  void SetUp() override {
    std::string prefix = "internal_page_test-" + RandomString();
    db_name_ = prefix + ".db";
    log_name_ = prefix + ".log";
    Recover();
    auto txn = tm_->Begin();
    PageRef page_ = p_->AllocateNewPage(txn, PageType::kInternalPage);
    internal_page_id_ = page_->PageID();
    EXPECT_SUCCESS(txn.PreCommit());
  }

  void Flush() { p_->GetPool()->FlushPageForTest(internal_page_id_); }

  void AssertPIDForKey(page_id_t pid, std::string_view key,
                       page_id_t expected) {
    auto txn = tm_->Begin();
    PageRef p = p_->GetPage(pid);

    page_id_t value;
    ASSERT_SUCCESS(p->GetPageForKey(txn, key, &value));
    ASSERT_EQ(value, expected);
    EXPECT_SUCCESS(txn.PreCommit());
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
    p_ = std::make_unique<PageManager>(db_name_, 10);
    l_ = std::make_unique<Logger>(log_name_);
    lm_ = std::make_unique<LockManager>();
    r_ = std::make_unique<RecoveryManager>(log_name_, p_->GetPool());
    tm_ = std::make_unique<TransactionManager>(lm_.get(), l_.get(), r_.get());
    // r_->RecoverFrom(0, tm_.get());
  }

  void TearDown() override {
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
  page_id_t internal_page_id_;
};

TEST_F(InternalPageTest, Construct) {}
TEST_F(InternalPageTest, SetMinimumTree) {
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(internal_page_id_);
  page->SetLowestValue(txn, 100);
  ASSERT_SUCCESS(page->Insert(txn, "b", 200));
}

TEST_F(InternalPageTest, GetPageForKeyMinimum) {
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(internal_page_id_);
    page->SetLowestValue(txn, 100);
    ASSERT_SUCCESS(page->Insert(txn, "b", 200));
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
  ASSERT_SUCCESS(page->Insert(txn, "d", 200));
  ASSERT_SUCCESS(page->Insert(txn, "a", 10));
  ASSERT_SUCCESS(page->Insert(txn, "b", 20));
  ASSERT_SUCCESS(page->Insert(txn, "e", 40));
  ASSERT_SUCCESS(page->Insert(txn, "f", 50));
  ASSERT_SUCCESS(page->Insert(txn, "g", 60));
  ASSERT_SUCCESS(page->Insert(txn, "c", 30));
}

TEST_F(InternalPageTest, GetPageForKey) {
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(internal_page_id_);
    page->SetLowestValue(txn, 2);

    ASSERT_SUCCESS(page->Insert(txn, "c", 23));
    ASSERT_SUCCESS(page->Insert(txn, "b", 20));
    ASSERT_SUCCESS(page->Insert(txn, "e", 40));
    ASSERT_SUCCESS(txn.PreCommit());
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
  ASSERT_SUCCESS(page->Insert(txn, "c", 200));
  ASSERT_SUCCESS(page->Insert(txn, "a", 10));
  ASSERT_SUCCESS(page->GetPageForKey(txn, "a", &pid));
  ASSERT_SUCCESS(page->GetPageForKey(txn, "alpha", &pid));
  ASSERT_EQ(pid, 10);

  ASSERT_SUCCESS(page->Insert(txn, "g", 60));
  ASSERT_SUCCESS(page->GetPageForKey(txn, "g", &pid));
  ASSERT_EQ(pid, 60);
  ASSERT_SUCCESS(page->GetPageForKey(txn, "guide", &pid));
  ASSERT_EQ(pid, 60);

  ASSERT_SUCCESS(page->Insert(txn, "e", 40));
  ASSERT_SUCCESS(page->GetPageForKey(txn, "e", &pid));
  ASSERT_EQ(pid, 40);
  ASSERT_SUCCESS(page->GetPageForKey(txn, "error", &pid));
  ASSERT_EQ(pid, 40);

  ASSERT_SUCCESS(page->Insert(txn, "f", 50));
  ASSERT_SUCCESS(page->GetPageForKey(txn, "f", &pid));
  ASSERT_EQ(pid, 50);
  ASSERT_SUCCESS(page->GetPageForKey(txn, "flight", &pid));
  ASSERT_EQ(pid, 50);

  ASSERT_SUCCESS(page->Insert(txn, "b", 20));
  ASSERT_SUCCESS(page->GetPageForKey(txn, "b", &pid));
  ASSERT_EQ(pid, 20);
  ASSERT_SUCCESS(page->GetPageForKey(txn, "battle", &pid));
  ASSERT_EQ(pid, 20);

  ASSERT_SUCCESS(page->Insert(txn, "c", 30));
  ASSERT_SUCCESS(page->GetPageForKey(txn, "c", &pid));
  ASSERT_EQ(pid, 30);
  ASSERT_SUCCESS(page->GetPageForKey(txn, "cut", &pid));
  ASSERT_EQ(pid, 30);
}

TEST_F(InternalPageTest, UpdateKey) {
  auto txn = tm_->Begin();
  {
    PageRef page = p_->GetPage(internal_page_id_);
    page->SetLowestValue(txn, 100);
    ASSERT_SUCCESS(page->Insert(txn, "a", 1));
    ASSERT_SUCCESS(page->Insert(txn, "b", 2));
    ASSERT_SUCCESS(page->Insert(txn, "c", 3));
    ASSERT_SUCCESS(page->Insert(txn, "d", 4));
    ASSERT_SUCCESS(page->Update(txn, "a", 5));
    ASSERT_SUCCESS(page->Update(txn, "b", 6));
    ASSERT_SUCCESS(page->Update(txn, "c", 7));
    ASSERT_SUCCESS(page->Update(txn, "d", 8));
    ASSERT_FAIL(page->Update(txn, "e", 60));
    ASSERT_FAIL(page->Update(txn, "f", 30));
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
  ASSERT_SUCCESS(page->Insert(txn, "c", 23));
  ASSERT_SUCCESS(page->Insert(txn, "b", 20));
  ASSERT_SUCCESS(page->Insert(txn, "e", 40));

  page_id_t pid;
  ASSERT_SUCCESS(page->GetPageForKey(txn, "alpha", &pid));
  ASSERT_EQ(pid, 2);
  ASSERT_SUCCESS(page->Delete(txn, "b"));
  ASSERT_SUCCESS(page->GetPageForKey(txn, "b", &pid));
  EXPECT_EQ(pid, 2);
  ASSERT_SUCCESS(page->Delete(txn, "e"));
  ASSERT_SUCCESS(page->GetPageForKey(txn, "e", &pid));
  EXPECT_EQ(pid, 23);
}

TEST_F(InternalPageTest, SplitInto) {
  auto txn = tm_->Begin();
  for (int i = 0; i < 8; ++i) {
    PageRef page = p_->AllocateNewPage(txn, PageType::kInternalPage);
    page->SetLowestValue(txn, 0);
    for (int j = 0; j < 8; ++j) {
      ASSERT_SUCCESS(page->Insert(txn, std::string(4000, '0' + j), j + 1));
    }
    LOG(TRACE) << *page;

    PageRef right = p_->AllocateNewPage(txn, PageType::kInternalPage);
    std::string mid;
    page->SplitInto(txn, std::string(4000, '0' + i), right.get(), &mid);
  }
}

TEST_F(InternalPageTest, Recovery) {
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(internal_page_id_);
    page->SetLowestValue(txn, 2);

    ASSERT_SUCCESS(page->Insert(txn, "c", 23));
    ASSERT_SUCCESS(page->Insert(txn, "b", 20));
    ASSERT_SUCCESS(page->Insert(txn, "e", 40));
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
    ASSERT_SUCCESS(page->Insert(txn, "c", 23));
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(internal_page_id_);
    ASSERT_SUCCESS(page->Insert(txn, "b", 20));
    ASSERT_SUCCESS(page->Insert(txn, "e", 40));
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

    ASSERT_SUCCESS(page->Insert(txn, "c", 23));
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    {
      PageRef page = p_->GetPage(internal_page_id_);
      ASSERT_SUCCESS(page->Insert(txn, "b", 20));
      ASSERT_SUCCESS(page->Insert(txn, "e", 40));
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
    ASSERT_SUCCESS(page->Insert(txn, "c", 23));
    ASSERT_SUCCESS(page->Insert(txn, "b", 20));
    ASSERT_SUCCESS(page->Insert(txn, "e", 40));
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(internal_page_id_);
    ASSERT_SUCCESS(page->Update(txn, "b", 200));
    ASSERT_SUCCESS(page->Update(txn, "e", 400));
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
    ASSERT_SUCCESS(page->Insert(txn, "c", 23));
    ASSERT_SUCCESS(page->Insert(txn, "b", 20));
    ASSERT_SUCCESS(page->Insert(txn, "e", 40));
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    {
      PageRef page = p_->GetPage(internal_page_id_);
      ASSERT_SUCCESS(page->Update(txn, "b", 2000));
      ASSERT_SUCCESS(page->Update(txn, "e", 4000));
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
    ASSERT_SUCCESS(page->Insert(txn, "b", 20));
    ASSERT_SUCCESS(page->Insert(txn, "e", 40));
    ASSERT_SUCCESS(page->Insert(txn, "c", 23));
    Flush();
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(internal_page_id_);
    ASSERT_SUCCESS(page->Delete(txn, "b"));
    ASSERT_SUCCESS(page->Delete(txn, "e"));
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
    ASSERT_SUCCESS(page->Insert(txn, "b", 20));
    ASSERT_SUCCESS(page->Insert(txn, "e", 40));
    ASSERT_SUCCESS(page->Insert(txn, "c", 23));
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    {
      PageRef page = p_->GetPage(internal_page_id_);
      ASSERT_SUCCESS(page->Delete(txn, "b"));
      ASSERT_SUCCESS(page->Delete(txn, "e"));
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