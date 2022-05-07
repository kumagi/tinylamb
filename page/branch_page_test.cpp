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

class BranchPageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string prefix = "branch_page_test-" + RandomString();
    db_name_ = prefix + ".db";
    log_name_ = prefix + ".log";
    Recover();
    auto txn = tm_->Begin();
    PageRef page_ = p_->AllocateNewPage(txn, PageType::kBranchPage);
    branch_page_id_ = page_->PageID();
    EXPECT_SUCCESS(txn.PreCommit());
  }

  void Flush() { p_->GetPool()->FlushPageForTest(branch_page_id_); }

  PageRef Page() { return p_->GetPage(branch_page_id_); }

  void AssertPIDForKey(std::string_view key, page_id_t expected) {
    auto txn = tm_->Begin();
    PageRef p = Page();
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
    tm_ = std::make_unique<TransactionManager>(lm_.get(), p_.get(), l_.get(),
                                               r_.get());
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
  page_id_t branch_page_id_{0};
};

TEST_F(BranchPageTest, Construct) {}
TEST_F(BranchPageTest, SetMinimumTree) {
  auto txn = tm_->Begin();
  PageRef page = Page();
  page->SetLowestValue(txn, 100);
  ASSERT_SUCCESS(page->InsertBranch(txn, "b", 200));
}

TEST_F(BranchPageTest, GetPageForKeyMinimum) {
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(branch_page_id_);
    page->SetLowestValue(txn, 100);
    ASSERT_SUCCESS(page->InsertBranch(txn, "b", 200));
    txn.PreCommit();
  }
  AssertPIDForKey("alpha", 100);
  AssertPIDForKey("b", 200);
  AssertPIDForKey("delta", 200);
}

TEST_F(BranchPageTest, InsertKey) {
  auto txn = tm_->Begin();
  PageRef page = Page();
  page->SetLowestValue(txn, 100);
  ASSERT_SUCCESS(page->InsertBranch(txn, "d", 200));
  ASSERT_SUCCESS(page->InsertBranch(txn, "a", 10));
  ASSERT_SUCCESS(page->InsertBranch(txn, "b", 20));
  ASSERT_SUCCESS(page->InsertBranch(txn, "e", 40));
  ASSERT_SUCCESS(page->InsertBranch(txn, "f", 50));
  ASSERT_SUCCESS(page->InsertBranch(txn, "g", 60));
  ASSERT_SUCCESS(page->InsertBranch(txn, "c", 30));
}

TEST_F(BranchPageTest, GetPageForKey) {
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    page->SetLowestValue(txn, 2);

    ASSERT_SUCCESS(page->InsertBranch(txn, "c", 23));
    ASSERT_SUCCESS(page->InsertBranch(txn, "b", 20));
    ASSERT_SUCCESS(page->InsertBranch(txn, "e", 40));
    ASSERT_SUCCESS(txn.PreCommit());
  }

  AssertPIDForKey("alpha", 2);
  AssertPIDForKey("b", 20);
  AssertPIDForKey("c", 23);
  AssertPIDForKey("zeta", 40);
}

TEST_F(BranchPageTest, InsertAndGetKey) {
  auto txn = tm_->Begin();
  PageRef page = Page();
  page->SetLowestValue(txn, 100);

  page_id_t pid;
  ASSERT_SUCCESS(page->InsertBranch(txn, "c", 200));
  ASSERT_SUCCESS(page->InsertBranch(txn, "a", 10));
  ASSERT_SUCCESS(page->GetPageForKey(txn, "a", &pid));
  ASSERT_SUCCESS(page->GetPageForKey(txn, "alpha", &pid));
  ASSERT_EQ(pid, 10);

  ASSERT_SUCCESS(page->InsertBranch(txn, "g", 60));
  ASSERT_SUCCESS(page->GetPageForKey(txn, "g", &pid));
  ASSERT_EQ(pid, 60);
  ASSERT_SUCCESS(page->GetPageForKey(txn, "guide", &pid));
  ASSERT_EQ(pid, 60);

  ASSERT_SUCCESS(page->InsertBranch(txn, "e", 40));
  ASSERT_SUCCESS(page->GetPageForKey(txn, "e", &pid));
  ASSERT_EQ(pid, 40);
  ASSERT_SUCCESS(page->GetPageForKey(txn, "error", &pid));
  ASSERT_EQ(pid, 40);

  ASSERT_SUCCESS(page->InsertBranch(txn, "f", 50));
  ASSERT_SUCCESS(page->GetPageForKey(txn, "f", &pid));
  ASSERT_EQ(pid, 50);
  ASSERT_SUCCESS(page->GetPageForKey(txn, "flight", &pid));
  ASSERT_EQ(pid, 50);

  ASSERT_SUCCESS(page->InsertBranch(txn, "b", 20));
  ASSERT_SUCCESS(page->GetPageForKey(txn, "b", &pid));
  ASSERT_EQ(pid, 20);
  ASSERT_SUCCESS(page->GetPageForKey(txn, "battle", &pid));
  ASSERT_EQ(pid, 20);

  ASSERT_SUCCESS(page->InsertBranch(txn, "c", 30));
  ASSERT_SUCCESS(page->GetPageForKey(txn, "c", &pid));
  ASSERT_EQ(pid, 30);
  ASSERT_SUCCESS(page->GetPageForKey(txn, "cut", &pid));
  ASSERT_EQ(pid, 30);
}

TEST_F(BranchPageTest, UpdateKey) {
  auto txn = tm_->Begin();
  {
    PageRef page = Page();
    page->SetLowestValue(txn, 100);
    ASSERT_SUCCESS(page->InsertBranch(txn, "a", 1));
    ASSERT_SUCCESS(page->InsertBranch(txn, "b", 2));
    ASSERT_SUCCESS(page->InsertBranch(txn, "c", 3));
    ASSERT_SUCCESS(page->InsertBranch(txn, "d", 4));
    ASSERT_SUCCESS(page->UpdateBranch(txn, "a", 5));
    ASSERT_SUCCESS(page->UpdateBranch(txn, "b", 6));
    ASSERT_SUCCESS(page->UpdateBranch(txn, "c", 7));
    ASSERT_SUCCESS(page->UpdateBranch(txn, "d", 8));
    ASSERT_FAIL(page->UpdateBranch(txn, "e", 60));
    ASSERT_FAIL(page->UpdateBranch(txn, "f", 30));
    txn.PreCommit();
  }

  AssertPIDForKey("a", 5);
  AssertPIDForKey("b", 6);
  AssertPIDForKey("c", 7);
  AssertPIDForKey("d", 8);
}

TEST_F(BranchPageTest, DeleteKey) {
  auto txn = tm_->Begin();
  PageRef page = Page();
  page->SetLowestValue(txn, 2);
  ASSERT_SUCCESS(page->InsertBranch(txn, "c", 23));
  ASSERT_SUCCESS(page->InsertBranch(txn, "b", 20));
  ASSERT_SUCCESS(page->InsertBranch(txn, "e", 40));

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

TEST_F(BranchPageTest, SplitInto) {
  auto txn = tm_->Begin();
  for (int i = 0; i < 8; ++i) {
    PageRef page = p_->AllocateNewPage(txn, PageType::kBranchPage);
    page->SetLowestValue(txn, 0);
    for (int j = 0; j < 8; ++j) {
      ASSERT_SUCCESS(
          page->InsertBranch(txn, std::string(4000, '0' + j), j + 1));
    }

    PageRef right = p_->AllocateNewPage(txn, PageType::kBranchPage);
    std::string mid;
    page->SplitInto(txn, std::string(4000, '0' + i), right.get(), &mid);
  }
}

TEST_F(BranchPageTest, Recovery) {
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    page->SetLowestValue(txn, 2);

    ASSERT_SUCCESS(page->InsertBranch(txn, "c", 23));
    ASSERT_SUCCESS(page->InsertBranch(txn, "b", 20));
    ASSERT_SUCCESS(page->InsertBranch(txn, "e", 40));
    txn.PreCommit();
  }

  Recover();  // Expect redo happen.
  r_->RecoverFrom(0, tm_.get());

  AssertPIDForKey("alpha", 2);
  AssertPIDForKey("b", 20);
  AssertPIDForKey("c", 23);
  AssertPIDForKey("zeta", 40);
}

TEST_F(BranchPageTest, InsertCrash) {
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    page->SetLowestValue(txn, 2);

    Flush();
    ASSERT_SUCCESS(page->InsertBranch(txn, "c", 23));
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    ASSERT_SUCCESS(page->InsertBranch(txn, "b", 20));
    ASSERT_SUCCESS(page->InsertBranch(txn, "e", 40));
  }

  Recover();  // Expect redo happen.
  r_->RecoverFrom(0, tm_.get());

  AssertPIDForKey("alpha", 2);
  AssertPIDForKey("b", 2);
  AssertPIDForKey("c", 23);
  AssertPIDForKey("zeta", 23);
}

TEST_F(BranchPageTest, InsertAbort) {
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    page->SetLowestValue(txn, 2);

    ASSERT_SUCCESS(page->InsertBranch(txn, "c", 23));
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    {
      PageRef page = Page();
      ASSERT_SUCCESS(page->InsertBranch(txn, "b", 20));
      ASSERT_SUCCESS(page->InsertBranch(txn, "e", 40));
    }
    Flush();
    txn.Abort();
  }

  Recover();  // Expect redo happen.
  r_->RecoverFrom(0, tm_.get());

  AssertPIDForKey("alpha", 2);
  AssertPIDForKey("b", 2);
  AssertPIDForKey("c", 23);
  AssertPIDForKey("zeta", 23);
}

TEST_F(BranchPageTest, UpdateCrash) {
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    page->SetLowestValue(txn, 2);
    ASSERT_SUCCESS(page->InsertBranch(txn, "c", 23));
    ASSERT_SUCCESS(page->InsertBranch(txn, "b", 20));
    ASSERT_SUCCESS(page->InsertBranch(txn, "e", 40));
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    ASSERT_SUCCESS(page->UpdateBranch(txn, "b", 200));
    ASSERT_SUCCESS(page->UpdateBranch(txn, "e", 400));
    txn.PreCommit();
    Flush();
  }

  Recover();  // Expect redo happen.
  r_->RecoverFrom(0, tm_.get());

  AssertPIDForKey("alpha", 2);
  AssertPIDForKey("b", 200);
  AssertPIDForKey("c", 23);
  AssertPIDForKey("zeta", 400);
}

TEST_F(BranchPageTest, UpdateAbort) {
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    page->SetLowestValue(txn, 2);
    ASSERT_SUCCESS(page->InsertBranch(txn, "c", 23));
    ASSERT_SUCCESS(page->InsertBranch(txn, "b", 20));
    ASSERT_SUCCESS(page->InsertBranch(txn, "e", 40));
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    {
      PageRef page = Page();
      ASSERT_SUCCESS(page->UpdateBranch(txn, "b", 2000));
      ASSERT_SUCCESS(page->UpdateBranch(txn, "e", 4000));
    }
    txn.Abort();
  }

  Recover();  // Expect redo happen.
  r_->RecoverFrom(0, tm_.get());

  AssertPIDForKey("alpha", 2);
  AssertPIDForKey("b", 20);
  AssertPIDForKey("c", 23);
  AssertPIDForKey("zeta", 40);
}

TEST_F(BranchPageTest, DeleteCrash) {
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    page->SetLowestValue(txn, 2);
    ASSERT_SUCCESS(page->InsertBranch(txn, "b", 20));
    ASSERT_SUCCESS(page->InsertBranch(txn, "e", 40));
    ASSERT_SUCCESS(page->InsertBranch(txn, "c", 23));
    Flush();
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    ASSERT_SUCCESS(page->Delete(txn, "b"));
    ASSERT_SUCCESS(page->Delete(txn, "e"));
  }

  Recover();  // Expect redo happen.
  r_->RecoverFrom(0, tm_.get());

  AssertPIDForKey("alpha", 2);
  AssertPIDForKey("b", 20);
  AssertPIDForKey("c", 23);
  AssertPIDForKey("zeta", 40);
}

TEST_F(BranchPageTest, DeleteAbort) {
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    page->SetLowestValue(txn, 2);
    ASSERT_SUCCESS(page->InsertBranch(txn, "b", 20));
    ASSERT_SUCCESS(page->InsertBranch(txn, "e", 40));
    ASSERT_SUCCESS(page->InsertBranch(txn, "c", 23));
    txn.PreCommit();
  }
  {
    auto txn = tm_->Begin();
    {
      PageRef page = Page();
      ASSERT_SUCCESS(page->Delete(txn, "b"));
      ASSERT_SUCCESS(page->Delete(txn, "e"));
    }
    txn.Abort();
    Flush();
  }

  Recover();  // Expect redo happen.
  r_->RecoverFrom(0, tm_.get());

  AssertPIDForKey("alpha", 2);
  AssertPIDForKey("b", 20);
  AssertPIDForKey("c", 23);
  AssertPIDForKey("zeta", 40);
}

TEST_F(BranchPageTest, UpdateHeavy) {
  std::mt19937 random(0);
  constexpr int kCount = 40;
  Transaction txn = tm_->Begin();
  std::vector<std::string> keys;
  std::unordered_map<std::string, page_id_t> kvp;
  keys.reserve(kCount);
  PageRef page = Page();
  page->SetLowestValue(txn, 999);
  for (int i = 0; i < kCount; ++i) {
    std::string key = RandomString((19937 * i) % 12 + 10);
    page_id_t value = random() % 10000;
    ASSERT_SUCCESS(page->InsertBranch(txn, key, value));
    keys.push_back(key);
    kvp.emplace(key, value);
  }
  for (int i = 0; i < kCount * 4; ++i) {
    {
      auto iter = kvp.begin();
      std::advance(iter, random() % kvp.size());
      ASSERT_SUCCESS(page->Delete(txn, iter->first));
      kvp.erase(iter);
    }
    std::string key = RandomString((19937 * i) % 32 + 100);
    page_id_t value = random() % 10000;
    ASSERT_SUCCESS(page->InsertBranch(txn, key, value));
    kvp[key] = value;
  }
  for (const auto& kv : kvp) {
    page_id_t val;
    page->GetPageForKey(txn, kv.first, &val);
    ASSERT_EQ(kvp[kv.first], val);
  }
}
}  // namespace tinylamb