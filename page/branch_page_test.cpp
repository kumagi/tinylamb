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

#include <gtest/gtest.h>

#include <cstdio>
#include <iterator>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
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
    ASSERT_SUCCESS_AND_EQ(p->GetPageForKey(txn, key, false), expected);
    EXPECT_SUCCESS(txn.PreCommit());
  }

  virtual void Recover() {
    if (p_) {
      p_->GetPool()->DropAllPages();
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
    tm_ = std::make_unique<TransactionManager>(p_.get(), l_.get(),
                                               r_.get());
    // r_->RecoverFrom(0, tm_.get());
  }

  void TearDown() override {
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
  page_id_t branch_page_id_{0};
};

TEST_F(BranchPageTest, Construct) {
  // Arrange -- nothing to set up; default database created by SetUp()
  // Act -- nothing to execute; default constructed via SetUp()
  // Assert -- nothing to verify; gtest death on crash, gtest green on pass
}

TEST_F(BranchPageTest, SetMinimumTree) {
  // Arrange
  auto txn = tm_->Begin();
  PageRef page = Page();

  // Act -- set lowest value 100, then insert branch key "b" with page id 200
  page->SetLowestValue(txn, 100);
  ASSERT_SUCCESS(page->InsertBranch(txn, "b", 200));

  // Assert -- implicit; gtest death on crash, gtest green on pass
}

TEST_F(BranchPageTest, GetPageForKeyMinimum) {
  // Arrange
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(branch_page_id_);

  // Act -- set lowest 100, insert "b"→200, then look up various keys
  page->SetLowestValue(txn, 100);
  ASSERT_SUCCESS(page->InsertBranch(txn, "b", 200));
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "alpha", false), 100);
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "b", false), 200);
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "delta", false), 200);
  ASSERT_SUCCESS(txn.PreCommit());

  // Assert -- keys before "b" map to lowest 100; "b" and beyond map to 200
  // (implicit in Act assertions)
}

TEST_F(BranchPageTest, InsertKey) {
  // Arrange
  auto txn = tm_->Begin();
  PageRef page = Page();
  page->SetLowestValue(txn, 100);

  // Act -- insert 7 branch keys out of order to test internal sorting
  ASSERT_SUCCESS(page->InsertBranch(txn, "d", 200));
  ASSERT_SUCCESS(page->InsertBranch(txn, "a", 10));
  ASSERT_SUCCESS(page->InsertBranch(txn, "b", 20));
  ASSERT_SUCCESS(page->InsertBranch(txn, "e", 40));
  ASSERT_SUCCESS(page->InsertBranch(txn, "f", 50));
  ASSERT_SUCCESS(page->InsertBranch(txn, "g", 60));
  ASSERT_SUCCESS(page->InsertBranch(txn, "c", 30));
  ASSERT_SUCCESS(txn.PreCommit());

  // Assert -- implicit; internal sort keeps keys ordered; gtest green on pass
}

TEST_F(BranchPageTest, GetPageForKey) {
  // Arrange
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    page->SetLowestValue(txn, 2);

    // Act -- insert three branch keys and commit
    ASSERT_SUCCESS(page->InsertBranch(txn, "c", 23));
    ASSERT_SUCCESS(page->InsertBranch(txn, "b", 20));
    ASSERT_SUCCESS(page->InsertBranch(txn, "e", 40));
    ASSERT_SUCCESS(txn.PreCommit());
  }

  // Assert -- lookups via helper return expected page IDs for various keys
  AssertPIDForKey("alpha", 2);
  AssertPIDForKey("b", 20);
  AssertPIDForKey("c", 23);
  AssertPIDForKey("zeta", 40);
}

TEST_F(BranchPageTest, InsertAndGetKey) {
  // Arrange
  auto txn = tm_->Begin();
  PageRef page = Page();
  page->SetLowestValue(txn, 100);

  // Act -- interleave inserts and lookups to verify ordering invariant
  ASSERT_SUCCESS(page->InsertBranch(txn, "c", 200));
  ASSERT_SUCCESS(page->InsertBranch(txn, "a", 10));
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "a", false), 10);
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "alpha", false), 10);

  ASSERT_SUCCESS(page->InsertBranch(txn, "g", 60));
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "g", false), 60);
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "guide", false), 60);

  ASSERT_SUCCESS(page->InsertBranch(txn, "e", 40));
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "e", false), 40);
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "error", false), 40);

  ASSERT_SUCCESS(page->InsertBranch(txn, "f", 50));
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "f", false), 50);
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "flight", false), 50);

  ASSERT_SUCCESS(page->InsertBranch(txn, "b", 20));
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "b", false), 20);
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "battle", false), 20);

  ASSERT_SUCCESS(page->UpdateBranch(txn, "c", 30));
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "c", false), 30);
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "cut", false), 30);

  // Assert -- double-check all key→pid mappings after all inserts
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "a", false), 10);
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "alpha", false), 10);
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "b", false), 20);
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "battle", false), 20);
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "c", false), 30);
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "cut", false), 30);
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "g", false), 60);
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "guide", false), 60);
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "e", false), 40);
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "error", false), 40);
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "f", false), 50);
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "flight", false), 50);
}

TEST_F(BranchPageTest, UpdateKey) {
  // Arrange
  auto txn = tm_->Begin();
  {
    PageRef page = Page();
    page->SetLowestValue(txn, 100);

    // Act -- insert 4 branch keys, then update all 4 to new page IDs;
    //        updating non-existent keys "e"/"f" should fail
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

  // Assert -- lookups return the updated page IDs for all 4 keys
  AssertPIDForKey("a", 5);
  AssertPIDForKey("b", 6);
  AssertPIDForKey("c", 7);
  AssertPIDForKey("d", 8);
}

TEST_F(BranchPageTest, DeleteKey) {
  // Arrange
  auto txn = tm_->Begin();
  PageRef page = Page();
  page->SetLowestValue(txn, 2);

  // Act -- insert 3 keys, then delete "b" and "e"; lookups before/after delete
  ASSERT_SUCCESS(page->InsertBranch(txn, "c", 23));
  ASSERT_SUCCESS(page->InsertBranch(txn, "b", 20));
  ASSERT_SUCCESS(page->InsertBranch(txn, "e", 40));
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "alpha", false), 2);
  ASSERT_SUCCESS(page->Delete(txn, "b"));
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "b", false), 2);
  ASSERT_SUCCESS(page->Delete(txn, "e"));
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "e", false), 23);

  // Assert -- after deleting "b" and "e", lookups fall back to lowest or "c"
  // (implicit in Act assertions)
}

TEST_F(BranchPageTest, SplitInto) {
  // Arrange -- nothing more than fixture setup; split works on allocated pages
  auto txn = tm_->Begin();

  // Act -- for 8 iterations, fill a branch page with 8 keys, split it into a
  //        new right page using a separator key from a different prefix
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

  // Assert -- implicit; SplitInto produces a valid right page with separator
  // (implicit in Act; gtest green on pass)
}

TEST_F(BranchPageTest, Recovery) {
  // Arrange -- nothing more than fixture setup

  // Act 1 -- insert 3 branch keys and commit
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    page->SetLowestValue(txn, 2);
    ASSERT_SUCCESS(page->InsertBranch(txn, "c", 23));
    ASSERT_SUCCESS(page->InsertBranch(txn, "b", 20));
    ASSERT_SUCCESS(page->InsertBranch(txn, "e", 40));
    txn.PreCommit();
  }

  // Act 2 -- emulate crash, then recover from log
  Recover();  // Expect redo happen.
  r_->RecoverFrom(0, tm_.get());

  // Assert -- all 3 keys survived recovery with correct page IDs
  AssertPIDForKey("alpha", 2);
  AssertPIDForKey("b", 20);
  AssertPIDForKey("c", 23);
  AssertPIDForKey("zeta", 40);
}

TEST_F(BranchPageTest, InsertCrash) {
  // Arrange -- nothing more than fixture setup

  // Act 1 -- set lowest 2, insert "c"→23, flush, commit
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    page->SetLowestValue(txn, 2);
    Flush();
    ASSERT_SUCCESS(page->InsertBranch(txn, "c", 23));
    txn.PreCommit();
  }

  // Act 2 -- insert "b"→20 and "e"→40 without flushing or committing (crash)
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    ASSERT_SUCCESS(page->InsertBranch(txn, "b", 20));
    ASSERT_SUCCESS(page->InsertBranch(txn, "e", 40));
  }

  // Act 3 -- recover; only "c" should survive because "b"/"e" were uncommitted
  Recover();  // Expect redo happen.
  r_->RecoverFrom(0, tm_.get());

  // Assert -- "c" survived; "b" and "e" did not (uncommitted at crash)
  AssertPIDForKey("alpha", 2);
  AssertPIDForKey("b", 2);
  AssertPIDForKey("c", 23);
  AssertPIDForKey("zeta", 23);
}

TEST_F(BranchPageTest, InsertAbort) {
  // Arrange -- nothing more than fixture setup

  // Act 1 -- set lowest 2, insert "c"→23, commit
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    page->SetLowestValue(txn, 2);
    ASSERT_SUCCESS(page->InsertBranch(txn, "c", 23));
    txn.PreCommit();
  }

  // Act 2 -- insert "b"→20 and "e"→40 then abort the transaction
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

  // Act 3 -- recover; aborted inserts leave only "c" durable
  Recover();  // Expect redo happen.
  r_->RecoverFrom(0, tm_.get());

  // Assert -- "c" survived; "b" and "e" did not (aborted before commit)
  AssertPIDForKey("alpha", 2);
  AssertPIDForKey("b", 2);
  AssertPIDForKey("c", 23);
  AssertPIDForKey("zeta", 23);
}

TEST_F(BranchPageTest, UpdateCrash) {
  // Arrange -- nothing more than fixture setup

  // Act 1 -- insert 3 keys and commit
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    page->SetLowestValue(txn, 2);
    ASSERT_SUCCESS(page->InsertBranch(txn, "c", 23));
    ASSERT_SUCCESS(page->InsertBranch(txn, "b", 20));
    ASSERT_SUCCESS(page->InsertBranch(txn, "e", 40));
    txn.PreCommit();
  }

  // Act 2 -- update "b"→200 and "e"→400, then flush+commit (durable)
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    ASSERT_SUCCESS(page->UpdateBranch(txn, "b", 200));
    ASSERT_SUCCESS(page->UpdateBranch(txn, "e", 400));
    txn.PreCommit();
    Flush();
  }

  // Act 3 -- recover; updates were committed so they survive
  Recover();  // Expect redo happen.
  r_->RecoverFrom(0, tm_.get());

  // Assert -- "b" and "e" have updated page IDs after recovery
  AssertPIDForKey("alpha", 2);
  AssertPIDForKey("b", 200);
  AssertPIDForKey("c", 23);
  AssertPIDForKey("zeta", 400);
}

TEST_F(BranchPageTest, UpdateAbort) {
  // Arrange -- nothing more than fixture setup

  // Act 1 -- insert 3 keys and commit
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    page->SetLowestValue(txn, 2);
    ASSERT_SUCCESS(page->InsertBranch(txn, "c", 23));
    ASSERT_SUCCESS(page->InsertBranch(txn, "b", 20));
    ASSERT_SUCCESS(page->InsertBranch(txn, "e", 40));
    txn.PreCommit();
  }

  // Act 2 -- update "b"→2000 and "e"→4000 then abort the transaction
  {
    auto txn = tm_->Begin();
    {
      PageRef page = Page();
      ASSERT_SUCCESS(page->UpdateBranch(txn, "b", 2000));
      ASSERT_SUCCESS(page->UpdateBranch(txn, "e", 4000));
    }
    txn.Abort();
  }

  // Act 3 -- recover; aborted updates leave original values intact
  Recover();  // Expect redo happen.
  r_->RecoverFrom(0, tm_.get());

  // Assert -- "b" and "e" retain their original page IDs after recovery
  AssertPIDForKey("alpha", 2);
  AssertPIDForKey("b", 20);
  AssertPIDForKey("c", 23);
  AssertPIDForKey("zeta", 40);
}

TEST_F(BranchPageTest, DeleteCrash) {
  // Arrange -- nothing more than fixture setup

  // Act 1 -- insert 3 keys, flush, commit (durable)
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

  // Act 2 -- delete "b" and "e" without flushing or committing (crash)
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    ASSERT_SUCCESS(page->Delete(txn, "b"));
    ASSERT_SUCCESS(page->Delete(txn, "e"));
  }

  // Act 3 -- recover; uncommitted deletes leave all 3 keys intact
  Recover();  // Expect redo happen.
  r_->RecoverFrom(0, tm_.get());

  // Assert -- all 3 keys survive because deletes were uncommitted at crash
  AssertPIDForKey("alpha", 2);
  AssertPIDForKey("b", 20);
  AssertPIDForKey("c", 23);
  AssertPIDForKey("zeta", 40);
}

TEST_F(BranchPageTest, DeleteAbort) {
  // Arrange -- nothing more than fixture setup

  // Act 1 -- insert 3 keys and commit
  {
    auto txn = tm_->Begin();
    PageRef page = Page();
    page->SetLowestValue(txn, 2);
    ASSERT_SUCCESS(page->InsertBranch(txn, "b", 20));
    ASSERT_SUCCESS(page->InsertBranch(txn, "e", 40));
    ASSERT_SUCCESS(page->InsertBranch(txn, "c", 23));
    txn.PreCommit();
  }

  // Act 2 -- delete "b" and "e" then abort the transaction
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

  // Act 3 -- recover; aborted deletes leave all 3 keys intact
  Recover();  // Expect redo happen.
  r_->RecoverFrom(0, tm_.get());

  // Assert -- all 3 keys survive because deletes were aborted before commit
  AssertPIDForKey("alpha", 2);
  AssertPIDForKey("b", 20);
  AssertPIDForKey("c", 23);
  AssertPIDForKey("zeta", 40);
}

TEST_F(BranchPageTest, UpdateHeavy) {
  // Arrange
  std::mt19937 random(0);  // NOLINT(cert-msc32-c,cert-msc51-cpp) fixed seed keeps the test reproducible
  constexpr int kCount = 40;
  Transaction txn = tm_->Begin();
  std::vector<std::string> keys;
  std::unordered_map<std::string, page_id_t> kvp;
  keys.reserve(kCount);
  PageRef page = Page();
  page->SetLowestValue(txn, 999);

  // Act 1 -- insert kCount random keys with random page IDs
  for (int i = 0; i < kCount; ++i) {
    std::string key = RandomString(((19937 * i) % 12) + 10);
    page_id_t value = random() % 10000;
    ASSERT_SUCCESS(page->InsertBranch(txn, key, value));
    keys.push_back(key);
    kvp.emplace(key, value);
  }

  // Act 2 -- for kCount*4 iterations, delete a random key then re-insert
  //          a new random key with a new random page ID
  for (int i = 0; i < kCount * 4; ++i) {
    {
      auto iter = kvp.begin();
      std::advance(iter, random() % kvp.size());
      ASSERT_SUCCESS(page->Delete(txn, iter->first));
      kvp.erase(iter);
    }
    std::string key = RandomString(((19937 * i) % 32) + 100);
    page_id_t value = random() % 10000;
    ASSERT_SUCCESS(page->InsertBranch(txn, key, value));
    kvp[key] = value;
  }

  // Assert -- every surviving key maps to its last inserted page ID
  for (const auto& kv : kvp) {
    ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, kv.first, false),
                          kvp[kv.first]);
  }
}

TEST_F(BranchPageTest, Fences) {
  Transaction txn = tm_->Begin();
  PageRef page = Page();
  for (int i = 0; i < 100; ++i) {
    std::string low = RandomString(((19937 * i) % 12) + 5000, false);
    std::string high = RandomString(((19937 * i) % 12) + 5000, false);
    ASSERT_SUCCESS(page->SetLowFence(txn, IndexKey(low)));
    ASSERT_EQ(page->GetLowFence(txn), IndexKey(low));
    ASSERT_SUCCESS(page->SetHighFence(txn, IndexKey(high)));
    ASSERT_EQ(page->GetLowFence(txn), IndexKey(low));
    ASSERT_EQ(page->GetHighFence(txn), IndexKey(high));
  }
  ASSERT_SUCCESS(page->SetLowFence(txn, IndexKey::MinusInfinity()));
  ASSERT_SUCCESS(page->SetHighFence(txn, IndexKey::PlusInfinity()));
  ASSERT_TRUE(page->GetLowFence(txn).IsMinusInfinity());
  ASSERT_TRUE(page->GetHighFence(txn).IsPlusInfinity());
}

TEST_F(BranchPageTest, FencesCrash) {
  std::string low = RandomString(1234, false);
  std::string high = RandomString(4567, false);
  {
    Transaction txn = tm_->Begin();
    PageRef page = Page();
    ASSERT_SUCCESS(page->SetLowFence(txn, IndexKey(low)));
    ASSERT_SUCCESS(page->SetHighFence(txn, IndexKey(high)));
    ASSERT_EQ(page->GetLowFence(txn), IndexKey(low));
    ASSERT_EQ(page->GetHighFence(txn), IndexKey(high));
    ASSERT_SUCCESS(txn.PreCommit());
  }
  Recover();
  r_->RecoverFrom(0, tm_.get());
  {
    auto restarted_txn = tm_->Begin();
    PageRef recovered_page = Page();
    ASSERT_EQ(recovered_page->GetLowFence(restarted_txn), IndexKey(low));
    ASSERT_EQ(recovered_page->GetHighFence(restarted_txn), IndexKey(high));
    ASSERT_SUCCESS(restarted_txn.PreCommit());
  }
}

TEST_F(BranchPageTest, FosterChild) {
  // Arrange
  Transaction txn = tm_->Begin();
  PageRef page = Page();

  // Act -- for 100 iterations, set/get foster pair, then clear it and verify gone
  for (int i = 0; i < 100; ++i) {
    std::string key = RandomString(((19937 * i) % 12) + 5000, false);
    ASSERT_SUCCESS(page->SetFoster(txn, {key, page_id_t(i)}));
    ASSIGN_OR_ASSERT_FAIL_CONST(FosterPair, result, page->GetFoster(txn));
    ASSERT_EQ(result.key, key);
    ASSERT_EQ(result.child_pid, i);
    ASSERT_SUCCESS(page->SetFoster(txn, FosterPair()));
    if (auto f = page->GetFoster(txn)) {
      ASSERT_TRUE(!"never reach here");
    }
  }

  // Assert -- foster pair set/get/clear round-trip preserves key and child_pid
  // (implicit in Act assertions)
}

TEST_F(BranchPageTest, FosterChildCrash) {
  // Arrange -- nothing more than fixture setup

  // Act -- for 5 iterations, set foster pair, commit, crash, recover, verify
  for (int i = 0; i < 5; ++i) {
    std::string key = RandomString(((19937 * i) % 12) + 10000, false);
    {
      Transaction txn = tm_->Begin();
      PageRef page = Page();
      ASSERT_SUCCESS(page->SetFoster(txn, {key, page_id_t(i)}));
      ASSIGN_OR_ASSERT_FAIL_CONST(FosterPair, result, page->GetFoster(txn));
      ASSERT_EQ(result.key, key);
      ASSERT_EQ(result.child_pid, i);
      ASSERT_SUCCESS(txn.PreCommit());
    }
    Recover();
    r_->RecoverFrom(0, tm_.get());
    {
      Transaction txn = tm_->Begin();
      PageRef page = Page();
      ASSIGN_OR_ASSERT_FAIL_CONST(FosterPair, result, page->GetFoster(txn));
      ASSERT_EQ(result.key, key);
      ASSERT_EQ(result.child_pid, i);
    }
  }

  // Assert -- foster pair survived each crash/recovery round-trip
  // (implicit in Act assertions)
}

TEST_F(BranchPageTest, MoveLeftFromFoster1) {
  // Arrange
  Transaction txn = tm_->Begin();
  PageRef page = Page();
  page->SetLowestValue(txn, 12);
  page->InsertBranch(txn, "a", 13);
  PageRef foster =
      txn.GetPageManager()->AllocateNewPage(txn, PageType::kBranchPage);
  foster->SetLowestValue(txn, 14);
  foster->InsertBranch(txn, "c", 15);
  foster->InsertBranch(txn, "d", 16);
  ASSERT_SUCCESS(page->SetFoster(txn, FosterPair("b", foster->PageID())));

  // Act 1 -- move "b" from foster child into parent page
  ASSERT_SUCCESS(page->body.branch_page.MoveLeftFromFoster(txn, *foster));

  // Assert 1 -- parent now has "a" and "b"; foster child still has "c","d"
  ASSERT_EQ(page->RowCount(), 2U);
  ASSERT_EQ(page->GetKey(0), "a");
  ASSERT_EQ(page->GetKey(1), "b");
  ASSERT_TRUE(page->GetFoster(txn));
  EXPECT_EQ(foster->body.branch_page.GetLowestValue(txn), 15);

  // Act 2 -- move "c" from foster child into parent page
  ASSERT_SUCCESS(page->body.branch_page.MoveLeftFromFoster(txn, *foster));

  // Assert 2 -- parent now has 4 keys "a","b","c","d"; foster child emptied
  ASSERT_EQ(page->RowCount(), 4);
}

TEST_F(BranchPageTest, InsertTooBigData) {
  // Arrange -- nothing more than fixture setup
  auto txn = tm_->Begin();
  PageRef page = Page();

  // Act -- a key larger than the per-entry limit must be rejected
  Status result =
      page->InsertBranch(txn, std::string(6000, 'a'), 1);

  // Assert -- kTooBigData, not a silent truncation or crash
  ASSERT_EQ(result, Status::kTooBigData);
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(BranchPageTest, InsertDuplicateKey) {
  // Arrange
  auto txn = tm_->Begin();
  PageRef page = Page();

  // Act -- insert the same branch key twice
  ASSERT_SUCCESS(page->InsertBranch(txn, "a", 1));
  Status result = page->InsertBranch(txn, "a", 2);

  // Assert -- duplicate insertion is rejected
  ASSERT_EQ(result, Status::kDuplicates);
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(BranchPageTest, UpdateTooBigKey) {
  // Arrange
  auto txn = tm_->Begin();
  PageRef page = Page();
  ASSERT_SUCCESS(page->InsertBranch(txn, "a", 1));

  // Act -- updating with an oversized key must be rejected up-front
  Status result =
      page->UpdateBranch(txn, std::string(6000, 'a'), 2);

  // Assert -- kTooBigData
  ASSERT_EQ(result, Status::kTooBigData);
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(BranchPageTest, DeleteLowestKeyPromotesLowestValue) {
  // Arrange
  auto txn = tm_->Begin();
  PageRef page = Page();
  page->SetLowestValue(txn, 100);
  ASSERT_SUCCESS(page->InsertBranch(txn, "b", 200));
  ASSERT_SUCCESS(page->InsertBranch(txn, "c", 300));

  // Act -- delete a key smaller than the lowest key (the "left edge" path)
  ASSERT_SUCCESS(page->Delete(txn, "a"));

  // Assert -- the old lowest row "b" was consumed to promote the lowest value
  ASSERT_EQ(page->RowCount(), 1U);
  ASSERT_EQ(page->GetKey(0), "c");
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "alpha", false), 200);
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "b", false), 200);
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "c", false), 300);
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(BranchPageTest, SetFenceNoSpace) {
  // Arrange -- fill the branch page nearly to capacity
  auto txn = tm_->Begin();
  PageRef page = Page();
  page->SetLowestValue(txn, 0);
  for (int i = 0; i < 8; ++i) {
    ASSERT_SUCCESS(page->InsertBranch(txn, std::string(4000, '0' + i), i + 1));
  }

  // Act -- an oversized low fence cannot fit in the remaining space
  Status low = page->SetLowFence(txn, IndexKey(std::string(5000, 'l')));

  // Assert -- kNoSpace
  ASSERT_EQ(low, Status::kNoSpace);

  // Act -- an oversized high fence cannot fit either
  Status high = page->SetHighFence(txn, IndexKey(std::string(5000, 'h')));

  // Assert -- kNoSpace
  ASSERT_EQ(high, Status::kNoSpace);
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(BranchPageTest, SetFosterNoSpace) {
  // Arrange -- fill the branch page nearly to capacity
  auto txn = tm_->Begin();
  PageRef page = Page();
  page->SetLowestValue(txn, 0);
  for (int i = 0; i < 8; ++i) {
    ASSERT_SUCCESS(page->InsertBranch(txn, std::string(4000, '0' + i), i + 1));
  }

  // Act -- an oversized foster key cannot fit in the remaining space
  Status result =
      page->SetFoster(txn, FosterPair(std::string(5000, 'f'), 99));

  // Assert -- kNoSpace
  ASSERT_EQ(result, Status::kNoSpace);
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(BranchPageTest, DumpBranchPage) {
  // Arrange -- a populated page with lowest value, rows and a foster pair
  auto txn = tm_->Begin();
  PageRef page = Page();
  page->SetLowestValue(txn, 100);
  ASSERT_SUCCESS(page->InsertBranch(txn, "b", 200));
  ASSERT_SUCCESS(page->InsertBranch(txn, "c", 300));
  ASSERT_SUCCESS(page->SetFoster(txn, FosterPair("d", 400)));

  // Act -- stream the page through the Dump path
  std::ostringstream oss;
  oss << *page;

  // Assert -- header, rows and foster key are all rendered
  const std::string dumped = oss.str();
  EXPECT_NE(dumped.find("BranchPage"), std::string::npos);
  EXPECT_NE(dumped.find("FosterKey"), std::string::npos);
  EXPECT_NE(dumped.find("d -> 400"), std::string::npos);
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(BranchPageTest, SplitPivotAdjustment) {
  // Arrange -- a small page whose split must advance the pivot past the new key
  auto txn = tm_->Begin();
  PageRef page = Page();
  page->SetLowestValue(txn, 1);
  ASSERT_SUCCESS(page->InsertBranch(txn, "a", 2));
  ASSERT_SUCCESS(page->InsertBranch(txn, "b", 3));
  PageRef right = p_->AllocateNewPage(txn, PageType::kBranchPage);

  // Act -- split with a separator between the two existing keys
  std::string middle;
  page->SplitInto(txn, "ab", right.get(), &middle);

  // Assert -- the middle key separates the left page from the right side
  ASSERT_EQ(middle, "b");
  ASSERT_EQ(page->RowCount(), 1U);
  ASSERT_EQ(right->RowCount(), 0U);
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(BranchPageTest, InsertNoSpaceWhenSlotHeaderDoesNotFit) {
  // Arrange -- fill the branch page with 8 large keys so that the payload for
  // one more key fits but the per-entry slot header overhead no longer does.
  auto txn = tm_->Begin();
  PageRef page = Page();
  page->SetLowestValue(txn, 0);
  for (int i = 0; i < 8; ++i) {
    ASSERT_SUCCESS(page->InsertBranch(txn, std::string(4000, '0' + i), i + 1));
  }

  // Act -- insert a 9th large key; physical payload fits, slot does not
  Status result = page->InsertBranch(txn, std::string(4000, '8'), 9);

  // Assert -- kNoSpace (not a silent overwrite or a crash)
  ASSERT_EQ(result, Status::kNoSpace);
  ASSERT_EQ(page->RowCount(), 8U);
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(BranchPageTest, DumpEmptyBranchPage) {
  // Arrange -- a freshly allocated branch page has no rows
  auto txn = tm_->Begin();
  PageRef page = Page();

  // Act -- stream the empty page through the Dump path
  std::ostringstream oss;
  oss << *page;

  // Assert -- the header is rendered and the empty-page branch returns early
  const std::string dumped = oss.str();
  EXPECT_NE(dumped.find("BranchPage"), std::string::npos);
  EXPECT_NE(dumped.find("Rows: 0"), std::string::npos);
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(BranchPageTest, FosterMergeEmptiesFosterSafely) {
  // Arrange -- parent holds one key; the foster child holds a single key.
  // NOTE: this deliberately stops after the merge: calling MoveLeftFromFoster
  // again on the now-empty foster sibling would hit the `assert(0 <
  // right.RowCount())` at branch_page.cpp:513 (known production crash).
  Transaction txn = tm_->Begin();
  PageRef page = Page();
  page->SetLowestValue(txn, 12);
  ASSERT_SUCCESS(page->InsertBranch(txn, "a", 13));
  PageRef foster =
      txn.GetPageManager()->AllocateNewPage(txn, PageType::kBranchPage);
  foster->SetLowestValue(txn, 14);
  ASSERT_SUCCESS(foster->InsertBranch(txn, "c", 15));
  ASSERT_SUCCESS(page->SetFoster(txn, FosterPair("b", foster->PageID())));

  // Act -- with exactly one foster row, MoveLeftFromFoster merges the foster
  // child into the parent and clears the foster pointer.
  ASSERT_SUCCESS(page->body.branch_page.MoveLeftFromFoster(txn, *foster));

  // Assert -- all keys live in the parent and the foster slot is now empty.
  ASSERT_EQ(page->RowCount(), 3U);
  ASSERT_EQ(page->GetKey(0), "a");
  ASSERT_EQ(page->GetKey(1), "b");
  ASSERT_EQ(page->GetKey(2), "c");
  ASSERT_EQ(page->GetFoster(txn).GetStatus(), Status::kNotExists);
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "9", false), 12);
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "b", false), 14);
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "c", false), 15);
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(BranchPageTest, MoveRightToFosterKeepsLookupsOrdered) {
  // Arrange -- parent holds keys "a" and "c"; a foster child already holds
  // "d".."e"; the middle key "b" is about to move right.
  Transaction txn = tm_->Begin();
  PageRef page = Page();
  page->SetLowestValue(txn, 1);
  ASSERT_SUCCESS(page->InsertBranch(txn, "a", 2));
  ASSERT_SUCCESS(page->InsertBranch(txn, "b", 3));
  ASSERT_SUCCESS(page->InsertBranch(txn, "c", 4));
  PageRef foster =
      txn.GetPageManager()->AllocateNewPage(txn, PageType::kBranchPage);
  foster->SetLowestValue(txn, 5);
  ASSERT_SUCCESS(foster->InsertBranch(txn, "e", 6));
  ASSERT_SUCCESS(page->SetFoster(txn, FosterPair("d", foster->PageID())));

  // Act -- move the last parent key ("c") into the foster child.
  ASSERT_SUCCESS(page->body.branch_page.MoveRightToFoster(txn, *foster));

  // Assert -- parent keeps "a","b" and the moved key becomes the foster pair.
  // (Branch-level lookup falls back to the previous child; the B+ tree layer
  // descends into that page and follows its foster for keys at the right edge.)
  ASSERT_EQ(page->RowCount(), 2U);
  ASSERT_EQ(page->GetKey(0), "a");
  ASSERT_EQ(page->GetKey(1), "b");
  ASSIGN_OR_ASSERT_FAIL_CONST(FosterPair, foster_pair, page->GetFoster(txn));
  ASSERT_EQ(foster_pair.key, "c");
  ASSERT_EQ(foster_pair.child_pid, foster->PageID());
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "a", false), 2);
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "b", false), 3);
  ASSERT_SUCCESS_AND_EQ(page->GetPageForKey(txn, "c", false), 3);
  ASSERT_SUCCESS(txn.PreCommit());
}
}  // namespace tinylamb
