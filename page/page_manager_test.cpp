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

#include "page_manager.hpp"

#include <cstdint>
#include <cstdio>
#include <memory>
#include <set>
#include <vector>
#include <string>
#include <tuple>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "page/free_page.hpp"
#include "page/page_ref.hpp"
#include "page_type.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

class PageManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string prefix = "page_manager_test-" + RandomString();
    db_name_ = prefix + ".db";
    log_name_ = prefix + ".log";
    Reset();
  }

  void Reset() {
    tm_.reset();
    rm_.reset();
    lm_.reset();
    l_.reset();
    p_.reset();
    p_ = std::make_unique<PageManager>(db_name_, 10);
    l_ = std::make_unique<Logger>(log_name_);
    lm_ = std::make_unique<LockManager>();
    // Wire the real recovery stack so a re-open after the simulated crash
    // replays committed WAL records exactly like PageStorage does.
    rm_ = std::make_unique<RecoveryManager>(log_name_, p_->GetPool());
    tm_ = std::make_unique<TransactionManager>(lm_.get(), p_.get(), l_.get(),
                                               rm_.get());
    // Boot-time recovery, same as PageStorage's constructor would do.
    // Tests that poke raw page bodies (unlogged) disable this to emulate a
    // clean restart instead of a crash.
    if (run_recovery_on_reset_) {
      rm_->RecoverFrom(0, tm_.get());
    }
  }

  void TearDown() override {
    std::ignore = std::remove(db_name_.c_str());
    std::ignore = std::remove(log_name_.c_str());
  }

  PageRef AllocatePage(PageType expected_type) {
    Transaction system_txn = tm_->Begin();
    PageRef new_page = p_->AllocateNewPage(system_txn, expected_type);
    system_txn.PreCommit();
    EXPECT_FALSE(new_page.IsNull());
    EXPECT_EQ(new_page->Type(), PageType::kFreePage);
    return new_page;
  }

  PageRef GetPage(uint64_t page_id) {
    Transaction system_txn = tm_->Begin();
    PageRef got_page = p_->GetPage(page_id);
    EXPECT_TRUE(!got_page.IsNull());
    return got_page;
  }

  void DestroyPage(Page* target) {
    Transaction system_txn = tm_->Begin();
    p_->DestroyPage(system_txn, target);
    system_txn.PreCommit();
  }

  std::string db_name_;
  std::string log_name_;
  std::unique_ptr<LockManager> lm_;
  std::unique_ptr<PageManager> p_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<TransactionManager> tm_;
  std::unique_ptr<RecoveryManager> rm_;
  bool run_recovery_on_reset_{true};
};

TEST_F(PageManagerTest, Construct) {
  // Arrange -- nothing to set up; default PageManager created by SetUp()
  // Act -- nothing to execute; default constructed via SetUp()
  // Assert -- nothing to verify; gtest green on pass, death on crash
}

TEST_F(PageManagerTest, AllocateNewPage) {
  // Arrange -- allocate a new free page via system transaction
  PageRef page = AllocatePage(PageType::kFreePage);

  // Act -- write a deterministic byte pattern into the free page body
  char* buff = page->body.free_page.FreeBody();
  for (size_t j = 0; j < FreePage::FreeBodySize(); ++j) {
    // Make sure no SEGV happen.
    // Union overlay: writes stay inside the kPageSize page allocation.
    // Union overlay: writes stay inside the kPageSize page allocation.
    buff[j] = static_cast<char>((page->PageID() + j) & 0xff);  // NOLINT(clang-analyzer-security.ArrayBound)
  }

  // Assert -- implicit; no SEGV means the page body is writable and sized correctly
}

TEST_F(PageManagerTest, AllocateMultipleNewPage) {
  // Arrange -- allocate 15+1 free pages, each written with a deterministic byte pattern
  constexpr int kPages = 15;
  std::set<page_id_t> allocated_ids;
  for (int i = 0; i <= kPages; ++i) {
    PageRef page = AllocatePage(PageType::kFreePage);
    char* buff = page->body.free_page.FreeBody();
    for (size_t j = 0; j < FreePage::FreeBodySize(); ++j) {
      // Union overlay: writes stay inside the kPageSize page allocation.
      buff[j] = static_cast<char>((page->PageID() + j) & 0xff);  // NOLINT(clang-analyzer-security.ArrayBound)
    }
    allocated_ids.insert(page->PageID());
  }

  // Act -- reset PageManager (drop pages) and re-read each allocated page.
  // The body patterns are raw unlogged writes, so this emulates a clean
  // shutdown (intact disk images, no WAL replay zeroes them back).
  run_recovery_on_reset_ = false;
  Reset();
  run_recovery_on_reset_ = true;
  for (const auto& id : allocated_ids) {
    PageRef ref = GetPage(id);
    FreePage& page = ref.GetFreePage();
    char* buff = page.FreeBody();

    // Assert -- each page's body still has the deterministic pattern written before reset
    for (size_t j = 0; j < kFreeBodySize; ++j) {
      ASSERT_EQ(buff[j], static_cast<char>((id + j) & 0xff));
    }
  }
}

TEST_F(PageManagerTest, DestroyPage) {
  // Arrange -- allocate and destroy 15 free pages
  for (int i = 0; i < 15; ++i) {
    PageRef page = AllocatePage(PageType::kFreePage);
    DestroyPage(page.get());
  }

  // Act -- re-allocate 15 free pages; PageManager should recycle destroyed page IDs
  for (int i = 0; i < 15; ++i) {
    PageRef page = AllocatePage(PageType::kFreePage);

    // Assert -- recycled page IDs must be <= 15 (the max ID ever allocated)
    ASSERT_LE(page->PageID(), 15);
  }
}

// Regression test derived from row_page_fuzzer (crash-b709ae1f). A page that
// was allocated and committed through the WAL must be readable again after a
// crash loses the whole buffer pool: recovery replays the log, so the page
// image does not need to have reached the .db file. PageManager::GetPage()
// currently returns an empty PageRef in this situation, which callers
// dereference unguarded.
TEST_F(PageManagerTest, CommittedPageSurvivesCrashWithoutFlush) {
  // Arrange -- allocate a page and commit the allocation durably.
  Transaction system_txn = tm_->Begin();
  PageRef allocated = p_->AllocateNewPage(system_txn, PageType::kFreePage);
  const uint64_t page_id = allocated->PageID();
  ASSERT_SUCCESS(system_txn.PreCommit());
  ASSERT_FALSE(allocated.IsNull());

  // Act -- emulate a crash: discard every resident page without writeback,
  // then rebuild all managers on top of the surviving WAL.
  p_->GetPool()->DropAllPages();
  Reset();

  // Assert -- the committed page comes back after recovery.
  Transaction read_txn = tm_->Begin();
  PageRef recovered = p_->GetPage(page_id);
  ASSERT_FALSE(recovered.IsNull())
      << "committed page " << page_id << " lost by crash recovery";
  EXPECT_EQ(recovered->Type(), PageType::kFreePage);
  read_txn.PreCommit();
}

// Bug-probe variants around the row_page_fuzzer null-deref finding
// (crash-b709ae1f): a committed page must come back after crash recovery.
// These pin the blast radius of the "committed page lost" behaviour.
TEST_F(PageManagerTest, CommittedRowPageWithRowsSurvivesCrash) {
  // Arrange -- allocate a row page, insert two rows, commit everything.
  Transaction txn = tm_->Begin();
  PageRef page = p_->AllocateNewPage(txn, PageType::kRowPage);
  const uint64_t page_id = page->PageID();
  const StatusOr<slot_t> s1 = page->Insert(txn, "first-row");
  const StatusOr<slot_t> s2 = page->Insert(txn, "second-row");
  ASSERT_SUCCESS(s1.GetStatus());
  ASSERT_SUCCESS(s2.GetStatus());
  ASSERT_SUCCESS(txn.PreCommit());

  // Act -- crash: drop every resident page, rebuild from the WAL.
  p_->GetPool()->DropAllPages();
  Reset();

  // Assert -- both rows read back through a fresh page reference.
  PageRef recovered = p_->GetPage(page_id);
  ASSERT_FALSE(recovered.IsNull())
      << "row page " << page_id << " lost by crash recovery";
  Transaction read_txn = tm_->Begin();
  const StatusOr<std::string_view> r1 = recovered->Read(read_txn, s1.Value());
  EXPECT_EQ(r1.GetStatus(), Status::kSuccess);
  if (r1.HasValue()) {
    EXPECT_EQ(r1.Value(), "first-row");
  }
  const StatusOr<std::string_view> r2 = recovered->Read(read_txn, s2.Value());
  EXPECT_EQ(r2.GetStatus(), Status::kSuccess);
  if (r2.HasValue()) {
    EXPECT_EQ(r2.Value(), "second-row");
  }
  read_txn.PreCommit();
}

TEST_F(PageManagerTest, MultipleCommittedPagesSurviveCrash) {
  // Arrange -- three independently committed pages.
  std::vector<uint64_t> ids;
  for (int i = 0; i < 3; ++i) {
    Transaction txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kFreePage);
    ids.push_back(page->PageID());
    ASSERT_SUCCESS(txn.PreCommit());
  }

  // Act -- crash and rebuild.
  p_->GetPool()->DropAllPages();
  Reset();

  // Assert -- every committed page is still there.
  for (const uint64_t id : ids) {
    PageRef recovered = p_->GetPage(id);
    EXPECT_FALSE(recovered.IsNull())
        << "committed page " << id << " lost by crash recovery";
  }
}

TEST_F(PageManagerTest, MetaPageSurvivesCrash) {
  // Arrange -- one committed allocation so the meta page has content.
  Transaction txn = tm_->Begin();
  PageRef page = p_->AllocateNewPage(txn, PageType::kFreePage);
  ASSERT_SUCCESS(txn.PreCommit());

  // Act -- crash and rebuild.
  p_->GetPool()->DropAllPages();
  Reset();

  // Assert -- meta page (id 0) always loads back.
  PageRef meta = p_->GetPage(0);
  EXPECT_FALSE(meta.IsNull());
  EXPECT_EQ(meta->Type(), PageType::kMetaPage);
}

TEST_F(PageManagerTest, GetPageForUnknownPageIdReturnsNullRef) {
  // Contract pin from the row_page_fuzzer crash (crash-b709ae1f): GetPage
  // signals "missing or broken page" with an EMPTY PageRef, not an error.
  // Callers (and harnesses) must check IsNull() before dereferencing - the
  // fuzzer harness did not and segfaulted.
  Transaction txn = tm_->Begin();
  const PageRef missing = p_->GetPage(999'999);
  EXPECT_TRUE(missing.IsNull());
  // The meta page, in contrast, always resolves.
  const PageRef meta = p_->GetPage(0);  // id 0 = meta page
  EXPECT_FALSE(meta.IsNull());
  txn.PreCommit();
}

}  // namespace tinylamb