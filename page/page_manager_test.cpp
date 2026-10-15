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
#include <string>
#include <tuple>
#include <vector>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "page/free_page.hpp"
#include "page/index_key.hpp"
#include "page/page_ref.hpp"
#include "page_type.hpp"
#include "recovery/log_record.hpp"
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
    p_ = PageManager::Create(db_name_, 10).MoveValue();
    l_ = Logger::Create(log_name_).MoveValue();
    lm_ = std::make_unique<LockManager>();
    // Wire the real recovery stack so a re-open after the simulated crash
    // replays committed WAL records exactly like PageStorage does.
    rm_ = std::make_unique<RecoveryManager>(log_name_, p_->GetPool());
    tm_ = std::make_unique<TransactionManager>(p_.get(), l_.get(), rm_.get());
    // Boot-time recovery, same as PageStorage's constructor would do.
    // Tests that poke raw page bodies (unlogged) disable this to emulate a
    // clean restart instead of a crash.
    if (run_recovery_on_reset_) {
      rm_->RecoverFrom(0, tm_.get());
    }
  }

  void TearDown() override {
    std::ignore = std::remove(db_name_.c_str());
    tinylamb::RemoveLogAndSegments(log_name_);
  }

  PageRef AllocatePage(PageType expected_type) {
    Transaction system_txn = tm_->Begin();
    PageRef new_page =
        p_->AllocateNewPage(system_txn, expected_type).MoveValue();
    system_txn.PreCommit();
    EXPECT_FALSE(new_page.IsNull());
    EXPECT_EQ(new_page->Type(), PageType::kFreePage);
    return new_page;
  }

  PageRef GetPage(uint64_t page_id) {
    Transaction system_txn = tm_->Begin();
    PageRef got_page = p_->GetPage(page_id).MoveValue();
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

// GetPage answers a page id the file cannot cover (past EOF, or an id an
// aborted growth destroyed after a healer published it) with an EMPTY PageRef
// under kSuccess. Every heap caller must test IsNull() before dereferencing;
// this pins the contract the bootstrap and Table::Insert guards rely on.
TEST_F(PageManagerTest, GetPagePastFileEndAnswersEmptyRef) {
  // Arrange -- a fresh manager whose file holds only the meta page
  // Act -- read a page id far past the file end
  Transaction system_txn = tm_->Begin();
  PageRef beyond = p_->GetPage(4'096).MoveValue();
  static_cast<void>(system_txn.PreCommit());
  // Assert -- empty ref, success status
  EXPECT_TRUE(beyond.IsNull());
}

// A stale SELF-reservation (this owner walked the chain on to a different
// predecessor) used to wedge ReserveTableTail with kConflicts for the rest of
// the process: nobody would ever link the abandoned claim. The reserve now
// retires it, so the fresh claim -- and other owners -- can proceed.
// Round 9 regression: a transaction that already holds a tail reservation
// on one table must never WAIT for another table's reservation. Two growers
// each holding the table the other waits for rode the full wait limit and
// then failed -- a reservation cycle the write-intent detector cannot see
// (the wait-for graph only tracks row intents). The watcher-aware overload
// refuses immediately instead.
TEST_F(PageManagerTest, ReserveTableTailWithHeldReservationFailsFast) {
  Transaction txn = tm_->Begin();
  // The watcher already grows table 200; a foreign grower holds table 100.
  ASSERT_EQ(p_->TryReserveTableTail(txn.ID(), 200, 200), Status::kSuccess);
  ASSERT_EQ(p_->TryReserveTableTail(77, 100, 100), Status::kSuccess);
  const auto start = std::chrono::steady_clock::now();
  const Status reserved = p_->ReserveTableTail(txn, 100, 100);
  const auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_EQ(reserved, Status::kConflicts);
  EXPECT_LT(elapsed, std::chrono::seconds(1));
  p_->ResolveTableTailReservations(txn.ID(), TableTailResolution::kDiscard);
  p_->ResolveTableTailReservations(77, TableTailResolution::kDiscard);
  ASSERT_SUCCESS(tm_->Abort(txn));
}

// A wounded transaction (deadlock-detector victim) must abandon a tail
// reservation wait at the first slice instead of riding the full limit --
// its abort releases the reservations everyone else is waiting for.
TEST_F(PageManagerTest, ReserveTableTailBailsWhenWounded) {
  ASSERT_EQ(p_->TryReserveTableTail(88, 300, 300), Status::kSuccess);
  Transaction txn = tm_->Begin();
  txn.Wound();
  const auto start = std::chrono::steady_clock::now();
  const Status reserved = p_->ReserveTableTail(txn, 300, 300);
  const auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_EQ(reserved, Status::kConflicts);
  EXPECT_LT(elapsed, std::chrono::seconds(1));
  p_->ResolveTableTailReservations(88, TableTailResolution::kDiscard);
  ASSERT_SUCCESS(tm_->Abort(txn));
}

TEST_F(PageManagerTest, ReserveTableTailRetiresStaleSelfReservation) {
  // Arrange -- register a table whose published tail is page 5 and let
  // owner 7 claim it
  EXPECT_EQ(p_->GetTableTailFor(7, 5, 5), 5U);
  EXPECT_EQ(p_->TryReserveTableTail(7, 5, 5), Status::kSuccess);
  // Act -- owner 7 re-claims with a predecessor the published tail already
  // outgrew (the caller rewalks on this kConflicts)
  EXPECT_EQ(p_->ReserveTableTail(7, 5, 6), Status::kConflicts);
  // Assert -- the abandoned claim is gone: another owner claims cleanly
  EXPECT_EQ(p_->TryReserveTableTail(8, 5, 5), Status::kSuccess);
  EXPECT_EQ(p_->TryReserveTableTail(9, 5, 5), Status::kConflicts);
}

// Fence writes that grow the payload must refuse bytes a FOREIGN index-undo
// reservation still claims (its abort would re-insert deleted separators),
// while the owner's own reservation never blocks its own structural step.
TEST_F(PageManagerTest, FenceAdmissionHonorsForeignUndoReservation) {
  // Arrange -- allocate a page and convert it to a leaf with infinite fences
  PageRef page = AllocatePage(PageType::kFreePage);
  {
    Transaction convert = tm_->Begin();
    ASSERT_SUCCESS(page->PageTypeChange(convert, PageType::kLeafPage));
    ASSERT_SUCCESS(convert.PreCommit());
  }
  const page_id_t pid = page->PageID();
  Transaction writer = tm_->Begin();
  Transaction reserver = tm_->Begin();
  tm_->ReserveIndexUndoBytes(reserver, pid, 1U << 20);
  // Act + Assert -- a foreign reservation consuming the page blocks growth
  EXPECT_EQ(page->SetHighFence(writer, IndexKey("zzz")), Status::kNoSpace);
  EXPECT_EQ(page->SetLowFence(writer, IndexKey("aaa")), Status::kNoSpace);
  // The owner's own reservation is excluded (newest-first undo frees the
  // fence's bytes before the older claim is ever re-applied).
  // Resolve the foreign holder, then check the self-exclusion: the owner's
  // own reservation is ignored (newest-first undo frees the fence's bytes
  // before the older claim is ever re-applied).
  EXPECT_EQ(reserver.Abort(), Status::kSuccess);
  tm_->ReserveIndexUndoBytes(writer, pid, 1U << 20);
  EXPECT_EQ(page->SetHighFence(writer, IndexKey("zzz")), Status::kSuccess);
  EXPECT_EQ(page->SetLowFence(writer, IndexKey("aaa")), Status::kSuccess);
  EXPECT_EQ(writer.PreCommit(), Status::kSuccess);
}

TEST_F(PageManagerTest, AllocateNewPage) {
  // Arrange -- allocate a new free page via system transaction
  PageRef page = AllocatePage(PageType::kFreePage);

  // Act -- write a deterministic byte pattern into the free page body
  char* buff = page->body.free_page.FreeBody();
  for (size_t j = 0; j < FreePage::FreeBodySize(); ++j) {
    // Make sure no SEGV happen.
    // Union overlay: writes stay inside the kPageSize page allocation.
    buff[j] = static_cast<char>(  // NOLINT(clang-analyzer-security.ArrayBound)
        (page->PageID() + j) & 0xff);
  }

  // Assert -- implicit; no SEGV means the page body is writable and sized
  // correctly
}

TEST_F(PageManagerTest, AllocateMultipleNewPage) {
  // Arrange -- allocate 15+1 free pages, each written with a deterministic byte
  // pattern
  constexpr int kPages = 15;
  std::set<page_id_t> allocated_ids;
  for (int i = 0; i <= kPages; ++i) {
    PageRef page = AllocatePage(PageType::kFreePage);
    char* buff = page->body.free_page.FreeBody();
    for (size_t j = 0; j < FreePage::FreeBodySize(); ++j) {
      // Union overlay: writes stay inside the kPageSize page allocation.
      const size_t body_index = j;
      buff[body_index] =  // NOLINT(clang-analyzer-security.ArrayBound)
          static_cast<char>((page->PageID() + j) & 0xff);
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

    // Assert -- each page's body still has the deterministic pattern written
    // before reset
    for (size_t j = 0; j < kFreeBodySize; ++j) {
      ASSERT_EQ(buff[j], static_cast<char>((id + j) & 0xff));
    }
  }
}

TEST_F(PageManagerTest, RecycledAllocationAdvancesAllocatorHighWater) {
  page_id_t recycled_id = 0;
  {
    Transaction txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kFreePage).MoveValue();
    recycled_id = page->PageID();
    ASSERT_SUCCESS(txn.PreCommit());
  }
  {
    Transaction txn = tm_->Begin();
    PageRef page = p_->GetPage(recycled_id).MoveValue();
    ASSERT_SUCCESS(p_->DestroyPage(txn, page.get()));
    ASSERT_SUCCESS(txn.PreCommit());
  }
  {
    PageRef meta = p_->GetPage(0).MoveValue();
    ASSERT_EQ(meta->body.meta_page.MaxPageCount(), recycled_id);
    meta->PageInit(0, PageType::kMetaPage);
    meta->body.meta_page.RebuildFreeListHead(recycled_id);
    ASSERT_EQ(meta->body.meta_page.MaxPageCount(), 0U);
    ASSERT_EQ(meta->body.meta_page.FirstFreePage(), recycled_id);
  }

  {
    Transaction txn = tm_->Begin();
    PageRef recycled = p_->AllocateNewPage(txn, PageType::kRowPage).MoveValue();
    ASSERT_EQ(recycled->PageID(), recycled_id);
    PageRef fresh = p_->AllocateNewPage(txn, PageType::kRowPage).MoveValue();
    EXPECT_NE(fresh->PageID(), recycled->PageID());
    EXPECT_EQ(fresh->PageID(), recycled_id + 1);
    ASSERT_SUCCESS(txn.PreCommit());
  }
  {
    PageRef meta = p_->GetPage(0).MoveValue();
    EXPECT_EQ(meta->body.meta_page.MaxPageCount(), recycled_id + 1);
  }

  Reset();
  Transaction after_recovery = tm_->Begin();
  PageRef next =
      p_->AllocateNewPage(after_recovery, PageType::kRowPage).MoveValue();
  EXPECT_EQ(next->PageID(), recycled_id + 2);
  ASSERT_SUCCESS(after_recovery.PreCommit());
}

TEST_F(PageManagerTest, RecycledAllocationAtLostHighWaterRestoresSnapshot) {
  std::vector<page_id_t> page_ids;
  {
    Transaction txn = tm_->Begin();
    for (int i = 0; i < 3; ++i) {
      PageRef page = p_->AllocateNewPage(txn, PageType::kRowPage).MoveValue();
      page_ids.push_back(page->PageID());
    }
    ASSERT_SUCCESS(txn.PreCommit());
  }
  {
    Transaction txn = tm_->Begin();
    for (const page_id_t page_id : page_ids) {
      PageRef page = p_->GetPage(page_id).MoveValue();
      ASSERT_SUCCESS(p_->DestroyPage(txn, page.get()));
    }
    ASSERT_SUCCESS(txn.PreCommit());
  }
  const page_id_t recycled_id = page_ids.back();
  const page_id_t old_next = page_ids[page_ids.size() - 2];
  {
    PageRef meta = p_->GetPage(0).MoveValue();
    meta->PageInit(0, PageType::kMetaPage);
    meta->body.meta_page.RebuildFreeListHead(recycled_id);
  }

  Transaction aborted = tm_->Begin();
  {
    PageRef page = p_->AllocateNewPage(aborted, PageType::kRowPage).MoveValue();
    ASSERT_EQ(page->PageID(), recycled_id);
  }
  ASSERT_SUCCESS(aborted.Abort());
  {
    PageRef page = p_->GetPage(recycled_id).MoveValue();
    ASSERT_EQ(page->Type(), PageType::kFreePage);
    EXPECT_EQ(page->body.free_page.NextFreePage(), old_next);
  }
  {
    PageRef meta = p_->GetPage(0).MoveValue();
    EXPECT_EQ(meta->body.meta_page.FirstFreePage(), recycled_id);
    EXPECT_EQ(meta->body.meta_page.MaxPageCount(), 0U);
  }

  Transaction reuse = tm_->Begin();
  PageRef page = p_->AllocateNewPage(reuse, PageType::kRowPage).MoveValue();
  EXPECT_EQ(page->PageID(), recycled_id);
  ASSERT_SUCCESS(reuse.PreCommit());
}

TEST_F(PageManagerTest, DestroyPage) {
  // Arrange -- allocate and destroy 15 free pages
  for (int i = 0; i < 15; ++i) {
    PageRef page = AllocatePage(PageType::kFreePage);
    DestroyPage(page.get());
  }

  // Act -- re-allocate 15 free pages; PageManager should recycle destroyed page
  // IDs
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
  uint64_t page_id = 0;
  {
    // Arrange -- allocate a page and commit the allocation durably.  The
    // PageRef/Transaction must die *before* the simulated crash: a real
    // crash cannot leave live references into the destroyed pool.
    Transaction system_txn = tm_->Begin();
    PageRef allocated =
        p_->AllocateNewPage(system_txn, PageType::kFreePage).MoveValue();
    page_id = allocated->PageID();
    ASSERT_SUCCESS(system_txn.PreCommit());
    ASSERT_FALSE(allocated.IsNull());
  }

  // Act -- emulate a crash: discard every resident page without writeback,
  // then rebuild all managers on top of the surviving WAL.
  p_->GetPool()->DropAllPages();
  Reset();

  // Assert -- the committed page comes back after recovery.
  Transaction read_txn = tm_->Begin();
  PageRef recovered = p_->GetPage(page_id).MoveValue();
  ASSERT_FALSE(recovered.IsNull()) << (page_id != 0U);
  EXPECT_EQ(recovered->Type(), PageType::kFreePage);
  read_txn.PreCommit();
}

// Bug-probe variants around the row_page_fuzzer null-deref finding
// (crash-b709ae1f): a committed page must come back after crash recovery.
// These pin the blast radius of the "committed page lost" behaviour.
TEST_F(PageManagerTest, CommittedRowPageWithRowsSurvivesCrash) {
  // Arrange -- allocate a row page, insert two rows, commit everything.
  uint64_t page_id = 0;
  slot_t first_slot = 0;
  slot_t second_slot = 0;
  {
    Transaction txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kRowPage).MoveValue();
    page_id = page->PageID();
    const StatusOr<slot_t> s1 = page->Insert(txn, "first-row");
    const StatusOr<slot_t> s2 = page->Insert(txn, "second-row");
    ASSERT_SUCCESS(s1.GetStatus());
    ASSERT_SUCCESS(s2.GetStatus());
    ASSERT_SUCCESS(txn.PreCommit());
    first_slot = s1.Value();
    second_slot = s2.Value();
  }

  // Act -- crash: drop every resident page, rebuild from the WAL.
  p_->GetPool()->DropAllPages();
  Reset();

  // Assert -- both rows read back through a fresh page reference.
  PageRef recovered = p_->GetPage(page_id).MoveValue();
  ASSERT_FALSE(recovered.IsNull()) << (page_id != 0U);
  Transaction read_txn = tm_->Begin();
  const StatusOr<std::string_view> r1 = recovered->Read(read_txn, first_slot);
  EXPECT_EQ(r1.GetStatus(), Status::kSuccess);
  if (r1.HasValue()) {
    EXPECT_EQ(r1.Value(), "first-row");
  }
  const StatusOr<std::string_view> r2 = recovered->Read(read_txn, second_slot);
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
    PageRef page = p_->AllocateNewPage(txn, PageType::kFreePage).MoveValue();
    ids.push_back(page->PageID());
    ASSERT_SUCCESS(txn.PreCommit());
  }

  // Act -- crash and rebuild.
  p_->GetPool()->DropAllPages();
  Reset();

  // Assert -- every committed page is still there.
  for (const uint64_t id : ids) {
    PageRef recovered = p_->GetPage(id).MoveValue();
    EXPECT_FALSE(recovered.IsNull())
        << "committed page " << id << " lost by crash recovery";
  }
}

TEST_F(PageManagerTest, MetaPageSurvivesCrash) {
  // Arrange -- one committed allocation so the meta page has content.
  {
    Transaction txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kFreePage).MoveValue();
    ASSERT_SUCCESS(txn.PreCommit());
  }

  // Act -- crash and rebuild.
  p_->GetPool()->DropAllPages();
  Reset();

  // Assert -- meta page (id 0) always loads back.
  PageRef meta = p_->GetPage(0).MoveValue();
  EXPECT_FALSE(meta.IsNull());
  EXPECT_EQ(meta->Type(), PageType::kMetaPage);
}

TEST_F(PageManagerTest, GetPageForUnknownPageIdReturnsNullRef) {
  // Contract pin from the row_page_fuzzer crash (crash-b709ae1f): GetPage
  // signals "missing or broken page" with an EMPTY PageRef, not an error.
  // Callers (and harnesses) must check IsNull() before dereferencing - the
  // fuzzer harness did not and segfaulted.
  Transaction txn = tm_->Begin();
  const PageRef missing = p_->GetPage(999'999).MoveValue();
  EXPECT_TRUE(missing.IsNull());
  // The meta page, in contrast, always resolves.
  const PageRef meta = p_->GetPage(0).MoveValue();  // id 0 = meta page
  EXPECT_FALSE(meta.IsNull());
  txn.PreCommit();
}

TEST_F(PageManagerTest, TableTailHintAdvancesWithoutRegressing) {
  constexpr page_id_t first = 41;
  EXPECT_EQ(p_->GetTableTail(first, 41), 41);

  p_->AdvanceTableTail(first, 41, 42);
  EXPECT_EQ(p_->GetTableTail(first, 41), 42);

  // A thread that started from the old tail must not overwrite a newer hint.
  p_->AdvanceTableTail(first, 41, 99);
  EXPECT_EQ(p_->GetTableTail(first, 41), 42);

  p_->AdvanceTableTail(first, 42, 43);
  EXPECT_EQ(p_->GetTableTail(first, 41), 43);
}

TEST_F(PageManagerTest, TableTailReservationPublishesOnCommitAndHidesOnAbort) {
  Transaction owner = tm_->Begin();
  Transaction observer = tm_->Begin();
  constexpr page_id_t kFirst = 51;
  constexpr page_id_t kNext = 52;
  ASSERT_EQ(p_->GetTableTailFor(owner.ID(), kFirst, kFirst), kFirst);
  ASSERT_SUCCESS(p_->TryReserveTableTail(owner.ID(), kFirst, kFirst));
  ASSERT_SUCCESS(p_->LinkReservedTableTail(owner.ID(), kFirst, kFirst, kNext));
  EXPECT_EQ(p_->GetTableTailFor(observer.ID(), kFirst, kFirst), kFirst);
  EXPECT_EQ(p_->CheckTableTailPage(observer.ID(), kFirst, kNext),
            Status::kConflicts);
  ASSERT_SUCCESS(owner.PreCommit());
  EXPECT_EQ(p_->GetTableTailFor(observer.ID(), kFirst, kFirst), kNext);
  EXPECT_EQ(p_->CheckTableTailPage(observer.ID(), kFirst, kNext),
            Status::kSuccess);

  Transaction aborted = tm_->Begin();
  constexpr page_id_t kAbortedFirst = 61;
  constexpr page_id_t kAbortedNext = 62;
  ASSERT_SUCCESS(
      p_->TryReserveTableTail(aborted.ID(), kAbortedFirst, kAbortedFirst));
  ASSERT_SUCCESS(p_->LinkReservedTableTail(aborted.ID(), kAbortedFirst,
                                           kAbortedFirst, kAbortedNext));
  ASSERT_SUCCESS(aborted.Abort());
  EXPECT_EQ(p_->GetTableTailFor(observer.ID(), kAbortedFirst, kAbortedFirst),
            kAbortedFirst);
  EXPECT_EQ(p_->CheckTableTailPage(observer.ID(), kAbortedFirst, kAbortedNext),
            Status::kSuccess);
  observer.Abort();
}

TEST_F(PageManagerTest, EmptyLinkedRowPageDestroyAbortRestoresLinks) {
  page_id_t first_id = 0;
  page_id_t second_id = 0;
  {
    Transaction txn = tm_->Begin();
    PageRef first = p_->AllocateNewPage(txn, PageType::kRowPage).MoveValue();
    PageRef second = p_->AllocateNewPage(txn, PageType::kRowPage).MoveValue();
    first_id = first->PageID();
    second_id = second->PageID();
    ASSERT_SUCCESS(txn.PreCommit());
  }
  {
    Transaction txn = tm_->Begin();
    PageRef first = p_->GetPage(first_id).MoveValue();
    PageRef second = p_->GetPage(second_id).MoveValue();
    ASSERT_SUCCESS(first->SetNextPage(txn, second_id));
    ASSERT_SUCCESS(second->SetPrevPage(txn, first_id));
    ASSERT_SUCCESS(txn.PreCommit());
  }
  ASSERT_SUCCESS(p_->GetPool()->FlushPageForTest(first_id));
  ASSERT_SUCCESS(p_->GetPool()->FlushPageForTest(second_id));

  Transaction destroy = tm_->Begin();
  {
    PageRef first = p_->GetPage(first_id).MoveValue();
    ASSERT_EQ(first->body.row_page.RowCount(), 0U);
    ASSERT_EQ(first->body.row_page.next_page_id_, second_id);
    ASSERT_SUCCESS(p_->DestroyPage(destroy, first.get()));
  }
  ASSERT_SUCCESS(destroy.Abort());

  {
    PageRef first = p_->GetPage(first_id).MoveValue();
    PageRef second = p_->GetPage(second_id).MoveValue();
    EXPECT_EQ(first->Type(), PageType::kRowPage);
    EXPECT_EQ(second->Type(), PageType::kRowPage);
    EXPECT_EQ(first->body.row_page.RowCount(), 0U);
    EXPECT_EQ(first->body.row_page.next_page_id_, second_id);
    EXPECT_EQ(second->body.row_page.prev_page_id_, first_id);
  }

  p_->GetPool()->DropAllPages();
  Reset();
  PageRef first = p_->GetPage(first_id).MoveValue();
  PageRef second = p_->GetPage(second_id).MoveValue();
  EXPECT_EQ(first->Type(), PageType::kRowPage);
  EXPECT_EQ(second->Type(), PageType::kRowPage);
  EXPECT_EQ(first->body.row_page.next_page_id_, second_id);
  EXPECT_EQ(second->body.row_page.prev_page_id_, first_id);
}

TEST_F(PageManagerTest, UndoDestroyBelowNewerDestroyRelinksFreeList) {
  // Undoing a destroy whose free-list entry no longer heads the chain (a
  // newer uncommitted destroy sits above it) must unlink the middle entry:
  // the restored page A stays live and the allocator never re-issues it.
  // Before the chain walk, the entry stayed linked and every subsequent
  // AllocateNewPage spun forever re-peeking the dead head.
  page_id_t a_id = 0;
  page_id_t b_id = 0;
  {
    Transaction txn = tm_->Begin();
    PageRef a = p_->AllocateNewPage(txn, PageType::kRowPage).MoveValue();
    PageRef b = p_->AllocateNewPage(txn, PageType::kRowPage).MoveValue();
    a_id = a->PageID();
    b_id = b->PageID();
    EXPECT_EQ(txn.PreCommit(), Status::kSuccess);
  }
  Transaction older = tm_->Begin();
  {
    PageRef a = p_->GetPage(a_id).MoveValue();
    EXPECT_EQ(p_->DestroyPage(older, a.get()), Status::kSuccess);
  }
  Transaction newer = tm_->Begin();
  {
    PageRef b = p_->GetPage(b_id).MoveValue();
    EXPECT_EQ(p_->DestroyPage(newer, b.get()), Status::kSuccess);
  }
  EXPECT_EQ(newer.PreCommit(), Status::kSuccess);
  // B heads the chain now; undoing the older destroy takes the mid-chain
  // path (relink B.next instead of the meta head).
  EXPECT_EQ(older.Abort(), Status::kSuccess);

  {
    PageRef a = p_->GetPage(a_id).MoveValue();
    EXPECT_EQ(a->Type(), PageType::kRowPage);
  }
  Transaction txn = tm_->Begin();
  PageRef recycled = p_->AllocateNewPage(txn, PageType::kRowPage).MoveValue();
  EXPECT_EQ(recycled->PageID(), b_id);
  txn.PreCommit();
}

TEST_F(PageManagerTest, StaleFreeListHeadDoesNotStallAllocation) {
  // When the head names a page that is live again and no undo can relink it,
  // the allocator must abandon the chain (ids return at the startup rebuild)
  // instead of spinning on the dead head.
  page_id_t a_id = 0;
  page_id_t b_id = 0;
  {
    Transaction txn = tm_->Begin();
    PageRef a = p_->AllocateNewPage(txn, PageType::kRowPage).MoveValue();
    PageRef b = p_->AllocateNewPage(txn, PageType::kRowPage).MoveValue();
    a_id = a->PageID();
    b_id = b->PageID();
    EXPECT_EQ(txn.PreCommit(), Status::kSuccess);
  }
  Transaction newer = tm_->Begin();
  {
    PageRef b = p_->GetPage(b_id).MoveValue();
    EXPECT_EQ(p_->DestroyPage(newer, b.get()), Status::kSuccess);
  }
  EXPECT_EQ(newer.PreCommit(), Status::kSuccess);
  Transaction older = tm_->Begin();
  {
    PageRef a = p_->GetPage(a_id).MoveValue();
    EXPECT_EQ(p_->DestroyPage(older, a.get()), Status::kSuccess);
  }
  EXPECT_EQ(older.PreCommit(), Status::kSuccess);
  // Resurrect A behind the allocator's back (the state a lost undo leaves):
  // the chain head now names a live page.
  {
    PageRef a = p_->GetPage(a_id).MoveValue();
    a->PageInit(a_id, PageType::kRowPage);
  }

  Transaction txn = tm_->Begin();
  PageRef fresh = p_->AllocateNewPage(txn, PageType::kRowPage).MoveValue();
  EXPECT_NE(fresh->PageID(), a_id);
  EXPECT_NE(fresh->PageID(), b_id);
  EXPECT_EQ(fresh->Type(), PageType::kRowPage);
  txn.PreCommit();
}

TEST_F(PageManagerTest, RuntimeFreshAllocationAbortReusesId) {
  page_id_t allocated_id = 0;
  Transaction txn = tm_->Begin();
  {
    PageRef page = p_->AllocateNewPage(txn, PageType::kRowPage).MoveValue();
    allocated_id = page->PageID();
    EXPECT_EQ(txn.PrevLSN(), 0U);
  }
  LogRecord allocation;
  ASSERT_SUCCESS(l_->WaitForDurable(txn.PrevRecordEndLSN()));
  ASSERT_TRUE(rm_->ReadLog(txn.PrevLSN(), &allocation));
  EXPECT_EQ(allocation.type, LogType::kSystemAllocPage);
  EXPECT_EQ(allocation.allocation_kind, PageAllocationKind::kFresh);
  EXPECT_TRUE(allocation.undo_data.empty());
  EXPECT_EQ(allocation.redo_data.size(), kAllocationMetaPayloadSize);
  page_id_t old_first = 0;
  page_id_t old_max = 0;
  ASSERT_TRUE(
      DecodeAllocationMetaPayload(allocation.redo_data, &old_first, &old_max));
  EXPECT_EQ(old_first, 0U);
  EXPECT_EQ(old_max, 0U);
  {
    PageRef meta = p_->GetPage(0).MoveValue();
    EXPECT_EQ(meta->body.meta_page.MaxPageCount(), allocated_id);
  }

  ASSERT_SUCCESS(txn.Abort());

  {
    PageRef page = p_->GetPage(allocated_id).MoveValue();
    ASSERT_FALSE(page.IsNull());
    EXPECT_EQ(page->Type(), PageType::kFreePage);
  }
  {
    PageRef meta = p_->GetPage(0).MoveValue();
    EXPECT_EQ(meta->body.meta_page.FirstFreePage(), 0U);
    EXPECT_EQ(meta->body.meta_page.MaxPageCount(), 0U);
  }

  Transaction reuse = tm_->Begin();
  PageRef page = p_->AllocateNewPage(reuse, PageType::kRowPage).MoveValue();
  EXPECT_EQ(page->PageID(), allocated_id);
  ASSERT_SUCCESS(reuse.PreCommit());
}

TEST_F(PageManagerTest,
       RuntimeRecycledAllocationAbortRestoresFreeBodyAndReusesId) {
  page_id_t allocated_id = 0;
  page_id_t old_max = 0;
  {
    Transaction txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kRowPage).MoveValue();
    allocated_id = page->PageID();
    ASSERT_SUCCESS(txn.PreCommit());
  }
  {
    Transaction txn = tm_->Begin();
    PageRef page = p_->GetPage(allocated_id).MoveValue();
    ASSERT_SUCCESS(p_->DestroyPage(txn, page.get()));
    ASSERT_SUCCESS(txn.PreCommit());
  }
  std::string old_free_body;
  page_id_t old_next_free = 0;
  txn_id_t old_destroyer = 0;
  {
    PageRef page = p_->GetPage(allocated_id).MoveValue();
    old_free_body.assign(reinterpret_cast<const char*>(&page->body),
                         kPageBodySize);
    old_next_free = page->body.free_page.NextFreePage();
    old_destroyer = page->body.free_page.Destroyer();
    PageRef meta = p_->GetPage(0).MoveValue();
    old_max = meta->body.meta_page.MaxPageCount();
    ASSERT_EQ(meta->body.meta_page.FirstFreePage(), allocated_id);
  }
  Transaction txn = tm_->Begin();
  {
    PageRef page = p_->AllocateNewPage(txn, PageType::kRowPage).MoveValue();
    ASSERT_EQ(page->PageID(), allocated_id);
  }
  LogRecord allocation;
  ASSERT_SUCCESS(l_->WaitForDurable(txn.PrevRecordEndLSN()));
  ASSERT_TRUE(rm_->ReadLog(txn.PrevLSN(), &allocation));
  EXPECT_EQ(allocation.type, LogType::kSystemAllocPage);
  EXPECT_EQ(allocation.allocation_kind, PageAllocationKind::kRecycled);
  EXPECT_EQ(allocation.previous_page_type, PageType::kFreePage);
  EXPECT_EQ(allocation.undo_data.size(), kAllocationFreePayloadSize);
  EXPECT_EQ(allocation.redo_data.size(), kAllocationMetaPayloadSize);
  page_id_t logged_next_free = 0;
  txn_id_t logged_destroyer = 0;
  ASSERT_TRUE(DecodeAllocationFreePayload(
      allocation.undo_data, &logged_next_free, &logged_destroyer));
  EXPECT_EQ(logged_next_free, old_next_free);
  EXPECT_EQ(logged_destroyer, old_destroyer);
  page_id_t logged_first_free = 0;
  page_id_t logged_max = 0;
  ASSERT_TRUE(DecodeAllocationMetaPayload(allocation.redo_data,
                                          &logged_first_free, &logged_max));
  EXPECT_EQ(logged_first_free, allocated_id);
  EXPECT_EQ(logged_max, old_max);

  ASSERT_SUCCESS(txn.Abort());

  {
    PageRef page = p_->GetPage(allocated_id).MoveValue();
    ASSERT_EQ(page->Type(), PageType::kFreePage);
    EXPECT_EQ(
        std::string(reinterpret_cast<const char*>(&page->body), kPageBodySize),
        old_free_body);
  }
  {
    PageRef meta = p_->GetPage(0).MoveValue();
    EXPECT_EQ(meta->body.meta_page.FirstFreePage(), allocated_id);
    EXPECT_EQ(meta->body.meta_page.MaxPageCount(), old_max);
  }

  Transaction reuse = tm_->Begin();
  PageRef page = p_->AllocateNewPage(reuse, PageType::kRowPage).MoveValue();
  EXPECT_EQ(page->PageID(), allocated_id);
  ASSERT_SUCCESS(reuse.PreCommit());
}

TEST_F(PageManagerTest, InPlaceAllocationUsesFullUndoAndReplays) {
  page_id_t page_id = 0;
  {
    Transaction txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kRowPage).MoveValue();
    page_id = page->PageID();
    ASSERT_SUCCESS(page->Insert(txn, "before").GetStatus());
    ASSERT_SUCCESS(txn.PreCommit());
  }

  std::string old_body;
  {
    PageRef page = p_->GetPage(page_id).MoveValue();
    old_body.assign(reinterpret_cast<const char*>(&page->body), kPageBodySize);
  }

  Transaction aborted = tm_->Begin();
  {
    PageRef page = p_->GetPage(page_id).MoveValue();
    ASSERT_SUCCESS(page->PageTypeChange(aborted, PageType::kLeafPage));
  }
  LogRecord allocation;
  ASSERT_SUCCESS(l_->WaitForDurable(aborted.PrevRecordEndLSN()));
  ASSERT_TRUE(rm_->ReadLog(aborted.PrevLSN(), &allocation));
  EXPECT_EQ(allocation.type, LogType::kSystemAllocPage);
  EXPECT_EQ(allocation.allocation_kind, PageAllocationKind::kInPlace);
  EXPECT_EQ(allocation.previous_page_type, PageType::kRowPage);
  EXPECT_EQ(allocation.allocated_page_type, PageType::kLeafPage);
  EXPECT_EQ(allocation.undo_data.size(), kPageBodySize);
  EXPECT_EQ(allocation.redo_data.size(), kAllocationMetaPayloadSize);
  ASSERT_SUCCESS(aborted.Abort());

  {
    PageRef page = p_->GetPage(page_id).MoveValue();
    EXPECT_EQ(page->Type(), PageType::kRowPage);
    EXPECT_EQ(
        std::string(reinterpret_cast<const char*>(&page->body), kPageBodySize),
        old_body);
  }

  Transaction committed = tm_->Begin();
  {
    PageRef page = p_->GetPage(page_id).MoveValue();
    ASSERT_SUCCESS(page->PageTypeChange(committed, PageType::kLeafPage));
  }
  ASSERT_SUCCESS(committed.PreCommit());
  Reset();
  PageRef recovered = p_->GetPage(page_id).MoveValue();
  ASSERT_FALSE(recovered.IsNull());
  EXPECT_EQ(recovered->Type(), PageType::kLeafPage);
}

TEST_F(PageManagerTest,
       RecycledAllocationDefersVersionInvalidationUntilCommit) {
  page_id_t allocated_id = 0;
  Transaction snapshot = tm_->Begin(true);
  {
    Transaction txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kRowPage).MoveValue();
    allocated_id = page->PageID();
    ASSERT_SUCCESS(page->Insert(txn, "before").GetStatus());
    ASSERT_SUCCESS(txn.PreCommit());
  }
  {
    Transaction txn = tm_->Begin();
    PageRef page = p_->GetPage(allocated_id).MoveValue();
    ASSERT_SUCCESS(page->Update(txn, 0, "after"));
    ASSERT_SUCCESS(txn.PreCommit());
  }
  ASSERT_TRUE(tm_->HasPageVersionChains(allocated_id));
  {
    Transaction txn = tm_->Begin();
    PageRef page = p_->GetPage(allocated_id).MoveValue();
    ASSERT_SUCCESS(p_->DestroyPage(txn, page.get()));
    ASSERT_SUCCESS(txn.PreCommit());
  }

  Transaction aborted = tm_->Begin();
  {
    PageRef page = p_->AllocateNewPage(aborted, PageType::kRowPage).MoveValue();
    ASSERT_EQ(page->PageID(), allocated_id);
    ASSERT_SUCCESS(page->Insert(aborted, "aborted-incarnation").GetStatus());
  }
  ASSERT_SUCCESS(aborted.Abort());
  EXPECT_TRUE(tm_->HasPageVersionChains(allocated_id));

  Transaction committed = tm_->Begin();
  {
    PageRef page =
        p_->AllocateNewPage(committed, PageType::kRowPage).MoveValue();
    ASSERT_EQ(page->PageID(), allocated_id);
    ASSERT_SUCCESS(page->Insert(committed, "new-incarnation").GetStatus());
  }
  ASSERT_SUCCESS(committed.PreCommit());
  {
    PageRef page = p_->GetPage(allocated_id).MoveValue();
    ASSERT_EQ(page->RowCount(), 1U);
    EXPECT_EQ(page->body.row_page.GetRow(0), "new-incarnation");
  }
  ASSERT_SUCCESS(snapshot.PreCommit());
}

TEST(PageManagerAllocationFailureTest, RollsBackAllocatorBeforeReturning) {
  const std::string prefix = "page_manager_alloc_failure-" + RandomString();
  const std::string db_name = prefix + ".db";
  const std::string log_name = prefix + ".log";
  constexpr size_t kSegmentSize = 64;
  auto manager = PageManager::Create(db_name, 10, false).MoveValue();
  auto logger = Logger::Create(log_name, 4096, 1, kSegmentSize).MoveValue();
  auto recovery = std::make_unique<RecoveryManager>(
      log_name, manager->GetPool(), kSegmentSize);
  auto transactions = std::make_unique<TransactionManager>(
      manager.get(), logger.get(), recovery.get());
  PageType target_type_before = PageType::kUnknown;
  {
    PageRef target = manager->GetPool()->GetPage(1).MoveValue();
    target_type_before = target->Type();
  }
  Transaction txn = transactions->Begin();
  const auto result =
      manager->AllocateNewPage(txn, PageType::kRowPage).GetStatus();
  EXPECT_EQ(result, Status::kTooBigData);
  EXPECT_EQ(txn.PrevLSN(), 0U);
  ASSERT_SUCCESS(txn.Abort());

  {
    PageRef meta = manager->GetPage(0).MoveValue();
    EXPECT_EQ(meta->body.meta_page.FirstFreePage(), 0U);
    EXPECT_EQ(meta->body.meta_page.MaxPageCount(), 0U);
    PageRef target = manager->GetPool()->GetPage(1).MoveValue();
    EXPECT_EQ(target->Type(), target_type_before);
  }

  manager->GetPool()->DropAllPages();
  transactions.reset();
  recovery.reset();
  logger->Finish();
  manager.reset();
  std::ignore = std::remove(db_name.c_str());
  RemoveLogAndSegments(log_name);
}

}  // namespace tinylamb
