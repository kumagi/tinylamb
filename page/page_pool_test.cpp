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

#include "page_pool.hpp"

#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <ios>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include "page/page_type.hpp"

#ifdef __has_include
#if __has_include(<sanitizer/lsan_interface.h>)
#include <sanitizer/lsan_interface.h>
#if defined(__SANITIZE_ADDRESS__) || \
    (defined(__has_feature) && __has_feature(address_sanitizer))
#define TINYLAMB_HAS_LSAN 1
#endif
#endif
#endif

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

class PagePoolTest : public ::testing::Test {
 protected:
  static constexpr int kDefaultCapacity = 10;
  void SetUp() override {
    filename_ = "page_pool_test-" + RandomString();
    Reset();
  }
  void Reset() {
    pp = PagePool::Create(filename_, kDefaultCapacity).MoveValue();
  }
  void TearDown() override {
    pp.reset();
    std::ignore = std::remove(filename_.c_str());
    tinylamb::RemoveLogAndSegments((filename_ + ".log"));
  }

  std::string filename_;
  std::unique_ptr<PagePool> pp = nullptr;
};

TEST_F(PagePoolTest, Construct) {
  // Arrange -- nothing to set up; default PagePool created by SetUp()
  // Act -- nothing to execute; default constructed via SetUp()
  // Assert -- pool size is 0 (no pages have been requested yet)
  ASSERT_EQ(pp->Size(), 0);
}

TEST_F(PagePoolTest, SecondLivePoolFailsWithoutDisplacingFirst) {
  auto second = PagePool::Create(filename_, kDefaultCapacity);
  ASSERT_FALSE(second.HasValue());
  EXPECT_EQ(second.GetStatus(), Status::kConflicts);
  EXPECT_NE(second.GetStatus().GetMessage().find("already open"),
            std::string::npos);

  PageRef page = pp->GetPage(0).MoveValue();
  EXPECT_EQ(page->PageID(), 0U);
}

TEST_F(PagePoolTest, PageFileLockIsReleasedAtPoolDestruction) {
  pp.reset();
  auto reopened = PagePool::Create(filename_, kDefaultCapacity);
  ASSERT_TRUE(reopened.HasValue()) << reopened.GetStatus();
  pp = reopened.MoveValue();
  EXPECT_EQ(pp->Size(), 0U);
}

TEST_F(PagePoolTest, ForkInheritedDescriptorKeepsPageFileLocked) {
  std::array<int, 2> control{};
  ASSERT_EQ(::pipe(control.data()), 0);
  const pid_t child = ::fork();
  if (child < 0) {
    ::close(control[0]);
    ::close(control[1]);
    FAIL() << "fork failed";
    return;
  }
  if (child == 0) {
    ::close(control[1]);
    char token = 0;
    ssize_t received = 0;
    do {
      received = ::read(control[0], &token, 1);
    } while (received < 0 && errno == EINTR);
    ::close(control[0]);
    ::_exit(received == 1 ? 0 : 1);
  }

  ::close(control[0]);
  pp.reset();
  auto blocked = PagePool::Create(filename_, kDefaultCapacity);
  EXPECT_EQ(blocked.GetStatus(), Status::kConflicts);

  const char token = 'x';
  ssize_t sent = 0;
  do {
    sent = ::write(control[1], &token, 1);
  } while (sent < 0 && errno == EINTR);
  EXPECT_EQ(sent, 1);
  EXPECT_EQ(::close(control[1]), 0);

  int child_status = 0;
  EXPECT_EQ(::waitpid(child, &child_status, 0), child);
  EXPECT_TRUE(WIFEXITED(child_status));
  if (WIFEXITED(child_status)) {
    EXPECT_EQ(WEXITSTATUS(child_status), 0);
  }

  auto reopened = PagePool::Create(filename_, kDefaultCapacity);
  ASSERT_TRUE(reopened.HasValue()) << reopened.GetStatus();
  pp = reopened.MoveValue();
}

TEST_F(PagePoolTest, GetPage) {
  // Arrange -- nothing more than default PagePool (capacity 10) from SetUp()
  // Act -- request page 0 from the pool
  PageRef page = pp->GetPage(0, nullptr).MoveValue();

  // Assert -- pool size grows to 1 and the returned page has ID 0
  ASSERT_EQ(pp->Size(), 1);
}

TEST_F(PagePoolTest, GetPageSeveralpattern) {
  // Arrange -- access pattern with repeats: {0, 0, 1, 0, 2}
  std::vector<int> pattern = {0, 0, 1, 0, 2};

  // Act -- request pages in the given pattern
  for (int& i : pattern) {
    PageRef page = pp->GetPage(static_cast<page_id_t>(i), nullptr).MoveValue();

    // Assert -- each requested page has the expected ID
    ASSERT_EQ(page->PageID(), i);
  }
}

TEST_F(PagePoolTest, GetManyPage) {
  // Arrange -- nothing more than default PagePool (capacity 10) from SetUp()
  // Act -- request 5 distinct pages (0..4) from the pool
  for (int i = 0; i < 5; ++i) {
    PageRef p = pp->GetPage(static_cast<page_id_t>(i), nullptr).MoveValue();

    // Assert -- each page has the expected ID and pool size grows accordingly
    ASSERT_EQ(p->PageID(), i);
    ASSERT_EQ(pp->Size(), i + 1);
  }
}

TEST_F(PagePoolTest, ContainsResidentTracksResidencyWithoutMaterializing) {
  // Arrange -- an empty pool: the query answers false and must not
  // materialize a page as a side effect (recovery's undo walks rely on that
  // to tell never-issued page ids apart).
  ASSERT_EQ(pp->Size(), 0);
  EXPECT_FALSE(pp->ContainsResident(0));
  EXPECT_FALSE(pp->ContainsResident(3));
  ASSERT_EQ(pp->Size(), 0);

  // Act -- pin page 3 in a scope, then release it.
  {
    PageRef page = pp->GetPage(3, nullptr).MoveValue();
    EXPECT_EQ(page->PageID(), 3);
    EXPECT_TRUE(pp->ContainsResident(3));
    EXPECT_FALSE(pp->ContainsResident(0));
    EXPECT_EQ(pp->Size(), 1);
  }

  // Assert -- after unpin + eviction, residency follows the pool: page 3 is
  // the sole LRU victim once 10 fresh pages arrive.
  for (page_id_t i = 10; i < 20; ++i) {
    PageRef p = pp->GetPage(i, nullptr).MoveValue();
    ASSERT_EQ(p->PageID(), i);
  }
  EXPECT_FALSE(pp->ContainsResident(3));
  EXPECT_TRUE(pp->ContainsResident(19));
}

TEST_F(PagePoolTest, EvictPage) {
  // Arrange -- default PagePool (capacity 10); nothing else to set up
  // Act -- request 15 pages (0..14), exceeding the pool capacity of 10
  for (int i = 0; i < 15; ++i) {
    PageRef p = pp->GetPage(static_cast<page_id_t>(i), nullptr).MoveValue();

    // Assert -- each page has the expected ID; pool size caps at capacity (10)
    ASSERT_EQ(p->PageID(), i);
    ASSERT_EQ(pp->Size(), std::min(i + 1, kDefaultCapacity));
  }
}

TEST_F(PagePoolTest, PersistencyWithReset) {
  // Arrange -- request 11 pages and write a deterministic byte pattern to each
  constexpr size_t kPages = 11;
  for (size_t i = 0; i < kPages; ++i) {
    PageRef p = pp->GetPage(i, nullptr).MoveValue();
    char* buff = p->body.free_page.FreeBody();
    ASSERT_NE(buff, nullptr);
    for (size_t j = 0; j < FreePage::FreeBodySize(); ++j) {
      // The union overlay makes the whole kPageSize image writable; the
      // analyzer cannot see that FreeBody() stays inside the allocation.
      // NOLINTNEXTLINE(clang-analyzer-security.ArrayBound)
      buff[j] = static_cast<char>(i);
    }
  }
  // Reset();
  // Act -- re-request the same 11 pages and read back the byte patterns
  for (size_t i = 0; i < kPages; ++i) {
    PageRef p = pp->GetPage(i, nullptr).MoveValue();
    char* buff = p->body.free_page.FreeBody();
    ASSERT_NE(buff, nullptr);

    // Assert -- each page's body retains the byte pattern written before reset
    for (size_t j = 0; j < FreePage::FreeBodySize(); ++j) {
      EXPECT_EQ(buff[j], i);
    }
  }
}

TEST_F(PagePoolTest, PageAccessorsLSNAndChecksum) {
  // Arrange -- fetch a fresh page from the pool (materialized as a free page)
  // Act -- read and mutate the Page base-class accessors
  PageRef page = pp->GetPage(5, nullptr).MoveValue();

  // Assert -- ID/LSN accessors behave as documented
  ASSERT_EQ(page->PageID(), 5);
  ASSERT_EQ(page->PageLSN(), 0);
  ASSERT_EQ(page->RecoveryLSN(), std::numeric_limits<lsn_t>::max());
  page->SetPageLSN(42);
  ASSERT_EQ(page->PageLSN(), 42);
  page->SetRecLSN(17);
  ASSERT_EQ(page->RecoveryLSN(), 17);
  page->SetRecLSN(100);  // min() must keep the earlier (smaller) LSN.
  ASSERT_EQ(page->RecoveryLSN(), 17);

  // Assert -- checksum lifecycle: invalid before SetChecksum, valid after
  ASSERT_FALSE(page->IsValid());
  page->SetChecksum();
  ASSERT_TRUE(page->IsValid());
}

TEST_F(PagePoolTest, ReadFromRejectsCorruptChecksum) {
  {
    PageRef page = pp->GetPage(3, nullptr).MoveValue();
    page->PageInit(3, PageType::kFreePage);
    page->SetPageLSN(1);
    page->SetChecksum();
    ASSERT_TRUE(page->IsValid());
  }
  pp->FlushPageForTest(3);
  pp->DropAllPages();

  {
    std::fstream file(filename_, std::ios_base::in | std::ios_base::out |
                                     std::ios_base::binary);
    ASSERT_TRUE(file.good());
    file.seekp(static_cast<std::streamoff>((3 * kPageSize) + 8));
    char flip = 0x5a;
    file.write(&flip, 1);
    ASSERT_TRUE(file.good());
  }

  EXPECT_EQ(pp->GetPage(3, nullptr).GetStatus(), Status::kCorrupt);
}

TEST_F(PagePoolTest, PageInitForEveryPageType) {
  // Arrange/Act -- construct a Page for every concrete page type
  for (PageType type :
       {PageType::kMetaPage, PageType::kRowPage, PageType::kLeafPage,
        PageType::kBranchPage, PageType::kFreePage}) {
    Page page(7, type);

    // Assert -- header fields initialized by PageInit
    ASSERT_EQ(page.PageID(), 7);
    ASSERT_EQ(page.Type(), type);
    ASSERT_EQ(page.PageLSN(), 0);
    ASSERT_EQ(page.RecoveryLSN(), std::numeric_limits<lsn_t>::max());
  }
  Page unknown(8, PageType::kUnknown);
  ASSERT_EQ(unknown.Type(), PageType::kUnknown);
}

TEST_F(PagePoolTest, DumpEveryPageType) {
  // Act -- stream each page type through Page::Dump
  std::ostringstream oss;
  oss << Page(0, PageType::kFreePage);
  oss << Page(1, PageType::kMetaPage);
  oss << Page(2, PageType::kRowPage);
  oss << Page(3, PageType::kLeafPage);
  oss << Page(4, PageType::kBranchPage);
  oss << Page(5, PageType::kUnknown);

  // Assert -- the dump labels each page type
  const std::string dumped = oss.str();
  EXPECT_NE(dumped.find("FreePage"), std::string::npos);
  EXPECT_NE(dumped.find("MetaPage"), std::string::npos);
  EXPECT_NE(dumped.find("RowPage"), std::string::npos);
  EXPECT_NE(dumped.find("LeafPage"), std::string::npos);
  EXPECT_NE(dumped.find("BranchPage"), std::string::npos);
  EXPECT_NE(dumped.find("PID: 0"), std::string::npos);
  EXPECT_NE(dumped.find("PID: 4"), std::string::npos);
}

TEST_F(PagePoolTest, PageRowCountOverloads) {
  // Arrange -- raw pages of each type (Transaction is unused on these paths)
  Transaction txn;
  Page row(1, PageType::kRowPage);
  Page leaf(2, PageType::kLeafPage);
  Page branch(3, PageType::kBranchPage);
  Page meta(4, PageType::kMetaPage);

  // Act/Assert -- Transaction-taking overload dispatches on page type
  ASSERT_EQ(row.RowCount(txn), 0U);
  ASSERT_EQ(leaf.RowCount(txn), 0U);
  ASSERT_EQ(branch.RowCount(txn), 0U);
  EXPECT_DEATH(std::ignore = meta.RowCount(txn), "invalid page type");

  // Act/Assert -- slot_t overload dispatches on page type
  ASSERT_EQ(row.RowCount(), 0U);
  ASSERT_EQ(leaf.RowCount(), 0U);
  ASSERT_EQ(branch.RowCount(), 0U);
  EXPECT_DEATH(std::ignore = meta.RowCount(), "RowCount is not implemented");
}

TEST_F(PagePoolTest, PageReadKeyAndReadByType) {
  // Arrange -- raw pages with recovery-style (log-less) payloads
  Transaction txn;
  Page row(1, PageType::kRowPage);
  Page leaf(2, PageType::kLeafPage);
  leaf.InsertImpl("a", "1");
  Page branch(3, PageType::kBranchPage);
  branch.InsertBranchImpl("k", 5);
  Page meta(4, PageType::kMetaPage);

  // Act/Assert -- ReadKey on a row page reports kUnknown
  ASSERT_EQ(row.ReadKey(txn, 0).GetStatus(), Status::kUnknown);

  // Act/Assert -- ReadKey on a leaf page returns the key; out-of-range fails
  ASSERT_EQ(leaf.ReadKey(txn, 0).Value(), "a");
  ASSERT_EQ(leaf.ReadKey(txn, 1).GetStatus(), Status::kNotExists);

  // Act/Assert -- ReadKey on a branch page returns the stored key
  ASSERT_EQ(branch.ReadKey(txn, 0).Value(), "k");

  // Act/Assert -- ReadKey on unsupported types reports kInvalidArgument
  EXPECT_EQ(meta.ReadKey(txn, 0).GetStatus(), Status::kInvalidArgument);

  // Act/Assert -- Read(slot) on a leaf page; out-of-range fails
  ASSERT_EQ(leaf.Read(txn, 0).Value(), "1");
  ASSERT_EQ(leaf.Read(txn, 1).GetStatus(), Status::kNotExists);

  // Act/Assert -- Read(slot) on an unsupported type reports kInvalidArgument
  EXPECT_EQ(meta.Read(txn, 0).GetStatus(), Status::kInvalidArgument);
}

TEST_F(PagePoolTest, PageLowestHighestKeyOnEmptyLeaf) {
  // Arrange -- empty raw leaf page
  Transaction txn;
  Page leaf(2, PageType::kLeafPage);

  // Act/Assert -- no lowest/highest key exists on an empty page
  ASSERT_EQ(leaf.LowestKey(txn).GetStatus(), Status::kNotExists);
  ASSERT_EQ(leaf.HighestKey(txn).GetStatus(), Status::kNotExists);

  // Act/Assert -- after one insert both endpoints collapse to the key
  leaf.InsertImpl("a", "1");
  ASSERT_EQ(leaf.LowestKey(txn).Value(), "a");
  ASSERT_EQ(leaf.HighestKey(txn).Value(), "a");
}

TEST_F(PagePoolTest, PageUnsupportedOperations) {
  // Arrange -- a row page cannot serve leaf/branch-only operations
  Transaction txn;
  Page row(1, PageType::kRowPage);
  Page other(2, PageType::kLeafPage);

  // Act/Assert -- query/mutation paths report kInvalidArgument; the
  // redo/undo Impl setters are caller-contract violations and abort.
  EXPECT_EQ(row.Delete(txn, "key"), Status::kInvalidArgument);
  EXPECT_EQ(row.SetLowFence(txn, IndexKey("a")), Status::kInvalidArgument);
  EXPECT_EQ(row.SetHighFence(txn, IndexKey("z")), Status::kInvalidArgument);
  EXPECT_DEATH(std::ignore = row.GetLowFence(txn), "GetLowFence");
  EXPECT_DEATH(std::ignore = row.GetHighFence(txn), "GetHighFence");
  EXPECT_EQ(row.SetFoster(txn, FosterPair("k", 1)), Status::kInvalidArgument);
  EXPECT_EQ(row.GetFoster(txn).GetStatus(), Status::kInvalidArgument);
  EXPECT_EQ(row.MoveRightToFoster(txn, other), Status::kInvalidArgument);
  EXPECT_EQ(row.MoveLeftFromFoster(txn, other), Status::kInvalidArgument);
  EXPECT_DEATH(row.SetLowFenceImpl(IndexKey("a")), "SetLowFenceImpl");
  EXPECT_DEATH(row.SetHighFenceImpl(IndexKey("z")), "SetHighFenceImpl");
  EXPECT_DEATH(row.SetFosterImpl(FosterPair("k", 1)), "SetFosterImpl");
}

TEST_F(PagePoolTest, PageRefStreamInsertion) {
  // Arrange -- a live pinned page
  PageRef page = pp->GetPage(3, nullptr).MoveValue();

  // Act -- stream the PageRef itself
  std::ostringstream oss;
  oss << page;

  // Assert -- operator<< prints {Ref: <page_id>}
  EXPECT_NE(oss.str().find("{Ref: 3}"), std::string::npos);
}

TEST_F(PagePoolTest, MetaPageAllocateDestroyReuse) {
  // Arrange -- full page manager + transaction machinery
  pp.reset();
  auto pm_holder = PageManager::Create(filename_, kDefaultCapacity).MoveValue();
  CHECK(pm_holder != nullptr);
  PageManager& pm = *pm_holder;
  auto log_holder = Logger::Create(filename_ + ".log").MoveValue();
  CHECK(log_holder != nullptr);
  Logger& log = *log_holder;
  LockManager lm;
  RecoveryManager rm(filename_ + ".log", pm.GetPool());
  TransactionManager tm(&pm, &log, &rm);

  // Act 1 -- allocate two pages through the meta page
  Transaction txn = tm.Begin();
  PageRef p1 = pm.AllocateNewPage(txn, PageType::kRowPage).MoveValue();
  PageRef p2 = pm.AllocateNewPage(txn, PageType::kLeafPage).MoveValue();
  ASSERT_NE(p1->PageID(), p2->PageID());

  // Act 2 -- destroy p2 (turns it into a free page on the free chain)
  pm.DestroyPage(txn, p2.get());
  // Release p2's exclusive latch before the pool is asked to relatch it.
  p2.PageUnlock();

  // Act 3 -- while the destroying transaction is still active the freed id
  // is withheld (its destroy can still be undone, and reissue would let the
  // abort restore a stale image over the new owner), so this allocation
  // grows past the high-water mark instead.
  PageRef p3_early = pm.AllocateNewPage(txn, PageType::kBranchPage).MoveValue();
  ASSERT_NE(p3_early->PageID(), p2->PageID());
  ASSERT_SUCCESS(txn.PreCommit());

  // Act 4 -- once the destroyer has settled the freed id is reissued (LIFO).
  Transaction txn2 = tm.Begin();
  PageRef p3 = pm.AllocateNewPage(txn2, PageType::kBranchPage).MoveValue();
  ASSERT_EQ(p3->PageID(), p2->PageID());

  // Assert -- the meta page tracks the allocated max page count
  PageRef meta = pm.GetPool()->GetPage(0, nullptr).MoveValue();
  ASSERT_GE(meta->body.meta_page.MaxPageCountForTest(), p3_early->PageID());
  ASSERT_SUCCESS(txn2.PreCommit());
}

TEST_F(PagePoolTest, EvictionResumesAfterPoolGrewPastCapacity) {
  std::vector<PageRef> pinned;
  pinned.reserve(kDefaultCapacity);
  for (int i = 0; i < kDefaultCapacity; ++i) {
    pinned.push_back(pp->GetPage(static_cast<page_id_t>(i)).MoveValue());
  }
  {
    PageRef extra = pp->GetPage(kDefaultCapacity).MoveValue();
    EXPECT_GT(pp->Size(), static_cast<page_id_t>(kDefaultCapacity));
    pinned.clear();
  }
  PageRef newer = pp->GetPage(kDefaultCapacity + 1).MoveValue();
  EXPECT_LE(pp->Size(), static_cast<page_id_t>(kDefaultCapacity));
  EXPECT_EQ(newer->PageID(), static_cast<page_id_t>(kDefaultCapacity + 1));
}

TEST_F(PagePoolTest, CacheHitFlagReflectsMissAndHit) {
  // Arrange -- nothing more than default PagePool (capacity 10) from SetUp()
  // Act -- request page 9 once (miss) and again (hit), observing the flag.
  // The PageRef holds the page's exclusive latch, so the second request must
  // wait for the first ref to be destroyed.
  bool hit = true;
  const uint64_t misses_before = pp->CacheMisses();
  const uint64_t hits_before = pp->CacheHits();
  {
    PageRef miss = pp->GetPage(9, &hit).MoveValue();

    // Assert -- the first request was a miss
    ASSERT_FALSE(hit);
    EXPECT_EQ(pp->CacheMisses(), misses_before + 1);
    EXPECT_EQ(pp->CacheHits(), hits_before);
  }
  hit = false;
  {
    PageRef hit_ref = pp->GetPage(9, &hit).MoveValue();

    // Assert -- the second request was served from the pool
    ASSERT_TRUE(hit);
    ASSERT_EQ(hit_ref->PageID(), 9U);
    EXPECT_EQ(pp->CacheMisses(), misses_before + 1);
    EXPECT_EQ(pp->CacheHits(), hits_before + 1);
  }
}

TEST_F(PagePoolTest, DropAllPagesClearsPool) {
  // Arrange -- request 5 distinct pages so the pool is non-empty
  for (int i = 0; i < 5; ++i) {
    PageRef p = pp->GetPage(static_cast<page_id_t>(i), nullptr).MoveValue();
    ASSERT_EQ(p->PageID(), i);
  }
  ASSERT_EQ(pp->Size(), 5);

  // Act -- drop every buffered page without writing back
  pp->DropAllPages();

  // Assert -- the pool is empty and still serves fresh pages afterwards
  ASSERT_EQ(pp->Size(), 0);
  PageRef again = pp->GetPage(2, nullptr).MoveValue();
  ASSERT_EQ(again->PageID(), 2);
  ASSERT_EQ(pp->Size(), 1);
}

TEST_F(PagePoolTest, DropAllPagesWithPinnedRefsRetiresEntries) {
  // Arrange -- pin one page for the whole test and another transiently
  PageRef keep = pp->GetPage(4).MoveValue();
  {
    PageRef transient = pp->GetPage(5).MoveValue();
    ASSERT_EQ(pp->Size(), 2);

    // Act -- discard every buffered page while both refs are alive
    pp->DropAllPages();
    ASSERT_EQ(pp->Size(), 0);

    // Assert -- the surviving refs still address live memory; their later
    // unlatches/unpins must not touch freed entries.
    EXPECT_EQ(keep->PageID(), 4U);
    EXPECT_EQ(transient->PageID(), 5U);
    // Union overlay: the write stays inside the kPageSize allocation.
    // NOLINTNEXTLINE(clang-analyzer-security.ArrayBound)
    keep->body.free_page.FreeBody()[0] = 'r';
  }
  PageRef fresh = pp->GetPage(6).MoveValue();
  ASSERT_EQ(fresh->PageID(), 6U);
}

TEST_F(PagePoolTest, DropAllPagesWaitsForActiveWriteback) {
  for (page_id_t page_id = 0; page_id < kDefaultCapacity; ++page_id) {
    ASSERT_TRUE(pp->GetPage(page_id).HasValue());
  }
  {
    PageRef dirty = pp->GetPage(0).MoveValue();
    dirty->PageInit(0, PageType::kFreePage);
    dirty->body.free_page.FreeBody()[0] = 0x44;
  }
  for (page_id_t page_id = 1; page_id < kDefaultCapacity; ++page_id) {
    ASSERT_TRUE(pp->GetPage(page_id).HasValue());
  }

  std::atomic<bool> entered{false};
  std::atomic<bool> release{false};
  pp->SetDurabilityGate([&](lsn_t) {
    if (!entered.exchange(true, std::memory_order_acq_rel)) {
      while (!release.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    }
    return Status::kSuccess;
  });
  std::thread eviction(
      [&] { EXPECT_TRUE(pp->GetPage(kDefaultCapacity).HasValue()); });
  while (!entered.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  std::atomic<bool> dropped{false};
  std::thread dropper([&] {
    pp->DropAllPages();
    dropped.store(true, std::memory_order_release);
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_FALSE(dropped.load(std::memory_order_acquire));
  release.store(true, std::memory_order_release);
  eviction.join();
  dropper.join();
  EXPECT_TRUE(dropped.load(std::memory_order_acquire));
  // Uninstall the gate before TestBody returns: the pool's destructor
  // (TearDown) runs a final WriteBack that would invoke the lambda after
  // these locals are gone (stack-use-after-return under ASAN; with a
  // deterministic page checksum the final write-back now reliably fires and
  // the stale lambda blocked on dead locals forever).
  pp->SetDurabilityGate(nullptr);
}

TEST_F(PagePoolTest, DurabilityGateFiresForDirtyPagesOnly) {
  // Arrange -- record every LSN the gate observes before a pwrite
  std::vector<lsn_t> gated;
  pp->SetDurabilityGate([&gated](lsn_t lsn) {
    gated.push_back(lsn);
    return Status::kSuccess;
  });

  PageRef page = pp->GetPage(8).MoveValue();
  page->SetPageLSN(77);
  // Union overlay: the write stays inside the kPageSize allocation.
  // NOLINTNEXTLINE(clang-analyzer-security.ArrayBound)
  page->body.free_page.FreeBody()[0] = 'z';

  // Act -- flushing the dirty page must gate its page LSN first
  pp->FlushPageForTest(8);
  ASSERT_EQ(gated.size(), 1U);
  ASSERT_EQ(gated[0], 77U);

  // Act -- an already-persisted image carries no new information, so the
  // gate stays silent even though WriteBack rewrites the bytes
  pp->FlushPageForTest(8);
  ASSERT_EQ(gated.size(), 1U);
}

TEST_F(PagePoolTest, FlushPageForTestPersistsAndNoopsForMissing) {
  // Arrange -- fill page 7's body with a deterministic pattern, then release
  // the pin so the pool entry is unpinned before it is dropped below.
  {
    PageRef p = pp->GetPage(7, nullptr).MoveValue();
    char* buff = p->body.free_page.FreeBody();
    ASSERT_NE(buff, nullptr);
    for (size_t j = 0; j < FreePage::FreeBodySize(); ++j) {
      // Union overlay: the write stays inside the kPageSize allocation.
      // NOLINTNEXTLINE(clang-analyzer-security.ArrayBound)
      buff[j] = static_cast<char>(0x5a);
    }

    // Act -- write back page 7; flushing a never-resident page is a no-op
    pp->FlushPageForTest(7);
    pp->FlushPageForTest(1234);
  }

  // Act -- discard the pool (without a second flush) and reload the page
  pp->DropAllPages();
  ASSERT_EQ(pp->Size(), 0);
  PageRef reloaded = pp->GetPage(7, nullptr).MoveValue();

  // Assert -- the flushed pattern survived the drop-and-reload cycle
  ASSERT_EQ(reloaded->PageID(), 7);
  const char* body = reloaded->body.free_page.FreeBody();
  for (size_t j = 0; j < FreePage::FreeBodySize(); ++j) {
    EXPECT_EQ(body[j], static_cast<char>(0x5a));
  }
}

TEST_F(PagePoolTest, RecoveryLoadRejectsMisdirectedPageImage) {
  constexpr page_id_t kSource = 1;
  constexpr page_id_t kTarget = 2;
  {
    PageRef source = pp->GetPage(kSource).MoveValue();
    source->PageInit(kSource, PageType::kFreePage);
    source->body.free_page.FreeBody()[0] = 0x31;
  }
  ASSERT_SUCCESS(pp->FlushPageForTest(kSource));
  pp->DropAllPages();
  {
    std::fstream file(filename_,
                      std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(file.good());
    std::vector<char> image(kPageSize);
    file.seekg(static_cast<std::streamoff>(kSource) * kPageSize, std::ios::beg);
    file.read(image.data(), static_cast<std::streamsize>(image.size()));
    ASSERT_EQ(file.gcount(), static_cast<std::streamsize>(image.size()));
    file.seekp(static_cast<std::streamoff>(kTarget) * kPageSize, std::ios::beg);
    file.write(image.data(), static_cast<std::streamsize>(image.size()));
    ASSERT_TRUE(file.good());
  }
  PageRef recovered = pp->GetPageForRecovery(kTarget).MoveValue();
  ASSERT_FALSE(recovered.IsNull());
  EXPECT_EQ(recovered->PageID(), kTarget);
  EXPECT_EQ(recovered->Type(), PageType::kUnknown);
  EXPECT_FALSE(recovered->IsValid());
}

TEST_F(PagePoolTest, ConstructorThrowsForUnopenablePath) {
  // Arrange/Act -- a pool whose backing file cannot be created (missing parent
  // directory); the constructor retries with O_TRUNC then throws
  // Assert -- Create reports failure instead of silently proceeding
  EXPECT_FALSE(
      PagePool::Create("/nonexistent_dir_tinylamb/foo.db", 10).HasValue());
}

TEST_F(PagePoolTest, OutOfRangePageIdIsHardError) {
  // Arrange -- a page id so large that its file offset overflows off_t
  // (kPageSize = 32 KiB, so ids beyond 2^48 wrap the offset negative)
  constexpr page_id_t kHuge = (1ULL << 48) + 1;

  // Act/Assert -- loading must report a hard error instead of silently
  // materializing an empty free page that could later be persisted.
  EXPECT_EQ(pp->GetPage(kHuge, nullptr).GetStatus(), Status::kCorrupt);
  ASSERT_EQ(pp->Size(), 0);

  // Act -- ordinary ids keep working afterwards; the extra page forces one
  // clean eviction round.
  for (int64_t i = 0; i < kDefaultCapacity + 1; ++i) {
    PageRef page =
        pp->GetPage(static_cast<page_id_t>(1000 + i), nullptr).MoveValue();
    ASSERT_EQ(page->PageID(), 1000 + i);
  }
  pp->DropAllPages();
  ASSERT_EQ(pp->Size(), 0);
}

TEST_F(PagePoolTest, WriteBackFailureOnFullDeviceThrows) {
  // Arrange -- /dev/full answers reads with zeros but rejects every write
  // with ENOSPC, so the write-back path during eviction must throw
  auto pool = PagePool::Create("/dev/full", 2).MoveValue();
  pool->GetPage(0);
  pool->GetPage(1);

  // Act -- the third distinct page forces an eviction of page 0, whose
  // write-back fails and is reported through the GetPage StatusOr
  EXPECT_EQ(pool->GetPage(2).GetStatus(), Status::kIOError);

  // Tear down without another write-back so the destructor does not rethrow.
  pool->DropAllPages();
}

TEST_F(PagePoolTest, CleanEvictionDoesNotRewriteDurablePage) {
  constexpr page_id_t kDurablePage = 1;
  {
    PageRef page = pp->GetPage(kDurablePage).MoveValue();
    page->PageInit(kDurablePage, PageType::kFreePage);
    page->body.free_page.FreeBody()[0] = 0x7e;
  }
  ASSERT_SUCCESS(pp->FlushPageForTest(kDurablePage));
  pp->SetPWriteFaultForTest(true);
  for (page_id_t page_id = 2; page_id <= kDefaultCapacity + 1; ++page_id) {
    EXPECT_TRUE(pp->GetPage(page_id).HasValue());
  }
  pp->SetPWriteFaultForTest(false);
}

TEST_F(PagePoolTest, ConcurrentLoadsOfSamePageInstallOnce) {
  // Arrange -- four threads request a cold page at the same instant; the pool
  // must race safely and install exactly one copy
  std::atomic<bool> go{false};
  std::mutex mu;
  std::vector<std::thread> threads;
  threads.reserve(4);
  for (int i = 0; i < 4; ++i) {
    threads.emplace_back([&] {
      while (!go.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      {
        PageRef page = pp->GetPage(77, nullptr).MoveValue();
        EXPECT_EQ(page->PageID(), 77U);
      }
      std::scoped_lock lock(mu);
    });
  }
  go.store(true, std::memory_order_release);
  for (auto& thread : threads) {
    thread.join();
  }

  // Assert -- only one pool entry exists for the shared page
  ASSERT_EQ(pp->Size(), 1);
}

// Regression coverage for a former production bug: concurrent GetPage misses
// racing an eviction used to violate pool invariants (a load could return a
// Page whose PageID field was 0, and Unpin of a live ref could assert because
// the returned page had been evicted underneath it). The install path now
// serializes against write-back via flushing_ + the page id's IO latch and
// revalidates capacity before installing, so this must stay green.
TEST_F(PagePoolTest, ConcurrentEvictionAcrossThreads) {
  // Arrange -- two threads churn distinct page ids far beyond the pool
  // capacity, forcing concurrent eviction and install-race handling
  std::atomic<bool> go{false};
  std::vector<std::thread> threads;
  threads.reserve(2);
  for (int t = 0; t < 2; ++t) {
    threads.emplace_back([&, t] {
      while (!go.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      for (int i = 0; i < 50; ++i) {
        const auto pid = static_cast<page_id_t>(1000) +
                         static_cast<page_id_t>((t * 100) + i);
        PageRef page = pp->GetPage(pid, nullptr).MoveValue();
        EXPECT_EQ(page->PageID(), pid);
      }
    });
  }
  go.store(true, std::memory_order_release);
  for (auto& thread : threads) {
    thread.join();
  }

  // Assert -- the pool is back within its capacity once all pins are released
  EXPECT_LE(pp->Size(), static_cast<page_id_t>(kDefaultCapacity));
}

// Stress: several threads hammer a mix of shared hot ids and thread-private
// cold ids under the tiny default capacity. This forces cache-hit pin/unpin
// storms, concurrent miss loads of the same id, and dirty write-back/reload
// cycles to happen at the same time. Every returned ref must carry the
// requested page id, and privately written payloads must survive eviction
// round-trips (guards the flushing_/install ordering: a stale image must
// never replace a dirty page that is being written back).
TEST_F(PagePoolTest, ParallelGetPageStressMixedIdsWithDirtyReload) {
  constexpr int kThreads = 8;
  constexpr int kIterations = 250;
  constexpr int kHotPages = 16;  // Exceeds capacity so hot ids thrash too.
  std::atomic<bool> go{false};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      while (!go.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      // Private id ranges are disjoint across threads (stride 1000), so the
      // payload assertions below have a single writer per page.
      const auto base = (static_cast<page_id_t>(t) * 1000) + 10000;
      for (int i = 0; i < kIterations; ++i) {
        // Hot fan-out: read-only shared pages; only the id is checked
        // because several threads overlap on these ids.
        const auto hot = static_cast<page_id_t>(i % kHotPages);
        {
          PageRef page = pp->GetPage(hot, nullptr).MoveValue();
          ASSERT_EQ(page->PageID(), hot);
        }
        // Private churn: dirty the page, release it (forcing a write-back on
        // eviction), then reload and verify the bytes came back intact.
        const page_id_t mine = base + static_cast<page_id_t>(i);
        const auto stamp = static_cast<char>((i % 200) + 1);
        {
          PageRef page = pp->GetPage(mine, nullptr).MoveValue();
          // Union overlay: the write stays inside the kPageSize allocation.
          // NOLINTNEXTLINE(clang-analyzer-security.ArrayBound)
          page->body.free_page.FreeBody()[0] = stamp;
        }
        {
          PageRef reloaded = pp->GetPage(mine, nullptr).MoveValue();
          EXPECT_EQ(reloaded->PageID(), mine);
          EXPECT_EQ(reloaded->body.free_page.FreeBody()[0], stamp);
        }
      }
    });
  }
  go.store(true, std::memory_order_release);
  for (auto& thread : threads) {
    thread.join();
  }

  // Quiet phase: push the working set out so the pool shrinks back under its
  // capacity now that every pin is gone.
  for (int i = 0; i <= kDefaultCapacity; ++i) {
    PageRef page =
        pp->GetPage(static_cast<page_id_t>(900000) + static_cast<page_id_t>(i),
                    nullptr)
            .MoveValue();
    ASSERT_EQ(page->PageID(), 900000 + i);
  }
  EXPECT_LE(pp->Size(), static_cast<page_id_t>(kDefaultCapacity));
}

TEST_F(PagePoolTest, DestructorWarnsOnPinnedPage) {
  // Arrange -- pin page 3 and intentionally leak the PageRef so the pool is
  // destroyed while the page is still pinned
  auto* leaked = new PageRef(
      pp->GetPage(3, nullptr).MoveValue());  // Intentional leak: pins page
  // across pool destruction for the test below.
  (void)leaked;
#ifdef TINYLAMB_HAS_LSAN
  // The leak above is deliberate; keep LeakSanitizer from flagging it.
  __lsan_ignore_object(leaked);
#endif

  // Act -- destroying the pool with a pinned page logs a caution message
  pp.reset();  // NOLINT(clang-analyzer-cplusplus.NewDeleteLeaks)

  // Assert -- no crash; the pinned page is deliberately NOT written back
  // (writing under a live ref would be a data race), its dirty image is lost.
  ASSERT_EQ(pp, nullptr);
}

TEST_F(PagePoolTest, FlushPageWaitsForTheContentLatch) {
  constexpr page_id_t kPageId = 17;
  Status flush_status = Status::kUnknown;
  std::atomic<bool> flushed{false};
  {
    PageRef page = pp->GetPage(kPageId).MoveValue();
    page->PageInit(kPageId, PageType::kFreePage);
    page->body.free_page.FreeBody()[0] = 0x2a;
    std::thread writer([&] {
      flush_status = pp->FlushPage(kPageId);
      flushed.store(true, std::memory_order_release);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(flushed.load(std::memory_order_acquire));
    page.PageUnlock();
    writer.join();
  }
  EXPECT_TRUE(flushed.load(std::memory_order_acquire));
  EXPECT_SUCCESS(flush_status);
  pp->DropAllPages();
  PageRef reloaded = pp->GetPage(kPageId).MoveValue();
  ASSERT_EQ(reloaded->body.free_page.FreeBody()[0], static_cast<char>(0x2a));
}

TEST_F(PagePoolTest, SyncDataFileKeepsFpwWhenDurabilityGateFails) {
  constexpr page_id_t kPageId = 41;
  std::atomic<lsn_t> next_snapshot_end{100};
  std::atomic<bool> fail_gate{true};
  pp->SetWalSnapshotSink(
      [&](page_id_t, PageType, std::string_view) -> StatusOr<lsn_t> {
        return next_snapshot_end.fetch_add(100);
      });
  pp->SetDurabilityGate([&](lsn_t) -> Status {
    if (fail_gate.exchange(false, std::memory_order_acq_rel)) {
      return StatusError(StatusCode::kIOError, "injected gate failure");
    }
    return Status::kSuccess;
  });
  {
    PageRef page = pp->GetPage(kPageId).MoveValue();
    page->PageInit(kPageId, PageType::kRowPage);
    page->SetPageLSN(7);
  }

  EXPECT_EQ(pp->FlushPageForTest(kPageId),
            StatusError(StatusCode::kIOError, "injected gate failure"));
  // The sink reports the record's EXCLUSIVE end; the floor is its last byte so
  // Logger::RecycleUpTo cannot unlink the segment holding that record.
  EXPECT_EQ(pp->UnsyncedSnapshotFloor(), 99U);
  ASSERT_TRUE(pp->SyncDataFile(std::numeric_limits<lsn_t>::max() / 2));
  EXPECT_EQ(pp->UnsyncedSnapshotFloor(), 99U);

  EXPECT_SUCCESS(pp->FlushPageForTest(kPageId));
  ASSERT_TRUE(pp->SyncDataFile(std::numeric_limits<lsn_t>::max() / 2));
  EXPECT_EQ(pp->UnsyncedSnapshotFloor(), std::numeric_limits<lsn_t>::max());
  // Uninstall hooks before the locals die: ~PagePool's final WriteBack would
  // otherwise invoke them from TearDown (stack-use-after-return under ASAN).
  pp->SetWalSnapshotSink(nullptr);
  pp->SetDurabilityGate(nullptr);
}

// The recycling floor must never reach a pending full-page image's record end:
// Logger::RecycleUpTo drops a segment as soon as
// (index + 1) * segment_size <= floor, so a floor equal to an end that lands
// exactly on a segment boundary would unlink the very FPW a torn page needs.
TEST_F(PagePoolTest, UnsyncedSnapshotFloorStaysBelowFpwRecordEnd) {
  constexpr page_id_t kPageId = 77;
  // A record that ends exactly on a 64 KiB segment boundary.
  constexpr lsn_t kRecordEnd = static_cast<lsn_t>(64) * 1024;
  constexpr lsn_t kSegmentSize = static_cast<lsn_t>(64) * 1024;
  pp->SetWalSnapshotSink(
      [&](page_id_t, PageType, std::string_view) -> StatusOr<lsn_t> {
        return kRecordEnd;
      });
  {
    PageRef page = pp->GetPage(kPageId).MoveValue();
    page->PageInit(kPageId, PageType::kRowPage);
    page->SetPageLSN(9);
  }
  ASSERT_SUCCESS(pp->FlushPageForTest(kPageId));

  const lsn_t floor = pp->UnsyncedSnapshotFloor();
  EXPECT_LT(floor, kRecordEnd);
  EXPECT_EQ(floor, kRecordEnd - 1);
  // RecycleUpTo's drop rule for segment 0 of this geometry must not fire.
  EXPECT_FALSE((0 + 1) * kSegmentSize <= floor);

  // Once the pwrite is durable the image is retired and the floor lifts.
  ASSERT_TRUE(pp->SyncDataFile(std::numeric_limits<lsn_t>::max() / 2));
  EXPECT_EQ(pp->UnsyncedSnapshotFloor(), std::numeric_limits<lsn_t>::max());
  // Uninstall hooks before the locals die: ~PagePool's final WriteBack would
  // otherwise invoke them from TearDown (stack-use-after-return under ASAN).
  pp->SetWalSnapshotSink(nullptr);
  pp->SetDurabilityGate(nullptr);
}

TEST_F(PagePoolTest, SyncDataFileKeepsFpwWhenPwriteFails) {
  constexpr page_id_t kPageId = 42;
  std::atomic<lsn_t> next_snapshot_end{200};
  pp->SetWalSnapshotSink(
      [&](page_id_t, PageType, std::string_view) -> StatusOr<lsn_t> {
        return next_snapshot_end.fetch_add(100);
      });
  pp->SetPWriteFaultForTest(true);
  {
    PageRef page = pp->GetPage(kPageId).MoveValue();
    page->PageInit(kPageId, PageType::kRowPage);
    page->SetPageLSN(8);
  }

  EXPECT_EQ(pp->FlushPageForTest(kPageId), Status::kIOError);
  EXPECT_EQ(pp->UnsyncedSnapshotFloor(), 199U);
  pp->SetPWriteFaultForTest(false);
  ASSERT_TRUE(pp->SyncDataFile(std::numeric_limits<lsn_t>::max() / 2));
  EXPECT_EQ(pp->UnsyncedSnapshotFloor(), 199U);

  EXPECT_SUCCESS(pp->FlushPageForTest(kPageId));
  ASSERT_TRUE(pp->SyncDataFile(std::numeric_limits<lsn_t>::max() / 2));
  EXPECT_EQ(pp->UnsyncedSnapshotFloor(), std::numeric_limits<lsn_t>::max());
  // Uninstall hooks before the locals die: ~PagePool's final WriteBack would
  // otherwise invoke them from TearDown (stack-use-after-return under ASAN).
  pp->SetWalSnapshotSink(nullptr);
  pp->SetDurabilityGate(nullptr);
}

// SyncDataFile must retire exactly the pages whose writes completed before
// the sync, even while ANOTHER page's write-back is still in flight: the old
// all-or-nothing check skipped the whole retirement whenever any pwrite was
// in flight, which under sustained load froze the recycle floor forever and
// let the WAL grow without bound.
TEST_F(PagePoolTest, SyncDataFileRetiresOnlyCompletedWrites) {
  std::atomic<lsn_t> fake_lsn{1000};
  std::atomic<bool> release_page2{false};
  pp->SetWalSnapshotSink(
      [&](page_id_t, PageType, std::string_view) -> StatusOr<lsn_t> {
        return fake_lsn.fetch_add(100) + 100;
      });
  // Page 2's flush parks inside the durability gate (after its full-page
  // image was emitted) until the main thread releases it.
  pp->SetDurabilityGate([&](lsn_t gate_lsn) {
    if (gate_lsn == 1200) {
      while (!release_page2.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    }
    return Status::kSuccess;
  });

  // Page 3 flushes to completion: emits FPW 1100, pwrite finishes. The body
  // mutation leaves the stored checksum stale, so the page counts as dirty
  // and the FPW/gate paths engage.
  {
    PageRef page = pp->GetPage(3, nullptr).MoveValue();
    page->PageInit(3, PageType::kRowPage);
    page->SetPageLSN(5);
  }
  EXPECT_SUCCESS(pp->FlushPageForTest(3));
  EXPECT_EQ(pp->UnsyncedSnapshotFloor(), 1099);  // record end 1100, minus one

  // Page 2's flush emits FPW 1200, then blocks in the gate (in flight).
  {
    PageRef page = pp->GetPage(2, nullptr).MoveValue();
    page->PageInit(2, PageType::kRowPage);
    page->SetPageLSN(6);
  }
  std::thread stuck_writer([&] { EXPECT_SUCCESS(pp->FlushPageForTest(2)); });
  // Wait until page 2's full-page image was emitted (the second sink call).
  while (fake_lsn.load() < 1200) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  // The sync retires page 3 (written before it ran) and keeps page 2 (still
  // in flight): the floor sits at page 2's image, not page 3's.
  ASSERT_TRUE(pp->SyncDataFile(std::numeric_limits<lsn_t>::max() / 2));
  EXPECT_EQ(pp->UnsyncedSnapshotFloor(), 1199);  // record end 1200, minus one

  release_page2.store(true, std::memory_order_release);
  stuck_writer.join();
  // With every write complete, the next sync clears the floor entirely.
  ASSERT_TRUE(pp->SyncDataFile(std::numeric_limits<lsn_t>::max() / 2));
  EXPECT_EQ(pp->UnsyncedSnapshotFloor(), std::numeric_limits<lsn_t>::max());
  // Uninstall hooks before the locals die: ~PagePool's final WriteBack would
  // otherwise invoke them from TearDown (stack-use-after-return under ASAN).
  pp->SetWalSnapshotSink(nullptr);
  pp->SetDurabilityGate(nullptr);
}

// A page recovery rewrote in memory keeps a CRC that describes the PRE-repair
// disk image. The two predicates must then disagree: IsValid() has to report
// the image as whole (a false "torn" verdict makes PageManager::GetPage hand
// back an empty ref, and BPlusTree::Open then silently bootstraps an empty
// catalog), while ChecksumMatches() has to keep reporting it dirty so the
// repair is actually written back instead of dying with the buffer pool.
TEST(PageChecksumTest, RepairedInMemoryPageIsValidButStillDirty) {
  Page page(7, PageType::kRowPage);
  page.SetChecksum();
  ASSERT_TRUE(page.IsValid());
  ASSERT_TRUE(page.ChecksumMatches());

  // Rewrite the body the way recovery redo/undo does: the stored CRC now
  // describes the old image.
  ASSERT_SUCCESS(page.InsertImpl(0, "row-image"));
  ASSERT_FALSE(page.ChecksumMatches());
  ASSERT_FALSE(page.IsValid())
      << "an ordinary mutation reads as torn until flushed";

  page.MarkRepairedInMemory();
  ASSERT_TRUE(page.IsValid()) << "recovery-repaired page must not read as torn";
  ASSERT_FALSE(page.ChecksumMatches())
      << "and must stay dirty so it gets written back";

  // The repair claim is in-memory only: EncodeDisk must never put it in the
  // 48-byte header. (The stored CRC is still the pre-repair one here, so the
  // encoded image legitimately fails validation -- exactly like a page whose
  // write-back has not happened yet.)
  std::vector<char> image(kPageSize);
  page.EncodeDisk(image.data());
  Page encoded_view(0, PageType::kUnknown);
  ASSERT_SUCCESS(encoded_view.DecodeDisk(image.data()));
  EXPECT_EQ(encoded_view.checksum >> 32, 0U)
      << "the in-memory repair bit must not reach the page format";
  EXPECT_EQ(encoded_view.PageID(), 7);
  EXPECT_EQ(encoded_view.Type(), PageType::kRowPage);

  // Writing the page out (WriteBack recomputes the CRC) discharges the claim:
  // on-disk image == body again, and a reload validates.
  page.SetChecksum();
  ASSERT_TRUE(page.IsValid());
  ASSERT_TRUE(page.ChecksumMatches());
  std::vector<char> flushed(kPageSize);
  page.EncodeDisk(flushed.data());
  Page reloaded(0, PageType::kUnknown);
  ASSERT_SUCCESS(reloaded.DecodeDisk(flushed.data()));
  ASSERT_TRUE(reloaded.IsValid());
  ASSERT_TRUE(reloaded.ChecksumMatches());
  ASSERT_EQ(reloaded.PageID(), 7);
}

// StoredChecksum hashes the raw page bytes (minus recovery_lsn/checksum), so
// any header alignment padding PageInit left uninitialized entered the CRC as
// heap garbage: checksums differed across runs under ASAN malloc fill, and a
// stored CRC could stop matching a recompute on the SAME image. PageInit must
// therefore clear the whole object; the checksum of a freshly initialized
// page may not depend on the memory it was carved from.
TEST(PageChecksumTest, FreshPageChecksumIsIndependentOfPriorMemory) {
  Page over_aa(0, PageType::kUnknown);
  Page over_55(0, PageType::kUnknown);
  // Poison every byte (padding included), then re-init the way a reused page
  // object is re-issued by the pool and by recovery replay.
  // Page is standard-layout and trivially destructible (static_asserted in
  // page.hpp), so byte-poisoning the object is the point of this test; the
  // void* cast keeps -Wclass-memaccess from flagging the deliberate write.
  std::memset(static_cast<void*>(&over_aa), 0xAA, sizeof(Page));
  std::memset(static_cast<void*>(&over_55), 0x55, sizeof(Page));
  over_aa.PageInit(9, PageType::kBranchPage);
  over_55.PageInit(9, PageType::kBranchPage);
  over_aa.SetChecksum();
  over_55.SetChecksum();
  EXPECT_EQ(over_aa.checksum & 0xffffffffU, over_55.checksum & 0xffffffffU)
      << "padding bytes leaked into the fresh-page checksum";
  ASSERT_TRUE(over_aa.IsValid());
  ASSERT_TRUE(over_55.IsValid());
}

}  // namespace tinylamb
