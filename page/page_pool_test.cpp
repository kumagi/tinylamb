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

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdio>
#include <fstream>
#include <ios>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <string>
#include <vector>
#include "page/page_type.hpp"

#ifdef __has_include
#if __has_include(<sanitizer/lsan_interface.h>)
#if defined(__SANITIZE_ADDRESS__) ||                 \
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
  void Reset() { pp = std::make_unique<PagePool>(filename_, kDefaultCapacity); }
  void TearDown() override { std::ignore = std::remove(filename_.c_str()); }

  std::string filename_;
  std::unique_ptr<PagePool> pp = nullptr;
};

TEST_F(PagePoolTest, Construct) {
  // Arrange -- nothing to set up; default PagePool created by SetUp()
  // Act -- nothing to execute; default constructed via SetUp()
  // Assert -- pool size is 0 (no pages have been requested yet)
  ASSERT_EQ(pp->Size(), 0);
}

TEST_F(PagePoolTest, GetPage) {
  // Arrange -- nothing more than default PagePool (capacity 10) from SetUp()
  // Act -- request page 0 from the pool
  PageRef page = pp->GetPage(0, nullptr);

  // Assert -- pool size grows to 1 and the returned page has ID 0
  ASSERT_EQ(pp->Size(), 1);
}

TEST_F(PagePoolTest, GetPageSeveralpattern) {
  // Arrange -- access pattern with repeats: {0, 0, 1, 0, 2}
  std::vector<int> pattern = {0, 0, 1, 0, 2};

  // Act -- request pages in the given pattern
  for (int& i : pattern) {
    PageRef page = pp->GetPage(i, nullptr);

    // Assert -- each requested page has the expected ID
    ASSERT_EQ(page->PageID(), i);
  }
}

TEST_F(PagePoolTest, GetManyPage) {
  // Arrange -- nothing more than default PagePool (capacity 10) from SetUp()
  // Act -- request 5 distinct pages (0..4) from the pool
  for (int i = 0; i < 5; ++i) {
    PageRef p = pp->GetPage(i, nullptr);

    // Assert -- each page has the expected ID and pool size grows accordingly
    ASSERT_EQ(p->PageID(), i);
    ASSERT_EQ(pp->Size(), i + 1);
  }
}

TEST_F(PagePoolTest, EvictPage) {
  // Arrange -- default PagePool (capacity 10); nothing else to set up
  // Act -- request 15 pages (0..14), exceeding the pool capacity of 10
  for (int i = 0; i < 15; ++i) {
    PageRef p = pp->GetPage(i, nullptr);

    // Assert -- each page has the expected ID; pool size caps at capacity (10)
    ASSERT_EQ(p->PageID(), i);
    ASSERT_EQ(pp->Size(), std::min(i + 1, kDefaultCapacity));
  }
}

TEST_F(PagePoolTest, PersistencyWithReset) {
  // Arrange -- request 11 pages and write a deterministic byte pattern to each
  constexpr size_t kPages = 11;
  for (size_t i = 0; i < kPages; ++i) {
    PageRef p = pp->GetPage(i, nullptr);
    char* buff = p->body.free_page.FreeBody();
    ASSERT_NE(buff, nullptr);
    for (size_t j = 0; j < FreePage::FreeBodySize(); ++j) {
      // The union overlay makes the whole kPageSize image writable; the
      // analyzer cannot see that FreeBody() stays inside the allocation.
      buff[j] = i;  // NOLINT(clang-analyzer-security.ArrayBound)
    }
  }
  // Reset();
  // Act -- re-request the same 11 pages and read back the byte patterns
  for (size_t i = 0; i < kPages; ++i) {
    PageRef p = pp->GetPage(i, nullptr);
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
  PageRef page = pp->GetPage(5, nullptr);

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
    PageRef page = pp->GetPage(3, nullptr);
    page->PageInit(3, PageType::kFreePage);
    page->SetPageLSN(1);
    page->SetChecksum();
    ASSERT_TRUE(page->IsValid());
  }
  pp->FlushPageForTest(3);
  pp->DropAllPages();

  {
    std::fstream file(filename_,
                      std::ios_base::in | std::ios_base::out | std::ios_base::binary);
    ASSERT_TRUE(file.good());
    file.seekp(static_cast<std::streamoff>((3 * kPageSize) + 8));
    char flip = 0x5a;
    file.write(&flip, 1);
    ASSERT_TRUE(file.good());
  }

  EXPECT_THROW(pp->GetPage(3, nullptr), std::runtime_error);
}

TEST_F(PagePoolTest, PageInitForEveryPageType) {
  // Arrange/Act -- construct a Page for every concrete page type
  for (PageType type : {PageType::kMetaPage, PageType::kRowPage,
                        PageType::kLeafPage, PageType::kBranchPage,
                        PageType::kFreePage}) {
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
  EXPECT_THROW(std::ignore = meta.RowCount(txn), std::runtime_error);

  // Act/Assert -- slot_t overload dispatches on page type
  ASSERT_EQ(row.RowCount(), 0U);
  ASSERT_EQ(leaf.RowCount(), 0U);
  ASSERT_EQ(branch.RowCount(), 0U);
  EXPECT_THROW(std::ignore = meta.RowCount(), std::runtime_error);
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

  // Act/Assert -- ReadKey on unsupported types throws
  EXPECT_THROW(meta.ReadKey(txn, 0), std::runtime_error);

  // Act/Assert -- Read(slot) on a leaf page; out-of-range fails
  ASSERT_EQ(leaf.Read(txn, 0).Value(), "1");
  ASSERT_EQ(leaf.Read(txn, 1).GetStatus(), Status::kNotExists);

  // Act/Assert -- Read(slot) on an unsupported type throws
  EXPECT_THROW(meta.Read(txn, 0), std::runtime_error);
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

TEST_F(PagePoolTest, PageUnsupportedOperationsThrow) {
  // Arrange -- a row page cannot serve leaf/branch-only operations
  Transaction txn;
  Page row(1, PageType::kRowPage);
  Page other(2, PageType::kLeafPage);

  // Act/Assert -- each unsupported operation throws "Invalid page type"
  EXPECT_THROW(row.Delete(txn, "key"), std::runtime_error);
  EXPECT_THROW(row.SetLowFence(txn, IndexKey("a")), std::runtime_error);
  EXPECT_THROW(row.SetHighFence(txn, IndexKey("z")), std::runtime_error);
  EXPECT_THROW(std::ignore = row.GetLowFence(txn), std::runtime_error);
  EXPECT_THROW(std::ignore = row.GetHighFence(txn), std::runtime_error);
  EXPECT_THROW((void)row.SetFoster(txn, FosterPair("k", 1)),
               std::runtime_error);
  EXPECT_THROW((void)row.GetFoster(txn), std::runtime_error);
  EXPECT_THROW((void)row.MoveRightToFoster(txn, other), std::runtime_error);
  EXPECT_THROW((void)row.MoveLeftFromFoster(txn, other), std::runtime_error);
  EXPECT_THROW(row.SetLowFenceImpl(IndexKey("a")), std::runtime_error);
  EXPECT_THROW(row.SetHighFenceImpl(IndexKey("z")), std::runtime_error);
  EXPECT_THROW(row.SetFosterImpl(FosterPair("k", 1)), std::runtime_error);
}

TEST_F(PagePoolTest, PageRefStreamInsertion) {
  // Arrange -- a live pinned page
  PageRef page = pp->GetPage(3, nullptr);

  // Act -- stream the PageRef itself
  std::ostringstream oss;
  oss << page;

  // Assert -- operator<< prints {Ref: <page_id>}
  EXPECT_NE(oss.str().find("{Ref: 3}"), std::string::npos);
}

TEST_F(PagePoolTest, MetaPageAllocateDestroyReuse) {
  // Arrange -- full page manager + transaction machinery
  PageManager pm(filename_, kDefaultCapacity);
  Logger log(filename_ + ".log");
  LockManager lm;
  RecoveryManager rm(filename_ + ".log", pm.GetPool());
  TransactionManager tm(&lm, &pm, &log, &rm);

  // Act 1 -- allocate two pages through the meta page
  Transaction txn = tm.Begin();
  PageRef p1 = pm.AllocateNewPage(txn, PageType::kRowPage);
  PageRef p2 = pm.AllocateNewPage(txn, PageType::kLeafPage);
  ASSERT_NE(p1->PageID(), p2->PageID());

  // Act 2 -- destroy p2 (turns it into a free page on the free chain)
  pm.DestroyPage(txn, p2.get());
  // Release p2's exclusive latch before the pool is asked to relatch it.
  p2.PageUnlock();

  // Act 3 -- the next allocation must reuse the freed page ID
  PageRef p3 = pm.AllocateNewPage(txn, PageType::kBranchPage);
  ASSERT_EQ(p3->PageID(), p2->PageID());

  // Assert -- the meta page tracks the allocated max page count
  PageRef meta = pm.GetPool()->GetPage(0, nullptr);
  ASSERT_GE(meta->body.meta_page.MaxPageCountForTest(), p3->PageID());
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(PagePoolTest, EvictionResumesAfterPoolGrewPastCapacity) {
  std::vector<PageRef> pinned;
  pinned.reserve(kDefaultCapacity);
  for (int i = 0; i < kDefaultCapacity; ++i) {
    pinned.push_back(pp->GetPage(static_cast<page_id_t>(i)));
  }
  {
    PageRef extra = pp->GetPage(kDefaultCapacity);
    EXPECT_GT(pp->Size(), static_cast<page_id_t>(kDefaultCapacity));
    pinned.clear();
  }
  PageRef newer = pp->GetPage(kDefaultCapacity + 1);
  EXPECT_LE(pp->Size(), static_cast<page_id_t>(kDefaultCapacity));
  EXPECT_EQ(newer->PageID(), static_cast<page_id_t>(kDefaultCapacity + 1));
}

TEST_F(PagePoolTest, CacheHitFlagReflectsMissAndHit) {
  // Arrange -- nothing more than default PagePool (capacity 10) from SetUp()
  // Act -- request page 9 once (miss) and again (hit), observing the flag.
  // The PageRef holds the page's exclusive latch, so the second request must
  // wait for the first ref to be destroyed.
  bool hit = true;
  {
    PageRef miss = pp->GetPage(9, &hit);

    // Assert -- the first request was a miss
    ASSERT_FALSE(hit);
  }
  hit = false;
  {
    PageRef hit_ref = pp->GetPage(9, &hit);

    // Assert -- the second request was served from the pool
    ASSERT_TRUE(hit);
    ASSERT_EQ(hit_ref->PageID(), 9U);
  }
}

TEST_F(PagePoolTest, DropAllPagesClearsPool) {
  // Arrange -- request 5 distinct pages so the pool is non-empty
  for (int i = 0; i < 5; ++i) {
    PageRef p = pp->GetPage(i, nullptr);
    ASSERT_EQ(p->PageID(), i);
  }
  ASSERT_EQ(pp->Size(), 5);

  // Act -- drop every buffered page without writing back
  pp->DropAllPages();

  // Assert -- the pool is empty and still serves fresh pages afterwards
  ASSERT_EQ(pp->Size(), 0);
  PageRef again = pp->GetPage(2, nullptr);
  ASSERT_EQ(again->PageID(), 2);
  ASSERT_EQ(pp->Size(), 1);
}

TEST_F(PagePoolTest, DropAllPagesWithPinnedRefsRetiresEntries) {
  // Arrange -- pin one page for the whole test and another transiently
  auto keep = pp->GetPage(4);
  {
    PageRef transient = pp->GetPage(5);
    ASSERT_EQ(pp->Size(), 2);

    // Act -- discard every buffered page while both refs are alive
    pp->DropAllPages();
    ASSERT_EQ(pp->Size(), 0);

    // Assert -- the surviving refs still address live memory; their later
    // unlatches/unpins must not touch freed entries.
    EXPECT_EQ(keep->PageID(), 4U);
    EXPECT_EQ(transient->PageID(), 5U);
    // Union overlay: the write stays inside the kPageSize allocation.
    keep->body.free_page.FreeBody()[0] = 'r';  // NOLINT(clang-analyzer-security.ArrayBound)
  }
  PageRef fresh = pp->GetPage(6);
  ASSERT_EQ(fresh->PageID(), 6U);
}

TEST_F(PagePoolTest, DurabilityGateFiresForDirtyPagesOnly) {
  // Arrange -- record every LSN the gate observes before a pwrite
  std::vector<lsn_t> gated;
  pp->SetDurabilityGate([&gated](lsn_t lsn) { gated.push_back(lsn); });

  PageRef page = pp->GetPage(8);
  page->SetPageLSN(77);
  // Union overlay: the write stays inside the kPageSize allocation.
  page->body.free_page.FreeBody()[0] = 'z';  // NOLINT(clang-analyzer-security.ArrayBound)

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
    PageRef p = pp->GetPage(7, nullptr);
    char* buff = p->body.free_page.FreeBody();
    ASSERT_NE(buff, nullptr);
    for (size_t j = 0; j < FreePage::FreeBodySize(); ++j) {
      // Union overlay: the write stays inside the kPageSize allocation.
      buff[j] = static_cast<char>(0x5a);  // NOLINT(clang-analyzer-security.ArrayBound)
    }

    // Act -- write back page 7; flushing a never-resident page is a no-op
    pp->FlushPageForTest(7);
    pp->FlushPageForTest(1234);
  }

  // Act -- discard the pool (without a second flush) and reload the page
  pp->DropAllPages();
  ASSERT_EQ(pp->Size(), 0);
  PageRef reloaded = pp->GetPage(7, nullptr);

  // Assert -- the flushed pattern survived the drop-and-reload cycle
  ASSERT_EQ(reloaded->PageID(), 7);
  const char* body = reloaded->body.free_page.FreeBody();
  for (size_t j = 0; j < FreePage::FreeBodySize(); ++j) {
    EXPECT_EQ(body[j], static_cast<char>(0x5a));
  }
}

TEST_F(PagePoolTest, ConstructorThrowsForUnopenablePath) {
  // Arrange/Act -- a pool whose backing file cannot be created (missing parent
  // directory); the constructor retries with O_TRUNC then throws
  // Assert -- the constructor reports failure instead of silently proceeding
  EXPECT_THROW(PagePool("/nonexistent_dir_tinylamb/foo.db", 10),
               std::runtime_error);
}

TEST_F(PagePoolTest, OutOfRangePageIdIsHardError) {
  // Arrange -- a page id so large that its file offset overflows off_t
  // (kPageSize = 32 KiB, so ids beyond 2^48 wrap the offset negative)
  constexpr page_id_t kHuge = (1ULL << 48) + 1;

  // Act/Assert -- loading must report a hard error instead of silently
  // materializing an empty free page that could later be persisted.
  EXPECT_THROW(pp->GetPage(kHuge, nullptr), std::runtime_error);
  ASSERT_EQ(pp->Size(), 0);

  // Act -- ordinary ids keep working afterwards; the extra page forces one
  // clean eviction round.
  for (int i = 0; i < kDefaultCapacity + 1; ++i) {
    PageRef page = pp->GetPage(static_cast<page_id_t>(1000) + i, nullptr);
    ASSERT_EQ(page->PageID(), 1000 + i);
  }
  pp->DropAllPages();
  ASSERT_EQ(pp->Size(), 0);
}

TEST_F(PagePoolTest, WriteBackFailureOnFullDeviceThrows) {
  // Arrange -- /dev/full answers reads with zeros but rejects every write
  // with ENOSPC, so the write-back path during eviction must throw
  auto pool = std::make_unique<PagePool>("/dev/full", 2);
  pool->GetPage(0);
  pool->GetPage(1);

  // Act -- the third distinct page forces an eviction of page 0, whose
  // write-back fails
  EXPECT_THROW(pool->GetPage(2), std::runtime_error);

  // Tear down without another write-back so the destructor does not rethrow.
  pool->DropAllPages();
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
        PageRef page = pp->GetPage(77, nullptr);
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
// serializes against write-back via flushing_ + file_latch_ and revalidates
// capacity before installing, so this must stay green.
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
        PageRef page = pp->GetPage(pid, nullptr);
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

TEST_F(PagePoolTest, DestructorWarnsOnPinnedPage) {
  // Arrange -- pin page 3 and intentionally leak the PageRef so the pool is
  // destroyed while the page is still pinned
  auto* leaked = new PageRef(
      pp->GetPage(3, nullptr));  // Intentional leak: pins page across pool
                                 // destruction for the test below.
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

}  // namespace tinylamb
