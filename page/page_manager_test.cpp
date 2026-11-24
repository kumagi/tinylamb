/**
 * Copyright 2026 KUMAZAKI Hiroki
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

#include "page/page_manager.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <future>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <thread>

#include "common/crc32c.hpp"
#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "common/memory_budget.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "page/page.hpp"
#include "page/page_lsm_store.hpp"
#include "page/page_ref.hpp"
#include "page/page_type.hpp"
#include "page/row_page.hpp"
#include "page/row_position.hpp"
#include "recovery/log_record.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/row.hpp"

namespace tinylamb {
namespace {

class PageManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    prefix_ = "page_manager_test-" + RandomString();
    pm_ = PageManager::Create(prefix_ + ".db", 64, false).MoveValue();
    logger_ = Logger::Create(prefix_ + ".log").MoveValue();
    pm_->SetDurableWaiter(
        [l = logger_.get()](lsn_t lsn) { return l->WaitForDurable(lsn); });
    rm_ = std::make_unique<RecoveryManager>(prefix_ + ".log", pm_->GetPool(),
                                            logger_->SegmentSize());
    rm_->SetLogger(logger_.get());
    tm_ = std::make_unique<TransactionManager>(pm_.get(), logger_.get(),
                                               rm_.get());
  }

  void TearDown() override {
    tm_.reset();
    rm_.reset();
    logger_.reset();
    pm_.reset();
    std::ignore = std::remove((prefix_ + ".db").c_str());
    std::ignore = std::remove((prefix_ + ".db.ckpt").c_str());
    std::ignore = std::remove((prefix_ + ".db.ckpt.tmp").c_str());
    std::ignore = std::remove((prefix_ + ".db.blocks").c_str());
    std::error_code ec;
    for (const auto& entry : std::filesystem::directory_iterator(".", ec)) {
      const std::string fname = entry.path().filename().string();
      if (fname.starts_with(prefix_ + ".db.blocks.") ||
          fname.starts_with(prefix_ + ".db.lsm.")) {
        std::filesystem::remove(entry.path(), ec);
      }
    }
    RemoveLogAndSegments(prefix_ + ".log");
  }

  std::string prefix_;
  std::unique_ptr<PageManager> pm_;
  std::unique_ptr<Logger> logger_;
  std::unique_ptr<RecoveryManager> rm_;
  std::unique_ptr<TransactionManager> tm_;
};

// GetPage refreshes byte accounting from live row-page fields. Those fields
// are mutated by writers under the page content latch only, so the refresh
// must take that latch (non-blockingly) rather than blocks_mu_ alone. Under
// TSAN this test would report a race on the live vector/refcount state with
// the old accounting site.
TEST_F(PageManagerTest, GetPageAccountingConcurrentWithPageWrites) {
  auto setup = tm_->Begin();
  ASSIGN_OR_ASSERT_FAIL(PageRef, page,
                        pm_->AllocateNewPage(setup, PageType::kRowPage));
  const page_id_t pid = page->PageID();
  ASSERT_TRUE(page->Insert(setup, "init").HasValue());
  page.PageUnlock();
  ASSERT_SUCCESS(setup.PreCommit());

  std::atomic<bool> done{false};
  std::vector<std::thread> readers;
  readers.reserve(4);
  for (int i = 0; i < 4; ++i) {
    readers.emplace_back([&] {
      while (!done.load(std::memory_order_relaxed)) {
        auto ref = pm_->GetPage(pid, /*shared=*/true);
        if (ref.HasValue()) {
          (void)ref.Value()->Type();
        }
      }
    });
  }
  auto writer = tm_->Begin();
  for (int i = 0; i < 512; ++i) {
    auto ref = pm_->GetPage(pid, /*shared=*/false);
    ASSERT_TRUE(ref.HasValue());
    ASSERT_TRUE(
        ref.Value()->Insert(writer, "v" + std::to_string(i)).HasValue());
  }
  done.store(true, std::memory_order_relaxed);
  for (auto& t : readers) {
    t.join();
  }
  ASSERT_SUCCESS(writer.PreCommit());
}

TEST_F(PageManagerTest, CommittedDestroySkipsObsoleteStubHydration) {
  auto setup = tm_->Begin();
  ASSIGN_OR_ASSERT_FAIL(PageRef, page,
                        pm_->AllocateNewPage(setup, PageType::kRowPage));
  const page_id_t pid = page->PageID();
  ASSERT_TRUE(page->Insert(setup, "before").HasValue());
  page.PageUnlock();
  ASSERT_SUCCESS(setup.PreCommit());
  const lsn_t compact_lsn = logger_->BufferedLSN();
  ASSERT_TRUE(
      pm_->ReplaceTableChainWithColumnarStubs(pid, {pid}, {1}, compact_lsn)
          .HasValue());
  const lsn_t destroy_lsn = compact_lsn + 100;
  pm_->SetRecoveryDestroyFloorLsns({{pid, destroy_lsn}});
  const auto update =
      LogRecord::UpdatingLogRecord(0, 100, pid, 0, "after", "before");
  // DROP's backing file is already gone. No hydration may be attempted.
  ASSERT_SUCCESS(pm_->ReplayCommittedRecord(compact_lsn + 1, update));
  ASSERT_SUCCESS(pm_->ReplayCommittedRecord(
      destroy_lsn, LogRecord::DestroyPageLogRecord(0, 100, pid)));
  EXPECT_EQ(pm_->GetPage(pid).GetStatus(), Status::kNotExists);
  pm_->ClearRecoveryDestroyFloorLsns();
}

TEST_F(PageManagerTest, CommitRetriesTransientOverlayReadBeforeCheckpoint) {
  auto setup = tm_->Begin();
  ASSIGN_OR_ASSERT_FAIL(PageRef, page,
                        pm_->AllocateNewPage(setup, PageType::kRowPage));
  const page_id_t pid = page->PageID();
  ASSERT_TRUE(page->Insert(setup, "before").HasValue());
  page.PageUnlock();
  ASSERT_SUCCESS(setup.PreCommit());
  auto update = tm_->Begin();
  ASSIGN_OR_ASSERT_FAIL(PageRef, changed, pm_->GetPage(pid));
  ASSERT_SUCCESS(changed->Update(update, 0, "after"));
  changed.PageUnlock();
  ASSERT_SUCCESS(pm_->EvictColdPagesForTest(0));
  ASSERT_GT(pm_->UncommittedSpillCount(), 0U);
  auto& budget = ProcessMemoryBudget::Global();
  budget.SetTestSpillIoFailCountdown(1);
  const Status committed = update.PreCommit();
  budget.SetTestSpillIoFailCountdown(0);
  ASSERT_SUCCESS(committed);
  ASSERT_SUCCESS(pm_->WriteFuzzyCheckpoint(logger_->BufferedLSN()));
  pm_->DropAllPages();
  ASSERT_SUCCESS(pm_->LoadFuzzyCheckpoint(nullptr));
  ASSIGN_OR_ASSERT_FAIL(PageRef, restored, pm_->GetPage(pid));
  EXPECT_EQ(restored->body.row_page.GetRow(0), "after");
}

TEST_F(PageManagerTest, CompactionDropsOverlayBeforeReusingTailPage) {
  auto setup = tm_->Begin();
  ASSIGN_OR_ASSERT_FAIL(PageRef, head,
                        pm_->AllocateNewPage(setup, PageType::kRowPage));
  ASSIGN_OR_ASSERT_FAIL(PageRef, tail,
                        pm_->AllocateNewPage(setup, PageType::kRowPage));
  const page_id_t first = head->PageID();
  const page_id_t retired = tail->PageID();
  ASSERT_TRUE(head->Insert(setup, "head").HasValue());
  ASSERT_TRUE(tail->Insert(setup, "old tail").HasValue());
  head.PageUnlock();
  tail.PageUnlock();
  ASSERT_SUCCESS(setup.PreCommit());
  pm_->SetBufferPoolLimitBytesForTest(1);
  auto compact = tm_->Begin();
  ASSERT_TRUE(compact.DestroyPageLog(retired).HasValue());
  ASSERT_SUCCESS(compact.PreCommit());
  // The pager no longer has to spill the destroy overlay under a tiny pool
  // (it may keep it resident); what matters is that none survives the chain
  // replacement and that the reused page reads back its new incarnation.
  ASSERT_TRUE(pm_->ReplaceTableChainWithColumnarStubs(
                     first, {first, retired}, {1}, logger_->BufferedLSN())
                  .HasValue());
  EXPECT_EQ(pm_->UncommittedSpillCount(), 0U);
  auto reuse = tm_->Begin();
  ASSIGN_OR_ASSERT_FAIL(PageRef, fresh,
                        pm_->AllocateNewPage(reuse, PageType::kRowPage));
  ASSERT_EQ(fresh->PageID(), retired);
  ASSERT_TRUE(fresh->Insert(reuse, "new incarnation").HasValue());
  fresh.PageUnlock();
  ASSERT_SUCCESS(reuse.PreCommit());
  ASSERT_SUCCESS(pm_->EvictColdPagesForTest(0));
  ASSIGN_OR_ASSERT_FAIL(PageRef, loaded, pm_->GetPage(retired));
  EXPECT_EQ(loaded->body.row_page.GetRow(0), "new incarnation");
}

TEST_F(PageManagerTest, CheckpointCleanupPreservesOtherCompanionSuffixes) {
  const std::filesystem::path lsm_tmp = prefix_ + ".db.lsm.tmp";
  const std::filesystem::path blocks_tmp = prefix_ + ".db.blocks.tmp";
  const std::filesystem::path lsm_stale = prefix_ + ".db.lsm.42";
  const std::filesystem::path blocks_stale = prefix_ + ".db.blocks.42";
  for (const auto& path : {lsm_tmp, blocks_tmp, lsm_stale, blocks_stale}) {
    std::ofstream file(path);
    ASSERT_TRUE(file.is_open());
    file << "fixture";
  }
  ASSERT_SUCCESS(pm_->WriteFuzzyCheckpoint(0));
  ASSERT_SUCCESS(pm_->LoadFuzzyCheckpoint(nullptr));
  EXPECT_TRUE(std::filesystem::exists(lsm_tmp));
  EXPECT_TRUE(std::filesystem::exists(blocks_tmp));
  EXPECT_FALSE(std::filesystem::exists(lsm_stale));
  EXPECT_FALSE(std::filesystem::exists(blocks_stale));
}

TEST_F(PageManagerTest, CheckpointRejectsUnrepresentableBlockGeneration) {
  ASSERT_SUCCESS(pm_->WriteFuzzyCheckpoint(0));
  const auto path = prefix_ + ".db";
  std::string bytes;
  {
    std::ifstream input(path, std::ios::binary);
    ASSERT_TRUE(input.good());
    bytes.assign(std::istreambuf_iterator<char>(input),
                 std::istreambuf_iterator<char>());
  }
  constexpr std::string_view kMagic = "TLSMCKP1";
  const size_t magic = bytes.find(kMagic);
  ASSERT_NE(magic, std::string::npos);
  const size_t block_generation =
      magic + kMagic.size() + (2 * sizeof(uint32_t)) + (2 * sizeof(uint64_t));
  ASSERT_GE(bytes.size(),
            block_generation + sizeof(uint64_t) + sizeof(uint32_t));
  constexpr uint64_t kUnrepresentable = uint64_t{1} << 32;
  std::memcpy(bytes.data() + block_generation, &kUnrepresentable,
              sizeof(kUnrepresentable));
  const size_t payload_size = bytes.size() - sizeof(uint32_t);
  const uint32_t crc = Crc32C(bytes.data(), payload_size);
  std::memcpy(bytes.data() + payload_size, &crc, sizeof(crc));
  {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    ASSERT_TRUE(output.good());
  }
  EXPECT_EQ(pm_->LoadFuzzyCheckpoint(nullptr), Status::kCorrupt);
}

TEST_F(PageManagerTest, CheckpointMergesResidentAndEvictedPageStreams) {
  pm_->SetBufferPoolLimitBytesForTest(size_t{64} << 10);
  std::vector<page_id_t> ids;
  ids.reserve(600);
  auto setup = tm_->Begin();
  for (int i = 0; i < 600; ++i) {
    ASSIGN_OR_ASSERT_FAIL(PageRef, page,
                          pm_->AllocateNewPage(setup, PageType::kRowPage));
    ASSERT_TRUE(page->Insert(setup, std::string(1500, 'x')).HasValue());
    ids.push_back(page->PageID());
  }
  ASSERT_SUCCESS(setup.PreCommit());
  ASSERT_SUCCESS(pm_->EvictColdPagesForTest(0));
  ASSERT_GE(pm_->EvictedPageCount(), 500U);
  // A high resident key must not jump over the next batch of evicted keys.
  // A low dirty resident key must also participate in file classification.
  {
    ASSIGN_OR_ASSERT_FAIL(PageRef, low, pm_->GetPage(ids.front()));
    ASSIGN_OR_ASSERT_FAIL(PageRef, high, pm_->GetPage(ids.back()));
    auto update = tm_->Begin();
    ASSERT_SUCCESS(low->Update(update, 0, "changed"));
    low.PageUnlock();
    high.PageUnlock();
    ASSERT_SUCCESS(update.PreCommit());
  }
  ASSIGN_OR_ASSERT_FAIL(PageRef, high, pm_->GetPage(ids.back()));
  ASSERT_SUCCESS(pm_->WriteFuzzyCheckpoint(logger_->BufferedLSN()));
  high.PageUnlock();
  pm_->DropAllPages();
  ASSERT_SUCCESS(pm_->LoadFuzzyCheckpoint(nullptr));
  auto reader = tm_->Begin();
  for (size_t i = 0; i < ids.size(); ++i) {
    ASSIGN_OR_ASSERT_FAIL(PageRef, page, pm_->GetPage(ids[i]));
    ASSIGN_OR_ASSERT_FAIL(std::string_view, value, page->Read(reader, 0));
    EXPECT_EQ(value, i == 0 ? "changed" : std::string(1500, 'x')) << i;
  }
}

TEST_F(PageManagerTest, AuditLsmScanKeepsOwnWriteUnderForeignOverwrite) {
  auto first = tm_->Begin();
  auto second = tm_->Begin();
  auto observer = tm_->Begin();
  constexpr page_id_t tree = 99999;
  ASSERT_SUCCESS(pm_->LsmInsert(first, tree, "key", "first"));
  ASSERT_SUCCESS(pm_->LsmUpdate(second, tree, "key", "second"));
  const auto scan = [&](const Transaction& txn) {
    return pm_->LsmScanRange(tree, "key", "key", true, &txn);
  };
  const auto own = scan(first);
  ASSERT_EQ(own.size(), 1U);
  EXPECT_EQ(own[0].second, "first");
  const auto foreign = scan(second);
  ASSERT_EQ(foreign.size(), 1U);
  EXPECT_EQ(foreign[0].second, "second");
  EXPECT_TRUE(scan(observer).empty());
  ASSERT_SUCCESS(pm_->LsmDelete(second, tree, "key"));
  const auto under_delete = scan(first);
  ASSERT_EQ(under_delete.size(), 1U);
  EXPECT_EQ(under_delete[0].second, "first");
  EXPECT_TRUE(scan(second).empty());
  EXPECT_TRUE(scan(observer).empty());
  ASSERT_SUCCESS(second.Abort());
  ASSERT_SUCCESS(first.Abort());
  ASSERT_SUCCESS(observer.Abort());
}

TEST_F(PageManagerTest, PendingLsmDestroyIsVisibleOnlyToItsOwner) {
  constexpr page_id_t tree = 991387;
  auto seed = tm_->Begin();
  ASSERT_SUCCESS(pm_->LsmInsert(seed, tree, "key", "committed"));
  ASSERT_SUCCESS(seed.PreCommit());

  auto destroyer = tm_->Begin();
  auto observer = tm_->Begin();
  ASSERT_SUCCESS(pm_->LsmDestroy(destroyer, tree));
  EXPECT_EQ(pm_->LsmDestroy(observer, tree), Status::kConflicts);
  EXPECT_EQ(pm_->LsmRead(tree, "key", &destroyer).GetStatus(),
            Status::kNotExists);
  EXPECT_TRUE(pm_->LsmScanRange(tree, "", "", true, &destroyer).empty());

  auto visible = pm_->LsmRead(tree, "key", &observer);
  ASSERT_TRUE(visible.HasValue()) << visible.GetStatus();
  EXPECT_EQ(visible.Value(), "committed");
  EXPECT_TRUE(pm_->LsmRead(tree, "key").HasValue());
  const auto rows = pm_->LsmScanRange(tree, "", "", true, &observer);
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows.front().second, "committed");
  ASSERT_SUCCESS(destroyer.Abort());
  EXPECT_TRUE(pm_->LsmRead(tree, "key", &observer).HasValue());
  ASSERT_SUCCESS(observer.Abort());

  auto committed_destroyer = tm_->Begin();
  ASSERT_SUCCESS(pm_->LsmDestroy(committed_destroyer, tree));
  ASSERT_SUCCESS(committed_destroyer.PreCommit());
  auto new_reader = tm_->Begin();
  EXPECT_EQ(pm_->LsmRead(tree, "key", &new_reader).GetStatus(),
            Status::kNotExists);
  EXPECT_TRUE(pm_->LsmScanRange(tree, "", "", true, &new_reader).empty());
  ASSERT_SUCCESS(new_reader.Abort());
}

// P3834/386: LsmInsert/LsmUpdate/LsmDelete probe under the SHARED tree latch
// (cold LSM block preads must not serialize the whole tree) and revalidate
// under the exclusive latch via the tree mutation epoch.  Pin the externally
// visible contract the revalidation must preserve: duplicate detection
// across commit promotion, key release through abort, pre-image-bearing
// updates, kNotExists on missing keys, and the destroy/conflict prologue.
TEST_F(PageManagerTest, TwoPhaseLsmWritersPreserveConflictAndPreimageContract) {
  constexpr page_id_t tree = 991386;
  auto t1 = tm_->Begin();
  ASSERT_SUCCESS(pm_->LsmInsert(t1, tree, "k", "v1"));
  {
    auto t2 = tm_->Begin();
    EXPECT_EQ(pm_->LsmInsert(t2, tree, "k", "other"), Status::kDuplicates);
    ASSERT_SUCCESS(pm_->LsmUpdate(t2, tree, "k", "v2"));
    ASSERT_SUCCESS(t2.Abort());
  }
  ASSERT_SUCCESS(t1.Abort());  // frees the key again
  auto t3 = tm_->Begin();
  ASSERT_SUCCESS(pm_->LsmInsert(t3, tree, "k", "v3"));
  ASSERT_SUCCESS(t3.PreCommit());  // promotes to committed_mem (epoch bump)
  auto t4 = tm_->Begin();
  EXPECT_EQ(pm_->LsmInsert(t4, tree, "k", "again"), Status::kDuplicates);
  EXPECT_EQ(pm_->LsmDelete(t4, tree, "missing"), Status::kNotExists);
  ASSERT_SUCCESS(pm_->LsmUpdate(t4, tree, "k", "v4"));  // pre-image = "v3"
  ASSERT_SUCCESS(pm_->LsmDelete(t4, tree, "k"));
  ASSERT_SUCCESS(t4.Abort());  // "k" stays resident as committed v3
  auto t5 = tm_->Begin();
  ASSERT_SUCCESS(pm_->LsmDestroy(t5, tree));
  auto t6 = tm_->Begin();
  EXPECT_EQ(pm_->LsmInsert(t6, tree, "k", "foreign"), Status::kConflicts);
  EXPECT_EQ(pm_->LsmInsert(t5, tree, "k", "reopen"), Status::kDuplicates);
  // (the promoted "k" entry is still resident until the destroy commit
  // applies; the forced revalidation after clearing own destroy must see it
  // exactly like the old exclusive-probe order did)
  ASSERT_SUCCESS(t6.Abort());
  ASSERT_SUCCESS(t5.PreCommit());
  auto t7 = tm_->Begin();
  ASSERT_SUCCESS(pm_->LsmInsert(t7, tree, "fresh", "after-destroy"));
  ASSERT_SUCCESS(t7.PreCommit());
}

// P411: checkpoint LSM rotation must serialize with in-flight flushes and
// background compactions.  Streams into the old `<db>.lsm.<gen>` fd used to
// race the generation swap: a run streamed before rotation ended up with
// block offsets that no longer resolved against the live fd (wrong bytes or
// IO errors on later reads).  Hammer flushes against checkpoints and verify
// every committed key survives, in memory and after reload.
TEST_F(PageManagerTest, CheckpointLsmRotationSerializesWithConcurrentFlushes) {
  constexpr page_id_t tree = 424242;
  constexpr int kChunks = 60;
  constexpr int kPerChunk = 50;
  // Shrink the buffer pool so the 1MiB of committed LSM data crosses the
  // write-buffer limit and flush/compaction streams actually run.
  pm_->SetBufferPoolLimitBytesForTest(1U << 20);
  std::vector<std::string> keys;
  keys.reserve(size_t{kChunks} * kPerChunk);
  std::vector<std::string> values;
  values.reserve(size_t{kChunks} * kPerChunk);
  for (int i = 0; i < kChunks * kPerChunk; ++i) {
    // NOLINTNEXTLINE(modernize-avoid-c-arrays) runtime-sized buffer
    char buf[16];
    // NOLINTNEXTLINE(cert-err33-c) error intentionally ignored
    std::snprintf(buf, sizeof(buf), "key-%06d", i);
    keys.emplace_back(buf);
    values.push_back("value-" + std::to_string(i) + std::string(90, 'p'));
  }

  std::atomic<bool> stop_ckpt{false};
  std::atomic<int> ckpt_fail{0};
  std::atomic<int> ckpt_ok{0};
  std::thread ckpter([&] {
    while (!stop_ckpt.load(std::memory_order_relaxed)) {
      if (pm_->WriteFuzzyCheckpoint(logger_->BufferedLSN()) ==
          Status::kSuccess) {
        ckpt_ok.fetch_add(1);
      } else {
        ckpt_fail.fetch_add(1);
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
  });

  for (int chunk = 0; chunk < kChunks; ++chunk) {
    auto txn = tm_->Begin();
    std::vector<LogRecord> recs;
    recs.reserve(kPerChunk);
    for (int j = 0; j < kPerChunk; ++j) {
      const int i = (chunk * kPerChunk) + j;
      ASSERT_SUCCESS(pm_->LsmInsert(txn, tree, keys[i], values[i]));
      LogRecord r(0, txn.ID(), LogType::kLsmPut);
      r.pid = tree;
      r.key = keys[i];
      r.redo_data = values[i];
      recs.push_back(std::move(r));
    }
    ASSERT_SUCCESS(
        pm_->ApplyTransactionCommit(txn.ID(), logger_->BufferedLSN(), recs));
    // Forces tree flush/compaction streams concurrently with the checker
    // thread's rotation window.
    pm_->FlushLsmBuffersForTest(tree);
    ASSERT_SUCCESS(txn.Abort());
  }
  stop_ckpt.store(true);
  ckpter.join();
  EXPECT_EQ(ckpt_fail.load(), 0);
  EXPECT_GT(ckpt_ok.load(), 0);
  pm_->FlushLsmBuffersForTest(tree);
  ASSERT_SUCCESS(pm_->WriteFuzzyCheckpoint(logger_->BufferedLSN()));

  auto reader = tm_->Begin();
  const auto live =
      pm_->LsmScanRange(tree, keys.front(), keys.back(), true, &reader);
  ASSERT_EQ(live.size(), keys.size());
  for (size_t i = 0; i < live.size(); ++i) {
    EXPECT_EQ(live[i].first, keys[i]) << i;
    EXPECT_EQ(live[i].second, values[i]) << i;
  }
  ASSERT_SUCCESS(reader.Abort());

  ASSERT_SUCCESS(pm_->LoadFuzzyCheckpoint(nullptr));
  const auto reloaded =
      pm_->LsmScanRange(tree, keys.front(), keys.back(), true);
  ASSERT_EQ(reloaded.size(), keys.size());
  for (size_t i = 0; i < reloaded.size(); ++i) {
    EXPECT_EQ(reloaded[i].first, keys[i]) << i;
    EXPECT_EQ(reloaded[i].second, values[i]) << i;
  }
}

// P144: exercise the restructured wait/notify loop of ReserveTableTailImpl:
// an uninvolved second transaction must block on a tail held by another and
// proceed once the holder resolves.  (With the txn_id_t overload the watcher
// is null, so the deferred wait-for-edge registration itself is a no-op; the
// deadlock-detect wiring is covered by the query-level transaction suites.)
TEST_F(PageManagerTest, TableTailWaiterProceedsAfterHolderRelease) {
  constexpr txn_id_t kA = 303;
  constexpr txn_id_t kB = 404;
  constexpr page_id_t kT = 21;
  ASSERT_SUCCESS(pm_->ReserveTableTail(kB, kT, kT));
  std::promise<void> entering;
  auto entered = entering.get_future();
  std::future<Status> waiter = std::async(std::launch::async, [&] {
    entering.set_value();
    return pm_->ReserveTableTail(kA, kT, kT);
  });
  EXPECT_EQ(entered.wait_for(std::chrono::seconds(4)),
            std::future_status::ready);
  EXPECT_EQ(waiter.wait_for(std::chrono::milliseconds(10)),
            std::future_status::timeout);
  pm_->ResolveTableTailReservations(kB, TableTailResolution::kDiscard);
  ASSERT_EQ(waiter.wait_for(std::chrono::seconds(4)),
            std::future_status::ready);
  ASSERT_SUCCESS(waiter.get());
  pm_->ResolveTableTailReservations(kA, TableTailResolution::kDiscard);
}

// sql_ooc_fuzzer finding: a transaction that had grown one table could not
// grow a second one ("transaction already holds a table tail reservation"),
// so a single-transaction bulk load into two tables failed outright.  An
// uncontended tail must be granted; only waiting while holding is refused.
TEST_F(PageManagerTest, OneTransactionMayHoldTailsOfSeveralTables) {
  constexpr txn_id_t kA = 101;
  constexpr txn_id_t kB = 202;
  constexpr page_id_t kT1 = 11;
  constexpr page_id_t kT2 = 12;
  constexpr page_id_t kT3 = 13;
  ASSERT_SUCCESS(pm_->ReserveTableTail(kA, kT1, kT1));
  ASSERT_SUCCESS(pm_->ReserveTableTail(kA, kT2, kT2));
  // B holds T3; A (holding T1/T2) must not wait for it: immediate conflict.
  ASSERT_SUCCESS(pm_->ReserveTableTail(kB, kT3, kT3));
  const Status contended = pm_->ReserveTableTail(kA, kT3, kT3);
  EXPECT_EQ(contended, Status::kConflicts);
  EXPECT_FALSE(contended.GetMessage().empty());
  pm_->ResolveTableTailReservations(kB, TableTailResolution::kDiscard);
  ASSERT_SUCCESS(pm_->ReserveTableTail(kA, kT3, kT3));
  pm_->ResolveTableTailReservations(kA, TableTailResolution::kDiscard);
}

TEST_F(PageManagerTest, AuditCheckpointCountsAreBoundedBeforeAllocation) {
  for (const bool rows : {true, false}) {
    std::string bytes("TLSMCKP1", 8);
    const auto append = [&]<typename T>(T value) {
      bytes.append(reinterpret_cast<const char*>(&value), sizeof(value));
    };
    append(uint32_t{2});
    append(uint64_t{0});   // replay start
    append(uint64_t{0});   // transaction high-water
    append(uint64_t{11});  // page high-water
    append(uint32_t{0});   // free pages
    append(uint32_t{rows ? 1U : 0U});
    if (rows) {
      append(uint64_t{11});
      append(uint64_t{0});
      append(uint64_t{0});
      append(uint64_t{0});
      append(std::numeric_limits<uint32_t>::max());
      append(uint32_t{0});
    } else {
      append(uint32_t{1});
      append(uint64_t{12});
      append(std::numeric_limits<uint32_t>::max());
    }
    append(Crc32C(bytes.data(), bytes.size()));
    // Give each shape its own file: writing both to the fixture's prefix_.db
    // truncatingly overwrote the first case and clobbered a file owned by
    // this test's PageManager. Remove it here instead of depending on the
    // fixture TearDown to sweep malformed checkpoints.
    const std::string bad_path =
        prefix_ + ".db.audit." + (rows ? "rows" : "no_rows");
    {
      std::ofstream output(bad_path, std::ios::binary | std::ios::trunc);
      output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
      ASSERT_TRUE(output.good());
    }
    {
      StatusOr<std::unique_ptr<PageManager>> corrupt_pm =
          PageManager::Create(bad_path, 64, false);
      EXPECT_EQ(corrupt_pm.GetStatus(), Status::kSuccess);
      if (corrupt_pm.HasValue()) {
        bool loaded = true;
        Status status = Status::kSuccess;
        EXPECT_NO_THROW(
            status = corrupt_pm.Value()->LoadFuzzyCheckpoint(nullptr, &loaded));
        EXPECT_EQ(status, Status::kCorrupt);
        EXPECT_FALSE(loaded);
      }
    }
    std::ignore = std::remove(bad_path.c_str());
  }
}

TEST_F(PageManagerTest, ReallocationReusesIdWithFreshPageWhileOldPageIsHeld) {
  Transaction setup = tm_->Begin();
  ASSIGN_OR_ASSERT_FAIL(PageRef, page,
                        pm_->AllocateNewPage(setup, PageType::kRowPage));
  const page_id_t pid = page->PageID();
  page.PageUnlock();
  ASSERT_SUCCESS(setup.PreCommit());
  Transaction remove = tm_->Begin();
  ASSIGN_OR_ASSERT_FAIL(PageRef, held, pm_->GetPage(pid));
  ASSERT_SUCCESS(pm_->DestroyPage(remove, held.get()));
  ASSERT_SUCCESS(remove.PreCommit());
  const lsn_t before = held->PageLSN();
  EXPECT_EQ(pm_->GetPage(pid).GetStatus(), Status::kNotExists);

  // The old reference owns the detached shell. Reuse its id in another
  // transaction while that reference still owns the old page's latch.
  auto allocation = std::async(std::launch::async, [&] {
    Transaction create = tm_->Begin();
    auto result = pm_->AllocateNewPage(create, PageType::kRowPage);
    if (!result.HasValue()) {
      return page_id_t{0};
    }
    PageRef fresh = result.MoveValue();
    const page_id_t actual = fresh->PageID();
    fresh.PageUnlock();
    return create.PreCommit() == Status::kSuccess ? actual : page_id_t{0};
  });
  const page_id_t allocated = allocation.get();
  ASSERT_EQ(allocated, pid);
  EXPECT_EQ(held->Type(), PageType::kUnknown);
  EXPECT_EQ(held->PageLSN(), before);
  ASSIGN_OR_ASSERT_FAIL(PageRef, fresh, pm_->GetPage(pid));
  EXPECT_NE(fresh.get(), held.get());
  EXPECT_EQ(fresh->Type(), PageType::kRowPage);
}

TEST_F(PageManagerTest, UnboundedRowBlockInsertUpdateDeleteAndFuzzyCheckpoint) {
  Transaction txn = tm_->Begin();
  ASSIGN_OR_ASSERT_FAIL(PageRef, page,
                        pm_->AllocateNewPage(txn, PageType::kRowPage));
  const page_id_t pid = page->PageID();

  // Insert and update rows within the block, then verify fuzzy checkpointing.
  const std::string initial_row(8192, 'x');
  ASSIGN_OR_ASSERT_FAIL(slot_t, slot, page->Insert(txn, initial_row));
  ASSIGN_OR_ASSERT_FAIL(std::string_view, read_back, page->Read(txn, slot));
  EXPECT_EQ(read_back, initial_row);

  const std::string updated_row(16384, 'y');
  ASSERT_SUCCESS(page->Update(txn, slot, updated_row));
  ASSIGN_OR_ASSERT_FAIL(std::string_view, read_updated, page->Read(txn, slot));
  EXPECT_EQ(read_updated, updated_row);
  page.PageUnlock();
  ASSERT_SUCCESS(txn.PreCommit());

  // Start an uncommitted transaction that modifies the slot, then write a
  // fuzzy checkpoint. The fuzzy checkpoint must capture only committed state!
  Transaction uncommitted = tm_->Begin();
  {
    ASSIGN_OR_ASSERT_FAIL(PageRef, p2, pm_->GetPage(pid));
    ASSERT_SUCCESS(p2->Update(uncommitted, slot, "uncommitted_dirty_value"));
  }
  ASSERT_SUCCESS(pm_->WriteFuzzyCheckpoint(100));
  ASSERT_SUCCESS(uncommitted.Abort());

  // Simulate crash + reload from fuzzy checkpoint.
  pm_->DropAllPages();
  lsn_t replay_start = 0;
  ASSERT_SUCCESS(pm_->LoadFuzzyCheckpoint(&replay_start));
  EXPECT_EQ(replay_start, 100U);

  Transaction check = tm_->Begin();
  ASSIGN_OR_ASSERT_FAIL(PageRef, recovered, pm_->GetPage(pid));
  ASSIGN_OR_ASSERT_FAIL(std::string_view, recovered_row,
                        recovered->Read(check, slot));
  EXPECT_EQ(recovered_row, updated_row);
  recovered.PageUnlock();
  ASSERT_SUCCESS(check.PreCommit());
}

TEST_F(PageManagerTest, ExclusiveFileLockRejectsConcurrentOpen) {
  StatusOr<std::unique_ptr<PageManager>> second =
      PageManager::Create(prefix_ + ".db", 64, false);
  EXPECT_EQ(second.GetStatus(), Status::kConflicts);
}

TEST_F(PageManagerTest,
       WriteFuzzyCheckpointAtomicRenamePreservesExclusiveLockAndData) {
  Transaction txn = tm_->Begin();
  ASSIGN_OR_ASSERT_FAIL(PageRef, page,
                        pm_->AllocateNewPage(txn, PageType::kRowPage));
  const page_id_t pid = page->PageID();
  ASSIGN_OR_ASSERT_FAIL(slot_t, slot, page->Insert(txn, "checkpoint_payload"));
  page.PageUnlock();
  ASSERT_SUCCESS(txn.PreCommit());

  ASSERT_SUCCESS(pm_->WriteFuzzyCheckpoint(42));
  EXPECT_FALSE(std::filesystem::exists(prefix_ + ".db.tmp"));

  // After atomic tmp+rename replacement, the new inode must still hold an
  // exclusive flock so concurrent opens fail with Status::kConflicts.
  StatusOr<std::unique_ptr<PageManager>> second =
      PageManager::Create(prefix_ + ".db", 64, false);
  EXPECT_EQ(second.GetStatus(), Status::kConflicts);

  pm_->DropAllPages();
  lsn_t replay_start = 0;
  ASSERT_SUCCESS(pm_->LoadFuzzyCheckpoint(&replay_start));
  EXPECT_EQ(replay_start, 42U);

  Transaction verify = tm_->Begin();
  ASSIGN_OR_ASSERT_FAIL(PageRef, loaded, pm_->GetPage(pid));
  ASSIGN_OR_ASSERT_FAIL(std::string_view, row, loaded->Read(verify, slot));
  EXPECT_EQ(row, "checkpoint_payload");
  loaded.PageUnlock();
  ASSERT_SUCCESS(verify.PreCommit());
}

TEST_F(PageManagerTest, AuditHoleProbeRetainsStagedDeleteIntent) {
  Transaction setup = tm_->Begin();
  ASSIGN_OR_ASSERT_FAIL(PageRef, page,
                        pm_->AllocateNewPage(setup, PageType::kRowPage));
  const page_id_t pid = page->PageID();
  ASSIGN_OR_ASSERT_FAIL(slot_t, first, page->Insert(setup, "first"));
  ASSIGN_OR_ASSERT_FAIL(slot_t, second, page->Insert(setup, "second"));
  ASSIGN_OR_ASSERT_FAIL(slot_t, third, page->Insert(setup, "third"));
  page.PageUnlock();
  ASSERT_SUCCESS(setup.PreCommit());
  Transaction owner = tm_->Begin();
  {
    ASSIGN_OR_ASSERT_FAIL(PageRef, current, pm_->GetPage(pid));
    ASSERT_SUCCESS(current->Delete(owner, first));
    ASSERT_SUCCESS(current->Delete(owner, third));
    ASSIGN_OR_ASSERT_FAIL(slot_t, replacement,
                          current->Insert(owner, "replacement"));
    EXPECT_EQ(replacement, first);
  }
  EXPECT_TRUE(owner.HasWriteSet(RowPosition(pid, third)));
  Transaction contender = tm_->Begin();
  EXPECT_FALSE(contender.TryAddWriteSet(RowPosition(pid, third)));
  ASSERT_SUCCESS(contender.Abort());
  ASSERT_SUCCESS(owner.Abort());
  ASSIGN_OR_ASSERT_FAIL(PageRef, restored, pm_->GetPage(pid));
  EXPECT_EQ(restored->body.row_page.GetRow(first), "first");
  EXPECT_EQ(restored->body.row_page.GetRow(second), "second");
  EXPECT_EQ(restored->body.row_page.GetRow(third), "third");
}

TEST_F(PageManagerTest, AuditAbortRestoresRowsAfterFreedSpaceIsReused) {
  for (const bool deleting : {false, true}) {
    SCOPED_TRACE(deleting);
    Transaction setup = tm_->Begin();
    ASSIGN_OR_ASSERT_FAIL(PageRef, page,
                          pm_->AllocateNewPage(setup, PageType::kRowPage));
    const page_id_t pid = page->PageID();
    const std::string original(8192, 'a');
    ASSIGN_OR_ASSERT_FAIL(slot_t, victim, page->Insert(setup, original));
    ASSIGN_OR_ASSERT_FAIL(slot_t, survivor,
                          page->Insert(setup, std::string(16000, 'b')));
    page.PageUnlock();
    ASSERT_SUCCESS(setup.PreCommit());
    Transaction undone = tm_->Begin();
    {
      ASSIGN_OR_ASSERT_FAIL(PageRef, current, pm_->GetPage(pid));
      if (deleting) {
        ASSERT_SUCCESS(current->Delete(undone, victim));
      } else {
        ASSERT_SUCCESS(current->Update(undone, victim, "short"));
      }
    }
    Transaction kept = tm_->Begin();
    const std::string enlarged(RowPage::kBlockBudgetBytes - 64, 'c');
    {
      ASSIGN_OR_ASSERT_FAIL(PageRef, current, pm_->GetPage(pid));
      ASSERT_SUCCESS(current->Update(kept, survivor, enlarged));
    }
    ASSERT_SUCCESS(kept.PreCommit());
    ASSERT_SUCCESS(undone.Abort());
    ASSIGN_OR_ASSERT_FAIL(PageRef, restored, pm_->GetPage(pid));
    EXPECT_EQ(restored->body.row_page.GetRow(victim), original);
    EXPECT_EQ(restored->body.row_page.GetRow(survivor), enlarged);
    EXPECT_EQ(restored->body.row_page.RowCount(), 2U);
  }
}

// Aborting an INSERT must restore the empty slot it came from. Using DeleteRow
// instead leaves the inserted row's length in hole_sizes_ as a phantom undo
// reservation; a later insert on a full page then subtracts it as a locked
// hole and wrongly returns kNoSpace.
TEST_F(PageManagerTest, AbortInsertClearsHoleReservation) {
  auto setup = tm_->Begin();
  ASSIGN_OR_ASSERT_FAIL(PageRef, page,
                        pm_->AllocateNewPage(setup, PageType::kRowPage));
  const page_id_t pid = page->PageID();
  ASSIGN_OR_ASSERT_FAIL(slot_t, kept,
                        page->Insert(setup, std::string(4096, 'a')));
  page.PageUnlock();
  ASSERT_SUCCESS(setup.PreCommit());

  slot_t aborted_slot = 0;
  {
    auto undone = tm_->Begin();
    ASSIGN_OR_ASSERT_FAIL(PageRef, ref, pm_->GetPage(pid));
    ASSIGN_OR_ASSERT_FAIL(slot_t, inserted,
                          ref->Insert(undone, std::string(8192, 'b')));
    aborted_slot = inserted;
    ref.PageUnlock();
    ASSERT_SUCCESS(undone.Abort());
  }

  ASSIGN_OR_ASSERT_FAIL(PageRef, after, pm_->GetPage(pid));
  EXPECT_TRUE(after->body.row_page.GetRow(aborted_slot).empty());
  EXPECT_EQ(after->body.row_page.HoleReservationForTest(aborted_slot), 0U);
  EXPECT_EQ(after->body.row_page.GetRow(kept), std::string(4096, 'a'));
  after.PageUnlock();
}

// P666: an UPDATE shrink frees bytes the abort-restore may need.  While the
// shrinker is open, foreign Inserts must treat the delta like a locked delete
// hole (available = free - reservations); RestoreLiveSlot itself stays
// unchecked, so unreserved consumption would push the page past
// kBlockBudgetBytes on abort.
TEST_F(PageManagerTest, UpdateShrinkReservesFreeSpaceForAbortRestore) {
  Transaction setup = tm_->Begin();
  ASSIGN_OR_ASSERT_FAIL(PageRef, page,
                        pm_->AllocateNewPage(setup, PageType::kRowPage));
  const page_id_t pid = page->PageID();
  ASSIGN_OR_ASSERT_FAIL(slot_t, victim,
                        page->Insert(setup, std::string(4096, 'a')));
  page.PageUnlock();
  ASSERT_SUCCESS(setup.PreCommit());

  constexpr size_t kShrunk = 64;
  const size_t freed = 4096 - kShrunk;
  Transaction shrinker = tm_->Begin();
  {
    ASSIGN_OR_ASSERT_FAIL(PageRef, current, pm_->GetPage(pid));
    ASSERT_SUCCESS(
        current->Update(shrinker, victim, std::string(kShrunk, 'b')));
  }
  EXPECT_TRUE(shrinker.HasWriteSet(RowPosition(pid, victim)));

  Transaction contender = tm_->Begin();
  {
    ASSIGN_OR_ASSERT_FAIL(PageRef, current, pm_->GetPage(pid));
    const size_t free = current->body.row_page.FreeSize();
    ASSERT_GT(free, freed);
    // Fits the raw free space but eats into the shrink reservation.
    StatusOr<slot_t> refused =
        current->Insert(contender, std::string(free - (freed / 2), 'c'));
    EXPECT_EQ(refused.GetStatus(), Status::kNoSpace)
        << "shrink delta must stay reserved for the abort-restore";
    // Inside the reservation it still inserts.
    ASSERT_SUCCESS(
        current->Insert(contender, std::string(free - freed - 64, 'd'))
            .GetStatus());
  }
  ASSERT_SUCCESS(contender.Abort());
  ASSERT_SUCCESS(shrinker.Abort());
  ASSIGN_OR_ASSERT_FAIL(PageRef, restored, pm_->GetPage(pid));
  EXPECT_EQ(restored->body.row_page.GetRow(victim), std::string(4096, 'a'));
  EXPECT_LE(restored->body.row_page.UsedBytes(), RowPage::kBlockBudgetBytes);
}

struct TestIndexEntry {
  RowPosition pos;
  Row include;
  friend Encoder& operator<<(Encoder& e, const TestIndexEntry& v) {
    e << v.pos << v.include;
    return e;
  }
  friend Decoder& operator>>(Decoder& d, TestIndexEntry& t) {
    d >> t.pos >> t.include;
    return d;
  }
};

TEST_F(PageManagerTest,
       MultiTxnNonUniqueIndexListDeltaIsolationCommitAbortAndRecovery) {
  constexpr page_id_t tree = 77777;
  const RowPosition pos0(11, 0);
  const RowPosition pos1(11, 1);
  const RowPosition pos2(11, 2);
  const auto encode_positions = [](std::initializer_list<RowPosition> rps) {
    std::vector<TestIndexEntry> entries;
    for (const RowPosition& rp : rps) {
      entries.push_back(TestIndexEntry{.pos = rp, .include = Row()});
    }
    return Encode(entries);
  };
  const auto decode_positions = [](std::string_view bytes) {
    StatusOr<std::vector<TestIndexEntry>> decoded =
        Decode<std::vector<TestIndexEntry>>(bytes);
    EXPECT_EQ(decoded.GetStatus(), Status::kSuccess);
    if (!decoded.HasValue()) {
      return std::vector<RowPosition>{};
    }
    std::vector<RowPosition> out;
    out.reserve(decoded.Value().size());
    for (const TestIndexEntry& e : decoded.Value()) {
      out.push_back(e.pos);
    }
    return out;
  };

  // Seed committed baseline [pos0] under "dup_key".
  {
    Transaction setup = tm_->Begin();
    ASSERT_SUCCESS(
        pm_->LsmInsert(setup, tree, "dup_key", encode_positions({pos0})));
    ASSERT_SUCCESS(setup.PreCommit());
  }

  // T1 removes pos0 and inserts pos1 ([pos0] -> [] -> [pos1]).
  // T2 concurrently reads the live list [pos1] and appends pos2 ([pos1, pos2]).
  Transaction t1 = tm_->Begin();
  Transaction t2 = tm_->Begin();
  Transaction obs = tm_->Begin();
  ASSERT_SUCCESS(pm_->LsmDelete(t1, tree, "dup_key"));
  ASSERT_SUCCESS(pm_->LsmInsert(t1, tree, "dup_key", encode_positions({pos1})));
  ASSERT_SUCCESS(
      pm_->LsmUpdate(t2, tree, "dup_key", encode_positions({pos1, pos2})));

  // Scan isolation:
  // - T1 sees only its own changes: [pos1]
  // - T2 sees committed baseline + its own delta: [pos0, pos2]
  // - Observer sees committed baseline: [pos0]
  const auto scan_t1 = pm_->LsmScanRange(tree, "dup_key", "dup_key", true, &t1);
  ASSERT_EQ(scan_t1.size(), 1U);
  EXPECT_EQ(decode_positions(scan_t1[0].second),
            (std::vector<RowPosition>{pos1}));

  const auto scan_t2 = pm_->LsmScanRange(tree, "dup_key", "dup_key", true, &t2);
  ASSERT_EQ(scan_t2.size(), 1U);
  EXPECT_EQ(decode_positions(scan_t2[0].second),
            (std::vector<RowPosition>{pos0, pos2}));

  const auto scan_obs =
      pm_->LsmScanRange(tree, "dup_key", "dup_key", true, &obs);
  ASSERT_EQ(scan_obs.size(), 1U);
  EXPECT_EQ(decode_positions(scan_obs[0].second),
            (std::vector<RowPosition>{pos0}));

  // Commit T2 while T1 is still active: committed baseline must become
  // [pos0, pos2] without baking in T1's uncommitted pos1 or deletion of pos0.
  ASSERT_SUCCESS(t2.PreCommit());
  const auto scan_obs_after_t2 =
      pm_->LsmScanRange(tree, "dup_key", "dup_key", true, &obs);
  ASSERT_EQ(scan_obs_after_t2.size(), 1U);
  EXPECT_EQ(decode_positions(scan_obs_after_t2[0].second),
            (std::vector<RowPosition>{pos0, pos2}));

  // Abort T1: live memtable and committed state must both equal [pos0, pos2].
  ASSERT_SUCCESS(t1.Abort());
  ASSERT_SUCCESS(obs.Abort());

  ASSIGN_OR_ASSERT_FAIL(std::string, live_after_abort,
                        pm_->LsmRead(tree, "dup_key"));
  EXPECT_EQ(decode_positions(live_after_abort),
            (std::vector<RowPosition>{pos0, pos2}));

  // Verify crash recovery also reconstructs [pos0, pos2] without phantom pos1.
  pm_->DropAllState();
  ASSERT_SUCCESS(rm_->RecoverFrom(0, tm_.get()));
  ASSIGN_OR_ASSERT_FAIL(std::string, recovered, pm_->LsmRead(tree, "dup_key"));
  EXPECT_EQ(decode_positions(recovered),
            (std::vector<RowPosition>{pos0, pos2}));
}

TEST_F(PageManagerTest, EvictsColdCommittedPagesToDiskAndReloadsOnAccess) {
  // Enforce a tiny 256 KiB buffer pool limit so multiple 20 KiB committed pages
  // trigger eviction to <db>.db.blocks and transparent reload on GetPage.
  pm_->SetBufferPoolLimitBytesForTest(256ULL * 1024ULL);
  std::vector<std::pair<page_id_t, slot_t>> refs;
  refs.reserve(40);
  const std::string payload(8000, 'X');
  for (int i = 0; i < 40; ++i) {
    Transaction txn = tm_->Begin();
    ASSIGN_OR_ASSERT_FAIL(PageRef, page,
                          pm_->AllocateNewPage(txn, PageType::kRowPage));
    const page_id_t pid = page->PageID();
    ASSIGN_OR_ASSERT_FAIL(slot_t, slot, page->Insert(txn, payload));
    page.PageUnlock();
    ASSERT_SUCCESS(txn.PreCommit());
    refs.emplace_back(pid, slot);
  }
  EXPECT_GT(pm_->EvictedPageCount(), 0U);
  EXPECT_LE(pm_->ResidentBytes(), 512U * 1024U);

  // Verify every page (evicted or resident) returns intact rows cleanly.
  Transaction reader = tm_->Begin();
  for (const auto& [pid, slot] : refs) {
    ASSIGN_OR_ASSERT_FAIL(PageRef, loaded, pm_->GetPage(pid, /*shared=*/true));
    ASSIGN_OR_ASSERT_FAIL(std::string_view, row, loaded->Read(reader, slot));
    EXPECT_EQ(row, payload);
  }
  ASSERT_SUCCESS(reader.PreCommit());

  // Persist v3 checkpoint (>1 MiB payload / evicted blocks), simulate restart,
  // and verify reload + lazy hydration work identically.
  const lsn_t ckpt_lsn = logger_->BufferedLSN();
  ASSERT_SUCCESS(logger_->WaitForDurable(ckpt_lsn));
  ASSERT_SUCCESS(pm_->WriteFuzzyCheckpoint(ckpt_lsn));
  pm_->DropAllPages();
  lsn_t loaded_lsn = 0;
  bool loaded = false;
  ASSERT_SUCCESS(pm_->LoadFuzzyCheckpoint(&loaded_lsn, &loaded));
  EXPECT_TRUE(loaded);
  EXPECT_EQ(loaded_lsn, ckpt_lsn);
  EXPECT_EQ(pm_->EvictedPageCount(), refs.size());

  Transaction post = tm_->Begin();
  for (const auto& [pid, slot] : refs) {
    ASSIGN_OR_ASSERT_FAIL(PageRef, loaded_page,
                          pm_->GetPage(pid, /*shared=*/false));
    ASSIGN_OR_ASSERT_FAIL(std::string_view, row, loaded_page->Read(post, slot));
    EXPECT_EQ(row, payload);
  }
  ASSERT_SUCCESS(post.PreCommit());
}

TEST_F(PageManagerTest, CompactNonResidentDirectoryAndRejectsV3Checkpoint) {
  pm_->SetBufferPoolLimitBytesForTest(size_t{64} << 10);

  constexpr int kPages = 600;
  std::vector<page_id_t> pids;
  std::vector<slot_t> slots;
  pids.reserve(kPages);
  slots.reserve(kPages);

  Transaction setup = tm_->Begin();
  for (int i = 0; i < kPages; ++i) {
    ASSIGN_OR_ASSERT_FAIL(PageRef, page,
                          pm_->AllocateNewPage(setup, PageType::kRowPage));
    const std::string payload =
        "compact_row_" + std::to_string(i) + "_" + std::string(1600, 'x');
    ASSIGN_OR_ASSERT_FAIL(slot_t, slot, page->Insert(setup, payload));
    pids.push_back(page->PageID());
    slots.push_back(slot);
    page.PageUnlock();
  }
  ASSERT_SUCCESS(setup.PreCommit());
  ASSERT_SUCCESS(pm_->EvictColdPagesForTest(size_t{32} << 10));

  EXPECT_GE(pm_->EvictedPageCount(), 500U);
  // Resident page memory stays near the target pool limit because clean
  // evicted pages are dropped from `blocks_` instead of retaining shells.
  EXPECT_LE(pm_->ResidentBytes(), size_t{96} << 10);
  // 16B sparse chunk table slots use well under 24B/evicted page when dense.
  EXPECT_LE(pm_->EvictedDirBytes(), (pm_->EvictedPageCount() * 24U) + 4096U);

  // Fault a subset of evicted pages back from `.blocks`.
  Transaction reader = tm_->Begin();
  for (const size_t i : {size_t{0}, size_t{17}, size_t{255}, size_t{256},
                         size_t{511}, size_t{599}}) {
    ASSIGN_OR_ASSERT_FAIL(PageRef, page, pm_->GetPage(pids[i]));
    ASSIGN_OR_ASSERT_FAIL(std::string_view, row, page->Read(reader, slots[i]));
    const std::string expected =
        "compact_row_" + std::to_string(i) + "_" + std::string(1600, 'x');
    EXPECT_EQ(row, expected);
    page.PageUnlock();
  }
  ASSERT_SUCCESS(reader.PreCommit());

  // Craft a version-3 header on top of a valid v4 checkpoint and confirm ADR
  // 0011 version rejection fails explicitly with Status::kCorrupt.
  ASSERT_SUCCESS(pm_->WriteFuzzyCheckpoint(logger_->BufferedLSN()));
  std::string ckpt_bytes;
  {
    std::ifstream in(prefix_ + ".db", std::ios::binary);
    ASSERT_TRUE(in.good());
    ckpt_bytes.assign(std::istreambuf_iterator<char>(in),
                      std::istreambuf_iterator<char>());
  }
  ASSERT_GE(ckpt_bytes.size(), 16U);
  const size_t magic_pos = ckpt_bytes.find("TLSMCKP1");
  ASSERT_NE(magic_pos, std::string::npos);
  const size_t ver_pos = magic_pos + 8;
  ASSERT_LE(ver_pos + sizeof(uint32_t) + sizeof(uint32_t), ckpt_bytes.size());
  {
    uint32_t ver = 0;
    std::memcpy(&ver, ckpt_bytes.data() + ver_pos, sizeof(ver));
    ASSERT_EQ(ver, 4U);
  }
  const std::string bad_path = prefix_ + ".db.v3";
  const uint32_t v3 = 3;
  std::memcpy(ckpt_bytes.data() + ver_pos, &v3, sizeof(v3));
  const uint32_t new_crc =
      Crc32C(ckpt_bytes.data(), ckpt_bytes.size() - sizeof(uint32_t));
  std::memcpy(ckpt_bytes.data() + ckpt_bytes.size() - sizeof(uint32_t),
              &new_crc, sizeof(new_crc));
  {
    std::ofstream out(bad_path, std::ios::binary | std::ios::trunc);
    out.write(ckpt_bytes.data(),
              static_cast<std::streamsize>(ckpt_bytes.size()));
    ASSERT_TRUE(out.good());
  }
  StatusOr<std::unique_ptr<PageManager>> bad_open =
      PageManager::Create(bad_path, 64, true);
  if (bad_open.HasValue()) {
    lsn_t start = 0;
    EXPECT_EQ(bad_open.Value()->LoadFuzzyCheckpoint(&start), Status::kCorrupt);
  } else {
    EXPECT_EQ(bad_open.GetStatus(), Status::kCorrupt);
  }
  std::ignore = std::remove(bad_path.c_str());
}

TEST_F(PageManagerTest,
       OutOfCoreDatasetBeyond5xPoolWithLsmCompactionAndRestart) {
  constexpr size_t kPoolBytes = size_t{96} << 10;
  pm_->SetBufferPoolLimitBytesForTest(kPoolBytes);

  // Write >5x pool size (>600 KiB) in RowPages plus multi-batch LSM entries so
  // both `.blocks.<gen>` and `.lsm.<gen>` (with K-way compaction across >=4
  // persisted runs) are exercised across restart.
  constexpr int kBatches = 5;
  constexpr int kPagesPerBatch = 36;
  constexpr int kLsmKeysPerBatch = 180;
  constexpr page_id_t kTreeA = 70001;
  constexpr page_id_t kTreeB = 70002;

  std::vector<std::pair<page_id_t, slot_t>> rows;
  rows.reserve(static_cast<size_t>(kBatches) * kPagesPerBatch);

  for (int b = 0; b < kBatches; ++b) {
    Transaction txn = tm_->Begin();
    for (int p = 0; p < kPagesPerBatch; ++p) {
      const int idx = (b * kPagesPerBatch) + p;
      ASSIGN_OR_ASSERT_FAIL(PageRef, page,
                            pm_->AllocateNewPage(txn, PageType::kRowPage));
      const std::string payload =
          "row_" + std::to_string(idx) + "_" + std::string(3400, 'r');
      ASSIGN_OR_ASSERT_FAIL(slot_t, slot, page->Insert(txn, payload));
      rows.emplace_back(page->PageID(), slot);
      page.PageUnlock();
    }
    for (int k = 0; k < kLsmKeysPerBatch; ++k) {
      const int kid = (b * kLsmKeysPerBatch) + k;
      std::array<char, 32> kbuf{};
      (void)std::snprintf(kbuf.data(), kbuf.size(), "k_%05d", kid);
      const std::string val(768, static_cast<char>('a' + (kid % 26)));
      ASSERT_SUCCESS(pm_->LsmInsert(txn, kTreeA, kbuf.data(), val));
      if (k % 3 == 0) {
        ASSERT_SUCCESS(pm_->LsmInsert(txn, kTreeB, kbuf.data(), val));
      }
      if (b > 0 && kid % 17 == 0) {
        std::array<char, 32> delbuf{};
        (void)std::snprintf(delbuf.data(), delbuf.size(), "k_%05d", kid - 1);
        ASSERT_SUCCESS(pm_->LsmDelete(txn, kTreeA, delbuf.data()));
      }
    }
    ASSERT_SUCCESS(txn.PreCommit());
    ASSERT_SUCCESS(pm_->WriteFuzzyCheckpoint(logger_->BufferedLSN()));
  }

  ASSERT_SUCCESS(pm_->EvictColdPagesForTest(kPoolBytes));
  EXPECT_GE(pm_->EvictedPageCount(), 100U);
  EXPECT_LE(pm_->ResidentBytes(), kPoolBytes * 2U);

  // Drop in-memory state and recover from the latest fuzzy checkpoint + WAL.
  tm_.reset();
  rm_.reset();
  logger_.reset();
  pm_.reset();

  pm_ = PageManager::Create(prefix_ + ".db", 64, true).MoveValue();
  pm_->SetBufferPoolLimitBytesForTest(kPoolBytes);
  logger_ = Logger::Create(prefix_ + ".log").MoveValue();
  pm_->SetDurableWaiter(
      [l = logger_.get()](lsn_t lsn) { return l->WaitForDurable(lsn); });
  rm_ = std::make_unique<RecoveryManager>(prefix_ + ".log", pm_->GetPool(),
                                          logger_->SegmentSize());
  rm_->SetLogger(logger_.get());
  tm_ =
      std::make_unique<TransactionManager>(pm_.get(), logger_.get(), rm_.get());
  ASSERT_SUCCESS(rm_->RecoverFrom(0, tm_.get()));

  Transaction verify = tm_->Begin();
  for (size_t i = 0; i < rows.size(); ++i) {
    ASSIGN_OR_ASSERT_FAIL(PageRef, page, pm_->GetPage(rows[i].first));
    ASSIGN_OR_ASSERT_FAIL(std::string_view, got,
                          page->Read(verify, rows[i].second));
    const std::string expected =
        "row_" + std::to_string(i) + "_" + std::string(3400, 'r');
    ASSERT_EQ(got, expected);
    page.PageUnlock();
  }

  const auto range_a =
      pm_->LsmScanRange(kTreeA, "k_00000", "k_99999", true, &verify);
  // Tree A inserts one key per (batch, slot) and tombstones kid-1 whenever
  // b > 0 && kid % 17 == 0. Mirror that generation here so a lost key or a
  // resurrected tombstone is caught, not just an empty scan.
  std::set<int> deleted_a;
  for (int b = 1; b < kBatches; ++b) {
    for (int k = 0; k < kLsmKeysPerBatch; ++k) {
      const int kid = (b * kLsmKeysPerBatch) + k;
      if (kid % 17 == 0) {
        deleted_a.insert(kid - 1);
      }
    }
  }
  EXPECT_EQ(range_a.size(), static_cast<size_t>(kBatches * kLsmKeysPerBatch) -
                                deleted_a.size());
  const auto range_b =
      pm_->LsmScanRange(kTreeB, "k_00000", "k_99999", true, &verify);
  EXPECT_EQ(range_b.size(),
            static_cast<size_t>(kBatches * (kLsmKeysPerBatch / 3)));

  // Spot-check deleted vs live keys after K-way merge + restart.
  const auto pt_del =
      pm_->LsmScanRange(kTreeA, "k_00186", "k_00186", true, &verify);
  EXPECT_TRUE(pt_del.empty());
  const auto pt_live =
      pm_->LsmScanRange(kTreeA, "k_00187", "k_00187", true, &verify);
  ASSERT_EQ(pt_live.size(), 1U);
  EXPECT_EQ(pt_live[0].second,
            std::string(768, static_cast<char>('a' + (187 % 26))));
  ASSERT_SUCCESS(verify.PreCommit());
}

TEST_F(PageManagerTest,
       UncommittedDirtyPagesSpillUnderTightPoolForUpdateInsertAndCommit) {
  constexpr size_t kPoolBytes = size_t{64} << 10;
  pm_->SetBufferPoolLimitBytesForTest(kPoolBytes);

  constexpr int kBasePages = 40;
  constexpr int kNewPages = 120;
  std::vector<std::pair<page_id_t, slot_t>> base_rows;
  base_rows.reserve(kBasePages);
  {
    Transaction base = tm_->Begin();
    for (int i = 0; i < kBasePages; ++i) {
      ASSIGN_OR_ASSERT_FAIL(PageRef, page,
                            pm_->AllocateNewPage(base, PageType::kRowPage));
      const std::string payload =
          "base_" + std::to_string(i) + "_" + std::string(3400, 'b');
      ASSIGN_OR_ASSERT_FAIL(slot_t, slot, page->Insert(base, payload));
      base_rows.emplace_back(page->PageID(), slot);
      page.PageUnlock();
    }
    ASSERT_SUCCESS(base.PreCommit());
  }

  std::vector<std::pair<page_id_t, slot_t>> new_rows;
  new_rows.reserve(kNewPages);
  {
    Transaction bulk = tm_->Begin();
    for (size_t i = 0; i < static_cast<size_t>(kBasePages); ++i) {
      ASSIGN_OR_ASSERT_FAIL(PageRef, page, pm_->GetPage(base_rows[i].first));
      const std::string upd =
          "upd_" + std::to_string(i) + "_" + std::string(3400, 'u');
      ASSERT_SUCCESS(page->Update(bulk, base_rows[i].second, upd));
      page.PageUnlock();
    }
    for (size_t i = 0; i < static_cast<size_t>(kNewPages); ++i) {
      ASSIGN_OR_ASSERT_FAIL(PageRef, page,
                            pm_->AllocateNewPage(bulk, PageType::kRowPage));
      const std::string ins =
          "new_" + std::to_string(i) + "_" + std::string(3400, 'n');
      ASSIGN_OR_ASSERT_FAIL(slot_t, slot, page->Insert(bulk, ins));
      new_rows.emplace_back(page->PageID(), slot);
      page.PageUnlock();
    }

    ASSERT_SUCCESS(pm_->EvictColdPagesForTest(kPoolBytes / 2));
    EXPECT_GT(pm_->UncommittedSpillEvictions(), 0U);
    EXPECT_GT(pm_->UncommittedSpillCount(), 0U);
    EXPECT_LE(pm_->ResidentBytes(), kPoolBytes);

    // Fault spilled uncommitted pages (both updated-committed and new
    // uncommitted-only pages) back before commit and check live values.
    {
      ASSIGN_OR_ASSERT_FAIL(PageRef, p0, pm_->GetPage(base_rows[0].first));
      ASSIGN_OR_ASSERT_FAIL(std::string_view, v0,
                            p0->Read(bulk, base_rows[0].second));
      EXPECT_EQ(v0, "upd_0_" + std::string(3400, 'u'));
      p0.PageUnlock();

      ASSIGN_OR_ASSERT_FAIL(PageRef, pn, pm_->GetPage(new_rows[0].first));
      ASSIGN_OR_ASSERT_FAIL(std::string_view, vn,
                            pn->Read(bulk, new_rows[0].second));
      EXPECT_EQ(vn, "new_0_" + std::string(3400, 'n'));
      pn.PageUnlock();
    }
    EXPECT_GT(pm_->UncommittedSpillFaults(), 0U);

    ASSERT_SUCCESS(bulk.PreCommit());
  }

  EXPECT_EQ(pm_->UncommittedSpillCount(), 0U);
  ASSERT_SUCCESS(pm_->WriteFuzzyCheckpoint(logger_->BufferedLSN()));

  // Restart and confirm every updated and newly inserted row persisted.
  tm_.reset();
  rm_.reset();
  logger_.reset();
  pm_.reset();
  pm_ = PageManager::Create(prefix_ + ".db", 64, true).MoveValue();
  pm_->SetBufferPoolLimitBytesForTest(kPoolBytes);
  logger_ = Logger::Create(prefix_ + ".log").MoveValue();
  pm_->SetDurableWaiter(
      [l = logger_.get()](lsn_t lsn) { return l->WaitForDurable(lsn); });
  rm_ = std::make_unique<RecoveryManager>(prefix_ + ".log", pm_->GetPool(),
                                          logger_->SegmentSize());
  rm_->SetLogger(logger_.get());
  tm_ =
      std::make_unique<TransactionManager>(pm_.get(), logger_.get(), rm_.get());
  ASSERT_SUCCESS(rm_->RecoverFrom(0, tm_.get()));

  Transaction verify = tm_->Begin();
  for (size_t i = 0; i < static_cast<size_t>(kBasePages); ++i) {
    ASSIGN_OR_ASSERT_FAIL(PageRef, p, pm_->GetPage(base_rows[i].first));
    ASSIGN_OR_ASSERT_FAIL(std::string_view, got,
                          p->Read(verify, base_rows[i].second));
    ASSERT_EQ(got, "upd_" + std::to_string(i) + "_" + std::string(3400, 'u'));
    p.PageUnlock();
  }
  for (size_t i = 0; i < static_cast<size_t>(kNewPages); ++i) {
    ASSIGN_OR_ASSERT_FAIL(PageRef, p, pm_->GetPage(new_rows[i].first));
    ASSIGN_OR_ASSERT_FAIL(std::string_view, got,
                          p->Read(verify, new_rows[i].second));
    ASSERT_EQ(got, "new_" + std::to_string(i) + "_" + std::string(3400, 'n'));
    p.PageUnlock();
  }
  ASSERT_SUCCESS(verify.PreCommit());
}

TEST_F(PageManagerTest, UncommittedSpilledPagesAbortAndCrashDiscardOverlays) {
  constexpr size_t kPoolBytes = size_t{64} << 10;
  pm_->SetBufferPoolLimitBytesForTest(kPoolBytes);

  constexpr int kBasePages = 24;
  constexpr int kExtraPages = 60;
  std::vector<std::pair<page_id_t, slot_t>> base_rows;
  base_rows.reserve(kBasePages);
  {
    Transaction setup = tm_->Begin();
    for (int i = 0; i < kBasePages; ++i) {
      ASSIGN_OR_ASSERT_FAIL(PageRef, p,
                            pm_->AllocateNewPage(setup, PageType::kRowPage));
      ASSIGN_OR_ASSERT_FAIL(slot_t, slot,
                            p->Insert(setup, "baseline_" + std::to_string(i) +
                                                 "_" + std::string(3400, 'z')));
      base_rows.emplace_back(p->PageID(), slot);
      p.PageUnlock();
    }
    ASSERT_SUCCESS(setup.PreCommit());
  }

  // 1) Aborted transaction after uncommitted page spill restores baseline rows
  // and drops uncommitted-only spilled pages cleanly.
  {
    Transaction doomed = tm_->Begin();
    for (size_t i = 0; i < static_cast<size_t>(kBasePages); ++i) {
      ASSIGN_OR_ASSERT_FAIL(PageRef, p, pm_->GetPage(base_rows[i].first));
      ASSERT_SUCCESS(p->Update(
          doomed, base_rows[i].second,
          "aborted_" + std::to_string(i) + "_" + std::string(3400, 'a')));
      p.PageUnlock();
    }
    for (int i = 0; i < kExtraPages; ++i) {
      ASSIGN_OR_ASSERT_FAIL(PageRef, p,
                            pm_->AllocateNewPage(doomed, PageType::kRowPage));
      ASSERT_SUCCESS(p->Insert(doomed, std::string(3400, 'q')).GetStatus());
      p.PageUnlock();
    }
    ASSERT_SUCCESS(pm_->EvictColdPagesForTest(kPoolBytes / 4));
    EXPECT_GT(pm_->UncommittedSpillEvictions(), 0U);
    EXPECT_LE(pm_->ResidentBytes(), kPoolBytes);
    ASSERT_SUCCESS(doomed.Abort());
  }
  EXPECT_EQ(pm_->UncommittedSpillCount(), 0U);

  // 2) Fuzzy checkpoint DURING an active spilled uncommitted TX followed by a
  // crash must persist ONLY committed baselines (ADR 0007 invariant).
  const uint64_t spills_before = pm_->UncommittedSpillEvictions();
  {
    Transaction loser = tm_->Begin();
    for (size_t i = 0; i < static_cast<size_t>(kBasePages); ++i) {
      ASSIGN_OR_ASSERT_FAIL(PageRef, p, pm_->GetPage(base_rows[i].first));
      ASSERT_SUCCESS(p->Update(
          loser, base_rows[i].second,
          "crashed_" + std::to_string(i) + "_" + std::string(3400, 'x')));
      p.PageUnlock();
    }
    std::vector<page_id_t> loser_new_pids;
    for (int i = 0; i < kExtraPages; ++i) {
      ASSIGN_OR_ASSERT_FAIL(PageRef, p,
                            pm_->AllocateNewPage(loser, PageType::kRowPage));
      loser_new_pids.push_back(p->PageID());
      ASSERT_SUCCESS(p->Insert(loser, std::string(3400, 'y')).GetStatus());
      p.PageUnlock();
    }
    ASSERT_SUCCESS(pm_->EvictColdPagesForTest(kPoolBytes / 4));
    EXPECT_GT(pm_->UncommittedSpillEvictions(), spills_before);
    ASSERT_SUCCESS(pm_->WriteFuzzyCheckpoint(logger_->BufferedLSN()));
    ASSERT_SUCCESS(logger_->WaitForDurable(logger_->BufferedLSN()));

    // Crash without abort or commit.
    pm_->DropAllPages();
    loser = Transaction();

    tm_.reset();
    rm_.reset();
    logger_.reset();
    pm_.reset();
    pm_ = PageManager::Create(prefix_ + ".db", 64, true).MoveValue();
    logger_ = Logger::Create(prefix_ + ".log").MoveValue();
    pm_->SetDurableWaiter(
        [l = logger_.get()](lsn_t lsn) { return l->WaitForDurable(lsn); });
    rm_ = std::make_unique<RecoveryManager>(prefix_ + ".log", pm_->GetPool(),
                                            logger_->SegmentSize());
    rm_->SetLogger(logger_.get());
    tm_ = std::make_unique<TransactionManager>(pm_.get(), logger_.get(),
                                               rm_.get());
    ASSERT_SUCCESS(rm_->RecoverFrom(0, tm_.get()));

    Transaction verify = tm_->Begin();
    for (size_t i = 0; i < static_cast<size_t>(kBasePages); ++i) {
      ASSIGN_OR_ASSERT_FAIL(PageRef, p, pm_->GetPage(base_rows[i].first));
      ASSIGN_OR_ASSERT_FAIL(std::string_view, got,
                            p->Read(verify, base_rows[i].second));
      ASSERT_EQ(got,
                "baseline_" + std::to_string(i) + "_" + std::string(3400, 'z'));
      p.PageUnlock();
    }
    for (page_id_t pid : loser_new_pids) {
      auto p_or = pm_->GetPage(pid);
      EXPECT_FALSE(p_or.HasValue());
    }
    ASSERT_SUCCESS(verify.PreCommit());
  }
}

TEST_F(PageManagerTest,
       ReplayCommittedLargeIndexListExactMatchAndGeneralDelta) {
  constexpr page_id_t tree = 88888;
  const auto encode_positions = [](const std::vector<RowPosition>& rps) {
    std::vector<TestIndexEntry> entries;
    entries.reserve(rps.size());
    for (const RowPosition& rp : rps) {
      entries.push_back(TestIndexEntry{.pos = rp, .include = Row()});
    }
    return Encode(entries);
  };
  const auto decode_positions = [](std::string_view bytes) {
    StatusOr<std::vector<TestIndexEntry>> decoded =
        Decode<std::vector<TestIndexEntry>>(bytes);
    EXPECT_EQ(decoded.GetStatus(), Status::kSuccess);
    if (!decoded.HasValue()) {
      return std::vector<RowPosition>{};
    }
    std::vector<RowPosition> out;
    out.reserve(decoded.Value().size());
    for (const TestIndexEntry& e : decoded.Value()) {
      out.push_back(e.pos);
    }
    return out;
  };

  std::vector<RowPosition> baseline;
  baseline.reserve(300);
  for (size_t i = 0; i < 300; ++i) {
    baseline.emplace_back(static_cast<page_id_t>(20 + (i / 50)),
                          static_cast<slot_t>(i % 50));
  }
  {
    Transaction setup = tm_->Begin();
    ASSERT_SUCCESS(
        pm_->LsmInsert(setup, tree, "hot_key", encode_positions(baseline)));
    ASSERT_SUCCESS(setup.PreCommit());
  }

  // Concurrent append (+50) followed by erase (-10) reading the live state
  // exercises both exact-equality fast path (when sequential) and general
  // delta.
  std::vector<RowPosition> after_t1 = baseline;
  after_t1.reserve(50);
  for (size_t i = 0; i < 50; ++i) {
    after_t1.emplace_back(static_cast<page_id_t>(100 + i), 0);
  }
  std::vector<RowPosition> after_t2 = after_t1;
  after_t2.erase(after_t2.begin(), after_t2.begin() + 10);

  Transaction t1 = tm_->Begin();
  Transaction t2 = tm_->Begin();
  ASSERT_SUCCESS(
      pm_->LsmUpdate(t1, tree, "hot_key", encode_positions(after_t1)));
  ASSERT_SUCCESS(
      pm_->LsmUpdate(t2, tree, "hot_key", encode_positions(after_t2)));
  // Commit in reverse order so replay applies t1 then t2 with exact deltas.
  ASSERT_SUCCESS(t2.PreCommit());
  ASSERT_SUCCESS(t1.PreCommit());

  pm_->DropAllState();
  ASSERT_SUCCESS(rm_->RecoverFrom(0, tm_.get()));

  ASSIGN_OR_ASSERT_FAIL(std::string, recovered, pm_->LsmRead(tree, "hot_key"));
  const std::vector<RowPosition> recovered_pos = decode_positions(recovered);
  EXPECT_EQ(recovered_pos, after_t2);
}

// #1110: Insert must not pay a write-intent acquire/release pair per hole.
// Pins the verdicts the restructured scan shares with the old per-hole
// probing: committed holes are first-fit candidates, foreign-intent holes
// only feed the locked-byte accounting (read-only probe), the too-large
// record short-circuits without touching any intent, and the row that is
// written reads back.
TEST_F(PageManagerTest, InsertOnHoleyPageKeepsIntentVerdicts) {
  auto setup = tm_->Begin();
  ASSIGN_OR_ASSERT_FAIL(PageRef, page,
                        pm_->AllocateNewPage(setup, PageType::kRowPage));
  const page_id_t pid = page->PageID();
  std::vector<slot_t> slots;
  slots.reserve(20);
  for (int i = 0; i < 20; ++i) {
    ASSIGN_OR_ASSERT_FAIL(slot_t, slot,
                          page->Insert(setup, "row-" + std::to_string(i)));
    slots.push_back(slot);
  }
  // Holes at index 3/7/11 belong to a committed delete (no intent held);
  // index 5/9 stay write-intent-locked by the open `foreign` txn below.
  for (const size_t idx : {3U, 7U, 11U}) {
    ASSERT_SUCCESS(page->Delete(setup, slots[idx]));
  }
  page.PageUnlock();
  ASSERT_SUCCESS(setup.PreCommit());

  auto foreign = tm_->Begin();
  {
    auto foreign_ref = pm_->GetPage(pid, /*shared=*/false);
    ASSERT_TRUE(foreign_ref.HasValue());
    for (const size_t idx : {5U, 9U}) {
      ASSERT_SUCCESS(foreign_ref.Value()->Delete(foreign, slots[idx]));
    }
    foreign_ref.Value().PageUnlock();
  }

  auto probe = tm_->Begin();
  auto probe_ref = pm_->GetPage(pid, /*shared=*/false);
  ASSERT_TRUE(probe_ref.HasValue());
  // First-fit lands on the earliest committed-freed hole (index 3): the
  // foreign-intent hole at index 5 must not steal it and must not be
  // acquired/released as a side effect.  #1110 contract: exactly ONE
  // write-intent acquire attempt (the candidate); the rest are read-only.
  RowPage::ResetHoleAcquireProbesForTest();
  ASSIGN_OR_ASSERT_FAIL(slot_t, reused, probe_ref.Value()->Insert(probe, "ok"));
  EXPECT_EQ(reused, slots[3]);
  EXPECT_EQ(RowPage::HoleAcquireProbesForTest(), 1U);
  ASSIGN_OR_ASSERT_FAIL(std::string_view, read_back,
                        probe_ref.Value()->Read(probe, reused));
  EXPECT_EQ(read_back, "ok");
  // A record beyond the whole block budget returns kNoSpace without ever
  // probing an intent: index 7's hole remains acquirable by this txn.
  const auto big = std::string(kPageBodySize * 2, 'z');
  RowPage::ResetHoleAcquireProbesForTest();
  EXPECT_EQ(probe_ref.Value()->Insert(probe, big).GetStatus(),
            Status::kNoSpace);
  EXPECT_EQ(RowPage::HoleAcquireProbesForTest(), 0U);
  const RowPosition hole(pid, slots[7]);
  ASSERT_TRUE(probe.TryAddWriteSet(hole));
  probe.ReleaseWriteIntent(hole);
  probe_ref.Value().PageUnlock();
  ASSERT_SUCCESS(foreign.PreCommit());
  ASSERT_SUCCESS(probe.PreCommit());
}

// Erasing the interior of a free-id range splits it in two; the split must
// honor kMaxRanges exactly like Insert/LoadRanges, or the checkpoint writer
// emits a free-range count the next LoadFuzzyCheckpoint rejects as kCorrupt.
TEST(BoundedFreeIdSetTest, EraseSplitEnforcesMaxRanges) {
  BoundedFreeIdSet free_ids;
  std::vector<std::pair<page_id_t, page_id_t>> ranges;
  ranges.reserve(BoundedFreeIdSet::kMaxRanges);
  for (size_t i = 0; i < BoundedFreeIdSet::kMaxRanges; ++i) {
    const auto lo = static_cast<page_id_t>((4 * i) + 1);
    ranges.emplace_back(lo, lo + 2);
  }
  free_ids.LoadRanges(ranges);
  ASSERT_EQ(free_ids.RangeCount(), BoundedFreeIdSet::kMaxRanges);
  // Interior erase of the lowest range: without the bound check the split
  // grows the map to kMaxRanges + 1.
  free_ids.Erase(2);
  EXPECT_LE(free_ids.RangeCount(), BoundedFreeIdSet::kMaxRanges);
  EXPECT_FALSE(free_ids.Contains(2));
  EXPECT_TRUE(free_ids.Contains(3));
  // Further interior splits stay bounded too.
  free_ids.Erase(6);
  EXPECT_LE(free_ids.RangeCount(), BoundedFreeIdSet::kMaxRanges);
  EXPECT_FALSE(free_ids.Contains(6));
}

TEST(EvictedPageTableTest, SetWithInvalidDescUndoesInsertBookkeeping) {
  // T4184: Set() used to only count empty->occupied transitions; writing an
  // invalid desc over an occupied slot left Count() overstating the live
  // entries forever (and kept Erase's count==0 chunk reclaim from firing).
  EvictedPageTable table;
  const EvictedPageDesc valid{
      .disk_offset = 4096, .disk_length = 1024, .file_gen = 1};
  table.Set(7, valid);
  ASSERT_TRUE(table.Contains(7));
  EXPECT_EQ(table.Count(), 1U);

  table.Set(7, EvictedPageDesc{});  // invalidate through Set
  EXPECT_FALSE(table.Contains(7));
  EXPECT_EQ(table.Count(), 0U);

  // Repeated valid/invalid round trips must not drift the count either.
  for (int i = 0; i < 4; ++i) {
    table.Set(7, valid);
    table.Set(7, EvictedPageDesc{});
  }
  EXPECT_EQ(table.Count(), 0U);
  table.Set(7, valid);
  EXPECT_EQ(table.Count(), 1U);
}

TEST(EvictedPageTableTest, InvalidDescNeverLeaksAnEmptyChunk) {
  EvictedPageTable table;
  const EvictedPageDesc valid{
      .disk_offset = 4096, .disk_length = 1024, .file_gen = 1};

  // Setting an invalid desc where no chunk exists must not allocate a chunk
  // that Erase would then refuse to reclaim (it reports {false, 0}).
  EXPECT_EQ(table.Set(7, EvictedPageDesc{}), 0);
  EXPECT_EQ(table.Bytes(), 0U);
  EXPECT_FALSE(table.Contains(7));
  EXPECT_FALSE(table.Erase(7).first);
  EXPECT_EQ(table.Bytes(), 0U);

  // A chunk emptied by invalidation through Set is reclaimed on the spot.
  EXPECT_GT(table.Set(7, valid), 0);
  EXPECT_GT(table.Bytes(), 0U);
  EXPECT_LT(table.Set(7, EvictedPageDesc{}), 0);
  EXPECT_EQ(table.Bytes(), 0U);
  EXPECT_FALSE(table.Contains(7));
}

// T1936: RowPage::InsertRowAt is the public low-level entry used by WAL
// hole-fill; internal callers pre-gate capacity but a slot at/over
// kMaxSlotsPerBlock must be rejected at the entry itself, not grow the
// packed slot index toward a 65535-entry (~256 KiB) page.
TEST(RowPageSlotCapTest, InsertRowAtRejectsSlotsAtOrPastCap) {
  RowPage page;
  page.Initialize();
  const StatusOr<slot_t> at_cap =
      page.InsertRowAt(RowPage::kMaxSlotsPerBlock, "r");
  ASSERT_FALSE(at_cap.HasValue());
  EXPECT_EQ(at_cap.GetStatus(), Status::kNoSpace);

  const StatusOr<slot_t> past_cap = page.InsertRowAt(
      static_cast<slot_t>(RowPage::kMaxSlotsPerBlock + 100), "r");
  ASSERT_FALSE(past_cap.HasValue());
  EXPECT_EQ(past_cap.GetStatus(), Status::kNoSpace);
  EXPECT_EQ(page.RowCount(), 0);

  const StatusOr<slot_t> in_range = page.InsertRowAt(0, "r");
  ASSERT_TRUE(in_range.HasValue());
  EXPECT_EQ(page.RowCount(), 1);
}

// T10851: PageInit is also called by recovery/abort paths that hold no
// PageRef. A recycled Page carrying the previous generation's exclusive
// owner would make IsExclusivelyOwnedByCurrentThread() spuriously true, so
// lock_page_if_needed would skip the content latch and race. PageInit must
// clear the owner so the next latch is always taken.
TEST(PageInitTest, ClearsStaleExclusiveOwner) {
  Page page(1, PageType::kRowPage);
  page.SetExclusiveOwner(std::this_thread::get_id());
  ASSERT_TRUE(page.IsExclusivelyOwnedByCurrentThread());

  page.PageInit(2, PageType::kRowPage);

  EXPECT_FALSE(page.IsExclusivelyOwnedByCurrentThread());
  EXPECT_EQ(page.PageID(), 2);
  EXPECT_EQ(page.Type(), PageType::kRowPage);
  // A second generation must not inherit it either.
  page.SetExclusiveOwner(std::this_thread::get_id());
  page.PageInit(3, PageType::kRowPage);
  EXPECT_FALSE(page.IsExclusivelyOwnedByCurrentThread());
  EXPECT_EQ(page.PageID(), 3);
}

// T10856: a positive WAL LSN with no connected durability waiter must not be
// silently reported durable. Otherwise EvictBatch / WriteFuzzyCheckpoint
// persist committed page images before the WAL reaches the durable frontier,
// breaking WAL-before-data (docs/recovery_invariants.md). A logger-less
// embedded PageManager only ever passes LSN 0, which stays a no-op.
TEST(PageManagerDurableWaiterTest,
     CheckpointWithoutWaiterFailsForPositiveWalLsn) {
  const std::string prefix = "pm_durable_waiter-" + RandomString();
  {
    // Deliberately no SetDurableWaiter: this is the embedded-with-logger
    // misconfiguration the barrier must catch.
    auto pm = PageManager::Create(prefix + ".db", 64, false).MoveValue();
    ASSERT_TRUE(pm != nullptr);

    // A committed alloc with a positive LSN stands in for committed WAL
    // records that back a disk-dirty page image.
    ASSERT_SUCCESS(pm->ReplayCommittedRecord(
        12345,
        LogRecord::AllocatePageLogRecord(0, 100, 1, PageType::kRowPage)));

    // The snapshot must refuse to be written: durability cannot be proven.
    EXPECT_EQ(pm->WriteFuzzyCheckpoint(0), Status::kRuntimeError);
  }
  std::ignore = std::remove((prefix + ".db").c_str());
}

}  // namespace
}  // namespace tinylamb
