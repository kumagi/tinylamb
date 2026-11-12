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

#include <chrono>
#include <cstdio>
#include <fstream>
#include <future>
#include <limits>
#include <memory>
#include <string>

#include "common/crc32c.hpp"
#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "page/page_ref.hpp"
#include "page/page_type.hpp"
#include "page/row_position.hpp"
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
    RemoveLogAndSegments(prefix_ + ".log");
  }

  std::string prefix_;
  std::unique_ptr<PageManager> pm_;
  std::unique_ptr<Logger> logger_;
  std::unique_ptr<RecoveryManager> rm_;
  std::unique_ptr<TransactionManager> tm_;
};

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
    {
      std::ofstream output(prefix_ + ".db", std::ios::binary | std::ios::trunc);
      output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
      ASSERT_TRUE(output.good());
    }
    bool loaded = true;
    Status status = Status::kSuccess;
    EXPECT_NO_THROW(status = pm_->LoadFuzzyCheckpoint(nullptr, &loaded));
    EXPECT_EQ(status, Status::kCorrupt);
    EXPECT_FALSE(loaded);
  }
}

TEST_F(PageManagerTest, AuditReallocationWaitsForContentLatchBeforeReset) {
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
  std::promise<void> entered;
  auto started = entered.get_future();
  auto allocation = std::async(std::launch::async, [&] {
    Transaction create = tm_->Begin();
    entered.set_value();
    auto result = pm_->AllocateNewPage(create, PageType::kRowPage);
    if (!result.HasValue()) {
      return page_id_t{0};
    }
    PageRef fresh = result.MoveValue();
    const page_id_t actual = fresh->PageID();
    fresh.PageUnlock();
    return create.PreCommit() == Status::kSuccess ? actual : page_id_t{0};
  });
  started.wait();
  EXPECT_EQ(allocation.wait_for(std::chrono::milliseconds(50)),
            std::future_status::timeout);
  // Holding the old reference must prevent reinitialization as well as
  // prevent the new caller from returning the page.
  EXPECT_EQ(held->Type(), PageType::kUnknown);
  EXPECT_EQ(held->PageLSN(), before);
  held.PageUnlock();
  EXPECT_EQ(allocation.get(), pid);
  ASSIGN_OR_ASSERT_FAIL(PageRef, reused, pm_->GetPage(pid));
  EXPECT_EQ(reused->Type(), PageType::kRowPage);
  EXPECT_GT(reused->PageLSN(), before);
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
    auto decoded = Decode<std::vector<TestIndexEntry>>(bytes).MoveValue();
    std::vector<RowPosition> out;
    out.reserve(decoded.size());
    for (const TestIndexEntry& e : decoded) {
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

}  // namespace
}  // namespace tinylamb
