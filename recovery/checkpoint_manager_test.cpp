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

#include "recovery/checkpoint_manager.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <tuple>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/serdes.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "page/page_ref.hpp"
#include "page/page_type.hpp"
#include "page/row_page_test.hpp"
#include "recovery/log_record.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"
#include "transaction/lock_manager.hpp"

namespace tinylamb {
class CheckpointTest : public RowPageTest {
 protected:
  void SetUp() override {
    std::string prefix = "checkpoint_test-" + RandomString();
    db_name_ = prefix + ".db";
    log_name_ = prefix + ".log";
    master_record_name_ = prefix + ".master.log";
    Recover();
    auto txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kRowPage).MoveValue();
    page_id_ = page->PageID();
    EXPECT_SUCCESS(txn.PreCommit());
  }

  void TearDown() override {
    cm_.reset();
    tm_.reset();
    lm_.reset();
    r_.reset();
    l_.reset();
    if (p_) {
      p_->GetPool()->DropAllPages();
    }
    p_.reset();
    // Best-effort cleanup; a missing file must not fail the test.
    (void)std::remove(db_name_.c_str());
    tinylamb::RemoveLogAndSegments(log_name_);
    (void)std::remove(master_record_name_.c_str());
  }

  void Recover() override {
    if (p_) {
      p_->GetPool()->DropAllPages();
    }
    cm_.reset();
    tm_.reset();
    lm_.reset();
    l_.reset();
    r_.reset();
    p_.reset();
    p_ = PageManager::Create(db_name_, 10).MoveValue();
    l_ = Logger::Create(log_name_).MoveValue();
    lm_ = std::make_unique<LockManager>();
    tm_ = std::make_unique<TransactionManager>(p_.get(), l_.get(), nullptr);
    r_ = std::make_unique<RecoveryManager>(log_name_, p_->GetPool());
    cm_ = std::make_unique<CheckpointManager>(master_record_name_, tm_.get(),
                                              p_->GetPool(), 1);
  }

  std::string db_name_;
  std::string log_name_;
  std::string master_record_name_;
  std::unique_ptr<RecoveryManager> r_;
  std::unique_ptr<CheckpointManager> cm_;
};

TEST_F(CheckpointTest, Construct) {
  // Arrange -- nothing to set up; default database created by SetUp()
  // Act -- nothing to execute; default constructed via SetUp()
  // Assert -- nothing to verify; gtest death on crash, gtest green on pass
}

// A crash between the master record's temp write and its rename leaves the
// .tmp file behind; constructing a CheckpointManager sweeps it so a
// long-lived deployment does not accumulate it.
TEST_F(CheckpointTest, ConstructorRemovesStaleMasterTmp) {
  // Arrange -- a stale temp file where WriteMasterRecord would have put one.
  const std::string tmp = master_record_name_ + ".tmp";
  {
    std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(out);
    out << "torn";
  }
  ASSERT_TRUE(std::filesystem::exists(tmp));

  // Act -- the fixture's cm_ predates this file; construct a fresh manager
  // over the same path (interval 0 keeps its worker parked).
  CheckpointManager sweeper(master_record_name_, tm_.get(), p_->GetPool(), 0);

  // Assert
  EXPECT_FALSE(std::filesystem::exists(tmp));
}

TEST_F(CheckpointTest, DoCheckpoint) {
  // Arrange -- nothing more than fixture setup

  // Act 1 -- insert a row, then checkpoint, then update+commit within {} scope
  InsertRow("expect this operation did not rerun");
  Transaction txn = tm_->Begin();
  slot_t slot = 0;
  {
    PageRef page = p_->GetPage(page_id_).MoveValue();
    ASSIGN_OR_ASSERT_FAIL(slot_t, inserted, page->Insert(txn, "inserted"));
    slot = inserted;
  }
  p_->GetPool()->FlushPageForTest(page_id_);
  cm_->WriteCheckpoint();
  {
    PageRef page = p_->GetPage(page_id_).MoveValue();
    ASSERT_SUCCESS(page->Update(txn, slot, "expect to be redone"));
    txn.PreCommit();
  }

  // Act 2 -- recover from log; redo should replay the post-checkpoint update
  Recover();
  r_->RecoverFrom(0, tm_.get());

  // Assert -- implicit; no crash, no explicit assertions; gtest green on pass
}

TEST_F(CheckpointTest, CheckpointRecovery) {
  // Arrange -- nothing more than fixture setup

  // Act 1 -- insert a row, then checkpoint at restart_point, then update+commit
  InsertRow("expect this operation did not rerun");
  Transaction txn = tm_->Begin();
  lsn_t restart_point = 0;
  slot_t result = 0;
  {
    PageRef page = p_->GetPage(page_id_).MoveValue();
    ASSIGN_OR_ASSERT_FAIL(slot_t, inserted, page->Insert(txn, "inserted"));
    result = inserted;
  }
  restart_point = cm_->WriteCheckpoint().Value();
  {
    PageRef page = p_->GetPage(page_id_).MoveValue();
    ASSERT_SUCCESS(page->Update(txn, result, "expect to be redone"));
    txn.PreCommit();
  }

  // Act 2 -- recover from restart_point; redo should replay the update
  Recover();
  r_->RecoverFrom(restart_point, tm_.get());

  // Assert -- recovered row reads back "expect to be redone"
  EXPECT_EQ(ReadRow(result), "expect to be redone");
}

TEST_F(CheckpointTest, CheckpointAbortRecovery) {
  // Arrange -- row 0 = "original message" committed
  ASSERT_TRUE(InsertRow("original message"));
  Transaction txn = tm_->Begin();
  const slot_t slot = 0;

  // Act 1 -- checkpoint, then update+insert without committing
  const lsn_t restart_point = cm_->WriteCheckpoint().Value();
  {
    PageRef page = p_->GetPage(page_id_).MoveValue();
    ASSERT_SUCCESS(page->Update(txn, slot, "aborted"));
    ASSERT_SUCCESS(page->Insert(txn, "will be deleted").GetStatus());
  }

  // Act 2 -- recover from restart_point; uncommitted changes discarded
  Recover();
  r_->RecoverFrom(restart_point, tm_.get());

  // Assert -- aborted txn leaves row 0 with original message, row count = 1
  ASSERT_EQ(GetRowCount(), 1);
  EXPECT_EQ(ReadRow(slot), "original message");
}

TEST_F(CheckpointTest, CheckpointUpdateAfterBeginCheckpoint) {
  // Arrange -- row 0 = "original message" committed
  ASSERT_TRUE(InsertRow("original message"));
  Transaction txn = tm_->Begin();
  const slot_t slot = 0;

  // Act 1 -- checkpoint with lambda that updates+inserts but does not commit;
  // the lambda acquires its own page latch so the checkpoint's DPT snapshot
  // never overlaps a foreign exclusive latch.
  const lsn_t restart_point =
      cm_->WriteCheckpoint([&]() {
           PageRef page = p_->GetPage(page_id_).MoveValue();
           ASSERT_SUCCESS(page->Update(txn, slot, "aborted"));
           ASSERT_SUCCESS(page->Insert(txn, "will be deleted").GetStatus());
         })
          .Value();

  // Act 2 -- recover from restart_point; uncommitted changes discarded
  Recover();
  r_->RecoverFrom(restart_point, tm_.get());

  // Assert -- aborted txn leaves row 0 with original message, row count = 1
  ASSERT_EQ(GetRowCount(), 1);
  EXPECT_EQ(ReadRow(slot), "original message");
}

TEST_F(CheckpointTest, PeriodicCheckpointRuns) {
  // Arrange -- fixture creates a CheckpointManager with a 1-second interval
  // Act -- start the background worker and wait for its first checkpoint
  cm_->Start();
  for (int i = 0; i < 400 && !std::filesystem::exists(master_record_name_);
       ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  // Assert -- the periodic worker called WriteCheckpoint, writing the master
  // record file
  EXPECT_TRUE(std::filesystem::exists(master_record_name_));
}

TEST_F(CheckpointTest, WorkerStaysParkedUntilStart) {
  // Arrange -- a fresh CheckpointManager whose background worker was never
  // started. The earlier implementation ran periodic checkpoints from the
  // constructor alone, so a manager built for explicit WriteCheckpoint calls
  // raced its owner's teardown (observed as a use-after-free when a fixture
  // swapped the logger underneath the running worker).
  InsertRow("explicit checkpoint only");
  // Act -- leave the worker parked and give a 1-second-interval worker ample
  // time to (wrongly) run several periodic passes.
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  // Assert -- no master record exists: no background checkpoint ever ran.
  EXPECT_FALSE(std::filesystem::exists(master_record_name_));
}

TEST_F(CheckpointTest, ZeroIntervalDisablesPeriodicCheckpointing) {
  // Arrange -- interval 0 means "explicit WriteCheckpoint only"; even an
  // explicit Start() must not arm the worker (interval 0 previously meant a
  // zero-delay busy loop hammering WriteCheckpoint as fast as it could).
  InsertRow("zero interval");
  std::unique_ptr<CheckpointManager> disabled =
      std::make_unique<CheckpointManager>(master_record_name_ + ".zero",
                                          tm_.get(), p_->GetPool(), 0);
  // Act -- start it and give the old busy loop plenty of iterations.
  disabled->Start();
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  // Assert -- no master record was ever written by the disabled worker.
  EXPECT_FALSE(std::filesystem::exists(master_record_name_ + ".zero"));
  disabled->Stop();
  std::filesystem::remove(master_record_name_ + ".zero");
}

TEST_F(CheckpointTest, LoserBelowCheckpointIsUndoneGlobally) {
  // Arrange -- commit a baseline row, then apply an uncommitted update whose
  // log record lands BEFORE the upcoming checkpoint LSN. Flush the dirty page
  // and empty the buffer pool before checkpointing, so the EndCheckpoint DPT
  // does not list the page: only a global loser-chain UNDO can revert it.
  ASSERT_TRUE(InsertRow("original message"));
  Transaction loser = tm_->Begin();
  {
    PageRef page = p_->GetPage(page_id_).MoveValue();
    ASSERT_SUCCESS(page->Update(loser, 0, "loser residue"));
    page.PageUnlock();
  }
  while (l_->CommittedLSN() < l_->BufferedLSN()) {
    std::this_thread::yield();
  }
  p_->GetPool()->FlushPageForTest(page_id_);
  p_->GetPool()->DropAllPages();
  const lsn_t restart_point = cm_->WriteCheckpoint().Value();

  // Act -- crash-reopen and recover from the checkpoint LSN.
  Recover();
  r_->RecoverFrom(restart_point, tm_.get());

  // Assert -- the loser update below the scan start was compensated.
  ASSERT_EQ(GetRowCount(), 1);
  EXPECT_EQ(ReadRow(0), "original message");
}

TEST_F(CheckpointTest, CheckpointDptUsesRecordStartForUnflushedMutation) {
  ASSERT_TRUE(InsertRow("before"));
  ASSERT_SUCCESS(p_->GetPool()->FlushPageForTest(page_id_));
  lsn_t mutation_start = 0;
  lsn_t mutation_end = 0;
  {
    Transaction txn = tm_->Begin();
    PageRef page = p_->GetPage(page_id_).MoveValue();
    ASSERT_SUCCESS(page->Update(txn, 0, "after"));
    mutation_start = txn.PrevLSN();
    mutation_end = txn.PrevRecordEndLSN();
    ASSERT_EQ(page->RecoveryLSN(), mutation_start);
    ASSERT_EQ(page->PageLSN(), mutation_end);
    ASSERT_SUCCESS(txn.PreCommit());
  }
  cm_->SetFlushBudgetMsForTest(0);

  const lsn_t checkpoint = cm_->WriteCheckpoint().Value();
  LogRecord begin;
  ASSERT_TRUE(r_->ReadLog(checkpoint, &begin));
  ASSERT_EQ(begin.type, LogType::kBeginCheckpoint);
  LogRecord end;
  ASSERT_TRUE(r_->ReadLog(checkpoint + begin.Size(), &end));
  ASSERT_EQ(end.type, LogType::kEndCheckpoint);
  const auto dpt = std::ranges::find_if(
      end.dirty_page_table,
      [this](const auto& entry) { return entry.first == page_id_; });
  ASSERT_NE(dpt, end.dirty_page_table.end());
  EXPECT_EQ(dpt->second, mutation_start);

  Recover();
  ASSERT_SUCCESS(r_->RecoverFrom(checkpoint, tm_.get()));
  EXPECT_EQ(ReadRow(0), "after");
}

TEST_F(CheckpointTest, SyncFailureDoesNotPublishMasterOrRecycle) {
  ASSERT_TRUE(InsertRow("before sync failure"));
  ASSERT_SUCCESS(cm_->WriteCheckpoint().GetStatus());
  std::ifstream before(master_record_name_, std::ios::binary);
  ASSERT_TRUE(before.good());
  const std::string master_before((std::istreambuf_iterator<char>(before)),
                                  std::istreambuf_iterator<char>());
  before.close();
  const lsn_t retained_before = l_->LowestRetainedLSN();

  p_->GetPool()->SetSyncDataFileFaultForTest(true);
  const auto failed = cm_->WriteCheckpoint();
  p_->GetPool()->SetSyncDataFileFaultForTest(false);

  EXPECT_EQ(failed.GetStatus(), Status::kIOError);
  EXPECT_EQ(l_->LowestRetainedLSN(), retained_before);
  std::ifstream after(master_record_name_, std::ios::binary);
  ASSERT_TRUE(after.good());
  const std::string master_after((std::istreambuf_iterator<char>(after)),
                                 std::istreambuf_iterator<char>());
  EXPECT_EQ(master_after, master_before);
}

TEST_F(CheckpointTest, AllocatorMetaRepairIsFlushedWithZeroBudget) {
  ASSERT_TRUE(InsertRow("allocator repair"));
  page_id_t first_free = 0;
  page_id_t max_page_count = 0;
  {
    PageRef meta = p_->GetPage(0).MoveValue();
    ASSERT_FALSE(meta.IsNull());
    first_free = meta->body.meta_page.FirstFreePage();
    max_page_count = meta->body.meta_page.MaxPageCount();
    meta->PageInit(0, PageType::kMetaPage);
    meta->body.meta_page.RebuildFreeListHead(first_free);
    meta->body.meta_page.RestoreMaxPageCount(max_page_count);
    ASSERT_FALSE(meta->ChecksumMatches());
  }

  cm_->SetFlushBudgetMsForTest(0);
  ASSERT_SUCCESS(cm_->WriteCheckpoint().GetStatus());

  PageRef meta = p_->GetPage(0).MoveValue();
  ASSERT_FALSE(meta.IsNull());
  EXPECT_TRUE(meta->ChecksumMatches());
  EXPECT_EQ(meta->RecoveryLSN(), std::numeric_limits<lsn_t>::max());
}

TEST_F(CheckpointTest, MetaFlushFailureDoesNotPublishMasterOrRecycle) {
  ASSERT_TRUE(InsertRow("allocator failure"));
  ASSERT_SUCCESS(cm_->WriteCheckpoint().GetStatus());
  std::ifstream before(master_record_name_, std::ios::binary);
  ASSERT_TRUE(before.good());
  const std::string master_before((std::istreambuf_iterator<char>(before)),
                                  std::istreambuf_iterator<char>());
  before.close();
  const lsn_t retained_before = l_->LowestRetainedLSN();
  {
    PageRef meta = p_->GetPage(0).MoveValue();
    ASSERT_FALSE(meta.IsNull());
    const page_id_t first_free = meta->body.meta_page.FirstFreePage();
    const page_id_t max_page_count = meta->body.meta_page.MaxPageCount();
    meta->PageInit(0, PageType::kMetaPage);
    meta->body.meta_page.RebuildFreeListHead(first_free);
    meta->body.meta_page.RestoreMaxPageCount(max_page_count);
  }

  p_->GetPool()->SetDurabilityGate([](lsn_t) {
    return StatusError(StatusCode::kIOError,
                       "injected meta write-back failure");
  });
  cm_->SetFlushBudgetMsForTest(0);
  const auto failed = cm_->WriteCheckpoint();
  p_->GetPool()->SetDurabilityGate({});

  EXPECT_EQ(failed.GetStatus(), Status::kIOError);
  EXPECT_EQ(l_->LowestRetainedLSN(), retained_before);
  std::ifstream after(master_record_name_, std::ios::binary);
  ASSERT_TRUE(after.good());
  const std::string master_after((std::istreambuf_iterator<char>(after)),
                                 std::istreambuf_iterator<char>());
  EXPECT_EQ(master_after, master_before);
}

TEST(CheckpointAllocatorHighWaterTest, PersistsMetaBeforeRecycling) {
  const std::string prefix =
      "checkpoint_allocator_high_water-" + RandomString();
  const std::string db_name = prefix + ".db";
  const std::string log_name = prefix + ".log";
  const std::string master_name = prefix + ".master";
  constexpr size_t kSegmentSize = static_cast<size_t>(4) * 1024;
  constexpr size_t kBufferSize = 1U << 20;

  auto pool = PageManager::Create(db_name, 2, false).MoveValue();
  ASSERT_NE(pool, nullptr);
  auto logger =
      Logger::Create(log_name, kBufferSize, 1, kSegmentSize).MoveValue();
  ASSERT_NE(logger, nullptr);
  auto recovery = std::make_unique<RecoveryManager>(log_name, pool->GetPool(),
                                                    kSegmentSize);
  auto transactions = std::make_unique<TransactionManager>(
      pool.get(), logger.get(), recovery.get());
  logger->SetRecycleBoundaryValidator([&](lsn_t lsn) {
    LogRecord probe;
    return recovery->ReadLog(lsn, &probe);
  });
  auto checkpoints = std::make_unique<CheckpointManager>(
      master_name, transactions.get(), pool->GetPool(), 0);
  checkpoints->SetFlushBudgetMsForTest(1000);

  page_id_t lower_id = 0;
  page_id_t protected_id = 0;
  {
    Transaction txn = transactions->Begin();
    PageRef page = pool->AllocateNewPage(txn, PageType::kRowPage).MoveValue();
    lower_id = page->PageID();
    ASSERT_FALSE(page.IsNull());
    ASSERT_SUCCESS(page->Insert(txn, "lower").GetStatus());
    page.PageUnlock();
    ASSERT_SUCCESS(txn.PreCommit());
  }
  for (int i = 0; i < 64; ++i) {
    ASSERT_FALSE(pool->GetPage(0).MoveValue().IsNull());
  }
  {
    Transaction txn = transactions->Begin();
    PageRef page = pool->AllocateNewPage(txn, PageType::kRowPage).MoveValue();
    protected_id = page->PageID();
    ASSERT_FALSE(page.IsNull());
    ASSERT_GT(protected_id, lower_id);
    ASSERT_SUCCESS(page->Insert(txn, "protected").GetStatus());
    page.PageUnlock();
    ASSERT_SUCCESS(txn.PreCommit());
  }
  for (int i = 0; i < 64; ++i) {
    ASSERT_FALSE(pool->GetPage(protected_id).MoveValue().IsNull());
  }
  for (int i = 0; i < 64; ++i) {
    ASSERT_FALSE(pool->GetPage(0).MoveValue().IsNull());
  }
  bool lower_cache_hit = false;
  {
    PageRef lower =
        pool->GetPool()->GetPage(lower_id, &lower_cache_hit).MoveValue();
    ASSERT_FALSE(lower_cache_hit);
    ASSERT_FALSE(lower.IsNull());
  }
  {
    PageRef meta = pool->GetPage(0).MoveValue();
    ASSERT_FALSE(meta.IsNull());
    ASSERT_NE(meta->RecoveryLSN(), std::numeric_limits<lsn_t>::max());
  }

  const std::string filler = LogRecord::BeginCheckpointLogRecord().Serialize();
  while (logger->BufferedLSN() <= static_cast<lsn_t>(kSegmentSize)) {
    ASSERT_SUCCESS(logger->AddLog(filler).GetStatus());
  }
  ASSERT_SUCCESS(logger->WaitForDurable(logger->BufferedLSN()));

  const StatusOr<lsn_t> checkpoint_result = checkpoints->WriteCheckpoint();
  ASSERT_SUCCESS(checkpoint_result.GetStatus());
  const lsn_t checkpoint = checkpoint_result.Value();
  ASSERT_GE(logger->LowestRetainedLSN(), static_cast<lsn_t>(kSegmentSize));

  LogRecord begin;
  ASSERT_TRUE(recovery->ReadLog(checkpoint, &begin));
  ASSERT_EQ(begin.type, LogType::kBeginCheckpoint);
  LogRecord end;
  ASSERT_TRUE(recovery->ReadLog(checkpoint + begin.Size(), &end));
  ASSERT_EQ(end.type, LogType::kEndCheckpoint);
  bool protected_in_dpt = false;
  for (const auto& entry : end.dirty_page_table) {
    EXPECT_NE(entry.first, static_cast<page_id_t>(kMetaPageId));
    protected_in_dpt = protected_in_dpt || entry.first == protected_id;
  }
  EXPECT_FALSE(protected_in_dpt);
  {
    PageRef meta = pool->GetPage(0).MoveValue();
    ASSERT_FALSE(meta.IsNull());
    ASSERT_EQ(meta->RecoveryLSN(), std::numeric_limits<lsn_t>::max());
  }

  std::array<char, kCheckpointMasterRecordSize> master_bytes{};
  std::ifstream master_in(master_name, std::ios::binary);
  ASSERT_TRUE(master_in.good());
  master_in.read(master_bytes.data(),
                 static_cast<std::streamsize>(master_bytes.size()));
  ASSERT_EQ(master_in.gcount(),
            static_cast<std::streamsize>(master_bytes.size()));
  lsn_t master_lsn = 0;
  DeserializeU64(master_bytes.data() + (sizeof(uint32_t) * 2), &master_lsn);
  ASSERT_EQ(master_lsn, checkpoint);

  pool->GetPool()->DropAllPages();
  checkpoints.reset();
  transactions.reset();
  recovery.reset();
  ASSERT_SUCCESS(logger->Finish());
  logger.reset();
  pool.reset();

  auto reopened_pool = PageManager::Create(db_name, 2, false).MoveValue();
  ASSERT_NE(reopened_pool, nullptr);
  auto reopened_logger =
      Logger::Create(log_name, kBufferSize, 1, kSegmentSize).MoveValue();
  ASSERT_NE(reopened_logger, nullptr);
  auto reopened_recovery = std::make_unique<RecoveryManager>(
      log_name, reopened_pool->GetPool(), kSegmentSize);
  auto reopened_transactions = std::make_unique<TransactionManager>(
      reopened_pool.get(), reopened_logger.get(), reopened_recovery.get());
  reopened_logger->SetRecycleBoundaryValidator([&](lsn_t lsn) {
    LogRecord probe;
    return reopened_recovery->ReadLog(lsn, &probe);
  });
  ASSERT_SUCCESS(
      reopened_recovery->RecoverFrom(master_lsn, reopened_transactions.get()));

  {
    Transaction read_txn = reopened_transactions->Begin();
    PageRef protected_page = reopened_pool->GetPage(protected_id).MoveValue();
    ASSERT_FALSE(protected_page.IsNull());
    ASSERT_EQ(protected_page->Type(), PageType::kRowPage);
    ASSERT_EQ(protected_page->RowCount(), 1U);
    EXPECT_EQ(protected_page->Read(read_txn, 0).Value(), "protected");
    ASSERT_SUCCESS(read_txn.PreCommit());
  }
  {
    Transaction alloc = reopened_transactions->Begin();
    PageRef fresh =
        reopened_pool->AllocateNewPage(alloc, PageType::kRowPage).MoveValue();
    ASSERT_FALSE(fresh.IsNull());
    ASSERT_GT(fresh->PageID(), protected_id);
    ASSERT_SUCCESS(alloc.PreCommit());
  }
  {
    Transaction verify_txn = reopened_transactions->Begin();
    PageRef protected_page = reopened_pool->GetPage(protected_id).MoveValue();
    ASSERT_FALSE(protected_page.IsNull());
    EXPECT_EQ(protected_page->Read(verify_txn, 0).Value(), "protected");
    ASSERT_SUCCESS(verify_txn.PreCommit());
  }

  reopened_pool->GetPool()->DropAllPages();
  reopened_transactions.reset();
  reopened_recovery.reset();
  ASSERT_SUCCESS(reopened_logger->Finish());
  reopened_logger.reset();
  reopened_pool.reset();

  std::ignore = std::remove(db_name.c_str());
  RemoveLogAndSegments(log_name);
  std::ignore = std::remove(master_name.c_str());
}

TEST(CheckpointSyncFailureTest, FailedSyncLeavesSegmentsAndMaster) {
  const std::string prefix = "checkpoint_sync_fault-" + RandomString();
  const std::string db_name = prefix + ".db";
  const std::string log_name = prefix + ".log";
  const std::string master_name = prefix + ".master";
  constexpr size_t kSegmentSize = 4096;
  auto pool = PageManager::Create(db_name, 32).MoveValue();
  auto logger = Logger::Create(log_name, 1U << 20, 1, kSegmentSize).MoveValue();
  ASSERT_NE(pool, nullptr);
  ASSERT_NE(logger, nullptr);
  RecoveryManager recovery(log_name, pool->GetPool(), kSegmentSize);
  TransactionManager transactions(pool.get(), logger.get(), &recovery);
  logger->SetRecycleBoundaryValidator([&](lsn_t lsn) {
    LogRecord probe;
    return recovery.ReadLog(lsn, &probe);
  });
  CheckpointManager checkpoints(master_name, &transactions, pool->GetPool(), 0);
  const std::string body(3000, 'x');
  for (page_id_t i = 0; i < 24; ++i) {
    ASSERT_SUCCESS(logger
                       ->AddLog(LogRecord::FullPageImageLogRecord(
                                    i, PageType::kRowPage, body)
                                    .Serialize())
                       .GetStatus());
  }
  ASSERT_SUCCESS(logger->WaitForDurable(logger->BufferedLSN()));
  ASSERT_SUCCESS(checkpoints.WriteCheckpoint().GetStatus());
  const lsn_t retained_before = logger->LowestRetainedLSN();
  ASSERT_GT(retained_before, 0U);
  for (page_id_t i = 0; i < 24; ++i) {
    ASSERT_SUCCESS(logger
                       ->AddLog(LogRecord::FullPageImageLogRecord(
                                    i + 100, PageType::kRowPage, body)
                                    .Serialize())
                       .GetStatus());
  }
  ASSERT_SUCCESS(logger->WaitForDurable(logger->BufferedLSN()));
  std::ifstream before(master_name, std::ios::binary);
  ASSERT_TRUE(before.good());
  const std::string master_before((std::istreambuf_iterator<char>(before)),
                                  std::istreambuf_iterator<char>());
  before.close();

  pool->GetPool()->SetSyncDataFileFaultForTest(true);
  const auto failed = checkpoints.WriteCheckpoint();
  pool->GetPool()->SetSyncDataFileFaultForTest(false);
  EXPECT_EQ(failed.GetStatus(), Status::kIOError);
  EXPECT_EQ(logger->LowestRetainedLSN(), retained_before);
  std::ifstream after(master_name, std::ios::binary);
  ASSERT_TRUE(after.good());
  const std::string master_after((std::istreambuf_iterator<char>(after)),
                                 std::istreambuf_iterator<char>());
  EXPECT_EQ(master_after, master_before);

  checkpoints.Stop();
  ASSERT_SUCCESS(logger->Finish());
  std::ignore = std::remove(db_name.c_str());
  RemoveLogAndSegments(log_name);
  std::ignore = std::remove(master_name.c_str());
}
}  // namespace tinylamb
