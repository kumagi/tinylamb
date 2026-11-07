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

#include "recovery/recovery_manager.hpp"

#include <cstdio>
#include <memory>
#include <string>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "index/lsm_index.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "page/page_type.hpp"
#include "recovery/log_record.hpp"
#include "recovery/logger.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {
namespace {

TEST(LogRecordTest, RoundTripSerialization) {
  const std::vector<LogRecord> records = {
      LogRecord::BeginLogRecord(0, 10),
      LogRecord::CommitLogRecord(0, 10),
      LogRecord::AbortLogRecord(0, 10),
      LogRecord::BeginCheckpointLogRecord(),
      LogRecord::InsertingLogRecord(0, 10, 5, 2, "hello_world"),
      LogRecord::UpdatingLogRecord(0, 10, 5, 2, "new_val", "old_val"),
      LogRecord::DeletingLogRecord(0, 10, 5, 2, "deleted_val"),
      LogRecord::SetRowPageNextLogRecord(0, 10, 5, 6, 0),
      LogRecord::SetRowPagePrevLogRecord(0, 10, 5, 4, 0),
      LogRecord::AllocatePageLogRecord(0, 10, 7, PageType::kRowPage),
      LogRecord::DestroyPageLogRecord(0, 10, 7),
      LogRecord::LsmPutLogRecord(0, 10, 3, "lsm_key", "lsm_val", "old", true),
      LogRecord::LsmDeleteLogRecord(0, 10, 3, "lsm_key", "old"),
      LogRecord::LsmDestroyLogRecord(0, 10, 3),
  };

  for (const LogRecord& orig : records) {
    const std::string bytes = orig.Serialize();
    LogRecord decoded;
    ASSERT_GT(decoded.Deserialize(bytes.data()), 0U);
    EXPECT_EQ(decoded, orig);
  }
}

class RecoveryManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    prefix_ = "recovery_manager_test-" + RandomString();
    Open();
  }

  void Open() {
    tm_.reset();
    rm_.reset();
    logger_.reset();
    pm_.reset();
    pm_ = PageManager::Create(prefix_ + ".db", 64, false).MoveValue();
    logger_ = Logger::Create(prefix_ + ".log").MoveValue();
    rm_ = std::make_unique<RecoveryManager>(prefix_ + ".log", pm_->GetPool(),
                                            logger_->SegmentSize());
    rm_->SetLogger(logger_.get());
    tm_ = std::make_unique<TransactionManager>(pm_.get(), logger_.get(),
                                               rm_.get());
    ASSERT_SUCCESS(rm_->RecoverFrom(0, tm_.get()));
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

TEST_F(RecoveryManagerTest, ReplaysCommittedAndIgnoresUncommittedLosers) {
  page_id_t block_pid = 0;
  page_id_t lsm_root = 0;
  slot_t committed_slot = 0;

  {
    Transaction txn = tm_->Begin();
    ASSIGN_OR_ASSERT_FAIL(PageRef, page,
                          pm_->AllocateNewPage(txn, PageType::kRowPage));
    block_pid = page->PageID();
    ASSIGN_OR_ASSERT_FAIL(slot_t, s, page->Insert(txn, "committed_row"));
    committed_slot = s;
    page.PageUnlock();

    ASSIGN_OR_ASSERT_FAIL(LsmIndex, idx, LsmIndex::Open(txn));
    lsm_root = idx.Root();
    ASSERT_SUCCESS(idx.Insert(txn, "k_committed", "v_committed"));
    ASSERT_SUCCESS(txn.PreCommit());
  }

  // Start a loser transaction that writes to WAL and modifies in-memory state,
  // then simulate a sudden crash without aborting or committing!
  {
    Transaction loser = tm_->Begin();
    ASSIGN_OR_ASSERT_FAIL(PageRef, page, pm_->GetPage(block_pid));
    ASSERT_SUCCESS(page->Update(loser, committed_slot, "loser_overwrite"));
    page.PageUnlock();
    LsmIndex idx(lsm_root);
    ASSERT_SUCCESS(idx.Insert(loser, "k_loser", "v_loser"));
    // Flush the loser's WAL records to disk before crashing.
    ASSERT_SUCCESS(logger_->WaitForDurable(logger_->BufferedLSN()));
    pm_->DropAllPages();
    // Detach loser without running runtime Abort: simulates process death.
    loser = Transaction();
  }

  // Reopen and run commit-filtered fuzzy recovery.
  Open();

  Transaction verify = tm_->Begin();
  ASSIGN_OR_ASSERT_FAIL(PageRef, page, pm_->GetPage(block_pid));
  ASSIGN_OR_ASSERT_FAIL(std::string_view, row,
                        page->Read(verify, committed_slot));
  EXPECT_EQ(row, "committed_row");
  page.PageUnlock();

  LsmIndex idx(lsm_root);
  ASSIGN_OR_ASSERT_FAIL(std::string, val, idx.Read(verify, "k_committed"));
  EXPECT_EQ(val, "v_committed");
  EXPECT_EQ(idx.Read(verify, "k_loser").GetStatus(), Status::kNotExists);
  ASSERT_SUCCESS(verify.PreCommit());
}

}  // namespace
}  // namespace tinylamb
