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
#include <string>
#include <cstdio>
#include <memory>
#include <filesystem>
#include <thread>
#include <chrono>

#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "gtest/gtest.h"
#include "page/page_ref.hpp"
#include "page/page_type.hpp"
#include "page/row_page_test.hpp"
#include "recovery/logger.hpp"
#include "transaction/lock_manager.hpp"
#include "recovery/recovery_manager.hpp"

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
    PageRef page = p_->AllocateNewPage(txn, PageType::kRowPage);
    page_id_ = page->PageID();
    EXPECT_SUCCESS(txn.PreCommit());
  }

  void TearDown() override {
    // Best-effort cleanup; a missing file must not fail the test.
    (void)std::remove(db_name_.c_str());
    (void)std::remove(log_name_.c_str());
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
    p_ = std::make_unique<PageManager>(db_name_, 10);
    l_ = std::make_unique<Logger>(log_name_);
    lm_ = std::make_unique<LockManager>();
    tm_ = std::make_unique<TransactionManager>(p_.get(), l_.get(),
                                               nullptr);
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

TEST_F(CheckpointTest, DoCheckpoint) {
  // Arrange -- nothing more than fixture setup

  // Act 1 -- insert a row, then checkpoint, then update+commit within {} scope
  InsertRow("expect this operation did not rerun");
  Transaction txn = tm_->Begin();
  slot_t slot = 0;
  {
    PageRef page = p_->GetPage(page_id_);
    ASSIGN_OR_ASSERT_FAIL(slot_t, inserted, page->Insert(txn, "inserted"));
    slot = inserted;
  }
  p_->GetPool()->FlushPageForTest(page_id_);
  cm_->WriteCheckpoint();
  {
    PageRef page = p_->GetPage(page_id_);
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
    PageRef page = p_->GetPage(page_id_);
    ASSIGN_OR_ASSERT_FAIL(slot_t, inserted, page->Insert(txn, "inserted"));
    result = inserted;
  }
  restart_point = cm_->WriteCheckpoint();
  {
    PageRef page = p_->GetPage(page_id_);
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
  const lsn_t restart_point = cm_->WriteCheckpoint();
  {
    PageRef page = p_->GetPage(page_id_);
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
  const lsn_t restart_point = cm_->WriteCheckpoint([&]() {
    PageRef page = p_->GetPage(page_id_);
    ASSERT_SUCCESS(page->Update(txn, slot, "aborted"));
    ASSERT_SUCCESS(page->Insert(txn, "will be deleted").GetStatus());
  });

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
  for (int i = 0;
       i < 400 && !std::filesystem::exists(master_record_name_); ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  // Assert -- the periodic worker called WriteCheckpoint, writing the master
  // record file
  EXPECT_TRUE(std::filesystem::exists(master_record_name_));
}

TEST_F(CheckpointTest, LoserBelowCheckpointIsUndoneGlobally) {
  // Arrange -- commit a baseline row, then apply an uncommitted update whose
  // log record lands BEFORE the upcoming checkpoint LSN. Flush the dirty page
  // and empty the buffer pool before checkpointing, so the EndCheckpoint DPT
  // does not list the page: only a global loser-chain UNDO can revert it.
  ASSERT_TRUE(InsertRow("original message"));
  Transaction loser = tm_->Begin();
  {
    PageRef page = p_->GetPage(page_id_);
    ASSERT_SUCCESS(page->Update(loser, 0, "loser residue"));
    page.PageUnlock();
  }
  while (l_->CommittedLSN() < l_->BufferedLSN()) {
    std::this_thread::yield();
  }
  p_->GetPool()->FlushPageForTest(page_id_);
  p_->GetPool()->DropAllPages();
  const lsn_t restart_point = cm_->WriteCheckpoint();

  // Act -- crash-reopen and recover from the checkpoint LSN.
  Recover();
  r_->RecoverFrom(restart_point, tm_.get());

  // Assert -- the loser update below the scan start was compensated.
  ASSERT_EQ(GetRowCount(), 1);
  EXPECT_EQ(ReadRow(0), "original message");
}
}  // namespace tinylamb
