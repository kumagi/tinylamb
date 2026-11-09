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

#include <cstdio>
#include <memory>
#include <string>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "page/page_ref.hpp"
#include "page/page_type.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

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

}  // namespace
}  // namespace tinylamb
