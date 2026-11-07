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

#include "recovery/checkpoint_manager.hpp"

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
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {
namespace {

class CheckpointManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    prefix_ = "checkpoint_manager_test-" + RandomString();
    Open(0);
  }

  void Open(lsn_t checkpoint_lsn) {
    cm_.reset();
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
    cm_ = std::make_unique<CheckpointManager>(prefix_ + ".last_checkpoint",
                                              tm_.get(), pm_->GetPool(), 0);
    ASSERT_SUCCESS(rm_->RecoverFrom(checkpoint_lsn, tm_.get()));
  }

  void TearDown() override {
    cm_.reset();
    tm_.reset();
    rm_.reset();
    logger_.reset();
    pm_.reset();
    std::ignore = std::remove((prefix_ + ".db").c_str());
    std::ignore = std::remove((prefix_ + ".db.ckpt").c_str());
    std::ignore = std::remove((prefix_ + ".db.ckpt.tmp").c_str());
    RemoveLogAndSegments(prefix_ + ".log");
    std::ignore = std::remove((prefix_ + ".last_checkpoint").c_str());
    std::ignore = std::remove((prefix_ + ".last_checkpoint.tmp").c_str());
  }

  std::string prefix_;
  std::unique_ptr<PageManager> pm_;
  std::unique_ptr<Logger> logger_;
  std::unique_ptr<RecoveryManager> rm_;
  std::unique_ptr<TransactionManager> tm_;
  std::unique_ptr<CheckpointManager> cm_;
};

TEST_F(CheckpointManagerTest,
       FuzzyCheckpointWithInFlightAndPostCheckpointTxns) {
  page_id_t pid = 0;
  page_id_t lsm_root = 0;
  slot_t s0 = 0;

  // 1. Pre-checkpoint committed transaction.
  {
    Transaction t1 = tm_->Begin();
    ASSIGN_OR_ASSERT_FAIL(PageRef, page,
                          pm_->AllocateNewPage(t1, PageType::kRowPage));
    pid = page->PageID();
    ASSIGN_OR_ASSERT_FAIL(slot_t, slot, page->Insert(t1, "before_ckpt"));
    s0 = slot;
    page.PageUnlock();

    ASSIGN_OR_ASSERT_FAIL(LsmIndex, idx, LsmIndex::Open(t1));
    lsm_root = idx.Root();
    ASSERT_SUCCESS(idx.Insert(t1, "k1", "v1"));
    ASSERT_SUCCESS(t1.PreCommit());
  }

  // 2. In-flight transaction that starts before the fuzzy checkpoint and
  // commits AFTER the fuzzy checkpoint.
  Transaction in_flight_winner = tm_->Begin();
  {
    LsmIndex idx(lsm_root);
    ASSERT_SUCCESS(idx.Insert(in_flight_winner, "k_inflight_win", "v_win"));
  }

  // 3. In-flight loser that starts before the fuzzy checkpoint and never
  // commits.
  Transaction in_flight_loser = tm_->Begin();
  {
    LsmIndex idx(lsm_root);
    ASSERT_SUCCESS(idx.Insert(in_flight_loser, "k_inflight_lose", "v_lose"));
  }

  // 4. Write lock-free fuzzy checkpoint while both transactions are active!
  ASSIGN_OR_ASSERT_FAIL(lsn_t, ckpt_lsn, cm_->WriteCheckpoint());
  EXPECT_GT(ckpt_lsn, 0U);

  // 5. Commit in_flight_winner, leave in_flight_loser uncommitted, and also
  // commit a post-checkpoint transaction.
  ASSERT_SUCCESS(in_flight_winner.PreCommit());
  {
    Transaction post = tm_->Begin();
    ASSIGN_OR_ASSERT_FAIL(PageRef, page, pm_->GetPage(pid));
    ASSERT_SUCCESS(page->Update(post, s0, "after_ckpt"));
    page.PageUnlock();
    LsmIndex idx(lsm_root);
    ASSERT_SUCCESS(idx.Insert(post, "k_post", "v_post"));
    ASSERT_SUCCESS(post.PreCommit());
  }

  // Simulate crash.
  ASSERT_SUCCESS(logger_->WaitForDurable(logger_->BufferedLSN()));
  pm_->DropAllPages();
  in_flight_loser = Transaction();

  // Recover from the fuzzy checkpoint!
  Open(ckpt_lsn);

  Transaction check = tm_->Begin();
  ASSIGN_OR_ASSERT_FAIL(PageRef, page, pm_->GetPage(pid));
  ASSIGN_OR_ASSERT_FAIL(std::string_view, row, page->Read(check, s0));
  EXPECT_EQ(row, "after_ckpt");
  page.PageUnlock();

  LsmIndex idx(lsm_root);
  EXPECT_EQ(idx.Read(check, "k1").MoveValue(), "v1");
  EXPECT_EQ(idx.Read(check, "k_inflight_win").MoveValue(), "v_win");
  EXPECT_EQ(idx.Read(check, "k_post").MoveValue(), "v_post");
  EXPECT_EQ(idx.Read(check, "k_inflight_lose").GetStatus(), Status::kNotExists);
  ASSERT_SUCCESS(check.PreCommit());
}

}  // namespace
}  // namespace tinylamb
