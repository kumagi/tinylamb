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
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

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
    std::error_code ec;
    for (const auto& entry : std::filesystem::directory_iterator(".", ec)) {
      // T4188: a failed enumeration or dereference must not silently leave
      // this test's .db.blocks.*/.db.lsm.* debris behind (AGENTS.md §3
      // throwaway-artifact contract); surface it loudly instead.
      if (ec) {
        ADD_FAILURE() << "TearDown directory scan failed: " << ec.message();
        break;
      }
      const std::string fname = entry.path().filename().string();
      if (fname.starts_with(prefix_ + ".db.blocks.") ||
          fname.starts_with(prefix_ + ".db.lsm.")) {
        std::filesystem::remove(entry.path(), ec);
        if (ec) {
          ADD_FAILURE() << "TearDown could not remove " << fname << ": "
                        << ec.message();
        }
      }
    }
    if (ec) {
      ADD_FAILURE() << "TearDown directory iteration error: " << ec.message();
    }
    RemoveLogAndSegments(prefix_ + ".log");
  }

  std::string prefix_;
  std::unique_ptr<PageManager> pm_;
  std::unique_ptr<Logger> logger_;
  std::unique_ptr<RecoveryManager> rm_;
  std::unique_ptr<TransactionManager> tm_;
};

// P642: `LogEnd()` folds a scan/IO failure into 0.  Callers that tell
// "WAL is empty" apart from "WAL is unreadable" must use `LogEndStatus()`
// so a transient I/O failure is not misdiagnosed as kCorrupt.
TEST(RecoveryManagerLogEndStatus, PropagatesIoFailureWhereLogEndFoldsToZero) {
  const std::string dir = "recovery_logend_" + RandomString();
  const std::string missing = dir + "/nested/nope.log";
  {
    RecoveryManager rm(missing, nullptr);
    const StatusOr<lsn_t> end = rm.LogEndStatus();
    ASSERT_FALSE(end.HasValue());
    EXPECT_EQ(end.GetStatus().GetCode(), StatusCode::kIOError);
    EXPECT_EQ(rm.LogEnd(), 0U);
  }
  std::filesystem::create_directories(dir);
  {
    // Present but empty WAL directory: the scan succeeds and positively
    // reports "no segments" (0), which is the only shape the kCorrupt
    // misdiagnosis guard must accept.
    const std::string empty = dir + "/empty.log";
    RecoveryManager rm(empty, nullptr);
    ASSERT_SUCCESS(rm.LogEndStatus().GetStatus());
    EXPECT_EQ(rm.LogEndStatus().Value(), 0U);
  }
  {
    // A readable unsuffixed segment reports its logical size, not 0.
    const std::string base = dir + "/data.log";
    {
      std::ofstream out(base);
      out << std::string(1234, 'x');
    }
    RecoveryManager rm(base, nullptr);
    ASSIGN_OR_ASSERT_FAIL(lsn_t, end, rm.LogEndStatus());
    EXPECT_EQ(end, 1234U);
  }
  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
}

TEST_F(RecoveryManagerTest, RejectsRecordsLargerThanTheLoggerLimit) {
  // Serialize records directly: AddLog already refuses the second one.
  const size_t overhead = LogRecord::InsertingLogRecord(0, 10, 5, 2, "").Size();
  const size_t max_payload = Logger::kMaxRecordSize - overhead;
  const auto record = [&](size_t size) {
    return LogRecord::InsertingLogRecord(0, 10, 5, 2, std::string(size, 'x'))
        .Serialize();
  };
  const auto path = prefix_ + "-size-limit.log";
  {
    std::ofstream log(path, std::ios::binary);
    log << record(max_payload) << record(max_payload + 1);
    ASSERT_TRUE(log.good());
  }

  // Keep both records inside a single segment; only their per-record length
  // distinguishes them, and both have a valid checksum.
  {
    RecoveryManager reader(path, pm_->GetPool(), size_t{64} << 20);
    LogRecord recovered;
    EXPECT_TRUE(reader.ReadLog(0, &recovered));
    EXPECT_EQ(recovered.Size(), Logger::kMaxRecordSize);
    EXPECT_FALSE(reader.ReadLog(Logger::kMaxRecordSize, &recovered));
  }
  std::filesystem::remove(path);
}

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

TEST_F(RecoveryManagerTest,
       RecoveryTxnBitmapHandlesSpacedAndAbortedTransactions) {
  std::vector<page_id_t> committed_pids;
  std::vector<slot_t> committed_slots;
  std::vector<page_id_t> aborted_pids;

  for (int i = 0; i < 64; ++i) {
    Transaction txn = tm_->Begin();
    ASSIGN_OR_ASSERT_FAIL(PageRef, p,
                          pm_->AllocateNewPage(txn, PageType::kRowPage));
    const page_id_t pid = p->PageID();
    if (i % 4 == 0) {
      aborted_pids.push_back(pid);
      p.PageUnlock();
      ASSERT_SUCCESS(txn.Abort());
      continue;
    }
    ASSIGN_OR_ASSERT_FAIL(slot_t, slot,
                          p->Insert(txn, "txn_val_" + std::to_string(i)));
    committed_pids.push_back(pid);
    committed_slots.push_back(slot);
    p.PageUnlock();
    ASSERT_SUCCESS(txn.PreCommit());
    if (i == 31) {
      ASSERT_SUCCESS(pm_->WriteFuzzyCheckpoint(logger_->BufferedLSN()));
    }
  }

  ASSERT_SUCCESS(logger_->WaitForDurable(logger_->BufferedLSN()));
  pm_->DropAllPages();
  Open();

  Transaction verify = tm_->Begin();
  for (size_t i = 0; i < committed_pids.size(); ++i) {
    ASSIGN_OR_ASSERT_FAIL(PageRef, p, pm_->GetPage(committed_pids[i]));
    ASSIGN_OR_ASSERT_FAIL(std::string_view, got,
                          p->Read(verify, committed_slots[i]));
    EXPECT_FALSE(got.empty());
    p.PageUnlock();
  }
  ASSERT_SUCCESS(verify.PreCommit());
}

// T2115: the LogRecord stream form rendered `type` as a raw uint8 and
// printed the pid/slot numeric_limits::max() sentinels for record kinds
// that never touch them (BEGIN/COMMIT/ABORT/checkpoints).  Names now,
// and location fields only when real.
TEST(LogRecordDumpTest, RendersTypeNameAndSuppressesSentinelLocation) {
  std::ostringstream oss;
  oss << LogRecord(1, 2, LogType::kBegin);
  EXPECT_EQ(oss.str(), "{prev:1 txn:2 type:kBegin}");

  LogRecord insert(3, 4, LogType::kInsertRow);
  insert.pid = 7;
  insert.slot = 2;
  oss.str("");
  oss << insert;
  EXPECT_EQ(oss.str(), "{prev:3 txn:4 type:kInsertRow pid:7 slot:2}");

  oss.str("");
  oss << LogRecord(5, 6, static_cast<LogType>(200));  // NOLINT(clang-analyzer-optin.core.EnumCastOutOfRange): exercise unknown log type
  EXPECT_EQ(oss.str(), "{prev:5 txn:6 type:kInvalid}");
}

}  // namespace
}  // namespace tinylamb
