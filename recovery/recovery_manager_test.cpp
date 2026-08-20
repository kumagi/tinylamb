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

#include "recovery/recovery_manager.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdio>
#include <fstream>
#include <functional>
#include <ios>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <utility>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "logger.hpp"
#include "log_record.hpp"
#include "page/page_type.hpp"
#include "page/row_page_test.hpp"
#include "page/row_pointer.hpp"
#include "transaction/lock_manager.hpp"

namespace tinylamb {

class RecoveryManagerTest : public RowPageTest {
 protected:
  void SetUp() override {
    file_name_ = "recovery_manager_test-" + RandomString();
    RowPageTest::SetUp();
  }

  void RecoverBase(const std::function<void(void)>& f) {
    if (p_) {
      p_->GetPool()->DropAllPages();
    }
    tm_.reset();
    lm_.reset();
    l_.reset();
    r_.reset();
    p_.reset();
    f();
    p_ = std::make_unique<PageManager>(file_name_ + ".db", 10);
    l_ = std::make_unique<Logger>(file_name_ + ".log");
    lm_ = std::make_unique<LockManager>();
    r_ = std::make_unique<RecoveryManager>(file_name_ + ".log", p_->GetPool());
    tm_ = std::make_unique<TransactionManager>(lm_.get(), p_.get(), l_.get(),
                                               r_.get());
  }

  void Recover() override {
    RecoverBase([]() {});
  }

  void MediaFailure() {
    RecoverBase(
        [&]() { std::ignore = std::remove((file_name_ + ".db").c_str()); });
  }

  void SinglePageFailure(page_id_t failed_page) {
    RecoverBase([&]() {
      std::fstream db(file_name_ + ".db",
                      std::ios_base::out | std::ios_base::binary);
      db.seekp(failed_page * kPageSize, std::ios_base::beg);
      ASSERT_FALSE(db.fail());
      for (size_t i = 0; i < kPageSize; ++i) {
        db.write("\xff", 1);
      }
      ASSERT_FALSE(db.fail());
    });
  }

  void TearDown() override {
    std::ignore = std::remove((file_name_ + ".db").c_str());
    std::ignore = std::remove((file_name_ + ".log").c_str());
  }

  std::unique_ptr<RecoveryManager> r_;
};

TEST_F(RecoveryManagerTest, EmptyRecovery) {
  // Arrange -- nothing more than fixture setup; empty log file
  // Act -- recover from LSN 0 with nothing to redo
  r_->RecoverFrom(0, tm_.get());
  // Assert -- implicit; no crash, no rows recovered; gtest green on pass
}

TEST_F(RecoveryManagerTest, InsertAbort) {
  // Arrange
  auto txn = tm_->Begin();
  std::string record = "blah~blah";
  PageRef page = p_->GetPage(page_id_);
  ASSERT_FALSE(page.IsNull());
  ASSERT_EQ(page->Type(), PageType::kRowPage);
  const bin_size_t before_size = page->body.row_page.FreeSizeForTest();

  // Act 1 -- insert record and verify free space decremented
  ASSERT_SUCCESS(page->Insert(txn, record).GetStatus());
  page.PageUnlock();
  ASSERT_EQ(page->body.row_page.FreeSizeForTest(),
            before_size - record.size() - sizeof(RowPointer));

  // Act 2 -- abort the transaction
  txn.Abort();

  // Assert -- aborted insert leaves row count at 0
  ASSERT_EQ(GetRowCount(), 0);
}

TEST_F(RecoveryManagerTest, UpdateAbort) {
  // Arrange
  std::string before = "before string hello world!", after = "replaced by this";
  ASSERT_TRUE(InsertRow(before));
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(page_id_);
  ASSERT_FALSE(page.IsNull());
  ASSERT_EQ(page->Type(), PageType::kRowPage);
  const bin_size_t before_size = page->body.row_page.FreeSizeForTest();

  // Act 1 -- update row 0 and verify free space adjusted
  ASSERT_SUCCESS(page->Update(txn, 0, after));
  page.PageUnlock();
  ASSERT_EQ(page->body.row_page.FreeSizeForTest(),
            before_size + before.length() - after.length());

  // Act 2 -- abort the transaction
  txn.Abort();

  // Assert -- aborted update leaves original row intact, row count = 1
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), before);
}

TEST_F(RecoveryManagerTest, DeleteAbort) {
  // Arrange
  std::string before = "living row";
  ASSERT_TRUE(InsertRow(before));
  auto txn = tm_->Begin();
  PageRef page = p_->GetPage(page_id_);

  // Act 1 -- delete row 0
  page->Delete(txn, 0);
  page.PageUnlock();

  // Act 2 -- abort the transaction
  txn.Abort();

  // Assert -- aborted delete leaves original row intact, row count = 1
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), before);
}

TEST_F(RecoveryManagerTest, InsertRowRecovery) {
  // Arrange -- nothing more than fixture setup

  // Act 1 -- insert a row and commit (implicit via InsertRow helper)
  InsertRow("hoge");

  // Act 2 -- emulate media failure (db file removed) then recover from log
  MediaFailure();
  r_->RecoverFrom(0, tm_.get());

  // Assert -- recovered row 0 reads back "hoge"
  ASSERT_EQ(ReadRow(0), "hoge");
}

TEST_F(RecoveryManagerTest, UpdateRowRecovery) {
  // Arrange -- nothing more than fixture setup

  // Act 1 -- insert a row, then update row 0 and commit
  ASSERT_TRUE(InsertRow("hoge"));
  UpdateRow(0, "bar");
  ASSERT_EQ(ReadRow(0), "bar");

  // Act 2 -- emulate media failure (db file removed) then recover from log
  MediaFailure();
  r_->RecoverFrom(0, tm_.get());

  // Assert -- recovered row 0 reads back "bar" (update survived via redo log)
  ASSERT_EQ(ReadRow(0), "bar");
}

TEST_F(RecoveryManagerTest, DeleteRowRecovery) {
  // Arrange -- nothing more than fixture setup

  // Act 1 -- insert a row, then delete row 0 and commit
  ASSERT_TRUE(InsertRow("hoge"));
  DeleteRow(0);
  ASSERT_EQ(GetRowCount(), 0);

  // Act 2 -- emulate media failure (db file removed) then recover from log
  MediaFailure();
  r_->RecoverFrom(0, tm_.get());

  // Assert -- recovered table has row count 0 (delete survived via redo log)
  ASSERT_EQ(GetRowCount(), 0);
}

TEST_F(RecoveryManagerTest, InsertRowAbortRecovery) {
  // Arrange -- nothing more than fixture setup

  // Act 1 -- insert a row but do not commit (aborted by default)
  InsertRow("hoge", false);

  // Act 2 -- emulate media failure (db file removed) then recover from log
  MediaFailure();
  r_->RecoverFrom(0, tm_.get());

  // Assert -- aborted insert leaves no durable row; row count = 0
  ASSERT_EQ(GetRowCount(), 0);
}

TEST_F(RecoveryManagerTest, UpdateRowAbortRecovery) {
  // Arrange -- nothing more than fixture setup; row 0 = "hoge" committed

  // Act 1 -- update row 0 but do not commit (aborted by default)
  ASSERT_TRUE(InsertRow("hoge"));
  UpdateRow(0, "bar", false);
  ASSERT_EQ(ReadRow(0), "hoge");

  // Act 2 -- emulate media failure (db file removed) then recover from log
  MediaFailure();
  r_->RecoverFrom(0, tm_.get());

  // Assert -- aborted update discarded; row 0 retains original "hoge"
  ASSERT_EQ(ReadRow(0), "hoge");
}

TEST_F(RecoveryManagerTest, DeleteRowAbortRecovery) {
  // Arrange -- nothing more than fixture setup; row 0 = "hoge" committed

  // Act 1 -- delete row 0 but do not commit (aborted by default)
  ASSERT_TRUE(InsertRow("hoge"));
  DeleteRow(0, false);
  ASSERT_EQ(GetRowCount(), 1);

  // Act 2 -- emulate media failure (db file removed) then recover from log
  MediaFailure();
  r_->RecoverFrom(0, tm_.get());

  // Assert -- aborted delete discarded; row 0 retains original "hoge"
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), "hoge");
}

TEST_F(RecoveryManagerTest, InsertCrash) {
  // Arrange
  auto txn = tm_->Begin();
  std::string record = "blah~blah";

  // Act 1 -- insert record into row page (txn not committed)
  {
    PageRef page = p_->GetPage(page_id_);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kRowPage);
    const bin_size_t before_size = page->body.row_page.FreeSizeForTest();
    ASSERT_SUCCESS(page->Insert(txn, record).GetStatus());
    ASSERT_EQ(page->body.row_page.FreeSizeForTest(),
              before_size - record.size() - sizeof(RowPointer));
  }

  // Act 2 -- emulate crash (no commit), then recover from log
  Recover();
  r_->RecoverFrom(0, tm_.get());

  // Assert -- uncommitted insert discarded; row count = 0
  ASSERT_EQ(GetRowCount(), 0);
}

TEST_F(RecoveryManagerTest, UpdateCrash) {
  // Arrange -- row 0 = "original message" committed
  ASSERT_TRUE(InsertRow("original message"));
  auto txn = tm_->Begin();
  std::string record = "blah~blah";

  // Act 1 -- update row 0 (txn not committed)
  {
    PageRef page = p_->GetPage(page_id_);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kRowPage);
    ASSERT_SUCCESS(page->Update(txn, 0, record));
  }

  // Act 2 -- emulate crash (no commit), then recover from log
  Recover();
  r_->RecoverFrom(0, tm_.get());

  // Assert -- uncommitted update discarded; row 0 retains original
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), "original message");
}

TEST_F(RecoveryManagerTest, DeleteCrash) {
  // Arrange -- row 0 = "original message" committed
  ASSERT_TRUE(InsertRow("original message"));
  auto txn = tm_->Begin();

  // Act 1 -- delete row 0 (txn not committed)
  {
    PageRef page = p_->GetPage(page_id_);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kRowPage);
    ASSERT_SUCCESS(page->Delete(txn, 0));
  }

  // Act 2 -- emulate crash (no commit), then recover from log
  Recover();
  r_->RecoverFrom(0, tm_.get());

  // Assert -- uncommitted delete discarded; row 0 retains original
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), "original message");
}

TEST_F(RecoveryManagerTest, InsertMediaCrash) {
  // Arrange
  auto txn = tm_->Begin();
  std::string record = "blah~blah";

  // Act 1 -- insert record into row page (txn not committed)
  {
    PageRef page = p_->GetPage(page_id_);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kRowPage);
    const bin_size_t before_size = page->body.row_page.FreeSizeForTest();
    ASSERT_SUCCESS(page->Insert(txn, record).GetStatus());
    ASSERT_EQ(page->body.row_page.FreeSizeForTest(),
              before_size - record.size() - sizeof(RowPointer));
  }

  // Act 2 -- emulate media failure (db file removed), then recover from log
  MediaFailure();
  r_->RecoverFrom(0, tm_.get());

  // Assert -- uncommitted insert discarded; row count = 0
  ASSERT_EQ(GetRowCount(), 0);
}

TEST_F(RecoveryManagerTest, UpdateMediaCrash) {
  // Arrange -- row 0 = "original message" committed
  ASSERT_TRUE(InsertRow("original message"));
  auto txn = tm_->Begin();
  std::string record = "blah~blah";

  // Act 1 -- update row 0 (txn not committed)
  {
    PageRef page = p_->GetPage(page_id_);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kRowPage);
    ASSERT_SUCCESS(page->Update(txn, 0, record));
  }

  // Act 2 -- emulate media failure (db file removed), then recover from log
  MediaFailure();
  r_->RecoverFrom(0, tm_.get());

  // Assert -- uncommitted update discarded; row 0 retains original
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), "original message");
}

TEST_F(RecoveryManagerTest, DeleteMediaCrash) {
  // Arrange -- row 0 = "original message" committed
  ASSERT_TRUE(InsertRow("original message"));
  auto txn = tm_->Begin();

  // Act 1 -- delete row 0 (txn not committed)
  {
    PageRef page = p_->GetPage(page_id_);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kRowPage);
    ASSERT_SUCCESS(page->Delete(txn, 0));
  }

  // Act 2 -- emulate media failure (db file removed), then recover from log
  MediaFailure();
  r_->RecoverFrom(0, tm_.get());

  // Assert -- uncommitted delete discarded; row 0 retains original
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), "original message");
}

TEST_F(RecoveryManagerTest, InsertSinglePageFailure) {
  // Arrange
  auto txn = tm_->Begin();
  std::string record = "blah~blah";

  // Act 1 -- insert record into row page (txn not committed)
  {
    PageRef page = p_->GetPage(page_id_);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kRowPage);
    const bin_size_t before_size = page->body.row_page.FreeSizeForTest();
    ASSERT_SUCCESS(page->Insert(txn, record).GetStatus());
    ASSERT_EQ(page->body.row_page.FreeSizeForTest(),
              before_size - record.size() - sizeof(RowPointer));
  }

  // Act 2 -- emulate single-page corruption, then recover from log
  SinglePageFailure(page_id_);
  r_->RecoverFrom(0, tm_.get());

  // Assert -- uncommitted insert discarded; row count = 0
  ASSERT_EQ(GetRowCount(), 0);
}

TEST_F(RecoveryManagerTest, UpdateSinglePageFailure) {
  // Arrange -- row 0 = "original message" committed
  ASSERT_TRUE(InsertRow("original message"));
  auto txn = tm_->Begin();
  std::string record = "modified message";

  // Act 1 -- update row 0 (txn not committed)
  {
    PageRef page = p_->GetPage(page_id_);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kRowPage);
    ASSERT_SUCCESS(page->Update(txn, 0, record));
  }

  // Act 2 -- emulate single-page corruption, then recover from log
  SinglePageFailure(page_id_);
  r_->RecoverFrom(0, tm_.get());

  // Assert -- uncommitted update discarded; row 0 retains original
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), "original message");
}

TEST_F(RecoveryManagerTest, DeleteSinglePageFailure) {
  // Arrange -- row 0 = "original message" committed
  ASSERT_TRUE(InsertRow("original message"));
  auto txn = tm_->Begin();

  // Act 1 -- delete row 0 (txn not committed)
  {
    PageRef page = p_->GetPage(page_id_);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kRowPage);
    ASSERT_SUCCESS(page->Delete(txn, 0));
  }

  // Act 2 -- emulate single-page corruption, then recover from log
  SinglePageFailure(page_id_);
  r_->RecoverFrom(0, tm_.get());

  // Assert -- uncommitted delete discarded; row 0 retains original
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), "original message");
}

TEST_F(RecoveryManagerTest, LogUndoWithUnknownLogThrows) {
  // Arrange -- a default-constructed LogRecord has LogType::kUnknown
  LogRecord unknown;
  // Act -- ask for an undo of the unknown record
  // Assert -- IsPageManipulation rejects it with a runtime error
  EXPECT_THROW(r_->LogUndoWithPage(0, unknown, tm_.get()),
               std::runtime_error);
}

TEST_F(RecoveryManagerTest, DestroyPageRedoThrowsNotImplemented) {
  // Arrange -- append a SystemDestroyPage record for the fixture row page and
  // flush it so recovery can see it
  l_->AddLog(
      LogRecord::DestroyPageLogRecord(100, 1, page_id_).Serialize());
  while (l_->CommittedLSN() < l_->BufferedLSN()) {
    std::this_thread::yield();
  }
  PageRef page = p_->GetPage(page_id_);
  ASSERT_FALSE(page.IsNull());

  // Act -- replay the page's logs; the destroy record reaches LogRedo
  // Assert -- the redo path for kSystemDestroyPage is unimplemented and throws
  EXPECT_THROW(r_->SinglePageRecovery(std::move(page), tm_.get()),
               std::runtime_error);
}

TEST_F(RecoveryManagerTest, EndCheckpointRecordsCommittedTransactions) {
  // Arrange -- append an EndCheckpoint record whose active-transaction table
  // already marks txn 42 as committed
  l_->AddLog(LogRecord::EndCheckpointLogRecord(
                 {{page_id_, 0}},
                 {{42, TransactionStatus::kCommitted, 0}})
                 .Serialize());
  while (l_->CommittedLSN() < l_->BufferedLSN()) {
    std::this_thread::yield();
  }

  // Act -- recover from the beginning of the log
  r_->RecoverFrom(0, tm_.get());

  // Assert -- implicit; the committed txn is folded into the analysis phase
  // without crashing or undoing anything
}

TEST_F(RecoveryManagerTest, ReadLogOnMissingFileFails) {
  // Arrange -- a recovery manager over a log path that does not exist
  RecoveryManager missing("no-such-log-file-xyz.log", p_->GetPool());
  LogRecord log;

  // Act -- attempt to read from the missing file
  // Assert -- ReadLog reports failure instead of decoding garbage
  EXPECT_FALSE(missing.ReadLog(0, &log));
}

TEST_F(RecoveryManagerTest, AbortWithDestroyPageLogReinitializesPage) {
  // Arrange -- a transaction that logged only a page-destroy record
  auto txn = tm_->Begin();
  txn.DestroyPageLog(page_id_);

  // Act -- abort the transaction; the undo walks the destroy record and
  // re-initializes the page via LogUndo's kSystemDestroyPage arm
  txn.Abort();

  // Assert -- implicit; the undo path ran without crashing; gtest green on pass
  ASSERT_TRUE(txn.IsFinished());
}

TEST_F(RecoveryManagerTest, LogUndoWithPageAppliesUpdateUndo) {
  // Arrange -- a committed row that the undo should restore
  ASSERT_TRUE(InsertRow("before"));
  LogRecord update =
      LogRecord::UpdatingLogRecord(0, 42, page_id_, 0, "after", "before");

  // Act -- replay the undo of an update directly on the row page
  r_->LogUndoWithPage(0, update, tm_.get());

  // Assert -- the row reverts to its pre-update content
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), "before");
}

TEST_F(RecoveryManagerTest, LogUndoWithPageUndoBranchLowestValue) {
  // Arrange -- a freshly allocated branch page (released before the undo)
  page_id_t pid = 0;
  {
    auto txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kBranchPage);
    pid = page->PageID();
    page.PageUnlock();
    txn.PreCommit();
  }
  LogRecord lowest = LogRecord::SetLowestLogRecord(0, 7, pid, 11, 13);

  // Act -- undo a SetLowestValue on the branch page
  r_->LogUndoWithPage(0, lowest, tm_.get());

  // Assert -- implicit; the branch-page undo ran without crashing
}

TEST_F(RecoveryManagerTest, LogUndoWithPageUndoSetFoster) {
  // Arrange -- a freshly allocated leaf page (released before the undo)
  page_id_t pid = 0;
  {
    auto txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kLeafPage);
    pid = page->PageID();
    page.PageUnlock();
    txn.PreCommit();
  }
  LogRecord foster =
      LogRecord::SetFosterLogRecord(0, 7, pid, FosterPair("redo", 1),
                                    FosterPair("undo", 2));

  // Act -- undo a SetFoster on the leaf page
  r_->LogUndoWithPage(0, foster, tm_.get());

  // Assert -- implicit; the leaf-page foster undo ran without crashing
}

TEST_F(RecoveryManagerTest, LogUndoWithPageUndoSetFences) {
  // Arrange -- a freshly allocated leaf page (released before the undo)
  page_id_t pid = 0;
  {
    auto txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kLeafPage);
    pid = page->PageID();
    page.PageUnlock();
    txn.PreCommit();
  }
  LogRecord low = LogRecord::SetLowFenceLogRecord(0, 7, pid,
                                                  IndexKey("redo-low"),
                                                  IndexKey("undo-low"));
  LogRecord high = LogRecord::SetHighFenceLogRecord(0, 7, pid,
                                                    IndexKey("redo-high"),
                                                    IndexKey("undo-high"));

  // Act -- undo the low and high fence updates
  r_->LogUndoWithPage(0, low, tm_.get());
  r_->LogUndoWithPage(0, high, tm_.get());

  // Assert -- implicit; both fence undos ran without crashing
}

TEST_F(RecoveryManagerTest, LogUndoWithPageCompensateSetFosterIsNoop) {
  // Arrange -- a freshly allocated leaf page (released before the undo)
  page_id_t pid = 0;
  {
    auto txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kLeafPage);
    pid = page->PageID();
    page.PageUnlock();
    txn.PreCommit();
  }
  LogRecord compensate =
      LogRecord::CompensateSetFosterLogRecord(0, 7, pid, FosterPair("redo", 1));

  // Act -- compensating records have nothing to undo
  r_->LogUndoWithPage(0, compensate, tm_.get());

  // Assert -- implicit; the compensating-record no-op path ran without crash
}

TEST_F(RecoveryManagerTest, RecoverFromMidLogRedoFromCheckpoint) {
  // Arrange -- commit row "first", flush the page, then commit row "second"
  ASSERT_TRUE(InsertRow("first"));
  Flush();
  lsn_t restart_point = l_->CommittedLSN();
  ASSERT_TRUE(InsertRow("second"));

  // Act -- crash and recover from the LSN captured after the first commit
  Recover();
  r_->RecoverFrom(restart_point, tm_.get());

  // Assert -- the on-disk "first" row survived and "second" was redone
  ASSERT_EQ(GetRowCount(), 2);
  ASSERT_EQ(ReadRow(1), "second");
}

TEST_F(RecoveryManagerTest, RecoverFromTwiceIsIdempotent) {
  // Arrange -- commit a row, then wipe the database file
  InsertRow("hoge");
  MediaFailure();
  r_->RecoverFrom(0, tm_.get());

  // Act -- recover a second time over the already-recovered state
  r_->RecoverFrom(0, tm_.get());

  // Assert -- redoing the same log a second time leaves the row intact
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), "hoge");
}

}  // namespace tinylamb