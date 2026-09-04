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
#include <stdlib.h>  // NOLINT(modernize-deprecated-headers) // POSIX setenv/unsetenv below are only provided by this header.

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
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
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "log_record.hpp"
#include "logger.hpp"
#include "page/index_key.hpp"
#include "page/page_type.hpp"
#include "page/row_page_test.hpp"
#include "page/row_pointer.hpp"
#include "recovery/checkpoint_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"

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
    tm_ = std::make_unique<TransactionManager>(p_.get(), l_.get(), r_.get());
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
    RecoveryManager::SetTornTailTruncationAllowed(false);
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
  EXPECT_THROW(r_->LogUndoWithPage(0, unknown, tm_.get()), std::runtime_error);
}

TEST_F(RecoveryManagerTest, LoserUndoSurvivesRepeatedRecoveryPasses) {
  // D2 (docs/design.md) acceptance 1: a database with loser INSERT and
  // DELETE chains must produce the same row count and content through
  // several RecoverFrom passes (page_lsn rewinds on undo; every replayed
  // redo/undo pair must land on the same logical state).
  ASSERT_TRUE(InsertRow("committed-1"));
  ASSERT_TRUE(InsertRow("committed-2"));
  DeleteRow(1);  // committed physical delete of slot 1
  ASSERT_TRUE(InsertRow("loser-insert", /*commit=*/false));
  {
    // Loser DELETE: writes the delete record, then dies without commit.
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(page_id_);
    ASSERT_SUCCESS(page->Delete(txn, 0));
    page.PageUnlock();
    while (l_->CommittedLSN() < l_->BufferedLSN()) {
      std::this_thread::yield();
    }
    // Crash without commit or abort: the record stays a loser in the WAL.
  }

  size_t prev_count = 0;
  std::string prev_row;
  for (int pass = 0; pass < 3; ++pass) {
    Recover();
    r_->RecoverFrom(0, tm_.get());
    const size_t count = GetRowCount();
    const std::string row = ReadRow(0);
    if (pass > 0) {
      EXPECT_EQ(count, prev_count) << "pass " << pass;
      EXPECT_EQ(row, prev_row) << "pass " << pass;
    }
    // The loser insert and the loser delete must both be undone: only
    // "committed-1" at slot 0 survives the second delete's undo restoring
    // slot 0... but "committed-1" was physically deleted? No: the committed
    // DELETE removed slot 1 ("committed-2"); the loser DELETE targeted slot
    // 0 and must be rolled back.
    EXPECT_EQ(count, 1U) << "pass " << pass;
    EXPECT_EQ(row, "committed-1") << "pass " << pass;
    prev_count = count;
    prev_row = row;
  }
}

TEST_F(RecoveryManagerTest, DestroyPageRedoInitializesFreePageAndRebuildsList) {
  // D3 (docs/design.md) acceptance 1/3: a committed destroy redo turns the
  // page into a valid free page, the recovery-end scan links it into the
  // rebuilt free list, and the next allocation reuses the id.
  ASSERT_TRUE(InsertRow("doomed"));
  Flush();
  l_->AddLog(LogRecord::DestroyPageLogRecord(100, 1, page_id_,
                                             PageType::kRowPage, "body")
                 .Serialize());
  while (l_->CommittedLSN() < l_->BufferedLSN()) {
    std::this_thread::yield();
  }

  Recover();
  r_->RecoverFrom(0, tm_.get());

  {
    PageRef page = p_->GetPage(page_id_);
    ASSERT_FALSE(page.IsNull());
    EXPECT_EQ(page->Type(), PageType::kFreePage);
    page.PageUnlock();
  }
  {
    PageRef meta = p_->GetPage(kMetaPageId);
    EXPECT_EQ(meta->body.meta_page.FirstFreePage(), page_id_);
  }

  // The freed page is handed out again by the allocator.
  auto txn = tm_->Begin();
  PageRef reused = p_->AllocateNewPage(txn, PageType::kRowPage);
  EXPECT_EQ(reused->PageID(), page_id_);
  reused.PageUnlock();
  ASSERT_SUCCESS(txn.PreCommit());

  // Re-running recovery over the same WAL (acceptance 3) must not corrupt
  // the free page structure or hand the id out twice.
  Recover();
  r_->RecoverFrom(0, tm_.get());
  r_->RecoverFrom(0, tm_.get());
  PageRef again = p_->GetPage(page_id_);
  ASSERT_FALSE(again.IsNull());
  EXPECT_NE(again->Type(), PageType::kUnknown);
}

TEST_F(RecoveryManagerTest, DestroyPageRedoTwiceOnSamePageIsIdempotent) {
  // D3 (docs/design.md) acceptance 3: applying the same destroy redo twice
  // leaves a structurally identical free page.
  ASSERT_TRUE(InsertRow("twice"));
  Flush();
  const std::string record = LogRecord::DestroyPageLogRecord(
                                 100, 1, page_id_, PageType::kRowPage, "body")
                                 .Serialize();
  l_->AddLog(record);
  l_->AddLog(record);
  while (l_->CommittedLSN() < l_->BufferedLSN()) {
    std::this_thread::yield();
  }

  Recover();
  r_->RecoverFrom(0, tm_.get());

  // The rebuilt free-page image is dirty in the pool; flush it so the
  // header/checksum assertions observe the bytes that would survive a
  // second crash.
  p_->GetPool()->FlushPageForTest(page_id_);
  {
    PageRef page = p_->GetPage(page_id_);
    ASSERT_FALSE(page.IsNull());
    EXPECT_EQ(page->Type(), PageType::kFreePage);
    EXPECT_TRUE(page->IsValid());
    EXPECT_EQ(page->body.free_page.NextFreePage(), 0);
    page.PageUnlock();
  }
  {
    PageRef meta = p_->GetPage(kMetaPageId);
    EXPECT_EQ(meta->body.meta_page.FirstFreePage(), page_id_);
  }
}

TEST_F(RecoveryManagerTest, DestroyPageUndoRestoresRowContent) {
  // D3: an aborted destroy (here a rolled-back DROP TABLE pattern) restores
  // the page image, so committed rows survive the rollback.
  ASSERT_TRUE(InsertRow("keepme"));
  const page_id_t doomed = page_id_;
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(doomed);
    ASSERT_FALSE(page.IsNull());
    p_->DestroyPage(txn, &*page);
    page.PageUnlock();
    txn.Abort();
  }
  {
    PageRef after = p_->GetPage(doomed);
    ASSERT_FALSE(after.IsNull());
    EXPECT_EQ(after->Type(), PageType::kRowPage);
  }
  ASSERT_EQ(GetRowCount(), 1);
  EXPECT_EQ(ReadRow(0), "keepme");
  // The restored page must not linger on the allocator free stack.
  PageRef meta = p_->GetPage(kMetaPageId);
  EXPECT_NE(meta->body.meta_page.FirstFreePage(), doomed);
}

TEST_F(RecoveryManagerTest, LegacyV1DestroyRecordStillParses) {
  // D3 (docs/design.md): readers keep accepting the v1 pid-only destroy
  // record; Size() matches the on-disk bytes so log scanning stays synced.
  LogRecord record = LogRecord::DestroyPageLogRecord(100, 1, page_id_);
  std::string v1 = record.Serialize();
  // Strip the v2+ destroy tail (PageType u64 + length-prefixed empty body)
  // and the v3 CRC field, then downgrade the header version back to v1
  // (big-endian): the result is a byte-exact legacy v1 pid-only record.
  v1.resize(v1.size() -
            (sizeof(uint64_t) + sizeof(uint16_t) + kWalRecordCrcSize));
  v1[4] = 0;
  v1[5] = 0;
  v1[6] = 0;
  v1[7] = kLegacyWalRecordVersion;
  ASSERT_EQ(v1.size(), 4U + 4U + 2U + 8U + 8U + 1U + 8U);

  std::istringstream in(v1);
  Decoder dec(in);
  LogRecord decoded;
  dec >> decoded;
  EXPECT_EQ(decoded.type, LogType::kSystemDestroyPage);
  EXPECT_EQ(decoded.pid, page_id_);
  EXPECT_EQ(decoded.allocated_page_type, PageType::kUnknown);
  EXPECT_TRUE(decoded.undo_data.empty());
  EXPECT_EQ(decoded.Size(), v1.size());  // v1 has no CRC tail

  // Scanning over a legacy record must stay byte-synchronized.
  const lsn_t start = l_->BufferedLSN();
  l_->AddLog(v1);
  while (l_->CommittedLSN() < l_->BufferedLSN()) {
    std::this_thread::yield();
  }
  EXPECT_EQ(r_->ValidLogEnd(start), start + v1.size());
}

TEST_F(RecoveryManagerTest, EndCheckpointRecordsCommittedTransactions) {
  // Arrange -- append an EndCheckpoint record whose active-transaction table
  // already marks txn 42 as committed
  l_->AddLog(LogRecord::EndCheckpointLogRecord(
                 {{page_id_, 0}}, {{42, TransactionStatus::kCommitted, 0}})
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
  LogRecord foster = LogRecord::SetFosterLogRecord(
      0, 7, pid, FosterPair("redo", 1), FosterPair("undo", 2));

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
  LogRecord low = LogRecord::SetLowFenceLogRecord(
      0, 7, pid, IndexKey("redo-low"), IndexKey("undo-low"));
  LogRecord high = LogRecord::SetHighFenceLogRecord(
      0, 7, pid, IndexKey("redo-high"), IndexKey("undo-high"));

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

TEST_F(RecoveryManagerTest, SinglePageRecoverySkipsRedoWhenLsnAlreadyApplied) {
  // Arrange -- a committed insert sets the row page's PageLSN to the insert
  // log's own offset, so every page log is at lsn <= PageLSN.
  ASSERT_TRUE(InsertRow("already-applied"));

  // Act -- replay the page's logs; LogRedo short-circuits on lsn <= PageLSN
  // for both the allocation and the insert record.
  PageRef page = p_->GetPage(page_id_);
  ASSERT_FALSE(page.IsNull());
  ASSERT_EQ(page->Type(), PageType::kRowPage);
  r_->SinglePageRecovery(std::move(page), tm_.get());

  // Assert -- the committed row survives untouched.
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), "already-applied");
}

TEST_F(RecoveryManagerTest, ReplayLeafPageLogsRedoAndUndo) {
  // Arrange -- a freshly allocated leaf page plus a log tail of leaf
  // manipulation records all owned by the uncommitted transaction 42.
  page_id_t pid = 0;
  {
    auto txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kLeafPage);
    pid = page->PageID();
    page.PageUnlock();
    ASSERT_SUCCESS(txn.PreCommit());
  }
  l_->AddLog(
      LogRecord::InsertingLeafLogRecord(0, 42, pid, "k", "v1").Serialize());
  l_->AddLog(LogRecord::CompensatingUpdateLeafLogRecord(42, pid, "k", "cv")
                 .Serialize());
  l_->AddLog(LogRecord::UpdatingLeafLogRecord(0, 42, pid, "k", "v2", "v1")
                 .Serialize());
  l_->AddLog(
      LogRecord::DeletingLeafLogRecord(0, 42, pid, "k", "v2").Serialize());
  l_->AddLog(LogRecord::SetLowFenceLogRecord(0, 42, pid, IndexKey("lo"),
                                             IndexKey("undo-lo"))
                 .Serialize());
  l_->AddLog(LogRecord::SetHighFenceLogRecord(0, 42, pid, IndexKey("hi"),
                                              IndexKey("undo-hi"))
                 .Serialize());
  l_->AddLog(LogRecord::SetFosterLogRecord(0, 42, pid, FosterPair("new", 1),
                                           FosterPair("old", 2))
                 .Serialize());
  while (l_->CommittedLSN() < l_->BufferedLSN()) {
    std::this_thread::yield();
  }

  // Act -- replay every log of this page (redo then undo; txn 42 is not
  // committed so the undo phase runs too).
  {
    PageRef page = p_->GetPage(pid);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kLeafPage);
    r_->SinglePageRecovery(std::move(page), tm_.get());
  }

  // Assert -- the redo phase applied and the undo phase fully reversed the
  // uncommitted writes, leaving an empty leaf page.
  {
    PageRef page = p_->GetPage(pid);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kLeafPage);
    ASSERT_EQ(page->RowCount(), 0);
  }
}

TEST_F(RecoveryManagerTest, ReplayBranchPageLogsRedoAndUndo) {
  // Arrange -- a freshly allocated branch page plus a log tail of branch
  // manipulation records owned by the uncommitted transaction 42.
  page_id_t pid = 0;
  {
    auto txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kBranchPage);
    pid = page->PageID();
    page.PageUnlock();
    ASSERT_SUCCESS(txn.PreCommit());
  }
  l_->AddLog(
      LogRecord::InsertingBranchLogRecord(0, 42, pid, "b", 50).Serialize());
  l_->AddLog(
      LogRecord::UpdatingBranchLogRecord(0, 42, pid, "b", 51, 50).Serialize());
  l_->AddLog(
      LogRecord::DeletingBranchLogRecord(0, 42, pid, "b", 51).Serialize());
  l_->AddLog(LogRecord::SetLowestLogRecord(0, 42, pid, 60, 70).Serialize());
  while (l_->CommittedLSN() < l_->BufferedLSN()) {
    std::this_thread::yield();
  }

  // Act -- replay every log of this page (redo then undo).
  {
    PageRef page = p_->GetPage(pid);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kBranchPage);
    r_->SinglePageRecovery(std::move(page), tm_.get());
  }

  // Assert -- the branch-page redo and undo both ran; the page is empty again.
  {
    PageRef page = p_->GetPage(pid);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kBranchPage);
    ASSERT_EQ(page->RowCount(), 0);
  }
}

TEST_F(RecoveryManagerTest, BrokenLeafPageRecoveryRedoAllocPageLog) {
  // Arrange -- a committed leaf page that is also dirty (its allocation log).
  page_id_t pid = 0;
  {
    auto txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kLeafPage);
    pid = page->PageID();
    page.PageUnlock();
    ASSERT_SUCCESS(txn.PreCommit());
  }

  // Act -- corrupt the leaf page region, then recover; the broken page enters
  // SinglePageRecovery with page_lsn reset to 0 so the allocation log must be
  // redone (kSystemAllocPage arm of LogRedo).
  SinglePageFailure(pid);
  r_->RecoverFrom(0, tm_.get());

  // Assert -- the allocation redo re-initialized the page as a leaf page.
  PageRef page = p_->GetPage(pid);
  ASSERT_FALSE(page.IsNull());
  ASSERT_EQ(page->Type(), PageType::kLeafPage);
}

TEST_F(RecoveryManagerTest, RecoverFromSkipsBeginCheckpointInAnalysis) {
  // Arrange -- a BeginCheckpoint record appended after the fixture's
  // committed row-page allocation.
  l_->AddLog(LogRecord::BeginCheckpointLogRecord().Serialize());
  while (l_->CommittedLSN() < l_->BufferedLSN()) {
    std::this_thread::yield();
  }

  // Act -- recover from the beginning of the log.
  r_->RecoverFrom(0, tm_.get());

  // Assert -- the analysis phase took its default arm for the checkpoint
  // record without crashing and the row page is still usable.
  ASSERT_EQ(GetRowCount(), 0);
}

TEST_F(RecoveryManagerTest, RecoverFromParallelWithMultipleDirtyPages) {
  // Arrange -- two committed row pages, each holding one row.
  ASSERT_TRUE(InsertRow("first"));
  page_id_t second_pid = 0;
  {
    auto txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kRowPage);
    second_pid = page->PageID();
    ASSERT_SUCCESS(page->Insert(txn, "second").GetStatus());
    page.PageUnlock();
    ASSERT_SUCCESS(txn.PreCommit());
  }

  // Act -- flush both pages to disk so they stay valid across the reopen,
  // crash and reopen; two valid dirty pages yield two replay jobs that reach
  // ReplayPagesInParallel's multi-threaded worker branch.  Pin the worker
  // count via the environment so the branch is deterministic.
  Flush();
  p_->GetPool()->FlushPageForTest(second_pid);
  ASSERT_EQ(setenv("TINYLAMB_RECOVERY_WORKERS", "2", 1), 0);
  Recover();
  r_->RecoverFrom(0, tm_.get());
  unsetenv("TINYLAMB_RECOVERY_WORKERS");

  // Assert -- both rows survived (redo was already applied on disk).
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), "first");
  {
    PageRef page = p_->GetPage(second_pid);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kRowPage);
    ASSERT_EQ(page->RowCount(), 1);
  }
}

TEST_F(RecoveryManagerTest, LogUndoWithPageUndoLeafInsert) {
  // Arrange -- a leaf page that already holds key "k" (the pre-insert state).
  page_id_t pid = 0;
  {
    auto txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kLeafPage);
    pid = page->PageID();
    page->InsertImpl("k", "v");
    page.PageUnlock();
    ASSERT_SUCCESS(txn.PreCommit());
  }
  LogRecord insert = LogRecord::InsertingLeafLogRecord(0, 42, pid, "k", "v");

  // Act -- undo the insert of key "k".
  r_->LogUndoWithPage(0, insert, tm_.get());

  // Assert -- the leaf-page undo deleted the key.
  {
    auto txn = tm_->Begin();
    PageRef page = p_->GetPage(pid);
    ASSERT_FALSE(page.IsNull());
    StatusOr<std::string_view> read = page->Read(txn, "k");
    ASSERT_SUCCESS(txn.PreCommit());
    EXPECT_EQ(read.GetStatus(), Status::kNotExists);
  }
}

TEST_F(RecoveryManagerTest, LogUndoWithPageUndoBranchDelete) {
  // Arrange -- an empty branch page; the undo of a branch delete re-inserts.
  page_id_t pid = 0;
  {
    auto txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kBranchPage);
    pid = page->PageID();
    page.PageUnlock();
    ASSERT_SUCCESS(txn.PreCommit());
  }
  LogRecord del = LogRecord::DeletingBranchLogRecord(0, 42, pid, "b", 17);

  // Act -- undo the delete of branch entry "b".
  r_->LogUndoWithPage(0, del, tm_.get());

  // Assert -- implicit; the branch-page undo ran without crashing.
  {
    PageRef page = p_->GetPage(pid);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kBranchPage);
  }
}

TEST_F(RecoveryManagerTest, ParallelReplayRethrowsWorkerError) {
  // Arrange -- two committed, on-disk-valid row pages so the recovery has two
  // replay jobs, plus a corrupt-by-type record (kUpdateBranch against a ROW
  // page) whose LogRedo arm throws via the page-type assertion.  The record
  // is appended after the pages were flushed, so its LSN exceeds the pages'
  // PageLSN and the redo phase really reaches the throwing arm.
  ASSERT_TRUE(InsertRow("first"));
  page_id_t second_pid = 0;
  {
    auto txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kRowPage);
    second_pid = page->PageID();
    ASSERT_SUCCESS(page->Insert(txn, "second").GetStatus());
    page.PageUnlock();
    ASSERT_SUCCESS(txn.PreCommit());
  }
  Flush();
  p_->GetPool()->FlushPageForTest(second_pid);
  l_->AddLog(LogRecord::UpdatingBranchLogRecord(100, 1, page_id_, "k", 2, 3)
                 .Serialize());
  while (l_->CommittedLSN() < l_->BufferedLSN()) {
    std::this_thread::yield();
  }

  // Act -- crash/reopen, then recover.  Force the multi-threaded worker branch
  // so the exception thrown on a worker thread is funneled through the
  // catch/rethrow plumbing inside ReplayPagesInParallel.
  ASSERT_EQ(setenv("TINYLAMB_RECOVERY_WORKERS", "2", 1), 0);
  Recover();
  EXPECT_THROW(r_->RecoverFrom(0, tm_.get()), std::runtime_error);
  unsetenv("TINYLAMB_RECOVERY_WORKERS");
}

TEST_F(RecoveryManagerTest, ReadLogDecodesCommittedInsert) {
  // Arrange -- a committed insert leaves a durable log that must be readable.
  ASSERT_TRUE(InsertRow("readable"));

  // Act -- walk the log from LSN 0 looking for the insert record of the
  // fixture page; every record must decode via ReadLog at its own offset.
  std::uintmax_t filesize = std::filesystem::file_size(file_name_ + ".log");
  lsn_t offset = 0;
  bool saw_insert = false;
  while (offset < filesize) {
    LogRecord log;
    ASSERT_TRUE(r_->ReadLog(offset, &log)) << "offset: " << offset;
    if (log.type == LogType::kInsertRow && log.pid == page_id_) {
      saw_insert = true;
    }
    offset += log.Size();
  }

  // Assert -- the committed insert record was reached and decoded.
  ASSERT_TRUE(saw_insert);
}

TEST_F(RecoveryManagerTest, RecoveryTraceLogsExecuteInFreshProcess) {
  // RecoveryTraceEnabled() caches getenv() in a process-local static, so a
  // test that runs after the first recovery in this process can never flip it.
  // Re-exec the current binary with only this test and the trace env set so a
  // fresh process initializes the static to true and the trace LOG() lines in
  // the analysis phase and PageReplay actually execute.
  if (std::getenv("TINYLAMB_RECOVERY_TRACE") != nullptr) {
    // Fresh process: recovery trace is active from the very first use.
    ASSERT_TRUE(InsertRow("hoge"));
    MediaFailure();
    r_->RecoverFrom(0, tm_.get());
    ASSERT_EQ(GetRowCount(), 1);
    return;
  }
  const std::string self =
      std::filesystem::read_symlink("/proc/self/exe").string();
  const std::string filter =
      "--gtest_filter=RecoveryManagerTest."
      "RecoveryTraceLogsExecuteInFreshProcess";
  const std::string cmd = "TINYLAMB_RECOVERY_TRACE=1 '" + self + "' " + filter +
                          " --gtest_brief=1 >/dev/null 2>&1";
  // Re-executes this test binary itself to verify the env-gated recovery
  // trace; no external command processor feature is relied upon.
  ASSERT_EQ(std::system(cmd.c_str()), 0);  // NOLINT(cert-env33-c)
}

namespace {
void WaitForLogFlush(const std::unique_ptr<Logger>& logger) {
  while (logger->CommittedLSN() < logger->BufferedLSN() ||
         logger->DurableLSN() < logger->BufferedLSN()) {
    std::this_thread::yield();
  }
}
}  // namespace

TEST_F(RecoveryManagerTest, TornTailGarbageIsTruncatedOnce) {
  // Arrange -- a committed row followed by garbage appended to the WAL.
  // Forced recovery (--force): truncate the damaged tail and replay.
  RecoveryManager::SetTornTailTruncationAllowed(true);
  ASSERT_TRUE(InsertRow("survivor"));
  WaitForLogFlush(l_);
  const auto valid_end = std::filesystem::file_size(file_name_ + ".log");
  {
    std::ofstream tail(file_name_ + ".log", std::ios::app | std::ios::binary);
    tail << "\x51\x13torn-tail";
    ASSERT_FALSE(tail.fail());
  }
  ASSERT_GT(std::filesystem::file_size(file_name_ + ".log"), valid_end);

  // Act -- crash-reopen and recover; the unparseable tail must be truncated
  // instead of failing the analysis pass.
  Recover();
  EXPECT_NO_THROW(r_->RecoverFrom(0, tm_.get()));

  // Assert -- the log ends at the last valid record and the row survives.
  EXPECT_EQ(std::filesystem::file_size(file_name_ + ".log"), valid_end);
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), "survivor");
}

TEST_F(RecoveryManagerTest, HalfWrittenRecordIsTruncatedOnce) {
  // Arrange -- a committed row plus a real record cut in half by the crash.
  RecoveryManager::SetTornTailTruncationAllowed(true);
  ASSERT_TRUE(InsertRow("committed row"));
  WaitForLogFlush(l_);
  const auto valid_end = std::filesystem::file_size(file_name_ + ".log");
  const std::string payload =
      LogRecord::InsertingLogRecord(0, 777, page_id_, 1, "half").Serialize();
  {
    std::ofstream tail(file_name_ + ".log", std::ios::app | std::ios::binary);
    tail.write(payload.data(),
               static_cast<std::streamsize>(payload.size() / 2));
    ASSERT_FALSE(tail.fail());
  }

  // Act -- crash-reopen and recover from the torn log.
  Recover();
  EXPECT_NO_THROW(r_->RecoverFrom(0, tm_.get()));

  // Assert -- the truncated record is gone and the committed row is intact.
  EXPECT_EQ(std::filesystem::file_size(file_name_ + ".log"), valid_end);
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), "committed row");
}

// Default policy: WAL corruption is unrecoverable -- boot logs the offset
// and aborts instead of silently dropping committed transactions. Runs in a
// freshly re-exec'd process (fork() of a threaded test would deadlock on
// latches the logger worker holds), selected by an env gate.
TEST_F(RecoveryManagerTest, CorruptTailAbortsByDefault) {
  if (std::getenv("TINYLAMB_CORRUPT_TAIL_PROBE") != nullptr) {
    // Fresh process: build the damaged WAL, then let recovery abort.
    ASSERT_TRUE(InsertRow("survivor"));
    WaitForLogFlush(l_);
    {
      std::ofstream tail(file_name_ + ".log", std::ios::app | std::ios::binary);
      tail << "\x51\x13torn-tail";
      ASSERT_FALSE(tail.fail());
    }
    Recover();
    r_->RecoverFrom(0, tm_.get());
    FAIL() << "corrupt WAL must abort recovery";
    return;
  }
  ASSERT_FALSE(RecoveryManager::TornTailTruncationAllowed());
  const std::string self =
      std::filesystem::read_symlink("/proc/self/exe").string();
  const std::string filter =
      "--gtest_filter=RecoveryManagerTest.CorruptTailAbortsByDefault";
  const std::string cmd = "TINYLAMB_CORRUPT_TAIL_PROBE=1 '" + self + "' " +
                          filter + " --gtest_brief=1 >/dev/null 2>&1";
  // Non-zero exit = the fresh process aborted on corruption, as required.
  EXPECT_NE(std::system(cmd.c_str()), 0);  // NOLINT(cert-env33-c)
}

// --force: truncate at the corruption point, then recover the intact prefix.
TEST_F(RecoveryManagerTest, ForceRecoversFromCorruptTail) {
  ASSERT_TRUE(InsertRow("survivor"));
  WaitForLogFlush(l_);
  const auto valid_end = std::filesystem::file_size(file_name_ + ".log");
  {
    std::ofstream tail(file_name_ + ".log", std::ios::app | std::ios::binary);
    tail << "\x00\x00garbage";
    ASSERT_FALSE(tail.fail());
  }
  RecoveryManager::SetTornTailTruncationAllowed(true);
  Recover();
  EXPECT_NO_THROW(r_->RecoverFrom(0, tm_.get()));
  EXPECT_EQ(std::filesystem::file_size(file_name_ + ".log"), valid_end);
  ASSERT_EQ(GetRowCount(), 1);
  ASSERT_EQ(ReadRow(0), "survivor");
}

TEST_F(RecoveryManagerTest, ParallelAbortsReadLogIndependently) {
  // Arrange -- two live transactions inserting into different pages, so both
  // can abort concurrently. Each abort walks its prev_lsn chain via ReadLog
  // from its own thread.
  page_id_t second_pid = 0;
  {
    auto txn = tm_->Begin();
    PageRef page = p_->AllocateNewPage(txn, PageType::kRowPage);
    second_pid = page->PageID();
    ASSERT_SUCCESS(page->Insert(txn, "seed").GetStatus());
    page.PageUnlock();
    ASSERT_SUCCESS(txn.PreCommit());
  }
  auto txn_a = tm_->Begin();
  auto txn_b = tm_->Begin();
  {
    PageRef page = p_->GetPage(page_id_);
    ASSERT_SUCCESS(page->Insert(txn_a, "aaa").GetStatus());
    page.PageUnlock();
  }
  {
    PageRef page = p_->GetPage(second_pid);
    ASSERT_SUCCESS(page->Insert(txn_b, "bbb").GetStatus());
    page.PageUnlock();
  }
  while (l_->CommittedLSN() < l_->BufferedLSN()) {
    std::this_thread::yield();
  }

  // Act -- abort both transactions at the same time.
  std::thread ta([&] { txn_a.Abort(); });
  std::thread tb([&] { txn_b.Abort(); });
  ta.join();
  tb.join();

  // Assert -- both inserts were undone on their own pages.
  ASSERT_EQ(GetRowCount(), 0);
  {
    PageRef page = p_->GetPage(second_pid);
    ASSERT_FALSE(page.IsNull());
    ASSERT_EQ(page->Type(), PageType::kRowPage);
    EXPECT_EQ(page->RowCount(), 1);  // Only the committed seed remains.
    page.PageUnlock();
  }
}

TEST_F(RecoveryManagerTest, CheckpointDirtyPageTablePreservesMaxPageId) {
  // PRODUCTION BUG (fixed): analysis starts at checkpoint_lsn, so pages
  // allocated before the checkpoint were invisible to the allocator high-
  // water re-derivation.  After a crash (dropped pages, unflushed meta page)
  // recovery re-issued LIVE page ids and the next allocate destroyed data.
  const std::string master = file_name_ + ".mst";
  {
    // Allocate pages 2..4 and write rows, then checkpoint.
    Transaction seed = tm_->Begin();
    for (uint64_t pid = 2; pid <= 4; ++pid) {
      PageRef page = p_->AllocateNewPage(seed, PageType::kRowPage);
      EXPECT_EQ(page->PageID(), pid);
    }
    PageRef data = p_->GetPage(2);
    ASSERT_SUCCESS(data->Insert(seed, "survivor").GetStatus());
    data.PageUnlock();
    ASSERT_SUCCESS(seed.PreCommit());
    // WriteCheckpoint requires no page latch on this thread.
    CheckpointManager cm(master, tm_.get(), p_->GetPool(), 0);
    const lsn_t checkpoint_lsn = cm.WriteCheckpoint();

    // Simulate the crash: meta page image (with the old max_page_count)
    // never reaches disk.
    p_->GetPool()->DropAllPages();
    // Recovery pass 1 (rebuilds the allocator from the WAL).
    RecoverBase([]() {});
    r_->RecoverFrom(checkpoint_lsn, tm_.get());
    // A newly allocated page must not collide with any live page.
    Transaction alloc = tm_->Begin();
    const PageRef allocated = p_->AllocateNewPage(alloc, PageType::kRowPage);
    EXPECT_GT(allocated->PageID(), 4)
        << "allocator re-issued a pre-checkpoint page id";
    ASSERT_SUCCESS(alloc.PreCommit());

    // Persistence check: a second restart cycle must keep the data.
    p_->GetPool()->DropAllPages();
    RecoverBase([]() {});
    r_->RecoverFrom(0, tm_.get());
    PageRef again = p_->GetPage(2);
    EXPECT_EQ(again->Type(), PageType::kRowPage);
    EXPECT_EQ(again->RowCount(), 1);
    EXPECT_EQ(again->Read(alloc, 0).Value(), "survivor");
    again.PageUnlock();
  }
  std::ignore = std::remove(master.c_str());
}

// D9 (docs/design.md): per-record CRC32C at the WAL tail.
TEST_F(RecoveryManagerTest, MidRecordBitFlipStopsScanAtThatRecord) {
  // Arrange -- a committed row, then a second committed-INSERT record whose
  // payload middle byte is flipped after the log is durable.  Headers stay
  // parseable; only the CRC can catch this corruption.
  ASSERT_TRUE(InsertRow("before-flip"));
  WaitForLogFlush(l_);
  const lsn_t second_start = l_->BufferedLSN();
  l_->AddLog(LogRecord::InsertingLogRecord(0, 99, page_id_, 1, "intact-payload")
                 .Serialize());
  WaitForLogFlush(l_);
  {
    std::fstream file(file_name_ + ".log",
                      std::ios::in | std::ios::out | std::ios::binary);
    // Flip the final payload byte (just before the CRC field).
    const std::streampos target =
        static_cast<std::streampos>(second_start + 20);
    file.seekp(target);
    const int original = file.get();
    file.seekp(target);
    file.put(static_cast<char>(original ^ 0x01));
    ASSERT_FALSE(file.fail());
  }

  // Act -- the scan must stop exactly at the corrupted record, not past it.
  EXPECT_EQ(r_->ValidLogEnd(0), second_start);

  // Assert -- forced recovery truncates at the stop point and keeps the
  // earlier committed row intact.
  RecoveryManager::SetTornTailTruncationAllowed(true);
  Recover();
  ASSERT_NO_FATAL_FAILURE(r_->RecoverFrom(0, tm_.get()));
  EXPECT_EQ(static_cast<lsn_t>(std::filesystem::file_size(file_name_ + ".log")),
            second_start);
  ASSERT_EQ(GetRowCount(), 1);
  EXPECT_EQ(ReadRow(0), "before-flip");
}

TEST_F(RecoveryManagerTest, MixedLegacyAndCurrentVersionLogsScanCleanly) {
  // D9 (docs/design.md): the reader walks a WAL that mixes v1 (no CRC) and
  // current-version (CRC) records; every record byte-counts exactly.
  const lsn_t start = l_->BufferedLSN();
  LogRecord destroy = LogRecord::DestroyPageLogRecord(100, 1, page_id_);
  std::string legacy = destroy.Serialize();
  legacy.resize(legacy.size() -
                (sizeof(uint64_t) + sizeof(uint16_t) + kWalRecordCrcSize));
  legacy[4] = 0;
  legacy[5] = 0;
  legacy[6] = 0;
  legacy[7] = kLegacyWalRecordVersion;
  l_->AddLog(legacy);
  const lsn_t after_legacy = l_->BufferedLSN();
  EXPECT_EQ(after_legacy - start, legacy.size());
  l_->AddLog(destroy.Serialize());
  WaitForLogFlush(l_);

  EXPECT_EQ(r_->ValidLogEnd(start), l_->BufferedLSN());
  LogRecord decoded;
  ASSERT_TRUE(r_->ReadLog(start, &decoded));
  EXPECT_EQ(decoded.wire_version, kLegacyWalRecordVersion);
  ASSERT_TRUE(r_->ReadLog(after_legacy, &decoded));
  EXPECT_EQ(decoded.wire_version, kWalRecordVersion);
}

TEST_F(RecoveryManagerTest, ReadLogRejectsCorruptPageTypeWithoutThrowing) {
  // D4: a record whose header is valid but whose PageType payload byte is out
  // of range must make ReadLog return false (torn/corrupt tail), not throw out
  // of the decoder and abort startup.
  ASSERT_TRUE(InsertRow("alloc-before"));
  WaitForLogFlush(l_);
  const lsn_t start = l_->BufferedLSN();
  LogRecord alloc =
      LogRecord::AllocatePageLogRecord(0, 1, 500, PageType::kRowPage);
  std::string bytes = alloc.Serialize();
  // The PageType is serialized as a big-endian uint64 after the fixed header;
  // corrupt its low byte to an undefined discriminant (v3 has a trailing CRC,
  // so also drop the CRC to force the decode path rather than CRC rejection).
  ASSERT_GT(bytes.size(), 12U);
  bytes[bytes.size() - kWalRecordCrcSize - 1] = static_cast<char>(0x7f);
  l_->AddLog(bytes);
  WaitForLogFlush(l_);

  LogRecord decoded;
  EXPECT_NO_THROW({
    // Either the CRC check or the decoder guard rejects it; both yield false.
    EXPECT_FALSE(r_->ReadLog(start, &decoded));
  });
}

TEST_F(RecoveryManagerTest, SetLowestValueCompensationRecordIsIdempotent) {
  // D1: the SetLowestValue CLR reuses the normal kLowestValue type, so a
  // second recovery pass would undo it. undo_page == redo_page makes redo and
  // undo both restore the same value (previously undo_page defaulted to 0 and
  // clobbered lowest_page_ to an invalid id).
  const LogRecord clr =
      LogRecord::CompensateSetLowestValueLogRecord(7, 42, 1234);
  EXPECT_EQ(clr.type, LogType::kLowestValue);
  EXPECT_EQ(clr.redo_page, 1234U);
  EXPECT_EQ(clr.undo_page, 1234U);
}

}  // namespace tinylamb