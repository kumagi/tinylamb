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

#include "transaction/transaction.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {

class TransactionTest : public ::testing::Test {
 public:
  void SetUp() override { Reset(); }

  virtual void Reset() {
    tm_.reset();
    lm_.reset();
    pm_.reset();
    l_.reset();
    // Unique log file per run: parallel test suites must not share a WAL.
    l_ = std::make_unique<Logger>("transaction_test-" + RandomString() +
                                  ".log");
    lm_ = std::make_unique<LockManager>();
    // Fixture premise: pm_/recovery_ stay null on purpose.  The tests below
    // only exercise lock/version bookkeeping; any page-accessing method would
    // crash on the null PageManager.
    tm_ = std::make_unique<TransactionManager>(lm_.get(), pm_.get(), l_.get(),
                                               nullptr);
  }

 protected:
  std::unique_ptr<LockManager> lm_;
  std::unique_ptr<PageManager> pm_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<TransactionManager> tm_;
};

TEST_F(TransactionTest, construct) {
  // Arrange -- nothing to set up; default TransactionManager created by SetUp()
  // Act -- nothing to execute; default constructed via SetUp()
  // Assert -- nothing to verify; gtest green on pass, death on crash
}

TEST_F(TransactionTest, UpgradePreservesUnrelatedSharedLocks) {
  const RowPosition upgraded(10, 1);
  const RowPosition preserved(10, 2);
  ASSERT_TRUE(lm_->GetSharedLock(upgraded, 1));
  ASSERT_TRUE(lm_->GetSharedLock(preserved, 2));

  ASSERT_TRUE(lm_->TryUpgradeLock(upgraded, 1));

  // Assert -- a foreign owner can neither release nor re-release the locks
  EXPECT_FALSE(lm_->ReleaseExclusiveLock(upgraded, 3));
  EXPECT_FALSE(lm_->ReleaseSharedLock(preserved, 3));

  EXPECT_TRUE(lm_->ReleaseExclusiveLock(upgraded, 1));
  EXPECT_TRUE(lm_->ReleaseSharedLock(preserved, 2));
}

TEST_F(TransactionTest, UpgradeRequiresSoleSharedOwner) {
  const RowPosition row(10, 1);
  ASSERT_TRUE(lm_->GetSharedLock(row, 1));
  ASSERT_TRUE(lm_->GetSharedLock(row, 2));

  EXPECT_FALSE(lm_->TryUpgradeLock(row, 1));
  EXPECT_FALSE(lm_->TryUpgradeLock(row, 3));

  EXPECT_TRUE(lm_->ReleaseSharedLock(row, 1));
  // Releasing someone else's shared lock is rejected.
  EXPECT_FALSE(lm_->ReleaseSharedLock(row, 1));
  EXPECT_TRUE(lm_->ReleaseSharedLock(row, 2));
}

TEST_F(TransactionTest, TransactionIdsIncrement) {
  // Arrange -- nothing more than fixture setup
  // Act -- begin three sequential transactions
  auto first = tm_->Begin();
  auto second = tm_->Begin();
  auto third = tm_->Begin();

  // Assert -- transaction ids are assigned in strictly increasing order
  ASSERT_EQ(first.ID(), 1U);
  ASSERT_EQ(second.ID(), 2U);
  ASSERT_EQ(third.ID(), 3U);
}

TEST_F(TransactionTest, ReadOnlyTransactionCommit) {
  // Arrange -- nothing more than fixture setup
  // Act -- commit a read-only transaction
  Transaction txn = tm_->Begin(true);
  ASSERT_TRUE(txn.IsReadOnly());
  ASSERT_EQ(txn.PreCommit(), Status::kSuccess);

  // Assert -- the read-only transaction finishes without writing a commit log
  ASSERT_TRUE(txn.IsFinished());
}

TEST_F(TransactionTest, ReadOnlyTransactionAbort) {
  // Arrange -- nothing more than fixture setup
  // Act -- abort a read-only transaction
  Transaction txn = tm_->Begin(true);
  txn.Abort();

  // Assert -- the read-only abort path skips log undo entirely
  ASSERT_TRUE(txn.IsFinished());
}

TEST_F(TransactionTest, MVCCVisibilityChain) {
  // Arrange -- a row position shared by all versions in this test
  const RowPosition rp(1, 1);

  // Act 1 -- writer 1 publishes version "v1"
  Transaction writer = tm_->Begin();
  ASSERT_TRUE(writer.AddWriteSet(rp));
  writer.RegisterVersionWrite(rp, std::nullopt, "v1");
  ASSERT_EQ(writer.PreCommit(), Status::kSuccess);
  writer.CommitWait();

  // Assert 1 -- a snapshot reader started after the commit sees "v1"
  {
    Transaction reader = tm_->Begin(true);
    ASSERT_SUCCESS_AND_EQ(tm_->ReadVersion(reader, rp, std::nullopt), "v1");

    // Act 2 -- writer 2 overwrites with "v2" while the reader's snapshot is open
    Transaction writer2 = tm_->Begin();
    ASSERT_TRUE(writer2.AddWriteSet(rp));
    writer2.RegisterVersionWrite(rp, "v1", "v2");

    // Assert 2 -- committed v1 stays visible; pending writes are not a plan gate
    ASSERT_FALSE(reader.RequiresHistoricalRead());
    ASSERT_SUCCESS_AND_EQ(tm_->ReadVersion(reader, rp, std::nullopt), "v1");
    // Assert 3 -- the writer itself sees its own uncommitted pending version
    ASSERT_SUCCESS_AND_EQ(tm_->ReadVersion(writer2, rp, std::nullopt), "v2");
    ASSERT_EQ(writer2.PreCommit(), Status::kSuccess);
  }

  // Assert 4 -- a fresh reader after both commits sees the newest version
  {
    Transaction reader = tm_->Begin(true);
    ASSERT_SUCCESS_AND_EQ(tm_->ReadVersion(reader, rp, std::nullopt), "v2");
    reader.PreCommit();
  }
}

TEST_F(TransactionTest, ReadVersionDeleteVisibleAsNotExists) {
  // Arrange -- a row position whose committed version is a deletion
  const RowPosition rp(9, 1);
  Transaction writer = tm_->Begin();
  ASSERT_TRUE(writer.AddWriteSet(rp));
  writer.RegisterVersionWrite(rp, "v", std::nullopt);
  ASSERT_EQ(writer.PreCommit(), Status::kSuccess);
  writer.CommitWait();

  // Act -- read the deleted row from a snapshot after the commit
  Transaction reader = tm_->Begin(true);
  auto result = tm_->ReadVersion(reader, rp, std::nullopt);

  // Assert -- a deleted version is visible as kNotExists, not as garbage
  ASSERT_EQ(result.GetStatus(), Status::kNotExists);
  reader.PreCommit();
}

TEST_F(TransactionTest, ReadVersionPhysicalFallback) {
  // Arrange -- a row position with no MVCC version history
  const RowPosition rp(77, 1);
  Transaction txn = tm_->Begin(true);

  // Act -- read it with and without a physical (page) value
  auto with_physical = tm_->ReadVersion(txn, rp, std::string_view("physical"));
  auto without_physical = tm_->ReadVersion(txn, rp, std::nullopt);

  // Assert -- the physical value falls back when no version chain exists
  ASSERT_SUCCESS_AND_EQ(with_physical, "physical");
  ASSERT_EQ(without_physical.GetStatus(), Status::kNotExists);
  txn.PreCommit();
}

TEST_F(TransactionTest, LockManagementThroughManager) {
  // Arrange -- a row position with no locks held yet
  const RowPosition rp(5, 1);

  // Act -- take an exclusive lock through the TransactionManager
  ASSERT_TRUE(tm_->GetExclusiveLock(rp));
  ASSERT_FALSE(tm_->GetExclusiveLock(rp));
  ASSERT_FALSE(tm_->GetSharedLock(rp));
  // Assert -- a foreign owner cannot release it; the owner can
  EXPECT_FALSE(lm_->ReleaseExclusiveLock(rp, 9));
  ASSERT_TRUE(lm_->ReleaseExclusiveLock(rp, 0));

  // Act -- take a shared lock and upgrade it to exclusive
  ASSERT_TRUE(tm_->GetSharedLock(rp));
  ASSERT_TRUE(tm_->TryUpgradeLock(rp));

  // Assert -- the upgraded exclusive lock releases cleanly
  ASSERT_TRUE(lm_->ReleaseExclusiveLock(rp, 0));
}

TEST_F(TransactionTest, CompensateLogsThroughManager) {
  // Arrange -- nothing more than fixture setup
  // Act -- append every flavor of compensating log record via the manager
  tm_->CompensateInsertLog(1, 3, 4);
  tm_->CompensateInsertLog(1, 3, "leaf-key");
  tm_->CompensateInsertBranchLog(1, 3, "branch-key");
  tm_->CompensateUpdateLog(1, 3, 4, "redo");
  tm_->CompensateUpdateLog(1, 3, "leaf-key", "redo");
  tm_->CompensateUpdateBranchLog(1, 3, "branch-key", 5);
  tm_->CompensateDeleteLog(1, 3, 4, "redo");
  tm_->CompensateDeleteLog(1, 3, "leaf-key", "redo");
  tm_->CompensateDeleteBranchLog(1, 3, "branch-key", 5);
  tm_->CompensateSetLowestValueLog(1, 3, 6);
  tm_->CompensateSetLowFenceLog(1, 3, IndexKey("low"));
  tm_->CompensateSetHighFenceLog(1, 3, IndexKey("high"));
  tm_->CompensateSetFosterLog(1, 3, FosterPair("fk", 7));

  // Assert -- implicit; every compensating record was appended without crash
}

TEST_F(TransactionTest, TransactionStatusStreaming) {
  // Arrange -- one value per TransactionStatus enum member
  // Act -- stream each status to a string buffer
  std::ostringstream oss;
  oss << TransactionStatus::kUnknown << "|" << TransactionStatus::kRunning << "|"
      << TransactionStatus::kCommitted << "|" << TransactionStatus::kAborted;
  // Assert -- every status has a documented textual representation
  EXPECT_EQ(oss.str(), "Unknown|Running|Committed|Aborted");
}

TEST_F(TransactionTest, AddWriteSetRejectedOnReadOnlyTransaction) {
  // Arrange -- a read-only transaction and a row position
  Transaction txn = tm_->Begin(true);
  const RowPosition rp(11, 1);
  // Act + Assert -- read-only transactions refuse to join the write set
  EXPECT_FALSE(txn.AddWriteSet(rp));
  EXPECT_TRUE(txn.IsReadOnly());
  ASSERT_EQ(txn.PreCommit(), Status::kSuccess);
}

TEST_F(TransactionTest, AddWriteSetFailsWhenLockHeldElsewhere) {
  // Arrange -- a row position whose exclusive lock is owned outside the txn
  const RowPosition rp(12, 1);
  Transaction txn = tm_->Begin();
  ASSERT_TRUE(lm_->GetExclusiveLock(rp, 42));
  // Act -- try to take the same row into the write set
  EXPECT_FALSE(txn.AddWriteSet(rp));
  // Assert -- the write set stays empty, so pre-commit has nothing to publish
  EXPECT_FALSE(lm_->ReleaseExclusiveLock(rp, txn.ID()));
  EXPECT_TRUE(lm_->ReleaseExclusiveLock(rp, 42));
  ASSERT_EQ(txn.PreCommit(), Status::kSuccess);
}

TEST_F(TransactionTest, DefaultTransactionReadVersionWithoutManager) {
  // Arrange -- a default-constructed Transaction (no TransactionManager)
  Transaction txn;
  const RowPosition rp(13, 1);
  // Act -- read with and without a physical fallback value
  auto without_physical = txn.ReadVersion(rp, std::nullopt);
  auto with_physical = txn.ReadVersion(rp, std::string_view("fallback"));
  // Assert -- no manager means no MVCC chain: physical or kNotExists
  EXPECT_EQ(without_physical.GetStatus(), Status::kNotExists);
  ASSERT_SUCCESS_AND_EQ(with_physical, "fallback");
}

TEST_F(TransactionTest, DefaultTransactionRegisterVersionWriteIsNoop) {
  // Arrange -- a default-constructed Transaction (no TransactionManager)
  Transaction txn;
  const RowPosition rp(13, 2);
  // Act + Assert -- version writes without a manager are silently ignored
  EXPECT_NO_FATAL_FAILURE(
      txn.RegisterVersionWrite(rp, std::nullopt, std::string_view("after")));
  EXPECT_NO_FATAL_FAILURE(
      txn.RegisterVersionWrite(rp, std::string_view("before"), std::nullopt));
}

TEST_F(TransactionTest, CrossWorkerReadSeesOwnUncommittedWrite) {
  // Arrange -- a write transaction with an uncommitted version registered by
  // worker A (this thread).
  const RowPosition rp(31, 1);
  Transaction txn = tm_->Begin();
  ASSERT_TRUE(txn.AddWriteSet(rp));
  txn.RegisterVersionWrite(rp, std::nullopt, "pending");
  EXPECT_EQ(txn.WriteEpoch(), 1U);

  // Act -- worker B (a different thread under the SAME transaction) re-reads
  // the row through its own cache shard.
  std::string seen;
  std::thread worker([&] {
    auto value = txn.ReadVersion(rp, std::nullopt);
    ASSERT_TRUE(value.HasValue());
    seen = std::string(value.Value());
  });
  worker.join();

  // Assert -- read-your-writes holds across workers: the pending value is
  // served, never a stale pre-write copy from another shard.
  EXPECT_EQ(seen, "pending");
}

TEST_F(TransactionTest, WriteEpochBumpsPerVersionWrite) {
  const RowPosition rp(32, 1);
  Transaction txn = tm_->Begin();
  ASSERT_TRUE(txn.AddWriteSet(rp));
  const uint64_t before = txn.WriteEpoch();
  txn.RegisterVersionWrite(rp, std::nullopt, "a");
  EXPECT_EQ(txn.WriteEpoch(), before + 1);
  txn.RegisterVersionWrite(rp, "a", "b");
  EXPECT_EQ(txn.WriteEpoch(), before + 2);

  // A same-thread read after the writes observes the newest pending value.
  auto value = txn.ReadVersion(rp, std::nullopt);
  ASSERT_SUCCESS_AND_EQ(value, "b");
  ASSERT_EQ(txn.PreCommit(), Status::kSuccess);
}

TEST_F(TransactionTest, ReadOnlyCacheHitServesStableSnapshot) {
  // Arrange -- one committed version, then a snapshot reader.
  const RowPosition rp(34, 1);
  Transaction writer = tm_->Begin();
  ASSERT_TRUE(writer.AddWriteSet(rp));
  writer.RegisterVersionWrite(rp, std::nullopt, "v1");
  ASSERT_EQ(writer.PreCommit(), Status::kSuccess);
  writer.CommitWait();

  Transaction reader = tm_->Begin(true);
  auto first = reader.ReadVersion(rp, std::nullopt);
  ASSERT_SUCCESS_AND_EQ(first, "v1");
  // Act -- a repeat read is served from this thread's cache shard...
  auto second = reader.ReadVersion(rp, std::nullopt);
  ASSERT_SUCCESS_AND_EQ(second, "v1");

  // ...even after another transaction commits a newer version: the reader's
  // fixed snapshot keeps seeing v1 through the cache-hit path.
  {
    Transaction writer2 = tm_->Begin();
    ASSERT_TRUE(writer2.AddWriteSet(rp));
    writer2.RegisterVersionWrite(rp, "v1", "v2");
    ASSERT_EQ(writer2.PreCommit(), Status::kSuccess);
    writer2.CommitWait();
  }
  auto third = reader.ReadVersion(rp, std::nullopt);
  ASSERT_SUCCESS_AND_EQ(third, "v1");
}

TEST_F(TransactionTest, PreCommitLoggerFailureLeavesAbortedNotHalfCommitted) {
  const RowPosition rp(33, 1);
  Transaction txn = tm_->Begin();
  ASSERT_TRUE(txn.AddWriteSet(rp));
  txn.RegisterVersionWrite(rp, std::nullopt, "doomed");

  // Point the WAL fd at /dev/full so the commit record can never persist and
  // WaitForDurable must surface a write error.
  const int full_fd = ::open("/dev/full", O_WRONLY);
  ASSERT_GE(full_fd, 0);
  ASSERT_EQ(::dup2(full_fd, l_->Fd()), l_->Fd());
  ASSERT_EQ(::close(full_fd), 0);

  EXPECT_THROW(txn.PreCommit(), std::runtime_error);

  // No half-finished state: locks were released (a foreign owner can take
  // the row) and the transaction reports finished instead of staying
  // committed-with-locks-forever.
  EXPECT_TRUE(txn.IsFinished());
  EXPECT_TRUE(lm_->GetExclusiveLock(rp, 4242));
  EXPECT_TRUE(lm_->ReleaseExclusiveLock(rp, 4242));
}

TEST(TransactionManagerTest, AbortedWriteIsNotVisibleToLaterReaders) {
  // A writer's pending version write must be rolled back on Abort() so a
  // later reader never observes the aborted value.  A write transaction logs
  // its Begin record, so the manager needs a real RecoveryManager (the shared
  // TransactionTest fixture intentionally runs without one).
  const std::string db_name = "abort_txn-test-" + RandomString() + ".db";
  const std::string log_name = "abort_txn-test-" + RandomString() + ".log";
  {
    PageManager pm(db_name, 10);
    Logger logger(log_name);
    LockManager lm;
    RecoveryManager rm(log_name, pm.GetPool());
    TransactionManager tm(&lm, &pm, &logger, &rm);

    const RowPosition rp(1, 1);
    Transaction writer = tm.Begin();
    ASSERT_TRUE(writer.AddWriteSet(rp));
    writer.RegisterVersionWrite(rp, std::nullopt, "ghost");
    writer.Abort();
    ASSERT_TRUE(writer.IsFinished());

    Transaction reader = tm.Begin(true);
    ASSERT_EQ(tm.ReadVersion(reader, rp, std::nullopt).GetStatus(),
              Status::kNotExists);
  }
  std::ignore = std::remove(db_name.c_str());
  std::ignore = std::remove(log_name.c_str());
}

}  // namespace tinylamb
