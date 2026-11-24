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

#include <barrier>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <ranges>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/memory_budget.hpp"
#include "common/platform/file_io.hpp"
#include "common/platform/process.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "page/page_type.hpp"
#include "recovery/log_record.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"
#include "transaction/transaction_manager.hpp"

namespace tinylamb {
// A write set can live on disk, so enumeration must expose I/O failures.
static_assert(!std::ranges::range<TransactionWriteSet>);
static_assert(!std::ranges::range<const TransactionWriteSet>);

class TransactionTest : public ::testing::Test {
 public:
  void SetUp() override { Reset(); }

  virtual void Reset() {
    tm_.reset();
    pm_.reset();
    l_.reset();
    if (!log_name_.empty()) {
      tinylamb::RemoveLogAndSegments(log_name_);
      log_name_.clear();
    }
    // Unique log file per run: parallel test suites must not share a WAL.
    log_name_ = "transaction_test-" + RandomString() + ".log";
    l_ = Logger::Create(log_name_).MoveValue();
    // Fixture premise: pm_/recovery_ stay null on purpose.  The tests below
    // only exercise lock/version bookkeeping; any page-accessing method would
    // crash on the null PageManager.
    tm_ = std::make_unique<TransactionManager>(pm_.get(), l_.get(), nullptr);
  }

  void TearDown() override {
    tm_.reset();
    pm_.reset();
    l_.reset();
    if (!log_name_.empty()) {
      tinylamb::RemoveLogAndSegments(log_name_);
      log_name_.clear();
    }
  }

 protected:
  std::string log_name_;
  std::unique_ptr<PageManager> pm_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<TransactionManager> tm_;
};

namespace {
class RecordingFinishHook final : public TransactionFinishHook {
 public:
  void OnTransactionFinished(TransactionStatus status) noexcept override {
    status_ = status;
    calls_++;
  }

  TransactionStatus status_{TransactionStatus::kUnknown};
  size_t calls_{0};
};
}  // namespace

// Round 9 regression: the index-undo reservation drain is the wait every
// the LSM index insert parks when a split's bytes are still reserved. A wounded
// waiter (deadlock-detector victim) must bail at the first slice -- its
// abort releases the very reservations everyone is waiting for -- instead
// of riding the full write-intent limit while holding its own row intents.
TEST_F(TransactionTest, ReservationDrainBailsWhenWounded) {
  Transaction holder = tm_->Begin();
  ASSERT_SUCCESS(holder.ReserveIndexUndoBytes(1234, 512));
  Transaction waiter = tm_->Begin();
  waiter.Wound();
  const auto start = std::chrono::steady_clock::now();
  const Status drained =
      waiter.WaitAllIndexReservationsDrain(std::chrono::seconds(30));
  const auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_EQ(drained, Status::kConflicts);
  EXPECT_LT(elapsed, std::chrono::seconds(5));
  ASSERT_SUCCESS(tm_->Abort(holder));
}

// The drain honors an explicit short budget: LSM index ops pass one wherever
// the caller may hold an index write gate, because a multi-second wait under
// the gate deadlocks against a reservation owner whose next index op needs
// the same gate. Past the budget the honest kConflicts surfaces.
TEST_F(TransactionTest, ReservationDrainHonorsShortBudget) {
  Transaction holder = tm_->Begin();
  ASSERT_SUCCESS(holder.ReserveIndexUndoBytes(4321, 512));
  Transaction waiter = tm_->Begin();
  const auto start = std::chrono::steady_clock::now();
  const Status drained =
      waiter.WaitAllIndexReservationsDrain(std::chrono::milliseconds(150));
  const auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_EQ(drained, Status::kConflicts);
  EXPECT_LT(elapsed, std::chrono::seconds(5));
  ASSERT_SUCCESS(tm_->Abort(holder));
}

TEST_F(TransactionTest, Construct) {
  // SetUp() default-constructs the manager; verify it can hand out a usable
  // transaction rather than relying on "no crash" alone.
  ASSERT_NE(tm_, nullptr);
  Transaction txn = tm_->Begin();
  EXPECT_NE(txn.ID(), 0U);
  EXPECT_EQ(txn.PreCommit(), Status::kSuccess);
}

TEST(TransactionFinishHookTest, ConcurrentFinalizationRunsHookOnce) {
  class Counter final : public TransactionFinishHook {
   public:
    void OnTransactionFinished(TransactionStatus /*status*/) noexcept override {
      ++calls;
    }
    std::atomic<int> calls{0};
  };
  auto hook = std::make_shared<Counter>();
  Transaction txn;
  txn.AddFinishHook(hook);
  txn.SetStatus(TransactionStatus::kCommitted);
  std::vector<std::thread> threads;
  threads.reserve(16);
  for (int i = 0; i < 16; ++i) {
    threads.emplace_back([&] { txn.FinalizeFinishHooks(); });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(hook->calls.load(), 1);
}

TEST(TransactionFinishHookTest, RegistrationRacingFinalizationRunsEachHook) {
  Transaction txn;
  txn.SetStatus(TransactionStatus::kCommitted);
  std::vector<std::shared_ptr<RecordingFinishHook>> hooks;
  hooks.reserve(64);
  for (int i = 0; i < 64; ++i) {
    hooks.push_back(std::make_shared<RecordingFinishHook>());
  }
  std::barrier start(2);
  {
    std::jthread finish([&] {
      start.arrive_and_wait();
      txn.FinalizeFinishHooks();
    });
    start.arrive_and_wait();
    for (const auto& hook : hooks) {
      txn.AddFinishHook(hook);
    }
  }
  const auto late = std::make_shared<RecordingFinishHook>();
  txn.AddFinishHook(late);
  hooks.push_back(late);
  for (const auto& hook : hooks) {
    EXPECT_EQ(hook->calls_, 1U);
    EXPECT_EQ(hook->status_, TransactionStatus::kCommitted);
  }
}

TEST_F(TransactionTest, FinishHooksRunOnCommitAbortAndMove) {
  auto committed_hook = std::make_shared<RecordingFinishHook>();
  Transaction committed = tm_->Begin();
  committed.AddFinishHook(committed_hook);
  ASSERT_EQ(committed.PreCommit(), Status::kSuccess);
  EXPECT_EQ(committed_hook->calls_, 1U);
  EXPECT_EQ(committed_hook->status_, TransactionStatus::kCommitted);

  auto aborted_hook = std::make_shared<RecordingFinishHook>();
  Transaction aborted = tm_->Begin();
  aborted.AddFinishHook(aborted_hook);
  ASSERT_EQ(tm_->Abort(aborted), Status::kSuccess);
  EXPECT_EQ(aborted_hook->calls_, 1U);
  EXPECT_EQ(aborted_hook->status_, TransactionStatus::kAborted);

  auto moved_hook = std::make_shared<RecordingFinishHook>();
  Transaction source = tm_->Begin();
  source.AddFinishHook(moved_hook);
  Transaction moved(std::move(source));
  ASSERT_EQ(moved.PreCommit(), Status::kSuccess);
  EXPECT_EQ(moved_hook->calls_, 1U);
  EXPECT_EQ(moved_hook->status_, TransactionStatus::kCommitted);
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

// Long-uptime gauges: an operator watching RuntimeStats must see the MVCC
// chain backlog and the live-transaction count move so a GC-pinning runaway
// transaction is diagnosable before memory becomes a problem.
TEST_F(TransactionTest, RuntimeStatsReportsGaugesForLongUptime) {
  tm_->SetMetricsEnabled(true);
  const RowPosition row(7, 1);
  Transaction publisher = tm_->Begin();
  ASSERT_TRUE(publisher.AddWriteSet(row, "before"));
  publisher.RegisterVersionWrite(row, "before", "after");
  ASSERT_EQ(publisher.PreCommit(), Status::kSuccess);

  Transaction writer = tm_->Begin();
  Transaction reader = tm_->Begin();
  const TransactionRuntimeStats stats = tm_->RuntimeStats();
  EXPECT_EQ(stats.active_transactions, 2U);
  // The committed chain from |publisher| (GC runs asynchronously and only
  // after kGcCommitThreshold commits, so it is still retained here).
  EXPECT_GE(stats.version_chains, 1U);
  EXPECT_GE(stats.current_commit_ts, 1U);
  EXPECT_NE(stats.oldest_snapshot_ts, 0U);
  EXPECT_LE(stats.oldest_snapshot_ts, stats.current_commit_ts);
  // publisher.AddWriteSet(row, "before") above must register a measured
  // attempt.
  EXPECT_GE(stats.write_intent_attempts, 1U);

  ASSERT_EQ(tm_->Abort(writer), Status::kSuccess);
  ASSERT_EQ(reader.PreCommit(), Status::kSuccess);
  EXPECT_EQ(tm_->RuntimeStats().active_transactions, 0U);
}

// RuntimeStats is an observability entry point that callers may invoke even
// on a TransactionManager constructed without a logger (WAL-less setups).
// It must report zero WAL gauges rather than dereferencing a null logger_.
TEST_F(TransactionTest, RuntimeStatsWithoutLoggerReportsZeroWalGauges) {
  TransactionManager no_logger(pm_.get(), nullptr, nullptr);
  const TransactionRuntimeStats stats = no_logger.RuntimeStats();
  EXPECT_EQ(stats.wal_sync_count, 0U);
  EXPECT_EQ(stats.wal_sync_ns, 0U);
  EXPECT_EQ(stats.wal_buffered_bytes, 0U);
  EXPECT_EQ(stats.wal_durable_bytes, 0U);
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

TEST_F(TransactionTest, WalScratchReuseKeepsCommitBytesExact) {
  Transaction txn = tm_->Begin();
  const std::string first_payload(1024, 'a');
  const std::string second_payload("b\0c", 3);
  const LogRecord first =
      LogRecord::InsertingLogRecord(0, txn.ID(), 1, 2, first_payload);
  const lsn_t first_start = txn.InsertLog(1, 2, first_payload).Value();
  const lsn_t second_start = txn.InsertLog(3, 4, second_payload).Value();
  ASSERT_EQ(first_start, 0U);
  ASSERT_EQ(second_start, first_start + first.Serialize().size());
  ASSERT_EQ(txn.PreCommit(), Status::kSuccess);
  ASSERT_EQ(l_->WaitForDurable(l_->BufferedLSN()), Status::kSuccess);

  const LogRecord second = LogRecord::InsertingLogRecord(first_start, txn.ID(),
                                                         3, 4, second_payload);
  const LogRecord commit(second_start, txn.ID(), LogType::kCommit);
  const std::string expected =
      first.Serialize() + second.Serialize() + commit.Serialize();

  std::ifstream file(log_name_, std::ios::binary);
  std::ostringstream contents;
  contents << file.rdbuf();
  EXPECT_EQ(contents.str(), expected);
}

TEST_F(TransactionTest, FirstLsnRemainsAtOldestRecord) {
  Transaction txn = tm_->Begin();
  const lsn_t first = txn.InsertLog(1, 0, "first").Value();
  const lsn_t second = txn.InsertLog(1, 1, "second").Value();
  EXPECT_EQ(txn.FirstLSN(), first);
  EXPECT_NE(txn.PrevLSN(), first);
  EXPECT_EQ(
      second,
      first + LogRecord::InsertingLogRecord(0, txn.ID(), 1, 0, "first").Size());
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(TransactionTest, FinishedTransactionCannotAppendWal) {
  Transaction txn = tm_->Begin();
  ASSERT_SUCCESS(txn.PreCommit());
  EXPECT_EQ(txn.InsertLog(1, 0, "forbidden").GetStatus(),
            Status::kRuntimeError);
}

TEST_F(TransactionTest, FinishedTransactionCannotPreCommitAgain) {
  Transaction txn = tm_->Begin();
  ASSERT_SUCCESS(txn.Abort());
  EXPECT_EQ(txn.PreCommit(), Status::kRuntimeError);
}

TEST_F(TransactionTest, WoundedTransactionCannotCommit) {
  Transaction txn = tm_->Begin();
  txn.Wound();
  EXPECT_EQ(txn.PreCommit(), Status::kConflicts);
  EXPECT_TRUE(txn.IsFinished());
}

TEST_F(TransactionTest, ReadOnlyTransactionCannotAppendWal) {
  Transaction txn = tm_->Begin(true);
  EXPECT_EQ(txn.InsertLog(1, 0, "forbidden").GetStatus(),
            Status::kInvalidArgument);
  EXPECT_FALSE(txn.HasLogRecord());
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(TransactionTest, ReadOnlyTransactionAbort) {
  // Arrange -- nothing more than fixture setup
  // Act -- abort a read-only transaction
  Transaction txn = tm_->Begin(true);
  ASSERT_SUCCESS(txn.Abort());

  // Assert -- the read-only abort path skips log undo entirely
  ASSERT_TRUE(txn.IsFinished());
}

TEST_F(TransactionTest, MvccVisibilityChain) {
  // Arrange -- a row position shared by all versions in this test
  const RowPosition rp(1, 1);

  // Act 1 -- writer 1 publishes version "v1"
  Transaction writer = tm_->Begin();
  ASSERT_TRUE(writer.AddWriteSet(rp));
  writer.RegisterVersionWrite(rp, std::nullopt, "v1");
  ASSERT_EQ(writer.PreCommit(), Status::kSuccess);
  ASSERT_SUCCESS(writer.CommitWait());

  // Assert 1 -- a snapshot reader started after the commit sees "v1"
  {
    Transaction reader = tm_->Begin(true);
    ASSERT_SUCCESS_AND_EQ(tm_->ReadVersion(reader, rp, std::nullopt), "v1");

    // Act 2 -- writer 2 overwrites with "v2" while the reader's snapshot is
    // open
    Transaction writer2 = tm_->Begin();
    ASSERT_TRUE(writer2.AddWriteSet(rp));
    writer2.RegisterVersionWrite(rp, "v1", "v2");

    // Assert 2 -- committed v1 stays visible; pending writes are not a plan
    // gate
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
    ASSERT_SUCCESS(reader.PreCommit());
  }
}

TEST_F(TransactionTest, ReadVersionDeleteVisibleAsNotExists) {
  // Arrange -- a row position whose committed version is a deletion
  const RowPosition rp(9, 1);
  Transaction writer = tm_->Begin();
  ASSERT_TRUE(writer.AddWriteSet(rp));
  writer.RegisterVersionWrite(rp, "v", std::nullopt);
  ASSERT_EQ(writer.PreCommit(), Status::kSuccess);
  ASSERT_SUCCESS(writer.CommitWait());

  // Act -- read the deleted row from a snapshot after the commit
  Transaction reader = tm_->Begin(true);
  auto result = tm_->ReadVersion(reader, rp, std::nullopt);

  // Assert -- a deleted version is visible as kNotExists, not as garbage
  ASSERT_EQ(result.GetStatus(), Status::kNotExists);
  ASSERT_SUCCESS(reader.PreCommit());
}

TEST_F(TransactionTest, ReadVersionPhysicalFallback) {
  // Arrange -- a row position with no MVCC version history
  const RowPosition rp(77, 1);
  Transaction txn = tm_->Begin(true);

  // Act -- read it with and without a physical (page) value
  auto with_physical = tm_->ReadVersion(txn, rp, std::string_view("physical"));
  auto without_physical = tm_->ReadVersion(txn, rp, std::nullopt);

  // Assert -- with no version chain the manager reports nullopt: the
  // latched page image is authoritative and the caller serves a view of
  // it (no per-read copy).
  ASSERT_SUCCESS_AND_EQ(with_physical, std::optional<std::string>());
  ASSERT_EQ(without_physical.GetStatus(), Status::kNotExists);
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(TransactionTest, GcDropsRedundantLatestVersionBeforeDeleteIntentRead) {
  const RowPosition rp(78, 1);
  {
    Transaction writer = tm_->Begin();
    ASSERT_TRUE(writer.AddWriteSet(rp));
    writer.RegisterVersionWrite(rp, std::nullopt, "physical");
    ASSERT_EQ(writer.PreCommit(), Status::kSuccess);
    ASSERT_SUCCESS(writer.CommitWait());
  }

  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (tm_->HasVersionChain(rp) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  ASSERT_FALSE(tm_->HasVersionChain(rp));

  // Delete reserves an unstaged write intent before it asks MVCC for the
  // before-image. With the redundant committed chain collected, the owner
  // must still see the authoritative physical row.
  Transaction deleting = tm_->Begin();
  ASSERT_TRUE(deleting.AddWriteSet(rp));
  ASSERT_SUCCESS_AND_EQ(
      tm_->ReadVersion(deleting, rp, std::string_view("physical")),
      std::optional<std::string>());
  ASSERT_SUCCESS(deleting.Abort());
}

TEST_F(TransactionTest, MvccWriteIntentCanFollowACommittedWriter) {
  const RowPosition rp(5, 1);
  Transaction first = tm_->Begin();
  Transaction concurrent = tm_->Begin();
  ASSERT_TRUE(first.AddWriteSet(rp));
  EXPECT_FALSE(concurrent.AddWriteSet(rp));
  first.RegisterVersionWrite(rp, "old", "new");
  ASSERT_EQ(first.PreCommit(), Status::kSuccess);

  // Strict write locking lets the concurrently-started transaction proceed
  // once the former owner commits; DML scans then resolve the predecessor's
  // newest committed image under this unstaged intent, not their old Begin
  // snapshot.
  EXPECT_TRUE(concurrent.AddWriteSet(rp));
  ASSERT_SUCCESS_AND_EQ(
      tm_->ReadVersion(concurrent, rp, std::string_view("new")), "new");
  EXPECT_EQ(tinylamb::Status::kSuccess, concurrent.Abort());

  Transaction fresh = tm_->Begin();
  EXPECT_TRUE(fresh.AddWriteSet(rp));
  EXPECT_EQ(tinylamb::Status::kSuccess, fresh.Abort());
}

TEST_F(TransactionTest, CompensateLogsThroughManager) {
  ASSERT_TRUE(tm_->CompensateUpdateLog(1, 3, 4, "redo").HasValue());
}

TEST_F(TransactionTest, TransactionStatusStreaming) {
  // Arrange -- one value per TransactionStatus enum member
  // Act -- stream each status to a string buffer
  std::ostringstream oss;
  oss << TransactionStatus::kUnknown << "|" << TransactionStatus::kRunning
      << "|" << TransactionStatus::kCommitted << "|"
      << TransactionStatus::kAborted;
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

TEST_F(TransactionTest, AddWriteSetFailsWhenMvccIntentHeldElsewhere) {
  const RowPosition rp(12, 1);
  Transaction owner = tm_->Begin();
  Transaction contender = tm_->Begin();
  ASSERT_TRUE(owner.AddWriteSet(rp));
  EXPECT_FALSE(contender.AddWriteSet(rp));
  ASSERT_SUCCESS(owner.Abort());
  EXPECT_TRUE(contender.AddWriteSet(rp));
  ASSERT_SUCCESS(contender.Abort());
}

TEST_F(TransactionTest, AuditReservationModeKeepsRegistrationWithoutDiscount) {
  const char* mode =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_INDEX_RESERVATION");
  const bool discount_off = mode != nullptr && std::string_view(mode) == "4";
  if (mode != nullptr && std::string_view(mode) != "0" && !discount_off) {
    GTEST_SKIP() << "TINYLAMB_DISABLE_INDEX_RESERVATION is set to an "
                    "unrecognized value; audit mode is not active";
  }
  Transaction owner = tm_->Begin();
  Transaction other = tm_->Begin();
  ASSERT_SUCCESS(owner.ReserveIndexUndoBytes(132, 64));
  EXPECT_TRUE(tm_->HasForeignIndexReservations(other.ID()));
  EXPECT_EQ(other.IndexUndoReservedBytes(132), discount_off ? 0U : 64U);
  EXPECT_EQ(owner.IndexUndoReservedBytes(132), 0U);
  ASSERT_SUCCESS(owner.Abort());
  EXPECT_FALSE(tm_->HasForeignIndexReservations(other.ID()));
}

TEST_F(TransactionTest, AuditWaitFailuresCarrySpecificDiagnostics) {
  const RowPosition rp(131, 1);
  Transaction done = tm_->Begin();
  ASSERT_SUCCESS(done.PreCommit());
  EXPECT_EQ(tm_->WaitForRowWriteIntent(done, rp), Status::kConflicts);
  EXPECT_EQ(done.LastIntentWaitFailure(),
            Transaction::IntentWaitFailure::kConflict);
  Transaction wounded = tm_->Begin();
  wounded.Wound();
  EXPECT_EQ(tm_->WaitForRowWriteIntent(wounded, rp), Status::kConflicts);
  EXPECT_EQ(wounded.LastIntentWaitFailure(),
            Transaction::IntentWaitFailure::kWounded);
  tm_->SetDeadlockPolicy(TransactionManager::DeadlockPolicy::kNoWait);
  Transaction no_wait = tm_->Begin();
  EXPECT_EQ(tm_->WaitForRowWriteIntent(no_wait, rp), Status::kConflicts);
  EXPECT_EQ(no_wait.LastIntentWaitFailure(),
            Transaction::IntentWaitFailure::kConflict);
}

TEST_F(TransactionTest, AuditWoundedReservationCannotBypassAcquisition) {
  const RowPosition rp(130, 1);
  Transaction owner = tm_->Begin();
  ASSERT_TRUE(owner.TryAddWriteSet(rp));
  owner.Wound();
  EXPECT_FALSE(owner.AddWriteSet(rp, "before"));
  EXPECT_FALSE(owner.AddWriteSet(rp));
  EXPECT_FALSE(owner.TryAddWriteSet(rp));
  ASSERT_SUCCESS(owner.Abort());
}

TEST_F(TransactionTest, AuditDefaultTransactionIntentApisAreSafe) {
  Transaction txn;
  const RowPosition rp(130, 2);
  EXPECT_FALSE(txn.SnapshotSeesRow(rp));
  EXPECT_FALSE(txn.AddWriteSet(rp));
  EXPECT_FALSE(txn.AddWriteSet(rp, "before"));
  EXPECT_FALSE(txn.TryAddWriteSet(rp));
  EXPECT_NO_FATAL_FAILURE(txn.ReleaseWriteIntent(rp));
  EXPECT_EQ(txn.CommitWait(), Status::kSuccess);
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

TEST_F(TransactionTest, FinishedTransactionRejectsReadAndWriteOperations) {
  const RowPosition rp(130, 3);
  for (const bool commit : {true, false}) {
    auto txn = tm_->Begin();
    if (commit) {
      ASSERT_SUCCESS(txn.PreCommit());
    } else {
      ASSERT_SUCCESS(txn.Abort());
    }
    ASSERT_TRUE(txn.IsFinished());
    EXPECT_FALSE(txn.AddReadSet(rp));
    EXPECT_FALSE(txn.ReadVersion(rp, std::string_view("physical")).HasValue());
    EXPECT_FALSE(txn.AddWriteSet(rp));
    EXPECT_FALSE(txn.AddWriteSet(rp, "before"));
    EXPECT_FALSE(txn.TryAddWriteSet(rp));
  }
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

TEST_F(TransactionTest, CancelStagedVersionReleasesPayloadAccounting) {
  // Canceling a staged version (e.g. after an InsertLog/UpdateLog failure)
  // drops the write-set entry. It must also give back the resident payload
  // bytes that RegisterVersionWrite charged, or repeated failed writes leak.
  const RowPosition rp(14, 2);
  const uint64_t baseline = tm_->RuntimeStats().version_payload_resident_bytes;
  Transaction txn = tm_->Begin();
  ASSERT_TRUE(txn.AddWriteSet(rp));
  txn.RegisterVersionWrite(rp, std::nullopt, "staged-payload-value");
  const uint64_t staged = tm_->RuntimeStats().version_payload_resident_bytes;
  ASSERT_GT(staged, baseline);

  ASSERT_TRUE(txn.CancelStagedVersion(rp));
  EXPECT_EQ(tm_->RuntimeStats().version_payload_resident_bytes, baseline);
  ASSERT_EQ(txn.PreCommit(), Status::kSuccess);
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
  ASSERT_SUCCESS(writer.CommitWait());

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
    ASSERT_SUCCESS(writer2.CommitWait());
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
  const int full_fd = platform::OpenWriteFailingDescriptor();
  ASSERT_GE(full_fd, 0);
  ASSERT_EQ(platform::Dup2(full_fd, l_->Fd()), l_->Fd());
  ASSERT_EQ(platform::CloseFile(full_fd), 0);

  EXPECT_NE(txn.PreCommit(), Status::kSuccess);

  // No half-finished state: the transaction reports finished and its MVCC
  // write intent is released even though WAL durability failed.
  EXPECT_TRUE(txn.IsFinished());
  Transaction retry = tm_->Begin();
  EXPECT_TRUE(retry.AddWriteSet(rp));
}

TEST(TransactionManagerTest, AbortedWriteIsNotVisibleToLaterReaders) {
  // A writer's pending version write must be rolled back on Abort() so a
  // later reader never observes the aborted value.  A write transaction logs
  // its Begin record, so the manager needs a real RecoveryManager (the shared
  // TransactionTest fixture intentionally runs without one).
  const std::string db_name = "abort_txn-test-" + RandomString() + ".db";
  const std::string log_name = "abort_txn-test-" + RandomString() + ".log";
  {
    auto pm_holder = PageManager::Create(db_name, 10).MoveValue();
    CHECK(pm_holder != nullptr);
    PageManager& pm = *pm_holder;
    auto logger_holder = Logger::Create(log_name).MoveValue();
    CHECK(logger_holder != nullptr);
    Logger& logger = *logger_holder;
    RecoveryManager rm(log_name, pm.GetPool());
    TransactionManager tm(&pm, &logger, &rm);

    const RowPosition rp(1, 1);
    Transaction writer = tm.Begin();
    ASSERT_TRUE(writer.AddWriteSet(rp));
    writer.RegisterVersionWrite(rp, std::nullopt, "ghost");
    ASSERT_SUCCESS(writer.Abort());
    ASSERT_TRUE(writer.IsFinished());

    Transaction reader = tm.Begin(true);
    ASSERT_EQ(tm.ReadVersion(reader, rp, std::nullopt).GetStatus(),
              Status::kNotExists);
  }
  std::ignore = std::remove(db_name.c_str());
  RemoveLogAndSegments(log_name);
}

TEST(TransactionManagerTest, ReaderSeesRowWhileWriterHoldsUnstagedIntent) {
  // PRODUCTION BUG (fixed): between AddWriteSet() and RegisterVersionWrite()
  // the chain had no committed version, so every OTHER reader received
  // kNotExists for a row that still exists -- a phantom disappearance.
  // RowPage::Update/Delete now install the before-image as the base
  // committed version while reserving the intent.
  const std::string db_name = "intent_window-test-" + RandomString() + ".db";
  const std::string log_name = "intent_window-test-" + RandomString() + ".log";
  {
    auto pm_holder = PageManager::Create(db_name, 10).MoveValue();
    CHECK(pm_holder != nullptr);
    PageManager& pm = *pm_holder;
    auto logger_holder = Logger::Create(log_name).MoveValue();
    CHECK(logger_holder != nullptr);
    Logger& logger = *logger_holder;
    RecoveryManager rm(log_name, pm.GetPool());
    TransactionManager tm(&pm, &logger, &rm);

    const RowPosition rp(2, 1);
    // Seed a committed base version the way RegisterVersionWrite does.
    Transaction seeder = tm.Begin();
    ASSERT_TRUE(seeder.AddWriteSet(rp));
    seeder.RegisterVersionWrite(rp, std::nullopt, "old");
    ASSERT_EQ(seeder.PreCommit(), Status::kSuccess);

    // A writer reserves its intent but has not staged a value yet.
    Transaction writer = tm.Begin();
    ASSERT_TRUE(writer.AddWriteSet(rp, std::string_view("old")));

    // A concurrent reader (older snapshot) must still see the old row.
    Transaction reader = tm.Begin();
    ASSERT_SUCCESS_AND_EQ(tm.ReadVersion(reader, rp, std::nullopt), "old");
    ASSERT_SUCCESS(reader.Abort());
    ASSERT_SUCCESS(writer.Abort());
  }
  std::ignore = std::remove(db_name.c_str());
  RemoveLogAndSegments(log_name);
}

TEST(TransactionManagerTest, AbortAfterPreCommitIsNoOp) {
  // PRODUCTION BUG (fixed): Abort() lacked an IsFinished guard, so calling it
  // after a successful PreCommit ran the undo walk and physically rolled
  // back already-committed writes.
  const std::string db_name =
      "abort_after_commit-test-" + RandomString() + ".db";
  const std::string log_name =
      "abort_after_commit-test-" + RandomString() + ".log";
  {
    auto pm_holder = PageManager::Create(db_name, 10).MoveValue();
    CHECK(pm_holder != nullptr);
    PageManager& pm = *pm_holder;
    auto logger_holder = Logger::Create(log_name).MoveValue();
    CHECK(logger_holder != nullptr);
    Logger& logger = *logger_holder;
    RecoveryManager rm(log_name, pm.GetPool());
    TransactionManager tm(&pm, &logger, &rm);

    const RowPosition rp(3, 1);
    Transaction writer = tm.Begin();
    ASSERT_TRUE(writer.AddWriteSet(rp));
    writer.RegisterVersionWrite(rp, std::nullopt, "committed-value");
    ASSERT_EQ(writer.PreCommit(), Status::kSuccess);
    ASSERT_TRUE(writer.IsFinished());

    // Must not roll back the committed write.
    EXPECT_EQ(tinylamb::Status::kSuccess, writer.Abort());
    ASSERT_TRUE(writer.IsFinished());

    Transaction reader = tm.Begin(true);
    ASSERT_SUCCESS_AND_EQ(tm.ReadVersion(reader, rp, std::nullopt),
                          "committed-value");
  }
  std::ignore = std::remove(db_name.c_str());
  RemoveLogAndSegments(log_name);
}

TEST_F(TransactionTest, WoundFlagSurvivesMove) {
  // PRODUCTION BUG (fixed): the move constructor/assignment dropped the
  // wounded_ flag, so a Wound-Wait victim that was moved escaped its
  // preemption and kept running with its write intents.
  Transaction victim = tm_->Begin();
  victim.Wound();
  ASSERT_TRUE(victim.IsWounded());

  Transaction moved = std::move(victim);
  EXPECT_TRUE(moved.IsWounded());
  EXPECT_EQ(tinylamb::Status::kSuccess, moved.Abort());
}

}  // namespace tinylamb

namespace tinylamb {
TEST_F(TransactionTest, ConcurrentExplicitGcPreservesPinnedVersions) {
  constexpr uint64_t kRows = 10000;
  auto old = tm_->Begin(true);
  {
    auto writer = tm_->Begin();
    for (uint64_t i = 0; i < kRows; ++i) {
      const RowPosition position(i + 100, 0);
      ASSERT_TRUE(writer.AddWriteSet(position, "before"));
      writer.RegisterVersionWrite(position, "before", "after");
    }
    ASSERT_SUCCESS(writer.PreCommit());
    ASSERT_SUCCESS(writer.CommitWait());
  }
  std::barrier start(3);
  const auto sweep = [&] {
    start.arrive_and_wait();
    for (size_t pass = 0; pass < 32; ++pass) {
      tm_->SweepCommittedVersionChains();
    }
  };
  {
    std::jthread first(sweep);
    std::jthread second(sweep);
    sweep();
  }
  EXPECT_EQ(tm_->RuntimeStats().version_chains, kRows);
  for (const uint64_t i : {uint64_t{0}, kRows / 2, kRows - 1}) {
    ASSERT_SUCCESS_AND_EQ(tm_->ReadVersion(old, RowPosition(i + 100, 0),
                                           std::string_view("after")),
                          std::optional<std::string>("before"));
  }
  ASSERT_SUCCESS(old.PreCommit());
  tm_->SweepCommittedVersionChains();
  EXPECT_EQ(tm_->RuntimeStats().version_chains, 0U);
}

TEST_F(TransactionTest, GcFinishesRotatingSweepWithoutAnotherWrite) {
  constexpr uint64_t kRows = 10000;  // exceeds the adaptive full-scan limit
  {
    auto writer = tm_->Begin();
    for (uint64_t i = 0; i < kRows; ++i) {
      const RowPosition position(i + 100, 0);
      ASSERT_TRUE(writer.AddWriteSet(position, "before"));
      writer.RegisterVersionWrite(position, "before", "after");
    }
    ASSERT_EQ(writer.PreCommit(), Status::kSuccess);
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (tm_->RuntimeStats().version_chains != 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  EXPECT_EQ(tm_->RuntimeStats().version_chains, 0);
}

TEST_F(TransactionTest,
       GcRetriesAfterLastOldSnapshotFinishesWithoutAWritableCommit) {
  const RowPosition position(321, 0);
  auto old = tm_->Begin(true);
  {
    auto writer = tm_->Begin();
    ASSERT_TRUE(writer.AddWriteSet(position, "before"));
    writer.RegisterVersionWrite(position, "before", "after");
    ASSERT_EQ(writer.PreCommit(), Status::kSuccess);
  }
  // Let the first GC pass observe the pin and drain its writable-commit
  // counter. Finishing this read-only transaction must suffice to reclaim.
  std::this_thread::sleep_for(std::chrono::milliseconds(250));
  EXPECT_TRUE(tm_->HasVersionChain(position));
  ASSERT_SUCCESS_AND_EQ(
      tm_->ReadVersion(old, position, std::string_view("after")),
      std::optional<std::string>("before"));
  ASSERT_EQ(old.PreCommit(), Status::kSuccess);
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (tm_->HasVersionChain(position) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  EXPECT_FALSE(tm_->HasVersionChain(position));
}
TEST_F(TransactionTest, GcRearmsForSuccessiveQuietBursts) {
  for (int burst = 0; burst < 2; ++burst) {
    auto old = tm_->Begin(true);
    std::vector<RowPosition> rows;
    rows.reserve(3);
    for (int i = 0; i < 3; ++i) {
      const RowPosition position(
          static_cast<page_id_t>(1000 + (burst * 10) + i), 0);
      rows.push_back(position);
      auto writer = tm_->Begin();
      ASSERT_TRUE(writer.AddWriteSet(position, "before"));
      writer.RegisterVersionWrite(position, "before", "after");
      ASSERT_SUCCESS(writer.PreCommit());
    }
    for (const auto& position : rows) {
      ASSERT_SUCCESS_AND_EQ(
          tm_->ReadVersion(old, position, std::string_view("after")),
          std::optional<std::string>("before"));
    }
    ASSERT_SUCCESS(old.PreCommit());
    // No more writable work: both sub-threshold bursts must be reclaimed
    // through the timed wake, including after the first counter reset.
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (tm_->RuntimeStats().version_chains != 0 &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    EXPECT_EQ(tm_->RuntimeStats().version_chains, 0U);
  }
}

// T389 (P422): move-ASSIGN dropped the statement timeout/deadline and debug
// fields that the move constructor transfers, so a relocated Transaction
// silently lost an explicit per-statement timer (or kept the moved-from
// object's one).
TEST_F(TransactionTest, MoveAssignmentTransfersStatementTimer) {
  Transaction source = tm_->Begin(/*read_only=*/true);
  source.SetStatementTimeout(std::chrono::milliseconds(5000));
  source.BeginStatementTimer();
  ASSERT_TRUE(source.HasStatementTimeout());
  const int64_t deadline_ns = source.StatementDeadlineNs();
  ASSERT_GT(deadline_ns, 0);

  Transaction sink;  // default-constructed: owns no registry slot
  sink = std::move(source);
  EXPECT_TRUE(sink.HasStatementTimeout());
  EXPECT_EQ(sink.StatementDeadlineNs(), deadline_ns);
  EXPECT_EQ(sink.CheckStatementTimeout(), Status::kSuccess);
}

namespace {

TEST_F(TransactionTest, WalStagingFailureDoesNotAppendRecord) {
  struct ResetKnob {  // NOLINT(cppcoreguidelines-special-member-functions):
                      // scope-owned cleanup.
    ~ResetKnob() {
      ProcessMemoryBudget::Global().SetTestMemoryFailCountdown(0);
    }
  } reset;
  Transaction txn = tm_->Begin();
  const lsn_t before = l_->BufferedLSN();
  ProcessMemoryBudget::Global().SetTestMemoryFailCountdown(1);
  const auto failed = txn.InsertLog(1, 0, "image");
  EXPECT_EQ(failed.GetStatus().GetCode(), StatusCode::kMemoryLimitExceeded);
  EXPECT_FALSE(txn.HasLogRecord());
  EXPECT_EQ(l_->BufferedLSN(), before);

  const auto retried = txn.InsertLog(1, 0, "image");
  ASSERT_TRUE(retried.HasValue()) << retried.GetStatus().GetMessage();
  EXPECT_EQ(retried.Value(), before);
  EXPECT_TRUE(txn.HasLogRecord());
}

class TransactionP6aTest : public ::testing::Test {
 protected:
  void SetUp() override {
    prefix_ = "txn_p6a_test-" + RandomString();
    Open(false);
  }

  void Open(bool open_existing) {
    tm_.reset();
    rm_.reset();
    logger_.reset();
    pm_.reset();
    pm_ = PageManager::Create(prefix_ + ".db", 64, open_existing).MoveValue();
    logger_ = Logger::Create(prefix_ + ".log").MoveValue();
    pm_->SetDurableWaiter(
        [l = logger_.get()](lsn_t lsn) { return l->WaitForDurable(lsn); });
    rm_ = std::make_unique<RecoveryManager>(prefix_ + ".log", pm_->GetPool(),
                                            logger_->SegmentSize());
    rm_->SetLogger(logger_.get());
    tm_ = std::make_unique<TransactionManager>(pm_.get(), logger_.get(),
                                               rm_.get());
    if (open_existing) {
      ASSERT_SUCCESS(rm_->RecoverFrom(0, tm_.get()));
    }
  }

  void TearDown() override {
    tm_.reset();
    rm_.reset();
    logger_.reset();
    pm_.reset();
    std::ignore = std::remove((prefix_ + ".db").c_str());
    std::ignore = std::remove((prefix_ + ".db.ckpt").c_str());
    std::ignore = std::remove((prefix_ + ".db.ckpt.tmp").c_str());
    std::ignore = std::remove((prefix_ + ".db.blocks").c_str());
    std::error_code ec;
    const std::filesystem::directory_iterator end;
    std::filesystem::directory_iterator it(".", ec);
    EXPECT_FALSE(ec) << "cannot sweep companion artifacts: " << ec.message();
    for (; !ec && it != end; it.increment(ec)) {
      const std::string fname = it->path().filename().string();
      if (fname.starts_with(prefix_ + ".db.blocks.") ||
          fname.starts_with(prefix_ + ".db.lsm.")) {
        std::error_code rem_ec;
        std::filesystem::remove(it->path(), rem_ec);
        EXPECT_FALSE(rem_ec)
            << "failed to remove " << fname << ": " << rem_ec.message();
      }
    }
    EXPECT_FALSE(ec) << "companion sweep failed: " << ec.message();
    RemoveLogAndSegments(prefix_ + ".log");
  }

  std::string prefix_;
  std::unique_ptr<PageManager> pm_;
  std::unique_ptr<Logger> logger_;
  std::unique_ptr<RecoveryManager> rm_;
  std::unique_ptr<TransactionManager> tm_;
};

TEST_F(TransactionP6aTest, WalRereadAbortUndoesFirstRecordAtZero) {
  tm_->SetWalRereadThresholdBytesForTest(128);
  Transaction txn = tm_->Begin();
  ASSIGN_OR_ASSERT_FAIL(PageRef, page,
                        pm_->AllocateNewPage(txn, PageType::kRowPage));
  const page_id_t pid = page->PageID();
  ASSERT_EQ(txn.FirstLSN(), 0U);
  ASSERT_SUCCESS(page->Insert(txn, std::string(1024, 'x')).GetStatus());
  page.PageUnlock();
  EXPECT_TRUE(txn.IsWalRereadMode());
  ASSERT_SUCCESS(txn.Abort());
  EXPECT_FALSE(tm_->FailStopped());
  EXPECT_FALSE(pm_->GetPage(pid).HasValue());
}

TEST_F(TransactionP6aTest,
       WalRereadModeCommitsAndAbortsBeyond4MiBAndSurvivesCrash) {
  // Commit a baseline page first so abort rollback can be verified against it.
  page_id_t base_pid = 0;
  slot_t base_slot = 0;
  {
    Transaction base = tm_->Begin();
    ASSIGN_OR_ASSERT_FAIL(PageRef, page,
                          pm_->AllocateNewPage(base, PageType::kRowPage));
    base_pid = page->PageID();
    ASSIGN_OR_ASSERT_FAIL(slot_t, slot, page->Insert(base, "baseline_value"));
    base_slot = slot;
    page.PageUnlock();
    ASSERT_SUCCESS(base.PreCommit());
  }

  // 1) Single transaction writing >5 MiB (>4 MiB ADR 0008 default threshold)
  // triggers WAL re-read mode and commits all rows cleanly across restart.
  constexpr int kLargeOps = 560;
  const std::string big_payload(9500, 'c');
  std::vector<std::pair<page_id_t, slot_t>> committed_rows;
  committed_rows.reserve(kLargeOps);
  {
    Transaction txn = tm_->Begin();
    for (int i = 0; i < kLargeOps; ++i) {
      ASSIGN_OR_ASSERT_FAIL(PageRef, page,
                            pm_->AllocateNewPage(txn, PageType::kRowPage));
      ASSIGN_OR_ASSERT_FAIL(slot_t, slot, page->Insert(txn, big_payload));
      committed_rows.emplace_back(page->PageID(), slot);
      page.PageUnlock();
    }
    EXPECT_TRUE(txn.IsWalRereadMode());
    ASSERT_SUCCESS(txn.PreCommit());
  }
  const TransactionRuntimeStats after_commit = tm_->RuntimeStats();
  EXPECT_GE(after_commit.wal_reread_activations, 1U);
  EXPECT_GT(after_commit.wal_reread_bytes, size_t{4} << 20);

  // 2) Second >5 MiB transaction updates baseline + allocates pages, enters WAL
  // re-read mode, then aborts cleanly via backward prev_lsn traversal.
  {
    Transaction abort_txn = tm_->Begin();
    {
      ASSIGN_OR_ASSERT_FAIL(PageRef, bp, pm_->GetPage(base_pid));
      ASSERT_SUCCESS(bp->Update(abort_txn, base_slot, "aborted_overwrite"));
      bp.PageUnlock();
    }
    for (int i = 0; i < kLargeOps; ++i) {
      ASSIGN_OR_ASSERT_FAIL(
          PageRef, page, pm_->AllocateNewPage(abort_txn, PageType::kRowPage));
      ASSERT_SUCCESS(page->Insert(abort_txn, big_payload).GetStatus());
      page.PageUnlock();
    }
    EXPECT_TRUE(abort_txn.IsWalRereadMode());
    ASSERT_SUCCESS(abort_txn.Abort());
  }
  EXPECT_GE(tm_->RuntimeStats().wal_reread_activations, 2U);

  // 3) Third >5 MiB transaction crashes before commit (simulate process death
  // after WAL flush without PreCommit or Abort).
  {
    Transaction loser = tm_->Begin();
    {
      ASSIGN_OR_ASSERT_FAIL(PageRef, bp, pm_->GetPage(base_pid));
      ASSERT_SUCCESS(bp->Update(loser, base_slot, "loser_crash_overwrite"));
      bp.PageUnlock();
    }
    for (int i = 0; i < kLargeOps; ++i) {
      ASSIGN_OR_ASSERT_FAIL(PageRef, page,
                            pm_->AllocateNewPage(loser, PageType::kRowPage));
      ASSERT_SUCCESS(page->Insert(loser, big_payload).GetStatus());
      page.PageUnlock();
    }
    EXPECT_TRUE(loser.IsWalRereadMode());
    ASSERT_SUCCESS(logger_->WaitForDurable(logger_->BufferedLSN()));
    pm_->DropAllPages();
    loser = Transaction();
  }

  // Reopen & recover: committed >4 MiB rows survive; aborted & loser writes
  // leave baseline untouched.
  Open(true);
  Transaction verify = tm_->Begin();
  {
    ASSIGN_OR_ASSERT_FAIL(PageRef, bp, pm_->GetPage(base_pid));
    ASSIGN_OR_ASSERT_FAIL(std::string_view, got, bp->Read(verify, base_slot));
    EXPECT_EQ(got, "baseline_value");
    bp.PageUnlock();
  }
  for (const auto& [pid, slot] : committed_rows) {
    ASSIGN_OR_ASSERT_FAIL(PageRef, p, pm_->GetPage(pid));
    ASSIGN_OR_ASSERT_FAIL(std::string_view, got, p->Read(verify, slot));
    ASSERT_EQ(got, big_payload);
    p.PageUnlock();
  }
  ASSERT_SUCCESS(verify.PreCommit());
}

TEST_F(TransactionP6aTest,
       WalRereadPreservesPendingLsmScanUnderForeignOverwrite) {
  tm_->SetWalRereadThresholdBytesForTest(4096);
  constexpr page_id_t kTree = 88001;

  Transaction txn_a = tm_->Begin();
  Transaction txn_b = tm_->Begin();
  // Push TXN A past the test WAL re-read threshold before mutating shared key.
  for (int i = 0; i < 64; ++i) {
    ASSERT_SUCCESS(pm_->LsmInsert(txn_a, kTree, "pad_" + std::to_string(i),
                                  std::string(256, 'p')));
  }
  EXPECT_TRUE(txn_a.IsWalRereadMode());

  ASSERT_SUCCESS(pm_->LsmInsert(txn_a, kTree, "shared", "from_a"));
  ASSERT_SUCCESS(pm_->LsmUpdate(txn_b, kTree, "shared", "from_b"));
  const auto scan_a =
      pm_->LsmScanRange(kTree, "shared", "shared", true, &txn_a);
  ASSERT_EQ(scan_a.size(), 1U);
  EXPECT_EQ(scan_a[0].second, "from_a");
  EXPECT_GT(tm_->RuntimeStats().wal_reread_bytes, 0U);

  ASSERT_SUCCESS(txn_a.PreCommit());
  ASSERT_SUCCESS(txn_b.Abort());
}

TEST_F(TransactionP6aTest,
       SpillableWriteSetHandlesManyIntentsOnCommitAndAbort) {
  tm_->SetWriteSetSpillThresholdForTest(64);
  const size_t prev_limit = ProcessMemoryBudget::Global().Limit();
  const size_t prev_storage = ProcessMemoryBudget::Global().StoragePoolLimit();
  const size_t prev_columnar =
      ProcessMemoryBudget::Global().ColumnarPoolLimit();
  struct BudgetGuard {
    size_t limit;
    size_t storage;
    size_t columnar;
    BudgetGuard(size_t lim, size_t stor, size_t col)
        : limit(lim), storage(stor), columnar(col) {}
    BudgetGuard(const BudgetGuard&) = delete;
    BudgetGuard& operator=(const BudgetGuard&) = delete;
    BudgetGuard(BudgetGuard&&) = delete;
    BudgetGuard& operator=(BudgetGuard&&) = delete;
    ~BudgetGuard() {
      ProcessMemoryBudget::Global().ResetForTest(limit, storage, columnar);
    }
  } guard{prev_limit, prev_storage, prev_columnar};
  ProcessMemoryBudget::Global().ResetForTest(size_t{2} << 20, size_t{512} << 10,
                                             size_t{256} << 10);

  constexpr size_t kRows = 2500;
  std::vector<RowPosition> positions;
  positions.reserve(kRows);
  for (size_t i = 0; i < kRows; ++i) {
    positions.emplace_back(static_cast<page_id_t>(10 + (i / 64)),
                           static_cast<slot_t>(i % 64));
  }

  // 1) Acquire >2500 write intents, spill write_set_ to SpillStore, release a
  // few intents, check HasWriteSet accuracy, then PreCommit.
  {
    Transaction writer = tm_->Begin();
    for (size_t i = 0; i < kRows; ++i) {
      ASSERT_TRUE(writer.AddWriteSet(positions[i], "before"));
      writer.RegisterVersionWrite(positions[i], "before", "after");
    }
    EXPECT_TRUE(writer.IsWriteSetDiskSpilled());
    // Re-checking existing and missing entries in spilled mode is exact.
    EXPECT_TRUE(writer.HasWriteSet(positions[0]));
    EXPECT_TRUE(writer.HasWriteSet(positions[kRows - 1]));
    EXPECT_FALSE(writer.HasWriteSet(RowPosition(99999, 0)));

    // Duplicate insert of held position succeeds without failing.
    EXPECT_TRUE(writer.AddWriteSet(positions[42]));

    ASSERT_SUCCESS(writer.PreCommit());
  }
  const TransactionRuntimeStats s1 = tm_->RuntimeStats();
  EXPECT_GE(s1.write_set_spills, 1U);
  EXPECT_GT(s1.write_set_spilled_bytes, 0U);

  // Verify every committed version is visible to a subsequent reader.
  Transaction reader = tm_->Begin(/*read_only=*/true);
  for (size_t i = 0; i < kRows; ++i) {
    ASSERT_SUCCESS_AND_EQ(
        tm_->ReadVersion(reader, positions[i], std::string_view("after")),
        std::optional<std::string>("after"));
  }
  ASSERT_SUCCESS(reader.PreCommit());

  // 2) Second spilled write_set_ aborts and releases every write intent so a
  // fresh writer can immediately acquire them without conflict.
  {
    Transaction aborted = tm_->Begin();
    for (size_t i = 0; i < kRows; ++i) {
      ASSERT_TRUE(aborted.AddWriteSet(positions[i], "after"));
      aborted.RegisterVersionWrite(positions[i], "after", "aborted");
    }
    EXPECT_TRUE(aborted.IsWriteSetDiskSpilled());
    ASSERT_SUCCESS(aborted.Abort());
  }
  {
    Transaction follower = tm_->Begin();
    EXPECT_TRUE(follower.TryAddWriteSet(positions[0]));
    EXPECT_TRUE(follower.TryAddWriteSet(positions[kRows - 1]));
    ASSERT_SUCCESS(follower.Abort());
  }
}

TEST_F(TransactionP6aTest, DiagnosticReadSetRemainsBounded) {
  Transaction txn = tm_->Begin(/*read_only=*/true);
  for (int i = 0; i < 4000; ++i) {
    EXPECT_TRUE(txn.AddReadSet(RowPosition(1, static_cast<slot_t>(i))));
  }
  std::ostringstream oss;
  oss << txn;
  EXPECT_NE(oss.str().find("read_set=4000"), std::string::npos);
  ASSERT_SUCCESS(txn.PreCommit());
}

TEST_F(TransactionP6aTest, ConcurrentVersionSpillPreservesCommittedPayloads) {
  tm_->SetVersionSpillThresholdBytesForTest(512);
  constexpr size_t kRows = 64;
  const std::string before(512, 'a');
  const std::string after(512, 'b');
  auto pin = tm_->Begin(/*read_only=*/true);
  std::barrier start(2);
  {
    const auto write_rows = [&](page_id_t first_page) {
      start.arrive_and_wait();
      auto txn = tm_->Begin();
      for (size_t i = 0; i < kRows; ++i) {
        const RowPosition rp(first_page + i, 0);
        ASSERT_TRUE(txn.AddWriteSet(rp, before));
        txn.RegisterVersionWrite(rp, before, after);
      }
      ASSERT_SUCCESS(txn.PreCommit());
    };
    std::jthread first(write_rows, 1000);
    std::jthread second(write_rows, 1256);
  }
  EXPECT_GT(tm_->RuntimeStats().version_spills, 0U);
  auto latest = tm_->Begin(/*read_only=*/true);
  for (page_id_t first_page : {1000, 1256}) {
    for (size_t i = 0; i < kRows; ++i) {
      const RowPosition rp(first_page + i, 0);
      ASSERT_SUCCESS_AND_EQ(tm_->ReadVersion(pin, rp, after),
                            std::optional<std::string>(before));
      ASSERT_SUCCESS_AND_EQ(tm_->ReadVersion(latest, rp, after),
                            std::optional<std::string>(after));
    }
  }
  ASSERT_SUCCESS(latest.PreCommit());
  ASSERT_SUCCESS(pin.PreCommit());
}

TEST_F(TransactionP6aTest, ColdMvccVersionPayloadsSpillAndReadAcrossSnapshots) {
  tm_->SetVersionSpillThresholdBytesForTest(512);
  constexpr int kRows = 32;
  std::vector<RowPosition> rps;
  rps.reserve(kRows);
  for (int i = 0; i < kRows; ++i) {
    rps.emplace_back(200 + (i / 8), static_cast<slot_t>(i % 8));
  }

  auto make_payload = [](char tag, int row) {
    return std::string(512, tag) + "-" + std::to_string(row);
  };

  // Initial committed version v1 (keeps before-image v0 on first mutation).
  {
    Transaction t = tm_->Begin();
    for (size_t i = 0; i < static_cast<size_t>(kRows); ++i) {
      const std::string v0 = make_payload('A', static_cast<int>(i));
      const std::string v1 = make_payload('B', static_cast<int>(i));
      ASSERT_TRUE(t.AddWriteSet(rps[i], v0));
      t.RegisterVersionWrite(rps[i], v0, v1);
    }
    ASSERT_SUCCESS(t.PreCommit());
  }

  Transaction snap_v1 = tm_->Begin(/*read_only=*/true);

  // Overwrite v1 -> v2 -> v3 while snap_v1 pins history in version chains.
  for (char ver : {'C', 'D'}) {
    Transaction t = tm_->Begin();
    for (size_t i = 0; i < static_cast<size_t>(kRows); ++i) {
      const std::string prev =
          make_payload(static_cast<char>(ver - 1), static_cast<int>(i));
      const std::string next = make_payload(ver, static_cast<int>(i));
      ASSERT_TRUE(t.AddWriteSet(rps[i], prev));
      t.RegisterVersionWrite(rps[i], prev, next);
    }
    ASSERT_SUCCESS(t.PreCommit());
  }

  const TransactionRuntimeStats stats = tm_->RuntimeStats();
  EXPECT_GT(stats.version_spills, 0U);
  EXPECT_GT(stats.version_spill_bytes, 0U);
  EXPECT_LE(stats.version_payload_resident_bytes, 1024U);

  // Verify snapshot read (v1) and latest read (v3) recover exact spilled bytes.
  Transaction reader_latest = tm_->Begin(/*read_only=*/true);
  for (size_t i = 0; i < static_cast<size_t>(kRows); ++i) {
    ASSERT_SUCCESS_AND_EQ(
        tm_->ReadVersion(snap_v1, rps[i], std::string_view("phys")),
        std::optional<std::string>(make_payload('B', static_cast<int>(i))));
    ASSERT_SUCCESS_AND_EQ(
        tm_->ReadVersion(reader_latest, rps[i], std::string_view("phys")),
        std::optional<std::string>(make_payload('D', static_cast<int>(i))));
    ASSERT_SUCCESS_AND_EQ(
        tm_->ReadLatestVersion(reader_latest, rps[i], std::string_view("phys")),
        std::optional<std::string>(make_payload('D', static_cast<int>(i))));
  }
  ASSERT_SUCCESS(reader_latest.PreCommit());
  ASSERT_SUCCESS(snap_v1.PreCommit());

  // Once snapshots finish and GC collects chains, resident payload bytes -> 0.
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (tm_->RuntimeStats().version_chains != 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  EXPECT_EQ(tm_->RuntimeStats().version_chains, 0U);
  EXPECT_EQ(tm_->RuntimeStats().version_payload_resident_bytes, 0U);
}

TEST_F(TransactionP6aTest,
       UncommittedStagedSpillsAbortAndIoFailurePropagatesCleanly) {
  tm_->SetVersionSpillThresholdBytesForTest(512);
  constexpr int kRows = 20;
  std::vector<RowPosition> rps;
  rps.reserve(kRows);
  for (int i = 0; i < kRows; ++i) {
    rps.emplace_back(300, static_cast<slot_t>(i));
  }

  {
    Transaction writer = tm_->Begin();
    for (size_t i = 0; i < static_cast<size_t>(kRows); ++i) {
      const std::string before = std::string(512, 'X') + std::to_string(i);
      const std::string after = std::string(512, 'Y') + std::to_string(i);
      ASSERT_TRUE(writer.AddWriteSet(rps[i], before));
      writer.RegisterVersionWrite(rps[i], before, after);
    }
    const TransactionRuntimeStats stats = tm_->RuntimeStats();
    EXPECT_GT(stats.version_spills, 0U);
    EXPECT_LE(stats.version_payload_resident_bytes, 1024U);

    // Writer sees its own staged values even after spill.
    for (size_t i = 0; i < static_cast<size_t>(kRows); ++i) {
      const std::string after = std::string(512, 'Y') + std::to_string(i);
      ASSERT_SUCCESS_AND_EQ(
          tm_->ReadVersion(writer, rps[i], std::string_view("phys")),
          std::optional<std::string>(after));
    }

    // Injected spill read I/O failure propagates cleanly as an error Status.
    ProcessMemoryBudget::Global().SetTestSpillIoFailCountdown(1);
    auto io_fail = tm_->ReadVersion(writer, rps[0], std::string_view("phys"));
    EXPECT_NE(io_fail.GetStatus(), Status::kSuccess);
    ProcessMemoryBudget::Global().SetTestSpillIoFailCountdown(0);

    ASSERT_SUCCESS(writer.Abort());
  }

  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (tm_->RuntimeStats().version_chains != 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  EXPECT_EQ(tm_->RuntimeStats().version_payload_resident_bytes, 0U);
}

TEST_F(TransactionP6aTest,
       StatementTimeoutAndGcPinWarningFireDeterministically) {
  struct TimeoutGuard {
    TimeoutGuard() = default;
    TimeoutGuard(const TimeoutGuard&) = delete;
    TimeoutGuard& operator=(const TimeoutGuard&) = delete;
    TimeoutGuard(TimeoutGuard&&) = delete;
    TimeoutGuard& operator=(TimeoutGuard&&) = delete;
    ~TimeoutGuard() {
      Transaction::SetDefaultStatementTimeoutForTest(
          std::chrono::milliseconds(-1));
    }
  } guard;

  Transaction::SetDefaultStatementTimeoutForTest(std::chrono::milliseconds(15));
  {
    Transaction timed = tm_->Begin(/*read_only=*/true);
    timed.BeginStatementTimer();
    EXPECT_TRUE(timed.HasStatementTimeout());
    EXPECT_EQ(timed.CheckStatementTimeout(), Status::kSuccess);
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
    const Status expired = timed.CheckStatementTimeout();
    // A statement timeout cancels the query (SQLSTATE 57014).
    EXPECT_EQ(expired.GetCode(), StatusCode::kQueryCanceled);
    EXPECT_NE(expired.GetMessage().find("timeout"), std::string::npos);

    timed.SetStatementTimeout(std::chrono::milliseconds(0));
    EXPECT_FALSE(timed.HasStatementTimeout());
    EXPECT_EQ(timed.CheckStatementTimeout(), Status::kSuccess);
    ASSERT_SUCCESS(timed.PreCommit());
  }
  Transaction::SetDefaultStatementTimeoutForTest(std::chrono::milliseconds(-1));

  // Verify rate-limited GC pin warning fires when an old snapshot pins chains.
  tm_->SetGcPinWarnThresholdsForTest(/*chain_limit=*/4, /*age_commits=*/8);
  Transaction blocker = tm_->Begin(/*read_only=*/true);
  for (int i = 0; i < 10; ++i) {
    Transaction w = tm_->Begin();
    const RowPosition rp(500, static_cast<slot_t>(i));
    ASSERT_TRUE(w.AddWriteSet(rp, "v0"));
    w.RegisterVersionWrite(rp, "v0", "v1");
    ASSERT_SUCCESS(w.PreCommit());
  }
  const auto warn_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (tm_->GcPinWarnCountForTest() == 0 &&
         std::chrono::steady_clock::now() < warn_deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  EXPECT_GE(tm_->GcPinWarnCountForTest(), 1U);
  ASSERT_SUCCESS(blocker.PreCommit());
}

TEST_F(TransactionP6aTest,
       WriteSetSpillIoFailurePreservesStatusWithoutMaskingAsConflict) {
  tm_->SetWriteSetSpillThresholdForTest(64);
  struct InjectResetGuard {  // NOLINT(cppcoreguidelines-special-member-functions):
                             // scope-owned cleanup.
    ~InjectResetGuard() { ProcessMemoryBudget::Global().ResetTestKnobs(); }
  } guard;

  Transaction writer = tm_->Begin();
  ProcessMemoryBudget::Global().SetTestSpillIoFailCountdown(1);
  bool failed = false;
  for (size_t i = 0; i < 2000; ++i) {
    const RowPosition rp(static_cast<page_id_t>(700 + (i / 64)),
                         static_cast<slot_t>(i % 64));
    if (!writer.TryAddWriteSet(rp)) {
      failed = true;
      break;
    }
  }
  ASSERT_TRUE(failed);
  EXPECT_EQ(writer.LastWriteSetFailureStatus().GetCode(), StatusCode::kIOError);
  // A transient write-set spill failure must not permanently poison an active
  // transaction whose invariants remain intact.
  EXPECT_FALSE(writer.IsFailed());
  ASSERT_SUCCESS(writer.Abort());
}

TEST_F(TransactionP6aTest,
       WriteSetSpillBufferChargeFailureKeepsIntentsReleaseable) {
  tm_->SetWriteSetSpillThresholdForTest(64);
  // NOLINTNEXTLINE(cppcoreguidelines-special-member-functions) guard type is
  // non-copyable by design
  struct
      InjectResetGuard {  // NOLINT(cppcoreguidelines-special-member-functions):
                          // scope-owned cleanup.
    ~InjectResetGuard() { ProcessMemoryBudget::Global().ResetTestKnobs(); }
  } guard;

  constexpr size_t kRows = 64;
  std::vector<RowPosition> rows;
  rows.reserve(kRows);
  for (size_t i = 0; i < kRows; ++i) {
    rows.emplace_back(static_cast<page_id_t>(800), static_cast<slot_t>(i));
  }

  Transaction writer = tm_->Begin();
  for (const RowPosition& rp : rows) {
    ASSERT_TRUE(writer.AddWriteSet(rp, "before"));
  }
  ASSERT_FALSE(writer.IsWriteSetDiskSpilled());
  auto staged_count = [&]() {
    size_t count = 0;
    for (const RowPosition& rp : rows) {
      count += writer.HasWriteSet(rp) ? 1U : 0U;
    }
    return count;
  };
  ASSERT_EQ(staged_count(), kRows);

  // The one-shot injection lands on the spill-buffer charge inside
  // transition_to_disk. The already staged in-memory entries (and the write
  // intents they track) must survive the failed spill transition intact.
  ProcessMemoryBudget::Global().SetTestMemoryFailCountdown(1);
  const RowPosition extra(800, static_cast<slot_t>(kRows));
  EXPECT_FALSE(writer.TryAddWriteSet(extra));
  EXPECT_EQ(writer.LastWriteSetFailureStatus().GetCode(),
            StatusCode::kMemoryLimitExceeded);
  EXPECT_FALSE(writer.IsWriteSetDiskSpilled());
  EXPECT_EQ(staged_count(), kRows);
  for (const RowPosition& rp : rows) {
    EXPECT_TRUE(writer.HasWriteSet(rp));
  }

  // Every original intent is still held: no contender can take one, and all
  // of them become available again once the writer aborts.
  {
    Transaction contender = tm_->Begin();
    for (const RowPosition& rp : rows) {
      EXPECT_FALSE(contender.TryAddWriteSet(rp));
    }
    ASSERT_SUCCESS(contender.Abort());
  }
  ASSERT_SUCCESS(writer.Abort());
  for (const RowPosition& rp : rows) {
    Transaction after = tm_->Begin();
    EXPECT_TRUE(after.TryAddWriteSet(rp));
    ASSERT_SUCCESS(after.Abort());
  }
}

}  // namespace

}  // namespace tinylamb
