/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "gtest/gtest.h"
#include "page/page_manager.hpp"
#include "page/row_position.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/sql_engine.hpp"
#include "recovery/log_record.hpp"
#include "recovery/logger.hpp"
#include "recovery/recovery_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {
// Executes one statement and returns the materialized rows.  A prepare or
// execution failure surfaces as a runtime_error carrying the diagnostic.
std::vector<Row> RunSql(SqlEngine* engine, TransactionContext* ctx,
                        const std::string& sql) {
  StatusOr<Executor> prepared = engine->Prepare(*ctx, sql);
  if (!prepared.HasValue()) {
    throw std::runtime_error(engine->LastError());
  }
  std::vector<Row> rows;
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    rows.push_back(row);
    row = Row();
  }
  const Status st = prepared.Value()->GetStatus();
  if (st != Status::kSuccess) {
    throw std::runtime_error(st.GetMessage());
  }
  return rows;
}

// Polls |condition| until it holds or |timeout| elapses.  Used only to await
// background workers (GC thread, deadlock detector) whose triggering is
// asynchronous by design; every assertion is on the final state.
bool WaitForCondition(const std::function<bool()>& condition,
                      std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!condition() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return condition();
}

// Redirects the WAL file descriptor to /dev/full: every subsequent write
// fails with ENOSPC.  The flush worker stays alive until the next write.
void PointWalAtDevFull(Logger* logger) {
  const int full_fd = ::open("/dev/full", O_WRONLY);
  ASSERT_GE(full_fd, 0);
  ASSERT_EQ(::dup2(full_fd, logger->Fd()), logger->Fd());
  ASSERT_EQ(::close(full_fd), 0);
}

// Kills the flush worker for good: after this, AddLog and WaitForDurable
// fail synchronously (Logger::Failed() latched).
void ForceWalFailure(Logger* logger) {
  PointWalAtDevFull(logger);
  std::ignore = logger->AddLog("dead");
  ASSERT_NE(logger->WaitForDurable(logger->BufferedLSN()), Status::kSuccess);
  ASSERT_TRUE(logger->Failed());
}

// Counts the committed entries inside the first "committed=[...]" of a
// DebugDumpVersionChains rendering.
size_t CountCommittedEntries(const std::string& dump) {
  const auto start = dump.find("committed=[");
  if (start == std::string::npos) {
    return 0;
  }
  const auto end = dump.find(']', start);
  if (end == std::string::npos) {
    return 0;
  }
  return static_cast<size_t>(
      std::count(dump.begin() + static_cast<std::ptrdiff_t>(start),
                 dump.begin() + static_cast<std::ptrdiff_t>(end), '{'));
}
}  // namespace

// ---------------------------------------------------------------------------
// SQL smoke: keeps the binary wired to the engine end of the transaction
// stack (the fixtures below exercise the layer directly).
// ---------------------------------------------------------------------------

class SqlFunctionCoverageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!GoogleSqlFrontend::Available()) {
      GTEST_SKIP() << true;
    }
    database_ = Database::Create("transaction_coverage_test").MoveValue();
    context_ = std::make_unique<TransactionContext>(database_->BeginContext());
    engine_ = std::make_unique<SqlEngine>(*database_);
  }
  void TearDown() override {
    engine_.reset();
    context_.reset();
    if (database_ != nullptr) {
      database_->DeleteAll();
    }
    database_.reset();
  }
  std::unique_ptr<Database> database_;
  std::unique_ptr<TransactionContext> context_;
  std::unique_ptr<SqlEngine> engine_;
};

TEST_F(SqlFunctionCoverageTest, Smoke) {
  const auto rows = RunSql(engine_.get(), context_.get(), "SELECT 1");
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(int64_t{1}));
}

// ---------------------------------------------------------------------------
// Manager-level fixture: WAL + version shards, no pages and no recovery
// (tests here never walk a log chain through the undo machinery).
// ---------------------------------------------------------------------------

class TransactionCoverageTest : public ::testing::Test {
 public:
  void SetUp() override { Reset(); }

  void Reset() {
    tm_.reset();
    l_.reset();
    if (!log_name_.empty()) {
      std::ignore = std::remove(log_name_.c_str());
      log_name_.clear();
    }
    log_name_ = "transaction_coverage-" + RandomString() + ".log";
    l_ = Logger::Create(log_name_).MoveValue();
    tm_ = std::make_unique<TransactionManager>(nullptr, l_.get(), nullptr);
  }

  void TearDown() {
    tm_.reset();
    l_.reset();
    if (!log_name_.empty()) {
      std::ignore = std::remove(log_name_.c_str());
      log_name_.clear();
    }
  }

  // Ten read-write no-op commits push commits_since_gc_ over the worker's
  // threshold so the next GC pass runs within a few milliseconds.
  void PumpGcCommits() {
    for (int i = 0; i < 10; ++i) {
      Transaction idle = tm_->Begin();
      ASSERT_EQ(idle.PreCommit(), Status::kSuccess);
    }
  }

 protected:
  std::string log_name_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<TransactionManager> tm_;
};

// ---------------------------------------------------------------------------
// Write intents: acquisition, try-acquisition, wounding, release.
// ---------------------------------------------------------------------------

TEST_F(TransactionCoverageTest, WoundedTransactionNeverAcquiresWriteIntent) {
  const RowPosition rp(1, 1);
  Transaction txn = tm_->Begin();
  EXPECT_FALSE(txn.IsWounded());
  txn.Wound();
  ASSERT_TRUE(txn.IsWounded());
  // The early wounded check fires before the chain is even inspected.
  EXPECT_FALSE(txn.AddWriteSet(rp));
  EXPECT_FALSE(tm_->HasVersionChain(rp));
  ASSERT_EQ(tm_->Abort(txn), Status::kSuccess);
}

TEST_F(TransactionCoverageTest, TryAddWriteSetRejectsReadOnlyTransaction) {
  const RowPosition rp(2, 1);
  Transaction txn = tm_->Begin(true);
  EXPECT_FALSE(txn.TryAddWriteSet(rp));
  EXPECT_FALSE(txn.AddWriteSet(rp));
  ASSERT_EQ(txn.PreCommit(), Status::kSuccess);
}

TEST_F(TransactionCoverageTest, TryAddWriteSetSucceedsOnceThenShortCircuits) {
  const RowPosition rp(3, 1);
  Transaction txn = tm_->Begin();
  ASSERT_TRUE(txn.TryAddWriteSet(rp));
  // Second attempt short-circuits on the write-set membership check.
  EXPECT_TRUE(txn.TryAddWriteSet(rp));
  EXPECT_TRUE(tm_->HasVersionChain(rp));
  ASSERT_EQ(tm_->Abort(txn), Status::kSuccess);
}

TEST_F(TransactionCoverageTest, TryAddWriteSetLosesToForeignIntent) {
  const RowPosition rp(4, 1);
  Transaction owner = tm_->Begin();
  Transaction contender = tm_->Begin();
  ASSERT_TRUE(owner.AddWriteSet(rp));
  // Non-waiting acquisition against a foreign pending intent fails without
  // blocking and counts as a conflict.
  EXPECT_FALSE(contender.TryAddWriteSet(rp));
  EXPECT_FALSE(contender.AddWriteSet(rp));
  owner.Abort();
  contender.Abort();
}

TEST_F(TransactionCoverageTest, DirectIntentAcquisitionIsIdempotentForOwner) {
  const RowPosition rp(5, 1);
  Transaction txn = tm_->Begin();
  ASSERT_TRUE(tm_->AcquireWriteIntent(txn, rp, /*wait=*/false));
  // The owner re-acquiring its own reserved intent succeeds without
  // reinstalling anything.
  EXPECT_TRUE(tm_->AcquireWriteIntent(txn, rp, /*wait=*/false));
  // Register the row so Abort() releases the reservation cleanly.
  ASSERT_TRUE(txn.AddWriteSet(rp));
  ASSERT_EQ(tm_->Abort(txn), Status::kSuccess);
}

TEST_F(TransactionCoverageTest, DirectIntentAcquisitionInstallsBeforeImage) {
  const RowPosition rp(6, 1);
  Transaction txn = tm_->Begin();
  // wait=false with a before-image installs the base committed version so
  // concurrent snapshots keep seeing the old row during the intent window.
  ASSERT_TRUE(tm_->AcquireWriteIntent(txn, rp, /*wait=*/false,
                                      std::optional<std::string_view>("img")));
  {
    Transaction reader = tm_->Begin(true);
    ASSERT_SUCCESS_AND_EQ(tm_->ReadVersion(reader, rp, std::nullopt), "img");
    reader.PreCommit();
  }
  ASSERT_TRUE(txn.AddWriteSet(rp));
  ASSERT_EQ(tm_->Abort(txn), Status::kSuccess);
}

TEST_F(TransactionCoverageTest, ReleaseWriteIntentTrueForUnknownRow) {
  const RowPosition rp(7, 1);
  Transaction txn = tm_->Begin();
  EXPECT_TRUE(tm_->ReleaseWriteIntent(txn, rp));
  EXPECT_FALSE(tm_->HasVersionChain(rp));
}

TEST_F(TransactionCoverageTest, ReleaseWriteIntentKeepsForeignIntent) {
  const RowPosition rp(8, 1);
  Transaction owner = tm_->Begin();
  Transaction outsider = tm_->Begin();
  ASSERT_TRUE(owner.AddWriteSet(rp));
  EXPECT_FALSE(tm_->ReleaseWriteIntent(outsider, rp));
  EXPECT_TRUE(tm_->HasVersionChain(rp));
  owner.Abort();
}

TEST_F(TransactionCoverageTest, ReleaseWriteIntentKeepsStagedIntent) {
  const RowPosition rp(9, 1);
  Transaction txn = tm_->Begin();
  ASSERT_TRUE(txn.AddWriteSet(rp));
  txn.RegisterVersionWrite(rp, std::nullopt, "staged");
  // A staged write is real work, not a discardable reservation.
  EXPECT_FALSE(tm_->ReleaseWriteIntent(txn, rp));
  EXPECT_TRUE(tm_->HasVersionChain(rp));
  ASSERT_EQ(tm_->Abort(txn), Status::kSuccess);
}

TEST_F(TransactionCoverageTest, ReleaseWriteIntentDropsOwnUnstagedIntent) {
  const RowPosition rp(10, 1);
  Transaction txn = tm_->Begin();
  ASSERT_TRUE(txn.AddWriteSet(rp));
  ASSERT_TRUE(tm_->HasVersionChain(rp));
  EXPECT_TRUE(tm_->ReleaseWriteIntent(txn, rp));
  // The chain was only an unstaged shell with no committed history: it is
  // erased outright so later snapshots do not trip over it.
  EXPECT_FALSE(tm_->HasVersionChain(rp));
  ASSERT_EQ(tm_->Abort(txn), Status::kSuccess);
}

TEST_F(TransactionCoverageTest, TransactionReleaseRemovesWriteSetEntry) {
  const RowPosition rp(11, 1);
  Transaction txn = tm_->Begin();
  ASSERT_TRUE(txn.AddWriteSet(rp));
  txn.ReleaseWriteIntent(rp);
  // If the write-set entry survived the release, the intent would still be
  // held and this foreign reservation would fail.
  Transaction other = tm_->Begin();
  EXPECT_TRUE(other.TryAddWriteSet(rp));
  other.Abort();
  ASSERT_EQ(tm_->Abort(txn), Status::kSuccess);
}

TEST_F(TransactionCoverageTest, TransactionReleaseIgnoresForeignIntent) {
  const RowPosition rp(12, 1);
  Transaction owner = tm_->Begin();
  Transaction outsider = tm_->Begin();
  ASSERT_TRUE(owner.AddWriteSet(rp));
  // Must be a silent no-op: the reservation belongs to someone else.
  outsider.ReleaseWriteIntent(rp);
  EXPECT_TRUE(tm_->HasVersionChain(rp));
  owner.Abort();
  ASSERT_EQ(tm_->Abort(outsider), Status::kSuccess);
}

TEST_F(TransactionCoverageTest, ReacquireWithBeforeImageInstallsIt) {
  const RowPosition rp(13, 1);
  Transaction writer = tm_->Begin();
  // First reservation carries no before-image...
  ASSERT_TRUE(writer.AddWriteSet(rp));
  // ...the re-acquisition supplies one, so the chain gains a base committed
  // version while the intent stays unstaged.
  ASSERT_TRUE(writer.AddWriteSet(rp, std::string_view("img")));
  {
    Transaction reader = tm_->Begin(true);
    ASSERT_SUCCESS_AND_EQ(tm_->ReadVersion(reader, rp, std::nullopt), "img");
    reader.PreCommit();
  }
  ASSERT_EQ(tm_->Abort(writer), Status::kSuccess);
}

// ---------------------------------------------------------------------------
// ReadVersion visibility matrix.
// ---------------------------------------------------------------------------

TEST_F(TransactionCoverageTest, ReadVersionServesOwnStagedValue) {
  const RowPosition rp(20, 1);
  Transaction writer = tm_->Begin();
  ASSERT_TRUE(writer.AddWriteSet(rp));
  writer.RegisterVersionWrite(rp, std::nullopt, "pending");
  ASSERT_SUCCESS_AND_EQ(tm_->ReadVersion(writer, rp, std::nullopt), "pending");
  ASSERT_EQ(tm_->Abort(writer), Status::kSuccess);
}

TEST_F(TransactionCoverageTest, ReadVersionOwnStagedTombstoneIsNotExists) {
  const RowPosition rp(21, 1);
  Transaction writer = tm_->Begin();
  ASSERT_TRUE(writer.AddWriteSet(rp));
  writer.RegisterVersionWrite(rp, std::nullopt, std::nullopt);
  EXPECT_EQ(tm_->ReadVersion(writer, rp, std::nullopt).GetStatus(),
            Status::kNotExists);
  ASSERT_EQ(tm_->Abort(writer), Status::kSuccess);
}

TEST_F(TransactionCoverageTest, ReadVersionEmptyChainFallsBackToPhysical) {
  const RowPosition rp(22, 1);
  Transaction writer = tm_->Begin();
  // A bare insert intent: chain with a pending owner and no committed entry.
  ASSERT_TRUE(writer.AddWriteSet(rp));
  EXPECT_EQ(tm_->ReadVersion(writer, rp, std::nullopt).GetStatus(),
            Status::kNotExists);
  ASSERT_SUCCESS_AND_EQ(tm_->ReadVersion(writer, rp, "phys"), "phys");
  {
    // Another snapshot falls back to the physical image the same way: the
    // unstaged intent of somebody else must not hide the row.
    Transaction reader = tm_->Begin(true);
    ASSERT_SUCCESS_AND_EQ(tm_->ReadVersion(reader, rp, "phys2"), "phys2");
    EXPECT_EQ(tm_->ReadVersion(reader, rp, std::nullopt).GetStatus(),
              Status::kNotExists);
    reader.PreCommit();
  }
  ASSERT_EQ(tm_->Abort(writer), Status::kSuccess);
}

TEST_F(TransactionCoverageTest,
       OwnUnstagedIntentReadsLatestCommittedValueAndDependsOnIt) {
  const RowPosition rp(23, 1);
  Transaction w1 = tm_->Begin();
  ASSERT_TRUE(w1.AddWriteSet(rp));
  w1.RegisterVersionWrite(rp, std::nullopt, "v1");
  ASSERT_EQ(w1.PreCommit(), Status::kSuccess);
  Transaction w2 = tm_->Begin();
  ASSERT_TRUE(w2.AddWriteSet(rp));
  w2.RegisterVersionWrite(rp, "v1", "v2");
  ASSERT_EQ(w2.PreCommit(), Status::kSuccess);

  // A writer that waited for the predecessors reserves an unstaged intent;
  // strict write locking lets its SET expression see the newest committed
  // image, and the commit records the durability dependency on it.
  Transaction w3 = tm_->Begin();
  ASSERT_TRUE(w3.AddWriteSet(rp));
  const lsn_t before = w3.DurabilityDependence();
  ASSERT_SUCCESS_AND_EQ(tm_->ReadVersion(w3, rp, std::nullopt), "v2");
  EXPECT_GT(w3.DurabilityDependence(), before);
  ASSERT_EQ(tm_->Abort(w3), Status::kSuccess);
}

TEST_F(TransactionCoverageTest, OwnUnstagedIntentOnTombstoneIsNotExists) {
  const RowPosition rp(24, 1);
  Transaction w1 = tm_->Begin();
  ASSERT_TRUE(w1.AddWriteSet(rp));
  w1.RegisterVersionWrite(rp, std::nullopt, "v1");
  ASSERT_EQ(w1.PreCommit(), Status::kSuccess);
  Transaction w2 = tm_->Begin();
  ASSERT_TRUE(w2.AddWriteSet(rp));
  w2.RegisterVersionWrite(rp, "v1", std::nullopt);
  ASSERT_EQ(w2.PreCommit(), Status::kSuccess);

  Transaction w3 = tm_->Begin();
  ASSERT_TRUE(w3.AddWriteSet(rp));
  EXPECT_EQ(tm_->ReadVersion(w3, rp, std::nullopt).GetStatus(),
            Status::kNotExists);
  ASSERT_EQ(tm_->Abort(w3), Status::kSuccess);
}

TEST_F(TransactionCoverageTest,
       CommittedReadRecordsDurabilityDependenceOnReader) {
  const RowPosition rp(25, 1);
  Transaction writer = tm_->Begin();
  ASSERT_TRUE(writer.AddWriteSet(rp));
  writer.RegisterVersionWrite(rp, std::nullopt, "committed");
  ASSERT_EQ(writer.PreCommit(), Status::kSuccess);
  writer.CommitWait();

  Transaction reader = tm_->Begin(true);
  EXPECT_EQ(reader.DurabilityDependence(), 0U);
  ASSERT_SUCCESS_AND_EQ(tm_->ReadVersion(reader, rp, std::nullopt),
                        "committed");
  // The published version carries its commit's durable point; reading it
  // makes the reader depend on that LSN.
  EXPECT_NE(reader.DurabilityDependence(), 0U);
  ASSERT_EQ(reader.PreCommit(), Status::kSuccess);
}

TEST_F(TransactionCoverageTest, SnapshotOlderThanEveryVersionSeesNothing) {
  const RowPosition rp(26, 1);
  // The reader's snapshot predates the only version on the chain.
  Transaction reader = tm_->Begin(true);
  Transaction writer = tm_->Begin();
  ASSERT_TRUE(writer.AddWriteSet(rp));
  writer.RegisterVersionWrite(rp, std::nullopt, "new");
  ASSERT_EQ(writer.PreCommit(), Status::kSuccess);

  EXPECT_EQ(tm_->ReadVersion(reader, rp, std::nullopt).GetStatus(),
            Status::kNotExists);
  EXPECT_FALSE(reader.SnapshotSeesRow(rp));
  reader.PreCommit();
}

// ---------------------------------------------------------------------------
// SnapshotSeesRow (insert slot-reuse gate).
// ---------------------------------------------------------------------------

TEST_F(TransactionCoverageTest, SnapshotSeesRowWithoutChainIsFalse) {
  const RowPosition rp(30, 1);
  Transaction txn = tm_->Begin();
  EXPECT_FALSE(txn.SnapshotSeesRow(rp));
  ASSERT_EQ(txn.PreCommit(), Status::kSuccess);
}

TEST_F(TransactionCoverageTest, SnapshotSeesRowOwnStagedWriteIsFalse) {
  const RowPosition rp(31, 1);
  Transaction writer = tm_->Begin();
  ASSERT_TRUE(writer.AddWriteSet(rp));
  writer.RegisterVersionWrite(rp, std::nullopt, "mine");
  // Reusing this slot behind the own staged value would mask the write.
  EXPECT_FALSE(writer.SnapshotSeesRow(rp));
  ASSERT_EQ(tm_->Abort(writer), Status::kSuccess);
}

TEST_F(TransactionCoverageTest, SnapshotSeesRowVisibleValueAndTombstone) {
  const RowPosition rp(32, 1);
  Transaction w1 = tm_->Begin();
  ASSERT_TRUE(w1.AddWriteSet(rp));
  w1.RegisterVersionWrite(rp, std::nullopt, "live");
  ASSERT_EQ(w1.PreCommit(), Status::kSuccess);
  {
    Transaction reader = tm_->Begin(true);
    EXPECT_TRUE(reader.SnapshotSeesRow(rp));
    reader.PreCommit();
  }
  Transaction w2 = tm_->Begin();
  ASSERT_TRUE(w2.AddWriteSet(rp));
  w2.RegisterVersionWrite(rp, "live", std::nullopt);
  ASSERT_EQ(w2.PreCommit(), Status::kSuccess);
  {
    Transaction reader = tm_->Begin(true);
    // A visible deletion is a row that cannot be seen.
    EXPECT_FALSE(reader.SnapshotSeesRow(rp));
    reader.PreCommit();
  }
}

// ---------------------------------------------------------------------------
// Diagnostics: chain dump, invalidation, registry bookkeeping, moves.
// ---------------------------------------------------------------------------

TEST_F(TransactionCoverageTest, DebugDumpVersionChainsRendersEveryShape) {
  const page_id_t pid = 40;
  const RowPosition committed(pid, 1);
  const RowPosition tombstoned(pid, 2);
  const RowPosition unstaged(pid, 3);
  const RowPosition staged(pid, 4);

  Transaction w1 = tm_->Begin();
  ASSERT_TRUE(w1.AddWriteSet(committed));
  w1.RegisterVersionWrite(committed, std::nullopt, "val");
  ASSERT_EQ(w1.PreCommit(), Status::kSuccess);

  Transaction w2 = tm_->Begin();
  ASSERT_TRUE(w2.AddWriteSet(tombstoned));
  w2.RegisterVersionWrite(tombstoned, std::nullopt, std::nullopt);
  ASSERT_EQ(w2.PreCommit(), Status::kSuccess);

  Transaction w3 = tm_->Begin();
  ASSERT_TRUE(w3.AddWriteSet(unstaged, std::string_view("before")));
  Transaction w4 = tm_->Begin();
  ASSERT_TRUE(w4.AddWriteSet(staged));
  w4.RegisterVersionWrite(staged, std::nullopt, "after");

  const std::string dump = tm_->DebugDumpVersionChains(pid);
  EXPECT_NE(dump.find("chain {40,1}"), std::string::npos);
  EXPECT_NE(dump.find("row:3B"), std::string::npos);  // "val" is 3 bytes
  EXPECT_NE(dump.find("null"), std::string::npos);    // the tombstone
  EXPECT_NE(dump.find("pending=txn"), std::string::npos);
  EXPECT_NE(dump.find("/unstaged"), std::string::npos);
  EXPECT_NE(dump.find("/staged"), std::string::npos);
  // Nothing is keyed to another page id.
  EXPECT_TRUE(tm_->DebugDumpVersionChains(pid + 100).empty());

  ASSERT_EQ(tm_->Abort(w3), Status::kSuccess);
  ASSERT_EQ(tm_->Abort(w4), Status::kSuccess);
}

TEST_F(TransactionCoverageTest, InvalidatePageVersionsDropsChainsAndWriteSet) {
  const page_id_t pid = 41;
  const RowPosition committed(pid, 1);
  const RowPosition fresh(pid, 2);

  Transaction seeder = tm_->Begin();
  ASSERT_TRUE(seeder.AddWriteSet(committed));
  seeder.RegisterVersionWrite(committed, std::nullopt, "old-page");
  ASSERT_EQ(seeder.PreCommit(), Status::kSuccess);

  Transaction reclaimer = tm_->Begin();
  ASSERT_TRUE(reclaimer.AddWriteSet(fresh));
  ASSERT_TRUE(tm_->HasVersionChain(committed));
  ASSERT_TRUE(tm_->HasVersionChain(fresh));

  // The page id is recycled: every chain keyed to it disappears and the
  // reclaimer's own write set drops the dead-incarnation entries.
  reclaimer.InvalidatePageVersions(pid);
  EXPECT_FALSE(tm_->HasVersionChain(committed));
  EXPECT_FALSE(tm_->HasVersionChain(fresh));

  // The pruned write set lets the reclaimer reserve a fresh intent instead
  // of silently skipping the acquisition on the stale entry.
  EXPECT_TRUE(reclaimer.AddWriteSet(fresh));
  ASSERT_TRUE(tm_->HasVersionChain(fresh));

  // The reclaimer-less overload is a plain chain wipe.
  tm_->InvalidatePageVersions(pid, nullptr);
  EXPECT_FALSE(tm_->HasVersionChain(fresh));
  ASSERT_EQ(tm_->Abort(reclaimer), Status::kSuccess);
}

TEST_F(TransactionCoverageTest, IsActiveTracksLifecycleAndDestructor) {
  tm_->SetSynchronousCommit(false);
  txn_id_t doomed_id = 0;
  {
    Transaction txn = tm_->Begin();
    doomed_id = txn.ID();
    EXPECT_TRUE(tm_->IsActive(doomed_id));
    // Destroying an unfinished transaction releases its registry slot and
    // snapshot pin instead of leaking a dangling pointer into the manager.
  }
  EXPECT_FALSE(tm_->IsActive(doomed_id));

  Transaction txn = tm_->Begin();
  EXPECT_TRUE(tm_->IsActive(txn.ID()));
  ASSERT_EQ(txn.PreCommit(), Status::kSuccess);
  EXPECT_FALSE(tm_->IsActive(txn.ID()));
}

TEST_F(TransactionCoverageTest, MoveConstructorCarriesRegistration) {
  Transaction first = tm_->Begin();
  const txn_id_t id = first.ID();
  Transaction second = std::move(first);
  EXPECT_EQ(second.ID(), id);
  EXPECT_TRUE(tm_->IsActive(id));
  ASSERT_EQ(tm_->Abort(second), Status::kSuccess);
}

TEST_F(TransactionCoverageTest, MoveAssignmentReregistersBothEnds) {
  Transaction keeper = tm_->Begin();
  const txn_id_t keeper_id = keeper.ID();
  Transaction replaced = tm_->Begin();
  const txn_id_t replaced_id = replaced.ID();
  EXPECT_TRUE(tm_->IsActive(replaced_id));

  replaced = std::move(keeper);
  // The target's old registry slot is gone, the source's slot moved here.
  EXPECT_FALSE(tm_->IsActive(replaced_id));
  EXPECT_TRUE(tm_->IsActive(keeper_id));
  ASSERT_EQ(tm_->Abort(replaced), Status::kSuccess);
}

// ---------------------------------------------------------------------------
// Metrics and the visibility-only commit pipeline.
// ---------------------------------------------------------------------------

TEST_F(TransactionCoverageTest, MetricsCountersObserveIntentAndCommitPath) {
  tm_->SetMetricsEnabled(true);
  const RowPosition rp(50, 1);

  Transaction writer = tm_->Begin();
  ASSERT_TRUE(writer.AddWriteSet(rp));  // write_intent_attempts
  Transaction contender = tm_->Begin();
  EXPECT_FALSE(contender.TryAddWriteSet(rp));  // write_intent_conflicts

  writer.RegisterVersionWrite(rp, std::nullopt, "v");
  ASSERT_EQ(writer.PreCommit(), Status::kSuccess);  // wal_wait counters
  writer.CommitWait();

  const TransactionRuntimeStats stats = tm_->RuntimeStats();
  EXPECT_GE(stats.write_intent_attempts, 1U);
  EXPECT_GE(stats.write_intent_conflicts, 1U);
  EXPECT_GE(stats.wal_wait_count, 1U);
  contender.Abort();
}

TEST_F(TransactionCoverageTest, CommitWaitReturnsOnceRecordsAreFlushed) {
  // A slow flush interval keeps the record buffered across the commit so
  // CommitWait really has to wait for the flusher instead of finding an
  // already-flushed tail.
  const std::string slow_name =
      "transaction_coverage_slow-" + RandomString() + ".log";
  auto slow =
      Logger::Create(slow_name, size_t{1024} * 1024 * 8, /*every_ms=*/300)
          .MoveValue();
  const RowPosition rp(51, 1);
  {
    TransactionManager slow_tm(nullptr, slow.get(), nullptr);
    // Skip the synchronous barrier: the commit record stays buffered and
    // only CommitWait waits for the flusher to catch up.
    slow_tm.SetSynchronousCommit(false);
    Transaction writer = slow_tm.Begin();
    // The WAL's first record starts at LSN 0 (the "no chain" sentinel):
    // CommitWait on a zero prev_lsn_ would trivially find the tail already
    // flushed.  Append a preamble record so the wait is real.
    ASSERT_TRUE(writer
                    .AppendLog(LogRecord(writer.PrevLSN(), writer.ID(),
                                         LogType::kBegin))
                    .HasValue());
    ASSERT_TRUE(writer.AddWriteSet(rp));
    writer.RegisterVersionWrite(rp, std::nullopt, "pipelined");
    ASSERT_EQ(writer.PreCommit(), Status::kSuccess);
    writer.CommitWait();
    EXPECT_GE(slow->CommittedLSN(), writer.PrevLSN());
  }
  EXPECT_EQ(std::remove(slow_name.c_str()), 0);
}

// ---------------------------------------------------------------------------
// Background GC worker.
// ---------------------------------------------------------------------------

TEST_F(TransactionCoverageTest, GcErasesChainOnceNoSnapshotNeedsIt) {
  const RowPosition rp(60, 1);
  Transaction writer = tm_->Begin();
  ASSERT_TRUE(writer.AddWriteSet(rp));
  writer.RegisterVersionWrite(rp, std::nullopt, "gone");
  ASSERT_EQ(writer.PreCommit(), Status::kSuccess);
  ASSERT_TRUE(tm_->HasVersionChain(rp));

  PumpGcCommits();
  // With no active snapshot the newest committed image is redundant (the
  // heap page is authoritative): the whole chain is dropped.
  EXPECT_TRUE(WaitForCondition([&] { return !tm_->HasVersionChain(rp); },
                               std::chrono::seconds(5)));
}

TEST_F(TransactionCoverageTest, GcTrimsOldVersionsBehindPinnedSnapshot) {
  const RowPosition rp(61, 1);
  Transaction w1 = tm_->Begin();
  ASSERT_TRUE(w1.AddWriteSet(rp));
  w1.RegisterVersionWrite(rp, std::nullopt, "v1");
  ASSERT_EQ(w1.PreCommit(), Status::kSuccess);
  Transaction w2 = tm_->Begin();
  ASSERT_TRUE(w2.AddWriteSet(rp));
  w2.RegisterVersionWrite(rp, "v1", "v2");
  ASSERT_EQ(w2.PreCommit(), Status::kSuccess);

  // Pin a snapshot that can still see v2 (but not v1).
  Transaction pin = tm_->Begin(true);
  ASSERT_SUCCESS_AND_EQ(tm_->ReadVersion(pin, rp, std::nullopt), "v2");

  Transaction w3 = tm_->Begin();
  ASSERT_TRUE(w3.AddWriteSet(rp));
  w3.RegisterVersionWrite(rp, "v2", "v3");
  ASSERT_EQ(w3.PreCommit(), Status::kSuccess);

  PumpGcCommits();
  // v1 is trimmed; the chain keeps exactly the versions the pinned snapshot
  // may still read (v2) plus the newest (v3).
  EXPECT_TRUE(WaitForCondition(
      [&] {
        return CountCommittedEntries(tm_->DebugDumpVersionChains(rp.page_id)) <=
               2;
      },
      std::chrono::seconds(5)));
  ASSERT_SUCCESS_AND_EQ(tm_->ReadVersion(pin, rp, std::nullopt), "v2");
  pin.Abort();
}

TEST_F(TransactionCoverageTest, GcErasesShellLeftByAbortedIntent) {
  const RowPosition rp(62, 1);
  Transaction txn = tm_->Begin();
  ASSERT_TRUE(txn.AddWriteSet(rp));
  ASSERT_TRUE(tm_->HasVersionChain(rp));
  // The abort resets the pending owner but leaves an empty chain shell.
  ASSERT_EQ(tm_->Abort(txn), Status::kSuccess);
  ASSERT_TRUE(tm_->HasVersionChain(rp));

  PumpGcCommits();
  EXPECT_TRUE(WaitForCondition([&] { return !tm_->HasVersionChain(rp); },
                               std::chrono::seconds(5)));
}

// ---------------------------------------------------------------------------
// Deadlock policies over the write-intent wait path.
// ---------------------------------------------------------------------------

TEST_F(TransactionCoverageTest, LegacyPolicyGivesUpAfterShortWait) {
  tm_->SetMetricsEnabled(true);
  const RowPosition rp(70, 1);
  Transaction holder = tm_->Begin();
  ASSERT_TRUE(holder.AddWriteSet(rp));

  std::atomic<bool> conflict{true};
  std::thread waiter([&] {
    Transaction t2 = tm_->Begin();
    // kLegacy: bounded 5ms wait, then a conflict.
    conflict = t2.AddWriteSet(rp);
    ASSERT_EQ(tm_->Abort(t2), Status::kSuccess);
  });
  waiter.join();
  EXPECT_FALSE(conflict.load());
  ASSERT_EQ(tm_->Abort(holder), Status::kSuccess);
}

TEST_F(TransactionCoverageTest, WaitDieRefusesYoungerWaiterImmediately) {
  tm_->SetDeadlockPolicy(TransactionManager::DeadlockPolicy::kWaitDie);
  tm_->SetMetricsEnabled(true);
  const RowPosition rp(71, 1);
  Transaction older = tm_->Begin();
  Transaction younger = tm_->Begin();
  ASSERT_TRUE(older.AddWriteSet(rp));
  // Wait-die: the younger transaction dies instead of waiting on an older
  // holder.
  EXPECT_FALSE(younger.AddWriteSet(rp));
  ASSERT_EQ(tm_->Abort(older), Status::kSuccess);
  ASSERT_EQ(tm_->Abort(younger), Status::kSuccess);
}

TEST_F(TransactionCoverageTest, WaitDieOlderWaitsThenProceedsWhenHolderAborts) {
  tm_->SetDeadlockPolicy(TransactionManager::DeadlockPolicy::kWaitDie);
  const RowPosition rp(72, 1);
  Transaction older = tm_->Begin();
  Transaction younger = tm_->Begin();
  ASSERT_TRUE(younger.AddWriteSet(rp));

  std::atomic<bool> acquired{false};
  std::thread waiter([&] {
    // The older transaction is allowed to wait for the younger holder.
    acquired = older.AddWriteSet(rp);
  });
  younger.Abort();
  waiter.join();
  EXPECT_TRUE(acquired.load());
  ASSERT_EQ(tm_->Abort(older), Status::kSuccess);
}

TEST_F(TransactionCoverageTest, WaitDieBoundedWaitTimesOutWhenHolderStalls) {
  tm_->SetDeadlockPolicy(TransactionManager::DeadlockPolicy::kWaitDie);
  tm_->SetWriteIntentWaitLimit(std::chrono::milliseconds(80));
  tm_->SetMetricsEnabled(true);
  const RowPosition rp(73, 1);
  Transaction older = tm_->Begin();
  Transaction holder = tm_->Begin();
  ASSERT_TRUE(holder.AddWriteSet(rp));
  // The older waiter never gets the intent: the safety-valve deadline ends
  // the wait with a conflict.
  EXPECT_FALSE(older.AddWriteSet(rp));
  ASSERT_EQ(tm_->Abort(holder), Status::kSuccess);
  ASSERT_EQ(tm_->Abort(older), Status::kSuccess);
}

TEST_F(TransactionCoverageTest, WoundWaitOlderPreemptsYoungerHolder) {
  tm_->SetDeadlockPolicy(TransactionManager::DeadlockPolicy::kWoundWait);
  const RowPosition rp(74, 1);
  Transaction older = tm_->Begin();
  Transaction younger = tm_->Begin();
  ASSERT_TRUE(younger.AddWriteSet(rp));

  std::atomic<bool> acquired{false};
  std::thread waiter([&] {
    // The older arrival wounds the younger holder through the active
    // registry, then takes the intent once the victim unwinds.
    acquired = older.AddWriteSet(rp);
  });
  EXPECT_TRUE(WaitForCondition([&] { return younger.IsWounded(); },
                               std::chrono::seconds(5)));
  ASSERT_EQ(tm_->Abort(younger), Status::kSuccess);
  waiter.join();
  EXPECT_TRUE(acquired.load());
  ASSERT_EQ(tm_->Abort(older), Status::kSuccess);
}

TEST_F(TransactionCoverageTest, WoundWaitVictimWoundedWhileWaitingElsewhere) {
  tm_->SetDeadlockPolicy(TransactionManager::DeadlockPolicy::kWoundWait);
  tm_->SetMetricsEnabled(true);
  const RowPosition queue_row(741, 1);
  const RowPosition victim_row(742, 1);
  // Ids: queue_holder(1) < wounder(2) < victim(3).  The victim is younger
  // than queue_holder, so it WAITS for the queue row; the wounder is older
  // than the victim, so touching the victim's row wounds it -- while the
  // victim is blocked inside its unrelated wait.
  Transaction queue_holder = tm_->Begin();
  Transaction wounder = tm_->Begin();
  Transaction victim = tm_->Begin();
  ASSERT_TRUE(queue_holder.AddWriteSet(queue_row));
  ASSERT_TRUE(victim.AddWriteSet(victim_row));

  const auto attempts_before = tm_->RuntimeStats().write_intent_attempts;
  std::atomic<bool> victim_conflict{true};
  std::thread victim_waiter([&] {
    victim_conflict = victim.AddWriteSet(queue_row);
    // Wounded transactions never make progress: the executor unwinds.
    if (victim_conflict.load()) {
      return;
    }
    ASSERT_EQ(tm_->Abort(victim), Status::kSuccess);
  });
  // The entry of the victim's acquire is observable through the metrics;
  // once it is inside, the wound lands mid-wait.
  EXPECT_TRUE(WaitForCondition(
      [&] {
        return tm_->RuntimeStats().write_intent_attempts > attempts_before;
      },
      std::chrono::seconds(5)));

  std::atomic<bool> wounder_acquired{false};
  std::thread wounder_thread([&] {
    // Wounds the victim through the registry, then waits for the victim's
    // abort to release the row.
    wounder_acquired = wounder.AddWriteSet(victim_row);
    ASSERT_EQ(tm_->Abort(wounder), Status::kSuccess);
  });
  victim_waiter.join();
  EXPECT_FALSE(victim_conflict.load());
  EXPECT_TRUE(victim.IsWounded());
  wounder_thread.join();
  EXPECT_TRUE(wounder_acquired.load());
  ASSERT_EQ(tm_->Abort(queue_holder), Status::kSuccess);
}

TEST_F(TransactionCoverageTest, WoundWaitBoundedWaitTimesOutWhenVictimStalls) {
  tm_->SetDeadlockPolicy(TransactionManager::DeadlockPolicy::kWoundWait);
  tm_->SetWriteIntentWaitLimit(std::chrono::milliseconds(80));
  tm_->SetMetricsEnabled(true);
  const RowPosition rp(75, 1);
  Transaction older = tm_->Begin();
  Transaction holder = tm_->Begin();
  ASSERT_TRUE(holder.AddWriteSet(rp));
  // The wound is delivered immediately, but the victim never releases here;
  // the wait limit converts the stall into a conflict.
  EXPECT_FALSE(older.AddWriteSet(rp));
  EXPECT_TRUE(holder.IsWounded());
  ASSERT_EQ(tm_->Abort(holder), Status::kSuccess);
  ASSERT_EQ(tm_->Abort(older), Status::kSuccess);
}

TEST_F(TransactionCoverageTest,
       DeadlockDetectBoundedWaitTimesOutAndDropsItsEdge) {
  tm_->SetDeadlockPolicy(TransactionManager::DeadlockPolicy::kDeadlockDetect);
  tm_->SetWriteIntentWaitLimit(std::chrono::milliseconds(80));
  tm_->SetMetricsEnabled(true);
  const RowPosition rp(76, 1);
  Transaction holder = tm_->Begin();
  Transaction waiter_txn = tm_->Begin();
  ASSERT_TRUE(holder.AddWriteSet(rp));
  // The wait-for edge is registered, then retracted by the escape hatch.
  EXPECT_FALSE(waiter_txn.AddWriteSet(rp));
  ASSERT_EQ(tm_->Abort(holder), Status::kSuccess);
  ASSERT_EQ(tm_->Abort(waiter_txn), Status::kSuccess);
}

TEST_F(TransactionCoverageTest, DeadlockDetectGrantsAfterHolderMovesOn) {
  tm_->SetDeadlockPolicy(TransactionManager::DeadlockPolicy::kDeadlockDetect);
  const RowPosition rp(77, 1);
  const RowPosition other(77, 2);
  Transaction holder = tm_->Begin();
  Transaction bystander = tm_->Begin();
  ASSERT_TRUE(holder.AddWriteSet(rp));
  ASSERT_TRUE(bystander.AddWriteSet(other));

  std::atomic<bool> acquired{false};
  std::atomic<bool> bystander_acquired{false};
  std::thread waiter([&] {
    Transaction t2 = tm_->Begin();
    // Registers a wait-for edge and waits; the holder's abort releases it.
    acquired = t2.AddWriteSet(rp);
    ASSERT_EQ(tm_->Abort(t2), Status::kSuccess);
  });
  std::thread bystander_waiter([&] {
    Transaction t3 = tm_->Begin();
    // A second edge whose holder is NOT the aborted transaction: it must
    // survive the holder's edge cleanup and be served when the bystander
    // releases its own row.
    bystander_acquired = t3.AddWriteSet(other);
    ASSERT_EQ(tm_->Abort(t3), Status::kSuccess);
  });
  // Aborting |holder| drops the waiter's edge (holder match) while walking
  // past the unrelated bystander edge.
  ASSERT_EQ(tm_->Abort(holder), Status::kSuccess);
  waiter.join();
  EXPECT_TRUE(acquired.load());
  // The bystander edge is still pending here; release it now.
  ASSERT_EQ(tm_->Abort(bystander), Status::kSuccess);
  bystander_waiter.join();
  EXPECT_TRUE(bystander_acquired.load());
}

TEST_F(TransactionCoverageTest, OwnerReacquiresOwnPendingUnderEveryPolicy) {
  const std::array<TransactionManager::DeadlockPolicy, 4> policies{
      TransactionManager::DeadlockPolicy::kLegacy,
      TransactionManager::DeadlockPolicy::kWaitDie,
      TransactionManager::DeadlockPolicy::kWoundWait,
      TransactionManager::DeadlockPolicy::kDeadlockDetect};
  for (const auto policy : policies) {
    Reset();
    tm_->SetDeadlockPolicy(policy);
    const RowPosition rp(770,
                         static_cast<uint32_t>(static_cast<int>(policy)) + 1);
    Transaction owner = tm_->Begin();
    // The waiting acquisition path recognizes its own pending intent and
    // returns it without installing anything new -- under every policy.
    ASSERT_TRUE(tm_->AcquireWriteIntent(owner, rp, /*wait=*/true));
    EXPECT_TRUE(tm_->AcquireWriteIntent(owner, rp, /*wait=*/true));
    ASSERT_TRUE(owner.AddWriteSet(rp));
    ASSERT_EQ(tm_->Abort(owner), Status::kSuccess);
  }
}

TEST_F(TransactionCoverageTest, DeadlockDetectorWoundsCycleParticipants) {
  tm_->SetDeadlockPolicy(TransactionManager::DeadlockPolicy::kDeadlockDetect);
  const RowPosition row_a(78, 1);
  const RowPosition row_b(79, 1);
  Transaction txn_a = tm_->Begin();
  Transaction txn_b = tm_->Begin();
  ASSERT_TRUE(txn_a.AddWriteSet(row_a));
  ASSERT_TRUE(txn_b.AddWriteSet(row_b));

  std::atomic<bool> a_acquired{false};
  std::atomic<bool> b_conflict{true};
  std::thread waiter_a([&] {
    // A waits for B's row: edge A -> B.  Once the victim below unwinds and
    // releases row_b, this strict wait completes (no first-updater-wins
    // abort): the detector spares the older transaction.
    a_acquired = txn_a.AddWriteSet(row_b);
  });
  std::thread waiter_b([&] {
    // B waits for A's row: edge B -> A closes the cycle.  The detector
    // wounds the maximum-id (youngest) participant of the cycle -- both
    // traversals of a two-node cycle select the same victim.
    b_conflict = txn_b.AddWriteSet(row_a);
    if (!b_conflict.load()) {
      // The executor unwinds a wounded transaction: aborting releases the
      // intents B still holds, which unblocks the older waiter.
      ASSERT_EQ(tm_->Abort(txn_b), Status::kSuccess);
    }
  });
  EXPECT_TRUE(WaitForCondition([&] { return txn_b.IsWounded(); },
                               std::chrono::seconds(5)));
  waiter_a.join();
  waiter_b.join();

  // Exactly the youngest cycle participant was victimized.
  EXPECT_TRUE(txn_b.IsWounded());
  EXPECT_FALSE(txn_a.IsWounded());
  EXPECT_TRUE(a_acquired.load());
  EXPECT_FALSE(b_conflict.load());

  ASSERT_EQ(tm_->Abort(txn_a), Status::kSuccess);
  Transaction fresh = tm_->Begin();
  EXPECT_TRUE(fresh.AddWriteSet(row_a));
  EXPECT_TRUE(fresh.AddWriteSet(row_b));
  ASSERT_EQ(tm_->Abort(fresh), Status::kSuccess);
}

// ---------------------------------------------------------------------------
// Durability failures: a WAL that cannot be written must never leave a
// half-finished transaction behind.
// ---------------------------------------------------------------------------

TEST_F(TransactionCoverageTest,
       PreCommitAddLogFailureAbortsAndReleasesIntents) {
  const RowPosition rp(80, 1);
  ForceWalFailure(l_.get());

  Transaction writer = tm_->Begin();
  ASSERT_TRUE(writer.AddWriteSet(rp));
  writer.RegisterVersionWrite(rp, std::nullopt, "doomed");
  // The commit record can never be appended: no version was published, so
  // the transaction reports aborted and its intents are released.
  EXPECT_NE(writer.PreCommit(), Status::kSuccess);
  EXPECT_TRUE(writer.IsFinished());

  Transaction retry = tm_->Begin();
  EXPECT_TRUE(retry.AddWriteSet(rp));
  // Finishing |retry| would need the (dead) WAL for its terminator record;
  // its destructor releases the registry slot instead.
  retry.RegisterVersionWrite(rp, "x", std::nullopt);
}

TEST_F(TransactionCoverageTest,
       PreCommitKeepsCommittedWhenFlushFailsAfterPublish) {
  const RowPosition rp(81, 1);
  Transaction writer = tm_->Begin();
  ASSERT_TRUE(writer.AddWriteSet(rp));
  writer.RegisterVersionWrite(rp, std::nullopt, "published");

  // Redirect the WAL before the commit record is even appended: the append
  // still succeeds (buffered), the flush worker dies on it, and the
  // synchronous-commit barrier fails after the versions were published.
  PointWalAtDevFull(l_.get());
  EXPECT_NE(writer.PreCommit(), Status::kSuccess);
  EXPECT_TRUE(writer.IsFinished());

  // Published versions cannot be retracted: a fresh snapshot sees the row,
  // and reading it records a durability dependency on the unflushed commit.
  Transaction reader = tm_->Begin(true);
  ASSERT_SUCCESS_AND_EQ(tm_->ReadVersion(reader, rp, std::nullopt),
                        "published");
  EXPECT_GT(reader.DurabilityDependence(), 0U);
  // A read-only abort never touches the (dead) WAL, so it still succeeds.
  ASSERT_EQ(reader.Abort(), Status::kSuccess);
}

TEST_F(TransactionCoverageTest,
       ReadOnlyPreCommitFailsWhenDependencyBarrierBreaks) {
  const RowPosition rp(82, 1);
  Transaction writer = tm_->Begin();
  ASSERT_TRUE(writer.AddWriteSet(rp));
  writer.RegisterVersionWrite(rp, std::nullopt, "v1");
  ASSERT_EQ(writer.PreCommit(), Status::kSuccess);
  writer.CommitWait();

  ForceWalFailure(l_.get());
  Transaction reader = tm_->Begin(true);
  ASSERT_SUCCESS_AND_EQ(tm_->ReadVersion(reader, rp, std::nullopt), "v1");
  ASSERT_GT(reader.DurabilityDependence(), 0U);
  // The D4 barrier for the observed commit cannot be satisfied anymore.
  EXPECT_NE(reader.PreCommit(), Status::kSuccess);
  EXPECT_TRUE(reader.IsFinished());
  // A finished transaction aborts as a no-op.
  ASSERT_EQ(reader.Abort(), Status::kSuccess);
}

// ---------------------------------------------------------------------------
// Abort: undo walk over a real log chain (needs a RecoveryManager).
// ---------------------------------------------------------------------------

class TransactionCoverageRecoveryTest : public ::testing::Test {
 public:
  void SetUp() override {
    db_name_ = "transaction_coverage_rec-" + RandomString() + ".db";
    log_name_ = "transaction_coverage_rec-" + RandomString() + ".log";
    pm_ = PageManager::Create(db_name_, 10).MoveValue();
    l_ = Logger::Create(log_name_).MoveValue();
    rm_ = std::make_unique<RecoveryManager>(log_name_, pm_->GetPool());
    tm_ = std::make_unique<TransactionManager>(pm_.get(), l_.get(), rm_.get());
  }

  void TearDown() {
    tm_.reset();
    rm_.reset();
    l_.reset();
    pm_.reset();
    std::ignore = std::remove(db_name_.c_str());
    std::ignore = std::remove(log_name_.c_str());
  }

 protected:
  std::string db_name_;
  std::string log_name_;
  std::unique_ptr<PageManager> pm_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<RecoveryManager> rm_;
  std::unique_ptr<TransactionManager> tm_;
};

TEST_F(TransactionCoverageRecoveryTest, AbortWalksUndoChainAndTerminates) {
  Transaction txn = tm_->Begin();
  // A non-page-manipulation record gives the undo walk real chain entries
  // to read back without depending on page contents.  The FIRST record of a
  // fresh WAL starts at LSN 0, which doubles as the "no chain" sentinel --
  // append a second record so prev_lsn_ is a walkable non-zero LSN.
  ASSERT_TRUE(txn.AppendLog(LogRecord(txn.PrevLSN(), txn.ID(), LogType::kBegin))
                  .HasValue());
  ASSERT_TRUE(txn.AppendLog(LogRecord(txn.PrevLSN(), txn.ID(), LogType::kBegin))
                  .HasValue());
  ASSERT_GT(txn.PrevLSN(), 0U);
  ASSERT_EQ(l_->WaitForDurable(txn.PrevRecordEndLSN()), Status::kSuccess);

  ASSERT_EQ(txn.Abort(), Status::kSuccess);
  EXPECT_TRUE(txn.IsFinished());
  // The abort terminator was appended above the undone chain.
  EXPECT_GT(txn.PrevLSN(), 0U);
}

TEST_F(TransactionCoverageRecoveryTest, AbortSurvivesUnreadableTailAndDeadWal) {
  // A slow flush interval guarantees the first flush attempt with payload
  // happens only after the WAL fd was redirected to /dev/full: the buffered
  // records can never reach the file, so the chain record is unreadable.
  const std::string slow_log =
      "transaction_coverage_slow2-" + RandomString() + ".log";
  const std::string slow_db =
      "transaction_coverage_slow2-" + RandomString() + ".db";
  auto slow_pm = PageManager::Create(slow_db, 10).MoveValue();
  auto slow_logger =
      Logger::Create(slow_log, size_t{1024} * 1024 * 8, /*every_ms=*/300)
          .MoveValue();
  {
    RecoveryManager slow_rm(slow_log, slow_pm->GetPool());
    TransactionManager slow_tm(slow_pm.get(), slow_logger.get(), &slow_rm);

    Transaction txn = slow_tm.Begin();
    // The first record of a fresh WAL starts at LSN 0, which doubles as the
    // "no chain" sentinel; append it so the next record gets a walkable
    // non-zero prev_lsn_.
    ASSERT_TRUE(
        txn.AppendLog(LogRecord(txn.PrevLSN(), txn.ID(), LogType::kBegin))
            .HasValue());
    ASSERT_TRUE(
        txn.AppendLog(LogRecord(txn.PrevLSN(), txn.ID(), LogType::kBegin))
            .HasValue());
    ASSERT_GT(txn.PrevLSN(), 0U);

    PointWalAtDevFull(slow_logger.get());
    // The durability wait inside Abort fails (warned), the chain record is
    // unreadable (nothing was flushed), and the terminator append fails on
    // the dead logger: the abort reports failure but still finishes
    // cleanly.
    EXPECT_NE(txn.Abort(), Status::kSuccess);
    EXPECT_TRUE(txn.IsFinished());
  }
  std::ignore = std::remove(slow_log.c_str());
  std::ignore = std::remove(slow_db.c_str());
}

TEST_F(TransactionCoverageRecoveryTest, AbortStopsUndoWhenCompensationFails) {
  Transaction txn = tm_->Begin();
  // Sentinel-dodging preamble: see AbortSurvivesUnreadableTailAndDeadWal.
  ASSERT_TRUE(txn.AppendLog(LogRecord(txn.PrevLSN(), txn.ID(), LogType::kBegin))
                  .HasValue());
  // An insert record for a page this database never had: reading it back
  // works, but undoing it needs a compensation record the dead WAL cannot
  // take, so the undo walk stops early and reports the failure.
  ASSERT_TRUE(txn.InsertLog(999999, 0, "row").HasValue());
  ASSERT_EQ(l_->WaitForDurable(txn.PrevRecordEndLSN()), Status::kSuccess);

  ForceWalFailure(l_.get());
  EXPECT_NE(txn.Abort(), Status::kSuccess);
  EXPECT_TRUE(txn.IsFinished());
}

// ---------------------------------------------------------------------------
// Row lock manager gaps.
// ---------------------------------------------------------------------------

TEST(LockManagerCoverageTest, SharedLockRefusedWhileExclusiveHeld) {
  LockManager lm;
  const RowPosition row(90, 1);
  ASSERT_TRUE(lm.GetExclusiveLock(row, 1, false));
  // Any exclusive holder, even for a different owner, blocks shared users.
  EXPECT_FALSE(lm.GetSharedLock(row, 2));
  EXPECT_TRUE(lm.ReleaseExclusiveLock(row, 1));
  EXPECT_TRUE(lm.GetSharedLock(row, 2));
  EXPECT_TRUE(lm.ReleaseSharedLock(row, 2));
}

TEST(LockManagerCoverageTest, NonWaitingExclusiveRefusedWhileHeld) {
  LockManager lm;
  const RowPosition row(91, 1);
  ASSERT_TRUE(lm.GetExclusiveLock(row, 1, false));
  EXPECT_FALSE(lm.GetExclusiveLock(row, 2, false));
  EXPECT_FALSE(lm.GetSharedLock(row, 2));
  EXPECT_TRUE(lm.ReleaseExclusiveLock(row, 1));
}

TEST(LockManagerCoverageTest, TryUpgradeIdempotentForHolderOnly) {
  LockManager lm;
  const RowPosition row(92, 1);
  ASSERT_TRUE(lm.GetExclusiveLock(row, 2, false));
  // Already exclusively held: the holder's re-upgrade succeeds, a foreign
  // owner's is refused.
  EXPECT_FALSE(lm.TryUpgradeLock(row, 1));
  EXPECT_TRUE(lm.TryUpgradeLock(row, 2));
  EXPECT_TRUE(lm.TryUpgradeLock(row, 2));
  EXPECT_TRUE(lm.ReleaseExclusiveLock(row, 2));
  EXPECT_FALSE(lm.ReleaseExclusiveLock(row, 2));

  // A sole shared owner can promote; a second shared holder cannot.
  ASSERT_TRUE(lm.GetSharedLock(row, 1));
  ASSERT_TRUE(lm.GetSharedLock(row, 3));
  EXPECT_FALSE(lm.TryUpgradeLock(row, 1));
  EXPECT_TRUE(lm.ReleaseSharedLock(row, 3));
  EXPECT_TRUE(lm.TryUpgradeLock(row, 1));
  EXPECT_TRUE(lm.ReleaseExclusiveLock(row, 1));
}

TEST(LockManagerCoverageTest, LockManagerStreamsCounts) {
  LockManager lm;
  const RowPosition shared_row(93, 1);
  const RowPosition exclusive_row(94, 1);
  ASSERT_TRUE(lm.GetSharedLock(shared_row, 1));
  ASSERT_TRUE(lm.GetSharedLock(shared_row, 2));
  ASSERT_TRUE(lm.GetExclusiveLock(exclusive_row, 3, false));
  std::ostringstream oss;
  oss << lm;
  EXPECT_EQ(oss.str(), "LockManager(shared=1, exclusive=1)");
  EXPECT_TRUE(lm.ReleaseExclusiveLock(exclusive_row, 3));
  EXPECT_TRUE(lm.ReleaseSharedLock(shared_row, 1));
  EXPECT_TRUE(lm.ReleaseSharedLock(shared_row, 2));
}

TEST(LockManagerCoverageTest, ExclusiveWaitExtendsWhileReleasesProgress) {
  LockManager lm;
  const RowPosition held(95, 1);
  const RowPosition unrelated(96, 1);
  ASSERT_TRUE(lm.GetExclusiveLock(held, 1));

  std::atomic<bool> contender_parked{false};
  std::atomic<bool> acquired{false};
  std::thread contender([&] {
    // Retry until granted: with release progress elsewhere the wait is
    // extended instead of reported as a timeout.  A short patience with a
    // much faster release cadence elsewhere makes every timed-out window
    // observe progress, exercising the extension branch.
    while (!acquired.load()) {
      contender_parked.store(true);
      acquired = lm.GetExclusiveLock(held, 2, std::chrono::milliseconds(20));
    }
  });
  // The churn runs on its own thread so the release activity continues
  // even if this thread is descheduled: the contender's 20ms windows then
  // always observe a release somewhere in the table and keep extending.
  std::atomic<bool> stop_churn{false};
  std::thread churn([&] {
    while (!stop_churn.load(std::memory_order_relaxed)) {
      if (lm.GetExclusiveLock(unrelated, 9, false)) {
        std::ignore = lm.ReleaseExclusiveLock(unrelated, 9);
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(4));
    }
  });
  // Hold the row through a timed churn window that starts only once the
  // contender is (about to be) parked, so its first wait overlaps the
  // release activity, then grant the lock.
  while (!contender_parked.load()) {
    std::this_thread::yield();
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  ASSERT_TRUE(lm.ReleaseExclusiveLock(held, 1));
  contender.join();
  stop_churn.store(true);
  churn.join();
  EXPECT_TRUE(acquired.load());
}

}  // namespace tinylamb
