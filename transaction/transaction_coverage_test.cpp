/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
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
#include "common/platform/file_io.hpp"
#include "common/platform/process.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
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
  // One retry for empty diagnostics: a failed Prepare with no LastError is
  // the external parse child being transiently unavailable under load, not
  // a legitimate SQL error (real errors always carry a message).
  StatusOr<Executor> prepared = engine->Prepare(*ctx, sql);
  if (!prepared.HasValue()) {
    if (engine->LastError().empty()) {
      prepared = engine->Prepare(*ctx, sql);
    }
    if (!prepared.HasValue()) {
      throw std::runtime_error(engine->LastError());
    }
  }
  std::vector<Row> rows;
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    rows.push_back(row);
    row = Row();
  }
  const Status st = prepared.Value()->GetStatus();
  if (st != Status::kSuccess) {
    if (st != Status::kSuccess) {
      std::ostringstream diag;
      diag << st;
      throw std::runtime_error(st.GetMessage().empty() ? diag.str()
                                                       : st.GetMessage());
    }
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
  const int full_fd = platform::OpenWriteFailingDescriptor();
  ASSERT_GE(full_fd, 0);
  ASSERT_EQ(platform::Dup2(full_fd, logger->Fd()), logger->Fd());
  ASSERT_EQ(platform::CloseFile(full_fd), 0);
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
      GTEST_SKIP() << "skipped";
    }
    database_ = Database::Create("transaction_coverage_test-" + RandomString())
                    .MoveValue();
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
      tinylamb::RemoveLogAndSegments(log_name_);
      log_name_.clear();
    }
    log_name_ = "transaction_coverage-" + RandomString() + ".log";
    l_ = Logger::Create(log_name_).MoveValue();
    tm_ = std::make_unique<TransactionManager>(nullptr, l_.get(), nullptr);
  }

  void TearDown() override {
    tm_.reset();
    l_.reset();
    if (!log_name_.empty()) {
      tinylamb::RemoveLogAndSegments(log_name_);
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

TEST_F(TransactionCoverageTest, PageChainPresenceIsExactPerPage) {
  // Pages 100 and 356 collide on PageChainShardIndex (131 * 256 ≡ 0 mod
  // 256), so a chain on one page must not poison the shard sibling: the
  // per-page answer is what gates the physical-scan fast path.
  const page_id_t poisoned = 100;
  const page_id_t shard_sibling = 356;
  const RowPosition with_chain(poisoned, 1);
  const RowPosition other_slot(poisoned, 2);
  Transaction txn = tm_->Begin();
  ASSERT_TRUE(txn.AddWriteSet(with_chain));
  EXPECT_TRUE(tm_->HasVersionChain(with_chain));
  EXPECT_TRUE(tm_->HasPageVersionChains(poisoned));
  // Same page, other slot: no chain on that row.
  EXPECT_FALSE(tm_->HasVersionChain(other_slot));
  // Same shard, other page: exact — this page has no chain, so physical-read
  // eligibility must survive the shard counter being nonzero.
  EXPECT_FALSE(tm_->HasPageVersionChains(shard_sibling));
  EXPECT_FALSE(tm_->HasVersionChain(RowPosition(shard_sibling, 1)));
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

TEST_F(TransactionCoverageTest, WriteSetSpillRevisitPublishesAllEntries) {
  std::vector<RowPosition> rows;
  rows.reserve(65);
  for (size_t slot = 0; slot < 65; ++slot) {
    rows.emplace_back(120, static_cast<slot_t>(slot));
  }

  Transaction anchor = tm_->Begin(true);
  Transaction writer = tm_->Begin();
  for (const RowPosition& row : rows) {
    ASSERT_TRUE(writer.AddWriteSet(row));
  }
  ASSERT_TRUE(writer.TryAddWriteSet(rows[63]));
  ASSERT_TRUE(writer.TryAddWriteSet(rows[64]));
  for (size_t slot = 0; slot < rows.size(); ++slot) {
    const std::string value = "value-" + std::to_string(slot);
    writer.RegisterVersionWrite(rows[slot], std::nullopt, value);
  }
  ASSERT_EQ(writer.PreCommit(), Status::kSuccess);

  Transaction reader = tm_->Begin(true);
  for (size_t slot = 0; slot < rows.size(); ++slot) {
    const std::string value = "value-" + std::to_string(slot);
    ASSERT_SUCCESS_AND_EQ(tm_->ReadVersion(reader, rows[slot], std::nullopt),
                          value);
  }
  ASSERT_EQ(reader.PreCommit(), Status::kSuccess);
  ASSERT_EQ(anchor.PreCommit(), Status::kSuccess);
}

TEST_F(TransactionCoverageTest,
       WriteSetSpillBeforeImageReacquiresNonLastEntry) {
  std::vector<RowPosition> rows;
  rows.reserve(65);
  for (size_t slot = 0; slot < 65; ++slot) {
    rows.emplace_back(121, static_cast<slot_t>(slot));
  }

  Transaction writer = tm_->Begin();
  for (const RowPosition& row : rows) {
    ASSERT_TRUE(writer.AddWriteSet(row));
  }
  ASSERT_TRUE(writer.AddWriteSet(rows[17], std::string_view("before")));

  Transaction reader = tm_->Begin(true);
  ASSERT_SUCCESS_AND_EQ(tm_->ReadVersion(reader, rows[17], std::nullopt),
                        "before");
  ASSERT_EQ(reader.PreCommit(), Status::kSuccess);
  ASSERT_EQ(tm_->Abort(writer), Status::kSuccess);
}

TEST_F(TransactionCoverageTest, WriteSetSpillMovePreservesEntries) {
  std::vector<RowPosition> rows;
  rows.reserve(65);
  for (size_t slot = 0; slot < 65; ++slot) {
    rows.emplace_back(123, static_cast<slot_t>(slot));
  }

  Transaction source = tm_->Begin();
  for (const RowPosition& row : rows) {
    ASSERT_TRUE(source.AddWriteSet(row));
  }
  Transaction moved(std::move(source));
  moved.RegisterVersionWrite(rows[17], std::nullopt, "moved");
  ASSERT_EQ(moved.PreCommit(), Status::kSuccess);

  Transaction reader = tm_->Begin(true);
  ASSERT_SUCCESS_AND_EQ(tm_->ReadVersion(reader, rows[17], std::nullopt),
                        "moved");
  ASSERT_EQ(reader.PreCommit(), Status::kSuccess);
}

TEST_F(TransactionCoverageTest, WriteSetSpillReleaseErasesSameEntry) {
  std::vector<RowPosition> rows;
  rows.reserve(65);
  for (size_t slot = 0; slot < 65; ++slot) {
    rows.emplace_back(122, static_cast<slot_t>(slot));
  }

  Transaction writer = tm_->Begin();
  for (const RowPosition& row : rows) {
    ASSERT_TRUE(writer.AddWriteSet(row));
  }
  writer.ReleaseWriteIntent(rows[17]);
  ASSERT_FALSE(tm_->HasVersionChain(rows[17]));

  Transaction contender = tm_->Begin();
  ASSERT_TRUE(contender.TryAddWriteSet(rows[17]));
  EXPECT_FALSE(writer.TryAddWriteSet(rows[17]));
  ASSERT_EQ(tm_->Abort(contender), Status::kSuccess);

  ASSERT_TRUE(writer.TryAddWriteSet(rows[17]));
  writer.RegisterVersionWrite(rows[17], std::nullopt, "reinserted");
  ASSERT_EQ(writer.PreCommit(), Status::kSuccess);

  Transaction reader = tm_->Begin(true);
  ASSERT_SUCCESS_AND_EQ(tm_->ReadVersion(reader, rows[17], std::nullopt),
                        "reinserted");
  ASSERT_EQ(reader.PreCommit(), Status::kSuccess);
}

TEST_F(TransactionCoverageTest, InvalidatePageVersionsErasesSpilledWriteSet) {
  const page_id_t pid = 123;
  const RowPosition other_page(pid + 1, 1);
  std::vector<RowPosition> rows;
  rows.reserve(65);
  for (size_t slot = 0; slot < 65; ++slot) {
    rows.emplace_back(pid, static_cast<slot_t>(slot));
  }

  Transaction reclaimer = tm_->Begin();
  for (const RowPosition& row : rows) {
    ASSERT_TRUE(reclaimer.AddWriteSet(row));
  }
  ASSERT_TRUE(reclaimer.AddWriteSet(other_page));

  reclaimer.InvalidatePageVersions(pid);
  EXPECT_TRUE(tm_->HasVersionChain(other_page));
  EXPECT_FALSE(tm_->HasVersionChain(rows[17]));
  ASSERT_TRUE(reclaimer.AddWriteSet(rows[17]));
  EXPECT_TRUE(tm_->HasVersionChain(rows[17]));
  ASSERT_EQ(tm_->Abort(reclaimer), Status::kSuccess);
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
  // nullopt = the latched page image is authoritative (no per-read copy).
  ASSERT_SUCCESS_AND_EQ(tm_->ReadVersion(writer, rp, "phys"),
                        std::optional<std::string>());
  {
    // Another snapshot falls back to the physical image the same way: the
    // unstaged intent of somebody else must not hide the row.
    Transaction reader = tm_->Begin(true);
    ASSERT_SUCCESS_AND_EQ(tm_->ReadVersion(reader, rp, "phys2"),
                          std::optional<std::string>());
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

  // A reader holding a pre-write snapshot keeps the version chains alive:
  // GarbageCollectVersions erases chains whose newest committed begin_ts is
  // at or below the oldest active snapshot, so under load a GC pass racing
  // the dump could otherwise reclaim exactly the chains under inspection.
  Transaction anchor = tm_->Begin(true);

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
  SCOPED_TRACE(dump);
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
  }
  EXPECT_FALSE(tm_->IsActive(doomed_id));

  Transaction txn = tm_->Begin();
  EXPECT_TRUE(tm_->IsActive(txn.ID()));
  ASSERT_EQ(txn.PreCommit(), Status::kSuccess);
  EXPECT_FALSE(tm_->IsActive(txn.ID()));
}

TEST_F(TransactionCoverageTest, TransactionCanOutliveDetachedManager) {
  auto transaction = std::make_unique<Transaction>(tm_->Begin());
  tm_.reset();
  transaction.reset();
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

TEST_F(TransactionCoverageTest, NoWaitFailsOlderWaiterImmediately) {
  // kNoWait (single-threaded executors such as the PostgreSQL server's
  // event loop): even the older transaction that wait-die would let wait
  // fails at once, because its holder cannot progress while it waits.
  tm_->SetDeadlockPolicy(TransactionManager::DeadlockPolicy::kNoWait);
  tm_->SetWriteIntentWaitLimit(std::chrono::milliseconds(5000));
  const RowPosition rp(76, 1);
  Transaction older = tm_->Begin();
  Transaction younger = tm_->Begin();
  ASSERT_TRUE(younger.AddWriteSet(rp));
  const auto begin = std::chrono::steady_clock::now();
  EXPECT_FALSE(older.AddWriteSet(rp));
  EXPECT_LT(std::chrono::steady_clock::now() - begin,
            std::chrono::milliseconds(1000));
  // The holder keeps its intent and finishes normally; afterwards the row
  // is free again.
  ASSERT_EQ(tm_->Abort(younger), Status::kSuccess);
  Transaction later = tm_->Begin();
  EXPECT_TRUE(later.AddWriteSet(rp));
  ASSERT_EQ(tm_->Abort(later), Status::kSuccess);
  ASSERT_EQ(tm_->Abort(older), Status::kSuccess);
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

TEST_F(TransactionCoverageTest, IntentWaitTimeoutBumpsTheTimeoutGauge) {
  tm_->SetDeadlockPolicy(TransactionManager::DeadlockPolicy::kDeadlockDetect);
  tm_->SetWriteIntentWaitLimit(std::chrono::milliseconds(80));
  const RowPosition rp(78, 1);
  Transaction holder = tm_->Begin();
  Transaction waiter_txn = tm_->Begin();
  ASSERT_TRUE(holder.AddWriteSet(rp));
  const uint64_t timeouts_before =
      tm_->RuntimeStats().write_intent_wait_timeouts;
  EXPECT_FALSE(waiter_txn.AddWriteSet(rp));
  // Every escape-hatch give-up is a multi-second stall in production, so the
  // gauge must move: an operator watching mvcc.write_intent.wait_timeouts
  // can see the latch-cycle rate without parsing logs.
  EXPECT_EQ(tm_->RuntimeStats().write_intent_wait_timeouts,
            timeouts_before + 1);
  ASSERT_EQ(tm_->Abort(holder), Status::kSuccess);
  ASSERT_EQ(tm_->Abort(waiter_txn), Status::kSuccess);
}

namespace {
// Scoped setenv for the startup-knob tests below; restores on scope exit.
class WaitLimitEnv {
 public:
  WaitLimitEnv(const char* name, const std::string& value) : name_(name) {
    const char* old = std::getenv(name);
    if (old != nullptr) {
      old_ = old;
    }
    setenv(name, value.c_str(), 1);
  }
  ~WaitLimitEnv() {
    if (old_.has_value()) {
      setenv(name_, old_->c_str(), 1);
    } else {
      unsetenv(name_);
    }
  }
  WaitLimitEnv(const WaitLimitEnv&) = delete;
  WaitLimitEnv& operator=(const WaitLimitEnv&) = delete;
  WaitLimitEnv(WaitLimitEnv&&) = delete;
  WaitLimitEnv& operator=(WaitLimitEnv&&) = delete;

 private:
  const char* name_;
  std::optional<std::string> old_;
};

// The env knob is read in the TransactionManager constructor, so each case
// needs its own logger + manager pair (and its own log file to clean up).
int64_t WaitLimitUnderEnv(const char* value) {
  const std::string log_name =
      "transaction_coverage-env-" + RandomString() + ".log";
  WaitLimitEnv env("TINYLAMB_WRITE_INTENT_WAIT_MS", value);
  auto logger = Logger::Create(log_name).MoveValue();
  TransactionManager tm(nullptr, logger.get(), nullptr);
  RemoveLogAndSegments(log_name);
  return tm.GetWriteIntentWaitLimit().count();
}
}  // namespace

TEST(TransactionManagerEnvTest, WriteIntentWaitLimitEnvOverride) {
  EXPECT_EQ(WaitLimitUnderEnv("123"), 123);
  EXPECT_EQ(WaitLimitUnderEnv("abc"), 5'000);
  EXPECT_EQ(WaitLimitUnderEnv("0"), 5'000);
  EXPECT_EQ(WaitLimitUnderEnv("-5"), 5'000);
}

// The index-undo reservation drain backs the B+Tree's structural retries: a
// timeout must report kConflicts only while a foreign reservation still
// holds bytes, and a drain that observes the reservations gone -- whether by
// waking or by expiring at the exact resolve moment -- must report success.
// The inverted form of that ternary turned "unblocked at the deadline" into a
// statement failure (recovery_extra_test concurrency wedge, 2026-10).
TEST_F(TransactionCoverageTest, IndexReservationDrainSemantics) {
  tm_->SetWriteIntentWaitLimit(std::chrono::milliseconds(5'000));
  constexpr page_id_t kPage = 900;
  Transaction holder = tm_->Begin();
  Transaction waiter = tm_->Begin();

  // Nothing outstanding: the drain answers immediately.
  EXPECT_EQ(tm_->WaitForAllIndexReservationsDrain(
                waiter.ID(), std::chrono::milliseconds(50)),
            Status::kSuccess);

  tm_->ReserveIndexUndoBytes(holder, kPage, 128);
  // A foreign reservation past the deadline is an honest give-up.
  EXPECT_EQ(tm_->WaitForAllIndexReservationsDrain(
                waiter.ID(), std::chrono::milliseconds(40)),
            Status::kConflicts);
  // The owner's own reservation never blocks its own drain.
  EXPECT_EQ(tm_->WaitForAllIndexReservationsDrain(
                holder.ID(), std::chrono::milliseconds(40)),
            Status::kSuccess);

  // A holder that resolves while the waiter waits wakes the drain via the
  // condition variable: success, not a timeout-flavored failure. Aborting
  // the holder runs ForgetTransaction, which drops its reservations.
  std::thread releaser([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    static_cast<void>(holder.Abort());
  });
  EXPECT_EQ(tm_->WaitForAllIndexReservationsDrain(waiter.ID(),
                                                  std::chrono::seconds(5)),
            Status::kSuccess);
  releaser.join();
  EXPECT_FALSE(waiter.IsFailed());

  static_cast<void>(waiter.PreCommit());
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
    // Registers a wait-for edge and waits. The holder's abort arms
    // IsAbortStarted, which lets the waiter give up instead of riding the
    // wait limit -- but a clean abort that releases the intent within one
    // poll tick still hands the row over, so BOTH outcomes are correct
    // here; only the latency is bounded. (The deterministic give-up path
    // has its own test in transaction_extra_test.)
    const auto start = std::chrono::steady_clock::now();
    acquired = t2.AddWriteSet(rp);
    EXPECT_LT(std::chrono::steady_clock::now() - start,
              std::chrono::seconds(2));
    if (!acquired.load()) {
      EXPECT_EQ(t2.LastIntentWaitFailure(),
                Transaction::IntentWaitFailure::kHolderAborting);
    }
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
  // Aborting |holder| ends the waiter's wait (via the abort flag, or the
  // intent release winning the race) while walking past the unrelated
  // bystander edge.
  ASSERT_EQ(tm_->Abort(holder), Status::kSuccess);
  waiter.join();
  // Either outcome is valid; see the waiter thread's contract above.
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
                         static_cast<slot_t>(static_cast<int>(policy)) + 1);
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

// A three-transaction cycle exercises hops through the wait-for walk that
// the two-node regression above never leaves: a -> b -> c -> a. Only the
// maximum-id participant (c) may be wounded, and the other two waits must
// complete once earlier participants unwind.
TEST_F(TransactionCoverageTest, DeadlockDetectorBreaksThreeTransactionCycle) {
  tm_->SetDeadlockPolicy(TransactionManager::DeadlockPolicy::kDeadlockDetect);
  const RowPosition row_a(88, 1);
  const RowPosition row_b(89, 1);
  const RowPosition row_c(90, 1);
  Transaction txn_a = tm_->Begin();
  Transaction txn_b = tm_->Begin();
  Transaction txn_c = tm_->Begin();
  ASSERT_TRUE(txn_a.AddWriteSet(row_a));
  ASSERT_TRUE(txn_b.AddWriteSet(row_b));
  ASSERT_TRUE(txn_c.AddWriteSet(row_c));

  std::atomic<bool> a_acquired{false};
  std::atomic<bool> b_acquired{false};
  std::atomic<bool> c_conflict{true};
  std::thread waiter_a([&] { a_acquired = txn_a.AddWriteSet(row_b); });
  std::thread waiter_b([&] { b_acquired = txn_b.AddWriteSet(row_c); });
  // The third edge closes the cycle; whichever thread runs last, the
  // detector wakes on its AddWaitForEdge notification and wounds the
  // maximum-id participant (c). The victim unwinds itself, exactly like an
  // executor would.
  std::thread waiter_c([&] {
    c_conflict = txn_c.AddWriteSet(row_a);
    if (!c_conflict.load()) {
      ASSERT_EQ(tm_->Abort(txn_c), Status::kSuccess);
    }
  });

  EXPECT_TRUE(WaitForCondition([&] { return txn_c.IsWounded(); },
                               std::chrono::seconds(5)));
  waiter_c.join();
  // c's abort released row_c, so b's strict wait completes without a wound.
  EXPECT_TRUE(WaitForCondition([&] { return b_acquired.load(); },
                               std::chrono::seconds(5)));
  waiter_b.join();
  // Unwinding b releases row_b, which is what a has been waiting for.
  ASSERT_EQ(tm_->Abort(txn_b), Status::kSuccess);
  waiter_a.join();
  EXPECT_TRUE(a_acquired.load());

  EXPECT_TRUE(txn_c.IsWounded());
  EXPECT_FALSE(txn_a.IsWounded());
  EXPECT_FALSE(txn_b.IsWounded());
  ASSERT_EQ(tm_->Abort(txn_a), Status::kSuccess);
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

  void TearDown() override {
    tm_.reset();
    rm_.reset();
    l_.reset();
    pm_.reset();
    std::ignore = std::remove(db_name_.c_str());
    tinylamb::RemoveLogAndSegments(log_name_);
  }

 protected:
  std::string db_name_;
  std::string log_name_;
  std::unique_ptr<PageManager> pm_;
  std::unique_ptr<Logger> l_;
  std::unique_ptr<RecoveryManager> rm_;
  std::unique_ptr<TransactionManager> tm_;
};

TEST_F(TransactionCoverageRecoveryTest,
       DestructorAbortsPhysicalWriteAndReleasesIntent) {
  page_id_t page_id = 0;
  {
    Transaction seed = tm_->Begin();
    PageRef page = pm_->AllocateNewPage(seed, PageType::kRowPage).MoveValue();
    page_id = page->PageID();
    ASSERT_SUCCESS(page->Insert(seed, "durable").GetStatus());
    page.PageUnlock();
    ASSERT_SUCCESS(seed.PreCommit());
  }
  {
    Transaction writer = tm_->Begin();
    PageRef page = pm_->GetPage(page_id).MoveValue();
    ASSERT_SUCCESS(page->Update(writer, 0, "uncommitted"));
    page.PageUnlock();
  }
  {
    PageRef page = pm_->GetPage(page_id).MoveValue();
    EXPECT_EQ(page->body.row_page.GetRow(0), "durable");
  }
  {
    Transaction updater = tm_->Begin();
    PageRef page = pm_->GetPage(page_id).MoveValue();
    ASSERT_SUCCESS(page->Update(updater, 0, "replacement"));
    page.PageUnlock();
    ASSERT_SUCCESS(updater.PreCommit());
  }
  PageRef page = pm_->GetPage(page_id).MoveValue();
  EXPECT_EQ(page->body.row_page.GetRow(0), "replacement");
}

TEST_F(TransactionCoverageRecoveryTest,
       MoveAssignmentAbortsReplacedActiveTransaction) {
  page_id_t page_id = 0;
  {
    Transaction seed = tm_->Begin();
    PageRef page = pm_->AllocateNewPage(seed, PageType::kRowPage).MoveValue();
    page_id = page->PageID();
    ASSERT_SUCCESS(page->Insert(seed, "row-zero").GetStatus());
    ASSERT_SUCCESS(page->Insert(seed, "row-one").GetStatus());
    page.PageUnlock();
    ASSERT_SUCCESS(seed.PreCommit());
  }

  Transaction target = tm_->Begin();
  const txn_id_t target_id = target.ID();
  {
    PageRef page = pm_->GetPage(page_id).MoveValue();
    ASSERT_SUCCESS(page->Update(target, 0, "target"));
  }
  Transaction source = tm_->Begin();
  const txn_id_t source_id = source.ID();
  {
    PageRef page = pm_->GetPage(page_id).MoveValue();
    ASSERT_SUCCESS(page->Update(source, 1, "source"));
  }

  target = std::move(source);
  EXPECT_FALSE(tm_->IsActive(target_id));
  EXPECT_TRUE(tm_->IsActive(source_id));
  {
    PageRef page = pm_->GetPage(page_id).MoveValue();
    EXPECT_EQ(page->body.row_page.GetRow(0), "row-zero");
    EXPECT_EQ(page->body.row_page.GetRow(1), "source");
  }
  {
    Transaction updater = tm_->Begin();
    PageRef page = pm_->GetPage(page_id).MoveValue();
    ASSERT_SUCCESS(page->Update(updater, 0, "replacement"));
    page.PageUnlock();
    ASSERT_SUCCESS(updater.PreCommit());
  }
  ASSERT_SUCCESS(target.PreCommit());
  PageRef page = pm_->GetPage(page_id).MoveValue();
  EXPECT_EQ(page->body.row_page.GetRow(0), "replacement");
  EXPECT_EQ(page->body.row_page.GetRow(1), "source");
}

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
  // records can never reach the file.
  const std::string slow_log =
      "transaction_coverage_slow2-" + RandomString() + ".log";
  const std::string slow_db =
      "transaction_coverage_slow2-" + RandomString() + ".db";
  auto slow_pm = PageManager::Create(slow_db, 10).MoveValue();
  // A 30 s interval keeps the first flush attempt (and the ENOSPC surface)
  // out of the test's wall time even under a fully loaded ctest run -- the
  // phase-1 abort below must see the WAL as still-healthy by construction,
  // not by scheduling luck.
  auto slow_logger =
      Logger::Create(slow_log, size_t{1024} * 1024 * 8, /*every_ms=*/30'000)
          .MoveValue();
  {
    RecoveryManager slow_rm(slow_log, slow_pm->GetPool());
    // The recovery manager reads the live logger's ring buffer, so undo
    // walks see their own records while they are still volatile.
    slow_rm.SetLogger(slow_logger.get());
    TransactionManager slow_tm(slow_pm.get(), slow_logger.get(), &slow_rm);

    // A second transaction whose chain is still pending when the WAL dies:
    // its abort must report the dead logger (terminator append fails) once
    // the failure has surfaced.
    Transaction pending = slow_tm.Begin();
    ASSERT_TRUE(pending
                    .AppendLog(LogRecord(pending.PrevLSN(), pending.ID(),
                                         LogType::kBegin))
                    .HasValue());
    ASSERT_TRUE(pending
                    .AppendLog(LogRecord(pending.PrevLSN(), pending.ID(),
                                         LogType::kBegin))
                    .HasValue());
    ASSERT_GT(pending.PrevLSN(), 0U);

    // The doomed transaction appends nothing: its abort rides the sentinel
    // record only.
    Transaction doomed = slow_tm.Begin();
    ASSERT_TRUE(doomed
                    .AppendLog(LogRecord(doomed.PrevLSN(), doomed.ID(),
                                         LogType::kBegin))
                    .HasValue());
    ASSERT_TRUE(doomed
                    .AppendLog(LogRecord(doomed.PrevLSN(), doomed.ID(),
                                         LogType::kBegin))
                    .HasValue());

    PointWalAtDevFull(slow_logger.get());
    // Scheduling decides whether the worker's first write attempt surfaced
    // the dead fd before the abort: if not, the ring-resident chain is read
    // from the ring, no CLR is owed (kBegin is not a page manipulation), and
    // the terminator append is accepted into the buffer (kSuccess -- the
    // abort pays no group-commit flush; the former pre-undo WaitForDurable
    // surfaced the doomed fd through every abort, an fsync amplification the
    // ring-read undo removed). If the worker surfaced it first, the
    // terminator append hits the latched dead logger (kIOError). Either way
    // the rollback is unaffected -- losing the volatile terminator to a
    // crash replays the identical rollback as restart loser cleanup -- and
    // the invariant under test is that the abort finishes cleanly.
    const Status doomed_abort = doomed.Abort();
    EXPECT_TRUE(doomed_abort == Status::kSuccess ||
                doomed_abort == Status::kIOError)
        << doomed_abort;
    EXPECT_TRUE(doomed.IsFinished());

    // The next durability barrier surfaces the dead fd and latches
    // Logger::Failed -- the dead WAL is the durability layer's verdict.
    EXPECT_NE(slow_logger->WaitForDurable(slow_logger->BufferedLSN()),
              Status::kSuccess);
    EXPECT_TRUE(slow_logger->Failed());

    // From here an abort whose records are still pending fails
    // deterministically (the terminator append hits the dead logger) but
    // still finishes cleanly.
    EXPECT_NE(pending.Abort(), Status::kSuccess);
    EXPECT_TRUE(pending.IsFinished());
  }
  std::ignore = std::remove(slow_log.c_str());
  std::ignore = std::remove(slow_db.c_str());
}

TEST_F(TransactionCoverageRecoveryTest, RealStorageCommitFailureStopsNewWork) {
  const RowPosition rp(900, 1);
  ForceWalFailure(l_.get());
  Transaction writer = tm_->Begin();
  ASSERT_TRUE(writer.AddWriteSet(rp));
  writer.RegisterVersionWrite(rp, std::nullopt, "uncommittable");
  EXPECT_NE(writer.PreCommit(), Status::kSuccess);
  EXPECT_TRUE(tm_->FailStopped());

  Transaction blocked = tm_->Begin();
  EXPECT_FALSE(blocked.AddWriteSet(rp));
  EXPECT_EQ(blocked.ReadVersion(rp, std::nullopt).GetStatus(),
            Status::kIOError);
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

}  // namespace tinylamb
