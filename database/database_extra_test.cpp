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

// End-to-end durability guarantees of the transaction engine under
// concurrent commits and interleaved checkpoints, verified across a
// simulated crash.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ranges>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "query/sql_engine.hpp"
#include "recovery/logger.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {
// Runs one statement and drains the result; returns nullopt on failure.
bool RunStatement(SqlEngine* engine, TransactionContext* ctx,
                  const std::string& sql, std::vector<Row>* rows = nullptr) {
  StatusOr<QueryResult> result = engine->Execute(*ctx, sql);
  if (!result.HasValue()) {
    return false;
  }
  QueryResult drained = result.MoveValue();
  if (rows != nullptr) {
    Row row;
    while (drained.Next(&row)) {
      rows->push_back(row);
      row = Row();
    }
  } else {
    drained.Drain();
  }
  return drained.GetStatus() == Status::kSuccess;
}

std::map<std::string, std::vector<char>> DatabaseArtifacts(
    const std::string& prefix) {
  std::map<std::string, std::vector<char>> artifacts;
  const auto capture = [&](const std::filesystem::path& path) {
    std::error_code error;
    if (!std::filesystem::is_regular_file(path, error)) {
      return;
    }
    std::ifstream input(path, std::ios::binary);
    artifacts.emplace(path.string(),
                      std::vector<char>(std::istreambuf_iterator<char>(input),
                                        std::istreambuf_iterator<char>()));
  };
  capture(prefix + ".db");
  capture(prefix + ".db.blocks");
  capture(prefix + ".db.lsm");
  capture(prefix + ".log");
  capture(prefix + ".log.meta");
  capture(prefix + ".log.meta.tmp");
  capture(prefix + ".last_checkpoint");
  capture(prefix + ".last_checkpoint.tmp");

  std::error_code error;
  std::filesystem::directory_iterator iterator(".", error);
  if (error) {
    return artifacts;
  }
  // DeleteAll()/RemoveCreatedDatabaseFiles() also remove the companion
  // block, LSM and TLCS families; the verification set must cover them or
  // a leak there would go unnoticed.
  const std::vector<std::string> family_prefixes = {
      prefix + ".log.",    prefix + ".db.blocks.", prefix + ".db.lsm.",
      prefix + ".db.tlcs", prefix + ".blocks.",    prefix + ".lsm.",
      prefix + ".tlcs",
  };
  for (const std::filesystem::directory_entry& entry : iterator) {
    const std::string filename = entry.path().filename().string();
    if (std::ranges::any_of(family_prefixes, [&](const std::string& p) {
          return filename.starts_with(p);
        })) {
      capture(entry.path());
    }
  }
  return artifacts;
}
}  // namespace

class DatabaseExtraTest : public ::testing::Test {
 protected:
  void SetUp() override {
    prefix_ = "database_extra_test-" + RandomString();
    db_ = Database::Create(prefix_).MoveValue();
  }

  void TearDown() override {
    if (db_ != nullptr) {
      db_->DeleteAll();
      db_.reset();
    }
  }

  std::string prefix_;
  std::unique_ptr<Database> db_;
};

TEST_F(DatabaseExtraTest, DurableWalFrontierWaitsForSuccessfulSync) {
  SqlEngine engine(*db_);
  auto setup = db_->BeginContext();
  ASSERT_TRUE(
      RunStatement(&engine, &setup, "CREATE TABLE durable_frontier (k INT64)"));
  ASSERT_SUCCESS(setup.PreCommit());
  const auto previous = db_->DurableWalLSN();
  auto writer = db_->BeginContext();
  ASSERT_TRUE(RunStatement(&engine, &writer,
                           "INSERT INTO durable_frontier VALUES (1)"));
  struct Gate {
    std::mutex mutex;
    std::condition_variable cv;
    bool entered{false};
    bool released{false};
  };
  auto gate = std::make_shared<Gate>();
  struct ResetHook {  // NOLINT(cppcoreguidelines-special-member-functions)
    std::shared_ptr<Gate> gate;
    ~ResetHook() {
      Logger::SetBeforeSyncHookForTest({});
      std::scoped_lock lock(gate->mutex);
      gate->released = true;
      gate->cv.notify_all();
    }
  } reset{gate};
  Logger::SetBeforeSyncHookForTest([gate](int) {
    std::unique_lock lock(gate->mutex);
    gate->entered = true;
    gate->cv.notify_all();
    gate->cv.wait(lock, [&] { return gate->released; });
    return 0;
  });
  ASSIGN_OR_ASSERT_FAIL(lsn_t, target, writer.PrepareCommitForDurableReply());
  {
    std::unique_lock lock(gate->mutex);
    ASSERT_TRUE(gate->cv.wait_for(lock, std::chrono::seconds(5),
                                  [&] { return gate->entered; }));
    EXPECT_GE(db_->DurableWalLSN(), previous);
    EXPECT_LT(db_->DurableWalLSN(), target);
    gate->released = true;
    gate->cv.notify_all();
  }
  ASSERT_SUCCESS(db_->WaitForCommitDurable(target));
  EXPECT_GE(db_->DurableWalLSN(), target);
}

TEST_F(DatabaseExtraTest, SecondLiveDatabaseOpenFailsWithoutChangingFiles) {
  SqlEngine engine(*db_);
  {
    TransactionContext ctx = db_->BeginContext();
    ASSERT_TRUE(
        RunStatement(&engine, &ctx, "CREATE TABLE single_writer (k INT64);"));
    ASSERT_TRUE(
        RunStatement(&engine, &ctx, "INSERT INTO single_writer VALUES (1);"));
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  ASSERT_SUCCESS(db_->WriteCheckpoint().GetStatus());
  const auto before = DatabaseArtifacts(prefix_);
  ASSERT_FALSE(before.empty());

  const auto second = Database::Create(prefix_);
  ASSERT_FALSE(second.HasValue());
  EXPECT_EQ(second.GetStatus(), Status::kConflicts);
  EXPECT_NE(second.GetStatus().GetMessage().find("already open"),
            std::string::npos);
  EXPECT_EQ(DatabaseArtifacts(prefix_), before);

  TransactionContext ctx = db_->BeginContext();
  ASSERT_TRUE(
      RunStatement(&engine, &ctx, "INSERT INTO single_writer VALUES (2);"));
  ASSERT_SUCCESS(ctx.PreCommit());
}

TEST_F(DatabaseExtraTest, DeleteAllClosesFilesAndLeavesTerminalDatabase) {
  SqlEngine engine(*db_);
  TransactionContext ctx = db_->BeginContext();
  ASSERT_TRUE(
      RunStatement(&engine, &ctx, "CREATE TABLE delete_all (k INT64);"));
  ASSERT_TRUE(
      RunStatement(&engine, &ctx, "INSERT INTO delete_all VALUES (1);"));
  ASSERT_SUCCESS(ctx.PreCommit());
  ASSERT_SUCCESS(db_->WriteCheckpoint().GetStatus());
  {
    std::ofstream segment(prefix_ + ".log.99999999999999999999",
                          std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(segment.good());
    segment << "segment";
  }
  ASSERT_FALSE(DatabaseArtifacts(prefix_).empty());

  db_->DeleteAll();
  EXPECT_TRUE(DatabaseArtifacts(prefix_).empty());

  auto replacement = Database::Create(prefix_);
  ASSERT_TRUE(replacement.HasValue()) << replacement.GetStatus();
  replacement.Value()->DeleteAll();
  EXPECT_TRUE(DatabaseArtifacts(prefix_).empty());
}

// Regression guard for the checkpoint ATT-classification race: a commit
// record appended just below a concurrent checkpoint's begin LSN made the
// active-transaction snapshot record the transaction as still running, and
// restart recovery then classified the DURABLY COMMITTED transaction as a
// loser and rolled it back. Writer threads commit continuously while the
// main thread checkpoints; every commit that reported success must survive
// a crash + reopen.
TEST_F(DatabaseExtraTest, CommittedRowsSurviveCrashDuringCheckpointStorm) {
  {
    SqlEngine engine(*db_);
    TransactionContext ctx = db_->BeginContext();
    ASSERT_TRUE(RunStatement(&engine, &ctx,
                             "CREATE TABLE storm (k INT64 NOT NULL, v INT64,"
                             " PRIMARY KEY (k))"))
        << engine.LastError();
    ASSERT_EQ(Status::kSuccess, ctx.PreCommit());
  }
  constexpr int kRows = 64;
  constexpr int kWriters = 4;
  static_assert(kRows % kWriters == 0);
  constexpr int kKeysPerWriter = kRows / kWriters;
  {
    SqlEngine engine(*db_);
    for (int i = 0; i < kRows; ++i) {
      TransactionContext ctx = db_->BeginContext();
      ASSERT_TRUE(RunStatement(
          &engine, &ctx,
          "INSERT INTO storm (k, v) VALUES (" + std::to_string(i) + ", 0)"));
      ASSERT_EQ(Status::kSuccess, ctx.PreCommit());
    }
  }

  std::mutex results_mu;
  std::map<int, int> committed_values;
  std::atomic<bool> stop{false};
  std::atomic<int> acknowledged{0};
  std::vector<std::thread> writers;
  writers.reserve(kWriters);
  for (int w = 0; w < kWriters; ++w) {
    writers.emplace_back([&, w] {
      SqlEngine engine(*db_);
      for (int round = 0; !stop.load(std::memory_order_relaxed) && round < 400;
           ++round) {
        const int key = (w * kKeysPerWriter) + (round % kKeysPerWriter);
        const int value = (w * 100000) + round;
        TransactionContext ctx = db_->BeginContext();
        const std::string sql =
            "UPDATE storm SET v = " + std::to_string(value) +
            " WHERE k = " + std::to_string(key);
        if (!RunStatement(&engine, &ctx, sql)) {
          continue;
        }
        if (ctx.PreCommit() != Status::kSuccess) {
          continue;
        }
        // The commit is acknowledged: after any later crash this value must
        // be what a fresh database serves for this key.
        std::scoped_lock lock(results_mu);
        committed_values[key] = value;
        acknowledged.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }
  // Interleave checkpoints with the commit stream: each one snapshots the
  // active-transaction table while commits are in flight.
  SqlEngine engine(*db_);
  for (int i = 0; i < 200 && acknowledged.load(std::memory_order_relaxed) < 50;
       ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    ASSERT_SUCCESS(db_->WriteCheckpoint().GetStatus());
  }
  stop.store(true, std::memory_order_relaxed);
  for (std::thread& w : writers) {
    w.join();
  }
  // The checkpoint storm must actually overlap the target number of commits;
  // otherwise the ATT classification race this test targets was never stressed.
  ASSERT_GE(acknowledged.load(std::memory_order_relaxed), 50);

  // Simulated crash + reopen: every acknowledged commit must be visible,
  // with exactly the acknowledged value.
  db_->EmulateCrash();
  db_.reset();
  db_ = Database::Create(prefix_).MoveValue();
  SqlEngine reopened(*db_);
  for (const auto& [key, value] : committed_values) {
    TransactionContext ctx = db_->BeginContext();
    std::vector<Row> rows;
    ASSERT_TRUE(RunStatement(
        &reopened, &ctx, "SELECT v FROM storm WHERE k = " + std::to_string(key),
        &rows));
    ASSERT_EQ(rows.size(), 1U) << "key " << key;
    ASSERT_EQ(rows[0][0], Value(static_cast<int64_t>(value)))
        << "key " << key << " lost or rolled back its committed value";
    ASSERT_SUCCESS(ctx.PreCommit());
  }
}

TEST_F(DatabaseExtraTest, MissingWalFailsClosed) {
  SqlEngine engine(*db_);
  TransactionContext ctx = db_->BeginContext();
  ASSERT_TRUE(
      RunStatement(&engine, &ctx, "CREATE TABLE wal_missing (k INT64);"))
      << engine.LastError();
  ASSERT_EQ(ctx.PreCommit(), Status::kSuccess);
  ASSERT_SUCCESS(db_->WriteCheckpoint().GetStatus());
  db_->EmulateCrash();
  db_.reset();

  std::error_code ec;
  RemoveLogAndSegments(prefix_ + ".log");
  std::filesystem::remove(prefix_ + ".last_checkpoint", ec);
  ASSERT_FALSE(ec) << ec.message();
  const auto reopened = Database::Create(prefix_);
  EXPECT_EQ(reopened.GetStatus(), Status::kCorrupt);
  std::filesystem::remove(prefix_ + ".db", ec);
  ASSERT_FALSE(ec) << ec.message();
  std::filesystem::remove(prefix_ + ".log", ec);
  ASSERT_FALSE(ec) << ec.message();
  std::filesystem::remove(prefix_ + ".log.meta", ec);
  ASSERT_FALSE(ec) << ec.message();
  std::filesystem::remove(prefix_ + ".last_checkpoint", ec);
  ASSERT_FALSE(ec) << ec.message();
}
TEST_F(DatabaseExtraTest, CleanReopenSkipsNoOpEndOfRecoveryCheckpoint) {
  SqlEngine engine(*db_);
  TransactionContext ctx = db_->BeginContext();
  ASSERT_TRUE(RunStatement(&engine, &ctx, "CREATE TABLE eor_skip (k INT64)"));
  ASSERT_TRUE(RunStatement(&engine, &ctx, "INSERT INTO eor_skip VALUES (1)"));
  ASSERT_SUCCESS(ctx.PreCommit());

  // First reopen replays the WAL and writes the EOR checkpoint snapshot.
  db_->EmulateCrash();
  db_.reset();
  db_ = Database::Create(prefix_).MoveValue();
  const auto db_path = prefix_ + ".db";
  ASSERT_TRUE(std::filesystem::exists(db_path));
  const auto before = std::filesystem::last_write_time(db_path);

  // Second reopen has nothing new to replay: the snapshot must not be
  // serialized/fsynced/renamed again.
  db_->EmulateCrash();
  db_.reset();
  db_ = Database::Create(prefix_).MoveValue();
  const auto after = std::filesystem::last_write_time(db_path);
  EXPECT_EQ(after, before);

  SqlEngine reopened(*db_);
  TransactionContext verify = db_->BeginContext();
  std::vector<Row> rows;
  ASSERT_TRUE(
      RunStatement(&reopened, &verify, "SELECT k FROM eor_skip", &rows));
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0][0], Value(int64_t{1}));
  ASSERT_SUCCESS(verify.PreCommit());
}

// #1100: the compiled-plan cache probes this instead of a catalog
// GetTable + full deserialization per retained table per hit.  The
// binding must be stable across repeated lookups and must break exactly
// when the identity changes (drop, recreate, unknown name).
TEST_F(DatabaseExtraTest, IsCurrentIncarnationTracksDdlLifecycle) {
  TransactionContext setup = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, created,
      db_->CreateTable(setup,
                       Schema("probe_t", {Column("v", ValueType::kInt64)})));
  ASSERT_SUCCESS(setup.PreCommit());

  TransactionContext reader = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, first, db_->GetTable(reader, "probe_t"));
  const std::shared_ptr<TableIncarnation>& state = first.IncarnationState();
  ASSERT_NE(state, nullptr);
  EXPECT_TRUE(db_->IsCurrentIncarnation("probe_t", state));
  // Repeated lookups bind the same identity object: the plan-cache probe is
  // answered by pointer equality, no churn across replays.
  ASSIGN_OR_ASSERT_FAIL(Table, second, db_->GetTable(reader, "probe_t"));
  EXPECT_EQ(second.IncarnationState(), state);
  EXPECT_TRUE(db_->IsCurrentIncarnation("probe_t", second.IncarnationState()));
  EXPECT_FALSE(db_->IsCurrentIncarnation("no_such_table", state));
  EXPECT_FALSE(db_->IsCurrentIncarnation("probe_t", nullptr));
  ASSERT_SUCCESS(reader.PreCommit());

  TransactionContext dropper = db_->BeginContext();
  ASSERT_SUCCESS(db_->DropTable(dropper, "probe_t"));
  ASSERT_SUCCESS(dropper.PreCommit());
  EXPECT_FALSE(db_->IsCurrentIncarnation("probe_t", state));

  TransactionContext rebuilder = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, fresh,
      db_->CreateTable(rebuilder,
                       Schema("probe_t", {Column("w", ValueType::kInt64)})));
  ASSERT_SUCCESS(rebuilder.PreCommit());
  EXPECT_FALSE(db_->IsCurrentIncarnation("probe_t", state));
  TransactionContext again = db_->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(Table, t3, db_->GetTable(again, "probe_t"));
  EXPECT_TRUE(db_->IsCurrentIncarnation("probe_t", t3.IncarnationState()));
  ASSERT_SUCCESS(again.PreCommit());
}

TEST_F(DatabaseExtraTest,
       AnalyzeAutoCompactsCommittedTablesAndSkipsUncommittedTxn) {
  struct AnalyzeTlcsEnvGuard {
    std::optional<std::string> prev;
    explicit AnalyzeTlcsEnvGuard(const char* value) {
      if (const char* cur = std::getenv("TINYLAMB_ANALYZE_AUTO_TLCS")) {
        prev = cur;
      }
      if (value != nullptr) {
        setenv("TINYLAMB_ANALYZE_AUTO_TLCS", value, 1);
      } else {
        unsetenv("TINYLAMB_ANALYZE_AUTO_TLCS");
      }
    }
    AnalyzeTlcsEnvGuard(const AnalyzeTlcsEnvGuard&) = delete;
    AnalyzeTlcsEnvGuard& operator=(const AnalyzeTlcsEnvGuard&) = delete;
    AnalyzeTlcsEnvGuard(AnalyzeTlcsEnvGuard&&) = delete;
    AnalyzeTlcsEnvGuard& operator=(AnalyzeTlcsEnvGuard&&) = delete;
    ~AnalyzeTlcsEnvGuard() {
      if (prev.has_value()) {
        setenv("TINYLAMB_ANALYZE_AUTO_TLCS", prev->c_str(), 1);
      } else {
        unsetenv("TINYLAMB_ANALYZE_AUTO_TLCS");
      }
    }
  };

  SqlEngine engine(*db_);
  {
    TransactionContext ctx = db_->BeginContext();
    ASSERT_TRUE(
        RunStatement(&engine, &ctx, "CREATE TABLE analyze_auto (k INT64);"));
    ASSERT_TRUE(RunStatement(&engine, &ctx,
                             "INSERT INTO analyze_auto VALUES (10), (20);"));
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  {
    AnalyzeTlcsEnvGuard off_guard(nullptr);
    {
      TransactionContext ctx = db_->BeginContext();
      ASSERT_TRUE(RunStatement(&engine, &ctx, "ANALYZE analyze_auto;"));
      ASSERT_SUCCESS(ctx.PreCommit());
    }
    {
      TransactionContext check = db_->BeginReadOnlyContext();
      ASSIGN_OR_ASSERT_FAIL(Table, tbl, db_->GetTable(check, "analyze_auto"));
      EXPECT_EQ(tbl.GetBackingTlcsStore(), nullptr);
      ASSERT_SUCCESS(check.PreCommit());
    }
  }

  AnalyzeTlcsEnvGuard on_guard("1");
  {
    TransactionContext ctx = db_->BeginContext();
    ASSERT_TRUE(RunStatement(&engine, &ctx, "ANALYZE analyze_auto;"));
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  {
    TransactionContext check = db_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db_->GetTable(check, "analyze_auto"));
    EXPECT_NE(tbl.GetBackingTlcsStore(), nullptr);
    EXPECT_TRUE(std::filesystem::exists(prefix_ + ".db.tlcs." +
                                        std::to_string(tbl.FirstPageId())));
    ASSERT_SUCCESS(check.PreCommit());
  }

  // Open transaction with uncommitted INSERT: ANALYZE must not bake dirty
  // rows into a backing TLCS file, and Abort() must roll back cleanly.
  {
    TransactionContext ctx = db_->BeginContext();
    ASSERT_TRUE(
        RunStatement(&engine, &ctx, "CREATE TABLE analyze_dirty (k INT64);"));
    ASSERT_SUCCESS(ctx.PreCommit());
  }
  {
    TransactionContext dirty = db_->BeginContext();
    ASSERT_TRUE(RunStatement(&engine, &dirty,
                             "INSERT INTO analyze_dirty VALUES (99);"));
    ASSERT_TRUE(RunStatement(&engine, &dirty, "ANALYZE analyze_dirty;"));
    ASSERT_SUCCESS(dirty.Abort());
  }
  {
    TransactionContext verify = db_->BeginReadOnlyContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, db_->GetTable(verify, "analyze_dirty"));
    EXPECT_EQ(tbl.GetBackingTlcsStore(), nullptr);
    std::vector<Row> rows;
    ASSERT_TRUE(RunStatement(&engine, &verify,
                             "SELECT COUNT(*) FROM analyze_dirty;", &rows));
    ASSERT_EQ(rows.size(), 1U);
    EXPECT_EQ(rows[0][0], Value(int64_t{0}));
    ASSERT_SUCCESS(verify.PreCommit());
  }
}

}  // namespace tinylamb
