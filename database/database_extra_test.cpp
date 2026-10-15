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
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
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
  const std::string segment_prefix = prefix + ".log.";
  for (const std::filesystem::directory_entry& entry : iterator) {
    const std::string filename = entry.path().filename().string();
    if (filename.starts_with(segment_prefix)) {
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
  ASSERT_GT(acknowledged.load(std::memory_order_relaxed), 0);

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
    ctx.PreCommit();
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
  const auto reopened = Database::Create(prefix_);
  EXPECT_EQ(reopened.GetStatus(), Status::kCorrupt);
  std::filesystem::remove(prefix_ + ".db", ec);
  std::filesystem::remove(prefix_ + ".log", ec);
  std::filesystem::remove(prefix_ + ".log.meta", ec);
  std::filesystem::remove(prefix_ + ".last_checkpoint", ec);
}
}  // namespace tinylamb
