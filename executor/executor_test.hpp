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

#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <initializer_list>
#include <iostream>
#include <limits>
#include <memory>
#include <new>
#include <numeric>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/join_kind.hpp"
#include "common/log_message.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/aggregation.hpp"
#include "executor/bitmap_scan.hpp"
#include "executor/constant_executor.hpp"
#include "executor/cross_join.hpp"
#include "executor/data_chunk.hpp"
#include "executor/delete.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/distinct.hpp"
#include "executor/executor_base.hpp"
#include "executor/full_scan.hpp"
#include "executor/hash_join.hpp"
#include "executor/hash_join_mode.hpp"
#include "executor/index_only_scan.hpp"
#include "executor/index_scan.hpp"
#include "executor/insert.hpp"
#include "executor/limit.hpp"
#include "executor/lookup_join.hpp"
#include "executor/parallel_aggregation.hpp"
#include "executor/parallel_scan.hpp"
#include "executor/projection.hpp"
#include "executor/query_memory.hpp"
#include "executor/relational.hpp"
#include "executor/selection.hpp"
#include "executor/sort.hpp"
#include "executor/topn.hpp"
#include "executor/update.hpp"
#include "executor/zone_map.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "gtest/gtest.h"
#include "index/index_schema.hpp"
#include "plan/full_scan_plan.hpp"
#include "plan/merge_join_plan.hpp"
#include "plan/product_plan.hpp"
#include "plan/selection_plan.hpp"
#include "plan/values_plan.hpp"
#include "query/query_data.hpp"
#include "query/sql_engine.hpp"
#include "table/lstable_bridge.hpp"
#include "transaction/transaction.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

inline constexpr const char* const kTableName = "SampleTable";

class SyntheticBatchExecutor final : public ExecutorBase {
 public:
  explicit SyntheticBatchExecutor(size_t row_count) : row_count_(row_count) {}

  size_t NextBatch(DataChunk* destination, size_t max_rows) override {
    const Schema schema("synthetic", {Column("value", ValueType::kInt64)});
    destination->Reset(schema, max_rows);
    const size_t batch_rows = std::min<size_t>(max_rows, 64);
    while (next_row_ < row_count_ && destination->Size() < batch_rows) {
      destination->Append(Row({Value(static_cast<int64_t>(next_row_ % 100))}),
                          RowPosition(1, static_cast<slot_t>(next_row_)));
      ++next_row_;
    }
    return destination->Size();
  }

  void Dump(std::ostream& out, int /*indent*/) const override {
    out << "SyntheticBatchExecutor";
  }

 private:
  size_t row_count_;
  size_t next_row_{0};
};

class ExecutorTest : public ::testing::Test {
 public:
  static void BulkInsert(Transaction& txn, Table& tbl,
                         std::initializer_list<Row> rows) {
    for (const auto& row : rows) {
      ASSERT_SUCCESS(tbl.Insert(txn, row).GetStatus());
    }
  }

  void SetUp() override {
    prefix_ = "executor_test-" + RandomString();
    Recover();
    Schema schema{
        kTableName,
        {Column("key", ValueType::kInt64), Column("name", ValueType::kVarChar),
         Column("score", ValueType::kDouble)}};
    TransactionContext ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(Table, tbl, rs_->CreateTable(ctx, schema));
    BulkInsert(ctx.txn_, tbl,
               {{Row({Value(0), Value("hello"), Value(1.2)})},
                {Row({Value(3), Value("piyo"), Value(12.2)})},
                {Row({Value(1), Value("world"), Value(4.9)})},
                {Row({Value(2), Value("arise"), Value(4.14)})}});
    ASSERT_SUCCESS(
        rs_->CreateIndex(ctx, kTableName, IndexSchema("Idx1", {1, 2})));
    ASSERT_SUCCESS(rs_->CreateIndex(
        ctx, kTableName,
        IndexSchema("Idx2", {1}, {1, 2}, IndexMode::kNonUnique)));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  void Recover() {
    if (rs_) {
      rs_->EmulateCrash();
      rs_.reset();
    }
    rs_ = Database::Create(prefix_).MoveValue();
  }

  void WaitForChainFreePages() const {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (rs_->TransactionStats().version_chains != 0) {
      ASSERT_LT(std::chrono::steady_clock::now(), deadline)
          << "background GC did not drop version chains";
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }

  void TearDown() override { rs_->DeleteAll(); }

  std::string prefix_;
  std::unique_ptr<Database> rs_;
};

inline const LookupSeek& SeekOf(const std::optional<LookupSeek>& probe) {
  assert(probe.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  return *probe;
}

inline Executor RangeProbeInput(std::vector<Row> rows) {
  return std::make_shared<ConstantExecutor>(std::move(rows));
}

inline std::vector<Row> Drain(ExecutorBase& join) {
  std::vector<Row> rows;
  Row row;
  while (join.Next(&row, nullptr)) {
    rows.push_back(row);
  }
  EXPECT_EQ(join.GetStatus(), Status::kSuccess);
  return rows;
}

class FailingBatchExecutor final : public ExecutorBase {
 public:
  explicit FailingBatchExecutor(size_t good_batches)
      : remaining_(good_batches) {}

  size_t NextBatch(DataChunk* destination, size_t max_rows) override {
    destination->Reset();
    for (size_t i = 0; i < max_rows && !failed_; ++i) {
      if (remaining_ == 0) {
        break;
      }
      if (remaining_ == 1) {
        failed_ = true;
        throw std::runtime_error("synthetic aggregation failure");
      }
      destination->Append(Row({Value(static_cast<int64_t>(emitted_))}),
                          RowPosition(1, static_cast<slot_t>(emitted_)));
      ++emitted_;
      --remaining_;
    }
    return destination->Size();
  }

  void Dump(std::ostream& out, int /*indent*/) const override {
    out << "FailingBatchExecutor";
  }

 private:
  size_t remaining_;
  size_t emitted_{0};
  bool failed_{false};
};

inline constexpr int64_t kParallelJoinBigLeft = 8000;
inline constexpr int64_t kParallelJoinBigRight = 200;

inline std::vector<Row> ShuffledJoinLeft() {
  std::vector<Row> rows;
  rows.reserve(static_cast<size_t>(kParallelJoinBigLeft));
  for (int64_t i = 0; i < kParallelJoinBigLeft; ++i) {
    rows.emplace_back(Row({Value(i % 100), Value(i)}));
  }
  return rows;
}

inline std::vector<Row> SortedJoinLeft() {
  std::vector<Row> rows;
  rows.reserve(static_cast<size_t>(kParallelJoinBigLeft));
  for (int64_t i = 0; i < kParallelJoinBigLeft; ++i) {
    rows.emplace_back(Row({Value(i / 80), Value(i)}));
  }
  return rows;
}

inline std::vector<Row> SortedJoinRight() {
  std::vector<Row> rows;
  rows.reserve(static_cast<size_t>(kParallelJoinBigRight));
  for (int64_t i = 0; i < kParallelJoinBigRight; ++i) {
    rows.emplace_back(Row({Value(i / 2), Value(i)}));
  }
  return rows;
}

inline const Schema& JoinLeftSchema() {
  static const Schema schema(
      "l", {Column("k", ValueType::kInt64), Column("v", ValueType::kInt64)});
  return schema;
}

inline const Schema& JoinRightSchema() {
  static const Schema schema(
      "r", {Column("k", ValueType::kInt64), Column("w", ValueType::kInt64)});
  return schema;
}

inline std::vector<Row> RelationalRun(Database& database,
                                      std::string_view sql) {
  TransactionContext context = database.BeginContext();
  SqlEngine engine(database);
  StatusOr<Executor> prepared = engine.Prepare(context, sql);
  std::vector<Row> rows;
  if (!prepared.HasValue()) {
    ADD_FAILURE() << sql << "\n" << engine.LastError();
    EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
    return rows;
  }
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    rows.push_back(row);
  }
  EXPECT_EQ(prepared.Value()->GetStatus(), Status::kSuccess) << sql;
  EXPECT_EQ(context.PreCommit(), Status::kSuccess);
  return rows;
}

inline std::string RelationalThrow(Database& database, std::string_view sql) {
  TransactionContext context = database.BeginContext();
  SqlEngine engine(database);
  StatusOr<Executor> prepared = engine.Prepare(context, sql);
  if (!prepared.HasValue()) {
    EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
    return "<prepare: " + engine.LastError() + ">";
  }
  try {
    Row row;
    while (prepared.Value()->Next(&row, nullptr)) {
    }
    const Status st = prepared.Value()->GetStatus();
    if (st != Status::kSuccess) {
      EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
      return st.GetMessage();
    }
  } catch (const std::exception& error) {
    EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
    return error.what();
  }
  EXPECT_EQ(tinylamb::Status::kSuccess, context.Abort());
  return "";
}

inline void ExpectMessageContains(const std::string& message,
                                  const std::string& substring) {
  EXPECT_NE(message.find(substring), std::string::npos) << message;
}

class ScopedQueryMemory {
 public:
  explicit ScopedQueryMemory(size_t limit)
      : original_(QueryMemoryBudget::Global().Limit()) {
    QueryMemoryBudget::Global().ResetForTest(limit);
  }
  ~ScopedQueryMemory() { QueryMemoryBudget::Global().ResetForTest(original_); }
  ScopedQueryMemory(const ScopedQueryMemory&) = delete;
  ScopedQueryMemory& operator=(const ScopedQueryMemory&) = delete;
  ScopedQueryMemory(ScopedQueryMemory&&) = delete;
  ScopedQueryMemory& operator=(ScopedQueryMemory&&) = delete;

 private:
  size_t original_;
};

inline void CreateWideTable(Database& rs, std::string_view name, size_t rows) {
  TransactionContext ctx = rs.BeginContext();
  Schema schema{
      std::string(name),
      {Column("key", ValueType::kInt64), Column("name", ValueType::kVarChar)}};
  StatusOr<Table> created = rs.CreateTable(ctx, schema);
  ASSERT_SUCCESS(created.GetStatus());
  for (int64_t key = 0; std::cmp_less(key, rows); ++key) {
    std::string payload = "row-" + std::to_string(key);
    payload.resize(96, 'x');
    ASSERT_SUCCESS(
        created.Value()
            .Insert(ctx.txn_,
                    Row(std::vector{Value(key), Value(std::string(payload))}))
            .GetStatus());
  }
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

inline long StatsValue(const std::string& text, const std::string& key) {
  const size_t pos = text.find(key);
  if (pos == std::string::npos) {
    return -1;
  }
  const size_t begin = pos + key.size();
  size_t end = begin;
  while (end < text.size() && std::isdigit(text[end]) != 0) {
    ++end;
  }
  return begin == end ? -1 : std::stol(text.substr(begin, end - begin));
}

inline std::string RelationalExplain(Database& database, std::string_view sql,
                                     bool analyze = false) {
  std::ostringstream out;
  for (const Row& row :
       RelationalRun(database, (analyze ? "EXPLAIN ANALYZE " : "EXPLAIN ") +
                                   std::string(sql))) {
    out << row[0].AsString() << '\n';
  }
  return out.str();
}

inline void CreateWideTableWithNullKeys(Database& rs, std::string_view name,
                                        size_t rows) {
  TransactionContext ctx = rs.BeginContext();
  Schema schema{
      std::string(name),
      {Column("key", ValueType::kInt64), Column("name", ValueType::kVarChar)}};
  StatusOr<Table> created = rs.CreateTable(ctx, schema);
  ASSERT_SUCCESS(created.GetStatus());
  for (int64_t key = 0; std::cmp_less(key, rows); ++key) {
    std::string payload = "row-" + std::to_string(key);
    payload.resize(96, 'x');
    Value key_value =
        (key % 20 == 0) ? Value() : Value(static_cast<int64_t>(key % 50));
    ASSERT_SUCCESS(
        created.Value()
            .Insert(ctx.txn_,
                    Row(std::vector{key_value, Value(std::string(payload))}))
            .GetStatus());
  }
  ASSERT_SUCCESS(ctx.txn_.PreCommit());
}

inline std::vector<std::pair<int64_t, int64_t>> KeyPairCounts(
    const std::vector<Row>& rows) {
  std::vector<std::pair<int64_t, int64_t>> out;
  out.reserve(rows.size());
  for (const Row& row : rows) {
    out.emplace_back(row[0].IsNull() ? -1 : row[0].value.int_value,
                     row[1].IsNull() ? -1 : row[1].value.int_value);
  }
  std::ranges::sort(out);
  return out;
}

}  // namespace tinylamb
