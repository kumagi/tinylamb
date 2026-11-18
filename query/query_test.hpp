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
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/window_eval.hpp"
#include "executor/detail/work_table_index.hpp"
#include "executor/executor_base.hpp"
#include "executor/parallel_scan.hpp"
#include "executor/query_memory.hpp"
#include "executor/spill_file.hpp"
#include "expression/expression.hpp"
#include "expression/window_function_expression.hpp"
#include "gtest/gtest.h"
#include "index/index_schema.hpp"
#include "query/plan_cache.hpp"
#include "query/sql_engine.hpp"
#include "query/statement.hpp"
#include "table/table_statistics.hpp"
#include "transaction/transaction.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

class QueryTest : public ::testing::Test {
 public:
  void SetUp() override {
    prefix_ = "query_test-" + RandomString();
    prefixes_.push_back(prefix_);
    db_ = Database::Create(prefix_).MoveValue();
    engines_.push_back(std::make_unique<SqlEngine>(*db_));
  }

  void TearDown() override {
    engines_.clear();
    db_.reset();
    // Last resort: the files are uniquely prefixed, sweep them directly.
    for (const auto& prefix : prefixes_) {
      static_cast<void>(::remove((prefix + ".db").c_str()));
      tinylamb::RemoveLogAndSegments((prefix + ".log"));
    }
  }

  std::unique_ptr<Database> MakeDatabase() {
    std::string p = "query_test-" + RandomString();
    prefixes_.push_back(p);
    return Database::Create(p).MoveValue();
  }

  std::string prefix_;
  std::unique_ptr<Database> db_;
  std::vector<std::unique_ptr<SqlEngine>> engines_;
  std::vector<std::string> prefixes_;
};

inline std::vector<Row> RunSql(TransactionContext& ctx, Database& db,
                               std::string_view sql) {
  SqlEngine engine(db);
  StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
  EXPECT_EQ(prepared.GetStatus(), Status::kSuccess) << sql << "\n"
                                                    << engine.LastError();
  std::vector<Row> rows;
  if (!prepared.HasValue()) {
    return rows;
  }
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    rows.push_back(row);
  }
  // EOF and failure are distinct: an executor that errors mid-drain must not
  // hand back a silently truncated result as if it were complete.
  EXPECT_EQ(prepared.Value()->GetStatus(), Status::kSuccess)
      << sql << "\n"
      << engine.LastError();
  return rows;
}

// Drains one statement through `engine` and returns the materialized rows.
inline std::vector<Row> RunPrepared(SqlEngine* engine, TransactionContext* ctx,
                                    const std::string& sql) {
  std::vector<Row> rows;
  StatusOr<Executor> prepared = engine->Prepare(*ctx, sql);
  EXPECT_EQ(prepared.GetStatus(), Status::kSuccess) << sql << "\n"
                                                    << engine->LastError();
  if (!prepared.HasValue()) {
    return rows;
  }
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    rows.push_back(row);
  }
  EXPECT_EQ(prepared.Value()->GetStatus(), Status::kSuccess)
      << sql << "\n"
      << engine->LastError();
  return rows;
}

// Runs a scalar SELECT to completion; execution-time errors (overflow, divide
// by zero) surface as exceptions from Next(), matching how the engine reports
// them to callers.
inline std::vector<Row> RunScalar(TransactionContext& ctx, Database& db,
                                  std::string_view sql) {
  SqlEngine engine(db);
  StatusOr<Executor> prepared = engine.Prepare(ctx, sql);
  EXPECT_TRUE(prepared.HasValue()) << engine.LastError();
  std::vector<Row> rows;
  if (!prepared.HasValue()) {
    return rows;
  }
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    rows.push_back(row);
  }
  const Status st = prepared.Value()->GetStatus();
  if (st != Status::kSuccess) {
    throw std::runtime_error(st.GetMessage());
  }
  return rows;
}

}  // namespace tinylamb
