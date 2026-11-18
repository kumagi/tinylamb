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

#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/parallel_work.hpp"
#include "common/query_parallelism.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/fused_grouped_join.hpp"
#include "executor/detail/grouped_numeric_hash.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/scan_tuple_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/typed_stream_aggregate.hpp"
#include "executor/detail/window_eval.hpp"
#include "executor/executor_base.hpp"
#include "executor/query_memory.hpp"
#include "executor/relational.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/rewrite.hpp"
#include "gtest/gtest.h"
#include "index/index_schema.hpp"
#include "query/sql_engine.hpp"
#include "query/statement.hpp"
#include "table/lstable_bridge.hpp"
#include "type/column.hpp"
#include "type/constraint.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

// Executes SQL through the SqlEngine in a fresh committed context and returns
// every produced row.
inline std::vector<Row> RelationalRun(Database& database,
                                      std::string_view sql) {
  TransactionContext context = database.BeginContext();
  SqlEngine engine(database);
  StatusOr<Executor> prepared = engine.Prepare(context, sql);
  std::vector<Row> rows;
  if (!prepared.HasValue()) {
    ADD_FAILURE() << sql << "\n" << engine.LastError();
    context.Abort();
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

// Collects EXPLAIN ANALYZE output lines for a statement.
inline std::string ExplainAnalyze(Database& database, std::string_view sql) {
  TransactionContext context = database.BeginContext();
  SqlEngine engine(database);
  StatusOr<Executor> prepared = engine.Prepare(
      context, std::string("EXPLAIN ANALYZE ") + std::string(sql));
  std::string profile;
  if (!prepared.HasValue()) {
    ADD_FAILURE() << sql << "\n" << engine.LastError();
    context.Abort();
    return profile;
  }
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    profile += std::string(row[0].value.varchar_value) + "\n";
  }
  EXPECT_EQ(prepared.Value()->GetStatus(), Status::kSuccess) << sql;
  EXPECT_EQ(context.PreCommit(), Status::kSuccess);
  return profile;
}

class RelationalRegressionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    prefix_ = "relational_regression-" + RandomString();
    rs_ = Database::Create(prefix_).MoveValue();
    TransactionContext ctx = rs_->BeginContext();
    ASSIGN_OR_ASSERT_FAIL(
        Table, agg,
        rs_->CreateTable(ctx,
                         Schema("AggTable", {Column("k", ValueType::kInt64),
                                             Column("v", ValueType::kInt64)})));
    for (int64_t i = 0; i < 200; ++i) {
      ASSERT_SUCCESS(
          agg.Insert(ctx.txn_, Row({Value(i % 10), Value(1)})).GetStatus());
    }
    ASSIGN_OR_ASSERT_FAIL(
        Table, small,
        rs_->CreateTable(
            ctx, Schema("SmallKeys", {Column("k", ValueType::kInt64)})));
    for (const int64_t key : {int64_t{1}, int64_t{2}, int64_t{3}, int64_t{4}}) {
      ASSERT_SUCCESS(small.Insert(ctx.txn_, Row({Value(key)})).GetStatus());
    }
    ASSIGN_OR_ASSERT_FAIL(
        Table, driver,
        rs_->CreateTable(ctx,
                         Schema("KeyTable", {Column("k", ValueType::kInt64)})));
    for (int64_t i = 0; i < 10; ++i) {
      ASSERT_SUCCESS(driver.Insert(ctx.txn_, Row({Value(i)})).GetStatus());
    }
    ASSERT_SUCCESS(ctx.txn_.PreCommit());
  }

  void TearDown() override { rs_->DeleteAll(); }

  std::string prefix_;
  std::unique_ptr<Database> rs_;
};

}  // namespace tinylamb
