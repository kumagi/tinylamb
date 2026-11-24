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
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/platform/random.hpp"
#include "common/random_string.hpp"
#include "common/status_or.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/constant_executor.hpp"
#include "executor/data_chunk.hpp"
#include "executor/executor_base.hpp"
#include "executor/hash_join.hpp"
#include "executor/update.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/query_expression.hpp"
#include "gtest/gtest.h"
#include "index/index_schema.hpp"
#include "plan/band_join_plan.hpp"
#include "plan/cascades.hpp"
#include "plan/full_scan_plan.hpp"
#include "plan/implementation_rules.hpp"
#include "plan/index_scan_plan.hpp"
#include "plan/merge_join_plan.hpp"
#include "plan/optimizer.hpp"
#include "plan/plan.hpp"
#include "plan/product_plan.hpp"
#include "plan/selection_plan.hpp"
#include "plan/sort_distinct_plan.hpp"
#include "plan/sort_plan.hpp"
#include "plan/zigzag_join_plan.hpp"
#include "query/query_data.hpp"
#include "query/statement.hpp"
#include "table/iterator.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "type/column_name.hpp"
#include "type/constraint.hpp"
#include "type/row.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

// Builds `EXISTS (SELECT <inner_select> FROM <table> WHERE <where>)` shaped
// subquery expressions for the decorrelation tests.
inline Expression ExistsSubquery(const std::vector<NamedExpression>& select,
                                 const std::string& table, Expression where,
                                 bool negated = false) {
  auto statement = std::make_shared<SelectStatement>(
      select, std::vector<std::string>{table}, std::move(where));
  Expression expression =
      QueryExpressionExp(std::move(statement), nullptr, true, false);
  return negated
             ? UnaryExpressionExp(std::move(expression), UnaryOperation::kNot)
             : expression;
}

class OptimizerTest : public ::testing::Test {
 public:
  void SetUp() override {
    prefix_ = "optimizer_test-" + RandomString();
    Recover();
    TransactionContext ctx = rs_->BeginContext();
    {
      ASSIGN_OR_ASSERT_FAIL(
          Table, tbl,
          rs_->CreateTable(ctx,
                           Schema("Sc1", {Column("c1", ValueType::kInt64),
                                          Column("c2", ValueType::kVarChar),
                                          Column("c3", ValueType::kDouble)})));
      for (int i = 0; i < 100; ++i) {
        ASSERT_SUCCESS(
            tbl.Insert(ctx.txn_,
                       Row({Value(i), Value("c2-" + std::to_string(i)),
                            Value(i + 9.9)}))
                .GetStatus());
      }
    }
    {
      ASSIGN_OR_ASSERT_FAIL(
          Table, tbl,
          rs_->CreateTable(ctx,
                           Schema("Sc2", {Column("d1", ValueType::kInt64),
                                          Column("d2", ValueType::kDouble),
                                          Column("d3", ValueType::kVarChar),
                                          Column("d4", ValueType::kInt64)})));
      for (int i = 0; i < 200; ++i) {
        ASSERT_SUCCESS(
            tbl.Insert(ctx.txn_,
                       Row({Value(i), Value(i + 0.2),
                            Value("d3-" + std::to_string(i % 10)), Value(16)}))
                .GetStatus());
      }
    }
    {
      ASSIGN_OR_ASSERT_FAIL(
          Table, tbl,
          rs_->CreateTable(ctx,
                           Schema("Sc3", {Column("e1", ValueType::kInt64),
                                          Column("e2", ValueType::kDouble)})));
      for (int i = 20; 0 < i; --i) {
        ASSERT_SUCCESS(
            tbl.Insert(ctx.txn_, Row({Value(i), Value(i + 53.4)})).GetStatus());
      }
    }
    {
      ASSIGN_OR_ASSERT_FAIL(
          Table, tbl,
          rs_->CreateTable(ctx,
                           Schema("Sc4", {Column("c1", ValueType::kInt64),
                                          Column("c2", ValueType::kVarChar)})));
      for (int i = 100; 0 < i; --i) {
        ASSERT_SUCCESS(
            tbl.Insert(ctx.txn_, Row({Value(i), Value(std::to_string(i % 4))}))
                .GetStatus());
      }
    }
    ASSERT_SUCCESS(rs_->CreateIndex(ctx, "Sc1", IndexSchema("KeyIdx", {1, 2})));
    ASSERT_SUCCESS(rs_->CreateIndex(ctx, "Sc1", IndexSchema("Sc1PK", {0})));
    ASSERT_SUCCESS(rs_->CreateIndex(ctx, "Sc2", IndexSchema("Sc2PK", {0})));
    ASSERT_SUCCESS(rs_->CreateIndex(
        ctx, "Sc2",
        IndexSchema("NameIdx", {2, 3}, {0, 1}, IndexMode::kNonUnique)));
    ASSERT_SUCCESS(rs_->CreateIndex(
        ctx, "Sc4", IndexSchema("Sc4_IDX", {1}, {}, IndexMode::kNonUnique)));
    ASSERT_SUCCESS(ctx.txn_.PreCommit());

    auto stat_tx = rs_->BeginContext();
    ASSERT_SUCCESS(rs_->RefreshStatistics(stat_tx, "Sc1"));
    ASSERT_SUCCESS(rs_->RefreshStatistics(stat_tx, "Sc2"));
    ASSERT_SUCCESS(rs_->RefreshStatistics(stat_tx, "Sc3"));
    ASSERT_SUCCESS(rs_->RefreshStatistics(stat_tx, "Sc4"));
    ASSERT_SUCCESS(stat_tx.PreCommit());
  }

  void Recover() {
    if (rs_) {
      rs_->EmulateCrash();
      rs_.reset();
    }
    rs_ = Database::Create(prefix_).MoveValue();
  }

  void TearDown() override { rs_->DeleteAll(); }

  [[nodiscard]] Status DumpAll(const QueryData& qd)
      const {  // Arrange: open context + rewrite query for debugging
    TransactionContext ctx = rs_->BeginContext();
    QueryData qd_resolved = qd;
    // DumpAll returns Status, so a gtest ASSERT_* is not usable here: propagate
    // a rewrite failure explicitly. Otherwise an Optimizer that tolerates the
    // un-rewritten QueryData would keep every DumpAll smoke test green even
    // when name resolution regressed.
    Status rewrite_status = qd_resolved.Rewrite(ctx);
    if (rewrite_status != Status::kSuccess) {
      return rewrite_status;
    }
    LOG(INFO) << qd << "\n";

    // Act: optimize plan + emit executor + execute query
    ASSIGN_OR_RETURN(Plan, plan, Optimizer::Optimize(qd_resolved, ctx));
    Executor exec = plan->EmitExecutor(ctx);
    LOG(INFO) << " --- Logical Plan ---\n" << *plan;
    LOG(INFO) << "\n --- Physical Plan ---\n" << *exec;
    LOG(INFO) << "\n --- Output ---\n" << plan->GetSchema();
    Row result;
    while (exec->Next(&result, nullptr)) {
      LOG(INFO) << result;
    }
    return Status::kSuccess;
  }

  std::string prefix_;
  std::unique_ptr<Database> rs_;
};

// gtest ASSERT_TRUE cannot be used in a non-void helper. value() keeps the
// failure explicit in every build mode instead of relying on debug-only assert.
inline const LookupSeek& SeekOf(const std::optional<LookupSeek>& probe) {
  if (!probe.has_value()) {
    throw std::logic_error("expected a lookup seek");
  }
  return probe.value();
}

// Row count above kParallelScanMinRows (8192) so a full scan is emitted as a
// parallel scan. It is deliberately below kParallelAggregationMinRows (32768):
// do NOT use this table to expect a ParallelAggregationExecutor -- use a
// dedicated larger row count for that.
inline constexpr int64_t kParallelTestRows = 9000;

inline void InsertAnalyzedBigTable(Database* db, std::string_view table) {
  TransactionContext writer = db->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, tbl,
      db->CreateTable(writer,
                      Schema(table, {Column("key", ValueType::kInt64),
                                     Column("payload", ValueType::kInt64)})));
  for (int64_t key = 0; key < kParallelTestRows; ++key) {
    ASSERT_SUCCESS(
        tbl.Insert(writer.txn_, Row({Value(key), Value(key * 3)})).GetStatus());
  }
  ASSERT_SUCCESS(writer.PreCommit());
  TransactionContext stats_ctx = db->BeginContext();
  ASSERT_SUCCESS(db->RefreshStatistics(stats_ctx, table));
  ASSERT_SUCCESS(stats_ctx.PreCommit());
}

// Analyzed table whose primary key arrives in ascending order, so a full scan
// over it may report the clustered index order (plan/full_scan_plan.hpp).
inline void InsertAnalyzedClusteredTable(Database* db, std::string_view table,
                                         int64_t rows) {
  TransactionContext writer = db->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, tbl,
      db->CreateTable(
          writer, Schema(table, {Column("key", ValueType::kInt64,
                                        Constraint(Constraint::kPrimaryKey)),
                                 Column("payload", ValueType::kInt64)})));
  for (int64_t key = 0; key < rows; ++key) {
    ASSERT_SUCCESS(
        tbl.Insert(writer.txn_, Row({Value(key), Value(key * 3)})).GetStatus());
  }
  ASSERT_SUCCESS(writer.PreCommit());
  TransactionContext stats_ctx = db->BeginContext();
  ASSERT_SUCCESS(db->RefreshStatistics(stats_ctx, table));
  ASSERT_SUCCESS(stats_ctx.PreCommit());
}

inline int CountRows(const Executor& executor) {
  Row row;
  int count = 0;
  while (executor->Next(&row, nullptr)) {
    ++count;
  }
  return count;
}

// Snapshot 1:1 proof end-to-end: TopnDim(id PRIMARY KEY, val) and
// TopnFact(id, dim_id, payload) with every fact.dim_id present in dim.id.
// Returns the fact ids of ORDER BY TopnFact.id LIMIT 5 over the inner join.
inline std::vector<int64_t> RunTopNOneToOneJoin(Database* db, bool disable_rule,
                                                std::string* plan_dump) {
  QueryData query{
      {"TopnFact", "TopnDim"},
      BinaryExpressionExp(ColumnValueExp(ColumnName("TopnFact", "dim_id")),
                          BinaryOperation::kEquals,
                          ColumnValueExp(ColumnName("TopnDim", "id"))),
      {NamedExpression("id", ColumnValueExp(ColumnName("TopnFact", "id")))}};
  query.order_expressions_.push_back(
      ColumnValueExp(ColumnName("TopnFact", "id")));
  query.order_ascending_.push_back(true);
  query.limit_count_ = 5;
  TransactionContext context = db->BeginContext();
  Status rewrite_status = query.Rewrite(context);
  EXPECT_EQ(rewrite_status, Status::kSuccess);
  if (rewrite_status != Status::kSuccess) {
    return {};
  }
  OptimizerOptions options = OptimizerOptions::Default();
  if (disable_rule) {
    EXPECT_TRUE(options.relational_rules.Remove(
        "topn_push_through_proven_one_to_one_join"));
  }
  auto plan_or = Optimizer::Optimize(query, context, options);
  EXPECT_EQ(plan_or.GetStatus(), Status::kSuccess);
  if (plan_or.GetStatus() != Status::kSuccess) {
    return {};
  }
  if (plan_dump != nullptr) {
    std::ostringstream dump;
    plan_or.Value()->Dump(dump, 0);
    *plan_dump = dump.str();
  }
  Executor executor = plan_or.Value()->EmitExecutor(context);
  std::vector<int64_t> ids;
  Row row;
  while (executor->Next(&row, nullptr)) {
    ids.push_back(row[0].value.int_value);
  }
  EXPECT_EQ(context.PreCommit(), Status::kSuccess);
  return ids;
}

inline void BuildTopNOneToOneTables(Database* db) {
  TransactionContext writer = db->BeginContext();
  ASSIGN_OR_ASSERT_FAIL(
      Table, dim,
      db->CreateTable(
          writer,
          Schema("TopnDim", {Column("id", ValueType::kInt64,
                                    Constraint(Constraint::kPrimaryKey)),
                             Column("val", ValueType::kInt64)})));
  ASSIGN_OR_ASSERT_FAIL(
      Table, fact,
      db->CreateTable(
          writer, Schema("TopnFact", {Column("id", ValueType::kInt64),
                                      Column("dim_id", ValueType::kInt64),
                                      Column("payload", ValueType::kInt64)})));
  for (int i = 0; i < 10; ++i) {
    ASSERT_SUCCESS(
        dim.Insert(writer.txn_, Row({Value(i), Value(i * 100)})).GetStatus());
  }
  for (int i = 0; i < 100; ++i) {
    ASSERT_SUCCESS(
        fact.Insert(writer.txn_, Row({Value(i), Value(i % 10), Value(i * 3)}))
            .GetStatus());
  }
  ASSERT_SUCCESS(writer.PreCommit());
}

}  // namespace tinylamb
