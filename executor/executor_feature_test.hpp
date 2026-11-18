/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#pragma once

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/join_kind.hpp"
#include "common/random_string.hpp"
#include "common/set_operation.hpp"
#include "common/status_or.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "executor/data_chunk.hpp"
#include "executor/executor_base.hpp"
#include "executor/hash_join.hpp"
#include "executor/hash_join_mode.hpp"
#include "executor/merge_join.hpp"
#include "executor/parallel_hash_join.hpp"
#include "executor/parallel_merge_join.hpp"
#include "executor/query_memory.hpp"
#include "executor/set_operation.hpp"
#include "executor/simd_comparison.hpp"
#include "executor/skip_scan_distinct.hpp"
#include "executor/values.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/unary_expression.hpp"
#include "index/index.hpp"
#include "index/index_schema.hpp"
#include "page/row_position.hpp"
#include "query/googlesql_frontend.hpp"
#include "query/sql_engine.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

// Executes one statement and returns the materialized rows.  A prepare or
// execution failure surfaces as a runtime_error carrying the diagnostic.
inline std::vector<Row> RunSql(SqlEngine* engine, TransactionContext* ctx,
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
    throw std::runtime_error(st.GetMessage());
  }
  return rows;
}

// Drains a directly-constructed executor row by row.
inline std::vector<Row> Drain(ExecutorBase* executor) {
  std::vector<Row> rows;
  Row row;
  RowPosition rp;
  while (executor->Next(&row, &rp)) {
    rows.push_back(row);
    row = Row();
  }
  return rows;
}

inline Executor Src(std::vector<Row> rows) {
  return std::make_shared<ValuesExecutor>(std::move(rows));
}

inline Row IRow(int64_t v) { return Row({Value(v)}); }

inline std::vector<Row> SequenceRows(int64_t begin, int64_t end) {
  std::vector<Row> rows;
  rows.reserve(static_cast<size_t>(end - begin));
  for (int64_t v = begin; v < end; ++v) {
    rows.push_back(IRow(v));
  }
  return rows;
}

// Every row of `expected` is matched exactly once against `actual`.
inline void ExpectSameMultiset(const std::vector<Row>& left,
                               const std::vector<Row>& right) {
  std::vector<Row> remaining = right;
  for (const Row& row : left) {
    bool found = false;
    for (size_t i = 0; i < remaining.size(); ++i) {
      if (remaining[i] == row) {
        remaining.erase(remaining.begin() + static_cast<long>(i));
        found = true;
        break;
      }
    }
    EXPECT_TRUE(found);
  }
  EXPECT_TRUE(remaining.empty());
}

// RAII shrink of the global query-memory budget; restores the previous limit
// on scope exit.  Used to force reactive-spill / hybrid-join paths.
class BudgetGuard {
 public:
  BudgetGuard(const BudgetGuard&) = delete;
  BudgetGuard& operator=(const BudgetGuard&) = delete;
  BudgetGuard(BudgetGuard&&) = delete;
  BudgetGuard& operator=(BudgetGuard&&) = delete;
  explicit BudgetGuard(size_t limit_bytes) {
    previous_ = QueryMemoryBudget::Global().Limit();
    QueryMemoryBudget::Global().ResetForTest(limit_bytes);
  }
  ~BudgetGuard() { QueryMemoryBudget::Global().ResetForTest(previous_); }

 private:
  size_t previous_{0};
};

// A child executor that yields nothing and latches a failure, for sticky
// error propagation tests.
class FailingSource : public ExecutorBase {
 public:
  size_t NextBatch(DataChunk* dst, size_t /*max_rows*/) override {
    dst->Reset();
    return 0;
  }
  void Dump(std::ostream& o, int /*indent*/) const override {
    o << "FailingSource";
  }
  [[nodiscard]] Status GetStatus() const override {
    return StatusError(StatusCode::kRuntimeError, "failing source");
  }
};

// A source that emits each row with an explicit RowPosition, for
// position-propagation tests (ValuesExecutor only ever yields the invalid
// default position, which cannot distinguish "propagated" from "dropped").
class PositionedSource : public ExecutorBase {
 public:
  explicit PositionedSource(std::vector<std::pair<Row, RowPosition>> rows)
      : rows_(std::move(rows)) {}
  size_t NextBatch(DataChunk* dst, size_t max_rows) override {
    dst->Reset();
    size_t emitted = 0;
    while (offset_ < rows_.size() && emitted < max_rows) {
      dst->Append(std::move(rows_[offset_].first), rows_[offset_].second);
      ++offset_;
      ++emitted;
    }
    return emitted;
  }
  void Dump(std::ostream& o, int /*indent*/) const override {
    o << "PositionedSource";
  }

 private:
  std::vector<std::pair<Row, RowPosition>> rows_;
  size_t offset_{0};
};

// Sorted rows with a single INT64 key plus a payload, each key repeated
// `dup` times.  Merge-join inputs must arrive ordered.
inline std::vector<Row> SortedKeyRows(int64_t begin, int64_t end, int64_t dup) {
  std::vector<Row> rows;
  for (int64_t k = begin; k < end; ++k) {
    for (int64_t d = 0; d < dup; ++d) {
      rows.push_back(Row({Value(k), Value((k * 10) + d)}));
    }
  }
  return rows;
}

// Deterministic ordering for result-set comparison.  Only for rows without
// NULL values (Value ordering against NULL throws by design).
inline void SortRowsByValue(std::vector<Row>* rows) {
  std::sort(rows->begin(), rows->end(), [](const Row& a, const Row& b) {
    const size_t n = std::min(a.values_.size(), b.values_.size());
    for (size_t i = 0; i < n; ++i) {
      if (a[i] == b[i]) {
        continue;
      }
      return a[i] < b[i];
    }
    return a.values_.size() < b.values_.size();
  });
}

}  // namespace tinylamb
