/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "common/platform/process.hpp"
#include "database/transaction_context.hpp"
#include "executor/executor_base.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/window_function_expression.hpp"
#include "query/sql_engine.hpp"

namespace tinylamb::window_coverage_test {
// Executes one statement and returns the materialized rows.  A prepare or
// execution failure surfaces as a runtime_error carrying the diagnostic.
inline std::vector<Row> RunSql(SqlEngine* engine, TransactionContext* ctx,
                               const std::string& sql) {
  // One retry for empty diagnostics: an empty LastError can indicate that an
  // external parse child is transiently unavailable under load. Other paths
  // may also return an empty diagnostic, so this retry is deliberately bounded.
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

// Non-throwing variant: errors come back as a failed StatusOr.
inline StatusOr<std::vector<Row>> TryRunSql(SqlEngine* engine,
                                            TransactionContext* ctx,
                                            const std::string& sql) {
  try {
    return RunSql(engine, ctx, sql);
  } catch (const std::exception& error) {
    return StatusError(StatusCode::kRuntimeError, error.what());
  }
}

// Prepares and drains `sql`, returning the executor's Dump() text so tests can
// assert route/counter evidence (ParallelScan, HashAggregate, fused typed
// folds, runtime filters, ...).  Returns nullopt on prepare or run failure.
inline std::optional<std::string> DumpSql(SqlEngine* engine,
                                          TransactionContext* ctx,
                                          const std::string& sql) {
  StatusOr<Executor> prepared = engine->Prepare(*ctx, sql);
  if (!prepared.HasValue()) {
    return std::nullopt;
  }
  Row row;
  while (prepared.Value()->Next(&row, nullptr)) {
    row = Row();
  }
  if (prepared.Value()->GetStatus() != Status::kSuccess) {
    return std::nullopt;
  }
  std::ostringstream dump;
  prepared.Value()->Dump(dump, 0);
  return dump.str();
}

// Restores an environment variable to the value it held on construction (or
// unsets it when it was absent) on destruction, so tests that toggle
// TINYLAMB_EXEC_ENGINE do not leak state into later tests.
// NOLINTNEXTLINE(cppcoreguidelines-special-member-functions): scoped test
// environment guard.
class EnvGuard {
 public:
  explicit EnvGuard(const char* name) : name_(name) {
    if (const char* value = ::tinylamb::platform::GetEnv(name)) {
      had_value_ = true;
      value_ = value;
    }
  }
  EnvGuard(const EnvGuard&) = delete;
  EnvGuard& operator=(const EnvGuard&) = delete;
  EnvGuard(EnvGuard&&) = delete;
  EnvGuard& operator=(EnvGuard&&) = delete;
  ~EnvGuard() { Restore(); }
  void Restore() {
    if (had_value_) {
      ::tinylamb::platform::SetEnv(name_.c_str(), value_.c_str(), 1);
    } else {
      ::tinylamb::platform::UnsetEnv(name_.c_str());
    }
  }

 private:
  std::string name_;
  std::string value_;
  bool had_value_{false};
};

// First row whose column `col` equals `key`, or nullptr.
inline const Row* FindRow(const std::vector<Row>& rows, size_t col,
                          const Value& key) {
  for (const Row& row : rows) {
    if (row[col] == key) {
      return &row;
    }
  }
  return nullptr;
}

inline WindowOrderTerm TermOnV(bool ascending = true) {
  return WindowOrderTerm{.expression = ColumnValueExp("v"),
                         .ascending = ascending,
                         .nulls_first = std::nullopt};
}

inline std::shared_ptr<WindowFunctionCallExpression> MakeWindow(
    const std::string& function, std::vector<Expression> args = {}) {
  auto window = std::make_shared<WindowFunctionCallExpression>();
  window->function = function;
  window->args = std::move(args);
  return window;
}

inline WindowFrameBound Bound(WindowFrameBoundType type,
                              Expression offset = nullptr) {
  return WindowFrameBound{.type = type, .offset = std::move(offset)};
}

inline WindowFrameBound OffsetBound(WindowFrameBoundType type, int64_t offset) {
  return Bound(type, ConstantValueExp(Value(offset)));
}

inline WindowFrameBound OffsetBoundDouble(WindowFrameBoundType type,
                                          double offset) {
  return Bound(type, ConstantValueExp(Value(offset)));
}

// Aggregate argument filters used by FILTER (WHERE ...) style tests.
inline Expression Vgt(int64_t threshold) {
  return BinaryExpressionExp(ColumnValueExp("v"), BinaryOperation::kGreaterThan,
                             ConstantValueExp(Value(threshold)));
}

}  // namespace tinylamb::window_coverage_test
