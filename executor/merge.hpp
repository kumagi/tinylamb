/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_MERGE_EXECUTOR_HPP
#define TINYLAMB_MERGE_EXECUTOR_HPP

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "executor/executor_base.hpp"
#include "expression/expression.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
class Table;
class Transaction;

struct WhenMatchedClause {
  enum class Action { kUpdate, kDelete };
  Action action{Action::kUpdate};
  Expression condition{nullptr};
  std::vector<std::pair<size_t, Expression>> assignments;

  static WhenMatchedClause Update(
      std::vector<std::pair<size_t, Expression>> assignments,
      Expression condition = nullptr) {
    WhenMatchedClause clause;
    clause.action = Action::kUpdate;
    clause.condition = std::move(condition);
    clause.assignments = std::move(assignments);
    return clause;
  }

  static WhenMatchedClause Delete(Expression condition = nullptr) {
    WhenMatchedClause clause;
    clause.action = Action::kDelete;
    clause.condition = std::move(condition);
    return clause;
  }
};

struct WhenNotMatchedClause {
  enum class Action { kInsert };
  Action action{Action::kInsert};
  Expression condition{nullptr};
  std::vector<Expression> values;
  std::vector<size_t> target_columns;

  static WhenNotMatchedClause Insert(
      std::vector<Expression> values,
      std::vector<size_t> target_columns = {},
      Expression condition = nullptr) {
    WhenNotMatchedClause clause;
    clause.action = Action::kInsert;
    clause.condition = std::move(condition);
    clause.values = std::move(values);
    clause.target_columns = std::move(target_columns);
    return clause;
  }
};

class MergeExecutor : public ExecutorBase {
 public:
  MergeExecutor(Transaction& txn, Table* target_table,
                Executor source, Schema source_schema,
                Expression on_condition,
                std::vector<WhenMatchedClause> matched_clauses,
                std::vector<WhenNotMatchedClause> not_matched_clauses);

  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& o, int indent) const override;

  [[nodiscard]] int64_t InsertedCount() const { return inserted_count_; }
  [[nodiscard]] int64_t UpdatedCount() const { return updated_count_; }
  [[nodiscard]] int64_t DeletedCount() const { return deleted_count_; }
  [[nodiscard]] int64_t TotalModified() const {
    return inserted_count_ + updated_count_ + deleted_count_;
  }

 private:
  Transaction* txn_;
  Table* target_table_;
  Executor source_;
  Schema source_schema_;
  Expression on_condition_;
  std::vector<WhenMatchedClause> matched_clauses_;
  std::vector<WhenNotMatchedClause> not_matched_clauses_;

  int64_t inserted_count_{0};
  int64_t updated_count_{0};
  int64_t deleted_count_{0};
  bool executed_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_MERGE_EXECUTOR_HPP
