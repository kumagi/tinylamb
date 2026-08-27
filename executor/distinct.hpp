/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_DISTINCT_EXECUTOR_HPP
#define TINYLAMB_DISTINCT_EXECUTOR_HPP
#include <vector>

#include "executor/executor_base.hpp"
#include "expression/expression.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {
class DistinctExecutor : public ExecutorBase {
 public:
  explicit DistinctExecutor(Executor source, Schema schema = {},
                            std::vector<Expression> distinct_on = {})
      : source_(std::move(source)),
        schema_(std::move(schema)),
        distinct_on_(std::move(distinct_on)) {}
  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& output, int indent) const override;

 private:
  Executor source_;
  Schema schema_;
  std::vector<Expression> distinct_on_;
  std::vector<Row> seen_;
  std::vector<std::vector<Value>> seen_keys_;
};

class SortDistinctExecutor final : public ExecutorBase {
 public:
  explicit SortDistinctExecutor(Executor source) : source_(std::move(source)) {}
  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& output, int indent) const override;

 private:
  Executor source_;
  Row previous_;
  bool have_previous_{false};
};
}  // namespace tinylamb
#endif
