/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_RETURNING_EXECUTOR_HPP
#define TINYLAMB_RETURNING_EXECUTOR_HPP

#include <memory>
#include <ostream>
#include <utility>
#include <vector>

#include "executor/executor_base.hpp"
#include "expression/named_expression.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class ReturningExecutor : public ExecutorBase {
 public:
  ReturningExecutor(Executor source, Schema input_schema,
                    std::vector<NamedExpression> returning_expressions)
      : source_(std::move(source)),
        input_schema_(std::move(input_schema)),
        returning_expressions_(std::move(returning_expressions)) {}
  ReturningExecutor(const ReturningExecutor&) = delete;
  ReturningExecutor(ReturningExecutor&&) = delete;
  ReturningExecutor& operator=(const ReturningExecutor&) = delete;
  ReturningExecutor& operator=(ReturningExecutor&&) = delete;
  ~ReturningExecutor() override = default;

  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  Executor source_;
  Schema input_schema_;
  std::vector<NamedExpression> returning_expressions_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_RETURNING_EXECUTOR_HPP
