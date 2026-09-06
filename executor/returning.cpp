/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/returning.hpp"

#include <cstddef>
#include <ostream>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "expression/named_expression.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

bool ReturningExecutor::Next(Row* dst, RowPosition* rp) {
  Row src_row;
  if (!source_->Next(&src_row, rp)) {
    return false;
  }
  std::vector<Value> projected_values;
  projected_values.reserve(returning_expressions_.size());
  for (const NamedExpression& expr : returning_expressions_) {
    projected_values.push_back(
        expr.expression->Evaluate(src_row, input_schema_));
  }
  *dst = Row(std::move(projected_values));
  return true;
}

void ReturningExecutor::Dump(std::ostream& o, int indent) const {
  o << "ReturningExecutor: \n" << Indent(static_cast<size_t>(indent) + 2);
  source_->Dump(o, indent + 2);
}

}  // namespace tinylamb
