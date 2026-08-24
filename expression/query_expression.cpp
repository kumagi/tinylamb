/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/query_expression.hpp"

#include <ostream>
#include <stdexcept>
#include <unordered_set>
#include <utility>
#include <vector>

#include "expression/binary_expression.hpp"
#include "expression/evaluation_context.hpp"
#include "expression/expression.hpp"
#include "type/column_name.hpp"
#include "type/value.hpp"

namespace tinylamb {

Value QueryExpression::Evaluate(const Row& /*row*/, const Schema& /*schema*/) const {
  throw std::runtime_error("query expression requires relational evaluation");
}

// Canonical subquery semantics, evaluated against the abstract context
// (improvement3.md A1).  The three shapes share one projection:
//   EXISTS        : emptiness of the projection (never UNKNOWN)
//   test IN (...) : SQL three-valued membership; a NULL test value or a NULL
//                   row value turns a miss into UNKNOWN, and negation applies
//                   three-valued NOT to that result
//   scalar        : first projected value, NULL when the subquery is empty
Value QueryExpression::Evaluate(const Row& row, const Schema& schema,
                                EvaluationContext& context) const {
  StatusOr<std::vector<Value>> rows =
      context.RunSubquery(*query_, &row);
  if (!rows.HasValue()) {
    throw std::runtime_error("subquery execution failed: " +
                             std::string(tinylamb::ToString(rows.GetStatus())));
  }
  const std::vector<Value> values = std::move(rows).MoveValue();
  if (exists_) {
    const bool any = !values.empty();
    return Value(negated_ ? !any : any);
  }
  if (test_) {
    const Value test_value = test_->Evaluate(row, schema, context);
    bool found = false;
    bool saw_null = test_value.IsNull();
    for (const Value& candidate : values) {
      saw_null = saw_null || candidate.IsNull();
      if (!found && !test_value.IsNull() && !candidate.IsNull() &&
          EvaluateBinary(BinaryOperation::kEquals, test_value, candidate)
              .Truthy()) {
        found = true;
      }
    }
    const Value membership =
        found ? Value(true) : (saw_null ? Value() : Value(false));
    if (!negated_) { return membership;
}
    return membership.IsNull() ? Value() : Value(!membership.Truthy());
  }
  if (values.empty()) { return {};
}
  return values.front();
}

std::unordered_set<ColumnName> QueryExpression::TouchedColumns() const {
  return test_ ? test_->TouchedColumns() : std::unordered_set<ColumnName>{};
}

std::string QueryExpression::ToString() const {
  if (exists_) { return negated_ ? "NOT EXISTS(...)" : "EXISTS(...)";
}
  if (test_) {
    return test_->ToString() + (negated_ ? " NOT IN(...)" : " IN(...)");
  }
  return "SCALAR_SUBQUERY(...)";
}

void QueryExpression::Dump(std::ostream& output) const { output << ToString(); }

}  // namespace tinylamb
