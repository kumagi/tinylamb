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
namespace {

BinaryOperation ParseBinaryOp(const std::string& text) {
  if (text == "=") {
    return BinaryOperation::kEquals;
  }
  if (text == "!=" || text == "<>") {
    return BinaryOperation::kNotEquals;
  }
  if (text == "<") {
    return BinaryOperation::kLessThan;
  }
  if (text == "<=") {
    return BinaryOperation::kLessThanEquals;
  }
  if (text == ">") {
    return BinaryOperation::kGreaterThan;
  }
  if (text == ">=") {
    return BinaryOperation::kGreaterThanEquals;
  }
  throw std::runtime_error("unsupported quantified comparison operator " +
                           text);
}

}  // namespace

Value QueryExpression::Evaluate(const Row& /*row*/,
                                const Schema& /*schema*/) const {
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
  StatusOr<std::vector<Value>> rows = context.RunSubquery(*query_, &row);
  if (!rows.HasValue()) {
    throw std::runtime_error("subquery execution failed: " +
                             std::string(tinylamb::ToString(rows.GetStatus())));
  }
  const std::vector<Value> values = std::move(rows).MoveValue();
  if (array_mode_) {
    std::string element_type;
    for (const Value& value : values) {
      if (!value.IsNull()) {
        switch (value.type) {
          case ValueType::kInt64:
            element_type = "INT64";
            break;
          case ValueType::kDouble:
            element_type = "FLOAT64";
            break;
          case ValueType::kVarChar:
            element_type = "STRING";
            break;
          case ValueType::kDate:
            element_type = "DATE";
            break;
          case ValueType::kArray:
            element_type = "ARRAY<" + value.ArrayElementSqlType() + ">";
            break;
          default:
            break;
        }
        break;
      }
    }
    return Value::Array(const_cast<std::vector<Value>&>(values), element_type);
  }
  if (exists_) {
    const bool any = !values.empty();
    return Value(negated_ ? !any : any);
  }
  if (test_) {
    const Value test_value = test_->Evaluate(row, schema, context);
    bool found = false;
    bool saw_null = test_value.IsNull();
    if (compare_op_.empty()) {
      // Plain IN membership: equality against each projected value.
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
      if (!negated_) {
        return membership;
      }
      return membership.IsNull() ? Value() : Value(!membership.Truthy());
    }
    // Quantified comparison: `<test> <op> {ANY|ALL} (<query>)`.
    const BinaryOperation op = ParseBinaryOp(compare_op_);
    for (const Value& candidate : values) {
      saw_null = saw_null || candidate.IsNull();
      if (found || test_value.IsNull() || candidate.IsNull()) {
        continue;
      }
      Value cmp;
      try {
        cmp = EvaluateBinary(op, test_value, candidate);
      } catch (...) {
        continue;
      }
      if (!cmp.IsNull() && cmp.Truthy()) {
        if (any_mode_) {
          found = true;
        } else {
          // ALL: a single false comparison fails the whole quantifier.
          return Value(false);
        }
      } else if (!any_mode_ && !cmp.IsNull()) {
        // ALL: NULL comparisons only block certainty (saw_null), but a
        // definite miss fails immediately.
        return Value(false);
      }
    }
    if (any_mode_) {
      return found ? Value(true) : (saw_null ? Value() : Value(false));
    }
    // ALL reached here only when every comparison was true or NULL.
    return saw_null ? Value() : Value(true);
  }
  if (values.empty()) {
    return {};
  }
  return values.front();
}

std::unordered_set<ColumnName> QueryExpression::TouchedColumns() const {
  return test_ ? test_->TouchedColumns() : std::unordered_set<ColumnName>{};
}

std::string QueryExpression::ToString() const {
  if (exists_) {
    return negated_ ? "NOT EXISTS(...)" : "EXISTS(...)";
  }
  if (test_) {
    return test_->ToString() + (negated_ ? " NOT IN(...)" : " IN(...)");
  }
  return "SCALAR_SUBQUERY(...)";
}

void QueryExpression::Dump(std::ostream& output) const { output << ToString(); }

}  // namespace tinylamb
