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

Value EvaluateQuantifiedComparison(BinaryOperation op, QuantifierMode mode,
                                   const Value& test,
                                   const std::vector<Value>& rows) {
  const bool is_all = mode == QuantifierMode::kAll;
  // Collation resolution: an explicit case-insensitive collator on any
  // operand (test value or row) applies to the whole comparison set.
  bool any_ci = test.IsCaseInsensitive();
  for (const Value& candidate : rows) {
    any_ci = any_ci || candidate.IsCaseInsensitive();
  }
  Value test_value = test;
  std::vector<Value> candidates = rows;
  if (any_ci && test.type == ValueType::kVarChar) {
    test_value = Value(FoldCase(test.value.varchar_value))
                     .WithCollation(test.Collation());
    for (Value& candidate : candidates) {
      if (candidate.type == ValueType::kVarChar) {
        candidate = Value(FoldCase(candidate.value.varchar_value))
                        .WithCollation(candidate.Collation());
      }
    }
  }
  bool saw_true = false;
  bool saw_unknown = false;
  // Collated LIKE rejects the '_' wildcard outright (validation error, not a
  // per-row UNKNOWN).
  if (op == BinaryOperation::kLike || op == BinaryOperation::kNotLike) {
    if (any_ci) {
      for (const Value& candidate : candidates) {
        if (!candidate.IsNull() && candidate.type == ValueType::kVarChar &&
            std::string_view(candidate.value.varchar_value)
                    .find('_') != std::string_view::npos) {
          throw std::runtime_error(
              "LIKE pattern has '_' which is not allowed when its operands "
              "have collation: " +
              std::string(candidate.value.varchar_value));
        }
      }
    }
  }
  for (const Value& candidate : candidates) {
    Value result;
    if (!test_value.IsNull() && !candidate.IsNull()) {
      try {
        result = EvaluateBinary(op, test_value, candidate);
      } catch (...) {
        result = Value();
      }
    }
    if (result.IsNull()) {
      saw_unknown = true;
      continue;
    }
    if (result.Truthy()) {
      saw_true = true;
      if (!is_all) { return Value(true); }
    } else if (is_all) {
      return Value(false);
    }
  }
  if (!is_all) { return saw_true ? Value(true) : (saw_unknown ? Value() : Value(false));
}
  // ALL: TRUE only when every comparison was TRUE.
  return saw_unknown ? Value() : Value(true);
}

Value QueryExpression::Evaluate(const Row& /*row*/, const Schema& /*schema*/) const {
  throw std::runtime_error("query expression requires relational evaluation");
}

// Canonical subquery semantics, evaluated against the abstract context
// (improvement3.md A1).  The shapes share one projection:
//   EXISTS        : emptiness of the projection (never UNKNOWN)
//   test IN (...) : SQL three-valued membership; a NULL test value or a NULL
//                   row value turns a miss into UNKNOWN, and negation applies
//                   three-valued NOT to that result
//   quantified    : x <op> ANY/ALL(...) with three-valued OR/AND combination
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
    if (mode_ != QuantifierMode::kIn) {
      return EvaluateQuantifiedComparison(op_, mode_, test_value, values);
    }
    bool found = false;
    bool saw_null = test_value.IsNull();
    for (const Value& candidate : values) {
      if (!test_value.IsNull() && !candidate.IsNull() &&
          test_value.Collation() != 0 && candidate.Collation() != 0 &&
          test_value.Collation() != candidate.Collation()) {
        throw std::runtime_error(
            "Collation conflict between the IN operands");
      }
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
    if (mode_ == QuantifierMode::kAny || mode_ == QuantifierMode::kAll) {
      return test_->ToString() + " " +
             std::string(tinylamb::ToString(op_)) +
             (mode_ == QuantifierMode::kAny ? " ANY(...)" : " ALL(...)");
    }
    return test_->ToString() + (negated_ ? " NOT IN(...)" : " IN(...)");
  }
  return "SCALAR_SUBQUERY(...)";
}

void QueryExpression::Dump(std::ostream& output) const { output << ToString(); }

}  // namespace tinylamb
