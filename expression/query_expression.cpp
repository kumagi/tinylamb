/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "expression/query_expression.hpp"

#include <ostream>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "expression/binary_expression.hpp"
#include "expression/evaluation_context.hpp"
#include "expression/expression.hpp"
#include "type/collation.hpp"
#include "type/column_name.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

StatusOr<Value> TryEvaluateQuantifiedComparison(
    BinaryOperation op, QuantifierMode mode, const Value& test,
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
            std::string_view(candidate.value.varchar_value).find('_') !=
                std::string_view::npos) {
          return StatusError(
              StatusCode::kInvalidArgument,
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
      // A comparison error on one row is UNKNOWN for that row (three-valued
      // logic), matching the old catch-all around the throwing evaluator.
      auto compared = TryEvaluateBinary(op, test_value, candidate);
      result = compared.HasValue() ? compared.MoveValue() : Value();
    }
    if (result.IsNull()) {
      saw_unknown = true;
      continue;
    }
    if (result.Truthy()) {
      saw_true = true;
      if (!is_all) {
        return Value(true);
      }
    } else if (is_all) {
      return Value(false);
    }
  }
  if (!is_all) {
    return saw_true ? Value(true) : (saw_unknown ? Value() : Value(false));
  }
  // ALL: TRUE only when every comparison was TRUE.
  return saw_unknown ? Value() : Value(true);
}

StatusOr<Value> QueryExpression::TryEvaluate(const Row& /*row*/,
                                             const Schema& /*schema*/) const {
  return StatusError(StatusCode::kRuntimeError,
                     "query expression requires relational evaluation");
}

Value QueryExpression::Evaluate(const Row& row, const Schema& schema) const {
  return ExcShimUnwrap(TryEvaluate(row, schema), "QueryExpression::Evaluate");
}

// Canonical subquery semantics, evaluated against the abstract context
// (improvement3.md A1).  The shapes share one projection:
//   EXISTS        : emptiness of the projection (never UNKNOWN)
//   test IN (...) : SQL three-valued membership; a NULL test value or a NULL
//                   row value turns a miss into UNKNOWN, and negation applies
//                   three-valued NOT to that result
//   quantified    : x <op> ANY/ALL(...) with three-valued OR/AND combination
//   scalar        : first projected value, NULL when the subquery is empty
StatusOr<Value> QueryExpression::TryEvaluate(const Row& row,
                                             const Schema& schema,
                                             EvaluationContext& context) const {
  StatusOr<EvaluationContext::SubqueryResult> rows =
      context.RunSubqueryDetailed(*query_, &row);
  if (!rows.HasValue()) {
    return StatusError(rows.GetStatus().GetCode(),
                       "subquery execution failed: " +
                           std::string(tinylamb::ToString(rows.GetStatus())));
  }
  EvaluationContext::SubqueryResult subquery = std::move(rows).MoveValue();
  const std::vector<Value> values = std::move(subquery.values);
  if (array_result_) {
    // Keep parity with the relational evaluator: an empty declared tag is
    // inferred from the produced elements first (a lambda-desugared
    // ARRAY_TRANSFORM returning strings must not be declared
    // ARRAY<INT64>), then the subquery's result schema, with INT64 as the
    // last resort for empty/all-NULL results.
    std::string element_type = array_element_sql_type_;
    if (element_type.empty()) {
      for (const Value& element : values) {
        if (!element.IsNull()) {
          element_type = std::string(ElementSqlTypeName(element.type));
          break;
        }
      }
      if (element_type.empty()) {
        element_type = std::move(subquery.element_type_hint);
      }
      if (element_type.empty()) {
        element_type = "INT64";
      }
    }
    return Value::Array(values, element_type);
  }
  if (exists_) {
    const bool any = !values.empty();
    return Value(negated_ ? !any : any);
  }
  if (test_) {
    ASSIGN_OR_RETURN(Value, test_value,
                     test_->TryEvaluate(row, schema, context));
    if (mode_ != QuantifierMode::kIn) {
      return TryEvaluateQuantifiedComparison(op_, mode_, test_value, values);
    }
    bool found = false;
    bool saw_null = test_value.IsNull();
    for (const Value& candidate : values) {
      if (found) {
        break;
      }
      if (HaveConflictingCollations(test_value, candidate)) {
        return StatusError(StatusCode::kInvalidArgument,
                           "Collation conflict between the IN operands");
      }
      saw_null = saw_null || candidate.IsNull();
      if (!found && !test_value.IsNull() && !candidate.IsNull()) {
        ASSIGN_OR_RETURN(
            Value, equal,
            TryEvaluateBinary(BinaryOperation::kEquals, test_value, candidate));
        if (equal.IsNull()) {
          saw_null = true;
        } else if (equal.Truthy()) {
          found = true;
        }
      }
    }
    Value membership =
        found ? Value(true) : (saw_null ? Value() : Value(false));
    if (!negated_) {
      return membership;
    }
    return membership.IsNull() ? Value() : Value(!membership.Truthy());
  }
  if (values.empty()) {
    return Value();
  }
  return values.front();
}

Value QueryExpression::Evaluate(const Row& row, const Schema& schema,
                                EvaluationContext& context) const {
  return ExcShimUnwrap(TryEvaluate(row, schema, context),
                       "QueryExpression::Evaluate");
}

// EXC-SHIM: deprecated throwing wrapper (common/exc_shim.hpp).
Value EvaluateQuantifiedComparison(BinaryOperation op, QuantifierMode mode,
                                   const Value& test,
                                   const std::vector<Value>& rows) {
  return ExcShimUnwrap(TryEvaluateQuantifiedComparison(op, mode, test, rows),
                       "EvaluateQuantifiedComparison");
}

std::unordered_set<ColumnName> QueryExpression::TouchedColumns() const {
  return test_ ? test_->TouchedColumns() : std::unordered_set<ColumnName>{};
}

std::string QueryExpression::ToString() const {
  if (exists_) {
    return negated_ ? "NOT EXISTS(...)" : "EXISTS(...)";
  }
  if (test_) {
    if (mode_ == QuantifierMode::kAny || mode_ == QuantifierMode::kAll) {
      return test_->ToString() + " " + std::string(tinylamb::ToString(op_)) +
             (mode_ == QuantifierMode::kAny ? " ANY(...)" : " ALL(...)");
    }
    return test_->ToString() + (negated_ ? " NOT IN(...)" : " IN(...)");
  }
  return "SCALAR_SUBQUERY(...)";
}

void QueryExpression::Dump(std::ostream& output) const { output << ToString(); }

}  // namespace tinylamb
