/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "expression/aggregate_expression.hpp"

#include <algorithm>
#include <cstddef>
#include <ostream>
#include <stdexcept>
#include <string>
#include <unordered_set>

#include "expression/expression.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"

namespace tinylamb {

std::unordered_set<ColumnName> AggregateExpression::TouchedColumns() const {
  // COUNT(*) carries no child at all; dereferencing it is a hard crash
  // (rule_fuzzer: SEGV inside Cascades rule application).
  std::unordered_set<ColumnName> columns =
      child_ ? child_->TouchedColumns() : std::unordered_set<ColumnName>{};
  if (having_cond_) {
    columns.merge(having_cond_->TouchedColumns());
  }
  if (where_filter_) {
    columns.merge(where_filter_->TouchedColumns());
  }
  for (const auto& term : inner_order_by_) {
    if (term.expression) {
      columns.merge(term.expression->TouchedColumns());
    }
  }
  if (secondary_arg_) {
    columns.merge(secondary_arg_->TouchedColumns());
  }
  for (const Expression& argument : trailing_args_) {
    if (argument) {
      columns.merge(argument->TouchedColumns());
    }
  }
  for (const Expression& key : inner_group_by_) {
    if (key) {
      columns.merge(key->TouchedColumns());
    }
  }
  return columns;
}

StatusOr<Value> AggregateExpression::TryEvaluate(
    const Row& /*row*/, const Schema& /*schema*/) const {
  // This method must not be called directly: the value of an aggregate
  // expression is calculated by the aggregator.
  return StatusError(StatusCode::kRuntimeError,
                     "aggregate expression cannot be evaluated directly");
}

Value AggregateExpression::Evaluate(const Row& row,
                                    const Schema& schema) const {
  return ExcShimUnwrap(TryEvaluate(row, schema),
                       "AggregateExpression::Evaluate");
}

Type AggregateExpression::ResultType(const Schema& schema) const {
  // P461/501: canonical declaration table (type/value.cpp); std::nullopt
  // means the result carries the child's type.
  if (const auto kind = CanonicalAggregateResultKind(type_); kind.has_value()) {
    switch (*kind) {
      case ValueType::kInt64:
        return {TypeTag::kBigInt};
      case ValueType::kDouble:
        return {TypeTag::kDouble};
      case ValueType::kVarChar:
        return {TypeTag::kVarChar};
      case ValueType::kArray:
        return {TypeTag::kArray};
      default:
        break;
    }
  }
  if (!child_) {
    // Only the INT64-declared aggregates above are valid without a column
    // child; others derive their type from the child and must not silently
    // claim INT64 (handled by the canonical table).
    return {TypeTag::kBigInt};
  }
  return child_->ResultType(schema);
}

Type AggregateExpression::ResultType(const Schema& left,
                                     const Schema& right) const {
  if (const auto kind = CanonicalAggregateResultKind(type_); kind.has_value()) {
    switch (*kind) {
      case ValueType::kInt64:
        return {TypeTag::kBigInt};
      case ValueType::kDouble:
        return {TypeTag::kDouble};
      case ValueType::kVarChar:
        return {TypeTag::kVarChar};
      case ValueType::kArray:
        return {TypeTag::kArray};
      default:
        break;
    }
  }
  if (!child_) {
    return {TypeTag::kBigInt};
  }
  return child_->ResultType(left, right);
}

std::string AggregateExpression::ToString() const {
  std::string out = ::tinylamb::ToString(type_) + "(" +
                    (distinct_ ? "DISTINCT " : "") +
                    (child_ ? child_->ToString() : "*");
  for (const Expression& argument : trailing_args_) {
    if (argument) {
      out += ", " + argument->ToString();
    }
  }
  if (secondary_arg_) {
    // Quote a string delimiter so the rendered SQL is re-parseable.
    std::string delim = secondary_arg_->ToString();
    if (secondary_arg_->Type() == TypeTag::kConstantValue) {
      std::string quoted = "\"";
      quoted.reserve((delim.size() * 2) + 2);
      for (char c : delim) {
        if (c == '"' || c == '\\') {
          quoted += '\\';  // NOLINT(readability-braces-around-statements)
        }
        quoted += c;
      }
      quoted += '"';
      delim = std::move(quoted);
    }
    out += ", " + delim;
  }
  if (ignore_nulls_) {
    out += " IGNORE NULLS";
  }
  if (where_filter_) {
    out += " WHERE " + where_filter_->ToString();
  }
  // A hand-built tree may carry a modifier before its condition is attached.
  // Do not emit a dangling HAVING clause for that incomplete node.
  if (having_ != AggregateHavingModifier::kNone && having_cond_) {
    out += having_ == AggregateHavingModifier::kMax ? " HAVING MAX "
                                                    : " HAVING MIN ";
    out += having_cond_->ToString();
  }
  if (inner_limit_.has_value()) {
    out += " LIMIT " + std::to_string(*inner_limit_);
  }
  // T4808: inner GROUP BY was invisible in the text form, so two aggregates
  // differing only by GROUP BY rendered identically -- the ToString-based
  // dedup/rewrite keys the reader contract (round 155 CLEAN) promises to
  // include it. Rendered before ORDER BY, matching GoogleSQL syntax.
  // TouchedColumns treats a null inner GROUP BY / ORDER BY expression as a
  // valid (optional) state (#2484). ToString walks the same vectors, so it
  // must guard the deref too. Render a placeholder for null items instead of
  // skipping them: ToString is the dedup/rewrite key, so "[nullptr, col]" must
  // not collapse onto the same text as a single-item "[col]".
  const bool has_group_by =
      std::any_of(inner_group_by_.begin(), inner_group_by_.end(),
                  [](const Expression& key) { return static_cast<bool>(key); });
  if (has_group_by) {
    out += " GROUP BY ";
    bool first = true;
    for (const Expression& key : inner_group_by_) {
      if (!first) {
        out += ", ";
      }
      out += key ? key->ToString() : "<null>";
      first = false;
    }
  }
  const bool has_order_by =
      std::any_of(inner_order_by_.begin(), inner_order_by_.end(),
                  [](const WindowOrderTerm& term) {
                    return static_cast<bool>(term.expression);
                  });
  if (has_order_by) {
    out += " ORDER BY ";
    bool first = true;
    for (const WindowOrderTerm& term : inner_order_by_) {
      if (!first) {
        out += ", ";
      }
      out += term.expression ? term.expression->ToString() : "<null>";
      if (term.expression) {
        if (!term.ascending) {
          out += " DESC";
        }
        if (term.nulls_first.has_value()) {
          out +=
              term.nulls_first.value()
                  ? " NULLS FIRST"  // NOLINT(bugprone-unchecked-optional-access)
                  : " NULLS LAST";
        }
      }
      first = false;
    }
  }
  return out + ")";
}

void AggregateExpression::Dump(std::ostream& o) const { o << ToString(); }

}  // namespace tinylamb
