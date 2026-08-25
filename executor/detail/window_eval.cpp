/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/window_eval.hpp"

#include <algorithm>
#include <cmath>
#include <map>
#include <memory>
#include <numeric>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "executor/detail/expression_eval.hpp"
#include "database/transaction_context.hpp"
#include "expression/array_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/unary_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "type/column.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
namespace relational_detail {
namespace {

using WindowNodeMap =
    std::map<const WindowFunctionCallExpression*, size_t>;  // sorted: stable $win naming

const WindowFunctionCallExpression* AsWindow(const Expression& expression) {
  if (expression && expression->Type() == TypeTag::kWindowFunctionExp) {
    return static_cast<const WindowFunctionCallExpression*>(expression.get());
  }
  return nullptr;
}

void CollectWindows(const Expression& expression, WindowNodeMap* found) {
  if (!expression) { return; }
  if (const auto* window = AsWindow(expression)) {
    if (!found->contains(window)) { (*found)[window] = found->size(); }
    return;  // nested OVER calls are invalid SQL; do not descend
  }
  switch (expression->Type()) {
    case TypeTag::kBinaryExp:
      CollectWindows(expression->AsBinaryExpression().Left(), found);
      CollectWindows(expression->AsBinaryExpression().Right(), found);
      break;
    case TypeTag::kUnaryExp:
      CollectWindows(expression->AsUnaryExpression().Child(), found);
      break;
    case TypeTag::kCaseExp: {
      const auto& value = expression->AsCaseExpression();
      for (const auto& [condition, result] : value.when_clauses_) {
        CollectWindows(condition, found);
        CollectWindows(result, found);
      }
      CollectWindows(value.else_clause_, found);
      break;
    }
    case TypeTag::kInExp: {
      const auto& value = expression->AsInExpression();
      CollectWindows(value.child_, found);
      for (const Expression& item : value.list_) { CollectWindows(item, found); }
      break;
    }
    case TypeTag::kFunctionCallExp:
      for (const Expression& argument :
           expression->AsFunctionCallExpression().Args()) {
        CollectWindows(argument, found);
      }
      break;
    case TypeTag::kArrayExp:
      for (const Expression& element :
           expression->AsArrayExpression().Elements()) {
        CollectWindows(element, found);
      }
      break;
    case TypeTag::kCastExp:
      CollectWindows(expression->AsCastExpression().Child(), found);
      break;
    default:
      break;
  }
}

bool ContainsWindow(const Expression& expression) {
  WindowNodeMap found;
  CollectWindows(expression, &found);
  return !found.empty();
}

using ReplacementMap =
    std::unordered_map<const WindowFunctionCallExpression*, Expression>;

Expression Rebuild(const Expression& expression, const ReplacementMap& map);

std::vector<Expression> RebuildAll(const std::vector<Expression>& items,
                                   const ReplacementMap& map) {
  std::vector<Expression> out;
  out.reserve(items.size());
  for (const Expression& item : items) { out.push_back(Rebuild(item, map)); }
  return out;
}

Expression Rebuild(const Expression& expression, const ReplacementMap& map) {
  if (!expression) { return expression; }
  if (const auto* window = AsWindow(expression)) {
    const auto found = map.find(window);
    return found != map.end() ? found->second : expression;
  }
  switch (expression->Type()) {
    case TypeTag::kBinaryExp:
      return BinaryExpressionExp(
          Rebuild(expression->AsBinaryExpression().Left(), map),
          expression->AsBinaryExpression().Op(),
          Rebuild(expression->AsBinaryExpression().Right(), map));
    case TypeTag::kUnaryExp:
      return UnaryExpressionExp(
          Rebuild(expression->AsUnaryExpression().Child(), map),
          expression->AsUnaryExpression().Op());
    case TypeTag::kCaseExp: {
      const auto& value = expression->AsCaseExpression();
      std::vector<std::pair<Expression, Expression>> clauses;
      for (const auto& [condition, result] : value.when_clauses_) {
        clauses.emplace_back(Rebuild(condition, map), Rebuild(result, map));
      }
      return CaseExpressionExp(std::move(clauses),
                               Rebuild(value.else_clause_, map));
    }
    case TypeTag::kInExp: {
      const auto& value = expression->AsInExpression();
      std::vector<Expression> items;
      items.reserve(value.list_.size());
      for (const Expression& item : value.list_) {
        items.push_back(Rebuild(item, map));
      }
      return InExpressionExp(Rebuild(value.child_, map), std::move(items));
    }
    case TypeTag::kFunctionCallExp:
      return FunctionCallExp(
          expression->AsFunctionCallExpression().FuncName(),
          RebuildAll(expression->AsFunctionCallExpression().Args(), map));
    case TypeTag::kArrayExp:
      return ArrayExpressionExp(
          RebuildAll(expression->AsArrayExpression().Elements(), map),
          expression->AsArrayExpression().ElementSqlType());
    case TypeTag::kCastExp:
      return CastExpressionExp(
          Rebuild(expression->AsCastExpression().Child(), map),
          expression->AsCastExpression().TargetTypeName(),
          expression->AsCastExpression().ReturnNullOnError());
    default:
      return expression;
  }
}

std::vector<Expression> RebuildAll(const std::vector<Expression>& items,
                                   const ReplacementMap& map);

// NULLS FIRST on ASC / NULLS LAST on DESC by default, matching ApplyOrderBy;
// an explicit NULLS FIRST/LAST overrides the default.
bool ValueLess(const Value& a, const Value& b, bool ascending,
               const std::optional<bool>& nulls_first = std::nullopt) {
  const bool nulls_first_value = nulls_first.value_or(ascending);
  if (a.IsNull() && b.IsNull()) { return false; }
  if (a.IsNull()) { return nulls_first_value; }
  if (b.IsNull()) { return !nulls_first_value; }
  try {
    return ascending ? a < b : b < a;
  } catch (...) {
    return false;
  }
}

bool ValuesEqual(const Value& a, const Value& b) {
  if (a.IsNull() || b.IsNull()) { return a.IsNull() && b.IsNull(); }
  try {
    return a == b;
  } catch (...) {
    return false;
  }
}

double NumericOf(const Value& value) {
  return value.type == ValueType::kDouble ? value.value.double_value
                                         : static_cast<double>(value.value.int_value);
}

std::string ElementSqlTypeOf(const Value& value) {
  switch (value.type) {
    case ValueType::kInt64:
      return "INT64";
    case ValueType::kDouble:
      return "FLOAT64";
    case ValueType::kVarChar:
      return "STRING";
    case ValueType::kDate:
      return "DATE";
    default:
      return {};
  }
}

struct WindowRuntime {
  TransactionContext& context;
  const Scope* outer{nullptr};
  const CteMap* ctes{nullptr};
  const Schema* schema{nullptr};

  Value EvalAt(const Expression& expression, const std::vector<Row>& rows,
               size_t position) const {
    Scope scope{.row=&rows[position], .schema=schema, .outer=outer};
    return Evaluate(expression, scope, nullptr, context, *ctes);
  }

  // Resolves [lo, hi] (inclusive) frame bounds within the ordered partition.
  std::pair<size_t, size_t> ResolveFrame(
      const WindowFunctionCallExpression& window,
      const std::vector<Row>& rows, const std::vector<size_t>& ordered,
      const std::vector<std::vector<Value>>& order_values, size_t position,
      const std::vector<size_t>& peer_end) const {
    const size_t m = ordered.size();
    if (!window.has_frame) {
      if (window.order_by.empty()) { return {0, m - 1}; }
      // Default frame: RANGE UNBOUNDED PRECEDING .. CURRENT ROW.
      return {0, peer_end[position]};
    }
    if (window.frame_unit == WindowFrameUnit::kGroups) {
      if (window.order_by.empty()) {
        throw std::runtime_error("GROUPS frame requires an ORDER BY key");
      }
      std::vector<size_t> group_start;
      std::vector<size_t> group_end;
      for (size_t i = 0; i < m;) {
        const size_t end = peer_end[i];
        group_start.push_back(i);
        group_end.push_back(end);
        i = end + 1;
      }
      size_t current_group = 0;
      while (group_end[current_group] < position) { ++current_group; }
      const size_t group_count = group_start.size();
      auto offset_of = [](const WindowFrameBound& bound) -> size_t {
        if (!bound.offset || bound.offset->Type() != TypeTag::kConstantValue) {
          throw std::runtime_error("GROUPS offset requires a constant value");
        }
        const double value = NumericOf(bound.offset->AsConstantValue().GetValue());
        if (value < 0) { throw std::runtime_error("GROUPS offset is negative"); }
        return static_cast<size_t>(value);
      };
      auto group_for_preceding = [&](size_t offset) {
        return offset > current_group ? size_t{0} : current_group - offset;
      };
      auto group_for_following = [&](size_t offset) {
        return std::min(current_group + offset, group_count - 1);
      };
      auto groups_start = [&](const WindowFrameBound& bound) {
        switch (bound.type) {
          case WindowFrameBoundType::kUnboundedPreceding: return size_t{0};
          case WindowFrameBoundType::kOffsetPreceding:
            return group_start[group_for_preceding(offset_of(bound))];
          case WindowFrameBoundType::kCurrentRow: return group_start[current_group];
          case WindowFrameBoundType::kOffsetFollowing:
            return group_start[group_for_following(offset_of(bound))];
          case WindowFrameBoundType::kUnboundedFollowing: return m - 1;
        }
        return size_t{0};
      };
      auto groups_end = [&](const WindowFrameBound& bound) {
        switch (bound.type) {
          case WindowFrameBoundType::kUnboundedPreceding: return size_t{0};
          case WindowFrameBoundType::kOffsetPreceding:
            return group_end[group_for_preceding(offset_of(bound))];
          case WindowFrameBoundType::kCurrentRow: return group_end[current_group];
          case WindowFrameBoundType::kOffsetFollowing:
            return group_end[group_for_following(offset_of(bound))];
          case WindowFrameBoundType::kUnboundedFollowing: return m - 1;
        }
        return size_t{0};
      };
      const size_t lo = groups_start(window.frame_start);
      const size_t hi = groups_end(window.frame_end);
      return lo > hi ? std::pair<size_t, size_t>{1, 0}
                     : std::pair<size_t, size_t>{lo, hi};
    }
    auto start_of = [&](const WindowFrameBound& bound) -> std::optional<size_t> {
      switch (bound.type) {
        case WindowFrameBoundType::kUnboundedPreceding:
          return 0;
        case WindowFrameBoundType::kCurrentRow:
          return position;
        case WindowFrameBoundType::kOffsetPreceding: {
          if (window.order_by.size() != 1 || !bound.offset ||
              bound.offset->Type() != TypeTag::kConstantValue) {
            throw std::runtime_error("RANGE offset requires one constant key");
          }
          const double off =
              NumericOf(bound.offset->AsConstantValue().GetValue());
          const Value& key = order_values[position][0];
          if (key.IsNull()) { return position; }
          for (size_t j = 0; j <= position; ++j) {
            const Value& candidate = order_values[j][0];
            if (candidate.IsNull()) { continue; }
            const double distance =
                NumericOf(key) - NumericOf(candidate);
            if (distance <= off + 1e-9) { return j; }
          }
          return position;
        }
        case WindowFrameBoundType::kOffsetFollowing:
        case WindowFrameBoundType::kUnboundedFollowing:
          return m - 1;
      }
      return m - 1;
    };
    auto end_of = [&](const WindowFrameBound& bound) -> std::optional<size_t> {
      switch (bound.type) {
        case WindowFrameBoundType::kUnboundedFollowing:
          return m - 1;
        case WindowFrameBoundType::kCurrentRow:
          return peer_end[position];
        case WindowFrameBoundType::kOffsetFollowing: {
          if (window.order_by.size() != 1 || !bound.offset ||
              bound.offset->Type() != TypeTag::kConstantValue) {
            throw std::runtime_error("RANGE offset requires one constant key");
          }
          const double off =
              NumericOf(bound.offset->AsConstantValue().GetValue());
          const Value& key = order_values[position][0];
          if (key.IsNull()) { return peer_end[position]; }
          std::optional<size_t> reached;
          for (size_t j = 0; j < m; ++j) {
            const Value& candidate = order_values[j][0];
            if (candidate.IsNull()) { continue; }
            const double distance =
                NumericOf(candidate) - NumericOf(key);
            if (distance <= off + 1e-9) { reached = j; }
          }
          return reached.value_or(peer_end[position]);
        }
        case WindowFrameBoundType::kUnboundedPreceding:
          return peer_end[position];
        case WindowFrameBoundType::kOffsetPreceding: {
          if (window.order_by.size() != 1 || !bound.offset ||
              bound.offset->Type() != TypeTag::kConstantValue) {
            throw std::runtime_error("RANGE offset requires one constant key");
          }
          const double off =
              NumericOf(bound.offset->AsConstantValue().GetValue());
          const Value& key = order_values[position][0];
          if (key.IsNull()) { return peer_end[position]; }
          std::optional<size_t> reached;
          for (size_t j = 0; j <= position; ++j) {
            const Value& candidate = order_values[j][0];
            if (candidate.IsNull()) { continue; }
            if (NumericOf(key) - NumericOf(candidate) >= off - 1e-9) {
              reached = j;
            }
          }
          return reached.value_or(peer_end[position]);
        }
      }
      return peer_end[position];
    };

    const auto lo = start_of(window.frame_start);
    const auto hi = end_of(window.frame_end);
    if (!lo.has_value() || !hi.has_value() || *lo > *hi) {
      return {1, 0};  // empty frame
    }
    return {*lo, *hi};
  }

  Value AggregateOverFrame(const WindowFunctionCallExpression& window,
                           const std::vector<Row>& rows,
                           const std::vector<size_t>& ordered,
                           const std::vector<std::vector<Value>>& order_values,
                           size_t current, size_t lo, size_t hi) const {
    const std::string& fn = window.function;
    if (hi < lo || ordered.empty() || hi >= ordered.size()) {
      if (fn == "COUNT") { return Value(static_cast<int64_t>(0)); }
      return Value();
    }
    // The window expression carries an optional row-level WHERE filter.
    const Expression& where_filter = window.where_filter;

    int64_t row_count = 0;
    bool count_star = fn == "COUNT" && window.args.size() == 1 &&
                      window.args[0]->Type() == TypeTag::kColumnValue &&
                      window.args[0]->AsColumnValue().GetColumnName().name == "*";
    std::vector<Value> values;
    Value delimiter;
    for (size_t p = lo; p <= hi; ++p) {
      bool excluded = false;
      switch (window.exclusion) {
        case WindowFrameExclusion::kCurrentRow:
          excluded = p == current;
          break;
        case WindowFrameExclusion::kGroup: {
          if (window.order_by.empty()) {
            excluded = true;
          } else if (order_values.empty() || order_values[current].empty()) {
            excluded = p == current;
          } else {
            excluded = order_values[p].size() == order_values[current].size();
            for (size_t t = 0; excluded && t < order_values[current].size();
                 ++t) {
              excluded = ValuesEqual(order_values[p][t],
                                    order_values[current][t]);
            }
          }
          break;
        }
        case WindowFrameExclusion::kTies: {
          if (window.order_by.empty()) {
            excluded = p != current;
          } else if (order_values.empty() || order_values[current].empty()) {
            excluded = false;
          } else {
            excluded = p != current &&
                       order_values[p].size() == order_values[current].size();
            for (size_t t = 0; excluded && t < order_values[current].size();
                 ++t) {
              excluded = ValuesEqual(order_values[p][t],
                                    order_values[current][t]);
            }
          }
          break;
        }
        case WindowFrameExclusion::kNone:
          break;
      }
      if (excluded) { continue; }
      const Row& row = rows[ordered[p]];
      Scope scope{.row=&row, .schema=schema, .outer=outer};
      // AGG(x WHERE cond) OVER (...): skip rows failing the filter.
      if (window.where_filter) {
        Value keep;
        try {
          keep = Evaluate(window.where_filter, scope, nullptr, context, *ctes);
        } catch (...) {
          keep = Value();
        }
        if (!Truthy(keep)) { continue; }
      }
      ++row_count;
      if (count_star) { continue; }
      if (!window.args.empty()) {
        values.push_back(Evaluate(window.args[0], scope, nullptr, context, *ctes));
      }
      if (fn == "STRING_AGG" && window.args.size() > 1) {
        delimiter = Evaluate(window.args[1], scope, nullptr, context, *ctes);
      }
    }

    if (window.distinct) {
      std::vector<Value> unique;
      for (Value& candidate : values) {
        bool duplicate = false;
        for (const Value& kept : unique) {
          if (ValuesEqual(candidate, kept)) { duplicate = true; break; }
        }
        if (!duplicate) { unique.push_back(std::move(candidate)); }
      }
      values = std::move(unique);
    }

    std::vector<Value> non_null;
    non_null.reserve(values.size());
    for (Value& value : values) {
      if (!value.IsNull()) { non_null.push_back(std::move(value)); }
    }

    if (fn == "COUNT") {
      return Value(count_star ? row_count : static_cast<int64_t>(non_null.size()));
    }
    if (fn == "APPROX_COUNT_DISTINCT") {
      std::vector<Value> unique;
      for (const Value& candidate : non_null) {
        bool duplicate = false;
        for (const Value& kept : unique) {
          if (ValuesEqual(candidate, kept)) { duplicate = true; break; }
        }
        if (!duplicate) { unique.push_back(candidate); }
      }
      return Value(static_cast<int64_t>(unique.size()));
    }
    if (fn == "ARRAY_CONCAT_AGG") {
      // Concatenates ARRAY elements across the frame rows.
      std::vector<Value> merged;
      std::string element_type;
      for (const Value& array : non_null) {
        if (array.IsNull() || !array.IsArray()) { continue; }
        for (const Value& element : array.ArrayElements()) {
          if (element_type.empty() && !element.IsNull()) {
            element_type = ElementSqlTypeOf(element);
          }
          merged.push_back(element);
        }
      }
      return Value::Array(std::move(merged), element_type);
    }
    if (fn == "APPROX_QUANTILES") {
      // Exact percentile interpolation over the frame; returns NUM quantiles
      // including both endpoints.
      if (non_null.empty() || window.args.size() < 2) { return Value(); }
      Value n_value = Evaluate(window.args[1], Scope{.row=&rows[ordered[lo]],
                                                    .schema=schema, .outer=outer},
                               nullptr, context, *ctes);
      const int64_t num = n_value.value.int_value;
      if (num <= 0) { return Value(); }
      std::vector<Value> sorted = non_null;
      std::sort(sorted.begin(), sorted.end(), [](const Value& a, const Value& b) {
        try {
          if (a.IsNull() || b.IsNull()) { return false; }
          return a < b;
        } catch (...) { return false; }
      });
      // APPROX_QUANTILES returns NUM+1 percentiles including both endpoints.
      std::vector<Value> quantiles;
      for (int64_t i = 0; i <= num; ++i) {
        const int64_t idx = i * static_cast<int64_t>(sorted.size() - 1) / num;
        quantiles.push_back(sorted[static_cast<size_t>(idx)]);
      }
      return Value::Array(std::move(quantiles), ElementSqlTypeOf(sorted[0]));
    }
    if (fn == "COUNTIF") {
      int64_t trues = 0;
      for (const Value& value : non_null) {
        if (Truthy(value)) { ++trues; }
      }
      return Value(trues);
    }
    if (non_null.empty()) { return Value(); }

    if (fn == "SUM" || fn == "AVG") {
      bool any_double = false;
      for (const Value& value : non_null) {
        if (value.type == ValueType::kDouble) { any_double = true; break; }
      }
      if (any_double) {
        double total = 0.0;
        for (const Value& value : non_null) { total += NumericOf(value); }
        return Value(total);
      }
      uint64_t total = 0;
      for (const Value& value : non_null) {
        total += static_cast<uint64_t>(value.value.int_value);
      }
      if (fn == "AVG") {
        return Value(static_cast<double>(total) /
                     static_cast<double>(non_null.size()));
      }
      return Value(static_cast<int64_t>(total));
    }
    if (fn == "MIN" || fn == "MAX") {
      const Value* best = &non_null[0];
      for (const Value& value : non_null) {
        if (fn == "MIN" ? ValueLess(value, *best, true)
                        : ValueLess(*best, value, true)) {
          best = &value;
        }
      }
      return *best;
    }
    if (fn == "ARRAY_AGG" || fn == "STRING_AGG") {
      std::vector<Value> final_values = std::move(non_null);
      if (window.inner_limit.has_value() &&
          final_values.size() > *window.inner_limit) {
        final_values.resize(*window.inner_limit);
      }
      if (fn == "STRING_AGG") {
        std::string sep = delimiter.IsNull() ? "," : delimiter.AsString();
        std::string out;
        for (size_t i = 0; i < final_values.size(); ++i) {
          if (i) { out += sep; }
          out += final_values[i].AsString();
        }
        return Value(std::move(out));
      }
      std::string element_type;
      for (const Value& value : final_values) {
        if (!value.IsNull()) {
          element_type = ElementSqlTypeOf(value);
          break;
        }
      }
      return Value::Array(std::move(final_values), element_type);
    }
    if (fn == "LOGICAL_AND" || fn == "LOGICAL_OR") {
      bool saw_true = false;
      bool saw_false = false;
      for (const Value& value : non_null) {
        if (Truthy(value)) { saw_true = true; } else { saw_false = true; }
      }
      if (fn == "LOGICAL_AND") {
        if (saw_false) { return Value(static_cast<int64_t>(0)); }
        return Value(static_cast<int64_t>(1));
      }
      if (saw_true) { return Value(static_cast<int64_t>(1)); }
      return Value(static_cast<int64_t>(0));
    }
    if (fn == "BIT_AND" || fn == "BIT_OR" || fn == "BIT_XOR") {
      int64_t acc = non_null[0].value.int_value;
      for (size_t i = 1; i < non_null.size(); ++i) {
        const int64_t next = non_null[i].value.int_value;
        if (fn == "BIT_AND") {
          acc &= next;
        } else if (fn == "BIT_OR") {
          acc |= next;
        } else {
          acc ^= next;
        }
      }
      return Value(acc);
    }
    throw std::runtime_error("unsupported window aggregate " + fn);
  }
};

struct WindowPartitionLayout {
  std::vector<size_t> ordered;
  std::vector<std::vector<Value>> order_values;
  std::vector<size_t> peer_end;
};

struct WindowOrderLayout {
  std::vector<WindowPartitionLayout> partitions;
};

std::string WindowLayoutKey(const WindowFunctionCallExpression& window) {
  std::string key;
  key.reserve(128);
  key.append("P:");
  for (const Expression& expression : window.partition_by) {
    key.append(expression ? expression->ToString() : "(null)");
    key.push_back('|');
  }
  key.append("O:");
  for (const WindowOrderTerm& term : window.order_by) {
    key.append(term.expression ? term.expression->ToString() : "(null)");
    key.push_back(term.ascending ? 'A' : 'D');
    if (term.nulls_first.has_value()) {
      key.push_back(*term.nulls_first ? 'F' : 'L');
    } else {
      key.push_back('D');
    }
    key.push_back('|');
  }
  return key;
}

WindowOrderLayout BuildWindowOrderLayout(
    const WindowFunctionCallExpression& window, std::vector<Row>& rows,
    const WindowRuntime& runtime) {
  WindowOrderLayout layout;
  std::vector<std::vector<Value>> partition_keys;
  for (size_t i = 0; i < rows.size(); ++i) {
    std::vector<Value> keys;
    keys.reserve(window.partition_by.size());
    for (const Expression& key : window.partition_by) {
      keys.push_back(runtime.EvalAt(key, rows, i));
    }
    size_t group = layout.partitions.size();
    for (size_t g = 0; g < partition_keys.size(); ++g) {
      bool equal = partition_keys[g].size() == keys.size();
      for (size_t t = 0; equal && t < keys.size(); ++t) {
        if (!ValuesEqual(partition_keys[g][t], keys[t])) { equal = false; }
      }
      if (equal) {
        group = g;
        break;
      }
    }
    if (group == layout.partitions.size()) {
      layout.partitions.emplace_back();
      partition_keys.push_back(std::move(keys));
    }
    layout.partitions[group].ordered.push_back(i);
  }

  for (WindowPartitionLayout& partition : layout.partitions) {
    partition.order_values.resize(partition.ordered.size());
    if (!window.order_by.empty()) {
      for (size_t k = 0; k < partition.ordered.size(); ++k) {
        auto& values = partition.order_values[k];
        values.reserve(window.order_by.size());
        for (const WindowOrderTerm& term : window.order_by) {
          values.push_back(
              runtime.EvalAt(term.expression, rows, partition.ordered[k]));
        }
      }
      std::vector<size_t> positions(partition.ordered.size());
      std::iota(positions.begin(), positions.end(), 0);
      std::stable_sort(positions.begin(), positions.end(),
                       [&](size_t a, size_t b) {
                         for (size_t t = 0; t < window.order_by.size(); ++t) {
                           const WindowOrderTerm& term = window.order_by[t];
                           if (ValuesEqual(partition.order_values[a][t],
                                           partition.order_values[b][t])) {
                             continue;
                           }
                           return ValueLess(partition.order_values[a][t],
                                            partition.order_values[b][t],
                                            term.ascending, term.nulls_first);
                         }
                         return false;
                       });
      std::vector<size_t> sorted_members(partition.ordered.size());
      std::vector<std::vector<Value>> sorted_values(partition.ordered.size());
      for (size_t k = 0; k < positions.size(); ++k) {
        sorted_members[k] = partition.ordered[positions[k]];
        sorted_values[k] = std::move(partition.order_values[positions[k]]);
      }
      partition.ordered = std::move(sorted_members);
      partition.order_values = std::move(sorted_values);
    }

    const size_t m = partition.ordered.size();
    partition.peer_end.resize(m);
    for (size_t a = 0; a < m; ++a) {
      size_t end = a;
      while (end + 1 < m) {
        bool equal = true;
        for (size_t t = 0; t < partition.order_values[a].size(); ++t) {
          if (!ValuesEqual(partition.order_values[a][t],
                           partition.order_values[end + 1][t])) {
            equal = false;
            break;
          }
        }
        if (!equal) { break; }
        ++end;
      }
      partition.peer_end[a] = end;
    }
  }
  return layout;
}

void ComputeOneWindow(TransactionContext& context,
                      const WindowFunctionCallExpression& window,
                      std::vector<Row>& rows, const Schema& schema,
                      const Scope* outer, const CteMap& ctes,
                      std::vector<Value>* out,
                      const WindowOrderLayout* cached_layout = nullptr) {
  const size_t n = rows.size();
  out->assign(n, Value());
  if (n == 0) { return; }

  WindowRuntime runtime{context, outer, &ctes, &schema};

  WindowOrderLayout local_layout;
  const WindowOrderLayout* layout = cached_layout;
  if (layout == nullptr) {
    local_layout = BuildWindowOrderLayout(window, rows, runtime);
    layout = &local_layout;
  }

  for (const WindowPartitionLayout& partition : layout->partitions) {
    const std::vector<size_t>& ordered = partition.ordered;
    const std::vector<std::vector<Value>>& order_values =
        partition.order_values;
    const std::vector<size_t>& peer_end = partition.peer_end;
    const size_t m = ordered.size();

    const std::string& fn = window.function;

    if (fn == "ROW_NUMBER") {
      for (size_t k = 0; k < m; ++k) {
        (*out)[ordered[k]] = Value(static_cast<int64_t>(k + 1));
      }
      continue;
    }
    if (fn == "RANK" || fn == "DENSE_RANK" || fn == "PERCENT_RANK" ||
        fn == "CUME_DIST") {
      int64_t dense = 0;
      size_t k = 0;
      while (k < m) {
        const size_t end = peer_end[k];
        ++dense;
        const int64_t rank = static_cast<int64_t>(k + 1);
        for (size_t p = k; p <= end; ++p) {
          if (fn == "RANK") {
            (*out)[ordered[p]] = Value(rank);
          } else if (fn == "DENSE_RANK") {
            (*out)[ordered[p]] = Value(dense);
          } else if (fn == "PERCENT_RANK") {
            const double denominator = m > 1 ? static_cast<double>(m - 1) : 1.0;
            (*out)[ordered[p]] =
                Value((static_cast<double>(rank) - 1.0) / denominator);
          } else {
            (*out)[ordered[p]] =
                Value(static_cast<double>(end + 1) / static_cast<double>(m));
          }
        }
        k = end + 1;
      }
      continue;
    }
    if (fn == "NTILE") {
      if (window.args.empty()) {
        throw std::runtime_error("NTILE requires an argument");
      }
      const Value buckets_value =
          runtime.EvalAt(window.args[0], rows, ordered[0]);
      const int64_t buckets = buckets_value.value.int_value;
      if (buckets <= 0) {
        throw std::runtime_error("NTILE requires a positive bucket count");
      }
      const int64_t base = static_cast<int64_t>(m) / buckets;
      const int64_t extra = static_cast<int64_t>(m) % buckets;
      size_t cursor = 0;
      for (int64_t b = 0; b < buckets && cursor < m; ++b) {
        const int64_t size = base + (b < extra ? 1 : 0);
        for (int64_t r = 0; r < size && cursor < m; ++r) {
          (*out)[ordered[cursor++]] = Value(b + 1);
        }
      }
      continue;
    }
    if (fn == "LAG" || fn == "LEAD") {
      int64_t offset = 1;
      if (window.args.size() > 1) {
        const Value offset_value =
            runtime.EvalAt(window.args[1], rows, ordered[0]);
        offset = offset_value.IsNull() ? 1 : offset_value.value.int_value;
      }
      for (size_t k = 0; k < m; ++k) {
        const int64_t target = fn == "LAG"
                                   ? static_cast<int64_t>(k) - offset
                                   : static_cast<int64_t>(k) + offset;
        if (target < 0 || target >= static_cast<int64_t>(m)) {
          if (window.args.size() > 2) {
            (*out)[ordered[k]] = runtime.EvalAt(window.args[2], rows, ordered[k]);
          }
          continue;
        }
        (*out)[ordered[k]] =
            runtime.EvalAt(window.args[0], rows, ordered[target]);
      }
      continue;
    }
    if (fn == "FIRST_VALUE" || fn == "LAST_VALUE" || fn == "NTH_VALUE") {
      for (size_t k = 0; k < m; ++k) {
        const auto [lo, hi] =
            runtime.ResolveFrame(window, rows, ordered, order_values, k,
                                 peer_end);
        if (hi < lo) { continue; }
        auto excluded = [&](size_t candidate) {
          if (window.exclusion == WindowFrameExclusion::kCurrentRow) {
            return candidate == k;
          }
          if (window.exclusion == WindowFrameExclusion::kNone) {
            return false;
          }
          if (window.order_by.empty()) {
            return window.exclusion == WindowFrameExclusion::kGroup ||
                   candidate != k;
          }
          if (order_values.empty() || order_values[k].empty()) {
            return window.exclusion == WindowFrameExclusion::kGroup
                       ? candidate == k
                       : false;
          }
          bool peer = order_values[candidate].size() == order_values[k].size();
          for (size_t t = 0; peer && t < order_values[k].size(); ++t) {
            peer = ValuesEqual(order_values[candidate][t], order_values[k][t]);
          }
          return window.exclusion == WindowFrameExclusion::kGroup
                     ? peer
                     : peer && candidate != k;
        };
        size_t target = lo;
        if (fn == "FIRST_VALUE") {
          while (target <= hi && excluded(target)) { ++target; }
        } else if (fn == "LAST_VALUE") {
          target = hi;
          while (target >= lo && excluded(target)) {
            if (target == 0) { break; }
            --target;
          }
        } else {
          if (window.args.size() < 2) {
            throw std::runtime_error("NTH_VALUE requires two arguments");
          }
          const Value nth_value =
              runtime.EvalAt(window.args[1], rows, ordered[k]);
          int64_t nth = nth_value.value.int_value;
          size_t frame_size = 0;
          for (size_t candidate = lo; candidate <= hi; ++candidate) {
            if (!excluded(candidate)) { ++frame_size; }
          }
          if (nth <= 0 || static_cast<size_t>(nth) > frame_size) {
            continue;
          }
          for (size_t candidate = lo; candidate <= hi; ++candidate) {
            if (!excluded(candidate) && --nth == 0) {
              target = candidate;
              break;
            }
          }
        }
        if (target < lo || target > hi || excluded(target)) { continue; }
        (*out)[ordered[k]] = runtime.EvalAt(window.args[0], rows, ordered[target]);
      }
      continue;
    }

    // Everything else is treated as an aggregate over the frame.
    for (size_t k = 0; k < m; ++k) {
      const auto [lo, hi] =
          runtime.ResolveFrame(window, rows, ordered, order_values, k,
                               peer_end);
      (*out)[ordered[k]] =
          runtime.AggregateOverFrame(window, rows, ordered, order_values,
                                     k, lo, hi);
    }
  }
}

}  // namespace

bool HasWindowFunctions(const SelectStatement& statement) {
  for (const NamedExpression& projection : statement.SelectList()) {
    if (ContainsWindow(projection.expression)) { return true; }
  }
  for (const SelectStatement::OrderByTerm& term : statement.OrderBy()) {
    if (ContainsWindow(term.expression)) { return true; }
  }
  return ContainsWindow(statement.Qualify());
}

namespace {

// QUALIFY may reference select-list aliases; inline them so filtering works
// against pre-projection rows.  Real FROM columns win over aliases.
Expression InlineAliases(const Expression& expression,
                         const std::unordered_map<std::string, Expression>& aliases,
                         const Schema& schema) {
  if (!expression) { return expression; }
  switch (expression->Type()) {
    case TypeTag::kColumnValue: {
      const ColumnName& name =
          expression->AsColumnValue().GetColumnName();
      if (!name.schema.empty() || schema.Offset(name) >= 0) {
        return expression;
      }
      const auto found = aliases.find(name.name);
      return found != aliases.end() ? found->second : expression;
    }
    case TypeTag::kBinaryExp:
      return BinaryExpressionExp(
          InlineAliases(expression->AsBinaryExpression().Left(), aliases, schema),
          expression->AsBinaryExpression().Op(),
          InlineAliases(expression->AsBinaryExpression().Right(), aliases, schema));
    case TypeTag::kUnaryExp:
      return UnaryExpressionExp(
          InlineAliases(expression->AsUnaryExpression().Child(), aliases, schema),
          expression->AsUnaryExpression().Op());
    case TypeTag::kCaseExp: {
      const auto& value = expression->AsCaseExpression();
      std::vector<std::pair<Expression, Expression>> clauses;
      for (const auto& [condition, result] : value.when_clauses_) {
        clauses.emplace_back(InlineAliases(condition, aliases, schema),
                             InlineAliases(result, aliases, schema));
      }
      return CaseExpressionExp(std::move(clauses),
                               InlineAliases(value.else_clause_, aliases, schema));
    }
    case TypeTag::kFunctionCallExp:
      return FunctionCallExp(
          expression->AsFunctionCallExpression().FuncName(),
          [&] {
            std::vector<Expression> out;
            for (const Expression& argument :
                 expression->AsFunctionCallExpression().Args()) {
              out.push_back(InlineAliases(argument, aliases, schema));
            }
            return out;
          }());
    default:
      return expression;
  }
}

}  // namespace

WindowedInput ApplyWindows(TransactionContext& context,
                          const SelectStatement& statement, Relation&& input,
                          const Scope* outer, const CteMap& ctes) {
  WindowedInput result;
  result.statement = std::make_shared<SelectStatement>(statement);

  WindowNodeMap windows;
  for (const NamedExpression& projection : statement.SelectList()) {
    CollectWindows(projection.expression, &windows);
  }
  for (const SelectStatement::OrderByTerm& term : statement.OrderBy()) {
    CollectWindows(term.expression, &windows);
  }
  CollectWindows(statement.Qualify(), &windows);
  if (windows.empty()) {
    result.input = std::move(input);
    return result;
  }

  input.FinishSpill();
  std::vector<Row> rows = std::move(input.rows);
  input.rows.clear();
  const Schema base_schema = input.schema;

  ReplacementMap replacements;
  std::vector<Column> extended_columns;
  const size_t base_width = base_schema.ColumnCount();
  for (size_t i = 0; i < base_width; ++i) {
    extended_columns.push_back(base_schema.GetColumn(i));
  }
  for (const auto& [window_node, index] : windows) {
    const std::string name = "$win" + std::to_string(index);
    replacements.emplace(window_node, ColumnValueExp(ColumnName(name)));
    extended_columns.emplace_back(name, ValueType::kNull);
  }

  // Compute every hidden column.
  std::unordered_map<std::string, WindowOrderLayout> layouts;
  std::vector<std::vector<Value>> computed;
  computed.reserve(windows.size());
  for (const auto& [window_node, index] : windows) {
    (void)index;
    const std::string layout_key = WindowLayoutKey(*window_node);
    auto [layout_it, inserted] = layouts.try_emplace(layout_key);
    if (inserted) {
      WindowRuntime runtime{context, outer, &ctes, &base_schema};
      layout_it->second = BuildWindowOrderLayout(*window_node, rows, runtime);
    }
    computed.emplace_back();
    ComputeOneWindow(context, *window_node, rows, base_schema, outer, ctes,
                     &computed.back(), &layout_it->second);
  }
  for (size_t r = 0; r < rows.size(); ++r) {
    for (size_t w = 0; w < windows.size(); ++w) {
      rows[r].values_.push_back(computed[w][r]);
    }
  }
  // Fix hidden column types from the computed values.
  for (size_t w = 0; w < windows.size(); ++w) {
    Column& column = extended_columns[base_width + w];
    for (const Row& row : rows) {
      const Value& candidate = row.values_[base_width + w];
      if (!candidate.IsNull() &&
          candidate.type != ValueType::kNull) {
        column = Column(column.Name(), ValueTypeOf(candidate));
        break;
      }
    }
  }

  Relation extended(context.execution_runtime());
  extended.schema = Schema("", std::move(extended_columns));
  extended.rows = std::move(rows);
  extended.FinishSpill();

  // Rewrite the statement copy on top of a fresh replacement map (the map was
  // consumed by reference above).
  std::vector<NamedExpression> rewritten_select;
  rewritten_select.reserve(statement.SelectList().size());
  std::unordered_map<std::string, Expression> aliases;
  for (const NamedExpression& projection : statement.SelectList()) {
    Expression rebuilt = Rebuild(projection.expression, replacements);
    rewritten_select.emplace_back(projection.name, rebuilt);
    if (!projection.name.empty()) {
      aliases.emplace(projection.name, rebuilt);
    }
  }
  std::vector<SelectStatement::OrderByTerm> rewritten_order;
  for (const SelectStatement::OrderByTerm& term : statement.OrderBy()) {
    rewritten_order.push_back({Rebuild(term.expression, replacements),
                               term.ascending});
  }
  result.statement->SetSelectList(std::move(rewritten_select));
  result.statement->SetOrderBy(std::move(rewritten_order));

  if (statement.Qualify()) {
    Expression qualify =
        InlineAliases(Rebuild(statement.Qualify(), replacements), aliases,
                      extended.schema);
    Relation filtered(context.execution_runtime());
    filtered.schema = extended.schema;
    extended.ForEachRow([&](const Row& row) {
      Scope scope{.row=&row, .schema=&extended.schema, .outer=outer};
      if (Truthy(Evaluate(qualify, scope, nullptr, context, ctes))) {
        filtered.AddRow(row);
      }
    });
    filtered.FinishSpill();
    result.input = std::move(filtered);
  } else {
    result.input = std::move(extended);
  }
  result.hidden_columns = windows.size();
  return result;
}

Relation TrimHiddenColumns(Relation&& input, size_t hidden_columns) {
  if (hidden_columns == 0) { return std::move(input); }
  Relation trimmed(input.runtime());
  std::vector<Column> columns;
  const size_t keep = input.schema.ColumnCount() - hidden_columns;
  for (size_t i = 0; i < keep; ++i) {
    columns.push_back(input.schema.GetColumn(i));
  }
  trimmed.schema = Schema("", std::move(columns));
  input.FinishSpill();
  for (Row& row : input.rows) {
    row.values_.resize(keep);
    trimmed.AddRow(std::move(row));
  }
  trimmed.FinishSpill();
  return trimmed;
}

}  // namespace relational_detail
}  // namespace tinylamb
