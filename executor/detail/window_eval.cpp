/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "common/platform/process.hpp"
#include "executor/detail/window_eval.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <numeric>
#include <optional>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/parallel_work.hpp"
#include "common/platform/extended_float.hpp"
#include "common/platform/int128_conversion.hpp"
#include "common/query_parallelism.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/window_eval_internal.hpp"
#include "executor/query_memory.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/array_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/collation.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/interval_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "query/statement.hpp"
#include "type/collation.hpp"
#include "type/column.hpp"
#include "type/date.hpp"
#include "type/interval.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

// Hash of a PARTITION BY key tuple. It only has to agree with the partition
// equality in the safe direction -- equal tuples must hash equally -- which is
// exactly the canonicalization std::hash<Value> already applies: NULL is one
// pattern, NaN folds to one pattern, and interval-shaped text hashes as the
// interval it renders. A collision (an INT64 and a UINT64 sharing bits, say) is
// harmless because the equality still decides.
size_t WindowPartitionKeyHash(const std::vector<Value>& key) {
  uint64_t hash = 0x9e3779b97f4a7c15ULL;
  // EqualUnderCollation folds BOTH sides when either operand is CI, so a key
  // containing any CI value must fold every VARCHAR component the same way.
  const bool any_ci = std::ranges::any_of(key, [](const Value& v) {
    return v.type == ValueType::kVarChar &&
           CollationOf(v) == Collation::kCaseInsensitive;
  });
  for (const Value& value : key) {
    const Value canonical =
        (any_ci && value.type == ValueType::kVarChar)
            ? ApplyCollation(value, Collation::kCaseInsensitive)
            : value;
    hash ^= std::hash<Value>()(canonical) + 0x9e3779b97f4a7c15ULL +
            (hash << 6) + (hash >> 2);
  }
  return static_cast<size_t>(hash);
}

namespace {
using FrameBounds = std::pair<size_t, size_t>;
}  // namespace
namespace {

using WindowNodeMap = std::map<const WindowFunctionCallExpression*,
                               size_t>;  // sorted: stable $win naming

const WindowFunctionCallExpression* AsWindow(const Expression& expression) {
  if (expression && expression->Type() == TypeTag::kWindowFunctionExp) {
    return dynamic_cast<const WindowFunctionCallExpression*>(
        expression
            .get());  // NOLINT(cppcoreguidelines-pro-type-static-cast-downcast)
  }
  return nullptr;
}

void CollectWindows(const Expression& expression, WindowNodeMap* found) {
  if (!expression) {
    return;
  }
  if (const auto* window = AsWindow(expression)) {
    if (!found->contains(window)) {
      (*found)[window] = found->size();
    }
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
      for (const Expression& item : value.list_) {
        CollectWindows(item, found);
      }
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
  for (const Expression& item : items) {
    out.push_back(Rebuild(item, map));
  }
  return out;
}

Expression Rebuild(const Expression& expression, const ReplacementMap& map) {
  if (!expression) {
    return expression;
  }
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
      clauses.reserve(value.when_clauses_.size());
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

// NULLS FIRST on ASC / NULLS LAST on DESC by default, matching ApplyOrderBy;
// an explicit NULLS FIRST/LAST overrides the default.
bool ValueLess(const Value& a, const Value& b, bool ascending,
               const std::optional<bool>& nulls_first = std::nullopt) {
  const bool nulls_first_value = nulls_first.value_or(ascending);
  if (a.IsNull() && b.IsNull()) {
    return false;
  }
  if (a.IsNull()) {
    return nulls_first_value;
  }
  if (b.IsNull()) {
    return !nulls_first_value;
  }
  const bool a_nan =
      a.type == ValueType::kDouble && std::isnan(a.value.double_value);
  const bool b_nan =
      b.type == ValueType::kDouble && std::isnan(b.value.double_value);
  if (a_nan && b_nan) {
    return false;
  }
  if (a_nan || b_nan) {
    return ascending ? a_nan : b_nan;
  }
  try {
    return ascending ? a < b : b < a;
  } catch (...) {
    // Heterogeneous keys have no operator<; falling back to false collapsed
    // them into one comparator equivalence class (stable_sort then kept
    // physical order, contradicting ORDER BY). Use the ORDER BY total order
    // instead, which ranks cross-type keys deterministically.
    const int c = CompareForOrderBy(a, b);
    return ascending ? c < 0 : c > 0;
  }
}

bool WindowOrderLess(const Value& a, const Value& b,
                     const WindowOrderTerm& term) {
  if (a.type == ValueType::kVarChar && b.type == ValueType::kVarChar &&
      !a.IsInterval() && !b.IsInterval()) {
    const int cmp = CompareWithCollation(a, b);
    return term.ascending ? cmp < 0 : cmp > 0;
  }
  return ValueLess(a, b, term.ascending, term.nulls_first);
}

bool ValuesEqual(const Value& a, const Value& b) {
  if (a.IsNull() || b.IsNull()) {
    return a.IsNull() && b.IsNull();
  }
  if (a.type == ValueType::kDouble && b.type == ValueType::kDouble &&
      std::isnan(a.value.double_value) && std::isnan(b.value.double_value)) {
    return true;  // all NaNs are one peer group
  }
  try {
    return a == b;
  } catch (...) {
    return false;
  }
}

// Hash of a PARTITION BY key tuple; see WindowPartitionKeyHash for the
// contract. Kept as a functor so the grouping table can name it.
struct PartitionKeyHash {
  size_t operator()(const std::vector<Value>& key) const {
    return WindowPartitionKeyHash(key);
  }
};

struct PartitionKeyEq {
  bool operator()(const std::vector<Value>& a,
                  const std::vector<Value>& b) const {
    if (a.size() != b.size()) {
      return false;
    }
    for (size_t i = 0; i < a.size(); ++i) {
      if (!EqualUnderCollation(a[i], b[i])) {
        return false;
      }
    }
    return true;
  }
};

double NumericOf(const Value& value) {
  if (value.type == ValueType::kDouble) {
    return value.value.double_value;
  }
  if (value.type == ValueType::kInt64) {
    return static_cast<double>(value.value.int_value);
  }
  return std::numeric_limits<double>::quiet_NaN();
}

// RANGE frame arithmetic widens INT64 keys to ExtendedFloat so differences
// near 2^63 stay exact. A UINT64 cell stores raw bits in `int_value` with an
// `IsUnsigned()` tag, so a plain signed cast would flip the magnitude (and
// ordering) of values above 2^63; reinterpret those bits as uint64 first.
platform::ExtendedFloat FrameNumericOf(const Value& value) {
  return value.IsUnsigned()
             ? static_cast<platform::ExtendedFloat>(
                   static_cast<uint64_t>(value.value.int_value))
             : static_cast<platform::ExtendedFloat>(value.value.int_value);
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

// Exact first-minus-second distance comparison against `off`, used for RANGE
// offset frame bounds. Both INT64 keys (where double conversion would lose
// precision near 2^63) and DOUBLE keys compare in ExtendedFloat so the two
// key types give identical frame membership for the same SQL.
bool DistanceWithin(const Value& key, const Value& candidate, double off,
                    bool inclusive_le) {
  if (key.type == ValueType::kInt64 && candidate.type == ValueType::kInt64) {
    const platform::ExtendedFloat diff =
        FrameNumericOf(key) - FrameNumericOf(candidate);
    const auto bound = static_cast<platform::ExtendedFloat>(off);
    return inclusive_le ? diff <= bound : diff >= bound;
  }
  // DOUBLE keys: promote before subtracting so the comparison uses the exact
  // difference of the two doubles (as the INT64 branch and the
  // INT64 comparison helpers do).  An absolute epsilon here would
  // misclassify boundaries at magnitude ~1 -- a candidate just outside the
  // offset would be pulled inside -- and make DOUBLE keys disagree with
  // INT64 keys for the same RANGE frame.
  const platform::ExtendedFloat diff =
      static_cast<platform::ExtendedFloat>(NumericOf(key)) -
      static_cast<platform::ExtendedFloat>(NumericOf(candidate));
  const auto bound = static_cast<platform::ExtendedFloat>(off);
  return inclusive_le ? diff <= bound : diff >= bound;
}

// Exact candidate <= key + off / candidate >= key - off.  DESC orderings
// mirror RANGE boundaries around the current key, which needs the flipped
// comparisons DistanceWithin does not express; INT64 limits stay in long
// double so key +/- off cannot wrap.
bool CandidateAtMostKeyPlusOffset(const Value& candidate, const Value& key,
                                  double off) {
  const bool key_int = key.type == ValueType::kInt64;
  const platform::ExtendedFloat limit =
      key_int ? FrameNumericOf(key) + static_cast<platform::ExtendedFloat>(off)
              : static_cast<platform::ExtendedFloat>(NumericOf(key) + off);
  const platform::ExtendedFloat cand =
      candidate.type == ValueType::kInt64
          ? FrameNumericOf(candidate)
          : static_cast<platform::ExtendedFloat>(NumericOf(candidate));
  return cand <= limit;
}

bool CandidateAtLeastKeyMinusOffset(const Value& candidate, const Value& key,
                                    double off) {
  const bool key_int = key.type == ValueType::kInt64;
  const platform::ExtendedFloat limit =
      key_int ? FrameNumericOf(key) - static_cast<platform::ExtendedFloat>(off)
              : static_cast<platform::ExtendedFloat>(NumericOf(key) - off);
  const platform::ExtendedFloat cand =
      candidate.type == ValueType::kInt64
          ? FrameNumericOf(candidate)
          : static_cast<platform::ExtendedFloat>(NumericOf(candidate));
  return cand >= limit;
}

}  // namespace

// Resolves [lo, hi] (inclusive) frame bounds within the ordered partition.
StatusOr<std::pair<size_t, size_t>> WindowRuntime::ResolveFrame(
    const WindowFunctionCallExpression& window,
    const std::vector<Row>& /*rows*/, std::span<const size_t> ordered,
    const WindowOrderValues& order_values, size_t position,
    const std::vector<size_t>& peer_end,
    const std::vector<size_t>& group_ends) {
  const size_t m = ordered.size();
  // Error bridge: the nested bound
  // lambdas below record validation failures here and return a sentinel;
  // each branch checks frame_error after computing the bounds.
  Status frame_error = Status::kSuccess;
  if (!window.has_frame) {
    if (window.order_by.empty()) {
      return std::pair<size_t, size_t>{0, m - 1};
    }
    // Default frame: RANGE UNBOUNDED PRECEDING .. CURRENT ROW.
    return std::pair<size_t, size_t>{0, peer_end[position]};
  }
  if (window.frame_unit == WindowFrameUnit::kRows) {
    // Physical row offsets: CURRENT ROW excludes peers.
    auto rows_offset = [&](const WindowFrameBound& bound) -> int64_t {
      if (!bound.offset || bound.offset->Type() != TypeTag::kConstantValue) {
        frame_error = StatusError(StatusCode::kInvalidArgument,
                                  "ROWS offset must be a constant integer");
        return 0;
      }
      const Value& val = bound.offset->AsConstantValue().GetValue();
      if (val.IsNull()) {
        frame_error = StatusError(
            StatusCode::kInvalidArgument,
            "Window frame offset for PRECEDING or FOLLOWING cannot be NULL");
        return 0;
      }
      if (val.type != ValueType::kInt64) {
        frame_error = StatusError(StatusCode::kInvalidArgument,
                                  "ROWS offset must be a constant integer");
        return 0;
      }
      const int64_t off = val.value.int_value;
      if (off < 0) {
        frame_error = StatusError(StatusCode::kInvalidArgument,
                                  "Window frame offset for PRECEDING or "
                                  "FOLLOWING must be non-negative");
        return 0;
      }
      return off;
    };
    auto rows_start =
        [&](const WindowFrameBound& bound) -> std::optional<size_t> {
      switch (bound.type) {
        case WindowFrameBoundType::kUnboundedPreceding:
          return 0;
        case WindowFrameBoundType::kCurrentRow:
          return position;
        case WindowFrameBoundType::kOffsetPreceding: {
          const int64_t off = rows_offset(bound);
          // A start bound reaching past the partition start clamps to it;
          // emptiness is decided by lo > hi afterwards.
          return static_cast<int64_t>(position) - off >= 0
                     ? std::optional<size_t>(position -
                                             static_cast<size_t>(off))
                     : std::optional<size_t>(size_t{0});
        }
        case WindowFrameBoundType::kOffsetFollowing: {
          const int64_t off = rows_offset(bound);
          // A start bound past the partition end leaves an empty frame.
          if (position + static_cast<size_t>(off) > m - 1) {
            return std::nullopt;
          }
          return position + static_cast<size_t>(off);
        }
        case WindowFrameBoundType::kUnboundedFollowing:
          return m - 1;
      }
      return std::nullopt;
    };
    auto rows_end =
        [&](const WindowFrameBound& bound) -> std::optional<size_t> {
      switch (bound.type) {
        case WindowFrameBoundType::kUnboundedFollowing:
          return m - 1;
        case WindowFrameBoundType::kCurrentRow:
          return position;
        case WindowFrameBoundType::kOffsetFollowing: {
          const int64_t off = rows_offset(bound);
          return position + static_cast<size_t>(off) <= m - 1
                     ? std::optional<size_t>(position +
                                             static_cast<size_t>(off))
                     : std::optional<size_t>(m - 1);
        }
        case WindowFrameBoundType::kOffsetPreceding: {
          const int64_t off = rows_offset(bound);
          return static_cast<int64_t>(position) - off >= 0
                     ? std::optional<size_t>(position -
                                             static_cast<size_t>(off))
                     : std::nullopt;
        }
        case WindowFrameBoundType::kUnboundedPreceding:
          return size_t{0};
      }
      return std::nullopt;
    };
    const auto lo = rows_start(window.frame_start);
    const auto hi = rows_end(window.frame_end);
    if (frame_error != Status::kSuccess) {
      return frame_error;
    }
    if (!lo.has_value() || !hi.has_value() || *lo > *hi) {
      return std::pair<size_t, size_t>{1, 0};  // empty frame
    }
    return std::pair<size_t, size_t>{*lo, *hi};
  }
  if (window.frame_unit == WindowFrameUnit::kGroups) {
    if (window.order_by.empty()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "GROUPS frame requires an ORDER BY key");
    }
    // The partition layout already holds one end position per peer group.
    // Locate the current group by binary search instead of rebuilding the
    // group table and scanning it from row zero on every call.
    std::vector<size_t> fallback_ends;
    const std::vector<size_t>* ends = &group_ends;
    if (ends->empty()) {
      fallback_ends.reserve(m);
      for (size_t i = 0; i < m;) {
        const size_t end = peer_end[i];
        fallback_ends.push_back(end);
        i = end + 1;
      }
      ends = &fallback_ends;
    }
    const auto group_first = [&](const size_t group) {
      return group == 0 ? size_t{0} : (*ends)[group - 1] + 1;
    };
    const size_t current_group = static_cast<size_t>(
        std::lower_bound(ends->begin(), ends->end(), position) - ends->begin());
    const size_t group_count = ends->size();
    auto offset_of = [&](const WindowFrameBound& bound) -> size_t {
      if (!bound.offset || bound.offset->Type() != TypeTag::kConstantValue) {
        frame_error = StatusError(StatusCode::kInvalidArgument,
                                  "GROUPS offset requires a constant value");
        return 0;
      }
      const Value& val = bound.offset->AsConstantValue().GetValue();
      if (val.IsNull()) {
        frame_error = StatusError(
            StatusCode::kInvalidArgument,
            "Window frame offset for PRECEDING or FOLLOWING cannot be NULL");
        return 0;
      }
      if (val.type != ValueType::kInt64 && val.type != ValueType::kDouble) {
        frame_error = StatusError(StatusCode::kInvalidArgument,
                                  "GROUPS offset must be numeric");
        return 0;
      }
      const double value = NumericOf(val);
      // NaN and negativity are reported before integrality: NaN is not an
      // integer either, and "-1.5" is primarily a negative offset.
      if (std::isnan(value)) {
        frame_error = StatusError(
            StatusCode::kInvalidArgument,
            "Window frame offset for PRECEDING or FOLLOWING cannot be NaN");
        return 0;
      }
      if (value < 0) {
        frame_error = StatusError(StatusCode::kInvalidArgument,
                                  "Window frame offset for PRECEDING or "
                                  "FOLLOWING must be non-negative");
        return 0;
      }
      if (val.type == ValueType::kInt64) {
        return static_cast<size_t>(std::min<uint64_t>(
            static_cast<uint64_t>(val.value.int_value), group_count));
      }
      if (!std::isfinite(value) || value != std::floor(value)) {
        frame_error = StatusError(StatusCode::kInvalidArgument,
                                  "GROUPS offset must be an integer");
        return 0;
      }
      // Offsets beyond the partition are equivalent to its group count.
      // Clamp before converting because a finite DOUBLE can exceed SIZE_MAX.
      return value >= static_cast<double>(group_count)
                 ? group_count
                 : static_cast<size_t>(value);
    };
    // Start bounds clamp at the partition head and end bounds at the
    // partition tail, but a start bound past the last peer group or an end
    // bound before the first one leaves an empty frame (nullopt), matching
    // the ROWS/RANGE branches instead of clamping onto an edge group.
    auto groups_start =
        [&](const WindowFrameBound& bound) -> std::optional<size_t> {
      switch (bound.type) {
        case WindowFrameBoundType::kUnboundedPreceding:
          return size_t{0};
        case WindowFrameBoundType::kOffsetPreceding: {
          const size_t offset = offset_of(bound);
          const size_t group =
              offset > current_group ? size_t{0} : current_group - offset;
          return group_first(group);
        }
        case WindowFrameBoundType::kCurrentRow:
          return group_first(current_group);
        case WindowFrameBoundType::kOffsetFollowing: {
          const size_t offset = offset_of(bound);
          // A start bound past the partition end leaves an empty frame.
          if (offset >= group_count - current_group) {
            return std::nullopt;
          }
          return group_first(current_group + offset);
        }
        case WindowFrameBoundType::kUnboundedFollowing:
          // Same as the ROWS branch (start = last row): the frame starts at
          // the last peer group.  (The SQL frontend rejects this spelling;
          // the evaluator stays total for direct callers.)
          return group_first(group_count - 1);
      }
      return std::nullopt;
    };
    auto groups_end =
        [&](const WindowFrameBound& bound) -> std::optional<size_t> {
      switch (bound.type) {
        case WindowFrameBoundType::kUnboundedPreceding:
          return size_t{0};
        case WindowFrameBoundType::kOffsetPreceding: {
          const size_t offset = offset_of(bound);
          // An end bound before the partition start leaves an empty frame.
          if (offset > current_group) {
            return std::nullopt;
          }
          return (*ends)[current_group - offset];
        }
        case WindowFrameBoundType::kCurrentRow:
          return (*ends)[current_group];
        case WindowFrameBoundType::kOffsetFollowing: {
          const size_t offset = offset_of(bound);
          const size_t group = offset >= group_count - current_group
                                   ? group_count - 1
                                   : current_group + offset;
          return (*ends)[group];
        }
        case WindowFrameBoundType::kUnboundedFollowing:
          return m - 1;
      }
      return std::nullopt;
    };
    const auto lo = groups_start(window.frame_start);
    const auto hi = groups_end(window.frame_end);
    if (frame_error != Status::kSuccess) {
      return frame_error;
    }
    if (!lo.has_value() || !hi.has_value() || *lo > *hi) {
      return std::pair<size_t, size_t>{1, 0};  // empty frame
    }
    return std::pair<size_t, size_t>{*lo, *hi};
  }
  // First row of the current row's peer group; RANGE `CURRENT ROW` start
  // bounds open at the group head (the end bound already closes on
  // peer_end), matching the aggregate-with-peers default frame.
  auto first_peer = [&](size_t idx) {
    size_t j = idx;
    while (j > 0) {
      const auto above = order_values[j - 1];
      const auto mine = order_values[idx];
      bool peer = above.size() == mine.size();
      for (size_t t = 0; peer && t < mine.size(); ++t) {
        peer = ValuesEqual(above[t], mine[t]);
      }
      if (!peer) {
        break;
      }
      --j;
    }
    return j;
  };
  // A RANGE frame boundary: a plain numeric offset, or a calendar-aware
  // interval for DATE keys (`RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND
  // CURRENT ROW` spells rolling time-series windows directly).  Interval
  // boundaries apply the engine's DATE + INTERVAL arithmetic (month-end
  // clamping included), so a DATE key at a month boundary keeps the same
  // window the scalar arithmetic produces.
  struct RangeBound {
    double numeric{0};
    bool is_interval{false};
    IntervalValue interval;
  };
  auto range_bound =
      [&](const WindowFrameBound& bound) -> std::optional<RangeBound> {
    if (window.order_by.size() != 1 || !bound.offset) {
      frame_error = StatusError(StatusCode::kInvalidArgument,
                                "RANGE offset requires one constant key");
      return std::nullopt;
    }
    if (bound.offset->Type() == TypeTag::kIntervalExp) {
      RangeBound parsed;
      parsed.is_interval = true;
      parsed.interval = bound.offset->AsIntervalExpression().GetIntervalValue();
      const IntervalValue& iv = parsed.interval;
      if (iv.months < 0 || iv.days < 0 || iv.nanos < 0) {
        frame_error = StatusError(StatusCode::kInvalidArgument,
                                  "Window frame interval offset for PRECEDING "
                                  "or FOLLOWING must be non-negative");
        return std::nullopt;
      }
      return parsed;
    }
    if (bound.offset->Type() != TypeTag::kConstantValue) {
      frame_error = StatusError(StatusCode::kInvalidArgument,
                                "RANGE offset requires one constant key");
      return std::nullopt;
    }
    const Value& off_val = bound.offset->AsConstantValue().GetValue();
    if (off_val.IsNull()) {
      frame_error = StatusError(
          StatusCode::kInvalidArgument,
          "Window frame offset for PRECEDING or FOLLOWING cannot be NULL");
      return std::nullopt;
    }
    if (off_val.type == ValueType::kVarChar) {
      // A constant-folded interval literal evaluates to its text form; a
      // non-interval string here was never a valid numeric offset anyway.
      if (StatusOr<IntervalValue> interval =
              IntervalValue::TryParse(off_val.value.varchar_value);
          interval.HasValue()) {
        RangeBound parsed;
        parsed.is_interval = true;
        parsed.interval = interval.MoveValue();
        if (parsed.interval.months < 0 || parsed.interval.days < 0 ||
            parsed.interval.nanos < 0) {
          frame_error =
              StatusError(StatusCode::kInvalidArgument,
                          "Window frame interval offset for PRECEDING "
                          "or FOLLOWING must be non-negative");
          return std::nullopt;
        }
        return parsed;
      }
    }
    if (off_val.type != ValueType::kInt64 &&
        off_val.type != ValueType::kDouble) {
      frame_error = StatusError(StatusCode::kInvalidArgument,
                                "RANGE offset must be numeric");
      return std::nullopt;
    }
    const double off = NumericOf(off_val);
    if (std::isnan(off)) {
      frame_error = StatusError(
          StatusCode::kInvalidArgument,
          "Window frame offset for PRECEDING or FOLLOWING cannot be NaN");
      return std::nullopt;
    }
    if (off < 0) {
      frame_error = StatusError(StatusCode::kInvalidArgument,
                                "Window frame offset for PRECEDING or "
                                "FOLLOWING must be non-negative");
      return std::nullopt;
    }
    RangeBound parsed;
    parsed.numeric = off;
    return parsed;
  };
  // Boundary day count for an interval bound over a DATE key:
  // `key + sign * interval`.  Sub-day components carry no DATE meaning and
  // are rejected so the window cannot silently widen or narrow.
  auto interval_boundary_days =
      [&](const Value& key, int sign,
          const IntervalValue& interval) -> std::optional<int64_t> {
    if (key.type != ValueType::kDate) {
      frame_error =
          StatusError(StatusCode::kInvalidArgument,
                      "RANGE INTERVAL offset requires a DATE order key");
      return std::nullopt;
    }
    if (interval.nanos != 0) {
      frame_error = StatusError(
          StatusCode::kInvalidArgument,
          "RANGE INTERVAL offset must be day-aligned for DATE keys");
      return std::nullopt;
    }
    int64_t days = key.value.int_value;
    if (interval.months != 0) {
      StatusOr<int64_t> shifted =
          TryAddDateIntervalDays(days, sign * interval.months, "month");
      if (!shifted.HasValue()) {
        frame_error = shifted.GetStatus();
        return std::nullopt;
      }
      days = shifted.MoveValue();
    }
    if (interval.days != 0) {
      StatusOr<int64_t> shifted =
          TryAddDateIntervalDays(days, sign * interval.days, "day");
      if (!shifted.HasValue()) {
        frame_error = shifted.GetStatus();
        return std::nullopt;
      }
      days = shifted.MoveValue();
    }
    return days;
  };
  auto start_of = [&](const WindowFrameBound& bound) -> std::optional<size_t> {
    switch (bound.type) {
      case WindowFrameBoundType::kUnboundedPreceding:
        return 0;
      case WindowFrameBoundType::kCurrentRow:
        return first_peer(position);
      case WindowFrameBoundType::kOffsetPreceding: {
        const auto opt_bound = range_bound(bound);
        if (!opt_bound.has_value()) {
          return std::nullopt;
        }
        const Value& key = order_values[position][0];
        if (key.IsNull()) {
          // NULLs are peers of each other: the offset bound collapses to
          // the peer group, so the frame opens at the group head.
          return first_peer(position);
        }
        if (key.type == ValueType::kDouble &&
            std::isnan(key.value.double_value)) {
          // NaNs form one peer group: the offset bound cannot distinguish
          // them from each other, so it collapses to the peer group head.
          return first_peer(position);
        }
        // ASC: first row with candidate >= key - off.  DESC mirrors the
        // band around the current key: first row with candidate <=
        // key + off.
        const bool asc = window.order_by[0].ascending;
        if (opt_bound->is_interval) {
          const auto boundary =
              interval_boundary_days(key, asc ? -1 : 1, opt_bound->interval);
          if (!boundary.has_value()) {
            return std::nullopt;
          }
          for (size_t j = 0; j <= position; ++j) {
            const Value& candidate = order_values[j][0];
            if (candidate.IsNull()) {
              continue;
            }
            if (asc ? candidate.value.int_value >= *boundary
                    : candidate.value.int_value <= *boundary) {
              return j;
            }
          }
          return position;
        }
        const double off = opt_bound->numeric;
        // A numeric RANGE offset only makes sense over a numeric key.  A DATE
        // key falls into NumericOf's NaN path, which makes every comparison
        // silently false (empty frame / NULL SUM) instead of an error.
        if (key.type != ValueType::kInt64 && key.type != ValueType::kDouble) {
          frame_error =
              StatusError(StatusCode::kInvalidArgument,
                          "RANGE numeric offset requires a numeric order key");
          return std::nullopt;
        }
        for (size_t j = 0; j <= position; ++j) {
          const Value& candidate = order_values[j][0];
          if (candidate.IsNull()) {
            continue;
          }
          if (asc ? DistanceWithin(key, candidate, off, true)
                  : CandidateAtMostKeyPlusOffset(candidate, key, off)) {
            return j;
          }
        }
        return position;
      }
      case WindowFrameBoundType::kOffsetFollowing: {
        // Start bound `N FOLLOWING`: the FIRST row whose key is >=
        // `key + off` (ASC) / <= `key - off` (DESC).
        const auto opt_bound = range_bound(bound);
        if (!opt_bound.has_value()) {
          return std::nullopt;
        }
        const Value& key = order_values[position][0];
        if (key.IsNull()) {
          // NULLs are peers of each other: the offset bound collapses to
          // the peer group, so the frame opens at the group head.
          return first_peer(position);
        }
        if (key.type == ValueType::kDouble &&
            std::isnan(key.value.double_value)) {
          // NaNs form one peer group: the offset bound cannot distinguish
          // them from each other, so it collapses to the peer group head.
          return first_peer(position);
        }
        const bool asc = window.order_by[0].ascending;
        if (opt_bound->is_interval) {
          const auto boundary =
              interval_boundary_days(key, asc ? 1 : -1, opt_bound->interval);
          if (!boundary.has_value()) {
            return std::nullopt;
          }
          for (size_t j = 0; j < m; ++j) {
            const Value& candidate = order_values[j][0];
            if (candidate.IsNull()) {
              continue;
            }
            if (asc ? candidate.value.int_value >= *boundary
                    : candidate.value.int_value <= *boundary) {
              return j;
            }
          }
          // No row satisfies the bound: the frame is empty.
          return std::nullopt;
        }
        const double off = opt_bound->numeric;
        // A numeric RANGE offset only makes sense over a numeric key.  A DATE
        // key falls into NumericOf's NaN path, which makes every comparison
        // silently false (empty frame / NULL SUM) instead of an error.
        if (key.type != ValueType::kInt64 && key.type != ValueType::kDouble) {
          frame_error =
              StatusError(StatusCode::kInvalidArgument,
                          "RANGE numeric offset requires a numeric order key");
          return std::nullopt;
        }
        for (size_t j = 0; j < m; ++j) {
          const Value& candidate = order_values[j][0];
          if (candidate.IsNull()) {
            continue;
          }
          if (asc ? DistanceWithin(  // NOLINT(readability-suspicious-call-argument)
                        candidate, key, off,
                        false)  // (scanned, current) order is intentional.
                  : CandidateAtMostKeyPlusOffset(candidate, key, -off)) {
            return j;
          }
        }
        // No row satisfies the bound: the frame is empty.
        return std::nullopt;
      }
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
        const auto opt_bound = range_bound(bound);
        if (!opt_bound.has_value()) {
          return std::nullopt;
        }
        const Value& key = order_values[position][0];
        if (key.IsNull()) {
          return peer_end[position];
        }
        // ASC: last row with candidate <= key + off.  DESC: last row with
        // candidate >= key - off.
        const bool asc = window.order_by[0].ascending;
        std::optional<size_t> reached;
        if (opt_bound->is_interval) {
          const auto boundary =
              interval_boundary_days(key, asc ? 1 : -1, opt_bound->interval);
          if (!boundary.has_value()) {
            return std::nullopt;
          }
          for (size_t j = 0; j < m; ++j) {
            const Value& candidate = order_values[j][0];
            if (candidate.IsNull()) {
              continue;
            }
            if (asc ? candidate.value.int_value <= *boundary
                    : candidate.value.int_value >= *boundary) {
              reached = j;
            }
          }
          return reached;
        }
        const double off = opt_bound->numeric;
        // A numeric RANGE offset only makes sense over a numeric key.  A DATE
        // key falls into NumericOf's NaN path, which makes every comparison
        // silently false (empty frame / NULL SUM) instead of an error.
        if (key.type != ValueType::kInt64 && key.type != ValueType::kDouble) {
          frame_error =
              StatusError(StatusCode::kInvalidArgument,
                          "RANGE numeric offset requires a numeric order key");
          return std::nullopt;
        }
        for (size_t j = 0; j < m; ++j) {
          const Value& candidate = order_values[j][0];
          if (candidate.IsNull()) {
            continue;
          }
          if (asc ? DistanceWithin(  // NOLINT(readability-suspicious-call-argument)
                        candidate, key, off,
                        true)  // (scanned, current) order is intentional.
                  : CandidateAtLeastKeyMinusOffset(candidate, key, off)) {
            reached = j;
          }
        }
        return reached.value_or(peer_end[position]);
      }
      case WindowFrameBoundType::kUnboundedPreceding:
        // As a frame END bound this is the first row of the partition,
        // so any CURRENT/FOLLOWING start bound produces an empty frame.
        return size_t{0};
      case WindowFrameBoundType::kOffsetPreceding: {
        const auto opt_bound = range_bound(bound);
        if (!opt_bound.has_value()) {
          return std::nullopt;
        }
        // RANGE `0 PRECEDING` is defined as CURRENT ROW, i.e. the bound is
        // the last row of the current row's peer group. The value scan below
        // is capped at `position`, which would truncate a peer group to its
        // first row.
        const bool zero_interval = opt_bound->is_interval &&
                                   opt_bound->interval.months == 0 &&
                                   opt_bound->interval.days == 0;
        if ((!opt_bound->is_interval && opt_bound->numeric == 0.0) ||
            zero_interval) {
          return peer_end[position];
        }
        const Value& key = order_values[position][0];
        if (key.IsNull()) {
          return peer_end[position];
        }
        // End bound "off PRECEDING": ASC keeps candidates <= key - off,
        // DESC keeps candidates >= key + off, computed exactly (key -/+ off
        // may underflow int64).  NULL keys sort lowest and stay inside an
        // unbounded-start frame.
        const bool asc = window.order_by[0].ascending;
        std::optional<size_t> reached;
        if (opt_bound->is_interval) {
          const auto boundary =
              interval_boundary_days(key, asc ? -1 : 1, opt_bound->interval);
          if (!boundary.has_value()) {
            return std::nullopt;
          }
          for (size_t j = 0; j <= position; ++j) {
            const Value& candidate = order_values[j][0];
            if (candidate.IsNull()) {
              continue;
            }
            if (asc ? candidate.value.int_value <= *boundary
                    : candidate.value.int_value >= *boundary) {
              reached = j;
            }
          }
          return reached;
        }
        const double off = opt_bound->numeric;
        // A numeric RANGE offset only makes sense over a numeric key.  A DATE
        // key falls into NumericOf's NaN path, which makes every comparison
        // silently false (empty frame / NULL SUM) instead of an error.
        if (key.type != ValueType::kInt64 && key.type != ValueType::kDouble) {
          frame_error =
              StatusError(StatusCode::kInvalidArgument,
                          "RANGE numeric offset requires a numeric order key");
          return std::nullopt;
        }
        const bool key_int = key.type == ValueType::kInt64;
        const platform::ExtendedFloat asc_limit =
            key_int
                ? FrameNumericOf(key) -
                      static_cast<platform::ExtendedFloat>(off)
                : static_cast<platform::ExtendedFloat>(NumericOf(key) - off);
        const platform::ExtendedFloat desc_limit =
            key_int
                ? FrameNumericOf(key) +
                      static_cast<platform::ExtendedFloat>(off)
                : static_cast<platform::ExtendedFloat>(NumericOf(key) + off);
        for (size_t j = 0; j <= position; ++j) {
          const Value& candidate = order_values[j][0];
          if (candidate.IsNull()) {
            continue;
          }
          const platform::ExtendedFloat cand =
              candidate.type == ValueType::kInt64
                  ? FrameNumericOf(candidate)
                  : static_cast<platform::ExtendedFloat>(NumericOf(candidate));
          if (asc ? cand <= asc_limit : cand >= desc_limit) {
            reached = j;
          }
        }
        return reached;
      }
    }
    return peer_end[position];
  };

  const auto lo = start_of(window.frame_start);
  const auto hi = end_of(window.frame_end);
  if (frame_error != Status::kSuccess) {
    return frame_error;
  }
  if (!lo.has_value() || !hi.has_value() || *lo > *hi) {
    return std::pair<size_t, size_t>{1, 0};  // empty frame
  }
  return std::pair<size_t, size_t>{*lo, *hi};
}

// Frame rows (positions within [lo, hi] of the ordered partition) that
// survive the frame EXCLUDE clause and the aggregate WHERE filter, in
// frame order.  Shared by the single-argument aggregate path, the
// two-argument COVAR/CORR collector, and APPROX_TOP_* so all of them
// honor identical frame semantics.
StatusOr<std::vector<size_t>> WindowRuntime::FrameSurvivors(
    const WindowFunctionCallExpression& window, const std::vector<Row>& rows,
    std::span<const size_t> ordered, const WindowOrderValues& order_values,
    size_t current, size_t lo, size_t hi) const {
  std::vector<size_t> survivors;
  survivors.reserve(hi - lo + 1);
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
            excluded =
                ValuesEqual(order_values[p][t], order_values[current][t]);
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
            excluded =
                ValuesEqual(order_values[p][t], order_values[current][t]);
          }
        }
        break;
      }
      case WindowFrameExclusion::kNone:
        break;
    }
    if (excluded) {
      continue;
    }
    const Row& row = rows[ordered[p]];
    Scope scope{.row = &row, .schema = schema, .outer = outer};
    // AGG(x WHERE cond) OVER (...): skip rows failing the filter.
    if (window.where_filter) {
      // A raising filter is a query error, exactly as for the grouped
      // AGG(x WHERE cond) path; it must not silently shrink the frame.
      ASSIGN_OR_RETURN(
          Value, keep,
          (TryEvaluate(window.where_filter, scope, nullptr, context, *ctes)));
      if (!Truthy(keep)) {
        continue;
      }
    }
    survivors.push_back(p);
  }
  return survivors;
}

StatusOr<Value> WindowRuntime::AggregateOverFrame(
    const WindowFunctionCallExpression& window, const std::vector<Row>& rows,
    std::span<const size_t> ordered, const WindowOrderValues& order_values,
    size_t current, size_t lo, size_t hi) const {
  const std::string& fn = window.function;
  if (hi < lo || ordered.empty() || hi >= ordered.size()) {
    if (fn == "COUNT" || fn == "COUNTIF") {
      return Value(static_cast<int64_t>(0));
    }
    return Value();
  }
  // The window expression carries an optional row-level WHERE filter.
  int64_t row_count = 0;
  bool count_star = fn == "COUNT" && window.args.size() == 1 &&
                    window.args[0]->Type() == TypeTag::kColumnValue &&
                    window.args[0]->AsColumnValue().GetColumnName().name == "*";
  std::vector<Value> values;
  // ARRAY_AGG(x ORDER BY k LIMIT n) keeps each row's key tuple until
  // the values have been ordered and the inner limit applied.
  const bool need_order_keys =
      fn == "ARRAY_AGG" && !window.inner_order_by.empty();
  std::vector<std::vector<Value>> order_keys;
  ASSIGN_OR_RETURN(
      std::vector<size_t>, frame_survivors,
      (FrameSurvivors(window, rows, ordered, order_values, current, lo, hi)));
  if (fn == "STRING_AGG") {
    if (window.args.empty()) {
      return Value();
    }
    AggregateExpression aggregate(AggregationType::kStringAgg, window.args[0],
                                  window.distinct);
    if (window.args.size() > 1) {
      aggregate.SetSecondaryArg(window.args[1]);
    }
    aggregate.SetInnerOrderBy(window.inner_order_by);
    aggregate.SetInnerLimit(window.inner_limit);
    AggregateAccumulator accumulator(&aggregate);
    for (const size_t p : frame_survivors) {
      const Scope scope{
          .row = &rows[ordered[p]], .schema = schema, .outer = outer};
      AggregateInput input;
      ASSIGN_OR_RETURN(
          Value, value,
          (TryEvaluate(aggregate.Child(), scope, nullptr, context, *ctes)));
      input.value = std::move(value);
      if (aggregate.SecondaryArg()) {
        ASSIGN_OR_RETURN(Value, delimiter,
                         (TryEvaluate(aggregate.SecondaryArg(), scope, nullptr,
                                      context, *ctes)));
        input.auxiliary = std::move(delimiter);
      }
      input.order_keys.reserve(window.inner_order_by.size());
      for (const WindowOrderTerm& term : window.inner_order_by) {
        ASSIGN_OR_RETURN(
            Value, key,
            (TryEvaluate(term.expression, scope, nullptr, context, *ctes)));
        input.order_keys.push_back(std::move(key));
      }
      RETURN_IF_FAIL(accumulator.Add(std::move(input)));
    }
    return accumulator.TryFinish();
  }
  if (fn == "APPROX_TOP_COUNT" || fn == "APPROX_TOP_SUM") {
    const bool top_sum = fn == "APPROX_TOP_SUM";
    if (frame_survivors.empty() || window.args.empty()) {
      return Value();
    }
    int64_t k = 1;
    const size_t n_arg = top_sum ? 2 : 1;
    if (window.args.size() > n_arg) {
      ASSIGN_OR_RETURN(Value, n_value,
                       (TryEvaluate(window.args[n_arg],
                                    (Scope{.row = &rows[ordered[lo]],
                                           .schema = schema,
                                           .outer = outer}),
                                    nullptr, context, *ctes)));
      if (n_value.IsNull()) {
        return StatusError(StatusCode::kInvalidArgument,
                           "Null top-k count is not supported");
      }
      if (n_value.type == ValueType::kInt64) {
        k = n_value.value.int_value;
      } else if (n_value.type == ValueType::kDouble) {
        const double d = n_value.value.double_value;
        if (std::isnan(d) || d < 1.0 || d > 100000.0) {
          return StatusError(StatusCode::kInvalidArgument,
                             "Invalid top-k count");
        }
        k = static_cast<int64_t>(d);
      } else {
        return StatusError(StatusCode::kInvalidArgument,
                           fn + " requires an integer count");
      }
      if (k <= 0 || k > 100000) {
        return StatusError(StatusCode::kInvalidArgument, "Invalid top-k count");
      }
    }
    const AggregateExpression aggregate(top_sum
                                            ? AggregationType::kApproxTopSum
                                            : AggregationType::kApproxTopCount,
                                        window.args[0]);
    AggregateAccumulator accumulator(&aggregate);
    std::string element_type;
    bool floating_weight = false;
    for (const size_t p : frame_survivors) {
      const Scope scope{
          .row = &rows[ordered[p]], .schema = schema, .outer = outer};
      ASSIGN_OR_RETURN(
          Value, value,
          (TryEvaluate(window.args[0], scope, nullptr, context, *ctes)));
      if (element_type.empty()) {
        element_type = ElementSqlTypeOf(value);
      }
      AggregateInput input;
      input.value = std::move(value);
      if (top_sum) {
        Value weight(1);
        if (window.args.size() > 1) {
          ASSIGN_OR_RETURN(
              Value, supplied_weight,
              (TryEvaluate(window.args[1], scope, nullptr, context, *ctes)));
          weight = std::move(supplied_weight);
        }
        if (weight.IsUnsigned() && weight.value.int_value < 0) {
          weight = Value(static_cast<double>(
              static_cast<uint64_t>(weight.value.int_value)));
        }
        floating_weight |= weight.type == ValueType::kDouble;
        input.trailing_values.push_back(std::move(weight));
      }
      input.trailing_values.emplace_back(k);
      RETURN_IF_FAIL(accumulator.Add(std::move(input)));
    }
    ASSIGN_OR_RETURN(Value, result, accumulator.TryFinish());
    if (!result.IsArray()) {
      return result;
    }
    if (element_type.empty()) {
      element_type = "STRING";
    }
    const std::string_view metric =
        top_sum ? (floating_weight ? "sum FLOAT64" : "sum INT64")
                : "count INT64";
    return Value::Array(
        result.ArrayElements(),
        "STRUCT<value " + element_type + ", " + std::string(metric) + ">");
  }
  values.reserve(frame_survivors.size());
  if (need_order_keys) {
    order_keys.reserve(frame_survivors.size());
  }
  for (const size_t p : frame_survivors) {
    const Row& row = rows[ordered[p]];
    Scope scope{.row = &row, .schema = schema, .outer = outer};
    ++row_count;
    if (count_star) {
      continue;
    }
    if (!window.args.empty()) {
      ASSIGN_OR_RETURN(
          Value, arg_value,
          (TryEvaluate(window.args[0], scope, nullptr, context, *ctes)));
      values.push_back(std::move(arg_value));
      if (need_order_keys) {
        std::vector<Value> keys;
        keys.reserve(window.inner_order_by.size());
        for (const WindowOrderTerm& term : window.inner_order_by) {
          ASSIGN_OR_RETURN(
              Value, key,
              (TryEvaluate(term.expression, scope, nullptr, context, *ctes)));
          keys.push_back(std::move(key));
        }
        order_keys.push_back(std::move(keys));
      }
    }
  }

  if (window.distinct) {
    // Dedup on the same canonical form the grouped accumulator's DISTINCT
    // mode uses (CanonicalDistinctValue + DistinctValueEqual), so a
    // case-insensitive pair folds here exactly like COUNT(DISTINCT ...).
    // The first-seen original value is kept for the aggregate input.
    std::vector<Value> unique;
    std::vector<Value> unique_canonical;
    std::vector<std::vector<Value>> unique_keys;
    unique.reserve(values.size());
    unique_canonical.reserve(values.size());
    if (!order_keys.empty()) {
      unique_keys.reserve(values.size());
    }
    for (size_t i = 0; i < values.size(); ++i) {
      Value& candidate = values[i];
      Value canonical = CanonicalDistinctValue(candidate);
      bool duplicate = false;
      for (const Value& kept : unique_canonical) {
        if (DistinctValueEqual{}(kept, canonical)) {
          duplicate = true;
          break;
        }
      }
      if (!duplicate) {
        unique.push_back(std::move(candidate));
        unique_canonical.push_back(std::move(canonical));
        if (!order_keys.empty()) {
          unique_keys.push_back(std::move(order_keys[i]));
        }
      }
    }
    values = std::move(unique);
    order_keys = std::move(unique_keys);
  }
  if (!order_keys.empty()) {
    std::vector<size_t> perm(values.size());
    std::iota(perm.begin(), perm.end(), 0);
    std::stable_sort(perm.begin(), perm.end(), [&](size_t a, size_t b) {
      const std::vector<Value>& keys_a = order_keys[a];
      const std::vector<Value>& keys_b = order_keys[b];
      for (size_t t = 0; t < window.inner_order_by.size(); ++t) {
        const WindowOrderTerm& term = window.inner_order_by[t];
        if (ValuesEqual(keys_a[t], keys_b[t])) {
          continue;
        }
        return ValueLess(keys_a[t], keys_b[t], term.ascending,
                         term.nulls_first);
      }
      return false;
    });
    std::vector<Value> sorted;
    sorted.reserve(values.size());
    for (const size_t i : perm) {
      sorted.push_back(std::move(values[i]));
    }
    values = std::move(sorted);
  }

  std::vector<Value> non_null;
  non_null.reserve(values.size());
  for (Value& value : values) {
    if ((fn != "ARRAY_AGG" || window.ignore_nulls) && !value.IsNull()) {
      non_null.push_back(std::move(value));
    }
  }

  if (fn == "COUNT") {
    return Value(count_star ? row_count
                            : static_cast<int64_t>(non_null.size()));
  }
  if (fn == "APPROX_COUNT_DISTINCT") {
    std::vector<Value> unique;
    unique.reserve(non_null.size());
    for (const Value& candidate : non_null) {
      bool duplicate = false;
      for (const Value& kept : unique) {
        if (ValuesEqual(candidate, kept)) {
          duplicate = true;
          break;
        }
      }
      if (!duplicate) {
        unique.push_back(candidate);
      }
    }
    return Value(static_cast<int64_t>(unique.size()));
  }
  if (fn == "ARRAY_CONCAT_AGG") {
    // Concatenates ARRAY elements across the frame rows.
    std::vector<Value> merged;
    std::string element_type;
    for (const Value& array : non_null) {
      if (array.IsNull() || !array.IsArray()) {
        continue;
      }
      for (const Value& element : array.ArrayElements()) {
        if (element_type.empty() && !element.IsNull()) {
          element_type = ElementSqlTypeOf(element);
        }
        merged.push_back(element);
      }
    }
    return Value::Array(std::move(merged), std::move(element_type));
  }
  if (fn == "APPROX_QUANTILES") {
    // Exact percentile interpolation over the frame; returns NUM quantiles
    // including both endpoints.
    if (non_null.empty() || window.args.size() < 2) {
      return Value();
    }
    ASSIGN_OR_RETURN(Value, n_value,
                     (TryEvaluate(window.args[1],
                                  (Scope{.row = &rows[ordered[lo]],
                                         .schema = schema,
                                         .outer = outer}),
                                  nullptr, context, *ctes)));
    if (n_value.IsNull()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "Null number of quantiles is not supported");
    }
    int64_t num = -1;
    if (n_value.type == ValueType::kInt64) {
      num = n_value.value.int_value;
    } else if (n_value.type == ValueType::kDouble) {
      const double d = n_value.value.double_value;
      if (std::isnan(d) || d < 1.0 || d > 100000.0) {
        return StatusError(StatusCode::kInvalidArgument,
                           "Invalid number of quantiles");
      }
      num = static_cast<int64_t>(d);
    } else {
      return StatusError(StatusCode::kInvalidArgument,
                         "APPROX_QUANTILES requires an integer count");
    }
    if (num <= 0 || num > 100000) {
      return StatusError(StatusCode::kInvalidArgument,
                         "Invalid number of quantiles");
    }
    // `non_null` is this frame's temporary; sort it in place instead of
    // deep-copying every Value (including VARCHAR payloads) per row.
    std::vector<Value> sorted = std::move(non_null);
    std::sort(sorted.begin(), sorted.end(), [](const Value& a, const Value& b) {
      return CompareForOrderBy(a, b) < 0;
    });
    // APPROX_QUANTILES returns NUM+1 percentiles including both endpoints.
    std::vector<Value> quantiles;
    quantiles.reserve(static_cast<size_t>(num) + 1);
    for (int64_t i = 0; i <= num; ++i) {
      // Compute in Int128: i * (n-1) can overflow int64 for very large frames
      // even though i and num are bounded.
      const platform::Int128 idx128 =
          static_cast<platform::Int128>(i) *
          static_cast<platform::Int128>(sorted.size() - 1) /
          static_cast<platform::Int128>(num);
      const auto idx =
          static_cast<int64_t>(idx128);  // NOLINT(modernize-use-auto)
      quantiles.push_back(sorted[static_cast<size_t>(idx)]);
    }
    return Value::Array(std::move(quantiles), ElementSqlTypeOf(sorted[0]));
  }
  if (fn == "COUNTIF") {
    int64_t trues = 0;
    for (const Value& value : non_null) {
      if (Truthy(value)) {
        ++trues;
      }
    }
    return Value(trues);
  }
  if (non_null.empty() && fn != "ARRAY_AGG") {
    return Value();
  }

  if (fn == "SUM" || fn == "AVG") {
    for (const Value& value : non_null) {
      if (value.type != ValueType::kInt64 && value.type != ValueType::kDouble) {
        return StatusError(StatusCode::kInvalidArgument,
                           fn + " requires a numeric argument");
      }
    }
    if (fn == "AVG") {
      // AVG sums through a double intermediate with tag-respecting casts
      // (AggregateAccumulator ground truth): never an overflow error.
      // A signed cast would flip the magnitude of a UINT64 cell.
      double total = 0.0;
      for (const Value& value : non_null) {
        total += value.type == ValueType::kDouble
                     ? value.value.double_value
                     : (value.IsUnsigned()
                            ? static_cast<double>(
                                  static_cast<uint64_t>(value.value.int_value))
                            : static_cast<double>(value.value.int_value));
      }
      return Value(total / static_cast<double>(non_null.size()));
    }
    // SUM mirrors AggregateAccumulator: doubles keep a separate running
    // total while integer inputs stay exact in the signed or uint64 domain;
    // any UINT64 input switches the integer total to an exact Int128
    // accumulation bounded to the uint64 range and the result carries the
    // UINT64 tag. A plain signed wrap would make SUM(u) OVER () disagree
    // with SUM(u) over the same rows in the same statement.
    double double_total = 0.0;
    uint64_t uint_total = 0;
    int64_t int_total = 0;
    bool sum_is_uint64 = false;
    bool saw_double = false;
    for (const Value& value : non_null) {
      if (value.type == ValueType::kDouble) {
        saw_double = true;
        double_total += value.value.double_value;
      } else if (sum_is_uint64 || value.IsUnsigned()) {
        const platform::Int128 prior =
            sum_is_uint64 ? static_cast<platform::Int128>(uint_total)
                          : static_cast<platform::Int128>(int_total);
        const platform::Int128 input =
            value.IsUnsigned()
                ? static_cast<platform::Int128>(
                      static_cast<uint64_t>(value.value.int_value))
                : static_cast<platform::Int128>(value.value.int_value);
        const platform::Int128 next = prior + input;
        constexpr platform::Int128 max_uint64 =
            std::numeric_limits<uint64_t>::max();
        if (next < 0 || next > max_uint64) {
          return StatusError(StatusCode::kIsInfinity, "uint64 overflow in SUM");
        }
        sum_is_uint64 = true;
        uint_total = static_cast<uint64_t>(next);
        int_total = 0;
      } else if (__builtin_add_overflow(int_total, value.value.int_value,
                                        &int_total)) {
        return StatusError(StatusCode::kIsInfinity, "integer overflow in SUM");
      }
    }
    if (!saw_double) {
      return sum_is_uint64
                 ? Value(static_cast<int64_t>(uint_total)).WithUnsigned()
                 : Value(int_total);
    }
    return Value(double_total + (sum_is_uint64
                                     ? static_cast<double>(uint_total)
                                     : static_cast<double>(int_total)));
  }
  if (fn == "MIN" || fn == "MAX") {
    for (const Value& value : non_null) {
      if (value.type == ValueType::kDouble &&
          std::isnan(value.value.double_value)) {
        return value;
      }
    }
    const Value* best = non_null.data();
    for (const Value& value : non_null) {
      if (fn == "MIN" ? ValueLess(value, *best, true)
                      : ValueLess(*best, value, true)) {
        best = &value;
      }
    }
    return *best;
  }
  if (fn == "ARRAY_AGG") {
    std::vector<Value> final_values =
        window.ignore_nulls ? std::move(non_null) : std::move(values);
    if (window.inner_limit.has_value() &&
        final_values.size() > *window.inner_limit) {
      final_values.resize(*window.inner_limit);
    }
    if (final_values.empty()) {
      return Value();
    }
    std::string element_type;
    for (const Value& value : final_values) {
      if (!value.IsNull()) {
        element_type = ElementSqlTypeOf(value);
        break;
      }
    }
    return Value::Array(std::move(final_values), std::move(element_type));
  }
  if (fn == "LOGICAL_AND" || fn == "LOGICAL_OR") {
    bool saw_true = false;
    bool saw_false = false;
    for (const Value& value : non_null) {
      if (Truthy(value)) {
        saw_true = true;
      } else {
        saw_false = true;
      }
    }
    if (fn == "LOGICAL_AND") {
      if (saw_false) {
        return Value(static_cast<int64_t>(0));
      }
      return Value(static_cast<int64_t>(1));
    }
    if (saw_true) {
      return Value(static_cast<int64_t>(1));
    }
    return Value(static_cast<int64_t>(0));
  }
  if (fn == "BIT_AND" || fn == "BIT_OR" || fn == "BIT_XOR") {
    for (const Value& value : non_null) {
      if (value.type != ValueType::kInt64) {
        return StatusError(StatusCode::kInvalidArgument,
                           fn + " requires an integer argument");
      }
    }
    bool bit_is_uint64 = false;
    for (const Value& value : non_null) {
      bit_is_uint64 = bit_is_uint64 || value.IsUnsigned();
    }
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
    // Match the grouped accumulator: a UINT64-tagged input keeps the result
    // in the unsigned domain instead of reinterpreting bit 63 as a sign.
    return bit_is_uint64 ? Value(acc).WithUnsigned() : Value(acc);
  }
  const auto numeric_of = [&](const Value& v) -> StatusOr<double> {
    if (v.type == ValueType::kDouble) {
      return v.value.double_value;
    }
    if (v.type == ValueType::kInt64) {
      return v.IsUnsigned()
                 ? static_cast<double>(static_cast<uint64_t>(v.value.int_value))
                 : static_cast<double>(v.value.int_value);
    }
    return StatusError(StatusCode::kInvalidArgument,
                       fn + " requires numeric arguments");
  };
  Status pairs_error{Status::kSuccess};
  struct FramePairStats {
    double covar_pop = 0;  // population covariance of (arg0, arg1)
    double var_arg0 = 0;   // population variance of the first argument
    double var_arg1 = 0;   // population variance of the second argument
    double mean_arg0 = 0;
    double mean_arg1 = 0;
    size_t n = 0;  // aligned non-NULL pair count
  };
  auto frame_pairs = [&]() -> std::optional<FramePairStats> {
    // Evaluates both arguments per aligned row (NULL on either side skips
    // the pair), over the frame rows that survive EXCLUDE and the WHERE
    // filter.  The pair count is exact even below 2; REGR_COUNT uses it.
    std::vector<std::pair<double, double>> pairs;
    StatusOr<std::vector<size_t>> survivors_or =
        FrameSurvivors(window, rows, ordered, order_values, current, lo, hi);
    if (!survivors_or.HasValue()) {
      pairs_error = survivors_or.GetStatus();
      return std::nullopt;
    }
    pairs.reserve(survivors_or.Value().size());
    for (const size_t r : survivors_or.Value()) {
      const size_t row = ordered[r];
      const Scope pair_scope{
          .row = &rows[row], .schema = schema, .outer = outer};
      StatusOr<Value> x_or =
          TryEvaluate(window.args[0], pair_scope, nullptr, context, *ctes);
      if (!x_or.HasValue()) {
        pairs_error = x_or.GetStatus();
        return std::nullopt;
      }
      Value x = x_or.MoveValue();
      if (x.IsNull()) {
        continue;
      }
      auto x_numeric = numeric_of(x);
      if (!x_numeric.HasValue()) {
        pairs_error = x_numeric.GetStatus();
        return std::nullopt;
      }
      const double xv = x_numeric.Value();
      double yv = xv;
      if (window.args.size() > 1) {
        StatusOr<Value> y_or =
            TryEvaluate(window.args[1], pair_scope, nullptr, context, *ctes);
        if (!y_or.HasValue()) {
          pairs_error = y_or.GetStatus();
          return std::nullopt;
        }
        Value y = y_or.MoveValue();
        if (y.IsNull()) {
          continue;
        }
        auto y_numeric = numeric_of(y);
        if (!y_numeric.HasValue()) {
          pairs_error = y_numeric.GetStatus();
          return std::nullopt;
        }
        yv = y_numeric.Value();
      }
      pairs.emplace_back(xv, yv);
    }
    const size_t n = pairs.size();
    FramePairStats stats;
    stats.n = n;
    if (n == 0) {
      return stats;
    }
    double sx = 0, sy = 0, sxx = 0, syy = 0, sxy = 0;
    for (const auto& [xv, yv] : pairs) {
      sx += xv;
      sy += yv;
      sxx += xv * xv;
      syy += yv * yv;
      sxy += xv * yv;
    }
    const double mx = sx / static_cast<double>(n);
    const double my = sy / static_cast<double>(n);
    stats.mean_arg0 = mx;
    stats.mean_arg1 = my;
    stats.covar_pop = (sxy / static_cast<double>(n)) - (mx * my);
    stats.var_arg0 = (sxx / static_cast<double>(n)) - (mx * mx);
    stats.var_arg1 = (syy / static_cast<double>(n)) - (my * my);
    return stats;
  };
  auto frame_variance = [&](bool samp) -> StatusOr<Value> {
    if (non_null.empty()) {
      return Value();
    }
    const size_t n = non_null.size();
    double mean = 0.0;
    for (const Value& value : non_null) {
      ASSIGN_OR_RETURN(double, number, numeric_of(value));
      mean += number;
    }
    mean /= static_cast<double>(n);
    double ss = 0.0;
    for (const Value& value : non_null) {
      ASSIGN_OR_RETURN(double, number, numeric_of(value));
      const double d = number - mean;
      ss += d * d;
    }
    double variance = ss / static_cast<double>(n);
    if (samp) {
      if (n < 2) {
        return Value();
      }
      variance *= static_cast<double>(n) / static_cast<double>(n - 1);
    }
    return Value(variance);
  };
  if (fn == "VAR_POP" || fn == "VAR_SAMP" || fn == "VARIANCE") {
    return frame_variance(fn != "VAR_POP");
  }
  if (fn == "STDDEV_POP" || fn == "STDDEV_SAMP" || fn == "STDDEV") {
    ASSIGN_OR_RETURN(Value, variance, frame_variance(fn != "STDDEV_POP"));
    if (variance.IsNull()) {
      return variance;
    }
    return Value(std::sqrt(variance.value.double_value));
  }
  if (fn == "ANY_VALUE") {
    return non_null[0];
  }
  if (fn == "COVAR_POP" || fn == "COVAR_SAMP" || fn == "CORR") {
    auto stats = frame_pairs();
    if (pairs_error != Status::kSuccess) {
      return pairs_error;
    }
    if (!stats.has_value() || stats->n < 2) {
      return Value();
    }
    const auto& [covar_pop, var_x, var_y, mean_x, mean_y, n] = *stats;
    (void)mean_x;
    (void)mean_y;
    if (fn == "COVAR_POP") {
      return Value(covar_pop);
    }
    if (fn == "COVAR_SAMP") {
      return Value(covar_pop * static_cast<double>(n) /
                   static_cast<double>(n - 1));
    }
    // CORR
    if (var_x <= 0.0 || var_y <= 0.0) {
      return Value();
    }
    return Value(covar_pop / std::sqrt(var_x * var_y));
  }
  if (fn == "REGR_SLOPE" || fn == "REGR_INTERCEPT" || fn == "REGR_COUNT" ||
      fn == "REGR_R2" || fn == "REGR_AVGX" || fn == "REGR_AVGY" ||
      fn == "REGR_SXX" || fn == "REGR_SYY" || fn == "REGR_SXY") {
    // REGR_*(y, x): y is the first argument, x the second; all statistics
    // mirror the grouped accumulator (population sums scaled by n).
    auto stats = frame_pairs();
    if (pairs_error != Status::kSuccess) {
      return pairs_error;
    }
    if (!stats.has_value()) {
      return Value();
    }
    // arg0 = y (first argument), arg1 = x (second argument).
    const double covar_pop = stats->covar_pop;
    const double var_x = stats->var_arg1;
    const double var_y = stats->var_arg0;
    const double mean_x = stats->mean_arg1;
    const double mean_y = stats->mean_arg0;
    const size_t n = stats->n;
    if (fn == "REGR_COUNT") {
      return Value(static_cast<int64_t>(n));
    }
    if (n < 2) {
      return Value();
    }
    const double sxy_total = covar_pop * static_cast<double>(n);
    const double sxx_total = var_x * static_cast<double>(n);
    const double syy_total = var_y * static_cast<double>(n);
    if (fn == "REGR_SXX") {
      return Value(sxx_total);
    }
    if (fn == "REGR_SYY") {
      return Value(syy_total);
    }
    if (fn == "REGR_SXY") {
      return Value(sxy_total);
    }
    if (fn == "REGR_AVGX") {
      return Value(mean_x);
    }
    if (fn == "REGR_AVGY") {
      return Value(mean_y);
    }
    if (var_x <= 0.0) {
      return Value();
    }
    const double slope = sxy_total / sxx_total;
    if (fn == "REGR_SLOPE") {
      return Value(slope);
    }
    if (fn == "REGR_INTERCEPT") {
      return Value(mean_y - (slope * mean_x));
    }
    // REGR_R2: undefined when either side is constant.
    if (var_y <= 0.0) {
      return Value();
    }
    return Value((sxy_total * sxy_total) / (sxx_total * syy_total));
  }
  if (fn == "MODE") {
    // Most frequent non-NULL frame value; ties keep the first value to
    // reach the maximum count (same deterministic choice as the grouped
    // accumulator).
    Value best;
    int64_t best_count = -1;
    for (size_t i = 0; i < non_null.size(); ++i) {
      int64_t seen = 0;
      for (size_t j = 0; j <= i; ++j) {
        if (ValuesEqual(non_null[j], non_null[i])) {
          ++seen;
        }
      }
      if (seen > best_count) {
        best_count = seen;
        best = non_null[i];
      }
    }
    return best;
  }
  if (fn == "PERCENTILE_CONT" || fn == "PERCENTILE_DISC") {
    // PERCENTILE_CONT(value, p) OVER (ORDER BY ...): p in [0,1] constant.
    std::vector<Value> frame_values = non_null;
    if (window.respect_nulls) {
      // NULLs join the frame and sort before every value. `values` cannot be
      // reused here: the non-NULL entries were moved into `non_null` above,
      // leaving moved-from NULL shells. Rebuild from the intact values plus
      // the original NULL slots instead.
      frame_values = non_null;
      frame_values.insert(frame_values.end(), values.size() - non_null.size(),
                          Value());
    }
    if (frame_values.empty() || window.args.size() < 2) {
      return Value();
    }
    ASSIGN_OR_RETURN(Value, p_value,
                     (TryEvaluate(window.args[1],
                                  (Scope{.row = &rows[ordered[lo]],
                                         .schema = schema,
                                         .outer = outer}),
                                  nullptr, context, *ctes)));
    if (p_value.IsNull()) {
      return Value();
    }
    double p = 0.0;
    if (p_value.type == ValueType::kDouble) {
      p = p_value.value.double_value;
    } else if (p_value.type == ValueType::kInt64) {
      p = static_cast<double>(p_value.value.int_value);
    } else {
      // std::stod silently ignores trailing garbage unless the consumed-byte
      // count is checked; '0.5junk' must not be accepted as a valid p.
      const std::string text(p_value.value.varchar_value);
      try {
        size_t consumed = 0;
        p = std::stod(text, &consumed);
        if (consumed != text.size()) {
          return Value();
        }
      } catch (...) {
        return Value();
      }
    }
    if (!std::isfinite(p) || p < 0.0 || p > 1.0) {
      return StatusError(StatusCode::kInvalidArgument,
                         "PERCENTILE argument must be between 0 and 1");
    }
    struct PercentileValue {
      Value value;
      std::optional<double> parsed;
    };
    std::vector<PercentileValue> sorted;
    sorted.reserve(frame_values.size());
    for (Value& value : frame_values) {
      std::optional<double> parsed;
      if (value.type == ValueType::kVarChar && !value.IsInterval()) {
        try {
          const std::string text(value.AsStringView());
          size_t consumed = 0;
          const double candidate = std::stod(text, &consumed);
          if (consumed == text.size()) {
            parsed = candidate;
          }
        // NOLINTNEXTLINE(bugprone-empty-catch): keep nonnumeric text fallback.
        } catch (const std::invalid_argument&) {  // NOLINT(bugprone-empty-catch)
          // Nonnumeric text keeps its text sort key.
        // NOLINTNEXTLINE(bugprone-empty-catch): keep out-of-range text fallback.
        } catch (const std::out_of_range&) {  // NOLINT(bugprone-empty-catch)
          // An unrepresentable number also keeps its text sort key.
        }
      }
      sorted.push_back({std::move(value), parsed});
    }
    // Give every value one fixed sort key. Pairwise parsing with a text
    // fallback can order the same value differently in different comparisons.
    std::sort(sorted.begin(), sorted.end(), [](const auto& a, const auto& b) {
      const Value a_key = a.parsed ? Value(*a.parsed) : a.value;
      const Value b_key = b.parsed ? Value(*b.parsed) : b.value;
      if (a_key.IsNull() || b_key.IsNull()) {
        return a_key.IsNull() && !b_key.IsNull();
      }
      const bool a_nan = a_key.type == ValueType::kDouble &&
                         std::isnan(a_key.value.double_value);
      const bool b_nan = b_key.type == ValueType::kDouble &&
                         std::isnan(b_key.value.double_value);
      if (a_nan || b_nan) {
        return a_nan && !b_nan;
      }
      return CompareForOrderBy(a_key, b_key) < 0;
    });
    const double rank = p * static_cast<double>(sorted.size() - 1);
    if (fn == "PERCENTILE_DISC") {
      // PERCENTILE_DISC rounds the rank UP to the next element.
      return sorted[static_cast<size_t>(std::ceil(rank))].value;
    }
    const auto lo_idx = static_cast<size_t>(std::floor(rank));
    const auto hi_idx = static_cast<size_t>(std::ceil(rank));
    if (lo_idx == hi_idx) {
      const Value& hit = sorted[lo_idx].value;
      if (hit.IsNull()) {
        return Value();
      }
      // PERCENTILE_CONT interpolates in double space, so numeric input
      // always yields FLOAT64 even when the rank lands exactly on an
      // element.  Non-numeric elements (e.g. numeric strings) keep their
      // own type.
      if (hit.type == ValueType::kInt64 || hit.type == ValueType::kDouble) {
        return Value(NumericOf(hit));
      }
      return hit;
    }
    // Interpolation across a NULL endpoint yields NULL.
    if (sorted[lo_idx].value.IsNull() || sorted[hi_idx].value.IsNull()) {
      return Value();
    }
    const double a =
        sorted[lo_idx].parsed.value_or(NumericOf(sorted[lo_idx].value));
    const double b =
        sorted[hi_idx].parsed.value_or(NumericOf(sorted[hi_idx].value));
    const double frac = rank - static_cast<double>(lo_idx);
    // a*(1-frac) + b*frac (NOT a + (b-a)*frac): the former keeps infinite
    // endpoints as-is instead of collapsing to NaN.
    return Value((a * (1.0 - frac)) + (b * frac));
  }
  if (fn == "ELEMENTWISE_SUM" || fn == "ELEMENTWISE_AVG") {
    const bool is_avg = fn == "ELEMENTWISE_AVG";
    bool saw_array = false;
    size_t width = 0;
    std::string input_elem_type;
    for (const Value& value : non_null) {
      if (!value.IsNull() && value.IsArray()) {
        saw_array = true;
        if (input_elem_type.empty()) {
          input_elem_type = value.ArrayElementSqlType();
        }
        width = std::max(width, value.ArrayElements().size());
      }
    }
    if (!saw_array) {
      return Value();
    }
    // The grouped accumulator labels the result from the first input
    // array's declared element type; keep the same mapping so both paths
    // agree on ARRAY<UINT64>/ARRAY<DOUBLE>/ARRAY<INT64>.
    auto mapped_elem_type = [](const std::string& input) -> const char* {
      std::string upper;
      upper.reserve(input.size());
      for (char c : input) {
        upper.push_back(
            static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
      }
      if (upper.find("UINT") != std::string::npos) {
        return "UINT64";
      }
      if (upper.find("DOUBLE") != std::string::npos ||
          upper.find("FLOAT") != std::string::npos) {
        return "DOUBLE";
      }
      return "INT64";
    };
    std::vector<Value> out;
    out.reserve(width);
    bool any_position_double = false;
    for (size_t pos = 0; pos < width; ++pos) {
      bool any_double = false;
      int64_t int_sum = 0;
      double double_sum = 0.0;
      int64_t seen = 0;
      for (const Value& value : non_null) {
        if (value.IsNull() || !value.IsArray()) {
          continue;
        }
        const auto& elements = value.ArrayElements();
        if (pos >= elements.size() || elements[pos].IsNull()) {
          continue;
        }
        const Value& element = elements[pos];
        ++seen;
        if (element.type == ValueType::kDouble) {
          any_double = true;
          double_sum += element.value.double_value;
        } else if (element.type == ValueType::kInt64 ||
                   element.type == ValueType::kDate) {
          // Same contract as the grouped accumulator: an INT64 lane raises
          // on overflow for SUM until a DOUBLE sibling widens it (AVG always
          // finishes in double, so it spills the addend instead).
          int64_t updated = 0;
          if (__builtin_add_overflow(int_sum, element.value.int_value,
                                     &updated)) {
            if (!is_avg && !any_double) {
              return StatusError(StatusCode::kIsInfinity,
                                 "integer overflow in ELEMENTWISE_SUM");
            }
            double_sum += static_cast<double>(element.value.int_value);
          } else {
            int_sum = updated;
          }
        } else {
          // VARCHAR/ARRAY/other payloads have no int_value to sum; reading
          // the union member anyway produced meaningless totals.
          return StatusError(StatusCode::kInvalidArgument,
                             "numeric value required");
        }
      }
      any_position_double = any_position_double || any_double;
      if (seen == 0) {
        out.emplace_back();
        continue;
      }
      if (is_avg) {
        out.emplace_back(
            (double_sum +
             static_cast<double>(int_sum)) /  // NOLINT(modernize-use-emplace)
            static_cast<double>(seen));
      } else if (any_double) {
        out.emplace_back(
            double_sum +
            static_cast<double>(int_sum));  // NOLINT(modernize-use-emplace)
      } else {
        out.emplace_back(Value(int_sum));  // NOLINT(modernize-use-emplace)
      }
    }
    // The label must cover every emitted element: a position that saw a
    // DOUBLE emits kDouble even when earlier positions stayed INT64, and AVG
    // is always DOUBLE -- matching the grouped accumulator's Finish.
    const std::string element_type =
        is_avg ? "DOUBLE"
               : (any_position_double ? "DOUBLE"
                                      : mapped_elem_type(input_elem_type));
    return Value::Array(std::move(out), element_type);
  }
  return StatusError(StatusCode::kInvalidArgument,
                     "unsupported window aggregate " + fn);
}

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

bool WindowLayoutNeedsOrderValues(const WindowFunctionCallExpression& window,
                                  bool* need_peer_end) {
  if (window.function == "ROW_NUMBER") {
    *need_peer_end = false;
    return false;
  }
  // The rank family is defined by peer groups, not by the frame: the general
  // evaluator indexes peer_end unconditionally, so even a pure ROWS frame
  // (which needs no peer boundaries for aggregates) must keep them here.
  if (window.function == "RANK" || window.function == "DENSE_RANK" ||
      window.function == "PERCENT_RANK" || window.function == "CUME_DIST") {
    *need_peer_end = true;
    return true;
  }
  const bool pure_rows_frame =
      window.has_frame && window.frame_unit == WindowFrameUnit::kRows &&
      !window.distinct && !window.where_filter &&
      window.inner_order_by.empty() && !window.inner_limit &&
      window.exclusion == WindowFrameExclusion::kNone &&
      !window.frame_start.offset && !window.frame_end.offset;
  if (pure_rows_frame) {
    *need_peer_end = false;
    return false;
  }
  *need_peer_end = true;
  return true;
}

StatusOr<WindowOrderLayout> BuildWindowOrderLayout(
    const WindowFunctionCallExpression& window, std::vector<Row>& rows,
    const WindowRuntime& runtime, std::optional<size_t> rank_bound,
    bool need_order_values, bool need_peer_end) {
  double* layout_ms = nullptr;
  if (auto* runtime_stats = runtime.context.execution_runtime()) {
    layout_ms = &runtime_stats->window_layout_ms;
  }
  PhaseTimer layout_timer(layout_ms);
  WindowOrderLayout layout;
  const auto column_offset =
      [&](const Expression& expression) -> std::optional<size_t> {
    if (!expression || expression->Type() != TypeTag::kColumnValue) {
      return std::nullopt;
    }
    const auto& name = expression->AsColumnValue().GetColumnName();
    if (name.name.find('.') != std::string::npos ||
        name.schema.find('.') != std::string::npos) {
      return std::nullopt;
    }
    auto offset = LocalColumnOffset(*runtime.schema, name);
    // Do not turn a row alias or a nested field into a scalar column.
    if (offset && runtime.schema->GetColumn(*offset).Name().name == name.name) {
      return offset;
    }
    return std::nullopt;
  };
  std::vector<std::optional<size_t>> partition_offsets;
  partition_offsets.reserve(window.partition_by.size());
  for (const auto& key : window.partition_by) {
    partition_offsets.push_back(column_offset(key));
  }
  std::vector<std::optional<size_t>> order_offsets;
  order_offsets.reserve(window.order_by.size());
  for (const auto& term : window.order_by) {
    order_offsets.push_back(column_offset(term.expression));
  }
  // Group the rows by their PARTITION BY tuple through a hash table. The
  // groups are appended in first-seen order, so the layout (and therefore every
  // output position) is the same as a scan that searched the groups found so
  // far; only the search is O(1) instead of O(partitions). A window over many
  // small partitions -- a per-customer rank over a whole sales table -- made
  // that linear search the dominant term.
  //
  // Equality stays EqualUnderCollation (see PartitionKeyEq): CI-tagged
  // VARCHARs fold like the direct-partition fast path and the OOC path, so
  // 'A'/'a' under COLLATE(s,'und:ci') land in the same partition everywhere.
  std::unordered_map<std::vector<Value>, size_t, PartitionKeyHash,
                     PartitionKeyEq>
      groups;
  bool integer_partition = false;
  size_t integer_column = 0;
  const auto single_partition =
      partition_offsets.size() == 1 ? partition_offsets.front() : std::nullopt;
  bool prove_integer_partition = false;
  ValueType partition_type = ValueType::kNull;
  if (single_partition) {
    integer_column = *single_partition;
    const auto& column = runtime.schema->GetColumn(integer_column);
    partition_type = column.Type();
    prove_integer_partition =
        !column.IsUnsigned() && (partition_type == ValueType::kInt64 ||
                                 partition_type == ValueType::kDate);
  }
  bool prove_direct_order =
      !order_offsets.empty() &&
      std::ranges::all_of(
          order_offsets, [](const auto& offset) { return offset.has_value(); });
  const auto integer_order_slot =
      prove_direct_order && order_offsets.size() == 1 ? order_offsets.front()
                                                      : std::nullopt;
  bool prove_integer_order = false;
  bool prove_double_order = false;
  ValueType order_type = ValueType::kNull;
  if (integer_order_slot) {
    const auto& column = runtime.schema->GetColumn(*integer_order_slot);
    order_type = column.Type();
    prove_integer_order =
        !column.IsUnsigned() &&
        (order_type == ValueType::kInt64 || order_type == ValueType::kDate);
    // A schema/runtime mismatch can still be an all-double column. Prove both
    // and keep double only when the integer proof fails, matching the old
    // separate scans.
    prove_double_order = true;
  }
  if (prove_integer_order || prove_double_order || prove_direct_order) {
    if (runtime.uniform_cell_types) {
      if (prove_direct_order) {
        for (const auto& offset : order_offsets) {
          const ValueType vt = runtime.schema->GetColumn(*offset).Type();
          if (vt != ValueType::kNull && vt != ValueType::kInt64 &&
              vt != ValueType::kDate && vt != ValueType::kDouble) {
            prove_direct_order = false;
            break;
          }
        }
      }
      if (integer_order_slot) {
        const auto& column = runtime.schema->GetColumn(*integer_order_slot);
        prove_integer_order = prove_direct_order && !column.IsUnsigned() &&
                              (column.Type() == ValueType::kInt64 ||
                               column.Type() == ValueType::kDate);
        prove_double_order = prove_direct_order && !prove_integer_order &&
                             (column.Type() == ValueType::kDouble ||
                              column.Type() == ValueType::kNull);
      }
    }
  }
  const bool need_order_scan =
      !runtime.uniform_cell_types &&
      (prove_direct_order || prove_integer_order || prove_double_order);
  // One pass proves every numeric fast path. Separate scans reread the same
  // rows once per flag; a window over hundreds of thousands of groups was
  // spending the layout on those proofs. The same pass records the integer
  // partition domain so a compact key can use an array instead of a hash.
  int64_t partition_min = 0;
  int64_t partition_max = 0;
  bool saw_partition_key = false;
  if (prove_integer_partition || need_order_scan) {
    const size_t prove_workers =
        rows.size() >= 16384
            ? std::min<size_t>({8, rows.size() / 4096,
                                QueryParallelWorkerLimit(
                                    std::thread::hardware_concurrency())})
            : 1;
    if (prove_workers > 1) {
      struct ProveState {
        bool int_part{false};
        bool direct_ord{false};
        bool int_ord{false};
        bool dbl_ord{false};
        bool saw{false};
        int64_t min_k{std::numeric_limits<int64_t>::max()};
        int64_t max_k{std::numeric_limits<int64_t>::min()};
      };
      std::vector<ProveState> states(prove_workers);
      RETURN_IF_FAIL(RunParallelTasks(prove_workers, [&](size_t w) {
        const size_t begin = (rows.size() * w) / prove_workers;
        const size_t end = (rows.size() * (w + 1)) / prove_workers;
        ProveState s;
        s.int_part = prove_integer_partition;
        s.direct_ord = need_order_scan && prove_direct_order;
        s.int_ord = need_order_scan && prove_integer_order;
        s.dbl_ord = need_order_scan && prove_double_order;
        for (size_t idx = begin; idx < end; ++idx) {
          const Row& row = rows[idx];
          if (s.int_part) {
            const Value& value = row[integer_column];
            if (!value.IsNull() && !runtime.uniform_cell_types &&
                (value.type != partition_type || value.IsUnsigned())) {
              s.int_part = false;
            } else if (!value.IsNull()) {
              if (!s.saw) {
                s.min_k = value.value.int_value;
                s.max_k = value.value.int_value;
                s.saw = true;
              } else {
                s.min_k = std::min(s.min_k, value.value.int_value);
                s.max_k = std::max(s.max_k, value.value.int_value);
              }
            }
          }
          if (s.direct_ord) {
            for (const auto& offset : order_offsets) {
              const auto type = row[*offset].type;
              if (type != ValueType::kNull && type != ValueType::kInt64 &&
                  type != ValueType::kDate && type != ValueType::kDouble) {
                s.direct_ord = false;
                s.int_ord = false;
                s.dbl_ord = false;
                break;
              }
            }
          }
          if (s.int_ord) {
            const Value& value = row[*integer_order_slot];
            if (!value.IsNull() &&
                (value.type != order_type || value.IsUnsigned())) {
              s.int_ord = false;
            }
          }
          if (s.dbl_ord) {
            const Value& value = row[*integer_order_slot];
            if (!value.IsNull() && value.type != ValueType::kDouble) {
              s.dbl_ord = false;
            }
          }
          if (!s.int_part && !s.direct_ord && !s.int_ord && !s.dbl_ord) {
            break;
          }
        }
        states[w] = s;
      }));
      for (const ProveState& s : states) {
        prove_integer_partition &= s.int_part;
        if (need_order_scan) {
          prove_direct_order &= s.direct_ord;
          prove_integer_order &= s.int_ord;
          prove_double_order &= s.dbl_ord;
        }
        if (s.saw) {
          if (!saw_partition_key) {
            partition_min = s.min_k;
            partition_max = s.max_k;
            saw_partition_key = true;
          } else {
            partition_min = std::min(partition_min, s.min_k);
            partition_max = std::max(partition_max, s.max_k);
          }
        }
      }
    } else {
      for (const Row& row : rows) {
        if (prove_integer_partition) {
          const Value& value = row[integer_column];
          if (!value.IsNull() && !runtime.uniform_cell_types &&
              (value.type != partition_type || value.IsUnsigned())) {
            prove_integer_partition = false;
          } else if (!value.IsNull()) {
            if (!saw_partition_key) {
              partition_min = value.value.int_value;
              partition_max = value.value.int_value;
              saw_partition_key = true;
            } else {
              partition_min = std::min(partition_min, value.value.int_value);
              partition_max = std::max(partition_max, value.value.int_value);
            }
          }
        }
        if (need_order_scan && prove_direct_order) {
          for (const auto& offset : order_offsets) {
            const auto type = row[*offset].type;
            if (type != ValueType::kNull && type != ValueType::kInt64 &&
                type != ValueType::kDate && type != ValueType::kDouble) {
              prove_direct_order = false;
              prove_integer_order = false;
              prove_double_order = false;
              break;
            }
          }
        }
        if (need_order_scan && prove_integer_order) {
          const Value& value = row[*integer_order_slot];
          if (!value.IsNull() &&
              (value.type != order_type || value.IsUnsigned())) {
            prove_integer_order = false;
          }
        }
        if (need_order_scan && prove_double_order) {
          const Value& value = row[*integer_order_slot];
          if (!value.IsNull() && value.type != ValueType::kDouble) {
            prove_double_order = false;
          }
        }
        if (!prove_integer_partition &&
            (!need_order_scan || (!prove_direct_order && !prove_integer_order &&
                                  !prove_double_order))) {
          break;
        }
      }
    }
  }
  integer_partition = prove_integer_partition;
  int64_t dense_partition_lo = 0;
  bool use_dense_partition = false;
  if (integer_partition && saw_partition_key &&
      partition_max >= partition_min) {
    const uint64_t span = static_cast<uint64_t>(partition_max) -
                          static_cast<uint64_t>(partition_min);
    if (span <= 1000000 &&
        (rows.empty() || span <= rows.size() * 4 || span <= 65536)) {
      dense_partition_lo = partition_min;
      use_dense_partition = true;
    }
  }
  const bool direct_order = prove_direct_order;
  const bool integer_order = prove_integer_order && direct_order;
  const bool double_order =
      prove_double_order && direct_order && !integer_order;
  const bool direct_partition = std::ranges::all_of(
      partition_offsets, [](const auto& offset) { return offset.has_value(); });
  std::vector<size_t> direct_partition_columns;
  std::vector<Collation> partition_collations;
  partition_collations.reserve(window.partition_by.size());
  for (const auto& pexpr : window.partition_by) {
    partition_collations.push_back(CollationOfExpression(pexpr));
  }
  for (const auto& offset : partition_offsets) {
    if (offset) {
      direct_partition_columns.push_back(*offset);
    }
  }
  // Input rows remain immutable while the partition layout is built. Store a
  // representative row index, and compare its values, instead of allocating
  // and copying a key tuple for every input row (including duplicate keys).
  // Hash and equality apply the *same* per-value transform
  // `ApplyKeyCollation(v, col)`: a value's own COLLATE tag folds it, and an
  // untagged value stays byte-identical (the branch below only skips that
  // identity call). The two can therefore never disagree about a pair even
  // when tagged and untagged rows are mixed in one column -- mixing splits the
  // pair exactly as the generic path's pre-folded keys do, so the radix
  // scatter cannot place one partition in two buckets.
  const auto hash_direct_partition = [&](size_t row) {
    uint64_t hash = 0x9e3779b97f4a7c15ULL;
    for (size_t idx = 0; idx < direct_partition_columns.size(); ++idx) {
      const Value& raw = rows[row][direct_partition_columns[idx]];
      const Collation col = partition_collations[idx];
      if (col != Collation::kNone || raw.Collation() != 0) {
        const Value folded = ApplyKeyCollation(raw, col);
        hash ^= std::hash<Value>{}(folded) + 0x9e3779b97f4a7c15ULL +
                (hash << 6) + (hash >> 2);
      } else {
        hash ^= std::hash<Value>{}(raw) + 0x9e3779b97f4a7c15ULL + (hash << 6) +
                (hash >> 2);
      }
    }
    return static_cast<size_t>(hash);
  };
  const auto equal_direct_partition = [&](size_t a, size_t b) {
    for (size_t idx = 0; idx < direct_partition_columns.size(); ++idx) {
      const size_t column = direct_partition_columns[idx];
      const Collation col = partition_collations[idx];
      const Value& va = rows[a][column];
      const Value& vb = rows[b][column];
      if (col != Collation::kNone || va.Collation() != 0 ||
          vb.Collation() != 0) {
        if (!ValuesEqual(ApplyKeyCollation(va, col),
                         ApplyKeyCollation(vb, col))) {
          return false;
        }
      } else if (!ValuesEqual(va, vb)) {
        return false;
      }
    }
    return true;
  };
  std::unordered_map<size_t, size_t, decltype(hash_direct_partition),
                     decltype(equal_direct_partition)>
      direct_groups(0, hash_direct_partition, equal_direct_partition);
  FlatIntKeyMap<size_t> integer_groups;
  std::optional<size_t> null_partition;
  const auto partition_started = std::chrono::steady_clock::now();
  const size_t part_workers =
      rows.size() >= 16384 && (integer_partition || direct_partition)
          ? std::min<size_t>(
                {8, rows.size() / 4096,
                 QueryParallelWorkerLimit(std::thread::hardware_concurrency())})
          : 1;
  std::vector<uint32_t> dense_partition;
  if (use_dense_partition && part_workers <= 1) {
    const uint64_t span = static_cast<uint64_t>(partition_max) -
                          static_cast<uint64_t>(partition_min);
    dense_partition.assign(static_cast<size_t>(span) + 1, UINT32_MAX);
  }
  if (partition_offsets.empty()) {
    layout.partitions.resize(1);
    layout.partitions[0].ordered.resize(rows.size());
    std::iota(layout.partitions[0].ordered.begin(),
              layout.partitions[0].ordered.end(), 0);
  } else if (part_workers > 1) {
    // Scatter rows into 64 hash/radix buckets in parallel, assign per-partition
    // counts inside each bucket cleanly, then scatter directly into a single
    // contiguous flat_ordered arena (eliminating thousands of per-partition
    // vector allocations and serial destructor loops).
    constexpr size_t kPartitionBuckets = 64;
    std::vector<std::array<std::vector<size_t>, kPartitionBuckets>>
        worker_buckets(part_workers);
    const size_t approx_per_bucket = std::max<size_t>(
        4, ((rows.size() / part_workers) / kPartitionBuckets) + 4);
    for (auto& buckets : worker_buckets) {
      for (auto& vec : buckets) {
        vec.reserve(approx_per_bucket);
      }
    }
    RETURN_IF_FAIL(RunParallelTasks(part_workers, [&](size_t w) {
      const size_t begin = (rows.size() * w) / part_workers;
      const size_t end = (rows.size() * (w + 1)) / part_workers;
      auto& buckets = worker_buckets[w];
      if (use_dense_partition) {
        for (size_t i = begin; i < end; ++i) {
          const Value& value = rows[i][integer_column];
          const size_t b =
              value.IsNull()
                  ? 0
                  : static_cast<size_t>(
                        (static_cast<uint64_t>(value.value.int_value) -
                         static_cast<uint64_t>(dense_partition_lo)) &
                        63ULL);
          buckets[b].push_back(i);
        }
      } else if (integer_partition) {
        for (size_t i = begin; i < end; ++i) {
          const Value& value = rows[i][integer_column];
          const size_t b =
              value.IsNull()
                  ? 0
                  : static_cast<size_t>(
                        (static_cast<uint64_t>(value.value.int_value) *
                         0x9e3779b97f4a7c15ULL) >>
                        58);
          buckets[b].push_back(i);
        }
      } else {
        for (size_t i = begin; i < end; ++i) {
          const uint64_t h = static_cast<uint64_t>(hash_direct_partition(i)) *
                             0x9e3779b97f4a7c15ULL;
          buckets[static_cast<size_t>(h >> 58)].push_back(i);
        }
      }
    }));
    std::array<size_t, kPartitionBuckets> bucket_base{};
    size_t total_flat = 0;
    for (size_t b = 0; b < kPartitionBuckets; ++b) {
      bucket_base[b] = total_flat;
      for (size_t tw = 0; tw < part_workers; ++tw) {
        total_flat += worker_buckets[tw][b].size();
      }
    }
    layout.flat_ordered.resize(total_flat);
    std::array<std::vector<WindowPartitionLayout>, kPartitionBuckets>
        bucket_parts;
    struct LocalBucketGroup {
      size_t count{0};
      size_t cursor{0};
      size_t first_row{0};
    };
    const size_t dense_bucket_slots =
        use_dense_partition
            ? static_cast<size_t>((static_cast<uint64_t>(partition_max) -
                                   static_cast<uint64_t>(dense_partition_lo)) >>
                                  6) +
                  1
            : 0;
    RETURN_IF_FAIL(RunParallelTasks(part_workers, [&](size_t w) {
      for (size_t b = w; b < kPartitionBuckets; b += part_workers) {
        size_t total_b = 0;
        for (size_t tw = 0; tw < part_workers; ++tw) {
          total_b += worker_buckets[tw][b].size();
        }
        if (total_b == 0) {
          continue;
        }
        std::vector<LocalBucketGroup> local_groups;
        std::vector<uint32_t> elem_group;
        elem_group.reserve(total_b);
        auto scatter_into_flat = [&](auto&& resolve_group) {
          for (size_t tw = 0; tw < part_workers; ++tw) {
            for (size_t i : worker_buckets[tw][b]) {
              const uint32_t g = resolve_group(i);
              ++local_groups[g].count;
              elem_group.push_back(g);
            }
          }
          auto& out = bucket_parts[b];
          out.reserve(local_groups.size());
          size_t pos = bucket_base[b];
          for (auto& grp : local_groups) {
            WindowPartitionLayout part;
            part.flat_begin = pos;
            part.flat_end = pos + grp.count;
            part.first_row = grp.first_row;
            out.push_back(std::move(part));
            grp.cursor = pos;
            pos += grp.count;
          }
          size_t e = 0;
          for (size_t tw = 0; tw < part_workers; ++tw) {
            for (size_t i : worker_buckets[tw][b]) {
              layout.flat_ordered[local_groups[elem_group[e++]].cursor++] = i;
            }
            std::vector<size_t>().swap(worker_buckets[tw][b]);
          }
        };
        if (use_dense_partition) {
          std::vector<uint32_t> slot_of(dense_bucket_slots, UINT32_MAX);
          uint32_t null_slot = UINT32_MAX;
          scatter_into_flat([&](size_t i) -> uint32_t {
            const auto& value = rows[i][integer_column];
            if (value.IsNull()) {
              if (null_slot == UINT32_MAX) {
                null_slot = static_cast<uint32_t>(local_groups.size());
                local_groups.push_back({0, 0, i});
              }
              return null_slot;
            }
            const auto local_idx = static_cast<size_t>(
                (static_cast<uint64_t>(value.value.int_value) -
                 static_cast<uint64_t>(dense_partition_lo)) >>
                6);
            uint32_t& slot = slot_of[local_idx];
            if (slot == UINT32_MAX) {
              slot = static_cast<uint32_t>(local_groups.size());
              local_groups.push_back({0, 0, i});
            }
            return slot;
          });
        } else if (integer_partition) {
          FlatIntKeyMap<uint32_t> local_int_groups;
          uint32_t local_null = UINT32_MAX;
          scatter_into_flat([&](size_t i) -> uint32_t {
            const auto& value = rows[i][integer_column];
            if (value.IsNull()) {
              if (local_null == UINT32_MAX) {
                local_null = static_cast<uint32_t>(local_groups.size());
                local_groups.push_back({0, 0, i});
              }
              return local_null;
            }
            bool inserted = false;
            auto* slot =
                local_int_groups.TryEmplace(value.value.int_value, &inserted);
            if (inserted) {
              *slot = static_cast<uint32_t>(local_groups.size());
              local_groups.push_back({0, 0, i});
            }
            return *slot;
          });
        } else {
          std::unordered_map<size_t, uint32_t, decltype(hash_direct_partition),
                             decltype(equal_direct_partition)>
              local_direct(0, hash_direct_partition, equal_direct_partition);
          scatter_into_flat([&](size_t i) -> uint32_t {
            const auto [entry, inserted] = local_direct.try_emplace(
                i, static_cast<uint32_t>(local_groups.size()));
            if (inserted) {
              local_groups.push_back({0, 0, i});
            }
            return entry->second;
          });
        }
      }
    }));
    size_t total_parts = 0;
    for (const auto& parts : bucket_parts) {
      total_parts += parts.size();
    }
    layout.partitions.reserve(total_parts);
    for (auto& parts : bucket_parts) {
      for (auto& p : parts) {
        layout.partitions.push_back(std::move(p));
      }
    }
    if (layout.partitions.size() > 1) {
      std::sort(
          layout.partitions.begin(), layout.partitions.end(),
          [](const WindowPartitionLayout& a, const WindowPartitionLayout& b) {
            return a.first_row < b.first_row;
          });
    }
  } else {
    for (size_t i = 0; i < rows.size(); ++i) {
      if (integer_partition) {
        const auto& value = rows[i][integer_column];
        size_t group = 0;
        if (value.IsNull()) {
          if (!null_partition) {
            null_partition = layout.partitions.size();
            layout.partitions.emplace_back();
          }
          group = *null_partition;
        } else if (use_dense_partition) {
          uint32_t& slot = dense_partition[static_cast<size_t>(
              value.value.int_value - dense_partition_lo)];
          if (slot == UINT32_MAX) {
            slot = static_cast<uint32_t>(layout.partitions.size());
            layout.partitions.emplace_back();
          }
          group = slot;
        } else {
          bool inserted = false;
          auto* slot =
              integer_groups.TryEmplace(value.value.int_value, &inserted);
          if (inserted) {
            *slot = layout.partitions.size();
            layout.partitions.emplace_back();
          }
          group = *slot;
        }
        layout.partitions[group].ordered.push_back(i);
        continue;
      }
      if (direct_partition) {
        const auto [entry, inserted] =
            direct_groups.try_emplace(i, layout.partitions.size());
        if (inserted) {
          layout.partitions.emplace_back();
        }
        layout.partitions[entry->second].ordered.push_back(i);
        continue;
      }
      std::vector<Value> keys;
      keys.reserve(window.partition_by.size());
      for (size_t k = 0; k < window.partition_by.size(); ++k) {
        if (const auto offset = partition_offsets[k]) {
          keys.push_back(
              ApplyKeyCollation(rows[i][*offset], partition_collations[k]));
        } else {
          ASSIGN_OR_RETURN(Value, partition_key,
                           runtime.EvalAt(window.partition_by[k], rows, i));
          keys.push_back(
              ApplyKeyCollation(partition_key, partition_collations[k]));
        }
      }
      const auto [entry, inserted] =
          groups.try_emplace(std::move(keys), layout.partitions.size());
      if (inserted) {
        layout.partitions.emplace_back();
      }
      layout.partitions[entry->second].ordered.push_back(i);
    }
  }
  if (::tinylamb::platform::GetEnv("TINYLAMB_GROUP_PHASES") != nullptr &&
      rows.size() >= 100000) {
    const double ms = std::chrono::duration<double, std::milli>(
                          std::chrono::steady_clock::now() - partition_started)
                          .count();
    std::cerr << "[window] partition rows=" << rows.size()
              << " parts=" << layout.partitions.size() << " ms=" << ms << "\n";
  }

  // Sorting row indices otherwise follows two pointers to a tagged Value
  // at every comparison. Encode a proven numeric column once into compact
  // sortable keys; original row indices break ties in encounter order.
  std::shared_ptr<QueryMemoryCharge> numeric_order_charge;
  std::vector<uint64_t> numeric_keys;
  std::vector<uint8_t> numeric_nulls;
  bool numeric_has_null = false;
  if ((integer_order || double_order) &&
      rows.size() <= SIZE_MAX / (sizeof(uint64_t) + sizeof(uint8_t))) {
    numeric_order_charge = TryReserveQueryMemory(
        rows.size() * (sizeof(uint64_t) + sizeof(uint8_t)));
    if (numeric_order_charge) {
      numeric_keys.resize(rows.size());
      numeric_nulls.resize(rows.size());
      constexpr uint64_t sign = uint64_t{1} << 63;
      const size_t encode_workers =
          rows.size() >= 16384
              ? std::min<size_t>({8, rows.size() / 4096,
                                  QueryParallelWorkerLimit(
                                      std::thread::hardware_concurrency())})
              : 1;
      auto encode_range = [&](size_t begin, size_t end, bool* has_null_out) {
        bool local_null = false;
        if (integer_order) {
          for (size_t r = begin; r < end; ++r) {
            const auto& value = rows[r][*integer_order_slot];
            if (value.IsNull()) {
              numeric_nulls[r] = 1;
              local_null = true;
            } else {
              numeric_keys[r] =
                  std::bit_cast<uint64_t>(value.value.int_value) ^ sign;
            }
          }
        } else {
          for (size_t r = begin; r < end; ++r) {
            const auto& value = rows[r][*integer_order_slot];
            if (value.IsNull()) {
              numeric_nulls[r] = 1;
              local_null = true;
            } else {
              const double number = value.value.double_value;
              if (std::isnan(number)) {
                numeric_keys[r] =
                    0;  // NaN sorts before every finite value/-inf.
              } else {
                const auto bits =
                    std::bit_cast<uint64_t>(number == 0.0 ? 0.0 : number);
                numeric_keys[r] = (bits & sign) != 0 ? ~bits : (bits ^ sign);
              }
            }
          }
        }
        *has_null_out = local_null;
      };
      if (encode_workers > 1) {
        std::vector<uint8_t> worker_nulls(encode_workers, 0);
        RETURN_IF_FAIL(RunParallelTasks(encode_workers, [&](size_t w) {
          bool hn = false;
          encode_range((rows.size() * w) / encode_workers,
                       (rows.size() * (w + 1)) / encode_workers, &hn);
          worker_nulls[w] = hn ? 1 : 0;
        }));
        for (uint8_t hn : worker_nulls) {
          numeric_has_null |= (hn != 0);
        }
      } else {
        encode_range(0, rows.size(), &numeric_has_null);
      }
    }
  }
  if (numeric_order_charge) {
    struct NumericSortItem {
      uint64_t key;
      size_t row;
    };
    struct NumericNullSortItem {
      uint64_t key;
      size_t row;
      uint8_t null_rank;
    };
    const bool ascending = window.order_by[0].ascending;
    const bool nulls_first = window.order_by[0].nulls_first.value_or(ascending);
    const size_t workers =
        rows.size() < 16384
            ? 1
            : std::min<size_t>({8, layout.partitions.size(),
                                QueryParallelWorkerLimit(
                                    std::thread::hardware_concurrency())});
    RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
      std::vector<NumericSortItem> items;
      std::vector<NumericNullSortItem> null_items;
      for (size_t p = worker; p < layout.partitions.size(); p += workers) {
        auto& partition = layout.partitions[p];
        std::span<size_t> ordered = layout.MutableOrdered(partition);
        const size_t n = ordered.size();
        if (n <= 1) {
          if (need_order_values && n == 1) {
            partition.order_values =
                WindowOrderValues(rows, ordered, *integer_order_slot);
          }
          if (need_peer_end && n == 1) {
            partition.peer_end.assign(1, 0);
          }
          continue;
        }
        if (!numeric_has_null) {
          items.resize(n);
          for (size_t k = 0; k < n; ++k) {
            const size_t r = ordered[k];
            const uint64_t raw = numeric_keys[r];
            items[k] = {ascending ? raw : ~raw, r};
          }
          constexpr auto ordered_less = [](const NumericSortItem& a,
                                           const NumericSortItem& b) {
            return a.key != b.key ? a.key < b.key : a.row < b.row;
          };
          if (rank_bound && *rank_bound < n) {
            // Rank counts rows before a peer group. The Kth row identifies the
            // final required peer; retain every tie, even beyond K, before the
            // stable-by-input-position sort. No floating arithmetic is changed.
            auto nth =
                items.begin() + static_cast<std::ptrdiff_t>(*rank_bound - 1);
            std::nth_element(items.begin(), nth, items.end(), ordered_less);
            const uint64_t boundary_key = nth->key;
            const auto end =
                std::partition(items.begin(), items.end(),
                               [boundary_key](const NumericSortItem& item) {
                                 return item.key <= boundary_key;
                               });
            const size_t kept = static_cast<size_t>(end - items.begin());
            items.resize(kept);
            if (!layout.flat_ordered.empty()) {
              partition.flat_end = partition.flat_begin + kept;
            } else {
              partition.ordered.resize(kept);
            }
            ordered = layout.MutableOrdered(partition);
          }
          std::sort(items.begin(), items.end(), ordered_less);
          for (size_t k = 0; k < items.size(); ++k) {
            ordered[k] = items[k].row;
          }
          if (need_order_values) {
            partition.order_values =
                WindowOrderValues(rows, ordered, *integer_order_slot);
          }
          if (need_peer_end) {
            const size_t size = items.size();
            partition.peer_end.resize(size);
            // The encoded domain has exactly the same peers: NULL is separate,
            // NaNs share one key, and signed zero was normalized before
            // sorting.
            for (size_t first = 0; first < size;) {
              const uint64_t first_key = items[first].key;
              size_t end = first + 1;
              while (end < size && items[end].key == first_key) {
                ++end;
              }
              std::fill(
                  partition.peer_end.begin() +
                      static_cast<std::ptrdiff_t>(first),
                  partition.peer_end.begin() + static_cast<std::ptrdiff_t>(end),
                  end - 1);
              first = end;
            }
          }
        } else {
          null_items.resize(n);
          const uint8_t null_pos = nulls_first ? 0 : 1;
          const uint8_t non_null_pos = nulls_first ? 1 : 0;
          for (size_t k = 0; k < n; ++k) {
            const size_t r = ordered[k];
            const uint64_t raw = numeric_keys[r];
            null_items[k] = {ascending ? raw : ~raw, r,
                             numeric_nulls[r] != 0 ? null_pos : non_null_pos};
          }
          constexpr auto ordered_less = [](const NumericNullSortItem& a,
                                           const NumericNullSortItem& b) {
            if (a.null_rank != b.null_rank) {
              return a.null_rank < b.null_rank;
            }
            return a.key != b.key ? a.key < b.key : a.row < b.row;
          };
          if (rank_bound && *rank_bound < n) {
            auto nth = null_items.begin() +
                       static_cast<std::ptrdiff_t>(*rank_bound - 1);
            std::nth_element(null_items.begin(), nth, null_items.end(),
                             ordered_less);
            const uint8_t boundary_null = nth->null_rank;
            const uint64_t boundary_key = nth->key;
            const auto end = std::partition(
                null_items.begin(), null_items.end(),
                [boundary_null, boundary_key](const NumericNullSortItem& item) {
                  if (item.null_rank != boundary_null) {
                    return item.null_rank < boundary_null;
                  }
                  return item.key <= boundary_key;
                });
            const size_t kept = static_cast<size_t>(end - null_items.begin());
            null_items.resize(kept);
            if (!layout.flat_ordered.empty()) {
              partition.flat_end = partition.flat_begin + kept;
            } else {
              partition.ordered.resize(kept);
            }
            ordered = layout.MutableOrdered(partition);
          }
          std::sort(null_items.begin(), null_items.end(), ordered_less);
          for (size_t k = 0; k < null_items.size(); ++k) {
            ordered[k] = null_items[k].row;
          }
          if (need_order_values) {
            partition.order_values =
                WindowOrderValues(rows, ordered, *integer_order_slot);
          }
          if (need_peer_end) {
            const size_t size = null_items.size();
            partition.peer_end.resize(size);
            for (size_t first = 0; first < size;) {
              const uint8_t first_null = null_items[first].null_rank;
              const uint64_t first_key = null_items[first].key;
              size_t end = first + 1;
              while (end < size && null_items[end].null_rank == first_null &&
                     null_items[end].key == first_key) {
                ++end;
              }
              std::fill(
                  partition.peer_end.begin() +
                      static_cast<std::ptrdiff_t>(first),
                  partition.peer_end.begin() + static_cast<std::ptrdiff_t>(end),
                  end - 1);
              first = end;
            }
          }
        }
      }
    }));
    return layout;
  }
  const auto sort_direct_partition = [&](std::span<size_t> ordered) {
    if (double_order) {
      const auto slot = *integer_order_slot;
      const bool ascending = window.order_by[0].ascending;
      const bool nulls_first =
          window.order_by[0].nulls_first.value_or(ascending);
      std::stable_sort(ordered.begin(), ordered.end(), [&](size_t a, size_t b) {
        const auto& lhs = rows[a][slot];
        const auto& rhs = rows[b][slot];
        if (lhs.IsNull() || rhs.IsNull()) {
          return lhs.IsNull() != rhs.IsNull() && (lhs.IsNull() == nulls_first);
        }
        const double l = lhs.value.double_value;
        const double r = rhs.value.double_value;
        if (std::isnan(l) || std::isnan(r)) {
          return std::isnan(l) != std::isnan(r) &&
                 (ascending ? std::isnan(l) : std::isnan(r));
        }
        return ascending ? l < r : l > r;
      });
    } else if (integer_order) {
      const auto slot = *integer_order_slot;
      const bool ascending = window.order_by[0].ascending;
      const bool nulls_first =
          window.order_by[0].nulls_first.value_or(ascending);
      std::stable_sort(ordered.begin(), ordered.end(), [&](size_t a, size_t b) {
        const auto& lhs = rows[a][slot];
        const auto& rhs = rows[b][slot];
        if (lhs.IsNull() || rhs.IsNull()) {
          return lhs.IsNull() != rhs.IsNull() && (lhs.IsNull() == nulls_first);
        }
        return ascending ? lhs.value.int_value < rhs.value.int_value
                         : lhs.value.int_value > rhs.value.int_value;
      });
    } else if (direct_order) {
      std::stable_sort(ordered.begin(), ordered.end(), [&](size_t a, size_t b) {
        for (size_t t = 0; t < order_offsets.size(); ++t) {
          const auto& lhs = rows[a][*order_offsets[t]];
          const auto& rhs = rows[b][*order_offsets[t]];
          if (ValuesEqual(lhs, rhs)) {
            continue;
          }
          const auto& term = window.order_by[t];
          return WindowOrderLess(lhs, rhs, term);
        }
        return false;
      });
    }
  };
  const auto compute_peer_end = [&](WindowPartitionLayout& partition,
                                    size_t m) {
    partition.peer_end.resize(m);
    // Discover each peer run once. Re-scanning its suffix for every row is
    // quadratic for constant ORDER BY keys (including an unordered window).
    for (size_t a = 0; a < m;) {
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
        if (!equal) {
          break;
        }
        ++end;
      }
      std::fill(
          partition.peer_end.begin() + static_cast<std::ptrdiff_t>(a),
          partition.peer_end.begin() + static_cast<std::ptrdiff_t>(end + 1),
          end);
      a = end + 1;
    }
  };
  if (direct_order || window.order_by.empty()) {
    const auto finish_general_pure = [&](WindowPartitionLayout& partition) {
      std::span<size_t> ordered = layout.MutableOrdered(partition);
      sort_direct_partition(ordered);
      const bool build_order_values =
          need_order_values || (need_peer_end && !window.order_by.empty());
      if (build_order_values) {
        if (integer_order || double_order) {
          partition.order_values =
              WindowOrderValues(rows, ordered, *integer_order_slot);
        } else {
          partition.order_values =
              WindowOrderValues(ordered.size(), window.order_by.size());
        }
        if (!window.order_by.empty() && !integer_order && !double_order) {
          for (size_t k = 0; k < ordered.size(); ++k) {
            auto values = partition.order_values[k];
            for (size_t t = 0; t < window.order_by.size(); ++t) {
              const auto& term = window.order_by[t];
              Value key =
                  ApplyKeyCollation(rows[ordered[k]][*order_offsets[t]],
                                    CollationOfExpression(term.expression));
              values[t] = std::move(key);
            }
          }
        }
      }
      if (need_peer_end) {
        compute_peer_end(partition, ordered.size());
      }
      if (build_order_values && !need_order_values) {
        partition.order_values = WindowOrderValues{};
      }
    };
    const size_t general_workers =
        rows.size() >= 16384 && layout.partitions.size() > 1
            ? std::min<size_t>({8, layout.partitions.size(),
                                QueryParallelWorkerLimit(
                                    std::thread::hardware_concurrency())})
            : 1;
    if (general_workers > 1) {
      RETURN_IF_FAIL(RunParallelTasks(general_workers, [&](size_t worker) {
        for (size_t p = worker; p < layout.partitions.size();
             p += general_workers) {
          finish_general_pure(layout.partitions[p]);
        }
      }));
    } else {
      for (WindowPartitionLayout& partition : layout.partitions) {
        finish_general_pure(partition);
      }
    }
    return layout;
  }
  std::vector<SortKeyOrder> order_domains;
  order_domains.reserve(window.order_by.size());
for (const auto& term : window.order_by) {
    order_domains.emplace_back(term.ascending, std::nullopt,
                               CollationOfExpression(term.expression));
  }
  for (WindowPartitionLayout& partition : layout.partitions) {
    std::span<size_t> ordered = layout.MutableOrdered(partition);
    const bool build_order_values =
        need_order_values || (need_peer_end && !window.order_by.empty()) ||
        (!window.order_by.empty() && !integer_order && !double_order &&
         !direct_order);
    if (build_order_values) {
      partition.order_values =
          WindowOrderValues(ordered.size(), window.order_by.size());
      for (size_t k = 0; k < ordered.size(); ++k) {
        auto values = partition.order_values[k];
        for (size_t t = 0; t < window.order_by.size(); ++t) {
          const auto& term = window.order_by[t];
          Value key;
          if (const auto offset = order_offsets[t]) {
            key = rows[ordered[k]][*offset];
          } else {
            ASSIGN_OR_RETURN(Value, evaluated,
                             runtime.EvalAt(term.expression, rows, ordered[k]));
            key = std::move(evaluated);
          }
          RETURN_IF_FAIL(order_domains[t].Observe(key));
          // COLLATE-wrapped keys sort case-insensitively ('und:ci'); folding
          // the hidden sort key is sufficient because ValueLess then only
          // sees the folded text. The collation comes from the value's tag
          // (the shared type/collation.hpp contract) rather than from the
          // expression shape, so a collator that is not a literal COLLATE
          // call — a parameter, a subquery output — folds too, and non-ASCII
          // bytes keep byte order instead of passing through std::tolower.
          key = ApplyKeyCollation(key, CollationOfExpression(term.expression));
          values[t] = std::move(key);
        }
      }
      std::vector<size_t> positions(ordered.size());
      std::iota(positions.begin(), positions.end(), 0);
      std::stable_sort(
          positions.begin(), positions.end(), [&](size_t a, size_t b) {
            for (size_t t = 0; t < window.order_by.size(); ++t) {
              const WindowOrderTerm& term = window.order_by[t];
              if (ValuesEqual(partition.order_values[a][t],
                              partition.order_values[b][t])) {
                continue;
              }
              return WindowOrderLess(partition.order_values[a][t],
                                     partition.order_values[b][t], term);
            }
            return false;
          });
      std::vector<size_t> sorted_members(ordered.size());
      WindowOrderValues sorted_values(ordered.size(), window.order_by.size());
      for (size_t k = 0; k < positions.size(); ++k) {
        sorted_members[k] = ordered[positions[k]];
        auto destination = sorted_values[k];
        auto source = partition.order_values[positions[k]];
        std::ranges::move(source, destination.begin());
      }
      std::ranges::copy(sorted_members, ordered.begin());
      partition.order_values = std::move(sorted_values);
    }

    if (need_peer_end) {
      compute_peer_end(partition, ordered.size());
    }
    if (build_order_values && !need_order_values) {
      partition.order_values = WindowOrderValues{};
    }
  }
  return layout;
}

bool IsPrefixWindowEligible(const WindowFunctionCallExpression& window,
                            const Schema& schema,
                            std::optional<size_t>* slot_out) {
  const std::string& fn = window.function;
  const bool is_op =
      fn == "SUM" || fn == "AVG" || fn == "MIN" || fn == "MAX" || fn == "COUNT";
  if (!is_op) {
    return false;
  }
  const bool whole_partition = !window.has_frame && window.order_by.empty();
  const bool prefix_frame =
      !window.has_frame ||
      (window.frame_start.type == WindowFrameBoundType::kUnboundedPreceding &&
       window.frame_end.type == WindowFrameBoundType::kCurrentRow &&
       !window.frame_start.offset && !window.frame_end.offset);
  if (!(whole_partition || prefix_frame) ||
      (window.frame_unit == WindowFrameUnit::kGroups &&
       window.order_by.empty()) ||
      window.distinct || window.where_filter ||
      !window.inner_order_by.empty() || window.inner_limit ||
      window.exclusion != WindowFrameExclusion::kNone ||
      window.args.size() != 1 || !window.args[0] ||
      window.args[0]->Type() != TypeTag::kColumnValue) {
    return false;
  }
  *slot_out = LocalColumnOffset(
      schema, window.args[0]->AsColumnValue().GetColumnName());
  return slot_out->has_value();
}

PrefixFrameMode ClassifyPrefixFrame(
    const WindowFunctionCallExpression& window) {
  if (!window.has_frame && window.order_by.empty()) {
    return PrefixFrameMode::kWholePartition;
  }
  if (window.has_frame && window.frame_unit == WindowFrameUnit::kRows) {
    return PrefixFrameMode::kRows;
  }
  return PrefixFrameMode::kPeer;
}

PrefixOp ClassifyPrefixOp(const std::string& fn) {
  if (fn == "COUNT") {
    return PrefixOp::kCount;
  }
  if (fn == "SUM") {
    return PrefixOp::kSum;
  }
  if (fn == "AVG") {
    return PrefixOp::kAvg;
  }
  if (fn == "MIN") {
    return PrefixOp::kMin;
  }
  return PrefixOp::kMax;
}

namespace {

template <typename AdvanceFn, typename ResultFn, typename EmitFn>
StatusOr<bool> RunPrefixFrameLoop(std::span<const size_t> ordered,
                                  const std::vector<size_t>& peer_end,
                                  PrefixFrameMode mode, AdvanceFn&& advance,
                                  ResultFn&& result, EmitFn&& emit) {
  const size_t m = ordered.size();
  Status error = Status::kSuccess;
  switch (mode) {
    case PrefixFrameMode::kWholePartition: {
      for (size_t k = 0; k < m; ++k) {
        if (!advance(ordered[k], k + 1, &error)) {
          if (!error.ok()) {
            return error;
          }
          return false;
        }
      }
      const Value total = result();
      for (size_t k = 0; k < m; ++k) {
        emit(ordered[k], Value(total));
      }
      return true;
    }
    case PrefixFrameMode::kRows: {
      for (size_t k = 0; k < m; ++k) {
        const size_t row_idx = ordered[k];
        if (!advance(row_idx, k + 1, &error)) {
          if (!error.ok()) {
            return error;
          }
          return false;
        }
        emit(row_idx, result());
      }
      return true;
    }
    case PrefixFrameMode::kPeer: {
      size_t consumed = 0;
      for (size_t k = 0; k < m;) {
        const size_t end = peer_end[k];
        while (consumed <= end) {
          if (!advance(ordered[consumed], consumed + 1, &error)) {
            if (!error.ok()) {
              return error;
            }
            return false;
          }
          ++consumed;
        }
        const Value val = result();
        for (; k <= end; ++k) {
          emit(ordered[k], Value(val));
        }
      }
      return true;
    }
  }
  return false;
}

template <PrefixOp kOp, bool kUniform, typename EmitFn>
StatusOr<bool> ComputeDoublePrefix(const std::vector<Row>& rows, size_t slot,
                                   std::span<const size_t> ordered,
                                   const std::vector<size_t>& peer_end,
                                   PrefixFrameMode mode, EmitFn&& emit) {
  if constexpr (kOp == PrefixOp::kSum || kOp == PrefixOp::kAvg) {
    double total = 0.0;
    int64_t count = 0;
    auto advance = [&](size_t row_idx, size_t /*next_k*/, Status* /*err*/) {
      const Value& v = rows[row_idx][slot];
      if (v.IsNull()) {
        return true;
      }
      if constexpr (!kUniform) {
        if (v.type != ValueType::kDouble || v.IsUnsigned()) {
          return false;
        }
      }
      total += v.value.double_value;
      ++count;
      return true;
    };
    auto result = [&]() {
      if (count == 0) {
        return Value();
      }
      if constexpr (kOp == PrefixOp::kAvg) {
        return Value(total / static_cast<double>(count));
      } else {
        return Value(total);
      }
    };
    return RunPrefixFrameLoop(ordered, peer_end, mode, advance, result, emit);
  } else {
    bool has_best = false;
    bool best_nan = false;
    double best = 0.0;
    auto advance = [&](size_t row_idx, size_t /*next_k*/, Status* /*err*/) {
      const Value& v = rows[row_idx][slot];
      if (v.IsNull()) {
        return true;
      }
      if constexpr (!kUniform) {
        if (v.type != ValueType::kDouble) {
          return false;
        }
      }
      const double cur = v.value.double_value;
      const bool cur_nan = std::isnan(cur);
      if (!has_best) {
        has_best = true;
        best = cur;
        best_nan = cur_nan;
      } else if (!best_nan &&
                 (cur_nan ||
                  (kOp == PrefixOp::kMin ? (cur < best) : (cur > best)))) {
        best = cur;
        best_nan = cur_nan;
      }
      return true;
    };
    auto result = [&]() { return has_best ? Value(best) : Value(); };
    return RunPrefixFrameLoop(ordered, peer_end, mode, advance, result, emit);
  }
}

template <PrefixOp kOp, bool kUniform, typename EmitFn>
StatusOr<bool> ComputeInt64Prefix(const std::string& fn,
                                  const std::vector<Row>& rows, size_t slot,
                                  std::span<const size_t> ordered,
                                  const std::vector<size_t>& peer_end,
                                  PrefixFrameMode mode, EmitFn&& emit) {
  if constexpr (kOp == PrefixOp::kSum || kOp == PrefixOp::kAvg) {
    int64_t total = 0;
    int64_t count = 0;
    const size_t m = ordered.size();
    auto advance = [&](size_t row_idx, size_t next_k, Status* err) {
      const Value& v = rows[row_idx][slot];
      if (v.IsNull()) {
        return true;
      }
      if constexpr (!kUniform) {
        if (v.type != ValueType::kInt64 || v.IsUnsigned()) {
          return false;
        }
      }
      if (__builtin_add_overflow(total, v.value.int_value, &total)) {
        if constexpr (!kUniform) {
          for (size_t rem_k = next_k; rem_k < m; ++rem_k) {
            const Value& rem = rows[ordered[rem_k]][slot];
            if (!rem.IsNull() &&
                (rem.type != ValueType::kInt64 || rem.IsUnsigned())) {
              return false;
            }
          }
        }
        *err =
            StatusError(StatusCode::kIsInfinity, "integer overflow in " + fn);
        return false;
      }
      ++count;
      return true;
    };
    auto result = [&]() {
      if (count == 0) {
        return Value();
      }
      if constexpr (kOp == PrefixOp::kAvg) {
        return Value(static_cast<double>(total) / static_cast<double>(count));
      } else {
        return Value(total);
      }
    };
    return RunPrefixFrameLoop(ordered, peer_end, mode, advance, result, emit);
  } else {
    bool has_best = false;
    int64_t best = 0;
    auto advance = [&](size_t row_idx, size_t /*next_k*/, Status* /*err*/) {
      const Value& v = rows[row_idx][slot];
      if (v.IsNull()) {
        return true;
      }
      if constexpr (!kUniform) {
        if (v.type != ValueType::kInt64 || v.IsUnsigned()) {
          return false;
        }
      }
      const int64_t cur = v.value.int_value;
      if (!has_best) {
        has_best = true;
        best = cur;
      } else if (kOp == PrefixOp::kMin ? (cur < best) : (cur > best)) {
        best = cur;
      }
      return true;
    };
    auto result = [&]() { return has_best ? Value(best) : Value(); };
    return RunPrefixFrameLoop(ordered, peer_end, mode, advance, result, emit);
  }
}

template <typename EmitFn>
StatusOr<bool> TryComputeColumnPrefixImpl(
    const WindowFunctionCallExpression& window, const std::vector<Row>& rows,
    size_t slot, std::span<const size_t> ordered,
    const std::vector<size_t>& peer_end, const Schema& schema,
    bool uniform_cell_types, EmitFn&& emit) {
  const size_t m = ordered.size();
  if (m == 0) {
    return true;
  }
  const PrefixOp op = ClassifyPrefixOp(window.function);
  const PrefixFrameMode mode = ClassifyPrefixFrame(window);
  if (op == PrefixOp::kCount) {
    int64_t count = 0;
    auto advance = [&](size_t row_idx, size_t /*next_k*/, Status* /*err*/) {
      count += !rows[row_idx][slot].IsNull();
      return true;
    };
    auto result = [&]() { return Value(count); };
    return RunPrefixFrameLoop(ordered, peer_end, mode, advance, result, emit);
  }

  const Column& col = schema.GetColumn(slot);
  const ValueType col_type = col.Type();
  const bool col_unsigned = col.IsUnsigned();

  const auto dispatch_double = [&]<bool kUniform>() -> StatusOr<bool> {
    switch (op) {
      case PrefixOp::kSum:
        return ComputeDoublePrefix<PrefixOp::kSum, kUniform>(
            rows, slot, ordered, peer_end, mode, emit);
      case PrefixOp::kAvg:
        return ComputeDoublePrefix<PrefixOp::kAvg, kUniform>(
            rows, slot, ordered, peer_end, mode, emit);
      case PrefixOp::kMin:
        return ComputeDoublePrefix<PrefixOp::kMin, kUniform>(
            rows, slot, ordered, peer_end, mode, emit);
      case PrefixOp::kMax:
        return ComputeDoublePrefix<PrefixOp::kMax, kUniform>(
            rows, slot, ordered, peer_end, mode, emit);
      default:
        return false;
    }
  };
  const auto dispatch_int64 = [&]<bool kUniform>() -> StatusOr<bool> {
    switch (op) {
      case PrefixOp::kSum:
        return ComputeInt64Prefix<PrefixOp::kSum, kUniform>(
            window.function, rows, slot, ordered, peer_end, mode, emit);
      case PrefixOp::kAvg:
        return ComputeInt64Prefix<PrefixOp::kAvg, kUniform>(
            window.function, rows, slot, ordered, peer_end, mode, emit);
      case PrefixOp::kMin:
        return ComputeInt64Prefix<PrefixOp::kMin, kUniform>(
            window.function, rows, slot, ordered, peer_end, mode, emit);
      case PrefixOp::kMax:
        return ComputeInt64Prefix<PrefixOp::kMax, kUniform>(
            window.function, rows, slot, ordered, peer_end, mode, emit);
      default:
        return false;
    }
  };
  if (col_type == ValueType::kDouble && !col_unsigned) {
    return uniform_cell_types ? dispatch_double.template operator()<true>()
                              : dispatch_double.template operator()<false>();
  }
  if (col_type == ValueType::kInt64 && !col_unsigned) {
    return uniform_cell_types ? dispatch_int64.template operator()<true>()
                              : dispatch_int64.template operator()<false>();
  }

  // Fallback for DATE/STRING MIN/MAX or schemas whose column type is untyped.
  const bool numeric = op == PrefixOp::kSum || op == PrefixOp::kAvg;
  const bool is_min = op == PrefixOp::kMin;
  const bool is_max = op == PrefixOp::kMax;
  ValueType numeric_type = ValueType::kNull;
  size_t consumed = 0;
  int64_t count = 0;
  int64_t int_total = 0;
  double double_total = 0;
  bool has_best_double = false;
  bool best_double_nan = false;
  double best_double = 0.0;
  bool has_best_int = false;
  int64_t best_int = 0;
  Value best;
  const auto is_supported_value = [&](const Value& value) {
    return (value.type == ValueType::kInt64 ||
            value.type == ValueType::kDouble ||
            (!numeric && (value.type == ValueType::kDate ||
                          value.type == ValueType::kVarChar))) &&
           (!numeric || !value.IsUnsigned()) &&
           (numeric_type == ValueType::kNull || numeric_type == value.type);
  };
  for (size_t k = 0; k < m; ++k) {
    const size_t end = mode == PrefixFrameMode::kWholePartition ? m - 1
                       : mode == PrefixFrameMode::kRows         ? k
                                                                : peer_end[k];
    while (consumed <= end) {
      const auto& value = rows[ordered[consumed++]][slot];
      if (value.IsNull()) {
        continue;
      }
      if (!is_supported_value(value)) {
        return false;
      }
      numeric_type = value.type;
      ++count;
      if (numeric) {
        if (numeric_type == ValueType::kDouble) {
          double_total += value.value.double_value;
        } else if (__builtin_add_overflow(int_total, value.value.int_value,
                                          &int_total)) {
          while (consumed < m) {
            const auto& rem = rows[ordered[consumed++]][slot];
            if (!rem.IsNull() && !is_supported_value(rem)) {
              return false;
            }
          }
          return StatusError(StatusCode::kIsInfinity,
                             "integer overflow in " + window.function);
        }
      } else if (is_min || is_max) {
        if (numeric_type == ValueType::kDouble) {
          const double cur = value.value.double_value;
          const bool cur_nan = std::isnan(cur);
          if (!has_best_double) {
            has_best_double = true;
            best_double = cur;
            best_double_nan = cur_nan;
          } else if (!best_double_nan &&
                     (cur_nan ||
                      (is_min ? (cur < best_double) : (cur > best_double)))) {
            best_double = cur;
            best_double_nan = cur_nan;
          }
        } else if (numeric_type == ValueType::kInt64 && !value.IsUnsigned() &&
                   best.IsNull()) {
          if (!has_best_int) {
            has_best_int = true;
            best_int = value.value.int_value;
          } else if (is_min ? (value.value.int_value < best_int)
                            : (value.value.int_value > best_int)) {
            best_int = value.value.int_value;
          }
        } else {
          if (has_best_int) {
            best = Value(best_int);
            has_best_int = false;
          }
          if (best.IsNull()) {
            best = value;
          } else if ((numeric_type == ValueType::kInt64 ||
                      numeric_type == ValueType::kDate) &&
                     !value.IsUnsigned() && !best.IsUnsigned()) {
            if (is_min ? (value.value.int_value < best.value.int_value)
                       : (value.value.int_value > best.value.int_value)) {
              best = value;
            }
          } else {
            const bool value_nan = value.type == ValueType::kDouble &&
                                   std::isnan(value.value.double_value);
            const bool best_nan = best.type == ValueType::kDouble &&
                                  std::isnan(best.value.double_value);
            if (!best_nan &&
                (value_nan || (is_min ? ValueLess(value, best, true)
                                      : ValueLess(best, value, true)))) {
              best = value;
            }
          }
        }
      }
    }
    if (is_min || is_max) {
      if (has_best_double) {
        emit(ordered[k], Value(best_double));
      } else if (has_best_int) {
        emit(ordered[k], Value(best_int));
      } else {
        emit(ordered[k], Value(best));
      }
    } else if (count != 0) {
      if (op == PrefixOp::kAvg) {
        emit(ordered[k], Value((numeric_type == ValueType::kDouble
                                    ? double_total
                                    : static_cast<double>(int_total)) /
                               static_cast<double>(count)));
      } else {
        emit(ordered[k], numeric_type == ValueType::kDouble
                             ? Value(double_total)
                             : Value(int_total));
      }
    } else {
      emit(ordered[k], Value());
    }
  }
  return true;
}

StatusOr<bool> TryComputeColumnPrefix(
    const WindowFunctionCallExpression& window, const std::vector<Row>& rows,
    std::optional<size_t> prefix_slot, std::span<const size_t> ordered,
    const std::vector<size_t>& peer_end, const Schema& schema,
    bool uniform_cell_types, std::vector<Value>* out) {
  std::optional<size_t> checked_slot;
  if (!prefix_slot || !IsPrefixWindowEligible(window, schema, &checked_slot)) {
    return false;
  }
  return TryComputeColumnPrefixImpl(
      window, rows, *prefix_slot, ordered, peer_end, schema, uniform_cell_types,
      [out](size_t row_idx, Value&& val) { (*out)[row_idx] = std::move(val); });
}

}  // namespace

StatusOr<bool> TryComputeColumnPrefixWithSummary(
    const WindowFunctionCallExpression& window, const std::vector<Row>& rows,
    size_t slot, std::span<const size_t> ordered,
    const std::vector<size_t>& peer_end, const Schema& schema,
    bool uniform_cell_types, std::vector<Value>& computed,
    WindowColTypeSummary& summary) {
  return TryComputeColumnPrefixImpl(window, rows, slot, ordered, peer_end,
                                    schema, uniform_cell_types,
                                    [&](size_t row_idx, Value&& val) {
                                      summary.Observe(val);
                                      computed[row_idx] = std::move(val);
                                    });
}

// NOLINTNEXTLINE(misc-use-anonymous-namespace): shared across this translation unit.
static StatusOr<bool> TryComputeSlidingDistinctRows(
    const WindowFunctionCallExpression& window, const std::vector<Row>& rows,
    std::span<const size_t> ordered, const WindowRuntime& runtime,
    std::vector<Value>* out) {
  const char* disable_env =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_WINDOW_DISTINCT_INVERSE");
  if (disable_env != nullptr && disable_env[0] == '1') {
    return false;
  }
  if (!window.distinct || window.args.size() != 1 || window.where_filter ||
      !window.inner_order_by.empty() || window.inner_limit.has_value() ||
      !window.has_frame || window.frame_unit != WindowFrameUnit::kRows ||
      (window.exclusion != WindowFrameExclusion::kNone &&
       window.exclusion != WindowFrameExclusion::kCurrentRow)) {
    return false;
  }
  if (window.function != "COUNT" && window.function != "SUM") {
    return false;
  }
  const Expression& arg = window.args[0];
  if (!arg) {
    return false;
  }
  const bool arg_is_col = arg->Type() == TypeTag::kColumnValue;
  const bool arg_is_const =
      arg->Type() == TypeTag::kConstantValue &&
      dynamic_cast<const ParameterSlot*>(arg.get()) == nullptr;
  if (!arg_is_col && !arg_is_const) {
    return false;
  }
  if (arg_is_col) {
    const auto& cname = arg->AsColumnValue().GetColumnName();
    if (cname.name == "*" ||
        !LocalColumnOffset(*runtime.schema, cname).has_value()) {
      return false;
    }
  }

  auto parse_const_offset = [](const WindowFrameBound& bound,
                               bool* ok) -> int64_t {
    if (bound.type == WindowFrameBoundType::kUnboundedPreceding ||
        bound.type == WindowFrameBoundType::kUnboundedFollowing ||
        bound.type == WindowFrameBoundType::kCurrentRow) {
      return 0;
    }
    if (!bound.offset || bound.offset->Type() != TypeTag::kConstantValue) {
      *ok = false;
      return 0;
    }
    const Value& val = bound.offset->AsConstantValue().GetValue();
    if (val.IsNull() || val.type != ValueType::kInt64 || val.IsUnsigned() ||
        val.value.int_value < 0) {
      *ok = false;
      return 0;
    }
    return val.value.int_value;
  };
  bool bounds_ok = true;
  const int64_t start_off = parse_const_offset(window.frame_start, &bounds_ok);
  const int64_t end_off = parse_const_offset(window.frame_end, &bounds_ok);
  if (!bounds_ok) {
    return false;
  }

  const size_t m = ordered.size();
  if (m == 0) {
    return true;
  }

  // Lean proof: lean/TinylambOpt/Research/WindowDistinctInverse.lean
  //   (window_distinct_inverse_sound, update_count_bound)
  const bool is_sum = window.function == "SUM";
  // COUNT(DISTINCT) accepts general Value types, so normalize peer equality
  // with CanonicalDistinctValue. SUM(DISTINCT) takes the arithmetic path only
  // for signed INT64 values (checked below); their raw values are already the
  // canonical keys used by the set and by the exact sum accumulator.
  std::vector<Value> vals;
  vals.reserve(m);
  for (size_t k = 0; k < m; ++k) {
    ASSIGN_OR_RETURN(Value, v, (runtime.EvalAt(arg, rows, ordered[k])));
    if (is_sum && !v.IsNull() &&
        (v.type != ValueType::kInt64 || v.IsUnsigned())) {
      return false;
    }
    vals.push_back(is_sum ? v : CanonicalDistinctValue(v));
  }

  auto compute_bounds = [&](size_t pos) -> std::pair<size_t, size_t> {
    std::optional<size_t> lo;
    switch (window.frame_start.type) {
      case WindowFrameBoundType::kUnboundedPreceding:
        lo = size_t{0};
        break;
      case WindowFrameBoundType::kCurrentRow:
        lo = pos;
        break;
      case WindowFrameBoundType::kOffsetPreceding: {
        const auto off = static_cast<uint64_t>(start_off);
        lo = (off <= pos) ? (pos - static_cast<size_t>(off)) : size_t{0};
        break;
      }
      case WindowFrameBoundType::kOffsetFollowing: {
        const auto off = static_cast<uint64_t>(start_off);
        if (off <= m - 1 - pos) {
          lo = pos + static_cast<size_t>(off);
        }
        break;
      }
      case WindowFrameBoundType::kUnboundedFollowing:
        lo = m - 1;
        break;
    }
    std::optional<size_t> hi;
    switch (window.frame_end.type) {
      case WindowFrameBoundType::kUnboundedFollowing:
        hi = m - 1;
        break;
      case WindowFrameBoundType::kCurrentRow:
        hi = pos;
        break;
      case WindowFrameBoundType::kOffsetFollowing: {
        const auto off = static_cast<uint64_t>(end_off);
        hi = (off <= m - 1 - pos) ? (pos + static_cast<size_t>(off)) : (m - 1);
        break;
      }
      case WindowFrameBoundType::kOffsetPreceding: {
        const auto off = static_cast<uint64_t>(end_off);
        if (off <= pos) {
          hi = pos - static_cast<size_t>(off);
        }
        break;
      }
      case WindowFrameBoundType::kUnboundedPreceding:
        hi = size_t{0};
        break;
    }
    if (!lo.has_value() || !hi.has_value() || *lo > *hi) {
      return {1, 0};
    }
    return {*lo, *hi};
  };

  std::unordered_map<Value, size_t, DistinctValueHash, DistinctValueEqual> mult;
  mult.reserve(std::min<size_t>(m, 256));
  size_t distinct_non_null = 0;
  platform::Int128 distinct_sum = 0;
  platform::Int128 abs_distinct_sum = 0;
  constexpr platform::Int128 kMaxInt64 = std::numeric_limits<int64_t>::max();
  constexpr platform::Int128 kMinInt64 = std::numeric_limits<int64_t>::min();

  auto add_pos = [&](size_t pos) {
    const Value& v = vals[pos];
    if (v.IsNull()) {
      return;
    }
    auto [it, inserted] = mult.emplace(v, 0);
    if (inserted || it->second == 0) {
      it->second = 1;
      ++distinct_non_null;
      if (is_sum) {
        const auto x = static_cast<platform::Int128>(v.value.int_value);
        distinct_sum += x;
        abs_distinct_sum += (x < 0 ? -x : x);
      }
    } else {
      ++(it->second);
    }
  };
  auto rem_pos = [&](size_t pos) -> bool {
    const Value& v = vals[pos];
    if (v.IsNull()) {
      return true;
    }
    auto it = mult.find(v);
    if (it == mult.end()) {
      // The slide/rebuild logic keeps mult == the frame multiset, so this is
      // unreachable; bail to the generic path rather than dereference end().
      return false;
    }
    if (--(it->second) == 0) {
      if (is_sum) {
        const auto x = static_cast<platform::Int128>(v.value.int_value);
        distinct_sum -= x;
        abs_distinct_sum -= (x < 0 ? -x : x);
      }
      mult.erase(it);
      --distinct_non_null;
    }
    return true;
  };

  bool cur_valid = false;
  size_t cur_lo = 1;
  size_t cur_hi = 0;
  for (size_t k = 0; k < m; ++k) {
    const auto [lo, hi] = compute_bounds(k);
    if (lo > hi) {
      if (cur_valid) {
        mult.clear();
        distinct_non_null = 0;
        distinct_sum = 0;
        abs_distinct_sum = 0;
        cur_valid = false;
      }
    } else if (!cur_valid || lo > cur_hi || lo < cur_lo || hi < cur_hi) {
      if (cur_valid) {
        mult.clear();
        distinct_non_null = 0;
        distinct_sum = 0;
        abs_distinct_sum = 0;
      }
      for (size_t p = lo; p <= hi; ++p) {
        add_pos(p);
      }
      cur_lo = lo;
      cur_hi = hi;
      cur_valid = true;
    } else {
      while (cur_lo < lo) {
        if (!rem_pos(cur_lo++)) {
          return false;
        }
      }
      while (cur_hi < hi) {
        add_pos(++cur_hi);
      }
    }

    // EXCLUDE CURRENT ROW removes the current row only when it belongs to
    // this frame. If an offset frame excludes k, there is nothing to remove
    // or restore in the frame multiset.
    const bool exclude_k =
        (window.exclusion == WindowFrameExclusion::kCurrentRow) && cur_valid &&
        lo <= k && k <= hi;
    if (exclude_k) {
      if (!rem_pos(k)) {
        return false;
      }
    }

    if (!is_sum) {
      (*out)[ordered[k]] = Value(static_cast<int64_t>(distinct_non_null));
    } else if (distinct_non_null == 0) {
      (*out)[ordered[k]] = Value();
    } else if (abs_distinct_sum > kMaxInt64 || distinct_sum < kMinInt64 ||
               distinct_sum > kMaxInt64) {
      return false;
    } else {
      (*out)[ordered[k]] = Value(static_cast<int64_t>(distinct_sum));
    }

    if (exclude_k) {
      add_pos(k);
    }
  }
  return true;
}

// Lean proof: lean/TinylambOpt/ResearchProofs/NthNonNullFrame.lean (sound)
// NOLINTNEXTLINE(misc-use-anonymous-namespace): shared across this translation unit.
static StatusOr<bool> TryComputeNthNonNullFrame(
    const WindowFunctionCallExpression& window, const std::vector<Row>& rows,
    std::span<const size_t> ordered, const WindowOrderValues& order_values,
    const std::vector<size_t>& peer_end, const std::vector<size_t>& group_ends,
    const WindowRuntime& runtime, std::vector<Value>* out) {
  const char* disable_env = ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_NTH_NON_NULL_FRAME");
  if (disable_env != nullptr && disable_env[0] == '1') {
    return false;
  }
  if (!window.ignore_nulls || window.where_filter ||
      (window.exclusion != WindowFrameExclusion::kNone &&
       window.exclusion != WindowFrameExclusion::kCurrentRow) ||
      window.args.empty()) {
    return false;
  }
  if (window.function != "FIRST_VALUE" && window.function != "LAST_VALUE" &&
      window.function != "NTH_VALUE") {
    return false;
  }
  if (window.function == "NTH_VALUE" && window.args.size() < 2) {
    return false;
  }
  const Expression& arg = window.args[0];
  if (!arg) {
    return false;
  }
  const bool arg_is_col = arg->Type() == TypeTag::kColumnValue;
  const bool arg_is_const =
      arg->Type() == TypeTag::kConstantValue &&
      dynamic_cast<const ParameterSlot*>(arg.get()) == nullptr;
  if (!arg_is_col && !arg_is_const) {
    return false;
  }
  if (arg_is_col) {
    const auto& cname = arg->AsColumnValue().GetColumnName();
    if (cname.name == "*" ||
        !LocalColumnOffset(*runtime.schema, cname).has_value()) {
      return false;
    }
  }
  const size_t m = ordered.size();
  if (m == 0) {
    return true;
  }

  std::vector<size_t> ranks(m + 1, 0);
  std::vector<Value> non_null_values;
  non_null_values.reserve(m);
  for (size_t i = 0; i < m; ++i) {
    ASSIGN_OR_RETURN(Value, v, (runtime.EvalAt(arg, rows, ordered[i])));
    if (!v.IsNull()) {
      non_null_values.push_back(std::move(v));
    }
    ranks[i + 1] = non_null_values.size();
  }

  for (size_t k = 0; k < m; ++k) {
    ASSIGN_OR_RETURN(
        FrameBounds, frame_bounds,
        (WindowRuntime::ResolveFrame(window, rows, ordered, order_values, k,
                                     peer_end, group_ends)));
    const size_t lo = frame_bounds.first;
    const size_t hi = frame_bounds.second;
    if (hi < lo || lo >= m) {
      continue;
    }
    const size_t hi_excl = std::min(hi + 1, m);
    const size_t a = ranks[lo];
    const size_t b = ranks[hi_excl];
    const size_t cut = ranks[k];
    const bool remove =
        (window.exclusion == WindowFrameExclusion::kCurrentRow) && lo <= k &&
        k < hi_excl && cut < ranks[k + 1];
    const size_t avail = (b - a) - (remove ? size_t{1} : size_t{0});
    uint64_t nth_u64 = 1;
    if (window.function == "LAST_VALUE") {
      if (avail == 0) {
        continue;
      }
      nth_u64 = avail;
    } else if (window.function == "NTH_VALUE") {
      ASSIGN_OR_RETURN(Value, nth_value,
                       (runtime.EvalAt(window.args[1], rows, ordered[k])));
      if (nth_value.IsNull()) {
        return StatusError(
            StatusCode::kInvalidArgument,
            "The N value for the NthValue function must not be NULL");
      }
      if (nth_value.type != ValueType::kInt64) {
        return StatusError(
            StatusCode::kInvalidArgument,
            "The N value for the NthValue function must be an integer");
      }
      const int64_t nth = nth_value.value.int_value;
      if (nth <= 0) {
        return StatusError(
            StatusCode::kInvalidArgument,
            "The N value for the NthValue function must be positive");
      }
      nth_u64 = static_cast<uint64_t>(nth);
    }
    if (nth_u64 > avail) {
      continue;
    }
    const size_t pos = a + static_cast<size_t>(nth_u64 - 1);
    const size_t target = (remove && cut <= pos) ? (pos + 1) : pos;
    if (target < b) {
      (*out)[ordered[k]] = non_null_values[target];
    }
  }
  return true;
}

// Lean proof: lean/TinylambOpt/ResearchProofs/RangeDistinctIndex.lean (sound)
// NOLINTNEXTLINE(misc-use-anonymous-namespace): shared across this translation unit.
static StatusOr<bool> TryComputeRangeDistinctIndex(
    const WindowFunctionCallExpression& window, const std::vector<Row>& rows,
    std::span<const size_t> ordered, const WindowOrderValues& order_values,
    const std::vector<size_t>& peer_end, const std::vector<size_t>& group_ends,
    const WindowRuntime& runtime, std::vector<Value>* out) {
  const char* disable_env =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_RANGE_DISTINCT_INDEX");
  if (disable_env != nullptr && disable_env[0] == '1') {
    return false;
  }
  if (!window.distinct || window.function != "COUNT" ||
      window.args.size() != 1 || window.where_filter ||
      !window.inner_order_by.empty() || window.inner_limit.has_value() ||
      window.exclusion != WindowFrameExclusion::kNone) {
    return false;
  }
  const Expression& arg = window.args[0];
  if (!arg) {
    return false;
  }
  const bool arg_is_col = arg->Type() == TypeTag::kColumnValue;
  const bool arg_is_const =
      arg->Type() == TypeTag::kConstantValue &&
      dynamic_cast<const ParameterSlot*>(arg.get()) == nullptr;
  if (!arg_is_col && !arg_is_const) {
    return false;
  }
  if (arg_is_col) {
    const auto& cname = arg->AsColumnValue().GetColumnName();
    if (cname.name == "*" ||
        !LocalColumnOffset(*runtime.schema, cname).has_value()) {
      return false;
    }
  }
  const size_t m = ordered.size();
  if (m == 0) {
    return true;
  }

  struct MarksNode {
    uint32_t count{0};
    uint32_t left{0};
    uint32_t right{0};
  };
  uint32_t depth = 1;
  while ((size_t{1} << depth) < m) {
    ++depth;
  }
  std::vector<MarksNode> nodes;
  nodes.reserve(1 + (2 * m * (depth + 1)));
  nodes.push_back(MarksNode{});  // node 0 = empty

  auto update_mark = [&](auto&& self, uint32_t node_idx, size_t pos, bool on,
                         uint32_t d) -> uint32_t {
    if (d == 0) {
      if (!on) {
        return 0;
      }
      const auto idx = static_cast<uint32_t>(nodes.size());
      nodes.push_back(MarksNode{.count = 1, .left = 0, .right = 0});
      return idx;
    }
    const uint32_t cur_left = nodes[node_idx].left;
    const uint32_t cur_right = nodes[node_idx].right;
    const size_t half = size_t{1} << (d - 1);
    uint32_t u = cur_left;
    uint32_t v = cur_right;
    if (pos < half) {
      u = self(self, cur_left, pos, on, d - 1);
    } else {
      v = self(self, cur_right, pos - half, on, d - 1);
    }
    const uint32_t total = nodes[u].count + nodes[v].count;
    if (total == 0) {
      return 0;
    }
    const auto idx = static_cast<uint32_t>(nodes.size());
    nodes.push_back(MarksNode{.count = total, .left = u, .right = v});
    return idx;
  };

  auto query_suffix = [&](auto&& self, uint32_t node_idx, size_t lo,
                          uint32_t d) -> uint32_t {
    if (node_idx == 0) {
      return 0;
    }
    if (lo == 0 || d == 0) {
      return lo == 0 ? nodes[node_idx].count : 0;
    }
    const size_t half = size_t{1} << (d - 1);
    if (lo < half) {
      return self(self, nodes[node_idx].left, lo, d - 1) +
             nodes[nodes[node_idx].right].count;
    }
    return self(self, nodes[node_idx].right, lo - half, d - 1);
  };

  std::vector<uint32_t> roots;
  roots.reserve(m + 1);
  uint32_t tree = 0;
  roots.push_back(tree);
  std::unordered_map<Value, size_t> previous;
  previous.reserve(std::min<size_t>(m, 4096));
  for (size_t i = 0; i < m; ++i) {
    ASSIGN_OR_RETURN(Value, v, (runtime.EvalAt(arg, rows, ordered[i])));
    if (!v.IsNull()) {
      Value key = CanonicalDistinctValue(v);
      auto [it, inserted] = previous.emplace(std::move(key), i);
      if (!inserted) {
        tree = update_mark(update_mark, tree, it->second, false, depth);
        it->second = i;
      }
      tree = update_mark(update_mark, tree, i, true, depth);
    }
    roots.push_back(tree);
  }

  for (size_t k = 0; k < m; ++k) {
    ASSIGN_OR_RETURN(
        FrameBounds, frame_bounds,
        (WindowRuntime::ResolveFrame(window, rows, ordered, order_values, k,
                                     peer_end, group_ends)));
    const size_t lo = frame_bounds.first;
    const size_t hi = frame_bounds.second;
    if (hi < lo || lo >= m) {
      (*out)[ordered[k]] = Value(int64_t{0});
      continue;
    }
    const size_t hi_excl = std::min(hi + 1, m);
    const uint32_t distinct_cnt =
        query_suffix(query_suffix, roots[hi_excl], lo, depth);
    (*out)[ordered[k]] = Value(static_cast<int64_t>(distinct_cnt));
  }
  return true;
}

Status ComputeOneWindowImpl(TransactionContext& context,
                            const WindowFunctionCallExpression& window,
                            std::vector<Row>& rows, const Schema& schema,
                            const Scope* outer, const CteMap& ctes,
                            std::vector<Value>* out,
                            const WindowOrderLayout* cached_layout,
                            bool uniform_cell_types) {
  const size_t n = rows.size();
  out->assign(n, Value());
  if (n == 0) {
    return Status::kSuccess;
  }

  WindowRuntime runtime{.context = context,
                        .outer = outer,
                        .ctes = &ctes,
                        .schema = &schema,
                        .uniform_cell_types = uniform_cell_types};

  WindowOrderLayout local_layout;
  const WindowOrderLayout* layout = cached_layout;
  if (layout == nullptr) {
    bool need_peer_end = true;
    const bool need_order_values =
        WindowLayoutNeedsOrderValues(window, &need_peer_end);
    ASSIGN_OR_RETURN(
        WindowOrderLayout, built_layout,
        (BuildWindowOrderLayout(window, rows, runtime, std::nullopt,
                                need_order_values, need_peer_end)));
    local_layout = std::move(built_layout);
    layout = &local_layout;
  }

  const std::string& fn = window.function;
  std::optional<size_t> prefix_slot;
  const bool prefix_eligible =
      IsPrefixWindowEligible(window, schema, &prefix_slot);
  if (prefix_eligible && n >= 16384 && layout->partitions.size() > 1) {
    const auto workers = std::min<size_t>(
        {8, layout->partitions.size(),
         QueryParallelWorkerLimit(std::thread::hardware_concurrency())});
    if (workers > 1) {
      // Each partition owns disjoint output positions. This helper only reads
      // column values; contextual expressions stay on the serial evaluator.
      // Prefix arithmetic retains its exact original order within each
      // partition.
      std::vector<Status> errors(workers, Status::kSuccess);
      std::vector<size_t> error_partitions(workers, SIZE_MAX);
      std::atomic<bool> unhandled{false};
      RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
        for (size_t p = worker; p < layout->partitions.size(); p += workers) {
          const auto& partition = layout->partitions[p];
          const auto result = TryComputeColumnPrefix(
              window, rows, prefix_slot, layout->Ordered(partition),
              partition.peer_end, schema, uniform_cell_types, out);
          if (!result.HasValue()) {
            if (errors[worker].ok()) {
              errors[worker] = result.GetStatus();
              error_partitions[worker] = p;
            }
          } else if (!result.Value()) {
            unhandled.store(true, std::memory_order_relaxed);
          }
        }
      }));
      if (!unhandled.load(std::memory_order_relaxed)) {
        const auto first = std::ranges::min_element(error_partitions);
        return *first == SIZE_MAX ? Status::kSuccess
                                  : errors[static_cast<size_t>(
                                        first - error_partitions.begin())];
      }
      // A mixed/unsupported partition needs the general evaluator. Recompute
      // in partition order so its errors retain the serial precedence, too.
    }
  }
  for (const WindowPartitionLayout& partition : layout->partitions) {
    std::span<const size_t> ordered = layout->Ordered(partition);
    const WindowOrderValues& order_values = partition.order_values;
    const std::vector<size_t>& peer_end = partition.peer_end;
    const size_t m = ordered.size();
    // ResolveFrame's GROUPS branch wants the inclusive end position of every
    // peer group in this partition (it binary-searches the current row's
    // group).  Build it once per partition from peer_end.
    std::vector<size_t> group_ends;
    for (size_t i = 0; i < m && i < peer_end.size();) {
      group_ends.push_back(peer_end[i]);
      i = peer_end[i] + 1;
    }

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
        const auto rank = static_cast<int64_t>(k + 1);
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
        return StatusError(StatusCode::kInvalidArgument,
                           "NTILE requires an argument");
      }
      ASSIGN_OR_RETURN(Value, buckets_value,
                       (runtime.EvalAt(window.args[0], rows, ordered[0])));
      if (buckets_value.IsNull()) {
        return StatusError(StatusCode::kInvalidArgument,
                           "The N value (number of buckets) for the NTILE "
                           "function must not be NULL");
      }
      if (buckets_value.type != ValueType::kInt64) {
        return StatusError(StatusCode::kInvalidArgument,
                           "NTILE requires an integer bucket count");
      }
      const int64_t buckets = buckets_value.value.int_value;
      if (buckets <= 0) {
        return StatusError(StatusCode::kInvalidArgument,
                           "The N value (number of buckets) for the NTILE "
                           "function must be positive");
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
      if (window.args.empty()) {
        return StatusError(StatusCode::kInvalidArgument,
                           fn + " requires at least one argument");
      }
      for (size_t k = 0; k < m; ++k) {
        int64_t offset = 1;
        if (window.args.size() > 1) {
          ASSIGN_OR_RETURN(Value, offset_value,
                           (runtime.EvalAt(window.args[1], rows, ordered[k])));
          if (offset_value.IsNull()) {
            return StatusError(
                StatusCode::kInvalidArgument,
                fn == "LAG"
                    ? "The offset to the function LAG must not be NULL"
                    : "The offset to the function LEAD must not be NULL");
          }
          if (offset_value.type != ValueType::kInt64) {
            return StatusError(
                StatusCode::kInvalidArgument,
                fn == "LAG"
                    ? "The offset to the function LAG must be an integer"
                    : "The offset to the function LEAD must be an integer");
          }
          offset = offset_value.value.int_value;
          if (offset < 0) {
            return StatusError(
                StatusCode::kInvalidArgument,
                fn == "LAG"
                    ? "The offset to the function LAG must not be negative"
                    : "The offset to the function LEAD must not be negative");
          }
        }
        // offset >= 0.  LAG's subtraction cannot underflow (k >= 0 keeps the
        // result >= -INT64_MAX); LEAD's addition can overflow for offsets
        // near INT64_MAX, so saturate -- any out-of-range target takes the
        // default-value branch below either way.
        auto target = static_cast<int64_t>(k);
        if (fn == "LAG") {
          target -= offset;
        } else if (__builtin_add_overflow(target, offset, &target)) {
          target = std::numeric_limits<int64_t>::max();
        }
        if (target < 0 || std::cmp_greater_equal(target, m)) {
          if (window.args.size() > 2) {
            ASSIGN_OR_RETURN(
                Value, default_value,
                (runtime.EvalAt(window.args[2], rows, ordered[k])));
            (*out)[ordered[k]] = std::move(default_value);
          }
          continue;
        }
        ASSIGN_OR_RETURN(
            Value, lag_value,
            (runtime.EvalAt(window.args[0], rows,
                            ordered[static_cast<size_t>(target)])));
        (*out)[ordered[k]] = std::move(lag_value);
      }
      continue;
    }
    if (fn == "FIRST_VALUE" || fn == "LAST_VALUE" || fn == "NTH_VALUE") {
      if (window.args.empty()) {
        return StatusError(StatusCode::kInvalidArgument,
                           fn + " requires at least one argument");
      }
      ASSIGN_OR_RETURN(
          bool, nth_non_null_done,
          (TryComputeNthNonNullFrame(window, rows, ordered, order_values,
                                     peer_end, group_ends, runtime, out)));
      if (nth_non_null_done) {
        continue;
      }
      for (size_t k = 0; k < m; ++k) {
        ASSIGN_OR_RETURN(
            FrameBounds, frame_bounds,
            (WindowRuntime::ResolveFrame(window, rows, ordered, order_values, k,
                                         peer_end, group_ends)));
        const size_t lo = frame_bounds.first;
        const size_t hi = frame_bounds.second;
        if (hi < lo) {
          continue;
        }
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
        bool found = false;
        Value selected;
        if (fn == "FIRST_VALUE") {
          for (size_t candidate = lo; candidate <= hi; ++candidate) {
            if (excluded(candidate)) {
              continue;
            }
            if (window.ignore_nulls) {
              ASSIGN_OR_RETURN(
                  Value, candidate_value,
                  (runtime.EvalAt(window.args[0], rows, ordered[candidate])));
              if (candidate_value.IsNull()) {
                continue;
              }
              selected = std::move(candidate_value);
            }
            target = candidate;
            found = true;
            break;
          }
        } else if (fn == "LAST_VALUE") {
          size_t candidate = hi;
          for (;;) {
            if (!excluded(candidate)) {
              if (window.ignore_nulls) {
                ASSIGN_OR_RETURN(
                    Value, candidate_value,
                    (runtime.EvalAt(window.args[0], rows, ordered[candidate])));
                if (candidate_value.IsNull()) {
                  if (candidate == lo) {
                    break;
                  }
                  --candidate;
                  continue;
                }
                selected = std::move(candidate_value);
              }
              target = candidate;
              found = true;
              break;
            }
            if (candidate == lo) {
              break;
            }
            --candidate;
          }
        } else {
          if (window.args.size() < 2) {
            return StatusError(StatusCode::kInvalidArgument,
                               "NTH_VALUE requires two arguments");
          }
          ASSIGN_OR_RETURN(Value, nth_value,
                           (runtime.EvalAt(window.args[1], rows, ordered[k])));
          if (nth_value.IsNull()) {
            return StatusError(
                StatusCode::kInvalidArgument,
                "The N value for the NthValue function must not be NULL");
          }
          if (nth_value.type != ValueType::kInt64) {
            return StatusError(
                StatusCode::kInvalidArgument,
                "The N value for the NthValue function must be an integer");
          }
          int64_t nth = nth_value.value.int_value;
          if (nth <= 0) {
            return StatusError(
                StatusCode::kInvalidArgument,
                "The N value for the NthValue function must be positive");
          }
          size_t seen = 0;
          for (size_t candidate = lo; candidate <= hi; ++candidate) {
            if (excluded(candidate)) {
              continue;
            }
            if (window.ignore_nulls) {
              ASSIGN_OR_RETURN(
                  Value, candidate_value,
                  (runtime.EvalAt(window.args[0], rows, ordered[candidate])));
              if (candidate_value.IsNull()) {
                continue;
              }
              selected = std::move(candidate_value);
            }
            ++seen;
            if (std::cmp_equal(seen, nth)) {
              target = candidate;
              found = true;
              break;
            }
          }
        }
        if (!found) {
          continue;
        }
        if (!window.ignore_nulls) {
          ASSIGN_OR_RETURN(
              Value, selected_value,
              (runtime.EvalAt(window.args[0], rows, ordered[target])));
          selected = std::move(selected_value);
        }
        (*out)[ordered[k]] = std::move(selected);
      }
      continue;
    }

    ASSIGN_OR_RETURN(bool, prefix_done,
                     TryComputeColumnPrefix(window, rows, prefix_slot, ordered,
                                            partition.peer_end, schema,
                                            uniform_cell_types, out));
    if (prefix_done) {
      continue;
    }

    ASSIGN_OR_RETURN(
        bool, distinct_done,
        (TryComputeSlidingDistinctRows(window, rows, ordered, runtime, out)));
    if (distinct_done) {
      continue;
    }

    ASSIGN_OR_RETURN(
        bool, range_distinct_done,
        (TryComputeRangeDistinctIndex(window, rows, ordered, order_values,
                                      peer_end, group_ends, runtime, out)));
    if (range_distinct_done) {
      continue;
    }

    // Everything else is treated as an aggregate over the frame.
    // PERCENTILE_CONT/DISC are analytic functions: they ignore the window
    // frame and always see the whole partition.
    const bool frame_ignoring =
        fn == "PERCENTILE_CONT" || fn == "PERCENTILE_DISC";
    // P349: when every row of the partition resolves to the same frame, the
    // aggregate is a per-partition constant — evaluate it once and broadcast
    // instead of re-running the argument evaluation, survivor filter, sort /
    // frequency scan m times per partition (PERCENTILE was O(m² log m),
    // frame-less MODE() OVER (PARTITION BY k) even O(m³), results identical
    // on every row).  Frame-ignoring functions always qualify; ordinary
    // aggregates qualify with the default (or explicit UNBOUNDED PRECEDING
    // .. CURRENT ROW) RANGE frame and no ORDER BY, where every row is a
    // peer of every other.  EXCLUDE clauses and ROWS/GROUPS-bounded frames
    // keep per-row resolution because their bounds depend on the current
    // row.  The hoist only fires for column/constant arguments and no
    // per-row filter, mirroring IsPrefixWindowEligible's determinism gate so
    // volatile arguments keep their per-row (re-)evaluation semantics.
    {
      const bool deterministic_inputs =
          !window.where_filter &&
          std::all_of(window.args.begin(), window.args.end(),
                      [](const Expression& arg) {
                        return arg && (arg->Type() == TypeTag::kColumnValue ||
                                       arg->Type() == TypeTag::kConstantValue);
                      });
      const bool default_whole_frame =
          window.order_by.empty() &&
          window.frame_unit != WindowFrameUnit::kRows &&
          // GROUPS frames keep per-row resolution: ResolveFrame raises
          // diagnostics (e.g. "GROUPS frame requires an ORDER BY key") that
          // the broadcast path would bypass.
          window.frame_unit != WindowFrameUnit::kGroups &&
          window.frame_start.type ==
              WindowFrameBoundType::kUnboundedPreceding &&
          !window.frame_start.offset &&
          window.frame_end.type == WindowFrameBoundType::kCurrentRow &&
          !window.frame_end.offset;
      if (deterministic_inputs &&
          window.exclusion == WindowFrameExclusion::kNone &&
          (frame_ignoring || default_whole_frame)) {
        ASSIGN_OR_RETURN(
            Value, agg_value,
            (runtime.AggregateOverFrame(window, rows, ordered, order_values, 0,
                                        0, m - 1)));
        for (size_t k = 0; k < m; ++k) {
          (*out)[ordered[k]] = agg_value;
        }
        continue;
      }
    }
    for (size_t k = 0; k < m; ++k) {
      FrameBounds frame_bounds{0, m - 1};
      if (!frame_ignoring) {
        ASSIGN_OR_RETURN(
            FrameBounds, resolved,
            (WindowRuntime::ResolveFrame(window, rows, ordered, order_values, k,
                                         peer_end, group_ends)));
        frame_bounds = resolved;
      }
      ASSIGN_OR_RETURN(Value, agg_value,
                       (runtime.AggregateOverFrame(
                           window, rows, ordered, order_values, k,
                           frame_bounds.first, frame_bounds.second)));
      (*out)[ordered[k]] = std::move(agg_value);
    }
  }
  return Status::kSuccess;
}

bool HasWindowFunctions(const SelectStatement& statement) {
  if (statement.Qualify() != nullptr) {
    return true;
  }
  for (const NamedExpression& projection : statement.SelectList()) {
    if (ContainsWindow(projection.expression)) {
      return true;
    }
  }
  return std::ranges::any_of(statement.OrderBy(),
                             [](const SelectStatement::OrderByTerm& term) {
                               return ContainsWindow(term.expression);
                             });
}

namespace {

// QUALIFY may reference select-list aliases; inline them so filtering works
// against pre-projection rows.  Real FROM columns win over aliases.
Expression InlineAliases(
    const Expression& expression,
    const std::unordered_map<std::string, Expression>& aliases,
    const Schema& schema) {
  if (!expression) {
    return expression;
  }
  switch (expression->Type()) {
    case TypeTag::kColumnValue: {
      const ColumnName& name = expression->AsColumnValue().GetColumnName();
      if (!name.schema.empty() || schema.Offset(name) >= 0) {
        return expression;
      }
      const auto found = aliases.find(name.name);
      return found != aliases.end() ? found->second : expression;
    }
    case TypeTag::kBinaryExp:
      return BinaryExpressionExp(
          InlineAliases(expression->AsBinaryExpression().Left(), aliases,
                        schema),
          expression->AsBinaryExpression().Op(),
          InlineAliases(expression->AsBinaryExpression().Right(), aliases,
                        schema));
    case TypeTag::kUnaryExp:
      return UnaryExpressionExp(
          InlineAliases(expression->AsUnaryExpression().Child(), aliases,
                        schema),
          expression->AsUnaryExpression().Op());
    case TypeTag::kCaseExp: {
      const auto& value = expression->AsCaseExpression();
      std::vector<std::pair<Expression, Expression>> clauses;
      clauses.reserve(value.when_clauses_.size());
      for (const auto& [condition, result] : value.when_clauses_) {
        clauses.emplace_back(InlineAliases(condition, aliases, schema),
                             InlineAliases(result, aliases, schema));
      }
      return CaseExpressionExp(
          std::move(clauses),
          InlineAliases(value.else_clause_, aliases, schema));
    }
    case TypeTag::kFunctionCallExp:
      return FunctionCallExp(
          expression->AsFunctionCallExpression().FuncName(), [&] {
            std::vector<Expression> out;
            for (const Expression& argument :
                 expression->AsFunctionCallExpression().Args()) {
              out.push_back(InlineAliases(argument, aliases, schema));
            }
            return out;
          }());
    case TypeTag::kQueryExp:
      // A subquery has its own scope; outer SELECT aliases do not apply.
      return expression;
    default: {
      // Every other composite node (IN lists, casts, array literals, ...)
      // is rebuilt through the shared children helper so aliases inside
      // them inline as well.
      std::vector<Expression> children = ExpressionChildren(expression);
      bool changed = false;
      for (Expression& child : children) {
        if (!child) {
          continue;
        }
        Expression mapped = InlineAliases(child, aliases, schema);
        if (mapped != child) {
          changed = true;
          child = std::move(mapped);
        }
      }
      return changed ? WithExpressionChildren(expression, std::move(children))
                     : expression;
    }
  }
}

}  // namespace

// Single-window entry point for the Cascades WindowExecutor, which plans
// each call through the memo instead of the statement-level ApplyWindows
// above. Layout caching stays internal (pass-through null).
Status ComputeOneWindow(TransactionContext& context,
                        const WindowFunctionCallExpression& window,
                        std::vector<Row>& rows, const Schema& schema,
                        const Scope* outer, const CteMap& ctes,
                        std::vector<Value>* out) {
  return ComputeOneWindowImpl(context, window, rows, schema, outer, ctes, out,
                              nullptr);
}

Status ComputeWindowColumns(
    TransactionContext& context,
    const std::vector<const WindowFunctionCallExpression*>& windows,
    std::vector<Row>& rows, const Schema& schema, const Scope* outer,
    const CteMap& ctes, std::vector<std::vector<Value>>* outputs) {
  outputs->assign(windows.size(), {});
  if (windows.empty() || rows.empty()) {
    return Status::kSuccess;
  }

  // Two calls may share a layout only if both their PARTITION/ORDER shape and
  // their layout-detail requirements agree. A layout built for ROW_NUMBER
  // (no peer/order values) reused by a RANGE frame would silently change the
  // frame, so the requirement flags are part of the key.
  const auto cache_key = [](const WindowFunctionCallExpression& window) {
    bool need_peer_end = true;
    const bool need_order_values =
        WindowLayoutNeedsOrderValues(window, &need_peer_end);
    std::string key = WindowLayoutKey(window);
    key.append(need_order_values ? "|V" : "|v");
    key.append(need_peer_end ? "|P" : "|p");
    return key;
  };
  struct Requirement {
    bool order_values{false};
    bool peer_end{false};
  };
  std::unordered_map<std::string, Requirement> requirements;
  for (const WindowFunctionCallExpression* window : windows) {
    Requirement& req = requirements[cache_key(*window)];
    bool need_peer_end = true;
    req.order_values |= WindowLayoutNeedsOrderValues(*window, &need_peer_end);
    req.peer_end |= need_peer_end;
  }

  WindowRuntime runtime{.context = context,
                        .outer = outer,
                        .ctes = &ctes,
                        .schema = &schema,
                        .uniform_cell_types = false};
  std::unordered_map<std::string, WindowOrderLayout> layouts;
  // Keep call order for deterministic error precedence; layouts are built on
  // first use from the union of the requirements of every call in the group.
  for (size_t i = 0; i < windows.size(); ++i) {
    const std::string key = cache_key(*windows[i]);
    auto it = layouts.find(key);
    if (it == layouts.end()) {
      const Requirement req = requirements.at(key);
      ASSIGN_OR_RETURN(
          WindowOrderLayout, built,
          (BuildWindowOrderLayout(*windows[i], rows, runtime, std::nullopt,
                                  req.order_values, req.peer_end)));
      it = layouts.emplace(key, std::move(built)).first;
    }
    RETURN_IF_FAIL(ComputeOneWindowImpl(context, *windows[i], rows, schema,
                                        outer, ctes, &(*outputs)[i],
                                        &it->second, false));
  }
  return Status::kSuccess;
}

namespace {

Status FinalizeWindowedStatement(TransactionContext& context,
                                 const SelectStatement& statement,
                                 const Scope* outer, const CteMap& ctes,
                                 const ReplacementMap& replacements,
                                 size_t hidden_count, Relation&& window_output,
                                 WindowedInput* result) {
  std::vector<NamedExpression> rewritten_select;
  rewritten_select.reserve(statement.SelectList().size());
  std::unordered_map<std::string, Expression> aliases;
  aliases.reserve(statement.SelectList().size());
  for (const NamedExpression& projection : statement.SelectList()) {
    Expression rebuilt = Rebuild(projection.expression, replacements);
    rewritten_select.emplace_back(projection.name, rebuilt);
    if (!projection.name.empty()) {
      aliases.emplace(projection.name, rebuilt);
    }
  }
  std::vector<SelectStatement::OrderByTerm> rewritten_order;
  rewritten_order.reserve(statement.OrderBy().size());
  for (const SelectStatement::OrderByTerm& term : statement.OrderBy()) {
    rewritten_order.push_back({Rebuild(term.expression, replacements),
                               term.ascending, term.nulls_first});
  }
  result->statement->SetSelectList(std::move(rewritten_select));
  result->statement->SetOrderBy(std::move(rewritten_order));

  if (statement.Qualify()) {
    Expression qualify =
        InlineAliases(Rebuild(statement.Qualify(), replacements), aliases,
                      window_output.schema);
    RETURN_IF_FAIL(
        FilterRelation(context, &window_output, {qualify}, outer, ctes));
  }
  result->input = std::move(window_output);
  result->hidden_columns = hidden_count;
  return Status::kSuccess;
}

}  // namespace

StatusOr<WindowedInput> ApplyWindows(TransactionContext& context,
                                     const SelectStatement& statement,
                                     Relation&& input, const Scope* outer,
                                     const CteMap& ctes,
                                     bool allow_window_halo) {
  double* window_ms = nullptr;
  if (auto* runtime_stats = context.execution_runtime()) {
    window_ms = &runtime_stats->window_ms;
  }
  PhaseTimer window_timer(window_ms);
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
    if (statement.Qualify()) {
      std::unordered_map<std::string, Expression> aliases;
      aliases.reserve(statement.SelectList().size());
      for (const NamedExpression& projection : statement.SelectList()) {
        if (!projection.name.empty()) {
          aliases.emplace(projection.name, projection.expression);
        }
      }
      Expression qualify =
          InlineAliases(statement.Qualify(), aliases, input.schema);
      RETURN_IF_FAIL(FilterRelation(context, &input, {qualify}, outer, ctes));
    }
    result.input = std::move(input);
    return result;
  }

  std::optional<size_t> rank_bound;
  static const bool rank_bound_disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_RANK_BOUND") != nullptr;
  if (!rank_bound_disabled && windows.size() == 1 && statement.Qualify()) {
    const auto* window = windows.begin()->first;
    std::unordered_map<std::string, Expression> aliases;
    for (const auto& projection : statement.SelectList()) {
      if (!projection.name.empty()) {
        aliases.emplace(projection.name, projection.expression);
      }
    }
    const auto qualify =
        InlineAliases(statement.Qualify(), aliases, input.schema);
    if (window->function == "RANK" && window->args.empty() &&
        !window->where_filter && !window->distinct &&
        window->inner_order_by.empty() && !window->inner_limit &&
        !window->has_frame && !window->order_by.empty() &&
        qualify->Type() == TypeTag::kBinaryExp) {
      const auto& predicate = qualify->AsBinaryExpression();
      if (AsWindow(predicate.Left()) == window && predicate.Right() &&
          predicate.Right()->Type() == TypeTag::kConstantValue &&
          (predicate.Op() == BinaryOperation::kLessThanEquals ||
           predicate.Op() == BinaryOperation::kLessThan)) {
        const auto& value = predicate.Right()->AsConstantValue().GetValue();
        if (value.type == ValueType::kInt64 && !value.IsUnsigned() &&
            value.value.int_value > 0) {
          const int64_t bound =
              value.value.int_value -
              (predicate.Op() == BinaryOperation::kLessThan ? 1 : 0);
          if (bound > 0 && std::in_range<size_t>(bound)) {
            rank_bound = static_cast<size_t>(bound);
            if (auto* runtime_stats = context.execution_runtime()) {
              ++runtime_stats->rank_bounds;
            }
          }
        }
      }
    }
  }

  RETURN_IF_FAIL(input.FinishSpill());
  RETURN_IF_FAIL(input.GetStatus());

  // Lean proof: lean/TinylambOpt/WindowHalo.lean (window_halo_sound)
  // When unpartitioned bounded-ROWS window calls share the outer ORDER BY
  // under a deterministic LIMIT/OFFSET, extract only the sorted halo slice
  // `[offset - max_preceding, min(n, offset + limit + max_following))` before
  // running window evaluation.
  const char* disable_halo_env = ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_WINDOW_HALO");
  if ((disable_halo_env == nullptr || disable_halo_env[0] != '1') &&
      allow_window_halo && !rank_bound && !input.HasSpill() &&
      statement.HasLimit() && statement.Limit() > 0 && !statement.WithTies() &&
      !statement.Qualify() && !statement.Distinct() &&
      !statement.HasDistinctOn() && statement.GroupBy().empty() &&
      !statement.Having() && !statement.OrderBy().empty() &&
      statement.Offset() <=
          std::numeric_limits<size_t>::max() - statement.Limit()) {
    WindowNodeMap order_windows;
    for (const SelectStatement::OrderByTerm& term : statement.OrderBy()) {
      CollectWindows(term.expression, &order_windows);
    }
    std::vector<slot_t> order_slots;
    std::vector<SortKeyOrder> order_specs;
    // WindowHalo trims input only to supply frame halos for window calls in
    // the outer ORDER BY. Ordinary ORDER BY/LIMIT belongs to the normal
    // sort-limit path and must never enter this rewrite.
    bool order_ok = !order_windows.empty();
    if (order_ok) {
      order_slots.reserve(statement.OrderBy().size());
      order_specs.reserve(statement.OrderBy().size());
      for (const SelectStatement::OrderByTerm& term : statement.OrderBy()) {
        if (!term.expression ||
            term.expression->Type() != TypeTag::kColumnValue) {
          order_ok = false;
          break;
        }
        const ColumnName& cn = term.expression->AsColumnValue().GetColumnName();
        if (cn.name == "*") {
          order_ok = false;
          break;
        }
        const auto off = LocalColumnOffset(input.schema, cn);
        if (!off.has_value()) {
          order_ok = false;
          break;
        }
        if (cn.schema.empty()) {
          for (const NamedExpression& proj : statement.SelectList()) {
            if (!proj.name.empty() && IdentifierEquals(proj.name, cn.name)) {
              std::optional<slot_t> proj_off;
              if (proj.expression &&
                  proj.expression->Type() == TypeTag::kColumnValue) {
                proj_off = LocalColumnOffset(
                    input.schema,
                    proj.expression->AsColumnValue().GetColumnName());
              }
              if (proj_off != off) {
                order_ok = false;
                break;
              }
            }
          }
          if (!order_ok) {
            break;
          }
        }
        order_slots.push_back(*off);
        order_specs.emplace_back(term.ascending, term.nulls_first,
                                 CollationOfExpression(term.expression));
      }
    }

    auto halo_expr_safe = [&](auto&& self, const Expression& expr) -> bool {
      if (!expr) {
        return false;
      }
      switch (expr->Type()) {
        case TypeTag::kWindowFunctionExp:
          return true;
        case TypeTag::kColumnValue: {
          const ColumnName& cn = expr->AsColumnValue().GetColumnName();
          return cn.name != "*" &&
                 LocalColumnOffset(input.schema, cn).has_value();
        }
        case TypeTag::kConstantValue:
          return dynamic_cast<const ParameterSlot*>(expr.get()) == nullptr;
        case TypeTag::kBinaryExp: {
          const auto op = expr->AsBinaryExpression().Op();
          if (op == BinaryOperation::kDivide ||
              op == BinaryOperation::kModulo || op == BinaryOperation::kAdd ||
              op == BinaryOperation::kSubtract ||
              op == BinaryOperation::kMultiply ||
              op == BinaryOperation::kShiftLeft ||
              op == BinaryOperation::kShiftRight) {
            return false;
          }
          return self(self, expr->AsBinaryExpression().Left()) &&
                 self(self, expr->AsBinaryExpression().Right());
        }
        default:
          return false;
      }
    };
    if (order_ok) {
      for (const NamedExpression& item : statement.SelectList()) {
        if (!halo_expr_safe(halo_expr_safe, item.expression)) {
          order_ok = false;
          break;
        }
      }
    }

    size_t max_preceding = 0;
    size_t max_following = 0;
    auto parse_nonneg_rows_off = [](const WindowFrameBound& b,
                                    bool* ok) -> size_t {
      if (!b.offset || b.offset->Type() != TypeTag::kConstantValue) {
        *ok = false;
        return 0;
      }
      const Value& val = b.offset->AsConstantValue().GetValue();
      if (val.IsNull() || val.type != ValueType::kInt64 || val.IsUnsigned() ||
          val.value.int_value < 0) {
        *ok = false;
        return 0;
      }
      return static_cast<size_t>(val.value.int_value);
    };
    if (order_ok) {
      for (const auto& [win_ptr, win_idx] : windows) {
        (void)win_idx;
        if (win_ptr == nullptr || !win_ptr->partition_by.empty() ||
            !win_ptr->has_frame ||
            win_ptr->frame_unit != WindowFrameUnit::kRows ||
            win_ptr->exclusion != WindowFrameExclusion::kNone ||
            win_ptr->where_filter || !win_ptr->inner_order_by.empty() ||
            win_ptr->inner_limit.has_value() ||
            (win_ptr->function != "SUM" && win_ptr->function != "COUNT" &&
             win_ptr->function != "AVG" && win_ptr->function != "MIN" &&
             win_ptr->function != "MAX")) {
          order_ok = false;
          break;
        }
        for (const Expression& arg : win_ptr->args) {
          if (!arg ||
              (arg->Type() != TypeTag::kColumnValue &&
               (arg->Type() != TypeTag::kConstantValue ||
                dynamic_cast<const ParameterSlot*>(arg.get()) != nullptr))) {
            order_ok = false;
            break;
          }
          if (arg->Type() == TypeTag::kColumnValue) {
            const ColumnName& cn = arg->AsColumnValue().GetColumnName();
            if (cn.name != "*" &&
                !LocalColumnOffset(input.schema, cn).has_value()) {
              order_ok = false;
              break;
            }
          }
        }
        if (!order_ok || win_ptr->order_by.size() != order_slots.size()) {
          order_ok = false;
          break;
        }
        for (size_t j = 0; j < order_slots.size(); ++j) {
          const WindowOrderTerm& wt = win_ptr->order_by[j];
          const SelectStatement::OrderByTerm& ot = statement.OrderBy()[j];
          if (!wt.expression ||
              wt.expression->Type() != TypeTag::kColumnValue) {
            order_ok = false;
            break;
          }
          const auto wslot = LocalColumnOffset(
              input.schema, wt.expression->AsColumnValue().GetColumnName());
          if (!wslot.has_value() || *wslot != order_slots[j] ||
              wt.ascending != ot.ascending ||
              wt.nulls_first.value_or(wt.ascending) !=
                  ot.nulls_first.value_or(ot.ascending) ||
              CollationOfExpression(wt.expression) !=
                  CollationOfExpression(ot.expression)) {
            order_ok = false;
            break;
          }
        }
        if (!order_ok) {
          break;
        }
        bool bounds_ok = true;
        switch (win_ptr->frame_start.type) {
          case WindowFrameBoundType::kOffsetPreceding:
            max_preceding = std::max(
                max_preceding,
                parse_nonneg_rows_off(win_ptr->frame_start, &bounds_ok));
            break;
          case WindowFrameBoundType::kCurrentRow:
            break;
          case WindowFrameBoundType::kOffsetFollowing:
            // A FOLLOWING lower bound does not require any input before the
            // current row; parse only to reject invalid offsets.
            (void)parse_nonneg_rows_off(win_ptr->frame_start, &bounds_ok);
            break;
          case WindowFrameBoundType::kUnboundedPreceding:
          case WindowFrameBoundType::kUnboundedFollowing:
            bounds_ok = false;
            break;
        }
        switch (win_ptr->frame_end.type) {
          case WindowFrameBoundType::kOffsetFollowing:
            max_following =
                std::max(max_following,
                         parse_nonneg_rows_off(win_ptr->frame_end, &bounds_ok));
            break;
          case WindowFrameBoundType::kCurrentRow:
            break;
          case WindowFrameBoundType::kOffsetPreceding:
            // A PRECEDING upper bound does not require any input after the
            // current row; parse only to reject invalid offsets.
            (void)parse_nonneg_rows_off(win_ptr->frame_end, &bounds_ok);
            break;
          case WindowFrameBoundType::kUnboundedPreceding:
          case WindowFrameBoundType::kUnboundedFollowing:
            bounds_ok = false;
            break;
        }
        if (!bounds_ok) {
          order_ok = false;
          break;
        }
      }
    }

    const size_t total_n = input.HasColumnarOnly() ? input.columnar->total_rows
                                                   : input.rows.size();
    if (order_ok && total_n > 0 && statement.Offset() < total_n) {
      const size_t halo_lo = statement.Offset() > max_preceding
                                 ? (statement.Offset() - max_preceding)
                                 : 0;
      const size_t out_end = statement.Offset() + statement.Limit();
      const size_t raw_halo_hi =
          out_end <= std::numeric_limits<size_t>::max() - max_following
              ? (out_end + max_following)
              : std::numeric_limits<size_t>::max();
      const size_t halo_hi = std::min(total_n, raw_halo_hi);
      if (halo_hi > halo_lo && (halo_hi - halo_lo) < total_n) {
        std::vector<size_t> idx(total_n);
        std::iota(idx.begin(), idx.end(), 0);
        const auto order_halo_indices = [&](const auto& compare) {
          // Partition off the rows before the requested halo, then sort only
          // the halo-sized prefix of the remainder. Sorting [0, halo_hi)
          // would spend O(halo_hi log halo_hi) work on rows we discard.
          if (halo_lo != 0) {
            std::nth_element(idx.begin(),
                             idx.begin() + static_cast<std::ptrdiff_t>(halo_lo),
                             idx.end(), compare);
          }
          std::partial_sort(idx.begin() + static_cast<std::ptrdiff_t>(halo_lo),
                            idx.begin() + static_cast<std::ptrdiff_t>(halo_hi),
                            idx.end(), compare);
        };
        Relation halo_input(context.execution_runtime());
        halo_input.schema = input.schema;
        halo_input.uniform_cell_types = input.uniform_cell_types;
        CopyExecutionStats(&halo_input, input);

        if (input.HasColumnarOnly()) {
          const auto& col_tbl = *input.columnar;
          std::vector<size_t> chunk_offsets;
          chunk_offsets.reserve(col_tbl.chunks.size() + 1);
          chunk_offsets.push_back(0);
          for (const auto& ch : col_tbl.chunks) {
            if (ch.columns.size() != halo_input.schema.ColumnCount()) {
              return StatusError(
                  StatusCode::kCorrupt,
                  "window halo columnar chunk does not match input schema");
            }
            chunk_offsets.push_back(chunk_offsets.back() + ch.rows);
          }
          std::vector<std::vector<Value>> sort_cols(
              order_slots.size(), std::vector<Value>(total_n));
          size_t base_r = 0;
          for (const auto& ch : col_tbl.chunks) {
            for (size_t j = 0; j < order_slots.size(); ++j) {
              const ColumnarColumn& ccol = ch.columns[order_slots[j]];
              for (size_t r = 0; r < ch.rows; ++r) {
                sort_cols[j][base_r + r] = ccol.ValueAt(r);
              }
            }
            base_r += ch.rows;
          }
          auto cmp_col = [&](size_t a, size_t b) {
            for (size_t j = 0; j < order_specs.size(); ++j) {
              const int c =
                  order_specs[j].Compare(sort_cols[j][a], sort_cols[j][b]);
              if (c != 0) {
                return c < 0;
              }
            }
            return a < b;
          };
          order_halo_indices(cmp_col);
          halo_input.rows.reserve(halo_hi - halo_lo);
          for (size_t pos = halo_lo; pos < halo_hi; ++pos) {
            const size_t global_r = idx[pos];
            const auto it = std::upper_bound(chunk_offsets.begin(),
                                             chunk_offsets.end(), global_r);
            const size_t ci =
                static_cast<size_t>(it - chunk_offsets.begin()) - 1;
            const size_t local_r = global_r - chunk_offsets[ci];
            Row row = col_tbl.chunks[ci].MaterializeRow(local_r);
            if (row.values_.size() != halo_input.schema.ColumnCount()) {
              return StatusError(
                  StatusCode::kCorrupt,
                  "window halo materialized row does not match input schema");
            }
            halo_input.AddRow(std::move(row));
          }
        } else {
          auto cmp_row = [&](size_t a, size_t b) {
            for (size_t j = 0; j < order_specs.size(); ++j) {
              const int c = order_specs[j].Compare(
                  input.rows[a][order_slots[j]], input.rows[b][order_slots[j]]);
              if (c != 0) {
                return c < 0;
              }
            }
            return a < b;
          };
          order_halo_indices(cmp_row);
          halo_input.rows.reserve(halo_hi - halo_lo);
          for (size_t pos = halo_lo; pos < halo_hi; ++pos) {
            halo_input.AddRow(std::move(input.rows[idx[pos]]));
          }
        }
        RETURN_IF_FAIL(halo_input.FinishSpill());
        input = std::move(halo_input);
        // Keep LIMIT unchanged: the halo contains the requested output slice
        // plus following rows used only to evaluate its window frames. Shifting
        // OFFSET by halo_lo keeps the post-window slice at the same rows.
        result.statement->SetOffset(statement.Offset() - halo_lo);
      }
    }
  }

  if (!rank_bound && input.HasColumnarOnly()) {
    if (auto* runtime_stats = context.execution_runtime()) {
      runtime_stats->window_input_rows += input.columnar->total_rows;
    }
    ReplacementMap replacements;
    std::vector<const WindowFunctionCallExpression*> win_nodes;
    std::vector<std::string> win_names;
    win_nodes.reserve(windows.size());
    win_names.reserve(windows.size());
    for (const auto& [window_node, index] : windows) {
      std::string name = "$win" + std::to_string(index);
      replacements.emplace(window_node, ColumnValueExp(ColumnName(name)));
      win_nodes.push_back(window_node);
      win_names.push_back(std::move(name));
    }
    ASSIGN_OR_RETURN(Relation, window_output,
                     (ExtendColumnarRelationWithWindowsImpl(
                         context, std::move(input), win_nodes, win_names, outer,
                         ctes, std::nullopt, /*record_window_timer=*/false)));
    RETURN_IF_FAIL(FinalizeWindowedStatement(
        context, statement, outer, ctes, replacements, windows.size(),
        std::move(window_output), &result));
    return result;
  }

  RETURN_IF_FAIL(input.EnsureRowsMaterializedChecked());
  std::vector<Row> rows;
  if (!input.HasSpill()) {
    // Keep the input's charge alive while the windows borrow these rows.
    // Moving the resident rows avoids copying every string and Value twice.
    rows = std::move(input.rows);
  } else {
    rows.reserve(input.TotalRows());
    RETURN_IF_FAIL(
        input.ForEachRow([&rows](const Row& row) { rows.push_back(row); }));
  }
  if (auto* runtime_stats = context.execution_runtime()) {
    runtime_stats->window_input_rows += rows.size();
  }
  const Schema base_schema = input.schema;

  ReplacementMap replacements;
  std::vector<Column> extended_columns;
  const size_t base_width = base_schema.ColumnCount();
  extended_columns.reserve(base_width + windows.size());
  for (size_t i = 0; i < base_width; ++i) {
    extended_columns.push_back(base_schema.GetColumn(i));
  }
  for (const auto& [window_node, index] : windows) {
    const std::string name = "$win" + std::to_string(index);
    replacements.emplace(window_node, ColumnValueExp(ColumnName(name)));
    extended_columns.emplace_back(name, ValueType::kNull);
  }

  // Compute every hidden column.
  std::vector<const WindowFunctionCallExpression*> win_nodes;
  win_nodes.reserve(windows.size());
  for (const auto& [window_node, index] : windows) {
    (void)index;
    win_nodes.push_back(window_node);
  }
  const size_t win_count = win_nodes.size();
  std::unordered_map<std::string, WindowOrderLayout> layouts;
  std::vector<WindowColTypeSummary> final_types(win_count);
  bool fused_direct_done = false;
  const size_t append_workers =
      rows.size() < 16384
          ? 1
          : std::min<size_t>({8, rows.size() / 8192,
                              QueryParallelWorkerLimit(
                                  std::thread::hardware_concurrency())});

  if (!rank_bound && !input.HasSpill() && !rows.empty() && win_count > 0) {
    const std::string first_layout_key = WindowLayoutKey(*win_nodes[0]);
    bool single_layout = true;
    std::vector<std::optional<size_t>> prefix_slots(win_count);
    bool all_prefix_eligible = true;
    for (size_t w = 0; w < win_count; ++w) {
      if (WindowLayoutKey(*win_nodes[w]) != first_layout_key) {
        single_layout = false;
      }
      if (!IsPrefixWindowEligible(*win_nodes[w], base_schema,
                                  &prefix_slots[w])) {
        all_prefix_eligible = false;
      }
    }
    if (single_layout && all_prefix_eligible) {
      bool need_peer_end = false;
      bool need_order_values = false;
      for (const auto* window_node : win_nodes) {
        bool win_peer = true;
        const bool win_vals =
            WindowLayoutNeedsOrderValues(*window_node, &win_peer);
        need_order_values = need_order_values || win_vals;
        need_peer_end = need_peer_end || win_peer;
      }
      WindowRuntime runtime{.context = context,
                            .outer = outer,
                            .ctes = &ctes,
                            .schema = &base_schema,
                            .uniform_cell_types = input.uniform_cell_types};
      ASSIGN_OR_RETURN(
          WindowOrderLayout, built_layout,
          (BuildWindowOrderLayout(*win_nodes[0], rows, runtime, std::nullopt,
                                  need_order_values, need_peer_end)));
      const size_t fused_workers =
          rows.size() < 16384 || built_layout.partitions.size() <= 1
              ? 1
              : std::min<size_t>({8, built_layout.partitions.size(),
                                  QueryParallelWorkerLimit(
                                      std::thread::hardware_concurrency())});
      std::vector<std::vector<Status>> errors(
          win_count, std::vector<Status>(fused_workers, Status::kSuccess));
      std::vector<std::vector<size_t>> error_partitions(
          win_count, std::vector<size_t>(fused_workers, SIZE_MAX));
      std::vector<std::vector<WindowColTypeSummary>> worker_types(
          fused_workers, std::vector<WindowColTypeSummary>(win_count));
      std::atomic<bool> any_unhandled{false};
      RETURN_IF_FAIL(RunParallelTasks(fused_workers, [&](size_t worker) {
        auto& local_types = worker_types[worker];
        for (size_t p = worker; p < built_layout.partitions.size();
             p += fused_workers) {
          const auto& partition = built_layout.partitions[p];
          std::span<const size_t> ordered = built_layout.Ordered(partition);
          for (size_t w = 0; w < win_count; ++w) {
            WindowColTypeSummary& summary = local_types[w];
            const auto eval_result = TryComputeColumnPrefixImpl(
                *win_nodes[w], rows, *prefix_slots[w], ordered,
                partition.peer_end, base_schema, input.uniform_cell_types,
                [&](size_t row_idx, Value&& val) {
                  summary.Observe(val);
                  auto& vals = rows[row_idx].values_;
                  if (w == 0) {
                    vals.reserve(base_width + win_count);
                  }
                  vals.push_back(std::move(val));
                });
            if (!eval_result.HasValue()) {
              if (errors[w][worker].ok()) {
                errors[w][worker] = eval_result.GetStatus();
                error_partitions[w][worker] = p;
              }
              break;
            }
            if (!eval_result.Value()) {
              any_unhandled.store(true, std::memory_order_relaxed);
              break;
            }
          }
        }
      }));
      if (any_unhandled.load(std::memory_order_relaxed)) {
        RETURN_IF_FAIL(RunParallelTasks(append_workers, [&](size_t worker) {
          const size_t begin = rows.size() * worker / append_workers;
          const size_t end = rows.size() * (worker + 1) / append_workers;
          for (size_t r = begin; r < end; ++r) {
            if (rows[r].values_.size() > base_width) {
              rows[r].values_.resize(base_width);
            }
          }
        }));
        layouts.emplace(first_layout_key, std::move(built_layout));
      } else {
        for (size_t w = 0; w < win_count; ++w) {
          const auto first = std::ranges::min_element(error_partitions[w]);
          if (*first != SIZE_MAX) {
            return errors[w][static_cast<size_t>(first -
                                                 error_partitions[w].begin())];
          }
          for (size_t worker = 0; worker < fused_workers; ++worker) {
            final_types[w].MergeFrom(worker_types[worker][w]);
          }
        }
        fused_direct_done = true;
      }
    }
  }

  std::vector<uint8_t> retained;
  if (!fused_direct_done) {
    std::vector<std::vector<Value>> computed;
    computed.reserve(win_count);
    for (const auto& [window_node, index] : windows) {
      (void)index;
      const std::string layout_key = WindowLayoutKey(*window_node);
      auto [layout_it, inserted] = layouts.try_emplace(layout_key);
      if (inserted) {
        bool need_peer_end = false;
        bool need_order_values = false;
        for (const auto& [other_node, other_idx] : windows) {
          (void)other_idx;
          if (WindowLayoutKey(*other_node) == layout_key) {
            bool win_peer = true;
            const bool win_vals =
                WindowLayoutNeedsOrderValues(*other_node, &win_peer);
            need_order_values = need_order_values || win_vals;
            need_peer_end = need_peer_end || win_peer;
          }
        }
        WindowRuntime runtime{.context = context,
                              .outer = outer,
                              .ctes = &ctes,
                              .schema = &base_schema,
                              .uniform_cell_types = input.uniform_cell_types};
        ASSIGN_OR_RETURN(
            WindowOrderLayout, built_layout,
            (BuildWindowOrderLayout(*window_node, rows, runtime, rank_bound,
                                    need_order_values, need_peer_end)));
        layout_it->second = std::move(built_layout);
      }
      computed.emplace_back();
      RETURN_IF_FAIL(ComputeOneWindowImpl(
          context, *window_node, rows, base_schema, outer, ctes,
          &computed.back(), &layout_it->second, input.uniform_cell_types));
    }
    if (rank_bound) {
      retained.resize(rows.size());
      for (size_t r = 0; r < rows.size(); ++r) {
        const auto& rank = computed.front()[r];
        retained[r] = static_cast<uint8_t>(
            !rank.IsNull() &&
            std::cmp_less_equal(rank.value.int_value, *rank_bound));
      }
    }
    std::vector<std::vector<WindowColTypeSummary>> worker_types(
        append_workers, std::vector<WindowColTypeSummary>(win_count));
    RETURN_IF_FAIL(RunParallelTasks(append_workers, [&](size_t worker) {
      const size_t begin = rows.size() * worker / append_workers;
      const size_t end = rows.size() * (worker + 1) / append_workers;
      auto& local_types = worker_types[worker];
      for (size_t r = begin; r < end; ++r) {
        if (rank_bound && retained[r] == 0) {
          continue;
        }
        rows[r].values_.reserve(base_width + win_count);
        for (size_t w = 0; w < win_count; ++w) {
          local_types[w].Observe(computed[w][r]);
          rows[r].values_.push_back(std::move(computed[w][r]));
        }
      }
    }));
    for (size_t w = 0; w < win_count; ++w) {
      for (size_t worker = 0; worker < append_workers; ++worker) {
        final_types[w].MergeFrom(worker_types[worker][w]);
      }
    }
  }
  bool propagated_uniform = input.uniform_cell_types;
  // Fix hidden column types from the computed values.
  for (size_t w = 0; w < win_count; ++w) {
    final_types[w].ApplyToColumn(&extended_columns[base_width + w],
                                 &propagated_uniform);
  }

  // The window columns were appended to the input rows. Rebuilding the
  // relation one AddRow at a time recharges and recopies every survivor.
  // Keep the existing charge when every row survives and nothing spilled.
  const size_t extra_charge = rows.size() * windows.size() * 48;
  const bool reuse_input =
      !rank_bound && !input.HasSpill() &&
      (extra_charge == 0 ||
       QueryMemoryBudget::Global().CanReserve(extra_charge));
  Relation extended(context.execution_runtime());
  if (reuse_input) {
    if (extra_charge != 0) {
      QueryMemoryBudget::Global().ReserveForced(extra_charge);
      input.charged_bytes_ += extra_charge;
    }
    input.rows = std::move(rows);
    input.schema = Schema("", std::move(extended_columns));
    input.uniform_cell_types = propagated_uniform;
  } else {
    extended.schema = Schema("", std::move(extended_columns));
    CopyExecutionStats(&extended, input);
    input.ResetContents();
    for (size_t r = 0; r < rows.size(); ++r) {
      if (!rank_bound || retained[r] != 0) {
        extended.AddRow(std::move(rows[r]));
      }
    }
  }
  Relation& window_output = reuse_input ? input : extended;
  RETURN_IF_FAIL(window_output.FinishSpill());
  RETURN_IF_FAIL(window_output.GetStatus());

  RETURN_IF_FAIL(FinalizeWindowedStatement(context, statement, outer, ctes,
                                           replacements, windows.size(),
                                           std::move(window_output), &result));
  return result;
}

}  // namespace tinylamb::relational_detail
