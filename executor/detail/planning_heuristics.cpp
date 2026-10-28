/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/planning_heuristics.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/hash_join_mode.hpp"
#include "executor/query_memory.hpp"
#include "executor/spill_file.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "query/statement.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

namespace {

bool IsVirtualValueTableField(const Schema& schema, const ColumnName& name) {
  if (schema.ColumnCount() != 1) {
    return false;
  }
  const ColumnName& only = schema.GetColumn(0).Name();
  if (only.name.empty()) {
    return false;
  }
  // A value table's implementation column is commonly named after the
  // table (for CREATE TABLE AS SELECT AS PROTO), not necessarily `$expr0`.
  // A dotted field reference against a one-column relation therefore
  // needs the sole payload column retained for dotted-field evaluation.
  if (name.name == only.name &&
      (name.schema.empty() || name.schema == only.schema)) {
    return false;
  }
  // A bare simple name never addresses a virtual payload field: dotted
  // access always spells a qualifier (alias or dotted path). Claiming bare
  // names here makes every bare predicate column "ambiguous" against any
  // single-column relation (single-column derived tables, single-column
  // base tables), which silently degrades their joins to cross products
  // (TPC-H Q20 at SF1: 800K x derived rows instead of a hash join).
  if (name.schema.empty()) {
    return false;
  }
  return only.schema.size() == name.schema.size() &&
         std::equal(only.schema.begin(), only.schema.end(), name.schema.begin(),
                    [](char left, char right) {
                      return std::tolower(static_cast<unsigned char>(left)) ==
                             std::tolower(static_cast<unsigned char>(right));
                    });
}

bool IdentifierEqualsLocal(const std::string& lhs, const std::string& rhs) {
  return lhs.size() == rhs.size() &&
         std::equal(lhs.begin(), lhs.end(), rhs.begin(),
                    [](char left, char right) {
                      return std::tolower(static_cast<unsigned char>(left)) ==
                             std::tolower(static_cast<unsigned char>(right));
                    });
}

std::string LowerIdentifierLocal(std::string value) {
  for (char& c : value) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  return value;
}

bool ContainsIdentifierLocal(const std::vector<std::string>& names,
                             const std::string& name) {
  return std::ranges::any_of(names, [&](const std::string& candidate) {
    return IdentifierEqualsLocal(candidate, name);
  });
}

std::vector<size_t> MatchingColumnOffsets(const Schema& schema,
                                          const std::string& name) {
  std::vector<size_t> offsets;
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    if (IdentifierEqualsLocal(schema.GetColumn(i).Name().name, name)) {
      offsets.push_back(i);
    }
  }
  return offsets;
}

bool IsDeclaredUsingEquality(const Expression& expression,
                             const std::vector<std::string>& using_columns) {
  if (!expression || expression->Type() != TypeTag::kBinaryExp ||
      using_columns.empty()) {
    return false;
  }
  const auto& binary = expression->AsBinaryExpression();
  if (binary.Op() != BinaryOperation::kEquals ||
      binary.Left()->Type() != TypeTag::kColumnValue ||
      binary.Right()->Type() != TypeTag::kColumnValue) {
    return false;
  }
  const ColumnName& lhs = binary.Left()->AsColumnValue().GetColumnName();
  const ColumnName& rhs = binary.Right()->AsColumnValue().GetColumnName();
  return lhs.schema.empty() && rhs.schema.empty() &&
         IdentifierEqualsLocal(lhs.name, rhs.name) &&
         ContainsIdentifierLocal(using_columns, lhs.name);
}

}  // namespace

namespace {

bool IsTargetColumn(const ColumnName& column,
                    const std::unordered_set<std::string>& relations) {
  return column.schema.empty() ? relations.contains(std::string())
                               : relations.contains(column.schema);
}

bool IsStrictOnRelations(const Expression& expression,
                         const std::unordered_set<std::string>& relations) {
  if (!expression) {
    return false;
  }
  switch (expression->Type()) {
    case TypeTag::kColumnValue:
      return IsTargetColumn(expression->AsColumnValue().GetColumnName(),
                            relations);
    case TypeTag::kBinaryExp: {
      const auto& binary = expression->AsBinaryExpression();
      switch (binary.Op()) {
        case BinaryOperation::kAdd:
        case BinaryOperation::kSubtract:
        case BinaryOperation::kMultiply:
        case BinaryOperation::kDivide:
        case BinaryOperation::kModulo:
        case BinaryOperation::kShiftLeft:
        case BinaryOperation::kShiftRight:
        case BinaryOperation::kXor:
          return IsStrictOnRelations(binary.Left(), relations) ||
                 IsStrictOnRelations(binary.Right(), relations);
        default:
          return false;
      }
    }
    case TypeTag::kCastExp:
      return IsStrictOnRelations(expression->AsCastExpression().Child(),
                                 relations);
    case TypeTag::kUnaryExp: {
      const auto& unary = expression->AsUnaryExpression();
      if (unary.Op() == UnaryOperation::kNot ||
          unary.Op() == UnaryOperation::kMinus ||
          unary.Op() == UnaryOperation::kBitwiseNot) {
        return IsStrictOnRelations(unary.Child(), relations);
      }
      return false;
    }
    case TypeTag::kInExp:
      return IsStrictOnRelations(expression->AsInExpression().child_,
                                 relations);
    case TypeTag::kFunctionCallExp: {
      const auto& function = expression->AsFunctionCallExpression();
      const std::string& name = function.FuncName();
      if (name != "abs" && name != "lower" && name != "upper" &&
          name != "length" && name != "trim") {
        return false;
      }
      return std::ranges::any_of(function.Args(), [&](const Expression& arg) {
        return IsStrictOnRelations(arg, relations);
      });
    }
    default:
      return false;
  }
}

bool ExpressionRejectsNullsOnRelations(
    const Expression& expression,
    const std::unordered_set<std::string>& relations) {
  if (!expression) {
    return false;
  }
  switch (expression->Type()) {
    case TypeTag::kColumnValue:
      return IsTargetColumn(expression->AsColumnValue().GetColumnName(),
                            relations);
    case TypeTag::kBinaryExp: {
      const auto& binary = expression->AsBinaryExpression();
      switch (binary.Op()) {
        case BinaryOperation::kAnd:
          return ExpressionRejectsNullsOnRelations(binary.Left(), relations) ||
                 ExpressionRejectsNullsOnRelations(binary.Right(), relations);
        case BinaryOperation::kOr:
        case BinaryOperation::kXor:
          return ExpressionRejectsNullsOnRelations(binary.Left(), relations) &&
                 ExpressionRejectsNullsOnRelations(binary.Right(), relations);
        case BinaryOperation::kEquals:
        case BinaryOperation::kNotEquals:
        case BinaryOperation::kLessThan:
        case BinaryOperation::kLessThanEquals:
        case BinaryOperation::kGreaterThan:
        case BinaryOperation::kGreaterThanEquals:
        case BinaryOperation::kLike:
        case BinaryOperation::kNotLike:
        case BinaryOperation::kAdd:
        case BinaryOperation::kSubtract:
        case BinaryOperation::kMultiply:
        case BinaryOperation::kDivide:
        case BinaryOperation::kModulo:
        case BinaryOperation::kShiftLeft:
        case BinaryOperation::kShiftRight:
          return IsStrictOnRelations(binary.Left(), relations) ||
                 IsStrictOnRelations(binary.Right(), relations);
        case BinaryOperation::kIsDistinctFrom:
        case BinaryOperation::kIsNotDistinctFrom:
          return false;
      }
      return false;
    }
    case TypeTag::kUnaryExp: {
      const auto& unary = expression->AsUnaryExpression();
      switch (unary.Op()) {
        case UnaryOperation::kIsNotNull:
        case UnaryOperation::kIsTrue:
        case UnaryOperation::kIsFalse:
        case UnaryOperation::kMinus:
        case UnaryOperation::kBitwiseNot:
          return IsStrictOnRelations(unary.Child(), relations);
        case UnaryOperation::kNot:
        case UnaryOperation::kIsNull:
        case UnaryOperation::kIsNotTrue:
        case UnaryOperation::kIsNotFalse:
          return false;
      }
      return false;
    }
    case TypeTag::kInExp:
      return IsStrictOnRelations(expression->AsInExpression().child_,
                                 relations);
    default:
      return false;
  }
}

}  // namespace

bool IsNullRejectingPredicate(const Expression& expression) {
  if (!expression) {
    return false;
  }
  std::unordered_set<std::string> relations;
  bool has_unqualified = false;
  for (const ColumnName& column : expression->TouchedColumns()) {
    if (column.schema.empty()) {
      has_unqualified = true;
    } else {
      relations.insert(column.schema);
    }
  }
  if (has_unqualified) {
    relations.insert(std::string());
  }
  if (relations.empty()) {
    return false;
  }
  return ExpressionRejectsNullsOnRelations(expression, relations);
}

std::vector<PredicateInfo> AnalyzePredicates(
    const Expression& where, const std::vector<Relation>& relations) {
  std::vector<PredicateInfo> result;
  for (const Expression& expression : SplitConjuncts(where)) {
    PredicateInfo predicate{.expression = expression,
                            .sources = {},
                            .resolved = true,
                            .contains_query = false};
    predicate.contains_query = ContainsQuery(expression);
    for (const ColumnName& column : expression->TouchedColumns()) {
      std::optional<size_t> owner;
      for (size_t i = 0; i < relations.size(); ++i) {
        if (!LocalColumnOffset(relations[i].schema, column) &&
            !IsVirtualValueTableField(relations[i].schema, column)) {
          continue;
        }
        if (owner) {
          predicate.resolved = false;
          break;
        }
        owner = i;
      }
      if (!owner) {
        predicate.resolved = false;
      }
      if (owner) {
        predicate.sources.insert(*owner);
      }
    }
    result.push_back(std::move(predicate));
  }
  return result;
}
Expression NecessaryLocalDisjunction(const Expression& expression,
                                     size_t source,
                                     const std::vector<Relation>& relations) {
  const std::vector<Expression> branches = SplitDisjuncts(expression);
  if (branches.size() < 2) {
    return nullptr;
  }
  std::vector<Expression> local_branches;
  local_branches.reserve(branches.size());
  for (const Expression& branch : branches) {
    std::vector<Expression> local;
    for (const Expression& conjunct : SplitConjuncts(branch)) {
      const std::vector<PredicateInfo> analyzed =
          AnalyzePredicates(conjunct, relations);
      if (analyzed.size() == 1 && analyzed[0].resolved &&
          !analyzed[0].contains_query && analyzed[0].sources.size() == 1 &&
          analyzed[0].sources.contains(source)) {
        local.push_back(conjunct);
      }
    }
    // A branch without a condition on this source can be true for every row;
    // in that case no source-local condition is implied by the disjunction.
    if (local.empty()) {
      return nullptr;
    }
    local_branches.push_back(CombineConjuncts(local));
  }
  return CombineDisjuncts(local_branches);
}

bool IsColumnEqualityPredicate(const Expression& predicate) {
  if (!predicate || predicate->Type() != TypeTag::kBinaryExp) {
    return false;
  }
  const BinaryExpression& binary = predicate->AsBinaryExpression();
  return (binary.Op() == BinaryOperation::kEquals ||
          binary.Op() == BinaryOperation::kIsNotDistinctFrom) &&
         binary.Left()->Type() == TypeTag::kColumnValue &&
         binary.Right()->Type() == TypeTag::kColumnValue;
}

namespace {

// Matches a bare column reference ("bias 0") or `column +/- integer literal`
// ("bias +/- literal") for biased join-key extraction.  `literal - column`
// and every other shape return false.
bool ColumnWithConstantBias(const Expression& expression, ColumnName* column,
                            int64_t* bias) {
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kColumnValue) {
    *column = expression->AsColumnValue().GetColumnName();
    *bias = 0;
    return true;
  }
  if (expression->Type() != TypeTag::kBinaryExp) {
    return false;
  }
  const BinaryExpression& binary = expression->AsBinaryExpression();
  if (binary.Op() != BinaryOperation::kAdd &&
      binary.Op() != BinaryOperation::kSubtract) {
    return false;
  }
  const Expression& left = binary.Left();
  const Expression& right = binary.Right();
  const Expression* column_side = nullptr;
  const Expression* constant_side = nullptr;
  if (left->Type() == TypeTag::kColumnValue &&
      right->Type() == TypeTag::kConstantValue) {
    column_side = &left;
    constant_side = &right;
  } else if (binary.Op() == BinaryOperation::kAdd &&
             left->Type() == TypeTag::kConstantValue &&
             right->Type() == TypeTag::kColumnValue) {
    column_side = &right;
    constant_side = &left;
  } else {
    return false;
  }
  const Value& constant = (*constant_side)->AsConstantValue().GetValue();
  if (constant.IsNull() || (constant.type != ValueType::kInt64 &&
                            constant.type != ValueType::kDate)) {
    return false;
  }
  const int64_t magnitude = constant.value.int_value;
  // Bounds keep the bias arithmetic far from int64 wrap-around; residual
  // verification stays the correctness backstop regardless.
  if (magnitude > (int64_t{1} << 40) || magnitude < -(int64_t{1} << 40)) {
    return false;
  }
  *column = (*column_side)->AsColumnValue().GetColumnName();
  *bias = binary.Op() == BinaryOperation::kSubtract ? -magnitude : magnitude;
  return true;
}

// Offset keys require integer-domain columns: the biased fast paths key on
// raw int64 storage, and the memcomparable encoder biases INT64/DATE only.
bool BiasedKeyTypesSupported(const Schema& schema, size_t column) {
  const ValueType type = schema.GetColumn(column).Type();
  return type == ValueType::kInt64 || type == ValueType::kDate;
}

// Canonical text for a column pair so `a.x = b.y` and `b.y = a.x` share one
// identity across branches and dedup sets.
std::string EqualityPairKey(const ColumnName& lhs, const ColumnName& rhs) {
  const std::string left = lhs.ToString();
  const std::string right = rhs.ToString();
  return left < right ? left + '\x1f' + right : right + '\x1f' + left;
}

// Column pairs joined by a strict `=` that EVERY branch of a disjunction
// implies: a row surviving `(b0 OR b1 OR ...)` satisfies some bi, and each
// bi contains the pair as a conjunct, so the equality is TRUE (never
// UNKNOWN) for every survivor.  The pairs may therefore key a hash join or
// a runtime filter while the original disjunction stays in the residual
// list re-verifying every matched pair -- the keys only ever prune
// candidates, they cannot add a row.
std::vector<std::pair<ColumnName, ColumnName>> DisjunctCommonEqualities(
    const Expression& predicate) {
  std::vector<std::pair<ColumnName, ColumnName>> common;
  if (!predicate || predicate->Type() != TypeTag::kBinaryExp ||
      predicate->AsBinaryExpression().Op() != BinaryOperation::kOr) {
    return common;
  }
  const std::vector<Expression> branches = SplitDisjuncts(predicate);
  if (branches.size() < 2 || branches.size() > 8) {
    return common;
  }
  std::vector<
      std::unordered_map<std::string, std::pair<ColumnName, ColumnName>>>
      per_branch;
  per_branch.reserve(branches.size());
  for (const Expression& branch : branches) {
    std::unordered_map<std::string, std::pair<ColumnName, ColumnName>> pairs;
    for (const Expression& conjunct : SplitConjuncts(branch)) {
      if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
        continue;
      }
      const BinaryExpression& binary = conjunct->AsBinaryExpression();
      if (binary.Op() != BinaryOperation::kEquals ||
          binary.Left()->Type() != TypeTag::kColumnValue ||
          binary.Right()->Type() != TypeTag::kColumnValue) {
        continue;
      }
      const ColumnName& lhs = binary.Left()->AsColumnValue().GetColumnName();
      const ColumnName& rhs = binary.Right()->AsColumnValue().GetColumnName();
      if (lhs.ToString() == rhs.ToString()) {
        continue;
      }
      pairs.insert_or_assign(EqualityPairKey(lhs, rhs),
                             std::make_pair(lhs, rhs));
    }
    if (pairs.empty()) {
      return common;
    }
    per_branch.push_back(std::move(pairs));
  }
  for (const auto& [key, pair] : per_branch.front()) {
    const bool shared = std::ranges::all_of(
        per_branch, [&](const auto& branch) { return branch.contains(key); });
    if (shared) {
      common.push_back(pair);
    }
  }
  return common;
}

// Builds synthetic two-source equality predicates for every disjunctive
// conjunct in `analyzed`: the pairs `DisjunctCommonEqualities` mines become
// plain `col = col` PredicateInfos that the join planner treats exactly like
// user-written equi-join edges (connectivity, hash keys, runtime filters).
// Only deterministic disjuncts participate -- `SafeToReduceEvaluationCount`
// rejects volatile/subquery-bearing shapes whose implication is not stable
// across evaluations.  Pairs that already exist as plain top-level
// equalities are skipped so composite-key estimation never double-counts.
std::vector<PredicateInfo> SynthesizeDisjunctEqualityPredicates(
    const std::vector<PredicateInfo>& analyzed,
    const std::vector<Relation>& relations) {
  std::vector<PredicateInfo> synthesized;
  std::unordered_set<std::string> known_pairs;
  for (const PredicateInfo& predicate : analyzed) {
    if (!predicate.resolved || predicate.contains_query ||
        predicate.sources.size() != 2 ||
        !IsColumnEqualityPredicate(predicate.expression) ||
        predicate.expression->AsBinaryExpression().Op() !=
            BinaryOperation::kEquals) {
      continue;
    }
    const BinaryExpression& binary = predicate.expression->AsBinaryExpression();
    known_pairs.insert(
        EqualityPairKey(binary.Left()->AsColumnValue().GetColumnName(),
                        binary.Right()->AsColumnValue().GetColumnName()));
  }
  for (const PredicateInfo& predicate : analyzed) {
    if (!predicate.resolved || predicate.contains_query ||
        predicate.sources.size() < 2 ||
        !SafeToReduceEvaluationCount(predicate.expression)) {
      continue;
    }
    for (const auto& [lhs, rhs] :
         DisjunctCommonEqualities(predicate.expression)) {
      if (!known_pairs.insert(EqualityPairKey(lhs, rhs)).second) {
        continue;
      }
      Expression equality = BinaryExpressionExp(
          ColumnValueExp(lhs), BinaryOperation::kEquals, ColumnValueExp(rhs));
      const std::vector<PredicateInfo> analyzed_equality =
          AnalyzePredicates(equality, relations);
      if (analyzed_equality.size() != 1 || !analyzed_equality[0].resolved ||
          analyzed_equality[0].sources.size() != 2) {
        continue;
      }
      synthesized.push_back(analyzed_equality[0]);
    }
  }
  return synthesized;
}

}  // namespace

std::vector<Expression> ResidualJoinPredicates(
    const Schema& /*left*/, const Schema& /*right*/,
    const std::vector<Expression>& predicates) {
  std::vector<Expression> residual;
  residual.reserve(predicates.size());
  for (const Expression& predicate : predicates) {
    residual.push_back(predicate);
  }
  return residual;
}

std::vector<EqualityKey> EqualityKeys(
    const Schema& left, const Schema& right,
    const std::vector<Expression>& predicates) {
  std::vector<EqualityKey> keys;
  for (const Expression& predicate : predicates) {
    if (!predicate || predicate->Type() != TypeTag::kBinaryExp) {
      continue;
    }
    const BinaryExpression& binary = predicate->AsBinaryExpression();
    const bool null_safe = binary.Op() == BinaryOperation::kIsNotDistinctFrom;
    if (IsColumnEqualityPredicate(predicate)) {
      const ColumnName& lhs = binary.Left()->AsColumnValue().GetColumnName();
      const ColumnName& rhs = binary.Right()->AsColumnValue().GetColumnName();
      const auto lhs_left = LocalColumnOffset(left, lhs);
      const auto lhs_right = LocalColumnOffset(right, lhs);
      const auto rhs_left = LocalColumnOffset(left, rhs);
      const auto rhs_right = LocalColumnOffset(right, rhs);
      if (lhs_left && rhs_right) {
        keys.push_back({*lhs_left, *rhs_right, null_safe});
      } else if (rhs_left && lhs_right) {
        keys.push_back({*rhs_left, *lhs_right, null_safe});
      }
      continue;
    }
    // `col = col +/- constant` becomes a biased join key (the offsets make
    // both sides hash equal when the predicate holds); the predicate stays
    // in the residual list, which re-verifies every matched pair, so the
    // bias only prunes hash buckets and can never add a row.
    if (null_safe || binary.Op() != BinaryOperation::kEquals) {
      continue;
    }
    int64_t bias = 0;
    const auto try_extract = [&](const Expression& biased_expr,
                                 const Expression& bare_expr) -> bool {
      ColumnName bare_column;
      if (!ColumnWithConstantBias(biased_expr, &bare_column, &bias) ||
          bias == 0) {
        return false;
      }
      if (bare_expr == nullptr || bare_expr->Type() != TypeTag::kColumnValue) {
        return false;
      }
      const ColumnName& other_column =
          bare_expr->AsColumnValue().GetColumnName();
      // The biased column may sit on either schema; the bare side must land
      // on the other schema with a key type the bias can ride on.
      const auto biased_left = LocalColumnOffset(left, bare_column);
      const auto biased_right = LocalColumnOffset(right, bare_column);
      const auto bare_left = LocalColumnOffset(left, other_column);
      const auto bare_right = LocalColumnOffset(right, other_column);
      if (biased_left && bare_right &&
          BiasedKeyTypesSupported(right, *bare_right)) {
        keys.push_back({*biased_left, *bare_right, false, bias, 0});
        return true;
      }
      if (biased_right && bare_left &&
          BiasedKeyTypesSupported(left, *bare_left)) {
        keys.push_back({*bare_left, *biased_right, false, 0, bias});
        return true;
      }
      return false;
    };
    try_extract(binary.Left(), binary.Right()) ||
        try_extract(binary.Right(), binary.Left());
  }
  return keys;
}

bool SingleIntegerJoinKey(const Schema& schema,
                          const std::vector<slot_t>& columns) {
  if (columns.size() != 1) {
    return false;
  }
  const ValueType type = schema.GetColumn(columns[0]).Type();
  return type == ValueType::kInt64 || type == ValueType::kDate;
}

bool HasNullKey(const Row& row, const std::vector<slot_t>& columns) {
  // NULL never equals anything, and SQL equality also excludes NaN: rows
  // with such join keys take no part in hash matching.
  return std::ranges::any_of(columns, [&](slot_t column) {
    const Value& value = row[column];
    return value.IsNull() || (value.type == ValueType::kDouble &&
                              std::isnan(value.value.double_value));
  });
}

int64_t IntegerJoinKey(const Row& row, slot_t column) {
  return row[column].value.int_value;
}

namespace {

// Bias folding must stay defined for extreme key values: a column holding
// INT64_MAX next to a `col = col + 1` predicate is valid data, and a signed
// add would be UB there. The residual predicate re-verifies every matched
// pair, so wrap-around only moves a row between hash buckets; it can never
// change the join's result.
int64_t AddJoinKeyBias(int64_t key, int64_t bias) {
  return static_cast<int64_t>(static_cast<uint64_t>(key) +
                              static_cast<uint64_t>(bias));
}

}  // namespace

// Fixed-width composite key over INT64 join columns.  Membership is raw
// int64 bit equality (mirrors IntegerJoinKey), so no memcomparable string
// is materialized per build/probe row -- a 6M-row composite-key build used
// to spend ~3 heap allocations per row inside EncodeJoinKey.  Key tuples
// wider than kMaxParts keep the string path.
struct CompositeIntKey {
  static constexpr size_t kMaxParts = 4;
  std::array<int64_t, kMaxParts> parts{};
  uint8_t count{0};
  bool operator==(const CompositeIntKey& rhs) const = default;
};

struct CompositeIntKeyHash {
  size_t operator()(const CompositeIntKey& key) const {
    size_t hash = key.count;
    for (uint8_t i = 0; i < key.count; ++i) {
      hash ^= std::hash<int64_t>{}(key.parts[i]) + 0x9e3779b97f4a7c15ULL +
              (hash << 6) + (hash >> 2);
    }
    return hash;
  }
};

// The composite fast path applies when both sides carry the same number of
// join columns (at least two: the single-column case is IntegerJoinKey's),
// all typed INT64 on both sides.  Pairwise type equality keeps the decision
// identical to the memcomparable encoding it replaces.
// NOLINTNEXTLINE(misc-use-anonymous-namespace)
static bool CompositeIntJoinKey(const Schema& left_schema,
                                const std::vector<slot_t>& left_columns,
                                const Schema& right_schema,
                                const std::vector<slot_t>& right_columns) {
  if (left_columns.size() != right_columns.size() || left_columns.size() < 2 ||
      left_columns.size() > CompositeIntKey::kMaxParts) {
    return false;
  }
  for (size_t i = 0; i < left_columns.size(); ++i) {
    if (left_schema.GetColumn(left_columns[i]).Type() != ValueType::kInt64 ||
        right_schema.GetColumn(right_columns[i]).Type() != ValueType::kInt64) {
      return false;
    }
  }
  return true;
}

// NOLINTNEXTLINE(misc-use-anonymous-namespace)
static CompositeIntKey EncodeCompositeIntKey(
    const Row& row, const std::vector<slot_t>& columns,
    const std::vector<int64_t>* offsets = nullptr) {
  CompositeIntKey key;
  for (size_t index = 0; index < columns.size(); ++index) {
    const slot_t column = columns[index];
    key.parts[key.count++] =
        AddJoinKeyBias(row[column].value.int_value,
                       offsets != nullptr ? (*offsets)[index] : 0);
  }
  return key;
}

namespace {

void AppendBigEndian(std::string* output, uint64_t value, size_t width) {
  const size_t offset = output->size();
  output->resize(offset + width);
  for (size_t i = 0; i < width; ++i) {
    (*output)[offset + i] = static_cast<char>(value >> ((width - i - 1) * 8));
  }
}

static void AppendMemcomparableValue(const Value& value, std::string* output);

void AppendMemcomparableInteger(int64_t input, ValueType prefix, bool flip_sign,
                                std::string* output) {
  output->push_back(static_cast<char>(prefix));
  const size_t offset = output->size();
  AppendBigEndian(output, static_cast<uint64_t>(input), sizeof(uint64_t));
  if (flip_sign) {
    (*output)[offset] ^= static_cast<char>(0x80);
  }
}

void AppendMemcomparableValue(const Value& value, std::string* output) {
  switch (value.type) {
    case ValueType::kNull:
      output->push_back(static_cast<char>(ValueType::kNull));
      return;
    case ValueType::kInt64:
      if (value.IsUnsigned()) {
        AppendMemcomparableInteger(value.value.int_value,
                                   static_cast<ValueType>(6), false, output);
      } else {
        AppendMemcomparableInteger(value.value.int_value, ValueType::kInt64,
                                   true, output);
      }
      return;
    case ValueType::kDate:
      AppendMemcomparableInteger(value.value.int_value, ValueType::kDate, true,
                                 output);
      return;
    case ValueType::kVarChar: {
      const std::string_view input = value.value.varchar_value;
      output->push_back(static_cast<char>(ValueType::kVarChar));
      if (input.empty()) {
        output->append(9, '\0');
        return;
      }
      size_t consumed = 0;
      while (consumed < input.size()) {
        const size_t remaining = input.size() - consumed;
        const size_t chunk = std::min<size_t>(8, remaining);
        const size_t offset = output->size();
        output->resize(offset + 9, '\0');
        std::memcpy(output->data() + offset, input.data() + consumed, chunk);
        consumed += chunk;
        (*output)[offset + 8] =
            static_cast<char>(consumed == input.size() ? chunk : 9);
      }
      return;
    }
    case ValueType::kDouble: {
      output->push_back(static_cast<char>(ValueType::kDouble));
      uint64_t bits = 0;
      std::memcpy(&bits, &value.value.double_value, sizeof(bits));
      if (value.value.double_value < 0.0) {
        bits = ~bits;
      }
      const size_t offset = output->size();
      AppendBigEndian(output, bits, sizeof(bits));
      if (!(value.value.double_value < 0.0)) {
        (*output)[offset] ^= static_cast<char>(0x80);
      }
      return;
    }
    case ValueType::kArray: {
      output->push_back(static_cast<char>(ValueType::kArray));
      const auto& elements = value.ArrayElements();
      const auto count = static_cast<uint32_t>(elements.size());
      AppendBigEndian(output, count, sizeof(count));
      output->append(value.ArrayElementSqlType());
      output->push_back('\0');
      for (const Value& element : elements) {
        if (element.IsNull()) {
          output->push_back('\0');
          continue;
        }
        output->push_back('\1');
        AppendMemcomparableValue(element, output);
      }
      return;
    }
  }
}

std::string EncodeJoinKeyInternal(const Row& row,
                                  const std::vector<slot_t>& columns,
                                  bool null_safe, bool fold_zero,
                                  const std::vector<int64_t>* offsets) {
  std::string key;
  key.reserve(columns.size() * (null_safe ? 10 : 9));
  for (size_t index = 0; index < columns.size(); ++index) {
    const slot_t column = columns[index];
    const Value& value = row[column];
    if (value.IsNull()) {
      key.push_back(null_safe ? '\0' : static_cast<char>(ValueType::kNull));
      continue;
    }
    if (null_safe) {
      key.push_back('\1');
    }
    const int64_t bias = offsets != nullptr ? (*offsets)[index] : 0;
    if (bias != 0 &&
        (value.type == ValueType::kInt64 || value.type == ValueType::kDate)) {
      Value biased = value;
      biased.value.int_value = AddJoinKeyBias(biased.value.int_value, bias);
      AppendMemcomparableValue(biased, &key);
    } else if (fold_zero && value.type == ValueType::kDouble &&
               value.value.double_value == 0.0) {
      Value positive_zero(0.0);
      AppendMemcomparableValue(positive_zero, &key);
    } else {
      AppendMemcomparableValue(value, &key);
    }
  }
  return key;
}

}  // namespace

std::string EncodeJoinKey(const Row& row, const std::vector<slot_t>& columns,
                          const std::vector<int64_t>* offsets) {
  // Join keys canonicalize doubles the way SQL equality demands: -0 folds to
  // +0 so both zeros share a bucket, while NaN bit patterns stay distinct
  // (NaN never joins with anything).
  return EncodeJoinKeyInternal(row, columns, false, true, offsets);
}

namespace {

Row ConcatRows(const Row& left, const Row& right) {
  std::vector<Value> values;
  values.reserve(left.values_.size() + right.values_.size());
  values.insert(values.end(), left.values_.begin(), left.values_.end());
  values.insert(values.end(), right.values_.begin(), right.values_.end());
  return Row(std::move(values));
}

}  // namespace

namespace {

template <typename Key, typename Hash = std::hash<Key>,
          typename Equal = std::equal_to<Key>>
class FlatJoinBuckets {
  struct Entry;
  static constexpr size_t kNoEntry = std::numeric_limits<size_t>::max();

 public:
  class const_iterator {
   public:
    const_iterator() = default;
    const_iterator(const FlatJoinBuckets* buckets, size_t index)
        : buckets_(buckets), index_(index) {}

    const Entry& operator*() const { return buckets_->entries_[index_]; }
    const Entry* operator->() const { return &buckets_->entries_[index_]; }
    const_iterator& operator++() {
      index_ = buckets_->entries_[index_].duplicate_next;
      return *this;
    }
    bool operator==(const const_iterator& other) const = default;

   private:
    const FlatJoinBuckets* buckets_{nullptr};
    size_t index_{kNoEntry};
  };

  void reserve(size_t count) {
    if (count > (std::numeric_limits<size_t>::max() - 1) / 2) {
      throw std::length_error("join bucket capacity overflow");
    }
    const size_t target = count + ((count / 2) + 1);
    size_t bucket_count = 8;
    while (bucket_count < target) {
      if (bucket_count > std::numeric_limits<size_t>::max() / 2) {
        throw std::length_error("join bucket capacity overflow");
      }
      bucket_count *= 2;
    }
    entries_.reserve(count);
    buckets_.assign(bucket_count, kNoEntry);
  }

  template <typename KeyLike>
  void emplace(KeyLike&& key, const Row* row) {
    const size_t bucket = BucketIndex(key);
    const size_t head = FindHead(key, bucket);
    const size_t index = entries_.size();
    entries_.push_back(
        Entry{std::forward<KeyLike>(key), row, head, buckets_[bucket]});
    buckets_[bucket] = index;
  }

  [[nodiscard]] std::pair<const_iterator, const_iterator> equal_range(
      const Key& key) const {
    const size_t bucket = BucketIndex(key);
    return {const_iterator(this, FindHead(key, bucket)),
            const_iterator(this, kNoEntry)};
  }

  // Warms the bucket-head slot for a key the caller will probe next.  The
  // dependent entries_ chain load cannot be prefetched before the bucket
  // word is resident, but starting that load one row earlier hides most of
  // the hash-table walk latency on probe-heavy joins.
  void PrefetchBucket(const Key& key) const {
    if (buckets_.empty()) {
      return;
    }
    __builtin_prefetch(static_cast<const size_t*>(&buckets_[BucketIndex(key)]));
  }

 private:
  struct Entry {
    Key first;
    const Row* second;
    size_t duplicate_next;
    size_t collision_next;
  };

  [[nodiscard]] size_t BucketIndex(const Key& key) const {
    return hash_(key) & (buckets_.size() - 1);
  }

  [[nodiscard]] size_t FindHead(const Key& key, size_t bucket) const {
    for (size_t index = buckets_[bucket]; index != kNoEntry;
         index = entries_[index].collision_next) {
      if (equal_(entries_[index].first, key)) {
        return index;
      }
    }
    return kNoEntry;
  }

  Hash hash_{};
  Equal equal_{};
  std::vector<size_t> buckets_;
  std::vector<Entry> entries_;
};

// Unlike ordinary equality joins, NULL is a legitimate key for
// IS NOT DISTINCT FROM. Encode the NULL/non-NULL state explicitly instead of
// passing Value::kNull to the regular mem-comparable encoder, which correctly
// rejects untyped NULL for ordinary hash joins.

std::string EncodeNullSafeJoinKey(const Row& row,
                                  const std::vector<slot_t>& columns) {
  return EncodeJoinKeyInternal(row, columns, true, false, nullptr);
}

bool SameSingleIntegerJoinKey(const Schema& left_schema,
                              const std::vector<slot_t>& left_columns,
                              const Schema& right_schema,
                              const std::vector<slot_t>& right_columns) {
  if (left_columns.size() != 1 || right_columns.size() != 1) {
    return false;
  }
  const ValueType left_type = left_schema.GetColumn(left_columns[0]).Type();
  return left_type == right_schema.GetColumn(right_columns[0]).Type() &&
         (left_type == ValueType::kInt64 || left_type == ValueType::kDate);
}

// Build-side frequency tables for join-cardinality estimation.  Split out of
// EstimateJoinRows so the greedy join-order sweep can reuse a candidate's map
// across levels: the candidate relation is fixed while only the accumulated
// probe side grows, so rebuilding the map per candidate per level repeats
// identical full-relation sweeps (seconds at TPC-H SF=1).
struct JoinKeyFrequencyMaps {
  bool single_int{false};
  bool composite_int{false};
  std::vector<slot_t> right_columns;
  // Stride used when the build side itself was sampled (1 = exact map).
  size_t build_stride{1};
  std::unordered_map<int64_t, size_t> single_int_frequencies;
  std::unordered_map<std::string, size_t> frequencies;
  std::unordered_map<CompositeIntKey, size_t, CompositeIntKeyHash>
      int_frequencies;
};

void BuildJoinKeyFrequencyMaps(const Relation& right,
                               const std::vector<slot_t>& right_columns,
                               bool single_int, bool composite_int,
                               JoinKeyFrequencyMaps* maps) {
  maps->single_int = single_int;
  maps->composite_int = composite_int;
  maps->right_columns = right_columns;
  static const size_t kBuildSampleTarget = [] {
    // A/B hatch: TINYLAMB_LEGACY_ESTIMATE=1 restores the 2M-row sampling
    // targets (finer estimates, slower planning sweeps).
    const char* legacy = std::getenv("TINYLAMB_LEGACY_ESTIMATE");
    return legacy != nullptr ? size_t{2'000'000} : size_t{512'000};
  }();
  static const bool build_sampling_disabled =
      std::getenv("TINYLAMB_LEGACY_PLANNER") != nullptr;
  const size_t total = right.TotalRows();
  maps->build_stride =
      build_sampling_disabled || total <= kBuildSampleTarget
          ? 1
          : (total + kBuildSampleTarget - 1) / kBuildSampleTarget;
  const size_t sample_rows =
      maps->build_stride == 1
          ? total
          : (total + maps->build_stride - 1) / maps->build_stride;
  if (single_int) {
    maps->single_int_frequencies.reserve(sample_rows);
  } else if (composite_int) {
    maps->int_frequencies.reserve(sample_rows);
  } else {
    maps->frequencies.reserve(sample_rows);
  }
  const auto add_row = [&](const Row& row) {
    if (HasNullKey(row, right_columns)) {
      return;
    }
    if (single_int) {
      ++maps->single_int_frequencies[IntegerJoinKey(row, right_columns[0])];
    } else if (composite_int) {
      ++maps->int_frequencies[EncodeCompositeIntKey(row, right_columns)];
    } else {
      ++maps->frequencies[EncodeJoinKeyInternal(row, right_columns, false,
                                                false, nullptr)];
    }
  };
  if (right.HasSpill()) {
    size_t index = 0;
    right.ForEachRow([&](const Row& row) {
      const size_t current = index++;
      if (maps->build_stride == 1 || current % maps->build_stride == 0) {
        add_row(row);
      }
    });
  } else if (maps->build_stride == 1) {
    for (const Row& row : right.rows) {
      add_row(row);
    }
  } else {
    for (size_t i = 0; i < right.rows.size(); i += maps->build_stride) {
      add_row(right.rows[i]);
    }
  }
}

size_t ProbeJoinEstimate(const Relation& left,
                         const std::vector<slot_t>& left_columns,
                         const JoinKeyFrequencyMaps& maps) {
  size_t estimate = 0;
  // Planning-only cardinality estimation on multi-million-row probe sides
  // costs seconds per join level (TPC-H Q9 sweeps a 6M-row lineitem once per
  // candidate).  Above a threshold, probe a deterministic stride sample and
  // scale the matched-key sum back up; near-tie estimates may flip ranking,
  // which is acceptable for a heuristic planner.
  static const size_t kEstimateSampleTarget = [] {
    const char* legacy = std::getenv("TINYLAMB_LEGACY_ESTIMATE");
    return legacy != nullptr ? size_t{2'000'000} : size_t{512'000};
  }();
  static const bool probe_sampling_disabled =
      std::getenv("TINYLAMB_LEGACY_PLANNER") != nullptr;
  const size_t left_rows = left.TotalRows();
  const size_t stride =
      probe_sampling_disabled || left_rows <= kEstimateSampleTarget
          ? 1
          : (left_rows + kEstimateSampleTarget - 1) / kEstimateSampleTarget;
  const auto probe_row = [&](const Row& row) {
    if (HasNullKey(row, left_columns)) {
      return;
    }
    if (maps.single_int) {
      const auto found = maps.single_int_frequencies.find(
          IntegerJoinKey(row, left_columns[0]));
      if (found != maps.single_int_frequencies.end()) {
        estimate += found->second * maps.build_stride;
      }
    } else if (maps.composite_int) {
      const auto found =
          maps.int_frequencies.find(EncodeCompositeIntKey(row, left_columns));
      if (found != maps.int_frequencies.end()) {
        estimate += found->second * maps.build_stride;
      }
    } else {
      const auto found = maps.frequencies.find(
          EncodeJoinKeyInternal(row, left_columns, false, false, nullptr));
      if (found != maps.frequencies.end()) {
        estimate += found->second * maps.build_stride;
      }
    }
  };
  if (left.HasSpill()) {
    size_t index = 0;
    left.ForEachRow([&](const Row& row) {
      const size_t current = index++;
      if (stride == 1 || current % stride == 0) {
        probe_row(row);
      }
    });
  } else if (stride == 1) {
    for (const Row& row : left.rows) {
      probe_row(row);
    }
  } else {
    for (size_t i = 0; i < left.rows.size(); i += stride) {
      probe_row(left.rows[i]);
    }
  }
  return stride > 1 ? estimate * stride : estimate;
}

// Deterministic key order for estimation: the estimate is a sum over key
// tuples, so any consistent pairwise reorder yields the same value while
// letting the per-candidate cache match across levels whose predicate order
// differs.  Both column lists come from one sort so the i-th left column
// stays paired with the i-th right column; ordering by the right offset
// (stable across levels, whose left schema keeps growing) maximizes cache
// reuse for the candidate-side frequency maps.
std::pair<std::vector<slot_t>, std::vector<slot_t>> AlignedEstimateKeys(
    const std::vector<EqualityKey>& keys) {
  std::vector<std::pair<slot_t, slot_t>> pairs;
  pairs.reserve(keys.size());
  for (const EqualityKey& key : keys) {
    pairs.emplace_back(key.right, key.left);
  }
  std::sort(pairs.begin(), pairs.end());
  std::vector<slot_t> left_columns;
  std::vector<slot_t> right_columns;
  left_columns.reserve(pairs.size());
  right_columns.reserve(pairs.size());
  for (const auto& pair : pairs) {
    right_columns.push_back(pair.first);
    left_columns.push_back(pair.second);
  }
  return {left_columns, right_columns};
}

}  // namespace

size_t EstimateJoinRows(const Relation& left, const Relation& right,
                        const std::vector<Expression>& predicates) {
  // Biased keys (`col = col +/- constant`) hash with folded constants at
  // execution; the raw-value frequency maps below cannot see that, so they
  // stay out of the estimate (the plain column=column keys keep the same
  // estimate shape as before biased keys existed).
  std::vector<EqualityKey> keys =
      EqualityKeys(left.schema, right.schema, predicates);
  std::erase_if(keys, [](const EqualityKey& key) {
    return key.left_offset != 0 || key.right_offset != 0;
  });
  if (keys.empty()) {
    if (left.TotalRows() == 0 || right.TotalRows() == 0) {
      return 0;
    }
    if (left.TotalRows() >
        std::numeric_limits<size_t>::max() / right.TotalRows()) {
      return std::numeric_limits<size_t>::max();
    }
    return left.TotalRows() * right.TotalRows();
  }
  const auto [left_columns, right_columns] = AlignedEstimateKeys(keys);
  const bool single_int = SameSingleIntegerJoinKey(left.schema, left_columns,
                                                   right.schema, right_columns);
  const bool composite_int = CompositeIntJoinKey(left.schema, left_columns,
                                                 right.schema, right_columns);
  JoinKeyFrequencyMaps maps;
  BuildJoinKeyFrequencyMaps(right, right_columns, single_int, composite_int,
                            &maps);
  return ProbeJoinEstimate(left, left_columns, maps);
}

// Greedy-loop variant that reuses each candidate's frequency map across
// levels: only the probe side (the accumulated join result) changes between
// calls, so a per-candidate cache turns repeated full-relation builds into
// one build per (candidate, join-column) pair.  Estimates stay identical to
// EstimateJoinRows.
// NOLINTNEXTLINE(misc-use-anonymous-namespace)
static size_t EstimateJoinRowsCached(
    const Relation& left, const Relation& candidate,
    const std::vector<Expression>& predicates,
    std::unordered_map<const Relation*, JoinKeyFrequencyMaps>* cache) {
  std::vector<EqualityKey> keys =
      EqualityKeys(left.schema, candidate.schema, predicates);
  std::erase_if(keys, [](const EqualityKey& key) {
    return key.left_offset != 0 || key.right_offset != 0;
  });
  if (keys.empty()) {
    if (left.TotalRows() == 0 || candidate.TotalRows() == 0) {
      return 0;
    }
    if (left.TotalRows() >
        std::numeric_limits<size_t>::max() / candidate.TotalRows()) {
      return std::numeric_limits<size_t>::max();
    }
    return left.TotalRows() * candidate.TotalRows();
  }
  const auto [left_columns, right_columns] = AlignedEstimateKeys(keys);
  const bool single_int = SameSingleIntegerJoinKey(
      left.schema, left_columns, candidate.schema, right_columns);
  const bool composite_int = CompositeIntJoinKey(
      left.schema, left_columns, candidate.schema, right_columns);
  auto [iter, inserted] = cache->try_emplace(&candidate);
  JoinKeyFrequencyMaps& maps = iter->second;
  if (inserted || maps.single_int != single_int ||
      maps.composite_int != composite_int ||
      maps.right_columns != right_columns) {
    BuildJoinKeyFrequencyMaps(candidate, right_columns, single_int,
                              composite_int, &maps);
  }
  return ProbeJoinEstimate(left, left_columns, maps);
}

size_t SpillPartitionOf(const std::string& key, size_t partitions) {
  return std::hash<std::string>{}(key) % partitions;
}

size_t SpillPartitionOf(int64_t key, size_t partitions) {
  return static_cast<size_t>(std::hash<int64_t>{}(key) % partitions);
}

// DeWitt-style Hybrid Hash Join: keep partition 0 resident; spill the rest.
// D8 (docs/design.md): supports null-safe equality keys (NULL matches NULL,
// ordinary NULL keys never match), LEFT/RIGHT/FULL outer, and reprocesses
// EVERY spilled partition -- spilled rows are never dropped or double-counted
// against the resident portion alone.  Over-budget partitions spill further;
// nothing is silently lost.
StatusOr<Relation> HybridHashJoin(
    Relation left, Relation right, const std::vector<slot_t>& left_columns,
    const std::vector<slot_t>& right_columns,
    const std::function<bool(const Row&)>& matches, bool left_join,
    size_t* join_comparisons, bool null_safe, bool right_join) {
  size_t build_estimate = 0;
  if (right.HasSpill()) {
    build_estimate = right.TotalRows() * 128;
  } else {
    for (const Row& row : right.rows) {
      build_estimate += EstimateRowBytes(row) + 64;
    }
  }
  const size_t partitions = HybridPartitionCount(build_estimate);
  QueryMemoryBudget& budget = QueryMemoryBudget::Global();
  // D8: with right-outer emission, NULL-key build rows must survive intake
  // (they never match but must be emitted); the integer fast path has no
  // reserved key for NULL, so byte-encode the whole partition set instead.
  // The null-safe byte encoding is injective for non-NULL rows, and probe
  // sides still exclude NULL keys for ordinary equality, so semantics hold.
  const bool integer_key = !null_safe && !right_join &&
                           SingleIntegerJoinKey(left.schema, left_columns) &&
                           SingleIntegerJoinKey(right.schema, right_columns);
  const slot_t left_key_column =
      integer_key ? left_columns[0] : static_cast<slot_t>(0);
  const slot_t right_key_column =
      integer_key ? right_columns[0] : static_cast<slot_t>(0);
  auto left_key_string = [&](const Row& row) {
    return EncodeNullSafeJoinKey(row, left_columns);
  };
  auto right_key_string = [&](const Row& row) {
    return EncodeNullSafeJoinKey(row, right_columns);
  };
  // Ordinary equality excludes NULL keys from matching; null-safe equality
  // treats NULL as a value.  A right-outer side must KEEP null-key build
  // rows so they can be emitted as unmatched (they never match a probe).
  auto build_key_excluded = [&](const Row& row) {
    return !null_safe && HasNullKey(row, right_columns) && !right_join;
  };
  auto probe_key_excluded = [&](const Row& row) {
    return !null_safe && HasNullKey(row, left_columns);
  };

  std::vector<Row> resident_right;
  QueryMemoryCharge resident_charge;
  std::vector<char> resident_matched;
  std::vector<SpillFile> left_parts(partitions);
  std::vector<SpillFile> right_parts(partitions);

  left.FinishSpill();
  right.FinishSpill();

  auto right_partition = [&](const Row& row) -> size_t {
    if (integer_key) {
      return SpillPartitionOf(IntegerJoinKey(row, right_key_column),
                              partitions);
    }
    return SpillPartitionOf(right_key_string(row), partitions);
  };
  auto left_partition = [&](const Row& row) -> size_t {
    if (integer_key) {
      return SpillPartitionOf(IntegerJoinKey(row, left_key_column), partitions);
    }
    return SpillPartitionOf(left_key_string(row), partitions);
  };

  right.ForEachRow([&](const Row& row) {
    if (build_key_excluded(row)) {
      return;
    }
    const size_t part = right_partition(row);
    const size_t bytes = EstimateRowBytes(row);
    if (part == 0 && budget.CanReserve(bytes)) {
      resident_charge.Add(bytes);
      resident_right.push_back(row);
      if (right_join) {
        resident_matched.push_back(0);
      }
    } else {
      right_parts[part].Append(row);
    }
  });
  right.rows.clear();
  right.rows.shrink_to_fit();
  right.ReleaseCharge();

  std::unordered_multimap<int64_t, const Row*> int_buckets;
  std::unordered_multimap<std::string, const Row*> str_buckets;
  if (integer_key) {
    int_buckets.reserve(resident_right.size());
    for (const Row& row : resident_right) {
      int_buckets.emplace(IntegerJoinKey(row, right_key_column), &row);
    }
  } else {
    str_buckets.reserve(resident_right.size());
    for (const Row& row : resident_right) {
      str_buckets.emplace(right_key_string(row), &row);
    }
  }

  Relation result(left.runtime());
  result.schema = left.schema + right.schema;
  const size_t right_width = right.schema.ColumnCount();
  const size_t left_width = left.schema.ColumnCount();

  auto mark_resident_match = [&](const Row& right_row) {
    if (!right_join || resident_right.empty()) {
      return;
    }
    const auto index = static_cast<size_t>(&right_row - resident_right.data());
    if (index < resident_matched.size()) {
      resident_matched[index] = 1;
    }
  };

  auto probe_resident = [&](const Row& left_row) {
    bool matched = false;
    if (integer_key) {
      const int64_t key = IntegerJoinKey(left_row, left_key_column);
      const auto [begin, end] = int_buckets.equal_range(key);
      for (auto iter = begin; iter != end; ++iter) {
        ++(*join_comparisons);
        Row combined = ConcatRows(left_row, *iter->second);
        if (matches(combined)) {
          mark_resident_match(*iter->second);
          result.AddRow(std::move(combined));
          matched = true;
        }
      }
    } else {
      const std::string key = left_key_string(left_row);
      const auto [begin, end] = str_buckets.equal_range(key);
      for (auto iter = begin; iter != end; ++iter) {
        ++(*join_comparisons);
        Row combined = ConcatRows(left_row, *iter->second);
        if (matches(combined)) {
          mark_resident_match(*iter->second);
          result.AddRow(std::move(combined));
          matched = true;
        }
      }
    }
    if (!matched && left_join) {
      std::vector<Value> nulls(right_width);
      result.AddRow(ConcatRows(left_row, Row(std::move(nulls))));
    }
  };

  left.ForEachRow([&](const Row& left_row) {
    if (probe_key_excluded(left_row)) {
      if (left_join) {
        std::vector<Value> nulls(right_width);
        result.AddRow(ConcatRows(left_row, Row(std::move(nulls))));
      }
      return;
    }
    const size_t part = left_partition(left_row);
    if (part == 0 && right_parts[0].Empty()) {
      probe_resident(left_row);
    } else {
      left_parts[part].Append(left_row);
    }
  });
  left.rows.clear();
  left.rows.shrink_to_fit();
  left.ReleaseCharge();

  int_buckets.clear();
  str_buckets.clear();
  const bool resident_folded = !right_parts[0].Empty();
  if (resident_folded) {
    for (const Row& row : resident_right) {
      right_parts[0].Append(row);
    }
  } else if (right_join) {
    // Partition 0 stayed fully resident and was probed live: emit the build
    // rows it never matched before releasing the resident image.
    for (size_t index = 0; index < resident_right.size(); ++index) {
      if (index < resident_matched.size() && resident_matched[index] != 0) {
        continue;
      }
      std::vector<Value> nulls(left_width);
      result.AddRow(Row(std::move(nulls)) + resident_right[index]);
    }
  }
  resident_right.clear();
  resident_right.shrink_to_fit();
  resident_charge.ReleaseAll();

  for (size_t part = 0; part < partitions; ++part) {
    left_parts[part].FinishWriting();
    right_parts[part].FinishWriting();
  }

  for (size_t part = 0; part < partitions; ++part) {
    if (left_parts[part].Empty() && right_parts[part].Empty()) {
      continue;
    }
    ASSIGN_OR_RETURN(std::vector<Row>, right_rows,
                     (right_parts[part].ReadAllRows()));
    QueryMemoryCharge part_charge;
    for (const Row& row : right_rows) {
      part_charge.Add(EstimateRowBytes(row));
    }
    std::vector<char> part_matched(right_join ? right_rows.size() : 0, 0);
    std::unordered_multimap<int64_t, const Row*> part_int_buckets;
    std::unordered_multimap<std::string, const Row*> part_str_buckets;
    if (integer_key) {
      part_int_buckets.reserve(right_rows.size());
      for (const Row& row : right_rows) {
        part_int_buckets.emplace(IntegerJoinKey(row, right_key_column), &row);
      }
    } else {
      part_str_buckets.reserve(right_rows.size());
      for (const Row& row : right_rows) {
        part_str_buckets.emplace(right_key_string(row), &row);
      }
    }
    left_parts[part].ForEachRow([&](const Row& left_row) {
      bool matched = false;
      if (integer_key) {
        const int64_t key = IntegerJoinKey(left_row, left_key_column);
        const auto [begin, end] = part_int_buckets.equal_range(key);
        for (auto iter = begin; iter != end; ++iter) {
          ++(*join_comparisons);
          Row combined = ConcatRows(left_row, *iter->second);
          if (matches(combined)) {
            if (right_join) {
              const auto index =
                  static_cast<size_t>(iter->second - right_rows.data());
              if (index < part_matched.size()) {
                part_matched[index] = 1;
              }
            }
            result.AddRow(std::move(combined));
            matched = true;
          }
        }
      } else {
        const std::string key = left_key_string(left_row);
        const auto [begin, end] = part_str_buckets.equal_range(key);
        for (auto iter = begin; iter != end; ++iter) {
          ++(*join_comparisons);
          Row combined = ConcatRows(left_row, *iter->second);
          if (matches(combined)) {
            if (right_join) {
              const auto index =
                  static_cast<size_t>(iter->second - right_rows.data());
              if (index < part_matched.size()) {
                part_matched[index] = 1;
              }
            }
            result.AddRow(std::move(combined));
            matched = true;
          }
        }
      }
      if (!matched && left_join) {
        std::vector<Value> nulls(right_width);
        result.AddRow(ConcatRows(left_row, Row(std::move(nulls))));
      }
    });
    // RIGHT / FULL: unmatched build rows of this partition, including rows
    // that only ever lived in the spill file (D8: never dropped).
    if (right_join) {
      for (size_t index = 0; index < right_rows.size(); ++index) {
        if (part_matched[index] != 0) {
          continue;
        }
        std::vector<Value> nulls(left_width);
        result.AddRow(Row(std::move(nulls)) + right_rows[index]);
      }
    }
  }
  result.FinishSpill();
  return result;
}

bool ShouldHybridJoin(const Relation& left, const Relation& right) {
  if (left.HasSpill() || right.HasSpill()) {
    return true;
  }
  // Rough hash-table estimate: keys + pointers for the build (right) side.
  size_t estimate = 0;
  for (const Row& row : right.rows) {
    estimate += EstimateRowBytes(row) + 64;
  }
  return PreferHybridHashJoin(estimate);
}

// Chunked parallel probe for big in-memory hash joins.  The serial probe
// iterates the left relation on one core while the other workers sit idle;
// at SF=1 the lineitem-side joins spend seconds there.  Callers gate this
// on: neither side spills (guaranteed by ShouldHybridJoin), no residual
// predicate re-entering evaluation, no null-safe keys, no RIGHT/FULL
// outer emission (it needs shared matched flags), and enough left rows to
// amortize thread startup.  Workers claim contiguous chunks of left.rows,
// join into a per-chunk buffer, and flush the buffers through result->AddRow
// one chunk at a time in strict chunk order, so output row order matches
// the serial loop exactly.  Left-outer unmatched emission happens inside the
// worker's chunk buffer, preserving order as well.  At most one buffer per
// worker is ever in flight, so the query memory budget (and the spill
// behavior inside AddRow) stays in force.  The build-side buckets are
// read-only during the probe.
constexpr size_t kParallelProbeMinLeftRows = 65536;
constexpr size_t kParallelProbeChunk = 8192;
constexpr size_t kParallelProbeMaxWorkers = 16;
// Flip the hash build onto the smaller side when the other side dwarfs it:
// the build's random bucket writes scale with the build row count, while a
// streamed probe walks its rows once.  Below this size the build is cheap
// enough that the flip's output-order change is not worth it.
constexpr size_t kFlipMinBuildRows = 1048576;
constexpr size_t kFlipRatio = 4;

template <typename Buckets, typename KeyOfRow>
static Status ParallelHashProbe(  // NOLINT(misc-use-anonymous-namespace)
    Buckets& buckets, const Relation& left,
    const std::vector<slot_t>& left_columns, KeyOfRow key_of_row,
    bool left_outer_emit, size_t right_width, size_t worker_count,
    Relation* result, size_t* join_comparisons, bool stream_is_left = true) {
  const size_t total = left.rows.size();
  const size_t num_chunks =
      (total + kParallelProbeChunk - 1) / kParallelProbeChunk;
  worker_count = std::min({worker_count, num_chunks, total});
  std::atomic<size_t> next_chunk{0};
  std::vector<std::vector<Row>> buffers(num_chunks);
  std::mutex flush_mutex;
  std::condition_variable flush_cv;
  size_t flush_cursor = 0;
  // Cache-line padded: every worker bumps its own counter once per emitted
  // match, so adjacent unpadded slots would false-share on the hot line.
  struct alignas(64) PaddedComparison {
    size_t value{0};
  };
  std::vector<PaddedComparison> comparisons(worker_count);
  std::mutex error_mutex;
  Status first_error{Status::kSuccess};

  // Every claimed chunk advances the cursor exactly once, even on the error
  // path, so waiters never block on a chunk whose owner already returned.
  auto flush_one = [&](size_t chunk) {
    std::unique_lock lock(flush_mutex);
    while (flush_cursor != chunk) {  // NOLINT(bugprone-infinite-loop)
      // flush_cursor only advances in OTHER waiters' iterations below; the
      // analyzer cannot model the cv handshake, so this wait looks infinite.
      flush_cv.wait(lock);
    }
    for (Row& row : buffers[chunk]) {
      result->AddRow(std::move(row));
    }
    buffers[chunk].clear();
    buffers[chunk].shrink_to_fit();
    ++flush_cursor;
    lock.unlock();
    flush_cv.notify_all();
  };

  auto run_worker = [&](size_t worker_id) {
    for (;;) {
      const size_t chunk = next_chunk.fetch_add(1);
      if (chunk >= num_chunks) {
        return;
      }
      try {
        const size_t begin = chunk * kParallelProbeChunk;
        const size_t end = std::min(total, begin + kParallelProbeChunk);
        std::vector<Row>& local = buffers[chunk];
        local.reserve(end - begin);
        // Software-pipeline the probe keys: compute the next row's key and
        // warm its bucket head before emitting the current row's matches, so
        // the hash-table pointer chase overlaps with concat work instead of
        // stalling the walk.  key_of_row is pure, and null-key rows skip the
        // probe anyway, so prefetching their (unused) keys is harmless.
        auto next_key = key_of_row(left.rows[begin]);
        for (size_t i = begin; i < end; ++i) {
          const Row& left_row = left.rows[i];
          // Carried across iterations: every move is followed by a fresh
          // assignment for the next i, except on the final one where the
          // loop exits before next_key is read again.
          auto key = std::move(next_key);  // NOLINT(bugprone-use-after-move)
          if (i + 1 < end) {
            auto upcoming = key_of_row(left.rows[i + 1]);
            buckets.PrefetchBucket(upcoming);
            next_key = std::move(upcoming);
          }
          if (HasNullKey(left_row, left_columns)) {
            if (left_outer_emit) {
              std::vector<Value> nulls(right_width);
              local.push_back(ConcatRows(left_row, Row(std::move(nulls))));
            }
            continue;
          }
          bool matched = false;
          const auto [eq_begin, eq_end] = buckets.equal_range(key);
          for (auto iter = eq_begin; iter != eq_end; ++iter) {
            ++comparisons[worker_id].value;
            Row combined = stream_is_left ? ConcatRows(left_row, *iter->second)
                                          : ConcatRows(*iter->second, left_row);
            local.push_back(std::move(combined));
            matched = true;
          }
          if (!matched && left_outer_emit) {
            std::vector<Value> nulls(right_width);
            local.push_back(ConcatRows(left_row, Row(std::move(nulls))));
          }
        }
      } catch (const std::exception& error) {
        std::scoped_lock error_lock(error_mutex);
        if (first_error.ok()) {
          first_error = StatusError(StatusCode::kRuntimeError, error.what());
        }
      } catch (...) {
        std::scoped_lock error_lock(error_mutex);
        if (first_error.ok()) {
          first_error =
              StatusError(StatusCode::kRuntimeError, "parallel probe failed");
        }
      }
      flush_one(chunk);
    }
  };

  if (worker_count <= 1) {
    run_worker(0);
  } else {
    std::vector<std::jthread> workers;
    workers.reserve(worker_count);
    for (size_t i = 0; i < worker_count; ++i) {
      workers.emplace_back([&, i] { run_worker(i); });
    }
  }
  for (const PaddedComparison& count : comparisons) {
    *join_comparisons += count.value;
  }
  return first_error;
}  // namespace

StatusOr<Relation> Join(TransactionContext& context, Relation left,
                        Relation right, const SelectSource& source,
                        const Scope* outer, const CteMap& ctes) {
  const auto join_begin = std::chrono::steady_clock::now();
  Relation result(context.execution_runtime());
  result.schema = left.schema + right.schema;
  result.hash_joins = left.hash_joins + right.hash_joins;
  result.hybrid_hash_joins = left.hybrid_hash_joins + right.hybrid_hash_joins;
  result.in_memory_hash_joins =
      left.in_memory_hash_joins + right.in_memory_hash_joins;
  result.nested_loop_joins = left.nested_loop_joins + right.nested_loop_joins;
  result.join_comparisons = left.join_comparisons + right.join_comparisons;
  result.peak_intermediate_rows =
      std::max(left.peak_intermediate_rows, right.peak_intermediate_rows);
  std::vector<std::string> merged_using;
  auto append_using = [&](const std::vector<std::string>& names) {
    for (const std::string& name : names) {
      const std::string lowered = LowerIdentifierLocal(name);
      if (!ContainsIdentifierLocal(merged_using, lowered)) {
        merged_using.push_back(lowered);
      }
    }
  };
  if (left.using_columns != nullptr) {
    append_using(*left.using_columns);
  }
  if (right.using_columns != nullptr) {
    append_using(*right.using_columns);
  }
  append_using(source.using_columns);
  std::vector<std::string> retained_using;
  retained_using.reserve(merged_using.size());
  for (const std::string& name : merged_using) {
    const bool declared_now =
        ContainsIdentifierLocal(source.using_columns, name);
    const bool right_collision =
        !MatchingColumnOffsets(right.schema, name).empty();
    if (!declared_now && right_collision) {
      continue;
    }
    retained_using.push_back(name);
  }
  if (!retained_using.empty()) {
    result.using_columns = std::make_shared<const std::vector<std::string>>(
        std::move(retained_using));
  }
  const bool want_left_nulls = source.join_type == JoinType::kLeft ||
                               source.join_type == JoinType::kFull;
  const bool want_right_nulls = source.join_type == JoinType::kRight ||
                                source.join_type == JoinType::kFull;
  // Unmatched right-side emission needs index-addressable rows; spillable
  // inputs are materialized up front (the relational join is the fallback
  // path, so this keeps semantics without touching the hybrid operator).
  if (want_right_nulls && right.HasSpill()) {
    ASSIGN_OR_RETURN(Relation, materialized_right, MaterializeRelation(right));
    right = std::move(materialized_right);
  }
  right.FinishSpill();
  const Row* right_base = right.rows.empty() ? nullptr : right.rows.data();
  std::vector<char> right_matched(want_right_nulls ? right.rows.size() : 0, 0);
  auto mark_right_match = [&](const Row& right_row) {
    if (!want_right_nulls || right_base == nullptr) {
      return;
    }
    const auto index = static_cast<size_t>(&right_row - right_base);
    if (index < right_matched.size()) {
      right_matched[index] = 1;
    }
  };
  const std::vector<Expression> predicates =
      SplitConjuncts(source.join_condition);
  std::vector<Expression> ordinary_predicates;
  ordinary_predicates.reserve(predicates.size());
  for (const Expression& predicate : predicates) {
    if (!IsDeclaredUsingEquality(predicate, source.using_columns)) {
      ordinary_predicates.push_back(predicate);
    }
  }
  std::vector<EqualityKey> equality_keys =
      EqualityKeys(left.schema, right.schema, ordinary_predicates);
  struct UsingJoinKey {
    std::vector<size_t> left_columns;
    std::vector<size_t> right_columns;
  };
  std::vector<UsingJoinKey> using_residuals;
  for (const std::string& name : source.using_columns) {
    std::vector<size_t> left_columns = MatchingColumnOffsets(left.schema, name);
    std::vector<size_t> right_columns =
        MatchingColumnOffsets(right.schema, name);
    if (left_columns.empty() || right_columns.empty()) {
      continue;
    }
    if (left_columns.size() > 1 &&
        (left.using_columns == nullptr ||
         !ContainsIdentifierLocal(*left.using_columns, name))) {
      return StatusError(StatusCode::kAmbiguousQuery,
                         "ambiguous USING column: " + name);
    }
    if (right_columns.size() > 1 &&
        (right.using_columns == nullptr ||
         !ContainsIdentifierLocal(*right.using_columns, name))) {
      return StatusError(StatusCode::kAmbiguousQuery,
                         "ambiguous USING column: " + name);
    }
    const bool same_single_type =
        left_columns.size() == 1 && right_columns.size() == 1 &&
        left.schema.GetColumn(left_columns.front()).Type() ==
            right.schema.GetColumn(right_columns.front()).Type();
    if (same_single_type) {
      equality_keys.push_back(
          {left_columns.front(), right_columns.front(), false});
    } else {
      using_residuals.push_back(
          {std::move(left_columns), std::move(right_columns)});
    }
  }
  const std::vector<Expression> residual =
      ResidualJoinPredicates(left.schema, right.schema, ordinary_predicates);
  Status residual_error{Status::kSuccess};
  auto matches = [&](const Row& combined) {
    const auto merged_value = [&](const std::vector<size_t>& columns,
                                  const size_t offset) -> const Value* {
      for (const size_t column : columns) {
        const Value& value = combined[column + offset];
        if (!value.IsNull()) {
          return &value;
        }
      }
      return nullptr;
    };
    for (const UsingJoinKey& key : using_residuals) {
      const Value* left_value = merged_value(key.left_columns, 0);
      const Value* right_value =
          merged_value(key.right_columns, left.schema.ColumnCount());
      if (left_value == nullptr || right_value == nullptr) {
        return false;
      }
      if ((left_value->type == ValueType::kDouble &&
           std::isnan(left_value->value.double_value)) ||
          (right_value->type == ValueType::kDouble &&
           std::isnan(right_value->value.double_value))) {
        return false;
      }
      StatusOr<Value> equal =
          TryBinary(BinaryOperation::kEquals, *left_value, *right_value);
      if (!equal.HasValue()) {
        residual_error = equal.GetStatus();
        return false;
      }
      if (!Truthy(equal.Value())) {
        return false;
      }
    }
    if (residual.empty()) {
      return true;
    }
    Scope scope{.row = &combined, .schema = &result.schema, .outer = outer};
    scope.using_columns = result.using_columns.get();
    return std::ranges::all_of(residual, [&](const Expression& predicate) {
      if (residual_error != Status::kSuccess) {
        return false;
      }
      StatusOr<Value> value =
          TryEvaluate(predicate, scope, nullptr, context, ctes);
      if (!value.HasValue()) {
        residual_error = value.GetStatus();
        return false;
      }
      return Truthy(value.Value());
    });
  };
  auto emit_unmatched = [&](const Row& left_row) {
    if (!want_left_nulls) {
      return;
    }
    std::vector<Value> nulls(right.schema.ColumnCount());
    result.AddRow(ConcatRows(left_row, Row(std::move(nulls))));
  };

  if (equality_keys.empty()) {
    ++result.nested_loop_joins;
    left.FinishSpill();
    right.FinishSpill();
    left.ForEachRow([&](const Row& left_row) {
      bool matched = false;
      right.ForEachRow([&](const Row& right_row) {
        ++result.join_comparisons;
        Row combined = ConcatRows(left_row, right_row);
        if (matches(combined)) {
          mark_right_match(right_row);
          result.AddRow(std::move(combined));
          matched = true;
        }
      });
      if (!matched) {
        emit_unmatched(left_row);
      }
    });
  } else {
    ++result.hash_joins;
    // D8 (docs/design.md): the hybrid operator now implements null-safe
    // keys and LEFT/RIGHT/FULL outer with full partition reprocessing.
    const bool has_null_safe_key = std::ranges::any_of(
        equality_keys, [](const EqualityKey& key) { return key.null_safe; });
    // Biased keys (`col = col +/- constant`) ride only the in-memory int and
    // memcomparable key paths; the hybrid spilling operator and null-safe
    // encoding cannot bias, so those entries degrade back to residuals
    // (semantics unchanged, the residual verifier keeps them exact).
    if (std::ranges::any_of(equality_keys,
                            [](const EqualityKey& key) {
                              return key.left_offset != 0 ||
                                     key.right_offset != 0;
                            }) &&
        (has_null_safe_key || ShouldHybridJoin(left, right))) {
      std::vector<EqualityKey> plain_keys;
      plain_keys.reserve(equality_keys.size());
      for (const EqualityKey& key : equality_keys) {
        if (key.left_offset == 0 && key.right_offset == 0) {
          plain_keys.push_back(key);
        }
      }
      equality_keys = std::move(plain_keys);
    }
    std::vector<slot_t> left_columns;
    std::vector<slot_t> right_columns;
    std::vector<int64_t> left_key_offsets;
    std::vector<int64_t> right_key_offsets;
    left_columns.reserve(equality_keys.size());
    right_columns.reserve(equality_keys.size());
    for (const EqualityKey& key : equality_keys) {
      left_columns.push_back(static_cast<slot_t>(key.left));
      right_columns.push_back(static_cast<slot_t>(key.right));
      left_key_offsets.push_back(key.left_offset);
      right_key_offsets.push_back(key.right_offset);
    }
    if (ShouldHybridJoin(left, right)) {
      ++result.hybrid_hash_joins;
      ASSIGN_OR_RETURN(
          Relation, joined,
          (HybridHashJoin(std::move(left), std::move(right), left_columns,
                          right_columns, matches, want_left_nulls,
                          &result.join_comparisons, has_null_safe_key,
                          want_right_nulls)));
      joined.hash_joins = result.hash_joins;
      joined.hybrid_hash_joins = result.hybrid_hash_joins;
      joined.in_memory_hash_joins = result.in_memory_hash_joins;
      joined.nested_loop_joins = result.nested_loop_joins;
      joined.join_comparisons = result.join_comparisons;
      joined.using_columns = result.using_columns;
      if (context.execution_runtime() != nullptr) {
        context.execution_runtime()->join_ms += ElapsedMs(join_begin);
      }
      RETURN_IF_FAIL(residual_error);
      return joined;
    }
    ++result.in_memory_hash_joins;
    // Parallel chunked probe (see ParallelHashProbe): needs a pure row
    // decision (no residual re-entering TryEvaluate), no shared right-side
    // matched flags, and a probe side big enough to amortize the threads.
    static const bool probe_disabled =
        std::getenv("TINYLAMB_DISABLE_PARALLEL_PROBE") != nullptr;
    const bool can_parallelize = !probe_disabled && residual.empty() &&
                                 using_residuals.empty() &&
                                 !has_null_safe_key && !want_right_nulls &&
                                 left.rows.size() >= kParallelProbeMinLeftRows;
    const size_t probe_workers =
        can_parallelize ? std::min<size_t>(kParallelProbeMaxWorkers,
                                           std::thread::hardware_concurrency())
                        : 1;
    const bool integer_key = !has_null_safe_key &&
                             SingleIntegerJoinKey(left.schema, left_columns) &&
                             SingleIntegerJoinKey(right.schema, right_columns);
    if (integer_key) {
      FlatJoinBuckets<int64_t> buckets;
      buckets.reserve(right.rows.size());
      for (const Row& row : right.rows) {
        if (has_null_safe_key || !HasNullKey(row, right_columns)) {
          buckets.emplace(AddJoinKeyBias(IntegerJoinKey(row, right_columns[0]),
                                         right_key_offsets[0]),
                          &row);
        }
      }
      if (can_parallelize) {
        RETURN_IF_FAIL(ParallelHashProbe(
            buckets, left, left_columns,
            [&](const Row& row) {
              return AddJoinKeyBias(IntegerJoinKey(row, left_columns[0]),
                                    left_key_offsets[0]);
            },
            want_left_nulls, right.schema.ColumnCount(), probe_workers, &result,
            &result.join_comparisons));
        RETURN_IF_FAIL(residual_error);
      } else {
        for (const Row& left_row : left.rows) {
          bool matched = false;
          if (has_null_safe_key || !HasNullKey(left_row, left_columns)) {
            const int64_t key = AddJoinKeyBias(
                IntegerJoinKey(left_row, left_columns[0]), left_key_offsets[0]);
            const auto [begin, end] = buckets.equal_range(key);
            for (auto iter = begin; iter != end; ++iter) {
              ++result.join_comparisons;
              Row combined = ConcatRows(left_row, *iter->second);
              if (matches(combined)) {
                mark_right_match(*iter->second);
                result.AddRow(std::move(combined));
                matched = true;
              }
            }
          }
          if (!matched) {
            emit_unmatched(left_row);
          }
        }
      }
    } else if (!has_null_safe_key &&
               CompositeIntJoinKey(left.schema, left_columns, right.schema,
                                   right_columns)) {
      FlatJoinBuckets<CompositeIntKey, CompositeIntKeyHash> buckets;
      buckets.reserve(right.rows.size());
      for (const Row& row : right.rows) {
        if (HasNullKey(row, right_columns)) {
          continue;
        }
        buckets.emplace(
            EncodeCompositeIntKey(row, right_columns, &right_key_offsets),
            &row);
      }
      if (can_parallelize) {
        RETURN_IF_FAIL(ParallelHashProbe(
            buckets, left, left_columns,
            [&](const Row& row) {
              return EncodeCompositeIntKey(row, left_columns,
                                           &left_key_offsets);
            },
            want_left_nulls, right.schema.ColumnCount(), probe_workers, &result,
            &result.join_comparisons));
        RETURN_IF_FAIL(residual_error);
      } else {
        for (const Row& left_row : left.rows) {
          bool matched = false;
          if (!HasNullKey(left_row, left_columns)) {
            const CompositeIntKey key = EncodeCompositeIntKey(
                left_row, left_columns, &left_key_offsets);
            const auto [begin, end] = buckets.equal_range(key);
            for (auto iter = begin; iter != end; ++iter) {
              ++result.join_comparisons;
              Row combined = ConcatRows(left_row, *iter->second);
              if (matches(combined)) {
                mark_right_match(*iter->second);
                result.AddRow(std::move(combined));
                matched = true;
              }
            }
          }
          if (!matched) {
            emit_unmatched(left_row);
          }
        }
      }
    } else {
      FlatJoinBuckets<std::string> buckets;
      buckets.reserve(right.rows.size());
      for (const Row& row : right.rows) {
        if (has_null_safe_key || !HasNullKey(row, right_columns)) {
          buckets.emplace(
              has_null_safe_key
                  ? EncodeNullSafeJoinKey(row, right_columns)
                  : EncodeJoinKey(row, right_columns, &right_key_offsets),
              &row);
        }
      }
      if (can_parallelize) {
        RETURN_IF_FAIL(ParallelHashProbe(
            buckets, left, left_columns,
            [&](const Row& row) {
              return EncodeJoinKey(row, left_columns, &left_key_offsets);
            },
            want_left_nulls, right.schema.ColumnCount(), probe_workers, &result,
            &result.join_comparisons));
        RETURN_IF_FAIL(residual_error);
      } else {
        for (const Row& left_row : left.rows) {
          bool matched = false;
          if (has_null_safe_key || !HasNullKey(left_row, left_columns)) {
            const std::string key =
                has_null_safe_key
                    ? EncodeNullSafeJoinKey(left_row, left_columns)
                    : EncodeJoinKey(left_row, left_columns, &left_key_offsets);
            const auto [begin, end] = buckets.equal_range(key);
            for (auto iter = begin; iter != end; ++iter) {
              ++result.join_comparisons;
              Row combined = ConcatRows(left_row, *iter->second);
              if (matches(combined)) {
                mark_right_match(*iter->second);
                result.AddRow(std::move(combined));
                matched = true;
              }
            }
          }
          if (!matched) {
            emit_unmatched(left_row);
          }
        }
      }
    }
  }
  // RIGHT / FULL OUTER: every build-side row that never matched emits once,
  // null-extended on the left.
  if (want_right_nulls) {
    for (size_t index = 0; index < right.rows.size(); ++index) {
      if (right_matched[index] != 0) {
        continue;
      }
      std::vector<Value> nulls(left.schema.ColumnCount());
      result.AddRow(ConcatRows(Row(std::move(nulls)), right.rows[index]));
    }
  }
  result.FinishSpill();
  result.peak_intermediate_rows =
      std::max(result.peak_intermediate_rows, result.TotalRows());
  if (context.execution_runtime() != nullptr) {
    context.execution_runtime()->join_ms += ElapsedMs(join_begin);
  }
  RETURN_IF_FAIL(residual_error);
  return result;
}

StatusOr<Relation> InnerJoin(TransactionContext& context, Relation left,
                             Relation right,
                             const std::vector<Expression>& predicates,
                             const Scope* outer, const CteMap& ctes) {
  const auto join_begin = std::chrono::steady_clock::now();
  Relation result(context.execution_runtime());
  result.schema = left.schema + right.schema;
  result.hash_joins = left.hash_joins + right.hash_joins;
  result.hybrid_hash_joins = left.hybrid_hash_joins + right.hybrid_hash_joins;
  result.in_memory_hash_joins =
      left.in_memory_hash_joins + right.in_memory_hash_joins;
  result.nested_loop_joins = left.nested_loop_joins + right.nested_loop_joins;
  result.join_comparisons = left.join_comparisons + right.join_comparisons;
  result.peak_intermediate_rows =
      std::max(left.peak_intermediate_rows, right.peak_intermediate_rows);
  std::vector<EqualityKey> equality_keys =
      EqualityKeys(left.schema, right.schema, predicates);
  const std::vector<Expression> residual =
      ResidualJoinPredicates(left.schema, right.schema, predicates);

  Status residual_error{Status::kSuccess};
  auto matches = [&](const Row& combined) {
    if (residual.empty()) {
      return true;
    }
    Scope scope{.row = &combined, .schema = &result.schema, .outer = outer};
    return std::ranges::all_of(residual, [&](const Expression& predicate) {
      if (residual_error != Status::kSuccess) {
        return false;
      }
      StatusOr<Value> value =
          TryEvaluate(predicate, scope, nullptr, context, ctes);
      if (!value.HasValue()) {
        residual_error = value.GetStatus();
        return false;
      }
      return Truthy(value.Value());
    });
  };

  if (equality_keys.empty()) {
    ++result.nested_loop_joins;
    left.FinishSpill();
    right.FinishSpill();
    left.ForEachRow([&](const Row& left_row) {
      right.ForEachRow([&](const Row& right_row) {
        ++result.join_comparisons;
        Row combined = ConcatRows(left_row, right_row);
        if (matches(combined)) {
          result.AddRow(std::move(combined));
        }
      });
    });
  } else {
    ++result.hash_joins;
    const bool has_null_safe_key = std::ranges::any_of(
        equality_keys, [](const EqualityKey& key) { return key.null_safe; });
    // D8 (docs/design.md): null-safe keys use the hybrid operator too, so a
    // spilled input is never silently reduced to its resident prefix.
    // Biased keys degrade to residuals here: the hybrid operator cannot
    // bias, and the residual verifier keeps results exact.
    if (std::ranges::any_of(equality_keys,
                            [](const EqualityKey& key) {
                              return key.left_offset != 0 ||
                                     key.right_offset != 0;
                            }) &&
        (has_null_safe_key || ShouldHybridJoin(left, right))) {
      std::vector<EqualityKey> plain_keys;
      plain_keys.reserve(equality_keys.size());
      for (const EqualityKey& key : equality_keys) {
        if (key.left_offset == 0 && key.right_offset == 0) {
          plain_keys.push_back(key);
        }
      }
      equality_keys = std::move(plain_keys);
    }
    std::vector<slot_t> left_columns;
    std::vector<slot_t> right_columns;
    std::vector<int64_t> left_key_offsets;
    std::vector<int64_t> right_key_offsets;
    left_columns.reserve(equality_keys.size());
    right_columns.reserve(equality_keys.size());
    for (const EqualityKey& key : equality_keys) {
      left_columns.push_back(static_cast<slot_t>(key.left));
      right_columns.push_back(static_cast<slot_t>(key.right));
      left_key_offsets.push_back(key.left_offset);
      right_key_offsets.push_back(key.right_offset);
    }
    if (ShouldHybridJoin(left, right)) {
      ++result.hybrid_hash_joins;
      ASSIGN_OR_RETURN(
          Relation, joined,
          (HybridHashJoin(std::move(left), std::move(right), left_columns,
                          right_columns, matches, false,
                          &result.join_comparisons, has_null_safe_key, false)));
      joined.hash_joins = result.hash_joins;
      joined.hybrid_hash_joins = result.hybrid_hash_joins;
      joined.in_memory_hash_joins = result.in_memory_hash_joins;
      joined.nested_loop_joins = result.nested_loop_joins;
      joined.join_comparisons = result.join_comparisons;
      if (context.execution_runtime() != nullptr) {
        context.execution_runtime()->join_ms += ElapsedMs(join_begin);
      }
      RETURN_IF_FAIL(residual_error);
      return joined;
    }
    ++result.in_memory_hash_joins;
    // Build the hash on the smaller side and stream the bigger side: the
    // build's random bucket writes scale with the build row count while a
    // streamed probe walks its rows once.  Emitted rows keep the
    // original-left ++ original-right column order; only the walk direction
    // flips, so the output row order may differ from the unflipped join
    // (inner-join consumers are order-insensitive; ORDER BY applies later).
    // Inner joins only: outer emissions and shared matched flags assume the
    // unflipped roles.
    static const bool flip_disabled =
        std::getenv("TINYLAMB_DISABLE_JOIN_FLIP") != nullptr;
    const bool flip = !flip_disabled && !has_null_safe_key &&
                      right.rows.size() >= kFlipMinBuildRows &&
                      right.rows.size() > left.rows.size() * kFlipRatio;
    const Relation& build_side = flip ? left : right;
    const Relation& probe_side = flip ? right : left;
    const std::vector<slot_t>& build_columns =
        flip ? left_columns : right_columns;
    const std::vector<slot_t>& probe_columns =
        flip ? right_columns : left_columns;
    const std::vector<int64_t>& build_key_offsets =
        flip ? left_key_offsets : right_key_offsets;
    const std::vector<int64_t>& probe_key_offsets =
        flip ? right_key_offsets : left_key_offsets;
    const bool can_parallelize =
        residual.empty() && !has_null_safe_key &&
        probe_side.rows.size() >= kParallelProbeMinLeftRows;
    const size_t probe_workers =
        can_parallelize ? std::min<size_t>(kParallelProbeMaxWorkers,
                                           std::thread::hardware_concurrency())
                        : 1;
    const bool integer_key = !has_null_safe_key &&
                             SingleIntegerJoinKey(left.schema, left_columns) &&
                             SingleIntegerJoinKey(right.schema, right_columns);
    if (integer_key) {
      FlatJoinBuckets<int64_t> buckets;
      buckets.reserve(build_side.rows.size());
      for (const Row& row : build_side.rows) {
        if (HasNullKey(row, build_columns)) {
          continue;
        }
        buckets.emplace(AddJoinKeyBias(IntegerJoinKey(row, build_columns[0]),
                                       build_key_offsets[0]),
                        &row);
      }
      if (can_parallelize) {
        RETURN_IF_FAIL(ParallelHashProbe(
            buckets, probe_side, probe_columns,
            [&](const Row& row) {
              return AddJoinKeyBias(IntegerJoinKey(row, probe_columns[0]),
                                    probe_key_offsets[0]);
            },
            false, build_side.schema.ColumnCount(), probe_workers, &result,
            &result.join_comparisons, !flip));
        RETURN_IF_FAIL(residual_error);
      } else {
        for (const Row& probe_row : probe_side.rows) {
          if (HasNullKey(probe_row, probe_columns)) {
            continue;
          }
          const int64_t key =
              AddJoinKeyBias(IntegerJoinKey(probe_row, probe_columns[0]),
                             probe_key_offsets[0]);
          const auto [begin, end] = buckets.equal_range(key);
          for (auto iter = begin; iter != end; ++iter) {
            ++result.join_comparisons;
            Row combined = flip ? ConcatRows(*iter->second, probe_row)
                                : ConcatRows(probe_row, *iter->second);
            if (matches(combined)) {
              result.AddRow(std::move(combined));
            }
          }
        }
      }
    } else if (!has_null_safe_key &&
               CompositeIntJoinKey(left.schema, left_columns, right.schema,
                                   right_columns)) {
      FlatJoinBuckets<CompositeIntKey, CompositeIntKeyHash> buckets;
      buckets.reserve(build_side.rows.size());
      for (const Row& row : build_side.rows) {
        if (HasNullKey(row, build_columns)) {
          continue;
        }
        buckets.emplace(
            EncodeCompositeIntKey(row, build_columns, &build_key_offsets),
            &row);
      }
      if (can_parallelize) {
        RETURN_IF_FAIL(ParallelHashProbe(
            buckets, probe_side, probe_columns,
            [&](const Row& row) {
              return EncodeCompositeIntKey(row, probe_columns,
                                           &probe_key_offsets);
            },
            false, build_side.schema.ColumnCount(), probe_workers, &result,
            &result.join_comparisons, !flip));
        RETURN_IF_FAIL(residual_error);
      } else {
        for (const Row& probe_row : probe_side.rows) {
          if (HasNullKey(probe_row, probe_columns)) {
            continue;
          }
          const CompositeIntKey key = EncodeCompositeIntKey(
              probe_row, probe_columns, &probe_key_offsets);
          const auto [begin, end] = buckets.equal_range(key);
          for (auto iter = begin; iter != end; ++iter) {
            ++result.join_comparisons;
            Row combined = flip ? ConcatRows(*iter->second, probe_row)
                                : ConcatRows(probe_row, *iter->second);
            if (matches(combined)) {
              result.AddRow(std::move(combined));
            }
          }
        }
      }
    } else {
      FlatJoinBuckets<std::string> buckets;
      buckets.reserve(build_side.rows.size());
      for (const Row& row : build_side.rows) {
        if (!has_null_safe_key && HasNullKey(row, build_columns)) {
          continue;
        }
        buckets.emplace(
            has_null_safe_key
                ? EncodeNullSafeJoinKey(row, build_columns)
                : EncodeJoinKey(row, build_columns, &build_key_offsets),
            &row);
      }
      if (can_parallelize) {
        RETURN_IF_FAIL(ParallelHashProbe(
            buckets, probe_side, probe_columns,
            [&](const Row& row) {
              return EncodeJoinKey(row, probe_columns, &probe_key_offsets);
            },
            false, build_side.schema.ColumnCount(), probe_workers, &result,
            &result.join_comparisons, !flip));
        RETURN_IF_FAIL(residual_error);
      } else {
        for (const Row& probe_row : probe_side.rows) {
          if (!has_null_safe_key && HasNullKey(probe_row, probe_columns)) {
            continue;
          }
          const std::string key =
              has_null_safe_key
                  ? EncodeNullSafeJoinKey(probe_row, probe_columns)
                  : EncodeJoinKey(probe_row, probe_columns, &probe_key_offsets);
          const auto [begin, end] = buckets.equal_range(key);
          for (auto iter = begin; iter != end; ++iter) {
            ++result.join_comparisons;
            Row combined = flip ? ConcatRows(*iter->second, probe_row)
                                : ConcatRows(probe_row, *iter->second);
            if (matches(combined)) {
              result.AddRow(std::move(combined));
            }
          }
        }
      }
    }
  }
  result.FinishSpill();
  result.peak_intermediate_rows =
      std::max(result.peak_intermediate_rows, result.TotalRows());
  if (context.execution_runtime() != nullptr) {
    context.execution_runtime()->join_ms += ElapsedMs(join_begin);
  }
  RETURN_IF_FAIL(residual_error);
  return result;
}

namespace {

struct FusedGroup {
  // Canonical group key, kept so the merge re-probes the shared map without
  // re-evaluating the key programs.
  Row key;
  Row representative;
  size_t state_offset{0};
  // count-star-only fusions accumulate here instead of through
  // AggregateAccumulator states: the per-pair work is one integer increment
  // and the merge is an integer add.
  int64_t raw_count{0};
};

// Bulk raw-count state: one map entry per distinct build key replaces the
// whole bucket chain.  `row` keeps the most recently inserted build row for
// that key -- the row a bucket-head walk would have filled a new group's
// representative from.
struct BulkCountValue {
  int64_t count{0};
  const Row* row{nullptr};
};

struct FusedWorkerLocal {
  GroupKeyMap offsets;
  // Single-INT64/DATE group-key fast path: raw-payload probes skip the
  // Value-vector hashing the generic map pays per pair.  Key spaces of the
  // two maps are disjoint (NULL keys route to the generic map, which groups
  // NULLs), so a group lives in exactly one of them.
  FlatIntKeyMap<size_t> int_offsets;
  std::vector<FusedGroup> groups;
  std::deque<AggregateAccumulator> states;
  // Reusable probe key: avoids a fresh vector per probe row (see
  // accumulate_into).  The transparent group-key hash/equal pair accepts the
  // bare value vector, so probes never construct a Row.
  std::vector<Value> key_scratch;
  Status error{Status::kSuccess};
  size_t comparisons{0};
};

// Per-program evaluation shape for the pair loop.  A single kLoadColumn
// program reads its cell straight from the scratch row; a fast-shaped
// program (columns/constants/int64+double arithmetic — see BytecodeProgram)
// runs the boxing-free interpreter; anything else keeps the canonical
// interpreter.
struct ProgramShape {
  int32_t direct_column{-1};
  bool fast{false};
  // Single-`col [NOT] LIKE 'const'` programs (kLoadColumn / kLoadConstant /
  // kBinaryVarchar) skip the interpreter round-trip entirely: the pair loop
  // reads the cell and calls the segment matcher directly.  `like_pattern`
  // points into the program's constant pool, which lives as long as the
  // fusion.  NULL or collated cells fall back to the program paths, matching
  // the fast slot's refusal contract.
  int32_t like_column{-1};
  std::string_view like_pattern;
  bool like_negated{false};
};

ProgramShape ClassifyProgram(const BytecodeProgram& program) {
  ProgramShape shape;
  shape.fast = program.FastInterpretable();
  const std::vector<BytecodeInstruction>& instructions = program.Instructions();
  if (instructions.size() == 1 &&
      instructions[0].opcode == BytecodeOp::kLoadColumn) {
    shape.direct_column = instructions[0].operand;
  }
  if (instructions.size() == 3 &&
      instructions[2].opcode == BytecodeOp::kBinaryVarchar &&
      (instructions[2].binary == BinaryOperation::kLike ||
       instructions[2].binary == BinaryOperation::kNotLike) &&
      instructions[0].opcode == BytecodeOp::kLoadColumn &&
      instructions[1].opcode == BytecodeOp::kLoadConstant) {
    // The stack pops right then left, so the constant is the pattern only
    // when it was pushed second.  A collated pattern never reaches the fast
    // interpreter (ProgramIsFastInterpretable keeps such programs canonical);
    // mirror that gate so both paths agree.
    const Value& pattern = program.Constants()[instructions[1].operand];
    if (pattern.type == ValueType::kVarChar && pattern.Collation() == 0) {
      shape.like_column = static_cast<int32_t>(instructions[0].operand);
      shape.like_pattern = pattern.value.varchar_value;
      shape.like_negated = instructions[2].binary == BinaryOperation::kNotLike;
    }
  }
  return shape;
}

// Column cells referenced by a program's loads (the scratch-mask copy set).
void CollectLoadCells(const BytecodeProgram& program,
                      std::vector<slot_t>* cells) {
  for (const BytecodeInstruction& instruction : program.Instructions()) {
    if (instruction.opcode == BytecodeOp::kLoadColumn) {
      cells->push_back(instruction.operand);
    }
  }
}

StatusOr<Value> EvaluateProgramValue(const BytecodeProgram& program,
                                     const ProgramShape& shape,
                                     const Row& scratch) {
  if (shape.direct_column >= 0) {
    return scratch[static_cast<slot_t>(shape.direct_column)];
  }
  if (shape.like_column >= 0) {
    const Value& cell = scratch[static_cast<slot_t>(shape.like_column)];
    // Collated operands keep the canonical verdict (case folding and the
    // '_' collation raise live in the evaluator); NULL propagates as the
    // comparison NULL it is, so both fall through to the program.
    if (!cell.IsNull() && cell.type == ValueType::kVarChar &&
        cell.Collation() == 0 && !cell.IsCaseInsensitive()) {
      const bool matched = Like(cell.value.varchar_value, shape.like_pattern);
      return Value(shape.like_negated ? !matched : matched);
    }
  }
  if (shape.fast) {
    Value out;
    Status error{Status::kSuccess};
    switch (program.TryEvaluateRowFast(scratch, &out, &error)) {
      case BytecodeProgram::FastEval::kDone:
        return out;
      case BytecodeProgram::FastEval::kError:
        return error;
      case BytecodeProgram::FastEval::kUnsupported:
        break;
    }
  }
  return program.TryEvaluateRow(scratch);
}

// Tries to fuse the final join of a grouped statement with the grouping
// itself: matched rows (and, for LEFT joins, unmatched left rows padded with
// NULLs) feed group keys and aggregate inputs straight into per-worker
// accumulators, and the joined relation is never materialized.  The emit
// mirrors Project's generic grouped path: per group, accumulators finish
// into an AggregateResultMap and the select list evaluates against the
// representative row.
//
// An empty optional means "shape not fusable" and the caller falls back to
// the materializing join; a real Status propagates.  Gates that need the
// joined schema live here: no spilled inputs, no null-safe keys, an
// equality join, and every per-row expression (join residuals, group keys,
// aggregate children) compiled to context-free bytecode.
StatusOr<std::optional<Relation>> TryFusedGroupedJoin(
    TransactionContext& context, const Relation& left, const Relation& right,
    const std::vector<Expression>& predicates, JoinType join_type,
    const Scope* outer, const CteMap& ctes, GroupedJoinFusionSpec* fusion) {
  static const bool disabled =
      std::getenv("TINYLAMB_DISABLE_GROUPED_JOIN_FUSION") != nullptr;
  const bool debug_fusion = std::getenv("TINYLAMB_FUSION_DEBUG") != nullptr;
  const auto not_fusable = [&](const char* why) {
    if (debug_fusion) {
      std::cerr << "[fusion] not fusable: " << why << "\n";
    }
    return std::optional<Relation>{};
  };
  if (debug_fusion) {
    std::cerr << "[fusion] try: eligible="
              << (fusion != nullptr && fusion->eligible)
              << " join_type=" << static_cast<int>(join_type)
              << " left_rows=" << left.TotalRows() << "\n";
  }
  if (disabled || fusion == nullptr || !fusion->eligible ||
      fusion->aggregates.empty() ||
      (join_type != JoinType::kInner && join_type != JoinType::kLeft)) {
    return not_fusable("head");
  }
  if (left.HasSpill() || right.HasSpill() || left.TotalRows() == 0) {
    return not_fusable("spill");
  }
  const auto join_begin = std::chrono::steady_clock::now();
  Relation result(context.execution_runtime());
  result.schema = left.schema + right.schema;
  // The build-side rows the hash table is built over; the residual hoist
  // below may drop rows whose right-only residual conjuncts already fail.
  std::vector<const Row*> build_rows;
  build_rows.reserve(right.rows.size());
  for (const Row& row : right.rows) {
    build_rows.push_back(&row);
  }
  const std::vector<EqualityKey> equality_keys =
      EqualityKeys(left.schema, right.schema, predicates);
  const std::vector<Expression> residuals =
      ResidualJoinPredicates(left.schema, right.schema, predicates);
  if (equality_keys.empty()) {
    return not_fusable("no equality key");
  }
  const bool has_null_safe_key = std::ranges::any_of(
      equality_keys, [](const EqualityKey& key) { return key.null_safe; });
  if (has_null_safe_key) {
    return not_fusable("null-safe key");
  }

  const size_t left_width = left.schema.ColumnCount();
  const size_t right_width = right.schema.ColumnCount();
  const auto compile_or = [&](const Expression& expression,
                              BytecodeCompiler::Context context_kind)
      -> std::optional<BytecodeProgram> {
    if (!expression) {
      return BytecodeProgram{};
    }
    return BytecodeCompiler::Compile(expression, result.schema, context_kind);
  };
  std::vector<BytecodeProgram> residual_programs;
  for (const Expression& predicate : residuals) {
    std::optional<BytecodeProgram> program =
        compile_or(predicate, BytecodeCompiler::Context::kFilter);
    if (!program.has_value()) {
      return not_fusable("residual compile");
    }
    residual_programs.push_back(std::move(*program));
  }
  std::vector<BytecodeProgram> key_programs;
  for (const Expression& key : fusion->group_keys) {
    std::optional<BytecodeProgram> program =
        compile_or(key, BytecodeCompiler::Context::kValue);
    if (!program.has_value()) {
      return not_fusable("key compile");
    }
    key_programs.push_back(std::move(*program));
  }
  std::vector<BytecodeProgram> child_programs;
  for (size_t aggregate_index = 0; aggregate_index < fusion->aggregates.size();
       ++aggregate_index) {
    if (fusion->count_star[aggregate_index]) {
      child_programs.emplace_back();
      continue;
    }
    std::optional<BytecodeProgram> program =
        compile_or(fusion->aggregates[aggregate_index]->Child(),
                   BytecodeCompiler::Context::kValue);
    if (!program.has_value()) {
      return not_fusable("child compile");
    }
    child_programs.push_back(std::move(*program));
  }

  // Pair-loop shape classification (see ProgramShape): direct-column reads,
  // boxing-free fast programs, and canonical fallbacks.
  std::vector<ProgramShape> residual_shapes;
  residual_shapes.reserve(residual_programs.size());
  for (const BytecodeProgram& program : residual_programs) {
    residual_shapes.push_back(ClassifyProgram(program));
  }
  // Build-side residual hoist: a residual conjunct that loads only
  // right-side cells (Q13's `o_comment NOT LIKE ...` under a customer
  // LEFT JOIN orders) does not depend on the probe row, so it can be
  // evaluated once per build row -- inside the worker-parallel relation
  // filter -- instead of once per candidate pair in the fused probe loop.
  // Soundness: removing a right row whose right-only conjunct fails is
  // equivalent to leaving it in and rejecting every pair it forms; for
  // kInner those pairs vanish either way, and for kLeft the matched-false
  // contract null-extends the probe row exactly when all of its bucket
  // entries are gone.  The throw-safe gate keeps the hoisted evaluation
  // from surfacing errors the per-pair path would only reach on
  // hash-matched rows.
  static const bool hoist_disabled =
      std::getenv("TINYLAMB_DISABLE_BUILD_RESIDUAL_HOIST") != nullptr;
  if (!hoist_disabled && !residual_programs.empty() && right.TotalRows() > 0) {
    // Evaluate each right-only residual once per build row (Q13's
    // `o_comment NOT LIKE ...` is row-invariant on the probe side) and drop
    // failing rows from the build pointer list -- no relation copy, and the
    // pair loop loses the per-pair residual dispatch.  Soundness: a dropped
    // right row is one whose every candidate pair would be residual-
    // rejected; kInner loses nothing and kLeft's matched-false contract
    // null-extends the probe row exactly the same.  Any evaluation error
    // abandons the hoist (the per-pair path keeps its exact error behavior).
    std::vector<size_t> hoisted_indices;
    for (size_t i = 0; i < residual_programs.size(); ++i) {
      if (!ExpressionCannotThrowForPlanner(residuals[i])) {
        continue;
      }
      std::vector<slot_t> loads;
      CollectLoadCells(residual_programs[i], &loads);
      if (!loads.empty() &&
          std::all_of(loads.begin(), loads.end(),
                      [&](slot_t cell) { return cell >= left_width; })) {
        hoisted_indices.push_back(i);
      }
    }
    if (!hoisted_indices.empty()) {
      std::vector<slot_t> hoist_cells;
      std::vector<ProgramShape> hoist_shapes;
      for (const size_t i : hoisted_indices) {
        CollectLoadCells(residual_programs[i], &hoist_cells);
        hoist_shapes.push_back(residual_shapes[i]);
      }
      Row hoist_scratch;
      hoist_scratch.values_.resize(left_width + right_width);
      bool hoist_error = false;
      std::vector<bool> keep(right.rows.size(), true);
      for (size_t ri = 0; ri < right.rows.size(); ++ri) {
        const Row& row = right.rows[ri];
        for (const slot_t cell : hoist_cells) {
          hoist_scratch.values_[cell] = row.values_[cell - left_width];
        }
        for (size_t i = 0; i < hoisted_indices.size(); ++i) {
          StatusOr<Value> verdict =
              EvaluateProgramValue(residual_programs[hoisted_indices[i]],
                                   hoist_shapes[i], hoist_scratch);
          if (!verdict.HasValue()) {
            hoist_error = true;
            break;
          }
          if (!Truthy(verdict.Value())) {
            keep[ri] = false;
            break;
          }
        }
        if (hoist_error) {
          break;
        }
      }
      if (!hoist_error) {
        std::vector<const Row*> filtered;
        filtered.reserve(build_rows.size());
        for (size_t ri = 0; ri < build_rows.size(); ++ri) {
          if (keep[ri]) {
            filtered.push_back(build_rows[ri]);
          }
        }
        build_rows = std::move(filtered);
        // Drop the hoisted programs (and their shapes), highest index first.
        for (size_t i = hoisted_indices.size(); i-- > 0;) {
          residual_programs.erase(
              residual_programs.begin() +
              static_cast<std::ptrdiff_t>(hoisted_indices[i]));
          residual_shapes.erase(
              residual_shapes.begin() +
              static_cast<std::ptrdiff_t>(hoisted_indices[i]));
        }
      }
    }
  }
  std::vector<ProgramShape> key_shapes;
  key_shapes.reserve(key_programs.size());
  for (const BytecodeProgram& program : key_programs) {
    key_shapes.push_back(ClassifyProgram(program));
  }
  std::vector<ProgramShape> child_shapes;
  child_shapes.reserve(child_programs.size());
  for (const BytecodeProgram& program : child_programs) {
    child_shapes.push_back(ClassifyProgram(program));
  }

  // Count-only fusions (Q13: COUNT(o_orderkey) per customer with every other
  // aggregate absent) accumulate a raw per-group integer instead of driving
  // the accumulator machinery 915K times: no per-pair Value boxing, no
  // per-group state construction, and an integer-add merge.  Two shapes
  // qualify, and they differ on unmatched LEFT rows: COUNT(*) counts the
  // null-extended row (+1), while COUNT(not-null column) counts nothing
  // (+0).  A not-null column child (bare load of a kNotNull/kPrimaryKey
  // column) can never contribute a NULL, so its COUNT equals COUNT(*) over
  // matched pairs.  Mixed shapes stay on the generic path.
  bool count_star_only = !fusion->aggregates.empty();
  for (const bool count_star : fusion->count_star) {
    count_star_only = count_star_only && count_star;
  }
  bool count_nonnull_only = false;
  {
    bool all_count_nonnull = !fusion->aggregates.empty();
    bool any_nonnull = false;
    for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
      if (fusion->count_star[i]) {
        all_count_nonnull = false;
        continue;
      }
      const AggregateExpression* aggregate = fusion->aggregates[i];
      const bool plain_column_child =
          child_shapes[i].direct_column >= 0 &&
          aggregate->Having() == AggregateHavingModifier::kNone &&
          aggregate->InnerOrderBy().empty() && !aggregate->SecondaryArg() &&
          aggregate->ExtraArgs().empty();
      bool not_null_column = false;
      if (plain_column_child) {
        const Column& column = result.schema.GetColumn(
            static_cast<size_t>(child_shapes[i].direct_column));
        const Constraint::ConstraintType ctype = column.GetConstraint().ctype;
        not_null_column =
            ctype == Constraint::kNotNull || ctype == Constraint::kPrimaryKey;
      }
      if (!not_null_column) {
        all_count_nonnull = false;
        break;
      }
      any_nonnull = true;
    }
    count_nonnull_only = all_count_nonnull && any_nonnull;
  }
  static const bool count_fusion_disabled =
      std::getenv("TINYLAMB_DISABLE_COUNT_STAR_FUSION") != nullptr;
  const bool raw_count_fusion =
      !count_fusion_disabled && (count_star_only || count_nonnull_only);
  // Scratch-mask copy: the pair loop fills only the cells the residual, key,
  // and child programs actually load (group representatives — which the final
  // select list reads in full — are re-filled completely on the rare
  // new-group path).  Empty when the programs reference every column anyway.
  std::vector<slot_t> scratch_cells;
  for (const std::vector<BytecodeProgram>* programs :
       {&residual_programs, &key_programs, &child_programs}) {
    for (const BytecodeProgram& program : *programs) {
      CollectLoadCells(program, &scratch_cells);
    }
  }
  std::sort(scratch_cells.begin(), scratch_cells.end());
  scratch_cells.erase(std::unique(scratch_cells.begin(), scratch_cells.end()),
                      scratch_cells.end());
  const bool mask_scratch = scratch_cells.size() < left_width + right_width;

  std::vector<slot_t> left_columns;
  std::vector<slot_t> right_columns;
  left_columns.reserve(equality_keys.size());
  right_columns.reserve(equality_keys.size());
  for (const EqualityKey& key : equality_keys) {
    left_columns.push_back(static_cast<slot_t>(key.left));
    right_columns.push_back(static_cast<slot_t>(key.right));
  }
  const bool integer_key = SingleIntegerJoinKey(left.schema, left_columns) &&
                           SingleIntegerJoinKey(right.schema, right_columns);
  const bool composite_int_key =
      !integer_key && CompositeIntJoinKey(left.schema, left_columns,
                                          right.schema, right_columns);
  // Biased keys hash with their constants folded in; the fused residual
  // programs still verify every matched pair against the original
  // predicates, so the bias only prunes buckets.
  std::vector<int64_t> left_key_offsets;
  std::vector<int64_t> right_key_offsets;
  left_key_offsets.reserve(equality_keys.size());
  right_key_offsets.reserve(equality_keys.size());
  for (const EqualityKey& key : equality_keys) {
    left_key_offsets.push_back(key.left_offset);
    right_key_offsets.push_back(key.right_offset);
  }

  const size_t total_left = left.TotalRows();
  // Single-INT64/DATE group-key fast path: when the fusion groups by one
  // plain column of that domain, the pair loop probes a flat int table with
  // the raw cell payload (NULL keys keep the generic map, which groups
  // NULLs).  Q13's customer-key grouping pays the generic vector hash once
  // per matched pair otherwise.
  bool int_group_key = false;
  slot_t int_group_slot = 0;
  ValueType int_group_type = ValueType::kNull;
  if (key_programs.size() == 1 && key_shapes[0].direct_column >= 0) {
    const Column& column = result.schema.GetColumn(
        static_cast<size_t>(key_shapes[0].direct_column));
    const ValueType type = column.Type();
    if ((type == ValueType::kInt64 && !column.IsUnsigned()) ||
        type == ValueType::kDate) {
      int_group_key = true;
      int_group_slot = static_cast<slot_t>(key_shapes[0].direct_column);
      int_group_type = type;
    }
  }
  // Classification shared by every per-group accumulator construction below;
  // identical for all groups of one aggregate.
  std::vector<AggregateAccumulator::Shape> aggregate_shapes;
  aggregate_shapes.reserve(fusion->aggregates.size());
  for (const AggregateExpression* aggregate : fusion->aggregates) {
    aggregate_shapes.push_back(
        AggregateAccumulator::AnalyzeShape(aggregate, nullptr));
  }
  // Bulk raw-count: once every residual is hoisted off the pair path and the
  // group key is one probe-side INT64/DATE column, a count-only fusion needs
  // no per-pair work at all -- each surviving bucket entry contributes
  // exactly +1 (COUNT(*) and COUNT(not-null column) coincide inside a
  // bucket) -- so the probe collapses to one equal_range, one group-map
  // probe and one integer add per outer row.  The per-pair scratch fills,
  // flat-map probes and build-row dereferences all disappear (Q13's customer
  // LEFT JOIN orders COUNT(o_orderkey) walked ~915K pairs at ~330 ns each).
  static const bool bulk_count_disabled =
      std::getenv("TINYLAMB_DISABLE_BULK_COUNT") != nullptr;
  const bool bulk_raw_count = !bulk_count_disabled && raw_count_fusion &&
                              residual_programs.empty() && int_group_key &&
                              int_group_slot < left_width;
  const auto accumulate_into = [&](FusedWorkerLocal& local, Row& scratch,
                                   const Row* left_row, const Row* right_row,
                                   bool masked) -> Status {
    const Value* fast_key = nullptr;
    if (int_group_key) {
      const Value& cell = scratch[int_group_slot];
      if (!cell.IsNull() && cell.type == int_group_type && !cell.IsUnsigned()) {
        fast_key = &cell;
      }
    }
    size_t group_index = 0;
    size_t* fast_slot = nullptr;
    bool new_group = false;
    if (fast_key != nullptr) {
      bool inserted = false;
      fast_slot =
          local.int_offsets.TryEmplace(fast_key->value.int_value, &inserted);
      if (!inserted) {
        group_index = *fast_slot;
      } else {
        // Only the insert pays the canonical-key construction, and the
        // stored key must equal what the generic map would hold so the
        // post-join merge dedups groups across workers.
        local.key_scratch.assign(1, *fast_key);
        new_group = true;
      }
    } else {
      // Probe with the reusable key scratch: the transparent group-key hash
      // takes a plain value vector, so only an actual group insert pays for
      // a Row construction (vector allocation + Value copies) per probe row.
      local.key_scratch.clear();
      for (size_t key_index = 0; key_index < key_programs.size(); ++key_index) {
        ASSIGN_OR_RETURN(Value, key_value,
                         EvaluateProgramValue(key_programs[key_index],
                                              key_shapes[key_index], scratch));
        local.key_scratch.push_back(CanonicalDistinctValue(key_value));
      }
      if (auto found = local.offsets.find(local.key_scratch);
          found != local.offsets.end()) {
        group_index = found->second;
      } else {
        new_group = true;
      }
    }
    if (new_group) {
      // New group: index into `groups` first (the vector may reallocate as
      // it grows, so capture the slot up front), then insert the map entry.
      // A masked pair fill leaves every unreferenced cell stale, and the
      // representative is read in full by the select list — restore the
      // complete left++right image first (rare: once per group).
      if (masked && left_row != nullptr) {
        for (size_t i = 0; i < left_width; ++i) {
          scratch.values_[i] = left_row->values_[i];
        }
        for (size_t i = 0; i < right_width; ++i) {
          scratch.values_[left_width + i] =
              right_row != nullptr ? right_row->values_[i] : Value();
        }
      }
      group_index = local.groups.size();
      FusedGroup group;
      if (!raw_count_fusion) {
        group.state_offset = local.states.size();
        for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
          local.states.emplace_back(fusion->aggregates[i], aggregate_shapes[i]);
        }
      }
      group.key = Row(local.key_scratch);
      group.representative = scratch;
      local.groups.push_back(std::move(group));
      if (fast_slot != nullptr) {
        *fast_slot = group_index;
      } else {
        local.offsets.emplace(Row(local.key_scratch), group_index);
      }
    }
    FusedGroup& group = local.groups[group_index];
    if (count_star_only) {
      group.raw_count += 1;
      return Status::kSuccess;
    }
    if (count_nonnull_only) {
      // COUNT(not-null column): unmatched LEFT rows contribute nothing.
      if (right_row != nullptr) {
        group.raw_count += 1;
      }
      return Status::kSuccess;
    }
    for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
      AggregateAccumulator& accumulator = local.states[group.state_offset + i];
      if (fusion->count_star[i]) {
        RETURN_IF_FAIL(accumulator.Add(Value(1)));
        continue;
      }
      ASSIGN_OR_RETURN(
          Value, child,
          EvaluateProgramValue(child_programs[i], child_shapes[i], scratch));
      RETURN_IF_FAIL(accumulator.Add(child));
    }
    return Status::kSuccess;
  };
  // Fills the pair scratch from the two sides.  `right_row == nullptr` pads
  // the right half with NULLs (unmatched left rows).  Masked fills touch
  // only the cells the compiled programs load.
  const auto fill_scratch = [&](Row& scratch, const Row& left_row,
                                const Row* right_row) {
    if (mask_scratch) {
      for (const slot_t cell : scratch_cells) {
        scratch.values_[cell] =
            cell < left_width
                ? left_row.values_[cell]
                : (right_row != nullptr ? right_row->values_[cell - left_width]
                                        : Value());
      }
      return;
    }
    for (size_t i = 0; i < left_width; ++i) {
      scratch.values_[i] = left_row.values_[i];
    }
    for (size_t i = 0; i < right_width; ++i) {
      scratch.values_[left_width + i] =
          right_row != nullptr ? right_row->values_[i] : Value();
    }
  };
  // Returns true when the pair passed every residual and was accumulated.
  // A residual-rejected pair is NOT a match: LEFT semantics must still
  // null-extend the outer row when every bucket pair for it was rejected.
  const auto process_pair = [&](FusedWorkerLocal& local, const Row& left_row,
                                const Row& right_row,
                                Row& scratch) -> StatusOr<bool> {
    fill_scratch(scratch, left_row, &right_row);
    for (size_t i = 0; i < residual_programs.size(); ++i) {
      ASSIGN_OR_RETURN(Value, verdict,
                       EvaluateProgramValue(residual_programs[i],
                                            residual_shapes[i], scratch));
      if (!Truthy(verdict)) {
        return false;
      }
    }
    RETURN_IF_FAIL(
        accumulate_into(local, scratch, &left_row, &right_row, mask_scratch));
    return true;
  };
  const auto unmatched_left = [&](FusedWorkerLocal& local, Row& scratch,
                                  const Row& left_row) -> Status {
    fill_scratch(scratch, left_row, nullptr);
    return accumulate_into(local, scratch, &left_row, nullptr, mask_scratch);
  };

  const size_t workers =
      std::max<size_t>(1, std::min<size_t>({kParallelProbeMaxWorkers,
                                            std::thread::hardware_concurrency(),
                                            ((total_left / 16384)) + 1}));
  std::vector<FusedWorkerLocal> locals(workers);
  std::vector<Row> scratch_storage(locals.size());
  for (Row& scratch : scratch_storage) {
    scratch.values_.resize(left_width + right_width);
  }
  std::atomic<size_t> next_chunk{0};
  const size_t chunk_rows = 8192;
  const size_t chunk_count = (total_left + chunk_rows - 1) / chunk_rows;

  // Build the hash table over the right side once, before the workers spawn
  // (the build side is read-only during the probe).
  std::optional<FlatJoinBuckets<int64_t>> int_buckets;
  std::optional<FlatJoinBuckets<CompositeIntKey, CompositeIntKeyHash>>
      composite_buckets;
  std::optional<FlatJoinBuckets<std::string>> key_buckets;
  std::optional<FlatIntKeyMap<BulkCountValue>> bulk_counts;
  const auto build_buckets = [&]() {
    if (bulk_raw_count) {
      // The duplicate count and the bucket-head row stand in for the whole
      // bucket chain: the probe never touches an Entry or a build Row.
      bulk_counts.emplace();
      for (const Row* row_ptr : build_rows) {
        const Row& row = *row_ptr;
        if (HasNullKey(row, right_columns)) {
          continue;
        }
        const int64_t build_key = AddJoinKeyBias(
            IntegerJoinKey(row, right_columns[0]), right_key_offsets[0]);
        bool inserted = false;
        BulkCountValue* slot = bulk_counts->TryEmplace(build_key, &inserted);
        slot->count = inserted ? 1 : slot->count + 1;
        slot->row = row_ptr;
      }
      return;
    }
    if (integer_key) {
      int_buckets.emplace();
      int_buckets->reserve(build_rows.size());
      for (const Row* row_ptr : build_rows) {
        const Row& row = *row_ptr;
        if (HasNullKey(row, right_columns)) {
          continue;
        }
        int_buckets->emplace(
            AddJoinKeyBias(IntegerJoinKey(row, right_columns[0]),
                           right_key_offsets[0]),
            &row);
      }
    } else if (composite_int_key) {
      composite_buckets.emplace();
      composite_buckets->reserve(build_rows.size());
      for (const Row* row_ptr : build_rows) {
        const Row& row = *row_ptr;
        if (HasNullKey(row, right_columns)) {
          continue;
        }
        composite_buckets->emplace(
            EncodeCompositeIntKey(row, right_columns, &right_key_offsets),
            &row);
      }
    } else {
      key_buckets.emplace();
      key_buckets->reserve(build_rows.size());
      for (const Row* row_ptr : build_rows) {
        const Row& row = *row_ptr;
        if (HasNullKey(row, right_columns)) {
          continue;
        }
        key_buckets->emplace(
            EncodeJoinKey(row, right_columns, &right_key_offsets), &row);
      }
    }
  };
  build_buckets();

  auto run_probe = [&](FusedWorkerLocal& local, Row& scratch) -> Status {
    if (integer_key) {
      const FlatJoinBuckets<int64_t>& buckets = *int_buckets;
      while (true) {
        const size_t chunk = next_chunk.fetch_add(1);
        if (chunk >= chunk_count) {
          return Status::kSuccess;
        }
        const size_t begin = chunk * chunk_rows;
        const size_t end = std::min(total_left, begin + chunk_rows);
        for (size_t ri = begin; ri < end; ++ri) {
          const Row& left_row = left.rows[ri];
          if (HasNullKey(left_row, left_columns)) {
            if (join_type == JoinType::kLeft) {
              RETURN_IF_FAIL(unmatched_left(local, scratch, left_row));
            }
            continue;
          }
          const int64_t key = AddJoinKeyBias(
              IntegerJoinKey(left_row, left_columns[0]), left_key_offsets[0]);
          if (bulk_raw_count) {
            // One open-addressing probe answers the whole join step: the
            // duplicate count stands in for the bucket walk (whose Entry
            // chain and per-pair build-row dereferences dominated this
            // loop), and the group key is read straight off the outer row.
            const BulkCountValue* bulk = bulk_counts->Find(key);
            if (bulk == nullptr) {
              if (join_type == JoinType::kLeft) {
                RETURN_IF_FAIL(unmatched_left(local, scratch, left_row));
              }
              continue;
            }
            local.comparisons += static_cast<size_t>(bulk->count);
            const Value& bulk_cell = left_row.values_[int_group_slot];
            size_t* bulk_slot = nullptr;
            bool new_group = false;
            size_t group_index = 0;
            if (!bulk_cell.IsNull() && bulk_cell.type == int_group_type &&
                !bulk_cell.IsUnsigned()) {
              bool inserted = false;
              bulk_slot = local.int_offsets.TryEmplace(
                  bulk_cell.value.int_value, &inserted);
              if (!inserted) {
                group_index = *bulk_slot;
              } else {
                local.key_scratch.assign(1, bulk_cell);
                new_group = true;
              }
            } else {
              local.key_scratch.clear();
              local.key_scratch.push_back(CanonicalDistinctValue(bulk_cell));
              if (auto found = local.offsets.find(local.key_scratch);
                  found != local.offsets.end()) {
                group_index = found->second;
              } else {
                new_group = true;
              }
            }
            if (new_group) {
              // The representative is the complete left++right image of the
              // bucket-head pair -- the same row the per-pair path captures
              // (the map stores the most recently inserted row per key,
              // which is where the bucket chain's head lands).
              for (size_t i = 0; i < left_width; ++i) {
                scratch.values_[i] = left_row.values_[i];
              }
              for (size_t i = 0; i < right_width; ++i) {
                scratch.values_[left_width + i] = bulk->row->values_[i];
              }
              group_index = local.groups.size();
              FusedGroup group;
              group.key = Row(local.key_scratch);
              group.representative = scratch;
              local.groups.push_back(std::move(group));
              if (bulk_slot != nullptr) {
                *bulk_slot = group_index;
              } else {
                local.offsets.emplace(Row(local.key_scratch), group_index);
              }
            }
            local.groups[group_index].raw_count += bulk->count;
            continue;
          }
          bool matched = false;
          const auto [first, last] = buckets.equal_range(key);
          for (auto iter = first; iter != last; ++iter) {
            ++local.comparisons;
            ASSIGN_OR_RETURN(
                bool, accumulated,
                process_pair(local, left_row, *iter->second, scratch));
            matched = matched || accumulated;
          }
          if (!matched && join_type == JoinType::kLeft) {
            RETURN_IF_FAIL(unmatched_left(local, scratch, left_row));
          }
        }
      }
    }
    if (composite_int_key) {
      const FlatJoinBuckets<CompositeIntKey, CompositeIntKeyHash>& buckets =
          *composite_buckets;
      while (true) {
        const size_t chunk = next_chunk.fetch_add(1);
        if (chunk >= chunk_count) {
          return Status::kSuccess;
        }
        const size_t begin = chunk * chunk_rows;
        const size_t end = std::min(total_left, begin + chunk_rows);
        for (size_t ri = begin; ri < end; ++ri) {
          const Row& left_row = left.rows[ri];
          if (HasNullKey(left_row, left_columns)) {
            if (join_type == JoinType::kLeft) {
              RETURN_IF_FAIL(unmatched_left(local, scratch, left_row));
            }
            continue;
          }
          const CompositeIntKey key =
              EncodeCompositeIntKey(left_row, left_columns, &left_key_offsets);
          bool matched = false;
          const auto [first, last] = buckets.equal_range(key);
          for (auto iter = first; iter != last; ++iter) {
            ++local.comparisons;
            ASSIGN_OR_RETURN(
                bool, accumulated,
                process_pair(local, left_row, *iter->second, scratch));
            matched = matched || accumulated;
          }
          if (!matched && join_type == JoinType::kLeft) {
            RETURN_IF_FAIL(unmatched_left(local, scratch, left_row));
          }
        }
      }
    }
    const FlatJoinBuckets<std::string>& buckets = *key_buckets;
    while (true) {
      const size_t chunk = next_chunk.fetch_add(1);
      if (chunk >= chunk_count) {
        return Status::kSuccess;
      }
      const size_t begin = chunk * chunk_rows;
      const size_t end = std::min(total_left, begin + chunk_rows);
      for (size_t ri = begin; ri < end; ++ri) {
        const Row& left_row = left.rows[ri];
        if (HasNullKey(left_row, left_columns)) {
          if (join_type == JoinType::kLeft) {
            RETURN_IF_FAIL(unmatched_left(local, scratch, left_row));
          }
          continue;
        }
        const std::string key =
            EncodeJoinKey(left_row, left_columns, &left_key_offsets);
        bool matched = false;
        const auto [first, last] = buckets.equal_range(key);
        for (auto iter = first; iter != last; ++iter) {
          ++local.comparisons;
          ASSIGN_OR_RETURN(
              bool, accumulated,
              process_pair(local, left_row, *iter->second, scratch));
          matched = matched || accumulated;
        }
        if (!matched && join_type == JoinType::kLeft) {
          RETURN_IF_FAIL(unmatched_left(local, scratch, left_row));
        }
      }
    }
  };
  {
    std::vector<std::jthread> threads;
    threads.reserve(locals.size());
    for (size_t w = 0; w < locals.size(); ++w) {
      threads.emplace_back([&, w] {
        Status status = run_probe(locals[w], scratch_storage[w]);
        if (status != Status::kSuccess) {
          locals[w].error = status;
        }
      });
    }
  }
  for (const FusedWorkerLocal& local : locals) {
    RETURN_IF_FAIL(local.error);
  }
  result.hash_joins = left.hash_joins + right.hash_joins + 1;
  result.in_memory_hash_joins =
      left.in_memory_hash_joins + right.in_memory_hash_joins + 1;
  result.nested_loop_joins = left.nested_loop_joins + right.nested_loop_joins;
  result.hybrid_hash_joins = left.hybrid_hash_joins + right.hybrid_hash_joins;
  result.join_comparisons = left.join_comparisons + right.join_comparisons;
  for (const FusedWorkerLocal& local : locals) {
    result.join_comparisons += local.comparisons;
  }

  // Merge partials in worker order.  Groups are visited through each
  // worker's insertion-ordered vector (not its hash map) so the emission
  // order is deterministic; a group first seen by an earlier worker merges
  // later partials into its adopted state.
  GroupKeyMap offsets;
  std::vector<FusedGroup> groups;
  std::deque<AggregateAccumulator> states;
  for (FusedWorkerLocal& local : locals) {
    for (FusedGroup& group : local.groups) {
      auto [iter, inserted] = offsets.emplace(group.key, groups.size());
      if (inserted) {
        if (!raw_count_fusion) {
          const size_t local_offset = group.state_offset;
          for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
            states.push_back(std::move(local.states[local_offset + i]));
          }
          group.state_offset = states.size() - fusion->aggregates.size();
        }
        groups.push_back(std::move(group));
      } else if (raw_count_fusion) {
        groups[iter->second].raw_count += group.raw_count;
      } else {
        FusedGroup& destination = groups[iter->second];
        for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
          AggregateAccumulator& target = states[destination.state_offset + i];
          RETURN_IF_FAIL(target.Merge(local.states[group.state_offset + i]));
        }
      }
    }
    local.groups.clear();
    local.states.clear();
  }
  // A keyless (global) aggregate over an empty join output still emits one
  // row: accumulators finish to their empty-input values (SUM -> NULL,
  // COUNT -> 0), exactly like the scalar AggregateExecutor.  Without this the
  // fused shape would drop the row entirely.
  if (fusion->group_keys.empty() && groups.empty()) {
    FusedGroup group;
    if (!raw_count_fusion) {
      group.state_offset = 0;
      for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
        states.emplace_back(fusion->aggregates[i], aggregate_shapes[i]);
      }
    }
    group.representative =
        Row(std::vector<Value>(result.schema.ColumnCount(), Value()));
    groups.push_back(std::move(group));
  }

  // Emit: finish the accumulators into an AggregateResultMap keyed by the
  // aggregate node, then evaluate the select list against the representative
  // row — the same contract as Project's generic grouped emit.
  std::vector<Column> output_columns;
  output_columns.reserve(fusion->select_items.size());
  for (const std::string& name : fusion->select_names) {
    output_columns.emplace_back(name, ValueType::kNull);
  }
  for (const FusedGroup& group : groups) {
    AggregateResultMap aggregate_results;
    aggregate_results.reserve(fusion->aggregates.size());
    if (raw_count_fusion) {
      for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
        aggregate_results.emplace(fusion->aggregates[i],
                                  Value(group.raw_count));
      }
    } else {
      for (size_t i = 0; i < fusion->aggregates.size(); ++i) {
        AggregateAccumulator& accumulator = states[group.state_offset + i];
        ASSIGN_OR_RETURN(Value, finished, accumulator.TryFinish());
        aggregate_results.emplace(accumulator.expression, std::move(finished));
      }
    }
    Scope scope{
        .row = &group.representative, .schema = &result.schema, .outer = outer};
    Row output_row;
    output_row.values_.reserve(fusion->select_items.size());
    for (const Expression& item : fusion->select_items) {
      ASSIGN_OR_RETURN(
          Value, value,
          (TryEvaluate(item, scope, &aggregate_results, context, ctes)));
      output_row.values_.push_back(std::move(value));
    }
    result.AddRow(std::move(output_row));
  }
  if (!result.rows.empty()) {
    for (size_t i = 0; i < output_columns.size(); ++i) {
      output_columns[i] =
          Column(output_columns[i].Name(), ValueTypeOf(result.rows[0][i]));
      output_columns[i].SetUnsigned(result.rows[0][i].IsUnsigned());
    }
  }
  result.schema = Schema("", std::move(output_columns));
  result.FinishSpill();
  result.peak_intermediate_rows =
      std::max(result.peak_intermediate_rows, result.TotalRows());
  if (context.execution_runtime() != nullptr) {
    ExecutionRuntime& runtime = *context.execution_runtime();
    runtime.join_ms += ElapsedMs(join_begin);
    ++runtime.grouped_join_fusions;
  }
  if (debug_fusion) {
    std::cerr << "[fusion] fused groups=" << groups.size() << "\n";
  }
  return std::optional<Relation>(std::move(result));
}

}  // namespace

bool IsSubset(const std::unordered_set<size_t>& values,
              const std::unordered_set<size_t>& superset) {
  return std::ranges::all_of(
      values, [&](size_t value) { return superset.contains(value); });
}

std::vector<SelectSource> ReduceOuterJoinsToInner(
    const SelectStatement& statement, const std::vector<Relation>& schemas,
    bool* reduced_all) {
  std::vector<SelectSource> reduced = statement.Sources();
  *reduced_all = false;
  if (reduced.size() != schemas.size()) {
    return reduced;
  }
  const Expression& where = statement.WhereClause();
  if (!where) {
    return reduced;
  }
  const std::vector<PredicateInfo> predicates =
      AnalyzePredicates(where, schemas);
  const bool resolved =
      std::ranges::all_of(predicates, [](const PredicateInfo& predicate) {
        return predicate.resolved && !predicate.contains_query;
      });
  if (!resolved) {
    return reduced;
  }
  const auto qualifier = [&](size_t index) {
    const SelectSource& source = reduced[index];
    return source.alias.empty() ? source.table : source.alias;
  };
  bool any_outer = false;
  bool all_reducible = true;
  for (size_t index = 1; index < reduced.size(); ++index) {
    const JoinType type = reduced[index].join_type;
    if (type != JoinType::kLeft && type != JoinType::kRight &&
        type != JoinType::kFull) {
      continue;
    }
    any_outer = true;
    if (reduced[index].query || reduced[index].unnest ||
        reduced[index].from_nested_join) {
      all_reducible = false;
      continue;
    }
    std::unordered_set<std::string> prefix;
    for (size_t source = 0; source < index; ++source) {
      const std::string name = qualifier(source);
      if (name.empty()) {
        all_reducible = false;
      } else {
        prefix.insert(name);
      }
    }
    std::unordered_set<std::string> right;
    const std::string right_name = qualifier(index);
    if (right_name.empty()) {
      all_reducible = false;
    } else {
      right.insert(right_name);
    }
    bool rejects = false;
    if (type == JoinType::kLeft) {
      rejects = ExpressionRejectsNullsOnRelations(where, right);
    } else if (type == JoinType::kRight) {
      rejects = ExpressionRejectsNullsOnRelations(where, prefix);
    } else {
      rejects = ExpressionRejectsNullsOnRelations(where, prefix) &&
                ExpressionRejectsNullsOnRelations(where, right);
    }
    if (rejects) {
      reduced[index].join_type = JoinType::kInner;
    } else {
      all_reducible = false;
    }
  }
  *reduced_all = any_outer && all_reducible;
  return reduced;
}

// Recursively gathers every column referenced by an expression, descending
// into subqueries so UNNEST arguments like ARRAY(SELECT ... a ...) report
// their correlated references.
namespace {

void CollectColumnsRecursive(  // NOLINT(misc-no-recursion)
    const Expression& expression, std::unordered_set<ColumnName>* columns) {
  if (!expression) {
    return;
  }
  if (expression->Type() == TypeTag::kQueryExp) {
    const QueryExpression& query = expression->AsQueryExpression();
    CollectStatementColumns(*query.Query(), columns);
    CollectColumnsRecursive(query.Test(), columns);
    return;
  }
  std::unordered_set<ColumnName> touched = expression->TouchedColumns();
  columns->merge(touched);
  for (const Expression& child : ExpressionChildren(expression)) {
    CollectColumnsRecursive(child, columns);
  }
}

}  // namespace

// Expands a lateral (correlated) UNNEST source against the relation
// accumulated from the preceding FROM items: for every prefix row the array
// expression is evaluated with that row in scope and each element becomes an
// output row joined with the prefix row.
// GCC 15 emits a spurious -Wfree-nonheap-object inside the inlined
// std::vector deallocation here even though every container lifetime is
// standard; suppress it just for this function.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wfree-nonheap-object"
namespace {

StatusOr<Relation> LateralExpandRelation(TransactionContext& context,
                                         const SelectSource& source,
                                         Relation& prefix, const Scope* outer,
                                         const CteMap& ctes) {
  Relation output(context.execution_runtime());
  const std::string qualifier = source.alias.empty() ? "unnest" : source.alias;
  auto element_schema_of = [&](const Value& array_val) {
    Relation elements = UnnestValueToRelation(source, array_val);
    if (!qualifier.empty()) {
      elements.schema = QualifySchema(elements.schema, qualifier);
    }
    return elements;
  };
  const Expression condition =
      source.join_condition ? source.join_condition : nullptr;
  // USING(name) equalities bind positionally: both operands share one bare
  // name, so combined-scope evaluation would be ambiguous (and after
  // coalescing, trivially true).  Each such conjunct resolves to a
  // prefix-side and an element-side column up front and compares them
  // directly; remaining conjuncts keep combined-scope evaluation.
  struct UsingEquality {
    size_t prefix_column;
    size_t element_column;
  };
  std::vector<UsingEquality> using_equalities;
  std::vector<Expression> residual_condition =
      condition ? SplitConjuncts(condition) : std::vector<Expression>{};
  if (!source.using_columns.empty() && condition) {
    auto declared = [&](const ColumnName& name) {
      if (!name.schema.empty()) {
        return false;
      }
      return std::ranges::any_of(
          source.using_columns, [&](const std::string& candidate) {
            return candidate.size() == name.name.size() &&
                   std::equal(
                       candidate.begin(), candidate.end(), name.name.begin(),
                       [](char lhs, char rhs) {
                         return std::tolower(static_cast<unsigned char>(lhs)) ==
                                std::tolower(static_cast<unsigned char>(rhs));
                       });
          });
    };
    // The element relation's column names are stable across expansions:
    // [alias] plus the offset alias when declared.
    std::vector<Column> probe{Column(
        source.alias.empty() ? "unnest" : source.alias, ValueType::kNull)};
    if (!source.offset_alias.empty()) {
      probe.emplace_back(source.offset_alias, ValueType::kInt64);
    }
    const Schema element_names("", std::move(probe));
    for (size_t conjunct_index = 0;
         conjunct_index < residual_condition.size();) {
      const Expression& conjunct = residual_condition[conjunct_index];
      bool handled = false;
      if (IsColumnEqualityPredicate(conjunct)) {
        const BinaryExpression& binary = conjunct->AsBinaryExpression();
        const ColumnName& ln = binary.Left()->AsColumnValue().GetColumnName();
        const ColumnName& rn = binary.Right()->AsColumnValue().GetColumnName();
        if (declared(ln) && declared(rn)) {
          // Hoisted out of the range-for: iterating a temporary vector here
          // trips a spurious -Wfree-nonheap-object diagnostic in GCC 15.
          const std::vector<std::pair<ColumnName, ColumnName>> both_orders{
              {ln, rn}, {rn, ln}};
          for (const auto& [outer_name, inner_name] : both_orders) {
            const auto prefix_offset =
                LocalColumnOffset(prefix.schema, outer_name);
            const auto element_offset =
                LocalColumnOffset(element_names, inner_name);
            if (prefix_offset && element_offset) {
              using_equalities.push_back({*prefix_offset, *element_offset});
              handled = true;
              break;
            }
          }
        }
      }
      if (handled) {
        residual_condition.erase(residual_condition.begin() +
                                 static_cast<ptrdiff_t>(conjunct_index));
      } else {
        ++conjunct_index;
      }
    }
  }
  const bool want_left_nulls = source.join_type == JoinType::kLeft ||
                               source.join_type == JoinType::kFull;
  const bool want_right_nulls = source.join_type == JoinType::kRight ||
                                source.join_type == JoinType::kFull;
  if (!source.using_columns.empty()) {
    output.using_columns =
        std::make_shared<const std::vector<std::string>>(source.using_columns);
  }
  const bool left_outer = want_left_nulls;
  bool any_prefix_row = false;
  bool schema_initialized = false;
  prefix.FinishSpill();
  Status elements_status = Status::kSuccess;
  prefix.ForEachRow([&](const Row& row) -> void {
    any_prefix_row = true;
    Scope scope{.row = &row, .schema = &prefix.schema, .outer = outer};
    Relation elements(context.execution_runtime());
    if (source.unnest) {
      StatusOr<Value> array_or =
          TryEvaluate(source.unnest, scope, nullptr, context, ctes);
      if (!array_or.HasValue()) {
        if (elements_status == Status::kSuccess) {
          elements_status = array_or.GetStatus();
        }
        return;
      }
      const Value array_val = array_or.MoveValue();
      elements = element_schema_of(array_val);
    } else if (source.query) {
      StatusOr<Relation> elements_query =
          ExecuteQuery(context, *source.query, &scope, ctes);
      if (!elements_query.HasValue()) {
        elements_status = elements_query.GetStatus();
        return;
      }
      elements = elements_query.MoveValue();
      if (!source.alias.empty()) {
        elements.schema = QualifySchema(elements.schema, source.alias);
      }
    }
    if (!schema_initialized) {
      output.schema = prefix.schema + elements.schema;
      schema_initialized = true;
    }
    // The element relation's shape is derived from the array value at hand,
    // so a later prefix row can in principle expand to fewer columns than
    // the row that initialized output.schema.  NULL-fill (or trim) back to
    // the initialized arity so every emitted row matches output.schema
    // exactly; Lookup and projection index rows by schema offsets.
    elements.FinishSpill();
    const size_t prefix_width = row.values_.size();
    std::vector<char> element_matched(
        want_right_nulls ? elements.TotalRows() : 0, 0);
    size_t element_index = 0;
    bool matched = false;
    elements.ForEachRow([&](const Row& element_row) {
      const size_t this_element = element_index++;
      Row combined = row + element_row;
      combined.values_.resize(output.schema.ColumnCount());
      if (condition) {
        Scope combined_scope{
            .row = &combined, .schema = &output.schema, .outer = outer};
        if (!source.using_columns.empty()) {
          combined_scope.using_columns = &source.using_columns;
        }
        bool all_match = true;
        for (const UsingEquality& key : using_equalities) {
          const Value& left_value = combined.values_[key.prefix_column];
          const Value& right_value =
              combined.values_[prefix_width + key.element_column];
          if (!Truthy(EvaluateBinary(BinaryOperation::kEquals, left_value,
                                     right_value))) {
            all_match = false;
            break;
          }
        }
        if (all_match) {
          for (const Expression& conjunct : residual_condition) {
            StatusOr<Value> conjunct_value =
                TryEvaluate(conjunct, combined_scope, nullptr, context, ctes);
            if (!conjunct_value.HasValue()) {
              if (elements_status == Status::kSuccess) {
                elements_status = conjunct_value.GetStatus();
              }
              return;
            }
            if (!Truthy(conjunct_value.Value())) {
              all_match = false;
              break;
            }
          }
        }
        if (!all_match) {
          return;
        }
      }
      if (want_right_nulls && this_element < element_matched.size()) {
        element_matched[this_element] = 1;
      }
      matched = true;
      output.AddRow(std::move(combined));
    });
    if (!matched && left_outer &&
        (condition != nullptr || elements.TotalRows() == 0)) {
      std::vector<Value> nulls(output.schema.ColumnCount() - row.values_.size(),
                               Value());
      output.AddRow(ConcatRows(row, Row(std::move(nulls))));
    }
    // RIGHT / FULL OUTER: within this prefix row's expansion, element rows
    // that matched nothing emit once, null-extended on the prefix side.
    if (want_right_nulls && !element_matched.empty()) {
      size_t replay_index = 0;
      elements.ForEachRow([&](const Row& element_row) {
        const size_t index = replay_index++;
        if (index < element_matched.size() && element_matched[index] == 0) {
          std::vector<Value> nulls(prefix.schema.ColumnCount(), Value());
          output.AddRow(Row(std::move(nulls)) + element_row);
        }
      });
    }
  });
  if (!any_prefix_row || !schema_initialized) {
    // Empty prefix (or every prefix row skipped before initialization):
    // still derive the output schema so downstream projection sees the
    // unnest columns; evaluating on an all-NULL representative row keeps
    // this exception-free for constant arrays.
    if (!schema_initialized) {
      Row representative(
          std::vector<Value>(prefix.schema.ColumnCount(), Value()));
      Scope scope{
          .row = &representative, .schema = &prefix.schema, .outer = outer};
      Relation elements(context.execution_runtime());
      if (source.unnest) {
        StatusOr<Value> array_or =
            TryEvaluate(source.unnest, scope, nullptr, context, ctes);
        if (!array_or.HasValue()) {
          return array_or.GetStatus();
        }
        const Value array_val = array_or.MoveValue();
        elements = element_schema_of(array_val);
      } else if (source.query) {
        StatusOr<Relation> elements_query =
            ExecuteQuery(context, *source.query, &scope, ctes);
        if (!elements_query.HasValue()) {
          return elements_query.GetStatus();
        }
        elements = elements_query.MoveValue();
        if (!source.alias.empty()) {
          elements.schema = QualifySchema(elements.schema, source.alias);
        }
      }
      output.schema = prefix.schema + elements.schema;
      schema_initialized = true;
    }
  }
  output.FinishSpill();
  if (elements_status != Status::kSuccess) {
    return elements_status;
  }
  return output;
}
}  // namespace
#pragma GCC diagnostic pop

StatusOr<Relation> BuildInput(TransactionContext& context,
                              const SelectStatement& statement,
                              const Scope* outer, const CteMap& ctes,
                              bool* where_fully_applied,
                              GroupedJoinFusionSpec* fusion) {
  *where_fully_applied = false;
  if (statement.Sources().empty()) {
    Relation singleton;
    singleton.rows.emplace_back();
    singleton.peak_intermediate_rows = 1;
    return singleton;
  }

  // Lateral UNNEST / LATERAL subquery: a FROM item whose UNNEST or LATERAL
  // subquery argument references columns of sibling FROM items or enclosing
  // scopes cannot be materialized in isolation; it expands per-row against
  // the relations accumulated so far.
  {
    bool lateral = false;
    for (const SelectSource& source : statement.Sources()) {
      if (source.is_lateral) {
        lateral = true;
        break;
      }
      if (source.unnest) {
        std::unordered_set<ColumnName> touched;
        CollectColumnsRecursive(source.unnest, &touched);
        if (!touched.empty()) {
          lateral = true;
          break;
        }
      }
    }
    if (lateral) {
      Relation result(context.execution_runtime());
      bool first = true;
      for (const SelectSource& source : statement.Sources()) {
        Relation relation(context.execution_runtime());
        if (source.unnest || source.is_lateral) {
          // Lateral expansion already merges the prefix with each element
          // row (applying the source's join condition); no separate join.
          if (first) {
            Relation singleton(context.execution_runtime());
            singleton.schema = Schema("", std::vector<Column>{});
            singleton.rows.emplace_back();
            singleton.peak_intermediate_rows = 1;
            StatusOr<Relation> expanded =
                LateralExpandRelation(context, source, singleton, outer, ctes);
            if (!expanded.HasValue()) {
              return expanded.GetStatus();
            }
            relation = expanded.MoveValue();
          } else {
            StatusOr<Relation> expanded =
                LateralExpandRelation(context, source, result, outer, ctes);
            if (!expanded.HasValue()) {
              return expanded.GetStatus();
            }
            relation = expanded.MoveValue();
          }
          result = std::move(relation);
          first = false;
        } else {
          ASSIGN_OR_RETURN(Relation, relation_r,
                           (LoadSource(context, source, outer, ctes)));
          relation = std::move(relation_r);
          if (first) {
            result = std::move(relation);
            first = false;
          } else {
            ASSIGN_OR_RETURN(Relation, result_r,
                             (Join(context, std::move(result),
                                   std::move(relation), source, outer, ctes)));
            result = std::move(result_r);
          }
        }
      }
      return result;
    }
  }

  std::vector<Relation> relations(statement.Sources().size());
  std::vector<bool> base_sources(statement.Sources().size(), false);
  std::vector<std::vector<slot_t>> projections(statement.Sources().size());
  for (size_t i = 0; i < statement.Sources().size(); ++i) {
    const SelectSource& source = statement.Sources()[i];
    base_sources[i] =
        !source.query && !source.unnest && !ctes.contains(source.table);
    if (!base_sources[i]) {
      ASSIGN_OR_RETURN(Relation, loaded_rel,
                       (LoadSource(context, source, outer, ctes)));
      relations[i] = std::move(loaded_rel);
      // A non-aggregate derived SELECT with a literal FALSE filter is known
      // to be empty independently of its source table.  Preserve the derived
      // schema but discard the rows before the outer join-order loop, so an
      // inner join can avoid opening its other side.
      const SelectStatement* derived = source.query.get();
      if (derived != nullptr && derived->WhereClause() &&
          derived->WhereClause()->Type() == TypeTag::kConstantValue &&
          !derived->WhereClause()->AsConstantValue().GetValue().Truthy() &&
          derived->GroupBy().empty() && !derived->Distinct() &&
          !std::ranges::any_of(
              derived->SelectList(), [](const NamedExpression& item) {
                return item.expression && ContainsAggregate(item.expression);
              })) {
        relations[i].ResetContents();
      }
      continue;
    }

    StatusOr<std::shared_ptr<Table>> table = context.GetTable(source.table);
    if (!table.HasValue()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "table " + source.table + " not found");
    }
    const std::string qualifier =
        source.alias.empty() ? source.table : source.alias;
    relations[i].schema =
        qualifier.empty()
            ? table.Value()->GetSchema()
            : QualifySchema(table.Value()->GetSchema(), qualifier);
    projections[i] = RequiredColumns(statement, relations[i].schema);
    if (const std::vector<slot_t>* shared =
            ReusableProjection(context, source.table)) {
      // The shared layout was derived from the raw table schema, so
      // alias-qualified references (T1.col) resolved against this source's
      // qualified schema may be missing from it; projecting fewer columns
      // than this source needs breaks evaluation downstream.
      std::vector<slot_t> merged(projections[i]);
      for (const slot_t slot : *shared) {
        if (std::ranges::find(merged, slot) == merged.end()) {
          merged.push_back(slot);
        }
      }
      std::ranges::sort(merged);
      projections[i] = std::move(merged);
    }
  }

  // Multi-alias tables share one projected cache entry: take the union of
  // columns required by every reference so each alias can reuse the scan.
  {
    std::unordered_map<std::string, std::set<slot_t>> column_union;
    for (size_t i = 0; i < statement.Sources().size(); ++i) {
      if (!base_sources[i] ||
          !ReusesBaseRelation(context, statement.Sources()[i])) {
        continue;
      }
      auto& columns = column_union[statement.Sources()[i].table];
      for (slot_t column : projections[i]) {
        columns.insert(column);
      }
    }
    for (size_t i = 0; i < statement.Sources().size(); ++i) {
      if (!base_sources[i] ||
          !ReusesBaseRelation(context, statement.Sources()[i])) {
        continue;
      }
      const auto& columns = column_union[statement.Sources()[i].table];
      projections[i].assign(columns.begin(), columns.end());
    }
  }

  bool all_outer_reducible = false;
  const std::vector<SelectSource> reduced_sources =
      ReduceOuterJoinsToInner(statement, relations, &all_outer_reducible);
  if (all_outer_reducible) {
    SelectStatement reduced_statement(statement);
    reduced_statement.SetSources(reduced_sources);
    return BuildInput(context, reduced_statement, outer, ctes,
                      where_fully_applied, fusion);
  }

  // OUTER JOIN predicates and USING merges have order-dependent semantics,
  // so retain their syntactic order. The optimizer below is valid for
  // inner/cross joins.
  const bool has_nested_join = std::any_of(
      statement.Sources().begin(), statement.Sources().end(),
      [](const SelectSource& source) { return source.from_nested_join; });
  const bool has_ordered_join = std::any_of(
      statement.Sources().begin() + 1, statement.Sources().end(),
      [](const SelectSource& source) {
        return source.join_type == JoinType::kLeft ||
               source.join_type == JoinType::kRight ||
               source.join_type == JoinType::kFull || source.from_nested_join ||
               source.unnest || !source.using_columns.empty();
      });
  if (has_nested_join) {
    // A parenthesized right-hand join is flattened by the frontend so that
    // column binding remains positional.  Its first source carries the
    // predicate of the outer join, while subsequent sources carry the
    // predicates inside the parentheses.  Reconstruct that right-deep tree
    // here before joining it to the prefix.
    size_t nested_begin = 0;
    while (nested_begin < statement.Sources().size() &&
           !statement.Sources()[nested_begin].from_nested_join) {
      ++nested_begin;
    }
    for (size_t i = 0; i < relations.size(); ++i) {
      if (base_sources[i]) {
        ASSIGN_OR_RETURN(Relation, loaded_rel,
                         (LoadSource(context, statement.Sources()[i], outer,
                                     ctes, &projections[i])));
        relations[i] = std::move(loaded_rel);
      }
    }
    Relation prefix = std::move(relations.front());
    for (size_t i = 1; i < nested_begin; ++i) {
      ASSIGN_OR_RETURN(
          Relation, prefix_r,
          (Join(context, std::move(prefix), std::move(relations[i]),
                statement.Sources()[i], outer, ctes)));
      prefix = std::move(prefix_r);
    }
    Relation nested = std::move(relations[nested_begin]);
    for (size_t i = nested_begin + 1; i < relations.size(); ++i) {
      ASSIGN_OR_RETURN(
          Relation, nested_r,
          (Join(context, std::move(nested), std::move(relations[i]),
                statement.Sources()[i], outer, ctes)));
      nested = std::move(nested_r);
    }
    return Join(context, std::move(prefix), std::move(nested),
                statement.Sources()[nested_begin], outer, ctes);
  }
  if (has_ordered_join) {
    // The ordered path keeps the syntactic join shape, but WHERE conjuncts
    // are still placed as early as they become referenceable, mirroring the
    // inner-join optimizer below: (a) single-source conjuncts filter the
    // owning scan, and (b) conjuncts whose last referenced table is a comma
    // (cross) source become that join's condition, turning what would be an
    // unbounded cross product into an equi/residual join.  Placement only
    // moves a conjunct to the first step where every source it references is
    // already joined -- never below the outer join that supplies one of its
    // columns -- and the full WHERE still runs above the joined output, so
    // outer-join semantics are unchanged.  Volatile or query-bearing
    // conjuncts are left for the top-level filter, since applying them twice
    // must not change how often they evaluate.
    // TINYLAMB_DISABLE_JOIN_REORDER=1 restores the strict syntactic visit
    // order (A/B hatch; results must be identical either way).
    static const bool disable_join_reorder =
        std::getenv("TINYLAMB_DISABLE_JOIN_REORDER") != nullptr;
    std::vector<PredicateInfo> ordered_predicates =
        AnalyzePredicates(statement.WhereClause(), relations);
    // Disjunctions that imply the same equi-join pair on every branch gain
    // synthetic join edges so the greedy reorder and the hash keys see the
    // same connectivity the Cascades memo infers (TPC-DS q13); the original
    // disjunction is untouched and keeps re-verifying matched pairs.
    std::vector<PredicateInfo> disjunct_edges =
        SynthesizeDisjunctEqualityPredicates(ordered_predicates, relations);
    ordered_predicates.insert(ordered_predicates.end(),
                              std::make_move_iterator(disjunct_edges.begin()),
                              std::make_move_iterator(disjunct_edges.end()));
    static const bool debug_ordered =
        std::getenv("TINYLAMB_DEBUG_ORDERED") != nullptr;
    if (debug_ordered) {
      for (size_t p = 0; p < ordered_predicates.size(); ++p) {
        const PredicateInfo& predicate = ordered_predicates[p];
        std::cerr << "[ordered] p" << p << " resolved=" << predicate.resolved
                  << " query=" << predicate.contains_query << " sources=[";
        for (size_t s : predicate.sources) {
          std::cerr << s << ',';
        }
        std::cerr << "] expr=" << predicate.expression->ToString() << '\n';
      }
    }
    std::vector<bool> predicate_placed(ordered_predicates.size(), false);
    const auto placeable = [&](const PredicateInfo& predicate) {
      return predicate.resolved && !predicate.contains_query &&
             SafeToReduceEvaluationCount(predicate.expression);
    };
    // Single-source conjuncts may only pre-filter a PRESERVED side.
    // Pre-filtering a null-supplying side converts some matched rows into
    // NULL-padded rows, which changes the outcome of WHERE terms that
    // distinguish padded rows -- `WHERE b.id IS NULL` over
    // `a LEFT JOIN b` (outer_to_anti) would otherwise resurrect the rows
    // whose match was filtered away.  A source is null-supplying when it is
    // the right side of a LEFT/FULL join; a RIGHT/FULL join additionally
    // makes every earlier source null-supplying.
    std::vector<char> filter_safe(relations.size(), 1);
    for (size_t i = 1; i < statement.Sources().size(); ++i) {
      const JoinType type = statement.Sources()[i].join_type;
      if (type == JoinType::kLeft || type == JoinType::kFull) {
        filter_safe[i] = 0;
      }
      if (type == JoinType::kRight || type == JoinType::kFull) {
        for (size_t j = 0; j < i; ++j) {
          filter_safe[j] = 0;
        }
      }
    }
    std::vector<std::vector<Expression>> ordered_local(relations.size());
    for (size_t p = 0; p < ordered_predicates.size(); ++p) {
      const PredicateInfo& predicate = ordered_predicates[p];
      if (!placeable(predicate) || predicate.sources.size() != 1) {
        continue;
      }
      const size_t owner = *predicate.sources.begin();
      if (!static_cast<bool>(filter_safe[owner])) {
        continue;
      }
      ordered_local[owner].push_back(predicate.expression);
      predicate_placed[p] = true;
    }
    if (debug_ordered) {
      for (size_t i = 0; i < ordered_local.size(); ++i) {
        if (!ordered_local[i].empty()) {
          std::cerr << "[ordered] local[" << i
                    << "] count=" << ordered_local[i].size()
                    << " base=" << base_sources[i] << '\n';
        }
      }
    }
    for (size_t i = 0; i < relations.size(); ++i) {
      if (base_sources[i]) {
        ASSIGN_OR_RETURN(
            Relation, loaded_rel,
            (LoadSource(
                context, statement.Sources()[i], outer, ctes, &projections[i],
                ordered_local[i].empty() ? nullptr : &ordered_local[i])));
        relations[i] = std::move(loaded_rel);
      } else if (!ordered_local[i].empty()) {
        RETURN_IF_FAIL(FilterRelation(context, &relations[i], ordered_local[i],
                                      outer, ctes));
      }
    }
    // Maximal runs of inner/cross sources reorder greedily by connectivity:
    // inner joins are associative and commutative, so visiting them in the
    // syntactic chain order is not required.  Outer/USING/nested/lateral
    // sources keep their syntactic position (their null-extension and merge
    // semantics are order dependent) and freeze the run boundaries.  The
    // mirror heuristic drives the choice: prefer candidates that connect to
    // the already-joined prefix, smallest estimated intermediate size first,
    // so dimension chains join before the fact-to-fact joins that need
    // their filters (TPC-DS q72 joins the 2002 date slice before inventory).
    std::vector<size_t> visit_order;
    {
      const auto reorderable = [&](size_t i) {
        const SelectSource& s = statement.Sources()[i];
        return (s.join_type == JoinType::kInner ||
                s.join_type == JoinType::kCross) &&
               !s.from_nested_join && !s.unnest && s.using_columns.empty();
      };
      // Connective conjuncts: the ON conditions of run members plus every
      // unplaced multi-source WHERE conjunct, analyzed per source set.
      struct ConnectiveConjunct {
        std::unordered_set<size_t> sources;
      };
      std::vector<ConnectiveConjunct> connectives;
      const auto collect = [&](const Expression& conjunct) {
        if (!conjunct) {
          return;
        }
        const std::vector<PredicateInfo> analyzed =
            AnalyzePredicates(conjunct, relations);
        if (analyzed.size() == 1 && analyzed[0].resolved) {
          connectives.push_back({analyzed[0].sources});
        }
      };
      for (size_t i = 1; i < relations.size(); ++i) {
        if (reorderable(i)) {
          for (const Expression& conjunct :
               SplitConjuncts(statement.Sources()[i].join_condition)) {
            collect(conjunct);
          }
        }
      }
      for (size_t p = 0; p < ordered_predicates.size(); ++p) {
        const PredicateInfo& predicate = ordered_predicates[p];
        if (!predicate_placed[p] && placeable(predicate) &&
            predicate.sources.size() >= 2) {
          connectives.push_back({predicate.sources});
        }
      }
      const auto connected_to = [&](size_t candidate,
                                    const std::unordered_set<size_t>& joined) {
        // The candidate is not part of `joined` yet: a conjunct connects it
        // when every source it references is either already joined or the
        // candidate itself, and at least one referenced source is new.
        std::unordered_set<size_t> with_candidate = joined;
        with_candidate.insert(candidate);
        const auto connects_candidate =
            [&](const ConnectiveConjunct& conjunct) {
              return conjunct.sources.contains(candidate) &&
                     conjunct.sources.size() >= 2 &&
                     IsSubset(conjunct.sources, with_candidate) &&
                     !IsSubset(conjunct.sources, joined);
            };
        return std::ranges::any_of(connectives, connects_candidate);
      };
      // A source may only join when its own ON condition binds against the
      // prefix plus itself: joining it earlier would leave the ON
      // referencing a relation that is not an input of the join.  ON
      // conjuncts that never resolved (outer-scope qualifiers, subqueries)
      // keep their historic residual handling and do not block.
      std::vector<std::unordered_set<size_t>> on_sources(relations.size());
      for (size_t i = 1; i < relations.size(); ++i) {
        for (const Expression& conjunct :
             SplitConjuncts(statement.Sources()[i].join_condition)) {
          if (!conjunct) {
            continue;
          }
          const std::vector<PredicateInfo> analyzed =
              AnalyzePredicates(conjunct, relations);
          if (analyzed.size() == 1 && analyzed[0].resolved) {
            on_sources[i].insert(analyzed[0].sources.begin(),
                                 analyzed[0].sources.end());
          }
        }
      }
      const auto on_bindable = [&](size_t candidate,
                                   const std::unordered_set<size_t>& joined) {
        std::unordered_set<size_t> with_candidate = joined;
        with_candidate.insert(candidate);
        return IsSubset(on_sources[candidate], with_candidate);
      };
      size_t run_begin = 1;
      while (run_begin < relations.size()) {
        if (disable_join_reorder) {
          visit_order.push_back(run_begin);
          ++run_begin;
          continue;
        }
        if (!reorderable(run_begin)) {
          visit_order.push_back(run_begin);
          ++run_begin;
          continue;
        }
        size_t run_end = run_begin;
        while (run_end < relations.size() && reorderable(run_end)) {
          ++run_end;
        }
        // Start the chain from the prefix-connected member with the fewest
        // rows (the min() estimate cannot see hash fanout, so simulating
        // every start ties on peak; the smallest filtered member is the
        // robust anchor).  Then greedily append connected candidates.
        std::unordered_set<size_t> prefix;
        for (size_t i = 0; i < run_begin; ++i) {
          prefix.insert(i);
        }
        size_t start = run_begin;
        bool have_start = false;
        for (size_t candidate = run_begin; candidate < run_end; ++candidate) {
          if (!on_bindable(candidate, prefix)) {
            continue;
          }
          if (run_begin > 1 && !connected_to(candidate, prefix)) {
            continue;
          }
          const size_t rows = relations[candidate].TotalRows();
          if (!have_start || rows < relations[start].TotalRows()) {
            have_start = true;
            start = candidate;
          }
        }
        if (!have_start) {
          // No member binds against the prefix (a disconnected cross
          // fragment): fall back to the syntactic order for this run.
          for (size_t idx = run_begin; idx < run_end; ++idx) {
            visit_order.push_back(idx);
          }
          run_begin = run_end;
          continue;
        }
        std::vector<size_t> order{start};
        std::unordered_set<size_t> sim_joined = prefix;
        sim_joined.insert(start);
        std::vector<size_t> remaining;
        for (size_t i = run_begin; i < run_end; ++i) {
          if (i != start) {
            remaining.push_back(i);
          }
        }
        while (!remaining.empty()) {
          bool any_bindable = false;
          for (size_t candidate : remaining) {
            any_bindable = any_bindable || on_bindable(candidate, sim_joined);
          }
          if (!any_bindable) {
            // The remaining fragment's ON conditions reference only its own
            // members (a disconnected inner fragment).  Reordering cannot
            // help; preserve the syntactic order, which is the historic
            // shape for this case.
            for (const size_t idx : remaining) {
              order.push_back(idx);
            }
            break;
          }
          size_t next = remaining.front();
          bool next_bindable = on_bindable(next, sim_joined);
          bool next_connected = next_bindable && connected_to(next, sim_joined);
          for (size_t candidate : remaining) {
            const bool cand_bindable = on_bindable(candidate, sim_joined);
            const bool cand_connected =
                cand_bindable && connected_to(candidate, sim_joined);
            const bool cheaper =
                relations[candidate].TotalRows() < relations[next].TotalRows();
            if ((cand_connected && !next_connected) ||
                (cand_connected == next_connected && cand_bindable &&
                 !next_bindable) ||
                (cand_connected == next_connected &&
                 cand_bindable == next_bindable && cheaper)) {
              next = candidate;
              next_connected = cand_connected;
              next_bindable = cand_bindable;
            }
          }
          order.push_back(next);
          sim_joined.insert(next);
          std::erase(remaining, next);
        }
        if (debug_ordered) {
          std::cerr << "[ordered] run [" << run_begin << ',' << run_end
                    << ") start=" << start << " order=";
          for (const size_t idx : order) {
            std::cerr << idx << ',';
          }
          std::cerr << '\n';
        }
        for (const size_t idx : order) {
          visit_order.push_back(idx);
        }
        run_begin = run_end;
      }
    }
    Relation result = std::move(relations.front());
    std::unordered_set<size_t> available{0};
    for (const size_t i : visit_order) {
      available.insert(i);
      SelectSource source = statement.Sources()[i];
      // Outer-join ON conditions keep their syntactic predicate: folding a
      // WHERE conjunct into them would change matching into null padding.
      // Inner and cross joins admit it: sigma_p(A join_c B) equals
      // A join_(c AND p) B, and the join machinery turns the equality
      // conjuncts into hash keys while the residuals re-verify per pair.
      if (source.join_type == JoinType::kCross ||
          source.join_type == JoinType::kInner) {
        std::vector<Expression> applicable;
        for (size_t p = 0; p < ordered_predicates.size(); ++p) {
          const PredicateInfo& predicate = ordered_predicates[p];
          if (predicate_placed[p] || !placeable(predicate) ||
              predicate.sources.size() < 2 || !predicate.sources.contains(i) ||
              !IsSubset(predicate.sources, available)) {
            continue;
          }
          // Same rule as the single-source pre-filters above: a conjunct
          // over a source that a LATER RIGHT/FULL join NULL-supplies must
          // not filter below that join.  `a CROSS JOIN b RIGHT JOIN c ...
          // WHERE f(a, b) IS NULL` evaluated f before the RIGHT join, turned
          // c rows whose matches all failed f into NULL-padded rows, and the
          // padded rows then passed the IS NULL test.
          if (!std::ranges::all_of(predicate.sources, [&](size_t source_index) {
                return static_cast<bool>(filter_safe[source_index]);
              })) {
            continue;
          }
          applicable.push_back(predicate.expression);
          predicate_placed[p] = true;
        }
        if (!applicable.empty()) {
          if (source.join_condition) {
            applicable.push_back(source.join_condition);
          }
          source.join_condition = CombineConjuncts(applicable);
        }
      }
      // Final ordered-join step of a fusable grouped statement with an empty
      // WHERE: the join (including its ON residuals, evaluated per matched
      // pair) feeds the aggregation directly.  Q13's inner LEFT JOIN lives
      // here.  The fused path never materializes the join.
      if (fusion != nullptr && fusion->eligible && i + 1 == relations.size() &&
          !statement.WhereClause() &&
          (source.join_type == JoinType::kLeft ||
           source.join_type == JoinType::kCross ||
           source.join_type == JoinType::kInner)) {
        std::vector<Expression> fusion_predicates;
        if (source.join_condition) {
          for (const Expression& conjunct :
               SplitConjuncts(source.join_condition)) {
            fusion_predicates.push_back(conjunct);
          }
        }
        const JoinType fused_type = source.join_type == JoinType::kLeft
                                        ? JoinType::kLeft
                                        : JoinType::kInner;
        ASSIGN_OR_RETURN(std::optional<Relation>, fused,
                         (TryFusedGroupedJoin(context, result, relations[i],
                                              fusion_predicates, fused_type,
                                              outer, ctes, fusion)));
        if (fused.has_value()) {
          result = std::move(*fused);
          fusion->fired = true;
          return result;
        }
      }
      ASSIGN_OR_RETURN(Relation, result_r,
                       (Join(context, std::move(result),
                             std::move(relations[i]), source, outer, ctes)));
      result = std::move(result_r);
    }
    return result;
  }

  // An already materialized inner-join input which is empty makes the whole
  // join empty.  Stop before opening any remaining base-table scans: this is
  // both a correctness-preserving short circuit and important for EXISTS /
  // derived-table plans, where an empty build side must not touch the probe
  // side at all.  Outer/USING/nested joins are excluded above because their
  // null-extension or merge semantics require the ordered path.
  bool has_empty_materialized_input = false;
  for (size_t i = 0; i < relations.size(); ++i) {
    if (!base_sources[i] && relations[i].TotalRows() == 0) {
      has_empty_materialized_input = true;
      break;
    }
  }
  if (has_empty_materialized_input) {
    Relation empty(context.execution_runtime());
    for (const Relation& relation : relations) {
      if (empty.schema.ColumnCount() == 0) {
        empty.schema = relation.schema;
      } else {
        empty.schema = empty.schema + relation.schema;
      }
    }
    *where_fully_applied = true;
    if (context.execution_runtime() != nullptr) {
      context.execution_runtime()->empty_build_short_circuit = true;
    }
    return empty;
  }

  std::vector<Expression> all_predicates =
      SplitConjuncts(statement.WhereClause());
  for (size_t i = 1; i < statement.Sources().size(); ++i) {
    const Expression& condition = statement.Sources()[i].join_condition;
    if (condition) {
      all_predicates.push_back(condition);
    }
  }
  const auto planning_begin = std::chrono::steady_clock::now();
  double join_estimate_ms = 0;
  // Join-condition conjuncts that the pipeline below cannot push down
  // (subquery predicates, constant-only conjuncts such as NOT EXISTS(SELECT 1))
  // must still gate the joined output; dropping them silently changes the
  // join semantics.  Unresolvable *plain-column* conjuncts keep their
  // historic silent-drop behavior: executor tests pin queries whose ON
  // clauses reference outer qualifiers and rely on the cross-join fallback.
  std::vector<Expression> join_residual;
  auto mark_applied = [&join_residual](const Expression& expression) {
    if (!expression) {
      return;
    }
    std::erase_if(join_residual, [&](const Expression& kept) {
      return kept.get() == expression.get();
    });
  };
  std::vector<PredicateInfo> predicates = AnalyzePredicates(
      all_predicates.empty() ? Expression() : CombineConjuncts(all_predicates),
      relations);
  // Synthetic equi-join edges implied by every branch of a disjunction
  // (TPC-DS q13 shape): they join the connectivity simulation, the runtime
  // key-filter loop and the per-step hash keys, while the original
  // disjunction stays applied as residual at its own join step.
  {
    std::vector<PredicateInfo> disjunct_edges =
        SynthesizeDisjunctEqualityPredicates(predicates, relations);
    predicates.insert(predicates.end(),
                      std::make_move_iterator(disjunct_edges.begin()),
                      std::make_move_iterator(disjunct_edges.end()));
  }
  {
    // Populate the residual list from the analyzed predicates so only
    // join-condition conjuncts whose evaluation is well-defined survive.
    std::unordered_set<const ExpressionBase*> from_joins;
    for (size_t i = 1; i < statement.Sources().size(); ++i) {
      for (const Expression& conjunct :
           SplitConjuncts(statement.Sources()[i].join_condition)) {
        if (conjunct) {
          from_joins.insert(conjunct.get());
        }
      }
    }
    for (const PredicateInfo& predicate : predicates) {
      const Expression& expr = predicate.expression;
      if (!expr || !from_joins.contains(expr.get())) {
        continue;
      }
      // Note: USING(name) conjuncts are fully handled by the ordered-join
      // path above, so a bare `x = x` seen here is a user-written ON
      // conjunct; it keeps normal residual/local handling instead of being
      // dropped (dropping it would lose the NULL rejection of `x = x`).
      // Historic behavior: an unresolvable plain-column conjunct is dropped
      // rather than evaluated against a schema where it cannot bind.
      if (!predicate.resolved && !predicate.contains_query &&
          !predicate.sources.empty()) {
        continue;
      }
      join_residual.push_back(expr);
    }
  }
  const std::vector<PredicateInfo> where_predicates =
      AnalyzePredicates(statement.WhereClause(), relations);
  auto locally_evaluable = [&](const PredicateInfo& predicate) {
    if (!predicate.resolved || predicate.sources.size() != 1) {
      return false;
    }
    if (!predicate.contains_query) {
      return true;
    }
    return ContainsOnlyUncorrelatedQueries(context, predicate.expression, ctes);
  };
  *where_fully_applied = std::ranges::all_of(
      where_predicates, [&](const PredicateInfo& predicate) {
        return locally_evaluable(predicate) ||
               (predicate.resolved && !predicate.contains_query &&
                predicate.sources.size() > 1);
      });
  std::vector<std::vector<Expression>> local_predicates(relations.size());
  for (size_t i = 0; i < relations.size(); ++i) {
    std::vector<Expression>& local = local_predicates[i];
    for (const PredicateInfo& predicate : predicates) {
      if (predicate.sources.contains(i) && locally_evaluable(predicate)) {
        local.push_back(predicate.expression);
        mark_applied(predicate.expression);
      } else if (predicate.resolved && !predicate.contains_query &&
                 predicate.sources.size() > 1) {
        Expression implied =
            NecessaryLocalDisjunction(predicate.expression, i, relations);
        if (implied) {
          local.push_back(std::move(implied));
        }
      }
    }
  }

  // Push uncorrelated `col IN (SELECT ...)` results as integer key filters
  // before scanning the owning base table (TPC-H Q18).
  if (context.execution_runtime() != nullptr) {
    for (const Expression& expression :
         SplitConjuncts(statement.WhereClause())) {
      if (!expression || expression->Type() != TypeTag::kQueryExp) {
        continue;
      }
      const QueryExpression& query = expression->AsQueryExpression();
      if (query.Exists() || query.Negated() || !query.Test() ||
          query.Test()->Type() != TypeTag::kColumnValue) {
        continue;
      }
      // The stashed set is the MEMBERSHIP set: it may only prune rows that
      // fail `test IN (rows)`.  `x > ANY (S)` / `x = ALL (S)` match on
      // operators other than equality (or over multiple rows), where S is
      // not a pruning superset: `j > ANY (SELECT 5,6)` would drop j=9.
      // Only the equality-IN shape (`kIn`, or `= ANY`) may stash.
      if (query.Mode() != QuantifierMode::kIn &&
          (query.Mode() != QuantifierMode::kAny ||
           query.Op() != BinaryOperation::kEquals)) {
        continue;
      }
      if (context.execution_runtime() != nullptr) {
        context.execution_runtime()->retained_statements.push_back(
            query.Query());
      }
      const Relation* membership =
          ExecuteCachedUncorrelated(context, *query.Query(), ctes);
      if (membership == nullptr || membership->rows.empty()) {
        continue;
      }
      // Dynamic filter over the membership set (Phase 2 "Dynamic Filter
      // Join / SIP / Bloom").  The builder keeps the [lo, hi] enclosure of the
      // distinct keys alongside the key set and picks the exact set / SIP
      // shape at Build() time, so a large membership no longer forfeits the
      // range half of the filter.
      std::vector<int64_t> keys;
      keys.reserve(membership->rows.size());
      ValueType membership_type = ValueType::kNull;
      bool all_int = true;
      auto consume_row = [&](const Row& row) {
        if (row.values_.empty() || row[0].IsNull()) {
          return;
        }
        if (row[0].type != ValueType::kInt64 &&
            row[0].type != ValueType::kDate) {
          all_int = false;
          return;
        }
        if (membership_type == ValueType::kNull) {
          membership_type = row[0].type;
        } else if (membership_type != row[0].type) {
          // A mixed INT64/DATE membership set has no single int64 comparison
          // domain; leave those rows out and let the equality-IN re-check
          // decide them.
          all_int = false;
          return;
        }
        keys.push_back(row[0].value.int_value);
      };
      if (membership->HasSpill()) {
        membership->ForEachRow(consume_row);
      } else {
        for (const Row& row : membership->rows) {
          consume_row(row);
        }
      }
      if (!all_int || keys.empty()) {
        continue;
      }
      std::shared_ptr<const RuntimeFilterGroup> membership_filter =
          RuntimeFilterBuilder::FromKeys(keys);
      if (membership_filter == nullptr) {
        continue;
      }
      const size_t membership_keys = membership_filter->DistinctKeys();
      const ColumnName& column = query.Test()->AsColumnValue().GetColumnName();
      for (size_t i = 0; i < relations.size(); ++i) {
        if (!base_sources[i]) {
          continue;
        }
        const auto offset = LocalColumnOffset(relations[i].schema, column);
        if (!offset) {
          continue;
        }
        if (!SingleIntegerJoinKey(relations[i].schema,
                                  {static_cast<slot_t>(*offset)})) {
          continue;
        }
        if (relations[i].schema.GetColumn(*offset).Type() != membership_type) {
          continue;
        }
        TableKeyFilter& stored =
            context.execution_runtime()
                ->table_key_filters[TableKeyFilterKey(statement.Sources()[i])];
        if (stored.empty() || membership_keys < stored.key_count) {
          // shared_ptr copy (refcount bump), not a move: the loop below keeps
          // handing the same group to every eligible source.
          stored.filter = membership_filter;
          stored.type = membership_type;
          stored.key_count = membership_keys;
          stored.column = static_cast<slot_t>(*offset);
          stored.owner = &statement;
        }
      }
    }
  }

  // Load selective relations first, then push their integer join keys into
  // later scans (TPC-H Q9: filter lineitem by partkeys matching LIKE).
  std::vector<size_t> estimated_rows(relations.size(),
                                     std::numeric_limits<size_t>::max());
  for (size_t i = 0; i < relations.size(); ++i) {
    if (!base_sources[i]) {
      estimated_rows[i] = relations[i].TotalRows();
      continue;
    }
    if (StatusOr<std::shared_ptr<TableStatistics>> stats =
            context.GetStats(statement.Sources()[i].table);
        stats.HasValue()) {
      estimated_rows[i] = stats.Value()->Rows();
    }
  }
  std::vector<size_t> load_order;
  load_order.reserve(relations.size());
  for (size_t i = 0; i < relations.size(); ++i) {
    load_order.push_back(i);
  }
  std::ranges::stable_sort(load_order, [&](size_t a, size_t b) {
    const auto rank = [&](size_t i) {
      if (!local_predicates[i].empty()) {
        return 0;
      }
      if (context.execution_runtime() != nullptr) {
        const auto stored = context.execution_runtime()->table_key_filters.find(
            TableKeyFilterKey(statement.Sources()[i]));
        if (stored != context.execution_runtime()->table_key_filters.end() &&
            stored->second.owner == &statement) {
          return 0;
        }
      }
      return 1;
    };
    const int a_rank = rank(a);
    const int b_rank = rank(b);
    if (a_rank != b_rank) {
      return a_rank < b_rank;
    }
    if (estimated_rows[a] != estimated_rows[b]) {
      return estimated_rows[a] < estimated_rows[b];
    }
    return a < b;
  });
  std::vector<bool> loaded(relations.size(), false);
  for (size_t i = 0; i < relations.size(); ++i) {
    if (!base_sources[i]) {
      loaded[i] = true;  // already materialized above
    }
  }
  // Transitive join-key domains: a relation whose only filter partners load
  // later (Q9: partsupp's ps_partkey = l_partkey, with lineitem loading
  // after partsupp) can still be pruned through an equality chain --
  // ps_partkey = l_partkey AND p_partkey = l_partkey makes part's
  // (selective, already-loaded) domain valid for partsupp.  Every resolved
  // two-source equality predicate is applied by the greedy join loop below,
  // so all columns of a class carry equal values in the final result, and
  // pruning a not-yet-loaded member to a loaded member's domain can only
  // remove rows that no surviving row could match.  Ambiguous pairings (a
  // name resolving in both relations) never enter the classes.
  struct RelColKey {
    size_t rel;
    ColumnName column;
    bool operator==(const RelColKey& other) const = default;
  };
  struct RelColKeyHash {
    size_t operator()(const RelColKey& key) const noexcept {
      return std::hash<size_t>()(key.rel) * 0x9e3779b97f4a7c15ULL ^
             std::hash<ColumnName>()(key.column);
    }
  };
  std::unordered_map<RelColKey, size_t, RelColKeyHash> key_class_node;
  std::vector<size_t> key_class_parent;
  static const bool transitive_filter_disabled =
      std::getenv("TINYLAMB_DISABLE_TRANSITIVE_KEY_FILTER") != nullptr;
  const auto key_class_find = [&](size_t node) {
    while (key_class_parent[node] != node) {
      key_class_parent[node] = key_class_parent[key_class_parent[node]];
      node = key_class_parent[node];
    }
    return node;
  };
  const auto key_class_node_id = [&](const RelColKey& key) {
    const size_t next_id = key_class_parent.size();
    const auto [it, inserted] = key_class_node.emplace(key, next_id);
    if (inserted) {
      key_class_parent.push_back(next_id);
    }
    return it->second;
  };
  const auto key_class_union = [&](size_t a, size_t b) {
    a = key_class_find(a);
    b = key_class_find(b);
    if (a != b) {
      key_class_parent[b] = a;
    }
  };
  {
    for (const PredicateInfo& predicate : predicates) {
      if (!predicate.resolved || predicate.contains_query ||
          predicate.sources.size() != 2 ||
          !IsColumnEqualityPredicate(predicate.expression) ||
          predicate.expression->AsBinaryExpression().Op() !=
              BinaryOperation::kEquals) {
        continue;
      }
      const size_t left_rel = *predicate.sources.begin();
      const size_t right_rel = *std::next(predicate.sources.begin());
      const BinaryExpression& binary =
          predicate.expression->AsBinaryExpression();
      const ColumnName& lhs = binary.Left()->AsColumnValue().GetColumnName();
      const ColumnName& rhs = binary.Right()->AsColumnValue().GetColumnName();
      const auto left_of_lhs =
          LocalColumnOffset(relations[left_rel].schema, lhs);
      const auto right_of_rhs =
          LocalColumnOffset(relations[right_rel].schema, rhs);
      const auto left_of_rhs =
          LocalColumnOffset(relations[left_rel].schema, rhs);
      const auto right_of_lhs =
          LocalColumnOffset(relations[right_rel].schema, lhs);
      const bool forward =
          left_of_lhs.has_value() && right_of_rhs.has_value() &&
          !left_of_rhs.has_value() && !right_of_lhs.has_value();
      const bool crossed =
          left_of_rhs.has_value() && right_of_lhs.has_value() &&
          !left_of_lhs.has_value() && !right_of_rhs.has_value();
      if (transitive_filter_disabled) {
        continue;
      }
      if (forward) {
        key_class_union(key_class_node_id({left_rel, lhs}),
                        key_class_node_id({right_rel, rhs}));
      } else if (crossed) {
        key_class_union(key_class_node_id({left_rel, rhs}),
                        key_class_node_id({right_rel, lhs}));
      }
    }
  }
  for (size_t idx : load_order) {
    if (!base_sources[idx]) {
      RETURN_IF_FAIL(FilterRelation(context, &relations[idx],
                                    local_predicates[idx], outer, ctes));
      loaded[idx] = true;
      continue;
    }
    // Dynamic filter built from the best driver side (Phase 2 "Dynamic Filter
    // Join / SIP / Bloom").  `key_filter` is a RuntimeFilterGroup, so the same
    // pruning hook serves the exact build key set, the [lo, hi] enclosure and
    // the Bloom membership filter.
    std::shared_ptr<const RuntimeFilterGroup> key_filter;
    ValueType key_filter_type{ValueType::kNull};
    std::optional<slot_t> key_column;  // offset in the projected scan row
    size_t best_driver_rows = std::numeric_limits<size_t>::max();
    double best_driver_coverage = 1.0;
    bool best_driver_selective = false;
    for (const PredicateInfo& predicate : predicates) {
      if (!predicate.resolved || predicate.contains_query ||
          predicate.sources.size() != 2 || !predicate.sources.contains(idx) ||
          !IsColumnEqualityPredicate(predicate.expression) ||
          predicate.expression->AsBinaryExpression().Op() !=
              BinaryOperation::kEquals) {
        continue;
      }
      size_t other = *predicate.sources.begin();
      if (other == idx) {
        other = *std::next(predicate.sources.begin());
      }
      if (!loaded[other]) {
        continue;
      }
      const bool other_selective =
          !local_predicates[other].empty() ||
          (context.execution_runtime() != nullptr && [&] {
            const auto stored =
                context.execution_runtime()->table_key_filters.find(
                    TableKeyFilterKey(statement.Sources()[other]));
            return stored !=
                       context.execution_runtime()->table_key_filters.end() &&
                   stored->second.owner == &statement;
          }());
      const size_t driver_rows = relations[other].TotalRows();
      // Prefer drivers that were themselves filtered (e.g. part LIKE), even
      // if an unfiltered neighbor has slightly fewer rows (supplier).
      if (key_column) {
        if (best_driver_selective && !other_selective) {
          continue;
        }
        if (best_driver_selective == other_selective &&
            driver_rows >= best_driver_rows) {
          continue;
        }
      }
      const BinaryExpression& binary =
          predicate.expression->AsBinaryExpression();
      const ColumnName& lhs = binary.Left()->AsColumnValue().GetColumnName();
      const ColumnName& rhs = binary.Right()->AsColumnValue().GetColumnName();
      const auto idx_left = LocalColumnOffset(relations[idx].schema, lhs);
      const auto idx_right = LocalColumnOffset(relations[idx].schema, rhs);
      const auto other_left = LocalColumnOffset(relations[other].schema, lhs);
      const auto other_right = LocalColumnOffset(relations[other].schema, rhs);
      std::optional<slot_t> idx_col;
      std::optional<slot_t> other_col;
      if (idx_left && other_right) {
        idx_col = static_cast<slot_t>(*idx_left);
        other_col = static_cast<slot_t>(*other_right);
      } else if (idx_right && other_left) {
        idx_col = static_cast<slot_t>(*idx_right);
        other_col = static_cast<slot_t>(*other_left);
      }
      if (!idx_col || !other_col) {
        continue;
      }
      if (!SameSingleIntegerJoinKey(relations[idx].schema, {*idx_col},
                                    relations[other].schema, {*other_col})) {
        // Type equality is part of the contract, not just "both integers":
        // INT64 and DATE share the int64 bit domain but not the comparison
        // domain, so a filter built from DATE keys could otherwise prune
        // INT64 probe rows whose bits merely look like dates.
        continue;
      }
      // Map full-schema column index into the projected scan layout.
      const auto& proj = projections[idx];
      const auto proj_it = std::ranges::find(proj, *idx_col);
      if (proj_it == proj.end()) {
        continue;
      }
      const auto projected_col =
          static_cast<slot_t>(std::distance(proj.begin(), proj_it));

      RuntimeFilterBuilder builder;
      relations[other].FinishSpill();
      relations[other].ForEachRow([&](const Row& row) {
        const Value& value = row[*other_col];
        if (value.IsNull()) {
          builder.ObserveNull();
        } else {
          builder.Observe(value.value.int_value);
        }
      });
      if (builder.empty() || builder.overflowed()) {
        continue;
      }
      std::shared_ptr<const RuntimeFilterGroup> candidate = builder.Build();
      if (candidate == nullptr || candidate->empty() ||
          candidate->Has(RuntimeFilterKind::kRejectAll)) {
        continue;
      }
      // Skip near-full key domains (e.g. all supplier keys vs lineitem): the
      // membership test then costs more than the row it saves.  A range-only
      // SIP filter is still worth building there, because the enclosure costs
      // two comparisons and prunes whatever falls outside the driver range.
      if (!other_selective && !builder.saw_null() &&
          candidate->DistinctKeys() >= driver_rows && driver_rows >= 1000 &&
          !candidate->Has(RuntimeFilterKind::kMinMax)) {
        continue;
      }
      key_filter = candidate;
      key_filter_type = relations[idx].schema.GetColumn(*idx_col).Type();
      key_column = projected_col;
      best_driver_rows = driver_rows;
      best_driver_selective = other_selective;
    }
    if (!key_column && !transitive_filter_disabled) {
      // No loaded direct partner: walk the partner column's equality class
      // (built above) for a loaded transitive driver.  The same soundness
      // rules as the direct pass apply -- type-matched integer columns,
      // projected-column mapping, and the near-full-domain skip -- so the
      // filter only ever prunes rows the join would reject anyway.
      for (const PredicateInfo& predicate : predicates) {
        if (!predicate.resolved || predicate.contains_query ||
            predicate.sources.size() != 2 || !predicate.sources.contains(idx) ||
            !IsColumnEqualityPredicate(predicate.expression) ||
            predicate.expression->AsBinaryExpression().Op() !=
                BinaryOperation::kEquals) {
          continue;
        }
        size_t other = *predicate.sources.begin();
        if (other == idx) {
          other = *std::next(predicate.sources.begin());
        }
        if (loaded[other]) {
          continue;  // the direct pass above already considered it
        }
        const BinaryExpression& binary =
            predicate.expression->AsBinaryExpression();
        const ColumnName& lhs = binary.Left()->AsColumnValue().GetColumnName();
        const ColumnName& rhs = binary.Right()->AsColumnValue().GetColumnName();
        const auto idx_left = LocalColumnOffset(relations[idx].schema, lhs);
        const auto idx_right = LocalColumnOffset(relations[idx].schema, rhs);
        const auto other_left = LocalColumnOffset(relations[other].schema, lhs);
        const auto other_right =
            LocalColumnOffset(relations[other].schema, rhs);
        std::optional<slot_t> idx_col;
        ColumnName partner_column;
        const bool forward = idx_left.has_value() && other_right.has_value() &&
                             !idx_right.has_value() && !other_left.has_value();
        const bool crossed = idx_right.has_value() && other_left.has_value() &&
                             !idx_left.has_value() && !other_right.has_value();
        if (forward) {
          idx_col = static_cast<slot_t>(*idx_left);
          partner_column = rhs;
        } else if (crossed) {
          idx_col = static_cast<slot_t>(*idx_right);
          partner_column = lhs;
        }
        if (!idx_col.has_value() || partner_column.Empty()) {
          continue;
        }
        const auto node_it = key_class_node.find({other, partner_column});
        if (node_it == key_class_node.end()) {
          continue;
        }
        const size_t rep = key_class_find(node_it->second);
        for (const auto& [member, member_id] : key_class_node) {
          if (key_class_find(member_id) != rep || member.rel == idx ||
              member.rel == other || !loaded[member.rel]) {
            continue;
          }
          const auto member_col =
              LocalColumnOffset(relations[member.rel].schema, member.column);
          if (!member_col.has_value()) {
            continue;
          }
          if (!SameSingleIntegerJoinKey(relations[idx].schema, {*idx_col},
                                        relations[member.rel].schema,
                                        {static_cast<slot_t>(*member_col)})) {
            continue;
          }
          const auto& proj = projections[idx];
          const auto proj_it = std::ranges::find(proj, *idx_col);
          if (proj_it == proj.end()) {
            continue;
          }
          const auto projected_col =
              static_cast<slot_t>(std::distance(proj.begin(), proj_it));
          RuntimeFilterBuilder builder;
          int64_t observed_min = 0;
          int64_t observed_max = -1;
          relations[member.rel].FinishSpill();
          relations[member.rel].ForEachRow([&](const Row& row) {
            const Value& value = row[*member_col];
            if (value.IsNull()) {
              builder.ObserveNull();
            } else {
              builder.Observe(value.value.int_value);
              observed_min = observed_max < 0 ? value.value.int_value
                                              : std::min(observed_min,
                                                         value.value.int_value);
              observed_max = std::max(observed_max, value.value.int_value);
            }
          });
          if (builder.empty() || builder.overflowed()) {
            continue;
          }
          std::shared_ptr<const RuntimeFilterGroup> candidate = builder.Build();
          if (candidate == nullptr || candidate->empty() ||
              candidate->Has(RuntimeFilterKind::kRejectAll)) {
            continue;
          }
          const bool member_selective =
              !local_predicates[member.rel].empty() ||
              (context.execution_runtime() != nullptr && [&] {
                const auto stored =
                    context.execution_runtime()->table_key_filters.find(
                        TableKeyFilterKey(statement.Sources()[member.rel]));
                return stored != context.execution_runtime()
                                     ->table_key_filters.end() &&
                       stored->second.owner == &statement;
              }());
          const size_t driver_rows = relations[member.rel].TotalRows();
          // Estimated pruning power: the fraction of the driver's key span
          // the filter covers.  A full-span domain (s_suppkey holds every
          // supplier key) rejects nothing however small the driver is; a
          // scattered domain (LIKE-selected p_partkey across the full key
          // range) prunes almost everything the join would reject.
          double coverage = 1.0;
          if (observed_max >= observed_min) {
            const double span = static_cast<double>(observed_max) -
                                static_cast<double>(observed_min) + 1.0;
            coverage =
                std::min(1.0, static_cast<double>(builder.DistinctKeys()) /
                                  std::max(1.0, span));
          }
          if (!member_selective && !builder.saw_null() &&
              candidate->DistinctKeys() >= driver_rows && driver_rows >= 1000 &&
              !candidate->Has(RuntimeFilterKind::kMinMax)) {
            continue;
          }
          const bool better = !key_filter ||
                              (member_selective && !best_driver_selective) ||
                              (member_selective == best_driver_selective &&
                               (coverage < best_driver_coverage - 1e-9 ||
                                (coverage <= best_driver_coverage + 1e-9 &&
                                 driver_rows < best_driver_rows)));
          if (!better) {
            continue;
          }
          key_filter = candidate;
          key_filter_type = relations[idx].schema.GetColumn(*idx_col).Type();
          key_column = projected_col;
          best_driver_rows = driver_rows;
          best_driver_selective = member_selective;
          best_driver_coverage = coverage;
        }
      }
    }
    const bool use_key_filter = key_column.has_value() && key_filter != nullptr;
    // Resolved lazily: the binding owns the group for the duration of the
    // LoadSource call below, which outlives every use inside it.
    ScanRuntimeFilter binding;
    if (use_key_filter && context.execution_runtime() != nullptr) {
      ++context.execution_runtime()->key_filter_scans;
      context.execution_runtime()->key_filter_keys +=
          key_filter->DistinctKeys();
      if (key_filter->Has(RuntimeFilterKind::kMinMax)) {
        ++context.execution_runtime()->key_filter_minmax;
      }
      if (key_filter->Has(RuntimeFilterKind::kBloom)) {
        ++context.execution_runtime()->key_filter_bloom;
      }
      context.execution_runtime()->key_filter_bytes +=
          key_filter->MemoryBytes();
      // Remember for later scans of this base table within this statement.
      // The stash is owner-scoped so other statements never inherit it.
      // Column is stored as the full-schema slot so other projections can
      // remap.
      const auto& proj = projections[idx];
      const slot_t full_slot = proj[*key_column];
      TableKeyFilter& stored =
          context.execution_runtime()
              ->table_key_filters[TableKeyFilterKey(statement.Sources()[idx])];
      stored.filter = key_filter;
      stored.type = key_filter_type;
      stored.key_count = key_filter->DistinctKeys();
      stored.column = full_slot;
      stored.owner = &statement;
      binding.group = key_filter;
      binding.type = key_filter_type;
      binding.column = *key_column;
    } else if (context.execution_runtime() != nullptr) {
      // Fall back to a stash created earlier within this same statement (e.g.
      // an uncorrelated IN pushdown). Entries owned by other statements are
      // never applied here.
      const auto stored = context.execution_runtime()->table_key_filters.find(
          TableKeyFilterKey(statement.Sources()[idx]));
      if (stored != context.execution_runtime()->table_key_filters.end() &&
          stored->second.owner == &statement && !stored->second.empty()) {
        const auto& proj = projections[idx];
        const auto proj_it = std::ranges::find(proj, stored->second.column);
        if (proj_it != proj.end()) {
          binding.group = stored->second.filter;
          binding.type = stored->second.type;
          binding.column =
              static_cast<slot_t>(std::distance(proj.begin(), proj_it));
        }
      }
    }
    ASSIGN_OR_RETURN(
        Relation, loaded_rel,
        (LoadSource(context, statement.Sources()[idx], outer, ctes,
                    &projections[idx], &local_predicates[idx], &binding)));
    relations[idx] = std::move(loaded_rel);
    loaded[idx] = true;
  }

  // Pick the best starting table.  The naive approach (smallest row
  // count) can be suboptimal for star-like join graphs where the
  // smallest table is a leaf with only one connection.  Simulate the
  // greedy join from every candidate start and pick the one that
  // yields the lowest estimated peak intermediate row count.
  size_t first = 0;
  size_t best_peak = std::numeric_limits<size_t>::max();
  for (size_t start = 0; start < relations.size(); ++start) {
    size_t sim_current = relations[start].TotalRows();
    size_t sim_peak = sim_current;
    std::unordered_set<size_t> sim_joined{start};
    std::unordered_set<size_t> sim_remaining;
    for (size_t i = 0; i < relations.size(); ++i) {
      if (i != start) {
        sim_remaining.insert(i);
      }
    }
    while (!sim_remaining.empty()) {
      size_t sim_next = *sim_remaining.begin();
      size_t sim_est = std::numeric_limits<size_t>::max();
      bool sim_next_connected = false;
      for (size_t cand : sim_remaining) {
        std::unordered_set<size_t> after = sim_joined;
        after.insert(cand);
        size_t app_count = 0;
        for (const PredicateInfo& p : predicates) {
          if (!p.resolved || p.contains_query || p.sources.size() < 2 ||
              !p.sources.contains(cand) || !IsSubset(p.sources, after)) {
            continue;
          }
          ++app_count;
        }
        const bool connected = app_count > 0;
        const size_t est =
            connected ? std::min(sim_current, relations[cand].TotalRows())
                      : sim_current * relations[cand].TotalRows();
        const bool cheaper =
            est < sim_est ||
            (est == sim_est &&
             relations[cand].TotalRows() < relations[sim_next].TotalRows());
        if ((connected && !sim_next_connected) ||
            (connected == sim_next_connected && cheaper)) {
          sim_next = cand;
          sim_est = est;
          sim_next_connected = connected;
        }
      }
      sim_peak = std::max(sim_peak, sim_current);
      sim_peak = std::max(sim_peak, relations[sim_next].TotalRows());
      sim_current = sim_est;
      sim_joined.insert(sim_next);
      sim_remaining.erase(sim_next);
    }
    if (sim_peak < best_peak) {
      best_peak = sim_peak;
      first = start;
    }
  }
  Relation result = std::move(relations[first]);
  std::unordered_set<size_t> joined{first};
  std::unordered_set<size_t> remaining;
  for (size_t i = 0; i < relations.size(); ++i) {
    if (i != first) {
      remaining.insert(i);
    }
  }

  // TINYLAMB_LEGACY_PLANNER=1 restores the per-call estimation behavior
  // (fresh maps, exact full-relation sweeps) for A/B comparisons.
  static const bool legacy_planner =
      std::getenv("TINYLAMB_LEGACY_PLANNER") != nullptr;
  std::unordered_map<const Relation*, JoinKeyFrequencyMaps> frequency_cache;
  while (!remaining.empty()) {
    size_t next = *remaining.begin();
    size_t next_estimate = std::numeric_limits<size_t>::max();
    bool next_connected = false;
    for (size_t candidate : remaining) {
      std::unordered_set<size_t> after = joined;
      after.insert(candidate);
      std::vector<Expression> applicable;
      for (const PredicateInfo& predicate : predicates) {
        if (!predicate.resolved || predicate.contains_query ||
            predicate.sources.size() < 2 ||
            !predicate.sources.contains(candidate) ||
            !IsSubset(predicate.sources, after)) {
          continue;
        }
        applicable.push_back(predicate.expression);
        // Note: mark_applied is intentionally NOT called here during
        // candidate evaluation.  Calling it would remove predicates
        // from join_residual for unselected candidates, corrupting
        // the residual filter.  We only mark after the winner is known.
      }
      const auto estimate_begin = std::chrono::steady_clock::now();
      const size_t estimate =
          legacy_planner
              ? EstimateJoinRows(result, relations[candidate], applicable)
              : EstimateJoinRowsCached(result, relations[candidate], applicable,
                                       &frequency_cache);
      join_estimate_ms += ElapsedMs(estimate_begin);
      const bool connected = !applicable.empty();
      const bool cheaper =
          estimate < next_estimate ||
          (estimate == next_estimate &&
           relations[candidate].TotalRows() < relations[next].TotalRows());
      if ((connected && !next_connected) ||
          (connected == next_connected && cheaper)) {
        next = candidate;
        next_estimate = estimate;
        next_connected = connected;
      }
    }

    std::unordered_set<size_t> after = joined;
    after.insert(next);
    std::vector<Expression> applicable;
    for (const PredicateInfo& predicate : predicates) {
      if (!predicate.resolved || predicate.contains_query ||
          predicate.sources.size() < 2 || !predicate.sources.contains(next) ||
          !IsSubset(predicate.sources, after)) {
        continue;
      }
      applicable.push_back(predicate.expression);
      mark_applied(predicate.expression);
    }
    // Final join step of a fusable grouped statement: feed the matched pairs
    // straight into the aggregation instead of materializing the joined
    // relation.  Everything was already filtered locally, so no join output
    // ever exists.  A nullopt (shape not fusable) falls through to the
    // materializing join below.
    if (std::getenv("TINYLAMB_FUSION_DEBUG") != nullptr) {
      // The spec is null for statements that never reached the shape gate;
      // reading `fusion->eligible` here would fault (oracle: any grouped
      // query with TINYLAMB_FUSION_DEBUG set).
      std::cerr << "[fusion] greedy hook: eligible="
                << (fusion != nullptr && fusion->eligible)
                << " fully_applied=" << *where_fully_applied
                << " last=" << (remaining.size() == 1)
                << " residual=" << join_residual.size() << "\n";
    }
    if (fusion != nullptr && fusion->eligible && *where_fully_applied &&
        remaining.size() == 1 && join_residual.empty()) {
      ASSIGN_OR_RETURN(
          std::optional<Relation>, fused,
          (TryFusedGroupedJoin(context, result, relations[next], applicable,
                               JoinType::kInner, outer, ctes, fusion)));
      if (fused.has_value()) {
        result = std::move(*fused);
        fusion->fired = true;
        joined.insert(next);
        remaining.erase(next);
        break;
      }
    }
    ASSIGN_OR_RETURN(
        Relation, result_r,
        (InnerJoin(context, std::move(result), std::move(relations[next]),
                   applicable, outer, ctes)));
    result = std::move(result_r);
    joined.insert(next);
    remaining.erase(next);
  }
  if (!join_residual.empty()) {
    // Subquery-bearing or constant-only ON conjuncts never made it into a
    // scan filter or join key; evaluate them over the joined rows now so the
    // inner-join semantics stay intact.
    RETURN_IF_FAIL(
        FilterRelation(context, &result, join_residual, outer, ctes));
  }
  if (context.execution_runtime() != nullptr) {
    context.execution_runtime()->join_estimate_ms += join_estimate_ms;
    context.execution_runtime()->planning_ms +=
        ElapsedMs(planning_begin) - join_estimate_ms;
  }
  return result;
}

}  // namespace tinylamb::relational_detail
