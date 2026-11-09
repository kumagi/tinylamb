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
#include "common/platform/hash.hpp"
#include "common/query_parallelism.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/fused_grouped_join.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/hash_join_mode.hpp"
#include "executor/query_memory.hpp"
#include "executor/spill_file.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/case_expression.hpp"
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
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/collation.hpp"
#include "type/column_name.hpp"
#include "type/date.hpp"
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
  if (name.name == only.name &&
      (name.schema.empty() || name.schema == only.schema)) {
    return false;
  }
  if (name.schema.empty()) {
    return false;
  }
  return IdentifierEquals(only.schema, name.schema);
}

std::vector<size_t> MatchingColumnOffsets(const Schema& schema,
                                          const std::string& name) {
  std::vector<size_t> offsets;
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    if (IdentifierEquals(schema.GetColumn(i).Name().name, name)) {
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
         IdentifierEquals(lhs.name, rhs.name) &&
         ContainsIdentifier(using_columns, lhs.name);
}

}  // namespace

namespace {

bool IsTargetColumn(const ColumnName& column,
                    const std::unordered_set<std::string>& relations) {
  return column.schema.empty() ? relations.contains(std::string())
                               : relations.contains(column.schema);
}

bool ExpressionRejectsNullsOnRelations(
    const Expression& expression,
    const std::unordered_set<std::string>& relations) {
  return ExpressionRejectsNullsWhere(
      expression,
      [&](const ColumnName& col) { return IsTargetColumn(col, relations); },
      /*bare_column_rejects_null=*/true);
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

}  // namespace

// Canonical text for a column pair so `a.x = b.y` and `b.y = a.x` share one
// identity across branches and dedup sets.
std::string EqualityPairKey(const ColumnName& lhs, const ColumnName& rhs) {
  const std::string left = lhs.ToString();
  const std::string right = rhs.ToString();
  return left < right ? left + '\x1f' + right : right + '\x1f' + left;
}

namespace {

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

}  // namespace

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

namespace {

bool MapsToExactEqualityKey(const Schema& left, const Schema& right,
                            const Expression& predicate) {
  if (!IsColumnEqualityPredicate(predicate)) {
    return false;
  }
  const BinaryExpression& binary = predicate->AsBinaryExpression();
  if (binary.Op() != BinaryOperation::kEquals) {
    return false;
  }
  const ColumnName& lhs = binary.Left()->AsColumnValue().GetColumnName();
  const ColumnName& rhs = binary.Right()->AsColumnValue().GetColumnName();
  if (IsVirtualValueTableField(left, lhs) ||
      IsVirtualValueTableField(right, lhs) ||
      IsVirtualValueTableField(left, rhs) ||
      IsVirtualValueTableField(right, rhs)) {
    return false;
  }
  const auto lhs_left = LocalColumnOffset(left, lhs);
  const auto lhs_right = LocalColumnOffset(right, lhs);
  const auto rhs_left = LocalColumnOffset(left, rhs);
  const auto rhs_right = LocalColumnOffset(right, rhs);
  std::optional<size_t> left_col;
  std::optional<size_t> right_col;
  if (lhs_left && rhs_right && !lhs_right && !rhs_left) {
    left_col = lhs_left;
    right_col = rhs_right;
  } else if (rhs_left && lhs_right && !rhs_right && !lhs_left) {
    left_col = rhs_left;
    right_col = lhs_right;
  } else {
    return false;
  }
  const Column& lc = left.GetColumn(*left_col);
  const Column& rc = right.GetColumn(*right_col);
  if (lc.Type() != rc.Type() || lc.IsUnsigned() || rc.IsUnsigned()) {
    return false;
  }
  return lc.Type() == ValueType::kInt64 || lc.Type() == ValueType::kDate;
}

}  // namespace

bool HasMixedKeySignednessOrType(const Relation& left, const Relation& right,
                                 const std::vector<EqualityKey>& keys) {
  const auto check_side = [](const Relation& rel, bool is_left,
                             const std::vector<EqualityKey>& eq_keys) {
    if (rel.HasSpill() || rel.rows.empty()) {
      return false;
    }
    bool needs_check = rel.rows.size() <= 4096;
    if (!needs_check) {
      for (const EqualityKey& key : eq_keys) {
        const size_t col = is_left ? key.left : key.right;
        if (col < rel.schema.ColumnCount() &&
            rel.schema.GetColumn(col).Name().schema.empty()) {
          needs_check = true;
          break;
        }
      }
    }
    if (!needs_check) {
      return false;
    }
    for (const EqualityKey& key : eq_keys) {
      const size_t col = is_left ? key.left : key.right;
      if (col >= rel.schema.ColumnCount()) {
        return true;
      }
      const Column& expected = rel.schema.GetColumn(col);
      for (const Row& row : rel.rows) {
        if (col >= row.values_.size()) {
          return true;
        }
        const Value& v = row.values_[col];
        if (v.IsNull()) {
          continue;
        }
        if (v.type != expected.Type() ||
            v.IsUnsigned() != expected.IsUnsigned()) {
          return true;
        }
      }
    }
    return false;
  };
  return check_side(left, true, keys) || check_side(right, false, keys);
}

std::vector<Expression> ResidualJoinPredicates(
    const Schema& left, const Schema& right,
    const std::vector<Expression>& predicates) {
  const std::vector<EqualityKey> keys = EqualityKeys(left, right, predicates);
  const bool can_elide_exact =
      !keys.empty() && keys.size() <= 4 &&
      std::ranges::all_of(keys, [&](const EqualityKey& key) {
        if (key.null_safe || key.left_offset != 0 || key.right_offset != 0 ||
            key.left >= left.ColumnCount() ||
            key.right >= right.ColumnCount()) {
          return false;
        }
        const Column& lc = left.GetColumn(key.left);
        const Column& rc = right.GetColumn(key.right);
        return lc.Type() == rc.Type() && !lc.IsUnsigned() && !rc.IsUnsigned() &&
               (lc.Type() == ValueType::kInt64 ||
                lc.Type() == ValueType::kDate);
      });
  std::vector<Expression> residual;
  residual.reserve(predicates.size());
  for (const Expression& predicate : predicates) {
    if (can_elide_exact && MapsToExactEqualityKey(left, right, predicate)) {
      continue;
    }
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

bool CompositeIntJoinKey(const Schema& left_schema,
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

CompositeIntKey EncodeCompositeIntKey(const Row& row,
                                      const std::vector<slot_t>& columns,
                                      const std::vector<int64_t>* offsets) {
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

// Unlike ordinary equality joins, NULL is a legitimate key for
// IS NOT DISTINCT FROM. Encode the NULL/non-NULL state explicitly instead of
// passing Value::kNull to the regular mem-comparable encoder, which correctly
// rejects untyped NULL for ordinary hash joins.

std::string EncodeNullSafeJoinKey(const Row& row,
                                  const std::vector<slot_t>& columns) {
  return EncodeJoinKeyInternal(row, columns, true, false, nullptr);
}

}  // namespace

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

namespace {

void BuildJoinKeyFrequencyMaps(const Relation& right,
                               const std::vector<slot_t>& right_columns,
                               bool single_int, bool composite_int,
                               JoinKeyFrequencyMaps* maps) {
  maps->single_int = single_int;
  maps->composite_int = composite_int;
  maps->right_columns = right_columns;
  maps->single_int_frequencies = FlatIntKeyMap<size_t>{};
  maps->frequencies.clear();
  maps->int_frequencies.clear();
  static const size_t kBuildSampleTarget = [] {
    // A/B hatch: TINYLAMB_LEGACY_ESTIMATE=1 restores the 2M-row sampling
    // targets (finer estimates, slower planning sweeps).
    const char* legacy = std::getenv("TINYLAMB_LEGACY_ESTIMATE");
    return legacy != nullptr ? size_t{2'000'000} : size_t{16'384};
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
    maps->single_int_frequencies.Reserve(sample_rows);
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
      bool inserted = false;
      size_t* slot = maps->single_int_frequencies.TryEmplace(
          IntegerJoinKey(row, right_columns[0]), &inserted);
      *slot = inserted ? 1 : *slot + 1;
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
    return legacy != nullptr ? size_t{2'000'000} : size_t{16'384};
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
      if (const size_t* found = maps.single_int_frequencies.Find(
              IntegerJoinKey(row, left_columns[0]));
          found != nullptr) {
        estimate += *found * maps.build_stride;
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
size_t EstimateJoinRowsCached(
    const Relation& left, const Relation& candidate,
    const std::vector<Expression>& predicates,
    std::unordered_map<const Relation*, std::vector<JoinKeyFrequencyMaps>>*
        cache) {
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
  std::vector<JoinKeyFrequencyMaps>& entries = (*cache)[&candidate];
  for (const JoinKeyFrequencyMaps& entry : entries) {
    if (entry.single_int == single_int &&
        entry.composite_int == composite_int &&
        entry.right_columns == right_columns) {
      return ProbeJoinEstimate(left, left_columns, entry);
    }
  }
  entries.emplace_back();
  BuildJoinKeyFrequencyMaps(candidate, right_columns, single_int, composite_int,
                            &entries.back());
  return ProbeJoinEstimate(left, left_columns, entries.back());
}

size_t SpillPartitionOf(const std::string& key, size_t partitions) {
  // Platform-stable: partition assignment decides the order spilled rows come
  // back in, which must not depend on the standard library's std::hash.
  return static_cast<size_t>(platform::StableHashBytes(key) % partitions);
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

  RETURN_IF_FAIL(left.FinishSpill());
  RETURN_IF_FAIL(right.FinishSpill());

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

  Status spill_err{Status::kSuccess};
  RETURN_IF_FAIL(right.ForEachRow([&](const Row& row) {
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
      const Status st = right_parts[part].Append(row);
      if (st != Status::kSuccess && spill_err == Status::kSuccess) {
        spill_err = st;
      }
    }
  }));
  RETURN_IF_FAIL(spill_err);
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

  RETURN_IF_FAIL(left.ForEachRow([&](const Row& left_row) {
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
      const Status st = left_parts[part].Append(left_row);
      if (st != Status::kSuccess && spill_err == Status::kSuccess) {
        spill_err = st;
      }
    }
  }));
  RETURN_IF_FAIL(spill_err);
  left.rows.clear();
  left.rows.shrink_to_fit();
  left.ReleaseCharge();

  int_buckets.clear();
  str_buckets.clear();
  const bool resident_folded = !right_parts[0].Empty();
  if (resident_folded) {
    for (const Row& row : resident_right) {
      RETURN_IF_FAIL(right_parts[0].Append(row));
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
    RETURN_IF_FAIL(left_parts[part].FinishWriting());
    RETURN_IF_FAIL(right_parts[part].FinishWriting());
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
    RETURN_IF_FAIL(left_parts[part].ForEachRow([&](const Row& left_row) {
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
    }));
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
  RETURN_IF_FAIL(result.FinishSpill());
  RETURN_IF_FAIL(result.GetStatus());
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
constexpr size_t kParallelProbeMinLeftRows = 16384;
constexpr size_t kParallelProbeChunk = 8192;
constexpr size_t kParallelProbeMaxWorkers = 16;

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
  std::vector<Relation> shards(worker_count);
  for (size_t w = 0; w < worker_count; ++w) {
    shards[w].set_runtime(result->runtime());
    shards[w].schema = result->schema;
  }
  // Cache-line padded: every worker bumps its own counter once per emitted
  // match, so adjacent unpadded slots would false-share on the hot line.
  struct alignas(64) PaddedComparison {
    size_t value{0};
  };
  std::vector<PaddedComparison> comparisons(worker_count);
  std::mutex error_mutex;
  Status first_error{Status::kSuccess};

  auto run_worker = [&](size_t worker_id) {
    Relation& local = shards[worker_id];
    for (size_t chunk = worker_id; chunk < num_chunks; chunk += worker_count) {
      try {
        const size_t begin = chunk * kParallelProbeChunk;
        const size_t end = std::min(total, begin + kParallelProbeChunk);
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
              local.AddRow(ConcatRows(left_row, Row(std::move(nulls))));
            }
            continue;
          }
          bool matched = false;
          const auto [eq_begin, eq_end] = buckets.equal_range(key);
          for (auto iter = eq_begin; iter != eq_end; ++iter) {
            ++comparisons[worker_id].value;
            Row combined = stream_is_left ? ConcatRows(left_row, *iter->second)
                                          : ConcatRows(*iter->second, left_row);
            local.AddRow(std::move(combined));
            matched = true;
          }
          if (!matched && left_outer_emit) {
            std::vector<Value> nulls(right_width);
            local.AddRow(ConcatRows(left_row, Row(std::move(nulls))));
          }
        }
      } catch (const std::exception& error) {
        std::scoped_lock error_lock(error_mutex);
        if (first_error.ok()) {
          first_error = StatusError(StatusCode::kRuntimeError, error.what());
        }
        return;
      } catch (...) {
        std::scoped_lock error_lock(error_mutex);
        if (first_error.ok()) {
          first_error =
              StatusError(StatusCode::kRuntimeError, "parallel probe failed");
        }
        return;
      }
    }
  };

  if (worker_count <= 1) {
    run_worker(0);
  } else {
    std::vector<std::jthread> workers;
    workers.reserve(worker_count - 1);
    for (size_t i = 1; i < worker_count; ++i) {
      workers.emplace_back([&, i] { run_worker(i); });
    }
    run_worker(0);
  }
  size_t total_in_memory = 0;
  for (size_t w = 0; w < worker_count; ++w) {
    if (!shards[w].HasSpill()) {
      total_in_memory += shards[w].rows.size();
    }
  }
  if (total_in_memory > 0) {
    result->rows.reserve(result->rows.size() + total_in_memory);
  }
  for (size_t w = 0; w < worker_count; ++w) {
    const Status fs = shards[w].FinishSpill();
    if (first_error.ok() && fs != Status::kSuccess) {
      first_error = fs;
    }
    if (shards[w].HasSpill()) {
      const Status it =
          shards[w].ForEachRow([&](const Row& row) { result->AddRow(row); });
      if (first_error.ok() && it != Status::kSuccess) {
        first_error = it;
      }
    } else {
      result->MergeInMemoryShard(std::move(shards[w]));
    }
    const Status gs = shards[w].GetStatus();
    if (first_error.ok() && gs != Status::kSuccess) {
      first_error = gs;
    }
    shards[w].ResetContents();
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
      const std::string lowered = AsciiToLower(name);
      if (!ContainsIdentifier(merged_using, lowered)) {
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
    const bool declared_now = ContainsIdentifier(source.using_columns, name);
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
  RETURN_IF_FAIL(right.FinishSpill());
  RETURN_IF_FAIL(left.GetStatus());
  RETURN_IF_FAIL(right.GetStatus());
  const Row* right_base = right.rows.empty() ? nullptr : right.rows.data();
  std::vector<char> right_matched(want_right_nulls ? right.TotalRows() : 0, 0);
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
         !ContainsIdentifier(*left.using_columns, name))) {
      return StatusError(StatusCode::kAmbiguousQuery,
                         "ambiguous USING column: " + name);
    }
    if (right_columns.size() > 1 &&
        (right.using_columns == nullptr ||
         !ContainsIdentifier(*right.using_columns, name))) {
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
      HasMixedKeySignednessOrType(left, right, equality_keys)
          ? ordinary_predicates
          : ResidualJoinPredicates(left.schema, right.schema,
                                   ordinary_predicates);
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
    RETURN_IF_FAIL(left.FinishSpill());
    RETURN_IF_FAIL(right.FinishSpill());
    RETURN_IF_FAIL(left.ForEachRow([&](const Row& left_row) {
      bool matched = false;
      size_t right_idx = 0;
      const Status st = right.ForEachRow([&](const Row& right_row) {
        ++result.join_comparisons;
        Row combined = ConcatRows(left_row, right_row);
        if (matches(combined)) {
          if (want_right_nulls && right_idx < right_matched.size()) {
            right_matched[right_idx] = 1;
          }
          result.AddRow(std::move(combined));
          matched = true;
        }
        ++right_idx;
      });
      if (st != Status::kSuccess && residual_error == Status::kSuccess) {
        residual_error = st;
      }
      if (!matched) {
        emit_unmatched(left_row);
      }
    }));
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
    size_t index = 0;
    RETURN_IF_FAIL(right.ForEachRow([&](const Row& right_row) {
      if (index < right_matched.size() && right_matched[index] == 0) {
        std::vector<Value> nulls(left.schema.ColumnCount());
        result.AddRow(ConcatRows(Row(std::move(nulls)), right_row));
      }
      ++index;
    }));
  }
  RETURN_IF_FAIL(result.FinishSpill());
  RETURN_IF_FAIL(result.GetStatus());
  result.peak_intermediate_rows =
      std::max(result.peak_intermediate_rows, result.TotalRows());
  if (context.execution_runtime() != nullptr) {
    context.execution_runtime()->join_ms += ElapsedMs(join_begin);
  }
  if (residual_error == Status::kSuccess && result.join_comparisons == 0 &&
      !residual.empty()) {
    Row dummy_row(std::vector<Value>(result.schema.ColumnCount()));
    Scope scope{.row = &dummy_row, .schema = &result.schema, .outer = outer};
    scope.using_columns = result.using_columns.get();
    const auto validate_columns = [&](const auto& self,
                                      const Expression& expr) -> Status {
      if (!expr || expr->Type() == TypeTag::kQueryExp) {
        return Status::kSuccess;
      }
      if (expr->Type() == TypeTag::kColumnValue) {
        const ColumnName& col = expr->AsColumnValue().GetColumnName();
        if (col.name != "*") {
          StatusOr<Value> val = TryLookup(col, scope);
          if (!val.HasValue()) {
            return val.GetStatus();
          }
        }
        return Status::kSuccess;
      }
      for (const Expression& child : ExpressionChildren(expr)) {
        RETURN_IF_FAIL(self(self, child));
      }
      return Status::kSuccess;
    };
    for (const Expression& predicate : residual) {
      const Status st = validate_columns(validate_columns, predicate);
      if (st != Status::kSuccess) {
        residual_error = st;
        break;
      }
    }
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
      HasMixedKeySignednessOrType(left, right, equality_keys)
          ? predicates
          : ResidualJoinPredicates(left.schema, right.schema, predicates);

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
    RETURN_IF_FAIL(left.FinishSpill());
    RETURN_IF_FAIL(right.FinishSpill());
    RETURN_IF_FAIL(left.ForEachRow([&](const Row& left_row) {
      const Status st = right.ForEachRow([&](const Row& right_row) {
        ++result.join_comparisons;
        Row combined = ConcatRows(left_row, right_row);
        if (matches(combined)) {
          result.AddRow(std::move(combined));
        }
      });
      if (st != Status::kSuccess && residual_error == Status::kSuccess) {
        residual_error = st;
      }
    }));
  } else {
    ++result.hash_joins;
    const bool has_null_safe_key = std::ranges::any_of(
        equality_keys, [](const EqualityKey& key) { return key.null_safe; });
    // Choose the build before deciding whether to partition. A spilled
    // probe does not require repartitioning when the smaller build still fits:
    // ForEachRow streams its resident prefix and spill files exactly once.
    static const bool flip_disabled =
        std::getenv("TINYLAMB_DISABLE_JOIN_FLIP") != nullptr;
    const bool flip = !flip_disabled && !has_null_safe_key &&
                      right.TotalRows() >= kFlipMinBuildRows &&
                      right.TotalRows() > left.TotalRows() * kFlipRatio &&
                      !left.HasSpill();
    const Relation& build_side = flip ? left : right;
    const Relation& probe_side = flip ? right : left;
    const size_t index_bytes = build_side.TotalRows() * 64;
    const bool hybrid = build_side.HasSpill() ||
                        !QueryMemoryBudget::Global().CanReserve(index_bytes);
    // D8 (docs/design.md): null-safe keys use the hybrid operator too, so a
    // spilled input is never silently reduced to its resident prefix.
    // Biased keys degrade to residuals here: the hybrid operator cannot
    // bias, and the residual verifier keeps results exact.
    if (std::ranges::any_of(equality_keys,
                            [](const EqualityKey& key) {
                              return key.left_offset != 0 ||
                                     key.right_offset != 0;
                            }) &&
        (has_null_safe_key || hybrid)) {
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
    if (hybrid) {
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
    const std::vector<slot_t>& build_columns =
        flip ? left_columns : right_columns;
    const std::vector<slot_t>& probe_columns =
        flip ? right_columns : left_columns;
    const std::vector<int64_t>& build_key_offsets =
        flip ? left_key_offsets : right_key_offsets;
    const std::vector<int64_t>& probe_key_offsets =
        flip ? right_key_offsets : left_key_offsets;
    const bool can_parallelize =
        residual.empty() && !has_null_safe_key && !probe_side.HasSpill() &&
        probe_side.rows.size() >= kParallelProbeMinLeftRows;
    const size_t probe_workers =
        can_parallelize ? std::min<size_t>(kParallelProbeMaxWorkers,
                                           std::thread::hardware_concurrency())
                        : 1;
    const bool integer_key = !has_null_safe_key &&
                             SingleIntegerJoinKey(left.schema, left_columns) &&
                             SingleIntegerJoinKey(right.schema, right_columns);
    QueryMemoryCharge index_charge(index_bytes);
    if (auto* runtime = context.execution_runtime()) {
      runtime->inner_hash_build_rows += build_side.TotalRows();
      runtime->inner_hash_index_estimate_bytes += index_bytes;
      runtime->inner_hash_left_builds += flip ? 1 : 0;
      runtime->inner_hash_spilled_probe_rows +=
          probe_side.HasSpill() ? probe_side.TotalRows() : 0;
    }
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
        RETURN_IF_FAIL(probe_side.ForEachRow([&](const Row& probe_row) {
          if (HasNullKey(probe_row, probe_columns)) {
            return;
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
        }));
      }
    } else if (!has_null_safe_key &&
               CompositeIntJoinKey(left.schema, left_columns, right.schema,
                                   right_columns)) {
      constexpr auto kInvalidPacked2x32 = static_cast<int64_t>(
          (static_cast<uint64_t>(0x80000000ULL) << 32) | 0x80000000ULL);
      auto fits_i32 = [](int64_t v) noexcept {
        return v >= static_cast<int64_t>(std::numeric_limits<int32_t>::min()) &&
               v <= static_cast<int64_t>(std::numeric_limits<int32_t>::max());
      };
      auto pack_2x32 = [](int64_t k0, int64_t k1) noexcept -> int64_t {
        return static_cast<int64_t>(
            (static_cast<uint64_t>(
                 static_cast<uint32_t>(static_cast<int32_t>(k0)))
             << 32) |
            static_cast<uint64_t>(
                static_cast<uint32_t>(static_cast<int32_t>(k1))));
      };
      bool can_pack_2x32 = (build_columns.size() == 2);
      if (can_pack_2x32) {
        for (const Row& row : build_side.rows) {
          if (HasNullKey(row, build_columns)) {
            continue;
          }
          const int64_t k0 = AddJoinKeyBias(
              IntegerJoinKey(row, build_columns[0]), build_key_offsets[0]);
          const int64_t k1 = AddJoinKeyBias(
              IntegerJoinKey(row, build_columns[1]), build_key_offsets[1]);
          if (!fits_i32(k0) || !fits_i32(k1) ||
              pack_2x32(k0, k1) == kInvalidPacked2x32) {
            can_pack_2x32 = false;
            break;
          }
        }
      }
      if (can_pack_2x32) {
        FlatJoinBuckets<int64_t> buckets;
        buckets.reserve(build_side.rows.size());
        for (const Row& row : build_side.rows) {
          if (HasNullKey(row, build_columns)) {
            continue;
          }
          const int64_t k0 = AddJoinKeyBias(
              IntegerJoinKey(row, build_columns[0]), build_key_offsets[0]);
          const int64_t k1 = AddJoinKeyBias(
              IntegerJoinKey(row, build_columns[1]), build_key_offsets[1]);
          buckets.emplace(pack_2x32(k0, k1), &row);
        }
        auto encode_probe_2x32 = [&](const Row& row) noexcept -> int64_t {
          if (HasNullKey(row, probe_columns)) {
            return kInvalidPacked2x32;
          }
          const int64_t k0 = AddJoinKeyBias(
              IntegerJoinKey(row, probe_columns[0]), probe_key_offsets[0]);
          const int64_t k1 = AddJoinKeyBias(
              IntegerJoinKey(row, probe_columns[1]), probe_key_offsets[1]);
          if (!fits_i32(k0) || !fits_i32(k1)) {
            return kInvalidPacked2x32;
          }
          return pack_2x32(k0, k1);
        };
        if (can_parallelize) {
          RETURN_IF_FAIL(ParallelHashProbe(
              buckets, probe_side, probe_columns, encode_probe_2x32, false,
              build_side.schema.ColumnCount(), probe_workers, &result,
              &result.join_comparisons, !flip));
          RETURN_IF_FAIL(residual_error);
        } else {
          RETURN_IF_FAIL(probe_side.ForEachRow([&](const Row& probe_row) {
            if (HasNullKey(probe_row, probe_columns)) {
              return;
            }
            const int64_t key = encode_probe_2x32(probe_row);
            if (key == kInvalidPacked2x32) {
              return;
            }
            const auto [begin, end] = buckets.equal_range(key);
            for (auto iter = begin; iter != end; ++iter) {
              ++result.join_comparisons;
              Row combined = flip ? ConcatRows(*iter->second, probe_row)
                                  : ConcatRows(probe_row, *iter->second);
              if (matches(combined)) {
                result.AddRow(std::move(combined));
              }
            }
          }));
        }
      } else {
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
          RETURN_IF_FAIL(probe_side.ForEachRow([&](const Row& probe_row) {
            if (HasNullKey(probe_row, probe_columns)) {
              return;
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
          }));
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
        RETURN_IF_FAIL(probe_side.ForEachRow([&](const Row& probe_row) {
          if (!has_null_safe_key && HasNullKey(probe_row, probe_columns)) {
            return;
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
        }));
      }
    }
  }
  RETURN_IF_FAIL(result.FinishSpill());
  RETURN_IF_FAIL(result.GetStatus());
  result.peak_intermediate_rows =
      std::max(result.peak_intermediate_rows, result.TotalRows());
  if (context.execution_runtime() != nullptr) {
    context.execution_runtime()->join_ms += ElapsedMs(join_begin);
  }
  if (residual_error == Status::kSuccess && result.join_comparisons == 0 &&
      !residual.empty()) {
    Row dummy_row(std::vector<Value>(result.schema.ColumnCount()));
    Scope scope{.row = &dummy_row, .schema = &result.schema, .outer = outer};
    scope.using_columns = result.using_columns.get();
    const auto validate_columns = [&](const auto& self,
                                      const Expression& expr) -> Status {
      if (!expr || expr->Type() == TypeTag::kQueryExp) {
        return Status::kSuccess;
      }
      if (expr->Type() == TypeTag::kColumnValue) {
        const ColumnName& col = expr->AsColumnValue().GetColumnName();
        if (col.name != "*") {
          StatusOr<Value> val = TryLookup(col, scope);
          if (!val.HasValue()) {
            return val.GetStatus();
          }
        }
        return Status::kSuccess;
      }
      for (const Expression& child : ExpressionChildren(expr)) {
        RETURN_IF_FAIL(self(self, child));
      }
      return Status::kSuccess;
    };
    for (const Expression& predicate : residual) {
      const Status st = validate_columns(validate_columns, predicate);
      if (st != Status::kSuccess) {
        residual_error = st;
        break;
      }
    }
  }
  RETURN_IF_FAIL(residual_error);
  return result;
}

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

}  // namespace tinylamb::relational_detail
