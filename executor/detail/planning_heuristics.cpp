/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/planning_heuristics.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
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
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/parallel_work.hpp"
#include "common/platform/endian.hpp"
#include "common/platform/hash.hpp"
#include "common/platform/int128_conversion.hpp"
#include "common/platform/process.hpp"
#include "common/query_parallelism.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/fused_grouped_join.hpp"
#include "executor/detail/planning_columnar_keys.hpp"
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
#include "expression/jit.hpp"
#include "expression/named_expression.hpp"
#include "expression/parameter_slot.hpp"
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
  offsets.reserve(schema.ColumnCount());
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
  relations.reserve(expression->TouchedColumns().size());
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
  std::vector<std::vector<Expression>> conjuncts;
  conjuncts.reserve(branches.size());
  for (const Expression& branch : branches) {
    conjuncts.push_back(SplitConjuncts(branch));
  }

  // All this needs to know is whether a conjunct touches a column outside
  // `source`. Resolving that with AnalyzePredicates repeats the case-
  // insensitive schema scan (LocalColumnOffset / IsVirtualValueTableField)
  // for every branch and every conjunct. Resolve each distinct column once
  // instead: a column is source-local only when `source` is its unique owner.
  // An owner shared with another relation, or no owner at all, makes every
  // predicate touching it non-local (same resolved/sources outcome as
  // AnalyzePredicates).
  std::unordered_map<ColumnName, bool> local_to_source;
  for (const std::vector<Expression>& branch : conjuncts) {
    for (const Expression& conjunct : branch) {
      if (!conjunct) {
        continue;
      }
      for (const ColumnName& column : conjunct->TouchedColumns()) {
        if (local_to_source.contains(column)) {
          continue;
        }
        bool owned_by_source = false;
        bool ambiguous = false;
        for (size_t i = 0; i < relations.size(); ++i) {
          if (!LocalColumnOffset(relations[i].schema, column) &&
              !IsVirtualValueTableField(relations[i].schema, column)) {
            continue;
          }
          if (i == source) {
            owned_by_source = true;
          } else {
            ambiguous = true;
            break;
          }
        }
        local_to_source.emplace(column, owned_by_source && !ambiguous);
      }
    }
  }

  std::vector<Expression> local_branches;
  local_branches.reserve(branches.size());
  for (const std::vector<Expression>& branch : conjuncts) {
    std::vector<Expression> local;
    local.reserve(branch.size());
    for (const Expression& conjunct : branch) {
      if (!conjunct || ContainsQuery(conjunct)) {
        continue;
      }
      bool local_conjunct = true;
      bool touched_any = false;
      for (const ColumnName& column : conjunct->TouchedColumns()) {
        touched_any = true;
        const auto it = local_to_source.find(column);
        if (it == local_to_source.end() || !it->second) {
          local_conjunct = false;
          break;
        }
      }
      // A constant conjunct touches no column and cannot attribute the
      // disjunction to this source (AnalyzePredicates gave it no sources).
      if (local_conjunct && touched_any) {
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
  if (constant.type != ValueType::kInt64 || constant.IsUnsigned()) {
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

// Bias arithmetic is exact only for signed INT64 operands on both sides.
// UINT64 mixed arithmetic can promote to DOUBLE and lose low bits.
bool BiasedKeyTypesSupported(const Schema& schema, size_t column) {
  const ValueType type = schema.GetColumn(column).Type();
  return type == ValueType::kInt64 && !schema.GetColumn(column).IsUnsigned();
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
  common.reserve(per_branch.front().size());
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

std::vector<std::pair<ColumnName, ColumnName>> NecessaryDisjunctEqualities(
    const Expression& predicate) {
  if (!SafeToReduceEvaluationCount(predicate)) {
    return {};
  }
  return DisjunctCommonEqualities(predicate);
}

// Builds synthetic two-source equality predicates for every disjunctive
// conjunct in `analyzed`: the pairs `DisjunctCommonEqualities` mines become
// plain `col = col` PredicateInfo that the join planner treats exactly like
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
  Schema all_schema;
  for (const Relation& rel : relations) {
    all_schema = all_schema + rel.schema;
  }
  for (const PredicateInfo& predicate : analyzed) {
    if (!predicate.resolved || predicate.contains_query ||
        predicate.sources.size() < 2 ||
        !SafeToReduceEvaluationCountWithSchema(predicate.expression,
                                               all_schema)) {
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
                            const Expression& predicate,
                            bool allow_binary_varchar = false) {
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
  if (lc.Type() == ValueType::kInt64 || lc.Type() == ValueType::kDate) {
    return true;
  }
  return allow_binary_varchar && lc.Type() == ValueType::kVarChar &&
         !lc.IsInterval() && !rc.IsInterval() &&
         lc.GetTextDomain() == rc.GetTextDomain();
}

}  // namespace

namespace {

// A VARCHAR's collation can live on the value without being declared by the
// schema. A raw-byte hash key is unsafe if either input carries a collation.
StatusOr<bool> HasCollatedJoinColumn(const Relation& relation, size_t column) {
  // `uniform_cell_types` only promises the declared ValueType for non-NULL
  // cells; inspect the actual cells so a COLLATE tag can never be missed.
  if (relation.schema.GetColumn(column).Type() != ValueType::kVarChar) {
    return false;
  }
  if (relation.HasColumnarOnly()) {
    Value cell;
    for (const auto& chunk : relation.columnar->chunks) {
      if (chunk.rows == 0) {
        continue;
      }
      // T4593: a projected chunk can be narrower than the schema; it simply
      // carries no evidence for this column. Do not fail the whole query.
      if (column >= chunk.columns.size()) {
        continue;
      }
      const auto& values = chunk.columns[column];
      for (size_t row = 0; row < chunk.rows; ++row) {
        values.WriteValueInto(row, &cell);
        if (!cell.IsNull() && (cell.IsCaseInsensitive() || cell.IsInterval())) {
          return true;
        }
      }
    }
    return false;
  }
  const auto collated = [column](const Row& row) {
    return column < row.Count() && !row[column].IsNull() &&
           (row[column].IsCaseInsensitive() || row[column].IsInterval());
  };
  if (!relation.HasSpill()) {
    return std::ranges::any_of(relation.rows, collated);
  }
  bool found = false;
  RETURN_IF_FAIL(relation.ForEachRow(
      [&](const Row& row) { found = found || collated(row); }));
  return found;
}

StatusOr<bool> RequiresCollatedJoinComparison(const Relation& left,
                                              const Relation& right,
                                              const EqualityKey& key) {
  ASSIGN_OR_RETURN(bool, left_collated, HasCollatedJoinColumn(left, key.left));
  if (left_collated) {
    return true;
  }
  return HasCollatedJoinColumn(right, key.right);
}

}  // namespace

Status DisableRawCollatedKeys(const Relation& left, const Relation& right,
                              std::vector<EqualityKey>* keys) {
  size_t keep = 0;
  for (const auto& key : *keys) {
    ASSIGN_OR_RETURN(bool, collated,
                     RequiresCollatedJoinComparison(left, right, key));
    if (!collated) {
      (*keys)[keep++] = key;
    }
  }
  keys->resize(keep);
  return Status::kSuccess;
}

bool HasMixedKeySignednessOrType(const Relation& left, const Relation& right,
                                 const std::vector<EqualityKey>& keys) {
  // A Relation already tracks whether every non-NULL cell matches its
  // declared schema ValueType (including signedness). When both sides are
  // certified uniform there is no mixed key type to find, and the row walks
  // below would only re-derive that same fact once per join. This is the
  // same invariant the columnar/scan/window fast paths already trust.
  if (left.uniform_cell_types && right.uniform_cell_types) {
    return false;
  }
  const auto check_side = [](const Relation& rel, bool is_left,
                             const std::vector<EqualityKey>& eq_keys) {
    if (rel.HasSpill()) {
      return false;
    }
    const bool is_col = rel.HasColumnarOnly();
    const size_t total_rows =
        is_col ? rel.columnar->total_rows : rel.rows.size();
    if (total_rows == 0) {
      return false;
    }
    bool needs_check = total_rows <= 4096;
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
    if (is_col) {
      Value v;
      for (const EqualityKey& key : eq_keys) {
        const size_t col = is_left ? key.left : key.right;
        if (col >= rel.schema.ColumnCount()) {
          return true;
        }
        const Column& expected = rel.schema.GetColumn(col);
        for (const ColumnarChunk& ch : rel.columnar->chunks) {
          if (ch.rows == 0) {
            continue;
          }
          if (col >= ch.columns.size()) {
            return true;
          }
          const ColumnarColumn& cc = ch.columns[col];
          if (cc.is_uniform) {
            if (cc.type != ValueType::kNull &&
                (cc.type != expected.Type() ||
                 cc.is_unsigned != expected.IsUnsigned())) {
              return true;
            }
          } else {
            for (size_t r = 0; r < ch.rows; ++r) {
              cc.WriteValueInto(r, &v);
              if (v.IsNull()) {
                continue;
              }
              if (v.type != expected.Type() ||
                  v.IsUnsigned() != expected.IsUnsigned()) {
                return true;
              }
            }
          }
        }
      }
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

// Lean proof:
// lean/TinylambOpt/ResearchProofs/BinaryCollationStringEquiJoinElision.lean
// (sound)
std::vector<Expression> ResidualJoinPredicates(
    const Schema& left, const Schema& right,
    const std::vector<Expression>& predicates, bool allow_binary_varchar) {
  static const bool varchar_elision_disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_VARCHAR_KEY_ELISION") !=
      nullptr;
  const bool elide_binary_varchar =
      allow_binary_varchar && !varchar_elision_disabled;
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
        if (lc.Type() != rc.Type() || lc.IsUnsigned() || rc.IsUnsigned()) {
          return false;
        }
        if (lc.Type() == ValueType::kInt64 || lc.Type() == ValueType::kDate) {
          return true;
        }
        return elide_binary_varchar && lc.Type() == ValueType::kVarChar &&
               !lc.IsInterval() && !rc.IsInterval() &&
               lc.GetTextDomain() == rc.GetTextDomain();
      });
  std::vector<Expression> residual;
  residual.reserve(predicates.size());
  for (const Expression& predicate : predicates) {
    if (can_elide_exact &&
        MapsToExactEqualityKey(left, right, predicate, elide_binary_varchar)) {
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
  keys.reserve(predicates.size());
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
      // Hash-key encodings only preserve equality inside one (type,
      // signedness) domain: a mismatched pair can never collide, so using
      // it as a key only poisons key filters and suppresses the residual
      // evaluation that would raise the type error (or apply numeric
      // promotion) the AST evaluator produces.  Leave it in the residual.
      const auto key_compatible = [&](size_t lcol, size_t rcol) {
        const Column& lc = left.GetColumn(lcol);
        const Column& rc = right.GetColumn(rcol);
        return lc.Type() == rc.Type() && !lc.IsUnsigned() && !rc.IsUnsigned();
      };
      if (lhs_left && rhs_right) {
        if (key_compatible(*lhs_left, *rhs_right)) {
          keys.push_back({*lhs_left, *rhs_right, null_safe});
        }
      } else if (rhs_left && lhs_right) {
        if (key_compatible(*rhs_left, *lhs_right)) {
          keys.push_back({*rhs_left, *lhs_right, null_safe});
        }
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
          BiasedKeyTypesSupported(left, *biased_left) &&
          BiasedKeyTypesSupported(right, *bare_right)) {
        keys.push_back({*biased_left, *bare_right, false, bias, 0});
        return true;
      }
      if (biased_right && bare_left &&
          BiasedKeyTypesSupported(right, *biased_right) &&
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
  const Column& column = schema.GetColumn(columns[0]);
  if (column.IsUnsigned()) {
    return false;
  }
  const ValueType type = column.Type();
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
    const ValueType left_type = left_schema.GetColumn(left_columns[i]).Type();
    const ValueType right_type =
        right_schema.GetColumn(right_columns[i]).Type();
    if (left_schema.GetColumn(left_columns[i]).IsUnsigned() ||
        right_schema.GetColumn(right_columns[i]).IsUnsigned() ||
        left_type != right_type ||
        (left_type != ValueType::kInt64 && left_type != ValueType::kDate)) {
      return false;
    }
  }
  return true;
}

namespace {

bool LegacyJoinEstimateEnabled() {
  static const bool enabled = [] {
    const char* legacy =
        ::tinylamb::platform::GetEnv("TINYLAMB_LEGACY_ESTIMATE");
    if (legacy == nullptr) {
      return false;
    }
    const std::string_view value(legacy);
    return !value.empty() && value != "0" && value != "false";
  }();
  return enabled;
}

size_t JoinEstimateSampleTarget() {
  return LegacyJoinEstimateEnabled() ? size_t{2'000'000} : size_t{2'048};
}

void BuildJoinKeyFrequencyMaps(const Relation& right,
                               const std::vector<slot_t>& right_columns,
                               bool single_int, bool composite_int,
                               bool allow_hashed, JoinKeyFrequencyMaps* maps) {
  maps->single_int = single_int;
  maps->hashed_key =
      !single_int && allow_hashed && !LegacyJoinEstimateEnabled();
  maps->hashed_fallback = false;
  maps->composite_int = composite_int && !maps->hashed_key;
  maps->right_columns = right_columns;
  maps->sampling_failed = false;
  maps->single_int_frequencies = FlatIntKeyMap<size_t>{};
  maps->frequencies.clear();
  maps->int_frequencies.clear();
  const size_t sample_target = JoinEstimateSampleTarget();
  static const bool build_sampling_disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_LEGACY_PLANNER") != nullptr;
  const size_t total = right.TotalRows();
  maps->build_stride = build_sampling_disabled || total <= sample_target
                           ? 1
                           : (total + sample_target - 1) / sample_target;
  const size_t sample_rows =
      maps->build_stride == 1
          ? total
          : (total + maps->build_stride - 1) / maps->build_stride;
  if (maps->single_int || maps->hashed_key) {
    maps->single_int_frequencies.Reserve(sample_rows);
  } else if (maps->composite_int) {
    maps->int_frequencies.reserve(sample_rows);
  } else {
    maps->frequencies.reserve(sample_rows);
  }
  bool hash_failed = false;
  const auto add_row = [&](const Row& row) {
    if (HasNullKey(row, right_columns)) {
      return;
    }
    if (maps->single_int) {
      bool inserted = false;
      size_t* slot = maps->single_int_frequencies.TryEmplace(
          IntegerJoinKey(row, right_columns[0]), &inserted);
      *slot = inserted ? 1 : *slot + 1;
    } else if (maps->hashed_key) {
      const std::optional<int64_t> hk =
          HashEstimateKeyTuple(row, right_columns);
      if (!hk.has_value()) {
        hash_failed = true;
        return;
      }
      bool inserted = false;
      size_t* slot = maps->single_int_frequencies.TryEmplace(*hk, &inserted);
      *slot = inserted ? 1 : *slot + 1;
    } else if (maps->composite_int) {
      ++maps->int_frequencies[EncodeCompositeIntKey(row, right_columns)];
    } else {
      ++maps->frequencies[EncodeJoinKeyInternal(row, right_columns, false,
                                                false, nullptr)];
    }
  };
  if (right.HasColumnarOnly()) {
    Value scratch;
    std::string key_buf;
    ForEachStridedColumnarPos(
        *right.columnar, maps->build_stride,
        [&](const ColumnarChunk& chunk, size_t r) {
          if (hash_failed ||
              ColumnarHasNullOrNaN(chunk, right_columns, r, &scratch)) {
            return;
          }
          if (maps->single_int) {
            bool inserted = false;
            size_t* slot = maps->single_int_frequencies.TryEmplace(
                chunk.columns[right_columns[0]].IntAt(r), &inserted);
            *slot = inserted ? 1 : *slot + 1;
          } else if (maps->hashed_key) {
            const std::optional<int64_t> hk =
                ColumnarHashEstimateKeyTuple(chunk, right_columns, r, &scratch);
            if (!hk.has_value()) {
              hash_failed = true;
              return;
            }
            bool inserted = false;
            size_t* slot =
                maps->single_int_frequencies.TryEmplace(*hk, &inserted);
            *slot = inserted ? 1 : *slot + 1;
          } else if (maps->composite_int) {
            ++maps->int_frequencies[ColumnarEncodeCompositeIntKey(
                chunk, right_columns, r, nullptr)];
          } else {
            ColumnarEncodeJoinKeyInternal(chunk, right_columns, r, false,
                                          nullptr, &scratch, &key_buf);
            ++maps->frequencies[key_buf];
          }
        });
  } else if (right.HasSpill()) {
    // The spill stream is sequential with no row index, so strided
    // positions still cost a full decode per estimate. Sample a bounded
    // prefix of ~sample_rows rows instead; build_stride scaling is kept.
    const Status sample_status =
        right.ForEachRowBounded(sample_rows, [&](const Row& row) {
          if (!hash_failed) {
            add_row(row);
          }
        });
    if (sample_status != Status::kSuccess) {
      maps->sampling_failed = true;
      return;
    }
  } else if (maps->build_stride == 1) {
    for (const Row& row : right.rows) {
      add_row(row);
      if (hash_failed) {
        break;
      }
    }
  } else {
    for (size_t i = 0; i < right.rows.size(); i += maps->build_stride) {
      add_row(right.rows[i]);
      if (hash_failed) {
        break;
      }
    }
  }
  if (hash_failed) {
    BuildJoinKeyFrequencyMaps(right, right_columns, single_int, composite_int,
                              false, maps);
    maps->hashed_fallback = true;
  }
}

size_t ProbeJoinEstimate(const Relation& left,
                         const std::vector<slot_t>& left_columns,
                         const JoinKeyFrequencyMaps& maps) {
  if (maps.sampling_failed) {
    return std::numeric_limits<size_t>::max();
  }
  size_t estimate = 0;
  // Planning-only cardinality estimation on multi-million-row probe sides
  // costs seconds per join level (TPC-H Q9 sweeps a 6M-row lineitem once per
  // candidate).  Above a threshold, probe a deterministic stride sample and
  // scale the matched-key sum back up; near-tie estimates may flip ranking,
  // which is acceptable for a heuristic planner.
  const size_t sample_target = JoinEstimateSampleTarget();
  static const bool probe_sampling_disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_LEGACY_PLANNER") != nullptr;
  const size_t left_rows = left.TotalRows();
  const size_t stride = probe_sampling_disabled || left_rows <= sample_target
                            ? 1
                            : (left_rows + sample_target - 1) / sample_target;
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
    } else if (maps.hashed_key) {
      const std::optional<int64_t> hk = HashEstimateKeyTuple(row, left_columns);
      if (!hk.has_value()) {
        return;
      }
      if (const size_t* found = maps.single_int_frequencies.Find(*hk);
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
  if (left.HasColumnarOnly()) {
    Value scratch;
    std::string key_buf;
    ForEachStridedColumnarPos(
        *left.columnar, stride, [&](const ColumnarChunk& chunk, size_t r) {
          if (ColumnarHasNullOrNaN(chunk, left_columns, r, &scratch)) {
            return;
          }
          if (maps.single_int) {
            if (const size_t* found = maps.single_int_frequencies.Find(
                    chunk.columns[left_columns[0]].IntAt(r));
                found != nullptr) {
              estimate += *found * maps.build_stride;
            }
          } else if (maps.hashed_key) {
            const std::optional<int64_t> hk =
                ColumnarHashEstimateKeyTuple(chunk, left_columns, r, &scratch);
            if (!hk.has_value()) {
              return;
            }
            if (const size_t* found = maps.single_int_frequencies.Find(*hk);
                found != nullptr) {
              estimate += *found * maps.build_stride;
            }
          } else if (maps.composite_int) {
            const auto found = maps.int_frequencies.find(
                ColumnarEncodeCompositeIntKey(chunk, left_columns, r, nullptr));
            if (found != maps.int_frequencies.end()) {
              estimate += found->second * maps.build_stride;
            }
          } else {
            ColumnarEncodeJoinKeyInternal(chunk, left_columns, r, false,
                                          nullptr, &scratch, &key_buf);
            const auto found = maps.frequencies.find(key_buf);
            if (found != maps.frequencies.end()) {
              estimate += found->second * maps.build_stride;
            }
          }
        });
  } else if (left.HasSpill()) {
    // Same sequential-format bound as the build side: cap the probe stream
    // at ~ceil(left_rows/stride) prefix rows and keep the *stride scaling.
    const size_t probe_sample =
        std::min(left_rows, (left_rows + stride - 1) / stride);
    const Status sample_status =
        left.ForEachRowBounded(probe_sample, probe_row);
    if (sample_status != Status::kSuccess) {
      return std::numeric_limits<size_t>::max();
    }
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
  // Remember whether any key existed before biased ones are filtered out.
  const bool had_any_keys = !keys.empty();
  std::erase_if(keys, [](const EqualityKey& key) {
    return key.left_offset != 0 || key.right_offset != 0;
  });
  if (had_any_keys && keys.empty()) {
    // A biased equality (k = k + c) is still an equality at execution time;
    // estimate it as a selective join instead of falling through to a cross
    // product just because the raw-frequency maps cannot fold the constant.
    return std::max(left.TotalRows(), right.TotalRows());
  }
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
  const bool allow_hashed =
      !single_int && EligibleHashedEstimateSchema(left.schema, left_columns,
                                                  right.schema, right_columns);
  JoinKeyFrequencyMaps maps;
  BuildJoinKeyFrequencyMaps(right, right_columns, single_int, composite_int,
                            allow_hashed, &maps);
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
  // Remember whether any key existed before biased ones are filtered out.
  const bool had_any_keys = !keys.empty();
  std::erase_if(keys, [](const EqualityKey& key) {
    return key.left_offset != 0 || key.right_offset != 0;
  });
  if (had_any_keys && keys.empty()) {
    return std::max(left.TotalRows(), candidate.TotalRows());
  }
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
  const bool eligible = !single_int && EligibleHashedEstimateSchema(
                                           left.schema, left_columns,
                                           candidate.schema, right_columns);
  std::vector<JoinKeyFrequencyMaps>& entries = (*cache)[&candidate];
  for (const JoinKeyFrequencyMaps& entry : entries) {
    if (entry.single_int == single_int &&
        entry.right_columns == right_columns &&
        (!entry.hashed_key ? (entry.composite_int == composite_int &&
                              (!eligible || entry.hashed_fallback))
                           : eligible)) {
      return ProbeJoinEstimate(left, left_columns, entry);
    }
  }
  entries.emplace_back();
  BuildJoinKeyFrequencyMaps(candidate, right_columns, single_int, composite_int,
                            eligible, &entries.back());
  return ProbeJoinEstimate(left, left_columns, entries.back());
}

namespace {
uint64_t MixPartitionHash(uint64_t h, uint64_t salt) {
  uint64_t z = h ^ salt;
  z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
  z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
  return z ^ (z >> 31);
}
}  // namespace

size_t SpillPartitionOfSalted(const std::string& key, size_t partitions,
                              uint64_t salt) {
  // Guard the modulo: callers must supply at least one partition.
  if (partitions == 0) {
    return 0;
  }
  // Platform-stable: partition assignment decides the order spilled rows come
  // back in, which must not depend on the standard library's std::hash.
  const uint64_t h = platform::StableHashBytes(key);
  return static_cast<size_t>((salt == 0 ? h : MixPartitionHash(h, salt)) %
                             partitions);
}

size_t SpillPartitionOfSalted(int64_t key, size_t partitions, uint64_t salt) {
  // Guard the modulo: callers must supply at least one partition.
  if (partitions == 0) {
    return 0;
  }
  // Same platform-stability contract as the string overload: partition
  // assignment decides the order spilled rows come back in, so it must not
  // depend on the standard library's std::hash.  Encode big-endian so the
  // byte string (and therefore the hash) is identical on every endianness.
  const uint64_t be = htobe64(static_cast<uint64_t>(key));
  // Examining an object's bytes through char* is allowed by the aliasing
  // rules; &be is suitably aligned and htobe64 fixes the byte order, so the
  // hash is identical on every endianness.
  const uint64_t h = platform::StableHashBytes(
      std::string_view(reinterpret_cast<const char*>(&be), sizeof(be)));
  return static_cast<size_t>((salt == 0 ? h : MixPartitionHash(h, salt)) %
                             partitions);
}

size_t SpillPartitionOf(const std::string& key, size_t partitions) {
  // Guard the modulo: callers must supply at least one partition.
  if (partitions == 0) {
    return 0;
  }
  return SpillPartitionOfSalted(key, partitions, 0);
}

size_t SpillPartitionOf(int64_t key, size_t partitions) {
  // Guard the modulo: callers must supply at least one partition.
  if (partitions == 0) {
    return 0;
  }
  return SpillPartitionOfSalted(key, partitions, 0);
}

// DeWitt-style Hybrid Hash Join: keep partition 0 resident; spill the rest.
// D8 (docs/design.md): supports null-safe equality keys (NULL matches NULL,
// ordinary NULL keys never match), LEFT/RIGHT/FULL outer, and reprocesses
// EVERY spilled partition -- spilled rows are never dropped or double-counted
// against the resident portion alone.  Over-budget partitions recursively
// re-partition (with salted hash) or fall back to chunked hash probe under
// skew.
StatusOr<Relation> HybridHashJoin(
    Relation left, Relation right, const std::vector<slot_t>& left_columns,
    const std::vector<slot_t>& right_columns,
    const std::function<bool(const Row&)>& matches, bool left_join,
    size_t* join_comparisons, bool null_safe, bool right_join) {
  size_t build_estimate = 0;
  if (right.HasSpill()) {
    build_estimate = right.TotalRows() * 128;
  } else if (right.HasColumnarOnly()) {
    build_estimate =
        std::max(right.EstimatedMemoryBytes(), right.TotalRows() * 128);
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
  // Partition selection and bucket lookup encode the same key twice per row.
  // Reuse one scratch buffer per side instead of allocating a temporary
  // std::string for every call; the scratch outlives each use below.
  std::string left_key_scratch;
  std::string right_key_scratch;
  auto left_key_string = [&](const Row& row) -> const std::string& {
    EncodeNullSafeJoinKeyInto(row, left_columns, &left_key_scratch);
    return left_key_scratch;
  };
  auto right_key_string = [&](const Row& row) -> const std::string& {
    EncodeNullSafeJoinKeyInto(row, right_columns, &right_key_scratch);
    return right_key_scratch;
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

  auto right_partition = [&](const Row& row, size_t part_count,
                             uint64_t salt) -> size_t {
    if (integer_key) {
      return SpillPartitionOfSalted(IntegerJoinKey(row, right_key_column),
                                    part_count, salt);
    }
    return SpillPartitionOfSalted(right_key_string(row), part_count, salt);
  };
  auto left_partition = [&](const Row& row, size_t part_count,
                            uint64_t salt) -> size_t {
    if (integer_key) {
      return SpillPartitionOfSalted(IntegerJoinKey(row, left_key_column),
                                    part_count, salt);
    }
    return SpillPartitionOfSalted(left_key_string(row), part_count, salt);
  };

  // TINYLAMB_QUERY_MEMORY_BYTES=0 (Unlimited) must not pin the resident
  // build partition at the 8 MiB floor: Limit() is 0 then, so the bounded
  // formula degenerates to the floor and forces spilling the budget was
  // meant to avoid. The real arbiter under Unlimited is CanReserve/the
  // process budget, so the synthetic cap is simply absent — the same
  // convention relational.cpp's part_mem_cap uses.
  const size_t resident_cap =
      budget.Unlimited()
          ? std::numeric_limits<size_t>::max()
          : std::max<size_t>(size_t{8} << 20, std::min(budget.Limit() / 4,
                                                       budget.Remaining() / 2));

  Status spill_err{Status::kSuccess};
  RETURN_IF_FAIL(right.ForEachRow([&](const Row& row) {
    if (build_key_excluded(row)) {
      return;
    }
    const size_t part = right_partition(row, partitions, 0);
    const size_t bytes = EstimateRowBytes(row);
    if (part == 0 && (resident_charge.Bytes() + bytes) <= resident_cap &&
        budget.CanReserve(bytes)) {
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
  right.ResetContents();

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
  // Reusable all-NULL row templates for unmatched outer-join rows; avoid
  // allocating a fresh vector<Value> per emitted row.
  const Row right_null_row{std::vector<Value>(right_width)};
  const Row left_null_row{std::vector<Value>(left_width)};

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
      const std::string& key = left_key_string(left_row);
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
      result.AddRow(ConcatRows(left_row, right_null_row));
    }
  };

  RETURN_IF_FAIL(left.ForEachRow([&](const Row& left_row) {
    if (probe_key_excluded(left_row)) {
      if (left_join) {
        result.AddRow(ConcatRows(left_row, right_null_row));
      }
      return;
    }
    const size_t part = left_partition(left_row, partitions, 0);
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
  left.ResetContents();

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
      result.AddRow(left_null_row + resident_right[index]);
    }
  }
  resident_right.clear();
  resident_right.shrink_to_fit();
  resident_charge.ReleaseAll();

  for (size_t part = 0; part < partitions; ++part) {
    RETURN_IF_FAIL(left_parts[part].FinishWriting());
    RETURN_IF_FAIL(right_parts[part].FinishWriting());
  }

  auto partition_budget_bytes = [&]() -> size_t {
    if (budget.Unlimited()) {
      return size_t{1} << 30;
    }
    const size_t min_floor = std::min<size_t>(
        size_t{16} << 20, std::max<size_t>(size_t{4096}, budget.Limit() / 4));
    return std::max<size_t>(
        min_floor, std::min(budget.Remaining() / 2, budget.Limit() / 4));
  };

  auto probe_right_slice =
      [&](const std::vector<Row>& right_rows, SpillFile& left_part,
          std::vector<char>* left_matched, bool emit_left_unmatched) -> Status {
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
    size_t left_idx = 0;
    RETURN_IF_FAIL(left_part.ForEachRow([&](const Row& left_row) {
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
        const std::string& key = left_key_string(left_row);
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
      if (matched && left_matched != nullptr &&
          left_idx < left_matched->size()) {
        (*left_matched)[left_idx] = 1;
      } else if (!matched && emit_left_unmatched) {
        result.AddRow(ConcatRows(left_row, right_null_row));
      }
      ++left_idx;
    }));
    if (right_join) {
      for (size_t index = 0; index < right_rows.size(); ++index) {
        if (part_matched[index] != 0) {
          continue;
        }
        result.AddRow(left_null_row + right_rows[index]);
      }
    }
    return Status::kSuccess;
  };

  // For pure inner joins when the left side of a partition is much smaller
  // than the right side and fits in `part_budget`, build buckets on left rows
  // and stream the right side once while keeping `left ++ right` output order.
  auto try_probe_inner_left_build =
      [&](SpillFile& left_part, SpillFile& right_part, size_t part_budget,
          bool* handled) -> Status {
    *handled = false;
    if (left_join || right_join ||
        left_part.Count() * 4 >= right_part.Count()) {
      return Status::kSuccess;
    }
    ASSIGN_OR_RETURN(SpillFile::RowReader, l_reader, left_part.OpenRowReader());
    std::vector<Row> left_rows;
    QueryMemoryCharge l_charge;
    bool loaded_all = true;
    Row row;
    bool has_row = false;
    while (true) {
      RETURN_IF_FAIL(l_reader.Next(&row, &has_row));
      if (!has_row) {
        break;
      }
      const size_t row_bytes = EstimateRowBytes(row) + 64;
      if (!left_rows.empty() && l_charge.Bytes() + row_bytes > part_budget) {
        loaded_all = false;
        break;
      }
      l_charge.Add(row_bytes);
      left_rows.push_back(std::move(row));
    }
    if (!loaded_all) {
      return Status::kSuccess;
    }
    left_part = SpillFile{};
    std::unordered_multimap<int64_t, const Row*> left_int_buckets;
    std::unordered_multimap<std::string, const Row*> left_str_buckets;
    if (integer_key) {
      left_int_buckets.reserve(left_rows.size());
      for (const Row& lr : left_rows) {
        left_int_buckets.emplace(IntegerJoinKey(lr, left_key_column), &lr);
      }
    } else {
      left_str_buckets.reserve(left_rows.size());
      for (const Row& lr : left_rows) {
        left_str_buckets.emplace(left_key_string(lr), &lr);
      }
    }
    RETURN_IF_FAIL(right_part.ForEachRow([&](const Row& right_row) {
      if (integer_key) {
        const int64_t key = IntegerJoinKey(right_row, right_key_column);
        const auto [begin, end] = left_int_buckets.equal_range(key);
        for (auto iter = begin; iter != end; ++iter) {
          ++(*join_comparisons);
          Row combined = ConcatRows(*iter->second, right_row);
          if (matches(combined)) {
            result.AddRow(std::move(combined));
          }
        }
      } else {
        const std::string& key = right_key_string(right_row);
        const auto [begin, end] = left_str_buckets.equal_range(key);
        for (auto iter = begin; iter != end; ++iter) {
          ++(*join_comparisons);
          Row combined = ConcatRows(*iter->second, right_row);
          if (matches(combined)) {
            result.AddRow(std::move(combined));
          }
        }
      }
    }));
    right_part = SpillFile{};
    *handled = true;
    return Status::kSuccess;
  };

  auto probe_chunked_skew = [&](SpillFile& left_part,
                                SpillFile& right_part) -> Status {
    const size_t chunk_budget = partition_budget_bytes();
    bool inner_swapped = false;
    RETURN_IF_FAIL(try_probe_inner_left_build(left_part, right_part,
                                              chunk_budget, &inner_swapped));
    if (inner_swapped) {
      return Status::kSuccess;
    }
    ASSIGN_OR_RETURN(SpillFile::RowReader, r_reader,
                     right_part.OpenRowReader());
    std::vector<char> left_matched;
    bool need_left_matched = false;
    size_t chunk_idx = 0;
    Row pending_row;
    bool has_pending = false;
    RETURN_IF_FAIL(r_reader.Next(&pending_row, &has_pending));
    while (has_pending) {
      std::vector<Row> chunk_rows;
      QueryMemoryCharge chunk_charge;
      while (has_pending) {
        const size_t row_bytes = EstimateRowBytes(pending_row) + 64;
        if (!chunk_rows.empty() &&
            chunk_charge.Bytes() + row_bytes > chunk_budget) {
          break;
        }
        chunk_charge.Add(row_bytes);
        chunk_rows.push_back(std::move(pending_row));
        RETURN_IF_FAIL(r_reader.Next(&pending_row, &has_pending));
      }
      if (chunk_idx == 0 && left_join && has_pending) {
        need_left_matched = true;
        left_matched.assign(left_part.Count(), 0);
      }
      const bool emit_unmatched_inline = left_join && !need_left_matched;
      RETURN_IF_FAIL(probe_right_slice(
          chunk_rows, left_part, need_left_matched ? &left_matched : nullptr,
          emit_unmatched_inline));
      ++chunk_idx;
    }
    if (need_left_matched) {
      size_t idx = 0;
      RETURN_IF_FAIL(left_part.ForEachRow([&](const Row& left_row) {
        if (idx < left_matched.size() && left_matched[idx] == 0) {
          std::vector<Value> nulls(right_width);
          result.AddRow(ConcatRows(left_row, Row(std::move(nulls))));
        }
        ++idx;
      }));
    }
    left_part = SpillFile{};
    right_part = SpillFile{};
    return Status::kSuccess;
  };

  constexpr size_t kMaxHybridDepth = 4;
  auto probe_partition = [&](auto& self, SpillFile& left_part,
                             SpillFile& right_part, size_t depth) -> Status {
    if (left_part.Empty() && right_part.Empty()) {
      left_part = SpillFile{};
      right_part = SpillFile{};
      return Status::kSuccess;
    }
    if (right_part.Empty()) {
      if (left_join) {
        RETURN_IF_FAIL(left_part.ForEachRow([&](const Row& left_row) {
          std::vector<Value> nulls(right_width);
          result.AddRow(ConcatRows(left_row, Row(std::move(nulls))));
        }));
      }
      left_part = SpillFile{};
      right_part = SpillFile{};
      return Status::kSuccess;
    }
    if (left_part.Empty()) {
      if (right_join) {
        RETURN_IF_FAIL(right_part.ForEachRow([&](const Row& right_row) {
          std::vector<Value> nulls(left_width);
          result.AddRow(Row(std::move(nulls)) + right_row);
        }));
      }
      left_part = SpillFile{};
      right_part = SpillFile{};
      return Status::kSuccess;
    }

    const size_t part_budget = partition_budget_bytes();
    bool inner_swapped = false;
    RETURN_IF_FAIL(try_probe_inner_left_build(left_part, right_part,
                                              part_budget, &inner_swapped));
    if (inner_swapped) {
      return Status::kSuccess;
    }

    // Stream build partition while checking memory budget.
    std::vector<Row> right_rows;
    QueryMemoryCharge part_charge;
    bool loaded_all = true;
    {
      ASSIGN_OR_RETURN(SpillFile::RowReader, r_reader,
                       right_part.OpenRowReader());
      Row row;
      bool has_row = false;
      while (true) {
        RETURN_IF_FAIL(r_reader.Next(&row, &has_row));
        if (!has_row) {
          break;
        }
        const size_t row_bytes = EstimateRowBytes(row) + 64;
        if (!right_rows.empty() &&
            part_charge.Bytes() + row_bytes > part_budget) {
          loaded_all = false;
          break;
        }
        part_charge.Add(row_bytes);
        right_rows.push_back(std::move(row));
      }
    }

    if (loaded_all) {
      right_part = SpillFile{};
      RETURN_IF_FAIL(probe_right_slice(right_rows, left_part, nullptr,
                                       /*emit_left_unmatched=*/left_join));
      left_part = SpillFile{};
      return Status::kSuccess;
    }

    right_rows.clear();
    right_rows.shrink_to_fit();
    part_charge.ReleaseAll();

    if (depth >= kMaxHybridDepth || right_part.Count() < 256) {
      return probe_chunked_skew(left_part, right_part);
    }

    const size_t est_part_bytes = std::max<size_t>(
        right_part.FileBytes(), right_part.Count() * size_t{128});
    const size_t target_sub_bytes =
        std::max<size_t>(part_budget / 2, size_t{4} << 20);
    const size_t sub_partitions = std::clamp<size_t>(
        (est_part_bytes + target_sub_bytes - 1) / target_sub_bytes, 4, 16);
    const uint64_t salt =
        static_cast<uint64_t>(depth + 1) * 0x9e3779b97f4a7c15ULL;

    std::vector<SpillFile> sub_left(sub_partitions);
    std::vector<SpillFile> sub_right(sub_partitions);
    Status sub_err{Status::kSuccess};
    RETURN_IF_FAIL(right_part.ForEachRow([&](const Row& row) {
      const size_t p = right_partition(row, sub_partitions, salt);
      const Status st = sub_right[p].Append(row);
      if (st != Status::kSuccess && sub_err == Status::kSuccess) {
        sub_err = st;
      }
    }));
    RETURN_IF_FAIL(sub_err);
    right_part = SpillFile{};

    RETURN_IF_FAIL(left_part.ForEachRow([&](const Row& row) {
      const size_t p = left_partition(row, sub_partitions, salt);
      const Status st = sub_left[p].Append(row);
      if (st != Status::kSuccess && sub_err == Status::kSuccess) {
        sub_err = st;
      }
    }));
    RETURN_IF_FAIL(sub_err);
    left_part = SpillFile{};

    size_t non_empty_right = 0;
    for (size_t p = 0; p < sub_partitions; ++p) {
      RETURN_IF_FAIL(sub_left[p].FinishWriting());
      RETURN_IF_FAIL(sub_right[p].FinishWriting());
      if (!sub_right[p].Empty()) {
        ++non_empty_right;
      }
    }

    for (size_t p = 0; p < sub_partitions; ++p) {
      if (sub_left[p].Empty() && sub_right[p].Empty()) {
        sub_left[p] = SpillFile{};
        sub_right[p] = SpillFile{};
        continue;
      }
      if (non_empty_right <= 1 && !sub_left[p].Empty() &&
          !sub_right[p].Empty()) {
        RETURN_IF_FAIL(probe_chunked_skew(sub_left[p], sub_right[p]));
      } else {
        RETURN_IF_FAIL(self(self, sub_left[p], sub_right[p], depth + 1));
      }
    }
    return Status::kSuccess;
  };

  for (size_t part = 0; part < partitions; ++part) {
    RETURN_IF_FAIL(probe_partition(probe_partition, left_parts[part],
                                   right_parts[part], 0));
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
  if (right.HasColumnarOnly()) {
    estimate =
        right.columnar->total_rows * (right.schema.ColumnCount() * 8 + 64);
  } else if (right.ChargedBytes() >= right.rows.size() * 16) {
    // AddRow already maintains the per-row byte total; avoid re-walking every
    // cell of the build side for each join.  Relations whose rows were
    // assigned directly have no (or an implausibly small) charge and take the
    // exact scan below.
    estimate = right.ChargedBytes() + (right.rows.size() * 64);
  } else {
    for (const Row& row : right.rows) {
      estimate += EstimateRowBytes(row) + 64;
    }
  }
  return PreferHybridHashJoin(estimate);
}

// Parallel probe for large in-memory hash joins. Workers own statically
// interleaved chunks and accumulate private Relations; worker-order merging is
// deterministic for a fixed worker count, but differs from serial probe order.
// Each Relation observes the query memory budget and can spill. Build buckets
// are read-only; RIGHT/FULL match tracking uses private worker bitmaps merged
// after the probe. Residuals must be pure positional programs; callers exclude
// null-safe keys.
constexpr size_t kParallelProbeMinLeftRows = 16384;
constexpr size_t kParallelProbeChunk = 8192;
constexpr size_t kParallelProbeMaxWorkers = 16;

// A row-only residual can run on probe workers without entering the shared
// transaction/evaluator context. Preserve conjunct order and short circuiting.
namespace {
std::optional<std::vector<BytecodeProgram>> CompileProbeResiduals(
    const std::vector<Expression>& predicates, const Schema& schema) {
  static const bool disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_PARALLEL_JOIN_RESIDUAL") !=
      nullptr;
  if (predicates.empty() || disabled || !BytecodeEnabled()) {
    return std::nullopt;
  }
  std::vector<BytecodeProgram> programs;
  programs.reserve(predicates.size());
  for (const auto& predicate : predicates) {
    if (!predicate || ContainsQuery(predicate) ||
        ContainsParameterSlot(predicate) ||
        !SafeToReduceEvaluationCountWithSchema(predicate, schema)) {
      return std::nullopt;
    }
    auto program = BytecodeCompiler::CompileRowCached(
        predicate, schema, BytecodeCompiler::Context::kFilter);
    if (!program || program->ResultType() == ValueType::kNull) {
      return std::nullopt;
    }
    programs.push_back(std::move(*program));
  }
  return programs;
}

}  // namespace

template <typename Buckets, typename KeyOfRow>
static Status ParallelHashProbe(  // NOLINT(misc-use-anonymous-namespace)
    Buckets& buckets, Relation& left, const std::vector<slot_t>& left_columns,
    KeyOfRow key_of_row, bool left_outer_emit, size_t right_width,
    size_t worker_count, Relation* result, size_t* join_comparisons,
    bool stream_is_left = true, const Row* build_base = nullptr,
    std::vector<char>* build_matched = nullptr,
    const std::vector<BytecodeProgram>* residuals = nullptr,
    std::vector<uint64_t>* build_matched_bits = nullptr,
    size_t fixed_row_bytes = 0) {
  const size_t total = left.rows.size();
  static const bool release_disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_PARALLEL_PROBE_RELEASE") !=
      nullptr;
  const size_t num_chunks =
      (total + kParallelProbeChunk - 1) / kParallelProbeChunk;
  worker_count = std::min({worker_count, num_chunks, total});
  std::vector<Relation> shards(worker_count);
  std::vector<ExecutionRuntime> shard_runtimes(worker_count);
  for (size_t w = 0; w < worker_count; ++w) {
    shards[w].set_runtime(&shard_runtimes[w]);
    shards[w].schema = result->schema;
  }
  // Cache-line padded: every worker bumps its own counter once per emitted
  // match, so adjacent unpadded slots would false-share on the hot line.
  struct alignas(64) PaddedComparison {
    size_t value{0};
  };
  std::vector<PaddedComparison> comparisons(worker_count);
  const bool use_private_bits = build_matched != nullptr &&
                                build_base != nullptr && worker_count > 1 &&
                                worker_count <= 8;
  const size_t bitset_words =
      use_private_bits ? (build_matched->size() + 63) / 64 : 0;
  std::vector<std::vector<uint64_t>> worker_bits;
  if (use_private_bits && bitset_words > 0) {
    worker_bits.resize(worker_count);
    for (size_t w = 0; w < worker_count; ++w) {
      worker_bits[w].assign(bitset_words, 0);
    }
  }
  const size_t flush_granularity = Relation::ChargeFlushGranularity();
  const auto emit_local_row = [&](Relation& local, Row&& out_row) {
    if (fixed_row_bytes != 0) {
      local.AddRowWithKnownBytes(std::move(out_row), fixed_row_bytes,
                                 flush_granularity);
    } else {
      local.AddRow(std::move(out_row));
    }
  };
  if (left_outer_emit && Relation::HasAmpleMemoryBudget() && worker_count > 0) {
    const size_t est_per_shard =
        std::min<size_t>((total + worker_count - 1) / worker_count, 65536);
    for (Relation& shard : shards) {
      shard.rows.reserve(est_per_shard);
    }
  }
  std::mutex error_mutex;
  Status first_error{Status::kSuccess};

  constexpr bool kHasHashedLookup =
      requires(const Buckets& b, const decltype(key_of_row(left.rows[0]))& k) {
        b.HashKey(k);
        b.equal_range_hashed(k, uint64_t{0});
        b.PrefetchBucketHashed(uint64_t{0});
      };

  auto run_worker = [&](size_t worker_id) {
    Relation& local = shards[worker_id];
    uint64_t* local_bits =
        !worker_bits.empty() ? worker_bits[worker_id].data() : nullptr;
    const auto mark_build_match = [&](const Row* build_row) {
      if (build_matched == nullptr) {
        return;
      }
      const auto idx = static_cast<size_t>(build_row - build_base);
      if (local_bits != nullptr) {
        local_bits[idx >> 6] |= (uint64_t{1} << (idx & 63));
        return;
      }
      if (worker_count <= 1) {
        (*build_matched)[idx] = 1;
        return;
      }
      std::atomic_ref<char> flag((*build_matched)[idx]);
      if (flag.load(std::memory_order_relaxed) == 0) {
        flag.store(1, std::memory_order_relaxed);
      }
    };
    const size_t begin = (total * worker_id) / worker_count;
    const size_t end = (total * (worker_id + 1)) / worker_count;
    if (begin >= end) {
      return;
    }
    try {
      const auto emit_unmatched_probe = [&](Row& probe_row) {
        if (stream_is_left) {
          probe_row.values_.resize(probe_row.Count() + right_width);
          emit_local_row(local, std::move(probe_row));
        } else if (probe_row.values_.capacity() >=
                   right_width + probe_row.Count()) {
          const size_t probe_width = probe_row.Count();
          probe_row.values_.resize(right_width + probe_width);
          std::move_backward(probe_row.values_.begin(),
                             probe_row.values_.begin() +
                                 static_cast<std::ptrdiff_t>(probe_width),
                             probe_row.values_.end());
          std::fill_n(probe_row.values_.begin(), right_width, Value());
          emit_local_row(local, std::move(probe_row));
        } else {
          std::vector<Value> padded(right_width + probe_row.Count());
          std::move(probe_row.values_.begin(), probe_row.values_.end(),
                    padded.begin() + static_cast<std::ptrdiff_t>(right_width));
          std::vector<Value>().swap(probe_row.values_);
          emit_local_row(local, Row(std::move(padded)));
        }
      };
      constexpr size_t kBatch = 16;
      using KeyType = std::decay_t<decltype(key_of_row(left.rows[begin]))>;
      struct ProbeSlot {
        KeyType key{};
        uint64_t hash{0};
        bool has_null{false};
      };
      std::array<ProbeSlot, kBatch> batch{};
      for (size_t chunk = begin; chunk < end; chunk += kBatch) {
        const size_t count = std::min(kBatch, end - chunk);
        for (size_t j = 0; j < count; ++j) {
          Row& probe_row = left.rows[chunk + j];
          const bool null_key = HasNullKey(probe_row, left_columns);
          batch[j].has_null = null_key;
          if (!null_key) {
            batch[j].key = key_of_row(probe_row);
            if constexpr (kHasHashedLookup) {
              batch[j].hash = buckets.HashKey(batch[j].key);
              buckets.PrefetchBucketHashed(batch[j].hash);
            } else {
              buckets.PrefetchBucket(batch[j].key);
            }
          }
        }
        for (size_t j = 0; j < count; ++j) {
          Row& left_row = left.rows[chunk + j];
          if (batch[j].has_null) {
            if (left_outer_emit) {
              emit_unmatched_probe(left_row);
            }
            continue;
          }
          const auto [eq_begin, eq_end] = [&]() {
            if constexpr (kHasHashedLookup) {
              return buckets.equal_range_hashed(batch[j].key, batch[j].hash);
            } else {
              return buckets.equal_range(batch[j].key);
            }
          }();
          if (eq_begin == eq_end) {
            if (left_outer_emit) {
              emit_unmatched_probe(left_row);
            }
            continue;
          }
          if (residuals != nullptr) {
            bool matched = false;
            for (auto iter = eq_begin; iter != eq_end; ++iter) {
              ++comparisons[worker_id].value;
              const Row* build_row = iter->second;
              Row combined = stream_is_left ? ConcatRows(left_row, *build_row)
                                            : ConcatRows(*build_row, left_row);
              bool accepted = true;
              for (const auto& program : *residuals) {
                auto value = TryEvaluateRowAuto(program, combined);
                if (!value.HasValue()) {
                  std::scoped_lock error_lock(error_mutex);
                  if (first_error.ok()) {
                    first_error = value.GetStatus();
                  }
                  return;
                }
                if (!Truthy(value.Value())) {
                  accepted = false;
                  break;
                }
              }
              if (!accepted) {
                continue;
              }
              matched = true;
              mark_build_match(build_row);
              emit_local_row(local, std::move(combined));
            }
            if (!matched && left_outer_emit) {
              emit_unmatched_probe(left_row);
            }
            continue;
          }
          for (auto iter = eq_begin; iter != eq_end;) {
            const Row* build_row = iter->second;
            // Advance while a borrowed probe key still has its original row.
            // Only the last match may consume the input buffer.
            ++iter;
            ++comparisons[worker_id].value;
            mark_build_match(build_row);
            if (iter == eq_end) {
              auto& values = left_row.values_;
              if (stream_is_left) {
                values.reserve(values.size() + build_row->values_.size());
                values.insert(values.end(), build_row->values_.begin(),
                              build_row->values_.end());
                emit_local_row(local, std::move(left_row));
              } else if (values.capacity() >=
                         build_row->values_.size() + values.size()) {
                const size_t probe_width = values.size();
                const size_t build_w = build_row->values_.size();
                values.resize(build_w + probe_width);
                std::move_backward(
                    values.begin(),
                    values.begin() + static_cast<std::ptrdiff_t>(probe_width),
                    values.end());
                std::copy(build_row->values_.begin(), build_row->values_.end(),
                          values.begin());
                emit_local_row(local, std::move(left_row));
              } else {
                std::vector<Value> combined;
                combined.reserve(build_row->values_.size() + values.size());
                combined.insert(combined.end(), build_row->values_.begin(),
                                build_row->values_.end());
                combined.insert(combined.end(),
                                std::make_move_iterator(values.begin()),
                                std::make_move_iterator(values.end()));
                std::vector<Value>().swap(values);
                emit_local_row(local, Row(std::move(combined)));
              }
              break;
            }
            emit_local_row(local, stream_is_left
                                      ? ConcatRows(left_row, *build_row)
                                      : ConcatRows(*build_row, left_row));
          }
        }
      }
      if (!release_disabled) {
        // Every probe key in this chunk has been consumed. Accepted rows
        // own their cells; rejected/copy-only rows no longer feed a probe.
        // Release those buffers on their current workers while still hot,
        // avoiding a serial coordinator pass over the rejected payloads.
        for (size_t i = begin; i < end; ++i) {
          if (!left.rows[i].values_.empty()) {
            std::vector<Value>().swap(left.rows[i].values_);
          }
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
  };

  RETURN_IF_FAIL(RunParallelTasks(worker_count, run_worker));
  if (!worker_bits.empty() && bitset_words > 0) {
    if (build_matched_bits != nullptr) {
      build_matched_bits->assign(bitset_words, 0);
      for (size_t w = 0; w < worker_count; ++w) {
        const uint64_t* src = worker_bits[w].data();
        uint64_t* dst = build_matched_bits->data();
        for (size_t k = 0; k < bitset_words; ++k) {
          dst[k] |= src[k];
        }
      }
    } else {
      for (size_t k = 0; k < bitset_words; ++k) {
        uint64_t word = 0;
        for (size_t w = 0; w < worker_count; ++w) {
          word |= worker_bits[w][k];
        }
        if (word == 0) {
          continue;
        }
        const size_t base_idx = k * 64;
        while (word != 0) {
          const auto bit = static_cast<size_t>(std::countr_zero(word));
          if (base_idx + bit < build_matched->size()) {
            (*build_matched)[base_idx + bit] = 1;
          }
          word &= (word - 1);
        }
      }
    }
  }
  if (result->runtime() != nullptr) {
    for (const auto& runtime : shard_runtimes) {
      result->runtime()->relation_spills += runtime.relation_spills;
    }
  }
  const Status merge_status = Relation::MergeShardsInto(shards, result);
  if (first_error.ok() && merge_status != Status::kSuccess) {
    first_error = merge_status;
  }
  for (const PaddedComparison& count : comparisons) {
    *join_comparisons += count.value;
  }
  return first_error;
}

namespace {
size_t FixedJoinRowBytes(const Relation& left, const Relation& right,
                         const Schema& result_schema) {
  if (!left.uniform_cell_types || !right.uniform_cell_types ||
      !Relation::HasAmpleMemoryBudget()) {
    return 0;
  }
  for (size_t col = 0; col < result_schema.ColumnCount(); ++col) {
    const ValueType vt = result_schema.GetColumn(col).Type();
    if (vt != ValueType::kInt64 && vt != ValueType::kDate &&
        vt != ValueType::kDouble && vt != ValueType::kNull) {
      return 0;
    }
  }
  return static_cast<size_t>(result_schema.ColumnCount()) * 40;
}
}  // namespace

template <typename Key, typename Hash = std::hash<Key>, typename SkipBuild,
          typename BuildKey, typename SkipProbe, typename ProbeKey,
          typename MatchFn, typename MarkBuildFn,
          typename UnmatchedProbeFn>
static Status ExecuteFlatBucketJoin(  // NOLINT(misc-use-anonymous-namespace)
    const Relation& build_side, Relation& probe_side,
    const std::vector<slot_t>& probe_columns, bool probe_outer_emit,
    bool stream_is_left, const Row* build_base,
    std::vector<char>* build_matched, std::vector<uint64_t>* build_matched_bits,
    size_t fixed_row_bytes, bool can_parallelize, size_t probe_workers,
    const std::vector<BytecodeProgram>* residuals, Relation* result,
    Status* residual_error, SkipBuild skip_build, BuildKey build_key,
    SkipProbe skip_probe, ProbeKey probe_key, MatchFn matches,
    MarkBuildFn mark_build, UnmatchedProbeFn emit_unmatched_probe) {
  const auto probe_with = [&](const auto& buckets) -> Status {
    if (can_parallelize) {
      RETURN_IF_FAIL(ParallelHashProbe(
          buckets, probe_side, probe_columns, probe_key, probe_outer_emit,
          build_side.schema.ColumnCount(), probe_workers, result,
          &result->join_comparisons, stream_is_left, build_base, build_matched,
          residuals, build_matched_bits, fixed_row_bytes));
      return *residual_error;
    }
    return probe_side.ForEachRow([&](const Row& probe_row) {
      bool matched = false;
      if (!skip_probe(probe_row)) {
        const auto key = probe_key(probe_row);
        const auto [begin, end] = buckets.equal_range(key);
        for (auto iter = begin; iter != end; ++iter) {
          ++result->join_comparisons;
          Row combined = stream_is_left ? ConcatRows(probe_row, *iter->second)
                                        : ConcatRows(*iter->second, probe_row);
          if (matches(combined)) {
            mark_build(*iter->second);
            result->AddRow(std::move(combined));
            matched = true;
          }
        }
      }
      if (!matched) {
        emit_unmatched_probe(probe_row);
      }
    });
  };
  const size_t build_workers =
      !build_side.HasSpill() && build_side.rows.size() >= 8192
          ? ParallelWorkerCount(build_side.rows.size() / 4096)
          : 1;
  if (build_workers > 1) {
    ShardedFlatJoinBuckets<Key, Hash> buckets;
    RETURN_IF_FAIL(buckets.BuildParallel(build_side.rows, build_key, skip_build,
                                         build_workers));
    return probe_with(buckets);
  }
  FlatJoinBuckets<Key, Hash> buckets;
  buckets.reserve(build_side.rows.size());
  for (const Row& row : build_side.rows) {
    if (!skip_build(row)) {
      buckets.emplace(build_key(row), &row);
    }
  }
  return probe_with(buckets);
}

namespace {

constexpr uint32_t kColumnarEmptyBucket = UINT32_MAX;

class FlatStringViewMap {
 public:
  struct Slot {
    uint64_t hash{0};
    const char* ptr{nullptr};
    uint32_t len{0};
    uint32_t head{kColumnarEmptyBucket};
  };

  static uint64_t HashSv(std::string_view sv) noexcept {
    uint64_t h = static_cast<uint64_t>(std::hash<std::string_view>{}(sv));
    h ^= MixEstimateHash64(static_cast<uint64_t>(sv.size()));
    return MixEstimateHash64(h);
  }

  void Reserve(size_t expected) {
    const size_t min_cap = std::max<size_t>(16, ((expected * 10) / 7) + 2);
    size_t cap = 16;
    while (cap < min_cap) {
      cap <<= 1;
    }
    if (cap > slots_.size()) {
      Rehash(cap);
    }
  }

  [[nodiscard]] const uint32_t* Find(std::string_view sv) const noexcept {
    return FindWithHash(sv, HashSv(sv));
  }

  [[nodiscard]] const uint32_t* FindWithHash(std::string_view sv,
                                             uint64_t h) const noexcept {
    if (slots_.empty()) {
      return nullptr;
    }
    size_t pos = static_cast<size_t>(h) & mask_;
    while (true) {
      const Slot& s = slots_[pos];
      if (s.head == kColumnarEmptyBucket) {
        return nullptr;
      }
      if (s.hash == h && s.len == sv.size() &&
          (s.len == 0 || std::memcmp(s.ptr, sv.data(), s.len) == 0)) {
        return &s.head;
      }
      pos = (pos + 1) & mask_;
    }
  }

  uint32_t* TryEmplaceWithHash(std::string_view sv, uint64_t h,
                               bool* inserted) {
    if ((size_ + 1) * 10 > slots_.size() * 7) {
      Rehash(std::max<size_t>(16, slots_.size() * 2));
    }
    size_t pos = static_cast<size_t>(h) & mask_;
    while (true) {
      Slot& s = slots_[pos];
      if (s.head == kColumnarEmptyBucket) {
        s.hash = h;
        s.ptr = sv.data();
        s.len = static_cast<uint32_t>(sv.size());
        s.head = 0;
        ++size_;
        if (inserted != nullptr) {
          *inserted = true;
        }
        return &s.head;
      }
      if (s.hash == h && s.len == sv.size() &&
          (s.len == 0 || std::memcmp(s.ptr, sv.data(), s.len) == 0)) {
        if (inserted != nullptr) {
          *inserted = false;
        }
        return &s.head;
      }
      pos = (pos + 1) & mask_;
    }
  }

  uint32_t* TryEmplace(std::string_view sv, bool* inserted) {
    return TryEmplaceWithHash(sv, HashSv(sv), inserted);
  }

 private:
  void Rehash(size_t new_cap) {
    std::vector<Slot> next(new_cap);
    const size_t new_mask = new_cap - 1;
    for (const Slot& old : slots_) {
      if (old.head == kColumnarEmptyBucket) {
        continue;
      }
      size_t pos = static_cast<size_t>(old.hash) & new_mask;
      while (next[pos].head != kColumnarEmptyBucket) {
        pos = (pos + 1) & new_mask;
      }
      next[pos] = old;
    }
    slots_ = std::move(next);
    mask_ = new_mask;
  }

  std::vector<Slot> slots_;
  size_t mask_{0};
  size_t size_{0};
};

class FlatCompositeIntMap {
 public:
  struct Slot {
    uint64_t hash{0};
    CompositeIntKey key{};
    uint32_t head{kColumnarEmptyBucket};
  };

  static uint64_t HashKey(const CompositeIntKey& k) noexcept {
    uint64_t h = MixEstimateHash64(k.count);
    for (uint8_t i = 0; i < k.count; ++i) {
      h ^= MixEstimateHash64(static_cast<uint64_t>(k.parts[i])) +
           0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
    }
    return h;
  }

  void Reserve(size_t expected) {
    const size_t min_cap = std::max<size_t>(16, ((expected * 10) / 7) + 2);
    size_t cap = 16;
    while (cap < min_cap) {
      cap <<= 1;
    }
    if (cap > slots_.size()) {
      Rehash(cap);
    }
  }

  [[nodiscard]] const uint32_t* Find(const CompositeIntKey& k) const noexcept {
    return FindWithHash(k, HashKey(k));
  }

  [[nodiscard]] const uint32_t* FindWithHash(const CompositeIntKey& k,
                                             uint64_t h) const noexcept {
    if (slots_.empty()) {
      return nullptr;
    }
    size_t pos = static_cast<size_t>(h) & mask_;
    while (true) {
      const Slot& s = slots_[pos];
      if (s.head == kColumnarEmptyBucket) {
        return nullptr;
      }
      if (s.hash == h && s.key == k) {
        return &s.head;
      }
      pos = (pos + 1) & mask_;
    }
  }

  uint32_t* TryEmplace(const CompositeIntKey& k, bool* inserted) {
    return TryEmplaceWithHash(k, HashKey(k), inserted);
  }

  uint32_t* TryEmplaceWithHash(const CompositeIntKey& k, uint64_t h,
                               bool* inserted) {
    if ((size_ + 1) * 10 > slots_.size() * 7) {
      Rehash(std::max<size_t>(16, slots_.size() * 2));
    }
    size_t pos = static_cast<size_t>(h) & mask_;
    while (true) {
      Slot& s = slots_[pos];
      if (s.head == kColumnarEmptyBucket) {
        s.hash = h;
        s.key = k;
        s.head = 0;
        ++size_;
        if (inserted != nullptr) {
          *inserted = true;
        }
        return &s.head;
      }
      if (s.hash == h && s.key == k) {
        if (inserted != nullptr) {
          *inserted = false;
        }
        return &s.head;
      }
      pos = (pos + 1) & mask_;
    }
  }

 private:
  void Rehash(size_t new_cap) {
    std::vector<Slot> next(new_cap);
    const size_t new_mask = new_cap - 1;
    for (const Slot& old : slots_) {
      if (old.head == kColumnarEmptyBucket) {
        continue;
      }
      size_t pos = static_cast<size_t>(old.hash) & new_mask;
      while (next[pos].head != kColumnarEmptyBucket) {
        pos = (pos + 1) & new_mask;
      }
      next[pos] = old;
    }
    slots_ = std::move(next);
    mask_ = new_mask;
  }

  std::vector<Slot> slots_;
  size_t mask_{0};
  size_t size_{0};
};

struct ColumnarColSummary {
  std::shared_ptr<const ColumnarTable> leaf_owner;
  size_t leaf_col{0};
  bool same_leaf_target{true};
  bool all_uniform{true};
  bool any_null{false};
  bool all_null{true};
  ValueType common_type{ValueType::kNull};
  bool common_unsigned{false};
  bool common_interval{false};
};

std::vector<ColumnarColSummary> SummarizeColumnarTable(
    const std::shared_ptr<const ColumnarTable>& tbl, const Schema& schema) {
  const size_t ncols = schema.ColumnCount();
  std::vector<ColumnarColSummary> out(ncols);
  for (size_t c = 0; c < ncols; ++c) {
    out[c].leaf_owner = tbl;
    out[c].leaf_col = c;
  }
  if (tbl == nullptr || tbl->chunks.empty()) {
    return out;
  }
  std::vector<bool> target_init(ncols, false);
  std::vector<bool> type_init(ncols, false);
  for (const ColumnarChunk& ch : tbl->chunks) {
    if (ch.rows == 0) {
      continue;
    }
    // T4593: projected ColumnarTables carry chunks narrower than the schema
    // (columnar_table.cpp resize(slots.size())); index only columns this
    // chunk actually provides, matching FinalizeColumnarJoinResult's guard.
    for (size_t c = 0; c < ncols && c < ch.columns.size(); ++c) {
      ColumnarColSummary& sum = out[c];
      const ColumnarColumn& col = ch.columns[c];
      std::shared_ptr<const ColumnarTable> cur_owner = tbl;
      size_t cur_col = c;
      if (col.encoding == ColumnarColumn::Encoding::kGathered &&
          col.gather_index != nullptr &&
          col.gather_index->source_table != nullptr) {
        cur_owner = col.gather_index->source_table;
        cur_col = col.gather_col;
      }
      if (!target_init[c]) {
        sum.leaf_owner = std::move(cur_owner);
        sum.leaf_col = cur_col;
        target_init[c] = true;
      } else if (sum.same_leaf_target &&
                 (sum.leaf_owner.get() != cur_owner.get() ||
                  sum.leaf_col != cur_col)) {
        sum.same_leaf_target = false;
        sum.leaf_owner = tbl;
        sum.leaf_col = c;
      }
      if (col.has_null || col.type == ValueType::kNull ||
          (col.encoding == ColumnarColumn::Encoding::kConstant &&
           col.const_is_null)) {
        sum.any_null = true;
      }
      if (!col.is_uniform) {
        sum.all_uniform = false;
        sum.all_null = false;
        continue;
      }
      if (col.type != ValueType::kNull) {
        sum.all_null = false;
        if (!type_init[c]) {
          sum.common_type = col.type;
          sum.common_unsigned = col.is_unsigned;
          sum.common_interval = col.is_interval;
          type_init[c] = true;
        } else if (sum.common_type != col.type ||
                   sum.common_unsigned != col.is_unsigned ||
                   sum.common_interval != col.is_interval) {
          sum.all_uniform = false;
        }
      }
    }
  }
  return out;
}

std::vector<size_t> ComputeColumnarBuildGroups(
    const ColumnarTable& tbl, const std::vector<ColumnarColSummary>& plan) {
  const size_t ncols = plan.size();
  std::vector<size_t> group_of(ncols, 0);
  std::vector<size_t> repr_cols;
  repr_cols.reserve(ncols);
  for (size_t c = 0; c < ncols; ++c) {
    if (!plan[c].same_leaf_target) {
      group_of[c] = repr_cols.size();
      repr_cols.push_back(c);
      continue;
    }
    size_t found = repr_cols.size();
    for (size_t g = 0; g < repr_cols.size(); ++g) {
      const size_t rc = repr_cols[g];
      if (!plan[rc].same_leaf_target ||
          plan[rc].leaf_owner.get() != plan[c].leaf_owner.get()) {
        continue;
      }
      bool same_gi = true;
      for (const ColumnarChunk& ch : tbl.chunks) {
        if (ch.rows == 0) {
          continue;
        }
        // T4593: narrower projected chunks simply carry no evidence for
        // these columns; skip them instead of indexing out of range.
        if (rc >= ch.columns.size() || c >= ch.columns.size()) {
          continue;
        }
        if (ch.columns[rc].gather_index.get() !=
            ch.columns[c].gather_index.get()) {
          same_gi = false;
          break;
        }
      }
      if (same_gi) {
        found = g;
        break;
      }
    }
    if (found == repr_cols.size()) {
      repr_cols.push_back(c);
    }
    group_of[c] = found;
  }
  return group_of;
}

Relation FinalizeColumnarJoinResult(
    TransactionContext& context, const Schema& schema,
    std::shared_ptr<const std::vector<std::string>> using_columns,
    const Relation& stats_seed, uint64_t comparisons,
    std::shared_ptr<ColumnarTable> table) {
  Relation result(context.execution_runtime());
  result.schema = schema;
  result.using_columns = std::move(using_columns);
  result.hash_joins = stats_seed.hash_joins;
  result.hybrid_hash_joins = stats_seed.hybrid_hash_joins;
  result.in_memory_hash_joins = stats_seed.in_memory_hash_joins;
  result.nested_loop_joins = stats_seed.nested_loop_joins;
  result.join_comparisons = stats_seed.join_comparisons + comparisons;
  const size_t out_rows = table != nullptr ? table->total_rows : 0;
  result.peak_intermediate_rows =
      std::max(stats_seed.peak_intermediate_rows, out_rows);
  bool all_uniform = true;
  if (table != nullptr) {
    for (const ColumnarChunk& ch : table->chunks) {
      if (ch.rows == 0) {
        continue;
      }
      for (size_t c = 0; c < ch.columns.size() && c < schema.ColumnCount();
           ++c) {
        const ColumnarColumn& col = ch.columns[c];
        const Column& scol = schema.GetColumn(c);
        if (!col.is_uniform ||
            (col.type != ValueType::kNull &&
             (col.type != scol.Type() || col.is_unsigned != scol.IsUnsigned() ||
              col.is_interval))) {
          all_uniform = false;
          break;
        }
      }
      if (!all_uniform) {
        break;
      }
    }
    result.SetColumnarTable(std::move(table), all_uniform);
  } else {
    result.SetColumnarTable(std::make_shared<ColumnarTable>(), true);
  }
  return result;
}

void AppendSideSingleRowToColumnarChunk(const ColumnarChunk& single_src,
                                        size_t row_count, const Schema& schema,
                                        ColumnarChunk* dst) {
  const size_t ncols = schema.ColumnCount();
  Value v;
  for (size_t c = 0; c < ncols; ++c) {
    const ColumnarColumn& src = single_src.columns[c];
    src.WriteValueInto(0, &v);
    ColumnarColumn out;
    const bool scalar_ok =
        !v.IsNull() && !v.IsCaseInsensitive() && v.type != ValueType::kArray &&
        (v.type == ValueType::kVarChar
             ? (v.Collation() == 0 || v.IsInterval())
             : (v.Collation() == 0 ||
                (v.type == ValueType::kInt64 && v.IsUnsigned())));
    if (v.IsNull()) {
      out.SetConstantNull(schema.GetColumn(c).Type(), false, false, row_count);
    } else if (scalar_ok) {
      out.Init(v.type, v.IsUnsigned(), v.IsInterval(), 0);
      out.encoding = ColumnarColumn::Encoding::kConstant;
      out.const_is_null = false;
      out.const_rows = row_count;
      if (v.type == ValueType::kInt64 || v.type == ValueType::kDate) {
        out.const_int = v.value.int_value;
      } else if (v.type == ValueType::kDouble) {
        out.const_double = v.value.double_value;
      } else if (v.type == ValueType::kVarChar) {
        out.const_string = dst->string_arena->Store(v.value.varchar_value);
      }
    } else {
      out.Init(ValueType::kNull, false, false, row_count);
      out.DemoteToFallback(row_count);
      out.fallback.assign(row_count, v);
      for (Value& cell : out.fallback) {
        if (cell.type == ValueType::kVarChar &&
            !cell.value.varchar_value.empty()) {
          cell.value.varchar_value =
              dst->string_arena->Store(cell.value.varchar_value);
          cell.owned_data.clear();
        }
      }
    }
    dst->columns.push_back(std::move(out));
  }
}

void AppendNullColumnsToChunk(size_t row_count, const Schema& schema,
                              ColumnarChunk* dst) {
  for (size_t c = 0; c < schema.ColumnCount(); ++c) {
    ColumnarColumn col;
    col.SetConstantNull(schema.GetColumn(c).Type(), false, false, row_count);
    dst->columns.push_back(std::move(col));
  }
}

void AppendAliasColumnsToChunk(const ColumnarChunk& src, ColumnarChunk* dst) {
  if (src.string_arena != nullptr) {
    dst->keepalives.push_back(src.string_arena);
  }
  for (const auto& ka : src.keepalives) {
    dst->keepalives.push_back(ka);
  }
  for (const ColumnarColumn& c : src.columns) {
    dst->columns.push_back(c);
  }
}

StatusOr<std::optional<Relation>> TryColumnarCrossOrNullJoin(
    TransactionContext& context, Relation& left, Relation& right,
    bool want_left_nulls, bool want_right_nulls, const Schema& result_schema,
    const std::vector<Expression>& residual,
    std::shared_ptr<const std::vector<std::string>> using_columns,
    const Relation& stats_seed) {
  if (left.HasSpill() || right.HasSpill() ||
      (!left.HasColumnarOnly() && !right.HasColumnarOnly()) ||
      !residual.empty()) {
    return std::nullopt;
  }
  const size_t nl = left.TotalRows();
  const size_t nr = right.TotalRows();
  if (nl > 1 && nr > 1) {
    return std::nullopt;
  }
  if ((nl == 0 && !want_right_nulls) || (nr == 0 && !want_left_nulls) ||
      (nl == 0 && nr == 0)) {
    return FinalizeColumnarJoinResult(context, result_schema,
                                      std::move(using_columns), stats_seed, 0,
                                      std::make_shared<ColumnarTable>());
  }
  auto to_col =
      [](Relation& rel) -> StatusOr<std::shared_ptr<const ColumnarTable>> {
    if (rel.HasColumnarOnly()) {
      return rel.columnar;
    }
    ASSIGN_OR_RETURN(std::shared_ptr<ColumnarTable>, built,
                     BuildColumnarTableFromRows(rel.schema, rel.rows));
    return std::shared_ptr<const ColumnarTable>(std::move(built));
  };
  auto out = std::make_shared<ColumnarTable>();
  if (nl == 0) {
    ASSIGN_OR_RETURN(std::shared_ptr<const ColumnarTable>, rtbl, to_col(right));
    for (const ColumnarChunk& rc : rtbl->chunks) {
      if (rc.rows == 0) {
        continue;
      }
      ColumnarChunk dst;
      dst.rows = rc.rows;
      dst.string_arena = std::make_shared<ChunkStringArena>();
      AppendNullColumnsToChunk(rc.rows, left.schema, &dst);
      AppendAliasColumnsToChunk(rc, &dst);
      out->total_rows += dst.rows;
      out->chunks.push_back(std::move(dst));
    }
    return FinalizeColumnarJoinResult(context, result_schema,
                                      std::move(using_columns), stats_seed, 0,
                                      std::move(out));
  }
  if (nr == 0) {
    ASSIGN_OR_RETURN(std::shared_ptr<const ColumnarTable>, ltbl, to_col(left));
    for (const ColumnarChunk& lc : ltbl->chunks) {
      if (lc.rows == 0) {
        continue;
      }
      ColumnarChunk dst;
      dst.rows = lc.rows;
      dst.string_arena = std::make_shared<ChunkStringArena>();
      AppendAliasColumnsToChunk(lc, &dst);
      AppendNullColumnsToChunk(lc.rows, right.schema, &dst);
      out->total_rows += dst.rows;
      out->chunks.push_back(std::move(dst));
    }
    return FinalizeColumnarJoinResult(context, result_schema,
                                      std::move(using_columns), stats_seed, 0,
                                      std::move(out));
  }
  ASSIGN_OR_RETURN(std::shared_ptr<const ColumnarTable>, ltbl, to_col(left));
  ASSIGN_OR_RETURN(std::shared_ptr<const ColumnarTable>, rtbl, to_col(right));
  const auto find_first_nonempty =
      [](const ColumnarTable& tbl) -> const ColumnarChunk* {
    for (const ColumnarChunk& ch : tbl.chunks) {
      if (ch.rows > 0) {
        return &ch;
      }
    }
    return nullptr;
  };
  if (nl == 1) {
    const ColumnarChunk* single = find_first_nonempty(*ltbl);
    if (single == nullptr) {
      return std::nullopt;
    }
    for (const ColumnarChunk& rc : rtbl->chunks) {
      if (rc.rows == 0) {
        continue;
      }
      ColumnarChunk dst;
      dst.rows = rc.rows;
      dst.string_arena = std::make_shared<ChunkStringArena>();
      AppendSideSingleRowToColumnarChunk(*single, rc.rows, left.schema, &dst);
      AppendAliasColumnsToChunk(rc, &dst);
      out->total_rows += dst.rows;
      out->chunks.push_back(std::move(dst));
    }
    return FinalizeColumnarJoinResult(context, result_schema,
                                      std::move(using_columns), stats_seed, nr,
                                      std::move(out));
  }
  const ColumnarChunk* single = find_first_nonempty(*rtbl);
  if (single == nullptr) {
    return std::nullopt;
  }
  for (const ColumnarChunk& lc : ltbl->chunks) {
    if (lc.rows == 0) {
      continue;
    }
    ColumnarChunk dst;
    dst.rows = lc.rows;
    dst.string_arena = std::make_shared<ChunkStringArena>();
    AppendAliasColumnsToChunk(lc, &dst);
    AppendSideSingleRowToColumnarChunk(*single, lc.rows, right.schema, &dst);
    out->total_rows += dst.rows;
    out->chunks.push_back(std::move(dst));
  }
  return FinalizeColumnarJoinResult(context, result_schema,
                                    std::move(using_columns), stats_seed, nl,
                                    std::move(out));
}

struct SrcIntLeafCache {
  enum Kind : uint8_t {
    kOther = 0,
    kAllNull,
    kConst,
    kFlat,
    kFoR0,
    kFoR1,
    kFoR2,
    kFoR4,
    kFoR8,
  };
  struct ChunkDesc {
    Kind kind{kOther};
    int64_t base{0};
    const int64_t* ints{nullptr};
    const uint8_t* for_bytes{nullptr};
    const uint8_t* nulls{nullptr};
  };
  const ColumnarTable* tbl{nullptr};
  size_t col{0};
  bool any_null{false};
  bool any_other{false};
  std::vector<ChunkDesc> chunks;

  void Ensure(const ColumnarTable* src_tbl, size_t src_col) {
    if (tbl == src_tbl && col == src_col) {
      return;
    }
    tbl = src_tbl;
    col = src_col;
    any_null = false;
    any_other = false;
    chunks.resize(src_tbl->chunks.size());
    for (size_t c = 0; c < src_tbl->chunks.size(); ++c) {
      ChunkDesc& d = chunks[c];
      d = ChunkDesc{};
      const auto& cols = src_tbl->chunks[c].columns;
      if (src_col >= cols.size()) {
        d.kind = kOther;
        any_other = true;
        any_null = true;
        continue;
      }
      const ColumnarColumn& sc = cols[src_col];
      if (!sc.is_uniform || sc.is_unsigned || sc.is_interval ||
          (sc.type != ValueType::kInt64 && sc.type != ValueType::kDate &&
           sc.type != ValueType::kNull)) {
        d.kind = kOther;
        any_other = true;
        any_null = true;
        continue;
      }
      if (sc.type == ValueType::kNull ||
          (sc.encoding == ColumnarColumn::Encoding::kConstant &&
           sc.const_is_null)) {
        d.kind = kAllNull;
        any_null = true;
        continue;
      }
      switch (sc.encoding) {
        case ColumnarColumn::Encoding::kConstant:
          d.kind = kConst;
          d.base = sc.const_int;
          break;
        case ColumnarColumn::Encoding::kFlat:
          d.kind = kFlat;
          d.ints = sc.ints.data();
          if (sc.has_null && !sc.nulls.empty()) {
            d.nulls = sc.nulls.data();
            any_null = true;
          }
          break;
        case ColumnarColumn::Encoding::kFrameOfReference:
          switch (sc.for_width) {
            case 0:
              d.kind = kFoR0;
              break;
            case 1:
              d.kind = kFoR1;
              break;
            case 2:
              d.kind = kFoR2;
              break;
            case 4:
              d.kind = kFoR4;
              break;
            case 8:
              d.kind = kFoR8;
              break;
            default:
              d.kind = kOther;
              any_other = true;
              any_null = true;
              continue;
          }
          d.base = sc.for_base;
          d.for_bytes = sc.for_bytes;
          if (sc.has_null && sc.borrowed_nulls != nullptr) {
            d.nulls = sc.borrowed_nulls;
            any_null = true;
          }
          break;
        default:
          d.kind = kOther;
          any_other = true;
          any_null = true;
          break;
      }
    }
  }
};

inline uint64_t DecodeIntTile64(const ColumnarColumn& col, uint32_t r0,
                                uint32_t cnt, SrcIntLeafCache* leaf_cache,
                                int64_t* __restrict vals) noexcept {
  const uint64_t full_mask =
      cnt == 64 ? ~uint64_t{0} : ((uint64_t{1} << cnt) - 1);
  if (!col.is_uniform || col.is_unsigned || col.is_interval) {
    uint64_t mask = 0;
    for (uint32_t i = 0; i < cnt; ++i) {
      if (col.TryReadNonNullInt(r0 + i, &vals[i])) {
        mask |= (uint64_t{1} << i);
      }
    }
    return mask;
  }
  if (col.type == ValueType::kNull) {
    return 0;
  }
  auto pack_nulls = [](const uint8_t* __restrict np, uint32_t cnt_n,
                       uint64_t fmask) noexcept -> uint64_t {
    if (np == nullptr) {
      return fmask;
    }
    uint64_t mask = 0;
    for (uint32_t i = 0; i < cnt_n; ++i) {
      mask |= static_cast<uint64_t>(np[i] == 0) << i;
    }
    return mask;
  };
  if (col.encoding == ColumnarColumn::Encoding::kFlat) {
    std::memcpy(vals, col.ints.data() + r0, static_cast<size_t>(cnt) * 8);
    const uint8_t* np = (col.has_null && !col.nulls.empty())
                            ? (col.nulls.data() + r0)
                            : nullptr;
    return pack_nulls(np, cnt, full_mask);
  }
  if (col.encoding == ColumnarColumn::Encoding::kFrameOfReference) {
    const auto base_u = static_cast<uint64_t>(col.for_base);
    const uint8_t* __restrict fb = col.for_bytes;
    bool ok_width = true;
    switch (col.for_width) {
      case 0:
        for (uint32_t i = 0; i < cnt; ++i) {
          vals[i] = col.for_base;
        }
        break;
      case 1: {
        const uint8_t* __restrict p = fb + r0;
        for (uint32_t i = 0; i < cnt; ++i) {
          vals[i] = static_cast<int64_t>(base_u + p[i]);
        }
        break;
      }
      case 2: {
        const uint8_t* __restrict p = fb + (static_cast<size_t>(r0) * 2);
        for (uint32_t i = 0; i < cnt; ++i) {
          uint16_t d = 0;
          std::memcpy(&d, p + (static_cast<size_t>(i) * 2), sizeof(d));
          vals[i] = static_cast<int64_t>(base_u + d);
        }
        break;
      }
      case 4: {
        const uint8_t* __restrict p = fb + (static_cast<size_t>(r0) * 4);
        for (uint32_t i = 0; i < cnt; ++i) {
          uint32_t d = 0;
          std::memcpy(&d, p + (static_cast<size_t>(i) * 4), sizeof(d));
          vals[i] = static_cast<int64_t>(base_u + d);
        }
        break;
      }
      case 8: {
        const uint8_t* __restrict p = fb + (static_cast<size_t>(r0) * 8);
        for (uint32_t i = 0; i < cnt; ++i) {
          uint64_t d = 0;
          std::memcpy(&d, p + (static_cast<size_t>(i) * 8), sizeof(d));
          vals[i] = static_cast<int64_t>(base_u + d);
        }
        break;
      }
      default:
        ok_width = false;
        break;
    }
    if (ok_width) {
      const uint8_t* np = (col.has_null && col.borrowed_nulls != nullptr)
                              ? (col.borrowed_nulls + r0)
                              : nullptr;
      return pack_nulls(np, cnt, full_mask);
    }
  }
  if (col.encoding == ColumnarColumn::Encoding::kGathered &&
      leaf_cache != nullptr && col.gather_index != nullptr &&
      col.gather_index->source_table != nullptr) {
    const ChunkGatherIndex* gi = col.gather_index.get();
    leaf_cache->Ensure(gi->source_table.get(), col.gather_col);
    const SrcIntLeafCache::ChunkDesc* __restrict descs =
        leaf_cache->chunks.data();
    const uint32_t* __restrict scs = gi->src_chunks.data() + r0;
    const uint32_t* __restrict srs = gi->src_rows.data() + r0;
    auto read_desc = [](const SrcIntLeafCache::ChunkDesc& d,
                        uint32_t sr) noexcept -> int64_t {
      switch (d.kind) {
        case SrcIntLeafCache::kConst:
        case SrcIntLeafCache::kFoR0:
          return d.base;
        case SrcIntLeafCache::kFlat:
          return d.ints[sr];
        case SrcIntLeafCache::kFoR1:
          return static_cast<int64_t>(static_cast<uint64_t>(d.base) +
                                      d.for_bytes[sr]);
        case SrcIntLeafCache::kFoR2: {
          uint16_t v = 0;
          std::memcpy(&v, d.for_bytes + (static_cast<size_t>(sr) * sizeof(v)),
                      sizeof(v));
          return static_cast<int64_t>(static_cast<uint64_t>(d.base) + v);
        }
        case SrcIntLeafCache::kFoR4: {
          uint32_t v = 0;
          std::memcpy(&v, d.for_bytes + (static_cast<size_t>(sr) * sizeof(v)),
                      sizeof(v));
          return static_cast<int64_t>(static_cast<uint64_t>(d.base) + v);
        }
        case SrcIntLeafCache::kFoR8: {
          uint64_t v = 0;
          std::memcpy(&v, d.for_bytes + (static_cast<size_t>(sr) * sizeof(v)),
                      sizeof(v));
          return static_cast<int64_t>(static_cast<uint64_t>(d.base) + v);
        }
        default:
          return 0;
      }
    };
    if (!gi->has_null && !leaf_cache->any_null && !leaf_cache->any_other) {
      for (uint32_t i = 0; i < cnt; ++i) {
        vals[i] = read_desc(descs[scs[i]], srs[i]);
      }
      return full_mask;
    }
    uint64_t mask = 0;
    for (uint32_t i = 0; i < cnt; ++i) {
      const uint32_t sr = srs[i];
      if (sr == ChunkGatherIndex::kNullRow) {
        continue;
      }
      const SrcIntLeafCache::ChunkDesc& d = descs[scs[i]];
      if (d.kind == SrcIntLeafCache::kAllNull) {
        continue;
      }
      if (d.kind == SrcIntLeafCache::kOther) {
        if (col.TryReadNonNullInt(r0 + i, &vals[i])) {
          mask |= (uint64_t{1} << i);
        }
        continue;
      }
      if (d.nulls != nullptr && d.nulls[sr] != 0) {
        continue;
      }
      vals[i] = read_desc(d, sr);
      mask |= (uint64_t{1} << i);
    }
    return mask;
  }
  uint64_t mask = 0;
  for (uint32_t i = 0; i < cnt; ++i) {
    if (col.TryReadNonNullInt(r0 + i, &vals[i])) {
      mask |= (uint64_t{1} << i);
    }
  }
  return mask;
}

bool ColumnIntBiasFitsNoOverflow(const ColumnarTable& tbl, slot_t col_idx,
                                 int64_t bias) noexcept {
  if (bias == 0) {
    return true;
  }
  const platform::Int128 kMin64 = std::numeric_limits<int64_t>::min();
  const platform::Int128 kMax64 = std::numeric_limits<int64_t>::max();
  const platform::Int128 lo128 = kMin64 - static_cast<platform::Int128>(bias);
  const platform::Int128 hi128 = kMax64 - static_cast<platform::Int128>(bias);
  const int64_t lo = lo128 < kMin64 ? std::numeric_limits<int64_t>::min()
                                    : static_cast<int64_t>(lo128);
  const int64_t hi = hi128 > kMax64 ? std::numeric_limits<int64_t>::max()
                                    : static_cast<int64_t>(hi128);
  for (const ColumnarChunk& ch : tbl.chunks) {
    if (ch.rows == 0) {
      continue;
    }
    const ColumnarColumn& c = ch.columns[col_idx];
    if (!c.is_uniform || c.is_unsigned || c.is_interval) {
      return false;
    }
    if (c.type == ValueType::kNull ||
        (c.encoding == ColumnarColumn::Encoding::kConstant &&
         c.const_is_null)) {
      continue;
    }
    if (c.type != ValueType::kInt64) {
      return false;
    }
    if (c.encoding == ColumnarColumn::Encoding::kConstant) {
      if (c.const_int < lo || c.const_int > hi) {
        return false;
      }
      continue;
    }
    if (c.encoding == ColumnarColumn::Encoding::kFlat) {
      if (!c.has_null) {
        const int64_t* vals = c.ints.data();
        bool bad = false;
        for (size_t r = 0; r < ch.rows; ++r) {
          bad |= (vals[r] < lo || vals[r] > hi);
        }
        if (bad) {
          return false;
        }
      } else {
        for (size_t r = 0; r < ch.rows; ++r) {
          if (c.IsNull(r)) {
            continue;
          }
          const int64_t v = c.ints[r];
          if (v < lo || v > hi) {
            return false;
          }
        }
      }
      continue;
    }
    if (c.encoding == ColumnarColumn::Encoding::kFrameOfReference &&
        !c.has_null && c.for_width <= 4) {
      const platform::Int128 max_delta =
          c.for_width == 0 ? 0
                           : ((platform::Int128{1}
                               << (static_cast<unsigned>(c.for_width) * 8U)) -
                              1);
      const auto min_val = static_cast<platform::Int128>(c.for_base);
      const platform::Int128 max_val = min_val + max_delta;
      if (min_val >= lo && max_val <= hi) {
        continue;
      }
    }
    for (size_t r = 0; r < ch.rows; ++r) {
      if (c.IsNull(r)) {
        continue;
      }
      const int64_t v = c.IntAt(r);
      if (v < lo || v > hi) {
        return false;
      }
    }
  }
  return true;
}

bool CanElideJoinResidualWithKeys(
    const Expression& predicate, const Schema& result_schema, size_t left_width,
    bool flip, const ColumnarTable& build_tbl, const ColumnarTable& probe_tbl,
    const std::vector<slot_t>& left_columns,
    const std::vector<slot_t>& right_columns,
    const std::vector<int64_t>& left_key_offsets,
    const std::vector<int64_t>& right_key_offsets,
    const std::vector<slot_t>& build_cols,
    const std::vector<slot_t>& probe_cols,
    const std::vector<int64_t>& build_biases,
    const std::vector<int64_t>& probe_biases,
    const std::vector<ColumnarColSummary>& build_plan,
    const std::vector<ColumnarColSummary>& probe_plan,
    std::vector<int8_t>* key_bias_safe) {
  if (!predicate || predicate->Type() != TypeTag::kBinaryExp) {
    return false;
  }
  const BinaryExpression& binary = predicate->AsBinaryExpression();
  if (binary.Op() != BinaryOperation::kEquals) {
    return false;
  }
  ColumnName l_name;
  ColumnName r_name;
  int64_t l_bias = 0;
  int64_t r_bias = 0;
  if (!ColumnWithConstantBias(binary.Left(), &l_name, &l_bias) ||
      !ColumnWithConstantBias(binary.Right(), &r_name, &r_bias)) {
    return false;
  }
  const int loff = result_schema.Offset(l_name);
  const int roff = result_schema.Offset(r_name);
  if (loff < 0 || roff < 0) {
    return false;
  }
  const bool bare_cols = binary.Left()->Type() == TypeTag::kColumnValue &&
                         binary.Right()->Type() == TypeTag::kColumnValue;
  const ColumnarTable& left_tbl = flip ? build_tbl : probe_tbl;
  const ColumnarTable& right_tbl = flip ? probe_tbl : build_tbl;
  for (size_t k = 0; k < left_columns.size(); ++k) {
    const int left_slot = static_cast<int>(left_columns[k]);
    const int right_slot = static_cast<int>(left_width + right_columns[k]);
    bool same_order = false;
    if (loff == left_slot && roff == right_slot) {
      same_order = true;
    } else if (loff == right_slot && roff == left_slot) {
      same_order = false;
    } else {
      continue;
    }
    const int64_t pred_left_bias = same_order ? l_bias : r_bias;
    const int64_t pred_right_bias = same_order ? r_bias : l_bias;
    if (static_cast<platform::Int128>(pred_left_bias) - pred_right_bias !=
        static_cast<platform::Int128>(left_key_offsets[k]) -
            right_key_offsets[k]) {
      continue;
    }
    const ColumnarColSummary& bp = build_plan[build_cols[k]];
    const ColumnarColSummary& pp = probe_plan[probe_cols[k]];
    if (bp.all_null || pp.all_null || !bp.all_uniform || !pp.all_uniform ||
        bp.common_unsigned || pp.common_unsigned || bp.common_interval ||
        pp.common_interval || bp.common_type != pp.common_type) {
      continue;
    }
    const ValueType vt = bp.common_type;
    if (vt == ValueType::kVarChar || vt == ValueType::kDate) {
      if (!bare_cols || build_biases[k] != 0 || probe_biases[k] != 0) {
        continue;
      }
      return true;
    }
    if (vt == ValueType::kInt64) {
      if ((*key_bias_safe)[k] < 0) {
        const bool ok = ColumnIntBiasFitsNoOverflow(build_tbl, build_cols[k],
                                                    build_biases[k]) &&
                        ColumnIntBiasFitsNoOverflow(probe_tbl, probe_cols[k],
                                                    probe_biases[k]);
        (*key_bias_safe)[k] = ok ? 1 : 0;
      }
      if ((*key_bias_safe)[k] == 1 &&
          ColumnIntBiasFitsNoOverflow(left_tbl, left_columns[k],
                                      pred_left_bias) &&
          ColumnIntBiasFitsNoOverflow(right_tbl, right_columns[k],
                                      pred_right_bias)) {
        return true;
      }
    }
  }
  return false;
}

StatusOr<std::optional<Relation>> TryColumnarHashJoin(
    TransactionContext& context, Relation& left, Relation& right, bool flip,
    bool want_left_nulls, bool want_right_nulls, bool has_null_safe_key,
    bool has_using_residuals, const std::vector<slot_t>& left_columns,
    const std::vector<slot_t>& right_columns,
    const std::vector<int64_t>& left_key_offsets,
    const std::vector<int64_t>& right_key_offsets,
    const std::vector<Expression>& residual,
    const std::optional<std::vector<BytecodeProgram>>& compiled_residuals,
    std::shared_ptr<const std::vector<std::string>> using_columns,
    const Relation& stats_seed) {
  if (left.HasSpill() || right.HasSpill() || has_null_safe_key ||
      has_using_residuals ||
      (!left.HasColumnarOnly() && !right.HasColumnarOnly()) ||
      (!residual.empty() && !compiled_residuals.has_value())) {
    return std::optional<Relation>{std::nullopt};
  }
  const Schema result_schema = left.schema + right.schema;
  const size_t nl = left.TotalRows();
  const size_t nr = right.TotalRows();
  if ((nl == 0 && !want_right_nulls) || (nr == 0 && !want_left_nulls) ||
      (nl == 0 && nr == 0)) {
    return std::optional<Relation>{FinalizeColumnarJoinResult(
        context, result_schema, std::move(using_columns), stats_seed, 0,
        std::make_shared<ColumnarTable>())};
  }
  // Re-encoding a row-based side into ColumnarTable duplicates it for the
  // duration of the build. Converting the larger relation costs more than
  // materializing the smaller columnar side to rows on the ordinary join
  // path, so only convert a side that is not the bigger input.
  const bool left_row_based = !left.HasColumnarOnly();
  const bool right_row_based = !right.HasColumnarOnly();
  if ((left_row_based && !right_row_based && nl > nr) ||
      (!left_row_based && right_row_based && nr > nl)) {
    return std::optional<Relation>{std::nullopt};
  }
  auto to_col =
      [](Relation& rel) -> StatusOr<std::shared_ptr<const ColumnarTable>> {
    if (rel.HasColumnarOnly()) {
      return rel.columnar;
    }
    ASSIGN_OR_RETURN(std::shared_ptr<ColumnarTable>, built,
                     BuildColumnarTableFromRows(rel.schema, rel.rows));
    // The columnar copy fully replaces the buffered rows; drop them so the
    // input is not held twice for the rest of the join.
    rel.ResetContents();
    return std::shared_ptr<const ColumnarTable>(std::move(built));
  };
  ASSIGN_OR_RETURN(std::shared_ptr<const ColumnarTable>, left_tbl,
                   to_col(left));
  ASSIGN_OR_RETURN(std::shared_ptr<const ColumnarTable>, right_tbl,
                   to_col(right));

  const bool probe_outer_emit = flip ? want_right_nulls : want_left_nulls;
  const bool want_build_nulls = flip ? want_left_nulls : want_right_nulls;
  const auto& build_tbl = flip ? left_tbl : right_tbl;
  const auto& probe_tbl = flip ? right_tbl : left_tbl;
  const Schema& build_schema = flip ? left.schema : right.schema;
  const Schema& probe_schema = flip ? right.schema : left.schema;
  const std::vector<slot_t>& build_cols = flip ? left_columns : right_columns;
  const std::vector<slot_t>& probe_cols = flip ? right_columns : left_columns;
  const std::vector<int64_t>& build_biases =
      flip ? left_key_offsets : right_key_offsets;
  const std::vector<int64_t>& probe_biases =
      flip ? right_key_offsets : left_key_offsets;

  std::vector<ColumnarColSummary> left_plan =
      SummarizeColumnarTable(left_tbl, left.schema);
  std::vector<ColumnarColSummary> right_plan =
      SummarizeColumnarTable(right_tbl, right.schema);
  const auto& build_plan = flip ? left_plan : right_plan;
  const auto& probe_plan = flip ? right_plan : left_plan;
  const std::vector<size_t> build_groups =
      ComputeColumnarBuildGroups(*build_tbl, build_plan);
  const size_t num_build_groups =
      build_groups.empty() ? 0 : (*std::ranges::max_element(build_groups) + 1);
  std::vector<size_t> build_group_repr(num_build_groups, 0);
  for (size_t c = build_groups.size(); c-- > 0;) {
    build_group_repr[build_groups[c]] = c;
  }

  const size_t n_build = build_tbl->total_rows;
  const size_t n_probe = probe_tbl->total_rows;
  if (n_build == 0 || n_probe == 0) {
    auto out = std::make_shared<ColumnarTable>();
    const auto& keep_tbl = n_build == 0 ? probe_tbl : build_tbl;
    const bool keep_left = (n_build == 0) ? !flip : flip;
    const Schema& null_schema = keep_left ? right.schema : left.schema;
    for (const ColumnarChunk& ch : keep_tbl->chunks) {
      if (ch.rows == 0) {
        continue;
      }
      ColumnarChunk dst;
      dst.rows = ch.rows;
      dst.string_arena = std::make_shared<ChunkStringArena>();
      if (!keep_left) {
        AppendNullColumnsToChunk(ch.rows, null_schema, &dst);
      }
      AppendAliasColumnsToChunk(ch, &dst);
      if (keep_left) {
        AppendNullColumnsToChunk(ch.rows, null_schema, &dst);
      }
      out->total_rows += dst.rows;
      out->chunks.push_back(std::move(dst));
    }
    return std::optional<Relation>{FinalizeColumnarJoinResult(
        context, result_schema, std::move(using_columns), stats_seed, 0,
        std::move(out))};
  }

  bool any_all_null_key = false;
  bool all_int_keys = !build_cols.empty();
  bool single_str_key = build_cols.size() == 1;
  for (size_t k = 0; k < build_cols.size(); ++k) {
    const ColumnarColSummary& bp = build_plan[build_cols[k]];
    const ColumnarColSummary& pp = probe_plan[probe_cols[k]];
    if (bp.all_null || pp.all_null) {
      any_all_null_key = true;
    }
    const bool int_ok = bp.all_uniform && pp.all_uniform &&
                        !bp.common_unsigned && !pp.common_unsigned &&
                        bp.common_type == pp.common_type &&
                        (bp.common_type == ValueType::kInt64 ||
                         bp.common_type == ValueType::kDate);
    all_int_keys &= int_ok;
    const bool str_ok = bp.all_uniform && pp.all_uniform &&
                        !bp.common_interval && !pp.common_interval &&
                        bp.common_type == ValueType::kVarChar &&
                        pp.common_type == ValueType::kVarChar;
    single_str_key &= str_ok;
  }

  std::vector<slot_t> res_left_slots;
  std::vector<slot_t> res_right_slots;
  const size_t left_width = left.schema.ColumnCount();
  std::vector<const BytecodeProgram*> active_residuals;
  if (compiled_residuals && !compiled_residuals->empty()) {
    if (compiled_residuals->size() == residual.size()) {
      std::vector<int8_t> key_bias_safe(left_columns.size(), -1);
      active_residuals.reserve(compiled_residuals->size());
      for (size_t pi = 0; pi < residual.size(); ++pi) {
        if (CanElideJoinResidualWithKeys(
                residual[pi], result_schema, left_width, flip, *build_tbl,
                *probe_tbl, left_columns, right_columns, left_key_offsets,
                right_key_offsets, build_cols, probe_cols, build_biases,
                probe_biases, build_plan, probe_plan, &key_bias_safe)) {
          continue;
        }
        active_residuals.push_back(&(*compiled_residuals)[pi]);
      }
    } else {
      active_residuals.reserve(compiled_residuals->size());
      for (const BytecodeProgram& program : *compiled_residuals) {
        active_residuals.push_back(&program);
      }
    }
  }
  const bool has_active_residuals = !active_residuals.empty();
  if (has_active_residuals) {
    for (const BytecodeProgram* program : active_residuals) {
      for (const BytecodeInstruction& inst : program->Instructions()) {
        if (inst.opcode == BytecodeOp::kLoadColumn) {
          const size_t slot = inst.operand;
          if (slot < left_width) {
            res_left_slots.push_back(static_cast<slot_t>(slot));
          } else if (slot < left_width + right.schema.ColumnCount()) {
            res_right_slots.push_back(static_cast<slot_t>(slot - left_width));
          }
        }
      }
    }
    std::ranges::sort(res_left_slots);
    res_left_slots.erase(std::ranges::unique(res_left_slots).begin(),
                         res_left_slots.end());
    std::ranges::sort(res_right_slots);
    res_right_slots.erase(std::ranges::unique(res_right_slots).begin(),
                          res_right_slots.end());
  }
  const std::vector<slot_t>& res_probe_slots =
      flip ? res_right_slots : res_left_slots;
  const std::vector<slot_t>& res_build_slots =
      flip ? res_left_slots : res_right_slots;
  const size_t res_probe_off = flip ? left_width : 0;
  const size_t res_build_off = flip ? 0 : left_width;

  struct FastResidualScaledDoubleCmp {
    bool active{false};
    bool probe_is_lhs{true};
    slot_t probe_col{0};
    slot_t build_col{0};
    double scale{1.0};
    BinaryOperation op{BinaryOperation::kLessThanEquals};
  } fast_res_scaled_cmp;
  if (active_residuals.size() == 1 && compiled_residuals.has_value() &&
      compiled_residuals->size() == residual.size()) {
    size_t active_pi = 0;
    for (size_t pi = 0; pi < residual.size(); ++pi) {
      if (&(*compiled_residuals)[pi] == active_residuals[0]) {
        active_pi = pi;
        break;
      }
    }
    const Expression& r_expr = residual[active_pi];
    if (r_expr && r_expr->Type() == TypeTag::kBinaryExp) {
      const auto& bin = r_expr->AsBinaryExpression();
      const BinaryOperation bop = bin.Op();
      if (bop == BinaryOperation::kLessThan ||
          bop == BinaryOperation::kLessThanEquals ||
          bop == BinaryOperation::kGreaterThan ||
          bop == BinaryOperation::kGreaterThanEquals) {
        auto unwrap_double_cast = [](const Expression& e) -> Expression {
          if (e && e->Type() == TypeTag::kCastExp &&
              e->AsCastExpression().TargetValueType() == ValueType::kDouble &&
              e->AsCastExpression().Child() != nullptr) {
            return e->AsCastExpression().Child();
          }
          return e;
        };
        struct ColSideRef {
          bool on_probe{false};
          slot_t col{0};
        };
        auto extract_col = [&](const Expression& raw_e,
                               ColSideRef* out) -> bool {
          const Expression& e = unwrap_double_cast(raw_e);
          if (!e || e->Type() != TypeTag::kColumnValue) {
            return false;
          }
          const auto off = LocalColumnOffset(
              result_schema, e->AsColumnValue().GetColumnName());
          if (!off.has_value() || *off >= result_schema.ColumnCount()) {
            return false;
          }
          const Column& c = result_schema.GetColumn(*off);
          if (c.IsUnsigned() || c.Type() != ValueType::kDouble) {
            return false;
          }
          if (*off < left_width) {
            out->on_probe = !flip;
            out->col = *off;
          } else {
            out->on_probe = flip;
            out->col = *off - left_width;
          }
          return true;
        };
        auto extract_const = [&](const Expression& raw_e, double* out) -> bool {
          const Expression& e = unwrap_double_cast(raw_e);
          if (!e || e->Type() != TypeTag::kConstantValue) {
            return false;
          }
          const Value& v = e->AsConstantValue().GetValue();
          if (v.IsNull()) {
            return false;
          }
          if (v.type == ValueType::kDouble) {
            if (!std::isfinite(v.value.double_value)) {
              return false;
            }
            *out = v.value.double_value;
            return true;
          }
          if (v.type == ValueType::kInt64 && !v.IsUnsigned()) {
            *out = static_cast<double>(v.value.int_value);
            return true;
          }
          return false;
        };
        auto extract_scaled = [&](const Expression& raw_e, ColSideRef* col_out,
                                  double* scale_out) -> bool {
          const Expression& e = unwrap_double_cast(raw_e);
          if (!e || e->Type() != TypeTag::kBinaryExp ||
              e->AsBinaryExpression().Op() != BinaryOperation::kMultiply) {
            return false;
          }
          const auto& mb = e->AsBinaryExpression();
          if (extract_const(mb.Left(), scale_out) &&
              extract_col(mb.Right(), col_out)) {
            return true;
          }
          if (extract_col(mb.Left(), col_out) &&
              extract_const(mb.Right(), scale_out)) {
            return true;
          }
          return false;
        };
        auto flip_op = [](BinaryOperation o) -> BinaryOperation {
          switch (o) {
            case BinaryOperation::kLessThan:
              return BinaryOperation::kGreaterThan;
            case BinaryOperation::kLessThanEquals:
              return BinaryOperation::kGreaterThanEquals;
            case BinaryOperation::kGreaterThan:
              return BinaryOperation::kLessThan;
            case BinaryOperation::kGreaterThanEquals:
              return BinaryOperation::kLessThanEquals;
            default:
              return o;
          }
        };
        ColSideRef ca;
        ColSideRef cb;
        double sc = 1.0;
        bool matched = false;
        BinaryOperation cmp_op = bop;
        if (extract_col(bin.Left(), &ca) &&
            extract_scaled(bin.Right(), &cb, &sc) &&
            ca.on_probe != cb.on_probe) {
          matched = true;
          cmp_op = bop;
        } else if (extract_scaled(bin.Left(), &cb, &sc) &&
                   extract_col(bin.Right(), &ca) &&
                   ca.on_probe != cb.on_probe) {
          matched = true;
          cmp_op = flip_op(bop);
        }
        if (matched) {
          const slot_t pcol = ca.on_probe ? ca.col : cb.col;
          const slot_t bcol = ca.on_probe ? cb.col : ca.col;
          if (probe_plan[pcol].all_uniform && build_plan[bcol].all_uniform &&
              (probe_plan[pcol].all_null ||
               probe_plan[pcol].common_type == ValueType::kDouble) &&
              (build_plan[bcol].all_null ||
               build_plan[bcol].common_type == ValueType::kDouble)) {
            fast_res_scaled_cmp = {.active = true,
                                   .probe_is_lhs = ca.on_probe,
                                   .probe_col = pcol,
                                   .build_col = bcol,
                                   .scale = sc,
                                   .op = cmp_op};
          }
        }
      }
    }
  }

  std::vector<uint32_t> build_chunk_of(n_build);
  std::vector<uint32_t> build_row_of(n_build);
  std::vector<uint32_t> chunk_base_bid(build_tbl->chunks.size(), 0);
  {
    uint32_t bid_acc = 0;
    for (size_t bc = 0; bc < build_tbl->chunks.size(); ++bc) {
      chunk_base_bid[bc] = bid_acc;
      bid_acc += static_cast<uint32_t>(build_tbl->chunks[bc].rows);
    }
  }
  std::vector<uint32_t> next_in_bucket(n_build, kColumnarEmptyBucket);

  enum class KeyMode {
    kEmptyNull,
    kSingleInt64,
    kPacked2x32,
    kMultiInt64,
    kSingleVarChar,
    kEncoded
  };
  KeyMode mode = KeyMode::kEncoded;
  if (any_all_null_key) {
    mode = KeyMode::kEmptyNull;
  } else if (all_int_keys && build_cols.size() == 1) {
    mode = KeyMode::kSingleInt64;
  } else if (all_int_keys && build_cols.size() == 2 &&
             ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_PACKED2X32_HJ") ==
                 nullptr &&
             JitPacked2x32HashProbe::Compile(false, false, false, false)
                 .has_value()) {
    mode = KeyMode::kPacked2x32;
  } else if (all_int_keys && build_cols.size() <= CompositeIntKey::kMaxParts) {
    mode = KeyMode::kMultiInt64;
  } else if (single_str_key) {
    mode = KeyMode::kSingleVarChar;
  }

  struct ChunkSliceTask {
    uint32_t chunk_idx{0};
    uint32_t row_begin{0};
    uint32_t row_end{0};
  };
  static const bool probe_disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_PARALLEL_PROBE") !=
      nullptr;
  const size_t max_w =
      QueryParallelWorkerLimit(std::thread::hardware_concurrency());

  static constexpr size_t kColumnarBuildShards = 64;
  const size_t build_workers =
      (!probe_disabled && max_w > 1 && mode != KeyMode::kEmptyNull &&
       ((mode == KeyMode::kEncoded && n_build >= 4096) || n_build >= 16384))
          ? std::min<size_t>(
                {kParallelProbeMaxWorkers, max_w, (n_build + 2047) / 2048})
          : 1;
  const bool sharded = build_workers > 1;

  auto int_shard_of = [](int64_t k) noexcept -> size_t {
    return (MixEstimateHash64(static_cast<uint64_t>(k)) >> 32) &
           (kColumnarBuildShards - 1);
  };

  FlatIntKeyMap<uint32_t> int_map;
  FlatCompositeIntMap comp_map;
  FlatStringViewMap str_map;
  std::vector<FlatIntKeyMap<uint32_t>> int_shards;
  std::vector<FlatCompositeIntMap> comp_shards;
  std::vector<FlatStringViewMap> str_shards;
  ChunkStringArena encoded_build_arena;
  std::vector<ChunkStringArena> encoded_worker_arenas;

  static_assert(kColumnarEmptyBucket == std::numeric_limits<uint32_t>::max());
  std::vector<uint64_t> packed_ht_keys;
  std::vector<uint32_t> packed_ht_heads;
  std::vector<uint64_t> packed_bloom_words;
  uint64_t packed_ht_mask = 0;
  uint64_t packed_bloom_word_mask = 0;
  bool packed_has_bloom = false;
  bool packed_built_chunk_map = false;

  if (mode == KeyMode::kPacked2x32) {
    struct PackedBuildItem {
      uint64_t key;
      uint32_t bid;
    };
    const slot_t bcol0 = build_cols[0];
    const slot_t bcol1 = build_cols[1];
    const int64_t b0 = build_biases[0];
    const int64_t b1 = build_biases[1];
    std::atomic<bool> packed_overflow{false};
    auto extract_slice = [&](const ColumnarChunk& ch, uint32_t bc, uint32_t r0,
                             uint32_t r1, SrcIntLeafCache* lc,
                             std::vector<PackedBuildItem>* out) {
      const uint32_t base = chunk_base_bid[bc];
      for (uint32_t r = r0; r < r1; ++r) {
        build_chunk_of[base + r] = bc;
        build_row_of[base + r] = r;
      }
      if (packed_overflow.load(std::memory_order_relaxed)) {
        return;
      }
      const ColumnarColumn& c0 = ch.columns[bcol0];
      const ColumnarColumn& c1 = ch.columns[bcol1];
      if (c0.encoding == ColumnarColumn::Encoding::kFlat && c0.is_uniform &&
          (!c0.has_null || c0.nulls.empty()) && c0.type != ValueType::kNull &&
          c1.encoding == ColumnarColumn::Encoding::kFlat && c1.is_uniform &&
          (!c1.has_null || c1.nulls.empty()) && c1.type != ValueType::kNull) {
        const int64_t* p0 = c0.ints.data();
        const int64_t* p1 = c1.ints.data();
        for (uint32_t r = r0; r < r1; ++r) {
          const int64_t v0 = b0 != 0 ? AddJoinKeyBias(p0[r], b0) : p0[r];
          const int64_t v1 = b1 != 0 ? AddJoinKeyBias(p1[r], b1) : p1[r];
          if (static_cast<int64_t>(static_cast<int32_t>(v0)) != v0 ||
              static_cast<int64_t>(static_cast<int32_t>(v1)) != v1) {
            packed_overflow.store(true, std::memory_order_relaxed);
            return;
          }
          const uint64_t packed =
              (static_cast<uint64_t>(
                   static_cast<uint32_t>(static_cast<int32_t>(v0)))
               << 32) |
              static_cast<uint64_t>(
                  static_cast<uint32_t>(static_cast<int32_t>(v1)));
          out->push_back({packed, base + r});
        }
        return;
      }
      std::array<int64_t, 64> t0{};
      std::array<int64_t, 64> t1{};
      for (uint32_t p = r0; p < r1; p += 64) {
        const uint32_t sub = std::min<uint32_t>(64, r1 - p);
        uint64_t valid = DecodeIntTile64(c0, p, sub, lc, t0.data());
        valid &= DecodeIntTile64(c1, p, sub, lc, t1.data());
        while (valid != 0) {
          const auto i = static_cast<uint32_t>(std::countr_zero(valid));
          valid &= (valid - 1);
          const int64_t v0 = b0 != 0 ? AddJoinKeyBias(t0[i], b0) : t0[i];
          const int64_t v1 = b1 != 0 ? AddJoinKeyBias(t1[i], b1) : t1[i];
          if (static_cast<int64_t>(static_cast<int32_t>(v0)) != v0 ||
              static_cast<int64_t>(static_cast<int32_t>(v1)) != v1) {
            packed_overflow.store(true, std::memory_order_relaxed);
            return;
          }
          const uint64_t packed =
              (static_cast<uint64_t>(
                   static_cast<uint32_t>(static_cast<int32_t>(v0)))
               << 32) |
              static_cast<uint64_t>(
                  static_cast<uint32_t>(static_cast<int32_t>(v1)));
          out->push_back({packed, base + p + i});
        }
      }
    };
    const size_t extract_workers = sharded ? build_workers : 1;
    std::vector<std::vector<PackedBuildItem>> worker_items(extract_workers);
    if (!sharded) {
      SrcIntLeafCache lc;
      worker_items[0].reserve(n_build);
      for (uint32_t bc = 0; bc < build_tbl->chunks.size(); ++bc) {
        const ColumnarChunk& ch = build_tbl->chunks[bc];
        extract_slice(ch, bc, 0, static_cast<uint32_t>(ch.rows), &lc,
                      worker_items.data());
        if (packed_overflow.load(std::memory_order_relaxed)) {
          break;
        }
      }
    } else {
      std::vector<ChunkSliceTask> ext_tasks;
      constexpr uint32_t kExtSliceRows = 4096;
      for (uint32_t bc = 0; bc < build_tbl->chunks.size(); ++bc) {
        const auto nrows = static_cast<uint32_t>(build_tbl->chunks[bc].rows);
        for (uint32_t r0 = 0; r0 < nrows; r0 += kExtSliceRows) {
          ext_tasks.push_back(
              {.chunk_idx = bc,
               .row_begin = r0,
               .row_end = std::min<uint32_t>(nrows, r0 + kExtSliceRows)});
        }
      }
      std::atomic<size_t> next_ext{0};
      RETURN_IF_FAIL(RunParallelTasks(extract_workers, [&](size_t w) {
        SrcIntLeafCache lc;
        auto& out = worker_items[w];
        out.reserve((n_build / extract_workers) + 64);
        while (true) {
          const size_t tid = next_ext.fetch_add(1, std::memory_order_relaxed);
          if (tid >= ext_tasks.size()) {
            break;
          }
          const ChunkSliceTask& t = ext_tasks[tid];
          extract_slice(build_tbl->chunks[t.chunk_idx], t.chunk_idx,
                        t.row_begin, t.row_end, &lc, &out);
        }
      }));
    }
    if (packed_overflow.load(std::memory_order_relaxed)) {
      mode = KeyMode::kMultiInt64;
    } else {
      size_t valid_build_count = 0;
      for (const auto& wi : worker_items) {
        valid_build_count += wi.size();
      }
      if (valid_build_count == 0) {
        mode = KeyMode::kEmptyNull;
        packed_built_chunk_map = true;
      } else {
        const size_t ht_cap =
            std::bit_ceil(std::max<size_t>(16, valid_build_count * 2));
        packed_ht_mask = ht_cap - 1;
        packed_ht_keys.assign(ht_cap, 0ULL);
        packed_ht_heads.assign(ht_cap, kColumnarEmptyBucket);
        packed_has_bloom = (valid_build_count >= 16384);
        if (packed_has_bloom) {
          const size_t bloom_bits = std::clamp<size_t>(
              std::bit_ceil(valid_build_count * 8), 4096, size_t{1} << 25);
          packed_bloom_words.assign(bloom_bits >> 6, 0ULL);
          packed_bloom_word_mask = (bloom_bits >> 6) - 1;
        }
        if (!sharded || ht_cap < 1024 || extract_workers <= 1) {
          for (auto& wi : worker_items) {
            for (const PackedBuildItem& it : wi) {
              const uint64_t h = HashPacked2x32Key(it.key);
              if (packed_has_bloom) {
                InsertPacked2x32Bloom(h, packed_bloom_words.data(),
                                      packed_bloom_word_mask);
              }
              auto slot = static_cast<size_t>(h & packed_ht_mask);
              while (packed_ht_heads[slot] != kColumnarEmptyBucket &&
                     packed_ht_keys[slot] != it.key) {
                slot = (slot + 1) & packed_ht_mask;
              }
              if (packed_ht_heads[slot] == kColumnarEmptyBucket) {
                packed_ht_keys[slot] = it.key;
                next_in_bucket[it.bid] = kColumnarEmptyBucket;
              } else {
                next_in_bucket[it.bid] = packed_ht_heads[slot];
              }
              packed_ht_heads[slot] = it.bid;
            }
            std::vector<PackedBuildItem>().swap(wi);
          }
        } else {
          constexpr size_t kBuildParts = 64;
          const size_t range_len = ht_cap >> 6;
          const uint32_t range_shift =
              static_cast<uint32_t>(std::bit_width(packed_ht_mask)) - 6U;
          struct PartItem {
            uint64_t key;
            uint64_t h;
            uint32_t bid;
          };
          std::vector<std::array<std::vector<PartItem>, kBuildParts>>
              part_items(extract_workers);
          RETURN_IF_FAIL(RunParallelTasks(extract_workers, [&](size_t w) {
            auto& parts = part_items[w];
            for (const PackedBuildItem& it : worker_items[w]) {
              const uint64_t h = HashPacked2x32Key(it.key);
              const auto p =
                  static_cast<size_t>((h & packed_ht_mask) >> range_shift);
              parts[p].push_back({it.key, h, it.bid});
            }
            std::vector<PackedBuildItem>().swap(worker_items[w]);
          }));
          std::array<std::vector<PartItem>, kBuildParts> overflow_items{};
          std::atomic<size_t> next_p{0};
          RETURN_IF_FAIL(RunParallelTasks(extract_workers, [&](size_t) {
            while (true) {
              const size_t p = next_p.fetch_add(1, std::memory_order_relaxed);
              if (p >= kBuildParts) {
                break;
              }
              const size_t r_begin = p * range_len;
              const size_t r_end = r_begin + range_len;
              for (size_t w = 0; w < extract_workers; ++w) {
                auto& bucket = part_items[w][p];
                for (const PartItem& it : bucket) {
                  if (packed_has_bloom) {
                    const auto w0 = static_cast<size_t>((it.h >> 6U) &
                                                        packed_bloom_word_mask);
                    const auto w1 = static_cast<size_t>((it.h >> 30U) &
                                                        packed_bloom_word_mask);
                    const uint64_t m0 = uint64_t{1} << (it.h & 63U);
                    const uint64_t m1 = uint64_t{1} << ((it.h >> 24U) & 63U);
                    __atomic_fetch_or(&packed_bloom_words[w0], m0,
                                      __ATOMIC_RELAXED);
                    __atomic_fetch_or(&packed_bloom_words[w1], m1,
                                      __ATOMIC_RELAXED);
                  }
                  auto slot = static_cast<size_t>(it.h & packed_ht_mask);
                  while (slot < r_end &&
                         packed_ht_heads[slot] != kColumnarEmptyBucket &&
                         packed_ht_keys[slot] != it.key) {
                    ++slot;
                  }
                  if (slot == r_end) {
                    overflow_items[p].push_back(it);
                  } else if (packed_ht_heads[slot] == kColumnarEmptyBucket) {
                    packed_ht_keys[slot] = it.key;
                    next_in_bucket[it.bid] = kColumnarEmptyBucket;
                    packed_ht_heads[slot] = it.bid;
                  } else {
                    next_in_bucket[it.bid] = packed_ht_heads[slot];
                    packed_ht_heads[slot] = it.bid;
                  }
                }
                std::vector<PartItem>().swap(bucket);
              }
            }
          }));
          for (size_t p = 0; p < kBuildParts; ++p) {
            for (const PartItem& it : overflow_items[p]) {
              auto slot = static_cast<size_t>(it.h & packed_ht_mask);
              while (packed_ht_heads[slot] != kColumnarEmptyBucket &&
                     packed_ht_keys[slot] != it.key) {
                slot = (slot + 1) & packed_ht_mask;
              }
              if (packed_ht_heads[slot] == kColumnarEmptyBucket) {
                packed_ht_keys[slot] = it.key;
                next_in_bucket[it.bid] = kColumnarEmptyBucket;
              } else {
                next_in_bucket[it.bid] = packed_ht_heads[slot];
              }
              packed_ht_heads[slot] = it.bid;
            }
          }
        }
      }
    }
  }

  if (mode == KeyMode::kPacked2x32 ||
      (mode == KeyMode::kEmptyNull && packed_built_chunk_map)) {
  } else if (!sharded) {
    for (uint32_t bc = 0; bc < build_tbl->chunks.size(); ++bc) {
      const uint32_t base = chunk_base_bid[bc];
      const auto rows = static_cast<uint32_t>(build_tbl->chunks[bc].rows);
      for (uint32_t r = 0; r < rows; ++r) {
        build_chunk_of[base + r] = bc;
        build_row_of[base + r] = r;
      }
    }
    if (mode == KeyMode::kSingleInt64) {
      int_map.Reserve(n_build);
      const slot_t bcol_idx = build_cols[0];
      const int64_t bias = build_biases[0];
      SrcIntLeafCache build_leaf_cache;
      for (size_t bc = 0; bc < build_tbl->chunks.size(); ++bc) {
        const ColumnarChunk& ch = build_tbl->chunks[bc];
        const uint32_t base = chunk_base_bid[bc];
        const ColumnarColumn& col = ch.columns[bcol_idx];
        if (col.encoding == ColumnarColumn::Encoding::kConstant) {
          if (col.IsNull(0)) {
            continue;
          }
          const int64_t k =
              bias != 0 ? AddJoinKeyBias(col.IntAt(0), bias) : col.IntAt(0);
          for (size_t r = 0; r < ch.rows; ++r) {
            const uint32_t bid = base + static_cast<uint32_t>(r);
            bool ins = false;
            uint32_t* head = int_map.TryEmplace(k, &ins);
            next_in_bucket[bid] = ins ? kColumnarEmptyBucket : *head;
            *head = bid;
          }
          continue;
        }
        if (col.encoding == ColumnarColumn::Encoding::kFlat && col.is_uniform &&
            (!col.has_null || col.nulls.empty()) &&
            col.type != ValueType::kNull) {
          const int64_t* ptr = col.ints.data();
          for (size_t r = 0; r < ch.rows; ++r) {
            const int64_t k = bias != 0 ? AddJoinKeyBias(ptr[r], bias) : ptr[r];
            const uint32_t bid = base + static_cast<uint32_t>(r);
            bool ins = false;
            uint32_t* head = int_map.TryEmplace(k, &ins);
            next_in_bucket[bid] = ins ? kColumnarEmptyBucket : *head;
            *head = bid;
          }
          continue;
        }
        std::array<int64_t, 64> tile_vals{};
        const auto nrows = static_cast<uint32_t>(ch.rows);
        for (uint32_t r0 = 0; r0 < nrows; r0 += 64) {
          const uint32_t cnt = std::min<uint32_t>(64, nrows - r0);
          uint64_t valid = DecodeIntTile64(col, r0, cnt, &build_leaf_cache,
                                           tile_vals.data());
          while (valid != 0) {
            const auto i = static_cast<uint32_t>(std::countr_zero(valid));
            valid &= (valid - 1);
            const int64_t k =
                bias != 0 ? AddJoinKeyBias(tile_vals[i], bias) : tile_vals[i];
            const uint32_t bid = base + r0 + i;
            bool ins = false;
            uint32_t* head = int_map.TryEmplace(k, &ins);
            next_in_bucket[bid] = ins ? kColumnarEmptyBucket : *head;
            *head = bid;
          }
        }
      }
    } else if (mode == KeyMode::kMultiInt64) {
      comp_map.Reserve(n_build);
      for (size_t bc = 0; bc < build_tbl->chunks.size(); ++bc) {
        const ColumnarChunk& ch = build_tbl->chunks[bc];
        const uint32_t base = chunk_base_bid[bc];
        for (size_t r = 0; r < ch.rows; ++r) {
          bool has_null = false;
          CompositeIntKey k;
          k.count = static_cast<uint8_t>(build_cols.size());
          for (size_t idx = 0; idx < build_cols.size(); ++idx) {
            const ColumnarColumn& col = ch.columns[build_cols[idx]];
            if (col.IsNull(r)) {
              has_null = true;
              break;
            }
            const int64_t raw = col.IntAt(r);
            k.parts[idx] = build_biases[idx] != 0
                               ? AddJoinKeyBias(raw, build_biases[idx])
                               : raw;
          }
          if (has_null) {
            continue;
          }
          const uint32_t bid = base + static_cast<uint32_t>(r);
          bool ins = false;
          uint32_t* head = comp_map.TryEmplace(k, &ins);
          next_in_bucket[bid] = ins ? kColumnarEmptyBucket : *head;
          *head = bid;
        }
      }
    } else if (mode == KeyMode::kSingleVarChar) {
      str_map.Reserve(n_build);
      const slot_t bcol_idx = build_cols[0];
      std::vector<uint64_t> dict_hashes;
      for (size_t bc = 0; bc < build_tbl->chunks.size(); ++bc) {
        const ColumnarChunk& ch = build_tbl->chunks[bc];
        const uint32_t base = chunk_base_bid[bc];
        const ColumnarColumn& col = ch.columns[bcol_idx];
        if (col.encoding == ColumnarColumn::Encoding::kDictionary &&
            col.dict != nullptr) {
          const size_t nd = col.DictEntryCount();
          dict_hashes.resize(nd);
          for (size_t d = 0; d < nd; ++d) {
            dict_hashes[d] =
                FlatStringViewMap::HashSv(col.dict->StringEntryAt(d));
          }
          for (size_t r = 0; r < ch.rows; ++r) {
            const uint32_t code = col.DictCodeAt(r);
            if (col.has_null &&
                (code == ColumnarColumn::kNullDictCode || code >= nd)) {
              continue;
            }
            const uint32_t bid = base + static_cast<uint32_t>(r);
            bool ins = false;
            uint32_t* head = str_map.TryEmplaceWithHash(
                col.dict->StringEntryAt(code), dict_hashes[code], &ins);
            next_in_bucket[bid] = ins ? kColumnarEmptyBucket : *head;
            *head = bid;
          }
          continue;
        }
        if (col.encoding == ColumnarColumn::Encoding::kFlat && col.is_uniform &&
            !col.has_null) {
          const std::string_view* ptr = col.strings.data();
          for (size_t r = 0; r < ch.rows; ++r) {
            const uint32_t bid = base + static_cast<uint32_t>(r);
            bool ins = false;
            uint32_t* head = str_map.TryEmplace(ptr[r], &ins);
            next_in_bucket[bid] = ins ? kColumnarEmptyBucket : *head;
            *head = bid;
          }
          continue;
        }
        for (size_t r = 0; r < ch.rows; ++r) {
          if (col.IsNull(r)) {
            continue;
          }
          const uint32_t bid = base + static_cast<uint32_t>(r);
          bool ins = false;
          uint32_t* head = str_map.TryEmplace(col.StringAt(r), &ins);
          next_in_bucket[bid] = ins ? kColumnarEmptyBucket : *head;
          *head = bid;
        }
      }
    } else if (mode == KeyMode::kEncoded) {
      str_map.Reserve(n_build);
      Value scratch;
      std::string key_buf;
      for (size_t bc = 0; bc < build_tbl->chunks.size(); ++bc) {
        const ColumnarChunk& ch = build_tbl->chunks[bc];
        const uint32_t base = chunk_base_bid[bc];
        for (size_t r = 0; r < ch.rows; ++r) {
          if (!TryColumnarEncodeJoinKeyNonNull(
                  ch, build_cols, r, true, &build_biases, &scratch, &key_buf)) {
            continue;
          }
          const std::string_view stored = encoded_build_arena.Store(key_buf);
          const uint32_t bid = base + static_cast<uint32_t>(r);
          bool ins = false;
          uint32_t* head = str_map.TryEmplace(stored, &ins);
          next_in_bucket[bid] = ins ? kColumnarEmptyBucket : *head;
          *head = bid;
        }
      }
    }
  } else {
    std::vector<ChunkSliceTask> build_tasks;
    constexpr uint32_t kBuildSliceRows = 4096;
    for (uint32_t bc = 0; bc < build_tbl->chunks.size(); ++bc) {
      const auto nrows = static_cast<uint32_t>(build_tbl->chunks[bc].rows);
      for (uint32_t r0 = 0; r0 < nrows; r0 += kBuildSliceRows) {
        build_tasks.push_back(
            {.chunk_idx = bc,
             .row_begin = r0,
             .row_end = std::min<uint32_t>(nrows, r0 + kBuildSliceRows)});
      }
    }
    const size_t shard_mask = kColumnarBuildShards - 1;
    std::atomic<size_t> next_build_task{0};
    std::atomic<size_t> next_shard{0};

    if (mode == KeyMode::kSingleInt64) {
      struct IntItem {
        int64_t key;
        uint32_t bid;
      };
      int_shards.resize(kColumnarBuildShards);
      std::vector<std::vector<IntItem>> buckets(build_workers *
                                                kColumnarBuildShards);
      const slot_t bcol_idx = build_cols[0];
      const int64_t bias = build_biases[0];
      RETURN_IF_FAIL(RunParallelTasks(build_workers, [&](size_t w) {
        const size_t w_base = w * kColumnarBuildShards;
        SrcIntLeafCache build_leaf_cache;
        while (true) {
          const size_t tid =
              next_build_task.fetch_add(1, std::memory_order_relaxed);
          if (tid >= build_tasks.size()) {
            break;
          }
          const ChunkSliceTask task = build_tasks[tid];
          const uint32_t bc = task.chunk_idx;
          const ColumnarChunk& ch = build_tbl->chunks[bc];
          const uint32_t base = chunk_base_bid[bc];
          for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
            build_chunk_of[base + r] = bc;
            build_row_of[base + r] = r;
          }
          const ColumnarColumn& col = ch.columns[bcol_idx];
          if (col.encoding == ColumnarColumn::Encoding::kConstant) {
            if (col.IsNull(0)) {
              continue;
            }
            const int64_t k =
                bias != 0 ? AddJoinKeyBias(col.IntAt(0), bias) : col.IntAt(0);
            auto& dst = buckets[w_base + int_shard_of(k)];
            for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
              dst.push_back({k, base + r});
            }
            continue;
          }
          if (col.encoding == ColumnarColumn::Encoding::kFlat &&
              col.is_uniform && (!col.has_null || col.nulls.empty()) &&
              col.type != ValueType::kNull) {
            const int64_t* ptr = col.ints.data();
            for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
              const int64_t k =
                  bias != 0 ? AddJoinKeyBias(ptr[r], bias) : ptr[r];
              buckets[w_base + int_shard_of(k)].push_back({k, base + r});
            }
            continue;
          }
          std::array<int64_t, 64> tile_vals{};
          for (uint32_t r0 = task.row_begin; r0 < task.row_end; r0 += 64) {
            const uint32_t cnt = std::min<uint32_t>(64, task.row_end - r0);
            uint64_t valid = DecodeIntTile64(col, r0, cnt, &build_leaf_cache,
                                             tile_vals.data());
            while (valid != 0) {
              const auto i = static_cast<uint32_t>(std::countr_zero(valid));
              valid &= (valid - 1);
              const int64_t k =
                  bias != 0 ? AddJoinKeyBias(tile_vals[i], bias) : tile_vals[i];
              buckets[w_base + int_shard_of(k)].push_back({k, base + r0 + i});
            }
          }
        }
      }));
      RETURN_IF_FAIL(RunParallelTasks(build_workers, [&](size_t) {
        while (true) {
          const size_t s = next_shard.fetch_add(1, std::memory_order_relaxed);
          if (s >= kColumnarBuildShards) {
            break;
          }
          size_t total = 0;
          for (size_t w = 0; w < build_workers; ++w) {
            total += buckets[(w * kColumnarBuildShards) + s].size();
          }
          if (total == 0) {
            continue;
          }
          FlatIntKeyMap<uint32_t>& map = int_shards[s];
          map.Reserve(total);
          for (size_t w = 0; w < build_workers; ++w) {
            for (const IntItem& it : buckets[(w * kColumnarBuildShards) + s]) {
              bool ins = false;
              uint32_t* head = map.TryEmplace(it.key, &ins);
              next_in_bucket[it.bid] = ins ? kColumnarEmptyBucket : *head;
              *head = it.bid;
            }
          }
        }
      }));
    } else if (mode == KeyMode::kMultiInt64) {
      struct CompItem {
        uint64_t h;
        CompositeIntKey key;
        uint32_t bid;
      };
      comp_shards.resize(kColumnarBuildShards);
      std::vector<std::vector<CompItem>> buckets(build_workers *
                                                 kColumnarBuildShards);
      RETURN_IF_FAIL(RunParallelTasks(build_workers, [&](size_t w) {
        const size_t w_base = w * kColumnarBuildShards;
        while (true) {
          const size_t tid =
              next_build_task.fetch_add(1, std::memory_order_relaxed);
          if (tid >= build_tasks.size()) {
            break;
          }
          const ChunkSliceTask task = build_tasks[tid];
          const uint32_t bc = task.chunk_idx;
          const ColumnarChunk& ch = build_tbl->chunks[bc];
          const uint32_t base = chunk_base_bid[bc];
          for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
            build_chunk_of[base + r] = bc;
            build_row_of[base + r] = r;
          }
          for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
            bool has_null = false;
            CompositeIntKey k;
            k.count = static_cast<uint8_t>(build_cols.size());
            for (size_t idx = 0; idx < build_cols.size(); ++idx) {
              const ColumnarColumn& col = ch.columns[build_cols[idx]];
              if (col.IsNull(r)) {
                has_null = true;
                break;
              }
              const int64_t raw = col.IntAt(r);
              k.parts[idx] = build_biases[idx] != 0
                                 ? AddJoinKeyBias(raw, build_biases[idx])
                                 : raw;
            }
            if (has_null) {
              continue;
            }
            const uint64_t h = FlatCompositeIntMap::HashKey(k);
            buckets[w_base + ((h >> 32) & shard_mask)].push_back(
                {h, k, base + r});
          }
        }
      }));
      RETURN_IF_FAIL(RunParallelTasks(build_workers, [&](size_t) {
        while (true) {
          const size_t s = next_shard.fetch_add(1, std::memory_order_relaxed);
          if (s >= kColumnarBuildShards) {
            break;
          }
          size_t total = 0;
          for (size_t w = 0; w < build_workers; ++w) {
            total += buckets[(w * kColumnarBuildShards) + s].size();
          }
          if (total == 0) {
            continue;
          }
          FlatCompositeIntMap& map = comp_shards[s];
          map.Reserve(total);
          for (size_t w = 0; w < build_workers; ++w) {
            for (const CompItem& it : buckets[(w * kColumnarBuildShards) + s]) {
              bool ins = false;
              uint32_t* head = map.TryEmplaceWithHash(it.key, it.h, &ins);
              next_in_bucket[it.bid] = ins ? kColumnarEmptyBucket : *head;
              *head = it.bid;
            }
          }
        }
      }));
    } else {
      struct StrItem {
        uint64_t h;
        std::string_view sv;
        uint32_t bid;
      };
      str_shards.resize(kColumnarBuildShards);
      if (mode == KeyMode::kEncoded) {
        encoded_worker_arenas.resize(build_workers);
      }
      std::vector<std::vector<StrItem>> buckets(build_workers *
                                                kColumnarBuildShards);
      const slot_t bcol_idx =
          mode == KeyMode::kSingleVarChar ? build_cols[0] : 0;
      RETURN_IF_FAIL(RunParallelTasks(build_workers, [&](size_t w) {
        const size_t w_base = w * kColumnarBuildShards;
        std::vector<uint64_t> dict_hashes;
        Value scratch;
        std::string key_buf;
        while (true) {
          const size_t tid =
              next_build_task.fetch_add(1, std::memory_order_relaxed);
          if (tid >= build_tasks.size()) {
            break;
          }
          const ChunkSliceTask task = build_tasks[tid];
          const uint32_t bc = task.chunk_idx;
          const ColumnarChunk& ch = build_tbl->chunks[bc];
          const uint32_t base = chunk_base_bid[bc];
          for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
            build_chunk_of[base + r] = bc;
            build_row_of[base + r] = r;
          }
          if (mode == KeyMode::kSingleVarChar) {
            const ColumnarColumn& col = ch.columns[bcol_idx];
            if (col.encoding == ColumnarColumn::Encoding::kDictionary &&
                col.dict != nullptr) {
              const size_t nd = col.DictEntryCount();
              const bool prehash =
                  nd <= static_cast<size_t>(task.row_end - task.row_begin) * 2;
              if (prehash) {
                dict_hashes.resize(nd);
                for (size_t d = 0; d < nd; ++d) {
                  dict_hashes[d] =
                      FlatStringViewMap::HashSv(col.dict->StringEntryAt(d));
                }
              }
              for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
                const uint32_t code = col.DictCodeAt(r);
                if (col.has_null &&
                    (code == ColumnarColumn::kNullDictCode || code >= nd)) {
                  continue;
                }
                const std::string_view sv = col.dict->StringEntryAt(code);
                const uint64_t h =
                    prehash ? dict_hashes[code] : FlatStringViewMap::HashSv(sv);
                buckets[w_base + ((h >> 32) & shard_mask)].push_back(
                    {h, sv, base + r});
              }
              continue;
            }
            if (col.encoding == ColumnarColumn::Encoding::kFlat &&
                col.is_uniform && !col.has_null) {
              const std::string_view* ptr = col.strings.data();
              for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
                const uint64_t h = FlatStringViewMap::HashSv(ptr[r]);
                buckets[w_base + ((h >> 32) & shard_mask)].push_back(
                    {h, ptr[r], base + r});
              }
              continue;
            }
            for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
              if (col.IsNull(r)) {
                continue;
              }
              const std::string_view sv = col.StringAt(r);
              const uint64_t h = FlatStringViewMap::HashSv(sv);
              buckets[w_base + ((h >> 32) & shard_mask)].push_back(
                  {h, sv, base + r});
            }
          } else {
            ChunkStringArena& arena = encoded_worker_arenas[w];
            for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
              if (!TryColumnarEncodeJoinKeyNonNull(ch, build_cols, r, true,
                                                   &build_biases, &scratch,
                                                   &key_buf)) {
                continue;
              }
              const std::string_view stored = arena.Store(key_buf);
              const uint64_t h = FlatStringViewMap::HashSv(stored);
              buckets[w_base + ((h >> 32) & shard_mask)].push_back(
                  {h, stored, base + r});
            }
          }
        }
      }));
      RETURN_IF_FAIL(RunParallelTasks(build_workers, [&](size_t) {
        while (true) {
          const size_t s = next_shard.fetch_add(1, std::memory_order_relaxed);
          if (s >= kColumnarBuildShards) {
            break;
          }
          size_t total = 0;
          for (size_t w = 0; w < build_workers; ++w) {
            total += buckets[(w * kColumnarBuildShards) + s].size();
          }
          if (total == 0) {
            continue;
          }
          FlatStringViewMap& map = str_shards[s];
          map.Reserve(total);
          for (size_t w = 0; w < build_workers; ++w) {
            for (const StrItem& it : buckets[(w * kColumnarBuildShards) + s]) {
              bool ins = false;
              uint32_t* head = map.TryEmplaceWithHash(it.sv, it.h, &ins);
              next_in_bucket[it.bid] = ins ? kColumnarEmptyBucket : *head;
              *head = it.bid;
            }
          }
        }
      }));
    }
  }

  auto find_int = [&](int64_t k) noexcept -> const uint32_t* {
    return sharded ? int_shards[int_shard_of(k)].Find(k) : int_map.Find(k);
  };
  auto find_comp = [&](const CompositeIntKey& k) noexcept -> const uint32_t* {
    if (!sharded) {
      return comp_map.Find(k);
    }
    const uint64_t h = FlatCompositeIntMap::HashKey(k);
    return comp_shards[(h >> 32) & (kColumnarBuildShards - 1)].FindWithHash(k,
                                                                            h);
  };
  auto find_str = [&](std::string_view sv) noexcept -> const uint32_t* {
    if (!sharded) {
      return str_map.Find(sv);
    }
    const uint64_t h = FlatStringViewMap::HashSv(sv);
    return str_shards[(h >> 32) & (kColumnarBuildShards - 1)].FindWithHash(sv,
                                                                           h);
  };

  std::vector<ChunkSliceTask> tasks;
  constexpr uint32_t kProbeSliceRows = 8192;
  for (uint32_t pc = 0; pc < probe_tbl->chunks.size(); ++pc) {
    const auto nrows = static_cast<uint32_t>(probe_tbl->chunks[pc].rows);
    for (uint32_t r0 = 0; r0 < nrows; r0 += kProbeSliceRows) {
      tasks.push_back(
          {.chunk_idx = pc,
           .row_begin = r0,
           .row_end = std::min<uint32_t>(nrows, r0 + kProbeSliceRows)});
    }
  }

  const size_t workers =
      (!probe_disabled && max_w > 1 && n_probe >= 8192 && tasks.size() > 1)
          ? std::min<size_t>({kParallelProbeMaxWorkers, max_w, tasks.size()})
          : 1;

  std::vector<uint8_t> shared_build_matched(want_build_nulls ? n_build : 0, 0);
  struct WorkerOutput {
    std::vector<ColumnarChunk> chunks;
    uint64_t comparisons{0};
  };
  std::vector<WorkerOutput> worker_outputs(workers);
  std::atomic<size_t> next_task{0};
  std::atomic<bool> probe_failed{false};
  std::mutex error_mutex;
  Status first_error{Status::kSuccess};

  // fill_build_side / emit_unmatched_slice only gather groups referenced by
  // non-all-null build columns; mark them once so the per-batch loops below
  // skip building indexes nobody reads (P101).
  std::vector<uint8_t> build_group_used(num_build_groups, 0);
  for (size_t c = 0; c < build_schema.ColumnCount(); ++c) {
    if (!build_plan[c].all_null) {
      build_group_used[build_groups[c]] = 1;
    }
  }

  // The probe-side gather grouping depends only on the chunk's column
  // encodings, not on which rows matched. Derive it once per chunk here
  // (serially, before the workers start) instead of re-deriving it inside
  // emit_matched_batch for every 2048-row output batch (P101).
  struct ProbeEmitLayout {
    std::vector<size_t> group_of;
    std::vector<size_t> target_cols;
    std::vector<size_t> repr_cols;
    std::vector<const ChunkGatherIndex*> repr_sgi;
  };
  const auto gather_source_of =
      [](const ColumnarColumn& col) -> const ChunkGatherIndex* {
    return (col.encoding == ColumnarColumn::Encoding::kGathered &&
            col.gather_index != nullptr &&
            col.gather_index->source_table != nullptr)
               ? col.gather_index.get()
               : nullptr;
  };
  std::vector<ProbeEmitLayout> probe_layouts(probe_tbl->chunks.size());
  for (uint32_t pc = 0; pc < probe_tbl->chunks.size(); ++pc) {
    const ColumnarChunk& pchunk = probe_tbl->chunks[pc];
    ProbeEmitLayout& layout = probe_layouts[pc];
    const size_t n_probe_cols = probe_schema.ColumnCount();
    layout.group_of.resize(n_probe_cols, 0);
    layout.target_cols.resize(n_probe_cols, 0);
    for (size_t c = 0; c < n_probe_cols; ++c) {
      const ColumnarColumn& pcol = pchunk.columns[c];
      const ChunkGatherIndex* sgi = gather_source_of(pcol);
      const ColumnarTable* stbl =
          sgi != nullptr ? sgi->source_table.get() : probe_tbl.get();
      layout.target_cols[c] = sgi != nullptr ? pcol.gather_col : c;
      size_t g = 0;
      for (; g < layout.repr_cols.size(); ++g) {
        const ChunkGatherIndex* rgi = layout.repr_sgi[g];
        const ColumnarTable* rtbl =
            rgi != nullptr ? rgi->source_table.get() : probe_tbl.get();
        if (rgi == sgi && rtbl == stbl) {
          break;
        }
      }
      if (g == layout.repr_cols.size()) {
        layout.repr_cols.push_back(c);
        layout.repr_sgi.push_back(sgi);
      }
      layout.group_of[c] = g;
    }
  }

  auto emit_matched_batch = [&](uint32_t pc, std::span<const uint32_t> p_rows,
                                std::span<const uint32_t> b_ids,
                                std::vector<ColumnarChunk>* out_chunks) {
    const size_t n = p_rows.size();
    if (n == 0) {
      return;
    }
    const ColumnarChunk& pchunk = probe_tbl->chunks[pc];
    const size_t n_probe_cols = probe_schema.ColumnCount();
    const ProbeEmitLayout& layout = probe_layouts[pc];
    std::vector<std::shared_ptr<ChunkGatherIndex>> p_gis;
    p_gis.reserve(layout.repr_cols.size());
    for (size_t g = 0; g < layout.repr_cols.size(); ++g) {
      const ColumnarColumn& pcol = pchunk.columns[layout.repr_cols[g]];
      const ChunkGatherIndex* sgi = layout.repr_sgi[g];
      auto gi = std::make_shared<ChunkGatherIndex>();
      gi->source_table =
          sgi != nullptr ? pcol.gather_index->source_table : probe_tbl;
      gi->src_chunks.reserve(n);
      gi->src_rows.reserve(n);
      if (sgi != nullptr) {
        for (uint32_t pr : p_rows) {
          const uint32_t tc = sgi->src_chunks[pr];
          const uint32_t tr = sgi->src_rows[pr];
          gi->src_chunks.push_back(tc);
          gi->src_rows.push_back(tr);
          if (tr == ChunkGatherIndex::kNullRow) {
            gi->has_null = true;
          }
        }
      } else {
        gi->src_chunks.assign(n, pc);
        gi->src_rows.insert(gi->src_rows.end(), p_rows.begin(), p_rows.end());
      }
      p_gis.push_back(std::move(gi));
    }

    std::vector<std::shared_ptr<ChunkGatherIndex>> b_gis(num_build_groups);
    for (size_t g = 0; g < num_build_groups; ++g) {
      if (build_group_used[g] == 0) {
        continue;
      }
      const size_t rc = build_group_repr[g];
      const bool flat = build_plan[rc].same_leaf_target;
      auto gi = std::make_shared<ChunkGatherIndex>();
      gi->source_table = flat ? build_plan[rc].leaf_owner : build_tbl;
      gi->src_chunks.reserve(n);
      gi->src_rows.reserve(n);
      for (uint32_t bid : b_ids) {
        if (bid == kColumnarEmptyBucket) {
          gi->src_chunks.push_back(0);
          gi->src_rows.push_back(ChunkGatherIndex::kNullRow);
          gi->has_null = true;
        } else {
          const uint32_t bc = build_chunk_of[bid];
          const uint32_t br = build_row_of[bid];
          if (flat && build_tbl->chunks[bc].columns[rc].encoding ==
                          ColumnarColumn::Encoding::kGathered) {
            const ChunkGatherIndex* sgi =
                build_tbl->chunks[bc].columns[rc].gather_index.get();
            const uint32_t tc = sgi->src_chunks[br];
            const uint32_t tr = sgi->src_rows[br];
            gi->src_chunks.push_back(tc);
            gi->src_rows.push_back(tr);
            if (tr == ChunkGatherIndex::kNullRow) {
              gi->has_null = true;
            }
          } else {
            gi->src_chunks.push_back(bc);
            gi->src_rows.push_back(br);
          }
        }
      }
      b_gis[g] = std::move(gi);
    }

    ColumnarChunk out_ch;
    out_ch.rows = n;
    out_ch.columns.resize(result_schema.ColumnCount());
    auto fill_probe_side = [&](size_t dst_offset) {
      for (size_t c = 0; c < n_probe_cols; ++c) {
        const ColumnarColumn& src_col = pchunk.columns[c];
        const ValueType vt = src_col.type != ValueType::kNull
                                 ? src_col.type
                                 : probe_schema.GetColumn(c).Type();
        if (src_col.encoding == ColumnarColumn::Encoding::kConstant &&
            (src_col.const_is_null || src_col.type == ValueType::kNull)) {
          out_ch.columns[dst_offset + c].SetConstantNull(
              vt, src_col.is_unsigned, src_col.is_interval, n);
          continue;
        }
        const auto& gi = p_gis[layout.group_of[c]];
        out_ch.columns[dst_offset + c].SetGathered(
            gi, layout.target_cols[c], vt, src_col.is_unsigned,
            src_col.is_interval, src_col.is_uniform,
            src_col.has_null || src_col.type == ValueType::kNull ||
                gi->has_null);
      }
    };
    auto fill_build_side = [&](size_t dst_offset) {
      for (size_t c = 0; c < build_schema.ColumnCount(); ++c) {
        const ColumnarColSummary& bs = build_plan[c];
        const ValueType vt = bs.common_type != ValueType::kNull
                                 ? bs.common_type
                                 : build_schema.GetColumn(c).Type();
        if (bs.all_null) {
          out_ch.columns[dst_offset + c].SetConstantNull(vt, bs.common_unsigned,
                                                         bs.common_interval, n);
          continue;
        }
        const size_t tcol = bs.same_leaf_target ? bs.leaf_col : c;
        const auto& gi = b_gis[build_groups[c]];
        out_ch.columns[dst_offset + c].SetGathered(
            gi, tcol, vt, bs.common_unsigned, bs.common_interval,
            bs.all_uniform, bs.any_null || gi->has_null);
      }
    };
    if (flip) {
      fill_build_side(0);
      fill_probe_side(build_schema.ColumnCount());
    } else {
      fill_probe_side(0);
      fill_build_side(n_probe_cols);
    }
    out_chunks->push_back(std::move(out_ch));
  };

  auto worker_fn = [&](size_t wid) {
    WorkerOutput& wout = worker_outputs[wid];
    SrcIntLeafCache probe_leaf_cache;
    Row scratch_row;
    if (has_active_residuals && !fast_res_scaled_cmp.active) {
      scratch_row.values_.resize(result_schema.ColumnCount());
    }
    Value key_scratch;
    std::string key_buf;
    std::vector<uint32_t> dict_heads;
    std::vector<uint32_t> match_p_rows;
    std::vector<uint32_t> match_b_ids;
    match_p_rows.reserve(4096);
    match_b_ids.reserve(4096);
    std::array<std::optional<JitPacked2x32HashProbe>, 16> packed_probe_cache;
    std::vector<int64_t> packed_tile_k0;
    std::vector<int64_t> packed_tile_k1;
    std::vector<uint8_t> packed_tile_n0;
    std::vector<uint8_t> packed_tile_n1;
    std::vector<uint32_t> packed_tile_heads;
    if (mode == KeyMode::kPacked2x32) {
      packed_tile_k0.resize(4096);
      packed_tile_k1.resize(4096);
      packed_tile_n0.resize(4096);
      packed_tile_n1.resize(4096);
      packed_tile_heads.resize(4096);
    }

    while (true) {
      // Only the cancellation flag is shared without the error lock.
      if (probe_failed.load(std::memory_order_relaxed)) {
        return;
      }
      const size_t tid = next_task.fetch_add(1, std::memory_order_relaxed);
      if (tid >= tasks.size()) {
        break;
      }
      const ChunkSliceTask task = tasks[tid];
      const ColumnarChunk& pchunk = probe_tbl->chunks[task.chunk_idx];
      match_p_rows.clear();
      match_b_ids.clear();

      auto flush_matches = [&]() {
        if (match_p_rows.size() >= kColumnarChunkRows) {
          size_t off = 0;
          while (match_p_rows.size() - off >= kColumnarChunkRows) {
            emit_matched_batch(
                task.chunk_idx,
                std::span<const uint32_t>(match_p_rows.data() + off,
                                          kColumnarChunkRows),
                std::span<const uint32_t>(match_b_ids.data() + off,
                                          kColumnarChunkRows),
                &wout.chunks);
            off += kColumnarChunkRows;
          }
          if (off > 0) {
            match_p_rows.erase(
                match_p_rows.begin(),
                match_p_rows.begin() + static_cast<ptrdiff_t>(off));
            match_b_ids.erase(
                match_b_ids.begin(),
                match_b_ids.begin() + static_cast<ptrdiff_t>(off));
          }
        }
      };

      auto handle_candidates = [&](uint32_t pr, uint32_t head) {
        bool matched = false;
        if (head != kColumnarEmptyBucket) {
          if (fast_res_scaled_cmp.active) {
            double pd = 0.0;
            if (pchunk.columns[fast_res_scaled_cmp.probe_col]
                    .TryReadNonNullDouble(pr, &pd)) {
              const slot_t rbcol = fast_res_scaled_cmp.build_col;
              const double sc = fast_res_scaled_cmp.scale;
              const bool p_lhs = fast_res_scaled_cmp.probe_is_lhs;
              const BinaryOperation rop = fast_res_scaled_cmp.op;
              for (uint32_t bid = head; bid != kColumnarEmptyBucket;
                   bid = next_in_bucket[bid]) {
                ++wout.comparisons;
                const uint32_t bc = build_chunk_of[bid];
                const uint32_t br = build_row_of[bid];
                double bd = 0.0;
                if (!build_tbl->chunks[bc].columns[rbcol].TryReadNonNullDouble(
                        br, &bd)) {
                  continue;
                }
                const double lhs_v = p_lhs ? pd : bd;
                const double rhs_v = sc * (p_lhs ? bd : pd);
                bool accepted = false;
                switch (rop) {
                  case BinaryOperation::kLessThan:
                    accepted = (lhs_v < rhs_v);
                    break;
                  case BinaryOperation::kLessThanEquals:
                    accepted = (lhs_v <= rhs_v);
                    break;
                  case BinaryOperation::kGreaterThan:
                    accepted = (lhs_v > rhs_v);
                    break;
                  case BinaryOperation::kGreaterThanEquals:
                    accepted = (lhs_v >= rhs_v);
                    break;
                  default:
                    break;
                }
                if (!accepted) {
                  continue;
                }
                matched = true;
                if (want_build_nulls &&
                    __atomic_load_n(&shared_build_matched[bid],
                                    __ATOMIC_RELAXED) == 0) {
                  __atomic_store_n(&shared_build_matched[bid], uint8_t{1},
                                   __ATOMIC_RELAXED);
                }
                match_p_rows.push_back(pr);
                match_b_ids.push_back(bid);
                if (match_p_rows.size() >= kColumnarChunkRows) {
                  flush_matches();
                }
              }
            } else {
              for (uint32_t bid = head; bid != kColumnarEmptyBucket;
                   bid = next_in_bucket[bid]) {
                ++wout.comparisons;
              }
            }
          } else {
            bool loaded_probe_res = false;
            for (uint32_t bid = head; bid != kColumnarEmptyBucket;
                 bid = next_in_bucket[bid]) {
              ++wout.comparisons;
              if (has_active_residuals) {
                if (!loaded_probe_res) {
                  for (slot_t ps : res_probe_slots) {
                    pchunk.columns[ps].WriteValueInto(
                        pr, &scratch_row.values_[res_probe_off + ps]);
                  }
                  loaded_probe_res = true;
                }
                const uint32_t bc = build_chunk_of[bid];
                const uint32_t br = build_row_of[bid];
                const ColumnarChunk& bchunk = build_tbl->chunks[bc];
                for (slot_t bs : res_build_slots) {
                  bchunk.columns[bs].WriteValueInto(
                      br, &scratch_row.values_[res_build_off + bs]);
                }
                bool accepted = true;
                for (const BytecodeProgram* program : active_residuals) {
                  auto val = TryEvaluateRowAuto(*program, scratch_row);
                  if (!val.HasValue()) {
                    std::scoped_lock lock(error_mutex);
                    if (first_error.ok()) {
                      first_error = val.GetStatus();
                      probe_failed.store(true, std::memory_order_relaxed);
                    }
                    return false;
                  }
                  if (!Truthy(val.Value())) {
                    accepted = false;
                    break;
                  }
                }
                if (!accepted) {
                  continue;
                }
              }
              matched = true;
              if (want_build_nulls &&
                  __atomic_load_n(&shared_build_matched[bid],
                                  __ATOMIC_RELAXED) == 0) {
                __atomic_store_n(&shared_build_matched[bid], uint8_t{1},
                                 __ATOMIC_RELAXED);
              }
              match_p_rows.push_back(pr);
              match_b_ids.push_back(bid);
              if (match_p_rows.size() >= kColumnarChunkRows) {
                flush_matches();
              }
            }
          }
        }
        if (!matched && probe_outer_emit) {
          match_p_rows.push_back(pr);
          match_b_ids.push_back(kColumnarEmptyBucket);
          if (match_p_rows.size() >= kColumnarChunkRows) {
            flush_matches();
          }
        }
        return true;
      };

      if (mode == KeyMode::kEmptyNull) {
        if (probe_outer_emit) {
          for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
            if (!handle_candidates(r, kColumnarEmptyBucket)) {
              return;
            }
          }
        }
      } else if (mode == KeyMode::kSingleInt64) {
        const ColumnarColumn& pcol = pchunk.columns[probe_cols[0]];
        const int64_t bias = probe_biases[0];
        if (pcol.encoding == ColumnarColumn::Encoding::kConstant) {
          uint32_t head = kColumnarEmptyBucket;
          if (!pcol.IsNull(0)) {
            const int64_t k =
                bias != 0 ? AddJoinKeyBias(pcol.IntAt(0), bias) : pcol.IntAt(0);
            if (const uint32_t* found = find_int(k)) {
              head = *found;
            }
          }
          if (head != kColumnarEmptyBucket || probe_outer_emit) {
            for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
              if (!handle_candidates(r, head)) {
                return;
              }
            }
          }
        } else if (pcol.encoding == ColumnarColumn::Encoding::kDictionary &&
                   pcol.dict != nullptr && pcol.DictEntryCount() <= 65536) {
          const size_t nd = pcol.DictEntryCount();
          dict_heads.assign(nd, kColumnarEmptyBucket);
          bool any_hit = false;
          for (size_t d = 0; d < nd; ++d) {
            const int64_t raw = pcol.dict->IntEntryAt(d);
            const int64_t k = bias != 0 ? AddJoinKeyBias(raw, bias) : raw;
            if (const uint32_t* found = find_int(k)) {
              dict_heads[d] = *found;
              any_hit = true;
            }
          }
          if (any_hit || probe_outer_emit) {
            for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
              const uint32_t code = pcol.DictCodeAt(r);
              const uint32_t head =
                  (pcol.has_null &&
                   (code == ColumnarColumn::kNullDictCode || code >= nd))
                      ? kColumnarEmptyBucket
                      : dict_heads[code];
              if (!handle_candidates(r, head)) {
                return;
              }
            }
          }
        } else if (pcol.encoding == ColumnarColumn::Encoding::kFlat &&
                   pcol.is_uniform && (!pcol.has_null || pcol.nulls.empty()) &&
                   pcol.type != ValueType::kNull) {
          const int64_t* ptr = pcol.ints.data();
          for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
            const int64_t k = bias != 0 ? AddJoinKeyBias(ptr[r], bias) : ptr[r];
            const uint32_t* found = find_int(k);
            if (!handle_candidates(r, found ? *found : kColumnarEmptyBucket)) {
              return;
            }
          }
        } else {
          std::array<int64_t, 64> tile_vals{};
          for (uint32_t r0 = task.row_begin; r0 < task.row_end; r0 += 64) {
            const uint32_t cnt = std::min<uint32_t>(64, task.row_end - r0);
            const uint64_t valid = DecodeIntTile64(
                pcol, r0, cnt, &probe_leaf_cache, tile_vals.data());
            const uint64_t full =
                cnt == 64 ? ~uint64_t{0} : ((uint64_t{1} << cnt) - 1);
            if (valid == full) {
              for (uint32_t i = 0; i < cnt; ++i) {
                const int64_t k = bias != 0 ? AddJoinKeyBias(tile_vals[i], bias)
                                            : tile_vals[i];
                const uint32_t* found = find_int(k);
                if (!handle_candidates(r0 + i,
                                       found ? *found : kColumnarEmptyBucket)) {
                  return;
                }
              }
            } else {
              for (uint32_t i = 0; i < cnt; ++i) {
                uint32_t head = kColumnarEmptyBucket;
                if ((valid & (uint64_t{1} << i)) != 0) {
                  const int64_t k = bias != 0
                                        ? AddJoinKeyBias(tile_vals[i], bias)
                                        : tile_vals[i];
                  if (const uint32_t* found = find_int(k)) {
                    head = *found;
                  }
                }
                if (!handle_candidates(r0 + i, head)) {
                  return;
                }
              }
            }
          }
        }
      } else if (mode == KeyMode::kPacked2x32) {
        const ColumnarColumn& pcol0 = pchunk.columns[probe_cols[0]];
        const ColumnarColumn& pcol1 = pchunk.columns[probe_cols[1]];
        const int64_t pb0 = probe_biases[0];
        const int64_t pb1 = probe_biases[1];
        const bool has_bias = (pb0 != 0 || pb1 != 0);
        auto prep_key_col = [&](const ColumnarColumn& col, uint32_t r0,
                                uint32_t cnt, int64_t* dst_k, uint8_t* dst_n,
                                const int64_t** out_k,
                                const uint8_t** out_n) -> bool {
          if (col.encoding == ColumnarColumn::Encoding::kFlat &&
              col.is_uniform && col.type != ValueType::kNull &&
              col.ints.size() >= r0 + cnt) {
            *out_k = col.ints.data() + r0;
            if (col.has_null && col.nulls.size() >= r0 + cnt) {
              *out_n = col.nulls.data() + r0;
              return true;
            }
            *out_n = nullptr;
            return false;
          }
          bool any_null = false;
          for (uint32_t off = 0; off < cnt; off += 64) {
            const uint32_t sub = std::min<uint32_t>(64, cnt - off);
            const uint64_t valid = DecodeIntTile64(
                col, r0 + off, sub, &probe_leaf_cache, dst_k + off);
            const uint64_t full =
                sub == 64 ? ~uint64_t{0} : ((uint64_t{1} << sub) - 1);
            if (valid == full) {
              std::memset(dst_n + off, 0, sub);
            } else {
              any_null = true;
              for (uint32_t b = 0; b < sub; ++b) {
                dst_n[off + b] =
                    static_cast<uint8_t>((valid & (uint64_t{1} << b)) == 0);
              }
            }
          }
          *out_k = dst_k;
          *out_n = any_null ? dst_n : nullptr;
          return any_null;
        };
        for (uint32_t r0 = task.row_begin; r0 < task.row_end; r0 += 4096) {
          const uint32_t cnt = std::min<uint32_t>(4096, task.row_end - r0);
          const int64_t* in_k0 = nullptr;
          const uint8_t* in_n0 = nullptr;
          const int64_t* in_k1 = nullptr;
          const uint8_t* in_n1 = nullptr;
          const bool has_n0 =
              prep_key_col(pcol0, r0, cnt, packed_tile_k0.data(),
                           packed_tile_n0.data(), &in_k0, &in_n0);
          const bool has_n1 =
              prep_key_col(pcol1, r0, cnt, packed_tile_k1.data(),
                           packed_tile_n1.data(), &in_k1, &in_n1);
          const auto cache_idx = static_cast<size_t>(
              (has_n0 ? 1U : 0U) | (has_n1 ? 2U : 0U) |
              (packed_has_bloom ? 4U : 0U) | (has_bias ? 8U : 0U));
          if (!packed_probe_cache[cache_idx].has_value()) {
            packed_probe_cache[cache_idx] = JitPacked2x32HashProbe::Compile(
                has_n0, has_n1, packed_has_bloom, has_bias);
          }
          if (packed_probe_cache[cache_idx].has_value()) {
            packed_probe_cache[cache_idx]->Probe(
                cnt, in_k0, in_n0, in_k1, in_n1, pb0, pb1,
                packed_has_bloom ? packed_bloom_words.data() : nullptr,
                packed_bloom_word_mask, packed_ht_keys.data(),
                packed_ht_heads.data(), packed_ht_mask,
                packed_tile_heads.data());
          } else {
            for (uint32_t i = 0; i < cnt; ++i) {
              uint32_t head = kColumnarEmptyBucket;
              if ((!has_n0 || in_n0[i] == 0) && (!has_n1 || in_n1[i] == 0)) {
                const int64_t v0 = has_bias && pb0 != 0
                                       ? AddJoinKeyBias(in_k0[i], pb0)
                                       : in_k0[i];
                const int64_t v1 = has_bias && pb1 != 0
                                       ? AddJoinKeyBias(in_k1[i], pb1)
                                       : in_k1[i];
                if (static_cast<int64_t>(static_cast<int32_t>(v0)) == v0 &&
                    static_cast<int64_t>(static_cast<int32_t>(v1)) == v1) {
                  const uint64_t packed =
                      (static_cast<uint64_t>(
                           static_cast<uint32_t>(static_cast<int32_t>(v0)))
                       << 32) |
                      static_cast<uint64_t>(
                          static_cast<uint32_t>(static_cast<int32_t>(v1)));
                  const uint64_t h = HashPacked2x32Key(packed);
                  auto slot = static_cast<size_t>(h & packed_ht_mask);
                  while (packed_ht_heads[slot] != kColumnarEmptyBucket) {
                    if (packed_ht_keys[slot] == packed) {
                      head = packed_ht_heads[slot];
                      break;
                    }
                    slot = (slot + 1) & packed_ht_mask;
                  }
                }
              }
              packed_tile_heads[i] = head;
            }
          }
          if (!has_active_residuals && !want_build_nulls) {
            for (uint32_t i = 0; i < cnt; ++i) {
              const uint32_t head = packed_tile_heads[i];
              if (head != kColumnarEmptyBucket) {
                for (uint32_t bid = head; bid != kColumnarEmptyBucket;
                     bid = next_in_bucket[bid]) {
                  ++wout.comparisons;
                  match_p_rows.push_back(r0 + i);
                  match_b_ids.push_back(bid);
                  if (match_p_rows.size() >= kColumnarChunkRows) {
                    flush_matches();
                  }
                }
              } else if (probe_outer_emit) {
                match_p_rows.push_back(r0 + i);
                match_b_ids.push_back(kColumnarEmptyBucket);
                if (match_p_rows.size() >= kColumnarChunkRows) {
                  flush_matches();
                }
              }
            }
          } else {
            for (uint32_t i = 0; i < cnt; ++i) {
              if (!handle_candidates(r0 + i, packed_tile_heads[i])) {
                return;
              }
            }
          }
        }
      } else if (mode == KeyMode::kMultiInt64) {
        for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
          bool has_null = false;
          CompositeIntKey k;
          k.count = static_cast<uint8_t>(probe_cols.size());
          for (size_t idx = 0; idx < probe_cols.size(); ++idx) {
            const ColumnarColumn& col = pchunk.columns[probe_cols[idx]];
            int64_t raw = 0;
            if (!col.TryReadNonNullInt(r, &raw)) {
              has_null = true;
              break;
            }
            k.parts[idx] = probe_biases[idx] != 0
                               ? AddJoinKeyBias(raw, probe_biases[idx])
                               : raw;
          }
          uint32_t head = kColumnarEmptyBucket;
          if (!has_null) {
            if (const uint32_t* found = find_comp(k)) {
              head = *found;
            }
          }
          if (!handle_candidates(r, head)) {
            return;
          }
        }
      } else if (mode == KeyMode::kSingleVarChar) {
        const ColumnarColumn& pcol = pchunk.columns[probe_cols[0]];
        if (pcol.encoding == ColumnarColumn::Encoding::kConstant) {
          uint32_t head = kColumnarEmptyBucket;
          if (!pcol.IsNull(0)) {
            if (const uint32_t* found = find_str(pcol.StringAt(0))) {
              head = *found;
            }
          }
          if (head != kColumnarEmptyBucket || probe_outer_emit) {
            for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
              if (!handle_candidates(r, head)) {
                return;
              }
            }
          }
        } else if (pcol.encoding == ColumnarColumn::Encoding::kDictionary &&
                   pcol.dict != nullptr && pcol.DictEntryCount() <= 65536) {
          const size_t nd = pcol.DictEntryCount();
          dict_heads.assign(nd, kColumnarEmptyBucket);
          bool any_hit = false;
          for (size_t d = 0; d < nd; ++d) {
            if (const uint32_t* found = find_str(pcol.dict->StringEntryAt(d))) {
              dict_heads[d] = *found;
              any_hit = true;
            }
          }
          if (any_hit || probe_outer_emit) {
            for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
              const uint32_t code = pcol.DictCodeAt(r);
              const uint32_t head =
                  (pcol.has_null &&
                   (code == ColumnarColumn::kNullDictCode || code >= nd))
                      ? kColumnarEmptyBucket
                      : dict_heads[code];
              if (!handle_candidates(r, head)) {
                return;
              }
            }
          }
        } else if (pcol.encoding == ColumnarColumn::Encoding::kFlat &&
                   pcol.is_uniform && !pcol.has_null) {
          const std::string_view* ptr = pcol.strings.data();
          for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
            const uint32_t* found = find_str(ptr[r]);
            if (!handle_candidates(r, found ? *found : kColumnarEmptyBucket)) {
              return;
            }
          }
        } else {
          for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
            uint32_t head = kColumnarEmptyBucket;
            std::string_view sv;
            if (pcol.TryReadNonNullString(r, &sv)) {
              if (const uint32_t* found = find_str(sv)) {
                head = *found;
              }
            }
            if (!handle_candidates(r, head)) {
              return;
            }
          }
        }
      } else {
        for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
          uint32_t head = kColumnarEmptyBucket;
          if (TryColumnarEncodeJoinKeyNonNull(pchunk, probe_cols, r, true,
                                              &probe_biases, &key_scratch,
                                              &key_buf)) {
            if (const uint32_t* found = find_str(key_buf)) {
              head = *found;
            }
          }
          if (!handle_candidates(r, head)) {
            return;
          }
        }
      }

      if (!match_p_rows.empty()) {
        emit_matched_batch(task.chunk_idx, match_p_rows, match_b_ids,
                           &wout.chunks);
      }
    }
  };

  if (workers > 1) {
    RETURN_IF_FAIL(RunParallelTasks(workers, worker_fn));
  } else {
    worker_fn(0);
  }
  RETURN_IF_FAIL(first_error);

  auto out_table = std::make_shared<ColumnarTable>();
  uint64_t total_comparisons = 0;
  for (WorkerOutput& wo : worker_outputs) {
    total_comparisons += wo.comparisons;
    for (ColumnarChunk& ch : wo.chunks) {
      if (ch.rows == 0) {
        continue;
      }
      out_table->total_rows += ch.rows;
      out_table->chunks.push_back(std::move(ch));
    }
  }

  if (want_build_nulls) {
    auto emit_unmatched_slice = [&](std::span<const uint32_t> batch_bids,
                                    std::vector<ColumnarChunk>* dst) {
      const size_t batch_n = batch_bids.size();
      if (batch_n == 0) {
        return;
      }
      std::vector<std::shared_ptr<ChunkGatherIndex>> b_gis(num_build_groups);
      for (size_t g = 0; g < num_build_groups; ++g) {
        if (build_group_used[g] == 0) {
          continue;
        }
        const size_t rc = build_group_repr[g];
        const bool flat = build_plan[rc].same_leaf_target;
        auto gi = std::make_shared<ChunkGatherIndex>();
        gi->source_table = flat ? build_plan[rc].leaf_owner : build_tbl;
        gi->src_chunks.reserve(batch_n);
        gi->src_rows.reserve(batch_n);
        for (uint32_t bid : batch_bids) {
          const uint32_t bc = build_chunk_of[bid];
          const uint32_t br = build_row_of[bid];
          if (flat && build_tbl->chunks[bc].columns[rc].encoding ==
                          ColumnarColumn::Encoding::kGathered) {
            const ChunkGatherIndex* sgi =
                build_tbl->chunks[bc].columns[rc].gather_index.get();
            const uint32_t tc = sgi->src_chunks[br];
            const uint32_t tr = sgi->src_rows[br];
            gi->src_chunks.push_back(tc);
            gi->src_rows.push_back(tr);
            if (tr == ChunkGatherIndex::kNullRow) {
              gi->has_null = true;
            }
          } else {
            gi->src_chunks.push_back(bc);
            gi->src_rows.push_back(br);
          }
        }
        b_gis[g] = std::move(gi);
      }
      ColumnarChunk uch;
      uch.rows = batch_n;
      if (flip) {
        uch.columns.resize(build_schema.ColumnCount());
        for (size_t c = 0; c < build_schema.ColumnCount(); ++c) {
          const ColumnarColSummary& bs = build_plan[c];
          const ValueType vt = bs.common_type != ValueType::kNull
                                   ? bs.common_type
                                   : build_schema.GetColumn(c).Type();
          if (bs.all_null) {
            uch.columns[c].SetConstantNull(vt, bs.common_unsigned,
                                           bs.common_interval, batch_n);
          } else {
            const size_t tcol = bs.same_leaf_target ? bs.leaf_col : c;
            const auto& gi = b_gis[build_groups[c]];
            uch.columns[c].SetGathered(gi, tcol, vt, bs.common_unsigned,
                                       bs.common_interval, bs.all_uniform,
                                       bs.any_null || gi->has_null);
          }
        }
        AppendNullColumnsToChunk(batch_n, probe_schema, &uch);
      } else {
        AppendNullColumnsToChunk(batch_n, probe_schema, &uch);
        const size_t base_col = probe_schema.ColumnCount();
        uch.columns.resize(base_col + build_schema.ColumnCount());
        for (size_t c = 0; c < build_schema.ColumnCount(); ++c) {
          const ColumnarColSummary& bs = build_plan[c];
          const ValueType vt = bs.common_type != ValueType::kNull
                                   ? bs.common_type
                                   : build_schema.GetColumn(c).Type();
          if (bs.all_null) {
            uch.columns[base_col + c].SetConstantNull(
                vt, bs.common_unsigned, bs.common_interval, batch_n);
          } else {
            const size_t tcol = bs.same_leaf_target ? bs.leaf_col : c;
            const auto& gi = b_gis[build_groups[c]];
            uch.columns[base_col + c].SetGathered(
                gi, tcol, vt, bs.common_unsigned, bs.common_interval,
                bs.all_uniform, bs.any_null || gi->has_null);
          }
        }
      }
      dst->push_back(std::move(uch));
    };

    const size_t u_workers = (!probe_disabled && max_w > 1 && n_build >= 16384)
                                 ? std::min<size_t>({kParallelProbeMaxWorkers,
                                                     max_w, n_build / 4096})
                                 : 1;
    if (u_workers > 1) {
      std::vector<std::vector<ColumnarChunk>> u_chunks(u_workers);
      RETURN_IF_FAIL(RunParallelTasks(u_workers, [&](size_t w) {
        const size_t lo = (n_build * w) / u_workers;
        const size_t hi = (n_build * (w + 1)) / u_workers;
        std::vector<uint32_t> batch;
        batch.reserve(kColumnarChunkRows);
        for (size_t i = lo; i < hi; ++i) {
          if (shared_build_matched[i] == 0) {
            batch.push_back(static_cast<uint32_t>(i));
            if (batch.size() == kColumnarChunkRows) {
              emit_unmatched_slice(batch, &u_chunks[w]);
              batch.clear();
            }
          }
        }
        if (!batch.empty()) {
          emit_unmatched_slice(batch, &u_chunks[w]);
        }
      }));
      for (auto& vec : u_chunks) {
        for (ColumnarChunk& ch : vec) {
          out_table->total_rows += ch.rows;
          out_table->chunks.push_back(std::move(ch));
        }
      }
    } else {
      std::vector<uint32_t> batch;
      batch.reserve(std::min<size_t>(kColumnarChunkRows, n_build));
      for (uint32_t i = 0; i < n_build; ++i) {
        if (shared_build_matched[i] == 0) {
          batch.push_back(i);
          if (batch.size() == kColumnarChunkRows) {
            emit_unmatched_slice(batch, &out_table->chunks);
            out_table->total_rows += kColumnarChunkRows;
            batch.clear();
          }
        }
      }
      if (!batch.empty()) {
        out_table->total_rows += batch.size();
        emit_unmatched_slice(batch, &out_table->chunks);
      }
    }
  }

  return std::optional<Relation>{FinalizeColumnarJoinResult(
      context, result_schema, std::move(using_columns), stats_seed,
      total_comparisons, std::move(out_table))};
}

// Bytes of inner rows one nested-loop chunk may hold: a bounded share of
// the query memory budget, mirroring partition_budget_bytes() above.
size_t NestedLoopInnerChunkBytes() {
  const QueryMemoryBudget& budget = QueryMemoryBudget::Global();
  if (budget.Unlimited()) {
    return size_t{1} << 30;
  }
  const size_t min_floor = std::min<size_t>(
      size_t{16} << 20, std::max<size_t>(size_t{4096}, budget.Limit() / 4));
  return std::max<size_t>(min_floor,
                          std::min(budget.Remaining() / 2, budget.Limit() / 4));
}

// Streams `rel` exactly once, invoking `fn(chunk, first_row_index)` per
// `chunk_bytes`-bounded run of rows.  A nested-loop join over a spilled
// inner uses it to re-scan the outer side per chunk instead of re-reading
// the inner's spill files once per outer row.
template <typename Fn>
Status ForEachRowChunk(Relation& rel, size_t chunk_bytes, Fn&& fn) {
  std::vector<Row> chunk;
  QueryMemoryCharge charge;
  size_t base = 0;
  Status first{Status::kSuccess};
  auto flush = [&] {
    if (chunk.empty() || first != Status::kSuccess) {
      return;
    }
    const Status st = fn(chunk, base);
    base += chunk.size();
    chunk.clear();
    charge.ReleaseAll();
    if (st != Status::kSuccess) {
      first = st;
    }
  };
  const Status stream = rel.ForEachRow([&](const Row& row) {
    if (first != Status::kSuccess) {
      return;
    }
    const size_t bytes = EstimateRowBytes(row) + 64;
    if (!chunk.empty() && charge.Bytes() + bytes > chunk_bytes) {
      flush();
    }
    charge.Add(bytes);
    chunk.push_back(row);
  });
  if (first == Status::kSuccess) {
    first = stream;
  }
  flush();
  return first;
}

}  // namespace

StatusOr<Relation> Join(TransactionContext& context, Relation left,
                        Relation right, const SelectSource& source,
                        const Scope* outer, const CteMap& ctes) {
  // An outer join elsewhere in the statement fixes the surrounding join
  // tree, not the build side of an inner edge. Reuse the inner operator's
  // smaller-build selection while retaining left-then-right column order.
  // USING metadata belongs to the ordered operator and stays on that path.
  if (source.join_type == JoinType::kInner && source.using_columns.empty() &&
      left.using_columns == nullptr && right.using_columns == nullptr &&
      right.TotalRows() >= kFlipMinBuildRows &&
      right.TotalRows() > left.TotalRows() * kFlipRatio && !left.HasSpill() &&
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_ORDERED_INNER_FLIP") ==
          nullptr) {
    const auto predicates = SplitConjuncts(source.join_condition);
    const Schema flip_schema = left.schema + right.schema;
    if (std::ranges::all_of(predicates, [&](const Expression& pred) {
          return SafeToReduceEvaluationCountWithSchema(pred, flip_schema);
        })) {
      return InnerJoin(context, std::move(left), std::move(right), predicates,
                       outer, ctes);
    }
  }
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
  RETURN_IF_FAIL(left.FinishSpill());
  RETURN_IF_FAIL(right.FinishSpill());
  RETURN_IF_FAIL(left.GetStatus());
  RETURN_IF_FAIL(right.GetStatus());
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
  const size_t raw_key_count = equality_keys.size();
  RETURN_IF_FAIL(DisableRawCollatedKeys(left, right, &equality_keys));
  const bool allow_varchar_elision = equality_keys.size() == raw_key_count;
  struct UsingJoinKey {
    std::vector<size_t> left_columns;
    std::vector<size_t> right_columns;
  };
  std::vector<UsingJoinKey> using_residuals;
  using_residuals.reserve(source.using_columns.size());
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
    // USING columns with the same ValueType but different unsigned tags do
    // NOT compare equal under the canonical `=` (-1 INT64 vs UINT64_MAX).
    // Only fold into a raw equality key when both sides share the tag;
    // otherwise keep a residual that rechecks with TryEvaluateBinary.
    const bool same_single_type =
        left_columns.size() == 1 && right_columns.size() == 1 &&
        left.schema.GetColumn(left_columns.front()).Type() ==
            right.schema.GetColumn(right_columns.front()).Type() &&
        left.schema.GetColumn(left_columns.front()).IsUnsigned() ==
            right.schema.GetColumn(right_columns.front()).IsUnsigned();
    if (same_single_type) {
      const EqualityKey key{.left=left_columns.front(), .right=right_columns.front(), .null_safe=false};
      ASSIGN_OR_RETURN(bool, collated,
                       RequiresCollatedJoinComparison(left, right, key));
      if (!collated) {
        equality_keys.push_back(key);
        continue;
      }
    }
    using_residuals.push_back(
        {std::move(left_columns), std::move(right_columns)});
  }
  const std::vector<Expression> residual =
      HasMixedKeySignednessOrType(left, right, equality_keys)
          ? ordinary_predicates
          : ResidualJoinPredicates(left.schema, right.schema,
                                   ordinary_predicates, allow_varchar_elision);
  const auto compiled_residuals =
      outer == nullptr && using_residuals.empty()
          ? CompileProbeResiduals(residual, result.schema)
          : std::nullopt;
  const auto* probe_residuals =
      compiled_residuals && !compiled_residuals->empty() ? &*compiled_residuals
                                                         : nullptr;
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
    if (using_residuals.empty()) {
      ASSIGN_OR_RETURN(
          std::optional<Relation>, cross_col,
          TryColumnarCrossOrNullJoin(context, left, right, want_left_nulls,
                                     want_right_nulls, result.schema, residual,
                                     result.using_columns, result));
      if (cross_col.has_value()) {
        if (context.execution_runtime() != nullptr) {
          context.execution_runtime()->join_ms += ElapsedMs(join_begin);
        }
        return *std::move(cross_col);
      }
    }
    if (right.HasColumnarOnly() &&
        QueryMemoryBudget::Global().CanReserve(right.TotalRows() * 128)) {
      RETURN_IF_FAIL(right.EnsureRowsMaterialized());
    }
    std::vector<char> right_matched(want_right_nulls ? right.TotalRows() : 0,
                                    0);
    Row combined_scratch{std::vector<Value>{}};
    auto probe_row = [&](const Row& left_row, const Row& right_row,
                         size_t right_idx, bool* matched) {
      ++result.join_comparisons;
      ConcatRowsInto(&combined_scratch, left_row, right_row);
      if (matches(combined_scratch)) {
        if (want_right_nulls && right_idx < right_matched.size()) {
          right_matched[right_idx] = 1;
        }
        result.AddRow(Row(combined_scratch));
        *matched = true;
      }
    };
    if (!right.HasSpill()) {
      RETURN_IF_FAIL(left.ForEachRow([&](const Row& left_row) {
        bool matched = false;
        size_t right_idx = 0;
        const Status st = right.ForEachRow([&](const Row& right_row) {
          probe_row(left_row, right_row, right_idx, &matched);
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
      // A spilled inner would otherwise be re-read from disk once per
      // outer row.  Stream it once in budget-bounded chunks and re-scan
      // the outer per chunk (block nested loop).
      std::vector<char> left_matched(want_left_nulls ? left.TotalRows() : 0, 0);
      const Status chunked = ForEachRowChunk(
          right, NestedLoopInnerChunkBytes(),
          [&](const std::vector<Row>& inner_rows, size_t inner_base) -> Status {
            size_t left_idx = 0;
            return left.ForEachRow([&](const Row& left_row) {
              bool matched = false;
              size_t right_idx = inner_base;
              for (const Row& right_row : inner_rows) {
                probe_row(left_row, right_row, right_idx, &matched);
                ++right_idx;
              }
              if (matched && left_idx < left_matched.size()) {
                left_matched[left_idx] = 1;
              }
              ++left_idx;
            });
          });
      if (chunked != Status::kSuccess && residual_error == Status::kSuccess) {
        residual_error = chunked;
      }
      if (want_left_nulls) {
        size_t left_idx = 0;
        RETURN_IF_FAIL(left.ForEachRow([&](const Row& left_row) {
          if (left_idx >= left_matched.size() || left_matched[left_idx] == 0) {
            emit_unmatched(left_row);
          }
          ++left_idx;
        }));
      }
    }
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
  } else {
    ++result.hash_joins;
    // The output vector otherwise doubles and moves every Row. A full outer
    // join emits about one row per input side; an inner join emits at least
    // the build matches. This only reserves the shell, not the cell storage.
    if (!left.HasSpill() && !right.HasSpill()) {
      const size_t hint =
          left.rows.size() + (want_right_nulls ? right.rows.size() : 0);
      if (hint > result.rows.capacity() && hint <= (size_t{1} << 20)) {
        result.rows.reserve(hint);
      }
    }
    // D8 (docs/design.md): the hybrid operator now implements null-safe
    // keys and LEFT/RIGHT/FULL outer with full partition reprocessing.
    const bool has_null_safe_key = std::ranges::any_of(
        equality_keys, [](const EqualityKey& key) { return key.null_safe; });
    static const bool flip_disabled =
        ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_JOIN_FLIP") != nullptr;
    const bool flip = !flip_disabled && !has_null_safe_key &&
                      using_residuals.empty() && source.using_columns.empty() &&
                      (source.join_type == JoinType::kFull ||
                       source.join_type == JoinType::kRight ||
                       source.join_type == JoinType::kLeft ||
                       source.join_type == JoinType::kInner) &&
                      right.TotalRows() >= kFlipMinBuildRows &&
                      right.TotalRows() > left.TotalRows() * kFlipRatio &&
                      !left.HasSpill();
    Relation* build_side_mut = flip ? &left : &right;
    const Relation& build_side = *build_side_mut;
    Relation& probe_side = flip ? right : left;
    const bool hybrid = ShouldHybridJoin(probe_side, build_side);
    // Biased keys (`col = col +/- constant`) ride only the in-memory int and
    // memcomparable key paths; the hybrid spilling operator and null-safe
    // encoding cannot bias, so those entries degrade back to residuals
    // (semantics unchanged, the residual verifier keeps them exact).
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
    left_key_offsets.reserve(equality_keys.size());
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
    ASSIGN_OR_RETURN(
        std::optional<Relation>, joined_col,
        (TryColumnarHashJoin(
            context, left, right, flip, want_left_nulls, want_right_nulls,
            has_null_safe_key, !using_residuals.empty(), left_columns,
            right_columns, left_key_offsets, right_key_offsets, residual,
            compiled_residuals, result.using_columns, result)));
    if (joined_col.has_value()) {
      if (context.execution_runtime() != nullptr) {
        context.execution_runtime()->join_ms += ElapsedMs(join_begin);
      }
      return *std::move(joined_col);
    }
    if (build_side_mut->HasColumnarOnly()) {
      RETURN_IF_FAIL(build_side_mut->EnsureRowsMaterialized());
    }
    if (probe_side.HasColumnarOnly() &&
        QueryMemoryBudget::Global().CanReserve(probe_side.TotalRows() * 128)) {
      RETURN_IF_FAIL(probe_side.EnsureRowsMaterialized());
    }
    const bool stream_is_left = !flip;
    const bool probe_outer_emit = flip ? want_right_nulls : want_left_nulls;
    const bool want_build_nulls = flip ? want_left_nulls : want_right_nulls;
    const std::vector<slot_t>& build_columns =
        flip ? left_columns : right_columns;
    const std::vector<slot_t>& probe_columns =
        flip ? right_columns : left_columns;
    const std::vector<int64_t>& build_key_offsets =
        flip ? left_key_offsets : right_key_offsets;
    const std::vector<int64_t>& probe_key_offsets =
        flip ? right_key_offsets : left_key_offsets;
    const Row* build_base =
        build_side.rows.empty() ? nullptr : build_side.rows.data();
    std::vector<char> build_matched(
        want_build_nulls ? build_side.TotalRows() : 0, 0);
    auto mark_build = [&](const Row& build_row) {
      if (!want_build_nulls || build_base == nullptr) {
        return;
      }
      const auto index = static_cast<size_t>(&build_row - build_base);
      if (index < build_matched.size()) {
        build_matched[index] = 1;
      }
    };
    auto emit_unmatched_probe = [&](const Row& probe_row) {
      if (!probe_outer_emit) {
        return;
      }
      if (stream_is_left) {
        std::vector<Value> nulls(right.schema.ColumnCount());
        result.AddRow(ConcatRows(probe_row, Row(std::move(nulls))));
      } else {
        std::vector<Value> nulls(left.schema.ColumnCount());
        result.AddRow(ConcatRows(Row(std::move(nulls)), probe_row));
      }
    };
    // Parallel chunked probe (see ParallelHashProbe): needs a pure row
    // decision (no residual re-entering TryEvaluate) and a probe side big
    // enough to amortize the threads. Unmatched build rows use private
    // per-worker bitmaps merged after probing.
    static const bool probe_disabled =
        ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_PARALLEL_PROBE") !=
        nullptr;
    bool can_parallelize =
        !probe_disabled &&
        (residual.empty() || compiled_residuals.has_value()) &&
        using_residuals.empty() && !has_null_safe_key &&
        !probe_side.HasSpill() &&
        probe_side.rows.size() >= kParallelProbeMinLeftRows;
    const size_t probe_workers =
        can_parallelize
            ? std::min<size_t>(
                  kParallelProbeMaxWorkers,
                  QueryParallelWorkerLimit(std::thread::hardware_concurrency()))
            : 1;
    std::shared_ptr<QueryMemoryCharge> matched_charge;
    if (can_parallelize && want_build_nulls) {
      matched_charge = TryReserveQueryMemory(build_side.rows.size());
      can_parallelize = matched_charge != nullptr;
    }
    auto* matched_ptr = want_build_nulls ? &build_matched : nullptr;
    std::vector<uint64_t> build_matched_bits;
    auto* matched_bits_ptr = want_build_nulls ? &build_matched_bits : nullptr;
    const size_t fixed_row_bytes =
        FixedJoinRowBytes(left, right, result.schema);
    const bool integer_key = !has_null_safe_key &&
                             SingleIntegerJoinKey(left.schema, left_columns) &&
                             SingleIntegerJoinKey(right.schema, right_columns);
    if (integer_key) {
      RETURN_IF_FAIL(ExecuteFlatBucketJoin<int64_t>(
          build_side, probe_side, probe_columns, probe_outer_emit,
          stream_is_left, build_base, matched_ptr, matched_bits_ptr,
          fixed_row_bytes, can_parallelize, probe_workers, probe_residuals,
          &result, &residual_error,
          [&](const Row& row) { return HasNullKey(row, build_columns); },
          [&](const Row& row) {
            return AddJoinKeyBias(IntegerJoinKey(row, build_columns[0]),
                                  build_key_offsets[0]);
          },
          [&](const Row& row) { return HasNullKey(row, probe_columns); },
          [&](const Row& row) {
            return AddJoinKeyBias(IntegerJoinKey(row, probe_columns[0]),
                                  probe_key_offsets[0]);
          },
          matches, mark_build, emit_unmatched_probe));
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
      const auto packed =
          BuildPackedJoinRanges(build_side, build_columns, build_key_offsets);
      if (packed) {
        RETURN_IF_FAIL(ExecuteFlatBucketJoin<int64_t>(
            build_side, probe_side, probe_columns, probe_outer_emit,
            stream_is_left, build_base, matched_ptr, matched_bits_ptr,
            fixed_row_bytes, can_parallelize, probe_workers, probe_residuals,
            &result, &residual_error,
            [&](const Row& row) { return HasNullKey(row, build_columns); },
            [&](const Row& row) {
              return packed->Encode(row, build_columns, build_key_offsets);
            },
            [&](const Row& row) { return HasNullKey(row, probe_columns); },
            [&](const Row& row) {
              return packed->Encode(row, probe_columns, probe_key_offsets);
            },
            matches, mark_build, emit_unmatched_probe));
      } else {
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
          RETURN_IF_FAIL(ExecuteFlatBucketJoin<int64_t>(
              build_side, probe_side, probe_columns, probe_outer_emit,
              stream_is_left, build_base, matched_ptr, matched_bits_ptr,
              fixed_row_bytes, can_parallelize, probe_workers, probe_residuals,
              &result, &residual_error,
              [&](const Row& row) { return HasNullKey(row, build_columns); },
              [&](const Row& row) {
                const int64_t k0 =
                    AddJoinKeyBias(IntegerJoinKey(row, build_columns[0]),
                                   build_key_offsets[0]);
                const int64_t k1 =
                    AddJoinKeyBias(IntegerJoinKey(row, build_columns[1]),
                                   build_key_offsets[1]);
                return pack_2x32(k0, k1);
              },
              [&](const Row& row) { return HasNullKey(row, probe_columns); },
              [&](const Row& row) noexcept -> int64_t {
                const int64_t k0 =
                    AddJoinKeyBias(IntegerJoinKey(row, probe_columns[0]),
                                   probe_key_offsets[0]);
                const int64_t k1 =
                    AddJoinKeyBias(IntegerJoinKey(row, probe_columns[1]),
                                   probe_key_offsets[1]);
                if (!fits_i32(k0) || !fits_i32(k1)) {
                  return kInvalidPacked2x32;
                }
                return pack_2x32(k0, k1);
              },
              matches, mark_build, emit_unmatched_probe));
        } else {
          RETURN_IF_FAIL(
              (ExecuteFlatBucketJoin<CompositeIntKey, CompositeIntKeyHash>(
                  build_side, probe_side, probe_columns, probe_outer_emit,
                  stream_is_left, build_base, matched_ptr, matched_bits_ptr,
                  fixed_row_bytes, can_parallelize, probe_workers,
                  probe_residuals, &result, &residual_error,
                  [&](const Row& row) {
                    return HasNullKey(row, build_columns);
                  },
                  [&](const Row& row) {
                    return EncodeCompositeIntKey(row, build_columns,
                                                 &build_key_offsets);
                  },
                  [&](const Row& row) {
                    return HasNullKey(row, probe_columns);
                  },
                  [&](const Row& row) {
                    return EncodeCompositeIntKey(row, probe_columns,
                                                 &probe_key_offsets);
                  },
                  matches, mark_build, emit_unmatched_probe)));
        }
      }
    } else if (!has_null_safe_key &&
               CanBorrowJoinKeys(build_side, probe_side, build_columns,
                                 probe_columns, build_key_offsets,
                                 probe_key_offsets)) {
      RETURN_IF_FAIL(
          (ExecuteFlatBucketJoin<BorrowedJoinKey, BorrowedJoinKeyHash>(
              build_side, probe_side, probe_columns, probe_outer_emit,
              stream_is_left, build_base, matched_ptr, matched_bits_ptr,
              fixed_row_bytes, can_parallelize, probe_workers, probe_residuals,
              &result, &residual_error,
              [&](const Row& row) { return HasNullKey(row, build_columns); },
              [&](const Row& row) {
                return BorrowedJoinKey{.row = &row,
                                       .columns = &build_columns,
                                       .biases = &build_key_offsets};
              },
              [&](const Row& row) { return HasNullKey(row, probe_columns); },
              [&](const Row& row) {
                return BorrowedJoinKey{.row = &row,
                                       .columns = &probe_columns,
                                       .biases = &probe_key_offsets};
              },
              matches, mark_build, emit_unmatched_probe)));
    } else {
      RETURN_IF_FAIL(ExecuteFlatBucketJoin<std::string>(
          build_side, probe_side, probe_columns, probe_outer_emit,
          stream_is_left, build_base, matched_ptr, matched_bits_ptr,
          fixed_row_bytes, can_parallelize, probe_workers, probe_residuals,
          &result, &residual_error,
          [&](const Row& row) {
            return !has_null_safe_key && HasNullKey(row, build_columns);
          },
          [&](const Row& row) {
            return has_null_safe_key
                       ? EncodeNullSafeJoinKey(row, build_columns)
                       : EncodeJoinKey(row, build_columns, &build_key_offsets);
          },
          [&](const Row& row) {
            return !has_null_safe_key && HasNullKey(row, probe_columns);
          },
          [&](const Row& row) {
            return has_null_safe_key
                       ? EncodeNullSafeJoinKey(row, probe_columns)
                       : EncodeJoinKey(row, probe_columns, &probe_key_offsets);
          },
          matches, mark_build, emit_unmatched_probe));
    }
    // Emit unmatched build-side rows (RIGHT/FULL without flip -> right;
    // LEFT/FULL with flip -> left), moving cells directly on the hot path.
    if (want_build_nulls) {
      const size_t pad_cols =
          flip ? right.schema.ColumnCount() : left.schema.ColumnCount();
      if (!build_side.HasSpill() && build_side.rows.size() >= 8192) {
        const size_t total_build = build_side.rows.size();
        const size_t workers = std::min<size_t>(
            ParallelWorkerCount(total_build / 4096), total_build);
        std::vector<Relation> shards(workers);
        std::vector<ExecutionRuntime> shard_runtimes(workers);
        for (size_t w = 0; w < workers; ++w) {
          shards[w].set_runtime(&shard_runtimes[w]);
          shards[w].schema = result.schema;
        }
        const size_t flush_granularity = Relation::ChargeFlushGranularity();
        auto emit_unmatched_row = [&](Relation& local, size_t i) {
          Row& build_row = build_side_mut->rows[i];
          if (flip) {
            Row out = std::move(build_row);
            out.values_.resize(out.values_.size() + pad_cols);
            if (fixed_row_bytes != 0) {
              local.AddRowWithKnownBytes(std::move(out), fixed_row_bytes,
                                         flush_granularity);
            } else {
              local.AddRow(std::move(out));
            }
          } else if (build_row.values_.capacity() >=
                     pad_cols + build_row.Count()) {
            const size_t bw = build_row.Count();
            build_row.values_.resize(pad_cols + bw);
            std::move_backward(
                build_row.values_.begin(),
                build_row.values_.begin() + static_cast<std::ptrdiff_t>(bw),
                build_row.values_.end());
            std::fill_n(build_row.values_.begin(), pad_cols, Value());
            if (fixed_row_bytes != 0) {
              local.AddRowWithKnownBytes(std::move(build_row), fixed_row_bytes,
                                         flush_granularity);
            } else {
              local.AddRow(std::move(build_row));
            }
          } else {
            Row out(std::vector<Value>(pad_cols + build_row.Count()));
            std::move(
                build_row.values_.begin(), build_row.values_.end(),
                out.values_.begin() + static_cast<std::ptrdiff_t>(pad_cols));
            std::vector<Value>().swap(build_row.values_);
            if (fixed_row_bytes != 0) {
              local.AddRowWithKnownBytes(std::move(out), fixed_row_bytes,
                                         flush_granularity);
            } else {
              local.AddRow(std::move(out));
            }
          }
        };
        RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
          Relation& local = shards[w];
          if (!build_matched_bits.empty()) {
            const size_t total_words = build_matched_bits.size();
            const size_t word_begin = (total_words * w) / workers;
            const size_t word_end = (total_words * (w + 1)) / workers;
            size_t unmatched_count = 0;
            for (size_t k = word_begin; k < word_end; ++k) {
              uint64_t inv = ~build_matched_bits[k];
              if (k + 1 == total_words && (total_build & 63) != 0) {
                inv &= (uint64_t{1} << (total_build & 63)) - 1;
              }
              unmatched_count += static_cast<size_t>(std::popcount(inv));
            }
            if (unmatched_count > 0 && Relation::HasAmpleMemoryBudget()) {
              local.rows.reserve(std::min<size_t>(unmatched_count, 65536));
            }
            for (size_t k = word_begin; k < word_end; ++k) {
              uint64_t inv = ~build_matched_bits[k];
              if (k + 1 == total_words && (total_build & 63) != 0) {
                inv &= (uint64_t{1} << (total_build & 63)) - 1;
              }
              const size_t base_i = k * 64;
              while (inv != 0) {
                const auto bit = static_cast<size_t>(std::countr_zero(inv));
                emit_unmatched_row(local, base_i + bit);
                inv &= (inv - 1);
              }
            }
            return;
          }
          const size_t begin = (total_build * w) / workers;
          const size_t end = (total_build * (w + 1)) / workers;
          size_t unmatched_count = 0;
          for (size_t i = begin; i < end; ++i) {
            if (i >= build_matched.size() || build_matched[i] == 0) {
              ++unmatched_count;
            }
          }
          if (unmatched_count > 0 && Relation::HasAmpleMemoryBudget()) {
            local.rows.reserve(std::min<size_t>(unmatched_count, 65536));
          }
          for (size_t i = begin; i < end; ++i) {
            if (i < build_matched.size() && build_matched[i] != 0) {
              continue;
            }
            emit_unmatched_row(local, i);
          }
        }));
        if (result.runtime() != nullptr) {
          for (const auto& rt : shard_runtimes) {
            result.runtime()->relation_spills += rt.relation_spills;
          }
        }
        RETURN_IF_FAIL(Relation::MergeShardsInto(shards, &result));
      } else {
        size_t index = 0;
        RETURN_IF_FAIL(build_side.ForEachRow([&](const Row& build_row) {
          const bool matched =
              !build_matched_bits.empty()
                  ? ((build_matched_bits[index >> 6] >> (index & 63)) & 1ULL) !=
                        0
                  : (index < build_matched.size() && build_matched[index] != 0);
          if (!matched) {
            std::vector<Value> nulls(pad_cols);
            result.AddRow(flip ? ConcatRows(build_row, Row(std::move(nulls)))
                               : ConcatRows(Row(std::move(nulls)), build_row));
          }
          ++index;
        }));
      }
    }
    RETURN_IF_FAIL(result.FinishSpill());
    RETURN_IF_FAIL(result.GetStatus());
    if (left.uniform_cell_types && right.uniform_cell_types &&
        !result.HasSpill()) {
      result.uniform_cell_types = true;
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
  const size_t raw_key_count = equality_keys.size();
  RETURN_IF_FAIL(DisableRawCollatedKeys(left, right, &equality_keys));
  const bool allow_varchar_elision = equality_keys.size() == raw_key_count;
  const std::vector<Expression> residual =
      HasMixedKeySignednessOrType(left, right, equality_keys)
          ? predicates
          : ResidualJoinPredicates(left.schema, right.schema, predicates,
                                   allow_varchar_elision);

  const auto compiled_residuals =
      outer == nullptr ? CompileProbeResiduals(residual, result.schema)
                       : std::nullopt;
  const auto* probe_residuals =
      compiled_residuals && !compiled_residuals->empty() ? &*compiled_residuals
                                                         : nullptr;
  const CompiledScanFilter residual_filter =
      CompileScanFilter(residual, result.schema);
  Status residual_error{Status::kSuccess};
  auto matches = [&](const Row& combined) {
    if (residual.empty()) {
      return true;
    }
    if (residual_error != Status::kSuccess) {
      return false;
    }
    // Join residuals share WHERE's scalar/three-valued semantics. Compile
    // once instead of walking the expression tree for every matching pair.
    return MatchScanFilter(combined, result.schema, residual_filter, outer,
                           context, ctes, &residual_error);
  };

  if (equality_keys.empty()) {
    ++result.nested_loop_joins;
    RETURN_IF_FAIL(left.FinishSpill());
    RETURN_IF_FAIL(right.FinishSpill());
    ASSIGN_OR_RETURN(
        std::optional<Relation>, cross_col,
        TryColumnarCrossOrNullJoin(context, left, right, false, false,
                                   result.schema, residual, nullptr, result));
    if (cross_col.has_value()) {
      if (context.execution_runtime() != nullptr) {
        context.execution_runtime()->join_ms += ElapsedMs(join_begin);
      }
      return *std::move(cross_col);
    }
    if (right.HasColumnarOnly() &&
        QueryMemoryBudget::Global().CanReserve(right.TotalRows() * 128)) {
      RETURN_IF_FAIL(right.EnsureRowsMaterialized());
    }
    Row combined_scratch{std::vector<Value>{}};
    if (!right.HasSpill()) {
      RETURN_IF_FAIL(left.ForEachRow([&](const Row& left_row) {
        const Status st = right.ForEachRow([&](const Row& right_row) {
          ++result.join_comparisons;
          ConcatRowsInto(&combined_scratch, left_row, right_row);
          if (matches(combined_scratch)) {
            result.AddRow(Row(combined_scratch));
          }
        });
        if (st != Status::kSuccess && residual_error == Status::kSuccess) {
          residual_error = st;
        }
      }));
    } else {
      // A spilled inner would otherwise be re-read from disk once per
      // outer row.  Stream it once in budget-bounded chunks and re-scan
      // the outer per chunk (block nested loop).
      const Status chunked = ForEachRowChunk(
          right, NestedLoopInnerChunkBytes(),
          [&](const std::vector<Row>& inner_rows, size_t) -> Status {
            return left.ForEachRow([&](const Row& left_row) {
              for (const Row& right_row : inner_rows) {
                ++result.join_comparisons;
                ConcatRowsInto(&combined_scratch, left_row, right_row);
                if (matches(combined_scratch)) {
                  result.AddRow(Row(combined_scratch));
                }
              }
            });
          });
      if (chunked != Status::kSuccess && residual_error == Status::kSuccess) {
        residual_error = chunked;
      }
    }
  } else {
    ++result.hash_joins;
    const bool has_null_safe_key = std::ranges::any_of(
        equality_keys, [](const EqualityKey& key) { return key.null_safe; });
    // Choose the build before deciding whether to partition. A spilled
    // probe does not require repartitioning when the smaller build still fits:
    // ForEachRow streams its resident prefix and spill files exactly once.
    static const bool flip_disabled =
        ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_JOIN_FLIP") != nullptr;
    const bool flip = !flip_disabled && !has_null_safe_key &&
                      right.TotalRows() >= kFlipMinBuildRows &&
                      right.TotalRows() > left.TotalRows() * kFlipRatio &&
                      !left.HasSpill();
    Relation* build_side_mut = flip ? &left : &right;
    const Relation& build_side = *build_side_mut;
    Relation& probe_side = flip ? right : left;
    const size_t index_bytes =
        build_side.TotalRows() *
        (build_side.HasColumnarOnly()
             ? ((build_side.schema.ColumnCount() * 24) + 64)
             : 64);
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
    left_key_offsets.reserve(equality_keys.size());
    std::vector<int64_t> right_key_offsets;
    right_key_offsets.reserve(equality_keys.size());
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
    QueryMemoryCharge index_charge(index_bytes);
    if (auto* runtime = context.execution_runtime()) {
      runtime->inner_hash_build_rows += build_side.TotalRows();
      runtime->inner_hash_index_estimate_bytes += index_bytes;
      runtime->inner_hash_left_builds += flip ? 1 : 0;
      runtime->inner_hash_spilled_probe_rows +=
          probe_side.HasSpill() ? probe_side.TotalRows() : 0;
    }
    ASSIGN_OR_RETURN(
        std::optional<Relation>, joined_col,
        (TryColumnarHashJoin(context, left, right, flip, false, false,
                             has_null_safe_key, false, left_columns,
                             right_columns, left_key_offsets, right_key_offsets,
                             residual, compiled_residuals, nullptr, result)));
    if (joined_col.has_value()) {
      if (context.execution_runtime() != nullptr) {
        context.execution_runtime()->join_ms += ElapsedMs(join_begin);
      }
      return *std::move(joined_col);
    }
    if (build_side_mut->HasColumnarOnly()) {
      RETURN_IF_FAIL(build_side_mut->EnsureRowsMaterialized());
    }
    if (probe_side.HasColumnarOnly() &&
        QueryMemoryBudget::Global().CanReserve(probe_side.TotalRows() * 128)) {
      RETURN_IF_FAIL(probe_side.EnsureRowsMaterialized());
    }
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
        (residual.empty() || compiled_residuals.has_value()) &&
        !has_null_safe_key && !probe_side.HasSpill() &&
        probe_side.rows.size() >= kParallelProbeMinLeftRows;
    const size_t probe_workers =
        can_parallelize
            ? std::min<size_t>(
                  kParallelProbeMaxWorkers,
                  QueryParallelWorkerLimit(std::thread::hardware_concurrency()))
            : 1;
    const bool integer_key = !has_null_safe_key &&
                             SingleIntegerJoinKey(left.schema, left_columns) &&
                             SingleIntegerJoinKey(right.schema, right_columns);
    const auto no_mark = [](const Row&) noexcept {};
    const auto no_unmatched = [](const Row&) noexcept {};
    const bool stream_is_left = !flip;
    const size_t fixed_row_bytes =
        FixedJoinRowBytes(left, right, result.schema);
    if (integer_key) {
      RETURN_IF_FAIL(ExecuteFlatBucketJoin<int64_t>(
          build_side, probe_side, probe_columns, false, stream_is_left, nullptr,
          nullptr, nullptr, fixed_row_bytes, can_parallelize, probe_workers,
          probe_residuals, &result, &residual_error,
          [&](const Row& row) { return HasNullKey(row, build_columns); },
          [&](const Row& row) {
            return AddJoinKeyBias(IntegerJoinKey(row, build_columns[0]),
                                  build_key_offsets[0]);
          },
          [&](const Row& row) { return HasNullKey(row, probe_columns); },
          [&](const Row& row) {
            return AddJoinKeyBias(IntegerJoinKey(row, probe_columns[0]),
                                  probe_key_offsets[0]);
          },
          matches, no_mark, no_unmatched));
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
        RETURN_IF_FAIL(ExecuteFlatBucketJoin<int64_t>(
            build_side, probe_side, probe_columns, false, stream_is_left,
            nullptr, nullptr, nullptr, fixed_row_bytes, can_parallelize,
            probe_workers, probe_residuals, &result, &residual_error,
            [&](const Row& row) { return HasNullKey(row, build_columns); },
            [&](const Row& row) {
              const int64_t k0 = AddJoinKeyBias(
                  IntegerJoinKey(row, build_columns[0]), build_key_offsets[0]);
              const int64_t k1 = AddJoinKeyBias(
                  IntegerJoinKey(row, build_columns[1]), build_key_offsets[1]);
              return pack_2x32(k0, k1);
            },
            [&](const Row& row) { return HasNullKey(row, probe_columns); },
            [&](const Row& row) noexcept -> int64_t {
              const int64_t k0 = AddJoinKeyBias(
                  IntegerJoinKey(row, probe_columns[0]), probe_key_offsets[0]);
              const int64_t k1 = AddJoinKeyBias(
                  IntegerJoinKey(row, probe_columns[1]), probe_key_offsets[1]);
              if (!fits_i32(k0) || !fits_i32(k1)) {
                return kInvalidPacked2x32;
              }
              return pack_2x32(k0, k1);
            },
            matches, no_mark, no_unmatched));
      } else {
        RETURN_IF_FAIL(
            (ExecuteFlatBucketJoin<CompositeIntKey, CompositeIntKeyHash>(
                build_side, probe_side, probe_columns, false, stream_is_left,
                nullptr, nullptr, nullptr, fixed_row_bytes, can_parallelize,
                probe_workers, probe_residuals, &result, &residual_error,
                [&](const Row& row) { return HasNullKey(row, build_columns); },
                [&](const Row& row) {
                  return EncodeCompositeIntKey(row, build_columns,
                                               &build_key_offsets);
                },
                [&](const Row& row) { return HasNullKey(row, probe_columns); },
                [&](const Row& row) {
                  return EncodeCompositeIntKey(row, probe_columns,
                                               &probe_key_offsets);
                },
                matches, no_mark, no_unmatched)));
      }
    } else if (!has_null_safe_key &&
               CanBorrowJoinKeys(build_side, probe_side, build_columns,
                                 probe_columns, build_key_offsets,
                                 probe_key_offsets)) {
      // Every predicate contributed exactly one equality key. Validated
      // domains and overflow-free biases make a bucket hit the final verdict.
      const bool exact_keys = predicates.size() == equality_keys.size();
      const bool parallel =
          can_parallelize ||
          (exact_keys && probe_side.rows.size() >= kParallelProbeMinLeftRows);
      const size_t borrowed_workers =
          can_parallelize
              ? probe_workers
              : std::min<size_t>(kParallelProbeMaxWorkers,
                                 QueryParallelWorkerLimit(
                                     std::thread::hardware_concurrency()));
      RETURN_IF_FAIL(
          (ExecuteFlatBucketJoin<BorrowedJoinKey, BorrowedJoinKeyHash>(
              build_side, probe_side, probe_columns, false, stream_is_left,
              nullptr, nullptr, nullptr, fixed_row_bytes, parallel,
              borrowed_workers, probe_residuals, &result, &residual_error,
              [&](const Row& row) { return HasNullKey(row, build_columns); },
              [&](const Row& row) {
                return BorrowedJoinKey{.row = &row,
                                       .columns = &build_columns,
                                       .biases = &build_key_offsets};
              },
              [&](const Row& row) { return HasNullKey(row, probe_columns); },
              [&](const Row& row) {
                return BorrowedJoinKey{.row = &row,
                                       .columns = &probe_columns,
                                       .biases = &probe_key_offsets};
              },
              [&](const Row& combined) {
                return exact_keys || matches(combined);
              },
              no_mark, no_unmatched)));
    } else {
      RETURN_IF_FAIL(ExecuteFlatBucketJoin<std::string>(
          build_side, probe_side, probe_columns, false, stream_is_left, nullptr,
          nullptr, nullptr, fixed_row_bytes, can_parallelize, probe_workers,
          probe_residuals, &result, &residual_error,
          [&](const Row& row) {
            return !has_null_safe_key && HasNullKey(row, build_columns);
          },
          [&](const Row& row) {
            return has_null_safe_key
                       ? EncodeNullSafeJoinKey(row, build_columns)
                       : EncodeJoinKey(row, build_columns, &build_key_offsets);
          },
          [&](const Row& row) {
            return !has_null_safe_key && HasNullKey(row, probe_columns);
          },
          [&](const Row& row) {
            return has_null_safe_key
                       ? EncodeNullSafeJoinKey(row, probe_columns)
                       : EncodeJoinKey(row, probe_columns, &probe_key_offsets);
          },
          matches, no_mark, no_unmatched));
    }
  }
  RETURN_IF_FAIL(result.FinishSpill());
  RETURN_IF_FAIL(result.GetStatus());
  if (left.uniform_cell_types && right.uniform_cell_types &&
      !result.HasSpill()) {
    result.uniform_cell_types = true;
  }
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
  // Resolve bare names against the same schemas used by predicate analysis.
  // A qualifier-only check misses valid null rejection in ordinary SQL such
  // as `WHERE return_reason = reason_key` after a LEFT JOIN.
  // ExpressionRejectsNullsWhere calls its matcher once per (outer join, WHERE
  // node); resolving a column with a fresh linear, case-insensitive schema
  // walk on every call made this quadratic in the number of joins. Resolve
  // each column touched by WHERE once up front and reuse the result.
  std::unordered_map<ColumnName, std::vector<size_t>> where_column_sources;
  for (const ColumnName& column : where->TouchedColumns()) {
    auto& sources = where_column_sources[column];
    for (size_t source = 0; source < schemas.size(); ++source) {
      if (LocalColumnOffset(schemas[source].schema, column)) {
        sources.push_back(source);
      }
    }
  }
  const auto rejects_side = [&](size_t begin, size_t end) {
    return ExpressionRejectsNullsWhere(
        where,
        [&](const ColumnName& column) {
          const auto found = where_column_sources.find(column);
          if (found == where_column_sources.end()) {
            return false;
          }
          return std::ranges::any_of(found->second, [&](size_t source) {
            return source >= begin && source < end;
          });
        },
        /*bare_column_rejects_null=*/true);
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
    bool rejects = false;
    if (type == JoinType::kLeft) {
      rejects = rejects_side(index, index + 1);
    } else if (type == JoinType::kRight) {
      rejects = rejects_side(0, index);
    } else {
      rejects = rejects_side(0, index) && rejects_side(index, index + 1);
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
