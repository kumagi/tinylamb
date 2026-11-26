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
#include "executor/detail/columnar_join.hpp"
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

namespace {

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

}  // namespace

StatusOr<bool> RequiresCollatedJoinComparison(const Relation& left,
                                              const Relation& right,
                                              const EqualityKey& key) {
  ASSIGN_OR_RETURN(bool, left_collated, HasCollatedJoinColumn(left, key.left));
  if (left_collated) {
    return true;
  }
  return HasCollatedJoinColumn(right, key.right);
}

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


}  // namespace tinylamb::relational_detail
