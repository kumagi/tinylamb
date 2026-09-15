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

#include "plan/optimizer.hpp"

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "plan/cascades.hpp"
#include "plan/distinct_plan.hpp"
#include "plan/empty_plan.hpp"
#include "plan/implementation_rules.hpp"
#include "plan/limit_plan.hpp"
#include "plan/plan.hpp"
#include "plan/product_plan.hpp"
#include "plan/projection_plan.hpp"
#include "plan/values_plan.hpp"
#include "query/query_data.hpp"
#include "query/statement.hpp"
#include "table/scan_options.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/constraint.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

// Expands a literal "*" into every column of every FROM relation (Phase 8):
// columns are qualified with the relation identity (alias when given, else
// the table name) so they resolve against the renamed scan schemas.  A
// qualified star expands only over the matching relation; a star matching no
// relation (unknown qualifier, or `*` with no FROM) is an error rather than a
// silently shrunken select list.
StatusOr<std::vector<NamedExpression>> ExpandSelect(
    const QueryData& query, TransactionContext& context) {
  const bool has_star =
      std::ranges::any_of(query.select_, [](const NamedExpression& selected) {
        return selected.expression->Type() == TypeTag::kColumnValue &&
               selected.expression->AsColumnValue().GetColumnName().name == "*";
      });
  if (!has_star) {
    return query.select_;
  }
  std::vector<NamedExpression> expanded;
  expanded.reserve(query.select_.size());
  for (const NamedExpression& selected : query.select_) {
    const bool is_star =
        selected.expression->Type() == TypeTag::kColumnValue &&
        selected.expression->AsColumnValue().GetColumnName().name == "*";
    if (!is_star) {
      expanded.push_back(selected);
      continue;
    }
    const ColumnName& requested =
        selected.expression->AsColumnValue().GetColumnName();
    bool matched_relation = false;
    for (const std::string& relation : query.from_) {
      const auto aliased = query.aliases_.find(relation);
      const std::string& physical =
          aliased == query.aliases_.end() ? relation : aliased->second;
      if (!requested.schema.empty() && requested.schema != relation &&
          requested.schema != physical) {
        continue;
      }
      const StatusOr<std::shared_ptr<Table>> found = context.GetTable(physical);
      if (UNLIKELY(!found.HasValue())) {
        // A table dropped between parsing and star expansion is a user-level
        // error, not a broken invariant: report it instead of aborting.
        return StatusError(found.GetStatus().GetCode(),
                           found.GetStatus().GetMessage());
      }
      matched_relation = true;
      const std::shared_ptr<Table>& table = found.Value();
      for (size_t i = 0; i < table->GetSchema().ColumnCount(); ++i) {
        expanded.emplace_back(
            ColumnName(relation, table->GetSchema().GetColumn(i).Name().name));
      }
    }
    if (!matched_relation) {
      return StatusError(
          StatusCode::kInvalidArgument,
          "unknown relation in select list: " +
              (requested.schema.empty() ? std::string("*")
                                        : requested.schema + ".*"));
    }
  }
  return expanded;
}

bool IsAggregate(const NamedExpression& expression) {
  return expression.expression->Type() == TypeTag::kAggregateExp;
}

// ---------------------------------------------------------------------------
// Unused inner-join elimination with a data proven proof.
//
// `SELECT c.customer_id FROM customers c JOIN regions r
//  ON c.region_id = r.region_id` can drop the join entirely when:
//   1. no projected / ordered / filtered column references `r`,
//   2. the only predicate touching `r` is `c.region_id = r.region_id`,
//   3. `r.region_id` is UNIQUE / PRIMARY KEY (so the join preserves the
//      left cardinality -- many-to-one), and
//   4. every `c.region_id` value is observed, in the current snapshot, to
//      be non-NULL and present in `r.region_id` (so the join drops no
//      rows).  Without a declared FOREIGN KEY constraint the containment
//      proof is checked against the transaction's own snapshot, which
//      keeps the rewrite exact for the data the query will read.
// The proof is limited to small tables so planning never scans a large
// fact table just to remove a join.
// ---------------------------------------------------------------------------

constexpr size_t kMaxJoinEliminationProofRows = 1 << 16;

struct RelationRef {
  std::unordered_set<std::string> relations;
  bool ok = true;
};

// Maps a touched column to its relation identity (alias when given, else
// the table name).  Unqualified columns resolve against every candidate
// relation whose schema owns the column; ambiguity marks them all.
std::unordered_set<std::string> ColumnRelations(
    const ColumnName& column,
    const std::unordered_map<std::string, std::shared_ptr<Table>>& tables) {
  std::unordered_set<std::string> result;
  if (!column.schema.empty()) {
    // The qualifier is the relation identity (alias when given, else the
    // physical table name).  Accept it only when the column really exists
    // there; offsets below therefore always resolve by bare column name.
    const auto found = tables.find(column.schema);
    if (found != tables.end() &&
        found->second->GetSchema().Offset(ColumnName("", column.name)) >= 0) {
      result.insert(column.schema);
      return result;
    }
    for (const auto& [relation, table] : tables) {
      if (table->GetSchema().Name() == column.schema &&
          table->GetSchema().Offset(ColumnName("", column.name)) >= 0) {
        result.insert(relation);
      }
    }
    return result;
  }
  for (const auto& [relation, table] : tables) {
    if (table->GetSchema().Offset(ColumnName("", column.name)) >= 0) {
      result.insert(relation);
    }
  }
  return result;
}

RelationRef TouchedRelations(
    const Expression& expression,
    const std::unordered_map<std::string, std::shared_ptr<Table>>& tables) {
  RelationRef ref;
  if (!expression) {
    return ref;
  }
  if (expression->Type() == TypeTag::kQueryExp ||
      expression->Type() == TypeTag::kAggregateExp) {
    ref.ok = false;
    return ref;
  }
  for (const ColumnName& column : expression->TouchedColumns()) {
    if (column.name == "*") {
      ref.ok = false;
      return ref;
    }
    const std::unordered_set<std::string> cols =
        ColumnRelations(column, tables);
    ref.relations.insert(cols.begin(), cols.end());
  }
  for (const Expression& child : ExpressionChildren(expression)) {
    RelationRef child_ref = TouchedRelations(child, tables);
    if (!child_ref.ok) {
      ref.ok = false;
      return ref;
    }
    ref.relations.insert(child_ref.relations.begin(),
                         child_ref.relations.end());
  }
  return ref;
}

using JoinKeySet = std::unordered_set<Value>;

std::optional<QueryData> TryEliminateUnusedJoins(
    const QueryData& query, const std::vector<NamedExpression>& expanded_select,
    TransactionContext& ctx) {
  if (query.from_.size() < 2 || query.require_row_position_) {
    return std::nullopt;
  }
  // Aggregate results (COUNT(*)) must observe the join's multiplicity.  The
  // grouped bridge feeds a constant-only core projection, so any projection
  // without a column reference keeps every join conservatively.
  const bool projection_uses_columns =
      std::ranges::any_of(expanded_select, [](const NamedExpression& item) {
        return item.expression && !item.expression->TouchedColumns().empty();
      });
  if (!projection_uses_columns) {
    return std::nullopt;
  }

  std::unordered_map<std::string, std::shared_ptr<Table>> tables;
  for (const std::string& relation : query.from_) {
    const auto aliased = query.aliases_.find(relation);
    const std::string& physical =
        aliased == query.aliases_.end() ? relation : aliased->second;
    StatusOr<std::shared_ptr<Table>> found = ctx.GetTable(physical);
    if (!found.HasValue()) {
      return std::nullopt;
    }
    tables.emplace(relation, found.Value());
  }

  // Relations referenced by the projection or the ordering keys.
  std::unordered_set<std::string> used;
  for (const NamedExpression& item : expanded_select) {
    const RelationRef ref = TouchedRelations(item.expression, tables);
    if (!ref.ok) {
      return std::nullopt;
    }
    used.insert(ref.relations.begin(), ref.relations.end());
  }
  for (const Expression& order : query.order_expressions_) {
    const RelationRef ref = TouchedRelations(order, tables);
    if (!ref.ok) {
      return std::nullopt;
    }
    used.insert(ref.relations.begin(), ref.relations.end());
  }

  // Split the WHERE conjuncts once: pure column equalities are join edge
  // candidates; everything else pins its relations.
  struct JoinEdge {
    Expression conjunct;
    std::string left_relation;
    ColumnName left_column;
    std::string right_relation;
    ColumnName right_column;
  };
  std::vector<JoinEdge> edges;
  std::unordered_map<std::string, size_t> edge_count;
  std::unordered_set<std::string> pinned;
  std::vector<Expression> conjuncts = SplitConjuncts(query.where_);
  for (const Expression& conjunct : conjuncts) {
    if (!conjunct) {
      continue;
    }
    bool is_edge = false;
    if (conjunct->Type() == TypeTag::kBinaryExp &&
        conjunct->AsBinaryExpression().Op() == BinaryOperation::kEquals &&
        conjunct->AsBinaryExpression().Left()->Type() ==
            TypeTag::kColumnValue &&
        conjunct->AsBinaryExpression().Right()->Type() ==
            TypeTag::kColumnValue) {
      const ColumnName& left = conjunct->AsBinaryExpression()
                                   .Left()
                                   ->AsColumnValue()
                                   .GetColumnName();
      const ColumnName& right = conjunct->AsBinaryExpression()
                                    .Right()
                                    ->AsColumnValue()
                                    .GetColumnName();
      std::unordered_set<std::string> left_rels = ColumnRelations(left, tables);
      std::unordered_set<std::string> right_rels =
          ColumnRelations(right, tables);
      if (left_rels.size() == 1 && right_rels.size() == 1 &&
          *left_rels.begin() != *right_rels.begin()) {
        JoinEdge edge;
        edge.conjunct = conjunct;
        edge.left_relation = *left_rels.begin();
        edge.left_column = left;
        edge.right_relation = *right_rels.begin();
        edge.right_column = right;
        ++edge_count[edge.left_relation];
        ++edge_count[edge.right_relation];
        edges.push_back(std::move(edge));
        is_edge = true;
      }
    }
    if (!is_edge) {
      const RelationRef ref = TouchedRelations(conjunct, tables);
      if (!ref.ok) {
        return std::nullopt;
      }
      pinned.insert(ref.relations.begin(), ref.relations.end());
    }
  }

  // A dimension candidate is referenced by exactly one equality edge and
  // nothing else; the edge partner is the kept (fact) side.
  struct EliminationPlan {
    std::string dim;
    std::string kept;
    Expression conjunct;
    ColumnName dim_column;
    ColumnName kept_column;
  };
  std::vector<EliminationPlan> plans;
  for (const JoinEdge& edge : edges) {
    const std::string& dim = used.contains(edge.right_relation)
                                 ? edge.left_relation
                                 : edge.right_relation;
    const std::string& kept =
        dim == edge.left_relation ? edge.right_relation : edge.left_relation;
    if (used.contains(dim) || pinned.contains(dim) || edge_count[dim] != 1) {
      continue;
    }
    EliminationPlan plan;
    plan.dim = dim;
    plan.kept = kept;
    plan.conjunct = edge.conjunct;
    if (dim == edge.left_relation) {
      plan.dim_column = edge.left_column;
      plan.kept_column = edge.right_column;
    } else {
      plan.dim_column = edge.right_column;
      plan.kept_column = edge.left_column;
    }
    plans.push_back(std::move(plan));
  }
  if (plans.empty()) {
    return std::nullopt;
  }

  QueryData rewritten = query;
  std::vector<Expression> kept_conjuncts = conjuncts;
  bool changed = false;
  for (const EliminationPlan& plan : plans) {
    if (rewritten.from_.size() <= 1) {
      break;
    }
    if (std::ranges::find(rewritten.from_, plan.dim) == rewritten.from_.end()) {
      continue;  // already eliminated as part of an earlier hop
    }
    const auto dim_table = tables.find(plan.dim);
    const auto kept_table = tables.find(plan.kept);
    if (dim_table == tables.end() || kept_table == tables.end()) {
      continue;
    }
    // (3) the dimension join key must be declared UNIQUE.
    const int dim_offset = dim_table->second->GetSchema().Offset(
        ColumnName("", plan.dim_column.name));
    const int kept_offset = kept_table->second->GetSchema().Offset(
        ColumnName("", plan.kept_column.name));
    if (dim_offset < 0 || kept_offset < 0) {
      continue;
    }
    const Constraint& dim_constraint =
        dim_table->second->GetSchema()
            .GetColumn(static_cast<size_t>(dim_offset))
            .GetConstraint();
    if (dim_constraint.ctype != Constraint::kPrimaryKey &&
        !dim_constraint.IsUnique()) {
      continue;
    }
    // Size gate: the proof scans both sides inside this snapshot.
    const auto rows_of = [&ctx](std::string_view physical) -> size_t {
      const auto stats = ctx.GetStats(physical);
      return stats.HasValue() ? stats.Value()->Rows() : 0;
    };
    if (rows_of(dim_table->second->GetSchema().Name()) +
            rows_of(kept_table->second->GetSchema().Name()) >
        kMaxJoinEliminationProofRows) {
      continue;
    }
    // (4) containment proof in the current transaction snapshot: every
    // kept-side key value must be non-NULL and present on the dim side.
    JoinKeySet dim_values;
    bool proof_failed = false;
    {
      auto iterator = dim_table->second->BeginFullScan(
          ctx.txn_, TableScanOptions{.projection = std::vector<slot_t>{
                                         static_cast<slot_t>(dim_offset)}});
      size_t scanned = 0;
      for (; iterator.IsValid(); ++iterator) {
        if (++scanned > kMaxJoinEliminationProofRows) {
          proof_failed = true;
          break;
        }
        const Value& cell = (*iterator)[0];
        if (!cell.IsNull()) {
          dim_values.insert(cell);
        }
      }
      if (!proof_failed) {
        auto kept_iterator = kept_table->second->BeginFullScan(
            ctx.txn_, TableScanOptions{.projection = std::vector<slot_t>{
                                           static_cast<slot_t>(kept_offset)}});
        size_t kept_scanned = 0;
        for (; kept_iterator.IsValid(); ++kept_iterator) {
          if (++kept_scanned > kMaxJoinEliminationProofRows) {
            proof_failed = true;
            break;
          }
          const Value& cell = (*kept_iterator)[0];
          if (cell.IsNull() || !dim_values.contains(cell)) {
            // An unmatched or NULL kept key means the join filters rows;
            // removal would change the result.
            proof_failed = true;
            break;
          }
        }
      }
    }
    if (proof_failed) {
      continue;
    }
    // Proof holds: drop the dimension and its equality conjunct.
    rewritten.from_.erase(std::ranges::find(rewritten.from_, plan.dim));
    rewritten.aliases_.erase(plan.dim);
    kept_conjuncts.erase(std::remove(kept_conjuncts.begin(),
                                     kept_conjuncts.end(), plan.conjunct),
                         kept_conjuncts.end());
    tables.erase(plan.dim);
    changed = true;
  }
  if (!changed) {
    return std::nullopt;
  }
  rewritten.where_ = kept_conjuncts.empty() ? ConstantValueExp(Value(true))
                                            : CombineConjuncts(kept_conjuncts);
  rewritten.select_ = expanded_select;
  return rewritten;
}

// M6: drop the null-supplying side of a LEFT join when nothing above it can
// observe the difference. A LEFT join emits exactly one row per preserved
// row; it multiplies left rows only through repeated matches, so when the
// equi-key is UNIQUE on the right side each left row matches at most once
// and the join is the identity over the preserved side. Unlike the inner
// case no containment proof is needed (an outer join never filters its
// preserved side), but the gate is correspondingly strict elsewhere:
// single LEFT edge, single equi-ON, and no SELECT/ORDER/WHERE reference to
// the dropped side (an orphaned reference has no sound placement).
// Returns the single-table rewrite, or nullopt when the proof fails.
std::optional<QueryData> TryEliminateUnusedOuterJoin(
    const QueryData& query, const std::vector<NamedExpression>& expanded_select,
    TransactionContext& ctx) {
  if (query.outer_joins_.size() != 1 || query.from_.size() != 2 ||
      query.require_row_position_) {
    return std::nullopt;
  }
  const QueryData::OuterJoinEdge& edge = query.outer_joins_.front();
  // M6+1: LEFT drops the right side, RIGHT drops the left side (mirror).
  // FULL preserves both sides, so nothing can be eliminated.
  if (edge.join_kind > 1 || edge.right_index != 1 || !edge.on_condition) {
    return std::nullopt;
  }
  const std::vector<Expression> on_conjuncts =
      SplitConjuncts(edge.on_condition);
  if (on_conjuncts.size() != 1 || !on_conjuncts[0] ||
      on_conjuncts[0]->Type() != TypeTag::kBinaryExp ||
      on_conjuncts[0]->AsBinaryExpression().Op() != BinaryOperation::kEquals ||
      on_conjuncts[0]->AsBinaryExpression().Left()->Type() !=
          TypeTag::kColumnValue ||
      on_conjuncts[0]->AsBinaryExpression().Right()->Type() !=
          TypeTag::kColumnValue) {
    return std::nullopt;
  }
  const std::string& left_rel = query.from_[0];
  const std::string& right_rel = query.from_[1];
  // Preserved side survives; the other side drops when its unique equi-key
  // proves at most one match per preserved row while nothing above names
  // it. (LEFT: drop right; RIGHT: drop left, mirrored.)
  const bool is_right_join = edge.join_kind == 1;
  const std::string& preserved_rel = is_right_join ? right_rel : left_rel;
  const std::string& dropped_rel = is_right_join ? left_rel : right_rel;

  std::unordered_map<std::string, std::shared_ptr<Table>> tables;
  for (const std::string& relation : query.from_) {
    const auto aliased = query.aliases_.find(relation);
    const std::string& physical =
        aliased == query.aliases_.end() ? relation : aliased->second;
    StatusOr<std::shared_ptr<Table>> found = ctx.GetTable(physical);
    if (!found.HasValue()) {
      return std::nullopt;
    }
    tables.emplace(relation, found.Value());
  }

  const ColumnName& first = on_conjuncts[0]
                                ->AsBinaryExpression()
                                .Left()
                                ->AsColumnValue()
                                .GetColumnName();
  const ColumnName& second = on_conjuncts[0]
                                 ->AsBinaryExpression()
                                 .Right()
                                 ->AsColumnValue()
                                 .GetColumnName();
  const std::unordered_set<std::string> first_rels =
      ColumnRelations(first, tables);
  const std::unordered_set<std::string> second_rels =
      ColumnRelations(second, tables);
  if (first_rels.size() != 1 || second_rels.size() != 1) {
    return std::nullopt;
  }
  const bool first_is_dropped = *first_rels.begin() == dropped_rel;
  const bool second_is_dropped = *second_rels.begin() == dropped_rel;
  if (first_is_dropped == second_is_dropped ||
      (*first_rels.begin() != preserved_rel && !first_is_dropped) ||
      (*second_rels.begin() != preserved_rel && !second_is_dropped)) {
    return std::nullopt;
  }
  const ColumnName& dropped_col = first_is_dropped ? first : second;
  const auto dropped_table = tables.find(dropped_rel);
  const int dropped_offset = dropped_table->second->GetSchema().Offset(
      ColumnName("", dropped_col.name));
  if (dropped_offset < 0) {
    return std::nullopt;
  }
  const Constraint& dropped_constraint =
      dropped_table->second->GetSchema()
          .GetColumn(static_cast<size_t>(dropped_offset))
          .GetConstraint();
  if (dropped_constraint.ctype != Constraint::kPrimaryKey &&
      !dropped_constraint.IsUnique()) {
    return std::nullopt;
  }

  // Nothing above the join may name the dropped side.
  const auto touches_dropped = [&](const Expression& expression) -> bool {
    if (!expression) {
      return false;
    }
    const RelationRef ref = TouchedRelations(expression, tables);
    if (!ref.ok) {
      return true;
    }
    return ref.relations.contains(dropped_rel);
  };
  for (const NamedExpression& item : expanded_select) {
    if (touches_dropped(item.expression)) {
      return std::nullopt;
    }
  }
  // M-window extraction replaced window calls in the SELECT list with
  // $winN references; the original expressions still carry the calls with
  // their partition/order keys, which may name the dropped side.
  for (const NamedExpression& item : query.select_) {
    if (touches_dropped(item.expression)) {
      return std::nullopt;
    }
  }
  for (const Expression& order : query.order_expressions_) {
    if (touches_dropped(order)) {
      return std::nullopt;
    }
  }
  for (const Expression& conjunct : SplitConjuncts(query.where_)) {
    if (touches_dropped(conjunct)) {
      return std::nullopt;
    }
  }
  // QUALIFY survives the rewrite verbatim; a predicate naming the dropped
  // side (legal after M-window routing) would turn the query unresolvable.
  if (touches_dropped(query.qualify_)) {
    return std::nullopt;
  }

  QueryData rewritten = query;
  rewritten.from_ = {preserved_rel};
  rewritten.outer_joins_.clear();
  rewritten.aliases_.erase(dropped_rel);
  rewritten.select_ = expanded_select;
  return rewritten;
}

std::vector<Expression> NormalizeOrderingForOutput(
    const std::vector<Expression>& ordering,
    const std::vector<NamedExpression>& outputs) {
  std::vector<Expression> normalized;
  normalized.reserve(ordering.size());
  for (const Expression& expression : ordering) {
    const auto found =
        std::ranges::find_if(outputs, [&](const NamedExpression& output) {
          return output.expression->ToString() == expression->ToString();
        });
    if (found != outputs.end() && !found->name.empty()) {
      normalized.push_back(ColumnValueExp(found->name));
    } else {
      normalized.push_back(expression);
    }
  }
  return normalized;
}

bool SameExpression(const Expression& left, const Expression& right) {
  return left == right || (left && right && left->Type() == right->Type() &&
                           left->ToString() == right->ToString());
}

bool SafeDeterministicExpression(  // NOLINT(misc-no-recursion)
    const Expression& expression) {
  if (!expression || expression->Type() == TypeTag::kQueryExp ||
      expression->Type() == TypeTag::kAggregateExp) {
    return false;
  }
  if (expression->Type() == TypeTag::kFunctionCallExp &&
      GetFunctionVolatility(
          expression->AsFunctionCallExpression().FuncName()) !=
          Volatility::kImmutable) {
    return false;
  }
  return std::ranges::all_of(ExpressionChildren(expression),
                             SafeDeterministicExpression);
}

Schema BuildInputSchema(
    const QueryData& query,
    const std::unordered_map<std::string, std::shared_ptr<Table>>& tables) {
  Schema result;
  for (const std::string& relation : query.from_) {
    const auto found = tables.find(relation);
    if (found == tables.end()) {
      continue;
    }
    const Schema& source = found->second->GetSchema();
    std::vector<Column> columns;
    columns.reserve(source.ColumnCount());
    for (size_t i = 0; i < source.ColumnCount(); ++i) {
      Column column = source.GetColumn(i);
      column.Name().schema = relation;
      columns.push_back(std::move(column));
    }
    result = result + Schema(relation, std::move(columns));
  }
  return result;
}

bool CanSimplifySelfComparison(const Expression& expression,
                               const Schema& input_schema) {
  if (!SafeDeterministicExpression(expression)) {
    return false;
  }
  try {
    const TypeTag type = expression->ResultType(input_schema).GetType();
    // IEEE NaN breaks x = x and x != x identities.
    return type == TypeTag::kBigInt || type == TypeTag::kVarChar ||
           type == TypeTag::kDate;
  } catch (const std::exception&) {
    return false;
  }
}

struct SimpleComparison {
  Expression key;
  BinaryOperation operation;
  Value constant;
};

std::optional<SimpleComparison> ExtractSimpleComparison(
    const Expression& expression);
std::optional<bool> EvaluateConstantPredicate(BinaryOperation operation,
                                              const Value& left,
                                              const Value& right);

Expression RewritePrefixLike(const Expression& expression) {
  if (!expression || expression->Type() != TypeTag::kBinaryExp) {
    return expression;
  }
  const auto& binary = expression->AsBinaryExpression();
  if (binary.Op() != BinaryOperation::kLike ||
      binary.Right()->Type() != TypeTag::kConstantValue) {
    return expression;
  }
  const Value pattern = binary.Right()->AsConstantValue().GetValue();
  if (pattern.IsNull() || pattern.type != ValueType::kVarChar) {
    return expression;
  }
  const std::string_view text = pattern.value.varchar_value;
  if (text.size() < 2 || text.back() != '%' ||
      text.find_first_of("%_\\", 0) != text.size() - 1) {
    return expression;
  }

  std::string lower(text.substr(0, text.size() - 1));
  std::string upper = lower;
  size_t position = upper.size();
  while (position > 0 &&
         static_cast<unsigned char>(upper[position - 1]) == 0xffU) {
    --position;
  }
  if (position == 0) {
    return expression;
  }
  upper.resize(position);
  ++upper.back();
  return BinaryExpressionExp(
      BinaryExpressionExp(binary.Left(), BinaryOperation::kGreaterThanEquals,
                          ConstantValueExp(Value(std::move(lower)))),
      BinaryOperation::kAnd,
      BinaryExpressionExp(binary.Left(), BinaryOperation::kLessThan,
                          ConstantValueExp(Value(std::move(upper)))));
}

struct NullTest {
  Expression child;
  bool is_null;
};

std::optional<NullTest> ExtractNullTest(const Expression& expression) {
  if (!expression || expression->Type() != TypeTag::kUnaryExp) {
    return std::nullopt;
  }
  const auto& unary = expression->AsUnaryExpression();
  if (unary.Op() != UnaryOperation::kIsNull &&
      unary.Op() != UnaryOperation::kIsNotNull) {
    return std::nullopt;
  }
  return NullTest{.child = unary.Child(),
                  .is_null = unary.Op() == UnaryOperation::kIsNull};
}

struct InTest {
  const InExpression* expression;
  bool negated;
};

std::optional<InTest> ExtractInTest(const Expression& expression) {
  if (!expression) {
    return std::nullopt;
  }
  if (expression->Type() == TypeTag::kInExp) {
    return InTest{.expression = &expression->AsInExpression(),
                  .negated = false};
  }
  if (expression->Type() == TypeTag::kUnaryExp &&
      expression->AsUnaryExpression().Op() == UnaryOperation::kNot &&
      expression->AsUnaryExpression().Child()->Type() == TypeTag::kInExp) {
    return InTest{
        .expression =
            &expression->AsUnaryExpression().Child()->AsInExpression(),
        .negated = true};
  }
  return std::nullopt;
}

bool SameConstantSet(const InExpression& left, const InExpression& right) {
  if (left.list_.size() != right.list_.size()) {
    return false;
  }
  return std::ranges::all_of(left.list_, [&](const Expression& candidate) {
    return std::ranges::any_of(right.list_, [&](const Expression& other) {
      return candidate->Type() == TypeTag::kConstantValue &&
             other->Type() == TypeTag::kConstantValue &&
             candidate->AsConstantValue().GetValue() ==
                 other->AsConstantValue().GetValue();
    });
  });
}

struct EqualityClass {
  std::vector<Expression> members;
  std::optional<Value> constant;
};

size_t FindEqualityClass(std::vector<EqualityClass>* classes,
                         const Expression& member) {
  for (size_t i = 0; i < classes->size(); ++i) {
    if (std::ranges::any_of((*classes)[i].members,
                            [&](const Expression& candidate) {
                              return SameExpression(candidate, member);
                            })) {
      return i;
    }
  }
  classes->push_back(
      EqualityClass{.members = {member}, .constant = std::nullopt});
  return classes->size() - 1;
}

bool ConstantsDiffer(const Value& left, const Value& right) {
  const std::optional<bool> equal =
      EvaluateConstantPredicate(BinaryOperation::kEquals, left, right);
  return equal.has_value() && !*equal;
}

bool PropagateEqualityConstants(std::vector<Expression>* conjuncts) {
  std::vector<EqualityClass> classes;
  for (const Expression& conjunct : *conjuncts) {
    if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
      continue;
    }
    const auto& binary = conjunct->AsBinaryExpression();
    if (binary.Op() != BinaryOperation::kEquals ||
        binary.Left()->Type() == TypeTag::kConstantValue ||
        binary.Right()->Type() == TypeTag::kConstantValue) {
      continue;
    }
    size_t left = FindEqualityClass(&classes, binary.Left());
    size_t right = FindEqualityClass(&classes, binary.Right());
    if (left == right) {
      continue;
    }
    classes[left].members.insert(classes[left].members.end(),
                                 classes[right].members.begin(),
                                 classes[right].members.end());
    classes.erase(classes.begin() + static_cast<std::ptrdiff_t>(right));
  }

  for (const Expression& conjunct : *conjuncts) {
    const std::optional<SimpleComparison> comparison =
        ExtractSimpleComparison(conjunct);
    if (!comparison || comparison->operation != BinaryOperation::kEquals) {
      continue;
    }
    const size_t group = FindEqualityClass(&classes, comparison->key);
    EqualityClass& equality_class = classes[group];
    if (equality_class.constant &&
        ConstantsDiffer(*equality_class.constant, comparison->constant)) {
      return false;
    }
    equality_class.constant = comparison->constant;
  }

  std::vector<Expression> rewritten;
  rewritten.reserve(conjuncts->size() + classes.size());
  for (const Expression& conjunct : *conjuncts) {
    bool replaced = false;
    if (conjunct && conjunct->Type() == TypeTag::kBinaryExp &&
        conjunct->AsBinaryExpression().Op() == BinaryOperation::kEquals) {
      const auto& binary = conjunct->AsBinaryExpression();
      const Expression& candidate =
          binary.Left()->Type() == TypeTag::kConstantValue ? binary.Right()
                                                           : binary.Left();
      const size_t group = FindEqualityClass(&classes, candidate);
      replaced = classes[group].constant.has_value();
    }
    if (!replaced) {
      rewritten.push_back(conjunct);
    }
  }
  for (const EqualityClass& equality : classes) {
    if (!equality.constant) {
      continue;
    }
    const Value& constant = *equality.constant;
    for (const Expression& member : equality.members) {
      rewritten.push_back(BinaryExpressionExp(member, BinaryOperation::kEquals,
                                              ConstantValueExp(constant)));
    }
  }
  *conjuncts = std::move(rewritten);
  return true;
}

std::optional<SimpleComparison> ExtractSimpleComparison(
    const Expression& expression) {
  if (!expression || expression->Type() != TypeTag::kBinaryExp) {
    return std::nullopt;
  }
  const auto& binary = expression->AsBinaryExpression();
  switch (binary.Op()) {
    case BinaryOperation::kEquals:
    case BinaryOperation::kNotEquals:
    case BinaryOperation::kLessThan:
    case BinaryOperation::kLessThanEquals:
    case BinaryOperation::kGreaterThan:
    case BinaryOperation::kGreaterThanEquals:
      break;
    default:
      return std::nullopt;
  }
  if (binary.Left()->Type() == TypeTag::kConstantValue ||
      binary.Right()->Type() != TypeTag::kConstantValue) {
    return std::nullopt;
  }
  const Value constant = binary.Right()->AsConstantValue().GetValue();
  if (constant.IsNull()) {
    return std::nullopt;
  }
  return SimpleComparison{
      .key = binary.Left(), .operation = binary.Op(), .constant = constant};
}

std::optional<bool> EvaluateConstantPredicate(BinaryOperation operation,
                                              const Value& left,
                                              const Value& right) {
  StatusOr<Value> result = TryEvaluateBinary(operation, left, right);
  if (!result.HasValue() || result.Value().IsNull()) {
    return std::nullopt;
  }
  return result.Value().Truthy();
}

bool ComparisonPairIsContradictory(const SimpleComparison& left,
                                   const SimpleComparison& right) {
  if (!SameExpression(left.key, right.key)) {
    return false;
  }

  const auto contradicts_equality = [](const SimpleComparison& equality,
                                       const SimpleComparison& constraint) {
    const std::optional<bool> accepted = EvaluateConstantPredicate(
        constraint.operation, equality.constant, constraint.constant);
    return accepted.has_value() && !*accepted;
  };
  if (left.operation == BinaryOperation::kEquals) {
    return contradicts_equality(left, right);
  }
  if (right.operation == BinaryOperation::kEquals) {
    return contradicts_equality(right, left);
  }

  const auto is_lower = [](BinaryOperation operation) {
    return operation == BinaryOperation::kGreaterThan ||
           operation == BinaryOperation::kGreaterThanEquals;
  };
  const auto is_upper = [](BinaryOperation operation) {
    return operation == BinaryOperation::kLessThan ||
           operation == BinaryOperation::kLessThanEquals;
  };
  const SimpleComparison* lower = &left;
  const SimpleComparison* upper = &right;
  if (is_upper(left.operation) && is_lower(right.operation)) {
    lower = &right;
    upper = &left;
  } else if (!is_lower(left.operation) || !is_upper(right.operation)) {
    return false;
  }

  const std::optional<bool> reversed = EvaluateConstantPredicate(
      BinaryOperation::kGreaterThan, lower->constant, upper->constant);
  if (reversed.value_or(false)) {
    return true;
  }
  const std::optional<bool> equal = EvaluateConstantPredicate(
      BinaryOperation::kEquals, lower->constant, upper->constant);
  return equal.value_or(false) &&
         (lower->operation == BinaryOperation::kGreaterThan ||
          upper->operation == BinaryOperation::kLessThan);
}

void RemoveSubsumedBounds(std::vector<Expression>* conjuncts) {
  std::vector<std::optional<SimpleComparison>> comparisons;
  comparisons.reserve(conjuncts->size());
  for (const Expression& conjunct : *conjuncts) {
    comparisons.push_back(ExtractSimpleComparison(conjunct));
  }
  std::vector<bool> keep(conjuncts->size(), true);
  const auto is_lower = [](BinaryOperation operation) {
    return operation == BinaryOperation::kGreaterThan ||
           operation == BinaryOperation::kGreaterThanEquals;
  };
  const auto is_upper = [](BinaryOperation operation) {
    return operation == BinaryOperation::kLessThan ||
           operation == BinaryOperation::kLessThanEquals;
  };
  for (size_t i = 0; i < comparisons.size(); ++i) {
    if (!comparisons[i]) {
      continue;
    }
    for (size_t j = i + 1; j < comparisons.size(); ++j) {
      if (!comparisons[j] ||
          !SameExpression(comparisons[i]->key, comparisons[j]->key)) {
        continue;
      }
      const SimpleComparison& left = *comparisons[i];
      const SimpleComparison& right = *comparisons[j];
      if (left.operation == BinaryOperation::kEquals ||
          right.operation == BinaryOperation::kEquals) {
        const size_t equality =
            left.operation == BinaryOperation::kEquals ? i : j;
        const size_t other = equality == i ? j : i;
        if (comparisons[other]->operation == BinaryOperation::kEquals) {
          keep[j] = false;
        } else {
          keep[other] = false;
        }
        continue;
      }
      const bool both_lower =
          is_lower(left.operation) && is_lower(right.operation);
      const bool both_upper =
          is_upper(left.operation) && is_upper(right.operation);
      if (!both_lower && !both_upper) {
        continue;
      }

      const std::optional<bool> left_greater = EvaluateConstantPredicate(
          BinaryOperation::kGreaterThan, left.constant, right.constant);
      const std::optional<bool> right_greater = EvaluateConstantPredicate(
          BinaryOperation::kGreaterThan, right.constant, left.constant);
      if (!left_greater || !right_greater) {
        continue;
      }
      if (*left_greater) {
        keep[both_lower ? j : i] = false;
      } else if (*right_greater) {
        keep[both_lower ? i : j] = false;
      } else {
        // Equal endpoints: strict bounds subsume inclusive bounds.
        const bool left_strict =
            left.operation == BinaryOperation::kGreaterThan ||
            left.operation == BinaryOperation::kLessThan;
        const bool right_strict =
            right.operation == BinaryOperation::kGreaterThan ||
            right.operation == BinaryOperation::kLessThan;
        if (left_strict != right_strict) {
          keep[left_strict ? j : i] = false;
        } else {
          keep[j] = false;
        }
      }
    }
  }

  std::vector<Expression> reduced;
  reduced.reserve(conjuncts->size());
  for (size_t i = 0; i < conjuncts->size(); ++i) {
    if (keep[i]) {
      reduced.push_back((*conjuncts)[i]);
    }
  }
  *conjuncts = std::move(reduced);
}

// WHERE rejects both FALSE and UNKNOWN. This permits stronger simplification
// than a general scalar-expression context while preserving SQL three-valued
// semantics in projections and CASE expressions.
Expression SimplifyFilterPredicate(const Expression& predicate,
                                   const Schema& input_schema) {
  std::vector<Expression> conjuncts;
  for (const Expression& conjunct : SplitConjuncts(predicate)) {
    std::vector<Expression> rewritten =
        SplitConjuncts(RewritePrefixLike(conjunct));
    conjuncts.insert(conjuncts.end(), rewritten.begin(), rewritten.end());
  }
  for (Expression& conjunct : conjuncts) {
    if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp) {
      continue;
    }
    const auto& binary = conjunct->AsBinaryExpression();
    if (!SameExpression(binary.Left(), binary.Right()) ||
        !CanSimplifySelfComparison(binary.Left(), input_schema)) {
      continue;
    }
    switch (binary.Op()) {
      case BinaryOperation::kEquals:
      case BinaryOperation::kLessThanEquals:
      case BinaryOperation::kGreaterThanEquals:
        conjunct =
            UnaryExpressionExp(binary.Left(), UnaryOperation::kIsNotNull);
        break;
      case BinaryOperation::kNotEquals:
      case BinaryOperation::kLessThan:
      case BinaryOperation::kGreaterThan:
        return ConstantValueExp(Value(false));
      default:
        break;
    }
  }

  const auto boolean_assertion =
      [](const Expression& expression) -> std::optional<bool> {
    if (!expression || expression->Type() != TypeTag::kUnaryExp) {
      return std::nullopt;
    }
    const auto& unary = expression->AsUnaryExpression();
    if (unary.Op() == UnaryOperation::kIsTrue) {
      return true;
    }
    if (unary.Op() == UnaryOperation::kIsFalse) {
      return false;
    }
    return std::nullopt;
  };
  for (size_t i = 0; i < conjuncts.size(); ++i) {
    const std::optional<bool> left_true = boolean_assertion(conjuncts[i]);
    if (!left_true.has_value()) {
      continue;
    }
    for (size_t j = i + 1; j < conjuncts.size(); ++j) {
      const std::optional<bool> right_true = boolean_assertion(conjuncts[j]);
      if (right_true.has_value() && *left_true != *right_true &&
          SameExpression(conjuncts[i]->AsUnaryExpression().Child(),
                         conjuncts[j]->AsUnaryExpression().Child())) {
        return ConstantValueExp(Value(false));
      }
    }
  }

  for (size_t i = 0; i < conjuncts.size(); ++i) {
    const std::optional<NullTest> left_null = ExtractNullTest(conjuncts[i]);
    const std::optional<InTest> left_in = ExtractInTest(conjuncts[i]);
    for (size_t j = i + 1; j < conjuncts.size(); ++j) {
      if (left_null) {
        const std::optional<NullTest> right_null =
            ExtractNullTest(conjuncts[j]);
        if (right_null && left_null->is_null != right_null->is_null &&
            SameExpression(left_null->child, right_null->child)) {
          return ConstantValueExp(Value(false));
        }
      }
      if (left_in) {
        const std::optional<InTest> right_in = ExtractInTest(conjuncts[j]);
        if (right_in && left_in->negated != right_in->negated &&
            SameExpression(left_in->expression->child_,
                           right_in->expression->child_) &&
            SameConstantSet(*left_in->expression, *right_in->expression)) {
          return ConstantValueExp(Value(false));
        }
      }
    }
  }

  if (!PropagateEqualityConstants(&conjuncts)) {
    return ConstantValueExp(Value(false));
  }

  std::vector<SimpleComparison> comparisons;
  comparisons.reserve(conjuncts.size());
  for (const Expression& conjunct : conjuncts) {
    if (std::optional<SimpleComparison> comparison =
            ExtractSimpleComparison(conjunct)) {
      comparisons.push_back(std::move(*comparison));
    }
  }
  for (size_t i = 0; i < comparisons.size(); ++i) {
    for (size_t j = i + 1; j < comparisons.size(); ++j) {
      if (ComparisonPairIsContradictory(comparisons[i], comparisons[j])) {
        return ConstantValueExp(Value(false));
      }
    }
  }
  RemoveSubsumedBounds(&conjuncts);

  // A disjunction whose every branch is provably false is itself false. Each
  // branch is simplified recursively (empty range, IS TRUE vs IS FALSE, ...),
  // so `WHERE (x > 100 AND x < 10) OR (enabled IS TRUE AND enabled IS FALSE)`
  // collapses to the constant FALSE and the scan is eliminated.
  for (const Expression& conjunct : conjuncts) {
    if (!conjunct || conjunct->Type() != TypeTag::kBinaryExp ||
        conjunct->AsBinaryExpression().Op() != BinaryOperation::kOr) {
      continue;
    }
    std::vector<Expression> branches;
    std::vector<const Expression*> stack{&conjunct};
    while (!stack.empty()) {
      const Expression& node = *stack.back();
      stack.pop_back();
      if (node->Type() == TypeTag::kBinaryExp &&
          node->AsBinaryExpression().Op() == BinaryOperation::kOr) {
        stack.push_back(&node->AsBinaryExpression().Right());
        stack.push_back(&node->AsBinaryExpression().Left());
        continue;
      }
      branches.push_back(node);
    }
    bool all_false = !branches.empty();
    for (const Expression& branch : branches) {
      const Expression folded = SimplifyFilterPredicate(branch, input_schema);
      if (!folded || folded->Type() != TypeTag::kConstantValue ||
          folded->AsConstantValue().GetValue().Truthy() ||
          folded->AsConstantValue().GetValue().IsNull()) {
        all_false = false;
        break;
      }
    }
    if (all_false) {
      return ConstantValueExp(Value(false));
    }
  }

  return CombineConjuncts(conjuncts);
}

// Relations a conjunct touches. Qualified names are relation identities
// (alias when given, else table name); unqualified names map to every FROM
// relation whose physical schema owns such a column (ambiguous names
// therefore stay above the join that combines their tables, preserving the
// executor's first-match resolution semantics). Opaque CTE aliases
// (M4 materialized cells and recursive leaves) resolve like relations for
// classification, but callers must never push their conjuncts into scan
// filters: neither implementation reads group filters, so only the residual
// above evaluates them. Returns false when the
// conjunct references something outside the FROM clause; such conjuncts stay
// in the root Selection fallback.
bool ConjunctRelations(
    const Expression& conjunct,
    const std::unordered_map<std::string, std::shared_ptr<Table>>& tables,
    const std::unordered_set<std::string>& materialized,
    std::unordered_set<std::string>* relations) {
  for (const ColumnName& column : conjunct->TouchedColumns()) {
    if (!column.schema.empty()) {
      if (tables.contains(column.schema) ||
          materialized.contains(column.schema)) {
        relations->insert(column.schema);
        continue;
      }
      return false;
    }
    size_t matches = 0;
    for (const auto& [relation, table] : tables) {
      if (table->GetSchema().Offset(column) >= 0) {
        relations->insert(relation);
        ++matches;
      }
    }
    if (matches == 0) {
      return false;
    }
  }
  return true;
}

bool ConjunctRelations(
    const Expression& conjunct,
    const std::unordered_map<std::string, std::shared_ptr<Table>>& tables,
    std::unordered_set<std::string>* relations) {
  const std::unordered_set<std::string> empty;
  return ConjunctRelations(conjunct, tables, empty, relations);
}

// True when the expression tree carries a subquery node at any depth. The
// M6 outer slice keeps every such shape on the relational interpreter, whose
// scope chain evaluates correlated and SELECT-list subqueries; the vectorized
// projection path has no subquery evaluation context.
bool ContainsQueryExpression(const Expression& expression) {
  if (!expression) {
    return false;
  }
  std::vector<Expression> stack{expression};
  while (!stack.empty()) {
    Expression current = std::move(stack.back());
    stack.pop_back();
    if (!current) {
      continue;
    }
    if (current->Type() == TypeTag::kQueryExp) {
      return true;
    }
    for (const Expression& child : ExpressionChildren(current)) {
      if (child) {
        stack.push_back(child);
      }
    }
  }
  return false;
}

// True when the expression tree carries a window-function call at any depth
// (never descending into subquery boundaries, whose windows belong to
// another scope).
bool ContainsWindowExpression(const Expression& expression) {
  if (!expression) {
    return false;
  }
  std::vector<Expression> stack{expression};
  while (!stack.empty()) {
    Expression current = std::move(stack.back());
    stack.pop_back();
    if (!current) {
      continue;
    }
    if (current->Type() == TypeTag::kWindowFunctionExp) {
      return true;
    }
    if (current->Type() == TypeTag::kQueryExp) {
      continue;
    }
    for (const Expression& child : ExpressionChildren(current)) {
      if (child) {
        stack.push_back(child);
      }
    }
  }
  return false;
}

// M6 minimal slice gate: one LEFT join over exactly two base relations with
// an explicit ON condition, no aggregation, no subqueries, no row-identity
// requirement. Anything else returns false so the caller falls back to the
// relational executor, which keeps the exact legacy semantics. RIGHT/FULL,
// join chains, and mixed inner/outer graphs are deliberate follow-ups, not
// silent generalizations: each widens the ON-vs-WHERE contract (matched
// below) in ways this slice does not prove.
bool SupportsOuterJoinSlice(const QueryData& query, bool has_aggregate,
                            const Expression& predicate,
                            const Expression& outer_on,
                            const std::vector<NamedExpression>& select) {
  if (query.outer_joins_.size() != 1 || query.from_.size() != 2) {
    return false;
  }
  const QueryData::OuterJoinEdge& edge = query.outer_joins_.front();
  // M6+1: single LEFT, RIGHT or FULL edge (2 tables, explicit ON). Chains,
  // USING, aggregates and row-positional (DML) cores stay relational.
  if (edge.join_kind > 2 || edge.right_index != 1) {
    return false;
  }
  if (!edge.on_condition || !outer_on) {
    return false;
  }
  if (has_aggregate || query.require_row_position_) {
    return false;
  }
  if (ContainsQueryExpression(predicate) || ContainsQueryExpression(outer_on)) {
    return false;
  }
  // Window calls in ON/WHERE belong to other scopes (or illegal SQL): the
  // outer lowering never evaluates them.
  if (ContainsWindowExpression(predicate) ||
      ContainsWindowExpression(outer_on)) {
    return false;
  }
  for (const NamedExpression& item : select) {
    if (ContainsQueryExpression(item.expression)) {
      return false;
    }
  }
  return std::ranges::all_of(
      query.order_expressions_,
      [](const Expression& order) { return !ContainsQueryExpression(order); });
}

// ---------------------------------------------------------------------------
// Subquery decorrelation (tpch Phase2-4 / P1-5): the canonical correlated
// shapes `x IN (SELECT c ...)`, `EXISTS (SELECT ... WHERE outer = inner)` and
// their negations become semi/anti hash joins. The tuned relational path keeps
// every other subquery shape via subquery_runtime; only conjuncts that reach
// this optimizer AND match a pattern exactly are rewritten.
// ---------------------------------------------------------------------------

constexpr size_t kMaxDecorrelationDepth = 8;
thread_local size_t tls_decorrelation_depth = 0;

enum class ColumnSide : uint8_t { kOuter, kInner };

struct ScopeMaps {
  // Column resolution mirrors QueryData::Rewrite: qualified names must name a
  // FROM relation; unqualified names map first-match with an ambiguity set.
  std::unordered_set<std::string> relations;
  std::unordered_map<std::string, std::string> by_name;
  std::unordered_set<std::string> ambiguous;
  // relation identity -> physical table name (for catalog lookups).
  std::unordered_map<std::string, std::string> physical_of;

  [[nodiscard]] std::optional<ColumnName> Resolve(
      const ColumnName& column) const {
    if (!column.schema.empty()) {
      if (!relations.contains(column.schema)) {
        return std::nullopt;
      }
      return column;
    }
    if (ambiguous.contains(column.name)) {
      return std::nullopt;
    }
    const auto found = by_name.find(column.name);
    if (found == by_name.end()) {
      return std::nullopt;
    }
    return ColumnName(found->second, column.name);
  }
};

ScopeMaps BuildScopeMaps(
    const std::vector<std::pair<std::string, const Schema*>>& relations) {
  ScopeMaps maps;
  for (const auto& [relation, schema] : relations) {
    maps.relations.insert(relation);
    maps.physical_of.emplace(relation, schema->Name());
    for (size_t i = 0; i < schema->ColumnCount(); ++i) {
      const std::string& name = schema->GetColumn(i).Name().name;
      if (!maps.by_name.contains(name)) {
        maps.by_name.emplace(name, relation);
      } else {
        maps.ambiguous.insert(name);
      }
    }
  }
  return maps;
}

// Resolves one column against the inner scope first (subquery shadowing),
// falling back to the outer scope. Unresolvable names reject the rewrite.
std::optional<std::pair<ColumnSide, ColumnName>> ResolveScoped(
    const ColumnName& column, const ScopeMaps& inner, const ScopeMaps& outer) {
  if (column.name == "*") {
    return std::nullopt;
  }
  if (!column.schema.empty()) {
    if (inner.relations.contains(column.schema)) {
      return std::make_pair(ColumnSide::kInner, column);
    }
    if (outer.relations.contains(column.schema)) {
      return std::make_pair(ColumnSide::kOuter, column);
    }
    return std::nullopt;
  }
  if (const auto resolved = inner.Resolve(column)) {
    return std::make_pair(ColumnSide::kInner, *resolved);
  }
  if (const auto resolved = outer.Resolve(column)) {
    return std::make_pair(ColumnSide::kOuter, *resolved);
  }
  return std::nullopt;
}

// Rewrites every column reference into its scope-resolved qualified form.
std::optional<Expression> QualifyExpression(  // NOLINT(misc-no-recursion)
    const Expression& expression, const ScopeMaps& inner,
    const ScopeMaps& outer) {
  if (!expression) {
    return expression;
  }
  if (expression->Type() == TypeTag::kColumnValue) {
    const auto resolved = ResolveScoped(
        expression->AsColumnValue().GetColumnName(), inner, outer);
    if (!resolved) {
      return std::nullopt;
    }
    return ColumnValueExp(resolved->second);
  }
  const std::vector<Expression> children = ExpressionChildren(expression);
  std::vector<Expression> qualified;
  qualified.reserve(children.size());
  for (const Expression& child : children) {
    auto rewritten = QualifyExpression(child, inner, outer);
    if (!rewritten) {
      return std::nullopt;
    }
    qualified.push_back(std::move(*rewritten));
  }
  return WithExpressionChildren(expression, qualified);
}

bool ContainsAggregateExp(
    const Expression& expression) {  // NOLINT(misc-no-recursion)
  if (!expression) {
    return false;
  }
  if (expression->Type() == TypeTag::kAggregateExp) {
    return true;
  }
  return std::ranges::any_of(
      ExpressionChildren(expression),
      [](const Expression& child) { return ContainsAggregateExp(child); });
}

struct DecorrelationSpec {
  JoinKind kind{};
  // The vectors form one composite semi/anti-join key.  Keeping all
  // correlated equalities is important: dropping the second equality turns
  // a correlated EXISTS into a wider, semantically incorrect match set.
  std::vector<Expression> outer_keys;
  std::vector<Expression> inner_keys;
  std::vector<std::string> from;
  std::unordered_map<std::string, std::string> aliases;
  std::vector<Expression> inner_conjuncts;
};

using TablesMap = std::unordered_map<std::string, std::shared_ptr<Table>>;

// True when the underlying table column cannot hold NULL. When this is false,
// NOT IN uses the null-aware anti implementation instead of the regular anti
// join.
bool ColumnIsNonNull(const ScopeMaps& scope, const ColumnName& column,
                     TransactionContext& ctx) {
  const auto found_relation = scope.physical_of.find(column.schema);
  if (found_relation == scope.physical_of.end()) {
    return false;
  }
  const StatusOr<std::shared_ptr<Table>> table =
      ctx.GetTable(found_relation->second);
  if (!table.HasValue()) {
    return false;
  }
  const int offset =
      table.Value()->GetSchema().Offset(ColumnName("", column.name));
  if (offset < 0) {
    return false;
  }
  const Constraint::ConstraintType ctype =
      table.Value()
          ->GetSchema()
          .GetColumn(static_cast<size_t>(offset))
          .GetConstraint()
          .ctype;
  return ctype == Constraint::kNotNull || ctype == Constraint::kPrimaryKey;
}

enum class ConjunctClass : uint8_t { kInnerOnly, kCrossEquality, kReject };

// Sorts one subquery conjunct: an equality whose operands live on opposite
// sides is the correlation key candidate; anything referencing only the inner
// scope rides along as build-side filter; everything else rejects.
ConjunctClass ClassifySubqueryConjunct(const Expression& conjunct,
                                       const ScopeMaps& inner,
                                       const ScopeMaps& outer,
                                       Expression* outer_key,
                                       Expression* inner_key) {
  ColumnSide left_side{ColumnSide::kInner};
  ColumnSide right_side{ColumnSide::kInner};
  Expression left_exp;
  Expression right_exp;
  const auto resolve_operand = [&](const Expression& operand, ColumnSide* side,
                                   Expression* qualified) {
    if (!operand || operand->Type() != TypeTag::kColumnValue) {
      return false;
    }
    if (operand->TouchedColumns().size() != 1) {
      return false;
    }
    const auto resolved =
        ResolveScoped(operand->AsColumnValue().GetColumnName(), inner, outer);
    if (!resolved) {
      return false;
    }
    *side = resolved->first;
    *qualified = ColumnValueExp(resolved->second);
    return true;
  };
  if (conjunct->Type() == TypeTag::kBinaryExp &&
      conjunct->AsBinaryExpression().Op() == BinaryOperation::kEquals) {
    const auto& binary = conjunct->AsBinaryExpression();
    if (resolve_operand(binary.Left(), &left_side, &left_exp) &&
        resolve_operand(binary.Right(), &right_side, &right_exp)) {
      if (left_side == right_side) {
        // Equality within one scope is an ordinary predicate of that scope.
        return left_side == ColumnSide::kInner ? ConjunctClass::kInnerOnly
                                               : ConjunctClass::kReject;
      }
      if (left_side == ColumnSide::kOuter) {
        *outer_key = std::move(left_exp);
        *inner_key = std::move(right_exp);
      } else {
        *outer_key = std::move(right_exp);
        *inner_key = std::move(left_exp);
      }
      return ConjunctClass::kCrossEquality;
    }
  }
  // Non-equality conjuncts survive only when purely inner-local.
  for (const ColumnName& column : conjunct->TouchedColumns()) {
    const auto resolved = ResolveScoped(column, inner, outer);
    if (!resolved || resolved->first != ColumnSide::kInner) {
      return ConjunctClass::kReject;
    }
  }
  return ConjunctClass::kInnerOnly;
}

// Attempts to turn one WHERE conjunct into a semi/anti join spec. Anything
// that does not match a canonical pattern exactly stays untouched so the
// existing evaluation paths keep handling it.
std::optional<DecorrelationSpec> TryDecorrelate(const Expression& conjunct,
                                                const ScopeMaps& outer_scope,
                                                TransactionContext& ctx) {
  Expression current = conjunct;
  bool negated = false;
  while (current->Type() == TypeTag::kUnaryExp &&
         current->AsUnaryExpression().Op() == UnaryOperation::kNot) {
    negated = !negated;
    current = current->AsUnaryExpression().Child();
  }
  if (current->Type() != TypeTag::kQueryExp) {
    return std::nullopt;
  }
  const QueryExpression& query_expression = current->AsQueryExpression();
  const bool exists_form = query_expression.Exists();
  negated = negated != query_expression.Negated();

  const SelectStatement& sub = *query_expression.Query();
  // Shapes the planner cannot preserve semantically stay on their existing
  // paths: aggregation/LIMIT/DISTINCT change the membership set; CTEs and
  // FROM-subqueries need their own runtime.
  if (!sub.WithQueries().empty() || !sub.GroupBy().empty() || sub.Having() ||
      sub.HasLimit() || sub.Offset() != 0 || sub.Distinct() ||
      sub.Sources().empty()) {
    return std::nullopt;
  }

  DecorrelationSpec spec;
  spec.kind = negated ? AntiJoinKind() : SemiJoinKind();

  // IN-form projects exactly one plain column; EXISTS ignores the list but
  // still rejects aggregates (EXISTS(SELECT COUNT(*)) is constant true).
  Expression test_column;
  if (!exists_form) {
    if (sub.SelectList().size() != 1) {
      return std::nullopt;
    }
    test_column = sub.SelectList().front().expression;
    if (!test_column || test_column->Type() != TypeTag::kColumnValue) {
      return std::nullopt;
    }
  }
  for (const NamedExpression& item : sub.SelectList()) {
    if (ContainsAggregateExp(item.expression)) {
      return std::nullopt;
    }
  }
  if (exists_form && query_expression.Test()) {
    return std::nullopt;
  }

  std::vector<std::pair<std::string, const Schema*>> schemas;
  std::vector<Expression> join_conditions;
  for (const SelectSource& source : sub.Sources()) {
    if (source.query != nullptr || source.join_type == JoinType::kLeft ||
        source.join_type == JoinType::kRight ||
        source.join_type == JoinType::kFull) {
      return std::nullopt;
    }
    const std::string relation =
        source.alias.empty() ? source.table : source.alias;
    spec.from.push_back(relation);
    if (!source.alias.empty() && source.alias != source.table) {
      spec.aliases.emplace(source.alias, source.table);
    }
    if (source.join_condition) {
      join_conditions.push_back(source.join_condition);
    }
  }
  for (const std::string& relation : spec.from) {
    const auto aliased = spec.aliases.find(relation);
    const std::string& physical =
        aliased == spec.aliases.end() ? relation : aliased->second;
    const StatusOr<std::shared_ptr<Table>> table = ctx.GetTable(physical);
    if (!table.HasValue()) {
      return std::nullopt;
    }
    schemas.emplace_back(relation, &table.Value()->GetSchema());
  }
  const ScopeMaps inner_scope = BuildScopeMaps(schemas);

  bool have_key = false;
  for (const Expression& extra : join_conditions) {
    Expression outer_key;
    Expression inner_key;
    switch (ClassifySubqueryConjunct(extra, inner_scope, outer_scope,
                                     &outer_key, &inner_key)) {
      case ConjunctClass::kInnerOnly: {
        auto qualified = QualifyExpression(extra, inner_scope, outer_scope);
        if (!qualified) {
          return std::nullopt;
        }
        spec.inner_conjuncts.push_back(std::move(*qualified));
        break;
      }
      case ConjunctClass::kCrossEquality:
        // NOT IN with extra correlated predicates has tuple-valued UNKNOWN
        // semantics which the null-aware anti join does not model.  Keep that
        // shape on the canonical evaluator; positive IN and EXISTS can use a
        // composite semi/anti key safely.
        if (!exists_form && negated) {
          return std::nullopt;
        }
        spec.outer_keys.push_back(std::move(outer_key));
        spec.inner_keys.push_back(std::move(inner_key));
        have_key = true;
        break;
      case ConjunctClass::kReject:
        return std::nullopt;
    }
  }
  for (const Expression& predicate : SplitConjuncts(sub.WhereClause())) {
    Expression outer_key;
    Expression inner_key;
    switch (ClassifySubqueryConjunct(predicate, inner_scope, outer_scope,
                                     &outer_key, &inner_key)) {
      case ConjunctClass::kInnerOnly: {
        auto qualified = QualifyExpression(predicate, inner_scope, outer_scope);
        if (!qualified) {
          return std::nullopt;
        }
        spec.inner_conjuncts.push_back(std::move(*qualified));
        break;
      }
      case ConjunctClass::kCrossEquality:
        if (!exists_form && negated) {
          return std::nullopt;
        }
        spec.outer_keys.push_back(std::move(outer_key));
        spec.inner_keys.push_back(std::move(inner_key));
        have_key = true;
        break;
      case ConjunctClass::kReject:
        return std::nullopt;
    }
  }

  if (exists_form) {
    // Uncorrelated EXISTS has no join keys; leave it to the runtime path.
    if (!have_key) {
      return std::nullopt;
    }
  } else {
    // The IN probe belongs to the outer scope, the projected column to the
    // subquery.
    if (!query_expression.Test() ||
        query_expression.Test()->Type() != TypeTag::kColumnValue ||
        query_expression.Test()->TouchedColumns().size() != 1) {
      return std::nullopt;
    }
    const auto probe =
        ResolveScoped(query_expression.Test()->AsColumnValue().GetColumnName(),
                      inner_scope, outer_scope);
    if (!probe || probe->first != ColumnSide::kOuter) {
      return std::nullopt;
    }
    spec.outer_keys.insert(spec.outer_keys.begin(),
                           ColumnValueExp(probe->second));
    const auto member = ResolveScoped(
        sub.SelectList().front().expression->AsColumnValue().GetColumnName(),
        inner_scope, outer_scope);
    if (!member || member->first != ColumnSide::kInner) {
      return std::nullopt;
    }
    spec.inner_keys.insert(spec.inner_keys.begin(),
                           ColumnValueExp(member->second));
    // NOT IN uses the cheaper regular anti join when both keys are known
    // non-null. Otherwise retain SQL's three-valued behavior in the
    // null-aware anti executor; NOT EXISTS has no such hazard.
    if (spec.kind == AntiJoinKind()) {
      // Composite correlated NOT IN is rejected above.  This branch is the
      // ordinary one-key NOT IN case.
      const ColumnName& outer_column =
          spec.outer_keys.front()->AsColumnValue().GetColumnName();
      if (!ColumnIsNonNull(outer_scope, outer_column, ctx) ||
          !ColumnIsNonNull(
              inner_scope,
              spec.inner_keys.front()->AsColumnValue().GetColumnName(), ctx)) {
        spec.kind = NullAwareAntiJoinKind();
      }
    }
  }
  return spec;
}

}  // namespace

StatusOr<Plan> Optimizer::Optimize(const QueryData& query,
                                   TransactionContext& ctx) {
  return Optimize(query, ctx, OptimizerOptions::Default());
}

StatusOr<Plan> Optimizer::OptimizeRelational(
    std::shared_ptr<const SelectStatement> statement, Schema output_schema,
    TransactionContext& ctx) {
  (void)ctx;
  cascades::Memo memo;
  const cascades::GroupId root =
      memo.EnsureDerivedGroup({}, "relational-ir-root");
  cascades::LogicalExpression logical;
  logical.operation = cascades::LogicalOperator::kRelational;
  logical.relational_statement = std::move(statement);
  logical.output_schema = std::move(output_schema);
  memo.AddExpression(root, std::move(logical));

  cascades::SearchEngine search(std::move(memo), cascades::RuleSet::Default());
  const std::optional<cascades::BestPlan> best = search.Optimize(
      root, cascades::PhysicalProperties{}, DefaultImplementationRules());
  if (!best) {
    return Status::kNotImplemented;
  }
  return best->plan;
}

StatusOr<Plan> Optimizer::Optimize(const QueryData& query,
                                   TransactionContext& ctx,
                                   const OptimizerOptions& options) {
  // Value contexts (select items, ORDER BY keys) must not gain inferred
  // predicates: `x IS NOT NULL AND ...` changes a NULL projection/sort key
  // into FALSE.  The inference rule is only sound in a filter context, so
  // it is stripped for these rewrites (WHERE keeps the full rule set).
  ExpressionRuleSet value_context_rules = options.expression_rules;
  value_context_rules.Remove("inner_join_not_null_inference");
  ExpressionRewriter value_rewriter(value_context_rules);
  bool order_rewritten = false;
  QueryData scalar_normalized = query;
  for (size_t i = 0; i < query.order_expressions_.size(); ++i) {
    if (!query.order_expressions_[i] ||
        ContainsWindowExpression(query.order_expressions_[i])) {
      continue;
    }
    ASSIGN_OR_RETURN(Expression, rewritten,
                     (value_rewriter.TryRewrite(query.order_expressions_[i])));
    if (rewritten->ToString() != query.order_expressions_[i]->ToString()) {
      order_rewritten = true;
      scalar_normalized.order_expressions_[i] = std::move(rewritten);
    }
  }
  if (order_rewritten) {
    return Optimize(scalar_normalized, ctx, options);
  }
  // ORDER BY literals do not distinguish rows. Remove them before building
  // the logical sort/top-N layers so direct optimizer callers and the SQL
  // facade share the same property and limit behavior.
  if (std::ranges::any_of(query.order_expressions_, [](const Expression& key) {
        return key && key->Type() == TypeTag::kConstantValue;
      })) {
    QueryData normalized = query;
    normalized.order_expressions_.clear();
    normalized.order_ascending_.clear();
    normalized.order_nulls_first_.clear();
    for (size_t i = 0; i < query.order_expressions_.size(); ++i) {
      const Expression& key = query.order_expressions_[i];
      if (key && key->Type() == TypeTag::kConstantValue) {
        continue;
      }
      normalized.order_expressions_.push_back(key);
      if (i < query.order_ascending_.size()) {
        normalized.order_ascending_.push_back(query.order_ascending_[i]);
      }
      if (i < query.order_nulls_first_.size()) {
        normalized.order_nulls_first_.push_back(query.order_nulls_first_[i]);
      }
    }
    return Optimize(normalized, ctx, options);
  }
  ASSIGN_OR_RETURN(std::vector<NamedExpression>, expanded_select,
                   (ExpandSelect(query, ctx)));
  for (NamedExpression& selected : expanded_select) {
    // Window calls bypass scalar rewriting: the rewriter rebuilds nodes
    // positionally and has no window case (its WithExpressionChildren would
    // abort), while extraction below needs pristine call text for
    // cross-clause dedup. Unrewritten calls evaluate identically in both
    // engines (the relational path never folds them either).
    if (selected.expression && !ContainsWindowExpression(selected.expression)) {
      ASSIGN_OR_RETURN(Expression, rewritten,
                       (value_rewriter.TryRewrite(selected.expression)));
      selected.expression = std::move(rewritten);
    }
  }

  // Window extraction (M-window): hoist window calls out of the SELECT
  // list, ORDER BY, and QUALIFY into shared `$winN` references; the kWindow
  // nodes are built after the join core below. Pure syntax (no catalog):
  // validation waits for rule_context. A nullopt means nested window calls
  // (invalid SQL); the whole query keeps the existing relational path via
  // kNotImplemented. SELECT, ORDER BY, and QUALIFY share one namespace, so
  // identical calls reuse outputs across clauses.
  std::vector<ExtractedWindow> window_specs;
  std::unordered_map<std::string, size_t> window_dedup;
  size_t window_counter = 0;
  Expression window_qualify;
  std::vector<Expression> effective_order = query.order_expressions_;
  {
    std::vector<NamedExpression> rewritten_select;
    rewritten_select.reserve(expanded_select.size());
    bool extraction_failed = false;
    const auto extract_into =
        [&](const Expression& source,
            std::optional<Expression>* rewritten_out) -> bool {
      if (!source) {
        if (rewritten_out != nullptr) {
          *rewritten_out = source;
        }
        return true;
      }
      std::optional<std::pair<Expression, std::vector<ExtractedWindow>>>
          extracted =
              ExtractWindowCalls(source, &window_counter, &window_dedup);
      if (!extracted.has_value()) {
        return false;
      }
      if (rewritten_out != nullptr) {
        *rewritten_out = std::move(extracted->first);
      }
      window_specs.insert(window_specs.end(),
                          std::make_move_iterator(extracted->second.begin()),
                          std::make_move_iterator(extracted->second.end()));
      return true;
    };
    for (NamedExpression& selected : expanded_select) {
      if (!selected.expression) {
        rewritten_select.push_back(selected);
        continue;
      }
      std::optional<Expression> rewritten;
      if (!extract_into(selected.expression, &rewritten)) {
        extraction_failed = true;
        break;
      }
      rewritten_select.emplace_back(selected.name, std::move(*rewritten));
    }
    for (Expression& order : effective_order) {
      if (!order) {
        continue;
      }
      std::optional<Expression> rewritten;
      if (!extract_into(order, &rewritten)) {
        extraction_failed = true;
        break;
      }
      order = std::move(*rewritten);
    }
    if (!extraction_failed && query.qualify_) {
      std::optional<Expression> rewritten;
      if (!extract_into(query.qualify_, &rewritten)) {
        extraction_failed = true;
      } else {
        window_qualify = std::move(*rewritten);
      }
    }
    if (extraction_failed) {
      return Status::kNotImplemented;
    }
    expanded_select = std::move(rewritten_select);
  }
  const bool has_window = !window_specs.empty();

  // Provable unused inner-join elimination: drop a dimension table whose
  // unique-keyed equality join provably neither filters nor multiplies the
  // kept rows (verified against the current snapshot for small tables).
  // Inner-only: outer joins carry padding semantics the proof does not model
  // (dedicated outer-elimination rules live in the memo instead).
  // Window extraction above moved every call into `window_specs` and left
  // `$winN` references. The elimination proofs below re-run Optimize on a
  // QueryData whose select list already carries those references, so the
  // recursive extraction would see no calls and build no kWindow node; the
  // surviving `$winN` columns would then be unresolvable. Skip elimination
  // while windows are live (a window call's partition/order keys can also
  // name a side the proof would drop).
  if (!has_window && query.from_.size() > 1 && query.outer_joins_.empty()) {
    if (std::optional<QueryData> eliminated =
            TryEliminateUnusedJoins(query, expanded_select, ctx)) {
      return Optimize(*eliminated, ctx, options);
    }
  }
  // M6: drop the null-supplying side of a LEFT join whose unique equi-key
  // proves at most one match per preserved row while nothing above names
  // it. An outer join never filters its preserved side, so no containment
  // proof is needed.
  if (!has_window && !query.outer_joins_.empty()) {
    if (std::optional<QueryData> eliminated =
            TryEliminateUnusedOuterJoin(query, expanded_select, ctx)) {
      return Optimize(*eliminated, ctx, options);
    }
  }

  const bool has_aggregate = std::ranges::any_of(expanded_select, IsAggregate);
  if (has_aggregate && !std::ranges::all_of(expanded_select, IsAggregate)) {
    return Status::kNotImplemented;
  }

  if (query.from_.empty()) {
    if (has_aggregate || expanded_select.empty() || has_window) {
      return Status::kNotImplemented;
    }
    Plan source = std::make_shared<DummyScanPlan>();
    Expression predicate =
        query.where_ ? Expression{} : ConstantValueExp(Value(true));
    if (query.where_) {
      // No schema is plumbed here: the projection schema is computed
      // downstream, and not_comparison falls back to its previous
      // (constant-literal-only) NaN guard without one.
      ASSIGN_OR_RETURN(Expression, rewritten,
                       (ExpressionRewriter(options.expression_rules)
                            .TryRewrite(query.where_)));
      predicate = std::move(rewritten);
    }
    if (predicate && predicate->Type() == TypeTag::kConstantValue) {
      const Value value = predicate->AsConstantValue().GetValue();
      if (value.IsNull() || !value.Truthy()) {
        source = std::make_shared<EmptyPlan>(std::move(source));
      }
    } else if (predicate) {
      return Status::kNotImplemented;
    }
    Plan plan =
        std::make_shared<ProjectionPlan>(std::move(source), expanded_select);
    if (query.distinct_) {
      plan = std::make_shared<DistinctPlan>(std::move(plan));
    }
    if (query.limit_count_ != 0 || query.limit_offset_ != 0) {
      plan = std::make_shared<LimitPlan>(std::move(plan), query.limit_count_,
                                         query.limit_offset_);
    }
    return plan;
  }

  const Expression source_predicate =
      query.where_ ? query.where_ : ConstantValueExp(Value(true));
  cascades::RuleContext rule_context;
  rule_context.transaction = &ctx;
  rule_context.query = &query;
  // Catalog objects are keyed by relation identity (alias when given, else
  // table name); scans rename their output schemas to this identity so
  // self-joins of one physical table stay distinguishable end-to-end.
  // Lifted CTEs (M4 cells and opaque recursive leaves) have no catalog
  // objects: cells scan shared rows and recursive leaves run the worktable
  // driver, both wired below, so they skip this population.
  for (const std::string& relation : query.from_) {
    if (query.lifted_ctes_.cells.contains(relation) ||
        query.lifted_ctes_.recursive.contains(relation)) {
      continue;
    }
    const auto aliased = query.aliases_.find(relation);
    const std::string& physical =
        aliased == query.aliases_.end() ? relation : aliased->second;
    ASSIGN_OR_RETURN(std::shared_ptr<Table>, table, ctx.GetTable(physical));
    ASSIGN_OR_RETURN(std::shared_ptr<TableStatistics>, table_statistics,
                     ctx.GetStats(physical));
    rule_context.tables.emplace(relation, std::move(table));
    rule_context.statistics.emplace(relation, std::move(table_statistics));
  }

  Schema input_schema = BuildInputSchema(query, rule_context.tables);
  // Lifted CTE schemas resolve references to non-catalog relations: cells
  // carry inferred types, opaque recursive leaves are Null-typed (typed
  // rewrites treat them conservatively). Without these entries the typed
  // rewrites below would throw on unknown columns.
  for (const std::string& relation : query.from_) {
    const auto cell = query.lifted_ctes_.cells.find(relation);
    if (cell != query.lifted_ctes_.cells.end() && cell->second != nullptr) {
      input_schema = input_schema + cell->second->schema;
      continue;
    }
    const auto rec = query.lifted_ctes_.recursive.find(relation);
    if (rec != query.lifted_ctes_.recursive.end()) {
      input_schema = input_schema + rec->second.output_schema;
    }
  }
  // Window calls bypass scalar rewriting (same rationale as the SELECT-list
  // skip above); a window-bearing predicate keeps the relational path
  // downstream, where it fails loudly like today instead of tripping the
  // rewriter's positional rewrite.
  // The rule choice mirrors BytecodeCompiler::Compile: NOT() over an ordered
  // double comparison is NaN-unsound, so the predicate keeps the
  // comparison-free rule set instead of Default().
  Expression predicate;
  if (ContainsWindowExpression(source_predicate)) {
    predicate = source_predicate;
  } else {
    const ExpressionRuleSet& predicate_rules =
        ContainsNotOfOrderedDoubleComparison(source_predicate, input_schema)
            ? NotComparisonFreeRules()
            : options.expression_rules;
    ExpressionRewriter predicate_rewriter(predicate_rules);
    predicate_rewriter.set_schema(&input_schema);
    ASSIGN_OR_RETURN(Expression, rewritten,
                     (predicate_rewriter.TryRewrite(source_predicate)));
    predicate = std::move(rewritten);
  }
  for (NamedExpression& selected : expanded_select) {
    selected.expression =
        RewriteTypedArithmetic(selected.expression, input_schema);
  }
  if (ContainsWindowExpression(predicate)) {
    // Window-bearing predicates keep the relational path downstream; leave
    // the predicate pristine (see the SELECT-list skip above).
  } else {
    predicate = SimplifyFilterPredicate(
        RewriteTypedArithmetic(predicate, input_schema), input_schema);
  }

  // M6 outer slice: rewrite the ON condition through the same scalar pipeline
  // as WHERE, then gate the whole query. A rejection returns kNotImplemented
  // so every caller falls back to the relational executor.
  const bool is_outer = !query.outer_joins_.empty();
  Expression outer_on;
  if (is_outer) {
    const QueryData::OuterJoinEdge& edge = query.outer_joins_.front();
    if (edge.on_condition) {
      if (ContainsWindowExpression(edge.on_condition)) {
        outer_on = edge.on_condition;
      } else {
        // Same NaN-unsoundness gate as the WHERE rewrite above: an ON
        // clause decides match/no-match, so NOT() over an ordered double
        // comparison must not flip to its (NaN-divergent) negation.
        const ExpressionRuleSet& on_rules =
            ContainsNotOfOrderedDoubleComparison(edge.on_condition,
                                                 input_schema)
                ? NotComparisonFreeRules()
                : options.expression_rules;
        ExpressionRewriter on_rewriter(on_rules);
        on_rewriter.set_schema(&input_schema);
        ASSIGN_OR_RETURN(Expression, rewritten,
                         (on_rewriter.TryRewrite(edge.on_condition)));
        outer_on = SimplifyFilterPredicate(
            RewriteTypedArithmetic(rewritten, input_schema), input_schema);
      }
    }
    if (!SupportsOuterJoinSlice(query, has_aggregate, predicate, outer_on,
                                expanded_select)) {
      return Status::kNotImplemented;
    }
  }

  // M-window slice gates (catalog scope available from here on). SELECT,
  // ORDER BY, and QUALIFY were already extracted into one `$winN` namespace
  // above; this validates scope. A rejection returns kNotImplemented so
  // every caller falls back to the relational executor, which keeps the
  // window behavior callers already observe.
  if (has_window) {
    if (has_aggregate || query.require_row_position_) {
      return Status::kNotImplemented;
    }
    if (ContainsQueryExpression(predicate)) {
      // Decorrelated semi/anti wraps above a windowed core are a follow-up;
      // keep those shapes on the runtime that scopes subqueries today.
      return Status::kNotImplemented;
    }
    // Every call must resolve against the input relations (bare names were
    // attributed by Rewrite; anything outside stays relational).
    for (const ExtractedWindow& spec : window_specs) {
      std::unordered_set<std::string> relations;
      if (!ConjunctRelations(spec.call, rule_context.tables, &relations)) {
        return Status::kNotImplemented;
      }
    }
  }

  // Subquery decorrelation (tpch Phase2-4 / P1-5): canonical IN / EXISTS /
  // NOT EXISTS conjuncts become semi/anti hash joins wrapped around the
  // optimized core. Skipped when a LIMIT/OFFSET folds into the plan: the
  // semi join must sit below any truncation to preserve SQL evaluation
  // order, and the memo has no operator between the two.
  std::vector<DecorrelationSpec> decorrelations;
  std::vector<NamedExpression> projection_items = expanded_select;
  // True when a hidden `$semiN` correlation key was appended to
  // projection_items: DISTINCT must then run above the final trim (over the
  // caller-visible columns only), not over the hidden-key superset.
  bool hidden_semi_keys_added = false;
  const Expression effective_predicate = [&] {
    std::vector<Expression> kept;
    // No decorrelation above an outer join in this slice: semi/anti wraps
    // would need the same scope proof as the join itself. Subquery conjuncts
    // were already rejected by SupportsOuterJoinSlice, so every conjunct is
    // kept and classified below.
    if (!is_outer && query.limit_count_ == 0 && query.limit_offset_ == 0 &&
        tls_decorrelation_depth < kMaxDecorrelationDepth &&
        predicate->Type() != TypeTag::kConstantValue) {
      ++tls_decorrelation_depth;
      struct DepthGuard {
        DepthGuard() = default;
        DepthGuard(const DepthGuard&) = delete;
        DepthGuard& operator=(const DepthGuard&) = delete;
        DepthGuard(DepthGuard&&) = delete;
        DepthGuard& operator=(DepthGuard&&) = delete;
        ~DepthGuard() { --tls_decorrelation_depth; }
      } guard;
      std::vector<std::pair<std::string, const Schema*>> outer_schemas;
      outer_schemas.reserve(rule_context.tables.size());
      for (const auto& [relation, table] : rule_context.tables) {
        outer_schemas.emplace_back(relation, &table->GetSchema());
      }
      const ScopeMaps outer_scope = BuildScopeMaps(outer_schemas);
      std::vector<std::pair<DecorrelationSpec, Expression>> candidates;
      for (const Expression& conjunct : SplitConjuncts(predicate)) {
        if (std::optional<DecorrelationSpec> spec =
                TryDecorrelate(conjunct, outer_scope, ctx)) {
          candidates.emplace_back(std::move(*spec), conjunct);
        } else {
          kept.push_back(conjunct);
        }
      }
      // The semi/anti join wraps ABOVE the root projection, so every probe
      // key must survive into the projection output; hidden `$semiN` items
      // keep unselected keys visible and the engine trims them afterwards.
      for (auto& candidate : candidates) {
        bool covered = true;
        for (const Expression& outer_key : candidate.first.outer_keys) {
          const ColumnName& key = outer_key->AsColumnValue().GetColumnName();
          const bool key_covered = std::ranges::any_of(
              expanded_select, [&](const NamedExpression& item) {
                return item.expression->Type() == TypeTag::kColumnValue &&
                       item.expression->AsColumnValue().GetColumnName() == key;
              });
          if (!key_covered && !has_aggregate) {
            // ProductPlan resolves its key against the left output schema.
            // Preserve the qualified source name here; an alias such as
            // `$semi0` would hide the key from the join contract.
            projection_items.emplace_back("", outer_key);
            hidden_semi_keys_added = true;
          }
          covered = covered && (key_covered || !has_aggregate);
        }
        if (covered) {
          decorrelations.push_back(std::move(candidate.first));
        } else {
          kept.push_back(candidate.second);  // keep the old route
        }
      }
    }
    if (!decorrelations.empty()) {
      return kept.empty() ? Expression(ConstantValueExp(Value(true)))
                          : CombineConjuncts(kept);
    }
    return Expression(predicate);
  }();

  std::unordered_set<ColumnName> touched =
      effective_predicate->TouchedColumns();
  for (const NamedExpression& selected : projection_items) {
    touched.merge(selected.expression->TouchedColumns());
  }
  // ON key columns must survive scan projection even when neither WHERE nor
  // the SELECT list names them (e.g. SELECT a.v ... ON a.k = b.k).
  if (is_outer && outer_on) {
    touched.merge(outer_on->TouchedColumns());
  }
  // Window input columns (arguments, partition/order keys, filters) must
  // likewise survive: the rewritten projection only names the `$winN`
  // outputs, so the underlying columns would otherwise be pruned away.
  if (has_window) {
    for (const ExtractedWindow& spec : window_specs) {
      touched.merge(spec.call->TouchedColumns());
    }
    for (const Expression& order : effective_order) {
      if (order) {
        touched.merge(order->TouchedColumns());
      }
    }
    if (window_qualify) {
      touched.merge(window_qualify->TouchedColumns());
    }
  }

  // Required-column computation (Phase 3): every touched column is needed on
  // each root-to-leaf path of a conjunctive query, so the per-relation
  // projection lists follow directly from the global touched set.
  const auto same_identifier = [](std::string_view left,
                                  std::string_view right) {
    if (left.size() != right.size()) {
      return false;
    }
    for (size_t i = 0; i < left.size(); ++i) {
      if (std::tolower(static_cast<unsigned char>(left[i])) !=
          std::tolower(static_cast<unsigned char>(right[i]))) {
        return false;
      }
    }
    return true;
  };
  for (const auto& [relation, table] : rule_context.tables) {
    const std::string physical_name{table->GetSchema().Name()};
    std::vector<NamedExpression> projection;
    for (size_t i = 0; i < table->GetSchema().ColumnCount(); ++i) {
      const ColumnName& table_column = table->GetSchema().GetColumn(i).Name();
      if (std::ranges::any_of(touched, [&](const ColumnName& column) {
            return same_identifier(table_column.name, column.name) &&
                   (column.schema.empty() ||
                    same_identifier(column.schema, relation) ||
                    same_identifier(column.schema, physical_name));
          })) {
        projection.emplace_back(table_column);
      }
    }
    rule_context.scan_projections.emplace(relation, std::move(projection));
  }

  // WHERE decomposition (Phase 2): conjuncts enter the memo; single-relation
  // conjuncts become scan-group filters, spanning conjuncts become join
  // conditions at their deepest covering join.
  std::vector<cascades::ConjunctInfo> conjuncts;
  bool needs_root_selection = false;
  // Opaque relations (M4): materialized cells and recursive leaves both
  // ignore group scan filters (neither implementation reads them), so
  // conjuncts touching them stay above as residuals.
  std::unordered_set<std::string> opaque_relations;
  for (const auto& [alias, cell] : query.lifted_ctes_.cells) {
    if (cell != nullptr) {
      opaque_relations.insert(alias);
    }
  }
  for (const auto& [alias, ref] : query.lifted_ctes_.recursive) {
    (void)ref;
    opaque_relations.insert(alias);
  }
  const auto touches_opaque =
      [&](const std::unordered_set<std::string>& relations) {
        return std::ranges::any_of(relations, [&](const std::string& relation) {
          return opaque_relations.contains(relation);
        });
      };
  const auto is_neutral_true = [](const Expression& conjunct) {
    return conjunct && conjunct->Type() == TypeTag::kConstantValue &&
           conjunct->AsConstantValue().GetValue().Truthy();
  };
  if (!is_outer) {
    for (const Expression& conjunct : SplitConjuncts(effective_predicate)) {
      // The neutral predicate is already represented by the scan group. It has
      // no relation identity and must not force the general memo route; doing
      // so would make a simple single-table query lose the direct access-path
      // costing (notably COUNT(*)'s covering IndexOnlyScan).
      if (is_neutral_true(conjunct)) {
        continue;
      }
      std::unordered_set<std::string> relations;
      if (!ConjunctRelations(conjunct, rule_context.tables, opaque_relations,
                             &relations) ||
          relations.empty()) {
        needs_root_selection = true;
        continue;
      }
      if (touches_opaque(relations)) {
        needs_root_selection = true;
        continue;
      }
      // Keep a final residual guard for single-relation predicates in a
      // multi-relation query.  Scan-group filters are still used for costing
      // and pushdown, but some join alternatives (notably the partitioned hash
      // path) can reorder or project away the filtered side before the group
      // predicate is attached.  Rechecking the original conjunct at the root
      // is cheap compared with returning rows that did not satisfy WHERE.
      if (relations.size() == 1 && query.from_.size() > 1) {
        needs_root_selection = true;
      }
      conjuncts.push_back({conjunct, {relations.begin(), relations.end()}});
    }
  } else {
    // M6+1 ON-vs-WHERE contract (slice: from_ = {L, R}, single edge).
    // Preserved vs null-supplying sides by kind: LEFT preserves L, RIGHT
    // preserves R, FULL preserves neither (both sides pad).
    // - WHERE over a preserved side may filter before the join (scan
    //   filter, kept as a root residual like the inner path).
    // - WHERE touching a null-supplying side, or spanning both sides,
    //   filters AFTER null padding and stays above the join only. Pushing
    //   it into that side's scan would drop padded rows it must observe.
    //   (FULL therefore keeps every WHERE above the join.)
    // - ON over the single null-supplying side may filter before the join
    //   (non-matching probe rows simply NULL-pad; the full ON rides as the
    //   join predicate residual).
    // - ON over a preserved side, or spanning, decides the match itself
    //   and lives only in the join predicate: pushing a preserved-side ON
    //   into its scan would drop rows that must survive as padded rows.
    //   (FULL keeps every ON in the join predicate only.)
    const std::string& left_rel = query.from_[0];
    const std::string& right_rel = query.from_[1];
    const uint8_t outer_kind = query.outer_joins_.front().join_kind;
    const bool is_full_outer = outer_kind == 2;
    // Preserved side whose single-relation WHERE may become a scan filter;
    // empty for FULL (no side is preserved).
    const std::string& preserved_rel = outer_kind == 1 ? right_rel : left_rel;
    // Null-supplying side whose single-relation ON may become a scan
    // filter; only meaningful when exactly one side pads (LEFT/RIGHT).
    const std::string& null_supplying_rel =
        outer_kind == 1 ? left_rel : right_rel;
    for (const Expression& conjunct : SplitConjuncts(effective_predicate)) {
      if (is_neutral_true(conjunct)) {
        continue;
      }
      std::unordered_set<std::string> relations;
      if (!ConjunctRelations(conjunct, rule_context.tables, opaque_relations,
                             &relations) ||
          relations.empty()) {
        needs_root_selection = true;
        continue;
      }
      needs_root_selection = true;
      if (touches_opaque(relations)) {
        continue;
      }
      if (!is_full_outer && relations.size() == 1 &&
          *relations.begin() == preserved_rel) {
        conjuncts.push_back({conjunct, {relations.begin(), relations.end()}});
      }
    }
    for (const Expression& conjunct : SplitConjuncts(outer_on)) {
      if (is_neutral_true(conjunct)) {
        continue;
      }
      std::unordered_set<std::string> relations;
      if (!ConjunctRelations(conjunct, rule_context.tables, opaque_relations,
                             &relations) ||
          relations.empty()) {
        // Every ON conjunct must be attributable to the join graph; an
        // unattributable ON has no sound placement in this slice.
        return Status::kNotImplemented;
      }
      if (touches_opaque(relations)) {
        continue;
      }
      if (!is_full_outer && relations.size() == 1 &&
          *relations.begin() == null_supplying_rel) {
        conjuncts.push_back({conjunct, {relations.begin(), relations.end()}});
      }
    }
  }
  // Custom relational rules may add join expressions without condition
  // payloads; the root Selection keeps the old guarantee that no residual
  // clause is silently dropped for them.
  if (options.relational_rules.Names() !=
      cascades::RuleSet::Default().Names()) {
    needs_root_selection = true;
  }

  cascades::Memo memo;
  cascades::GroupId search_root = cascades::kInvalidGroup;
  if (!is_outer) {
    search_root = memo.Build(query.from_, conjuncts);
  } else {
    // The plain two-relation group built here carries an inner join that is
    // unreachable from the outer root below; singleton scan filters (from
    // `conjuncts`) are the only part reused. The outer join itself lives in
    // a tag-distinguished derived group so inner and outer alternatives can
    // never share a group (D1: one group, one meaning).
    ASSIGN_OR_RETURN(cascades::GroupId, unused_inner,
                     memo.TryBuild(query.from_, conjuncts));
    (void)unused_inner;
    const cascades::GroupId left = memo.EnsureGroup({query.from_[0]});
    const cascades::GroupId right = memo.EnsureGroup({query.from_[1]});
    const cascades::GroupId outer =
        memo.EnsureDerivedGroup(query.from_, "outer_join");
    memo.AddExpression(outer, cascades::Memo::NewOuterJoin(
                                  left, right, outer_on,
                                  query.outer_joins_.front().join_kind));
    search_root = outer;
  }
  // Materialized CTE leaves (M4): one kValues alternative per materialized
  // singleton, sharing the cell rows across every reference site through the
  // memo group (no per-site copies). The stray kScan that EnsureGroup adds
  // yields no implementation (no catalog objects), so it never wins costing.
  // EnsureGroup CHECK-aborts on an alias outside the join graph, so probe
  // first: the lifting contract only admits top-level FROM sites today, and
  // if that ever drifts the memo must degrade (skip the leaf), not kill the
  // process.
  for (const auto& [alias, cell] : query.lifted_ctes_.cells) {
    if (cell == nullptr || !memo.ContainsRelation({alias})) {
      continue;
    }
    const cascades::GroupId singleton = memo.EnsureGroup({alias});
    memo.AddExpression(singleton,
                       cascades::LogicalExpression{
                           .operation = cascades::LogicalOperator::kValues,
                           .values = cell->rows,
                           .output_schema = cell->schema});
  }
  // Opaque recursive leaves (M4): one childless kRecursiveCte alternative
  // per lifted recursive singleton. The fixpoint executes through the proven
  // worktable driver inside the executor; the memo only costs and composes
  // the leaf (same stray-kScan story as above).
  for (const auto& [alias, ref] : query.lifted_ctes_.recursive) {
    if (ref.body == nullptr || !memo.ContainsRelation({alias})) {
      continue;
    }
    const cascades::GroupId singleton = memo.EnsureGroup({alias});
    memo.AddExpression(
        singleton, cascades::LogicalExpression{
                       .operation = cascades::LogicalOperator::kRecursiveCte,
                       .relational_statement = ref.body,
                       .output_schema = ref.output_schema,
                       .cte_name = alias,
                       .depth_spec = ref.depth_spec});
  }
  // Publish table schemas (including unique constraints) so join-elimination
  // rules can prove key uniqueness instead of trusting column names.
  {
    std::unordered_map<std::string, Schema> table_schemas;
    for (const std::string& relation : query.from_) {
      const auto aliased = query.aliases_.find(relation);
      const std::string& physical =
          aliased == query.aliases_.end() ? relation : aliased->second;
      StatusOr<std::shared_ptr<Table>> found = ctx.GetTable(physical);
      if (!found.HasValue()) {
        continue;
      }
      table_schemas.emplace(relation, found.Value()->GetSchema());
    }
    memo.SetTableSchemas(table_schemas);
  }
  if (needs_root_selection) {
    const cascades::GroupId selection =
        memo.EnsureDerivedGroup(query.from_, "selection");
    memo.AddExpression(selection,
                       cascades::LogicalExpression{
                           .operation = cascades::LogicalOperator::kSelection,
                           .children = {search_root},
                           .predicate = effective_predicate});
    search_root = selection;
  }
  // Window layers (M-window): one kWindow node per distinct partition
  // signature, chained bottom-up above the filtered core (SQL evaluates
  // WHERE before window functions). QUALIFY filters the windowed rows above
  // the chain, still below the final projection.
  if (has_window) {
    std::vector<std::string> partition_order;
    std::unordered_map<std::string, std::vector<NamedExpression>>
        grouped_targets;
    std::unordered_map<std::string, std::vector<Expression>> grouped_keys;
    for (const ExtractedWindow& spec : window_specs) {
      if (spec.call->Type() != TypeTag::kWindowFunctionExp) {
        return Status::kNotImplemented;
      }
      const auto& call = spec.call->AsWindowFunctionCallExpression();
      std::string signature;
      // Length-prefixed elements: raw '|' concatenation collides when a key
      // text itself contains the separator ("a|b" as one key vs "a","b").
      signature += std::to_string(call.partition_by.size()) + ":";
      for (const Expression& key : call.partition_by) {
        const std::string text = key ? key->ToString() : std::string("<null>");
        signature += std::to_string(text.size()) + "/" + text;
      }
      if (!grouped_targets.contains(signature)) {
        partition_order.push_back(signature);
        grouped_keys.emplace(signature, call.partition_by);
      }
      grouped_targets[signature].emplace_back(spec.output.name, spec.call);
    }
    for (const std::string& signature : partition_order) {
      // Tag by partition signature (like split_window): distinct specs live
      // in distinct groups so alternatives never mix meanings (D1).
      const cascades::GroupId window =
          memo.EnsureDerivedGroup(query.from_, "window|" + signature);
      memo.AddExpression(
          window, cascades::LogicalExpression{
                      .operation = cascades::LogicalOperator::kWindow,
                      .children = {search_root},
                      .target_list = std::move(grouped_targets[signature]),
                      .partition_by = std::move(grouped_keys[signature])});
      search_root = window;
    }
    if (window_qualify) {
      const cascades::GroupId qualify =
          memo.EnsureDerivedGroup(query.from_, "qualify");
      memo.AddExpression(qualify,
                         cascades::LogicalExpression{
                             .operation = cascades::LogicalOperator::kSelection,
                             .children = {search_root},
                             .predicate = window_qualify});
      search_root = qualify;
    }
  }
  if (has_aggregate) {
    const cascades::GroupId aggregation =
        memo.EnsureDerivedGroup(query.from_, "aggregation");
    memo.AddExpression(aggregation,
                       cascades::LogicalExpression{
                           .operation = cascades::LogicalOperator::kAggregation,
                           .children = {search_root},
                           .table = "",
                           .predicate = std::nullopt,
                           .target_list = projection_items});
    search_root = aggregation;
  } else {
    const cascades::GroupId projection =
        memo.EnsureDerivedGroup(query.from_, "projection");
    memo.AddExpression(projection,
                       cascades::LogicalExpression{
                           .operation = cascades::LogicalOperator::kProjection,
                           .children = {search_root},
                           .table = "",
                           .predicate = std::nullopt,
                           .target_list = projection_items});
    search_root = projection;
  }
  if (query.distinct_ && !hidden_semi_keys_added) {
    const cascades::GroupId distinct =
        memo.EnsureDerivedGroup(query.from_, "distinct");
    memo.AddExpression(distinct,
                       cascades::LogicalExpression{
                           .operation = cascades::LogicalOperator::kDistinct,
                           .children = {search_root}});
    search_root = distinct;
  }
  if (!query.order_expressions_.empty() &&
      query.order_expressions_.size() == query.order_ascending_.size() &&
      query.limit_count_ != 0) {
    const std::vector<Expression> sort_expressions =
        NormalizeOrderingForOutput(effective_order, projection_items);
    const cascades::GroupId topn = memo.EnsureDerivedGroup(query.from_, "topn");
    std::vector<NamedExpression> topn_keys;
    topn_keys.reserve(sort_expressions.size());
    for (const Expression& expression : sort_expressions) {
      topn_keys.emplace_back("", expression);
    }
    memo.AddExpression(topn, cascades::LogicalExpression{
                                 .operation = cascades::LogicalOperator::kTopN,
                                 .children = {search_root},
                                 .target_list = std::move(topn_keys),
                                 .sort_ascending = query.order_ascending_,
                                 .sort_nulls_first = query.order_nulls_first_,
                                 .limit_count = query.limit_count_,
                                 .limit_offset = query.limit_offset_});
    search_root = topn;
  } else if (!query.order_expressions_.empty() &&
             query.order_expressions_.size() == query.order_ascending_.size()) {
    const std::vector<Expression> sort_expressions =
        NormalizeOrderingForOutput(effective_order, projection_items);
    const cascades::GroupId sort = memo.EnsureDerivedGroup(query.from_, "sort");
    std::vector<NamedExpression> sort_keys;
    sort_keys.reserve(sort_expressions.size());
    for (const Expression& expression : sort_expressions) {
      sort_keys.emplace_back("", expression);
    }
    memo.AddExpression(sort, cascades::LogicalExpression{
                                 .operation = cascades::LogicalOperator::kSort,
                                 .children = {search_root},
                                 .target_list = std::move(sort_keys),
                                 .sort_ascending = query.order_ascending_,
                                 .sort_nulls_first = query.order_nulls_first_});
    search_root = sort;
  }
  if ((query.limit_count_ != 0 || query.limit_offset_ != 0) &&
      (query.limit_count_ == 0 || query.order_expressions_.empty() ||
       query.order_expressions_.size() != query.order_ascending_.size())) {
    const cascades::GroupId limit =
        memo.EnsureDerivedGroup(query.from_, "limit");
    memo.AddExpression(limit,
                       cascades::LogicalExpression{
                           .operation = cascades::LogicalOperator::kLimit,
                           .children = {search_root},
                           .table = "",
                           .predicate = std::nullopt,
                           .target_list = {},
                           .limit_count = query.limit_count_,
                           .limit_offset = query.limit_offset_});
    search_root = limit;
  }

  cascades::PhysicalProperties properties;
  properties.require_row_position = query.require_row_position_;
  properties.wait_for_write_intent = query.wait_for_write_intent_;
  properties.access_method = options.access_method;
  if (query.order_expressions_.size() == query.order_ascending_.size() &&
      !query.order_expressions_.empty()) {
    std::vector<ColumnName> ordering;
    bool all_columns = true;
    for (const Expression& order : query.order_expressions_) {
      if (order->Type() != TypeTag::kColumnValue) {
        all_columns = false;
        break;
      }
      ordering.push_back(order->AsColumnValue().GetColumnName());
    }
    if (all_columns) {
      properties.ordering = std::move(ordering);
    }
  }
  if (query.limit_count_ != 0 || query.limit_offset_ != 0) {
    properties.limit_hint = query.limit_offset_ + query.limit_count_;
  }

  // OLTP statements overwhelmingly consist of one indexed relation.  Their
  // search space has no joins and therefore no useful Cascades exploration:
  // cost the exact same scan alternatives directly and build the root layers
  // without memo worklists, rule matching, or best-property hash tables.
  // Keep the general search for custom rule sets and decorrelated subqueries.
  // A single materialized CTE (M4) also takes the fast path: its Values
  // alternative carries an explicit residual there, which subsumes the root
  // Selection the general search would add.
  const bool default_rules = options.relational_rules.Names() ==
                                 cascades::RuleSet::Default().Names() &&
                             options.disabled_implementation_rules.empty() &&
                             options.extra_implementation_rules.empty();
  const bool single_materialized =
      query.from_.size() == 1 &&
      query.lifted_ctes_.cells.contains(query.from_.front());
  if (query.from_.size() == 1 && default_rules && decorrelations.empty() &&
      (single_materialized || query.lifted_ctes_.cells.empty()) &&
      (single_materialized || !needs_root_selection) && !has_window) {
    return OptimizeSingleRelation(query, effective_predicate, projection_items,
                                  has_aggregate, query.distinct_, properties,
                                  rule_context);
  }

  const cascades::ImplementationRuleSet* implementation_rules =
      &DefaultImplementationRules();
  cascades::ImplementationRuleSet customized;
  if (!options.disabled_implementation_rules.empty() ||
      !options.extra_implementation_rules.empty()) {
    customized = DefaultImplementationRules();
    for (const std::string& disabled : options.disabled_implementation_rules) {
      customized.Remove(disabled);
    }
    for (const cascades::ImplementationRule& extra :
         options.extra_implementation_rules) {
      customized.Add(extra);
    }
    implementation_rules = &customized;
  }

  cascades::SearchEngine search(std::move(memo), options.relational_rules);
  search.SetStepBudget(options.search_step_budget);
  std::optional<cascades::BestPlan> best = search.Optimize(
      search_root, properties, *implementation_rules, rule_context);
  if (!best) {
    return Status::kNotImplemented;
  }

  // A projection group can be subsumed by a commuted join alternative when
  // the required output properties are otherwise satisfied.  Keep the
  // selected output contract explicit at the boundary: join reordering must
  // never leak the optimizer's internal column order to callers.
  if (!has_aggregate) {
    Plan projection =
        std::make_shared<ProjectionPlan>(best->plan, projection_items);
    const Schema& projected_schema = projection->GetSchema();
    const Schema& actual_schema = best->plan->GetSchema();
    bool same_layout =
        projected_schema.ColumnCount() == actual_schema.ColumnCount();
    for (size_t i = 0; same_layout && i < projected_schema.ColumnCount(); ++i) {
      same_layout = projected_schema.GetColumn(i).Name() ==
                    actual_schema.GetColumn(i).Name();
    }
    if (!same_layout) {
      best->plan = std::move(projection);
    }
  }

  // Emit the decorrelated semi/anti joins around the optimized core: the
  // inner side is planned recursively (nested subqueries recurse further),
  // then the wrap keeps the outer schema, rows, and row positions.
  for (DecorrelationSpec& spec : decorrelations) {
    QueryData inner_query;
    inner_query.from_ = spec.from;
    inner_query.aliases_ = spec.aliases;
    inner_query.where_ = CombineConjuncts(spec.inner_conjuncts);
    if (!inner_query.where_) {
      inner_query.where_ = ConstantValueExp(Value(true));
    }
    for (const Expression& inner_key : spec.inner_keys) {
      inner_query.select_.emplace_back(
          inner_key->AsColumnValue().GetColumnName());
    }
    ASSIGN_OR_RETURN(Plan, inner_plan, Optimize(inner_query, ctx, options));
    std::vector<ColumnName> outer_keys;
    std::vector<ColumnName> inner_keys;
    outer_keys.reserve(spec.outer_keys.size());
    inner_keys.reserve(spec.inner_keys.size());
    for (const Expression& outer_key : spec.outer_keys) {
      outer_keys.push_back(outer_key->AsColumnValue().GetColumnName());
    }
    for (const Expression& inner_key : spec.inner_keys) {
      inner_keys.push_back(inner_key->AsColumnValue().GetColumnName());
    }
    best->plan = std::make_shared<ProductPlan>(
        best->plan, std::move(outer_keys), inner_plan, std::move(inner_keys),
        spec.kind);
  }

  // Correlation keys added only for the semi/anti join are implementation
  // columns.  Restore the caller-visible projection after all joins so a
  // decorrelated predicate never leaks hidden key columns into the result.
  if (!has_aggregate && !decorrelations.empty()) {
    best->plan = std::make_shared<ProjectionPlan>(best->plan, expanded_select);
    // With hidden semi-join keys in the core output, an in-search DISTINCT
    // would dedupe over the implementation columns and leak duplicates that
    // differ only in a key; dedupe above the trim instead.
    if (query.distinct_ && hidden_semi_keys_added) {
      best->plan = std::make_shared<DistinctPlan>(best->plan);
    }
  }

  if (options.dump_memo) {
    std::ostringstream dump;
    search.GetMemo().Dump(dump);
    std::vector<std::string> applied(search.AppliedRuleNames().begin(),
                                     search.AppliedRuleNames().end());
    std::ranges::sort(applied);
    dump << "applied rules:";
    for (const std::string& name : applied) {
      dump << ' ' << name;
    }
    if (search.BudgetExhausted()) {
      dump << "\n[exploration budget exhausted: best-so-far plan]";
    }
    dump << "\nchosen plan:\n" << *best->plan;
    LOG(INFO) << "cascades memo:\n" << dump.str();
  }
  return best->plan;
}

}  // namespace tinylamb
