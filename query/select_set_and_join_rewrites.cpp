/**
 * Copyright 2026 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/set_operation.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/window_eval.hpp"
#include "executor/executor_base.hpp"
#include "executor/relational.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "query/query_data.hpp"
#include "query/select_rewrites.hpp"
#include "query/select_rewrites_internal.hpp"
#include "query/sql_engine.hpp"
#include "query/statement.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "table/tlcs_file.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

using namespace select_rewrite_detail;
namespace select_rewrite_detail {

bool TpcdsExprHasParam(const Expression& expr) {  // NOLINT(misc-no-recursion)
  if (!expr) {
    return false;
  }
  // Template-bound SELECT literals (`!UsesPreparedValues()`) already carry the
  // concrete bound Value for this execution; callers disarm compiled-plan
  // caching (`dml_fold_applied_ = true`) whenever a literal-dependent rewrite
  // fires. Only deferred INSERT PreparedValues slots lack a prepare-time Value.
  if (const auto* slot = dynamic_cast<const ParameterSlot*>(expr.get());
      slot != nullptr && slot->UsesPreparedValues()) {
    return true;
  }
  if (expr->Type() == TypeTag::kQueryExp) {
    const auto& q = expr->AsQueryExpression();
    if (q.Test() && TpcdsExprHasParam(q.Test())) {
      return true;
    }
    if (q.Query() != nullptr) {
      const SelectStatement& s = *q.Query();
      if (TpcdsExprHasParam(s.WhereClause()) || TpcdsExprHasParam(s.Having()) ||
          TpcdsExprHasParam(s.Qualify())) {
        return true;
      }
      for (const NamedExpression& item : s.SelectList()) {
        if (TpcdsExprHasParam(item.expression)) {
          return true;
        }
      }
      // The statement-level scanner covers these clauses; the expression-level
      // one must too, or a parameter hidden in a subquery's GROUP BY / ORDER BY
      // / DISTINCT ON would be treated as a closed constant at plan time.
      for (const Expression& e : s.GroupBy()) {
        if (TpcdsExprHasParam(e)) {
          return true;
        }
      }
      for (const auto& ord : s.OrderBy()) {
        if (TpcdsExprHasParam(ord.expression)) {
          return true;
        }
      }
      for (const Expression& d : s.DistinctOn()) {
        if (TpcdsExprHasParam(d)) {
          return true;
        }
      }
    }
    return false;
  }
  return std::ranges::any_of(ExpressionChildren(expr), TpcdsExprHasParam);
}

bool TpcdsStmtHasParam(  // NOLINT(misc-no-recursion)
    const SelectStatement& stmt) {
  if (TpcdsExprHasParam(stmt.WhereClause()) ||
      TpcdsExprHasParam(stmt.Having()) || TpcdsExprHasParam(stmt.Qualify())) {
    return true;
  }
  for (const NamedExpression& item : stmt.SelectList()) {
    if (TpcdsExprHasParam(item.expression)) {
      return true;
    }
  }
  for (const Expression& g : stmt.GroupBy()) {
    if (TpcdsExprHasParam(g)) {
      return true;
    }
  }
  for (const auto& ord : stmt.OrderBy()) {
    if (TpcdsExprHasParam(ord.expression)) {
      return true;
    }
  }
  for (const Expression& d : stmt.DistinctOn()) {
    if (TpcdsExprHasParam(d)) {
      return true;
    }
  }
  for (const SelectSource& src : stmt.Sources()) {
    if (TpcdsExprHasParam(src.join_condition) ||
        TpcdsExprHasParam(src.unnest) ||
        (src.query != nullptr && TpcdsStmtHasParam(*src.query))) {
      return true;
    }
  }
  for (const auto& br : stmt.UnionAll()) {
    if (br != nullptr && TpcdsStmtHasParam(*br)) {
      return true;
    }
  }
  if (const auto& tree = stmt.GetSetOperationTree(); tree != nullptr) {
    if (tree->first != nullptr && TpcdsStmtHasParam(*tree->first)) {
      return true;
    }
    for (const auto& br : tree->branches) {
      // NOLINTNEXTLINE(readability-use-anyofallof) explicit loop is clearer
      // here
      if (br != nullptr && TpcdsStmtHasParam(*br)) {
        return true;
      }
    }
  }
  return std::ranges::any_of(stmt.WithQueries(), [](const auto& entry) {
    const auto& [name, body] = entry;
    (void)name;
    return body != nullptr && TpcdsStmtHasParam(*body);
  });
}

bool IsSafeDisjointTagValue(const Value& val) noexcept {
  if (val.IsNull() || val.Collation() != 0) {
    return false;
  }
  return val.type == ValueType::kVarChar || val.type == ValueType::kInt64 ||
         val.type == ValueType::kDate;
}

std::optional<Value> TpcdsEvalClosedConstant(const Expression& expr) {
  if (!expr || TpcdsExprHasParam(expr)) {
    return std::nullopt;
  }
  if (expr->Type() == TypeTag::kConstantValue) {
    return expr->AsConstantValue().GetValue();
  }
  // Parameter slots were already rejected above; this second guard only
  // needs to rule out subqueries, aggregates, volatile nodes and free
  // columns before a constant expression is evaluated at plan time.
  if (ContainsQueryExpression(expr) ||
      relational_detail::ContainsAggregate(expr) ||
      !DerivedExpressionIsImmutable(expr) || !expr->TouchedColumns().empty()) {
    return std::nullopt;
  }
  StatusOr<Value> evaluated = expr->TryEvaluate(Row{}, Schema{});
  if (!evaluated.HasValue()) {
    return std::nullopt;
    // NOLINTNEXTLINE(readability-use-anyofallof) explicit loop is clearer here
  }
  return evaluated.MoveValue();
}

bool SelectListHasStarOrModifiers(const SelectStatement& stmt) {
  return std::ranges::any_of(
      stmt.SelectList(), [](const NamedExpression& item) {
        if (item.star_modifiers != nullptr || !item.expression) {
          // The frontend can represent a (possibly qualified) star with a null
          // expression. Treat it conservatively as a star: rewrites that
          // project named columns must not run on such an item, since its
          // output arity is not known from the select list alone.
          return true;
        }
        if (item.expression->Type() == TypeTag::kColumnValue &&
            item.expression->AsColumnValue().GetColumnName().name == "*") {
          return true;
        }
        return false;
      });
}

std::string ExplicitOrColumnOutputName(const NamedExpression& item) {
  if (!item.name.empty()) {
    return item.name;
  }
  if (item.expression && item.expression->Type() == TypeTag::kColumnValue) {
    const ColumnName& col = item.expression->AsColumnValue().GetColumnName();
    if (!col.name.empty() && col.name != "*") {
      return col.name;
    }
  }
  return {};
}

std::string InferSelectItemOutputName(const NamedExpression& item,
                                      size_t index) {
  std::string explicit_or_col = ExplicitOrColumnOutputName(item);
  if (!explicit_or_col.empty()) {
    return explicit_or_col;
  }
  // Synthetic name for internal slot bookkeeping only. It intentionally
  // mirrors the engine's fallback label for an unnamed expression, but must
  // never be written back into a SELECT item's `name`: that would make the
  // client-visible RowDescription label depend on whether a rewrite fired.
  return "$col_" + std::to_string(index);
}

bool ExtractPureUnionAllBranches(  // NOLINT(misc-no-recursion)
    const SelectStatement& union_stmt,
    std::vector<std::shared_ptr<SelectStatement>>* out_branches) {
  if (union_stmt.UnionDistinct() || union_stmt.UnionByName() ||
      union_stmt.HasLimit() || union_stmt.Offset() != 0 ||
      !union_stmt.OrderBy().empty() || !union_stmt.WithQueries().empty()) {
    return false;
  }
  // Unwrap a trivial pass-through projection wrapper `SELECT c1, c2 FROM
  // (SELECT ... UNION ALL SELECT ...) sq1` (e.g. TPC-DS Q02 `wscs`).
  if (union_stmt.UnionAll().empty() &&
      union_stmt.GetSetOperationTree() == nullptr &&
      union_stmt.GroupBy().empty() && !union_stmt.IsGroupAll() &&
      !union_stmt.Having() && !union_stmt.Distinct() &&
      !union_stmt.HasDistinctOn() && !union_stmt.WhereClause() &&
      !union_stmt.Qualify() && !union_stmt.ForUpdate() &&
      !union_stmt.AsStruct() && !union_stmt.WithTies() &&
      !SelectListHasStarOrModifiers(union_stmt) &&
      union_stmt.Sources().size() == 1) {
    const SelectSource& sq = union_stmt.Sources()[0];
    if (sq.query != nullptr && !sq.unnest && !sq.is_lateral && !sq.sample &&
        sq.using_columns.empty() && !sq.from_nested_join &&
        !sq.join_condition) {
      std::vector<std::shared_ptr<SelectStatement>> inner_branches;
      if (ExtractPureUnionAllBranches(*sq.query, &inner_branches) &&
          !inner_branches.empty() &&
          inner_branches[0]->SelectList().size() ==
              union_stmt.SelectList().size()) {
        bool passthrough = true;
        for (size_t c = 0; c < union_stmt.SelectList().size(); ++c) {
          const NamedExpression& outer_item = union_stmt.SelectList()[c];
          const std::string inner_nm =
              ExplicitOrColumnOutputName(inner_branches[0]->SelectList()[c]);
          const std::string outer_nm = ExplicitOrColumnOutputName(outer_item);
          if (inner_nm.empty() || outer_nm.empty() ||
              !IdentifierEquals(outer_nm, inner_nm) || !outer_item.expression ||
              outer_item.expression->Type() != TypeTag::kColumnValue) {
            passthrough = false;
            break;
          }
          const ColumnName& oc =
              outer_item.expression->AsColumnValue().GetColumnName();
          if ((!oc.schema.empty() &&
               (sq.alias.empty() || !IdentifierEquals(oc.schema, sq.alias))) ||
              !IdentifierEquals(oc.name, inner_nm)) {
            passthrough = false;
            break;
          }
        }
        if (passthrough) {
          *out_branches = std::move(inner_branches);
          return true;
        }
      }
    }
  }
  for (const SetOperationMatch& m : union_stmt.Matches()) {
    if (m.by_name || m.corresponding || !m.columns.empty()) {
      return false;
    }
  }
  if (const auto& tree = union_stmt.GetSetOperationTree(); tree != nullptr) {
    if (tree->first == nullptr || tree->branches.empty() ||
        tree->kinds.size() != tree->branches.size()) {
      return false;
    }
    for (SetOperationKind k : tree->kinds) {
      if (k != SetOperationKind::kUnionAll) {
        return false;
      }
    }
    out_branches->push_back(std::make_shared<SelectStatement>(*tree->first));
    for (const auto& br : tree->branches) {
      if (br == nullptr) {
        return false;
      }
      out_branches->push_back(std::make_shared<SelectStatement>(*br));
    }
    return out_branches->size() >= 2;
  }
  if (union_stmt.UnionAll().empty() ||
      union_stmt.SetOperationKinds().size() != union_stmt.UnionAll().size()) {
    return false;
  }
  for (SetOperationKind k : union_stmt.SetOperationKinds()) {
    if (k != SetOperationKind::kUnionAll) {
      return false;
    }
  }
  auto head = std::make_shared<SelectStatement>(union_stmt);
  head->ClearUnionAll();
  out_branches->push_back(std::move(head));
  for (const auto& br : union_stmt.UnionAll()) {
    if (br == nullptr) {
      return false;
    }
    out_branches->push_back(std::make_shared<SelectStatement>(*br));
  }
  return out_branches->size() >= 2;
}

// Lean proof: lean/TinylambOpt/TpcdsNextDiscovery.lean
// (using_join_inner_prefix_reorder_and_filter_sound,
//  outer_and_using_join_prefix_reorder_sound)
namespace {
bool PullInnerJoinsBeforeLeftJoinsOne(
    SelectStatement* stmt, TransactionContext& ctx,
    const std::unordered_map<std::string, std::shared_ptr<SelectStatement>>&
        ctes) {
  if (stmt == nullptr || stmt->Sources().size() < 3 ||
      !stmt->UnionAll().empty() || stmt->GetSetOperationTree() != nullptr ||
      SelectListHasStarOrModifiers(*stmt)) {
    return false;
  }
  struct SourceInfo {
    std::string alias;
    std::string table;
    std::unordered_set<std::string> lowered_cols;
    std::vector<std::string> using_columns;
  };
  auto resolve_source_info =
      [&](const SelectSource& src) -> std::optional<SourceInfo> {
    if (src.is_lateral || src.unnest || src.sample || src.from_nested_join ||
        (src.join_type != JoinType::kCross &&
         src.join_type != JoinType::kInner &&
         src.join_type != JoinType::kLeft)) {
      return std::nullopt;
    }
    SourceInfo info;
    info.table = src.table;
    info.alias = src.alias.empty() ? src.table : src.alias;
    info.using_columns = src.using_columns;
    if (info.alias.empty()) {
      return std::nullopt;
    }
    if (src.query != nullptr) {
      if (SelectListHasStarOrModifiers(*src.query)) {
        return std::nullopt;
      }
      for (const NamedExpression& item : src.query->SelectList()) {
        const std::string out_nm = ExplicitOrColumnOutputName(item);
        if (out_nm.empty()) {
          return std::nullopt;
        }
        info.lowered_cols.insert(LoweredStarName(out_nm));
      }
      return info;
    }
    if (const auto cte_it = ctes.find(src.table); cte_it != ctes.end()) {
      if (cte_it->second == nullptr ||
          SelectListHasStarOrModifiers(*cte_it->second)) {
        return std::nullopt;
      }
      for (const NamedExpression& item : cte_it->second->SelectList()) {
        const std::string out_nm = ExplicitOrColumnOutputName(item);
        if (out_nm.empty()) {
          return std::nullopt;
        }
        info.lowered_cols.insert(LoweredStarName(out_nm));
      }
      return info;
    }
    StatusOr<std::shared_ptr<Table>> tbl = ctx.GetTable(src.table);
    if (!tbl.HasValue()) {
      return std::nullopt;
    }
    const Schema& schema = tbl.Value()->GetSchema();
    for (size_t i = 0; i < schema.ColumnCount(); ++i) {
      info.lowered_cols.insert(
          LoweredStarName(schema.GetColumn(i).Name().name));
    }
    return info;
  };

  std::vector<SelectSource> sources = stmt->Sources();
  std::vector<SourceInfo> infos;
  infos.reserve(sources.size());
  for (const SelectSource& s : sources) {
    std::optional<SourceInfo> inf = resolve_source_info(s);
    if (!inf.has_value()) {
      return false;
    }
    infos.push_back(std::move(*inf));
  }

  bool any_swapped = false;
  bool progress = true;
  while (progress) {
    progress = false;
    for (size_t i = 1; i + 1 < sources.size(); ++i) {
      if (sources[i].join_type != JoinType::kLeft ||
          (sources[i + 1].join_type != JoinType::kInner &&
           sources[i + 1].join_type != JoinType::kCross)) {
        continue;
      }
      const SelectSource& c_src = sources[i + 1];
      const SourceInfo& b_info = infos[i];
      bool ok = true;
      if (c_src.join_condition) {
        if (ContainsQueryExpression(c_src.join_condition) ||
            relational_detail::ContainsAggregate(c_src.join_condition) ||
            !DerivedExpressionIsImmutable(c_src.join_condition)) {
          ok = false;
        } else {
          for (const ColumnName& col : c_src.join_condition->TouchedColumns()) {
            if (col.name == "*") {
              ok = false;
              break;
            }
            if (!col.schema.empty()) {
              if (IdentifierEquals(col.schema, b_info.alias) ||
                  (!b_info.table.empty() &&
                   IdentifierEquals(col.schema, b_info.table))) {
                ok = false;
                break;
              }
            } else if (b_info.lowered_cols.contains(
                           LoweredStarName(col.name))) {
              ok = false;
              break;
            }
          }
        }
      }
      // USING joins carry their predicate in `using_columns` rather than in
      // `join_condition`.  Moving `c_src` below the LEFT JOIN changes what its
      // USING columns resolve against, so refuse the swap whenever a USING
      // column names a column of the source being crossed.  Symmetrically, the
      // LEFT source's own USING predicate must not read the moved-in source.
      // This is the C++ guard corresponding to the two Lean theorems above.
      if (ok) {
        const auto using_reads = [](const std::vector<std::string>& cols,
                                    const SourceInfo& other) {
          return std::ranges::any_of(cols, [&](const std::string& col) {
            return other.lowered_cols.contains(LoweredStarName(col));
          });
        };
        if (using_reads(c_src.using_columns, b_info) ||
            using_reads(sources[i].using_columns, infos[i + 1])) {
          ok = false;
        }
      }
      if (!ok) {
        continue;
      }
      std::swap(sources[i], sources[i + 1]);
      std::swap(infos[i], infos[i + 1]);
      any_swapped = true;
      progress = true;
    }
  }
  if (any_swapped) {
    stmt->SetSources(std::move(sources));
  }
  return any_swapped;
}
}  // namespace

}  // namespace select_rewrite_detail

// Lean proof: lean/TinylambOpt/TpcdsDiscovery.lean
//   (outer_join_intervening_inner_to_antijoin_sound,
//    inner_prefix_reorder_and_filter_under_left_joins_sound)
bool PullInnerJoinsBeforeLeftJoins(SelectStatement* select,
                                   TransactionContext& ctx) {
  if (select == nullptr) {
    return false;
  }
  bool changed = false;
  for (const auto& [name, body] : select->WithQueries()) {
    if (select->IsRecursiveWith(name) || body == nullptr) {
      continue;
    }
    changed |= PullInnerJoinsBeforeLeftJoinsOne(body.get(), ctx,
                                                select->WithQueries());
  }
  for (SelectSource& src :
       const_cast<std::vector<SelectSource>&>(select->Sources())) {
    if (src.query != nullptr && !src.is_lateral) {
      changed |= PullInnerJoinsBeforeLeftJoinsOne(src.query.get(), ctx,
                                                  select->WithQueries());
    }
  }
  changed |=
      PullInnerJoinsBeforeLeftJoinsOne(select, ctx, select->WithQueries());
  return changed;
}

namespace select_rewrite_detail {

namespace {
bool DistributeOneUnionGroupBy(SelectStatement* stmt, TransactionContext& ctx) {
  if (stmt == nullptr || stmt->GroupBy().empty() || stmt->IsGroupAll() ||
      stmt->Having() || stmt->Qualify() || stmt->Distinct() ||
      stmt->HasDistinctOn() || stmt->ForUpdate() || stmt->AsStruct() ||
      stmt->WithTies() || !stmt->WithQueries().empty() ||
      !stmt->UnionAll().empty() || stmt->GetSetOperationTree() != nullptr ||
      relational_detail::HasWindowFunctions(*stmt) ||
      SelectListHasStarOrModifiers(*stmt) || TpcdsStmtHasParam(*stmt) ||
      stmt->Sources().size() < 2) {
    return false;
  }
  std::optional<size_t> u_idx;
  for (size_t i = 0; i < stmt->Sources().size(); ++i) {
    const SelectSource& s = stmt->Sources()[i];
    if (s.is_lateral || s.unnest || s.sample || s.from_nested_join ||
        !s.using_columns.empty() ||
        (s.join_type != JoinType::kCross && s.join_type != JoinType::kInner)) {
      return false;
    }
    if (s.query != nullptr) {
      if (u_idx.has_value() || s.alias.empty()) {
        return false;
      }
      u_idx = i;
    } else if (s.table.empty()) {
      return false;
    }
  }
  if (!u_idx.has_value()) {
    return false;
  }
  const SelectSource& u_src = stmt->Sources()[*u_idx];
  std::vector<std::shared_ptr<SelectStatement>> branches;
  if (!ExtractPureUnionAllBranches(*u_src.query, &branches)) {
    return false;
  }

  std::vector<std::shared_ptr<Table>> dim_holders;
  std::vector<GroupingRelation> dim_rels;
  std::unordered_set<std::string> dim_names_lower;
  for (size_t i = 0; i < stmt->Sources().size(); ++i) {
    if (i == *u_idx) {
      continue;
    }
    const SelectSource& ds = stmt->Sources()[i];
    StatusOr<std::shared_ptr<Table>> tbl = ctx.GetTable(ds.table);
    if (!tbl.HasValue()) {
      return false;
    }
    dim_holders.push_back(tbl.MoveValue());
    const std::string alias = ds.alias.empty() ? ds.table : ds.alias;
    if (!dim_names_lower.insert(LoweredStarName(alias)).second) {
      return false;
    }
    dim_rels.push_back(
        {.name = alias, .schema = &dim_holders.back()->GetSchema()});
  }

  const size_t u_arity = branches[0]->SelectList().size();
  if (u_arity == 0) {
    return false;
  }
  std::vector<std::string> u_col_names;
  u_col_names.reserve(u_arity);
  std::unordered_map<std::string, size_t> u_col_index;
  u_col_index.reserve(u_arity);
  for (size_t c = 0; c < u_arity; ++c) {
    const std::string out_nm =
        ExplicitOrColumnOutputName(branches[0]->SelectList()[c]);
    if (out_nm.empty()) {
      return false;
    }
    std::string low = LoweredStarName(out_nm);
    if (!u_col_index.emplace(low, c).second) {
      return false;
    }
    u_col_names.push_back(out_nm);
  }

  auto qualify_against_rels =
      [&](auto& self, const Expression& expr,
          const std::vector<GroupingRelation>& rels,
          const std::unordered_map<std::string, size_t>* allow_u,
          const std::string& u_alias) -> std::optional<Expression> {
    if (!expr) {
      return expr;
    }
    if (expr->Type() == TypeTag::kQueryExp ||
        expr->Type() == TypeTag::kWindowFunctionExp) {
      return std::nullopt;
    }
    if (expr->Type() == TypeTag::kColumnValue) {
      const ColumnName& col = expr->AsColumnValue().GetColumnName();
      if (col.name == "*") {
        return expr;
      }
      if (!col.schema.empty()) {
        if (allow_u != nullptr && IdentifierEquals(col.schema, u_alias)) {
          if (!allow_u->contains(LoweredStarName(col.name))) {
            return std::nullopt;
          }
          return ColumnValueExp(ColumnName(u_alias, col.name));
        }
        GroupingRelation resolved;
        if (!ResolveGroupingColumn(rels, col, &resolved)) {
          return std::nullopt;
        }
        return ColumnValueExp(ColumnName(resolved.name, col.name));
      }
      const std::string low = LoweredStarName(col.name);
      const bool in_u = allow_u != nullptr && allow_u->contains(low);
      GroupingRelation resolved_dim;
      const bool in_dim = ResolveGroupingColumn(rels, col, &resolved_dim);
      if (in_u && in_dim) {
        return std::nullopt;
      }
      if (in_u) {
        return ColumnValueExp(ColumnName(u_alias, col.name));
      }
      if (in_dim) {
        return ColumnValueExp(ColumnName(resolved_dim.name, col.name));
      }
      return std::nullopt;
    }
    std::vector<Expression> kids = ExpressionChildren(expr);
    for (Expression& k : kids) {
      std::optional<Expression> qk = self(self, k, rels, allow_u, u_alias);
      if (!qk.has_value()) {
        return std::nullopt;
      }
      k = std::move(*qk);
    }
    return WithExpressionChildren(expr, std::move(kids));
  };

  // Validate and qualify outer GroupBy, WhereClause, D join conditions, and
  // SelectList aggregates.
  std::vector<Expression> qual_group_keys;
  for (const Expression& gk : stmt->GroupBy()) {
    if (!gk || gk->Type() != TypeTag::kColumnValue) {
      return false;
    }
    std::optional<Expression> qgk = qualify_against_rels(
        qualify_against_rels, gk, dim_rels, &u_col_index, u_src.alias);
    if (!qgk.has_value()) {
      return false;
    }
    qual_group_keys.push_back(std::move(*qgk));
  }

  std::vector<Expression> outer_where_parts;
  if (stmt->WhereClause()) {
    if (!DerivedExpressionIsImmutable(stmt->WhereClause()) ||
        relational_detail::ContainsAggregate(stmt->WhereClause())) {
      return false;
    }
    std::optional<Expression> qw =
        qualify_against_rels(qualify_against_rels, stmt->WhereClause(),
                             dim_rels, &u_col_index, u_src.alias);
    if (!qw.has_value()) {
      return false;
    }
    outer_where_parts.push_back(std::move(*qw));
  }
  for (const auto& i : stmt->Sources()) {
    if (i.join_condition) {
      if (!DerivedExpressionIsImmutable(i.join_condition) ||
          relational_detail::ContainsAggregate(i.join_condition)) {
        return false;
      }
      std::optional<Expression> qjc =
          qualify_against_rels(qualify_against_rels, i.join_condition, dim_rels,
                               &u_col_index, u_src.alias);
      if (!qjc.has_value()) {
        return false;
      }
      outer_where_parts.push_back(std::move(*qjc));
    }
  }
  bool has_dim_only_filter = false;
  for (const Expression& ow : outer_where_parts) {
    for (const Expression& conj : SplitConjuncts(ow)) {
      const auto touched = conj->TouchedColumns();
      if (!touched.empty() &&
          std::ranges::none_of(touched, [&](const ColumnName& cn) {
            return IdentifierEquals(cn.schema, u_src.alias);
          })) {
        has_dim_only_filter = true;
        break;
      }
    }
    if (has_dim_only_filter) {
      break;
    }
  }
  if (!has_dim_only_filter) {
    return false;
  }

  struct OuterSelectSlot {
    std::string out_name;
    bool is_agg{false};
    size_t key_idx{0};
    AggregationType agg_type{AggregationType::kSum};
    Expression qual_agg_arg;
  };
  std::vector<OuterSelectSlot> select_slots;
  for (size_t idx = 0; idx < stmt->SelectList().size(); ++idx) {
    const NamedExpression& item = stmt->SelectList()[idx];
    OuterSelectSlot slot;
    slot.out_name = InferSelectItemOutputName(item, idx);
    if (item.expression->Type() == TypeTag::kAggregateExp) {
      const auto& agg = item.expression->AsAggregateExpression();
      if (agg.Distinct() || agg.NeedsGroupContext() || agg.HasWhereFilter() ||
          !agg.InnerGroupBy().empty() || !agg.InnerOrderBy().empty() ||
          !agg.Child() || !DerivedExpressionIsImmutable(agg.Child())) {
        return false;
      }
      if (agg.GetType() != AggregationType::kSum &&
          agg.GetType() != AggregationType::kCount &&
          agg.GetType() != AggregationType::kMin &&
          agg.GetType() != AggregationType::kMax) {
        return false;
      }
      std::optional<Expression> qarg =
          qualify_against_rels(qualify_against_rels, agg.Child(), dim_rels,
                               &u_col_index, u_src.alias);
      if (!qarg.has_value()) {
        return false;
      }
      slot.is_agg = true;
      slot.agg_type = agg.GetType();
      slot.qual_agg_arg = std::move(*qarg);
      select_slots.push_back(std::move(slot));
      continue;
    }
    std::optional<Expression> qitem =
        qualify_against_rels(qualify_against_rels, item.expression, dim_rels,
                             &u_col_index, u_src.alias);
    if (!qitem.has_value() || (*qitem)->Type() != TypeTag::kColumnValue) {
      return false;
    }
    const ColumnName& item_col = (*qitem)->AsColumnValue().GetColumnName();
    std::optional<size_t> matched_gk;
    for (size_t g = 0; g < qual_group_keys.size(); ++g) {
      const ColumnName& gcol =
          qual_group_keys[g]->AsColumnValue().GetColumnName();
      if (IdentifierEquals(gcol.schema, item_col.schema) &&
          IdentifierEquals(gcol.name, item_col.name)) {
        matched_gk = g;
        break;
      }
    }
    if (!matched_gk.has_value()) {
      return false;
    }
    slot.is_agg = false;
    slot.key_idx = *matched_gk;
    select_slots.push_back(std::move(slot));
  }

  // Rebind outer OrderBy terms against outer SelectList output names or group
  // keys.
  std::vector<SelectStatement::OrderByTerm> new_outer_order;
  for (const auto& term : stmt->OrderBy()) {
    SelectStatement::OrderByTerm nt = term;
    if (term.expression && term.expression->Type() == TypeTag::kConstantValue) {
      new_outer_order.push_back(std::move(nt));
      continue;
    }
    if (term.expression && term.expression->Type() == TypeTag::kColumnValue) {
      const ColumnName& oc = term.expression->AsColumnValue().GetColumnName();
      if (oc.schema.empty()) {
        bool matched_alias = false;
        for (auto& select_slot : select_slots) {
          if (IdentifierEquals(select_slot.out_name, oc.name)) {
            nt.expression =
                ColumnValueExp(ColumnName("", select_slot.out_name));
            matched_alias = true;
            break;
          }
        }
        if (matched_alias) {
          new_outer_order.push_back(std::move(nt));
          continue;
        }
      }
      std::optional<Expression> qord =
          qualify_against_rels(qualify_against_rels, term.expression, dim_rels,
                               &u_col_index, u_src.alias);
      if (qord.has_value() && (*qord)->Type() == TypeTag::kColumnValue) {
        const ColumnName& qc = (*qord)->AsColumnValue().GetColumnName();
        bool matched_gk = false;
        for (size_t g = 0; g < qual_group_keys.size(); ++g) {
          const ColumnName& gcol =
              qual_group_keys[g]->AsColumnValue().GetColumnName();
          if (IdentifierEquals(gcol.schema, qc.schema) &&
              IdentifierEquals(gcol.name, qc.name)) {
            nt.expression = ColumnValueExp(
                ColumnName("__tpcds_upre", "__gk_" + std::to_string(g)));
            matched_gk = true;
            break;
          }
        }
        if (matched_gk) {
          new_outer_order.push_back(std::move(nt));
          continue;
        }
      }
    }
    return false;
  }

  // Build each pre-aggregated branch.
  std::vector<std::shared_ptr<SelectStatement>> pre_branches;
  pre_branches.reserve(branches.size());
  for (const auto& br : branches) {
    if (!br->GroupBy().empty() || br->Having() || br->Distinct() ||
        br->HasDistinctOn() || br->HasLimit() || br->Offset() != 0 ||
        br->Qualify() || !br->WithQueries().empty() ||
        !br->UnionAll().empty() || br->GetSetOperationTree() != nullptr ||
        relational_detail::HasWindowFunctions(*br) ||
        br->SelectList().size() != u_arity ||
        SelectListHasStarOrModifiers(*br) || br->Sources().empty()) {
      return false;
    }
    std::vector<std::shared_ptr<Table>> b_holders;
    std::vector<GroupingRelation> b_rels;
    for (const SelectSource& bs : br->Sources()) {
      if (bs.table.empty() || bs.query != nullptr || bs.is_lateral ||
          bs.unnest || bs.sample || bs.from_nested_join ||
          !bs.using_columns.empty() ||
          (bs.join_type != JoinType::kCross &&
           bs.join_type != JoinType::kInner &&
           bs.join_type != JoinType::kLeft)) {
        return false;
      }
      const std::string balias = bs.alias.empty() ? bs.table : bs.alias;
      if (dim_names_lower.contains(LoweredStarName(balias)) ||
          dim_names_lower.contains(LoweredStarName(bs.table))) {
        return false;
      }
      StatusOr<std::shared_ptr<Table>> btbl = ctx.GetTable(bs.table);
      if (!btbl.HasValue()) {
        return false;
      }
      b_holders.push_back(btbl.MoveValue());
      b_rels.push_back(
          {.name = balias, .schema = &b_holders.back()->GetSchema()});
    }

    std::vector<Expression> b_outputs(u_arity);
    for (size_t c = 0; c < u_arity; ++c) {
      const Expression& bexpr = br->SelectList()[c].expression;
      if (!bexpr || ContainsQueryExpression(bexpr) ||
          relational_detail::ContainsAggregate(bexpr) ||
          !DerivedExpressionIsImmutable(bexpr)) {
        return false;
      }
      std::optional<Expression> qb = qualify_against_rels(
          qualify_against_rels, bexpr, b_rels, nullptr, "");
      if (!qb.has_value()) {
        return false;
      }
      b_outputs[c] = std::move(*qb);
    }

    auto subst_u = [&](auto& self,
                       const Expression& expr) -> std::optional<Expression> {
      if (!expr) {
        return expr;
      }
      if (expr->Type() == TypeTag::kColumnValue) {
        const ColumnName& col = expr->AsColumnValue().GetColumnName();
        if (IdentifierEquals(col.schema, u_src.alias)) {
          const auto found = u_col_index.find(LoweredStarName(col.name));
          if (found == u_col_index.end() || found->second >= b_outputs.size()) {
            return std::nullopt;
          }
          return b_outputs[found->second];
        }
        return expr;
      }
      std::vector<Expression> kids = ExpressionChildren(expr);
      for (Expression& k : kids) {
        auto mapped = self(self, k);
        if (!mapped.has_value()) {
          return std::nullopt;
        }
        k = std::move(*mapped);
      }
      return WithExpressionChildren(expr, std::move(kids));
    };

    std::vector<SelectSource> merged_sources;
    merged_sources.reserve(br->Sources().size());
    for (SelectSource bs : br->Sources()) {
      if (bs.alias.empty()) {
        bs.alias = bs.table;
      }
      if (bs.join_condition) {
        if (!DerivedExpressionIsImmutable(bs.join_condition) ||
            ContainsQueryExpression(bs.join_condition) ||
            relational_detail::ContainsAggregate(bs.join_condition)) {
          return false;
        }
        std::optional<Expression> qbjc = qualify_against_rels(
            qualify_against_rels, bs.join_condition, b_rels, nullptr, "");
        if (!qbjc.has_value()) {
          return false;
        }
        bs.join_condition = std::move(*qbjc);
      }
      merged_sources.push_back(std::move(bs));
    }
    for (size_t i = 0; i < stmt->Sources().size(); ++i) {
      if (i == *u_idx) {
        continue;
      }
      SelectSource ds = stmt->Sources()[i];
      if (ds.alias.empty()) {
        ds.alias = ds.table;
      }
      ds.join_type = JoinType::kCross;
      ds.join_condition = nullptr;
      merged_sources.push_back(std::move(ds));
    }

    std::vector<Expression> b_where_parts;
    if (br->WhereClause()) {
      if (!DerivedExpressionIsImmutable(br->WhereClause()) ||
          ContainsQueryExpression(br->WhereClause()) ||
          relational_detail::ContainsAggregate(br->WhereClause())) {
        return false;
      }
      std::optional<Expression> qbw = qualify_against_rels(
          qualify_against_rels, br->WhereClause(), b_rels, nullptr, "");
      if (!qbw.has_value()) {
        return false;
      }
      b_where_parts.push_back(std::move(*qbw));
    }
    for (const Expression& ow : outer_where_parts) {
      auto sub_where = subst_u(subst_u, ow);
      if (!sub_where.has_value()) {
        return false;
      }
      b_where_parts.push_back(std::move(*sub_where));
    }

    std::vector<Expression> b_group;
    std::vector<NamedExpression> b_select;
    for (size_t g = 0; g < qual_group_keys.size(); ++g) {
      auto sub_g = subst_u(subst_u, qual_group_keys[g]);
      if (!sub_g.has_value() || !*sub_g) {
        return false;
      }
      b_select.emplace_back("__gk_" + std::to_string(g), *sub_g);
      if ((*sub_g)->Type() != TypeTag::kConstantValue) {
        b_group.push_back(std::move(*sub_g));
      }
    }
    if (b_group.empty()) {
      b_group.push_back(b_select.front().expression);
    }
    size_t agg_seq = 0;
    for (const OuterSelectSlot& slot : select_slots) {
      if (!slot.is_agg) {
        continue;
      }
      auto sub_arg = subst_u(subst_u, slot.qual_agg_arg);
      if (!sub_arg.has_value()) {
        return false;
      }
      b_select.emplace_back(
          "__pagg_" + std::to_string(agg_seq++),
          AggregateExpressionExp(slot.agg_type, std::move(*sub_arg), false));
    }

    auto pre = std::make_shared<SelectStatement>();
    pre->SetSources(std::move(merged_sources));
    pre->SetWhereClause(
        b_where_parts.empty() ? nullptr : CombineConjuncts(b_where_parts));
    pre->SetGroupBy(std::move(b_group));
    pre->SetSelectList(std::move(b_select));
    pre_branches.push_back(std::move(pre));
  }

  auto union_subq = std::make_shared<SelectStatement>(*pre_branches[0]);
  for (size_t b = 1; b < pre_branches.size(); ++b) {
    union_subq->AddUnionAll(pre_branches[b]);
  }

  std::vector<Expression> new_outer_group;
  new_outer_group.reserve(qual_group_keys.size());
  for (size_t g = 0; g < qual_group_keys.size(); ++g) {
    new_outer_group.push_back(ColumnValueExp(
        ColumnName("__tpcds_upre", "__gk_" + std::to_string(g))));
  }
  std::vector<NamedExpression> new_outer_select;
  size_t agg_seq = 0;
  for (const OuterSelectSlot& slot : select_slots) {
    if (!slot.is_agg) {
      new_outer_select.emplace_back(
          slot.out_name,
          ColumnValueExp(ColumnName("__tpcds_upre",
                                    "__gk_" + std::to_string(slot.key_idx))));
    } else {
      const AggregationType outer_op = slot.agg_type == AggregationType::kCount
                                           ? AggregationType::kSum
                                           : slot.agg_type;
      new_outer_select.emplace_back(
          slot.out_name,
          AggregateExpressionExp(
              outer_op,
              ColumnValueExp(ColumnName("__tpcds_upre",
                                        "__pagg_" + std::to_string(agg_seq++))),
              false));
    }
  }

  stmt->SetSources({SelectSource("", "__tpcds_upre", std::move(union_subq),
                                 JoinType::kCross, nullptr)});
  stmt->SetWhereClause(nullptr);
  stmt->SetGroupBy(std::move(new_outer_group));
  stmt->SetSelectList(std::move(new_outer_select));
  stmt->SetOrderBy(std::move(new_outer_order));
  return true;
}
}  // namespace

}  // namespace select_rewrite_detail

// Lean proof: lean/TinylambOpt/TpcdsDiscovery.lean
//   (zero_padded_union_branch_preagg_sound,
//    zero_padded_union_naive_null_bug,
//    zero_padded_union_naive_coalesce_bug)
bool DistributeZeroPaddedUnionGroupBy(SelectStatement* select,
                                      TransactionContext& ctx) {
  if (select == nullptr) {
    return false;
  }
  bool changed = false;
  for (const auto& [name, body] : select->WithQueries()) {
    if (select->IsRecursiveWith(name) || body == nullptr) {
      continue;
    }
    changed |= DistributeOneUnionGroupBy(body.get(), ctx);
  }
  changed |= DistributeOneUnionGroupBy(select, ctx);
  return changed;
}

// Lean proof: lean/TinylambOpt/TpcdsDiscovery.lean
//   (union_cte_disjunctive_pushdown_and_branch_prune_sound,
//    grouped_self_join_conditional_pivot_sound,
//    self_join_pivot_null_key_bug,
//    self_join_pivot_missing_year_bug)
bool SpecializeAndPivotUnionCtes(SelectStatement* outer,
                                 TransactionContext& ctx) {
  if (outer == nullptr || outer->WithQueries().empty() ||
      !outer->UnionAll().empty() || outer->GetSetOperationTree() != nullptr ||
      SelectListHasStarOrModifiers(*outer) || TpcdsStmtHasParam(*outer)) {
    return false;
  }
  bool any_changed = false;

  // Step 3a: Split tag-discriminated UNION ALL CTEs into per-branch CTEs.
  std::vector<std::string> cte_names = outer->WithQueryOrder();
  for (const std::string& cte_name : cte_names) {
    if (outer->IsRecursiveWith(cte_name)) {
      continue;
    }
    const auto cte_it = outer->WithQueries().find(cte_name);
    if (cte_it == outer->WithQueries().end() || cte_it->second == nullptr) {
      continue;
    }
    std::unordered_set<std::string> single_set{cte_name};
    std::unordered_map<std::string, size_t> ref_counts;
    CountCteReferencesInStatement(*outer, single_set, &ref_counts);
    const size_t total_refs = ref_counts[cte_name];
    if (total_refs < 2) {
      continue;
    }

    std::vector<size_t> site_indices;
    bool all_top_inner = true;
    std::unordered_set<std::string> seen_aliases;
    for (size_t i = 0; i < outer->Sources().size(); ++i) {
      const SelectSource& s = outer->Sources()[i];
      if (s.table == cte_name) {
        const std::string alias = s.alias.empty() ? s.table : s.alias;
        if (s.query != nullptr || s.is_lateral || s.unnest || s.sample ||
            s.from_nested_join || !s.using_columns.empty() ||
            (s.join_type != JoinType::kCross &&
             s.join_type != JoinType::kInner) ||
            !seen_aliases.insert(LoweredStarName(alias)).second) {
          all_top_inner = false;
          break;
        }
        if (alias != cte_name && StatementQualifiesName(*outer, cte_name)) {
          all_top_inner = false;
          break;
        }
        site_indices.push_back(i);
      }
    }
    if (!all_top_inner || site_indices.size() != total_refs) {
      continue;
    }

    std::vector<std::shared_ptr<SelectStatement>> branches;
    if (!ExtractPureUnionAllBranches(*cte_it->second, &branches)) {
      continue;
    }
    if (relational_detail::StatementContainsVolatileFunction(*cte_it->second)) {
      continue;
    }
    const size_t arity = branches[0]->SelectList().size();
    if (arity == 0 || SelectListHasStarOrModifiers(*branches[0])) {
      continue;
    }
    bool arity_ok = true;
    for (size_t b = 1; b < branches.size(); ++b) {
      if (branches[b]->SelectList().size() != arity ||
          SelectListHasStarOrModifiers(*branches[b])) {
        arity_ok = false;
        break;
      }
    }
    if (!arity_ok) {
      continue;
    }

    std::vector<Expression> outer_conjuncts =
        SplitConjuncts(outer->WhereClause());
    for (size_t si : site_indices) {
      if (outer->Sources()[si].join_condition) {
        for (const Expression& c :
             SplitConjuncts(outer->Sources()[si].join_condition)) {
          outer_conjuncts.push_back(c);
        }
      }
    }

    std::optional<size_t> matched_tag_col;
    std::vector<size_t> site_branch_map(site_indices.size(), 0);
    for (size_t col_idx = 0; col_idx < arity; ++col_idx) {
      const std::string& tag_name = branches[0]->SelectList()[col_idx].name;
      if (tag_name.empty()) {
        continue;
      }
      std::vector<Value> branch_tags;
      bool const_ok = true;
      for (const auto& br : branches) {
        std::optional<Value> v =
            TpcdsEvalClosedConstant(br->SelectList()[col_idx].expression);
        if (!v.has_value() || !IsSafeDisjointTagValue(*v) ||
            (!branch_tags.empty() && v->type != branch_tags.front().type)) {
          const_ok = false;
          break;
        }
        branch_tags.push_back(std::move(*v));
      }
      if (!const_ok) {
        continue;
      }
      bool distinct = true;
      for (size_t a = 0; a < branch_tags.size() && distinct; ++a) {
        for (size_t b = a + 1; b < branch_tags.size(); ++b) {
          if (branch_tags[a] == branch_tags[b]) {
            distinct = false;
            break;
          }
        }
      }
      if (!distinct) {
        continue;
      }

      bool all_sites_pinned = true;
      for (size_t s = 0; s < site_indices.size(); ++s) {
        const SelectSource& src = outer->Sources()[site_indices[s]];
        const std::string salias = src.alias.empty() ? src.table : src.alias;
        std::optional<size_t> found_b;
        for (const Expression& conj : outer_conjuncts) {
          if (!conj || conj->Type() != TypeTag::kBinaryExp ||
              conj->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
            continue;
          }
          const auto& bin = conj->AsBinaryExpression();
          auto check_side =
              [&](const Expression& col_side,
                  const Expression& val_side) -> std::optional<Value> {
            if (!col_side || col_side->Type() != TypeTag::kColumnValue) {
              return std::nullopt;
            }
            const ColumnName& cn = col_side->AsColumnValue().GetColumnName();
            if (!IdentifierEquals(cn.schema, salias) ||
                !IdentifierEquals(cn.name, tag_name)) {
              return std::nullopt;
            }
            return TpcdsEvalClosedConstant(val_side);
          };
          std::optional<Value> sv = check_side(bin.Left(), bin.Right());
          if (!sv.has_value()) {
            sv = check_side(bin.Right(), bin.Left());
          }
          if (!sv.has_value()) {
            continue;
          }
          for (size_t b = 0; b < branch_tags.size(); ++b) {
            if (*sv == branch_tags[b]) {
              found_b = b;
              break;
            }
          }
          if (found_b.has_value()) {
            break;
          }
        }
        if (!found_b.has_value()) {
          all_sites_pinned = false;
          break;
        }
        site_branch_map[s] = *found_b;
      }
      if (all_sites_pinned) {
        matched_tag_col = col_idx;
        break;
      }
    }
    if (!matched_tag_col.has_value()) {
      continue;
    }

    std::vector<SelectSource> new_sources = outer->Sources();
    std::unordered_map<size_t, std::string> created_branch_ctes;
    for (size_t s = 0; s < site_indices.size(); ++s) {
      const size_t b = site_branch_map[s];
      auto it = created_branch_ctes.find(b);
      if (it == created_branch_ctes.end()) {
        auto specialized = std::make_shared<SelectStatement>(*branches[b]);
        std::vector<NamedExpression> aligned = specialized->SelectList();
        for (size_t c = 0; c < arity; ++c) {
          aligned[c].name = branches[0]->SelectList()[c].name;
        }
        specialized->SetSelectList(std::move(aligned));
        std::string b_name =
            "__tpcds_uch_" + cte_name + "_" + std::to_string(b);
        // Generated names must not collide with a user-declared CTE:
        // AddWithQuery would keep the user's body while redirecting the site
        // to this name (and push a duplicate into with_query_order_).
        for (size_t suffix = 0; outer->WithQueries().contains(b_name);
             ++suffix) {
          b_name = "__tpcds_uch_" + cte_name + "_" + std::to_string(b) + "_" +
                   std::to_string(suffix);
        }
        outer->AddWithQuery(b_name, std::move(specialized));
        it = created_branch_ctes.emplace(b, std::move(b_name)).first;
      }
      SelectSource& target_src = new_sources[site_indices[s]];
      if (target_src.alias.empty()) {
        target_src.alias = cte_name;
      }
      target_src.table = it->second;
    }
    outer->SetSources(std::move(new_sources));
    outer->RemoveWithQuery(cte_name);
    any_changed = true;
  }

  // Run FD GROUP BY reduction on non-recursive CTE bodies (reduces Q4/Q11/Q74
  // 8-column customer GROUP BY down to `(c_customer_id, d_year)`).
  for (const auto& [name, body] : outer->WithQueries()) {
    if (!outer->IsRecursiveWith(name) && body != nullptr) {
      any_changed |= ReduceGroupByFunctionalDependencies(body.get(), ctx);
    }
  }

  // Step 3b & 3c: Push disjunctive group-key bounds into shared grouped CTEs,
  // and pivot multi-year self-joins into single-pass conditional aggregates.
  std::vector<std::string> post_cte_names = outer->WithQueryOrder();
  for (const std::string& cte_name : post_cte_names) {
    if (outer->IsRecursiveWith(cte_name)) {
      continue;
    }
    const auto cte_it = outer->WithQueries().find(cte_name);
    if (cte_it == outer->WithQueries().end() || cte_it->second == nullptr) {
      continue;
    }
    SelectStatement& body = *cte_it->second;
    if (body.GroupBy().size() < 2 || body.IsGroupAll() || body.Having() ||
        body.Distinct() || body.HasDistinctOn() || body.HasLimit() ||
        body.Offset() != 0 || body.Qualify() || !body.WithQueries().empty() ||
        !body.UnionAll().empty() || body.GetSetOperationTree() != nullptr ||
        relational_detail::HasWindowFunctions(body) ||
        relational_detail::StatementContainsVolatileFunction(body) ||
        SelectListHasStarOrModifiers(body)) {
      continue;
    }

    std::unordered_set<std::string> single_set{cte_name};
    std::unordered_map<std::string, size_t> ref_counts;
    CountCteReferencesInStatement(*outer, single_set, &ref_counts);
    const size_t total_refs = ref_counts[cte_name];
    if (total_refs < 2) {
      continue;
    }

    // Require all outer sources to be plain inner/cross joins so site WHERE
    // predicates and conditional-aggregate pivots preserve join multiplicity.
    bool outer_all_inner = true;
    std::vector<size_t> site_indices;
    std::unordered_set<std::string> site_aliases_lower;
    for (size_t i = 0; i < outer->Sources().size(); ++i) {
      const SelectSource& s = outer->Sources()[i];
      if (s.join_type != JoinType::kCross && s.join_type != JoinType::kInner) {
        outer_all_inner = false;
        break;
      }
      if (s.table == cte_name) {
        const std::string alias = s.alias.empty() ? s.table : s.alias;
        if (s.query != nullptr || s.is_lateral || s.unnest || s.sample ||
            s.from_nested_join || !s.using_columns.empty() ||
            !site_aliases_lower.insert(LoweredStarName(alias)).second ||
            (alias != cte_name && StatementQualifiesName(*outer, cte_name))) {
          outer_all_inner = false;
          break;
        }
        site_indices.push_back(i);
      }
    }
    if (!outer_all_inner || site_indices.size() != total_refs) {
      continue;
    }

    // Resolve body relations and certified PK relations.
    // Lean proof:
    // lean/TinylambOpt/ResearchProofs/Wave9TpcdsMultiChannelAndTpch.lean
    //   (YearPairPivotSelfJoinElim_sound)
    // Also supports a single derived-table source (e.g. TPC-DS Q75 `all_sales`
    // over a UNION DISTINCT `sales_detail` subquery) so the discriminator
    // predicate pushes into every UNION branch and the outer year-pair self-
    // join collapses into a single grouped scan.
    std::vector<std::shared_ptr<Table>> body_holders;
    body_holders.reserve(body.Sources().size());
    std::vector<Schema> derived_schemas;
    std::vector<GroupingRelation> body_rels;
    body_rels.reserve(body.Sources().size());
    bool body_rels_ok = true;
    bool body_is_single_derived = false;
    if (body.Sources().size() == 1 && body.Sources()[0].query != nullptr &&
        !body.Sources()[0].is_lateral && !body.Sources()[0].unnest &&
        !body.Sources()[0].sample && !body.Sources()[0].from_nested_join &&
        body.Sources()[0].using_columns.empty() &&
        (body.Sources()[0].join_type == JoinType::kCross ||
         body.Sources()[0].join_type == JoinType::kInner)) {
      const SelectSource& bs = body.Sources()[0];
      const SelectStatement& dq = *bs.query;
      std::vector<Column> dcols;
      dcols.reserve(dq.SelectList().size());
      std::unordered_set<std::string> seen_dcols;
      bool dcols_ok = !dq.SelectList().empty();
      for (const NamedExpression& item : dq.SelectList()) {
        std::string pub = item.name;
        if (pub.empty() && item.expression &&
            item.expression->Type() == TypeTag::kColumnValue) {
          pub = item.expression->AsColumnValue().GetColumnName().name;
        }
        if (pub.empty() || !item.expression ||
            (item.expression->Type() == TypeTag::kColumnValue &&
             item.expression->AsColumnValue().GetColumnName().name == "*") ||
            !seen_dcols.insert(LoweredStarName(pub)).second) {
          dcols_ok = false;
          break;
        }
        dcols.emplace_back(std::move(pub), ValueType::kInt64);
      }
      if (dcols_ok) {
        const std::string dname = bs.alias.empty() ? "__derived" : bs.alias;
        derived_schemas.emplace_back(dname, std::move(dcols));
        body_rels.push_back({.name = dname, .schema = &derived_schemas.back()});
        body_is_single_derived = true;
      } else {
        body_rels_ok = false;
      }
    } else {
      for (const SelectSource& bs : body.Sources()) {
        if (bs.table.empty() || bs.query != nullptr || bs.is_lateral ||
            bs.unnest || bs.sample || bs.from_nested_join ||
            !bs.using_columns.empty() ||
            (bs.join_type != JoinType::kCross &&
             bs.join_type != JoinType::kInner)) {
          body_rels_ok = false;
          break;
        }
        StatusOr<std::shared_ptr<Table>> btbl = ctx.GetTable(bs.table);
        if (!btbl.HasValue()) {
          body_rels_ok = false;
          break;
        }
        body_holders.push_back(btbl.MoveValue());
        body_rels.push_back({.name = bs.alias.empty() ? bs.table : bs.alias,
                             .schema = &body_holders.back()->GetSchema()});
      }
    }
    if (!body_rels_ok || body_rels.empty()) {
      continue;
    }

    struct BodyGroupKey {
      Expression expr;
      GroupingRelation rel;
      std::string col_name;
      std::string out_name;
    };
    std::vector<BodyGroupKey> gkeys;
    bool gkeys_ok = true;
    for (const Expression& gk : body.GroupBy()) {
      if (!gk || gk->Type() != TypeTag::kColumnValue) {
        gkeys_ok = false;
        break;
      }
      const ColumnName& gc = gk->AsColumnValue().GetColumnName();
      GroupingRelation grel;
      if (!ResolveGroupingColumn(body_rels, gc, &grel)) {
        gkeys_ok = false;
        break;
      }
      std::string matched_out;
      for (const NamedExpression& item : body.SelectList()) {
        if (!item.expression ||
            item.expression->Type() != TypeTag::kColumnValue) {
          continue;
        }
        GroupingRelation irel;
        const ColumnName& ic = item.expression->AsColumnValue().GetColumnName();
        const std::string pub = item.name.empty() ? ic.name : item.name;
        if (!pub.empty() && ResolveGroupingColumn(body_rels, ic, &irel) &&
            irel.name == grel.name && IdentifierEquals(ic.name, gc.name)) {
          matched_out = pub;
          break;
        }
      }
      if (matched_out.empty()) {
        gkeys_ok = false;
        break;
      }
      gkeys.push_back({.expr = gk,
                       .rel = std::move(grel),
                       .col_name = gc.name,
                       .out_name = std::move(matched_out)});
    }
    if (!gkeys_ok) {
      continue;
    }

    std::vector<Expression> outer_conjuncts =
        SplitConjuncts(outer->WhereClause());
    for (size_t si : site_indices) {
      if (outer->Sources()[si].join_condition) {
        for (const Expression& c :
             SplitConjuncts(outer->Sources()[si].join_condition)) {
          outer_conjuncts.push_back(c);
        }
      }
    }

    // Look for a group-key column `disc_idx` pinned to a closed constant at
    // every consumer site.
    std::optional<size_t> disc_idx;
    std::vector<Value> site_disc_vals(site_indices.size());
    for (size_t k = 0; k < gkeys.size(); ++k) {
      bool all_pinned = true;
      std::vector<Value> vals;
      vals.reserve(site_indices.size());
      for (unsigned long site_indice : site_indices) {
        const SelectSource& src = outer->Sources()[site_indice];
        const std::string salias = src.alias.empty() ? src.table : src.alias;
        std::optional<Value> pinned;
        for (const Expression& conj : outer_conjuncts) {
          if (!conj || conj->Type() != TypeTag::kBinaryExp ||
              conj->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
            continue;
          }
          const auto& bin = conj->AsBinaryExpression();
          auto extract_const =
              [&](const Expression& cside,
                  const Expression& vside) -> std::optional<Value> {
            if (!cside || cside->Type() != TypeTag::kColumnValue) {
              return std::nullopt;
            }
            const ColumnName& cn = cside->AsColumnValue().GetColumnName();
            if (!IdentifierEquals(cn.schema, salias) ||
                !IdentifierEquals(cn.name, gkeys[k].out_name)) {
              return std::nullopt;
            }
            return TpcdsEvalClosedConstant(vside);
          };
          std::optional<Value> cv = extract_const(bin.Left(), bin.Right());
          if (!cv.has_value()) {
            cv = extract_const(bin.Right(), bin.Left());
          }
          if (cv.has_value() && IsSafeDisjointTagValue(*cv)) {
            pinned = std::move(*cv);
            break;
          }
        }
        if (!pinned.has_value() ||
            (!vals.empty() && pinned->type != vals.front().type)) {
          all_pinned = false;
          break;
        }
        vals.push_back(std::move(*pinned));
      }
      if (all_pinned) {
        disc_idx = k;
        site_disc_vals = std::move(vals);
        break;
      }
    }
    if (!disc_idx.has_value()) {
      continue;
    }

    // Step 3b: Push disjunctive filter `(disc = v_0 OR ... OR disc = v_{r-1})`
    // into `body.WhereClause()` (sound even if Step 3c below declines!).
    std::vector<Value> unique_disc_vals;
    unique_disc_vals.reserve(site_disc_vals.size());
    for (const Value& v : site_disc_vals) {
      if (!std::ranges::any_of(unique_disc_vals,
                               [&](const Value& u) { return u == v; })) {
        unique_disc_vals.push_back(v);
      }
    }
    Expression disc_disjunction;
    for (const Value& uv : unique_disc_vals) {
      Expression eq =
          BinaryExpressionExp(gkeys[*disc_idx].expr, BinaryOperation::kEquals,
                              ConstantValueExp(uv));
      disc_disjunction =
          disc_disjunction
              ? BinaryExpressionExp(disc_disjunction, BinaryOperation::kOr,
                                    std::move(eq))
              : std::move(eq);
    }
    if (disc_disjunction) {
      Expression new_bw =
          body.WhereClause()
              ? BinaryExpressionExp(body.WhereClause(), BinaryOperation::kAnd,
                                    disc_disjunction)
              : disc_disjunction;
      body.SetWhereClause(std::move(new_bw));
      any_changed = true;
      // When the CTE body aggregates over a single derived SELECT or UNION
      // [ALL|DISTINCT] subquery (e.g. TPC-DS Q75 `all_sales` over
      // `sales_detail`), push the discriminator disjunction directly into every
      // branch WHERE clause so all base-table scans and UNION DISTINCT drop
      // non-target years upfront.
      // Lean proof:
      // lean/TinylambOpt/ResearchProofs/Wave9TpcdsMultiChannelAndTpch.lean
      //   (YearPairPivotSelfJoinElim_sound)
      if (body_is_single_derived && !body.Sources().empty() &&
          body.Sources()[0].query != nullptr) {
        SelectStatement* dq = body.Sources()[0].query.get();
        std::optional<size_t> disc_pos;
        for (size_t idx = 0; idx < dq->SelectList().size(); ++idx) {
          const NamedExpression& ditem = dq->SelectList()[idx];
          std::string pub = ditem.name;
          if (pub.empty() && ditem.expression &&
              ditem.expression->Type() == TypeTag::kColumnValue) {
            pub = ditem.expression->AsColumnValue().GetColumnName().name;
          }
          if (IdentifierEquals(pub, gkeys[*disc_idx].col_name)) {
            disc_pos = idx;
            break;
          }
        }
        bool push_ok = disc_pos.has_value() && !dq->HasLimit() &&
                       dq->Offset() == 0 && dq->OrderBy().empty() &&
                       !dq->Qualify() && dq->WithQueries().empty();
        for (size_t b = 0; push_ok && b < dq->UnionAll().size(); ++b) {
          const SetOperationKind kind = b < dq->SetOperationKinds().size()
                                            ? dq->SetOperationKinds()[b]
                                            : SetOperationKind::kUnionAll;
          const SetOperationMatch match = dq->MatchFor(b);
          if ((kind != SetOperationKind::kUnionAll &&
               kind != SetOperationKind::kUnion) ||
              match.corresponding || match.by_name || !match.columns.empty() ||
              dq->UnionAll()[b] == nullptr) {
            push_ok = false;
          }
        }
        if (push_ok) {
          std::vector<SelectStatement*> branches;
          std::unordered_set<SelectStatement*> seen_branches;
          auto add_branch = [&](SelectStatement* br) {
            if (br != nullptr && seen_branches.insert(br).second) {
              branches.push_back(br);
            }
          };
          add_branch(dq);
          for (const auto& br : dq->UnionAll()) {
            add_branch(br.get());
          }
          if (const auto& tree = dq->GetSetOperationTree(); tree != nullptr) {
            for (const SetOperationKind kind : tree->kinds) {
              if (kind != SetOperationKind::kUnionAll &&
                  kind != SetOperationKind::kUnion) {
                push_ok = false;
                break;
              }
            }
            if (push_ok) {
              add_branch(tree->first.get());
              for (const auto& br : tree->branches) {
                add_branch(br.get());
              }
            }
          }
          for (const SelectStatement* br : branches) {
            if (!br->GroupBy().empty() || br->Having() || br->HasLimit() ||
                br->Offset() != 0 || br->Qualify() ||
                !br->WithQueries().empty() ||
                *disc_pos >= br->SelectList().size() ||
                !br->SelectList()[*disc_pos].expression ||
                br->SelectList()[*disc_pos].expression->Type() !=
                    TypeTag::kColumnValue) {
              push_ok = false;
              break;
            }
          }
          if (push_ok) {
            for (SelectStatement* br : branches) {
              const Expression& b_disc = br->SelectList()[*disc_pos].expression;
              Expression b_disj;
              for (const Value& uv : unique_disc_vals) {
                Expression eq = BinaryExpressionExp(
                    b_disc, BinaryOperation::kEquals, ConstantValueExp(uv));
                b_disj = b_disj ? BinaryExpressionExp(std::move(b_disj),
                                                      BinaryOperation::kOr,
                                                      std::move(eq))
                                : std::move(eq);
              }
              if (b_disj) {
                Expression bw = br->WhereClause()
                                    ? BinaryExpressionExp(br->WhereClause(),
                                                          BinaryOperation::kAnd,
                                                          std::move(b_disj))
                                    : std::move(b_disj);
                br->SetWhereClause(std::move(bw));
              }
            }
          }
        }
      }
    }

    // Step 3c: Check if all `r` sites have pairwise-distinct `site_disc_vals`
    // and equate all remaining group keys `g != *disc_idx` across all sites.
    if (unique_disc_vals.size() != site_indices.size()) {
      continue;
    }

    // Build transitive column-equality graph over `outer_conjuncts`.
    std::unordered_map<std::string, std::vector<std::string>> eq_adj;
    auto col_token = [](std::string_view schema, std::string_view col) {
      return LoweredStarName(schema) + "." + LoweredStarName(col);
    };
    for (const Expression& conj : outer_conjuncts) {
      if (!conj || conj->Type() != TypeTag::kBinaryExp ||
          conj->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
        continue;
      }
      const auto& bin = conj->AsBinaryExpression();
      if (!bin.Left() || !bin.Right() ||
          bin.Left()->Type() != TypeTag::kColumnValue ||
          bin.Right()->Type() != TypeTag::kColumnValue) {
        continue;
      }
      const ColumnName& lc = bin.Left()->AsColumnValue().GetColumnName();
      const ColumnName& rc = bin.Right()->AsColumnValue().GetColumnName();
      if (lc.schema.empty() || rc.schema.empty()) {
        continue;
      }
      const std::string lt = col_token(lc.schema, lc.name);
      const std::string rt = col_token(rc.schema, rc.name);
      eq_adj[lt].push_back(rt);
      eq_adj[rt].push_back(lt);
    }
    auto are_equated = [&](const std::string& a, const std::string& b) {
      if (a == b) {
        return true;
      }
      std::unordered_set<std::string> visited{a};
      std::vector<std::string> queue{a};
      while (!queue.empty()) {
        std::string cur = std::move(queue.back());
        queue.pop_back();
        const auto it = eq_adj.find(cur);
        if (it == eq_adj.end()) {
          continue;
        }
        for (const std::string& nxt : it->second) {
          if (nxt == b) {
            return true;
          }
          if (visited.insert(nxt).second) {
            queue.push_back(nxt);
          }
        }
      }
      return false;
    };

    const std::string rep_alias =
        outer->Sources()[site_indices[0]].alias.empty()
            ? outer->Sources()[site_indices[0]].table
            : outer->Sources()[site_indices[0]].alias;
    // Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
    // (bijective_dim_key_cte_deferral_sound)
    auto find_body_table_name =
        [&](const std::string& rel_name) -> std::string {
      for (const SelectSource& bs : body.Sources()) {
        const std::string bal = bs.alias.empty() ? bs.table : bs.alias;
        if (IdentifierEquals(bal, rel_name)) {
          return bs.table;
        }
      }
      return {};
    };
    std::vector<bool> key_equated_across_sites(gkeys.size(), false);
    std::unordered_set<std::string> certified_shared_rels;
    for (size_t k = 0; k < gkeys.size(); ++k) {
      if (k == *disc_idx) {
        continue;
      }
      const std::string rep_tok = col_token(rep_alias, gkeys[k].out_name);
      bool eq_all = true;
      for (size_t s = 1; s < site_indices.size(); ++s) {
        const SelectSource& src = outer->Sources()[site_indices[s]];
        const std::string salias = src.alias.empty() ? src.table : src.alias;
        if (!are_equated(rep_tok, col_token(salias, gkeys[k].out_name))) {
          eq_all = false;
          break;
        }
      }
      key_equated_across_sites[k] = eq_all;
      if (eq_all) {
        if (ColumnIsPrimaryKey(gkeys[k].rel, gkeys[k].col_name) ||
            InspectRuntimeUniqueNonNullColumn(
                ctx, find_body_table_name(gkeys[k].rel.name), gkeys[k].col_name)
                .unique_non_null) {
          certified_shared_rels.insert(gkeys[k].rel.name);
        }
      }
    }
    bool all_shared_keys_equated = true;
    for (size_t k = 0; k < gkeys.size(); ++k) {
      if (k == *disc_idx) {
        continue;
      }
      if (!key_equated_across_sites[k] &&
          !certified_shared_rels.contains(gkeys[k].rel.name)) {
        all_shared_keys_equated = false;
        break;
      }
    }
    if (!all_shared_keys_equated) {
      continue;
    }

    // Classify every item in `body.SelectList()`.
    enum class ItemKind : uint8_t { kSharedOrFd, kDiscKey, kConst, kAgg };
    struct BodyItemSpec {
      std::string out_name;
      ItemKind kind{ItemKind::kSharedOrFd};
      Expression expr;
      AggregationType agg_type{AggregationType::kSum};
      Expression agg_arg;
    };
    std::vector<BodyItemSpec> item_specs;
    item_specs.reserve(body.SelectList().size());
    bool items_ok = true;
    std::unordered_set<std::string> seen_body_out;
    for (const NamedExpression& item : body.SelectList()) {
      std::string pub = item.name;
      if (pub.empty() && item.expression &&
          item.expression->Type() == TypeTag::kColumnValue) {
        pub = item.expression->AsColumnValue().GetColumnName().name;
      }
      if (pub.empty() || !item.expression ||
          !seen_body_out.insert(LoweredStarName(pub)).second) {
        items_ok = false;
        break;
      }
      BodyItemSpec spec;
      spec.out_name = std::move(pub);
      spec.expr = item.expression;
      if (std::optional<Value> cv = TpcdsEvalClosedConstant(item.expression);
          cv.has_value()) {
        spec.kind = ItemKind::kConst;
        spec.expr = ConstantValueExp(*cv);
        item_specs.push_back(std::move(spec));
        continue;
      }
      if (item.expression->Type() == TypeTag::kColumnValue) {
        const ColumnName& ic = item.expression->AsColumnValue().GetColumnName();
        GroupingRelation irel;
        if (!ResolveGroupingColumn(body_rels, ic, &irel)) {
          items_ok = false;
          break;
        }
        if (irel.name == gkeys[*disc_idx].rel.name &&
            IdentifierEquals(ic.name, gkeys[*disc_idx].col_name)) {
          spec.kind = ItemKind::kDiscKey;
          item_specs.push_back(std::move(spec));
          continue;
        }
        bool is_shared_or_fd = certified_shared_rels.contains(irel.name);
        for (size_t k = 0; k < gkeys.size() && !is_shared_or_fd; ++k) {
          if (k != *disc_idx && irel.name == gkeys[k].rel.name &&
              IdentifierEquals(ic.name, gkeys[k].col_name)) {
            is_shared_or_fd = true;
          }
        }
        if (!is_shared_or_fd) {
          items_ok = false;
          break;
        }
        spec.kind = ItemKind::kSharedOrFd;
        item_specs.push_back(std::move(spec));
        continue;
      }
      if (item.expression->Type() == TypeTag::kAggregateExp) {
        const auto& agg = item.expression->AsAggregateExpression();
        if (agg.Distinct() || agg.NeedsGroupContext() || agg.HasWhereFilter() ||
            !agg.InnerGroupBy().empty() || !agg.InnerOrderBy().empty() ||
            !agg.Child() || ContainsQueryExpression(agg.Child()) ||
            !DerivedExpressionIsImmutable(agg.Child())) {
          items_ok = false;
          break;
        }
        if (agg.GetType() != AggregationType::kSum &&
            agg.GetType() != AggregationType::kCount &&
            agg.GetType() != AggregationType::kMin &&
            agg.GetType() != AggregationType::kMax) {
          items_ok = false;
          break;
        }
        spec.kind = ItemKind::kAgg;
        spec.agg_type = agg.GetType();
        spec.agg_arg = agg.Child();
        item_specs.push_back(std::move(spec));
        continue;
      }
      items_ok = false;
      break;
    }
    if (!items_ok) {
      continue;
    }

    // Build replacement map `(lowered_site_alias, lowered_col_name) ->
    // Expression`.
    std::vector<NamedExpression> piv_select;
    piv_select.reserve(item_specs.size());
    std::unordered_map<std::string, Expression> site_col_map;
    for (size_t idx = 0; idx < item_specs.size(); ++idx) {
      const BodyItemSpec& spec = item_specs[idx];
      if (spec.kind == ItemKind::kSharedOrFd) {
        piv_select.emplace_back(spec.out_name, spec.expr);
        for (unsigned long site_indice : site_indices) {
          const SelectSource& src = outer->Sources()[site_indice];
          const std::string salias = src.alias.empty() ? src.table : src.alias;
          site_col_map.emplace(
              col_token(salias, spec.out_name),
              ColumnValueExp(ColumnName(rep_alias, spec.out_name)));
        }
      } else if (spec.kind == ItemKind::kConst) {
        for (unsigned long site_indice : site_indices) {
          const SelectSource& src = outer->Sources()[site_indice];
          const std::string salias = src.alias.empty() ? src.table : src.alias;
          site_col_map.emplace(col_token(salias, spec.out_name), spec.expr);
        }
      } else if (spec.kind == ItemKind::kDiscKey) {
        for (size_t s = 0; s < site_indices.size(); ++s) {
          const SelectSource& src = outer->Sources()[site_indices[s]];
          const std::string salias = src.alias.empty() ? src.table : src.alias;
          site_col_map.emplace(col_token(salias, spec.out_name),
                               ConstantValueExp(site_disc_vals[s]));
        }
      } else if (spec.kind == ItemKind::kAgg) {
        for (size_t s = 0; s < site_indices.size(); ++s) {
          const SelectSource& src = outer->Sources()[site_indices[s]];
          const std::string salias = src.alias.empty() ? src.table : src.alias;
          const std::string cname =
              "__tpcds_piv_" + std::to_string(s) + "_" + std::to_string(idx);
          Expression cond = BinaryExpressionExp(
              gkeys[*disc_idx].expr, BinaryOperation::kEquals,
              ConstantValueExp(site_disc_vals[s]));
          Expression branch_val =
              (spec.agg_arg->Type() == TypeTag::kColumnValue &&
               spec.agg_arg->AsColumnValue().GetColumnName().name == "*")
                  ? ConstantValueExp(Value(static_cast<int64_t>(1)))
                  : spec.agg_arg;
          piv_select.emplace_back(
              cname,
              AggregateExpressionExp(
                  spec.agg_type,
                  CaseExpressionExp({{std::move(cond), std::move(branch_val)}},
                                    nullptr),
                  false));
          site_col_map.emplace(col_token(salias, spec.out_name),
                               ColumnValueExp(ColumnName(rep_alias, cname)));
        }
      }
    }
    for (size_t s = 0; s < site_indices.size(); ++s) {
      const std::string cnt_name = "__tpcds_yr_cnt_" + std::to_string(s);
      Expression cond =
          BinaryExpressionExp(gkeys[*disc_idx].expr, BinaryOperation::kEquals,
                              ConstantValueExp(site_disc_vals[s]));
      piv_select.emplace_back(
          cnt_name,
          AggregateExpressionExp(
              AggregationType::kCount,
              CaseExpressionExp(
                  {{std::move(cond),
                    ConstantValueExp(Value(static_cast<int64_t>(1)))}},
                  nullptr),
              false));
    }

    auto rebind_outer_expr =
        [&](auto& self, const Expression& expr) -> std::optional<Expression> {
      if (!expr) {
        return expr;
      }
      if (expr->Type() == TypeTag::kQueryExp) {
        return std::nullopt;
      }
      if (expr->Type() == TypeTag::kColumnValue) {
        const ColumnName& cn = expr->AsColumnValue().GetColumnName();
        if (cn.name == "*") {
          return std::nullopt;
        }
        if (cn.schema.empty()) {
          if (seen_body_out.contains(LoweredStarName(cn.name))) {
            return std::nullopt;
          }
          return expr;
        }
        if (site_aliases_lower.contains(LoweredStarName(cn.schema))) {
          const auto found = site_col_map.find(col_token(cn.schema, cn.name));
          if (found == site_col_map.end()) {
            return std::nullopt;
          }
          return found->second;
        }
        return expr;
      }
      std::vector<Expression> kids = ExpressionChildren(expr);
      for (Expression& k : kids) {
        std::optional<Expression> rk = self(self, k);
        if (!rk.has_value()) {
          return std::nullopt;
        }
        k = std::move(*rk);
      }
      return WithExpressionChildren(expr, std::move(kids));
    };

    std::vector<NamedExpression> new_outer_sel;
    bool rebind_ok = true;
    for (size_t i = 0; i < outer->SelectList().size(); ++i) {
      const NamedExpression& item = outer->SelectList()[i];
      std::string preserved_name = InferSelectItemOutputName(item, i);
      std::optional<Expression> r =
          rebind_outer_expr(rebind_outer_expr, item.expression);
      if (!r.has_value()) {
        rebind_ok = false;
        break;
      }
      new_outer_sel.emplace_back(std::move(preserved_name), std::move(*r));
    }
    std::optional<Expression> new_outer_where =
        rebind_outer_expr(rebind_outer_expr, outer->WhereClause());
    std::optional<Expression> new_outer_having =
        rebind_outer_expr(rebind_outer_expr, outer->Having());
    std::optional<Expression> new_outer_qualify =
        rebind_outer_expr(rebind_outer_expr, outer->Qualify());
    if (!rebind_ok || !new_outer_where.has_value() ||
        !new_outer_having.has_value() || !new_outer_qualify.has_value()) {
      continue;
    }
    std::vector<Expression> new_outer_group;
    for (const Expression& g : outer->GroupBy()) {
      std::optional<Expression> rg = rebind_outer_expr(rebind_outer_expr, g);
      if (!rg.has_value()) {
        rebind_ok = false;
        break;
      }
      new_outer_group.push_back(std::move(*rg));
    }
    std::vector<SelectStatement::OrderByTerm> new_outer_order =
        outer->OrderBy();
    for (auto& term : new_outer_order) {
      std::optional<Expression> ro =
          rebind_outer_expr(rebind_outer_expr, term.expression);
      if (!ro.has_value()) {
        rebind_ok = false;
        break;
      }
      term.expression = std::move(*ro);
    }
    // DISTINCT ON lives in the same scope as the select list and ORDER BY;
    // leaving a site-alias reference there after the sites are merged would
    // dangle. A key that cannot be rebound keeps the statement untouched.
    std::vector<Expression> new_outer_distinct_on;
    new_outer_distinct_on.reserve(outer->DistinctOn().size());
    for (const Expression& key : outer->DistinctOn()) {
      std::optional<Expression> rd = rebind_outer_expr(rebind_outer_expr, key);
      if (!rd.has_value()) {
        rebind_ok = false;
        break;
      }
      new_outer_distinct_on.push_back(std::move(*rd));
    }
    if (!rebind_ok) {
      continue;
    }

    std::string pivcte_name = "__tpcds_pivcte_" + cte_name;
    for (size_t suffix = 0; outer->WithQueries().contains(pivcte_name);
         ++suffix) {
      pivcte_name = "__tpcds_pivcte_" + cte_name + "_" + std::to_string(suffix);
    }

    std::unordered_set<size_t> drop_set(site_indices.begin() + 1,
                                        site_indices.end());
    std::vector<SelectSource> new_outer_sources;
    std::vector<Expression> extra_where;
    for (size_t i = 0; i < outer->Sources().size(); ++i) {
      SelectSource s = outer->Sources()[i];
      if (s.join_condition) {
        std::optional<Expression> rjc =
            rebind_outer_expr(rebind_outer_expr, s.join_condition);
        if (!rjc.has_value()) {
          rebind_ok = false;
          break;
        }
        if (drop_set.contains(i)) {
          extra_where.push_back(std::move(*rjc));
        } else {
          s.join_condition = std::move(*rjc);
        }
      }
      if (drop_set.contains(i)) {
        continue;
      }
      if (i == site_indices[0]) {
        s.alias = rep_alias;
        s.table = pivcte_name;
      }
      new_outer_sources.push_back(std::move(s));
    }
    if (!rebind_ok) {
      continue;
    }

    // Build pivoted CTE body with explicit NOT NULL guards on shared keys
    // (`self_join_pivot_null_key_bug`) and per-year count > 0 guards
    // (`self_join_pivot_missing_year_bug`).
    auto piv_body = std::make_shared<SelectStatement>(body);
    std::vector<Expression> piv_group;
    std::vector<Expression> piv_where = SplitConjuncts(piv_body->WhereClause());
    for (size_t k = 0; k < gkeys.size(); ++k) {
      if (k == *disc_idx) {
        continue;
      }
      piv_group.push_back(gkeys[k].expr);
      if (key_equated_across_sites[k]) {
        piv_where.push_back(
            UnaryExpressionExp(gkeys[k].expr, UnaryOperation::kIsNotNull));
      }
    }
    piv_body->SetWhereClause(CombineConjuncts(piv_where));
    piv_body->SetGroupBy(std::move(piv_group));
    piv_body->SetSelectList(std::move(piv_select));

    std::vector<Expression> final_outer_where;
    if (*new_outer_where) {
      final_outer_where.push_back(std::move(*new_outer_where));
    }
    for (Expression& ew : extra_where) {
      final_outer_where.push_back(std::move(ew));
    }
    for (size_t s = 0; s < site_indices.size(); ++s) {
      final_outer_where.push_back(BinaryExpressionExp(
          ColumnValueExp(
              ColumnName(rep_alias, "__tpcds_yr_cnt_" + std::to_string(s))),
          BinaryOperation::kGreaterThan,
          ConstantValueExp(Value(static_cast<int64_t>(0)))));
    }

    outer->AddWithQuery(pivcte_name, std::move(piv_body));
    outer->RemoveWithQuery(cte_name);
    outer->SetSources(std::move(new_outer_sources));
    outer->SetSelectList(std::move(new_outer_sel));
    outer->SetWhereClause(CombineConjuncts(final_outer_where));
    outer->SetGroupBy(std::move(new_outer_group));
    outer->SetHaving(std::move(*new_outer_having));
    outer->SetQualify(std::move(*new_outer_qualify));
    if (!new_outer_distinct_on.empty()) {
      outer->SetDistinctOn(std::move(new_outer_distinct_on));
    }
    outer->SetOrderBy(std::move(new_outer_order));
    any_changed = true;
  }

  return any_changed;
}

// Lean proof: lean/TinylambOpt/TpcdsDiscovery.lean
//   (full_outer_indicator_inclusion_exclusion_sound,
//    full_outer_indicator_partial_null_key_bug,
//    full_outer_indicator_empty_null_bug)
bool RewriteFullOuterIndicatorSumsToSemiJoin(SelectStatement* select) {
  if (select == nullptr || select->Sources().size() != 2 ||
      select->WhereClause() || !select->GroupBy().empty() || select->Having() ||
      select->Distinct() || select->HasDistinctOn() || select->Qualify() ||
      !select->OrderBy().empty() || !select->UnionAll().empty() ||
      select->GetSetOperationTree() != nullptr || select->ForUpdate() ||
      select->AsStruct() || select->WithTies() ||
      SelectListHasStarOrModifiers(*select) ||
      (select->HasLimit() && (select->Limit() == 0 || select->Offset() != 0))) {
    return false;
  }
  const SelectSource& l_src = select->Sources()[0];
  const SelectSource& r_src = select->Sources()[1];
  if (l_src.join_type != JoinType::kCross ||
      r_src.join_type != JoinType::kFull || !r_src.join_condition ||
      l_src.is_lateral || r_src.is_lateral || l_src.unnest || r_src.unnest ||
      l_src.sample || r_src.sample || l_src.from_nested_join ||
      r_src.from_nested_join || !l_src.using_columns.empty() ||
      !r_src.using_columns.empty() || l_src.table.empty() ||
      r_src.table.empty()) {
    return false;
  }
  const std::string l_alias = l_src.alias.empty() ? l_src.table : l_src.alias;
  const std::string r_alias = r_src.alias.empty() ? r_src.table : r_src.alias;
  if (IdentifierEquals(l_alias, r_alias)) {
    return false;
  }

  const auto l_cte = select->WithQueries().find(l_src.table);
  const auto r_cte = select->WithQueries().find(r_src.table);
  if (l_cte == select->WithQueries().end() ||
      r_cte == select->WithQueries().end() || l_cte->second == nullptr ||
      r_cte->second == nullptr || select->IsRecursiveWith(l_src.table) ||
      select->IsRecursiveWith(r_src.table)) {
    return false;
  }

  // Extract ON equalities `L.lk_j = R.rk_j`.
  std::vector<std::pair<std::string, std::string>> key_pairs;
  for (const Expression& conj : SplitConjuncts(r_src.join_condition)) {
    if (!conj || conj->Type() != TypeTag::kBinaryExp ||
        conj->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
      return false;
    }
    const auto& bin = conj->AsBinaryExpression();
    if (!bin.Left() || !bin.Right() ||
        bin.Left()->Type() != TypeTag::kColumnValue ||
        bin.Right()->Type() != TypeTag::kColumnValue) {
      return false;
    }
    const ColumnName& lc = bin.Left()->AsColumnValue().GetColumnName();
    const ColumnName& rc = bin.Right()->AsColumnValue().GetColumnName();
    if (IdentifierEquals(lc.schema, l_alias) &&
        IdentifierEquals(rc.schema, r_alias)) {
      key_pairs.emplace_back(lc.name, rc.name);
    } else if (IdentifierEquals(lc.schema, r_alias) &&
               IdentifierEquals(rc.schema, l_alias)) {
      key_pairs.emplace_back(rc.name, lc.name);
    } else {
      return false;
    }
  }
  if (key_pairs.empty()) {
    return false;
  }

  auto body_is_nodup_on_keys = [&](const SelectStatement& body,
                                   bool left_side) -> bool {
    if (!body.UnionAll().empty() || body.GetSetOperationTree() != nullptr ||
        body.HasLimit() || body.Offset() != 0 ||
        relational_detail::StatementContainsVolatileFunction(body) ||
        SelectListHasStarOrModifiers(body)) {
      return false;
    }
    std::unordered_map<std::string, Expression> out_to_expr;
    for (const NamedExpression& item : body.SelectList()) {
      if (item.name.empty() || !item.expression) {
        return false;
      }
      out_to_expr.emplace(LoweredStarName(item.name), item.expression);
    }
    std::vector<std::string> key_expr_strs;
    for (const auto& [lk, rk] : key_pairs) {
      const std::string& kn = left_side ? lk : rk;
      const auto it = out_to_expr.find(LoweredStarName(kn));
      if (it == out_to_expr.end()) {
        return false;
      }
      key_expr_strs.push_back(it->second->ToString());
    }
    if (body.Distinct() && !body.HasDistinctOn() &&
        body.SelectList().size() == key_pairs.size()) {
      return true;
    }
    if (!body.GroupBy().empty() && !body.IsGroupAll()) {
      return std::ranges::all_of(body.GroupBy(), [&](const Expression& gk) {
        return gk && std::ranges::find(key_expr_strs, gk->ToString()) !=
                         key_expr_strs.end();
      });
    }
    return false;
  };
  if (!body_is_nodup_on_keys(*l_cte->second, true) ||
      !body_is_nodup_on_keys(*r_cte->second, false)) {
    return false;
  }

  // Classify every SELECT item as LeftOnly, RightOnly, or Both on some key
  // pair `probe_idx` (all items must share the same `probe_idx`).
  enum class IndicatorKind : uint8_t { kLeftOnly, kRightOnly, kBoth };
  struct IndicatorSlot {
    std::string out_name;
    IndicatorKind kind{IndicatorKind::kBoth};
  };
  std::optional<size_t> shared_probe_idx;
  std::vector<IndicatorSlot> slots;
  for (size_t idx = 0; idx < select->SelectList().size(); ++idx) {
    const NamedExpression& item = select->SelectList()[idx];
    if (!item.expression || item.expression->Type() != TypeTag::kAggregateExp) {
      return false;
    }
    const auto& agg = item.expression->AsAggregateExpression();
    if (agg.GetType() != AggregationType::kSum || agg.Distinct() ||
        agg.NeedsGroupContext() || agg.HasWhereFilter() || !agg.Child() ||
        agg.Child()->Type() != TypeTag::kCaseExp) {
      return false;
    }
    const auto& ce = agg.Child()->AsCaseExpression();
    if (ce.when_clauses_.size() != 1 || !ce.else_clause_) {
      return false;
    }
    const auto then_v = TpcdsEvalClosedConstant(ce.when_clauses_[0].second);
    const auto else_v = TpcdsEvalClosedConstant(ce.else_clause_);
    if (!then_v.has_value() || !else_v.has_value() ||
        then_v->type != ValueType::kInt64 ||
        else_v->type != ValueType::kInt64 || then_v->IsNull() ||
        else_v->IsNull() || then_v->value.int_value != 1 ||
        else_v->value.int_value != 0) {
      return false;
    }
    std::vector<Expression> cond_parts =
        SplitConjuncts(ce.when_clauses_[0].first);
    if (cond_parts.size() != 2) {
      return false;
    }
    std::optional<std::pair<size_t, bool>> l_test;
    std::optional<std::pair<size_t, bool>> r_test;
    for (const Expression& p : cond_parts) {
      if (!p || p->Type() != TypeTag::kUnaryExp) {
        return false;
      }
      const UnaryOperation uop = p->AsUnaryExpression().Op();
      if (uop != UnaryOperation::kIsNull && uop != UnaryOperation::kIsNotNull) {
        return false;
      }
      const Expression& ch = p->AsUnaryExpression().Child();
      if (!ch || ch->Type() != TypeTag::kColumnValue) {
        return false;
      }
      const ColumnName& cn = ch->AsColumnValue().GetColumnName();
      for (size_t k = 0; k < key_pairs.size(); ++k) {
        if (IdentifierEquals(cn.schema, l_alias) &&
            IdentifierEquals(cn.name, key_pairs[k].first)) {
          l_test = {k, uop == UnaryOperation::kIsNotNull};
        } else if (IdentifierEquals(cn.schema, r_alias) &&
                   IdentifierEquals(cn.name, key_pairs[k].second)) {
          r_test = {k, uop == UnaryOperation::kIsNotNull};
        }
      }
    }
    if (!l_test.has_value() || !r_test.has_value() ||
        l_test->first != r_test->first ||
        (!l_test->second && !r_test->second)) {
      return false;
    }
    if (shared_probe_idx.has_value() && *shared_probe_idx != l_test->first) {
      return false;
    }
    shared_probe_idx = l_test->first;
    IndicatorKind kind = IndicatorKind::kBoth;
    if (l_test->second && !r_test->second) {
      kind = IndicatorKind::kLeftOnly;
    } else if (!l_test->second && r_test->second) {
      kind = IndicatorKind::kRightOnly;
    }
    slots.push_back(
        {.out_name = InferSelectItemOutputName(item, idx), .kind = kind});
  }
  if (!shared_probe_idx.has_value() || slots.empty()) {
    return false;
  }

  auto build_side_stats_query = [&](const std::string& cte_table,
                                    bool left_side) {
    const std::string sal = left_side ? "__ls" : "__rs";
    std::vector<Expression> all_not_null;
    Expression any_other_null;
    for (size_t k = 0; k < key_pairs.size(); ++k) {
      const std::string& kn =
          left_side ? key_pairs[k].first : key_pairs[k].second;
      all_not_null.push_back(UnaryExpressionExp(
          ColumnValueExp(ColumnName(sal, kn)), UnaryOperation::kIsNotNull));
      if (k != *shared_probe_idx) {
        Expression is_null = UnaryExpressionExp(
            ColumnValueExp(ColumnName(sal, kn)), UnaryOperation::kIsNull);
        any_other_null =
            any_other_null
                ? BinaryExpressionExp(any_other_null, BinaryOperation::kOr,
                                      std::move(is_null))
                : std::move(is_null);
      }
    }
    const std::string& probe_kn = left_side
                                      ? key_pairs[*shared_probe_idx].first
                                      : key_pairs[*shared_probe_idx].second;
    Expression one = ConstantValueExp(Value(static_cast<int64_t>(1)));
    Expression part_expr =
        any_other_null
            ? AggregateExpressionExp(
                  AggregationType::kCount,
                  CaseExpressionExp(
                      {{BinaryExpressionExp(
                            UnaryExpressionExp(
                                ColumnValueExp(ColumnName(sal, probe_kn)),
                                UnaryOperation::kIsNotNull),
                            BinaryOperation::kAnd, std::move(any_other_null)),
                        one}},
                      nullptr),
                  false)
            : ConstantValueExp(Value(static_cast<int64_t>(0)));

    auto q = std::make_shared<SelectStatement>();
    q->SetSources(
        {SelectSource(cte_table, sal, nullptr, JoinType::kCross, nullptr)});
    q->SetSelectList({
        NamedExpression("tot", AggregateExpressionExp(
                                   AggregationType::kCount,
                                   ColumnValueExp(ColumnName("", "*")), false)),
        NamedExpression(
            "ok", AggregateExpressionExp(
                      AggregationType::kCount,
                      CaseExpressionExp({{CombineConjuncts(all_not_null), one}},
                                        nullptr),
                      false)),
        NamedExpression("part", std::move(part_expr)),
    });
    return q;
  };

  auto both_q = std::make_shared<SelectStatement>();
  both_q->SetSources({
      SelectSource(l_src.table, "__bl", nullptr, JoinType::kCross, nullptr),
      SelectSource(r_src.table, "__br", nullptr, JoinType::kCross, nullptr),
  });
  std::vector<Expression> both_eqs;
  both_eqs.reserve(key_pairs.size());
  for (const auto& [lk, rk] : key_pairs) {
    both_eqs.push_back(BinaryExpressionExp(
        ColumnValueExp(ColumnName("__bl", lk)), BinaryOperation::kEquals,
        ColumnValueExp(ColumnName("__br", rk))));
  }
  both_q->SetWhereClause(CombineConjuncts(both_eqs));
  both_q->SetSelectList({NamedExpression(
      "inter",
      AggregateExpressionExp(AggregationType::kCount,
                             ColumnValueExp(ColumnName("", "*")), false))});

  Expression non_empty = BinaryExpressionExp(
      BinaryExpressionExp(ColumnValueExp(ColumnName("__foj_l", "tot")),
                          BinaryOperation::kAdd,
                          ColumnValueExp(ColumnName("__foj_r", "tot"))),
      BinaryOperation::kGreaterThan,
      ConstantValueExp(Value(static_cast<int64_t>(0))));
  Expression typed_null = ConstantValueExp(Value());
  Expression inter_ref = ColumnValueExp(ColumnName("__foj_b", "inter"));

  std::vector<NamedExpression> new_select;
  for (const IndicatorSlot& slot : slots) {
    Expression val_expr;
    if (slot.kind == IndicatorKind::kLeftOnly) {
      val_expr = BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp(ColumnName("__foj_l", "ok")),
                              BinaryOperation::kSubtract, inter_ref),
          BinaryOperation::kAdd, ColumnValueExp(ColumnName("__foj_l", "part")));
    } else if (slot.kind == IndicatorKind::kRightOnly) {
      val_expr = BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp(ColumnName("__foj_r", "ok")),
                              BinaryOperation::kSubtract, inter_ref),
          BinaryOperation::kAdd, ColumnValueExp(ColumnName("__foj_r", "part")));
    } else {
      val_expr = inter_ref;
    }
    new_select.emplace_back(
        slot.out_name,
        CaseExpressionExp({{non_empty, std::move(val_expr)}}, typed_null));
  }

  select->SetSources({
      SelectSource("", "__foj_l", build_side_stats_query(l_src.table, true),
                   JoinType::kCross, nullptr),
      SelectSource("", "__foj_r", build_side_stats_query(r_src.table, false),
                   JoinType::kCross, nullptr),
      SelectSource("", "__foj_b", std::move(both_q), JoinType::kCross, nullptr),
  });
  select->SetSelectList(std::move(new_select));
  return true;
}

namespace select_rewrite_detail {

namespace {
std::optional<std::string> WindowNameForAggType(AggregationType t,
                                                bool allow_count) {
  switch (t) {
    case AggregationType::kAvg:
      return "AVG";
    case AggregationType::kSum:
      return "SUM";
    case AggregationType::kMin:
      return "MIN";
    case AggregationType::kMax:
      return "MAX";
    case AggregationType::kCount:
      return allow_count ? std::optional<std::string>("COUNT") : std::nullopt;
    default:
      return std::nullopt;
  }
}
}  // namespace

namespace {
bool RewriteSiblingDerivedAggToWindow(SelectStatement* outer) {
  if (outer == nullptr || outer->Sources().size() < 2 ||
      !outer->UnionAll().empty() || outer->GetSetOperationTree() != nullptr ||
      SelectListHasStarOrModifiers(*outer)) {
    return false;
  }
  for (const SelectSource& s : outer->Sources()) {
    if (s.join_type != JoinType::kCross && s.join_type != JoinType::kInner) {
      return false;
    }
  }

  std::vector<Expression> outer_conjuncts =
      SplitConjuncts(outer->WhereClause());
  for (const SelectSource& s : outer->Sources()) {
    if (s.join_condition) {
      for (const Expression& c : SplitConjuncts(s.join_condition)) {
        outer_conjuncts.push_back(c);
      }
    }
  }

  for (size_t sb_idx = 0; sb_idx < outer->Sources().size(); ++sb_idx) {
    const SelectSource& sb = outer->Sources()[sb_idx];
    if (sb.query == nullptr || sb.alias.empty() || sb.is_lateral || sb.unnest ||
        sb.sample || sb.from_nested_join || !sb.using_columns.empty()) {
      continue;
    }
    const SelectStatement& sb_q = *sb.query;
    if (sb_q.Sources().size() != 1 || sb_q.Sources()[0].query == nullptr ||
        sb_q.Sources()[0].is_lateral || sb_q.GroupBy().empty() ||
        sb_q.IsGroupAll() || sb_q.Having() || sb_q.WhereClause() ||
        sb_q.Distinct() || sb_q.HasDistinctOn() || sb_q.HasLimit() ||
        sb_q.Offset() != 0 || sb_q.Qualify() || !sb_q.WithQueries().empty() ||
        !sb_q.UnionAll().empty() || sb_q.GetSetOperationTree() != nullptr ||
        relational_detail::HasWindowFunctions(sb_q) ||
        SelectListHasStarOrModifiers(sb_q)) {
      continue;
    }
    const SelectStatement& sa_q = *sb_q.Sources()[0].query;
    if (relational_detail::StatementContainsVolatileFunction(sa_q) ||
        TpcdsStmtHasParam(sa_q) || SelectListHasStarOrModifiers(sa_q)) {
      continue;
    }
    const std::string sa_alias = sb_q.Sources()[0].alias;
    const std::string sa_fp = sa_q.Fingerprint();

    std::unordered_set<std::string> sa_cols_lower;
    std::vector<std::string> sa_col_names;
    bool sa_cols_ok = true;
    for (const NamedExpression& item : sa_q.SelectList()) {
      const std::string out_nm = ExplicitOrColumnOutputName(item);
      if (out_nm.empty() ||
          !sa_cols_lower.insert(LoweredStarName(out_nm)).second) {
        sa_cols_ok = false;
        break;
      }
      sa_col_names.push_back(out_nm);
    }
    if (!sa_cols_ok || sa_col_names.empty()) {
      continue;
    }

    std::vector<std::string> sb_key_cols;
    bool keys_ok = true;
    for (const Expression& gk : sb_q.GroupBy()) {
      if (!gk || gk->Type() != TypeTag::kColumnValue) {
        keys_ok = false;
        break;
      }
      const ColumnName& gc = gk->AsColumnValue().GetColumnName();
      if ((!gc.schema.empty() && !IdentifierEquals(gc.schema, sa_alias)) ||
          !sa_cols_lower.contains(LoweredStarName(gc.name))) {
        keys_ok = false;
        break;
      }
      sb_key_cols.push_back(gc.name);
    }
    if (!keys_ok) {
      continue;
    }

    struct WinAggSpec {
      std::string sb_out_name;
      std::string win_col_name;
      std::string fn_name;
      Expression arg_on_base;
    };
    std::unordered_map<std::string, std::string> sb_key_out_to_sa;
    std::vector<WinAggSpec> win_aggs;
    bool sb_sel_ok = true;
    for (const NamedExpression& item : sb_q.SelectList()) {
      const std::string sb_out = ExplicitOrColumnOutputName(item);
      if (sb_out.empty() || !item.expression) {
        sb_sel_ok = false;
        break;
      }
      if (item.expression->Type() == TypeTag::kColumnValue) {
        const ColumnName& ic = item.expression->AsColumnValue().GetColumnName();
        if ((!ic.schema.empty() && !IdentifierEquals(ic.schema, sa_alias)) ||
            !std::ranges::any_of(sb_key_cols, [&](const std::string& k) {
              return IdentifierEquals(k, ic.name);
            })) {
          sb_sel_ok = false;
          break;
        }
        sb_key_out_to_sa.emplace(LoweredStarName(sb_out), ic.name);
        continue;
      }
      if (item.expression->Type() == TypeTag::kAggregateExp) {
        const auto& agg = item.expression->AsAggregateExpression();
        const auto fn = WindowNameForAggType(agg.GetType(), true);
        if (!fn.has_value() || agg.Distinct() || agg.NeedsGroupContext() ||
            agg.HasWhereFilter() || !agg.InnerGroupBy().empty() ||
            !agg.InnerOrderBy().empty() || !agg.Child() ||
            agg.Child()->Type() != TypeTag::kColumnValue) {
          sb_sel_ok = false;
          break;
        }
        const ColumnName& ac = agg.Child()->AsColumnValue().GetColumnName();
        Expression base_arg;
        if (ac.name == "*" && agg.GetType() == AggregationType::kCount) {
          base_arg = ColumnValueExp(ColumnName("", "*"));
        } else if ((ac.schema.empty() ||
                    IdentifierEquals(ac.schema, sa_alias)) &&
                   sa_cols_lower.contains(LoweredStarName(ac.name))) {
          base_arg = ColumnValueExp(ColumnName("__tpcds_sc_base", ac.name));
        } else {
          sb_sel_ok = false;
          break;
        }
        win_aggs.push_back(
            {.sb_out_name = item.name,
             .win_col_name = "__tpcds_sb_win_" + IntegerToName(win_aggs.size()),
             .fn_name = *fn,
             .arg_on_base = std::move(base_arg)});
        continue;
      }
      sb_sel_ok = false;
      break;
    }
    if (!sb_sel_ok || win_aggs.empty()) {
      continue;
    }

    for (size_t sc_idx = 0; sc_idx < outer->Sources().size(); ++sc_idx) {
      if (sc_idx == sb_idx) {
        continue;
      }
      const SelectSource& sc = outer->Sources()[sc_idx];
      if (sc.query == nullptr || sc.alias.empty() || sc.is_lateral ||
          sc.unnest || sc.sample || sc.from_nested_join ||
          !sc.using_columns.empty() || IdentifierEquals(sb.alias, sc.alias) ||
          sc.query->Fingerprint() != sa_fp) {
        continue;
      }

      // Check that every `sb_key_col` is equated between `sb.alias` and
      // `sc.alias` in `outer_conjuncts`.
      bool all_keys_bound = true;
      for (const std::string& kn : sb_key_cols) {
        std::string sb_out_for_kn;
        for (const auto& [sb_out_low, sa_kn] : sb_key_out_to_sa) {
          if (IdentifierEquals(sa_kn, kn)) {
            sb_out_for_kn = sb_out_low;
            break;
          }
        }
        if (sb_out_for_kn.empty()) {
          all_keys_bound = false;
          break;
        }
        bool bound = false;
        for (const Expression& conj : outer_conjuncts) {
          if (!conj || conj->Type() != TypeTag::kBinaryExp ||
              conj->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
            continue;
          }
          const auto& bin = conj->AsBinaryExpression();
          if (!bin.Left() || !bin.Right() ||
              bin.Left()->Type() != TypeTag::kColumnValue ||
              bin.Right()->Type() != TypeTag::kColumnValue) {
            continue;
          }
          const ColumnName& lc = bin.Left()->AsColumnValue().GetColumnName();
          const ColumnName& rc = bin.Right()->AsColumnValue().GetColumnName();
          const bool m1 = IdentifierEquals(lc.schema, sb.alias) &&
                          LoweredStarName(lc.name) == sb_out_for_kn &&
                          IdentifierEquals(rc.schema, sc.alias) &&
                          IdentifierEquals(rc.name, kn);
          const bool m2 = IdentifierEquals(rc.schema, sb.alias) &&
                          LoweredStarName(rc.name) == sb_out_for_kn &&
                          IdentifierEquals(lc.schema, sc.alias) &&
                          IdentifierEquals(lc.name, kn);
          if (m1 || m2) {
            bound = true;
            break;
          }
        }
        if (!bound) {
          all_keys_bound = false;
          break;
        }
      }
      if (!all_keys_bound) {
        continue;
      }

      std::unordered_map<std::string, Expression> sb_col_repl;
      for (const auto& [sb_out_low, sa_kn] : sb_key_out_to_sa) {
        sb_col_repl.emplace(sb_out_low,
                            ColumnValueExp(ColumnName(sc.alias, sa_kn)));
      }
      std::unordered_set<std::string> sb_only_agg_names;
      for (const WinAggSpec& w : win_aggs) {
        const std::string low = LoweredStarName(w.sb_out_name);
        sb_col_repl.emplace(
            low, ColumnValueExp(ColumnName(sc.alias, w.win_col_name)));
        if (!sa_cols_lower.contains(low)) {
          sb_only_agg_names.insert(low);
        }
      }

      auto map_expr = [&](auto& self,
                          const Expression& expr) -> std::optional<Expression> {
        if (!expr) {
          return expr;
        }
        if (expr->Type() == TypeTag::kQueryExp) {
          return std::nullopt;
        }
        if (expr->Type() == TypeTag::kColumnValue) {
          const ColumnName& cn = expr->AsColumnValue().GetColumnName();
          if (cn.name == "*") {
            return std::nullopt;
          }
          if (!cn.schema.empty()) {
            if (IdentifierEquals(cn.schema, sb.alias)) {
              const auto it = sb_col_repl.find(LoweredStarName(cn.name));
              if (it == sb_col_repl.end()) {
                return std::nullopt;
              }
              return it->second;
            }
            return expr;
          }
          const std::string low = LoweredStarName(cn.name);
          if (sb_only_agg_names.contains(low)) {
            return sb_col_repl.at(low);
          }
          if (sb_key_out_to_sa.contains(low)) {
            return std::nullopt;
          }
          return expr;
        }
        std::vector<Expression> kids = ExpressionChildren(expr);
        for (Expression& k : kids) {
          std::optional<Expression> mk = self(self, k);
          if (!mk.has_value()) {
            return std::nullopt;
          }
          k = std::move(*mk);
        }
        return WithExpressionChildren(expr, std::move(kids));
      };

      std::vector<NamedExpression> new_sel;
      bool ok = true;
      for (size_t i = 0; i < outer->SelectList().size(); ++i) {
        const NamedExpression& item = outer->SelectList()[i];
        std::optional<Expression> m = map_expr(map_expr, item.expression);
        if (!m.has_value()) {
          ok = false;
          break;
        }
        new_sel.emplace_back(InferSelectItemOutputName(item, i), std::move(*m));
      }
      std::optional<Expression> new_where =
          map_expr(map_expr, outer->WhereClause());
      std::optional<Expression> new_having =
          map_expr(map_expr, outer->Having());
      std::optional<Expression> new_qualify =
          map_expr(map_expr, outer->Qualify());
      if (!ok || !new_where.has_value() || !new_having.has_value() ||
          !new_qualify.has_value()) {
        continue;
      }
      std::vector<Expression> new_group;
      for (const Expression& g : outer->GroupBy()) {
        std::optional<Expression> mg = map_expr(map_expr, g);
        if (!mg.has_value()) {
          ok = false;
          break;
        }
        new_group.push_back(std::move(*mg));
      }
      std::vector<SelectStatement::OrderByTerm> new_order = outer->OrderBy();
      for (auto& term : new_order) {
        std::optional<Expression> mo = map_expr(map_expr, term.expression);
        if (!mo.has_value()) {
          ok = false;
          break;
        }
        term.expression = std::move(*mo);
      }
      // The sb source is dropped below, so its output references in DISTINCT
      // ON must be remapped onto the surviving window source as well.
      std::vector<Expression> new_distinct_on;
      new_distinct_on.reserve(outer->DistinctOn().size());
      for (const Expression& key : outer->DistinctOn()) {
        std::optional<Expression> md = map_expr(map_expr, key);
        if (!md.has_value()) {
          ok = false;
          break;
        }
        new_distinct_on.push_back(std::move(*md));
      }
      if (!ok) {
        continue;
      }

      std::vector<SelectSource> new_sources;
      std::vector<Expression> extra_where;
      for (size_t i = 0; i < outer->Sources().size(); ++i) {
        SelectSource s = outer->Sources()[i];
        if (s.join_condition) {
          std::optional<Expression> mjc = map_expr(map_expr, s.join_condition);
          if (!mjc.has_value()) {
            ok = false;
            break;
          }
          if (i == sb_idx) {
            extra_where.push_back(std::move(*mjc));
          } else {
            s.join_condition = std::move(*mjc);
          }
        }
        if (i != sb_idx && s.query != nullptr && i != sc_idx) {
          // Reject if a sibling subquery correlated onto `sb`.
          if (s.is_lateral) {
            ok = false;
            break;
          }
        }
        if (i == sb_idx) {
          continue;
        }
        if (i == sc_idx) {
          std::vector<Expression> part_keys;
          part_keys.reserve(sb_key_cols.size());
          for (const std::string& kn : sb_key_cols) {
            part_keys.push_back(
                ColumnValueExp(ColumnName("__tpcds_sc_base", kn)));
          }
          std::vector<NamedExpression> win_sel;
          win_sel.reserve(sa_col_names.size());
          for (const std::string& c : sa_col_names) {
            win_sel.emplace_back(
                c, ColumnValueExp(ColumnName("__tpcds_sc_base", c)));
          }
          for (const WinAggSpec& w : win_aggs) {
            win_sel.emplace_back(
                w.win_col_name, WindowFunctionCallExp(
                                    w.fn_name, {w.arg_on_base}, part_keys, {}));
          }
          auto wrapped = std::make_shared<SelectStatement>();
          wrapped->SetSources({SelectSource("", "__tpcds_sc_base", s.query,
                                            JoinType::kCross, nullptr)});
          wrapped->SetSelectList(std::move(win_sel));
          wrapped->MarkComplex();
          s.query = std::move(wrapped);
        }
        new_sources.push_back(std::move(s));
      }
      if (!ok) {
        continue;
      }

      std::vector<Expression> final_where;
      if (*new_where) {
        final_where.push_back(std::move(*new_where));
      }
      for (Expression& ew : extra_where) {
        final_where.push_back(std::move(ew));
      }
      for (const std::string& kn : sb_key_cols) {
        final_where.push_back(
            UnaryExpressionExp(ColumnValueExp(ColumnName(sc.alias, kn)),
                               UnaryOperation::kIsNotNull));
      }

      outer->SetSources(std::move(new_sources));
      outer->SetSelectList(std::move(new_sel));
      outer->SetWhereClause(CombineConjuncts(final_where));
      outer->SetGroupBy(std::move(new_group));
      outer->SetHaving(std::move(*new_having));
      outer->SetQualify(std::move(*new_qualify));
      if (!new_distinct_on.empty()) {
        outer->SetDistinctOn(std::move(new_distinct_on));
      }
      outer->SetOrderBy(std::move(new_order));
      return true;
    }
  }
  return false;
}
}  // namespace

namespace {
bool RewriteCorrelatedCteScalarAggToWindow(SelectStatement* outer) {
  if (outer == nullptr || outer->WithQueries().empty() ||
      !outer->WhereClause() || !outer->UnionAll().empty() ||
      outer->GetSetOperationTree() != nullptr ||
      SelectListHasStarOrModifiers(*outer)) {
    return false;
  }
  for (const SelectSource& s : outer->Sources()) {
    if (s.join_type != JoinType::kCross && s.join_type != JoinType::kInner) {
      return false;
    }
  }

  std::unordered_set<std::string> all_ctes;
  for (const auto& [name, body] : outer->WithQueries()) {
    (void)body;
    all_ctes.insert(name);
  }
  std::unordered_map<std::string, size_t> counts;
  CountCteReferencesInStatement(*outer, all_ctes, &counts);

  for (const std::string& cte_name : outer->WithQueryOrder()) {
    if (outer->IsRecursiveWith(cte_name) || counts[cte_name] != 2) {
      continue;
    }
    const auto cte_it = outer->WithQueries().find(cte_name);
    if (cte_it == outer->WithQueries().end() || cte_it->second == nullptr) {
      continue;
    }
    const SelectStatement& cte_body = *cte_it->second;
    if (relational_detail::StatementContainsVolatileFunction(cte_body) ||
        SelectListHasStarOrModifiers(cte_body) ||
        cte_body.SelectList().empty()) {
      continue;
    }
    std::unordered_set<std::string> cte_cols_lower;
    std::vector<std::string> cte_col_names;
    cte_col_names.reserve(cte_body.SelectList().size());
    bool cols_ok = true;
    for (const NamedExpression& item : cte_body.SelectList()) {
      if (item.name.empty() ||
          !cte_cols_lower.insert(LoweredStarName(item.name)).second) {
        cols_ok = false;
        break;
      }
      cte_col_names.push_back(item.name);
    }
    if (!cols_ok) {
      continue;
    }

    std::optional<size_t> ctr1_idx;
    for (size_t i = 0; i < outer->Sources().size(); ++i) {
      const SelectSource& s = outer->Sources()[i];
      if (s.table == cte_name) {
        if (ctr1_idx.has_value() || s.query != nullptr || s.is_lateral ||
            s.unnest || s.sample || s.from_nested_join ||
            !s.using_columns.empty()) {
          ctr1_idx.reset();
          break;
        }
        ctr1_idx = i;
      }
    }
    if (!ctr1_idx.has_value()) {
      continue;
    }
    const SelectSource& ctr1 = outer->Sources()[*ctr1_idx];
    const std::string ctr1_alias = ctr1.alias.empty() ? ctr1.table : ctr1.alias;
    if (ctr1_alias != cte_name && StatementQualifiesName(*outer, cte_name)) {
      continue;
    }

    std::vector<Expression> where_conjuncts =
        SplitConjuncts(outer->WhereClause());
    for (size_t ci = 0; ci < where_conjuncts.size(); ++ci) {
      const Expression& conj = where_conjuncts[ci];
      if (!conj || conj->Type() != TypeTag::kBinaryExp) {
        continue;
      }
      const auto& cmp = conj->AsBinaryExpression();
      const BinaryOperation op = cmp.Op();
      if (op != BinaryOperation::kEquals && op != BinaryOperation::kNotEquals &&
          op != BinaryOperation::kLessThan &&
          op != BinaryOperation::kLessThanEquals &&
          op != BinaryOperation::kGreaterThan &&
          op != BinaryOperation::kGreaterThanEquals) {
        continue;
      }
      for (int qside = 0; qside < 2; ++qside) {
        const Expression& qcand = qside == 0 ? cmp.Left() : cmp.Right();
        const Expression& other = qside == 0 ? cmp.Right() : cmp.Left();
        if (!qcand || qcand->Type() != TypeTag::kQueryExp || !other ||
            ContainsQueryExpression(other)) {
          continue;
        }
        const auto& qe = qcand->AsQueryExpression();
        if (qe.Exists() || qe.Negated() || qe.ArrayResult() || qe.Test() ||
            qe.Query() == nullptr) {
          continue;
        }
        const SelectStatement& sq = *qe.Query();
        if (sq.Sources().size() != 1 || sq.Sources()[0].table != cte_name ||
            sq.Sources()[0].query != nullptr || sq.Sources()[0].is_lateral ||
            sq.Sources()[0].unnest || sq.Sources()[0].sample ||
            !sq.WithQueries().empty() || !sq.GroupBy().empty() || sq.Having() ||
            sq.Distinct() || sq.HasDistinctOn() || sq.HasLimit() ||
            sq.Offset() != 0 || sq.Qualify() || !sq.UnionAll().empty() ||
            sq.GetSetOperationTree() != nullptr ||
            sq.SelectList().size() != 1 || !sq.WhereClause()) {
          continue;
        }
        const std::string ctr2_alias = sq.Sources()[0].alias.empty()
                                           ? sq.Sources()[0].table
                                           : sq.Sources()[0].alias;
        if (IdentifierEquals(ctr1_alias, ctr2_alias)) {
          continue;
        }

        // Unwrap `AGG(col)` or `AGG(col) * const` / `const * AGG(col)`.
        const Expression& proj = sq.SelectList()[0].expression;
        const AggregateExpression* agg_ptr = nullptr;
        Expression scale_factor;
        bool scale_on_left = false;
        if (proj && proj->Type() == TypeTag::kAggregateExp) {
          agg_ptr = &proj->AsAggregateExpression();
        } else if (proj && proj->Type() == TypeTag::kBinaryExp &&
                   proj->AsBinaryExpression().Op() ==
                       BinaryOperation::kMultiply) {
          const auto& mul = proj->AsBinaryExpression();
          if (mul.Left() && mul.Left()->Type() == TypeTag::kAggregateExp &&
              TpcdsEvalClosedConstant(mul.Right()).has_value()) {
            agg_ptr = &mul.Left()->AsAggregateExpression();
            scale_factor = mul.Right();
            scale_on_left = false;
          } else if (mul.Right() &&
                     mul.Right()->Type() == TypeTag::kAggregateExp &&
                     TpcdsEvalClosedConstant(mul.Left()).has_value()) {
            agg_ptr = &mul.Right()->AsAggregateExpression();
            scale_factor = mul.Left();
            scale_on_left = true;
          }
        }
        if (agg_ptr == nullptr || agg_ptr->Distinct() ||
            agg_ptr->NeedsGroupContext() || agg_ptr->HasWhereFilter() ||
            !agg_ptr->InnerGroupBy().empty() ||
            !agg_ptr->InnerOrderBy().empty() || !agg_ptr->Child() ||
            agg_ptr->Child()->Type() != TypeTag::kColumnValue) {
          continue;
        }
        // Reject COUNT: COUNT(*) over an empty match (when ctr1.key IS NULL) is
        // 0 rather than NULL, whereas AVG/SUM/MIN/MAX return NULL.
        const auto fn = WindowNameForAggType(agg_ptr->GetType(), false);
        if (!fn.has_value()) {
          continue;
        }
        const ColumnName& ac =
            agg_ptr->Child()->AsColumnValue().GetColumnName();
        if ((!ac.schema.empty() && !IdentifierEquals(ac.schema, ctr2_alias)) ||
            !cte_cols_lower.contains(LoweredStarName(ac.name))) {
          continue;
        }

        std::vector<std::string> part_col_names;
        bool sq_where_ok = true;
        for (const Expression& sconj : SplitConjuncts(sq.WhereClause())) {
          if (!sconj || sconj->Type() != TypeTag::kBinaryExp ||
              sconj->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
            sq_where_ok = false;
            break;
          }
          const auto& sbin = sconj->AsBinaryExpression();
          if (!sbin.Left() || !sbin.Right() ||
              sbin.Left()->Type() != TypeTag::kColumnValue ||
              sbin.Right()->Type() != TypeTag::kColumnValue) {
            sq_where_ok = false;
            break;
          }
          const ColumnName& lc = sbin.Left()->AsColumnValue().GetColumnName();
          const ColumnName& rc = sbin.Right()->AsColumnValue().GetColumnName();
          std::string matched_col;
          if (IdentifierEquals(lc.schema, ctr1_alias) &&
              IdentifierEquals(rc.schema, ctr2_alias) &&
              IdentifierEquals(lc.name, rc.name) &&
              cte_cols_lower.contains(LoweredStarName(lc.name))) {
            matched_col = lc.name;
          } else if (IdentifierEquals(rc.schema, ctr1_alias) &&
                     IdentifierEquals(lc.schema, ctr2_alias) &&
                     IdentifierEquals(lc.name, rc.name) &&
                     cte_cols_lower.contains(LoweredStarName(rc.name))) {
            matched_col = rc.name;
          }
          if (matched_col.empty()) {
            sq_where_ok = false;
            break;
          }
          if (!std::ranges::any_of(part_col_names, [&](const std::string& p) {
                return IdentifierEquals(p, matched_col);
              })) {
            part_col_names.push_back(std::move(matched_col));
          }
        }
        if (!sq_where_ok || part_col_names.empty()) {
          continue;
        }

        const std::string win_col = "__tpcds_ctr_win";
        std::vector<Expression> part_exprs;
        part_exprs.reserve(part_col_names.size());
        // Both loops are intentionally inside this body (8-space scope) and
        // follow the collected declaration order; keep them formatted that
        // way so the partition/SELECT list boundaries stay unambiguous.
        for (const std::string& p : part_col_names) {
          part_exprs.push_back(
              ColumnValueExp(ColumnName("__tpcds_ctr_base", p)));
        }
        std::vector<NamedExpression> win_sel;
        win_sel.reserve(cte_col_names.size());
        for (const std::string& c : cte_col_names) {
          win_sel.emplace_back(
              c, ColumnValueExp(ColumnName("__tpcds_ctr_base", c)));
        }
        win_sel.emplace_back(
            win_col,
            WindowFunctionCallExp(
                *fn, {ColumnValueExp(ColumnName("__tpcds_ctr_base", ac.name))},
                std::move(part_exprs), {}));
        auto win_subq = std::make_shared<SelectStatement>();
        win_subq->SetSources({SelectSource(
            cte_name, "__tpcds_ctr_base", nullptr, JoinType::kCross, nullptr)});
        win_subq->SetSelectList(std::move(win_sel));
        win_subq->MarkComplex();

        Expression replacement =
            ColumnValueExp(ColumnName(ctr1_alias, win_col));
        if (scale_factor) {
          replacement = scale_on_left
                            ? BinaryExpressionExp(scale_factor,
                                                  BinaryOperation::kMultiply,
                                                  std::move(replacement))
                            : BinaryExpressionExp(std::move(replacement),
                                                  BinaryOperation::kMultiply,
                                                  scale_factor);
        }
        where_conjuncts[ci] =
            qside == 0 ? BinaryExpressionExp(std::move(replacement), op, other)
                       : BinaryExpressionExp(other, op, std::move(replacement));
        for (const std::string& p : part_col_names) {
          where_conjuncts.push_back(
              UnaryExpressionExp(ColumnValueExp(ColumnName(ctr1_alias, p)),
                                 UnaryOperation::kIsNotNull));
        }

        std::vector<SelectSource> new_sources = outer->Sources();
        new_sources[*ctr1_idx].table.clear();
        new_sources[*ctr1_idx].alias = ctr1_alias;
        new_sources[*ctr1_idx].query = std::move(win_subq);
        outer->SetSources(std::move(new_sources));
        outer->SetWhereClause(CombineConjuncts(where_conjuncts));
        return true;
      }
    }
  }
  return false;
}
}  // namespace

}  // namespace select_rewrite_detail

// Lean proof: lean/TinylambOpt/TpcdsDiscovery.lean
//   (self_join_group_agg_to_window_sound,
//    self_join_agg_window_null_key_bug)
bool RewriteSelfJoinAggregateToWindow(SelectStatement* outer) {
  if (outer == nullptr) {
    return false;
  }
  bool changed = false;
  while (RewriteSiblingDerivedAggToWindow(outer)) {
    changed = true;
  }
  while (RewriteCorrelatedCteScalarAggToWindow(outer)) {
    changed = true;
  }
  return changed;
}

namespace select_rewrite_detail {

namespace {
bool DerivedQueryIsSelfContainedAndExpensive(const SelectStatement& q) {
  if (!q.WithQueries().empty() || !q.UnionAll().empty() ||
      q.GetSetOperationTree() != nullptr ||
      relational_detail::StatementContainsVolatileFunction(q) ||
      TpcdsStmtHasParam(q) || SelectListHasStarOrModifiers(q) ||
      q.SelectList().empty() || q.Sources().empty()) {
    return false;
  }
  const bool expensive = !q.GroupBy().empty() || q.Having() != nullptr ||
                         ContainsQueryExpression(q.WhereClause()) ||
                         ContainsQueryExpression(q.Having());
  if (!expensive) {
    return false;
  }
  std::unordered_set<std::string> seen_out;
  for (const NamedExpression& item : q.SelectList()) {
    if (item.name.empty() ||
        !seen_out.insert(LoweredStarName(item.name)).second) {
      return false;
    }
  }
  std::unordered_set<std::string> local_aliases;
  for (const SelectSource& s : q.Sources()) {
    if (s.table.empty() || s.query != nullptr || s.is_lateral || s.unnest ||
        s.sample || s.from_nested_join) {
      return false;
    }
    local_aliases.insert(LoweredStarName(s.table));
    if (!s.alias.empty()) {
      local_aliases.insert(LoweredStarName(s.alias));
    }
  }
  // Ensure no outer-qualified column references exist in `q`'s own scope.
  auto expr_self_contained = [&](auto& self, const Expression& e) -> bool {
    if (!e) {
      return true;
    }
    if (e->Type() == TypeTag::kQueryExp) {
      const auto& sq = e->AsQueryExpression();
      if (sq.Test() && !self(self, sq.Test())) {
        return false;
      }
      if (sq.Query() == nullptr || !sq.Query()->WithQueries().empty() ||
          sq.Query()->Sources().empty()) {
        return false;
      }
      std::unordered_set<std::string> sq_local = local_aliases;
      for (const SelectSource& ss : sq.Query()->Sources()) {
        if (ss.table.empty() || ss.query != nullptr || ss.is_lateral) {
          return false;
        }
        sq_local.insert(LoweredStarName(ss.table));
        if (!ss.alias.empty()) {
          sq_local.insert(LoweredStarName(ss.alias));
        }
        // NOLINTNEXTLINE(readability-use-anyofallof) explicit loop is clearer
        // here
      }
      auto check_sq_cols = [&](const Expression& se) {
        if (!se) {
          return true;
        }
        return std::ranges::all_of(
            se->TouchedColumns(), [&](const ColumnName& cn) {
              return cn.schema.empty() ||
                     sq_local.contains(LoweredStarName(cn.schema));
            });
        // NOLINTNEXTLINE(readability-use-anyofallof) explicit loop is clearer
        // here
      };
      // Every clause that can contain an expression must be checked, not just
      // WHERE/HAVING/SELECT: an outer-qualified reference hiding in the
      // subquery's ORDER BY (or DISTINCT ON / GROUP BY / QUALIFY) would be
      // re-bound to the wrong scope once the derived query is hoisted.
      if (!check_sq_cols(sq.Query()->WhereClause()) ||
          !check_sq_cols(sq.Query()->Having()) ||
          !check_sq_cols(sq.Query()->Qualify())) {
        return false;
      }
      for (const Expression& group_expr : sq.Query()->GroupBy()) {
        if (!check_sq_cols(group_expr)) {
          return false;
        }
      }
      for (const Expression& distinct_expr : sq.Query()->DistinctOn()) {
        if (!check_sq_cols(distinct_expr)) {
          return false;
        }
      }
      for (const auto& term : sq.Query()->OrderBy()) {
        if (!check_sq_cols(term.expression)) {
          return false;
        }
      }
      return std::ranges::all_of(sq.Query()->SelectList(),
                                 [&](const NamedExpression& si) {
                                   return check_sq_cols(si.expression);
                                 });
    }
    if (e->Type() == TypeTag::kColumnValue) {
      const ColumnName& cn = e->AsColumnValue().GetColumnName();
      return cn.schema.empty() ||
             local_aliases.contains(LoweredStarName(cn.schema));
    }
    return std::ranges::all_of(ExpressionChildren(e), [&](const Expression& c) {
      return self(self, c);
    });
  };
  if (!expr_self_contained(expr_self_contained, q.WhereClause()) ||
      !expr_self_contained(expr_self_contained, q.Having()) ||
      !expr_self_contained(expr_self_contained, q.Qualify())) {
    return false;
  }
  for (const NamedExpression& item : q.SelectList()) {
    if (!expr_self_contained(expr_self_contained, item.expression)) {
      return false;
    }
  }
  for (const Expression& g : q.GroupBy()) {
    if (!expr_self_contained(expr_self_contained, g)) {
      return false;
    }
  }
  for (const Expression& d : q.DistinctOn()) {
    if (!expr_self_contained(expr_self_contained, d)) {
      return false;
    }
  }
  for (const auto& term : q.OrderBy()) {
    if (!expr_self_contained(expr_self_contained, term.expression)) {
      return false;
    }
  }
  return true;
}
}  // namespace

}  // namespace select_rewrite_detail

// Lean proof: lean/TinylambOpt/TpcdsDiscovery.lean
//   (shared subquery factorization / multiset reflexivity)
bool HoistIdenticalDerivedSubqueriesToCte(SelectStatement* outer) {
  if (outer == nullptr || !outer->UnionAll().empty() ||
      outer->GetSetOperationTree() != nullptr) {
    return false;
  }
  std::unordered_map<std::string, std::vector<SelectSource*>> groups;
  std::vector<std::string> fp_order;
  auto collect = [&](auto& self, SelectStatement* cur, int depth) -> void {
    if (cur == nullptr || depth > 3) {
      return;
    }
    for (SelectSource& src :
         const_cast<std::vector<SelectSource>&>(cur->Sources())) {
      if (src.query == nullptr || src.alias.empty() || src.is_lateral ||
          src.unnest || src.sample || src.from_nested_join ||
          !src.using_columns.empty()) {
        continue;
      }
      if (DerivedQueryIsSelfContainedAndExpensive(*src.query)) {
        const std::string fp = src.query->Fingerprint();
        if (!groups.contains(fp)) {
          fp_order.push_back(fp);
        }
        groups[fp].push_back(&src);
        continue;
      }
      if (src.query->WithQueries().empty()) {
        self(self, src.query.get(), depth + 1);
      }
    }
  };
  collect(collect, outer, 0);

  bool hoisted = false;
  size_t seq = 0;
  for (const std::string& fp : fp_order) {
    std::vector<SelectSource*>& sites = groups[fp];
    if (sites.size() < 2) {
      continue;
    }
    std::string cte_name = "__tpcds_hoisted_sq_" + std::to_string(seq++);
    while (outer->WithQueries().contains(cte_name)) {
      cte_name = "__tpcds_hoisted_sq_" + std::to_string(seq++);
    }
    auto body = std::make_shared<SelectStatement>(*sites[0]->query);
    outer->AddWithQuery(cte_name, std::move(body));
    for (SelectSource* site : sites) {
      site->table = cte_name;
      site->query = nullptr;
    }
    hoisted = true;
  }
  return hoisted;
}

// Lean proof: lean/TinylambOpt/TpcdsDiscovery.lean
//   (disjoint_union_group_by_bypass_sound,
//    agg_union_null_tag_collision_bug)
bool BypassDisjointUnionGroupBy(SelectStatement* select) {
  if (select == nullptr || select->Sources().size() != 1 ||
      select->GroupBy().empty() || select->IsGroupAll() || select->Qualify() ||
      select->Distinct() || select->HasDistinctOn() || select->ForUpdate() ||
      select->AsStruct() || select->WithTies() ||
      !select->WithQueries().empty() || !select->UnionAll().empty() ||
      select->GetSetOperationTree() != nullptr ||
      relational_detail::HasWindowFunctions(*select) ||
      SelectListHasStarOrModifiers(*select) || TpcdsStmtHasParam(*select)) {
    return false;
  }
  const SelectSource& foo = select->Sources()[0];
  if (foo.query == nullptr || foo.alias.empty() || foo.is_lateral ||
      foo.unnest || foo.sample || foo.from_nested_join ||
      !foo.using_columns.empty() || foo.join_type != JoinType::kCross) {
    return false;
  }

  std::vector<std::shared_ptr<SelectStatement>> branches;
  if (!ExtractPureUnionAllBranches(*foo.query, &branches)) {
    return false;
  }
  const size_t arity = branches[0]->SelectList().size();
  if (arity == 0 || SelectListHasStarOrModifiers(*branches[0])) {
    return false;
  }
  std::vector<std::string> branch0_names;
  std::unordered_map<std::string, size_t> col_to_idx;
  col_to_idx.reserve(arity);
  for (size_t c = 0; c < arity; ++c) {
    const std::string nm =
        ExplicitOrColumnOutputName(branches[0]->SelectList()[c]);
    if (nm.empty() || !col_to_idx.emplace(LoweredStarName(nm), c).second) {
      return false;
    }
    branch0_names.push_back(nm);
  }
  for (size_t b = 1; b < branches.size(); ++b) {
    if (branches[b]->SelectList().size() != arity ||
        SelectListHasStarOrModifiers(*branches[b])) {
      return false;
    }
  }

  // Check if at least one outer GroupBy key maps to a column projected as
  // pairwise-distinct safe constants across all branches.
  bool found_disjoint_key = false;
  for (const Expression& gk : select->GroupBy()) {
    if (!gk || gk->Type() != TypeTag::kColumnValue) {
      continue;
    }
    const ColumnName& gc = gk->AsColumnValue().GetColumnName();
    if (!gc.schema.empty() && !IdentifierEquals(gc.schema, foo.alias)) {
      continue;
    }
    const auto it = col_to_idx.find(LoweredStarName(gc.name));
    if (it == col_to_idx.end()) {
      continue;
    }
    const size_t cidx = it->second;
    std::vector<Value> tags;
    bool all_const = true;
    for (const auto& br : branches) {
      const Expression& bexpr = br->SelectList()[cidx].expression;
      if (!bexpr || bexpr->Type() != TypeTag::kConstantValue) {
        all_const = false;
        break;
      }
      const Value& v = bexpr->AsConstantValue().GetValue();
      if (!IsSafeDisjointTagValue(v) ||
          (!tags.empty() && v.type != tags.front().type)) {
        all_const = false;
        break;
      }
      tags.push_back(v);
    }
    if (!all_const) {
      continue;
    }
    bool distinct = true;
    for (size_t a = 0; a < tags.size() && distinct; ++a) {
      for (size_t b = a + 1; b < tags.size(); ++b) {
        if (tags[a] == tags[b]) {
          distinct = false;
          break;
        }
      }
    }
    if (distinct) {
      found_disjoint_key = true;
      break;
    }
  }
  if (!found_disjoint_key) {
    return false;
  }

  // Ensure every outer SelectList item has a deterministic unique output name
  // and every outer OrderBy term binds cleanly to an ordinal or output column
  // for `ExecuteSetOperation`'s post-union merge sort!
  // NOLINTNEXTLINE(performance-inefficient-string-concatenation) short fixed
  // concatenations
  std::vector<std::string> outer_out_names;
  std::unordered_set<std::string> seen_outer_names;
  for (size_t i = 0; i < select->SelectList().size(); ++i) {
    std::string nm = InferSelectItemOutputName(select->SelectList()[i], i);
    if (!seen_outer_names.insert(LoweredStarName(nm)).second) {
      nm += "_";
      nm += std::to_string(i);
      seen_outer_names.insert(LoweredStarName(nm));
    }
    outer_out_names.push_back(std::move(nm));
  }

  std::vector<SelectStatement::OrderByTerm> merged_outer_order;
  for (const auto& term : select->OrderBy()) {
    SelectStatement::OrderByTerm mt = term;
    if (!term.expression) {
      return false;
    }
    if (term.expression->Type() == TypeTag::kConstantValue) {
      merged_outer_order.push_back(std::move(mt));
      continue;
    }
    std::optional<size_t> matched_out;
    for (size_t i = 0; i < select->SelectList().size(); ++i) {
      const NamedExpression& si = select->SelectList()[i];
      if (term.expression->Type() == TypeTag::kColumnValue) {
        const ColumnName& oc = term.expression->AsColumnValue().GetColumnName();
        if (oc.schema.empty() &&
            IdentifierEquals(outer_out_names[i], oc.name)) {
          matched_out = i;
          break;
        }
        if (si.expression && si.expression->Type() == TypeTag::kColumnValue) {
          const ColumnName& sc = si.expression->AsColumnValue().GetColumnName();
          if ((oc.schema.empty() || IdentifierEquals(oc.schema, foo.alias)) &&
              (sc.schema.empty() || IdentifierEquals(sc.schema, foo.alias)) &&
              IdentifierEquals(oc.name, sc.name)) {
            matched_out = i;
            break;
          }
        }
      }
      if (si.expression &&
          si.expression->ToString() == term.expression->ToString()) {
        matched_out = i;
        break;
      }
    }
    if (!matched_out.has_value()) {
      return false;
    }
    mt.expression =
        ColumnValueExp(ColumnName("", outer_out_names[*matched_out]));
    merged_outer_order.push_back(std::move(mt));
  }

  const bool has_outer_limit = select->HasLimit();
  const size_t outer_limit = select->Limit();
  const size_t outer_offset = select->Offset();
  if (has_outer_limit &&
      outer_offset > std::numeric_limits<size_t>::max() - outer_limit) {
    return false;
  }
  const std::optional<size_t> branch_limit =
      has_outer_limit ? std::optional<size_t>(outer_limit + outer_offset)
                      : std::nullopt;

  std::vector<std::shared_ptr<SelectStatement>> flat_branches;
  flat_branches.reserve(branches.size());
  for (const auto& branch : branches) {
    auto aligned_b = std::make_shared<SelectStatement>(*branch);
    std::vector<NamedExpression> b_sel = aligned_b->SelectList();
    for (size_t c = 0; c < arity; ++c) {
      b_sel[c].name = branch0_names[c];
    }
    aligned_b->SetSelectList(std::move(b_sel));

    auto b_outer = std::make_shared<SelectStatement>(*select);
    std::vector<NamedExpression> named_sel = b_outer->SelectList();
    for (size_t i = 0; i < named_sel.size(); ++i) {
      named_sel[i].name = outer_out_names[i];
    }
    b_outer->SetSelectList(std::move(named_sel));
    if (!has_outer_limit) {
      b_outer->SetOrderBy({});
    }
    b_outer->SetLimit(branch_limit);
    b_outer->SetOffset(0);
    std::vector<SelectSource> b_srcs = b_outer->Sources();
    b_srcs[0].query = std::move(aligned_b);
    b_outer->SetSources(std::move(b_srcs));

    if (!FlattenGroupedDerivedJoinCore(b_outer.get())) {
      return false;
    }

    // Strip constant expressions from `b_outer->GroupBy()` as long as at least
    // one non-constant grouping key remains.
    std::vector<Expression> non_const_group;
    non_const_group.reserve(b_outer->GroupBy().size());
    for (const Expression& g : b_outer->GroupBy()) {
      if (g && g->Type() != TypeTag::kConstantValue) {
        non_const_group.push_back(g);
      }
    }
    if (!non_const_group.empty() &&
        non_const_group.size() < b_outer->GroupBy().size()) {
      b_outer->SetGroupBy(std::move(non_const_group));
    }
    flat_branches.push_back(std::move(b_outer));
  }

  bool disjoint_ordered_by_head = false;
  if (has_outer_limit && !merged_outer_order.empty()) {
    const Expression& lead_ord = merged_outer_order[0].expression;
    std::optional<size_t> lead_idx;
    if (lead_ord && lead_ord->Type() == TypeTag::kConstantValue) {
      const Value& ov = lead_ord->AsConstantValue().GetValue();
      if (!ov.IsNull() && ov.type == ValueType::kInt64 &&
          ov.value.int_value >= 1 &&
          static_cast<size_t>(ov.value.int_value) <= outer_out_names.size()) {
        lead_idx = static_cast<size_t>(ov.value.int_value) - 1;
      }
    } else if (lead_ord && lead_ord->Type() == TypeTag::kColumnValue) {
      const ColumnName& oc = lead_ord->AsColumnValue().GetColumnName();
      for (size_t i = 0; i < outer_out_names.size(); ++i) {
        if (oc.schema.empty() &&
            IdentifierEquals(outer_out_names[i], oc.name)) {
          lead_idx = i;
          break;
        }
      }
    }
    if (lead_idx.has_value()) {
      std::vector<std::pair<Value, std::shared_ptr<SelectStatement>>> keyed;
      bool all_lead_const = true;
      for (const auto& fb : flat_branches) {
        const Expression& e = fb->SelectList()[*lead_idx].expression;
        if (!e || e->Type() != TypeTag::kConstantValue ||
            TpcdsExprHasParam(e)) {
          all_lead_const = false;
          break;
        }
        const Value& v = e->AsConstantValue().GetValue();
        if (!IsSafeDisjointTagValue(v) ||
            (!keyed.empty() && v.type != keyed.front().first.type)) {
          all_lead_const = false;
          break;
        }
        keyed.emplace_back(v, fb);
      }
      if (all_lead_const) {
        bool distinct = true;
        for (size_t a = 0; a < keyed.size() && distinct; ++a) {
          for (size_t b = a + 1; b < keyed.size(); ++b) {
            if (keyed[a].first == keyed[b].first) {
              distinct = false;
              break;
            }
          }
        }
        if (distinct) {
          const bool asc = merged_outer_order[0].ascending;
          std::ranges::sort(keyed, [&](const auto& lhs, const auto& rhs) {
            return asc ? (lhs.first < rhs.first) : (rhs.first < lhs.first);
          });
          for (size_t i = 0; i < keyed.size(); ++i) {
            flat_branches[i] = std::move(keyed[i].second);
          }
          disjoint_ordered_by_head = true;
        }
      }
    }
  }

  auto tree = std::make_shared<SetOperationTree>();
  tree->grouped = true;
  tree->disjoint_ordered_by_head = disjoint_ordered_by_head;
  tree->first = flat_branches[0];
  for (size_t b = 1; b < flat_branches.size(); ++b) {
    tree->branches.push_back(flat_branches[b]);
    tree->kinds.push_back(SetOperationKind::kUnionAll);
  }

  *select = *flat_branches[0];
  select->ClearUnionAll();
  for (size_t b = 1; b < flat_branches.size(); ++b) {
    select->AddUnionAll(flat_branches[b]);
  }
  select->SetSetOperationTree(std::move(tree));
  select->SetOrderBy(std::move(merged_outer_order));
  select->SetLimit(has_outer_limit ? std::optional<size_t>(outer_limit)
                                   : std::nullopt);
  select->SetOffset(outer_offset);
  return true;
}

// Lean proof: lean/TinylambOpt/ClickBenchDiscovery.lean
// (group_by_functional_expr_reduction_sound)
bool PruneRedundantGroupByKeys(SelectStatement* select) {
  if (select == nullptr) {
    return false;
  }
  bool changed = false;
  for (const auto& [name, body] : select->WithQueries()) {
    if (!select->IsRecursiveWith(name) && body != nullptr) {
      changed |= PruneRedundantGroupByKeys(body.get());
    }
  }
  for (const SelectSource& src : select->Sources()) {
    if (src.query != nullptr) {
      changed |= PruneRedundantGroupByKeys(src.query.get());
    }
  }
  for (const auto& branch : select->UnionAll()) {
    if (branch != nullptr) {
      changed |= PruneRedundantGroupByKeys(branch.get());
    }
  }
  if (select->IsGroupAll() || select->GroupBy().size() < 2 ||
      select->HasDistinctOn() || select->WithTies()) {
    return changed;
  }

  std::vector<Expression> reduced;
  reduced.reserve(select->GroupBy().size());
  std::unordered_set<std::string> seen_deterministic;
  for (const Expression& key : select->GroupBy()) {
    if (!key) {
      return changed;
    }
    if (key->Type() == TypeTag::kConstantValue) {
      // A bare integer GROUP BY ordinal has already been resolved to its
      // SELECT item by this stage; grouping by a constant cannot split rows,
      // so dropping it is safe as long as at least one real key remains (the
      // all-constant case is guarded below because it changes the empty-input
      // row count).
      continue;
    }
    if (!ContainsQueryExpression(key) && !ContainsParameterSlot(key) &&
        DerivedExpressionIsImmutable(key)) {
      const std::string repr = key->ToString();
      if (!seen_deterministic.insert(repr).second) {
        continue;
      }
    }
    reduced.push_back(key);
  }
  // Never reduce an all-constant GROUP BY (`SELECT COUNT(*) FROM empty GROUP BY
  // 1` emits 0 rows whereas keyless `SELECT COUNT(*) FROM empty` emits 1 row).
  if (!reduced.empty() && reduced.size() < select->GroupBy().size()) {
    select->SetGroupBy(std::move(reduced));
    changed = true;
  }
  return changed;
}

namespace select_rewrite_detail {

// CTE declarations in a stable order: the declared order first, then any
// entry missing from WithQueryOrder in name order.  Iterating WithQueries()
// directly would give an unordered_map-dependent (and thus non-reproducible)
// order for hand-built statements whose WithQueryOrder is empty.
namespace {
std::vector<std::string> OrderedCteNames(const SelectStatement& src) {
  std::vector<std::string> names;
  names.reserve(src.WithQueries().size());
  for (const std::string& name : src.WithQueryOrder()) {
    if (src.WithQueries().contains(name)) {
      names.push_back(name);
    }
  }
  std::vector<std::string> rest;
  for (const auto& [name, body] : src.WithQueries()) {
    (void)body;
    if (std::find(names.begin(), names.end(), name) == names.end()) {
      rest.push_back(name);
    }
  }
  std::sort(rest.begin(), rest.end());
  names.insert(names.end(), rest.begin(), rest.end());
  return names;
}
}  // namespace

namespace {
void CopyCte(SelectStatement* container, const SelectStatement& src,
             const std::string& name) {
  const auto it = src.WithQueries().find(name);
  if (it == src.WithQueries().end()) {
    return;
  }
  if (src.IsRecursiveWith(name)) {
    container->AddRecursiveWithQuery(name, it->second);
  } else {
    container->AddWithQuery(name, it->second);
  }
  if (const RecursiveDepthSpec* depth = src.RecursiveDepthOf(name)) {
    container->SetRecursiveDepth(name, *depth);
  }
}
}  // namespace

void SyncSetOpHeadFields(
    SelectStatement* container, const SelectStatement& head,
    const std::vector<const SelectStatement*>* other_leaves) {
  container->SetSources(head.Sources());
  container->SetWhereClause(head.WhereClause());
  container->SetSelectList(head.SelectList());
  container->SetGroupBy(head.GroupBy());
  container->SetHaving(head.Having());
  container->SetQualify(head.Qualify());
  container->SetDistinct(head.Distinct());
  // A set operation has no single row position per output row, so FOR UPDATE
  // must never be inherited from a leaf: copying it here would bypass the
  // explicit diagnostic that refuses locking reads on set operations.
  container->SetForUpdate(false);
  // Leaf rewrites can add/remove CTE declarations. Keep the container's view
  // in lockstep, carrying declaration order and the recursive/depth markers
  // that a plain AddWithQuery would not preserve. Compare the order too: a
  // rewrite that only reorders declarations leaves the map identical.
  if (container->WithQueries() != head.WithQueries() ||
      container->WithQueryOrder() != head.WithQueryOrder()) {
    container->ClearWithQueries();
    for (const std::string& name : OrderedCteNames(head)) {
      CopyCte(container, head, name);
    }
  }
  // Rewrites of a non-head branch can mint auxiliary CTEs as well; those must
  // be visible in the set-operation container's WITH scope too.
  if (other_leaves != nullptr) {
    for (const SelectStatement* leaf : *other_leaves) {
      if (leaf == nullptr) {
        continue;
      }
      for (const std::string& name : OrderedCteNames(*leaf)) {
        if (!container->WithQueries().contains(name)) {
          CopyCte(container, *leaf, name);
        }
      }
    }
  }
  // ORDER BY / LIMIT / OFFSET, DISTINCT ON and WITH TIES are set-operation
  // container-level clauses: callers set them explicitly after the leaves are
  // rewritten, so they are deliberately not copied from a leaf here.
}

namespace {
bool ExprContainsWindowLocal(const Expression& expr) {
  if (!expr) {
    return false;
  }
  std::vector<Expression> stack{expr};
  while (!stack.empty()) {
    Expression cur = std::move(stack.back());
    stack.pop_back();
    if (!cur) {
      continue;
    }
    if (cur->Type() == TypeTag::kWindowFunctionExp) {
      return true;
    }
    if (cur->Type() == TypeTag::kQueryExp) {
      continue;
    }
    for (const Expression& child : ExpressionChildren(cur)) {
      if (child) {
        stack.push_back(child);
      }
    }
  }
  return false;
}
}  // namespace

namespace {
bool CanonicalizeTwoValuedStmtImpl(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt, std::unordered_set<SelectStatement*>* visited);
}  // namespace

namespace {
Expression CanonicalizeTwoValuedExprImpl(  // NOLINT(misc-no-recursion)
    const Expression& expr, std::unordered_set<SelectStatement*>* visited,
    bool* changed) {
  if (!expr) {
    return expr;
  }
  if (expr->Type() == TypeTag::kQueryExp) {
    const auto& q = expr->AsQueryExpression();
    if (q.Query() != nullptr) {
      if (CanonicalizeTwoValuedStmtImpl(q.Query().get(), visited)) {
        *changed = true;
      }
    }
    if (q.Test()) {
      bool test_changed = false;
      Expression new_test =
          CanonicalizeTwoValuedExprImpl(q.Test(), visited, &test_changed);
      if (test_changed) {
        *changed = true;
        // A kQueryExp exposes only its Test() leg through ExpressionChildren;
        // WithExpressionChildren with this single child rebuilds the
        // QueryExpression with the original Query/Exists/Negated/Op/Mode
        // discriminators intact (no query=nullptr fallback).
        return WithExpressionChildren(expr, {std::move(new_test)});
      }
    }
    return expr;
  }
  std::vector<Expression> children = ExpressionChildren(expr);
  bool child_changed = false;
  for (Expression& c : children) {
    if (c) {
      c = CanonicalizeTwoValuedExprImpl(c, visited, &child_changed);
    }
  }
  Expression rebuilt =
      child_changed ? WithExpressionChildren(expr, std::move(children)) : expr;
  if (child_changed) {
    *changed = true;
  }
  if (rebuilt->Type() == TypeTag::kUnaryExp &&
      rebuilt->AsUnaryExpression().Op() == UnaryOperation::kNot) {
    const Expression& inner = rebuilt->AsUnaryExpression().Child();
    if (inner && inner->Type() == TypeTag::kUnaryExp) {
      std::optional<UnaryOperation> dual;
      switch (inner->AsUnaryExpression().Op()) {
        case UnaryOperation::kIsNull:
          dual = UnaryOperation::kIsNotNull;
          break;
        case UnaryOperation::kIsNotNull:
          dual = UnaryOperation::kIsNull;
          break;
        case UnaryOperation::kIsTrue:
          dual = UnaryOperation::kIsNotTrue;
          break;
        case UnaryOperation::kIsNotTrue:
          dual = UnaryOperation::kIsTrue;
          break;
        case UnaryOperation::kIsFalse:
          dual = UnaryOperation::kIsNotFalse;
          break;
        case UnaryOperation::kIsNotFalse:
          dual = UnaryOperation::kIsFalse;
          break;
        default:
          break;
      }
      if (dual.has_value()) {
        *changed = true;
        return UnaryExpressionExp(inner->AsUnaryExpression().Child(), *dual);
      }
    }
  }
  return rebuilt;
}
}  // namespace

namespace {
bool CanonicalizeTwoValuedLeaf(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt, std::unordered_set<SelectStatement*>* visited) {
  bool changed = false;
  for (const auto& [name, body] : stmt->WithQueries()) {
    (void)name;
    if (body != nullptr) {
      changed |= CanonicalizeTwoValuedStmtImpl(body.get(), visited);
    }
  }
  std::vector<SelectSource> sources = stmt->Sources();
  bool src_changed = false;
  for (SelectSource& src : sources) {
    if (src.query != nullptr) {
      changed |= CanonicalizeTwoValuedStmtImpl(src.query.get(), visited);
    }
    if (src.join_condition) {
      bool jc_changed = false;
      src.join_condition = CanonicalizeTwoValuedExprImpl(src.join_condition,
                                                         visited, &jc_changed);
      src_changed |= jc_changed;
    }
    if (src.unnest) {
      bool un_changed = false;
      src.unnest =
          CanonicalizeTwoValuedExprImpl(src.unnest, visited, &un_changed);
      src_changed |= un_changed;
    }
  }
  if (src_changed) {
    stmt->SetSources(std::move(sources));
    changed = true;
  }
  if (stmt->WhereClause()) {
    bool w_changed = false;
    Expression nw =
        CanonicalizeTwoValuedExprImpl(stmt->WhereClause(), visited, &w_changed);
    if (w_changed) {
      stmt->SetWhereClause(std::move(nw));
      changed = true;
    }
  }
  if (stmt->Having()) {
    bool h_changed = false;
    Expression nh =
        CanonicalizeTwoValuedExprImpl(stmt->Having(), visited, &h_changed);
    if (h_changed) {
      stmt->SetHaving(std::move(nh));
      changed = true;
    }
  }
  if (stmt->Qualify()) {
    bool q_changed = false;
    Expression nq =
        CanonicalizeTwoValuedExprImpl(stmt->Qualify(), visited, &q_changed);
    if (q_changed) {
      stmt->SetQualify(std::move(nq));
      changed = true;
    }
  }
  if (!stmt->SelectList().empty()) {
    std::vector<NamedExpression> sel = stmt->SelectList();
    bool s_changed = false;
    for (NamedExpression& item : sel) {
      if (item.expression) {
        item.expression =
            CanonicalizeTwoValuedExprImpl(item.expression, visited, &s_changed);
      }
    }
    if (s_changed) {
      stmt->SetSelectList(std::move(sel));
      changed = true;
    }
  }
  if (!stmt->GroupBy().empty()) {
    std::vector<Expression> grp = stmt->GroupBy();
    bool g_changed = false;
    for (Expression& g : grp) {
      if (g) {
        g = CanonicalizeTwoValuedExprImpl(g, visited, &g_changed);
      }
    }
    if (g_changed) {
      stmt->SetGroupBy(std::move(grp));
      changed = true;
    }
  }
  if (!stmt->OrderBy().empty()) {
    std::vector<SelectStatement::OrderByTerm> ord = stmt->OrderBy();
    bool o_changed = false;
    for (auto& term : ord) {
      if (term.expression) {
        term.expression =
            CanonicalizeTwoValuedExprImpl(term.expression, visited, &o_changed);
      }
    }
    if (o_changed) {
      stmt->SetOrderBy(std::move(ord));
      changed = true;
    }
  }
  return changed;
}
}  // namespace

namespace {
bool CanonicalizeTwoValuedStmtImpl(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt, std::unordered_set<SelectStatement*>* visited) {
  if (stmt == nullptr) {
    return false;
  }
  bool changed = false;
  if (!stmt->UnionAll().empty() || stmt->GetSetOperationTree() != nullptr) {
    for (const auto& [name, body] : stmt->WithQueries()) {
      (void)name;
      if (body != nullptr) {
        changed |= CanonicalizeTwoValuedStmtImpl(body.get(), visited);
      }
    }
    if (!stmt->OrderBy().empty()) {
      std::vector<SelectStatement::OrderByTerm> ord = stmt->OrderBy();
      bool o_changed = false;
      for (auto& term : ord) {
        if (term.expression) {
          term.expression = CanonicalizeTwoValuedExprImpl(term.expression,
                                                          visited, &o_changed);
        }
      }
      if (o_changed) {
        stmt->SetOrderBy(std::move(ord));
        changed = true;
      }
    }
  }
  changed |= VisitSetOpLeavesAndSync(stmt, visited, [&](SelectStatement* leaf) {
    return CanonicalizeTwoValuedLeaf(leaf, visited);
  });
  return changed;
}
}  // namespace

}  // namespace select_rewrite_detail

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (two_valued_predicate_not_involution_sound, sqlIsNull_never_tNull)
bool CanonicalizeTwoValuedNegations(SelectStatement* select) {
  std::unordered_set<SelectStatement*> visited;
  return CanonicalizeTwoValuedStmtImpl(select, &visited);
}

namespace select_rewrite_detail {

namespace {
bool CanonicalizeAliasGroupByStmtImpl(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt, TransactionContext& ctx,
    const std::unordered_map<std::string, std::shared_ptr<SelectStatement>>&
        enclosing_ctes,
    std::unordered_set<SelectStatement*>* visited);
}  // namespace

namespace {
bool CanonicalizeAliasGroupByLeaf(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt, TransactionContext& ctx,
    const std::unordered_map<std::string, std::shared_ptr<SelectStatement>>&
        enclosing_ctes,
    std::unordered_set<SelectStatement*>* visited) {
  bool changed = false;
  std::unordered_map<std::string, std::shared_ptr<SelectStatement>>
      active_ctes = enclosing_ctes;
  for (const auto& [name, body] : stmt->WithQueries()) {
    if (!stmt->IsRecursiveWith(name) && body != nullptr) {
      changed |= CanonicalizeAliasGroupByStmtImpl(body.get(), ctx, active_ctes,
                                                  visited);
      active_ctes[name] = body;
    }
  }
  for (const SelectSource& src : stmt->Sources()) {
    if (src.query != nullptr && !src.is_lateral) {
      changed |= CanonicalizeAliasGroupByStmtImpl(src.query.get(), ctx,
                                                  active_ctes, visited);
    }
  }
  if (stmt->GroupBy().empty() || stmt->IsGroupAll() || stmt->HasDistinctOn() ||
      stmt->WithTies() || SelectListHasStarOrModifiers(*stmt) ||
      TpcdsStmtHasParam(*stmt) || stmt->Sources().empty()) {
    return changed;
  }

  struct RelCols {
    std::string alias;
    std::string table;
    // A multimap: two output columns whose names differ only by case collapse
    // to the same lowered key, and that must stay *ambiguous* rather than
    // silently losing one of the entries to emplace().
    std::unordered_multimap<std::string, std::string> lowered_to_exact;
  };
  std::vector<RelCols> rels;
  rels.reserve(stmt->Sources().size());
  for (const SelectSource& src : stmt->Sources()) {
    if (src.unnest || src.is_lateral || src.from_nested_join ||
        !src.using_columns.empty()) {
      return changed;
    }
    RelCols rc;
    rc.table = src.table;
    rc.alias = src.alias.empty() ? src.table : src.alias;
    if (rc.alias.empty()) {
      return changed;
    }
    if (src.query != nullptr) {
      if (SelectListHasStarOrModifiers(*src.query)) {
        return changed;
      }
      for (const NamedExpression& item : src.query->SelectList()) {
        const std::string out_nm = ExplicitOrColumnOutputName(item);
        if (out_nm.empty()) {
          return changed;
        }
        rc.lowered_to_exact.emplace(LoweredStarName(out_nm), out_nm);
      }
    } else if (const auto cte_it = active_ctes.find(src.table);
               cte_it != active_ctes.end()) {
      if (cte_it->second == nullptr ||
          SelectListHasStarOrModifiers(*cte_it->second)) {
        return changed;
      }
      for (const NamedExpression& item : cte_it->second->SelectList()) {
        const std::string out_nm = ExplicitOrColumnOutputName(item);
        if (out_nm.empty()) {
          return changed;
        }
        rc.lowered_to_exact.emplace(LoweredStarName(out_nm), out_nm);
      }
    } else {
      StatusOr<std::shared_ptr<Table>> tbl = ctx.GetTable(src.table);
      if (!tbl.HasValue()) {
        return changed;
      }
      const Schema& schema = tbl.Value()->GetSchema();
      for (size_t i = 0; i < schema.ColumnCount(); ++i) {
        const std::string& cn = schema.GetColumn(i).Name().name;
        rc.lowered_to_exact.emplace(LoweredStarName(cn), cn);
      }
    }
    rels.push_back(std::move(rc));
  }

  auto col_resolves_uniquely = [&](const ColumnName& col) -> bool {
    if (col.name.empty() || col.name == "*") {
      return false;
    }
    const std::string low = LoweredStarName(col.name);
    size_t hits = 0;
    for (const RelCols& rc : rels) {
      if (!col.schema.empty()) {
        if (IdentifierEquals(col.schema, rc.alias) ||
            (!rc.table.empty() && IdentifierEquals(col.schema, rc.table))) {
          hits += rc.lowered_to_exact.count(low);
        }
      } else {
        hits += rc.lowered_to_exact.count(low);
      }
    }
    return hits == 1;
  };

  std::vector<Expression> new_group = stmt->GroupBy();
  bool local_changed = false;
  for (Expression& gk : new_group) {
    if (!gk || gk->Type() != TypeTag::kColumnValue) {
      continue;
    }
    const ColumnName& gcol = gk->AsColumnValue().GetColumnName();
    if (!gcol.schema.empty() || gcol.name.empty() || gcol.name == "*") {
      continue;
    }
    const std::string low = LoweredStarName(gcol.name);
    size_t src_hits = 0;
    const RelCols* matched_rel = nullptr;
    std::string exact_col;
    for (const RelCols& rc : rels) {
      const size_t n = rc.lowered_to_exact.count(low);
      if (n == 0) {
        continue;
      }
      // More than one column in the same relation matches the lowered name
      // (e.g. `Foo` and `foo`): the reference is ambiguous, so do not record a
      // single match and let the `src_hits != 1` guard skip qualification.
      src_hits += n;
      if (n == 1) {
        matched_rel = &rc;
        exact_col = rc.lowered_to_exact.find(low)->second;
      }
    }
    if (src_hits == 1) {
      // Base columns win over SELECT aliases in GoogleSQL: qualify when a
      // SELECT item reuses the same alias name for a derived expression (Q08).
      bool shadowed_by_diff_expr = false;
      for (const NamedExpression& item : stmt->SelectList()) {
        if (!item.name.empty() && IdentifierEquals(item.name, gcol.name) &&
            (!item.expression ||
             item.expression->Type() != TypeTag::kColumnValue ||
             !IdentifierEquals(
                 item.expression->AsColumnValue().GetColumnName().name,
                 gcol.name))) {
          shadowed_by_diff_expr = true;
          break;
        }
      }
      if (shadowed_by_diff_expr) {
        gk = ColumnValueExp(ColumnName(matched_rel->alias, exact_col));
        local_changed = true;
      }
      continue;
    }
    if (src_hits == 0) {
      const NamedExpression* alias_item = nullptr;
      size_t alias_hits = 0;
      for (const NamedExpression& item : stmt->SelectList()) {
        if (!item.name.empty() && IdentifierEquals(item.name, gcol.name)) {
          alias_item = &item;
          ++alias_hits;
        }
      }
      if (alias_hits != 1 || alias_item->expression == nullptr ||
          alias_item->expression->Type() == TypeTag::kConstantValue ||
          ContainsQueryExpression(alias_item->expression) ||
          relational_detail::ContainsAggregate(alias_item->expression) ||
          ExprContainsWindowLocal(alias_item->expression) ||
          !DerivedExpressionIsImmutable(alias_item->expression)) {
        continue;
      }
      const auto touched = alias_item->expression->TouchedColumns();
      if (touched.empty() ||
          !std::ranges::all_of(touched, col_resolves_uniquely)) {
        continue;
      }
      if (stmt->Having()) {
        const auto having_cols = stmt->Having()->TouchedColumns();
        const bool having_uses_alias =
            std::ranges::any_of(having_cols, [&](const ColumnName& hc) {
              return hc.schema.empty() && IdentifierEquals(hc.name, gcol.name);
            });
        if (having_uses_alias) {
          continue;
        }
      }
      gk = alias_item->expression;
      local_changed = true;
    }
  }
  if (local_changed) {
    stmt->SetGroupBy(std::move(new_group));
    changed = true;
  }
  return changed;
}
}  // namespace

namespace {
bool CanonicalizeAliasGroupByStmtImpl(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt, TransactionContext& ctx,
    const std::unordered_map<std::string, std::shared_ptr<SelectStatement>>&
        enclosing_ctes,
    std::unordered_set<SelectStatement*>* visited) {
  if (stmt == nullptr) {
    return false;
  }
  bool changed = false;
  std::unordered_map<std::string, std::shared_ptr<SelectStatement>>
      active_ctes = enclosing_ctes;
  const bool is_set_op =
      !stmt->UnionAll().empty() || stmt->GetSetOperationTree() != nullptr;
  for (const auto& [name, body] : stmt->WithQueries()) {
    if (!stmt->IsRecursiveWith(name) && body != nullptr) {
      if (is_set_op) {
        // A set-op container owns its CTEs directly; process them here before
        // the leaf visit (the leaves do not see the container's WITH list).
        changed |= CanonicalizeAliasGroupByStmtImpl(body.get(), ctx,
                                                    active_ctes, visited);
      }
      // Register every non-recursive CTE for the leaf visit, not only the
      // set-op case, so forward references resolve there too. Recursive
      // entries stay opaque (their body is its own work-table scope).
      active_ctes[name] = body;
    }
  }
  changed |= VisitSetOpLeavesAndSync(stmt, visited, [&](SelectStatement* leaf) {
    return CanonicalizeAliasGroupByLeaf(leaf, ctx, active_ctes, visited);
  });
  return changed;
}
}  // namespace

}  // namespace select_rewrite_detail

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (select_alias_group_by_canonicalization_sound)
bool CanonicalizeSelectAliasGroupBy(SelectStatement* select,
                                    TransactionContext& ctx) {
  std::unordered_set<SelectStatement*> visited;
  return CanonicalizeAliasGroupByStmtImpl(select, ctx, {}, &visited);
}

namespace select_rewrite_detail {

namespace {
bool ExprHasGroupContextMod(const Expression& expr) {
  if (!expr) {
    return false;
  }
  std::vector<Expression> stack{expr};
  while (!stack.empty()) {
    Expression cur = std::move(stack.back());
    stack.pop_back();
    if (!cur) {
      continue;
    }
    if (cur->Type() == TypeTag::kWindowFunctionExp) {
      return true;
    }
    if (cur->Type() == TypeTag::kAggregateExp) {
      const auto& agg = cur->AsAggregateExpression();
      if (agg.NeedsGroupContext() || !agg.InnerGroupBy().empty()) {
        return true;
      }
    }
    if (cur->Type() == TypeTag::kQueryExp) {
      continue;
    }
    for (const Expression& child : ExpressionChildren(cur)) {
      if (child) {
        stack.push_back(child);
      }
    }
  }
  return false;
}
}  // namespace

bool ValidateGroupedCteTarget(
    SelectStatement* grouped_stmt,
    std::unordered_map<std::string, ColumnName>* out_to_group_col) {
  if (grouped_stmt == nullptr || grouped_stmt->GroupBy().empty() ||
      grouped_stmt->IsGroupAll() || grouped_stmt->Distinct() ||
      grouped_stmt->HasDistinctOn() || grouped_stmt->HasLimit() ||
      grouped_stmt->Offset() != 0 || grouped_stmt->Qualify() ||
      grouped_stmt->WithTies() || !grouped_stmt->UnionAll().empty() ||
      grouped_stmt->GetSetOperationTree() != nullptr ||
      !grouped_stmt->WithQueries().empty() ||
      relational_detail::HasWindowFunctions(*grouped_stmt) ||
      SelectListHasStarOrModifiers(*grouped_stmt) ||
      TpcdsStmtHasParam(*grouped_stmt) ||
      ExprHasGroupContextMod(grouped_stmt->Having())) {
    return false;
  }
  for (const NamedExpression& item : grouped_stmt->SelectList()) {
    if (ExprHasGroupContextMod(item.expression)) {
      return false;
    }
  }
  for (size_t idx = 0; idx < grouped_stmt->SelectList().size(); ++idx) {
    const NamedExpression& item = grouped_stmt->SelectList()[idx];
    if (!item.expression || item.expression->Type() != TypeTag::kColumnValue) {
      continue;
    }
    const ColumnName& sc = item.expression->AsColumnValue().GetColumnName();
    for (const Expression& gk : grouped_stmt->GroupBy()) {
      if (!gk || gk->Type() != TypeTag::kColumnValue) {
        continue;
      }
      const ColumnName& gc = gk->AsColumnValue().GetColumnName();
      if (IdentifierEquals(sc.name, gc.name) &&
          (sc.schema.empty() || gc.schema.empty() ||
           IdentifierEquals(sc.schema, gc.schema))) {
        const std::string out_nm = InferSelectItemOutputName(item, idx);
        const ColumnName chosen = !gc.schema.empty() ? gc : sc;
        const auto [it, inserted] =
            out_to_group_col->emplace(LoweredStarName(out_nm), chosen);
        if (!inserted && !(it->second == chosen)) {
          // Two output columns share a (lowercased) name but resolve to
          // different GROUP BY columns. Pushing either one's predicate would
          // silently drop rows the other site needs, so refuse the mapping.
          return false;
        }
        break;
      }
    }
  }
  return !out_to_group_col->empty();
}

namespace {
Expression MakeValueDomainPredicate(const ColumnName& col,
                                    const std::vector<Value>& vals) {
  if (vals.size() == 1) {
    return BinaryExpressionExp(ColumnValueExp(col), BinaryOperation::kEquals,
                               ConstantValueExp(vals[0]));
  }
  if (!vals.empty() && !vals[0].IsNull() && !vals[0].IsUnsigned() &&
      (vals[0].type == ValueType::kInt64 || vals[0].type == ValueType::kDate)) {
    bool all_same_int = true;
    int64_t lo = vals[0].value.int_value;
    int64_t hi = vals[0].value.int_value;
    std::vector<int64_t> uniq;
    uniq.reserve(vals.size());
    for (const Value& v : vals) {
      if (v.IsNull() || v.IsUnsigned() || v.type != vals[0].type) {
        all_same_int = false;
        break;
      }
      lo = std::min(lo, v.value.int_value);
      hi = std::max(hi, v.value.int_value);
      if (std::ranges::find(uniq, v.value.int_value) == uniq.end()) {
        uniq.push_back(v.value.int_value);
      }
    }
    if (all_same_int && !uniq.empty()) {
      Value lo_val = vals[0];
      lo_val.value.int_value = lo;
      Value hi_val = vals[0];
      hi_val.value.int_value = hi;
      Expression hull = BinaryExpressionExp(
          BinaryExpressionExp(ColumnValueExp(col),
                              BinaryOperation::kGreaterThanEquals,
                              ConstantValueExp(lo_val)),
          BinaryOperation::kAnd,
          BinaryExpressionExp(ColumnValueExp(col),
                              BinaryOperation::kLessThanEquals,
                              ConstantValueExp(hi_val)));
      if (hi >= lo &&
          static_cast<uint64_t>(hi) - static_cast<uint64_t>(lo) + 1ULL ==
              uniq.size()) {
        return hull;
      }
      std::vector<Expression> list;
      list.reserve(vals.size());
      for (const Value& v : vals) {
        list.push_back(ConstantValueExp(v));
      }
      return BinaryExpressionExp(
          std::move(hull), BinaryOperation::kAnd,
          InExpressionExp(ColumnValueExp(col), std::move(list)));
    }
  }
  std::vector<Expression> list;
  list.reserve(vals.size());
  for (const Value& v : vals) {
    list.push_back(ConstantValueExp(v));
  }
  return InExpressionExp(ColumnValueExp(col), std::move(list));
}
}  // namespace

namespace {
void PushPredicateIntoUnionQueryIfSafe(
    SelectStatement* u_stmt, std::string_view target_out_col,
    const std::vector<Value>& domain_vals,
    std::optional<std::pair<Value, Value>> closed_range,
    TransactionContext& ctx) {
  if (u_stmt == nullptr) {
    return;
  }
  std::string cur_col(target_out_col);
  while (
      u_stmt != nullptr && u_stmt->UnionAll().empty() &&
      u_stmt->GetSetOperationTree() == nullptr &&
      u_stmt->Sources().size() == 1 && u_stmt->Sources()[0].query != nullptr &&
      !u_stmt->Sources()[0].unnest && !u_stmt->Sources()[0].is_lateral &&
      !u_stmt->Sources()[0].sample &&
      u_stmt->Sources()[0].using_columns.empty() &&
      !u_stmt->Sources()[0].from_nested_join &&
      !u_stmt->Sources()[0].join_condition && !u_stmt->WhereClause() &&
      u_stmt->GroupBy().empty() && !u_stmt->IsGroupAll() && !u_stmt->Having() &&
      !u_stmt->Distinct() && !u_stmt->HasDistinctOn() && !u_stmt->HasLimit() &&
      u_stmt->Offset() == 0 && !u_stmt->Qualify() &&
      u_stmt->WithQueries().empty() && !SelectListHasStarOrModifiers(*u_stmt)) {
    const std::string sub_al = u_stmt->Sources()[0].alias;
    std::string mapped_inner;
    for (size_t idx = 0; idx < u_stmt->SelectList().size(); ++idx) {
      const NamedExpression& item = u_stmt->SelectList()[idx];
      if (!IdentifierEquals(InferSelectItemOutputName(item, idx), cur_col)) {
        continue;
      }
      if (!item.expression ||
          item.expression->Type() != TypeTag::kColumnValue) {
        return;
      }
      const ColumnName& cn = item.expression->AsColumnValue().GetColumnName();
      if (!cn.schema.empty() && !sub_al.empty() &&
          !IdentifierEquals(cn.schema, sub_al)) {
        return;
      }
      mapped_inner = cn.name;
      break;
    }
    if (mapped_inner.empty()) {
      return;
    }
    cur_col = std::move(mapped_inner);
    u_stmt = u_stmt->Sources()[0].query.get();
  }
  if (u_stmt == nullptr ||
      (u_stmt->UnionAll().empty() &&
       u_stmt->GetSetOperationTree() == nullptr) ||
      u_stmt->UnionByName() || u_stmt->HasLimit() || u_stmt->Offset() != 0 ||
      !u_stmt->OrderBy().empty() || !u_stmt->WithQueries().empty()) {
    return;
  }
  for (const SetOperationMatch& m : u_stmt->Matches()) {
    if (m.by_name || m.corresponding || !m.columns.empty()) {
      return;
    }
  }
  for (SetOperationKind k : u_stmt->SetOperationKinds()) {
    if (k != SetOperationKind::kUnionAll && k != SetOperationKind::kUnion) {
      return;
    }
  }
  if (const auto& tree = u_stmt->GetSetOperationTree(); tree != nullptr) {
    for (SetOperationKind k : tree->kinds) {
      if (k != SetOperationKind::kUnionAll && k != SetOperationKind::kUnion) {
        return;
      }
    }
  }

  std::vector<SelectStatement*> leaves;
  std::unordered_set<SelectStatement*> seen;
  auto collect_leaves = [&](auto& self, SelectStatement* s) -> bool {
    while (
        s != nullptr && s->UnionAll().empty() &&
        s->GetSetOperationTree() == nullptr && s->Sources().size() == 1 &&
        s->Sources()[0].query != nullptr && !s->Sources()[0].unnest &&
        !s->Sources()[0].is_lateral && !s->Sources()[0].sample &&
        s->Sources()[0].using_columns.empty() &&
        !s->Sources()[0].from_nested_join && !s->Sources()[0].join_condition &&
        !s->WhereClause() && s->GroupBy().empty() && !s->IsGroupAll() &&
        !s->Having() && !s->Distinct() && !s->HasDistinctOn() &&
        !s->HasLimit() && s->Offset() == 0 && !s->Qualify() &&
        s->WithQueries().empty() && s->SelectList().size() == 1 &&
        s->SelectList()[0].expression &&
        s->SelectList()[0].expression->Type() == TypeTag::kColumnValue &&
        s->SelectList()[0].expression->AsColumnValue().GetColumnName().name ==
            "*") {
      s = s->Sources()[0].query.get();
    }
    if (s == nullptr) {
      return false;
    }
    if (!seen.insert(s).second) {
      return true;
    }
    if (s->UnionAll().empty() && s->GetSetOperationTree() == nullptr) {
      leaves.push_back(s);
      return true;
    }
    if (const auto& tree = s->GetSetOperationTree(); tree != nullptr) {
      for (SetOperationKind k : tree->kinds) {
        if (k != SetOperationKind::kUnionAll && k != SetOperationKind::kUnion) {
          return false;
        }
      }
      if (!self(self, tree->first.get())) {
        return false;
      }
      for (const auto& br : tree->branches) {
        if (!self(self, br.get())) {
          return false;
        }
      }
    } else {
      for (SetOperationKind k : s->SetOperationKinds()) {
        if (k != SetOperationKind::kUnionAll && k != SetOperationKind::kUnion) {
          return false;
        }
      }
      leaves.push_back(s);
    }
    for (const auto& br : s->UnionAll()) {
      if (!self(self, br.get())) {
        return false;
      }
    }
    return true;
  };
  if (!collect_leaves(collect_leaves, u_stmt) || leaves.empty()) {
    return;
  }

  std::optional<size_t> col_idx;
  for (size_t c = 0; c < leaves[0]->SelectList().size(); ++c) {
    const std::string out_nm =
        ExplicitOrColumnOutputName(leaves[0]->SelectList()[c]);
    if (!out_nm.empty() && IdentifierEquals(out_nm, cur_col)) {
      if (col_idx.has_value()) {
        return;
      }
      col_idx = c;
    }
  }
  if (!col_idx.has_value()) {
    return;
  }

  std::vector<ColumnName> branch_cols;
  branch_cols.reserve(leaves.size());
  for (SelectStatement* br : leaves) {
    // Both the flat UnionAll and the SetOperationTree paths funnel through
    // collect_leaves, so this guard (including ORDER BY, which previously was
    // only checked on the set-op container) must cover every leaf.
    if (!br->GroupBy().empty() || br->IsGroupAll() || br->Having() ||
        br->HasLimit() || br->Offset() != 0 || !br->OrderBy().empty() ||
        br->Qualify() || br->WithTies() || !br->WithQueries().empty() ||
        relational_detail::HasWindowFunctions(*br) ||
        SelectListHasStarOrModifiers(*br) ||
        *col_idx >= br->SelectList().size()) {
      return;
    }
    const Expression& bexpr = br->SelectList()[*col_idx].expression;
    if (!bexpr || bexpr->Type() != TypeTag::kColumnValue) {
      return;
    }
    const ColumnName& bcol = bexpr->AsColumnValue().GetColumnName();
    if (bcol.name.empty() || bcol.name == "*") {
      return;
    }
    size_t hits = 0;
    ColumnName resolved_bcol = bcol;
    for (const SelectSource& bs : br->Sources()) {
      if (bs.table.empty() || bs.query != nullptr || bs.unnest ||
          bs.is_lateral) {
        return;
      }
      StatusOr<std::shared_ptr<Table>> btbl = ctx.GetTable(bs.table);
      if (!btbl.HasValue()) {
        return;
      }
      const std::string balias = bs.alias.empty() ? bs.table : bs.alias;
      if (!bcol.schema.empty() && !IdentifierEquals(bcol.schema, balias) &&
          !IdentifierEquals(bcol.schema, bs.table)) {
        continue;
      }
      const Schema& bschema = btbl.Value()->GetSchema();
      for (size_t i = 0; i < bschema.ColumnCount(); ++i) {
        if (IdentifierEquals(bschema.GetColumn(i).Name().name, bcol.name)) {
          if (bs.join_type != JoinType::kCross &&
              bs.join_type != JoinType::kInner) {
            return;
          }
          ++hits;
          resolved_bcol = ColumnName(balias, bschema.GetColumn(i).Name().name);
          break;
        }
      }
    }
    if (hits != 1) {
      return;
    }
    branch_cols.push_back(std::move(resolved_bcol));
  }

  for (size_t i = 0; i < leaves.size(); ++i) {
    Expression pred =
        closed_range.has_value()
            ? BinaryExpressionExp(
                  BinaryExpressionExp(ColumnValueExp(branch_cols[i]),
                                      BinaryOperation::kGreaterThanEquals,
                                      ConstantValueExp(closed_range->first)),
                  BinaryOperation::kAnd,
                  BinaryExpressionExp(ColumnValueExp(branch_cols[i]),
                                      BinaryOperation::kLessThanEquals,
                                      ConstantValueExp(closed_range->second)))
            : MakeValueDomainPredicate(branch_cols[i], domain_vals);
    Expression new_where;
    if (leaves[i]->WhereClause()) {
      new_where = BinaryExpressionExp(leaves[i]->WhereClause(),
                                      BinaryOperation::kAnd, std::move(pred));
    } else {
      new_where = std::move(pred);
    }
    leaves[i]->SetWhereClause(std::move(new_where));
  }
  const auto& tree = u_stmt->GetSetOperationTree();
  if (tree != nullptr && !tree->grouped && tree->first != nullptr &&
      tree->first->UnionAll().empty() &&
      tree->first->GetSetOperationTree() == nullptr) {
    SyncSetOpHeadFields(u_stmt, *tree->first);
  }
}
}  // namespace

namespace {
void PushDomainIntoUnionDerivedIfSafe(SelectStatement* outer_with,
                                      SelectStatement* grouped_stmt,
                                      const ColumnName& gk_col,
                                      const std::vector<Value>& domain_vals,
                                      TransactionContext& ctx) {
  auto resolve_single_use_union_target =
      [&](const SelectSource& src) -> SelectStatement* {
    if (src.query != nullptr) {
      return src.query.get();
    }
    if (outer_with == nullptr || src.table.empty() ||
        outer_with->IsRecursiveWith(src.table) ||
        outer_with->RecursiveDepthOf(src.table) != nullptr) {
      return nullptr;
    }
    const auto wit = outer_with->WithQueries().find(src.table);
    if (wit == outer_with->WithQueries().end() || wit->second == nullptr ||
        CountNestedCteReferences(*outer_with, src.table, true, true) != 1) {
      return nullptr;
    }
    return wit->second.get();
  };

  if (grouped_stmt->Sources().size() == 1) {
    const SelectSource& u_src = grouped_stmt->Sources()[0];
    SelectStatement* u_target = resolve_single_use_union_target(u_src);
    const std::string ualias = u_src.alias.empty() ? u_src.table : u_src.alias;
    if (u_target == nullptr || u_src.unnest || u_src.is_lateral ||
        u_src.sample || !u_src.using_columns.empty() ||
        u_src.from_nested_join || u_src.join_condition) {
      return;
    }
    if (!gk_col.schema.empty() && !ualias.empty() &&
        !IdentifierEquals(gk_col.schema, ualias) &&
        (u_src.table.empty() ||
         !IdentifierEquals(gk_col.schema, u_src.table))) {
      return;
    }
    PushPredicateIntoUnionQueryIfSafe(u_target, gk_col.name, domain_vals,
                                      std::nullopt, ctx);
    return;
  }
  if (grouped_stmt->Sources().size() != 2) {
    return;
  }
  // 2-source grouped CTE `FROM union_sub u, dim d WHERE d.pk = u.fk AND d.gk IN
  // (...)` (e.g. TPC-DS Q02 `wswscs`): derive `[min_pk, max_pk]` on `d` and
  // push the SARGable range filter into `u`'s UNION branches.
  size_t u_si = 2;
  size_t d_si = 2;
  SelectStatement* u_target = nullptr;
  for (size_t si = 0; si < 2; ++si) {
    const SelectSource& s = grouped_stmt->Sources()[si];
    if (s.unnest || s.is_lateral || s.sample || !s.using_columns.empty() ||
        s.from_nested_join ||
        (s.join_type != JoinType::kCross && s.join_type != JoinType::kInner)) {
      return;
    }
    if (SelectStatement* cand = resolve_single_use_union_target(s);
        cand != nullptr && u_si >= 2) {
      u_si = si;
      u_target = cand;
    } else if (!s.table.empty() &&
               (outer_with == nullptr ||
                !outer_with->WithQueries().contains(s.table))) {
      d_si = si;
    }
  }
  if (u_si >= 2 || d_si >= 2 || u_target == nullptr) {
    return;
  }
  const SelectSource& u_src = grouped_stmt->Sources()[u_si];
  const SelectSource& d_src = grouped_stmt->Sources()[d_si];
  StatusOr<std::shared_ptr<Table>> dtbl_or = ctx.GetTable(d_src.table);
  if (!dtbl_or.HasValue() || dtbl_or.Value() == nullptr ||
      dtbl_or.Value()->BackingColumnarRowCount() > 100000) {
    return;
  }
  const Schema& dschema = dtbl_or.Value()->GetSchema();
  const std::string dalias = d_src.alias.empty() ? d_src.table : d_src.alias;
  const std::string ualias = u_src.alias.empty() ? u_src.table : u_src.alias;
  if ((!gk_col.schema.empty() && !IdentifierEquals(gk_col.schema, dalias) &&
       !IdentifierEquals(gk_col.schema, d_src.table)) ||
      dschema.Offset(ColumnName("", gk_col.name)) < 0) {
    return;
  }
  std::vector<Expression> all_conjs =
      SplitConjuncts(grouped_stmt->WhereClause());
  all_conjs.reserve(all_conjs.size() + grouped_stmt->Sources().size());
  for (const SelectSource& gs : grouped_stmt->Sources()) {
    if (gs.join_condition) {
      for (Expression& jc : SplitConjuncts(gs.join_condition)) {
        all_conjs.push_back(std::move(jc));
      }
    }
  }
  std::string dim_pk_col;
  std::string union_fk_col;
  for (const Expression& conj : all_conjs) {
    if (!conj || conj->Type() != TypeTag::kBinaryExp ||
        conj->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
      continue;
    }
    const Expression& l = conj->AsBinaryExpression().Left();
    const Expression& r = conj->AsBinaryExpression().Right();
    if (!l || !r || l->Type() != TypeTag::kColumnValue ||
        r->Type() != TypeTag::kColumnValue) {
      continue;
    }
    const ColumnName& lc = l->AsColumnValue().GetColumnName();
    const ColumnName& rc = r->AsColumnValue().GetColumnName();
    auto is_dim_int_col = [&](const ColumnName& cn, std::string* out) -> bool {
      if (!cn.schema.empty() && !IdentifierEquals(cn.schema, dalias) &&
          !IdentifierEquals(cn.schema, d_src.table)) {
        return false;
      }
      const int off = dschema.Offset(ColumnName("", cn.name));
      if (off < 0) {
        return false;
      }
      const Column& cm = dschema.GetColumn(static_cast<size_t>(off));
      if (cm.IsUnsigned() ||
          (cm.Type() != ValueType::kInt64 && cm.Type() != ValueType::kDate)) {
        return false;
      }
      *out = cm.Name().name;
      return true;
    };
    auto is_union_col = [&](const ColumnName& cn, std::string* out) -> bool {
      if (!cn.schema.empty() && !ualias.empty() &&
          !IdentifierEquals(cn.schema, ualias) &&
          (u_src.table.empty() || !IdentifierEquals(cn.schema, u_src.table))) {
        return false;
      }
      if (cn.schema.empty() && dschema.Offset(ColumnName("", cn.name)) >= 0) {
        return false;
      }
      *out = cn.name;
      return !out->empty();
    };
    std::string d_cand;
    std::string u_cand;
    if (is_dim_int_col(lc, &d_cand) && is_union_col(rc, &u_cand)) {
      dim_pk_col = std::move(d_cand);
      union_fk_col = std::move(u_cand);
      break;
    }
    if (is_dim_int_col(rc, &d_cand) && is_union_col(lc, &u_cand)) {
      dim_pk_col = std::move(d_cand);
      union_fk_col = std::move(u_cand);
      break;
    }
  }
  if (dim_pk_col.empty() || union_fk_col.empty()) {
    return;
  }
  SelectStatement d_bounds;
  SelectSource dscan;
  dscan.table = d_src.table;
  dscan.alias = dalias;
  d_bounds.SetSources({std::move(dscan)});
  d_bounds.SetWhereClause(
      MakeValueDomainPredicate(ColumnName(dalias, gk_col.name), domain_vals));
  const Expression pk_exp = ColumnValueExp(ColumnName(dalias, dim_pk_col));
  d_bounds.SetSelectList({
      NamedExpression(
          "__lo", AggregateExpressionExp(AggregationType::kMin, pk_exp, false)),
      NamedExpression(
          "__hi", AggregateExpressionExp(AggregationType::kMax, pk_exp, false)),
  });
  StatusOr<relational_detail::Relation> brel = relational_detail::ExecuteQuery(
      ctx, d_bounds, nullptr, {}, nullptr, false);
  if (brel.HasValue() && brel.Value().HasColumnarOnly()) {
    if (brel.Value().EnsureRowsMaterialized() != Status::kSuccess) {
      return;
    }
  }
  if (!brel.HasValue() || brel.Value().HasSpill() ||
      brel.Value().rows.size() != 1 || brel.Value().rows[0].Count() != 2 ||
      brel.Value().rows[0][0].IsNull() || brel.Value().rows[0][1].IsNull()) {
    return;
  }
  PushPredicateIntoUnionQueryIfSafe(
      u_target, union_fk_col, {},
      std::pair{brel.Value().rows[0][0], brel.Value().rows[0][1]}, ctx);
}
}  // namespace

namespace {
bool CollectSharedGroupedCteSitesInConsumer(  // NOLINT(misc-no-recursion)
    SelectStatement* consumer, const std::string& name,
    std::vector<SharedCteSite>* sites) {
  if (consumer == nullptr || !consumer->UnionAll().empty() ||
      consumer->GetSetOperationTree() != nullptr) {
    return false;
  }
  size_t non_src_refs = 0;
  CountNestedCteReferencesInExpression(consumer->WhereClause(), name,
                                       &non_src_refs);
  for (const NamedExpression& item : consumer->SelectList()) {
    CountNestedCteReferencesInExpression(item.expression, name, &non_src_refs);
  }
  for (const Expression& gk : consumer->GroupBy()) {
    CountNestedCteReferencesInExpression(gk, name, &non_src_refs);
  }
  CountNestedCteReferencesInExpression(consumer->Having(), name, &non_src_refs);
  CountNestedCteReferencesInExpression(consumer->Qualify(), name,
                                       &non_src_refs);
  for (const auto& ot : consumer->OrderBy()) {
    CountNestedCteReferencesInExpression(ot.expression, name, &non_src_refs);
  }
  if (non_src_refs != 0) {
    return false;
  }
  for (const SelectSource& source : consumer->Sources()) {
    size_t cond_refs = 0;
    CountNestedCteReferencesInExpression(source.join_condition, name,
                                         &cond_refs);
    CountNestedCteReferencesInExpression(source.unnest, name, &cond_refs);
    if (cond_refs != 0) {
      return false;
    }
    if (source.query != nullptr) {
      if (CountNestedCteReferences(*source.query, name, true, true) == 0) {
        continue;
      }
      if (source.unnest || source.is_lateral || source.sample.has_value() ||
          !source.using_columns.empty() || source.from_nested_join ||
          source.query->WithQueries().contains(name) ||
          (source.join_type != JoinType::kCross &&
           source.join_type != JoinType::kInner)) {
        return false;
      }
      if (!CollectSharedGroupedCteSitesInConsumer(source.query.get(), name,
                                                  sites)) {
        return false;
      }
      continue;
    }
    if (source.table != name) {
      continue;
    }
    if (source.unnest || source.is_lateral || source.sample.has_value() ||
        (source.join_type != JoinType::kCross &&
         source.join_type != JoinType::kInner)) {
      return false;
    }
    sites->push_back(
        SharedCteSite{.consumer = consumer,
                      .alias = source.alias.empty() ? name : source.alias});
  }
  return true;
}
}  // namespace

namespace {
bool CollectSharedGroupedCteSites(SelectStatement* outer,
                                  const std::string& name,
                                  std::vector<SharedCteSite>* sites) {
  if (!CollectSharedGroupedCteSitesInConsumer(outer, name, sites)) {
    return false;
  }
  return std::ranges::all_of(outer->WithQueries(), [&](const auto& item) {
    const auto& [entry, body] = item;
    if (entry == name || body == nullptr) {
      return true;
    }
    if (CountNestedCteReferences(*body, name, true, true) == 0) {
      return true;
    }
    return !body->WithQueries().contains(name) &&
           CollectSharedGroupedCteSitesInConsumer(body.get(), name, sites);
  });
}
}  // namespace

}  // namespace select_rewrite_detail

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (shared_grouped_union_distinct_cte_disjunctive_pushdown_sound)
bool PushSharedGroupedCteKeyRestrictions(SelectStatement* outer,
                                         TransactionContext& ctx) {
  if (outer == nullptr || outer->WithQueries().empty() ||
      TpcdsStmtHasParam(*outer)) {
    return false;
  }
  bool any_pushed = false;
  std::vector<std::string> cte_names = outer->WithQueryOrder();
  for (const std::string& name : cte_names) {
    if (outer->IsRecursiveWith(name) ||
        outer->RecursiveDepthOf(name) != nullptr) {
      continue;
    }
    const auto cte_it = outer->WithQueries().find(name);
    if (cte_it == outer->WithQueries().end() || cte_it->second == nullptr) {
      continue;
    }
    SelectStatement& body = *cte_it->second;
    if (TpcdsStmtHasParam(body) || !body.WithQueries().empty()) {
      continue;
    }

    std::vector<SharedCteSite> sites;
    if (!CollectSharedGroupedCteSites(outer, name, &sites) ||
        sites.size() < 2) {
      continue;
    }
    bool consumers_safe = true;
    for (const SharedCteSite& site : sites) {
      if (!site.consumer->UnionAll().empty() ||
          site.consumer->GetSetOperationTree() != nullptr ||
          TpcdsStmtHasParam(*site.consumer)) {
        consumers_safe = false;
        break;
      }
      for (const SelectSource& cs : site.consumer->Sources()) {
        if (cs.join_type == JoinType::kRight ||
            cs.join_type == JoinType::kFull) {
          consumers_safe = false;
          break;
        }
      }
      if (!consumers_safe) {
        break;
      }
    }
    if (!consumers_safe) {
      continue;
    }

    SelectStatement* grouped_target = nullptr;
    std::unordered_map<std::string, ColumnName> cte_out_to_group_col;
    if (!body.GroupBy().empty()) {
      if (ValidateGroupedCteTarget(&body, &cte_out_to_group_col)) {
        grouped_target = &body;
      }
    } else if (!body.Having() && !body.Distinct() && !body.HasDistinctOn() &&
               !body.HasLimit() && body.Offset() == 0 && !body.Qualify() &&
               !body.WithTies() && body.UnionAll().empty() &&
               body.GetSetOperationTree() == nullptr &&
               !relational_detail::HasWindowFunctions(body) &&
               !SelectListHasStarOrModifiers(body) &&
               body.Sources().size() == 1 &&
               (!body.WhereClause() ||
                (!ContainsQueryExpression(body.WhereClause()) &&
                 !relational_detail::ContainsAggregate(body.WhereClause()) &&
                 DerivedExpressionIsImmutable(body.WhereClause())))) {
      const SelectSource& s0 = body.Sources()[0];
      const bool clean_select = std::ranges::all_of(
          body.SelectList(), [](const NamedExpression& item) {
            return item.expression != nullptr &&
                   !ContainsQueryExpression(item.expression) &&
                   !relational_detail::ContainsAggregate(item.expression) &&
                   DerivedExpressionIsImmutable(item.expression);
          });
      if (clean_select && s0.query != nullptr && !s0.alias.empty() &&
          !s0.unnest && !s0.is_lateral && !s0.sample &&
          s0.using_columns.empty() && !s0.from_nested_join &&
          !s0.join_condition) {
        std::unordered_map<std::string, ColumnName> inner_map;
        if (ValidateGroupedCteTarget(s0.query.get(), &inner_map)) {
          for (size_t idx = 0; idx < body.SelectList().size(); ++idx) {
            const NamedExpression& item = body.SelectList()[idx];
            if (item.expression->Type() != TypeTag::kColumnValue) {
              continue;
            }
            const ColumnName& col =
                item.expression->AsColumnValue().GetColumnName();
            if (!col.schema.empty() &&
                !IdentifierEquals(col.schema, s0.alias)) {
              continue;
            }
            if (const auto it = inner_map.find(LoweredStarName(col.name));
                it != inner_map.end()) {
              cte_out_to_group_col.emplace(
                  LoweredStarName(InferSelectItemOutputName(item, idx)),
                  it->second);
            }
          }
          if (!cte_out_to_group_col.empty()) {
            grouped_target = s0.query.get();
          }
        }
      }
    }
    if (grouped_target == nullptr || cte_out_to_group_col.empty()) {
      continue;
    }

    std::vector<std::string> candidate_keys;
    candidate_keys.reserve(cte_out_to_group_col.size());
    for (const auto& [k, v] : cte_out_to_group_col) {
      (void)v;
      candidate_keys.push_back(k);
    }
    std::ranges::sort(candidate_keys);

    for (const std::string& out_low : candidate_keys) {
      const ColumnName& gk_col = cte_out_to_group_col.at(out_low);
      std::vector<Value> union_vals;
      bool all_sites_bound = true;

      auto matches_site_col = [&](const Expression& e,
                                  const SharedCteSite& site) -> bool {
        if (!e || e->Type() != TypeTag::kColumnValue) {
          return false;
        }
        const ColumnName& cn = e->AsColumnValue().GetColumnName();
        if (!IdentifierEquals(cn.name, out_low)) {
          return false;
        }
        if (!cn.schema.empty()) {
          return IdentifierEquals(cn.schema, site.alias);
        }
        return site.consumer->Sources().size() == 1;
      };

      for (const SharedCteSite& site : sites) {
        std::vector<Value> site_vals;
        for (const Expression& conj :
             SplitConjuncts(site.consumer->WhereClause())) {
          if (!conj) {
            continue;
          }
          if (conj->Type() == TypeTag::kBinaryExp &&
              conj->AsBinaryExpression().Op() == BinaryOperation::kEquals) {
            const Expression& lhs = conj->AsBinaryExpression().Left();
            const Expression& rhs = conj->AsBinaryExpression().Right();
            std::optional<Value> cv;
            if (matches_site_col(lhs, site)) {
              cv = TpcdsEvalClosedConstant(rhs);
            } else if (matches_site_col(rhs, site)) {
              cv = TpcdsEvalClosedConstant(lhs);
            }
            if (cv.has_value() && IsSafeDisjointTagValue(*cv)) {
              site_vals.push_back(std::move(*cv));
              break;
            }
          } else if (conj->Type() == TypeTag::kInExp) {
            const auto& in_exp = conj->AsInExpression();
            if (matches_site_col(in_exp.child_, site) &&
                !in_exp.list_.empty() && in_exp.list_.size() <= 32) {
              std::vector<Value> parsed;
              bool ok = true;
              for (const Expression& item_e : in_exp.list_) {
                std::optional<Value> cv = TpcdsEvalClosedConstant(item_e);
                if (!cv.has_value() || !IsSafeDisjointTagValue(*cv) ||
                    (!parsed.empty() && cv->type != parsed.front().type)) {
                  ok = false;
                  break;
                }
                parsed.push_back(std::move(*cv));
              }
              if (ok && !parsed.empty()) {
                site_vals = std::move(parsed);
                break;
              }
            }
          }
        }
        if (site_vals.empty() && site.consumer->Sources().size() == 2) {
          // Extract domain from an inner-joined selective dimension table
          // (e.g. TPC-DS Q02 `FROM wswscs, date_dim WHERE d_week_seq = ... AND
          // d_year = 2001`).
          size_t dim_si = 2;
          for (size_t si = 0; si < 2; ++si) {
            const SelectSource& s = site.consumer->Sources()[si];
            const std::string al = s.alias.empty() ? s.table : s.alias;
            if (!IdentifierEquals(al, site.alias) && !s.table.empty() &&
                s.query == nullptr && !s.unnest && !s.is_lateral && !s.sample &&
                s.using_columns.empty() && !s.from_nested_join &&
                (s.join_type == JoinType::kCross ||
                 s.join_type == JoinType::kInner)) {
              dim_si = si;
            }
          }
          if (dim_si < 2) {
            const SelectSource& dsrc = site.consumer->Sources()[dim_si];
            StatusOr<std::shared_ptr<Table>> dtbl_or = ctx.GetTable(dsrc.table);
            if (dtbl_or.HasValue() && dtbl_or.Value() != nullptr &&
                dtbl_or.Value()->BackingColumnarRowCount() <= 100000) {
              const Schema& dschema = dtbl_or.Value()->GetSchema();
              const std::string dalias =
                  dsrc.alias.empty() ? dsrc.table : dsrc.alias;
              std::string matched_dim_col;
              std::vector<Expression> dim_local_preds;
              std::vector<Expression> site_conjs =
                  SplitConjuncts(site.consumer->WhereClause());
              site_conjs.reserve(site_conjs.size() +
                                 site.consumer->Sources().size());
              for (const SelectSource& cs : site.consumer->Sources()) {
                if (cs.join_condition) {
                  for (Expression& jc : SplitConjuncts(cs.join_condition)) {
                    site_conjs.push_back(std::move(jc));
                  }
                }
              }
              for (const Expression& conj : site_conjs) {
                if (!conj || ContainsQueryExpression(conj) ||
                    relational_detail::ContainsAggregate(conj) ||
                    !DerivedExpressionIsImmutable(conj)) {
                  continue;
                }
                if (conj->Type() == TypeTag::kBinaryExp &&
                    conj->AsBinaryExpression().Op() ==
                        BinaryOperation::kEquals) {
                  const Expression& l = conj->AsBinaryExpression().Left();
                  const Expression& r = conj->AsBinaryExpression().Right();
                  auto extract_dcol =
                      [&](const Expression& side) -> std::string {
                    if (!side || side->Type() != TypeTag::kColumnValue) {
                      return {};
                    }
                    const ColumnName& cn =
                        side->AsColumnValue().GetColumnName();
                    if (!cn.schema.empty() &&
                        !IdentifierEquals(cn.schema, dalias) &&
                        !IdentifierEquals(cn.schema, dsrc.table)) {
                      return {};
                    }
                    const int doff = dschema.Offset(ColumnName("", cn.name));
                    return doff >= 0
                               ? dschema.GetColumn(static_cast<size_t>(doff))
                                     .Name()
                                     .name
                               : std::string{};
                  };
                  if (matches_site_col(l, site)) {
                    std::string dc = extract_dcol(r);
                    if (!dc.empty()) {
                      matched_dim_col = std::move(dc);
                      continue;
                    }
                  } else if (matches_site_col(r, site)) {
                    std::string dc = extract_dcol(l);
                    if (!dc.empty()) {
                      matched_dim_col = std::move(dc);
                      continue;
                    }
                  }
                }
                bool only_dim = true;
                bool any_dim = false;
                for (const ColumnName& cn : conj->TouchedColumns()) {
                  if (!cn.schema.empty()) {
                    if (IdentifierEquals(cn.schema, dalias) ||
                        IdentifierEquals(cn.schema, dsrc.table)) {
                      any_dim = true;
                    } else {
                      only_dim = false;
                      break;
                    }
                  } else if (dschema.Offset(ColumnName("", cn.name)) >= 0 &&
                             !IdentifierEquals(cn.name, out_low)) {
                    any_dim = true;
                  } else {
                    only_dim = false;
                    break;
                  }
                }
                if (only_dim && any_dim) {
                  dim_local_preds.push_back(conj);
                }
              }
              if (!matched_dim_col.empty() && !dim_local_preds.empty()) {
                SelectStatement dquery;
                SelectSource dscan;
                dscan.table = dsrc.table;
                dscan.alias = dalias;
                dquery.SetSources({std::move(dscan)});
                dquery.SetWhereClause(CombineConjuncts(dim_local_preds));
                dquery.SetDistinct(true);
                dquery.SetSelectList({NamedExpression(
                    "__dk",
                    ColumnValueExp(ColumnName(dalias, matched_dim_col)))});
                StatusOr<relational_detail::Relation> drel_or =
                    relational_detail::ExecuteQuery(ctx, dquery, nullptr, {},
                                                    nullptr, false);
                if (drel_or.HasValue() && drel_or.Value().HasColumnarOnly()) {
                  if (drel_or.Value().EnsureRowsMaterialized() !=
                      Status::kSuccess) {
                    return false;
                  }
                }
                if (drel_or.HasValue() && !drel_or.Value().HasSpill() &&
                    drel_or.Value().TotalRows() > 0 &&
                    drel_or.Value().TotalRows() <= 128) {
                  std::vector<Value> extracted;
                  bool ext_ok = true;
                  for (const Row& r : drel_or.Value().rows) {
                    if (r.Count() != 1 || r[0].IsNull()) {
                      continue;
                    }
                    if (!IsSafeDisjointTagValue(r[0]) ||
                        (!extracted.empty() &&
                         r[0].type != extracted.front().type)) {
                      ext_ok = false;
                      break;
                    }
                    extracted.push_back(r[0]);
                  }
                  if (ext_ok && !extracted.empty()) {
                    site_vals = std::move(extracted);
                  }
                }
              }
            }
          }
        }
        if (site_vals.empty()) {
          all_sites_bound = false;
          break;
        }
        for (const Value& sv : site_vals) {
          if (!union_vals.empty() && sv.type != union_vals.front().type) {
            all_sites_bound = false;
            break;
          }
          const bool dup = std::ranges::any_of(
              union_vals, [&](const Value& u) { return u == sv; });
          if (!dup) {
            union_vals.push_back(sv);
          }
        }
        if (!all_sites_bound || union_vals.size() > 256) {
          all_sites_bound = false;
          break;
        }
      }
      if (!all_sites_bound || union_vals.empty()) {
        continue;
      }

      // Verify `gk_col` resolves to a non-null-supplying source of
      // `grouped_target`.
      bool gk_safe_in_target = false;
      size_t gk_hits = 0;
      ColumnName resolved_gk_col = gk_col;
      for (const SelectSource& gs : grouped_target->Sources()) {
        if (gs.unnest || gs.is_lateral || gs.from_nested_join ||
            !gs.using_columns.empty()) {
          gk_hits = 2;
          break;
        }
        const std::string galias = gs.alias.empty() ? gs.table : gs.alias;
        if (!gk_col.schema.empty() &&
            !IdentifierEquals(gk_col.schema, galias) &&
            (gs.table.empty() || !IdentifierEquals(gk_col.schema, gs.table))) {
          continue;
        }
        bool has_col = false;
        std::string matched_col_name = gk_col.name;
        if (gs.query != nullptr) {
          const SelectStatement* qhead = gs.query.get();
          if (qhead->SelectList().empty() &&
              qhead->GetSetOperationTree() != nullptr &&
              qhead->GetSetOperationTree()->first != nullptr) {
            qhead = qhead->GetSetOperationTree()->first.get();
          }
          for (const NamedExpression& si : qhead->SelectList()) {
            const std::string nm = ExplicitOrColumnOutputName(si);
            if (!nm.empty() && IdentifierEquals(nm, gk_col.name)) {
              has_col = true;
              matched_col_name = nm;
              break;
            }
          }
        } else if (!gs.table.empty()) {
          if (const auto gcte_it = outer->WithQueries().find(gs.table);
              gcte_it != outer->WithQueries().end()) {
            if (outer->IsRecursiveWith(gs.table) ||
                gcte_it->second == nullptr) {
              gk_hits = 2;
              break;
            }
            const SelectStatement* qhead = gcte_it->second.get();
            if (qhead->SelectList().empty() &&
                qhead->GetSetOperationTree() != nullptr &&
                qhead->GetSetOperationTree()->first != nullptr) {
              qhead = qhead->GetSetOperationTree()->first.get();
            }
            for (const NamedExpression& si : qhead->SelectList()) {
              const std::string nm = ExplicitOrColumnOutputName(si);
              if (!nm.empty() && IdentifierEquals(nm, gk_col.name)) {
                has_col = true;
                matched_col_name = nm;
                break;
              }
            }
          } else {
            StatusOr<std::shared_ptr<Table>> gtbl = ctx.GetTable(gs.table);
            if (!gtbl.HasValue()) {
              gk_hits = 2;
              break;
            }
            const Schema& gschema = gtbl.Value()->GetSchema();
            for (size_t i = 0; i < gschema.ColumnCount(); ++i) {
              if (IdentifierEquals(gschema.GetColumn(i).Name().name,
                                   gk_col.name)) {
                has_col = true;
                matched_col_name = gschema.GetColumn(i).Name().name;
                break;
              }
            }
          }
        }
        if (has_col) {
          if (gs.join_type != JoinType::kCross &&
              gs.join_type != JoinType::kInner) {
            gk_hits = 2;
            break;
          }
          ++gk_hits;
          gk_safe_in_target = true;
          resolved_gk_col = ColumnName(galias, matched_col_name);
        }
      }
      if (!gk_safe_in_target || gk_hits != 1) {
        continue;
      }

      Expression domain_pred =
          MakeValueDomainPredicate(resolved_gk_col, union_vals);
      grouped_target->SetWhereClause(
          grouped_target->WhereClause()
              ? BinaryExpressionExp(grouped_target->WhereClause(),
                                    BinaryOperation::kAnd,
                                    std::move(domain_pred))
              : std::move(domain_pred));
      PushDomainIntoUnionDerivedIfSafe(outer, grouped_target, resolved_gk_col,
                                       union_vals, ctx);
      any_pushed = true;
    }
  }
  return any_pushed;
}

namespace select_rewrite_detail {

namespace {
bool RewriteOneDistinctBranchSurrogateKeys(SelectStatement* br,
                                           TransactionContext& ctx) {
  if (br == nullptr || !br->Distinct() || br->HasDistinctOn() ||
      !br->GroupBy().empty() || br->IsGroupAll() || br->Having() ||
      br->Qualify() || br->HasLimit() || br->Offset() != 0 ||
      !br->OrderBy().empty() || br->WithTies() || br->ForUpdate() ||
      br->AsStruct() || !br->WithQueries().empty() || !br->UnionAll().empty() ||
      br->GetSetOperationTree() != nullptr ||
      relational_detail::HasWindowFunctions(*br) ||
      SelectListHasStarOrModifiers(*br) || TpcdsStmtHasParam(*br) ||
      br->Sources().size() < 2 || br->SelectList().empty()) {
    return false;
  }

  std::vector<std::shared_ptr<Table>> holders;
  std::vector<GroupingRelation> rels;
  std::vector<size_t> est_rows;
  std::unordered_set<std::string> seen_aliases;
  holders.reserve(br->Sources().size());
  rels.reserve(br->Sources().size());
  est_rows.reserve(br->Sources().size());

  for (const SelectSource& s : br->Sources()) {
    if (s.table.empty() || s.query != nullptr || s.unnest || s.is_lateral ||
        s.sample || !s.using_columns.empty() || s.from_nested_join ||
        (s.join_type != JoinType::kCross && s.join_type != JoinType::kInner)) {
      return false;
    }
    StatusOr<std::shared_ptr<Table>> tbl = ctx.GetTable(s.table);
    if (!tbl.HasValue()) {
      return false;
    }
    const std::string alias = s.alias.empty() ? s.table : s.alias;
    if (!seen_aliases.insert(LoweredStarName(alias)).second) {
      return false;
    }
    size_t rows = 0;
    if (StatusOr<std::shared_ptr<TableStatistics>> st = ctx.GetStats(s.table);
        st.HasValue() && st.Value() != nullptr) {
      rows = st.Value()->Rows();
    }
    holders.push_back(tbl.MoveValue());
    rels.push_back({.name = alias, .schema = &holders.back()->GetSchema()});
    est_rows.push_back(rows);
  }

  struct ResolvedCol {
    size_t rel_idx{0};
    std::string alias;
    std::string exact_col;
    ValueType type{ValueType::kNull};
    // Name mirrors what is actually stored: an UNSIGNED or INTERVAL integer
    // domain. It is NOT a collation flag (Column has no collation API here);
    // it marks keys this rewrite does not treat as fixed signed scalars.
    bool has_unsigned_or_interval{false};
  };
  auto resolve_col = [&](const ColumnName& col) -> std::optional<ResolvedCol> {
    if (col.name.empty() || col.name == "*") {
      return std::nullopt;
    }
    std::optional<ResolvedCol> found;
    for (size_t r = 0; r < rels.size(); ++r) {
      const SelectSource& src = br->Sources()[r];
      if (!col.schema.empty() && !IdentifierEquals(col.schema, rels[r].name) &&
          !IdentifierEquals(col.schema, src.table)) {
        continue;
      }
      const Schema& schema = *rels[r].schema;
      for (size_t c = 0; c < schema.ColumnCount(); ++c) {
        const Column& sc = schema.GetColumn(c);
        if (IdentifierEquals(sc.Name().name, col.name)) {
          if (found.has_value()) {
            return std::nullopt;
          }
          found = ResolvedCol{
              .rel_idx = r,
              .alias = rels[r].name,
              .exact_col = sc.Name().name,
              .type = sc.Type(),
              .has_unsigned_or_interval = sc.IsUnsigned() || sc.IsInterval(),
          };
          break;
        }
      }
    }
    return found;
  };

  auto qualify_expr = [&](auto& self,
                          const Expression& expr) -> std::optional<Expression> {
    if (!expr) {
      return expr;
    }
    if (expr->Type() == TypeTag::kQueryExp ||
        expr->Type() == TypeTag::kWindowFunctionExp ||
        expr->Type() == TypeTag::kAggregateExp) {
      return std::nullopt;
    }
    if (expr->Type() == TypeTag::kColumnValue) {
      std::optional<ResolvedCol> rc =
          resolve_col(expr->AsColumnValue().GetColumnName());
      if (!rc.has_value()) {
        return std::nullopt;
      }
      return ColumnValueExp(ColumnName(rc->alias, rc->exact_col));
    }
    std::vector<Expression> kids = ExpressionChildren(expr);
    for (Expression& k : kids) {
      std::optional<Expression> qk = self(self, k);
      if (!qk.has_value()) {
        return std::nullopt;
      }
      k = std::move(*qk);
    }
    return WithExpressionChildren(expr, std::move(kids));
  };

  struct SelectSlot {
    std::string out_name;
    ResolvedCol col;
  };
  std::vector<SelectSlot> slots;
  slots.reserve(br->SelectList().size());
  for (size_t i = 0; i < br->SelectList().size(); ++i) {
    const NamedExpression& item = br->SelectList()[i];
    if (!item.expression || item.expression->Type() != TypeTag::kColumnValue) {
      return false;
    }
    std::optional<ResolvedCol> rc =
        resolve_col(item.expression->AsColumnValue().GetColumnName());
    if (!rc.has_value()) {
      return false;
    }
    slots.push_back(SelectSlot{
        .out_name = InferSelectItemOutputName(item, i),
        .col = std::move(*rc),
    });
  }

  std::vector<Expression> all_conjuncts;
  auto add_pred = [&](const Expression& pred) -> bool {
    if (!pred) {
      return true;
    }
    if (ContainsQueryExpression(pred) ||
        relational_detail::ContainsAggregate(pred) ||
        !DerivedExpressionIsImmutable(pred)) {
      return false;
    }
    for (const Expression& c : SplitConjuncts(pred)) {
      std::optional<Expression> qc = qualify_expr(qualify_expr, c);
      if (!qc.has_value()) {
        return false;
      }
      all_conjuncts.push_back(std::move(*qc));
    }
    return true;
  };
  if (!add_pred(br->WhereClause())) {
    return false;
  }
  for (const SelectSource& s : br->Sources()) {
    if (!add_pred(s.join_condition)) {
      return false;
    }
  }
  if (all_conjuncts.empty()) {
    return false;
  }

  for (size_t dim_idx = 0; dim_idx < rels.size(); ++dim_idx) {
    if (est_rows[dim_idx] < 10000) {
      continue;
    }
    size_t max_core_rows = 0;
    for (size_t i = 0; i < rels.size(); ++i) {
      if (i != dim_idx) {
        max_core_rows = std::max(max_core_rows, est_rows[i]);
      }
    }
    if (max_core_rows < 50000) {
      continue;
    }

    size_t dim_varchar_slots = 0;
    bool core_slots_fixed_scalar = true;
    for (const SelectSlot& sl : slots) {
      if (sl.col.rel_idx == dim_idx) {
        if (sl.col.type == ValueType::kVarChar) {
          ++dim_varchar_slots;
        }
      } else if (sl.col.type == ValueType::kVarChar ||
                 sl.col.type == ValueType::kArray ||
                 sl.col.has_unsigned_or_interval) {
        core_slots_fixed_scalar = false;
        break;
      }
    }
    if (dim_varchar_slots == 0 || !core_slots_fixed_scalar) {
      continue;
    }

    std::vector<Expression> core_conjuncts;
    std::optional<ResolvedCol> core_fk;
    std::optional<ResolvedCol> dim_pk;
    size_t dim_conjunct_count = 0;
    bool ok = true;
    for (const Expression& conj : all_conjuncts) {
      const auto touched = conj->TouchedColumns();
      const bool touches_dim =
          std::ranges::any_of(touched, [&](const ColumnName& cn) {
            return IdentifierEquals(cn.schema, rels[dim_idx].name);
          });
      if (!touches_dim) {
        core_conjuncts.push_back(conj);
        continue;
      }
      ++dim_conjunct_count;
      if (dim_conjunct_count > 1 || conj->Type() != TypeTag::kBinaryExp ||
          conj->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
        ok = false;
        break;
      }
      const Expression& lhs = conj->AsBinaryExpression().Left();
      const Expression& rhs = conj->AsBinaryExpression().Right();
      if (!lhs || !rhs || lhs->Type() != TypeTag::kColumnValue ||
          rhs->Type() != TypeTag::kColumnValue) {
        ok = false;
        break;
      }
      std::optional<ResolvedCol> lrc =
          resolve_col(lhs->AsColumnValue().GetColumnName());
      std::optional<ResolvedCol> rrc =
          resolve_col(rhs->AsColumnValue().GetColumnName());
      if (!lrc.has_value() || !rrc.has_value()) {
        ok = false;
        break;
      }
      if (lrc->rel_idx == dim_idx && rrc->rel_idx != dim_idx) {
        dim_pk = std::move(*lrc);
        core_fk = std::move(*rrc);
      } else if (rrc->rel_idx == dim_idx && lrc->rel_idx != dim_idx) {
        core_fk = std::move(*lrc);
        dim_pk = std::move(*rrc);
      } else {
        ok = false;
        break;
      }
    }
    if (!ok || dim_conjunct_count != 1 || !core_fk.has_value() ||
        !dim_pk.has_value() || core_fk->type != dim_pk->type ||
        (core_fk->type != ValueType::kInt64 &&
         core_fk->type != ValueType::kDate) ||
        core_fk->has_unsigned_or_interval || dim_pk->has_unsigned_or_interval) {
      continue;
    }

    std::vector<SelectSource> pre_sources;
    pre_sources.reserve(br->Sources().size() - 1);
    for (size_t i = 0; i < br->Sources().size(); ++i) {
      if (i == dim_idx) {
        continue;
      }
      SelectSource cs = br->Sources()[i];
      if (cs.alias.empty()) {
        cs.alias = cs.table;
      }
      cs.join_type = JoinType::kCross;
      cs.join_condition = nullptr;
      pre_sources.push_back(std::move(cs));
    }

    // `core_fk = dim_pk` rejects NULL `core_fk`; filtering NULL `core_fk`
    // before GROUP BY avoids hashing unmatched NULL foreign keys.
    core_conjuncts.push_back(UnaryExpressionExp(
        ColumnValueExp(ColumnName(core_fk->alias, core_fk->exact_col)),
        UnaryOperation::kIsNotNull));

    std::vector<ColumnName> pre_key_cols;
    auto ensure_pre_key = [&](const ColumnName& cn) -> size_t {
      for (size_t k = 0; k < pre_key_cols.size(); ++k) {
        if (IdentifierEquals(pre_key_cols[k].schema, cn.schema) &&
            IdentifierEquals(pre_key_cols[k].name, cn.name)) {
          return k;
        }
      }
      const size_t idx = pre_key_cols.size();
      pre_key_cols.push_back(cn);
      return idx;
    };
    const size_t fk_slot =
        ensure_pre_key(ColumnName(core_fk->alias, core_fk->exact_col));
    std::vector<size_t> slot_pre_idx(slots.size(), 0);
    for (size_t s = 0; s < slots.size(); ++s) {
      if (slots[s].col.rel_idx != dim_idx) {
        slot_pre_idx[s] = ensure_pre_key(
            ColumnName(slots[s].col.alias, slots[s].col.exact_col));
      }
    }

    std::vector<NamedExpression> pre_select;
    std::vector<Expression> pre_group;
    pre_select.reserve(pre_key_cols.size());
    pre_group.reserve(pre_key_cols.size());
    for (size_t k = 0; k < pre_key_cols.size(); ++k) {
      pre_select.emplace_back("__k" + std::to_string(k),
                              ColumnValueExp(pre_key_cols[k]));
      pre_group.push_back(ColumnValueExp(pre_key_cols[k]));
    }

    auto pre_stmt = std::make_shared<SelectStatement>();
    pre_stmt->SetSources(std::move(pre_sources));
    pre_stmt->SetWhereClause(CombineConjuncts(core_conjuncts));
    pre_stmt->SetGroupBy(std::move(pre_group));
    pre_stmt->SetSelectList(std::move(pre_select));

    SelectSource pre_src("", "__tpcds_sk_pre", std::move(pre_stmt),
                         JoinType::kCross, nullptr);
    SelectSource dim_src = br->Sources()[dim_idx];
    if (dim_src.alias.empty()) {
      dim_src.alias = dim_src.table;
    }
    dim_src.join_type = JoinType::kCross;
    dim_src.join_condition = nullptr;

    std::vector<NamedExpression> outer_select;
    outer_select.reserve(slots.size());
    for (size_t s = 0; s < slots.size(); ++s) {
      if (slots[s].col.rel_idx == dim_idx) {
        outer_select.emplace_back(
            slots[s].out_name,
            ColumnValueExp(ColumnName(dim_src.alias, slots[s].col.exact_col)));
      } else {
        outer_select.emplace_back(
            slots[s].out_name,
            ColumnValueExp(ColumnName("__tpcds_sk_pre",
                                      "__k" + IntegerToName(slot_pre_idx[s]))));
      }
    }

    br->SetSources({std::move(pre_src), std::move(dim_src)});
    br->SetWhereClause(BinaryExpressionExp(
        ColumnValueExp(
            ColumnName("__tpcds_sk_pre", "__k" + std::to_string(fk_slot))),
        BinaryOperation::kEquals,
        ColumnValueExp(ColumnName(dim_pk->alias, dim_pk->exact_col))));
    br->SetSelectList(std::move(outer_select));
    br->SetDistinct(true);
    return true;
  }
  return false;
}
}  // namespace

namespace {
bool PushDistinctSurrogateKeysImpl(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt, TransactionContext& ctx,
    std::unordered_set<SelectStatement*>* visited) {
  if (stmt == nullptr) {
    return false;
  }
  bool changed = false;
  for (const auto& [name, body] : stmt->WithQueries()) {
    if (!stmt->IsRecursiveWith(name) && body != nullptr) {
      changed |= PushDistinctSurrogateKeysImpl(body.get(), ctx, visited);
    }
  }
  for (const SelectSource& src : stmt->Sources()) {
    if (src.query != nullptr && !src.is_lateral) {
      changed |= PushDistinctSurrogateKeysImpl(src.query.get(), ctx, visited);
    }
  }
  if (!stmt->UnionAll().empty() || stmt->GetSetOperationTree() != nullptr) {
    changed |= VisitSetOpLeavesAndSync(stmt, visited, [&](SelectStatement* br) {
      return RewriteOneDistinctBranchSurrogateKeys(br, ctx);
    });
  }
  return changed;
}
}  // namespace

}  // namespace select_rewrite_detail

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (eager_distinct_surrogate_keys_setop_sound,
//    except_below_non_injective_dim_unsound)
bool PushDistinctSurrogateKeysBeforeDimJoins(SelectStatement* outer,
                                             TransactionContext& ctx) {
  std::unordered_set<SelectStatement*> visited;
  return PushDistinctSurrogateKeysImpl(outer, ctx, &visited);
}

}  // namespace tinylamb
