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
#include "common/platform/process.hpp"
#include "common/set_operation.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/typed_stream_aggregate.hpp"
#include "executor/detail/window_eval.hpp"
#include "executor/relational.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
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

namespace {
template <typename Fn>
Expression ReplaceSubExpressions(  // NOLINT(misc-no-recursion)
    const Expression& expr, Fn&& fn) {
  if (!expr) {
    return expr;
  }
  if (std::optional<Expression> direct = fn(expr); direct.has_value()) {
    return std::move(*direct);
  }
  std::vector<Expression> children = ExpressionChildren(expr);
  bool changed = false;
  for (Expression& child : children) {
    Expression next = ReplaceSubExpressions(child, fn);
    if (next.get() != child.get()) {
      changed = true;
    }
    child = std::move(next);
  }
  Expression rebuilt =
      changed ? WithExpressionChildren(expr, std::move(children)) : expr;
  if (rebuilt && rebuilt->Type() == TypeTag::kAggregateExp) {
    const auto& agg = rebuilt->AsAggregateExpression();
    bool agg_changed = false;
    Expression next_filter = agg.WhereFilter();
    if (next_filter) {
      Expression mapped = ReplaceSubExpressions(next_filter, fn);
      if (mapped.get() != next_filter.get()) {
        agg_changed = true;
        next_filter = std::move(mapped);
      }
    }
    Expression next_having = agg.HavingCondition();
    if (next_having) {
      Expression mapped = ReplaceSubExpressions(next_having, fn);
      if (mapped.get() != next_having.get()) {
        agg_changed = true;
        next_having = std::move(mapped);
      }
    }
    std::vector<WindowOrderTerm> next_order = agg.InnerOrderBy();
    for (WindowOrderTerm& term : next_order) {
      if (term.expression) {
        Expression mapped = ReplaceSubExpressions(term.expression, fn);
        if (mapped.get() != term.expression.get()) {
          agg_changed = true;
          term.expression = std::move(mapped);
        }
      }
    }
    Expression next_secondary = agg.SecondaryArg();
    if (next_secondary) {
      Expression mapped = ReplaceSubExpressions(next_secondary, fn);
      if (mapped.get() != next_secondary.get()) {
        agg_changed = true;
        next_secondary = std::move(mapped);
      }
    }
    std::vector<Expression> next_trailing = agg.TrailingArgs();
    for (Expression& extra : next_trailing) {
      if (extra) {
        Expression mapped = ReplaceSubExpressions(extra, fn);
        if (mapped.get() != extra.get()) {
          agg_changed = true;
          extra = std::move(mapped);
        }
      }
    }
    std::vector<Expression> next_inner_group = agg.InnerGroupBy();
    for (Expression& key : next_inner_group) {
      if (key) {
        Expression mapped = ReplaceSubExpressions(key, fn);
        if (mapped.get() != key.get()) {
          agg_changed = true;
          key = std::move(mapped);
        }
      }
    }
    if (agg_changed) {
      auto fresh = std::make_shared<AggregateExpression>(
          agg.GetType(), agg.Child(), agg.Distinct());
      fresh->SetIgnoreNulls(agg.IgnoreNulls());
      if (agg.Having() != AggregateHavingModifier::kNone || next_having) {
        fresh->SetHaving(agg.Having(), std::move(next_having));
      }
      if (!next_order.empty()) {
        fresh->SetInnerOrderBy(std::move(next_order));
      }
      if (agg.InnerLimit().has_value()) {
        fresh->SetInnerLimit(agg.InnerLimit());
      }
      if (next_secondary) {
        fresh->SetSecondaryArg(std::move(next_secondary));
      }
      if (!next_trailing.empty()) {
        fresh->SetTrailingArgs(std::move(next_trailing));
      }
      if (next_filter) {
        fresh->SetWhereFilter(std::move(next_filter));
      }
      if (!agg.ArrayElementSqlType().empty()) {
        fresh->SetArrayElementSqlType(agg.ArrayElementSqlType());
      }
      if (!next_inner_group.empty()) {
        fresh->SetInnerGroupBy(std::move(next_inner_group));
      }
      rebuilt = std::move(fresh);
    }
  }
  return rebuilt;
}
}  // namespace

// Unwraps `(SELECT expr AS c1, ..., * FROM base_tbl)` inside FROM so
// downstream grouped/PK rewrites see the base table directly (e.g. Q23 `sq1`).
namespace {
bool UnwrapStarPlusComputedBaseSubqueries(SelectStatement* stmt,
                                          TransactionContext& ctx) {
  if (stmt == nullptr || stmt->Sources().empty() ||
      SelectListHasStarOrModifiers(*stmt) || TpcdsStmtHasParam(*stmt)) {
    return false;
  }
  bool any = false;
  for (size_t si = 0; si < stmt->Sources().size(); ++si) {
    SelectSource src = stmt->Sources()[si];
    if (src.query == nullptr || src.alias.empty() || src.unnest ||
        src.is_lateral || src.sample || !src.using_columns.empty() ||
        src.from_nested_join ||
        (src.join_type != JoinType::kCross &&
         src.join_type != JoinType::kInner)) {
      continue;
    }
    const SelectStatement& sq = *src.query;
    if (sq.Sources().size() != 1 || sq.WhereClause() || !sq.GroupBy().empty() ||
        sq.IsGroupAll() || sq.Having() || sq.Distinct() || sq.HasDistinctOn() ||
        sq.HasLimit() || sq.Offset() != 0 || sq.Qualify() ||
        !sq.OrderBy().empty() || !sq.WithQueries().empty() ||
        !sq.UnionAll().empty() || sq.GetSetOperationTree() != nullptr ||
        relational_detail::HasWindowFunctions(sq) ||
        sq.SelectList().size() < 2) {
      continue;
    }
    const SelectSource& bsrc = sq.Sources()[0];
    if (bsrc.table.empty() || bsrc.query != nullptr || bsrc.unnest ||
        bsrc.is_lateral || bsrc.sample || !bsrc.using_columns.empty() ||
        bsrc.from_nested_join) {
      continue;
    }
    StatusOr<std::shared_ptr<Table>> btbl = ctx.GetTable(bsrc.table);
    if (!btbl.HasValue() || btbl.Value() == nullptr) {
      continue;
    }
    const Schema& bschema = btbl.Value()->GetSchema();
    const std::string balias = bsrc.alias.empty() ? bsrc.table : bsrc.alias;
    const NamedExpression& last_it = sq.SelectList().back();
    if (!last_it.expression ||
        last_it.expression->Type() != TypeTag::kColumnValue ||
        last_it.expression->AsColumnValue().GetColumnName().name != "*") {
      continue;
    }
    std::unordered_map<std::string, Expression> computed_map;
    bool ok = true;
    for (size_t i = 0; i + 1 < sq.SelectList().size(); ++i) {
      const NamedExpression& ci = sq.SelectList()[i];
      if (ci.name.empty() || !ci.expression ||
          ContainsQueryExpression(ci.expression) ||
          relational_detail::ContainsAggregate(ci.expression) ||
          !DerivedExpressionIsImmutable(ci.expression) ||
          bschema.Offset(ColumnName("", ci.name)) >= 0) {
        ok = false;
        break;
      }
      Expression rebound = ReplaceSubExpressions(
          ci.expression,
          [&](const Expression& node) -> std::optional<Expression> {
            if (!node || node->Type() != TypeTag::kColumnValue) {
              return std::nullopt;
            }
            const ColumnName& cn = node->AsColumnValue().GetColumnName();
            if (!cn.schema.empty() && !IdentifierEquals(cn.schema, balias) &&
                !IdentifierEquals(cn.schema, bsrc.table)) {
              ok = false;
              return std::nullopt;
            }
            const int boff = bschema.Offset(ColumnName("", cn.name));
            if (boff < 0) {
              ok = false;
              return std::nullopt;
            }
            return ColumnValueExp(ColumnName(
                src.alias,
                bschema.GetColumn(static_cast<size_t>(boff)).Name().name));
          });
      if (!ok ||
          !computed_map.emplace(LoweredStarName(ci.name), std::move(rebound))
               .second) {
        ok = false;
        break;
      }
    }
    if (!ok || computed_map.empty()) {
      continue;
    }
    auto subst = [&](const Expression& expr) -> Expression {
      if (!expr) {
        return expr;
      }
      return ReplaceSubExpressions(
          expr, [&](const Expression& node) -> std::optional<Expression> {
            if (!node || node->Type() != TypeTag::kColumnValue) {
              return std::nullopt;
            }
            const ColumnName& cn = node->AsColumnValue().GetColumnName();
            if (!cn.schema.empty() && !IdentifierEquals(cn.schema, src.alias)) {
              return std::nullopt;
            }
            const auto it = computed_map.find(LoweredStarName(cn.name));
            return it != computed_map.end()
                       ? std::optional<Expression>(it->second)
                       : std::nullopt;
          });
    };
    std::vector<NamedExpression> new_sel;
    new_sel.reserve(stmt->SelectList().size());
    for (size_t i = 0; i < stmt->SelectList().size(); ++i) {
      new_sel.emplace_back(InferSelectItemOutputName(stmt->SelectList()[i], i),
                           subst(stmt->SelectList()[i].expression));
    }
    std::vector<Expression> new_grp;
    new_grp.reserve(stmt->GroupBy().size());
    for (const Expression& g : stmt->GroupBy()) {
      new_grp.push_back(subst(g));
    }
    std::vector<SelectStatement::OrderByTerm> new_ord = stmt->OrderBy();
    for (auto& t : new_ord) {
      t.expression = subst(t.expression);
    }
    std::vector<SelectSource> new_srcs = stmt->Sources();
    for (SelectSource& s : new_srcs) {
      if (s.join_condition) {
        s.join_condition = subst(s.join_condition);
      }
    }
    new_srcs[si].table = bsrc.table;
    new_srcs[si].query = nullptr;
    stmt->SetSources(std::move(new_srcs));
    stmt->SetSelectList(std::move(new_sel));
    stmt->SetGroupBy(std::move(new_grp));
    stmt->SetOrderBy(std::move(new_ord));
    if (stmt->WhereClause()) {
      stmt->SetWhereClause(subst(stmt->WhereClause()));
    }
    if (stmt->Having()) {
      stmt->SetHaving(subst(stmt->Having()));
    }
    if (stmt->Qualify()) {
      stmt->SetQualify(subst(stmt->Qualify()));
    }
    any = true;
  }
  return any;
}
}  // namespace

namespace {
bool DeferOneUniqueDimJoinPastGroupBy(SelectStatement* stmt,
                                      TransactionContext& ctx) {
  if (stmt == nullptr || stmt->GroupBy().empty() || stmt->IsGroupAll() ||
      stmt->Distinct() || stmt->HasDistinctOn() || stmt->Qualify() ||
      stmt->HasLimit() || stmt->Offset() != 0 || !stmt->OrderBy().empty() ||
      stmt->WithTies() || stmt->ForUpdate() || stmt->AsStruct() ||
      !stmt->WithQueries().empty() || !stmt->UnionAll().empty() ||
      stmt->GetSetOperationTree() != nullptr ||
      relational_detail::HasWindowFunctions(*stmt) ||
      relational_detail::StatementContainsVolatileFunction(*stmt) ||
      SelectListHasStarOrModifiers(*stmt) || TpcdsStmtHasParam(*stmt) ||
      stmt->Sources().size() < 2) {
    return false;
  }
  (void)UnwrapStarPlusComputedBaseSubqueries(stmt, ctx);

  struct RelMeta {
    std::string table_name;
    std::string alias;
    std::shared_ptr<Table> table;
    const Schema* schema{nullptr};
    uint64_t est_rows{0};
  };
  std::vector<RelMeta> rels(stmt->Sources().size());
  std::unordered_set<std::string> seen_aliases;
  uint64_t max_est = 0;
  for (size_t si = 0; si < stmt->Sources().size(); ++si) {
    const SelectSource& s = stmt->Sources()[si];
    if (s.table.empty() || s.query != nullptr || s.unnest || s.is_lateral ||
        s.sample || !s.using_columns.empty() || s.from_nested_join ||
        (s.join_type != JoinType::kCross && s.join_type != JoinType::kInner)) {
      return false;
    }
    StatusOr<std::shared_ptr<Table>> tbl = ctx.GetTable(s.table);
    if (!tbl.HasValue() || tbl.Value() == nullptr) {
      return false;
    }
    const std::string al = s.alias.empty() ? s.table : s.alias;
    if (!seen_aliases.insert(LoweredStarName(al)).second) {
      return false;
    }
    uint64_t est = tbl.Value()->BackingColumnarRowCount();
    if (auto st = ctx.GetStats(s.table); st.HasValue()) {
      est = std::max<uint64_t>(est, st.Value()->Rows());
    }
    max_est = std::max(max_est, est);
    rels[si] = RelMeta{.table_name = s.table,
                       .alias = al,
                       .table = tbl.Value(),
                       .schema = &tbl.Value()->GetSchema(),
                       .est_rows = est};
  }

  auto resolve_col =
      [&](const ColumnName& cn) -> std::optional<std::pair<size_t, size_t>> {
    if (cn.name.empty() || cn.name == "*") {
      return std::nullopt;
    }
    if (!cn.schema.empty()) {
      for (size_t si = 0; si < rels.size(); ++si) {
        if (IdentifierEquals(cn.schema, rels[si].alias) ||
            (stmt->Sources()[si].alias.empty() &&
             IdentifierEquals(cn.schema, rels[si].table_name))) {
          const int off = rels[si].schema->Offset(ColumnName("", cn.name));
          if (off >= 0) {
            return std::pair{si, static_cast<size_t>(off)};
          }
        }
      }
      return std::nullopt;
    }
    std::optional<std::pair<size_t, size_t>> hit;
    for (size_t si = 0; si < rels.size(); ++si) {
      const int off = rels[si].schema->Offset(ColumnName("", cn.name));
      if (off >= 0) {
        if (hit.has_value()) {
          return std::nullopt;
        }
        hit = std::pair{si, static_cast<size_t>(off)};
      }
    }
    return hit;
  };

  auto expr_sources = [&](const Expression& expr,
                          std::unordered_set<size_t>* out) {
    if (!expr) {
      return true;
    }
    if (ContainsQueryExpression(expr)) {
      return false;
    }
    for (const ColumnName& cn : expr->TouchedColumns()) {
      auto r = resolve_col(cn);
      if (!r.has_value()) {
        return false;
      }
      out->insert(r->first);
    }
    return true;
  };

  std::vector<Expression> all_preds = SplitConjuncts(stmt->WhereClause());
  all_preds.reserve(all_preds.size() + stmt->Sources().size() - 1);
  for (size_t si = 1; si < stmt->Sources().size(); ++si) {
    if (stmt->Sources()[si].join_condition) {
      for (Expression& c : SplitConjuncts(stmt->Sources()[si].join_condition)) {
        all_preds.push_back(std::move(c));
      }
    }
  }

  for (size_t dim_si = 0; dim_si < rels.size(); ++dim_si) {
    if (rels[dim_si].est_rows > 250000 ||
        rels[dim_si].est_rows * 2 >= max_est) {
      continue;
    }
    // Tiny PK lookup tables (<= 64 rows, e.g. warehouse/store/call_center)
    // inside a >=3-source join execute directly as L1-resident
    // DeferredLSTablePkLookup dictionaries inside FusedGroupedJoin (enabling
    // split_key_gid + def_cell=1), whereas deferring them past GROUP BY adds a
    // post-aggregate join and breaks single-probe-key packing. Keep deferral
    // for 2-source -> 1-table reduction.
    if (rels.size() >= 3 && rels[dim_si].est_rows <= 64) {
      continue;
    }
    // Check predicates touching dim_si: must be ONLY one `fk_col = dim.pk_col`
    // plus optional non-throwing local `dim.col IS NOT NULL` guards.
    size_t dim_preds = 0;
    size_t pk_slot = 0;
    std::pair<size_t, size_t> fk_ref{0, 0};
    std::vector<Expression> core_preds;
    std::vector<Expression> dim_local_preds;
    bool pred_ok = true;
    for (const Expression& pr : all_preds) {
      std::unordered_set<size_t> ps;
      if (!expr_sources(pr, &ps)) {
        pred_ok = false;
        break;
      }
      if (!ps.contains(dim_si)) {
        core_preds.push_back(pr);
        continue;
      }
      if (ps.size() == 1 && pr->Type() == TypeTag::kUnaryExp &&
          pr->AsUnaryExpression().Op() == UnaryOperation::kIsNotNull &&
          pr->AsUnaryExpression().Child() &&
          pr->AsUnaryExpression().Child()->Type() == TypeTag::kColumnValue) {
        auto dr = resolve_col(
            pr->AsUnaryExpression().Child()->AsColumnValue().GetColumnName());
        if (!dr.has_value() || dr->first != dim_si) {
          pred_ok = false;
          break;
        }
        dim_local_preds.push_back(UnaryExpressionExp(
            ColumnValueExp(ColumnName(
                rels[dim_si].alias,
                rels[dim_si].schema->GetColumn(dr->second).Name().name)),
            UnaryOperation::kIsNotNull));
        continue;
      }
      ++dim_preds;
      if (dim_preds > 1 || pr->Type() != TypeTag::kBinaryExp ||
          pr->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
        pred_ok = false;
        break;
      }
      const Expression& l = pr->AsBinaryExpression().Left();
      const Expression& r = pr->AsBinaryExpression().Right();
      if (!l || !r || l->Type() != TypeTag::kColumnValue ||
          r->Type() != TypeTag::kColumnValue) {
        pred_ok = false;
        break;
      }
      auto lr = resolve_col(l->AsColumnValue().GetColumnName());
      auto rr = resolve_col(r->AsColumnValue().GetColumnName());
      if (!lr.has_value() || !rr.has_value() ||
          (lr->first == dim_si) == (rr->first == dim_si)) {
        pred_ok = false;
        break;
      }
      const auto dim_r = (lr->first == dim_si) ? *lr : *rr;
      const auto oth_r = (lr->first == dim_si) ? *rr : *lr;
      const Column& pkc = rels[dim_si].schema->GetColumn(dim_r.second);
      const Column& fkc = rels[oth_r.first].schema->GetColumn(oth_r.second);
      if (pkc.Type() != ValueType::kInt64 || pkc.IsUnsigned() ||
          fkc.Type() != ValueType::kInt64 || fkc.IsUnsigned()) {
        pred_ok = false;
        break;
      }
      pk_slot = dim_r.second;
      fk_ref = oth_r;
    }
    if (!pred_ok || dim_preds != 1) {
      continue;
    }
    if (!dim_local_preds.empty() && stmt->Having() &&
        !DerivedExpressionIsImmutable(stmt->Having())) {
      continue;
    }

    // Check GroupBy: at least one unique dim key `dim.uk_col` AND at least one
    // extra dim attribute/expr in GroupBy or SelectList that benefits from
    // deferral, while non-dim GroupBy items are pure ColumnValues.
    bool has_unique_dim_gkey = false;
    size_t dim_referenced_cols = 0;
    std::vector<std::pair<size_t, size_t>> retained_non_dim_gkeys;
    bool gkeys_ok = true;
    for (const Expression& gk : stmt->GroupBy()) {
      std::unordered_set<size_t> gs;
      if (!gk || !DerivedExpressionIsImmutable(gk) || !expr_sources(gk, &gs)) {
        gkeys_ok = false;
        break;
      }
      if (gs.contains(dim_si)) {
        if (gs.size() != 1) {
          gkeys_ok = false;
          break;
        }
        ++dim_referenced_cols;
        if (!has_unique_dim_gkey && gk->Type() == TypeTag::kColumnValue) {
          auto gr = resolve_col(gk->AsColumnValue().GetColumnName());
          if (gr.has_value() && gr->first == dim_si) {
            const std::string& uk_name =
                rels[dim_si].schema->GetColumn(gr->second).Name().name;
            if (InspectRuntimeUniqueNonNullColumn(ctx, rels[dim_si].table_name,
                                                  uk_name)
                    .unique_non_null) {
              has_unique_dim_gkey = true;
            }
          }
        }
      } else {
        if (gk->Type() != TypeTag::kColumnValue) {
          gkeys_ok = false;
          break;
        }
        auto gr = resolve_col(gk->AsColumnValue().GetColumnName());
        if (!gr.has_value()) {
          gkeys_ok = false;
          break;
        }
        if (std::ranges::find(retained_non_dim_gkeys, *gr) ==
            retained_non_dim_gkeys.end()) {
          retained_non_dim_gkeys.push_back(*gr);
        }
      }
    }
    if (!gkeys_ok || !has_unique_dim_gkey) {
      continue;
    }
    // Also verify `pk_slot` is unique non-null on `dim_si`.
    const std::string& pk_name =
        rels[dim_si].schema->GetColumn(pk_slot).Name().name;
    if (!InspectRuntimeUniqueNonNullColumn(ctx, rels[dim_si].table_name,
                                           pk_name)
             .unique_non_null) {
      continue;
    }

    // Check Having and SelectList: no AggregateExpression may touch `dim_si`,
    // and Having may not touch `dim_si` outside aggregates either.
    std::vector<const AggregateExpression*> aggs;
    std::unordered_set<std::string> seen_agg_str;
    std::unordered_map<std::string, size_t> agg_to_idx;
    bool exprs_ok = true;
    auto check_and_collect = [&](auto& self, const Expression& e,
                                 bool allow_dim_scalar) -> bool {
      if (!e) {
        return true;
      }
      if (e->Type() == TypeTag::kQueryExp ||
          e->Type() == TypeTag::kWindowFunctionExp) {
        return false;
      }
      if (e->Type() ==
          TypeTag::
              kAggregateExp) {  // NOLINT(readability-simplify-boolean-expr)
        const auto& agg = e->AsAggregateExpression();
        if (agg.Distinct() || agg.NeedsGroupContext() || agg.HasWhereFilter() ||
            !agg.InnerGroupBy().empty() || !agg.InnerOrderBy().empty() ||
            agg.InnerLimit().has_value() ||
            agg.Having() != AggregateHavingModifier::kNone) {
          return false;
        }
        if (agg.Child() &&
            (agg.Child()->Type() !=
                 TypeTag::
                     kColumnValue ||  // NOLINT(readability-simplify-boolean-expr)
             agg.Child()->AsColumnValue().GetColumnName().name != "*")) {
          std::unordered_set<size_t> cs;
          if (!expr_sources(agg.Child(), &cs) || cs.contains(dim_si)) {
            return false;
          }
        }
        const std::string key = e->ToString();
        if (seen_agg_str.insert(key).second) {
          agg_to_idx.emplace(key, aggs.size());
          aggs.push_back(&agg);
        }
        return true;
      }
      if (e->Type() == TypeTag::kColumnValue) {
        auto r = resolve_col(e->AsColumnValue().GetColumnName());
        if (!r.has_value()) {
          return false;
        }
        if (r->first == dim_si) {
          ++dim_referenced_cols;
          return allow_dim_scalar;
        }
        return *r == fk_ref || std::ranges::find(retained_non_dim_gkeys, *r) !=
                                   retained_non_dim_gkeys.end();
      }
      for (const Expression& ch : ExpressionChildren(e)) {
        if (!self(self, ch, allow_dim_scalar)) {
          return false;
        }
      }
      return true;
    };

    if (stmt->Having() && !check_and_collect(check_and_collect, stmt->Having(),
                                             /*allow_dim_scalar=*/false)) {
      exprs_ok = false;
    }
    for (const NamedExpression& it : stmt->SelectList()) {
      if (!exprs_ok || !check_and_collect(check_and_collect, it.expression,
                                          /*allow_dim_scalar=*/true)) {
        exprs_ok = false;
        break;
      }
    }
    if (!exprs_ok || dim_referenced_cols < 2) {
      continue;
    }
    // Build inner pre-grouped subquery `__tpcds_dim_pre`.
    std::vector<SelectSource> pre_sources;
    pre_sources.reserve(stmt->Sources().size() - 1);
    for (size_t si = 0; si < stmt->Sources().size(); ++si) {
      if (si == dim_si) {
        continue;
      }
      SelectSource cp = stmt->Sources()[si];
      cp.alias = rels[si].alias;
      cp.join_type = JoinType::kCross;
      cp.join_condition = nullptr;
      pre_sources.push_back(std::move(cp));
    }
    auto qualify_core_expr = [&](const Expression& e) -> Expression {
      if (!e) {
        return e;
      }
      return ReplaceSubExpressions(
          e, [&](const Expression& node) -> std::optional<Expression> {
            if (!node || node->Type() != TypeTag::kColumnValue) {
              return std::nullopt;
            }
            const ColumnName& cn = node->AsColumnValue().GetColumnName();
            if (cn.name == "*") {
              return std::nullopt;
            }
            auto r = resolve_col(cn);
            if (!r.has_value()) {
              return std::nullopt;
            }
            return ColumnValueExp(ColumnName(
                rels[r->first].alias,
                rels[r->first].schema->GetColumn(r->second).Name().name));
          });
    };

    const Expression fk_col_expr = ColumnValueExp(ColumnName(
        rels[fk_ref.first].alias,
        rels[fk_ref.first].schema->GetColumn(fk_ref.second).Name().name));
    std::vector<Expression> pre_where;
    pre_where.reserve(core_preds.size() + 1);
    for (const Expression& cp : core_preds) {
      pre_where.push_back(qualify_core_expr(cp));
    }
    pre_where.push_back(
        UnaryExpressionExp(fk_col_expr, UnaryOperation::kIsNotNull));

    std::vector<NamedExpression> pre_select;
    pre_select.reserve(1 + retained_non_dim_gkeys.size() + aggs.size());
    std::vector<Expression> pre_group;
    pre_select.emplace_back("__fk", fk_col_expr);
    pre_group.push_back(fk_col_expr);
    std::vector<size_t> gk_pre_slot(retained_non_dim_gkeys.size(), 0);
    for (size_t gi = 0; gi < retained_non_dim_gkeys.size(); ++gi) {
      if (retained_non_dim_gkeys[gi] == fk_ref) {
        gk_pre_slot[gi] = 0;
        continue;
      }
      const size_t sl = pre_select.size();
      gk_pre_slot[gi] = sl;
      const Expression ge = ColumnValueExp(
          ColumnName(rels[retained_non_dim_gkeys[gi].first].alias,
                     rels[retained_non_dim_gkeys[gi].first]
                         .schema->GetColumn(retained_non_dim_gkeys[gi].second)
                         .Name()
                         .name));
      pre_select.emplace_back("__gk" + std::to_string(sl), ge);
      pre_group.push_back(ge);
    }
    for (size_t ai = 0; ai < aggs.size(); ++ai) {
      const AggregateExpression& a = *aggs[ai];
      Expression qual_child =
          (a.Child() && a.Child()->Type() == TypeTag::kColumnValue &&
           a.Child()->AsColumnValue().GetColumnName().name == "*")
              ? a.Child()
              : qualify_core_expr(a.Child());
      pre_select.emplace_back(
          "__agg" + std::to_string(ai),
          AggregateExpressionExp(a.GetType(), std::move(qual_child), false));
    }

    auto pre_stmt = std::make_shared<SelectStatement>();
    pre_stmt->SetSources(std::move(pre_sources));
    pre_stmt->SetWhereClause(CombineConjuncts(pre_where));
    pre_stmt->SetGroupBy(std::move(pre_group));
    if (stmt->Having()) {
      pre_stmt->SetHaving(qualify_core_expr(stmt->Having()));
    }
    pre_stmt->SetSelectList(std::move(pre_select));

    const std::string pre_alias = "__tpcds_dim_pre";
    SelectSource pre_src("", pre_alias, std::move(pre_stmt), JoinType::kCross,
                         nullptr);
    SelectSource dim_src = stmt->Sources()[dim_si];
    dim_src.alias = rels[dim_si].alias;
    dim_src.join_type = JoinType::kCross;
    dim_src.join_condition = nullptr;

    auto rewrite_outer_expr = [&](const Expression& e) -> Expression {
      if (!e) {
        return e;
      }
      return ReplaceSubExpressions(
          e, [&](const Expression& node) -> std::optional<Expression> {
            if (!node) {
              return std::nullopt;
            }
            if (node->Type() == TypeTag::kAggregateExp) {
              const auto it = agg_to_idx.find(node->ToString());
              if (it != agg_to_idx.end()) {
                return ColumnValueExp(ColumnName(
                    pre_alias, "__agg" + std::to_string(it->second)));
              }
              return std::nullopt;
            }
            if (node->Type() == TypeTag::kColumnValue) {
              auto r = resolve_col(node->AsColumnValue().GetColumnName());
              if (!r.has_value()) {
                return std::nullopt;
              }
              if (r->first == dim_si) {
                return ColumnValueExp(ColumnName(
                    rels[dim_si].alias,
                    rels[dim_si].schema->GetColumn(r->second).Name().name));
              }
              if (*r == fk_ref) {
                return ColumnValueExp(ColumnName(pre_alias, "__fk"));
              }
              for (size_t gi = 0; gi < retained_non_dim_gkeys.size(); ++gi) {
                if (retained_non_dim_gkeys[gi] == *r) {
                  const size_t sl = gk_pre_slot[gi];
                  return ColumnValueExp(ColumnName(
                      pre_alias,
                      sl == 0 ? "__fk" : ("__gk" + std::to_string(sl))));
                }
              }
            }
            return std::nullopt;
          });
    };

    std::vector<NamedExpression> out_sel;
    out_sel.reserve(stmt->SelectList().size());
    for (size_t i = 0; i < stmt->SelectList().size(); ++i) {
      out_sel.emplace_back(
          InferSelectItemOutputName(stmt->SelectList()[i], i),
          rewrite_outer_expr(stmt->SelectList()[i].expression));
    }

    std::vector<Expression> outer_where;
    outer_where.reserve(1 + dim_local_preds.size());
    outer_where.push_back(BinaryExpressionExp(
        ColumnValueExp(ColumnName(pre_alias, "__fk")), BinaryOperation::kEquals,
        ColumnValueExp(ColumnName(rels[dim_si].alias, pk_name))));
    for (Expression& dlp : dim_local_preds) {
      outer_where.push_back(std::move(dlp));
    }
    stmt->SetSources({std::move(pre_src), std::move(dim_src)});
    stmt->SetWhereClause(CombineConjuncts(outer_where));
    stmt->SetGroupBy({});
    stmt->SetHaving(nullptr);
    stmt->SetSelectList(std::move(out_sel));
    return true;
  }
  return false;
}
}  // namespace

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (rollup_sum_homomorphic_arbitrary_expr_sound)
// Lean proof: lean/TinylambOpt/TpcdsNextDiscovery.lean
//   (rollup_two_level_state_combine_sound,
//   rollup_three_level_state_combine_sound,
//    rollup_grand_total_requires_coalesce)
// Preserves identical FROM+WHERE input shape across ROLLUP / GROUPING SETS
// UNION ALL branches (including DOUBLE aggregates such as Q67) so the executor
// scans and joins the base tables once instead of per-branch.
namespace {
bool HasSharedScanUnionAllBranches(const SelectStatement& stmt) {
  if (stmt.UnionAll().size() < 2 || stmt.GetSetOperationTree() != nullptr) {
    return false;
  }
  auto shareable_part = [](const SelectStatement& part) {
    return !part.GroupBy().empty() && !part.Distinct() &&
           !part.HasDistinctOn() && !part.Having() && !part.Qualify() &&
           !relational_detail::StatementContainsVolatileFunction(part);
  };
  if (!shareable_part(stmt)) {
    return false;
  }
  auto input_shape = [](const SelectStatement& part) {
    SelectStatement shape(part);
    shape.SetSelectList({});
    shape.SetGroupBy({});
    shape.SetGroupAll(false);
    shape.SetHaving(nullptr);
    shape.SetOrderBy({});
    shape.SetLimit(std::nullopt);
    shape.SetOffset(0);
    shape.SetWithTies(false);
    shape.ClearUnionAll();
    shape.SetSetOperationTree(nullptr);
    shape.ClearWithQueries();
    return std::string(shape.Fingerprint());
  };
  const std::string head_shape = input_shape(stmt);
  for (size_t i = 0; i < stmt.UnionAll().size(); ++i) {
    if (stmt.UnionAll()[i] == nullptr) {
      return false;
    }
    const SelectStatement& part = *stmt.UnionAll()[i];
    const auto match = stmt.MatchFor(i);
    if (!shareable_part(part) || part.HasLimit() || part.Offset() != 0 ||
        !part.OrderBy().empty() || !part.WithQueries().empty() ||
        !part.UnionAll().empty() || match.by_name || match.corresponding ||
        !match.columns.empty() ||
        (i < stmt.SetOperationKinds().size() &&
         stmt.SetOperationKinds()[i] != SetOperationKind::kUnionAll) ||
        input_shape(part) != head_shape) {
      return false;
    }
  }
  return true;
}
}  // namespace

namespace {
bool DeferUniqueDimJoinPastGroupByImpl(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt, TransactionContext& ctx,
    std::unordered_set<SelectStatement*>* visited) {
  if (stmt == nullptr) {
    return false;
  }
  bool changed = false;
  for (const auto& [name, body] : stmt->WithQueries()) {
    if (!stmt->IsRecursiveWith(name) && body != nullptr) {
      changed |= DeferUniqueDimJoinPastGroupByImpl(body.get(), ctx, visited);
    }
  }
  for (const SelectSource& src : stmt->Sources()) {
    if (src.query != nullptr && !src.is_lateral) {
      changed |=
          DeferUniqueDimJoinPastGroupByImpl(src.query.get(), ctx, visited);
    }
  }
  if (!stmt->UnionAll().empty() || stmt->GetSetOperationTree() != nullptr) {
    if (!stmt->UnionAll().empty() &&
        (relational_detail::CanUseExactIntegerGroupingSets(ctx, *stmt) ||
         HasSharedScanUnionAllBranches(*stmt))) {
      return changed;
    }
    changed |= VisitSetOpLeavesAndSync(stmt, visited, [&](SelectStatement* br) {
      return DeferOneUniqueDimJoinPastGroupBy(br, ctx);
    });
  } else {
    changed |= DeferOneUniqueDimJoinPastGroupBy(stmt, ctx);
  }
  return changed;
}
}  // namespace

namespace {
bool SubqueryMayReferenceDimSource(  // NOLINT(misc-no-recursion)
    const SelectStatement& q, std::string_view dim_alias,
    std::string_view dim_table, const Schema& dim_schema);
}  // namespace

namespace {
bool ExprMayReferenceDimInsideSubquery(  // NOLINT(misc-no-recursion)
    const Expression& e, std::string_view dim_alias, std::string_view dim_table,
    const Schema& dim_schema) {
  if (!e) {
    return false;
  }
  if (e->Type() == TypeTag::kQueryExp) {
    const auto& q = e->AsQueryExpression();
    if (q.Test() && ExprMayReferenceDimInsideSubquery(q.Test(), dim_alias,
                                                      dim_table, dim_schema)) {
      return true;
    }
    return q.Query() != nullptr &&
           SubqueryMayReferenceDimSource(*q.Query(), dim_alias, dim_table,
                                         dim_schema);
  }
  if (e->Type() == TypeTag::kColumnValue) {
    const ColumnName& cn = e->AsColumnValue().GetColumnName();
    if (cn.name == "*") {
      return !cn.schema.empty() &&
             (IdentifierEquals(cn.schema, dim_alias) ||
              (!dim_table.empty() &&
               IdentifierEquals(
                   cn.schema,
                   dim_table)));  // NOLINT(readability-use-anyofallof)
    }
    if (!cn.schema.empty()) {
      return IdentifierEquals(cn.schema, dim_alias) ||
             (!dim_table.empty() && IdentifierEquals(cn.schema, dim_table));
    }
    return dim_schema.Offset(ColumnName("", cn.name)) >= 0;
  }
  return std::ranges::any_of(ExpressionChildren(e), [&](const Expression& ch) {
    return ExprMayReferenceDimInsideSubquery(ch, dim_alias, dim_table,
                                             dim_schema);
  });
}
}  // namespace

namespace {
bool SubqueryMayReferenceDimSource(  // NOLINT(misc-no-recursion)
    const SelectStatement& q, std::string_view dim_alias,
    std::string_view dim_table, const Schema& dim_schema) {
  for (const auto& [name, body] : q.WithQueries()) {
    (void)name;
    if (body != nullptr && SubqueryMayReferenceDimSource(
                               *body, dim_alias, dim_table, dim_schema)) {
      return true;
    }
  }
  for (const SelectSource& s : q.Sources()) {
    if (s.query != nullptr && SubqueryMayReferenceDimSource(
                                  *s.query, dim_alias, dim_table, dim_schema)) {
      return true;
    }
    if (ExprMayReferenceDimInsideSubquery(s.join_condition, dim_alias,
                                          dim_table, dim_schema) ||
        ExprMayReferenceDimInsideSubquery(s.unnest, dim_alias, dim_table,
                                          dim_schema)) {
      return true;
    }
  }
  if (ExprMayReferenceDimInsideSubquery(q.WhereClause(), dim_alias, dim_table,
                                        dim_schema) ||
      ExprMayReferenceDimInsideSubquery(q.Having(), dim_alias, dim_table,
                                        dim_schema) ||
      ExprMayReferenceDimInsideSubquery(q.Qualify(), dim_alias, dim_table,
                                        dim_schema)) {
    return true;
  }
  for (const NamedExpression& it : q.SelectList()) {
    if (ExprMayReferenceDimInsideSubquery(it.expression, dim_alias, dim_table,
                                          dim_schema)) {
      return true;
    }
  }
  for (const Expression& g : q.GroupBy()) {
    if (ExprMayReferenceDimInsideSubquery(g, dim_alias, dim_table,
                                          dim_schema)) {
      return true;
    }
  }
  for (const auto& ot : q.OrderBy()) {
    if (ExprMayReferenceDimInsideSubquery(ot.expression, dim_alias, dim_table,
                                          dim_schema)) {
      return true;
    }
  }
  for (const auto& br : q.UnionAll()) {
    if (br != nullptr &&
        SubqueryMayReferenceDimSource(*br, dim_alias, dim_table, dim_schema)) {
      return true;
    }
  }
  if (const auto& tree = q.GetSetOperationTree(); tree != nullptr) {
    if (tree->first != nullptr &&
        SubqueryMayReferenceDimSource(*tree->first, dim_alias, dim_table,
                                      dim_schema)) {
      return true;
    }
    for (const auto& br : tree->branches) {
      if (br != nullptr && SubqueryMayReferenceDimSource(
                               *br, dim_alias, dim_table, dim_schema)) {
        return true;
      }
    }
  }
  return false;
}
}  // namespace

namespace {
bool EliminateUnreferencedPkInOneSelect(
    SelectStatement* stmt,
    const std::unordered_map<std::string, const SelectStatement*>& visible_ctes,
    TransactionContext& ctx) {
  if (stmt == nullptr || stmt->Sources().size() < 2 || stmt->ForUpdate() ||
      stmt->AsStruct() || SelectListHasStarOrModifiers(*stmt) ||
      TpcdsStmtHasParam(*stmt) || !stmt->UnionAll().empty() ||
      stmt->GetSetOperationTree() != nullptr) {
    return false;
  }
  bool any_eliminated = false;
  for (size_t iter = 0; iter < 4 && stmt->Sources().size() >= 2; ++iter) {
    struct SrcInfo {
      std::string table;
      std::string alias;
      std::shared_ptr<Table> tbl;
      Schema schema;
      bool is_base{false};
      uint64_t est_rows{0};
    };
    std::vector<SrcInfo> srcs(stmt->Sources().size());
    std::unordered_set<std::string> seen_al;
    bool srcs_ok = true;
    auto populate_query_schema = [&](const SelectStatement& q,
                                     Schema* out_schema) -> bool {
      const SelectStatement* qhead = &q;
      if (qhead->SelectList().empty() &&
          qhead->GetSetOperationTree() != nullptr &&
          qhead->GetSetOperationTree()->first != nullptr) {
        qhead = qhead->GetSetOperationTree()->first.get();
      }
      if (qhead->SelectList().empty() || SelectListHasStarOrModifiers(*qhead)) {
        return false;
      }
      std::vector<Column> cols;
      cols.reserve(qhead->SelectList().size());
      for (size_t ci = 0; ci < qhead->SelectList().size(); ++ci) {
        cols.emplace_back(
            InferSelectItemOutputName(qhead->SelectList()[ci], ci),
            ValueType::kNull);
      }
      *out_schema = Schema("", std::move(cols));
      return true;
    };
    for (size_t si = 0; si < stmt->Sources().size(); ++si) {
      const SelectSource& s = stmt->Sources()[si];
      if (s.unnest || s.is_lateral || s.sample || !s.using_columns.empty() ||
          s.from_nested_join ||
          (s.join_type != JoinType::kCross &&
           s.join_type != JoinType::kInner)) {
        srcs_ok = false;
        break;
      }
      const std::string al = s.alias.empty() ? s.table : s.alias;
      if (al.empty() || !seen_al.insert(LoweredStarName(al)).second) {
        srcs_ok = false;
        break;
      }
      srcs[si].table = s.table;
      srcs[si].alias = al;
      if (s.query != nullptr) {
        if (!populate_query_schema(*s.query, &srcs[si].schema)) {
          srcs_ok = false;
          break;
        }
      } else if (const auto cte_it =
                     visible_ctes.find(LoweredStarName(s.table));
                 cte_it != visible_ctes.end()) {
        if (cte_it->second == nullptr ||
            !populate_query_schema(*cte_it->second, &srcs[si].schema)) {
          srcs_ok = false;
          break;
        }
      } else {
        StatusOr<std::shared_ptr<Table>> t = ctx.GetTable(s.table);
        if (!t.HasValue() || t.Value() == nullptr) {
          srcs_ok = false;
          break;
        }
        srcs[si].tbl = t.Value();
        srcs[si].schema = t.Value()->GetSchema();
        srcs[si].is_base = true;
        uint64_t est = t.Value()->BackingColumnarRowCount();
        if (auto st = ctx.GetStats(s.table); st.HasValue()) {
          est = std::max<uint64_t>(est, st.Value()->Rows());
        }
        srcs[si].est_rows = est;
      }
    }
    if (!srcs_ok) {
      break;
    }

    auto resolve_col =
        [&](const ColumnName& cn) -> std::optional<std::pair<size_t, size_t>> {
      if (cn.name.empty() || cn.name == "*") {
        return std::nullopt;
      }
      if (!cn.schema.empty()) {
        for (size_t si = 0; si < srcs.size(); ++si) {
          if (IdentifierEquals(cn.schema, srcs[si].alias) ||
              (stmt->Sources()[si].alias.empty() && !srcs[si].table.empty() &&
               IdentifierEquals(cn.schema, srcs[si].table))) {
            const int off = srcs[si].schema.Offset(ColumnName("", cn.name));
            if (off >= 0) {
              return std::pair{si, static_cast<size_t>(off)};
            }
          }
        }
        return std::nullopt;
      }
      std::optional<std::pair<size_t, size_t>> hit;
      for (size_t si = 0; si < srcs.size(); ++si) {
        const int off = srcs[si].schema.Offset(ColumnName("", cn.name));
        if (off >= 0) {
          if (hit.has_value()) {
            return std::nullopt;
          }
          hit = std::pair{si, static_cast<size_t>(off)};
        }
      }
      return hit;
    };

    std::vector<Expression> all_preds = SplitConjuncts(stmt->WhereClause());
    for (size_t si = 1; si < stmt->Sources().size(); ++si) {
      if (stmt->Sources()[si].join_condition) {
        for (Expression& c :
             SplitConjuncts(stmt->Sources()[si].join_condition)) {
          all_preds.push_back(std::move(c));
        }
      }
    }

    bool dropped_one = false;
    for (size_t dim_si = 0; dim_si < srcs.size(); ++dim_si) {
      if (!srcs[dim_si].is_base || srcs[dim_si].est_rows > 250000) {
        continue;
      }
      size_t dim_pred_cnt = 0;
      size_t pk_slot = 0;
      Expression fk_col_expr;
      std::vector<Expression> other_preds;
      bool preds_ok = true;
      for (const Expression& pr : all_preds) {
        if (!pr) {
          continue;
        }
        bool touches_dim = false;
        if (ContainsQueryExpression(pr)) {
          // Conservatively block if correlated to dim_si.
          for (const ColumnName& cn : pr->TouchedColumns()) {
            auto r = resolve_col(cn);
            if (!r.has_value() || r->first == dim_si) {
              preds_ok = false;
              break;
            }
          }
          if (!preds_ok ||
              ExprMayReferenceDimInsideSubquery(
                  pr, srcs[dim_si].alias,
                  stmt->Sources()[dim_si].alias.empty() ? srcs[dim_si].table
                                                        : std::string_view{},
                  srcs[dim_si].schema)) {
            preds_ok = false;
            break;
          }
          other_preds.push_back(pr);
          continue;
        }
        for (const ColumnName& cn : pr->TouchedColumns()) {
          auto r = resolve_col(cn);
          if (!r.has_value()) {
            preds_ok = false;
            break;
          }
          if (r->first == dim_si) {
            touches_dim = true;
          }
        }
        if (!preds_ok) {
          break;
        }
        if (!touches_dim) {
          other_preds.push_back(pr);
          continue;
        }
        ++dim_pred_cnt;
        if (dim_pred_cnt > 1 || pr->Type() != TypeTag::kBinaryExp ||
            pr->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
          preds_ok = false;
          break;
        }
        const Expression& l = pr->AsBinaryExpression().Left();
        const Expression& r = pr->AsBinaryExpression().Right();
        if (!l || !r || l->Type() != TypeTag::kColumnValue ||
            r->Type() != TypeTag::kColumnValue) {
          preds_ok = false;
          break;
        }
        auto lr = resolve_col(l->AsColumnValue().GetColumnName());
        auto rr = resolve_col(r->AsColumnValue().GetColumnName());
        if (!lr.has_value() || !rr.has_value() ||
            (lr->first == dim_si) == (rr->first == dim_si)) {
          preds_ok = false;
          break;
        }
        const auto dim_r = (lr->first == dim_si) ? *lr : *rr;
        const auto oth_r = (lr->first == dim_si) ? *rr : *lr;
        if (!srcs[oth_r.first].is_base) {
          preds_ok = false;
          break;
        }
        const Column& pkc = srcs[dim_si].schema.GetColumn(dim_r.second);
        const Column& fkc = srcs[oth_r.first].schema.GetColumn(oth_r.second);
        if (pkc.Type() != ValueType::kInt64 || pkc.IsUnsigned() ||
            fkc.Type() != ValueType::kInt64 || fkc.IsUnsigned()) {
          preds_ok = false;
          break;
        }
        pk_slot = dim_r.second;
        fk_col_expr = ColumnValueExp(
            ColumnName(srcs[oth_r.first].alias, fkc.Name().name));
      }
      if (!preds_ok || dim_pred_cnt != 1 || !fk_col_expr) {
        continue;
      }

      // Check that every reference to `dim_si` in SelectList/GroupBy/Having/
      // OrderBy/Qualify/DISTINCT ON is ONLY `dim_si.pk_slot` (which equals
      // `fk_col_expr` bit-for-bit on INT64!).
      bool only_pk_refs = true;
      auto check_expr = [&](auto& self, const Expression& e) -> bool {
        if (!e) {
          return true;
        }
        if (e->Type() == TypeTag::kQueryExp) {
          const auto& q = e->AsQueryExpression();
          if (q.Test() && !self(self, q.Test())) {
            return false;
          }
          return q.Query() != nullptr &&
                 !SubqueryMayReferenceDimSource(
                     *q.Query(), srcs[dim_si].alias,
                     stmt->Sources()[dim_si].alias.empty() ? srcs[dim_si].table
                                                           : std::string_view{},
                     srcs[dim_si].schema);
        }
        if (e->Type() == TypeTag::kColumnValue) {
          const ColumnName& cn = e->AsColumnValue().GetColumnName();
          if (cn.name == "*") {
            return true;
          }
          auto r = resolve_col(cn);
          if (!r.has_value()) {
            return false;
          }
          if (r->first == dim_si) {
            return r->second == pk_slot;
          }
          return true;
        }
        for (const Expression& ch : ExpressionChildren(e)) {
          if (!self(self, ch)) {
            return false;
          }
        }
        return true;
      };
      for (const NamedExpression& it : stmt->SelectList()) {
        if (!check_expr(check_expr, it.expression)) {
          only_pk_refs = false;
          break;
        }
      }
      for (const Expression& g : stmt->GroupBy()) {
        if (!only_pk_refs || !check_expr(check_expr, g)) {
          only_pk_refs = false;
          break;
        }
      }
      if (!only_pk_refs || !check_expr(check_expr, stmt->Having()) ||
          !check_expr(check_expr, stmt->Qualify())) {
        continue;
      }
      for (const auto& ot : stmt->OrderBy()) {
        if (!check_expr(check_expr, ot.expression)) {
          only_pk_refs = false;
          break;
        }
      }
      for (const Expression& key : stmt->DistinctOn()) {
        if (!check_expr(check_expr, key)) {
          only_pk_refs = false;
          break;
        }
      }
      if (!only_pk_refs) {
        continue;
      }

      const std::string& pk_name =
          srcs[dim_si].schema.GetColumn(pk_slot).Name().name;
      const RuntimeUniqueColInfo uinfo =
          InspectRuntimeUniqueNonNullColumn(ctx, srcs[dim_si].table, pk_name);
      if (!uinfo.unique_non_null) {
        continue;
      }
      if (uinfo.row_count > 0 &&
          (uinfo.max_int < uinfo.min_int ||
           static_cast<uint64_t>(uinfo.max_int) -
                   static_cast<uint64_t>(uinfo.min_int) + 1ULL !=
               uinfo.row_count)) {
        continue;
      }

      auto subst_pk = [&](const Expression& e) -> Expression {
        if (!e) {
          return e;
        }
        return ReplaceSubExpressions(
            e, [&](const Expression& node) -> std::optional<Expression> {
              if (!node || node->Type() != TypeTag::kColumnValue) {
                return std::nullopt;
              }
              const ColumnName& cn = node->AsColumnValue().GetColumnName();
              if (cn.name == "*") {
                return std::nullopt;
              }
              auto r = resolve_col(cn);
              if (r.has_value() && r->first == dim_si && r->second == pk_slot) {
                return fk_col_expr;
              }
              return std::nullopt;
            });
      };

      std::vector<NamedExpression> new_sel;
      new_sel.reserve(stmt->SelectList().size());
      for (size_t i = 0; i < stmt->SelectList().size(); ++i) {
        new_sel.emplace_back(
            InferSelectItemOutputName(stmt->SelectList()[i], i),
            subst_pk(stmt->SelectList()[i].expression));
      }
      std::vector<Expression> new_grp;
      new_grp.reserve(stmt->GroupBy().size());
      for (const Expression& g : stmt->GroupBy()) {
        new_grp.push_back(subst_pk(g));
      }
      std::vector<SelectStatement::OrderByTerm> new_ord = stmt->OrderBy();
      for (auto& ot : new_ord) {
        ot.expression = subst_pk(ot.expression);
      }

      std::vector<SelectSource> new_sources;
      new_sources.reserve(stmt->Sources().size() - 1);
      for (size_t si = 0; si < stmt->Sources().size(); ++si) {
        if (si == dim_si) {
          continue;
        }
        SelectSource cp = stmt->Sources()[si];
        if (cp.alias.empty() && !cp.table.empty()) {
          cp.alias = cp.table;
        }
        cp.join_type = JoinType::kCross;
        cp.join_condition = nullptr;
        new_sources.push_back(std::move(cp));
      }

      if (uinfo.row_count == 0) {
        other_preds.push_back(BinaryExpressionExp(
            ConstantValueExp(Value(int64_t{0})), BinaryOperation::kEquals,
            ConstantValueExp(Value(int64_t{1}))));
      } else {
        other_preds.push_back(BinaryExpressionExp(
            fk_col_expr, BinaryOperation::kGreaterThanEquals,
            ConstantValueExp(Value(uinfo.min_int))));
        other_preds.push_back(
            BinaryExpressionExp(fk_col_expr, BinaryOperation::kLessThanEquals,
                                ConstantValueExp(Value(uinfo.max_int))));
      }

      std::vector<Expression> new_distinct_on;
      new_distinct_on.reserve(stmt->DistinctOn().size());
      for (const Expression& key : stmt->DistinctOn()) {
        new_distinct_on.push_back(subst_pk(key));
      }
      stmt->SetSources(std::move(new_sources));
      stmt->SetWhereClause(CombineConjuncts(other_preds));
      stmt->SetSelectList(std::move(new_sel));
      stmt->SetGroupBy(std::move(new_grp));
      stmt->SetOrderBy(std::move(new_ord));
      if (!new_distinct_on.empty()) {
        stmt->SetDistinctOn(std::move(new_distinct_on));
      }
      if (stmt->Having()) {
        stmt->SetHaving(subst_pk(stmt->Having()));
      }
      if (stmt->Qualify()) {
        stmt->SetQualify(subst_pk(stmt->Qualify()));
      }
      any_eliminated = true;
      dropped_one = true;
      break;
    }
    if (!dropped_one) {
      break;
    }
  }
  return any_eliminated;
}
}  // namespace

namespace {
bool EliminateUnreferencedPkImpl(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt,
    std::unordered_map<std::string, const SelectStatement*> visible_ctes,
    TransactionContext& ctx, std::unordered_set<SelectStatement*>* visited) {
  if (stmt == nullptr) {
    return false;
  }
  bool changed = false;
  for (const std::string& name : stmt->WithQueryOrder()) {
    const auto it = stmt->WithQueries().find(name);
    const SelectStatement* body_ptr =
        (it != stmt->WithQueries().end()) ? it->second.get() : nullptr;
    if (!stmt->IsRecursiveWith(name) && body_ptr != nullptr) {
      changed |= EliminateUnreferencedPkImpl(it->second.get(), visible_ctes,
                                             ctx, visited);
      visible_ctes[LoweredStarName(name)] = body_ptr;
    } else {
      visible_ctes[LoweredStarName(name)] = nullptr;
    }
  }
  for (const auto& [name, body] : stmt->WithQueries()) {
    (void)body;
    if (!visible_ctes.contains(LoweredStarName(name))) {
      visible_ctes[LoweredStarName(name)] = nullptr;
    }
  }
  for (const SelectSource& src : stmt->Sources()) {
    if (src.query != nullptr && !src.is_lateral) {
      changed |= EliminateUnreferencedPkImpl(src.query.get(), visible_ctes, ctx,
                                             visited);
    }
  }
  if (!stmt->UnionAll().empty() || stmt->GetSetOperationTree() != nullptr) {
    if (!stmt->UnionAll().empty() &&
        (relational_detail::CanUseExactIntegerGroupingSets(ctx, *stmt) ||
         HasSharedScanUnionAllBranches(*stmt))) {
      return changed;
    }
    changed |= VisitSetOpLeavesAndSync(stmt, visited, [&](SelectStatement* br) {
      return EliminateUnreferencedPkInOneSelect(br, visible_ctes, ctx);
    });
  } else {
    changed |= EliminateUnreferencedPkInOneSelect(stmt, visible_ctes, ctx);
  }
  return changed;
}
}  // namespace

namespace {
bool DistributeDistinctUnionDimJoinsOneSelect(
    SelectStatement* stmt, const std::unordered_set<std::string>& cte_names,
    TransactionContext& ctx) {
  if (stmt == nullptr || !stmt->Distinct() || stmt->HasDistinctOn() ||
      !stmt->GroupBy().empty() || stmt->IsGroupAll() || stmt->Having() ||
      stmt->Qualify() || stmt->HasLimit() || stmt->Offset() != 0 ||
      !stmt->OrderBy().empty() || stmt->WithTies() || stmt->ForUpdate() ||
      stmt->AsStruct() || !stmt->WithQueries().empty() ||
      !stmt->UnionAll().empty() || stmt->GetSetOperationTree() != nullptr ||
      relational_detail::HasWindowFunctions(*stmt) ||
      SelectListHasStarOrModifiers(*stmt) || TpcdsStmtHasParam(*stmt) ||
      stmt->Sources().size() < 3) {
    return false;
  }

  std::optional<size_t> u_idx;
  std::vector<std::shared_ptr<SelectStatement>> u_branches;
  for (size_t si = 0; si < stmt->Sources().size(); ++si) {
    const SelectSource& s = stmt->Sources()[si];
    if (s.unnest || s.is_lateral || s.sample || !s.using_columns.empty() ||
        s.from_nested_join ||
        (s.join_type != JoinType::kCross && s.join_type != JoinType::kInner)) {
      return false;
    }
    if (s.query != nullptr) {
      if (u_idx.has_value() || s.alias.empty() ||
          !ExtractPureUnionAllBranches(*s.query, &u_branches) ||
          u_branches.size() < 2) {
        return false;
      }
      u_idx = si;
    } else if (s.table.empty() ||
               cte_names.contains(LoweredStarName(s.table))) {
      return false;
    }
  }
  if (!u_idx.has_value()) {
    return false;
  }

  const SelectSource& u_src = stmt->Sources()[*u_idx];
  const std::string u_alias = u_src.alias;
  const size_t arity = u_branches[0]->SelectList().size();
  if (arity == 0 || SelectListHasStarOrModifiers(*u_branches[0])) {
    return false;
  }
  std::vector<std::string> u_col_names;
  u_col_names.reserve(arity);
  std::unordered_set<std::string> seen_ucols;  // NOLINT(modernize-loop-convert)
  for (size_t c = 0; c < arity; ++c) {
    std::string nm = ExplicitOrColumnOutputName(u_branches[0]->SelectList()[c]);
    if (nm.empty() || !seen_ucols.insert(LoweredStarName(nm)).second) {
      return false;
    }
    u_col_names.push_back(std::move(nm));
  }
  for (auto& u_branche : u_branches) {  // NOLINT(modernize-loop-convert)
    const SelectStatement& br = *u_branche;
    if (br.SelectList().size() != arity || SelectListHasStarOrModifiers(br) ||
        !br.GroupBy().empty() || br.IsGroupAll() || br.Having() ||
        br.Distinct() || br.HasLimit() || br.Offset() != 0 ||
        !br.WithQueries().empty() ||
        relational_detail::HasWindowFunctions(br)) {
      return false;
    }
  }

  struct OuterRel {
    std::string table;
    std::string alias;
    Schema schema;  // NOLINT(performance-inefficient-vector-operation)
    uint64_t est_rows{0};
  };
  std::vector<OuterRel> rels(stmt->Sources().size());
  for (size_t si = 0; si < stmt->Sources().size(); ++si) {
    if (si == *u_idx) {
      std::vector<Column> cols;
      cols.reserve(u_col_names.size());
      for (const std::string& nm : u_col_names) {
        cols.emplace_back(
            nm, ValueType::
                    kNull);  // NOLINT(performance-inefficient-vector-operation)
      }
      rels[si] = {
          .table = "", .alias = u_alias, .schema = Schema("", std::move(cols))};
      continue;
    }
    const SelectSource& s = stmt->Sources()[si];
    StatusOr<std::shared_ptr<Table>> tbl = ctx.GetTable(s.table);
    if (!tbl.HasValue() || tbl.Value() == nullptr) {
      return false;
    }
    uint64_t est = tbl.Value()->BackingColumnarRowCount();
    if (auto st = ctx.GetStats(s.table); st.HasValue()) {
      est = std::max<uint64_t>(est, st.Value()->Rows());
    }
    rels[si] = {.table = s.table,
                .alias = s.alias.empty() ? s.table : s.alias,
                .schema = tbl.Value()->GetSchema(),
                .est_rows = est};
  }

  auto resolve_col =
      [&](const ColumnName& cn) -> std::optional<std::pair<size_t, size_t>> {
    if (cn.name.empty() || cn.name == "*") {
      return std::nullopt;
    }
    if (!cn.schema.empty()) {
      for (size_t si = 0; si < rels.size(); ++si) {
        if (IdentifierEquals(cn.schema, rels[si].alias) ||
            (stmt->Sources()[si].alias.empty() && !rels[si].table.empty() &&
             IdentifierEquals(cn.schema, rels[si].table))) {
          const int off = rels[si].schema.Offset(ColumnName("", cn.name));
          if (off >= 0) {
            return std::pair{si, static_cast<size_t>(off)};
          }
        }
      }
      return std::nullopt;
    }
    std::optional<std::pair<size_t, size_t>> hit;
    for (size_t si = 0; si < rels.size(); ++si) {
      const int off = rels[si].schema.Offset(ColumnName("", cn.name));
      if (off >= 0) {
        if (hit.has_value()) {
          return std::nullopt;
        }
        hit = std::pair{si, static_cast<size_t>(off)};
      }
    }
    return hit;
  };

  // Identify sources referenced in `SelectList`.
  std::unordered_set<size_t> select_srcs;
  for (const NamedExpression& it : stmt->SelectList()) {
    if (!it.expression || ContainsQueryExpression(it.expression) ||
        relational_detail::ContainsAggregate(it.expression)) {
      return false;
    }
    for (const ColumnName& cn : it.expression->TouchedColumns()) {
      auto r = resolve_col(cn);
      if (!r.has_value()) {
        return false;
      }
      select_srcs.insert(r->first);
    }
  }
  if (select_srcs.contains(*u_idx)) {
    return false;
  }

  std::vector<Expression> all_preds = SplitConjuncts(stmt->WhereClause());
  for (size_t si = 1; si < stmt->Sources().size(); ++si) {
    if (stmt->Sources()[si].join_condition) {
      for (Expression& c : SplitConjuncts(stmt->Sources()[si].join_condition)) {
        all_preds.push_back(std::move(c));
      }
    }
  }

  // Pushable dimensions: small base tables not in `select_srcs`, with >= 1
  // local filter predicate, whose cross-predicates ONLY connect to `*u_idx`.
  std::vector<bool> has_local_pred(rels.size(), false);
  std::vector<bool> touches_keep(rels.size(), false);
  for (const Expression& pr : all_preds) {
    if (!pr || ContainsQueryExpression(pr) ||
        !DerivedExpressionIsImmutable(pr)) {
      return false;
    }
    std::unordered_set<size_t> ps;
    for (const ColumnName& cn : pr->TouchedColumns()) {
      auto r = resolve_col(cn);
      if (!r.has_value()) {
        return false;
      }
      ps.insert(r->first);
    }
    if (ps.size() == 1) {
      has_local_pred[*ps.begin()] = true;
    } else {
      for (size_t a : ps) {
        for (size_t b : ps) {
          if (a != b && a != *u_idx && select_srcs.contains(b)) {
            touches_keep[a] = true;
          }
        }
      }
    }
  }

  std::unordered_set<size_t> push_set;
  for (size_t si = 0; si < rels.size(); ++si) {
    if (si != *u_idx && !select_srcs.contains(si) && has_local_pred[si] &&
        !touches_keep[si] && rels[si].est_rows <= 100000) {
      push_set.insert(si);
    }
  }
  if (push_set.empty()) {
    return false;
  }
  // Verify no push_set table joins to a non-push_set non-u_idx table.
  std::vector<Expression> pushed_preds;
  std::vector<Expression> kept_preds;
  std::vector<size_t> bridge_u_cols;
  for (const Expression& pr : all_preds) {
    std::unordered_set<size_t> ps;
    std::vector<size_t> u_cols_in_pr;
    for (const ColumnName& cn : pr->TouchedColumns()) {
      auto r = resolve_col(cn);
      ps.insert(r->first);
      if (r->first == *u_idx) {
        u_cols_in_pr.push_back(r->second);
      }
    }
    bool any_push = false;
    bool any_keep = false;
    for (size_t s : ps) {
      if (push_set.contains(s)) {
        any_push = true;
      } else if (s != *u_idx) {
        any_keep = true;
      }
    }
    if (any_push && any_keep) {
      return false;
    }
    if (any_push || (ps.size() == 1 && ps.contains(*u_idx))) {
      pushed_preds.push_back(pr);
    } else {
      kept_preds.push_back(pr);
      for (size_t uc : u_cols_in_pr) {
        if (std::ranges::find(bridge_u_cols, uc) == bridge_u_cols.end()) {
          bridge_u_cols.push_back(uc);
        }
      }
    }
  }
  if (bridge_u_cols.empty() || bridge_u_cols.size() > 2) {
    return false;
  }

  // Rewrite each branch of `u_branches` as a wrapped derived query joining
  // `push_set` and grouping on `bridge_u_cols`, combined via UNION DISTINCT.
  std::vector<std::shared_ptr<SelectStatement>> new_branches;
  new_branches.reserve(u_branches.size());
  for (size_t bi = 0; bi < u_branches.size(); ++bi) {
    const std::string sub_u_alias = "__tpcds_ubr_" + std::to_string(bi);
    auto br_copy = std::make_shared<SelectStatement>(*u_branches[bi]);
    std::vector<SelectSource> br_srcs;
    br_srcs.emplace_back("", sub_u_alias, std::move(br_copy), JoinType::kCross,
                         nullptr);
    for (size_t si = 0; si < stmt->Sources().size(); ++si) {
      if (!push_set.contains(si)) {
        continue;
      }
      SelectSource ps = stmt->Sources()[si];
      ps.alias = rels[si].alias;
      ps.join_type = JoinType::kCross;
      ps.join_condition = nullptr;
      br_srcs.push_back(std::move(ps));
    }
    auto rebind_pushed = [&](const Expression& e) -> Expression {
      return ReplaceSubExpressions(
          e, [&](const Expression& node) -> std::optional<Expression> {
            if (!node || node->Type() != TypeTag::kColumnValue) {
              return std::nullopt;
            }
            auto r = resolve_col(node->AsColumnValue().GetColumnName());
            if (!r.has_value()) {
              return std::nullopt;
            }
            if (r->first == *u_idx) {
              return ColumnValueExp(
                  ColumnName(sub_u_alias, u_col_names[r->second]));
            }
            return ColumnValueExp(ColumnName(
                rels[r->first].alias,
                rels[r->first].schema.GetColumn(r->second).Name().name));
          });
    };
    std::vector<Expression> br_where;
    br_where.reserve(pushed_preds.size());
    for (const Expression& pp : pushed_preds) {
      br_where.push_back(rebind_pushed(pp));
    }
    std::vector<NamedExpression> br_sel;
    std::vector<Expression> br_grp;
    for (size_t uc : bridge_u_cols) {
      Expression ce = ColumnValueExp(ColumnName(sub_u_alias, u_col_names[uc]));
      br_sel.emplace_back(u_col_names[uc], ce);
      br_grp.push_back(ce);
    }
    auto wrapped = std::make_shared<SelectStatement>();
    wrapped->SetSources(std::move(br_srcs));
    wrapped->SetWhereClause(CombineConjuncts(br_where));
    wrapped->SetGroupBy(std::move(br_grp));
    wrapped->SetSelectList(std::move(br_sel));
    new_branches.push_back(std::move(wrapped));
  }

  auto union_stmt = std::make_shared<SelectStatement>(*new_branches[0]);
  for (size_t bi = 1; bi < new_branches.size(); ++bi) {
    union_stmt->AddSetOperation(SetOperationKind::kUnion, new_branches[bi]);
  }

  auto qualify_kept = [&](const Expression& e) -> Expression {
    return ReplaceSubExpressions(
        e, [&](const Expression& node) -> std::optional<Expression> {
          if (!node || node->Type() != TypeTag::kColumnValue) {
            return std::nullopt;
          }
          auto r = resolve_col(node->AsColumnValue().GetColumnName());
          if (!r.has_value()) {
            return std::nullopt;
          }
          if (r->first == *u_idx) {
            return ColumnValueExp(ColumnName(u_alias, u_col_names[r->second]));
          }
          return ColumnValueExp(ColumnName(
              rels[r->first].alias,
              rels[r->first].schema.GetColumn(r->second).Name().name));
        });
  };

  std::vector<SelectSource> new_outer_srcs;
  new_outer_srcs.reserve(1 + stmt->Sources().size());
  new_outer_srcs.emplace_back("", u_alias, std::move(union_stmt),
                              JoinType::kCross, nullptr);
  for (size_t si = 0; si < stmt->Sources().size(); ++si) {
    if (si == *u_idx || push_set.contains(si)) {
      continue;
    }
    SelectSource ks =
        stmt->Sources()
            [si];  // NOLINT(performance-inefficient-vector-operation)
    ks.alias = rels[si].alias;
    ks.join_type = JoinType::kCross;
    ks.join_condition = nullptr;
    new_outer_srcs.push_back(
        std::move(ks));  // NOLINT(performance-inefficient-vector-operation)
  }
  std::vector<Expression> new_outer_where;
  new_outer_where.reserve(kept_preds.size());
  for (const Expression& kp : kept_preds) {
    new_outer_where.push_back(
        qualify_kept(kp));  // NOLINT(performance-inefficient-vector-operation)
  }
  std::vector<NamedExpression> new_outer_sel;
  new_outer_sel.reserve(stmt->SelectList().size());
  for (size_t i = 0; i < stmt->SelectList().size(); ++i) {
    new_outer_sel
        .emplace_back(  // NOLINT(performance-inefficient-vector-operation)
            InferSelectItemOutputName(stmt->SelectList()[i], i),
            qualify_kept(stmt->SelectList()[i].expression));
  }
  stmt->SetSources(std::move(new_outer_srcs));
  stmt->SetWhereClause(CombineConjuncts(new_outer_where));
  stmt->SetSelectList(std::move(new_outer_sel));
  return true;
}
}  // namespace

}  // namespace select_rewrite_detail

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (bijective_dim_key_cte_deferral_sound)
bool DeferUniqueDimJoinPastGroupBy(SelectStatement* outer,
                                   TransactionContext& ctx) {
  std::unordered_set<SelectStatement*> visited;
  return DeferUniqueDimJoinPastGroupByImpl(outer, ctx, &visited);
}

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (unreferenced_unique_pk_inner_join_to_semi_sound)
bool EliminateUnreferencedUniquePkInnerJoins(SelectStatement* outer,
                                             TransactionContext& ctx) {
  std::unordered_set<SelectStatement*> visited;
  return EliminateUnreferencedPkImpl(outer, {}, ctx, &visited);
}

namespace select_rewrite_detail {

namespace {
bool IsGuaranteedSingletonScalarAggQuery(const SelectStatement& q,
                                         std::string* out_col_name) {
  if (!q.GroupBy().empty() || q.IsGroupAll() || q.Having() || q.Qualify() ||
      q.Distinct() || q.HasDistinctOn() || q.HasLimit() || q.Offset() != 0 ||
      q.WithTies() || q.ForUpdate() || q.AsStruct() || !q.OrderBy().empty() ||
      !q.UnionAll().empty() || q.GetSetOperationTree() != nullptr ||
      !q.WithQueries().empty() || q.Sources().empty() ||
      q.SelectList().size() != 1 || SelectListHasStarOrModifiers(q) ||
      TpcdsStmtHasParam(q) || relational_detail::HasWindowFunctions(q)) {
    return false;
  }
  const Expression& se = q.SelectList()[0].expression;
  if (!se || se->Type() != TypeTag::kAggregateExp) {
    return false;
  }
  const auto& agg = se->AsAggregateExpression();
  if ((agg.GetType() != AggregationType::kMin &&
       agg.GetType() != AggregationType::kMax &&
       agg.GetType() != AggregationType::kSum &&
       agg.GetType() != AggregationType::kAvg &&
       agg.GetType() != AggregationType::kCount) ||
      agg.NeedsGroupContext() || !agg.InnerGroupBy().empty() ||
      agg.WhereFilter() || agg.Having() != AggregateHavingModifier::kNone ||
      agg.HavingCondition() || !agg.InnerOrderBy().empty() ||
      agg.SecondaryArg() || !agg.TrailingArgs().empty() ||
      (agg.Child() != nullptr && !DerivedExpressionIsImmutable(agg.Child()))) {
    return false;
  }
  std::string col = InferSelectItemOutputName(q.SelectList()[0], 0);
  if (col.empty()) {
    return false;
  }
  if (out_col_name != nullptr) {
    *out_col_name = std::move(col);
  }
  return true;
}
}  // namespace

namespace {
bool EliminateSingletonScalarCrossJoinInOneSelect(
    SelectStatement* stmt,
    const std::unordered_map<std::string, const SelectStatement*>& visible_ctes,
    const std::unordered_set<std::string>& top_level_singleton_ctes,
    TransactionContext& ctx) {
  if (stmt == nullptr || stmt->Sources().size() < 2 ||
      stmt->GroupBy().empty() || stmt->IsGroupAll() || stmt->ForUpdate() ||
      stmt->AsStruct() || SelectListHasStarOrModifiers(*stmt) ||
      TpcdsStmtHasParam(*stmt) || !stmt->UnionAll().empty() ||
      stmt->GetSetOperationTree() != nullptr ||
      relational_detail::HasWindowFunctions(*stmt)) {
    return false;
  }

  bool any_changed = false;
  for (size_t iter = 0; iter < 4 && stmt->Sources().size() >= 2; ++iter) {
    struct SrcEntry {
      std::string table;
      std::string alias;
      Schema schema;
      std::shared_ptr<SelectStatement> scalar_subquery;
    };
    std::vector<SrcEntry> srcs(stmt->Sources().size());
    std::unordered_set<std::string> seen_al;
    bool srcs_ok = true;
    auto populate_query_schema = [&](const SelectStatement& q,
                                     Schema* out_schema) -> bool {
      const SelectStatement* qhead = &q;
      if (qhead->SelectList().empty() &&
          qhead->GetSetOperationTree() != nullptr &&
          qhead->GetSetOperationTree()->first != nullptr) {
        qhead = qhead->GetSetOperationTree()->first.get();
      }
      if (qhead->SelectList().empty() || SelectListHasStarOrModifiers(*qhead)) {
        return false;
      }
      std::vector<Column> cols;
      cols.reserve(qhead->SelectList().size());
      for (size_t ci = 0; ci < qhead->SelectList().size(); ++ci) {
        cols.emplace_back(
            InferSelectItemOutputName(qhead->SelectList()[ci], ci),
            ValueType::kNull);
      }
      *out_schema = Schema("", std::move(cols));
      return true;
    };

    for (size_t si = 0; si < stmt->Sources().size(); ++si) {
      const SelectSource& s = stmt->Sources()[si];
      if (s.unnest || s.is_lateral || s.sample || !s.using_columns.empty() ||
          s.from_nested_join ||
          (s.join_type != JoinType::kCross &&
           s.join_type != JoinType::kInner)) {
        srcs_ok = false;
        break;
      }
      const std::string al = s.alias.empty() ? s.table : s.alias;
      if (al.empty() || !seen_al.insert(LoweredStarName(al)).second) {
        srcs_ok = false;
        break;
      }
      srcs[si].table = s.table;
      srcs[si].alias = al;
      if (s.query != nullptr) {
        if (!populate_query_schema(*s.query, &srcs[si].schema)) {
          srcs_ok = false;
          break;
        }
        std::string sc_col;
        if (!s.join_condition &&
            IsGuaranteedSingletonScalarAggQuery(*s.query, &sc_col) &&
            relational_detail::StatementUsesOnlyScopes(ctx, *s.query, {}, {})) {
          srcs[si].scalar_subquery = s.query;
        }
      } else if (const auto cte_it =
                     visible_ctes.find(LoweredStarName(s.table));
                 cte_it != visible_ctes.end()) {
        if (cte_it->second == nullptr ||
            !populate_query_schema(*cte_it->second, &srcs[si].schema)) {
          srcs_ok = false;
          break;
        }
        if (!s.join_condition &&
            top_level_singleton_ctes.contains(LoweredStarName(s.table)) &&
            srcs[si].schema.ColumnCount() == 1) {
          const std::string col_nm = srcs[si].schema.GetColumn(0).Name().name;
          auto sq = std::make_shared<SelectStatement>();
          SelectSource csrc;
          csrc.table = s.table;
          csrc.alias = s.table;
          sq->SetSources({std::move(csrc)});
          sq->SetSelectList({NamedExpression(
              col_nm, ColumnValueExp(ColumnName(s.table, col_nm)))});
          srcs[si].scalar_subquery = std::move(sq);
        }
      } else {
        StatusOr<std::shared_ptr<Table>> t = ctx.GetTable(s.table);
        if (!t.HasValue() || t.Value() == nullptr) {
          srcs_ok = false;
          break;
        }
        srcs[si].schema = t.Value()->GetSchema();
      }
    }
    if (!srcs_ok) {
      break;
    }

    auto resolve_col =
        [&](const ColumnName& cn) -> std::optional<std::pair<size_t, size_t>> {
      if (cn.name.empty() || cn.name == "*") {
        return std::nullopt;
      }
      if (!cn.schema.empty()) {
        for (size_t si = 0; si < srcs.size(); ++si) {
          if (IdentifierEquals(cn.schema, srcs[si].alias) ||
              (stmt->Sources()[si].alias.empty() && !srcs[si].table.empty() &&
               IdentifierEquals(cn.schema, srcs[si].table))) {
            const int off = srcs[si].schema.Offset(ColumnName("", cn.name));
            if (off >= 0) {
              return std::pair{si, static_cast<size_t>(off)};
            }
          }
        }
        return std::nullopt;
      }
      std::optional<std::pair<size_t, size_t>> hit;
      for (size_t si = 0; si < srcs.size(); ++si) {
        const int off = srcs[si].schema.Offset(ColumnName("", cn.name));
        if (off >= 0) {
          if (hit.has_value()) {
            return std::nullopt;
          }
          hit = std::pair{si, static_cast<size_t>(off)};
        }
      }
      return hit;
    };

    std::vector<Expression> all_preds = SplitConjuncts(stmt->WhereClause());
    for (size_t si = 1; si < stmt->Sources().size(); ++si) {
      if (stmt->Sources()[si].join_condition) {
        for (Expression& c :
             SplitConjuncts(stmt->Sources()[si].join_condition)) {
          all_preds.push_back(std::move(c));
        }
      }
    }

    bool dropped = false;
    for (size_t sc_si = 0; sc_si < srcs.size(); ++sc_si) {
      if (srcs[sc_si].scalar_subquery == nullptr ||
          srcs[sc_si].schema.ColumnCount() != 1) {
        continue;
      }
      const std::string_view sc_table = stmt->Sources()[sc_si].alias.empty()
                                            ? srcs[sc_si].table
                                            : std::string_view{};

      auto expr_touches_sc = [&](const Expression& e) -> bool {
        if (!e) {
          return false;
        }
        for (const ColumnName& cn : e->TouchedColumns()) {
          auto r = resolve_col(cn);
          if (!r.has_value() || r->first == sc_si) {
            return true;
          }
        }
        return ContainsQueryExpression(e) &&
               ExprMayReferenceDimInsideSubquery(e, srcs[sc_si].alias, sc_table,
                                                 srcs[sc_si].schema);
      };

      bool used_in_where_or_grp = false;
      for (const Expression& pr : all_preds) {
        if (expr_touches_sc(pr)) {
          used_in_where_or_grp = true;
          break;
        }
      }
      for (const Expression& gk : stmt->GroupBy()) {
        if (used_in_where_or_grp || expr_touches_sc(gk)) {
          used_in_where_or_grp = true;
          break;
        }
      }
      // DISTINCT ON is evaluated in the same scope as the select list; an
      // sc.quality reference there would dangle once the sc source is dropped.
      for (const Expression& key : stmt->DistinctOn()) {
        if (used_in_where_or_grp || expr_touches_sc(key)) {
          used_in_where_or_grp = true;
          break;
        }
      }
      if (used_in_where_or_grp || expr_touches_sc(stmt->Qualify())) {
        continue;
      }

      auto is_idem_sc_agg = [&](const Expression& node) -> bool {
        if (!node || node->Type() != TypeTag::kAggregateExp) {
          return false;
        }
        const auto& agg = node->AsAggregateExpression();
        if ((agg.GetType() != AggregationType::kMin &&
             agg.GetType() != AggregationType::kMax) ||
            agg.WhereFilter() ||
            agg.Having() != AggregateHavingModifier::kNone ||
            !agg.InnerOrderBy().empty() || agg.SecondaryArg() ||
            !agg.TrailingArgs().empty() || !agg.Child() ||
            agg.Child()->Type() != TypeTag::kColumnValue) {
          return false;
        }
        auto r = resolve_col(agg.Child()->AsColumnValue().GetColumnName());
        return r.has_value() && r->first == sc_si && r->second == 0;
      };

      auto validate_expr = [&](auto& self, const Expression& e) -> bool {
        if (!e) {
          return true;
        }
        if (is_idem_sc_agg(e)) {
          return true;
        }
        if (e->Type() == TypeTag::kQueryExp) {
          const auto& q = e->AsQueryExpression();
          if (q.Test() && !self(self, q.Test())) {
            return false;
          }
          return q.Query() != nullptr &&
                 !SubqueryMayReferenceDimSource(*q.Query(), srcs[sc_si].alias,
                                                sc_table, srcs[sc_si].schema);
        }
        if (e->Type() == TypeTag::kColumnValue) {
          const ColumnName& cn = e->AsColumnValue().GetColumnName();
          if (cn.name == "*") {
            return false;
          }
          auto r = resolve_col(cn);
          return r.has_value() && r->first != sc_si;
        }
        if (e->Type() == TypeTag::kAggregateExp) {
          const auto& agg = e->AsAggregateExpression();
          if ((agg.WhereFilter() && !self(self, agg.WhereFilter())) ||
              (agg.HavingCondition() && !self(self, agg.HavingCondition())) ||
              (agg.SecondaryArg() && !self(self, agg.SecondaryArg()))) {
            return false;
          }
          for (const Expression& ta : agg.TrailingArgs()) {
            if (!self(self, ta)) {
              return false;
            }
          }
          for (const auto& io : agg.InnerOrderBy()) {
            if (!self(self, io.expression)) {
              return false;
            }
          }
          for (const Expression& ig : agg.InnerGroupBy()) {
            if (!self(self, ig)) {
              return false;
            }
          }
        }
        for (const Expression& ch : ExpressionChildren(e)) {
          if (!self(self, ch)) {
            return false;
          }
        }
        return true;
      };

      bool refs_ok = true;
      for (const NamedExpression& it : stmt->SelectList()) {
        if (!validate_expr(validate_expr, it.expression)) {
          refs_ok = false;
          break;
        }
      }
      if (!refs_ok || !validate_expr(validate_expr, stmt->Having())) {
        continue;
      }
      for (const auto& ot : stmt->OrderBy()) {
        if (!validate_expr(validate_expr, ot.expression)) {
          refs_ok = false;
          break;
        }
      }
      for (const Expression& key : stmt->DistinctOn()) {
        if (!validate_expr(validate_expr, key)) {
          refs_ok = false;
          break;
        }
      }
      if (!refs_ok) {
        continue;
      }

      Expression replacement = QueryExpressionExp(srcs[sc_si].scalar_subquery);
      auto rewrite_expr = [&](const Expression& e) -> Expression {
        if (!e) {
          return e;
        }
        return ReplaceSubExpressions(
            e, [&](const Expression& node) -> std::optional<Expression> {
              if (is_idem_sc_agg(node)) {
                return replacement;
              }
              return std::nullopt;
            });
      };

      std::vector<NamedExpression> new_sel;
      new_sel.reserve(stmt->SelectList().size());
      for (size_t i = 0; i < stmt->SelectList().size(); ++i) {
        new_sel.emplace_back(
            InferSelectItemOutputName(stmt->SelectList()[i], i),
            rewrite_expr(stmt->SelectList()[i].expression));
      }
      std::vector<SelectStatement::OrderByTerm> new_ord = stmt->OrderBy();
      for (auto& ot : new_ord) {
        ot.expression = rewrite_expr(ot.expression);
      }
      std::vector<SelectSource> new_sources;
      new_sources.reserve(stmt->Sources().size() - 1);
      for (size_t si = 0; si < stmt->Sources().size(); ++si) {
        if (si == sc_si) {
          continue;
        }
        new_sources.push_back(stmt->Sources()[si]);
      }
      if (!new_sources.empty()) {
        new_sources[0].join_type = JoinType::kCross;
      }
      std::vector<Expression> new_distinct_on;
      new_distinct_on.reserve(stmt->DistinctOn().size());
      for (const Expression& key : stmt->DistinctOn()) {
        new_distinct_on.push_back(rewrite_expr(key));
      }
      stmt->SetSources(std::move(new_sources));
      stmt->SetSelectList(std::move(new_sel));
      stmt->SetOrderBy(std::move(new_ord));
      if (!new_distinct_on.empty()) {
        stmt->SetDistinctOn(std::move(new_distinct_on));
      }
      if (stmt->Having()) {
        stmt->SetHaving(rewrite_expr(stmt->Having()));
      }
      any_changed = true;
      dropped = true;
      break;
    }
    if (!dropped) {
      break;
    }
  }
  return any_changed;
}
}  // namespace

namespace {
bool EliminateSingletonScalarCrossJoinsImpl(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt,
    std::unordered_map<std::string, const SelectStatement*> visible_ctes,
    std::unordered_set<std::string> top_level_singleton_ctes, bool is_top_level,
    TransactionContext& ctx, std::unordered_set<SelectStatement*>* visited) {
  if (stmt == nullptr) {
    return false;
  }
  bool changed = false;
  for (const std::string& name : stmt->WithQueryOrder()) {
    const auto it = stmt->WithQueries().find(name);
    const SelectStatement* body_ptr =
        (it != stmt->WithQueries().end()) ? it->second.get() : nullptr;
    const std::string low = LoweredStarName(name);
    if (!stmt->IsRecursiveWith(name) && body_ptr != nullptr) {
      changed |= EliminateSingletonScalarCrossJoinsImpl(
          it->second.get(), visible_ctes, top_level_singleton_ctes, false, ctx,
          visited);
      visible_ctes[low] = body_ptr;
      std::string sc_col;
      if (is_top_level &&
          IsGuaranteedSingletonScalarAggQuery(*body_ptr, &sc_col)) {
        top_level_singleton_ctes.insert(low);
      } else {
        top_level_singleton_ctes.erase(low);
      }
    } else {
      visible_ctes[low] = nullptr;
      top_level_singleton_ctes.erase(low);
    }
  }
  for (const auto& [name, body] : stmt->WithQueries()) {
    (void)body;
    const std::string low = LoweredStarName(name);
    if (!visible_ctes.contains(low)) {
      visible_ctes[low] = nullptr;
      top_level_singleton_ctes.erase(low);
    }
  }
  for (const SelectSource& src : stmt->Sources()) {
    if (src.query != nullptr && !src.is_lateral) {
      changed |= EliminateSingletonScalarCrossJoinsImpl(
          src.query.get(), visible_ctes, top_level_singleton_ctes, false, ctx,
          visited);
    }
  }
  if (!stmt->UnionAll().empty() || stmt->GetSetOperationTree() != nullptr) {
    if (!stmt->UnionAll().empty() &&
        (relational_detail::CanUseExactIntegerGroupingSets(ctx, *stmt) ||
         HasSharedScanUnionAllBranches(*stmt))) {
      return changed;
    }
    changed |= VisitSetOpLeavesAndSync(stmt, visited, [&](SelectStatement* br) {
      return EliminateSingletonScalarCrossJoinInOneSelect(
          br, visible_ctes, top_level_singleton_ctes, ctx);
    });
  } else {
    changed |= EliminateSingletonScalarCrossJoinInOneSelect(
        stmt, visible_ctes, top_level_singleton_ctes, ctx);
  }
  return changed;
}
}  // namespace

}  // namespace select_rewrite_detail

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (singleton_scalar_cross_join_elim_sound,
//    trap_singleton_cross_join_sum_not_idempotent)
bool EliminateSingletonScalarCrossJoins(SelectStatement* outer,
                                        TransactionContext& ctx) {
  std::unordered_set<SelectStatement*> visited;
  return EliminateSingletonScalarCrossJoinsImpl(outer, {}, {}, true, ctx,
                                                &visited);
}

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (distinct_union_all_dim_distribution_sound)
bool DistributeDistinctUnionDimJoins(SelectStatement* outer,
                                     TransactionContext& ctx) {
  if (outer == nullptr) {
    return false;
  }
  std::unordered_set<std::string> cte_names;
  for (const auto& [name, body] : outer->WithQueries()) {
    (void)body;
    cte_names.insert(LoweredStarName(name));
  }
  bool changed = false;
  for (const auto& [name, body] : outer->WithQueries()) {
    if (!outer->IsRecursiveWith(name) && body != nullptr) {
      changed |=
          DistributeDistinctUnionDimJoinsOneSelect(body.get(), cte_names, ctx);
    }
  }
  changed |= DistributeDistinctUnionDimJoinsOneSelect(outer, cte_names, ctx);
  return changed;
}

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (adjacent_rank_self_join_to_lag_lead_sound)
bool RewriteAdjacentRankSelfJoinToLagLead(SelectStatement* outer) {
  if (outer == nullptr || outer->WithQueries().size() < 2 ||
      TpcdsStmtHasParam(*outer)) {
    return false;
  }
  bool any_changed = false;
  const std::vector<std::string> order = outer->WithQueryOrder();
  for (const std::string& v2_name : order) {
    if (outer->IsRecursiveWith(v2_name)) {
      continue;
    }
    const auto v2_it = outer->WithQueries().find(v2_name);
    if (v2_it == outer->WithQueries().end() || v2_it->second == nullptr) {
      continue;
    }
    SelectStatement& v2 = *v2_it->second;
    if (v2.Sources().size() != 3 || !v2.GroupBy().empty() || v2.IsGroupAll() ||
        v2.Having() || v2.Distinct() || v2.HasDistinctOn() || v2.HasLimit() ||
        v2.Offset() != 0 || v2.Qualify() || !v2.OrderBy().empty() ||
        !v2.WithQueries().empty() || !v2.UnionAll().empty() ||
        v2.GetSetOperationTree() != nullptr ||
        relational_detail::HasWindowFunctions(v2) ||
        SelectListHasStarOrModifiers(v2) || !v2.WhereClause()) {
      continue;
    }
    const std::string v1_name = v2.Sources()[0].table;
    if (v1_name.empty() || outer->IsRecursiveWith(v1_name) ||
        IdentifierEquals(v1_name, v2_name)) {
      continue;
    }
    const auto v1_it = outer->WithQueries().find(v1_name);
    if (v1_it == outer->WithQueries().end() || v1_it->second == nullptr) {
      continue;
    }
    // `v1` must be referenced ONLY inside `v2` (3 times).
    std::unordered_map<std::string, size_t> total_v1_refs;
    CountCteReferencesInStatement(*outer, {v1_name}, &total_v1_refs);
    if (total_v1_refs[v1_name] != 3) {
      continue;
    }
    std::array<std::string, 3> aliases;
    bool srcs_ok = true;
    for (size_t i = 0; i < 3; ++i) {
      const SelectSource& s = v2.Sources()[i];
      if (!IdentifierEquals(s.table, v1_name) || s.query != nullptr ||
          s.unnest || s.is_lateral || s.sample || !s.using_columns.empty() ||
          s.from_nested_join ||
          (s.join_type != JoinType::kCross &&
           s.join_type != JoinType::kInner)) {
        srcs_ok = false;
        break;
      }
      aliases[i] = s.alias.empty() ? s.table : s.alias;
      for (size_t j = 0; j < i; ++j) {
        if (IdentifierEquals(aliases[i], aliases[j])) {
          srcs_ok = false;
        }
      }
    }
    if (!srcs_ok) {
      continue;
    }

    SelectStatement& v1 = *v1_it->second;
    if (v1.GroupBy().empty() || v1.IsGroupAll() || v1.Distinct() ||
        v1.HasDistinctOn() || v1.HasLimit() || v1.Offset() != 0 ||
        v1.Qualify() || !v1.OrderBy().empty() || !v1.WithQueries().empty() ||
        !v1.UnionAll().empty() || v1.GetSetOperationTree() != nullptr ||
        SelectListHasStarOrModifiers(v1) || TpcdsStmtHasParam(v1)) {
      continue;
    }

    // Locate the `RANK()` / `ROW_NUMBER()` / `DENSE_RANK()` column in `v1`.
    std::unordered_map<std::string, size_t> v1_out_map;
    std::optional<size_t> rn_idx;
    const WindowFunctionCallExpression* rn_win = nullptr;
    for (size_t i = 0; i < v1.SelectList().size(); ++i) {
      const std::string nm = InferSelectItemOutputName(v1.SelectList()[i], i);
      if (nm.empty() || !v1_out_map.emplace(LoweredStarName(nm), i).second) {
        rn_idx.reset();
        v1_out_map.clear();
        break;
      }
      const Expression& e = v1.SelectList()[i].expression;
      if (e && e->Type() == TypeTag::kWindowFunctionExp) {
        const auto& wf = e->AsWindowFunctionCallExpression();
        if ((IdentifierEquals(wf.function, "rank") ||
             IdentifierEquals(wf.function, "row_number") ||
             IdentifierEquals(wf.function, "dense_rank")) &&
            wf.args.empty() && !wf.where_filter && !wf.distinct &&
            wf.inner_order_by.empty() && !wf.inner_limit.has_value() &&
            !wf.has_frame && !wf.partition_by.empty() && !wf.order_by.empty()) {
          rn_idx = i;
          rn_win = &wf;
        }
      }
    }
    if (!rn_idx.has_value() || rn_win == nullptr || v1_out_map.empty()) {
      continue;
    }
    const std::string rn_out_name =
        InferSelectItemOutputName(v1.SelectList()[*rn_idx], *rn_idx);

    // Verify `rn_win->partition_by` U `rn_win->order_by` covers ALL of
    // `v1.GroupBy()` (guaranteeing 0 ties per partition!).
    std::unordered_set<std::string> po_exprs;
    std::vector<std::string> part_out_names;
    bool po_ok = true;
    for (const Expression& pe : rn_win->partition_by) {
      if (!pe || pe->Type() != TypeTag::kColumnValue) {
        po_ok = false;
        break;
      }
      po_exprs.insert(pe->ToString());
      std::string found_out;
      for (size_t i = 0; i < v1.SelectList().size(); ++i) {
        if (v1.SelectList()[i].expression &&
            v1.SelectList()[i].expression->ToString() == pe->ToString()) {
          found_out = InferSelectItemOutputName(v1.SelectList()[i], i);
          break;
        }
      }
      if (found_out.empty()) {
        po_ok = false;
        break;
      }
      part_out_names.push_back(std::move(found_out));
    }
    for (const WindowOrderTerm& ot : rn_win->order_by) {
      if (!ot.expression || ot.expression->Type() != TypeTag::kColumnValue) {
        po_ok = false;
        break;
      }
      po_exprs.insert(ot.expression->ToString());
    }
    if (!po_ok) {
      continue;
    }
    for (const Expression& gk : v1.GroupBy()) {
      if (!gk || !po_exprs.contains(gk->ToString())) {
        po_ok = false;
        break;
      }
    }
    if (!po_ok) {
      continue;
    }

    // The three sources are matched in the fixed order
    // (center, lag, lead) = (0, 1, 2), i.e. (v1, v1_lag, v1_lead).
    // Other orderings of the same self-join shape are not considered here.
    std::vector<Expression> v2_conjs = SplitConjuncts(v2.WhereClause());
    for (size_t si = 1; si < 3; ++si) {
      if (v2.Sources()[si].join_condition) {
        for (Expression& jc : SplitConjuncts(v2.Sources()[si].join_condition)) {
          v2_conjs.push_back(std::move(jc));
        }
      }
    }

    auto match_col_of = [&](const Expression& e, size_t src_i,
                            std::string* col_out) -> bool {
      if (!e || e->Type() != TypeTag::kColumnValue) {
        return false;
      }
      const ColumnName& cn = e->AsColumnValue().GetColumnName();
      if (!IdentifierEquals(cn.schema, aliases[src_i])) {
        return false;
      }
      *col_out = cn.name;
      return true;
    };

    // Check `(center = 0, lag = 1, lead = 2)`:
    const size_t c_i = 0;
    const size_t lg_i = 1;
    const size_t ld_i = 2;
    std::unordered_set<std::string> eq_lg_cols;
    eq_lg_cols.reserve(v2_conjs.size());
    std::unordered_set<std::string> eq_ld_cols;
    bool has_lag_rn = false;
    bool has_lead_rn = false;
    bool conjs_ok = true;
    for (const Expression& cj : v2_conjs) {
      if (!cj || cj->Type() != TypeTag::kBinaryExp ||
          cj->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
        conjs_ok = false;
        break;
      }
      const Expression& l = cj->AsBinaryExpression().Left();
      const Expression& r = cj->AsBinaryExpression().Right();
      std::string lc;
      std::string rc;
      if (match_col_of(l, c_i, &lc) && match_col_of(r, lg_i, &rc) &&
          IdentifierEquals(lc, rc)) {
        eq_lg_cols.insert(LoweredStarName(lc));
        continue;
      }
      if (match_col_of(r, c_i, &lc) && match_col_of(l, lg_i, &rc) &&
          IdentifierEquals(lc, rc)) {
        eq_lg_cols.insert(LoweredStarName(lc));
        continue;
      }
      if (match_col_of(l, c_i, &lc) && match_col_of(r, ld_i, &rc) &&
          IdentifierEquals(lc, rc)) {
        eq_ld_cols.insert(LoweredStarName(lc));
        continue;
      }
      if (match_col_of(r, c_i, &lc) && match_col_of(l, ld_i, &rc) &&
          IdentifierEquals(lc, rc)) {
        eq_ld_cols.insert(LoweredStarName(lc));
        continue;
      }
      // Match `v1.rn = v1_lag.rn + 1` or `v1.rn = v1_lead.rn - 1`.
      auto check_rn_offset = [&](const Expression& lhs_c,
                                 const Expression& rhs_bin) -> bool {
        std::string c_rn;
        if (!match_col_of(lhs_c, c_i, &c_rn) ||
            !IdentifierEquals(c_rn, rn_out_name) || !rhs_bin ||
            rhs_bin->Type() != TypeTag::kBinaryExp) {
          return false;
        }
        const auto& b = rhs_bin->AsBinaryExpression();
        if ((b.Op() != BinaryOperation::kAdd &&
             b.Op() != BinaryOperation::kSubtract) ||
            !b.Right() || b.Right()->Type() != TypeTag::kConstantValue) {
          return false;
        }
        const Value& kv = b.Right()->AsConstantValue().GetValue();
        if (kv.IsNull() || kv.type != ValueType::kInt64 ||
            kv.value.int_value != 1) {
          return false;
        }
        std::string o_rn;
        if (b.Op() == BinaryOperation::kAdd &&
            match_col_of(b.Left(), lg_i, &o_rn) &&
            IdentifierEquals(o_rn, rn_out_name)) {
          has_lag_rn = true;
          return true;
        }
        if (b.Op() == BinaryOperation::kSubtract &&
            match_col_of(b.Left(), ld_i, &o_rn) &&
            IdentifierEquals(o_rn, rn_out_name)) {
          has_lead_rn = true;
          return true;
        }
        return false;
      };
      if (check_rn_offset(l, r) || check_rn_offset(r, l)) {
        continue;
      }
      conjs_ok = false;
      break;
    }
    if (!conjs_ok || !has_lag_rn || !has_lead_rn ||
        eq_lg_cols.size() != part_out_names.size() ||
        eq_ld_cols.size() != part_out_names.size()) {
      continue;
    }
    bool exact_part_match = true;
    for (const std::string& pnm : part_out_names) {
      const std::string plow = LoweredStarName(pnm);
      if (!eq_lg_cols.contains(plow) || !eq_ld_cols.contains(plow)) {
        exact_part_match = false;
        break;
      }
    }
    if (!exact_part_match) {
      continue;
    }

    // Inspect `v2.SelectList()`: center refs allowed, plus lag/lead refs to
    // pure aggregate/column items of `v1`.
    std::unordered_map<std::string, std::string> lag_col_to_win;
    std::unordered_map<std::string, std::string> lead_col_to_win;
    bool rn_used_in_v2 = false;
    bool v2_sel_ok = true;
    size_t synth_seq = 0;
    std::vector<NamedExpression> extra_v1_wins;

    auto make_offset_win = [&](std::string fn_name,
                               Expression arg) -> Expression {
      auto w = std::make_shared<WindowFunctionCallExpression>();
      w->function = std::move(fn_name);
      w->args = {std::move(arg), ConstantValueExp(Value(int64_t{1}))};
      w->partition_by = rn_win->partition_by;
      w->order_by = rn_win->order_by;
      return w;
    };

    auto ensure_neighbor_win = [&](const std::string& v1_col, bool is_lag,
                                   std::string* out_win_name) -> bool {
      auto& map = is_lag ? lag_col_to_win : lead_col_to_win;
      const std::string clow = LoweredStarName(v1_col);
      if (const auto it = map.find(clow); it != map.end()) {
        *out_win_name = it->second;
        return true;
      }
      const auto vit = v1_out_map.find(clow);
      if (vit == v1_out_map.end()) {
        return false;
      }
      const Expression& v1_expr = v1.SelectList()[vit->second].expression;
      auto expr_is_pure_group_output = [&](auto& self,
                                           const Expression& node) -> bool {
        if (!node) {
          return true;
        }
        if (node->Type() == TypeTag::kQueryExp ||
            node->Type() == TypeTag::kWindowFunctionExp ||
            TpcdsExprHasParam(node)) {
          return false;
        }
        if (node->Type() == TypeTag::kFunctionCallExp &&
            GetFunctionVolatility(
                node->AsFunctionCallExpression().FuncName()) !=
                Volatility::kImmutable) {
          return false;
        }
        if (node->Type() == TypeTag::kAggregateExp) {
          const auto& agg = node->AsAggregateExpression();
          if (agg.NeedsGroupContext() || !agg.InnerGroupBy().empty() ||
              agg.WhereFilter() ||
              agg.Having() != AggregateHavingModifier::kNone ||
              agg.HavingCondition() || !agg.InnerOrderBy().empty() ||
              agg.SecondaryArg() || !agg.TrailingArgs().empty()) {
            return false;
          }
        }
        for (const Expression& ch : ExpressionChildren(node)) {
          if (!self(self, ch)) {
            return false;
          }
        }
        return true;
      };
      if (!v1_expr ||
          !expr_is_pure_group_output(expr_is_pure_group_output, v1_expr)) {
        return false;
      }
      const std::string wname = (is_lag ? "__tpcds_lag_" : "__tpcds_lead_") +
                                std::to_string(synth_seq++);
      extra_v1_wins.emplace_back(
          wname, make_offset_win(is_lag ? "LAG" : "LEAD", v1_expr));
      map.emplace(clow, wname);
      *out_win_name = wname;
      return true;
    };

    std::vector<NamedExpression> new_v2_sel;
    new_v2_sel.reserve(v2.SelectList().size());
    for (size_t i = 0; i < v2.SelectList().size(); ++i) {
      std::string out_nm = InferSelectItemOutputName(v2.SelectList()[i], i);
      Expression rewritten = ReplaceSubExpressions(
          v2.SelectList()[i].expression,
          [&](const Expression& node) -> std::optional<Expression> {
            if (!node) {
              return std::nullopt;
            }
            if (node->Type() == TypeTag::kQueryExp) {
              v2_sel_ok = false;
              return std::nullopt;
            }
            if (node->Type() != TypeTag::kColumnValue) {
              return std::nullopt;
            }
            const ColumnName& cn = node->AsColumnValue().GetColumnName();
            if (cn.schema.empty()) {
              v2_sel_ok = false;
              return std::nullopt;
            }
            if (IdentifierEquals(cn.schema, aliases[c_i])) {
              if (IdentifierEquals(cn.name, rn_out_name)) {
                rn_used_in_v2 = true;
              }
              return std::nullopt;
            }
            if (IdentifierEquals(cn.schema, aliases[lg_i])) {
              std::string wn;
              if (!ensure_neighbor_win(cn.name, /*is_lag=*/true, &wn)) {
                v2_sel_ok = false;
                return std::nullopt;
              }
              return ColumnValueExp(ColumnName(aliases[c_i], wn));
            }
            if (IdentifierEquals(cn.schema, aliases[ld_i])) {
              std::string wn;
              if (!ensure_neighbor_win(cn.name, /*is_lag=*/false, &wn)) {
                v2_sel_ok = false;
                return std::nullopt;
              }
              return ColumnValueExp(ColumnName(aliases[c_i], wn));
            }
            v2_sel_ok = false;
            return std::nullopt;
          });
      if (!v2_sel_ok) {
        break;
      }
      new_v2_sel.emplace_back(std::move(out_nm), std::move(rewritten));
    }
    if (!v2_sel_ok) {
      continue;
    }

    const std::string has_lag_col = "__tpcds_has_lag";
    const std::string has_lead_col = "__tpcds_has_lead";
    extra_v1_wins.emplace_back(
        has_lag_col,
        make_offset_win("LAG", ConstantValueExp(Value(int64_t{1}))));
    extra_v1_wins.emplace_back(
        has_lead_col,
        make_offset_win("LEAD", ConstantValueExp(Value(int64_t{1}))));

    std::vector<NamedExpression> new_v1_sel;
    new_v1_sel.reserve(v1.SelectList().size() + extra_v1_wins.size());
    for (size_t i = 0; i < v1.SelectList().size(); ++i) {
      if (i == *rn_idx && !rn_used_in_v2) {
        continue;
      }
      new_v1_sel.push_back(v1.SelectList()[i]);
    }
    for (NamedExpression& ew : extra_v1_wins) {
      new_v1_sel.push_back(std::move(ew));
    }
    v1.SetSelectList(std::move(new_v1_sel));

    SelectSource center_src = v2.Sources()[c_i];
    center_src.alias = aliases[c_i];
    center_src.join_type = JoinType::kCross;
    center_src.join_condition = nullptr;
    std::vector<Expression> new_v2_where;
    new_v2_where.reserve(part_out_names.size() + 2);
    for (const std::string& pnm : part_out_names) {
      new_v2_where.push_back(
          UnaryExpressionExp(ColumnValueExp(ColumnName(aliases[c_i], pnm)),
                             UnaryOperation::kIsNotNull));
    }
    new_v2_where.push_back(UnaryExpressionExp(
        ColumnValueExp(ColumnName(aliases[c_i], has_lag_col)),
        UnaryOperation::kIsNotNull));
    new_v2_where.push_back(UnaryExpressionExp(
        ColumnValueExp(ColumnName(aliases[c_i], has_lead_col)),
        UnaryOperation::kIsNotNull));

    v2.SetSources({std::move(center_src)});
    v2.SetWhereClause(CombineConjuncts(new_v2_where));
    v2.SetSelectList(std::move(new_v2_sel));
    any_changed = true;
  }
  return any_changed;
}

namespace select_rewrite_detail {

namespace {
bool FoldSingletonRankInStatement(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt, std::unordered_set<SelectStatement*>* visited) {
  if (stmt == nullptr || !visited->insert(stmt).second) {
    return false;
  }
  bool changed = false;
  for (const auto& [name, body] : stmt->WithQueries()) {
    if (!stmt->IsRecursiveWith(name) && body != nullptr) {
      changed |= FoldSingletonRankInStatement(body.get(), visited);
    }
  }
  for (const SelectSource& src : stmt->Sources()) {
    if (src.query != nullptr) {
      changed |= FoldSingletonRankInStatement(src.query.get(), visited);
    }
  }
  for (const auto& br : stmt->UnionAll()) {
    if (br != nullptr) {
      changed |= FoldSingletonRankInStatement(br.get(), visited);
    }
  }
  if (const auto& tree = stmt->GetSetOperationTree(); tree != nullptr) {
    if (tree->first != nullptr) {
      changed |= FoldSingletonRankInStatement(tree->first.get(), visited);
    }
    for (const auto& br : tree->branches) {
      if (br != nullptr) {
        changed |= FoldSingletonRankInStatement(br.get(), visited);
      }
    }
  }

  auto fold_subqueries_in_expr = [&](auto& self, const Expression& e) -> void {
    if (!e) {
      return;
    }
    if (e->Type() == TypeTag::kQueryExp) {
      const auto& q = e->AsQueryExpression();
      if (q.Query() != nullptr) {
        changed |= FoldSingletonRankInStatement(q.Query().get(), visited);
      }
      self(self, q.Test());
      return;
    }
    for (const Expression& ch : ExpressionChildren(e)) {
      self(self, ch);
    }
  };
  fold_subqueries_in_expr(fold_subqueries_in_expr, stmt->WhereClause());
  fold_subqueries_in_expr(fold_subqueries_in_expr, stmt->Having());
  fold_subqueries_in_expr(fold_subqueries_in_expr, stmt->Qualify());

  // Check if `stmt` is a plain single-grouping-set GROUP BY query whose
  // `GroupBy()` keys are all covered by a
  // `RANK()`/`DENSE_RANK()`/`ROW_NUMBER()` window's `partition_by`.
  if (!stmt->GroupBy().empty() && !stmt->IsGroupAll() &&
      stmt->UnionAll().empty() && stmt->GetSetOperationTree() == nullptr) {
    auto norm_col = [](const Expression& e) -> std::string {
      if (!e || e->Type() != TypeTag::kColumnValue) {
        return {};
      }
      const ColumnName& cn = e->AsColumnValue().GetColumnName();
      return LoweredStarName(cn.name);
    };
    std::vector<std::string> gb_names;
    bool gb_simple = true;
    for (const Expression& g : stmt->GroupBy()) {
      std::string nm = norm_col(g);
      if (nm.empty()) {
        gb_simple = false;
        break;
      }
      gb_names.push_back(std::move(nm));
    }
    if (gb_simple) {
      bool any_folded_here = false;
      std::vector<NamedExpression> new_sel;
      new_sel.reserve(stmt->SelectList().size());
      for (size_t i = 0; i < stmt->SelectList().size(); ++i) {
        std::string out_nm =
            InferSelectItemOutputName(stmt->SelectList()[i], i);
        Expression replaced = ReplaceSubExpressions(
            stmt->SelectList()[i].expression,
            [&](const Expression& node) -> std::optional<Expression> {
              if (!node || node->Type() != TypeTag::kWindowFunctionExp) {
                return std::nullopt;
              }
              const auto& wf = node->AsWindowFunctionCallExpression();
              if ((!IdentifierEquals(wf.function, "rank") &&
                   !IdentifierEquals(wf.function, "dense_rank") &&
                   !IdentifierEquals(wf.function, "row_number")) ||
                  !wf.args.empty() || wf.where_filter || wf.distinct ||
                  !wf.inner_order_by.empty() || wf.inner_limit.has_value() ||
                  wf.has_frame || wf.partition_by.empty()) {
                return std::nullopt;
              }
              std::unordered_set<std::string> pnames;
              for (const Expression& pe : wf.partition_by) {
                std::string pn = norm_col(pe);
                if (!pn.empty()) {
                  pnames.insert(std::move(pn));
                }
              }
              for (const std::string& gn : gb_names) {
                if (!pnames.contains(gn)) {
                  return std::nullopt;
                }
              }
              any_folded_here = true;
              return ConstantValueExp(Value(int64_t{1}));
            });
        new_sel.emplace_back(std::move(out_nm), std::move(replaced));
      }
      if (any_folded_here) {
        stmt->SetSelectList(std::move(new_sel));
        changed = true;
      }
    }
  }

  // Also absorb `col IN (SELECT col FROM (SELECT col, 1 AS ranking FROM <same>
  // GROUP BY col) WHERE ranking <= K)` (where 1 <= K is TRUE and <same> matches
  // `stmt`'s own base tables + non-subquery predicates) into `col IS NOT NULL`.
  if (stmt->WhereClause() && !stmt->Sources().empty()) {
    auto canonical_base_sig = [](const SelectStatement& s,
                                 const Expression* skip_conj,
                                 std::string* sig_out) -> bool {
      std::unordered_map<std::string, std::string> alias_to_tbl;
      std::vector<std::string> tbl_multiset;
      tbl_multiset.reserve(s.Sources().size());
      for (const SelectSource& src : s.Sources()) {
        if (src.table.empty() || src.query != nullptr || src.unnest ||
            src.is_lateral || src.sample || !src.using_columns.empty() ||
            src.from_nested_join || src.join_condition ||
            (src.join_type != JoinType::kCross &&
             src.join_type != JoinType::kInner)) {
          return false;
        }
        const std::string tlow = LoweredStarName(src.table);
        tbl_multiset.push_back(tlow);
        alias_to_tbl.emplace(
            LoweredStarName(src.alias.empty() ? src.table : src.alias), tlow);
      }
      std::ranges::sort(tbl_multiset);
      // Reject self-joins of the same base table so alias stripping is exact.
      for (size_t i = 1; i < tbl_multiset.size(); ++i) {
        if (tbl_multiset[i] == tbl_multiset[i - 1]) {
          return false;
        }
      }
      std::vector<std::string> norm_preds;
      for (const Expression& c : SplitConjuncts(s.WhereClause())) {
        if (skip_conj != nullptr && c.get() == skip_conj->get()) {
          continue;
        }
        if (!c || ContainsQueryExpression(c) ||
            !DerivedExpressionIsImmutable(c)) {
          return false;
        }
        Expression stripped = ReplaceSubExpressions(
            c, [&](const Expression& node) -> std::optional<Expression> {
              if (!node || node->Type() != TypeTag::kColumnValue) {
                return std::nullopt;
              }
              const ColumnName& cn = node->AsColumnValue().GetColumnName();
              return ColumnValueExp(ColumnName("", LoweredStarName(cn.name)));
            });
        // Canonicalize commutative `a = b` order.
        if (stripped->Type() == TypeTag::kBinaryExp &&
            stripped->AsBinaryExpression().Op() == BinaryOperation::kEquals) {
          std::string ls = stripped->AsBinaryExpression().Left()->ToString();
          std::string rs = stripped->AsBinaryExpression().Right()->ToString();
          if (rs < ls) {
            stripped =
                BinaryExpressionExp(stripped->AsBinaryExpression().Right(),
                                    BinaryOperation::kEquals,
                                    stripped->AsBinaryExpression().Left());
          }
        }
        norm_preds.push_back(stripped->ToString());
      }
      std::ranges::sort(norm_preds);
      std::string sig;
      for (const std::string& t : tbl_multiset) {
        sig += t + ",";
      }
      sig += "|";
      for (const std::string& p : norm_preds) {
        sig += p + "&";
      }
      *sig_out = std::move(sig);
      return true;
    };

    std::vector<Expression> conjs = SplitConjuncts(stmt->WhereClause());
    bool where_changed = false;
    for (Expression& cj : conjs) {
      if (!cj || cj->Type() != TypeTag::kQueryExp) {
        continue;
      }
      const auto& qexp = cj->AsQueryExpression();
      if (qexp.Exists() || qexp.Negated() || qexp.ArrayResult() ||
          !qexp.Test() || qexp.Test()->Type() != TypeTag::kColumnValue ||
          qexp.Query() == nullptr) {
        continue;
      }
      const std::string outer_col_low =
          LoweredStarName(qexp.Test()->AsColumnValue().GetColumnName().name);
      const SelectStatement* cur_sq = qexp.Query().get();
      // Unwrap `SELECT s_state FROM (<inner>) tmp1 WHERE ranking <= 5` when
      // `ranking` in `<inner>` is ConstantValue(1) and `1 <= 5` folds to true!
      if (cur_sq->Sources().size() == 1 &&
          cur_sq->Sources()[0].query != nullptr && cur_sq->GroupBy().empty() &&
          !cur_sq->Having() && !cur_sq->HasLimit() && cur_sq->Offset() == 0 &&
          cur_sq->UnionAll().empty() &&
          cur_sq->GetSetOperationTree() == nullptr &&
          cur_sq->SelectList().size() == 1 &&
          cur_sq->SelectList()[0].expression &&
          cur_sq->SelectList()[0].expression->Type() == TypeTag::kColumnValue) {
        const SelectStatement& inner_sq = *cur_sq->Sources()[0].query;
        const std::string proj_nm = cur_sq->SelectList()[0]
                                        .expression->AsColumnValue()
                                        .GetColumnName()
                                        .name;
        std::unordered_map<std::string, Expression> const_cols;
        std::string inner_key_col;
        for (size_t i = 0; i < inner_sq.SelectList().size(); ++i) {
          const std::string inm =
              InferSelectItemOutputName(inner_sq.SelectList()[i], i);
          const Expression& ie = inner_sq.SelectList()[i].expression;
          if (IdentifierEquals(inm, proj_nm) && ie &&
              ie->Type() == TypeTag::kColumnValue) {
            inner_key_col =
                LoweredStarName(ie->AsColumnValue().GetColumnName().name);
          } else if (ie && ie->Type() == TypeTag::kConstantValue) {
            const_cols.emplace(LoweredStarName(inm), ie);
          }
        }
        bool wrapper_true = !cur_sq->WhereClause();
        if (cur_sq->WhereClause() && !const_cols.empty()) {
          Expression subst_w = ReplaceSubExpressions(
              cur_sq->WhereClause(),
              [&](const Expression& node) -> std::optional<Expression> {
                if (!node || node->Type() != TypeTag::kColumnValue) {
                  return std::nullopt;
                }
                const auto it = const_cols.find(LoweredStarName(
                    node->AsColumnValue().GetColumnName().name));
                return it != const_cols.end()
                           ? std::optional<Expression>(it->second)
                           : std::nullopt;
              });
          if (std::optional<Value> wv = TpcdsEvalClosedConstant(subst_w);
              wv.has_value() && !wv->IsNull() &&
              wv->type == ValueType::kInt64 && wv->value.int_value != 0) {
            wrapper_true = true;
          }
        }
        if (wrapper_true && inner_key_col == outer_col_low &&
            inner_sq.GroupBy().size() == 1 && !inner_sq.Having() &&
            !inner_sq.HasLimit() && inner_sq.Offset() == 0 &&
            inner_sq.UnionAll().empty() &&
            inner_sq.GetSetOperationTree() == nullptr &&
            inner_sq.GroupBy()[0] &&
            inner_sq.GroupBy()[0]->Type() == TypeTag::kColumnValue &&
            LoweredStarName(
                inner_sq.GroupBy()[0]->AsColumnValue().GetColumnName().name) ==
                outer_col_low) {
          std::string outer_sig;
          std::string inner_sig;
          if (canonical_base_sig(*stmt, &cj, &outer_sig) &&
              canonical_base_sig(inner_sq, nullptr, &inner_sig) &&
              outer_sig == inner_sig) {
            cj = UnaryExpressionExp(qexp.Test(), UnaryOperation::kIsNotNull);
            where_changed = true;
          }
        }
      }
    }
    if (where_changed) {
      stmt->SetWhereClause(CombineConjuncts(conjs));
      changed = true;
    }
  }

  return changed;
}
}  // namespace

}  // namespace select_rewrite_detail

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (singleton_partition_rank_const_one_sound)
bool FoldSingletonPartitionRankToOne(SelectStatement* outer) {
  std::unordered_set<SelectStatement*> visited;
  return FoldSingletonRankInStatement(outer, &visited);
}

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (multi_join_shared_unique_dim_deferral_sound,
//    trap_non_unique_uk_dim_collapse_unsound)
bool CollapseSharedUniqueDimAcrossJoinedSubqueries(SelectStatement* outer,
                                                   TransactionContext& ctx) {
  if (outer == nullptr || !outer->UnionAll().empty() ||
      outer->GetSetOperationTree() != nullptr || outer->ForUpdate() ||
      outer->AsStruct() || SelectListHasStarOrModifiers(*outer) ||
      TpcdsStmtHasParam(*outer) || outer->Sources().size() < 2) {
    return false;
  }
  for (const SelectSource& s : outer->Sources()) {
    if ((s.join_type != JoinType::kCross && s.join_type != JoinType::kInner) ||
        s.is_lateral || s.unnest || s.sample || s.from_nested_join ||
        !s.using_columns.empty()) {
      return false;
    }
  }

  struct DeferredDimSite {
    size_t src_idx{0};
    std::string outer_alias;
    std::string pre_alias;
    std::string fk_pre_col;
    std::string dim_table;
    std::string dim_alias;
    std::string pk_col;
    std::shared_ptr<Table> dim_tbl;
    // Lowered outer output col name -> actual dim schema col name.
    std::unordered_map<std::string, std::string> out_to_dim_col;
  };

  std::vector<DeferredDimSite> sites;
  sites.reserve(outer->Sources().size());
  for (size_t si = 0; si < outer->Sources().size(); ++si) {
    const SelectSource& src = outer->Sources()[si];
    if (src.query == nullptr || src.alias.empty()) {
      continue;
    }
    const SelectStatement& dq = *src.query;
    if (!dq.WithQueries().empty() || !dq.UnionAll().empty() ||
        dq.GetSetOperationTree() != nullptr || !dq.GroupBy().empty() ||
        dq.IsGroupAll() || dq.Having() || dq.Distinct() || dq.HasDistinctOn() ||
        dq.Qualify() || dq.HasLimit() || dq.Offset() != 0 ||
        !dq.OrderBy().empty() || dq.WithTies() || dq.Sources().size() != 2 ||
        SelectListHasStarOrModifiers(dq)) {
      continue;
    }
    const SelectSource& pre_src = dq.Sources()[0];
    const SelectSource& dim_src = dq.Sources()[1];
    if (pre_src.query == nullptr || pre_src.alias.empty() ||
        pre_src.is_lateral || pre_src.unnest || pre_src.sample ||
        pre_src.join_condition || dim_src.table.empty() ||
        dim_src.query != nullptr || dim_src.is_lateral || dim_src.unnest ||
        dim_src.sample || dim_src.join_condition ||
        (dim_src.join_type != JoinType::kCross &&
         dim_src.join_type != JoinType::kInner) ||
        outer->WithQueries().contains(dim_src.table)) {
      continue;
    }
    StatusOr<std::shared_ptr<Table>> dim_tbl_or = ctx.GetTable(dim_src.table);
    if (!dim_tbl_or.HasValue() || dim_tbl_or.Value() == nullptr) {
      continue;
    }
    std::shared_ptr<Table> dim_tbl = dim_tbl_or.MoveValue();
    const Schema& dim_schema = dim_tbl->GetSchema();
    const std::string dim_alias =
        dim_src.alias.empty() ? dim_src.table : dim_src.alias;
    const std::vector<Expression> dq_conjs = SplitConjuncts(dq.WhereClause());
    if (dq_conjs.size() != 1 || !dq_conjs[0] ||
        dq_conjs[0]->Type() != TypeTag::kBinaryExp ||
        dq_conjs[0]->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
      continue;
    }
    const BinaryExpression& eq = dq_conjs[0]->AsBinaryExpression();
    if (!eq.Left() || !eq.Right() ||
        eq.Left()->Type() != TypeTag::kColumnValue ||
        eq.Right()->Type() != TypeTag::kColumnValue) {
      continue;
    }
    const ColumnName& lc = eq.Left()->AsColumnValue().GetColumnName();
    const ColumnName& rc = eq.Right()->AsColumnValue().GetColumnName();
    std::string fk_pre_col;
    std::string pk_col;
    if (IdentifierEquals(lc.schema, pre_src.alias) &&
        IdentifierEquals(rc.schema, dim_alias)) {
      fk_pre_col = lc.name;
      pk_col = rc.name;
    } else if (IdentifierEquals(rc.schema, pre_src.alias) &&
               IdentifierEquals(lc.schema, dim_alias)) {
      fk_pre_col = rc.name;
      pk_col = lc.name;
    } else {
      continue;
    }
    const int pk_off = dim_schema.Offset(ColumnName("", pk_col));
    if (pk_off < 0) {
      continue;
    }
    const std::string actual_pk =
        dim_schema.GetColumn(static_cast<size_t>(pk_off)).Name().name;
    const GroupingRelation dim_grel{.name = dim_alias, .schema = &dim_schema};
    if (!ColumnIsPrimaryKey(dim_grel, actual_pk) &&
        !InspectRuntimeUniqueNonNullColumn(ctx, dim_src.table, actual_pk)
             .unique_non_null) {
      continue;
    }
    // Verify `fk_pre_col` is projected by `pre_src.query`.
    bool fk_found = false;
    for (size_t i = 0; i < pre_src.query->SelectList().size(); ++i) {
      if (IdentifierEquals(
              InferSelectItemOutputName(pre_src.query->SelectList()[i], i),
              fk_pre_col)) {
        fk_found = true;
        break;
      }
    }
    if (!fk_found) {
      continue;
    }
    std::unordered_map<std::string, std::string> out_to_dim;
    bool items_ok = true;
    std::unordered_set<std::string> seen_out;
    for (size_t i = 0; i < dq.SelectList().size(); ++i) {
      const NamedExpression& it = dq.SelectList()[i];
      const std::string out_nm = InferSelectItemOutputName(it, i);
      if (out_nm.empty() || !seen_out.insert(LoweredStarName(out_nm)).second ||
          !it.expression || it.expression->Type() != TypeTag::kColumnValue) {
        items_ok = false;
        break;
      }
      const ColumnName& cn = it.expression->AsColumnValue().GetColumnName();
      if (IdentifierEquals(cn.schema, dim_alias)) {
        const int doff = dim_schema.Offset(ColumnName("", cn.name));
        if (doff < 0) {
          items_ok = false;
          break;
        }
        out_to_dim.emplace(
            LoweredStarName(out_nm),
            dim_schema.GetColumn(static_cast<size_t>(doff)).Name().name);
      } else if (!IdentifierEquals(cn.schema, pre_src.alias)) {
        items_ok = false;
        break;
      }
    }
    if (!items_ok || out_to_dim.empty()) {
      continue;
    }
    sites.push_back(DeferredDimSite{.src_idx = si,
                                    .outer_alias = src.alias,
                                    .pre_alias = pre_src.alias,
                                    .fk_pre_col = fk_pre_col,
                                    .dim_table = dim_src.table,
                                    .dim_alias = dim_alias,
                                    .pk_col = actual_pk,
                                    .dim_tbl = std::move(dim_tbl),
                                    .out_to_dim_col = std::move(out_to_dim)});
  }
  if (sites.size() < 2) {
    return false;
  }

  // Build transitive column-equality graph over outer WHERE + INNER ON.
  auto col_token = [](std::string_view schema, std::string_view col) {
    return LoweredStarName(schema) + "." + LoweredStarName(col);
  };
  std::vector<Expression> outer_conjs = SplitConjuncts(outer->WhereClause());
  for (const SelectSource& s : outer->Sources()) {
    if (s.join_condition) {
      for (const Expression& c : SplitConjuncts(s.join_condition)) {
        outer_conjs.push_back(c);
      }
    }
  }
  std::unordered_map<std::string, std::vector<std::string>> eq_adj;
  for (const Expression& conj : outer_conjs) {
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

  // Group sites by `(dim_table, pk_col)`.
  std::map<std::pair<std::string, std::string>, std::vector<size_t>> groups;
  for (size_t i = 0; i < sites.size(); ++i) {
    groups[{LoweredStarName(sites[i].dim_table),
            LoweredStarName(sites[i].pk_col)}]
        .push_back(i);
  }

  for (const auto& [gkey, gidxs] : groups) {
    (void)gkey;
    if (gidxs.size() < 2) {
      continue;
    }
    const DeferredDimSite& s0 = sites[gidxs[0]];
    const Schema& dim_schema = s0.dim_tbl->GetSchema();
    const GroupingRelation dim_grel{.name = s0.dim_alias,
                                    .schema = &dim_schema};

    // Find a certified unique non-null column `uk_col` of `dim_table` on which
    // ALL `gidxs` are transitively equated in `outer`.
    bool certified_uk_equated = false;
    for (size_t ci = 0; ci < dim_schema.ColumnCount(); ++ci) {
      const std::string& cand_uk = dim_schema.GetColumn(ci).Name().name;
      // Quick check: every site must project `cand_uk` first before paying
      // `InspectRuntimeUniqueNonNullColumn`.
      std::vector<std::string> site_uk_tokens;
      site_uk_tokens.reserve(gidxs.size());
      bool all_project_cand = true;
      for (const size_t gi : gidxs) {
        std::string found_out;
        for (const auto& [out_low, dim_col] : sites[gi].out_to_dim_col) {
          if (IdentifierEquals(dim_col, cand_uk)) {
            found_out = out_low;
            break;
          }
        }
        if (found_out.empty()) {
          all_project_cand = false;
          break;
        }
        site_uk_tokens.push_back(col_token(sites[gi].outer_alias, found_out));
      }
      if (!all_project_cand) {
        continue;
      }
      bool all_eq = true;
      for (size_t j = 1; j < site_uk_tokens.size(); ++j) {
        if (!are_equated(site_uk_tokens[0], site_uk_tokens[j])) {
          all_eq = false;
          break;
        }
      }
      if (!all_eq) {
        continue;
      }
      if (ColumnIsPrimaryKey(dim_grel, cand_uk) ||
          InspectRuntimeUniqueNonNullColumn(ctx, s0.dim_table, cand_uk)
              .unique_non_null) {
        certified_uk_equated = true;
        break;
      }
    }
    if (!certified_uk_equated) {
      continue;
    }

    // Mint a fresh outer alias for the single shared dimension join.
    std::unordered_set<std::string> used_outer_names;
    for (const SelectSource& s : outer->Sources()) {
      if (!s.alias.empty()) {
        used_outer_names.insert(LoweredStarName(s.alias));
      }
      if (!s.table.empty()) {
        used_outer_names.insert(LoweredStarName(s.table));
      }
    }
    std::string shared_dim_alias =
        "__tpcds_shared_dim_" + LoweredStarName(s0.dim_table);
    size_t suffix = 1;
    while (used_outer_names.contains(LoweredStarName(shared_dim_alias))) {
      shared_dim_alias = "__tpcds_shared_dim_" + LoweredStarName(s0.dim_table) +
                         "_" + std::to_string(suffix++);
    }

    // Map `(outer_site_alias.out_col) -> shared_dim_alias.dim_col`, and rewrite
    // each site's derived query to drop `dim_src` and expose `__shared_fk`.
    std::unordered_map<std::string, Expression> qualified_dim_map;
    std::unordered_set<std::string> collapsed_site_aliases;
    collapsed_site_aliases.reserve(gidxs.size());
    std::vector<SelectSource> new_outer_sources = outer->Sources();
    std::vector<std::pair<std::string, std::string>> site_fk_cols;
    site_fk_cols.reserve(gidxs.size());

    for (const size_t gi : gidxs) {
      const DeferredDimSite& site = sites[gi];
      collapsed_site_aliases.insert(LoweredStarName(site.outer_alias));
      for (const auto& [out_low, dim_col] : site.out_to_dim_col) {
        qualified_dim_map.emplace(
            col_token(site.outer_alias, out_low),
            ColumnValueExp(ColumnName(shared_dim_alias, dim_col)));
      }
      const SelectStatement& old_dq = *new_outer_sources[site.src_idx].query;
      std::vector<NamedExpression> stripped_sel;
      std::unordered_set<std::string> existing_names;
      std::string fk_out_name;
      for (size_t i = 0; i < old_dq.SelectList().size(); ++i) {
        const NamedExpression& it = old_dq.SelectList()[i];
        const std::string out_nm = InferSelectItemOutputName(it, i);
        const ColumnName& cn = it.expression->AsColumnValue().GetColumnName();
        if (IdentifierEquals(cn.schema, site.pre_alias)) {
          stripped_sel.emplace_back(out_nm, it.expression);
          existing_names.insert(LoweredStarName(out_nm));
          if (fk_out_name.empty() &&
              IdentifierEquals(cn.name, site.fk_pre_col)) {
            fk_out_name = out_nm;
          }
        }
      }
      if (fk_out_name.empty()) {
        fk_out_name = "__shared_fk";
        size_t fk_suf = 1;
        while (existing_names.contains(LoweredStarName(fk_out_name))) {
          fk_out_name = "__shared_fk_" + IntegerToName(fk_suf++);
        }
        stripped_sel.emplace_back(
            fk_out_name,
            ColumnValueExp(ColumnName(site.pre_alias, site.fk_pre_col)));
      }
      auto stripped_dq = std::make_shared<SelectStatement>();
      stripped_dq->SetSources({old_dq.Sources()[0]});
      stripped_dq->SetSelectList(std::move(stripped_sel));
      new_outer_sources[site.src_idx].query = std::move(stripped_dq);
      site_fk_cols.emplace_back(site.outer_alias, fk_out_name);
    }

    // Abort if any outer expression references an unqualified column that could
    // have come from a collapsed site's stripped dimension outputs.
    bool unqualified_conflict = false;
    auto rewrite_expr = [&](const Expression& e) -> Expression {
      if (!e) {
        return e;
      }
      return ReplaceSubExpressions(
          e, [&](const Expression& node) -> std::optional<Expression> {
            if (!node || node->Type() != TypeTag::kColumnValue) {
              return std::nullopt;
            }
            const ColumnName& cn = node->AsColumnValue().GetColumnName();
            if (cn.schema.empty()) {
              for (const size_t gi : gidxs) {
                if (sites[gi].out_to_dim_col.contains(
                        LoweredStarName(cn.name))) {
                  unqualified_conflict = true;
                  break;
                }
              }
              return std::nullopt;
            }
            const auto it =
                qualified_dim_map.find(col_token(cn.schema, cn.name));
            if (it != qualified_dim_map.end()) {
              return it->second;
            }
            return std::nullopt;
          });
    };

    std::vector<NamedExpression> new_sel;
    new_sel.reserve(outer->SelectList().size());
    for (const NamedExpression& it : outer->SelectList()) {
      new_sel.emplace_back(it.name, rewrite_expr(it.expression));
    }
    for (SelectSource& s : new_outer_sources) {
      if (s.join_condition) {
        s.join_condition = rewrite_expr(s.join_condition);
      }
    }
    Expression new_where = rewrite_expr(outer->WhereClause());
    std::vector<Expression> new_group;
    new_group.reserve(outer->GroupBy().size());
    for (const Expression& g : outer->GroupBy()) {
      new_group.push_back(rewrite_expr(g));
    }
    Expression new_having = rewrite_expr(outer->Having());
    Expression new_qualify = rewrite_expr(outer->Qualify());
    std::vector<SelectStatement::OrderByTerm> new_order = outer->OrderBy();
    for (auto& ot : new_order) {
      ot.expression = rewrite_expr(ot.expression);
    }
    if (unqualified_conflict) {
      continue;
    }

    // Drop redundant `shared_dim.col = shared_dim.col` conjuncts on certified
    // unique non-null columns, and append `__fk` equijoins + shared dim PK
    // join.
    std::vector<Expression> final_where;
    for (const Expression& c : SplitConjuncts(new_where)) {
      if (c && c->Type() == TypeTag::kBinaryExp &&
          c->AsBinaryExpression().Op() == BinaryOperation::kEquals &&
          c->AsBinaryExpression().Left() && c->AsBinaryExpression().Right() &&
          c->AsBinaryExpression().Left()->Type() == TypeTag::kColumnValue &&
          c->AsBinaryExpression().Right()->Type() == TypeTag::kColumnValue) {
        const ColumnName& l =
            c->AsBinaryExpression().Left()->AsColumnValue().GetColumnName();
        const ColumnName& r =
            c->AsBinaryExpression().Right()->AsColumnValue().GetColumnName();
        if (IdentifierEquals(l.schema, shared_dim_alias) &&
            IdentifierEquals(r.schema, shared_dim_alias) &&
            IdentifierEquals(l.name, r.name) &&
            (ColumnIsPrimaryKey(dim_grel, l.name) ||
             InspectRuntimeUniqueNonNullColumn(ctx, s0.dim_table, l.name)
                 .unique_non_null)) {
          continue;
        }
      }
      final_where.push_back(c);
    }
    const Expression rep_fk = ColumnValueExp(
        ColumnName(site_fk_cols[0].first, site_fk_cols[0].second));
    for (size_t j = 1; j < site_fk_cols.size(); ++j) {
      final_where.push_back(BinaryExpressionExp(
          rep_fk, BinaryOperation::kEquals,
          ColumnValueExp(
              ColumnName(site_fk_cols[j].first, site_fk_cols[j].second))));
    }
    final_where.push_back(BinaryExpressionExp(
        rep_fk, BinaryOperation::kEquals,
        ColumnValueExp(ColumnName(shared_dim_alias, s0.pk_col))));

    new_outer_sources.emplace_back(s0.dim_table, shared_dim_alias, nullptr,
                                   JoinType::kCross, nullptr);
    outer->SetSources(std::move(new_outer_sources));
    outer->SetSelectList(std::move(new_sel));
    outer->SetWhereClause(CombineConjuncts(final_where));
    outer->SetGroupBy(std::move(new_group));
    outer->SetHaving(std::move(new_having));
    outer->SetQualify(std::move(new_qualify));
    outer->SetOrderBy(std::move(new_order));
    return true;
  }
  return false;
}

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (left_join_disjunctive_null_rejecting_semi_filter_sound,
//    trap_left_join_non_strict_predicate_semi_unsound,
//    packed_int32_pair_bijection_sound)
bool PushDisjunctiveNullRejectingLeftJoinSemiFilter(SelectStatement* outer,
                                                    TransactionContext& ctx) {
  if (outer == nullptr || outer->WithQueries().empty() ||
      !outer->UnionAll().empty() || outer->GetSetOperationTree() != nullptr ||
      outer->ForUpdate() || outer->AsStruct() ||
      SelectListHasStarOrModifiers(*outer) || TpcdsStmtHasParam(*outer) ||
      outer->Sources().size() < 2 || !outer->WhereClause()) {
    return false;
  }
  const SelectSource& s0 = outer->Sources()[0];
  if (s0.table.empty() || s0.query != nullptr || s0.unnest || s0.sample ||
      s0.is_lateral || outer->IsRecursiveWith(s0.table) ||
      !outer->WithQueries().contains(s0.table)) {
    return false;
  }
  const std::string s0_alias = s0.alias.empty() ? s0.table : s0.alias;
  SelectStatement* s0_body = outer->WithQueries().at(s0.table).get();
  std::unordered_map<std::string, ColumnName> s0_out_to_gk;
  if (s0_body == nullptr || s0_body->Sources().empty() ||
      !ValidateGroupedCteTarget(s0_body, &s0_out_to_gk) ||
      relational_detail::StatementContainsVolatileFunction(*s0_body)) {
    return false;
  }
  for (const SelectSource& bs : s0_body->Sources()) {
    if (bs.join_type == JoinType::kRight || bs.join_type == JoinType::kFull) {
      return false;
    }
  }
  const SelectSource& fact_src = s0_body->Sources()[0];
  if (fact_src.table.empty() || fact_src.query != nullptr || fact_src.unnest ||
      fact_src.sample || fact_src.is_lateral ||
      outer->WithQueries().contains(fact_src.table)) {
    return false;
  }
  StatusOr<std::shared_ptr<Table>> fact_tbl_or = ctx.GetTable(fact_src.table);
  if (!fact_tbl_or.HasValue() || fact_tbl_or.Value() == nullptr) {
    return false;
  }
  const Schema& fact_schema = fact_tbl_or.Value()->GetSchema();
  const std::string fact_alias =
      fact_src.alias.empty() ? fact_src.table : fact_src.alias;

  // Ensure none of the other sources in `s0_body` share `fact_schema` column
  // names ambiguously when resolving unqualified `s0_out_to_gk` columns.
  std::unordered_set<std::string> other_s0_cols;
  for (size_t bi = 1; bi < s0_body->Sources().size(); ++bi) {
    const SelectSource& bs = s0_body->Sources()[bi];
    if (bs.table.empty() || bs.query != nullptr ||
        outer->WithQueries().contains(bs.table)) {
      return false;
    }
    StatusOr<std::shared_ptr<Table>> btbl = ctx.GetTable(bs.table);
    if (!btbl.HasValue() || btbl.Value() == nullptr) {
      return false;
    }
    for (size_t ci = 0; ci < btbl.Value()->GetSchema().ColumnCount(); ++ci) {
      other_s0_cols.insert(
          LoweredStarName(btbl.Value()->GetSchema().GetColumn(ci).Name().name));
    }
  }

  // Count CTE references across `*outer`; require `s0.table` and all `LEFT
  // JOIN` CTEs to be single-use before this rewrite.
  std::unordered_set<std::string> all_cte_names;
  for (const auto& [nm, body] : outer->WithQueries()) {
    (void)body;
    all_cte_names.insert(nm);
  }
  std::unordered_map<std::string, size_t> cte_ref_counts;
  CountCteReferencesInStatement(*outer, all_cte_names, &cte_ref_counts);
  if (cte_ref_counts[s0.table] != 1) {
    return false;
  }

  const std::vector<std::string>& w_order = outer->WithQueryOrder();
  const auto s0_ord_it = std::ranges::find(w_order, s0.table);
  if (s0_ord_it == w_order.end()) {
    return false;
  }
  const size_t s0_ord = static_cast<size_t>(s0_ord_it - w_order.begin());

  struct RightLeftJoinCte {
    size_t src_idx{0};
    std::string cte_name;
    std::string alias;
    SelectStatement* body{nullptr};
    std::unordered_map<std::string, ValueType> col_types;
    std::unordered_map<std::string, std::string> out_actual_name;
    std::unordered_map<std::string, ColumnName> out_to_gk;
    // `(r_out_low, s0_out_low)` equijoin pairs from `ON`.
    std::vector<std::pair<std::string, std::string>> eq_pairs;
  };

  std::unordered_map<std::string, std::string> s0_out_actual;
  std::unordered_map<std::string, ValueType> s0_col_types;
  for (size_t i = 0; i < s0_body->SelectList().size(); ++i) {
    const std::string nm =
        InferSelectItemOutputName(s0_body->SelectList()[i], i);
    s0_out_actual.emplace(LoweredStarName(nm), nm);
  }
  for (const auto& [out_low, gk] : s0_out_to_gk) {
    if (gk.schema.empty() || IdentifierEquals(gk.schema, fact_alias)) {
      const int foff = fact_schema.Offset(ColumnName("", gk.name));
      if (foff >= 0 && !other_s0_cols.contains(LoweredStarName(gk.name))) {
        s0_col_types.emplace(
            out_low, fact_schema.GetColumn(static_cast<size_t>(foff)).Type());
      }
    }
  }

  std::unordered_set<std::string> seen_outer_aliases{LoweredStarName(s0_alias)};
  std::vector<RightLeftJoinCte> right_ctes;
  right_ctes.reserve(outer->Sources().size() - 1);
  for (size_t si = 1; si < outer->Sources().size(); ++si) {
    const SelectSource& rs = outer->Sources()[si];
    if (rs.join_type != JoinType::kLeft || rs.table.empty() ||
        rs.query != nullptr || rs.unnest || rs.sample || rs.is_lateral ||
        !rs.using_columns.empty() || rs.from_nested_join ||
        !rs.join_condition || outer->IsRecursiveWith(rs.table) ||
        !outer->WithQueries().contains(rs.table)) {
      return false;
    }
    const std::string ralias = rs.alias.empty() ? rs.table : rs.alias;
    if (!seen_outer_aliases.insert(LoweredStarName(ralias)).second) {
      return false;
    }
    SelectStatement* rbody = outer->WithQueries().at(rs.table).get();
    if (rbody == nullptr ||
        relational_detail::StatementContainsVolatileFunction(*rbody)) {
      return false;
    }
    RightLeftJoinCte rinfo;
    rinfo.src_idx = si;
    rinfo.cte_name = rs.table;
    rinfo.alias = ralias;
    rinfo.body = rbody;
    // The group-key mapping is an optional pushdown hint: a body that is not
    // a simple grouped CTE simply contributes no mapping.
    // ValidateGroupedCteTarget can, however, have inserted entries before it
    // decides to fail, so clear any partial result instead of letting it drive
    // an unsafe pushdown.
    if (!ValidateGroupedCteTarget(rbody, &rinfo.out_to_gk)) {
      rinfo.out_to_gk.clear();
    }
    for (size_t i = 0; i < rbody->SelectList().size(); ++i) {
      const std::string nm =
          InferSelectItemOutputName(rbody->SelectList()[i], i);
      rinfo.out_actual_name.emplace(LoweredStarName(nm), nm);
    }
    // Parse `rs.join_condition`: require pure conjunction of `r_col = s0_col`.
    const std::vector<Expression> on_conjs = SplitConjuncts(rs.join_condition);
    if (on_conjs.empty()) {
      return false;
    }
    bool on_ok = true;
    for (const Expression& oc : on_conjs) {
      if (!oc || oc->Type() != TypeTag::kBinaryExp ||
          oc->AsBinaryExpression().Op() != BinaryOperation::kEquals ||
          !oc->AsBinaryExpression().Left() ||
          !oc->AsBinaryExpression().Right() ||
          oc->AsBinaryExpression().Left()->Type() != TypeTag::kColumnValue ||
          oc->AsBinaryExpression().Right()->Type() != TypeTag::kColumnValue) {
        on_ok = false;
        break;
      }
      const ColumnName& lc =
          oc->AsBinaryExpression().Left()->AsColumnValue().GetColumnName();
      const ColumnName& rc =
          oc->AsBinaryExpression().Right()->AsColumnValue().GetColumnName();
      auto belongs_to_r = [&](const ColumnName& cn) {
        if (!cn.schema.empty()) {
          return IdentifierEquals(cn.schema, ralias);
        }
        return rinfo.out_actual_name.contains(LoweredStarName(cn.name)) &&
               !s0_out_actual.contains(LoweredStarName(cn.name));
      };
      auto belongs_to_s0 = [&](const ColumnName& cn) {
        if (!cn.schema.empty()) {
          return IdentifierEquals(cn.schema, s0_alias);
        }
        return s0_out_actual.contains(LoweredStarName(cn.name)) &&
               !rinfo.out_actual_name.contains(LoweredStarName(cn.name));
      };
      if (belongs_to_r(lc) && belongs_to_s0(rc)) {
        rinfo.eq_pairs.emplace_back(LoweredStarName(lc.name),
                                    LoweredStarName(rc.name));
      } else if (belongs_to_r(rc) && belongs_to_s0(lc)) {
        rinfo.eq_pairs.emplace_back(LoweredStarName(rc.name),
                                    LoweredStarName(lc.name));
      } else {
        on_ok = false;
        break;
      }
    }
    if (!on_ok || rinfo.eq_pairs.empty()) {
      return false;
    }
    right_ctes.push_back(std::move(rinfo));
  }

  auto resolve_unique_outer_owner = [&](const ColumnName& cn, int* owner_out,
                                        std::string* col_low_out) {
    *col_low_out = LoweredStarName(cn.name);
    if (!cn.schema.empty()) {
      if (IdentifierEquals(cn.schema, s0_alias) &&
          s0_out_actual.contains(*col_low_out)) {
        *owner_out = 0;
        return true;
      }
      for (size_t i = 0; i < right_ctes.size(); ++i) {
        if (IdentifierEquals(cn.schema, right_ctes[i].alias) &&
            right_ctes[i].out_actual_name.contains(*col_low_out)) {
          *owner_out = static_cast<int>(i + 1);
          return true;
        }
      }
      return false;
    }
    int hits = 0;
    int last = -1;
    if (s0_out_actual.contains(*col_low_out)) {
      ++hits;
      last = 0;
    }
    for (size_t i = 0; i < right_ctes.size(); ++i) {
      if (right_ctes[i].out_actual_name.contains(*col_low_out)) {
        ++hits;
        last = static_cast<int>(i + 1);
      }
    }
    if (hits != 1) {
      return false;
    }
    *owner_out = last;
    return true;
  };

  // Collect `s0.<gk_out> = const_val` equalities from `outer->WhereClause()`.
  const std::vector<Expression> outer_conjs =
      SplitConjuncts(outer->WhereClause());
  std::unordered_map<std::string, Value> pinned_s0_consts;
  for (const Expression& conj : outer_conjs) {
    if (!conj || conj->Type() != TypeTag::kBinaryExp ||
        conj->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
      continue;
    }
    const auto& bin = conj->AsBinaryExpression();
    auto check_const = [&](const Expression& cside, const Expression& vside) {
      if (!cside || cside->Type() != TypeTag::kColumnValue) {
        return;
      }
      int owner = -1;
      std::string clow;
      if (!resolve_unique_outer_owner(cside->AsColumnValue().GetColumnName(),
                                      &owner, &clow) ||
          owner != 0 || !s0_out_to_gk.contains(clow)) {
        return;
      }
      std::optional<Value> cv = TpcdsEvalClosedConstant(vside);
      if (cv.has_value() && IsSafeDisjointTagValue(*cv)) {
        pinned_s0_consts.emplace(clow, *cv);
      }
    };
    check_const(bin.Left(), bin.Right());
    check_const(bin.Right(), bin.Left());
  }

  // Flatten top-level `OR` branches of a candidate disjunctive conjunct.
  auto split_or = [](auto&& self, const Expression& e,
                     std::vector<Expression>* out) -> void {
    if (e && e->Type() == TypeTag::kBinaryExp &&
        e->AsBinaryExpression().Op() == BinaryOperation::kOr) {
      self(self, e->AsBinaryExpression().Left(), out);
      self(self, e->AsBinaryExpression().Right(), out);
      return;
    }
    out->push_back(e);
  };

  for (const Expression& conj : outer_conjs) {
    std::vector<Expression> or_branches;
    split_or(split_or, conj, &or_branches);
    if (or_branches.size() < 2) {
      continue;
    }
    std::vector<Expression> exists_branches;
    std::unordered_set<size_t> used_r_indices;
    bool disj_ok = true;

    for (const Expression& br : or_branches) {
      // This pushdown adds a correlated probe to the fact scan. Require a
      // column-to-literal comparison so the EXISTS build can compile its
      // local filter. A computed operand such as COALESCE(qty, 0) would
      // otherwise send the whole disjunction through per-row subqueries.
      if (!br || br->Type() != TypeTag::kBinaryExp) {
        disj_ok = false;
        break;
      }
      const auto& comparison = br->AsBinaryExpression();
      const auto column_and_literal = [](const Expression& column,
                                         const Expression& literal) {
        return column && column->Type() == TypeTag::kColumnValue && literal &&
               literal->Type() == TypeTag::kConstantValue;
      };
      if (!IsComparison(comparison.Op()) ||
          (!column_and_literal(comparison.Left(), comparison.Right()) &&
           !column_and_literal(comparison.Right(), comparison.Left()))) {
        disj_ok = false;
        break;
      }
      // Every column in `br` must belong to ONE right CTE `r_owner` (`>= 1`).
      int r_owner = -1;
      bool cols_ok = true;
      Expression null_subst = ReplaceSubExpressions(
          br, [&](const Expression& node) -> std::optional<Expression> {
            if (!node || node->Type() != TypeTag::kColumnValue) {
              return std::nullopt;
            }
            int owner = -1;
            std::string clow;
            if (!resolve_unique_outer_owner(
                    node->AsColumnValue().GetColumnName(), &owner, &clow) ||
                owner < 1) {
              cols_ok = false;
              return std::nullopt;
            }
            if (r_owner < 0) {
              r_owner = owner;
            } else if (r_owner != owner) {
              cols_ok = false;
              return std::nullopt;
            }
            return ConstantValueExp(Value());
          });
      if (!cols_ok || r_owner < 1) {
        disj_ok = false;
        break;
      }
      // Mechanical Theorem 29 check (`evalBool p_i nullRow != some true`):
      // evaluate `br` on all-NULL right columns via ground-truth AST evaluator!
      std::optional<Value> on_null = TpcdsEvalClosedConstant(null_subst);
      if (!on_null.has_value() || on_null->Truthy()) {
        disj_ok = false;
        break;
      }

      const auto ri_idx = static_cast<size_t>(r_owner - 1);
      const RightLeftJoinCte& rcte = right_ctes[ri_idx];
      const auto r_ord_it = std::ranges::find(w_order, rcte.cte_name);
      if (r_ord_it == w_order.end() ||
          std::cmp_greater_equal(r_ord_it - w_order.begin(), s0_ord) ||
          cte_ref_counts[rcte.cte_name] != 1) {
        disj_ok = false;
        break;
      }
      for (const SelectSource& rbs : rcte.body->Sources()) {
        if (rbs.join_type == JoinType::kRight ||
            rbs.join_type == JoinType::kFull) {
          disj_ok = false;
          break;
        }
      }
      if (!disj_ok) {
        break;
      }

      // Build `EXISTS (SELECT 1 FROM <rcte.cte_name> AS __semi_r WHERE ...)`.
      const std::string semi_alias = "__semi_r";
      std::vector<Expression> sub_where;
      size_t correlated_fact_keys = 0;
      for (const auto& [r_col_low, s0_col_low] : rcte.eq_pairs) {
        const auto r_act_it = rcte.out_actual_name.find(r_col_low);
        if (r_act_it == rcte.out_actual_name.end()) {
          disj_ok = false;
          break;
        }
        const Expression r_col_exp =
            ColumnValueExp(ColumnName(semi_alias, r_act_it->second));
        if (const auto pin_it = pinned_s0_consts.find(s0_col_low);
            pin_it != pinned_s0_consts.end()) {
          sub_where.push_back(
              BinaryExpressionExp(r_col_exp, BinaryOperation::kEquals,
                                  ConstantValueExp(pin_it->second)));
          continue;
        }
        const auto gk_it = s0_out_to_gk.find(s0_col_low);
        const auto vt_it = s0_col_types.find(s0_col_low);
        if (gk_it == s0_out_to_gk.end() || vt_it == s0_col_types.end() ||
            (vt_it->second != ValueType::kInt64 &&
             vt_it->second != ValueType::kDate)) {
          disj_ok = false;
          break;
        }
        const int foff = fact_schema.Offset(ColumnName("", gk_it->second.name));
        if (foff < 0 ||
            fact_schema.GetColumn(static_cast<size_t>(foff)).IsUnsigned()) {
          disj_ok = false;
          break;
        }
        const std::string actual_fact_col =
            fact_schema.GetColumn(static_cast<size_t>(foff)).Name().name;
        sub_where.push_back(BinaryExpressionExp(
            r_col_exp, BinaryOperation::kEquals,
            ColumnValueExp(ColumnName(fact_alias, actual_fact_col))));
        ++correlated_fact_keys;
      }
      if (!disj_ok || correlated_fact_keys == 0 || correlated_fact_keys > 2) {
        disj_ok = false;
        break;
      }

      Expression qual_br = ReplaceSubExpressions(
          br, [&](const Expression& node) -> std::optional<Expression> {
            if (!node || node->Type() != TypeTag::kColumnValue) {
              return std::nullopt;
            }
            const ColumnName& cn = node->AsColumnValue().GetColumnName();
            const auto r_act_it =
                rcte.out_actual_name.find(LoweredStarName(cn.name));
            if (r_act_it == rcte.out_actual_name.end()) {
              return std::nullopt;
            }
            return ColumnValueExp(ColumnName(semi_alias, r_act_it->second));
          });
      sub_where.push_back(std::move(qual_br));

      auto sub_stmt = std::make_shared<SelectStatement>();
      sub_stmt->SetSources({SelectSource(rcte.cte_name, semi_alias, nullptr,
                                         JoinType::kCross, nullptr)});
      sub_stmt->SetSelectList(
          {NamedExpression("", ConstantValueExp(Value(int64_t{1})))});
      sub_stmt->SetWhereClause(CombineConjuncts(sub_where));
      exists_branches.push_back(
          QueryExpressionExp(std::move(sub_stmt), nullptr, true, false));
      used_r_indices.insert(ri_idx);
    }

    if (!disj_ok || exists_branches.size() != or_branches.size()) {
      continue;
    }

    // Push `pinned_s0_consts` into `s0_body` and into each single-use right CTE
    // equated on that key so the materialized CTEs only aggregate the active
    // year/slice!
    for (const auto& [s0_col_low, cval] : pinned_s0_consts) {
      const ColumnName& s0_gk = s0_out_to_gk.at(s0_col_low);
      Expression s0_eq =
          BinaryExpressionExp(ColumnValueExp(s0_gk), BinaryOperation::kEquals,
                              ConstantValueExp(cval));
      s0_body->SetWhereClause(s0_body->WhereClause()
                                  ? BinaryExpressionExp(s0_body->WhereClause(),
                                                        BinaryOperation::kAnd,
                                                        std::move(s0_eq))
                                  : std::move(s0_eq));
      for (RightLeftJoinCte& rcte : right_ctes) {
        if (cte_ref_counts[rcte.cte_name] != 1) {
          continue;
        }
        bool r_no_right_full = true;
        for (const SelectSource& rbs : rcte.body->Sources()) {
          if (rbs.join_type == JoinType::kRight ||
              rbs.join_type == JoinType::kFull) {
            r_no_right_full = false;
            break;
          }
        }
        if (!r_no_right_full) {
          continue;
        }
        for (const auto& [r_col_low, mapped_s0_low] : rcte.eq_pairs) {
          if (mapped_s0_low == s0_col_low) {
            if (const auto rgk_it = rcte.out_to_gk.find(r_col_low);
                rgk_it != rcte.out_to_gk.end()) {
              Expression req = BinaryExpressionExp(
                  ColumnValueExp(rgk_it->second), BinaryOperation::kEquals,
                  ConstantValueExp(cval));
              rcte.body->SetWhereClause(
                  rcte.body->WhereClause()
                      ? BinaryExpressionExp(rcte.body->WhereClause(),
                                            BinaryOperation::kAnd,
                                            std::move(req))
                      : std::move(req));
            }
          }
        }
      }
    }

    Expression semi_or = exists_branches[0];
    for (size_t i = 1; i < exists_branches.size(); ++i) {
      semi_or = BinaryExpressionExp(std::move(semi_or), BinaryOperation::kOr,
                                    std::move(exists_branches[i]));
    }

    // Wrap `s0_body->Sources()[0]` (`fact_src`) as a filtered single-table
    // derived source so `ScanBaseTable` evaluates `semi_or` directly on
    // `LSTableStore` via packed `(int32, int32)` `PrebuiltMembershipPredicate`
    // before `LEFT JOIN` and `GROUP BY`!
    auto fact_sub = std::make_shared<SelectStatement>();
    fact_sub->SetSources({SelectSource(fact_src.table, fact_alias, nullptr,
                                       JoinType::kCross, nullptr)});
    std::vector<NamedExpression> fact_sel;
    fact_sel.reserve(fact_schema.ColumnCount());
    for (size_t ci = 0; ci < fact_schema.ColumnCount(); ++ci) {
      const std::string& cnm = fact_schema.GetColumn(ci).Name().name;
      fact_sel.emplace_back(cnm, ColumnValueExp(ColumnName(fact_alias, cnm)));
    }
    fact_sub->SetSelectList(std::move(fact_sel));
    fact_sub->SetWhereClause(std::move(semi_or));

    std::vector<SelectSource> new_s0_sources = s0_body->Sources();
    new_s0_sources[0] = SelectSource("", fact_alias, std::move(fact_sub),
                                     JoinType::kCross, nullptr);
    s0_body->SetSources(std::move(new_s0_sources));
    return true;
  }
  return false;
}

namespace select_rewrite_detail {

// Returns true when `expr` contains any kAggregateExp node.
namespace {
bool ContainsAggregateExpression(  // NOLINT(misc-no-recursion)
    const Expression& expr) {
  if (!expr) {
    return false;
  }
  if (expr->Type() == TypeTag::kAggregateExp) {
    return true;
  }
  return std::ranges::any_of(ExpressionChildren(expr),
                             [](const Expression& child) {
                               return ContainsAggregateExpression(child);
                             });
}
}  // namespace

namespace {
void CollectWindowFunctionCalls(  // NOLINT(misc-no-recursion)
    const Expression& expr,
    std::vector<const WindowFunctionCallExpression*>* out) {
  if (!expr) {
    return;
  }
  if (expr->Type() == TypeTag::kWindowFunctionExp) {
    out->push_back(&expr->AsWindowFunctionCallExpression());
  }
  for (const Expression& child : ExpressionChildren(expr)) {
    CollectWindowFunctionCalls(child, out);
  }
}
}  // namespace

namespace {
bool QualifyPartitionColumnMatches(const ColumnName& a, const ColumnName& b,
                                   size_t source_count) {
  if (!IdentifierEquals(a.name, b.name)) {
    return false;
  }
  if (!a.schema.empty() && !b.schema.empty()) {
    return IdentifierEquals(a.schema, b.schema);
  }
  if (a.schema.empty() && b.schema.empty()) {
    return true;
  }
  return source_count == 1;
}
}  // namespace

namespace {
bool IsVolatileOrUnsafeQualifyFunction(std::string_view fn) {
  static constexpr std::array<std::string_view, 6> kAllowed = {
      "coalesce", "ifnull", "nullif", "greatest", "least", "between"};
  return std::ranges::none_of(kAllowed, [&](std::string_view allowed) {
    return IdentifierEquals(fn, allowed);
  });
}
}  // namespace

namespace {
bool IsSafeQualifyPushdownConjunct(  // NOLINT(misc-no-recursion)
    const Expression& expr) {
  if (!expr) {
    return false;
  }
  switch (expr->Type()) {
    case TypeTag::kColumnValue:
      return expr->AsColumnValue().GetColumnName().name != "*";
    case TypeTag::kConstantValue:
      return dynamic_cast<const ParameterSlot*>(expr.get()) == nullptr;
    case TypeTag::kBinaryExp: {
      const auto& bin = expr->AsBinaryExpression();
      switch (bin.Op()) {
        case BinaryOperation::kEquals:
        case BinaryOperation::kNotEquals:
        case BinaryOperation::kLessThan:
        case BinaryOperation::kLessThanEquals:
        case BinaryOperation::kGreaterThan:
        case BinaryOperation::kGreaterThanEquals:
        case BinaryOperation::kLike:
        case BinaryOperation::kNotLike:
        case BinaryOperation::kIsDistinctFrom:
        case BinaryOperation::kIsNotDistinctFrom:
        case BinaryOperation::kAnd:
        case BinaryOperation::kOr:
        case BinaryOperation::kXor:
          return IsSafeQualifyPushdownConjunct(bin.Left()) &&
                 IsSafeQualifyPushdownConjunct(bin.Right());
        default:
          return false;
      }
    }
    case TypeTag::kUnaryExp: {
      const auto& unary = expr->AsUnaryExpression();
      switch (unary.Op()) {
        case UnaryOperation::kIsNull:
        case UnaryOperation::kIsNotNull:
        case UnaryOperation::kIsTrue:
        case UnaryOperation::kIsNotTrue:
        case UnaryOperation::kIsFalse:
        case UnaryOperation::kIsNotFalse:
        case UnaryOperation::kNot:
          return IsSafeQualifyPushdownConjunct(unary.Child());
        default:
          return false;
      }
    }
    case TypeTag::kInExp:
    case TypeTag::kCaseExp: {
      const auto children = ExpressionChildren(expr);
      return !children.empty() &&
             std::ranges::all_of(children, [](const Expression& child) {
               return IsSafeQualifyPushdownConjunct(child);
             });
    }
    case TypeTag::kFunctionCallExp: {
      const auto& fn = expr->AsFunctionCallExpression();
      if (IsVolatileOrUnsafeQualifyFunction(fn.FuncName())) {
        return false;
      }
      return std::ranges::all_of(fn.Args(), [](const Expression& arg) {
        return IsSafeQualifyPushdownConjunct(arg);
      });
    }
    default:
      return false;
  }
}
}  // namespace

namespace {
bool IsCountStarOrNonNullConst(const Expression& expr) {
  if (!expr || expr->Type() != TypeTag::kAggregateExp) {
    return false;
  }
  const auto& agg = expr->AsAggregateExpression();
  if (agg.GetType() != AggregationType::kCount || agg.Distinct() ||
      agg.HasWhereFilter() || agg.NeedsGroupContext() ||
      agg.HasInnerGroupBy() || agg.IgnoreNulls()) {
    return false;
  }
  const Expression& child = agg.Child();
  if (!child) {
    return true;
  }
  if (child->Type() == TypeTag::kColumnValue) {
    return child->AsColumnValue().GetColumnName().name == "*";
  }
  if (child->Type() == TypeTag::kConstantValue &&
      dynamic_cast<const ParameterSlot*>(child.get()) == nullptr) {
    return !child->AsConstantValue().GetValue().IsNull();
  }
  return false;
}
}  // namespace

}  // namespace select_rewrite_detail

// Lean proof: lean/TinylambOpt/ProposedProofs/QualifyPushdown.lean (sound)
bool PushPartitionOnlyQualifyToWhere(  // NOLINT(misc-no-recursion)
    SelectStatement* select) {
  if (select == nullptr) {
    return false;
  }
  const char* disable_env =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_QUALIFY_PUSHDOWN");
  if (disable_env != nullptr && disable_env[0] != '\0' &&
      disable_env[0] != '0') {
    return false;
  }

  bool changed = false;
  // A CTE body may be shared by multiple reference sites. Rewriting its
  // QUALIFY in place according to this statement's use can change the rows
  // seen by every other consumer, so leave CTE definitions at their own
  // statement boundary. Source subqueries below are uniquely owned.
  for (const SelectSource& source : select->Sources()) {
    if (source.query != nullptr &&
        PushPartitionOnlyQualifyToWhere(source.query.get())) {
      changed = true;
    }
  }

  if (!select->Qualify() || select->HasDistinctOn() || select->ForUpdate() ||
      !select->UnionAll().empty() || select->GetSetOperationTree() != nullptr ||
      select->IsGroupAll()) {
    return changed;
  }

  const bool is_grouped = !select->GroupBy().empty();
  if (select->Having() != nullptr) {
    return changed;
  }

  std::vector<const WindowFunctionCallExpression*> windows;
  for (const NamedExpression& item : select->SelectList()) {
    CollectWindowFunctionCalls(item.expression, &windows);
    if (!is_grouped && ContainsAggregateExpression(item.expression)) {
      return changed;
    }
  }
  CollectWindowFunctionCalls(select->Qualify(), &windows);
  if (!is_grouped && ContainsAggregateExpression(select->Qualify())) {
    return changed;
  }
  for (const auto& term : select->OrderBy()) {
    CollectWindowFunctionCalls(term.expression, &windows);
    if (!is_grouped && ContainsAggregateExpression(term.expression)) {
      return changed;
    }
  }
  if (windows.empty()) {
    return changed;
  }

  const size_t source_count = select->Sources().size();
  std::vector<ColumnName> common_partitions;
  for (const Expression& part_expr : windows.front()->partition_by) {
    if (part_expr && part_expr->Type() == TypeTag::kColumnValue) {
      common_partitions.push_back(part_expr->AsColumnValue().GetColumnName());
    }
  }
  if (common_partitions.empty()) {
    return changed;
  }

  for (size_t wi = 1; wi < windows.size(); ++wi) {
    std::vector<ColumnName> win_cols;
    for (const Expression& part_expr : windows[wi]->partition_by) {
      if (part_expr && part_expr->Type() == TypeTag::kColumnValue) {
        win_cols.push_back(part_expr->AsColumnValue().GetColumnName());
      }
    }
    std::erase_if(common_partitions, [&](const ColumnName& cand) {
      return std::ranges::none_of(win_cols, [&](const ColumnName& wc) {
        return QualifyPartitionColumnMatches(cand, wc, source_count);
      });
    });
    if (common_partitions.empty()) {
      return changed;
    }
  }

  if (is_grouped) {
    std::vector<ColumnName> group_cols;
    for (const Expression& g : select->GroupBy()) {
      if (g && g->Type() == TypeTag::kColumnValue) {
        group_cols.push_back(g->AsColumnValue().GetColumnName());
      }
    }
    std::erase_if(common_partitions, [&](const ColumnName& cand) {
      return std::ranges::none_of(group_cols, [&](const ColumnName& gc) {
        return QualifyPartitionColumnMatches(cand, gc, source_count);
      });
    });
    if (common_partitions.empty()) {
      return changed;
    }
  }

  std::vector<std::string> shadowed_aliases;
  for (const NamedExpression& item : select->SelectList()) {
    if (item.name.empty()) {
      continue;
    }
    const bool same_named_column =
        item.expression && item.expression->Type() == TypeTag::kColumnValue &&
        IdentifierEquals(item.expression->AsColumnValue().GetColumnName().name,
                         item.name);
    if (!same_named_column) {
      shadowed_aliases.push_back(item.name);
    }
  }

  const std::vector<Expression> qualify_conjuncts =
      SplitConjuncts(select->Qualify());
  std::vector<Expression> pushable;
  std::vector<Expression> remaining;
  pushable.reserve(qualify_conjuncts.size());
  remaining.reserve(qualify_conjuncts.size());
  for (const Expression& conjunct : qualify_conjuncts) {
    if (!conjunct) {
      continue;
    }
    bool can_push = IsSafeQualifyPushdownConjunct(conjunct) &&
                    !ContainsQueryExpression(conjunct);
    std::unordered_set<ColumnName> touched;
    if (can_push) {
      touched = conjunct->TouchedColumns();
      if (touched.empty()) {
        can_push = false;
      }
    }
    if (can_push) {
      for (const ColumnName& col : touched) {
        const bool shadowed = std::ranges::any_of(
            shadowed_aliases, [&](const std::string& alias) {
              return col.schema.empty() && IdentifierEquals(alias, col.name);
            });
        const bool in_common = std::ranges::any_of(
            common_partitions, [&](const ColumnName& part_col) {
              return QualifyPartitionColumnMatches(col, part_col, source_count);
            });
        if (shadowed || !in_common) {
          can_push = false;
          break;
        }
      }
    }
    if (can_push) {
      pushable.push_back(conjunct);
    } else {
      remaining.push_back(conjunct);
    }
  }

  if (pushable.empty()) {
    return changed;
  }

  Expression pushed = CombineConjuncts(pushable);
  if (select->WhereClause() != nullptr) {
    select->SetWhereClause(BinaryExpressionExp(
        select->WhereClause(), BinaryOperation::kAnd, std::move(pushed)));
  } else {
    select->SetWhereClause(std::move(pushed));
  }
  select->SetQualify(remaining.empty() ? nullptr : CombineConjuncts(remaining));
  return true;
}

// Lean proof: lean/TinylambOpt/ResearchProofs/OuterJoinCounts.lean (sound)
bool RewriteOuterJoinGroupedCountToHistograms(  // NOLINT(misc-no-recursion)
    SelectStatement* select, TransactionContext& ctx) {
  if (select == nullptr) {
    return false;
  }
  const char* disable_env =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_OUTER_JOIN_COUNTS");
  if (disable_env != nullptr && disable_env[0] != '\0' &&
      disable_env[0] != '0') {
    return false;
  }

  bool changed = false;
  for (const auto& [cte_name, cte_body] : select->WithQueries()) {
    (void)cte_name;
    if (cte_body != nullptr &&
        RewriteOuterJoinGroupedCountToHistograms(cte_body.get(), ctx)) {
      changed = true;
    }
  }
  for (const SelectSource& source : select->Sources()) {
    if (source.query != nullptr &&
        RewriteOuterJoinGroupedCountToHistograms(source.query.get(), ctx)) {
      changed = true;
    }
  }

  if (select->Sources().size() != 2 || select->WhereClause() != nullptr ||
      select->Having() != nullptr || select->Qualify() != nullptr ||
      select->Distinct() || select->HasDistinctOn() || select->WithTies() ||
      select->ForUpdate() || select->IsGroupAll() || select->AsStruct() ||
      !select->UnionAll().empty() || select->GetSetOperationTree() != nullptr ||
      select->GroupBy().size() != 1 || select->SelectList().empty()) {
    return changed;
  }

  const SelectSource& s0 = select->Sources()[0];
  const SelectSource& s1 = select->Sources()[1];
  if (s0.table.empty() || s1.table.empty() || s0.query != nullptr ||
      s1.query != nullptr || s0.unnest != nullptr || s1.unnest != nullptr ||
      select->IsRecursiveWith(s0.table) || select->IsRecursiveWith(s1.table) ||
      s0.sample.has_value() || s1.sample.has_value()) {
    return changed;
  }
  for (const auto& [cte_name, cte_body] : select->WithQueries()) {
    (void)cte_body;
    if (IdentifierEquals(cte_name, s0.table) ||
        IdentifierEquals(cte_name, s1.table)) {
      return changed;
    }
  }

  const JoinType jt = s1.join_type;
  if (jt != JoinType::kFull && jt != JoinType::kLeft &&
      jt != JoinType::kInner) {
    return changed;
  }
  if (!s1.join_condition || s1.join_condition->Type() != TypeTag::kBinaryExp) {
    return changed;
  }
  const auto& eq = s1.join_condition->AsBinaryExpression();
  if (eq.Op() != BinaryOperation::kEquals || !eq.Left() || !eq.Right() ||
      eq.Left()->Type() != TypeTag::kColumnValue ||
      eq.Right()->Type() != TypeTag::kColumnValue) {
    return changed;
  }

  const StatusOr<std::shared_ptr<Table>> t0_or = ctx.GetTable(s0.table);
  const StatusOr<std::shared_ptr<Table>> t1_or = ctx.GetTable(s1.table);
  if (!t0_or.HasValue() || !t1_or.HasValue() || t0_or.Value() == nullptr ||
      t1_or.Value() == nullptr) {
    return changed;
  }
  const Schema& schema0 = t0_or.Value()->GetSchema();
  const Schema& schema1 = t1_or.Value()->GetSchema();

  const std::string l_alias = s0.alias.empty() ? s0.table : s0.alias;
  const std::string r_alias = s1.alias.empty() ? s1.table : s1.alias;

  auto find_col = [](const Schema& schema,
                     const std::string& name) -> std::optional<Column> {
    for (size_t i = 0; i < schema.ColumnCount(); ++i) {
      const Column& c = schema.GetColumn(i);
      if (IdentifierEquals(c.Name().name, name)) {
        return c;
      }
    }
    return std::nullopt;
  };
  auto is_side_col = [&](const ColumnName& col, bool left_side,
                         std::string* resolved_col_name) -> bool {
    const std::string& own_alias = left_side ? l_alias : r_alias;
    const std::string& other_alias = left_side ? r_alias : l_alias;
    const Schema& own_schema = left_side ? schema0 : schema1;
    const Schema& other_schema = left_side ? schema1 : schema0;
    const std::optional<Column> own_col = find_col(own_schema, col.name);
    if (!own_col.has_value()) {
      return false;
    }
    if (!col.schema.empty()) {
      if (!IdentifierEquals(col.schema, own_alias)) {
        return false;
      }
      if (IdentifierEquals(l_alias, r_alias)) {
        return false;
      }
    } else if (find_col(other_schema, col.name).has_value() ||
               IdentifierEquals(own_alias, other_alias)) {
      return false;
    }
    if (resolved_col_name != nullptr) {
      *resolved_col_name = own_col->Name().name;
    }
    return true;
  };

  const ColumnName& eq_a = eq.Left()->AsColumnValue().GetColumnName();
  const ColumnName& eq_b = eq.Right()->AsColumnValue().GetColumnName();
  std::string l_key_name;
  std::string r_key_name;
  const bool side_a_first = is_side_col(eq_a, true, &l_key_name) &&
                            is_side_col(eq_b, false, &r_key_name);
  const bool side_b_first = !side_a_first &&
                            is_side_col(eq_b, true, &l_key_name) &&
                            is_side_col(eq_a, false, &r_key_name);
  if (!side_a_first && !side_b_first) {
    return changed;
  }

  const std::optional<Column> lcol = find_col(schema0, l_key_name);
  const std::optional<Column> rcol = find_col(schema1, r_key_name);
  if (!lcol.has_value() || !rcol.has_value() || lcol->Type() != rcol->Type() ||
      lcol->IsUnsigned() != rcol->IsUnsigned()) {
    return changed;
  }
  if (lcol->Type() != ValueType::kInt64 && lcol->Type() != ValueType::kDate &&
      lcol->Type() != ValueType::kVarChar) {
    return changed;
  }

  auto is_left_key_expr = [&](const Expression& e) -> bool {
    if (!e || e->Type() != TypeTag::kColumnValue) {
      return false;
    }
    std::string resolved;
    return is_side_col(e->AsColumnValue().GetColumnName(), true, &resolved) &&
           IdentifierEquals(resolved, l_key_name);
  };
  auto is_right_key_expr = [&](const Expression& e) -> bool {
    if (!e || e->Type() != TypeTag::kColumnValue) {
      return false;
    }
    std::string resolved;
    return is_side_col(e->AsColumnValue().GetColumnName(), false, &resolved) &&
           IdentifierEquals(resolved, r_key_name);
  };
  auto is_coalesce_keys_expr = [&](const Expression& e) -> bool {
    if (!e || e->Type() != TypeTag::kFunctionCallExp) {
      return false;
    }
    const auto& fn = e->AsFunctionCallExpression();
    if (!IdentifierEquals(fn.FuncName(), "coalesce") || fn.Args().size() != 2) {
      return false;
    }
    return (is_left_key_expr(fn.Args()[0]) &&
            is_right_key_expr(fn.Args()[1])) ||
           (jt != JoinType::kLeft && is_right_key_expr(fn.Args()[0]) &&
            is_left_key_expr(fn.Args()[1]));
  };
  auto matches_group_key = [&](const Expression& e) -> bool {
    if (jt == JoinType::kFull) {
      return is_coalesce_keys_expr(e);
    }
    if (jt == JoinType::kLeft) {
      return is_left_key_expr(e) || is_coalesce_keys_expr(e);
    }
    return is_left_key_expr(e) || is_right_key_expr(e) ||
           is_coalesce_keys_expr(e);
  };

  if (!matches_group_key(select->GroupBy()[0])) {
    return changed;
  }

  size_t count_items = 0;
  for (const NamedExpression& item : select->SelectList()) {
    if (matches_group_key(item.expression)) {
      continue;
    }
    if (IsCountStarOrNonNullConst(item.expression)) {
      ++count_items;
      continue;
    }
    return changed;
  }
  if (count_items == 0) {
    return changed;
  }

  for (const auto& term : select->OrderBy()) {
    if (!term.expression) {
      return changed;
    }
    if (matches_group_key(term.expression) ||
        IsCountStarOrNonNullConst(term.expression)) {
      continue;
    }
    bool matches_alias = false;
    if (term.expression->Type() == TypeTag::kColumnValue) {
      const ColumnName& cn = term.expression->AsColumnValue().GetColumnName();
      if (cn.schema.empty()) {
        size_t hits = 0;
        for (const NamedExpression& item : select->SelectList()) {
          const std::string label =
              item.name.empty() ? ExplicitOrColumnOutputName(item) : item.name;
          if (!label.empty() && IdentifierEquals(label, cn.name)) {
            ++hits;
          }
        }
        matches_alias = (hits == 1);
      }
    }
    if (!matches_alias) {
      return changed;
    }
  }

  auto make_hist = [](const std::string& table_name,
                      const std::string& src_alias,
                      const std::string& key_name) {
    auto hist = std::make_shared<SelectStatement>();
    hist->SetSources({SelectSource(table_name, src_alias, nullptr,
                                   JoinType::kCross, nullptr)});
    Expression key_col = ColumnValueExp(ColumnName(src_alias, key_name));
    hist->SetSelectList({
        NamedExpression("__k", key_col),
        NamedExpression(
            "__cnt",
            AggregateExpressionExp(AggregationType::kCount,
                                   ColumnValueExp(ColumnName("", "*")), false)),
    });
    hist->SetGroupBy({key_col});
    return hist;
  };

  std::shared_ptr<SelectStatement> hist_l =
      make_hist(s0.table, "__ojc_ls", l_key_name);
  std::shared_ptr<SelectStatement> hist_r =
      make_hist(s1.table, "__ojc_rs", r_key_name);

  Expression join_cond = BinaryExpressionExp(
      ColumnValueExp(ColumnName("__ojc_l", "__k")), BinaryOperation::kEquals,
      ColumnValueExp(ColumnName("__ojc_r", "__k")));
  select->SetSources({
      SelectSource("", "__ojc_l", std::move(hist_l), JoinType::kCross, nullptr),
      SelectSource("", "__ojc_r", std::move(hist_r), jt, std::move(join_cond)),
  });

  Expression outer_key;
  Expression outer_cnt;
  if (jt == JoinType::kFull) {
    outer_key = FunctionCallExp("coalesce",
                                {ColumnValueExp(ColumnName("__ojc_l", "__k")),
                                 ColumnValueExp(ColumnName("__ojc_r", "__k"))});
    Expression prod = BinaryExpressionExp(
        FunctionCallExp("coalesce",
                        {ColumnValueExp(ColumnName("__ojc_l", "__cnt")),
                         ConstantValueExp(Value(int64_t{1}))}),
        BinaryOperation::kMultiply,
        FunctionCallExp("coalesce",
                        {ColumnValueExp(ColumnName("__ojc_r", "__cnt")),
                         ConstantValueExp(Value(int64_t{1}))}));
    outer_cnt =
        AggregateExpressionExp(AggregationType::kSum, std::move(prod), false);
    select->SetGroupBy({outer_key});
  } else if (jt == JoinType::kLeft) {
    outer_key = ColumnValueExp(ColumnName("__ojc_l", "__k"));
    outer_cnt = BinaryExpressionExp(
        ColumnValueExp(ColumnName("__ojc_l", "__cnt")),
        BinaryOperation::kMultiply,
        FunctionCallExp("coalesce",
                        {ColumnValueExp(ColumnName("__ojc_r", "__cnt")),
                         ConstantValueExp(Value(int64_t{1}))}));
    select->SetGroupBy({});
  } else {
    outer_key = ColumnValueExp(ColumnName("__ojc_l", "__k"));
    outer_cnt =
        BinaryExpressionExp(ColumnValueExp(ColumnName("__ojc_l", "__cnt")),
                            BinaryOperation::kMultiply,
                            ColumnValueExp(ColumnName("__ojc_r", "__cnt")));
    select->SetGroupBy({});
  }

  std::vector<NamedExpression> new_select;
  new_select.reserve(select->SelectList().size());
  for (const NamedExpression& item : select->SelectList()) {
    if (matches_group_key(item.expression)) {
      std::string out_name = item.name;
      if (out_name.empty()) {
        out_name = ExplicitOrColumnOutputName(item);
      }
      new_select.emplace_back(std::move(out_name), outer_key);
    } else {
      new_select.emplace_back(item.name, outer_cnt);
    }
  }
  select->SetSelectList(std::move(new_select));

  if (!select->OrderBy().empty()) {
    std::vector<SelectStatement::OrderByTerm> new_order = select->OrderBy();
    for (auto& term : new_order) {
      if (matches_group_key(term.expression)) {
        term.expression = outer_key;
      } else if (IsCountStarOrNonNullConst(term.expression)) {
        term.expression = outer_cnt;
      }
    }
    select->SetOrderBy(std::move(new_order));
  }

  return true;
}

namespace select_rewrite_detail {

namespace {
bool ContainsExpensiveStringOp(  // NOLINT(misc-no-recursion)
    const Expression& expr) {
  if (!expr) {
    return false;
  }
  if (expr->Type() == TypeTag::kCaseExp) {
    return true;
  }
  if (expr->Type() == TypeTag::kFunctionCallExp) {
    const std::string fn =
        LoweredStarName(expr->AsFunctionCallExpression().FuncName());
    return !(fn.starts_with("extract") || fn == "date_part" ||
             fn == "date_trunc" || fn == "year" || fn == "month" ||
             fn == "day" || fn == "hour" || fn == "minute" || fn == "second");
  }
  if (expr->Type() == TypeTag::kBinaryExp) {
    const BinaryOperation op = expr->AsBinaryExpression().Op();
    if (op == BinaryOperation::kLike || op == BinaryOperation::kNotLike) {
      return true;
    }
  }
  return std::ranges::any_of(
      ExpressionChildren(expr),
      [](const Expression& child) { return ContainsExpensiveStringOp(child); });
}
}  // namespace

namespace {
Expression RequalifySingleSourceColumns(  // NOLINT(misc-no-recursion)
    const Expression& expr, const std::string& old_table,
    const std::string& old_alias, const std::string& new_alias) {
  if (!expr) {
    return expr;
  }
  if (expr->Type() == TypeTag::kColumnValue) {
    const ColumnName& cn = expr->AsColumnValue().GetColumnName();
    if (cn.schema.empty() || IdentifierEquals(cn.schema, old_table) ||
        (!old_alias.empty() && IdentifierEquals(cn.schema, old_alias))) {
      return ColumnValueExp(ColumnName(new_alias, cn.name));
    }
    return expr;
  }
  const std::vector<Expression> children = ExpressionChildren(expr);
  if (children.empty()) {
    return expr;
  }
  bool changed = false;
  std::vector<Expression> updated;
  updated.reserve(children.size());
  for (const Expression& child : children) {
    Expression next =
        RequalifySingleSourceColumns(child, old_table, old_alias, new_alias);
    if (next.get() != child.get()) {
      changed = true;
    }
    updated.push_back(std::move(next));
  }
  if (!changed) {
    return expr;
  }
  return WithExpressionChildren(expr, std::move(updated));
}
}  // namespace

namespace {
bool IsHoistableGroupedDerived(const SelectSource& src) {
  if (src.query == nullptr || src.is_lateral || src.unnest ||
      src.sample.has_value() || !src.using_columns.empty() ||
      src.from_nested_join || src.join_condition != nullptr) {
    return false;
  }
  const SelectStatement& q = *src.query;
  if (q.GroupBy().empty() || !q.WithQueries().empty() ||
      !q.UnionAll().empty() || q.GetSetOperationTree() != nullptr ||
      q.HasLimit() || q.Offset() != 0 || q.Qualify() || q.Distinct() ||
      !q.DistinctOn().empty() || relational_detail::HasWindowFunctions(q) ||
      q.Sources().empty()) {
    return false;
  }
  for (const SelectSource& bs : q.Sources()) {
    if (bs.table.empty() || bs.query != nullptr || bs.unnest || bs.is_lateral ||
        bs.sample.has_value() || !bs.using_columns.empty() ||
        bs.from_nested_join) {
      return false;
    }
    if (bs.join_condition && ContainsQueryExpression(bs.join_condition)) {
      return false;
    }
  }
  if (q.WhereClause() && ContainsQueryExpression(q.WhereClause())) {
    return false;
  }
  if (q.Having() && ContainsQueryExpression(q.Having())) {
    return false;
  }
  for (const Expression& g : q.GroupBy()) {
    if (!g || ContainsQueryExpression(g)) {
      return false;
    }
  }
  return std::ranges::all_of(q.SelectList(), [](const NamedExpression& item) {
    return item.expression && !ContainsQueryExpression(item.expression);
  });
}
}  // namespace

namespace {
std::string CanonicalGroupedDerivedFingerprint(const SelectStatement& q) {
  std::string fp;
  fp.reserve(256);
  fp += "S:";
  for (const SelectSource& bs : q.Sources()) {
    fp += bs.table;
    fp += '/';
    fp += bs.alias;
    fp += '/';
    fp += std::to_string(static_cast<int>(bs.join_type));
    fp += '/';
    if (bs.join_condition) {
      fp += bs.join_condition->ToString();
    }
    fp += ';';
  }
  fp += "|W:";
  if (q.WhereClause()) {
    fp += q.WhereClause()->ToString();
  }
  fp += "|G:";
  for (const Expression& g : q.GroupBy()) {
    fp += g->ToString();
    fp += ',';
  }
  fp += "|H:";
  if (q.Having()) {
    fp += q.Having()->ToString();
  }
  fp += "|P:";
  for (const NamedExpression& item : q.SelectList()) {
    fp += item.name;
    fp += '=';
    fp += item.expression->ToString();
    fp += ',';
  }
  return fp;
}
}  // namespace

}  // namespace select_rewrite_detail

// Lean proof:
// lean/TinylambOpt/ResearchProofs/Wave9TpcdsMultiChannelAndTpch.lean
//   (GroupAvgThresholdTwoPassSplit_sound)
bool HoistDuplicateDerivedTablesToCte(SelectStatement* select) {
  if (select == nullptr || select->Sources().size() < 2) {
    return false;
  }
  auto& top_sources = const_cast<std::vector<SelectSource>&>(select->Sources());
  std::unordered_map<std::string, std::vector<SelectSource*>> by_fp;
  std::vector<std::string> fp_order;
  const auto record = [&](SelectSource* s) {
    if (!IsHoistableGroupedDerived(*s)) {
      return;
    }
    std::string fp = CanonicalGroupedDerivedFingerprint(*s->query);
    auto& bucket = by_fp[fp];
    if (bucket.empty()) {
      fp_order.push_back(std::move(fp));
    }
    bucket.push_back(s);
  };
  for (SelectSource& src : top_sources) {
    if (src.query == nullptr) {
      continue;
    }
    record(&src);
    if (!src.is_lateral && !src.unnest && !src.sample.has_value() &&
        src.query->WithQueries().empty() && src.query->UnionAll().empty() &&
        src.query->GetSetOperationTree() == nullptr) {
      auto& inner_sources =
          const_cast<std::vector<SelectSource>&>(src.query->Sources());
      for (SelectSource& isrc : inner_sources) {
        if (isrc.query != nullptr) {
          record(&isrc);
        }
      }
    }
  }

  bool hoisted = false;
  size_t next_id = 0;
  for (const std::string& fp : fp_order) {
    auto& sites = by_fp[fp];
    if (sites.size() < 2) {
      continue;
    }
    std::string cte_name;
    do {
      cte_name = "__cse_dt_" + std::to_string(next_id++);
    } while (select->WithQueries().contains(cte_name));
    auto body = std::make_shared<SelectStatement>(*sites[0]->query);
    select->AddWithQuery(cte_name, std::move(body));
    for (SelectSource* site : sites) {
      site->table = cte_name;
      site->query.reset();
    }
    hoisted = true;
  }
  return hoisted;
}

// Lean proof: lean/TinylambOpt/ResearchProofs/CloudBI/PublicBI.lean
//   (sound_GroupBeforeSuffix)
bool PreGroupBeforeExpressionGroupBy(SelectStatement* select,
                                     TransactionContext& ctx) {
  if (select == nullptr || select->Sources().size() != 1 ||
      !select->WithQueries().empty() || !select->UnionAll().empty() ||
      select->GetSetOperationTree() != nullptr || select->Having() ||
      select->Qualify() || select->Distinct() || select->HasLimit() ||
      select->Offset() != 0 || !select->DistinctOn().empty() ||
      relational_detail::HasWindowFunctions(*select) ||
      select->GroupBy().empty() || select->GroupBy().size() > 4) {
    return false;
  }
  const SelectSource& src = select->Sources().front();
  if (src.table.empty() || src.query != nullptr || src.unnest ||
      src.is_lateral || src.sample.has_value() || !src.using_columns.empty() ||
      src.from_nested_join || src.join_condition != nullptr) {
    return false;
  }
  if (select->WhereClause() && ContainsQueryExpression(select->WhereClause())) {
    return false;
  }
  StatusOr<std::shared_ptr<Table>> table_or = ctx.GetTable(src.table);
  if (!table_or.HasValue()) {
    return false;
  }
  const Schema& schema = table_or.Value()->GetSchema();
  const std::string src_qual = src.alias.empty() ? src.table : src.alias;

  std::vector<std::string> base_cols;
  std::unordered_set<std::string> base_col_set;
  bool has_expensive_varchar_key = false;
  std::unordered_set<std::string> group_expr_strs;
  for (const Expression& key : select->GroupBy()) {
    if (!key || ContainsQueryExpression(key)) {
      return false;
    }
    group_expr_strs.insert(key->ToString());
    const bool is_expr_key = (key->Type() != TypeTag::kColumnValue);
    const bool is_expensive = is_expr_key && ContainsExpensiveStringOp(key);
    bool touches_varchar = false;
    for (const ColumnName& cn : key->TouchedColumns()) {
      if (!cn.schema.empty() && !IdentifierEquals(cn.schema, src.table) &&
          (src.alias.empty() || !IdentifierEquals(cn.schema, src.alias))) {
        return false;
      }
      const int off = schema.Offset(ColumnName("", cn.name));
      if (off < 0) {
        return false;
      }
      const Column& col_meta = schema.GetColumn(static_cast<slot_t>(off));
      if (col_meta.Type() == ValueType::kVarChar) {
        touches_varchar = true;
      }
      const std::string canonical_name = col_meta.Name().name;
      if (base_col_set.insert(LoweredStarName(canonical_name)).second) {
        base_cols.push_back(canonical_name);
      }
    }
    if (is_expensive && touches_varchar) {
      has_expensive_varchar_key = true;
    }
  }
  if (!has_expensive_varchar_key || base_cols.empty() || base_cols.size() > 3) {
    return false;
  }

  size_t count_items = 0;
  for (const NamedExpression& item : select->SelectList()) {
    if (!item.expression || ContainsQueryExpression(item.expression)) {
      return false;
    }
    if (IsCountStarOrNonNullConst(item.expression)) {
      ++count_items;
      continue;
    }
    if (group_expr_strs.contains(item.expression->ToString())) {
      continue;
    }
    return false;
  }
  if (count_items == 0) {
    return false;
  }

  for (const auto& term : select->OrderBy()) {
    if (!term.expression || ContainsQueryExpression(term.expression)) {
      return false;
    }
    if (group_expr_strs.contains(term.expression->ToString()) ||
        IsCountStarOrNonNullConst(term.expression)) {
      continue;
    }
    bool matches_alias = false;
    if (term.expression->Type() == TypeTag::kColumnValue) {
      const ColumnName& cn = term.expression->AsColumnValue().GetColumnName();
      if (cn.schema.empty()) {
        size_t hits = 0;
        for (const NamedExpression& item : select->SelectList()) {
          const std::string label =
              item.name.empty() ? ExplicitOrColumnOutputName(item) : item.name;
          if (!label.empty() && IdentifierEquals(label, cn.name)) {
            ++hits;
          }
        }
        matches_alias = (hits == 1);
      }
    }
    if (!matches_alias) {
      return false;
    }
  }

  constexpr const char* kPreAlias = "__pre_g";
  constexpr const char* kPreCnt = "__pre_cnt";
  auto inner = std::make_shared<SelectStatement>();
  inner->SetSources({src});
  if (select->WhereClause()) {
    inner->SetWhereClause(select->WhereClause());
  }
  std::vector<NamedExpression> inner_select;
  std::vector<Expression> inner_group;
  inner_select.reserve(base_cols.size() + 1);
  inner_group.reserve(base_cols.size());
  for (const std::string& col : base_cols) {
    Expression col_ref = ColumnValueExp(ColumnName(src_qual, col));
    inner_select.emplace_back(col, col_ref);
    inner_group.push_back(std::move(col_ref));
  }
  inner_select.emplace_back(
      kPreCnt,
      AggregateExpressionExp(AggregationType::kCount,
                             ColumnValueExp(ColumnName("", "*")), false));
  inner->SetSelectList(std::move(inner_select));
  inner->SetGroupBy(std::move(inner_group));

  Expression outer_sum = AggregateExpressionExp(
      AggregationType::kSum, ColumnValueExp(ColumnName(kPreAlias, kPreCnt)),
      false);

  std::vector<Expression> new_group;
  new_group.reserve(select->GroupBy().size());
  for (const Expression& key : select->GroupBy()) {
    new_group.push_back(
        RequalifySingleSourceColumns(key, src.table, src.alias, kPreAlias));
  }

  std::vector<NamedExpression> new_select;
  new_select.reserve(select->SelectList().size());
  for (const NamedExpression& item : select->SelectList()) {
    if (IsCountStarOrNonNullConst(item.expression)) {
      new_select.emplace_back(item.name, outer_sum);
    } else {
      std::string out_name = item.name;
      if (out_name.empty()) {
        out_name = ExplicitOrColumnOutputName(item);
      }
      new_select.emplace_back(
          std::move(out_name),
          RequalifySingleSourceColumns(item.expression, src.table, src.alias,
                                       kPreAlias));
    }
  }

  if (!select->OrderBy().empty()) {
    std::vector<SelectStatement::OrderByTerm> new_order = select->OrderBy();
    for (auto& term : new_order) {
      if (IsCountStarOrNonNullConst(term.expression)) {
        term.expression = outer_sum;
      } else if (group_expr_strs.contains(term.expression->ToString())) {
        term.expression = RequalifySingleSourceColumns(
            term.expression, src.table, src.alias, kPreAlias);
      }
    }
    select->SetOrderBy(std::move(new_order));
  }

  select->SetSources({SelectSource("", kPreAlias, std::move(inner),
                                   JoinType::kCross, nullptr)});
  select->SetWhereClause(nullptr);
  select->SetGroupBy(std::move(new_group));
  select->SetSelectList(std::move(new_select));
  return true;
}

// Lean proof: lean/TinylambOpt/ResearchProofs/CloudBI/PublicBI.lean
//   (sound_ConstantGroup)
bool CollapseConstantGroupBy(SelectStatement* select, TransactionContext& ctx) {
  if (select == nullptr || select->Sources().size() != 1 ||
      !select->WithQueries().empty() || !select->UnionAll().empty() ||
      select->GetSetOperationTree() != nullptr || select->Having() ||
      select->Qualify() || select->Distinct() ||
      !select->DistinctOn().empty() ||
      relational_detail::HasWindowFunctions(*select) ||
      select->GroupBy().empty() || !select->WhereClause() ||
      ContainsQueryExpression(select->WhereClause())) {
    return false;
  }
  const SelectSource& src = select->Sources().front();
  if (src.table.empty() || src.query != nullptr || src.unnest ||
      src.is_lateral || src.sample.has_value() || !src.using_columns.empty() ||
      src.from_nested_join || src.join_condition != nullptr) {
    return false;
  }
  StatusOr<std::shared_ptr<Table>> table_or = ctx.GetTable(src.table);
  if (!table_or.HasValue()) {
    return false;
  }
  const Schema& schema = table_or.Value()->GetSchema();

  auto matches_src = [&](const std::string& qual) {
    return qual.empty() || IdentifierEquals(qual, src.table) ||
           (!src.alias.empty() && IdentifierEquals(qual, src.alias));
  };

  std::unordered_map<std::string, Expression> const_by_col;
  for (const Expression& conj : SplitConjuncts(select->WhereClause())) {
    if (!conj || conj->Type() != TypeTag::kBinaryExp) {
      continue;
    }
    const auto& bin = conj->AsBinaryExpression();
    if (bin.Op() != BinaryOperation::kEquals) {
      continue;
    }
    const Expression* col_expr = nullptr;
    const Expression* val_expr = nullptr;
    if (bin.Left()->Type() == TypeTag::kColumnValue &&
        bin.Right()->Type() == TypeTag::kConstantValue) {
      col_expr = &bin.Left();
      val_expr = &bin.Right();
    } else if (bin.Right()->Type() == TypeTag::kColumnValue &&
               bin.Left()->Type() == TypeTag::kConstantValue) {
      col_expr = &bin.Right();
      val_expr = &bin.Left();
    } else {
      continue;
    }
    const ColumnName& cn = (*col_expr)->AsColumnValue().GetColumnName();
    if (!matches_src(cn.schema)) {
      continue;
    }
    const int off = schema.Offset(ColumnName("", cn.name));
    if (off < 0) {
      continue;
    }
    const Value& val = (*val_expr)->AsConstantValue().GetValue();
    if (val.IsNull() ||
        val.type != schema.GetColumn(static_cast<slot_t>(off)).Type()) {
      continue;
    }
    const_by_col.try_emplace(LoweredStarName(cn.name), *val_expr);
  }
  if (const_by_col.empty()) {
    return false;
  }

  for (const Expression& key : select->GroupBy()) {
    if (!key || key->Type() != TypeTag::kColumnValue) {
      return false;
    }
    const ColumnName& cn = key->AsColumnValue().GetColumnName();
    if (!matches_src(cn.schema) ||
        !const_by_col.contains(LoweredStarName(cn.name))) {
      return false;
    }
  }

  size_t agg_count = 0;
  for (const NamedExpression& item : select->SelectList()) {
    if (!item.expression || ContainsQueryExpression(item.expression)) {
      return false;
    }
    if (item.expression->Type() == TypeTag::kColumnValue) {
      const ColumnName& cn = item.expression->AsColumnValue().GetColumnName();
      if (!matches_src(cn.schema) ||
          !const_by_col.contains(LoweredStarName(cn.name))) {
        return false;
      }
      continue;
    }
    if (item.expression->Type() == TypeTag::kAggregateExp) {
      const auto& agg = item.expression->AsAggregateExpression();
      if (agg.NeedsGroupContext()) {
        return false;
      }
      ++agg_count;
      continue;
    }
    return false;
  }
  if (agg_count == 0) {
    return false;
  }

  constexpr const char* kCgAlias = "__cg";
  constexpr const char* kCgCnt = "__cg_cnt";
  auto inner = std::make_shared<SelectStatement>();
  inner->SetSources({src});
  inner->SetWhereClause(select->WhereClause());

  std::vector<NamedExpression> inner_select;
  inner_select.reserve(agg_count + 1);
  inner_select.emplace_back(
      kCgCnt,
      AggregateExpressionExp(AggregationType::kCount,
                             ColumnValueExp(ColumnName("", "*")), false));

  std::vector<NamedExpression> outer_select;
  outer_select.reserve(select->SelectList().size());
  size_t next_agg = 0;
  for (const NamedExpression& item : select->SelectList()) {
    std::string out_name = item.name;
    if (out_name.empty()) {
      out_name = ExplicitOrColumnOutputName(item);
    }
    if (item.expression->Type() == TypeTag::kColumnValue) {
      const ColumnName& cn = item.expression->AsColumnValue().GetColumnName();
      outer_select.emplace_back(std::move(out_name),
                                const_by_col.at(LoweredStarName(cn.name)));
    } else {
      const std::string slot_name = "__cg_a_" + std::to_string(next_agg++);
      inner_select.emplace_back(slot_name, item.expression);
      outer_select.emplace_back(
          std::move(out_name), ColumnValueExp(ColumnName(kCgAlias, slot_name)));
    }
  }
  inner->SetSelectList(std::move(inner_select));

  select->SetSources({SelectSource("", kCgAlias, std::move(inner),
                                   JoinType::kCross, nullptr)});
  select->SetWhereClause(BinaryExpressionExp(
      ColumnValueExp(ColumnName(kCgAlias, kCgCnt)),
      BinaryOperation::kGreaterThan, ConstantValueExp(Value(int64_t{0}))));
  select->SetGroupBy({});
  select->SetOrderBy({});
  select->SetSelectList(std::move(outer_select));
  return true;
}

namespace select_rewrite_detail {

namespace {
int SourceIndexForColumn(const ColumnName& cn,
                         const std::vector<SelectSource>& sources) {
  if (cn.schema.empty()) {
    return -1;
  }
  for (size_t i = 0; i < sources.size(); ++i) {
    const std::string& alias =
        sources[i].alias.empty() ? sources[i].table : sources[i].alias;
    if (IdentifierEquals(cn.schema, alias) ||
        IdentifierEquals(cn.schema, sources[i].table)) {
      return static_cast<int>(i);
    }
  }
  return -1;
}
}  // namespace

namespace {
uint32_t TouchedSourceMask(const Expression& expr,
                           const std::vector<SelectSource>& sources) {
  if (!expr) {
    return 0;
  }
  uint32_t mask = 0;
  for (const ColumnName& cn : expr->TouchedColumns()) {
    const int idx = SourceIndexForColumn(cn, sources);
    if (idx < 0) {
      return ~uint32_t{0};
    }
    mask |= (uint32_t{1} << static_cast<uint32_t>(idx));
  }
  return mask;
}
}  // namespace

namespace {
bool TryFactorThreeTableInSubquery(SelectStatement* sq) {
  if (sq == nullptr || sq->Sources().size() != 3 ||
      !sq->WithQueries().empty() || !sq->GroupBy().empty() || sq->Having() ||
      sq->Distinct() || !sq->DistinctOn().empty() || sq->HasLimit() ||
      sq->Offset() != 0 || sq->Qualify() || !sq->UnionAll().empty() ||
      sq->GetSetOperationTree() != nullptr ||
      relational_detail::HasWindowFunctions(*sq) ||
      sq->SelectList().size() != 1) {
    return false;
  }
  const auto& sources = sq->Sources();
  for (const SelectSource& s : sources) {
    if (s.table.empty() || s.query != nullptr || s.unnest || s.is_lateral ||
        s.sample.has_value() || !s.using_columns.empty() ||
        s.from_nested_join ||
        (s.join_type != JoinType::kInner && s.join_type != JoinType::kCross)) {
      return false;
    }
  }
  if (TouchedSourceMask(sq->SelectList()[0].expression, sources) != 1U) {
    return false;
  }

  std::vector<Expression> all_conj;
  for (size_t i = 1; i < 3; ++i) {
    if (sources[i].join_condition) {
      auto parts = SplitConjuncts(sources[i].join_condition);
      all_conj.insert(all_conj.end(), parts.begin(), parts.end());
    }
  }
  if (sq->WhereClause()) {
    auto parts = SplitConjuncts(sq->WhereClause());
    all_conj.insert(all_conj.end(), parts.begin(), parts.end());
  }

  std::vector<Expression> x_only;
  std::vector<Expression> xf_conds;
  std::vector<Expression> xe_conds;
  std::vector<Expression> fe_conds;
  for (const Expression& c : all_conj) {
    if (!c || ContainsQueryExpression(c)) {
      return false;
    }
    const uint32_t m = TouchedSourceMask(c, sources);
    if (m == ~uint32_t{0} || m == 7U) {
      return false;
    }
    if (m == 0U || m == 1U) {
      x_only.push_back(c);
    } else if (m == 3U) {
      xf_conds.push_back(c);
    } else if (m == 5U) {
      xe_conds.push_back(c);
    } else {
      // m in {2, 4, 6}: touches only F and/or E.
      fe_conds.push_back(c);
    }
  }
  if (xf_conds.empty() || xe_conds.empty() || fe_conds.empty()) {
    return false;
  }

  // Require at least one expression equijoin between X and E (`expr(X) =
  // expr(E)` where `expr(E)` is non-column, e.g. `COALESCE(x.k,-1) =
  // COALESCE(e.k,-1)`), and all XF/XE conditions to be binary equalities
  // separating X from F or E.
  struct JoinPair {
    Expression x_side;
    Expression right_side;
    bool from_e{false};
  };
  std::vector<JoinPair> join_pairs;
  bool has_expr_eq_e = false;
  for (const Expression& c : xf_conds) {
    if (c->Type() != TypeTag::kBinaryExp ||
        c->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
      return false;
    }
    const auto& b = c->AsBinaryExpression();
    const uint32_t lm = TouchedSourceMask(b.Left(), sources);
    const uint32_t rm = TouchedSourceMask(b.Right(), sources);
    if (lm == 1U && rm == 2U) {
      join_pairs.push_back({b.Left(), b.Right(), false});
    } else if (lm == 2U && rm == 1U) {
      join_pairs.push_back({b.Right(), b.Left(), false});
    } else {
      return false;
    }
  }
  for (const Expression& c : xe_conds) {
    if (c->Type() != TypeTag::kBinaryExp ||
        c->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
      return false;
    }
    const auto& b = c->AsBinaryExpression();
    const uint32_t lm = TouchedSourceMask(b.Left(), sources);
    const uint32_t rm = TouchedSourceMask(b.Right(), sources);
    if (lm == 1U && rm == 4U) {
      if (b.Right()->Type() != TypeTag::kColumnValue) {
        has_expr_eq_e = true;
      }
      join_pairs.push_back({b.Left(), b.Right(), true});
    } else if (lm == 4U && rm == 1U) {
      if (b.Left()->Type() != TypeTag::kColumnValue) {
        has_expr_eq_e = true;
      }
      join_pairs.push_back({b.Right(), b.Left(), true});
    } else {
      return false;
    }
  }
  if (!has_expr_eq_e) {
    return false;
  }

  constexpr const char* kFkAlias = "__fk_del";
  auto inner = std::make_shared<SelectStatement>();
  SelectSource s_f = sources[1];
  SelectSource s_e = sources[2];
  s_f.join_type = JoinType::kCross;
  s_f.join_condition = nullptr;
  s_e.join_type = JoinType::kCross;
  s_e.join_condition = nullptr;
  inner->SetSources({std::move(s_f), std::move(s_e)});
  inner->SetWhereClause(CombineConjuncts(fe_conds));

  std::vector<NamedExpression> inner_select;
  std::vector<Expression> inner_group;
  std::vector<Expression> outer_join_conds;
  inner_select.reserve(join_pairs.size());
  inner_group.reserve(join_pairs.size());
  outer_join_conds.reserve(join_pairs.size());
  for (size_t i = 0; i < join_pairs.size(); ++i) {
    const std::string col_name = "__fk_k" + std::to_string(i);
    inner_select.emplace_back(col_name, join_pairs[i].right_side);
    inner_group.push_back(join_pairs[i].right_side);
    outer_join_conds.push_back(
        BinaryExpressionExp(join_pairs[i].x_side, BinaryOperation::kEquals,
                            ColumnValueExp(ColumnName(kFkAlias, col_name))));
  }
  inner->SetSelectList(std::move(inner_select));
  inner->SetGroupBy(std::move(inner_group));

  SelectSource s_x = sources[0];
  sq->SetSources({
      std::move(s_x),
      SelectSource("", kFkAlias, std::move(inner), JoinType::kInner,
                   CombineConjuncts(outer_join_conds)),
  });
  sq->SetWhereClause(x_only.empty() ? Expression{} : CombineConjuncts(x_only));
  return true;
}
}  // namespace

}  // namespace select_rewrite_detail

// Lean proof: lean/TinylambOpt/ResearchProofs/DuckDBGaps/FileMasks.lean
//   (FileKeyAntiJoin_sound)
bool FactorDeleteAntiJoinSubquery(SelectStatement* select) {
  if (select == nullptr || !select->WhereClause()) {
    return false;
  }
  bool changed = false;
  for (const Expression& conj : SplitConjuncts(select->WhereClause())) {
    if (!conj) {
      continue;
    }
    Expression target = conj;
    if (target->Type() == TypeTag::kUnaryExp &&
        target->AsUnaryExpression().Op() == UnaryOperation::kNot) {
      target = target->AsUnaryExpression().Child();
    }
    if (!target || target->Type() != TypeTag::kQueryExp) {
      continue;
    }
    const auto& qe = target->AsQueryExpression();
    if (qe.Exists() || qe.ArrayResult() || !qe.Test() ||
        qe.Query() == nullptr) {
      continue;
    }
    if (TryFactorThreeTableInSubquery(
            const_cast<SelectStatement*>(qe.Query().get()))) {
      changed = true;
    }
  }
  return changed;
}

// Lean proof: lean/TinylambOpt/ResearchProofs/Wave9TpcdsRollupAndSetOps.lean
//   (SurrogateKeyPreAggRollup_sound)
bool PreAggregateSurrogateKeyBeforeRollup(  // NOLINT(misc-no-recursion)
    SelectStatement* outer, TransactionContext& ctx) {
  if (outer == nullptr) {
    return false;
  }
  bool any_changed = false;
  for (const auto& [name, body] : outer->WithQueries()) {
    if (!outer->IsRecursiveWith(name) && body != nullptr) {
      any_changed |= PreAggregateSurrogateKeyBeforeRollup(body.get(), ctx);
    }
  }
  for (const SelectSource& src : outer->Sources()) {
    if (src.query != nullptr && !src.is_lateral) {
      any_changed |= PreAggregateSurrogateKeyBeforeRollup(src.query.get(), ctx);
    }
  }
  if (outer->UnionAll().empty() || outer->GetSetOperationTree() != nullptr ||
      outer->Distinct() || outer->HasDistinctOn() || outer->Having() ||
      outer->Qualify() || outer->GroupBy().size() < 2 ||
      outer->Sources().size() < 2 ||
      relational_detail::HasWindowFunctions(*outer) ||
      relational_detail::StatementContainsVolatileFunction(*outer)) {
    return any_changed;
  }

  // Collect all branches (head + UnionAll) and verify identical FROM+WHERE
  // input shape across every UNION ALL branch (ROLLUP / GROUPING SETS shape).
  std::vector<SelectStatement*> branches;
  branches.reserve(1 + outer->UnionAll().size());
  branches.push_back(outer);
  auto input_shape = [](const SelectStatement& part) {
    SelectStatement shape(part);
    shape.SetSelectList({});
    shape.SetGroupBy({});
    shape.SetGroupAll(false);
    shape.SetHaving(nullptr);
    shape.SetOrderBy({});
    shape.SetLimit(std::nullopt);
    shape.SetOffset(0);
    shape.SetWithTies(false);
    shape.ClearUnionAll();
    shape.SetSetOperationTree(nullptr);
    shape.ClearWithQueries();
    return std::string(shape.Fingerprint());
  };
  const std::string head_shape = input_shape(*outer);
  for (size_t i = 0; i < outer->UnionAll().size(); ++i) {
    const auto& br = outer->UnionAll()[i];
    if (br == nullptr) {
      return false;
    }
    const auto match = outer->MatchFor(i);
    if (br->Distinct() || br->HasDistinctOn() || br->Having() ||
        br->Qualify() || br->HasLimit() || br->Offset() != 0 ||
        !br->OrderBy().empty() || !br->WithQueries().empty() ||
        !br->UnionAll().empty() || br->GetSetOperationTree() != nullptr ||
        match.by_name || match.corresponding || !match.columns.empty() ||
        (i < outer->SetOperationKinds().size() &&
         outer->SetOperationKinds()[i] != SetOperationKind::kUnionAll) ||
        relational_detail::HasWindowFunctions(*br) ||
        relational_detail::StatementContainsVolatileFunction(*br) ||
        input_shape(*br) != head_shape) {
      return false;
    }
    branches.push_back(br.get());
  }

  const auto& sources = outer->Sources();
  std::vector<std::shared_ptr<Table>> tables;
  tables.reserve(sources.size());
  for (const SelectSource& s : sources) {
    if (s.table.empty() || s.query != nullptr || s.unnest || s.is_lateral ||
        s.sample.has_value() || !s.using_columns.empty() ||
        s.from_nested_join ||
        (s.join_type != JoinType::kInner && s.join_type != JoinType::kCross)) {
      return false;
    }
    StatusOr<std::shared_ptr<Table>> t = ctx.GetTable(s.table);
    if (!t.HasValue() || t.Value() == nullptr) {
      return false;
    }
    tables.push_back(t.MoveValue());
  }

  auto resolve_source_idx = [&](const ColumnName& cn) -> int {
    if (!cn.schema.empty()) {
      for (size_t i = 0; i < sources.size(); ++i) {
        const std::string& al =
            sources[i].alias.empty() ? sources[i].table : sources[i].alias;
        if (IdentifierEquals(cn.schema, al) ||
            IdentifierEquals(cn.schema, sources[i].table)) {
          return static_cast<int>(i);
        }
      }
      return -1;
    }
    int found = -1;
    for (size_t i = 0; i < tables.size(); ++i) {
      if (tables[i]->GetSchema().Offset(ColumnName("", cn.name)) >= 0) {
        if (found >= 0) {
          return -1;
        }
        found = static_cast<int>(i);
      }
    }
    return found;
  };

  // All GROUP BY columns across every branch must belong to one single
  // dimension source `dim_idx`.
  int dim_idx = -1;
  for (const SelectStatement* br : branches) {
    for (const Expression& gk : br->GroupBy()) {
      if (!gk) {
        return false;
      }
      if (br->IsGroupAll() && gk->Type() == TypeTag::kConstantValue) {
        continue;
      }
      if (gk->Type() != TypeTag::kColumnValue) {
        return false;
      }
      const int si = resolve_source_idx(gk->AsColumnValue().GetColumnName());
      if (si < 0) {
        return false;
      }
      if (dim_idx < 0) {
        dim_idx = si;
      } else if (dim_idx != si) {
        return false;
      }
    }
  }
  if (dim_idx < 0) {
    return false;
  }

  // Collect all WHERE + JOIN ON conjuncts and verify that `dim_idx` is touched
  // by exactly one equijoin `fk_col = dim_pk` (no other filters on `dim_idx`).
  std::vector<Expression> all_conj;
  for (const SelectSource& s : sources) {
    if (s.join_condition) {
      auto parts = SplitConjuncts(s.join_condition);
      all_conj.insert(all_conj.end(), parts.begin(), parts.end());
    }
  }
  if (outer->WhereClause()) {
    auto parts = SplitConjuncts(outer->WhereClause());
    all_conj.insert(all_conj.end(), parts.begin(), parts.end());
  }

  Expression fk_expr;
  Expression dim_pk_expr;
  std::vector<Expression> pre_where;
  size_t dim_conj_count = 0;
  for (const Expression& c : all_conj) {
    if (!c || ContainsQueryExpression(c)) {
      return false;
    }
    bool touches_dim = false;
    for (const ColumnName& cn : c->TouchedColumns()) {
      const int si = resolve_source_idx(cn);
      if (si < 0) {
        return false;
      }
      if (si == dim_idx) {
        touches_dim = true;
      }
    }
    if (!touches_dim) {
      pre_where.push_back(c);
      continue;
    }
    ++dim_conj_count;
    if (dim_conj_count > 1 || c->Type() != TypeTag::kBinaryExp ||
        c->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
      return false;
    }
    const auto& bin = c->AsBinaryExpression();
    if (bin.Left()->Type() != TypeTag::kColumnValue ||
        bin.Right()->Type() != TypeTag::kColumnValue) {
      return false;
    }
    const int lsi =
        resolve_source_idx(bin.Left()->AsColumnValue().GetColumnName());
    const int rsi =
        resolve_source_idx(bin.Right()->AsColumnValue().GetColumnName());
    if (lsi == dim_idx && rsi >= 0 && rsi != dim_idx) {
      dim_pk_expr = bin.Left();
      fk_expr = bin.Right();
    } else if (rsi == dim_idx && lsi >= 0 && lsi != dim_idx) {
      dim_pk_expr = bin.Right();
      fk_expr = bin.Left();
    } else {
      return false;
    }
  }
  if (dim_conj_count != 1 || !fk_expr || !dim_pk_expr) {
    return false;
  }

  const std::string& dim_table = sources[static_cast<size_t>(dim_idx)].table;
  const std::string dim_pk_name =
      dim_pk_expr->AsColumnValue().GetColumnName().name;
  const RuntimeUniqueColInfo uinfo =
      InspectRuntimeUniqueNonNullColumn(ctx, dim_table, dim_pk_name);
  if (!uinfo.unique_non_null) {
    return false;
  }

  // Verify every SELECT item across every branch is either a constant/NULL,
  // a bare column from `dim_idx`, or a decomposable aggregate (`AVG(col)`,
  // `SUM(col)`, `COUNT(col)`, `COUNT(*)`) whose argument does not touch
  // `dim_idx`.
  struct PreAggEntry {
    AggregationType type{AggregationType::kCount};
    Expression arg;  // nullptr for COUNT(*)
    std::string sum_col;
    std::string cnt_col;
  };
  std::vector<PreAggEntry> agg_entries;
  std::unordered_map<std::string, size_t> agg_by_str;

  for (const SelectStatement* br : branches) {
    for (const NamedExpression& item : br->SelectList()) {
      if (!item.expression || ContainsQueryExpression(item.expression)) {
        return false;
      }
      const Expression& e = item.expression;
      if (e->Type() == TypeTag::kConstantValue) {
        continue;
      }
      if (e->Type() == TypeTag::kColumnValue) {
        if (resolve_source_idx(e->AsColumnValue().GetColumnName()) != dim_idx) {
          return false;
        }
        continue;
      }
      if (e->Type() == TypeTag::kAggregateExp) {
        const auto& agg = e->AsAggregateExpression();
        if (agg.Distinct() || agg.NeedsGroupContext()) {
          return false;
        }
        const AggregationType at = agg.GetType();
        if (at != AggregationType::kAvg && at != AggregationType::kSum &&
            at != AggregationType::kCount) {
          return false;
        }
        const Expression& arg = agg.Child();
        const bool is_count_star =
            at == AggregationType::kCount &&
            (!arg || (arg->Type() == TypeTag::kColumnValue &&
                      arg->AsColumnValue().GetColumnName().name == "*"));
        if (!is_count_star) {
          if (!arg || arg->Type() != TypeTag::kColumnValue) {
            return false;
          }
          const int asi =
              resolve_source_idx(arg->AsColumnValue().GetColumnName());
          if (asi < 0 || asi == dim_idx) {
            return false;
          }
          const ColumnName& acn = arg->AsColumnValue().GetColumnName();
          const Schema& asch = tables[static_cast<size_t>(asi)]->GetSchema();
          const int aoff = asch.Offset(ColumnName("", acn.name));
          if (aoff < 0) {
            return false;
          }
          const Column& acol = asch.GetColumn(static_cast<slot_t>(aoff));
          if (at != AggregationType::kCount &&
              (acol.IsUnsigned() || acol.IsInterval() ||
               (acol.Type() != ValueType::kInt64 &&
                acol.Type() != ValueType::kDouble))) {
            return false;
          }
        }
        const std::string key = e->ToString();
        if (!agg_by_str.contains(key)) {
          const size_t idx = agg_entries.size();
          agg_by_str.emplace(key, idx);
          PreAggEntry entry;
          entry.type = at;
          entry.arg = is_count_star ? Expression{} : arg;
          if (at == AggregationType::kAvg || at == AggregationType::kSum) {
            entry.sum_col = "__rp_s" + std::to_string(idx);
          }
          if (at == AggregationType::kAvg || at == AggregationType::kCount) {
            entry.cnt_col = "__rp_c" + std::to_string(idx);
          }
          agg_entries.push_back(std::move(entry));
        }
        continue;
      }
      // Allow `CAST(NULL AS ...)` emitted by ROLLUP expansion.
      if (e->TouchedColumns().empty()) {
        continue;
      }
      return false;
    }
  }
  if (agg_entries.empty()) {
    return false;
  }

  std::string cte_name = "__rollup_pre";
  for (size_t k = 0; outer->WithQueries().contains(cte_name); ++k) {
    cte_name = "__rollup_pre_" + std::to_string(k);
  }
  constexpr const char* kRpAlias = "__rp";
  constexpr const char* kRpFk = "__rp_fk";

  auto pre = std::make_shared<SelectStatement>();
  std::vector<SelectSource> pre_sources;
  pre_sources.reserve(sources.size() - 1);
  for (size_t i = 0; i < sources.size(); ++i) {
    if (std::cmp_equal(i, dim_idx)) {
      continue;
    }
    SelectSource s = sources[i];
    s.join_type = JoinType::kCross;
    s.join_condition = nullptr;
    pre_sources.push_back(std::move(s));
  }
  pre->SetSources(std::move(pre_sources));
  pre_where.push_back(UnaryExpressionExp(fk_expr, UnaryOperation::kIsNotNull));
  pre->SetWhereClause(CombineConjuncts(pre_where));
  pre->SetGroupBy({fk_expr});

  std::vector<NamedExpression> pre_select;
  pre_select.emplace_back(kRpFk, fk_expr);
  for (const PreAggEntry& entry : agg_entries) {
    if (!entry.sum_col.empty()) {
      pre_select.emplace_back(
          entry.sum_col,
          AggregateExpressionExp(AggregationType::kSum, entry.arg, false));
    }
    if (!entry.cnt_col.empty()) {
      Expression cnt_arg =
          entry.arg ? entry.arg : ColumnValueExp(ColumnName("", "*"));
      pre_select.emplace_back(
          entry.cnt_col, AggregateExpressionExp(AggregationType::kCount,
                                                std::move(cnt_arg), false));
    }
  }
  pre->SetSelectList(std::move(pre_select));
  outer->AddWithQuery(cte_name, std::move(pre));

  SelectSource dim_src = sources[static_cast<size_t>(dim_idx)];
  dim_src.join_type = JoinType::kCross;
  dim_src.join_condition = nullptr;
  Expression rp_join_eq =
      BinaryExpressionExp(ColumnValueExp(ColumnName(kRpAlias, kRpFk)),
                          BinaryOperation::kEquals, dim_pk_expr);

  for (SelectStatement* br : branches) {
    br->SetSources({
        SelectSource(cte_name, kRpAlias, nullptr, JoinType::kCross, nullptr),
        dim_src,
    });
    br->SetWhereClause(rp_join_eq);
    std::vector<NamedExpression> new_sel;
    new_sel.reserve(br->SelectList().size());
    for (const NamedExpression& item : br->SelectList()) {
      if (item.expression &&
          item.expression->Type() == TypeTag::kAggregateExp) {
        const size_t idx = agg_by_str.at(item.expression->ToString());
        const PreAggEntry& entry = agg_entries[idx];
        Expression combined;
        if (entry.type == AggregationType::kAvg) {
          Expression sum_s = AggregateExpressionExp(
              AggregationType::kSum,
              ColumnValueExp(ColumnName(kRpAlias, entry.sum_col)), false);
          Expression sum_c = AggregateExpressionExp(
              AggregationType::kSum,
              ColumnValueExp(ColumnName(kRpAlias, entry.cnt_col)), false);
          Expression is_zero =
              BinaryExpressionExp(sum_c, BinaryOperation::kEquals,
                                  ConstantValueExp(Value(int64_t{0})));
          Expression num =
              BinaryExpressionExp(ConstantValueExp(Value(1.0)),
                                  BinaryOperation::kMultiply, std::move(sum_s));
          Expression ratio = BinaryExpressionExp(
              std::move(num), BinaryOperation::kDivide, std::move(sum_c));
          combined = CaseExpressionExp(
              {{std::move(is_zero), ConstantValueExp(Value())}},
              std::move(ratio));
        } else if (entry.type == AggregationType::kSum) {
          combined = AggregateExpressionExp(
              AggregationType::kSum,
              ColumnValueExp(ColumnName(kRpAlias, entry.sum_col)), false);
        } else {
          combined = FunctionCallExp(
              "coalesce",
              {AggregateExpressionExp(
                   AggregationType::kSum,
                   ColumnValueExp(ColumnName(kRpAlias, entry.cnt_col)), false),
               ConstantValueExp(Value(int64_t{0}))});
        }
        new_sel.emplace_back(item.name, std::move(combined));
      } else {
        new_sel.push_back(item);
      }
    }
    br->SetSelectList(std::move(new_sel));
  }
  return true;
}

// Lean proof: lean/TinylambOpt/ResearchProofs/CloudBI/NestedBI.lean
//   (sound_FlattenSum, sound_BranchHistogram)
bool FoldNestedSumUnionAllBranches(SelectStatement* select) {
  if (select == nullptr || select->Sources().size() != 1 ||
      !select->WithQueries().empty() || !select->UnionAll().empty() ||
      select->GetSetOperationTree() != nullptr || select->WhereClause() ||
      !select->GroupBy().empty() || select->Having() || select->Qualify() ||
      select->Distinct() || select->HasLimit() || select->Offset() != 0 ||
      !select->OrderBy().empty() || select->SelectList().size() != 1) {
    return false;
  }
  const auto is_simple_sum_of_col = [](const SelectStatement& q,
                                       std::string* col_name_out) -> bool {
    if (q.SelectList().size() != 1) {
      return false;
    }
    const Expression& expr = q.SelectList()[0].expression;
    if (!expr || expr->Type() != TypeTag::kAggregateExp) {
      return false;
    }
    const auto& agg = expr->AsAggregateExpression();
    if (agg.GetType() != AggregationType::kSum || agg.Distinct() ||
        agg.HasWhereFilter() || !agg.Child() ||
        agg.Child()->Type() != TypeTag::kColumnValue) {
      return false;
    }
    if (col_name_out != nullptr) {
      *col_name_out = agg.Child()->AsColumnValue().GetColumnName().name;
    }
    return true;
  };
  const auto is_pure_union_all_stmt = [](const SelectStatement& q) -> bool {
    if (q.UnionDistinct() || q.UnionByName() || !q.WithQueries().empty() ||
        q.HasLimit() || q.Offset() != 0 || !q.OrderBy().empty()) {
      return false;
    }
    if (const auto& tree = q.GetSetOperationTree(); tree != nullptr) {
      if (!tree->first || tree->branches.empty()) {
        return false;
      }
      return std::ranges::all_of(tree->kinds, [](SetOperationKind k) {
        return k == SetOperationKind::kUnionAll;
      });
    }
    if (q.UnionAll().empty()) {
      return false;
    }
    return std::ranges::all_of(q.SetOperationKinds(), [](SetOperationKind k) {
      return k == SetOperationKind::kUnionAll;
    });
  };
  const auto is_simple_leaf_sum = [](const SelectStatement& q) -> bool {
    if (q.Sources().empty() || !q.WithQueries().empty() ||
        !q.UnionAll().empty() || q.GetSetOperationTree() != nullptr ||
        !q.GroupBy().empty() || q.Having() || q.Qualify() || q.Distinct() ||
        q.HasLimit() || q.Offset() != 0 || !q.OrderBy().empty() ||
        q.SelectList().size() != 1) {
      return false;
    }
    for (const SelectSource& s : q.Sources()) {
      if (s.query != nullptr || s.table.empty() || s.is_lateral || s.unnest ||
          s.sample.has_value()) {
        return false;
      }
    }
    const Expression& expr = q.SelectList()[0].expression;
    if (!expr || expr->Type() != TypeTag::kAggregateExp) {
      return false;
    }
    const auto& agg = expr->AsAggregateExpression();
    return agg.GetType() == AggregationType::kSum && !agg.Distinct() &&
           !agg.HasWhereFilter() && agg.Child() != nullptr &&
           !ContainsQueryExpression(agg.Child());
  };

  std::string outer_sum_col;
  if (!is_simple_sum_of_col(*select, &outer_sum_col)) {
    return false;
  }
  auto& top_sources = const_cast<std::vector<SelectSource>&>(select->Sources());
  SelectSource& top_src = top_sources[0];
  if (top_src.query == nullptr || top_src.is_lateral || top_src.unnest ||
      top_src.sample.has_value()) {
    return false;
  }

  std::vector<std::shared_ptr<SelectStatement>> leaves;
  bool flattened_any = false;
  auto collect_leaves = [&](auto&& self, const SelectStatement& node,
                            int depth) -> bool {
    if (depth > 8) {
      return false;
    }
    if (is_pure_union_all_stmt(node)) {
      if (const auto& tree = node.GetSetOperationTree(); tree != nullptr) {
        if (!tree->first || !self(self, *tree->first, depth + 1)) {
          return false;
        }
        return std::ranges::all_of(tree->branches, [&](const auto& br) {
          return br && self(self, *br, depth + 1);
        });
      }
      // Split head (copy without UnionAll) and tail branches.
      auto head = std::make_shared<SelectStatement>(node);
      head->ClearUnionAll();
      if (!self(self, *head, depth + 1)) {
        return false;
      }
      return std::ranges::all_of(node.UnionAll(), [&](const auto& br) {
        return br && self(self, *br, depth + 1);
      });
    }
    // Nested `SELECT SUM(col) FROM (... UNION ALL ...) alias`
    std::string inner_col;
    if (node.Sources().size() == 1 && node.Sources()[0].query != nullptr &&
        !node.Sources()[0].is_lateral && !node.Sources()[0].unnest &&
        !node.Sources()[0].sample.has_value() && node.WithQueries().empty() &&
        node.UnionAll().empty() && node.GetSetOperationTree() == nullptr &&
        !node.WhereClause() && node.GroupBy().empty() && !node.Having() &&
        !node.Qualify() && !node.Distinct() && !node.HasLimit() &&
        node.Offset() == 0 && node.OrderBy().empty() &&
        is_simple_sum_of_col(node, &inner_col) &&
        is_pure_union_all_stmt(*node.Sources()[0].query)) {
      flattened_any = true;
      return self(self, *node.Sources()[0].query, depth + 1);
    }
    if (is_simple_leaf_sum(node)) {
      leaves.push_back(std::make_shared<SelectStatement>(node));
      return true;
    }
    return false;
  };

  if (!collect_leaves(collect_leaves, *top_src.query, 0) || leaves.size() < 2) {
    return false;
  }

  // Deduplicate structurally identical leaf branches and count multiplicity.
  std::unordered_map<std::string, size_t> fp_to_idx;
  std::vector<std::shared_ptr<SelectStatement>> unique_leaves;
  std::vector<int64_t> multiplicities;
  unique_leaves.reserve(leaves.size());
  multiplicities.reserve(leaves.size());
  for (auto& leaf : leaves) {
    // Normalize output alias to `outer_sum_col` before fingerprinting so
    // branches differing only in alias still fold.
    std::vector<NamedExpression> norm_sel = leaf->SelectList();
    norm_sel[0].name = outer_sum_col;
    leaf->SetSelectList(std::move(norm_sel));
    std::string fp = CanonicalGroupedDerivedFingerprint(*leaf);
    auto [it, inserted] =
        fp_to_idx.emplace(std::move(fp), unique_leaves.size());
    if (inserted) {
      unique_leaves.push_back(std::move(leaf));
      multiplicities.push_back(1);
    } else {
      ++multiplicities[it->second];
    }
  }

  if (!flattened_any && unique_leaves.size() == leaves.size()) {
    return false;
  }

  for (size_t i = 0; i < unique_leaves.size(); ++i) {
    if (multiplicities[i] > 1) {
      std::vector<NamedExpression> sel = unique_leaves[i]->SelectList();
      sel[0].expression =
          BinaryExpressionExp(sel[0].expression, BinaryOperation::kMultiply,
                              ConstantValueExp(Value(multiplicities[i])));
      unique_leaves[i]->SetSelectList(std::move(sel));
    }
  }

  auto folded = std::make_shared<SelectStatement>(*unique_leaves[0]);
  folded->ClearUnionAll();
  for (size_t i = 1; i < unique_leaves.size(); ++i) {
    folded->AddUnionAll(std::move(unique_leaves[i]));
  }
  top_src.query = std::move(folded);
  return true;
}

}  // namespace tinylamb
