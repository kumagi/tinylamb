/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/platform/process.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/relational_fast_paths.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/window_eval.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "table/tlcs_file.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

// Lean proof: lean/TinylambOpt/PredicateAndRelationalLowering.lean
//   (partitioned_order_by_limit_band_pruning_sound)
StatusOr<std::optional<Relation>> TryExecutePartitionBandedOrderByLimit(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& inherited_ctes, bool columnar_output) {
  thread_local int band_depth = 0;
  if (band_depth > 0) {
    return std::optional<Relation>{};
  }
  if (!statement.WithQueries().empty() || !statement.UnionAll().empty() ||
      statement.GetSetOperationTree() != nullptr ||
      !statement.GroupBy().empty() || statement.IsGroupAll() ||
      statement.Having() || statement.Distinct() || statement.HasDistinctOn() ||
      statement.WithTies() || statement.AsStruct() || statement.ForUpdate() ||
      !statement.HasLimit() || statement.Limit() == 0 ||
      statement.Limit() > 4096 || statement.Offset() > 1024 ||
      statement.OrderBy().empty() || !statement.OrderBy()[0].ascending ||
      StatementContainsVolatileFunction(statement)) {
    return std::optional<Relation>{};
  }
  const Expression& ord0 = statement.OrderBy()[0].expression;
  if (!ord0 || ord0->Type() != TypeTag::kColumnValue) {
    return std::optional<Relation>{};
  }

  auto check_windows_partitioned_by =
      [](const Expression& root, std::string_view key_name,
         std::string_view src_alias, bool allow_agg_outside_win) -> bool {
    if (!root || ContainsParameterSlot(root)) {
      return !root;
    }
    auto walk = [&](auto& self, const Expression& e, bool inside_win) -> bool {
      if (!e) {
        return true;
      }
      if (e->Type() == TypeTag::kQueryExp) {
        return false;
      }
      if (e->Type() == TypeTag::kAggregateExp && !inside_win &&
          !allow_agg_outside_win) {
        return false;
      }
      if (e->Type() == TypeTag::kWindowFunctionExp) {
        const auto& wf = e->AsWindowFunctionCallExpression();
        const bool has_part =
            std::ranges::any_of(wf.partition_by, [&](const Expression& pe) {
              if (!pe || pe->Type() != TypeTag::kColumnValue) {
                return false;
              }
              const ColumnName& cn = pe->AsColumnValue().GetColumnName();
              return IdentifierEquals(cn.name, key_name) &&
                     (cn.schema.empty() || src_alias.empty() ||
                      IdentifierEquals(cn.schema, src_alias));
            });
        if (!has_part) {
          return false;
        }
        for (const Expression& ch : ExpressionChildren(e)) {
          if (!self(self, ch, true)) {
            return false;
          }
        }
        return true;
      }
      for (const Expression& ch : ExpressionChildren(e)) {
        if (!self(self, ch, inside_win)) {
          return false;
        }
      }
      return true;
    };
    return walk(walk, root, false);
  };

  // Walk down single-source derived wrappers (0..3 hops) to the 2-source join.
  std::string cur_key = ord0->AsColumnValue().GetColumnName().name;
  if (cur_key.empty() || cur_key == "*") {
    return std::optional<Relation>{};
  }
  const SelectStatement* cur = &statement;
  size_t spine_hops = 0;
  while (cur != nullptr && cur->Sources().size() == 1 && spine_hops < 4) {
    if (!cur->WithQueries().empty() || !cur->UnionAll().empty() ||
        cur->GetSetOperationTree() != nullptr || !cur->GroupBy().empty() ||
        cur->IsGroupAll() || cur->Having() || cur->Distinct() ||
        cur->HasDistinctOn() ||
        (cur != &statement &&
         (cur->HasLimit() || cur->Offset() != 0 || cur->WithTies())) ||
        ContainsQuery(cur->WhereClause()) ||
        ContainsAggregate(cur->WhereClause()) ||
        ContainsParameterSlot(cur->WhereClause()) ||
        StatementContainsVolatileFunction(*cur)) {
      return std::optional<Relation>{};
    }
    const SelectSource& s0 = cur->Sources()[0];
    if (!s0.query || s0.unnest || s0.is_lateral || s0.sample ||
        !s0.using_columns.empty() || s0.from_nested_join || s0.join_condition) {
      return std::optional<Relation>{};
    }
    const std::string sub_alias = s0.alias;
    bool is_star =
        cur->SelectList().size() == 1 && cur->SelectList()[0].expression &&
        cur->SelectList()[0].expression->Type() == TypeTag::kColumnValue &&
        cur->SelectList()[0].expression->AsColumnValue().GetColumnName().name ==
            "*" &&
        cur->SelectList()[0].star_modifiers == nullptr;
    std::string next_key;
    if (is_star) {
      next_key = cur_key;
    } else {
      for (size_t idx = 0; idx < cur->SelectList().size(); ++idx) {
        const NamedExpression& it = cur->SelectList()[idx];
        if (!it.expression || it.star_modifiers != nullptr) {
          return std::optional<Relation>{};
        }
        if (IdentifierEquals(ProjectionName(it, idx), cur_key)) {
          if (it.expression->Type() != TypeTag::kColumnValue) {
            return std::optional<Relation>{};
          }
          const ColumnName& cn = it.expression->AsColumnValue().GetColumnName();
          if (!cn.schema.empty() && !sub_alias.empty() &&
              !IdentifierEquals(cn.schema, sub_alias)) {
            return std::optional<Relation>{};
          }
          next_key = cn.name;
        }
      }
      if (next_key.empty()) {
        return std::optional<Relation>{};
      }
    }
    for (const NamedExpression& it : cur->SelectList()) {
      if (!check_windows_partitioned_by(it.expression, next_key, sub_alias,
                                        false)) {
        return std::optional<Relation>{};
      }
    }
    if (!check_windows_partitioned_by(cur->Qualify(), next_key, sub_alias,
                                      false) ||
        !check_windows_partitioned_by(cur->WhereClause(), next_key, sub_alias,
                                      false)) {
      return std::optional<Relation>{};
    }
    cur_key = std::move(next_key);
    cur = s0.query.get();
    ++spine_hops;
  }

  if (cur == nullptr || cur->Sources().size() != 2 ||
      !cur->WithQueries().empty() || !cur->UnionAll().empty() ||
      cur->GetSetOperationTree() != nullptr || !cur->GroupBy().empty() ||
      cur->IsGroupAll() || cur->Having() || cur->Distinct() ||
      cur->HasDistinctOn() || cur->HasLimit() || cur->Offset() != 0 ||
      cur->Qualify() || HasWindowFunctions(*cur) ||
      ContainsQuery(cur->WhereClause()) ||
      ContainsParameterSlot(cur->WhereClause()) ||
      StatementContainsVolatileFunction(*cur)) {
    return std::optional<Relation>{};
  }
  const SelectSource& js0 = cur->Sources()[0];
  const SelectSource& js1 = cur->Sources()[1];
  if (!js0.query || !js1.query || js0.unnest || js1.unnest || js0.is_lateral ||
      js1.is_lateral || js0.sample || js1.sample ||
      !js0.using_columns.empty() || !js1.using_columns.empty() ||
      js0.from_nested_join || js1.from_nested_join || js0.alias.empty() ||
      js1.alias.empty() || js0.join_condition ||
      (js1.join_type != JoinType::kFull && js1.join_type != JoinType::kInner &&
       js1.join_type != JoinType::kLeft && js1.join_type != JoinType::kRight)) {
    return std::optional<Relation>{};
  }
  for (const NamedExpression& it : cur->SelectList()) {
    if (!it.expression || it.star_modifiers != nullptr ||
        ContainsQuery(it.expression) || ContainsAggregate(it.expression) ||
        ContainsParameterSlot(it.expression)) {
      return std::optional<Relation>{};
    }
  }

  // Extract `(k0_out, k1_out)` from the join's projected partition key
  // `cur_key` (e.g. `CASE WHEN NOT web.item_sk IS NULL THEN web.item_sk ELSE
  // store.item_sk END` or `COALESCE(web.item_sk, store.item_sk)`).
  Expression key_proj;
  for (size_t idx = 0; idx < cur->SelectList().size(); ++idx) {
    if (IdentifierEquals(ProjectionName(cur->SelectList()[idx], idx),
                         cur_key)) {
      key_proj = cur->SelectList()[idx].expression;
      break;
    }
  }
  if (!key_proj) {
    return std::optional<Relation>{};
  }
  auto match_side_col = [&](const Expression& e, std::string_view want_alias,
                            std::string* col_out) -> bool {
    if (!e || e->Type() != TypeTag::kColumnValue) {
      return false;
    }
    const ColumnName& cn = e->AsColumnValue().GetColumnName();
    if (!IdentifierEquals(cn.schema, want_alias) || cn.name.empty() ||
        cn.name == "*") {
      return false;
    }
    *col_out = cn.name;
    return true;
  };
  std::string k0_out;
  std::string k1_out;
  if (key_proj->Type() == TypeTag::kCaseExp) {
    const auto& ce = key_proj->AsCaseExpression();
    if (ce.when_clauses_.size() == 1 && ce.else_clause_ &&
        ce.when_clauses_[0].first &&
        ce.when_clauses_[0].first->Type() == TypeTag::kUnaryExp) {
      const auto& ue = ce.when_clauses_[0].first->AsUnaryExpression();
      std::string cond_c;
      std::string then_c0;
      std::string then_c1;
      std::string else_c0;
      std::string else_c1;
      if (ue.Op() == UnaryOperation::kIsNotNull) {
        if (match_side_col(ue.Child(), js0.alias, &cond_c) &&
            match_side_col(ce.when_clauses_[0].second, js0.alias, &then_c0) &&
            IdentifierEquals(cond_c, then_c0) &&
            match_side_col(ce.else_clause_, js1.alias, &else_c1)) {
          k0_out = then_c0;
          k1_out = else_c1;
        } else if (match_side_col(ue.Child(), js1.alias, &cond_c) &&
                   match_side_col(ce.when_clauses_[0].second, js1.alias,
                                  &then_c1) &&
                   IdentifierEquals(cond_c, then_c1) &&
                   match_side_col(ce.else_clause_, js0.alias, &else_c0)) {
          k0_out = else_c0;
          k1_out = then_c1;
        }
      } else if (ue.Op() == UnaryOperation::kIsNull) {
        if (match_side_col(ue.Child(), js0.alias, &cond_c) &&
            match_side_col(ce.when_clauses_[0].second, js1.alias, &then_c1) &&
            match_side_col(ce.else_clause_, js0.alias, &else_c0) &&
            IdentifierEquals(cond_c, else_c0)) {
          k0_out = else_c0;
          k1_out = then_c1;
        }
      }
    }
  } else if (key_proj->Type() == TypeTag::kFunctionCallExp &&
             IdentifierEquals(key_proj->AsFunctionCallExpression().FuncName(),
                              "coalesce") &&
             key_proj->AsFunctionCallExpression().Args().size() == 2) {
    const auto& args = key_proj->AsFunctionCallExpression().Args();
    if (!match_side_col(args[0], js0.alias, &k0_out) ||
        !match_side_col(args[1], js1.alias, &k1_out)) {
      k0_out.clear();
      k1_out.clear();
    }
  } else if ((js1.join_type == JoinType::kInner ||
              js1.join_type == JoinType::kLeft) &&
             match_side_col(key_proj, js0.alias, &k0_out)) {
    // k1_out inferred from ON equality below.
  }
  if (k0_out.empty()) {
    return std::optional<Relation>{};
  }

  // Verify ON condition contains `js0.k0_out = js1.k1_out`.
  bool found_key_eq = false;
  for (const Expression& jc : SplitConjuncts(js1.join_condition)) {
    if (!jc || jc->Type() != TypeTag::kBinaryExp ||
        jc->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
      continue;
    }
    const auto& bin = jc->AsBinaryExpression();
    std::string c0;
    std::string c1;
    if ((match_side_col(bin.Left(), js0.alias, &c0) &&
         match_side_col(bin.Right(), js1.alias, &c1)) ||
        (match_side_col(bin.Right(), js0.alias, &c0) &&
         match_side_col(bin.Left(), js1.alias, &c1))) {
      if (IdentifierEquals(c0, k0_out) &&
          (k1_out.empty() || IdentifierEquals(c1, k1_out))) {
        k1_out = c1;
        found_key_eq = true;
        break;
      }
    }
  }
  if (!found_key_eq || k1_out.empty()) {
    return std::optional<Relation>{};
  }

  struct BranchBandTarget {
    ColumnName base_col;
    bool guaranteed_non_null{false};
    int64_t min_int{1};
    uint64_t est_rows{0};
  };
  auto inspect_grouped_branch =
      [&](const SelectStatement& qb,
          std::string_view want_out) -> std::optional<BranchBandTarget> {
    if (!qb.WithQueries().empty() || !qb.UnionAll().empty() ||
        qb.GetSetOperationTree() != nullptr || qb.GroupBy().empty() ||
        qb.IsGroupAll() || qb.Distinct() || qb.HasDistinctOn() ||
        qb.HasLimit() || qb.Offset() != 0 || qb.Qualify() || qb.Having() ||
        ContainsQuery(qb.WhereClause()) ||
        ContainsParameterSlot(qb.WhereClause()) ||
        StatementContainsVolatileFunction(qb)) {
      return std::nullopt;
    }
    ColumnName raw_col;
    bool found_out = false;
    for (size_t idx = 0; idx < qb.SelectList().size(); ++idx) {
      const NamedExpression& it = qb.SelectList()[idx];
      if (!it.expression || it.star_modifiers != nullptr) {
        return std::nullopt;
      }
      if (IdentifierEquals(ProjectionName(it, idx), want_out)) {
        if (it.expression->Type() != TypeTag::kColumnValue) {
          return std::nullopt;
        }
        raw_col = it.expression->AsColumnValue().GetColumnName();
        found_out = true;
      }
    }
    if (!found_out || raw_col.name.empty() || raw_col.name == "*") {
      return std::nullopt;
    }
    const bool in_gby =
        std::ranges::any_of(qb.GroupBy(), [&](const Expression& gk) {
          if (!gk || gk->Type() != TypeTag::kColumnValue) {
            return false;
          }
          const ColumnName& gc = gk->AsColumnValue().GetColumnName();
          return IdentifierEquals(gc.name, raw_col.name) &&
                 (gc.schema.empty() || raw_col.schema.empty() ||
                  IdentifierEquals(gc.schema, raw_col.schema));
        });
    if (!in_gby) {
      return std::nullopt;
    }
    size_t hits = 0;
    BranchBandTarget target;
    for (const SelectSource& bs : qb.Sources()) {
      if (bs.table.empty() || bs.query != nullptr || bs.unnest ||
          bs.is_lateral || bs.sample || !bs.using_columns.empty() ||
          bs.from_nested_join || inherited_ctes.contains(bs.table)) {
        return std::nullopt;
      }
      auto tbl_or = context.GetTable(bs.table);
      if (!tbl_or.HasValue() || tbl_or.Value() == nullptr) {
        return std::nullopt;
      }
      const std::shared_ptr<Table>& tbl = tbl_or.Value();
      const std::string balias = bs.alias.empty() ? bs.table : bs.alias;
      if (!raw_col.schema.empty() &&
          !IdentifierEquals(raw_col.schema, balias) &&
          !IdentifierEquals(raw_col.schema, bs.table)) {
        continue;
      }
      const Schema& bschema = tbl->GetSchema();
      const int off = bschema.Offset(ColumnName("", raw_col.name));
      if (off < 0) {
        continue;
      }
      if (bs.join_type != JoinType::kCross &&
          bs.join_type != JoinType::kInner) {
        return std::nullopt;
      }
      const auto slot = static_cast<slot_t>(off);
      const Column& cm = bschema.GetColumn(slot);
      if (cm.Type() != ValueType::kInt64 || cm.IsUnsigned()) {
        return std::nullopt;
      }
      ++hits;
      target.base_col = ColumnName(balias, cm.Name().name);
      target.est_rows = tbl->BackingColumnarRowCount();
      if (auto st = context.GetStats(bs.table); st.HasValue()) {
        target.est_rows =
            std::max<uint64_t>(target.est_rows, st.Value()->Rows());
      }
      const uint64_t gen = tbl->LayoutGeneration();
      const page_id_t first_pid = tbl->FirstPageId();
      if (const auto lst = tbl->GetLSTableStore();
          lst != nullptr && lst->uncommitted_owner_txn == 0 &&
          lst->layout_generation == gen && lst->first_pid == first_pid) {
        if (slot < lst->column_min_int.size() && lst->total_rows > 0) {
          target.min_int = lst->column_min_int[slot];
        }
        if (slot < lst->column_has_null.size() &&
            lst->column_has_null[slot] == 0 &&
            !context.txn_.HasAnyPageVersionChains() &&
            (context.txn_.IsReadOnly() || context.txn_.WriteSetEmpty())) {
          target.guaranteed_non_null = true;
        }
      } else if (const auto tlcs = tbl->GetBackingTlcsStore();
                 gen != 0 && !tbl->RowsRewrittenInPlace() && tlcs != nullptr &&
                 tlcs->LayoutGeneration() == gen &&
                 tlcs->FirstPageId() == first_pid && tlcs->TotalRows() > 0 &&
                 tlcs->HasColumnSynopses() &&
                 slot < tlcs->ColumnSynopses().size()) {
        const auto& syn = tlcs->ColumnSynopses()[slot];
        if (syn.min_int <= syn.max_int) {
          target.min_int = syn.min_int;
        }
        if (!syn.has_null && !context.txn_.HasAnyPageVersionChains() &&
            (context.txn_.IsReadOnly() || context.txn_.WriteSetEmpty())) {
          target.guaranteed_non_null = true;
        }
      }
    }
    if (hits != 1) {
      return std::nullopt;
    }
    for (const Expression& wconj : SplitConjuncts(qb.WhereClause())) {
      if (wconj && wconj->Type() == TypeTag::kUnaryExp &&
          wconj->AsUnaryExpression().Op() == UnaryOperation::kIsNotNull &&
          wconj->AsUnaryExpression().Child() &&
          wconj->AsUnaryExpression().Child()->Type() == TypeTag::kColumnValue) {
        const ColumnName& wc =
            wconj->AsUnaryExpression().Child()->AsColumnValue().GetColumnName();
        if (IdentifierEquals(wc.name, target.base_col.name) &&
            (wc.schema.empty() ||
             IdentifierEquals(wc.schema, target.base_col.schema))) {
          target.guaranteed_non_null = true;
        }
      }
    }
    for (const NamedExpression& it : qb.SelectList()) {
      if (!check_windows_partitioned_by(it.expression, target.base_col.name,
                                        target.base_col.schema, true)) {
        return std::nullopt;
      }
    }
    return target;
  };

  const auto t0 = inspect_grouped_branch(*js0.query, k0_out);
  const auto t1 = inspect_grouped_branch(*js1.query, k1_out);
  if (!t0.has_value() || !t1.has_value() ||
      (t0->est_rows + t1->est_rows) < 100000) {
    return std::optional<Relation>{};
  }

  const int64_t lo_bound = std::min(t0->min_int, t1->min_int);
  const int64_t need_rows =  // NOLINT(modernize-use-auto)
      static_cast<int64_t>(statement.Limit() + statement.Offset());
  const std::array<int64_t, 2> band_widths{
      std::max<int64_t>(384, need_rows * 4),
      std::max<int64_t>(4096, need_rows * 32)};
  const bool nulls_in_prefix =
      statement.OrderBy()[0].nulls_first.value_or(true);

  struct BandDepthGuard {  // NOLINT(cppcoreguidelines-special-member-functions)
    int* depth;
    explicit BandDepthGuard(int* d) : depth(d) { ++(*depth); }
    ~BandDepthGuard() { --(*depth); }
  } depth_guard(&band_depth);

  auto make_band_pred = [&](const BranchBandTarget& tgt, int64_t hi) {
    Expression le =
        BinaryExpressionExp(std::make_shared<ColumnValue>(tgt.base_col),
                            BinaryOperation::kLessThanEquals,
                            std::make_shared<ConstantValue>(Value(hi)));
    if (!nulls_in_prefix || tgt.guaranteed_non_null) {
      return le;
    }
    return BinaryExpressionExp(std::make_shared<UnaryExpression>(
                                   std::make_shared<ColumnValue>(tgt.base_col),
                                   UnaryOperation::kIsNull),
                               BinaryOperation::kOr, std::move(le));
  };

  for (int64_t width : band_widths) {
    int64_t hi_bound = 0;
    if (__builtin_add_overflow(lo_bound, width, &hi_bound)) {
      break;
    }
    SelectStatement banded_root(statement);
    SelectStatement* edit = &banded_root;
    for (size_t h = 0; h < spine_hops; ++h) {
      std::vector<SelectSource> srcs = edit->Sources();
      srcs[0].query = std::make_shared<SelectStatement>(*srcs[0].query);
      SelectStatement* next_edit = srcs[0].query.get();
      edit->SetSources(std::move(srcs));
      edit = next_edit;
    }
    std::vector<SelectSource> jsrcs = edit->Sources();
    auto q0_copy = std::make_shared<SelectStatement>(*jsrcs[0].query);
    auto q1_copy = std::make_shared<SelectStatement>(*jsrcs[1].query);
    Expression p0 = make_band_pred(*t0, hi_bound);
    Expression p1 = make_band_pred(*t1, hi_bound);
    q0_copy->SetWhereClause(q0_copy->WhereClause()
                                ? BinaryExpressionExp(q0_copy->WhereClause(),
                                                      BinaryOperation::kAnd,
                                                      std::move(p0))
                                : std::move(p0));
    q1_copy->SetWhereClause(q1_copy->WhereClause()
                                ? BinaryExpressionExp(q1_copy->WhereClause(),
                                                      BinaryOperation::kAnd,
                                                      std::move(p1))
                                : std::move(p1));
    jsrcs[0].query = std::move(q0_copy);
    jsrcs[1].query = std::move(q1_copy);
    edit->SetSources(std::move(jsrcs));

    ASSIGN_OR_RETURN(Relation, cand_rel,
                     ExecuteQuery(context, banded_root, nullptr, inherited_ctes,
                                  nullptr, columnar_output));
    RETURN_IF_FAIL(cand_rel.GetStatus());
    if (!cand_rel.HasSpill() && cand_rel.TotalRows() >= statement.Limit()) {
      if (!columnar_output && cand_rel.HasColumnarOnly()) {
        RETURN_IF_FAIL(cand_rel.EnsureRowsMaterialized());
      }
      return std::optional<Relation>(std::move(cand_rel));
    }
  }
  return std::optional<Relation>{};
}

// Lean proof:
// lean/TinylambOpt/ResearchProofs/SemijoinFiltersAndGroupThresholds.lean
//   (LexicographicDimPrefixTopK_sound)
// When a top-level ORDER BY <dim_col>, ... LIMIT K joins a small base
// dimension table D (e.g. `store`, <= 50k rows) against a derived-table
// pipeline whose windows/aggregates are partitioned/grouped by the equijoin
// key (e.g. TPC-DS Q65 `s_store_sk = sc.ss_store_sk` with `ORDER BY
// s_store_name, i_item_desc LIMIT 100`), scan D first, group its non-NULL
// INT64 join keys by distinct <dim_col> in exact ORDER BY order, and evaluate
// only a leading prefix of groups by pushing `IN (prefix_pks)` into both D and
// the derived pipeline.  Once the prefix yields >= LIMIT + OFFSET rows, the
// remaining dimension groups cannot affect the Top-K output.
StatusOr<std::optional<Relation>> TryExecuteLexicographicDimPrefixTopK(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& inherited_ctes, bool columnar_output) {
  static const bool disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_LEX_DIM_PREFIX_TOPK") !=
      nullptr;
  thread_local int lex_prefix_depth = 0;
  if (disabled || lex_prefix_depth > 0) {
    return std::optional<Relation>{};
  }
  if (!statement.WithQueries().empty() || !statement.UnionAll().empty() ||
      statement.GetSetOperationTree() != nullptr ||
      !statement.GroupBy().empty() || statement.IsGroupAll() ||
      statement.Having() || statement.Qualify() || statement.Distinct() ||
      statement.HasDistinctOn() || statement.WithTies() ||
      statement.AsStruct() || statement.ForUpdate() || !statement.HasLimit() ||
      statement.Limit() == 0 || statement.Limit() > 4096 ||
      statement.Offset() > 1024 || statement.OrderBy().empty() ||
      statement.Sources().size() < 2 || HasWindowFunctions(statement) ||
      StatementContainsVolatileFunction(statement) ||
      ContainsQuery(statement.WhereClause()) ||
      ContainsParameterSlot(statement.WhereClause())) {
    return std::optional<Relation>{};
  }
  const auto& ord0_term = statement.OrderBy()[0];
  if (!ord0_term.expression ||
      ord0_term.expression->Type() != TypeTag::kColumnValue) {
    return std::optional<Relation>{};
  }
  const ColumnName& ord0_cn =
      ord0_term.expression->AsColumnValue().GetColumnName();
  if (ord0_cn.name.empty() || ord0_cn.name == "*") {
    return std::optional<Relation>{};
  }

  // Validate outer sources: all inner/cross, no lateral/unnest/sample/USING.
  const auto& sources = statement.Sources();
  for (size_t i = 0; i < sources.size(); ++i) {
    const SelectSource& src = sources[i];
    if (src.unnest != nullptr || src.is_lateral || src.sample.has_value() ||
        !src.using_columns.empty() || src.from_nested_join ||
        (i > 0 && src.join_type != JoinType::kInner &&
         src.join_type != JoinType::kCross)) {
      return std::optional<Relation>{};
    }
  }

  // Locate the base dimension source owning `ord0_cn`.
  std::optional<size_t> dim_idx;
  std::shared_ptr<Table> dim_table;
  slot_t ord0_slot = 0;
  for (size_t i = 0; i < sources.size(); ++i) {
    const SelectSource& src = sources[i];
    if (src.table.empty() || src.query != nullptr ||
        inherited_ctes.contains(AsciiToLower(src.table))) {
      continue;
    }
    const std::string alias = src.alias.empty() ? src.table : src.alias;
    if (!ord0_cn.schema.empty() && !IdentifierEquals(ord0_cn.schema, alias) &&
        !IdentifierEquals(ord0_cn.schema, src.table)) {
      continue;
    }
    auto tbl_or = context.GetTable(src.table);
    if (!tbl_or.HasValue() || !tbl_or.Value()) {
      continue;
    }
    const Schema& sch = tbl_or.Value()->GetSchema();
    const int off = sch.Offset(ColumnName("", ord0_cn.name));
    if (off < 0) {
      continue;
    }
    if (dim_idx.has_value()) {
      // Ambiguous column name across multiple base tables.
      return std::optional<Relation>{};
    }
    dim_idx = i;
    dim_table = tbl_or.Value();
    ord0_slot = static_cast<slot_t>(off);
  }
  if (!dim_idx || !dim_table) {
    return std::optional<Relation>{};
  }
  constexpr uint64_t kMaxSmallDimRows = 50000;
  if (dim_table->BackingColumnarRowCount() > kMaxSmallDimRows) {
    return std::optional<Relation>{};
  }
  const Schema& dim_schema = dim_table->GetSchema();
  const Column& ord0_col = dim_schema.GetColumn(ord0_slot);
  if ((ord0_col.Type() != ValueType::kVarChar &&
       ord0_col.Type() != ValueType::kInt64 &&
       ord0_col.Type() != ValueType::kDate) ||
      ord0_col.IsUnsigned()) {
    return std::optional<Relation>{};
  }

  // Collect all outer conjuncts (WHERE + INNER JOIN ON) and find an equijoin
  // `dim_pk = drv_key` linking `dim_idx` to a derived-table source `drv_idx`.
  std::vector<Expression> all_conjuncts;
  if (statement.WhereClause()) {
    auto wc = SplitConjuncts(statement.WhereClause());
    all_conjuncts.insert(all_conjuncts.end(), wc.begin(), wc.end());
  }
  for (const SelectSource& src : sources) {
    if (src.join_condition) {
      auto jc = SplitConjuncts(src.join_condition);
      all_conjuncts.insert(all_conjuncts.end(), jc.begin(), jc.end());
    }
  }

  const std::string dim_alias = sources[*dim_idx].alias.empty()
                                    ? sources[*dim_idx].table
                                    : sources[*dim_idx].alias;
  auto is_dim_col = [&](const ColumnName& cn) -> std::optional<slot_t> {
    if (!cn.schema.empty() && !IdentifierEquals(cn.schema, dim_alias) &&
        !IdentifierEquals(cn.schema, sources[*dim_idx].table)) {
      return std::nullopt;
    }
    const int off = dim_schema.Offset(ColumnName("", cn.name));
    if (off < 0) {
      return std::nullopt;
    }
    return static_cast<slot_t>(off);
  };

  auto find_derived_col = [&](const ColumnName& cn)
      -> std::optional<std::pair<size_t, std::string>> {
    std::optional<std::pair<size_t, std::string>> found;
    for (size_t i = 0; i < sources.size(); ++i) {
      if (i == *dim_idx || !sources[i].query) {
        continue;
      }
      if (!cn.schema.empty() &&
          !IdentifierEquals(cn.schema, sources[i].alias)) {
        continue;
      }
      const SelectStatement& q = *sources[i].query;
      for (size_t oi = 0; oi < q.SelectList().size(); ++oi) {
        const std::string out_nm = ProjectionName(q.SelectList()[oi], oi);
        if (IdentifierEquals(out_nm, cn.name)) {
          if (found.has_value()) {
            return std::nullopt;
          }
          found = {i, out_nm};
          break;
        }
      }
    }
    return found;
  };

  std::optional<size_t> drv_idx;
  Expression dim_pk_expr;
  std::string drv_key_out_name;
  for (const Expression& conj : all_conjuncts) {
    if (!conj || conj->Type() != TypeTag::kBinaryExp) {
      continue;
    }
    const auto& bin = conj->AsBinaryExpression();
    if (bin.Op() != BinaryOperation::kEquals || !bin.Left() || !bin.Right() ||
        bin.Left()->Type() != TypeTag::kColumnValue ||
        bin.Right()->Type() != TypeTag::kColumnValue) {
      continue;
    }
    const ColumnName& lcn = bin.Left()->AsColumnValue().GetColumnName();
    const ColumnName& rcn = bin.Right()->AsColumnValue().GetColumnName();
    auto ldim = is_dim_col(lcn);
    auto rdim = is_dim_col(rcn);
    if (ldim && !rdim) {
      if (auto rdrv = find_derived_col(rcn)) {
        const Column& pk_col = dim_schema.GetColumn(*ldim);
        if (pk_col.Type() == ValueType::kInt64 && !pk_col.IsUnsigned()) {
          dim_pk_expr = bin.Left();
          drv_idx = rdrv->first;
          drv_key_out_name = rdrv->second;
          break;
        }
      }
    } else if (rdim && !ldim) {
      if (auto ldrv = find_derived_col(lcn)) {
        const Column& pk_col = dim_schema.GetColumn(*rdim);
        if (pk_col.Type() == ValueType::kInt64 && !pk_col.IsUnsigned()) {
          dim_pk_expr = bin.Right();
          drv_idx = ldrv->first;
          drv_key_out_name = ldrv->second;
          break;
        }
      }
    }
  }
  if (!drv_idx || !dim_pk_expr) {
    return std::optional<Relation>{};
  }

  // Walk down `sources[*drv_idx].query` (0..3 single-source wrapper hops) to
  // the deepest grouped/base query, verifying every window is partitioned by
  // the tracked key and every GROUP BY contains the tracked key.
  auto check_windows_partitioned = [](const Expression& root,
                                      std::string_view key_nm) -> bool {
    if (!root) {
      return true;
    }
    auto walk = [&](auto& self, const Expression& e) -> bool {
      if (!e) {
        return true;
      }
      if (e->Type() == TypeTag::kQueryExp) {
        return false;
      }
      if (e->Type() == TypeTag::kWindowFunctionExp) {
        const auto& wf = e->AsWindowFunctionCallExpression();
        const bool has_part =
            std::ranges::any_of(wf.partition_by, [&](const Expression& pe) {
              return pe && pe->Type() == TypeTag::kColumnValue &&
                     IdentifierEquals(pe->AsColumnValue().GetColumnName().name,
                                      key_nm);
            });
        if (!has_part) {
          return false;
        }
      }
      for (const Expression& ch : ExpressionChildren(e)) {
        if (!self(self, ch)) {
          return false;
        }
      }
      return true;
    };
    return walk(walk, root);
  };

  size_t drv_hops = 0;
  std::string tracked_key = drv_key_out_name;
  const SelectStatement* cur = sources[*drv_idx].query.get();
  ColumnName deepest_key_col;
  while (cur != nullptr && drv_hops <= 3) {
    if (!cur->WithQueries().empty() || !cur->UnionAll().empty() ||
        cur->GetSetOperationTree() != nullptr || cur->IsGroupAll() ||
        cur->Having() || cur->HasLimit() || cur->Offset() != 0 ||
        cur->WithTies() || cur->Distinct() || cur->HasDistinctOn() ||
        StatementContainsVolatileFunction(*cur) ||
        ContainsQuery(cur->WhereClause()) ||
        ContainsParameterSlot(cur->WhereClause())) {
      return std::optional<Relation>{};
    }
    // Map `tracked_key` through `cur->SelectList()`.
    std::optional<ColumnName> mapped_cn;
    for (size_t oi = 0; oi < cur->SelectList().size(); ++oi) {
      if (IdentifierEquals(ProjectionName(cur->SelectList()[oi], oi),
                           tracked_key)) {
        const Expression& e = cur->SelectList()[oi].expression;
        if (!e || e->Type() != TypeTag::kColumnValue) {
          return std::optional<Relation>{};
        }
        mapped_cn = e->AsColumnValue().GetColumnName();
        break;
      }
    }
    if (!mapped_cn || mapped_cn->name.empty() || mapped_cn->name == "*") {
      return std::optional<Relation>{};
    }
    // All windows on this level must be partitioned by `mapped_cn->name`.
    for (const NamedExpression& it : cur->SelectList()) {
      if (!check_windows_partitioned(it.expression, mapped_cn->name)) {
        return std::optional<Relation>{};
      }
    }
    if (!check_windows_partitioned(cur->Qualify(), mapped_cn->name)) {
      return std::optional<Relation>{};
    }
    // If this level has GROUP BY, `mapped_cn->name` must be a GROUP BY key.
    if (!cur->GroupBy().empty()) {
      const bool in_gby =
          std::ranges::any_of(cur->GroupBy(), [&](const Expression& gk) {
            return gk && gk->Type() == TypeTag::kColumnValue &&
                   IdentifierEquals(gk->AsColumnValue().GetColumnName().name,
                                    mapped_cn->name);
          });
      if (!in_gby) {
        return std::optional<Relation>{};
      }
    }
    // If this level wraps a single derived table, descend; otherwise stop here.
    if (cur->Sources().size() == 1 && cur->Sources()[0].query != nullptr &&
        cur->Sources()[0].unnest == nullptr && !cur->Sources()[0].is_lateral &&
        !cur->Sources()[0].sample.has_value()) {
      tracked_key = mapped_cn->name;
      cur = cur->Sources()[0].query.get();
      ++drv_hops;
      continue;
    }
    // Deepest level: must be base tables (all inner/cross) and not a tiny scan.
    if (cur->Sources().empty()) {
      return std::optional<Relation>{};
    }
    bool all_base_ok = true;
    for (size_t bi = 0; bi < cur->Sources().size(); ++bi) {
      const SelectSource& bs = cur->Sources()[bi];
      if (bs.table.empty() || bs.query != nullptr || bs.unnest != nullptr ||
          bs.is_lateral || bs.sample.has_value() || !bs.using_columns.empty() ||
          inherited_ctes.contains(AsciiToLower(bs.table)) ||
          (bi > 0 && bs.join_type != JoinType::kInner &&
           bs.join_type != JoinType::kCross)) {
        all_base_ok = false;
        break;
      }
    }
    if (!all_base_ok) {
      return std::optional<Relation>{};
    }
    deepest_key_col = *mapped_cn;
    break;
  }
  if (deepest_key_col.name.empty()) {
    return std::optional<Relation>{};
  }

  // Scan the small dimension table `dim_idx` once to order and group its PKs
  // by distinct `ord0` values in exact ORDER BY order.
  SelectStatement dim_scan;
  SelectSource dim_src = sources[*dim_idx];
  dim_src.join_type = JoinType::kInner;
  dim_src.join_condition = Expression{};
  dim_scan.SetSources({std::move(dim_src)});
  dim_scan.SetSelectList({NamedExpression("__ord0", ord0_term.expression),
                          NamedExpression("__dpk", dim_pk_expr)});
  bool dim_where_applied = false;
  ASSIGN_OR_RETURN(
      Relation, dim_rel,
      (BuildInput(context, dim_scan, nullptr, inherited_ctes,
                  &dim_where_applied, nullptr, /*ignore_projection_star=*/false,
                  /*columnar_output=*/false)));
  RETURN_IF_FAIL(dim_rel.EnsureRowsMaterialized());
  if (dim_rel.HasSpill() || dim_rel.rows.empty() ||
      dim_rel.rows.size() > kMaxSmallDimRows) {
    return std::optional<Relation>{};
  }
  const auto dim_ord_off = LocalColumnOffset(dim_rel.schema, ord0_cn);
  const auto dim_pk_off = LocalColumnOffset(
      dim_rel.schema, dim_pk_expr->AsColumnValue().GetColumnName());
  if (!dim_ord_off || !dim_pk_off) {
    return std::optional<Relation>{};
  }

  struct DimEntry {
    Value ord_val;
    int64_t pk{0};
  };
  std::vector<DimEntry> entries;
  entries.reserve(dim_rel.rows.size());
  for (const Row& r : dim_rel.rows) {
    const Value& pkv = r[*dim_pk_off];
    if (pkv.IsNull() || pkv.type != ValueType::kInt64 || pkv.IsUnsigned()) {
      continue;
    }
    entries.push_back(
        DimEntry{.ord_val = r[*dim_ord_off], .pk = pkv.value.int_value});
  }
  if (entries.empty()) {
    return std::optional<Relation>{};
  }

  const bool asc = ord0_term.ascending;
  const bool nulls_first = ord0_term.nulls_first.value_or(asc);
  auto ord_less = [&](const Value& a, const Value& b) -> bool {
    if (a.IsNull() && b.IsNull()) {
      return false;
    }
    if (a.IsNull()) {
      return nulls_first;
    }
    if (b.IsNull()) {
      return !nulls_first;
    }
    if (a.type == ValueType::kVarChar && b.type == ValueType::kVarChar) {
      return asc ? (a.value.varchar_value < b.value.varchar_value)
                 : (a.value.varchar_value > b.value.varchar_value);
    }
    return asc ? (a.value.int_value < b.value.int_value)
               : (a.value.int_value > b.value.int_value);
  };
  auto ord_eq = [](const Value& a, const Value& b) -> bool {
    if (a.IsNull() && b.IsNull()) {
      return true;
    }
    if (a.IsNull() || b.IsNull()) {
      return false;
    }
    if (a.type == ValueType::kVarChar && b.type == ValueType::kVarChar) {
      return a.value.varchar_value == b.value.varchar_value;
    }
    return a.value.int_value == b.value.int_value;
  };

  std::ranges::sort(entries, [&](const DimEntry& a, const DimEntry& b) {
    if (ord_less(a.ord_val, b.ord_val)) {
      return true;
    }
    if (ord_less(b.ord_val, a.ord_val)) {
      return false;
    }
    return a.pk < b.pk;
  });

  // Group PKs by distinct `ord_val` (all ties on `ord0` stay in one group).
  std::vector<std::vector<int64_t>> pk_groups;
  std::vector<Value> group_reps;
  for (const DimEntry& e : entries) {
    if (pk_groups.empty() || !ord_eq(e.ord_val, group_reps.back())) {
      pk_groups.emplace_back();
      group_reps.push_back(e.ord_val);
    }
    if (pk_groups.back().empty() || pk_groups.back().back() != e.pk) {
      pk_groups.back().push_back(e.pk);
    }
  }
  if (pk_groups.size() <= 1) {
    return std::optional<Relation>{};
  }
  // Cost gate: only attempt speculative prefix execution when the dimension
  // table has enough rows (>= 24, e.g. SF=10 store=102 vs SF=1 store=12) that
  // the leading group yields >= LIMIT rows on the first attempt without
  // re-scanning the fact table on fallback.
  static const bool force_lex_prefix =
      ::tinylamb::platform::GetEnv("TINYLAMB_FORCE_LEX_DIM_PREFIX_TOPK") !=
      nullptr;
  if (!force_lex_prefix && entries.size() < 24) {
    return std::optional<Relation>{};
  }

  struct LexDepthGuard {
    int* depth;
    explicit LexDepthGuard(int* d) : depth(d) { ++(*depth); }
    LexDepthGuard(const LexDepthGuard&) = delete;
    LexDepthGuard& operator=(const LexDepthGuard&) = delete;
    LexDepthGuard(LexDepthGuard&&) = delete;
    LexDepthGuard& operator=(LexDepthGuard&&) = delete;
    ~LexDepthGuard() { --(*depth); }
  } depth_guard(&lex_prefix_depth);

  const size_t want_rows = statement.Limit() + statement.Offset();
  // Try only the leading primary-key group once; if it yields < want_rows,
  // fall back immediately to the full-table execution path.
  const std::array<size_t, 1> try_group_counts{1};
  size_t prev_tried = 0;
  for (size_t ng : try_group_counts) {
    if (ng >= pk_groups.size() || ng <= prev_tried) {
      break;
    }
    prev_tried = ng;
    std::vector<int64_t> prefix_pks;
    for (size_t g = 0; g < ng; ++g) {
      prefix_pks.insert(prefix_pks.end(), pk_groups[g].begin(),
                        pk_groups[g].end());
    }
    if (prefix_pks.empty() || prefix_pks.size() * 2 >= entries.size()) {
      break;
    }

    auto make_in_pred = [&](const Expression& col_expr) -> Expression {
      if (prefix_pks.size() == 1) {
        return BinaryExpressionExp(col_expr, BinaryOperation::kEquals,
                                   ConstantValueExp(Value(prefix_pks[0])));
      }
      std::vector<Expression> in_items;
      in_items.reserve(prefix_pks.size());
      for (int64_t pk : prefix_pks) {
        in_items.push_back(ConstantValueExp(Value(pk)));
      }
      return std::make_shared<InExpression>(col_expr, std::move(in_items));
    };

    // Clone statement and push `IN (prefix_pks)` to outer WHERE (on dim_pk)
    // and to the deepest query inside `sources[*drv_idx].query`.
    SelectStatement narrowed(statement);
    narrowed.SetLimit(want_rows);
    narrowed.SetOffset(0);
    {
      std::vector<Expression> outer_wc;
      if (narrowed.WhereClause()) {
        outer_wc = SplitConjuncts(narrowed.WhereClause());
      }
      outer_wc.push_back(make_in_pred(dim_pk_expr));
      narrowed.SetWhereClause(CombineConjuncts(outer_wc));
    }
    std::vector<SelectSource> new_outer_sources = narrowed.Sources();
    auto cloned_root =
        std::make_shared<SelectStatement>(*new_outer_sources[*drv_idx].query);
    SelectStatement* edit = cloned_root.get();
    for (size_t h = 0; h < drv_hops; ++h) {
      std::vector<SelectSource> sub_srcs = edit->Sources();
      auto cloned_child = std::make_shared<SelectStatement>(*sub_srcs[0].query);
      sub_srcs[0].query = cloned_child;
      edit->SetSources(std::move(sub_srcs));
      edit = cloned_child.get();
    }
    {
      std::vector<Expression> inner_wc;
      if (edit->WhereClause()) {
        inner_wc = SplitConjuncts(edit->WhereClause());
      }
      inner_wc.push_back(make_in_pred(ColumnValueExp(deepest_key_col)));
      edit->SetWhereClause(CombineConjuncts(inner_wc));
    }
    new_outer_sources[*drv_idx].query = std::move(cloned_root);
    narrowed.SetSources(std::move(new_outer_sources));

    ASSIGN_OR_RETURN(Relation, cand_rel,
                     ExecuteQuery(context, narrowed, nullptr, inherited_ctes,
                                  nullptr, columnar_output));
    RETURN_IF_FAIL(cand_rel.GetStatus());
    if (!cand_rel.HasSpill() && cand_rel.TotalRows() >= want_rows) {
      if (statement.Offset() > 0) {
        RETURN_IF_FAIL(cand_rel.EnsureRowsMaterialized());
        if (cand_rel.rows.size() > statement.Offset()) {
          cand_rel.rows.erase(
              cand_rel.rows.begin(),
              cand_rel.rows.begin() +
                  static_cast<std::ptrdiff_t>(statement.Offset()));
          if (cand_rel.rows.size() > statement.Limit()) {
            cand_rel.rows.resize(statement.Limit());
          }
        } else {
          cand_rel.rows.clear();
        }
      } else if (!columnar_output && cand_rel.HasColumnarOnly()) {
        RETURN_IF_FAIL(cand_rel.EnsureRowsMaterialized());
      }
      return std::optional<Relation>(std::move(cand_rel));
    }
  }
  return std::optional<Relation>{};
}

}  // namespace tinylamb::relational_detail
