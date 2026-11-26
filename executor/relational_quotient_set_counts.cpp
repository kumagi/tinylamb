/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/aggregation.hpp"
#include "executor/detail/columnar_tlcs_fast_path.hpp"
#include "executor/detail/explain_format.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/relational_fast_paths.hpp"
#include "executor/detail/relational_set_ops.hpp"
#include "executor/detail/relational_sort_limit.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/window_eval.hpp"
#include "executor/relational.hpp"
#include "executor/table_sample.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "plan/plan.hpp"
#include "table/full_scan_iterator.hpp"
#include "table/iterator.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "table/tlcs_file.hpp"
#include "type/column.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
// (quotient_gid_setop_cardinality_sound)
StatusOr<std::optional<Relation>> TryExecuteQuotientGidDistinctSetOpCount(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& inherited_ctes) {
  if (!statement.WithQueries().empty() || !statement.UnionAll().empty() ||
      statement.GetSetOperationTree() != nullptr || statement.WhereClause() ||
      !statement.GroupBy().empty() || statement.IsGroupAll() ||
      statement.Having() || statement.Qualify() || statement.Distinct() ||
      !statement.OrderBy().empty() || statement.Offset() != 0 ||
      (statement.HasLimit() && statement.Limit() == 0) ||
      statement.AsStruct() || statement.ForUpdate() ||
      HasWindowFunctions(statement) || statement.SelectList().size() != 1 ||
      statement.Sources().size() != 1) {
    return std::optional<Relation>{};
  }
  const Expression& top_expr = statement.SelectList()[0].expression;
  if (!top_expr || top_expr->Type() != TypeTag::kAggregateExp) {
    return std::optional<Relation>{};
  }
  const AggregateExpression& top_agg = top_expr->AsAggregateExpression();
  if (!IsCountStar(top_agg) || top_agg.Distinct() || top_agg.WhereFilter() ||
      top_agg.Having() != AggregateHavingModifier::kNone ||
      !top_agg.InnerOrderBy().empty() || top_agg.InnerLimit().has_value()) {
    return std::optional<Relation>{};
  }
  const SelectSource& top_src = statement.Sources()[0];
  if (!top_src.query || top_src.unnest || top_src.sample ||
      top_src.is_lateral || top_src.from_nested_join ||
      !top_src.using_columns.empty() || top_src.join_condition) {
    return std::optional<Relation>{};
  }
  const SelectStatement& derived = *top_src.query;
  if (!derived.WithQueries().empty() || derived.HasLimit() ||
      derived.Offset() != 0 || derived.WithTies() ||
      StatementContainsVolatileFunction(derived)) {
    return std::optional<Relation>{};
  }

  SelectStatement head_copy(derived);
  std::vector<const SelectStatement*> branches;
  std::vector<SetOperationKind> ops;
  if (derived.GetSetOperationTree() != nullptr &&
      derived.GetSetOperationTree()->grouped) {
    const SetOperationTree& tree = *derived.GetSetOperationTree();
    if (!tree.first || tree.branches.empty() ||
        tree.kinds.size() != tree.branches.size()) {
      return std::optional<Relation>{};
    }
    for (const auto& m : derived.Matches()) {
      if (m.by_name || m.corresponding || !m.columns.empty()) {
        return std::optional<Relation>{};
      }
    }
    branches.push_back(tree.first.get());
    for (const auto& b : tree.branches) {
      if (!b) {
        return std::optional<Relation>{};
      }
      branches.push_back(b.get());
    }
    ops = tree.kinds;
  } else if (!derived.UnionAll().empty()) {
    if (derived.SetOperationKinds().size() != derived.UnionAll().size()) {
      return std::optional<Relation>{};
    }
    for (size_t i = 0; i < derived.UnionAll().size(); ++i) {
      const SetOperationMatch m = derived.MatchFor(i);
      if (m.by_name || m.corresponding || !m.columns.empty() ||
          !derived.UnionAll()[i]) {
        return std::optional<Relation>{};
      }
    }
    head_copy.ClearUnionAll();
    head_copy.SetSetOperationTree(nullptr);
    head_copy.SetOrderBy({});
    branches.push_back(&head_copy);
    for (const auto& b : derived.UnionAll()) {
      branches.push_back(b.get());
    }
    ops = derived.SetOperationKinds();
  } else {
    return std::optional<Relation>{};
  }

  if (branches.size() < 2 || branches.size() > 8 ||
      !std::ranges::all_of(ops, IsPresenceOnlySetOperation)) {
    return std::optional<Relation>{};
  }

  struct BranchPlan {
    size_t dim_src_idx{0};
    Expression fk_expr;
    Expression other_expr;
    bool direct_subquery{false};
    const SelectStatement* subquery_ptr{nullptr};
    size_t sub_fk_slot{0};
    size_t sub_other_slot{0};
    std::vector<Expression> non_dim_where;
  };

  std::string shared_dim_table;
  std::shared_ptr<Table> shared_dim_ptr;
  size_t shared_pk_slot = 0;
  std::vector<int> pos_dim_slot;  // -1 for other_col
  std::optional<size_t> shared_other_pos;
  std::vector<BranchPlan> plans(branches.size());

  for (size_t bi = 0; bi < branches.size(); ++bi) {
    const SelectStatement& br = *branches[bi];
    if (!br.WithQueries().empty() || !br.UnionAll().empty() ||
        br.GetSetOperationTree() != nullptr || br.Having() || br.Qualify() ||
        br.HasLimit() || br.Offset() != 0 || br.WithTies() || br.AsStruct() ||
        br.ForUpdate() || HasWindowFunctions(br) ||
        StatementContainsVolatileFunction(br) ||
        ContainsQuery(br.WhereClause()) || br.Sources().size() < 2 ||
        br.SelectList().size() < 2 || br.SelectList().size() > 8) {
      return std::optional<Relation>{};
    }
    for (const NamedExpression& it : br.SelectList()) {
      if (!it.expression || ContainsAggregate(it.expression) ||
          ContainsQuery(it.expression) ||
          ContainsParameterSlot(it.expression)) {
        return std::optional<Relation>{};
      }
    }
    if (!br.GroupBy().empty()) {
      for (const Expression& g : br.GroupBy()) {
        if (!g) {
          return std::optional<Relation>{};
        }
        const bool in_sel = std::ranges::any_of(
            br.SelectList(), [&](const NamedExpression& it) {
              return it.expression &&
                     it.expression->ToString() == g->ToString();
            });
        if (!in_sel) {
          return std::optional<Relation>{};
        }
      }
    }

    std::vector<Schema> src_schemas(br.Sources().size());
    for (size_t si = 0; si < br.Sources().size(); ++si) {
      const SelectSource& src = br.Sources()[si];
      const bool inner_ok = (si == 0 || src.join_type == JoinType::kInner ||
                             src.join_type == JoinType::kCross) &&
                            !src.unnest && !src.sample && !src.is_lateral &&
                            !src.from_nested_join &&
                            src.using_columns.empty() &&
                            !ContainsQuery(src.join_condition);
      if (!inner_ok) {
        return std::optional<Relation>{};
      }
      const std::string alias = src.alias.empty() ? src.table : src.alias;
      if (src.query) {
        std::vector<Column> cols;
        for (size_t ci = 0; ci < src.query->SelectList().size(); ++ci) {
          const auto& item = src.query->SelectList()[ci];
          if (!item.expression ||
              (item.expression->Type() == TypeTag::kColumnValue &&
               item.expression->AsColumnValue().GetColumnName().name == "*")) {
            return std::optional<Relation>{};
          }
          cols.emplace_back(ProjectionName(item, ci), ValueType::kNull);
        }
        src_schemas[si] = QualifySchema(Schema("", std::move(cols)), alias);
      } else if (auto cte = inherited_ctes.find(src.table);
                 cte != inherited_ctes.end() && cte->second != nullptr) {
        src_schemas[si] = QualifySchema(cte->second->schema, alias);
      } else {
        auto tbl = context.GetTable(src.table);
        if (!tbl.HasValue()) {
          return std::optional<Relation>{};
        }
        src_schemas[si] = QualifySchema(tbl.Value()->GetSchema(), alias);
      }
    }

    auto resolve_col =
        [&](const ColumnName& cn) -> std::optional<std::pair<size_t, size_t>> {
      std::optional<std::pair<size_t, size_t>> hit;
      for (size_t si = 0; si < src_schemas.size(); ++si) {
        if (auto off = LocalColumnOffset(src_schemas[si], cn)) {
          if (hit.has_value()) {
            return std::nullopt;
          }
          hit = std::pair{si, *off};
        }
      }
      return hit;
    };

    auto touches_source = [&](const Expression& expr, size_t target_si,
                              bool* ok_out) -> bool {
      bool found = false;
      for (const ColumnName& cn : expr->TouchedColumns()) {
        auto r = resolve_col(cn);
        if (!r.has_value()) {
          *ok_out = false;
          return false;
        }
        if (r->first == target_si) {
          found = true;
        }
      }
      return found;
    };

    std::vector<Expression> all_preds = SplitConjuncts(br.WhereClause());
    for (size_t si = 1; si < br.Sources().size(); ++si) {
      if (br.Sources()[si].join_condition) {
        for (Expression& c : SplitConjuncts(br.Sources()[si].join_condition)) {
          all_preds.push_back(std::move(c));
        }
      }
    }

    bool matched_branch = false;
    for (size_t cand_si = 0; cand_si < br.Sources().size(); ++cand_si) {
      const SelectSource& dsrc = br.Sources()[cand_si];
      if (dsrc.query || inherited_ctes.contains(dsrc.table)) {
        continue;
      }
      if (bi > 0 && !IdentifierEquals(dsrc.table, shared_dim_table)) {
        continue;
      }
      auto dtbl_or = context.GetTable(dsrc.table);
      if (!dtbl_or.HasValue()) {
        continue;
      }
      const std::shared_ptr<Table>& dtbl =
          dtbl_or
              .Value();  // NOLINT(performance-unnecessary-copy-initialization)
      uint64_t est_rows = dtbl->BackingColumnarRowCount();
      if (auto st = context.GetStats(dsrc.table); st.HasValue()) {
        est_rows = std::max<uint64_t>(est_rows, st.Value()->Rows());
      }
      if (est_rows > 250000) {
        continue;
      }
      const Schema& dschema = dtbl->GetSchema();

      size_t dim_pred_count = 0;
      size_t cand_pk_slot = 0;
      Expression cand_fk_expr;
      std::vector<Expression> non_dim_preds;
      bool preds_valid = true;
      for (const Expression& pr : all_preds) {
        bool res_ok = true;
        const bool uses_dim = touches_source(pr, cand_si, &res_ok);
        if (!res_ok) {
          preds_valid = false;
          break;
        }
        if (!uses_dim) {
          non_dim_preds.push_back(pr);
          continue;
        }
        ++dim_pred_count;
        if (dim_pred_count > 1 || pr->Type() != TypeTag::kBinaryExp) {
          preds_valid = false;
          break;
        }
        const BinaryExpression& bin = pr->AsBinaryExpression();
        if (bin.Op() != BinaryOperation::kEquals || !bin.Left() ||
            !bin.Right()) {
          preds_valid = false;
          break;
        }
        bool lhs_ok = true;
        bool rhs_ok = true;
        const bool lhs_dim = touches_source(bin.Left(), cand_si, &lhs_ok);
        const bool rhs_dim = touches_source(bin.Right(), cand_si, &rhs_ok);
        if (!lhs_ok || !rhs_ok || lhs_dim == rhs_dim) {
          preds_valid = false;
          break;
        }
        const Expression& dim_side = lhs_dim ? bin.Left() : bin.Right();
        const Expression& fk_side = lhs_dim ? bin.Right() : bin.Left();
        if (dim_side->Type() != TypeTag::kColumnValue) {
          preds_valid = false;
          break;
        }
        auto pk_res = resolve_col(dim_side->AsColumnValue().GetColumnName());
        if (!pk_res.has_value() || pk_res->first != cand_si) {
          preds_valid = false;
          break;
        }
        const Column& pk_col = dschema.GetColumn(pk_res->second);
        if (pk_col.Type() != ValueType::kInt64 || pk_col.IsUnsigned()) {
          preds_valid = false;
          break;
        }
        if (bi > 0 && pk_res->second != shared_pk_slot) {
          preds_valid = false;
          break;
        }
        cand_pk_slot = pk_res->second;
        cand_fk_expr = fk_side;
      }
      if (!preds_valid || dim_pred_count != 1 || !cand_fk_expr) {
        continue;
      }

      std::vector<int> cand_pos_slots(br.SelectList().size(), -1);
      std::optional<size_t> cand_other_pos;
      Expression cand_other_expr;
      size_t dim_out_cols = 0;
      bool sel_ok = true;
      for (size_t pi = 0; pi < br.SelectList().size(); ++pi) {
        const Expression& se = br.SelectList()[pi].expression;
        bool se_res_ok = true;
        const bool uses_dim = touches_source(se, cand_si, &se_res_ok);
        if (!se_res_ok) {
          sel_ok = false;
          break;
        }
        if (uses_dim) {
          if (se->Type() != TypeTag::kColumnValue) {
            sel_ok = false;
            break;
          }
          auto d_res = resolve_col(se->AsColumnValue().GetColumnName());
          if (!d_res.has_value() || d_res->first != cand_si) {
            sel_ok = false;
            break;
          }
          const Column& dc = dschema.GetColumn(d_res->second);
          if (dc.IsInterval() || (dc.Type() != ValueType::kVarChar &&
                                  dc.Type() != ValueType::kInt64 &&
                                  dc.Type() != ValueType::kDate &&
                                  dc.Type() != ValueType::kDouble)) {
            sel_ok = false;
            break;
          }
          cand_pos_slots[pi] = static_cast<int>(d_res->second);
          ++dim_out_cols;
        } else {
          if (cand_other_pos.has_value()) {
            sel_ok = false;
            break;
          }
          cand_other_pos = pi;
          cand_other_expr = se;
        }
      }
      if (!sel_ok || dim_out_cols == 0) {
        continue;
      }
      if (bi == 0) {
        shared_dim_table = dsrc.table;
        shared_dim_ptr = dtbl;
        shared_pk_slot = cand_pk_slot;
        pos_dim_slot = cand_pos_slots;
        shared_other_pos = cand_other_pos;
      } else if (cand_pos_slots != pos_dim_slot ||
                 cand_other_pos != shared_other_pos) {
        continue;
      }

      BranchPlan bp;
      bp.dim_src_idx = cand_si;
      bp.fk_expr = cand_fk_expr;
      bp.other_expr = cand_other_expr;
      bp.non_dim_where = std::move(non_dim_preds);
      if (br.Sources().size() == 2 && bp.non_dim_where.empty()) {
        const size_t other_si = 1 - cand_si;
        const SelectSource& osrc = br.Sources()[other_si];
        if (osrc.query && bp.fk_expr->Type() == TypeTag::kColumnValue &&
            (!shared_other_pos.has_value() ||
             bp.other_expr->Type() == TypeTag::kColumnValue)) {
          auto fk_r = resolve_col(bp.fk_expr->AsColumnValue().GetColumnName());
          std::optional<std::pair<size_t, size_t>> ot_r =
              shared_other_pos.has_value()
                  ? resolve_col(bp.other_expr->AsColumnValue().GetColumnName())
                  : std::pair<size_t, size_t>{other_si, 0};
          if (fk_r.has_value() && fk_r->first == other_si && ot_r.has_value() &&
              ot_r->first == other_si) {
            bp.direct_subquery = true;
            bp.subquery_ptr = osrc.query.get();
            bp.sub_fk_slot = fk_r->second;
            bp.sub_other_slot = ot_r->second;
          }
        }
      }
      plans[bi] = std::move(bp);
      matched_branch = true;
      break;
    }
    if (!matched_branch) {
      return std::optional<Relation>{};
    }
  }

  // Scan shared dimension D once: SELECT pk_col, uniq_dim_cols... FROM D.
  const Schema& dschema = shared_dim_ptr->GetSchema();
  std::vector<size_t> uniq_dim_slots;
  for (int sl : pos_dim_slot) {
    if (sl >= 0 && std::ranges::find(uniq_dim_slots, static_cast<size_t>(sl)) ==
                       uniq_dim_slots.end()) {
      uniq_dim_slots.push_back(static_cast<size_t>(sl));
    }
  }
  SelectStatement dim_stmt;
  SelectSource dim_scan_src;
  dim_scan_src.table = shared_dim_table;
  dim_stmt.SetSources({std::move(dim_scan_src)});
  std::vector<NamedExpression> dim_select;
  dim_select.reserve(1 + uniq_dim_slots.size());
  dim_select.emplace_back(
      "__pk", std::make_shared<ColumnValue>(ColumnName(
                  "", dschema.GetColumn(shared_pk_slot).Name().name)));
  for (size_t di = 0; di < uniq_dim_slots.size(); ++di) {
    dim_select.emplace_back(
        "__d" + std::to_string(di),
        std::make_shared<ColumnValue>(
            ColumnName("", dschema.GetColumn(uniq_dim_slots[di]).Name().name)));
  }
  dim_stmt.SetSelectList(std::move(dim_select));
  ASSIGN_OR_RETURN(
      Relation, dim_rel,
      ExecuteQuery(context, dim_stmt, nullptr, inherited_ctes, nullptr, true));
  RETURN_IF_FAIL(dim_rel.GetStatus());
  if (dim_rel.HasSpill()) {
    return std::optional<Relation>{};
  }

  std::vector<uint32_t> pk_to_gid;
  GroupKeyMap tuple_to_gid;
  uint32_t next_gid = 1;
  bool pk_valid = true;

  if (dim_rel.columnar != nullptr && !dim_rel.columnar->chunks.empty()) {
    int64_t max_pk = -1;
    for (const ColumnarChunk& ch : dim_rel.columnar->chunks) {
      if (ch.columns.empty()) {
        pk_valid = false;
        break;
      }
      const ColumnarColumn& pkc = ch.columns[0];
      for (size_t r = 0; r < ch.rows; ++r) {
        if (pkc.IsNull(r)) {
          pk_valid = false;
          break;
        }
        const int64_t pk = pkc.IntAt(r);
        if (pk < 0 || pk > 1000000) {
          pk_valid = false;
          break;
        }
        max_pk = std::max(max_pk, pk);
      }
      if (!pk_valid) {
        break;
      }
    }
    if (!pk_valid) {
      return std::optional<Relation>{};
    }
    if (max_pk >= 0) {
      pk_to_gid.assign(static_cast<size_t>(max_pk) + 1, 0);
      // Fast bijective 2-col proven-distinct dictionary path across all chunks:
      const LSTableColumnDictionary* shared_d1 = nullptr;
      const LSTableColumnDictionary* shared_d2 = nullptr;
      bool fast_dict_2col =
          (!dim_rel.columnar->chunks.empty() && uniq_dim_slots.size() == 2 &&
           dim_rel.columnar->chunks[0].columns.size() == 3);
      if (fast_dict_2col) {
        shared_d1 = dim_rel.columnar->chunks[0].columns[1].dict;
        shared_d2 = dim_rel.columnar->chunks[0].columns[2].dict;
        fast_dict_2col =
            (shared_d1 != nullptr && shared_d2 != nullptr &&
             shared_d1->bytewise_equality && shared_d2->bytewise_equality);
        for (const ColumnarChunk& ch : dim_rel.columnar->chunks) {
          if (!fast_dict_2col) {
            break;
          }
          fast_dict_2col = (ch.columns.size() == 3 &&
                            ch.columns[1].encoding ==
                                ColumnarColumn::Encoding::kDictionary &&
                            ch.columns[2].encoding ==
                                ColumnarColumn::Encoding::kDictionary &&
                            ch.columns[1].dict == shared_d1 &&
                            ch.columns[2].dict == shared_d2);
        }
      }
      if (fast_dict_2col) {
        FlatIntKeyMap<uint32_t> code_pair_to_gid;
        code_pair_to_gid.Reserve(dim_rel.TotalRows());
        for (const ColumnarChunk& ch : dim_rel.columnar->chunks) {
          const ColumnarColumn& pkc = ch.columns[0];
          const ColumnarColumn& c1 = ch.columns[1];
          const ColumnarColumn& c2 = ch.columns[2];
          for (size_t r = 0; r < ch.rows; ++r) {
            const auto pk = static_cast<size_t>(pkc.IntAt(r));
            if (pk_to_gid[pk] != 0) {
              return std::optional<Relation>{};  // duplicate PK!
            }
            const uint32_t d1 = c1.IsNull(r) ? 0U : (c1.DictCodeAt(r) + 1U);
            const uint32_t d2 = c2.IsNull(r) ? 0U : (c2.DictCodeAt(r) + 1U);
            const auto packed_codes = static_cast<int64_t>(
                (static_cast<uint64_t>(d1) << 32U) | static_cast<uint64_t>(d2));
            bool emplaced = false;
            uint32_t* slot =
                code_pair_to_gid.TryEmplace(packed_codes, &emplaced);
            if (emplaced) {
              *slot = next_gid++;
            }
            pk_to_gid[pk] = *slot;
          }
        }
      } else {
        std::vector<Value> key_vals(uniq_dim_slots.size());
        for (const ColumnarChunk& ch : dim_rel.columnar->chunks) {
          const ColumnarColumn& pkc = ch.columns[0];
          for (size_t r = 0; r < ch.rows; ++r) {
            const auto pk = static_cast<size_t>(pkc.IntAt(r));
            if (pk_to_gid[pk] != 0) {
              return std::optional<Relation>{};  // duplicate PK!
            }
            for (size_t di = 0; di < uniq_dim_slots.size(); ++di) {
              key_vals[di] =
                  CanonicalDistinctValue(ch.columns[1 + di].ValueAt(r));
            }
            auto it = tuple_to_gid.find(key_vals);
            uint32_t gid = 0;
            if (it != tuple_to_gid.end()) {
              gid = static_cast<uint32_t>(it->second);
            } else {
              gid = next_gid++;
              tuple_to_gid.emplace(Row(key_vals), gid);
            }
            pk_to_gid[pk] = gid;
          }
        }
      }
    }
  } else {
    int64_t max_pk = -1;
    for (const Row& row : dim_rel.rows) {
      if (row.Size() < 1 + uniq_dim_slots.size() || row[0].IsNull() ||
          row[0].type != ValueType::kInt64) {
        return std::optional<Relation>{};
      }
      const int64_t pk = row[0].value.int_value;
      if (pk < 0 || pk > 1000000) {
        return std::optional<Relation>{};
      }
      max_pk = std::max(max_pk, pk);
    }
    if (max_pk >= 0) {
      pk_to_gid.assign(static_cast<size_t>(max_pk) + 1, 0);
      std::vector<Value> key_vals(uniq_dim_slots.size());
      for (const Row& row : dim_rel.rows) {
        const auto pk = static_cast<size_t>(row[0].value.int_value);
        if (pk_to_gid[pk] != 0) {
          return std::optional<Relation>{};
        }
        for (size_t di = 0; di < uniq_dim_slots.size(); ++di) {
          key_vals[di] = CanonicalDistinctValue(row[1 + di]);
        }
        auto it = tuple_to_gid.find(key_vals);
        uint32_t gid = 0;
        if (it != tuple_to_gid.end()) {
          gid = static_cast<uint32_t>(it->second);
        } else {
          gid = next_gid++;
          tuple_to_gid.emplace(Row(key_vals), gid);
        }
        pk_to_gid[pk] = gid;
      }
    }
  }

  struct WidePairHash {
    size_t operator()(const std::pair<uint32_t, int64_t>& p) const noexcept {
      uint64_t x = (static_cast<uint64_t>(p.first) * 0x9e3779b97f4a7c15ULL) ^
                   static_cast<uint64_t>(p.second);
      x = (x ^ (x >> 30U)) * 0xbf58476d1ce4e5b9ULL;
      return static_cast<size_t>(x ^ (x >> 31U));
    }
  };
  struct QuotientSet {
    FlatIntKeyMap<uint8_t> packed_i32;
    FlatIntKeyMap<uint8_t> null_other_gids;
    std::unordered_set<std::pair<uint32_t, int64_t>, WidePairHash> wide_pairs;
  };

  auto insert_key = [](FlatIntKeyMap<uint8_t>& m, int64_t k) {
    bool emplaced = false;
    uint8_t* slot = m.TryEmplace(k, &emplaced);
    if (emplaced) {
      *slot = 1;
    }
  };

  std::deque<SelectStatement> owned_reduced;
  std::vector<const SelectStatement*> branch_exec_ptrs(branches.size(),
                                                       nullptr);
  std::vector<size_t> branch_fk_slots(branches.size(), 0);
  std::vector<size_t> branch_other_slots(branches.size(), 1);
  for (size_t bi = 0; bi < branches.size(); ++bi) {
    const SelectStatement& br = *branches[bi];
    const BranchPlan& bp = plans[bi];
    if (bp.direct_subquery && bp.subquery_ptr != nullptr) {
      branch_exec_ptrs[bi] = bp.subquery_ptr;
      branch_fk_slots[bi] = bp.sub_fk_slot;
      branch_other_slots[bi] = bp.sub_other_slot;
      continue;
    }
    SelectStatement reduced(br);
    std::vector<SelectSource> new_sources;
    new_sources.reserve(br.Sources().size() - 1);
    for (size_t si = 0; si < br.Sources().size(); ++si) {
      if (si == bp.dim_src_idx) {
        continue;
      }
      SelectSource cp = br.Sources()[si];
      if (new_sources.empty()) {
        cp.join_type = JoinType::kInner;
        cp.join_condition = Expression{};
      } else {
        cp.join_condition = Expression{};
      }
      new_sources.push_back(std::move(cp));
    }
    reduced.SetSources(std::move(new_sources));
    std::vector<Expression> wc = bp.non_dim_where;
    wc.push_back(std::make_shared<UnaryExpression>(bp.fk_expr,
                                                   UnaryOperation::kIsNotNull));
    reduced.SetWhereClause(CombineConjuncts(wc));
    std::vector<NamedExpression> sel;
    std::vector<Expression> grp;
    sel.emplace_back("__fk", bp.fk_expr);
    grp.push_back(bp.fk_expr);
    if (shared_other_pos.has_value()) {
      sel.emplace_back("__ot", bp.other_expr);
      grp.push_back(bp.other_expr);
    }
    reduced.SetSelectList(std::move(sel));
    reduced.SetDistinct(false);
    reduced.SetGroupBy(std::move(grp));
    reduced.SetOrderBy({});
    owned_reduced.push_back(std::move(reduced));
    branch_exec_ptrs[bi] = &owned_reduced.back();
  }
  ASSIGN_OR_RETURN(std::optional<std::vector<Relation>>, par_branch_rels,
                   TryExecuteBranchesParallel(context, branch_exec_ptrs,
                                              inherited_ctes, true));

  // Lean proofs:
  //   lean/TinylambOpt/ResearchProofs/DuckDBGaps/Retail.lean
  //     (ShrinkingIntersect_sound, ExclusionErase_sound)
  auto eval_branch = [&](size_t bi,
                         const QuotientSet* intersect_filter = nullptr,
                         QuotientSet* except_target =
                             nullptr) -> StatusOr<std::optional<QuotientSet>> {
    Relation rel(context.execution_runtime());
    if (par_branch_rels.has_value()) {
      rel = std::move((*par_branch_rels)[bi]);
      RETURN_IF_FAIL(rel.GetStatus());
    } else {
      ASSIGN_OR_RETURN(Relation, run_rel,
                       ExecuteQuery(context, *branch_exec_ptrs[bi], nullptr,
                                    inherited_ctes, nullptr, true));
      RETURN_IF_FAIL(run_rel.GetStatus());
      rel = std::move(run_rel);
    }
    const size_t fk_slot = branch_fk_slots[bi];
    const size_t other_slot = branch_other_slots[bi];

    if (rel.HasSpill() || fk_slot >= rel.schema.ColumnCount()) {
      return std::optional<QuotientSet>{};
    }
    const Column& fkc_meta = rel.schema.GetColumn(fk_slot);
    if (fkc_meta.Type() != ValueType::kInt64 &&
        fkc_meta.Type() != ValueType::kNull) {
      return std::optional<QuotientSet>{};
    }
    if (shared_other_pos.has_value()) {
      if (other_slot >= rel.schema.ColumnCount()) {
        return std::optional<QuotientSet>{};
      }
      const Column& otc_meta = rel.schema.GetColumn(other_slot);
      if ((otc_meta.Type() != ValueType::kInt64 &&
           otc_meta.Type() != ValueType::kDate &&
           otc_meta.Type() != ValueType::kNull) ||
          otc_meta.IsUnsigned()) {
        return std::optional<QuotientSet>{};
      }
    }

    QuotientSet qs;
    if (except_target == nullptr) {
      const size_t reserve_n =
          intersect_filter != nullptr
              ? std::min(rel.TotalRows(), intersect_filter->packed_i32.Size())
              : rel.TotalRows();
      qs.packed_i32.Reserve(reserve_n);
    }
    auto insert_pair = [&](int64_t fk_val, bool other_null, int64_t other_val) {
      if (fk_val < 0 || static_cast<size_t>(fk_val) >= pk_to_gid.size()) {
        return;
      }
      const uint32_t gid = pk_to_gid[static_cast<size_t>(fk_val)];
      if (gid == 0) {
        return;
      }
      if (!shared_other_pos.has_value() || other_null) {
        const auto g = static_cast<int64_t>(gid);
        if (except_target != nullptr) {
          if (const uint8_t* v = except_target->null_other_gids.Find(g);
              v != nullptr) {
            *const_cast<uint8_t*>(v) = 0;
          }
          return;
        }
        if (intersect_filter != nullptr &&
            intersect_filter->null_other_gids.Find(g) == nullptr) {
          return;
        }
        insert_key(qs.null_other_gids, g);
        return;
      }
      if (other_val >= std::numeric_limits<int32_t>::min() &&
          other_val <= std::numeric_limits<int32_t>::max()) {
        const auto u_other =
            static_cast<uint32_t>(static_cast<int32_t>(other_val));
        const auto packed =
            static_cast<int64_t>((static_cast<uint64_t>(gid) << 32U) |
                                 static_cast<uint64_t>(u_other));
        if (except_target != nullptr) {
          if (const uint8_t* v = except_target->packed_i32.Find(packed);
              v != nullptr) {
            *const_cast<uint8_t*>(v) = 0;
          }
          return;
        }
        if (intersect_filter != nullptr &&
            intersect_filter->packed_i32.Find(packed) == nullptr) {
          return;
        }
        insert_key(qs.packed_i32, packed);
      } else {
        if (except_target != nullptr) {
          except_target->wide_pairs.erase({gid, other_val});
          return;
        }
        if (intersect_filter != nullptr &&
            !intersect_filter->wide_pairs.contains({gid, other_val})) {
          return;
        }
        qs.wide_pairs.emplace(gid, other_val);
      }
    };

    if (rel.columnar != nullptr) {
      for (const ColumnarChunk& ch : rel.columnar->chunks) {
        if (fk_slot >= ch.columns.size() ||
            (shared_other_pos.has_value() && other_slot >= ch.columns.size())) {
          return std::optional<QuotientSet>{};
        }
        const ColumnarColumn& fkc = ch.columns[fk_slot];
        if (!shared_other_pos.has_value()) {
          for (size_t r = 0; r < ch.rows; ++r) {
            if (!fkc.IsNull(r)) {
              insert_pair(fkc.IntAt(r), true, 0);
            }
          }
          continue;
        }
        const ColumnarColumn& otc = ch.columns[other_slot];
        if (fkc.IsFlat() && fkc.is_uniform && !fkc.has_null &&
            (fkc.type == ValueType::kInt64) && otc.IsFlat() && otc.is_uniform &&
            !otc.has_null &&
            (otc.type == ValueType::kInt64 || otc.type == ValueType::kDate)) {
          const int64_t* fk_data = fkc.ints.data();
          const int64_t* ot_data = otc.ints.data();
          for (size_t r = 0; r < ch.rows; ++r) {
            insert_pair(fk_data[r], false, ot_data[r]);
          }
        } else {
          for (size_t r = 0; r < ch.rows; ++r) {
            if (fkc.IsNull(r)) {
              continue;
            }
            const bool onull = otc.IsNull(r);
            insert_pair(fkc.IntAt(r), onull, onull ? 0 : otc.IntAt(r));
          }
        }
      }
    } else {
      for (const Row& row : rel.rows) {
        if (fk_slot >= row.Size() || row[fk_slot].IsNull()) {
          continue;
        }
        if (row[fk_slot].type != ValueType::kInt64 ||
            row[fk_slot].IsUnsigned()) {
          return std::optional<QuotientSet>{};
        }
        bool onull = true;
        int64_t oval = 0;
        if (shared_other_pos.has_value()) {
          if (other_slot >= row.Size()) {
            return std::optional<QuotientSet>{};
          }
          onull = row[other_slot].IsNull();
          if (!onull) {
            if ((row[other_slot].type != ValueType::kInt64 &&
                 row[other_slot].type != ValueType::kDate) ||
                row[other_slot].IsUnsigned()) {
              return std::optional<QuotientSet>{};
            }
            oval = row[other_slot].value.int_value;
          }
        }
        insert_pair(row[fk_slot].value.int_value, onull, oval);
      }
    }
    return std::optional<QuotientSet>(std::move(qs));
  };

  auto apply_qop = [&](QuotientSet a, QuotientSet& b,
                       SetOperationKind op) -> QuotientSet {
    if (op == SetOperationKind::kUnion) {
      b.packed_i32.ForEach(
          [&](int64_t k, uint8_t) { insert_key(a.packed_i32, k); });
      b.null_other_gids.ForEach(
          [&](int64_t g, uint8_t) { insert_key(a.null_other_gids, g); });
      for (const auto& wp : b.wide_pairs) {
        a.wide_pairs.insert(wp);
      }
      return a;
    }
    QuotientSet out;
    if (op == SetOperationKind::kIntersect) {
      const bool swap_i32 = b.packed_i32.Size() < a.packed_i32.Size();
      FlatIntKeyMap<uint8_t>& small_i32 =
          swap_i32 ? b.packed_i32 : a.packed_i32;
      const FlatIntKeyMap<uint8_t>& large_i32 =
          swap_i32 ? a.packed_i32 : b.packed_i32;
      out.packed_i32.Reserve(small_i32.Size());
      small_i32.ForEach([&](int64_t k, uint8_t) {
        if (large_i32.Find(k) != nullptr) {
          insert_key(out.packed_i32, k);
        }
      });
      a.null_other_gids.ForEach([&](int64_t g, uint8_t) {
        if (b.null_other_gids.Find(g) != nullptr) {
          insert_key(out.null_other_gids, g);
        }
      });
      for (const auto& wp : a.wide_pairs) {
        if (b.wide_pairs.contains(wp)) {
          out.wide_pairs.insert(wp);
        }
      }
      return out;
    }
    // kExcept
    out.packed_i32.Reserve(a.packed_i32.Size());
    a.packed_i32.ForEach([&](int64_t k, uint8_t) {
      if (b.packed_i32.Find(k) == nullptr) {
        insert_key(out.packed_i32, k);
      }
    });
    a.null_other_gids.ForEach([&](int64_t g, uint8_t) {
      if (b.null_other_gids.Find(g) == nullptr) {
        insert_key(out.null_other_gids, g);
      }
    });
    for (const auto& wp : a.wide_pairs) {
      if (!b.wide_pairs.contains(wp)) {
        out.wide_pairs.insert(wp);
      }
    }
    return out;
  };

  const bool all_intersect =
      !ops.empty() && std::ranges::all_of(ops, [](SetOperationKind op) {
        return op == SetOperationKind::kIntersect;
      });
  const bool all_except =
      !ops.empty() && std::ranges::all_of(ops, [](SetOperationKind op) {
        return op == SetOperationKind::kExcept;
      });

  int64_t total = 0;
  if (all_intersect) {
    // ShrinkingIntersect_sound: seed with the smallest branch and probe-filter
    // every subsequent branch against the shrinking candidate set.
    std::vector<size_t> order(ops.size() + 1);
    std::iota(order.begin(), order.end(), 0);
    if (par_branch_rels.has_value()) {
      std::ranges::sort(order, [&](size_t a, size_t b) {
        return (*par_branch_rels)[a].TotalRows() <
               (*par_branch_rels)[b].TotalRows();
      });
    }
    ASSIGN_OR_RETURN(std::optional<QuotientSet>, acc_opt,
                     eval_branch(order[0]));
    if (!acc_opt.has_value()) {
      return std::optional<Relation>{};
    }
    QuotientSet acc = std::move(*acc_opt);
    for (size_t k = 1; k < order.size(); ++k) {
      if (acc.packed_i32.Size() == 0 && acc.null_other_gids.Size() == 0 &&
          acc.wide_pairs.empty()) {
        break;
      }
      ASSIGN_OR_RETURN(std::optional<QuotientSet>, narrowed,
                       eval_branch(order[k], &acc, nullptr));
      if (!narrowed.has_value()) {
        return std::optional<Relation>{};
      }
      acc = std::move(*narrowed);
    }
    total = static_cast<int64_t>(acc.packed_i32.Size() +
                                 acc.null_other_gids.Size() +
                                 acc.wide_pairs.size());
  } else if (all_except) {
    // ExclusionErase_sound: materialize branch 0 once, then erase matching keys
    // from every EXCEPT branch in-place without allocating branch hash sets.
    ASSIGN_OR_RETURN(std::optional<QuotientSet>, acc_opt, eval_branch(0));
    if (!acc_opt.has_value()) {
      return std::optional<Relation>{};
    }
    QuotientSet acc = std::move(*acc_opt);
    for (size_t i = 0; i < ops.size(); ++i) {
      ASSIGN_OR_RETURN(std::optional<QuotientSet>, erased,
                       eval_branch(i + 1, nullptr, &acc));
      if (!erased.has_value()) {
        return std::optional<Relation>{};
      }
    }
    size_t live = acc.wide_pairs.size();
    acc.packed_i32.ForEach([&](int64_t, uint8_t v) {
      if (v != 0) {
        ++live;
      }
    });
    acc.null_other_gids.ForEach([&](int64_t, uint8_t v) {
      if (v != 0) {
        ++live;
      }
    });
    total = static_cast<int64_t>(live);
  } else {
    ASSIGN_OR_RETURN(std::optional<QuotientSet>, first_qs, eval_branch(0));
    if (!first_qs.has_value()) {
      return std::optional<Relation>{};
    }
    std::vector<QuotientSet> high_terms;
    high_terms.push_back(std::move(*first_qs));
    std::vector<SetOperationKind> low_ops;
    for (size_t i = 0; i < ops.size(); ++i) {
      if (ops[i] == SetOperationKind::kIntersect) {
        ASSIGN_OR_RETURN(std::optional<QuotientSet>, next_qs,
                         eval_branch(i + 1, &high_terms.back(), nullptr));
        if (!next_qs.has_value()) {
          return std::optional<Relation>{};
        }
        high_terms.back() = std::move(*next_qs);
      } else {
        ASSIGN_OR_RETURN(std::optional<QuotientSet>, next_qs,
                         eval_branch(i + 1));
        if (!next_qs.has_value()) {
          return std::optional<Relation>{};
        }
        low_ops.push_back(ops[i]);
        high_terms.push_back(std::move(*next_qs));
      }
    }
    QuotientSet acc = std::move(high_terms[0]);
    for (size_t i = 0; i < low_ops.size(); ++i) {
      acc = apply_qop(std::move(acc), high_terms[i + 1], low_ops[i]);
    }
    total = static_cast<int64_t>(acc.packed_i32.Size() +
                                 acc.null_other_gids.Size() +
                                 acc.wide_pairs.size());
  }
  Relation out(context.execution_runtime());
  out.schema = Schema("", {Column(ProjectionName(statement.SelectList()[0], 0),
                                  ValueType::kInt64)});
  out.AddRow(Row({Value(total)}));
  RETURN_IF_FAIL(out.GetStatus());
  return std::optional<Relation>(std::move(out));
}

}  // namespace tinylamb::relational_detail
