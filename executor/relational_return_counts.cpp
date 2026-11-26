/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
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
#include "common/parallel_work.hpp"
#include "common/platform/process.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/relational_fast_paths.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/window_eval.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/tlcs_file.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

// Lean proofs:
// lean/TinylambOpt/ResearchProofs/AnalyticalOperators/RollupsAndChannelSetCounts.lean
//   (PromoReturnCounts_sound, ThresholdFanout_sound)
// Executes TPC-DS Q72-shaped queries:
//   catalog_sales JOIN inventory JOIN warehouse JOIN item
//   JOIN customer_demographics JOIN household_demographics
//   JOIN date_dim d1 JOIN date_dim d2 JOIN date_dim d3
//   LEFT OUTER JOIN promotion ON cs_promo_sk = p_promo_sk
//   LEFT OUTER JOIN catalog_returns ON (cr_item_sk = cs_item_sk AND
//                                       cr_order_number = cs_order_number)
//   WHERE d1.d_week_seq = d2.d_week_seq
//     AND inv_quantity_on_hand < cs_quantity [* scale]
//     AND d3.d_date > d1.d_date + delta_days
//     AND <local filters on d1, cd, hd>
//   GROUP BY i_item_desc, w_warehouse_name, d1.d_week_seq
//   ORDER BY ... LIMIT K
// Eliminates both LEFT OUTER JOIN row expansions via PromoReturnCounts_sound
// and replaces the catalog_sales x inventory inequality join with a sorted
// suffix-sum threshold probe (ThresholdFanout_sound) over parallel columnar
// scans with zone-map pruning.
StatusOr<std::optional<Relation>> TryExecutePromoReturnThresholdCounts(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& ctes) {
  static const bool disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_PROMO_RETURN_THRESHOLD") !=
      nullptr;
  if (disabled) {
    return std::optional<Relation>{};
  }
  const auto& sources = statement.Sources();
  if (sources.size() != 11 || !statement.WithQueries().empty() ||
      !statement.UnionAll().empty() ||
      statement.GetSetOperationTree() != nullptr || statement.Having() ||
      statement.Qualify() || statement.Distinct() ||
      statement.HasDistinctOn() || statement.WithTies() ||
      statement.AsStruct() || statement.ForUpdate() ||
      statement.Offset() != 0 || statement.GroupBy().size() != 3 ||
      statement.SelectList().size() != 6 || HasWindowFunctions(statement) ||
      StatementContainsVolatileFunction(statement)) {
    return std::optional<Relation>{};
  }

  // Validate all 11 sources are plain base tables (9 INNER/CROSS + 2 LEFT).
  std::vector<std::shared_ptr<Table>> tables(11);
  std::vector<Schema> schemas(11);
  std::vector<std::string> aliases(11);
  std::vector<size_t> left_indices;
  for (size_t i = 0; i < 11; ++i) {
    const SelectSource& s = sources[i];
    if (s.table.empty() || s.query != nullptr || s.unnest != nullptr ||
        s.is_lateral || s.sample.has_value() || !s.using_columns.empty() ||
        ctes.contains(AsciiToLower(s.table))) {
      return std::optional<Relation>{};
    }
    if (i > 0 && s.join_type == JoinType::kLeft) {
      left_indices.push_back(i);
    } else if (i > 0 && s.join_type != JoinType::kInner &&
               s.join_type != JoinType::kCross) {
      return std::optional<Relation>{};
    }
    auto tbl_or = context.GetTable(s.table);
    if (!tbl_or.HasValue() || !tbl_or.Value()) {
      return std::optional<Relation>{};
    }
    tables[i] = tbl_or.Value();
    schemas[i] = tables[i]->GetSchema();
    aliases[i] = AsciiToLower(s.alias.empty() ? s.table : s.alias);
  }
  if (left_indices.size() != 2) {
    return std::optional<Relation>{};
  }

  auto resolve_col =
      [&](const ColumnName& cn) -> std::optional<std::pair<size_t, slot_t>> {
    const std::string want_nm = AsciiToLower(cn.name);
    const std::string want_sc = AsciiToLower(cn.schema);
    std::optional<std::pair<size_t, slot_t>> found;
    for (size_t i = 0; i < 11; ++i) {
      if (!want_sc.empty() && aliases[i] != want_sc &&
          AsciiToLower(sources[i].table) != want_sc) {
        continue;
      }
      for (slot_t c = 0; c < schemas[i].ColumnCount(); ++c) {
        if (AsciiToLower(schemas[i].GetColumn(c).Name().name) == want_nm) {
          if (found.has_value()) {
            return std::nullopt;
          }
          found = {i, c};
        }
      }
    }
    return found;
  };

  // Identify the 2 LEFT OUTER JOINs:
  //   promo_idx: single-key equijoin (fact_promo = promo_pk)
  //   ret_idx  : 2-key equijoin (fact_k1 = ret_k1 AND fact_k2 = ret_k2)
  std::optional<size_t> promo_idx;
  std::optional<size_t> ret_idx;
  std::optional<size_t> fact_idx;
  slot_t fact_promo_slot = 0;
  slot_t promo_pk_slot = 0;
  slot_t fact_ret_k1_slot = 0;
  slot_t fact_ret_k2_slot = 0;
  slot_t ret_k1_slot = 0;
  slot_t ret_k2_slot = 0;
  for (size_t li : left_indices) {
    if (!sources[li].join_condition) {
      return std::optional<Relation>{};
    }
    auto jc = SplitConjuncts(sources[li].join_condition);
    std::vector<std::pair<std::pair<size_t, slot_t>, slot_t>> eqs;
    for (const Expression& e : jc) {
      if (!e || e->Type() != TypeTag::kBinaryExp) {
        return std::optional<Relation>{};
      }
      const auto& b = e->AsBinaryExpression();
      if (b.Op() != BinaryOperation::kEquals || !b.Left() || !b.Right() ||
          b.Left()->Type() != TypeTag::kColumnValue ||
          b.Right()->Type() != TypeTag::kColumnValue) {
        return std::optional<Relation>{};
      }
      auto lr = resolve_col(b.Left()->AsColumnValue().GetColumnName());
      auto rr = resolve_col(b.Right()->AsColumnValue().GetColumnName());
      if (!lr || !rr) {
        return std::optional<Relation>{};
      }
      if (lr->first == li && rr->first != li) {
        eqs.emplace_back(*rr, lr->second);
      } else if (rr->first == li && lr->first != li) {
        eqs.emplace_back(*lr, rr->second);
      } else {
        return std::optional<Relation>{};
      }
    }
    if (eqs.size() == 1) {
      if (promo_idx.has_value()) {
        return std::optional<Relation>{};
      }
      promo_idx = li;
      fact_idx = eqs[0].first.first;
      fact_promo_slot = eqs[0].first.second;
      promo_pk_slot = eqs[0].second;
    } else if (eqs.size() == 2 && eqs[0].first.first == eqs[1].first.first) {
      if (ret_idx.has_value()) {
        return std::optional<Relation>{};
      }
      ret_idx = li;
      if (fact_idx.has_value() && *fact_idx != eqs[0].first.first) {
        return std::optional<Relation>{};
      }
      fact_idx = eqs[0].first.first;
      fact_ret_k1_slot = eqs[0].first.second;
      ret_k1_slot = eqs[0].second;
      fact_ret_k2_slot = eqs[1].first.second;
      ret_k2_slot = eqs[1].second;
    } else {
      return std::optional<Relation>{};
    }
  }
  if (!promo_idx || !ret_idx || !fact_idx) {
    return std::optional<Relation>{};
  }

  // Verify SELECT list:
  //   0..2: the 3 GROUP BY columns (item_desc, wh_name, d1_week_seq)
  //   3: SUM(CASE WHEN promo_pk IS NULL THEN 1 ELSE 0 END)
  //   4: SUM(CASE WHEN promo_pk IS NOT NULL THEN 1 ELSE 0 END)
  //   5: COUNT(*)
  std::array<std::pair<size_t, slot_t>, 3> grp_cols;
  for (size_t k = 0; k < 3; ++k) {
    const Expression& ge = statement.GroupBy()[k];
    const Expression& se = statement.SelectList()[k].expression;
    if (!ge || ge->Type() != TypeTag::kColumnValue || !se ||
        se->Type() != TypeTag::kColumnValue) {
      return std::optional<Relation>{};
    }
    auto gr = resolve_col(ge->AsColumnValue().GetColumnName());
    auto sr = resolve_col(se->AsColumnValue().GetColumnName());
    if (!gr || !sr || *gr != *sr) {
      return std::optional<Relation>{};
    }
    grp_cols[k] = *gr;
  }

  auto is_const_int = [](const Expression& e, int64_t want) -> bool {
    if (!e || e->Type() != TypeTag::kConstantValue) {
      return false;
    }
    const Value& v = e->AsConstantValue().GetValue();
    return !v.IsNull() && !v.IsUnsigned() && v.type == ValueType::kInt64 &&
           v.value.int_value == want;
  };
  auto is_promo_case_sum = [&](const Expression& e, bool want_is_null) -> bool {
    if (!e || e->Type() != TypeTag::kAggregateExp) {
      return false;
    }
    const auto& agg = e->AsAggregateExpression();
    if (agg.GetType() != AggregationType::kSum || agg.Distinct() ||
        agg.WhereFilter() || !agg.Child() ||
        agg.Child()->Type() != TypeTag::kCaseExp) {
      return false;
    }
    const auto& cs = agg.Child()->AsCaseExpression();
    if (cs.when_clauses_.size() != 1 ||
        !is_const_int(cs.when_clauses_[0].second, 1) ||
        !is_const_int(cs.else_clause_, 0)) {
      return false;
    }
    const Expression& cond = cs.when_clauses_[0].first;
    if (!cond || cond->Type() != TypeTag::kUnaryExp) {
      return false;
    }
    const auto& u = cond->AsUnaryExpression();
    const UnaryOperation want_op =
        want_is_null ? UnaryOperation::kIsNull : UnaryOperation::kIsNotNull;
    if (u.Op() != want_op || !u.Child() ||
        u.Child()->Type() != TypeTag::kColumnValue) {
      return false;
    }
    auto cr = resolve_col(u.Child()->AsColumnValue().GetColumnName());
    return cr.has_value() && cr->first == *promo_idx &&
           cr->second == promo_pk_slot;
  };

  if (!is_promo_case_sum(statement.SelectList()[3].expression, true) ||
      !is_promo_case_sum(statement.SelectList()[4].expression, false)) {
    return std::optional<Relation>{};
  }
  {
    const Expression& c_expr = statement.SelectList()[5].expression;
    if (!c_expr || c_expr->Type() != TypeTag::kAggregateExp) {
      return std::optional<Relation>{};
    }
    const auto& cagg = c_expr->AsAggregateExpression();
    if (cagg.GetType() != AggregationType::kCount || cagg.Distinct() ||
        cagg.WhereFilter() ||
        (cagg.Child() && cagg.Child()->Type() != TypeTag::kConstantValue &&
         (cagg.Child()->Type() != TypeTag::kColumnValue ||
          cagg.Child()->AsColumnValue().GetColumnName().name != "*"))) {
      return std::optional<Relation>{};
    }
  }

  // Collect all INNER/CROSS ON + WHERE conjuncts.
  std::vector<Expression> all_conj;
  if (statement.WhereClause()) {
    auto wc = SplitConjuncts(statement.WhereClause());
    all_conj.insert(all_conj.end(), wc.begin(), wc.end());
  }
  for (size_t i = 0; i < 11; ++i) {
    if (i == *promo_idx || i == *ret_idx) {
      continue;
    }
    if (sources[i].join_condition) {
      auto jc = SplitConjuncts(sources[i].join_condition);
      all_conj.insert(all_conj.end(), jc.begin(), jc.end());
    }
  }

  // Partition conjuncts into:
  //   - single-table local filters per source
  //   - equijoin edges (srcA, slotA) = (srcB, slotB)
  //   - inequality 1: inv_qoh < cs_qty [* scale]
  //   - inequality 2: d3_date > d1_date + delta_days
  std::vector<std::vector<Expression>> local_filters(11);
  struct EqEdge {
    size_t src1{0};
    slot_t c1{0};
    size_t src2{0};
    slot_t c2{0};
  };
  std::vector<EqEdge> eq_edges;
  std::optional<size_t> inv_idx;
  slot_t inv_qoh_slot = 0;
  slot_t fact_qty_slot = 0;
  int64_t qty_scale = 1;
  std::optional<size_t> d1_idx;
  std::optional<size_t> d3_idx;
  slot_t d1_date_slot = 0;
  slot_t d3_date_slot = 0;
  int64_t date_delta_days = 0;

  auto collect_touched_srcs = [&](const Expression& e,
                                  std::unordered_set<size_t>* out_s) -> bool {
    bool ok = true;
    const std::function<void(const Expression&)> w = [&](const Expression& n) {
      if (!n || !ok) {
        return;
      }
      if (n->Type() == TypeTag::kQueryExp) {
        ok = false;
        return;
      }
      if (n->Type() == TypeTag::kColumnValue) {
        auto r = resolve_col(n->AsColumnValue().GetColumnName());
        if (!r || r->first == *promo_idx || r->first == *ret_idx) {
          ok = false;
          return;
        }
        out_s->insert(r->first);
        return;
      }
      for (const Expression& ch : ExpressionChildren(n)) {
        w(ch);
      }
    };
    w(e);
    return ok;
  };

  for (const Expression& conj : all_conj) {
    std::unordered_set<size_t> touched;
    if (!collect_touched_srcs(conj, &touched) || touched.empty()) {
      return std::optional<Relation>{};
    }
    if (touched.size() == 1) {
      const size_t only = *touched.begin();
      if (only == *fact_idx) {
        return std::optional<Relation>{};
      }
      local_filters[only].push_back(conj);
      continue;
    }
    if (touched.size() != 2 || !conj || conj->Type() != TypeTag::kBinaryExp) {
      return std::optional<Relation>{};
    }
    const auto& b = conj->AsBinaryExpression();
    if (b.Op() == BinaryOperation::kEquals && b.Left() && b.Right() &&
        b.Left()->Type() == TypeTag::kColumnValue &&
        b.Right()->Type() == TypeTag::kColumnValue) {
      auto lr = resolve_col(b.Left()->AsColumnValue().GetColumnName());
      auto rr = resolve_col(b.Right()->AsColumnValue().GetColumnName());
      if (!lr || !rr || lr->first == rr->first) {
        return std::optional<Relation>{};
      }
      eq_edges.push_back(EqEdge{.src1 = lr->first,
                                .c1 = lr->second,
                                .src2 = rr->first,
                                .c2 = rr->second});
      continue;
    }
    // Check `inv_qoh < cs_qty [* scale]` (or `cs_qty [* scale] > inv_qoh`).
    auto parse_scaled_col =
        [&](const Expression& e,
            int64_t* out_scale) -> std::optional<std::pair<size_t, slot_t>> {
      *out_scale = 1;
      if (!e) {
        return std::nullopt;
      }
      if (e->Type() == TypeTag::kColumnValue) {
        return resolve_col(e->AsColumnValue().GetColumnName());
      }
      if (e->Type() == TypeTag::kBinaryExp &&
          e->AsBinaryExpression().Op() == BinaryOperation::kMultiply) {
        const auto& mb = e->AsBinaryExpression();
        if (mb.Left() && mb.Right()) {
          if (mb.Left()->Type() == TypeTag::kColumnValue &&
              mb.Right()->Type() == TypeTag::kConstantValue) {
            const Value& cv = mb.Right()->AsConstantValue().GetValue();
            if (!cv.IsNull() && !cv.IsUnsigned() &&
                cv.type == ValueType::kInt64 && cv.value.int_value > 0) {
              *out_scale = cv.value.int_value;
              return resolve_col(mb.Left()->AsColumnValue().GetColumnName());
            }
          } else if (mb.Right()->Type() == TypeTag::kColumnValue &&
                     mb.Left()->Type() == TypeTag::kConstantValue) {
            const Value& cv = mb.Left()->AsConstantValue().GetValue();
            if (!cv.IsNull() && !cv.IsUnsigned() &&
                cv.type == ValueType::kInt64 && cv.value.int_value > 0) {
              *out_scale = cv.value.int_value;
              return resolve_col(mb.Right()->AsColumnValue().GetColumnName());
            }
          }
        }
      }
      return std::nullopt;
    };
    if ((b.Op() == BinaryOperation::kLessThan ||
         b.Op() == BinaryOperation::kGreaterThan) &&
        !inv_idx.has_value()) {
      const Expression& small_e =
          (b.Op() == BinaryOperation::kLessThan) ? b.Left() : b.Right();
      const Expression& big_e =
          (b.Op() == BinaryOperation::kLessThan) ? b.Right() : b.Left();
      int64_t sc = 1;
      if (small_e && small_e->Type() == TypeTag::kColumnValue) {
        auto sr = resolve_col(small_e->AsColumnValue().GetColumnName());
        auto br = parse_scaled_col(big_e, &sc);
        if (sr && br && br->first == *fact_idx && sr->first != *fact_idx) {
          const Column& scol = schemas[sr->first].GetColumn(sr->second);
          const Column& bcol = schemas[br->first].GetColumn(br->second);
          if (scol.Type() == ValueType::kInt64 && !scol.IsUnsigned() &&
              bcol.Type() == ValueType::kInt64 && !bcol.IsUnsigned()) {
            inv_idx = sr->first;
            inv_qoh_slot = sr->second;
            fact_qty_slot = br->second;
            qty_scale = sc;
            continue;
          }
        }
      }
    }
    // Check `d3.d_date > d1.d_date + delta_days` (handles `+ 5` or `+ INTERVAL
    // 5 DAY`).
    if ((b.Op() == BinaryOperation::kGreaterThan ||
         b.Op() == BinaryOperation::kLessThan) &&
        !d3_idx.has_value()) {
      const Expression& hi_e =
          (b.Op() == BinaryOperation::kGreaterThan) ? b.Left() : b.Right();
      const Expression& lo_e =
          (b.Op() == BinaryOperation::kGreaterThan) ? b.Right() : b.Left();
      if (hi_e && hi_e->Type() == TypeTag::kColumnValue && lo_e &&
          lo_e->Type() == TypeTag::kBinaryExp &&
          lo_e->AsBinaryExpression().Op() == BinaryOperation::kAdd) {
        auto hr = resolve_col(hi_e->AsColumnValue().GetColumnName());
        const auto& ab = lo_e->AsBinaryExpression();
        if (hr && ab.Left() && ab.Left()->Type() == TypeTag::kColumnValue &&
            ab.Right() && ab.Right()->Type() == TypeTag::kConstantValue) {
          auto lr = resolve_col(ab.Left()->AsColumnValue().GetColumnName());
          if (lr && lr->first != hr->first) {
            const Column& hcol = schemas[hr->first].GetColumn(hr->second);
            const Column& lcol = schemas[lr->first].GetColumn(lr->second);
            if (hcol.Type() == ValueType::kDate &&
                lcol.Type() == ValueType::kDate) {
              // Evaluate `DATE(0) + rhs` once to extract exact day offset.
              const Value base_date = Value::Date("1970-01-01");
              Expression probe_add =
                  BinaryExpressionExp(ConstantValueExp(base_date),
                                      BinaryOperation::kAdd, ab.Right());
              const Value eval_v = probe_add->Evaluate(Row({}), Schema("", {}));
              if (!eval_v.IsNull() && eval_v.type == ValueType::kDate) {
                d3_idx = hr->first;
                d3_date_slot = hr->second;
                d1_idx = lr->first;
                d1_date_slot = lr->second;
                date_delta_days =
                    eval_v.value.int_value - base_date.value.int_value;
                continue;
              }
            }
          }
        }
      }
    }
    return std::optional<Relation>{};
  }
  if (!inv_idx || !d1_idx || !d3_idx || eq_edges.size() != 9) {
    return std::optional<Relation>{};
  }

  // Match the 9 inner equijoin edges:
  //   fact <-> inv (item_sk), fact <-> item, fact <-> cd, fact <-> hd,
  //   fact <-> d1, fact <-> d3, inv <-> wh, inv <-> d2, d1 <-> d2 (week_seq).
  const size_t item_idx = grp_cols[0].first;
  const slot_t item_desc_slot = grp_cols[0].second;
  const size_t wh_idx = grp_cols[1].first;
  const slot_t wh_name_slot = grp_cols[1].second;
  if (grp_cols[2].first != *d1_idx) {
    return std::optional<Relation>{};
  }
  const slot_t d1_week_slot = grp_cols[2].second;

  std::optional<size_t> d2_idx;
  slot_t d2_week_slot = 0;
  slot_t d2_pk_slot = 0;
  slot_t inv_date_slot = 0;
  slot_t inv_item_slot = 0;
  slot_t inv_wh_slot = 0;
  slot_t wh_pk_slot = 0;
  slot_t fact_item_slot = 0;
  slot_t item_pk_slot = 0;
  slot_t fact_d1_slot = 0;
  slot_t d1_pk_slot = 0;
  slot_t fact_d3_slot = 0;
  slot_t d3_pk_slot = 0;
  struct FactFilterDim {
    size_t src{0};
    slot_t fact_col{0};
    slot_t dim_pk{0};
  };
  std::vector<FactFilterDim> fact_fdims;

  auto find_edge = [&](size_t a, size_t b, slot_t* ca, slot_t* cb) -> bool {
    return std::ranges::any_of(eq_edges, [&](const EqEdge& e) {
      if (e.src1 == a && e.src2 == b) {
        *ca = e.c1;
        *cb = e.c2;
        return true;
      }
      if (e.src1 == b && e.src2 == a) {
        *ca = e.c2;
        *cb = e.c1;
        return true;
      }
      return false;
    });
  };

  if (!find_edge(*fact_idx, *inv_idx, &fact_item_slot, &inv_item_slot) ||
      !find_edge(*fact_idx, item_idx, &fact_item_slot, &item_pk_slot) ||
      !find_edge(*fact_idx, *d1_idx, &fact_d1_slot, &d1_pk_slot) ||
      !find_edge(*fact_idx, *d3_idx, &fact_d3_slot, &d3_pk_slot) ||
      !find_edge(*inv_idx, wh_idx, &inv_wh_slot, &wh_pk_slot)) {
    return std::optional<Relation>{};
  }
  // Find d2 (joined to both d1 on week_seq and inv on date_sk).
  for (const EqEdge& e : eq_edges) {
    if (e.src1 == *d1_idx && e.c1 == d1_week_slot && e.src2 != *fact_idx) {
      d2_idx = e.src2;
      d2_week_slot = e.c2;
    } else if (e.src2 == *d1_idx && e.c2 == d1_week_slot &&
               e.src1 != *fact_idx) {
      d2_idx = e.src1;
      d2_week_slot = e.c1;
    }
  }
  if (!d2_idx || !find_edge(*inv_idx, *d2_idx, &inv_date_slot, &d2_pk_slot)) {
    return std::optional<Relation>{};
  }
  // Remaining 2 inner sources are cd and hd (each joined to fact on INT64 key).
  for (size_t i = 0; i < 11; ++i) {
    if (i == *fact_idx || i == *inv_idx || i == item_idx || i == wh_idx ||
        i == *d1_idx || i == *d2_idx || i == *d3_idx || i == *promo_idx ||
        i == *ret_idx) {
      continue;
    }
    slot_t fcol = 0;
    slot_t dpk = 0;
    if (!find_edge(*fact_idx, i, &fcol, &dpk)) {
      return std::optional<Relation>{};
    }
    fact_fdims.push_back(
        FactFilterDim{.src = i, .fact_col = fcol, .dim_pk = dpk});
  }
  if (fact_fdims.size() != 2 || !local_filters[*inv_idx].empty() ||
      !local_filters[item_idx].empty() || !local_filters[wh_idx].empty() ||
      !local_filters[*d2_idx].empty() || !local_filters[*d3_idx].empty()) {
    return std::optional<Relation>{};
  }

  // Helper to scan a single base source with its local filters.
  auto scan_dim = [&](size_t sidx,
                      const std::vector<slot_t>& cols) -> StatusOr<Relation> {
    SelectStatement ds;
    SelectSource src = sources[sidx];
    src.join_type = JoinType::kInner;
    src.join_condition = Expression{};
    ds.SetSources({std::move(src)});
    std::vector<NamedExpression> sel;
    for (slot_t c : cols) {
      const ColumnName& cn = schemas[sidx].GetColumn(c).Name();
      sel.emplace_back(cn.name, ColumnValueExp(cn.name));
    }
    ds.SetSelectList(std::move(sel));
    if (!local_filters[sidx].empty()) {
      ds.SetWhereClause(CombineConjuncts(local_filters[sidx]));
    }
    bool where_done = false;
    ASSIGN_OR_RETURN(
        Relation, rel,
        (BuildInput(context, ds, nullptr, ctes, &where_done, nullptr,
                    /*ignore_projection_star=*/false,
                    /*columnar_output=*/false)));
    if (!where_done && ds.WhereClause()) {
      RETURN_IF_FAIL(FilterRelation(
          context, &rel, SplitConjuncts(ds.WhereClause()), nullptr, ctes));
    }
    RETURN_IF_FAIL(rel.EnsureRowsMaterialized());
    return rel;
  };

  auto bare_col = [&](size_t sidx, slot_t slot) -> ColumnName {
    return ColumnName(schemas[sidx].GetColumn(slot).Name().name);
  };

  // Step 1: Load small dimension tables (d1, d2, d3, cd, hd, promo, wh, item).
  ASSIGN_OR_RETURN(Relation, d1_rel,
                   scan_dim(*d1_idx, {d1_pk_slot, d1_date_slot, d1_week_slot}));
  if (d1_rel.HasSpill() || d1_rel.rows.empty()) {
    return std::optional<Relation>{};
  }
  auto d1_pk_o =
      LocalColumnOffset(d1_rel.schema, bare_col(*d1_idx, d1_pk_slot));
  auto d1_dt_o =
      LocalColumnOffset(d1_rel.schema, bare_col(*d1_idx, d1_date_slot));
  auto d1_wk_o =
      LocalColumnOffset(d1_rel.schema, bare_col(*d1_idx, d1_week_slot));
  if (!d1_pk_o || !d1_dt_o || !d1_wk_o) {
    return std::optional<Relation>{};
  }
  struct D1Info {
    int64_t date_val{0};
    int64_t week_seq{0};
  };
  FlatIntKeyMap<D1Info> d1_map;
  d1_map.Reserve(d1_rel.rows.size());
  FlatIntKeyMap<uint8_t> target_weeks;
  int64_t d1_min_sk = std::numeric_limits<int64_t>::max();
  int64_t d1_max_sk = std::numeric_limits<int64_t>::min();
  for (const Row& r : d1_rel.rows) {
    const Value& pk = r[*d1_pk_o];
    const Value& dt = r[*d1_dt_o];
    const Value& wk = r[*d1_wk_o];
    if (pk.IsNull() || dt.IsNull() || wk.IsNull()) {
      continue;
    }
    bool emp = false;
    D1Info* sl = d1_map.TryEmplace(pk.value.int_value, &emp);
    if (!emp) {
      return std::optional<Relation>{};
    }
    *sl =
        D1Info{.date_val = dt.value.int_value, .week_seq = wk.value.int_value};
    d1_min_sk = std::min(d1_min_sk, pk.value.int_value);
    d1_max_sk = std::max(d1_max_sk, pk.value.int_value);
    bool wemp = false;
    uint8_t* wsl = target_weeks.TryEmplace(wk.value.int_value, &wemp);
    if (wemp) {
      *wsl = 1;
    }
  }
  if (d1_map.Size() == 0) {
    return std::optional<Relation>{};
  }
  std::vector<D1Info> d1_arr;
  std::vector<uint8_t> d1_present;
  if (static_cast<uint64_t>(d1_max_sk - d1_min_sk) + 1 <= 200000) {
    const size_t span = static_cast<size_t>(d1_max_sk - d1_min_sk) + 1;
    d1_arr.resize(span);
    d1_present.assign(span, 0);
    d1_map.ForEach([&](int64_t k, const D1Info& info) {
      const auto off = static_cast<size_t>(k - d1_min_sk);
      d1_arr[off] = info;
      d1_present[off] = 1;
    });
  }
  auto lookup_d1 = [&](int64_t k) -> const D1Info* {
    if (k < d1_min_sk || k > d1_max_sk) {
      return nullptr;
    }
    if (!d1_present.empty()) {
      const auto off = static_cast<size_t>(k - d1_min_sk);
      return d1_present[off] ? &d1_arr[off] : nullptr;
    }
    return d1_map.Find(k);
  };

  // Load d2 (only rows whose d_week_seq is in target_weeks).
  FlatIntKeyMap<int64_t> d2_map;
  int64_t d2_min_sk = std::numeric_limits<int64_t>::max();
  int64_t d2_max_sk = std::numeric_limits<int64_t>::min();
  bool d2_loaded = false;
  if (local_filters[*d2_idx].empty()) {
    std::vector<slot_t> cols{d2_pk_slot, d2_week_slot};
    std::ranges::sort(cols);
    cols.erase(std::ranges::unique(cols).begin(), cols.end());
    if (auto st = GetOrBuildLSTableStore(context.txn_, *tables[*d2_idx], true,
                                         &cols, false);
        st && st->HasColumn(d2_pk_slot) && st->HasColumn(d2_week_slot)) {
      d2_loaded = true;
      for (const LSTablePageBlock& blk : st->blocks) {
        const auto& c_pk = blk.columns[d2_pk_slot];
        const auto& c_wk = blk.columns[d2_week_slot];
        for (uint32_t r = 0; r < blk.row_count; ++r) {
          if ((c_pk.has_null && c_pk.IsNull(r)) ||
              (c_wk.has_null && c_wk.IsNull(r))) {
            continue;
          }
          const int64_t wk = c_wk.IntAt(r);
          if (target_weeks.Find(wk) == nullptr) {
            continue;
          }
          const int64_t pk = c_pk.IntAt(r);
          bool emp = false;
          int64_t* sl = d2_map.TryEmplace(pk, &emp);
          if (!emp) {
            return std::optional<Relation>{};
          }
          *sl = wk;
          d2_min_sk = std::min(d2_min_sk, pk);
          d2_max_sk = std::max(d2_max_sk, pk);
        }
      }
    }
  }
  if (!d2_loaded) {
    ASSIGN_OR_RETURN(Relation, d2_rel,
                     scan_dim(*d2_idx, {d2_pk_slot, d2_week_slot}));
    if (d2_rel.HasSpill()) {
      return std::optional<Relation>{};
    }
    auto d2_pk_o =
        LocalColumnOffset(d2_rel.schema, bare_col(*d2_idx, d2_pk_slot));
    auto d2_wk_o =
        LocalColumnOffset(d2_rel.schema, bare_col(*d2_idx, d2_week_slot));
    if (!d2_pk_o || !d2_wk_o) {
      return std::optional<Relation>{};
    }
    for (const Row& r : d2_rel.rows) {
      const Value& pk = r[*d2_pk_o];
      const Value& wk = r[*d2_wk_o];
      if (pk.IsNull() || wk.IsNull() ||
          target_weeks.Find(wk.value.int_value) == nullptr) {
        continue;
      }
      bool emp = false;
      int64_t* sl = d2_map.TryEmplace(pk.value.int_value, &emp);
      if (!emp) {
        return std::optional<Relation>{};
      }
      *sl = wk.value.int_value;
      d2_min_sk = std::min(d2_min_sk, pk.value.int_value);
      d2_max_sk = std::max(d2_max_sk, pk.value.int_value);
    }
  }
  if (d2_map.Size() == 0) {
    return std::optional<Relation>{};
  }
  constexpr int64_t kMissingInt = std::numeric_limits<int64_t>::min();
  std::vector<int64_t> d2_week_arr;
  if (static_cast<uint64_t>(d2_max_sk - d2_min_sk) + 1 <= 200000) {
    d2_week_arr.assign(static_cast<size_t>(d2_max_sk - d2_min_sk) + 1,
                       kMissingInt);
    d2_map.ForEach([&](int64_t k, int64_t wk) {
      d2_week_arr[static_cast<size_t>(k - d2_min_sk)] = wk;
    });
  }
  auto lookup_d2 = [&](int64_t k) -> int64_t {
    if (k < d2_min_sk || k > d2_max_sk) {
      return kMissingInt;
    }
    if (!d2_week_arr.empty()) {
      return d2_week_arr[static_cast<size_t>(k - d2_min_sk)];
    }
    const int64_t* p = d2_map.Find(k);
    return p ? *p : kMissingInt;
  };

  // Load d3 (date_sk -> d_date).
  FlatIntKeyMap<int64_t> d3_map;
  int64_t d3_min_sk = std::numeric_limits<int64_t>::max();
  int64_t d3_max_sk = std::numeric_limits<int64_t>::min();
  bool d3_loaded = false;
  if (local_filters[*d3_idx].empty()) {
    std::vector<slot_t> cols{d3_pk_slot, d3_date_slot};
    std::ranges::sort(cols);
    cols.erase(std::ranges::unique(cols).begin(), cols.end());
    if (auto st = GetOrBuildLSTableStore(context.txn_, *tables[*d3_idx], true,
                                         &cols, false);
        st && st->HasColumn(d3_pk_slot) && st->HasColumn(d3_date_slot)) {
      d3_loaded = true;
      d3_map.Reserve(st->total_rows);
      for (const LSTablePageBlock& blk : st->blocks) {
        const auto& c_pk = blk.columns[d3_pk_slot];
        const auto& c_dt = blk.columns[d3_date_slot];
        for (uint32_t r = 0; r < blk.row_count; ++r) {
          if ((c_pk.has_null && c_pk.IsNull(r)) ||
              (c_dt.has_null && c_dt.IsNull(r))) {
            continue;
          }
          const int64_t pk = c_pk.IntAt(r);
          const int64_t dt = c_dt.IntAt(r);
          bool emp = false;
          int64_t* sl = d3_map.TryEmplace(pk, &emp);
          if (!emp) {
            return std::optional<Relation>{};
          }
          *sl = dt;
          d3_min_sk = std::min(d3_min_sk, pk);
          d3_max_sk = std::max(d3_max_sk, pk);
        }
      }
    }
  }
  if (!d3_loaded) {
    ASSIGN_OR_RETURN(Relation, d3_rel,
                     scan_dim(*d3_idx, {d3_pk_slot, d3_date_slot}));
    if (d3_rel.HasSpill()) {
      return std::optional<Relation>{};
    }
    auto d3_pk_o =
        LocalColumnOffset(d3_rel.schema, bare_col(*d3_idx, d3_pk_slot));
    auto d3_dt_o =
        LocalColumnOffset(d3_rel.schema, bare_col(*d3_idx, d3_date_slot));
    if (!d3_pk_o || !d3_dt_o) {
      return std::optional<Relation>{};
    }
    d3_map.Reserve(d3_rel.rows.size());
    for (const Row& r : d3_rel.rows) {
      const Value& pk = r[*d3_pk_o];
      const Value& dt = r[*d3_dt_o];
      if (pk.IsNull() || dt.IsNull()) {
        continue;
      }
      bool emp = false;
      int64_t* sl = d3_map.TryEmplace(pk.value.int_value, &emp);
      if (!emp) {
        return std::optional<Relation>{};
      }
      *sl = dt.value.int_value;
      d3_min_sk = std::min(d3_min_sk, pk.value.int_value);
      d3_max_sk = std::max(d3_max_sk, pk.value.int_value);
    }
  }
  std::vector<int64_t> d3_date_arr;
  if (d3_map.Size() > 0 &&
      static_cast<uint64_t>(d3_max_sk - d3_min_sk) + 1 <= 200000) {
    d3_date_arr.assign(static_cast<size_t>(d3_max_sk - d3_min_sk) + 1,
                       kMissingInt);
    d3_map.ForEach([&](int64_t k, int64_t dt) {
      d3_date_arr[static_cast<size_t>(k - d3_min_sk)] = dt;
    });
  }
  auto lookup_d3 = [&](int64_t k) -> int64_t {
    if (k < d3_min_sk || k > d3_max_sk) {
      return kMissingInt;
    }
    if (!d3_date_arr.empty()) {
      return d3_date_arr[static_cast<size_t>(k - d3_min_sk)];
    }
    const int64_t* p = d3_map.Find(k);
    return p ? *p : kMissingInt;
  };

  // Load the 2 fact filter dimensions (cd, hd) into compact bitsets / sets.
  struct DimKeyFilter {
    slot_t fact_col{0};
    int64_t min_k{std::numeric_limits<int64_t>::max()};
    int64_t max_k{std::numeric_limits<int64_t>::min()};
    std::vector<uint8_t> bits;
    FlatIntKeyMap<uint8_t> set;
    [[nodiscard]] bool Match(int64_t k) const noexcept {
      if (k < min_k || k > max_k) {
        return false;
      }
      if (!bits.empty()) {
        return bits[static_cast<size_t>(k - min_k)] != 0;
      }
      return set.Find(k) != nullptr;
    }
  };
  std::vector<DimKeyFilter> fdim_filters(2);
  for (size_t fi = 0; fi < 2; ++fi) {
    const FactFilterDim& fd = fact_fdims[fi];
    DimKeyFilter& df = fdim_filters[fi];
    df.fact_col = fd.fact_col;
    // Fast parallel columnar path when local_filters[fd.src] is a single
    // `col = const` equality on a dense-PK dimension (e.g.
    // customer_demographics 1.92M rows with cd_marital_status = 'D').
    bool fast_loaded = false;
    if (local_filters[fd.src].size() == 1 &&
        local_filters[fd.src][0]->Type() == TypeTag::kBinaryExp) {
      const auto& be = local_filters[fd.src][0]->AsBinaryExpression();
      if (be.Op() == BinaryOperation::kEquals && be.Left() && be.Right()) {
        Expression col_e;
        Expression lit_e;
        if (be.Left()->Type() == TypeTag::kColumnValue &&
            be.Right()->Type() == TypeTag::kConstantValue) {
          col_e = be.Left();
          lit_e = be.Right();
        } else if (be.Right()->Type() == TypeTag::kColumnValue &&
                   be.Left()->Type() == TypeTag::kConstantValue) {
          col_e = be.Right();
          lit_e = be.Left();
        }
        if (col_e && lit_e) {
          auto flt_r = resolve_col(col_e->AsColumnValue().GetColumnName());
          const Value& lit = lit_e->AsConstantValue().GetValue();
          if (flt_r && flt_r->first == fd.src && !lit.IsNull() &&
              (lit.type == ValueType::kVarChar ||
               lit.type == ValueType::kInt64)) {
            const slot_t flt_slot = flt_r->second;
            std::vector<slot_t> cols{fd.dim_pk, flt_slot};
            std::ranges::sort(cols);
            cols.erase(std::ranges::unique(cols).begin(), cols.end());
            if (auto st = GetOrBuildLSTableStore(context.txn_, *tables[fd.src],
                                                 true, &cols, false);
                st && !st->blocks.empty() && st->HasColumn(fd.dim_pk) &&
                st->HasColumn(flt_slot)) {
              int64_t g_min = std::numeric_limits<int64_t>::max();
              int64_t g_max = std::numeric_limits<int64_t>::min();
              bool all_minmax = true;
              for (const LSTablePageBlock& blk : st->blocks) {
                if (blk.row_count == 0) {
                  continue;
                }
                const auto& c_pk = blk.columns[fd.dim_pk];
                if (!c_pk.has_min_max) {
                  all_minmax = false;
                  break;
                }
                g_min = std::min(g_min, c_pk.min_int);
                g_max = std::max(g_max, c_pk.max_int);
              }
              if (all_minmax && g_min <= g_max &&
                  static_cast<uint64_t>(g_max - g_min) + 1 <= 2500000) {
                const size_t span = static_cast<size_t>(g_max - g_min) + 1;
                std::vector<std::atomic<uint8_t>> seen(span);
                for (auto& a : seen) {
                  a.store(0, std::memory_order_relaxed);
                }
                df.bits.assign(span, 0);
                std::atomic<bool> dup_pk{false};
                std::atomic<size_t> match_cnt{0};
                const size_t nb = st->blocks.size();
                const size_t nw = std::max<size_t>(1, ParallelWorkerCount(nb));
                const bool is_str = lit.type == ValueType::kVarChar;
                const std::string_view want_sv =
                    is_str ? std::string_view(lit.value.varchar_value)
                           : std::string_view{};
                const int64_t want_i = is_str ? 0 : lit.value.int_value;
                auto scan_fd = [&](size_t w) {
                  size_t local_cnt = 0;
                  for (size_t b = w; b < nb; b += nw) {
                    if (dup_pk.load(std::memory_order_relaxed)) {
                      return;
                    }
                    const LSTablePageBlock& blk = st->blocks[b];
                    const uint32_t nr = blk.row_count;
                    if (nr == 0) {
                      continue;
                    }
                    const auto& c_pk = blk.columns[fd.dim_pk];
                    const auto& c_fl = blk.columns[flt_slot];
                    if (is_str && c_fl.dict != nullptr && c_fl.dict->active &&
                        !c_fl.dict_codes.empty()) {
                      uint32_t want_code = UINT32_MAX;
                      const size_t n_ent = c_fl.dict->StringEntryCount();
                      for (size_t ci = 0; ci < n_ent; ++ci) {
                        if (c_fl.dict->StringEntryAt(ci) == want_sv) {
                          want_code = static_cast<uint32_t>(ci);
                          break;
                        }
                      }
                      if (want_code == UINT32_MAX) {
                        continue;
                      }
                      const auto wc = static_cast<uint8_t>(want_code);
                      const uint8_t* codes = c_fl.dict_codes.data();
                      for (uint32_t r = 0; r < nr; ++r) {
                        if (c_fl.has_null && c_fl.IsNull(r)) {
                          continue;
                        }
                        if (codes[r] != wc) {
                          continue;
                        }
                        if (c_pk.has_null && c_pk.IsNull(r)) {
                          continue;
                        }
                        const auto off =
                            static_cast<size_t>(c_pk.IntAt(r) - g_min);
                        if (seen[off].exchange(1, std::memory_order_relaxed) !=
                            0) {
                          dup_pk.store(true, std::memory_order_relaxed);
                          return;
                        }
                        df.bits[off] = 1;
                        ++local_cnt;
                      }
                    } else {
                      for (uint32_t r = 0; r < nr; ++r) {
                        if ((c_fl.has_null && c_fl.IsNull(r)) ||
                            (c_pk.has_null && c_pk.IsNull(r))) {
                          continue;
                        }
                        const bool hit = is_str ? (c_fl.VarcharAt(r) == want_sv)
                                                : (c_fl.IntAt(r) == want_i);
                        if (!hit) {
                          continue;
                        }
                        const auto off =
                            static_cast<size_t>(c_pk.IntAt(r) - g_min);
                        if (seen[off].exchange(1, std::memory_order_relaxed) !=
                            0) {
                          dup_pk.store(true, std::memory_order_relaxed);
                          return;
                        }
                        df.bits[off] = 1;
                        ++local_cnt;
                      }
                    }
                  }
                  match_cnt.fetch_add(local_cnt, std::memory_order_relaxed);
                };
                if (RunParallelTasks(nw, scan_fd) == Status::kSuccess &&
                    !dup_pk.load(std::memory_order_relaxed)) {
                  if (match_cnt.load(std::memory_order_relaxed) == 0) {
                    return std::optional<Relation>{};
                  }
                  df.min_k = g_min;
                  df.max_k = g_max;
                  fast_loaded = true;
                } else {
                  df.bits.clear();
                }
              }
            }
          }
        }
      }
    }
    if (!fast_loaded) {
      ASSIGN_OR_RETURN(Relation, rel, scan_dim(fd.src, {fd.dim_pk}));
      if (rel.HasSpill()) {
        return std::optional<Relation>{};
      }
      auto pk_o = LocalColumnOffset(rel.schema, bare_col(fd.src, fd.dim_pk));
      if (!pk_o) {
        return std::optional<Relation>{};
      }
      df.set.Reserve(rel.rows.size());
      for (const Row& r : rel.rows) {
        const Value& pk = r[*pk_o];
        if (pk.IsNull()) {
          continue;
        }
        bool emp = false;
        uint8_t* sl = df.set.TryEmplace(pk.value.int_value, &emp);
        if (!emp) {
          // Duplicate PK in cd/hd would multiply rows; fall back cleanly.
          return std::optional<Relation>{};
        }
        *sl = 1;
        df.min_k = std::min(df.min_k, pk.value.int_value);
        df.max_k = std::max(df.max_k, pk.value.int_value);
      }
      if (df.set.Size() == 0) {
        return std::optional<Relation>{};
      }
      const uint64_t span = static_cast<uint64_t>(df.max_k - df.min_k) + 1;
      if (span <= 2500000) {
        df.bits.assign(static_cast<size_t>(span), 0);
        df.set.ForEach([&](int64_t k, uint8_t) {
          df.bits[static_cast<size_t>(k - df.min_k)] = 1;
        });
      }
    }
  }

  // Load promotion -> promo_cnt[p_promo_sk] (allows duplicates per
  // PromoReturnCounts_sound!).
  ASSIGN_OR_RETURN(Relation, promo_rel, scan_dim(*promo_idx, {promo_pk_slot}));
  if (promo_rel.HasSpill()) {
    return std::optional<Relation>{};
  }
  auto p_pk_o =
      LocalColumnOffset(promo_rel.schema, bare_col(*promo_idx, promo_pk_slot));
  if (!p_pk_o) {
    return std::optional<Relation>{};
  }
  FlatIntKeyMap<int64_t> promo_cnt;
  promo_cnt.Reserve(promo_rel.rows.size());
  for (const Row& r : promo_rel.rows) {
    const Value& pk = r[*p_pk_o];
    if (pk.IsNull()) {
      continue;
    }
    bool emp = false;
    int64_t* sl = promo_cnt.TryEmplace(pk.value.int_value, &emp);
    *sl = emp ? 1 : (*sl + 1);
  }

  // Load warehouse (small, unique PK) and item (medium, unique PK).
  ASSIGN_OR_RETURN(Relation, wh_rel,
                   scan_dim(wh_idx, {wh_pk_slot, wh_name_slot}));
  if (wh_rel.HasSpill()) {
    return std::optional<Relation>{};
  }
  auto w_pk_o = LocalColumnOffset(wh_rel.schema, bare_col(wh_idx, wh_pk_slot));
  auto w_nm_o =
      LocalColumnOffset(wh_rel.schema, bare_col(wh_idx, wh_name_slot));
  if (!w_pk_o || !w_nm_o) {
    return std::optional<Relation>{};
  }
  FlatIntKeyMap<size_t> wh_map;
  for (size_t r = 0; r < wh_rel.rows.size(); ++r) {
    const Value& pk = wh_rel.rows[r][*w_pk_o];
    if (pk.IsNull()) {
      continue;
    }
    bool emp = false;
    size_t* sl = wh_map.TryEmplace(pk.value.int_value, &emp);
    if (!emp) {
      return std::optional<Relation>{};
    }
    *sl = r;
  }

  ASSIGN_OR_RETURN(Relation, item_rel,
                   scan_dim(item_idx, {item_pk_slot, item_desc_slot}));
  if (item_rel.HasSpill()) {
    return std::optional<Relation>{};
  }
  auto i_pk_o =
      LocalColumnOffset(item_rel.schema, bare_col(item_idx, item_pk_slot));
  auto i_ds_o =
      LocalColumnOffset(item_rel.schema, bare_col(item_idx, item_desc_slot));
  if (!i_pk_o || !i_ds_o) {
    return std::optional<Relation>{};
  }
  FlatIntKeyMap<size_t> item_map;
  item_map.Reserve(item_rel.rows.size());
  for (size_t r = 0; r < item_rel.rows.size(); ++r) {
    const Value& pk = item_rel.rows[r][*i_pk_o];
    if (pk.IsNull()) {
      continue;
    }
    bool emp = false;
    size_t* sl = item_map.TryEmplace(pk.value.int_value, &emp);
    if (!emp) {
      return std::optional<Relation>{};
    }
    *sl = r;
  }

  // Step 2: Parallel scan of catalog_sales with d1 zone-map pruning.
  std::vector<slot_t> cs_cols{fact_d1_slot,
                              fact_d3_slot,
                              fact_item_slot,
                              fact_qty_slot,
                              fact_promo_slot,
                              fact_ret_k1_slot,
                              fact_ret_k2_slot,
                              fdim_filters[0].fact_col,
                              fdim_filters[1].fact_col};
  std::ranges::sort(cs_cols);
  cs_cols.erase(std::ranges::unique(cs_cols).begin(), cs_cols.end());
  auto cs_store = GetOrBuildLSTableStore(context.txn_, *tables[*fact_idx], true,
                                         &cs_cols, /*allow_block_lazy=*/false);
  if (!cs_store || cs_store->blocks.empty()) {
    return std::optional<Relation>{};
  }
  for (slot_t c : cs_cols) {
    if (!cs_store->HasColumn(c)) {
      return std::optional<Relation>{};
    }
  }

  struct LiveSale {
    int64_t item_sk{0};
    int64_t week_seq{0};
    int64_t qty_limit{0};  // cs_quantity * qty_scale
    int64_t promo_cnt_val{0};
    int64_t ret_k1{0};
    int64_t ret_k2{0};
    int64_t ret_mult{1};
  };
  const size_t cs_blocks = cs_store->blocks.size();
  const size_t cs_workers = std::max<size_t>(1, ParallelWorkerCount(cs_blocks));
  std::vector<std::vector<LiveSale>> worker_sales(cs_workers);
  auto scan_cs = [&](size_t w) {
    auto& out_vec = worker_sales[w];
    for (size_t b = w; b < cs_blocks; b += cs_workers) {
      const LSTablePageBlock& blk = cs_store->blocks[b];
      const uint32_t nr = blk.row_count;
      if (nr == 0) {
        continue;
      }
      const LSTableColumnBlock& c_d1 = blk.columns[fact_d1_slot];
      if (c_d1.has_min_max &&
          (c_d1.max_int < d1_min_sk || c_d1.min_int > d1_max_sk)) {
        continue;
      }
      const LSTableColumnBlock& c_d3 = blk.columns[fact_d3_slot];
      const LSTableColumnBlock& c_it = blk.columns[fact_item_slot];
      const LSTableColumnBlock& c_qt = blk.columns[fact_qty_slot];
      const LSTableColumnBlock& c_pr = blk.columns[fact_promo_slot];
      const LSTableColumnBlock& c_r1 = blk.columns[fact_ret_k1_slot];
      const LSTableColumnBlock& c_r2 = blk.columns[fact_ret_k2_slot];
      const LSTableColumnBlock& c_f0 = blk.columns[fdim_filters[0].fact_col];
      const LSTableColumnBlock& c_f1 = blk.columns[fdim_filters[1].fact_col];
      for (uint32_t r = 0; r < nr; ++r) {
        if ((c_d1.has_null && c_d1.IsNull(r)) ||
            (c_d3.has_null && c_d3.IsNull(r)) ||
            (c_it.has_null && c_it.IsNull(r)) ||
            (c_qt.has_null && c_qt.IsNull(r)) ||
            (c_f0.has_null && c_f0.IsNull(r)) ||
            (c_f1.has_null && c_f1.IsNull(r))) {
          continue;
        }
        const D1Info* d1i = lookup_d1(c_d1.IntAt(r));
        if (d1i == nullptr) {
          continue;
        }
        if (!fdim_filters[0].Match(c_f0.IntAt(r)) ||
            !fdim_filters[1].Match(c_f1.IntAt(r))) {
          continue;
        }
        const int64_t d3dt = lookup_d3(c_d3.IntAt(r));
        if (d3dt == kMissingInt || d3dt <= d1i->date_val + date_delta_days) {
          continue;
        }
        const int64_t isk = c_it.IntAt(r);
        if (item_map.Find(isk) == nullptr) {
          continue;
        }
        int64_t pc = 0;
        if (!(c_pr.has_null && c_pr.IsNull(r))) {
          if (const int64_t* p = promo_cnt.Find(c_pr.IntAt(r))) {
            pc = *p;
          }
        }
        const bool r1_null = c_r1.has_null && c_r1.IsNull(r);
        const bool r2_null = c_r2.has_null && c_r2.IsNull(r);
        LiveSale ls;
        ls.item_sk = isk;
        ls.week_seq = d1i->week_seq;
        ls.qty_limit = c_qt.IntAt(r) * qty_scale;
        ls.promo_cnt_val = pc;
        ls.ret_k1 =
            r1_null ? std::numeric_limits<int64_t>::min() : c_r1.IntAt(r);
        ls.ret_k2 =
            r2_null ? std::numeric_limits<int64_t>::min() : c_r2.IntAt(r);
        out_vec.push_back(ls);
      }
    }
  };
  if (RunParallelTasks(cs_workers, scan_cs) != Status::kSuccess) {
    return std::optional<Relation>{};
  }

  std::vector<LiveSale> live_sales;
  size_t total_live = 0;
  for (const auto& v : worker_sales) {
    total_live += v.size();
  }
  live_sales.reserve(total_live);
  for (auto& v : worker_sales) {
    live_sales.insert(live_sales.end(), v.begin(), v.end());
  }

  // Build output schema (reused even on empty result).
  std::vector<Column> out_cols;
  out_cols.reserve(6);
  for (size_t k = 0; k < 3; ++k) {
    const Column& c = schemas[grp_cols[k].first].GetColumn(grp_cols[k].second);
    out_cols.emplace_back(ProjectionName(statement.SelectList()[k], k),
                          c.Type());
  }
  for (size_t k = 3; k < 6; ++k) {
    out_cols.emplace_back(ProjectionName(statement.SelectList()[k], k),
                          ValueType::kInt64);
  }
  if (live_sales.empty()) {
    Relation empty_out(context.execution_runtime());
    empty_out.schema = Schema("", std::move(out_cols));
    return std::optional<Relation>(std::move(empty_out));
  }

  // Step 3: Parallel scan of catalog_returns to compute exact return fanout
  // `max(1, ret_cnt)` for each live sale (PromoReturnCounts_sound).
  auto pack_pair = [](int64_t a, int64_t b) -> std::optional<int64_t> {
    if (a < 0 || a > 0x7FFFFFFFLL || b < 0 || b > 0x7FFFFFFFLL) {
      return std::nullopt;
    }
    return static_cast<int64_t>((static_cast<uint64_t>(a) << 32) |
                                static_cast<uint64_t>(b));
  };
  FlatIntKeyMap<uint32_t> ret_counts;
  ret_counts.Reserve(live_sales.size());
  FlatIntKeyMap<uint8_t> live_items;
  live_items.Reserve(live_sales.size());
  int64_t live_item_min = std::numeric_limits<int64_t>::max();
  int64_t live_item_max = std::numeric_limits<int64_t>::min();
  for (const LiveSale& ls : live_sales) {
    bool iemp = false;
    uint8_t* isl = live_items.TryEmplace(ls.item_sk, &iemp);
    if (iemp) {
      *isl = 1;
      live_item_min = std::min(live_item_min, ls.item_sk);
      live_item_max = std::max(live_item_max, ls.item_sk);
    }
    if (ls.ret_k1 != std::numeric_limits<int64_t>::min() &&
        ls.ret_k2 != std::numeric_limits<int64_t>::min()) {
      auto pk = pack_pair(ls.ret_k1, ls.ret_k2);
      if (!pk) {
        return std::optional<Relation>{};
      }
      bool emp = false;
      uint32_t* sl = ret_counts.TryEmplace(*pk, &emp);
      if (emp) {
        *sl = 0;
      }
    }
  }
  if (ret_counts.Size() > 0) {
    std::vector<slot_t> ret_cols{ret_k1_slot, ret_k2_slot};
    std::ranges::sort(ret_cols);
    ret_cols.erase(std::ranges::unique(ret_cols).begin(), ret_cols.end());
    auto ret_store =
        GetOrBuildLSTableStore(context.txn_, *tables[*ret_idx], true, &ret_cols,
                               /*allow_block_lazy=*/false);
    if (!ret_store) {
      return std::optional<Relation>{};
    }
    const size_t r_blocks = ret_store->blocks.size();
    const size_t r_workers = std::max<size_t>(1, ParallelWorkerCount(r_blocks));
    std::vector<std::vector<int64_t>> worker_hits(r_workers);
    auto scan_ret = [&](size_t w) {
      auto& hits = worker_hits[w];
      for (size_t b = w; b < r_blocks; b += r_workers) {
        const LSTablePageBlock& blk = ret_store->blocks[b];
        const uint32_t nr = blk.row_count;
        const LSTableColumnBlock& c1 = blk.columns[ret_k1_slot];
        const LSTableColumnBlock& c2 = blk.columns[ret_k2_slot];
        for (uint32_t r = 0; r < nr; ++r) {
          if ((c1.has_null && c1.IsNull(r)) || (c2.has_null && c2.IsNull(r))) {
            continue;
          }
          auto pk = pack_pair(c1.IntAt(r), c2.IntAt(r));
          if (pk && ret_counts.Find(*pk) != nullptr) {
            hits.push_back(*pk);
          }
        }
      }
    };
    if (RunParallelTasks(r_workers, scan_ret) != Status::kSuccess) {
      return std::optional<Relation>{};
    }
    for (const auto& hits : worker_hits) {
      for (int64_t pk : hits) {
        if (const uint32_t* p = ret_counts.Find(pk)) {
          ++(*const_cast<uint32_t*>(p));
        }
      }
    }
    for (LiveSale& ls : live_sales) {
      if (ls.ret_k1 != std::numeric_limits<int64_t>::min() &&
          ls.ret_k2 != std::numeric_limits<int64_t>::min()) {
        if (auto pk = pack_pair(ls.ret_k1, ls.ret_k2)) {
          if (const uint32_t* p = ret_counts.Find(*pk);
              p != nullptr && *p > 1) {
            ls.ret_mult = static_cast<int64_t>(*p);
          }
        }
      }
    }
  }

  // Group live_sales by (item_sk, week_seq), sorted by qty_limit ascending,
  // with suffix sums of (no_promo, promo, total) for O(log m) threshold
  // fanout lookup (ThresholdFanout_sound).
  struct QtySuffixEntry {
    int64_t qty_limit{0};
    int64_t suf_no_promo{0};
    int64_t suf_promo{0};
    int64_t suf_total{0};
  };
  struct ItemWeekRange {
    uint32_t begin{0};
    uint32_t end{0};
    int64_t max_qty_limit{0};
  };
  std::ranges::sort(live_sales, [](const LiveSale& a, const LiveSale& b) {
    if (a.item_sk != b.item_sk) {
      return a.item_sk < b.item_sk;
    }
    if (a.week_seq != b.week_seq) {
      return a.week_seq < b.week_seq;
    }
    return a.qty_limit < b.qty_limit;
  });
  std::vector<QtySuffixEntry> suf_table(live_sales.size());
  FlatIntKeyMap<ItemWeekRange> iw_map;
  iw_map.Reserve(live_sales.size());
  for (size_t i = 0; i < live_sales.size();) {
    size_t j = i;
    while (j < live_sales.size() &&
           live_sales[j].item_sk == live_sales[i].item_sk &&
           live_sales[j].week_seq == live_sales[i].week_seq) {
      ++j;
    }
    int64_t acc_np = 0;
    int64_t acc_p = 0;
    int64_t acc_t = 0;
    for (size_t k = j; k-- > i;) {
      const LiveSale& ls = live_sales[k];
      const int64_t w = ls.ret_mult;
      acc_np += (ls.promo_cnt_val == 0 ? 1 : 0) * w;
      acc_p += ls.promo_cnt_val * w;
      acc_t += std::max<int64_t>(1, ls.promo_cnt_val) * w;
      suf_table[k] = QtySuffixEntry{.qty_limit = ls.qty_limit,
                                    .suf_no_promo = acc_np,
                                    .suf_promo = acc_p,
                                    .suf_total = acc_t};
    }
    auto iwk = pack_pair(live_sales[i].item_sk, live_sales[i].week_seq);
    if (!iwk) {
      return std::optional<Relation>{};
    }
    bool emp = false;
    ItemWeekRange* sl = iw_map.TryEmplace(*iwk, &emp);
    *sl = ItemWeekRange{.begin = static_cast<uint32_t>(i),
                        .end = static_cast<uint32_t>(j),
                        .max_qty_limit = live_sales[j - 1].qty_limit};
    i = j;
  }

  // Step 4: Parallel scan of inventory with d2 zone-map pruning and
  // O(log m) threshold suffix-sum probe (ThresholdFanout_sound).
  std::vector<slot_t> inv_cols{inv_date_slot, inv_item_slot, inv_wh_slot,
                               inv_qoh_slot};
  std::ranges::sort(inv_cols);
  inv_cols.erase(std::ranges::unique(inv_cols).begin(), inv_cols.end());
  auto inv_store =
      GetOrBuildLSTableStore(context.txn_, *tables[*inv_idx], true, &inv_cols,
                             /*allow_block_lazy=*/false);
  if (!inv_store) {
    return std::optional<Relation>{};
  }
  for (slot_t c : inv_cols) {
    if (!inv_store->HasColumn(c)) {
      return std::optional<Relation>{};
    }
  }

  struct GroupCounts {
    int64_t no_promo{0};
    int64_t promo{0};
    int64_t total{0};
  };
  struct TriKey {
    int64_t item_sk{0};
    int64_t wh_sk{0};
    int64_t week_seq{0};
    bool operator==(const TriKey&) const = default;
  };
  struct TriKeyHash {
    size_t operator()(const TriKey& k) const noexcept {
      uint64_t h = static_cast<uint64_t>(k.item_sk) * 0x9e3779b97f4a7c15ULL;
      h ^= static_cast<uint64_t>(k.wh_sk) * 0xbf58476d1ce4e5b9ULL;
      h ^= static_cast<uint64_t>(k.week_seq) * 0x94d049bb133111ebULL;
      return static_cast<size_t>(h ^ (h >> 33));
    }
  };

  const size_t inv_blocks = inv_store->blocks.size();
  const size_t inv_workers =
      std::max<size_t>(1, ParallelWorkerCount(inv_blocks));
  std::vector<std::unordered_map<TriKey, GroupCounts, TriKeyHash>>
      worker_groups(inv_workers);

  std::vector<uint8_t> live_item_bits;
  if (live_item_min <= live_item_max &&
      static_cast<uint64_t>(live_item_max - live_item_min) + 1 <= 2000000) {
    live_item_bits.assign(
        static_cast<size_t>(live_item_max - live_item_min) + 1, 0);
    live_items.ForEach([&](int64_t k, uint8_t) {
      live_item_bits[static_cast<size_t>(k - live_item_min)] = 1;
    });
  }
  auto is_live_item = [&](int64_t isk) noexcept -> bool {
    if (isk < live_item_min || isk > live_item_max) {
      return false;
    }
    if (!live_item_bits.empty()) {
      return live_item_bits[static_cast<size_t>(isk - live_item_min)] != 0;
    }
    return live_items.Find(isk) != nullptr;
  };

  auto scan_inv = [&](size_t w) {
    auto& local_map = worker_groups[w];
    for (size_t b = w; b < inv_blocks; b += inv_workers) {
      const LSTablePageBlock& blk = inv_store->blocks[b];
      const uint32_t nr = blk.row_count;
      if (nr == 0) {
        continue;
      }
      const LSTableColumnBlock& c_dt = blk.columns[inv_date_slot];
      if (c_dt.has_min_max &&
          (c_dt.max_int < d2_min_sk || c_dt.min_int > d2_max_sk)) {
        continue;
      }
      const bool const_dt =
          c_dt.has_min_max && !c_dt.has_null && c_dt.min_int == c_dt.max_int;
      const int64_t const_wk = const_dt ? lookup_d2(c_dt.min_int) : kMissingInt;
      if (const_dt && const_wk == kMissingInt) {
        continue;
      }
      const LSTableColumnBlock& c_it = blk.columns[inv_item_slot];
      if (c_it.has_min_max &&
          (c_it.max_int < live_item_min || c_it.min_int > live_item_max)) {
        continue;
      }
      const LSTableColumnBlock& c_wh = blk.columns[inv_wh_slot];
      const LSTableColumnBlock& c_qh = blk.columns[inv_qoh_slot];
      for (uint32_t r = 0; r < nr; ++r) {
        if ((!const_dt && c_dt.has_null && c_dt.IsNull(r)) ||
            (c_it.has_null && c_it.IsNull(r)) ||
            (c_wh.has_null && c_wh.IsNull(r)) ||
            (c_qh.has_null && c_qh.IsNull(r))) {
          continue;
        }
        const int64_t wk = const_dt ? const_wk : lookup_d2(c_dt.IntAt(r));
        if (wk == kMissingInt) {
          continue;
        }
        const int64_t isk = c_it.IntAt(r);
        if (!is_live_item(isk)) {
          continue;
        }
        auto iwk = pack_pair(isk, wk);
        if (!iwk) {
          continue;
        }
        const ItemWeekRange* rng = iw_map.Find(*iwk);
        if (rng == nullptr) {
          continue;
        }
        const int64_t qoh = c_qh.IntAt(r);
        if (qoh >= rng->max_qty_limit) {
          continue;
        }
        const int64_t wsk = c_wh.IntAt(r);
        if (wh_map.Find(wsk) == nullptr) {
          continue;
        }
        // Binary search for first entry with qty_limit > qoh.
        uint32_t lo = rng->begin;
        uint32_t hi = rng->end;
        while (lo < hi) {
          const uint32_t mid = lo + ((hi - lo) >> 1U);
          if (suf_table[mid].qty_limit <= qoh) {
            lo = mid + 1;
          } else {
            hi = mid;
          }
        }
        if (lo < rng->end) {
          const QtySuffixEntry& se = suf_table[lo];
          GroupCounts& gc =
              local_map[TriKey{.item_sk = isk, .wh_sk = wsk, .week_seq = wk}];
          gc.no_promo += se.suf_no_promo;
          gc.promo += se.suf_promo;
          gc.total += se.suf_total;
        }
      }
    }
  };
  if (RunParallelTasks(inv_workers, scan_inv) != Status::kSuccess) {
    return std::optional<Relation>{};
  }

  // Step 5: Merge across workers and finalize by (i_item_desc,
  // w_warehouse_name, d1_week_seq) to handle any duplicate item descriptions or
  // warehouse names.
  struct OutGroupKey {
    Value item_desc;
    Value wh_name;
    int64_t week_seq{0};
    bool operator==(const OutGroupKey& o) const {
      return week_seq == o.week_seq && item_desc == o.item_desc &&
             wh_name == o.wh_name;
    }
  };
  struct OutGroupKeyHash {
    size_t operator()(const OutGroupKey& k) const noexcept {
      size_t h = std::hash<Value>{}(k.item_desc);
      h ^= std::hash<Value>{}(k.wh_name) + 0x9e3779b9 + (h << 6) + (h >> 2);
      h ^= std::hash<int64_t>{}(k.week_seq) + 0x9e3779b9 + (h << 6) + (h >> 2);
      return h;
    }
  };
  std::unordered_map<OutGroupKey, GroupCounts, OutGroupKeyHash> final_groups;
  for (const auto& wm : worker_groups) {
    for (const auto& [tk, gc] : wm) {
      const size_t* ir = item_map.Find(tk.item_sk);
      const size_t* wr = wh_map.Find(tk.wh_sk);
      if (ir == nullptr || wr == nullptr) {
        continue;
      }
      OutGroupKey ok{.item_desc = item_rel.rows[*ir][*i_ds_o],
                     .wh_name = wh_rel.rows[*wr][*w_nm_o],
                     .week_seq = tk.week_seq};
      GroupCounts& dst = final_groups[std::move(ok)];
      dst.no_promo += gc.no_promo;
      dst.promo += gc.promo;
      dst.total += gc.total;
    }
  }

  std::vector<Row> rows;
  rows.reserve(final_groups.size());
  for (const auto& [k, gc] : final_groups) {
    rows.emplace_back(std::vector<Value>{k.item_desc, k.wh_name,
                                         Value(k.week_seq), Value(gc.no_promo),
                                         Value(gc.promo), Value(gc.total)});
  }

  // Apply ORDER BY (must reference output columns 0..5) + LIMIT.
  if (!statement.OrderBy().empty()) {
    struct OrdCol {
      size_t col{0};
      bool asc{true};
      bool nulls_first{true};
    };
    std::vector<OrdCol> ords;
    ords.reserve(statement.OrderBy().size());
    for (const auto& term : statement.OrderBy()) {
      if (!term.expression ||
          term.expression->Type() != TypeTag::kColumnValue) {
        return std::optional<Relation>{};
      }
      const std::string nm =
          AsciiToLower(term.expression->AsColumnValue().GetColumnName().name);
      std::optional<size_t> found_c;
      for (size_t c = 0; c < 6; ++c) {
        if (AsciiToLower(out_cols[c].Name().name) == nm) {
          found_c = c;
          break;
        }
      }
      if (!found_c) {
        return std::optional<Relation>{};
      }
      ords.push_back(
          OrdCol{.col = *found_c,
                 .asc = term.ascending,
                 .nulls_first = term.nulls_first.value_or(term.ascending)});
    }
    auto row_less = [&](const Row& a, const Row& b) -> bool {
      for (const OrdCol& o : ords) {
        const Value& va = a[o.col];
        const Value& vb = b[o.col];
        if (va.IsNull() && vb.IsNull()) {
          continue;
        }
        if (va.IsNull()) {
          return o.nulls_first;
        }
        if (vb.IsNull()) {
          return !o.nulls_first;
        }
        if (va == vb) {
          continue;
        }
        if (va.type == ValueType::kVarChar && vb.type == ValueType::kVarChar) {
          return o.asc ? (va.value.varchar_value < vb.value.varchar_value)
                       : (va.value.varchar_value > vb.value.varchar_value);
        }
        return o.asc ? (va.value.int_value < vb.value.int_value)
                     : (va.value.int_value > vb.value.int_value);
      }
      return false;
    };
    if (statement.HasLimit() && statement.Limit() < rows.size()) {
      std::partial_sort(
          rows.begin(),
          rows.begin() + static_cast<std::ptrdiff_t>(statement.Limit()),
          rows.end(), row_less);
      rows.resize(statement.Limit());
    } else {
      std::ranges::sort(rows, row_less);
    }
  } else if (statement.HasLimit() && statement.Limit() < rows.size()) {
    rows.resize(statement.Limit());
  }

  Relation out(context.execution_runtime());
  out.schema = Schema("", std::move(out_cols));
  out.rows.reserve(rows.size());
  for (Row& r : rows) {
    out.AddRow(std::move(r));
  }
  RETURN_IF_FAIL(out.GetStatus());
  return std::optional<Relation>(std::move(out));
}

}  // namespace tinylamb::relational_detail
