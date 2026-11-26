/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
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
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
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
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/tlcs_file.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

// Lean-backed (AnalyticalOperators/RevenueAggregatesAndJoinWitnesses.lean:
// MarketShareWeights_sound, YearRoutes_sound) fast path for 8-table
// star/snowflake market-share queries (TPC-H Q8):
//   SELECT EXTRACT(year FROM o_orderdate) AS o_year,
//          SUM(CASE WHEN n2.n_name = 'X' THEN l_extendedprice * (1 -
//          l_discount)
//                   ELSE 0 END) /
//          SUM(l_extendedprice * (1 - l_discount)) AS mkt_share
//   FROM part, supplier, lineitem, orders, customer, nation n1, nation n2,
//        region
//   WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey
//     AND l_orderkey = o_orderkey AND o_custkey = c_custkey
//     AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey
//     AND s_nationkey = n2.n_nationkey
//     AND <local filters on part, orders, region, ...>
//   GROUP BY EXTRACT(year FROM o_orderdate) [ORDER BY ...]
StatusOr<std::optional<Relation>> TryExecuteMarketShareYearRoutes(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& ctes) {
  if (::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_MARKET_SHARE_ROUTES") !=
      nullptr) {
    return std::optional<Relation>{};
  }
  if (!statement.WithQueries().empty() || !statement.UnionAll().empty() ||
      statement.GetSetOperationTree() != nullptr || statement.Distinct() ||
      statement.Having() || statement.HasLimit() || statement.Offset() > 0 ||
      statement.SelectList().size() != 2 || statement.GroupBy().size() != 1) {
    return std::optional<Relation>{};
  }
  const auto& sources = statement.Sources();
  if (sources.size() != 8) {
    return std::optional<Relation>{};
  }
  for (const auto& s : sources) {
    if (s.query != nullptr || s.unnest != nullptr || s.is_lateral ||
        s.sample.has_value() || !s.using_columns.empty() || s.table.empty() ||
        ctes.contains(AsciiToLower(s.table)) ||
        (s.join_type != JoinType::kInner && s.join_type != JoinType::kCross)) {
      return std::optional<Relation>{};
    }
  }

  std::vector<std::shared_ptr<Table>> tables(8);
  std::vector<Schema> schemas(8);
  std::vector<std::string> aliases(8);
  for (size_t i = 0; i < 8; ++i) {
    ASSIGN_OR_RETURN(std::shared_ptr<Table>, tbl,
                     context.GetTable(sources[i].table));
    if (!tbl) {
      return std::optional<Relation>{};
    }
    tables[i] = std::move(tbl);
    schemas[i] = tables[i]->GetSchema();
    aliases[i] = sources[i].alias.empty() ? sources[i].table : sources[i].alias;
  }

  auto resolve_col =
      [&](const ColumnName& cn) -> std::optional<std::pair<size_t, slot_t>> {
    std::optional<std::pair<size_t, slot_t>> found;
    for (size_t i = 0; i < 8; ++i) {
      if (!cn.schema.empty() && cn.schema != aliases[i] &&
          cn.schema != sources[i].table) {
        continue;
      }
      for (slot_t c = 0; c < schemas[i].ColumnCount(); ++c) {
        if (schemas[i].GetColumn(c).Name().name == cn.name) {
          if (found.has_value()) {
            return std::nullopt;
          }
          found = {i, c};
        }
      }
    }
    return found;
  };

  auto match_extract_year =
      [&](const Expression& e) -> std::optional<std::pair<size_t, slot_t>> {
    if (!e || e->Type() != TypeTag::kFunctionCallExp) {
      return std::nullopt;
    }
    const auto& fc = e->AsFunctionCallExpression();
    if (fc.FuncName() != "extract_year" || fc.Args().size() != 1 ||
        !fc.Args()[0] || fc.Args()[0]->Type() != TypeTag::kColumnValue) {
      return std::nullopt;
    }
    auto r = resolve_col(fc.Args()[0]->AsColumnValue().GetColumnName());
    if (!r ||
        schemas[r->first].GetColumn(r->second).Type() != ValueType::kDate) {
      return std::nullopt;
    }
    return r;
  };
  auto grp_year = match_extract_year(statement.GroupBy()[0]);
  auto sel_year = match_extract_year(statement.SelectList()[0].expression);
  if (!grp_year || !sel_year || *grp_year != *sel_year) {
    return std::optional<Relation>{};
  }
  const size_t ord_idx = grp_year->first;
  const slot_t ord_date_slot = grp_year->second;

  const Expression& sel1 = statement.SelectList()[1].expression;
  if (!sel1 || sel1->Type() != TypeTag::kBinaryExp ||
      sel1->AsBinaryExpression().Op() != BinaryOperation::kDivide) {
    return std::optional<Relation>{};
  }
  const Expression& num_e = sel1->AsBinaryExpression().Left();
  const Expression& den_e = sel1->AsBinaryExpression().Right();
  if (!num_e || !den_e || num_e->Type() != TypeTag::kAggregateExp ||
      den_e->Type() != TypeTag::kAggregateExp) {
    return std::optional<Relation>{};
  }
  const auto& num_agg = num_e->AsAggregateExpression();
  const auto& den_agg = den_e->AsAggregateExpression();
  if (num_agg.GetType() != AggregationType::kSum || num_agg.Distinct() ||
      num_agg.WhereFilter() || !num_agg.Child() ||
      den_agg.GetType() != AggregationType::kSum || den_agg.Distinct() ||
      den_agg.WhereFilter() || !den_agg.Child()) {
    return std::optional<Relation>{};
  }
  if (num_agg.Child()->Type() != TypeTag::kCaseExp) {
    return std::optional<Relation>{};
  }
  const auto& ce = num_agg.Child()->AsCaseExpression();
  if (ce.when_clauses_.size() != 1 || !ce.else_clause_) {
    return std::optional<Relation>{};
  }
  auto is_zero_const = [](const Expression& e) -> bool {
    if (!e || e->Type() != TypeTag::kConstantValue) {
      return false;
    }
    const Value& v = e->AsConstantValue().GetValue();
    return (v.type == ValueType::kInt64 && v.value.int_value == 0) ||
           (v.type == ValueType::kDouble && v.value.double_value == 0.0);
  };
  if (!is_zero_const(ce.else_clause_)) {
    return std::optional<Relation>{};
  }
  const Expression& wcond = ce.when_clauses_[0].first;
  if (!wcond || wcond->Type() != TypeTag::kBinaryExp ||
      wcond->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
    return std::optional<Relation>{};
  }
  Expression n2_col_e;
  Expression country_e;
  if (wcond->AsBinaryExpression().Left()->Type() == TypeTag::kColumnValue &&
      wcond->AsBinaryExpression().Right()->Type() == TypeTag::kConstantValue) {
    n2_col_e = wcond->AsBinaryExpression().Left();
    country_e = wcond->AsBinaryExpression().Right();
  } else if (wcond->AsBinaryExpression().Right()->Type() ==
                 TypeTag::kColumnValue &&
             wcond->AsBinaryExpression().Left()->Type() ==
                 TypeTag::kConstantValue) {
    n2_col_e = wcond->AsBinaryExpression().Right();
    country_e = wcond->AsBinaryExpression().Left();
  } else {
    return std::optional<Relation>{};
  }
  auto n2_name_r = resolve_col(n2_col_e->AsColumnValue().GetColumnName());
  const Value& country_val = country_e->AsConstantValue().GetValue();
  if (!n2_name_r || country_val.IsNull() ||
      country_val.type != ValueType::kVarChar) {
    return std::optional<Relation>{};
  }
  const size_t n2_idx = n2_name_r->first;
  const slot_t n2_name_slot = n2_name_r->second;
  const std::string target_country(country_val.value.varchar_value);

  auto match_volume = [&](const Expression& e)
      -> std::optional<std::tuple<size_t, slot_t, slot_t>> {
    if (!e || e->Type() != TypeTag::kBinaryExp ||
        e->AsBinaryExpression().Op() != BinaryOperation::kMultiply) {
      return std::nullopt;
    }
    auto match_one_minus =
        [&](const Expression& sub) -> std::optional<std::pair<size_t, slot_t>> {
      if (!sub || sub->Type() != TypeTag::kBinaryExp ||
          sub->AsBinaryExpression().Op() != BinaryOperation::kSubtract) {
        return std::nullopt;
      }
      const Expression& l = sub->AsBinaryExpression().Left();
      const Expression& r = sub->AsBinaryExpression().Right();
      if (!l || !r || l->Type() != TypeTag::kConstantValue ||
          r->Type() != TypeTag::kColumnValue) {
        return std::nullopt;
      }
      const Value& lv = l->AsConstantValue().GetValue();
      const bool is_one =
          (lv.type == ValueType::kInt64 && lv.value.int_value == 1) ||
          (lv.type == ValueType::kDouble && lv.value.double_value == 1.0);
      if (!is_one) {
        return std::nullopt;
      }
      return resolve_col(r->AsColumnValue().GetColumnName());
    };
    const Expression& l = e->AsBinaryExpression().Left();
    const Expression& r = e->AsBinaryExpression().Right();
    std::optional<std::pair<size_t, slot_t>> pr;
    std::optional<std::pair<size_t, slot_t>> dr;
    if (l && l->Type() == TypeTag::kColumnValue) {
      dr = match_one_minus(r);
      if (dr.has_value()) {
        pr = resolve_col(l->AsColumnValue().GetColumnName());
      }
    }
    if (!pr && r && r->Type() == TypeTag::kColumnValue) {
      dr = match_one_minus(l);
      if (dr.has_value()) {
        pr = resolve_col(r->AsColumnValue().GetColumnName());
      }
    }
    if (!pr || !dr || pr->first != dr->first) {
      return std::nullopt;
    }
    return std::make_tuple(pr->first, pr->second, dr->second);
  };
  auto vol_then = match_volume(ce.when_clauses_[0].second);
  auto vol_den = match_volume(den_agg.Child());
  if (!vol_then || !vol_den || *vol_then != *vol_den) {
    return std::optional<Relation>{};
  }
  const size_t fact_idx = std::get<0>(*vol_then);
  const slot_t fact_price_slot = std::get<1>(*vol_then);
  const slot_t fact_disc_slot = std::get<2>(*vol_then);
  if (fact_idx == ord_idx || fact_idx == n2_idx || ord_idx == n2_idx) {
    return std::optional<Relation>{};
  }

  std::vector<Expression> all_conj;
  if (statement.WhereClause()) {
    auto wc = SplitConjuncts(statement.WhereClause());
    all_conj.insert(all_conj.end(), wc.begin(), wc.end());
  }
  for (const auto& s : sources) {
    if (s.join_condition) {
      auto jc = SplitConjuncts(s.join_condition);
      all_conj.insert(all_conj.end(), jc.begin(), jc.end());
    }
  }

  std::vector<std::vector<Expression>> local_filters(8);
  struct EqEdge {
    size_t src1{0};
    slot_t c1{0};
    size_t src2{0};
    slot_t c2{0};
  };
  std::vector<EqEdge> eq_edges;
  auto collect_touched = [&](const Expression& e, std::set<size_t>* out,
                             auto& self) -> bool {
    if (!e) {
      return true;
    }
    switch (e->Type()) {
      case TypeTag::kColumnValue: {
        auto r = resolve_col(e->AsColumnValue().GetColumnName());
        if (!r) {
          return false;
        }
        out->insert(r->first);
        return true;
      }
      case TypeTag::kConstantValue:
        return true;
      case TypeTag::kBinaryExp:
        return self(e->AsBinaryExpression().Left(), out, self) &&
               self(e->AsBinaryExpression().Right(), out, self);
      case TypeTag::kUnaryExp:
        return self(e->AsUnaryExpression().Child(), out, self);
      case TypeTag::kFunctionCallExp:
        for (const auto& a : e->AsFunctionCallExpression().Args()) {
          if (!self(a, out, self)) {
            return false;
          }
        }
        return true;
      case TypeTag::kInExp:
        if (!self(e->AsInExpression().child_, out, self)) {
          return false;
        }
        for (const auto& item : e->AsInExpression().list_) {
          if (!self(item, out, self)) {
            return false;
          }
        }
        return true;
      case TypeTag::kCastExp:
        return self(e->AsCastExpression().Child(), out, self);
      default:
        return false;
    }
  };
  for (const Expression& conj : all_conj) {
    std::set<size_t> touched;
    if (!collect_touched(conj, &touched, collect_touched) || touched.empty()) {
      return std::optional<Relation>{};
    }
    if (touched.size() == 1) {
      local_filters[*touched.begin()].push_back(conj);
      continue;
    }
    if (touched.size() == 2 && conj->Type() == TypeTag::kBinaryExp &&
        conj->AsBinaryExpression().Op() == BinaryOperation::kEquals &&
        conj->AsBinaryExpression().Left()->Type() == TypeTag::kColumnValue &&
        conj->AsBinaryExpression().Right()->Type() == TypeTag::kColumnValue) {
      auto l = resolve_col(
          conj->AsBinaryExpression().Left()->AsColumnValue().GetColumnName());
      auto r = resolve_col(
          conj->AsBinaryExpression().Right()->AsColumnValue().GetColumnName());
      if (!l || !r || l->first == r->first) {
        return std::optional<Relation>{};
      }
      eq_edges.push_back(EqEdge{.src1 = l->first,
                                .c1 = l->second,
                                .src2 = r->first,
                                .c2 = r->second});
      continue;
    }
    return std::optional<Relation>{};
  }
  if (eq_edges.size() != 7 || !local_filters[fact_idx].empty()) {
    return std::optional<Relation>{};
  }

  std::vector<std::vector<std::tuple<size_t, slot_t, slot_t>>> adj(8);
  for (const EqEdge& e : eq_edges) {
    if (schemas[e.src1].GetColumn(e.c1).Type() != ValueType::kInt64 ||
        schemas[e.src2].GetColumn(e.c2).Type() != ValueType::kInt64) {
      return std::optional<Relation>{};
    }
    adj[e.src1].emplace_back(e.src2, e.c1, e.c2);
    adj[e.src2].emplace_back(e.src1, e.c2, e.c1);
  }
  if (adj[fact_idx].size() != 3 || adj[n2_idx].size() != 1 ||
      adj[ord_idx].size() != 2) {
    return std::optional<Relation>{};
  }
  std::optional<size_t> part_idx;
  std::optional<size_t> sup_idx;
  slot_t fact_part_slot = 0;
  slot_t part_pk_slot = 0;
  slot_t fact_sup_slot = 0;
  slot_t sup_pk_slot = 0;
  slot_t fact_ord_slot = 0;
  slot_t ord_pk_slot = 0;
  for (const auto& [nb, fc, nc] : adj[fact_idx]) {
    if (nb == ord_idx) {
      fact_ord_slot = fc;
      ord_pk_slot = nc;
    } else if (adj[nb].size() == 1) {
      part_idx = nb;
      fact_part_slot = fc;
      part_pk_slot = nc;
    } else if (adj[nb].size() == 2) {
      sup_idx = nb;
      fact_sup_slot = fc;
      sup_pk_slot = nc;
    } else {
      return std::optional<Relation>{};
    }
  }
  if (!part_idx || !sup_idx) {
    return std::optional<Relation>{};
  }
  slot_t sup_n2_slot = 0;
  slot_t n2_pk_slot = 0;
  bool sup_to_n2 = false;
  for (const auto& [nb, sc, nc] : adj[*sup_idx]) {
    if (nb == n2_idx) {
      sup_n2_slot = sc;
      n2_pk_slot = nc;
      sup_to_n2 = true;
    }
  }
  if (!sup_to_n2) {
    return std::optional<Relation>{};
  }
  auto next_in_chain =
      [&](size_t cur,
          size_t prev) -> std::optional<std::tuple<size_t, slot_t, slot_t>> {
    std::optional<std::tuple<size_t, slot_t, slot_t>> res;
    for (const auto& t : adj[cur]) {
      if (std::get<0>(t) == prev) {
        continue;
      }
      if (res.has_value()) {
        return std::nullopt;
      }
      res = t;
    }
    return res;
  };
  auto ord_c = next_in_chain(ord_idx, fact_idx);
  if (!ord_c) {
    return std::optional<Relation>{};
  }
  const size_t cust_idx = std::get<0>(*ord_c);
  const slot_t ord_cust_slot = std::get<1>(*ord_c);
  const slot_t cust_pk_slot = std::get<2>(*ord_c);
  if (adj[cust_idx].size() != 2) {
    return std::optional<Relation>{};
  }
  auto c_n1 = next_in_chain(cust_idx, ord_idx);
  if (!c_n1) {
    return std::optional<Relation>{};
  }
  const size_t n1_idx = std::get<0>(*c_n1);
  const slot_t cust_n1_slot = std::get<1>(*c_n1);
  const slot_t n1_pk_slot = std::get<2>(*c_n1);
  if (adj[n1_idx].size() != 2) {
    return std::optional<Relation>{};
  }
  auto n1_reg = next_in_chain(n1_idx, cust_idx);
  if (!n1_reg) {
    return std::optional<Relation>{};
  }
  const size_t reg_idx = std::get<0>(*n1_reg);
  const slot_t n1_reg_slot = std::get<1>(*n1_reg);
  const slot_t reg_pk_slot = std::get<2>(*n1_reg);
  if (adj[reg_idx].size() != 1) {
    return std::optional<Relation>{};
  }
  std::set<size_t> all_idx{fact_idx, *part_idx, *sup_idx, n2_idx,
                           ord_idx,  cust_idx,  n1_idx,   reg_idx};
  if (all_idx.size() != 8) {
    return std::optional<Relation>{};
  }

  int64_t ord_date_lo = std::numeric_limits<int64_t>::min();
  int64_t ord_date_hi = std::numeric_limits<int64_t>::max();
  for (const Expression& lf : local_filters[ord_idx]) {
    if (!lf || lf->Type() != TypeTag::kBinaryExp) {
      return std::optional<Relation>{};
    }
    const auto& be = lf->AsBinaryExpression();
    auto parse_bound = [&](const Expression& c_e, const Expression& v_e,
                           BinaryOperation op) -> bool {
      if (!c_e || !v_e || c_e->Type() != TypeTag::kColumnValue ||
          v_e->Type() != TypeTag::kConstantValue) {
        return false;
      }
      auto cr = resolve_col(c_e->AsColumnValue().GetColumnName());
      const Value& v = v_e->AsConstantValue().GetValue();
      if (!cr || cr->first != ord_idx || cr->second != ord_date_slot ||
          v.IsNull() || v.type != ValueType::kDate) {
        return false;
      }
      const int64_t d = v.value.int_value;
      switch (op) {
        case BinaryOperation::kGreaterThanEquals:
          ord_date_lo = std::max(ord_date_lo, d);
          return true;
        case BinaryOperation::kGreaterThan:
          ord_date_lo = std::max(ord_date_lo, d + 1);
          return true;
        case BinaryOperation::kLessThanEquals:
          ord_date_hi = std::min(ord_date_hi, d);
          return true;
        case BinaryOperation::kLessThan:
          ord_date_hi = std::min(ord_date_hi, d - 1);
          return true;
        case BinaryOperation::kEquals:
          ord_date_lo = std::max(ord_date_lo, d);
          ord_date_hi = std::min(ord_date_hi, d);
          return true;
        default:
          return false;
      }
    };
    auto flip_op = [](BinaryOperation op) -> BinaryOperation {
      switch (op) {
        case BinaryOperation::kLessThan:
          return BinaryOperation::kGreaterThan;
        case BinaryOperation::kLessThanEquals:
          return BinaryOperation::kGreaterThanEquals;
        case BinaryOperation::kGreaterThan:
          return BinaryOperation::kLessThan;
        case BinaryOperation::kGreaterThanEquals:
          return BinaryOperation::kLessThanEquals;
        default:
          return op;
      }
    };
    if (!parse_bound(be.Left(), be.Right(), be.Op()) &&
        !parse_bound(be.Right(), be.Left(), flip_op(be.Op()))) {
      return std::optional<Relation>{};
    }
  }

  bool order_by_year = false;
  bool order_desc = false;
  const std::string out_year_name =
      ProjectionName(statement.SelectList()[0], 0);
  if (!statement.OrderBy().empty()) {
    if (statement.OrderBy().size() != 1) {
      return std::optional<Relation>{};
    }
    const auto& ob = statement.OrderBy()[0];
    const auto oy = match_extract_year(ob.expression);
    const bool matches_year =
        (oy && *oy == *grp_year) ||
        (ob.expression && ob.expression->Type() == TypeTag::kColumnValue &&
         ob.expression->AsColumnValue().GetColumnName().name == out_year_name);
    if (!matches_year) {
      return std::optional<Relation>{};
    }
    order_by_year = true;
    order_desc = !ob.ascending;
  }

  auto bare_col = [&](size_t sidx, slot_t slot) -> ColumnName {
    return ColumnName(schemas[sidx].GetColumn(slot).Name().name);
  };
  auto scan_dim = [&](size_t sidx,
                      std::vector<slot_t> cols) -> StatusOr<Relation> {
    std::ranges::sort(cols);
    cols.erase(std::ranges::unique(cols).begin(), cols.end());
    SelectStatement ds;
    SelectSource src;
    src.table = sources[sidx].table;
    src.alias = aliases[sidx];
    src.join_type = JoinType::kInner;
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

  auto make_empty_result = [&]() -> Relation {
    Relation out(context.execution_runtime());
    out.schema =
        Schema("", {Column(ProjectionName(statement.SelectList()[0], 0),
                           ValueType::kInt64),
                    Column(ProjectionName(statement.SelectList()[1], 1),
                           ValueType::kDouble)});
    return out;
  };

  // Step 1: Tiny chain region -> n1 (with multiplicity).
  ASSIGN_OR_RETURN(Relation, reg_rel, scan_dim(reg_idx, {reg_pk_slot}));
  if (reg_rel.HasSpill()) {
    return std::optional<Relation>{};
  }
  auto r_pk_o =
      LocalColumnOffset(reg_rel.schema, bare_col(reg_idx, reg_pk_slot));
  if (!r_pk_o) {
    return std::optional<Relation>{};
  }
  FlatIntKeyMap<uint32_t> reg_cnt;
  for (const Row& r : reg_rel.rows) {
    if (r[*r_pk_o].IsNull()) {
      continue;
    }
    bool emp = false;
    uint32_t* slot = reg_cnt.TryEmplace(r[*r_pk_o].value.int_value, &emp);
    *slot = emp ? 1U : (*slot + 1U);
  }
  if (reg_cnt.Size() == 0) {
    return std::optional<Relation>(make_empty_result());
  }

  ASSIGN_OR_RETURN(Relation, n1_rel,
                   scan_dim(n1_idx, {n1_pk_slot, n1_reg_slot}));
  if (n1_rel.HasSpill()) {
    return std::optional<Relation>{};
  }
  auto n1_pk_o = LocalColumnOffset(n1_rel.schema, bare_col(n1_idx, n1_pk_slot));
  auto n1_rg_o =
      LocalColumnOffset(n1_rel.schema, bare_col(n1_idx, n1_reg_slot));
  if (!n1_pk_o || !n1_rg_o) {
    return std::optional<Relation>{};
  }
  FlatIntKeyMap<uint32_t> n1_mult;
  for (const Row& r : n1_rel.rows) {
    if (r[*n1_pk_o].IsNull() || r[*n1_rg_o].IsNull()) {
      continue;
    }
    const uint32_t* rc = reg_cnt.Find(r[*n1_rg_o].value.int_value);
    if (rc == nullptr) {
      continue;
    }
    bool emp = false;
    uint32_t* sl = n1_mult.TryEmplace(r[*n1_pk_o].value.int_value, &emp);
    *sl = emp ? *rc : (*sl + *rc);
  }
  if (n1_mult.Size() == 0) {
    return std::optional<Relation>(make_empty_result());
  }

  // Step 2: n2 -> supplier (MarketShareWeights_sound: allRows & chosen counts).
  ASSIGN_OR_RETURN(Relation, n2_rel,
                   scan_dim(n2_idx, {n2_pk_slot, n2_name_slot}));
  if (n2_rel.HasSpill()) {
    return std::optional<Relation>{};
  }
  auto n2_pk_o = LocalColumnOffset(n2_rel.schema, bare_col(n2_idx, n2_pk_slot));
  auto n2_nm_o =
      LocalColumnOffset(n2_rel.schema, bare_col(n2_idx, n2_name_slot));
  if (!n2_pk_o || !n2_nm_o) {
    return std::optional<Relation>{};
  }
  struct N2Mult {
    uint32_t all{0};
    uint32_t chosen{0};
  };
  FlatIntKeyMap<N2Mult> n2_mult;
  for (const Row& r : n2_rel.rows) {
    if (r[*n2_pk_o].IsNull()) {
      continue;
    }
    const bool is_chosen = !r[*n2_nm_o].IsNull() &&
                           r[*n2_nm_o].type == ValueType::kVarChar &&
                           r[*n2_nm_o].value.varchar_value == target_country;
    bool emp = false;
    N2Mult* slot = n2_mult.TryEmplace(r[*n2_pk_o].value.int_value, &emp);
    if (emp) {
      *slot = N2Mult{.all = 1U, .chosen = is_chosen ? 1U : 0U};
    } else {
      slot->all += 1U;
      slot->chosen += is_chosen ? 1U : 0U;
    }
  }
  if (n2_mult.Size() == 0) {
    return std::optional<Relation>(make_empty_result());
  }

  struct SupWeights {
    int64_t min_k{std::numeric_limits<int64_t>::max()};
    int64_t max_k{std::numeric_limits<int64_t>::min()};
    std::vector<N2Mult> arr;
    FlatIntKeyMap<N2Mult> map;
    [[nodiscard]] N2Mult Get(int64_t sk) const noexcept {
      if (sk < min_k || sk > max_k) {
        return N2Mult{.all = 0, .chosen = 0};
      }
      if (!arr.empty()) {
        return arr[static_cast<size_t>(sk - min_k)];
      }
      const N2Mult* p = map.Find(sk);
      return p != nullptr ? *p : N2Mult{.all = 0, .chosen = 0};
    }
  } sup_w;
  if (local_filters[*sup_idx].empty()) {
    std::vector<slot_t> scols{sup_pk_slot, sup_n2_slot};
    std::ranges::sort(scols);
    scols.erase(std::ranges::unique(scols).begin(), scols.end());
    if (auto st = GetOrBuildLSTableStore(context.txn_, *tables[*sup_idx], true,
                                         &scols, false);
        st && st->HasColumn(sup_pk_slot) && st->HasColumn(sup_n2_slot)) {
      sup_w.map.Reserve(st->total_rows);
      for (const LSTablePageBlock& blk : st->blocks) {
        const auto& c_pk = blk.columns[sup_pk_slot];
        const auto& c_nk = blk.columns[sup_n2_slot];
        for (uint32_t r = 0; r < blk.row_count; ++r) {
          if ((c_pk.has_null && c_pk.IsNull(r)) ||
              (c_nk.has_null && c_nk.IsNull(r))) {
            continue;
          }
          const N2Mult* nm = n2_mult.Find(c_nk.IntAt(r));
          if (nm == nullptr) {
            continue;
          }
          const int64_t sk = c_pk.IntAt(r);
          bool emp = false;
          N2Mult* sl = sup_w.map.TryEmplace(sk, &emp);
          if (emp) {
            *sl = *nm;
          } else {
            sl->all += nm->all;
            sl->chosen += nm->chosen;
          }
          sup_w.min_k = std::min(sup_w.min_k, sk);
          sup_w.max_k = std::max(sup_w.max_k, sk);
        }
      }
    } else {
      return std::optional<Relation>{};
    }
  } else {
    ASSIGN_OR_RETURN(Relation, sup_rel,
                     scan_dim(*sup_idx, {sup_pk_slot, sup_n2_slot}));
    if (sup_rel.HasSpill()) {
      return std::optional<Relation>{};
    }
    auto spk_o =
        LocalColumnOffset(sup_rel.schema, bare_col(*sup_idx, sup_pk_slot));
    auto snk_o =
        LocalColumnOffset(sup_rel.schema, bare_col(*sup_idx, sup_n2_slot));
    if (!spk_o || !snk_o) {
      return std::optional<Relation>{};
    }
    sup_w.map.Reserve(sup_rel.rows.size());
    for (const Row& r : sup_rel.rows) {
      if (r[*spk_o].IsNull() || r[*snk_o].IsNull()) {
        continue;
      }
      const N2Mult* nm = n2_mult.Find(r[*snk_o].value.int_value);
      if (nm == nullptr) {
        continue;
      }
      const int64_t sk = r[*spk_o].value.int_value;
      bool emp = false;
      N2Mult* sl = sup_w.map.TryEmplace(sk, &emp);
      if (emp) {
        *sl = *nm;
      } else {
        sl->all += nm->all;
        sl->chosen += nm->chosen;
      }
      sup_w.min_k = std::min(sup_w.min_k, sk);
      sup_w.max_k = std::max(sup_w.max_k, sk);
    }
  }
  if (sup_w.map.Size() == 0) {
    return std::optional<Relation>(make_empty_result());
  }
  if (static_cast<uint64_t>(sup_w.max_k - sup_w.min_k) + 1 <= 2000000) {
    sup_w.arr.assign(static_cast<size_t>(sup_w.max_k - sup_w.min_k) + 1,
                     N2Mult{.all = 0, .chosen = 0});
    sup_w.map.ForEach([&](int64_t k, N2Mult m) {
      sup_w.arr[static_cast<size_t>(k - sup_w.min_k)] = m;
    });
  }

  // Step 3: Load customer -> dense multiplicity array cust_mult[c_custkey].
  struct DenseMult {
    int64_t min_k{std::numeric_limits<int64_t>::max()};
    int64_t max_k{std::numeric_limits<int64_t>::min()};
    std::vector<uint32_t> arr;
    FlatIntKeyMap<uint32_t> map;
    [[nodiscard]] uint32_t Get(int64_t k) const noexcept {
      if (k < min_k || k > max_k) {
        return 0;
      }
      if (!arr.empty()) {
        return arr[static_cast<size_t>(k - min_k)];
      }
      const uint32_t* p = map.Find(k);
      return p != nullptr ? *p : 0;
    }
  };
  DenseMult cust_mult;
  bool cust_fast = false;
  if (local_filters[cust_idx].empty()) {
    std::vector<slot_t> ccols{cust_pk_slot, cust_n1_slot};
    std::ranges::sort(ccols);
    ccols.erase(std::ranges::unique(ccols).begin(), ccols.end());
    if (auto st = GetOrBuildLSTableStore(context.txn_, *tables[cust_idx], true,
                                         &ccols, false);
        st && !st->blocks.empty() && st->HasColumn(cust_pk_slot) &&
        st->HasColumn(cust_n1_slot)) {
      int64_t gmin = std::numeric_limits<int64_t>::max();
      int64_t gmax = std::numeric_limits<int64_t>::min();
      bool all_mm = true;
      for (const LSTablePageBlock& blk : st->blocks) {
        if (blk.row_count == 0) {
          continue;
        }
        const auto& c_pk = blk.columns[cust_pk_slot];
        if (!c_pk.has_min_max) {
          all_mm = false;
          break;
        }
        gmin = std::min(gmin, c_pk.min_int);
        gmax = std::max(gmax, c_pk.max_int);
      }
      if (all_mm && gmin <= gmax &&
          static_cast<uint64_t>(gmax - gmin) + 1 <= 20000000) {
        const size_t span = static_cast<size_t>(gmax - gmin) + 1;
        std::vector<std::atomic<uint32_t>> cnt(span);
        for (auto& a : cnt) {
          a.store(0, std::memory_order_relaxed);
        }
        std::atomic<size_t> matched{0};
        const size_t nb = st->blocks.size();
        const size_t nw = std::max<size_t>(1, ParallelWorkerCount(nb));
        auto scan_c = [&](size_t w) {
          size_t lcnt = 0;
          for (size_t b = w; b < nb; b += nw) {
            const LSTablePageBlock& blk = st->blocks[b];
            const auto& c_pk = blk.columns[cust_pk_slot];
            const auto& c_nk = blk.columns[cust_n1_slot];
            for (uint32_t r = 0; r < blk.row_count; ++r) {
              if ((c_pk.has_null && c_pk.IsNull(r)) ||
                  (c_nk.has_null && c_nk.IsNull(r))) {
                continue;
              }
              const uint32_t* nm = n1_mult.Find(c_nk.IntAt(r));
              if (nm == nullptr) {
                continue;
              }
              cnt[static_cast<size_t>(c_pk.IntAt(r) - gmin)].fetch_add(
                  *nm, std::memory_order_relaxed);
              ++lcnt;
            }
          }
          matched.fetch_add(lcnt, std::memory_order_relaxed);
        };
        if (RunParallelTasks(nw, scan_c) == Status::kSuccess) {
          if (matched.load(std::memory_order_relaxed) == 0) {
            return std::optional<Relation>(make_empty_result());
          }
          cust_mult.arr.resize(span);
          for (size_t i = 0; i < span; ++i) {
            cust_mult.arr[i] = cnt[i].load(std::memory_order_relaxed);
          }
          cust_mult.min_k = gmin;
          cust_mult.max_k = gmax;
          cust_fast = true;
        }
      }
    }
  }
  if (!cust_fast) {
    ASSIGN_OR_RETURN(Relation, cust_rel,
                     scan_dim(cust_idx, {cust_pk_slot, cust_n1_slot}));
    if (cust_rel.HasSpill()) {
      return std::optional<Relation>{};
    }
    auto cpk_o =
        LocalColumnOffset(cust_rel.schema, bare_col(cust_idx, cust_pk_slot));
    auto cnk_o =
        LocalColumnOffset(cust_rel.schema, bare_col(cust_idx, cust_n1_slot));
    if (!cpk_o || !cnk_o) {
      return std::optional<Relation>{};
    }
    for (const Row& r : cust_rel.rows) {
      if (r[*cpk_o].IsNull() || r[*cnk_o].IsNull()) {
        continue;
      }
      const uint32_t* nm = n1_mult.Find(r[*cnk_o].value.int_value);
      if (nm == nullptr) {
        continue;
      }
      const int64_t ck = r[*cpk_o].value.int_value;
      bool emp = false;
      uint32_t* sl = cust_mult.map.TryEmplace(ck, &emp);
      *sl = emp ? *nm : (*sl + *nm);
      cust_mult.min_k = std::min(cust_mult.min_k, ck);
      cust_mult.max_k = std::max(cust_mult.max_k, ck);
    }
    if (cust_mult.map.Size() == 0) {
      return std::optional<Relation>(make_empty_result());
    }
  }

  // Step 4: Load part -> dense multiplicity array part_mult[p_partkey].
  DenseMult part_mult;
  bool part_fast = false;
  if (local_filters[*part_idx].size() == 1 &&
      local_filters[*part_idx][0]->Type() == TypeTag::kBinaryExp &&
      local_filters[*part_idx][0]->AsBinaryExpression().Op() ==
          BinaryOperation::kEquals) {
    const auto& be = local_filters[*part_idx][0]->AsBinaryExpression();
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
      if (flt_r && flt_r->first == *part_idx && !lit.IsNull() &&
          (lit.type == ValueType::kVarChar || lit.type == ValueType::kInt64)) {
        const slot_t flt_slot = flt_r->second;
        std::vector<slot_t> pcols{part_pk_slot, flt_slot};
        std::ranges::sort(pcols);
        pcols.erase(std::ranges::unique(pcols).begin(), pcols.end());
        if (auto st = GetOrBuildLSTableStore(context.txn_, *tables[*part_idx],
                                             true, &pcols, false);
            st && !st->blocks.empty() && st->HasColumn(part_pk_slot) &&
            st->HasColumn(flt_slot)) {
          int64_t gmin = std::numeric_limits<int64_t>::max();
          int64_t gmax = std::numeric_limits<int64_t>::min();
          bool all_mm = true;
          for (const LSTablePageBlock& blk : st->blocks) {
            if (blk.row_count == 0) {
              continue;
            }
            const auto& c_pk = blk.columns[part_pk_slot];
            if (!c_pk.has_min_max) {
              all_mm = false;
              break;
            }
            gmin = std::min(gmin, c_pk.min_int);
            gmax = std::max(gmax, c_pk.max_int);
          }
          if (all_mm && gmin <= gmax &&
              static_cast<uint64_t>(gmax - gmin) + 1 <= 20000000) {
            const size_t span = static_cast<size_t>(gmax - gmin) + 1;
            std::vector<std::atomic<uint32_t>> cnt(span);
            for (auto& a : cnt) {
              a.store(0, std::memory_order_relaxed);
            }
            std::atomic<size_t> matched{0};
            const size_t nb = st->blocks.size();
            const size_t nw = std::max<size_t>(1, ParallelWorkerCount(nb));
            const bool is_str = lit.type == ValueType::kVarChar;
            const std::string_view want_sv =
                is_str ? std::string_view(lit.value.varchar_value)
                       : std::string_view{};
            const int64_t want_i = is_str ? 0 : lit.value.int_value;
            auto scan_p = [&](size_t w) {
              size_t lcnt = 0;
              for (size_t b = w; b < nb; b += nw) {
                const LSTablePageBlock& blk = st->blocks[b];
                const uint32_t nr = blk.row_count;
                if (nr == 0) {
                  continue;
                }
                const auto& c_pk = blk.columns[part_pk_slot];
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
                    if ((c_fl.has_null && c_fl.IsNull(r)) || codes[r] != wc ||
                        (c_pk.has_null && c_pk.IsNull(r))) {
                      continue;
                    }
                    cnt[static_cast<size_t>(c_pk.IntAt(r) - gmin)].fetch_add(
                        1U, std::memory_order_relaxed);
                    ++lcnt;
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
                    cnt[static_cast<size_t>(c_pk.IntAt(r) - gmin)].fetch_add(
                        1U, std::memory_order_relaxed);
                    ++lcnt;
                  }
                }
              }
              matched.fetch_add(lcnt, std::memory_order_relaxed);
            };
            if (RunParallelTasks(nw, scan_p) == Status::kSuccess) {
              if (matched.load(std::memory_order_relaxed) == 0) {
                return std::optional<Relation>(make_empty_result());
              }
              part_mult.arr.resize(span);
              for (size_t i = 0; i < span; ++i) {
                part_mult.arr[i] = cnt[i].load(std::memory_order_relaxed);
              }
              part_mult.min_k = gmin;
              part_mult.max_k = gmax;
              part_fast = true;
            }
          }
        }
      }
    }
  }
  if (!part_fast) {
    ASSIGN_OR_RETURN(Relation, part_rel, scan_dim(*part_idx, {part_pk_slot}));
    if (part_rel.HasSpill()) {
      return std::optional<Relation>{};
    }
    auto ppk_o =
        LocalColumnOffset(part_rel.schema, bare_col(*part_idx, part_pk_slot));
    if (!ppk_o) {
      return std::optional<Relation>{};
    }
    for (const Row& r : part_rel.rows) {
      if (r[*ppk_o].IsNull()) {
        continue;
      }
      const int64_t pk = r[*ppk_o].value.int_value;
      bool emp = false;
      uint32_t* sl = part_mult.map.TryEmplace(pk, &emp);
      *sl = emp ? 1U : (*sl + 1U);
      part_mult.min_k = std::min(part_mult.min_k, pk);
      part_mult.max_k = std::max(part_mult.max_k, pk);
    }
    if (part_mult.map.Size() == 0) {
      return std::optional<Relation>(make_empty_result());
    }
    if (static_cast<uint64_t>(part_mult.max_k - part_mult.min_k) + 1 <=
        20000000) {
      part_mult.arr.assign(
          static_cast<size_t>(part_mult.max_k - part_mult.min_k) + 1, 0);
      part_mult.map.ForEach([&](int64_t k, uint32_t m) {
        part_mult.arr[static_cast<size_t>(k - part_mult.min_k)] = m;
      });
    }
  }

  // Step 5: Parallel scan of lineitem rejecting 99%+ rows on
  // part_mult[l_partkey] first, accumulating exact (num, den) weighted totals
  // per l_orderkey (MarketShareWeights_sound).
  std::vector<slot_t> lcols{fact_part_slot, fact_sup_slot, fact_ord_slot,
                            fact_price_slot, fact_disc_slot};
  std::ranges::sort(lcols);
  lcols.erase(std::ranges::unique(lcols).begin(), lcols.end());
  auto l_store = GetOrBuildLSTableStore(context.txn_, *tables[fact_idx], true,
                                        &lcols, /*allow_block_lazy=*/false);
  if (!l_store || l_store->blocks.empty()) {
    return std::optional<Relation>{};
  }
  for (slot_t c : lcols) {
    if (!l_store->HasColumn(c)) {
      return std::optional<Relation>{};
    }
  }
  const bool price_is_dbl =
      schemas[fact_idx].GetColumn(fact_price_slot).Type() == ValueType::kDouble;
  const bool disc_is_dbl =
      schemas[fact_idx].GetColumn(fact_disc_slot).Type() == ValueType::kDouble;
  auto double_at = [](const LSTableColumnBlock& col, uint32_t r,
                      bool is_dbl) noexcept -> double {
    const int64_t bits = col.IntAt(r);
    if (!is_dbl) {
      return static_cast<double>(bits);
    }
    double d = 0.0;
    std::memcpy(&d, &bits, sizeof(d));
    return d;
  };

  struct ShareTotals {
    double num_sum{0.0};
    double den_sum{0.0};
    uint64_t num_cnt{0};
    uint64_t den_cnt{0};
  };
  struct OrderContrib {
    int64_t orderkey{0};
    ShareTotals tot;
  };
  const size_t l_blocks = l_store->blocks.size();
  const size_t l_workers = std::max<size_t>(1, ParallelWorkerCount(l_blocks));
  std::vector<std::vector<OrderContrib>> worker_lines(l_workers);
  auto scan_l = [&](size_t w) {
    auto& out_vec = worker_lines[w];
    for (size_t b = w; b < l_blocks; b += l_workers) {
      const LSTablePageBlock& blk = l_store->blocks[b];
      const uint32_t nr = blk.row_count;
      if (nr == 0) {
        continue;
      }
      const auto& c_pk = blk.columns[fact_part_slot];
      if (c_pk.has_min_max &&
          (c_pk.max_int < part_mult.min_k || c_pk.min_int > part_mult.max_k)) {
        continue;
      }
      const auto& c_sk = blk.columns[fact_sup_slot];
      const auto& c_ok = blk.columns[fact_ord_slot];
      const auto& c_pr = blk.columns[fact_price_slot];
      const auto& c_ds = blk.columns[fact_disc_slot];
      for (uint32_t r = 0; r < nr; ++r) {
        if (c_pk.has_null && c_pk.IsNull(r)) {
          continue;
        }
        const uint32_t pm = part_mult.Get(c_pk.IntAt(r));
        if (pm == 0) {
          continue;
        }
        if ((c_sk.has_null && c_sk.IsNull(r)) ||
            (c_ok.has_null && c_ok.IsNull(r))) {
          continue;
        }
        const N2Mult sw = sup_w.Get(c_sk.IntAt(r));
        if (sw.all == 0) {
          continue;
        }
        const bool v_null = (c_pr.has_null && c_pr.IsNull(r)) ||
                            (c_ds.has_null && c_ds.IsNull(r));
        ShareTotals st;
        const uint64_t w_all = static_cast<uint64_t>(pm) * sw.all;
        const uint64_t w_ch = static_cast<uint64_t>(pm) * sw.chosen;
        if (v_null) {
          st.num_cnt = w_all - w_ch;
        } else {
          const double vol = double_at(c_pr, r, price_is_dbl) *
                             (1.0 - double_at(c_ds, r, disc_is_dbl));
          st.num_sum = static_cast<double>(w_ch) * vol;
          st.den_sum = static_cast<double>(w_all) * vol;
          st.num_cnt = w_all;
          st.den_cnt = w_all;
        }
        out_vec.push_back(OrderContrib{.orderkey = c_ok.IntAt(r), .tot = st});
      }
    }
  };
  if (RunParallelTasks(l_workers, scan_l) != Status::kSuccess) {
    return std::optional<Relation>{};
  }

  size_t total_lc = 0;
  for (const auto& v : worker_lines) {
    total_lc += v.size();
  }
  if (total_lc == 0) {
    return std::optional<Relation>(make_empty_result());
  }
  FlatIntKeyMap<ShareTotals> order_agg;
  order_agg.Reserve(total_lc);
  int64_t ord_min_k = std::numeric_limits<int64_t>::max();
  int64_t ord_max_k = std::numeric_limits<int64_t>::min();
  for (const auto& v : worker_lines) {
    for (const OrderContrib& oc : v) {
      bool emp = false;
      ShareTotals* sl = order_agg.TryEmplace(oc.orderkey, &emp);
      if (emp) {
        *sl = oc.tot;
      } else {
        sl->num_sum += oc.tot.num_sum;
        sl->den_sum += oc.tot.den_sum;
        sl->num_cnt += oc.tot.num_cnt;
        sl->den_cnt += oc.tot.den_cnt;
      }
      ord_min_k = std::min(ord_min_k, oc.orderkey);
      ord_max_k = std::max(ord_max_k, oc.orderkey);
    }
  }

  // Step 6: Parallel scan of orders with o_orderdate zone-map pruning and
  // YearRoutes_sound routing into per-year totals.
  std::vector<slot_t> ocols{ord_pk_slot, ord_cust_slot, ord_date_slot};
  std::ranges::sort(ocols);
  ocols.erase(std::ranges::unique(ocols).begin(), ocols.end());
  auto o_store = GetOrBuildLSTableStore(context.txn_, *tables[ord_idx], true,
                                        &ocols, /*allow_block_lazy=*/false);
  if (!o_store || o_store->blocks.empty()) {
    return std::optional<Relation>{};
  }
  for (slot_t c : ocols) {
    if (!o_store->HasColumn(c)) {
      return std::optional<Relation>{};
    }
  }

  const size_t o_blocks = o_store->blocks.size();
  const size_t o_workers = std::max<size_t>(1, ParallelWorkerCount(o_blocks));
  std::vector<std::map<int64_t, ShareTotals>> worker_years(o_workers);
  auto scan_o = [&](size_t w) {
    auto& ym = worker_years[w];
    for (size_t b = w; b < o_blocks; b += o_workers) {
      const LSTablePageBlock& blk = o_store->blocks[b];
      const uint32_t nr = blk.row_count;
      if (nr == 0) {
        continue;
      }
      const auto& c_dt = blk.columns[ord_date_slot];
      if (c_dt.has_min_max &&
          (c_dt.max_int < ord_date_lo || c_dt.min_int > ord_date_hi)) {
        continue;
      }
      const auto& c_ok = blk.columns[ord_pk_slot];
      if (c_ok.has_min_max &&
          (c_ok.max_int < ord_min_k || c_ok.min_int > ord_max_k)) {
        continue;
      }
      const auto& c_ck = blk.columns[ord_cust_slot];
      for (uint32_t r = 0; r < nr; ++r) {
        if ((c_ok.has_null && c_ok.IsNull(r)) ||
            (c_ck.has_null && c_ck.IsNull(r)) ||
            (c_dt.has_null && c_dt.IsNull(r))) {
          continue;
        }
        const int64_t dt = c_dt.IntAt(r);
        if (dt < ord_date_lo || dt > ord_date_hi) {
          continue;
        }
        const uint32_t cm = cust_mult.Get(c_ck.IntAt(r));
        if (cm == 0) {
          continue;
        }
        const ShareTotals* oa = order_agg.Find(c_ok.IntAt(r));
        if (oa == nullptr) {
          continue;
        }
        const std::chrono::year_month_day ymd{
            std::chrono::sys_days{std::chrono::days{static_cast<int>(dt)}}};
        const auto yr = static_cast<int64_t>(static_cast<int>(ymd.year()));
        ShareTotals& dst = ym[yr];
        const auto cm_d = static_cast<double>(cm);
        dst.num_sum += cm_d * oa->num_sum;
        dst.den_sum += cm_d * oa->den_sum;
        dst.num_cnt += static_cast<uint64_t>(cm) * oa->num_cnt;
        dst.den_cnt += static_cast<uint64_t>(cm) * oa->den_cnt;
      }
    }
  };
  if (RunParallelTasks(o_workers, scan_o) != Status::kSuccess) {
    return std::optional<Relation>{};
  }

  std::map<int64_t, ShareTotals> merged_years;
  for (const auto& ym : worker_years) {
    for (const auto& [yr, st] : ym) {
      ShareTotals& dst = merged_years[yr];
      dst.num_sum += st.num_sum;
      dst.den_sum += st.den_sum;
      dst.num_cnt += st.num_cnt;
      dst.den_cnt += st.den_cnt;
    }
  }

  Relation out = make_empty_result();
  std::vector<std::pair<int64_t, ShareTotals>> ordered(merged_years.begin(),
                                                       merged_years.end());
  if (order_by_year && order_desc) {
    std::ranges::reverse(ordered);
  }
  out.rows.reserve(ordered.size());
  for (const auto& [yr, st] : ordered) {
    Value ratio;
    if (st.num_cnt > 0 && st.den_cnt > 0) {
      if (st.den_sum == 0.0) {
        return std::optional<Relation>{};
      }
      ratio = Value(st.num_sum / st.den_sum);
    }
    out.AddRow(Row({Value(yr), ratio}));
  }
  RETURN_IF_FAIL(out.GetStatus());
  return std::optional<Relation>(std::move(out));
}

}  // namespace tinylamb::relational_detail
