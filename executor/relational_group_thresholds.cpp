/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <array>
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
#include "executor/detail/relation.hpp"
#include "executor/detail/relational_fast_paths.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/window_eval.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/tlcs_file.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

// Lean-backed (IndividualQueryGaps.lean: TwoPassGroupAvgThreshold_sound)
// self-join elimination for group-average threshold queries (TPC-DS Q65):
//   SELECT ... FROM ...,
//     (SELECT k, AVG(v) AS ave FROM (<sa>) GROUP BY k) AS sb,
//     (<sc>) AS sc
//   WHERE sb.k = sc.k AND sc.v <= c * sb.ave AND ...
// where <sa> and <sc> are structurally identical (Fingerprint match).
//
// Evaluates <sc> ONCE, computes per-k (sum, count) in Pass 1, filters rows of
// <sc> against c * (sum / count) in Pass 2, binds the filtered relation as a
// precomputed CTE, drops `sb` and the self-join predicates, and delegates the
// simplified query to ExecuteQuery.
StatusOr<std::optional<Relation>> TryExecuteTwoPassGroupAvgThreshold(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& ctes, bool columnar_output) {
  thread_local int two_pass_depth = 0;
  if (two_pass_depth > 0 ||
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_TWO_PASS_GROUP_AVG") !=
          nullptr) {
    return std::optional<Relation>{};
  }
  if (!statement.UnionAll().empty() ||
      statement.GetSetOperationTree() != nullptr || !statement.WhereClause() ||
      statement.Sources().size() < 2) {
    return std::optional<Relation>{};
  }
  const auto& sources = statement.Sources();
  for (size_t i = 0; i < sources.size(); ++i) {
    if (i > 0 && sources[i].join_type != JoinType::kInner &&
        sources[i].join_type != JoinType::kCross) {
      return std::optional<Relation>{};
    }
  }

  // Find a pair (sb_idx, sc_idx) where sb = SELECT k, AVG(v) FROM (sa) GROUP BY
  // k and sa.Fingerprint() == sc.Fingerprint().
  struct MatchInfo {
    size_t sb_idx{0};
    size_t sc_idx{0};
    std::string sb_alias;
    std::string sc_alias;
    std::string sb_key_name;
    std::string sb_ave_name;
    std::string sc_key_name;
    std::string sc_val_name;
  };
  std::optional<MatchInfo> match;
  for (size_t i = 0; i < sources.size() && !match; ++i) {
    const SelectSource& sbs = sources[i];
    if (!sbs.query || sbs.unnest || sbs.is_lateral || sbs.sample.has_value() ||
        !sbs.using_columns.empty() || sbs.alias.empty()) {
      continue;
    }
    const SelectStatement& sb = *sbs.query;
    if (!sb.WithQueries().empty() || !sb.UnionAll().empty() ||
        sb.GetSetOperationTree() != nullptr || sb.WhereClause() ||
        sb.Having() || sb.Distinct() || sb.HasDistinctOn() || sb.HasLimit() ||
        sb.Offset() != 0 || HasWindowFunctions(sb) ||
        sb.Sources().size() != 1 || !sb.Sources()[0].query ||
        sb.GroupBy().size() != 1 || sb.SelectList().size() != 2) {
      continue;
    }
    const Expression& gb0 = sb.GroupBy()[0];
    if (!gb0 || gb0->Type() != TypeTag::kColumnValue) {
      continue;
    }
    const std::string sa_key_col = gb0->AsColumnValue().GetColumnName().name;
    std::string sb_key_name;
    std::string sb_ave_name;
    std::string sa_val_col;
    bool ok_sel = true;
    for (size_t k = 0; k < 2; ++k) {
      const NamedExpression& ne = sb.SelectList()[k];
      if (!ne.expression) {
        ok_sel = false;
        break;
      }
      if (ne.expression->Type() == TypeTag::kColumnValue &&
          IdentifierEquals(ne.expression->AsColumnValue().GetColumnName().name,
                           sa_key_col) &&
          sb_key_name.empty()) {
        sb_key_name = ProjectionName(ne, k);
      } else if (ne.expression->Type() == TypeTag::kAggregateExp &&
                 sb_ave_name.empty()) {
        const auto& agg = ne.expression->AsAggregateExpression();
        if (agg.GetType() != AggregationType::kAvg || agg.Distinct() ||
            agg.WhereFilter() || !agg.Child() ||
            agg.Child()->Type() != TypeTag::kColumnValue) {
          ok_sel = false;
          break;
        }
        sa_val_col = agg.Child()->AsColumnValue().GetColumnName().name;
        sb_ave_name = ProjectionName(ne, k);
      } else {
        ok_sel = false;
        break;
      }
    }
    if (!ok_sel || sb_key_name.empty() || sb_ave_name.empty() ||
        sa_val_col.empty()) {
      continue;
    }
    const SelectStatement& sa = *sb.Sources()[0].query;
    if (StatementContainsVolatileFunction(sa)) {
      continue;
    }
    const std::string sa_fp = sa.Fingerprint();
    for (size_t j = 0; j < sources.size(); ++j) {
      if (j == i) {
        continue;
      }
      const SelectSource& scs = sources[j];
      if (!scs.query || scs.unnest || scs.is_lateral ||
          scs.sample.has_value() || !scs.using_columns.empty() ||
          scs.alias.empty()) {
        continue;
      }
      if (scs.query->Fingerprint() == sa_fp) {
        match = MatchInfo{.sb_idx = i,
                          .sc_idx = j,
                          .sb_alias = AsciiToLower(sbs.alias),
                          .sc_alias = AsciiToLower(scs.alias),
                          .sb_key_name = AsciiToLower(sb_key_name),
                          .sb_ave_name = AsciiToLower(sb_ave_name),
                          .sc_key_name = AsciiToLower(sa_key_col),
                          .sc_val_name = AsciiToLower(sa_val_col)};
        break;
      }
    }
  }
  if (!match) {
    return std::optional<Relation>{};
  }

  // Locate the equijoin conjunct `sb.k = sc.k` and the threshold conjunct
  // `sc.v <op> ratio * sb.ave` in WHERE.
  const auto conjuncts = SplitConjuncts(statement.WhereClause());
  std::optional<size_t> eq_idx;
  std::optional<size_t> thr_idx;
  BinaryOperation thr_op = BinaryOperation::kLessThanEquals;
  double thr_ratio = 1.0;

  auto is_col_of = [](const Expression& e, std::string_view alias,
                      std::string_view col, bool allow_unqual) -> bool {
    if (!e || e->Type() != TypeTag::kColumnValue) {
      return false;
    }
    const ColumnName& cn = e->AsColumnValue().GetColumnName();
    if (!IdentifierEquals(cn.name, col)) {
      return false;
    }
    if (cn.schema.empty()) {
      return allow_unqual;
    }
    return IdentifierEquals(cn.schema, alias);
  };
  auto match_scaled_ave = [&](const Expression& e, double* ratio_out) -> bool {
    if (is_col_of(e, match->sb_alias, match->sb_ave_name, true)) {
      *ratio_out = 1.0;
      return true;
    }
    if (!e || e->Type() != TypeTag::kBinaryExp ||
        e->AsBinaryExpression().Op() != BinaryOperation::kMultiply) {
      return false;
    }
    const Expression& l = e->AsBinaryExpression().Left();
    const Expression& r = e->AsBinaryExpression().Right();
    auto const_dbl = [](const Expression& c) -> std::optional<double> {
      if (!c || c->Type() != TypeTag::kConstantValue) {
        return std::nullopt;
      }
      const Value& v = c->AsConstantValue().GetValue();
      if (v.IsNull()) {
        return std::nullopt;
      }
      if (v.type == ValueType::kDouble) {
        return v.value.double_value;
      }
      if (v.type == ValueType::kInt64) {
        return static_cast<double>(v.value.int_value);
      }
      return std::nullopt;
    };
    if (auto d = const_dbl(l);
        d && is_col_of(r, match->sb_alias, match->sb_ave_name, true)) {
      *ratio_out = *d;
      return true;
    }
    if (auto d = const_dbl(r);
        d && is_col_of(l, match->sb_alias, match->sb_ave_name, true)) {
      *ratio_out = *d;
      return true;
    }
    return false;
  };
  auto flip_cmp = [](BinaryOperation op) -> std::optional<BinaryOperation> {
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
        return std::nullopt;
    }
  };

  for (size_t ci = 0; ci < conjuncts.size(); ++ci) {
    const Expression& c = conjuncts[ci];
    if (!c || c->Type() != TypeTag::kBinaryExp) {
      continue;
    }
    const auto& be = c->AsBinaryExpression();
    if (be.Op() == BinaryOperation::kEquals && !eq_idx) {
      if ((is_col_of(be.Left(), match->sb_alias, match->sb_key_name, false) &&
           is_col_of(be.Right(), match->sc_alias, match->sc_key_name, false)) ||
          (is_col_of(be.Right(), match->sb_alias, match->sb_key_name, false) &&
           is_col_of(be.Left(), match->sc_alias, match->sc_key_name, false))) {
        eq_idx = ci;
        continue;
      }
    }
    if (!thr_idx && flip_cmp(be.Op()).has_value()) {
      double ratio = 1.0;
      if (is_col_of(be.Left(), match->sc_alias, match->sc_val_name, true) &&
          match_scaled_ave(be.Right(), &ratio)) {
        thr_idx = ci;
        thr_op = be.Op();
        thr_ratio = ratio;
        continue;
      }
      if (is_col_of(be.Right(), match->sc_alias, match->sc_val_name, true) &&
          match_scaled_ave(be.Left(), &ratio)) {
        thr_idx = ci;
        thr_op = *flip_cmp(be.Op());
        thr_ratio = ratio;
        continue;
      }
    }
  }
  if (!eq_idx || !thr_idx) {
    return std::optional<Relation>{};
  }

  // Ensure sb_alias / sb_ave_name are NOT referenced anywhere else in the
  // query (remaining WHERE, ON, SELECT, GROUP BY, HAVING, ORDER BY).
  auto expr_refs_sb = [&](const Expression& e, auto& self) -> bool {
    if (!e) {
      return false;
    }
    if (e->Type() == TypeTag::kColumnValue) {
      const ColumnName& cn = e->AsColumnValue().GetColumnName();
      if (!cn.schema.empty() && IdentifierEquals(cn.schema, match->sb_alias)) {
        return true;
      }
      if (cn.schema.empty() && IdentifierEquals(cn.name, match->sb_ave_name)) {
        return true;
      }
      return false;
    }
    for (const Expression& ch : ExpressionChildren(e)) {
      if (self(ch, self)) {
        return true;
      }
    }
    return false;
  };
  for (size_t ci = 0; ci < conjuncts.size(); ++ci) {
    if (ci == *eq_idx || ci == *thr_idx) {
      continue;
    }
    if (expr_refs_sb(conjuncts[ci], expr_refs_sb)) {
      return std::optional<Relation>{};
    }
  }
  for (const auto& s : sources) {
    if (expr_refs_sb(s.join_condition, expr_refs_sb)) {
      return std::optional<Relation>{};
    }
  }
  for (const auto& ne : statement.SelectList()) {
    if (expr_refs_sb(ne.expression, expr_refs_sb)) {
      return std::optional<Relation>{};
    }
  }
  for (const auto& g : statement.GroupBy()) {
    if (expr_refs_sb(g, expr_refs_sb)) {
      return std::optional<Relation>{};
    }
  }
  if (expr_refs_sb(statement.Having(), expr_refs_sb)) {
    return std::optional<Relation>{};
  }
  for (const auto& ob : statement.OrderBy()) {
    if (expr_refs_sb(ob.expression, expr_refs_sb)) {
      return std::optional<Relation>{};
    }
  }

  // Evaluate `sc` ONCE and apply TwoPassGroupAvgThreshold_sound.
  struct DepthGuard {
    int& d;
    explicit DepthGuard(int& ref) : d(ref) { ++d; }
    DepthGuard(const DepthGuard&) = delete;
    DepthGuard& operator=(const DepthGuard&) = delete;
    DepthGuard(DepthGuard&&) = delete;
    DepthGuard& operator=(DepthGuard&&) = delete;
    ~DepthGuard() { --d; }
  } guard(two_pass_depth);

  ASSIGN_OR_RETURN(Relation, sc_rel,
                   ExecuteQuery(context, *sources[match->sc_idx].query, nullptr,
                                ctes, nullptr, /*columnar_output=*/false));
  if (sc_rel.HasSpill()) {
    return std::optional<Relation>{};
  }
  RETURN_IF_FAIL(sc_rel.EnsureRowsMaterialized());
  auto key_o = LocalColumnOffset(sc_rel.schema, ColumnName(match->sc_key_name));
  auto val_o = LocalColumnOffset(sc_rel.schema, ColumnName(match->sc_val_name));
  if (!key_o || !val_o) {
    return std::optional<Relation>{};
  }
  const ValueType kt = sc_rel.schema.GetColumn(*key_o).Type();
  const ValueType vt = sc_rel.schema.GetColumn(*val_o).Type();
  if ((kt != ValueType::kInt64 && kt != ValueType::kDate) ||
      (vt != ValueType::kDouble && vt != ValueType::kInt64)) {
    return std::optional<Relation>{};
  }

  // Pass 1: accumulate per-key (sum, count) skipping NULL keys and NULL vals.
  struct SumCnt {
    double sum{0.0};
    uint64_t cnt{0};
  };
  FlatIntKeyMap<SumCnt> grp_sc;
  grp_sc.Reserve(std::max<size_t>(16, sc_rel.rows.size() / 4));
  for (const Row& r : sc_rel.rows) {
    const Value& kv = r[*key_o];
    const Value& vv = r[*val_o];
    if (kv.IsNull() || vv.IsNull()) {
      continue;
    }
    const double vd = (vt == ValueType::kDouble)
                          ? vv.value.double_value
                          : static_cast<double>(vv.value.int_value);
    bool emp = false;
    SumCnt* sl = grp_sc.TryEmplace(kv.value.int_value, &emp);
    if (emp) {
      *sl = SumCnt{.sum = vd, .cnt = 1};
    } else {
      sl->sum += vd;
      sl->cnt += 1;
    }
  }

  // Pass 2: filter sc_rel rows where v <op> ratio * (sum / cnt).
  auto filtered = std::make_shared<Relation>(context.execution_runtime());
  filtered->schema = sc_rel.schema;
  filtered->rows.reserve(sc_rel.rows.size() / 4);
  for (Row& r : sc_rel.rows) {
    const Value& kv = r[*key_o];
    const Value& vv = r[*val_o];
    if (kv.IsNull() || vv.IsNull()) {
      continue;
    }
    const SumCnt* sc = grp_sc.Find(kv.value.int_value);
    if (sc == nullptr || sc->cnt == 0) {
      continue;
    }
    const double vd = (vt == ValueType::kDouble)
                          ? vv.value.double_value
                          : static_cast<double>(vv.value.int_value);
    const double rhs = thr_ratio * (sc->sum / static_cast<double>(sc->cnt));
    bool keep = false;
    switch (thr_op) {
      case BinaryOperation::kLessThanEquals:
        keep = (vd <= rhs);
        break;
      case BinaryOperation::kLessThan:
        keep = (vd < rhs);
        break;
      case BinaryOperation::kGreaterThanEquals:
        keep = (vd >= rhs);
        break;
      case BinaryOperation::kGreaterThan:
        keep = (vd > rhs);
        break;
      default:
        break;
    }
    if (keep) {
      filtered->AddRow(std::move(r));
    }
  }
  RETURN_IF_FAIL(filtered->GetStatus());

  // Bind filtered `sc` into a precomputed CTE and simplify the outer query.
  const std::string cte_name = "__two_pass_avg_sc";
  CteMap next_ctes = ctes;
  next_ctes[cte_name] = std::move(filtered);

  SelectStatement simplified = statement;
  std::vector<SelectSource> new_sources;
  new_sources.reserve(sources.size() - 1);
  for (size_t i = 0; i < sources.size(); ++i) {
    if (i == match->sb_idx) {
      continue;
    }
    if (i == match->sc_idx) {
      SelectSource s;
      s.table = cte_name;
      s.alias = sources[i].alias;
      s.join_type =
          new_sources.empty() ? JoinType::kInner : sources[i].join_type;
      new_sources.push_back(std::move(s));
    } else {
      SelectSource s = sources[i];
      if (new_sources.empty()) {
        s.join_type = JoinType::kInner;
      }
      new_sources.push_back(std::move(s));
    }
  }
  simplified.SetSources(std::move(new_sources));

  std::vector<Expression> rem_conj;
  rem_conj.reserve(conjuncts.size() - 2);
  for (size_t ci = 0; ci < conjuncts.size(); ++ci) {
    if (ci == *eq_idx || ci == *thr_idx) {
      continue;
    }
    rem_conj.push_back(conjuncts[ci]);
  }
  simplified.SetWhereClause(rem_conj.empty() ? nullptr
                                             : CombineConjuncts(rem_conj));

  ASSIGN_OR_RETURN(Relation, out,
                   ExecuteQuery(context, simplified, nullptr, next_ctes,
                                nullptr, columnar_output));
  return std::optional<Relation>(std::move(out));
}

// Lean-backed (DuckDBGaps/Orders.lean: DirectionBuckets_sound) fast path for
// 6-table bidirectional nation-pair shipping volume queries (TPC-H Q7):
//   SELECT n1.n_name, n2.n_name, EXTRACT(year FROM l_shipdate),
//          SUM(l_extendedprice * (1 - l_discount))
//   FROM supplier, lineitem, orders, customer, nation n1, nation n2
//   WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey
//     AND c_custkey = o_custkey AND s_nationkey = n1.n_nationkey
//     AND c_nationkey = n2.n_nationkey
//     AND ((n1.n_name = 'A' AND n2.n_name = 'B')
//       OR (n1.n_name = 'B' AND n2.n_name = 'A'))
//     AND l_shipdate BETWEEN lo AND hi
//   GROUP BY n1.n_name, n2.n_name, EXTRACT(year FROM l_shipdate)
//   ORDER BY ...
StatusOr<std::optional<Relation>> TryExecuteDirectionBuckets(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& ctes) {
  if (::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_DIRECTION_BUCKETS") !=
      nullptr) {
    return std::optional<Relation>{};
  }
  if (!statement.WithQueries().empty() || !statement.UnionAll().empty() ||
      statement.GetSetOperationTree() != nullptr || statement.Distinct() ||
      statement.Having() || statement.HasLimit() || statement.Offset() > 0 ||
      HasWindowFunctions(statement) || statement.SelectList().size() != 4 ||
      statement.GroupBy().size() != 3 || statement.Sources().size() != 6) {
    return std::optional<Relation>{};
  }
  const auto& sources = statement.Sources();
  for (size_t i = 0; i < 6; ++i) {
    const auto& s = sources[i];
    if (s.query != nullptr || s.unnest != nullptr || s.is_lateral ||
        s.sample.has_value() || !s.using_columns.empty() || s.table.empty() ||
        ctes.contains(AsciiToLower(s.table)) ||
        (i > 0 && s.join_type != JoinType::kInner &&
         s.join_type != JoinType::kCross)) {
      return std::optional<Relation>{};
    }
  }
  std::vector<std::shared_ptr<Table>> tables(6);
  std::vector<Schema> schemas(6);
  std::vector<std::string> aliases(6);
  for (size_t i = 0; i < 6; ++i) {
    ASSIGN_OR_RETURN(std::shared_ptr<Table>, tbl,
                     context.GetTable(sources[i].table));
    if (!tbl) {
      return std::optional<Relation>{};
    }
    tables[i] = std::move(tbl);
    schemas[i] = tables[i]->GetSchema();
    aliases[i] = AsciiToLower(sources[i].alias.empty() ? sources[i].table
                                                       : sources[i].alias);
  }
  auto resolve_col =
      [&](const ColumnName& cn) -> std::optional<std::pair<size_t, slot_t>> {
    const std::string wn = AsciiToLower(cn.name);
    const std::string ws = AsciiToLower(cn.schema);
    std::optional<std::pair<size_t, slot_t>> found;
    for (size_t i = 0; i < 6; ++i) {
      if (!ws.empty() && aliases[i] != ws &&
          AsciiToLower(sources[i].table) != ws) {
        continue;
      }
      for (slot_t c = 0; c < schemas[i].ColumnCount(); ++c) {
        if (AsciiToLower(schemas[i].GetColumn(c).Name().name) == wn) {
          if (found.has_value()) {
            return std::nullopt;
          }
          found = {i, c};
        }
      }
    }
    return found;
  };

  // Match SELECT[0..2] = (n1.name, n2.name, extract_year(fact.shipdate)) and
  // GROUP BY[0..2] matching the same 3 expressions.
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
  if (!statement.SelectList()[0].expression ||
      statement.SelectList()[0].expression->Type() != TypeTag::kColumnValue ||
      !statement.SelectList()[1].expression ||
      statement.SelectList()[1].expression->Type() != TypeTag::kColumnValue) {
    return std::optional<Relation>{};
  }
  auto n1_nm = resolve_col(
      statement.SelectList()[0].expression->AsColumnValue().GetColumnName());
  auto n2_nm = resolve_col(
      statement.SelectList()[1].expression->AsColumnValue().GetColumnName());
  auto ship_yr = match_extract_year(statement.SelectList()[2].expression);
  if (!n1_nm || !n2_nm || !ship_yr || n1_nm->first == n2_nm->first ||
      n1_nm->first == ship_yr->first || n2_nm->first == ship_yr->first) {
    return std::optional<Relation>{};
  }
  // Verify GROUP BY matches SELECT[0..2] (in any order).
  std::array<bool, 3> gb_has{false, false, false};
  for (const Expression& g : statement.GroupBy()) {
    if (g && g->Type() == TypeTag::kColumnValue) {
      auto gr = resolve_col(g->AsColumnValue().GetColumnName());
      if (gr && *gr == *n1_nm) {
        gb_has[0] = true;
      } else if (gr && *gr == *n2_nm) {
        gb_has[1] = true;
      } else {
        return std::optional<Relation>{};
      }
    } else if (auto gy = match_extract_year(g); gy && *gy == *ship_yr) {
      gb_has[2] = true;
    } else {
      return std::optional<Relation>{};
    }
  }
  if (!gb_has[0] || !gb_has[1] || !gb_has[2]) {
    return std::optional<Relation>{};
  }

  // Match SELECT[3]: SUM(fact.price * (1 - fact.disc)).
  const Expression& s3 = statement.SelectList()[3].expression;
  if (!s3 || s3->Type() != TypeTag::kAggregateExp) {
    return std::optional<Relation>{};
  }
  const auto& agg = s3->AsAggregateExpression();
  if (agg.GetType() != AggregationType::kSum || agg.Distinct() ||
      agg.WhereFilter() || !agg.Child() ||
      agg.Child()->Type() != TypeTag::kBinaryExp ||
      agg.Child()->AsBinaryExpression().Op() != BinaryOperation::kMultiply) {
    return std::optional<Relation>{};
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
    if ((lv.type != ValueType::kInt64 || lv.value.int_value != 1) &&
        (lv.type != ValueType::kDouble || lv.value.double_value != 1.0)) {
      return std::nullopt;
    }
    return resolve_col(r->AsColumnValue().GetColumnName());
  };
  const auto& mul = agg.Child()->AsBinaryExpression();
  std::optional<std::pair<size_t, slot_t>> pr;
  std::optional<std::pair<size_t, slot_t>> dr;
  if (mul.Left()->Type() == TypeTag::kColumnValue) {
    dr = match_one_minus(mul.Right());
    if (dr.has_value()) {
      pr = resolve_col(mul.Left()->AsColumnValue().GetColumnName());
    }
  }
  if (!pr && mul.Right()->Type() == TypeTag::kColumnValue) {
    dr = match_one_minus(mul.Left());
    if (dr.has_value()) {
      pr = resolve_col(mul.Right()->AsColumnValue().GetColumnName());
    }
  }
  const size_t fact_idx = ship_yr->first;
  const slot_t fact_date_slot = ship_yr->second;
  if (!pr || !dr || pr->first != fact_idx || dr->first != fact_idx) {
    return std::optional<Relation>{};
  }
  const slot_t fact_price_slot = pr->second;
  const slot_t fact_disc_slot = dr->second;
  const size_t n1_idx = n1_nm->first;
  const slot_t n1_name_slot = n1_nm->second;
  const size_t n2_idx = n2_nm->first;
  const slot_t n2_name_slot = n2_nm->second;

  // Partition WHERE + ON conjuncts into:
  //   - 5 equijoin edges
  //   - date bounds on fact.shipdate
  //   - symmetric OR: (n1.name = A AND n2.name = B) OR (n1.name = B AND
  //     n2.name = A) with A != B
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

  struct EqEdge {
    size_t src1{0};
    slot_t c1{0};
    size_t src2{0};
    slot_t c2{0};
  };
  std::vector<EqEdge> eq_edges;
  int64_t date_lo = std::numeric_limits<int64_t>::min();
  int64_t date_hi = std::numeric_limits<int64_t>::max();
  std::optional<std::pair<std::string, std::string>> nation_pair;

  auto match_name_eq = [&](const Expression& e, size_t want_src,
                           slot_t want_col) -> std::optional<std::string> {
    if (!e || e->Type() != TypeTag::kBinaryExp ||
        e->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
      return std::nullopt;
    }
    const auto& be = e->AsBinaryExpression();
    Expression ce;
    Expression le;
    if (be.Left()->Type() == TypeTag::kColumnValue &&
        be.Right()->Type() == TypeTag::kConstantValue) {
      ce = be.Left();
      le = be.Right();
    } else if (be.Right()->Type() == TypeTag::kColumnValue &&
               be.Left()->Type() == TypeTag::kConstantValue) {
      ce = be.Right();
      le = be.Left();
    } else {
      return std::nullopt;
    }
    auto r = resolve_col(ce->AsColumnValue().GetColumnName());
    const Value& v = le->AsConstantValue().GetValue();
    if (!r || r->first != want_src || r->second != want_col || v.IsNull() ||
        v.type != ValueType::kVarChar) {
      return std::nullopt;
    }
    return std::string(v.value.varchar_value);
  };
  auto match_and_pair = [&](const Expression& e)
      -> std::optional<std::pair<std::string, std::string>> {
    auto cs = SplitConjuncts(e);
    if (cs.size() != 2) {
      return std::nullopt;
    }
    if (auto a = match_name_eq(cs[0], n1_idx, n1_name_slot)) {
      if (auto b = match_name_eq(cs[1], n2_idx, n2_name_slot)) {
        return std::pair{*a, *b};
      }
    }
    if (auto a = match_name_eq(cs[1], n1_idx, n1_name_slot)) {
      if (auto b = match_name_eq(cs[0], n2_idx, n2_name_slot)) {
        return std::pair{*a, *b};
      }
    }
    return std::nullopt;
  };

  for (const Expression& conj : all_conj) {
    if (!conj || conj->Type() != TypeTag::kBinaryExp) {
      return std::optional<Relation>{};
    }
    const auto& be = conj->AsBinaryExpression();
    if (be.Op() == BinaryOperation::kOr && !nation_pair) {
      auto p1 = match_and_pair(be.Left());
      auto p2 = match_and_pair(be.Right());
      if (p1 && p2 && p1->first == p2->second && p1->second == p2->first &&
          p1->first != p1->second) {
        nation_pair = *p1;
        continue;
      }
      return std::optional<Relation>{};
    }
    if (be.Op() == BinaryOperation::kEquals &&
        be.Left()->Type() == TypeTag::kColumnValue &&
        be.Right()->Type() == TypeTag::kColumnValue) {
      auto l = resolve_col(be.Left()->AsColumnValue().GetColumnName());
      auto r = resolve_col(be.Right()->AsColumnValue().GetColumnName());
      if (!l || !r || l->first == r->first) {
        return std::optional<Relation>{};
      }
      eq_edges.push_back(EqEdge{.src1 = l->first,
                                .c1 = l->second,
                                .src2 = r->first,
                                .c2 = r->second});
      continue;
    }
    // Date bound on fact.shipdate.
    auto try_date = [&](const Expression& c_e, const Expression& v_e,
                        BinaryOperation op) -> bool {
      if (!c_e || !v_e || c_e->Type() != TypeTag::kColumnValue ||
          v_e->Type() != TypeTag::kConstantValue) {
        return false;
      }
      auto cr = resolve_col(c_e->AsColumnValue().GetColumnName());
      const Value& v = v_e->AsConstantValue().GetValue();
      if (!cr || cr->first != fact_idx || cr->second != fact_date_slot ||
          v.IsNull() || v.type != ValueType::kDate) {
        return false;
      }
      const int64_t d = v.value.int_value;
      switch (op) {
        case BinaryOperation::kGreaterThanEquals:
          date_lo = std::max(date_lo, d);
          return true;
        case BinaryOperation::kGreaterThan:
          date_lo = std::max(date_lo, d + 1);
          return true;
        case BinaryOperation::kLessThanEquals:
          date_hi = std::min(date_hi, d);
          return true;
        case BinaryOperation::kLessThan:
          date_hi = std::min(date_hi, d - 1);
          return true;
        case BinaryOperation::kEquals:
          date_lo = std::max(date_lo, d);
          date_hi = std::min(date_hi, d);
          return true;
        default:
          return false;
      }
    };
    auto flip = [](BinaryOperation op) -> BinaryOperation {
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
    if (!try_date(be.Left(), be.Right(), be.Op()) &&
        !try_date(be.Right(), be.Left(), flip(be.Op()))) {
      return std::optional<Relation>{};
    }
  }
  if (!nation_pair || eq_edges.size() != 5) {
    return std::optional<Relation>{};
  }

  // Verify exact 6-node chain: n1 -- sup -- fact -- ord -- cust -- n2.
  std::vector<std::vector<std::tuple<size_t, slot_t, slot_t>>> adj(6);
  for (const EqEdge& e : eq_edges) {
    if (schemas[e.src1].GetColumn(e.c1).Type() != ValueType::kInt64 ||
        schemas[e.src2].GetColumn(e.c2).Type() != ValueType::kInt64) {
      return std::optional<Relation>{};
    }
    adj[e.src1].emplace_back(e.src2, e.c1, e.c2);
    adj[e.src2].emplace_back(e.src1, e.c2, e.c1);
  }
  if (adj[n1_idx].size() != 1 || adj[n2_idx].size() != 1 ||
      adj[fact_idx].size() != 2) {
    return std::optional<Relation>{};
  }
  auto next_hop =
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
  const auto [sup_idx, n1_pk_slot, sup_n1_slot] = adj[n1_idx][0];
  auto sup_f = next_hop(sup_idx, n1_idx);
  if (!sup_f || std::get<0>(*sup_f) != fact_idx) {
    return std::optional<Relation>{};
  }
  const slot_t sup_pk_slot = std::get<1>(*sup_f);
  const slot_t fact_sup_slot = std::get<2>(*sup_f);

  auto f_ord = next_hop(fact_idx, sup_idx);
  if (!f_ord) {
    return std::optional<Relation>{};
  }
  const size_t ord_idx = std::get<0>(*f_ord);
  const slot_t fact_ord_slot = std::get<1>(*f_ord);
  const slot_t ord_pk_slot = std::get<2>(*f_ord);

  auto o_cust = next_hop(ord_idx, fact_idx);
  if (!o_cust) {
    return std::optional<Relation>{};
  }
  const size_t cust_idx = std::get<0>(*o_cust);
  const slot_t ord_cust_slot = std::get<1>(*o_cust);
  const slot_t cust_pk_slot = std::get<2>(*o_cust);

  auto c_n2 = next_hop(cust_idx, ord_idx);
  if (!c_n2 || std::get<0>(*c_n2) != n2_idx) {
    return std::optional<Relation>{};
  }
  const slot_t cust_n2_slot = std::get<1>(*c_n2);
  const slot_t n2_pk_slot = std::get<2>(*c_n2);

  // Check ORDER BY: only allow empty or (supp_nation ASC, cust_nation ASC,
  // l_year ASC).
  if (!statement.OrderBy().empty()) {
    if (statement.OrderBy().size() != 3) {
      return std::optional<Relation>{};
    }
    const std::string p0 = ProjectionName(statement.SelectList()[0], 0);
    const std::string p1 = ProjectionName(statement.SelectList()[1], 1);
    const std::string p2 = ProjectionName(statement.SelectList()[2], 2);
    for (size_t k = 0; k < 3; ++k) {
      if (!statement.OrderBy()[k].ascending) {
        return std::optional<Relation>{};
      }
      const Expression& oe = statement.OrderBy()[k].expression;
      bool ok = false;
      if (k == 0 && oe && oe->Type() == TypeTag::kColumnValue) {
        const auto& cn = oe->AsColumnValue().GetColumnName();
        auto r = resolve_col(cn);
        ok = (r && *r == *n1_nm) || IdentifierEquals(cn.name, p0);
      } else if (k == 1 && oe && oe->Type() == TypeTag::kColumnValue) {
        const auto& cn = oe->AsColumnValue().GetColumnName();
        auto r = resolve_col(cn);
        ok = (r && *r == *n2_nm) || IdentifierEquals(cn.name, p1);
      } else if (k == 2 && oe) {
        const auto yr = match_extract_year(oe);
        ok = (yr && *yr == *ship_yr) ||
             (oe->Type() == TypeTag::kColumnValue &&
              IdentifierEquals(oe->AsColumnValue().GetColumnName().name, p2));
      }
      if (!ok) {
        return std::optional<Relation>{};
      }
    }
  }

  auto make_empty = [&]() -> Relation {
    Relation out(context.execution_runtime());
    out.schema =
        Schema("", {Column(ProjectionName(statement.SelectList()[0], 0),
                           ValueType::kVarChar),
                    Column(ProjectionName(statement.SelectList()[1], 1),
                           ValueType::kVarChar),
                    Column(ProjectionName(statement.SelectList()[2], 2),
                           ValueType::kInt64),
                    Column(ProjectionName(statement.SelectList()[3], 3),
                           ValueType::kDouble)});
    return out;
  };

  struct SideCnt {
    uint32_t a{0};
    uint32_t b{0};
  };
  const std::string& nameA = nation_pair->first;
  const std::string& nameB = nation_pair->second;

  // Step 1: Load n1 and n2 into (cntA, cntB) maps.
  auto load_nation = [&](size_t idx, slot_t pk_s,
                         slot_t nm_s) -> std::optional<FlatIntKeyMap<SideCnt>> {
    std::vector<slot_t> cols{pk_s, nm_s};
    std::ranges::sort(cols);
    cols.erase(std::ranges::unique(cols).begin(), cols.end());
    auto st =
        GetOrBuildLSTableStore(context.txn_, *tables[idx], true, &cols, false);
    if (!st || !st->HasColumn(pk_s) || !st->HasColumn(nm_s)) {
      return std::nullopt;
    }
    FlatIntKeyMap<SideCnt> m;
    for (const LSTablePageBlock& blk : st->blocks) {
      const auto& c_pk = blk.columns[pk_s];
      const auto& c_nm = blk.columns[nm_s];
      for (uint32_t r = 0; r < blk.row_count; ++r) {
        if ((c_pk.has_null && c_pk.IsNull(r)) ||
            (c_nm.has_null && c_nm.IsNull(r))) {
          continue;
        }
        const std::string_view sv = c_nm.VarcharAt(r);
        const bool is_a = (sv == nameA);
        const bool is_b = (sv == nameB);
        if (!is_a && !is_b) {
          continue;
        }
        bool emp = false;
        SideCnt* sl = m.TryEmplace(c_pk.IntAt(r), &emp);
        if (emp) {
          *sl = SideCnt{.a = 0, .b = 0};
        }
        sl->a += is_a ? 1U : 0U;
        sl->b += is_b ? 1U : 0U;
      }
    }
    return m;
  };
  auto n1_map = load_nation(n1_idx, n1_pk_slot, n1_name_slot);
  auto n2_map = load_nation(n2_idx, n2_pk_slot, n2_name_slot);
  if (!n1_map || !n2_map) {
    return std::optional<Relation>{};
  }
  if (n1_map->Size() == 0 || n2_map->Size() == 0) {
    return std::optional<Relation>(make_empty());
  }

  // Helper: map a table (pk, fk) through upstream SideCnt map into a dense
  // array or FlatIntKeyMap.
  struct SideLookup {
    int64_t min_k{std::numeric_limits<int64_t>::max()};
    int64_t max_k{std::numeric_limits<int64_t>::min()};
    std::vector<SideCnt> arr;
    std::vector<uint64_t> bits;
    FlatIntKeyMap<SideCnt> map;
    [[nodiscard]] SideCnt Get(int64_t k) const noexcept {
      if (k < min_k || k > max_k) {
        return SideCnt{.a = 0, .b = 0};
      }
      const auto off = static_cast<size_t>(k - min_k);
      if (!arr.empty()) {
        return arr[off];
      }
      if (!bits.empty() &&
          (bits[off >> 6] & (uint64_t{1} << (off & 63))) == 0) {
        return SideCnt{.a = 0, .b = 0};
      }
      const SideCnt* p = map.Find(k);
      return p != nullptr ? *p : SideCnt{.a = 0, .b = 0};
    }
  };
  auto build_side = [&](size_t idx, slot_t pk_s, slot_t fk_s,
                        auto&& fk_lookup) -> std::optional<SideLookup> {
    std::vector<slot_t> cols{pk_s, fk_s};
    std::ranges::sort(cols);
    cols.erase(std::ranges::unique(cols).begin(), cols.end());
    auto st =
        GetOrBuildLSTableStore(context.txn_, *tables[idx], true, &cols, false);
    if (!st || !st->HasColumn(pk_s) || !st->HasColumn(fk_s)) {
      return std::nullopt;
    }
    const size_t nb = st->blocks.size();
    const size_t nw = std::max<size_t>(1, ParallelWorkerCount(nb));
    std::vector<std::vector<std::pair<int64_t, SideCnt>>> wout(nw);
    auto scan = [&](size_t w) {
      auto& out = wout[w];
      for (size_t b = w; b < nb; b += nw) {
        const LSTablePageBlock& blk = st->blocks[b];
        const auto& c_pk = blk.columns[pk_s];
        const auto& c_fk = blk.columns[fk_s];
        for (uint32_t r = 0; r < blk.row_count; ++r) {
          if ((c_pk.has_null && c_pk.IsNull(r)) ||
              (c_fk.has_null && c_fk.IsNull(r))) {
            continue;
          }
          const SideCnt sc = fk_lookup(c_fk.IntAt(r));
          if (sc.a == 0 && sc.b == 0) {
            continue;
          }
          out.emplace_back(c_pk.IntAt(r), sc);
        }
      }
    };
    if (RunParallelTasks(nw, scan) != Status::kSuccess) {
      return std::nullopt;
    }
    SideLookup res;
    size_t tot = 0;
    for (const auto& v : wout) {
      tot += v.size();
    }
    if (tot == 0) {
      return res;
    }
    for (const auto& v : wout) {
      for (const auto& [k, sc] : v) {
        res.min_k = std::min(res.min_k, k);
        res.max_k = std::max(res.max_k, k);
      }
    }
    const uint64_t span = static_cast<uint64_t>(res.max_k - res.min_k) + 1;
    if (span <= 1000000 && span <= tot * 4) {
      res.arr.assign(static_cast<size_t>(span), SideCnt{.a = 0, .b = 0});
      for (const auto& v : wout) {
        for (const auto& [k, sc] : v) {
          SideCnt& d = res.arr[static_cast<size_t>(k - res.min_k)];
          d.a += sc.a;
          d.b += sc.b;
        }
      }
    } else {
      res.map.Reserve(tot);
      if (span <= 64000000) {
        res.bits.assign(static_cast<size_t>((span + 63) >> 6), 0);
      }
      for (const auto& v : wout) {
        for (const auto& [k, sc] : v) {
          if (!res.bits.empty()) {
            const auto off = static_cast<size_t>(k - res.min_k);
            res.bits[off >> 6] |= (uint64_t{1} << (off & 63));
          }
          bool emp = false;
          SideCnt* sl = res.map.TryEmplace(k, &emp);
          if (emp) {
            *sl = sc;
          } else {
            sl->a += sc.a;
            sl->b += sc.b;
          }
        }
      }
    }
    return res;
  };

  auto sup_side = build_side(
      sup_idx, sup_pk_slot, sup_n1_slot, [&](int64_t nk) noexcept -> SideCnt {
        const SideCnt* p = n1_map->Find(nk);
        return p != nullptr ? *p : SideCnt{.a = 0, .b = 0};
      });
  auto cust_side =
      build_side(cust_idx, cust_pk_slot, cust_n2_slot,
                 [&](int64_t nk) noexcept -> SideCnt {
                   const SideCnt* p = n2_map->Find(nk);
                   return p != nullptr ? *p : SideCnt{.a = 0, .b = 0};
                 });
  if (!sup_side || !cust_side) {
    return std::optional<Relation>{};
  }
  if (sup_side->min_k > sup_side->max_k ||
      cust_side->min_k > cust_side->max_k) {
    return std::optional<Relation>(make_empty());
  }
  auto ord_side = build_side(
      ord_idx, ord_pk_slot, ord_cust_slot,
      [&](int64_t ck) noexcept -> SideCnt { return cust_side->Get(ck); });
  if (!ord_side) {
    return std::optional<Relation>{};
  }
  if (ord_side->min_k > ord_side->max_k) {
    return std::optional<Relation>(make_empty());
  }

  // Step 2: Parallel scan of lineitem with l_shipdate zone-map pruning and
  // DirectionBuckets_sound directed (A->B, B->A) accumulation.
  std::vector<slot_t> lcols{fact_sup_slot, fact_ord_slot, fact_date_slot,
                            fact_price_slot, fact_disc_slot};
  std::ranges::sort(lcols);
  lcols.erase(std::ranges::unique(lcols).begin(), lcols.end());
  auto l_store = GetOrBuildLSTableStore(context.txn_, *tables[fact_idx], true,
                                        &lcols, false);
  if (!l_store) {
    return std::optional<Relation>{};
  }
  for (slot_t c : lcols) {
    if (!l_store->HasColumn(c)) {
      return std::optional<Relation>{};
    }
  }
  const bool price_dbl =
      schemas[fact_idx].GetColumn(fact_price_slot).Type() == ValueType::kDouble;
  const bool disc_dbl =
      schemas[fact_idx].GetColumn(fact_disc_slot).Type() == ValueType::kDouble;
  auto dbl_at = [](const LSTableColumnBlock& col, uint32_t r,
                   bool is_d) noexcept -> double {
    const int64_t bits = col.IntAt(r);
    if (!is_d) {
      return static_cast<double>(bits);
    }
    double d = 0.0;
    std::memcpy(&d, &bits, sizeof(d));
    return d;
  };

  struct RevBucket {
    double sum{0.0};
    uint64_t non_null_cnt{0};
    bool present{false};
  };
  const size_t l_blocks = l_store->blocks.size();
  const size_t l_workers = std::max<size_t>(1, ParallelWorkerCount(l_blocks));
  std::vector<std::array<std::map<int64_t, RevBucket>, 2>> wb(l_workers);
  auto scan_l = [&](size_t w) {
    auto& loc = wb[w];
    for (size_t b = w; b < l_blocks; b += l_workers) {
      const LSTablePageBlock& blk = l_store->blocks[b];
      const uint32_t nr = blk.row_count;
      if (nr == 0) {
        continue;
      }
      const auto& c_dt = blk.columns[fact_date_slot];
      if (c_dt.has_min_max &&
          (c_dt.max_int < date_lo || c_dt.min_int > date_hi)) {
        continue;
      }
      const auto& c_sk = blk.columns[fact_sup_slot];
      const auto& c_ok = blk.columns[fact_ord_slot];
      const auto& c_pr = blk.columns[fact_price_slot];
      const auto& c_ds = blk.columns[fact_disc_slot];
      for (uint32_t r = 0; r < nr; ++r) {
        if ((c_dt.has_null && c_dt.IsNull(r)) ||
            (c_sk.has_null && c_sk.IsNull(r)) ||
            (c_ok.has_null && c_ok.IsNull(r))) {
          continue;
        }
        const int64_t dt = c_dt.IntAt(r);
        if (dt < date_lo || dt > date_hi) {
          continue;
        }
        const SideCnt ss = sup_side->Get(c_sk.IntAt(r));
        if (ss.a == 0 && ss.b == 0) {
          continue;
        }
        const SideCnt os = ord_side->Get(c_ok.IntAt(r));
        const uint64_t w_ab = static_cast<uint64_t>(ss.a) * os.b;
        const uint64_t w_ba = static_cast<uint64_t>(ss.b) * os.a;
        if (w_ab == 0 && w_ba == 0) {
          continue;
        }
        const std::chrono::year_month_day ymd{
            std::chrono::sys_days{std::chrono::days{static_cast<int>(dt)}}};
        const int64_t yr = static_cast<int64_t>(static_cast<int>(ymd.year()));
        const bool v_null = (c_pr.has_null && c_pr.IsNull(r)) ||
                            (c_ds.has_null && c_ds.IsNull(r));
        const double vol = v_null ? 0.0
                                  : dbl_at(c_pr, r, price_dbl) *
                                        (1.0 - dbl_at(c_ds, r, disc_dbl));
        if (w_ab > 0) {
          RevBucket& dst = loc[0][yr];
          dst.present = true;
          if (!v_null) {
            dst.sum += static_cast<double>(w_ab) * vol;
            dst.non_null_cnt += w_ab;
          }
        }
        if (w_ba > 0) {
          RevBucket& dst = loc[1][yr];
          dst.present = true;
          if (!v_null) {
            dst.sum += static_cast<double>(w_ba) * vol;
            dst.non_null_cnt += w_ba;
          }
        }
      }
    }
  };
  if (RunParallelTasks(l_workers, scan_l) != Status::kSuccess) {
    return std::optional<Relation>{};
  }

  std::array<std::map<int64_t, RevBucket>, 2> merged;
  for (const auto& loc : wb) {
    for (size_t dir = 0; dir < 2; ++dir) {
      for (const auto& [yr, rb] : loc[dir]) {
        RevBucket& dst = merged[dir][yr];
        dst.present = true;
        dst.sum += rb.sum;
        dst.non_null_cnt += rb.non_null_cnt;
      }
    }
  }

  struct OutRow {
    std::string s_nat;
    std::string c_nat;
    int64_t yr{0};
    Value rev;
  };
  std::vector<OutRow> rows;
  for (size_t dir = 0; dir < 2; ++dir) {
    const std::string& fn = (dir == 0) ? nameA : nameB;
    const std::string& tn = (dir == 0) ? nameB : nameA;
    for (const auto& [yr, rb] : merged[dir]) {
      if (!rb.present) {
        continue;
      }
      rows.push_back(
          OutRow{.s_nat = fn,
                 .c_nat = tn,
                 .yr = yr,
                 .rev = rb.non_null_cnt > 0 ? Value(rb.sum) : Value()});
    }
  }
  std::ranges::sort(rows, [](const OutRow& a, const OutRow& b) {
    if (a.s_nat != b.s_nat) {
      return a.s_nat < b.s_nat;
    }
    if (a.c_nat != b.c_nat) {
      return a.c_nat < b.c_nat;
    }
    return a.yr < b.yr;
  });
  Relation out = make_empty();
  out.rows.reserve(rows.size());
  for (OutRow& r : rows) {
    out.AddRow(Row({Value(std::move(r.s_nat)), Value(std::move(r.c_nat)),
                    Value(r.yr), r.rev}));
  }
  RETURN_IF_FAIL(out.GetStatus());
  return std::optional<Relation>(std::move(out));
}

}  // namespace tinylamb::relational_detail
