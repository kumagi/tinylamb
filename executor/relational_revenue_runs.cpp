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
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/parallel_work.hpp"
#include "common/platform/process.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "database/transaction_context.hpp"
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

// Lean-backed (DuckDBGaps/Orders.lean: RevenueRuns_sound) fast path for
// 3-table shipping-priority revenue Top-K queries (TPC-H Q3):
//   SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue,
//          o_orderdate, o_shippriority
//   FROM customer, orders, lineitem
//   WHERE c_mktsegment = 'X' AND c_custkey = o_custkey
//     AND l_orderkey = o_orderkey AND o_orderdate < d1 AND l_shipdate > d2
//   GROUP BY l_orderkey, o_orderdate, o_shippriority
//   ORDER BY revenue DESC, o_orderdate ASC LIMIT K
//
// Exploits contiguous l_orderkey runs per morsel (zero hash-table lookups for
// interior runs!) + exact `mergeBoundary` across morsels (RevenueRuns_sound).
StatusOr<std::optional<Relation>> TryExecuteContiguousRevenueRuns(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& ctes) {
  if (::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_REVENUE_RUNS") !=
      nullptr) {
    return std::optional<Relation>{};
  }
  if (!statement.WithQueries().empty() || !statement.UnionAll().empty() ||
      statement.GetSetOperationTree() != nullptr || statement.Distinct() ||
      statement.Having() || !statement.HasLimit() || statement.Limit() == 0 ||
      statement.Limit() > 4096 || statement.Offset() > 0 ||
      statement.WithTies() || HasWindowFunctions(statement) ||
      !statement.WhereClause() || statement.Sources().size() != 3 ||
      statement.GroupBy().size() != 3 || statement.SelectList().size() != 4) {
    return std::optional<Relation>{};
  }
  const auto& sources = statement.Sources();
  for (size_t i = 0; i < 3; ++i) {
    const auto& s = sources[i];
    if (s.query != nullptr || s.unnest != nullptr || s.is_lateral ||
        s.sample.has_value() || !s.using_columns.empty() || s.table.empty() ||
        ctes.contains(AsciiToLower(s.table)) ||
        (i > 0 && s.join_type != JoinType::kInner &&
         s.join_type != JoinType::kCross)) {
      return std::optional<Relation>{};
    }
  }
  std::vector<std::shared_ptr<Table>> tables(3);
  std::vector<Schema> schemas(3);
  std::vector<std::string> aliases(3);
  for (size_t i = 0; i < 3; ++i) {
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
    for (size_t i = 0; i < 3; ++i) {
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

  // Match SELECT: (l_orderkey, SUM(price*(1-disc)), o_orderdate,
  // o_shippriority).
  if (!statement.SelectList()[0].expression ||
      statement.SelectList()[0].expression->Type() != TypeTag::kColumnValue ||
      !statement.SelectList()[1].expression ||
      statement.SelectList()[1].expression->Type() != TypeTag::kAggregateExp ||
      !statement.SelectList()[2].expression ||
      statement.SelectList()[2].expression->Type() != TypeTag::kColumnValue ||
      !statement.SelectList()[3].expression ||
      statement.SelectList()[3].expression->Type() != TypeTag::kColumnValue) {
    return std::optional<Relation>{};
  }
  auto s0_r = resolve_col(
      statement.SelectList()[0].expression->AsColumnValue().GetColumnName());
  auto s2_r = resolve_col(
      statement.SelectList()[2].expression->AsColumnValue().GetColumnName());
  auto s3_r = resolve_col(
      statement.SelectList()[3].expression->AsColumnValue().GetColumnName());
  if (!s0_r || !s2_r || !s3_r || s2_r->first != s3_r->first ||
      s0_r->first == s2_r->first) {
    return std::optional<Relation>{};
  }
  const size_t line_idx = s0_r->first;
  const slot_t l_ok_slot = s0_r->second;
  const size_t ord_idx = s2_r->first;
  const slot_t o_dt_slot = s2_r->second;
  const slot_t o_sp_slot = s3_r->second;
  const size_t cust_idx = 3 - line_idx - ord_idx;
  if (schemas[line_idx].GetColumn(l_ok_slot).Type() != ValueType::kInt64 ||
      schemas[ord_idx].GetColumn(o_dt_slot).Type() != ValueType::kDate ||
      schemas[ord_idx].GetColumn(o_sp_slot).Type() != ValueType::kInt64) {
    return std::optional<Relation>{};
  }

  // Verify GROUP BY matches (l_orderkey, o_orderdate, o_shippriority).
  std::array<bool, 3> gb_ok{false, false, false};
  for (const Expression& g : statement.GroupBy()) {
    if (!g || g->Type() != TypeTag::kColumnValue) {
      return std::optional<Relation>{};
    }
    auto gr = resolve_col(g->AsColumnValue().GetColumnName());
    if (gr && *gr == *s0_r) {
      gb_ok[0] = true;
    } else if (gr && *gr == *s2_r) {
      gb_ok[1] = true;
    } else if (gr && *gr == *s3_r) {
      gb_ok[2] = true;
    } else {
      return std::optional<Relation>{};
    }
  }
  if (!gb_ok[0] || !gb_ok[1] || !gb_ok[2]) {
    return std::optional<Relation>{};
  }

  // Match SUM(l_extendedprice * (1 - l_discount)).
  const auto& agg =
      statement.SelectList()[1].expression->AsAggregateExpression();
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
  if (!pr || !dr || pr->first != line_idx || dr->first != line_idx) {
    return std::optional<Relation>{};
  }
  const slot_t l_pr_slot = pr->second;
  const slot_t l_ds_slot = dr->second;

  // Check ORDER BY: (revenue DESC, o_orderdate ASC).
  if (statement.OrderBy().size() != 2 || statement.OrderBy()[0].ascending ||
      !statement.OrderBy()[1].ascending) {
    return std::optional<Relation>{};
  }
  const std::string rev_name = ProjectionName(statement.SelectList()[1], 1);
  const std::string dt_name = ProjectionName(statement.SelectList()[2], 2);
  if (!statement.OrderBy()[0].expression ||
      statement.OrderBy()[0].expression->Type() != TypeTag::kColumnValue ||
      !IdentifierEquals(statement.OrderBy()[0]
                            .expression->AsColumnValue()
                            .GetColumnName()
                            .name,
                        rev_name)) {
    return std::optional<Relation>{};
  }
  if (!statement.OrderBy()[1].expression ||
      statement.OrderBy()[1].expression->Type() != TypeTag::kColumnValue) {
    return std::optional<Relation>{};
  }
  {
    const auto& cn =
        statement.OrderBy()[1].expression->AsColumnValue().GetColumnName();
    auto r = resolve_col(cn);
    if ((!r || *r != *s2_r) && !IdentifierEquals(cn.name, dt_name)) {
      return std::optional<Relation>{};
    }
  }

  // Parse WHERE + ON conjuncts:
  //   c_custkey = o_custkey, l_orderkey = o_orderkey,
  //   c_mktsegment = 'X', o_orderdate < d1, l_shipdate > d2.
  std::optional<slot_t> c_pk_slot;
  std::optional<slot_t> o_ck_slot;
  std::optional<slot_t> o_pk_slot;
  std::optional<slot_t> c_flt_slot;
  std::string c_flt_val;
  int64_t o_dt_hi = std::numeric_limits<int64_t>::max();
  std::optional<slot_t> l_sd_slot;
  int64_t l_sd_lo = std::numeric_limits<int64_t>::min();

  std::vector<Expression> all_conj = SplitConjuncts(statement.WhereClause());
  for (const auto& s : sources) {
    if (s.join_condition) {
      auto jc = SplitConjuncts(s.join_condition);
      all_conj.insert(all_conj.end(), jc.begin(), jc.end());
    }
  }
  for (const Expression& conj : all_conj) {
    if (!conj || conj->Type() != TypeTag::kBinaryExp) {
      return std::optional<Relation>{};
    }
    const auto& be = conj->AsBinaryExpression();
    if (be.Op() == BinaryOperation::kEquals &&
        be.Left()->Type() == TypeTag::kColumnValue &&
        be.Right()->Type() == TypeTag::kColumnValue) {
      auto l = resolve_col(be.Left()->AsColumnValue().GetColumnName());
      auto r = resolve_col(be.Right()->AsColumnValue().GetColumnName());
      if (!l || !r || l->first == r->first) {
        return std::optional<Relation>{};
      }
      if ((l->first == cust_idx && r->first == ord_idx) ||
          (r->first == cust_idx && l->first == ord_idx)) {
        if (r->first == cust_idx) {
          std::swap(l, r);
        }
        c_pk_slot = l->second;
        o_ck_slot = r->second;
        continue;
      }
      if ((l->first == line_idx && r->first == ord_idx) ||
          (r->first == line_idx && l->first == ord_idx)) {
        if (r->first == line_idx) {
          std::swap(l, r);
        }
        if (l->second != l_ok_slot) {
          return std::optional<Relation>{};
        }
        o_pk_slot = r->second;
        continue;
      }
      return std::optional<Relation>{};
    }
    Expression ce;
    Expression le;
    BinaryOperation op = be.Op();
    if (be.Left()->Type() == TypeTag::kColumnValue &&
        be.Right()->Type() == TypeTag::kConstantValue) {
      ce = be.Left();
      le = be.Right();
    } else if (be.Right()->Type() == TypeTag::kColumnValue &&
               be.Left()->Type() == TypeTag::kConstantValue) {
      ce = be.Right();
      le = be.Left();
      if (op == BinaryOperation::kLessThan) {
        op = BinaryOperation::kGreaterThan;
      } else if (op == BinaryOperation::kGreaterThan) {
        op = BinaryOperation::kLessThan;
      } else if (op == BinaryOperation::kLessThanEquals) {
        op = BinaryOperation::kGreaterThanEquals;
      } else if (op == BinaryOperation::kGreaterThanEquals) {
        op = BinaryOperation::kLessThanEquals;
      }
    } else {
      return std::optional<Relation>{};
    }
    auto cr = resolve_col(ce->AsColumnValue().GetColumnName());
    const Value& lv = le->AsConstantValue().GetValue();
    if (!cr || lv.IsNull()) {
      return std::optional<Relation>{};
    }
    if (cr->first == cust_idx && op == BinaryOperation::kEquals &&
        lv.type == ValueType::kVarChar && !c_flt_slot) {
      c_flt_slot = cr->second;
      c_flt_val = std::string(lv.value.varchar_value);
      continue;
    }
    if (cr->first == ord_idx && cr->second == o_dt_slot &&
        lv.type == ValueType::kDate &&
        (op == BinaryOperation::kLessThan ||
         op == BinaryOperation::kLessThanEquals)) {
      o_dt_hi = std::min(o_dt_hi, op == BinaryOperation::kLessThan
                                      ? (lv.value.int_value - 1)
                                      : lv.value.int_value);
      continue;
    }
    if (cr->first == line_idx &&
        schemas[line_idx].GetColumn(cr->second).Type() == ValueType::kDate &&
        lv.type == ValueType::kDate &&
        (op == BinaryOperation::kGreaterThan ||
         op == BinaryOperation::kGreaterThanEquals)) {
      if (l_sd_slot && *l_sd_slot != cr->second) {
        return std::optional<Relation>{};
      }
      l_sd_slot = cr->second;
      l_sd_lo = std::max(l_sd_lo, op == BinaryOperation::kGreaterThan
                                      ? (lv.value.int_value + 1)
                                      : lv.value.int_value);
      continue;
    }
    return std::optional<Relation>{};
  }
  if (!c_pk_slot || !o_ck_slot || !o_pk_slot || !c_flt_slot || !l_sd_slot) {
    return std::optional<Relation>{};
  }

  auto make_empty = [&]() -> Relation {
    Relation out(context.execution_runtime());
    out.schema =
        Schema("", {Column(ProjectionName(statement.SelectList()[0], 0),
                           ValueType::kInt64),
                    Column(rev_name, ValueType::kDouble),
                    Column(dt_name, ValueType::kDate),
                    Column(ProjectionName(statement.SelectList()[3], 3),
                           ValueType::kInt64)});
    return out;
  };

  // Step 1: Parallel scan of customer -> dense cust_mult[c_custkey - c_min].
  std::vector<slot_t> ccols{*c_pk_slot, *c_flt_slot};
  std::ranges::sort(ccols);
  ccols.erase(std::ranges::unique(ccols).begin(), ccols.end());
  auto c_store = GetOrBuildLSTableStore(context.txn_, *tables[cust_idx], true,
                                        &ccols, false);
  if (!c_store || !c_store->HasColumn(*c_pk_slot) ||
      !c_store->HasColumn(*c_flt_slot)) {
    return std::optional<Relation>{};
  }
  int64_t c_min = std::numeric_limits<int64_t>::max();
  int64_t c_max = std::numeric_limits<int64_t>::min();
  for (const LSTablePageBlock& blk : c_store->blocks) {
    if (blk.row_count == 0) {
      continue;
    }
    const auto& c_pk = blk.columns[*c_pk_slot];
    if (!c_pk.has_min_max) {
      return std::optional<Relation>{};
    }
    c_min = std::min(c_min, c_pk.min_int);
    c_max = std::max(c_max, c_pk.max_int);
  }
  if (c_min > c_max || static_cast<uint64_t>(c_max - c_min) + 1 > 20000000) {
    return std::optional<Relation>{};
  }
  const size_t c_span = static_cast<size_t>(c_max - c_min) + 1;
  std::vector<std::atomic<uint32_t>> c_cnt(c_span);
  for (auto& a : c_cnt) {
    a.store(0, std::memory_order_relaxed);
  }
  std::atomic<size_t> c_matched{0};
  const size_t cb = c_store->blocks.size();
  const size_t cw = std::max<size_t>(1, ParallelWorkerCount(cb));
  auto scan_c = [&](size_t w) {
    size_t lcnt = 0;
    for (size_t b = w; b < cb; b += cw) {
      const LSTablePageBlock& blk = c_store->blocks[b];
      const auto& c_pk = blk.columns[*c_pk_slot];
      const auto& c_fl = blk.columns[*c_flt_slot];
      if (c_fl.dict != nullptr && c_fl.dict->active &&
          !c_fl.dict_codes.empty()) {
        uint32_t wc = UINT32_MAX;
        for (size_t i = 0; i < c_fl.dict->StringEntryCount(); ++i) {
          if (c_fl.dict->StringEntryAt(i) == c_flt_val) {
            wc = static_cast<uint32_t>(i);
            break;
          }
        }
        if (wc == UINT32_MAX) {
          continue;
        }
        const auto target = static_cast<uint8_t>(wc);
        const uint8_t* codes = c_fl.dict_codes.data();
        for (uint32_t r = 0; r < blk.row_count; ++r) {
          if ((c_fl.has_null && c_fl.IsNull(r)) || codes[r] != target ||
              (c_pk.has_null && c_pk.IsNull(r))) {
            continue;
          }
          c_cnt[static_cast<size_t>(c_pk.IntAt(r) - c_min)].fetch_add(
              1U, std::memory_order_relaxed);
          ++lcnt;
        }
      } else {
        for (uint32_t r = 0; r < blk.row_count; ++r) {
          if ((c_fl.has_null && c_fl.IsNull(r)) ||
              (c_pk.has_null && c_pk.IsNull(r)) ||
              c_fl.VarcharAt(r) != c_flt_val) {
            continue;
          }
          c_cnt[static_cast<size_t>(c_pk.IntAt(r) - c_min)].fetch_add(
              1U, std::memory_order_relaxed);
          ++lcnt;
        }
      }
    }
    c_matched.fetch_add(lcnt, std::memory_order_relaxed);
  };
  if (RunParallelTasks(cw, scan_c) != Status::kSuccess) {
    return std::optional<Relation>{};
  }
  if (c_matched.load(std::memory_order_relaxed) == 0) {
    return std::optional<Relation>(make_empty());
  }
  std::vector<uint32_t> cust_mult(c_span);
  for (size_t i = 0; i < c_span; ++i) {
    cust_mult[i] = c_cnt[i].load(std::memory_order_relaxed);
  }

  // Step 2: Parallel scan of orders with o_orderdate zone-map pruning ->
  // qualifying orders map `ord_info[o_orderkey]`.
  std::vector<slot_t> ocols{*o_pk_slot, *o_ck_slot, o_dt_slot, o_sp_slot};
  std::ranges::sort(ocols);
  ocols.erase(std::ranges::unique(ocols).begin(), ocols.end());
  auto o_store = GetOrBuildLSTableStore(context.txn_, *tables[ord_idx], true,
                                        &ocols, false);
  if (!o_store || !o_store->HasColumn(*o_pk_slot) ||
      !o_store->HasColumn(*o_ck_slot) || !o_store->HasColumn(o_dt_slot) ||
      !o_store->HasColumn(o_sp_slot)) {
    return std::optional<Relation>{};
  }
  struct OrdInfo {
    int64_t orderdate{0};
    int64_t shipprio{0};
    uint32_t mult{0};  // 0 = absent
  };
  struct QualOrd {
    int64_t ok{0};
    int64_t dt{0};
    int64_t sp{0};
    uint32_t mult{0};
  };
  const size_t ob = o_store->blocks.size();
  const size_t ow = std::max<size_t>(1, ParallelWorkerCount(ob));
  std::vector<std::vector<QualOrd>> w_ords(ow);
  auto scan_o = [&](size_t w) {
    auto& out = w_ords[w];
    for (size_t b = w; b < ob; b += ow) {
      const LSTablePageBlock& blk = o_store->blocks[b];
      const uint32_t nr = blk.row_count;
      if (nr == 0) {
        continue;
      }
      const auto& c_dt = blk.columns[o_dt_slot];
      if (c_dt.has_min_max && c_dt.min_int > o_dt_hi) {
        continue;
      }
      const auto& c_ok = blk.columns[*o_pk_slot];
      const auto& c_ck = blk.columns[*o_ck_slot];
      const auto& c_sp = blk.columns[o_sp_slot];
      for (uint32_t r = 0; r < nr; ++r) {
        if ((c_dt.has_null && c_dt.IsNull(r)) ||
            (c_ok.has_null && c_ok.IsNull(r)) ||
            (c_ck.has_null && c_ck.IsNull(r)) ||
            (c_sp.has_null && c_sp.IsNull(r))) {
          continue;
        }
        const int64_t dt = c_dt.IntAt(r);
        if (dt > o_dt_hi) {
          continue;
        }
        const int64_t ck = c_ck.IntAt(r);
        if (ck < c_min || ck > c_max) {
          continue;
        }
        const uint32_t cm = cust_mult[static_cast<size_t>(ck - c_min)];
        if (cm == 0) {
          continue;
        }
        out.push_back(QualOrd{
            .ok = c_ok.IntAt(r), .dt = dt, .sp = c_sp.IntAt(r), .mult = cm});
      }
    }
  };
  if (RunParallelTasks(ow, scan_o) != Status::kSuccess) {
    return std::optional<Relation>{};
  }
  size_t tot_o = 0;
  int64_t o_min = std::numeric_limits<int64_t>::max();
  int64_t o_max = std::numeric_limits<int64_t>::min();
  for (const auto& v : w_ords) {
    tot_o += v.size();
    for (const QualOrd& q : v) {
      o_min = std::min(o_min, q.ok);
      o_max = std::max(o_max, q.ok);
    }
  }
  if (tot_o == 0) {
    return std::optional<Relation>(make_empty());
  }
  const uint64_t o_span = static_cast<uint64_t>(o_max - o_min) + 1;
  const bool dense_ord = (o_span <= 500000 && o_span <= tot_o * 4);
  if (!dense_ord && tot_o > 50000) {
    // Large sparse order set: serial FlatIntKeyMap build would exceed the
    // parallel canonical join; defer cleanly.
    return std::optional<Relation>{};
  }
  std::vector<OrdInfo> ord_arr;
  std::vector<uint64_t> ord_bits;
  FlatIntKeyMap<OrdInfo> ord_map;
  if (dense_ord) {
    ord_arr.assign(static_cast<size_t>(o_span), OrdInfo{});
    for (const auto& v : w_ords) {
      for (const QualOrd& q : v) {
        OrdInfo& dst = ord_arr[static_cast<size_t>(q.ok - o_min)];
        if (dst.mult != 0) {
          // Duplicate o_orderkey with possibly different date/priority -> fall
          // back cleanly.
          return std::optional<Relation>{};
        }
        dst = OrdInfo{.orderdate = q.dt, .shipprio = q.sp, .mult = q.mult};
      }
    }
  } else {
    ord_map.Reserve(tot_o);
    if (o_span <= 64000000) {
      ord_bits.assign(static_cast<size_t>((o_span + 63) >> 6), 0);
    }
    for (const auto& v : w_ords) {
      for (const QualOrd& q : v) {
        if (!ord_bits.empty()) {
          const auto off = static_cast<size_t>(q.ok - o_min);
          ord_bits[off >> 6] |= (uint64_t{1} << (off & 63));
        }
        bool emp = false;
        OrdInfo* sl = ord_map.TryEmplace(q.ok, &emp);
        if (!emp) {
          return std::optional<Relation>{};
        }
        *sl = OrdInfo{.orderdate = q.dt, .shipprio = q.sp, .mult = q.mult};
      }
    }
  }
  auto lookup_ord = [&](int64_t ok) noexcept -> const OrdInfo* {
    if (ok < o_min || ok > o_max) {
      return nullptr;
    }
    const auto off = static_cast<size_t>(ok - o_min);
    if (dense_ord) {
      const OrdInfo& oi = ord_arr[off];
      return oi.mult != 0 ? &oi : nullptr;
    }
    if (!ord_bits.empty() &&
        (ord_bits[off >> 6] & (uint64_t{1} << (off & 63))) == 0) {
      return nullptr;
    }
    return ord_map.Find(ok);
  };

  // Step 3: Parallel scan of lineitem exploiting contiguous l_orderkey runs
  // (`RevenueRuns_sound`) + exact boundary merge (`mergeBoundary`)!
  std::vector<slot_t> lcols{l_ok_slot, *l_sd_slot, l_pr_slot, l_ds_slot};
  std::ranges::sort(lcols);
  lcols.erase(std::ranges::unique(lcols).begin(), lcols.end());
  auto l_store = GetOrBuildLSTableStore(context.txn_, *tables[line_idx], true,
                                        &lcols, false);
  if (!l_store || !l_store->HasColumn(l_ok_slot) ||
      !l_store->HasColumn(*l_sd_slot) || !l_store->HasColumn(l_pr_slot) ||
      !l_store->HasColumn(l_ds_slot)) {
    return std::optional<Relation>{};
  }
  const bool pr_dbl =
      schemas[line_idx].GetColumn(l_pr_slot).Type() == ValueType::kDouble;
  const bool ds_dbl =
      schemas[line_idx].GetColumn(l_ds_slot).Type() == ValueType::kDouble;
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

  struct RunEntry {
    int64_t ok{0};
    double rev{0.0};
    uint64_t non_null{0};
    int64_t orderdate{0};
    int64_t shipprio{0};
  };
  const size_t K = statement.Limit();
  auto entry_better = [](const RunEntry& a,
                         const RunEntry& b) noexcept -> bool {
    const bool an = (a.non_null == 0);
    const bool bn = (b.non_null == 0);
    if (an != bn) {
      return !an;  // DESC: non-null before null (default NULLS LAST on DESC)
    }
    if (!an && a.rev != b.rev) {
      return a.rev > b.rev;
    }
    if (a.orderdate != b.orderdate) {
      return a.orderdate < b.orderdate;
    }
    return a.ok < b.ok;
  };

  // Partition lineitem blocks into contiguous worker slices (not strided!) so
  // each worker sees a contiguous range of l_orderkey runs for
  // `RevenueRuns_sound` + `mergeBoundary`.
  const size_t lb = l_store->blocks.size();
  const size_t lw = std::max<size_t>(1, ParallelWorkerCount(lb));
  struct WorkerRuns {
    bool monotonic{true};
    bool has_first{false};
    RunEntry first_run;
    bool has_last{false};
    RunEntry last_run;
    std::vector<RunEntry> topk;
  };
  std::vector<WorkerRuns> w_runs(lw);
  std::atomic<bool> any_non_mono{false};

  auto push_topk = [&](std::vector<RunEntry>& hp, const RunEntry& e) {
    if (hp.size() < K) {
      hp.push_back(e);
      std::push_heap(hp.begin(), hp.end(), entry_better);
    } else if (entry_better(e, hp.front())) {
      std::pop_heap(hp.begin(), hp.end(), entry_better);
      hp.back() = e;
      std::push_heap(hp.begin(), hp.end(), entry_better);
    }
  };

  auto scan_l_runs = [&](size_t w) {
    const size_t b_begin = (lb * w) / lw;
    const size_t b_end = (lb * (w + 1)) / lw;
    WorkerRuns& wr = w_runs[w];
    wr.topk.reserve(K + 1);
    bool have_cur = false;
    RunEntry cur;
    int64_t prev_seen_ok = std::numeric_limits<int64_t>::min();
    for (size_t b = b_begin; b < b_end; ++b) {
      if (any_non_mono.load(std::memory_order_relaxed)) {
        return;
      }
      const LSTablePageBlock& blk = l_store->blocks[b];
      const uint32_t nr = blk.row_count;
      if (nr == 0) {
        continue;
      }
      const auto& c_sd = blk.columns[*l_sd_slot];
      if (c_sd.has_min_max && c_sd.max_int < l_sd_lo) {
        continue;
      }
      const auto& c_ok = blk.columns[l_ok_slot];
      if (c_ok.has_min_max && (c_ok.max_int < o_min || c_ok.min_int > o_max)) {
        continue;
      }
      const auto& c_pr = blk.columns[l_pr_slot];
      const auto& c_ds = blk.columns[l_ds_slot];
      for (uint32_t r = 0; r < nr; ++r) {
        if ((c_sd.has_null && c_sd.IsNull(r)) ||
            (c_ok.has_null && c_ok.IsNull(r))) {
          continue;
        }
        const int64_t ok = c_ok.IntAt(r);
        if (ok < prev_seen_ok) {
          wr.monotonic = false;
          any_non_mono.store(true, std::memory_order_relaxed);
          return;
        }
        prev_seen_ok = ok;
        if (c_sd.IntAt(r) < l_sd_lo) {
          continue;
        }
        const OrdInfo* oi = lookup_ord(ok);
        if (oi == nullptr) {
          continue;
        }
        const bool vn = (c_pr.has_null && c_pr.IsNull(r)) ||
                        (c_ds.has_null && c_ds.IsNull(r));
        const double vol = vn ? 0.0
                              : dbl_at(c_pr, r, pr_dbl) *
                                    (1.0 - dbl_at(c_ds, r, ds_dbl)) *
                                    static_cast<double>(oi->mult);
        if (!have_cur) {
          cur = RunEntry{.ok = ok,
                         .rev = vol,
                         .non_null = vn ? 0U : static_cast<uint64_t>(oi->mult),
                         .orderdate = oi->orderdate,
                         .shipprio = oi->shipprio};
          have_cur = true;
        } else if (cur.ok == ok) {
          if (!vn) {
            cur.rev += vol;
            cur.non_null += oi->mult;
          }
        } else {
          // Run ended: first run goes to `first_run` (may straddle left
          // boundary); all interior runs go directly to `wr.topk`!
          if (!wr.has_first) {
            wr.first_run = cur;
            wr.has_first = true;
          } else {
            push_topk(wr.topk, cur);
          }
          cur = RunEntry{.ok = ok,
                         .rev = vol,
                         .non_null = vn ? 0U : static_cast<uint64_t>(oi->mult),
                         .orderdate = oi->orderdate,
                         .shipprio = oi->shipprio};
        }
      }
    }
    if (have_cur) {
      if (!wr.has_first) {
        wr.first_run = cur;
        wr.has_first = true;
      } else {
        wr.last_run = cur;
        wr.has_last = true;
      }
    }
  };
  if (RunParallelTasks(lw, scan_l_runs) != Status::kSuccess) {
    return std::optional<Relation>{};
  }
  if (any_non_mono.load(std::memory_order_relaxed)) {
    // Non-contiguous l_orderkey: fall back cleanly to canonical executor.
    return std::optional<Relation>{};
  }

  // Step 4: Verify global monotonicity across worker boundaries and apply
  // `mergeBoundary` (`RevenueRuns_sound`) + merge Top-K heaps.
  std::vector<RunEntry> global_topk;
  global_topk.reserve(K + 1);
  bool has_pending = false;
  RunEntry pending;
  for (size_t w = 0; w < lw; ++w) {
    const WorkerRuns& wr = w_runs[w];
    if (!wr.has_first) {
      continue;
    }
    if (has_pending) {
      if (wr.first_run.ok < pending.ok) {
        return std::optional<Relation>{};
      }
      if (wr.first_run.ok == pending.ok) {
        pending.rev += wr.first_run.rev;
        pending.non_null += wr.first_run.non_null;
        if (wr.has_last) {
          push_topk(global_topk, pending);
          pending = wr.last_run;
        }
      } else {
        push_topk(global_topk, pending);
        if (wr.has_last) {
          push_topk(global_topk, wr.first_run);
          pending = wr.last_run;
        } else {
          pending = wr.first_run;
        }
      }
    } else {
      if (wr.has_last) {
        push_topk(global_topk, wr.first_run);
        pending = wr.last_run;
        has_pending = true;
      } else {
        pending = wr.first_run;
        has_pending = true;
      }
    }
    for (const RunEntry& e : wr.topk) {
      push_topk(global_topk, e);
    }
  }
  if (has_pending) {
    push_topk(global_topk, pending);
  }
  std::ranges::sort(global_topk, entry_better);

  Relation out = make_empty();
  out.rows.reserve(global_topk.size());
  for (const RunEntry& e : global_topk) {
    out.AddRow(Row({Value(e.ok), e.non_null > 0 ? Value(e.rev) : Value(),
                    Value::DateFromDays(e.orderdate), Value(e.shipprio)}));
  }
  RETURN_IF_FAIL(out.GetStatus());
  return std::optional<Relation>(std::move(out));
}

}  // namespace tinylamb::relational_detail
