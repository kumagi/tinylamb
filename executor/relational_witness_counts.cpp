/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
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
// LateSlack_sound, RetireWitness_sound) fast path for single-table grouped
// COUNT(*) with correlated EXISTS on a paired inequality `l_commitdate <
// l_receiptdate` (TPC-H Q4):
//   SELECT o_orderpriority, COUNT(*) FROM orders
//   WHERE o_orderdate >= lo AND o_orderdate < hi
//     AND EXISTS (SELECT * FROM lineitem
//                 WHERE l_orderkey = o_orderkey AND l_commitdate <
//                 l_receiptdate)
//   GROUP BY o_orderpriority [ORDER BY o_orderpriority]
StatusOr<std::optional<Relation>> TryExecuteRetireWitnessExists(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& ctes) {
  if (::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_RETIRE_WITNESS") !=
      nullptr) {
    return std::optional<Relation>{};
  }
  if (!statement.WithQueries().empty() || !statement.UnionAll().empty() ||
      statement.GetSetOperationTree() != nullptr || statement.Distinct() ||
      statement.Having() || statement.HasLimit() || statement.Offset() > 0 ||
      HasWindowFunctions(statement) || !statement.WhereClause() ||
      statement.Sources().size() != 1 || statement.GroupBy().size() != 1 ||
      statement.SelectList().size() != 2) {
    return std::optional<Relation>{};
  }
  const SelectSource& osrc = statement.Sources()[0];
  if (osrc.query != nullptr || osrc.unnest != nullptr || osrc.is_lateral ||
      osrc.sample.has_value() || !osrc.using_columns.empty() ||
      osrc.table.empty() || ctes.contains(AsciiToLower(osrc.table))) {
    return std::optional<Relation>{};
  }
  ASSIGN_OR_RETURN(std::shared_ptr<Table>, o_tbl, context.GetTable(osrc.table));
  if (!o_tbl) {
    return std::optional<Relation>{};
  }
  const Schema o_schema = o_tbl->GetSchema();
  const std::string o_alias =
      AsciiToLower(osrc.alias.empty() ? osrc.table : osrc.alias);
  const std::string o_tbl_lc = AsciiToLower(osrc.table);
  auto resolve_o = [&](const ColumnName& cn) -> std::optional<slot_t> {
    if (!cn.schema.empty()) {
      const std::string sc = AsciiToLower(cn.schema);
      if (sc != o_alias && sc != o_tbl_lc) {
        return std::nullopt;
      }
    }
    const std::string nm = AsciiToLower(cn.name);
    for (slot_t c = 0; c < o_schema.ColumnCount(); ++c) {
      if (AsciiToLower(o_schema.GetColumn(c).Name().name) == nm) {
        return c;
      }
    }
    return std::nullopt;
  };

  // Match GROUP BY[0] and SELECT[0] = prio_slot (kVarChar), SELECT[1] =
  // COUNT(*).
  if (!statement.GroupBy()[0] ||
      statement.GroupBy()[0]->Type() != TypeTag::kColumnValue ||
      !statement.SelectList()[0].expression ||
      statement.SelectList()[0].expression->Type() != TypeTag::kColumnValue ||
      !statement.SelectList()[1].expression ||
      statement.SelectList()[1].expression->Type() != TypeTag::kAggregateExp) {
    return std::optional<Relation>{};
  }
  auto gb_slot =
      resolve_o(statement.GroupBy()[0]->AsColumnValue().GetColumnName());
  auto sel0_slot = resolve_o(
      statement.SelectList()[0].expression->AsColumnValue().GetColumnName());
  if (!gb_slot || !sel0_slot || *gb_slot != *sel0_slot ||
      o_schema.GetColumn(*gb_slot).Type() != ValueType::kVarChar) {
    return std::optional<Relation>{};
  }
  const slot_t prio_slot = *gb_slot;
  const auto& cagg =
      statement.SelectList()[1].expression->AsAggregateExpression();
  if (cagg.GetType() != AggregationType::kCount || cagg.Distinct() ||
      cagg.WhereFilter() ||
      (cagg.Child() && cagg.Child()->Type() != TypeTag::kConstantValue &&
       (cagg.Child()->Type() != TypeTag::kColumnValue ||
        cagg.Child()->AsColumnValue().GetColumnName().name != "*"))) {
    return std::optional<Relation>{};
  }

  // Check ORDER BY: empty or ORDER BY prio_slot [ASC|DESC].
  bool ord_desc = false;
  if (!statement.OrderBy().empty()) {
    if (statement.OrderBy().size() != 1) {
      return std::optional<Relation>{};
    }
    const auto& ob = statement.OrderBy()[0];
    if (!ob.expression || ob.expression->Type() != TypeTag::kColumnValue) {
      return std::optional<Relation>{};
    }
    const auto& cn = ob.expression->AsColumnValue().GetColumnName();
    auto r = resolve_o(cn);
    const std::string p0 = ProjectionName(statement.SelectList()[0], 0);
    if ((!r || *r != prio_slot) && !IdentifierEquals(cn.name, p0)) {
      return std::optional<Relation>{};
    }
    ord_desc = !ob.ascending;
  }

  // Parse WHERE conjuncts: date bounds on one outer DATE column + 1 EXISTS
  // subquery.
  std::optional<slot_t> date_slot;
  int64_t date_lo = std::numeric_limits<int64_t>::min();
  int64_t date_hi = std::numeric_limits<int64_t>::max();
  const SelectStatement* exists_sub = nullptr;

  auto eval_const_date = [&](const Expression& e) -> std::optional<int64_t> {
    if (!e) {
      return std::nullopt;
    }
    if (e->Type() == TypeTag::kConstantValue) {
      const Value& v = e->AsConstantValue().GetValue();
      if (!v.IsNull() && v.type == ValueType::kDate) {
        return v.value.int_value;
      }
      return std::nullopt;
    }
    // Support constant expressions like date_add(DATE '1997-06-01', INTERVAL 3
    // month).
    Row empty_row;
    Schema empty_schema;
    auto ev = e->TryEvaluate(empty_row, empty_schema);
    if (ev.HasValue() && !ev.Value().IsNull() &&
        ev.Value().type == ValueType::kDate) {
      return ev.Value().value.int_value;
    }
    return std::nullopt;
  };

  for (const Expression& conj : SplitConjuncts(statement.WhereClause())) {
    if (!conj) {
      return std::optional<Relation>{};
    }
    if (conj->Type() == TypeTag::kQueryExp) {
      const auto& qe = conj->AsQueryExpression();
      if (!qe.Exists() || qe.Negated() || !qe.Query() ||
          exists_sub != nullptr) {
        return std::optional<Relation>{};
      }
      exists_sub = qe.Query().get();
      continue;
    }
    if (conj->Type() != TypeTag::kBinaryExp) {
      return std::optional<Relation>{};
    }
    const auto& be = conj->AsBinaryExpression();
    auto try_bound = [&](const Expression& ce, const Expression& ve,
                         BinaryOperation op) -> bool {
      if (!ce || ce->Type() != TypeTag::kColumnValue) {
        return false;
      }
      auto sl = resolve_o(ce->AsColumnValue().GetColumnName());
      if (!sl || o_schema.GetColumn(*sl).Type() != ValueType::kDate) {
        return false;
      }
      auto dv = eval_const_date(ve);
      if (!dv) {
        return false;
      }
      if (date_slot && *date_slot != *sl) {
        return false;
      }
      date_slot = sl;
      switch (op) {
        case BinaryOperation::kGreaterThanEquals:
          date_lo = std::max(date_lo, *dv);
          return true;
        case BinaryOperation::kGreaterThan:
          date_lo = std::max(date_lo, *dv + 1);
          return true;
        case BinaryOperation::kLessThanEquals:
          date_hi = std::min(date_hi, *dv);
          return true;
        case BinaryOperation::kLessThan:
          date_hi = std::min(date_hi, *dv - 1);
          return true;
        case BinaryOperation::kEquals:
          date_lo = std::max(date_lo, *dv);
          date_hi = std::min(date_hi, *dv);
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
    if (!try_bound(be.Left(), be.Right(), be.Op()) &&
        !try_bound(be.Right(), be.Left(), flip(be.Op()))) {
      return std::optional<Relation>{};
    }
  }
  if (exists_sub == nullptr || !date_slot) {
    return std::optional<Relation>{};
  }

  // Inspect EXISTS subquery: single base table `lineitem`, WHERE
  // `l_orderkey = o_orderkey AND l_commitdate < l_receiptdate`.
  if (!exists_sub->WithQueries().empty() || !exists_sub->UnionAll().empty() ||
      exists_sub->GetSetOperationTree() != nullptr ||
      !exists_sub->GroupBy().empty() || exists_sub->Having() ||
      !exists_sub->WhereClause() || exists_sub->Sources().size() != 1) {
    return std::optional<Relation>{};
  }
  const SelectSource& lsrc = exists_sub->Sources()[0];
  if (lsrc.query != nullptr || lsrc.unnest != nullptr || lsrc.is_lateral ||
      lsrc.sample.has_value() || !lsrc.using_columns.empty() ||
      lsrc.table.empty() || ctes.contains(AsciiToLower(lsrc.table))) {
    return std::optional<Relation>{};
  }
  ASSIGN_OR_RETURN(std::shared_ptr<Table>, l_tbl, context.GetTable(lsrc.table));
  if (!l_tbl) {
    return std::optional<Relation>{};
  }
  const Schema l_schema = l_tbl->GetSchema();
  const std::string l_alias =
      AsciiToLower(lsrc.alias.empty() ? lsrc.table : lsrc.alias);
  const std::string l_tbl_lc = AsciiToLower(lsrc.table);
  auto resolve_l = [&](const ColumnName& cn) -> std::optional<slot_t> {
    if (!cn.schema.empty()) {
      const std::string sc = AsciiToLower(cn.schema);
      if (sc != l_alias && sc != l_tbl_lc) {
        return std::nullopt;
      }
    }
    const std::string nm = AsciiToLower(cn.name);
    for (slot_t c = 0; c < l_schema.ColumnCount(); ++c) {
      if (AsciiToLower(l_schema.GetColumn(c).Name().name) == nm) {
        return c;
      }
    }
    return std::nullopt;
  };

  std::optional<slot_t> o_key_slot;
  std::optional<slot_t> l_key_slot;
  std::optional<slot_t> l_lo_slot;  // l_commitdate (< l_hi_slot)
  std::optional<slot_t> l_hi_slot;  // l_receiptdate
  for (const Expression& conj : SplitConjuncts(exists_sub->WhereClause())) {
    if (!conj || conj->Type() != TypeTag::kBinaryExp) {
      return std::optional<Relation>{};
    }
    const auto& be = conj->AsBinaryExpression();
    if (!be.Left() || !be.Right() ||
        be.Left()->Type() != TypeTag::kColumnValue ||
        be.Right()->Type() != TypeTag::kColumnValue) {
      return std::optional<Relation>{};
    }
    const ColumnName& ln = be.Left()->AsColumnValue().GetColumnName();
    const ColumnName& rn = be.Right()->AsColumnValue().GetColumnName();
    if (be.Op() == BinaryOperation::kEquals && !o_key_slot) {
      auto lk = resolve_l(ln);
      auto ok = resolve_o(rn);
      if (!lk || !ok) {
        lk = resolve_l(rn);
        ok = resolve_o(ln);
      }
      if (!lk || !ok || l_schema.GetColumn(*lk).Type() != ValueType::kInt64 ||
          o_schema.GetColumn(*ok).Type() != ValueType::kInt64) {
        return std::optional<Relation>{};
      }
      l_key_slot = lk;
      o_key_slot = ok;
      continue;
    }
    if ((be.Op() == BinaryOperation::kLessThan ||
         be.Op() == BinaryOperation::kGreaterThan) &&
        !l_lo_slot) {
      auto c1 = resolve_l(ln);
      auto c2 = resolve_l(rn);
      if (!c1 || !c2) {
        return std::optional<Relation>{};
      }
      const ValueType t1 = l_schema.GetColumn(*c1).Type();
      const ValueType t2 = l_schema.GetColumn(*c2).Type();
      if ((t1 != ValueType::kDate && t1 != ValueType::kInt64) || t1 != t2) {
        return std::optional<Relation>{};
      }
      if (be.Op() == BinaryOperation::kLessThan) {
        l_lo_slot = c1;
        l_hi_slot = c2;
      } else {
        l_lo_slot = c2;
        l_hi_slot = c1;
      }
      continue;
    }
    return std::optional<Relation>{};
  }
  if (!o_key_slot || !l_key_slot || !l_lo_slot || !l_hi_slot) {
    return std::optional<Relation>{};
  }

  auto make_empty = [&]() -> Relation {
    Relation out(context.execution_runtime());
    out.schema =
        Schema("", {Column(ProjectionName(statement.SelectList()[0], 0),
                           ValueType::kVarChar),
                    Column(ProjectionName(statement.SelectList()[1], 1),
                           ValueType::kInt64)});
    return out;
  };

  // Step 1: Parallel scan of orders with o_orderdate zone-map pruning.
  std::vector<slot_t> ocols{*o_key_slot, *date_slot, prio_slot};
  std::ranges::sort(ocols);
  ocols.erase(std::ranges::unique(ocols).begin(), ocols.end());
  auto o_store =
      GetOrBuildLSTableStore(context.txn_, *o_tbl, true, &ocols, false);
  if (!o_store || !o_store->HasColumn(*o_key_slot) ||
      !o_store->HasColumn(*date_slot) || !o_store->HasColumn(prio_slot)) {
    return std::optional<Relation>{};
  }
  struct QualOrder {
    int64_t key{0};
    uint32_t bi{0};
    uint32_t ri{0};
  };
  const size_t o_blocks = o_store->blocks.size();
  const size_t o_workers = std::max<size_t>(1, ParallelWorkerCount(o_blocks));
  std::vector<std::vector<QualOrder>> w_orders(o_workers);
  auto scan_o = [&](size_t w) {
    auto& out = w_orders[w];
    for (size_t b = w; b < o_blocks; b += o_workers) {
      const LSTablePageBlock& blk = o_store->blocks[b];
      const uint32_t nr = blk.row_count;
      if (nr == 0) {
        continue;
      }
      const auto& c_dt = blk.columns[*date_slot];
      if (c_dt.has_min_max &&
          (c_dt.max_int < date_lo || c_dt.min_int > date_hi)) {
        continue;
      }
      const auto& c_ok = blk.columns[*o_key_slot];
      for (uint32_t r = 0; r < nr; ++r) {
        if ((c_dt.has_null && c_dt.IsNull(r)) ||
            (c_ok.has_null && c_ok.IsNull(r))) {
          continue;
        }
        const int64_t dt = c_dt.IntAt(r);
        if (dt < date_lo || dt > date_hi) {
          continue;
        }
        out.push_back(QualOrder{
            .key = c_ok.IntAt(r), .bi = static_cast<uint32_t>(b), .ri = r});
      }
    }
  };
  if (RunParallelTasks(o_workers, scan_o) != Status::kSuccess) {
    return std::optional<Relation>{};
  }
  size_t tot_o = 0;
  int64_t ok_min = std::numeric_limits<int64_t>::max();
  int64_t ok_max = std::numeric_limits<int64_t>::min();
  for (const auto& v : w_orders) {
    tot_o += v.size();
    for (const QualOrder& q : v) {
      ok_min = std::min(ok_min, q.key);
      ok_max = std::max(ok_max, q.key);
    }
  }
  if (tot_o == 0) {
    return std::optional<Relation>(make_empty());
  }

  // Build compact bit-packed demand set `pending_bits` + `retired_bits`
  // (750 KB for 6M key span) instead of 1 byte/key, avoiding page-fault
  // overhead while retiring each key at most once (RetireWitness_sound).
  const uint64_t ok_span = static_cast<uint64_t>(ok_max - ok_min) + 1;
  const bool use_bitset = (ok_span <= 64000000);
  std::vector<uint64_t> pending_bits;
  std::vector<std::atomic<uint64_t>> retired_bits;
  FlatIntKeyMap<uint8_t> pending_map;
  if (use_bitset) {
    const auto nwords = static_cast<size_t>((ok_span + 63) >> 6);
    pending_bits.assign(nwords, 0);
    retired_bits = std::vector<std::atomic<uint64_t>>(nwords);
    for (auto& a : retired_bits) {
      a.store(0, std::memory_order_relaxed);
    }
    for (const auto& v : w_orders) {
      for (const QualOrder& q : v) {
        const auto off = static_cast<size_t>(q.key - ok_min);
        pending_bits[off >> 6] |= (uint64_t{1} << (off & 63));
      }
    }
  } else {
    pending_map.Reserve(tot_o);
    for (const auto& v : w_orders) {
      for (const QualOrder& q : v) {
        bool emp = false;
        uint8_t* sl = pending_map.TryEmplace(q.key, &emp);
        *sl = 1;
      }
    }
  }

  // Step 2: Parallel scan of lineitem with l_orderkey zone-map pruning.
  // Retire each pending key on its first late line (`l_lo < l_hi`,
  // LateSlack_sound + RetireWitness_sound).
  std::vector<slot_t> lcols{*l_key_slot, *l_lo_slot, *l_hi_slot};
  std::ranges::sort(lcols);
  lcols.erase(std::ranges::unique(lcols).begin(), lcols.end());
  auto l_store =
      GetOrBuildLSTableStore(context.txn_, *l_tbl, true, &lcols, false);
  if (!l_store || !l_store->HasColumn(*l_key_slot) ||
      !l_store->HasColumn(*l_lo_slot) || !l_store->HasColumn(*l_hi_slot)) {
    return std::optional<Relation>{};
  }
  const size_t l_blocks = l_store->blocks.size();
  const size_t l_workers = std::max<size_t>(1, ParallelWorkerCount(l_blocks));
  std::vector<std::vector<int64_t>> w_retired(l_workers);
  auto scan_l = [&](size_t w) {
    auto& retired = w_retired[w];
    for (size_t b = w; b < l_blocks; b += l_workers) {
      const LSTablePageBlock& blk = l_store->blocks[b];
      const uint32_t nr = blk.row_count;
      if (nr == 0) {
        continue;
      }
      const auto& c_ok = blk.columns[*l_key_slot];
      if (c_ok.has_min_max &&
          (c_ok.max_int < ok_min || c_ok.min_int > ok_max)) {
        continue;
      }
      const auto& c_lo = blk.columns[*l_lo_slot];
      const auto& c_hi = blk.columns[*l_hi_slot];
      for (uint32_t r = 0; r < nr; ++r) {
        if ((c_ok.has_null && c_ok.IsNull(r)) ||
            (c_lo.has_null && c_lo.IsNull(r)) ||
            (c_hi.has_null && c_hi.IsNull(r))) {
          continue;
        }
        const int64_t k = c_ok.IntAt(r);
        if (k < ok_min || k > ok_max) {
          continue;
        }
        if (use_bitset) {
          const auto off = static_cast<size_t>(k - ok_min);
          const size_t wi = off >> 6;
          const uint64_t mask = uint64_t{1} << (off & 63);
          if ((pending_bits[wi] & mask) == 0 ||
              (retired_bits[wi].load(std::memory_order_relaxed) & mask) != 0) {
            continue;
          }
          if (c_lo.IntAt(r) >= c_hi.IntAt(r)) {
            continue;
          }
          retired_bits[wi].fetch_or(mask, std::memory_order_relaxed);
        } else {
          if (pending_map.Find(k) == nullptr) {
            continue;
          }
          if (c_lo.IntAt(r) >= c_hi.IntAt(r)) {
            continue;
          }
          retired.push_back(k);
        }
      }
    }
  };
  if (RunParallelTasks(l_workers, scan_l) != Status::kSuccess) {
    return std::optional<Relation>{};
  }
  if (!use_bitset) {
    for (const auto& v : w_retired) {
      for (int64_t k : v) {
        if (const uint8_t* p = pending_map.Find(k)) {
          *const_cast<uint8_t*>(p) = 2;
        }
      }
    }
  }

  // Step 3: Tally full outer multiplicities in parallel across o_workers
  // (decoding o_orderpriority ONLY for retired orders!).
  std::vector<std::unordered_map<std::string, int64_t>> w_prio_counts(
      o_workers);
  std::vector<int64_t> w_null_cnt(o_workers, 0);
  auto tally_o = [&](size_t w) {
    auto& local = w_prio_counts[w];
    int64_t lnull = 0;
    for (const QualOrder& q : w_orders[w]) {
      bool is_retired = false;
      if (use_bitset) {
        const auto off = static_cast<size_t>(q.key - ok_min);
        is_retired = (retired_bits[off >> 6].load(std::memory_order_relaxed) &
                      (uint64_t{1} << (off & 63))) != 0;
      } else {
        const uint8_t* p = pending_map.Find(q.key);
        is_retired = (p != nullptr && *p == 2);
      }
      if (!is_retired) {
        continue;
      }
      const auto& c_pr = o_store->blocks[q.bi].columns[prio_slot];
      if (c_pr.has_null && c_pr.IsNull(q.ri)) {
        ++lnull;
      } else {
        const std::string_view sv = c_pr.VarcharAt(q.ri);
        auto it = local.find(std::string(sv));
        if (it == local.end()) {
          local.emplace(std::string(sv), 1);
        } else {
          ++it->second;
        }
      }
    }
    w_null_cnt[w] = lnull;
  };
  if (RunParallelTasks(o_workers, tally_o) != Status::kSuccess) {
    return std::optional<Relation>{};
  }
  std::unordered_map<std::string, int64_t> prio_counts;
  int64_t null_prio_cnt = 0;
  for (size_t w = 0; w < o_workers; ++w) {
    null_prio_cnt += w_null_cnt[w];
    for (const auto& [k, cnt] : w_prio_counts[w]) {
      prio_counts[k] += cnt;
    }
  }

  std::vector<std::pair<Value, int64_t>> out_pairs;
  out_pairs.reserve(prio_counts.size() + (null_prio_cnt > 0 ? 1 : 0));
  if (null_prio_cnt > 0) {
    out_pairs.emplace_back(Value(), null_prio_cnt);
  }
  for (auto& [k, cnt] : prio_counts) {
    out_pairs.emplace_back(Value(std::string(k)), cnt);
  }
  std::ranges::sort(out_pairs, [&](const auto& a, const auto& b) {
    if (a.first.IsNull() != b.first.IsNull()) {
      return ord_desc ? !a.first.IsNull() : a.first.IsNull();
    }
    if (a.first.IsNull()) {
      return false;
    }
    return ord_desc
               ? (a.first.value.varchar_value > b.first.value.varchar_value)
               : (a.first.value.varchar_value < b.first.value.varchar_value);
  });
  Relation out = make_empty();
  out.rows.reserve(out_pairs.size());
  for (auto& [pv, cnt] : out_pairs) {
    out.AddRow(Row({std::move(pv), Value(cnt)}));
  }
  RETURN_IF_FAIL(out.GetStatus());
  return std::optional<Relation>(std::move(out));
}

// Lean-backed (AnalyticalOperators/RevenueAggregatesAndJoinWitnesses.lean:
// RebucketOrders_sound) fast path for LEFT OUTER JOIN order-count distribution
// histograms (TPC-H Q13):
//   SELECT c_count, COUNT(*) AS custdist
//   FROM (SELECT c_custkey, COUNT(o_orderkey) AS c_count
//         FROM customer LEFT OUTER JOIN orders
//           ON c_custkey = o_custkey AND o_comment NOT LIKE '%pat1%pat2%'
//         GROUP BY c_custkey) c_orders
//   GROUP BY c_count ORDER BY custdist DESC, c_count DESC
StatusOr<std::optional<Relation>> TryExecuteRebucketLeftJoinHist(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& ctes) {
  if (::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_REBUCKET_ORDERS") !=
      nullptr) {
    return std::optional<Relation>{};
  }
  if (!statement.WithQueries().empty() || !statement.UnionAll().empty() ||
      statement.GetSetOperationTree() != nullptr || statement.WhereClause() ||
      statement.Having() || statement.Distinct() || statement.HasDistinctOn() ||
      statement.HasLimit() || statement.Offset() > 0 ||
      HasWindowFunctions(statement) || statement.Sources().size() != 1 ||
      statement.GroupBy().size() != 1 || statement.SelectList().size() != 2) {
    return std::optional<Relation>{};
  }
  const SelectSource& outer_src = statement.Sources()[0];
  if (!outer_src.query || outer_src.unnest || outer_src.is_lateral ||
      outer_src.sample.has_value() || !outer_src.using_columns.empty()) {
    return std::optional<Relation>{};
  }
  // Outer SELECT must be (c_count, COUNT(*)) GROUP BY c_count.
  if (!statement.GroupBy()[0] ||
      statement.GroupBy()[0]->Type() != TypeTag::kColumnValue ||
      !statement.SelectList()[0].expression ||
      statement.SelectList()[0].expression->Type() != TypeTag::kColumnValue ||
      !statement.SelectList()[1].expression ||
      statement.SelectList()[1].expression->Type() != TypeTag::kAggregateExp) {
    return std::optional<Relation>{};
  }
  const std::string outer_cnt_col =
      statement.GroupBy()[0]->AsColumnValue().GetColumnName().name;
  if (!IdentifierEquals(statement.SelectList()[0]
                            .expression->AsColumnValue()
                            .GetColumnName()
                            .name,
                        outer_cnt_col)) {
    return std::optional<Relation>{};
  }
  const auto& outer_agg =
      statement.SelectList()[1].expression->AsAggregateExpression();
  if (outer_agg.GetType() != AggregationType::kCount || outer_agg.Distinct() ||
      outer_agg.WhereFilter() ||
      (outer_agg.Child() &&
       outer_agg.Child()->Type() != TypeTag::kConstantValue &&
       (outer_agg.Child()->Type() != TypeTag::kColumnValue ||
        outer_agg.Child()->AsColumnValue().GetColumnName().name != "*"))) {
    return std::optional<Relation>{};
  }

  // Inner derived query: customer LEFT OUTER JOIN orders ON ... GROUP BY
  // c_custkey.
  const SelectStatement& inner = *outer_src.query;
  if (!inner.WithQueries().empty() || !inner.UnionAll().empty() ||
      inner.GetSetOperationTree() != nullptr || inner.WhereClause() ||
      inner.Having() || inner.Distinct() || inner.HasLimit() ||
      inner.Offset() != 0 || HasWindowFunctions(inner) ||
      inner.Sources().size() != 2 || inner.GroupBy().size() != 1 ||
      inner.SelectList().size() != 2) {
    return std::optional<Relation>{};
  }
  const SelectSource& csrc = inner.Sources()[0];
  const SelectSource& osrc = inner.Sources()[1];
  if (csrc.query || osrc.query || csrc.unnest || osrc.unnest ||
      csrc.is_lateral || osrc.is_lateral || csrc.table.empty() ||
      osrc.table.empty() || ctes.contains(AsciiToLower(csrc.table)) ||
      ctes.contains(AsciiToLower(osrc.table)) ||
      osrc.join_type != JoinType::kLeft || !osrc.join_condition) {
    return std::optional<Relation>{};
  }
  ASSIGN_OR_RETURN(std::shared_ptr<Table>, c_tbl, context.GetTable(csrc.table));
  ASSIGN_OR_RETURN(std::shared_ptr<Table>, o_tbl, context.GetTable(osrc.table));
  if (!c_tbl || !o_tbl) {
    return std::optional<Relation>{};
  }
  const Schema c_schema = c_tbl->GetSchema();
  const Schema o_schema = o_tbl->GetSchema();
  const std::string c_alias =
      AsciiToLower(csrc.alias.empty() ? csrc.table : csrc.alias);
  const std::string o_alias =
      AsciiToLower(osrc.alias.empty() ? osrc.table : osrc.alias);
  const std::string c_tbl_lc = AsciiToLower(csrc.table);
  const std::string o_tbl_lc = AsciiToLower(osrc.table);

  auto resolve_in =
      [&](const ColumnName& cn) -> std::optional<std::pair<size_t, slot_t>> {
    const std::string wn = AsciiToLower(cn.name);
    const std::string ws = AsciiToLower(cn.schema);
    std::optional<std::pair<size_t, slot_t>> found;
    auto scan_sc = [&](size_t idx, const Schema& sc, const std::string& al,
                       const std::string& tn) -> bool {
      if (!ws.empty() && al != ws && tn != ws) {
        return true;
      }
      for (slot_t c = 0; c < sc.ColumnCount(); ++c) {
        if (AsciiToLower(sc.GetColumn(c).Name().name) == wn) {
          if (found.has_value()) {
            return false;
          }
          found = {idx, c};
        }
      }
      return true;
    };
    if (!scan_sc(0, c_schema, c_alias, c_tbl_lc) ||
        !scan_sc(1, o_schema, o_alias, o_tbl_lc)) {
      return std::nullopt;
    }
    return found;
  };

  // Check inner GROUP BY[0] = c_custkey (kInt64) and inner SELECT has
  // COUNT(o_orderkey) aliased to outer_cnt_col.
  if (!inner.GroupBy()[0] ||
      inner.GroupBy()[0]->Type() != TypeTag::kColumnValue) {
    return std::optional<Relation>{};
  }
  auto gb_r = resolve_in(inner.GroupBy()[0]->AsColumnValue().GetColumnName());
  if (!gb_r || gb_r->first != 0 ||
      c_schema.GetColumn(gb_r->second).Type() != ValueType::kInt64) {
    return std::optional<Relation>{};
  }
  const slot_t c_pk_slot = gb_r->second;

  std::optional<slot_t> o_cnt_slot;
  for (size_t k = 0; k < 2; ++k) {
    const NamedExpression& ne = inner.SelectList()[k];
    if (!ne.expression) {
      return std::optional<Relation>{};
    }
    if (IdentifierEquals(ProjectionName(ne, k), outer_cnt_col)) {
      if (ne.expression->Type() != TypeTag::kAggregateExp) {
        return std::optional<Relation>{};
      }
      const auto& a = ne.expression->AsAggregateExpression();
      if (a.GetType() != AggregationType::kCount || a.Distinct() ||
          a.WhereFilter() || !a.Child() ||
          a.Child()->Type() != TypeTag::kColumnValue) {
        return std::optional<Relation>{};
      }
      auto cr = resolve_in(a.Child()->AsColumnValue().GetColumnName());
      if (!cr || cr->first != 1) {
        return std::optional<Relation>{};
      }
      o_cnt_slot = cr->second;
    }
  }
  if (!o_cnt_slot) {
    return std::optional<Relation>{};
  }

  // Parse LEFT JOIN ON conjuncts: `c_custkey = o_custkey` + optional
  // `o_comment NOT LIKE '%p1%p2%'` (or `NOT (o_comment LIKE '...')`).
  std::optional<slot_t> o_fk_slot;
  std::optional<slot_t> o_cmt_slot;
  std::vector<std::string> not_like_parts;
  for (const Expression& conj : SplitConjuncts(osrc.join_condition)) {
    if (!conj) {
      return std::optional<Relation>{};
    }
    if (conj->Type() == TypeTag::kBinaryExp &&
        conj->AsBinaryExpression().Op() == BinaryOperation::kEquals &&
        conj->AsBinaryExpression().Left()->Type() == TypeTag::kColumnValue &&
        conj->AsBinaryExpression().Right()->Type() == TypeTag::kColumnValue &&
        !o_fk_slot) {
      auto l = resolve_in(
          conj->AsBinaryExpression().Left()->AsColumnValue().GetColumnName());
      auto r = resolve_in(
          conj->AsBinaryExpression().Right()->AsColumnValue().GetColumnName());
      if (!l || !r) {
        return std::optional<Relation>{};
      }
      if (r->first == 0) {
        std::swap(l, r);
      }
      if (l->first != 0 || l->second != c_pk_slot || r->first != 1 ||
          o_schema.GetColumn(r->second).Type() != ValueType::kInt64) {
        return std::optional<Relation>{};
      }
      o_fk_slot = r->second;
      continue;
    }
    // Match `col NOT LIKE '%a%b%'` or `NOT (col LIKE '%a%b%')`.
    const Expression* like_node = nullptr;
    if (conj->Type() == TypeTag::kBinaryExp &&
        conj->AsBinaryExpression().Op() == BinaryOperation::kNotLike) {
      like_node = &conj;
    } else if (conj->Type() == TypeTag::kUnaryExp &&
               conj->AsUnaryExpression().Op() == UnaryOperation::kNot &&
               conj->AsUnaryExpression().Child() &&
               conj->AsUnaryExpression().Child()->Type() ==
                   TypeTag::kBinaryExp &&
               conj->AsUnaryExpression().Child()->AsBinaryExpression().Op() ==
                   BinaryOperation::kLike) {
      like_node = &conj->AsUnaryExpression().Child();
    }
    if (like_node != nullptr && !o_cmt_slot) {
      const auto& be = (*like_node)->AsBinaryExpression();
      if (be.Left()->Type() != TypeTag::kColumnValue ||
          be.Right()->Type() != TypeTag::kConstantValue) {
        return std::optional<Relation>{};
      }
      auto cr = resolve_in(be.Left()->AsColumnValue().GetColumnName());
      const Value& pv = be.Right()->AsConstantValue().GetValue();
      if (!cr || cr->first != 1 ||
          o_schema.GetColumn(cr->second).Type() != ValueType::kVarChar ||
          pv.IsNull() || pv.type != ValueType::kVarChar) {
        return std::optional<Relation>{};
      }
      const std::string_view pat = pv.value.varchar_value;
      if (pat.size() < 2 || pat.front() != '%' || pat.back() != '%' ||
          pat.find('_') != std::string_view::npos ||
          pat.find('\\') != std::string_view::npos) {
        return std::optional<Relation>{};
      }
      // Split pat on '%' into non-empty literal segments (e.g.
      // "%special%requests%" -> {"special", "requests"}).
      size_t pos = 0;
      while (pos < pat.size()) {
        size_t next = pat.find('%', pos);
        if (next == std::string_view::npos) {
          next = pat.size();
        }
        if (next > pos) {
          not_like_parts.emplace_back(pat.substr(pos, next - pos));
        }
        pos = next + 1;
      }
      if (not_like_parts.empty() || not_like_parts.size() > 3) {
        return std::optional<Relation>{};
      }
      o_cmt_slot = cr->second;
      continue;
    }
    return std::optional<Relation>{};
  }
  if (!o_fk_slot) {
    return std::optional<Relation>{};
  }

  // Check ORDER BY: empty or (custdist DESC/ASC, c_count DESC/ASC).
  struct OrdTerm {
    size_t col{0};  // 0: c_count, 1: custdist
    bool asc{true};
  };
  std::vector<OrdTerm> ord_terms;
  const std::string p0 = ProjectionName(statement.SelectList()[0], 0);
  const std::string p1 = ProjectionName(statement.SelectList()[1], 1);
  for (const auto& ob : statement.OrderBy()) {
    if (!ob.expression || ob.expression->Type() != TypeTag::kColumnValue) {
      return std::optional<Relation>{};
    }
    const std::string nm = ob.expression->AsColumnValue().GetColumnName().name;
    if (IdentifierEquals(nm, p0)) {
      ord_terms.push_back(OrdTerm{.col = 0, .asc = ob.ascending});
    } else if (IdentifierEquals(nm, p1)) {
      ord_terms.push_back(OrdTerm{.col = 1, .asc = ob.ascending});
    } else {
      return std::optional<Relation>{};
    }
  }

  // Step 1: Load customer keys into a dense presence/multiplicity array.
  std::vector<slot_t> ccols{c_pk_slot};
  auto c_store =
      GetOrBuildLSTableStore(context.txn_, *c_tbl, true, &ccols, false);
  if (!c_store || !c_store->HasColumn(c_pk_slot)) {
    return std::optional<Relation>{};
  }
  int64_t c_min = std::numeric_limits<int64_t>::max();
  int64_t c_max = std::numeric_limits<int64_t>::min();
  bool has_null_cust = false;
  for (const LSTablePageBlock& blk : c_store->blocks) {
    if (blk.row_count == 0) {
      continue;
    }
    const auto& c_pk = blk.columns[c_pk_slot];
    if (c_pk.has_null) {
      has_null_cust = true;
    }
    if (!c_pk.has_min_max) {
      return std::optional<Relation>{};
    }
    c_min = std::min(c_min, c_pk.min_int);
    c_max = std::max(c_max, c_pk.max_int);
  }
  if (has_null_cust || c_min > c_max ||
      static_cast<uint64_t>(c_max - c_min) + 1 > 20000000) {
    return std::optional<Relation>{};
  }
  const size_t span = static_cast<size_t>(c_max - c_min) + 1;
  std::vector<uint8_t> cust_present(span, 0);
  // Note: GROUP BY c_custkey collapses duplicate c_custkey rows into 1 group
  // (with COUNT(o_orderkey) multiplied by customer multiplicity if duplicated).
  // Require unique c_custkey for exact COUNT(o_orderkey); fall back on
  // duplicate c_custkey.
  std::vector<std::atomic<uint8_t>> seen_c(span);
  for (auto& a : seen_c) {
    a.store(0, std::memory_order_relaxed);
  }
  std::atomic<bool> dup_c{false};
  const size_t cb = c_store->blocks.size();
  const size_t cw = std::max<size_t>(1, ParallelWorkerCount(cb));
  auto scan_c = [&](size_t w) {
    for (size_t b = w; b < cb; b += cw) {
      if (dup_c.load(std::memory_order_relaxed)) {
        return;
      }
      const LSTablePageBlock& blk = c_store->blocks[b];
      const auto& c_pk = blk.columns[c_pk_slot];
      for (uint32_t r = 0; r < blk.row_count; ++r) {
        const auto off = static_cast<size_t>(c_pk.IntAt(r) - c_min);
        if (seen_c[off].exchange(1, std::memory_order_relaxed) != 0) {
          dup_c.store(true, std::memory_order_relaxed);
          return;
        }
        cust_present[off] = 1;
      }
    }
  };
  if (RunParallelTasks(cw, scan_c) != Status::kSuccess ||
      dup_c.load(std::memory_order_relaxed)) {
    return std::optional<Relation>{};
  }

  // Step 2: Parallel scan of orders counting qualifying non-excluded orders per
  // customer (RebucketOrders_sound).
  std::vector<slot_t> ocols{*o_fk_slot, *o_cnt_slot};
  if (o_cmt_slot) {
    ocols.push_back(*o_cmt_slot);
  }
  std::ranges::sort(ocols);
  ocols.erase(std::ranges::unique(ocols).begin(), ocols.end());
  auto o_store =
      GetOrBuildLSTableStore(context.txn_, *o_tbl, true, &ocols, false);
  if (!o_store || !o_store->HasColumn(*o_fk_slot) ||
      !o_store->HasColumn(*o_cnt_slot) ||
      (o_cmt_slot && !o_store->HasColumn(*o_cmt_slot))) {
    return std::optional<Relation>{};
  }
  std::vector<std::atomic<uint32_t>> ord_cnt(span);
  for (auto& a : ord_cnt) {
    a.store(0, std::memory_order_relaxed);
  }
  auto matches_like = [&](std::string_view sv) noexcept -> bool {
    size_t pos = 0;
    for (const std::string& seg : not_like_parts) {
      const size_t found = sv.find(seg, pos);
      if (found == std::string_view::npos) {
        return false;
      }
      pos = found + seg.size();
    }
    return true;
  };
  const size_t ob = o_store->blocks.size();
  const size_t ow = std::max<size_t>(1, ParallelWorkerCount(ob));
  auto scan_o = [&](size_t w) {
    for (size_t b = w; b < ob; b += ow) {
      const LSTablePageBlock& blk = o_store->blocks[b];
      const uint32_t nr = blk.row_count;
      if (nr == 0) {
        continue;
      }
      const auto& c_fk = blk.columns[*o_fk_slot];
      if (c_fk.has_min_max && (c_fk.max_int < c_min || c_fk.min_int > c_max)) {
        continue;
      }
      const auto& c_ok = blk.columns[*o_cnt_slot];
      const LSTableColumnBlock* c_cm =
          o_cmt_slot ? &blk.columns[*o_cmt_slot] : nullptr;
      for (uint32_t r = 0; r < nr; ++r) {
        if ((c_fk.has_null && c_fk.IsNull(r)) ||
            (c_ok.has_null && c_ok.IsNull(r))) {
          continue;
        }
        const int64_t ck = c_fk.IntAt(r);
        if (ck < c_min || ck > c_max) {
          continue;
        }
        const auto off = static_cast<size_t>(ck - c_min);
        if (cust_present[off] == 0) {
          continue;
        }
        if (c_cm != nullptr) {
          if (c_cm->has_null && c_cm->IsNull(r)) {
            continue;
          }
          if (matches_like(c_cm->VarcharAt(r))) {
            continue;
          }
        }
        ord_cnt[off].fetch_add(1U, std::memory_order_relaxed);
      }
    }
  };
  if (RunParallelTasks(ow, scan_o) != Status::kSuccess) {
    return std::optional<Relation>{};
  }

  // Step 3: Build bucket histogram (preserving c_count == 0 customers).
  std::unordered_map<int64_t, int64_t> hist;
  for (size_t i = 0; i < span; ++i) {
    if (cust_present[i] != 0) {
      ++hist[static_cast<int64_t>(ord_cnt[i].load(std::memory_order_relaxed))];
    }
  }
  std::vector<std::pair<int64_t, int64_t>> out_rows(hist.begin(), hist.end());
  if (!ord_terms.empty()) {
    std::ranges::sort(out_rows, [&](const auto& a, const auto& b) {
      for (const OrdTerm& t : ord_terms) {
        const int64_t va = (t.col == 0) ? a.first : a.second;
        const int64_t vb = (t.col == 0) ? b.first : b.second;
        if (va != vb) {
          return t.asc ? (va < vb) : (va > vb);
        }
      }
      return false;
    });
  }
  Relation out(context.execution_runtime());
  out.schema = Schema(
      "", {Column(p0, ValueType::kInt64), Column(p1, ValueType::kInt64)});
  out.rows.reserve(out_rows.size());
  for (const auto& [cnt, dist] : out_rows) {
    out.AddRow(Row({Value(cnt), Value(dist)}));
  }
  RETURN_IF_FAIL(out.GetStatus());
  return std::optional<Relation>(std::move(out));
}

}  // namespace tinylamb::relational_detail
