/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/parallel_work.hpp"
#include "common/platform/process.hpp"
#include "common/set_operation.hpp"
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
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
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

// Lean-backed
// (lean/TinylambOpt/ResearchProofs/QueryRewrites/SurrogateKeyRollupsAndSetCounts.lean:
// SurrogateIntersectSetCount_sound) fast path for multi-channel customer/date
// INTERSECT/EXCEPT COUNT(*) queries (TPC-DS Q38 & Q87):
//   SELECT COUNT(*) FROM (
//     SELECT DISTINCT c_last_name, c_first_name, d_date
//     FROM fact_0, dimA, dimB WHERE fact_0.fkA = dimA.pk AND fact_0.fkB =
//     dimB.pk
//       AND <shared dimA filter>
//     INTERSECT DISTINCT / EXCEPT DISTINCT
//     SELECT DISTINCT c_last_name, c_first_name, d_date
//     FROM fact_1, dimA, dimB WHERE ...
//   ) alias [LIMIT L]
//
// Maps dimA projected values -> dense uint32_t date_id and dimB projected
// (col1, col2) pairs -> dense uint32_t name_id ONCE, then executes all fact
// scans as zero-join, zero-string-hash parallel packed-uint64 set operations
// (smallest-first for INTERSECT, with live_names bitset semi-filtering).
StatusOr<std::optional<Relation>> TryExecuteSurrogateIntersectCount(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& ctes) {
  if (::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_SURROGATE_INTERSECT") !=
      nullptr) {
    return std::optional<Relation>{};
  }
  if (!statement.WithQueries().empty() || !statement.UnionAll().empty() ||
      statement.GetSetOperationTree() != nullptr || statement.WhereClause() ||
      !statement.GroupBy().empty() || statement.Having() ||
      statement.Distinct() || statement.HasDistinctOn() ||
      statement.Offset() != 0 ||
      (statement.HasLimit() && statement.Limit() == 0) ||
      statement.SelectList().size() != 1 || statement.Sources().size() != 1 ||
      HasWindowFunctions(statement)) {
    return std::optional<Relation>{};
  }
  // Outer SELECT must be COUNT(*) or COUNT(1).
  const Expression& s0 = statement.SelectList()[0].expression;
  if (!s0 || s0->Type() != TypeTag::kAggregateExp) {
    return std::optional<Relation>{};
  }
  const auto& cagg = s0->AsAggregateExpression();
  if (cagg.GetType() != AggregationType::kCount || cagg.Distinct() ||
      cagg.WhereFilter() ||
      (cagg.Child() && cagg.Child()->Type() != TypeTag::kConstantValue &&
       (cagg.Child()->Type() != TypeTag::kColumnValue ||
        cagg.Child()->AsColumnValue().GetColumnName().name != "*"))) {
    return std::optional<Relation>{};
  }
  const SelectSource& outer_src = statement.Sources()[0];
  if (!outer_src.query || outer_src.unnest || outer_src.is_lateral ||
      outer_src.sample.has_value() || !outer_src.using_columns.empty()) {
    return std::optional<Relation>{};
  }
  const SelectStatement& derived = *outer_src.query;
  if (!derived.WithQueries().empty() || derived.HasLimit() ||
      derived.Offset() != 0) {
    return std::optional<Relation>{};
  }
  // Collect branches from either SetOperationTree (parenthesized / nested
  // INTERSECT/EXCEPT) or flat UnionAll() + SetOperationKinds().
  std::optional<SetOperationKind> op_kind_opt;
  std::vector<const SelectStatement*> branches;
  auto collect_set_branches = [&](const SelectStatement& node,
                                  auto& self) -> bool {
    if (!node.WithQueries().empty() || node.HasLimit() || node.Offset() != 0) {
      return false;
    }
    if (const auto& tree = node.GetSetOperationTree()) {
      if (!tree->first || tree->branches.empty() ||
          tree->kinds.size() != tree->branches.size()) {
        return false;
      }
      for (SetOperationKind k : tree->kinds) {
        if (k != SetOperationKind::kIntersect &&
            k != SetOperationKind::kExcept) {
          return false;
        }
        if (!op_kind_opt.has_value()) {
          op_kind_opt = k;
        } else if (*op_kind_opt != k) {
          return false;
        }
      }
      if (!self(*tree->first, self)) {
        return false;
      }
      for (const auto& br : tree->branches) {
        if (!br) {
          return false;
        }
        // For EXCEPT, only left-nested trees are equivalent to flat
        // A EXCEPT B EXCEPT C; reject right-nested EXCEPT trees.
        if (op_kind_opt.has_value() &&
            *op_kind_opt == SetOperationKind::kExcept &&
            (br->GetSetOperationTree() != nullptr || !br->UnionAll().empty())) {
          return false;
        }
        if (!self(*br, self)) {
          return false;
        }
      }
      return true;
    }
    if (!node.UnionAll().empty()) {
      if (node.SetOperationKinds().size() != node.UnionAll().size()) {
        return false;
      }
      for (SetOperationKind k : node.SetOperationKinds()) {
        if (k != SetOperationKind::kIntersect &&
            k != SetOperationKind::kExcept) {
          return false;
        }
        if (!op_kind_opt.has_value()) {
          op_kind_opt = k;
        } else if (*op_kind_opt != k) {
          return false;
        }
      }
      branches.push_back(&node);
      for (const auto& u : node.UnionAll()) {
        if (!u) {
          return false;
        }
        if (op_kind_opt.has_value() &&
            *op_kind_opt == SetOperationKind::kExcept &&
            (u->GetSetOperationTree() != nullptr || !u->UnionAll().empty())) {
          return false;
        }
        if (!self(*u, self)) {
          return false;
        }
      }
      return true;
    }
    branches.push_back(&node);
    return true;
  };
  if (!collect_set_branches(derived, collect_set_branches) ||
      !op_kind_opt.has_value() || branches.size() < 2) {
    return std::optional<Relation>{};
  }
  const bool is_intersect = (*op_kind_opt == SetOperationKind::kIntersect);

  struct BranchInfo {
    std::shared_ptr<Table> fact_tbl;
    slot_t fact_a_slot{0};
    slot_t fact_b_slot{0};
    std::string dimA_table;
    std::string dimB_table;
    slot_t dimA_pk{0};
    slot_t dimA_val{0};
    slot_t dimB_pk{0};
    slot_t dimB_c1{0};
    slot_t dimB_c2{0};
    std::vector<Expression> dimA_filters;
    std::vector<Expression> dimB_filters;
    std::string dimA_filter_key;
    std::string dimB_filter_key;
    std::array<int, 3> out_sig{};  // maps select[0..2] to {0:A, 1:B1, 2:B2}
  };
  std::vector<BranchInfo> binfo(branches.size());

  for (size_t bi = 0; bi < branches.size(); ++bi) {
    const SelectStatement& br = *branches[bi];
    if (!br.WithQueries().empty() || !br.GroupBy().empty() || br.IsGroupAll() ||
        br.Having() || br.HasLimit() || br.Offset() != 0 ||
        HasWindowFunctions(br) || br.SelectList().size() != 3 ||
        br.Sources().size() != 3) {
      return std::optional<Relation>{};
    }
    std::vector<std::shared_ptr<Table>> btbls(3);
    std::vector<Schema> bschemas(3);
    std::vector<std::string> baliases(3);
    for (size_t i = 0; i < 3; ++i) {
      const SelectSource& s = br.Sources()[i];
      if (s.table.empty() || s.query != nullptr || s.unnest != nullptr ||
          s.is_lateral || s.sample.has_value() || !s.using_columns.empty() ||
          ctes.contains(AsciiToLower(s.table)) ||
          (i > 0 && s.join_type != JoinType::kInner &&
           s.join_type != JoinType::kCross)) {
        return std::optional<Relation>{};
      }
      ASSIGN_OR_RETURN(std::shared_ptr<Table>, tbl, context.GetTable(s.table));
      if (!tbl) {
        return std::optional<Relation>{};
      }
      btbls[i] = std::move(tbl);
      bschemas[i] = btbls[i]->GetSchema();
      baliases[i] = AsciiToLower(s.alias.empty() ? s.table : s.alias);
    }
    auto resolve_bcol =
        [&](const ColumnName& cn) -> std::optional<std::pair<size_t, slot_t>> {
      const std::string wn = AsciiToLower(cn.name);
      const std::string ws = AsciiToLower(cn.schema);
      std::optional<std::pair<size_t, slot_t>> found;
      for (size_t i = 0; i < 3; ++i) {
        if (!ws.empty() && baliases[i] != ws &&
            AsciiToLower(br.Sources()[i].table) != ws) {
          continue;
        }
        for (slot_t c = 0; c < bschemas[i].ColumnCount(); ++c) {
          if (AsciiToLower(bschemas[i].GetColumn(c).Name().name) == wn) {
            if (found.has_value()) {
              return std::nullopt;
            }
            found = {i, c};
          }
        }
      }
      return found;
    };

    // Resolve the 3 SELECT columns: must touch 2 sources (1 col from dimA,
    // 2 cols from dimB); the untouched 3rd source is fact.
    std::array<std::pair<size_t, slot_t>, 3> sel_cols;
    std::array<int, 3> src_sel_cnt{0, 0, 0};
    for (size_t k = 0; k < 3; ++k) {
      const Expression& e = br.SelectList()[k].expression;
      if (!e || e->Type() != TypeTag::kColumnValue) {
        return std::optional<Relation>{};
      }
      auto r = resolve_bcol(e->AsColumnValue().GetColumnName());
      if (!r) {
        return std::optional<Relation>{};
      }
      sel_cols[k] = *r;
      ++src_sel_cnt[r->first];
    }
    std::optional<size_t> fact_i;
    std::optional<size_t> dimA_i;
    std::optional<size_t> dimB_i;
    for (size_t i = 0; i < 3; ++i) {
      if (src_sel_cnt[i] == 0) {
        fact_i = i;
      } else if (src_sel_cnt[i] == 1) {
        dimA_i = i;
      } else if (src_sel_cnt[i] == 2) {
        dimB_i = i;
      }
    }
    if (!fact_i || !dimA_i || !dimB_i) {
      return std::optional<Relation>{};
    }

    BranchInfo& info = binfo[bi];
    info.fact_tbl = btbls[*fact_i];
    info.dimA_table = AsciiToLower(br.Sources()[*dimA_i].table);
    info.dimB_table = AsciiToLower(br.Sources()[*dimB_i].table);
    bool b_first_set = false;
    for (size_t k = 0; k < 3; ++k) {
      if (sel_cols[k].first == *dimA_i) {
        info.dimA_val = sel_cols[k].second;
        info.out_sig[k] = 0;
      } else if (!b_first_set) {
        info.dimB_c1 = sel_cols[k].second;
        info.out_sig[k] = 1;
        b_first_set = true;
      } else {
        info.dimB_c2 = sel_cols[k].second;
        info.out_sig[k] = 2;
      }
    }
    const ValueType a_val_t = bschemas[*dimA_i].GetColumn(info.dimA_val).Type();
    if (a_val_t != ValueType::kDate && a_val_t != ValueType::kInt64) {
      return std::optional<Relation>{};
    }
    if (bschemas[*dimB_i].GetColumn(info.dimB_c1).Type() !=
            ValueType::kVarChar ||
        bschemas[*dimB_i].GetColumn(info.dimB_c2).Type() !=
            ValueType::kVarChar) {
      return std::optional<Relation>{};
    }

    // Partition WHERE + ON conjuncts into local filters and 2 equijoin edges.
    std::vector<Expression> all_conj;
    if (br.WhereClause()) {
      auto wc = SplitConjuncts(br.WhereClause());
      all_conj.insert(all_conj.end(), wc.begin(), wc.end());
    }
    for (const auto& s : br.Sources()) {
      if (s.join_condition) {
        auto jc = SplitConjuncts(s.join_condition);
        all_conj.insert(all_conj.end(), jc.begin(), jc.end());
      }
    }
    auto collect_touched = [&](const Expression& e, std::set<size_t>* out,
                               auto& self) -> bool {
      if (!e) {
        return true;
      }
      switch (e->Type()) {
        case TypeTag::kColumnValue: {
          auto r = resolve_bcol(e->AsColumnValue().GetColumnName());
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
        case TypeTag::kCastExp:
          return self(e->AsCastExpression().Child(), out, self);
        default:
          return false;
      }
    };

    bool has_edge_a = false;
    bool has_edge_b = false;
    for (const Expression& conj : all_conj) {
      std::set<size_t> touched;
      if (!collect_touched(conj, &touched, collect_touched) ||
          touched.empty()) {
        return std::optional<Relation>{};
      }
      if (touched.size() == 1) {
        const size_t t = *touched.begin();
        if (t == *fact_i) {
          return std::optional<Relation>{};
        }
        if (t == *dimA_i) {
          info.dimA_filters.push_back(conj);
        } else {
          info.dimB_filters.push_back(conj);
        }
        continue;
      }
      if (touched.size() == 2 && conj->Type() == TypeTag::kBinaryExp &&
          conj->AsBinaryExpression().Op() == BinaryOperation::kEquals &&
          conj->AsBinaryExpression().Left()->Type() == TypeTag::kColumnValue &&
          conj->AsBinaryExpression().Right()->Type() == TypeTag::kColumnValue) {
        auto l = resolve_bcol(
            conj->AsBinaryExpression().Left()->AsColumnValue().GetColumnName());
        auto r = resolve_bcol(conj->AsBinaryExpression()
                                  .Right()
                                  ->AsColumnValue()
                                  .GetColumnName());
        if (!l || !r || l->first == r->first) {
          return std::optional<Relation>{};
        }
        if (r->first == *fact_i) {
          std::swap(l, r);
        }
        if (l->first != *fact_i) {
          return std::optional<Relation>{};
        }
        if (bschemas[l->first].GetColumn(l->second).Type() !=
                ValueType::kInt64 ||
            bschemas[r->first].GetColumn(r->second).Type() !=
                ValueType::kInt64) {
          return std::optional<Relation>{};
        }
        if (r->first == *dimA_i && !has_edge_a) {
          info.fact_a_slot = l->second;
          info.dimA_pk = r->second;
          has_edge_a = true;
          continue;
        }
        if (r->first == *dimB_i && !has_edge_b) {
          info.fact_b_slot = l->second;
          info.dimB_pk = r->second;
          has_edge_b = true;
          continue;
        }
      }
      return std::optional<Relation>{};
    }
    if (!has_edge_a || !has_edge_b) {
      return std::optional<Relation>{};
    }
    auto filter_sig = [](const std::vector<Expression>& fs) -> std::string {
      std::vector<std::string> parts;
      parts.reserve(fs.size());
      for (const Expression& e : fs) {
        parts.push_back(e->ToString());
      }
      std::ranges::sort(parts);
      std::string out;
      for (const std::string& p : parts) {
        out += p;
        out.push_back('\x1f');
      }
      return out;
    };
    info.dimA_filter_key = filter_sig(info.dimA_filters);
    info.dimB_filter_key = filter_sig(info.dimB_filters);

    if (bi > 0) {
      const BranchInfo& b0 = binfo[0];
      if (info.dimA_table != b0.dimA_table ||
          info.dimB_table != b0.dimB_table || info.dimA_pk != b0.dimA_pk ||
          info.dimA_val != b0.dimA_val || info.dimB_pk != b0.dimB_pk ||
          info.dimB_c1 != b0.dimB_c1 || info.dimB_c2 != b0.dimB_c2 ||
          info.dimA_filter_key != b0.dimA_filter_key ||
          info.dimB_filter_key != b0.dimB_filter_key ||
          info.out_sig != b0.out_sig) {
        return std::optional<Relation>{};
      }
    }
  }

  auto make_count_result = [&](int64_t cnt) -> StatusOr<Relation> {
    Relation out(context.execution_runtime());
    out.schema =
        Schema("", {Column(ProjectionName(statement.SelectList()[0], 0),
                           ValueType::kInt64)});
    out.AddRow(Row({Value(cnt)}));
    RETURN_IF_FAIL(out.GetStatus());
    return out;
  };

  // Step 1: Load dimA (e.g. date_dim) ONCE and assign compact date_id (1..nA).
  const BranchInfo& b0 = binfo[0];
  ASSIGN_OR_RETURN(std::shared_ptr<Table>, dimA_tbl,
                   context.GetTable(b0.dimA_table));
  ASSIGN_OR_RETURN(std::shared_ptr<Table>, dimB_tbl,
                   context.GetTable(b0.dimB_table));
  if (!dimA_tbl || !dimB_tbl) {
    return std::optional<Relation>{};
  }
  const Schema dimA_schema = dimA_tbl->GetSchema();
  const Schema dimB_schema = dimB_tbl->GetSchema();

  auto scan_shared_dim =
      [&](const std::string& tbl_name, const Schema& sc,
          std::vector<slot_t> cols,
          const std::vector<Expression>& filters) -> StatusOr<Relation> {
    std::ranges::sort(cols);
    cols.erase(std::ranges::unique(cols).begin(), cols.end());
    SelectStatement ds;
    SelectSource src;
    src.table = tbl_name;
    src.join_type = JoinType::kInner;
    ds.SetSources({std::move(src)});
    std::vector<NamedExpression> sel;
    for (slot_t c : cols) {
      const ColumnName& cn = sc.GetColumn(c).Name();
      sel.emplace_back(cn.name, ColumnValueExp(cn.name));
    }
    ds.SetSelectList(std::move(sel));
    if (!filters.empty()) {
      ds.SetWhereClause(CombineConjuncts(filters));
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

  ASSIGN_OR_RETURN(Relation, dimA_rel,
                   scan_shared_dim(b0.dimA_table, dimA_schema,
                                   {b0.dimA_pk, b0.dimA_val}, b0.dimA_filters));
  if (dimA_rel.HasSpill()) {
    return std::optional<Relation>{};
  }
  auto dA_pk_o = LocalColumnOffset(
      dimA_rel.schema,
      ColumnName(dimA_schema.GetColumn(b0.dimA_pk).Name().name));
  auto dA_vl_o = LocalColumnOffset(
      dimA_rel.schema,
      ColumnName(dimA_schema.GetColumn(b0.dimA_val).Name().name));
  if (!dA_pk_o || !dA_vl_o) {
    return std::optional<Relation>{};
  }
  FlatIntKeyMap<uint32_t> date_val_to_id;
  uint32_t next_date_id = 1;
  uint32_t null_date_id = 0;
  FlatIntKeyMap<uint32_t> dimA_map;
  dimA_map.Reserve(dimA_rel.rows.size());
  int64_t dA_min = std::numeric_limits<int64_t>::max();
  int64_t dA_max = std::numeric_limits<int64_t>::min();
  for (const Row& r : dimA_rel.rows) {
    const Value& pk = r[*dA_pk_o];
    if (pk.IsNull()) {
      continue;
    }
    const Value& vl = r[*dA_vl_o];
    uint32_t did = 0;
    if (vl.IsNull()) {
      if (null_date_id == 0) {
        null_date_id = next_date_id++;
      }
      did = null_date_id;
    } else {
      bool emp = false;
      uint32_t* sl = date_val_to_id.TryEmplace(vl.value.int_value, &emp);
      if (emp) {
        *sl = next_date_id++;
      }
      did = *sl;
    }
    bool pemp = false;
    uint32_t* psl = dimA_map.TryEmplace(pk.value.int_value, &pemp);
    if (!pemp && *psl != did) {
      return std::optional<Relation>{};
    }
    *psl = did;
    dA_min = std::min(dA_min, pk.value.int_value);
    dA_max = std::max(dA_max, pk.value.int_value);
  }
  if (dimA_map.Size() == 0) {
    ASSIGN_OR_RETURN(Relation, z, make_count_result(0));
    return std::optional<Relation>(std::move(z));
  }
  std::vector<uint32_t> dimA_arr;
  if (static_cast<uint64_t>(dA_max - dA_min) + 1 <= 200000) {
    dimA_arr.assign(static_cast<size_t>(dA_max - dA_min) + 1, 0);
    dimA_map.ForEach([&](int64_t k, uint32_t id) {
      dimA_arr[static_cast<size_t>(k - dA_min)] = id;
    });
  }
  auto lookup_dimA = [&](int64_t k) noexcept -> uint32_t {
    if (k < dA_min || k > dA_max) {
      return 0;
    }
    if (!dimA_arr.empty()) {
      return dimA_arr[static_cast<size_t>(k - dA_min)];
    }
    const uint32_t* p = dimA_map.Find(k);
    return p ? *p : 0;
  };

  // Step 2: Load dimB (e.g. customer) ONCE and assign compact name_id (1..nB)
  // to each distinct (c1, c2) pair (treating NULL == NULL).
  uint32_t next_name_id = 1;
  FlatIntKeyMap<uint32_t> dimB_map;
  int64_t dB_min = std::numeric_limits<int64_t>::max();
  int64_t dB_max = std::numeric_limits<int64_t>::min();

  auto hash_pair = [](bool n1, std::string_view s1, bool n2,
                      std::string_view s2) noexcept -> uint64_t {
    uint64_t h = 14695981039346656037ULL;
    auto mix = [&](std::string_view sv, bool is_null) {
      h ^= is_null ? 0x9e3779b97f4a7c15ULL : 0x517cc1b727220a95ULL;
      h *= 1099511628211ULL;
      if (!is_null) {
        h ^= std::hash<std::string_view>{}(sv);
        h *= 1099511628211ULL;
      }
    };
    mix(s1, n1);
    mix(s2, n2);
    return h ^ (h >> 33);
  };

  bool dimB_fast = false;
  if (b0.dimB_filters.empty()) {
    std::vector<slot_t> bcols{b0.dimB_pk, b0.dimB_c1, b0.dimB_c2};
    std::ranges::sort(bcols);
    bcols.erase(std::ranges::unique(bcols).begin(), bcols.end());
    if (auto st = GetOrBuildLSTableStore(context.txn_, *dimB_tbl, true, &bcols,
                                         /*allow_block_lazy=*/false);
        st && st->HasColumn(b0.dimB_pk) && st->HasColumn(b0.dimB_c1) &&
        st->HasColumn(b0.dimB_c2)) {
      dimB_fast = true;
      // Zero-allocation open-addressed intern table referencing (blk, row)
      // directly inside `st`.
      size_t cap = 16;
      while (cap < (st->total_rows * 2) + 16) {
        cap <<= 1U;
      }
      struct InternSlot {
        uint64_t hash{0};
        uint32_t bi{0};
        uint32_t ri{0};
        uint32_t id{0};
      };
      std::vector<InternSlot> table(cap);
      const size_t mask = cap - 1;
      dimB_map.Reserve(st->total_rows);
      for (size_t bi = 0; bi < st->blocks.size(); ++bi) {
        const LSTablePageBlock& blk = st->blocks[bi];
        const auto& c_pk = blk.columns[b0.dimB_pk];
        const auto& c_1 = blk.columns[b0.dimB_c1];
        const auto& c_2 = blk.columns[b0.dimB_c2];
        for (uint32_t r = 0; r < blk.row_count; ++r) {
          if (c_pk.has_null && c_pk.IsNull(r)) {
            continue;
          }
          const bool n1 = c_1.has_null && c_1.IsNull(r);
          const bool n2 = c_2.has_null && c_2.IsNull(r);
          const std::string_view sv1 =
              n1 ? std::string_view{} : c_1.VarcharAt(r);
          const std::string_view sv2 =
              n2 ? std::string_view{} : c_2.VarcharAt(r);
          const uint64_t h = hash_pair(n1, sv1, n2, sv2);
          size_t idx = static_cast<size_t>(h) & mask;
          uint32_t nid = 0;
          while (true) {
            InternSlot& sl = table[idx];
            if (sl.id == 0) {
              nid = next_name_id++;
              sl = InternSlot{.hash = h,
                              .bi = static_cast<uint32_t>(bi),
                              .ri = r,
                              .id = nid};
              break;
            }
            if (sl.hash == h) {
              const LSTablePageBlock& ob = st->blocks[sl.bi];
              const auto& oc1 = ob.columns[b0.dimB_c1];
              const auto& oc2 = ob.columns[b0.dimB_c2];
              const bool on1 = oc1.has_null && oc1.IsNull(sl.ri);
              const bool on2 = oc2.has_null && oc2.IsNull(sl.ri);
              if (on1 == n1 && on2 == n2 &&
                  (n1 || oc1.VarcharAt(sl.ri) == sv1) &&
                  (n2 || oc2.VarcharAt(sl.ri) == sv2)) {
                nid = sl.id;
                break;
              }
            }
            idx = (idx + 1) & mask;
          }
          const int64_t pk = c_pk.IntAt(r);
          bool pemp = false;
          uint32_t* psl = dimB_map.TryEmplace(pk, &pemp);
          if (!pemp && *psl != nid) {
            return std::optional<Relation>{};
          }
          *psl = nid;
          dB_min = std::min(dB_min, pk);
          dB_max = std::max(dB_max, pk);
        }
      }
    }
  }
  if (!dimB_fast) {
    ASSIGN_OR_RETURN(
        Relation, dimB_rel,
        scan_shared_dim(b0.dimB_table, dimB_schema,
                        {b0.dimB_pk, b0.dimB_c1, b0.dimB_c2}, b0.dimB_filters));
    if (dimB_rel.HasSpill()) {
      return std::optional<Relation>{};
    }
    auto dB_pk_o = LocalColumnOffset(
        dimB_rel.schema,
        ColumnName(dimB_schema.GetColumn(b0.dimB_pk).Name().name));
    auto dB_1_o = LocalColumnOffset(
        dimB_rel.schema,
        ColumnName(dimB_schema.GetColumn(b0.dimB_c1).Name().name));
    auto dB_2_o = LocalColumnOffset(
        dimB_rel.schema,
        ColumnName(dimB_schema.GetColumn(b0.dimB_c2).Name().name));
    if (!dB_pk_o || !dB_1_o || !dB_2_o) {
      return std::optional<Relation>{};
    }
    std::unordered_map<std::string, uint32_t> pair_to_id;
    pair_to_id.reserve(dimB_rel.rows.size());
    std::string key_buf;
    dimB_map.Reserve(dimB_rel.rows.size());
    for (const Row& r : dimB_rel.rows) {
      const Value& pk = r[*dB_pk_o];
      if (pk.IsNull()) {
        continue;
      }
      const Value& val1 = r[*dB_1_o];
      const Value& val2 = r[*dB_2_o];
      key_buf.clear();
      key_buf.push_back(val1.IsNull() ? '\0' : '\1');
      if (!val1.IsNull()) {
        key_buf.append(val1.value.varchar_value);
      }
      key_buf.push_back('\x1f');
      key_buf.push_back(val2.IsNull() ? '\0' : '\1');
      if (!val2.IsNull()) {
        key_buf.append(val2.value.varchar_value);
      }
      auto [it, ins] = pair_to_id.emplace(key_buf, next_name_id);
      if (ins) {
        ++next_name_id;
      }
      const uint32_t nid = it->second;
      bool pemp = false;
      uint32_t* psl = dimB_map.TryEmplace(pk.value.int_value, &pemp);
      if (!pemp && *psl != nid) {
        return std::optional<Relation>{};
      }
      *psl = nid;
      dB_min = std::min(dB_min, pk.value.int_value);
      dB_max = std::max(dB_max, pk.value.int_value);
    }
  }
  if (dimB_map.Size() == 0) {
    ASSIGN_OR_RETURN(Relation, z, make_count_result(0));
    return std::optional<Relation>(std::move(z));
  }
  std::vector<uint32_t> dimB_arr;
  if (static_cast<uint64_t>(dB_max - dB_min) + 1 <= 20000000) {
    dimB_arr.assign(static_cast<size_t>(dB_max - dB_min) + 1, 0);
    dimB_map.ForEach([&](int64_t k, uint32_t id) {
      dimB_arr[static_cast<size_t>(k - dB_min)] = id;
    });
  }
  auto lookup_dimB = [&](int64_t k) noexcept -> uint32_t {
    if (k < dB_min || k > dB_max) {
      return 0;
    }
    if (!dimB_arr.empty()) {
      return dimB_arr[static_cast<size_t>(k - dB_min)];
    }
    const uint32_t* p = dimB_map.Find(k);
    return p ? *p : 0;
  };

  // Step 3: Load LSTableStore for each fact branch.
  std::vector<std::shared_ptr<const LSTableStore>> fact_stores(binfo.size());
  for (size_t i = 0; i < binfo.size(); ++i) {
    std::vector<slot_t> fcols{binfo[i].fact_a_slot, binfo[i].fact_b_slot};
    std::ranges::sort(fcols);
    fcols.erase(std::ranges::unique(fcols).begin(), fcols.end());
    auto st = GetOrBuildLSTableStore(context.txn_, *binfo[i].fact_tbl, true,
                                     &fcols, /*allow_block_lazy=*/false);
    if (!st || !st->HasColumn(binfo[i].fact_a_slot) ||
        !st->HasColumn(binfo[i].fact_b_slot)) {
      return std::optional<Relation>{};
    }
    fact_stores[i] = std::move(st);
  }

  // Order branches smallest-first for INTERSECT; keep original order for
  // EXCEPT.
  std::vector<size_t> order(binfo.size());
  std::iota(order.begin(), order.end(), 0);
  if (is_intersect) {
    std::ranges::sort(order, [&](size_t a, size_t b) {
      return fact_stores[a]->total_rows < fact_stores[b]->total_rows;
    });
  }

  auto pack_key = [](uint32_t nid, uint32_t did) noexcept -> int64_t {
    return static_cast<int64_t>((static_cast<uint64_t>(nid) << 32) | did);
  };

  // Seed set from order[0].
  FlatIntKeyMap<uint8_t> cur_set;
  std::vector<uint8_t> live_names(next_name_id, 0);
  {
    const size_t bi0 = order[0];
    const auto& st = *fact_stores[bi0];
    const slot_t fa = binfo[bi0].fact_a_slot;
    const slot_t fb = binfo[bi0].fact_b_slot;
    const size_t nb = st.blocks.size();
    const size_t nw = std::max<size_t>(1, ParallelWorkerCount(nb));
    std::vector<std::vector<int64_t>> wkeys(nw);
    auto scan0 = [&](size_t w) {
      auto& out = wkeys[w];
      for (size_t b = w; b < nb; b += nw) {
        const LSTablePageBlock& blk = st.blocks[b];
        const uint32_t nr = blk.row_count;
        if (nr == 0) {
          continue;
        }
        const auto& c_a = blk.columns[fa];
        if (c_a.has_min_max && (c_a.max_int < dA_min || c_a.min_int > dA_max)) {
          continue;
        }
        const auto& c_b = blk.columns[fb];
        for (uint32_t r = 0; r < nr; ++r) {
          if ((c_a.has_null && c_a.IsNull(r)) ||
              (c_b.has_null && c_b.IsNull(r))) {
            continue;
          }
          const uint32_t did = lookup_dimA(c_a.IntAt(r));
          if (did == 0) {
            continue;
          }
          const uint32_t nid = lookup_dimB(c_b.IntAt(r));
          if (nid == 0) {
            continue;
          }
          out.push_back(pack_key(nid, did));
        }
      }
    };
    if (RunParallelTasks(nw, scan0) != Status::kSuccess) {
      return std::optional<Relation>{};
    }
    size_t tot = 0;
    for (const auto& v : wkeys) {
      tot += v.size();
    }
    cur_set.Reserve(std::min<size_t>(tot, 1U << 22));
    for (const auto& v : wkeys) {
      for (int64_t k : v) {
        bool emp = false;
        uint8_t* sl = cur_set.TryEmplace(k, &emp);
        if (emp) {
          *sl = 1;
          live_names[static_cast<uint32_t>(static_cast<uint64_t>(k) >> 32)] = 1;
        }
      }
    }
  }
  if (cur_set.Size() == 0) {
    ASSIGN_OR_RETURN(Relation, z, make_count_result(0));
    return std::optional<Relation>(std::move(z));
  }

  // Probe subsequent branches.
  for (size_t oi = 1; oi < order.size(); ++oi) {
    const size_t bi = order[oi];
    const auto& st = *fact_stores[bi];
    const slot_t fa = binfo[bi].fact_a_slot;
    const slot_t fb = binfo[bi].fact_b_slot;
    const size_t nb = st.blocks.size();
    const size_t nw = std::max<size_t>(1, ParallelWorkerCount(nb));
    std::vector<std::vector<int64_t>> whits(nw);
    auto scan_probe = [&](size_t w) {
      auto& hits = whits[w];
      for (size_t b = w; b < nb; b += nw) {
        const LSTablePageBlock& blk = st.blocks[b];
        const uint32_t nr = blk.row_count;
        if (nr == 0) {
          continue;
        }
        const auto& c_a = blk.columns[fa];
        if (c_a.has_min_max && (c_a.max_int < dA_min || c_a.min_int > dA_max)) {
          continue;
        }
        const auto& c_b = blk.columns[fb];
        for (uint32_t r = 0; r < nr; ++r) {
          if ((c_a.has_null && c_a.IsNull(r)) ||
              (c_b.has_null && c_b.IsNull(r))) {
            continue;
          }
          const uint32_t did = lookup_dimA(c_a.IntAt(r));
          if (did == 0) {
            continue;
          }
          const uint32_t nid = lookup_dimB(c_b.IntAt(r));
          if (nid == 0 || live_names[nid] == 0) {
            continue;
          }
          const int64_t k = pack_key(nid, did);
          if (const uint8_t* p = cur_set.Find(k); p && *p != 0) {
            hits.push_back(k);
          }
        }
      }
    };
    if (RunParallelTasks(nw, scan_probe) != Status::kSuccess) {
      return std::optional<Relation>{};
    }
    if (is_intersect) {
      FlatIntKeyMap<uint8_t> next_set;
      std::vector<uint8_t> next_live(next_name_id, 0);
      for (const auto& hits : whits) {
        for (int64_t k : hits) {
          bool emp = false;
          uint8_t* sl = next_set.TryEmplace(k, &emp);
          if (emp) {
            *sl = 1;
            next_live[static_cast<uint32_t>(static_cast<uint64_t>(k) >> 32)] =
                1;
          }
        }
      }
      cur_set = std::move(next_set);
      live_names = std::move(next_live);
      if (cur_set.Size() == 0) {
        ASSIGN_OR_RETURN(Relation, z, make_count_result(0));
        return std::optional<Relation>(std::move(z));
      }
    } else {
      for (const auto& hits : whits) {
        for (int64_t k : hits) {
          if (const uint8_t* p = cur_set.Find(k)) {
            *const_cast<uint8_t*>(p) = 0;
          }
        }
      }
    }
  }

  int64_t final_cnt = 0;
  if (is_intersect) {
    final_cnt = static_cast<int64_t>(cur_set.Size());
  } else {
    cur_set.ForEach([&](int64_t, uint8_t v) {
      if (v != 0) {
        ++final_cnt;
      }
    });
  }
  ASSIGN_OR_RETURN(Relation, res, make_count_result(final_cnt));
  return std::optional<Relation>(std::move(res));
}

}  // namespace tinylamb::relational_detail
