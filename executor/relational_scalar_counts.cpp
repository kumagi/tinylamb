/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <array>
#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/parallel_work.hpp"
#include "common/query_parallelism.hpp"
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/aggregation.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/relational_fast_paths.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/window_eval.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "table/table.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

// Fast scalar `COUNT(*)` / `COUNT(CASE WHEN col IS [NOT] NULL ... THEN 1 END)`
// directly over a ColumnarTable CTE without materializing Row objects.
StatusOr<std::optional<Relation>> TryExecuteColumnarNullMaskScalarCounts(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& inherited_ctes) {
  if (!statement.WithQueries().empty() || !statement.UnionAll().empty() ||
      statement.GetSetOperationTree() != nullptr || statement.WhereClause() ||
      !statement.GroupBy().empty() || statement.IsGroupAll() ||
      statement.Having() || statement.Qualify() || statement.Distinct() ||
      statement.HasDistinctOn() || !statement.OrderBy().empty() ||
      statement.Offset() != 0 ||
      (statement.HasLimit() && statement.Limit() == 0) ||
      statement.AsStruct() || statement.ForUpdate() ||
      HasWindowFunctions(statement) || statement.Sources().size() != 1 ||
      statement.SelectList().empty()) {
    return std::optional<Relation>{};
  }
  const SelectSource& src = statement.Sources()[0];
  if (src.query || src.unnest || src.is_lateral || src.sample ||
      !src.using_columns.empty() || src.from_nested_join ||
      src.join_condition) {
    return std::optional<Relation>{};
  }
  const auto cte_it = inherited_ctes.find(src.table);
  if (cte_it == inherited_ctes.end() || cte_it->second == nullptr ||
      !cte_it->second->HasColumnarOnly() ||
      cte_it->second->columnar == nullptr || cte_it->second->HasSpill()) {
    return std::optional<Relation>{};
  }
  const Relation& cte_rel = *cte_it->second;
  const std::string alias = src.alias.empty() ? src.table : src.alias;
  const Schema view_schema = QualifySchema(cte_rel.schema, alias);

  struct NullClause {
    struct Atom {
      size_t col{0};
      bool want_not_null{true};
    };
    bool is_or{false};
    std::vector<Atom> atoms;
  };
  struct ItemSpec {
    enum class Kind : uint8_t { kConst, kCountStar, kCountMasked };
    Kind kind{Kind::kConst};
    Value const_val;
    std::vector<NullClause> conj_clauses;
  };

  auto parse_atom = [&](const Expression& e, NullClause::Atom* out) -> bool {
    if (!e || e->Type() != TypeTag::kUnaryExp) {
      return false;
    }
    const auto& u = e->AsUnaryExpression();
    if ((u.Op() != UnaryOperation::kIsNull &&
         u.Op() != UnaryOperation::kIsNotNull) ||
        !u.Child() || u.Child()->Type() != TypeTag::kColumnValue) {
      return false;
    }
    const auto off = LocalColumnOffset(
        view_schema, u.Child()->AsColumnValue().GetColumnName());
    if (!off.has_value()) {
      return false;
    }
    out->col = *off;
    out->want_not_null = (u.Op() == UnaryOperation::kIsNotNull);
    return true;
  };

  auto split_disjuncts = [](const Expression& e, std::vector<Expression>* out) {
    std::vector<Expression> stack{e};
    while (!stack.empty()) {
      Expression cur = std::move(stack.back());
      stack.pop_back();
      if (cur && cur->Type() == TypeTag::kBinaryExp &&
          cur->AsBinaryExpression().Op() == BinaryOperation::kOr) {
        stack.push_back(cur->AsBinaryExpression().Right());
        stack.push_back(cur->AsBinaryExpression().Left());
      } else {
        out->push_back(std::move(cur));
      }
    }
  };

  std::vector<ItemSpec> specs(statement.SelectList().size());
  bool any_agg = false;
  for (size_t i = 0; i < statement.SelectList().size(); ++i) {
    const Expression& expr = statement.SelectList()[i].expression;
    if (!expr || statement.SelectList()[i].star_modifiers != nullptr) {
      return std::optional<Relation>{};
    }
    if (expr->Type() == TypeTag::kConstantValue) {
      specs[i].kind = ItemSpec::Kind::kConst;
      specs[i].const_val = expr->AsConstantValue().GetValue();
      continue;
    }
    if (expr->Type() != TypeTag::kAggregateExp) {
      return std::optional<Relation>{};
    }
    const auto& agg = expr->AsAggregateExpression();
    if (agg.GetType() != AggregationType::kCount || agg.Distinct() ||
        agg.WhereFilter() || agg.Having() != AggregateHavingModifier::kNone ||
        !agg.InnerOrderBy().empty() || agg.InnerLimit().has_value() ||
        agg.NeedsGroupContext()) {
      return std::optional<Relation>{};
    }
    any_agg = true;
    if (IsCountStar(agg)) {
      specs[i].kind = ItemSpec::Kind::kCountStar;
      continue;
    }
    if (!agg.Child() || agg.Child()->Type() != TypeTag::kCaseExp) {
      return std::optional<Relation>{};
    }
    const auto& ce = agg.Child()->AsCaseExpression();
    if (ce.when_clauses_.size() != 1 || ce.else_clause_ != nullptr ||
        !ce.when_clauses_[0].second ||
        ce.when_clauses_[0].second->Type() != TypeTag::kConstantValue ||
        ce.when_clauses_[0].second->AsConstantValue().GetValue().IsNull()) {
      return std::optional<Relation>{};
    }
    specs[i].kind = ItemSpec::Kind::kCountMasked;
    for (const Expression& cj : SplitConjuncts(ce.when_clauses_[0].first)) {
      NullClause cl;
      NullClause::Atom single{};
      if (parse_atom(cj, &single)) {
        cl.is_or = false;
        cl.atoms.push_back(single);
        specs[i].conj_clauses.push_back(std::move(cl));
        continue;
      }
      std::vector<Expression> disjs;
      split_disjuncts(cj, &disjs);
      if (disjs.size() < 2) {
        return std::optional<Relation>{};
      }
      cl.is_or = true;
      for (const Expression& dj : disjs) {
        NullClause::Atom da{};
        if (!parse_atom(dj, &da)) {
          return std::optional<Relation>{};
        }
        cl.atoms.push_back(da);
      }
      specs[i].conj_clauses.push_back(std::move(cl));
    }
    if (specs[i].conj_clauses.empty()) {
      return std::optional<Relation>{};
    }
  }
  if (!any_agg) {
    return std::optional<Relation>{};
  }

  std::vector<int64_t> counts(specs.size(), 0);
  for (const ColumnarChunk& ch : cte_rel.columnar->chunks) {
    if (ch.rows == 0) {
      continue;
    }
    bool chunk_any_null = false;
    for (const ColumnarColumn& cc : ch.columns) {
      if (cc.has_null || cc.type == ValueType::kNull) {
        chunk_any_null = true;
        break;
      }
    }
    for (size_t i = 0; i < specs.size(); ++i) {
      if (specs[i].kind == ItemSpec::Kind::kCountStar) {
        counts[i] += static_cast<int64_t>(ch.rows);
        continue;
      }
      if (specs[i].kind != ItemSpec::Kind::kCountMasked) {
        continue;
      }
      if (!chunk_any_null) {
        bool all_pass = true;
        for (const NullClause& cl : specs[i].conj_clauses) {
          bool cl_pass =
              !cl.is_or;  // NOLINT(readability-simplify-boolean-expr)
          for (const auto& at : cl.atoms) {
            if (cl.is_or) {
              cl_pass = cl_pass || at.want_not_null;
            } else {
              cl_pass = cl_pass && at.want_not_null;
            }
          }
          if (!cl_pass) {
            all_pass = false;
            break;
          }
        }
        if (all_pass) {
          counts[i] += static_cast<int64_t>(ch.rows);
        }
        continue;
      }
      int64_t local_cnt = 0;
      for (size_t r = 0; r < ch.rows; ++r) {
        bool row_pass = true;
        for (const NullClause& cl : specs[i].conj_clauses) {
          bool cl_pass =
              !cl.is_or;  // NOLINT(readability-simplify-boolean-expr)
          for (const auto& at : cl.atoms) {
            const bool is_not_null =
                at.col < ch.columns.size() && !ch.columns[at.col].IsNull(r);
            const bool sat = (is_not_null == at.want_not_null);
            if (cl.is_or) {
              if (sat) {
                cl_pass = true;
                break;
              }
            } else if (!sat) {
              cl_pass = false;
              break;
            }
          }
          if (!cl_pass) {
            row_pass = false;
            break;
          }
        }
        local_cnt += static_cast<int64_t>(row_pass);
      }
      counts[i] += local_cnt;
    }
  }

  Relation out(context.execution_runtime());
  std::vector<Column> out_cols;
  std::vector<Value> out_vals;
  out_cols.reserve(specs.size());
  out_vals.reserve(specs.size());
  for (size_t i = 0; i < specs.size(); ++i) {
    const Value v = specs[i].kind == ItemSpec::Kind::kConst ? specs[i].const_val
                                                            : Value(counts[i]);
    out_cols.emplace_back(ProjectionName(statement.SelectList()[i], i),
                          v.IsNull() ? ValueType::kNull : v.type);
    out_vals.push_back(v);
  }
  out.schema = Schema("", std::move(out_cols));
  out.AddRow(Row(std::move(out_vals)));
  RETURN_IF_FAIL(out.GetStatus());
  return std::optional<Relation>(std::move(out));
}

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (packed_2x32_join_key_bijection_sound,
//   radix_partitioned_union_distinct_sound)
StatusOr<std::optional<Relation>> TryExecuteRadixPacked2x32JoinCount(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& inherited_ctes) {
  if (!statement.WithQueries().empty() || !statement.UnionAll().empty() ||
      statement.GetSetOperationTree() != nullptr ||
      !statement.GroupBy().empty() || statement.IsGroupAll() ||
      statement.Having() || statement.Qualify() || statement.Distinct() ||
      statement.HasDistinctOn() || !statement.OrderBy().empty() ||
      statement.Offset() != 0 ||
      (statement.HasLimit() && statement.Limit() == 0) ||
      statement.AsStruct() || statement.ForUpdate() ||
      HasWindowFunctions(statement) || statement.Sources().size() != 2 ||
      statement.SelectList().size() != 1) {
    return std::optional<Relation>{};
  }
  const Expression& sel0 = statement.SelectList()[0].expression;
  if (!sel0 || sel0->Type() != TypeTag::kAggregateExp ||
      !IsCountStar(sel0->AsAggregateExpression()) ||
      sel0->AsAggregateExpression().Distinct() ||
      sel0->AsAggregateExpression().WhereFilter() ||
      sel0->AsAggregateExpression().Having() !=
          AggregateHavingModifier::kNone) {
    return std::optional<Relation>{};
  }
  const SelectSource& s0 = statement.Sources()[0];
  const SelectSource& s1 = statement.Sources()[1];
  if (s0.query || s1.query || s0.unnest || s1.unnest || s0.is_lateral ||
      s1.is_lateral || s0.sample || s1.sample || !s0.using_columns.empty() ||
      !s1.using_columns.empty() || s0.from_nested_join || s1.from_nested_join ||
      s0.join_condition ||
      (s1.join_type != JoinType::kCross && s1.join_type != JoinType::kInner)) {
    return std::optional<Relation>{};
  }
  const auto c0_it = inherited_ctes.find(s0.table);
  const auto c1_it = inherited_ctes.find(s1.table);
  if (c0_it == inherited_ctes.end() || c1_it == inherited_ctes.end() ||
      c0_it->second == nullptr || c1_it->second == nullptr ||
      !c0_it->second->HasColumnarOnly() || !c1_it->second->HasColumnarOnly() ||
      c0_it->second->columnar == nullptr ||
      c1_it->second->columnar == nullptr || c0_it->second->HasSpill() ||
      c1_it->second->HasSpill()) {
    return std::optional<Relation>{};
  }
  const ColumnarTable& lt = *c0_it->second->columnar;
  const ColumnarTable& rt = *c1_it->second->columnar;
  if (lt.total_rows + rt.total_rows < 16384) {
    return std::optional<Relation>{};
  }
  const Schema lview = QualifySchema(c0_it->second->schema,
                                     s0.alias.empty() ? s0.table : s0.alias);
  const Schema rview = QualifySchema(c1_it->second->schema,
                                     s1.alias.empty() ? s1.table : s1.alias);

  std::vector<Expression> preds = SplitConjuncts(statement.WhereClause());
  if (s1.join_condition) {
    for (Expression& jc : SplitConjuncts(s1.join_condition)) {
      preds.push_back(std::move(jc));
    }
  }
  if (preds.size() != 2) {
    return std::optional<Relation>{};
  }
  std::array<slot_t, 2> l_cols{};
  std::array<slot_t, 2> r_cols{};
  for (size_t k = 0; k < 2; ++k) {
    if (!preds[k] || preds[k]->Type() != TypeTag::kBinaryExp ||
        preds[k]->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
      return std::optional<Relation>{};
    }
    const auto& bin = preds[k]->AsBinaryExpression();
    if (!bin.Left() || !bin.Right() ||
        bin.Left()->Type() != TypeTag::kColumnValue ||
        bin.Right()->Type() != TypeTag::kColumnValue) {
      return std::optional<Relation>{};
    }
    const ColumnName& a = bin.Left()->AsColumnValue().GetColumnName();
    const ColumnName& b = bin.Right()->AsColumnValue().GetColumnName();
    auto la = LocalColumnOffset(lview, a);
    auto rb = LocalColumnOffset(rview, b);
    auto lb = LocalColumnOffset(lview, b);
    auto ra = LocalColumnOffset(rview, a);
    size_t l_off = 0;
    size_t r_off = 0;
    if (la.has_value() && rb.has_value() && !lb.has_value() &&
        !ra.has_value()) {
      l_off = *la;
      r_off = *rb;
    } else if (lb.has_value() && ra.has_value() && !la.has_value() &&
               !rb.has_value()) {
      l_off = *lb;
      r_off = *ra;
    } else {
      return std::optional<Relation>{};
    }
    const Column& lc = lview.GetColumn(l_off);
    const Column& rc = rview.GetColumn(r_off);
    if (lc.IsUnsigned() || rc.IsUnsigned() || lc.Type() != rc.Type() ||
        (lc.Type() != ValueType::kInt64 && lc.Type() != ValueType::kDate)) {
      return std::optional<Relation>{};
    }
    l_cols[k] = static_cast<slot_t>(l_off);
    r_cols[k] = static_cast<slot_t>(r_off);
  }

  constexpr size_t kPartitions = 64;
  const size_t workers = std::max<size_t>(
      1, std::min<size_t>(
             QueryParallelWorkerLimit(std::thread::hardware_concurrency()), 8));

  auto partition_side =
      [&](const ColumnarTable& tbl, const std::array<slot_t, 2>& cols,
          std::vector<std::array<std::vector<int64_t>, kPartitions>>*
              worker_bins,
          bool* ok_out) {
        *ok_out = true;
        worker_bins->resize(workers);
        std::vector<std::pair<uint32_t, std::pair<uint32_t, uint32_t>>> slices;
        constexpr size_t kMorsel = 16384;
        for (uint32_t ci = 0; ci < tbl.chunks.size(); ++ci) {
          const size_t nr = tbl.chunks[ci].rows;
          for (size_t off = 0; off < nr; off += kMorsel) {
            slices.push_back(
                {ci,
                 {static_cast<uint32_t>(off),
                  static_cast<uint32_t>(std::min(nr, off + kMorsel))}});
          }
        }
        if (slices.empty()) {
          return;
        }
        std::atomic<bool> range_ok{true};
        const size_t used_w = std::min(workers, slices.size());
        const size_t base_est = tbl.total_rows / (used_w * kPartitions);
        const size_t per_bucket_est = base_est + (base_est / 8) + 64;
        auto run_w = [&](size_t w) {
          auto& bins = (*worker_bins)[w];
          for (auto& b : bins) {
            b.reserve(per_bucket_est);
          }
          for (size_t si = w; si < slices.size(); si += used_w) {
            if (!range_ok.load(std::memory_order_relaxed)) {
              return;
            }
            const ColumnarChunk& ch = tbl.chunks[slices[si].first];
            if (cols[0] >= ch.columns.size() || cols[1] >= ch.columns.size()) {
              range_ok.store(false, std::memory_order_relaxed);
              return;
            }
            const ColumnarColumn& c0 = ch.columns[cols[0]];
            const ColumnarColumn& c1 = ch.columns[cols[1]];
            const uint32_t beg = slices[si].second.first;
            const uint32_t end = slices[si].second.second;
            if (c0.IsFlat() && c0.is_uniform && c1.IsFlat() && c1.is_uniform) {
              const int64_t* __restrict v0 = c0.ints.data();
              const int64_t* __restrict v1 = c1.ints.data();
              const uint8_t* __restrict n0 =
                  c0.has_null ? c0.nulls.data() : nullptr;
              const uint8_t* __restrict n1 =
                  c1.has_null ? c1.nulls.data() : nullptr;
              for (uint32_t r = beg; r < end; ++r) {
                if ((n0 != nullptr && n0[r] != 0) ||
                    (n1 != nullptr && n1[r] != 0)) {
                  continue;
                }
                const int64_t k0 = v0[r];
                const int64_t k1 = v1[r];
                if (k0 < INT32_MIN || k0 > INT32_MAX || k1 < INT32_MIN ||
                    k1 > INT32_MAX) [[unlikely]] {
                  range_ok.store(false, std::memory_order_relaxed);
                  return;
                }
                const uint64_t packed =
                    (static_cast<uint64_t>(
                         static_cast<uint32_t>(static_cast<int32_t>(k0)))
                     << 32U) |
                    static_cast<uint64_t>(
                        static_cast<uint32_t>(static_cast<int32_t>(k1)));
                const uint64_t h =
                    (packed ^ (packed >> 33U)) * 0xff51afd7ed558ccdULL;
                bins[(h >> 56U) & (kPartitions - 1)].push_back(
                    static_cast<int64_t>(packed));
              }
            } else {
              for (uint32_t r = beg; r < end; ++r) {
                if (c0.IsNull(r) || c1.IsNull(r)) {
                  continue;
                }
                const int64_t k0 = c0.IntAt(r);
                const int64_t k1 = c1.IntAt(r);
                if (k0 < INT32_MIN || k0 > INT32_MAX || k1 < INT32_MIN ||
                    k1 > INT32_MAX) [[unlikely]] {
                  range_ok.store(false, std::memory_order_relaxed);
                  return;
                }
                const uint64_t packed =
                    (static_cast<uint64_t>(
                         static_cast<uint32_t>(static_cast<int32_t>(k0)))
                     << 32U) |
                    static_cast<uint64_t>(
                        static_cast<uint32_t>(static_cast<int32_t>(k1)));
                const uint64_t h =
                    (packed ^ (packed >> 33U)) * 0xff51afd7ed558ccdULL;
                bins[(h >> 56U) & (kPartitions - 1)].push_back(
                    static_cast<int64_t>(packed));
              }
            }
          }
        };
        if (used_w <= 1) {
          run_w(0);
        } else if (RunParallelTasks(used_w, run_w) != Status::kSuccess) {
          *ok_out = false;
          return;
        }
        *ok_out = range_ok.load(std::memory_order_relaxed);
      };

  std::vector<std::array<std::vector<int64_t>, kPartitions>> l_bins;
  std::vector<std::array<std::vector<int64_t>, kPartitions>> r_bins;
  bool lok = false;
  bool rok = false;
  partition_side(lt, l_cols, &l_bins, &lok);
  if (!lok) {
    return std::optional<Relation>{};
  }
  partition_side(rt, r_cols, &r_bins, &rok);
  if (!rok) {
    return std::optional<Relation>{};
  }

  std::vector<int64_t> worker_counts(workers, 0);
  auto join_worker = [&](size_t w) {
    struct GenSlot {
      int64_t key{0};
      uint32_t count{0};
      uint32_t gen{0};
    };
    std::vector<GenSlot> table;
    uint32_t cur_gen = 0;
    int64_t w_acc = 0;
    for (size_t p = w; p < kPartitions; p += workers) {
      size_t l_n = 0;
      size_t r_n = 0;
      for (const auto& wb : l_bins) {
        l_n += wb[p].size();
      }
      for (const auto& wb : r_bins) {
        r_n += wb[p].size();
      }
      if (l_n == 0 || r_n == 0) {
        continue;
      }
      const bool build_r = (r_n <= l_n);
      const size_t build_n = build_r ? r_n : l_n;
      const auto& build_bins = build_r ? r_bins : l_bins;
      const auto& probe_bins = build_r ? l_bins : r_bins;
      size_t cap = 64;
      while (cap < build_n * 2) {
        cap <<= 1U;
      }
      if (table.size() < cap) {
        table.assign(cap, GenSlot{});
        cur_gen = 0;
      }
      if (++cur_gen == 0) {
        std::memset(table.data(), 0, table.size() * sizeof(GenSlot));
        cur_gen = 1;
      }
      const size_t mask = cap - 1;
      GenSlot* __restrict slots = table.data();
      for (const auto& wb : build_bins) {
        const int64_t* __restrict keys = wb[p].data();
        const size_t kn = wb[p].size();
        for (size_t i = 0; i < kn; ++i) {
          const int64_t k = keys[i];
          const auto u = static_cast<uint64_t>(k);
          size_t idx =
              static_cast<size_t>((u ^ (u >> 33U)) * 0xff51afd7ed558ccdULL) &
              mask;
          while (true) {
            GenSlot& s = slots[idx];
            if (s.gen != cur_gen) {
              s.key = k;
              s.count = 1;
              s.gen = cur_gen;
              break;
            }
            if (s.key == k) {
              ++s.count;
              break;
            }
            idx = (idx + 1) & mask;
          }
        }
      }
      for (const auto& wb : probe_bins) {
        const int64_t* __restrict keys = wb[p].data();
        const size_t kn = wb[p].size();
        for (size_t i = 0; i < kn; ++i) {
          const int64_t k = keys[i];
          const auto u = static_cast<uint64_t>(k);
          size_t idx =
              static_cast<size_t>((u ^ (u >> 33U)) * 0xff51afd7ed558ccdULL) &
              mask;
          while (true) {
            const GenSlot& s = slots[idx];
            if (s.gen != cur_gen) {
              break;
            }
            if (s.key == k) {
              w_acc += static_cast<int64_t>(s.count);
              break;
            }
            idx = (idx + 1) & mask;
          }
        }
      }
    }
    worker_counts[w] = w_acc;
  };
  if (RunParallelTasks(workers, join_worker) != Status::kSuccess) {
    return std::optional<Relation>{};
  }
  int64_t total_matches = 0;
  for (int64_t c : worker_counts) {
    total_matches += c;
  }

  Relation out(context.execution_runtime());
  out.schema = Schema("", {Column(ProjectionName(statement.SelectList()[0], 0),
                                  ValueType::kInt64)});
  out.AddRow(Row({Value(total_matches)}));
  RETURN_IF_FAIL(out.GetStatus());
  return std::optional<Relation>(std::move(out));
}

// Lean proof:
//   lean/TinylambOpt/ResearchProofs/StorageExecution/PostingJoinsA.lean
//     (Following.OffsetCount, Following.IntegralSum,
//     Following.LeftMultiplicity,
//      Following.PruneExtent, Following.RepeatedProbe, Annulus.TwoSlices,
//      Annulus.OffsetCount, Annulus.IntegralSum, Annulus.LeftMultiplicity,
//      Annulus.RepeatedProbe)
//   lean/TinylambOpt/ResearchProofs/StorageExecution/BagKernelsA.lean
//     (MultiplicityRuns.JoinFanout, MultiplicityRuns.OuterWeight)
//   lean/TinylambOpt/ResearchProofs/StorageExecution/DerivedZonesA.lean
//     (Distance.BandInverse)
// Evaluates scalar COUNT(*), COUNT(col), SUM(col) over a 2-source INNER or LEFT
// band/distance/annulus join in O((|L| + |R|) log |R|) using a sorted right
// posting column + prefix-sum arrays, without materializing join pairs.
StatusOr<std::optional<Relation>> TryExecutePostingBandJoinScalarAgg(
    TransactionContext& context, const SelectStatement& statement,
    const CteMap& inherited_ctes) {
  if (!statement.WithQueries().empty() || !statement.UnionAll().empty() ||
      statement.GetSetOperationTree() != nullptr || statement.WhereClause() ||
      !statement.GroupBy().empty() || statement.IsGroupAll() ||
      statement.Having() || statement.Qualify() || statement.Distinct() ||
      statement.HasDistinctOn() || !statement.OrderBy().empty() ||
      statement.Offset() != 0 ||
      (statement.HasLimit() && statement.Limit() == 0) ||
      statement.AsStruct() || statement.ForUpdate() ||
      HasWindowFunctions(statement) || statement.Sources().size() != 2 ||
      statement.SelectList().empty()) {
    return std::optional<Relation>{};
  }
  const SelectSource& s0 = statement.Sources()[0];
  const SelectSource& s1 = statement.Sources()[1];
  if (s0.query || s1.query || s0.unnest || s1.unnest || s0.is_lateral ||
      s1.is_lateral || s0.sample || s1.sample || !s0.using_columns.empty() ||
      !s1.using_columns.empty() || s0.from_nested_join || s1.from_nested_join ||
      s0.join_condition || !s1.join_condition ||
      (s1.join_type != JoinType::kInner && s1.join_type != JoinType::kLeft)) {
    return std::optional<Relation>{};
  }
  const bool is_left_join = (s1.join_type == JoinType::kLeft);

  auto load_source = [&](const SelectSource& src, std::vector<Row>* rows_out,
                         Schema* schema_out) -> StatusOr<bool> {
    const std::string alias = src.alias.empty() ? src.table : src.alias;
    const auto cte_it = inherited_ctes.find(src.table);
    if (cte_it != inherited_ctes.end() && cte_it->second != nullptr) {
      const Relation& rel = *cte_it->second;
      if (rel.HasSpill()) {
        return false;
      }
      *schema_out = QualifySchema(rel.schema, alias);
      if (rel.HasColumnarOnly() && rel.columnar != nullptr) {
        const ColumnarTable& tbl = *rel.columnar;
        rows_out->reserve(tbl.total_rows);
        for (const ColumnarChunk& ch : tbl.chunks) {
          for (size_t r = 0; r < ch.rows; ++r) {
            std::vector<Value> vals;
            vals.reserve(ch.columns.size());
            for (const ColumnarColumn& col : ch.columns) {
              vals.push_back(col.ValueAt(r));
            }
            rows_out->emplace_back(std::move(vals));
          }
        }
      } else {
        *rows_out = rel.rows;
      }
      return true;
    }
    auto tbl_or = context.GetTable(src.table);
    if (!tbl_or.HasValue() || tbl_or.Value() == nullptr) {
      return false;
    }
    const std::shared_ptr<Table>& tbl = tbl_or.Value();
    *schema_out = QualifySchema(tbl->GetSchema(), alias);
    Iterator iter = tbl->BeginFullScan(context.txn_);
    while (iter.IsValid()) {
      rows_out->push_back(*iter);
      ++iter;
    }
    RETURN_IF_FAIL(iter.GetStatus());
    return true;
  };

  // Resolve schemas first by checking CTE / catalog before scanning any rows.
  auto peek_schema = [&](const SelectSource& src) -> std::optional<Schema> {
    const std::string alias = src.alias.empty() ? src.table : src.alias;
    const auto cte_it = inherited_ctes.find(src.table);
    if (cte_it != inherited_ctes.end() && cte_it->second != nullptr) {
      if (cte_it->second->HasSpill()) {
        return std::nullopt;
      }
      return QualifySchema(cte_it->second->schema, alias);
    }
    auto tbl_or = context.GetTable(src.table);
    if (!tbl_or.HasValue() || tbl_or.Value() == nullptr) {
      return std::nullopt;
    }
    return QualifySchema(tbl_or.Value()->GetSchema(), alias);
  };
  const auto l_schema_opt = peek_schema(s0);
  const auto r_schema_opt = peek_schema(s1);
  if (!l_schema_opt.has_value() || !r_schema_opt.has_value()) {
    return std::optional<Relation>{};
  }
  const Schema combined_schema = *l_schema_opt + *r_schema_opt;
  const size_t left_cols = l_schema_opt->ColumnCount();

  auto resolve_col = [&](const Expression& e) -> std::optional<size_t> {
    if (!e || e->Type() != TypeTag::kColumnValue) {
      return std::nullopt;
    }
    return LocalColumnOffset(combined_schema,
                             e->AsColumnValue().GetColumnName());
  };

  // Parse band / distance / annulus join condition on a single
  // (left_key, right_key) pair.
  std::optional<size_t> l_key_col;
  std::optional<size_t> r_key_col;
  bool has_lower = false;
  bool lower_inc = true;
  int64_t lower_off = 0;
  bool has_upper = false;
  bool upper_inc = true;
  int64_t upper_off = 0;
  bool has_hole = false;
  bool hole_inc =
      true;  // true: exclude [l - r, l + r]; false: exclude (l - r, l + r)
  int64_t hole_radius = 0;
  bool impossible = false;
  bool any_nonzero_off = false;
  bool uses_abs_diff = false;
  enum class BucketJoinMode : uint8_t {
    kNone = 0,
    kSameBucket,   // DIV(r.k, B) = DIV(l.k, B)  (PostingJoins.SameBucket)
    kScaledFloor,  // DIV(r.k, S) = l.k          (PostingJoins.ScaledFloor)
  };
  BucketJoinMode bucket_mode = BucketJoinMode::kNone;
  int64_t bucket_width = 0;

  auto tighten_lower = [&](int64_t off, bool inc) {
    if (!has_lower || off > lower_off || (off == lower_off && !inc)) {
      has_lower = true;
      lower_off = off;
      lower_inc = inc;
    }
  };
  auto tighten_upper = [&](int64_t off, bool inc) {
    if (!has_upper || off < upper_off || (off == upper_off && !inc)) {
      has_upper = true;
      upper_off = off;
      upper_inc = inc;
    }
  };
  auto widen_hole = [&](int64_t radius, bool inc) {
    if (!has_hole || radius > hole_radius || (radius == hole_radius && inc)) {
      has_hole = true;
      hole_radius = radius;
      hole_inc = inc;
    }
  };

  auto parse_abs_diff = [&](const Expression& e, size_t* lcol_out,
                            size_t* rcol_out) -> bool {
    if (!e || e->Type() != TypeTag::kFunctionCallExp) {
      return false;
    }
    const auto& fn = e->AsFunctionCallExpression();
    if (fn.FuncName() != "abs" || fn.Args().size() != 1 || !fn.Args()[0] ||
        fn.Args()[0]->Type() != TypeTag::kBinaryExp) {
      return false;
    }
    const auto& sub = fn.Args()[0]->AsBinaryExpression();
    if (sub.Op() != BinaryOperation::kSubtract) {
      return false;
    }
    const auto c1 = resolve_col(sub.Left());
    const auto c2 = resolve_col(sub.Right());
    if (!c1.has_value() || !c2.has_value()) {
      return false;
    }
    if (*c1 < left_cols && *c2 >= left_cols) {
      *lcol_out = *c1;
      *rcol_out = *c2 - left_cols;
      return true;
    }
    if (*c2 < left_cols && *c1 >= left_cols) {
      *lcol_out = *c2;
      *rcol_out = *c1 - left_cols;
      return true;
    }
    return false;
  };

  auto parse_shifted_left = [&](const Expression& e, size_t* lcol_out,
                                int64_t* off_out) -> bool {
    if (const auto c = resolve_col(e); c.has_value() && *c < left_cols) {
      *lcol_out = *c;
      *off_out = 0;
      return true;
    }
    if (!e || e->Type() != TypeTag::kBinaryExp) {
      return false;
    }
    const auto& b = e->AsBinaryExpression();
    if (b.Op() != BinaryOperation::kAdd &&
        b.Op() != BinaryOperation::kSubtract) {
      return false;
    }
    const auto lc = resolve_col(b.Left());
    if (!lc.has_value() || *lc >= left_cols || !b.Right() ||
        b.Right()->Type() != TypeTag::kConstantValue) {
      return false;
    }
    const Value& cv = b.Right()->AsConstantValue().GetValue();
    if (cv.IsNull() || cv.type != ValueType::kInt64 || cv.IsUnsigned()) {
      return false;
    }
    if (b.Op() == BinaryOperation::kSubtract &&
        cv.value.int_value == std::numeric_limits<int64_t>::min()) {
      return false;
    }
    *lcol_out = *lc;
    *off_out = (b.Op() == BinaryOperation::kAdd) ? cv.value.int_value
                                                 : -cv.value.int_value;
    return true;
  };

  auto parse_div_col = [&](const Expression& e, size_t* col_out,
                           int64_t* div_out) -> bool {
    if (!e || e->Type() != TypeTag::kFunctionCallExp) {
      return false;
    }
    const auto& fn = e->AsFunctionCallExpression();
    if (fn.FuncName() != "div" || fn.Args().size() != 2 || !fn.Args()[0] ||
        !fn.Args()[1] || fn.Args()[1]->Type() != TypeTag::kConstantValue) {
      return false;
    }
    const auto c = resolve_col(fn.Args()[0]);
    if (!c.has_value()) {
      return false;
    }
    const Value& dv = fn.Args()[1]->AsConstantValue().GetValue();
    if (dv.IsNull() || dv.type != ValueType::kInt64 || dv.IsUnsigned() ||
        dv.value.int_value <= 0) {
      return false;
    }
    *col_out = *c;
    *div_out = dv.value.int_value;
    return true;
  };

  const std::vector<Expression> conjuncts = SplitConjuncts(s1.join_condition);
  if (conjuncts.empty()) {
    return std::optional<Relation>{};
  }
  if (conjuncts.size() == 1 && conjuncts[0] &&
      conjuncts[0]->Type() == TypeTag::kBinaryExp &&
      conjuncts[0]->AsBinaryExpression().Op() == BinaryOperation::kEquals) {
    const auto& eq = conjuncts[0]->AsBinaryExpression();
    size_t c1 = 0;
    size_t c2 = 0;
    int64_t d1 = 0;
    int64_t d2 = 0;
    if (parse_div_col(eq.Left(), &c1, &d1) &&
        parse_div_col(eq.Right(), &c2, &d2) && d1 == d2) {
      if (c1 < left_cols && c2 >= left_cols) {
        l_key_col = c1;
        r_key_col = c2 - left_cols;
        bucket_mode = BucketJoinMode::kSameBucket;
        bucket_width = d1;
        any_nonzero_off = true;
      } else if (c2 < left_cols && c1 >= left_cols) {
        l_key_col = c2;
        r_key_col = c1 - left_cols;
        bucket_mode = BucketJoinMode::kSameBucket;
        bucket_width = d1;
        any_nonzero_off = true;
      }
    } else if (parse_div_col(eq.Left(), &c1, &d1) && c1 >= left_cols) {
      const auto lc = resolve_col(eq.Right());
      if (lc.has_value() && *lc < left_cols) {
        l_key_col = lc;
        r_key_col = c1 - left_cols;
        bucket_mode = BucketJoinMode::kScaledFloor;
        bucket_width = d1;
        any_nonzero_off = true;
      }
    } else if (parse_div_col(eq.Right(), &c2, &d2) && c2 >= left_cols) {
      const auto lc = resolve_col(eq.Left());
      if (lc.has_value() && *lc < left_cols) {
        l_key_col = lc;
        r_key_col = c2 - left_cols;
        bucket_mode = BucketJoinMode::kScaledFloor;
        bucket_width = d2;
        any_nonzero_off = true;
      }
    }
  }
  for (const Expression& conj : conjuncts) {
    if (bucket_mode != BucketJoinMode::kNone) {
      break;
    }
    if (!conj || conj->Type() != TypeTag::kBinaryExp) {
      return std::optional<Relation>{};
    }
    const auto& b = conj->AsBinaryExpression();
    if (b.Op() != BinaryOperation::kLessThan &&
        b.Op() != BinaryOperation::kLessThanEquals &&
        b.Op() != BinaryOperation::kGreaterThan &&
        b.Op() != BinaryOperation::kGreaterThanEquals) {
      return std::optional<Relation>{};
    }
    size_t lc = 0;
    int64_t off = 0;
    const auto l_direct = resolve_col(b.Left());
    const auto r_direct = resolve_col(b.Right());
    if (r_direct.has_value() && *r_direct >= left_cols &&
        parse_shifted_left(b.Left(), &lc, &off)) {
      const size_t rc = *r_direct - left_cols;
      if ((l_key_col.has_value() && *l_key_col != lc) ||
          (r_key_col.has_value() && *r_key_col != rc)) {
        return std::optional<Relation>{};
      }
      l_key_col = lc;
      r_key_col = rc;
      any_nonzero_off = any_nonzero_off || (off != 0);
      if (b.Op() == BinaryOperation::kLessThan) {
        tighten_lower(off, false);
      } else if (b.Op() == BinaryOperation::kLessThanEquals) {
        tighten_lower(off, true);
      } else if (b.Op() == BinaryOperation::kGreaterThan) {
        tighten_upper(off, false);
      } else {
        tighten_upper(off, true);
      }
    } else if (l_direct.has_value() && *l_direct >= left_cols &&
               parse_shifted_left(b.Right(), &lc, &off)) {
      const size_t rc = *l_direct - left_cols;
      if ((l_key_col.has_value() && *l_key_col != lc) ||
          (r_key_col.has_value() && *r_key_col != rc)) {
        return std::optional<Relation>{};
      }
      l_key_col = lc;
      r_key_col = rc;
      any_nonzero_off = any_nonzero_off || (off != 0);
      if (b.Op() == BinaryOperation::kGreaterThan) {
        tighten_lower(off, false);
      } else if (b.Op() == BinaryOperation::kGreaterThanEquals) {
        tighten_lower(off, true);
      } else if (b.Op() == BinaryOperation::kLessThan) {
        tighten_upper(off, false);
      } else {
        tighten_upper(off, true);
      }
    } else {
      // Distance / Annulus predicate: ABS(r.k - l.k) <op> c (or c <op>
      // ABS(...)).
      size_t abs_lc = 0;
      size_t abs_rc = 0;
      BinaryOperation abs_op = b.Op();
      const Expression* const_expr = nullptr;
      if (parse_abs_diff(b.Left(), &abs_lc, &abs_rc) && b.Right() &&
          b.Right()->Type() == TypeTag::kConstantValue) {
        const_expr = &b.Right();
      } else if (parse_abs_diff(b.Right(), &abs_lc, &abs_rc) && b.Left() &&
                 b.Left()->Type() == TypeTag::kConstantValue) {
        const_expr = &b.Left();
        if (abs_op == BinaryOperation::kLessThan) {
          abs_op = BinaryOperation::kGreaterThan;
        } else if (abs_op == BinaryOperation::kLessThanEquals) {
          abs_op = BinaryOperation::kGreaterThanEquals;
        } else if (abs_op == BinaryOperation::kGreaterThan) {
          abs_op = BinaryOperation::kLessThan;
        } else {
          abs_op = BinaryOperation::kLessThanEquals;
        }
      } else {
        return std::optional<Relation>{};
      }
      const Value& cv = (*const_expr)->AsConstantValue().GetValue();
      if (cv.IsNull() || cv.type != ValueType::kInt64 || cv.IsUnsigned() ||
          cv.value.int_value == std::numeric_limits<int64_t>::min()) {
        return std::optional<Relation>{};
      }
      if ((l_key_col.has_value() && *l_key_col != abs_lc) ||
          (r_key_col.has_value() && *r_key_col != abs_rc)) {
        return std::optional<Relation>{};
      }
      l_key_col = abs_lc;
      r_key_col = abs_rc;
      uses_abs_diff = true;
      any_nonzero_off = true;
      const int64_t c = cv.value.int_value;
      if (abs_op == BinaryOperation::kLessThan ||
          abs_op == BinaryOperation::kLessThanEquals) {
        const bool inc = (abs_op == BinaryOperation::kLessThanEquals);
        if (c < 0 || (c == 0 && !inc)) {
          impossible = true;
        } else {
          tighten_lower(-c, inc);
          tighten_upper(c, inc);
        }
      } else {
        const bool strict_outside = (abs_op == BinaryOperation::kGreaterThan);
        if (c > 0 || (c == 0 && strict_outside)) {
          widen_hole(c, strict_outside);
        }
      }
    }
  }
  if (!l_key_col.has_value() || !r_key_col.has_value()) {
    return std::optional<Relation>{};
  }
  const Column& lk_meta = l_schema_opt->GetColumn(*l_key_col);
  const Column& rk_meta = r_schema_opt->GetColumn(*r_key_col);
  if (lk_meta.IsUnsigned() || rk_meta.IsUnsigned() ||
      lk_meta.Type() != rk_meta.Type() ||
      (lk_meta.Type() != ValueType::kInt64 &&
       lk_meta.Type() != ValueType::kDate) ||
      (lk_meta.Type() == ValueType::kDate &&
       (any_nonzero_off || uses_abs_diff))) {
    return std::optional<Relation>{};
  }

  // Validate SELECT list: only COUNT(*), COUNT(col), SUM(col) on L or R.
  struct AggItem {
    enum class Kind : uint8_t {
      kCountStar = 0,
      kCountLeft,
      kCountRight,
      kSumLeftInt,
      kSumLeftDouble,
      kSumRightInt,
      kSumRightDouble,
    };
    Kind kind{Kind::kCountStar};
    size_t col{0};
    std::vector<int64_t> r_cnt_prefix;
    std::vector<__int128> r_int_prefix;
    std::vector<double> r_dbl_prefix;
    int64_t count_acc{0};
    __int128 int_sum_acc{0};
    double dbl_sum_acc{0.0};
    bool has_value{false};
  };
  std::vector<AggItem> items;
  items.reserve(statement.SelectList().size());
  for (const auto& si : statement.SelectList()) {
    if (!si.expression || si.expression->Type() != TypeTag::kAggregateExp) {
      return std::optional<Relation>{};
    }
    const auto& agg = si.expression->AsAggregateExpression();
    if (agg.Distinct() || agg.WhereFilter() ||
        agg.Having() != AggregateHavingModifier::kNone) {
      return std::optional<Relation>{};
    }
    if (IsCountStar(agg)) {
      AggItem item;
      item.kind = AggItem::Kind::kCountStar;
      items.push_back(std::move(item));
      continue;
    }
    if (agg.GetType() != AggregationType::kCount &&
        agg.GetType() != AggregationType::kSum) {
      return std::optional<Relation>{};
    }
    const auto col_off = resolve_col(agg.Child());
    if (!col_off.has_value()) {
      return std::optional<Relation>{};
    }
    const bool is_right = (*col_off >= left_cols);
    const size_t local_col = is_right ? (*col_off - left_cols) : *col_off;
    const Column& cm = is_right ? r_schema_opt->GetColumn(local_col)
                                : l_schema_opt->GetColumn(local_col);
    if (agg.GetType() == AggregationType::kCount) {
      AggItem item;
      item.kind =
          is_right ? AggItem::Kind::kCountRight : AggItem::Kind::kCountLeft;
      item.col = local_col;
      items.push_back(std::move(item));
    } else {
      if (cm.IsUnsigned() ||
          (cm.Type() != ValueType::kInt64 && cm.Type() != ValueType::kDouble)) {
        return std::optional<Relation>{};
      }
      const bool is_dbl = (cm.Type() == ValueType::kDouble);
      AggItem item;
      item.kind = is_right ? (is_dbl ? AggItem::Kind::kSumRightDouble
                                     : AggItem::Kind::kSumRightInt)
                           : (is_dbl ? AggItem::Kind::kSumLeftDouble
                                     : AggItem::Kind::kSumLeftInt);
      item.col = local_col;
      items.push_back(std::move(item));
    }
  }

  std::vector<Row> left_rows;
  std::vector<Row> right_rows;
  Schema l_schema;
  Schema r_schema;
  ASSIGN_OR_RETURN(bool, l_ok, load_source(s0, &left_rows, &l_schema));
  if (!l_ok) {
    return std::optional<Relation>{};
  }
  ASSIGN_OR_RETURN(bool, r_ok, load_source(s1, &right_rows, &r_schema));
  if (!r_ok) {
    return std::optional<Relation>{};
  }

  // Guard int64 overflow for `left + offset` and `r - l` before taking the fast
  // path.
  if (any_nonzero_off || uses_abs_diff) {
    constexpr auto kMinI64 =
        static_cast<__int128>(std::numeric_limits<int64_t>::min());
    constexpr auto kMaxI64 =
        static_cast<__int128>(std::numeric_limits<int64_t>::max());
    int64_t l_min = std::numeric_limits<int64_t>::max();
    int64_t l_max = std::numeric_limits<int64_t>::min();
    bool any_l = false;
    for (const Row& lr : left_rows) {
      const Value& v = lr[*l_key_col];
      if (v.IsNull()) {
        continue;
      }
      const int64_t raw = v.value.int_value;
      l_min = std::min(l_min, raw);
      l_max = std::max(l_max, raw);
      any_l = true;
      const auto lv = static_cast<__int128>(raw);
      if ((has_lower &&
           (lv + lower_off < kMinI64 || lv + lower_off > kMaxI64)) ||
          (has_upper &&
           (lv + upper_off < kMinI64 || lv + upper_off > kMaxI64)) ||
          (has_hole &&
           (lv - hole_radius < kMinI64 || lv + hole_radius > kMaxI64))) {
        return std::optional<Relation>{};
      }
    }
    if (uses_abs_diff && any_l) {
      for (const Row& rr : right_rows) {
        const Value& v = rr[*r_key_col];
        if (v.IsNull()) {
          continue;
        }
        const auto rv = static_cast<__int128>(v.value.int_value);
        const __int128 d1 = rv - static_cast<__int128>(l_min);
        const __int128 d2 = rv - static_cast<__int128>(l_max);
        if (d1 <= kMinI64 || d1 > kMaxI64 || d2 <= kMinI64 || d2 > kMaxI64) {
          return std::optional<Relation>{};
        }
      }
    }
  }

  // Build sorted right posting keys (skipping NULL keys).
  struct PostingEntry {
    int64_t key{0};
    size_t row_idx{0};
  };
  std::vector<PostingEntry> r_sorted;
  r_sorted.reserve(right_rows.size());
  for (size_t i = 0; i < right_rows.size(); ++i) {
    const Value& v = right_rows[i][*r_key_col];
    if (v.IsNull()) {
      continue;
    }
    r_sorted.push_back({.key = v.value.int_value, .row_idx = i});
  }
  std::ranges::sort(r_sorted, [](const PostingEntry& a, const PostingEntry& b) {
    return a.key < b.key;
  });

  // SameBucket / ScaledFloor proofs hold over Nat (nonnegative int64 keys).
  if (bucket_mode != BucketJoinMode::kNone) {
    if (!r_sorted.empty() && r_sorted.front().key < 0) {
      return std::optional<Relation>{};
    }
    for (const Row& lr : left_rows) {
      const Value& lkv = lr[*l_key_col];
      if (!lkv.IsNull() && lkv.value.int_value < 0) {
        return std::optional<Relation>{};
      }
    }
  }

  // Build prefix-sum arrays over sorted right postings
  // (PostingJoins.IntegralSum).
  const size_t n_r = r_sorted.size();
  for (AggItem& item : items) {
    if (item.kind == AggItem::Kind::kCountRight) {
      item.r_cnt_prefix.assign(n_r + 1, 0);
      for (size_t i = 0; i < n_r; ++i) {
        const bool nonnull =
            !right_rows[r_sorted[i].row_idx][item.col].IsNull();
        item.r_cnt_prefix[i + 1] =
            item.r_cnt_prefix[i] + static_cast<int64_t>(nonnull);
      }
    } else if (item.kind == AggItem::Kind::kSumRightInt) {
      item.r_cnt_prefix.assign(n_r + 1, 0);
      item.r_int_prefix.assign(n_r + 1, 0);
      for (size_t i = 0; i < n_r; ++i) {
        const Value& v = right_rows[r_sorted[i].row_idx][item.col];
        item.r_cnt_prefix[i + 1] =
            item.r_cnt_prefix[i] + static_cast<int64_t>(!v.IsNull());
        item.r_int_prefix[i + 1] =
            item.r_int_prefix[i] +
            (v.IsNull() ? __int128{0}
                        : static_cast<__int128>(v.value.int_value));
      }
    } else if (item.kind == AggItem::Kind::kSumRightDouble) {
      item.r_cnt_prefix.assign(n_r + 1, 0);
      item.r_dbl_prefix.assign(n_r + 1, 0.0);
      for (size_t i = 0; i < n_r; ++i) {
        const Value& v = right_rows[r_sorted[i].row_idx][item.col];
        if (!v.IsNull() && !std::isfinite(v.value.double_value)) {
          // Prefix subtraction cannot handle Inf - Inf or NaN; fall back.
          return std::optional<Relation>{};
        }
        item.r_cnt_prefix[i + 1] =
            item.r_cnt_prefix[i] + static_cast<int64_t>(!v.IsNull());
        item.r_dbl_prefix[i + 1] =
            item.r_dbl_prefix[i] + (v.IsNull() ? 0.0 : v.value.double_value);
        if (!std::isfinite(item.r_dbl_prefix[i + 1])) {
          return std::optional<Relation>{};
        }
      }
    }
  }

  // Check if non-null left keys are monotonically non-decreasing; when so and
  // there is no annulus hole, two forward fingers replace binary searches with
  // a linear <= 2*(|L| + |R|) comparison sweep:
  //   lean/TinylambOpt/ResearchProofs/EngineDecisions/Goal_Exec_RangeFingers.lean
  //     (RangeFingers.Goal)
  bool use_range_fingers =
      (!impossible && !has_hole && n_r > 0 && left_rows.size() >= 2);
  if (use_range_fingers) {
    bool seen_left = false;
    int64_t prev_left = 0;
    for (const Row& lr : left_rows) {
      const Value& lkv = lr[*l_key_col];
      if (lkv.IsNull()) {
        continue;
      }
      const int64_t kv = lkv.value.int_value;
      if (seen_left && kv < prev_left) {
        use_range_fingers = false;
        break;
      }
      prev_left = kv;
      seen_left = true;
    }
  }
  size_t finger_lo = 0;
  size_t finger_hi = 0;

  // Probe sorted right postings for each left row in O(log |R|), with a
  // direct-mapped per-key probe cache (PostingJoins.RepeatedProbe) so
  // duplicate left keys resolve in O(1) without repeating binary searches.
  struct ProbeCacheEntry {
    std::array<std::pair<size_t, size_t>, 2> slices{{{0, 0}, {0, 0}}};
    int64_t key{0};
    uint8_t slice_cnt{0};
    uint8_t valid{0};
  };
  constexpr size_t kProbeCacheSize = 1024;
  constexpr size_t kProbeCacheMask = kProbeCacheSize - 1;
  std::vector<ProbeCacheEntry> probe_cache;
  const bool use_probe_cache =
      (!use_range_fingers && left_rows.size() >= 16 && n_r >= 4);
  if (use_probe_cache) {
    probe_cache.resize(kProbeCacheSize);
  }
  for (const Row& lr : left_rows) {
    const Value& lkv = lr[*l_key_col];
    std::array<std::pair<size_t, size_t>, 2> slices{{{0, 0}, {0, 0}}};
    size_t slice_cnt = 0;
    if (!impossible && !lkv.IsNull() && n_r > 0) {
      const int64_t raw_key = lkv.value.int_value;
      const int64_t cache_key = (bucket_mode == BucketJoinMode::kSameBucket)
                                    ? (raw_key / bucket_width)
                                    : raw_key;
      size_t cache_slot = 0;
      bool cache_hit = false;
      if (use_range_fingers) {
        const auto lv = static_cast<__int128>(raw_key);
        size_t begin = 0;
        size_t end = n_r;
        if (bucket_mode != BucketJoinMode::kNone) {
          const auto bw = static_cast<__int128>(bucket_width);
          const __int128 lo = (bucket_mode == BucketJoinMode::kSameBucket)
                                  ? ((lv / bw) * bw)
                                  : (lv * bw);
          const __int128 hi = lo + bw - 1;
          while (finger_lo < n_r &&
                 static_cast<__int128>(r_sorted[finger_lo].key) < lo) {
            ++finger_lo;
          }
          while (finger_hi < n_r &&
                 static_cast<__int128>(r_sorted[finger_hi].key) <= hi) {
            ++finger_hi;
          }
          begin = finger_lo;
          end = finger_hi;
        } else {
          if (has_lower) {
            const __int128 lo = lv + static_cast<__int128>(lower_off);
            while (finger_lo < n_r &&
                   (lower_inc
                        ? (static_cast<__int128>(r_sorted[finger_lo].key) < lo)
                        : (static_cast<__int128>(r_sorted[finger_lo].key) <=
                           lo))) {
              ++finger_lo;
            }
            begin = finger_lo;
          }
          if (has_upper) {
            const __int128 hi = lv + static_cast<__int128>(upper_off);
            while (
                finger_hi < n_r &&
                (upper_inc
                     ? (static_cast<__int128>(r_sorted[finger_hi].key) <= hi)
                     : (static_cast<__int128>(r_sorted[finger_hi].key) < hi))) {
              ++finger_hi;
            }
            end = finger_hi;
          }
        }
        if (begin < end) {
          slices[slice_cnt++] = {begin, end};
        }
        cache_hit = true;
      } else if (use_probe_cache) {
        const auto u = static_cast<uint64_t>(cache_key);
        cache_slot = static_cast<size_t>(
                         ((u ^ (u >> 33U)) * 0xff51afd7ed558ccdULL) >> 54U) &
                     kProbeCacheMask;
        const ProbeCacheEntry& ce = probe_cache[cache_slot];
        if (ce.valid != 0 && ce.key == cache_key) {
          slices = ce.slices;
          slice_cnt = ce.slice_cnt;
          cache_hit = true;
        }
      }
      if (!cache_hit) {
        const auto lv = static_cast<__int128>(raw_key);
        const auto r_min = static_cast<__int128>(r_sorted.front().key);
        const auto r_max = static_cast<__int128>(r_sorted.back().key);
        size_t begin = 0;
        size_t end = n_r;
        bool pruned = false;
        if (bucket_mode != BucketJoinMode::kNone) {
          const auto bw = static_cast<__int128>(bucket_width);
          const __int128 lo = (bucket_mode == BucketJoinMode::kSameBucket)
                                  ? ((lv / bw) * bw)
                                  : (lv * bw);
          const __int128 hi = lo + bw - 1;
          if (r_max < lo || r_min > hi) {
            pruned = true;
          } else {
            if (r_min < lo) {
              const auto it = std::lower_bound(
                  r_sorted.begin(), r_sorted.end(), lo,
                  [](const PostingEntry& e, __int128 bound) {
                    return static_cast<__int128>(e.key) < bound;
                  });
              begin = static_cast<size_t>(it - r_sorted.begin());
            }
            if (r_max > hi) {
              const auto it = std::lower_bound(
                  r_sorted.begin() + static_cast<std::ptrdiff_t>(begin),
                  r_sorted.end(), hi,
                  [](const PostingEntry& e, __int128 bound) {
                    return static_cast<__int128>(e.key) <= bound;
                  });
              end = static_cast<size_t>(it - r_sorted.begin());
            }
          }
        } else {
          if (has_lower) {
            const __int128 lo = lv + static_cast<__int128>(lower_off);
            if (lower_inc ? (r_max < lo) : (r_max <= lo)) {
              pruned = true;
            } else if (lower_inc ? (r_min < lo) : (r_min <= lo)) {
              const auto it = std::lower_bound(
                  r_sorted.begin(), r_sorted.end(), lo,
                  [lower_inc](const PostingEntry& e, __int128 bound) {
                    const auto kv = static_cast<__int128>(e.key);
                    return lower_inc ? (kv < bound) : (kv <= bound);
                  });
              begin = static_cast<size_t>(it - r_sorted.begin());
            }
          }
          if (!pruned && has_upper) {
            const __int128 hi = lv + static_cast<__int128>(upper_off);
            if (upper_inc ? (r_min > hi) : (r_min >= hi)) {
              pruned = true;
            } else if (upper_inc ? (r_max > hi) : (r_max >= hi)) {
              const auto it = std::lower_bound(
                  r_sorted.begin() + static_cast<std::ptrdiff_t>(begin),
                  r_sorted.end(), hi,
                  [upper_inc](const PostingEntry& e, __int128 bound) {
                    const auto kv = static_cast<__int128>(e.key);
                    return upper_inc ? (kv <= bound) : (kv < bound);
                  });
              end = static_cast<size_t>(it - r_sorted.begin());
            }
          }
        }
        if (!pruned && begin < end) {
          if (!has_hole) {
            slices[slice_cnt++] = {begin, end};
          } else {
            // Annulus.TwoSlices: split [begin, end) around excluded hole
            // [lv - hole_radius, lv + hole_radius] (when hole_inc) or
            // (lv - hole_radius, lv + hole_radius) (when !hole_inc).
            const __int128 h_lo = lv - static_cast<__int128>(hole_radius);
            const __int128 h_hi = lv + static_cast<__int128>(hole_radius);
            const auto it_left = std::lower_bound(
                r_sorted.begin() + static_cast<std::ptrdiff_t>(begin),
                r_sorted.begin() + static_cast<std::ptrdiff_t>(end), h_lo,
                [hole_inc](const PostingEntry& e, __int128 bound) {
                  const auto kv = static_cast<__int128>(e.key);
                  return hole_inc ? (kv < bound) : (kv <= bound);
                });
            const auto left_end =
                static_cast<size_t>(it_left - r_sorted.begin());
            if (begin < left_end) {
              slices[slice_cnt++] = {begin, left_end};
            }
            const auto it_right = std::lower_bound(
                r_sorted.begin() + static_cast<std::ptrdiff_t>(left_end),
                r_sorted.begin() + static_cast<std::ptrdiff_t>(end), h_hi,
                [hole_inc](const PostingEntry& e, __int128 bound) {
                  const auto kv = static_cast<__int128>(e.key);
                  return hole_inc ? (kv <= bound) : (kv < bound);
                });
            const auto right_beg =
                static_cast<size_t>(it_right - r_sorted.begin());
            if (right_beg < end) {
              slices[slice_cnt++] = {right_beg, end};
            }
          }
        }
        if (use_probe_cache) {
          ProbeCacheEntry& ce = probe_cache[cache_slot];
          ce.slices = slices;
          ce.key = cache_key;
          ce.slice_cnt = static_cast<uint8_t>(slice_cnt);
          ce.valid = 1;
        }
      }
    }

    int64_t fan = 0;
    for (size_t s = 0; s < slice_cnt; ++s) {
      fan += static_cast<int64_t>(slices[s].second - slices[s].first);
    }
    const int64_t left_mult = is_left_join ? std::max<int64_t>(1, fan) : fan;
    if (left_mult == 0) {
      continue;
    }
    for (AggItem& item : items) {
      switch (item.kind) {
        case AggItem::Kind::kCountStar:
          item.count_acc += left_mult;
          break;
        case AggItem::Kind::kCountLeft:
          if (!lr[item.col].IsNull()) {
            item.count_acc += left_mult;
          }
          break;
        case AggItem::Kind::kSumLeftInt:
          if (!lr[item.col].IsNull()) {
            item.int_sum_acc +=
                static_cast<__int128>(left_mult) *
                static_cast<__int128>(lr[item.col].value.int_value);
            item.has_value = true;
          }
          break;
        case AggItem::Kind::kSumLeftDouble:
          if (!lr[item.col].IsNull()) {
            const double lv_d = lr[item.col].value.double_value;
            if (!std::isfinite(lv_d)) {
              return std::optional<Relation>{};
            }
            item.dbl_sum_acc += static_cast<double>(left_mult) * lv_d;
            if (!std::isfinite(item.dbl_sum_acc)) {
              return std::optional<Relation>{};
            }
            item.has_value = true;
          }
          break;
        case AggItem::Kind::kCountRight:
          for (size_t s = 0; s < slice_cnt; ++s) {
            item.count_acc += item.r_cnt_prefix[slices[s].second] -
                              item.r_cnt_prefix[slices[s].first];
          }
          break;
        case AggItem::Kind::kSumRightInt:
          for (size_t s = 0; s < slice_cnt; ++s) {
            const int64_t rc = item.r_cnt_prefix[slices[s].second] -
                               item.r_cnt_prefix[slices[s].first];
            if (rc > 0) {
              item.int_sum_acc += item.r_int_prefix[slices[s].second] -
                                  item.r_int_prefix[slices[s].first];
              item.has_value = true;
            }
          }
          break;
        case AggItem::Kind::kSumRightDouble:
          for (size_t s = 0; s < slice_cnt; ++s) {
            const int64_t rc = item.r_cnt_prefix[slices[s].second] -
                               item.r_cnt_prefix[slices[s].first];
            if (rc > 0) {
              item.dbl_sum_acc += item.r_dbl_prefix[slices[s].second] -
                                  item.r_dbl_prefix[slices[s].first];
              if (!std::isfinite(item.dbl_sum_acc)) {
                return std::optional<Relation>{};
              }
              item.has_value = true;
            }
          }
          break;
      }
    }
  }

  // Materialize single output row with exact SQL overflow checks.
  constexpr auto kMinI64 =
      static_cast<__int128>(std::numeric_limits<int64_t>::min());
  constexpr auto kMaxI64 =
      static_cast<__int128>(std::numeric_limits<int64_t>::max());
  Relation out(context.execution_runtime());
  std::vector<Column> out_cols;
  std::vector<Value> out_vals;
  out_cols.reserve(items.size());
  out_vals.reserve(items.size());
  for (size_t i = 0; i < items.size(); ++i) {
    const AggItem& item = items[i];
    const std::string name = ProjectionName(statement.SelectList()[i], i);
    switch (item.kind) {
      case AggItem::Kind::kCountStar:
      case AggItem::Kind::kCountLeft:
      case AggItem::Kind::kCountRight:
        out_cols.emplace_back(name, ValueType::kInt64);
        out_vals.emplace_back(item.count_acc);
        break;
      case AggItem::Kind::kSumLeftInt:
      case AggItem::Kind::kSumRightInt:
        out_cols.emplace_back(name, ValueType::kInt64);
        if (!item.has_value) {
          out_vals.emplace_back();
        } else if (item.int_sum_acc < kMinI64 || item.int_sum_acc > kMaxI64) {
          return StatusError(StatusCode::kIsInfinity,
                             "integer overflow in SUM");
        } else {
          out_vals.emplace_back(static_cast<int64_t>(item.int_sum_acc));
        }
        break;
      case AggItem::Kind::kSumLeftDouble:
      case AggItem::Kind::kSumRightDouble:
        out_cols.emplace_back(name, ValueType::kDouble);
        if (!item.has_value) {
          out_vals.emplace_back();
        } else {
          out_vals.emplace_back(item.dbl_sum_acc);
        }
        break;
    }
  }
  out.schema = Schema("", std::move(out_cols));
  out.AddRow(Row(std::move(out_vals)));
  RETURN_IF_FAIL(out.GetStatus());
  return std::optional<Relation>(std::move(out));
}

}  // namespace tinylamb::relational_detail
