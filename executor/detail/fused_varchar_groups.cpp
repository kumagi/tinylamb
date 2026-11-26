/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/fused_varchar_groups.hpp"

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
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/parallel_work.hpp"
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/fused_grouped_join.hpp"
#include "executor/detail/fused_grouped_join_internal.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "expression/jit.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {
StatusOr<bool> DenseVarcharGroups::TryAggregate() const {
  bool v2_ok = true;
  std::array<int32_t, 2> key_full_col{-1, -1};
  std::array<bool, 2> key_on_right{false, false};
  for (size_t k = 0; k < 2; ++k) {
    const int32_t dc = key_shapes[k].direct_column;
    if (dc < 0) {
      v2_ok = false;
      break;
    }
    key_full_col[k] = dc;
    if (std::cmp_less(dc, left_width)) {
      const Column& lc = left.schema.GetColumn(static_cast<size_t>(dc));
      if (lc.Type() != ValueType::kVarChar) {
        v2_ok = false;
        break;
      }
      key_on_right[k] = false;
    } else {
      const size_t rc_idx = static_cast<size_t>(dc) - left_width;
      if (rc_idx >= right.schema.ColumnCount() ||
          right.schema.GetColumn(rc_idx).Type() != ValueType::kVarChar) {
        v2_ok = false;
        break;
      }
      key_on_right[k] = true;
    }
  }
  if (v2_ok && key_on_right[0] && key_on_right[1]) {
    v2_ok = false;
  }
  struct V2AggSpec {
    bool is_case{false};
    bool else_null{false};
    bool cond_on_right{false};
    int32_t val_col{-1};
    int32_t cond_col{-1};
    BinaryOperation cond_op{BinaryOperation::kEquals};
    int64_t cond_const{0};
  };
  const size_t v2_aggs = fusion->aggregates.size();
  std::vector<V2AggSpec> v2_specs(v2_aggs);
  if (v2_ok) {
    for (size_t a = 0; a < v2_aggs; ++a) {
      if (raw_aggregates.slot_agg_kinds[a] != RawAggKind::kSumInt64) {
        v2_ok = false;
        break;
      }
      const ProgramShape& sh = child_shapes[a];
      if (sh.direct_column >= 0 &&
          std::cmp_less(sh.direct_column, left_width) &&
          left.schema.GetColumn(static_cast<size_t>(sh.direct_column)).Type() ==
              ValueType::kInt64 &&
          !left.schema.GetColumn(static_cast<size_t>(sh.direct_column))
               .IsUnsigned()) {
        v2_specs[a].is_case = false;
        v2_specs[a].val_col = sh.direct_column;
      } else if (sh.case_cond == ProgramShape::CaseCondKind::kIntComparisons &&
                 sh.case_int_comparisons.size() == 1 &&
                 sh.case_int_comparisons[0].subtract < 0 &&
                 sh.case_int_comparisons[0].left >= 0 &&
                 sh.case_val == ProgramShape::CaseValKind::kDirectInt64Col &&
                 sh.case_val_col0 >= 0 &&
                 std::cmp_less(sh.case_val_col0, left_width)) {
        const auto& cmp = sh.case_int_comparisons[0];
        const bool c_right = std::cmp_greater_equal(cmp.left, left_width);
        const Column& cc =
            c_right ? right.schema.GetColumn(static_cast<size_t>(cmp.left) -
                                             left_width)
                    : left.schema.GetColumn(static_cast<size_t>(cmp.left));
        const Column& vc =
            left.schema.GetColumn(static_cast<size_t>(sh.case_val_col0));
        if ((cc.Type() != ValueType::kInt64 && cc.Type() != ValueType::kDate) ||
            cc.IsUnsigned() || vc.Type() != ValueType::kInt64 ||
            vc.IsUnsigned()) {
          v2_ok = false;
          break;
        }
        v2_specs[a].is_case = true;
        v2_specs[a].else_null = sh.case_else_null;
        v2_specs[a].cond_on_right = c_right;
        v2_specs[a].val_col = sh.case_val_col0;
        v2_specs[a].cond_col =
            c_right ? static_cast<int32_t>(static_cast<size_t>(cmp.left) -
                                           left_width)
                    : cmp.left;
        v2_specs[a].cond_op = cmp.op;
        v2_specs[a].cond_const = cmp.constant;
      } else {
        v2_ok = false;
        break;
      }
    }
  }
  struct V2OutItem {
    uint8_t kind{0};  // 0 = key0, 1 = key1, 2 = agg
    size_t agg_idx{0};
  };
  std::vector<V2OutItem> v2_out_items;
  if (v2_ok) {
    v2_out_items.reserve(fusion->select_items.size());
    for (const Expression& item : fusion->select_items) {
      if (!item) {
        v2_ok = false;
        break;
      }
      if (item->Type() == TypeTag::kColumnValue) {
        const auto off = LocalColumnOffset(
            result.schema, item->AsColumnValue().GetColumnName());
        if (!off.has_value()) {
          v2_ok = false;
          break;
        }
        if (std::cmp_equal(*off, key_full_col[0])) {
          v2_out_items.push_back({0, 0});
        } else if (std::cmp_equal(*off, key_full_col[1])) {
          v2_out_items.push_back({1, 0});
        } else {
          v2_ok = false;
          break;
        }
      } else if (item->Type() == TypeTag::kAggregateExp) {
        const auto it = std::ranges::find(fusion->aggregates,
                                          &item->AsAggregateExpression());
        if (it == fusion->aggregates.end()) {
          v2_ok = false;
          break;
        }
        v2_out_items.push_back({2, static_cast<size_t>(std::distance(
                                       fusion->aggregates.begin(), it))});
      } else {
        v2_ok = false;
        break;
      }
    }
  }
  const size_t jk_col = left_columns[0];
  if (v2_ok) {
    for (const ColumnarChunk& ch : left.columnar->chunks) {
      if (ch.rows == 0) {
        continue;
      }
      auto chk_int = [&](size_t c) {
        if (c >= ch.columns.size()) {
          return false;
        }
        const ColumnarColumn& col = ch.columns[c];
        return col.is_uniform && !col.is_unsigned &&
               (col.type == ValueType::kInt64 || col.type == ValueType::kDate);
      };
      if (!chk_int(jk_col)) {
        v2_ok = false;
        break;
      }
      for (size_t k = 0; k < 2; ++k) {
        if (!key_on_right[k]) {
          const auto kc = static_cast<size_t>(key_full_col[k]);
          if (kc >= ch.columns.size() || !ch.columns[kc].is_uniform ||
              ch.columns[kc].type != ValueType::kVarChar) {
            v2_ok = false;
            break;
          }
        }
      }
      if (!v2_ok) {
        break;
      }
      for (const auto& sp : v2_specs) {
        if (!chk_int(static_cast<size_t>(sp.val_col)) ||
            (sp.is_case && !sp.cond_on_right &&
             !chk_int(static_cast<size_t>(sp.cond_col)))) {
          v2_ok = false;
          break;
        }
      }
      if (!v2_ok) {
        break;
      }
    }
  }
  if (v2_ok) {
    // Per-key dictionary representation: either right-indexed,
    // gathered-source indexed (zero string reads on probe rows!), or direct
    // left-row indexed.
    struct V2KeyDict {
      bool on_right{false};
      bool gathered_src{false};
      size_t card{0};
      uint32_t null_id{std::numeric_limits<uint32_t>::max()};
      std::shared_ptr<std::vector<std::string>> reps;
      // on_right: [right_row_count]
      std::vector<uint32_t> right_subgid;
      // gathered_src: source_table chunk_prefix + flat [src_total_rows]
      const ColumnarTable* src_table{nullptr};
      size_t src_col{0};
      std::vector<size_t> src_chunk_prefix;
      std::vector<uint32_t> src_subgid;
      // direct left fallback: [nchunks][chunk_rows]
      std::vector<std::vector<uint32_t>> left_subgids;
    };
    std::array<V2KeyDict, 2> kdicts;
    Value rv_tmp;
    for (size_t k = 0; k < 2 && v2_ok; ++k) {
      V2KeyDict& kd = kdicts[k];
      kd.on_right = key_on_right[k];
      kd.reps = std::make_shared<std::vector<std::string>>();
      auto get_or_add_null = [&]() -> uint32_t {
        if (kd.null_id == std::numeric_limits<uint32_t>::max()) {
          kd.null_id = static_cast<uint32_t>(kd.reps->size());
          kd.reps->emplace_back();
        }
        return kd.null_id;
      };
      if (kd.on_right) {
        const auto rcol = static_cast<slot_t>(
            static_cast<size_t>(key_full_col[k]) - left_width);
        kd.right_subgid.assign(right_row_count,
                               std::numeric_limits<uint32_t>::max());
        std::unordered_map<std::string, uint32_t> smap;
        smap.reserve(std::min<size_t>(right_row_count, 16384));
        std::string_view last_sv;
        uint32_t last_id = std::numeric_limits<uint32_t>::max();
        for (size_t ri = 0; ri < right_row_count; ++ri) {
          const Value& rv =
              read_right_cell(static_cast<uint32_t>(ri), rcol, &rv_tmp);
          if (rv.IsNull()) {
            if (kd.reps->size() >= 65535 &&
                kd.null_id == std::numeric_limits<uint32_t>::max()) {
              v2_ok = false;
              break;
            }
            kd.right_subgid[ri] = get_or_add_null();
            continue;
          }
          if (rv.type != ValueType::kVarChar || rv.IsCaseInsensitive()) {
            v2_ok = false;
            break;
          }
          const std::string_view sv = rv.value.varchar_value;
          if (last_id != std::numeric_limits<uint32_t>::max() &&
              sv == last_sv) {
            kd.right_subgid[ri] = last_id;
            continue;
          }
          const auto next_id = static_cast<uint32_t>(kd.reps->size());
          const auto [it, ins] = smap.emplace(std::string(sv), next_id);
          if (ins) {
            if (kd.reps->size() >= 65535) {
              v2_ok = false;
              break;
            }
            kd.reps->push_back(it->first);
          }
          last_sv = it->first;
          last_id = it->second;
          kd.right_subgid[ri] = last_id;
        }
        kd.card = kd.reps->size();
      } else {
        const auto lcol = static_cast<size_t>(key_full_col[k]);
        // Check if all non-empty chunks are kGathered from the same small
        // source_table (e.g. warehouse 10 rows, item 6,464 rows).
        const ColumnarTable* common_src = nullptr;
        size_t common_src_col = 0;
        bool all_gathered = true;
        for (const ColumnarChunk& ch : left.columnar->chunks) {
          if (ch.rows == 0) {
            continue;
          }
          const ColumnarColumn& c = ch.columns[lcol];
          if (c.encoding != ColumnarColumn::Encoding::kGathered ||
              c.gather_index == nullptr ||
              c.gather_index->source_table == nullptr ||
              c.gather_index->has_null) {
            all_gathered = false;
            break;
          }
          if (common_src == nullptr) {
            common_src = c.gather_index->source_table.get();
            common_src_col = c.gather_col;
          } else if (common_src != c.gather_index->source_table.get() ||
                     common_src_col != c.gather_col) {
            all_gathered = false;
            break;
          }
        }
        if (all_gathered && common_src != nullptr &&
            common_src->total_rows > 0 && common_src->total_rows <= 65535) {
          kd.gathered_src = true;
          kd.src_table = common_src;
          kd.src_col = common_src_col;
          kd.src_chunk_prefix.resize(common_src->chunks.size() + 1, 0);
          for (size_t sci = 0; sci < common_src->chunks.size(); ++sci) {
            kd.src_chunk_prefix[sci + 1] =
                kd.src_chunk_prefix[sci] + common_src->chunks[sci].rows;
          }
          kd.src_subgid.assign(common_src->total_rows,
                               std::numeric_limits<uint32_t>::max());
          std::unordered_map<std::string, uint32_t> smap;
          smap.reserve(std::min<size_t>(common_src->total_rows, 16384));
          std::string_view last_sv;
          uint32_t last_id = std::numeric_limits<uint32_t>::max();
          for (size_t sci = 0; sci < common_src->chunks.size() && v2_ok;
               ++sci) {
            const ColumnarChunk& sch = common_src->chunks[sci];
            if (sch.rows == 0) {
              continue;
            }
            if (common_src_col >= sch.columns.size() ||
                !sch.columns[common_src_col].is_uniform ||
                sch.columns[common_src_col].type != ValueType::kVarChar) {
              v2_ok = false;
              break;
            }
            const ColumnarColumn& scol = sch.columns[common_src_col];
            const size_t base = kd.src_chunk_prefix[sci];
            for (size_t sr = 0; sr < sch.rows; ++sr) {
              if (scol.IsNull(sr)) {
                if (kd.reps->size() >= 65535 &&
                    kd.null_id == std::numeric_limits<uint32_t>::max()) {
                  v2_ok = false;
                  break;
                }
                kd.src_subgid[base + sr] = get_or_add_null();
                continue;
              }
              const std::string_view sv = scol.StringAt(sr);
              if (last_id != std::numeric_limits<uint32_t>::max() &&
                  sv == last_sv) {
                kd.src_subgid[base + sr] = last_id;
                continue;
              }
              const auto next_id = static_cast<uint32_t>(kd.reps->size());
              const auto [it, ins] = smap.emplace(std::string(sv), next_id);
              if (ins) {
                if (kd.reps->size() >= 65535) {
                  v2_ok = false;
                  break;
                }
                kd.reps->push_back(it->first);
              }
              last_sv = it->first;
              last_id = it->second;
              kd.src_subgid[base + sr] = last_id;
            }
          }
          kd.card = kd.reps->size();
        } else {
          // Direct scan fallback for low-cardinality flat left VARCHAR col.
          kd.left_subgids.resize(left.columnar->chunks.size());
          std::vector<std::string_view> l_views;
          l_views.reserve(128);
          kd.reps->reserve(128);
          std::string_view last_lsv;
          uint32_t last_lid = std::numeric_limits<uint32_t>::max();
          for (size_t ci = 0; ci < left.columnar->chunks.size() && v2_ok;
               ++ci) {
            const ColumnarChunk& ch = left.columnar->chunks[ci];
            if (ch.rows == 0) {
              continue;
            }
            const ColumnarColumn& lkc = ch.columns[lcol];
            auto& dst_ids = kd.left_subgids[ci];
            dst_ids.resize(ch.rows);
            for (size_t r = 0; r < ch.rows; ++r) {
              if (lkc.has_null && lkc.IsNull(r)) {
                if (kd.reps->size() >= 128 &&
                    kd.null_id == std::numeric_limits<uint32_t>::max()) {
                  v2_ok = false;
                  break;
                }
                const uint32_t nid = get_or_add_null();
                l_views.clear();
                for (const std::string& s : *kd.reps) {
                  l_views.emplace_back(s);
                }
                dst_ids[r] = nid;
                continue;
              }
              const std::string_view sv = lkc.StringAt(r);
              if (last_lid != std::numeric_limits<uint32_t>::max() &&
                  sv == last_lsv) [[likely]] {
                dst_ids[r] = last_lid;
                continue;
              }
              uint32_t found = std::numeric_limits<uint32_t>::max();
              for (size_t li = 0; li < l_views.size(); ++li) {
                if (li == kd.null_id) {
                  continue;
                }
                if (l_views[li] == sv) {
                  found = static_cast<uint32_t>(li);
                  break;
                }
              }
              if (found == std::numeric_limits<uint32_t>::max()) {
                if (kd.reps->size() >= 128) {
                  v2_ok = false;
                  break;
                }
                found = static_cast<uint32_t>(kd.reps->size());
                kd.reps->emplace_back(sv);
                l_views.clear();
                for (const std::string& s : *kd.reps) {
                  l_views.emplace_back(s);
                }
              }
              last_lsv = l_views[found];
              last_lid = found;
              dst_ids[r] = found;
            }
          }
          kd.card = kd.reps->size();
        }
      }
    }
    // Precompute right-side CASE condition bitmasks once per right row.
    std::array<std::vector<uint8_t>, 4> right_cond_ok;
    if (v2_ok) {
      auto eval_int_op = [](int64_t cv, BinaryOperation op,
                            int64_t k) noexcept -> bool {
        switch (op) {
          case BinaryOperation::kLessThan:
            return cv < k;
          case BinaryOperation::kLessThanEquals:
            return cv <= k;
          case BinaryOperation::kGreaterThan:
            return cv > k;
          case BinaryOperation::kGreaterThanEquals:
            return cv >= k;
          case BinaryOperation::kEquals:
            return cv == k;
          case BinaryOperation::kNotEquals:
            return cv != k;
          default:
            return false;
        }
      };
      for (size_t a = 0; a < v2_aggs; ++a) {
        const auto& sp = v2_specs[a];
        if (sp.is_case && sp.cond_on_right) {
          right_cond_ok[a].assign(right_row_count, 0);
          for (size_t ri = 0; ri < right_row_count; ++ri) {
            const Value& rv =
                read_right_cell(static_cast<uint32_t>(ri),
                                static_cast<slot_t>(sp.cond_col), &rv_tmp);
            if (!rv.IsNull() &&
                (rv.type == ValueType::kInt64 || rv.type == ValueType::kDate) &&
                !rv.IsUnsigned() &&
                eval_int_op(rv.value.int_value, sp.cond_op, sp.cond_const)) {
              right_cond_ok[a][ri] = 1;
            }
          }
        }
      }
    }
    const size_t card0 = kdicts[0].card;
    const size_t card1 = kdicts[1].card;
    const size_t dom =
        (v2_ok && card0 > 0 && card1 > 0 && card0 <= 524288 / card1)
            ? (card0 * card1)
            : 0;
    const size_t v2_workers = std::max<size_t>(
        1, std::min<size_t>(workers, left.columnar->total_rows / 4096));
    const size_t mem_bytes =
        v2_workers * dom * (1 + v2_aggs * (sizeof(int64_t) + 1));
    std::shared_ptr<QueryMemoryCharge> v2_charge;
    if (v2_ok && dom > 0 && dom <= 524288 && mem_bytes <= (size_t{128} << 20)) {
      v2_charge = TryReserveQueryMemory(mem_bytes);
    }
    if (v2_ok && v2_charge != nullptr) {
      struct V2WorkerState {
        std::vector<uint8_t> seen;
        std::vector<int64_t> sums;
        std::vector<uint8_t> has_val;
      };
      std::vector<V2WorkerState> wstates(v2_workers);
      std::atomic<bool> overflow_flag{false};
      const size_t nchunks = left.columnar->chunks.size();
      std::vector<size_t> chunk_prefix(nchunks + 1, 0);
      for (size_t ci = 0; ci < nchunks; ++ci) {
        chunk_prefix[ci + 1] =
            chunk_prefix[ci] + left.columnar->chunks[ci].rows;
      }
      const size_t total_lrows = chunk_prefix[nchunks];
      const int64_t jk_bias = left_key_offsets[0];
      const int64_t dmin = dense_int_min;
      const size_t dspan = dense_int_idxs.size();
      const uint32_t* __restrict didxs = dense_int_idxs.data();

      RETURN_IF_FAIL(RunParallelTasks(v2_workers, [&](size_t w) {
        V2WorkerState& st = wstates[w];
        st.seen.assign(dom, 0);
        st.sums.assign(dom * v2_aggs, 0);
        st.has_val.assign(dom * v2_aggs, 0);
        uint8_t* __restrict seen_p = st.seen.data();
        int64_t* __restrict sums_p = st.sums.data();
        uint8_t* __restrict hv_p = st.has_val.data();

        const size_t gbegin = (total_lrows * w) / v2_workers;
        const size_t gend = (total_lrows * (w + 1)) / v2_workers;
        for (size_t ci = 0; ci < nchunks; ++ci) {
          const size_t c_lo = chunk_prefix[ci];
          const size_t c_hi = chunk_prefix[ci + 1];
          if (c_hi <= gbegin) continue;
          if (c_lo >= gend) break;
          const size_t rbegin = gbegin > c_lo ? (gbegin - c_lo) : 0;
          const size_t rend = gend < c_hi ? (gend - c_lo) : (c_hi - c_lo);
          const ColumnarChunk& ch = left.columnar->chunks[ci];
          const ColumnarColumn& jk_c = ch.columns[jk_col];
          const bool jk_flat = jk_c.IsFlat();
          const int64_t* __restrict jk_p = jk_flat ? jk_c.ints.data() : nullptr;
          const bool jk_null = jk_c.has_null;

          // Key 0 / Key 1 chunk accessors.
          std::array<const uint32_t*, 2> k_gi_chunks{nullptr, nullptr};
          std::array<const uint32_t*, 2> k_gi_rows{nullptr, nullptr};
          std::array<const uint32_t*, 2> k_direct_ids{nullptr, nullptr};
          for (size_t k = 0; k < 2; ++k) {
            if (kdicts[k].gathered_src) {
              const auto* gi = ch.columns[static_cast<size_t>(key_full_col[k])]
                                   .gather_index.get();
              k_gi_chunks[k] = gi->src_chunks.data();
              k_gi_rows[k] = gi->src_rows.data();
            } else if (!kdicts[k].on_right) {
              k_direct_ids[k] = kdicts[k].left_subgids[ci].data();
            }
          }
          const auto resolve_key = [&](size_t k, size_t r,
                                       uint32_t r_idx) noexcept -> uint32_t {
            if (kdicts[k].on_right) {
              return kdicts[k].right_subgid[r_idx];
            }
            if (kdicts[k].gathered_src) {
              const size_t sidx =
                  kdicts[k].src_chunk_prefix[k_gi_chunks[k][r]] +
                  k_gi_rows[k][r];
              return kdicts[k].src_subgid[sidx];
            }
            return k_direct_ids[k][r];
          };

          std::array<const ColumnarColumn*, 4> val_c{};
          std::array<const int64_t*, 4> val_p{};
          std::array<const ColumnarColumn*, 4> cond_c{};
          std::array<const int64_t*, 4> cond_p{};
          for (size_t a = 0; a < v2_aggs; ++a) {
            val_c[a] = &ch.columns[static_cast<size_t>(v2_specs[a].val_col)];
            val_p[a] = val_c[a]->IsFlat() ? val_c[a]->ints.data() : nullptr;
            if (v2_specs[a].is_case && !v2_specs[a].cond_on_right) {
              cond_c[a] =
                  &ch.columns[static_cast<size_t>(v2_specs[a].cond_col)];
              cond_p[a] =
                  cond_c[a]->IsFlat() ? cond_c[a]->ints.data() : nullptr;
            }
          }
          for (size_t r = rbegin; r < rend; ++r) {
            if (jk_null && jk_c.IsNull(r)) continue;
            const int64_t jv = jk_flat ? jk_p[r] : jk_c.IntAt(r);
            const uint64_t off =
                static_cast<uint64_t>(AddJoinKeyBias(jv, jk_bias)) -
                static_cast<uint64_t>(dmin);
            if (off >= dspan) continue;
            const uint32_t r_idx = didxs[off];
            if (r_idx == kInvalidRightIdx) continue;
            const uint32_t sg0 = resolve_key(0, r, r_idx);
            const uint32_t sg1 = resolve_key(1, r, r_idx);
            const size_t gid = (static_cast<size_t>(sg0) * card1) + sg1;
            seen_p[gid] = 1;
            const size_t base = gid * v2_aggs;
            for (size_t a = 0; a < v2_aggs; ++a) {
              const auto& sp = v2_specs[a];
              if (sp.is_case) {
                bool c_ok = false;
                if (sp.cond_on_right) {
                  c_ok = right_cond_ok[a][r_idx] != 0;
                } else if (!cond_c[a]->has_null || !cond_c[a]->IsNull(r)) {
                  const int64_t cv =
                      cond_p[a] != nullptr ? cond_p[a][r] : cond_c[a]->IntAt(r);
                  switch (sp.cond_op) {
                    case BinaryOperation::kLessThan:
                      c_ok = cv < sp.cond_const;
                      break;
                    case BinaryOperation::kLessThanEquals:
                      c_ok = cv <= sp.cond_const;
                      break;
                    case BinaryOperation::kGreaterThan:
                      c_ok = cv > sp.cond_const;
                      break;
                    case BinaryOperation::kGreaterThanEquals:
                      c_ok = cv >= sp.cond_const;
                      break;
                    case BinaryOperation::kEquals:
                      c_ok = cv == sp.cond_const;
                      break;
                    case BinaryOperation::kNotEquals:
                      c_ok = cv != sp.cond_const;
                      break;
                    default:
                      break;
                  }
                }
                if (!c_ok) {
                  if (!sp.else_null) {
                    hv_p[base + a] = 1;
                  }
                  continue;
                }
              }
              if (val_c[a]->has_null && val_c[a]->IsNull(r)) {
                continue;
              }
              const int64_t vv =
                  val_p[a] != nullptr ? val_p[a][r] : val_c[a]->IntAt(r);
              int64_t next_sum = 0;
              if (__builtin_add_overflow(sums_p[base + a], vv, &next_sum))
                  [[unlikely]] {
                overflow_flag.store(true, std::memory_order_relaxed);
                return;
              }
              sums_p[base + a] = next_sum;
              hv_p[base + a] = 1;
            }
          }
        }
      }));
      if (!overflow_flag.load(std::memory_order_relaxed)) {
        std::vector<Column> v2_cols;
        v2_cols.reserve(v2_out_items.size());
        for (size_t c = 0; c < v2_out_items.size(); ++c) {
          const std::string& nm = c < fusion->select_names.size()
                                      ? fusion->select_names[c]
                                      : std::string();
          v2_cols.emplace_back(nm, v2_out_items[c].kind == 2
                                       ? ValueType::kInt64
                                       : ValueType::kVarChar);
        }
        const Schema out_schema("", v2_cols);
        auto out_table = std::make_shared<ColumnarTable>();
        out_table->AppendKeepalive(kdicts[0].reps);
        out_table->AppendKeepalive(kdicts[1].reps);
        constexpr size_t kV2ChunkRows = 8192;
        auto make_chunk = [&]() {
          ColumnarChunk ch;
          ch.InitFromSchema(out_schema, 0);
          ch.AppendKeepaliveUnique(kdicts[0].reps);
          ch.AppendKeepaliveUnique(kdicts[1].reps);
          for (size_t c = 0; c < v2_out_items.size(); ++c) {
            if (v2_out_items[c].kind == 2) {
              ch.columns[c].ints.reserve(kV2ChunkRows);
            } else {
              ch.columns[c].strings.reserve(kV2ChunkRows);
            }
          }
          return ch;
        };
        ColumnarChunk dst_chunk = make_chunk();
        size_t out_n = 0;
        bool merge_overflow = false;
        for (size_t g = 0; g < dom; ++g) {
          bool any_seen = false;
          for (size_t w = 0; w < v2_workers; ++w) {
            if (wstates[w].seen[g] != 0) {
              any_seen = true;
              break;
            }
          }
          if (!any_seen) {
            continue;
          }
          std::array<int64_t, 4> gsum{};
          std::array<uint8_t, 4> ghv{};
          const size_t base = g * v2_aggs;
          for (size_t w = 0; w < v2_workers; ++w) {
            if (wstates[w].seen[g] == 0) {
              continue;
            }
            for (size_t a = 0; a < v2_aggs; ++a) {
              if (wstates[w].has_val[base + a] != 0) {
                int64_t ns = 0;
                if (__builtin_add_overflow(gsum[a], wstates[w].sums[base + a],
                                           &ns)) [[unlikely]] {
                  merge_overflow = true;
                  break;
                }
                gsum[a] = ns;
                ghv[a] = 1;
              }
            }
            if (merge_overflow) {
              break;
            }
          }
          if (merge_overflow) {
            break;
          }
          if (dst_chunk.rows >= kV2ChunkRows) {
            out_table->AppendChunk(std::move(dst_chunk));
            dst_chunk = make_chunk();
          }
          const size_t i0 = g / card1;
          const size_t i1 = g % card1;
          for (size_t c = 0; c < v2_out_items.size(); ++c) {
            ColumnarColumn& dc = dst_chunk.columns[c];
            if (v2_out_items[c].kind == 0) {
              if (i0 == kdicts[0].null_id) {
                if (!dc.has_null) {
                  dc.has_null = true;
                  dc.nulls.assign(dc.strings.size(), 0);
                }
                dc.strings.emplace_back();
                dc.nulls.push_back(1);
              } else {
                dc.strings.emplace_back((*kdicts[0].reps)[i0]);
                if (dc.has_null) {
                  dc.nulls.push_back(0);
                }
              }
            } else if (v2_out_items[c].kind == 1) {
              if (i1 == kdicts[1].null_id) {
                if (!dc.has_null) {
                  dc.has_null = true;
                  dc.nulls.assign(dc.strings.size(), 0);
                }
                dc.strings.emplace_back();
                dc.nulls.push_back(1);
              } else {
                dc.strings.emplace_back((*kdicts[1].reps)[i1]);
                if (dc.has_null) {
                  dc.nulls.push_back(0);
                }
              }
            } else {
              const size_t a = v2_out_items[c].agg_idx;
              if (ghv[a] != 0) {
                dc.ints.push_back(gsum[a]);
                if (dc.has_null) {
                  dc.nulls.push_back(0);
                }
              } else {
                if (!dc.has_null) {
                  dc.has_null = true;
                  dc.nulls.assign(dc.ints.size(), 0);
                }
                dc.ints.push_back(0);
                dc.nulls.push_back(1);
              }
            }
          }
          ++dst_chunk.rows;
          ++out_n;
        }
        if (!merge_overflow) {
          if (dst_chunk.rows > 0) {
            out_table->AppendChunk(std::move(dst_chunk));
          }
          const bool non_empty = out_table->total_rows > 0;
          result.schema = Schema("", std::move(v2_cols));
          result.SetColumnarTable(std::move(out_table), non_empty);
          result.peak_intermediate_rows =
              std::max(result.peak_intermediate_rows, result.TotalRows());
          if (context.execution_runtime() != nullptr) {
            ExecutionRuntime& runtime = *context.execution_runtime();
            runtime.join_ms += ElapsedMs(join_begin);
            ++runtime.grouped_join_fusions;
          }
          if (debug_fusion) {
            std::cerr << "[fusion] v2_dense_2key_varchar groups=" << out_n
                      << " card0=" << card0 << " card1=" << card1
                      << " workers=" << v2_workers << "\n";
          }
          return true;
        }
      }
    }
  }
  return false;
}
}  // namespace tinylamb::relational_detail
