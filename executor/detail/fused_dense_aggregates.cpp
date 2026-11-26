/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/fused_dense_aggregates.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <span>
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
#include "expression/expression.hpp"
#include "expression/jit.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {
StatusOr<std::optional<Relation>> DenseFilteredGroups::TryRun() const {
  const size_t nblocks = store.blocks.size();
  const bool c0_dbl =
      left.schema.GetColumn(static_cast<size_t>(lop.col0)).Type() ==
      ValueType::kDouble;
  const bool c1_dbl =
      lop.op != 2 &&
      left.schema.GetColumn(static_cast<size_t>(lop.col1)).Type() ==
          ValueType::kDouble;
  const auto lo_u = static_cast<uint64_t>(dense_int_min);
  const size_t span_sz = dense_int_idxs.size();
  const uint32_t* ridxs = dense_int_idxs.data();
  // Pack dense_int_idxs into a compact 1-bit L1-resident bitset (32x
  // smaller than uint32_t[] — e.g. 12.5 KiB for 100k suppliers in Q11).
  std::vector<uint64_t> hit_bits((span_sz + 63) >> 6, 0ULL);
  for (size_t i = 0; i < span_sz; ++i) {
    if (ridxs[i] != kInvalidRightIdx) {
      hit_bits[i >> 6] |= uint64_t{1} << (i & 63);
    }
  }
  const uint64_t* hbits = hit_bits.data();
  const uint8_t op_code = lop.op;
  const double scale = lop.scale;

  std::vector<std::vector<int64_t>> w_keys(workers);
  std::vector<std::vector<double>> w_sums(workers);
  std::atomic<bool> saw_nonfinite{false};
  std::atomic<bool> saw_unsorted{false};
  std::atomic<bool> saw_decode_error{false};

  auto stream_worker = [&](size_t tid) {
    const size_t b_begin = (nblocks * tid) / workers;
    const size_t b_end = (nblocks * (tid + 1)) / workers;
    if (b_begin >= b_end) {
      return;
    }
    auto& out_k = w_keys[tid];
    auto& out_s = w_sums[tid];
    out_k.reserve(65536);
    out_s.reserve(65536);
    bool has_cur = false;
    int64_t cur_k = 0;
    double cur_sum = 0.0;
    std::vector<slot_t> stream_mat_cols{g_base, j_base, c0_base};
    if (lop.op != 2) {
      stream_mat_cols.push_back(c1_base);
    }
    SortUniqueSlots(&stream_mat_cols);
    LSTableStore::LazyBlockScratch lazy_scratch;
    for (size_t bi = b_begin; bi < b_end; ++bi) {
      const uint32_t rc = store.blocks[bi].row_count;
      if (rc == 0) {
        continue;
      }
      lazy_scratch.transient.clear();
      const LSTablePageBlock* mat =
          store.MaterializeBlockView(bi, &lazy_scratch, stream_mat_cols);
      if (mat == nullptr) {
        // A failed block materialization is corruption/I/O, not a
        // monotonicity violation — surface it instead of silently
        // degrading to the non-fast path.
        saw_decode_error.store(true, std::memory_order_relaxed);
        saw_unsorted.store(true, std::memory_order_relaxed);
        return;
      }
      const LSTablePageBlock& blk = *mat;
      const int64_t* gcells = blk.columns[g_base].int_cells.data();
      const int64_t* jcells = blk.columns[j_base].int_cells.data();
      const int64_t* c0cells = blk.columns[c0_base].int_cells.data();
      const int64_t* c1cells =
          op_code == 2 ? nullptr : blk.columns[c1_base].int_cells.data();
      for (uint32_t r = 0; r < rc; ++r) {
        const uint64_t off = static_cast<uint64_t>(jcells[r]) - lo_u;
        if (off >= span_sz ||
            ((hbits[off >> 6] >> (off & 63)) & uint64_t{1}) == 0) {
          continue;
        }
        const double v0 = c0_dbl ? std::bit_cast<double>(c0cells[r])
                                 : static_cast<double>(c0cells[r]);
        double dv = v0;
        if (op_code != 2) {
          const double v1 = c1_dbl ? std::bit_cast<double>(c1cells[r])
                                   : static_cast<double>(c1cells[r]);
          dv = (op_code == 1) ? (v0 * v1) : (v0 * (scale - v1));
        }
        const int64_t gk = gcells[r];
        if (has_cur) {
          if (gk == cur_k) {
            cur_sum += dv;
          } else if (gk > cur_k) {
            if (!std::isfinite(cur_sum)) {
              saw_nonfinite.store(true, std::memory_order_relaxed);
            }
            out_k.push_back(cur_k);
            out_s.push_back(cur_sum);
            cur_k = gk;
            cur_sum = dv;
          } else {
            saw_unsorted.store(true, std::memory_order_relaxed);
            return;
          }
        } else {
          cur_k = gk;
          cur_sum = dv;
          has_cur = true;
        }
      }
    }
    if (has_cur) {
      if (!std::isfinite(cur_sum)) {
        saw_nonfinite.store(true, std::memory_order_relaxed);
      }
      out_k.push_back(cur_k);
      out_s.push_back(cur_sum);
    }
  };

  if (workers == 1) {
    stream_worker(0);
  } else {
    RETURN_IF_FAIL(RunParallelTasks(workers, stream_worker));
  }

  if (saw_decode_error.load(std::memory_order_relaxed)) {
    return StatusError(StatusCode::kCorrupt,
                       "LSTable block materialization failed");
  }

  if (!saw_nonfinite.load(std::memory_order_relaxed) &&
      !saw_unsorted.load(std::memory_order_relaxed)) {
    // Merge boundary keys between consecutive non-empty worker slices.
    std::vector<size_t> start_off(workers, 0);
    size_t prev_w = workers;
    for (size_t tid = 0; tid < workers; ++tid) {
      if (w_keys[tid].empty()) {
        continue;
      }
      if (prev_w < workers && w_keys[prev_w].back() == w_keys[tid].front()) {
        w_sums[prev_w].back() += w_sums[tid].front();
        start_off[tid] = 1;
      }
      if (start_off[tid] < w_keys[tid].size()) {
        prev_w = tid;
      }
    }
    auto out_table = std::make_shared<ColumnarTable>();
    const ValueType gtype = left.schema.GetColumn(g_pcol).Type();
    size_t total_out_groups = 0;
    if (fusion->post_having_active) {
      // Lean proof: lean/TinylambOpt/TpchSf10Discovery.lean
      //   (grouped_having_global_sum_fubini_sound)
      double threshold = fusion->post_having_constant_or_factor;
      bool valid_threshold = true;
      if (fusion->post_having_is_self_sum_factor) {
        double grand_total = 0.0;
        bool any_group = false;
        for (size_t tid = 0; tid < workers; ++tid) {
          const size_t s0 = start_off[tid];
          for (size_t i = s0; i < w_sums[tid].size(); ++i) {
            any_group = true;
            grand_total += w_sums[tid][i];
          }
        }
        if (!any_group) {
          valid_threshold = false;
        } else {
          threshold = grand_total * fusion->post_having_constant_or_factor;
          if (!std::isfinite(threshold)) {
            valid_threshold = false;
          }
        }
      }
      if (valid_threshold) {
        const BinaryOperation hop = fusion->post_having_op;
        ColumnarColumn col_k;
        col_k.type = gtype;
        col_k.is_uniform = true;
        col_k.has_null = false;
        col_k.encoding = ColumnarColumn::Encoding::kFlat;
        ColumnarColumn col_s;
        col_s.type = ValueType::kDouble;
        col_s.is_uniform = true;
        col_s.has_null = false;
        col_s.encoding = ColumnarColumn::Encoding::kFlat;
        for (size_t tid = 0; tid < workers; ++tid) {
          const size_t s0 = start_off[tid];
          const auto& ks = w_keys[tid];
          const auto& ss = w_sums[tid];
          for (size_t i = s0; i < ks.size(); ++i) {
            const double val = ss[i];
            bool pass = false;
            switch (hop) {
              case BinaryOperation::kEquals:
                pass = (val == threshold);
                break;
              case BinaryOperation::kNotEquals:
                pass = (val != threshold);
                break;
              case BinaryOperation::kLessThan:
                pass = (val < threshold);
                break;
              case BinaryOperation::kLessThanEquals:
                pass = (val <= threshold);
                break;
              case BinaryOperation::kGreaterThan:
                pass = (val > threshold);
                break;
              case BinaryOperation::kGreaterThanEquals:
                pass = (val >= threshold);
                break;
              default:
                break;
            }
            if (pass) {
              col_k.ints.push_back(ks[i]);
              col_s.doubles.push_back(val);
            }
          }
        }
        total_out_groups = col_k.ints.size();
        if (total_out_groups > 0) {
          ColumnarChunk chunk;
          chunk.rows = total_out_groups;
          chunk.columns.push_back(std::move(col_k));
          chunk.columns.push_back(std::move(col_s));
          out_table->AppendChunk(std::move(chunk));
        }
      }
      fusion->post_having_applied = true;
    } else {
      for (size_t tid = 0; tid < workers; ++tid) {
        const size_t s0 = start_off[tid];
        if (s0 >= w_keys[tid].size()) {
          continue;
        }
        const size_t cnt = w_keys[tid].size() - s0;
        total_out_groups += cnt;
        ColumnarColumn col_k;
        col_k.type = gtype;
        col_k.is_uniform = true;
        col_k.has_null = false;
        col_k.encoding = ColumnarColumn::Encoding::kFlat;
        if (s0 == 0) {
          col_k.ints = std::move(w_keys[tid]);
        } else {
          col_k.ints.assign(w_keys[tid].begin() + static_cast<ptrdiff_t>(s0),
                            w_keys[tid].end());
        }
        ColumnarColumn col_s;
        col_s.type = ValueType::kDouble;
        col_s.is_uniform = true;
        col_s.has_null = false;
        col_s.encoding = ColumnarColumn::Encoding::kFlat;
        if (s0 == 0) {
          col_s.doubles = std::move(w_sums[tid]);
        } else {
          col_s.doubles.assign(w_sums[tid].begin() + static_cast<ptrdiff_t>(s0),
                               w_sums[tid].end());
        }
        ColumnarChunk chunk;
        chunk.rows = cnt;
        chunk.columns.push_back(std::move(col_k));
        chunk.columns.push_back(std::move(col_s));
        out_table->AppendChunk(std::move(chunk));
      }
    }
    Relation out_rel(context.execution_runtime());
    std::string n0 =
        !fusion->select_names.empty() && !fusion->select_names[0].empty()
            ? fusion->select_names[0]
            : left.schema.GetColumn(g_pcol).Name().name;
    std::string n1 =
        fusion->select_names.size() > 1 && !fusion->select_names[1].empty()
            ? fusion->select_names[1]
            : "agg_0";
    out_rel.schema = Schema("", {Column(std::move(n0), gtype),
                                 Column(std::move(n1), ValueType::kDouble)});
    out_rel.SetColumnarTable(std::move(out_table), total_out_groups > 0);
    out_rel.peak_intermediate_rows = total_out_groups;
    if (context.execution_runtime() != nullptr) {
      context.execution_runtime()->join_ms += ElapsedMs(join_begin);
      ++context.execution_runtime()->grouped_join_fusions;
    }
    fusion->fired = true;
    return std::optional<Relation>(std::move(out_rel));
  }
  return std::optional<Relation>{};
}

StatusOr<bool> DenseProfitGroups::TryRun() const {
  const size_t r_key_col =
      static_cast<size_t>(key_shapes[rk].direct_column) - left_width;
  const auto l_date_col = static_cast<size_t>(key_shapes[lk].extract_date_col);
  const ProgramShape& psh = child_shapes[0];
  const std::array<int32_t, 4> pcols = {psh.profit_col0, psh.profit_col1,
                                        psh.profit_col2, psh.profit_col3};
  const double pconst = psh.profit_const;
  const size_t r_rows =
      right_col_only ? right_arg.columnar->total_rows : build_rows.size();
  // Extract right group key into compact uint16_t IDs (<= 256 distinct).
  std::vector<uint16_t> right_gid(r_rows, 0);
  std::vector<Value> right_gid_vals;
  std::unordered_map<std::string, uint16_t> right_str_map;
  bool right_ok = (r_rows > 0 && r_rows <= 524288);
  const ValueType rk_type = right_arg.schema.GetColumn(r_key_col).Type();
  if (right_ok && rk_type != ValueType::kVarChar) {
    right_ok = false;
  }
  if (right_ok) {
    auto assign_rstr = [&](size_t idx, std::string_view sv) -> bool {
      auto it = right_str_map.find(std::string(sv));
      if (it == right_str_map.end()) {
        if (right_gid_vals.size() >= 256) {
          return false;
        }
        const auto id = static_cast<uint16_t>(right_gid_vals.size());
        right_str_map.emplace(std::string(sv), id);
        right_gid_vals.emplace_back(std::string(sv));
        right_gid[idx] = id;
      } else {
        right_gid[idx] = it->second;
      }
      return true;
    };
    if (right_col_only) {
      size_t base = 0;
      for (const auto& rch : right_arg.columnar->chunks) {
        const ColumnarColumn& rkc = rch.columns[r_key_col];
        if (!rkc.is_uniform || rkc.has_null ||
            rkc.type != ValueType::kVarChar) {
          right_ok = false;
          break;
        }
        for (size_t r = 0; r < rch.rows; ++r) {
          if (!assign_rstr(base + r, rkc.StringAt(r))) {
            right_ok = false;
            break;
          }
        }
        if (!right_ok) {
          break;
        }
        base += rch.rows;
      }
    } else {
      for (size_t r = 0; r < r_rows; ++r) {
        const Value& v = build_rows[r]->values_[r_key_col];
        if (v.IsNull() || v.type != ValueType::kVarChar ||
            !assign_rstr(r, v.AsStringView())) {
          right_ok = false;
          break;
        }
      }
    }
  }
  // Pre-extract any right-side profit columns into flat double arrays.
  std::array<std::vector<double>, 4> right_pvals;
  if (right_ok) {
    for (size_t pi = 0; pi < 4 && right_ok; ++pi) {
      if (std::cmp_less(pcols[pi], left_width)) {
        continue;
      }
      const size_t rc = static_cast<size_t>(pcols[pi]) - left_width;
      right_pvals[pi].resize(r_rows);
      if (right_col_only) {
        size_t base = 0;
        for (const auto& rch : right_arg.columnar->chunks) {
          const ColumnarColumn& col = rch.columns[rc];
          if (!col.is_uniform || col.has_null ||
              col.encoding != ColumnarColumn::Encoding::kFlat) {
            right_ok = false;
            break;
          }
          if (col.type == ValueType::kDouble) {
            std::memcpy(right_pvals[pi].data() + base, col.doubles.data(),
                        rch.rows * sizeof(double));
          } else if (col.type == ValueType::kInt64 && !col.is_unsigned) {
            for (size_t r = 0; r < rch.rows; ++r) {
              right_pvals[pi][base + r] = static_cast<double>(col.ints[r]);
            }
          } else {
            right_ok = false;
            break;
          }
          base += rch.rows;
        }
      } else {
        for (size_t r = 0; r < r_rows; ++r) {
          const Value& v = build_rows[r]->values_[rc];
          if (v.IsNull()) {
            right_ok = false;
            break;
          }
          if (v.type == ValueType::kDouble) {
            right_pvals[pi][r] = v.value.double_value;
          } else if (v.type == ValueType::kInt64 && !v.IsUnsigned()) {
            right_pvals[pi][r] = static_cast<double>(v.value.int_value);
          } else {
            right_ok = false;
            break;
          }
        }
      }
    }
  }
  if (right_ok && !right_gid_vals.empty()) {
    const size_t n_rg = right_gid_vals.size();
    const size_t slot_count = n_rg * 256U;
    const size_t nworkers = locals.size();
    std::vector<std::vector<double>> w_sums(
        nworkers, std::vector<double>(slot_count, 0.0));
    std::vector<std::vector<uint8_t>> w_hits(
        nworkers, std::vector<uint8_t>(slot_count, 0));
    std::vector<uint64_t> w_cmps(nworkers, 0);
    std::atomic<bool> fast_bail{false};

    const auto& lchunks = left.columnar->chunks;
    std::vector<ColProbeMorsel> fast_morsels;
    fast_morsels.reserve(lchunks.size() * 4);
    for (size_t ci = 0; ci < lchunks.size(); ++ci) {
      const size_t nrows = lchunks[ci].rows;
      for (size_t r0 = 0; r0 < nrows; r0 += 16384) {
        fast_morsels.push_back(ColProbeMorsel{
            .ci = static_cast<uint32_t>(ci),
            .r_begin = static_cast<uint32_t>(r0),
            .r_end = static_cast<uint32_t>(std::min(nrows, r0 + 16384))});
      }
    }
    const size_t n_fm = fast_morsels.size();
    const UniqueIntSlot* const utbl = unique_int_table.data();
    const size_t umask = unique_int_mask;
    const uint16_t* const rgids = right_gid.data();
    const int64_t off0 = left_key_offsets[0];
    const int64_t off1 = left_key_offsets[1];
    const slot_t lk0 = left_columns[0];
    const slot_t lk1 = left_columns[1];
    const std::array<const double*, 4> rp_ptrs = {
        right_pvals[0].empty() ? nullptr : right_pvals[0].data(),
        right_pvals[1].empty() ? nullptr : right_pvals[1].data(),
        right_pvals[2].empty() ? nullptr : right_pvals[2].data(),
        right_pvals[3].empty() ? nullptr : right_pvals[3].data()};

    auto run_fast_profit = [&](size_t w) {
      double* const sums = w_sums[w].data();
      uint8_t* const hits = w_hits[w].data();
      uint64_t cmp = 0;
      for (size_t mi = w; mi < n_fm; mi += nworkers) {
        if (fast_bail.load(std::memory_order_relaxed)) {
          return;
        }
        const ColProbeMorsel mor = fast_morsels[mi];
        const ColumnarChunk& ch = lchunks[mor.ci];
        const ColumnarColumn& c_k0 = ch.columns[lk0];
        const ColumnarColumn& c_k1 = ch.columns[lk1];
        const ColumnarColumn& c_dt = ch.columns[l_date_col];
        if (!c_k0.is_uniform || c_k0.has_null ||
            c_k0.encoding != ColumnarColumn::Encoding::kFlat ||
            !c_k1.is_uniform || c_k1.has_null ||
            c_k1.encoding != ColumnarColumn::Encoding::kFlat ||
            !c_dt.is_uniform || c_dt.has_null ||
            c_dt.encoding != ColumnarColumn::Encoding::kFlat ||
            c_dt.type != ValueType::kDate) {
          fast_bail.store(true, std::memory_order_relaxed);
          return;
        }
        std::array<const double*, 4> ldbl{nullptr, nullptr, nullptr, nullptr};
        std::array<const int64_t*, 4> lint{nullptr, nullptr, nullptr, nullptr};
        for (size_t pi = 0; pi < 4; ++pi) {
          if (std::cmp_greater_equal(pcols[pi], left_width)) {
            continue;
          }
          const ColumnarColumn& pc = ch.columns[static_cast<size_t>(pcols[pi])];
          if (!pc.is_uniform || pc.has_null ||
              pc.encoding != ColumnarColumn::Encoding::kFlat) {
            fast_bail.store(true, std::memory_order_relaxed);
            return;
          }
          if (pc.type == ValueType::kDouble) {
            ldbl[pi] = pc.doubles.data();
          } else if (pc.type == ValueType::kInt64 && !pc.is_unsigned) {
            lint[pi] = pc.ints.data();
          } else {
            fast_bail.store(true, std::memory_order_relaxed);
            return;
          }
        }
        const int64_t* const k0p = c_k0.ints.data();
        const int64_t* const k1p = c_k1.ints.data();
        const int64_t* const dtp = c_dt.ints.data();
        auto read_p = [&](size_t pi, size_t r, uint32_t r_idx) noexcept {
          if (rp_ptrs[pi] != nullptr) {
            return rp_ptrs[pi][r_idx];
          }
          return ldbl[pi] != nullptr ? ldbl[pi][r]
                                     : static_cast<double>(lint[pi][r]);
        };
        for (size_t r = mor.r_begin; r < mor.r_end; ++r) {
          const int64_t k0 = AddJoinKeyBias(k0p[r], off0);
          const int64_t k1 = AddJoinKeyBias(k1p[r], off1);
          if (!FitsFusedInt32(k0) || !FitsFusedInt32(k1)) [[unlikely]] {
            continue;
          }
          const int64_t key = PackFusedInt32Pair(k0, k1);
          size_t slot = HashUniqueIntKey(key) & umask;
          uint32_t r_idx = kInvalidRightIdx;
          while (true) {
            const UniqueIntSlot& entry = utbl[slot];
            if (entry.r_idx == kInvalidRightIdx) {
              break;
            }
            if (entry.key == key) [[likely]] {
              r_idx = entry.r_idx;
              break;
            }
            slot = (slot + 1) & umask;
          }
          if (r_idx == kInvalidRightIdx) [[unlikely]] {
            continue;
          }
          ++cmp;
          const int64_t days = dtp[r];
          if (days < 0 || std::cmp_greater_equal(days, kFastYearLut.size()))
              [[unlikely]] {
            fast_bail.store(true, std::memory_order_relaxed);
            return;
          }
          const int32_t yr = kFastYearLut[static_cast<size_t>(days)];
          if (yr < 1900 || yr >= 2156) [[unlikely]] {
            fast_bail.store(true, std::memory_order_relaxed);
            return;
          }
          const double v0 = read_p(0, r, r_idx);
          const double v1 = read_p(1, r, r_idx);
          const double v2 = read_p(2, r, r_idx);
          const double v3 = read_p(3, r, r_idx);
          const double dv = (v0 * (pconst - v1)) - (v2 * v3);
          const size_t gslot = (static_cast<size_t>(rgids[r_idx]) << 8U) |
                               static_cast<size_t>(yr - 1900);
          sums[gslot] += dv;
          hits[gslot] = 1;
        }
      }
      for (size_t s = 0; s < slot_count; ++s) {
        if (hits[s] != 0 && !std::isfinite(sums[s])) {
          fast_bail.store(true, std::memory_order_relaxed);
          return;
        }
      }
      w_cmps[w] = cmp;
    };

    if (nworkers == 1) {
      run_fast_profit(0);
    } else {
      RETURN_IF_FAIL(RunParallelTasks(nworkers, run_fast_profit));
    }
    if (!fast_bail.load(std::memory_order_relaxed)) {
      std::vector<double> merged_sums(slot_count, 0.0);
      std::vector<uint8_t> merged_hits(slot_count, 0);
      bool all_finite = true;
      for (size_t w = 0; w < nworkers; ++w) {
        for (size_t s = 0; s < slot_count; ++s) {
          if (w_hits[w][s] != 0) {
            merged_sums[s] += w_sums[w][s];
            merged_hits[s] = 1;
          }
        }
      }
      for (size_t s = 0; s < slot_count; ++s) {
        if (merged_hits[s] != 0 && !std::isfinite(merged_sums[s])) {
          all_finite = false;
          break;
        }
      }
      if (all_finite) {
        auto out_table = std::make_shared<ColumnarTable>();
        ColumnarColumn col_rk;
        col_rk.type = ValueType::kVarChar;
        col_rk.is_uniform = true;
        col_rk.has_null = false;
        col_rk.encoding = ColumnarColumn::Encoding::kFlat;
        ChunkStringArena* rk_arena = col_rk.EnsureLocalArena();
        ColumnarColumn col_lk;
        col_lk.type = ValueType::kInt64;
        col_lk.is_uniform = true;
        col_lk.has_null = false;
        col_lk.encoding = ColumnarColumn::Encoding::kFlat;
        ColumnarColumn col_agg;
        col_agg.type = ValueType::kDouble;
        col_agg.is_uniform = true;
        col_agg.has_null = false;
        col_agg.encoding = ColumnarColumn::Encoding::kFlat;
        size_t out_rows = 0;
        for (size_t rg = 0; rg < n_rg; ++rg) {
          const std::string_view sv = right_gid_vals[rg].AsStringView();
          for (size_t y = 0; y < 256; ++y) {
            const size_t s = (rg << 8U) | y;
            if (merged_hits[s] != 0) {
              col_rk.strings.push_back(rk_arena->Store(sv));
              col_lk.ints.push_back(1900 + static_cast<int64_t>(y));
              col_agg.doubles.push_back(merged_sums[s]);
              ++out_rows;
            }
          }
        }
        if (out_rows > 0) {
          ColumnarChunk chunk;
          chunk.rows = out_rows;
          if (rk == 0) {
            chunk.columns.push_back(std::move(col_rk));
            chunk.columns.push_back(std::move(col_lk));
          } else {
            chunk.columns.push_back(std::move(col_lk));
            chunk.columns.push_back(std::move(col_rk));
          }
          chunk.columns.push_back(std::move(col_agg));
          out_table->AppendChunk(std::move(chunk));
        }
        std::string n0 =
            !fusion->select_names.empty() && !fusion->select_names[0].empty()
                ? fusion->select_names[0]
                : "col_0";
        std::string n1 =
            fusion->select_names.size() > 1 && !fusion->select_names[1].empty()
                ? fusion->select_names[1]
                : "col_1";
        std::string n2 =
            fusion->select_names.size() > 2 && !fusion->select_names[2].empty()
                ? fusion->select_names[2]
                : "agg_0";
        const ValueType t0 = rk == 0 ? ValueType::kVarChar : ValueType::kInt64;
        const ValueType t1 = rk == 0 ? ValueType::kInt64 : ValueType::kVarChar;
        result.schema =
            Schema("", {Column(std::move(n0), t0), Column(std::move(n1), t1),
                        Column(std::move(n2), ValueType::kDouble)});
        result.SetColumnarTable(std::move(out_table), out_rows > 0);
        result.hash_joins = left.hash_joins + right.hash_joins + 1;
        result.in_memory_hash_joins =
            left.in_memory_hash_joins + right.in_memory_hash_joins + 1;
        result.nested_loop_joins =
            left.nested_loop_joins + right.nested_loop_joins;
        result.hybrid_hash_joins =
            left.hybrid_hash_joins + right.hybrid_hash_joins;
        result.join_comparisons =
            left.join_comparisons + right.join_comparisons;
        for (uint64_t c : w_cmps) {
          result.join_comparisons += c;
        }
        result.peak_intermediate_rows = out_rows;
        if (context.execution_runtime() != nullptr) {
          context.execution_runtime()->join_ms += ElapsedMs(join_begin);
          ++context.execution_runtime()->grouped_join_fusions;
        }
        fusion->fired = true;
        return true;
      }
    }
  }
  return false;
}

}  // namespace tinylamb::relational_detail
