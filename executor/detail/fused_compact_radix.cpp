/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "executor/detail/fused_compact_radix.hpp"

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
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/parallel_work.hpp"
#include "common/platform/process.hpp"
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/fused_grouped_join_internal.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/jit.hpp"
#include "expression/parameter_slot.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {
StatusOr<bool> CompactRadixGroups::TryEmit() const {
  // Lean proof: lean/TinylambOpt/TpchSf10VectorizedExec.lean
  //   (radix_compact_sum_partition_eq)
  // Fast path for packed-2x32 single-SUM/AVG-double radix aggregation with
  // direct columnar output (e.g. TPC-DS Q65 with 2.31M groups): accumulate
  // into a compact 32-byte struct per group and emit each partition's
  // ColumnarChunk on the worker thread while still hot in L2 cache, avoiding
  // 2.31M FusedGroup (~250B) + FusedRawArena + group_ptrs allocations.
  struct CompactSelectCol {
    uint8_t kind{0};  // 0=slot0, 1=slot1, 2=agg0, 3=const, 4=right, 5=pk
    std::string name;
    ValueType type{ValueType::kNull};
    bool is_unsigned{false};
    bool is_interval{false};
    bool is_uniform{true};
    bool has_null{false};
    bool all_null{false};
    Value constant{};
    size_t col_idx{0};  // right column index for kind=4
    // Metadata & LUTs for kind=5 (kPkCol)
    uint8_t pk_shift{0};
    uint64_t pk_mask{0};
    std::shared_ptr<const void> store_owner;
    std::vector<int64_t> lut_i64;
    std::vector<double> lut_f64;
    std::vector<std::string_view> lut_sv;
    std::vector<uint8_t> lut_null;
    bool lut_any_null{false};
    bool lut_all_null{false};
  };
  size_t fallback_worker_groups_cnt = 0;
  for (const FusedWorkerLocal& local : locals) {
    fallback_worker_groups_cnt += local.groups.size();
  }
  const bool split_compact_ready =
      layout.use_split_key_gid && layout.split_key_unpack_only &&
      !layout.split_has_left_probe && right_col_only &&
      layout.right_row_count > 0 && right_col_owner != nullptr &&
      fallback_worker_groups_cnt == 0 &&
      (!layout.split_uses_right_subgid ||
       (layout.split_right_rep_ptr != nullptr &&
        layout.split_right_rep_count == layout.split_right_subgid_count));
  const bool use_split_compact =
      split_compact_ready && !layout.packed_2x32_unpack_only;
  std::vector<CompactSelectCol> compact_cols;
  bool compact_radix_double =
      !radix_tuple_is_int && agg_count == 1 && !has_stat_raw_agg &&
      raw_aggregates.raw_sum_fusion && !raw_aggregates.raw_count_fusion &&
      fusion->aggregates.size() == 1 &&
      (raw_aggregates.slot_agg_kinds[0] == RawAggKind::kSumDouble ||
       raw_aggregates.slot_agg_kinds[0] == RawAggKind::kAvgDouble) &&
      (left_numeric[0].op == 1 || left_numeric[0].op == 2 ||
       left_numeric[0].op == 6 || left_numeric[0].op == 9 ||
       left_numeric[0].op == 17) &&
      (layout.packed_2x32_unpack_only || use_split_compact) &&
      fusion->columnar_output && fusion->output_sink == nullptr &&
      result.using_columns == nullptr && !fusion->having &&
      !fusion->post_having_active && fusion->topn_limit == 0 &&
      !fusion->rollup_count_distribution && !result.HasSpill() &&
      !fusion->select_items.empty();
  if (compact_radix_double && !use_split_compact) {
    compact_cols.reserve(fusion->select_items.size());
    for (size_t si = 0; si < fusion->select_items.size(); ++si) {
      const Expression& expr = fusion->select_items[si];
      std::string col_name =
          si < fusion->select_names.size() ? fusion->select_names[si] : "";
      if (expr != nullptr && expr->Type() == TypeTag::kAggregateExp &&
          &expr->AsAggregateExpression() == fusion->aggregates[0]) {
        CompactSelectCol ccol;
        ccol.kind = 2;
        ccol.name = std::move(col_name);
        ccol.type = ValueType::kDouble;
        compact_cols.push_back(std::move(ccol));
        continue;
      }
      if (expr != nullptr && expr->Type() == TypeTag::kColumnValue) {
        const auto off = LocalColumnOffset(
            result.schema, expr->AsColumnValue().GetColumnName());
        if (off.has_value() && (*off == layout.packed_2x32_slot0 ||
                                *off == layout.packed_2x32_slot1)) {
          const uint8_t k = (*off == layout.packed_2x32_slot0) ? 0 : 1;
          const ValueType kt =
              result.schema.GetColumn(static_cast<size_t>(*off)).Type();
          if (kt == ValueType::kInt64 || kt == ValueType::kDate) {
            CompactSelectCol ccol;
            ccol.kind = k;
            ccol.name = std::move(col_name);
            ccol.type = kt;
            compact_cols.push_back(std::move(ccol));
            continue;
          }
        }
      }
      compact_radix_double = false;
      break;
    }
  } else if (compact_radix_double && use_split_compact) {
    struct RightSummary {
      bool all_uniform{true};
      bool any_null{false};
      bool all_null{true};
      ValueType vt{ValueType::kNull};
      bool is_unsigned{false};
      bool is_interval{false};
    };
    const size_t rw = right.schema.ColumnCount();
    std::vector<RightSummary> rsums(rw);
    std::vector<bool> r_init(rw, false);
    for (const ColumnarChunk& rch : right_arg.columnar->chunks) {
      if (rch.rows == 0) {
        continue;
      }
      for (size_t rc = 0; rc < rw && rc < rch.columns.size(); ++rc) {
        const ColumnarColumn& col = rch.columns[rc];
        RightSummary& s = rsums[rc];
        if (col.has_null || col.type == ValueType::kNull ||
            (col.encoding == ColumnarColumn::Encoding::kConstant &&
             col.const_is_null)) {
          s.any_null = true;
        }
        if (!col.is_uniform) {
          s.all_uniform = false;
          s.all_null = false;
          continue;
        }
        if (col.type != ValueType::kNull) {
          s.all_null = false;
          if (!r_init[rc]) {
            s.vt = col.type;
            s.is_unsigned = col.is_unsigned;
            s.is_interval = col.is_interval;
            r_init[rc] = true;
          } else if (s.vt != col.type || s.is_unsigned != col.is_unsigned ||
                     s.is_interval != col.is_interval) {
            s.all_uniform = false;
          }
        }
      }
    }
    compact_cols.reserve(fusion->select_items.size());
    for (size_t si = 0; si < fusion->select_items.size(); ++si) {
      const Expression& expr = fusion->select_items[si];
      std::string col_name =
          si < fusion->select_names.size() ? fusion->select_names[si] : "";
      if (expr == nullptr) {
        compact_radix_double = false;
        break;
      }
      if (expr->Type() == TypeTag::kAggregateExp &&
          &expr->AsAggregateExpression() == fusion->aggregates[0]) {
        CompactSelectCol ccol;
        ccol.kind = 2;
        ccol.name = std::move(col_name);
        ccol.type = ValueType::kDouble;
        compact_cols.push_back(std::move(ccol));
        continue;
      }
      if (expr->Type() == TypeTag::kConstantValue &&
          !ContainsParameterSlot(expr)) {
        const Value& cv = expr->AsConstantValue().GetValue();
        if (cv.IsCaseInsensitive() || cv.type == ValueType::kArray ||
            (!cv.IsNull() && !cv.IsUnsigned() && cv.Collation() != 0)) {
          compact_radix_double = false;
          break;
        }
        CompactSelectCol ccol;
        ccol.kind = 3;
        ccol.name = std::move(col_name);
        ccol.type = cv.IsNull() ? ValueType::kNull : cv.type;
        ccol.is_unsigned = cv.IsUnsigned();
        ccol.is_interval = cv.IsInterval();
        ccol.has_null = cv.IsNull();
        ccol.all_null = cv.IsNull();
        ccol.constant = cv;
        compact_cols.push_back(std::move(ccol));
        continue;
      }
      if (expr->Type() == TypeTag::kColumnValue) {
        const auto off = LocalColumnOffset(
            result.schema, expr->AsColumnValue().GetColumnName());
        if (!off.has_value()) {
          compact_radix_double = false;
          break;
        }
        if (*off >= left_width && (*off - left_width) < rw) {
          const size_t rc = *off - left_width;
          const RightSummary& rs = rsums[rc];
          if (!rs.all_uniform) {
            compact_radix_double = false;
            break;
          }
          const Column& rcol_def = right.schema.GetColumn(rc);
          const ValueType out_vt =
              rs.all_null
                  ? ValueType::kNull
                  : (rs.vt != ValueType::kNull ? rs.vt : rcol_def.Type());
          CompactSelectCol ccol;
          ccol.kind = 4;
          ccol.name = std::move(col_name);
          ccol.type = out_vt;
          ccol.is_unsigned = rs.is_unsigned;
          ccol.is_interval = rs.is_interval;
          ccol.is_uniform = rs.all_uniform;
          ccol.has_null = rs.any_null;
          ccol.all_null = rs.all_null;
          ccol.col_idx = rc;
          compact_cols.push_back(std::move(ccol));
          continue;
        }
        if (*off < left_width && *off < 16 && def_col_to_pk_lookup[*off] >= 0) {
          const auto li = static_cast<size_t>(
              static_cast<unsigned char>(def_col_to_pk_lookup[*off]));
          const PreparedDeferredPkLookup& lkp = prepared_pk_lookups[li];
          const PreparedDeferredPkGroup& pk_grp = def_pk_groups[lkp.group_idx];
          if (pk_grp.use_subgid && pk_grp.subgid_rep_ptr != nullptr &&
              !lkp.blocks.empty() && pk_grp.subgid_count > 0 &&
              (lkp.payload_type != ValueType::kVarChar ||
               pk_grp.store_owner != nullptr) &&
              (lkp.payload_type == ValueType::kInt64 ||
               lkp.payload_type == ValueType::kDate ||
               lkp.payload_type == ValueType::kDouble ||
               lkp.payload_type == ValueType::kVarChar)) {
            const uint32_t card = pk_grp.subgid_count;
            CompactSelectCol ccol;
            ccol.kind = 5;
            ccol.name = std::move(col_name);
            ccol.type = lkp.payload_type;
            ccol.is_unsigned = lkp.payload_unsigned;
            ccol.pk_shift = pk_grp.subgid_shift;
            ccol.pk_mask =
                card <= 1 ? 0ULL
                          : ((uint64_t{1} << std::bit_width(card - 1)) - 1ULL);
            ccol.store_owner = pk_grp.store_owner;
            ccol.lut_null.assign(card, 0);
            bool any_null = false;
            bool all_null = true;
            if (lkp.payload_type == ValueType::kVarChar) {
              ccol.lut_sv.resize(card);
            } else if (lkp.payload_type == ValueType::kDouble) {
              ccol.lut_f64.resize(card, 0.0);
            } else {
              ccol.lut_i64.resize(card, 0);
            }
            const auto* rep_ptrs = pk_grp.subgid_rep_ptr;
            const auto* blk_ptr = lkp.blocks.data();
            for (uint32_t sg = 0; sg < card; ++sg) {
              const auto [nb, rin] = rep_ptrs[sg];
              const PreparedDeferredPkBlock& pb = blk_ptr[nb];
              if (pb.has_null_payload != 0 && pb.pay_blk->IsNull(rin)) {
                ccol.lut_null[sg] = 1;
                any_null = true;
              } else {
                all_null = false;
                if (lkp.payload_type == ValueType::kVarChar) {
                  ccol.lut_sv[sg] = pb.pay_blk->VarcharAt(rin);
                } else if (lkp.payload_type == ValueType::kDouble) {
                  const auto dv = std::bit_cast<double>(pb.pay_cells[rin]);
                  ccol.lut_f64[sg] = (dv == 0.0) ? 0.0 : dv;
                } else {
                  ccol.lut_i64[sg] = pb.pay_cells[rin];
                }
              }
            }
            ccol.lut_any_null = any_null;
            ccol.lut_all_null = all_null;
            ccol.has_null = any_null;
            ccol.all_null = all_null;
            compact_cols.push_back(std::move(ccol));
            continue;
          }
        }
        std::optional<size_t> eq_rc;
        if (*off < left_width && *off < left.schema.ColumnCount() &&
            (!layout.split_uses_right_subgid ||
             layout.split_right_subgid_count == layout.right_row_count)) {
          const Column& lc = left.schema.GetColumn(*off);
          if (lc.Type() != ValueType::kNull &&
              lc.Type() != ValueType::kVarChar) {
            for (const EqualityKey& eq : equality_keys) {
              if (eq.left == *off && eq.left_offset == 0 &&
                  eq.right_offset == 0 && !eq.null_safe && eq.right < rw) {
                const Column& rc_def = right.schema.GetColumn(eq.right);
                if (lc.Type() == rc_def.Type() &&
                    lc.IsUnsigned() == rc_def.IsUnsigned() &&
                    rc_def.Type() != ValueType::kVarChar) {
                  eq_rc = eq.right;
                  break;
                }
              }
            }
          }
        }
        if (eq_rc.has_value()) {
          const size_t rc = *eq_rc;
          const RightSummary& rs = rsums[rc];
          if (rs.all_uniform) {
            const Column& rcol_def = right.schema.GetColumn(rc);
            const ValueType out_vt =
                rs.all_null
                    ? ValueType::kNull
                    : (rs.vt != ValueType::kNull ? rs.vt : rcol_def.Type());
            CompactSelectCol ccol;
            ccol.kind = 4;
            ccol.name = std::move(col_name);
            ccol.type = out_vt;
            ccol.is_unsigned = rs.is_unsigned;
            ccol.is_interval = rs.is_interval;
            ccol.is_uniform = rs.all_uniform;
            ccol.has_null = rs.any_null;
            ccol.all_null = rs.all_null;
            ccol.col_idx = rc;
            compact_cols.push_back(std::move(ccol));
            continue;
          }
        }
      }
      compact_radix_double = false;
      break;
    }
  }
  if (compact_radix_double) {
    // Lean proof: lean/TinylambOpt/TpchSf10VectorizedExec.lean
    //   (jit_radix_key_double_agg_sound)
    // Reusable per-worker generation-stamped SoA open-addressed table driven
    // by JitRadixKeyDoubleAgg with zero-copy right/PK columnar emission in
    // kColumnarChunkRows (2048) slices (TPC-DS Q65/Q67).
    const bool is_avg =
        (raw_aggregates.slot_agg_kinds[0] == RawAggKind::kAvgDouble);
    const bool raise_inf = raw_aggregates.slot_raises_inf[0];
    const std::optional<JitRadixKeyDoubleAgg> jit_radix_agg =
        (::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_JIT_RADIX_AGG") ==
         nullptr)
            ? JitRadixKeyDoubleAgg::Compile(raise_inf)
            : std::nullopt;
    const bool need_right_gi =
        use_split_compact &&
        std::ranges::any_of(compact_cols, [](const CompactSelectCol& cc) {
          return cc.kind == 4 && !cc.all_null;
        });
    const uint64_t split_r_mask =
        (!use_split_compact || !layout.split_uses_right_subgid ||
         layout.split_right_subgid_count <= 1)
            ? 0ULL
            : ((uint64_t{1}
                << std::bit_width(layout.split_right_subgid_count - 1)) -
               1ULL);
    const size_t out_w = compact_cols.size();
    std::vector<std::vector<ColumnarChunk>> part_chunks(kRadixParts);
    std::vector<std::vector<uint8_t>> part_col_nonnull(
        kRadixParts, std::vector<uint8_t>(out_w, 0));
    std::vector<size_t> part_group_counts(kRadixParts, 0);
    std::atomic<size_t> next_part{0};
    std::mutex err_mu;
    Status compact_err{Status::kSuccess};
    const size_t part_workers = std::min<size_t>(workers, kRadixParts);
    RETURN_IF_FAIL(RunParallelTasks(part_workers, [&](size_t) {
      std::vector<uint64_t> ht_keys;
      std::vector<double> ht_sums;
      std::vector<uint64_t> ht_counts;
      std::vector<uint32_t> ht_gens;
      std::vector<uint8_t> ht_inf;
      std::vector<uint32_t> occupied;
      uint32_t cur_gen = 0;
      auto grow_table = [&](size_t new_cap, uint32_t n_occ) {
        std::vector<uint64_t> next_keys(new_cap, 0);
        std::vector<double> next_sums(new_cap, 0.0);
        std::vector<uint64_t> next_counts(new_cap, 0);
        std::vector<uint32_t> next_gens(new_cap, 0);
        std::vector<uint8_t> next_inf(raise_inf ? new_cap : 0, 0);
        const auto new_max_load = static_cast<uint32_t>((new_cap * 7) / 10);
        std::vector<uint32_t> next_occ(new_max_load);
        cur_gen = 1;
        const size_t nmask = new_cap - 1;
        for (uint32_t j = 0; j < n_occ; ++j) {
          const uint32_t old_pos = occupied[j];
          const uint64_t k = ht_keys[old_pos];
          auto pos = static_cast<size_t>(HashPacked2x32Key(k)) & nmask;
          while (next_gens[pos] == cur_gen) {
            pos = (pos + 1) & nmask;
          }
          next_keys[pos] = k;
          next_sums[pos] = ht_sums[old_pos];
          next_counts[pos] = ht_counts[old_pos];
          next_gens[pos] = cur_gen;
          if (raise_inf) {
            next_inf[pos] = ht_inf[old_pos];
          }
          next_occ[j] = static_cast<uint32_t>(pos);
        }
        ht_keys.swap(next_keys);
        ht_sums.swap(next_sums);
        ht_counts.swap(next_counts);
        ht_gens.swap(next_gens);
        if (raise_inf) {
          ht_inf.swap(next_inf);
        }
        occupied.swap(next_occ);
      };
      while (true) {
        const size_t p = next_part.fetch_add(1, std::memory_order_relaxed);
        if (p >= kRadixParts) {
          break;
        }
        size_t part_total = 0;
        for (auto& wb : worker_radix_buckets) {
          part_total += wb[p].keys.size();
        }
        if (part_total == 0) {
          continue;
        }
        size_t need_cap = 128;
        const size_t est_groups =
            std::min<size_t>(std::max<size_t>(part_total / 2, 64), 131072);
        while (need_cap < est_groups * 2) {
          need_cap <<= 1U;
        }
        if (ht_keys.size() < need_cap) {
          ht_keys.assign(need_cap, 0);
          ht_sums.assign(need_cap, 0.0);
          ht_counts.assign(need_cap, 0);
          ht_gens.assign(need_cap, 0);
          ht_inf.assign(raise_inf ? need_cap : 0, 0);
          cur_gen = 1;
        } else {
          ++cur_gen;
          if (cur_gen == 0) {
            std::fill(ht_gens.begin(), ht_gens.end(), 0);
            cur_gen = 1;
          }
        }
        auto max_load = static_cast<uint32_t>((ht_keys.size() * 7) / 10);
        uint32_t n_occ = 0;
        occupied.resize(max_load);
        if (jit_radix_agg.has_value()) {
          for (auto& wb : worker_radix_buckets) {
            const RadixSoABucket& bucket = wb[p];
            const size_t bsz = bucket.keys.size();
            size_t off = 0;
            while (off < bsz) {
              const auto chunk_n = static_cast<uint32_t>(std::min<size_t>(
                  bsz - off, std::numeric_limits<uint32_t>::max()));
              const uint32_t done = jit_radix_agg->Accumulate(
                  chunk_n, bucket.keys.data() + off, bucket.dvals.data() + off,
                  bucket.valids.data() + off, ht_keys.data(), ht_sums.data(),
                  ht_counts.data(), ht_gens.data(),
                  raise_inf ? ht_inf.data() : nullptr, cur_gen,
                  static_cast<uint64_t>(ht_keys.size() - 1), max_load,
                  occupied.data(), &n_occ);
              off += done;
              if (off < bsz) [[unlikely]] {
                grow_table(ht_keys.size() * 2, n_occ);
                max_load = static_cast<uint32_t>((ht_keys.size() * 7) / 10);
              }
            }
          }
        } else {
          size_t mask = ht_keys.size() - 1;
          for (auto& wb : worker_radix_buckets) {
            const RadixSoABucket& bucket = wb[p];
            const size_t bsz = bucket.keys.size();
            for (size_t idx = 0; idx < bsz; ++idx) {
              const uint64_t key = bucket.keys[idx];
              const double dval = bucket.dvals[idx];
              const uint8_t valid = bucket.valids[idx];
              auto pos = static_cast<size_t>(HashPacked2x32Key(key)) & mask;
              while (true) {
                if (ht_gens[pos] != cur_gen) {
                  if (n_occ >= max_load) [[unlikely]] {
                    grow_table(ht_keys.size() * 2, n_occ);
                    mask = ht_keys.size() - 1;
                    max_load = static_cast<uint32_t>((ht_keys.size() * 7) / 10);
                    pos = static_cast<size_t>(HashPacked2x32Key(key)) & mask;
                    continue;
                  }
                  ht_keys[pos] = key;
                  ht_sums[pos] = 0.0;
                  ht_counts[pos] = 0;
                  ht_gens[pos] = cur_gen;
                  if (raise_inf) {
                    ht_inf[pos] = 0;
                  }
                  occupied[n_occ++] = static_cast<uint32_t>(pos);
                  if (valid != 0) [[likely]] {
                    ht_sums[pos] = dval;
                    ht_counts[pos] = 1;
                  }
                  break;
                }
                if (ht_keys[pos] == key) [[likely]] {
                  if (valid != 0) [[likely]] {
                    const double prev = ht_sums[pos];
                    const double next = prev + dval;
                    if (raise_inf && std::isinf(next) && !std::isinf(prev) &&
                        !std::isinf(dval)) [[unlikely]] {
                      ht_inf[pos] = 1;
                    }
                    ht_sums[pos] = next;
                    ht_counts[pos] += 1;
                  }
                  break;
                }
                pos = (pos + 1) & mask;
              }
            }
          }
        }
        occupied.resize(n_occ);
        const size_t n = n_occ;
        if (n == 0) {
          continue;
        }
        if (raise_inf) {
          bool any_inf = false;
          for (uint32_t pos : occupied) {
            if (ht_inf[pos] != 0) {
              any_inf = true;
              break;
            }
          }
          if (any_inf) {
            std::scoped_lock lk(err_mu);
            if (compact_err.ok()) {
              compact_err = StatusError(StatusCode::kIsInfinity,
                                        "SUM overflow: result is infinite");
            }
            return;
          }
        }
        part_group_counts[p] = n;
        auto& out_pchunks = part_chunks[p];
        out_pchunks.reserve((n + kColumnarChunkRows - 1) / kColumnarChunkRows);
        for (size_t off = 0; off < n; off += kColumnarChunkRows) {
          const size_t cnt = std::min<size_t>(kColumnarChunkRows, n - off);
          ColumnarChunk chunk;
          chunk.rows = cnt;
          chunk.columns.resize(out_w);
          std::shared_ptr<ChunkGatherIndex> right_gi;
          if (need_right_gi) {
            right_gi = std::make_shared<ChunkGatherIndex>();
            right_gi->source_table = right_col_owner;
            right_gi->src_chunks.resize(cnt);
            right_gi->src_rows.resize(cnt);
            for (size_t i = 0; i < cnt; ++i) {
              const uint64_t key = ht_keys[occupied[off + i]];
              const uint32_t r_subgid =
                  layout.split_uses_right_subgid
                      ? static_cast<uint32_t>(
                            (key >> layout.split_right_shift) & split_r_mask)
                      : 0U;
              const uint32_t r_idx = layout.split_uses_right_subgid
                                         ? layout.split_right_rep_ptr[r_subgid]
                                         : 0U;
              const auto [rch, rr] = right_locator.Locate(r_idx);
              right_gi->src_chunks[i] = static_cast<uint32_t>(rch);
              right_gi->src_rows[i] = static_cast<uint32_t>(rr);
            }
          }
          for (size_t ci = 0; ci < out_w; ++ci) {
            const CompactSelectCol& cc = compact_cols[ci];
            ColumnarColumn& col = chunk.columns[ci];
            if (cc.kind == 0 || cc.kind == 1) {
              col.Init(cc.type, cc.is_unsigned, cc.is_interval, 0);
              col.ints.resize(cnt);
              int64_t* dst = col.ints.data();
              if (cc.kind == 0) {
                for (size_t i = 0; i < cnt; ++i) {
                  dst[i] = static_cast<int64_t>(
                      static_cast<int32_t>(ht_keys[occupied[off + i]] >> 32));
                }
              } else {
                for (size_t i = 0; i < cnt; ++i) {
                  dst[i] = static_cast<int64_t>(static_cast<int32_t>(
                      ht_keys[occupied[off + i]] & 0xffffffffULL));
                }
              }
              part_col_nonnull[p][ci] = 1;
            } else if (cc.kind == 2) {
              col.Init(ValueType::kDouble, false, false, 0);
              col.doubles.resize(cnt);
              double* dst = col.doubles.data();
              bool any_null = false;
              for (size_t i = 0; i < cnt; ++i) {
                const uint32_t pos = occupied[off + i];
                const uint64_t c = ht_counts[pos];
                if (c == 0) {
                  any_null = true;
                  dst[i] = 0.0;
                } else {
                  part_col_nonnull[p][ci] = 1;
                  dst[i] = is_avg ? (ht_sums[pos] / static_cast<double>(c))
                                  : ht_sums[pos];
                }
              }
              if (any_null) {
                col.has_null = true;
                col.nulls.assign(cnt, 0);
                for (size_t i = 0; i < cnt; ++i) {
                  if (ht_counts[occupied[off + i]] == 0) {
                    col.nulls[i] = 1;
                  }
                }
              }
            } else if (cc.kind == 3) {
              TrySetConstantColumnarColumn(&col, ValueType::kNull, false, false,
                                           cc.constant, cnt, &chunk);
              if (!cc.constant.IsNull()) {
                part_col_nonnull[p][ci] = 1;
              }
            } else if (cc.kind == 4) {
              if (cc.all_null) {
                col.SetConstantNull(ValueType::kNull, cc.is_unsigned,
                                    cc.is_interval, cnt);
              } else {
                col.SetGathered(right_gi, cc.col_idx, cc.type, cc.is_unsigned,
                                cc.is_interval, cc.is_uniform, cc.has_null);
                if (!cc.has_null) {
                  part_col_nonnull[p][ci] = 1;
                } else {
                  for (size_t i = 0; i < cnt; ++i) {
                    if (!col.IsNull(i)) {
                      part_col_nonnull[p][ci] = 1;
                      break;
                    }
                  }
                }
              }
            } else {
              // kind == 5: kPkCol via pre-extracted subgid LUT.
              if (cc.lut_all_null) {
                col.SetConstantNull(ValueType::kNull, cc.is_unsigned, false,
                                    cnt);
              } else {
                col.Init(cc.type, cc.is_unsigned, false, 0);
                const uint8_t sh = cc.pk_shift;
                const uint64_t mk = cc.pk_mask;
                const bool chk_null = cc.lut_any_null;
                bool chunk_any_null = false;
                bool nonnull_seen = false;
                if (cc.type == ValueType::kVarChar) {
                  col.encoding_owner = cc.store_owner;
                  chunk.AppendKeepaliveUnique(cc.store_owner);
                  col.strings.resize(cnt);
                  std::string_view* dst = col.strings.data();
                  const std::string_view* src = cc.lut_sv.data();
                  if (!chk_null) {
                    nonnull_seen = true;
                    for (size_t i = 0; i < cnt; ++i) {
                      const auto sg = static_cast<size_t>(
                          (ht_keys[occupied[off + i]] >> sh) & mk);
                      dst[i] = src[sg];
                    }
                  } else {
                    const uint8_t* nul = cc.lut_null.data();
                    for (size_t i = 0; i < cnt; ++i) {
                      const auto sg = static_cast<size_t>(
                          (ht_keys[occupied[off + i]] >> sh) & mk);
                      if (nul[sg] != 0) {
                        if (!chunk_any_null) {
                          chunk_any_null = true;
                          col.has_null = true;
                          col.nulls.assign(cnt, 0);
                        }
                        col.nulls[i] = 1;
                        dst[i] = {};
                      } else {
                        dst[i] = src[sg];
                        nonnull_seen = true;
                      }
                    }
                  }
                } else if (cc.type == ValueType::kDouble) {
                  col.doubles.resize(cnt);
                  double* dst = col.doubles.data();
                  const double* src = cc.lut_f64.data();
                  if (!chk_null) {
                    nonnull_seen = true;
                    for (size_t i = 0; i < cnt; ++i) {
                      const auto sg = static_cast<size_t>(
                          (ht_keys[occupied[off + i]] >> sh) & mk);
                      dst[i] = src[sg];
                    }
                  } else {
                    const uint8_t* nul = cc.lut_null.data();
                    for (size_t i = 0; i < cnt; ++i) {
                      const auto sg = static_cast<size_t>(
                          (ht_keys[occupied[off + i]] >> sh) & mk);
                      if (nul[sg] != 0) {
                        if (!chunk_any_null) {
                          chunk_any_null = true;
                          col.has_null = true;
                          col.nulls.assign(cnt, 0);
                        }
                        col.nulls[i] = 1;
                        dst[i] = 0.0;
                      } else {
                        dst[i] = src[sg];
                        nonnull_seen = true;
                      }
                    }
                  }
                } else {
                  col.ints.resize(cnt);
                  int64_t* dst = col.ints.data();
                  const int64_t* src = cc.lut_i64.data();
                  if (!chk_null) {
                    nonnull_seen = true;
                    for (size_t i = 0; i < cnt; ++i) {
                      const auto sg = static_cast<size_t>(
                          (ht_keys[occupied[off + i]] >> sh) & mk);
                      dst[i] = src[sg];
                    }
                  } else {
                    const uint8_t* nul = cc.lut_null.data();
                    for (size_t i = 0; i < cnt; ++i) {
                      const auto sg = static_cast<size_t>(
                          (ht_keys[occupied[off + i]] >> sh) & mk);
                      if (nul[sg] != 0) {
                        if (!chunk_any_null) {
                          chunk_any_null = true;
                          col.has_null = true;
                          col.nulls.assign(cnt, 0);
                        }
                        col.nulls[i] = 1;
                        dst[i] = 0;
                      } else {
                        dst[i] = src[sg];
                        nonnull_seen = true;
                      }
                    }
                  }
                }
                if (nonnull_seen) {
                  part_col_nonnull[p][ci] = 1;
                }
              }
            }
          }
          out_pchunks.push_back(std::move(chunk));
        }
      }
    }));
    std::vector<std::array<RadixSoABucket, kRadixParts>>().swap(
        worker_radix_buckets);
    bounded_radix_charge.reset();
    RETURN_IF_FAIL(compact_err);
    std::vector<uint8_t> any_col_nonnull(out_w, 0);
    size_t total_compact_groups = 0;
    for (size_t p = 0; p < kRadixParts; ++p) {
      total_compact_groups += part_group_counts[p];
      for (size_t ci = 0; ci < out_w; ++ci) {
        any_col_nonnull[ci] |= part_col_nonnull[p][ci];
      }
    }
    // Merge any out-of-range / NULL-key fallback groups from worker locals
    // (disjoint from the packed non-NULL int32 radix keys) into one final
    // chunk.
    ColumnarChunk fallback_chunk;
    {
      std::vector<FusedGroup> fb_groups;
      GroupKeyMap fb_map;
      for (FusedWorkerLocal& local : locals) {
        for (FusedGroup& g : local.groups) {
          auto [it, ins] = fb_map.emplace(g.key, fb_groups.size());
          if (ins) {
            fb_groups.push_back(std::move(g));
          } else {
            FusedGroup& dst = fb_groups[it->second];
            const double prev = dst.RawDoubles()[0];
            const double add = g.RawDoubles()[0];
            const double next = prev + add;
            if (g.RawInfFlags()[0] != 0 ||
                (std::isinf(next) && !std::isinf(prev) && !std::isinf(add))) {
              dst.RawInfFlags()[0] = 1;
            }
            dst.RawDoubles()[0] = next;
            dst.RawCounts()[0] += g.RawCounts()[0];
          }
        }
        std::vector<FusedGroup>().swap(local.groups);
        local.offsets = GroupKeyMap();
        local.int_offsets = FlatIntKeyMap<size_t>();
      }
      const size_t nfb = fb_groups.size();
      if (nfb > 0) {
        if (raise_inf) {
          for (const FusedGroup& g : fb_groups) {
            if (g.RawInfFlags()[0] != 0) {
              return StatusError(StatusCode::kIsInfinity,
                                 "SUM overflow: result is infinite");
            }
          }
        }
        fallback_chunk.rows = nfb;
        fallback_chunk.columns.resize(compact_cols.size());
        for (size_t ci = 0; ci < compact_cols.size(); ++ci) {
          ColumnarColumn& col = fallback_chunk.columns[ci];
          col.encoding = ColumnarColumn::Encoding::kFlat;
          col.is_uniform = true;
          col.is_unsigned = compact_cols[ci].is_unsigned;
          col.is_interval = compact_cols[ci].is_interval;
          col.type = compact_cols[ci].type;
          if (compact_cols[ci].kind == 0 || compact_cols[ci].kind == 1) {
            const size_t ki = compact_cols[ci].kind;
            col.ints.resize(nfb, 0);
            bool any_null = false;
            for (size_t i = 0; i < nfb; ++i) {
              const Value& kv = fb_groups[i].key.values_[ki];
              if (kv.IsNull()) {
                any_null = true;
              } else {
                any_col_nonnull[ci] = 1;
                col.ints[i] = kv.value.int_value;
              }
            }
            if (any_null) {
              col.has_null = true;
              col.nulls.resize(nfb, 0);
              for (size_t i = 0; i < nfb; ++i) {
                if (fb_groups[i].key.values_[ki].IsNull()) {
                  col.nulls[i] = 1;
                }
              }
            }
          } else {
            col.doubles.resize(nfb, 0.0);
            bool any_null = false;
            for (size_t i = 0; i < nfb; ++i) {
              const uint64_t cnt = fb_groups[i].RawCounts()[0];
              if (cnt == 0) {
                any_null = true;
              } else {
                any_col_nonnull[ci] = 1;
                col.doubles[i] = is_avg ? (fb_groups[i].RawDoubles()[0] /
                                           static_cast<double>(cnt))
                                        : fb_groups[i].RawDoubles()[0];
              }
            }
            if (any_null) {
              col.has_null = true;
              col.nulls.resize(nfb, 0);
              for (size_t i = 0; i < nfb; ++i) {
                if (fb_groups[i].RawCounts()[0] == 0) {
                  col.nulls[i] = 1;
                }
              }
            }
          }
        }
        total_compact_groups += nfb;
      }
      for (FusedWorkerLocal& local : locals) {
        local.raw_arena.blocks.clear();
      }
    }
    std::vector<Column> compact_out_cols;
    compact_out_cols.reserve(out_w);
    for (size_t ci = 0; ci < out_w; ++ci) {
      const auto& cc = compact_cols[ci];
      if (total_compact_groups == 0 || any_col_nonnull[ci] == 0) {
        compact_out_cols.emplace_back(cc.name, ValueType::kNull);
      } else {
        Column col_def(cc.name, cc.type);
        col_def.SetUnsigned(cc.is_unsigned);
        col_def.SetInterval(cc.is_interval);
        compact_out_cols.push_back(std::move(col_def));
      }
    }
    auto out_col = std::make_shared<ColumnarTable>();
    if (use_split_compact && right_col_owner != nullptr) {
      out_col->AppendKeepalive(right_col_owner);
    }
    for (const CompactSelectCol& cc : compact_cols) {
      if (cc.kind == 5 && cc.store_owner != nullptr) {
        out_col->AppendKeepalive(cc.store_owner);
      }
    }
    for (size_t p = 0; p < kRadixParts; ++p) {
      for (ColumnarChunk& ch : part_chunks[p]) {
        if (ch.rows == 0) {
          continue;
        }
        for (size_t ci = 0; ci < out_w; ++ci) {
          if (any_col_nonnull[ci] == 0 && compact_cols[ci].kind != 3) {
            ch.columns[ci].SetConstantNull(
                ValueType::kNull, compact_cols[ci].is_unsigned,
                compact_cols[ci].is_interval, ch.rows);
          }
        }
        out_col->AppendChunk(std::move(ch));
      }
    }
    if (fallback_chunk.rows > 0) {
      for (size_t ci = 0; ci < out_w; ++ci) {
        if (any_col_nonnull[ci] == 0 && compact_cols[ci].kind != 3) {
          fallback_chunk.columns[ci].SetConstantNull(
              ValueType::kNull, compact_cols[ci].is_unsigned,
              compact_cols[ci].is_interval, fallback_chunk.rows);
        }
      }
      out_col->AppendChunk(std::move(fallback_chunk));
    }
    result.schema = Schema("", compact_out_cols);
    result.SetColumnarTable(std::move(out_col), total_compact_groups > 0);
    result.peak_intermediate_rows =
        std::max(result.peak_intermediate_rows, result.TotalRows());
    if (result.runtime() != nullptr) {
      ++result.runtime()->partitioned_aggregate_merges;
    }
    if (context.execution_runtime() != nullptr) {
      ExecutionRuntime& runtime = *context.execution_runtime();
      runtime.join_ms += ElapsedMs(join_begin);
      ++runtime.grouped_join_fusions;
    }
    if (track_group_phases) {
      std::cerr << "[fusion] compact_radix_double total_ms="
                << ElapsedMs(join_begin) << " groups=" << total_compact_groups
                << "\n";
    }
    return true;
  }
  return false;
}
}  // namespace tinylamb::relational_detail
