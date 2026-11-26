/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/fused_group_probe.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/platform/extended_float.hpp"
#include "common/status_or.hpp"
#include "executor/detail/analytics_factorized_join.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/fused_grouped_join.hpp"
#include "executor/detail/fused_grouped_join_internal.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "expression/expression.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {
std::vector<slot_t> BuildDeferredProbeMatCols(
    const DeferredLSTableProbe& probe) {
  std::vector<slot_t> cols;
  cols.reserve(probe.projection.size() + probe.integer_peeks.size() +
               probe.null_peeks.size());
  cols.insert(cols.end(), probe.projection.begin(), probe.projection.end());
  for (const IntegerPeekCompare& pred : probe.integer_peeks) {
    cols.push_back(pred.column);
  }
  for (const NullPeekCompare& test : probe.null_peeks) {
    cols.push_back(test.column);
  }
  SortUniqueSlots(&cols);
  return cols;
}

Status FusedGroupProbe::Run(size_t w, FusedWorkerLocal& local,
                            Row& scratch) const {
  const auto add_dw_double = [](DirectGidWorker& dw, size_t slot, double dv) {
    const double prev = dw.doubles[slot];
    const double next = prev + dv;
    if (std::isinf(next) && !std::isinf(prev) && !std::isinf(dv)) {
      dw.inf_flags[slot] = 1;
    }
    dw.doubles[slot] = next;
    dw.counts[slot] += 1;
  };
  const auto add_dw_int = [](DirectGidWorker& dw, size_t slot, int64_t iv) {
    int64_t next = 0;
    if (__builtin_add_overflow(dw.ints[slot], iv, &next)) {
      dw.inf_flags[slot] = 1;
    } else {
      dw.ints[slot] = next;
    }
    dw.counts[slot] += 1;
  };
  const auto charge_right_case_op = [&](const LeftNumericAgg& op, size_t r_idx,
                                        bool has_right, auto&& add_double,
                                        auto&& add_int, auto&& add_count,
                                        auto&& as_double, auto&& present) {
    const uint8_t rc = has_right ? op.right_cond[r_idx] : uint8_t{0};
    switch (op.op) {
      case 10: {
        if (rc == 1) {
          double d0 = 0.0;
          if (as_double(op.col0, &d0)) [[likely]] {
            add_double(d0);
          }
        } else if (!op.case_else_null) {
          add_double(0.0);
        }
        break;
      }
      case 11: {
        if (rc == 1) {
          double d0 = 0.0;
          double d1 = 0.0;
          if (as_double(op.col0, &d0) && as_double(op.col1, &d1)) [[likely]] {
            add_double(d0 * (op.scale - d1));
          }
        } else if (!op.case_else_null) {
          add_double(0.0);
        }
        break;
      }
      case 12:
        if (rc == 1) {
          add_int(1);
        } else if (!op.case_else_null) {
          add_count();
        }
        break;
      case 13:
        if (rc == 1) {
          if (op.case_val_kind == 1 ||
              (op.case_val_kind == 2 && present(op.col0)) ||
              (op.case_val_kind == 3 && present(op.col0) && present(op.col1))) {
            add_count();
          }
        } else if (!op.case_else_null) {
          add_count();
        }
        break;
      default:
        break;
    }
  };
  const auto charge_numeric_with = [&](size_t r_idx, bool has_right,
                                       auto&& add_double_fn, auto&& add_int_fn,
                                       auto&& inc_count_fn, auto&& add_stat_fn,
                                       auto&& as_double, auto&& as_int,
                                       auto&& present) {
    for (size_t i = 0; i < direct.agg_count; ++i) {
      const LeftNumericAgg& op = deferred.left_numeric[i];
      switch (op.op) {
        case 0:
          inc_count_fn(i);
          break;
        case 1:
        case 6: {
          double d0 = 0;
          double d1 = 0;
          if (!as_double(op.col0, &d0) || !as_double(op.col1, &d1)) {
            break;
          }
          add_double_fn(i, op.op == 1 ? d0 * d1 : d0 * (op.scale - d1));
          break;
        }
        case 2: {
          double d0 = 0;
          if (!as_double(op.col0, &d0)) {
            break;
          }
          add_double_fn(i, d0);
          break;
        }
        case 3: {
          int64_t iv = 0;
          if (!as_int(op.col0, &iv)) {
            break;
          }
          add_int_fn(i, iv);
          break;
        }
        case 4:
          if (present(op.col0)) {
            inc_count_fn(i);
          }
          break;
        case 7:
          if (present(op.col0) && present(op.col1)) {
            inc_count_fn(i);
          }
          break;
        case 8: {
          platform::ExtendedFloat x = 0;
          if (op.is_int_col) {
            int64_t iv = 0;
            if (!as_int(op.col0, &iv)) {
              break;
            }
            x = static_cast<platform::ExtendedFloat>(iv);
          } else {
            double d0 = 0;
            if (!as_double(op.col0, &d0)) {
              break;
            }
            x = static_cast<platform::ExtendedFloat>(d0);
          }
          add_stat_fn(i, x);
          break;
        }
        case 10:
        case 11:
        case 12:
        case 13:
          charge_right_case_op(
              op, r_idx, has_right, [&](double dv) { add_double_fn(i, dv); },
              [&](int64_t iv) { add_int_fn(i, iv); },
              [&]() { inc_count_fn(i); }, as_double, present);
          break;
        case 17: {
          double d0 = 0.0;
          double d1 = 0.0;
          const double dv = (as_double(op.col0, &d0) && as_double(op.col1, &d1))
                                ? (d0 * d1)
                                : 0.0;
          add_double_fn(i, dv);
          break;
        }
        default:
          break;
      }
    }
  };
  const auto charge_numeric = [&](DirectGidWorker& dw, uint32_t gid,
                                  size_t r_idx, auto&& as_double, auto&& as_int,
                                  auto&& present) {
    dw.matched[gid] = 1;
    if (direct.single_double_sum) {
      const LeftNumericAgg& op = deferred.left_numeric[0];
      if (op.op == 1 || op.op == 6) {
        double d0 = 0.0;
        double d1 = 0.0;
        if (as_double(op.col0, &d0) && as_double(op.col1, &d1)) {
          add_dw_double(dw, gid, op.op == 1 ? d0 * d1 : d0 * (op.scale - d1));
        }
        return;
      }
      if (op.op == 2) {
        double d0 = 0.0;
        if (as_double(op.col0, &d0)) {
          add_dw_double(dw, gid, d0);
        }
        return;
      }
    }
    const size_t base = static_cast<size_t>(gid) * direct.agg_count;
    charge_numeric_with(
        r_idx, true,
        [&](size_t i, double dv) { add_dw_double(dw, base + i, dv); },
        [&](size_t i, int64_t iv) { add_dw_int(dw, base + i, iv); },
        [&](size_t i) { dw.counts[base + i] += 1; },
        [&](size_t i, platform::ExtendedFloat x) {
          const size_t slot = base + i;
          dw.ext_sx[slot] += x;
          dw.ext_sxx[slot] += x * x;
          dw.counts[slot] += 1;
        },
        as_double, as_int, present);
  };
  const auto charge_group_numeric = [&](FusedGroup& group, size_t r_idx,
                                        bool has_right, auto&& as_double,
                                        auto&& as_int, auto&& present) {
    charge_numeric_with(
        r_idx, has_right,
        [&](size_t i, double dv) {
          const double prev = group.RawDoubles()[i];
          const double next = prev + dv;
          if (std::isinf(next) && !std::isinf(prev) && !std::isinf(dv)) {
            group.RawInfFlags()[i] = 1;
          }
          group.RawDoubles()[i] = next;
          group.RawCounts()[i] += 1;
        },
        [&](size_t i, int64_t iv) {
          int64_t next = 0;
          if (__builtin_add_overflow(group.RawInts()[i], iv, &next)) {
            group.RawInfFlags()[i] = 1;
          } else {
            group.RawInts()[i] = next;
          }
          group.RawCounts()[i] += 1;
        },
        [&](size_t i) { group.RawCounts()[i] += 1; },
        [&](size_t i, platform::ExtendedFloat x) {
          group.RawStatX()[i] += x;
          group.RawStatXX()[i] += x * x;
          group.RawCounts()[i] += 1;
        },
        as_double, as_int, present);
  };
  const auto scatter_radix_tuple =
      [&](size_t worker_id, uint64_t ukey, auto&& as_double, auto&& as_int,
          auto&& present, auto&& eval_op9) -> Status {
    const LeftNumericAgg& rop = deferred.left_numeric[deferred.radix_primary];
    uint64_t t_bits = 0;
    uint8_t t_valid = 0;
    if (deferred.radix_tuple_is_int) {
      int64_t iv = 0;
      if (as_int(rop.col0, &iv)) [[likely]] {
        t_bits = static_cast<uint64_t>(iv);
        t_valid = 1;
      }
    } else {
      switch (rop.op) {
        case 0:
          t_valid = 1;
          break;
        case 1:
        case 6: {
          double d0 = 0.0;
          double d1 = 0.0;
          if (as_double(rop.col0, &d0) && as_double(rop.col1, &d1)) [[likely]] {
            const double dv = rop.op == 1 ? d0 * d1 : d0 * (rop.scale - d1);
            t_bits = std::bit_cast<uint64_t>(dv);
            t_valid = 1;
          }
          break;
        }
        case 2:
        case 8: {
          double dv = 0.0;
          if (as_double(rop.col0, &dv)) [[likely]] {
            t_bits = std::bit_cast<uint64_t>(dv);
            t_valid = 1;
          }
          break;
        }
        case 4:
          if (present(rop.col0)) [[likely]] {
            t_valid = 1;
          }
          break;
        case 5:
          break;
        case 7:
          if (present(rop.col0) && present(rop.col1)) [[likely]] {
            t_valid = 1;
          }
          break;
        case 9: {
          double dv = 0.0;
          bool is_null = false;
          RETURN_IF_FAIL(eval_op9(rop.orig_agg, &dv, &is_null));
          if (!is_null) [[likely]] {
            t_bits = std::bit_cast<uint64_t>(dv);
            t_valid = 1;
          }
          break;
        }
        case 17: {
          double d0 = 0.0;
          double d1 = 0.0;
          const double dv =
              (as_double(rop.col0, &d0) && as_double(rop.col1, &d1)) ? (d0 * d1)
                                                                     : 0.0;
          t_bits = std::bit_cast<uint64_t>(dv);
          t_valid = 1;
          break;
        }
        default:
          break;
      }
    }
    RadixSoABucket& bucket =
        deferred.worker_radix_buckets[worker_id][FusedRadixBucket(ukey)];
    bucket.keys.push_back(ukey);
    bucket.valids.push_back(t_valid);
    if (deferred.radix_tuple_is_int) {
      bucket.ivals.push_back(static_cast<int64_t>(t_bits));
    } else {
      bucket.dvals.push_back(std::bit_cast<double>(t_bits));
    }
    return Status::kSuccess;
  };
  const uint32_t* const right_gids =
      keys.identity_right_gid ? nullptr : keys.right_row_gid.data();
  const auto gid_of = [&](uint32_t r_idx) -> uint32_t {
    return right_gids == nullptr ? r_idx : right_gids[r_idx];
  };
  if (source.deferred_left_probe != nullptr) {
    // Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
    //   (interleaved_worker_partition_bijection_sound)
    // Assign 64-block stripes deterministically across workers (`w, w+W,
    // ...`) so parallel floating-point / ROLLUP partial sums are 100%
    // reproducible across runs while preserving PK-cursor locality and
    // date-range balance.
    const LSTableStore& store = *source.deferred_left_probe->store;
    const size_t total_blocks = store.blocks.size();
    size_t stripe_idx = w;
    size_t block_cursor = 0, block_end = 0;
    const auto next_block = [&]() {
      if (block_cursor == block_end) {
        block_cursor = stripe_idx * 64;
        block_end = block_cursor + 64;
        stripe_idx += source.workers;
      }
      return block_cursor++;
    };
    const size_t base_proj_cols = source.deferred_left_probe->projection.size();
    const auto* peek_ptr = source.deferred_left_probe->integer_peeks.empty()
                               ? nullptr
                               : &source.deferred_left_probe->integer_peeks;
    const ScanRuntimeFilter& primary_rf =
        source.deferred_left_probe->runtime_filter;
    const RuntimeFilterGroup* active_rf_group =
        primary_rf.enabled() ? primary_rf.group.get() : nullptr;
    const std::optional<slot_t> full_rf_col =
        (active_rf_group != nullptr && primary_rf.column < base_proj_cols)
            ? std::optional<slot_t>(
                  source.deferred_left_probe->projection[primary_rf.column])
            : std::nullopt;
    const bool rf_has_dense =
        active_rf_group != nullptr && active_rf_group->has_dense_bits();
    const uint64_t* const rf_dense_bits =
        rf_has_dense ? active_rf_group->dense_bits_data() : nullptr;
    const size_t rf_dense_words =
        rf_has_dense ? active_rf_group->dense_bits_word_count() : 0;
    const int64_t rf_lo = rf_has_dense ? active_rf_group->Lo() : 0;
    const int64_t rf_hi = rf_has_dense ? active_rf_group->Hi() : 0;
    const bool rf_col_monotonic =
        rf_has_dense && full_rf_col.has_value() &&
        *full_rf_col < store.column_is_monotonic.size() &&
        store.column_is_monotonic[*full_rf_col] != 0;
    Row left_row;
    left_row.values_.resize(source.left_width);
    for (size_t c = 0; c < base_proj_cols; ++c) {
      if (source.def_overwritten_by_pk[c] == 0) {
        left_row.values_[c].type = source.def_base_types[c];
      }
    }
    struct DefPkGroupCursor {
      int64_t last_key{std::numeric_limits<int64_t>::min()};
      bool have_last{false};
      bool last_matched{false};
      uint32_t last_nb{0};
      uint32_t last_row_in_blk{0};
      uint32_t last_subgid{0};
    };
    std::array<DefPkGroupCursor, 4> pk_group_cursors{};
    std::array<const LSTableColumnBlock*, 16> proj_cols{};
    std::array<const int64_t*, 16> proj_int_ptrs{};
    std::array<uint8_t, 16> proj_has_null{};
    const auto write_pk_group_payloads = [&](size_t gi,
                                             const DefPkGroupCursor& cur) {
      const uint32_t nb = cur.last_nb;
      const uint32_t rin = cur.last_row_in_blk;
      for (const size_t li : source.def_pk_groups[gi].lookup_indices) {
        const PreparedDeferredPkLookup& lkp = source.prepared_pk_lookups[li];
        const PreparedDeferredPkBlock& bm = lkp.blocks[nb];
        Value& dst = left_row.values_[lkp.target_proj_col];
        if (bm.has_null_payload != 0 && bm.pay_blk->IsNull(rin)) [[unlikely]] {
          dst = Value();
        } else if (lkp.payload_type == ValueType::kVarChar) {
          dst.type = ValueType::kVarChar;
          dst.SetCollation(0);
          dst.owned_data.clear();
          dst.value.varchar_value = bm.pay_blk->VarcharAt(rin);
        } else if (lkp.payload_type == ValueType::kDouble) {
          dst.type = ValueType::kDouble;
          dst.SetCollation(0);
          dst.value.double_value = std::bit_cast<double>(bm.pay_cells[rin]);
        } else {
          dst.type = lkp.payload_type;
          dst.SetCollation(0);
          if (lkp.payload_unsigned) {
            dst.SetUnsigned();
          }
          dst.value.int_value = bm.pay_cells[rin];
        }
      }
    };
    const auto resolve_def_pk_groups = [&](uint32_t r,
                                           bool write_all_payloads) -> bool {
      local.have_cur_pk_subgids = false;
      for (size_t gi = 0; gi < source.def_pk_groups.size(); ++gi) {
        const PreparedDeferredPkGroup& grp = source.def_pk_groups[gi];
        DefPkGroupCursor& cur = pk_group_cursors[gi];
        if (proj_has_null[grp.pk_col] != 0 && proj_cols[grp.pk_col]->IsNull(r))
            [[unlikely]] {
          return false;
        }
        const int64_t k = proj_int_ptrs[grp.pk_col][r];
        if (k < grp.min_k || k > grp.max_k) [[unlikely]] {
          return false;
        }
        if (!cur.have_last || k != cur.last_key) {
          if (grp.dense_ptr != nullptr) {
            const auto& entry = grp.dense_ptr[static_cast<size_t>(
                static_cast<uint64_t>(k) - static_cast<uint64_t>(grp.min_k))];
            cur.last_key = k;
            cur.have_last = true;
            if (entry.nb == std::numeric_limits<uint32_t>::max()) [[unlikely]] {
              cur.last_matched = false;
              return false;
            }
            cur.last_nb = entry.nb;
            cur.last_row_in_blk = entry.rin;
            cur.last_matched = true;
            if (grp.use_subgid) {
              cur.last_subgid = entry.subgid;
            }
          } else {
            const auto nb_count = static_cast<uint32_t>(grp.blocks.size());
            const auto* const meta_ptr = grp.blocks.data();
            uint32_t nb = cur.last_nb;
            if (nb >= nb_count || k > meta_ptr[nb].max_k ||
                k < meta_ptr[nb].min_k) {
              if (nb + 1 < nb_count && k <= meta_ptr[nb + 1].max_k &&
                  k >= meta_ptr[nb + 1].min_k) {
                nb = nb + 1;
              } else {
                const auto b = static_cast<size_t>(
                    (static_cast<__uint128_t>(static_cast<uint64_t>(k) -
                                              static_cast<uint64_t>(grp.min_k))
                     << PreparedDeferredPkLookup::kDirShift) /
                    grp.span);
                const uint32_t lo = grp.bucket_lo[b];
                const uint32_t hi =
                    std::min<uint32_t>(nb_count - 1, grp.bucket_lo[b + 1] + 1);
                if (hi - lo <= 4) {
                  nb = lo;
                  while (nb < hi && meta_ptr[nb].max_k < k) {
                    ++nb;
                  }
                } else {
                  const auto it =
                      std::lower_bound(grp.blk_max_keys.begin() + lo,
                                       grp.blk_max_keys.begin() + hi + 1, k);
                  nb = static_cast<uint32_t>(
                      std::distance(grp.blk_max_keys.begin(), it));
                }
              }
            }
            const bool same_blk_monotonic =
                cur.have_last && nb == cur.last_nb && k > cur.last_key;
            if (nb != cur.last_nb) {
              cur.last_row_in_blk = 0;
            }
            cur.last_nb = nb;
            cur.last_key = k;
            cur.have_last = true;
            const auto& bm = meta_ptr[nb];
            if (k < bm.min_k || k > bm.max_k) [[unlikely]] {
              cur.last_matched = false;
              return false;
            }
            uint32_t row_in_blk = bm.row_count;
            if (bm.dense_contiguous != 0) {
              row_in_blk = static_cast<uint32_t>(
                  static_cast<uint64_t>(k) - static_cast<uint64_t>(bm.min_k));
              cur.last_row_in_blk = row_in_blk;
            } else {
              const int64_t* const begin_ptr = bm.pk_cells;
              const int64_t* const end_ptr = begin_ptr + bm.row_count;
              const int64_t* search_begin = begin_ptr;
              const int64_t* search_end = end_ptr;
              if (same_blk_monotonic && cur.last_row_in_blk < bm.row_count) {
                const uint32_t cur_r = cur.last_row_in_blk;
                if (begin_ptr[cur_r] == k) {
                  row_in_blk = cur_r;
                  search_begin = end_ptr;
                } else if (begin_ptr[cur_r] < k) {
                  if (cur_r + 1 < bm.row_count && begin_ptr[cur_r + 1] == k) {
                    row_in_blk = cur_r + 1;
                    search_begin = end_ptr;
                  } else {
                    search_begin = begin_ptr + cur_r + 1;
                    if (cur_r + 16 < bm.row_count &&
                        begin_ptr[cur_r + 16] >= k) {
                      search_end = begin_ptr + cur_r + 17;
                    } else if (cur_r + 64 < bm.row_count &&
                               begin_ptr[cur_r + 64] >= k) {
                      search_begin = begin_ptr + cur_r + 16;
                      search_end = begin_ptr + cur_r + 65;
                    }
                  }
                }
              }
              if (search_begin < search_end) {
                const int64_t* const found_ptr =
                    std::lower_bound(search_begin, search_end, k);
                const auto off = static_cast<uint32_t>(found_ptr - begin_ptr);
                cur.last_row_in_blk = off;
                if (found_ptr != search_end && *found_ptr == k) [[likely]] {
                  row_in_blk = off;
                }
              } else {
                cur.last_row_in_blk = row_in_blk;
              }
            }
            if (row_in_blk >= bm.row_count) [[unlikely]] {
              cur.last_matched = false;
              return false;
            }
            cur.last_matched = true;
            if (grp.use_subgid) {
              cur.last_subgid = grp.subgid_ptr[bm.row_base + row_in_blk];
            }
          }
        } else if (!cur.last_matched) [[unlikely]] {
          return false;
        }
        if (grp.use_subgid) {
          local.cur_pk_subgids[gi] = cur.last_subgid;
        }
        if (write_all_payloads) {
          write_pk_group_payloads(gi, cur);
        }
      }
      local.have_cur_pk_subgids = true;
      return true;
    };
    const auto write_base_left_cols = [&](uint32_t r) {
      for (size_t c = 0; c < base_proj_cols; ++c) {
        if (source.def_overwritten_by_pk[c] != 0) {
          continue;
        }
        Value& dst = left_row.values_[c];
        if (proj_has_null[c] != 0 && proj_cols[c]->IsNull(r)) [[unlikely]] {
          dst = Value();
          continue;
        }
        const ValueType vt = source.def_base_types[c];
        dst.type = vt;
        const int64_t raw = proj_int_ptrs[c][r];
        if (vt == ValueType::kDouble) {
          dst.value.double_value = std::bit_cast<double>(raw);
        } else {
          dst.value.int_value = raw;
        }
      }
    };
    const auto finish_lazy_left_row = [&](uint32_t r) {
      for (size_t gi = 0; gi < source.def_pk_groups.size(); ++gi) {
        write_pk_group_payloads(gi, pk_group_cursors[gi]);
      }
      write_base_left_cols(r);
    };
    const auto materialize_current_left_row = [&](uint32_t r) -> bool {
      if (!source.def_pk_groups.empty() && !resolve_def_pk_groups(r, true)) {
        return false;
      }
      write_base_left_cols(r);
      return true;
    };
    const auto read_def_left_raw = [&](size_t c, uint32_t r, int64_t* out_raw,
                                       ValueType* out_type,
                                       bool* out_unsigned) -> bool {
      const int8_t li = c < 16 ? source.def_col_to_pk_lookup[c] : int8_t{-1};
      if (li >= 0) {
        const PreparedDeferredPkLookup& lkp =
            source.prepared_pk_lookups[static_cast<size_t>(li)];
        if (lkp.payload_type == ValueType::kVarChar) {
          return false;
        }
        const DefPkGroupCursor& cur = pk_group_cursors[lkp.group_idx];
        const PreparedDeferredPkBlock& bm = lkp.blocks[cur.last_nb];
        if (bm.has_null_payload != 0 && bm.pay_blk->IsNull(cur.last_row_in_blk))
            [[unlikely]] {
          return false;
        }
        *out_raw = bm.pay_cells[cur.last_row_in_blk];
        *out_type = lkp.payload_type;
        *out_unsigned = lkp.payload_unsigned;
        return true;
      }
      if (proj_has_null[c] != 0 && proj_cols[c]->IsNull(r)) [[unlikely]] {
        return false;
      }
      *out_raw = proj_int_ptrs[c][r];
      *out_type = source.def_base_types[c];
      *out_unsigned = false;
      return true;
    };
    const auto read_pair_raw_int = [&](slot_t col, uint32_t r, uint32_t r_idx,
                                       int64_t* out_val, ValueType* out_type,
                                       bool* out_unsigned) -> bool {
      if (col < source.left_width) {
        return read_def_left_raw(col, r, out_val, out_type, out_unsigned);
      }
      Value rv_tmp;
      const Value& v =
          source.read_right_cell(r_idx, col - source.left_width, &rv_tmp);
      if (v.IsNull()) [[unlikely]] {
        return false;
      }
      *out_val = v.value.int_value;
      *out_type = v.type;
      *out_unsigned = v.IsUnsigned();
      return true;
    };
    const auto eval_double_arith_cells = [&](size_t agg_idx, uint32_t r,
                                             uint32_t r_idx, double* out,
                                             bool* is_null) -> Status {
      const CompiledDefDoubleArith& plan =
          deferred.compiled_double_arith[agg_idx];
      switch (plan.pattern) {
        case CompiledDefDoubleArith::Pattern::kSubSubAddDivConst: {
          const uint32_t c0 = plan.fast_cols[0];
          const uint32_t c1 = plan.fast_cols[1];
          const uint32_t c2 = plan.fast_cols[2];
          const uint32_t c3 = plan.fast_cols[3];
          if ((proj_has_null[c0] | proj_has_null[c1] | proj_has_null[c2] |
               proj_has_null[c3]) == 0) [[likely]] {
            const auto v0 = std::bit_cast<double>(proj_int_ptrs[c0][r]);
            const auto v1 = std::bit_cast<double>(proj_int_ptrs[c1][r]);
            const auto v2 = std::bit_cast<double>(proj_int_ptrs[c2][r]);
            const auto v3 = std::bit_cast<double>(proj_int_ptrs[c3][r]);
            *out = (((v0 - v1) - v2) + v3) / plan.fast_div;
            *is_null = false;
            return Status::kSuccess;
          }
          if ((proj_has_null[c0] != 0 && proj_cols[c0]->IsNull(r)) ||
              (proj_has_null[c1] != 0 && proj_cols[c1]->IsNull(r)) ||
              (proj_has_null[c2] != 0 && proj_cols[c2]->IsNull(r)) ||
              (proj_has_null[c3] != 0 && proj_cols[c3]->IsNull(r))) {
            *is_null = true;
            return Status::kSuccess;
          }
          const auto v0 = std::bit_cast<double>(proj_int_ptrs[c0][r]);
          const auto v1 = std::bit_cast<double>(proj_int_ptrs[c1][r]);
          const auto v2 = std::bit_cast<double>(proj_int_ptrs[c2][r]);
          const auto v3 = std::bit_cast<double>(proj_int_ptrs[c3][r]);
          *out = (((v0 - v1) - v2) + v3) / plan.fast_div;
          *is_null = false;
          return Status::kSuccess;
        }
        case CompiledDefDoubleArith::Pattern::kSubDivConst: {
          const uint32_t c0 = plan.fast_cols[0];
          const uint32_t c1 = plan.fast_cols[1];
          if ((proj_has_null[c0] | proj_has_null[c1]) != 0 &&
              ((proj_has_null[c0] != 0 && proj_cols[c0]->IsNull(r)) ||
               (proj_has_null[c1] != 0 && proj_cols[c1]->IsNull(r)))) {
            *is_null = true;
            return Status::kSuccess;
          }
          const auto v0 = std::bit_cast<double>(proj_int_ptrs[c0][r]);
          const auto v1 = std::bit_cast<double>(proj_int_ptrs[c1][r]);
          *out = (v0 - v1) / plan.fast_div;
          *is_null = false;
          return Status::kSuccess;
        }
        case CompiledDefDoubleArith::Pattern::kSub:
        case CompiledDefDoubleArith::Pattern::kAdd: {
          const uint32_t c0 = plan.fast_cols[0];
          const uint32_t c1 = plan.fast_cols[1];
          if ((proj_has_null[c0] | proj_has_null[c1]) != 0 &&
              ((proj_has_null[c0] != 0 && proj_cols[c0]->IsNull(r)) ||
               (proj_has_null[c1] != 0 && proj_cols[c1]->IsNull(r)))) {
            *is_null = true;
            return Status::kSuccess;
          }
          const auto v0 = std::bit_cast<double>(proj_int_ptrs[c0][r]);
          const auto v1 = std::bit_cast<double>(proj_int_ptrs[c1][r]);
          *out = plan.pattern == CompiledDefDoubleArith::Pattern::kSub
                     ? (v0 - v1)
                     : (v0 + v1);
          *is_null = false;
          return Status::kSuccess;
        }
        case CompiledDefDoubleArith::Pattern::kGeneral:
          break;
      }
      std::array<double, 16> values{};
      std::array<uint8_t, 16> nulls{};
      size_t depth = 0;
      for (const CompiledDefDoubleArith::Step& st : plan.steps) {
        switch (st.kind) {
          case 0: {
            const uint32_t col = st.operand;
            if (proj_has_null[col] != 0 && proj_cols[col]->IsNull(r))
                [[unlikely]] {
              nulls[depth] = 1;
            } else {
              nulls[depth] = 0;
              values[depth] = std::bit_cast<double>(proj_int_ptrs[col][r]);
            }
            ++depth;
            break;
          }
          case 8: {
            const uint32_t col = st.operand;
            if (proj_has_null[col] != 0 && proj_cols[col]->IsNull(r))
                [[unlikely]] {
              nulls[depth] = 1;
            } else {
              nulls[depth] = 0;
              values[depth] = static_cast<double>(proj_int_ptrs[col][r]);
            }
            ++depth;
            break;
          }
          case 1: {
            Value rv_tmp;
            const Value& v = source.read_right_cell(r_idx, st.operand, &rv_tmp);
            nulls[depth] = static_cast<uint8_t>(v.IsNull());
            if (!nulls[depth]) {
              values[depth] = v.value.double_value;
            }
            ++depth;
            break;
          }
          case 2:
            values[depth] = st.cst;
            nulls[depth++] = 0;
            break;
          case 3: {
            const size_t rhs = --depth;
            const size_t lhs = depth - 1;
            nulls[lhs] |= nulls[rhs];
            if (!nulls[lhs]) [[likely]] {
              values[lhs] += values[rhs];
            }
            break;
          }
          case 4: {
            const size_t rhs = --depth;
            const size_t lhs = depth - 1;
            nulls[lhs] |= nulls[rhs];
            if (!nulls[lhs]) [[likely]] {
              values[lhs] -= values[rhs];
            }
            break;
          }
          case 5: {
            const size_t rhs = --depth;
            const size_t lhs = depth - 1;
            nulls[lhs] |= nulls[rhs];
            if (!nulls[lhs]) [[likely]] {
              values[lhs] *= values[rhs];
            }
            break;
          }
          case 6: {
            const size_t rhs = --depth;
            const size_t lhs = depth - 1;
            nulls[lhs] |= nulls[rhs];
            if (!nulls[lhs]) [[likely]] {
              if (values[rhs] == 0.0) {
                return StatusError(StatusCode::kIsInfinity, "division by zero");
              }
              const double quotient = values[lhs] / values[rhs];
              if (std::isfinite(values[lhs]) && std::isfinite(values[rhs]) &&
                  std::isinf(quotient)) {
                return StatusError(StatusCode::kIsInfinity, "double overflow");
              }
              values[lhs] = quotient;
            }
            break;
          }
          case 7: {
            const size_t lhs = depth - 1;
            if (!nulls[lhs]) [[likely]] {
              values[lhs] /= st.cst;
            }
            break;
          }
          default:
            break;
        }
      }
      *out = values[0];
      *is_null = nulls[0] != 0;
      return Status::kSuccess;
    };
    const auto read_def_agg_double = [&](int32_t col, uint32_t r,
                                         double* out) noexcept -> bool {
      const auto c = static_cast<size_t>(col);
      if (proj_has_null[c] != 0 && proj_cols[c]->IsNull(r)) [[unlikely]] {
        return false;
      }
      const int64_t raw = proj_int_ptrs[c][r];
      *out = source.def_base_types[c] == ValueType::kDouble
                 ? std::bit_cast<double>(raw)
                 : static_cast<double>(raw);
      return true;
    };
    const auto read_def_agg_int = [&](int32_t col, uint32_t r,
                                      int64_t* out) noexcept -> bool {
      const auto c = static_cast<size_t>(col);
      if (proj_has_null[c] != 0 && proj_cols[c]->IsNull(r)) [[unlikely]] {
        return false;
      }
      *out = proj_int_ptrs[c][r];
      return true;
    };
    const auto eval_pk_eq_cond =
        [&](const LeftNumericAgg& op) noexcept -> bool {
      const PreparedDeferredPkLookup& lkp =
          source.prepared_pk_lookups[static_cast<size_t>(op.pk_lookup_idx)];
      const DefPkGroupCursor& cur = pk_group_cursors[lkp.group_idx];
      const PreparedDeferredPkBlock& blk = lkp.blocks[cur.last_nb];
      if (blk.has_null_payload != 0 &&
          blk.pay_blk->IsNull(cur.last_row_in_blk)) {
        return false;
      }
      return blk.pay_cells[cur.last_row_in_blk] == op.pk_eq_val;
    };
    const auto process_def_match = [&](uint32_t r, uint32_t r_idx) -> Status {
      if (deferred.def_compact_split_probe) {
        if (!source.def_pk_groups.empty() && !resolve_def_pk_groups(r, false)) {
          return Status::kSuccess;
        }
        const uint32_t r_subgid = !layout.split_uses_right_subgid
                                      ? 0U
                                      : layout.split_right_subgid_ptr[r_idx];
        uint64_t pk_packed = 0;
        bool pk_ok = true;
        if (layout.split_uses_pk_subgid) {
          for (size_t gi = 0; gi < source.def_pk_groups.size(); ++gi) {
            if (source.def_pk_groups[gi].use_subgid) {
              if (local.cur_pk_subgids[gi] ==
                  std::numeric_limits<uint32_t>::max()) [[unlikely]] {
                pk_ok = false;
                break;
              }
              pk_packed |= static_cast<uint64_t>(local.cur_pk_subgids[gi])
                           << source.def_pk_groups[gi].subgid_shift;
            }
          }
        }
        if (r_subgid != std::numeric_limits<uint32_t>::max() && pk_ok)
            [[likely]] {
          uint64_t packed_u64 = pk_packed;
          if (layout.split_uses_right_subgid) {
            packed_u64 |= static_cast<uint64_t>(r_subgid)
                          << layout.split_right_shift;
          }
          if (source.compact_raw_left_key) {
            const size_t col = *layout.split_left_slot;
            int64_t value = 0;
            ValueType type = ValueType::kNull;
            bool is_unsigned = false;
            if (!read_def_left_raw(col, r, &value, &type, &is_unsigned) ||
                type != layout.split_left_type || is_unsigned ||
                !std::in_range<int32_t>(value)) [[unlikely]] {
              return StatusError(StatusCode::kCorrupt,
                                 "packed COUNT key differs from scan metadata");
            }
            packed_u64 |= static_cast<uint32_t>(static_cast<int32_t>(value));
          }
          ++local.comparisons;
          return callbacks.record_compact_split(local, packed_u64);
        }
        return Status::kSuccess;
      }
      if (deferred.def_group_cell_probe) {
        if (!source.def_pk_groups.empty() && !resolve_def_pk_groups(r, false)) {
          return Status::kSuccess;
        }
        if (deferred.use_dense_split_direct_gid) {
          const uint32_t r_subgid = !layout.split_uses_right_subgid
                                        ? 0U
                                        : layout.split_right_subgid_ptr[r_idx];
          if (r_subgid == std::numeric_limits<uint32_t>::max()) [[unlikely]] {
            return Status::kSuccess;
          }
          uint32_t gid = r_subgid;
          for (const size_t gi : deferred.dense_pk_group_indices) {
            const uint32_t psg = local.cur_pk_subgids[gi];
            if (psg == std::numeric_limits<uint32_t>::max()) [[unlikely]] {
              return Status::kSuccess;
            }
            gid = (gid * source.def_pk_groups[gi].subgid_count) + psg;
          }
          ++local.comparisons;
          DirectGidWorker& dw = direct.direct_gid_workers[w];
          if (deferred.use_packed_double_cells) {
            DirectDoubleCell& cell = dw.double_cells[gid];
            cell.meta |= 1ULL;
            const auto charge_packed = [&](double dv) noexcept {
              const double prev = cell.sum;
              const double next = prev + dv;
              uint64_t meta = (cell.meta + 4ULL) | 1ULL;
              if (std::isinf(next) && !std::isinf(prev) && !std::isinf(dv))
                  [[unlikely]] {
                meta |= 2ULL;
              }
              cell.sum = next;
              cell.meta = meta;
            };
            const LeftNumericAgg& op = deferred.left_numeric[0];
            switch (op.op) {
              case 1:
              case 6: {
                double d0 = 0.0;
                double d1 = 0.0;
                if (read_def_agg_double(op.col0, r, &d0) &&
                    read_def_agg_double(op.col1, r, &d1)) [[likely]] {
                  charge_packed(op.op == 1 ? d0 * d1 : d0 * (op.scale - d1));
                }
                break;
              }
              case 2: {
                double dv = 0.0;
                if (read_def_agg_double(op.col0, r, &dv)) [[likely]] {
                  charge_packed(dv);
                }
                break;
              }
              case 9: {
                double dv = 0.0;
                bool is_null = false;
                RETURN_IF_FAIL(eval_double_arith_cells(
                    static_cast<size_t>(op.orig_agg), r, r_idx, &dv, &is_null));
                if (!is_null) [[likely]] {
                  charge_packed(dv);
                }
                break;
              }
              case 10:
              case 11:
                charge_right_case_op(
                    op, r_idx, true, [&](double dv) { charge_packed(dv); },
                    [&](int64_t) {}, []() {},
                    [&](int32_t col, double* out) {
                      return read_def_agg_double(col, r, out);
                    },
                    [&](int32_t col) {
                      const auto c = static_cast<size_t>(col);
                      return proj_has_null[c] == 0 || !proj_cols[c]->IsNull(r);
                    });
                break;
              case 14: {
                const uint8_t rc = (r_idx != kInvalidRightIdx)
                                       ? op.right_cond[r_idx]
                                       : uint8_t{0};
                if (rc == 1) {
                  double dv = 0.0;
                  bool is_null = false;
                  RETURN_IF_FAIL(
                      eval_double_arith_cells(static_cast<size_t>(op.orig_agg),
                                              r, r_idx, &dv, &is_null));
                  if (!is_null) [[likely]] {
                    charge_packed(dv);
                  }
                } else if (rc == 0 && !op.case_else_null) {
                  charge_packed(0.0);
                }
                break;
              }
              case 16: {
                if (eval_pk_eq_cond(op)) {
                  double dv = 0.0;
                  bool is_null = false;
                  RETURN_IF_FAIL(
                      eval_double_arith_cells(static_cast<size_t>(op.orig_agg),
                                              r, r_idx, &dv, &is_null));
                  if (!is_null) [[likely]] {
                    charge_packed(dv);
                  }
                } else if (!op.case_else_null) {
                  charge_packed(0.0);
                }
                break;
              }
              case 17: {
                double d0 = 0.0;
                double d1 = 0.0;
                const double dv = (read_def_agg_double(op.col0, r, &d0) &&
                                   read_def_agg_double(op.col1, r, &d1))
                                      ? (d0 * d1)
                                      : 0.0;
                charge_packed(dv);
                break;
              }
              default:
                break;
            }
            return Status::kSuccess;
          }
          dw.matched[gid] = 1;
          const size_t base = static_cast<size_t>(gid) * direct.agg_count;
          if (deferred.disjoint_bucket5_ready) {
            // Lean proof:
            // lean/TinylambOpt/ResearchProofs/Wave9TpcdsRollupAndSetOps.lean
            //   (DisjointRangeBucketHistogram_sound)
            int64_t a = 0;
            int64_t b = 0;
            if (read_def_agg_int(deferred.disjoint_bucket5_col0, r, &a) &&
                read_def_agg_int(deferred.disjoint_bucket5_col1, r, &b))
                [[likely]] {
              int64_t d = 0;
              if (__builtin_sub_overflow(a, b, &d)) [[unlikely]] {
                return StatusError(StatusCode::kIsInfinity,
                                   "integer overflow in subtraction");
              }
              const size_t target_slot =
                  base +
                  deferred
                      .disjoint_bucket5_slots[ClassifyDisjointRangeBucket5(d)];
              int64_t next = 0;
              if (__builtin_add_overflow(dw.ints[target_slot], 1, &next))
                  [[unlikely]] {
                dw.inf_flags[target_slot] = 1;
              } else {
                dw.ints[target_slot] = next;
              }
            }
            dw.counts[base + 0] += 1;
            dw.counts[base + 1] += 1;
            dw.counts[base + 2] += 1;
            dw.counts[base + 3] += 1;
            dw.counts[base + 4] += 1;
            return Status::kSuccess;
          }
          for (size_t i = 0; i < direct.agg_count; ++i) {
            const LeftNumericAgg& op = deferred.left_numeric[i];
            const size_t slot = base + i;
            switch (op.op) {
              case 0:
                dw.counts[slot] += 1;
                break;
              case 1:
              case 6: {
                double d0 = 0.0;
                double d1 = 0.0;
                if (read_def_agg_double(op.col0, r, &d0) &&
                    read_def_agg_double(op.col1, r, &d1)) [[likely]] {
                  add_dw_double(dw, slot,
                                op.op == 1 ? d0 * d1 : d0 * (op.scale - d1));
                }
                break;
              }
              case 17: {
                double d0 = 0.0;
                double d1 = 0.0;
                const double dv = (read_def_agg_double(op.col0, r, &d0) &&
                                   read_def_agg_double(op.col1, r, &d1))
                                      ? (d0 * d1)
                                      : 0.0;
                add_dw_double(dw, slot, dv);
                break;
              }
              case 2: {
                double dv = 0.0;
                if (read_def_agg_double(op.col0, r, &dv)) [[likely]] {
                  add_dw_double(dw, slot, dv);
                }
                break;
              }
              case 3: {
                int64_t iv = 0;
                if (read_def_agg_int(op.col0, r, &iv)) [[likely]] {
                  add_dw_int(dw, slot, iv);
                }
                break;
              }
              case 4: {
                const auto c0 = static_cast<size_t>(op.col0);
                if (proj_has_null[c0] == 0 || !proj_cols[c0]->IsNull(r)) {
                  dw.counts[slot] += 1;
                }
                break;
              }
              case 7: {
                const auto c0 = static_cast<size_t>(op.col0);
                const auto c1 = static_cast<size_t>(op.col1);
                if ((proj_has_null[c0] == 0 || !proj_cols[c0]->IsNull(r)) &&
                    (proj_has_null[c1] == 0 || !proj_cols[c1]->IsNull(r))) {
                  dw.counts[slot] += 1;
                }
                break;
              }
              case 8: {
                platform::ExtendedFloat x = 0;
                bool ok_x = false;
                if (op.is_int_col) {
                  int64_t iv = 0;
                  if (read_def_agg_int(op.col0, r, &iv)) [[likely]] {
                    x = static_cast<platform::ExtendedFloat>(iv);
                    ok_x = true;
                  }
                } else {
                  double dv = 0.0;
                  if (read_def_agg_double(op.col0, r, &dv)) [[likely]] {
                    x = static_cast<platform::ExtendedFloat>(dv);
                    ok_x = true;
                  }
                }
                if (ok_x) [[likely]] {
                  dw.ext_sx[slot] += x;
                  dw.ext_sxx[slot] += x * x;
                  dw.counts[slot] += 1;
                }
                break;
              }
              case 9: {
                double dv = 0.0;
                bool is_null = false;
                RETURN_IF_FAIL(eval_double_arith_cells(
                    static_cast<size_t>(op.orig_agg), r, r_idx, &dv, &is_null));
                if (!is_null) [[likely]] {
                  add_dw_double(dw, slot, dv);
                }
                break;
              }
              case 10:
              case 11:
              case 12:
              case 13:
                charge_right_case_op(
                    op, r_idx, true,
                    [&](double dv) { add_dw_double(dw, slot, dv); },
                    [&](int64_t iv) { add_dw_int(dw, slot, iv); },
                    [&]() { dw.counts[slot] += 1; },
                    [&](int32_t col, double* out) {
                      return read_def_agg_double(col, r, out);
                    },
                    [&](int32_t col) {
                      const auto c = static_cast<size_t>(col);
                      return proj_has_null[c] == 0 || !proj_cols[c]->IsNull(r);
                    });
                break;
              case 14: {
                const uint8_t rc = (r_idx != kInvalidRightIdx)
                                       ? op.right_cond[r_idx]
                                       : uint8_t{0};
                if (rc == 1) {
                  double dv = 0.0;
                  bool is_null = false;
                  RETURN_IF_FAIL(
                      eval_double_arith_cells(static_cast<size_t>(op.orig_agg),
                                              r, r_idx, &dv, &is_null));
                  if (!is_null) [[likely]] {
                    add_dw_double(dw, slot, dv);
                  }
                } else if (rc == 0 && !op.case_else_null) {
                  add_dw_double(dw, slot, 0.0);
                }
                break;
              }
              case 15: {
                int64_t a = 0;
                int64_t b = 0;
                int64_t add = 0;
                if (read_def_agg_int(op.col0, r, &a) &&
                    read_def_agg_int(op.col1, r, &b)) [[likely]] {
                  int64_t d = 0;
                  if (__builtin_sub_overflow(a, b, &d)) [[unlikely]] {
                    return StatusError(StatusCode::kIsInfinity,
                                       "integer overflow in subtraction");
                  }
                  const bool lo_ok =
                      !op.diff_has_lo ||
                      (op.diff_lo_incl ? (d >= op.diff_lo) : (d > op.diff_lo));
                  const bool hi_ok =
                      !op.diff_has_hi ||
                      (op.diff_hi_incl ? (d <= op.diff_hi) : (d < op.diff_hi));
                  if (lo_ok && hi_ok) {
                    add = 1;
                  }
                }
                add_dw_int(dw, slot, add);
                break;
              }
              case 16: {
                if (eval_pk_eq_cond(op)) {
                  double dv = 0.0;
                  bool is_null = false;
                  RETURN_IF_FAIL(
                      eval_double_arith_cells(static_cast<size_t>(op.orig_agg),
                                              r, r_idx, &dv, &is_null));
                  if (!is_null) [[likely]] {
                    add_dw_double(dw, slot, dv);
                  }
                } else if (!op.case_else_null) {
                  add_dw_double(dw, slot, 0.0);
                }
                break;
              }
              default:
                break;
            }
          }
          return Status::kSuccess;
        }
        bool key_ok = false;
        int64_t lookup_key = 0;
        uint64_t computed_key_hash = 0;
        bool has_computed_key_hash = false;
        bool need_rep_left = true;
        if (layout.use_split_key_gid) {
          const uint32_t r_subgid = !layout.split_uses_right_subgid
                                        ? 0U
                                        : layout.split_right_subgid_ptr[r_idx];
          int64_t l_val = 0;
          bool l_ok = !layout.split_has_left_probe;
          if (layout.split_has_left_probe) {
            const auto& l_shape = source.key_shapes[layout.split_left_k];
            if (l_shape.direct_column >= 0) {
              ValueType ltype = ValueType::kNull;
              bool lunsigned = false;
              if (read_def_left_raw(static_cast<size_t>(l_shape.direct_column),
                                    r, &l_val, &ltype, &lunsigned) &&
                  !lunsigned &&
                  (ltype == ValueType::kInt64 || ltype == ValueType::kDate) &&
                  l_val >= static_cast<int64_t>(
                               std::numeric_limits<int32_t>::min()) &&
                  l_val <=
                      static_cast<int64_t>(std::numeric_limits<int32_t>::max()))
                  [[likely]] {
                l_ok = true;
              }
            }
          }
          bool pk_ok = true;
          uint64_t pk_packed = 0;
          if (layout.split_uses_pk_subgid) {
            for (size_t gi = 0; gi < source.def_pk_groups.size(); ++gi) {
              if (source.def_pk_groups[gi].use_subgid) {
                if (local.cur_pk_subgids[gi] ==
                    std::numeric_limits<uint32_t>::max()) [[unlikely]] {
                  pk_ok = false;
                  break;
                }
                pk_packed |= static_cast<uint64_t>(local.cur_pk_subgids[gi])
                             << source.def_pk_groups[gi].subgid_shift;
              }
            }
          }
          if (r_subgid != std::numeric_limits<uint32_t>::max() && l_ok && pk_ok)
              [[likely]] {
            uint64_t packed_u64 = pk_packed;
            if (layout.split_uses_right_subgid) {
              packed_u64 |= static_cast<uint64_t>(r_subgid)
                            << layout.split_right_shift;
            }
            if (layout.split_has_left_probe) {
              packed_u64 |= static_cast<uint64_t>(
                  static_cast<uint32_t>(static_cast<int32_t>(l_val)));
            }
            lookup_key = std::bit_cast<int64_t>(packed_u64);
            computed_key_hash = packed_u64;
            has_computed_key_hash = true;
            need_rep_left = !layout.split_key_unpack_only;
            key_ok = true;
          }
        } else if (layout.packed_2x32_group_key) {
          int64_t v0 = 0;
          int64_t v1 = 0;
          ValueType t0 = ValueType::kNull;
          ValueType t1 = ValueType::kNull;
          bool u0_uns = false;
          bool u1_uns = false;
          if (read_pair_raw_int(layout.packed_2x32_slot0, r, r_idx, &v0, &t0,
                                &u0_uns) &&
              read_pair_raw_int(layout.packed_2x32_slot1, r, r_idx, &v1, &t1,
                                &u1_uns) &&
              t0 == layout.packed_2x32_type0 && !u0_uns &&
              t1 == layout.packed_2x32_type1 && !u1_uns &&
              v0 >= static_cast<int64_t>(std::numeric_limits<int32_t>::min()) &&
              v0 <= static_cast<int64_t>(std::numeric_limits<int32_t>::max()) &&
              v1 >= static_cast<int64_t>(std::numeric_limits<int32_t>::min()) &&
              v1 <= static_cast<int64_t>(std::numeric_limits<int32_t>::max()))
              [[likely]] {
            const uint64_t packed_u64 =
                (static_cast<uint64_t>(
                     static_cast<uint32_t>(static_cast<int32_t>(v0)))
                 << 32) |
                static_cast<uint32_t>(static_cast<int32_t>(v1));
            lookup_key = std::bit_cast<int64_t>(packed_u64);
            computed_key_hash = packed_u64;
            has_computed_key_hash = true;
            need_rep_left = !layout.packed_2x32_unpack_only;
            key_ok = true;
          }
        } else {
          ValueType kt = ValueType::kNull;
          bool k_uns = false;
          if (read_pair_raw_int(source.int_group_slot, r, r_idx, &lookup_key,
                                &kt, &k_uns) &&
              kt == source.int_group_type && !k_uns) [[likely]] {
            key_ok = true;
          }
        }
        if (key_ok) [[likely]] {
          ++local.comparisons;
          if (deferred.use_radix_agg) {
            RETURN_IF_FAIL(scatter_radix_tuple(
                w, std::bit_cast<uint64_t>(lookup_key),
                [&](int32_t col, double* out) {
                  return read_def_agg_double(col, r, out);
                },
                [&](int32_t col, int64_t* out) {
                  return read_def_agg_int(col, r, out);
                },
                [&](int32_t col) {
                  const auto c = static_cast<size_t>(col);
                  return proj_has_null[c] == 0 || !proj_cols[c]->IsNull(r);
                },
                [&](int32_t orig_agg, double* out_dv, bool* out_null) {
                  return eval_double_arith_cells(static_cast<size_t>(orig_agg),
                                                 r, r_idx, out_dv, out_null);
                }));
            return Status::kSuccess;
          }
          bool inserted = false;
          size_t* slot = local.int_offsets.TryEmplace(lookup_key, &inserted);
          size_t group_index = 0;
          if (!inserted) [[likely]] {
            group_index = *slot;
          } else {
            group_index = local.groups.size();
            FusedGroup group;
            local.raw_arena.Initialize(group, direct.agg_count);
            if (!raw_aggregates.skip_accumulator_states) {
              group.state_offset = local.states.size();
              for (size_t i = 0; i < source.fusion->aggregates.size(); ++i) {
                local.states.emplace_back(source.fusion->aggregates[i],
                                          source.aggregate_shapes[i]);
              }
            }
            group.key_hash = computed_key_hash;
            group.has_key_hash = has_computed_key_hash;
            if (need_rep_left) {
              finish_lazy_left_row(r);
              local.owned_rep_left.push_back(left_row);
              group.rep_left = &local.owned_rep_left.back();
            }
            group.rep_right_idx = r_idx;
            local.groups.push_back(std::move(group));
            *slot = group_index;
          }
          FusedGroup& group = local.groups[group_index];
          if (deferred.disjoint_bucket5_ready) {
            // Lean proof:
            // lean/TinylambOpt/ResearchProofs/Wave9TpcdsRollupAndSetOps.lean
            //   (DisjointRangeBucketHistogram_sound)
            int64_t a = 0;
            int64_t b = 0;
            if (read_def_agg_int(deferred.disjoint_bucket5_col0, r, &a) &&
                read_def_agg_int(deferred.disjoint_bucket5_col1, r, &b))
                [[likely]] {
              int64_t d = 0;
              if (__builtin_sub_overflow(a, b, &d)) [[unlikely]] {
                return StatusError(StatusCode::kIsInfinity,
                                   "integer overflow in subtraction");
              }
              const size_t target_i =
                  deferred
                      .disjoint_bucket5_slots[ClassifyDisjointRangeBucket5(d)];
              int64_t next = 0;
              if (__builtin_add_overflow(group.RawInts()[target_i], 1, &next))
                  [[unlikely]] {
                group.RawInfFlags()[target_i] = 1;
              } else {
                group.RawInts()[target_i] = next;
              }
            }
            int64_t* cnts = group.RawCounts();
            cnts[0] += 1;
            cnts[1] += 1;
            cnts[2] += 1;
            cnts[3] += 1;
            cnts[4] += 1;
            return Status::kSuccess;
          }
          for (size_t i = 0; i < direct.agg_count; ++i) {
            const LeftNumericAgg& op = deferred.left_numeric[i];
            switch (op.op) {
              case 0:
                group.RawCounts()[i] += 1;
                break;
              case 1: {
                double d0 = 0.0;
                double d1 = 0.0;
                if (read_def_agg_double(op.col0, r, &d0) &&
                    read_def_agg_double(op.col1, r, &d1)) [[likely]] {
                  const double dv = d0 * d1;
                  const double prev = group.RawDoubles()[i];
                  const double next = prev + dv;
                  if (std::isinf(next) && !std::isinf(prev) &&
                      !std::isinf(dv)) {
                    group.RawInfFlags()[i] = 1;
                  }
                  group.RawDoubles()[i] = next;
                  group.RawCounts()[i] += 1;
                }
                break;
              }
              case 17: {
                double d0 = 0.0;
                double d1 = 0.0;
                const double dv = (read_def_agg_double(op.col0, r, &d0) &&
                                   read_def_agg_double(op.col1, r, &d1))
                                      ? (d0 * d1)
                                      : 0.0;
                const double prev = group.RawDoubles()[i];
                const double next = prev + dv;
                if (std::isinf(next) && !std::isinf(prev) && !std::isinf(dv)) {
                  group.RawInfFlags()[i] = 1;
                }
                group.RawDoubles()[i] = next;
                group.RawCounts()[i] += 1;
                break;
              }
              case 2: {
                double dv = 0.0;
                if (read_def_agg_double(op.col0, r, &dv)) [[likely]] {
                  const double prev = group.RawDoubles()[i];
                  const double next = prev + dv;
                  if (std::isinf(next) && !std::isinf(prev) &&
                      !std::isinf(dv)) {
                    group.RawInfFlags()[i] = 1;
                  }
                  group.RawDoubles()[i] = next;
                  group.RawCounts()[i] += 1;
                }
                break;
              }
              case 3: {
                int64_t iv = 0;
                if (read_def_agg_int(op.col0, r, &iv)) [[likely]] {
                  int64_t next = 0;
                  if (__builtin_add_overflow(group.RawInts()[i], iv, &next)) {
                    group.RawInfFlags()[i] = 1;
                  } else {
                    group.RawInts()[i] = next;
                  }
                  group.RawCounts()[i] += 1;
                }
                break;
              }
              case 4: {
                const auto c0 = static_cast<size_t>(op.col0);
                if (proj_has_null[c0] == 0 || !proj_cols[c0]->IsNull(r)) {
                  group.RawCounts()[i] += 1;
                }
                break;
              }
              case 5:
                break;
              case 6: {
                double d0 = 0.0;
                double d1 = 0.0;
                if (read_def_agg_double(op.col0, r, &d0) &&
                    read_def_agg_double(op.col1, r, &d1)) [[likely]] {
                  const double dv = d0 * (op.scale - d1);
                  const double prev = group.RawDoubles()[i];
                  const double next = prev + dv;
                  if (std::isinf(next) && !std::isinf(prev) &&
                      !std::isinf(dv)) {
                    group.RawInfFlags()[i] = 1;
                  }
                  group.RawDoubles()[i] = next;
                  group.RawCounts()[i] += 1;
                }
                break;
              }
              case 7: {
                const auto c0 = static_cast<size_t>(op.col0);
                const auto c1 = static_cast<size_t>(op.col1);
                if ((proj_has_null[c0] == 0 || !proj_cols[c0]->IsNull(r)) &&
                    (proj_has_null[c1] == 0 || !proj_cols[c1]->IsNull(r))) {
                  group.RawCounts()[i] += 1;
                }
                break;
              }
              case 8: {
                platform::ExtendedFloat x = 0;
                bool ok_x = false;
                if (op.is_int_col) {
                  int64_t iv = 0;
                  if (read_def_agg_int(op.col0, r, &iv)) [[likely]] {
                    x = static_cast<platform::ExtendedFloat>(iv);
                    ok_x = true;
                  }
                } else {
                  double dv = 0.0;
                  if (read_def_agg_double(op.col0, r, &dv)) [[likely]] {
                    x = static_cast<platform::ExtendedFloat>(dv);
                    ok_x = true;
                  }
                }
                if (ok_x) [[likely]] {
                  group.RawStatX()[i] += x;
                  group.RawStatXX()[i] += x * x;
                  group.RawCounts()[i] += 1;
                }
                break;
              }
              case 9: {
                double dv = 0.0;
                bool is_null = false;
                RETURN_IF_FAIL(eval_double_arith_cells(
                    static_cast<size_t>(op.orig_agg), r, r_idx, &dv, &is_null));
                if (!is_null) [[likely]] {
                  const double prev = group.RawDoubles()[i];
                  const double next = prev + dv;
                  if (std::isinf(next) && !std::isinf(prev) &&
                      !std::isinf(dv)) {
                    group.RawInfFlags()[i] = 1;
                  }
                  group.RawDoubles()[i] = next;
                  group.RawCounts()[i] += 1;
                }
                break;
              }
              case 10:
              case 11:
              case 12:
              case 13:
                charge_right_case_op(
                    op, r_idx, r_idx != kInvalidRightIdx,
                    [&](double dv) {
                      const double prev = group.RawDoubles()[i];
                      const double next = prev + dv;
                      if (std::isinf(next) && !std::isinf(prev) &&
                          !std::isinf(dv)) {
                        group.RawInfFlags()[i] = 1;
                      }
                      group.RawDoubles()[i] = next;
                      group.RawCounts()[i] += 1;
                    },
                    [&](int64_t iv) {
                      int64_t next = 0;
                      if (__builtin_add_overflow(group.RawInts()[i], iv,
                                                 &next)) {
                        group.RawInfFlags()[i] = 1;
                      } else {
                        group.RawInts()[i] = next;
                      }
                      group.RawCounts()[i] += 1;
                    },
                    [&]() { group.RawCounts()[i] += 1; },
                    [&](int32_t col, double* out) {
                      return read_def_agg_double(col, r, out);
                    },
                    [&](int32_t col) {
                      const auto c = static_cast<size_t>(col);
                      return proj_has_null[c] == 0 || !proj_cols[c]->IsNull(r);
                    });
                break;
              case 14: {
                const uint8_t rc = (r_idx != kInvalidRightIdx)
                                       ? op.right_cond[r_idx]
                                       : uint8_t{0};
                if (rc == 1) {
                  double dv = 0.0;
                  bool is_null = false;
                  RETURN_IF_FAIL(
                      eval_double_arith_cells(static_cast<size_t>(op.orig_agg),
                                              r, r_idx, &dv, &is_null));
                  if (!is_null) [[likely]] {
                    const double prev = group.RawDoubles()[i];
                    const double next = prev + dv;
                    if (std::isinf(next) && !std::isinf(prev) &&
                        !std::isinf(dv)) {
                      group.RawInfFlags()[i] = 1;
                    }
                    group.RawDoubles()[i] = next;
                    group.RawCounts()[i] += 1;
                  }
                } else if (rc == 0 && !op.case_else_null) {
                  group.RawDoubles()[i] = group.RawDoubles()[i] + 0.0;
                  group.RawCounts()[i] += 1;
                }
                break;
              }
              case 15: {
                int64_t a = 0;
                int64_t b = 0;
                int64_t add = 0;
                if (read_def_agg_int(op.col0, r, &a) &&
                    read_def_agg_int(op.col1, r, &b)) [[likely]] {
                  int64_t d = 0;
                  if (__builtin_sub_overflow(a, b, &d)) [[unlikely]] {
                    return StatusError(StatusCode::kIsInfinity,
                                       "integer overflow in subtraction");
                  }
                  const bool lo_ok =
                      !op.diff_has_lo ||
                      (op.diff_lo_incl ? (d >= op.diff_lo) : (d > op.diff_lo));
                  const bool hi_ok =
                      !op.diff_has_hi ||
                      (op.diff_hi_incl ? (d <= op.diff_hi) : (d < op.diff_hi));
                  if (lo_ok && hi_ok) {
                    add = 1;
                  }
                }
                int64_t next = 0;
                if (__builtin_add_overflow(group.RawInts()[i], add, &next)) {
                  group.RawInfFlags()[i] = 1;
                } else {
                  group.RawInts()[i] = next;
                }
                group.RawCounts()[i] += 1;
                break;
              }
              case 16: {
                if (eval_pk_eq_cond(op)) {
                  double dv = 0.0;
                  bool is_null = false;
                  RETURN_IF_FAIL(
                      eval_double_arith_cells(static_cast<size_t>(op.orig_agg),
                                              r, r_idx, &dv, &is_null));
                  if (!is_null) [[likely]] {
                    const double prev = group.RawDoubles()[i];
                    const double next = prev + dv;
                    if (std::isinf(next) && !std::isinf(prev) &&
                        !std::isinf(dv)) {
                      group.RawInfFlags()[i] = 1;
                    }
                    group.RawDoubles()[i] = next;
                    group.RawCounts()[i] += 1;
                  }
                } else if (!op.case_else_null) {
                  group.RawDoubles()[i] = group.RawDoubles()[i] + 0.0;
                  group.RawCounts()[i] += 1;
                }
                break;
              }
              default:
                break;
            }
          }
          return Status::kSuccess;
        }
      }
      if (!materialize_current_left_row(r)) {
        return Status::kSuccess;
      }
      ++local.comparisons;
      if (keys.use_preassigned_build_groups) {
        return callbacks.accumulate_preassigned_pair(w, scratch, left_row,
                                                     r_idx);
      }
      size_t cached_group = kNoCachedGroup;
      ASSIGN_OR_RETURN(
          bool, accumulated,
          ProcessPair(local, left_row, r_idx, scratch, &cached_group));
      (void)accumulated;
      return Status::kSuccess;
    };
    auto eval_int_peek = [](BinaryOperation op, int64_t a,
                            int64_t b) noexcept -> bool {
      switch (op) {
        case BinaryOperation::kEquals:
          return a == b;
        case BinaryOperation::kNotEquals:
          return a != b;
        case BinaryOperation::kLessThan:
          return a < b;
        case BinaryOperation::kLessThanEquals:
          return a <= b;
        case BinaryOperation::kGreaterThan:
          return a > b;
        case BinaryOperation::kGreaterThanEquals:
          return a >= b;
        default:
          return false;
      }
    };
    if (direct.scalar_fact_charge) {
      DirectGidWorker& dw = direct.direct_gid_workers[w];
      const std::vector<slot_t> scalar_mat_cols =
          BuildDeferredProbeMatCols(*source.deferred_left_probe);
      LSTableStore::LazyBlockScratch lazy_scratch;
      while (true) {
        const size_t bi = next_block();
        if (bi >= total_blocks) {
          return Status::kSuccess;
        }
        const LSTablePageBlock& meta_blk = store.blocks[bi];
        if (meta_blk.row_count == 0 ||
            !meta_blk.MatchesZoneMaps(peek_ptr, active_rf_group, full_rf_col)) {
          continue;
        }
        lazy_scratch.transient.clear();
        const LSTablePageBlock* mat =
            store.MaterializeBlockView(bi, &lazy_scratch, scalar_mat_cols);
        if (mat == nullptr) {
          return Status::kIOError;
        }
        const LSTablePageBlock& blk = *mat;
        for (size_t c = 0; c < base_proj_cols; ++c) {
          const LSTableColumnBlock& cblk =
              blk.columns[source.deferred_left_probe->projection[c]];
          proj_cols[c] = &cblk;
          proj_int_ptrs[c] = cblk.int_cells.data();
          proj_has_null[c] = static_cast<uint8_t>(cblk.has_null);
        }
        const LSTableColumnBlock* const rf_col =
            full_rf_col.has_value() ? &blk.columns[*full_rf_col] : nullptr;
        struct ScalarExtraRf {
          const LSTableColumnBlock* col{nullptr};
          const uint64_t* dense_bits{nullptr};
          int64_t lo{0};
          int64_t hi{0};
          bool has_null{false};
          // ScanRuntimeFilter's contract (common/runtime_filter.hpp): a
          // value whose decoded type differs from the filter's domain is
          // never rejected. int_cells only describes INT64/DATE blocks, so
          // the row-level reads below must be skipped for anything else
          // instead of comparing incomparable bit patterns.
          bool type_ok{true};
          const RuntimeFilterGroup* group{nullptr};
        };
        std::array<ScalarExtraRf, 4> extra_rfs{};
        size_t extra_rf_count = 0;
        for (const ScanRuntimeFilter& ext : primary_rf.extra) {
          if (!ext.enabled() || ext.column >= base_proj_cols ||
              extra_rf_count >= extra_rfs.size()) {
            continue;
          }
          const LSTableColumnBlock* ecol = proj_cols[ext.column];
          const bool hd = ext.group->has_dense_bits();
          extra_rfs[extra_rf_count++] = ScalarExtraRf{
              .col = ecol,
              .dense_bits = hd ? ext.group->dense_bits_data() : nullptr,
              .lo = hd ? ext.group->Lo() : 0,
              .hi = hd ? ext.group->Hi() : 0,
              .has_null = ecol->has_null,
              .type_ok = (ecol->type == ext.type),
              .group = ext.group.get(),
          };
        }
        for (uint32_t r = 0; r < blk.row_count; ++r) {
          if (std::ranges::any_of(source.deferred_left_probe->null_peeks,
                                  [&](const NullPeekCompare& test) {
                                    return blk.columns[test.column].IsNull(r) !=
                                           test.expect_null;
                                  })) {
            continue;
          }
          if (rf_col != nullptr) {
            if (rf_col->has_null && rf_col->IsNull(r)) {
              continue;
            }
            const int64_t kv = rf_col->int_cells[r];
            if (rf_has_dense) {
              const uint64_t off =
                  static_cast<uint64_t>(kv) - static_cast<uint64_t>(rf_lo);
              if (kv < rf_lo || kv > rf_hi ||
                  (rf_dense_bits[off >> 6] & (uint64_t{1} << (off & 63))) ==
                      0) {
                continue;
              }
            } else if (active_rf_group->Reject(kv)) {
              continue;
            }
          }
          bool extra_ok = true;
          for (size_t ei = 0; ei < extra_rf_count; ++ei) {
            const ScalarExtraRf& erf = extra_rfs[ei];
            if (erf.has_null && erf.col->IsNull(r)) {
              extra_ok = false;
              break;
            }
            if (!erf.type_ok) {
              continue;
            }
            const int64_t kv = erf.col->int_cells[r];
            if (erf.dense_bits != nullptr) {
              if (kv < erf.lo || kv > erf.hi) {
                extra_ok = false;
                break;
              }
              const uint64_t off =
                  static_cast<uint64_t>(kv) - static_cast<uint64_t>(erf.lo);
              if ((erf.dense_bits[off >> 6] & (uint64_t{1} << (off & 63))) ==
                  0) {
                extra_ok = false;
                break;
              }
            } else if (erf.group->Reject(kv)) {
              extra_ok = false;
              break;
            }
          }
          if (!extra_ok) {
            continue;
          }
          if (peek_ptr != nullptr) {
            bool peek_ok = true;
            for (const IntegerPeekCompare& pred : *peek_ptr) {
              const LSTableColumnBlock& pcol = blk.columns[pred.column];
              if ((pcol.has_null && pcol.IsNull(r)) ||
                  !eval_int_peek(pred.op, pcol.int_cells[r], pred.constant)) {
                peek_ok = false;
                break;
              }
            }
            if (!peek_ok) {
              continue;
            }
          }
          ++local.comparisons;
          charge_numeric(
              dw, 0, 0,
              [&](int32_t col, double* out) {
                const auto c = static_cast<size_t>(col);
                if (proj_has_null[c] != 0 && proj_cols[c]->IsNull(r)) {
                  return false;
                }
                const int64_t raw = proj_int_ptrs[c][r];
                if (source.def_base_types[c] == ValueType::kDouble) {
                  *out = std::bit_cast<double>(raw);
                } else {
                  *out = static_cast<double>(raw);
                }
                return true;
              },
              [&](int32_t col, int64_t* out) {
                const auto c = static_cast<size_t>(col);
                if (proj_has_null[c] != 0 && proj_cols[c]->IsNull(r)) {
                  return false;
                }
                *out = proj_int_ptrs[c][r];
                return true;
              },
              [&](int32_t col) {
                const auto c = static_cast<size_t>(col);
                return proj_has_null[c] == 0 || !proj_cols[c]->IsNull(r);
              });
        }
      }
    }
    const slot_t jk0 = source.left_columns[0];
    const slot_t jk1 = keys.packed_2x32_int_key ? source.left_columns[1] : 0;
    const int64_t off0 = source.left_key_offsets[0];
    const int64_t off1 =
        keys.packed_2x32_int_key ? source.left_key_offsets[1] : 0;
    bool charge_from_cells = deferred.left_numeric_probe &&
                             source.prepared_pk_lookups.empty() &&
                             keys.use_dense_int_rows;
    if (charge_from_cells) {
      for (size_t ai = 0; ai < direct.agg_count; ++ai) {
        const LeftNumericAgg& op = deferred.left_numeric[ai];
        if ((op.col0 >= 0 &&
             (std::cmp_greater_equal(op.col0, base_proj_cols) ||
              source.def_overwritten_by_pk[static_cast<size_t>(op.col0)] !=
                  0)) ||
            (op.col1 >= 0 &&
             (std::cmp_greater_equal(op.col1, base_proj_cols) ||
              source.def_overwritten_by_pk[static_cast<size_t>(op.col1)] !=
                  0))) {
          charge_from_cells = false;
          break;
        }
      }
    }
    // The cache owns contiguous numeric columns as well as page views.
    // A monotonic unique-build probe can scan those columns without doing
    // projection and zone-map setup for every small heap page. NULL-bearing
    // columns and unsupported expressions retain the page-aware path.
    const auto flat_column = [&](size_t c) -> const int64_t* {
      const auto sc = static_cast<slot_t>(c);
      const auto col = store.IntegerColumn(sc);
      if (!store.HasColumn(sc) || c >= store.column_has_null.size() ||
          store.column_has_null[c] || col.size() != store.total_rows) {
        return nullptr;
      }
      return col.data();
    };
    const size_t full_jk = source.deferred_left_probe->projection[jk0];
    bool flat_ready =
        charge_from_cells && direct.single_double_sum && source.integer_key &&
        !keys.ranked_int_bits.empty() && off0 == 0 &&
        source.deferred_left_probe->null_peeks.empty() &&
        full_jk < store.column_is_monotonic.size() &&
        store.column_is_monotonic[full_jk] != 0 &&
        (deferred.left_numeric[0].op == 1 || deferred.left_numeric[0].op == 2 ||
         deferred.left_numeric[0].op == 6);
    const int64_t* flat_keys = flat_ready ? flat_column(full_jk) : nullptr;
    const int64_t* flat_a = nullptr;
    const int64_t* flat_b = nullptr;
    struct FlatPredicate {
      const int64_t* cells;
      BinaryOperation op;
      int64_t value;
    };
    struct FlatFilter {
      const int64_t* cells;
      const RuntimeFilterGroup* group;
    };
    std::vector<FlatPredicate> flat_predicates;
    std::vector<FlatFilter> flat_filters;
    if (flat_ready) {
      const auto& op = deferred.left_numeric[0];
      flat_a = flat_column(source.deferred_left_probe->projection[op.col0]);
      if (op.op != 2) {
        flat_b = flat_column(source.deferred_left_probe->projection[op.col1]);
      }
      flat_ready = flat_keys != nullptr && flat_a != nullptr &&
                   (op.op == 2 || flat_b != nullptr);
      if (peek_ptr != nullptr) {
        for (const auto& pred : *peek_ptr) {
          const auto* cells = flat_column(pred.column);
          flat_ready = flat_ready && cells != nullptr;
          flat_predicates.push_back({cells, pred.op, pred.constant});
        }
      }
      const auto add_filter = [&](const ScanRuntimeFilter& filter) {
        if (!filter.enabled() || filter.column >= base_proj_cols) {
          return;
        }
        const auto* cells =
            flat_column(source.deferred_left_probe->projection[filter.column]);
        flat_ready = flat_ready && cells != nullptr;
        flat_filters.push_back({cells, filter.group.get()});
      };
      add_filter(primary_rf);
      for (const auto& filter : primary_rf.extra) {
        add_filter(filter);
      }
    }
    if (flat_ready) {
      DirectGidWorker& dw =
          direct.direct_gid_workers[deferred.disjoint_direct_groups ? 0 : w];
      const auto& op = deferred.left_numeric[0];
      const bool a_double =
          source.def_base_types[op.col0] == ValueType::kDouble;
      const bool b_double =
          op.op != 2 && source.def_base_types[op.col1] == ValueType::kDouble;
      const auto add_row = [&](uint32_t gid, size_t r) {
        ++local.comparisons;
        dw.matched[gid] = 1;
        const double a = a_double ? std::bit_cast<double>(flat_a[r])
                                  : static_cast<double>(flat_a[r]);
        const double b = op.op == 2
                             ? 0
                             : (b_double ? std::bit_cast<double>(flat_b[r])
                                         : static_cast<double>(flat_b[r]));
        add_dw_double(
            dw, gid,
            op.op == 2 ? a : (op.op == 1 ? a * b : a * (op.scale - b)));
      };
      const bool single_greater =
          flat_predicates.size() == 1 &&
          flat_predicates[0].op == BinaryOperation::kGreaterThan &&
          std::ranges::all_of(flat_filters, [&](const auto& filter) {
            return filter.cells == flat_keys;
          });
      const auto consume_range = [&](int64_t key, uint32_t gid, size_t begin,
                                     size_t end) {
        if (gid == kInvalidGid) {
          return;
        }
        for (const auto& filter : flat_filters) {
          if (filter.cells == flat_keys && filter.group->Reject(key)) {
            return;
          }
        }
        // Lean proof: lean/TinylambOpt/TpchSf10VectorizedExec.lean
        //   (finite_sum_implies_no_inf, dense_group_block_accum_monoid_eq)
        // A single range predicate over a non-NULL flat column needs one
        // comparison per candidate, accumulating into a register across the
        // entire same-gid run and writing to `dw` once per run.
        if (single_greater) {
          const auto* cells = flat_predicates[0].cells;
          const int64_t threshold = flat_predicates[0].value;
          const uint8_t op_code = op.op;
          const double scale = op.scale;
          double run_sum = 0.0;
          uint32_t run_hits = 0;
          if (op_code == 6 && a_double && b_double) {
            for (size_t r = begin; r < end; ++r) {
              if (cells[r] > threshold) {
                const auto a = std::bit_cast<double>(flat_a[r]);
                const auto b = std::bit_cast<double>(flat_b[r]);
                run_sum += a * (scale - b);
                ++run_hits;
              }
            }
          } else if (op_code == 1 && a_double && b_double) {
            for (size_t r = begin; r < end; ++r) {
              if (cells[r] > threshold) {
                const auto a = std::bit_cast<double>(flat_a[r]);
                const auto b = std::bit_cast<double>(flat_b[r]);
                run_sum += a * b;
                ++run_hits;
              }
            }
          } else if (op_code == 2 && a_double) {
            for (size_t r = begin; r < end; ++r) {
              if (cells[r] > threshold) {
                run_sum += std::bit_cast<double>(flat_a[r]);
                ++run_hits;
              }
            }
          } else {
            for (size_t r = begin; r < end; ++r) {
              if (cells[r] > threshold) {
                add_row(gid, r);
              }
            }
            return;
          }
          if (run_hits > 0) {
            local.comparisons += run_hits;
            dw.matched[gid] = 1;
            if (std::isfinite(run_sum)) [[likely]] {
              const double prev = dw.doubles[gid];
              const double next = prev + run_sum;
              dw.doubles[gid] = next;
              if (std::isinf(next) && !std::isinf(prev)) [[unlikely]] {
                dw.inf_flags[gid] = 1;
              }
              dw.counts[gid] += run_hits;
            } else {
              // Slow exact fallback if any row produced ±inf or NaN.
              for (size_t r = begin; r < end; ++r) {
                if (cells[r] > threshold) {
                  const auto a = std::bit_cast<double>(flat_a[r]);
                  const double b =
                      op_code == 2 ? 0.0 : std::bit_cast<double>(flat_b[r]);
                  add_dw_double(dw, gid,
                                op_code == 2
                                    ? a
                                    : (op_code == 1 ? a * b : a * (scale - b)));
                }
              }
            }
          }
          return;
        }
        for (size_t r = begin; r < end; ++r) {
          bool selected = true;
          for (const auto& pred : flat_predicates) {
            if (!eval_int_peek(pred.op, pred.cells[r], pred.value)) {
              selected = false;
              break;
            }
          }
          if (selected) {
            for (const auto& filter : flat_filters) {
              if (filter.cells != flat_keys &&
                  filter.group->Reject(filter.cells[r])) {
                selected = false;
                break;
              }
            }
          }
          if (selected) {
            add_row(gid, r);
          }
        }
      };
      if (deferred.flat_run_index) {
        struct PendingRun {
          int64_t key;
          uint32_t gid;
          uint32_t begin;
          uint32_t end;
        };
        std::array<PendingRun, 128> pending{};
        size_t pending_count = 0;
        const auto flush_runs = [&]() {
          // Separate index reads and fact reads so several independent
          // cache misses can be in flight instead of one per matching key.
          for (size_t i = 0; i < pending_count; ++i) {
            const auto off =
                static_cast<uint64_t>(pending[i].key) -
                static_cast<uint64_t>(deferred.flat_run_index->minimum);
            if (deferred.flat_run_bounds &&
                off < deferred.flat_run_bounds->packed.size()) {
              __builtin_prefetch(deferred.flat_run_bounds->packed.data() + off,
                                 0, 1);
            } else if (off < deferred.flat_run_index->offsets.size() - 1) {
              __builtin_prefetch(deferred.flat_run_index->offsets.data() + off,
                                 0, 1);
              __builtin_prefetch(
                  deferred.flat_run_index->offsets.data() + off + 1, 0, 1);
            }
          }
          for (size_t i = 0; i < pending_count; ++i) {
            auto& run = pending[i];
            if (deferred.flat_run_bounds &&
                !deferred.flat_run_bounds->MayExceed(
                    run.key, flat_predicates[0].value)) {
              run.begin = run.end = 0;
              continue;
            }
            std::tie(run.begin, run.end) =
                deferred.flat_run_index->Find(run.key);
            if (run.begin == run.end) {
              continue;
            }
            for (const auto& pred : flat_predicates) {
              __builtin_prefetch(pred.cells + run.begin, 0, 1);
              if (run.end - run.begin > 8) {
                __builtin_prefetch(pred.cells + run.begin + 8, 0, 1);
              }
            }
            if (flat_a != nullptr) {
              __builtin_prefetch(flat_a + run.begin, 0, 1);
            }
            if (flat_b != nullptr) {
              __builtin_prefetch(flat_b + run.begin, 0, 1);
            }
            for (const auto& filter : flat_filters) {
              if (filter.cells != flat_keys) {
                __builtin_prefetch(filter.cells + run.begin, 0, 1);
              }
            }
          }
          for (size_t i = 0; i < pending_count; ++i) {
            const auto& run = pending[i];
            if (run.begin != run.end) {
              consume_range(run.key, run.gid, run.begin, run.end);
            }
          }
          pending_count = 0;
        };
        if (keys.key_cache_charge) {
          for (size_t ci = w; ci < keys.key_cache.size();
               ci += source.workers) {
            for (const auto& entry : keys.key_cache[ci]) {
              pending[pending_count++] = {.key = entry.key,
                                          .gid = gid_of(entry.row),
                                          .begin = 0,
                                          .end = 0};
              if (pending_count == pending.size()) {
                flush_runs();
              }
            }
          }
        } else {
          const size_t first_word =
              keys.ranked_int_bits.size() * w / source.workers;
          const size_t last_word =
              keys.ranked_int_bits.size() * (w + 1) / source.workers;
          for (size_t wi = first_word; wi < last_word; ++wi) {
            uint64_t bits = keys.ranked_int_bits[wi];
            uint32_t rank = keys.ranked_int_prefix[wi];
            while (bits != 0) {
              const int64_t key =
                  keys.dense_int_min +
                  static_cast<int64_t>((wi * 64) + std::countr_zero(bits));
              const uint32_t gid = gid_of(keys.ranked_int_rows[rank++]);
              pending[pending_count++] = {
                  .key = key, .gid = gid, .begin = 0, .end = 0};
              if (pending_count == pending.size()) {
                flush_runs();
              }
              bits &= bits - 1;
            }
          }
        }
        flush_runs();
        return Status::kSuccess;
      }
      size_t r = store.total_rows * w / source.workers;
      const size_t end = store.total_rows * (w + 1) / source.workers;
      while (r < end) {
        const int64_t key = flat_keys[r];
        uint64_t idx = static_cast<uint64_t>(key) -
                       static_cast<uint64_t>(keys.dense_int_min);
        if (key >= keys.dense_int_min && idx >= keys.ranked_int_span) {
          break;
        }
        if (key < keys.dense_int_min || (keys.ranked_int_bits[idx >> 6] &
                                         (uint64_t{1} << (idx & 63))) == 0) {
          idx = key < keys.dense_int_min ? 0 : idx + 1;
          size_t wi = idx >> 6;
          if (wi >= keys.ranked_int_bits.size()) {
            break;
          }
          uint64_t bits =
              keys.ranked_int_bits[wi] & (~uint64_t{0} << (idx & 63));
          while (bits == 0 && ++wi < keys.ranked_int_bits.size()) {
            bits = keys.ranked_int_bits[wi];
          }
          if (wi == keys.ranked_int_bits.size()) {
            break;
          }
          const int64_t next_key =
              keys.dense_int_min +
              static_cast<int64_t>((wi * 64) + std::countr_zero(bits));
          size_t lo = r + 1, step = 4;
          while (step < end - lo && flat_keys[lo + step] < next_key) {
            lo += step;
            step = std::min(step * 2, end - lo);
          }
          const size_t hi = lo + std::min(step + 1, end - lo);
          r = static_cast<size_t>(
              std::lower_bound(flat_keys + lo, flat_keys + hi, next_key) -
              flat_keys);
          continue;
        }
        const uint64_t word = keys.ranked_int_bits[idx >> 6];
        const uint64_t lower = (uint64_t{1} << (idx & 63)) - 1;
        const uint32_t rank =
            keys.ranked_int_prefix[idx >> 6] +
            static_cast<uint32_t>(std::popcount(word & lower));
        const uint32_t gid = gid_of(keys.ranked_int_rows[rank]);
        const size_t begin = r;
        do {
          ++r;
        } while (r < end && flat_keys[r] == key);
        consume_range(key, gid, begin, r);
      }
      return Status::kSuccess;
    }
    std::vector<uint32_t> rf_sel_buf;
    std::vector<uint64_t> radix_blk_keys;
    std::vector<double> radix_blk_dvals;
    std::vector<uint8_t> radix_blk_valids;
    std::vector<uint8_t> radix_blk_parts;
    bool rf_dense_all_set = false;
    if (rf_has_dense && rf_hi >= rf_lo) {
      const uint64_t span =
          static_cast<uint64_t>(rf_hi) - static_cast<uint64_t>(rf_lo);
      const auto full_words = static_cast<size_t>(span >> 6);
      if (full_words < rf_dense_words) {
        bool ok = true;
        for (size_t wi = 0; wi < full_words; ++wi) {
          if (rf_dense_bits[wi] != ~uint64_t{0}) {
            ok = false;
            break;
          }
        }
        if (ok) {
          const uint32_t rem_bits = static_cast<uint32_t>(span & 63) + 1U;
          const uint64_t mask = rem_bits == 64U
                                    ? ~uint64_t{0}
                                    : ((uint64_t{1} << rem_bits) - 1ULL);
          if ((rf_dense_bits[full_words] & mask) == mask) {
            rf_dense_all_set = true;
          }
        }
      }
    }
    const bool has_null_peeks = !source.deferred_left_probe->null_peeks.empty();
    const std::vector<slot_t> def_mat_cols =
        BuildDeferredProbeMatCols(*source.deferred_left_probe);
    LSTableStore::LazyBlockScratch lazy_scratch;
    while (true) {
      const size_t bi = next_block();
      if (bi >= total_blocks) {
        return Status::kSuccess;
      }
      const LSTablePageBlock& meta_blk = store.blocks[bi];
      if (meta_blk.row_count == 0) {
        continue;
      }
      if (!meta_blk.MatchesZoneMaps(peek_ptr, active_rf_group, full_rf_col)) {
        continue;
      }
      bool extra_zm_reject = false;
      for (const ScanRuntimeFilter& ext : primary_rf.extra) {
        if (!ext.enabled() || ext.column >= base_proj_cols) {
          continue;
        }
        if (ext.group->RejectsAll()) {
          extra_zm_reject = true;
          break;
        }
        const slot_t ext_full =
            source.deferred_left_probe->projection[ext.column];
        if (ext_full < meta_blk.columns.size()) {
          const LSTableColumnBlock& mcol = meta_blk.columns[ext_full];
          // RejectRange speaks the int64 key domain; a block column of
          // any other type must not feed it (same guard as the sister
          // delayed-scan loop and RuntimeFilterGroup::Reject).
          if ((mcol.type == ValueType::kInt64 ||
               mcol.type == ValueType::kDate) &&
              mcol.type == ext.type &&
              (!mcol.has_min_max ||
               ext.group->RejectRange(mcol.min_int, mcol.max_int))) {
            extra_zm_reject = true;
            break;
          }
        }
      }
      if (extra_zm_reject) {
        continue;
      }
      // A dense unique build key occupies [dense_int_min, dense_int_min +
      // span). A block whose join key is entirely outside that range cannot
      // match an inner probe, including NULL keys (NULL never matches).
      if (keys.use_dense_int_rows && source.integer_key &&
          !keys.packed_2x32_int_key && !keys.dense_int_idxs.empty() &&
          jk0 < base_proj_cols && source.left_key_offsets[0] == 0 &&
          !DenseIntBlockMayMatch(
              meta_blk, source.deferred_left_probe->projection[jk0],
              keys.dense_int_min, keys.dense_int_idxs, kInvalidRightIdx)) {
        continue;
      }
      bool peek_all_pass = true;
      if (peek_ptr != nullptr) {
        for (const IntegerPeekCompare& pred : *peek_ptr) {
          const LSTableColumnBlock& pcol = meta_blk.columns[pred.column];
          if (pcol.has_null || !pcol.has_min_max) {
            peek_all_pass = false;
            break;
          }
          bool all_ok = false;
          switch (pred.op) {
            case BinaryOperation::kEquals:
              all_ok = (pcol.min_int == pred.constant &&
                        pcol.max_int == pred.constant);
              break;
            case BinaryOperation::kNotEquals:
              all_ok = (pred.constant < pcol.min_int ||
                        pred.constant > pcol.max_int);
              break;
            case BinaryOperation::kLessThan:
              all_ok = (pcol.max_int < pred.constant);
              break;
            case BinaryOperation::kLessThanEquals:
              all_ok = (pcol.max_int <= pred.constant);
              break;
            case BinaryOperation::kGreaterThan:
              all_ok = (pcol.min_int > pred.constant);
              break;
            case BinaryOperation::kGreaterThanEquals:
              all_ok = (pcol.min_int >= pred.constant);
              break;
            default:
              break;
          }
          if (!all_ok) {
            peek_all_pass = false;
            break;
          }
        }
      }
      lazy_scratch.transient.clear();
      const LSTablePageBlock* mat =
          store.MaterializeBlockView(bi, &lazy_scratch, def_mat_cols);
      if (mat == nullptr) {
        return Status::kIOError;
      }
      const LSTablePageBlock& blk = *mat;
      for (size_t c = 0; c < base_proj_cols; ++c) {
        const LSTableColumnBlock& cblk =
            blk.columns[source.deferred_left_probe->projection[c]];
        proj_cols[c] = &cblk;
        proj_int_ptrs[c] = cblk.int_cells.data();
        proj_has_null[c] = static_cast<uint8_t>(cblk.has_null);
      }
      const LSTableColumnBlock* const rf_col =
          full_rf_col.has_value() ? &blk.columns[*full_rf_col] : nullptr;
      const bool rf_col_has_null = rf_col != nullptr && rf_col->has_null;
      const bool blk_rf_monotonic = rf_col_monotonic && !rf_col_has_null;
      struct DefExtraRf {
        const LSTableColumnBlock* col{nullptr};
        const uint64_t* dense_bits{nullptr};
        int64_t lo{0};
        int64_t hi{0};
        bool has_null{false};
        // See ScalarExtraRf: the decoded type must match the filter's
        // domain before int_cells is read.
        bool type_ok{true};
        const RuntimeFilterGroup* group{nullptr};
      };
      std::array<DefExtraRf, 4> extra_rfs{};
      size_t extra_rf_count = 0;
      for (const ScanRuntimeFilter& ext : primary_rf.extra) {
        if (!ext.enabled() || ext.column >= base_proj_cols ||
            extra_rf_count >= extra_rfs.size()) {
          continue;
        }
        const LSTableColumnBlock* ecol = proj_cols[ext.column];
        const bool hd = ext.group->has_dense_bits();
        extra_rfs[extra_rf_count++] = DefExtraRf{
            .col = ecol,
            .dense_bits = hd ? ext.group->dense_bits_data() : nullptr,
            .lo = hd ? ext.group->Lo() : 0,
            .hi = hd ? ext.group->Hi() : 0,
            .has_null = ecol->has_null,
            .type_ok = (ecol->type == ext.type),
            .group = ext.group.get(),
        };
      }
      const int64_t* const jk0_ints = proj_int_ptrs[jk0];
      const int64_t* const jk1_ints =
          keys.packed_2x32_int_key ? proj_int_ptrs[jk1] : nullptr;
      const bool jk_has_null =
          proj_has_null[jk0] != 0 ||
          (keys.packed_2x32_int_key && proj_has_null[jk1] != 0);
      const bool fast_1pk_dense =
          (keys.use_dense_int_rows || keys.use_unique_int_table) &&
          !keys.packed_2x32_int_key && !source.bulk_raw_count &&
          (keys.use_dense_int_rows ||
           (!raw_aggregates.count_star_only && !charge_from_cells)) &&
          !(raw_aggregates.count_star_only && direct.use_direct_gid_agg &&
            source.prepared_pk_lookups.empty()) &&
          !charge_from_cells && source.def_pk_groups.size() == 1;
      const size_t split_lcol =
          (fast_1pk_dense && layout.split_has_left_probe &&
           layout.split_left_k < source.key_shapes.size() &&
           source.key_shapes[layout.split_left_k].direct_column >= 0 &&
           std::cmp_less(source.key_shapes[layout.split_left_k].direct_column,
                         source.left_width))
              ? static_cast<size_t>(
                    source.key_shapes[layout.split_left_k].direct_column)
              : base_proj_cols;
      const int8_t split_left_li = split_lcol < 16
                                       ? source.def_col_to_pk_lookup[split_lcol]
                                       : int8_t{-1};
      const bool split_left_from_pk0 =
          fast_1pk_dense && layout.split_has_left_probe && split_left_li >= 0 &&
          source.prepared_pk_lookups[static_cast<size_t>(split_left_li)]
                  .group_idx == 0;
      const bool split_left_direct =
          fast_1pk_dense && layout.split_has_left_probe && split_left_li < 0 &&
          split_lcol < base_proj_cols;
      const bool fast_split_left_ok = !layout.split_has_left_probe ||
                                      split_left_from_pk0 || split_left_direct;
      const bool fast_split_arith_agg =
          fast_1pk_dense && fast_split_left_ok &&
          deferred.def_group_cell_probe && layout.use_split_key_gid &&
          layout.split_key_unpack_only &&
          !deferred.use_dense_split_direct_gid && !deferred.use_radix_agg &&
          !layout.packed_2x32_group_key &&
          raw_aggregates.skip_accumulator_states && direct.agg_count == 1 &&
          deferred.left_numeric[0].op == 9 &&
          deferred.compiled_double_arith[static_cast<size_t>(
                                             deferred.left_numeric[0].orig_agg)]
                  .pattern != CompiledDefDoubleArith::Pattern::kGeneral;
      const bool fast_split_col_agg =
          fast_1pk_dense && fast_split_left_ok &&
          deferred.def_group_cell_probe && layout.use_split_key_gid &&
          layout.split_key_unpack_only &&
          !deferred.use_dense_split_direct_gid && !deferred.use_radix_agg &&
          !layout.packed_2x32_group_key &&
          raw_aggregates.skip_accumulator_states && direct.agg_count == 1 &&
          deferred.left_numeric[0].op == 2;
      const bool fast_split_count_or_distinct =
          fast_1pk_dense && fast_split_left_ok &&
          !deferred.def_group_cell_probe && !deferred.def_compact_split_probe &&
          (raw_aggregates.count_star_only ||
           source.fusion->aggregates.empty()) &&
          !keys.use_preassigned_build_groups && !source.compact_counts &&
          !raw_aggregates.raw_sum_fusion && source.residual_programs.empty() &&
          layout.use_split_key_gid && layout.split_key_unpack_only &&
          !layout.packed_2x32_group_key &&
          source.join_type == JoinType::kInner &&
          !source.fusion->rollup_count_distribution &&
          source.all_direct_group_keys && !source.left.HasSpill();
      const int64_t* const split_lcol_ints =
          split_left_direct ? proj_int_ptrs[split_lcol] : nullptr;
      const LSTableColumnBlock* const split_lcol_blk =
          split_left_direct ? proj_cols[split_lcol] : nullptr;
      const uint8_t split_lcol_has_null =
          split_left_direct ? proj_has_null[split_lcol] : uint8_t{0};
      const ValueType split_lcol_vt = split_left_direct
                                          ? source.def_base_types[split_lcol]
                                          : ValueType::kNull;
      const bool split_lcol_type_ok =
          split_left_direct && (split_lcol_vt == ValueType::kInt64 ||
                                split_lcol_vt == ValueType::kDate);
      bool have_cached_pk_lval = false;
      int64_t cached_pk_key = 0;
      bool cached_pk_lval_ok = !layout.split_has_left_probe;
      uint32_t cached_pk_lval_bits = 0;
      const PreparedDeferredPkGroup* const pk0_ptr =
          fast_1pk_dense ? source.def_pk_groups.data() : nullptr;
      DefPkGroupCursor* const cur0_ptr =
          fast_1pk_dense ? pk_group_cursors.data() : nullptr;
      const LSTableColumnBlock* const pk0_col =
          fast_1pk_dense ? proj_cols[pk0_ptr->pk_col] : nullptr;
      const int64_t* const pk0_ints =
          fast_1pk_dense ? proj_int_ptrs[pk0_ptr->pk_col] : nullptr;
      const uint8_t pk0_has_null =
          fast_1pk_dense ? proj_has_null[pk0_ptr->pk_col] : uint8_t{0};
      const int64_t pk0_min_k = fast_1pk_dense ? pk0_ptr->min_k : 0;
      const int64_t pk0_max_k = fast_1pk_dense ? pk0_ptr->max_k : -1;
      const bool pk0_use_subgid =
          fast_1pk_dense && pk0_ptr->use_subgid && layout.split_uses_pk_subgid;
      const uint8_t pk0_shift =
          fast_1pk_dense ? pk0_ptr->subgid_shift : uint8_t{0};
      const bool fast_dense_split_probe =
          deferred.use_dense_split_direct_gid &&
          !direct.use_shared_direct_gid && !direct.use_direct_gid_agg &&
          (keys.use_dense_int_rows || keys.use_unique_int_table) &&
          !deferred.def_compact_split_probe && !source.def_pk_groups.empty() &&
          source.def_pk_groups.size() <= 3 && direct.agg_count == 1 &&
          (deferred.left_numeric[0].op == 0 ||
           deferred.left_numeric[0].op == 2 ||
           deferred.left_numeric[0].op == 3 ||
           deferred.left_numeric[0].op == 9) &&
          (deferred.left_numeric[0].op != 9 ||
           deferred
                   .compiled_double_arith[static_cast<size_t>(
                       deferred.left_numeric[0].orig_agg)]
                   .pattern != CompiledDefDoubleArith::Pattern::kGeneral) &&
          std::ranges::all_of(source.def_pk_groups, [](const auto& g) {
            return g.dense_ptr != nullptr && g.total_rows > 0 &&
                   g.max_k >= g.min_k && g.dense_size == g.span;
          });
      const bool want_da_arith =
          fast_split_arith_agg ||
          (fast_dense_split_probe && deferred.left_numeric[0].op == 9);
      const bool want_da_col =
          fast_split_col_agg ||
          (fast_dense_split_probe && deferred.left_numeric[0].op == 2);
      const bool want_da_int =
          fast_dense_split_probe && deferred.left_numeric[0].op == 3;
      const size_t dpk_n =
          fast_dense_split_probe ? source.def_pk_groups.size() : 0;
      uint32_t dense_right_stride = 1;
      std::array<uint32_t, 3> dpk_stride{};
      std::array<int64_t, 3> dpk_min{};
      std::array<uint64_t, 3> dpk_span{};
      std::array<const PreparedDeferredPkGroup::DensePkEntry*, 3> dpk_dense{};
      std::array<uint8_t, 3> dpk_use_subgid{};
      std::array<const LSTableColumnBlock*, 3> dpk_col{};
      std::array<const int64_t*, 3> dpk_ints{};
      std::array<uint8_t, 3> dpk_has_null{};
      if (fast_dense_split_probe) {
        for (size_t idx = deferred.dense_pk_group_indices.size(); idx > 0;
             --idx) {
          const size_t gi = deferred.dense_pk_group_indices[idx - 1];
          dpk_stride[gi] = dense_right_stride;
          dense_right_stride *= source.def_pk_groups[gi].subgid_count;
        }
        for (size_t gi = 0; gi < dpk_n; ++gi) {
          const auto& g = source.def_pk_groups[gi];
          dpk_min[gi] = g.min_k;
          dpk_span[gi] = static_cast<uint64_t>(g.dense_size);
          dpk_dense[gi] = g.dense_ptr;
          dpk_use_subgid[gi] = static_cast<uint8_t>(g.use_subgid ? 1 : 0);
          dpk_col[gi] = proj_cols[static_cast<size_t>(g.pk_col)];
          dpk_ints[gi] = proj_int_ptrs[static_cast<size_t>(g.pk_col)];
          dpk_has_null[gi] = proj_has_null[static_cast<size_t>(g.pk_col)];
        }
      }
      DirectGidWorker* const dw_fast_ptr =
          fast_dense_split_probe ? &direct.direct_gid_workers[w] : nullptr;
      DirectDoubleCell* const dw_packed_cells =
          (fast_dense_split_probe && deferred.use_packed_double_cells)
              ? dw_fast_ptr->double_cells.data()
              : nullptr;
      const CompiledDefDoubleArith* const da_plan =
          want_da_arith ? &deferred.compiled_double_arith[static_cast<size_t>(
                              deferred.left_numeric[0].orig_agg)]
                        : nullptr;
      const auto da_pattern = want_da_arith
                                  ? da_plan->pattern
                                  : CompiledDefDoubleArith::Pattern::kGeneral;
      const uint32_t da_c0 =
          want_da_arith
              ? da_plan->fast_cols[0]
              : (want_da_col
                     ? static_cast<uint32_t>(deferred.left_numeric[0].col0)
                     : (want_da_int ? static_cast<uint32_t>(
                                          deferred.left_numeric[0].col0)
                                    : 0U));
      const uint32_t da_c1 = want_da_arith ? da_plan->fast_cols[1] : 0U;
      const uint32_t da_c2 = want_da_arith ? da_plan->fast_cols[2] : 0U;
      const uint32_t da_c3 = want_da_arith ? da_plan->fast_cols[3] : 0U;
      const double da_div = want_da_arith ? da_plan->fast_div : 1.0;
      const int64_t* const da_p0 = (want_da_arith || want_da_col || want_da_int)
                                       ? proj_int_ptrs[da_c0]
                                       : nullptr;
      const int64_t* const da_p1 =
          want_da_arith ? proj_int_ptrs[da_c1] : nullptr;
      const int64_t* const da_p2 =
          want_da_arith ? proj_int_ptrs[da_c2] : nullptr;
      const int64_t* const da_p3 =
          want_da_arith ? proj_int_ptrs[da_c3] : nullptr;
      const bool da_c0_is_double =
          want_da_col && source.def_base_types[da_c0] == ValueType::kDouble;
      const bool da_all_nonnull =
          want_da_arith
              ? ((da_pattern ==
                  CompiledDefDoubleArith::Pattern::kSubSubAddDivConst)
                     ? ((proj_has_null[da_c0] | proj_has_null[da_c1] |
                         proj_has_null[da_c2] | proj_has_null[da_c3]) == 0)
                     : ((proj_has_null[da_c0] | proj_has_null[da_c1]) == 0))
              : ((want_da_col || want_da_int) ? (proj_has_null[da_c0] == 0)
                                              : false);
      const bool use_rf_sel = rf_col != nullptr && rf_has_dense &&
                              !blk_rf_monotonic && rf_hi >= rf_lo &&
                              rf_col->int_cells.size() >= blk.row_count;
      size_t rf_sel_count = blk.row_count;
      if (use_rf_sel) {
        if (rf_sel_buf.size() < blk.row_count) {
          rf_sel_buf.resize(blk.row_count);
        }
        rf_sel_count =
            PrefilterDenseRfBlock(rf_col->int_cells.data(), blk.row_count,
                                  rf_lo, rf_hi, rf_dense_bits, rf_dense_all_set,
                                  rf_col, rf_col_has_null, rf_sel_buf.data());
        if (rf_sel_count == 0) {
          continue;
        }
      }
      const uint32_t* const rf_sel_ptr =
          use_rf_sel ? rf_sel_buf.data() : nullptr;
      const size_t radix_agg_col =
          (deferred.left_numeric.size() > deferred.radix_primary &&
           deferred.left_numeric[deferred.radix_primary].col0 >= 0)
              ? static_cast<size_t>(
                    deferred.left_numeric[deferred.radix_primary].col0)
              : base_proj_cols;
      // Lean proof:
      // lean/TinylambOpt/ResearchProofs/MonoContiguousFactSlice.lean (sound)
      const bool mono_contiguous_rf = blk_rf_monotonic && rf_col != nullptr &&
                                      rf_has_dense && rf_dense_all_set &&
                                      rf_hi >= rf_lo &&
                                      rf_col->int_cells.size() >= blk.row_count;
      const bool radix_rf_ok =
          (!blk_rf_monotonic && (use_rf_sel || rf_col == nullptr)) ||
          mono_contiguous_rf;
      if (!has_null_peeks && extra_rf_count == 0 &&
          (peek_all_pass || peek_ptr == nullptr) && radix_rf_ok &&
          source.join_type == JoinType::kInner && source.integer_key &&
          !keys.packed_2x32_int_key && source.left_key_offsets[0] == 0 &&
          !source.bulk_raw_count && !charge_from_cells &&
          keys.use_dense_int_rows && !keys.dense_int_idxs.empty() &&
          keys.ranked_int_bits.empty() && deferred.def_group_cell_probe &&
          source.residual_programs.empty() &&
          !deferred.def_compact_split_probe && !layout.use_split_key_gid &&
          !deferred.use_dense_split_direct_gid && !direct.use_direct_gid_agg &&
          !direct.use_shared_direct_gid && source.def_pk_groups.empty() &&
          layout.packed_2x32_group_key && deferred.use_radix_agg &&
          direct.single_double_sum && !deferred.uses_double_arith_cell_op &&
          !deferred.radix_tuple_is_int &&
          deferred.radix_primary < deferred.left_numeric.size() &&
          deferred.left_numeric[deferred.radix_primary].op == 2 &&
          jk0 < base_proj_cols && layout.packed_2x32_slot0 < base_proj_cols &&
          layout.packed_2x32_slot1 < base_proj_cols &&
          radix_agg_col < base_proj_cols &&
          source.def_base_types[layout.packed_2x32_slot0] ==
              layout.packed_2x32_type0 &&
          source.def_base_types[layout.packed_2x32_slot1] ==
              layout.packed_2x32_type1 &&
          (source.def_base_types[radix_agg_col] == ValueType::kDouble ||
           source.def_base_types[radix_agg_col] == ValueType::kInt64) &&
          proj_cols[jk0]->int_cells.size() >= blk.row_count &&
          proj_cols[layout.packed_2x32_slot0]->int_cells.size() >=
              blk.row_count &&
          proj_cols[layout.packed_2x32_slot1]->int_cells.size() >=
              blk.row_count &&
          proj_cols[radix_agg_col]->int_cells.size() >= blk.row_count) {
        uint32_t r_begin = 0;
        auto r_end = static_cast<uint32_t>(rf_sel_count);
        if (mono_contiguous_rf) {
          r_end = blk.row_count;
          const int64_t* const rf_cells = rf_col->int_cells.data();
          if (!rf_col->has_min_max || rf_col->min_int < rf_lo) {
            r_begin = static_cast<uint32_t>(
                std::lower_bound(rf_cells, rf_cells + blk.row_count, rf_lo) -
                rf_cells);
          }
          if (!rf_col->has_min_max || rf_col->max_int > rf_hi) {
            r_end = static_cast<uint32_t>(
                std::upper_bound(rf_cells + r_begin, rf_cells + blk.row_count,
                                 rf_hi) -
                rf_cells);
          }
          if (r_begin >= r_end) {
            continue;
          }
        }
        const size_t blk_rows = r_end - r_begin;
        if (radix_blk_keys.size() < blk_rows) {
          radix_blk_keys.resize(blk_rows);
          radix_blk_dvals.resize(blk_rows);
          radix_blk_valids.resize(blk_rows);
          radix_blk_parts.resize(blk_rows);
        }
        const int64_t* const __restrict__ jk_ints = jk0_ints;
        const LSTableColumnBlock* const __restrict__ jk_null_blk =
            proj_has_null[jk0] != 0 ? proj_cols[jk0] : nullptr;
        const size_t s0 = layout.packed_2x32_slot0;
        const size_t s1 = layout.packed_2x32_slot1;
        const int64_t* const __restrict__ k0_ints = proj_int_ptrs[s0];
        const LSTableColumnBlock* const __restrict__ k0_null_blk =
            proj_has_null[s0] != 0 ? proj_cols[s0] : nullptr;
        const int64_t* const __restrict__ k1_ints = proj_int_ptrs[s1];
        const LSTableColumnBlock* const __restrict__ k1_null_blk =
            proj_has_null[s1] != 0 ? proj_cols[s1] : nullptr;
        const int64_t* const __restrict__ agg_ints =
            proj_int_ptrs[radix_agg_col];
        const LSTableColumnBlock* const __restrict__ agg_null_blk =
            proj_has_null[radix_agg_col] != 0 ? proj_cols[radix_agg_col]
                                              : nullptr;
        const bool agg_is_double =
            (source.def_base_types[radix_agg_col] == ValueType::kDouble);
        const uint32_t* const __restrict__ dense_ptr =
            keys.dense_int_idxs.data();
        const uint64_t dense_sz = keys.dense_int_idxs.size();
        const int64_t dense_min = keys.dense_int_min;
        RadixSoABucket* const __restrict__ buckets_w =
            deferred.worker_radix_buckets[w].data();
        constexpr int64_t kI32Min = std::numeric_limits<int32_t>::min();
        constexpr int64_t kI32Max = std::numeric_limits<int32_t>::max();
        uint64_t* const __restrict__ bk = radix_blk_keys.data();
        double* const __restrict__ bd = radix_blk_dvals.data();
        uint8_t* const __restrict__ bv = radix_blk_valids.data();
        uint8_t* const __restrict__ bp = radix_blk_parts.data();
        std::array<uint32_t, kRadixParts> pcnt{};
        size_t m = 0;
        for (uint32_t ri = r_begin; ri < r_end; ++ri) {
          const uint32_t r = use_rf_sel ? rf_sel_ptr[ri] : ri;
          if (jk_null_blk != nullptr && jk_null_blk->IsNull(r)) [[unlikely]] {
            continue;
          }
          const uint64_t idx = static_cast<uint64_t>(jk_ints[r]) -
                               static_cast<uint64_t>(dense_min);
          if (idx >= dense_sz) {
            continue;
          }
          const uint32_t r_idx = dense_ptr[idx];
          if (r_idx == kInvalidRightIdx) {
            continue;
          }
          const int64_t v0 = k0_ints[r];
          const int64_t v1 = k1_ints[r];
          if ((k0_null_blk != nullptr && k0_null_blk->IsNull(r)) ||
              (k1_null_blk != nullptr && k1_null_blk->IsNull(r)) ||
              v0 < kI32Min || v0 > kI32Max || v1 < kI32Min || v1 > kI32Max)
              [[unlikely]] {
            RETURN_IF_FAIL(process_def_match(r, r_idx));
            continue;
          }
          const uint64_t ukey = (static_cast<uint64_t>(static_cast<uint32_t>(
                                     static_cast<int32_t>(v0)))
                                 << 32) |
                                static_cast<uint32_t>(static_cast<int32_t>(v1));
          double dv = 0.0;
          uint8_t valid = 1;
          if (agg_null_blk != nullptr && agg_null_blk->IsNull(r)) [[unlikely]] {
            valid = 0;
          } else {
            const int64_t raw = agg_ints[r];
            dv = agg_is_double ? std::bit_cast<double>(raw)
                               : static_cast<double>(raw);
          }
          const auto part = static_cast<uint8_t>(FusedRadixBucket(ukey));
          bk[m] = ukey;
          bd[m] = dv;
          bv[m] = valid;
          bp[m] = part;
          ++pcnt[part];
          ++m;
        }
        if (m > 0) {
          std::array<uint64_t*, kRadixParts> dst_k{};
          std::array<double*, kRadixParts> dst_d{};
          std::array<uint8_t*, kRadixParts> dst_v{};
          std::array<uint32_t, kRadixParts> ppos{};
          for (size_t p = 0; p < kRadixParts; ++p) {
            if (pcnt[p] == 0) {
              continue;
            }
            const size_t old_n = buckets_w[p].keys.size();
            const size_t new_n = old_n + pcnt[p];
            buckets_w[p].keys.resize(new_n);
            buckets_w[p].dvals.resize(new_n);
            buckets_w[p].valids.resize(new_n);
            dst_k[p] = buckets_w[p].keys.data() + old_n;
            dst_d[p] = buckets_w[p].dvals.data() + old_n;
            dst_v[p] = buckets_w[p].valids.data() + old_n;
          }
          for (size_t i = 0; i < m; ++i) {
            const uint8_t p = bp[i];
            const uint32_t pos = ppos[p]++;
            dst_k[p][pos] = bk[i];
            dst_d[p][pos] = bd[i];
            dst_v[p][pos] = bv[i];
          }
          local.comparisons += m;
        }
        continue;
      }
      for (uint32_t ri = 0; ri < rf_sel_count; ++ri) {
        uint32_t r = use_rf_sel ? rf_sel_ptr[ri] : ri;
        if (has_null_peeks &&
            std::ranges::any_of(source.deferred_left_probe->null_peeks,
                                [&](const NullPeekCompare& test) {
                                  return blk.columns[test.column].IsNull(r) !=
                                         test.expect_null;
                                })) {
          continue;
        }
        if (!use_rf_sel && rf_col != nullptr) {
          if (rf_col_has_null && rf_col->IsNull(r)) [[unlikely]] {
            continue;
          }
          const int64_t kv = rf_col->int_cells[r];
          if (rf_has_dense) {
            bool accepted = false;
            if (kv >= rf_lo && kv <= rf_hi) {
              const uint64_t off =
                  static_cast<uint64_t>(kv) - static_cast<uint64_t>(rf_lo);
              accepted =
                  (rf_dense_bits[off >> 6] & (uint64_t{1} << (off & 63))) != 0;
            }
            if (!accepted) {
              if (blk_rf_monotonic) {
                if (kv > rf_hi) {
                  break;
                }
                int64_t next_valid_key = rf_lo;
                if (kv >= rf_lo) {
                  const uint64_t next_off = static_cast<uint64_t>(kv) -
                                            static_cast<uint64_t>(rf_lo) + 1ULL;
                  auto w_idx = static_cast<size_t>(next_off >> 6);
                  if (w_idx >= rf_dense_words) {
                    break;
                  }
                  uint64_t word =
                      rf_dense_bits[w_idx] & (~0ULL << (next_off & 63));
                  while (word == 0) {
                    if (++w_idx >= rf_dense_words) {
                      break;
                    }
                    word = rf_dense_bits[w_idx];
                  }
                  if (w_idx >= rf_dense_words) {
                    break;
                  }
                  const uint64_t found_off =
                      (static_cast<uint64_t>(w_idx) << 6) +
                      static_cast<uint64_t>(std::countr_zero(word));
                  next_valid_key = rf_lo + static_cast<int64_t>(found_off);
                  if (next_valid_key > rf_hi) {
                    break;
                  }
                }
                if (rf_col->has_min_max && next_valid_key > rf_col->max_int) {
                  break;
                }
                const uint32_t row_count = blk.row_count;
                const int64_t* const cells = rf_col->int_cells.data();
                if (r + 1 < row_count && cells[r + 1] < next_valid_key) {
                  uint32_t lo = r + 1;
                  uint32_t step = 4;
                  while (lo + step < row_count &&
                         cells[lo + step] < next_valid_key) {
                    lo += step;
                    step <<= 1;
                  }
                  const uint32_t hi = std::min(row_count, lo + step + 1);
                  const int64_t* found =
                      std::lower_bound(cells + lo, cells + hi, next_valid_key);
                  ri = static_cast<uint32_t>(found - cells) - 1;
                }
              }
              continue;
            }
          } else if (active_rf_group->Reject(kv)) {
            continue;
          }
        }
        if (extra_rf_count != 0) {
          bool extra_ok = true;
          for (size_t ei = 0; ei < extra_rf_count; ++ei) {
            const DefExtraRf& erf = extra_rfs[ei];
            if (erf.has_null && erf.col->IsNull(r)) [[unlikely]] {
              extra_ok = false;
              break;
            }
            if (!erf.type_ok) {
              continue;
            }
            const int64_t kv = erf.col->int_cells[r];
            if (erf.dense_bits != nullptr) {
              if (kv < erf.lo || kv > erf.hi) {
                extra_ok = false;
                break;
              }
              const uint64_t off =
                  static_cast<uint64_t>(kv) - static_cast<uint64_t>(erf.lo);
              if ((erf.dense_bits[off >> 6] & (uint64_t{1} << (off & 63))) ==
                  0) {
                extra_ok = false;
                break;
              }
            } else if (erf.group->Reject(kv)) {
              extra_ok = false;
              break;
            }
          }
          if (!extra_ok) {
            continue;
          }
        }
        if (!peek_all_pass && peek_ptr != nullptr) {
          bool peek_ok = true;
          for (const IntegerPeekCompare& pred : *peek_ptr) {
            const LSTableColumnBlock& pcol = blk.columns[pred.column];
            if ((pcol.has_null && pcol.IsNull(r)) ||
                !eval_int_peek(pred.op, pcol.int_cells[r], pred.constant)) {
              peek_ok = false;
              break;
            }
          }
          if (!peek_ok) {
            continue;
          }
        }
        if (jk_has_null) [[unlikely]] {
          if ((proj_has_null[jk0] != 0 && proj_cols[jk0]->IsNull(r)) ||
              (keys.packed_2x32_int_key && proj_has_null[jk1] != 0 &&
               proj_cols[jk1]->IsNull(r))) {
            continue;
          }
        }
        int64_t key = 0;
        if (!keys.packed_2x32_int_key) {
          key = AddJoinKeyBias(jk0_ints[r], off0);
        } else {
          const int64_t k0 = AddJoinKeyBias(jk0_ints[r], off0);
          const int64_t k1 = AddJoinKeyBias(jk1_ints[r], off1);
          if (!FitsFusedInt32(k0) || !FitsFusedInt32(k1)) [[unlikely]] {
            continue;
          }
          key = PackFusedInt32Pair(k0, k1);
        }
        if (source.bulk_raw_count) {
          const BulkCountValue* bulk = nullptr;
          if (keys.use_dense_bulk) {
            const uint64_t idx = static_cast<uint64_t>(key) -
                                 static_cast<uint64_t>(keys.dense_bulk_min);
            if (idx < keys.dense_bulk_counts.size() &&
                keys.dense_bulk_counts[idx].r_idx != kInvalidRightIdx) {
              bulk = &keys.dense_bulk_counts[idx];
            }
          } else {
            bulk = keys.bulk_counts->Find(key);
          }
          if (bulk == nullptr || !materialize_current_left_row(r)) {
            continue;
          }
          local.comparisons += bulk->pairs;
          const Value& bulk_cell = left_row.values_[source.int_group_slot];
          size_t* bulk_slot = nullptr;
          bool new_group = false;
          size_t group_index = 0;
          if (!bulk_cell.IsNull() && bulk_cell.type == source.int_group_type &&
              !bulk_cell.IsUnsigned()) {
            bool inserted = false;
            bulk_slot = local.int_offsets.TryEmplace(bulk_cell.value.int_value,
                                                     &inserted);
            if (!inserted) {
              group_index = *bulk_slot;
            } else {
              local.key_scratch.assign(1, bulk_cell);
              new_group = true;
            }
          } else {
            local.key_scratch.clear();
            local.key_scratch.push_back(CanonicalDistinctValue(bulk_cell));
            if (auto found = local.offsets.find(local.key_scratch);
                found != local.offsets.end()) {
              group_index = found->second;
            } else {
              new_group = true;
            }
          }
          if (new_group) {
            group_index = local.groups.size();
            FusedGroup group;
            local.owned_rep_left.push_back(left_row);
            group.rep_left = &local.owned_rep_left.back();
            group.rep_right_idx = bulk->r_idx;
            if (bulk_slot != nullptr) {
              if (!source.all_direct_group_keys) {
                group.key = Row(local.key_scratch);
              }
              local.groups.push_back(std::move(group));
              *bulk_slot = group_index;
            } else {
              // T4639/T4640: building two Rows from key_scratch paid two
              // full key materializations (array + per-Value string copies)
              // per new group. Move the scratch into one key, copy that
              // into the offsets map, and move it into group.key -- same
              // double ownership, half the allocations.
              Row local_key(std::move(local.key_scratch));
              local.offsets.emplace(Row(local_key), group_index);
              group.key = std::move(local_key);
              local.groups.push_back(std::move(group));
            }
          }
          local.groups[group_index].raw_count += bulk->count;
          continue;
        }
        if (keys.use_dense_int_rows || keys.use_unique_int_table) {
          uint32_t r_idx = kInvalidRightIdx;
          if (keys.use_dense_int_rows) {
            const uint64_t idx = static_cast<uint64_t>(key) -
                                 static_cast<uint64_t>(keys.dense_int_min);
            if (!keys.ranked_int_bits.empty()) {
              if (idx < keys.ranked_int_span) {
                const uint64_t word = keys.ranked_int_bits[idx >> 6];
                const uint64_t bit = uint64_t{1} << (idx & 63);
                if ((word & bit) != 0) {
                  const uint32_t rank =
                      keys.ranked_int_prefix[idx >> 6] +
                      static_cast<uint32_t>(std::popcount(word & (bit - 1)));
                  r_idx = keys.ranked_int_rows[rank];
                }
              }
            } else if (idx < keys.dense_int_idxs.size()) {
              r_idx = keys.dense_int_idxs[idx];
            }
          } else {
            size_t slot = HashUniqueIntKey(key) & keys.unique_int_mask;
            while (keys.unique_int_table[slot].r_idx != kInvalidRightIdx) {
              if (keys.unique_int_table[slot].key == key) [[likely]] {
                r_idx = keys.unique_int_table[slot].r_idx;
                break;
              }
              slot = (slot + 1) & keys.unique_int_mask;
            }
          }
          if (r_idx == kInvalidRightIdx) {
            continue;
          }
          // COUNT(*) and existence-only fusions never read a fact cell.
          // The build row already carries the group, so a matched gid is
          // the whole update.
          if (keys.use_dense_int_rows && raw_aggregates.count_star_only &&
              direct.use_direct_gid_agg && source.prepared_pk_lookups.empty()) {
            const uint32_t gid = gid_of(r_idx);
            if (gid != kInvalidGid) {
              DirectGidWorker& dw =
                  direct
                      .direct_gid_workers[direct.use_shared_direct_gid ? 0 : w];
              ++local.comparisons;
              if (direct.use_shared_direct_gid) {
                __atomic_store_n(&dw.matched[gid], 1, __ATOMIC_RELAXED);
                __atomic_fetch_add(&dw.counts[gid], 1, __ATOMIC_RELAXED);
              } else {
                dw.matched[gid] = 1;
                dw.counts[gid] += 1;
              }
              continue;
            }
          }
          if (keys.use_dense_int_rows && charge_from_cells) {
            const uint32_t gid = gid_of(r_idx);
            if (gid != kInvalidGid) {
              ++local.comparisons;
              DirectGidWorker& dw = direct.direct_gid_workers[w];
              charge_numeric(
                  dw, gid, r_idx,
                  [&](int32_t col, double* out) {
                    const auto c = static_cast<size_t>(col);
                    if (proj_has_null[c] != 0 && proj_cols[c]->IsNull(r)) {
                      return false;
                    }
                    const int64_t raw = proj_int_ptrs[c][r];
                    if (source.def_base_types[c] == ValueType::kDouble) {
                      *out = std::bit_cast<double>(raw);
                    } else {
                      *out = static_cast<double>(raw);
                    }
                    return true;
                  },
                  [&](int32_t col, int64_t* out) {
                    const auto c = static_cast<size_t>(col);
                    if (proj_has_null[c] != 0 && proj_cols[c]->IsNull(r)) {
                      return false;
                    }
                    *out = proj_int_ptrs[c][r];
                    return true;
                  },
                  [&](int32_t col) {
                    const auto c = static_cast<size_t>(col);
                    return proj_has_null[c] == 0 || !proj_cols[c]->IsNull(r);
                  });
              continue;
            }
          }
          if (fast_dense_split_probe) {
            const uint32_t r_subgid =
                !layout.split_uses_right_subgid
                    ? 0U
                    : layout.split_right_subgid_ptr[r_idx];
            if (r_subgid == std::numeric_limits<uint32_t>::max()) [[unlikely]] {
              continue;
            }
            uint32_t gid = layout.split_uses_right_subgid
                               ? (r_subgid * dense_right_stride)
                               : 0U;
            bool pk_ok = true;
            for (size_t gi = 0; gi < dpk_n; ++gi) {
              if (dpk_has_null[gi] != 0 && dpk_col[gi]->IsNull(r))
                  [[unlikely]] {
                pk_ok = false;
                break;
              }
              const uint64_t off = static_cast<uint64_t>(dpk_ints[gi][r]) -
                                   static_cast<uint64_t>(dpk_min[gi]);
              if (off >= dpk_span[gi]) [[unlikely]] {
                pk_ok = false;
                break;
              }
              const auto& entry = dpk_dense[gi][off];
              if (entry.nb == std::numeric_limits<uint32_t>::max())
                  [[unlikely]] {
                pk_ok = false;
                break;
              }
              if (dpk_use_subgid[gi] != 0) {
                if (entry.subgid == std::numeric_limits<uint32_t>::max())
                    [[unlikely]] {
                  pk_ok = false;
                  break;
                }
                gid += entry.subgid * dpk_stride[gi];
              }
            }
            if (!pk_ok) {
              continue;
            }
            ++local.comparisons;
            if (deferred.use_packed_double_cells) {
              DirectDoubleCell& cell = dw_packed_cells[gid];
              double dv = 0.0;
              bool is_null = false;
              if (want_da_col) {
                if (!da_all_nonnull && proj_cols[da_c0]->IsNull(r))
                    [[unlikely]] {
                  is_null = true;
                } else {
                  const int64_t raw = da_p0[r];
                  dv = da_c0_is_double ? std::bit_cast<double>(raw)
                                       : static_cast<double>(raw);
                }
              } else {
                if (!da_all_nonnull) {
                  if ((proj_has_null[da_c0] != 0 &&
                       proj_cols[da_c0]->IsNull(r)) ||
                      (proj_has_null[da_c1] != 0 &&
                       proj_cols[da_c1]->IsNull(r)) ||
                      (da_pattern == CompiledDefDoubleArith::Pattern::
                                         kSubSubAddDivConst &&
                       ((proj_has_null[da_c2] != 0 &&
                         proj_cols[da_c2]->IsNull(r)) ||
                        (proj_has_null[da_c3] != 0 &&
                         proj_cols[da_c3]->IsNull(r))))) {
                    is_null = true;
                  }
                }
                if (!is_null) [[likely]] {
                  const auto v0 = std::bit_cast<double>(da_p0[r]);
                  const auto v1 = std::bit_cast<double>(da_p1[r]);
                  switch (da_pattern) {
                    case CompiledDefDoubleArith::Pattern::kSubSubAddDivConst:
                      dv = (((v0 - v1) - std::bit_cast<double>(da_p2[r])) +
                            std::bit_cast<double>(da_p3[r])) /
                           da_div;
                      break;
                    case CompiledDefDoubleArith::Pattern::kSubDivConst:
                      dv = (v0 - v1) / da_div;
                      break;
                    case CompiledDefDoubleArith::Pattern::kSub:
                      dv = v0 - v1;
                      break;
                    case CompiledDefDoubleArith::Pattern::kAdd:
                      dv = v0 + v1;
                      break;
                    default:
                      break;
                  }
                }
              }
              if (is_null) [[unlikely]] {
                cell.meta |= 1ULL;
              } else {
                const double prev = cell.sum;
                const double next = prev + dv;
                uint64_t meta = (cell.meta + 4ULL) | 1ULL;
                if (std::isinf(next) && !std::isinf(prev) && !std::isinf(dv))
                    [[unlikely]] {
                  meta |= 2ULL;
                }
                cell.sum = next;
                cell.meta = meta;
              }
            } else if (deferred.left_numeric[0].op == 0) {
              dw_fast_ptr->matched[gid] = 1;
              dw_fast_ptr->counts[gid] += 1;
            } else {
              dw_fast_ptr->matched[gid] = 1;
              if (da_all_nonnull || !proj_cols[da_c0]->IsNull(r)) [[likely]] {
                const int64_t iv = da_p0[r];
                int64_t next = 0;
                if (__builtin_add_overflow(dw_fast_ptr->ints[gid], iv, &next)) {
                  dw_fast_ptr->inf_flags[gid] = 1;
                } else {
                  dw_fast_ptr->ints[gid] = next;
                }
                dw_fast_ptr->counts[gid] += 1;
              }
            }
            continue;
          }
          if (fast_1pk_dense &&
              (deferred.def_compact_split_probe || fast_split_arith_agg ||
               fast_split_col_agg || fast_split_count_or_distinct)) {
            if (pk0_has_null != 0 && pk0_col->IsNull(r)) [[unlikely]] {
              local.have_cur_pk_subgids = false;
              continue;
            }
            const int64_t pk_k = pk0_ints[r];
            if (pk_k < pk0_min_k || pk_k > pk0_max_k) [[unlikely]] {
              local.have_cur_pk_subgids = false;
              continue;
            }
            if (!cur0_ptr->have_last || pk_k != cur0_ptr->last_key)
                [[unlikely]] {
              if (!resolve_def_pk_groups(r, false)) [[unlikely]] {
                have_cached_pk_lval = false;
                continue;
              }
              if (split_left_from_pk0) {
                int64_t lv = 0;
                ValueType ltype = ValueType::kNull;
                bool luns = false;
                cached_pk_lval_ok =
                    read_def_left_raw(split_lcol, r, &lv, &ltype, &luns) &&
                    !luns &&
                    (ltype == ValueType::kInt64 || ltype == ValueType::kDate) &&
                    lv >= static_cast<int64_t>(
                              std::numeric_limits<int32_t>::min()) &&
                    lv <= static_cast<int64_t>(
                              std::numeric_limits<int32_t>::max());
                cached_pk_lval_bits =
                    cached_pk_lval_ok
                        ? static_cast<uint32_t>(static_cast<int32_t>(lv))
                        : 0U;
                cached_pk_key = pk_k;
                have_cached_pk_lval = true;
              }
            } else {
              if (!cur0_ptr->last_matched) [[unlikely]] {
                local.have_cur_pk_subgids = false;
                continue;
              }
              if (pk0_ptr->use_subgid) {
                local.cur_pk_subgids[0] = cur0_ptr->last_subgid;
              }
              local.have_cur_pk_subgids = true;
              if (split_left_from_pk0 && (!have_cached_pk_lval ||
                                          cached_pk_key != pk_k)) [[unlikely]] {
                int64_t lv = 0;
                ValueType ltype = ValueType::kNull;
                bool luns = false;
                cached_pk_lval_ok =
                    read_def_left_raw(split_lcol, r, &lv, &ltype, &luns) &&
                    !luns &&
                    (ltype == ValueType::kInt64 || ltype == ValueType::kDate) &&
                    lv >= static_cast<int64_t>(
                              std::numeric_limits<int32_t>::min()) &&
                    lv <= static_cast<int64_t>(
                              std::numeric_limits<int32_t>::max());
                cached_pk_lval_bits =
                    cached_pk_lval_ok
                        ? static_cast<uint32_t>(static_cast<int32_t>(lv))
                        : 0U;
                cached_pk_key = pk_k;
                have_cached_pk_lval = true;
              }
            }
            const uint32_t r_subgid =
                !layout.split_uses_right_subgid
                    ? 0U
                    : layout.split_right_subgid_ptr[r_idx];
            if (r_subgid == std::numeric_limits<uint32_t>::max()) [[unlikely]] {
              if (deferred.def_compact_split_probe) {
                continue;
              }
              RETURN_IF_FAIL(process_def_match(r, r_idx));
              continue;
            }
            bool lval_ok = !layout.split_has_left_probe || cached_pk_lval_ok;
            uint32_t lval_bits = split_left_from_pk0 ? cached_pk_lval_bits : 0U;
            if (split_left_direct) {
              if (!split_lcol_type_ok ||
                  (split_lcol_has_null != 0 && split_lcol_blk->IsNull(r)))
                  [[unlikely]] {
                lval_ok = false;
              } else {
                const int64_t lv = split_lcol_ints[r];
                lval_ok = lv >= static_cast<int64_t>(
                                    std::numeric_limits<int32_t>::min()) &&
                          lv <= static_cast<int64_t>(
                                    std::numeric_limits<int32_t>::max());
                lval_bits = static_cast<uint32_t>(static_cast<int32_t>(lv));
              }
            }
            if (!lval_ok) [[unlikely]] {
              RETURN_IF_FAIL(process_def_match(r, r_idx));
              continue;
            }
            uint64_t packed_u64 = layout.split_uses_right_subgid
                                      ? (static_cast<uint64_t>(r_subgid)
                                         << layout.split_right_shift)
                                      : 0ULL;
            if (pk0_use_subgid) {
              const uint32_t psg = local.cur_pk_subgids[0];
              if (psg == std::numeric_limits<uint32_t>::max()) [[unlikely]] {
                if (deferred.def_compact_split_probe) {
                  continue;
                }
                RETURN_IF_FAIL(process_def_match(r, r_idx));
                continue;
              }
              packed_u64 |= static_cast<uint64_t>(psg) << pk0_shift;
            }
            if (layout.split_has_left_probe) {
              packed_u64 |= static_cast<uint64_t>(lval_bits);
            }
            ++local.comparisons;
            if (deferred.def_compact_split_probe) {
              if (source.compact_log_allowed &&
                  (local.compact_count_inputs == 1024 ||
                   local.compact_count_inputs == 16384)) [[unlikely]] {
                RETURN_IF_FAIL(
                    callbacks.record_compact_split(local, packed_u64));
              } else if (local.compact_count_log) {
                ++local.compact_count_inputs;
                uint64_t h = packed_u64 + 0x9e3779b97f4a7c15ULL;
                h = (h ^ (h >> 30U)) * 0xbf58476d1ce4e5b9ULL;
                h = (h ^ (h >> 27U)) * 0x94d049bb133111ebULL;
                h ^= h >> 31U;
                local.compact_split_shards[h & (kCompactSplitShards - 1)]
                    .push_back(packed_u64);
              } else {
                ++local.compact_count_inputs;
                bool inserted = false;
                size_t* slot = local.int_offsets.TryEmplace(
                    std::bit_cast<int64_t>(packed_u64), &inserted);
                if (!inserted) [[likely]] {
                  ++local.compact_counts[*slot].count;
                } else {
                  CompactCountGroup probe;
                  probe.hash = packed_u64;
                  probe.count = 1;
                  *slot = local.compact_counts.size();
                  local.compact_counts.push_back(probe);
                }
              }
              continue;
            }
            bool inserted = false;
            size_t* slot = local.int_offsets.TryEmplace(
                std::bit_cast<int64_t>(packed_u64), &inserted);
            if (fast_split_count_or_distinct) {
              if (!inserted) [[likely]] {
                if (raw_aggregates.count_star_only) {
                  ++local.groups[*slot].raw_count;
                }
              } else {
                const size_t group_index = local.groups.size();
                FusedGroup group;
                group.key_hash = packed_u64;
                group.has_key_hash = true;
                group.rep_right_idx = r_idx;
                group.raw_count = raw_aggregates.count_star_only ? 1 : 0;
                local.groups.push_back(std::move(group));
                *slot = group_index;
              }
              continue;
            }
            size_t group_index = 0;
            if (!inserted) [[likely]] {
              group_index = *slot;
            } else {
              group_index = local.groups.size();
              FusedGroup group;
              local.raw_arena.Initialize(group, direct.agg_count);
              group.key_hash = packed_u64;
              group.has_key_hash = true;
              group.rep_right_idx = r_idx;
              local.groups.push_back(std::move(group));
              *slot = group_index;
            }
            FusedGroup& group = local.groups[group_index];
            double dv = 0.0;
            bool is_null = false;
            if (fast_split_arith_agg) {
              if (!da_all_nonnull) {
                if ((proj_has_null[da_c0] != 0 &&
                     proj_cols[da_c0]->IsNull(r)) ||
                    (proj_has_null[da_c1] != 0 &&
                     proj_cols[da_c1]->IsNull(r)) ||
                    (da_pattern ==
                         CompiledDefDoubleArith::Pattern::kSubSubAddDivConst &&
                     ((proj_has_null[da_c2] != 0 &&
                       proj_cols[da_c2]->IsNull(r)) ||
                      (proj_has_null[da_c3] != 0 &&
                       proj_cols[da_c3]->IsNull(r))))) {
                  is_null = true;
                }
              }
              if (!is_null) [[likely]] {
                const auto v0 = std::bit_cast<double>(da_p0[r]);
                const auto v1 = std::bit_cast<double>(da_p1[r]);
                switch (da_pattern) {
                  case CompiledDefDoubleArith::Pattern::kSubSubAddDivConst:
                    dv = (((v0 - v1) - std::bit_cast<double>(da_p2[r])) +
                          std::bit_cast<double>(da_p3[r])) /
                         da_div;
                    break;
                  case CompiledDefDoubleArith::Pattern::kSubDivConst:
                    dv = (v0 - v1) / da_div;
                    break;
                  case CompiledDefDoubleArith::Pattern::kSub:
                    dv = v0 - v1;
                    break;
                  case CompiledDefDoubleArith::Pattern::kAdd:
                    dv = v0 + v1;
                    break;
                  default:
                    break;
                }
              }
            } else {
              if (!da_all_nonnull && proj_cols[da_c0]->IsNull(r)) [[unlikely]] {
                is_null = true;
              } else {
                const int64_t raw = da_p0[r];
                dv = da_c0_is_double ? std::bit_cast<double>(raw)
                                     : static_cast<double>(raw);
              }
            }
            if (!is_null) [[likely]] {
              const double prev = group.RawDoubles()[0];
              const double next = prev + dv;
              if (std::isinf(next) && !std::isinf(prev) && !std::isinf(dv))
                  [[unlikely]] {
                group.RawInfFlags()[0] = 1;
              }
              group.RawDoubles()[0] = next;
              group.RawCounts()[0] += 1;
            }
            continue;
          }
          RETURN_IF_FAIL(process_def_match(r, r_idx));
          continue;
        }
        const auto [first, last] = keys.int_buckets->equal_range(key);
        if (first == last) {
          continue;
        }
        if (deferred.def_group_cell_probe || deferred.def_compact_split_probe) {
          for (auto iter = first; iter != last; ++iter) {
            RETURN_IF_FAIL(process_def_match(r, iter->second));
          }
        } else {
          if (!materialize_current_left_row(r)) {
            continue;
          }
          if (keys.use_preassigned_build_groups) {
            for (auto iter = first; iter != last; ++iter) {
              ++local.comparisons;
              RETURN_IF_FAIL(callbacks.accumulate_preassigned_pair(
                  w, scratch, left_row, iter->second));
            }
          } else {
            size_t cached_group = kNoCachedGroup;
            for (auto iter = first; iter != last; ++iter) {
              ++local.comparisons;
              ASSIGN_OR_RETURN(bool, accumulated,
                               ProcessPair(local, left_row, iter->second,
                                           scratch, &cached_group));
              (void)accumulated;
            }
          }
        }
      }
    }
  }
  if (source.left.HasColumnarOnly()) {
    const auto& chunks = source.left.columnar->chunks;
    const size_t n_col_morsels = source.col_morsels.size();
    const bool preassigned_no_left_row =
        keys.use_preassigned_build_groups &&
        !direct.use_partitioned_count_distinct && !direct.ordered_sum_charge &&
        (source.fusion->aggregates.empty() || raw_aggregates.count_star_only);
    Row col_left_row;
    if (source.singleton_right_relation) {
      for (size_t mi = w; mi < n_col_morsels; mi += source.workers) {
        const ColProbeMorsel mor = source.col_morsels[mi];
        const ColumnarChunk& ch = chunks[mor.ci];
        for (size_t r = mor.r_begin; r < mor.r_end; ++r) {
          if (!preassigned_no_left_row) {
            ch.MaterializeRowInto(r, &col_left_row);
          }
          ++local.comparisons;
          if (keys.use_preassigned_build_groups) {
            RETURN_IF_FAIL(callbacks.accumulate_preassigned_pair(
                w, scratch, col_left_row, uint32_t{0}));
          } else {
            size_t cached_group = kNoCachedGroup;
            ASSIGN_OR_RETURN(bool, accumulated,
                             ProcessPair(local, col_left_row, uint32_t{0},
                                         scratch, &cached_group));
            (void)accumulated;
          }
        }
      }
      return Status::kSuccess;
    }
    const bool col_raw_fast =
        !keys.use_preassigned_build_groups && !source.compact_counts &&
        source.residual_programs.empty() && raw_aggregates.raw_sum_fusion &&
        !raw_aggregates.has_count_distinct && source.all_direct_group_keys &&
        deferred.left_cell_agg_ready && !deferred.uses_double_arith_cell_op &&
        !source.right_ptr->HasSpill() &&
        (layout.use_left_packed_key ||
         (layout.use_split_key_gid && !layout.split_uses_pk_subgid &&
          (!layout.split_has_left_probe ||
           source.key_shapes[layout.split_left_k].direct_column >= 0)) ||
         layout.packed_2x32_group_key || source.int_group_key);
    const bool col_raw_unmatched_ok = col_raw_fast &&
                                      !layout.left_pack_uses_right &&
                                      !layout.split_uses_right_subgid;
    const auto col_read_raw_int =
        [&](slot_t slot_col, const ColumnarChunk& ch, size_t r, uint32_t r_idx,
            int64_t* out, ValueType* out_t, bool* out_uns) noexcept -> bool {
      if (slot_col < source.left_width) {
        const ColumnarColumn& cc = ch.columns[slot_col];
        if (cc.IsNull(r)) [[unlikely]] {
          return false;
        }
        if (cc.is_uniform) [[likely]] {
          *out_t = cc.type;
          *out_uns = cc.is_unsigned;
          *out = cc.IntAt(r);
          return true;
        }
        Value tmp;
        cc.WriteValueInto(r, &tmp);
        if (tmp.IsNull()) {
          return false;
        }
        *out_t = tmp.type;
        *out_uns = tmp.IsUnsigned();
        *out = tmp.value.int_value;
        return true;
      }
      if (r_idx == kInvalidRightIdx) {
        return false;
      }
      Value rv_tmp;
      const Value& rv =
          source.read_right_cell(r_idx, slot_col - source.left_width, &rv_tmp);
      if (rv.IsNull()) [[unlikely]] {
        return false;
      }
      *out_t = rv.type;
      *out_uns = rv.IsUnsigned();
      *out = rv.value.int_value;
      return true;
    };
    const auto process_col_raw_match = [&](size_t ci, const ColumnarChunk& ch,
                                           size_t r, uint32_t r_idx) -> Status {
      bool key_ok = false;
      int64_t lookup_key = 0;
      uint64_t computed_key_hash = 0;
      bool has_computed_key_hash = false;
      bool need_rep_left = true;
      if (layout.use_left_packed_key) {
        uint64_t packed_u64 = 0;
        if (layout.left_pack_uses_right) {
          const uint32_t r_subgid = layout.split_right_subgid_ptr[r_idx];
          packed_u64 |= static_cast<uint64_t>(r_subgid)
                        << layout.split_right_shift;
        }
        if (layout.left_pack_uses_dict) {
          const size_t li = layout.left_pack_chunk_offsets[ci] + r;
          packed_u64 |= static_cast<uint64_t>(layout.left_pack_dict_subgid[li])
                        << layout.left_pack_dict_shift;
        }
        for (const LeftPackedIntCol& lpi : layout.left_pack_ints) {
          const ColumnarColumn& cc = ch.columns[lpi.col];
          uint64_t code = lpi.null_code;
          if (cc.encoding == ColumnarColumn::Encoding::kFlat && cc.is_uniform &&
              cc.ints.size() > r &&
              (!cc.has_null || cc.nulls.empty() || cc.nulls.size() > r)) {
            if (!cc.has_null || cc.nulls.empty() || cc.nulls[r] == 0) {
              code = static_cast<uint64_t>(cc.ints[r]) -
                     static_cast<uint64_t>(lpi.min_val);
            }
          } else if (cc.encoding == ColumnarColumn::Encoding::kGathered) {
            const ColumnarColumn* leaf = nullptr;
            size_t lr = 0;
            if (cc.ResolveLeaf(r, &leaf, &lr)) {
              const int64_t iv = leaf->is_uniform ? leaf->IntAt(lr)
                                                  : ColumnarIntValue(*leaf, lr);
              code = static_cast<uint64_t>(iv) -
                     static_cast<uint64_t>(lpi.min_val);
            }
          } else if (!cc.IsNull(r)) {
            code = static_cast<uint64_t>(
                       cc.is_uniform ? cc.IntAt(r) : ColumnarIntValue(cc, r)) -
                   static_cast<uint64_t>(lpi.min_val);
          }
          packed_u64 |= code << lpi.shift;
        }
        lookup_key = std::bit_cast<int64_t>(packed_u64);
        computed_key_hash = packed_u64;
        has_computed_key_hash = true;
        need_rep_left = !layout.left_pack_unpack_only;
        key_ok = true;
      } else if (layout.use_split_key_gid) {
        const uint32_t r_subgid = !layout.split_uses_right_subgid
                                      ? 0U
                                      : layout.split_right_subgid_ptr[r_idx];
        int64_t l_val = 0;
        bool l_ok = !layout.split_has_left_probe;
        if (layout.split_has_left_probe) {
          const auto& l_shape = source.key_shapes[layout.split_left_k];
          ValueType ltype = ValueType::kNull;
          bool luns = false;
          if (l_shape.direct_column >= 0 &&
              col_read_raw_int(static_cast<slot_t>(l_shape.direct_column), ch,
                               r, r_idx, &l_val, &ltype, &luns) &&
              !luns &&
              (ltype == ValueType::kInt64 || ltype == ValueType::kDate) &&
              std::in_range<int32_t>(l_val)) [[likely]] {
            l_ok = true;
          }
        }
        if (r_subgid != std::numeric_limits<uint32_t>::max() && l_ok)
            [[likely]] {
          uint64_t packed_u64 = 0;
          if (layout.split_uses_right_subgid) {
            packed_u64 |= static_cast<uint64_t>(r_subgid)
                          << layout.split_right_shift;
          }
          if (layout.split_has_left_probe) {
            packed_u64 |= static_cast<uint64_t>(
                static_cast<uint32_t>(static_cast<int32_t>(l_val)));
          }
          lookup_key = std::bit_cast<int64_t>(packed_u64);
          computed_key_hash = packed_u64;
          has_computed_key_hash = true;
          need_rep_left = !layout.split_key_unpack_only;
          key_ok = true;
        }
      } else if (layout.packed_2x32_group_key) {
        int64_t v0 = 0;
        int64_t v1 = 0;
        ValueType t0 = ValueType::kNull;
        ValueType t1 = ValueType::kNull;
        bool u0 = false;
        bool u1 = false;
        if (col_read_raw_int(layout.packed_2x32_slot0, ch, r, r_idx, &v0, &t0,
                             &u0) &&
            col_read_raw_int(layout.packed_2x32_slot1, ch, r, r_idx, &v1, &t1,
                             &u1) &&
            t0 == layout.packed_2x32_type0 && !u0 &&
            t1 == layout.packed_2x32_type1 && !u1 &&
            std::in_range<int32_t>(v0) && std::in_range<int32_t>(v1))
            [[likely]] {
          const uint64_t packed_u64 =
              (static_cast<uint64_t>(
                   static_cast<uint32_t>(static_cast<int32_t>(v0)))
               << 32) |
              static_cast<uint32_t>(static_cast<int32_t>(v1));
          lookup_key = std::bit_cast<int64_t>(packed_u64);
          computed_key_hash = packed_u64;
          has_computed_key_hash = true;
          need_rep_left = !layout.packed_2x32_unpack_only;
          key_ok = true;
        }
      } else {
        ValueType kt = ValueType::kNull;
        bool kuns = false;
        if (col_read_raw_int(source.int_group_slot, ch, r, r_idx, &lookup_key,
                             &kt, &kuns) &&
            kt == source.int_group_type && !kuns) [[likely]] {
          key_ok = true;
        }
      }
      if (!key_ok) [[unlikely]] {
        ch.MaterializeRowInto(r, &col_left_row);
        if (r_idx == kInvalidRightIdx) {
          return callbacks.unmatched_left(local, scratch, col_left_row);
        }
        size_t cached_group = kNoCachedGroup;
        ASSIGN_OR_RETURN(
            bool, accumulated,
            ProcessPair(local, col_left_row, r_idx, scratch, &cached_group));
        (void)accumulated;
        return Status::kSuccess;
      }
      const auto col_agg_double = [&](int32_t col, double* out) {
        const ColumnarColumn& cc = ch.columns[static_cast<size_t>(col)];
        if (cc.IsNull(r)) [[unlikely]] {
          return false;
        }
        if (cc.is_uniform) [[likely]] {
          if (cc.type == ValueType::kDouble) {
            *out = cc.DoubleAt(r);
            return true;
          }
          if (cc.type == ValueType::kInt64 && !cc.is_unsigned) {
            *out = static_cast<double>(cc.IntAt(r));
            return true;
          }
          return false;
        }
        Value tmp;
        cc.WriteValueInto(r, &tmp);
        return !tmp.IsNull() && ReadFusedDouble(tmp, out);
      };
      const auto col_agg_int = [&](int32_t col, int64_t* out) {
        const ColumnarColumn& cc = ch.columns[static_cast<size_t>(col)];
        if (cc.IsNull(r)) [[unlikely]] {
          return false;
        }
        if (cc.is_uniform) [[likely]] {
          if (cc.type != ValueType::kInt64 || cc.is_unsigned) {
            return false;
          }
          *out = cc.IntAt(r);
          return true;
        }
        Value tmp;
        cc.WriteValueInto(r, &tmp);
        if (tmp.IsNull() || tmp.type != ValueType::kInt64 || tmp.IsUnsigned()) {
          return false;
        }
        *out = tmp.value.int_value;
        return true;
      };
      const auto col_agg_present = [&](int32_t col) {
        return !ch.columns[static_cast<size_t>(col)].IsNull(r);
      };
      if (deferred.use_radix_agg) {
        return scatter_radix_tuple(
            w, std::bit_cast<uint64_t>(lookup_key), col_agg_double, col_agg_int,
            col_agg_present,
            [](int32_t, double*, bool*) { return Status::kSuccess; });
      }
      bool inserted = false;
      size_t* slot = local.int_offsets.TryEmplace(lookup_key, &inserted);
      size_t group_index = 0;
      if (!inserted) [[likely]] {
        group_index = *slot;
      } else {
        group_index = local.groups.size();
        FusedGroup group;
        local.raw_arena.Initialize(group, direct.agg_count);
        group.key_hash = computed_key_hash;
        group.has_key_hash = has_computed_key_hash;
        if (need_rep_left) {
          group.rep_col_chunk = static_cast<uint32_t>(ci);
          group.rep_col_row = static_cast<uint32_t>(r);
        }
        group.rep_right_idx = r_idx;
        local.groups.push_back(std::move(group));
        *slot = group_index;
      }
      charge_group_numeric(
          local.groups[group_index],
          r_idx != kInvalidRightIdx ? static_cast<size_t>(r_idx) : size_t{0},
          r_idx != kInvalidRightIdx, col_agg_double, col_agg_int,
          col_agg_present);
      return Status::kSuccess;
    };
    if (source.integer_key || keys.packed_2x32_int_key) {
      if (source.integer_key && raw_aggregates.count_star_only &&
          keys.use_dense_int_rows && keys.use_preassigned_build_groups &&
          source.join_type == JoinType::kInner &&
          !direct.direct_gid_workers.empty()) {
        const slot_t key_col = source.left_columns[0];
        const int64_t bias = source.left_key_offsets[0];
        DirectGidWorker& dw =
            direct.direct_gid_workers[direct.use_shared_direct_gid ? 0 : w];
        const uint32_t* dense = keys.dense_int_idxs.data();
        const size_t dense_n = keys.dense_int_idxs.size();
        const int64_t dense_base = keys.dense_int_min;
        for (size_t mi = w; mi < n_col_morsels; mi += source.workers) {
          const ColProbeMorsel mor = source.col_morsels[mi];
          const ColumnarChunk& ch = chunks[mor.ci];
          const ColumnarColumn& kc = ch.columns[key_col];
          if (kc.encoding == ColumnarColumn::Encoding::kConstant &&
              ch.rows > 0 && kc.IsNull(0)) {
            continue;
          }
          const bool kc_fast =
              kc.is_uniform && !kc.is_unsigned &&
              (kc.type == ValueType::kInt64 || kc.type == ValueType::kDate);
          for (size_t r = mor.r_begin; r < mor.r_end; ++r) {
            if (kc_fast ? kc.IsNull(r) : ColumnarCellIsNullOrNaN(kc, r)) {
              continue;
            }
            const int64_t raw_k =
                kc_fast ? kc.IntAt(r) : ColumnarIntValue(kc, r);
            const int64_t key = AddJoinKeyBias(raw_k, bias);
            const uint64_t idx =
                static_cast<uint64_t>(key) - static_cast<uint64_t>(dense_base);
            if (idx >= dense_n) {
              continue;
            }
            const uint32_t r_idx = dense[idx];
            if (r_idx == kInvalidRightIdx) {
              continue;
            }
            const uint32_t gid = gid_of(r_idx);
            if (gid == kInvalidGid) {
              continue;
            }
            ++local.comparisons;
            if (direct.use_shared_direct_gid) {
              __atomic_store_n(&dw.matched[gid], 1, __ATOMIC_RELAXED);
              __atomic_fetch_add(&dw.counts[gid], 1, __ATOMIC_RELAXED);
            } else {
              dw.matched[gid] = 1;
              dw.counts[gid] += 1;
            }
          }
        }
        return Status::kSuccess;
      }
      if (source.integer_key && deferred.left_numeric_probe &&
          keys.use_dense_int_rows) {
        const slot_t key_col = source.left_columns[0];
        const int64_t bias = source.left_key_offsets[0];
        DirectGidWorker& dw = direct.direct_gid_workers[w];
        const uint32_t* dense = keys.dense_int_idxs.data();
        const size_t dense_n = keys.dense_int_idxs.size();
        const int64_t dense_base = keys.dense_int_min;
        for (size_t mi = w; mi < n_col_morsels; mi += source.workers) {
          const ColProbeMorsel mor = source.col_morsels[mi];
          const ColumnarChunk& ch = chunks[mor.ci];
          const ColumnarColumn& kc = ch.columns[key_col];
          if (kc.encoding == ColumnarColumn::Encoding::kConstant &&
              ch.rows > 0 && kc.IsNull(0)) {
            continue;
          }
          const bool kc_fast =
              kc.is_uniform && !kc.is_unsigned &&
              (kc.type == ValueType::kInt64 || kc.type == ValueType::kDate);
          for (size_t r = mor.r_begin; r < mor.r_end; ++r) {
            if (kc_fast ? kc.IsNull(r) : ColumnarCellIsNullOrNaN(kc, r)) {
              continue;
            }
            const int64_t raw_k =
                kc_fast ? kc.IntAt(r) : ColumnarIntValue(kc, r);
            const int64_t key = AddJoinKeyBias(raw_k, bias);
            const uint64_t idx =
                static_cast<uint64_t>(key) - static_cast<uint64_t>(dense_base);
            if (idx >= dense_n) {
              continue;
            }
            const uint32_t r_idx = dense[idx];
            if (r_idx == kInvalidRightIdx) {
              continue;
            }
            const uint32_t gid = gid_of(r_idx);
            if (gid == kInvalidGid) {
              continue;
            }
            ++local.comparisons;
            charge_numeric(
                dw, gid, r_idx,
                [&](int32_t col, double* out) {
                  const ColumnarColumn& cc =
                      ch.columns[static_cast<size_t>(col)];
                  if (cc.IsNull(r)) {
                    return false;
                  }
                  if (cc.is_uniform) {
                    if (cc.type == ValueType::kDouble) {
                      *out = cc.DoubleAt(r);
                      return true;
                    }
                    if (cc.type == ValueType::kInt64 && !cc.is_unsigned) {
                      *out = static_cast<double>(cc.IntAt(r));
                      return true;
                    }
                    return false;
                  }
                  Value tmp;
                  cc.WriteValueInto(r, &tmp);
                  return !tmp.IsNull() && ReadFusedDouble(tmp, out);
                },
                [&](int32_t col, int64_t* out) {
                  const ColumnarColumn& cc =
                      ch.columns[static_cast<size_t>(col)];
                  if (cc.IsNull(r)) {
                    return false;
                  }
                  if (cc.is_uniform) {
                    if (cc.type != ValueType::kInt64 || cc.is_unsigned) {
                      return false;
                    }
                    *out = cc.IntAt(r);
                    return true;
                  }
                  Value tmp;
                  cc.WriteValueInto(r, &tmp);
                  if (tmp.IsNull() || tmp.type != ValueType::kInt64 ||
                      tmp.IsUnsigned()) {
                    return false;
                  }
                  *out = tmp.value.int_value;
                  return true;
                },
                [&](int32_t col) {
                  return !ch.columns[static_cast<size_t>(col)].IsNull(r);
                });
          }
        }
        return Status::kSuccess;
      }
      for (size_t mi = w; mi < n_col_morsels; mi += source.workers) {
        const ColProbeMorsel mor = source.col_morsels[mi];
        const size_t ci = mor.ci;
        const ColumnarChunk& ch = chunks[ci];
        const ColumnarColumn& kc0 = ch.columns[source.left_columns[0]];
        const ColumnarColumn* kc1 = keys.packed_2x32_int_key
                                        ? &ch.columns[source.left_columns[1]]
                                        : nullptr;
        const bool kc0_fast =
            kc0.is_uniform && !kc0.is_unsigned &&
            (kc0.type == ValueType::kInt64 || kc0.type == ValueType::kDate);
        const bool kc1_fast =
            kc1 != nullptr && kc1->is_uniform && !kc1->is_unsigned &&
            (kc1->type == ValueType::kInt64 || kc1->type == ValueType::kDate);
        for (size_t r = mor.r_begin; r < mor.r_end; ++r) {
          int64_t raw0 = 0, raw1 = 0;
          if (!ReadFusedIntCell(kc0, r, kc0_fast, &raw0) ||
              (kc1 != nullptr && !ReadFusedIntCell(*kc1, r, kc1_fast, &raw1))) {
            if (source.join_type == JoinType::kLeft) {
              if (col_raw_unmatched_ok) {
                RETURN_IF_FAIL(
                    process_col_raw_match(ci, ch, r, kInvalidRightIdx));
              } else {
                ch.MaterializeRowInto(r, &col_left_row);
                RETURN_IF_FAIL(
                    callbacks.unmatched_left(local, scratch, col_left_row));
              }
            }
            continue;
          }
          int64_t key = 0;
          if (source.integer_key) {
            key = AddJoinKeyBias(raw0, source.left_key_offsets[0]);
          } else {
            const int64_t k0 = AddJoinKeyBias(raw0, source.left_key_offsets[0]);
            const int64_t k1 = AddJoinKeyBias(raw1, source.left_key_offsets[1]);
            if (!FitsFusedInt32(k0) || !FitsFusedInt32(k1)) {
              if (source.join_type == JoinType::kLeft) {
                if (col_raw_unmatched_ok) {
                  RETURN_IF_FAIL(
                      process_col_raw_match(ci, ch, r, kInvalidRightIdx));
                } else {
                  ch.MaterializeRowInto(r, &col_left_row);
                  RETURN_IF_FAIL(
                      callbacks.unmatched_left(local, scratch, col_left_row));
                }
              }
              continue;
            }
            key = PackFusedInt32Pair(k0, k1);
          }
          if (source.bulk_raw_count) {
            const BulkCountValue* bulk = nullptr;
            if (keys.use_dense_bulk) {
              const uint64_t idx = static_cast<uint64_t>(key) -
                                   static_cast<uint64_t>(keys.dense_bulk_min);
              if (idx < keys.dense_bulk_counts.size() &&
                  keys.dense_bulk_counts[idx].r_idx != kInvalidRightIdx) {
                bulk = &keys.dense_bulk_counts[idx];
              }
            } else {
              bulk = keys.bulk_counts->Find(key);
            }
            if (bulk == nullptr) {
              if (source.join_type == JoinType::kLeft) {
                ch.MaterializeRowInto(r, &col_left_row);
                RETURN_IF_FAIL(
                    callbacks.unmatched_left(local, scratch, col_left_row));
              }
              continue;
            }
            local.comparisons += bulk->pairs;
            const ColumnarColumn& gcc = ch.columns[source.int_group_slot];
            if (gcc.is_uniform && !gcc.is_unsigned &&
                gcc.type == source.int_group_type && !gcc.IsNull(r)) {
              bool inserted = false;
              size_t* bulk_slot =
                  local.int_offsets.TryEmplace(gcc.IntAt(r), &inserted);
              size_t group_index = 0;
              if (!inserted) {
                group_index = *bulk_slot;
              } else {
                group_index = local.groups.size();
                FusedGroup group;
                group.rep_col_chunk = static_cast<uint32_t>(ci);
                group.rep_col_row = static_cast<uint32_t>(r);
                group.rep_right_idx = bulk->r_idx;
                if (!source.all_direct_group_keys) {
                  Value bulk_k;
                  gcc.WriteValueInto(r, &bulk_k);
                  group.key = Row(std::vector<Value>{std::move(bulk_k)});
                }
                local.groups.push_back(std::move(group));
                *bulk_slot = group_index;
              }
              local.groups[group_index].raw_count += bulk->count;
              continue;
            }
            ch.MaterializeRowInto(r, &col_left_row);
            const Value& bulk_cell =
                col_left_row.values_[source.int_group_slot];
            size_t* bulk_slot = nullptr;
            bool new_group = false;
            size_t group_index = 0;
            if (!bulk_cell.IsNull() &&
                bulk_cell.type == source.int_group_type &&
                !bulk_cell.IsUnsigned()) {
              bool inserted = false;
              bulk_slot = local.int_offsets.TryEmplace(
                  bulk_cell.value.int_value, &inserted);
              if (!inserted) {
                group_index = *bulk_slot;
              } else {
                local.key_scratch.assign(1, bulk_cell);
                new_group = true;
              }
            } else {
              local.key_scratch.clear();
              local.key_scratch.push_back(CanonicalDistinctValue(bulk_cell));
              if (auto found = local.offsets.find(local.key_scratch);
                  found != local.offsets.end()) {
                group_index = found->second;
              } else {
                new_group = true;
              }
            }
            if (new_group) {
              group_index = local.groups.size();
              local.owned_rep_left.push_back(col_left_row);
              FusedGroup group;
              group.rep_left = &local.owned_rep_left.back();
              group.rep_right_idx = bulk->r_idx;
              if (bulk_slot != nullptr) {
                if (!source.all_direct_group_keys) {
                  group.key = Row(local.key_scratch);
                }
                local.groups.push_back(std::move(group));
                *bulk_slot = group_index;
              } else {
                // T4639/T4640: single key construction + one copy into
                // the map (see sibling bulk sites).
                Row local_key(std::move(local.key_scratch));
                local.offsets.emplace(Row(local_key), group_index);
                group.key = std::move(local_key);
                local.groups.push_back(std::move(group));
              }
            }
            local.groups[group_index].raw_count += bulk->count;
            continue;
          }
          if (keys.use_dense_int_rows) {
            const uint64_t idx = static_cast<uint64_t>(key) -
                                 static_cast<uint64_t>(keys.dense_int_min);
            const uint32_t r_idx = idx < keys.dense_int_idxs.size()
                                       ? keys.dense_int_idxs[idx]
                                       : kInvalidRightIdx;
            bool matched = false;
            if (r_idx != kInvalidRightIdx) {
              ++local.comparisons;
              if (source.anti_unmatched_only &&
                  source.residual_programs.empty()) {
                matched = true;
              } else if (col_raw_fast) {
                RETURN_IF_FAIL(process_col_raw_match(ci, ch, r, r_idx));
                matched = true;
              } else {
                if (!preassigned_no_left_row) {
                  ch.MaterializeRowInto(r, &col_left_row);
                }
                if (keys.use_preassigned_build_groups) {
                  RETURN_IF_FAIL(callbacks.accumulate_preassigned_pair(
                      w, scratch, col_left_row, r_idx));
                  matched = true;
                } else {
                  size_t cached_group = kNoCachedGroup;
                  ASSIGN_OR_RETURN(bool, accumulated,
                                   ProcessPair(local, col_left_row, r_idx,
                                               scratch, &cached_group));
                  matched = accumulated;
                }
              }
            }
            if (!matched && source.join_type == JoinType::kLeft) {
              if (col_raw_unmatched_ok) {
                RETURN_IF_FAIL(
                    process_col_raw_match(ci, ch, r, kInvalidRightIdx));
              } else {
                if (r_idx == kInvalidRightIdx || preassigned_no_left_row) {
                  ch.MaterializeRowInto(r, &col_left_row);
                }
                RETURN_IF_FAIL(
                    callbacks.unmatched_left(local, scratch, col_left_row));
              }
            }
            continue;
          }
          if (keys.use_unique_int_table) {
            size_t slot = HashUniqueIntKey(key) & keys.unique_int_mask;
            uint32_t r_idx = kInvalidRightIdx;
            while (true) {
              const UniqueIntSlot& entry = keys.unique_int_table[slot];
              if (entry.r_idx == kInvalidRightIdx) {
                break;
              }
              if (entry.key == key) [[likely]] {
                r_idx = entry.r_idx;
                break;
              }
              slot = (slot + 1) & keys.unique_int_mask;
            }
            bool matched = false;
            if (r_idx != kInvalidRightIdx) [[likely]] {
              ++local.comparisons;
              if (source.anti_unmatched_only &&
                  source.residual_programs.empty()) {
                matched = true;
              } else if (col_raw_fast) {
                RETURN_IF_FAIL(process_col_raw_match(ci, ch, r, r_idx));
                matched = true;
              } else {
                if (!preassigned_no_left_row) {
                  ch.MaterializeRowInto(r, &col_left_row);
                }
                if (keys.use_preassigned_build_groups) {
                  RETURN_IF_FAIL(callbacks.accumulate_preassigned_pair(
                      w, scratch, col_left_row, r_idx));
                  matched = true;
                } else {
                  size_t cached_group = kNoCachedGroup;
                  ASSIGN_OR_RETURN(bool, accumulated,
                                   ProcessPair(local, col_left_row, r_idx,
                                               scratch, &cached_group));
                  matched = accumulated;
                }
              }
            }
            if (!matched && source.join_type == JoinType::kLeft) {
              if (col_raw_unmatched_ok) {
                RETURN_IF_FAIL(
                    process_col_raw_match(ci, ch, r, kInvalidRightIdx));
              } else {
                if (r_idx == kInvalidRightIdx || preassigned_no_left_row) {
                  ch.MaterializeRowInto(r, &col_left_row);
                }
                RETURN_IF_FAIL(
                    callbacks.unmatched_left(local, scratch, col_left_row));
              }
            }
            continue;
          }
          bool matched = false;
          const auto [first, last] = keys.int_buckets->equal_range(key);
          if (first != last) {
            if (source.anti_unmatched_only &&
                source.residual_programs.empty()) {
              ++local.comparisons;
              matched = true;
            } else if (col_raw_fast) {
              for (auto iter = first; iter != last; ++iter) {
                ++local.comparisons;
                RETURN_IF_FAIL(process_col_raw_match(ci, ch, r, iter->second));
              }
              matched = true;
            } else {
              if (!preassigned_no_left_row) {
                ch.MaterializeRowInto(r, &col_left_row);
              }
              size_t cached_group = kNoCachedGroup;
              for (auto iter = first; iter != last; ++iter) {
                ++local.comparisons;
                if (keys.use_preassigned_build_groups) {
                  RETURN_IF_FAIL(callbacks.accumulate_preassigned_pair(
                      w, scratch, col_left_row, iter->second));
                  matched = true;
                } else {
                  ASSIGN_OR_RETURN(
                      bool, accumulated,
                      ProcessPair(local, col_left_row, iter->second, scratch,
                                  &cached_group));
                  matched = matched || accumulated;
                  if (matched && source.anti_unmatched_only) {
                    break;
                  }
                }
              }
            }
          }
          if (!matched && source.join_type == JoinType::kLeft) {
            if (col_raw_unmatched_ok) {
              RETURN_IF_FAIL(
                  process_col_raw_match(ci, ch, r, kInvalidRightIdx));
            } else {
              if (first == last || preassigned_no_left_row) {
                ch.MaterializeRowInto(r, &col_left_row);
              }
              RETURN_IF_FAIL(
                  callbacks.unmatched_left(local, scratch, col_left_row));
            }
          }
        }
      }
      return Status::kSuccess;
    }
    if (keys.use_dense_composite) {
      for (size_t mi = w; mi < n_col_morsels; mi += source.workers) {
        const ColProbeMorsel mor = source.col_morsels[mi];
        const auto& chunk = chunks[mor.ci];
        for (size_t r = mor.r_begin; r < mor.r_end; ++r) {
          CompositeIntKey key;
          uint32_t right_row = kInvalidRightIdx;
          if (ReadFusedCompositeKey(chunk, source.left_columns,
                                    source.left_key_offsets, r, &key)) {
            const uint64_t offset =
                static_cast<uint64_t>(key.parts[0]) -
                static_cast<uint64_t>(keys.dense_composite_min);
            if (offset < keys.dense_composite_rows.size()) {
              const uint32_t candidate = keys.dense_composite_rows[offset];
              if (candidate != kInvalidRightIdx &&
                  keys.dense_composite_keys[candidate] == key) {
                right_row = candidate;
              }
            }
          }
          if (right_row == kInvalidRightIdx &&
              source.join_type == JoinType::kInner) {
            continue;
          }
          if (!preassigned_no_left_row || right_row == kInvalidRightIdx) {
            chunk.MaterializeRowInto(r, &col_left_row);
          }
          bool matched = false;
          if (right_row != kInvalidRightIdx) {
            ++local.comparisons;
            if (keys.use_preassigned_build_groups) {
              RETURN_IF_FAIL(callbacks.accumulate_preassigned_pair(
                  w, scratch, col_left_row, right_row));
              matched = true;
            } else {
              ASSIGN_OR_RETURN(
                  bool, accumulated,
                  ProcessPair(local, col_left_row, right_row, scratch));
              matched = accumulated;
            }
          }
          if (!matched && source.join_type == JoinType::kLeft) {
            if (right_row != kInvalidRightIdx && preassigned_no_left_row) {
              chunk.MaterializeRowInto(r, &col_left_row);
            }
            RETURN_IF_FAIL(
                callbacks.unmatched_left(local, scratch, col_left_row));
          }
        }
      }
      return Status::kSuccess;
    }
    if (source.composite_int_key) {
      const FlatJoinIdxBuckets<CompositeIntKey, CompositeIntKeyHash>& buckets =
          *keys.composite_buckets;
      for (size_t mi = w; mi < n_col_morsels; mi += source.workers) {
        const ColProbeMorsel mor = source.col_morsels[mi];
        const ColumnarChunk& ch = chunks[mor.ci];
        for (size_t r = mor.r_begin; r < mor.r_end; ++r) {
          CompositeIntKey key;
          if (!ReadFusedCompositeKey(ch, source.left_columns,
                                     source.left_key_offsets, r, &key)) {
            if (source.join_type == JoinType::kLeft) {
              ch.MaterializeRowInto(r, &col_left_row);
              RETURN_IF_FAIL(
                  callbacks.unmatched_left(local, scratch, col_left_row));
            }
            continue;
          }
          bool matched = false;
          size_t cached_group = kNoCachedGroup;
          const auto [first, last] = buckets.equal_range(key);
          if (first == last && source.join_type == JoinType::kInner) {
            continue;
          }
          if (!preassigned_no_left_row || first == last) {
            ch.MaterializeRowInto(r, &col_left_row);
          }
          for (auto iter = first; iter != last; ++iter) {
            ++local.comparisons;
            if (keys.use_preassigned_build_groups) {
              RETURN_IF_FAIL(callbacks.accumulate_preassigned_pair(
                  w, scratch, col_left_row, iter->second));
              matched = true;
            } else {
              ASSIGN_OR_RETURN(bool, accumulated,
                               ProcessPair(local, col_left_row, iter->second,
                                           scratch, &cached_group));
              matched = matched || accumulated;
            }
          }
          if (!matched && source.join_type == JoinType::kLeft) {
            if (first != last && preassigned_no_left_row) {
              ch.MaterializeRowInto(r, &col_left_row);
            }
            RETURN_IF_FAIL(
                callbacks.unmatched_left(local, scratch, col_left_row));
          }
        }
      }
      return Status::kSuccess;
    }
    const FlatJoinIdxBuckets<std::string>& buckets = *keys.key_buckets;
    for (size_t mi = w; mi < n_col_morsels; mi += source.workers) {
      const ColProbeMorsel mor = source.col_morsels[mi];
      const ColumnarChunk& ch = chunks[mor.ci];
      for (size_t r = mor.r_begin; r < mor.r_end; ++r) {
        ch.MaterializeRowInto(r, &col_left_row);
        if (HasNullKey(col_left_row, source.left_columns)) {
          if (source.join_type == JoinType::kLeft) {
            RETURN_IF_FAIL(
                callbacks.unmatched_left(local, scratch, col_left_row));
          }
          continue;
        }
        const std::string key = EncodeJoinKey(col_left_row, source.left_columns,
                                              &source.left_key_offsets);
        bool matched = false;
        size_t cached_group = kNoCachedGroup;
        const auto [first, last] = buckets.equal_range(key);
        for (auto iter = first; iter != last; ++iter) {
          ++local.comparisons;
          if (keys.use_preassigned_build_groups) {
            RETURN_IF_FAIL(callbacks.accumulate_preassigned_pair(
                w, scratch, col_left_row, iter->second));
            matched = true;
          } else {
            ASSIGN_OR_RETURN(bool, accumulated,
                             ProcessPair(local, col_left_row, iter->second,
                                         scratch, &cached_group));
            matched = matched || accumulated;
          }
        }
        if (!matched && source.join_type == JoinType::kLeft) {
          RETURN_IF_FAIL(
              callbacks.unmatched_left(local, scratch, col_left_row));
        }
      }
    }
    return Status::kSuccess;
  }
  if (source.left.HasSpill()) {
    // Spill readers own one row at a time. Feed it directly into groups;
    // neither the probe nor its potentially huge join output is copied.
    const auto probe_row = [&](const Row& left_row) -> Status {
      bool matched = false;
      const auto visit = [&](const auto& buckets, const auto& key) -> Status {
        const auto [first, last] = buckets.equal_range(key);
        for (auto iter = first; iter != last; ++iter) {
          ++local.comparisons;
          ASSIGN_OR_RETURN(bool, accumulated,
                           ProcessPair(local, left_row, iter->second, scratch));
          matched = matched || accumulated;
        }
        return Status::kSuccess;
      };
      if (!HasNullKey(left_row, source.left_columns)) {
        if (source.integer_key) {
          RETURN_IF_FAIL(visit(
              *keys.int_buckets,
              AddJoinKeyBias(IntegerJoinKey(left_row, source.left_columns[0]),
                             source.left_key_offsets[0])));
        } else if (keys.packed_2x32_int_key) {
          const int64_t k0 =
              AddJoinKeyBias(IntegerJoinKey(left_row, source.left_columns[0]),
                             source.left_key_offsets[0]);
          const int64_t k1 =
              AddJoinKeyBias(IntegerJoinKey(left_row, source.left_columns[1]),
                             source.left_key_offsets[1]);
          if (FitsFusedInt32(k0) && FitsFusedInt32(k1)) {
            RETURN_IF_FAIL(
                visit(*keys.int_buckets, PackFusedInt32Pair(k0, k1)));
          }
        } else if (source.composite_int_key) {
          RETURN_IF_FAIL(
              visit(*keys.composite_buckets,
                    EncodeCompositeIntKey(left_row, source.left_columns,
                                          &source.left_key_offsets)));
        } else {
          RETURN_IF_FAIL(visit(*keys.key_buckets,
                               EncodeJoinKey(left_row, source.left_columns,
                                             &source.left_key_offsets)));
        }
      }
      if (!matched && source.join_type == JoinType::kLeft) {
        RETURN_IF_FAIL(callbacks.unmatched_left(local, scratch, left_row));
      }
      return Status::kSuccess;
    };
    Status evaluation = Status::kSuccess;
    RETURN_IF_FAIL(source.left.ForEachRow([&](const Row& row) {
      if (evaluation.ok()) {
        evaluation = probe_row(row);
      }
    }));
    return evaluation;
  }
  if (source.singleton_right_relation) {
    for (size_t chunk = w; chunk < source.chunk_count;
         chunk += source.workers) {
      const size_t begin = chunk * source.probe_chunk_rows;
      const size_t end =
          std::min(source.total_left, begin + source.probe_chunk_rows);
      for (size_t ri = begin; ri < end; ++ri) {
        const Row& left_row = source.left.rows[ri];
        ++local.comparisons;
        if (keys.use_preassigned_build_groups) {
          RETURN_IF_FAIL(callbacks.accumulate_preassigned_pair(
              w, scratch, left_row, uint32_t{0}));
        } else {
          size_t cached_group = kNoCachedGroup;
          ASSIGN_OR_RETURN(bool, accumulated,
                           ProcessPair(local, left_row, uint32_t{0}, scratch,
                                       &cached_group));
          (void)accumulated;
        }
      }
    }
    return Status::kSuccess;
  }
  if (source.integer_key || keys.packed_2x32_int_key) {
    // COUNT(*) with a dense unique build key only needs the foreign key.
    // The group id was assigned on the build row, so the fact row's other
    // cells and the scratch copy are not read.
    if (source.integer_key && raw_aggregates.count_star_only &&
        keys.use_dense_int_rows && keys.use_preassigned_build_groups &&
        source.join_type == JoinType::kInner && !source.left.HasSpill() &&
        !direct.direct_gid_workers.empty()) {
      const slot_t key_col = source.left_columns[0];
      const int64_t bias = source.left_key_offsets[0];
      DirectGidWorker& dw =
          direct.direct_gid_workers[direct.use_shared_direct_gid ? 0 : w];
      const uint32_t* dense = keys.dense_int_idxs.data();
      const size_t dense_n = keys.dense_int_idxs.size();
      const int64_t dense_base = keys.dense_int_min;
      for (size_t chunk = w; chunk < source.chunk_count;
           chunk += source.workers) {
        const size_t begin = chunk * source.probe_chunk_rows;
        const size_t end =
            std::min(source.total_left, begin + source.probe_chunk_rows);
        for (size_t ri = begin; ri < end; ++ri) {
          const Value& cell = source.left.rows[ri].values_[key_col];
          if (cell.IsNull()) {
            continue;
          }
          const int64_t key = AddJoinKeyBias(cell.value.int_value, bias);
          const uint64_t idx =
              static_cast<uint64_t>(key) - static_cast<uint64_t>(dense_base);
          if (idx >= dense_n) {
            continue;
          }
          const uint32_t r_idx = dense[idx];
          if (r_idx == kInvalidRightIdx) {
            continue;
          }
          const uint32_t gid = gid_of(r_idx);
          if (gid == kInvalidGid) {
            continue;
          }
          ++local.comparisons;
          if (direct.use_shared_direct_gid) {
            __atomic_store_n(&dw.matched[gid], 1, __ATOMIC_RELAXED);
            __atomic_fetch_add(&dw.counts[gid], 1, __ATOMIC_RELAXED);
          } else {
            dw.matched[gid] = 1;
            dw.counts[gid] += 1;
          }
        }
      }
      return Status::kSuccess;
    }
    if (source.integer_key && deferred.left_numeric_probe &&
        keys.use_dense_int_rows && !source.left.HasSpill()) {
      const slot_t key_col = source.left_columns[0];
      const int64_t bias = source.left_key_offsets[0];
      DirectGidWorker& dw = direct.direct_gid_workers[w];
      const uint32_t* dense = keys.dense_int_idxs.data();
      const size_t dense_n = keys.dense_int_idxs.size();
      const int64_t dense_base = keys.dense_int_min;
      for (size_t chunk = w; chunk < source.chunk_count;
           chunk += source.workers) {
        const size_t begin = chunk * source.probe_chunk_rows;
        const size_t end =
            std::min(source.total_left, begin + source.probe_chunk_rows);
        for (size_t ri = begin; ri < end; ++ri) {
          const Row& left_row = source.left.rows[ri];
          const Value& cell = left_row.values_[key_col];
          if (cell.IsNull()) {
            continue;
          }
          const int64_t key = AddJoinKeyBias(cell.value.int_value, bias);
          const uint64_t idx =
              static_cast<uint64_t>(key) - static_cast<uint64_t>(dense_base);
          if (idx >= dense_n) {
            continue;
          }
          const uint32_t r_idx = dense[idx];
          if (r_idx == kInvalidRightIdx) {
            continue;
          }
          const uint32_t gid = gid_of(r_idx);
          if (gid == kInvalidGid) {
            continue;
          }
          ++local.comparisons;
          charge_numeric(
              dw, gid, r_idx,
              [&](int32_t col, double* out) {
                const Value& value = left_row.values_[static_cast<size_t>(col)];
                return !value.IsNull() && ReadFusedDouble(value, out);
              },
              [&](int32_t col, int64_t* out) {
                const Value& value = left_row.values_[static_cast<size_t>(col)];
                if (value.IsNull() || value.type != ValueType::kInt64 ||
                    value.IsUnsigned()) {
                  return false;
                }
                *out = value.value.int_value;
                return true;
              },
              [&](int32_t col) {
                return !left_row.values_[static_cast<size_t>(col)].IsNull();
              });
        }
      }
      return Status::kSuccess;
    }
    for (size_t chunk = w; chunk < source.chunk_count;
         chunk += source.workers) {
      const size_t begin = chunk * source.probe_chunk_rows;
      const size_t end =
          std::min(source.total_left, begin + source.probe_chunk_rows);
      for (size_t ri = begin; ri < end; ++ri) {
        const Row& left_row = source.left.rows[ri];
        if (HasNullKey(left_row, source.left_columns)) {
          if (source.join_type == JoinType::kLeft) {
            RETURN_IF_FAIL(callbacks.unmatched_left(local, scratch, left_row));
          }
          continue;
        }
        int64_t key = 0;
        if (source.integer_key) {
          key = AddJoinKeyBias(IntegerJoinKey(left_row, source.left_columns[0]),
                               source.left_key_offsets[0]);
        } else {
          const int64_t k0 =
              AddJoinKeyBias(IntegerJoinKey(left_row, source.left_columns[0]),
                             source.left_key_offsets[0]);
          const int64_t k1 =
              AddJoinKeyBias(IntegerJoinKey(left_row, source.left_columns[1]),
                             source.left_key_offsets[1]);
          if (!FitsFusedInt32(k0) || !FitsFusedInt32(k1)) {
            if (source.join_type == JoinType::kLeft) {
              RETURN_IF_FAIL(
                  callbacks.unmatched_left(local, scratch, left_row));
            }
            continue;
          }
          key = PackFusedInt32Pair(k0, k1);
        }
        if (source.bulk_raw_count) {
          // One open-addressing probe answers the whole join step: the
          // duplicate count stands in for the bucket walk (whose Entry
          // chain and per-pair build-row dereferences dominated this
          // loop), and the group key is read straight off the outer row.
          const BulkCountValue* bulk = nullptr;
          if (keys.use_dense_bulk) {
            const uint64_t idx = static_cast<uint64_t>(key) -
                                 static_cast<uint64_t>(keys.dense_bulk_min);
            if (idx < keys.dense_bulk_counts.size() &&
                keys.dense_bulk_counts[idx].r_idx != kInvalidRightIdx) {
              bulk = &keys.dense_bulk_counts[idx];
            }
          } else {
            bulk = keys.bulk_counts->Find(key);
          }
          if (bulk == nullptr) {
            if (source.join_type == JoinType::kLeft) {
              RETURN_IF_FAIL(
                  callbacks.unmatched_left(local, scratch, left_row));
            }
            continue;
          }
          local.comparisons += bulk->pairs;
          const Value& bulk_cell = left_row.values_[source.int_group_slot];
          size_t* bulk_slot = nullptr;
          bool new_group = false;
          size_t group_index = 0;
          if (!bulk_cell.IsNull() && bulk_cell.type == source.int_group_type &&
              !bulk_cell.IsUnsigned()) {
            bool inserted = false;
            bulk_slot = local.int_offsets.TryEmplace(bulk_cell.value.int_value,
                                                     &inserted);
            if (!inserted) {
              group_index = *bulk_slot;
            } else {
              local.key_scratch.assign(1, bulk_cell);
              new_group = true;
            }
          } else {
            local.key_scratch.clear();
            local.key_scratch.push_back(CanonicalDistinctValue(bulk_cell));
            if (auto found = local.offsets.find(local.key_scratch);
                found != local.offsets.end()) {
              group_index = found->second;
            } else {
              new_group = true;
            }
          }
          if (new_group) {
            group_index = local.groups.size();
            FusedGroup group;
            group.rep_left = &left_row;
            group.rep_right_idx = bulk->r_idx;
            if (bulk_slot != nullptr) {
              if (!source.all_direct_group_keys) {
                group.key = Row(local.key_scratch);
              }
              local.groups.push_back(std::move(group));
              *bulk_slot = group_index;
            } else {
              // T4639/T4640: building two Rows from key_scratch paid two
              // full key materializations (array + per-Value string copies)
              // per new group. Move the scratch into one key, copy that
              // into the offsets map, and move it into group.key -- same
              // double ownership, half the allocations.
              Row local_key(std::move(local.key_scratch));
              local.offsets.emplace(Row(local_key), group_index);
              group.key = std::move(local_key);
              local.groups.push_back(std::move(group));
            }
          }
          local.groups[group_index].raw_count += bulk->count;
          continue;
        }
        if (keys.use_dense_int_rows) {
          const uint64_t idx = static_cast<uint64_t>(key) -
                               static_cast<uint64_t>(keys.dense_int_min);
          const uint32_t r_idx = idx < keys.dense_int_idxs.size()
                                     ? keys.dense_int_idxs[idx]
                                     : kInvalidRightIdx;
          bool matched = false;
          if (r_idx != kInvalidRightIdx) {
            ++local.comparisons;
            if (keys.use_preassigned_build_groups) {
              RETURN_IF_FAIL(callbacks.accumulate_preassigned_pair(
                  w, scratch, left_row, r_idx));
              matched = true;
            } else {
              size_t cached_group = kNoCachedGroup;
              ASSIGN_OR_RETURN(
                  bool, accumulated,
                  ProcessPair(local, left_row, r_idx, scratch, &cached_group));
              matched = accumulated;
            }
          }
          if (!matched && source.join_type == JoinType::kLeft) {
            RETURN_IF_FAIL(callbacks.unmatched_left(local, scratch, left_row));
          }
          continue;
        }
        if (keys.use_unique_int_table) {
          if (ri + 8 < end) {
            const Row& next_row = source.left.rows[ri + 8];
            if (!HasNullKey(next_row, source.left_columns)) [[likely]] {
              if (source.integer_key) {
                const int64_t nk = AddJoinKeyBias(
                    IntegerJoinKey(next_row, source.left_columns[0]),
                    source.left_key_offsets[0]);
                __builtin_prefetch(&keys.unique_int_table[HashUniqueIntKey(nk) &
                                                          keys.unique_int_mask],
                                   0, 1);
              } else {
                const int64_t nk0 = AddJoinKeyBias(
                    IntegerJoinKey(next_row, source.left_columns[0]),
                    source.left_key_offsets[0]);
                const int64_t nk1 = AddJoinKeyBias(
                    IntegerJoinKey(next_row, source.left_columns[1]),
                    source.left_key_offsets[1]);
                if (FitsFusedInt32(nk0) && FitsFusedInt32(nk1)) [[likely]] {
                  __builtin_prefetch(
                      &keys.unique_int_table[HashUniqueIntKey(
                                                 PackFusedInt32Pair(nk0, nk1)) &
                                             keys.unique_int_mask],
                      0, 1);
                }
              }
            }
          }
          size_t slot = HashUniqueIntKey(key) & keys.unique_int_mask;
          uint32_t r_idx = kInvalidRightIdx;
          while (true) {
            const UniqueIntSlot& entry = keys.unique_int_table[slot];
            if (entry.r_idx == kInvalidRightIdx) {
              break;
            }
            if (entry.key == key) [[likely]] {
              r_idx = entry.r_idx;
              break;
            }
            slot = (slot + 1) & keys.unique_int_mask;
          }
          bool matched = false;
          if (r_idx != kInvalidRightIdx) [[likely]] {
            ++local.comparisons;
            if (keys.use_preassigned_build_groups) {
              RETURN_IF_FAIL(callbacks.accumulate_preassigned_pair(
                  w, scratch, left_row, r_idx));
              matched = true;
            } else {
              size_t cached_group = kNoCachedGroup;
              ASSIGN_OR_RETURN(
                  bool, accumulated,
                  ProcessPair(local, left_row, r_idx, scratch, &cached_group));
              matched = accumulated;
            }
          }
          if (!matched && source.join_type == JoinType::kLeft) {
            RETURN_IF_FAIL(callbacks.unmatched_left(local, scratch, left_row));
          }
          continue;
        }
        if (ri + 4 < end) {
          const Row& next_row = source.left.rows[ri + 4];
          if (!HasNullKey(next_row, source.left_columns)) [[likely]] {
            if (source.integer_key) {
              keys.int_buckets->PrefetchBucket(AddJoinKeyBias(
                  IntegerJoinKey(next_row, source.left_columns[0]),
                  source.left_key_offsets[0]));
            } else {
              const int64_t nk0 = AddJoinKeyBias(
                  IntegerJoinKey(next_row, source.left_columns[0]),
                  source.left_key_offsets[0]);
              const int64_t nk1 = AddJoinKeyBias(
                  IntegerJoinKey(next_row, source.left_columns[1]),
                  source.left_key_offsets[1]);
              if (FitsFusedInt32(nk0) && FitsFusedInt32(nk1)) [[likely]] {
                keys.int_buckets->PrefetchBucket(PackFusedInt32Pair(nk0, nk1));
              }
            }
          }
        }
        bool matched = false;
        size_t cached_group = kNoCachedGroup;
        const auto [first, last] = keys.int_buckets->equal_range(key);
        for (auto iter = first; iter != last; ++iter) {
          ++local.comparisons;
          if (keys.use_preassigned_build_groups) {
            RETURN_IF_FAIL(callbacks.accumulate_preassigned_pair(
                w, scratch, left_row, iter->second));
            matched = true;
          } else {
            ASSIGN_OR_RETURN(bool, accumulated,
                             ProcessPair(local, left_row, iter->second, scratch,
                                         &cached_group));
            matched = matched || accumulated;
          }
        }
        if (!matched && source.join_type == JoinType::kLeft) {
          RETURN_IF_FAIL(callbacks.unmatched_left(local, scratch, left_row));
        }
      }
    }
    return Status::kSuccess;
  }
  if (source.composite_int_key) {
    const FlatJoinIdxBuckets<CompositeIntKey, CompositeIntKeyHash>& buckets =
        *keys.composite_buckets;
    for (size_t chunk = w; chunk < source.chunk_count;
         chunk += source.workers) {
      const size_t begin = chunk * source.probe_chunk_rows;
      const size_t end =
          std::min(source.total_left, begin + source.probe_chunk_rows);
      for (size_t ri = begin; ri < end; ++ri) {
        const Row& left_row = source.left.rows[ri];
        if (HasNullKey(left_row, source.left_columns)) {
          if (source.join_type == JoinType::kLeft) {
            RETURN_IF_FAIL(callbacks.unmatched_left(local, scratch, left_row));
          }
          continue;
        }
        const CompositeIntKey key = EncodeCompositeIntKey(
            left_row, source.left_columns, &source.left_key_offsets);
        bool matched = false;
        size_t cached_group = kNoCachedGroup;
        const auto [first, last] = buckets.equal_range(key);
        for (auto iter = first; iter != last; ++iter) {
          ++local.comparisons;
          if (keys.use_preassigned_build_groups) {
            RETURN_IF_FAIL(callbacks.accumulate_preassigned_pair(
                w, scratch, left_row, iter->second));
            matched = true;
          } else {
            ASSIGN_OR_RETURN(bool, accumulated,
                             ProcessPair(local, left_row, iter->second, scratch,
                                         &cached_group));
            matched = matched || accumulated;
          }
        }
        if (!matched && source.join_type == JoinType::kLeft) {
          RETURN_IF_FAIL(callbacks.unmatched_left(local, scratch, left_row));
        }
      }
    }
    return Status::kSuccess;
  }
  const FlatJoinIdxBuckets<std::string>& buckets = *keys.key_buckets;
  const auto probe_key_bucket_range = [&](size_t begin, size_t end) -> Status {
    for (size_t ri = begin; ri < end; ++ri) {
      const Row& left_row = source.left.rows[ri];
      if (HasNullKey(left_row, source.left_columns)) {
        if (source.join_type == JoinType::kLeft) {
          RETURN_IF_FAIL(callbacks.unmatched_left(local, scratch, left_row));
        }
        continue;
      }
      const std::string key = EncodeJoinKey(left_row, source.left_columns,
                                            &source.left_key_offsets);
      bool matched = false;
      size_t cached_group = kNoCachedGroup;
      const auto [first, last] = buckets.equal_range(key);
      for (auto iter = first; iter != last; ++iter) {
        ++local.comparisons;
        if (keys.use_preassigned_build_groups) {
          RETURN_IF_FAIL(callbacks.accumulate_preassigned_pair(
              w, scratch, left_row, iter->second));
          matched = true;
        } else {
          ASSIGN_OR_RETURN(bool, accumulated,
                           ProcessPair(local, left_row, iter->second, scratch,
                                       &cached_group));
          matched = matched || accumulated;
        }
      }
      if (!matched && source.join_type == JoinType::kLeft) {
        RETURN_IF_FAIL(callbacks.unmatched_left(local, scratch, left_row));
      }
    }
    return Status::kSuccess;
  };
  if (keys.fanout_key_bounds.size() == source.workers + 1) {
    RETURN_IF_FAIL(probe_key_bucket_range(keys.fanout_key_bounds[w],
                                          keys.fanout_key_bounds[w + 1]));
  } else {
    for (size_t chunk = w; chunk < source.chunk_count;
         chunk += source.workers) {
      const size_t begin = chunk * source.probe_chunk_rows;
      const size_t end =
          std::min(source.total_left, begin + source.probe_chunk_rows);
      RETURN_IF_FAIL(probe_key_bucket_range(begin, end));
    }
  }
  return Status::kSuccess;
}
}  // namespace tinylamb::relational_detail
