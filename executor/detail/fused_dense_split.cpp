/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/fused_dense_split.hpp"

#include "common/parallel_work.hpp"
#include "common/platform/range_selection.hpp"
#include "executor/detail/scan_filter.hpp"
#include "type/date.hpp"

namespace tinylamb::relational_detail {

void SortUniqueSlots(std::vector<slot_t>* cols) {
  std::sort(cols->begin(), cols->end());
  cols->erase(std::unique(cols->begin(), cols->end()), cols->end());
}

bool DenseIntBlockMayMatch(const LSTablePageBlock& meta_blk, slot_t full_key,
                           int64_t dense_int_min,
                           std::span<const uint32_t> dense_int_idxs,
                           uint32_t invalid_idx) noexcept {
  if (full_key >= meta_blk.columns.size() || dense_int_idxs.empty()) {
    return true;
  }
  const LSTableColumnBlock& jk = meta_blk.columns[full_key];
  if (!jk.has_min_max) {
    return true;
  }
  const uint64_t span = dense_int_idxs.size();
  const bool above = jk.min_int >= dense_int_min &&
                     static_cast<uint64_t>(jk.min_int) -
                             static_cast<uint64_t>(dense_int_min) >=
                         span;
  if (jk.max_int < dense_int_min || above) {
    return false;
  }
  if (jk.min_int > jk.max_int) {
    return true;
  }
  const uint64_t lo_off = jk.min_int >= dense_int_min
                              ? static_cast<uint64_t>(jk.min_int) -
                                    static_cast<uint64_t>(dense_int_min)
                              : 0;
  const uint64_t hi_off = std::min<uint64_t>(
      static_cast<uint64_t>(jk.max_int) - static_cast<uint64_t>(dense_int_min),
      span - 1);
  if (hi_off - lo_off <= 512) {
    for (uint64_t off = lo_off; off <= hi_off; ++off) {
      if (dense_int_idxs[off] != invalid_idx) {
        return true;
      }
    }
    return false;
  }
  return true;
}

// Register-tight, out-of-line dense RF pre-filter over one LSTablePageBlock.
// Keeps the 28M-row rejection loop out of TryFusedGroupedJoin's large stack
// frame (zero stack spills) and checks the unsigned range test BEFORE null
// bitmaps or bitset loads.
// Lean proof: lean/TinylambOpt/TpchSf10VectorizedExec.lean
//   (dense_rf_prefilter_block_sound)
[[gnu::noinline]] size_t PrefilterDenseRfBlock(
    const int64_t* __restrict__ vals, uint32_t nrows, int64_t rf_lo,
    int64_t rf_hi, const uint64_t* __restrict__ bits, bool all_bits_set,
    const LSTableColumnBlock* rf_col, bool has_null,
    uint32_t* __restrict__ out_sel) noexcept {
  const auto lo_u = static_cast<uint64_t>(rf_lo);
  const uint64_t span_u = static_cast<uint64_t>(rf_hi) - lo_u;
  size_t n_sel = 0;
  if (all_bits_set) {
    uint32_t selected =
        platform::SelectInt64ClosedRange(vals, nrows, rf_lo, rf_hi, out_sel);
    if (has_null) {
      uint32_t nonnull = 0;
      for (uint32_t i = 0; i < selected; ++i) {
        if (!rf_col->IsNull(out_sel[i])) {
          out_sel[nonnull++] = out_sel[i];
        }
      }
      selected = nonnull;
    }
    return selected;
  }
  if (!has_null) {
    for (uint32_t r = 0; r < nrows; ++r) {
      const uint64_t off = static_cast<uint64_t>(vals[r]) - lo_u;
      if (off <= span_u && ((bits[off >> 6] >> (off & 63)) & 1ULL) != 0)
          [[unlikely]] {
        out_sel[n_sel++] = r;
      }
    }
  } else {
    for (uint32_t r = 0; r < nrows; ++r) {
      const uint64_t off = static_cast<uint64_t>(vals[r]) - lo_u;
      if (off <= span_u && ((bits[off >> 6] >> (off & 63)) & 1ULL) != 0)
          [[unlikely]] {
        if (!rf_col->IsNull(r)) {
          out_sel[n_sel++] = r;
        }
      }
    }
  }

  return n_sel;
}

StatusOr<std::optional<Relation>> DenseSplitExecution::Run() const {
  const uint32_t pk0_card = jit_dense_split.pk0_card;
  const uint64_t dom = jit_dense_split.dom;
  constexpr uint8_t kPartShift = kDenseSplitPartShift;
  constexpr size_t kPartSize = size_t{1} << kPartShift;
  const size_t n_parts = (dom + kPartSize - 1) >> kPartShift;

  std::vector<uint32_t> key_to_rsubgid(dense_int_idxs.size(), UINT32_MAX);
  for (size_t i = 0; i < dense_int_idxs.size(); ++i) {
    const uint32_t r_idx = dense_int_idxs[i];
    if (r_idx != kInvalidRightIdx) {
      key_to_rsubgid[i] =
          split_uses_right_subgid ? split_right_subgid_ptr[r_idx] : 0U;
    }
  }
  std::vector<uint32_t> pk0_subgid_lut(pk0.dense_size, UINT32_MAX);
  for (size_t i = 0; i < pk0.dense_size; ++i) {
    const auto& e = pk0.dense_ptr[i];
    if (e.nb != std::numeric_limits<uint32_t>::max() && e.subgid < pk0_card) {
      pk0_subgid_lut[i] = e.subgid;
    }
  }

  const LSTableStore& store = *probe.store;
  const size_t total_blocks = store.blocks.size();
  const size_t base_proj_cols = probe.projection.size();
  const slot_t jk0_slot = left_columns[0];
  const slot_t pk0_slot = pk0.pk_col;
  const auto* peek_ptr =
      probe.integer_peeks.empty() ? nullptr : &probe.integer_peeks;
  const ScanRuntimeFilter& primary_rf = probe.runtime_filter;
  const RuntimeFilterGroup* active_rf_group =
      primary_rf.enabled() ? primary_rf.group.get() : nullptr;
  const std::optional<slot_t> full_rf_col =
      (active_rf_group != nullptr && primary_rf.column < base_proj_cols)
          ? std::optional<slot_t>(probe.projection[primary_rf.column])
          : std::nullopt;
  const bool rf_has_dense =
      active_rf_group != nullptr && active_rf_group->has_dense_bits();
  const uint64_t* const rf_dense_bits =
      rf_has_dense ? active_rf_group->dense_bits_data() : nullptr;
  const size_t rf_dense_words =
      rf_has_dense ? active_rf_group->dense_bits_word_count() : 0;
  const int64_t rf_lo = rf_has_dense ? active_rf_group->Lo() : 0;
  const int64_t rf_hi = rf_has_dense ? active_rf_group->Hi() : 0;
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
        const uint64_t mask =
            rem_bits == 64U ? ~uint64_t{0} : ((uint64_t{1} << rem_bits) - 1ULL);
        if ((rf_dense_bits[full_words] & mask) == mask) {
          rf_dense_all_set = true;
        }
      }
    }
  }
  const bool has_null_peeks = !probe.null_peeks.empty();
  const auto eval_int_peek = [](BinaryOperation op, int64_t a,
                                int64_t c) noexcept -> bool {
    switch (op) {
      case BinaryOperation::kEquals:
        return a == c;
      case BinaryOperation::kNotEquals:
        return a != c;
      case BinaryOperation::kLessThan:
        return a < c;
      case BinaryOperation::kLessThanEquals:
        return a <= c;
      case BinaryOperation::kGreaterThan:
        return a > c;
      case BinaryOperation::kGreaterThanEquals:
        return a >= c;
      default:
        return false;
    }
  };

  std::vector<std::vector<std::vector<uint32_t>>> worker_part_gids(
      workers, std::vector<std::vector<uint32_t>>(n_parts));
  std::vector<std::vector<std::vector<double>>> worker_part_vals(
      workers, std::vector<std::vector<double>>(n_parts));
  std::vector<Status> p1_errs(workers, Status::kSuccess);

  const auto run_phase1 = [&](size_t w) {
    std::array<std::optional<JitDenseSplitRadixAgg::ExtractKernel>, 128>
        extract_cache;
    std::vector<uint32_t> rf_sel_buf;
    std::vector<uint32_t> stripe_gids;
    std::vector<double> stripe_vals;
    std::vector<int64_t> dummy_zeros;
    std::vector<uint8_t> dummy_ones;
    std::vector<uint32_t> stripe_hist(n_parts, 0);
    std::vector<uint32_t*> dst_gids(n_parts, nullptr);
    std::vector<double*> dst_vals(n_parts, nullptr);
    std::vector<uint32_t> dst_pos(n_parts, 0);
    std::array<const LSTableColumnBlock*, 16> proj_cols{};
    struct DefExtraRf {
      const LSTableColumnBlock* col{nullptr};
      const uint64_t* dense_bits{nullptr};
      int64_t lo{0};
      int64_t hi{0};
      bool has_null{false};
      bool type_ok{true};
      const RuntimeFilterGroup* group{nullptr};
    };
    std::array<DefExtraRf, 4> extra_rfs{};
    std::vector<slot_t> p1_mat_cols;
    p1_mat_cols.push_back(probe.projection[jk0_slot]);
    p1_mat_cols.push_back(probe.projection[pk0_slot]);
    if (full_rf_col.has_value()) {
      p1_mat_cols.push_back(*full_rf_col);
    }
    for (const ScanRuntimeFilter& ext : primary_rf.extra) {
      if (ext.enabled() && ext.column < base_proj_cols) {
        p1_mat_cols.push_back(probe.projection[ext.column]);
      }
    }
    for (const IntegerPeekCompare& pred : probe.integer_peeks) {
      p1_mat_cols.push_back(pred.column);
    }
    for (const NullPeekCompare& test : probe.null_peeks) {
      p1_mat_cols.push_back(test.column);
    }
    for (size_t vi = 0; vi < 4; ++vi) {
      if (jit_dense_split.need_v[vi]) {
        p1_mat_cols.push_back(probe.projection[jit_dense_split.v_cols[vi]]);
      }
    }
    SortUniqueSlots(&p1_mat_cols);
    LSTableStore::LazyBlockScratch lazy_scratch;

    for (size_t stripe = w; stripe * 64 < total_blocks; stripe += workers) {
      const size_t b_begin = stripe * 64;
      const size_t b_end = std::min(total_blocks, b_begin + 64);
      std::fill(stripe_hist.begin(), stripe_hist.end(), 0U);
      size_t stripe_m = 0;

      for (size_t bi = b_begin; bi < b_end; ++bi) {
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
          const slot_t ext_full = probe.projection[ext.column];
          if (ext_full < meta_blk.columns.size()) {
            const LSTableColumnBlock& mcol = meta_blk.columns[ext_full];
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
        if (!packed_2x32_int_key && left_key_offsets[0] == 0 &&
            jk0_slot < base_proj_cols &&
            !DenseIntBlockMayMatch(meta_blk, probe.projection[jk0_slot],
                                   dense_int_min, dense_int_idxs,
                                   kInvalidRightIdx)) {
          continue;
        }
        bool peek_all_pass = true;
        if (peek_ptr != nullptr) {
          for (const IntegerPeekCompare& pred : *peek_ptr) {
            const LSTableColumnBlock& pcol = meta_blk.columns[pred.column];
            if (pcol.has_null || !pcol.has_min_max ||
                !eval_int_peek(pred.op, pcol.min_int, pred.constant) ||
                !eval_int_peek(pred.op, pcol.max_int, pred.constant)) {
              peek_all_pass = false;
              break;
            }
          }
        }
        lazy_scratch.transient.clear();
        const LSTablePageBlock* mat =
            store.MaterializeBlockView(bi, &lazy_scratch, p1_mat_cols);
        if (mat == nullptr) {
          p1_errs[w] = Status::kIOError;
          return;
        }
        const LSTablePageBlock& blk = *mat;
        for (size_t c = 0; c < base_proj_cols; ++c) {
          proj_cols[c] = &blk.columns[probe.projection[c]];
        }
        const LSTableColumnBlock& jk_cb = *proj_cols[jk0_slot];
        const LSTableColumnBlock& pk0_cb = *proj_cols[pk0_slot];
        if (jk_cb.type == ValueType::kNull || pk0_cb.type == ValueType::kNull) {
          continue;
        }
        const LSTableColumnBlock* const rf_col =
            full_rf_col.has_value() ? &blk.columns[*full_rf_col] : nullptr;
        const bool rf_col_has_null = rf_col != nullptr && rf_col->has_null;
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

        bool use_sel = false;
        uint32_t row_cnt = blk.row_count;
        if (!has_null_peeks && extra_rf_count == 0 &&
            (peek_all_pass || peek_ptr == nullptr)) {
          if (rf_col != nullptr) {
            if (rf_has_dense && rf_hi >= rf_lo &&
                rf_col->int_cells.size() >= blk.row_count) {
              if (!rf_dense_all_set || rf_col_has_null ||
                  !rf_col->has_min_max || rf_col->min_int < rf_lo ||
                  rf_col->max_int > rf_hi) {
                if (rf_sel_buf.size() < blk.row_count) {
                  rf_sel_buf.resize(blk.row_count);
                }
                row_cnt = static_cast<uint32_t>(PrefilterDenseRfBlock(
                    rf_col->int_cells.data(), blk.row_count, rf_lo, rf_hi,
                    rf_dense_bits, rf_dense_all_set, rf_col, rf_col_has_null,
                    rf_sel_buf.data()));
                if (row_cnt == 0) {
                  continue;
                }
                use_sel = true;
              }
            } else {
              if (rf_sel_buf.size() < blk.row_count) {
                rf_sel_buf.resize(blk.row_count);
              }
              uint32_t out_n = 0;
              for (uint32_t r = 0; r < blk.row_count; ++r) {
                if (rf_col_has_null && rf_col->IsNull(r)) {
                  continue;
                }
                if (active_rf_group->Reject(rf_col->IntAt(r))) {
                  continue;
                }
                rf_sel_buf[out_n++] = r;
              }
              if (out_n == 0) {
                continue;
              }
              row_cnt = out_n;
              use_sel = true;
            }
          }
        } else {
          if (rf_sel_buf.size() < blk.row_count) {
            rf_sel_buf.resize(blk.row_count);
          }
          uint32_t out_n = 0;
          for (uint32_t r = 0; r < blk.row_count; ++r) {
            if (has_null_peeks &&
                std::ranges::any_of(
                    probe.null_peeks, [&](const NullPeekCompare& test) {
                      return blk.columns[test.column].IsNull(r) !=
                             test.expect_null;
                    })) {
              continue;
            }
            if (rf_col != nullptr) {
              if (rf_col_has_null && rf_col->IsNull(r)) {
                continue;
              }
              const int64_t kv = rf_col->IntAt(r);
              if (rf_has_dense) {
                if (kv < rf_lo || kv > rf_hi) {
                  continue;
                }
                const uint64_t off =
                    static_cast<uint64_t>(kv) - static_cast<uint64_t>(rf_lo);
                if ((rf_dense_bits[off >> 6] & (uint64_t{1} << (off & 63))) ==
                    0) {
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
                if (erf.has_null && erf.col->IsNull(r)) {
                  extra_ok = false;
                  break;
                }
                if (!erf.type_ok) {
                  continue;
                }
                const int64_t kv = erf.col->IntAt(r);
                if (erf.dense_bits != nullptr) {
                  if (kv < erf.lo || kv > erf.hi) {
                    extra_ok = false;
                    {
                    }
                    break;
                  }
                  const uint64_t off =
                      static_cast<uint64_t>(kv) - static_cast<uint64_t>(erf.lo);
                  {
                  }
                  if ((erf.dense_bits[off >> 6] &
                       (uint64_t{1} << (off & 63))) == 0) {
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
                    !eval_int_peek(pred.op, pcol.IntAt(r), pred.constant)) {
                  peek_ok = false;
                  break;
                }
              }
              if (!peek_ok) {
                continue;
              }
            }
            rf_sel_buf[out_n++] = r;
          }
          if (out_n == 0) {
            continue;
          }
          row_cnt = out_n;
          use_sel = true;
        }

        if (stripe_m + row_cnt > stripe_gids.size()) {
          const size_t new_cap =
              std::max<size_t>(stripe_m + row_cnt, stripe_gids.size() * 2);
          stripe_gids.resize(new_cap);
          stripe_vals.resize(new_cap);
        }

        const auto null_ptr_of =
            [&](const LSTableColumnBlock& cb) noexcept -> const uint8_t* {
          if (!cb.has_null) {
            return nullptr;
          }
          const auto sp = cb.Nulls();
          return sp.size() >= blk.row_count ? sp.data() : nullptr;
        };
        const uint8_t* jk_nulls = null_ptr_of(jk_cb);
        const uint8_t* pk0_nulls = null_ptr_of(pk0_cb);
        std::array<const int64_t*, 4> vp{nullptr, nullptr, nullptr, nullptr};
        std::array<const uint8_t*, 4> np{nullptr, nullptr, nullptr, nullptr};
        uint8_t val_null_mask = 0;
        bool ensured_dummy = false;
        for (size_t vi = 0; vi < 4; ++vi) {
          if (!jit_dense_split.need_v[vi]) {
            continue;
          }
          const LSTableColumnBlock& vcb =
              *proj_cols[jit_dense_split.v_cols[vi]];
          if (vcb.type == ValueType::kNull) {
            if (!ensured_dummy) {
              if (dummy_zeros.size() < blk.row_count) {
                dummy_zeros.assign(blk.row_count, 0);
              }
              if (dummy_ones.size() < blk.row_count) {
                dummy_ones.assign(blk.row_count, uint8_t{1});
              }
              ensured_dummy = true;
            }
            vp[vi] = dummy_zeros.data();
            np[vi] = dummy_ones.data();
            val_null_mask |= static_cast<uint8_t>(1U << vi);
          } else {
            vp[vi] = vcb.int_cells.data();
            np[vi] = null_ptr_of(vcb);
            if (np[vi] != nullptr) {
              val_null_mask |= static_cast<uint8_t>(1U << vi);
            }
          }
        }
        const auto spec_idx = static_cast<size_t>(
            (use_sel ? 1U : 0U) | (jk_nulls != nullptr ? 2U : 0U) |
            (pk0_nulls != nullptr ? 4U : 0U) |
            (static_cast<unsigned>(val_null_mask) << 3U));
        if (!extract_cache[spec_idx].has_value()) {
          JitDenseSplitRadixAgg::ExtractSpec espec{
              .op = jit_dense_split.expr_op,
              .use_sel = use_sel,
              .jk_has_null = (jk_nulls != nullptr),
              .pk0_has_null = (pk0_nulls != nullptr),
              .val_null_mask = val_null_mask,
              .part_shift = kPartShift,
          };
          extract_cache[spec_idx] =
              JitDenseSplitRadixAgg::CompileExtract(espec);
          if (!extract_cache[spec_idx].has_value()) {
            p1_errs[w] =
                StatusError(StatusCode::kRuntimeError,
                            "JitDenseSplitRadixAgg::CompileExtract failed");
            return;
          }
        }
        const uint32_t m_blk = extract_cache[spec_idx]->Run(
            row_cnt, blk.row_count, use_sel ? rf_sel_buf.data() : nullptr,
            jk_cb.int_cells.data(), jk_nulls, dense_int_min,
            dense_int_idxs.size(), key_to_rsubgid.data(),
            pk0_cb.int_cells.data(), pk0_nulls, pk0.min_k, pk0.dense_size,
            pk0_subgid_lut.data(), pk0_card, vp[0], np[0], vp[1], np[1], vp[2],
            np[2], vp[3], np[3], jit_dense_split.div_const,
            stripe_gids.data() + stripe_m, stripe_vals.data() + stripe_m,
            stripe_hist);
        if (m_blk == JitDenseSplitRadixAgg::kInvalidExtractCount) {
          p1_errs[w] = StatusError(StatusCode::kCorrupt,
                                   "invalid dense split row or partition");
          return;
        }
        stripe_m += m_blk;
      }
      if (stripe_m > 0) {
        for (size_t p = 0; p < n_parts; ++p) {
          const uint32_t cnt_p = stripe_hist[p];
          if (cnt_p == 0) {
            dst_gids[p] = nullptr;
            dst_vals[p] = nullptr;
            dst_pos[p] = 0;
            continue;
          }
          const size_t old_sz = worker_part_gids[w][p].size();
          worker_part_gids[w][p].resize(old_sz + cnt_p);
          worker_part_vals[w][p].resize(old_sz + cnt_p);
          dst_gids[p] = worker_part_gids[w][p].data();
          dst_vals[p] = worker_part_vals[w][p].data();
          dst_pos[p] = static_cast<uint32_t>(old_sz);
        }
        jit_dense_split.scatter_k->Run(static_cast<uint32_t>(stripe_m),
                                       stripe_gids.data(), stripe_vals.data(),
                                       dst_gids.data(), dst_vals.data(),
                                       dst_pos.data());
      }
    }
  };

  if (workers == 1) {
    run_phase1(0);
  } else {
    RETURN_IF_FAIL(RunParallelTasks(workers, run_phase1));
  }
  for (const Status& st : p1_errs) {
    RETURN_IF_FAIL(st);
  }
  std::vector<uint32_t>().swap(key_to_rsubgid);
  std::vector<uint32_t>().swap(pk0_subgid_lut);

  const bool need_right_gi = std::ranges::any_of(
      jit_dense_split.sel_items,
      [](const auto& s) { return s.kind == 2 && s.vt != ValueType::kNull; });
  const size_t sel_n = jit_dense_split.sel_items.size();
  std::vector<std::vector<ColumnarChunk>> part_chunks(n_parts);
  std::vector<std::vector<uint8_t>> part_col_nonnull(
      n_parts, std::vector<uint8_t>(sel_n, 0));
  std::atomic<size_t> next_p{0};
  std::atomic<bool> any_inf{false};
  std::atomic<uint64_t> total_matched_rows{0};

  const auto run_phase2 = [&](size_t) {
    std::vector<double> sums(kPartSize, 0.0);
    std::vector<uint64_t> counts(jit_dense_split.is_avg ? kPartSize : 0, 0);
    std::vector<uint64_t> matched_bits(kPartSize / 64, 0);
    std::vector<uint64_t> nonnull_bits(kPartSize / 64, 0);
    std::vector<uint32_t> matched_lgs;
    matched_lgs.reserve(kPartSize);

    while (true) {
      const size_t p = next_p.fetch_add(1, std::memory_order_relaxed);
      if (p >= n_parts) {
        break;
      }

      size_t part_tuples = 0;
      for (size_t w = 0; w < workers; ++w) {
        part_tuples += worker_part_gids[w][p].size();
      }
      if (part_tuples == 0) {
        continue;
      }
      total_matched_rows.fetch_add(part_tuples, std::memory_order_relaxed);

      const size_t part_len =
          std::min<size_t>(kPartSize, dom - (p << kPartShift));
      const size_t n_words = (part_len + 63) >> 6;
      std::fill_n(sums.data(), part_len, 0.0);
      if (jit_dense_split.is_avg) {
        std::fill_n(counts.data(), part_len, 0ULL);
      }
      std::fill_n(matched_bits.data(), n_words, 0ULL);
      std::fill_n(nonnull_bits.data(), n_words, 0ULL);
      uint8_t local_inf = 0;

      for (size_t w = 0; w < workers; ++w) {
        if (worker_part_gids[w][p].empty()) {
          continue;
        }
        jit_dense_split.accum_k->Run(
            static_cast<uint32_t>(worker_part_gids[w][p].size()),
            worker_part_gids[w][p].data(), worker_part_vals[w][p].data(),
            sums.data(), jit_dense_split.is_avg ? counts.data() : nullptr,
            matched_bits.data(), nonnull_bits.data(), &local_inf);
        std::vector<uint32_t>().swap(worker_part_gids[w][p]);
        std::vector<double>().swap(worker_part_vals[w][p]);
      }
      if (jit_dense_split.raise_inf && local_inf != 0) {
        any_inf.store(true, std::memory_order_relaxed);
        return;
      }

      matched_lgs.clear();
      for (size_t wi = 0; wi < n_words; ++wi) {
        uint64_t mb = matched_bits[wi];
        while (mb != 0) {
          const auto bit = static_cast<uint32_t>(std::countr_zero(mb));
          mb &= mb - 1;
          matched_lgs.push_back(static_cast<uint32_t>((wi << 6) | bit));
        }
      }
      if (matched_lgs.empty()) {
        continue;
      }

      auto& out_pchunks = part_chunks[p];
      out_pchunks.reserve((matched_lgs.size() + kColumnarChunkRows - 1) /
                          kColumnarChunkRows);
      for (size_t off = 0; off < matched_lgs.size();
           off += kColumnarChunkRows) {
        const size_t cnt =
            std::min<size_t>(kColumnarChunkRows, matched_lgs.size() - off);
        ColumnarChunk chunk;
        chunk.rows = cnt;
        chunk.columns.resize(sel_n);

        std::shared_ptr<ChunkGatherIndex> right_gi;
        if (need_right_gi) {
          right_gi = std::make_shared<ChunkGatherIndex>();
          right_gi->source_table = right_col_owner;
          right_gi->src_chunks.resize(cnt);
          right_gi->src_rows.resize(cnt);
          for (size_t r = 0; r < cnt; ++r) {
            const uint64_t gid = (p << kPartShift) | matched_lgs[off + r];
            const uint32_t r_subgid =
                split_uses_right_subgid ? static_cast<uint32_t>(gid / pk0_card)
                                        : 0U;
            const uint32_t r_idx =
                split_uses_right_subgid ? split_right_rep_ptr[r_subgid] : 0U;
            const auto [rch, rr] = right_locator.Locate(r_idx);
            right_gi->src_chunks[r] = static_cast<uint32_t>(rch);
            right_gi->src_rows[r] = static_cast<uint32_t>(rr);
          }
        }

        for (size_t c = 0; c < sel_n; ++c) {
          const auto& item = jit_dense_split.sel_items[c];
          ColumnarColumn& col = chunk.columns[c];
          if (item.kind == 0) {
            col.Init(ValueType::kDouble, false, false, 0);
            col.doubles.resize(cnt);
            double* dst = col.doubles.data();
            bool any_null = false;
            for (size_t r = 0; r < cnt; ++r) {
              const uint32_t lg = matched_lgs[off + r];
              if ((nonnull_bits[lg >> 6] & (uint64_t{1} << (lg & 63))) == 0) {
                any_null = true;
                dst[r] = 0.0;
              } else {
                part_col_nonnull[p][c] = 1;
                dst[r] = jit_dense_split.is_avg
                             ? (sums[lg] / static_cast<double>(counts[lg]))
                             : sums[lg];
              }
            }
            if (any_null) {
              col.has_null = true;
              col.nulls.assign(cnt, 0);
              for (size_t r = 0; r < cnt; ++r) {
                const uint32_t lg = matched_lgs[off + r];
                if ((nonnull_bits[lg >> 6] & (uint64_t{1} << (lg & 63))) == 0) {
                  col.nulls[r] = 1;
                }
              }
            }
          } else if (item.kind == 1) {
            TrySetConstantColumnarColumn(&col, ValueType::kNull, false, false,
                                         item.constant, cnt, &chunk);
            if (!item.constant.IsNull()) {
              part_col_nonnull[p][c] = 1;
            }
          } else if (item.kind == 2) {
            if (item.vt == ValueType::kNull) {
              col.SetConstantNull(ValueType::kNull, item.is_unsigned,
                                  item.is_interval, cnt);
            } else {
              col.SetGathered(right_gi, item.idx, item.vt, item.is_unsigned,
                              item.is_interval, item.is_uniform, item.has_null);
              if (!item.has_null) {
                part_col_nonnull[p][c] = 1;
              } else {
                for (size_t r = 0; r < cnt; ++r) {
                  if (!col.IsNull(r)) {
                    part_col_nonnull[p][c] = 1;
                    break;
                  }
                }
              }
            }
          } else {
            const PreparedDeferredPkLookup& lkp = prepared_pk_lookups[item.idx];
            const auto* rep_ptrs = pk0.subgid_rep_ptr;
            const auto* blk_ptr = lkp.blocks.data();
            col.Init(item.vt, item.is_unsigned, false, 0);
            bool nonnull_seen = false;
            if (item.vt == ValueType::kVarChar) {
              col.encoding_owner = pk0.store_owner;
              chunk.AppendKeepaliveUnique(pk0.store_owner);
              col.strings.resize(cnt);
              std::string_view* dst = col.strings.data();
              for (size_t r = 0; r < cnt; ++r) {
                const auto sg = static_cast<uint32_t>(
                    ((p << kPartShift) | matched_lgs[off + r]) % pk0_card);
                const auto [nb, rin] = rep_ptrs[sg];
                const PreparedDeferredPkBlock& pb = blk_ptr[nb];
                if (pb.has_null_payload != 0 && pb.pay_blk->IsNull(rin)) {
                  if (!col.has_null) {
                    col.has_null = true;
                    col.nulls.assign(cnt, 0);
                  }
                  col.nulls[r] = 1;
                  dst[r] = {};
                } else {
                  dst[r] = pb.pay_blk->VarcharAt(rin);
                  nonnull_seen = true;
                }
              }
            } else if (item.vt == ValueType::kDouble) {
              col.doubles.resize(cnt);
              double* dst = col.doubles.data();
              for (size_t r = 0; r < cnt; ++r) {
                const auto sg = static_cast<uint32_t>(
                    ((p << kPartShift) | matched_lgs[off + r]) % pk0_card);
                const auto [nb, rin] = rep_ptrs[sg];
                const PreparedDeferredPkBlock& pb = blk_ptr[nb];
                if (pb.has_null_payload != 0 && pb.pay_blk->IsNull(rin)) {
                  if (!col.has_null) {
                    col.has_null = true;
                    col.nulls.assign(cnt, 0);
                  }
                  col.nulls[r] = 1;
                  dst[r] = 0.0;
                } else {
                  const auto dv = std::bit_cast<double>(pb.pay_cells[rin]);
                  dst[r] = (dv == 0.0) ? 0.0 : dv;
                  nonnull_seen = true;
                }
              }
            } else {
              col.ints.resize(cnt);
              int64_t* dst = col.ints.data();
              for (size_t r = 0; r < cnt; ++r) {
                const auto sg = static_cast<uint32_t>(
                    ((p << kPartShift) | matched_lgs[off + r]) % pk0_card);
                const auto [nb, rin] = rep_ptrs[sg];
                const PreparedDeferredPkBlock& pb = blk_ptr[nb];
                if (pb.has_null_payload != 0 && pb.pay_blk->IsNull(rin)) {
                  if (!col.has_null) {
                    col.has_null = true;
                    col.nulls.assign(cnt, 0);
                  }
                  col.nulls[r] = 1;
                  dst[r] = 0;
                } else {
                  dst[r] = pb.pay_cells[rin];
                  nonnull_seen = true;
                }
              }
            }
            if (nonnull_seen) {
              part_col_nonnull[p][c] = 1;
            }
          }
        }
        out_pchunks.push_back(std::move(chunk));
      }
    }
  };

  const size_t p2_workers = std::min<size_t>(workers, n_parts);
  if (p2_workers <= 1) {
    run_phase2(0);
  } else {
    RETURN_IF_FAIL(RunParallelTasks(p2_workers, run_phase2));
  }
  if (any_inf.load(std::memory_order_relaxed)) {
    return StatusError(StatusCode::kIsInfinity,
                       "SUM overflow: result is infinite");
  }

  std::vector<uint8_t> any_col_nonnull(sel_n, 0);
  for (size_t p = 0; p < n_parts; ++p) {
    for (size_t c = 0; c < sel_n; ++c) {
      any_col_nonnull[c] |= part_col_nonnull[p][c];
    }
  }

  auto out_col = std::make_shared<ColumnarTable>();
  if (right_col_owner != nullptr) {
    out_col->AppendKeepalive(right_col_owner);
  }
  if (pk0.store_owner != nullptr) {
    out_col->AppendKeepalive(pk0.store_owner);
  }
  for (size_t p = 0; p < n_parts; ++p) {
    for (ColumnarChunk& ch : part_chunks[p]) {
      if (ch.rows == 0) {
        continue;
      }
      for (size_t c = 0; c < sel_n; ++c) {
        if (any_col_nonnull[c] == 0 && jit_dense_split.sel_items[c].kind != 1) {
          ch.columns[c].SetConstantNull(ValueType::kNull, false, false,
                                        ch.rows);
        }
      }
      out_col->AppendChunk(std::move(ch));
    }
  }

  std::vector<Column> out_cols;
  out_cols.reserve(sel_n);
  const size_t out_total = out_col->total_rows;
  for (size_t c = 0; c < sel_n; ++c) {
    const auto& item = jit_dense_split.sel_items[c];
    if (out_total == 0 || any_col_nonnull[c] == 0) {
      out_cols.emplace_back(item.name, ValueType::kNull);
    } else {
      Column col_def(item.name, item.vt);
      col_def.SetUnsigned(item.is_unsigned);
      col_def.SetInterval(item.is_interval);
      out_cols.push_back(std::move(col_def));
    }
  }
  result.schema = Schema("", std::move(out_cols));
  result.SetColumnarTable(std::move(out_col), out_total > 0);
  result.hash_joins = left.hash_joins + right.hash_joins + 1;
  result.in_memory_hash_joins =
      left.in_memory_hash_joins + right.in_memory_hash_joins + 1;
  result.nested_loop_joins = left.nested_loop_joins + right.nested_loop_joins;
  result.hybrid_hash_joins = left.hybrid_hash_joins + right.hybrid_hash_joins;
  result.join_comparisons = left.join_comparisons + right.join_comparisons +
                            total_matched_rows.load(std::memory_order_relaxed);
  result.peak_intermediate_rows =
      std::max(result.peak_intermediate_rows, out_total);
  if (result.runtime() != nullptr) {
    ++result.runtime()->partitioned_aggregate_merges;
  }
  if (context.execution_runtime() != nullptr) {
    ExecutionRuntime& runtime = *context.execution_runtime();
    runtime.join_ms += ElapsedMs(join_begin);
    ++runtime.grouped_join_fusions;
  }
  fusion.fired = true;
  return std::optional<Relation>(std::move(result));
}

}  // namespace tinylamb::relational_detail
