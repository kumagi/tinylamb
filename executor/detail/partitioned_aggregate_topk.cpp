/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/parallel_work.hpp"
#include "common/platform/process.hpp"
#include "common/query_parallelism.hpp"
#include "common/status_or.hpp"
#include "executor/detail/columnar_tlcs_fast_path.hpp"
#include "executor/detail/columnar_tlcs_internal.hpp"
#include "executor/detail/partitioned_aggregate_internal.hpp"
#include "executor/detail/typed_stream_aggregate_partitioned.hpp"
#include "table/tlcs_file.hpp"
#include "type/collation.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail::partitioned_detail {
// Lean proof: lean/TinylambOpt/ClickBenchDiscovery.lean
// Lean proofs:
//   - lean/TinylambOpt/ClickBenchRound2.lean
//     (unordered_group_by_key_fiber_subset_sound,
//      unordered_group_by_nonkey_filter_corrupts_count_bug)
//   - lean/TinylambOpt/ResearchProofs/Wave9ClickBenchHighCardAndFilter.lean
//     (GroupByLimitAnyOrderEarlyStop_sound)
// CB-2: Key-fiber subset aggregation for unordered `GROUP BY ... LIMIT K`
// (ClickBench Q18: `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY
// UserID, SearchPhrase LIMIT 10`). Filtering input rows by a predicate `P(k1)`
// on an integer/date prefix key `k1` alone preserves 100% exact full-table
// aggregates for every surviving group while skipping decompression of heavy
// VARCHAR keys (`SearchPhrase`) on >99% of blocks!
StatusOr<std::optional<Relation>> TryRunKeyFiberUnorderedLimitAggregate(
    const PartitionedColumnarAggRequest& req, const KeyProjectionMap& km) {
  if (!req.order_bindings.empty() || !req.stmt.HasLimit() ||
      req.stmt.Limit() == 0 || req.any_scalar_distinct ||
      req.candidate_blocks.size() <= 8 || HasCheckedIntSum(req.agg_specs)) {
    return std::optional<Relation>{};
  }
  if (req.stmt.Offset() >
      std::numeric_limits<size_t>::max() - req.stmt.Limit()) {
    return std::optional<Relation>{};
  }
  const size_t topk_cap = req.stmt.Offset() + req.stmt.Limit();
  if (topk_cap == 0 || topk_cap > 512) {
    return std::optional<Relation>{};
  }

  // Every physical key must be infallible so skipping non-fiber rows can never
  // mask a runtime expression error.
  size_t fiber_phys_idx = SIZE_MAX;
  for (size_t p = 0; p < km.phys_specs.size(); ++p) {
    const ColKeyKind kind = km.phys_specs[p].kind;
    if (kind != ColKeyKind::kColInt64 && kind != ColKeyKind::kColDate &&
        kind != ColKeyKind::kColDouble && kind != ColKeyKind::kColVarChar &&
        kind != ColKeyKind::kCaseWhenVarChar) {
      return std::optional<Relation>{};
    }
    if (fiber_phys_idx == SIZE_MAX &&
        (kind == ColKeyKind::kColInt64 || kind == ColKeyKind::kColDate)) {
      fiber_phys_idx = p;
    }
  }
  if (fiber_phys_idx == SIZE_MAX) {
    return std::optional<Relation>{};
  }
  for (const ColAggSpec& as : req.agg_specs) {
    if (IsDistinctAggKind(as.kind)) {
      return std::optional<Relation>{};
    }
  }

  // Step 1: Seed up to `target_fiber_keys` distinct values of `fiber_col` from
  // the leading candidate blocks (decoding ONLY `preds` + `fiber_col`!).
  const size_t target_fiber_keys =
      std::min<size_t>(64, std::max<size_t>(16, topk_cap * 2));
  const ColGroupKeySpec& fiber_spec = km.phys_specs[fiber_phys_idx];
  const ColumnarStageSchedule seed_sched =
      BuildColumnarStageSchedule(req.table_schema, req.preds, {fiber_spec}, {});
  const size_t ncols = req.table_schema.ColumnCount();

  static constexpr size_t kFiberSlots = 256;
  static constexpr size_t kFiberMask = kFiberSlots - 1;
  struct FiberSlot {
    uint64_t hash{0};
    int64_t key{0};
    bool occupied{false};
  };
  std::array<FiberSlot, kFiberSlots> fiber_ht{};
  size_t fiber_count = 0;
  bool fiber_has_null = false;
  int64_t fiber_min = std::numeric_limits<int64_t>::max();
  int64_t fiber_max = std::numeric_limits<int64_t>::min();
  bool exhausted_all_blocks = true;

  auto fiber_contains_nonnull = [&](int64_t v) noexcept -> bool {
    if (v < fiber_min || v > fiber_max) {
      return false;
    }
    const uint64_t h = MixHash64(static_cast<uint64_t>(v));
    size_t pos = static_cast<size_t>(h) & kFiberMask;
    while (fiber_ht[pos].occupied) {
      if (fiber_ht[pos].hash == h && fiber_ht[pos].key == v) {
        return true;
      }
      pos = (pos + 1) & kFiberMask;
    }
    return false;
  };

  auto fiber_insert_nonnull = [&](int64_t v) noexcept -> bool {
    const uint64_t h = MixHash64(static_cast<uint64_t>(v));
    size_t pos = static_cast<size_t>(h) & kFiberMask;
    while (fiber_ht[pos].occupied) {
      if (fiber_ht[pos].hash == h && fiber_ht[pos].key == v) {
        return false;
      }
      pos = (pos + 1) & kFiberMask;
    }
    fiber_ht[pos] = {.hash = h, .key = v, .occupied = true};
    fiber_min = std::min(fiber_min, v);
    fiber_max = std::max(fiber_max, v);
    ++fiber_count;
    return true;
  };

  {
    BlockDecodeScratch seed_scratch;
    std::vector<const TlcsFile::DecodedColumnChunk*> col_ptrs(ncols, nullptr);
    std::vector<uint32_t> cand_rows;
    uint64_t dummy_scanned = 0;
    const size_t max_seed_blocks =
        std::min<size_t>(8, req.candidate_blocks.size());
    size_t bi = 0;
    for (; bi < max_seed_blocks; ++bi) {
      ASSIGN_OR_RETURN(
          bool, any_rows,
          DecodeAndFilterBlock(*req.tlcs, req.candidate_blocks[bi],
                               req.table_schema, req.preds, seed_sched,
                               &seed_scratch, &col_ptrs, &cand_rows,
                               &dummy_scanned));
      if (!any_rows) {
        continue;
      }
      const auto* fch = col_ptrs[fiber_spec.full_col];
      const bool no_null = !fch->has_null;
      for (const uint32_t r : cand_rows) {
        if (!no_null && fch->IsNull(r)) {
          if (!fiber_has_null) {
            fiber_has_null = true;
            ++fiber_count;
          }
        } else {
          (void)fiber_insert_nonnull(fch->int_cells[r]);
        }
        if (fiber_count >= target_fiber_keys) {
          exhausted_all_blocks = false;
          break;
        }
      }
      if (fiber_count >= target_fiber_keys) {
        break;
      }
    }
    if (bi < req.candidate_blocks.size() && fiber_count < target_fiber_keys) {
      exhausted_all_blocks = false;
    }
  }

  if (fiber_count == 0) {
    return std::optional<Relation>{};
  }
  if (!exhausted_all_blocks && fiber_count < std::min<size_t>(4, topk_cap)) {
    return std::optional<Relation>{};
  }

  // Step 2: Zone-map prune `candidate_blocks` on `fiber_col`!
  std::vector<uint32_t> fiber_blocks;
  if (!fiber_has_null && fiber_min <= fiber_max) {
    const uint32_t dg_count = req.tlcs->DirGroupCount();
    const uint32_t dg_size = req.tlcs->DirGroupSize();
    std::vector<std::shared_ptr<const TlcsFile::DirGroup>> dir_groups(dg_count);
    for (uint32_t g = 0; g < dg_count; ++g) {
      ASSIGN_OR_RETURN(std::shared_ptr<const TlcsFile::DirGroup>, grp,
                       req.tlcs->GetDirGroup(g));
      dir_groups[g] = std::move(grp);
    }
    const slot_t fcol = fiber_spec.full_col;
    fiber_blocks.reserve(req.candidate_blocks.size());
    for (const uint32_t bidx : req.candidate_blocks) {
      const auto& blk = dir_groups[bidx / dg_size]->blocks[bidx % dg_size];
      const auto& m = blk.columns[fcol];
      if (m.encoding == TlcsColumnEncoding::kConstNull) {
        continue;
      }
      if (m.has_min_max && (m.max_int < fiber_min || m.min_int > fiber_max)) {
        continue;
      }
      fiber_blocks.push_back(bidx);
    }
  } else {
    fiber_blocks = req.candidate_blocks;
  }

  // Determine remaining payload columns (`SearchPhrase`, `agg_specs`, etc.)
  // decoded ONLY on blocks where `fiber_rows` is non-empty!
  std::vector<uint8_t> in_seed(ncols, 0);
  for (const slot_t c : seed_sched.stage1a_cols) {
    in_seed[c] = 1;
  }
  for (const slot_t c : seed_sched.stage1b_cols) {
    in_seed[c] = 1;
  }
  for (const slot_t c : seed_sched.stage2_cols) {
    in_seed[c] = 1;
  }
  for (const slot_t c : seed_sched.cond_cols) {
    in_seed[c] = 1;
  }
  std::vector<uint8_t> extra_need(ncols, 0);
  std::vector<uint8_t> extra_bytes(ncols, 0);
  for (const auto& ks : km.phys_specs) {
    if (ks.kind == ColKeyKind::kConstInt64) {
      continue;
    }
    extra_need[ks.full_col] = 1;
    if (req.table_schema.GetColumn(ks.full_col).Type() == ValueType::kVarChar) {
      extra_bytes[ks.full_col] = 1;
    }
    for (const auto& cc : ks.case_conditions) {
      extra_need[cc.left] = 1;
      if (!cc.constant_payload) {
        extra_need[cc.right] = 1;
      }
    }
  }
  for (const ColAggSpec& as : req.agg_specs) {
    if (as.kind == ColAggKind::kCountStar) {
      continue;
    }
    extra_need[as.full_col] = 1;
    if (as.needs_bytes) {
      extra_bytes[as.full_col] = 1;
    }
  }
  ColumnarStageSchedule scan_sched = seed_sched;
  for (size_t i = 0; i < scan_sched.stage1b_cols.size(); ++i) {
    if (extra_bytes[scan_sched.stage1b_cols[i]] != 0U) {
      scan_sched.stage1b_len_only[i] = 0;
    }
  }
  for (size_t i = 0; i < scan_sched.stage2_cols.size(); ++i) {
    if (extra_bytes[scan_sched.stage2_cols[i]] != 0U) {
      scan_sched.stage2_len_only[i] = 0;
    }
  }
  std::vector<slot_t> extra_cols;
  std::vector<uint8_t> extra_len_only;
  extra_cols.reserve(ncols);
  extra_len_only.reserve(ncols);
  for (size_t c = 0; c < ncols; ++c) {
    if (extra_need[c] != 0U && in_seed[c] == 0U) {
      extra_cols.push_back(static_cast<slot_t>(c));
      extra_len_only.push_back(extra_bytes[c] != 0U ? 0 : 1);
    }
  }

  const size_t workers = std::max<size_t>(
      1, std::min<size_t>(
             {QueryParallelWorkerLimit(std::thread::hardware_concurrency()),
              std::max<size_t>(1, fiber_blocks.size()), size_t{16}}));
  std::vector<QueryMemoryCharge> w_charges(workers);
  std::vector<std::unique_ptr<WorkerGroupTable>> w_tables;
  w_tables.reserve(workers);
  for (size_t w = 0; w < workers; ++w) {
    w_tables.push_back(std::make_unique<WorkerGroupTable>(
        &km.phys_specs, &req.agg_specs, &w_charges[w]));
  }
  std::vector<Status> w_status(workers, Status::kSuccess);
  std::atomic<size_t> next_b{0};
  std::atomic<uint64_t> total_scanned{0};
  std::atomic<uint64_t> total_matching{0};
  const size_t n_phys = km.phys_specs.size();

  RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t tid) {
    BlockDecodeScratch scratch;
    TlcsFile::DecodedBlock blk_extra;
    std::vector<uint8_t> io_extra;
    std::vector<const TlcsFile::DecodedColumnChunk*> col_ptrs(ncols, nullptr);
    std::vector<uint32_t> cand_rows;
    std::vector<uint32_t> fiber_rows;
    std::vector<RuntimeKeyCell> probe_keys(n_phys);
    std::string regex_scratch;
    TimestampPrefixCache ts_cache;
    RegexReplaceCache regex_cache;
    uint64_t local_scanned = 0;
    uint64_t local_matching = 0;
    WorkerGroupTable& wt = *w_tables[tid];

    while (true) {
      const size_t bi = next_b.fetch_add(1, std::memory_order_relaxed);
      if (bi >= fiber_blocks.size()) {
        break;
      }
      const uint32_t bidx = fiber_blocks[bi];
      auto dec_or = DecodeAndFilterBlock(*req.tlcs, bidx, req.table_schema,
                                         req.preds, scan_sched, &scratch,
                                         &col_ptrs, &cand_rows, &local_scanned);
      if (!dec_or.HasValue()) {
        w_status[tid] = dec_or.GetStatus();
        return;
      }
      if (!dec_or.Value()) {
        continue;
      }
      local_matching += cand_rows.size();

      const auto* fch = col_ptrs[fiber_spec.full_col];
      const bool no_null = !fch->has_null;
      fiber_rows.clear();
      for (const uint32_t r : cand_rows) {
        if (!no_null && fch->IsNull(r)) {
          if (fiber_has_null) {
            fiber_rows.push_back(r);
          }
        } else if (fiber_contains_nonnull(fch->int_cells[r])) {
          fiber_rows.push_back(r);
        }
      }
      if (fiber_rows.empty()) {
        continue;
      }

      if (!extra_cols.empty()) {
        Status est = req.tlcs->DecodeBlockColumnsWithOptions(
            bidx, extra_cols, req.table_schema, &blk_extra, true, &io_extra,
            extra_len_only);
        if (est != Status::kSuccess) {
          w_status[tid] = std::move(est);
          return;
        }
        for (size_t i = 0; i < extra_cols.size(); ++i) {
          col_ptrs[extra_cols[i]] = &blk_extra.columns[i];
        }
      }

      for (const uint32_t r : fiber_rows) {
        Status kst = Status::kSuccess;
        for (size_t k = 0; k < n_phys; ++k) {
          if (!ExtractSingleKeyCell(km.phys_specs[k], col_ptrs, r,
                                    &probe_keys[k], &regex_scratch, &ts_cache,
                                    &kst, &regex_cache)) {
            w_status[tid] = std::move(kst);
            return;
          }
        }
        const uint64_t kh =
            GroupKeySliceHasher::HashTyped(probe_keys.data(), km.phys_specs);
        auto gid_or = wt.FindOrInsert(probe_keys.data(), kh);
        if (!gid_or.HasValue()) {
          w_status[tid] = gid_or.GetStatus();
          return;
        }
        RuntimeAggState* ast = wt.AggsAt(gid_or.Value());
        for (size_t a = 0; a < req.agg_specs.size(); ++a) {
          Status fst = FoldRowIntoAggState(req.agg_specs[a], col_ptrs, r,
                                           ast[a], wt.Arena());
          if (fst != Status::kSuccess) {
            w_status[tid] = std::move(fst);
            return;
          }
        }
      }
    }
    total_scanned.fetch_add(local_scanned, std::memory_order_relaxed);
    total_matching.fetch_add(local_matching, std::memory_order_relaxed);
  }));

  for (const Status& st : w_status) {
    if (st == StatusCode::kInvalidArgument ||
        st == StatusCode::kMemoryLimitExceeded) {
      return std::optional<Relation>{};
    }
    RETURN_IF_FAIL(st);
  }

  const auto merge_begin = std::chrono::steady_clock::now();
  QueryMemoryCharge merge_charge;
  WorkerGroupTable merged(&km.phys_specs, &req.agg_specs, &merge_charge);
  size_t est_total = 0;
  for (const auto& wt : w_tables) {
    est_total += wt->NumGroups();
  }
  Status rst = merged.Reserve(std::max<size_t>(16, est_total));
  if (rst == StatusCode::kMemoryLimitExceeded) {
    return std::optional<Relation>{};
  }
  RETURN_IF_FAIL(rst);
  for (size_t w = 0; w < workers; ++w) {
    WorkerGroupTable& src = *w_tables[w];
    for (size_t g = 0; g < src.NumGroups(); ++g) {
      Status mst = merged.MergeFromGroup(src.KeysAt(g), src.HashAt(g),
                                         {src.AggsAt(g), req.agg_specs.size()},
                                         /*borrow_strings=*/true);
      if (mst == StatusCode::kMemoryLimitExceeded) {
        return std::optional<Relation>{};
      }
      RETURN_IF_FAIL(mst);
    }
  }

  std::vector<std::vector<SurvivorGroupRow>> single_surv(1);
  std::vector<std::unique_ptr<QueryMemoryCharge>> survivor_charges(1);
  QueryMemoryCharge& survivor_charge = SurvivorChargeFor(survivor_charges, 0);
  auto& out_vec = single_surv[0];
  std::vector<RuntimeKeyCell> full_keys(req.key_specs.size());
  std::vector<Value> finished_aggs(req.agg_specs.size());
  std::vector<Value> cached_keys(req.key_specs.size());
  std::vector<Value> having_scratch(req.agg_specs.size());

  const size_t ng = merged.NumGroups();
  for (size_t g = 0; g < ng; ++g) {
    const RuntimeAggState* ast = merged.AggsAt(g);
    ASSIGN_OR_RETURN(
        bool, passes,
        EvalPositionalHavingOnStates(req.positional_having, req.agg_specs, ast,
                                     &having_scratch));
    if (!passes) {
      continue;
    }
    km.ReconstructFull(req.key_specs, merged.KeysAt(g), full_keys.data());
    for (size_t a = 0; a < req.agg_specs.size(); ++a) {
      ASSIGN_OR_RETURN(Value, finished_value,
                       (FinishAggState(req.agg_specs[a], ast[a])));
      finished_aggs[a] = std::move(finished_value);
    }
    std::fill(cached_keys.begin(), cached_keys.end(), Value());
    SurvivorGroupRow row;
    row.emit_vals.reserve(req.emit_bindings.size());
    for (const auto& eb : req.emit_bindings) {
      ASSIGN_OR_RETURN(Value, ev,
                       EvaluateBindingValue(eb, req.key_specs, full_keys.data(),
                                            finished_aggs, &cached_keys));
      row.emit_vals.push_back(std::move(ev));
    }
    PushChargedSurvivor(&out_vec, &survivor_charge, std::move(row));
    if (out_vec.size() >= topk_cap) {
      break;
    }
  }

  if (out_vec.size() < topk_cap && !exhausted_all_blocks) {
    return std::optional<Relation>{};
  }

  return FinalizePartitionedRelation(req, single_surv, ng, total_scanned.load(),
                                     total_matching.load(), merge_begin,
                                     "key-fiber-limit", &survivor_charges);
}

// Lean proof: lean/TinylambOpt/ClickBenchRound2.lean
//   (quotient_bucket_count_ge_group_count, quotient_bucket_topk_pruning_sound,
//    quotient_bucket_asc_order_pruning_bug)
// CB-7: Exact heavy-hitter pruning via quotient hash-bucket upper bounds for
// `GROUP BY keys ORDER BY COUNT(*) DESC LIMIT K` (ClickBench Q13, Q15, Q17,
// Q19, Q34, Q35, Q37, Q38). Eliminates multi-GB VARCHAR slab serialization and
// disk spilling by counting quotient buckets in L2/L3 cache first (2^16 buckets
// for 16..127 filtered blocks, 2^20 buckets for >= 128 blocks), then
// aggregating exact key tuples only for the <= 2,048 heaviest buckets and
// certifying `tau > max_bucket_outside_S` before returning!
// Lean proofs:
//   lean/TinylambOpt/ResearchProofs/DuckDBGaps/Hits.lean
//     (CancellationDebt_sound, CertifiedWinners_sound)
StatusOr<std::optional<Relation>> TryRunQuotientBucketHeavyHittersTopK(
    const PartitionedColumnarAggRequest& req, const KeyProjectionMap& km) {
  if (!req.stmt.HasLimit() || req.stmt.Limit() == 0 ||
      !req.positional_having.empty() || req.any_scalar_distinct ||
      req.agg_specs.size() != 1 ||
      req.agg_specs[0].kind != ColAggKind::kCountStar ||
      req.order_bindings.empty() ||
      req.order_bindings[0].binding.kind != OutputSlotBinding::Kind::kAgg ||
      req.order_bindings[0].binding.index != 0 ||
      req.order_bindings[0].order.Ascending() ||
      req.candidate_blocks.size() < 16) {
    return std::optional<Relation>{};
  }
  if (req.stmt.Offset() >
      std::numeric_limits<size_t>::max() - req.stmt.Limit()) {
    return std::optional<Relation>{};
  }
  const size_t topk_cap = req.stmt.Offset() + req.stmt.Limit();
  if (topk_cap == 0 || topk_cap > 256) {
    return std::optional<Relation>{};
  }

  bool any_varchar_key = false;
  for (const ColGroupKeySpec& ks : km.phys_specs) {
    if (IsVarCharKeyKind(ks.kind)) {
      any_varchar_key = true;
      break;
    }
  }
  const bool single_raw_int_key =
      km.phys_specs.size() == 1 &&
      (km.phys_specs[0].kind == ColKeyKind::kColInt64 ||
       km.phys_specs[0].kind == ColKeyKind::kColDate);
  const bool numeric_pair =
      km.phys_specs.size() == 2 &&
      std::ranges::all_of(km.phys_specs, [&](const auto& key) {
        return (key.kind == ColKeyKind::kColInt64 ||
                key.kind == ColKeyKind::kColDate) &&
               !req.table_schema.GetColumn(key.full_col).IsUnsigned();
      });
  const bool try_duplicate_sieve =
      numeric_pair && req.order_bindings.size() == 1 &&
      req.candidate_blocks.size() >= 512 &&
      std::ranges::none_of(req.emit_bindings,
                           [](const auto& item) {
                             return item.kind ==
                                    OutputSlotBinding::Kind::kPostAgg;
                           }) &&
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_TLCS_DUPLICATE_SIEVE") ==
          nullptr;
  bool near_unique = false;
  if (try_duplicate_sieve) {
    // The sampled column is sufficient to estimate pair uniqueness. A
    // sparse sample may miss duplicates, so it only selects the algorithm.
    constexpr size_t kBlocks = 8;
    constexpr size_t kRowsPerBlock = 1024;
    const std::array columns{km.phys_specs[0].full_col};
    TlcsFile::DecodedBlock sample;
    std::vector<uint8_t> scratch;
    std::vector<int64_t> keys;
    keys.reserve(kBlocks * kRowsPerBlock);
    size_t sampled_rows = 0;
    for (size_t i = 0; i < kBlocks; ++i) {
      const uint32_t block =
          req.candidate_blocks[(i * req.candidate_blocks.size()) / kBlocks];
      RETURN_IF_FAIL(req.tlcs->DecodeBlockColumnsWithOptions(
          block, columns, req.table_schema, &sample, true, &scratch, {}, true));
      if (sample.row_count == 0) {
        continue;
      }
      const auto& col = sample.columns[0];
      const size_t rows = std::min<size_t>(sample.row_count, kRowsPerBlock);
      sampled_rows += rows;
      for (size_t r = 0; r < rows; ++r) {
        if (!col.IsNull(r)) {
          keys.push_back(col.int_cells[r]);
        }
      }
    }
    std::ranges::sort(keys);
    const auto duplicates = std::ranges::unique(keys);
    const size_t nulls = sampled_rows - keys.size();
    near_unique =
        keys.size() >= kRowsPerBlock &&
        nulls + static_cast<size_t>(duplicates.size()) <= (sampled_rows / 100);
  }
  const bool try_numeric_quotient =
      numeric_pair && !near_unique && req.order_bindings.size() == 1 &&
      req.candidate_blocks.size() >= 512 &&
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_QUOTIENT_FUSION") ==
          nullptr;
  if (!any_varchar_key && !single_raw_int_key && !near_unique &&
      !try_numeric_quotient) {
    return std::optional<Relation>{};
  }

  const size_t bucket_bits = (req.candidate_blocks.size() < 128) ? 16U : 20U;
  const size_t num_buckets = size_t{1} << bucket_bits;
  const size_t bucket_mask = num_buckets - 1;
  static constexpr size_t kHeavyBuckets = 2048;

  const size_t workers = std::max<size_t>(
      1, std::min<size_t>(
             {QueryParallelWorkerLimit(std::thread::hardware_concurrency()),
              req.candidate_blocks.size(), size_t{16}}));

  QueryMemoryCharge sketch_charge;
  const size_t reserve_bytes = (workers * num_buckets * sizeof(uint32_t)) +
                               (num_buckets * sizeof(uint64_t)) +
                               (num_buckets / 8);
  if (!sketch_charge.TryAdd(reserve_bytes)) {
    return std::optional<Relation>{};
  }

  const size_t n_phys = km.phys_specs.size();
  const size_t ncols = req.table_schema.ColumnCount();
  const ColumnarStageSchedule sched = BuildColumnarStageSchedule(
      req.table_schema, req.preds, km.phys_specs, req.agg_specs);
  const ColumnarStageSchedule leading_key_sched =
      near_unique
          ? BuildColumnarStageSchedule(req.table_schema, req.preds,
                                       {km.phys_specs[0]}, req.agg_specs)
          : ColumnarStageSchedule{};
  const bool single_raw_varchar_key =
      n_phys == 1 && km.phys_specs[0].kind == ColKeyKind::kColVarChar;

  // The directory counts also cover repeated block references. Prove the
  // largest possible counter up front, so every increment in either pass
  // is exact without a saturation check in the row loop.
  // Lean proof: lean/TinylambOpt/QuotientFusion.lean (bucket_increment_fits)
  uint64_t input_bound = 0;
  std::vector<uint64_t> block_row_starts;
  const bool store_block_starts = near_unique && req.preds.Empty();
  if (store_block_starts) {
    block_row_starts.reserve(req.candidate_blocks.size() + 1);
  }
  for (uint32_t block : req.candidate_blocks) {
    if (store_block_starts) {
      block_row_starts.push_back(input_bound);
    }
    ASSIGN_OR_RETURN(uint32_t, count, req.tlcs->BlockRowCount(block));
    input_bound += count;
    if (input_bound > UINT32_MAX) {
      return std::optional<Relation>{};
    }
  }

  using GroupTables = std::vector<std::unique_ptr<WorkerGroupTable>>;
  const auto make_tables = [&](std::vector<QueryMemoryCharge>& charges) {
    GroupTables tables;
    tables.reserve(workers);
    for (auto& charge : charges) {
      tables.push_back(std::make_unique<WorkerGroupTable>(
          &km.phys_specs, &req.agg_specs, &charge));
    }
    return tables;
  };
  const auto has_bucket = [](std::span<const uint64_t> mask, size_t bucket) {
    return ((mask[bucket / 64] >> (bucket % 64)) & 1) != 0;
  };
  const auto bucket_of = [bucket_mask](uint64_t hash) {
    return static_cast<size_t>((hash >> 32U) ^ hash) & bucket_mask;
  };
  const bool direct = req.preds.Empty() && sched.cond_cols.empty() &&
                      sched.stage1a_cols.empty() &&
                      sched.stage1c_cols.empty() && sched.stage2_cols.empty() &&
                      !sched.stage1b_cols.empty();
  const bool specialize =
      direct && ::tinylamb::platform::GetEnv(
                    "TINYLAMB_DISABLE_QUOTIENT_FUSION") == nullptr;
  const auto string_fingerprint_jit =
      specialize && single_raw_varchar_key && input_bound >= (1U << 24)
          ? JitStringFingerprint::Compile()
          : std::nullopt;
  const auto count_group = [&](WorkerGroupTable& table,
                               const RuntimeKeyCell* keys,
                               uint64_t bucket_hash) -> Status {
    // The shortlist may deliberately use a coarse fingerprint. Rehash the
    // selected bytes or key tuple for the exact table to keep probes short.
    const uint64_t hash =
        (string_fingerprint_jit || near_unique)
            ? (km.phys_specs.size() == 1
                   ? GroupKeySliceHasher::HashSingle(*keys,
                                                     km.phys_specs.front())
                   : GroupKeySliceHasher::HashTyped(keys, km.phys_specs))
            : bucket_hash;
    ASSIGN_OR_RETURN(uint32_t, group, table.FindOrInsert(keys, hash));
    auto* state = table.AggsAt(group);
    ++state[0].count;
    state[0].has_value = true;
    return Status::kSuccess;
  };
  const RuntimeKeyCell null_cell{.is_null = true};
  const RuntimeKeyCell empty_cell;
  const uint64_t null_hash =
      single_raw_varchar_key || single_raw_int_key || near_unique
          ? GroupKeySliceHasher::HashTyped(&null_cell, {km.phys_specs.front()})
          : 0;
  const uint64_t empty_hash =
      string_fingerprint_jit
          ? JitStringFingerprint::Scalar({}, GroupKeySliceHasher::kSeed)
      : single_raw_varchar_key
          ? GroupKeySliceHasher::HashTyped(&empty_cell, km.phys_specs)
          : 0;

  std::atomic<uint64_t> total_scanned{0};
  std::atomic<uint64_t> total_matching{0};
  const bool read_projected_streams_directly =
      specialize && sched.stage1b_cols.size() <= 4;
  const auto group_hash_jit =
      direct && near_unique ? JitGroupHash::Compile() : std::nullopt;
  std::optional<JitDatePartHash> date_part_jit;
  std::shared_ptr<const std::vector<TlcsFile::VarcharBlockBound>> date_bounds;
  if (specialize && n_phys == 3 && req.candidate_blocks.size() >= 512 &&
      (km.phys_specs[0].kind == ColKeyKind::kColInt64 ||
       km.phys_specs[0].kind == ColKeyKind::kColDate) &&
      km.phys_specs[1].kind == ColKeyKind::kUtcTimestampExtract &&
      km.phys_specs[2].kind == ColKeyKind::kColVarChar) {
    const auto& date = km.phys_specs[1];
    date_part_jit = JitDatePartHash::Compile(
        date.trunc_unit == UtcTimestampTruncUnit::kHour ? 11 : 14);
    if (date_part_jit) {
      ASSIGN_OR_RETURN(decltype(date_bounds), bounds,
                       req.tlcs->GetOrComputeVarcharBlockBounds(
                           date.full_col, req.table_schema));
      date_bounds = std::move(bounds);
    }
  }
  struct SavedSelection {
    std::vector<uint16_t> rows;
    bool complete{false};
  };
  std::vector<SavedSelection> selections;
  QueryMemoryCharge selection_charge;
  std::atomic<size_t> selection_bytes{0};
  constexpr size_t kSelectionBudget = size_t{64} << 20;
  if (near_unique && !req.preds.Empty() &&
      selection_charge.TryAdd(kSelectionBudget + (req.candidate_blocks.size() *
                                                  sizeof(SavedSelection)))) {
    selections.resize(req.candidate_blocks.size());
  }
  const auto scan_keys =
      [&](std::span<const uint32_t> blocks, bool report, auto&& emit,
          bool leading_only = false,
          std::span<const uint64_t> prefetch_words = {},
          std::span<const uint64_t> replay_rows = {}) -> Status {
    const size_t active_workers = std::min(workers, blocks.size());
    const auto& scan_sched = leading_only ? leading_key_sched : sched;
    std::vector<Status> errors(active_workers, Status::kSuccess);
    std::atomic<size_t> next{0};
    RETURN_IF_FAIL(RunParallelTasks(active_workers, [&](size_t worker) {
      BlockDecodeScratch scratch;
      std::vector<const TlcsFile::DecodedColumnChunk*> columns(ncols, nullptr);
      std::vector<uint32_t> candidates;
      std::vector<RuntimeKeyCell> keys(n_phys);
      TimestampPrefixCache timestamps;
      RegexReplaceCache regexes;
      std::string regex_scratch;
      uint64_t scanned = 0;
      uint64_t matching = 0;
      constexpr size_t kHashBatch = 128;
      std::array<uint64_t, kHashBatch> hashes{};
      std::array<int64_t, kHashBatch> date_parts{};

      for (size_t i = next.fetch_add(1, std::memory_order_relaxed);
           i < blocks.size();
           i = next.fetch_add(1, std::memory_order_relaxed)) {
        const uint32_t block = blocks[i];
        const auto emit_row = [&](const RuntimeKeyCell* cells, uint64_t hash,
                                  uint32_t row) {
          if constexpr (std::is_invocable_v<decltype(emit), size_t,
                                            const RuntimeKeyCell*, uint64_t,
                                            size_t, uint32_t>) {
            return emit(worker, cells, hash, i, row);
          } else {
            return emit(worker, cells, hash);
          }
        };
        const auto visit = [&](auto rows) -> Status {
          if (single_raw_varchar_key) {
            const auto& chunk = *columns[km.phys_specs[0].full_col];
            const auto visit_text =
                [&]<bool Nullable, bool Encoded>() -> Status {
              // Encoded rows can reuse the hash by code. For raw strings, a
              // pointer-cache probe would only pay off for repeated empties.
              std::array<std::optional<uint64_t>, Encoded ? 256 : 0> hashes{};
              for (uint32_t row : rows) {
                if constexpr (Nullable) {
                  if (chunk.IsNull(row)) {
                    RETURN_IF_FAIL(emit_row(&null_cell, null_hash,
                                            static_cast<uint32_t>(row)));
                    continue;
                  }
                }
                const auto text = chunk.VarcharAt(row);
                const RuntimeKeyCell key{
                    .str_ptr = text.data(),
                    .str_len = static_cast<uint32_t>(text.size())};
                const auto hash_text = [&] {
                  if (text.empty()) {
                    return empty_hash;
                  }
                  return string_fingerprint_jit
                             ? JitStringFingerprint::Scalar(
                                   text, GroupKeySliceHasher::kSeed)
                             : GroupKeySliceHasher::HashSingleVarChar(text);
                };
                uint64_t hash = 0;
                if constexpr (Encoded) {
                  if (row < chunk.dict_codes.size()) {
                    auto& cached = hashes[chunk.dict_codes[row]];
                    if (!cached) cached = hash_text();
                    hash = *cached;
                  } else {
                    hash = empty_hash;
                  }
                } else {
                  hash = hash_text();
                }
                RETURN_IF_FAIL(emit_row(&key, hash, row));
              }
              return Status::kSuccess;
            };
            if (chunk.dict_encoded && chunk.dict != nullptr) {
              return chunk.has_null
                         ? visit_text.template operator()<true, true>()
                         : visit_text.template operator()<false, true>();
            }
            return chunk.has_null
                       ? visit_text.template operator()<true, false>()
                       : visit_text.template operator()<false, false>();
          }
          if (single_raw_int_key || leading_only) {
            const auto& chunk = *columns[km.phys_specs[0].full_col];
            const auto visit_int = [&]<bool Nullable>() -> Status {
              for (uint32_t row : rows) {
                if constexpr (Nullable) {
                  if (chunk.IsNull(row)) {
                    RETURN_IF_FAIL(emit_row(&null_cell, null_hash,
                                            static_cast<uint32_t>(row)));
                    continue;
                  }
                }
                const RuntimeKeyCell key{.int_val = chunk.int_cells[row]};
                const uint64_t hash =
                    MixHash64(GroupKeySliceHasher::kSeed ^
                              MixHash64(static_cast<uint64_t>(key.int_val)));
                RETURN_IF_FAIL(emit_row(&key, hash, row));
              }
              return Status::kSuccess;
            };
            return chunk.has_null ? visit_int.template operator()<true>()
                                  : visit_int.template operator()<false>();
          }
          if (numeric_pair) {
            const auto& a = *columns[km.phys_specs[0].full_col];
            const auto& b = *columns[km.phys_specs[1].full_col];
            const auto visit_pair = [&]<bool Nullable>() -> Status {
              for (const uint32_t row : rows) {
                const std::array<RuntimeKeyCell, 2> pair{{
                    {.is_null = Nullable && a.IsNull(row),
                     .int_val = a.int_cells[row]},
                    {.is_null = Nullable && b.IsNull(row),
                     .int_val = b.int_cells[row]},
                }};
                // The first key alone is a sound bucket key. The resident
                // table below still tests both cells for equality.
                const uint64_t h =
                    !near_unique ? GroupKeySliceHasher::HashTyped(pair.data(),
                                                                  km.phys_specs)
                    : pair[0].is_null
                        ? null_hash
                        : MixHash64(GroupKeySliceHasher::kSeed ^
                                    MixHash64(static_cast<uint64_t>(
                                        pair[0].int_val)));
                RETURN_IF_FAIL(emit_row(pair.data(), h, row));
              }
              return Status::kSuccess;
            };
            return a.has_null || b.has_null
                       ? visit_pair.template operator()<true>()
                       : visit_pair.template operator()<false>();
          }
          for (uint32_t row : rows) {
            Status status = Status::kSuccess;
            for (size_t k = 0; k < n_phys; ++k) {
              if (!ExtractSingleKeyCell(km.phys_specs[k], columns, row,
                                        &keys[k], &regex_scratch, &timestamps,
                                        &status, &regexes)) {
                return status;
              }
            }
            const uint64_t hash =
                GroupKeySliceHasher::HashTyped(keys.data(), km.phys_specs);
            RETURN_IF_FAIL(emit_row(keys.data(), hash, row));
          }
          return Status::kSuccess;
        };

        Status status = Status::kSuccess;
        if (!leading_only && !selections.empty() && selections[i].complete) {
          // The first scan saved the exact predicate survivors from this
          // snapshot. Decode only their keys during the replay.
          const auto& saved = selections[i].rows;
          if (saved.empty()) {
            continue;
          }
          const std::array key_columns{km.phys_specs[0].full_col,
                                       km.phys_specs[1].full_col};
          status = req.tlcs->DecodeBlockColumnsWithOptions(
              block, key_columns, req.table_schema, &scratch.blk1b, true,
              &scratch.io1b, {}, read_projected_streams_directly);
          if (status.ok()) {
            for (size_t col = 0; col < key_columns.size(); ++col) {
              columns[key_columns[col]] = &scratch.blk1b.columns[col];
            }
            status = visit(std::span<const uint16_t>(saved));
          }
        } else if (direct) {
          // Drive the key loop with an implicit row-index range. For an
          // unfiltered query we can omit the identity selection vector.
          status = req.tlcs->DecodeBlockColumnsWithOptions(
              block, scan_sched.stage1b_cols, req.table_schema, &scratch.blk1b,
              true, &scratch.io1b, scan_sched.stage1b_len_only,
              read_projected_streams_directly);
          if (status.ok()) {
            for (size_t col = 0; col < scan_sched.stage1b_cols.size(); ++col) {
              columns[scan_sched.stage1b_cols[col]] =
                  &scratch.blk1b.columns[col];
            }
            const uint32_t count = scratch.blk1b.row_count;
            scanned += count;
            matching += count;
            const auto* a = columns[km.phys_specs[0].full_col];
            const auto* b = near_unique && !leading_only
                                ? columns[km.phys_specs[1].full_col]
                                : nullptr;
            if (!replay_rows.empty() && count != 0) {
              candidates.clear();
              const uint64_t start = block_row_starts[i];
              const uint64_t last = start + count - 1;
              for (size_t word = start / 64; word <= last / 64; ++word) {
                uint64_t mask = replay_rows[word];
                if (word == start / 64) {
                  mask &= UINT64_MAX << (start % 64);
                }
                if (word == last / 64) {
                  mask &= UINT64_MAX >> (63 - (last % 64));
                }
                while (mask != 0) {
                  candidates.push_back(static_cast<uint32_t>(
                      (word * 64) + std::countr_zero(mask) - start));
                  mask &= mask - 1;
                }
              }
              status = visit(std::span<const uint32_t>(candidates));
            } else if (km.phys_specs.size() == 1 && string_fingerprint_jit &&
                       !a->dict_encoded &&
                       a->varchar_ends.size() == count) {
              constexpr uint32_t first = 0;
              for (size_t base = 0; base < count && status.ok();) {
                const size_t n =
                    base == 0 ? 1 : std::min(kHashBatch, count - base);
                const uint32_t* starts =
                    base == 0 ? &first : a->varchar_ends.data() + base - 1;
                string_fingerprint_jit->Hash(
                    reinterpret_cast<const char*>(a->varchar_bytes.data()),
                    a->varchar_bytes.size(), starts,
                    a->varchar_ends.data() + base, hashes.data(), n,
                    GroupKeySliceHasher::kSeed);
                for (size_t lane = 0; lane < n; ++lane) {
                  const auto row = base + lane;
                  if (a->has_null && a->IsNull(row)) {
                    status = emit_row(&null_cell, null_hash,
                                      static_cast<uint32_t>(row));
                  } else {
                    const auto value = a->VarcharAt(row);
                    keys[0] = RuntimeKeyCell{
                        .str_ptr = value.data(),
                        .str_len = static_cast<uint32_t>(value.size())};
                    status = emit_row(keys.data(), hashes[lane],
                                      static_cast<uint32_t>(row));
                  }
                  if (!status.ok()) {
                    break;
                  }
                }
                base += n;
              }
            } else if (date_bounds &&
                       (*date_bounds)[block].all_canonical_utc_ts &&
                       !a->has_null) {
              const auto* date = columns[km.phys_specs[1].full_col];
              const auto* text = columns[km.phys_specs[2].full_col];
              if (!date->has_null && !date->dict_encoded && !text->has_null &&
                  date->varchar_ends.size() == count) {
                const uint32_t first = 0;
                const uint64_t empty = HashView64({});
                size_t base = 0;
                while (base < count && status.ok()) {
                  const size_t n =
                      base == 0 ? 1 : std::min(kHashBatch, count - base);
                  const uint32_t* starts =
                      base == 0 ? &first : date->varchar_ends.data() + base - 1;
                  date_part_jit->Hash(
                      a->int_cells.data() + base,
                      reinterpret_cast<const char*>(date->varchar_bytes.data()),
                      starts, date_parts.data(), hashes.data(), n,
                      GroupKeySliceHasher::kSeed);
                  for (size_t lane = 0; lane < n; ++lane) {
                    const auto row = base + lane;
                    const auto value = text->VarcharAt(row);
                    const uint64_t entry =
                        value.empty() ? empty : HashView64(value);
                    const uint64_t hash = MixHash64(
                        hashes[lane] ^ (entry + 2 * 0x9e3779b97f4a7c15ULL));
                    const std::array<RuntimeKeyCell, 3> cells{
                        {{.int_val = a->int_cells[row]},
                         {.int_val = date_parts[lane]},
                         // RuntimeKeyCell carries str_len.
                         // NOLINTNEXTLINE(bugprone-suspicious-stringview-data-usage)
                         {.str_ptr = value.data(),
                          .str_len = static_cast<uint32_t>(value.size())}}};
                    status = emit_row(cells.data(), hash,
                                      static_cast<uint32_t>(row));
                    if (!status.ok()) {
                      break;
                    }
                  }
                  base += n;
                }
              } else {
                status = visit(std::views::iota(uint32_t{0}, count));
              }
            } else if (group_hash_jit && !a->has_null &&
                       (b == nullptr || !b->has_null)) {
              for (size_t base = 0; base < count && status.ok();
                   base += kHashBatch) {
                const size_t n = std::min(kHashBatch, count - base);
                group_hash_jit->Hash(a->int_cells.data() + base, hashes.data(),
                                     n, GroupKeySliceHasher::kSeed);
                for (size_t lane = 0; lane < n; ++lane) {
                  constexpr size_t kAhead = 16;
                  if (!prefetch_words.empty() && lane + kAhead < n) {
                    const size_t word = (hashes[lane + kAhead] >> 16) &
                                        (prefetch_words.size() - 1);
                    PrefetchBitmapWord(prefetch_words.data() + word);
                  }
                  const size_t row = base + lane;
                  const std::array<RuntimeKeyCell, 2> pair{
                      {{.int_val = a->int_cells[row]},
                       {.int_val = b == nullptr ? 0 : b->int_cells[row]}}};
                  status = emit_row(pair.data(), hashes[lane],
                                    static_cast<uint32_t>(row));
                  if (!status.ok()) {
                    break;
                  }
                }
              }
            } else {
              status = visit(std::views::iota(uint32_t{0}, count));
            }
          }
        } else {
          auto decoded = DecodeAndFilterBlock(
              *req.tlcs, block, req.table_schema, req.preds, scan_sched,
              &scratch, &columns, &candidates, &scanned);
          if (!decoded.HasValue()) {
            status = decoded.GetStatus();
          } else {
            if (leading_only && !selections.empty()) {
              const size_t n = decoded.Value() ? candidates.size() : 0;
              const size_t bytes = n * sizeof(uint16_t);
              const size_t old =
                  selection_bytes.fetch_add(bytes, std::memory_order_relaxed);
              if (old <= kSelectionBudget && bytes <= kSelectionBudget - old &&
                  (n == 0 || candidates.back() <= UINT16_MAX)) {
                auto& saved = selections[i];
                saved.rows.assign(candidates.begin(), candidates.begin() + n);
                saved.complete = true;
              }
            }
            if (decoded.Value()) {
              matching += candidates.size();
              status = visit(std::span<const uint32_t>(candidates));
            }
          }
        }
        if (!status.ok()) {
          errors[worker] = std::move(status);
          return;
        }
      }
      if (report) {
        total_scanned.fetch_add(scanned, std::memory_order_relaxed);
        total_matching.fetch_add(matching, std::memory_order_relaxed);
      }
    }));
    for (const Status& error : errors) {
      RETURN_IF_FAIL(error);
    }
    return Status::kSuccess;
  };

  const auto finish =
      [&](GroupTables& w_tables, uint32_t rest_max_bucket,
          const char* label) -> StatusOr<std::optional<Relation>> {
    const auto merge_begin = std::chrono::steady_clock::now();
    WorkerGroupTable& merged = *w_tables[0];
    for (size_t w = 1; w < workers; ++w) {
      WorkerGroupTable& src = *w_tables[w];
      for (size_t g = 0; g < src.NumGroups(); ++g) {
        const RuntimeKeyCell* gk = src.KeysAt(g);
        const uint64_t kh = src.HashAt(g);
        auto dst_or = merged.FindOrInsert(gk, kh, /*borrow_strings=*/false);
        if (!dst_or.HasValue()) {
          if (dst_or.GetStatus() == StatusCode::kMemoryLimitExceeded) {
            return std::optional<Relation>{};
          }
          return dst_or.GetStatus();
        }
        RuntimeAggState* d_ast = merged.AggsAt(dst_or.Value());
        d_ast[0].count += src.AggsAt(g)[0].count;
        d_ast[0].has_value = true;
      }
      w_tables[w].reset();
    }

    const size_t ng = merged.NumGroups();
    int64_t tau = 0;
    if (rest_max_bucket > 0) {
      if (ng < topk_cap) {
        return std::optional<Relation>{};
      }
      std::vector<int64_t> all_cnts(ng);
      for (size_t g = 0; g < ng; ++g) {
        all_cnts[g] = merged.AggsAt(g)[0].count;
      }
      std::nth_element(all_cnts.begin(),
                       std::next(all_cnts.begin(),
                                 static_cast<std::ptrdiff_t>(topk_cap - 1)),
                       all_cnts.end(), std::greater<int64_t>{});
      tau = all_cnts[topk_cap - 1];
      // Soundness certificate (`quotient_bucket_topk_pruning_sound`): every
      // bucket outside `heavy_bits` has total row count `<= rest_max_bucket <
      // tau`, so NO group outside `heavy_bits` can reach or tie `tau`!
      if (tau <= 0 || std::cmp_less_equal(tau, rest_max_bucket)) {
        return std::optional<Relation>{};
      }
    }

    const PartOrderKind fast_order_kind =
        ClassifyPartOrderKind(req.order_bindings, req.key_specs, req.agg_specs);
    std::vector<std::vector<SurvivorGroupRow>> single_surv(1);
    std::vector<std::unique_ptr<QueryMemoryCharge>> surv_charges(1);
    auto& out_vec = single_surv[0];
    QueryMemoryCharge& survivor_charge = SurvivorChargeFor(surv_charges, 0);

    std::vector<RuntimeKeyCell> full_keys(req.key_specs.size());
    std::vector<Value> finished_aggs(1);
    std::vector<Value> cached_keys(req.key_specs.size());

    std::vector<size_t> shortlist;
    if (near_unique && ng > topk_cap) {
      shortlist.resize(ng);
      for (size_t g = 0; g < ng; ++g) {
        shortlist[g] = g;
      }
      std::nth_element(
          shortlist.begin(),
          std::next(shortlist.begin(), static_cast<std::ptrdiff_t>(topk_cap)),
          shortlist.end(), [&](size_t a, size_t b) {
            return merged.AggsAt(a)[0].count > merged.AggsAt(b)[0].count;
          });
      shortlist.resize(topk_cap);
    }
    const size_t emit_count = shortlist.empty() ? ng : shortlist.size();
    for (size_t i = 0; i < emit_count; ++i) {
      const size_t g = shortlist.empty() ? i : shortlist[i];
      const RuntimeAggState* ast = merged.AggsAt(g);
      if (rest_max_bucket > 0 && ast[0].count < tau) {
        continue;
      }
      km.ReconstructFull(req.key_specs, merged.KeysAt(g), full_keys.data());
      SurvivorGroupRow row;
      if (fast_order_kind != PartOrderKind::kNone) {
        const auto& ob0 = req.order_bindings[0].binding;
        RETURN_IF_FAIL(EvaluateNumericOrderValue(
            ob0,
            ob0.kind == OutputSlotBinding::Kind::kAgg
                ? &req.agg_specs[ob0.index]
                : nullptr,
            ob0.kind == OutputSlotBinding::Kind::kKey
                ? &req.key_specs[ob0.index]
                : nullptr,
            ob0.kind == OutputSlotBinding::Kind::kAgg ? &ast[ob0.index]
                                                      : nullptr,
            ob0.kind == OutputSlotBinding::Kind::kKey ? &full_keys[ob0.index]
                                                      : nullptr,
            &row.num_null, &row.num_int, &row.num_double));
      }
      finished_aggs[0] = Value(ast[0].count);
      std::fill(cached_keys.begin(), cached_keys.end(), Value());
      if (fast_order_kind == PartOrderKind::kNone &&
          !req.order_bindings.empty()) {
        row.order_keys.resize(req.order_bindings.size());
        for (size_t k = 0; k < req.order_bindings.size(); ++k) {
          ASSIGN_OR_RETURN(Value, ov,
                           EvaluateBindingValue(req.order_bindings[k].binding,
                                                req.key_specs, full_keys.data(),
                                                finished_aggs, &cached_keys));
          row.order_keys[k] = std::move(ov);
        }
      }
      row.emit_vals.reserve(req.emit_bindings.size());
      for (const auto& eb : req.emit_bindings) {
        ASSIGN_OR_RETURN(
            Value, ev,
            EvaluateBindingValue(eb, req.key_specs, full_keys.data(),
                                 finished_aggs, &cached_keys));
        row.emit_vals.push_back(std::move(ev));
      }
      PushChargedSurvivor(&out_vec, &survivor_charge, std::move(row));
    }

    return FinalizePartitionedRelation(
        req, single_surv, ng, total_scanned.load(), total_matching.load(),
        merge_begin, label, &surv_charges);
  };
  if (near_unique) {
    // The estimate only chooses whether to try this layout. Every hit is
    // still checked by the full key table, including hash collisions.
    const size_t wanted = static_cast<size_t>(std::clamp<uint64_t>(
        input_bound * 8, uint64_t{1} << 22, uint64_t{1} << 30));
    const size_t bits = std::bit_ceil(wanted);
    const bool use_two_bits = req.preds.Empty();
    const size_t words = (bits / 64) * (use_two_bits ? 2 : 1);
    QueryMemoryCharge bitmap_charge;
    // Dynamically sized uninitialized buffer is zeroed in parallel below.
    // NOLINTNEXTLINE(modernize-avoid-c-arrays)
    std::unique_ptr<uint64_t[]> bitmaps;
    const auto tag = [&](uint64_t hash) {
      const size_t word = (hash >> 16) & (words - 1);
      // Keep two independent bit positions in the same memory word, so a
      // single atomic read/modify/write publishes both sightings.
      const uint64_t mask = use_two_bits
                                ? ((uint64_t{1} << (hash & 31)) |
                                   (uint64_t{1} << (32 + ((hash >> 5) & 31))))
                                : (uint64_t{1} << (hash & 63));
      return std::pair{word, mask};
    };
    // Lean: duplicate_key_retained and word_partition_sound. All sightings
    // for a word go to the same worker in the bitmap reduction.
    constexpr size_t kShards = 256;
    const size_t shard_words = words / kShards;
    QueryMemoryCharge shard_charge;
    QueryMemoryCharge replay_charge;
    std::vector<uint64_t> replay;
    const size_t replay_words = (input_bound + 63) / 64;
    constexpr size_t kTagBits = 35;
    constexpr uint64_t kTagMask = (uint64_t{1} << kTagBits) - 1;
    const bool shard_sightings =
        group_hash_jit && input_bound >= (uint64_t{1} << 22) &&
        input_bound <= (uint64_t{1} << (64 - kTagBits)) &&
        sizeof(size_t) >= 8 &&
        ::tinylamb::platform::GetEnv(
            "TINYLAMB_DISABLE_SHARDED_DUPLICATE_SIEVE") == nullptr &&
        replay_charge.TryAdd(replay_words * sizeof(uint64_t)) &&
        shard_charge.TryAdd(
            (input_bound * 2 + workers * (kShards * 64 + 2 * shard_words)) *
            sizeof(uint64_t));
    if (shard_sightings) {
      replay.resize(replay_words);
      std::vector<std::array<std::vector<uint64_t>, kShards>> queues(workers);
      RETURN_IF_FAIL(scan_keys(
          req.candidate_blocks, true,
          [&](size_t worker, const RuntimeKeyCell*, uint64_t hash, size_t block,
              uint32_t row) {
            const size_t word = (hash >> 16) & (words - 1);
            auto& queue = queues[worker][word / shard_words];
            if (queue.size() == queue.capacity()) {
              queue.reserve(std::max<size_t>(64, 2 * queue.capacity()));
            }
            const uint64_t ordinal = block_row_starts[block] + row;
            // Lean: packet_replay_roundtrip. Preserve the 25-bit word
            // number, the two lane positions and the ordinal independently.
            queue.push_back((ordinal << kTagBits) |
                            (static_cast<uint64_t>(word) << 10) |
                            (hash & 1023));
            return Status::kSuccess;
          },
          /*leading_only=*/true));
      std::atomic<size_t> next_shard{0};
      RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t) {
        std::vector<uint64_t> seen(shard_words);
        std::vector<uint64_t> repeated(shard_words);
        const auto local_tag = [&](uint64_t packet) {
          const uint64_t compact = packet & kTagMask;
          const uint64_t hash = ((compact >> 10) << 16) | (compact & 1023);
          const auto [word, mask] = tag(hash);
          return std::pair{word & (shard_words - 1), mask};
        };
        for (size_t shard = next_shard.fetch_add(1, std::memory_order_relaxed);
             shard < kShards;
             shard = next_shard.fetch_add(1, std::memory_order_relaxed)) {
          std::ranges::fill(seen, 0);
          std::ranges::fill(repeated, 0);
          for (const auto& worker : queues) {
            for (const uint64_t packet : worker[shard]) {
              const auto [word, mask] = local_tag(packet);
              repeated[word] |= seen[word] & mask;
              seen[word] |= mask;
            }
          }
          size_t singletons = 0;
          for (const auto& worker : queues) {
            for (const uint64_t packet : worker[shard]) {
              const auto [word, mask] = local_tag(packet);
              if ((repeated[word] & mask) != mask && singletons++ >= topk_cap) {
                continue;
              }
              const uint64_t ordinal = packet >> kTagBits;
              std::atomic_ref(replay[ordinal / 64])
                  .fetch_or(uint64_t{1} << (ordinal % 64),
                            std::memory_order_relaxed);
            }
          }
        }
      }));
    } else {
      replay_charge.ReleaseAll();
      if (!bitmap_charge.TryAdd(2 * words * sizeof(uint64_t))) {
        return std::nullopt;
      }
      // Uninitialized dynamic buffer is zeroed in parallel.
      // NOLINTNEXTLINE(modernize-avoid-c-arrays)
      bitmaps = std::make_unique_for_overwrite<uint64_t[]>(2 * words);
      RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t w) {
        const size_t size = 2 * words;
        // Dynamically indexed raw bitmap.
        // NOLINTNEXTLINE(modernize-avoid-c-arrays)
        std::fill(bitmaps.get() + ((size * w) / workers),
                  bitmaps.get() + ((size * (w + 1)) / workers), uint64_t{0});
      }));
      RETURN_IF_FAIL(scan_keys(
          req.candidate_blocks, true,
          [&](size_t, const RuntimeKeyCell*, uint64_t hash) {
            const auto [word, mask] = tag(hash);
            // Dynamically indexed raw bitmap.
            // NOLINTNEXTLINE(modernize-avoid-c-arrays)
            const uint64_t old = std::atomic_ref(bitmaps[word])
                                     .fetch_or(mask, std::memory_order_relaxed);
            if ((old & mask) != 0) {
              // Dynamically indexed raw bitmap.
              // NOLINTNEXTLINE(modernize-avoid-c-arrays)
              std::atomic_ref(bitmaps[word + words])
                  .fetch_or(old & mask, std::memory_order_relaxed);
            }
            return Status::kSuccess;
          },
          /*leading_only=*/true, std::span(bitmaps.get(), words)));
    }
    shard_charge.ReleaseAll();
    std::vector<QueryMemoryCharge> charges(workers);
    auto tables = make_tables(charges);
    std::vector<size_t> singletons(workers, 0);
    const Status status = scan_keys(
        req.candidate_blocks, false,
        [&](size_t w, const RuntimeKeyCell* keys, uint64_t hash) -> Status {
          if (!shard_sightings) {
            const auto [word, mask] = tag(hash);
            // Dynamically indexed raw bitmap.
            // NOLINTNEXTLINE(modernize-avoid-c-arrays)
            const bool repeated = (bitmaps[word + words] & mask) == mask;
            // All dropped singleton groups tie on the only ORDER BY term.
            // Retain enough witnesses to supply LIMIT plus OFFSET.
            if (!repeated && singletons[w]++ >= topk_cap) {
              return Status::kSuccess;
            }
          }
          return count_group(*tables[w], keys, hash);
        },
        /*leading_only=*/false,
        shard_sightings
            ? std::span<const uint64_t>{}
            : std::span<const uint64_t>(bitmaps.get() + words, words),
        replay);
    if (status == StatusCode::kMemoryLimitExceeded) {
      return std::nullopt;
    }
    RETURN_IF_FAIL(status);
    bitmaps.reset();
    bitmap_charge.ReleaseAll();
    return finish(tables, 0,
                  shard_sightings ? "sharded-duplicate-sieve-topk"
                                  : "duplicate-sieve-topk");
  }

  std::vector<std::vector<uint32_t>> worker_buckets(
      workers, std::vector<uint32_t>(num_buckets, 0));
  const auto sketch = [&](size_t worker, const RuntimeKeyCell*, uint64_t hash) {
    ++worker_buckets[worker][bucket_of(hash)];
    return Status::kSuccess;
  };
  const auto sum_buckets = [&] {
    std::vector<uint32_t> result(num_buckets, 0);
    for (const auto& local : worker_buckets) {
      for (size_t b = 0; b < num_buckets; ++b) {
        result[b] += local[b];
      }
    }
    return result;
  };
  const auto choose_buckets = [&](const std::vector<uint32_t>& counts,
                                  size_t capacity) {
    auto ranked = counts;
    std::nth_element(
        ranked.begin(),
        std::next(ranked.begin(), static_cast<std::ptrdiff_t>(capacity - 1)),
        ranked.end(), std::greater<uint32_t>{});
    const uint32_t cutoff = ranked[capacity - 1];
    std::vector<uint64_t> mask(num_buckets / 64, 0);
    // Give strictly higher counts priority. Break ties by bucket index
    // only when building the speculative mask.
    size_t remaining = capacity;
    for (size_t b = 0; b < num_buckets; ++b) {
      if (counts[b] > cutoff) {
        mask[b >> 6U] |= uint64_t{1} << (b & 63U);
        --remaining;
      }
    }
    if (cutoff != 0) {
      for (size_t b = 0; remaining != 0 && b < num_buckets; ++b) {
        if (counts[b] == cutoff) {
          mask[b >> 6U] |= uint64_t{1} << (b & 63U);
          --remaining;
        }
      }
    }
    return mask;
  };
  const auto outside_max = [&](const std::vector<uint32_t>& counts,
                               std::span<const uint64_t> mask) {
    uint32_t result = 0;
    for (size_t b = 0; b < num_buckets; ++b) {
      if (!has_bucket(mask, b)) {
        result = std::max(result, counts[b]);
      }
    }
    return result;
  };

  // A small stratified sample proposes buckets. Its estimates only
  // choose the mask. Every count used for the result or certificate comes
  // from the full, fused sweep below.
  const bool can_seed =
      (specialize && (single_raw_varchar_key || single_raw_int_key)) ||
      try_numeric_quotient || date_bounds;
  constexpr size_t group_cap = 8192;
  QueryMemoryCharge speculation_charge;
  std::vector<uint64_t> seeded;
  if (can_seed && speculation_charge.TryAdd(4 * workers * group_cap *
                                            (sizeof(RuntimeAggState) +
                                             n_phys * sizeof(RuntimeKeyCell) +
                                             sizeof(uint64_t)))) {
    const size_t blocks = req.candidate_blocks.size();
    const size_t samples = std::clamp<size_t>(blocks / 128, 8, 256);
    std::vector<uint32_t> sample;
    sample.reserve(samples);
    for (size_t i = 0; i < samples; ++i) {
      sample.push_back(req.candidate_blocks[(i * (blocks / samples)) +
                                            std::min(i, blocks % samples)]);
    }
    if (scan_keys(sample, false, sketch).ok()) {
      auto counts = sum_buckets();
      if (!try_numeric_quotient ||
          std::ranges::count_if(counts, [](uint32_t v) { return v >= 8; }) >=
              static_cast<std::ptrdiff_t>(topk_cap)) {
        seeded = choose_buckets(counts, std::max<size_t>(128, topk_cap));
      }
    }
    for (auto& counts : worker_buckets) {
      std::ranges::fill(counts, 0);
    }
  }
  if (try_numeric_quotient && seeded.empty()) {
    return std::nullopt;
  }

  std::vector<uint32_t> global_buckets;
  if (!seeded.empty()) {
    std::vector<QueryMemoryCharge> charges(workers);
    auto tables = make_tables(charges);
    std::vector<uint8_t> complete(workers, 1);
    // Lean proof: lean/TinylambOpt/QuotientFusion.lean
    // (fused_bucket_count, fused_group_count, fused_topk_certificate)
    const auto fused = [&](size_t worker, const RuntimeKeyCell* keys,
                           uint64_t hash) -> Status {
      const size_t bucket = bucket_of(hash);
      ++worker_buckets[worker][bucket];
      if (complete[worker] != 0 && has_bucket(seeded, bucket)) {
        auto& table = *tables[worker];
        if (table.NumGroups() == group_cap ||
            !count_group(table, keys, hash).ok()) {
          // Continue building the entire sketch so the exact replay can
          // reuse it even after a speculative memory refusal.
          complete[worker] = 0;
        }
      }
      return Status::kSuccess;
    };
    const Status status = scan_keys(req.candidate_blocks, true, fused);
    if (status == StatusCode::kInvalidArgument ||
        status == StatusCode::kMemoryLimitExceeded) {
      return std::optional<Relation>{};
    }
    RETURN_IF_FAIL(status);
    global_buckets = sum_buckets();
    if (std::ranges::all_of(complete, [](uint8_t done) { return done != 0; })) {
      ASSIGN_OR_RETURN(std::optional<Relation>, result,
                       finish(tables, outside_max(global_buckets, seeded),
                              "quotient-fused-topk"));
      if (result) {
        return result;
      }
    }
  } else {
    const Status status = scan_keys(req.candidate_blocks, true, sketch);
    if (status == StatusCode::kInvalidArgument ||
        status == StatusCode::kMemoryLimitExceeded) {
      return std::optional<Relation>{};
    }
    RETURN_IF_FAIL(status);
    global_buckets = sum_buckets();
  }
  std::vector<uint64_t>().swap(seeded);
  speculation_charge.ReleaseAll();
  worker_buckets.clear();

  auto heavy_bits = choose_buckets(global_buckets, kHeavyBuckets);
  const uint32_t rest_max_bucket = outside_max(global_buckets, heavy_bits);
  if (rest_max_bucket > 0 &&
      *std::ranges::max_element(global_buckets) <= rest_max_bucket) {
    return std::optional<Relation>{};
  }
  std::vector<uint32_t>().swap(global_buckets);
  std::vector<QueryMemoryCharge> charges(workers);
  auto tables = make_tables(charges);
  const Status status = scan_keys(
      req.candidate_blocks, false,
      [&](size_t worker, const RuntimeKeyCell* keys, uint64_t hash) -> Status {
        if (!has_bucket(heavy_bits, bucket_of(hash))) {
          return Status::kSuccess;
        }
        return count_group(*tables[worker], keys, hash);
      });
  if (status == StatusCode::kInvalidArgument ||
      status == StatusCode::kMemoryLimitExceeded) {
    return std::optional<Relation>{};
  }
  RETURN_IF_FAIL(status);
  return finish(tables, rest_max_bucket, "quotient-heavy-topk");
}
}  // namespace tinylamb::relational_detail::partitioned_detail
