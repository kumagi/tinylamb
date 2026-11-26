/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#pragma once
#include "executor/detail/columnar_tlcs_internal.hpp"
#include "executor/detail/typed_stream_aggregate_partitioned.hpp"

namespace tinylamb::relational_detail::partitioned_detail {
inline void PrefetchBitmapWord(const uint64_t* word) {
#if defined(__GNUC__) || defined(__clang__)
  __builtin_prefetch(word, 0, 1);
#else
  (void)word;
#endif
}

inline bool IsVarCharKeyKind(ColKeyKind kind) noexcept {
  return kind == ColKeyKind::kColVarChar ||
         kind == ColKeyKind::kCaseWhenVarChar ||
         kind == ColKeyKind::kRegexReplaceVarChar;
}
inline bool IsDistinctAggKind(ColAggKind kind) noexcept {
  return kind == ColAggKind::kCountDistinctInt64 ||
         kind == ColAggKind::kCountDistinctDate ||
         kind == ColAggKind::kCountDistinctDouble ||
         kind == ColAggKind::kCountDistinctVarChar;
}

// A checked SUM(INT64) is order-sensitive: whether the running sum wraps
// depends on the sequence a group's rows reach the accumulator in, and the
// partitioned kernels fold in row order only when Pass 1 runs a single worker
// (with the dynamic block hand-off, the worker-major merge of the per-shard
// buffers is an arbitrary row order otherwise). Keep plans with a checked SUM
// on one worker; COUNT/MIN/MAX plans stay parallel. Same contract as the
// typed-stream aggregate gate and the non-partitioned TLCS grouped gate.
inline bool HasCheckedIntSum(const std::vector<ColAggSpec>& agg_specs) {
  return std::any_of(
      agg_specs.begin(), agg_specs.end(),
      [](const ColAggSpec& s) { return s.kind == ColAggKind::kSumInt64; });
}
enum class PartOrderKind : uint8_t { kNone, kInt, kDouble };
struct SurvivorGroupRow {
  bool num_null{false};
  int64_t num_int{0};
  double num_double{0.0};
  std::vector<Value> order_keys;
  std::vector<Value> emit_vals;
};

PartOrderKind ClassifyPartOrderKind(
    const std::vector<ColumnarOrderBinding>& order_bindings,
    const std::vector<ColGroupKeySpec>& key_specs,
    const std::vector<ColAggSpec>& agg_specs) noexcept;

QueryMemoryCharge& SurvivorChargeFor(
    std::vector<std::unique_ptr<QueryMemoryCharge>>& charges, size_t shard);

void PushChargedSurvivor(std::vector<SurvivorGroupRow>* rows,
                         QueryMemoryCharge* charge, SurvivorGroupRow row);

void PushChargedSurvivor(std::vector<SurvivorGroupRow>* rows,
                         ChunkedCharge* charge, SurvivorGroupRow row);

Status EvaluateNumericOrderValue(const OutputSlotBinding& b,
                                 const ColAggSpec* agg_spec,
                                 const ColGroupKeySpec* key_spec,
                                 const RuntimeAggState* agg_st,
                                 const RuntimeKeyCell* key_cell,
                                 bool* out_is_null, int64_t* out_int,
                                 double* out_double) noexcept;

Relation FinalizePartitionedRelation(
    const PartitionedColumnarAggRequest& req,
    std::vector<std::vector<SurvivorGroupRow>>& shard_survivors,
    size_t total_groups, uint64_t total_scanned, uint64_t total_matching,
    std::chrono::steady_clock::time_point merge_begin, const char* path_label,
    std::vector<std::unique_ptr<QueryMemoryCharge>>* survivor_charges =
        nullptr);

StatusOr<std::optional<Relation>> TryRunKeyFiberUnorderedLimitAggregate(
    const PartitionedColumnarAggRequest& req, const KeyProjectionMap& km);

StatusOr<std::optional<Relation>> TryRunQuotientBucketHeavyHittersTopK(
    const PartitionedColumnarAggRequest& req, const KeyProjectionMap& km);

}  // namespace tinylamb::relational_detail::partitioned_detail
