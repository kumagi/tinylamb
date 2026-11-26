/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#pragma once
#include "executor/detail/fused_group_key_layout.hpp"
#include "executor/detail/fused_raw_aggregates.hpp"
#include "executor/detail/planning_columnar_keys.hpp"
#include "expression/bytecode.hpp"

namespace tinylamb::relational_detail {

struct BulkCountValue {
  int64_t count{0};
  size_t pairs{0};
  uint32_t r_idx{kInvalidRightIdx};
};

struct ColProbeMorsel {
  uint32_t ci;
  uint32_t r_begin;
  uint32_t r_end;
};

struct BuildKey {
  int64_t key;
  uint32_t row;
};

struct DirectDoubleCell {
  double sum{0.0};
  uint64_t meta{0};  // bit 0: matched, bit 1: inf, bits 2..63: count
};

struct DirectGidWorker {
  std::vector<uint8_t> matched;
  std::vector<double> doubles;
  std::vector<int64_t> ints;
  std::vector<int64_t> counts;
  std::vector<uint8_t> inf_flags;
  std::vector<platform::ExtendedFloat> ext_sx;
  std::vector<platform::ExtendedFloat> ext_sxx;
  std::vector<DirectDoubleCell> double_cells;
};

inline constexpr uint32_t kInvalidGid = std::numeric_limits<uint32_t>::max();

struct OrderedSumInput {
  double value{0};
  uint32_t gid{kInvalidGid};
  bool nonnull{false};
};

struct CompiledDefDoubleArith {
  enum class Pattern : uint8_t {
    kGeneral = 0,
    kSub = 1,
    kAdd = 2,
    kSubDivConst = 3,
    kSubSubAddDivConst = 4,
  };
  struct Step {
    // 0: load def col, 1: load right col, 2: load const,
    // 3: add, 4: sub, 5: mul, 6: div, 7: div by finite |c| >= 1.0
    uint8_t kind{0};
    uint32_t operand{0};
    double cst{0.0};
  };
  Pattern pattern{Pattern::kGeneral};
  std::array<uint32_t, 4> fast_cols{};
  double fast_div{1.0};
  std::vector<Step> steps;
};

inline constexpr size_t kCompactSplitShards = 256;
inline constexpr size_t kNoCachedGroup = std::numeric_limits<size_t>::max();

inline constexpr auto kInvalidFusedPacked2x32 = static_cast<int64_t>(
    (static_cast<uint64_t>(0x80000000ULL) << 32) | 0x80000000ULL);
inline bool ColumnarCellIsNullOrNaN(const ColumnarColumn& col,
                                    size_t r) noexcept {
  if (col.IsNull(r)) {
    return true;
  }
  if (col.is_uniform) {
    return col.type == ValueType::kDouble && std::isnan(col.DoubleAt(r));
  }
  Value tmp;
  col.WriteValueInto(r, &tmp);
  return tmp.IsNull() ||
         (tmp.type == ValueType::kDouble && std::isnan(tmp.value.double_value));
}

inline bool FitsFusedInt32(int64_t v) noexcept {
  return v >= static_cast<int64_t>(std::numeric_limits<int32_t>::min()) &&
         v <= static_cast<int64_t>(std::numeric_limits<int32_t>::max());
}

inline int64_t PackFusedInt32Pair(int64_t k0, int64_t k1) noexcept {
  return static_cast<int64_t>(
      (static_cast<uint64_t>(static_cast<uint32_t>(static_cast<int32_t>(k0)))
       << 32) |
      static_cast<uint64_t>(static_cast<uint32_t>(static_cast<int32_t>(k1))));
}

inline bool ReadFusedDouble(const Value& v, double* out) noexcept {
  if (v.type == ValueType::kDouble) {
    *out = v.value.double_value;
    return true;
  }
  if (v.type == ValueType::kInt64 && !v.IsUnsigned()) {
    *out = static_cast<double>(v.value.int_value);
    return true;
  }
  return false;
}

inline bool ReadFusedIntCell(const ColumnarColumn& kc, size_t r,
                             bool fast_uniform, int64_t* out) noexcept {
  if (fast_uniform) {
    return kc.TryReadNonNullInt(r, out);
  }
  if (ColumnarCellIsNullOrNaN(kc, r)) {
    return false;
  }
  *out = ColumnarIntValue(kc, r);
  return true;
}

inline bool ReadFusedCompositeKey(const ColumnarChunk& chunk,
                                  const std::vector<slot_t>& columns,
                                  const std::vector<int64_t>& biases,
                                  size_t row, CompositeIntKey* key) {
  *key = CompositeIntKey{};
  for (size_t i = 0; i < columns.size(); ++i) {
    const auto& column = chunk.columns[columns[i]];
    const bool fast =
        column.is_uniform && !column.is_unsigned &&
        (column.type == ValueType::kInt64 || column.type == ValueType::kDate);
    int64_t raw = 0;
    if (!ReadFusedIntCell(column, row, fast, &raw)) {
      return false;
    }
    key->parts[key->count++] = AddJoinKeyBias(raw, biases[i]);
  }
  return true;
}

inline size_t FusedRadixBucket(uint64_t ukey) noexcept {
  uint64_t x = ukey + 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30U)) * 0xbf58476d1ce4e5b9ULL;
  return static_cast<size_t>((x ^ (x >> 27U)) >> 58U);
}

struct FusedProbeSource {
  const Relation& left;
  GroupedJoinFusionSpec* fusion{nullptr};
  const DeferredLSTableProbe* deferred_left_probe{nullptr};
  const Relation* right_ptr{nullptr};
  size_t left_width{0};
  const std::vector<BytecodeProgram>& residual_programs;
  const std::vector<ProgramShape>& key_shapes;
  const std::vector<slot_t>& left_columns;
  const std::vector<int64_t>& left_key_offsets;
  size_t total_left{0};
  const std::vector<AggregateAccumulator::Shape>& aggregate_shapes;
  const FusedRightCellReader& read_right_cell;
  const std::vector<ValueType>& def_base_types;
  const std::vector<uint8_t>& def_overwritten_by_pk;
  const std::vector<PreparedDeferredPkLookup>& prepared_pk_lookups;
  const std::vector<PreparedDeferredPkGroup>& def_pk_groups;
  const std::array<int8_t, 16>& def_col_to_pk_lookup;
  size_t workers{0};
  size_t probe_chunk_rows{0};
  size_t chunk_count{0};
  const std::vector<ColProbeMorsel>& col_morsels;
  slot_t int_group_slot{0};
  JoinType join_type{JoinType::kInner};
  bool singleton_right_relation{false};
  bool all_direct_group_keys{false};
  bool integer_key{false};
  bool composite_int_key{false};
  bool int_group_key{false};
  ValueType int_group_type{ValueType::kNull};
  bool anti_unmatched_only{false};
  bool bulk_raw_count{false};
  bool compact_raw_left_key{false};
  bool compact_counts{false};
  bool compact_log_allowed{false};
};

struct FusedProbeKeys {
  const std::optional<FlatJoinIdxBuckets<int64_t>>& int_buckets;
  const std::optional<FlatJoinIdxBuckets<CompositeIntKey, CompositeIntKeyHash>>&
      composite_buckets;
  const std::vector<CompositeIntKey>& dense_composite_keys;
  const std::vector<uint32_t>& dense_composite_rows;
  int64_t dense_composite_min{0};
  const std::optional<FlatJoinIdxBuckets<std::string>>& key_buckets;
  const std::optional<FlatIntKeyMap<BulkCountValue>>& bulk_counts;
  const std::vector<BulkCountValue>& dense_bulk_counts;
  int64_t dense_bulk_min{0};
  const std::vector<std::vector<BuildKey>>& key_cache;
  const std::shared_ptr<QueryMemoryCharge>& key_cache_charge;
  const std::vector<uint32_t>& dense_int_idxs;
  const std::vector<uint64_t>& ranked_int_bits;
  const std::vector<uint32_t>& ranked_int_prefix;
  const std::vector<uint32_t>& ranked_int_rows;
  uint64_t ranked_int_span{0};
  int64_t dense_int_min{0};
  const std::vector<UniqueIntSlot>& unique_int_table;
  size_t unique_int_mask{0};
  const std::vector<size_t>& fanout_key_bounds;
  const std::vector<uint32_t>& right_row_gid;
  bool use_dense_composite{false};
  bool use_dense_bulk{false};
  bool use_dense_int_rows{false};
  bool packed_2x32_int_key{false};
  bool use_unique_int_table{false};
  bool identity_right_gid{false};
  bool use_preassigned_build_groups{false};
};

struct FusedProbeDirect {
  size_t agg_count{0};
  std::vector<DirectGidWorker>& direct_gid_workers;
  const std::shared_ptr<QueryMemoryCharge>& ordered_sum_charge;
  bool use_direct_gid_agg{false};
  bool use_partitioned_count_distinct{false};
  bool single_double_sum{false};
  bool use_shared_direct_gid{false};
  bool scalar_fact_charge{false};
};

struct FusedProbeDeferred {
  const std::array<LeftNumericAgg, kRawAggregateCapacity>& left_numeric;
  const std::vector<CompiledDefDoubleArith>& compiled_double_arith;
  const std::array<uint8_t, 5>& disjoint_bucket5_slots;
  const std::vector<size_t>& dense_pk_group_indices;
  size_t radix_primary{0};
  std::vector<std::array<RadixSoABucket, kRadixParts>>& worker_radix_buckets;
  const std::shared_ptr<const LSTableIntegerRuns>& flat_run_index;
  const std::shared_ptr<const LSTableIntegerRunBounds>& flat_run_bounds;
  int32_t disjoint_bucket5_col0{0};
  int32_t disjoint_bucket5_col1{0};
  bool left_cell_agg_ready{false};
  bool uses_double_arith_cell_op{false};
  bool left_numeric_probe{false};
  bool disjoint_bucket5_ready{false};
  bool def_compact_split_probe{false};
  bool def_group_cell_probe{false};
  bool use_dense_split_direct_gid{false};
  bool use_packed_double_cells{false};
  bool use_radix_agg{false};
  bool radix_tuple_is_int{false};
  bool disjoint_direct_groups{false};
};

std::vector<slot_t> BuildDeferredProbeMatCols(
    const DeferredLSTableProbe& probe);

struct FusedProbeCallbacks {
  std::function<Status(FusedWorkerLocal&, uint64_t)> record_compact_split;
  std::function<StatusOr<bool>(FusedWorkerLocal&, const Row&, uint32_t, Row&,
                               size_t*)>
      process_pair;
  std::function<Status(FusedWorkerLocal&, Row&, const Row&)> unmatched_left;
  std::function<Status(size_t, Row&, const Row&, uint32_t)>
      accumulate_preassigned_pair;
};

struct FusedGroupProbe {
  const FusedGroupKeyLayout& layout;
  const FusedRawAggregates& raw_aggregates;
  FusedProbeSource source;
  FusedProbeKeys keys;
  FusedProbeDirect direct;
  FusedProbeDeferred deferred;
  FusedProbeCallbacks callbacks;
  Status Run(size_t w, FusedWorkerLocal& local, Row& scratch) const;

 private:
  StatusOr<bool> ProcessPair(FusedWorkerLocal& local, const Row& left_row,
                             uint32_t r_idx, Row& scratch,
                             size_t* cached_group = nullptr) const {
    return callbacks.process_pair(local, left_row, r_idx, scratch,
                                  cached_group);
  }
};
}  // namespace tinylamb::relational_detail
