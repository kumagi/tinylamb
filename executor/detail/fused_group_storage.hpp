/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#pragma once

#include <array>
#include <bit>
#include <cstdint>
#include <deque>
#include <memory>
#include <span>
#include <vector>

#include "common/platform/extended_float.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/fused_dense_split.hpp"
#include "executor/detail/planning_heuristics.hpp"

namespace tinylamb::relational_detail {

inline constexpr size_t kRawAggregateCapacity = 24;

struct FusedRawBlock {
  static constexpr size_t kGroups = 1024;
  explicit FusedRawBlock(size_t aggregates, bool has_stat = false,
                         bool has_double = true, bool has_int = true)
      : doubles(has_double ? kGroups * aggregates : 0),
        ints(has_int ? kGroups * aggregates : 0),
        counts(kGroups * aggregates),
        flags(kGroups * aggregates),
        ext_sx(has_stat ? kGroups * aggregates : 0),
        ext_sxx(has_stat ? kGroups * aggregates : 0) {}
  std::vector<double> doubles;
  std::vector<int64_t> ints;
  std::vector<int64_t> counts;
  std::vector<uint8_t> flags;
  std::vector<platform::ExtendedFloat> ext_sx;
  std::vector<platform::ExtendedFloat> ext_sxx;
};

struct FusedGroup {
  // Canonical group key, kept so the merge re-probes the shared map without
  // re-evaluating the key programs.
  Row key;
  std::unique_ptr<Row> representative;
  const Row* rep_left{nullptr};
  uint32_t rep_col_chunk{kInvalidRightIdx};
  uint32_t rep_col_row{0};
  uint32_t rep_right_idx{kInvalidRightIdx};
  size_t state_offset{0};
  // count-star-only fusions accumulate here instead of through
  // AggregateAccumulator states: the per-pair work is one integer increment
  // and the merge is an integer add.
  int64_t raw_count{0};
  uint64_t key_hash{0};
  bool has_key_hash{false};
  // COUNT/group-presence needs none of these arrays. Other aggregations use
  // exactly their aggregate count in stable blocks instead of eight cells
  // in every group, including every count-only group.
  // The worker/merge arena owns the block through result materialization.
  FusedRawBlock* raw_owner{nullptr};
  size_t raw_base{0};
  [[nodiscard]] double* RawDoubles() const {
    return raw_owner->doubles.data() + raw_base;
  }
  [[nodiscard]] int64_t* RawInts() const {
    return raw_owner->ints.data() + raw_base;
  }
  [[nodiscard]] int64_t* RawCounts() const {
    return raw_owner->counts.data() + raw_base;
  }
  [[nodiscard]] uint8_t* RawInfFlags() const {
    return raw_owner->flags.data() + raw_base;
  }
  [[nodiscard]] platform::ExtendedFloat* RawStatX() const {
    return raw_owner->ext_sx.data() + raw_base;
  }
  [[nodiscard]] platform::ExtendedFloat* RawStatXX() const {
    return raw_owner->ext_sxx.data() + raw_base;
  }
  std::unique_ptr<FlatIntKeyMap<uint8_t>> raw_int_set;
};

enum class RawAggKind : uint8_t {
  kNone = 0,
  kSumDouble,
  kSumInt64,
  kAvgDouble,
  kStddevSamp,
  kStddevPop,
  kVarSamp,
  kVarPop,
  kCountStar,
  kCountValue,
  kConstantExtremum,
  kCountDistinctInt64,
};

struct LeftPackedIntCol {
  slot_t col{0};
  ValueType type{ValueType::kNull};
  int64_t min_val{0};
  uint64_t null_code{0};
  uint64_t mask{0};
  uint8_t shift{0};
};
struct FusedRightCellReader {
  const RightColLocator& right_locator;
  const Relation* const& right_ptr;
  const bool& right_col_only;
  const Value& kNullCell;
  const Value& operator()(uint32_t r_idx, size_t rcol, Value* scratch) const {
    if (r_idx == kInvalidRightIdx) {
      return kNullCell;
    }
    if (right_col_only) {
      right_locator.WriteValue(rcol, r_idx, scratch);
      return *scratch;
    }
    return right_ptr->rows[r_idx].values_[rcol];
  }
};
struct PreparedPkCellReader {
  const std::vector<PreparedDeferredPkLookup>& prepared_pk_lookups;
  void operator()(size_t li, uint32_t nb, uint32_t rin, Value* out) const {
    const PreparedDeferredPkLookup& m = prepared_pk_lookups[li];
    const PreparedDeferredPkBlock& pb = m.blocks[nb];
    if (pb.has_null_payload != 0 && pb.pay_blk->IsNull(rin)) {
      *out = Value();
      return;
    }
    if (m.payload_type == ValueType::kVarChar) {
      out->type = ValueType::kVarChar;
      out->SetCollation(0);
      out->value.varchar_value = pb.pay_blk->VarcharAt(rin);
    } else if (m.payload_type == ValueType::kDouble) {
      const auto dv = std::bit_cast<double>(pb.pay_cells[rin]);
      out->type = ValueType::kDouble;
      out->SetCollation(0);
      out->value.double_value = dv == 0.0 ? 0.0 : dv;
    } else {
      out->type = m.payload_type;
      out->SetCollation(0);
      if (m.payload_unsigned) {
        out->SetUnsigned();
      }
      out->value.int_value = pb.pay_cells[rin];
    }
  }
};

struct FusedRawArena {
  std::vector<std::unique_ptr<FusedRawBlock>> blocks;
  size_t used{FusedRawBlock::kGroups};
  bool has_stat{false};
  bool has_double{true};
  bool has_int{true};
  void Initialize(FusedGroup& group, size_t aggregates) {
    if (group.raw_owner != nullptr || aggregates == 0) {
      return;
    }
    if (used == FusedRawBlock::kGroups) {
      blocks.push_back(std::make_unique<FusedRawBlock>(aggregates, has_stat,
                                                       has_double, has_int));
      used = 0;
    }
    const size_t base = used++ * aggregates;
    group.raw_owner = blocks.back().get();
    group.raw_base = base;
  }
};

struct CompactCountGroup {
  uint64_t hash{0};
  size_t next{SIZE_MAX};
  int64_t count{0};
  const Row* left{nullptr};
  uint32_t right_idx{kInvalidRightIdx};
};

struct FusedWorkerLocal {
  FusedRawArena raw_arena;
  GroupKeyMap offsets;
  // Single-INT64/DATE group-key fast path: raw-payload probes skip the
  // Value-vector hashing the generic map pays per pair.  Key spaces of the
  // two maps are disjoint (NULL keys route to the generic map, which groups
  // NULLs), so a group lives in exactly one of them.
  FlatIntKeyMap<size_t> int_offsets;
  std::vector<FusedGroup> groups;
  std::vector<CompactCountGroup> compact_counts;
  std::vector<std::vector<uint64_t>> compact_split_shards;
  size_t compact_count_inputs{0};
  bool compact_count_log{false};
  std::deque<AggregateAccumulator> states;
  // Reusable probe key: avoids a fresh vector per probe row (see
  // accumulate_into).  The transparent group-key hash/equal pair accepts the
  // bare value vector, so probes never construct a Row.
  std::vector<Value> key_scratch;
  std::vector<const Value*> key_ptrs;
  std::deque<Row> owned_rep_left;
  std::array<uint32_t, 8192> local_2key_gid{};
  bool local_2key_gid_init{false};
  std::array<uint32_t, 4> cur_pk_subgids{};
  bool have_cur_pk_subgids{false};
  Status error{Status::kSuccess};
  size_t comparisons{0};
};

struct LeftNumericAgg {
  int32_t col0{-1};
  int32_t col1{-1};
  int32_t orig_agg{-1};
  uint8_t op{255};
  bool is_int_col{false};
  bool case_else_null{false};
  uint8_t case_val_kind{0};
  const uint8_t* right_cond{nullptr};
  double scale{0};
  bool diff_has_lo{false};
  bool diff_has_hi{false};
  bool diff_lo_incl{false};
  bool diff_hi_incl{true};
  int64_t diff_lo{0};
  int64_t diff_hi{0};
  int32_t pk_lookup_idx{-1};
  int64_t pk_eq_val{0};
};

struct RadixSoABucket {
  std::vector<uint64_t> keys;
  std::vector<double> dvals;
  std::vector<int64_t> ivals;
  std::vector<uint8_t> valids;
};
inline constexpr size_t kRadixParts = 64;

}  // namespace tinylamb::relational_detail
