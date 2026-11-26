/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#pragma once
#include "executor/detail/fused_group_storage.hpp"

namespace tinylamb::relational_detail {

struct FusedAggregateInputs {
  const Relation& left;
  const Relation& right_arg;
  JoinType join_type;
  GroupedJoinFusionSpec* fusion;
  const DeferredLSTableProbe* deferred_left_probe;
  size_t effective_left_rows;
  bool right_col_only;
  const Relation* right_ptr;
  const Relation& right;
  const Relation& result;
  size_t left_width;
  size_t right_width;
  const std::vector<BytecodeProgram>& child_programs;
  size_t right_build_count;
  const std::vector<ProgramShape>& child_shapes;
};
struct FusedRawAggregates {
  bool count_star_only{};
  bool count_nonnull_only{};
  std::optional<slot_t> count_right_column{};
  bool unconditional_matched_count{};
  bool raw_count_fusion{};
  std::array<RawAggKind, kRawAggregateCapacity> raw_agg_kinds{};
  std::array<Value, kRawAggregateCapacity> raw_constants{};
  bool raw_sum_fusion{};
  bool has_count_distinct{};
  std::array<int8_t, kRawAggregateCapacity> raw_agg_slot{};
  std::array<uint8_t, kRawAggregateCapacity> raw_slot_origin{};
  std::array<RawAggKind, kRawAggregateCapacity> slot_agg_kinds{};
  std::array<bool, kRawAggregateCapacity> raw_const_folded{};
  size_t raw_slot_count{};
  std::array<bool, kRawAggregateCapacity> slot_raises_inf{};
  bool skip_accumulator_states{};
  const char* Classify(const FusedAggregateInputs& input);
};

}  // namespace tinylamb::relational_detail
