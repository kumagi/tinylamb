/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#pragma once
#include "executor/detail/fused_group_storage.hpp"
#include "executor/detail/fused_raw_aggregates.hpp"

namespace tinylamb::relational_detail {

struct DenseVarcharGroups {
  const FusedRawAggregates& raw_aggregates;
  TransactionContext& context;
  const Relation& left;
  GroupedJoinFusionSpec* fusion{nullptr};
  bool debug_fusion{false};
  const Relation& right;
  std::chrono::steady_clock::time_point join_begin;
  Relation& result;
  size_t left_width{0};
  const std::vector<ProgramShape>& key_shapes;
  const std::vector<ProgramShape>& child_shapes;
  const std::vector<slot_t>& left_columns;
  const std::vector<int64_t>& left_key_offsets;
  const FusedRightCellReader& read_right_cell;
  size_t right_row_count{0};
  size_t workers{0};
  const std::vector<uint32_t>& dense_int_idxs;
  int64_t dense_int_min{0};
  [[nodiscard]] StatusOr<bool> TryAggregate() const;
};

}  // namespace tinylamb::relational_detail
