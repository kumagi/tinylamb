/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#pragma once
#include "executor/detail/fused_group_probe.hpp"

namespace tinylamb::relational_detail {

struct DenseFilteredGroups {
  TransactionContext& context;
  const Relation& left;
  GroupedJoinFusionSpec* fusion{nullptr};
  std::chrono::steady_clock::time_point join_begin;
  size_t workers{0};
  const std::vector<uint32_t>& dense_int_idxs;
  int64_t dense_int_min{0};
  const tinylamb::LSTableStore& store;
  size_t g_pcol{0};
  const LeftNumericAgg& lop;
  slot_t g_base{0};
  slot_t j_base{0};
  slot_t c0_base{0};
  slot_t c1_base{0};
  [[nodiscard]] StatusOr<std::optional<Relation>> TryRun() const;
};

struct DenseProfitGroups {
  TransactionContext& context;
  const Relation& left;
  const Relation& right_arg;
  GroupedJoinFusionSpec* fusion{nullptr};
  bool right_col_only{false};
  std::vector<const Row*> build_rows;
  const Relation& right;
  std::chrono::steady_clock::time_point join_begin;
  Relation& result;
  size_t left_width{0};
  const std::vector<ProgramShape>& key_shapes;
  const std::vector<ProgramShape>& child_shapes;
  const std::vector<slot_t>& left_columns;
  const std::vector<int64_t>& left_key_offsets;
  const std::array<int16_t, 25568>& kFastYearLut;
  const std::vector<FusedWorkerLocal>& locals;
  const std::vector<UniqueIntSlot>& unique_int_table;
  size_t unique_int_mask{0};
  int rk{0};
  int lk{0};
  [[nodiscard]] StatusOr<bool> TryRun() const;
};

}  // namespace tinylamb::relational_detail
