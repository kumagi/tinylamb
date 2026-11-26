/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#pragma once
#include "executor/detail/fused_group_key_layout.hpp"
#include "executor/detail/fused_raw_aggregates.hpp"

namespace tinylamb::relational_detail {
struct FusedGroupEmitter {
  const FusedRawAggregates& raw_aggregates;
  const FusedGroupKeyLayout& layout;
  TransactionContext& context;
  const Relation& left;
  const Relation& right_arg;
  const CteMap& ctes;
  const Scope* outer{nullptr};
  GroupedJoinFusionSpec* fusion{nullptr};
  const DeferredLSTableProbe* deferred_left_probe{nullptr};
  size_t effective_left_rows{0};
  const std::shared_ptr<const ColumnarTable>& right_col_owner;
  const RightColLocator& right_locator;
  const Relation* right_ptr{nullptr};
  const Relation& right;
  std::chrono::steady_clock::time_point join_begin;
  Relation& result;
  const std::vector<EqualityKey>& equality_keys;
  size_t left_width{0};
  size_t right_width{0};
  const std::vector<ProgramShape>& key_shapes;
  const Value& kNullCell;
  const FusedRightCellReader& read_right_cell;
  const std::vector<uint8_t>& def_overwritten_by_pk;
  const std::vector<PreparedDeferredPkLookup>& prepared_pk_lookups;
  const std::vector<PreparedDeferredPkGroup>& def_pk_groups;
  const std::array<int8_t, 16>& def_col_to_pk_lookup;
  const PreparedPkCellReader& read_pk_cell;
  std::chrono::steady_clock::time_point phase_buckets_done;
  std::chrono::steady_clock::time_point phase_states_done;
  std::chrono::steady_clock::time_point phase_probe_begin;
  std::chrono::steady_clock::time_point phase_merge_begin;
  std::chrono::steady_clock::time_point& phase_merge_done;
  std::chrono::steady_clock::time_point& phase_emit_begin;
  std::vector<FusedGroup>& groups;
  std::deque<AggregateAccumulator>& states;
  std::vector<FusedGroup*>& group_ptrs;
  JoinType join_type{JoinType::kInner};
  bool right_col_only{false};
  bool has_double_raw_agg{false};
  bool has_int_raw_agg{false};
  bool has_stat_raw_agg{false};
  bool track_group_phases{false};
  bool use_group_ptrs{false};

  [[nodiscard]] Status Emit() const;
};

}  // namespace tinylamb::relational_detail
