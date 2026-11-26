/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#pragma once
#include "executor/detail/fused_group_key_layout.hpp"
#include "executor/detail/fused_raw_aggregates.hpp"

namespace tinylamb::relational_detail {

struct CompactRadixGroups {
  const FusedRawAggregates& raw_aggregates;
  const FusedGroupKeyLayout& layout;
  TransactionContext& context;
  const Relation& left;
  const Relation& right_arg;
  GroupedJoinFusionSpec* fusion{nullptr};
  bool right_col_only{false};
  const std::shared_ptr<const ColumnarTable>& right_col_owner;
  const RightColLocator& right_locator;
  const Relation& right;
  std::chrono::steady_clock::time_point join_begin;
  Relation& result;
  const std::vector<EqualityKey>& equality_keys;
  size_t left_width{0};
  const std::vector<PreparedDeferredPkLookup>& prepared_pk_lookups;
  const std::vector<PreparedDeferredPkGroup>& def_pk_groups;
  const std::array<int8_t, 16>& def_col_to_pk_lookup;
  bool has_stat_raw_agg{false};
  size_t workers{0};
  std::vector<FusedWorkerLocal>& locals;
  size_t agg_count{0};
  const std::array<LeftNumericAgg, kRawAggregateCapacity>& left_numeric;
  bool radix_tuple_is_int{false};
  std::shared_ptr<QueryMemoryCharge>& bounded_radix_charge;
  std::vector<std::array<RadixSoABucket, kRadixParts>>& worker_radix_buckets;
  bool track_group_phases{false};
  [[nodiscard]] StatusOr<bool> TryEmit() const;
};

}  // namespace tinylamb::relational_detail
