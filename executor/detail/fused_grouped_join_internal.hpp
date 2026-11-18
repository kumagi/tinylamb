/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/fused_grouped_join.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "expression/bytecode.hpp"
#include "table/lstable_bridge.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

inline constexpr uint32_t kInvalidRightIdx =
    std::numeric_limits<uint32_t>::max();

struct UniqueIntSlot {
  int64_t key{0};
  uint32_t r_idx{std::numeric_limits<uint32_t>::max()};
  uint32_t pad{0};
};

inline size_t HashUniqueIntKey(int64_t k) noexcept {
  uint64_t h = static_cast<uint64_t>(k) * 0x9e3779b97f4a7c15ULL;
  h ^= h >> 32;
  return static_cast<size_t>(h);
}

inline bool TrySetConstantColumnarColumn(ColumnarColumn* col,
                                         ValueType fallback_vt,
                                         bool fallback_unsigned,
                                         bool fallback_interval,
                                         const Value& cv, size_t count,
                                         ColumnarChunk* chunk) {
  if (cv.IsNull()) {
    col->SetConstantNull(fallback_vt, fallback_unsigned, fallback_interval,
                         count);
    return true;
  }
  if (cv.type == ValueType::kArray || cv.IsCaseInsensitive()) {
    return false;
  }
  if ((cv.type == ValueType::kInt64 || cv.type == ValueType::kDate) &&
      (cv.IsUnsigned() || cv.Collation() == 0)) {
    col->SetConstantInt(cv.type, cv.IsUnsigned(), cv.value.int_value, count);
    return true;
  }
  if (cv.type == ValueType::kDouble && cv.Collation() == 0) {
    col->SetConstantDouble(cv.value.double_value, count);
    return true;
  }
  if (cv.type == ValueType::kVarChar && cv.Collation() == 0) {
    ChunkStringArena* arena = chunk->EnsureArena();
    col->SetConstantString(arena->Store(cv.value.varchar_value),
                           cv.IsInterval(), count, chunk->string_arena);
    return true;
  }
  return false;
}

struct DistinctLSTableProbeOutcome {
  bool handled{false};
  std::optional<Relation> result;
};

struct DistinctLSTableProbeContext {
  TransactionContext& context;
  const Relation& left;
  const Relation& right;
  GroupedJoinFusionSpec* fusion{nullptr};
  Relation& result;
  const std::optional<FlatJoinIdxBuckets<int64_t>>& int_buckets;
  const std::vector<EqualityKey>& equality_keys;
  const std::vector<BytecodeProgram>& residual_programs;
  const DeferredLSTableProbe* deferred_left_probe{nullptr};
  const std::vector<BytecodeProgram>& key_programs;
  const std::vector<ProgramShape>& key_shapes;
  size_t left_width{0};
  const std::vector<uint8_t>& def_overwritten_by_pk;
  const std::vector<slot_t>& left_columns;
  const std::vector<int64_t>& left_key_offsets;
  std::chrono::steady_clock::time_point join_begin;
  const std::vector<uint32_t>& dense_int_idxs;
  int64_t dense_int_min{0};
  const std::vector<UniqueIntSlot>& unique_int_table;
  size_t unique_int_mask{0};
  size_t worker_count{1};
  JoinType join_type{JoinType::kInner};
  bool use_dense_int_rows{false};
  bool use_unique_int_table{false};
  bool singleton_right_relation{false};
  bool integer_key{false};
  bool packed_2x32_int_key{false};
  bool prepared_pk_lookups_empty{true};
  bool def_local_preds_ok{false};
  bool distinct_fact_singleton{false};
  bool debug_fusion{false};
};

StatusOr<DistinctLSTableProbeOutcome> TryDistinctLSTableProbe(
    const DistinctLSTableProbeContext& ctx);

}  // namespace tinylamb::relational_detail
