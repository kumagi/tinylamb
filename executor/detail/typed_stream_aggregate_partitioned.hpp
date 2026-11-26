/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_TYPED_STREAM_AGGREGATE_PARTITIONED_HPP
#define TINYLAMB_EXECUTOR_DETAIL_TYPED_STREAM_AGGREGATE_PARTITIONED_HPP

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/columnar_tlcs_fast_path.hpp"
#include "executor/detail/columnar_tlcs_internal.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/typed_stream_aggregate.hpp"
#include "query/statement.hpp"
#include "table/tlcs_file.hpp"
#include "type/schema.hpp"

namespace tinylamb::relational_detail {

struct PartitionedColumnarAggRequest {
  const SelectStatement& stmt;
  const std::shared_ptr<const TlcsFile>& tlcs;
  const Schema& qualified_schema;
  const Schema& table_schema;
  const ColumnarPredicateSet& preds;
  const std::vector<uint32_t>& candidate_blocks;
  const std::vector<ColGroupKeySpec>& key_specs;
  const std::vector<ColAggSpec>& agg_specs;
  const std::vector<PositionalHaving>& positional_having;
  const std::vector<OutputSlotBinding>& emit_bindings;
  const std::vector<ColumnarOrderBinding>& order_bindings;
  ExecutionRuntime* runtime{nullptr};
  bool publish_lstable_marker{false};
  const std::string& fold_cache_key;
  std::chrono::steady_clock::time_point scan_begin;
  double dir_ms{0.0};
  bool any_scalar_distinct{false};
};

// Sharded 2-pass parallel GROUP BY / COUNT(DISTINCT) engine directly over
// `TlcsFile` blocks (Wave B2). Handles skewed COUNT(DISTINCT) pairs via
// `(group_key, distinct_val)` pair-sharding + L1 dedup cache, pure fixed-width
// numeric multi-keys via L3-sized shard flat hash tables, and high-cardinality
// VarChar/computed keys via self-tuning L1 combiners + progressive slab
// release. Falls back to `std::nullopt` whenever `QueryMemoryCharge` cannot fit
// or the shape is outside supported fast specializations.
StatusOr<std::optional<Relation>> TryRunPartitionedColumnarTlcsAggregate(
    const PartitionedColumnarAggRequest& req);

// Lean proof:
// lean/TinylambOpt/ResearchProofs/BoundedMultiKeyDirectArrayAggregate.lean
// (sound)
StatusOr<std::optional<Relation>> TryRunDirectTwoKeyBoundedIntAggregate(
    const PartitionedColumnarAggRequest& req,
    const KeyProjectionMap* precomputed_km = nullptr);

// Lean proof:
// lean/TinylambOpt/ResearchProofs/MultiBlockLocalDictTwoStageAggregate.lean
// (sound)
StatusOr<std::optional<Relation>> TryRunSingleVarCharLocalDictAggregate(
    const PartitionedColumnarAggRequest& req,
    const KeyProjectionMap* precomputed_km = nullptr);

}  // namespace tinylamb::relational_detail

#endif  // TINYLAMB_EXECUTOR_DETAIL_TYPED_STREAM_AGGREGATE_PARTITIONED_HPP
