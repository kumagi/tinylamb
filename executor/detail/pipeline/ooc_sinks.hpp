/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_PIPELINE_OOC_SINKS_HPP
#define TINYLAMB_EXECUTOR_DETAIL_PIPELINE_OOC_SINKS_HPP

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/query_grant.hpp"
#include "common/runtime_filter.hpp"
#include "common/set_operation.hpp"
#include "common/spill/spill_store.hpp"
#include "common/status_or.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/ooc/external_sorter.hpp"
#include "executor/detail/ooc/recursive_partitioner.hpp"
#include "executor/detail/ooc/spillable_store.hpp"
#include "executor/detail/pipeline/pipeline_ir.hpp"
#include "executor/grouped_aggregate.hpp"
#include "executor/hash_join.hpp"
#include "executor/join_kind.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/expression.hpp"
#include "expression/jit.hpp"
#include "expression/named_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "page/row_position.hpp"
#include "type/collation.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class Index;
class SelectStatement;
class Table;
struct RecursiveDepthSpec;

// Evaluates `expr` on a single `row` resolved against `schema` cleanly without
// throwing exceptions across Status boundaries.
[[nodiscard]] StatusOr<Value> EvalRowExpression(const Expression& expr,
                                                const Schema& schema,
                                                const Row& row);

// --- 1. OOC Sort & TopN Sink (`ExternalSorter`) ---

class OocSortSink final : public PipelineSink {
 public:
  struct KeySpec {
    Expression expression;
    bool ascending{true};
    std::optional<bool> nulls_first{std::nullopt};
    Collation collation{Collation::kNone};
    // When the key is a direct input column, use this slot instead of
    // resolving expression->name against the schema (names can collide).
    std::optional<size_t> column_slot{std::nullopt};
  };

  OocSortSink(std::vector<KeySpec> keys, Schema input_schema,
              std::optional<size_t> limit = std::nullopt, size_t offset = 0,
              std::string label = "Sort");

  Status Init(PipelineContext* ctx, size_t worker_count) override;
  Status Consume(PipelineContext* ctx, size_t worker_id,
                 const DataChunk& input) override;
  StatusOr<std::shared_ptr<PipelineSource>> Finalize(
      PipelineContext* ctx) override;
  [[nodiscard]] std::string Describe() const override;
  void Explain(std::ostream& out, int indent) const override;
  void DumpExplain(std::ostream& out, int indent) const override;

  [[nodiscard]] bool Spilled() const noexcept {
    return sorter_ && sorter_->Spilled();
  }

 private:
  [[nodiscard]] Status EnsureSorter(PipelineContext* ctx, size_t payload_cols);

  std::vector<KeySpec> keys_;
  Schema input_schema_;
  std::optional<size_t> limit_;
  size_t offset_{0};
  std::string label_;

  std::vector<std::optional<slot_t>> direct_slots_;
  std::vector<size_t> computed_key_indices_;
  size_t payload_cols_{0};
  int64_t seq_{0};
  size_t input_rows_{0};
  size_t output_rows_{0};
  bool finalized_{false};
  std::unique_ptr<ExternalSorter> sorter_;
  std::optional<QueryMemoryGrant::ActiveConsumerGuard> consumer_guard_;
};

// --- 2. OOC Distinct Sink (`RecursivePartitioner` + `ExternalSorter` skew) ---

class OocDistinctSink final : public PipelineSink {
 public:
  OocDistinctSink(Schema input_schema, std::vector<slot_t> key_cols = {},
                  std::string label = "Distinct",
                  std::vector<Expression> distinct_on_exprs = {},
                  bool preserve_input_order = false);
  OocDistinctSink(const OocDistinctSink&) = delete;
  OocDistinctSink& operator=(const OocDistinctSink&) = delete;
  OocDistinctSink(OocDistinctSink&&) = delete;
  OocDistinctSink& operator=(OocDistinctSink&&) = delete;
  ~OocDistinctSink() override;

  Status Init(PipelineContext* ctx, size_t worker_count) override;
  Status Consume(PipelineContext* ctx, size_t worker_id,
                 const DataChunk& input) override;
  StatusOr<std::shared_ptr<PipelineSource>> Finalize(
      PipelineContext* ctx) override;
  [[nodiscard]] std::string Describe() const override { return label_; }
  void DumpExplain(std::ostream& out, int indent) const override;

 private:
  [[nodiscard]] Status TransitionToSpill(PipelineContext* ctx);
  [[nodiscard]] StatusOr<Row> ExtractKey(const Row& row) const;
  Status SpillRow(RecursivePartitioner& partitioner, Row row, uint64_t seq);

  Schema input_schema_;
  std::vector<slot_t> key_cols_;
  std::string label_;
  std::vector<Expression> distinct_on_exprs_;
  bool preserve_input_order_{false};

  std::optional<QueryMemoryGrant::ActiveConsumerGuard> consumer_guard_;
  std::optional<QueryMemoryGrant::LocalBatchCache> grant_cache_;
  relational_detail::GroupKeyMap index_map_;
  std::vector<Row> unique_rows_;
  size_t mem_bytes_{0};
  size_t budget_bytes_{0};
  bool spilled_{false};
  uint64_t next_seq_{0};
  std::unique_ptr<RecursivePartitioner> partitioner_;
  // Sticky error from the partitioner's hash functor: its uint64_t-only
  // signature cannot return Status, so ExtractKey failures are recorded here
  // and surfaced at Finalize instead of silently bucketing every row to 0.
  Status partition_key_status_{Status::kSuccess};
};

// --- 3. OOC Grouped Aggregation Sink ---

class OocGroupAggSink final : public PipelineSink {
 public:
  // Outputs columns in exact order: [group_keys[0..G-1], aggregates[0..A-1]].
  OocGroupAggSink(Schema input_schema, std::vector<Expression> group_keys,
                  std::vector<const AggregateExpression*> aggregates,
                  Schema output_schema, std::string label = "GroupAgg");
  OocGroupAggSink(Schema input_schema, std::vector<Expression> group_keys,
                  std::vector<Expression> agg_expressions, Schema output_schema,
                  std::string label = "GroupAgg");
  OocGroupAggSink(const OocGroupAggSink&) = delete;
  OocGroupAggSink& operator=(const OocGroupAggSink&) = delete;
  OocGroupAggSink(OocGroupAggSink&&) = delete;
  OocGroupAggSink& operator=(OocGroupAggSink&&) = delete;
  ~OocGroupAggSink() override;

  Status Init(PipelineContext* ctx, size_t worker_count) override;
  Status Consume(PipelineContext* ctx, size_t worker_id,
                 const DataChunk& input) override;
  StatusOr<std::shared_ptr<PipelineSource>> Finalize(
      PipelineContext* ctx) override;
  [[nodiscard]] StatusOr<std::shared_ptr<PipelineSource>>
  TryConsumeExactRowCount(PipelineContext* ctx, uint64_t row_count) override;
  [[nodiscard]] std::string Describe() const override { return label_; }
  void SetExplainHeader(std::string header) {
    explain_header_ = std::move(header);
  }
  void Explain(std::ostream& out, int indent) const override;
  void DumpExplain(std::ostream& out, int indent) const override;

 private:
  [[nodiscard]] Status ExtractCanonicalGroupKeys(const Row& input_row,
                                                 Row* canonical_out) const;
  [[nodiscard]] Status FlushPartialTableToPartitioner();
  [[nodiscard]] Status TransitionNonCombinableToSpill(PipelineContext* ctx);

  Schema input_schema_;
  std::vector<Expression> group_keys_;
  std::vector<Expression> aggregate_owners_;
  std::vector<const AggregateExpression*> aggregates_;
  Schema output_schema_;
  std::string label_;
  std::string explain_header_;

  std::vector<relational_detail::AggregateAccumulator::Shape> shapes_;
  std::vector<std::optional<slot_t>> group_key_slots_;
  bool all_combinable_{false};
  bool has_collection_agg_{false};
  std::optional<slot_t> single_distinct_child_slot_;
  size_t single_distinct_agg_index_{0};
  // Owned DISTINCT-stripped clone for the O(1) sorted skew fallback when
  // the aggregate list has one plain COUNT/SUM/AVG(DISTINCT expr) and only
  // non-distinct non-collection siblings.
  std::shared_ptr<AggregateExpression> skew_strip_aggregate_;

  std::optional<QueryMemoryGrant::ActiveConsumerGuard> consumer_guard_;
  std::optional<QueryMemoryGrant::LocalBatchCache> grant_cache_;
  size_t budget_bytes_{0};
  bool spilled_{false};
  bool finalized_{false};
  uint64_t total_input_rows_{0};
  std::shared_ptr<std::atomic<size_t>> output_groups_;

  std::unique_ptr<AggregateGroupTable> agg_table_;
  std::vector<Row> raw_rows_;
  size_t mem_bytes_{0};
  std::unique_ptr<RecursivePartitioner> partitioner_;
  // Sticky error from the partitioner's hash functor (uint64_t-only signature):
  // ExtractCanonicalGroupKeys failures must reach Finalize instead of being
  // swallowed into bucket 0 and producing a wrong grouping.
  Status partition_key_status_{Status::kSuccess};
};

// --- 4. OOC Adaptive Grace Hash Join (`OocHashBuildSink` + the OOC hash-join
// probe source)
// ---

struct OocHashJoinSpec {
  std::vector<slot_t> left_cols;
  std::vector<slot_t> right_cols;
  Schema left_schema;
  Schema right_schema;
  JoinKind kind{JoinKind::kInner};
  std::vector<bool> key_null_safe;
  Expression residual_predicate;
  Schema residual_schema;
  bool build_left{false};
  bool semi_reduction{false};
  bool dynamic_filter{false};
  // Optional dynamic runtime filter target on the probe scan source.
  std::shared_ptr<PipelineSource> runtime_filter_probe_target;
  std::optional<slot_t> runtime_filter_probe_col;
  ValueType runtime_filter_probe_type{ValueType::kNull};
};

struct OocBuildSpillBucket {
  SpillSegmentId segment_id{kInvalidSpillSegmentId};
  uint64_t row_count{0};
  uint64_t estimated_bytes{0};
};

struct OocHashJoinContext {
  explicit OocHashJoinContext(OocHashJoinSpec spec_in)
      : spec(std::move(spec_in)),
        any_null_safe(std::ranges::any_of(spec.key_null_safe,
                                          [](bool safe) { return safe; })) {}
  OocHashJoinContext(const OocHashJoinContext&) = delete;
  OocHashJoinContext& operator=(const OocHashJoinContext&) = delete;
  OocHashJoinContext(OocHashJoinContext&&) = delete;
  OocHashJoinContext& operator=(OocHashJoinContext&&) = delete;
  ~OocHashJoinContext();

  SpillStore* spill_store{nullptr};
  std::weak_ptr<const bool> spill_store_alive;
  uint64_t build_total_rows{0};

  // Semi-reduction runtime state (only used when spec.semi_reduction holds).
  size_t semi_reduction_probe_keys{0};
  size_t semi_build_rows{0};
  size_t semi_build_rows_kept{0};
  size_t semi_duplicate_keys_pruned{0};
  std::unique_ptr<SpillableStore> reduced_probe_store;
  std::unique_ptr<JoinHashIndex> semi_probe_domain;

  // Dynamic-filter runtime state.
  size_t dynamic_filter_keys{0};
  size_t actual_probe_rows{0};
  size_t join_matches{0};
  size_t dynamic_filter_probe_rows{0};
  size_t dynamic_filter_rejected{0};
  size_t dynamic_filter_null_rejected{0};

  // Spilled Grace Hash build buckets (`spilled`):
  size_t io_buf_bytes{16384};
  uint64_t total_build_bytes{0};

  // Explain snapshots / weak handles for child rendering (never pin memory).
  std::weak_ptr<PipelineSource> build_source_weak;
  std::weak_ptr<PipelineSource> probe_source_weak;
  std::shared_ptr<const RuntimeFilterGroup> runtime_filter;
  std::vector<std::shared_ptr<PipelineOperator>> build_ops_explain;
  std::vector<std::shared_ptr<PipelineOperator>> probe_ops_explain;
  std::vector<uint8_t> semi_kept_ordinals;
  std::vector<Row> build_rows;
  std::vector<RowPosition> build_positions;
  std::vector<uint8_t> build_matched;
  std::vector<OocBuildSpillBucket> build_buckets;
  std::string build_explain_snapshot;
  std::string probe_explain_snapshot;
  std::string semi_reduction_description;
  std::string dynamic_filter_description;

  // In-memory fast-path build state (`!spilled`):
  JoinHashIndex hash_index;
  OocHashJoinSpec spec;
  uint32_t root_fanout_bits{4};
  bool finalized{false};
  bool spilled{false};
  bool any_null_safe{false};
  bool build_has_null_key{false};
  bool semi_intake_completed{false};
  bool semi_reduction_active{false};
  bool dynamic_filter_published{false};
  bool small_build_runtime_filter{false};
  mutable bool probe_opened{false};
  JoinHashIndex::KeyMode key_mode{JoinHashIndex::KeyMode::kBytes};
  ValueType int_key_type{ValueType::kInt64};
  // Held from Build through Probe EOF when build stays in memory so probe-side
  // operators see accurate ActiveConsumerCount and resident hash/row charges.
  std::optional<QueryMemoryGrant::ActiveConsumerGuard> resident_guard;
  std::unique_ptr<QueryMemoryGrant::LocalBatchCache> resident_grant_cache;

  void ReleaseResidentMemory() noexcept;
};

// Optional pre-build stage for Semi/Anti joins when SemiReduction is active:
// consumes & spills the probe stream into `join_ctx->reduced_probe_store`
// while collecting the bounded probe key domain in
// `join_ctx->semi_probe_domain`.
class OocSemiProbeIntakeSink final : public PipelineSink {
 public:
  explicit OocSemiProbeIntakeSink(std::shared_ptr<OocHashJoinContext> join_ctx,
                                  std::string label = "SemiProbeIntakeSink");
  OocSemiProbeIntakeSink(const OocSemiProbeIntakeSink&) = delete;
  OocSemiProbeIntakeSink& operator=(const OocSemiProbeIntakeSink&) = delete;
  OocSemiProbeIntakeSink(OocSemiProbeIntakeSink&&) = delete;
  OocSemiProbeIntakeSink& operator=(OocSemiProbeIntakeSink&&) = delete;
  ~OocSemiProbeIntakeSink() override;

  Status Init(PipelineContext* ctx, size_t worker_count) override;
  Status Consume(PipelineContext* ctx, size_t worker_id,
                 const DataChunk& input) override;
  StatusOr<std::shared_ptr<PipelineSource>> Finalize(
      PipelineContext* ctx) override;
  [[nodiscard]] std::string Describe() const override { return label_; }

 private:
  std::shared_ptr<OocHashJoinContext> join_ctx_;
  std::string label_;
  std::optional<QueryMemoryGrant::ActiveConsumerGuard> consumer_guard_;
  std::optional<QueryMemoryGrant::LocalBatchCache> grant_cache_;
  size_t budget_bytes_{0};
  size_t max_domain_keys_{size_t{1} << 20};
  size_t domain_bytes_{0};
  size_t domain_keys_{0};
  bool domain_capped_{false};
  std::unique_ptr<JoinHashIndex> domain_;
  std::string key_scratch_;
};

// Stage 0 sink: builds hash index in memory up to B_op or spills build rows
// into radix buckets using `RecursivePartitioner::RadixBucket`.
class OocHashBuildSink final : public PipelineSink {
 public:
  explicit OocHashBuildSink(std::shared_ptr<OocHashJoinContext> join_ctx,
                            std::string label = "HashBuildSink");
  OocHashBuildSink(const OocHashBuildSink&) = delete;
  OocHashBuildSink& operator=(const OocHashBuildSink&) = delete;
  OocHashBuildSink(OocHashBuildSink&&) = delete;
  OocHashBuildSink& operator=(OocHashBuildSink&&) = delete;
  ~OocHashBuildSink() override;

  Status Init(PipelineContext* ctx, size_t worker_count) override;
  Status Consume(PipelineContext* ctx, size_t worker_id,
                 const DataChunk& input) override;
  StatusOr<std::shared_ptr<PipelineSource>> Finalize(
      PipelineContext* ctx) override;
  [[nodiscard]] std::string Describe() const override { return label_; }

 private:
  [[nodiscard]] Status TransitionToSpill(PipelineContext* ctx);

  std::shared_ptr<OocHashJoinContext> join_ctx_;
  std::string label_;
  std::optional<QueryMemoryGrant::ActiveConsumerGuard> consumer_guard_;
  size_t budget_bytes_{0};
  size_t mem_bytes_{0};
  bool spilled_{false};
  std::vector<std::unique_ptr<SpillSegmentWriter>> bucket_writers_;
  std::vector<DataChunk> bucket_chunks_;
  std::vector<size_t> bucket_batch_bytes_;
  std::vector<uint64_t> bucket_row_counts_;
  std::vector<uint64_t> bucket_est_bytes_;
  size_t bucket_total_batch_bytes_{0};
  std::string key_scratch_;
  std::string codec_scratch_;
  std::unique_ptr<JoinHashIndex> semi_seen_keys_;
  size_t semi_seen_bytes_{0};
  bool semi_seen_capped_{false};
};

class FilteredHashProbe {
 public:
  enum class Mode { kDirect, kCompact };

  [[nodiscard]] static Mode ChooseMode(const JoinHashIndex& index,
                                       const int64_t* filter, size_t count,
                                       BinaryOperation op, int64_t constant);
  // Both columns are contiguous, non-NULL signed INT64 vectors. Their
  // storage and the resident hash index must outlive the cursor.
  void Start(Mode mode, const JitInt64Kernels& kernel,
             const JoinHashIndex& index, const int64_t* filter,
             const int64_t* keys, size_t count, int64_t constant);
  [[nodiscard]] size_t Next(size_t* row, size_t* accepted);

 private:
  struct Candidate {
    size_t row;
    uint64_t hash;
  };
  const JitInt64Kernels* kernel_{nullptr};
  const JoinHashIndex* index_{nullptr};
  JoinHashIndex::Int64Slots slots_{};
  const int64_t* filter_{nullptr};
  const int64_t* keys_{nullptr};
  size_t count_{0};
  int64_t constant_{0};
  size_t next_{0};
  uint64_t direct_cursor_{0};
  Mode mode_{Mode::kDirect};
  std::vector<uint8_t> mask_;
  std::vector<Candidate> candidates_;
};

// Stage 1 adaptive probe source:
// - When `!join_ctx->spilled`, pulls probe subpipeline chunks streamingly and
//   probes `join_ctx->hash_index` in O(1) probe RAM (plus Right/Full outer
//   unmatched build emission at EOF).
// - When `join_ctx->spilled`, partitions probe chunks across identical radix
//   buckets and runs partition-by-partition Grace Hash Join (with recursive
//   splitting & Sort-Merge `ExternalSorter` fallback on skew).
class OocAdaptiveHashProbeSource final : public PipelineSource {
 public:
  struct ActiveProbeRowState {
    bool active{false};
    bool matched_any{false};
    bool has_non_safe_null{false};
    uint64_t gidx{0};
    size_t next_entry{JoinHashIndex::kNil};
    Row row;
    RowPosition pos;

    void Reset() {
      active = false;
      matched_any = false;
      has_non_safe_null = false;
      gidx = 0;
      next_entry = JoinHashIndex::kNil;
      row = Row();
      pos = RowPosition();
    }
  };

  OocAdaptiveHashProbeSource(
      std::shared_ptr<OocHashJoinContext> join_ctx,
      std::shared_ptr<PipelineSource> probe_source,
      std::vector<std::shared_ptr<PipelineOperator>> probe_ops,
      std::string label = "HashJoin");
  OocAdaptiveHashProbeSource(const OocAdaptiveHashProbeSource&) = delete;
  OocAdaptiveHashProbeSource& operator=(const OocAdaptiveHashProbeSource&) =
      delete;
  OocAdaptiveHashProbeSource(OocAdaptiveHashProbeSource&&) = delete;
  OocAdaptiveHashProbeSource& operator=(OocAdaptiveHashProbeSource&&) = delete;
  ~OocAdaptiveHashProbeSource() override;

  Status Open(PipelineContext* ctx) override;
  StatusOr<bool> NextBatch(PipelineContext* ctx, DataChunk* out,
                           size_t max_rows) override;
  [[nodiscard]] std::string Describe() const override { return label_; }
  void Explain(std::ostream& out, int indent) const override;
  void DumpExplain(std::ostream& out, int indent) const override;

 private:
  void RenderTree(std::ostream& out, int indent, bool is_dump) const;
  struct PendingJoinPair {
    SpillSegmentId build_seg{kInvalidSpillSegmentId};
    uint64_t build_rows{0};
    uint64_t build_bytes{0};
    uint64_t build_parent_bytes{0};
    SpillSegmentId probe_seg{kInvalidSpillSegmentId};
    uint64_t probe_rows{0};
    uint64_t probe_bytes{0};
    uint32_t depth{0};
    uint32_t bit_offset{0};
  };

  struct GracePartitionCursor;

  [[nodiscard]] Status PullNextProbeChunk(PipelineContext* ctx, DataChunk* out);
  void EnableFilteredProbe();
  [[nodiscard]] bool StartFilteredProbe(const DataChunk& batch);
  [[nodiscard]] Status PrepareGracePartitions(PipelineContext* ctx);
  [[nodiscard]] Status AdvanceGraceCursor(PipelineContext* ctx);
  [[nodiscard]] Status OpenNextPartitionPair(PipelineContext* ctx,
                                             PendingJoinPair pair);
  void CleanupGracePairs() noexcept;

  std::shared_ptr<OocHashJoinContext> join_ctx_;
  std::shared_ptr<PipelineSource> probe_source_;
  std::vector<std::shared_ptr<PipelineOperator>> probe_ops_;
  std::string label_;

  bool opened_{false};
  bool probe_eof_{false};
  bool drain_unmatched_build_{false};
  size_t unmatched_build_idx_{0};

  // Streaming in-memory probe state:
  DataChunk active_probe_chunk_;
  size_t active_probe_row_{0};
  ActiveProbeRowState active_probe_state_;
  std::string key_scratch_;
  struct FilteredProbePredicate {
    slot_t column;
    slot_t key;
    BinaryOperation operation;
    int64_t constant;
  };
  std::optional<FilteredProbePredicate> filtered_probe_predicate_;
  const Schema* filtered_probe_input_{nullptr};
  size_t filtered_probe_filter_idx_{0};
  std::vector<slot_t> filtered_probe_output_slots_;
  std::optional<FilteredHashProbe::Mode> forced_probe_mode_;
  std::optional<JitInt64Kernels> direct_probe_kernel_;
  std::optional<JitInt64Kernels> compact_probe_kernel_;
  FilteredHashProbe filtered_probe_;
  size_t filtered_probe_rows_seen_{0};
  size_t direct_probe_batches_{0};
  size_t compact_probe_batches_{0};
  bool filtered_probe_active_{false};
  bool filtered_probe_exact_runtime_filter_{false};

  // Spilled Grace Hash state:
  std::vector<PendingJoinPair> pending_pairs_;
  std::unique_ptr<GracePartitionCursor> active_pair_cursor_;
  std::optional<QueryMemoryGrant::ActiveConsumerGuard> consumer_guard_;
  std::optional<QueryMemoryGrant::LocalBatchCache> grant_cache_;
};

// Out-of-core Window function evaluation sink (`ooca-v2.md` §5.3 P3b).
//
// Buffers input rows up to max(kMinWindowMemBudget, budget_bytes_ / 2)
// on small inputs (zero spill
// overhead and native input-order preservation), then transitions to sorted
// `ExternalSorter` passes grouped by unique `(PARTITION BY, ORDER BY)` specs:
// - Normal partitions (`<= max(16 KiB, B_op / 2)`) evaluate via canonical
// `ComputeOneWindow`.
// - Overflowing single partitions (G4 skew: e.g. `no PARTITION BY` over 1M+
//   rows under tiny `B_op`) spill to a single `SpillStore` segment in exact
//   sorted order and evaluate stream-by-stream (`O(1)` ranking/lag/lead/prefix
//   accumulators + bounded-memory sliding frame cursors) within `B_op`.
class OocWindowSink final : public PipelineSink {
 public:
  OocWindowSink(Schema input_schema, Schema output_schema,
                std::vector<NamedExpression> outputs,
                bool preserve_input_order = false,
                bool spill_capable_plan = false,
                size_t plan_memory_budget_bytes = 0);

  Status Init(PipelineContext* ctx, size_t worker_count) override;
  Status Consume(PipelineContext* ctx, size_t worker_id,
                 const DataChunk& input) override;
  StatusOr<std::shared_ptr<PipelineSource>> Finalize(
      PipelineContext* ctx) override;
  [[nodiscard]] std::string Describe() const override;
  void Explain(std::ostream& out, int indent) const override;
  void DumpExplain(std::ostream& out, int indent) const override;

  [[nodiscard]] bool Spilled() const noexcept;
  [[nodiscard]] uint64_t PartitionCount() const noexcept {
    return runtime_partitions_->load(std::memory_order_relaxed);
  }
  [[nodiscard]] size_t PeakResidentRows() const noexcept {
    return runtime_peak_resident_rows_->load(std::memory_order_relaxed);
  }

  struct CallSpec {
    size_t output_idx{0};
    const WindowFunctionCallExpression* expr{nullptr};
  };
  struct SpecGroup {
    std::vector<Expression> partition_by;
    std::vector<WindowOrderTerm> order_by;
    std::vector<slot_t> partition_slots;
    std::vector<slot_t> order_slots;
    std::vector<Expression> computed_keys;
    std::vector<CallSpec> calls;
  };

 private:
  Status BuildGroups();
  Status AppendKeyedRow(PipelineContext* ctx, Row&& base_row, uint64_t seq,
                        const SpecGroup& group, ExternalSorter* sorter) const;
  Status TransitionToOoc(PipelineContext* ctx);

  Schema input_schema_;
  Schema output_schema_;
  std::vector<NamedExpression> outputs_;
  bool preserve_input_order_{false};
  bool spill_capable_plan_{false};
  size_t plan_memory_budget_bytes_{0};

  std::optional<QueryMemoryGrant::ActiveConsumerGuard> consumer_guard_;
  std::optional<QueryMemoryGrant::LocalBatchCache> grant_cache_;
  size_t budget_bytes_{0};
  size_t in_mem_budget_{0};
  std::vector<SpecGroup> groups_;

  bool ooc_mode_{false};
  uint64_t next_seq_{0};
  uint64_t input_rows_{0};
  size_t buffered_bytes_{0};
  std::vector<Row> in_mem_rows_;
  std::unique_ptr<ExternalSorter> stage0_sorter_;
  std::unique_ptr<SpillableStore> passthrough_store_;

  std::shared_ptr<std::atomic<bool>> runtime_spilled_{
      std::make_shared<std::atomic<bool>>(false)};
  std::shared_ptr<std::atomic<uint64_t>> runtime_partitions_{
      std::make_shared<std::atomic<uint64_t>>(0)};
  std::shared_ptr<std::atomic<size_t>> runtime_peak_resident_rows_{
      std::make_shared<std::atomic<size_t>>(0)};
};

// --- 6. OOC SetOperations, IncrementalSort & Multi-Index/Scan Leaves (P3b) ---

struct OocSetOpInputStream {
  std::shared_ptr<PipelineSource> source;
  std::vector<std::shared_ptr<PipelineOperator>> operators;
  Schema schema;
};

// Streaming concatenation (`UNION ALL` without ORDER BY).
class OocConcatSource final : public PipelineSource {
 public:
  OocConcatSource(std::vector<OocSetOpInputStream> inputs, Schema output_schema,
                  const std::vector<ValueType>& common_types = {});

  Status Open(PipelineContext* ctx) override;
  StatusOr<bool> NextBatch(PipelineContext* ctx, DataChunk* out,
                           size_t max_rows) override;
  [[nodiscard]] std::string Describe() const override { return "SetOperation"; }
  void Explain(std::ostream& out, int indent) const override;
  void DumpExplain(std::ostream& out, int indent) const override;

 private:
  std::vector<OocSetOpInputStream> inputs_;
  Schema output_schema_;
  std::vector<ValueType> common_types_;
  Status common_type_status_{Status::kSuccess};
  size_t active_idx_{0};
  bool opened_{false};
};

// Streaming K-way ordered merge (`UNION ALL` with pre-sorted children).
class OocMergeAppendSource final : public PipelineSource {
 public:
  OocMergeAppendSource(std::vector<OocSetOpInputStream> inputs,
                       std::vector<OocSortSink::KeySpec> order_keys,
                       Schema output_schema,
                       const std::vector<ValueType>& common_types = {});

  Status Open(PipelineContext* ctx) override;
  StatusOr<bool> NextBatch(PipelineContext* ctx, DataChunk* out,
                           size_t max_rows) override;
  [[nodiscard]] std::string Describe() const override { return "MergeAppend"; }
  void Explain(std::ostream& out, int indent) const override;
  void DumpExplain(std::ostream& out, int indent) const override;

 private:
  struct Cursor {
    OocSetOpInputStream stream;
    DataChunk chunk;
    size_t row_idx{0};
    bool eof{false};
    std::vector<Value> cached_keys;
  };

  [[nodiscard]] int CompareHeads(size_t lhs_idx, size_t rhs_idx) const;

  std::vector<Cursor> cursors_;
  std::vector<OocSortSink::KeySpec> order_keys_;
  std::vector<SortKeyOrder> key_orders_;
  Schema output_schema_;
  std::vector<ValueType> common_types_;
  Status common_type_status_{Status::kSuccess};
};

// Shared coordinator for OOC set operations (`UNION`, `INTERSECT [ALL]`,
// `EXCEPT [ALL]`). Receives chunks from all children via `OocSetOpInputSink`,
// spills into `RecursivePartitioner` whenever exceeding `B_op`, and yields
// `OocSetOpOutputSource` (which uses `ExternalSorter` skew fallback on any
// oversized partition).
class OocSetOpCoordinator {
 public:
  OocSetOpCoordinator(SetOperationKind operation, Schema output_schema,
                      std::vector<Schema> child_schemas);
  OocSetOpCoordinator(const OocSetOpCoordinator&) = delete;
  OocSetOpCoordinator& operator=(const OocSetOpCoordinator&) = delete;
  OocSetOpCoordinator(OocSetOpCoordinator&&) = delete;
  OocSetOpCoordinator& operator=(OocSetOpCoordinator&&) = delete;
  ~OocSetOpCoordinator();

  void RegisterExplainSource(std::shared_ptr<PipelineSource> src);
  Status ConsumeFromSource(PipelineContext* ctx, size_t source_idx,
                           const DataChunk& input);
  StatusOr<std::shared_ptr<PipelineSource>> Finalize(PipelineContext* ctx);

  void DumpExplain(std::ostream& out, int indent) const;

 private:
  Status TransitionToSpill(PipelineContext* ctx);
  Status CoerceRow(Row* row, size_t expected_cols) const;

  SetOperationKind operation_;
  Schema output_schema_;
  size_t num_sources_{0};
  std::vector<ValueType> common_types_;
  Status common_type_status_{Status::kSuccess};
  std::optional<size_t> width_;

  bool initialized_{false};
  std::optional<QueryMemoryGrant::ActiveConsumerGuard> consumer_guard_;
  std::optional<QueryMemoryGrant::LocalBatchCache> grant_cache_;
  size_t budget_bytes_{0};
  size_t in_mem_budget_{0};
  size_t mem_bytes_{0};
  bool spilled_{false};
  bool type_widened_after_spill_{false};
  std::vector<std::vector<Row>> in_mem_rows_;
  std::unique_ptr<RecursivePartitioner> partitioner_;
  std::vector<std::shared_ptr<PipelineSource>> explain_sources_;
};

class OocSetOpInputSink final : public PipelineSink {
 public:
  OocSetOpInputSink(std::shared_ptr<OocSetOpCoordinator> coord,
                    size_t source_idx, bool is_final,
                    std::shared_ptr<PipelineSource> explain_src);

  Status Init(PipelineContext* ctx, size_t worker_count) override;
  Status Consume(PipelineContext* ctx, size_t worker_id,
                 const DataChunk& input) override;
  StatusOr<std::shared_ptr<PipelineSource>> Finalize(
      PipelineContext* ctx) override;
  [[nodiscard]] std::string Describe() const override { return "SetOperation"; }
  void DumpExplain(std::ostream& out, int indent) const override;

 private:
  std::shared_ptr<OocSetOpCoordinator> coord_;
  size_t source_idx_{0};
  bool is_final_{false};
  std::shared_ptr<PipelineSource> explain_src_;
};

// Streaming IncrementalSort (`IncrementalSortPlan`): groups runs sharing equal
// `prefix_keys`, sorts each run by `suffix_keys` in bounded `ExternalSorter`
// (`<= B_op`) and emits it before reading the next. There is no LIMIT
// early-stop here: a retained LimitOp downstream applies the cutoff.
class IncrementalSortSource final : public PipelineSource {
 public:
  IncrementalSortSource(std::shared_ptr<PipelineSource> upstream_source,
                        std::vector<std::shared_ptr<PipelineOperator>> ops,
                        Schema schema,
                        std::vector<OocSortSink::KeySpec> prefix_keys,
                        std::vector<OocSortSink::KeySpec> suffix_keys);

  Status Open(PipelineContext* ctx) override;
  StatusOr<bool> NextBatch(PipelineContext* ctx, DataChunk* out,
                           size_t max_rows) override;
  [[nodiscard]] std::string Describe() const override {
    return "IncrementalSort";
  }
  void Explain(std::ostream& out, int indent) const override;
  void DumpExplain(std::ostream& out, int indent) const override;

 private:
  [[nodiscard]] StatusOr<Row> ExtractPrefix(const Row& row);
  Status FillNextGroup(PipelineContext* ctx);

  std::shared_ptr<PipelineSource> upstream_source_;
  std::vector<std::shared_ptr<PipelineOperator>> upstream_ops_;
  Schema schema_;
  std::vector<OocSortSink::KeySpec> prefix_keys_;
  std::vector<OocSortSink::KeySpec> suffix_keys_;
  std::vector<SortKeyOrder> prefix_orders_, suffix_orders_;

  std::optional<QueryMemoryGrant::ActiveConsumerGuard> consumer_guard_;
  size_t budget_bytes_{0};
  DataChunk in_chunk_;
  size_t in_idx_{0};
  bool upstream_eof_{false};
  bool has_lookahead_{false};
  Row lookahead_row_;
  Row lookahead_prefix_;

  std::unique_ptr<ExternalSorter> group_sorter_;
  std::optional<Row> singleton_row_;
  uint64_t singleton_runs_{0};
  TransactionContext* txn_{nullptr};
};

// Passthrough explain wrapper for `IndexSkipScanPlan`.
class IndexSkipScanSource final : public PipelineSource {
 public:
  IndexSkipScanSource(std::shared_ptr<PipelineSource> inner_source,
                      std::vector<std::shared_ptr<PipelineOperator>> ops,
                      Schema schema);

  Status Open(PipelineContext* ctx) override;
  StatusOr<bool> NextBatch(PipelineContext* ctx, DataChunk* out,
                           size_t max_rows) override;
  [[nodiscard]] std::string Describe() const override {
    return "IndexSkipScan";
  }
  void Explain(std::ostream& out, int indent) const override;
  void DumpExplain(std::ostream& out, int indent) const override;

 private:
  std::shared_ptr<PipelineSource> inner_source_;
  std::vector<std::shared_ptr<PipelineOperator>> ops_;
  Schema schema_;
};

// Streaming 1-row MinMax index leaf source (`MinMaxIndexPlan`).
class MinMaxIndexSource final : public PipelineSource {
 public:
  MinMaxIndexSource(std::shared_ptr<PipelineSource> child_source,
                    std::vector<std::shared_ptr<PipelineOperator>> ops,
                    Schema out_schema, size_t value_slot);

  Status Open(PipelineContext* ctx) override;
  StatusOr<bool> NextBatch(PipelineContext* ctx, DataChunk* out,
                           size_t max_rows) override;
  [[nodiscard]] std::string Describe() const override { return "MinMaxIndex"; }
  void Explain(std::ostream& out, int indent) const override;
  void DumpExplain(std::ostream& out, int indent) const override;

 private:
  std::shared_ptr<PipelineSource> child_source_;
  std::vector<std::shared_ptr<PipelineOperator>> ops_;
  Schema out_schema_;
  size_t value_slot_{0};
  bool emitted_{false};
};

struct OocIndexRange {
  const Index* index{nullptr};
  std::vector<Value> begin_key;
  std::vector<Value> end_key;
};

using OocScanFallbackFactory =
    std::function<std::shared_ptr<PipelineSource>(PipelineContext*)>;

// OOC Bitmap Index Scan (`BitmapScanPlan`): sorts & merges/intersects per-range
// `RowPosition` streams via `ExternalSorter` (strictly `<= B_op` RAM), then
// fetches heap rows + applies residual predicate.
class OocBitmapScanSource final : public PipelineSource {
 public:
  OocBitmapScanSource(const Table* table, std::vector<OocIndexRange> ranges,
                      bool is_and, Expression where, Schema schema,
                      OocScanFallbackFactory fallback_factory = {});

  Status Open(PipelineContext* ctx) override;
  StatusOr<bool> NextBatch(PipelineContext* ctx, DataChunk* out,
                           size_t max_rows) override;
  [[nodiscard]] std::string Describe() const override {
    return is_and_ ? "BitmapAnd" : "BitmapOr";
  }
  void DumpExplain(std::ostream& out, int indent) const override;

 private:
  struct SortedPosCursor {
    std::unique_ptr<ExternalSorter> sorter;
    RowPosition cur;
    bool valid{false};
  };

  static Status AdvanceCursor(SortedPosCursor* c);
  StatusOr<bool> NextPosition(RowPosition* out);

  const Table* table_{nullptr};
  std::vector<OocIndexRange> ranges_;
  bool is_and_{false};
  Expression where_;
  std::vector<Expression> where_conjuncts_;
  Schema schema_;
  OocScanFallbackFactory fallback_factory_;

  bool fallback_full_scan_{false};
  std::shared_ptr<PipelineSource> fallback_source_;
  std::vector<SortedPosCursor> cursors_;
  std::optional<RowPosition> last_emitted_pos_;
  TransactionContext* txn_{nullptr};
  size_t scanned_positions_{0};
};

// OOC Multi-Index Covering Scan (`MultiIndexCoveringScanPlan`): sorts each
// participant index's `(RowPosition, heap_needed, covered_cols...)` via
// `ExternalSorter` and streams K-way intersection cleanly within `B_op`.
class OocMultiIndexCoveringSource final : public PipelineSource {
 public:
  OocMultiIndexCoveringSource(const Table* table,
                              std::vector<OocIndexRange> participants,
                              Expression where,
                              std::vector<slot_t> covered_slots,
                              Schema output_schema, bool force_fallback = false,
                              OocScanFallbackFactory fallback_factory = {});

  Status Open(PipelineContext* ctx) override;
  StatusOr<bool> NextBatch(PipelineContext* ctx, DataChunk* out,
                           size_t max_rows) override;
  [[nodiscard]] std::string Describe() const override {
    return "MultiIndexCoveringScan";
  }
  void DumpExplain(std::ostream& out, int indent) const override;

 private:
  struct ParticipantState {
    OocIndexRange range;
    std::vector<std::pair<size_t, size_t>> fills;
    std::unique_ptr<ExternalSorter> sorter;
    RowPosition cur_pos;
    Row cur_row;
    bool valid{false};
  };

  static Status AdvanceParticipant(ParticipantState* p);
  Status BuildFallbackScan(PipelineContext* ctx);

  const Table* table_{nullptr};
  std::vector<ParticipantState> participants_;
  Expression where_;
  std::vector<Expression> where_conjuncts_;
  std::vector<slot_t> covered_slots_;
  Schema output_schema_;
  bool force_fallback_{false};
  OocScanFallbackFactory fallback_factory_;

  std::shared_ptr<PipelineSource> fallback_source_;
  TransactionContext* txn_{nullptr};
};

// --- 7. OOC MergeJoin, BandJoin, CrossJoin/BNL, RecursiveCte, Apply (P3b) ---

// Streaming two-cursor MergeJoin (`MergeJoinPlan`). Buffers at most one
// equal-key right run inside `SpillableStore` (`<= B_op`) and tracks
// right-outer matches via a compact packed bitset.
class OocMergeJoinSource final : public PipelineSource {
 public:
  OocMergeJoinSource(OocSetOpInputStream left, OocSetOpInputStream right,
                     std::vector<slot_t> left_cols,
                     std::vector<slot_t> right_cols, JoinKind kind,
                     size_t left_width, size_t right_width, Expression residual,
                     Schema residual_schema, Schema output_schema);

  Status Open(PipelineContext* ctx) override;
  StatusOr<bool> NextBatch(PipelineContext* ctx, DataChunk* out,
                           size_t max_rows) override;
  [[nodiscard]] std::string Describe() const override { return "MergeJoin"; }
  void DumpExplain(std::ostream& out, int indent) const override;

 private:
  struct RowCursor {
    OocSetOpInputStream stream;
    DataChunk chunk;
    size_t idx{0};
    bool eof{false};
    bool has_cur{false};
    Row cur;
    RowPosition cur_pos;
  };

  static Status AdvanceCursor(PipelineContext* ctx, RowCursor* c);
  [[nodiscard]] int CompareKeys(const Row& l, const Row& r) const;
  [[nodiscard]] int CompareLeftKeys(const Row& a, const Row& b) const;
  [[nodiscard]] static bool KeyCannotMatch(const Row& row,
                                           const std::vector<slot_t>& cols);
  [[nodiscard]] StatusOr<bool> PairPasses(const Row& l, const Row& r) const;
  Status EmitNextMatchChunk(PipelineContext* ctx, DataChunk* out,
                            size_t max_rows);

  RowCursor left_;
  RowCursor right_;
  std::vector<slot_t> left_cols_;
  std::vector<slot_t> right_cols_;
  JoinKind kind_{JoinKind::kInner};
  size_t left_width_{0};
  size_t right_width_{0};
  Expression residual_;
  std::vector<Expression> residual_conjuncts_;
  Schema residual_schema_;
  Schema output_schema_;
  size_t budget_bytes_{0};

  // Active equal-key run cross-product state:
  bool in_run_{false};
  std::unique_ptr<SpillableStore> right_run_store_;
  std::vector<uint64_t> right_matched_bits_;
  Row active_left_row_;
  RowPosition active_left_pos_;
  bool active_left_matched_{false};
  bool scan_right_active_{false};
  DataChunk active_right_chunk_;
  size_t active_right_idx_{0};
  uint64_t active_right_global_idx_{0};
  bool draining_unmatched_right_{false};
  DataChunk unmatched_right_chunk_;
  size_t unmatched_right_idx_{0};
  uint64_t unmatched_right_global_idx_{0};
};

// Streaming/spilling BandJoin (`BandJoinPlan`): drains right into
// `SpillableStore`, validates ordering, and evaluates left blocks against
// sorted right window using a working budget floored at 16 KiB
// (`max(16 KiB, budget_bytes)`). Left blocks target at most
// `max(16 KiB, budget_bytes_ / 2)`; for tiny grants this floor can exceed the
// operator's requested budget.
class OocBandJoinSource final : public PipelineSource {
 public:
  OocBandJoinSource(OocSetOpInputStream left, OocSetOpInputStream right,
                    slot_t left_col, slot_t right_col,
                    BinaryOperation comparison, Expression predicate,
                    Schema residual_schema, JoinKind kind, size_t left_width,
                    size_t right_width, Schema output_schema);

  Status Open(PipelineContext* ctx) override;
  StatusOr<bool> NextBatch(PipelineContext* ctx, DataChunk* out,
                           size_t max_rows) override;
  [[nodiscard]] std::string Describe() const override { return "BandJoin"; }
  void DumpExplain(std::ostream& out, int indent) const override;

 private:
  [[nodiscard]] StatusOr<std::optional<int64_t>> ReadKey(const Row& r,
                                                         slot_t col) const;
  [[nodiscard]] StatusOr<bool> PairPasses(const Row& l, const Row& r) const;
  Status PullNextLeftBlock(PipelineContext* ctx);

  OocSetOpInputStream left_;
  OocSetOpInputStream right_;
  slot_t left_col_{0};
  slot_t right_col_{0};
  BinaryOperation comparison_{BinaryOperation::kLessThan};
  Expression predicate_;
  std::vector<Expression> predicate_conjuncts_;
  Schema residual_schema_;
  JoinKind kind_{JoinKind::kInner};
  size_t left_width_{0};
  size_t right_width_{0};
  Schema output_schema_;
  size_t budget_bytes_{0};

  std::unique_ptr<SpillableStore> right_store_;
  std::optional<ValueType> key_type_;
  bool have_prev_left_key_{false};
  int64_t prev_left_key_{0};
  std::vector<uint64_t> right_matched_bits_;

  // Left block + right pass streaming state:
  struct LeftEntry {
    Row row;
    RowPosition pos;
    std::optional<int64_t> key;
    bool matched{false};
    uint64_t matches{0};
  };
  bool left_eof_{false};
  DataChunk in_left_chunk_;
  size_t in_left_idx_{0};
  std::vector<LeftEntry> left_block_;
  size_t cur_left_idx_{0};
  // Sorted (key, left_block_ index) pairs for non-NULL left keys. A right row
  // binary-searches this to visit only left entries inside its band instead
  // of the whole block (O(R log L + matches) rather than O(R x L)).
  std::vector<std::pair<int64_t, size_t>> left_key_index_;
  size_t cand_lo_{0};
  size_t cand_hi_{0};
  size_t cur_cand_idx_{0};
  bool right_row_current_{false};
  bool right_pass_active_{false};
  DataChunk cur_right_chunk_;
  size_t cur_right_idx_{0};
  uint64_t cur_right_global_idx_{0};
  bool flushing_left_outer_{false};
  size_t left_outer_idx_{0};
  bool flushing_right_outer_{false};
  bool right_outer_done_{false};
  uint64_t right_outer_global_idx_{0};
  // The BandJoin contract emits joined rows left-major (left input order,
  // right-key order inside one left row) while the band pass visits pairs
  // right-major. Kinds that emit combined rows therefore route each matched
  // pair through a bounded sorter tagged (left block index, right global
  // index); the sorted replay is the contract order.
  std::unique_ptr<ExternalSorter> pair_sorter_;
  bool emitting_pairs_{false};
  Row tagged_row_;
};

// Shared build context for non-equi ProductPlan (`CrossJoin` / BNL).
struct OocCrossJoinContext {
  JoinKind kind{JoinKind::kInner};
  size_t left_width{0};
  size_t right_width{0};
  Schema left_schema;
  Schema right_schema;
  Schema combined_schema;
  Schema output_schema;
  Expression residual;
  std::vector<Expression> residual_conjuncts;
  bool prefer_bnl{false};
  size_t budget_bytes{0};
  size_t block_size_hint{1024};

  std::unique_ptr<SpillableStore> right_store;
  std::optional<QueryMemoryGrant::ActiveConsumerGuard> build_guard;
  std::shared_ptr<PipelineSource> right_source_explain;
  std::vector<std::shared_ptr<PipelineOperator>> right_ops_explain;
};

class OocCrossJoinBuildSink final : public PipelineSink {
 public:
  explicit OocCrossJoinBuildSink(std::shared_ptr<OocCrossJoinContext> ctx);

  Status Init(PipelineContext* pctx, size_t worker_count) override;
  Status Consume(PipelineContext* pctx, size_t worker_id,
                 const DataChunk& input) override;
  StatusOr<std::shared_ptr<PipelineSource>> Finalize(
      PipelineContext* pctx) override;
  [[nodiscard]] std::string Describe() const override;
  void DumpExplain(std::ostream& out, int indent) const override;

 private:
  std::shared_ptr<OocCrossJoinContext> cross_ctx_;
};

class
    OocCrossJoinProbeSource final  // NOLINT(cppcoreguidelines-special-member-functions):
                                   // scope-owned cleanup.
    : public PipelineSource {
 public:
  OocCrossJoinProbeSource(std::shared_ptr<OocCrossJoinContext> cross_ctx,
                          OocSetOpInputStream left_stream);
  ~OocCrossJoinProbeSource() override;

  Status Open(PipelineContext* ctx) override;
  StatusOr<bool> NextBatch(PipelineContext* ctx, DataChunk* out,
                           size_t max_rows) override;
  [[nodiscard]] std::string Describe() const override;
  void DumpExplain(std::ostream& out, int indent) const override;

 private:
  struct LeftEntry {
    Row row;
    RowPosition pos;
    bool matched{false};
    uint64_t matches{0};
  };

  struct DisjunctiveJoinIndex;

  [[nodiscard]] StatusOr<bool> PairPasses(const Row& left_row,
                                          const Row& right_row) const;
  Status FillNextLeftBlock(PipelineContext* ctx);
  [[nodiscard]] StatusOr<bool> NextDisjunctiveBatch(PipelineContext* ctx,
                                                    DataChunk* out,
                                                    size_t max_rows);

  std::shared_ptr<OocCrossJoinContext> cross_ctx_;
  OocSetOpInputStream left_;
  std::unique_ptr<DisjunctiveJoinIndex> disj_index_;
  bool left_eof_{false};
  DataChunk in_left_chunk_;
  size_t in_left_idx_{0};
  std::vector<LeftEntry> left_block_;
  std::vector<uint64_t> right_matched_bits_;

  bool scanning_right_{false};
  DataChunk cur_right_chunk_;
  size_t cur_right_row_idx_{0};
  uint64_t cur_right_global_idx_{0};
  size_t left_scan_idx_{0};
  // Left-major output: restart the right-side cursor for every left row.
  bool right_pass_at_start_{false};

  bool flushing_left_unmatched_{false};
  size_t left_flush_idx_{0};
  bool flushing_right_unmatched_{false};
  bool right_outer_done_{false};
  uint64_t right_flush_global_idx_{0};
};

// OOC Recursive CTE evaluator (`RecursiveCtePlan`): replaces unbounded memory
// accumulation with `SpillableStore` result/delta working sets + OOC sorted
// anti-join deduplication when `UNION` distinct exceeds `B_op`.
class OocRecursiveCteSource final : public PipelineSource {
 public:
  OocRecursiveCteSource(std::string cte_name,
                        std::shared_ptr<const SelectStatement> body,
                        std::optional<RecursiveDepthSpec> depth_spec,
                        Schema output_schema);

  Status Open(PipelineContext* ctx) override;
  StatusOr<bool> NextBatch(PipelineContext* ctx, DataChunk* out,
                           size_t max_rows) override;
  [[nodiscard]] std::string Describe() const override {
    return "RecursiveUnion";
  }
  void DumpExplain(std::ostream& out, int indent) const override;

 private:
  std::string cte_name_;
  std::shared_ptr<const SelectStatement> body_;
  std::optional<RecursiveDepthSpec> depth_spec_;
  Schema output_schema_;

  std::unique_ptr<SpillableStore> result_store_;
  bool initialized_{false};
};

// OOC Correlated Apply / Lateral Join (`ApplyPlan`): streams outer batches,
// caches correlated inner results up to max(kMinJoinMemBudget, B_op / 2)
// LRU bytes, or spills
// uncorrelated/large inner subplans cleanly via `SpillableStore`.
class OocApplySource final : public PipelineSource {
 public:
  OocApplySource(OocSetOpInputStream outer,
                 std::optional<OocSetOpInputStream> inner_plan_stream,
                 Schema outer_schema,
                 std::shared_ptr<const SelectStatement> inner_query,
                 std::string inner_alias, Expression predicate, JoinKind kind,
                 Schema output_schema);

  Status Open(PipelineContext* ctx) override;
  StatusOr<bool> NextBatch(PipelineContext* ctx, DataChunk* out,
                           size_t max_rows) override;
  [[nodiscard]] std::string Describe() const override { return "Apply"; }
  void DumpExplain(std::ostream& out, int indent) const override;

 private:
  struct LruEntry {
    Row key;
    // Shared immutable rows: the cache and the active consumer both hold a
    // reference instead of deep-copying the whole inner result on every hit.
    std::shared_ptr<const std::vector<Row>> rows;
    size_t bytes{0};
    // Cheap pre-filter for the bounded LRU probe: only rows hashing to the
    // same value need the (collation-aware) exact equality check.
    size_t key_hash{0};
  };

  OocSetOpInputStream outer_;
  std::optional<OocSetOpInputStream> inner_plan_stream_;
  Schema outer_schema_;
  Schema inner_schema_;
  Schema combined_schema_;
  std::shared_ptr<const SelectStatement> inner_query_;
  std::string inner_alias_;
  Expression predicate_;
  JoinKind kind_{JoinKind::kInner};
  Schema output_schema_;
  TransactionContext* txn_{nullptr};
  size_t budget_bytes_{0};
  size_t cache_budget_bytes_{0};

  // Uncorrelated static inner child spilled/cached once via SpillableStore:
  bool static_inner_ready_{false};
  std::unique_ptr<SpillableStore> static_inner_store_;

  // Bounded LRU cache for correlated inner queries
  // (max(kMinJoinMemBudget, B_op / 2)):
  std::vector<LruEntry> lru_cache_;
  size_t lru_bytes_{0};

  // Active outer row streaming state:
  DataChunk outer_chunk_;
  size_t outer_idx_{0};
  bool outer_eof_{false};
  bool has_outer_row_{false};
  Row cur_outer_row_;
  RowPosition cur_outer_pos_;
  bool matched_any_{false};
  // Mutable build buffer for a freshly evaluated inner result; once complete
  // it is moved into an immutable shared view used by the cache and reader.
  std::vector<Row> active_inner_rows_;
  std::shared_ptr<const std::vector<Row>> active_inner_view_;
  std::unique_ptr<SpillableStore> active_inner_spill_;
  bool reading_inner_spill_{false};
  DataChunk active_inner_chunk_;
  size_t active_inner_chunk_idx_{0};
  size_t active_inner_idx_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_DETAIL_PIPELINE_OOC_SINKS_HPP
