/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_PIPELINE_LOWERING_HPP
#define TINYLAMB_EXECUTOR_PIPELINE_LOWERING_HPP

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/status_or.hpp"
#include "executor/detail/pipeline/ooc_sinks.hpp"
#include "executor/detail/pipeline/pipeline_ir.hpp"
#include "executor/executor_base.hpp"
#include "expression/named_expression.hpp"
#include "plan/plan.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class TransactionContext;

struct LoweredStream {
  std::shared_ptr<PipelineSource> source;
  std::vector<std::shared_ptr<PipelineOperator>> operators;
  Schema schema;

  // Dynamic runtime-filter tracing back to the leaf scan's physical columns.
  struct ScanOrigin {
    std::shared_ptr<PipelineSource> leaf_source;
    Schema leaf_schema;
    std::vector<ColumnName> output_to_leaf_col;
  };
  std::optional<ScanOrigin> scan_origin;
};

using CustomPlanLowerer =
    std::function<StatusOr<bool>(const Plan& plan, TransactionContext& ctx,
                                 PipelineGraph* graph, LoweredStream* out)>;

struct PipelineLoweringOptions {
  bool apply_post_distinct{false};
  // Positional columns forming the post-distinct duplicate key. Empty means
  // the whole row is the key; a non-empty prefix (e.g. the visible select
  // list when hidden ORDER BY carriers trail it) dedupes on those columns
  // only while the full row still flows to the sort/limit stages.
  std::vector<slot_t> post_distinct_key_cols;
  std::vector<OocSortSink::KeySpec> post_sort_keys;
  std::optional<size_t> post_limit;
  size_t post_offset{0};
  std::vector<NamedExpression> post_projection;
  Schema post_projection_schema;
  bool keep_plan_explain_text{false};
  size_t requested_dop{1};
  CustomPlanLowerer custom_plan_lowerer{};
};

// Recursively lowers a physical Plan into a topological DAG of PipelineStages
// feeding a pullable root `PipelineExecutor`.
//
// - O(1)-streaming leaves (`FullScan`, `IndexScan`, `IndexOnlyScan`, `Values`,
//   `Empty`, `DummyScan`, `SkipScanDistinct`) wrap into
//   `StreamingExecutorSource` (zero buffering, zero legacy telemetry).
// - Pipeline breakers (`Sort`, `TopN`, `Distinct`,
//   `Aggregation`/`HashAggregate`/`SortAggregate`/`StreamAggregate`, `Window`,
//   `SetOperation`, `IncrementalSort`, `ProductPlan`, `MergeJoin`, `BandJoin`,
//   `RecursiveCte`, `Apply`, `LateMaterializationJoin`, `ZigZagJoin`) lower to
//   OOC spillable/streaming sinks and sources under
//   `executor/detail/pipeline/`, yielding zero legacy fallbacks on supported
//   SQL.
class PipelineLowering {
 public:
  [[nodiscard]] static StatusOr<PipelineGraph> Lower(
      const Plan& plan, TransactionContext& ctx,
      const PipelineLoweringOptions& options = {});

  [[nodiscard]] static StatusOr<Executor> LowerToExecutor(
      const Plan& plan, TransactionContext& ctx,
      const PipelineLoweringOptions& options = {});

  [[nodiscard]] static Status LowerNode(const Plan& plan,
                                        TransactionContext& ctx,
                                        const CustomPlanLowerer& custom_hook,
                                        PipelineGraph* graph,
                                        LoweredStream* out);

  static Status BreakWithSink(PipelineGraph* graph, LoweredStream* stream,
                              std::shared_ptr<PipelineSink> sink,
                              const std::string& stage_name,
                              const Schema& out_schema,
                              bool preserve_scan_origin = false);
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_PIPELINE_LOWERING_HPP
