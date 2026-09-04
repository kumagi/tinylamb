/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_RELATIONAL_EXECUTOR_HPP
#define TINYLAMB_RELATIONAL_EXECUTOR_HPP

#include <memory>
#include <vector>

#include "executor/executor_base.hpp"
#include "type/row.hpp"

namespace tinylamb {

class SelectStatement;
class TransactionContext;
class PlanBase;

// Bridge for plan/GroupByPlan: materializes `core_plan` (the Cascades-
// optimized FROM + WHERE core) and runs the statement's grouping finish
// pipeline (Project / HAVING / DISTINCT / ORDER BY / LIMIT) over it.
Executor EmitGroupedFinishExecutor(
    TransactionContext& context, std::shared_ptr<PlanBase> core_plan,
    std::shared_ptr<const SelectStatement> statement);

class RelationalExecutor : public ExecutorBase {
 public:
  RelationalExecutor(TransactionContext& context,
                     std::shared_ptr<const SelectStatement> statement);
  bool Next(Row* destination, RowPosition* position) override;
  void Dump(std::ostream& output, int indent) const override;
  void Explain(std::ostream& output, int indent) const override;

 private:
  void Initialize();

  // Not owned. The executor must not outlive the TransactionContext it was
  // created with (same lifetime contract as full_scan.hpp's `table_`).
  TransactionContext* context_;
  std::shared_ptr<const SelectStatement> statement_;
  std::vector<Row> rows_;
  size_t offset_{0};
  bool initialized_{false};
  size_t hash_joins_{0};
  size_t hybrid_hash_joins_{0};
  size_t in_memory_hash_joins_{0};
  size_t nested_loop_joins_{0};
  size_t join_comparisons_{0};
  size_t peak_intermediate_rows_{0};
  size_t relation_spills_{0};
  size_t correlated_index_builds_{0};
  size_t correlated_index_probes_{0};
  size_t correlated_result_cache_hits_{0};
  size_t correlated_distinct_keys_{0};
  size_t uncorrelated_cache_hits_{0};
  size_t uncorrelated_hash_builds_{0};
  size_t uncorrelated_hash_probes_{0};
  double scan_ms_{0};
  double filter_ms_{0};
  double join_ms_{0};
  double project_ms_{0};
  double sort_ms_{0};
  size_t base_scan_cache_hits_{0};
  size_t aggregate_input_rows_{0};
  size_t aggregate_groups_{0};
  size_t aggregate_updates_{0};
  size_t scan_rows_{0};
  size_t scan_output_rows_{0};
  size_t scan_values_decoded_{0};
  size_t scan_values_available_{0};
  size_t exists_short_circuit_queries_{0};
  size_t key_filter_scans_{0};
  size_t key_filter_keys_{0};
  size_t key_filter_rejected_{0};
  size_t key_filter_null_rejected_{0};
  bool empty_build_short_circuit_{false};
  bool null_aware_anti_build_contains_null_{false};
};

}  // namespace tinylamb

#endif  // TINYLAMB_RELATIONAL_EXECUTOR_HPP
