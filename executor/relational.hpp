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

class RelationalExecutor : public ExecutorBase {
 public:
  RelationalExecutor(TransactionContext& context,
                     std::shared_ptr<const SelectStatement> statement);
  bool Next(Row* destination, RowPosition* position) override;
  void Dump(std::ostream& output, int indent) const override;
  void Explain(std::ostream& output, int indent) const override;

 private:
  void Initialize();

  TransactionContext* context_;
  std::shared_ptr<const SelectStatement> statement_;
  std::vector<Row> rows_;
  size_t offset_{0};
  bool initialized_{false};
  size_t hash_joins_{0};
  size_t nested_loop_joins_{0};
  size_t join_comparisons_{0};
  size_t peak_intermediate_rows_{0};
  size_t correlated_index_builds_{0};
  size_t correlated_index_probes_{0};
  size_t correlated_result_cache_hits_{0};
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
};

}  // namespace tinylamb

#endif  // TINYLAMB_RELATIONAL_EXECUTOR_HPP
