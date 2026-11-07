/**
 * Copyright 2026 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef TINYLAMB_EXECUTOR_WINDOW_HPP
#define TINYLAMB_EXECUTOR_WINDOW_HPP

#include <cstddef>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "executor/data_chunk.hpp"
#include "executor/executor_base.hpp"
#include "executor/spill_file.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class TransactionContext;

// Evaluates window-function calls computed by the shared planning-time
// extraction (one `$winN` output column per call) over a row-preserving
// child. Each call runs through the canonical window evaluator
// (executor/detail/window_eval), so engine and planner agree on ranking,
// framing, and NULL placement by construction.
//
// Window functions observe whole partitions, so the operator cannot stream a
// partition. It has two shapes:
//
//  - buffered (the default): the whole input is held in memory and emitted in
//    input order. The operator preserves the child's order.
//  - partitioned spill: once `memory_budget_bytes` is non-zero (and every call
//    partitions by the same expression list) the drained input is written to
//    `spill_partition_count` files by PARTITION BY hash, so every partition
//    stays inside one file, and one file is resident at a time. Rows therefore
//    leave in spill-file order, not input order -- the plan drops the ordering
//    claim for that shape (WindowPlan::IsOrderedBy) instead of lying about it.
//    A single partition larger than the budget still has to be resident: the
//    bound is one spill file, not one partition.
class WindowExecutor : public ExecutorBase {
 public:
  // Physical form of the plan's WindowMemoryPolicy. 0 bytes keeps the
  // historical buffered shape.
  struct Options {
    size_t memory_budget_bytes{0};
    size_t spill_partition_count{8};
  };

  WindowExecutor(const WindowExecutor&) = delete;
  WindowExecutor& operator=(const WindowExecutor&) = delete;
  WindowExecutor(WindowExecutor&&) = delete;
  WindowExecutor& operator=(WindowExecutor&&) = delete;
  WindowExecutor(TransactionContext& context, Executor child, Schema schema,
                 std::vector<NamedExpression> outputs);
  WindowExecutor(TransactionContext& context, Executor child, Schema schema,
                 std::vector<NamedExpression> outputs, Options options);
  ~WindowExecutor() override = default;

  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& o, int indent) const override;
  void Explain(std::ostream& o, int indent) const override;

  // Spill accounting, for Dump and for tests that pin the memory bound: how
  // many files the input was partitioned across, and the largest number of
  // rows resident at once (0 for the buffered shape until it is drained).
  [[nodiscard]] size_t SpillFileCount() const { return spill_file_count_; }
  [[nodiscard]] size_t PeakResidentRows() const { return peak_resident_rows_; }
  [[nodiscard]] bool SpillEnabled() const { return spill_enabled_; }

 private:
  // Fills rows_ with the next computed chunk and rewinds read_offset_.
  bool NextChunk();
  bool EnsureComputed();
  bool ComputeColumns();
  bool DrainIntoSpills(const std::vector<Expression>& partition_by);
  // The PARTITION BY list every call shares, or nullptr when the calls
  // disagree (then the input cannot be split without cutting a partition).
  const std::vector<Expression>* SharedPartitionKey() const;
  [[nodiscard]] Schema InputSchema() const;

  TransactionContext& context_;
  Executor child_;
  Schema schema_;
  std::vector<NamedExpression> outputs_;
  std::vector<Row> rows_;
  size_t read_offset_{0};
  bool computed_{false};
  bool buffered_done_{false};
  // Set once nothing more can be produced: a false return has to stay false,
  // because a materializing parent is allowed to pull again.
  bool exhausted_{false};
  // Number of outputs already appended to rows_. A mid-loop failure leaves
  // these in place while computed_ stays false; the next EnsureComputed
  // resumes from here instead of appending duplicate columns.
  size_t appended_columns_{0};
  Options options_;
  bool spill_enabled_{false};
  bool drained_{false};
  std::vector<SpillFile> spills_;
  size_t next_spill_{0};
  size_t spill_file_count_{0};
  size_t peak_resident_rows_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_WINDOW_HPP
