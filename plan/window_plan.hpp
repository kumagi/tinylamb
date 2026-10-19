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

#ifndef TINYLAMB_WINDOW_PLAN_HPP
#define TINYLAMB_WINDOW_PLAN_HPP

#include <algorithm>
#include <cstddef>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "expression/named_expression.hpp"
#include "plan/plan.hpp"
#include "table/table_statistics.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class ExecutorBase;

// Working set a window may hold before it hands rows to its spill path.
// Window evaluation needs whole partitions, so the unit of spilling is the
// PARTITION BY group: the input is hash-partitioned into spill files and one
// file is resident at a time.
inline constexpr size_t kWindowDefaultMemoryBudgetBytes =
    64UL * 1024UL * 1024UL;
// Row width the policy uses to turn a budget into a row count (the same
// estimate the sort shape and the cascades MemoryBudget use).
inline constexpr size_t kWindowEstimatedRowBytes = 64;
// Spill files the partitioned shape writes.
inline constexpr size_t kWindowDefaultSpillPartitions = 8;

// Working-memory contract the `window` implementation rule attaches to the
// node. It is logical information: it says how much the shape may hold and
// therefore whether it may reorder rows (a spilling shape emits one spill file
// at a time). `memory_budget_bytes == 0` means "buffered only": the whole
// input stays in memory and the child's ordering survives.
struct WindowMemoryPolicy {
  size_t memory_budget_bytes{0};
  size_t spill_partition_count{kWindowDefaultSpillPartitions};
};

// Rows of the estimated width that fit in `memory`'s budget. An unbounded
// policy never reaches the capacity, so it always keeps the input in memory.
[[nodiscard]] inline size_t WindowRowCapacity(
    const WindowMemoryPolicy& memory = WindowMemoryPolicy{}) {
  if (memory.memory_budget_bytes == 0) {
    return std::numeric_limits<size_t>::max();
  }
  return std::max<size_t>(
      1, memory.memory_budget_bytes / kWindowEstimatedRowBytes);
}

// The policy a row estimate gets: buffered-only while the input fits the
// default budget, the spilling shape once it does not. A NaN (degenerate)
// estimate keeps the buffered shape rather than winning the spill-heavy one by
// accident; an infinite estimate means "too big to fit" and spills.
[[nodiscard]] inline WindowMemoryPolicy WindowPolicyForRows(double rows) {
  WindowMemoryPolicy policy;
  policy.memory_budget_bytes = kWindowDefaultMemoryBudgetBytes;
  if (!(rows > static_cast<double>(WindowRowCapacity(policy)))) {
    policy.memory_budget_bytes = 0;
  }
  return policy;
}

// Logical window evaluation info: one kWindow node's worth of window calls
// (name = hidden `$winN` output column) over a row-preserving child. The
// executor appends one computed column per call.
//
// A buffered shape preserves input order and therefore keeps the child's
// ordering claim. A spill-capable shape (finite budget, and every call sharing
// one PARTITION BY list) emits one spill file at a time, so it claims no order:
// the claim has to be decided at plan time, before the runtime row count is
// known, and an underestimate must never leave a false claim behind.
class WindowPlan final : public PlanBase {
 public:
  WindowPlan(Plan child, std::vector<NamedExpression> window_outputs)
      : WindowPlan(std::move(child), std::move(window_outputs),
                   WindowMemoryPolicy{}) {}
  WindowPlan(Plan child, std::vector<NamedExpression> window_outputs,
             WindowMemoryPolicy memory);

  WindowPlan(const WindowPlan&) = delete;
  WindowPlan(WindowPlan&&) = delete;
  WindowPlan& operator=(const WindowPlan&) = delete;
  WindowPlan& operator=(WindowPlan&&) = delete;
  ~WindowPlan() override = default;

  Executor EmitExecutor(TransactionContext& ctx) const override;
  [[nodiscard]] bool ContainsParameterSlots() const override {
    return child_ != nullptr && child_->ContainsParameterSlots();
  }
  bool InstallParameterSlots(const std::vector<Value>& parameters,
                             size_t* cursor,
                             std::vector<Expression>* slots) override {
    return child_ == nullptr ||
           child_->InstallParameterSlots(parameters, cursor, slots);
  }

  [[nodiscard]] const Table* ScanSource() const override { return nullptr; }
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return stats_;
  }
  [[nodiscard]] const Schema& GetSchema() const override { return schema_; }
  [[nodiscard]] size_t AccessRowCount() const override {
    return child_ != nullptr ? child_->AccessRowCount() : 0;
  }
  [[nodiscard]] size_t EmitRowCount() const override {
    return child_ != nullptr ? child_->EmitRowCount() : 0;
  }
  // Window evaluation appends columns positionally. A buffered shape keeps the
  // input order and therefore the child's claim; a spill-capable shape emits
  // one spill file at a time and claims nothing.
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending) const override;
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending,
      const std::vector<std::optional<bool>>& nulls_first) const override;
  [[nodiscard]] const Plan& GetSource() const { return child_; }
  [[nodiscard]] const std::vector<NamedExpression>& WindowOutputs() const {
    return window_outputs_;
  }
  [[nodiscard]] const WindowMemoryPolicy& Memory() const { return memory_; }
  // The shape may hand rows to a spill path: a finite budget AND one PARTITION
  // BY list shared by every call (a spill file is chosen from that key, so two
  // calls with different partitions could not both keep their groups whole).
  [[nodiscard]] bool SpillCapable() const {
    return memory_.memory_budget_bytes != 0 && CallsShareOnePartitionKey();
  }
  // True when every window call partitions by the same expression list. Called
  // by SpillCapable(), so it is also the place the spill key comes from.
  [[nodiscard]] bool CallsShareOnePartitionKey() const;
  void Dump(std::ostream& o, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

 private:
  Plan child_;
  std::vector<NamedExpression> window_outputs_;
  Schema schema_;
  TableStatistics stats_;
  WindowMemoryPolicy memory_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_WINDOW_PLAN_HPP
