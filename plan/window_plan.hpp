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

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "expression/named_expression.hpp"
#include "plan/plan.hpp"
#include "table/table_statistics.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class ExecutorBase;

// Logical window evaluation info: one kWindow node's worth of window calls
// (name = hidden `$winN` output column) over a row-preserving child. The
// executor appends one computed column per call and preserves input order.
class WindowPlan final : public PlanBase {
 public:
  WindowPlan(Plan child, std::vector<NamedExpression> window_outputs);

  WindowPlan(const WindowPlan&) = delete;
  WindowPlan(WindowPlan&&) = delete;
  WindowPlan& operator=(const WindowPlan&) = delete;
  WindowPlan& operator=(WindowPlan&&) = delete;
  ~WindowPlan() override = default;

  Executor EmitExecutor(TransactionContext& ctx) const override;

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
  // Window evaluation appends columns positionally without reordering rows.
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending) const override;
  [[nodiscard]] const Plan& GetSource() const { return child_; }
  [[nodiscard]] const std::vector<NamedExpression>& WindowOutputs() const {
    return window_outputs_;
  }
  void Dump(std::ostream& o, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

 private:
  Plan child_;
  std::vector<NamedExpression> window_outputs_;
  Schema schema_;
  TableStatistics stats_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_WINDOW_PLAN_HPP
