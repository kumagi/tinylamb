/**
 * Copyright 2023 KUMAZAKI Hiroki
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

#ifndef TINYLAMB_SELECTION_PLAN_HPP
#define TINYLAMB_SELECTION_PLAN_HPP

#include <utility>

#include "expression/expression.hpp"
#include "plan/plan.hpp"
#include "table/table_statistics.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class SelectionPlan : public PlanBase {
 public:
  SelectionPlan(Plan src, Expression exp, TableStatistics stat)
      : src_(std::move(src)), exp_(std::move(exp)), stats_(std::move(stat)) {}
  SelectionPlan(const SelectionPlan&) = delete;
  SelectionPlan(SelectionPlan&&) = delete;
  SelectionPlan& operator=(const SelectionPlan&) = delete;
  SelectionPlan& operator=(SelectionPlan&&) = delete;
  ~SelectionPlan() override = default;

  Executor EmitExecutor(TransactionContext& ctx) const override;

  [[nodiscard]] const Table* ScanSource() const override {
    return src_->ScanSource();
  };
  [[nodiscard]] const Schema& GetSchema() const override;
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return stats_;
  }

  [[nodiscard]] size_t AccessRowCount() const override;
  [[nodiscard]] size_t EmitRowCount() const override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  Plan src_;
  Expression exp_;
  TableStatistics stats_;
};

}  // namespace tinylamb
#endif  // TINYLAMB_SELECTION_PLAN_HPP
