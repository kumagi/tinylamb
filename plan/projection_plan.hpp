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

#ifndef TINYLAMB_PROJECTION_PLAN_HPP
#define TINYLAMB_PROJECTION_PLAN_HPP

#include <memory>
#include <utility>
#include <vector>

#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "plan/plan.hpp"
#include "table/table_statistics.hpp"

namespace tinylamb {

class ProjectionPlan : public PlanBase {
 public:
  ProjectionPlan(Plan src, std::vector<NamedExpression> project_columns);
  ProjectionPlan(Plan src, const std::vector<ColumnName>& project_columns);
  ProjectionPlan(const ProjectionPlan&) = delete;
  ProjectionPlan(ProjectionPlan&&) = delete;
  ProjectionPlan& operator=(const ProjectionPlan&) = delete;
  ProjectionPlan& operator=(ProjectionPlan&&) = delete;
  ~ProjectionPlan() override = default;

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
  [[nodiscard]] std::string ToString() const override;

 private:
  [[nodiscard]] Schema CalcSchema() const;

  Plan src_;
  std::vector<NamedExpression> columns_;
  Schema output_schema_;
  TableStatistics stats_;
};

}  // namespace tinylamb
#endif  // TINYLAMB_PROJECTION_PLAN_HPP
