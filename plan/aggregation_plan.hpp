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

#ifndef TINYLAMB_AGGREGATION_PLAN_HPP
#define TINYLAMB_AGGREGATION_PLAN_HPP

#include <memory>
#include <vector>

#include "expression/named_expression.hpp"
#include "plan/plan.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class AggregationPlan : public PlanBase {
 public:
  AggregationPlan(Plan child, std::vector<NamedExpression> aggregates);
  [[nodiscard]] const Schema& GetSchema() const override;
  [[nodiscard]] Executor EmitExecutor(TransactionContext& ctx) const override;
  [[nodiscard]] const Table* ScanSource() const override;
  [[nodiscard]] const TableStatistics& GetStats() const override;
  [[nodiscard]] size_t AccessRowCount() const override;
  [[nodiscard]] size_t EmitRowCount() const override;
  [[nodiscard]] std::string ToString() const override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  [[nodiscard]] Schema GenerateSchema() const;
  Plan child_;
  std::vector<NamedExpression> aggregates_;
  mutable Schema schema_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_AGGREGATION_PLAN_HPP
