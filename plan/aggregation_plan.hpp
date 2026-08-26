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
#include <utility>
#include <vector>

#include "expression/named_expression.hpp"
#include "plan/plan.hpp"
#include "type/schema.hpp"

namespace tinylamb {

enum class AggregationStrategy {
  kHash,
  kSort,
  kStream,
};

class AggregationPlan : public PlanBase {
 public:
  AggregationPlan(Plan child, std::vector<NamedExpression> aggregates,
                  AggregationStrategy strategy = AggregationStrategy::kHash);
  [[nodiscard]] const Schema& GetSchema() const override;
  [[nodiscard]] Executor EmitExecutor(TransactionContext& ctx) const override;
  [[nodiscard]] const Table* ScanSource() const override;
  [[nodiscard]] const TableStatistics& GetStats() const override;
  [[nodiscard]] size_t AccessRowCount() const override;
  [[nodiscard]] size_t EmitRowCount() const override;
  [[nodiscard]] AggregationStrategy Strategy() const { return strategy_; }
  [[nodiscard]] std::string ToString() const override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  [[nodiscard]] Schema GenerateSchema() const;
  Plan child_;
  std::vector<NamedExpression> aggregates_;
  AggregationStrategy strategy_;
  // Declaration order matters: schema_ is initialized from child_ and
  // aggregates_ in the constructor's initializer list.
  Schema schema_;
};

// Physical names are kept as distinct plan types so EXPLAIN and rule tests can
// tell the chosen aggregation algorithm apart.  The current Cascades
// grouping payload is scalar (no GROUP BY keys); all three algorithms share
// the same scalar accumulator implementation until grouped keys are carried
// by the logical payload.
class HashAggregatePlan final : public AggregationPlan {
 public:
  HashAggregatePlan(Plan child, std::vector<NamedExpression> aggregates)
      : AggregationPlan(std::move(child), std::move(aggregates),
                        AggregationStrategy::kHash) {}
};

class SortAggregatePlan final : public AggregationPlan {
 public:
  SortAggregatePlan(Plan child, std::vector<NamedExpression> aggregates)
      : AggregationPlan(std::move(child), std::move(aggregates),
                        AggregationStrategy::kSort) {}
};

class StreamAggregatePlan final : public AggregationPlan {
 public:
  StreamAggregatePlan(Plan child, std::vector<NamedExpression> aggregates)
      : AggregationPlan(std::move(child), std::move(aggregates),
                        AggregationStrategy::kStream) {}
};

}  // namespace tinylamb

#endif  // TINYLAMB_AGGREGATION_PLAN_HPP
