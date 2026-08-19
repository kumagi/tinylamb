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

#ifndef TINYLAMB_OPTIMIZER_HPP
#define TINYLAMB_OPTIMIZER_HPP

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "common/status_or.hpp"
#include "expression/rewrite.hpp"
#include "plan/cascades.hpp"
#include "query/query_data.hpp"

namespace tinylamb {
class ExecutorBase;
class TransactionContext;
class PlanBase;
typedef std::shared_ptr<PlanBase> Plan;

struct OptimizerOptions {
  cascades::RuleSet relational_rules;
  ExpressionRuleSet expression_rules;
  std::vector<cascades::ImplementationRule> extra_implementation_rules;
  std::unordered_set<std::string> disabled_implementation_rules;

  [[nodiscard]] static OptimizerOptions Default() {
    OptimizerOptions options;
    options.relational_rules = cascades::RuleSet::Default();
    options.expression_rules = ExpressionRuleSet::Default();
    return options;
  }
};

class Optimizer {
 public:
  explicit Optimizer() = default;

  static StatusOr<Plan> Optimize(const QueryData& query,
                                 TransactionContext& ctx);
  static StatusOr<Plan> Optimize(const QueryData& query,
                                 TransactionContext& ctx,
                                 const OptimizerOptions& options);
};

}  // namespace tinylamb

#endif  // TINYLAMB_OPTIMIZER_HPP
