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
using Plan = std::shared_ptr<PlanBase>;

struct OptimizerOptions {
  cascades::RuleSet relational_rules;
  ExpressionRuleSet expression_rules;
  std::vector<cascades::ImplementationRule> extra_implementation_rules;
  std::unordered_set<std::string> disabled_implementation_rules;
  // Phase 5 access-path hint applied to the root physical properties.
  cascades::AccessMethod access_method{cascades::AccessMethod::kAny};
  // Phase 9 diagnostics: log the explored memo and the chosen plan.
  bool dump_memo{false};

  [[nodiscard]] static const OptimizerOptions& Default() {
    static const OptimizerOptions options = [] {
      OptimizerOptions built;
      built.relational_rules = cascades::RuleSet::Default();
      built.expression_rules = ExpressionRuleSet::Default();
      return built;
    }();
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
