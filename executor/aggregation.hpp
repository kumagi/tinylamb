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

#ifndef TINYLAMB_AGGREGATION_EXECUTOR_HPP
#define TINYLAMB_AGGREGATION_EXECUTOR_HPP

#include <memory>
#include <vector>

#include "executor/executor_base.hpp"
#include "expression/named_expression.hpp"

namespace tinylamb {

class AggregationExecutor : public ExecutorBase {
 public:
  AggregationExecutor(std::shared_ptr<ExecutorBase> child,
                      std::vector<NamedExpression> aggregates);
  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  std::shared_ptr<ExecutorBase> child_;
  std::vector<NamedExpression> aggregates_;
  bool executed_ = false;
};

}  // namespace tinylamb

#endif  // TINYLAMB_AGGREGATION_EXECUTOR_HPP
