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
#include <optional>
#include <vector>

#include "executor/executor_base.hpp"
#include "expression/named_expression.hpp"
#include "expression/jit.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class AggregationExecutor : public ExecutorBase {
 public:
  AggregationExecutor(std::shared_ptr<ExecutorBase> child, Schema input_schema,
                      std::vector<NamedExpression> aggregates,
                      size_t jit_threshold_rows = 20'000'000);
  bool Next(Row* dst, RowPosition* rp) override;
  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& o, int indent) const override;
  [[nodiscard]] size_t JitBatches() const { return jit_batches_; }

 private:
  std::shared_ptr<ExecutorBase> child_;
  Schema input_schema_;
  std::vector<NamedExpression> aggregates_;
  bool executed_ = false;
  DataChunk input_batch_;
  bool jit_sum_eligible_{false};
  uint16_t jit_sum_column_{0};
  size_t jit_threshold_rows_;
  size_t rows_seen_{0};
  bool jit_attempted_{false};
  std::optional<JitInt64Kernels> jit_sum_;
  size_t jit_batches_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_AGGREGATION_EXECUTOR_HPP
