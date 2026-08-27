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

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "executor/detail/expression_eval.hpp"
#include "executor/executor_base.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/jit.hpp"
#include "expression/named_expression.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

// COUNT(*) is spelled as a kCount aggregate whose child is the column "*".
// This predicate is the single authority for that shape.
[[nodiscard]] inline bool IsCountStar(const AggregateExpression& aggregate) {
  return aggregate.GetType() == AggregationType::kCount &&
         aggregate.Child()->Type() == TypeTag::kColumnValue &&
         aggregate.Child()->AsColumnValue().GetColumnName().name == "*";
}

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
  bool NextGeneric(Row* dst);

  // How each aggregate reads its input for the vectorized accumulator path.
  // kGeneric falls back to the per-row Evaluate loop below.
  enum class AggregateInputKind : uint8_t {
    kCountStar,      // COUNT(*): fed straight from the batch row count.
    kTypedColumn,    // non-distinct column reference over int64/double data.
    kTypedConstant,  // non-distinct constant argument folded once per batch.
    kGeneric,
  };
  struct AggregateInput {
    AggregateInputKind kind{AggregateInputKind::kGeneric};
    size_t column{0};
    ValueType type{ValueType::kNull};
    Value constant;
  };
  // Accumulates one whole batch through the typed accumulators.  Returns
  // false when the batch layout does not match the declared types; the caller
  // must then run the generic per-row loop for that batch untouched.
  bool AccumulateTypedBatch(std::vector<Value>* results,
                            std::vector<int64_t>* counts,
                            const DataChunk& chunk) const;

  std::shared_ptr<ExecutorBase> child_;
  Schema input_schema_;
  std::vector<NamedExpression> aggregates_;
  std::vector<AggregateInput> inputs_;
  // Aggregates whose semantics live in AggregateAccumulator (buffered or
  // specialized types such as ARRAY_AGG / ELEMENTWISE_* / statistics) are
  // delegated instead of using the inline switch in NextGeneric.
  std::vector<bool> delegates_to_accumulator_;
  std::vector<std::unique_ptr<relational_detail::AggregateAccumulator>>
      accumulators_;
  bool all_typed_{false};
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
