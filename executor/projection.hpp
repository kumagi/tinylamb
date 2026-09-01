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

#ifndef TINYLAMB_PROJECTION_HPP
#define TINYLAMB_PROJECTION_HPP

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "executor_base.hpp"
#include "expression/named_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/jit.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class Projection : public ExecutorBase {
 public:
  Projection(std::vector<NamedExpression> expressions, Schema input_schema,
             Executor src, size_t jit_threshold_rows = 20'000'000);
  Projection(const Projection&) = delete;
  Projection(Projection&&) = delete;
  Projection& operator=(const Projection&) = delete;
  Projection& operator=(Projection&&) = delete;
  ~Projection() override = default;

  bool Next(Row* dst, RowPosition* rp) override;
  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& o, int indent) const override;
  [[nodiscard]] size_t JitBatches() const { return jit_batches_; }

 private:
  std::vector<NamedExpression> expressions_;
  // Repeated scalar subexpressions are evaluated into hidden batch columns
  // and the visible expressions refer to those columns.  This keeps CSE
  // batch-local (and therefore safe across volatile/query expressions) while
  // avoiding an additional materialized relation.
  std::vector<Expression> cse_expressions_;
  std::vector<std::string> cse_names_;
  std::vector<size_t> cse_use_counts_;
  Schema augmented_schema_;
  Schema input_schema_;
  Executor src_;
  DataChunk input_batch_;
  DataChunk cse_input_batch_;
  DataChunk output_batch_;
  size_t output_offset_{0};
  std::vector<std::optional<BytecodeProgram>> cse_bytecodes_;
  std::vector<std::optional<BytecodeProgram>> bytecodes_;
  struct JitProjectionState {
    bool eligible{false};
    bool attempted{false};
    uint16_t column{0};
    int64_t multiplier{1};
    int64_t addend{0};
    size_t rows_seen{0};
    std::optional<JitInt64Kernels> kernel;
  };
  std::vector<JitProjectionState> jit_states_;
  size_t jit_threshold_rows_;
  size_t jit_batches_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_PROJECTION_HPP
