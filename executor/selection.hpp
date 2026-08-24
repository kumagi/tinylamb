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

#ifndef TINYLAMB_SELECTION_HPP
#define TINYLAMB_SELECTION_HPP

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "executor_base.hpp"
#include "expression/expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/jit.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {
class ExpressionBase;

class Selection : public ExecutorBase {
 public:
  Selection(Expression exp, Schema schema, Executor src,
            size_t jit_threshold_rows = 20'000'000);
  Selection(const Selection&) = delete;
  Selection(Selection&&) = delete;
  Selection& operator=(const Selection&) = delete;
  Selection& operator=(Selection&&) = delete;
  ~Selection() override = default;

  bool Next(Row* dst, RowPosition* rp) override;
  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;

  void Dump(std::ostream& o, int indent) const override;
  [[nodiscard]] size_t SkippedBatches() const { return skipped_batches_; }
  [[nodiscard]] size_t JitBatches() const { return jit_batches_; }

 private:
  Expression exp_;
  Schema schema_;
  Executor src_;
  DataChunk input_batch_;
  DataChunk output_batch_;
  size_t output_offset_{0};
  size_t skipped_batches_{0};
  std::optional<BytecodeProgram> bytecode_;
  std::optional<JitInt64Kernels> jit_filter_;
  bool jit_attempted_{false};
  uint16_t jit_column_{0};
  int64_t jit_constant_{0};
  BinaryOperation jit_operation_{BinaryOperation::kEquals};
  size_t jit_batches_{0};
  size_t jit_threshold_rows_;
  size_t rows_seen_{0};
  // Surviving row indices of the current input batch; reused across batches
  // so the hot path stays allocation-free.
  std::vector<uint32_t> selection_vector_;
};
}  // namespace tinylamb

#endif  // TINYLAMB_SELECTION_HPP
