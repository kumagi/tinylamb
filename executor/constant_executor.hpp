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

#ifndef TINYLAMB_CONSTANT_EXECUTOR_HPP
#define TINYLAMB_CONSTANT_EXECUTOR_HPP

#include <vector>

#include "executor/executor_base.hpp"
#include "type/row.hpp"

namespace tinylamb {

class ConstantExecutor : public ExecutorBase {
 public:
  explicit ConstantExecutor(Row row) : rows_({std::move(row)}) {}
  explicit ConstantExecutor(std::vector<Row> rows) : rows_(std::move(rows)) {}
  bool Next(Row* row, RowPosition* rp) override;
  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  std::vector<Row> rows_;
  size_t offset_{0};
};

// Explicit zero-row source used by EmptyPlan. Keeping a distinct EXPLAIN name
// makes contradiction elimination observable without opening the discarded
// scan child.
class EmptyResultExecutor final : public ConstantExecutor {
 public:
  EmptyResultExecutor() : ConstantExecutor(std::vector<Row>{}) {}
  void Dump(std::ostream& o, int indent) const override;
};

}  // namespace tinylamb

#endif  // TINYLAMB_CONSTANT_EXECUTOR_HPP
