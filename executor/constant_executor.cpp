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

#include "executor/constant_executor.hpp"

#include <cstddef>
#include <ostream>
#include <utility>

#include "executor/data_chunk.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"

namespace tinylamb {

bool ConstantExecutor::Next(Row* row, RowPosition* /*rp*/) {
  if (offset_ >= rows_.size()) {
    return false;
  }
  // Each row is emitted once, so moving out is safe.
  *row = std::move(rows_[offset_++]);
  return true;
}

size_t ConstantExecutor::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset();
  while (offset_ < rows_.size() && destination->Size() < max_rows) {
    destination->Append(std::move(rows_[offset_++]));
  }
  return destination->Size();
}

void ConstantExecutor::Dump(std::ostream& o, int /*indent*/) const {
  o << "ConstantExecutor";
}

void EmptyResultExecutor::Dump(std::ostream& o, int /*indent*/) const {
  o << "EmptyResult\nFilter short_circuit_rhs_evaluations=0";
}

}  // namespace tinylamb
