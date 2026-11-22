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

size_t ConstantExecutor::NextBatch(DataChunk* destination, size_t max_rows) {
  if (schema_.ColumnCount() == 0 && !rows_.empty()) {
    std::vector<Column> cols;
    cols.reserve(rows_.front().values_.size());
    for (const Value& v : rows_.front().values_) {
      Column col("", v.type);
      // Keep the row-level type tags the values actually carry.
      col.SetUnsigned(v.IsUnsigned());
      col.SetInterval(v.IsInterval());
      cols.push_back(std::move(col));
    }
    // Same contract as ValuesExecutor: a mismatched row width would break
    // the DataChunk invariant checked by Append.
    for (const Row& row : rows_) {
      if (row.values_.size() != cols.size()) {
        FailWith(StatusError(StatusCode::kInvalidArgument,
                             "constant row width mismatch"));
        return 0;
      }
    }
    schema_ = Schema("", std::move(cols));
  }
  destination->Reset(schema_);
  while (offset_ < rows_.size() && destination->Size() < max_rows) {
    destination->Append(std::move(rows_[offset_++]));
  }
  return destination->Size();
}

void ConstantExecutor::Dump(std::ostream& o, int /*indent*/) const {
  o << "ConstantExecutor";
}

void EmptyResultExecutor::Dump(std::ostream& o, int /*indent*/) const {
  // T2114: the old form hardcoded `Filter short_circuit_rhs_evaluations=0`
  // -- a measured-looking counter this node never increments (identical
  // before EXPLAIN and after EXPLAIN ANALYZE) -- and its bare '\n' dropped
  // the second line to column 0 under an indented parent.  The node has no
  // runtime state to report: name only.
  o << "EmptyResult";
}

}  // namespace tinylamb
