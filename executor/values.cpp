/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0 license. */
#include "values.hpp"

#include <cstddef>
#include <ostream>
#include <utility>

#include "executor/data_chunk.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"

namespace tinylamb {

size_t ValuesExecutor::NextBatch(DataChunk* destination, size_t max_rows) {
  if (schema_.ColumnCount() == 0 && !rows_.empty()) {
    std::vector<Column> cols;
    cols.reserve(rows_.front().values_.size());
    for (const Value& v : rows_.front().values_) {
      Column col("", v.type);
      // The row-level type tags are part of the value's identity. Dropping
      // them here would infer a schema that disagrees with the data seen by
      // downstream comparison and JIT paths.
      col.SetUnsigned(v.IsUnsigned());
      col.SetInterval(v.IsInterval());
      cols.push_back(std::move(col));
    }
    // Every VALUES row must agree with the anchor row's width; otherwise a
    // later row would silently lose its extra cells (or assert) on Append.
    for (const Row& row : rows_) {
      if (row.values_.size() != cols.size()) {
        FailWith(StatusError(StatusCode::kInvalidArgument,
                             "values row width mismatch"));
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

void ValuesExecutor::Dump(std::ostream& output, int /*indent*/) const {
  output << "ValuesExecutor";
}

}  // namespace tinylamb
