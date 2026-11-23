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
  if (!schema_initialized_ && !rows_.empty()) {
    schema_initialized_ = true;
    const size_t width = rows_.front().Count();
    for (const Row& row : rows_) {
      if (row.Count() != width) {
        destination->Reset();
        offset_ = rows_.size();
        FailWith(StatusError(StatusCode::kInvalidArgument,
                             "values row width mismatch"));
        return 0;
      }
    }
    std::vector<Column> cols;
    cols.reserve(width);
    for (size_t i = 0; i < width; ++i) {
      const Value* sample = nullptr;
      for (const Row& row : rows_) {
        const Value& value = row[i];
        if (value.IsNull()) {
          continue;
        }
        if (sample == nullptr) {
          sample = &value;
        } else if (value.type != sample->type ||
                   value.IsUnsigned() != sample->IsUnsigned() ||
                   value.IsInterval() != sample->IsInterval()) {
          // Let each batch infer a mixed column from its actual cells, so
          // the first cell of the batch cannot inherit another row's flags.
          sample = nullptr;
          break;
        }
      }
      Column& col = cols.emplace_back("", (sample != nullptr) ? sample->type
                                                 : ValueType::kNull);
      if (sample != nullptr) {
        col.SetUnsigned(sample->IsUnsigned());
        col.SetInterval(sample->IsInterval());
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
