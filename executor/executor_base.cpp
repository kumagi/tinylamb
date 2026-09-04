/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/executor_base.hpp"

#include <cstddef>
#include <utility>

#include "executor/data_chunk.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"

namespace tinylamb {

size_t ExecutorBase::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset();
  // Reset() keeps a default capacity; grow once for larger batches.
  if (max_rows > kDefaultVectorSize) {
    destination->Reserve(max_rows);
  }
  Row row;
  while (destination->Size() < max_rows) {
    RowPosition position;
    if (!Next(&row, &position)) {
      break;
    }
    destination->Append(std::move(row), position);
  }
  return destination->Size();
}

}  // namespace tinylamb
