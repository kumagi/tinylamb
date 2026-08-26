/** Copyright 2026 KUMAZAKI Hiroki. Licensed under the Apache-2.0 license. */
#include "values.hpp"

#include <ostream>
#include <utility>

#include "common/constants.hpp"
#include "executor/data_chunk.hpp"

namespace tinylamb {

bool ValuesExecutor::Next(Row* row, RowPosition* /*rp*/) {
  if (offset_ >= rows_.size()) { return false; }
  *row = std::move(rows_[offset_++]);
  return true;
}

size_t ValuesExecutor::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset();
  while (offset_ < rows_.size() && destination->Size() < max_rows) {
    destination->Append(std::move(rows_[offset_++]));
  }
  return destination->Size();
}

void ValuesExecutor::Dump(std::ostream& output, int /*indent*/) const {
  output << "ValuesExecutor";
}

}  // namespace tinylamb
