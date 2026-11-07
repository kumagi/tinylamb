/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/executor_base.hpp"

#include <cstddef>
#include <utility>

#include "executor/data_chunk.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"

namespace tinylamb {

bool ExecutorBase::Next(Row* dst, RowPosition* rp) {
  DataChunk chunk;
  if (NextBatch(&chunk, 1) == 0) {
    return false;
  }
  if (dst != nullptr) {
    *dst = chunk.RowAt(0);
  }
  if (rp != nullptr) {
    *rp = chunk.PositionAt(0);
  }
  return true;
}

}  // namespace tinylamb
