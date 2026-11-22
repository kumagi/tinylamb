/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/executor_base.hpp"

#include <cstddef>
#include <utility>

#include "executor/data_chunk.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"

namespace tinylamb {

bool ExecutorBase::Next(Row* dst, RowPosition* rp) {
  // T4494: row-at-a-time fetching used to construct a fresh DataChunk and
  // pull exactly one row per call; buffer a full batch instead and serve it
  // row by row.  Mixed batch/row consumers must drain via
  // NextBatchDraining().
  if (row_buffer_pos_ >= row_buffer_.Size()) {
    row_buffer_.Reset();
    row_buffer_pos_ = 0;
    if (NextBatch(&row_buffer_, kDefaultVectorSize) == 0) {
      return false;
    }
  }
  if (dst != nullptr) {
    *dst = row_buffer_.RowAt(row_buffer_pos_);
  }
  if (rp != nullptr) {
    *rp = row_buffer_.PositionAt(row_buffer_pos_);
  }
  ++row_buffer_pos_;
  return true;
}

size_t ExecutorBase::NextBatchDraining(DataChunk* destination,
                                       size_t max_rows) {
  size_t produced = 0;
  // Same contract as NextBatch: the destination owns the batch and the caller
  // reads only [0, returned). Without this Reset a second small draining call
  // would append behind the previous batch's tail while the caller re-read
  // the stale prefix, re-emitting the same rows.
  destination->Reset();
  while (produced < max_rows && row_buffer_pos_ < row_buffer_.Size()) {
    destination->AppendFrom(row_buffer_, row_buffer_pos_);
    ++row_buffer_pos_;
    ++produced;
  }
  if (produced > 0) {
    // The batch call below resets `destination`; stop after handing over
    // what Next() had already buffered so no row is dropped. A draining call
    // therefore returns at most the buffered rows (or one NextBatch pull when
    // nothing was buffered): a short batch is normal even while the source
    // still has rows, and callers must keep pulling until 0 is returned.
    return produced;
  }
  return NextBatch(destination, max_rows);
}

}  // namespace tinylamb
