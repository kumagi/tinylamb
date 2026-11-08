/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/limit.hpp"

#include <algorithm>
#include <cstddef>
#include <ostream>

#include "common/constants.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
namespace tinylamb {
bool LimitExecutor::NextInput(Row* dst, RowPosition* position,
                              size_t requested) {
  if (input_offset_ >= input_batch_.Size()) {
    input_offset_ = 0;
    if (source_->NextBatch(&input_batch_,
                           std::min(requested, kDefaultVectorSize)) == 0) {
      FailWithChildOf(*source_);
      return false;
    }
  }
  *dst = input_batch_.RowAt(input_offset_);
  if (position != nullptr) {
    *position = input_batch_.PositionAt(input_offset_);
  }
  ++input_offset_;
  return true;
}

size_t LimitExecutor::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset();
  Row ignored;
  while (skipped_ < offset_) {
    if (!NextInput(&ignored, nullptr, offset_ - skipped_)) {
      FailWithChildOf(*source_);
      return 0;
    }
    ++skipped_;
    ++consumed_rows_;
  }
  if (limit_ != 0 && emitted_ >= limit_) {
    early_stop_ = true;
    return 0;
  }
  const size_t remaining = limit_ == 0 ? max_rows : (limit_ - emitted_);
  const size_t batch_limit = std::min(max_rows, remaining);
  Row dst;
  RowPosition rp;
  while (destination->Size() < batch_limit) {
    // Pass demand through batch-buffering projections/filters. Calling their
    // row interface can fill a default vector (and read the whole filtered
    // relation) before this LIMIT sees even its first row.
    const size_t want =
        limit_ == 0
            ? (batch_limit - destination->Size())
            : std::min(batch_limit - destination->Size(), limit_ - emitted_);
    if (!NextInput(&dst, &rp, std::max<size_t>(1, want))) {
      FailWithChildOf(*source_);
      break;
    }
    destination->Append(std::move(dst), rp);
    ++emitted_;
    ++consumed_rows_;
  }
  return destination->Size();
}
void LimitExecutor::Dump(std::ostream& output, int indent) const {
  output << "Limit: " << limit_ << " offset " << offset_
         << " (Limit count=" << limit_ << " offset=" << offset_ << ")\n"
         << Indent(static_cast<size_t>(indent) + 2);
  source_->Dump(output, indent + 2);
  if (early_stop_) {
    output << "\n"
           << Indent(static_cast<size_t>(indent))
           << "Limit early_stop=true consumed_rows=" << consumed_rows_;
  }
}
}  // namespace tinylamb
