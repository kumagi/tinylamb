/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/cardinality_probe.hpp"

#include <chrono>
#include <cstddef>
#include <ostream>
#include <string>

#include "executor/data_chunk.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"

namespace tinylamb {

bool CardinalityProbe::Next(Row* dst, RowPosition* rp) {
  if (!child_) {
    return false;
  }
  const auto start = std::chrono::steady_clock::now();
  const bool has_next = child_->Next(dst, rp);
  const auto end = std::chrono::steady_clock::now();
  elapsed_nanos_ += static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
  if (has_next) {
    ++actual_rows_;
  }
  return has_next;
}

size_t CardinalityProbe::NextBatch(DataChunk* destination, size_t max_rows) {
  if (!child_) {
    return 0;
  }
  const auto start = std::chrono::steady_clock::now();
  const size_t count = child_->NextBatch(destination, max_rows);
  const auto end = std::chrono::steady_clock::now();
  elapsed_nanos_ += static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
  actual_rows_ += count;
  return count;
}

void CardinalityProbe::Dump(std::ostream& o, int indent) const {
  o << std::string(indent, ' ') << "CardinalityProbe (op: "
    << (operator_name_.empty() ? "unnamed" : operator_name_)
    << ", est: " << estimated_cardinality_ << ", act: " << actual_rows_
    << ", error: " << CardinalityError() << "x)\n";
  if (child_) {
    child_->Dump(o, indent + 2);
  }
}

}  // namespace tinylamb
