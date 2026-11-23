/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/minmax_index.hpp"

#include <cstddef>
#include <ostream>

#include "common/constants.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

namespace {
// Guard for the "first non-NULL value is the extremum" contract. Only the
// scalar types whose Value::operator< agrees with the index's memcomparable
// key order are checked: VARCHAR's canonical (collated) order, FLOAT64's NaN
// canonicalization and unsigned integers can legitimately disagree with that
// byte order, so comparing them here would raise false alarms. This is a
// miswiring check, not a replacement for a full sort validation.
bool OutOfOrderForExtremum(const Value& first, const Value& later,
                           bool descending) {
  if (first.type != later.type ||
      (first.type != ValueType::kInt64 && first.type != ValueType::kDate) ||
      first.IsUnsigned() || later.IsUnsigned()) {
    return false;
  }
  return descending ? (later > first) : (later < first);
}
}  // namespace

size_t MinMaxIndexExecutor::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset();
  if (GetStatus() != Status::kSuccess) {
    return 0;
  }
  if (max_rows == 0 || emitted_) {
    return 0;
  }
  // emitted_ is set only once a row is actually produced. Setting it before
  // reading the source would make a later retry after a source error return 0
  // immediately instead of re-notifying the sticky failure (GetStatus()
  // contract).

  // T3767: pull the source in vector-sized batches and probe the wanted
  // column through ColumnVector::IsNull.  The old loop asked for one row per
  // NextBatch call (paying Reset/Reserve/scan-set-up per row over a NULL
  // prefix or low-selectivity column) and materialized a full Row via
  // RowAt() just to test one slot.
  DataChunk batch;
  while (source_->NextBatch(&batch, kDefaultVectorSize) != 0) {
    if (value_slot_ >= batch.ColumnCount()) {
      FailWith(StatusError(StatusCode::kInvalidArgument,
                           "MinMaxIndexExecutor: value column missing"));
      return 0;
    }
    const ColumnVector& column = batch.ColumnAt(value_slot_);
    for (size_t i = 0; i < batch.Size(); ++i) {
      if (column.IsNull(i)) {
        continue;
      }
      // Valid only because the source is ordered on value_slot_ (ascending
      // for MIN / descending for MAX) -- see the constructor contract.
      // Check the neighbours already in this batch: an unordered child no
      // longer silently turns an arbitrary value into MIN/MAX.
      const Value first = column.ValueAt(i);
      for (size_t j = i + 1; j < batch.Size(); ++j) {
        if (column.IsNull(j)) {
          continue;
        }
        if (OutOfOrderForExtremum(first, column.ValueAt(j), descending_)) {
          FailWith(StatusError(StatusCode::kInvalidArgument,
                               "MinMaxIndexExecutor: unordered source"));
          return 0;
        }
      }
      destination->Append(Row({first}));
      emitted_ = true;
      return 1;
    }
  }
  if (source_->GetStatus() != Status::kSuccess) {
    FailWith(source_->GetStatus());
    return 0;
  }

  // MIN/MAX over an empty or all-NULL input is one row containing NULL.
  destination->Append(Row({Value()}));
  emitted_ = true;
  return 1;
}

void MinMaxIndexExecutor::Dump(std::ostream& output, int indent) const {
  output << Indent(static_cast<size_t>(indent)) << "MinMaxIndex limit=1\n";
  source_->Dump(output, indent + 2);
}

}  // namespace tinylamb
