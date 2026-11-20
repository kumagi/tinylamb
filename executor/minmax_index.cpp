/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/minmax_index.hpp"

#include <cstddef>
#include <ostream>

#include "common/constants.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

size_t MinMaxIndexExecutor::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset();
  if (max_rows == 0 || emitted_) {
    return 0;
  }
  emitted_ = true;

  // T3767: pull the source in vector-sized batches and probe the wanted
  // column through ColumnVector::IsNull.  The old loop asked for one row per
  // NextBatch call (paying Reset/Reserve/scan-set-up per row over a NULL
  // prefix or low-selectivity column) and materialized a full Row via
  // RowAt() just to test one slot.
  DataChunk batch;
  while (source_->NextBatch(&batch, kDefaultVectorSize) != 0) {
    if (value_slot_ >= batch.ColumnCount()) {
      continue;
    }
    const ColumnVector& column = batch.ColumnAt(value_slot_);
    for (size_t i = 0; i < batch.Size(); ++i) {
      if (!column.IsNull(i)) {
        destination->Append(Row({column.ValueAt(i)}));
        return 1;
      }
    }
  }
  if (source_->GetStatus() != Status::kSuccess) {
    FailWith(source_->GetStatus());
    return 0;
  }

  // MIN/MAX over an empty or all-NULL input is one row containing NULL.
  destination->Append(Row({Value()}));
  return 1;
}

void MinMaxIndexExecutor::Dump(std::ostream& output, int indent) const {
  output << Indent(static_cast<size_t>(indent)) << "MinMaxIndex limit=1\n";
  source_->Dump(output, indent + 2);
}

}  // namespace tinylamb
