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

  DataChunk batch;
  while (source_->NextBatch(&batch, 1) != 0) {
    for (size_t i = 0; i < batch.Size(); ++i) {
      const Row input = batch.RowAt(i);
      if (value_slot_ < input.values_.size() && !input[value_slot_].IsNull()) {
        destination->Append(Row({input[value_slot_]}));
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
