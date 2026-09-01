/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/minmax_index.hpp"

#include "common/constants.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

bool MinMaxIndexExecutor::Next(Row* destination, RowPosition* position) {
  if (emitted_) {
    return false;
  }
  emitted_ = true;

  Row input;
  RowPosition input_position;
  while (source_->Next(&input, &input_position)) {
    if (value_slot_ < input.values_.size() &&
        !input[value_slot_].IsNull()) {
      *destination = Row({input[value_slot_]});
      // Scalar aggregates do not expose a row position. Keep the argument
      // accepted by ExecutorBase harmless for callers that pass one.
      (void)position;
      return true;
    }
  }

  // MIN/MAX over an empty or all-NULL input is one row containing NULL.
  *destination = Row({Value()});
  (void)position;
  return true;
}

void MinMaxIndexExecutor::Dump(std::ostream& output, int indent) const {
  output << Indent(indent) << "MinMaxIndex limit=1\n";
  source_->Dump(output, indent + 2);
}

}  // namespace tinylamb
