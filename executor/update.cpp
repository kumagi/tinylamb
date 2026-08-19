/**
 * Copyright 2023 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#include "update.hpp"

#include <cassert>
#include <cstdint>
#include <ostream>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "table/table.hpp"
#include "type/value.hpp"

namespace tinylamb {

bool Update::Next(Row* dst, RowPosition* rp) {
  if (finished_) {
    return false;
  }
  std::vector<std::pair<Row, RowPosition>> pending;
  Row new_row;
  RowPosition position;
  while (src_->Next(&new_row, &position)) {
    assert(position.IsValid());
    pending.emplace_back(std::move(new_row), position);
  }
  int64_t update_count = 0;
  for (auto& [row, row_position] : pending) {
    StatusOr<RowPosition> p = target_->Update(*txn_, row_position, row);
    if (p.GetStatus() != Status::kSuccess) {
      break;
    }
    update_count++;
  }
  *dst = Row({Value("Update Rows"), Value(update_count)});
  if (rp != nullptr) {
    *rp = RowPosition();
  }
  finished_ = true;
  return true;
}

void Update::Dump(std::ostream& o, int indent) const {
  o << "Update: " << target_->GetSchema().Name() << "\n" << Indent(indent + 2);
  src_->Dump(o, indent + 2);
}

}  // namespace tinylamb
