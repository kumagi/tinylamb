//
// Created by kumagi on 22/05/11.
//

#include "executor/insert.hpp"

#include <ostream>

#include "table/table.hpp"

namespace tinylamb {

bool Insert::Next(Row* dst, RowPosition* rp) {
  if (finished_) {
    return false;
  }
  int64_t insertion_count = 0;
  Row new_row;
  while (src_->Next(&new_row, nullptr)) {
    target_->Insert(*txn_, new_row);
    insertion_count++;
  }
  *dst = Row({Value("Insert Rows"), Value(insertion_count)});
  if (rp != nullptr) {
    *rp = RowPosition();  // Fill with an invalid data.
  }
  finished_ = true;
  return true;
}

void Insert::Dump(std::ostream& o, int indent) const {
  o << "Insert: " << target_->GetSchema().Name() << "\n" << Indent(indent + 2);
  src_->Dump(o, indent + 2);
}

}  // namespace tinylamb