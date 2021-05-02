//
// Created by kumagi on 22/05/11.
//

#include "update.hpp"

#include <ostream>

#include "common/constants.hpp"
#include "table/table.hpp"

namespace tinylamb {

bool Update::Next(Row* dst, RowPosition* rp) {
  if (finished_) {
    return false;
  }
  int64_t update_count = 0;
  Row new_row;
  RowPosition position;
  while (src_->Next(&new_row, &position)) {
    assert(position.IsValid());
    StatusOr<RowPosition> p = target_->Update(*txn_, position, new_row);
    if (p.GetStatus() != Status::kSuccess) {
      finished_ = true;
      return true;
    }
    update_count++;
  }
  *dst = Row({Value("Update Rows"), Value(update_count)});
  if (rp != nullptr) {
    *rp = RowPosition();  // Fill with an invalid data.
  }
  finished_ = true;
  return true;
}

void Update::Dump(std::ostream& o, int indent) const {
  o << "Update: " << target_->GetSchema().Name() << "\n" << Indent(indent + 2);
  src_->Dump(o, indent + 2);
}

}  // namespace tinylamb