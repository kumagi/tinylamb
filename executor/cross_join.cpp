//
// Created by kumagi on 2022/03/02.
//

#include "cross_join.hpp"

namespace tinylamb {

CrossJoin::CrossJoin(Executor left, Executor right)
    : left_(std::move(left)), right_(std::move(right)) {}

bool CrossJoin::Next(Row* dst, RowPosition* /*rp*/) {
  if (!table_constructed_) {
    TableConstruct();
  }
  if (right_iter_ == right_table_.end()) {
    if (!left_->Next(&hold_left_, nullptr)) {
      return false;
    }
    right_iter_ = right_table_.begin();
  }
  *dst = hold_left_ + *right_iter_++;
  return true;
}

void CrossJoin::Dump(std::ostream& o, int indent) const {
  o << "CrossJoin: \n" << Indent(indent + 2);
  left_->Dump(o, indent + 2);
  o << "\n" << Indent(indent + 2);
  right_->Dump(o, indent + 2);
}

void CrossJoin::TableConstruct() {
  Row right_row;
  while (right_->Next(&right_row, nullptr)) {
    right_table_.push_back(right_row);
  }
  right_iter_ = right_table_.end();
  table_constructed_ = true;
}

}  // namespace tinylamb
