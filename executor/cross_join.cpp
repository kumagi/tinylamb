/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


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

void CrossJoin::TableConstruct() {
  Row right_row;
  while (right_->Next(&right_row, nullptr)) {
    right_table_.push_back(right_row);
  }
  right_iter_ = right_table_.end();
  table_constructed_ = true;
}

void CrossJoin::Dump(std::ostream& o, int indent) const {
  o << "CrossJoin: \n" << Indent(indent + 2);
  left_->Dump(o, indent + 2);
  o << "\n" << Indent(indent + 2);
  right_->Dump(o, indent + 2);
}

}  // namespace tinylamb
