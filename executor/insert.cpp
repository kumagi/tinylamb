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