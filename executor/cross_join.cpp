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

#include <cstddef>
#include <ostream>
#include <utility>

#include "common/constants.hpp"
#include "executor_base.hpp"
#include "page/row_position.hpp"

namespace tinylamb {
CrossJoin::CrossJoin(Executor left, Executor right)
    : left_(std::move(left)), right_(std::move(right)) {}

size_t CrossJoin::NextBatch(DataChunk* destination, size_t max_rows) {
  destination->Reset();
  if (max_rows == 0) {
    return 0;
  }
  if (!table_constructed_) {
    TableConstruct();
  }
  // An empty right side produces zero rows; without this guard the iterator
  // below would dereference end().
  if (right_table_.empty()) {
    return 0;
  }
  DataChunk left_batch;
  while (destination->Size() < max_rows) {
    if (right_iter_ == right_table_.end()) {
      if (left_->NextBatch(&left_batch, 1) == 0) {
        FailWithChildOf(*left_);
        break;
      }
      hold_left_ = left_batch.RowAt(0);
      right_iter_ = right_table_.begin();
    }
    destination->Append(hold_left_ + *right_iter_++);
  }
  return destination->Size();
}

void CrossJoin::TableConstruct() {
  DataChunk batch;
  while (right_->NextBatch(&batch) != 0) {
    for (size_t i = 0; i < batch.Size(); ++i) {
      right_table_.push_back(batch.RowAt(i));
    }
  }
  FailWithChildOf(*right_);
  right_iter_ = right_table_.end();
  table_constructed_ = true;
}

void CrossJoin::Dump(std::ostream& o, int indent) const {
  o << "CrossJoin: \n" << Indent(static_cast<size_t>(indent) + 2);
  left_->Dump(o, indent + 2);
  o << "\n" << Indent(static_cast<size_t>(indent) + 2);
  right_->Dump(o, indent + 2);
}
}  // namespace tinylamb
