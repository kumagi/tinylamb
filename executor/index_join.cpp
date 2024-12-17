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
// Created by kumagi on 22/07/14.
//

#include "executor/index_join.hpp"

#include <memory>
#include <ostream>
#include <sstream>
#include <utility>
#include <vector>

#include "type/value.hpp"
#include "common/constants.hpp"
#include "executor/executor_base.hpp"
#include "table/table.hpp"

namespace tinylamb {
IndexJoin::IndexJoin(Transaction& txn, Executor left,
                     std::vector <slot_t> left_cols, const Table& tbl,
                     const Index& idx, std::vector <slot_t> right_cols)
  : txn_(txn),
    left_(std::move(left)),
    left_cols_(std::move(left_cols)),
    right_(tbl),
    right_idx_(idx),
    right_it_(nullptr),
    right_cols_(std::move(right_cols)) {
}

bool IndexJoin::Load() {
  for (;;) {
    if (!left_->Next(&hold_left_, nullptr)) {
      return false;
    }
    Value left_key = hold_left_.Extract(left_cols_)[0];
    right_it_ = std::make_unique <IndexScanIterator>(right_, right_idx_, txn_,
                                                     left_key, left_key);
    if (right_it_->IsValid()) {
      return true;
    }
  }
}

bool IndexJoin::Next(Row* dst, RowPosition* /* rp */) {
  if (!hold_left_.IsValid() && !Load()) {
    return false;
  }
  for (;;) {
    Row left_key = hold_left_.Extract(left_cols_);
    Row right_row = **right_it_;
    Row right_key = right_row.Extract(right_cols_);
    ++*right_it_;
    *dst = hold_left_ + right_row;
    if (!right_it_->IsValid() && !Load()) {
      return left_key == right_key;
    }
    if (left_key == right_key) {
      return true;
    }
  }
}

void IndexJoin::Dump(std::ostream& o, int indent) const {
  std::stringstream ss;
  ss << "left: {";
  for (size_t i = 0; i < left_cols_.size(); ++i) {
    if (0 < i) {
      ss << ", ";
    }
    ss << left_cols_[i];
  }
  ss << "} right: with " << right_idx_ << " {";
  for (size_t i = 0; i < right_cols_.size(); ++i) {
    if (0 < i) {
      ss << ", ";
    }
    ss << right_cols_[i];
  }
  ss << "}";
  o << "IndexJoin: " << ss.str() << "\n" << Indent(indent + 2);
  left_->Dump(o, indent + 2);
  o << "\n" << Indent(indent + 2) << right_.GetSchema();
}
} // namespace tinylamb
