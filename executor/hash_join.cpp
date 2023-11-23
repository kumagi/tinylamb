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

#include "hash_join.hpp"

#include <utility>

#include "common/debug.hpp"

namespace tinylamb {

HashJoin::HashJoin(Executor left, std::vector<slot_t> left_cols, Executor right,
                   std::vector<slot_t> right_cols)
    : left_(std::move(left)),
      left_cols_(std::move(left_cols)),
      right_(std::move(right)),
      right_cols_(std::move(right_cols)) {}

bool HashJoin::Next(Row* dst, RowPosition* rp) {
  if (!bucket_constructed_) {
    BucketConstruct();
  }
  if (right_buckets_iterator_ != right_buckets_.end()) {
    *dst = hold_left_ + right_buckets_iterator_->second;
  } else {
    for (;;) {
      if (!left_->Next(&hold_left_, rp)) {
        return false;
      }
      left_key_ = hold_left_.Extract(left_cols_).EncodeMemcomparableFormat();
      right_buckets_iterator_ = right_buckets_.find(left_key_);
      if (right_buckets_iterator_ != right_buckets_.end()) {
        *dst = hold_left_ + right_buckets_iterator_->second;
        break;
      }
    }
  }

  if ((++right_buckets_iterator_) != right_buckets_.end() &&
      right_buckets_iterator_->first != left_key_) {
    right_buckets_iterator_ = right_buckets_.end();
    left_key_.clear();
  }
  return true;
}

void HashJoin::BucketConstruct() {
  Row row;
  while (right_->Next(&row, nullptr)) {
    right_buckets_.emplace(row.Extract(right_cols_).EncodeMemcomparableFormat(),
                           row);
  }
  right_buckets_iterator_ = right_buckets_.end();
  bucket_constructed_ = true;
}

void HashJoin::Dump(std::ostream& o, int indent) const {
  std::stringstream ss;
  ss << "left: {";
  for (size_t i = 0; i < left_cols_.size(); ++i) {
    if (0 < i) {
      ss << ", ";
    }
    ss << left_cols_[i];
  }
  ss << "} right: {";
  for (size_t i = 0; i < right_cols_.size(); ++i) {
    if (0 < i) {
      ss << ", ";
    }
    ss << right_cols_[i];
  }
  ss << "}";
  o << "HashJoin: " << ss.str() << "\n" << Indent(indent + 2);
  left_->Dump(o, indent + 2);
  o << "\n" << Indent(indent + 2);
  right_->Dump(o, indent + 2);
}

}  // namespace tinylamb