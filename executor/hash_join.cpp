//
// Created by kumagi on 2022/02/23.
//

#include "hash_join.hpp"

namespace tinylamb {

HashJoin::HashJoin(std::unique_ptr<ExecutorBase>&& left,
                   std::vector<size_t> left_cols,
                   std::unique_ptr<ExecutorBase>&& right,
                   std::vector<size_t> right_cols)
    : left_(std::move(left)),
      left_cols_(std::move(left_cols)),
      right_(std::move(right)),
      right_cols_(std::move(right_cols)) {}

bool HashJoin::Next(Row* dst) {
  if (!bucket_constructed_) BucketConstruct();
  if (right_buckets_iterator_ != right_buckets_.end()) {
    *dst = hold_left_ + right_buckets_iterator_->second;
  } else {
    for (;;) {
      if (!left_->Next(&hold_left_)) return false;
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
  while (right_->Next(&row)) {
    right_buckets_.emplace(row.Extract(right_cols_).EncodeMemcomparableFormat(),
                           row);
  }
  right_buckets_iterator_ = right_buckets_.end();
  bucket_constructed_ = true;
}

void HashJoin::Dump(std::ostream& o, int indent) const {
  o << "Hash Join: ";
  left_->Dump(o, indent + 2);
  o << "\n" << Indent(indent) << "           ";
  right_->Dump(o, indent + 2);
}

}  // namespace tinylamb