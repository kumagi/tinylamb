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


#include "b_plus_tree_iterator.hpp"

#include "common/debug.hpp"
#include "index/b_plus_tree.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

BPlusTreeIterator::BPlusTreeIterator(BPlusTree* tree, Transaction* txn,
                                     std::string_view begin,
                                     std::string_view end, bool ascending)
    : tree_(tree), txn_(txn), begin_(begin), end_(end) {
  if (!end.empty() && !begin.empty() && end < begin) {
    std::runtime_error("invalid begin & end");
  }
  if (ascending) {
    if (begin.empty()) {
      PageRef leaf = tree->LeftmostPage(*txn_);
      pid_ = leaf->PageID();
      idx_ = 0;
      valid_ = true;
    } else {
      PageRef leaf = tree_->FindLeaf(*txn_, begin, false);
      pid_ = leaf->PageID();
      idx_ = leaf->body.leaf_page.Find(begin);
      valid_ = 0 <= idx_ && idx_ < leaf->body.leaf_page.row_count_;
    }
  } else {
    if (end.empty()) {
      PageRef leaf = tree->RightmostPage(*txn_);
      pid_ = leaf->PageID();
      idx_ = leaf->body.leaf_page.row_count_ - 1;
      valid_ = true;
    } else {
      PageRef leaf = tree_->FindLeaf(*txn_, end, false);
      pid_ = leaf->PageID();
      idx_ = leaf->body.leaf_page.Find(end);
      valid_ = 0 <= idx_ && idx_ < leaf->body.leaf_page.row_count_;
    }
  }
}

std::string BPlusTreeIterator::Key() const {
  return std::string(
      txn_->PageManager()->GetPage(pid_)->body.leaf_page.GetKey(idx_));
}

std::string BPlusTreeIterator::Value() const {
  return std::string(
      txn_->PageManager()->GetPage(pid_)->body.leaf_page.GetValue(idx_));
}

BPlusTreeIterator& BPlusTreeIterator::operator++() {
  PageRef ref = txn_->PageManager()->GetPage(pid_);
  LeafPage* const lp = &ref->body.leaf_page;
  idx_++;
  if (lp->row_count_ <= idx_) {
    if (pid_ == 0) {
      valid_ = false;
      return *this;
    }
    if (auto foster = ref->GetFoster(*txn_)) {
      const FosterPair& foster_pair = foster.Value();
      pid_ = foster_pair.child_pid;
      PageRef next_ref = txn_->PageManager()->GetPage(pid_);
      ref.PageUnlock();
      idx_ = 0;
      if (next_ref->body.leaf_page.row_count_ == 0 ||
          (!end_.empty() && end_ < next_ref->body.leaf_page.GetKey(idx_))) {
        valid_ = false;
      }

      return *this;
    }
    IndexKey high_fence = ref->GetHighFence(*txn_);
    if (high_fence.IsPlusInfinity()) {
      valid_ = false;
      return *this;
    }
    PageRef next_ref =
        tree_->FindLeaf(*txn_, high_fence.GetKey().Value(), false);
    ref.PageUnlock();
    idx_ = 0;
    pid_ = next_ref->PageID();
    if (next_ref->body.leaf_page.row_count_ == 0 ||
        (!end_.empty() && end_ < next_ref->body.leaf_page.GetKey(idx_))) {
      valid_ = false;
    }
    return *this;
  }
  if (!end_.empty() && end_ < lp->GetKey(idx_)) {
    valid_ = false;
  }
  return *this;
}

BPlusTreeIterator& BPlusTreeIterator::operator--() {
  PageRef ref = txn_->PageManager()->GetPage(pid_);
  LeafPage* const lp = &ref->body.leaf_page;
  if (0 == idx_) {
    if (pid_ == 0) {
      valid_ = false;
    }
    IndexKey low_fence = ref->GetLowFence(*txn_);
    if (low_fence.IsMinusInfinity()) {
      valid_ = false;
      return *this;
    }
    PageRef prev_ref = tree_->FindLeaf(*txn_, low_fence.GetKey().Value(), true);
    idx_ = prev_ref->body.leaf_page.row_count_ - 1;
    pid_ = prev_ref->PageID();
    if (prev_ref->body.leaf_page.row_count_ == 0 ||
        (!begin_.empty() && prev_ref->body.leaf_page.GetKey(idx_) < begin_)) {
      valid_ = false;
    }
    ref.PageUnlock();
    return *this;
  }
  --idx_;
  if (!begin_.empty() && lp->GetKey(idx_) < begin_) {
    valid_ = false;
  }
  return *this;
}

}  // namespace tinylamb