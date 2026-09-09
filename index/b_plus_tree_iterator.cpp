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

#include <string_view>
#include <utility>

#include "common/constants.hpp"
#include "index/b_plus_tree.hpp"
#include "page/index_key.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

BPlusTreeIterator::BPlusTreeIterator(BPlusTree* tree, Transaction* txn,
                                     std::string_view begin,
                                     std::string_view end, bool ascending)
    : tree_(tree), txn_(txn), begin_(begin), end_(end) {
  CHECK_MSG(end.empty() || begin.empty() || !(end < begin),
            "invalid begin & end");
  const Status init = [&]() -> Status {
    if (ascending) {
      if (begin.empty()) {
        // Land on the first leaf holding any key.  Foster links cover a
        // deleted-and-emptied leftmost leaf whose split was already absorbed
        // into the parent; when no foster remains, cross the high fence with
        // the same walk PositionAtOrAbove uses (an empty begin matches every
        // key, so a non-empty tree must never scan as empty).
        ASSIGN_OR_RETURN(PageRef, leaf, tree->LeftmostPage(*txn_));
        for (;;) {
          if (0 < leaf->body.leaf_page.row_count_) {
            break;
          }
          if (auto foster = leaf->GetFoster(*txn_)) {
            ASSIGN_OR_RETURN(PageRef, child,
                             txn_->GetPageManager()->GetPage(
                                 foster.Value().child_pid, true));
            leaf.PageUnlock();
            leaf = std::move(child);
            continue;
          }
          const IndexKey high_fence = leaf->GetHighFence(*txn_);
          if (high_fence.IsPlusInfinity()) {
            break;  // Truly empty tree.
          }
          const std::string seek(high_fence.GetKey().Value());
          leaf.PageUnlock();
          ASSIGN_OR_RETURN(PageRef, next_leaf,
                           tree_->FindLeafReadOnly(*txn_, seek, false));
          leaf = std::move(next_leaf);
        }
        pid_ = leaf->PageID();
        idx_ = 0;
        valid_ = leaf->body.leaf_page.row_count_ > 0;
        if (valid_ && !end.empty() && end < leaf->body.leaf_page.GetKey(idx_)) {
          valid_ = false;
        }
      } else {
        // Construction never mutates: read-only lookups keep RO transactions
        // on shared latches and avoid GrowTreeHeightIfNeeded/foster absorption.
        ASSIGN_OR_RETURN(PageRef, leaf,
                         tree_->FindLeafReadOnly(*txn_, begin, false));
        // A prefix seek key (e.g. a point range over the leading columns of a
        // composite key) can exhaust the leaf the descent lands on: the
        // separator that routed the descent left may itself start with the
        // seek key, so its extensions continue in later leaves. Walk forward
        // instead of reporting an empty range.
        ASSIGN_OR_RETURN(bool, positioned,
                         tree_->PositionAtOrAbove(leaf, idx_, *txn_, begin));
        valid_ = positioned;
        pid_ = valid_ ? leaf->PageID() : 0;
        if (valid_ && !end.empty() && end < leaf->body.leaf_page.GetKey(idx_)) {
          valid_ = false;
        }
      }
    } else {
      if (end.empty()) {
        ASSIGN_OR_RETURN(PageRef, leaf, tree->RightmostPage(*txn_));
        pid_ = leaf->PageID();
        idx_ = leaf->body.leaf_page.row_count_ == 0
                   ? 0
                   : leaf->body.leaf_page.row_count_ - 1;
        valid_ = leaf->body.leaf_page.row_count_ > 0;
        // Mirror the other construction branches: validate the OPPOSITE bound
        // here too, or a DESC scan with only a lower bound emits the maximum
        // key even when every key is below it.
        if (valid_ && !begin.empty() &&
            leaf->body.leaf_page.GetKey(idx_) < begin) {
          valid_ = false;
        }
      } else {
        // Find() is only a lower_bound: when `end` is absent we must not start
        // on a key above `end`, nor report an empty scan when keys below `end`
        // exist. PositionBelow lands on the last key < `end` instead.
        ASSIGN_OR_RETURN(PageRef, leaf,
                         tree_->FindLeafReadOnly(*txn_, end, false));
        pid_ = leaf->PageID();
        ASSIGN_OR_RETURN(bool, positioned,
                         tree_->PositionBelow(leaf, idx_, *txn_, end));
        valid_ = positioned;
        if (valid_) {
          pid_ = leaf->PageID();
          if (!begin_.empty() && leaf->body.leaf_page.GetKey(idx_) < begin_) {
            valid_ = false;
          }
        }
      }
    }
    return Status::kSuccess;
  }();
  if (init != Status::kSuccess) {
    status_ = init;
    valid_ = false;
  }
}

std::string BPlusTreeIterator::Key() const {
  StatusOr<PageRef> ref = txn_->GetPageManager()->GetPage(pid_, true);
  if (!ref.HasValue()) {
    status_ = ref.GetStatus();
    return {};
  }
  return std::string(ref.Value()->body.leaf_page.GetKey(idx_));
}

std::string BPlusTreeIterator::Value() const {
  StatusOr<PageRef> ref = txn_->GetPageManager()->GetPage(pid_, true);
  if (!ref.HasValue()) {
    status_ = ref.GetStatus();
    return {};
  }
  return std::string(ref.Value()->body.leaf_page.GetValue(idx_));
}

BPlusTreeIterator& BPlusTreeIterator::operator++() {
  if (!valid_) {
    // Advancing past the end must stay a no-op instead of touching pages
    // through a stale pid/index pair.
    return *this;
  }
  StatusOr<PageRef> ref_so = txn_->GetPageManager()->GetPage(pid_, true);
  if (!ref_so.HasValue()) {
    status_ = ref_so.GetStatus();
    valid_ = false;
    return *this;
  }
  PageRef ref = ref_so.MoveValue();
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
      StatusOr<PageRef> next_so = txn_->GetPageManager()->GetPage(pid_, true);
      if (!next_so.HasValue()) {
        status_ = next_so.GetStatus();
        valid_ = false;
        return *this;
      }
      PageRef next_ref = next_so.MoveValue();
      ref.PageUnlock();
      // Empty foster-tail leaves are reachable after deletions (the deleter
      // only refeeds its own chain).  Skip past them instead of ending the
      // scan early, mirroring the operator-- handling.
      while (next_ref->body.leaf_page.row_count_ == 0) {
        if (auto nested = next_ref->GetFoster(*txn_)) {
          pid_ = nested.Value().child_pid;
          StatusOr<PageRef> following_so =
              txn_->GetPageManager()->GetPage(pid_, true);
          if (!following_so.HasValue()) {
            status_ = following_so.GetStatus();
            valid_ = false;
            return *this;
          }
          PageRef following = following_so.MoveValue();
          next_ref.PageUnlock();
          next_ref = std::move(following);
          continue;
        }
        valid_ = false;
        return *this;
      }
      idx_ = 0;
      if (!end_.empty() && end_ < next_ref->body.leaf_page.GetKey(idx_)) {
        valid_ = false;
      }

      return *this;
    }
    IndexKey high_fence = ref->GetHighFence(*txn_);
    if (high_fence.IsPlusInfinity()) {
      valid_ = false;
      return *this;
    }
    ref.PageUnlock();
    // Crossing the fence can land on a leaf emptied by deletions (the
    // deleter only refeeds its own foster chain, and branch rebalance never
    // merges a zero-row child).  Walk the same foster/fence chain the
    // constructor's begin path uses: an in-range key may sit beyond any
    // number of empty leaves, so a single landing is not conclusive.
    StatusOr<PageRef> next_so =
        tree_->FindLeafReadOnly(*txn_, high_fence.GetKey().Value(), false);
    if (!next_so.HasValue()) {
      status_ = next_so.GetStatus();
      valid_ = false;
      return *this;
    }
    PageRef next_ref = next_so.MoveValue();
    for (;;) {
      if (next_ref->body.leaf_page.row_count_ != 0) {
        break;
      }
      if (auto foster = next_ref->GetFoster(*txn_)) {
        pid_ = foster.Value().child_pid;
        StatusOr<PageRef> child_so =
            txn_->GetPageManager()->GetPage(pid_, true);
        if (!child_so.HasValue()) {
          status_ = child_so.GetStatus();
          valid_ = false;
          return *this;
        }
        PageRef child = child_so.MoveValue();
        next_ref.PageUnlock();
        next_ref = std::move(child);
        continue;
      }
      const IndexKey nested_fence = next_ref->GetHighFence(*txn_);
      if (nested_fence.IsPlusInfinity()) {
        break;
      }
      const std::string nested_seek(nested_fence.GetKey().Value());
      next_ref.PageUnlock();
      StatusOr<PageRef> nested_so =
          tree_->FindLeafReadOnly(*txn_, nested_seek, false);
      if (!nested_so.HasValue()) {
        status_ = nested_so.GetStatus();
        valid_ = false;
        return *this;
      }
      next_ref = nested_so.MoveValue();
    }
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
  if (!valid_) {
    // Moving back before the begin must stay a no-op like operator++.
    return *this;
  }
  StatusOr<PageRef> ref_so = txn_->GetPageManager()->GetPage(pid_, true);
  if (!ref_so.HasValue()) {
    status_ = ref_so.GetStatus();
    valid_ = false;
    return *this;
  }
  PageRef ref = ref_so.MoveValue();
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
    const page_id_t left_from = pid_;
    const std::string seek_key(low_fence.GetKey().Value());
    ref.PageUnlock();
    // Inclusive seek stops before re-entering the page we left (foster parent).
    StatusOr<PageRef> prev_so =
        tree_->FindLeafReadOnly(*txn_, seek_key, false, left_from);
    if (!prev_so.HasValue()) {
      status_ = prev_so.GetStatus();
      valid_ = false;
      return *this;
    }
    PageRef prev_ref = prev_so.MoveValue();
    if (prev_ref->PageID() == left_from) {
      // We were the chain head: less_than yields the true left sibling.
      prev_ref.PageUnlock();
      StatusOr<PageRef> probe_so =
          tree_->FindLeafReadOnly(*txn_, seek_key, true, left_from);
      if (!probe_so.HasValue()) {
        status_ = probe_so.GetStatus();
        valid_ = false;
        return *this;
      }
      prev_ref = probe_so.MoveValue();
      while (auto foster = prev_ref->GetFoster(*txn_)) {
        if (foster.Value().child_pid == left_from) {
          break;
        }
        StatusOr<PageRef> child_so =
            txn_->GetPageManager()->GetPage(foster.Value().child_pid, true);
        if (!child_so.HasValue()) {
          status_ = child_so.GetStatus();
          valid_ = false;
          return *this;
        }
        PageRef child = child_so.MoveValue();
        prev_ref.PageUnlock();
        prev_ref = std::move(child);
      }
    }
    // Empty foster-tail leaves are reachable after deletions (the deleter
    // only refeeds its own chain, leaving the predecessor's foster link).
    // Skip past them instead of ending the scan early.
    while (prev_ref->PageID() != left_from &&
           prev_ref->body.leaf_page.row_count_ == 0) {
      IndexKey prev_low = prev_ref->GetLowFence(*txn_);
      if (prev_low.IsMinusInfinity()) {
        break;
      }
      const page_id_t stop = prev_ref->PageID();
      const std::string prev_seek(prev_low.GetKey().Value());
      prev_ref.PageUnlock();
      StatusOr<PageRef> hop_so =
          tree_->FindLeafReadOnly(*txn_, prev_seek, true, stop);
      if (!hop_so.HasValue()) {
        status_ = hop_so.GetStatus();
        valid_ = false;
        return *this;
      }
      prev_ref = hop_so.MoveValue();
      if (prev_ref->PageID() == stop) {
        break;
      }
    }
    if (prev_ref->PageID() == left_from ||
        prev_ref->body.leaf_page.row_count_ == 0) {
      valid_ = false;
      return *this;
    }
    idx_ = prev_ref->body.leaf_page.row_count_ - 1;
    pid_ = prev_ref->PageID();
    if (!begin_.empty() && prev_ref->body.leaf_page.GetKey(idx_) < begin_) {
      valid_ = false;
    }
    return *this;
  }
  --idx_;
  if (!begin_.empty() && lp->GetKey(idx_) < begin_) {
    valid_ = false;
  }
  return *this;
}

}  // namespace tinylamb
