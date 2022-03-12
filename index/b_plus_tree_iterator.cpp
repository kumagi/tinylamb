//
// Created by kumagi on 2022/02/09.
//

#include "b_plus_tree_iterator.hpp"

#include "b_plus_tree.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"

namespace tinylamb {

BPlusTreeIterator::BPlusTreeIterator(BPlusTree* tree, Transaction* txn,
                                     std::basic_string_view<char> begin,
                                     std::basic_string_view<char> end,
                                     bool ascending)
    : tree_(tree), txn_(txn), begin_(begin), end_(end) {
  if (!end.empty() && !begin.empty() && end < begin) {
    std::runtime_error("invalid begin & end");
  }
  if (ascending) {
    if (begin.empty()) {
      PageRef leaf = tree->LeftmostPage(*txn_);
      pid_ = leaf->PageID();
      idx_ = 0;
    } else {
      PageRef leaf =
          tree_->FindLeaf(*txn_, begin, tree_->pm_->GetPage(tree_->root_));
      pid_ = leaf->PageID();
      idx_ = leaf->body.leaf_page.Find(begin);
    }
  } else {
    if (end.empty()) {
      PageRef leaf = tree->RightmostPage(*txn_);
      pid_ = leaf->PageID();
      idx_ = leaf->body.leaf_page.row_count_ - 1;
    } else {
      PageRef leaf =
          tree_->FindLeaf(*txn_, end, tree_->pm_->GetPage(tree_->root_));
      pid_ = leaf->PageID();
      idx_ = leaf->body.leaf_page.Find(end);
    }
  }
  valid_ = true;
}

std::string_view BPlusTreeIterator::operator*() {
  return tree_->pm_->GetPage(pid_)->body.leaf_page.GetValue(idx_);
}

BPlusTreeIterator& BPlusTreeIterator::operator++() {
  PageRef ref = tree_->pm_->GetPage(pid_);
  LeafPage* const lp = &ref->body.leaf_page;
  idx_++;
  if (lp->row_count_ <= idx_) {
    pid_ = lp->next_pid_;
    if (pid_ == 0) {
      valid_ = false;
      return *this;
    }
    PageRef next_ref = tree_->pm_->GetPage(pid_);
    ref.PageUnlock();
    idx_ = 0;
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
  PageRef ref = tree_->pm_->GetPage(pid_);
  LeafPage* const lp = &ref->body.leaf_page;
  if (0 == idx_) {
    pid_ = lp->prev_pid_;
    if (pid_ == 0) valid_ = false;
    PageRef prev_ref = tree_->pm_->GetPage(pid_);
    ref.PageUnlock();
    idx_ = prev_ref->body.leaf_page.row_count_ - 1;
    if (prev_ref->body.leaf_page.row_count_ == 0 ||
        (!begin_.empty() && prev_ref->body.leaf_page.GetKey(idx_) < begin_)) {
      valid_ = false;
    }
    return *this;
  } else {
    --idx_;
  }
  if (!begin_.empty() && lp->GetKey(idx_) < begin_) valid_ = false;
  return *this;
}

}  // namespace tinylamb