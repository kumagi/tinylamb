//
// Created by kumagi on 2022/02/01.
//
#include "b_plus_tree.hpp"

#include <vector>

#include "b_plus_tree_iterator.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

BPlusTree::BPlusTree(Transaction& txn, page_id_t default_root)
    : root_(default_root) {
  PageRef root = txn.PageManager()->GetPage(default_root);
  if (!root.IsValid()) {
    PageRef new_root =
        txn.PageManager()->AllocateNewPage(txn, PageType::kLeafPage);
    root_ = new_root->PageID();
  }
}

BPlusTree::BPlusTree(page_id_t given_root) : root_(given_root) {}

PageRef BPlusTree::FindLeafForInsert(Transaction& txn, std::string_view key,
                                     PageRef&& page) {
  if (page->Type() == PageType::kLeafPage) {
    return std::move(page);
  }
  assert(page->Type() == PageType::kBranchPage);
  assert(0 < page->RowCount());
  ASSIGN_OR_CRASH(page_id_t, next, page->GetPageForKey(txn, key));
  PageRef next_ref = txn.PageManager()->GetPage(next);
  return FindLeafForInsert(txn, key, std::move(next_ref));
}

PageRef BPlusTree::FindLeaf(Transaction& txn, std::string_view key,
                            PageRef&& root) {}

PageRef BPlusTree::FindLeftmostPage(Transaction& txn, PageRef&& root) {
  if (root->Type() == PageType::kLeafPage) {
    return std::move(root);
  }
  return FindLeftmostPage(
      txn, txn.PageManager()->GetPage(root->body.branch_page.lowest_page_));
}

PageRef BPlusTree::FindRightmostPage(Transaction& txn, PageRef&& root) {
  if (root->Type() == PageType::kLeafPage) {
    return std::move(root);
  }
  return FindRightmostPage(
      txn, txn.PageManager()->GetPage(root->body.branch_page.GetValue(
               root->body.branch_page.RowCount() - 1)));
}

PageRef BPlusTree::LeftmostPage(Transaction& txn) {
  return FindLeftmostPage(txn, txn.PageManager()->GetPage(root_));
}

PageRef BPlusTree::RightmostPage(Transaction& txn) {
  return FindRightmostPage(txn, txn.PageManager()->GetPage(root_));
}

void BPlusTree::GrowTreeHeightIfNeeded(Transaction& txn) const {
  PageRef root = txn.PageManager()->GetPage(root_);
  StatusOr<FosterPair> root_foster = root->GetFoster(txn);
  if (!root_foster.HasValue()) {
    return;
  }
  const FosterPair& new_right = root_foster.Value();
  if (root->Type() == PageType::kBranchPage) {
    const BranchPage& root_page = root->body.branch_page;
    PageRef new_left =
        txn.PageManager()->AllocateNewPage(txn, PageType::kBranchPage);
    new_left->SetLowestValue(txn, root_page.lowest_page_);
    for (slot_t i = 0; i < root->RowCount(); ++i) {
      new_left->InsertBranch(txn, root_page.GetKey(i), root_page.GetValue(i));
    }
    Status s = root->SetFoster(txn, FosterPair("", 0));
    assert(s == Status::kSuccess);
    while (0 < root_page.RowCount()) {
      s = root->Delete(txn, root_page.GetKey(0));
      STATUS(s, "Delete must success");
    }
    root->SetLowestValue(txn, new_left->PageID());
    root->InsertBranch(txn, new_right.key, new_right.child_pid);
    return;
  }
  assert(root->Type() == PageType::kLeafPage);
  // Swap parent node to the branch node.
  PageRef new_left =
      txn.PageManager()->AllocateNewPage(txn, PageType::kLeafPage);
  const LeafPage& root_page = root->body.leaf_page;
  for (slot_t i = 0; i < root->body.leaf_page.RowCount(); ++i) {
    new_left->InsertLeaf(txn, root_page.GetKey(i), root_page.GetValue(i));
  }
  PageRef right_page = txn.PageManager()->GetPage(new_right.child_pid);
  root->PageTypeChange(txn, PageType::kBranchPage);
  root->SetLowestValue(txn, new_left->PageID());
  root->InsertBranch(txn, new_right.key, new_right.child_pid);

  // TODO(kumagi): fix low/high fences.
  Status s = root->SetFoster(txn, FosterPair("", 0));
  assert(s == Status::kSuccess);
}

PageRef BPlusTree::FindLeaf(Transaction& txn, std::string_view key) {
  GrowTreeHeightIfNeeded(txn);
  PageRef curr = txn.PageManager()->GetPage(root_);
  assert(curr->GetFoster(txn).GetStatus() == Status::kNotExists);
  while (curr->Type() != PageType::kLeafPage) {
    ASSIGN_OR_CRASH(page_id_t, next, curr->GetPageForKey(txn, key));
    PageRef next_page = txn.PageManager()->GetPage(next);
    if (auto next_foster = next_page->GetFoster(txn)) {
      const FosterPair& new_child = next_foster.Value();
      Status s = curr->InsertBranch(txn, new_child.key, new_child.child_pid);
      if (s != Status::kSuccess) {
        PageRef right =
            txn.PageManager()->AllocateNewPage(txn, PageType::kBranchPage);
        std::string middle;
        curr->body.branch_page.Split(curr->PageID(), txn, key, &*right,
                                     &middle);
        s = curr->SetFoster(txn, FosterPair(middle, right->PageID()));
        assert(s == Status::kSuccess);
        if (new_child.key < middle) {
          s = curr->InsertBranch(txn, new_child.key, new_child.child_pid);
          STATUS(s, "left insert");
        } else {
          s = right->InsertBranch(txn, new_child.key, new_child.child_pid);
          STATUS(s, "right insert");
        }
        if (middle <= key) {
          curr = std::move(right);
        }
      }
      s = next_page->SetFoster(txn, FosterPair("", 0));
      assert(s == Status::kSuccess);
      if (new_child.key <= key) {
        next_page.PageUnlock();
        next_page =
            txn.PageManager()->GetPage(curr->GetPageForKey(txn, key).Value());
      }
    }
    curr = std::move(next_page);  // Releases parent lock here.
  }
  return curr;
}

Status BPlusTree::SplitAndInsert(Transaction& txn, PageRef&& leaf,
                                 std::string_view key, std::string_view value) {
  PageRef right = txn.PageManager()->AllocateNewPage(txn, PageType::kLeafPage);
  leaf->body.leaf_page.Split(leaf->PageID(), txn, key, value, right.get());
  RETURN_IF_FAIL(
      leaf->SetFoster(txn, FosterPair(right->GetKey(0), right->PageID())));
  if (key < right->GetKey(0)) {
    return leaf->InsertLeaf(txn, key, value);
  }
  return right->InsertLeaf(txn, key, value);
}

Status BPlusTree::Insert(Transaction& txn, std::string_view key,
                         std::string_view value) {
  PageRef curr = FindLeaf(txn, key);
  assert(curr->Type() == PageType::kLeafPage);
  assert(curr->GetFoster(txn).GetStatus() == Status::kNotExists);
  if (curr->InsertLeaf(txn, key, value) == Status::kNoSpace) {
    return SplitAndInsert(txn, std::move(curr), key, value);
  }
  return Status::kSuccess;
}

Status BPlusTree::Update(Transaction& txn, std::string_view key,
                         std::string_view value) {
  PageRef curr = FindLeaf(txn, key);
  Status s = curr->Update(txn, key, value);
  if (s == Status::kNoSpace) {
    s = curr->Delete(txn, key);
    STATUS(s, Status::kSuccess);
    return SplitAndInsert(txn, std::move(curr), key, value);
  }
  return s;
}

Status BPlusTree::Delete(Transaction& txn, std::string_view key) {
  PageRef curr = FindLeaf(txn, key);
  Status s = curr->Delete(txn, key);
  if (s != Status::kSuccess) {
    return s;
  }
  /*
  while (ref->RowCount() == 0 && !parents.empty()) {
    PageRef parent = std::move(parents.back());
    if (ref->Type() == PageType::kLeafPage) {
      // TODO(kumagi): Steal values from right leaf.d
      parent->Delete(txn, key);
    } else if (ref->Type() == PageType::kBranchPage) {
      page_id_t orphan = ref->body.branch_page.lowest_page_;
      const BranchPage& bp = parent->body.branch_page;
      if (bp.lowest_page_ == ref->PageID()) {
        // If this node is the leftmost child of the parent,
        // Steal left most entry from right sibling.
        std::string parent_key(parent->GetKey(0));
        PageRef right = txn.PageManager()->GetPage(bp.GetValue(0));
        std::string right_leftmost_key(right->GetKey(0));
        parent->Delete(txn, right_leftmost_key);

        // Stealing leftmost key&value from right sibling.
        page_id_t prev_lowest = right->body.branch_page.lowest_page_;
        page_id_t next_lowest = right->body.branch_page.GetValue(0);
        s = right->Delete(txn, right_leftmost_key);
        right->SetLowestValue(txn, next_lowest);
        ref->InsertBranch(txn, parent_key, prev_lowest);
        if (0 < right->RowCount()) {
          // Steal
          parent->InsertBranch(txn, right_leftmost_key, right->PageID());
        } else {
          // Merge
          ref->InsertBranch(txn, right_leftmost_key, next_lowest);
        }
      } else {
        // In other case, steal right most entry from left sibling.
        int idx = bp.Search(key);
        assert(0 <= idx);
        std::string parent_key(bp.GetKey(idx));  // needs deep copy.
        const page_id_t left_sibling = [&]() {
          if (idx == 0) {
            return bp.lowest_page_;
          }
          return bp.GetValue(idx - 1);
        }();
        PageRef left = txn.PageManager()->GetPage(left_sibling);
        const int rightmost_idx = left->RowCount() - 1;
        std::string left_rightmost_key(left->GetKey(rightmost_idx));
        size_t left_rightmost_value = left->GetPage(rightmost_idx);
        s = left->Delete(txn, left_rightmost_key);
        ref->InsertBranch(txn, parent_key, orphan);
        parent->Delete(txn, key);
        if (0 < left->RowCount()) {
          // Steal
          ref->SetLowestValue(txn, left_rightmost_value);
          parent->InsertBranch(txn, left_rightmost_key, ref->PageID());
        } else {
          // Merge
          if (idx == 0) {
            parent->SetLowestValue(txn, ref->PageID());
          } else {
            parent->UpdateBranch(txn, parent->GetKey(idx - 1), ref->PageID());
          }
          ref->SetLowestValue(txn, left->body.branch_page.lowest_page_);
          ref->InsertBranch(txn, left_rightmost_key, left_rightmost_value);
        }
        assert(s == Status::kSuccess);
      }
    }
    if (ref->RowCount() == 0 && ref->PageID() != root_) {
      txn.PageManager()->DestroyPage(txn, ref.get());
    }
    ref.PageUnlock();
    ref = std::move(parent);
  }
  if (ref->RowCount() == 0) {
    if (ref->Type() == PageType::kBranchPage) {
      ref.PageUnlock();
      // Root is empty, move entire dada into root.
      assert(ref->PageID() == root_);
      PageRef next_root =
          txn.PageManager()->GetPage(ref->body.branch_page.lowest_page_);
      if (next_root->Type() == PageType::kBranchPage) {
        const BranchPage& bp = next_root->body.branch_page;
        ref->SetLowestValue(txn, bp.lowest_page_);
        while (0 < bp.row_count_) {
          std::string moving_key(bp.GetKey(0));
          s = ref->InsertBranch(txn, moving_key, bp.GetValue(0));
          assert(s == Status::kSuccess);
          next_root->Delete(txn, moving_key);
        }
      } else if (next_root->Type() == PageType::kLeafPage) {
        ref->PageTypeChange(txn, PageType::kLeafPage);
        const LeafPage& lp = next_root->body.leaf_page;
        while (0 < lp.row_count_) {
          std::string moving_key(lp.GetKey(0));
          ref->InsertLeaf(txn, moving_key, lp.GetValue(0));
          next_root->Delete(txn, moving_key);
        }
      }
      next_root.PageUnlock();
      txn.PageManager()->DestroyPage(txn, next_root.get());
    }
  }
   */
  return s;
}

StatusOr<std::string_view> BPlusTree::Read(Transaction& txn,
                                           std::string_view key) {
  PageRef curr = txn.PageManager()->GetPage(root_);
  while (curr->Type() == PageType::kBranchPage) {
    ASSIGN_OR_RETURN(page_id_t, next, curr->GetPageForKey(txn, key));
    PageRef next_page = txn.PageManager()->GetPage(next);
    if (auto maybe_foster = next_page->GetFoster(txn)) {
      const FosterPair& foster_child = maybe_foster.Value();
      if (foster_child.key <= key) {
        next_page = txn.PageManager()->GetPage(foster_child.child_pid);
      }
    }
    curr = std::move(next_page);  // Releases parent lock here.
  }
  assert(curr->Type() == PageType::kLeafPage);
  if (auto maybe_foster = curr->GetFoster(txn)) {
    const FosterPair& foster_child = maybe_foster.Value();
    if (foster_child.key <= key) {
      PageRef foster_page = txn.PageManager()->GetPage(foster_child.child_pid);
      return foster_page->Read(txn, key);
    }
  }
  return curr->Read(txn, key);
}

BPlusTreeIterator BPlusTree::Begin(Transaction& txn, std::string_view left,
                                   std::string_view right, bool ascending) {
  return {this, &txn, left, right, ascending};
}

bool BPlusTree::SanityCheckForTest(PageManager* pm) const {
  PageRef page = pm->GetPage(root_);
  if (page->Type() == PageType::kLeafPage) {
    return page->body.leaf_page.SanityCheckForTest();
  }
  if (page->Type() == PageType::kBranchPage) {
    return page->body.branch_page.SanityCheckForTest(pm);
  }
  return false;
}

namespace {

std::string OmittedString(std::string_view original, int length) {
  if ((size_t)length < original.length()) {
    std::string omitted_key = std::string(original).substr(0, 8);
    omitted_key +=
        "..(" + std::to_string(original.length() - length + 4) + "bytes)..";
    omitted_key += original.substr(original.length() - 8);
    return omitted_key;
  }
  return std::string(original);
}

void DumpLeafPage(Transaction& txn, PageRef&& page, std::ostream& o,
                  int indent) {
  o << "L[" << page->PageID() << "]: ";
  indent += 2 + std::to_string(page->PageID()).size() + 3;
  for (slot_t i = 0; i < page->RowCount(); ++i) {
    if (0 < i) {
      o << Indent(indent);
    }
    ASSIGN_OR_CRASH(std::string_view, key, page->ReadKey(txn, i));
    ASSIGN_OR_CRASH(std::string_view, value, page->Read(txn, i));
    o << OmittedString(key, 80) << ": " << OmittedString(value, 30) << "\n";
  }
  if (auto maybe_foster = page->GetFoster(txn)) {
    const FosterPair& foster = maybe_foster.Value();
    o << Indent(indent) << " | foster[" << OmittedString(foster.key, 80)
      << "] from [" << page->PageID() << "]\n";
    PageRef child = txn.PageManager()->GetPage(foster.child_pid);
    o << Indent(indent + 1);
    DumpLeafPage(txn, std::move(child), o, indent + 1);
  }
}
}  // namespace

void BPlusTree::DumpBranch(Transaction& txn, std::ostream& o, PageRef&& page,
                           int indent) const {
  if (page->Type() == PageType::kLeafPage) {
    o << Indent(indent);
    DumpLeafPage(txn, std::move(page), o, indent);
  } else if (page->Type() == PageType::kBranchPage) {
    DumpBranch(txn, o,
               txn.PageManager()->GetPage(page->body.branch_page.lowest_page_),
               indent + 4);
    if (page->RowCount() == 0) {
      o << Indent(indent) << "(Empty branch): " << *page << "\n";
      return;
    }
    for (slot_t i = 0; i < page->RowCount(); ++i) {
      ASSIGN_OR_CRASH(std::string_view, key, page->ReadKey(txn, i));
      o << Indent(indent) << "B[" << page->PageID()
        << "]: " << OmittedString(key, 20) << "\n";
      ASSIGN_OR_CRASH(page_id_t, pid, page->GetPageForKey(txn, key));
      DumpBranch(txn, o, txn.PageManager()->GetPage(pid), indent + 4);
    }
    if (auto maybe_foster = page->GetFoster(txn)) {
      const FosterPair& foster = maybe_foster.Value();
      o << Indent(indent) << "| branch foster[" << OmittedString(foster.key, 80)
        << "]\n";
      PageRef child = txn.PageManager()->GetPage(foster.child_pid);
      o << Indent(indent + 1);
      DumpBranch(txn, o, std::move(child), indent + 1);
    }
  } else {
    LOG(ERROR) << "Page: " << *page << " : Invalid page type";
    abort();
  }
}

void BPlusTree::Dump(Transaction& txn, std::ostream& o, int indent) const {
  PageRef root_page = txn.PageManager()->GetPage(root_);
  if (root_page->Type() == PageType::kLeafPage) {
    DumpLeafPage(txn, std::move(root_page), o, indent);
  } else {
    DumpBranch(txn, o, std::move(root_page), indent);
  }
  o << "\n";
}

}  // namespace tinylamb