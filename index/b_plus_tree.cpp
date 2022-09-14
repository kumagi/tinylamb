//
// Created by kumagi on 2022/02/01.
//
#include "b_plus_tree.hpp"

#include <vector>

#include "b_plus_tree_iterator.hpp"
#include "common/debug.hpp"
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

PageRef BPlusTree::FindLeftmostPage(Transaction& txn, PageRef&& page) {
  while (page->Type() != PageType::kLeafPage) {
    assert(page->Type() == PageType::kBranchPage);
    page = txn.PageManager()->GetPage(page->body.branch_page.lowest_page_);
  }
  return std::move(page);
}

PageRef BPlusTree::FindRightmostPage(Transaction& txn, PageRef&& page) {
  while (page->Type() != PageType::kLeafPage) {
    assert(page->Type() == PageType::kBranchPage);
    page = txn.PageManager()->GetPage(
        page->body.branch_page.GetValue(page->body.branch_page.RowCount()));
  }
  return std::move(page);
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
      // next_page->SetHighFence(txn, IndexKey(new_child.key));
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
  // right->SetLowFence(txn, IndexKey(right->GetKey(0)));
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
  Status s = curr->InsertLeaf(txn, key, value);
  if (s == Status::kNoSpace) {
    return SplitAndInsert(txn, std::move(curr), key, value);
  }
  return s;
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
  PageRef curr = txn.PageManager()->GetPage(root_);
  if (curr->RowCount() == 1 && curr->PageID() == root_ &&
      curr->Type() == PageType::kBranchPage) {
    PageRef prev_page =
        txn.PageManager()->GetPage(curr->body.branch_page.lowest_page_);
    PageRef next_page = txn.PageManager()->GetPage(curr->GetPage(0));
    if ((key < curr->GetKey(0) && prev_page->RowCount() == 1 &&
         !prev_page->GetFoster(txn)) ||
        (curr->GetKey(0) <= key && next_page->RowCount() == 1 &&
         !next_page->GetFoster(txn))) {
      if (prev_page->Type() == PageType::kLeafPage &&
          !prev_page->GetFoster(txn).HasValue()) {
        // Lift up leaf node.
        std::string old_key(curr->GetKey(0));
        curr->PageTypeChange(txn, PageType::kLeafPage);
        for (size_t i = 0; i < prev_page->RowCount(); ++i) {
          std::string_view prev_key = prev_page->GetKey(i);
          STATUS(curr->InsertLeaf(txn, prev_key,
                                  prev_page->Read(txn, prev_key).Value()),
                 "Insert");
        }
        STATUS(curr->SetFoster(txn, FosterPair(old_key, next_page->PageID())),
               "Set root leaf foster");
      } else {
        // Lift up branch node.
        assert(prev_page->Type() == PageType::kBranchPage);
        std::string old_key(curr->GetKey(0));
        if ((prev_page->RowCount() == 1 && key < old_key) ||
            (next_page->RowCount() == 1 && old_key <= key)) {
          STATUS(curr->Delete(txn, next_page->GetKey(0)), "Delete all");
          curr->SetLowestValue(txn, prev_page->body.branch_page.lowest_page_);
          for (slot_t i = 0; i < prev_page->RowCount(); ++i) {
            STATUS(curr->InsertBranch(txn, prev_page->GetKey(i),
                                      prev_page->GetPage(i)),
                   "Insert root");
          }
          STATUS(curr->SetFoster(txn, FosterPair(old_key, next_page->PageID())),
                 "Root foster");
        }
      }
      if (!curr->GetFoster(txn).HasValue()) {
        if (auto foster = next_page->GetFoster(txn)) {
          STATUS(curr->SetFoster(txn, foster.Value()), "Set foster");
          STATUS(next_page->SetFoster(txn, FosterPair()), "Delete foster");
        }
        while (0 < next_page->RowCount()) {
          next_page->Delete(txn, next_page->GetKey(0));
        }
      }
    }
  }
  while (curr->Type() != PageType::kLeafPage) {
    if (auto curr_foster = curr->GetFoster(txn)) {
      const FosterPair& foster = curr_foster.Value();
      PageRef child_page = txn.PageManager()->GetPage(foster.child_pid);
      assert(child_page->Type() == PageType::kBranchPage);
      if (curr->RowCount() == 1 && child_page->RowCount() == 1) {
        // Merge
        Status s = curr->InsertBranch(
            txn, foster.key, child_page->body.branch_page.lowest_page_);
        STATUS(s, "Merge success1");
        s = curr->InsertBranch(txn, child_page->GetKey(0),
                               child_page->GetPage(0));
        STATUS(s, "Merge success2");
        s = curr->SetFoster(txn, FosterPair());
        STATUS(s, "Foster delete");
      } else if (foster.key <= key && child_page->RowCount() == 1) {
        // Re-balance left to right.
        assert(curr->Type() == PageType::kBranchPage);
        curr->body.branch_page.MoveRightToFoster(txn, *child_page);
        assert(child_page->RowCount() == 2);
        assert(1 <= curr->RowCount());
      } else if (key < foster.key && curr->RowCount() == 1) {
        // Re-balance right to left.
        assert(curr->Type() == PageType::kBranchPage);
        curr->body.branch_page.MoveLeftFromFoster(txn, *child_page);
        assert(curr->RowCount() == 2);
        assert(1 <= child_page->RowCount());
      }
      auto latest_foster = curr->GetFoster(txn);
      if (latest_foster.HasValue()) {
        const FosterPair& fp = latest_foster.Value();
        if (fp.key <= key) {
          curr = std::move(child_page);
        }
      }
    }
    assert(curr->Type() == PageType::kBranchPage);
    int next_idx = curr->body.branch_page.Search(key);
    PageRef next_page =
        next_idx < 0
            ? txn.PageManager()->GetPage(curr->body.branch_page.lowest_page_)
            : txn.PageManager()->GetPage(
                  curr->body.branch_page.GetValue(next_idx));
    if (next_page->RowCount() == 1 && !next_page->GetFoster(txn)) {
      // To avoid empty node, make the node have sibling.
      if (next_idx < curr->RowCount() - 1) {
        // Make right sibling a foster child and rebalance.
        std::string_view next_key = curr->body.branch_page.GetKey(next_idx + 1);
        PageRef new_foster = txn.PageManager()->GetPage(
            curr->body.branch_page.GetValue(next_idx + 1));
        RETURN_IF_FAIL(next_page->SetFoster(
            txn, FosterPair(next_key, new_foster->PageID())));
        RETURN_IF_FAIL(curr->Delete(txn, next_key));
      } else {
        // Make this foster child of left sibling.
        PageRef new_foster_parent = txn.PageManager()->GetPage(
            curr->body.branch_page.GetValue(next_idx - 1));
        std::string_view next_key = curr->GetKey(curr->RowCount() - 1);
        RETURN_IF_FAIL(new_foster_parent->SetFoster(
            txn, FosterPair(next_key, next_page->PageID())));
        RETURN_IF_FAIL(curr->Delete(txn, next_key));
        next_page = std::move(new_foster_parent);
      }
    }
    curr = std::move(next_page);  // Releases parent lock here.
  }
  // LOG(WARN) << txn.PageManager()->GetPage(root_);
  assert(curr->Type() == PageType::kLeafPage);
  if (auto maybe_foster = curr->GetFoster(txn)) {
    const FosterPair& foster = maybe_foster.Value();
    PageRef right = txn.PageManager()->GetPage(foster.child_pid);
    if (curr->RowCount() == 1 && key < foster.key) {
      if (right->RowCount() == 1) {
        // Merge Leaf.
        curr->InsertLeaf(txn, right->GetKey(0),
                         right->body.leaf_page.GetValue(0));
        if (auto right_foster = right->GetFoster(txn)) {
          const FosterPair& right_fp = right_foster.Value();
          PageRef right_child = txn.PageManager()->GetPage(right_fp.child_pid);
          STATUS(curr->SetFoster(txn, right_fp), "Pull up foster");
        } else {
          STATUS(curr->SetFoster(txn, FosterPair()), "Delete foster");
        }
        // txn.PageManager()->DestroyPage(txn, &*right);
      } else {
        curr->body.leaf_page.MoveLeftFromFoster(txn, *right);
      }
    } else if (right->RowCount() == 1 && foster.key <= key) {
      if (curr->RowCount() == 1) {
        // Merge leaf
        curr->InsertLeaf(txn, right->GetKey(0),
                         right->body.leaf_page.GetValue(0));
        if (auto right_foster = right->GetFoster(txn)) {
          const FosterPair& right_fp = right_foster.Value();
          PageRef right_child = txn.PageManager()->GetPage(right_fp.child_pid);
          STATUS(curr->SetFoster(txn, right_fp), "Pull up foster");
        } else {
          STATUS(curr->SetFoster(txn, FosterPair()), "Delete foster");
        }
        // txn.PageManager()->DestroyPage(txn, &*right);
      } else {
        curr->body.leaf_page.MoveRightToFoster(txn, *right);
        curr = std::move(right);
      }
    }
  }
  RETURN_IF_FAIL(curr->Delete(txn, key));
  return Status::kSuccess;
}

StatusOr<std::string_view> BPlusTree::Read(Transaction& txn,
                                           std::string_view key) {
  PageRef curr = txn.PageManager()->GetPage(root_);
  while (curr->Type() == PageType::kBranchPage) {
    if (auto maybe_foster = curr->GetFoster(txn)) {
      const FosterPair& foster_child = maybe_foster.Value();
      if (foster_child.key <= key) {
        curr = txn.PageManager()->GetPage(foster_child.child_pid);
        continue;
      }
    }
    ASSIGN_OR_CRASH(page_id_t, maybe_next, curr->GetPageForKey(txn, key));
    curr = txn.PageManager()->GetPage(maybe_next);
  }
  assert(curr->Type() == PageType::kLeafPage);
  while (auto maybe_foster = curr->GetFoster(txn)) {
    const FosterPair& foster_child = maybe_foster.Value();
    if (foster_child.key <= key) {
      curr = txn.PageManager()->GetPage(foster_child.child_pid);
      continue;
    }
    break;
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
    o << Indent(indent) << " | F[" << foster.child_pid
      << "]: " << OmittedString(foster.key, 80) << " from [" << page->PageID()
      << "]\n";
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
    if (page->RowCount() == 0) {
      o << Indent(indent) << "(No Slot for " << page->PageID() << ")\n";
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
      o << Indent(indent) << " | branch foster["
        << OmittedString(foster.key, 80) << "]\n";
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