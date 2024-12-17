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

#include "b_plus_tree.hpp"

#include <cstdlib>

#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <ostream>
#include <string>
#include <utility>

#include "b_plus_tree_iterator.hpp"
#include "common/constants.hpp"
#include "common/debug.hpp"
#include "common/log_message.hpp"
#include "common/status_or.hpp"
#include "page/index_key.hpp"
#include "page/leaf_page.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "page/page_type.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {
namespace {
void DumpLeafPage(Transaction& txn, PageRef& page, std::ostream& o, int indent);
}

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
        page->body.branch_page.GetValue(page->body.branch_page.RowCount() - 1));
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
      COERCE(root->Delete(txn, root_page.GetKey(0)));
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
  COERCE(new_left->SetHighFence(txn, IndexKey(new_right.key)));
  PageRef right_page = txn.PageManager()->GetPage(new_right.child_pid);
  root->PageTypeChange(txn, PageType::kBranchPage);
  root->SetLowestValue(txn, new_left->PageID());
  root->InsertBranch(txn, new_right.key, new_right.child_pid);

  // TODO(kumagi): fix low/high fences.
  Status s = root->SetFoster(txn, FosterPair("", 0));
  assert(s == Status::kSuccess);
}

PageRef BPlusTree::FindLeaf(Transaction& txn, std::string_view key,
                            bool less_than) {
  GrowTreeHeightIfNeeded(txn);
  PageRef curr = txn.PageManager()->GetPage(root_);
  assert(curr->GetFoster(txn).GetStatus() == Status::kNotExists);
  while (curr->Type() != PageType::kLeafPage) {
    assert(curr->Type() == PageType::kBranchPage);
    ASSIGN_OR_CRASH(page_id_t, next, curr->GetPageForKey(txn, key, less_than));
    PageRef next_page = txn.PageManager()->GetPage(next);
    assert(next_page->PageID() == next);
    while (auto next_foster = next_page->GetFoster(txn)) {
      const FosterPair& new_child = next_foster.Value();
      Status s = curr->InsertBranch(txn, new_child.key, new_child.child_pid);
      if (s != Status::kSuccess) {
        PageRef right =
            txn.PageManager()->AllocateNewPage(txn, PageType::kBranchPage);
        std::string middle;
        curr->body.branch_page.Split(curr->PageID(), txn, key, &*right,
                                     &middle);
        COERCE(right->SetLowFence(txn, IndexKey(middle)));
        COERCE(right->SetHighFence(txn, next_page->GetHighFence(txn)));
        s = curr->SetFoster(txn, FosterPair(middle, right->PageID()));
        assert(s == Status::kSuccess);
        if (new_child.key < middle) {
          s = curr->InsertBranch(txn, new_child.key, new_child.child_pid);
          COERCE(s);
        } else {
          s = right->InsertBranch(txn, new_child.key, new_child.child_pid);
          COERCE(s);
        }
        if (middle <= key) {
          curr = std::move(right);
        }
      }
      s = next_page->SetFoster(txn, FosterPair("", 0));
      COERCE(next_page->SetHighFence(txn, IndexKey(new_child.key)));
      COERCE(s);
      if (new_child.key <= key) {
        next_page.PageUnlock();
        next_page = txn.PageManager()->GetPage(
            curr->GetPageForKey(txn, key, false).Value());
      }
    }
    curr = std::move(next_page);  // Releases parent lock here.
  }
  return curr;
}

Status BPlusTree::LeafInsert(Transaction& txn, PageRef& leaf,
                             std::string_view key, std::string_view value) {
  assert(leaf->Type() == PageType::kLeafPage);
  if (auto foster = leaf->GetFoster(txn)) {
    const FosterPair& right = foster.Value();
    if (right.key <= key) {
      leaf.PageUnlock();
      PageRef foster_child = txn.PageManager()->GetPage(right.child_pid);
      return LeafInsert(txn, foster_child, key, value);
    }
  }

  for (;;) {
    Status leaf_result = leaf->InsertLeaf(txn, key, value);
    if (leaf_result == Status::kTooBigData) {
      return leaf_result;
    }
    if (leaf_result == Status::kSuccess) {
      return Status::kSuccess;
    }
    PageRef right =
        txn.PageManager()->AllocateNewPage(txn, PageType::kLeafPage);
    leaf->body.leaf_page.Split(leaf->PageID(), txn, key, value, right.get());
    COERCE(right->SetLowFence(txn, IndexKey(right->GetKey(0))));
    COERCE(right->SetHighFence(txn, leaf->GetHighFence(txn)));
    if (auto curr_foster = leaf->GetFoster(txn)) {
      COERCE(right->SetFoster(txn, curr_foster.Value()));
    }
    COERCE(leaf->SetFoster(txn, FosterPair(right->GetKey(0), right->PageID())));
    if (key < right->GetKey(0)) {
      continue;
    }
    return LeafInsert(txn, right, key, value);
  }
}

Status BPlusTree::SetFosterRecursively(Transaction& txn, PageRef& parent,
                                       PageRef& new_child,
                                       std::string_view foster_key) {
  COERCE(parent->SetHighFence(txn, new_child->GetHighFence(txn)));
  while (auto foster = parent->GetFoster(txn)) {
    const FosterPair& foster_pair = foster.Value();
    parent = txn.PageManager()->GetPage(foster_pair.child_pid);
    COERCE(parent->SetHighFence(txn, new_child->GetHighFence(txn)));
  }
  assert(!parent->GetFoster(txn).HasValue());
  Status s =
      parent->SetFoster(txn, FosterPair(foster_key, new_child->PageID()));
  if (s == Status::kSuccess) {
    return Status::kSuccess;
  }
  assert(s == Status::kNoSpace);
  PageRef new_foster_child =
      txn.PageManager()->AllocateNewPage(txn, parent->Type());
  std::string middle;
  if (parent->Type() == PageType::kBranchPage) {
    parent->body.branch_page.Split(parent->PageID(), txn, foster_key,
                                   &*new_foster_child, &middle);
    COERCE(
        parent->SetFoster(txn, FosterPair(middle, new_foster_child->PageID())));
    COERCE(new_foster_child->SetLowFence(txn, IndexKey(middle)));
  } else {
    assert(parent->Type() == PageType::kLeafPage);
    parent->body.leaf_page.Split(parent->PageID(), txn, foster_key, "        ",
                                 &*new_foster_child);
    COERCE(parent->SetFoster(txn, FosterPair(new_foster_child->GetKey(0),
                                             new_foster_child->PageID())));
    COERCE(new_foster_child->SetLowFence(
        txn, IndexKey(new_foster_child->GetKey(0))));
  }

  COERCE(new_foster_child->SetHighFence(txn, parent->GetHighFence(txn)));
  COERCE(new_foster_child->SetFoster(
      txn, FosterPair(foster_key, new_child->PageID())));
  COERCE(parent->SetHighFence(txn, new_child->GetHighFence(txn)));
  return Status::kSuccess;
}

Status BPlusTree::Insert(Transaction& txn, std::string_view key,
                         std::string_view value) {
  PageRef curr = FindLeaf(txn, key, false);
  assert(curr->Type() == PageType::kLeafPage);
  assert(curr->GetFoster(txn).GetStatus() == Status::kNotExists);
  return LeafInsert(txn, curr, key, value);
}

Status BPlusTree::Update(Transaction& txn, std::string_view key,
                         std::string_view value) {
  PageRef curr = FindLeaf(txn, key, false);
  Status s = curr->Update(txn, key, value);
  if (s == Status::kNoSpace) {
    COERCE(curr->Delete(txn, key));
    return LeafInsert(txn, curr, key, value);
  }
  return s;
}

Status HandleFoster(Transaction& txn, PageRef& curr, std::string_view key) {
  if (auto curr_foster = curr->GetFoster(txn)) {
    const FosterPair& foster = curr_foster.Value();
    PageRef right_page = txn.PageManager()->GetPage(foster.child_pid);
    if (foster.key <= key) {
      if (right_page->RowCount() == 1) {
        if (curr->RowCount() == 1) {
          // Merge tree
          RETURN_IF_FAIL(curr->MoveLeftFromFoster(txn, *right_page));
          if (auto right_foster = right_page->GetFoster(txn)) {
            RETURN_IF_FAIL(curr->SetFoster(txn, right_foster.Value()));
          } else {
            RETURN_IF_FAIL(curr->SetFoster(txn, FosterPair()));
          }
        } else {
          RETURN_IF_FAIL(curr->MoveRightToFoster(txn, *right_page));
        }
      }
      return Status::kSuccess;
    }
    if (curr->RowCount() == 1) {
      COERCE(curr->MoveLeftFromFoster(txn, *right_page));
      if (right_page->RowCount() == 0) {
        if (auto right_foster = right_page->GetFoster(txn)) {
          RETURN_IF_FAIL(curr->SetFoster(txn, right_foster.Value()));
        }
      }
    }
  }
  return Status::kSuccess;
}

Status BPlusTree::Delete(Transaction& txn, std::string_view key) {
  PageRef curr = txn.PageManager()->GetPage(root_);
  STATUS(HandleFoster(txn, curr, key), "Foster operation must success");
  if (curr->RowCount() == 1 && curr->PageID() == root_ &&
      curr->Type() == PageType::kBranchPage) {
    PageRef prev_page =
        txn.PageManager()->GetPage(curr->body.branch_page.lowest_page_);
    PageRef next_page = txn.PageManager()->GetPage(curr->GetPage(0));
    if ((key < curr->GetKey(0) && prev_page->RowCount() == 1 &&
         !prev_page->GetFoster(txn)) ||
        (curr->GetKey(0) <= key && next_page->RowCount() == 1 &&
         !next_page->GetFoster(txn) && !curr->GetFoster(txn))) {
      if (prev_page->Type() == PageType::kLeafPage) {
        // Lift up leaf node.
        std::string old_key(curr->GetKey(0));
        curr->PageTypeChange(txn, PageType::kLeafPage);
        for (size_t i = 0; i < prev_page->RowCount(); ++i) {
          std::string_view prev_key = prev_page->GetKey(i);
          COERCE(curr->InsertLeaf(txn, prev_key,
                                  prev_page->Read(txn, prev_key).Value()));
        }
        if (auto foster = prev_page->GetFoster(txn)) {
          const FosterPair& foster_pair = foster.Value();
          PageRef child_page =
              txn.PageManager()->GetPage(foster_pair.child_pid);
          for (;;) {
            if (auto child_foster = child_page->GetFoster(txn)) {
              const FosterPair& grand_foster = child_foster.Value();
              child_page = txn.PageManager()->GetPage(grand_foster.child_pid);
              continue;
            }
            break;
          }
          assert(!child_page->GetFoster(txn).HasValue());
          COERCE(child_page->SetFoster(
              txn, FosterPair(next_page->GetKey(0), next_page->PageID())));
          COERCE(curr->SetFoster(txn, foster_pair));
        } else {
          COERCE(
              curr->SetFoster(txn, FosterPair(old_key, next_page->PageID())));
        }
      } else {
        // Lift up branch node.
        assert(prev_page->Type() == PageType::kBranchPage);
        std::string old_key(next_page->GetLowFence(txn).GetKey().Value());
        if ((prev_page->RowCount() == 1 && key < old_key) ||
            (next_page->RowCount() == 1 && old_key <= key)) {
          COERCE(curr->Delete(txn, next_page->GetKey(0)));
          curr->SetLowestValue(txn, prev_page->body.branch_page.lowest_page_);
          for (slot_t i = 0; i < prev_page->RowCount(); ++i) {
            COERCE(curr->InsertBranch(txn, prev_page->GetKey(i),
                                      prev_page->GetPage(i)));
          }
          page_id_t foster_root_pid = next_page->page_id;
          PageRef foster_parent = std::move(prev_page);
          std::string foster_key = old_key;

          while (auto maybe_foster = foster_parent->GetFoster(txn)) {
            const FosterPair& fp = maybe_foster.Value();
            if (foster_root_pid == next_page->page_id) {
              foster_root_pid = fp.child_pid;
              foster_key = fp.key;
            }
            foster_parent = txn.PageManager()->GetPage(fp.child_pid);
          }
          COERCE(foster_parent->SetFoster(
              txn, FosterPair(old_key, next_page->PageID())));
          COERCE(curr->SetFoster(txn, FosterPair(foster_key, foster_root_pid)));
        }
      }
      if (!curr->GetFoster(txn).HasValue()) {
        if (auto foster = next_page->GetFoster(txn)) {
          COERCE(curr->SetFoster(txn, foster.Value()));
          COERCE(next_page->SetFoster(txn, FosterPair()));
        }
        while (0 < next_page->RowCount()) {
          STATUS(next_page->Delete(txn, next_page->GetKey(0)), "Has value");
        }
      }
    }
  }
  while (curr->Type() == PageType::kBranchPage) {
    COERCE(HandleFoster(txn, curr, key));
    if (auto foster_pair = curr->GetFoster(txn)) {
      const FosterPair& foster = foster_pair.Value();
      if (foster.key <= key) {
        PageRef next = txn.PageManager()->GetPage(foster.child_pid);
        if (next->RowCount() == 1) {
          COERCE(curr->MoveRightToFoster(txn, *next));
        }
        curr = std::move(next);
        continue;
      }
    }
    assert(curr->Type() == PageType::kBranchPage);
    int next_idx = curr->body.branch_page.Search(key, false);
    PageRef next_page =
        next_idx < 0
            ? txn.PageManager()->GetPage(curr->body.branch_page.lowest_page_)
            : txn.PageManager()->GetPage(
                  curr->body.branch_page.GetValue(next_idx));
    if (next_page->RowCount() == 1 && !next_page->GetFoster(txn)) {
      if (next_idx < curr->RowCount() - 1) {
        // To avoid empty node, make the node have sibling.
        // Make right sibling a foster child and rebalance.
        std::string_view next_key = curr->body.branch_page.GetKey(next_idx + 1);
        PageRef new_foster = txn.PageManager()->GetPage(
            curr->body.branch_page.GetValue(next_idx + 1));
        COERCE(SetFosterRecursively(txn, next_page, new_foster, next_key));
        COERCE(curr->Delete(txn, next_key));
      } else {
        // Make this foster child of left sibling.
        PageRef new_foster_parent = txn.PageManager()->GetPage(
            curr->body.branch_page.GetValue(next_idx - 1));
        std::string_view next_key = curr->GetKey(curr->RowCount() - 1);
        COERCE(
            SetFosterRecursively(txn, new_foster_parent, next_page, next_key));
        COERCE(curr->Delete(txn, next_key));
        next_page = std::move(new_foster_parent);
      }
    }
    curr = std::move(next_page);  // Releases parent lock here.
  }
  assert(curr->Type() == PageType::kLeafPage);
  for (;;) {
    STATUS(HandleFoster(txn, curr, key), "Foster operation must success");
    if (auto foster_pair = curr->GetFoster(txn)) {
      const FosterPair& foster = foster_pair.Value();
      if (foster.key <= key) {
        PageRef next = txn.PageManager()->GetPage(foster.child_pid);
        if (1 < next->RowCount()) {
          curr = std::move(next);
        }
        continue;
      }
    }
    break;
  }
  RETURN_IF_FAIL(curr->Delete(txn, key));
  return Status::kSuccess;
}

StatusOr<std::string_view> BPlusTree::Read(Transaction& txn,
                                           std::string_view key) const {
  PageRef curr = txn.PageManager()->GetPage(root_);
  while (curr->Type() == PageType::kBranchPage) {
    if (auto maybe_foster = curr->GetFoster(txn)) {
      const FosterPair& foster_child = maybe_foster.Value();
      if (foster_child.key <= key) {
        curr = txn.PageManager()->GetPage(foster_child.child_pid);
        continue;
      }
    }
    ASSIGN_OR_CRASH(page_id_t, maybe_next,
                    curr->GetPageForKey(txn, key, false));
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
void DumpLeafPage(Transaction& txn, PageRef& page, std::ostream& o,
                  int indent) {
  o << ": L[" << page->PageID() << "] {" << page->GetLowFence(txn) << "~"
    << page->GetHighFence(txn) << "}: ";
  indent += 2 + std::to_string(page->PageID()).size() + 3;
  for (slot_t i = 0; i < page->RowCount(); ++i) {
    if (0 < i) {
      o << Indent(indent);
    }
    ASSIGN_OR_CRASH(std::string_view, key, page->ReadKey(txn, i));
    ASSIGN_OR_CRASH(std::string_view, value, page->Read(txn, i));
    o << OmittedString(key, 20) << ": " << OmittedString(value, 30) << "\n";
  }
  if (auto maybe_foster = page->GetFoster(txn)) {
    const FosterPair& foster = maybe_foster.Value();
    o << Indent(indent) << " | F[" << foster.child_pid
      << "]: " << HeadString(foster.key, 80) << " from [" << page->PageID()
      << "]\n";
    PageRef child = txn.PageManager()->GetPage(foster.child_pid);
    o << Indent(indent + 1);
    DumpLeafPage(txn, child, o, indent + 1);
  }
}
}  // namespace

void BPlusTree::DumpBranch(Transaction& txn, std::ostream& o, PageRef& page,
                           int indent) const {
  if (page->Type() == PageType::kLeafPage) {
    o << Indent(indent);
    DumpLeafPage(txn, page, o, indent);
  } else if (page->Type() == PageType::kBranchPage) {
    PageRef branch_page =
        txn.PageManager()->GetPage(page->body.branch_page.lowest_page_);
    DumpBranch(txn, o, branch_page, indent + 4);
    if (page->RowCount() == 0) {
      o << Indent(indent) << "(No Slot for " << page->PageID() << ")\n";
      return;
    }
    for (slot_t i = 0; i < page->RowCount(); ++i) {
      ASSIGN_OR_CRASH(std::string_view, key, page->ReadKey(txn, i));
      o << Indent(indent) << "B[" << page->PageID()
        << "]: " << page->GetLowFence(txn) << "~" << page->GetHighFence(txn)
        << ": " << OmittedString(key, 20) << "\n";
      ASSIGN_OR_CRASH(page_id_t, pid, page->GetPageForKey(txn, key, false));
      PageRef current_page = txn.PageManager()->GetPage(pid);
      DumpBranch(txn, o, current_page, indent + 4);
    }
    if (auto maybe_foster = page->GetFoster(txn)) {
      const FosterPair& foster = maybe_foster.Value();
      o << Indent(indent) << " | branch foster["
        << OmittedString(foster.key, 10) << "]\n";
      PageRef child = txn.PageManager()->GetPage(foster.child_pid);
      o << Indent(indent + 1);
      DumpBranch(txn, o, child, indent + 1);
    }
  } else {
    LOG(FATAL) << "Page: " << *page << " : Invalid page type";
    abort();
  }
}

void BPlusTree::Dump(Transaction& txn, std::ostream& o, int indent) const {
  PageRef root_page = txn.PageManager()->GetPage(root_);
  if (root_page->Type() == PageType::kLeafPage) {
    DumpLeafPage(txn, root_page, o, indent);
  } else {
    DumpBranch(txn, o, root_page, indent);
  }
  o << "\n";
}
}  // namespace tinylamb
