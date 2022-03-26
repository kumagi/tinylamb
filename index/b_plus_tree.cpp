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

BPlusTree::BPlusTree(page_id_t root, PageManager* pm) : root_(root), pm_(pm) {}

PageRef BPlusTree::FindLeafForInsert(Transaction& txn, std::string_view key,
                                     PageRef&& page,
                                     std::vector<PageRef>& parents) {
  if (page->Type() == PageType::kLeafPage) {
    return std::move(page);
  }
  page_id_t next;
  if (page->GetPageForKey(txn, key, &next) != Status::kSuccess) {
    LOG(ERROR) << "No next page found";
  }
  PageRef next_ref = pm_->GetPage(next);
  parents.emplace_back(std::move(page));
  return FindLeafForInsert(txn, key, std::move(next_ref), parents);
}

PageRef BPlusTree::FindLeaf(Transaction& txn, std::string_view key,
                            PageRef&& root) {
  if (root->Type() == PageType::kLeafPage) return std::move(root);
  page_id_t next;
  if (root->GetPageForKey(txn, key, &next) != Status::kSuccess) {
    LOG(ERROR) << "No next page found";
  }
  PageRef next_ref = pm_->GetPage(next);
  return FindLeaf(txn, key, std::move(next_ref));
}

Status BPlusTree::InsertBranch(Transaction& txn, std::string_view key,
                               page_id_t right, std::vector<PageRef>& parents) {
  if (parents.empty()) {
    // Swap parent node to the branch node.
    PageRef root = pm_->GetPage(root_);
    if (root->Type() == PageType::kBranchPage) {
      const BranchPage& root_page = root->body.branch_page;
      PageRef new_left = pm_->AllocateNewPage(txn, PageType::kBranchPage);
      new_left->SetLowestValue(txn, root_page.lowest_page_);
      for (slot_t i = 0; i < root->RowCount(); ++i) {
        new_left->InsertBranch(txn, root_page.GetKey(i), root_page.GetValue(i));
      }
      while (0 < root_page.RowCount()) {
        root->Delete(txn, root_page.GetKey(0));
      }
      root->SetLowestValue(txn, new_left->PageID());
      root->InsertBranch(txn, key, right);
    } else {
      assert(root->Type() == PageType::kLeafPage);
      PageRef new_left = pm_->AllocateNewPage(txn, PageType::kLeafPage);
      const LeafPage& root_page = root->body.leaf_page;
      for (slot_t i = 0; i < root->body.leaf_page.RowCount(); ++i) {
        new_left->InsertLeaf(txn, root_page.GetKey(i), root_page.GetValue(i));
      }
      PageRef right_page = pm_->GetPage(right);
      right_page->SetPrevNext(txn, new_left->PageID(),
                              right_page->body.leaf_page.next_pid_);
      root->PageTypeChange(txn, PageType::kBranchPage);
      root->SetLowestValue(txn, new_left->PageID());
      root->InsertBranch(txn, key, right);
      new_left->SetPrevNext(txn, 0, right);
    }
    return Status::kSuccess;
  } else {
    PageRef branch(std::move(parents.back()));
    parents.pop_back();
    Status s = branch->InsertBranch(txn, key, right);
    if (s == Status::kSuccess) return Status::kSuccess;
    if (s == Status::kNoSpace) {
      PageRef new_node = pm_->AllocateNewPage(txn, PageType::kBranchPage);
      std::string new_key;
      branch->SplitInto(txn, key, new_node.get(), &new_key);
      s = [&]() {
        if (key < new_key) {
          return branch->InsertBranch(txn, key, right);
        } else {
          return new_node->InsertBranch(txn, key, right);
        }
      }();
      if (s != Status::kSuccess) return s;
      branch.PageUnlock();
      return InsertBranch(txn, new_key, new_node->PageID(), parents);
    }
    throw std::runtime_error("unknown status:" + std::string(ToString(s)));
  }
}

PageRef BPlusTree::FindLeftmostPage(Transaction& txn, PageRef&& root) {
  if (root->Type() == PageType::kLeafPage) return std::move(root);
  return FindLeftmostPage(txn,
                          pm_->GetPage(root->body.branch_page.lowest_page_));
}

PageRef BPlusTree::FindRightmostPage(Transaction& txn, PageRef&& root) {
  if (root->Type() == PageType::kLeafPage) return std::move(root);
  page_id_t next =
      root->body.branch_page.GetValue(root->body.branch_page.RowCount() - 1);
  return FindRightmostPage(txn, pm_->GetPage(next));
}

PageRef BPlusTree::LeftmostPage(Transaction& txn) {
  return FindLeftmostPage(txn, pm_->GetPage(root_));
}

PageRef BPlusTree::RightmostPage(Transaction& txn) {
  return FindRightmostPage(txn, pm_->GetPage(root_));
}

Status BPlusTree::Insert(Transaction& txn, std::string_view key,
                         std::string_view value) {
  std::vector<PageRef> parents;
  PageRef target = FindLeafForInsert(txn, key, pm_->GetPage(root_), parents);
  Status s = target->InsertLeaf(txn, key, value);
  if (s == Status::kSuccess) return Status::kSuccess;
  if (s == Status::kNoSpace) {
    // No enough space? Split!
    PageRef new_page = pm_->AllocateNewPage(txn, PageType::kLeafPage);
    target->body.leaf_page.Split(target->PageID(), txn, key, value,
                                 new_page.get());
    target.PageUnlock();
    if (new_page->RowCount() == 0 || new_page->GetKey(0) < key) {
      s = new_page->InsertLeaf(txn, key, value);
      STATUS(s, "new_page");
    } else {
      s = target->InsertLeaf(txn, key, value);
      STATUS(s, "BPT, target");
    }
    std::string_view middle_key;
    new_page->LowestKey(txn, &middle_key);
    new_page.PageUnlock();
    return InsertBranch(txn, middle_key, new_page->PageID(), parents);
  }
  return s;
}

Status BPlusTree::Update(Transaction& txn, std::string_view key,
                         std::string_view value) {
  PageRef leaf = FindLeaf(txn, key, pm_->GetPage(root_));
  Status s = leaf->Update(txn, key, value);
  if (s == Status::kSuccess) return Status::kSuccess;
  if (s == Status::kNoSpace) {
    // No enough space? Split!
    leaf->Delete(txn, key);
    leaf.PageUnlock();
    std::vector<PageRef> parents;
    PageRef target = FindLeafForInsert(txn, key, pm_->GetPage(root_), parents);
    PageRef new_page = pm_->AllocateNewPage(txn, PageType::kLeafPage);
    target->body.leaf_page.Split(target->PageID(), txn, key, value,
                                 new_page.get());
    target.PageUnlock();
    if (new_page->RowCount() == 0 || new_page->GetKey(0) < key) {
      s = new_page->InsertLeaf(txn, key, value);
      STATUS(s, "new_page");
    } else {
      s = target->InsertLeaf(txn, key, value);
      STATUS(s, "BPT, target");
    }
    LOG(DEBUG) << s;
    std::string_view middle_key;
    new_page->LowestKey(txn, &middle_key);
    new_page.PageUnlock();
    return InsertBranch(txn, middle_key, new_page->PageID(), parents);
  }
  return s;
}

void ChangeLeftMostKey(Transaction& txn, std::string_view old_next,
                       std::string_view new_next,
                       std::vector<PageRef>& parents) {
  for (int idx = (int)parents.size() - 1; 0 <= idx; --idx) {
    PageRef& page = parents[idx];
    page_id_t value;
    Status s = page->FindForKey(txn, old_next, &value);
    if (s == Status::kSuccess) {
      s = page->Delete(txn, old_next);
      assert(s == Status::kSuccess);
      s = page->InsertBranch(txn, new_next, value);
      assert(s == Status::kSuccess);
    } else {
      break;
    }
  }
}

Status BPlusTree::Delete(Transaction& txn, std::string_view key) {
  PageRef root = pm_->GetPage(root_);
  std::vector<PageRef> parents;
  PageRef leaf = FindLeafForInsert(txn, key, std::move(root), parents);
  // TODO(kumagi): We need to merge the leaf and delete the entire node.
  std::string next_leftmost = leaf->GetKey(0) == key && 1 < leaf->RowCount()
                                  ? std::string(leaf->GetKey(1))
                                  : "";
  Status s = leaf->Delete(txn, key);
  if (s != Status::kSuccess) return s;
  if (!next_leftmost.empty()) {
    ChangeLeftMostKey(txn, key, next_leftmost, parents);
  }

  PageRef ref = std::move(leaf);
  while (ref->RowCount() == 0 && !parents.empty()) {
    LOG(ERROR) << "page destroy: " << ref->PageID();
    ref.PageUnlock();
    pm_->DestroyPage(txn, ref.get());
    PageRef parent = std::move(parents.back());
    parent->Delete(txn, key);
    parents.pop_back();
    ref = std::move(parent);
  }
  return s;
}

Status BPlusTree::Read(Transaction& txn, std::string_view key,
                       std::string_view* dst) {
  *dst = "";
  PageRef leaf = FindLeaf(txn, key, pm_->GetPage(root_));
  return leaf->Read(txn, key, dst);
}

BPlusTreeIterator BPlusTree::Begin(Transaction& txn, std::string_view left,
                                   std::string_view right, bool ascending) {
  return {this, &txn, left, right, ascending};
}

bool BPlusTree::SanityCheckForTest(PageManager* pm) const {
  PageRef page = pm_->GetPage(root_);
  if (page->Type() == PageType::kLeafPage) {
    return page->body.leaf_page.SanityCheckForTest();
  } else if (page->Type() == PageType::kBranchPage) {
    return page->body.branch_page.SanityCheckForTest(pm);
  }
  return false;
}

namespace {

std::string OmittedString(std::string_view original, int length) {
  if (length < original.length()) {
    std::string omitted_key = std::string(original).substr(0, 8);
    omitted_key +=
        "..(" + std::to_string(original.length() - length + 4) + "bytes)..";
    omitted_key += original.substr(original.length() - 8);
    return omitted_key;
  } else {
    return std::string(original);
  }
}

void DumpLeafPage(Transaction& txn, PageRef&& page, std::ostream& o,
                  int indent) {
  o << "L[" << page->PageID() << "]: ";
  indent += 2 + std::to_string(page->PageID()).size() + 3;
  for (slot_t i = 0; i < page->RowCount(); ++i) {
    if (0 < i) o << Indent(indent);
    std::string_view key;
    page->ReadKey(txn, i, &key);
    std::string_view value;
    page->Read(txn, i, &value);
    o << OmittedString(key, 80) << ": " << OmittedString(value, 30) << "\n";
  }
}
}  // namespace

void BPlusTree::DumpBranch(Transaction& txn, std::ostream& o, PageRef&& page,
                           int indent) const {
  if (page->Type() == PageType::kLeafPage) {
    o << Indent(indent);
    DumpLeafPage(txn, std::move(page), o, indent);
  } else if (page->Type() == PageType::kBranchPage) {
    DumpBranch(txn, o, pm_->GetPage(page->body.branch_page.lowest_page_),
               indent + 4);
    for (slot_t i = 0; i < page->RowCount(); ++i) {
      std::string_view key;
      page->ReadKey(txn, i, &key);
      o << Indent(indent) << "B[" << page->PageID()
        << "]: " << OmittedString(key, 20) << "\n";
      page_id_t pid;
      if (page->GetPageForKey(txn, key, &pid) == Status::kSuccess) {
        DumpBranch(txn, o, pm_->GetPage(pid), indent + 4);
      }
    }
  }
}

void BPlusTree::Dump(Transaction& txn, std::ostream& o, int indent) const {
  DumpBranch(txn, o, pm_->GetPage(root_), indent);
  o << "\n";
}

}  // namespace tinylamb