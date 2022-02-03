//
// Created by kumagi on 2022/02/01.
//
#include "b_plus_tree.hpp"

#include <cstring>
#include <vector>

#include "page/page_manager.hpp"
#include "page/page_ref.hpp"

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

Status BPlusTree::InsertInternal(Transaction& txn, std::string_view key,
                                 page_id_t left, page_id_t right,
                                 std::vector<PageRef>& parents) {
  if (parents.empty()) {
    PageRef new_parent = pm_->AllocateNewPage(txn, PageType::kInternalPage);
    new_parent->SetLowestValue(txn, left);
    new_parent->Insert(txn, key, right);
    root_ = new_parent->PageID();
    return Status::kSuccess;
  } else {
    PageRef internal(std::move(parents.back()));
    parents.pop_back();
    Status result = internal->Insert(txn, key, right);
    if (result == Status::kSuccess) return Status::kSuccess;
    if (result == Status::kNoSpace) {
      PageRef new_node = pm_->AllocateNewPage(txn, PageType::kInternalPage);
      std::string_view new_key;
      internal->SplitInto(txn, new_node.get(), &new_key);
      if (key < new_key) {
        internal->Insert(txn, key, right);
      } else {
        new_node->Insert(txn, key, right);
      }
      return InsertInternal(txn, new_key, internal->PageID(),
                            new_node->PageID(), parents);
    }
    throw std::runtime_error("unknown status:" + std::string(ToString(result)));
  }
}

Status BPlusTree::Insert(Transaction& txn, std::string_view key,
                         std::string_view value) {
  std::vector<PageRef> parents;
  PageRef target = FindLeafForInsert(txn, key, pm_->GetPage(root_), parents);
  Status insert_result = target->Insert(txn, key, value);
  if (insert_result == Status::kSuccess) {
    return Status::kSuccess;
  } else if (insert_result == Status::kNoSpace) {
    // No enough space? Split!
    target.PageUnlock();
    PageRef new_page = pm_->AllocateNewPage(txn, PageType::kLeafPage);
    target->Split(txn, new_page.get());
    std::string_view middle_key;
    new_page->LowestKey(txn, &middle_key);
    if (key < middle_key) {
      target->Insert(txn, key, value);
    } else {
      new_page->Insert(txn, key, value);
    }
    return InsertInternal(txn, middle_key, target->PageID(), new_page->PageID(),
                          parents);
  }
  throw std::runtime_error("unknown insert status");
}

Status BPlusTree::Update(Transaction& txn, std::string_view key,
                         std::string_view value) {
  PageRef leaf = FindLeaf(txn, key, pm_->GetPage(root_));
  return leaf->Update(txn, key, value);
}
Status BPlusTree::Delete(Transaction& txn, std::string_view key) {
  PageRef leaf = FindLeaf(txn, key, pm_->GetPage(root_));
  // TODO(kumagi): We need to merge the leaf and delete the entire node.
  return leaf->Delete(txn, key);
}
Status BPlusTree::Read(Transaction& txn, std::string_view key,
                       std::string_view* dst) {
  *dst = "";
  PageRef leaf = FindLeaf(txn, key, pm_->GetPage(root_));
  return leaf->Read(txn, key, dst);
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
  for (int i = 0; i < page->RowCount(); ++i) {
    if (0 < i) o << Indent(indent);
    std::string_view key;
    page->ReadKey(txn, i, &key);
    std::string_view value;
    page->Read(txn, i, &value);
    o << OmittedString(key, 20) << ": " << OmittedString(value, 20) << "\n";
  }
}
}  // namespace

void BPlusTree::DumpInternal(Transaction& txn, std::ostream& o, PageRef&& page,
                             int indent) const {
  if (page->Type() == PageType::kLeafPage) {
    o << Indent(indent);
    DumpLeafPage(txn, std::move(page), o, indent);
  } else if (page->Type() == PageType::kInternalPage) {
    page_id_t lowest;
    if (page->LowestPage(txn, &lowest) == Status::kSuccess) {
      DumpInternal(txn, o, pm_->GetPage(lowest), indent + 2);
    }
    for (int i = 0; i < page->RowCount(); ++i) {
      std::string_view key;
      page->ReadKey(txn, i, &key);
      o << Indent(indent) << OmittedString(key, 20) << "\n";
      page_id_t pid;
      if (page->Read(txn, key, &pid) == Status::kSuccess) {
        DumpInternal(txn, o, pm_->GetPage(pid), indent + 2);
      }
    }
  }
}

void BPlusTree::Dump(Transaction& txn, std::ostream& o, int indent) const {
  DumpInternal(txn, o, pm_->GetPage(root_), indent);
  o << "\n";
}

}  // namespace tinylamb