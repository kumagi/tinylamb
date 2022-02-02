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
    if (parents.empty()) {
      PageRef new_parent = pm_->AllocateNewPage(txn, PageType::kInternalPage);
      new_parent->SetLowestValue(txn, target->page_id);
      new_parent->Insert(txn, middle_key, new_page->page_id);
      root_ = new_parent->PageID();
    } else {
      parents.back()->Insert(txn, middle_key, new_page->PageID());
    }
    return Status::kSuccess;
  } else {
    return Status::kUnknown;
  }
}

Status BPlusTree::Update(Transaction& txn, std::string_view key,
                         std::string_view value) {
  return Status::kSuccess;
}
Status BPlusTree::Delete(Transaction& txn, std::string_view key) {
  return Status::kSuccess;
}
Status BPlusTree::Read(Transaction& txn, std::string_view key,
                       std::string_view* dst) {
  *dst = "";
  return Status::kSuccess;
}

namespace {
void DumpLeafPage(Transaction& txn, PageRef&& page, std::ostream& o,
                  int indent) {
  for (int i = 0; i < page->RowCount(); ++i) {
    if (0 < i) o << Indent(indent);
    std::string_view key;
    page->ReadKey(txn, i, &key);
    std::string_view value;
    page->Read(txn, i, &value);
    if (20 < value.length()) {
      std::string omitted_value = std::string(value).substr(0, 8);
      omitted_value += "..(" + std::to_string(value.length() - 16) + "bytes)..";
      omitted_value += value.substr(value.length() - 8);
      o << key << ": " << omitted_value << "\n";
    } else {
      o << key << ": " << value << "\n";
    }
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
      o << Indent(indent) << key << "\n";
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