//
// Created by kumagi on 2022/02/01.
//

#include "b_plus_tree.hpp"

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

bool BPlusTree::Insert(Transaction& txn, std::string_view key,
                       std::string_view value) {
  std::vector<PageRef> parents;
  PageRef target = FindLeafForInsert(txn, key, pm_->GetPage(root_), parents);
  Status insert_result = target->Insert(txn, key, value);
  if (insert_result == Status::kSuccess) {
    return true;
  } else if (insert_result == Status::kNoSpace) {
    // No enough space? Split!
    PageRef new_page = pm_->AllocateNewPage(txn, PageType::kLeafPage);
    target->Split(txn, new_page.get());
    std::string_view lowest_key;
    new_page->LowestKey(txn, &lowest_key);
    parents.back()->Insert(txn, lowest_key, new_page->PageID());
    return true;
  } else {
    return false;
  }
}

bool BPlusTree::Update(Transaction& txn, std::string_view key,
                       std::string_view value) {
  return true;
}
bool BPlusTree::Delete(Transaction& txn, std::string_view key) { return true; }
bool BPlusTree::Read(Transaction& txn, std::string_view key,
                     std::string_view* dst) {
  *dst = "";
  return true;
}

}  // namespace tinylamb