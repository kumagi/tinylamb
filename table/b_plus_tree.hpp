//
// Created by kumagi on 2022/02/01.
//

#ifndef TINYLAMB_B_PLUS_TREE_HPP
#define TINYLAMB_B_PLUS_TREE_HPP
#include <string_view>
#include <vector>

#include "common/constants.hpp"
#include "page/page_ref.hpp"

namespace tinylamb {

class BPlusTreeIterator;
class Transaction;
class PageManager;
class PageRef;

/*
 * A versatile persistent data structure, which supports { string => string }.
 */
class BPlusTree {
 private:
  PageRef FindLeafForInsert(Transaction& txn, std::string_view key,
                            PageRef&& page, std::vector<PageRef>& parents);

  PageRef FindLeaf(Transaction& txn, std::string_view key, PageRef&& root);
  Status InsertInternal(Transaction& txn, std::string_view key, page_id_t left,
                        page_id_t right, std::vector<PageRef>& parents);
  PageRef FindLeftmostPage(Transaction& txn, PageRef&& root);
  PageRef FindRightmostPage(Transaction& txn, PageRef&& root);
  PageRef LeftmostPage(Transaction& txn);
  PageRef RightmostPage(Transaction& txn);

 public:
  BPlusTree(page_id_t root, PageManager* pm);
  Status Insert(Transaction& txn, std::string_view key, std::string_view value);
  Status Update(Transaction& txn, std::string_view key, std::string_view value);
  Status Delete(Transaction& txn, std::string_view key);
  Status Read(Transaction& txn, std::string_view key, std::string_view* dst);
  void Dump(Transaction& txn, std::ostream& o, int indent = 0) const;
  [[nodiscard]] page_id_t Root() const { return root_; }
  BPlusTreeIterator Begin(Transaction& txn, std::string_view left = "",
                          std::string_view right = "", bool ascending = true);

 private:
  friend class BPlusTreeIterator;
  void DumpInternal(Transaction& txn, std::ostream& o, PageRef&& page,
                    int indent = 0) const;

 private:
  page_id_t root_;
  PageManager* pm_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_B_PLUS_TREE_HPP
