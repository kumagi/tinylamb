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

 public:
  BPlusTree(page_id_t root, PageManager* pm);
  Status Insert(Transaction& txn, std::string_view key, std::string_view value);
  Status Update(Transaction& txn, std::string_view key, std::string_view value);
  Status Delete(Transaction& txn, std::string_view key);
  Status Read(Transaction& txn, std::string_view key, std::string_view* dst);
  void Dump(Transaction& txn, std::ostream& o, int indent = 0) const;

 private:
  void DumpInternal(Transaction& txn, std::ostream& o, PageRef&& page,
                    int indent = 0) const;

 private:
  page_id_t root_;
  PageManager* pm_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_B_PLUS_TREE_HPP
