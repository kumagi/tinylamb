//
// Created by kumagi on 2022/02/01.
//

#ifndef TINYLAMB_B_PLUS_TREE_HPP
#define TINYLAMB_B_PLUS_TREE_HPP
#include <string_view>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
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
 public:
  BPlusTree(Transaction& txn, page_id_t default_root);
  explicit BPlusTree(page_id_t given_root);
  Status Insert(Transaction& txn, std::string_view key, std::string_view value);
  Status Update(Transaction& txn, std::string_view key, std::string_view value);
  Status Delete(Transaction& txn, std::string_view key);
  StatusOr<std::string_view> Read(Transaction& txn, std::string_view key);
  void Dump(Transaction& txn, std::ostream& o, int indent = 0) const;
  [[nodiscard]] page_id_t Root() const { return root_; }
  BPlusTreeIterator Begin(Transaction& txn, std::string_view left = "",
                          std::string_view right = "", bool ascending = true);
  bool operator==(const BPlusTree& rhs) const = default;
  bool SanityCheckForTest(PageManager* pm) const;

 private:
  static Status LeafInsert(Transaction& txn, PageRef& leaf,
                           std::string_view key, std::string_view value);
  static Status SetFosterRecursively(Transaction& txn, PageRef& parent,
                                     PageRef& new_child,
                                     std::string_view foster_key);
  void GrowTreeHeightIfNeeded(Transaction& txn) const;
  PageRef FindLeaf(Transaction& txn, std::string_view key);

  PageRef FindLeftmostPage(Transaction& txn, PageRef&& root);
  PageRef FindRightmostPage(Transaction& txn, PageRef&& root);
  PageRef LeftmostPage(Transaction& txn);
  PageRef RightmostPage(Transaction& txn);

  friend class BPlusTreeIterator;
  friend class IndexScanIterator;
  void DumpBranch(Transaction& txn, std::ostream& o, PageRef&& page,
                  int indent = 0) const;

  page_id_t root_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_B_PLUS_TREE_HPP
