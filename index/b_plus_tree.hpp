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
  // The optional |hint_leaf| carries the leaf where a previous operation on
  // this same tree landed. When the live tree still routes |key| to that
  // page (verified against the current root and the leaf fences), the
  // root-to-leaf walk is skipped; otherwise a normal descent runs. On
  // success the finally-latched landing leaf is stored back, so consecutive
  // operations can chain the cursor. Pass nullptr (default) to opt out.
  Status Insert(Transaction& txn, std::string_view key, std::string_view value,
                page_id_t* hint_leaf = nullptr);
  Status Update(Transaction& txn, std::string_view key, std::string_view value,
                page_id_t* hint_leaf = nullptr);
  Status Delete(Transaction& txn, std::string_view key) const;
  StatusOr<std::string_view> Read(Transaction& txn, std::string_view key,
                                  page_id_t* hint_leaf = nullptr) const;
  void Dump(Transaction& txn, std::ostream& o, int indent = 0) const;
  [[nodiscard]] page_id_t Root() const { return root_; }
  BPlusTreeIterator Begin(Transaction& txn, std::string_view left = "",
                          std::string_view right = "", bool ascending = true);
  friend std::ostream& operator<<(std::ostream& o, const BPlusTree& t) {
    o << "BPlusTree(root=" << t.root_ << ")";
    return o;
  }
  bool operator==(const BPlusTree& rhs) const = default;
  bool SanityCheckForTest(PageManager* pm) const;
  // Returns a page emptied by a merge to the MetaPage free list. No-op unless
  // the page is provably orphaned: not `protected_pid`, row-less,
  // foster-less, and (for branches) without a remaining lowest child.
  // Public only so the file-local foster-merge helper can recycle pages.
  static void ReclaimIfOrphaned(Transaction& txn, PageRef& page,
                                page_id_t protected_pid);

 private:
  static Status LeafInsert(Transaction& txn, PageRef& leaf,
                           std::string_view key, std::string_view value);
  // Descending-scan start position: last key strictly below `end`, retreating
  // leftwards through leaves; false when no such key exists.
  bool PositionBelow(PageRef& leaf, size_t& idx, Transaction& txn,
                     std::string_view end);
  static Status SetFosterRecursively(Transaction& txn, PageRef& parent,
                                     PageRef& new_child,
                                     std::string_view foster_key);
  // Mutates the tree (allocates pages and rebuilds the root); deliberately
  // non-const so read-only paths cannot grow the tree by accident.
  void GrowTreeHeightIfNeeded(Transaction& txn) const;
  // Follows the foster chain rightwards while the chain key is <= `key`.
  static void FollowFosterChain(Transaction& txn, PageRef& leaf,
                                std::string_view key);
  PageRef FindLeaf(Transaction& txn, std::string_view key, bool less_than);
  // Landing-leaf reuse: validate `hint` against the live tree (current root
  // routing plus fence bracketing) and return it latched, following foster
  // chains rightwards; fall back to a full FindLeaf descent otherwise.
  PageRef FindLeafFromHint(Transaction& txn, std::string_view key,
                           page_id_t hint);
  // Read-only leaf lookup that follows foster chains without absorbing them.
  // If stop_before is non-zero, do not descend into that foster child.
  PageRef FindLeafReadOnly(Transaction& txn, std::string_view key,
                           bool less_than, page_id_t stop_before = 0) const;

  static PageRef FindLeftmostPage(Transaction& txn, PageRef&& page);
  static PageRef FindRightmostPage(Transaction& txn, PageRef&& page);
  PageRef LeftmostPage(Transaction& txn) const;
  PageRef RightmostPage(Transaction& txn) const;

  friend class BPlusTreeIterator;
  friend class IndexScanIterator;
  void DumpBranch(Transaction& txn, std::ostream& o, PageRef& page,
                  int indent = 0) const;

  page_id_t root_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_B_PLUS_TREE_HPP
