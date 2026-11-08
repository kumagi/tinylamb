/**
 * Copyright 2026 KUMAZAKI Hiroki
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

#include "index/lsm_index.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "page/page_type.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

LsmIndexIterator::LsmIndexIterator(LsmIndex* tree, Transaction* txn,
                                   std::string_view begin, std::string_view end,
                                   bool ascending)
    : tree_(tree), txn_(txn) {
  if (tree_ == nullptr || txn_ == nullptr ||
      txn_->GetPageManager() == nullptr) {
    valid_ = false;
    return;
  }
  entries_ = txn_->GetPageManager()->LsmScanRange(tree_->Root(), begin, end,
                                                  true, txn_->ID());
  if (entries_.empty()) {
    valid_ = false;
    return;
  }
  valid_ = true;
  idx_ = ascending ? 0 : entries_.size() - 1;
}

std::string LsmIndexIterator::Key() const {
  if (!valid_ || idx_ >= entries_.size()) {
    return {};
  }
  return entries_[idx_].first;
}

std::string LsmIndexIterator::Value() const {
  if (!valid_ || idx_ >= entries_.size()) {
    return {};
  }
  if (!stable_leaf_cache_ && txn_ != nullptr && tree_ != nullptr &&
      txn_->GetPageManager() != nullptr) {
    StatusOr<std::string> live =
        txn_->GetPageManager()->LsmRead(tree_->Root(), entries_[idx_].first);
    if (live.HasValue()) {
      return live.MoveValue();
    }
    if (live.GetStatus() == Status::kNotExists) {
      return {sizeof(uint64_t), '\0'};
    }
  }
  return entries_[idx_].second;
}

LsmIndexIterator& LsmIndexIterator::operator++() {
  if (!valid_) {
    return *this;
  }
  ++idx_;
  if (idx_ >= entries_.size()) {
    valid_ = false;
  }
  return *this;
}

LsmIndexIterator& LsmIndexIterator::operator--() {
  if (!valid_) {
    return *this;
  }
  if (idx_ == 0) {
    valid_ = false;
  } else {
    --idx_;
  }
  return *this;
}

std::ostream& operator<<(std::ostream& o, const LsmIndexIterator& it) {
  if (!it.IsValid()) {
    o << "<invalid LsmIndexIterator>";
  } else {
    o << "LsmIndexIterator(idx=" << it.idx_ << "/" << it.entries_.size() << ")";
  }
  return o;
}

StatusOr<LsmIndex> LsmIndex::Open(Transaction& txn, page_id_t root) {
  if (root == 0) {
    ASSIGN_OR_RETURN(
        PageRef, page,
        txn.GetPageManager()->AllocateNewPage(txn, PageType::kRowPage));
    root = page->PageID();
  }
  return LsmIndex(root);
}

Status LsmIndex::Insert(Transaction& txn, std::string_view key,
                        std::string_view value, page_id_t* hint_leaf) const {
  if (hint_leaf != nullptr) {
    *hint_leaf = root_;
  }
  return txn.GetPageManager()->LsmInsert(txn, root_, key, value);
}

Status LsmIndex::Update(Transaction& txn, std::string_view key,
                        std::string_view value, page_id_t* hint_leaf) const {
  if (hint_leaf != nullptr) {
    *hint_leaf = root_;
  }
  return txn.GetPageManager()->LsmUpdate(txn, root_, key, value);
}

Status LsmIndex::Delete(Transaction& txn, std::string_view key) const {
  return txn.GetPageManager()->LsmDelete(txn, root_, key);
}

StatusOr<std::string> LsmIndex::Read(Transaction& txn, std::string_view key,
                                     page_id_t* hint_leaf) const {
  if (hint_leaf != nullptr) {
    *hint_leaf = root_;
  }
  return txn.GetPageManager()->LsmRead(root_, key);
}

LsmIndexIterator LsmIndex::Begin(Transaction& txn, std::string_view begin,
                                 std::string_view end, bool ascending) {
  return {this, &txn, begin, end, ascending};
}

StatusOr<std::vector<std::string>> LsmIndex::ScanPartitionBoundaries(
    Transaction& txn, std::string_view begin, std::string_view end,
    size_t leaves_per_partition) const {
  const std::vector<std::pair<std::string, std::string>> entries =
      txn.GetPageManager()->LsmScanRange(root_, begin, end, true, txn.ID());
  constexpr size_t kKeysPerVirtualLeaf = 128;
  const size_t step =
      std::max<size_t>(1, leaves_per_partition) * kKeysPerVirtualLeaf;
  std::vector<std::string> boundaries;
  for (size_t i = step; i < entries.size(); i += step) {
    if (begin.empty() || entries[i].first > begin) {
      if (boundaries.empty() || entries[i].first > boundaries.back()) {
        boundaries.push_back(entries[i].first);
      }
    }
  }
  return boundaries;
}

Status LsmIndex::Destroy(Transaction& txn) const {
  return txn.GetPageManager()->LsmDestroy(txn, root_);
}

void LsmIndex::Dump(std::ostream& o, int indent) const {
  o << std::string(static_cast<size_t>(std::max(0, indent)), ' ')
    << "LsmIndex(root=" << root_ << ")";
}

}  // namespace tinylamb
