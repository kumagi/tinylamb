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

#include "table/full_scan_iterator.hpp"

#include <cassert>
#include <ostream>
#include <string_view>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "iterator_base.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"

namespace tinylamb {

FullScanIterator::FullScanIterator(
    const Table* table, Transaction* txn,
    std::optional<std::vector<slot_t>> projection,
    const std::unordered_set<int64_t>* key_filter,
    std::optional<slot_t> key_column)
    : table_(table),
      txn_(txn),
      pos_(table_->first_pid_, 0),
      projection_(std::move(projection)),
      key_filter_(key_filter),
      key_column_(key_column) {
  page_ = std::make_unique<PageRef>(txn->GetPageManager()->GetPage(
      pos_.page_id, txn->IsReadOnly()));
  SeekVisibleRow();
}

FullScanIterator::FullScanIterator(
    const Table* table, Transaction* txn, std::vector<page_id_t> pages,
    std::optional<std::vector<slot_t>> projection,
    const std::unordered_set<int64_t>* key_filter,
    std::optional<slot_t> key_column)
    : table_(table),
      txn_(txn),
      pos_(pages.empty() ? ~0ULL : pages.front(), 0),
      projection_(std::move(projection)),
      pages_(std::move(pages)),
      key_filter_(key_filter),
      key_column_(key_column) {
  if (!pos_.IsValid()) return;
  page_ = std::make_unique<PageRef>(txn->GetPageManager()->GetPage(
      pos_.page_id, txn->IsReadOnly()));
  SeekVisibleRow();
}

IteratorBase& FullScanIterator::operator++() {
  if (!pos_.IsValid()) return *this;
  ++pos_.slot;
  if (page_ == nullptr) {
    page_ = std::make_unique<PageRef>(
        txn_->GetPageManager()->GetPage(pos_.page_id, txn_->IsReadOnly()));
  }
  SeekVisibleRow();
  return *this;
}

void FullScanIterator::SeekVisibleRow() {
  while (pos_.IsValid()) {
    while (pos_.slot < (*page_)->body.row_page.RowMax()) {
      StatusOr<std::string_view> row = (*page_)->Read(*txn_, pos_.slot);
      if (row.HasValue()) {
        if (key_filter_ && key_column_) {
          const auto key = Row::TryPeekInteger(
              row.Value().data(), table_->GetSchema(), *key_column_);
          if (!key || !key_filter_->contains(*key)) {
            ++pos_.slot;
            continue;
          }
        }
        DeserializeCurrent(row.Value());
        if (!txn_->IsReadOnly()) {
          page_.reset();
        }
        return;
      }
      ++pos_.slot;
    }
    if (!AdvancePage()) break;
  }
  pos_.page_id = ~0ULL;
  current_row_.Clear();
}

bool FullScanIterator::AdvancePage() {
  page_id_t next_page = 0;
  if (pages_) {
    ++page_index_;
    if (page_index_ < pages_->size()) next_page = (*pages_)[page_index_];
  } else {
    next_page = (*page_)->body.row_page.next_page_id_;
  }
  page_.reset();
  if (next_page == 0) return false;
  pos_ = RowPosition(next_page, 0);
  page_ = std::make_unique<PageRef>(txn_->GetPageManager()->GetPage(
      next_page, txn_->IsReadOnly()));
  return true;
}

void FullScanIterator::DeserializeCurrent(std::string_view row) {
  if (projection_) {
    current_row_.DeserializeProjected(row.data(), table_->schema_,
                                      *projection_);
  } else {
    current_row_.Deserialize(row.data(), table_->schema_);
  }
}

IteratorBase& FullScanIterator::operator--() {
  assert(!"not implemented function");
  return *this;
}

bool FullScanIterator::IsValid() const { return pos_.IsValid(); }

const Row& FullScanIterator::operator*() const { return current_row_; }
Row& FullScanIterator::operator*() { return current_row_; }

void FullScanIterator::Dump(std::ostream& o, int /*indent*/) const {
  o << "FullScan: " << table_->schema_.Name();
}

}  // namespace tinylamb
