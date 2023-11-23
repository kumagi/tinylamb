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

#ifndef TINYLAMB_FULL_SCAN_ITERATOR_HPP
#define TINYLAMB_FULL_SCAN_ITERATOR_HPP
#include <memory>

#include "page/row_position.hpp"
#include "table/iterator_base.hpp"
#include "type/row.hpp"

namespace tinylamb {
class Table;
class Transaction;

class FullScanIterator : public IteratorBase {
 public:
  ~FullScanIterator() override = default;
  bool operator==(const FullScanIterator& rhs) const {
    return table_ == rhs.table_ && txn_ == rhs.txn_ &&
           current_row_ == rhs.current_row_;
  }

  bool operator!=(const FullScanIterator& rhs) const {
    return !(operator==(rhs));
  }

  [[nodiscard]] bool IsValid() const override;
  [[nodiscard]] RowPosition Position() const override { return pos_; }
  IteratorBase& operator++() override;
  IteratorBase& operator--() override;
  const Row& operator*() const override;
  Row& operator*() override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  friend class Table;
  friend class FullScan;
  FullScanIterator(const Table* table, Transaction* txn);

  const Table* table_;
  Transaction* txn_;
  RowPosition pos_;
  Row current_row_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_FULL_SCAN_ITERATOR_HPP
