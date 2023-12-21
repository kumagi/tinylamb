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

#ifndef TINYLAMB_TABLE_HPP
#define TINYLAMB_TABLE_HPP

#include <unordered_map>
#include <utility>
#include <vector>

#include "common/status_or.hpp"
#include "full_scan_iterator.hpp"
#include "index/index.hpp"
#include "iterator.hpp"
#include "page/row_position.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class Transaction;
class Decoder;
class Encoder;
struct Row;
struct RowPosition;

class Table {
 public:
  struct IndexValueType {
    RowPosition pos;
    Row include;
    friend Encoder& operator<<(Encoder& e, const IndexValueType& v);
    friend Decoder& operator>>(Decoder& d, IndexValueType& t);
  };

  Table() = default;
  Table(Schema sc, page_id_t pid)
      : schema_(std::move(sc)), first_pid_(pid), last_pid_(pid) {}
  Table(const Table&) = default;
  Table(Table&&) = default;
  Table& operator=(const Table&) = default;
  Table& operator=(Table&&) = default;
  ~Table() = default;

  Status CreateIndex(Transaction& txn, const IndexSchema& idx);

  StatusOr<RowPosition> Insert(Transaction& txn, const Row& row);

  StatusOr<RowPosition> Update(Transaction& txn, const RowPosition& pos,
                               const Row& row);

  Status Delete(Transaction& txn, RowPosition pos);

  StatusOr<Row> Read(Transaction& txn, RowPosition pos) const;

  Iterator BeginFullScan(Transaction& txn) const;
  Iterator BeginIndexScan(Transaction& txn, const Index& index,
                          const Value& begin = Value(),
                          const Value& end = Value(),
                          bool ascending = true) const;

  [[nodiscard]] std::unordered_map<slot_t, size_t> AvailableKeyIndex() const;

  [[nodiscard]] const Schema& GetSchema() const { return schema_; }
  [[nodiscard]] size_t IndexCount() const { return indexes_.size(); }

  friend Encoder& operator<<(Encoder& e, const Table& t);
  friend Decoder& operator>>(Decoder& d, Table& t);
  bool operator==(const Table& rhs) const = default;
  [[nodiscard]] const Index& GetIndex(size_t offset) const {
    return indexes_[offset];
  }

 private:
  Status IndexInsert(Transaction& txn, const Index& idx, const Row& new_row,
                     const RowPosition& pos);
  Status IndexDelete(Transaction& txn, const Index& idx,
                     const RowPosition& pos);

  friend class Database;
  friend class FullScanIterator;
  friend class IndexScanIterator;
  friend class TableInterface;

  Schema schema_;
  page_id_t first_pid_{};
  page_id_t last_pid_{};
  std::vector<Index> indexes_{};
};

}  // namespace tinylamb

#endif  // TINYLAMB_TABLE_HPP
