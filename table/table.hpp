//
// Created by kumagi on 2022/02/06.
//

#ifndef TINYLAMB_TABLE_HPP
#define TINYLAMB_TABLE_HPP

#include <unordered_map>
#include <utility>
#include <vector>

#include "common/status_or.hpp"
#include "full_scan_iterator.hpp"
#include "index/index.hpp"
#include "index/index_scan_iterator.hpp"
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
  Table() = default;
  Table(Schema sc, page_id_t pid)
      : schema_(std::move(sc)), first_pid_(pid), last_pid_(pid) {}
  ~Table() = default;

  Status CreateIndex(Transaction& txn, const IndexSchema& idx);

  StatusOr<RowPosition> Insert(Transaction& txn, const Row& row);

  StatusOr<RowPosition> Update(Transaction& txn, const RowPosition& pos,
                               const Row& row);

  Status Delete(Transaction& txn, RowPosition pos);

  StatusOr<Row> Read(Transaction& txn, RowPosition pos) const;

  Iterator BeginFullScan(Transaction& txn) const;
  Iterator BeginIndexScan(Transaction& txn, const Index& index,
                          const Value& begin, const Value& end = Value(),
                          bool ascending = true) const;

  [[nodiscard]] std::unordered_map<slot_t, size_t> AvailableKeyIndex() const;

  [[nodiscard]] const Schema& GetSchema() const { return schema_; }
  [[nodiscard]] size_t IndexCount() const { return indexes_.size(); }

  friend Encoder& operator<<(Encoder& e, const Table& t);
  friend Decoder& operator>>(Decoder& d, Table& t);
  [[nodiscard]] const Index& GetIndex(size_t offset) const {
    return indexes_[offset];
  }
  void AddIndex(const Index& idx) { indexes_.emplace_back(idx); }

 private:
  Status IndexInsert(Transaction& txn, const Row& new_row,
                     const RowPosition& pos);
  Status IndexDelete(Transaction& txn, const RowPosition& pos);

  friend class RelationStorage;
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
