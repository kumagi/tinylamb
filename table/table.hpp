//
// Created by kumagi on 2022/02/06.
//

#ifndef TINYLAMB_TABLE_HPP
#define TINYLAMB_TABLE_HPP

#include <utility>
#include <vector>

#include "full_scan_iterator.hpp"
#include "index_scan_iterator.hpp"
#include "page/row_position.hpp"
#include "table/index.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class Transaction;
class Row;
class RowPosition;
class PageManager;

class Table {
 public:
  Table(PageManager* pm, Schema sc, page_id_t pid, std::vector<Index> indices)
      : pm_(pm),
        schema_(std::move(sc)),
        first_pid_(pid),
        last_pid_(pid),
        indices_(std::move(indices)) {}

  Status Insert(Transaction& txn, const Row& row, RowPosition* rp);

  Status Update(Transaction& txn, RowPosition pos, const Row& row);

  Status Delete(Transaction& txn, RowPosition pos) const;

  Status Read(Transaction& txn, RowPosition pos, Row* result) const;

  Status ReadByKey(Transaction& txn, std::string_view index_name,
                   const Row& keys, Row* result) const;

  FullScanIterator BeginFullScan(Transaction& txn);
  IndexScanIterator BeginIndexScan(Transaction& txn,
                                   std::string_view index_name,
                                   const Row& begin, const Row& end = Row(),
                                   bool ascending = true);

 private:
  page_id_t GetIndex(std::string_view name);
  friend class FullScanIterator;
  friend class IndexScanIterator;

  PageManager* pm_;
  Schema schema_;
  page_id_t first_pid_{};
  page_id_t last_pid_{};
  std::vector<Index> indices_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_TABLE_HPP
