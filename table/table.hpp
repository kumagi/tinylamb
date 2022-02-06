//
// Created by kumagi on 2022/02/06.
//

#ifndef TINYLAMB_TABLE_HPP
#define TINYLAMB_TABLE_HPP

#include <utility>
#include <vector>

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
        last_pid_(pid),
        indices_(std::move(indices)) {}

  Status Insert(Transaction& txn, const Row& row, RowPosition* rp);

  Status Update(Transaction& txn, RowPosition pos, const Row& row);

  Status Delete(Transaction& txn, RowPosition pos) const;

  Status Read(Transaction& txn, RowPosition pos, Row* result) const;

  Status ReadByKey(Transaction& txn, std::string_view index_name,
                   const Row& keys, Row* result) const;

  PageManager* pm_;
  Schema schema_;
  page_id_t last_pid_{};
  std::vector<Index> indices_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_TABLE_HPP
