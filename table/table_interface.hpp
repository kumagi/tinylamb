//
// Created by kumagi on 2022/02/23.
//

#ifndef TINYLAMB_TABLE_INTERFACE_HPP
#define TINYLAMB_TABLE_INTERFACE_HPP

#include <memory>

#include "common/constants.hpp"
#include "table/iterator.hpp"

namespace tinylamb {

class Transaction;
class Row;
class RowPosition;

class TableInterface {
 public:
  virtual ~TableInterface() = default;
  virtual Status Insert(Transaction& txn, const Row& row, RowPosition* rp) = 0;

  virtual Status Update(Transaction& txn, RowPosition pos, const Row& row) = 0;

  virtual Status Delete(Transaction& txn, RowPosition pos) = 0;

  virtual Status Read(Transaction& txn, RowPosition pos, Row* result) const = 0;

  virtual Status ReadByKey(Transaction& txn, std::string_view index_name,
                           const Row& keys, Row* result) const = 0;

  virtual Iterator BeginFullScan(Transaction& txn) = 0;
  virtual Iterator BeginIndexScan(Transaction& txn, std::string_view index_name,
                                  const Row& begin, const Row& end,
                                  bool ascending) = 0;
};

}  // namespace tinylamb

#endif  // TINYLAMB_TABLE_INTERFACE_HPP
