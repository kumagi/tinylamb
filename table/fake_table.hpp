//
// Created by kumagi on 2022/02/23.
//

#ifndef TINYLAMB_FAKE_TABLE_HPP
#define TINYLAMB_FAKE_TABLE_HPP

#include <vector>

#include "table/iterator_base.hpp"
#include "table/table_interface.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {
class Encoder;
class Decoder;

class FakeIterator : public IteratorBase {
 public:
  explicit FakeIterator(const std::vector<Row>& table)
      : table_(&table), iter_(table.begin()) {}
  ~FakeIterator() override = default;
  [[nodiscard]] bool IsValid() const override { return iter_ != table_->end(); }
  const Row& operator*() const override { return *iter_; }
  IteratorBase& operator++() override {
    ++iter_;
    return *this;
  }
  IteratorBase& operator--() override {
    --iter_;
    return *this;
  }
  const std::vector<Row>* table_;
  std::vector<Row>::const_iterator iter_;
};

// Supports full interface of table but never support transaction.
class FakeTable : public TableInterface {
 public:
  FakeTable() = default;
  FakeTable(std::initializer_list<Row> table) : table_(table) {}
  ~FakeTable() override = default;

  Status Insert(Transaction& txn, const Row& row, RowPosition* rp) override;

  Status Update(Transaction& txn, const Row& row, RowPosition* pos) override;

  Status Delete(Transaction& txn, RowPosition pos) override;

  Status Read(Transaction& txn, RowPosition pos, Row* result) const override;

  Status ReadByKey(Transaction& txn, std::string_view index_name,
                   const Row& keys, Row* result) const override;

  Iterator BeginFullScan(Transaction& txn) const override;

  Iterator BeginIndexScan(Transaction& txn, std::string_view index_name,
                          const Row& begin, const Row& end,
                          bool ascending) override;

  [[nodiscard]] Schema GetSchema() const override {
    return Schema("FakeTable", {});
  }

 private:
  std::vector<Row> table_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_FAKE_TABLE_HPP
