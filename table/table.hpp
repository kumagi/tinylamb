//
// Created by kumagi on 2022/02/06.
//

#ifndef TINYLAMB_TABLE_HPP
#define TINYLAMB_TABLE_HPP

#include <utility>
#include <vector>

#include "full_scan_iterator.hpp"
#include "index/index.hpp"
#include "index/index_scan_iterator.hpp"
#include "page/row_position.hpp"
#include "table/table_interface.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class Transaction;
class PageManager;
class Decoder;
class Encoder;
struct Row;
struct RowPosition;

class Table : public TableInterface {
 public:
  Table() = default;
  explicit Table(PageManager* pm) : pm_(pm) {}
  Table(PageManager* pm, Schema sc, page_id_t pid, std::vector<Index> indices)
      : pm_(pm),
        schema_(std::move(sc)),
        first_pid_(pid),
        last_pid_(pid),
        indices_(std::move(indices)) {}
  ~Table() override = default;

  Status Insert(Transaction& txn, const Row& row, RowPosition* rp) override;

  Status Update(Transaction& txn, RowPosition pos, const Row& row) override;

  Status Delete(Transaction& txn, RowPosition pos) override;

  Status Read(Transaction& txn, RowPosition pos, Row* result) const override;

  Status ReadByKey(Transaction& txn, std::string_view index_name,
                   const Row& keys, Row* result) const override;

  Iterator BeginFullScan(Transaction& txn) const override;
  Iterator BeginIndexScan(Transaction& txn, std::string_view index_name,
                          const Row& begin, const Row& end = Row(),
                          bool ascending = true) override;

  [[nodiscard]] Schema GetSchema() const override { return schema_; }

  friend Encoder& operator<<(Encoder& e, const Table& t);
  friend Decoder& operator>>(Decoder& d, Table& t);

 private:
  friend class Catalog;
  friend class FullScanIterator;
  friend class IndexScanIterator;
  friend class TableInterface;
  friend class FullScanPlan;
  page_id_t GetIndex(std::string_view name);

  PageManager* pm_{};
  Schema schema_;
  page_id_t first_pid_{};
  page_id_t last_pid_{};
  std::vector<Index> indices_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_TABLE_HPP
