/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_TID_SCAN_HPP
#define TINYLAMB_TID_SCAN_HPP

#include <cstddef>
#include <ostream>
#include <utility>
#include <vector>

#include "executor/executor_base.hpp"
#include "page/row_position.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class TidScan : public ExecutorBase {
 public:
  TidScan(Transaction& txn, const Table& table,
          std::vector<RowPosition> positions, Schema schema);
  TidScan(const TidScan&) = delete;
  TidScan(TidScan&&) = delete;
  TidScan& operator=(const TidScan&) = delete;
  TidScan& operator=(TidScan&&) = delete;
  ~TidScan() override = default;

  bool Next(Row* dst, RowPosition* rp) override;
  void Dump(std::ostream& o, int indent) const override;

  [[nodiscard]] size_t PositionCount() const { return positions_.size(); }

 private:
  Transaction* txn_;
  const Table* table_;
  std::vector<RowPosition> positions_;
  Schema schema_;
  size_t offset_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_TID_SCAN_HPP
