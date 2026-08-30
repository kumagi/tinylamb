/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_BITMAP_SCAN_PLAN_HPP
#define TINYLAMB_BITMAP_SCAN_PLAN_HPP

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "plan/plan.hpp"
#include "table/table_statistics.hpp"
#include "type/value.hpp"

namespace tinylamb {
class Index;
class Table;

enum class BitmapCombine : uint8_t { kAnd, kOr };

struct BitmapIndexRange {
  const Index* index{nullptr};
  std::vector<Value> begin_key;
  std::vector<Value> end_key;
};

class BitmapScanPlan final : public PlanBase {
 public:
  BitmapScanPlan(const Table& table, const TableStatistics& statistics,
                 std::vector<BitmapIndexRange> ranges, BitmapCombine combine,
                 Expression where, size_t estimated_rows,
                 size_t access_rows);

  Executor EmitExecutor(TransactionContext& txn) const override;
  [[nodiscard]] const Table* ScanSource() const override { return &table_; }
  [[nodiscard]] const Schema& GetSchema() const override;
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return statistics_;
  }
  [[nodiscard]] size_t AccessRowCount() const override { return access_rows_; }
  [[nodiscard]] size_t EmitRowCount() const override { return estimated_rows_; }
  void Dump(std::ostream& output, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

  [[nodiscard]] const Table& GetTable() const { return table_; }
  [[nodiscard]] const std::vector<BitmapIndexRange>& Ranges() const {
    return ranges_;
  }
  [[nodiscard]] BitmapCombine Combine() const { return combine_; }
  [[nodiscard]] const Expression& Where() const { return where_; }

 private:
  const Table& table_;
  TableStatistics statistics_;
  std::vector<BitmapIndexRange> ranges_;
  BitmapCombine combine_;
  Expression where_;
  size_t estimated_rows_;
  size_t access_rows_;
};

}  // namespace tinylamb
#endif  // TINYLAMB_BITMAP_SCAN_PLAN_HPP
