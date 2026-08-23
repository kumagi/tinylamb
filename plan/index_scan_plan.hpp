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

#ifndef TINYLAMB_INDEX_SCAN_PLAN_HPP
#define TINYLAMB_INDEX_SCAN_PLAN_HPP

#include <utility>
#include <vector>

#include "plan/plan.hpp"
#include "table/table_statistics.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"

namespace tinylamb {
class Index;
class Table;

class IndexScanPlan final : public PlanBase {
 public:
  IndexScanPlan(const Table& table, const Index& index,
                const TableStatistics& ts, std::vector<Value> begin_key,
                std::vector<Value> end_key, bool ascending, Expression where,
                std::vector<ColumnName> provided_order = {});
  // Point-union access for constant IN lists: one [begin,end] pair per
  // distinct value. Ordering credit only survives a single range.
  IndexScanPlan(
      const Table& table, const Index& index, const TableStatistics& ts,
      std::vector<std::pair<std::vector<Value>, std::vector<Value>>> ranges,
      bool ascending, Expression where,
      std::vector<ColumnName> provided_order = {});
  IndexScanPlan(const IndexScanPlan&) = delete;
  IndexScanPlan(IndexScanPlan&&) = delete;
  IndexScanPlan& operator=(const IndexScanPlan&) = delete;
  IndexScanPlan& operator=(IndexScanPlan&&) = delete;
  ~IndexScanPlan() override = default;

  Executor EmitExecutor(TransactionContext& txn) const override;

  [[nodiscard]] const Table* ScanSource() const override { return &table_; }
  [[nodiscard]] const Schema& GetSchema() const override;
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return stats_;
  }

  [[nodiscard]] size_t AccessRowCount() const override;
  [[nodiscard]] size_t EmitRowCount() const override;
  [[nodiscard]] bool IsOrderedBy(const std::vector<Expression>& expressions,
                                 const std::vector<bool>& ascending) const override;
  void Dump(std::ostream& o, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

 private:
  const Table& table_;
  const Index& index_;
  TableStatistics stats_;
  std::vector<Value> begin_key_;
  std::vector<Value> end_key_;
  std::vector<std::pair<std::vector<Value>, std::vector<Value>>> point_ranges_;
  bool ascending_;
  Expression where_;
  std::vector<ColumnName> provided_order_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_INDEX_SCAN_PLAN_HPP
