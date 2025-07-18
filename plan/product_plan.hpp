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

#ifndef TINYLAMB_PRODUCT_PLAN_HPP
#define TINYLAMB_PRODUCT_PLAN_HPP

#include "expression/expression.hpp"
#include "plan/plan.hpp"
#include "table/table_statistics.hpp"

namespace tinylamb {
class ExecutorBase;

class ProductPlan final : public PlanBase {
 public:
  ProductPlan(Plan left_src, std::vector<ColumnName> left_cols, Plan right_src,
              std::vector<ColumnName> right_cols);
  ProductPlan(Plan left_src, std::vector<ColumnName> left_cols,
              const Table& right_tbl, const Index& idx,
              std::vector<ColumnName> right_cols,
              const TableStatistics& right_ts);
  ProductPlan(Plan left_src, Plan right_src);
  ProductPlan(const ProductPlan&) = delete;
  ProductPlan(ProductPlan&&) = delete;
  ProductPlan& operator=(const ProductPlan&) = delete;
  ProductPlan& operator=(ProductPlan&&) = delete;
  ~ProductPlan() override = default;

  Executor EmitExecutor(TransactionContext& ctx) const override;

  [[nodiscard]] const Table* ScanSource() const override { return nullptr; };
  [[nodiscard]] const Schema& GetSchema() const override;
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return stats_;
  }

  [[nodiscard]] size_t AccessRowCount() const override;
  [[nodiscard]] size_t EmitRowCount() const override;
  void Dump(std::ostream& o, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

 private:
  Plan left_src_;
  Plan right_src_;
  std::vector<ColumnName> left_cols_;
  std::vector<ColumnName> right_cols_;
  const Table* right_tbl_;
  const Index* right_idx_;
  const TableStatistics* right_ts_;
  Schema output_schema_;
  TableStatistics stats_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PRODUCT_PLAN_HPP
