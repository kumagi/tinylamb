/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_PLAN_SET_OPERATION_PLAN_HPP
#define TINYLAMB_PLAN_SET_OPERATION_PLAN_HPP

#include <utility>
#include <vector>

#include "common/set_operation.hpp"
#include "plan/plan.hpp"
#include "plan/sort_plan.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class SetOperationPlan final : public PlanBase {
 public:
  SetOperationPlan(std::vector<Plan> children, SetOperationKind operation)
      : children_(std::move(children)),
        operation_(operation),
        schema_(GenerateSchema()) {}
  SetOperationPlan(std::vector<Plan> children, SetOperationKind operation,
                   std::vector<SortKey> order_keys)
      : children_(std::move(children)),
        operation_(operation),
        order_keys_(std::move(order_keys)),
        schema_(GenerateSchema()) {}

  Executor EmitExecutor(TransactionContext& context) const override;
  [[nodiscard]] const Table* ScanSource() const override { return nullptr; }
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return children_.front()->GetStats();
  }
  [[nodiscard]] const Schema& GetSchema() const override { return schema_; }
  [[nodiscard]] size_t AccessRowCount() const override;
  [[nodiscard]] size_t EmitRowCount() const override;
  [[nodiscard]] bool IsOrderedBy(
      const std::vector<Expression>& expressions,
      const std::vector<bool>& ascending) const override;
  void Dump(std::ostream& output, int indent) const override;
  [[nodiscard]] std::string ToString() const override;
  [[nodiscard]] const std::vector<Plan>& Children() const { return children_; }
  [[nodiscard]] SetOperationKind Operation() const { return operation_; }
  [[nodiscard]] const std::vector<SortKey>& OrderKeys() const {
    return order_keys_;
  }

 private:
  [[nodiscard]] Schema GenerateSchema() const;
  std::vector<Plan> children_;
  SetOperationKind operation_{SetOperationKind::kUnionAll};
  std::vector<SortKey> order_keys_;
  Schema schema_;
};

using UnionAllPlan = SetOperationPlan;

}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_SET_OPERATION_PLAN_HPP
