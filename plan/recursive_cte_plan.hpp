/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_PLAN_RECURSIVE_CTE_PLAN_HPP
#define TINYLAMB_PLAN_RECURSIVE_CTE_PLAN_HPP

#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "plan/plan.hpp"
#include "query/statement.hpp"
#include "table/table_statistics.hpp"
#include "type/schema.hpp"

namespace tinylamb {

class RecursiveCtePlan final : public PlanBase {
 public:
  RecursiveCtePlan(Plan child, std::string cte_name,
                   std::shared_ptr<const SelectStatement> body,
                   std::optional<RecursiveDepthSpec> depth_spec = std::nullopt,
                   Schema schema = Schema{});

  RecursiveCtePlan(Plan child, Plan recursive_child, std::string cte_name,
                   std::shared_ptr<const SelectStatement> body,
                   std::optional<RecursiveDepthSpec> depth_spec = std::nullopt,
                   Schema schema = Schema{});

  RecursiveCtePlan(const RecursiveCtePlan&) = delete;
  RecursiveCtePlan(RecursiveCtePlan&&) = delete;
  RecursiveCtePlan& operator=(const RecursiveCtePlan&) = delete;
  RecursiveCtePlan& operator=(RecursiveCtePlan&&) = delete;
  ~RecursiveCtePlan() override = default;

  Executor EmitExecutor(TransactionContext& ctx) const override;

  [[nodiscard]] const Table* ScanSource() const override { return nullptr; }
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return stats_;
  }
  [[nodiscard]] const Schema& GetSchema() const override { return schema_; }
  [[nodiscard]] size_t AccessRowCount() const override {
    return child_ ? child_->AccessRowCount() : 1;
  }
  [[nodiscard]] size_t EmitRowCount() const override {
    return (child_ ? child_->EmitRowCount() : 1) * 10;
  }
  [[nodiscard]] const Plan& Child() const { return child_; }
  [[nodiscard]] const Plan& RecursiveChild() const { return recursive_child_; }
  [[nodiscard]] const std::string& CteName() const { return cte_name_; }
  [[nodiscard]] const std::shared_ptr<const SelectStatement>& Body() const {
    return body_;
  }
  [[nodiscard]] const std::optional<RecursiveDepthSpec>& DepthSpec() const {
    return depth_spec_;
  }

  void Dump(std::ostream& output, int indent) const override;
  [[nodiscard]] std::string ToString() const override;

 private:
  Plan child_;
  Plan recursive_child_{nullptr};
  std::string cte_name_;
  std::shared_ptr<const SelectStatement> body_;
  std::optional<RecursiveDepthSpec> depth_spec_;
  Schema schema_;
  TableStatistics stats_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_RECURSIVE_CTE_PLAN_HPP
