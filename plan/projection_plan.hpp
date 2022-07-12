//
// Created by kumagi on 2022/03/01.
//

#ifndef TINYLAMB_PROJECTION_PLAN_HPP
#define TINYLAMB_PROJECTION_PLAN_HPP

#include <memory>
#include <utility>
#include <vector>

#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "plan/plan.hpp"
#include "table/table_statistics.hpp"

namespace tinylamb {

class ProjectionPlan : public PlanBase {
 public:
  ProjectionPlan(Plan src, std::vector<NamedExpression> project_columns);
  ProjectionPlan(Plan src, const std::vector<ColumnName>& project_columns);
  ProjectionPlan(const ProjectionPlan&) = delete;
  ProjectionPlan(ProjectionPlan&&) = delete;
  ProjectionPlan& operator=(const ProjectionPlan&) = delete;
  ProjectionPlan& operator=(ProjectionPlan&&) = delete;
  ~ProjectionPlan() override = default;

  Executor EmitExecutor(TransactionContext& ctx) const override;
  [[nodiscard]] const Schema& GetSchema() const override;
  [[nodiscard]] const TableStatistics& GetStats() const override {
    return stats_;
  }

  [[nodiscard]] size_t AccessRowCount() const override;
  [[nodiscard]] size_t EmitRowCount() const override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  [[nodiscard]] Schema CalcSchema() const;

  Plan src_;
  std::vector<NamedExpression> columns_;
  Schema output_schema_;
  TableStatistics stats_;
};

}  // namespace tinylamb
#endif  // TINYLAMB_PROJECTION_PLAN_HPP
