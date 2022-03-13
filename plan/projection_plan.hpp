//
// Created by kumagi on 2022/03/01.
//

#ifndef TINYLAMB_PROJECTION_PLAN_HPP
#define TINYLAMB_PROJECTION_PLAN_HPP

#include <memory>
#include <utility>
#include <vector>

#include "executor/named_expression.hpp"
#include "expression/expression.hpp"
#include "plan/plan_base.hpp"

namespace tinylamb {

class ProjectionPlan : public PlanBase {
 public:
  ProjectionPlan(Plan src, std::vector<NamedExpression> project_columns);
  ProjectionPlan(Plan src, const std::vector<std::string>& project_columns)
      : src_(std::move(std::move(src))) {
    columns_.reserve(project_columns.size());
    for (const auto& col : project_columns) {
      columns_.emplace_back(col);
    }
  }

  ~ProjectionPlan() override = default;

  Executor EmitExecutor(TransactionContext& ctx) const override;
  [[nodiscard]] Schema GetSchema(TransactionContext& ctx) const override;

  [[nodiscard]] size_t AccessRowCount(TransactionContext& txn) const override;
  [[nodiscard]] size_t EmitRowCount(TransactionContext& txn) const override;
  void Dump(std::ostream& o, int indent) const override;

 private:
  Plan src_;
  std::vector<NamedExpression> columns_;
};

}  // namespace tinylamb
#endif  // TINYLAMB_PROJECTION_PLAN_HPP
