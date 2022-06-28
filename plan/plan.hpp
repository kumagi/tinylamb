//
// Created by kumagi on 2022/03/01.
//

#ifndef TINYLAMB_PLAN_HPP
#define TINYLAMB_PLAN_HPP

#include <memory>
#include <vector>

#include "executor/executor_base.hpp"
#include "executor/named_expression.hpp"
#include "expression/expression.hpp"
#include "type/schema.hpp"

namespace tinylamb {
class Table;
class TransactionContext;
class TableStatistics;
class Index;

class PlanBase {
 public:
  virtual ~PlanBase() = default;
  virtual Executor EmitExecutor(TransactionContext& txn) const = 0;
  [[nodiscard]] virtual const Schema& GetSchema() const = 0;
  [[nodiscard]] virtual size_t AccessRowCount() const = 0;
  [[nodiscard]] virtual size_t EmitRowCount() const = 0;
  virtual void Dump(std::ostream& o, int indent) const = 0;
  friend std::ostream& operator<<(std::ostream& o, const PlanBase& p) {
    p.Dump(o, 0);
    return o;
  }
};

typedef std::shared_ptr<PlanBase> Plan;
Plan NewFullScanPlan(const Table& table, const TableStatistics& ts);
Plan NewIndexScanPlan(const Table& table, const Index& index,
                      const TableStatistics& ts, const Value& begin,
                      const Value& end, bool ascending,
                      const Expression& where);
Plan NewProductPlan(const Plan& left_src, std::vector<slot_t> left_cols,
                    const Plan& right_src, std::vector<slot_t> right_cols);
Plan NewProductPlan(const Plan& left_src, const Plan& right_src);
Plan NewSelectionPlan(const Plan& src, const Expression& exp,
                      const TableStatistics& stat);
Plan NewProjectionPlan(const Plan& src,
                       std::vector<NamedExpression> project_columns);
Plan NewProjectionPlan(Plan src,
                       const std::vector<std::string>& project_columns);
}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_HPP
