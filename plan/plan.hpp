//
// Created by kumagi on 2022/03/01.
//

#ifndef TINYLAMB_PLAN_HPP
#define TINYLAMB_PLAN_HPP

#include <memory>
#include <vector>

#include "executor/executor_base.hpp"
#include "expression/expression.hpp"
#include "expression/named_expression.hpp"
#include "type/schema.hpp"

namespace tinylamb {
class Table;
class TransactionContext;
class TableStatistics;
class Index;

class PlanBase {
 public:
  PlanBase() = default;
  PlanBase(const PlanBase&) = delete;
  PlanBase& operator=(const PlanBase&) = delete;
  PlanBase(PlanBase&&) = delete;
  PlanBase& operator=(PlanBase&&) = delete;
  virtual ~PlanBase() = default;

  virtual Executor EmitExecutor(TransactionContext& txn) const = 0;

  virtual const TableStatistics& GetStats() const = 0;
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
}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_HPP
