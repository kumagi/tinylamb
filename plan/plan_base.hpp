//
// Created by kumagi on 2022/03/01.
//

#ifndef TINYLAMB_PLAN_BASE_HPP
#define TINYLAMB_PLAN_BASE_HPP

#include <memory>

#include "executor/executor_base.hpp"
#include "type/schema.hpp"

namespace tinylamb {
class TransactionContext;

class PlanBase {
 public:
  virtual ~PlanBase() = default;
  virtual std::unique_ptr<ExecutorBase> EmitExecutor(
      TransactionContext& txn) const = 0;
  [[nodiscard]] virtual Schema GetSchema(TransactionContext& txn) const = 0;
  [[nodiscard]] virtual size_t AccessRowCount(
      TransactionContext& txn) const = 0;
  [[nodiscard]] virtual size_t EmitRowCount(TransactionContext& txn) const = 0;
  virtual void Dump(std::ostream& o, int indent) const = 0;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_BASE_HPP
