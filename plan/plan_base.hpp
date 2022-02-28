//
// Created by kumagi on 2022/03/01.
//

#ifndef TINYLAMB_PLAN_BASE_HPP
#define TINYLAMB_PLAN_BASE_HPP

#include <memory>

#include "executor/executor_base.hpp"

namespace tinylamb {
class Transaction;

class PlanBase {
 public:
  virtual ~PlanBase() = default;
  virtual std::unique_ptr<ExecutorBase> EmitExecutor(
      Transaction& txn) const = 0;
  [[nodiscard]] virtual Schema GetSchema() const = 0;
};

}  // namespace tinylamb

#endif  // TINYLAMB_PLAN_BASE_HPP
