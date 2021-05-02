//
// Created by kumagi on 2022/03/02.
//

#ifndef TINYLAMB_OPTIMIZER_HPP
#define TINYLAMB_OPTIMIZER_HPP

#include <memory>

#include "common/status_or.hpp"
#include "executor/executor_base.hpp"
#include "query/query_data.hpp"

namespace tinylamb {
class ExecutorBase;
class TransactionContext;
class PlanBase;
typedef std::shared_ptr<PlanBase> Plan;

class Optimizer {
 public:
  explicit Optimizer() = default;

  static StatusOr<Plan> Optimize(const QueryData& query,
                                 TransactionContext& ctx);
};

}  // namespace tinylamb

#endif  // TINYLAMB_OPTIMIZER_HPP
