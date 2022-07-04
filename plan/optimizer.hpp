//
// Created by kumagi on 2022/03/02.
//

#ifndef TINYLAMB_OPTIMIZER_HPP
#define TINYLAMB_OPTIMIZER_HPP

#include <memory>

#include "executor/executor_base.hpp"
#include "query/query_data.hpp"

namespace tinylamb {
class ExecutorBase;
class TransactionContext;

class Optimizer {
 public:
  explicit Optimizer() = default;

  static Status Optimize(const QueryData& query, TransactionContext& ctx,
                         Schema& schema, Executor& exec);
};

}  // namespace tinylamb

#endif  // TINYLAMB_OPTIMIZER_HPP
