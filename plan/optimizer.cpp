//
// Created by kumagi on 2022/03/02.
//

#include "optimizer.hpp"

#include "database/transaction_context.hpp"
#include "plan.hpp"

namespace tinylamb {

std::unique_ptr<ExecutorBase> Optimizer::Optimize(TransactionContext& ctx) {
  if (query_.from_.empty()) throw std::runtime_error("No table specified");

  Plan result = Plan::FullScan(query_.from_[0]);
  for (size_t i = 1; i < query_.from_.size(); ++i) {
    result = Plan::Product(result, Plan::FullScan(query_.from_[i]));
  }
  result = Plan::Selection(result, query_.where_);
  result = Plan::Projection(result, query_.select_);
  return result.EmitExecutor(ctx);
}

}  // namespace tinylamb