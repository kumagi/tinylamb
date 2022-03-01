//
// Created by kumagi on 2022/03/02.
//

#ifndef TINYLAMB_OPTIMIZER_HPP
#define TINYLAMB_OPTIMIZER_HPP

#include <memory>

#include "database/query_data.hpp"

namespace tinylamb {
class ExecutorBase;
class TransactionContext;

class Optimizer {
 public:
  Optimizer(QueryData query) : query_(std::move(query)) {}

  std::unique_ptr<ExecutorBase> Optimize(TransactionContext& ctx);

 private:
  QueryData query_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_OPTIMIZER_HPP
