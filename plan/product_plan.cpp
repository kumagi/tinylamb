//
// Created by kumagi on 2022/03/01.
//

#include "plan/product_plan.hpp"

#include "executor/hash_join.hpp"
#include "type/schema.hpp"

namespace tinylamb {
std::unique_ptr<ExecutorBase> ProductPlan::EmitExecutor(
    Transaction& txn) const {
  // TODO(kumagi): there will be much more executors to join.
  return std::make_unique<HashJoin>(left_src_->EmitExecutor(txn), left_cols_,
                                    right_src_->EmitExecutor(txn), right_cols_);
}

[[nodiscard]] Schema ProductPlan::GetSchema() const {
  return left_src_->GetSchema() + right_src_->GetSchema();
}
}  // namespace tinylamb