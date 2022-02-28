//
// Created by kumagi on 2022/03/01.
//

#include "plan/selection_plan.hpp"

#include "executor/selection.hpp"
#include "expression/expression_base.hpp"

namespace tinylamb {
std::unique_ptr<ExecutorBase> SelectionPlan::EmitExecutor(
    tinylamb::Transaction& txn) const {
  return std::make_unique<Selection>(std::move(exp_), src_->GetSchema(),
                                     src_->EmitExecutor(txn));
}

Schema SelectionPlan::GetSchema() const { return src_->GetSchema(); }

}  // namespace tinylamb