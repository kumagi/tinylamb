//
// Created by kumagi on 2022/03/01.
//

#include "plan/selection_plan.hpp"

#include <memory>

#include "executor/selection.hpp"
#include "expression/expression_base.hpp"
#include "table/table_statistics.hpp"

namespace tinylamb {

std::unique_ptr<ExecutorBase> SelectionPlan::EmitExecutor(
    TransactionContext& ctx) const {
  return std::make_unique<Selection>(exp_, src_.GetSchema(ctx),
                                     src_.EmitExecutor(ctx));
}

Schema SelectionPlan::GetSchema(TransactionContext& ctx) const {
  return src_.GetSchema(ctx);
}

size_t SelectionPlan::AccessRowCount(TransactionContext& ctx) const {
  return src_.EmitRowCount(ctx);
}

size_t SelectionPlan::EmitRowCount(TransactionContext& ctx) const {
  return src_.EmitRowCount(ctx) /
         stats_.ReductionFactor(GetSchema(ctx), exp_.exp_.get());
}

void SelectionPlan::Dump(std::ostream& o, int indent) const {
  o << "Select: [";
  exp_.Dump(o);
  o << "]\n" << Indent(indent + 2);
  src_.Dump(o, indent + 2);
}

}  // namespace tinylamb