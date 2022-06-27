//
// Created by kumagi on 2022/03/01.
//

#include "plan/selection_plan.hpp"

#include <memory>

#include "executor/selection.hpp"
#include "expression/expression.hpp"
#include "table/table_statistics.hpp"

namespace tinylamb {

Executor SelectionPlan::EmitExecutor(TransactionContext& ctx) const {
  return std::make_shared<Selection>(exp_, src_->GetSchema(),
                                     src_->EmitExecutor(ctx));
}

const Schema& SelectionPlan::GetSchema() const { return src_->GetSchema(); }

size_t SelectionPlan::AccessRowCount(TransactionContext& ctx) const {
  return src_->EmitRowCount(ctx);
}

size_t SelectionPlan::EmitRowCount(TransactionContext& ctx) const {
  return static_cast<size_t>(static_cast<double>(src_->EmitRowCount(ctx)) /
                             stats_.ReductionFactor(GetSchema(), exp_));
}

void SelectionPlan::Dump(std::ostream& o, int indent) const {
  o << "Select: [";
  exp_->Dump(o);
  o << "]\n" << Indent(indent + 2);
  src_->Dump(o, indent + 2);
}

}  // namespace tinylamb