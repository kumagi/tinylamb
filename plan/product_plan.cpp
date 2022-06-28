//
// Created by kumagi on 2022/03/01.
//

#include "plan/product_plan.hpp"

#include <iostream>
#include <utility>

#include "executor/cross_join.hpp"
#include "executor/hash_join.hpp"
#include "type/schema.hpp"

namespace tinylamb {

ProductPlan::ProductPlan(Plan left_src, std::vector<slot_t> left_cols,
                         Plan right_src, std::vector<slot_t> right_cols)
    : left_src_(std::move(left_src)),
      right_src_(std::move(right_src)),
      left_cols_(std::move(left_cols)),
      right_cols_(std::move(right_cols)),
      output_schema_(left_src_->GetSchema() + right_src_->GetSchema()) {}

ProductPlan::ProductPlan(Plan left_src, Plan right_src)
    : left_src_(std::move(left_src)), right_src_(std::move(right_src)) {}

Executor ProductPlan::EmitExecutor(TransactionContext& ctx) const {
  if (left_cols_.empty() && right_cols_.empty()) {
    return std::make_shared<CrossJoin>(left_src_->EmitExecutor(ctx),
                                       right_src_->EmitExecutor(ctx));
  }
  return std::make_shared<HashJoin>(left_src_->EmitExecutor(ctx), left_cols_,
                                    right_src_->EmitExecutor(ctx), right_cols_);
}

[[nodiscard]] const Schema& ProductPlan::GetSchema() const {
  return output_schema_;
}

size_t ProductPlan::AccessRowCount() const {
  if (left_cols_.empty() && right_cols_.empty()) {
    return left_src_->AccessRowCount() +
           (1 + left_src_->EmitRowCount() * right_src_->AccessRowCount());
  }
  // Cost of hash join.
  return left_src_->AccessRowCount() + right_src_->AccessRowCount();
}

size_t ProductPlan::EmitRowCount() const {
  if (left_cols_.empty() && right_cols_.empty()) {
    return left_src_->EmitRowCount() * right_src_->EmitRowCount();
  }
  return std::min(left_src_->EmitRowCount(), right_src_->EmitRowCount());
}

void ProductPlan::Dump(std::ostream& o, int indent) const {
  o << "Product: (estimated cost: " << EmitRowCount() << ")";
  if (!left_cols_.empty() || !right_cols_.empty()) {
    o << "left {";
    for (size_t i = 0; i < left_cols_.size(); ++i) {
      if (0 < i) {
        o << ", ";
      }
      o << left_cols_[i];
    }
    o << "} right {";
    for (size_t i = 0; i < right_cols_.size(); ++i) {
      if (0 < i) {
        o << ", ";
      }
      o << right_cols_[i];
    }
    o << "} ";
  }
  o << "\n" << Indent(indent + 2);
  left_src_->Dump(o, indent + 2);
  o << "\n" << Indent(indent + 2);
  right_src_->Dump(o, indent + 2);
}

}  // namespace tinylamb