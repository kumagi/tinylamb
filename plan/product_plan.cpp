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

ProductPlan::ProductPlan(Plan left_src, std::vector<size_t> left_cols,
                         Plan right_src, std::vector<size_t> right_cols)
    : left_src_(std::move(left_src)),
      right_src_(std::move(right_src)),
      left_cols_(std::move(left_cols)),
      right_cols_(std::move(right_cols)) {}

ProductPlan::ProductPlan(Plan left_src, Plan right_src)
    : left_src_(std::move(left_src)), right_src_(std::move(right_src)) {}

std::unique_ptr<ExecutorBase> ProductPlan::EmitExecutor(
    TransactionContext& ctx) const {
  if (left_cols_.empty() && right_cols_.empty()) {
    return std::make_unique<CrossJoin>(left_src_.EmitExecutor(ctx),
                                       right_src_.EmitExecutor(ctx));
  }
  return std::make_unique<HashJoin>(left_src_.EmitExecutor(ctx), left_cols_,
                                    right_src_.EmitExecutor(ctx), right_cols_);
}

[[nodiscard]] Schema ProductPlan::GetSchema(TransactionContext& ctx) const {
  return left_src_.GetSchema(ctx) + right_src_.GetSchema(ctx);
}

int ProductPlan::AccessRowCount(TransactionContext& ctx) const {
  if (left_cols_.empty() && right_cols_.empty()) {
    return left_src_.AccessRowCount(ctx) +
           (1 + left_src_.EmitRowCount(ctx) * right_src_.AccessRowCount(ctx));
  }
  // Cost of hash join.
  return left_src_.AccessRowCount(ctx) + right_src_.AccessRowCount(ctx);
}

int ProductPlan::EmitRowCount(TransactionContext& ctx) const {
  if (left_cols_.empty() && right_cols_.empty()) {
    return left_src_.EmitRowCount(ctx) * right_src_.EmitRowCount(ctx);
  }
  return std::min(left_src_.EmitRowCount(ctx), right_src_.EmitRowCount(ctx));
}

void ProductPlan::Dump(std::ostream& o, int indent) const {
  o << "Product: ";
  if (!left_cols_.empty() || !right_cols_.empty()) {
    o << "left {";
    for (size_t i = 0; i < left_cols_.size(); ++i) {
      if (0 < i) o << ", ";
      o << left_cols_[i];
    }
    o << "} right {";
    for (size_t i = 0; i < right_cols_.size(); ++i) {
      if (0 < i) o << ", ";
      o << right_cols_[i];
    }
    o << "} ";
  }
  o << "\n" << Indent(indent + 2);
  left_src_.Dump(o, indent + 2);
  o << "\n" << Indent(indent + 2);
  right_src_.Dump(o, indent + 2);
}

}  // namespace tinylamb