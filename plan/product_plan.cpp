//
// Created by kumagi on 2022/03/01.
//

#include "plan/product_plan.hpp"

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
  // TODO(kumagi): there will be much more executors to join.
  return std::make_unique<HashJoin>(left_src_.EmitExecutor(ctx), left_cols_,
                                    right_src_.EmitExecutor(ctx), right_cols_);
}

[[nodiscard]] Schema ProductPlan::GetSchema(TransactionContext& ctx) const {
  return left_src_.GetSchema(ctx) + right_src_.GetSchema(ctx);
}

void ProductPlan::Dump(std::ostream& o, int indent) const {
  o << "Product: ";
  left_src_.Dump(o, indent + 2);
  o << "[";
  for (size_t i = 0; i < left_cols_.size(); ++i) {
    if (0 < i) o << ", ";
    o << left_cols_[i];
  }
  o << "]\n" << Indent(indent) << "         ";
  right_src_.Dump(o, indent + 2);
  o << "[";
  for (size_t i = 0; i < right_cols_.size(); ++i) {
    if (0 < i) o << ", ";
    o << right_cols_[i];
  }
  o << "]";
}

}  // namespace tinylamb