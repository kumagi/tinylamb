//
// Created by kumagi on 2022/02/21.
//

#include "selection.hpp"

#include "common/log_message.hpp"
#include "expression/expression_base.hpp"

namespace tinylamb {

Selection::Selection(std::unique_ptr<ExpressionBase>&& exp, Schema* schema,
                     ExecutorBase* src)
    : exp_(std::move(exp)), schema_(schema), src_(src) {}

bool Selection::Next(Row* dst) {
  Row orig;
  while (src_->Next(&orig)) {
    Value result = exp_->Evaluate(orig, schema_);
    if (result.value.int_value) {
      *dst = orig;
      return true;
    }
  }
  return false;
}

}  // namespace tinylamb