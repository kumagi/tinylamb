//
// Created by kumagi on 2022/02/21.
//

#include "selection.hpp"

#include <utility>

#include "common/log_message.hpp"
#include "expression/expression_base.hpp"

namespace tinylamb {

Selection::Selection(Expression& exp, Schema schema,
                     std::unique_ptr<ExecutorBase>&& src)
    : exp_(exp), schema_(std::move(schema)), src_(std::move(src)) {}

bool Selection::Next(Row* dst) {
  Row orig;
  while (src_->Next(&orig)) {
    Value result = exp_.Evaluate(orig, &schema_);
    if (result.value.int_value) {
      *dst = orig;
      return true;
    }
  }
  return false;
}

void Selection::Dump(std::ostream& o, int indent) const {
  o << "Selection: " << exp_ << "\n" << Indent(indent + 2);
  src_->Dump(o, indent + 2);
}

}  // namespace tinylamb