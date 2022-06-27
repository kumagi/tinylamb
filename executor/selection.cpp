//
// Created by kumagi on 2022/02/21.
//

#include "selection.hpp"

#include <utility>

#include "common/log_message.hpp"
#include "expression/expression.hpp"

namespace tinylamb {

Selection::Selection(Expression exp, Schema schema, Executor src)
    : exp_(std::move(exp)), schema_(std::move(schema)), src_(std::move(src)) {}

bool Selection::Next(Row* dst, RowPosition* rp) {
  Row orig;
  while (src_->Next(&orig, rp)) {
    if (exp_->Evaluate(orig, schema_).value.int_value != 0) {
      *dst = orig;
      return true;
    }
  }
  return false;
}

void Selection::Dump(std::ostream& o, int indent) const {
  o << "Selection: " << *exp_ << "\n" << Indent(indent + 2);
  src_->Dump(o, indent + 2);
}

}  // namespace tinylamb