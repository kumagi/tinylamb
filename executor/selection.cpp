/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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