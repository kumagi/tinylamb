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


#include "projection.hpp"

#include "common/log_message.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

bool Projection::Next(Row* dst, RowPosition* rp) {
  Row orig;
  dst->Clear();
  if (!src_->Next(&orig, rp)) {
    return false;
  }
  std::vector<Value> result;
  result.reserve(expressions_.size());
  for (const auto& exp : expressions_) {
    result.push_back(exp.expression->Evaluate(orig, input_schema_));
  }
  *dst = Row(std::move(result));
  return true;
}

void Projection::Dump(std::ostream& o, int indent) const {
  o << "Projection: [";
  for (size_t i = 0; i < expressions_.size(); ++i) {
    if (0 < i) {
      o << ", ";
    }
    o << expressions_[i];
  }
  o << "]\n" << Indent(indent + 2);
  src_->Dump(o, indent + 2);
}

}  // namespace tinylamb