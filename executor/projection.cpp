//
// Created by kumagi on 2022/02/21.
//

#include "projection.hpp"

#include "common/log_message.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

bool Projection::Next(Row* dst) {
  Row orig;
  dst->Clear();
  if (!src_->Next(&orig)) return false;
  std::vector<Value> result;
  result.reserve(expressions_.size());
  for (const auto& exp : expressions_) {
    result.push_back(exp.expression.Evaluate(orig, &input_schema_));
  }
  *dst = Row(std::move(result));
  return true;
}

void Projection::Dump(std::ostream& o, int indent) const {
  o << "Projection: [";
  for (int i = 0; i < expressions_.size(); ++i) {
    if (0 < i) o << ", ";
    o << expressions_[i];
  }
  o << "]\n" << Indent(indent + 2);
  src_->Dump(o, indent + 2);
}

}  // namespace tinylamb