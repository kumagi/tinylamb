#include "type/column.hpp"

#include <iostream>
#include <utility>

#include "common/encoder.hpp"

namespace tinylamb {

Column::Column(std::string_view name, ValueType type, Constraint cst)
    : name_(name), type_(type), constraint_(std::move(cst)) {}

std::ostream& operator<<(std::ostream& o, const Column& c) {
  o << c.name_;
  if (c.type_ != ValueType::kNull) {
    o << ": " << ValueTypeToString(c.type_);
  }
  if (!c.constraint_.IsNothing()) {
    o << "(" << c.constraint_ << ")";
  }
  return o;
}

Encoder& operator<<(Encoder& a, const Column& c) {
  a << c.name_ << c.type_ << c.constraint_;
  return a;
}

Decoder& operator>>(Decoder& e, Column& c) {
  e >> c.name_ >> c.type_ >> c.constraint_;
  return e;
}

}  // namespace tinylamb

uint64_t std::hash<tinylamb::Column>::operator()(
    const tinylamb::Column& c) const {
  uint64_t result = std::hash<std::string_view>()(c.Name());
  result += std::hash<tinylamb::ValueType>()(c.Type());
  result += std::hash<tinylamb::Constraint>()(c.GetConstraint());
  return result;
}
