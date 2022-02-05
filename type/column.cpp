#include "type/column.hpp"

#include <iostream>

namespace tinylamb {

Column::Column(std::string_view name, ValueType type, Constraint cst)
    : name_(name), type_(type), constraint_(cst) {}

size_t Column::Serialize(char* dst) const {
  char* const original_position = dst;
  dst += SerializeStringView(dst, name_);
  *dst++ = static_cast<char>(type_);
  dst += constraint_.Serialize(dst);
  return dst - original_position;
}

size_t Column::Deserialize(const char* src) {
  const char* const original_position = src;
  std::string_view n;
  src += DeserializeStringView(src, &n);
  name_ = n;
  type_ = static_cast<ValueType>(*src++);
  src += constraint_.Deserialize(src);
  return src - original_position;
}

size_t Column::Size() const {
  size_t ret = SerializeSize(name_);
  ret += sizeof(ValueType);
  ret += constraint_.Size();
  return ret;
}

bool Column::operator==(const Column& rhs) const {
  return name_ == rhs.name_ && type_ == rhs.type_ &&
         constraint_ == rhs.constraint_;
}

std::ostream& operator<<(std::ostream& o, const Column& c) {
  o << c.name_ << ": " << ValueTypeToString(c.type_);
  if (!c.constraint_.IsNothing()) {
    o << " (" << c.constraint_ << ")";
  }
  return o;
}

}  // namespace tinylamb

uint64_t std::hash<tinylamb::Column>::operator()(
    const tinylamb::Column& c) const {
  uint64_t result = std::hash<std::string_view>()(c.Name());
  result += std::hash<tinylamb::ValueType>()(c.Type());
  result += std::hash<tinylamb::Constraint>()(c.GetConstraint());
  return result;
}
