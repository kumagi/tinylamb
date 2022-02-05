//
// Created by kumagi on 2022/02/06.
//

#include "type/constraint.hpp"

#include "type/value.hpp"

namespace tinylamb {

size_t Constraint::Serialize(char* pos) const {
  *reinterpret_cast<ConstraintType*>(pos++) = ctype;
  switch (ctype) {
    case kDefault:
    case kForeign:
    case kCheck:
      *reinterpret_cast<ValueType*>(pos++) = value.type;
      return sizeof(ConstraintType) + sizeof(ValueType) + value.Serialize(pos);

    default:
      return sizeof(ctype);
  }
}

size_t Constraint::Deserialize(const char* src) {
  ctype = *reinterpret_cast<const ConstraintType*>(src++);
  switch (ctype) {
    case kDefault:
    case kForeign:
    case kCheck: {
      ValueType type = *reinterpret_cast<const ValueType*>(src++);
      return sizeof(ConstraintType) + sizeof(ValueType) +
             value.Deserialize(src, type);
    }

    default:
      return sizeof(ctype);
  }
}

size_t Constraint::Size() const {
  switch (ctype) {
    case kDefault:
    case kForeign:
    case kCheck:
      return sizeof(ConstraintType) + sizeof(ValueType) + value.Size();

    default:
      return sizeof(ConstraintType);
  }
}

bool Constraint::operator==(const Constraint& rhs) const {
  if (ctype != rhs.ctype) return false;
  switch (ctype) {
    case kDefault:
    case kForeign:
    case kCheck:
      return value == rhs.value;

    default:
      return true;
  }
}

std::ostream& operator<<(std::ostream& o, const Constraint& c) {
  switch (c.ctype) {
    case tinylamb::Constraint::kNothing:
      o << "(No constraint)";
      break;
    case tinylamb::Constraint::kNotNull:
      o << "NOT NULL";
      break;
    case tinylamb::Constraint::kUnique:
      o << "UNIQUE";
      break;
    case tinylamb::Constraint::kPrimary:
      o << "PRIMARY KEY";
      break;
    case tinylamb::Constraint::kIndex:
      o << "INDEX";
      break;

    case tinylamb::Constraint::kDefault:
      o << "DEFAULT(" << c.value << ")";
      break;
    case tinylamb::Constraint::kForeign:
      o << "FOREIGN(" << c.value << ")";
      break;
    case tinylamb::Constraint::kCheck:
      o << "CHECK(" << c.value << ")";
      break;
  }
  return o;
}

}  // namespace tinylamb

uint64_t std::hash<tinylamb::Constraint>::operator()(
    const tinylamb::Constraint& c) const {
  uint64_t result = std::hash<uint8_t>()(c.ctype);
  switch (c.ctype) {
    case tinylamb::Constraint::kDefault:
    case tinylamb::Constraint::kForeign:
    case tinylamb::Constraint::kCheck:
      result += std::hash<tinylamb::Value>()(c.value);

    default:
      break;
  }
  return result;
}