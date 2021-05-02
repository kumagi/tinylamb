//
// Created by kumagi on 22/07/07.
//
#include "type/column_name.hpp"

#include <ostream>
#include <tuple>

#include "common/decoder.hpp"
#include "common/encoder.hpp"
namespace tinylamb {

std::ostream& operator<<(std::ostream& o, const ColumnName& c) {
  o << c.ToString();
  return o;
}

std::string ColumnName::ToString() const {
  return schema.empty() ? name : schema + "." + name;
}

bool ColumnName::operator<(const ColumnName& rhs) const {
  return std::tie(schema, name) < std::tie(rhs.schema, rhs.name);
}

Encoder& operator<<(Encoder& a, const ColumnName& c) {
  a << c.schema << c.name;
  return a;
}

Decoder& operator>>(Decoder& e, ColumnName& c) {
  e >> c.schema >> c.name;
  return e;
}

}  // namespace tinylamb
uint64_t std::hash<tinylamb::ColumnName>::operator()(
    const tinylamb::ColumnName& c) const {
  uint64_t result = std::hash<std::string_view>()(c.schema);
  result = std::hash<std::string_view>()(c.name);
  return result;
}
