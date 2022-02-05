#include "type/row.hpp"

#include <cstring>
#include <iostream>

#include "page/row_position.hpp"
#include "schema.hpp"
#include "value.hpp"

namespace tinylamb {

size_t Row::Serialize(char* dst) const {
  const char* const original_offset = dst;
  dst += SerializeSlot(dst, values_.size());
  for (const auto& v : values_) {
    dst += v.Serialize(dst);
  }
  return dst - original_offset;
}

size_t Row::Deserialize(const char* src, const Schema& sc) {
  const char* const original_offset = src;
  uint16_t count;
  src += DeserializeSlot(src, &count);
  values_.clear();
  values_.reserve(count);
  for (uint16_t i = 0; i < count; ++i) {
    Value v;
    src += v.Deserialize(src, sc.GetColumn(i).Type());
    values_.push_back(v);
  }
  return src - original_offset;
}

[[nodiscard]] size_t Row::Size() const {
  size_t ret = sizeof(uint16_t);
  for (const auto& v : values_) {
    ret += v.Size();
  }
  return ret;
}

bool Row::operator==(const Row& rhs) const { return values_ == rhs.values_; }

std::ostream& operator<<(std::ostream& o, const Row& r) {
  o << "[";
  for (size_t i = 0; i < r.values_.size(); ++i) {
    if (0 < i) o << ", ";
    o << r.values_[i];
  }
  o << "]";
  return o;
}

}  // namespace tinylamb

uint64_t std::hash<tinylamb::Row>::operator()(const tinylamb::Row& row) {
  uint64_t ret = 0;
  for (const auto& v : row.values_) {
    ret += std::hash<tinylamb::Value>()(v);
  }
  return ret;
}