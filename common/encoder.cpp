//
// Created by kumagi on 2022/02/26.
//

#include "encoder.hpp"

#include <cassert>

namespace tinylamb {

Encoder& Encoder::operator<<(std::string_view sv) {
  assert(sv.size() < std::numeric_limits<bin_size_t>::max());
  const auto sz = static_cast<bin_size_t>(sv.size());
  os_->write(reinterpret_cast<const char*>(&sz), sizeof(bin_size_t));
  os_->write(sv.data(), sv.size());
  return *this;
}

Encoder& Encoder::operator<<(uint8_t u8) {
  os_->write(reinterpret_cast<const char*>(&u8), sizeof(u8));
  return *this;
}

Encoder& Encoder::operator<<(slot_t slot) {
  os_->write(reinterpret_cast<const char*>(&slot), sizeof(slot));
  return *this;
}

Encoder& Encoder::operator<<(page_id_t pid) {
  os_->write(reinterpret_cast<const char*>(&pid), sizeof(pid));
  return *this;
}

Encoder& Encoder::operator<<(int64_t i64) {
  os_->write(reinterpret_cast<const char*>(&i64), sizeof(i64));
  return *this;
}

Encoder& Encoder::operator<<(double d) {
  os_->write(reinterpret_cast<const char*>(&d), sizeof(d));
  return *this;
}

Encoder& Encoder::operator<<(ValueType v) {
  os_->write(reinterpret_cast<const char*>(&v), sizeof(v));
  return *this;
}

Encoder& Encoder::operator<<(bool v) {
  os_->write(reinterpret_cast<const char*>(&v), sizeof(v));
  return *this;
}

}  // namespace tinylamb