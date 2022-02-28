//
// Created by kumagi on 2022/02/26.
//

#include "decoder.hpp"

namespace tinylamb {

Decoder& Decoder::operator>>(std::string& str) {
  bin_size_t size;
  is_->read(reinterpret_cast<char*>(&size), sizeof(size));
  str.resize(size);
  is_->read(str.data(), size);
  return *this;
}

Decoder& Decoder::operator>>(uint8_t& u8) {
  is_->read(reinterpret_cast<char*>(&u8), sizeof(u8));
  return *this;
}

Decoder& Decoder::operator>>(slot_t& slot) {
  is_->read(reinterpret_cast<char*>(&slot), sizeof(slot));
  return *this;
}

Decoder& Decoder::operator>>(page_id_t& pid) {
  is_->read(reinterpret_cast<char*>(&pid), sizeof(pid));
  return *this;
}

Decoder& Decoder::operator>>(int64_t& i64) {
  is_->read(reinterpret_cast<char*>(&i64), sizeof(i64));
  return *this;
}

Decoder& Decoder::operator>>(double& d) {
  is_->read(reinterpret_cast<char*>(&d), sizeof(d));
  return *this;
}

Decoder& Decoder::operator>>(ValueType& v) {
  is_->read(reinterpret_cast<char*>(&v), sizeof(v));
  return *this;
}

}  // namespace tinylamb