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

Decoder& Decoder::operator>>(int64_t& i64) {
  is_->read(reinterpret_cast<char*>(&i64), sizeof(i64));
  return *this;
}

Decoder& Decoder::operator>>(uint64_t& u64) {
  is_->read(reinterpret_cast<char*>(&u64), sizeof(u64));
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

Decoder& Decoder::operator>>(bool& v) {
  is_->read(reinterpret_cast<char*>(&v), sizeof(v));
  return *this;
}

}  // namespace tinylamb