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

#ifndef TINYLAMB_DECODER_HPP
#define TINYLAMB_DECODER_HPP

#include <cstdint>
#include <ios>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <vector>

#include "common/constants.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

class Decoder {
 public:
  explicit Decoder(std::istream& is) : is_(&is) {}
  Decoder& operator>>(std::string& str);
  Decoder& operator>>(uint8_t& u8);
  Decoder& operator>>(uint32_t& u32);
  Decoder& operator>>(slot_t& slot);
  Decoder& operator>>(int64_t& i64);
  Decoder& operator>>(uint64_t& u64);
  Decoder& operator>>(double& d);
  Decoder& operator>>(ValueType& v);
  Decoder& operator>>(bool& v);

  template <typename T>
  Decoder& operator>>(std::vector<T>& vec) {
    uint64_t size = 0;
    *this >> size;
    vec.clear();
    constexpr uint64_t kMaxDecodedElements = 1 << 20;
    if (size > kMaxDecodedElements) {
      is_->setstate(std::ios::failbit);
      return *this;
    }
    vec.resize(static_cast<size_t>(size));
    for (uint64_t i = 0; i < size; ++i) {
      *this >> vec[i];
    }
    return *this;
  }

  template <typename T, typename U>
  Decoder& operator>>(std::pair<T, U>& p) {
    *this >> p.first >> p.second;
    return *this;
  }

 private:
  std::istream* is_;
};

// Decode an object from its in-memory encoded form. Throws when the stream
// runs out mid-decode so callers never receive a partially built object.
// T must be default constructible and provide `Decoder& operator>>(Decoder&, T&)`.
template <typename T>
T Decode(std::string_view src) {
  std::string buffer(src);
  std::stringstream ss(buffer);
  Decoder dec(ss);
  T ret;
  dec >> ret;
  if (!ss) {
    throw std::runtime_error("Decode failed: truncated or corrupt input");
  }
  return ret;
}

}  // namespace tinylamb

#endif  // TINYLAMB_DECODER_HPP
