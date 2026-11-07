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

#ifndef TINYLAMB_ENCODER_HPP
#define TINYLAMB_ENCODER_HPP

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

class Encoder {
 public:
  explicit Encoder(std::ostream& os) : os_(&os) {}
  // Appends the same binary representation without the per-field ostream
  // sentry/virtual stream-buffer dispatch. The destination must not also
  // back a string_view being encoded.
  explicit Encoder(std::string& bytes) : bytes_(&bytes) {}
  Encoder& operator<<(std::string_view sv);
  Encoder& operator<<(uint8_t u8);
  Encoder& operator<<(uint32_t u32);
  Encoder& operator<<(slot_t slot);
  Encoder& operator<<(int64_t i64);
  Encoder& operator<<(uint64_t u64);
  Encoder& operator<<(double d);
  Encoder& operator<<(ValueType v);
  Encoder& operator<<(bool v);

  template <typename T>
  Encoder& operator<<(const std::vector<T>& vec) {
    uint64_t size = vec.size();
    *this << size;
    for (const auto& elm : vec) {
      *this << elm;
    }
    return *this;
  }

  template <typename T, typename U>
  Encoder& operator<<(const std::pair<T, U>& p) {
    *this << p.first << p.second;
    return *this;
  }

 private:
  void Write(const char* data, size_t size) {
    if (bytes_) {
      bytes_->append(data, size);
    } else {
      os_->write(data, static_cast<std::streamsize>(size));
    }
  }
  std::ostream* os_{nullptr};
  std::string* bytes_{nullptr};
};

template <typename T>
std::string Encode(const T& src) {
  std::string bytes;
  Encoder enc(bytes);
  enc << src;
  return bytes;
}

}  // namespace tinylamb

#endif  // TINYLAMB_ENCODER_HPP
