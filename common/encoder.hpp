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
#include "common/status_or.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

class Encoder {
 public:
  explicit Encoder(std::ostream& os) : os_(&os) {}
  // Appends the same binary representation without the per-field ostream
  // sentry/virtual stream-buffer dispatch. Aliased string views are copied
  // before appending their length prefix.
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

  // A format error stops further writes. The record must be discarded on
  // failure. Stream I/O errors are reported separately by the ostream.
  [[nodiscard]] Status GetStatus() const {
    if (format_error_ != nullptr) {
      return StatusError(StatusCode::kTooBigData, format_error_);
    }
    return Status::kSuccess;
  }

  template <typename T>
  Encoder& operator<<(const std::vector<T>& vec) {
    if (format_error_ != nullptr) {
      return *this;
    }
    const uint64_t size = vec.size();
    if (size > kMaxSerializedVectorElements) {
      format_error_ = "too many vector elements to encode";
      return *this;
    }
    *this << size;
    for (const auto& elm : vec) {
      *this << elm;
      if (format_error_ != nullptr) {
        break;
      }
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
    if (format_error_ != nullptr) {
      return;
    }
    if (bytes_ != nullptr) {
      bytes_->append(data, size);
    } else {
      os_->write(data, static_cast<std::streamsize>(size));
    }
  }
  std::ostream* os_{nullptr};
  std::string* bytes_{nullptr};
  const char* format_error_{nullptr};
};

template <typename T>
StatusOr<std::string> TryEncode(const T& src) {
  std::string bytes;
  Encoder enc(bytes);
  enc << src;
  RETURN_IF_FAIL(enc.GetStatus());
  return bytes;
}

// For values that are known to fit, e.g. values already checked on insert.
// Callers encoding arbitrary input use TryEncode to keep failures recoverable.
template <typename T>
std::string Encode(const T& src) {
  return TryEncode(src).MoveValue();
}

}  // namespace tinylamb

#endif  // TINYLAMB_ENCODER_HPP
