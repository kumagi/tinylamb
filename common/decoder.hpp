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
#include <istream>
#include <memory>
#include <streambuf>
#include <string>
#include <string_view>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
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
    try {
      vec.resize(static_cast<size_t>(size));
    } catch (const std::bad_alloc&) {
      // A corrupt length (or a large element type) can make even the bounded
      // resize fail; surface it as a stream failure at the Status boundary.
      is_->setstate(std::ios::failbit);
      return *this;
    }
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

  // Sticky failure signal for decode helpers that must reject a value after
  // it has already been read (unknown discriminants, torn payloads).
  void Fail() { is_->setstate(std::ios::failbit); }
  [[nodiscard]] bool Failed() const { return is_->fail(); }
  // True when no more bytes remain. Lets decoders read optional trailing
  // fields written by newer encoders without tripping failbit on old
  // payloads (e.g. ColumnStats::correlation_).
  [[nodiscard]] bool AtEnd() {
    return is_->peek() == std::char_traits<char>::eof();
  }

 private:
  std::istream* is_;
};

namespace detail {

// Non-owning get area over a caller-owned byte range so Decode() can parse
// in place: the previous std::string copy plus std::stringstream double-
// copied every payload (index-key decode runs per entry on scan hot paths).
// The default underflow() reports EOF once the area is consumed, which is
// exactly the failbit behaviour the istream-based Decoder relies on, and
// peek()-at-end keeps AtEnd()'s optional-trailing-field contract intact.
class StringViewBuffer final : public std::streambuf {
 public:
  explicit StringViewBuffer(std::string_view src) {
    if (!src.empty()) {
      auto* data = const_cast<char*>(
          src.data());  // NOLINT(cppcoreguidelines-pro-type-const-cast)
      setg(data, data, data + src.size());
    }
  }
};

}  // namespace detail

// Decode an object from its in-memory encoded form. Returns a Corrupt status
// when the stream runs out mid-decode so callers never receive a partially
// built object. T must be default constructible and provide `Decoder&
// operator>>(Decoder&, T&)`. |src| must outlive the call (it always does:
// decoding completes before Decode returns).
template <typename T>
StatusOr<T> Decode(std::string_view src) {
  detail::StringViewBuffer buffer(src);
  std::istream is(&buffer);
  Decoder dec(is);
  T ret;
  dec >> ret;
  if (!is) {
    return StatusError(StatusCode::kCorrupt,
                       "Decode failed: truncated or corrupt input");
  }
  return ret;
}

}  // namespace tinylamb

#endif  // TINYLAMB_DECODER_HPP
