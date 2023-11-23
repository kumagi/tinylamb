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

#ifndef TINYLAMB_INDEX_KEY_HPP
#define TINYLAMB_INDEX_KEY_HPP

#include <cstring>
#include <string>

#include "common/constants.hpp"
#include "common/debug.hpp"
#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "common/log_message.hpp"
#include "common/serdes.hpp"
#include "common/status_or.hpp"

namespace tinylamb {

class IndexKey final {
  IndexKey(bool plus_infinity, bool minus_infinity, std::string_view key)
      : is_plus_infinity_(plus_infinity),
        is_minus_infinity_(minus_infinity),
        key_(key) {}
  enum EncodedIndexKeyType : uint8_t {
    kMinusInfinity = 0,
    kPlusInfinity = 1,
    kString = 2
  };

 public:
  IndexKey() = default;
  explicit IndexKey(std::string_view key)
      : is_plus_infinity_(false), is_minus_infinity_(false), key_(key) {}
  static IndexKey PlusInfinity() { return {true, false, ""}; }
  static IndexKey MinusInfinity() { return {false, true, ""}; }
  static IndexKey Deserialize(const char* src) {
    bin_size_t size = 0;
    memcpy(&size, src, sizeof(bin_size_t));
    return IndexKey(std::string_view{src + sizeof(bin_size_t), size});
  }
  IndexKey(const IndexKey&) = default;
  IndexKey(IndexKey&&) = default;
  IndexKey& operator=(const IndexKey&) = default;
  IndexKey& operator=(IndexKey&&) = default;
  bool operator==(const IndexKey&) const = default;
  ~IndexKey() = default;
  bool operator<(const IndexKey& rhs) const {
    if ((IsPlusInfinity() && rhs.IsPlusInfinity()) ||
        (IsMinusInfinity() && rhs.IsMinusInfinity())) {
      return false;  // non comparable.
    }
    if (IsPlusInfinity() || rhs.IsMinusInfinity()) {
      return false;
    }
    if (rhs.IsPlusInfinity() || IsMinusInfinity()) {
      return true;
    }
    return key_ < rhs.key_;
  }
  bool operator<(std::string_view rhs) const {
    if (IsPlusInfinity()) {
      return false;
    }
    if (IsMinusInfinity()) {
      return true;
    }
    return key_ < rhs;
  }
  friend bool operator<(std::string_view lhs, const IndexKey& rhs) {
    if (rhs.IsPlusInfinity()) {
      return true;
    }
    if (rhs.IsMinusInfinity()) {
      return false;
    }
    return lhs < rhs.key_;
  }

  friend Encoder& operator<<(Encoder& e, const IndexKey& key) {
    if (key.is_plus_infinity_) {
      e << EncodedIndexKeyType::kPlusInfinity;
    } else if (key.is_minus_infinity_) {
      e << EncodedIndexKeyType::kMinusInfinity;
    } else {
      e << EncodedIndexKeyType::kString << key.key_;
    }
    return e;
  }
  friend Decoder& operator>>(Decoder& d, IndexKey& key) {
    EncodedIndexKeyType type{};
    d >> reinterpret_cast<uint8_t&>(type);
    switch (type) {
      case EncodedIndexKeyType::kPlusInfinity:
        key = {true, false, ""};
        break;
      case EncodedIndexKeyType::kMinusInfinity:
        key = {false, true, ""};
        break;
      case EncodedIndexKeyType::kString: {
        std::string decoded_key;
        d >> decoded_key;
        key = {false, false, decoded_key};
      }
    }
    return d;
  }

  [[nodiscard]] bool IsPlusInfinity() const { return is_plus_infinity_; }
  [[nodiscard]] bool IsMinusInfinity() const { return is_minus_infinity_; }
  [[nodiscard]] bool IsNotInfinity() const {
    return !is_minus_infinity_ && !is_plus_infinity_;
  }
  [[nodiscard]] StatusOr<std::string_view> GetKey() const {
    if (is_plus_infinity_ || is_minus_infinity_) {
      return Status::kIsInfinity;
    }
    return {key_};
  }
  friend std::ostream& operator<<(std::ostream& o, const IndexKey& key) {
    if (key.is_minus_infinity_) {
      o << "-(inf)";
    } else if (key.is_plus_infinity_) {
      o << "+(inf)";
    } else {
      o << OmittedString(key.key_, 5);
    }
    return o;
  }

 private:
  bool is_plus_infinity_{false};
  bool is_minus_infinity_{false};
  std::string key_;
};

inline size_t SerializeSize(const IndexKey& ik) {
  if (ik.IsMinusInfinity() || ik.IsPlusInfinity()) {
    return 0;
  }
  ASSIGN_OR_CRASH(std::string_view, key, ik.GetKey());
  return sizeof(bin_size_t) + key.size();
}

inline std::string SerializeIndexKey(const IndexKey& ik) {
  assert(ik.IsNotInfinity());
  std::string ret(SerializeSize(ik), 0);
  SerializeStringView(ret.data(), ik.GetKey().Value());
  return ret;
}

}  // namespace tinylamb

namespace std {

template <>
class hash<tinylamb::IndexKey> {
 public:
  uint64_t operator()(const ::tinylamb::IndexKey& ik) const {
    uint64_t ret = 0xcafebabe;
    ret += std::hash<bool>()(ik.IsPlusInfinity());
    ret += std::hash<bool>()(ik.IsMinusInfinity());
    tinylamb::StatusOr<std::string_view> optional_key = ik.GetKey();
    if (optional_key.HasValue()) {
      ret += std::hash<std::string_view>()(optional_key.Value());
    }
    return ret;
  }
};

}  // namespace std

#endif  // TINYLAMB_INDEX_KEY_HPP
