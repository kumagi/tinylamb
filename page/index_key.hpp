//
// Created by kumagi on 22/08/01.
//

#ifndef TINYLAMB_INDEX_KEY_HPP
#define TINYLAMB_INDEX_KEY_HPP

#include <cstring>
#include <string>

#include "common/debug.hpp"
#include "common/decoder.hpp"
#include "common/encoder.hpp"
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
  IndexKey() {}
  explicit IndexKey(std::string_view key)
      : is_plus_infinity_(false), is_minus_infinity_(false), key_(key) {}
  static IndexKey PlusInfinity() { return {true, false, ""}; }
  static IndexKey MinusInfinity() { return {false, true, ""}; }
  static IndexKey Deserialize(const char* src) {
    bin_size_t size;
    memcpy(&size, src, sizeof(bin_size_t));
    return IndexKey(std::string_view{src + sizeof(bin_size_t), size});
  }
  IndexKey(const IndexKey&) = default;
  IndexKey(IndexKey&&) = default;
  IndexKey& operator=(const IndexKey&) = default;
  IndexKey& operator=(IndexKey&&) = default;
  bool operator==(const IndexKey&) const = default;
  ~IndexKey() = default;
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
    EncodedIndexKeyType type;
    d >> (uint8_t&)type;
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
  std::string key_{""};
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
