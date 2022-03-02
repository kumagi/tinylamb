//
// Created by kumagi on 2021/06/01.
//

#ifndef TINYLAMB_VALUE_HPP
#define TINYLAMB_VALUE_HPP

#include <cstdint>

#include "common/serdes.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
class Encoder;
class Decoder;

class Value {
 public:
  Value() = default;
  explicit Value(int int_val);
  explicit Value(int64_t int_val);
  // explicit Value(std::string_view varchar_val);
  explicit Value(std::string&& str_val);
  explicit Value(double double_value);
  Value(const Value& o);

  [[nodiscard]] size_t Size() const;

  size_t Serialize(char* dst) const;

  size_t Deserialize(const char* src, ValueType as_type);

  [[nodiscard]] std::string EncodeMemcomparableFormat() const;
  size_t DecodeMemcomparableFormat(const char* src);

  bool operator==(const Value& rhs) const;
  bool operator!=(const Value& rhs) const { return !operator==(rhs); }
  bool operator<(const Value& rhs) const;
  bool operator>(const Value& rhs) const;
  bool operator<=(const Value& rhs) const { return !operator>(rhs); }
  bool operator>=(const Value& rhs) const { return !operator<(rhs); }

  Value operator+(const Value& rhs) const;
  Value operator-(const Value& rhs) const;
  Value operator*(const Value& rhs) const;
  Value operator/(const Value& rhs) const;
  Value operator%(const Value& rhs) const;

  Value operator&(const Value& rhs) const;
  Value operator|(const Value& rhs) const;
  Value operator^(const Value& rhs) const;

  [[nodiscard]] std::string AsString() const;
  Value& operator=(const Value& rhs);
  friend std::ostream& operator<<(std::ostream& o, const Value& v);
  friend Encoder& operator<<(Encoder& o, const Value& v);
  friend Decoder& operator>>(Decoder& o, Value& v);

  union {
    int64_t int_value;
    std::string_view varchar_value;
    double double_value;
  } value{0};
  ValueType type{ValueType::kUnknown};
  std::string owned_data;
};

}  // namespace tinylamb

template <>
class std::hash<tinylamb::Value> {
 public:
  uint64_t operator()(const tinylamb::Value& c) const;
};

#endif  // TINYLAMB_VALUE_HPP
