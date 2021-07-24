#ifndef TINYLAMB_COLUMN_HPP
#define TINYLAMB_COLUMN_HPP

#include <cassert>
#include <memory>
#include <string_view>
#include <vector>

#include "log_message.hpp"
#include "page/row_position.hpp"
#include "value_type.hpp"

namespace tinylamb {

enum class Restriction : uint8_t {
  kNoRestriction = 0,
  kUnique = 1,
  kPrimaryKey = 2
};

// Column format layout is like below.
// | name_length(2) | type(1) | value_length(2) | restriction(1) | offset(2) |
// name (variable length) |
class Column {
 public:
  Column() = default;
  Column(const Column& c) : owned_data(c.data), data(owned_data) {}

  explicit Column(const char* ptr);
  Column(std::string_view name, ValueType type, uint16_t value_length,
         enum Restriction rst, uint16_t offset);
  [[nodiscard]] std::string_view Name() const {
    size_t name_len = *reinterpret_cast<const uint16_t*>(data.data());
    LOG(TRACE) << "name len: " << name_len;
    return std::string_view(&data[8], name_len);
  }
  [[nodiscard]] ValueType Type() const;
  [[nodiscard]] uint16_t ValueLength() const;
  [[nodiscard]] enum Restriction Restriction() const;
  [[nodiscard]] uint16_t Offset() const;
  [[nodiscard]] size_t FootprintSize() const {
    return 2 + 1 + 2 + 1 + 2 + Name().size();
  }
  bool operator==(const Column& rhs) const { return data == rhs.data; }

  friend std::ostream& operator<<(std::ostream& o, const Column& c) {
    o << c.Name() << "{";
    switch (c.Type()) {
      case ValueType::kUnknown:
        o << "(Unknown type)";
        break;
      case ValueType::kInt64:
        o << "int64";
        break;
      case ValueType::kVarChar:
        o << "varchar(" << c.ValueLength() << ")";
        break;
    }
    o << "}";
    return o;
  }

  std::string owned_data;
  std::string_view data;
};

}  // namespace tinylamb

namespace std {

template <>
class hash<tinylamb::Column> {
 public:
  uint64_t operator()(const tinylamb::Column& c) const;
};

}  // namespace std

#endif  // TINYLAMB_COLUMN_HPP
