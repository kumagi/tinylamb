#ifndef TINYLAMB_COLUMN_HPP
#define TINYLAMB_COLUMN_HPP

#include <cassert>
#include <memory>
#include <string_view>
#include <vector>

#include "common/decoder.hpp"
#include "common/log_message.hpp"
#include "page/row_position.hpp"
#include "type/constraint.hpp"
#include "value_type.hpp"

namespace tinylamb {
class Encoder;

class Column {
 public:
  Column() = default;
  explicit Column(std::string_view name, ValueType type = ValueType::kNull,
                  Constraint cst = Constraint());

  [[nodiscard]] std::string_view Name() const { return name_; }
  [[nodiscard]] ValueType Type() const { return type_; }
  [[nodiscard]] Constraint GetConstraint() const { return constraint_; }

  bool operator==(const Column& rhs) const = default;
  friend std::ostream& operator<<(std::ostream& o, const Column& c);
  friend Encoder& operator<<(Encoder& a, const Column& c);
  friend Decoder& operator>>(Decoder& e, Column& c);

 private:
  std::string name_;
  ValueType type_{};
  Constraint constraint_{Constraint::kNothing};
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
