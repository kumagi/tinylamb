//
// Created by kumagi on 2021/06/01.
//

#ifndef TINYLAMB_SCHEMA_HPP
#define TINYLAMB_SCHEMA_HPP

#include <cassert>
#include <vector>

#include "common/log_message.hpp"
#include "page/row_position.hpp"
#include "type/column.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

class Schema {
 public:
  Schema() = default;
  Schema(std::string_view schema_name, std::vector<Column> columns);
  [[nodiscard]] size_t ColumnCount() const { return columns_.size(); }
  [[nodiscard]] std::string_view Name() const { return name_; }
  size_t Serialize(char* dst) const;
  size_t Deserialize(const char* dst);
  [[nodiscard]] size_t Size() const;
  [[nodiscard]] const Column& GetColumn(size_t idx) const {
    return columns_[idx];
  }
  bool operator==(const Schema& rhs) const;
  friend std::ostream& operator<<(std::ostream& o, const Schema& s);

 private:
  std::string name_;
  std::vector<Column> columns_;
};

}  // namespace tinylamb

namespace std {
template <>
class hash<tinylamb::Schema> {
 public:
  uint64_t operator()(const tinylamb::Schema& sc) const;
};
}  // namespace std

#endif  // TINYLAMB_SCHEMA_HPP
