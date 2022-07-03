//
// Created by kumagi on 2021/06/01.
//

#ifndef TINYLAMB_SCHEMA_HPP
#define TINYLAMB_SCHEMA_HPP

#include <cassert>
#include <unordered_set>
#include <vector>

#include "common/log_message.hpp"
#include "type/column.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

class Schema {
 public:
  Schema() = default;
  Schema(std::string_view schema_name, std::vector<Column> columns);
  [[nodiscard]] slot_t ColumnCount() const { return columns_.size(); }
  [[nodiscard]] std::string_view Name() const { return name_; }
  [[nodiscard]] const Column& GetColumn(size_t idx) const {
    return columns_[idx];
  }
  [[nodiscard]] std::unordered_set<std::string> ColumnSet() const;
  [[nodiscard]] int Offset(std::string_view name) const;

  Schema operator+(const Schema& rhs) const;
  bool operator==(const Schema& rhs) const = default;
  friend std::ostream& operator<<(std::ostream& o, const Schema& s);
  friend Encoder& operator<<(Encoder& a, const Schema& sc);
  friend Decoder& operator>>(Decoder& a, Schema& sc);

 private:
  std::string name_;
  std::vector<Column> columns_;
};

}  // namespace tinylamb

#endif  // TINYLAMB_SCHEMA_HPP
