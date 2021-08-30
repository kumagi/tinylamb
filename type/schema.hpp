//
// Created by kumagi on 2021/06/01.
//

#ifndef TINYLAMB_SCHEMA_HPP
#define TINYLAMB_SCHEMA_HPP

#include <cassert>
#include <vector>

#include "log_message.hpp"
#include "page/row_position.hpp"
#include "type/column.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

// Schema format in page is layout like below.
// | row_page (8bytes) | name_length(1byte) | schema_name (variable length) |
// | column_count(1byte) |
// | column(0) (variable_length) |  column(1) (variable_length) | column(n)... |
class Schema {
 public:
  Schema() = default;

  explicit Schema(char* ptr);

  explicit Schema(const char* ptr);

  Schema(std::string_view schema_name, const std::vector<Column>& columns,
         uint64_t page_id);

  // First PageID of this schema's table payload exists.
  [[nodiscard]] uint64_t RowPage() const {
    return *reinterpret_cast<const uint64_t*>(&data[0]);
  }

  // Length of this schema's name.
  // This value is limited under <= 255.
  [[nodiscard]] uint8_t NameLength() const {
    return *reinterpret_cast<const uint8_t*>(&data[8]);
  }

  [[nodiscard]] uint8_t ColumnCount() const { return data[9 + NameLength()]; }

  [[nodiscard]] size_t FixedRowSize() const;

  [[nodiscard]] std::string_view Name() const {
    return std::string_view(&data[9], NameLength());
  }

  [[nodiscard]] std::string_view Data() const { return data; }

  [[nodiscard]] size_t Size() const { return data.size(); }

  [[nodiscard]] bool OwnData() const { return !owned_data.empty(); }

  [[nodiscard]] Column GetColumn(size_t idx) const;

  bool operator==(const Schema& rhs) const {
    return data == rhs.data;
  }

  friend std::ostream& operator<<(std::ostream& o, const Schema& s) {
    o << s.Name() << ": [";
    for (size_t i = 0; i < s.offsets.size(); ++i) {
      if (0 < i) o << ", ";
      o << s.GetColumn(i);
    }
    o << "]@" << s.RowPage();
    return o;
  }

 private:
  // If owned, this member has actual data.
  std::string owned_data = {};

  // Physical data of the schema data, it may point owned_data.
  std::string_view data = {};

  // Vector offg offset for each elements of this schema.
  std::vector<uint16_t> offsets = {};
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
