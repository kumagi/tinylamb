//
// Created by kumagi on 2021/06/01.
//

#ifndef TINYLAMB_SCHEMA_HPP
#define TINYLAMB_SCHEMA_HPP

#include <vector>
#include <cassert>

#include "macro.hpp"
#include "page/row_position.hpp"
#include "value_type.hpp"

namespace tinylamb {

enum Restriction : uint8_t {
  kNoRestriction = 0,
  kUnique = 1,
  kPrimaryKey = 2
};

// Column format layout is like below.
// | name_length | type | value_length | restriction | offset | name (variable
// length) |
struct Column {
  MAPPING_ONLY(Column);

  void AddRestriction(Restriction r) { restriction |= r; }
  [[nodiscard]] size_t PhysicalSize() const;
  [[nodiscard]] std::string_view Name() const;
  [[nodiscard]] uint64_t CalcHash() const;

  friend std::ostream& operator<<(std::ostream& o, const Column& c) {
    o << c.Name() << "{";
    switch (c.type) {
      case ValueType::kUnknown:
        o << "(Unknown type)";
        break;
      case ValueType::kInt64:
        o << "int64";
        break;
      case ValueType::kVarChar:
        o << "varchar(" << c.value_length << ")";
        break;
    }
    o << "}";
    return o;
  }

  // Never owns data.
  uint8_t name_length;
  ValueType type = ValueType::kUnknown;
  uint16_t value_length = 0;
  uint16_t restriction = 0;
  uint32_t offset = 0;
  char name[0];  // variable length.
};

// Schema format in page is layout like below.
// | table_root (8bytes) | name_length(1byte) | name payload (variable length) |
// | column_count(1byte) |
// | column(0) (variable_length) |  column(1) (variable_length) | column(n)... |
class Schema {
  Schema() = default;

 public:
  explicit Schema(std::string_view name);

  static Schema Read(char* ptr);

  [[nodiscard]] uint8_t ColumnCount() const;

  void AddColumn(std::string_view name, ValueType type, size_t value_length,
                 uint16_t restriction);

  void SetTableRoot(uint64_t root);

  [[nodiscard]] uint64_t GetTableRoot() const {
    return table_root;
  }

  [[nodiscard]] size_t FixedRowSize() const;

  [[nodiscard]] std::string_view Name() const;

  [[nodiscard]] char* Data() { return data; }

  [[nodiscard]] const char* Data() const { return data; }

  [[nodiscard]] size_t Size() const { return length; }

  [[nodiscard]] bool OwnData() const { return !owned_data.empty(); }

  [[nodiscard]] const Column& GetColumn(size_t idx) const;

  [[nodiscard]] uint64_t CalcChecksum() const;

  friend std::ostream& operator<<(std::ostream& o, const Schema& s) {
    o << s.Name() << ": [";
    for (size_t i = 0; i < s.columns.size(); ++i) {
      if (0 < i) o << ", ";
      o << s.GetColumn(i);
    }
    o << "]@" << s.table_root;
    return o;
  }

 private:
  void MakeOwnData();

  void SetColumnCount(uint8_t count);

 private:
  // If owned, this member has actual data.
  std::string owned_data;

  // Physical position of the schema data.
  char* data = nullptr;

  // Data length in bytes. If the data is owned, length must equal to
  // owned_data.size()
  size_t length = 0;

  // Length of this schema's name.
  // This value is limited under <= 255.
  uint8_t name_length = 0;

  uint64_t table_root = 0;

  // Vector of offset for each elements of this schema.
  std::vector<uint16_t> columns = {};

  // Origin of the row, if exists.
  RowPosition pos;
};

}  // namespace tinylamb

#endif  // TINYLAMB_SCHEMA_HPP
