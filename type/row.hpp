//
// Created by kumagi on 2019/09/08.
//

#ifndef TINYLAMB_ROW_HPP
#define TINYLAMB_ROW_HPP

#include <cstring>
#include <ostream>

#include "page/row_position.hpp"
#include "schema.hpp"
#include "value.hpp"

namespace tinylamb {

struct Row {
  Row() = default;
  Row(void* d, size_t l, RowPosition p);
  Row(std::string_view payload, RowPosition p);

  // Shallow copy.
  Row& operator=(const Row& orig);

  void SetValue(const Schema& sc, size_t idx, const Value& v);

  bool Write(std::string_view from);

  bool Read(char* data, size_t length, const RowPosition& pos);

  [[nodiscard]] const char* Data() const { return data.data(); }

  [[nodiscard]] size_t Size() const { return data.size(); }

  bool GetValue(Schema& sc, uint16_t idx, Value& dst);

  void MakeOwned();

  [[nodiscard]] bool IsOwned() const { return !owned_data.empty(); }

  friend std::ostream& operator<<(std::ostream& o, const Row& r) {
    if (r.pos.IsValid()) {
      o << r.pos << ": ";
    }
    if (!r.owned_data.empty()) {
      o << "(owned)";
    }
    o << r.data;
    return o;
  }
  std::string_view data;
  RowPosition pos;  // Origin of the row, if exists.
  std::string owned_data;
};

}  // namespace tinylamb

namespace std {
template <>
class hash<tinylamb::Row> {
 public:
  uint64_t operator()(const tinylamb::Row& row);
};

}  // namespace std

#endif  // TINYLAMB_ROW_HPP
