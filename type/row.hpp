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

  size_t Serialize(char* dst) const;
  size_t Deserialize(const char* src, const Schema& sc);
  [[nodiscard]] size_t Size() const;

  bool operator==(const Row& rhs) const;
  friend std::ostream& operator<<(std::ostream& o, const Row& r);

  std::vector<Value> values_;
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
