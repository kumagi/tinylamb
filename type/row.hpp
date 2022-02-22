//
// Created by kumagi on 2019/09/08.
//

#ifndef TINYLAMB_ROW_HPP
#define TINYLAMB_ROW_HPP

#include <cstring>
#include <utility>
#include <vector>

#include "type/value.hpp"

namespace tinylamb {
class Schema;

struct Row {
  Row() = default;
  Row(std::initializer_list<Value> v) : values_(v) {}
  explicit Row(std::vector<Value> v) : values_(std::move(v)) {}

  void Add(const Value& v);
  Value& operator[](size_t i);
  const Value& operator[](size_t i) const;
  size_t Serialize(char* dst) const;
  size_t Deserialize(const char* src, const Schema& sc);
  [[nodiscard]] size_t Size() const;
  [[nodiscard]] std::string EncodeMemcomparableFormat() const;
  void Clear() { values_.clear(); }
  [[nodiscard]] bool IsValid() const { return values_.empty(); }
  [[nodiscard]] Row Extract(const std::vector<size_t>& elms) const;
  Row operator+(const Row& rhs) const;

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
