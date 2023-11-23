/**
 * Copyright 2023 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


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
  Row(const Row&) = default;
  Row(Row&&) = default;
  Row& operator=(const Row&) = default;
  Row& operator=(Row&&) = default;
  ~Row() = default;

  Value& operator[](size_t i);
  const Value& operator[](size_t i) const;
  size_t Serialize(char* dst) const;
  size_t Deserialize(const char* src, const Schema& sc);
  [[nodiscard]] size_t Size() const;
  [[nodiscard]] std::string EncodeMemcomparableFormat() const;
  void DecodeMemcomparableFormat(std::string_view src);
  void Clear() { values_.clear(); }
  [[nodiscard]] bool IsValid() const { return !values_.empty(); }
  [[nodiscard]] Row Extract(const std::vector<slot_t>& elms) const;
  Row operator+(const Row& rhs) const;

  bool operator==(const Row& rhs) const = default;
  bool operator!=(const Row& rhs) const { return !operator==(rhs); }
  friend std::ostream& operator<<(std::ostream& o, const Row& r);
  friend Encoder& operator<<(Encoder& e, const Row& r);
  friend Decoder& operator>>(Decoder& d, Row& r);

  std::vector<Value> values_;
};

}  // namespace tinylamb

namespace std {
template <>
class hash<tinylamb::Row> {
 public:
  uint64_t operator()(const tinylamb::Row& row) const;
};

}  // namespace std

#endif  // TINYLAMB_ROW_HPP
