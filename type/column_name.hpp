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

//
// Created by kumagi on 22/07/07.
//

#ifndef TINYLAMB_COLUMN_NAME_HPP
#define TINYLAMB_COLUMN_NAME_HPP

#include <iosfwd>
#include <string>
#include <cstdint>

namespace tinylamb {

class Encoder;
class Decoder;

struct ColumnName {
 public:
  ColumnName() = default;
  ColumnName(std::string_view sc_name, std::string_view attr_name)
      : schema(sc_name), name(attr_name) {}
  ColumnName(const ColumnName&) = default;
  ColumnName(ColumnName&&) = default;
  ColumnName& operator=(const ColumnName&) = default;
  ColumnName& operator=(ColumnName&&) = default;
  ~ColumnName() = default;

  explicit ColumnName(std::string_view na) {
    size_t pos = na.find('.');
    if (pos == std::string::npos) {
      name = na;
    } else {
      schema.assign(na.data(), pos);
      name.assign(na.data() + pos + 1, na.size() - schema.size() - 1);
    }
  }
  [[nodiscard]] std::string ToString() const;
  bool operator==(const ColumnName&) const = default;
  bool operator<(const ColumnName&) const;
  [[nodiscard]] bool Empty() const { return schema.empty() && name.empty(); }
  friend std::ostream& operator<<(std::ostream& o, const ColumnName& c);
  friend Encoder& operator<<(Encoder& a, const ColumnName& c);
  friend Decoder& operator>>(Decoder& e, ColumnName& c);
  std::string schema;
  std::string name;
};

}  // namespace tinylamb

namespace std {

template <>
class hash<tinylamb::ColumnName> {
 public:
  uint64_t operator()(const tinylamb::ColumnName& c) const;
};

}  // namespace std

#endif  // TINYLAMB_COLUMN_NAME_HPP
