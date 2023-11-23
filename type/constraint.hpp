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

#ifndef TINYLAMB_CONSTRAINT_HPP
#define TINYLAMB_CONSTRAINT_HPP

#include <cstddef>
#include <cstdint>

#include "type/value.hpp"

namespace tinylamb {

/*
 * NOT NULL Constraint − Ensures that a column cannot have NULL value.
 * DEFAULT Constraint − Provides a default value for a column when none is
 *                      specified.
 * UNIQUE Constraint − Ensures that all values in a column are different.
 * PRIMARY Key − Uniquely identifies each row/record in a database table.
 * FOREIGN GetKey − Uniquely identifies a row/record in any of the given
 *               database table.
 * CHECK Constraint − The CHECK constraint ensures that all the values in a
 *                    column satisfies certain conditions.
 * INDEX − Used to create and retrieve data from the database very quickly.
 */

struct Constraint {
 public:
  enum ConstraintType : uint8_t {
    kNothing,
    kNotNull,
    kDefault,
    kUnique,
    kPrimaryKey,
    kForeign,  // Won't implemented.
    kCheck,    // Won't implemented.
    kIndex,
  };

  Constraint() = default;
  explicit Constraint(ConstraintType ctype) : ctype(ctype) {}
  Constraint(ConstraintType ctype, const Value& v) : ctype(ctype), value(v) {}

  [[nodiscard]] size_t Size() const;
  bool operator==(const Constraint& rhs) const;
  [[nodiscard]] bool IsNothing() const { return ctype == kNothing; }
  [[nodiscard]] bool IsUnique() const {
    return ctype == kUnique || ctype == kPrimaryKey;
  }
  friend std::ostream& operator<<(std::ostream& o, const Constraint& c);
  friend Encoder& operator<<(Encoder& a, const Constraint& c);
  friend Decoder& operator>>(Decoder& a, Constraint& c);

  ConstraintType ctype{kNothing};
  Value value{};
};

}  // namespace tinylamb

template <>
class std::hash<tinylamb::Constraint> {
 public:
  uint64_t operator()(const tinylamb::Constraint& c) const;
};

#endif  // TINYLAMB_CONSTRAINT_HPP
