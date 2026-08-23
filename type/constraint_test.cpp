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

#include "type/constraint.hpp"

#include <cstddef>
#include <functional>
#include <sstream>

#include "common/log_message.hpp"
#include "common/test_util.hpp"
#include "gtest/gtest.h"
#include "type/value_type.hpp"
#include "value.hpp"

namespace tinylamb {

TEST(Constraint, Construct) {
  // Arrange -- nothing more than default Constraint kinds
  // Act -- construct two Constraints of different kinds/values
  Constraint c(Constraint::kNothing);
  Constraint s(Constraint::kDefault, Value("hello"));
  // Assert -- implicit; no crash, no explicit assertions; gtest green on pass
}

TEST(Constraint, SerializeDeserialize) {
  // Arrange -- nothing more than default Constraint kinds
  // Act -- serialize+deserialize round-trip for 5 Constraint variants
  SerializeDeserializeTest(Constraint(Constraint::kNothing));
  SerializeDeserializeTest(Constraint(Constraint::kDefault, Value(2)));
  SerializeDeserializeTest(Constraint(Constraint::kUnique));
  SerializeDeserializeTest(Constraint(Constraint::kPrimaryKey));
  SerializeDeserializeTest(Constraint(Constraint::kNotNull));
  // Assert -- implicit; SerializeDeserializeTest macro asserts equality
}

TEST(Constraint, SerializeRemainingTypes) {
  // Arrange -- the remaining constraint kinds not covered elsewhere
  // Act + Assert -- round-trip serialization preserves them
  SerializeDeserializeTest(Constraint(Constraint::kForeign, Value(7)));
  SerializeDeserializeTest(Constraint(Constraint::kCheck, Value("x > 0")));
  SerializeDeserializeTest(Constraint(Constraint::kIndex));
}

TEST(Constraint, Size) {
  // Arrange -- one constraint per kind, plus value-carrying kinds
  // Act + Assert -- Size() matches the serialized footprint
  ASSERT_EQ(Constraint(Constraint::kNothing).Size(), 1);
  ASSERT_EQ(Constraint(Constraint::kNotNull).Size(), 1);
  ASSERT_EQ(Constraint(Constraint::kUnique).Size(), 1);
  ASSERT_EQ(Constraint(Constraint::kPrimaryKey).Size(), 1);
  ASSERT_EQ(Constraint(Constraint::kIndex).Size(), 1);
  const size_t expected =
      sizeof(Constraint::ConstraintType) + sizeof(ValueType) +
      Value("hello").Size();
  ASSERT_EQ(Constraint(Constraint::kDefault, Value("hello")).Size(), expected);
  ASSERT_EQ(Constraint(Constraint::kForeign, Value("t(id)")).Size(), expected);
  ASSERT_EQ(Constraint(Constraint::kCheck, Value("c > 0")).Size(), expected);
}

TEST(Constraint, Equality) {
  // Arrange -- several pairs of constraints
  // Act + Assert -- == compares kinds, and compares values for kDefault
  ASSERT_EQ(Constraint(Constraint::kNothing), Constraint(Constraint::kNothing));
  ASSERT_NE(Constraint(Constraint::kNothing), Constraint(Constraint::kUnique));
  ASSERT_EQ(Constraint(Constraint::kDefault, Value(2)),
            Constraint(Constraint::kDefault, Value(2)));
  ASSERT_NE(Constraint(Constraint::kDefault, Value(2)),
            Constraint(Constraint::kDefault, Value(3)));
  ASSERT_EQ(Constraint(Constraint::kForeign, Value(2)),
            Constraint(Constraint::kForeign, Value(3)));
}

TEST(Constraint, Accessors) {
  // Arrange -- constraint kinds exercising IsNothing / IsUnique
  // Act + Assert -- boolean helpers behave as documented
  ASSERT_TRUE(Constraint(Constraint::kNothing).IsNothing());
  ASSERT_FALSE(Constraint(Constraint::kNotNull).IsNothing());
  ASSERT_TRUE(Constraint(Constraint::kUnique).IsUnique());
  ASSERT_TRUE(Constraint(Constraint::kPrimaryKey).IsUnique());
  ASSERT_FALSE(Constraint(Constraint::kNotNull).IsUnique());
}

TEST(Constraint, StreamAllKinds) {
  // Arrange -- one constraint per kind
  // Act -- stream each kind to a stringstream
  std::ostringstream oss;
  oss << Constraint(Constraint::kNothing) << "|"
      << Constraint(Constraint::kNotNull) << "|"
      << Constraint(Constraint::kUnique) << "|"
      << Constraint(Constraint::kPrimaryKey) << "|"
      << Constraint(Constraint::kIndex) << "|"
      << Constraint(Constraint::kDefault, Value(2)) << "|"
      << Constraint(Constraint::kForeign, Value("t(c)")) << "|"
      << Constraint(Constraint::kCheck, Value("c > 0"));
  // Assert -- human-readable strings for every kind
  ASSERT_EQ(oss.str(),
            "(No constraint)|NOT NULL|UNIQUE|PRIMARY KEY|INDEX|DEFAULT(2)|"
            "FOREIGN(\"t(c)\")|CHECK(\"c > 0\")");
}

TEST(Constraint, Hash) {
  // Arrange -- several constraints
  // Act -- hash every kind and repeated identical constraints
  std::hash<Constraint> hasher;
  ASSERT_NE(hasher(Constraint(Constraint::kNothing)),
            hasher(Constraint(Constraint::kNotNull)));
  ASSERT_NE(hasher(Constraint(Constraint::kUnique)),
            hasher(Constraint(Constraint::kPrimaryKey)));
  ASSERT_EQ(hasher(Constraint(Constraint::kDefault, Value(2))),
            hasher(Constraint(Constraint::kDefault, Value(2))));
  ASSERT_NE(hasher(Constraint(Constraint::kDefault, Value(2))),
            hasher(Constraint(Constraint::kDefault, Value(3))));
  ASSERT_EQ(hasher(Constraint(Constraint::kForeign, Value(1))),
            hasher(Constraint(Constraint::kForeign, Value(1))));
}

TEST(Constraint, Dump) {
  // Arrange -- nothing more than two Constraints of different kinds
  // Act -- stream Constraints to LOG (no assertion; output-only)
  LOG(INFO) << Constraint(Constraint::kNothing);
  LOG(WARN) << Constraint(Constraint::kDefault, Value(2));
  // Assert -- implicit; no crash, no explicit assertions; gtest green on pass
}

}  // namespace tinylamb