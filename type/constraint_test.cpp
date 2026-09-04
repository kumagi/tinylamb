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

TEST(ConstraintTest, Constructor_WithDifferentTypes_InitializesCorrectly) {
  Constraint c(Constraint::kNothing);
  Constraint s(Constraint::kDefault, Value("hello"));

  EXPECT_TRUE(c.IsNothing());
  EXPECT_FALSE(s.IsNothing());
}

TEST(ConstraintTest, SerializeDeserialize_StandardTypes_PreservesEquality) {
  SerializeDeserializeTest(Constraint(Constraint::kNothing));
  SerializeDeserializeTest(Constraint(Constraint::kDefault, Value(2)));
  SerializeDeserializeTest(Constraint(Constraint::kUnique));
  SerializeDeserializeTest(Constraint(Constraint::kPrimaryKey));
  SerializeDeserializeTest(Constraint(Constraint::kNotNull));
}

TEST(ConstraintTest, SerializeDeserialize_RemainingTypes_PreservesEquality) {
  SerializeDeserializeTest(Constraint(Constraint::kForeign, Value(7)));
  SerializeDeserializeTest(Constraint(Constraint::kCheck, Value("x > 0")));
  SerializeDeserializeTest(Constraint(Constraint::kIndex));
}

TEST(ConstraintTest, Size_ForAllConstraintKinds_MatchesByteFootprint) {
  const size_t nothing_size = Constraint(Constraint::kNothing).Size();
  const size_t not_null_size = Constraint(Constraint::kNotNull).Size();
  const size_t unique_size = Constraint(Constraint::kUnique).Size();
  const size_t pk_size = Constraint(Constraint::kPrimaryKey).Size();
  const size_t index_size = Constraint(Constraint::kIndex).Size();
  const size_t expected_val_size = sizeof(Constraint::ConstraintType) +
                                   sizeof(ValueType) + Value("hello").Size();
  const size_t default_size =
      Constraint(Constraint::kDefault, Value("hello")).Size();
  const size_t foreign_size =
      Constraint(Constraint::kForeign, Value("t(id)")).Size();
  const size_t check_size =
      Constraint(Constraint::kCheck, Value("c > 0")).Size();

  ASSERT_EQ(nothing_size, 1);
  ASSERT_EQ(not_null_size, 1);
  ASSERT_EQ(unique_size, 1);
  ASSERT_EQ(pk_size, 1);
  ASSERT_EQ(index_size, 1);
  ASSERT_EQ(default_size, expected_val_size);
  ASSERT_EQ(foreign_size, expected_val_size);
  ASSERT_EQ(check_size, expected_val_size);
}

TEST(ConstraintTest,
     Equality_WithEqualAndUnequalInstances_EvaluatesExpectedly) {
  Constraint nothing1(Constraint::kNothing);
  Constraint nothing2(Constraint::kNothing);
  Constraint unique(Constraint::kUnique);
  Constraint def2_a(Constraint::kDefault, Value(2));
  Constraint def2_b(Constraint::kDefault, Value(2));
  Constraint def3(Constraint::kDefault, Value(3));
  Constraint foreign2(Constraint::kForeign, Value(2));
  Constraint foreign3(Constraint::kForeign, Value(3));

  ASSERT_EQ(nothing1, nothing2);
  ASSERT_NE(nothing1, unique);
  ASSERT_EQ(def2_a, def2_b);
  ASSERT_NE(def2_a, def3);
  // Fixed: kForeign now compares its payload (a referenced table), matching
  // std::hash; different payloads are different constraints.
  ASSERT_NE(foreign2, foreign3);
  ASSERT_EQ(foreign2, Constraint(Constraint::kForeign, Value(2)));
  ASSERT_NE(foreign2, unique);
}

TEST(ConstraintTest, Accessors_ForVariousKinds_ReturnsExpectedBooleans) {
  Constraint nothing(Constraint::kNothing);
  Constraint not_null(Constraint::kNotNull);
  Constraint unique(Constraint::kUnique);
  Constraint pk(Constraint::kPrimaryKey);

  ASSERT_TRUE(nothing.IsNothing());
  ASSERT_FALSE(not_null.IsNothing());
  ASSERT_TRUE(unique.IsUnique());
  ASSERT_TRUE(pk.IsUnique());
  ASSERT_FALSE(not_null.IsUnique());
}

TEST(ConstraintTest, StreamOperator_ForAllKinds_RendersExpectedFormat) {
  std::ostringstream oss;

  oss << Constraint(Constraint::kNothing) << "|"
      << Constraint(Constraint::kNotNull) << "|"
      << Constraint(Constraint::kUnique) << "|"
      << Constraint(Constraint::kPrimaryKey) << "|"
      << Constraint(Constraint::kIndex) << "|"
      << Constraint(Constraint::kDefault, Value(2)) << "|"
      << Constraint(Constraint::kForeign, Value("t(c)")) << "|"
      << Constraint(Constraint::kCheck, Value("c > 0"));

  ASSERT_EQ(oss.str(),
            "(No constraint)|NOT NULL|UNIQUE|PRIMARY KEY|INDEX|DEFAULT(2)|"
            "FOREIGN(\"t(c)\")|CHECK(\"c > 0\")");
}

TEST(ConstraintTest, Hash_WithDistinctAndMatchingKinds_HashesAppropriately) {
  std::hash<Constraint> hasher;

  const size_t h_nothing = hasher(Constraint(Constraint::kNothing));
  const size_t h_notnull = hasher(Constraint(Constraint::kNotNull));
  const size_t h_unique = hasher(Constraint(Constraint::kUnique));
  const size_t h_pk = hasher(Constraint(Constraint::kPrimaryKey));
  const size_t h_def2_a = hasher(Constraint(Constraint::kDefault, Value(2)));
  const size_t h_def2_b = hasher(Constraint(Constraint::kDefault, Value(2)));
  const size_t h_def3 = hasher(Constraint(Constraint::kDefault, Value(3)));
  const size_t h_for1_a = hasher(Constraint(Constraint::kForeign, Value(1)));
  const size_t h_for1_b = hasher(Constraint(Constraint::kForeign, Value(1)));

  ASSERT_NE(h_nothing, h_notnull);
  ASSERT_NE(h_unique, h_pk);
  ASSERT_EQ(h_def2_a, h_def2_b);
  ASSERT_NE(h_def2_a, h_def3);
  ASSERT_EQ(h_for1_a, h_for1_b);
}

TEST(ConstraintTest, Dump_ToLogStream_SucceedsWithoutCrash) {
  Constraint c(Constraint::kNothing);
  Constraint d(Constraint::kDefault, Value(2));

  LOG(INFO) << c;
  LOG(WARN) << d;
}

TEST(ConstraintTest, Equality_IsConsistentWithHash_ForValueBearingKinds) {
  // Fixed: kForeign/kCheck ignored `value` in == while std::hash included
  // it, violating the unordered-container contract (equal objects with
  // different hashes).
  std::hash<Constraint> hasher;

  const Constraint foreign_a(Constraint::kForeign, Value("t1"));
  const Constraint foreign_b(Constraint::kForeign, Value("t2"));
  const Constraint foreign_a2(Constraint::kForeign, Value("t1"));
  const Constraint check_a(Constraint::kCheck, Value("x > 0"));
  const Constraint check_b(Constraint::kCheck, Value("x < 0"));

  EXPECT_NE(foreign_a, foreign_b);
  EXPECT_EQ(hasher(foreign_a), hasher(foreign_a2));
  EXPECT_EQ(foreign_a, foreign_a2);
  EXPECT_NE(check_a, check_b);

  // Equal-by-== objects must hash equally (unordered container contract).
  EXPECT_EQ(hasher(foreign_a), hasher(foreign_a2));
}

}  // namespace tinylamb