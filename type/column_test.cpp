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

#include "type/column.hpp"

#include <functional>
#include <sstream>

#include "column_name.hpp"
#include "common/log_message.hpp"
#include "common/test_util.hpp"
#include "constraint.hpp"
#include "gtest/gtest.h"
#include "value_type.hpp"

namespace tinylamb {
TEST(ColumnTest, Constructor_WithValidArguments_InitializesMembers) {
  Column c(ColumnName("test_column"), ValueType::kInt64);
  Column d(ColumnName("next_column"), ValueType::kVarChar,
           Constraint(Constraint::kUnique));

  EXPECT_EQ(c.Name().name, "test_column");
  EXPECT_EQ(c.Type(), ValueType::kInt64);
  EXPECT_EQ(d.Name().name, "next_column");
  EXPECT_EQ(d.Type(), ValueType::kVarChar);
}

TEST(ColumnTest, SerializeDeserialize_ForDiverseVariants_PreservesState) {
  SerializeDeserializeTest(
      Column(ColumnName("test_column"), ValueType::kInt64));
  SerializeDeserializeTest(Column(ColumnName("next_column"), ValueType::kDouble,
                                  Constraint(Constraint::kUnique)));
}

TEST(ColumnTest, Dump_ToLogStream_SucceedsWithoutCrash) {
  Column c1(ColumnName("test_column"), ValueType::kInt64);
  Column c2(ColumnName("next_column"), ValueType::kDouble,
            Constraint(Constraint::kUnique));

  LOG(INFO) << c1;
  LOG(ERROR) << c2;
}

TEST(ColumnTest,
     StreamOperator_WithVariousTypesAndConstraints_FormatsExpectedString) {
  Column c1(ColumnName("int_col"), ValueType::kInt64);
  Column c2(ColumnName("uniq_col"), ValueType::kDouble,
            Constraint(Constraint::kUnique));
  Column c3(ColumnName("bare_col"), ValueType::kNull);
  std::ostringstream oss;

  oss << c1 << "|" << c2 << "|" << c3;

  ASSERT_EQ(oss.str(), "int_col: Integer|uniq_col: Double(UNIQUE)|bare_col");
}

TEST(ColumnTest, Accessors_WhenConstructedFromStringView_ReturnMatchingValues) {
  Column c("named_col", ValueType::kVarChar, Constraint(Constraint::kDefault));

  const std::string name = c.Name().name;
  const ValueType type = c.Type();
  const Constraint::ConstraintType ctype = c.GetConstraint().ctype;

  ASSERT_EQ(name, "named_col");
  ASSERT_EQ(type, ValueType::kVarChar);
  ASSERT_EQ(ctype, Constraint::kDefault);
}

TEST(ColumnTest,
     Hash_WithDistinctAndIdenticalColumns_DifferentiatesAndMatches) {
  Column c1("a", ValueType::kInt64);
  Column c2("b", ValueType::kInt64);
  Column c3("a", ValueType::kDouble);
  Column c4("a", ValueType::kInt64, Constraint(Constraint::kUnique));
  Column c5("a", ValueType::kInt64);
  std::hash<Column> hasher;

  const size_t h1 = hasher(c1);
  const size_t h2 = hasher(c2);
  const size_t h3 = hasher(c3);
  const size_t h4 = hasher(c4);
  const size_t h5 = hasher(c5);

  ASSERT_NE(h1, h2);
  ASSERT_NE(h1, h3);
  ASSERT_NE(h1, h4);
  ASSERT_EQ(h1, h5);
}
}  // namespace tinylamb
