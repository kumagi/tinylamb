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
TEST(ColumnTest, Construct) {
  // Arrange -- nothing more than default ColumnName/ValueType/Constraint
  // Act -- construct two Columns with different names/types/constraints
  Column c(ColumnName("test_column"), ValueType::kInt64);
  Column d(ColumnName("next_column"), ValueType::kVarChar,
            Constraint(Constraint::kUnique));
  // Assert -- implicit; no crash, no explicit assertions; gtest green on pass
}

TEST(ColumnTest, SerializeDeserialize) {
  // Arrange -- nothing more than default ColumnName/ValueType/Constraint
  // Act -- serialize+deserialize round-trip for 2 Column variants
  SerializeDeserializeTest(
      Column(ColumnName("test_column"), ValueType::kInt64));
  SerializeDeserializeTest(Column(ColumnName("next_column"), ValueType::kDouble,
                                   Constraint(Constraint::kUnique)));
  // Assert -- implicit; SerializeDeserializeTest macro asserts equality
}

TEST(ColumnTest, Dump) {
  // Arrange -- nothing more than two Columns with different names/types
  // Act -- stream Columns to LOG (no assertion; output-only)
  LOG(INFO) << Column(ColumnName("test_column"), ValueType::kInt64);
  LOG(ERROR) << Column(ColumnName("next_column"), ValueType::kDouble,
                        Constraint(Constraint::kUnique));
  // Assert -- implicit; no crash, no explicit assertions; gtest green on pass
}

TEST(ColumnTest, Stream) {
  // Arrange -- columns with and without constraints, and a null-typed column
  // Act -- stream each to a stringstream
  std::ostringstream oss;
  oss << Column(ColumnName("int_col"), ValueType::kInt64) << "|"
      << Column(ColumnName("uniq_col"), ValueType::kDouble,
                Constraint(Constraint::kUnique))
      << "|" << Column(ColumnName("bare_col"), ValueType::kNull);
  // Assert -- type is rendered unless kNull; constraint is appended
  ASSERT_EQ(oss.str(),
            "int_col: Integer|uniq_col: Double(UNIQUE)|bare_col");
}

TEST(ColumnTest, Accessors) {
  // Arrange -- a column built through the string_view constructor
  Column c("named_col", ValueType::kVarChar, Constraint(Constraint::kDefault));
  // Act + Assert -- accessors expose the stored name, type and constraint
  ASSERT_EQ(c.Name().name, "named_col");
  ASSERT_EQ(c.Type(), ValueType::kVarChar);
  ASSERT_EQ(c.GetConstraint().ctype, Constraint::kDefault);
}

TEST(ColumnTest, Hash) {
  // Arrange -- several columns
  // Act -- hash each of them
  std::hash<Column> hasher;
  // Assert -- different names/types/constraints hash differently
  ASSERT_NE(hasher(Column("a", ValueType::kInt64)),
            hasher(Column("b", ValueType::kInt64)));
  ASSERT_NE(hasher(Column("a", ValueType::kInt64)),
            hasher(Column("a", ValueType::kDouble)));
  ASSERT_NE(hasher(Column("a", ValueType::kInt64,
                          Constraint(Constraint::kUnique))),
            hasher(Column("a", ValueType::kInt64)));
  ASSERT_EQ(hasher(Column("a", ValueType::kInt64)),
            hasher(Column("a", ValueType::kInt64)));
}
}  // namespace tinylamb
