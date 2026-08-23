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

#include "schema.hpp"

#include <sstream>
#include <string>
#include <unordered_set>

#include "column_name.hpp"
#include "common/log_message.hpp"
#include "common/test_util.hpp"
#include "constraint.hpp"
#include "gtest/gtest.h"
#include "value_type.hpp"

namespace tinylamb {

TEST(SchemaTest, Construct) {
  // Arrange -- nothing more than default ColumnName/ValueType/Constraint
  // Act -- construct two Schemas with different column counts
  Schema s("sample", {Column(ColumnName("c1"), ValueType::kInt64,
                              Constraint(Constraint::kPrimaryKey)),
                       Column(ColumnName("c2"), ValueType::kDouble)});
  Schema t("next_schema", {Column(ColumnName("c1"), ValueType::kInt64,
                                   Constraint(Constraint::kPrimaryKey)),
                            Column(ColumnName("c2"), ValueType::kDouble),
                            Column(ColumnName("c3"), ValueType::kVarChar)});
  // Assert -- implicit; no crash, no explicit assertions; gtest green on pass
}

TEST(SchemaTest, SerializeDeserialize) {
  // Arrange -- nothing more than default ColumnName/ValueType/Constraint
  // Act -- serialize+deserialize round-trip for 2 Schema variants
  SerializeDeserializeTest(
      Schema("sample", {Column(ColumnName("c1"), ValueType::kInt64,
                                Constraint(Constraint::kPrimaryKey)),
                         Column(ColumnName("c2"), ValueType::kDouble)}));
  SerializeDeserializeTest(
      Schema("next_schema", {Column(ColumnName("c1"), ValueType::kInt64,
                                     Constraint(Constraint::kPrimaryKey)),
                              Column(ColumnName("c2"), ValueType::kDouble),
                              Column(ColumnName("c3"), ValueType::kVarChar)}));
  // Assert -- implicit; SerializeDeserializeTest macro asserts equality
}

TEST(SchemaTest, Dump) {
  // Arrange -- nothing more than two Schemas with different column counts
  // Act -- stream Schemas to LOG (no assertion; output-only)
  LOG(INFO) << Schema("sample", {Column(ColumnName("c1"), ValueType::kInt64,
                                         Constraint(Constraint::kPrimaryKey)),
                                  Column(ColumnName("c2"), ValueType::kDouble)});
  LOG(WARN) << Schema("next_schema",
                       {Column(ColumnName("c1"), ValueType::kInt64,
                               Constraint(Constraint::kPrimaryKey)),
                        Column(ColumnName("c2"), ValueType::kDouble),
                        Column(ColumnName("c3"), ValueType::kVarChar)});
  // Assert -- implicit; no crash, no explicit assertions; gtest green on pass
}

TEST(SchemaTest, ColumnSet) {
  // Arrange -- a schema with two named columns and one unnamed column
  Schema s("sample", {Column("c1", ValueType::kInt64),
                      Column("c2", ValueType::kDouble),
                      Column("", ValueType::kVarChar)});
  // Act -- materialize the column set
  const std::unordered_set<ColumnName> set = s.ColumnSet();
  // Assert -- all three columns are present, keyed by the schema name
  ASSERT_EQ(set.size(), 3);
  ASSERT_NE(set.find(ColumnName("sample", "c1")), set.end());
  ASSERT_NE(set.find(ColumnName("sample", "c2")), set.end());
  ASSERT_NE(set.find(ColumnName("sample", "")), set.end());
}

TEST(SchemaTest, Offset) {
  // Arrange -- a schema whose columns carry the schema prefix
  Schema s("sample", {Column("c1", ValueType::kInt64),
                      Column("c2", ValueType::kDouble),
                      Column("c3", ValueType::kVarChar)});
  // Act + Assert -- unqualified lookup finds the column offset
  ASSERT_EQ(s.Offset(ColumnName("c1")), 0);
  ASSERT_EQ(s.Offset(ColumnName("c2")), 1);
  ASSERT_EQ(s.Offset(ColumnName("c3")), 2);
  // Act + Assert -- schema-qualified lookup matches only the same schema
  ASSERT_EQ(s.Offset(ColumnName("sample", "c1")), 0);
  ASSERT_EQ(s.Offset(ColumnName("other", "c1")), -1);
  // Assert -- unknown columns are reported as -1
  ASSERT_EQ(s.Offset(ColumnName("nope")), -1);
}

TEST(SchemaTest, Concatenate) {
  // Arrange -- two schemas to merge
  Schema left("left", {Column("a", ValueType::kInt64)});
  Schema right("right", {Column("b", ValueType::kDouble),
                         Column("c", ValueType::kVarChar)});
  // Act -- concatenate with operator+
  Schema merged = left + right;
  // Assert -- columns are concatenated and the merged schema is unnamed
  ASSERT_EQ(merged.ColumnCount(), 3);
  ASSERT_EQ(merged.GetColumn(0).Name().name, "a");
  ASSERT_EQ(merged.GetColumn(1).Name().name, "b");
  ASSERT_EQ(merged.GetColumn(2).Name().name, "c");
  ASSERT_EQ(merged.Name(), "");
}

TEST(SchemaTest, Stream) {
  // Arrange -- a schema with a couple of columns
  Schema s("sample", {Column("c1", ValueType::kInt64),
                      Column("c2", ValueType::kDouble)});
  // Act -- stream the schema
  std::ostringstream oss;
  oss << s;
  // Assert -- the name and both columns are rendered
  ASSERT_NE(oss.str().find("sample"), std::string::npos);
  ASSERT_NE(oss.str().find("c1"), std::string::npos);
  ASSERT_NE(oss.str().find("c2"), std::string::npos);
}
}  // namespace tinylamb