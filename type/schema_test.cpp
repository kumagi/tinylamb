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

TEST(SchemaTest, Constructor_WithValidColumns_InitializesCorrectly) {
  Schema s("sample", {Column(ColumnName("c1"), ValueType::kInt64,
                              Constraint(Constraint::kPrimaryKey)),
                       Column(ColumnName("c2"), ValueType::kDouble)});
  Schema t("next_schema", {Column(ColumnName("c1"), ValueType::kInt64,
                                   Constraint(Constraint::kPrimaryKey)),
                            Column(ColumnName("c2"), ValueType::kDouble),
                            Column(ColumnName("c3"), ValueType::kVarChar)});

  EXPECT_EQ(s.ColumnCount(), 2);
  EXPECT_EQ(s.Name(), "sample");
  EXPECT_EQ(t.ColumnCount(), 3);
  EXPECT_EQ(t.Name(), "next_schema");
}

TEST(SchemaTest, SerializeDeserialize_DiverseSchemas_PreservesStructure) {
  SerializeDeserializeTest(
      Schema("sample", {Column(ColumnName("c1"), ValueType::kInt64,
                                Constraint(Constraint::kPrimaryKey)),
                         Column(ColumnName("c2"), ValueType::kDouble)}));
  SerializeDeserializeTest(
      Schema("next_schema", {Column(ColumnName("c1"), ValueType::kInt64,
                                     Constraint(Constraint::kPrimaryKey)),
                              Column(ColumnName("c2"), ValueType::kDouble),
                              Column(ColumnName("c3"), ValueType::kVarChar)}));
}

TEST(SchemaTest, Dump_ToLogStream_SucceedsWithoutCrash) {
  Schema s("sample", {Column(ColumnName("c1"), ValueType::kInt64,
                             Constraint(Constraint::kPrimaryKey)),
                      Column(ColumnName("c2"), ValueType::kDouble)});
  Schema t("next_schema",
           {Column(ColumnName("c1"), ValueType::kInt64,
                   Constraint(Constraint::kPrimaryKey)),
            Column(ColumnName("c2"), ValueType::kDouble),
            Column(ColumnName("c3"), ValueType::kVarChar)});

  LOG(INFO) << s;
  LOG(WARN) << t;
}

TEST(SchemaTest, ColumnSet_FromMixedNamedAndUnnamedColumns_CollectsAllColumnNames) {
  Schema s("sample", {Column("c1", ValueType::kInt64),
                      Column("c2", ValueType::kDouble),
                      Column("", ValueType::kVarChar)});

  const std::unordered_set<ColumnName> set = s.ColumnSet();

  ASSERT_EQ(set.size(), 3);
  ASSERT_NE(set.find(ColumnName("sample", "c1")), set.end());
  ASSERT_NE(set.find(ColumnName("sample", "c2")), set.end());
  ASSERT_NE(set.find(ColumnName("sample", "")), set.end());
}

TEST(SchemaTest, Offset_WithQualifiedAndUnqualifiedNames_ReturnsIndexOrMinusOne) {
  Schema s("sample", {Column("c1", ValueType::kInt64),
                      Column("c2", ValueType::kDouble),
                      Column("c3", ValueType::kVarChar)});

  const ssize_t off_c1 = s.Offset(ColumnName("c1"));
  const ssize_t off_c2 = s.Offset(ColumnName("c2"));
  const ssize_t off_c3 = s.Offset(ColumnName("c3"));
  const ssize_t off_sample_c1 = s.Offset(ColumnName("sample", "c1"));
  const ssize_t off_other_c1 = s.Offset(ColumnName("other", "c1"));
  const ssize_t off_nope = s.Offset(ColumnName("nope"));

  ASSERT_EQ(off_c1, 0);
  ASSERT_EQ(off_c2, 1);
  ASSERT_EQ(off_c3, 2);
  ASSERT_EQ(off_sample_c1, 0);
  ASSERT_EQ(off_other_c1, -1);
  ASSERT_EQ(off_nope, -1);
}

TEST(SchemaTest, PlusOperator_ConcatenatingTwoSchemas_MergesColumnsAndClearsName) {
  Schema left("left", {Column("a", ValueType::kInt64)});
  Schema right("right", {Column("b", ValueType::kDouble),
                         Column("c", ValueType::kVarChar)});

  Schema merged = left + right;

  ASSERT_EQ(merged.ColumnCount(), 3);
  ASSERT_EQ(merged.GetColumn(0).Name().name, "a");
  ASSERT_EQ(merged.GetColumn(1).Name().name, "b");
  ASSERT_EQ(merged.GetColumn(2).Name().name, "c");
  ASSERT_EQ(merged.Name(), "");
}

TEST(SchemaTest, StreamOperator_ForPopulatedSchema_RendersNameAndColumns) {
  Schema s("sample", {Column("c1", ValueType::kInt64),
                      Column("c2", ValueType::kDouble)});
  std::ostringstream oss;

  oss << s;

  ASSERT_NE(oss.str().find("sample"), std::string::npos);
  ASSERT_NE(oss.str().find("c1"), std::string::npos);
  ASSERT_NE(oss.str().find("c2"), std::string::npos);
}
}  // namespace tinylamb