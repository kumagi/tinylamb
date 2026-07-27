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
}  // namespace tinylamb