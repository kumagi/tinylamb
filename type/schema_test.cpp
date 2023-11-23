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

#include "common/test_util.hpp"
#include "gtest/gtest.h"

namespace tinylamb {

TEST(SchemaTest, Construct) {
  Schema s("sample", {Column(ColumnName("c1"), ValueType::kInt64,
                             Constraint(Constraint::kPrimaryKey)),
                      Column(ColumnName("c2"), ValueType::kDouble)});
  Schema t("next_schema", {Column(ColumnName("c1"), ValueType::kInt64,
                                  Constraint(Constraint::kPrimaryKey)),
                           Column(ColumnName("c2"), ValueType::kDouble),
                           Column(ColumnName("c3"), ValueType::kVarChar)});
}

TEST(SchemaTest, SerializeDeserialize) {
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

TEST(SchemaTest, Dump) {
  LOG(INFO) << Schema("sample", {Column(ColumnName("c1"), ValueType::kInt64,
                                        Constraint(Constraint::kPrimaryKey)),
                                 Column(ColumnName("c2"), ValueType::kDouble)});
  LOG(WARN) << Schema("next_schema",
                      {Column(ColumnName("c1"), ValueType::kInt64,
                              Constraint(Constraint::kPrimaryKey)),
                       Column(ColumnName("c2"), ValueType::kDouble),
                       Column(ColumnName("c3"), ValueType::kVarChar)});
}
}  // namespace tinylamb