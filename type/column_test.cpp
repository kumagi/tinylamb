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
}  // namespace tinylamb
