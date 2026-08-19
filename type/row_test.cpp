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

#include "type/row.hpp"

#include "gtest/gtest.h"
#include "type/column.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

TEST(RowTest, construct) {
  // Arrange -- nothing more than default Row ctor
  // Act -- default-construct a Row
  Row r;
  // Assert -- implicit; no crash, no explicit assertions; gtest green on pass
}

TEST(RowTest, SerializeDeserializePreservesNulls) {
  const Schema schema("nullable", {Column("integer", ValueType::kInt64),
                                   Column("text", ValueType::kVarChar),
                                   Column("number", ValueType::kDouble)});
  const Row original({Value(42), Value(), Value(3.5)});
  std::vector<char> buffer(original.Size());

  EXPECT_EQ(original.Serialize(buffer.data()), original.Size());
  Row restored;
  EXPECT_EQ(restored.Deserialize(buffer.data(), schema), original.Size());

  ASSERT_EQ(restored.values_.size(), 3);
  EXPECT_EQ(restored[0], Value(42));
  EXPECT_TRUE(restored[1].IsNull());
  EXPECT_EQ(restored[2], Value(3.5));
}

TEST(RowTest, DeserializeProjectedKeepsOnlyRequestedColumns) {
  const Schema schema("projected", {Column("id", ValueType::kInt64),
                                     Column("ignored", ValueType::kVarChar),
                                     Column("nullable", ValueType::kDouble),
                                     Column("name", ValueType::kVarChar)});
  const Row original(
      {Value(7), Value("discard me"), Value(), Value("kept")});
  std::vector<char> buffer(original.Size());
  original.Serialize(buffer.data());

  Row projected;
  EXPECT_EQ(projected.DeserializeProjected(buffer.data(), schema, {0, 2, 3}),
            original.Size());
  ASSERT_EQ(projected.values_.size(), 3);
  EXPECT_EQ(projected[0], Value(7));
  EXPECT_TRUE(projected[1].IsNull());
  EXPECT_EQ(projected[2], Value("kept"));

  Row count_star;
  EXPECT_EQ(count_star.DeserializeProjected(buffer.data(), schema, {}),
            original.Size());
  EXPECT_TRUE(count_star.values_.empty());
}

}  // namespace tinylamb
