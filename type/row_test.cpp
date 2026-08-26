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

#include <cstddef>
#include <functional>
#include <unordered_map>
#include <vector>

#include "gtest/gtest.h"
#include "type/column.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

TEST(RowTest, DefaultConstructor_WhenCalled_CreatesEmptyRow) {
  Row r;

  EXPECT_TRUE(r.values_.empty());
}

TEST(RowTest, SerializeDeserialize_WithNullValues_PreservesNulls) {
  const Schema schema("nullable", {Column("integer", ValueType::kInt64),
                                   Column("text", ValueType::kVarChar),
                                   Column("number", ValueType::kDouble)});
  const Row original({Value(42), Value(), Value(3.5)});
  std::vector<char> buffer(original.Size());

  const size_t written = original.Serialize(buffer.data());
  Row restored;
  const size_t read = restored.Deserialize(buffer.data(), schema);

  EXPECT_EQ(written, original.Size());
  EXPECT_EQ(read, original.Size());
  ASSERT_EQ(restored.values_.size(), 3);
  EXPECT_EQ(restored[0], Value(42));
  EXPECT_TRUE(restored[1].IsNull());
  EXPECT_EQ(restored[2], Value(3.5));
}

TEST(RowTest, DeserializeProjected_WithSubsetIndices_KeepsOnlyRequestedColumns) {
  const Schema schema("projected", {Column("id", ValueType::kInt64),
                                     Column("ignored", ValueType::kVarChar),
                                     Column("nullable", ValueType::kDouble),
                                     Column("name", ValueType::kVarChar)});
  const Row original(
      {Value(7), Value("discard me"), Value(), Value("kept")});
  std::vector<char> buffer(original.Size());
  original.Serialize(buffer.data());

  Row projected;
  const size_t read_proj = projected.DeserializeProjected(buffer.data(), schema, {0, 2, 3});
  Row count_star;
  const size_t read_star = count_star.DeserializeProjected(buffer.data(), schema, {});

  EXPECT_EQ(read_proj, original.Size());
  ASSERT_EQ(projected.values_.size(), 3);
  EXPECT_EQ(projected[0], Value(7));
  EXPECT_TRUE(projected[1].IsNull());
  EXPECT_EQ(projected[2], Value("kept"));
  EXPECT_EQ(read_star, original.Size());
  EXPECT_TRUE(count_star.values_.empty());
}

TEST(RowTest, SerializeRoundTrip_WithDateAndBinaryData_PreservesAllFields) {
  const Schema schema("mixed", {Column("id", ValueType::kInt64),
                                Column("when", ValueType::kDate),
                                Column("blob", ValueType::kVarChar),
                                Column("score", ValueType::kDouble),
                                Column("note", ValueType::kVarChar)});
  const std::string blob("\x00\x01\xff binary \x00 data", 17);
  const Row original({Value(7), Value::Date("2021-03-04"),
                      Value(std::string(blob)), Value(2.5), Value()});
  std::vector<char> buffer(original.Size());

  const size_t written = original.Serialize(buffer.data());
  Row restored;
  const size_t read = restored.Deserialize(buffer.data(), schema);

  EXPECT_EQ(written, original.Size());
  EXPECT_EQ(read, original.Size());
  ASSERT_EQ(restored.values_.size(), 5);
  EXPECT_EQ(restored[0], Value(7));
  EXPECT_EQ(restored[1], Value::Date("2021-03-04"));
  EXPECT_EQ(restored[2], Value(std::string(blob)));
  EXPECT_EQ(restored[3], Value(2.5));
  EXPECT_TRUE(restored[4].IsNull());
}

TEST(RowTest, Hash_WithSwappedValues_ProducesDifferentHashes) {
  std::hash<Row> hasher;
  const Row forward({Value(1), Value(2)});
  const Row swapped({Value(2), Value(1)});

  const size_t h_fwd = hasher(forward);
  const size_t h_swap = hasher(swapped);
  const size_t h_fwd2 = hasher(forward);

  EXPECT_NE(h_fwd, h_swap);
  EXPECT_EQ(h_fwd, h_fwd2);
}

TEST(RowTest, UnorderedMap_WithNullKeyRows_SupportsInsertionAndLookup) {
  std::unordered_map<Row, size_t> groups;

  ++groups[Row({Value(), Value("a")})];
  ++groups[Row({Value(), Value("a")})];
  ++groups[Row({Value(1), Value("a")})];

  ASSERT_EQ(groups.size(), 2U);
  EXPECT_EQ(groups[Row({Value(), Value("a")})], 2U);
}

TEST(RowTest, TryPeekInteger_ForIntAndNonIntColumns_ReturnsValueOrNullopt) {
  const Schema schema("s", {Column("name", ValueType::kVarChar),
                            Column("id", ValueType::kInt64)});
  const Row row({Value("hello"), Value(int64_t{42})});
  std::vector<char> buf(row.Size());
  row.Serialize(buf.data());

  const auto peek_str = Row::TryPeekInteger(buf.data(), schema, 0);
  const auto peek_int = Row::TryPeekInteger(buf.data(), schema, 1);
  const auto peek_oob = Row::TryPeekInteger(buf.data(), schema, 5);

  EXPECT_EQ(peek_str, std::nullopt);
  EXPECT_EQ(peek_int, 42);
  EXPECT_EQ(peek_oob, std::nullopt);
}

TEST(RowTest, Extract_WithOutOfBoundsIndices_ReturnsEmptyRow) {
  const Row row({Value(1), Value(2)});

  const Row extracted = row.Extract({5});

  EXPECT_EQ(extracted.values_.size(), 0U);
}

}  // namespace tinylamb

