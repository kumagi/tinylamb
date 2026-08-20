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

#include "type/column_name.hpp"

#include "gtest/gtest.h"

namespace tinylamb {

TEST(ColumnNameTest, Construct) {
  // Arrange -- nothing more than two ColumnName strings
  // Act -- construct two ColumnName objects (one qualified, one bare)
  ColumnName a("test.ColumnName");
  ColumnName b("ColumnName");
  // Assert -- implicit; no crash, no explicit assertions; gtest green on pass
}

TEST(ColumnNameTest, Check) {
  // Arrange -- two ColumnName strings (one qualified, one bare)
  ColumnName a("test.ColumnName");

  // Act -- inspect schema and name components
  // Assert -- qualified name splits into schema="test", name="ColumnName"
  ASSERT_EQ("test", a.schema);
  ASSERT_EQ("ColumnName", a.name);
  // Act/Assert -- bare name has empty schema, name="foobar"
  ColumnName b("foobar");
  ASSERT_TRUE(b.schema.empty());
  ASSERT_TRUE("foobar");
}

TEST(ColumnNameTest, ToString) {
  // Arrange -- qualified ColumnName "Foo.Bar"
  ColumnName a("Foo.Bar");

  // Act + Assert -- ToString round-trips the original qualified name
  ASSERT_EQ(a.ToString(), "Foo.Bar");
}

TEST(ColumnNameTest, ConstructorsAndEmpty) {
  // Arrange -- no input; default-constructed ColumnName
  // Act -- construct via every public constructor
  ColumnName def;
  ColumnName two("s", "c");
  ColumnName bare("name");
  ColumnName empty_schema("t.");
  ColumnName dot(".");

  // Assert -- default ctor is empty
  EXPECT_TRUE(def.Empty());
  EXPECT_TRUE(def.schema.empty());
  EXPECT_TRUE(def.name.empty());
  // Assert -- the two-argument ctor splits schema and name
  EXPECT_EQ(two.schema, "s");
  EXPECT_EQ(two.name, "c");
  EXPECT_FALSE(two.Empty());
  EXPECT_EQ(two.ToString(), "s.c");
  // Assert -- a bare name has an empty schema
  EXPECT_TRUE(bare.schema.empty());
  EXPECT_EQ(bare.name, "name");
  EXPECT_EQ(bare.ToString(), "name");
  // Assert -- "t." keeps the schema and leaves the name empty
  EXPECT_EQ(empty_schema.schema, "t");
  EXPECT_TRUE(empty_schema.name.empty());
  EXPECT_FALSE(empty_schema.Empty());
  // Assert -- "." parses to an empty schema and name
  EXPECT_TRUE(dot.schema.empty());
  EXPECT_TRUE(dot.name.empty());
  EXPECT_TRUE(dot.Empty());
}

TEST(ColumnNameTest, EqualityOrderingAndHash) {
  // Act -- build equivalent and differing ColumnNames
  ColumnName a("t.c");
  ColumnName b("t", "c");
  ColumnName c("t.d");
  ColumnName d("u.c");

  // Assert -- equality is by schema and name, not by spelling
  EXPECT_EQ(a, b);
  EXPECT_NE(a, c);
  EXPECT_NE(c, d);
  EXPECT_NE(a, ColumnName());
  // Assert -- ordering follows (schema, name) lexicographically
  EXPECT_TRUE(a < c);  // t.c < t.d
  EXPECT_TRUE(a < d);  // t.c < u.c
  EXPECT_FALSE(c < a);
  EXPECT_FALSE(a < b);  // equal names are not less-than
  EXPECT_TRUE(ColumnName("") < a);
  // Assert -- equivalent names hash identically, distinct names do not
  EXPECT_EQ(std::hash<ColumnName>{}(a), std::hash<ColumnName>{}(b));
  EXPECT_NE(std::hash<ColumnName>{}(a), std::hash<ColumnName>{}(c));
}

TEST(ColumnNameTest, StreamsToDumpString) {
  // Act -- stream qualified and bare names into ostringstreams
  std::ostringstream oss_qualified;
  oss_qualified << ColumnName("schema.column");
  std::ostringstream oss_bare;
  oss_bare << ColumnName("column");

  // Assert -- the stream operator renders ToString()
  EXPECT_EQ(oss_qualified.str(), "schema.column");
  EXPECT_EQ(oss_bare.str(), "column");
}

}  // namespace tinylamb
