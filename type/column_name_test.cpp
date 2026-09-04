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

#include <sstream>

#include "gtest/gtest.h"

namespace tinylamb {

TEST(ColumnNameTest,
     Constructor_WithQualifiedAndBareStrings_SplitsSchemaAndName) {
  ColumnName a("test.ColumnName");
  ColumnName b("foobar");

  EXPECT_EQ(a.schema, "test");
  EXPECT_EQ(a.name, "ColumnName");
  EXPECT_TRUE(b.schema.empty());
  EXPECT_EQ(b.name, "foobar");
}

TEST(ColumnNameTest, ToString_WhenQualified_ReturnsOriginalString) {
  ColumnName a("Foo.Bar");

  const std::string result = a.ToString();

  ASSERT_EQ(result, "Foo.Bar");
}

TEST(ColumnNameTest,
     Constructors_WithVariousInputFormats_PopulateFieldsCorrectly) {
  ColumnName def;
  ColumnName two("s", "c");
  ColumnName bare("name");
  ColumnName empty_schema("t.");
  ColumnName dot(".");

  EXPECT_TRUE(def.Empty());
  EXPECT_TRUE(def.schema.empty());
  EXPECT_TRUE(def.name.empty());
  EXPECT_EQ(two.schema, "s");
  EXPECT_EQ(two.name, "c");
  EXPECT_FALSE(two.Empty());
  EXPECT_EQ(two.ToString(), "s.c");
  EXPECT_TRUE(bare.schema.empty());
  EXPECT_EQ(bare.name, "name");
  EXPECT_EQ(bare.ToString(), "name");
  EXPECT_EQ(empty_schema.schema, "t");
  EXPECT_TRUE(empty_schema.name.empty());
  EXPECT_FALSE(empty_schema.Empty());
  EXPECT_TRUE(dot.schema.empty());
  EXPECT_TRUE(dot.name.empty());
  EXPECT_TRUE(dot.Empty());
}

TEST(ColumnNameTest,
     Operators_WithMultipleInstances_EvaluatesEqualityOrderingAndHash) {
  ColumnName a("t.c");
  ColumnName b("t", "c");
  ColumnName c("t.d");
  ColumnName d("u.c");

  const bool eq_ab = (a == b);
  const bool ne_ac = (a != c);
  const bool ne_cd = (c != d);
  const bool ne_def = (a != ColumnName());
  const bool lt_ac = (a < c);
  const bool lt_ad = (a < d);
  const bool lt_ca = (c < a);
  const bool lt_ab = (a < b);
  const bool lt_empty = (ColumnName("") < a);
  const size_t hash_a = std::hash<ColumnName>{}(a);
  const size_t hash_b = std::hash<ColumnName>{}(b);
  const size_t hash_c = std::hash<ColumnName>{}(c);

  EXPECT_TRUE(eq_ab);
  EXPECT_TRUE(ne_ac);
  EXPECT_TRUE(ne_cd);
  EXPECT_TRUE(ne_def);
  EXPECT_TRUE(lt_ac);
  EXPECT_TRUE(lt_ad);
  EXPECT_FALSE(lt_ca);
  EXPECT_FALSE(lt_ab);
  EXPECT_TRUE(lt_empty);
  EXPECT_EQ(hash_a, hash_b);
  EXPECT_NE(hash_a, hash_c);
}

TEST(ColumnNameTest,
     StreamOperator_ForQualifiedAndBareNames_RendersMatchingString) {
  std::ostringstream oss_qualified;
  std::ostringstream oss_bare;

  oss_qualified << ColumnName("schema.column");
  oss_bare << ColumnName("column");

  EXPECT_EQ(oss_qualified.str(), "schema.column");
  EXPECT_EQ(oss_bare.str(), "column");
}

}  // namespace tinylamb
