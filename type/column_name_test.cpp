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
  ColumnName a("test.ColumnName");
  ColumnName b("ColumnName");
}

TEST(ColumnNameTest, Check) {
  ColumnName a("test.ColumnName");
  ASSERT_EQ("test", a.schema);
  ASSERT_EQ("ColumnName", a.name);
  ColumnName b("foobar");
  ASSERT_TRUE(b.schema.empty());
  ASSERT_TRUE("foobar");
}

TEST(ColumnNameTest, ToString) {
  ColumnName a("Foo.Bar");
  ASSERT_EQ(a.ToString(), "Foo.Bar");
}

}  // namespace tinylamb
