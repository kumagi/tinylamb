//
// Created by kumagi on 22/07/07.
//
#include "type/column_name.hpp"

#include "common/test_util.hpp"
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
