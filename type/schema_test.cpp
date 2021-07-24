#include "schema.hpp"

#include "gtest/gtest.h"

namespace tinylamb {

TEST(SchemaTest, ConstructEmptySchema) {
  Schema s("foo", {}, 0);
  ASSERT_EQ(s.Name(), "foo");
  ASSERT_TRUE(s.OwnData());
  ASSERT_EQ(s.Size(), 8 + 1 + 3 + 1);
  ASSERT_EQ(s.ColumnCount(), 0);
  std::cout << s << "\n";
}

TEST(SchemaTest, ReadEmptySchema) {
  Schema s("foo", {}, 0);
  Schema t = Schema(s.Data().data());
  ASSERT_EQ(t.Name(), "foo");
  ASSERT_FALSE(t.OwnData());
  ASSERT_EQ(t.Size(), 8 + 1 + 3 + 1);
  ASSERT_EQ(t.ColumnCount(), 0);
  std::cout << t << "\n";
}

TEST(SchemaTest, Construct) {
  Column c1("intc", ValueType::kInt64, 8, Restriction::kNoRestriction, 0);
  Column c2("charc", ValueType::kVarChar, 12, Restriction::kNoRestriction, 8);
  std::vector<Column> columns = {c1, c2};
  Schema s("piyo", columns, 2);
  ASSERT_EQ(s.Name(), "piyo");
  ASSERT_TRUE(s.OwnData());
  std::cout << s.GetColumn(0) << " c1\n";
  std::cout << s.GetColumn(1) << " c2\n";
  ASSERT_EQ(s.GetColumn(0), c1);
  ASSERT_EQ(s.GetColumn(1), c2);
  ASSERT_EQ(s.Size(), 8 + 1 + 4 + 1 + s.GetColumn(0).data.size() +
                          s.GetColumn(1).data.size());
  ASSERT_EQ(s.ColumnCount(), 2);
  std::cout << s << "\n";
}

TEST(SchemaTest, CopySchema) {
  Column c1("intc", ValueType::kInt64, 8, Restriction::kNoRestriction, 0);
  Column c2("charc", ValueType::kVarChar, 12, Restriction::kNoRestriction, 8);
  std::vector<Column> columns = {c1, c2};
  Schema s("piyo", columns, 2);
  Schema t(s.Data().data());
  ASSERT_FALSE(t.OwnData());
  ASSERT_EQ(t.Name(), "piyo");
  ASSERT_EQ(t.GetColumn(0), c1);
  ASSERT_EQ(t.GetColumn(1), c2);
  ASSERT_EQ(t.Size(), 8 + 1 + 4 + 1 + s.GetColumn(0).data.size() +
                          s.GetColumn(1).data.size());
  ASSERT_EQ(t.ColumnCount(), 2);
  std::cout << t << "\n";
}

}  // namespace tinylamb