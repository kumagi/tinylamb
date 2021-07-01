#include "gtest/gtest.h"
#include "schema.hpp"

namespace tinylamb {

TEST(SchemaTest, ConstructEmptySchema) {
  Schema s("foo");
  ASSERT_EQ(s.Name(), "foo");
  ASSERT_TRUE(s.OwnData());
  ASSERT_EQ(s.Size(), 8 + 1 + 3 + 1);
  ASSERT_EQ(s.ColumnCount(), 0);
  std::cout << s << "\n";
}

TEST(SchemaTest, ReadEmptySchema) {
  Schema s("foo");
  Schema t = Schema::Read(s.Data());
  ASSERT_EQ(t.Name(), "foo");
  ASSERT_FALSE(t.OwnData());
  ASSERT_EQ(t.Size(), 8 + 1 + 3 + 1);
  ASSERT_EQ(t.ColumnCount(), 0);
  std::cout << t << "\n";
}

TEST(SchemaTest, Construct) {
  Schema s("piyo");
  s.AddColumn("intc", ValueType::kInt64, 8, 0);
  s.AddColumn("charc", ValueType::kVarChar, 12, 0);
  ASSERT_EQ(s.Name(), "piyo");
  ASSERT_TRUE(s.OwnData());
  ASSERT_EQ(s.Size(), 8 + 1 + 4 + 1
                              + s.GetColumn(0).PhysicalSize()
                              + s.GetColumn(1).PhysicalSize());
  ASSERT_EQ(s.ColumnCount(), 2);
  std::cout << s << "\n";
}

TEST(SchemaTest, ReadSchema) {
  Schema s("piyo");
  s.AddColumn("intc", ValueType::kInt64, 8, 0);
  s.AddColumn("charc", ValueType::kVarChar, 12, 0);
  Schema t = Schema::Read(s.Data());
  ASSERT_EQ(t.Name(), "piyo");
  ASSERT_FALSE(t.OwnData());
  ASSERT_EQ(t.Size(), 8 + 1 + 4 + 1
                              + s.GetColumn(0).PhysicalSize()
                              + s.GetColumn(1).PhysicalSize());
  ASSERT_EQ(t.ColumnCount(), 2);
  std::cout << t << "\n";
}

}  // namespace tinylamb