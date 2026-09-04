/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "gtest/gtest.h"
#include "page/page.hpp"
#include "type/date.hpp"

namespace tinylamb {

TEST(PaxPageTest, PageTypePersistsColumnarChunk) {
  const Schema schema(
      "pax",
      {Column("id", ValueType::kInt64), Column("price", ValueType::kDouble),
       Column("name", ValueType::kVarChar), Column("day", ValueType::kDate)});
  DataChunk input(schema, 3);
  input.Append(Row({Value(int64_t{7}), Value(1.5), Value("one"),
                    Value::Date("2026-08-24")}));
  input.Append(Row({Value(), Value(-0.0), Value(""), Value()}));
  input.Append(Row({Value(int64_t{-9}), Value(3.25), Value("three"),
                    Value::Date("1970-01-01")}));

  Page page(41, PageType::kPaxPage);
  ASSERT_EQ(page.body.pax_page.Store(input), Status::kSuccess);
  ASSERT_EQ(page.Type(), PageType::kPaxPage);
  auto restored = page.body.pax_page.Load();
  ASSERT_TRUE(restored.HasValue());
  ASSERT_EQ(restored.Value().Size(), input.Size());
  for (size_t row = 0; row < input.Size(); ++row) {
    EXPECT_EQ(restored.Value().RowAt(row), input.RowAt(row));
  }
}

TEST(PaxPageTest, RejectsChunkThatDoesNotFitOnePage) {
  const Schema schema("pax", {Column("payload", ValueType::kVarChar)});
  DataChunk input(schema, 2);
  input.Append(Row({Value(std::string(kPageBodySize, 'x'))}));
  Page page(42, PageType::kPaxPage);
  EXPECT_EQ(page.body.pax_page.Store(input), Status::kNoSpace);
}

}  // namespace tinylamb
