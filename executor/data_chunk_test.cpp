/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/data_chunk.hpp"

#include "gtest/gtest.h"
#include "type/column.hpp"

namespace tinylamb {

TEST(DataChunkTest, StoresTypedColumnsNullsAndRowPositions) {
  const Schema schema("batch", {Column("id", ValueType::kInt64),
                                 Column("name", ValueType::kVarChar),
                                 Column("score", ValueType::kDouble)});
  DataChunk chunk(schema, 4);
  chunk.Append(Row({Value(1), Value("one"), Value(1.5)}),
               RowPosition(7, 2));
  chunk.Append(Row({Value(2), Value(), Value(2.5)}), RowPosition(7, 3));

  ASSERT_EQ(chunk.Size(), 2);
  ASSERT_EQ(chunk.ColumnCount(), 3);
  EXPECT_EQ(chunk.ColumnAt(0).ValueAt(1), Value(2));
  EXPECT_TRUE(chunk.ColumnAt(1).IsNull(1));
  EXPECT_EQ(chunk.ColumnAt(1).NullBitmap()[0] & uint64_t{2}, uint64_t{2});
  EXPECT_EQ(chunk.RowAt(0), Row({Value(1), Value("one"), Value(1.5)}));
  EXPECT_EQ(chunk.PositionAt(1), RowPosition(7, 3));

  chunk.Reset();
  EXPECT_TRUE(chunk.Empty());
  EXPECT_EQ(chunk.ColumnCount(), 3);
}

TEST(DataChunkTest, SupportsZeroColumnCountStarBatches) {
  DataChunk chunk(std::vector<ValueType>{}, 2);
  chunk.Append(Row(), RowPosition(1, 1));
  chunk.Append(Row(), RowPosition(1, 2));
  EXPECT_EQ(chunk.Size(), 2);
  EXPECT_EQ(chunk.ColumnCount(), 0);
  EXPECT_TRUE(chunk.RowAt(0).values_.empty());
}

TEST(DataChunkTest, CopiesSelectedRowsBetweenChunks) {
  DataChunk input(std::vector<ValueType>{ValueType::kInt64,
                                         ValueType::kVarChar});
  input.Append(Row({Value(1), Value("drop")}));
  input.Append(Row({Value(2), Value("keep")}));
  DataChunk output;
  output.Append(input, 1);
  ASSERT_EQ(output.Size(), 1);
  EXPECT_EQ(output.RowAt(0), Row({Value(2), Value("keep")}));
}

TEST(DataChunkTest, MaintainsZoneMapsAcrossNullsAndReset) {
  DataChunk chunk(std::vector<ValueType>{ValueType::kInt64});
  chunk.Append(Row({Value(9)}));
  chunk.Append(Row({Value()}));
  chunk.Append(Row({Value(2)}));
  chunk.Append(Row({Value(7)}));
  const ZoneMap& zone = chunk.ZoneMapAt(0);
  ASSERT_TRUE(zone.Minimum());
  ASSERT_TRUE(zone.Maximum());
  EXPECT_EQ(*zone.Minimum(), Value(2));
  EXPECT_EQ(*zone.Maximum(), Value(9));
  EXPECT_EQ(zone.NullCount(), 1U);
  EXPECT_FALSE(zone.MayMatch(BinaryOperation::kGreaterThan, Value(20)));
  EXPECT_TRUE(zone.MayMatch(BinaryOperation::kGreaterThanEquals, Value(9)));

  chunk.Reset();
  EXPECT_FALSE(chunk.ZoneMapAt(0).Minimum());
  EXPECT_EQ(chunk.ZoneMapAt(0).ValueCount(), 0U);
}

}  // namespace tinylamb
