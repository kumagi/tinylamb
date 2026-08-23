/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/data_chunk.hpp"
#include <cstdint>
#include <optional>
#include <vector>
#include <stdexcept>
#include <utility>

#include "executor/zone_map.hpp"
#include "common/constants.hpp"
#include "gtest/gtest.h"
#include "type/column.hpp"
#include "type/schema.hpp"
#include "type/value_type.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

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
  if (!zone.Minimum() || !zone.Maximum()) {
    GTEST_FAIL() << "zone map not populated";
    return;
  }
  EXPECT_EQ(*zone.Minimum(), Value(2));
  EXPECT_EQ(*zone.Maximum(), Value(9));
  EXPECT_EQ(zone.NullCount(), 1U);
  EXPECT_FALSE(zone.MayMatch(BinaryOperation::kGreaterThan, Value(20)));
  EXPECT_TRUE(zone.MayMatch(BinaryOperation::kGreaterThanEquals, Value(9)));

  chunk.Reset();
  EXPECT_FALSE(chunk.ZoneMapAt(0).Minimum());
  EXPECT_EQ(chunk.ZoneMapAt(0).ValueCount(), 0U);
}

TEST(DataChunkTest, ResetWithSchemaOverridesInferredNullTypes) {
  DataChunk chunk;
  chunk.Append(Row({Value(), Value("a")}));
  const Schema schema("orders", {Column("carrier", ValueType::kInt64),
                                 Column("name", ValueType::kVarChar)});
  chunk.Reset(schema, 4);
  EXPECT_EQ(chunk.ColumnAt(0).Type(), ValueType::kInt64);
  chunk.Append(Row({Value(), Value("b")}));
  chunk.Append(Row({Value(int64_t{9}), Value("c")}));
  EXPECT_TRUE(chunk.ColumnAt(0).IsNull(0));
  EXPECT_EQ(chunk.ColumnAt(0).ValueAt(1), Value(int64_t{9}));
}

TEST(DataChunkTest, InfersNullThenPromotesType) {
  DataChunk chunk;
  chunk.Append(Row({Value(), Value("a")}));
  chunk.Append(Row({Value(int64_t{7}), Value("b")}));
  ASSERT_EQ(chunk.Size(), 2);
  EXPECT_TRUE(chunk.ColumnAt(0).IsNull(0));
  EXPECT_EQ(chunk.ColumnAt(0).ValueAt(1), Value(int64_t{7}));
  EXPECT_EQ(chunk.ColumnAt(0).Type(), ValueType::kInt64);
}

TEST(DataChunkTest, AppendTypeMismatchThrows) {
  DataChunk chunk(std::vector<ValueType>{ValueType::kInt64});
  // Appending a double into an int64 column must be rejected.
  EXPECT_THROW(chunk.Append(Row({Value(1.5)})), std::invalid_argument);
  // A failed append must not poison zone maps: a later int64 append succeeds.
  EXPECT_NO_THROW(chunk.Append(Row({Value(int64_t{3})})));
  ASSERT_EQ(chunk.Size(), 1);
  EXPECT_EQ(chunk.ColumnAt(0).ValueAt(0), Value(int64_t{3}));
}

TEST(DataChunkTest, AppendNullToDoubleColumnKeepsType) {
  DataChunk chunk(std::vector<ValueType>{ValueType::kDouble});
  chunk.Append(Row({Value()}));
  chunk.Append(Row({Value(2.5)}));
  ASSERT_EQ(chunk.Size(), 2);
  EXPECT_EQ(chunk.ColumnAt(0).Type(), ValueType::kDouble);
  EXPECT_TRUE(chunk.ColumnAt(0).IsNull(0));
  EXPECT_EQ(chunk.ColumnAt(0).ValueAt(0), Value());
  EXPECT_EQ(chunk.ColumnAt(0).ValueAt(1), Value(2.5));
}

TEST(DataChunkTest, InfersDoubleStorageFromFirstNonNull) {
  DataChunk chunk;
  chunk.Append(Row({Value()}));
  chunk.Append(Row({Value(1.5)}));
  ASSERT_EQ(chunk.Size(), 2);
  EXPECT_EQ(chunk.ColumnAt(0).Type(), ValueType::kDouble);
  EXPECT_TRUE(chunk.ColumnAt(0).IsNull(0));
  EXPECT_EQ(chunk.ColumnAt(0).ValueAt(1), Value(1.5));
  EXPECT_EQ(chunk.RowAt(1), Row({Value(1.5)}));
}

TEST(DataChunkTest, HasLayoutMatchesSchema) {
  const Schema schema("t", {Column("id", ValueType::kInt64),
                             Column("name", ValueType::kVarChar)});
  DataChunk chunk(schema);
  EXPECT_TRUE(chunk.HasLayout(schema));
  const Schema other("t2", {Column("id", ValueType::kDouble),
                            Column("name", ValueType::kVarChar)});
  EXPECT_FALSE(chunk.HasLayout(other));
  const Schema wide("t3", {Column("id", ValueType::kInt64)});
  EXPECT_FALSE(chunk.HasLayout(wide));
}

TEST(DataChunkTest, HasLayoutAcceptsNullTypeColumns) {
  DataChunk chunk;
  chunk.Append(Row({Value()}));
  const Schema schema("t", {Column("id", ValueType::kInt64)});
  EXPECT_TRUE(chunk.HasLayout(schema));
}

TEST(DataChunkTest, AppendChunkWidthMismatchThrows) {
  DataChunk source(std::vector<ValueType>{ValueType::kInt64,
                                          ValueType::kVarChar});
  source.Append(Row({Value(1), Value("x")}));
  DataChunk target(std::vector<ValueType>{ValueType::kInt64});
  EXPECT_ANY_THROW(target.Append(source, 0));
}

TEST(DataChunkTest, AppendLvalueRowWithPositions) {
  DataChunk chunk(std::vector<ValueType>{ValueType::kInt64,
                                         ValueType::kVarChar});
  Row row({Value(10), Value("ten")});
  chunk.Append(row, RowPosition(3, 5));
  Row row2({Value(20), Value("twenty")});
  chunk.Append(row2, RowPosition(3, 6));
  ASSERT_EQ(chunk.Size(), 2);
  EXPECT_EQ(chunk.PositionAt(0), RowPosition(3, 5));
  EXPECT_EQ(chunk.PositionAt(1), RowPosition(3, 6));
  EXPECT_EQ(chunk.RowAt(1), Row({Value(20), Value("twenty")}));
}

TEST(DataChunkTest, AppendDateColumnRoundTrips) {
  DataChunk chunk(std::vector<ValueType>{ValueType::kDate});
  chunk.Append(Row({Value::DateFromDays(5)}));
  chunk.Append(Row({Value()}));
  chunk.Append(Row({Value::DateFromDays(3)}));
  ASSERT_EQ(chunk.Size(), 3);
  EXPECT_EQ(chunk.ColumnAt(0).ValueAt(0), Value::DateFromDays(5));
  EXPECT_TRUE(chunk.ColumnAt(0).IsNull(1));
  EXPECT_EQ(chunk.ColumnAt(0).ValueAt(1), Value());
  EXPECT_EQ(chunk.RowAt(2), Row({Value::DateFromDays(3)}));
}

TEST(DataChunkTest, AppendRowCopiesValuesForLaterMutation) {
  DataChunk chunk(std::vector<ValueType>{ValueType::kVarChar});
  std::string text = "hello";
  Row row({Value(std::move(text))});
  chunk.Append(row);
  row.values_[0] = Value("mutated");
  EXPECT_EQ(chunk.RowAt(0), Row({Value("hello")}));
}

TEST(DataChunkTest, ReserveThenAppendBeyondCapacity) {
  DataChunk chunk(std::vector<ValueType>{ValueType::kInt64,
                                         ValueType::kVarChar},
                  2);
  chunk.Reserve(8);
  for (int i = 0; i < 20; ++i) {
    chunk.Append(Row({Value(int64_t{i}), Value("row")}),
                 RowPosition(1, static_cast<slot_t>(i)));
  }
  ASSERT_EQ(chunk.Size(), 20);
  for (int i = 0; i < 20; ++i) {
    EXPECT_EQ(chunk.RowAt(i), Row({Value(int64_t{i}), Value("row")}));
    EXPECT_EQ(chunk.PositionAt(i), RowPosition(1, static_cast<slot_t>(i)));
  }
}

TEST(DataChunkTest, AppendRowFromColumnsBuildsRowsColumnWise) {
  DataChunk input(std::vector<ValueType>{ValueType::kInt64,
                                         ValueType::kVarChar});
  input.Append(Row({Value(1), Value("one")}), RowPosition(2, 0));
  input.Append(Row({Value(), Value()}), RowPosition(2, 1));
  input.Append(Row({Value(3), Value("three")}), RowPosition(2, 2));

  DataChunk projected;
  std::vector<const ColumnVector*> sources = {&input.ColumnAt(1),
                                              &input.ColumnAt(0)};
  projected.AppendRowFromColumns(sources, 0, input.PositionAt(0));
  projected.AppendRowFromColumns(sources, 2, input.PositionAt(2));

  ASSERT_EQ(projected.Size(), 2);
  ASSERT_EQ(projected.ColumnCount(), 2);
  EXPECT_EQ(projected.ColumnAt(0).Type(), ValueType::kVarChar);
  EXPECT_EQ(projected.ColumnAt(1).Type(), ValueType::kInt64);
  EXPECT_EQ(projected.RowAt(0), Row({Value("one"), Value(1)}));
  EXPECT_EQ(projected.RowAt(1), Row({Value("three"), Value(3)}));
  EXPECT_EQ(projected.PositionAt(1), RowPosition(2, 2));
  const ZoneMap& names = projected.ZoneMapAt(0);
  if (!names.Minimum() || !names.Maximum()) {
    GTEST_FAIL() << "zone map not populated";
    return;
  }
  EXPECT_EQ(*names.Minimum(), Value("one"));
  EXPECT_EQ(*names.Maximum(), Value("three"));
  EXPECT_EQ(projected.ZoneMapAt(1).NullCount(), 0U);
}

TEST(DataChunkTest, AppendRowFromColumnsInfersNullOnlyColumns) {
  DataChunk input(std::vector<ValueType>{ValueType::kInt64});
  input.Append(Row({Value()}));
  input.Append(Row({Value(5)}));

  DataChunk projected;
  std::vector<const ColumnVector*> sources = {&input.ColumnAt(0)};
  projected.AppendRowFromColumns(sources, 0);
  projected.AppendRowFromColumns(sources, 1);
  ASSERT_EQ(projected.Size(), 2);
  EXPECT_TRUE(projected.ColumnAt(0).IsNull(0));
  EXPECT_FALSE(projected.ColumnAt(0).IsNull(1));
  EXPECT_EQ(projected.ColumnAt(0).ValueAt(1), Value(5));
  EXPECT_EQ(projected.ZoneMapAt(0).NullCount(), 1U);
  EXPECT_EQ(projected.ZoneMapAt(0).ValueCount(), 1U);
}

TEST(DataChunkTest, AppendRowFromColumnsWidthMismatchThrows) {
  DataChunk input(std::vector<ValueType>{ValueType::kInt64,
                                         ValueType::kVarChar});
  input.Append(Row({Value(1), Value("x")}));
  DataChunk target(std::vector<ValueType>{ValueType::kInt64});
  std::vector<const ColumnVector*> sources = {&input.ColumnAt(0),
                                              &input.ColumnAt(1)};
  EXPECT_ANY_THROW(target.AppendRowFromColumns(sources, 0));
}

TEST(DataChunkTest, CopyRowBetweenChunksPreservesNullsAndZoneMap) {
  DataChunk input(std::vector<ValueType>{ValueType::kInt64,
                                         ValueType::kVarChar,
                                         ValueType::kDate});
  input.Append(Row({Value(4), Value("b"), Value::DateFromDays(9)}));
  input.Append(Row({Value(), Value("a"), Value()}));
  DataChunk output;
  output.Append(input, 1);
  output.Append(input, 0);
  ASSERT_EQ(output.Size(), 2);
  EXPECT_EQ(output.ColumnAt(0).Type(), ValueType::kInt64);
  EXPECT_TRUE(output.ColumnAt(0).IsNull(0));
  EXPECT_EQ(output.ColumnAt(0).ValueAt(1), Value(4));
  EXPECT_EQ(output.ColumnAt(1).ValueAt(0), Value("a"));
  EXPECT_EQ(output.ColumnAt(2).ValueAt(1), Value::DateFromDays(9));
  // Zone maps must survive the unboxed copy, including the date type.
  const std::optional<Value> date_minimum = output.ZoneMapAt(2).Minimum();
  if (!date_minimum) {
    GTEST_FAIL() << "zone map not populated";
    return;
  }
  EXPECT_EQ(*date_minimum, Value::DateFromDays(9));
  EXPECT_TRUE(output.ZoneMapAt(2).MayMatch(BinaryOperation::kEquals,
                                           Value::DateFromDays(9)));
  EXPECT_FALSE(output.ZoneMapAt(2).MayMatch(BinaryOperation::kEquals,
                                            Value::DateFromDays(10)));
  EXPECT_EQ(output.ZoneMapAt(0).NullCount(), 1U);
  EXPECT_EQ(output.ZoneMapAt(1).Minimum(), Value("a"));
}

TEST(DataChunkTest, CopyRowTypeMismatchThrowsWithoutPartialAppend) {
  DataChunk source(std::vector<ValueType>{ValueType::kDouble});
  source.Append(Row({Value(1.5)}));
  DataChunk target(std::vector<ValueType>{ValueType::kInt64});
  EXPECT_THROW(target.Append(source, 0), std::invalid_argument);
  EXPECT_EQ(target.Size(), 0U);
  EXPECT_NO_THROW(target.Append(Row({Value(int64_t{2})})));
}

}  // namespace tinylamb
