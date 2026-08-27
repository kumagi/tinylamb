/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/data_chunk.hpp"
#include <cstdint>
#include <optional>
#include <vector>
#include <stdexcept>
#include <utility>

#include "executor/dictionary_batch_aggregation.hpp"
#include "executor/selection_vector.hpp"
#include "executor/simd_comparison.hpp"
#include "executor/vectorized_expression.hpp"
#include "executor/zone_map.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "common/constants.hpp"
#include "gtest/gtest.h"
#include "type/column.hpp"
#include "type/schema.hpp"
#include "type/value_type.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

TEST(DataChunkTest, Append_TypedColumnsAndNulls_StoresCorrectValuesAndPositions) {
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

TEST(DataChunkTest, Append_ZeroColumns_StoresRowCount) {
  DataChunk chunk(std::vector<ValueType>{}, 2);
  chunk.Append(Row(), RowPosition(1, 1));
  chunk.Append(Row(), RowPosition(1, 2));
  EXPECT_EQ(chunk.Size(), 2);
  EXPECT_EQ(chunk.ColumnCount(), 0);
  EXPECT_TRUE(chunk.RowAt(0).values_.empty());
}

TEST(DataChunkTest, Append_FromAnotherChunk_CopiesSelectedRows) {
  DataChunk input(std::vector<ValueType>{ValueType::kInt64,
                                         ValueType::kVarChar});
  input.Append(Row({Value(1), Value("drop")}));
  input.Append(Row({Value(2), Value("keep")}));
  DataChunk output;
  output.Append(input, 1);
  ASSERT_EQ(output.Size(), 1);
  EXPECT_EQ(output.RowAt(0), Row({Value(2), Value("keep")}));
}

TEST(DataChunkTest, ZoneMap_AfterAppendsAndReset_MaintainsStatistics) {
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

TEST(DataChunkTest, Reset_WithSchema_OverridesInferredNullTypes) {
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

TEST(DataChunkTest, Append_NullThenNonNull_InfersAndPromotesType) {
  DataChunk chunk;
  chunk.Append(Row({Value(), Value("a")}));
  chunk.Append(Row({Value(int64_t{7}), Value("b")}));
  ASSERT_EQ(chunk.Size(), 2);
  EXPECT_TRUE(chunk.ColumnAt(0).IsNull(0));
  EXPECT_EQ(chunk.ColumnAt(0).ValueAt(1), Value(int64_t{7}));
  EXPECT_EQ(chunk.ColumnAt(0).Type(), ValueType::kInt64);
}

TEST(DataChunkTest, Append_TypeMismatch_ThrowsInvalidArgument) {
  DataChunk chunk(std::vector<ValueType>{ValueType::kInt64});
  // Appending a double into an int64 column must be rejected.
  EXPECT_THROW(chunk.Append(Row({Value(1.5)})), std::invalid_argument);
  // A failed append must not poison zone maps: a later int64 append succeeds.
  EXPECT_NO_THROW(chunk.Append(Row({Value(int64_t{3})})));
  ASSERT_EQ(chunk.Size(), 1);
  EXPECT_EQ(chunk.ColumnAt(0).ValueAt(0), Value(int64_t{3}));
}

TEST(DataChunkTest, Append_NullToDoubleColumn_PreservesType) {
  DataChunk chunk(std::vector<ValueType>{ValueType::kDouble});
  chunk.Append(Row({Value()}));
  chunk.Append(Row({Value(2.5)}));
  ASSERT_EQ(chunk.Size(), 2);
  EXPECT_EQ(chunk.ColumnAt(0).Type(), ValueType::kDouble);
  EXPECT_TRUE(chunk.ColumnAt(0).IsNull(0));
  EXPECT_EQ(chunk.ColumnAt(0).ValueAt(0), Value());
  EXPECT_EQ(chunk.ColumnAt(0).ValueAt(1), Value(2.5));
}

TEST(DataChunkTest, Append_NullThenDouble_InfersDoubleType) {
  DataChunk chunk;
  chunk.Append(Row({Value()}));
  chunk.Append(Row({Value(1.5)}));
  ASSERT_EQ(chunk.Size(), 2);
  EXPECT_EQ(chunk.ColumnAt(0).Type(), ValueType::kDouble);
  EXPECT_TRUE(chunk.ColumnAt(0).IsNull(0));
  EXPECT_EQ(chunk.ColumnAt(0).ValueAt(1), Value(1.5));
  EXPECT_EQ(chunk.RowAt(1), Row({Value(1.5)}));
}

TEST(DataChunkTest, HasLayout_DifferentSchemas_ReturnsExpectedBoolean) {
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

TEST(DataChunkTest, HasLayout_WithNullColumn_MatchesSchema) {
  DataChunk chunk;
  chunk.Append(Row({Value()}));
  const Schema schema("t", {Column("id", ValueType::kInt64)});
  EXPECT_TRUE(chunk.HasLayout(schema));
}

TEST(DataChunkTest, Append_ChunkWidthMismatch_Throws) {
  DataChunk source(std::vector<ValueType>{ValueType::kInt64,
                                          ValueType::kVarChar});
  source.Append(Row({Value(1), Value("x")}));
  DataChunk target(std::vector<ValueType>{ValueType::kInt64});
  EXPECT_ANY_THROW(target.Append(source, 0));
}

TEST(DataChunkTest, Append_LvalueRowWithPositions_StoresCorrectly) {
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

TEST(DataChunkTest, Append_DateColumn_RoundTripsCorrectly) {
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

TEST(DataChunkTest, Append_RowValueMutatedAfterAppend_KeepsOriginalValue) {
  DataChunk chunk(std::vector<ValueType>{ValueType::kVarChar});
  std::string text = "hello";
  Row row({Value(std::move(text))});
  chunk.Append(row);
  row.values_[0] = Value("mutated");
  EXPECT_EQ(chunk.RowAt(0), Row({Value("hello")}));
}

TEST(DataChunkTest, Reserve_ThenAppendBeyondCapacity_GrowsAndStoresData) {
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

TEST(DataChunkTest, AppendRowFromColumns_GivenColumnSources_BuildsRowsColumnWise) {
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

TEST(DataChunkTest, AppendRowFromColumns_WithNullValues_InfersNullOnlyColumns) {
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

TEST(DataChunkTest, AppendRowFromColumns_WidthMismatch_Throws) {
  DataChunk input(std::vector<ValueType>{ValueType::kInt64,
                                         ValueType::kVarChar});
  input.Append(Row({Value(1), Value("x")}));
  DataChunk target(std::vector<ValueType>{ValueType::kInt64});
  std::vector<const ColumnVector*> sources = {&input.ColumnAt(0),
                                              &input.ColumnAt(1)};
  EXPECT_ANY_THROW(target.AppendRowFromColumns(sources, 0));
}

TEST(DataChunkTest, Append_FromSourceChunk_PreservesNullsAndZoneMaps) {
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

TEST(DataChunkTest, Append_TypeMismatchFromSourceChunk_ThrowsWithoutPartialAppend) {
  DataChunk source(std::vector<ValueType>{ValueType::kDouble});
  source.Append(Row({Value(1.5)}));
  DataChunk target(std::vector<ValueType>{ValueType::kInt64});
  EXPECT_THROW(target.Append(source, 0), std::invalid_argument);
  EXPECT_EQ(target.Size(), 0U);
  EXPECT_NO_THROW(target.Append(Row({Value(int64_t{2})})));
}

TEST(ValidityBitmapTest, BasicOperationsAndBitwiseOps) {
  ValidityBitmap bm(130, false);
  EXPECT_EQ(bm.Size(), 130);
  EXPECT_EQ(bm.CountValid(), 0);
  EXPECT_TRUE(bm.NoneValid());

  bm.SetBit(0);
  bm.SetBit(63);
  bm.SetBit(64);
  bm.SetBit(129);
  EXPECT_EQ(bm.CountValid(), 4);
  EXPECT_TRUE(bm.Get(0));
  EXPECT_TRUE(bm.Get(63));
  EXPECT_TRUE(bm.Get(64));
  EXPECT_TRUE(bm.Get(129));
  EXPECT_FALSE(bm.Get(1));

  bm.ClearBit(63);
  EXPECT_FALSE(bm.Get(63));
  EXPECT_EQ(bm.CountValid(), 3);

  ValidityBitmap bm2(130, false);
  bm2.SetBit(0);
  bm2.SetBit(1);

  ValidityBitmap and_res = bm.BitwiseAnd(bm2);
  EXPECT_EQ(and_res.CountValid(), 1);
  EXPECT_TRUE(and_res.Get(0));
  EXPECT_FALSE(and_res.Get(1));

  ValidityBitmap or_res = bm.BitwiseOr(bm2);
  EXPECT_EQ(or_res.CountValid(), 4);
  EXPECT_TRUE(or_res.Get(0));
  EXPECT_TRUE(or_res.Get(1));
  EXPECT_TRUE(or_res.Get(64));
  EXPECT_TRUE(or_res.Get(129));
}

TEST(SelectionVectorTest, FilterAndToSelectionVector) {
  ValidityBitmap bm(10, false);
  bm.SetBit(1);
  bm.SetBit(3);
  bm.SetBit(7);

  SelectionVector sel;
  bm.ToSelectionVector(&sel);
  ASSERT_EQ(sel.Size(), 3);
  EXPECT_EQ(sel[0], 1);
  EXPECT_EQ(sel[1], 3);
  EXPECT_EQ(sel[2], 7);

  SelectionVector initial;
  initial.Initialize(10);
  EXPECT_EQ(initial.Size(), 10);

  SelectionVector filtered;
  initial.Filter(bm, &filtered);
  ASSERT_EQ(filtered.Size(), 3);
  EXPECT_EQ(filtered[0], 1);
  EXPECT_EQ(filtered[1], 3);
  EXPECT_EQ(filtered[2], 7);

  SelectionVector sliced = initial.Slice(2, 4);
  ASSERT_EQ(sliced.Size(), 4);
  EXPECT_EQ(sliced[0], 2);
  EXPECT_EQ(sliced[1], 3);
  EXPECT_EQ(sliced[2], 4);
  EXPECT_EQ(sliced[3], 5);
}

TEST(VectorizedExpressionTest, EvaluateArithmeticAndComparison) {
  const Schema schema("t", {Column("a", ValueType::kInt64),
                            Column("b", ValueType::kInt64),
                            Column("score", ValueType::kDouble)});
  DataChunk chunk(schema, 4);
  chunk.Append(Row({Value(int64_t{10}), Value(int64_t{2}), Value(1.5)}));
  chunk.Append(Row({Value(int64_t{20}), Value(int64_t{5}), Value(2.5)}));
  chunk.Append(Row({Value(int64_t{30}), Value(), Value(3.5)}));

  // a + b
  Expression add_expr = BinaryExpressionExp(
      ColumnValueExp("a"), BinaryOperation::kAdd, ColumnValueExp("b"));
  ColumnVector add_res =
      VectorizedExpression::Evaluate(add_expr, schema, chunk);
  ASSERT_EQ(add_res.Size(), 3);
  EXPECT_EQ(add_res.ValueAt(0), Value(int64_t{12}));
  EXPECT_EQ(add_res.ValueAt(1), Value(int64_t{25}));
  EXPECT_TRUE(add_res.IsNull(2));

  // a > 15
  Expression cmp_expr = BinaryExpressionExp(
      ColumnValueExp("a"), BinaryOperation::kGreaterThan,
      ConstantValueExp(Value(int64_t{15})));
  ValidityBitmap filter_mask =
      VectorizedExpression::EvaluateFilter(cmp_expr, schema, chunk);
  ASSERT_EQ(filter_mask.Size(), 3);
  EXPECT_FALSE(filter_mask.Get(0));
  EXPECT_TRUE(filter_mask.Get(1));
  EXPECT_TRUE(filter_mask.Get(2));

  SelectionVector sel;
  VectorizedExpression::FilterDataChunk(cmp_expr, schema, chunk, &sel);
  ASSERT_EQ(sel.Size(), 2);
  EXPECT_EQ(sel[0], 1);
  EXPECT_EQ(sel[1], 2);
}

TEST(SimdComparisonKernelTest, IntegerDoubleStringComparison) {
  std::vector<int64_t> ints = {10, 20, 30, 40, 50};
  ValidityBitmap int_mask;
  SimdComparisonKernel::CompareInt64(ints.data(), ints.size(),
                                     BinaryOperation::kGreaterThan, 25,
                                     &int_mask);
  EXPECT_EQ(int_mask.CountValid(), 3);
  EXPECT_FALSE(int_mask.Get(0));
  EXPECT_FALSE(int_mask.Get(1));
  EXPECT_TRUE(int_mask.Get(2));
  EXPECT_TRUE(int_mask.Get(3));
  EXPECT_TRUE(int_mask.Get(4));

  std::vector<double> doubles = {1.5, 2.5, 3.5, 4.5};
  ValidityBitmap dbl_mask;
  SimdComparisonKernel::CompareDouble(doubles.data(), doubles.size(),
                                      BinaryOperation::kLessThanEquals, 2.5,
                                      &dbl_mask);
  EXPECT_EQ(dbl_mask.CountValid(), 2);
  EXPECT_TRUE(dbl_mask.Get(0));
  EXPECT_TRUE(dbl_mask.Get(1));
  EXPECT_FALSE(dbl_mask.Get(2));

  std::vector<std::string_view> strings = {"apple", "banana", "cherry", "date"};
  ValidityBitmap str_mask;
  SimdComparisonKernel::CompareStringPrefix(strings.data(), strings.size(),
                                           BinaryOperation::kEquals, "banana",
                                           &str_mask);
  EXPECT_EQ(str_mask.CountValid(), 1);
  EXPECT_TRUE(str_mask.Get(1));
}

TEST(DictionaryBatchAggregationTest, GroupByCountSumAvg) {
  const Schema schema("sales", {Column("category", ValueType::kVarChar),
                                Column("amount", ValueType::kInt64)});
  DataChunk chunk(schema, 6);
  chunk.Append(Row({Value("Fruit"), Value(int64_t{100})}));
  chunk.Append(Row({Value("Veg"), Value(int64_t{50})}));
  chunk.Append(Row({Value("Fruit"), Value(int64_t{200})}));
  chunk.Append(Row({Value("Veg"), Value(int64_t{150})}));
  chunk.Append(Row({Value("Fruit"), Value(int64_t{300})}));

  DictionaryBatchAggregation sum_agg(schema, 0, 1,
                                     DictionaryBatchAggregation::AggOp::kSum);
  sum_agg.AccumulateChunk(chunk);
  EXPECT_EQ(sum_agg.GroupCount(), 2);

  const Schema out_schema("res", {Column("category", ValueType::kVarChar),
                                  Column("total", ValueType::kDouble)});
  DataChunk out_chunk = sum_agg.EmitResult(out_schema);
  ASSERT_EQ(out_chunk.Size(), 2);

  // Group 0 ("Fruit"): sum = 600
  EXPECT_EQ(out_chunk.ColumnAt(0).ValueAt(0), Value("Fruit"));
  EXPECT_EQ(out_chunk.ColumnAt(1).ValueAt(0), Value(600.0));

  // Group 1 ("Veg"): sum = 200
  EXPECT_EQ(out_chunk.ColumnAt(0).ValueAt(1), Value("Veg"));
  EXPECT_EQ(out_chunk.ColumnAt(1).ValueAt(1), Value(200.0));
}

TEST(VectorizedBooleanAndBitwiseAggregationTest, BitwiseAggregates) {
  const Schema schema("bits", {Column("v", ValueType::kInt64)});
  DataChunk chunk(schema, 8);
  chunk.Append(Row({Value(int64_t{0b1111})}));
  chunk.Append(Row({Value(int64_t{0b0110})}));
  chunk.Append(Row({Value(int64_t{0b0011})}));
  chunk.Append(Row({Value()}));  // Null row should be ignored in aggregates

  // BIT_AND: 0b1111 & 0b0110 & 0b0011 = 0b0010 (2)
  EXPECT_EQ(chunk.AggregateBitAnd(0), Value(int64_t{0b0010}));
  EXPECT_EQ(VectorizedExpression::AggregateBitAnd(chunk.ColumnAt(0)),
            Value(int64_t{0b0010}));
  EXPECT_EQ(VectorizedExpression::Aggregate(AggregationType::kBitAnd,
                                            chunk.ColumnAt(0)),
            Value(int64_t{0b0010}));

  // BIT_OR: 0b1111 | 0b0110 | 0b0011 = 0b1111 (15)
  EXPECT_EQ(chunk.AggregateBitOr(0), Value(int64_t{0b1111}));
  EXPECT_EQ(VectorizedExpression::AggregateBitOr(chunk.ColumnAt(0)),
            Value(int64_t{0b1111}));
  EXPECT_EQ(VectorizedExpression::Aggregate(AggregationType::kBitOr,
                                            chunk.ColumnAt(0)),
            Value(int64_t{0b1111}));

  // BIT_XOR: 0b1111 ^ 0b0110 ^ 0b0011 = 0b1010 (10)
  EXPECT_EQ(chunk.AggregateBitXor(0), Value(int64_t{0b1010}));
  EXPECT_EQ(VectorizedExpression::AggregateBitXor(chunk.ColumnAt(0)),
            Value(int64_t{0b1010}));
  EXPECT_EQ(VectorizedExpression::Aggregate(AggregationType::kBitXor,
                                            chunk.ColumnAt(0)),
            Value(int64_t{0b1010}));

  // With SelectionVector selecting row 0 and row 1:
  SelectionVector sel({0, 1});
  EXPECT_EQ(chunk.AggregateBitAnd(0, &sel), Value(int64_t{0b0110}));
  EXPECT_EQ(chunk.AggregateBitOr(0, &sel), Value(int64_t{0b1111}));
  EXPECT_EQ(chunk.AggregateBitXor(0, &sel), Value(int64_t{0b1001}));
}

TEST(VectorizedBooleanAndBitwiseAggregationTest, LogicalAndOrAggregates) {
  const Schema schema("bools", {Column("a", ValueType::kInt64),
                                Column("b", ValueType::kInt64),
                                Column("c", ValueType::kInt64)});
  DataChunk chunk(schema, 4);
  chunk.Append(Row({Value(int64_t{1}), Value(int64_t{0}), Value()}));
  chunk.Append(Row({Value(int64_t{1}), Value(int64_t{0}), Value()}));
  chunk.Append(Row({Value(int64_t{1}), Value(int64_t{1}), Value()}));

  // Column "a" has all true: LOGICAL_AND = 1, LOGICAL_OR = 1
  EXPECT_EQ(chunk.AggregateLogicalAnd(0), Value(int64_t{1}));
  EXPECT_EQ(chunk.AggregateLogicalOr(0), Value(int64_t{1}));

  // Column "b" has mix of 0 and 1: LOGICAL_AND = 0, LOGICAL_OR = 1
  EXPECT_EQ(chunk.AggregateLogicalAnd(1), Value(int64_t{0}));
  EXPECT_EQ(chunk.AggregateLogicalOr(1), Value(int64_t{1}));

  // Column "c" has all NULL: LOGICAL_AND = NULL, LOGICAL_OR = NULL
  EXPECT_TRUE(chunk.AggregateLogicalAnd(2).IsNull());
  EXPECT_TRUE(chunk.AggregateLogicalOr(2).IsNull());

  // VectorizedExpression binary logical evaluation: a AND b
  Expression and_expr = BinaryExpressionExp(
      ColumnValueExp("a"), BinaryOperation::kAnd, ColumnValueExp("b"));
  ColumnVector and_res =
      VectorizedExpression::Evaluate(and_expr, schema, chunk);
  ASSERT_EQ(and_res.Size(), 3);
  EXPECT_EQ(and_res.ValueAt(0), Value(int64_t{0}));
  EXPECT_EQ(and_res.ValueAt(1), Value(int64_t{0}));
  EXPECT_EQ(and_res.ValueAt(2), Value(int64_t{1}));

  // a OR b
  Expression or_expr = BinaryExpressionExp(
      ColumnValueExp("a"), BinaryOperation::kOr, ColumnValueExp("b"));
  ColumnVector or_res =
      VectorizedExpression::Evaluate(or_expr, schema, chunk);
  ASSERT_EQ(or_res.Size(), 3);
  EXPECT_EQ(or_res.ValueAt(0), Value(int64_t{1}));
  EXPECT_EQ(or_res.ValueAt(1), Value(int64_t{1}));
  EXPECT_EQ(or_res.ValueAt(2), Value(int64_t{1}));

  // a XOR b
  Expression xor_expr = BinaryExpressionExp(
      ColumnValueExp("a"), BinaryOperation::kXor, ColumnValueExp("b"));
  ColumnVector xor_res =
      VectorizedExpression::Evaluate(xor_expr, schema, chunk);
  ASSERT_EQ(xor_res.Size(), 3);
  EXPECT_EQ(xor_res.ValueAt(0), Value(int64_t{1}));
  EXPECT_EQ(xor_res.ValueAt(1), Value(int64_t{1}));
  EXPECT_EQ(xor_res.ValueAt(2), Value(int64_t{0}));
}

}  // namespace tinylamb
