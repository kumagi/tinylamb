/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "page/pax_block.hpp"
#include <cstdint>
#include <cstddef>
#include <string>

#include "executor/data_chunk.hpp"
#include "gtest/gtest.h"
#include "page/pax_layout.hpp"
#include "type/date.hpp"
#include "type/schema.hpp"
#include "type/value_type.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

TEST(PaxBlockTest, DictionaryAndBitPackingRoundTripWithNulls) {
  const Schema schema("pax", {Column("id", ValueType::kInt64),
                                Column("status", ValueType::kVarChar),
                                Column("shipdate", ValueType::kDate)});
  DataChunk chunk(schema, 256);
  for (int64_t row = 0; row < 256; ++row) {
    chunk.Append(Row({Value(1000 + (row % 16)),
                      row == 17 ? Value()
                                : Value(row % 2 != 0 ? "OPEN" : "DONE"),
                      Value::DateFromDays(ParseDateDays("1995-01-01") +
                                          (row % 32))}));
  }

  const PaxBlock block = PaxBlock::Encode(chunk);
  EXPECT_EQ(block.ColumnAt(0).Encoding(), PaxEncoding::kBitPacked);
  EXPECT_EQ(block.ColumnAt(1).Encoding(), PaxEncoding::kDictionary);
  EXPECT_EQ(block.ColumnAt(2).Encoding(), PaxEncoding::kBitPacked);
  EXPECT_LT(block.CompressedBytes(), 256U * (8U + 8U + 8U));
  for (size_t row = 0; row < chunk.Size(); ++row) {
    EXPECT_EQ(block.RowAt(row), chunk.RowAt(row));
  }
}

TEST(PaxBlockTest, DateUsesIntegerDaysAndCalendarArithmetic) {
  const Value leap = Value::Date("2024-02-29");
  EXPECT_EQ(leap.AsString(), "2024-02-29");
  EXPECT_EQ(FormatDateDays(AddDateIntervalDays(leap.DateDays(), 1, "year")),
            "2025-02-28");
  EXPECT_EQ(FormatDateDays(AddDateIntervalDays(leap.DateDays(), 1, "day")),
            "2024-03-01");
  EXPECT_EQ(leap.Size(), sizeof(int64_t));
}

TEST(PaxBlockTest, PlainFallbackForHighCardinalityVarchar) {
  const Schema schema("pax", {Column("name", ValueType::kVarChar)});
  DataChunk chunk(schema, 64);
  for (int64_t row = 0; row < 64; ++row) {
    chunk.Append(Row({Value("unique_value_" + std::to_string(row) + "_" +
                           std::string(32, 'x'))}));
  }

  const PaxBlock block = PaxBlock::Encode(chunk);
  EXPECT_EQ(block.ColumnAt(0).Encoding(), PaxEncoding::kPlain);
  for (size_t row = 0; row < chunk.Size(); ++row) {
    EXPECT_EQ(block.RowAt(row), chunk.RowAt(row));
  }
}

TEST(PaxBlockTest, PlainFallbackWithNullsPreserved) {
  const Schema schema("pax", {Column("name", ValueType::kVarChar)});
  DataChunk chunk(schema, 32);
  for (int64_t row = 0; row < 32; ++row) {
    chunk.Append(Row({row % 3 == 0 ? Value()
                                   : Value("unique_" + std::to_string(row))}));
  }

  const PaxBlock block = PaxBlock::Encode(chunk);
  EXPECT_EQ(block.ColumnAt(0).Encoding(), PaxEncoding::kPlain);
  for (size_t row = 0; row < chunk.Size(); ++row) {
    EXPECT_EQ(block.RowAt(row), chunk.RowAt(row));
  }
}

}  // namespace tinylamb
