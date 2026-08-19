/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "page/pax_block.hpp"

#include "gtest/gtest.h"
#include "type/date.hpp"

namespace tinylamb {

TEST(PaxBlockTest, DictionaryAndBitPackingRoundTripWithNulls) {
  const Schema schema("pax", {Column("id", ValueType::kInt64),
                                Column("status", ValueType::kVarChar),
                                Column("shipdate", ValueType::kDate)});
  DataChunk chunk(schema, 256);
  for (int64_t row = 0; row < 256; ++row) {
    chunk.Append(Row({Value(1000 + row % 16),
                      row == 17 ? Value() : Value(row % 2 ? "OPEN" : "DONE"),
                      Value::DateFromDays(ParseDateDays("1995-01-01") +
                                          row % 32)}));
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

}  // namespace tinylamb
