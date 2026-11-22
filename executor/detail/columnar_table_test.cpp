/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

#include "executor/detail/columnar_table.hpp"

#include <array>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "gtest/gtest.h"
#include "table/lstable_bridge.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

using relational_detail::ChunkGatherIndex;
using relational_detail::ColumnarChunk;
using relational_detail::ColumnarColumn;
using relational_detail::ColumnarTable;
using relational_detail::CompiledColumnarProjection;

TEST(ColumnarTableTest, DictionaryNullsStayNullThroughScalarAccessors) {
  const std::array<uint8_t, 2> codes{0, LSTableColumnDictionary::kNullCode};
  LSTableColumnDictionary dictionary;
  dictionary.double_entries = {1.25};
  ColumnarColumn column;
  column.SetBorrowedNarrowDict(&dictionary, nullptr, ValueType::kDouble, false,
                               false, true, codes.data(), nullptr,
                               codes.size());
  EXPECT_FALSE(column.IsNull(0));
  EXPECT_EQ(column.DoubleAt(0), 1.25);
  EXPECT_EQ(column.ValueAt(0), Value(1.25));
  EXPECT_TRUE(column.IsNull(1));
  EXPECT_EQ(column.DoubleAt(1), 0.0);
  EXPECT_TRUE(column.ValueAt(1).IsNull());
}

TEST(ColumnarTableTest, FallbackSubstringOwnsItsBytes) {
  ColumnarColumn source;
  source.Init(ValueType::kVarChar);
  source.AppendValue(Value("prefix" + std::string(120, 'x')).WithCollation(2),
                     nullptr, 0);
  source.AppendNull(1);
  ASSERT_FALSE(source.is_uniform);

  ColumnarColumn output;
  output.Init(ValueType::kVarChar);
  ASSERT_TRUE(
      output.AppendSubstringFrom(source, 0, nullptr, 2, 7, 100, true, 0));
  EXPECT_EQ(output.StringAt(0), std::string(100, 'x'));
  EXPECT_NE(output.StringAt(0).data(),
            source.fallback[0].AsStringView().data() + 6);
  EXPECT_TRUE(output.ValueAt(1).IsNull());
}

TEST(ColumnarTableTest, PartialSliceRetainsColumnLocalArenaAfterSourceDrop) {
  auto input = std::make_shared<ColumnarTable>();
  {
    ColumnarChunk chunk;
    chunk.columns.resize(1);
    chunk.columns[0].Init(ValueType::kVarChar);
    const std::string text(128, 'z');
    // Trigger kConstant -> MaterializeToFlat(nullptr) via AppendNull so the
    // string slab lives solely on the column's local arena, not
    // chunk.keepalives.
    chunk.columns[0].AppendConstant(Value(std::string(text)), 4, nullptr, 0);
    chunk.columns[0].AppendNull(4);
    chunk.rows = 5;
    input->AppendChunk(std::move(chunk));
  }

  std::shared_ptr<ColumnarTable> sliced =
      relational_detail::SliceColumnarTable(*input, 2, 3);
  input.reset();

  ASSERT_EQ(sliced->total_rows, 3U);
  Schema out_schema("s", {Column("c", ValueType::kVarChar)});
  std::vector<Row> rows;
  ASSERT_EQ(relational_detail::MaterializeColumnarTableToRows(
                *sliced, out_schema, &rows, nullptr),
            Status::kSuccess);
  ASSERT_EQ(rows.size(), 3U);
  const std::string expected(128, 'z');
  EXPECT_EQ(rows[0][0], Value(std::string(expected)));
  EXPECT_EQ(rows[1][0], Value(std::string(expected)));
  EXPECT_TRUE(rows[2][0].IsNull());
}

TEST(ColumnarTableTest, MultiOwnerAppendSliceFromRetainsAllSourceArenas) {
  ColumnarColumn dst;
  dst.Init(ValueType::kVarChar);
  {
    ColumnarColumn src1;
    src1.Init(ValueType::kVarChar);
    src1.AppendConstant(Value(std::string(96, 'a')), 2, nullptr, 0);
    src1.AppendNull(2);

    ColumnarColumn src2;
    src2.Init(ValueType::kVarChar);
    src2.AppendConstant(Value(std::string(96, 'b')), 2, nullptr, 0);
    src2.AppendNull(2);

    dst.AppendSliceFrom(src1, nullptr, 3, nullptr, 0);
    dst.AppendSliceFrom(src2, nullptr, 3, nullptr, 3);
  }
  EXPECT_EQ(dst.ValueAt(0), Value(std::string(96, 'a')));
  EXPECT_EQ(dst.ValueAt(1), Value(std::string(96, 'a')));
  EXPECT_TRUE(dst.ValueAt(2).IsNull());
  EXPECT_EQ(dst.ValueAt(3), Value(std::string(96, 'b')));
  EXPECT_EQ(dst.ValueAt(4), Value(std::string(96, 'b')));
  EXPECT_TRUE(dst.ValueAt(5).IsNull());
}

TEST(ColumnarTableTest,
     PassThroughProjectionRetainsColumnLocalArenaAfterInputDrop) {
  Schema schema("t", {Column(ColumnName("t", "c"), ValueType::kVarChar)});
  CompiledColumnarProjection plan = CompiledColumnarProjection::Compile(
      {ColumnValueExp(ColumnName("t", "c"))}, schema, schema);
  ASSERT_TRUE(plan.all_fast);

  auto input = std::make_shared<ColumnarTable>();
  {
    ColumnarChunk chunk;
    chunk.columns.resize(1);
    chunk.columns[0].Init(ValueType::kVarChar);
    chunk.columns[0].AppendConstant(Value(std::string(110, 'p')), 3, nullptr,
                                    0);
    chunk.columns[0].AppendNull(3);
    chunk.rows = 4;
    input->AppendChunk(std::move(chunk));
  }

  auto projected_or = relational_detail::TryProjectColumnarTable(plan, *input);
  ASSERT_TRUE(projected_or.HasValue()) << projected_or.GetStatus();
  ASSERT_TRUE(projected_or.Value().has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access) harness assumes success
  std::shared_ptr<ColumnarTable> projected = *projected_or.MoveValue();
  input.reset();

  ASSERT_EQ(projected->total_rows, 4U);
  std::vector<Row> rows;
  ASSERT_EQ(relational_detail::MaterializeColumnarTableToRows(
                *projected, schema, &rows, nullptr),
            Status::kSuccess);
  ASSERT_EQ(rows.size(), 4U);
  EXPECT_EQ(rows[0][0], Value(std::string(110, 'p')));
  EXPECT_EQ(rows[2][0], Value(std::string(110, 'p')));
  EXPECT_TRUE(rows[3][0].IsNull());
}

TEST(ColumnarTableTest, NullTypedSchemaColumnPreservesVarcharValues) {
  Schema null_schema("t", {Column("c", ValueType::kNull)});
  std::vector<Row> rows{Row({Value()}), Row({Value("hello_varchar")}),
                        Row({Value()}), Row({Value("world")})};
  auto tbl_or =
      relational_detail::BuildColumnarTableFromRows(null_schema, rows);
  ASSERT_TRUE(tbl_or.HasValue()) << tbl_or.GetStatus();
  std::shared_ptr<ColumnarTable> tbl = tbl_or.MoveValue();
  ASSERT_EQ(tbl->total_rows, 4U);
  std::vector<Row> out;
  Schema out_schema("t", {Column("c", ValueType::kVarChar)});
  ASSERT_EQ(relational_detail::MaterializeColumnarTableToRows(*tbl, out_schema,
                                                              &out, nullptr),
            Status::kSuccess);
  EXPECT_EQ(out, rows);

  // Direct AppendValue without an external arena must not drop strings either.
  ColumnarColumn direct;
  direct.Init(ValueType::kNull);
  direct.AppendValue(Value(), nullptr, 0);
  direct.AppendValue(Value("direct_non_empty"), nullptr, 1);
  EXPECT_TRUE(direct.ValueAt(0).IsNull());
  EXPECT_EQ(direct.ValueAt(1), Value("direct_non_empty"));
}

TEST(ColumnarTableTest, GatheredSliceFallsBackWhenChunksShorterThanRows) {
  auto base = std::make_shared<ColumnarTable>();
  Schema schema("t", {Column("id", ValueType::kInt64)});
  {
    ColumnarChunk ch;
    ch.InitFromSchema(schema);
    ch.AppendRow(Row({Value(int64_t{10})}));
    ch.AppendRow(Row({Value(int64_t{20})}));
    base->AppendChunk(std::move(ch));
  }
  auto malformed_gi = std::make_shared<ChunkGatherIndex>();
  malformed_gi->source_table = base;
  malformed_gi->src_rows = {0, 1};
  // A partial slice containing the second row cannot preserve the gathered
  // encoding because the matching source chunk index is missing.
  malformed_gi->src_chunks = {0};

  ColumnarTable tbl;
  ColumnarChunk ch;
  ch.rows = 2;
  ch.columns.resize(1);
  ch.columns[0].SetGathered(malformed_gi, 0, ValueType::kInt64, false, false,
                            true, false);
  tbl.AppendChunk(std::move(ch));

  std::shared_ptr<ColumnarTable> sliced =
      relational_detail::SliceColumnarTable(tbl, 1, 1);
  ASSERT_EQ(sliced->total_rows, 1U);
  ASSERT_EQ(sliced->chunks.size(), 1U);
  EXPECT_NE(sliced->chunks[0].columns[0].encoding,
            ColumnarColumn::Encoding::kGathered);
  std::vector<Row> out;
  ASSERT_EQ(relational_detail::MaterializeColumnarTableToRows(*sliced, schema,
                                                              &out, nullptr),
            Status::kSuccess);
  ASSERT_EQ(out.size(), 1U);
  EXPECT_TRUE(out[0][0].IsNull());
}

TEST(ColumnarTableTest, EstimatedBytesDeduplicatesMoreThanFourGatherIndices) {
  auto base = std::make_shared<ColumnarTable>();
  Schema schema("t", {Column("id", ValueType::kInt64)});
  {
    ColumnarChunk ch;
    ch.InitFromSchema(schema);
    for (int64_t i = 0; i < 64; ++i) {
      ch.AppendRow(Row({Value(i)}));
    }
    base->AppendChunk(std::move(ch));
  }
  auto make_gi = [&]() {
    auto gi = std::make_shared<ChunkGatherIndex>();
    gi->source_table = base;
    gi->src_chunks.assign(64, 0);
    gi->src_rows.assign(64, 0);
    return gi;
  };
  ColumnarChunk single_gi_chunk;
  single_gi_chunk.rows = 64;
  single_gi_chunk.columns.resize(12);
  const auto shared_gi = make_gi();
  for (size_t c = 0; c < 12; ++c) {
    single_gi_chunk.columns[c].SetGathered(shared_gi, 0, ValueType::kInt64,
                                           false, false, true, false);
  }
  const size_t gi_bytes =
      (shared_gi->src_chunks.size() + shared_gi->src_rows.size()) *
      sizeof(uint32_t);

  // Two distinct chunks sharing 12 columns over >4 distinct gather indices:
  // duplicate pointers beyond index 4 must still be deduplicated.
  ColumnarChunk twelve_distinct_gi_chunk;
  twelve_distinct_gi_chunk.rows = 64;
  twelve_distinct_gi_chunk.columns.resize(12);
  std::vector<std::shared_ptr<ChunkGatherIndex>> gis;
  gis.reserve(6);
  for (size_t c = 0; c < 6; ++c) {
    gis.push_back(make_gi());
  }
  for (size_t c = 0; c < 12; ++c) {
    twelve_distinct_gi_chunk.columns[c].SetGathered(
        gis[c % 6], 0, ValueType::kInt64, false, false, true, false);
  }
  EXPECT_EQ(twelve_distinct_gi_chunk.EstimatedBytes() -
                single_gi_chunk.EstimatedBytes(),
            5 * gi_bytes);
}

}  // namespace
}  // namespace tinylamb
