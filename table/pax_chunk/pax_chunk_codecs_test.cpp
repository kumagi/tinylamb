/**
 * Copyright 2026 KUMAZAKI Hiroki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

#include "table/fuzz/fuzz_chunk_common.hpp"
#include "table/pax_chunk/pax_chunk_internal.hpp"

namespace tinylamb {
namespace {

using Scheme = PaxChunk::Scheme;

PaxChunk AuditSingleColumnChunk(Scheme scheme, uint16_t rows,
                                std::vector<char> data, uint32_t param = 0) {
  PaxChunk::Footer footer;
  footer.row_group_count = 1;
  footer.column_count = 1;
  footer.row_counts = {rows};
  footer.column_offsets = {0};
  footer.encodings = {static_cast<uint8_t>(scheme)};
  footer.params = {param};
  data.resize(data.size() + 4);  // trailing footer-size word
  return {std::move(data), std::move(footer)};
}

TEST(PaxChunkCodecsTest, AuditBuilderRejectsCorruptRowsAndUnsortedKeys) {
  for (bool text_payload : {false, true}) {
    const Schema schema(
        "audit",
        {Column("k", ValueType::kInt64),
         Column("v", text_payload ? ValueType::kVarChar : ValueType::kInt64)});
    const std::vector<Row> rows{
        Row({Value(1), text_payload ? Value("one") : Value(10)}),
        Row({Value(2), text_payload ? Value("two") : Value(20)})};
    const auto run = fuzz::MakeRun(rows, schema);
    const std::vector<SortedRun::Entry> original{run.GetEntry(0),
                                                 run.GetEntry(1)};
    for (bool compressed : {false, true}) {
      ASSERT_TRUE(
          PaxChunk::BuildFromSortedRun(run, schema, compressed).HasValue());
      for (int corruption = 0; corruption < 5; ++corruption) {
        auto entries = original;
        auto blob = run.Blob();
        if (corruption == 0) {
          entries[1].row_offset_ = blob.size();
        }
        if (corruption == 1) {
          blob.resize(entries[1].row_offset_ + 1);
        }
        if (corruption == 2) {
          std::swap(entries[0], entries[1]);
        }
        if (corruption == 3) {
          entries[1].key_offset_ = UINT32_MAX;
        }
        if (corruption == 4) {
          std::fill_n(blob.data() + entries[1].key_offset_, 4, char(-1));
        }
        const SortedRun bad(std::move(blob), std::move(entries), 0);
        const auto built =
            PaxChunk::BuildFromSortedRun(bad, schema, compressed);
        EXPECT_EQ(built.GetStatus(), Status::kCorrupt) << corruption;
      }
    }
  }
}

TEST(PaxChunkCodecsTest, AuditNonIntegerZoneMapsCannotPruneIntegerRanges) {
  Schema schema("zone_audit", {Column("id", ValueType::kInt64),
                               Column("text", ValueType::kVarChar),
                               Column("real", ValueType::kDouble)});
  const std::vector<Row> rows{Row({Value(10), Value("42"), Value(42.5)}),
                              Row({Value(20), Value("99"), Value(99.5)})};
  for (bool compressed : {false, true}) {
    const auto chunk = PaxChunk::BuildFromSortedRun(fuzz::MakeRun(rows, schema),
                                                    schema, compressed)
                           .MoveValue();
    EXPECT_FALSE(chunk.RGOverlapsCol(0, 0, 40, 100));
    for (size_t column : {1U, 2U}) {
      EXPECT_TRUE(chunk.RGOverlapsCol(0, column, 40, 100));
      EXPECT_TRUE(chunk.RGOverlapsCol(0, column, -100, -40));
    }
  }
}

TEST(PaxChunkCodecsTest, AuditProjectionSupportsWideSchemas) {
  PaxChunk::VarcharGroupCache cache;
  const std::array<slot_t, 4> selected{0, 1023, 1024, 65534};
  cache.SelectColumns(selected);
  for (slot_t col : selected) {
    EXPECT_TRUE(cache.IsColumnSelected(col));
  }
  EXPECT_FALSE(cache.IsColumnSelected(1));
  EXPECT_FALSE(cache.IsColumnSelected(65533));
  cache.SelectColumns(std::array<slot_t, 1>{3});
  EXPECT_TRUE(cache.IsColumnSelected(3));
  EXPECT_FALSE(cache.IsColumnSelected(1024));
}

TEST(PaxChunkCodecsTest, AuditReduceRejectsScaledOverflowAcrossSchemes) {
  const std::vector<int64_t> values(1024, INT64_MAX);
  for (Scheme scheme :
       {Scheme::kPlain, Scheme::kConst, Scheme::kRle, Scheme::kBp128}) {
    auto encoded = codec_benchmark::EncodeInt64Block(scheme, values);
    ASSERT_TRUE(encoded.has_value());
    if (!encoded.has_value()) {
      continue;
    }
    auto chunk = AuditSingleColumnChunk(scheme, values.size(), encoded->bytes,
                                        encoded->param);
    ReducePlan plan;
    plan.op = AggOp::kSum;
    plan.col = 0;
    plan.coeff = INT64_MAX;
    EXPECT_FALSE(ReduceColumn(chunk, plan).valid);
    plan.coeff = 1;
    const auto result = ReduceColumn(chunk, plan);
    EXPECT_TRUE(result.valid);
    EXPECT_TRUE(result.sum == static_cast<__int128>(INT64_MAX) * values.size());
  }
}

TEST(PaxChunkCodecsTest, AuditPartialZoneMapsCannotProveWholeColumnBounds) {
  PaxChunk::Footer footer;
  footer.row_group_count = 2;
  footer.column_count = 1;
  footer.row_counts = {1, 1};
  footer.all_zm_present = true;
  footer.all_zone_mins = {1};
  footer.all_zone_maxs = {2};
  PaxChunk chunk({}, std::move(footer));
  int64_t minimum = -77, maximum = -77;
  EXPECT_NE(chunk.ColumnMinMax(0, &minimum, &maximum), Status::kSuccess);
  EXPECT_EQ(minimum, -77);
  EXPECT_EQ(maximum, -77);
}

TEST(PaxChunkCodecsTest, AuditConstMinMaxIgnoresAllNullCells) {
  std::vector<char> bytes;
  AppendBE64(bytes, 42);
  auto chunk = AuditSingleColumnChunk(Scheme::kConst, 2, std::move(bytes));
  auto footer = chunk.GetFooter();
  footer.null_present = true;
  footer.null_bitmaps = {0xC0};
  PaxChunk all_null(chunk.Data(), std::move(footer));
  for (AggOp op : {AggOp::kMin, AggOp::kMax}) {
    ReducePlan plan;
    plan.op = op;
    plan.col = 0;
    EXPECT_FALSE(ReduceColumn(all_null, plan).valid);
  }
}

TEST(PaxChunkCodecsTest, AuditCodecBenchmarkRejectsOversizedBlock) {
  std::vector<int64_t> values(65536);
  for (size_t i = 0; i < values.size(); ++i) {
    values[i] = static_cast<int64_t>(i);
  }
  EXPECT_FALSE(codec_benchmark::EncodeInt64Block(Scheme::kDict, values));
  EXPECT_TRUE(codec_benchmark::EncodeInt64Block(Scheme::kPlain, values));
  EXPECT_TRUE(codec_benchmark::EncodeInt64Block(Scheme::kRle, values));
  const std::vector<uint64_t> bits(65536, 0);
  EXPECT_FALSE(codec_benchmark::EncodeDoubleBitsBlock(Scheme::kGorillaC, bits));
  EXPECT_TRUE(codec_benchmark::EncodeDoubleBitsBlock(Scheme::kPlain, bits));
  const std::vector<std::string> strings(65536, "a");
  EXPECT_FALSE(
      codec_benchmark::EncodeVarcharBlock(Scheme::kFrontCode, strings));
  EXPECT_TRUE(codec_benchmark::EncodeVarcharBlock(Scheme::kPlain, strings));
}

TEST(PaxChunkCodecsTest, AuditGroupSumReportsCorruptGroupAndWrapsExplicitly) {
  std::vector<char> data;
  AppendBE64(data, static_cast<uint64_t>(INT64_MAX));
  auto chunk = AuditSingleColumnChunk(Scheme::kConst, 2, std::move(data));
  auto result = GroupBySumColumns(chunk, {0}, 0);
  ASSERT_TRUE(result.HasValue());
  ASSERT_EQ(result.Value().size(), 1U);
  EXPECT_EQ(result.Value()[0].sum, -2);
  EXPECT_EQ(result.Value()[0].count, 2U);
  auto footer = chunk.GetFooter();
  footer.column_offsets[0] = UINT32_MAX;
  PaxChunk corrupt(chunk.Data(), std::move(footer));
  EXPECT_EQ(GroupBySumColumns(corrupt, {0}, 0).GetStatus(),
            Status::kCorruption);
  EXPECT_EQ(GroupBySumColumns(chunk, {1}, 0).GetStatus(),
            Status::kInvalidArgument);
}

TEST(PaxChunkCodecsTest, AuditDoubleAggregatesRejectOversizedFallback) {
  auto chunk = AuditSingleColumnChunk(
      Scheme::kConst, PaxChunk::kRowGroupSize + 1, std::vector<char>(8));
  uint64_t count = 99;
  double sum = 99;
  EXPECT_FALSE(Chimp128ColumnCountSum(chunk, 0, PaxChunk::CompareOp::kEq, 0,
                                      &count, &sum));
  EXPECT_FALSE(AlpRdTansColumnCountSum(chunk, 0, PaxChunk::CompareOp::kEq, 0,
                                       &count, &sum));
  EXPECT_FALSE(
      PatasColumnCountSum(chunk, 0, PaxChunk::CompareOp::kEq, 0, &count, &sum));
  EXPECT_EQ(count, 99U);
  EXPECT_EQ(sum, 99);
}

TEST(PaxChunkCodecsTest, AuditOpDictClampComparesDecodedEmptyString) {
  const std::vector<std::string_view> values{"abc", "def", "ghi", "abc"};
  auto encoded = EncOpDictStr(values);
  ASSERT_FALSE(encoded.buf.empty());
  // Three dictionary entries use two bits/code; 3 is a corrupt high code,
  // decoded as the empty string, which is absent from this dictionary.
  encoded.buf[2] = static_cast<char>(0x1B);
  auto chunk =
      AuditSingleColumnChunk(Scheme::kOpDict, 4, encoded.buf, encoded.param);
  for (StrScanOp op :
       {StrScanOp::kEq, StrScanOp::kNe, StrScanOp::kLt, StrScanOp::kLe,
        StrScanOp::kGt, StrScanOp::kGe, StrScanOp::kPrefix}) {
    for (const std::string& needle : {"", "abc", "a", "def", "zzz"}) {
      EXPECT_EQ(VarcharFilterCount(chunk, 0, op, needle),
                VarcharFilterSelection(chunk, 0, op, needle).count);
    }
  }
  EXPECT_EQ(VarcharFilterCount(chunk, 0, StrScanOp::kEq, ""), 1U);
  EXPECT_EQ(VarcharFilterCount(chunk, 0, StrScanOp::kEq, "abc"), 1U);
}

TEST(PaxChunkCodecsTest, AuditCodecRejectsTruncatedCheckpointCount) {
  const std::vector<int64_t> values(
      (static_cast<size_t>(UINT16_MAX) + 1) * kInterpBlockRows, 1);
  // INTERP stores checkpoint count in u16; this previously emitted 0 in the
  // header while returning an apparently successful encoded block.
  EXPECT_FALSE(codec_benchmark::EncodeInt64Block(Scheme::kInterp, values));
}

TEST(PaxChunkCodecsTest, AuditBp128FullWidthRoundTrip) {
  std::vector<int64_t> values(257);
  for (size_t i = 0; i < values.size(); ++i) {
    values[i] = i % 2 == 0 ? std::numeric_limits<int64_t>::min()
                           : std::numeric_limits<int64_t>::max();
  }
  const EncodedCol encoded = EncBp128I64(values);
  Bp128Region region{};
  ASSERT_TRUE(ParseBp128Region(encoded.buf.data(), values.size(), encoded.param,
                               encoded.buf.data() + encoded.buf.size(),
                               &region));
  std::vector<int64_t> decoded(values.size());
  DecodeBp128All(region, decoded.data());
  EXPECT_EQ(decoded, values);
}

TEST(PaxChunkCodecsTest, ScaledSumChecksProductAndAdditionBounds) {
  constexpr __int128 kMin = std::numeric_limits<__int128>::min();
  constexpr __int128 kMax = std::numeric_limits<__int128>::max();
  struct Case {
    int64_t coefficient;
    __int128 subtotal;
    __int128 initial;
    bool fits;
    __int128 expected;
  };
  const auto cases = std::to_array<Case>({
      {.coefficient = 0,
       .subtotal = kMin,
       .initial = kMax,
       .fits = true,
       .expected = kMax},
      {.coefficient = 1,
       .subtotal = kMin,
       .initial = 0,
       .fits = true,
       .expected = kMin},
      {.coefficient = 1,
       .subtotal = kMax,
       .initial = 0,
       .fits = true,
       .expected = kMax},
      {.coefficient = -1,
       .subtotal = kMin,
       .initial = 0,
       .fits = false,
       .expected = 0},
      {.coefficient = -1,
       .subtotal = kMax,
       .initial = 0,
       .fits = true,
       .expected = -kMax},
      {.coefficient = 2,
       .subtotal = kMax / 2,
       .initial = 0,
       .fits = true,
       .expected = kMax - 1},
      {.coefficient = 2,
       .subtotal = (kMax / 2) + 1,
       .initial = 0,
       .fits = false,
       .expected = 0},
      {.coefficient = 2,
       .subtotal = kMin / 2,
       .initial = 0,
       .fits = true,
       .expected = kMin},
      {.coefficient = 2,
       .subtotal = (kMin / 2) - 1,
       .initial = 0,
       .fits = false,
       .expected = 0},
      {.coefficient = -2,
       .subtotal = (kMax / 2) + 1,
       .initial = 0,
       .fits = true,
       .expected = kMin},
      {.coefficient = -2,
       .subtotal = (kMax / 2) + 2,
       .initial = 0,
       .fits = false,
       .expected = 0},
      {.coefficient = -2,
       .subtotal = (kMin / 2) + 1,
       .initial = 0,
       .fits = true,
       .expected = kMax - 1},
      {.coefficient = -2,
       .subtotal = kMin / 2,
       .initial = 0,
       .fits = false,
       .expected = 0},
      {.coefficient = 3,
       .subtotal = kMin / 3,
       .initial = 0,
       .fits = true,
       .expected = (kMin / 3) * 3},
      {.coefficient = 3,
       .subtotal = (kMin / 3) - 1,
       .initial = 0,
       .fits = false,
       .expected = 0},
      {.coefficient = -3,
       .subtotal = kMax / 3,
       .initial = 0,
       .fits = true,
       .expected = -(kMax / 3) * 3},
      {.coefficient = -3,
       .subtotal = (kMax / 3) + 1,
       .initial = 0,
       .fits = false,
       .expected = 0},
      {.coefficient = INT64_MIN,
       .subtotal = static_cast<__int128>(1) << 64,
       .initial = 0,
       .fits = true,
       .expected = kMin},
      {.coefficient = INT64_MIN,
       .subtotal = (static_cast<__int128>(1) << 64) + 1,
       .initial = 0,
       .fits = false,
       .expected = 0},
      {.coefficient = 1,
       .subtotal = 1,
       .initial = kMax,
       .fits = false,
       .expected = kMax},
      {.coefficient = 1,
       .subtotal = -1,
       .initial = kMin,
       .fits = false,
       .expected = kMin},
      {.coefficient = 1,
       .subtotal = kMin,
       .initial = kMax,
       .fits = true,
       .expected = -1},
      {.coefficient = 1,
       .subtotal = kMax,
       .initial = kMin,
       .fits = true,
       .expected = -1},
      {.coefficient = 2,
       .subtotal = kMax,
       .initial = kMin,
       .fits = false,
       .expected = kMin},
  });
  for (size_t i = 0; i < cases.size(); ++i) {
    SCOPED_TRACE(i);
    const Case& test = cases[i];
    __int128 total = test.initial;
    EXPECT_EQ(TryAccumulateScaledSum(test.coefficient, test.subtotal, &total),
              test.fits);
    EXPECT_EQ(total, test.expected);
  }
}

TEST(PaxChunkCodecsTest, BitWriterAndWordBitReaderRoundTrip) {
  BitWriter writer;
  for (int w = 1; w <= 64; ++w) {
    const uint64_t mask =
        (w == 64) ? ~uint64_t{0} : ((uint64_t{1} << w) - 1ULL);
    const uint64_t val =
        (0xA5A5F0F012345678ULL ^ static_cast<uint64_t>(w)) & mask;
    writer.put(val, w);
  }
  writer.finish();

  WordBitReader reader(writer.buf.data(),
                       writer.buf.data() + writer.buf.size());
  for (int w = 1; w <= 64; ++w) {
    const uint64_t mask =
        (w == 64) ? ~uint64_t{0} : ((uint64_t{1} << w) - 1ULL);
    const uint64_t expected =
        (0xA5A5F0F012345678ULL ^ static_cast<uint64_t>(w)) & mask;
    EXPECT_EQ(reader.get(w), expected) << "width=" << w;
  }
}

TEST(PaxChunkCodecsTest, ZigZagEncodingRoundTrip) {
  const std::vector<int64_t> inputs = {
      0, 1, -1, 2, -2, 42, -42, 1000000000LL, -1000000000LL};
  for (int64_t v : inputs) {
    const uint64_t zz = StreamVByteZigZag(v);
    EXPECT_EQ(StreamVByteUnZigZag(zz), v);
    const uint64_t zz64 = EncodeZigZag64(static_cast<uint64_t>(v));
    EXPECT_EQ(static_cast<int64_t>(DecodeZigZag64(zz64)), v);
  }
}

TEST(PaxChunkCodecsTest, ConstAndRleAndRleBpAndRleIdxRoundTrip) {
  // 1. kConst
  const EncodedCol c = EncConstI64(777);
  EXPECT_EQ(c.scheme, Scheme::kConst);
  EXPECT_EQ(Int64RegionAt(c.buf.data(), c.scheme, c.param, 100, 55,
                          c.buf.data() + c.buf.size()),
            777);

  // 2. Sorted runs for kRle / kRleBp / kRleIdx
  std::vector<int64_t> runs;
  for (int r = 0; r < 16; ++r) {
    for (int k = 0; k < 20; ++k) {
      runs.push_back(100 + (r * 10));
    }
  }
  const auto n = static_cast<uint32_t>(runs.size());

  const EncodedCol rle = EncRleI64(runs);
  ASSERT_FALSE(rle.buf.empty());
  for (uint32_t i = 0; i < n; i += 17) {
    EXPECT_EQ(Int64RegionAt(rle.buf.data(), rle.scheme, rle.param, n, i,
                            rle.buf.data() + rle.buf.size()),
              runs[i]);
  }

  const EncodedCol rle_bp = EncRleBpI64(runs);
  ASSERT_FALSE(rle_bp.buf.empty());
  RleBpRegion parsed_bp{};
  ASSERT_TRUE(ParseRleBpRegion(rle_bp.buf.data(), n, rle_bp.param,
                               rle_bp.buf.data() + rle_bp.buf.size(),
                               &parsed_bp));
  EXPECT_EQ(RleBpValueAt(parsed_bp, 0), 100);
  EXPECT_EQ(RleBpLengthAt(parsed_bp, 0), 20U);

  const EncodedCol rle_idx = EncRleIdxI64(runs);
  ASSERT_FALSE(rle_idx.buf.empty());
  RleIdxRegion parsed_idx{};
  ASSERT_TRUE(ParseRleIdxRegion(rle_idx.buf.data(), n, rle_idx.param,
                                rle_idx.buf.data() + rle_idx.buf.size(),
                                &parsed_idx));
  EXPECT_EQ(RleIdxLowerBound(parsed_idx, 150), 5U);
  EXPECT_EQ(RleIdxUpperBound(parsed_idx, 150), 6U);
  EXPECT_EQ(RleIdxRowBefore(parsed_idx, 5U), 100U);
}

TEST(PaxChunkCodecsTest, StreamVByteAndRle8AndRleDeltaRoundTrip) {
  std::vector<int64_t> vals(256);
  for (size_t i = 0; i < vals.size(); ++i) {
    vals[i] = static_cast<int64_t>((i * 3) - 50);
  }
  const auto n = static_cast<uint32_t>(vals.size());

  // StreamVByte
  const EncodedCol svb = EncStreamVByteI64(vals);
  ASSERT_FALSE(svb.buf.empty());
  StreamVByteRegion svb_reg{};
  ASSERT_TRUE(ParseStreamVByteRegion(
      svb.buf.data(), n, svb.param, svb.buf.data() + svb.buf.size(), &svb_reg));
  std::vector<int64_t> decoded_svb(n);
  DecodeStreamVByteAll(svb_reg, decoded_svb.data());
  EXPECT_EQ(decoded_svb, vals);
  EXPECT_EQ(StreamVByteLowerBound(svb_reg, vals[100]), 100U);

  // Rle8
  const EncodedCol rle8 = EncRle8I64(vals);
  ASSERT_FALSE(rle8.buf.empty());
  std::vector<int64_t> decoded_rle8(n);
  ASSERT_TRUE(DecodeRle8All(rle8.buf.data(), n, rle8.param,
                            rle8.buf.data() + rle8.buf.size(),
                            decoded_rle8.data()));
  EXPECT_EQ(decoded_rle8, vals);
  EXPECT_EQ(Rle8ValueAtParsed(rle8.buf.data(), n, rle8.param,
                              rle8.buf.data() + rle8.buf.size(), 77),
            vals[77]);

  // RleDelta
  const EncodedCol rle_delta = EncRleDeltaI64(vals);
  ASSERT_FALSE(rle_delta.buf.empty());
  std::vector<int64_t> decoded_rd(n);
  ASSERT_TRUE(DecodeRleDeltaAll(rle_delta.buf.data(), n, rle_delta.param,
                                rle_delta.buf.data() + rle_delta.buf.size(),
                                decoded_rd.data()));
  EXPECT_EQ(decoded_rd, vals);
}

TEST(PaxChunkCodecsTest, Bp128AndFastPforAndDeltaPForAndSimdForRoundTrip) {
  std::vector<int64_t> vals(300);
  for (size_t i = 0; i < vals.size(); ++i) {
    vals[i] = 1000 + static_cast<int64_t>((i * 5) + (i % 7));
  }
  // Inject a couple of outliers for PFOR exception lanes
  vals[33] = 500000;
  vals[177] = 900000;
  const auto n = static_cast<uint32_t>(vals.size());

  // Bp128
  const EncodedCol bp128 = EncBp128I64(vals);
  ASSERT_FALSE(bp128.buf.empty());
  Bp128Region bp_reg{};
  ASSERT_TRUE(ParseBp128Region(bp128.buf.data(), n, bp128.param,
                               bp128.buf.data() + bp128.buf.size(), &bp_reg));
  std::vector<int64_t> dec_bp(n);
  DecodeBp128All(bp_reg, dec_bp.data());
  EXPECT_EQ(dec_bp, vals);
  uint64_t bp_cnt = 0;
  int64_t bp_sum = 0;
  ASSERT_TRUE(Bp128RegionCountSumParsed(bp_reg, PaxChunk::CompareOp::kGe, 0,
                                        &bp_cnt, &bp_sum));
  EXPECT_EQ(bp_cnt, n);

  // FastPfor
  const EncodedCol fpfor = EncFastPforI64(vals);
  ASSERT_FALSE(fpfor.buf.empty());
  FastPforRegion fp_reg{};
  ASSERT_TRUE(ParseFastPforRegion(fpfor.buf.data(), n, fpfor.param,
                                  fpfor.buf.data() + fpfor.buf.size(),
                                  &fp_reg));
  std::vector<int64_t> dec_fp(n);
  DecodeFastPforAll(fp_reg, dec_fp.data());
  EXPECT_EQ(dec_fp, vals);

  // DeltaFastPfor on stationary deltas
  std::vector<int64_t> delta_vals(300);
  int64_t cur = 1000;
  for (size_t i = 0; i < delta_vals.size(); ++i) {
    cur += static_cast<int64_t>(5 + (i % 4));
    if (i == 70) {
      cur += 50000;
    }
    delta_vals[i] = cur;
  }
  const EncodedCol dfpfor = EncDeltaFastPforI64(delta_vals);
  ASSERT_FALSE(dfpfor.buf.empty());
  DeltaFastPforRegion dfp_reg{};
  ASSERT_TRUE(ParseDeltaFastPforRegion(dfpfor.buf.data(), n, dfpfor.param,
                                       dfpfor.buf.data() + dfpfor.buf.size(),
                                       &dfp_reg));
  std::vector<int64_t> dec_dfp(n);
  DecodeDeltaFastPforAll(dfp_reg, dec_dfp.data());
  EXPECT_EQ(dec_dfp, delta_vals);

  // SimdFor
  const EncodedCol sfor = EncSimdForI64(vals);
  ASSERT_FALSE(sfor.buf.empty());
  SimdForRegion sf_reg{};
  ASSERT_TRUE(ParseSimdForRegion(sfor.buf.data(), n, sfor.param,
                                 sfor.buf.data() + sfor.buf.size(), &sf_reg));
  std::vector<int64_t> dec_sf(n);
  DecodeSimdForAll(sf_reg, dec_sf.data());
  EXPECT_EQ(dec_sf, vals);
}

TEST(PaxChunkCodecsTest, EliasFanoAndInterpAndPBitSRoundTrip) {
  std::vector<int64_t> mono(200);
  for (size_t i = 0; i < mono.size(); ++i) {
    mono[i] = static_cast<int64_t>((i * 11) + 50);
  }
  const auto n = static_cast<uint32_t>(mono.size());

  // EliasFano
  const EncodedCol ef = EncEliasFanoI64(mono);
  ASSERT_FALSE(ef.buf.empty());
  for (uint32_t i = 0; i < n; i += 19) {
    EXPECT_EQ(Int64AtEliasFano(ef.buf.data(), ef.param, n, i,
                               ef.buf.data() + ef.buf.size(), nullptr),
              mono[i]);
  }
  uint32_t lb_pos = 0;
  ASSERT_TRUE(RegionLowerBound(ef.buf.data(), ef.scheme, ef.param, n, mono[45],
                               &lb_pos, ef.buf.data() + ef.buf.size(),
                               nullptr));
  EXPECT_EQ(lb_pos, 45U);

  // Interp
  const EncodedCol interp = EncInterpI64(mono);
  ASSERT_FALSE(interp.buf.empty());
  InterpRegion interp_reg{};
  ASSERT_TRUE(ParseInterpRegion(interp.buf.data(), n, interp.param,
                                interp.buf.data() + interp.buf.size(),
                                &interp_reg));
  std::vector<int64_t> dec_interp(n);
  DecodeInterpAll(interp_reg, dec_interp.data());
  EXPECT_EQ(dec_interp, mono);

  // PBitS
  const EncodedCol pbits = EncPBitSI64(mono, kNoEncodeBudget, true);
  ASSERT_FALSE(pbits.buf.empty());
  PBitSRegion pbits_reg{};
  ASSERT_TRUE(ParsePBitSRegion(pbits.buf.data(), n, pbits.param,
                               pbits.buf.data() + pbits.buf.size(),
                               &pbits_reg));
  std::vector<int64_t> dec_pbits(n);
  DecodePBitSAll(pbits_reg, dec_pbits.data());
  EXPECT_EQ(dec_pbits, mono);
}

TEST(PaxChunkCodecsTest, Float64AndVarcharCodecsRoundTrip) {
  std::vector<uint64_t> fbits(128);
  for (size_t i = 0; i < fbits.size(); ++i) {
    const double d = 10.25 + (static_cast<double>(i) * 0.5);
    fbits[i] = Chimp128Bits(d);
  }
  const auto fn = static_cast<uint32_t>(fbits.size());

  const EncodedCol chimp128 = EncChimp128F64(fbits);
  ASSERT_FALSE(chimp128.buf.empty());
  Chimp128Region c128_reg{};
  ASSERT_TRUE(ParseChimp128Region(chimp128.buf.data(), fn, chimp128.param,
                                  chimp128.buf.data() + chimp128.buf.size(),
                                  &c128_reg));
  EXPECT_TRUE(Chimp128MetadataMatches(c128_reg));

  const EncodedCol patas = EncPatasF64(fbits);
  ASSERT_FALSE(patas.buf.empty());
  PatasRegion patas_reg{};
  ASSERT_TRUE(ParsePatasRegion(patas.buf.data(), fn, patas.param,
                               patas.buf.data() + patas.buf.size(),
                               &patas_reg));
  EXPECT_TRUE(PatasMetadataMatches(patas_reg));

  // Varchar codecs: FrontCode, OpDict
  std::vector<std::string> strs;
  std::vector<std::string_view> svs;
  strs.reserve(64);
  svs.reserve(64);
  for (int i = 0; i < 64; ++i) {
    strs.push_back("Customer#" + std::to_string(1000 + (i % 8)));
  }
  for (const auto& s : strs) {
    svs.emplace_back(s);
  }
  const auto sn = static_cast<uint32_t>(svs.size());

  const EncodedCol op_dict = EncOpDictStr(svs);
  ASSERT_FALSE(op_dict.buf.empty());
  for (uint32_t i = 0; i < sn; i += 7) {
    const Value v =
        VarcharRegionAt(op_dict.buf.data(), op_dict.scheme, op_dict.param, sn,
                        i, op_dict.buf.data() + op_dict.buf.size());
    EXPECT_EQ(v.value.varchar_value, svs[i]);
  }

  const EncodedCol fc = EncFrontCodeStr(svs);
  ASSERT_FALSE(fc.buf.empty());
  for (uint32_t i = 0; i < sn; i += 9) {
    std::string out;
    ASSERT_TRUE(DecodeFrontCodeAtInto(fc.buf.data(), fc.param, sn, i,
                                      fc.buf.data() + fc.buf.size(), &out));
    EXPECT_EQ(out, svs[i]);
  }
}

}  // namespace
}  // namespace tinylamb
