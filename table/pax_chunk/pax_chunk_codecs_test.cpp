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

#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include "table/pax_chunk/pax_chunk_internal.hpp"

namespace tinylamb {
namespace {

using Scheme = PaxChunk::Scheme;

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
      runs.push_back(100 + r * 10);
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
    vals[i] = static_cast<int64_t>(i * 3 - 50);
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
    vals[i] = 1000 + static_cast<int64_t>(i * 5 + (i % 7));
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
    if (i == 70) cur += 50000;
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
    mono[i] = static_cast<int64_t>(i * 11 + 50);
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
    const double d = 10.25 + static_cast<double>(i) * 0.5;
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
