/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "table/tlcs_compression.hpp"

#include <array>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <string>
#include <vector>

#include "common/random_string.hpp"
#include "common/test_util.hpp"
#include "database/database.hpp"
#include "database/transaction_context.hpp"
#include "gtest/gtest.h"
#include "table/iterator.hpp"
#include "table/table.hpp"
#include "table/tlcs_file.hpp"
#include "type/column.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb::tlcs_compression {
namespace {

TEST(TlcsCompressionTest, CompactNullsBitmapAndSparseModesRoundTrip) {
  // 1. Dense ~50% NULLs -> 1-bit bitmap (8x smaller).
  std::vector<uint8_t> dense(1000);
  for (size_t i = 0; i < dense.size(); ++i) {
    dense[i] = static_cast<uint8_t>((i * 7 + 3) % 2);
  }
  std::vector<uint8_t> enc;
  ASSERT_TRUE(TryEncodeCompactNulls(dense, &enc));
  EXPECT_EQ(enc[0], 0x01U);
  EXPECT_LT(enc.size(), dense.size() / 7);
  std::vector<uint8_t> dec;
  ASSERT_TRUE(
      DecodeCompactNulls(enc, static_cast<uint32_t>(dense.size()), &dec));
  EXPECT_EQ(dec, dense);

  // 2. Sparse NULLs (3 out of 1000) -> tag 0x02 (17 bytes vs 1000 bytes!).
  std::vector<uint8_t> sparse_nulls(1000, 0);
  sparse_nulls[5] = 1;
  sparse_nulls[500] = 1;
  sparse_nulls[999] = 1;
  ASSERT_TRUE(TryEncodeCompactNulls(sparse_nulls, &enc));
  EXPECT_EQ(enc[0], 0x02U);
  EXPECT_EQ(enc.size(), 17U);
  ASSERT_TRUE(DecodeCompactNulls(enc, 1000, &dec));
  EXPECT_EQ(dec, sparse_nulls);

  // 3. Sparse non-NULLs (2 out of 1000) -> tag 0x03 (13 bytes vs 1000 bytes!).
  std::vector<uint8_t> sparse_valid(1000, 1);
  sparse_valid[10] = 0;
  sparse_valid[900] = 0;
  ASSERT_TRUE(TryEncodeCompactNulls(sparse_valid, &enc));
  EXPECT_EQ(enc[0], 0x03U);
  EXPECT_EQ(enc.size(), 13U);
  ASSERT_TRUE(DecodeCompactNulls(enc, 1000, &dec));
  EXPECT_EQ(dec, sparse_valid);

  // 4. Backward compatibility: raw n-byte nullmap still decodes verbatim.
  ASSERT_TRUE(
      DecodeCompactNulls(dense, static_cast<uint32_t>(dense.size()), &dec));
  EXPECT_EQ(dec, dense);

  // 5. Wave 11 (BitLayouts.RowMajor.SparseGather + Nullable): sparse gather
  // matches full decode across raw, bitmap (0x01), sparse-null (0x02), and
  // sparse-valid (0x03) encodings.
  const std::vector<uint32_t> probe{0, 1, 5, 10, 499, 500, 900, 998, 999};
  auto check_gather = [&](const std::vector<uint8_t>& orig) {
    std::vector<uint8_t> packed;
    ASSERT_TRUE(TryEncodeCompactNulls(orig, &packed));
    std::vector<uint8_t> got(probe.size(), 0xFFU);
    ASSERT_TRUE(GatherCompactNulls(packed, static_cast<uint32_t>(orig.size()),
                                   probe, got.data()));
    for (size_t j = 0; j < probe.size(); ++j) {
      EXPECT_EQ(got[j], orig[probe[j]]) << "probe idx " << j;
    }
    // Also verify raw gather.
    std::vector<uint8_t> got_raw(probe.size(), 0xFFU);
    ASSERT_TRUE(GatherCompactNulls(orig, static_cast<uint32_t>(orig.size()),
                                   probe, got_raw.data()));
    EXPECT_EQ(got_raw, got);
  };
  check_gather(dense);
  check_gather(sparse_nulls);
  check_gather(sparse_valid);
}

TEST(TlcsCompressionTest, TemporalText47BitRoundTripAndRandomAccess) {
  constexpr uint32_t kRows = 200;
  std::vector<char> bytes;
  std::vector<uint32_t> ends;
  std::vector<uint8_t> nulls(kRows, 0);
  std::vector<std::string> expected;
  for (uint32_t i = 0; i < kRows; ++i) {
    if (i % 29 == 0) {
      nulls[i] = 1;
      expected.emplace_back();
    } else {
      std::array<char, 32> buf{};
      (void)std::snprintf(buf.data(), buf.size(),
                          "2013-07-%02u %02u:%02u:%02u+00", 1U + (i % 28U),
                          i % 24U, i % 60U, (i * 7U) % 60U);
      expected.emplace_back(buf.data());
      bytes.insert(bytes.end(), expected.back().begin(), expected.back().end());
    }
    ends.push_back(static_cast<uint32_t>(bytes.size()));
  }

  std::vector<uint8_t> enc;
  ASSERT_TRUE(TryEncodeTemporalText(bytes, ends, nulls.data(), kRows, &enc));
  EXPECT_EQ(enc.size(), kRows * kTemporalTextPackedBytes);
  EXPECT_LT(enc.size() * 4,
            bytes.size() + (static_cast<size_t>(kRows) * 4U));  // > 4x smaller!

  std::vector<char> dec_bytes;
  std::vector<uint32_t> dec_ends;
  ASSERT_TRUE(
      DecodeTemporalText(enc, nulls.data(), kRows, &dec_bytes, &dec_ends));
  EXPECT_EQ(dec_bytes, bytes);
  EXPECT_EQ(dec_ends, ends);

  // O(1) random access (`PackedSeek`).
  std::string single;
  ASSERT_TRUE(DecodeTemporalTextRow(enc, 1, &single));
  EXPECT_EQ(single, expected[1]);
  ASSERT_TRUE(DecodeTemporalTextRow(enc, 199, &single));
  EXPECT_EQ(single, expected[199]);

  // Non-timestamp strings must be rejected cleanly.
  const std::string bad = "not-a-timestamp";
  const std::vector<char> bad_b(bad.begin(), bad.end());
  const std::vector<uint32_t> bad_e{static_cast<uint32_t>(bad.size())};
  EXPECT_FALSE(TryEncodeTemporalText(bad_b, bad_e, nullptr, 1, &enc));
}

TEST(TlcsCompressionTest, VarCharFrontDictRoundTripAndShrinksUrls) {
  constexpr uint32_t kRows = 500;
  std::vector<char> bytes;
  std::vector<uint32_t> ends;
  std::vector<uint8_t> nulls(kRows, 0);
  for (uint32_t i = 0; i < kRows; ++i) {
    if (i % 37 == 0) {
      nulls[i] = 1;
    } else {
      std::string url =
          "https://www.example-shop.com/category/electronics/item-" +
          std::to_string(i % 40) + "/details.html";
      if (i % 50 == 1) {
        url.clear();  // test empty string preservation
      }
      bytes.insert(bytes.end(), url.begin(), url.end());
    }
    ends.push_back(static_cast<uint32_t>(bytes.size()));
  }

  std::vector<uint8_t> enc;
  ASSERT_TRUE(
      TryEncodeVarCharFrontDict(bytes, ends, nulls.data(), kRows, &enc));
  const size_t raw_total = bytes.size() + (kRows * sizeof(uint32_t));
  EXPECT_LT(enc.size() * 5, raw_total);  // > 5x smaller on URLs!

  std::vector<char> dec_bytes;
  std::vector<uint32_t> dec_ends;
  ASSERT_TRUE(
      DecodeVarCharFrontDict(enc, nulls.data(), kRows, &dec_bytes, &dec_ends));
  EXPECT_EQ(dec_bytes, bytes);
  EXPECT_EQ(dec_ends, ends);
}

TEST(TlcsCompressionTest, BitPackedForWithGcdStrideRoundTrip) {
  constexpr uint32_t kRows = 256;
  std::vector<int64_t> ints(kRows);
  std::vector<uint8_t> nulls(kRows, 0);
  int64_t mn = std::numeric_limits<int64_t>::max();
  int64_t mx = std::numeric_limits<int64_t>::min();
  for (uint32_t i = 0; i < kRows; ++i) {
    if (i % 41 == 0) {
      nulls[i] = 1;
      ints[i] = 0;
    } else {
      // Stride = 60, range 0..15 steps -> 4 bits/row instead of 2 bytes/row!
      const int64_t v = -1200 + (static_cast<int64_t>(i % 16U) * 60);
      ints[i] = v;
      mn = std::min(mn, v);
      mx = std::max(mx, v);
    }
  }
  std::vector<uint8_t> enc;
  ASSERT_TRUE(TryEncodeBitPackedFor(ints, nulls.data(), kRows, mn, mx,
                                    kRows * 2ULL, &enc));
  EXPECT_LT(enc.size(), kRows);  // ~137 bytes vs 512 bytes!

  std::vector<int64_t> dec(kRows, 0);
  ASSERT_TRUE(DecodeBitPackedFor(enc, kRows, mn, dec.data()));
  for (uint32_t i = 0; i < kRows; ++i) {
    if (nulls[i] == 0) {
      EXPECT_EQ(dec[i], ints[i]) << "row " << i;
    }
  }

  // Wave 8 (BitLayouts.RowMajor.SparseGather): sparse random-access gather
  // matches full decode across edge positions (0, middle, last) and arbitrary
  // bit widths (including >56-bit cross-word boundary).
  const std::vector<uint32_t> sparse_rows{0, 1, 7, 8, 15, 63, 128, 254, 255};
  std::vector<int64_t> gathered(sparse_rows.size(), 0);
  ASSERT_TRUE(GatherBitPackedFor(enc, kRows, mn, sparse_rows, gathered.data()));
  for (size_t j = 0; j < sparse_rows.size(); ++j) {
    EXPECT_EQ(gathered[j], dec[sparse_rows[j]]) << "sparse idx " << j;
  }

  // Test wide bit width (bw = 59) where bit_off + bw > 64 crosses 9 bytes.
  constexpr uint32_t kWideRows = 64;
  std::vector<int64_t> wide(kWideRows);
  int64_t w_mn = 0;
  int64_t w_mx = 0;
  for (uint32_t i = 0; i < kWideRows; ++i) {
    wide[i] = (static_cast<int64_t>(i) << 53) + static_cast<int64_t>(i * 17U);
    w_mx = std::max(w_mx, wide[i]);
  }
  std::vector<uint8_t> wide_enc;
  ASSERT_TRUE(TryEncodeBitPackedFor(wide, nullptr, kWideRows, w_mn, w_mx,
                                    kWideRows * 8ULL, &wide_enc));
  std::vector<int64_t> wide_dec(kWideRows, 0);
  ASSERT_TRUE(DecodeBitPackedFor(wide_enc, kWideRows, w_mn, wide_dec.data()));
  const std::vector<uint32_t> wide_rows{0, 1, 2, 3, 7, 31, 62, 63};
  std::vector<int64_t> wide_gathered(wide_rows.size(), 0);
  ASSERT_TRUE(GatherBitPackedFor(wide_enc, kWideRows, w_mn, wide_rows,
                                 wide_gathered.data()));
  for (size_t j = 0; j < wide_rows.size(); ++j) {
    EXPECT_EQ(wide_gathered[j], wide_dec[wide_rows[j]]) << "wide idx " << j;
  }
}

TEST(TlcsCompressionTest, XorDoubleBitExactRoundTrip) {
  constexpr uint32_t kRows = 256;
  std::vector<int64_t> bits(kRows);
  constexpr std::array<double, 8> vals = {
      0.0,   -0.0,  19.95, 19.95,
      19.95, 20.00, 100.5, std::numeric_limits<double>::quiet_NaN()};
  for (uint32_t i = 0; i < kRows; ++i) {
    const uint32_t bucket = i / 16U;
    const double d =
        (i < 16) ? vals[i % 8U] : (10.0 + (static_cast<double>(bucket) * 0.5));
    std::memcpy(&bits[i], &d, sizeof(double));
  }
  std::vector<uint8_t> enc;
  ASSERT_TRUE(TryEncodeXorDouble(bits, kRows, &enc));
  EXPECT_LT(enc.size() * 4, kRows * 8U);  // > 4x smaller!

  std::vector<int64_t> dec(kRows, 0);
  ASSERT_TRUE(DecodeXorDouble(enc, kRows, dec.data()));
  EXPECT_EQ(dec, bits);  // Bit-exact including -0.0 and NaN payloads!
}

TEST(TlcsCompressionTest, PhraseTableDpShortestParseAndZstdRoundTrip) {
  constexpr uint32_t kRows = 300;
  std::vector<char> bytes;
  std::vector<uint32_t> ends;
  std::vector<uint8_t> nulls(kRows, 0);
  for (uint32_t i = 0; i < kRows; ++i) {
    if (i % 43 == 0) {
      nulls[i] = 1;
    } else {
      // Every row is unique (high-cardinality URLs), testing substring phrase
      // sharing rather than whole-string dictionary deduplication.
      std::string url =
          "https://www.analytics-portal.example.org/v2/dashboard/session-" +
          std::to_string(i) + "/metrics?region=ap-northeast-1";
      bytes.insert(bytes.end(), url.begin(), url.end());
    }
    ends.push_back(static_cast<uint32_t>(bytes.size()));
  }
  const size_t raw_total = bytes.size() + (kRows * sizeof(uint32_t));

  // 1. Pure-C++ PhraseTable DP shortest parse.
  std::vector<uint8_t> enc_pt;
  ASSERT_TRUE(
      TryEncodeVarCharPhraseTable(bytes, ends, nulls.data(), kRows, &enc_pt));
  EXPECT_LT(enc_pt.size() * 2, raw_total);  // > 2x smaller on unique URLs!
  std::vector<char> dec_b;
  std::vector<uint32_t> dec_e;
  ASSERT_TRUE(
      DecodeVarCharPhraseTable(enc_pt, nulls.data(), kRows, &dec_b, &dec_e));
  EXPECT_EQ(dec_b, bytes);
  EXPECT_EQ(dec_e, ends);

  // 2. Zstandard frame container (when libzstd is linked).
  if (HasZstdSupport()) {
    std::vector<uint8_t> enc_z;
    ASSERT_TRUE(TryEncodeZstdVarChar(bytes, ends, nulls.data(), kRows,
                                     raw_total, /*level=*/3, &enc_z));
    EXPECT_LT(enc_z.size() * 10, raw_total);  // > 10x smaller on unique URLs!
    ASSERT_TRUE(DecodeZstdVarChar(enc_z, nulls.data(), kRows, &dec_b, &dec_e));
    EXPECT_EQ(dec_b, bytes);
    EXPECT_EQ(dec_e, ends);

    // Tampered CRC must be rejected (`Container.sound_VerifiedContainer`).
    enc_z[6] ^= 0xFFU;
    EXPECT_FALSE(DecodeZstdVarChar(enc_z, nulls.data(), kRows, &dec_b, &dec_e));
  }
}

TEST(TlcsCompressionTest, EndToEndTlcsFileShrinksStorageAndMatchesExactly) {
  Schema schema(
      "events",
      {Column("id", ValueType::kInt64), Column("ts", ValueType::kVarChar),
       Column("url", ValueType::kVarChar), Column("flag", ValueType::kInt64),
       Column("price", ValueType::kDouble),
       Column("const_tag", ValueType::kVarChar)});
  constexpr int kRows = 2000;
  std::vector<Row> expected;
  expected.reserve(kRows);
  for (int i = 0; i < kRows; ++i) {
    std::array<char, 32> ts_buf{};
    (void)std::snprintf(ts_buf.data(), ts_buf.size(),
                        "2013-07-%02d %02d:%02d:%02d+00", 1 + (i % 28), i % 24,
                        i % 60, (i * 11) % 60);
    std::string url =
        "https://news.example.org/2013/07/article-" + std::to_string(i % 25);
    expected.emplace_back(std::vector<Value>{
        Value(static_cast<int64_t>(i) * 100),
        (i % 50 == 0) ? Value() : Value(std::string(ts_buf.data())),
        Value(std::move(url)), Value(int64_t{i % 4}),
        Value(10.0 + (static_cast<double>(i % 5) * 0.25)),
        Value(std::string("CONSTANT_BLOCK_TAG"))});
  }

  auto build_and_measure = [&](bool disable_compression,
                               std::vector<Row>* scanned_out) -> uint64_t {
    const std::string prefix = "tlcs_comp_e2e_" + RandomString();
    std::unique_ptr<Database> db = Database::Create(prefix).MoveValue();
    page_id_t first_pid = 0;
    {
      TransactionContext ctx = db->BeginContext();
      StatusOr<Table> tbl_or = db->CreateTable(ctx, schema);
      EXPECT_TRUE(tbl_or.HasValue());
      Table tbl = tbl_or.MoveValue();
      first_pid = tbl.FirstPageId();
      for (const Row& r : expected) {
        EXPECT_TRUE(tbl.Insert(ctx.txn_, r).HasValue());
      }
      EXPECT_EQ(ctx.txn_.PreCommit(), Status::kSuccess);
    }
    if (disable_compression) {
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      setenv("TINYLAMB_DISABLE_TLCS_COMPRESSION", "1", 1);
    } else {
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      unsetenv("TINYLAMB_DISABLE_TLCS_COMPRESSION");
    }
    {
      TransactionContext ctx = db->BeginContext();
      EXPECT_EQ(db->CompactTableToTlcs(ctx, "events"), Status::kSuccess);
      if (scanned_out != nullptr) {
        StatusOr<Table> tbl_or = db->GetTable(ctx, "events");
        EXPECT_TRUE(tbl_or.HasValue());
        Table tbl = tbl_or.MoveValue();
        Iterator iter = tbl.BeginFullScan(ctx.txn_);
        while (iter.IsValid()) {
          scanned_out->push_back(*iter);
          ++iter;
        }
      }
    }
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    unsetenv("TINYLAMB_DISABLE_TLCS_COMPRESSION");
    const std::string tlcs_path =
        prefix + ".db.tlcs." + std::to_string(first_pid);
    return std::filesystem::file_size(tlcs_path);
  };

  std::vector<Row> scanned;
  const uint64_t comp_size = build_and_measure(false, &scanned);
  const uint64_t raw_size = build_and_measure(true, nullptr);
  (void)std::fprintf(
      stderr,
      "[TlcsCompressionTest] raw_tlcs=%llu B, compressed_tlcs=%llu B "
      "(%.2fx smaller)\n",
      static_cast<unsigned long long>(raw_size),
      static_cast<unsigned long long>(comp_size),
      static_cast<double>(raw_size) /
          static_cast<double>(std::max<uint64_t>(1, comp_size)));
  EXPECT_LT(comp_size * 2, raw_size)
      << "comp_size=" << comp_size << " raw_size=" << raw_size;
  ASSERT_EQ(scanned.size(), expected.size());
  for (size_t i = 0; i < expected.size(); ++i) {
    ASSERT_EQ(scanned[i], expected[i]) << "mismatch at row " << i;
  }
}

TEST(TlcsCompressionTest, ClickBenchLayoutStorageCodecs) {
  // 1. TieOrientation: Lean #guard example {{"abc"}, {}, {"z", "abd"}} where
  // reversing group 2 strictly reduces front-coded payload bytes.
  const std::vector<std::vector<std::string_view>> groups = {
      {"abc"},
      {},
      {"z", "abd"},
  };
  const TieOrientationPlan tie_plan = ChooseOptimalTieOrientations(groups);
  ASSERT_EQ(tie_plan.reversed.size(), groups.size());
  EXPECT_EQ(tie_plan.reversed[2], 1U);
  // Forward order ("abc","z","abd"): (2+3)+(2+1)+(2+3) = 13 bytes.
  // Reversed group 2 ("abc","abd","z"): (2+3)+(2+1)+(2+1) = 11 bytes.
  EXPECT_EQ(tie_plan.min_bytes, 11U);

  // 2. ClockWindow: Lean #guard example [63, 64, 65], w=2, exc=10 -> base=62
  // (zigzag(62) = 124 fits in 1 byte, whereas zigzag(64) = 128 takes 2 bytes!).
  const std::array<int64_t, 3> deltas = {63, 64, 65};
  const ClockWindowPlan cw = ChooseOptimalClockWindow(deltas, 2U, 10U);
  EXPECT_EQ(cw.base, 62);
  EXPECT_EQ(cw.exception_count, 0U);

  // 3. HashExceptions: Lean #guard example [(1,7),(1,8),(2,7),(1,7)].
  const std::array<uint32_t, 4> codes = {1, 1, 2, 1};
  const std::array<uint64_t, 4> hashes = {7, 8, 7, 7};
  const HashExceptionsTable ht = EncodeHashExceptions(codes, hashes);
  ASSERT_EQ(ht.exceptions.size(), 1U);
  EXPECT_EQ(ht.exceptions[0].first, 1U);
  EXPECT_EQ(ht.exceptions[0].second, 8ULL);
  for (uint32_t i = 0; i < 4; ++i) {
    EXPECT_EQ(ht.HashAt(i, codes[i]), hashes[i]);
  }
}

}  // namespace
}  // namespace tinylamb::tlcs_compression
