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

// Deterministic seed-corpus replay suite for the 11 table/fuzz/ storage
// harnesses (row_codec, memcmp, pax_mutate, lsm_merge, memtable_model,
// schema, chunk_footer, region_scheme, nullmap_varchar, footer_sections,
// encode_agree). Generates the structured seed corpus in-memory and drives
// each harness entry point directly under ctest.

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "table/fuzz/fuzz_chunk_common.hpp"
#include "table/fuzz/fuzz_helpers.hpp"
#include "table/fuzz/fuzz_scheme_cases.hpp"
#include "table/fuzz/fuzz_str_cases.hpp"

namespace tinylamb::fuzz {

namespace {

// This replay harness is single-threaded; the process environment is global
// here by design. Mirror region_scheme_fuzzer.cpp's ScopedForcedScheme: the
// getenv/setenv/unsetenv calls are annotated for that deliberate choice.
class ScopedForceScheme {
 public:
  explicit ScopedForceScheme(const char* value) {
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    if (const char* old = std::getenv("TINYLAMB_FORCE_SCHEME")) {
      previous_ = old;
    }
    setenv("TINYLAMB_FORCE_SCHEME", value, 1);  // NOLINT(concurrency-mt-unsafe)
  }
  ~ScopedForceScheme() {
    if (previous_) {
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      setenv("TINYLAMB_FORCE_SCHEME", previous_->c_str(), 1);
    } else {
      unsetenv("TINYLAMB_FORCE_SCHEME");  // NOLINT(concurrency-mt-unsafe)
    }
  }
  ScopedForceScheme(const ScopedForceScheme&) = delete;
  ScopedForceScheme& operator=(const ScopedForceScheme&) = delete;
  ScopedForceScheme(ScopedForceScheme&&) = delete;
  ScopedForceScheme& operator=(ScopedForceScheme&&) = delete;

 private:
  std::optional<std::string> previous_;
};

struct Rng {
  uint64_t s;
  explicit Rng(uint64_t seed) : s(seed == 0 ? 0x9E3779B97F4A7C15ULL : seed) {}
  uint32_t Next() {
    s ^= s << 13;
    s ^= s >> 7;
    s ^= s << 17;
    return static_cast<uint32_t>(s >> 32);
  }
  uint32_t Below(uint32_t n) { return n == 0 ? 0 : Next() % n; }
};

std::vector<std::vector<uint8_t>> MakeRowCodecSeeds() {
  std::vector<std::vector<uint8_t>> payloads = {
      {},
      {0x00},
      {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F},
      {0x01, 0x80, 0, 0, 0, 0, 0, 0, 0},
      {0x02, 0x03, 'a', 'b', 'c'},
      {0x02, 0xFF, 0xFF},
      {0x03, 0, 0, 0, 0, 0, 0, 0, 0xF0},
      {0x02, 0x00},
  };
  std::vector<std::vector<uint8_t>> seeds;
  for (uint8_t seed_byte = 0; seed_byte < 16; ++seed_byte) {
    for (uint8_t mode = 0; mode < 4; ++mode) {
      for (const auto& payload : payloads) {
        // A 2-byte {seed, mode=0} input hits no fuzzer branch at all;
        // skip it so the corpus only contains meaningful cases.
        if (mode == 0 && payload.empty()) {
          continue;
        }
        std::vector<uint8_t> seed{seed_byte, mode};
        seed.insert(seed.end(), payload.begin(), payload.end());
        seeds.push_back(std::move(seed));
      }
    }
  }
  return seeds;
}

TEST(PaxStorageFuzzerTest, RowCodecSeeds) {
  for (const auto& seed : MakeRowCodecSeeds()) {
    EXPECT_EQ(0, FuzzRowCodecOneInput(seed.data(), seed.size()));
  }
}

TEST(PaxStorageFuzzerTest, MemcmpSeeds) {
  for (const auto& seed : MakeRowCodecSeeds()) {
    EXPECT_EQ(0, FuzzMemcmpOneInput(seed.data(), seed.size()));
  }
  // Regression for crash-701fd9e9961b2a5720972128439912684e2205b3:
  // kMemcomparableInterval (0x07) with all-zero 8-byte nanos/months payload
  // decodes to INT64_MIN; IntervalValue::TryToString -> TryParse must round-
  // trip INT64_MIN components without overflowing positive int64_t before
  // applying the leading minus sign.
  std::vector<uint8_t> interval_int64_min_repro(91, 0x00);
  const std::vector<uint8_t> prefix = {
      0x00, 0x00, 0x03, 0xff, 0xff, 0x49, 0x33, 0x32, 0x00, 0x00, 0x00, 0x07,
      0x02, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02};
  for (size_t i = 0; i < prefix.size(); ++i) {
    interval_int64_min_repro[i] = prefix[i];
  }
  EXPECT_EQ(0, FuzzMemcmpOneInput(interval_int64_min_repro.data(),
                                  interval_int64_min_repro.size()));

  // Also test all-zero months, days, and nanos (all three = INT64_MIN).
  std::vector<uint8_t> all_int64_min_interval(25, 0x00);
  all_int64_min_interval[0] = 0x07;
  EXPECT_EQ(0, FuzzMemcmpOneInput(all_int64_min_interval.data(),
                                  all_int64_min_interval.size()));
}

TEST(PaxStorageFuzzerTest, PaxMutateSeeds) {
  Rng rng(0xA5A5C0DEU);
  for (int i = 0; i < 24; ++i) {
    std::vector<uint8_t> seed;
    const size_t mask_len = 1 + rng.Below(48);
    seed.reserve(mask_len + 9);
    for (size_t k = 0; k < mask_len; ++k) {
      seed.push_back(static_cast<uint8_t>(rng.Next() & 0xFFU));
    }
    const auto col = static_cast<uint8_t>(rng.Below(4));
    const int64_t probe = static_cast<int64_t>(rng.Next()) * 1000;
    seed.push_back(col);
    const auto* p = reinterpret_cast<const uint8_t*>(&probe);
    seed.insert(seed.end(), p, p + 8);
    EXPECT_EQ(0, FuzzPaxMutateOneInput(seed.data(), seed.size()));
  }
  for (int variant = 0; variant < 2; ++variant) {
    std::vector<uint8_t> seed(64, variant == 0 ? 0x00 : 0xFF);
    const int64_t probe = variant == 0 ? 0 : -1;
    seed.erase(seed.end() - 9, seed.end());
    seed.push_back(0);  // col
    const auto* p = reinterpret_cast<const uint8_t*>(&probe);
    seed.insert(seed.end(), p, p + 8);
    EXPECT_EQ(0, FuzzPaxMutateOneInput(seed.data(), seed.size()));
  }
}

TEST(PaxStorageFuzzerTest, LsmMergeSeeds) {
  Rng rng(0xBEEF0001U);
  for (int nruns = 1; nruns <= 4; ++nruns) {
    for (int variant = 0; variant < 6; ++variant) {
      std::vector<uint8_t> seed;
      // T4271: the fuzzer reads the 0x10 bit of data[0] as
      // "reverse-run-merge path"; nruns-1 (0..3) never set it, so the
      // deterministic corpus never exercised that path. Alternate it by
      // variant (the reverse of one run is a no-op, keep those plain).
      seed.push_back(static_cast<uint8_t>(
          (nruns - 1) |
          (((variant & 1) != 0)
               ? 0x10
               : 0)));  // NOLINT(readability-implicit-bool-conversion)
      seed.push_back(static_cast<uint8_t>(variant));
      for (int r = 0; r < nruns; ++r) {
        const int n_keys = 1 + static_cast<int>(rng.Below(16));
        seed.push_back(static_cast<uint8_t>(n_keys));
        for (int k = 0; k < n_keys; ++k) {
          int64_t id = 0;
          switch (variant) {
            case 0:
              id = k;
              break;
            case 1:
              id = (k % 7 == 3) ? -k : k;
              break;
            case 2:
              id = 3;
              break;
            case 3:
              id = 0x7FFFFFFFFFFFFFFFLL;
              break;
            case 4:
              id = -k - 1;
              break;
            default:
              id = static_cast<int64_t>(rng.Next());
              break;
          }
          const auto* p = reinterpret_cast<const uint8_t*>(&id);
          seed.insert(seed.end(), p, p + 8);
        }
      }
      EXPECT_EQ(0, FuzzLsmMergeOneInput(seed.data(), seed.size()));
    }
  }
}

TEST(PaxStorageFuzzerTest, MemtableModelSeeds) {
  Rng meta(0x4D54U);
  for (int i = 0; i < 12; ++i) {
    for (const size_t sz : {size_t{32}, size_t{56}, size_t{128}}) {
      std::vector<uint8_t> seed(sz, 0);
      const uint64_t v = (static_cast<uint64_t>(meta.Next()) << 32) |
                         static_cast<uint64_t>(meta.Next());
      for (int b = 0; b < 8; ++b) {
        seed[static_cast<size_t>(b)] =
            static_cast<uint8_t>((v >> (8 * b)) & 0xFFU);
      }
      EXPECT_EQ(0, FuzzMemtableModelOneInput(seed.data(), seed.size()));
    }
  }
}

TEST(PaxStorageFuzzerTest, SchemaSeeds) {
  Rng meta(0x5343U);
  for (int i = 0; i < 12; ++i) {
    for (const size_t sz : {size_t{64}, size_t{88}, size_t{160}}) {
      std::vector<uint8_t> seed(sz, 0);
      const uint64_t v = (static_cast<uint64_t>(meta.Next()) << 32) |
                         static_cast<uint64_t>(meta.Next());
      for (int b = 0; b < 8; ++b) {
        seed[static_cast<size_t>(b)] =
            static_cast<uint8_t>((v >> (8 * b)) & 0xFFU);
      }
      EXPECT_EQ(0, FuzzSchemaOneInput(seed.data(), seed.size()));
    }
  }
}

TEST(PaxStorageFuzzerTest, ChunkFooterSeeds) {
  Rng rng(0x5EEDF00DU);
  for (uint8_t target = 0; target < 7; ++target) {
    for (uint8_t shape = 0; shape < 16; ++shape) {
      for (int resign = 0; resign < 2; ++resign) {
        std::vector<uint8_t> seed;
        seed.push_back(shape);
        seed.push_back(target);
        seed.push_back(
            static_cast<uint8_t>((static_cast<uint32_t>(resign) & 0x1U) |
                                 ((static_cast<uint32_t>(shape) & 0x3U) << 1)));
        seed.push_back(static_cast<uint8_t>(1U + rng.Below(6)));
        for (int k = 0; k < 4; ++k) {
          const uint32_t roll = rng.Next();
          seed.push_back(static_cast<uint8_t>(roll));
          seed.push_back(static_cast<uint8_t>(roll >> 8));
          seed.push_back(static_cast<uint8_t>(roll >> 16));
          seed.push_back(static_cast<uint8_t>(roll >> 24));
        }
        const auto probe = static_cast<int64_t>(rng.Next());
        const auto* p = reinterpret_cast<const uint8_t*>(&probe);
        seed.insert(seed.end(), p, p + 8);
        EXPECT_EQ(0, FuzzChunkFooterOneInput(seed.data(), seed.size()));
      }
    }
  }
}

// P340: when the kPostings fold failed to parse, control fell into the
// generic per-row loop, which cannot decode the self-length-less Rice
// stream under the region_end == nullptr convention -- every row degraded
// to 0 and the corrupt region answered a plausible `0 OP lit` count.  The
// fold must now surface kCorrupt, and a valid region must still answer the
// exact row-mirror count.
TEST(PaxStorageFuzzerTest, PostingsCountFoldRejectsUndecodableParam) {
  const Schema sc = MakeStorageSchema();
  PaxChunk chunk;
  bool encoded = false;
  for (uint8_t shape : {0, 2, 4, 6}) {
    ScopedForceScheme force("POSTINGS");
    auto built =
        PaxChunk::BuildFromSortedRun(MakeRun(MakeStorageRows(shape), sc), sc);
    ASSERT_TRUE(built.HasValue());
    chunk = built.MoveValue();
    const auto& enc = chunk.GetFooter().encodings;
    if (enc.size() > 4 &&
        enc[4] == static_cast<uint8_t>(PaxChunk::Scheme::kPostings)) {
      encoded = true;
      break;
    }
  }
  ASSERT_TRUE(encoded);

  size_t footer_off = 0;
  size_t footer_size = 0;
  ASSERT_TRUE(WireLayout(chunk.Data(), &footer_off, &footer_size));
  const PaxChunk::Footer& f = chunk.GetFooter();
  ASSERT_GT(f.column_offsets.size(), 5U);
  ASSERT_FALSE(f.row_counts.empty());
  const std::vector<char>& b = chunk.Data();
  const size_t data_end = b.size() - footer_size - 4;
  const size_t off = f.column_offsets[4];
  const size_t succ = f.column_offsets[5];
  ASSERT_GT(succ, off);
  ASSERT_LE(succ, data_end);
  const auto n = static_cast<uint32_t>(f.row_counts[0]);
  const auto param = static_cast<uint32_t>((static_cast<uint8_t>(b[off]) << 8) |
                                           static_cast<uint8_t>(b[off + 1]));

  const auto good = PaxChunk::ByteSliceNullAwareCount(
      b.data() + off, PaxChunk::Scheme::kPostings, param, n, nullptr,
      PaxChunk::CompareOp::kEq, /*lit=*/2);
  ASSERT_TRUE(good.HasValue());
  uint32_t mirror = 0;
  for (uint32_t i = 0; i < n; ++i) {
    const auto row = chunk.ReadRow(i, sc);
    ASSERT_TRUE(row.HasValue());
    const Value& v = row.Value()[4];
    if (!v.IsNull() && v.value.int_value == 2) {
      ++mirror;
    }
  }
  EXPECT_EQ(good.Value(), mirror);

  // param != header d: PostingsRegionCountSum's parse declines; the old
  // fallback would have answered the (plausible but wrong) all-rows-zero
  // count instead of reporting corruption.
  const auto bad = PaxChunk::ByteSliceNullAwareCount(
      b.data() + off, PaxChunk::Scheme::kPostings, param + 1, n, nullptr,
      PaxChunk::CompareOp::kEq, /*lit=*/2);
  ASSERT_FALSE(bad.HasValue());
  EXPECT_EQ(bad.GetStatus().GetCode(), StatusCode::kCorrupt);
}

TEST(PaxStorageFuzzerTest, ForceSchemeRestoresPreviousValue) {
  ScopedForceScheme outer("ORIGINAL");
  {
    ScopedForceScheme inner("TEMPORARY");
    EXPECT_STREQ(std::getenv("TINYLAMB_FORCE_SCHEME"), "TEMPORARY");
  }
  EXPECT_STREQ(std::getenv("TINYLAMB_FORCE_SCHEME"), "ORIGINAL");
}

TEST(PaxStorageFuzzerTest, ForcedDoubleCasesReachTheRequestedEncoding) {
  const Schema schema = MakeStorageSchema();
  auto check = [&schema](const auto& cases) {
    for (const auto& item : cases) {
      if (item.expected != PaxChunk::Scheme::kSfor &&
          item.expected != PaxChunk::Scheme::kExpFor) {
        continue;
      }
      ASSERT_EQ(schema.GetColumn(item.col).Type(), ValueType::kDouble);
      bool reached = false;
      for (size_t i = 0; i < item.nshapes; ++i) {
        ScopedForceScheme force(item.name);
        auto chunk = PaxChunk::BuildFromSortedRun(
            MakeRun(MakeStorageRowsForScheme(item.shapes[i], item.expected),
                    schema),
            schema);
        ASSERT_TRUE(chunk.HasValue());
        const auto& encodings = chunk.Value().GetFooter().encodings;
        if (item.col < encodings.size() &&
            encodings[item.col] == static_cast<uint8_t>(item.expected)) {
          reached = true;
          break;
        }
      }
      EXPECT_TRUE(reached) << item.name;
    }
  };
  check(kSchemeCases);
  check(kStrCases);
}

TEST(PaxStorageFuzzerTest, RegionSchemeSeeds) {
  Rng rng(0xC0FFEE01U);
  for (uint8_t kase = 0; kase < kSchemeCaseCount; ++kase) {
    for (int resign = 0; resign < 2; ++resign) {
      std::vector<uint8_t> seed;
      seed.push_back(kase);
      seed.push_back(static_cast<uint8_t>(rng.Below(5)));
      seed.push_back(
          static_cast<uint8_t>((static_cast<uint32_t>(resign) & 0x1U) |
                               ((static_cast<uint32_t>(kase) & 0x3U) << 1)));
      seed.push_back(static_cast<uint8_t>(1U + rng.Below(6)));
      for (int k = 0; k < 4; ++k) {
        const uint32_t roll = rng.Next();
        seed.push_back(static_cast<uint8_t>(roll));
        seed.push_back(static_cast<uint8_t>(roll >> 8));
        seed.push_back(static_cast<uint8_t>(roll >> 16));
        seed.push_back(static_cast<uint8_t>(roll >> 24));
      }
      const int64_t probe = static_cast<int64_t>(rng.Next()) % 4096;
      const auto* p = reinterpret_cast<const uint8_t*>(&probe);
      seed.insert(seed.end(), p, p + 8);
      EXPECT_EQ(0, FuzzRegionSchemeOneInput(seed.data(), seed.size()));
    }
  }
}

TEST(PaxStorageFuzzerTest, NullmapVarcharSeeds) {
  Rng rng(0xA0B0C0D0U);
  for (uint8_t kase = 0; kase < kStrCaseCount; ++kase) {
    for (uint8_t shape = 0; shape < 16; ++shape) {
      for (int resign = 0; resign < 2; ++resign) {
        std::vector<uint8_t> seed;
        seed.push_back(kase);
        seed.push_back(shape);
        seed.push_back(
            static_cast<uint8_t>((static_cast<uint32_t>(resign) & 0x1U) |
                                 ((static_cast<uint32_t>(shape) & 0x3U) << 1)));
        seed.push_back(static_cast<uint8_t>(1U + rng.Below(6)));
        for (int k = 0; k < 4; ++k) {
          const uint32_t roll = rng.Next();
          seed.push_back(static_cast<uint8_t>(roll));
          seed.push_back(static_cast<uint8_t>(roll >> 8));
          seed.push_back(static_cast<uint8_t>(roll >> 16));
          seed.push_back(static_cast<uint8_t>(roll >> 24));
        }
        const int64_t probe = static_cast<int64_t>(rng.Next()) % 4096;
        const auto* p = reinterpret_cast<const uint8_t*>(&probe);
        seed.insert(seed.end(), p, p + 8);
        EXPECT_EQ(0, FuzzNullmapVarcharOneInput(seed.data(), seed.size()));
      }
    }
  }
}

TEST(PaxStorageFuzzerTest, FooterSectionsSeeds) {
  constexpr int kModes = 4;
  for (const uint8_t shape : {0, 8, 15, 16, 32, 48}) {
    const size_t section_count = FooterSectionCountForSeed(shape);
    ASSERT_GT(section_count, 0U);
    ASSERT_LE(section_count, 256U);
    for (size_t sec = 0; sec < section_count; ++sec) {
      for (int mode = 0; mode < kModes; ++mode) {
        std::vector<uint8_t> seed(32, 0);
        seed[0] = shape;
        seed[1] = static_cast<uint8_t>(sec);
        seed[2] = static_cast<uint8_t>(1 + (sec % 7));
        seed[3] = static_cast<uint8_t>((sec * 3) % 64);
        seed[4] = static_cast<uint8_t>(mode);
        for (size_t k = 5; k < 8; ++k) {
          seed[k] = static_cast<uint8_t>(sec + k);
        }
        const uint64_t probe = (static_cast<uint64_t>(sec) * 1000003ULL) +
                               (static_cast<uint64_t>(mode) * 7919ULL);
        for (int k = 0; k < 8; ++k) {
          seed[24 + static_cast<size_t>(k)] =
              static_cast<uint8_t>((probe >> (8 * k)) & 0xFFU);
        }
        EXPECT_EQ(0, FuzzFooterSectionsOneInput(seed.data(), seed.size()));
      }
    }
  }
}

TEST(PaxStorageFuzzerTest, EncodeAgreeSeeds) {
  Rng rng(0x5A0B1E00U);
  for (uint32_t force = 0; force < 41; ++force) {
    for (const uint8_t count_idx :
         {uint8_t{0}, uint8_t{3}, uint8_t{8}, uint8_t{14}, uint8_t{17}}) {
      for (const uint8_t shape :
           {uint8_t{0}, uint8_t{3}, uint8_t{8}, uint8_t{16}}) {
        std::vector<uint8_t> seed;
        seed.push_back(static_cast<uint8_t>(force));
        seed.push_back(shape);
        seed.push_back(count_idx);
        for (int k = 0; k < 5; ++k) {
          const uint32_t r = rng.Next();
          seed.push_back(static_cast<uint8_t>(r));
          seed.push_back(static_cast<uint8_t>(r >> 8));
          seed.push_back(static_cast<uint8_t>(r >> 16));
          seed.push_back(static_cast<uint8_t>(r >> 24));
        }
        const int64_t probe = static_cast<int64_t>(rng.Next()) % 4096;
        const auto* p = reinterpret_cast<const uint8_t*>(&probe);
        seed.insert(seed.end(), p, p + 8);
        EXPECT_EQ(0, FuzzEncodeAgreeOneInput(seed.data(), seed.size()));
      }
    }
  }
}

}  // namespace
}  // namespace tinylamb::fuzz
