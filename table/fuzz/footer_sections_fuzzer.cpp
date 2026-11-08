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

// Footer-SECTION fuzzing: aim mutations at the named optional footer sections
// instead of at "the footer".
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "common/constants.hpp"
#include "table/fuzz/fuzz_chunk_common.hpp"
#include "table/fuzz/fuzz_helpers.hpp"
#include "table/pax_chunk/pax_chunk.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb::fuzz {
namespace {

struct Section {
  const char* name;
  size_t off;
  size_t len;
};

struct SectionMap {
  std::vector<Section> sections;
  size_t footer_off = 0;
  size_t footer_size = 0;
  bool mapped = false;
};

bool MapSections(const PaxChunk::Footer& f, const std::vector<char>& b,
                 SectionMap* out) {
  size_t footer_off = 0;
  size_t footer_size = 0;
  if (!WireLayout(b, &footer_off, &footer_size)) {
    return false;
  }
  if (footer_size < 16) {
    return false;
  }
  const size_t rg = f.row_group_count;
  const size_t cc = f.column_count;
  const size_t ncell = rg * cc;
  const size_t foot_end = footer_off + footer_size;
  out->footer_off = footer_off;
  out->footer_size = footer_size;

  size_t null_flag_at = 0;
  size_t ext_flag_at = 0;
  size_t inv_flag_at = 0;
  size_t xor_flag_at = 0;
  size_t pgm_flag_at = 0;
  size_t hash_flag_at = 0;
  size_t inv_count_at = 0;
  size_t xor_count_at = 0;
  size_t pgm_count_at = 0;

  size_t p = footer_off;
  auto need = [&](size_t n) { return n <= foot_end - p; };
  p += 30;
  if (!need(rg * 2)) {
    return false;
  }
  out->sections.push_back({"row_counts", p, rg * 2});
  p += rg * 2;
  if (!need(ncell * 4)) {
    return false;
  }
  out->sections.push_back({"column_offsets", p, ncell * 4});
  p += ncell * 4;
  if (f.zone_map_present) {
    if (!need(rg * 16)) {
      return false;
    }
    out->sections.push_back({"zone_maps", p, rg * 16});
    p += rg * 16;
  }
  if (!need(ncell)) {
    return false;
  }
  out->sections.push_back({"encodings", p, ncell});
  p += ncell;
  if (!need(ncell * 4)) {
    return false;
  }
  out->sections.push_back({"params", p, ncell * 4});
  p += ncell * 4;

  auto section = [&](const char* name, bool present, size_t len,
                     size_t* flag_at) -> bool {
    if (!need(1)) {
      return false;
    }
    *flag_at = p;
    p += 1;
    out->sections.push_back({name, *flag_at, 1});
    if (present) {
      if (!need(len)) {
        return false;
      }
      out->sections.push_back({name, p, len});
      p += len;
    }
    return true;
  };
  size_t unused_flag = 0;
  if (!section("col_stats", f.col_stats_present, cc * 17, &unused_flag)) {
    return false;
  }
  if (!section("all_zone_maps", f.all_zm_present,
               (f.all_zone_mins.size() * 8) + (f.all_zone_maxs.size() * 8),
               &unused_flag)) {
    return false;
  }
  if (!section("bloom", f.bloom_present, f.rg_blooms.size() * 8,
               &unused_flag)) {
    return false;
  }
  if (!section("rg_sums", f.rg_sums_present, f.rg_sums.size() * 8,
               &unused_flag)) {
    return false;
  }
  if (!section("hll", f.hll_present, f.hll_sketches.size(), &unused_flag)) {
    return false;
  }

  if (!need(1)) {
    return false;
  }
  hash_flag_at = p;
  p += 1;
  out->sections.push_back({"hash_idx_flag", hash_flag_at, 1});
  if (f.hash_idx_present) {
    if (!need(1)) {
      return false;
    }
    const size_t log2_at = p;
    const auto log2 = static_cast<size_t>(static_cast<uint8_t>(b[p]));
    p += 1;
    out->sections.push_back({"hash_idx_log2", log2_at, 1});
    if (log2 >= 32) {
      return false;
    }
    const size_t sz = static_cast<size_t>(1) << log2;
    if (!need(sz * 12)) {
      return false;
    }
    out->sections.push_back({"hash_idx_body", p, sz * 12});
    p += sz * 12;
  }

  if (!section("null_bitmaps", f.null_present, f.null_bitmaps.size(),
               &null_flag_at)) {
    return false;
  }
  constexpr size_t kHistBins = 16;
  constexpr size_t kExtStatsPerColumn = (size_t{5} * 8) + (kHistBins * 8);
  if (!section("ext_stats", f.ext_stats_present, cc * kExtStatsPerColumn,
               &ext_flag_at)) {
    return false;
  }
  const size_t pos_after_ext = p;

  const size_t crc_bytes = f.format_version >= 2 ? 8 : 0;
  if (foot_end < pos_after_ext + crc_bytes) {
    return false;
  }
  size_t tail = foot_end - crc_bytes;
  if (f.pgm_present) {
    const size_t body = 8 + (f.pgm_segments.size() * 24);
    if (tail < body + 1) {
      return false;
    }
    const size_t body_at = tail - body;
    pgm_flag_at = body_at - 1;
    out->sections.push_back({"pgm_flag", pgm_flag_at, 1});
    out->sections.push_back({"pgm_eps_nseg", body_at, 8});
    pgm_count_at = body_at + 4;
    if (!f.pgm_segments.empty()) {
      out->sections.push_back({"pgm_first_segment", body_at + 8, 24});
    }
    tail = body_at - 1;
  }
  if (f.xor_present) {
    size_t body = 4;
    for (const auto& c : f.xor_cells) {
      body += 16 + c.table.size();
    }
    if (tail < body + 1) {
      return false;
    }
    const size_t body_at = tail - body;
    xor_flag_at = body_at - 1;
    out->sections.push_back({"xor8_flag", xor_flag_at, 1});
    out->sections.push_back({"xor8_count", body_at, 4});
    xor_count_at = body_at;
    if (!f.xor_cells.empty()) {
      out->sections.push_back({"xor8_first_cell", body_at + 4, 16});
    }
    tail = body_at - 1;
  }
  if (f.inv_idx_present) {
    if (tail <= pos_after_ext + 5) {
      return false;
    }
    inv_flag_at = pos_after_ext;
    inv_count_at = pos_after_ext + 1;
    out->sections.push_back({"inv_idx_flag", inv_flag_at, 1});
    out->sections.push_back({"inv_idx_count", inv_count_at, 4});
  } else if (tail != pos_after_ext) {
    return false;
  }

  auto flag_is_set = [&](size_t at) {
    return at != 0 && at < b.size() && static_cast<uint8_t>(b[at]) == 1;
  };
  auto count_is = [&](size_t at, uint64_t want) {
    return at != 0 && at + 4 <= b.size() && LoadBE32(b, at) == want;
  };
  if (f.null_present && !flag_is_set(null_flag_at)) {
    return false;
  }
  if (f.ext_stats_present && !flag_is_set(ext_flag_at)) {
    return false;
  }
  if (f.hash_idx_present && !flag_is_set(hash_flag_at)) {
    return false;
  }
  if (f.pgm_present && !flag_is_set(pgm_flag_at)) {
    return false;
  }
  if (f.xor_present && !flag_is_set(xor_flag_at)) {
    return false;
  }
  if (f.inv_idx_present && !flag_is_set(inv_flag_at)) {
    return false;
  }
  if (f.inv_idx_present && !count_is(inv_count_at, f.inv_index.size())) {
    return false;
  }
  if (f.xor_present && !count_is(xor_count_at, f.xor_cells.size())) {
    return false;
  }
  if (f.pgm_present && !count_is(pgm_count_at, f.pgm_segments.size())) {
    return false;
  }

  out->mapped = true;
  return true;
}

constexpr std::array<uint8_t, 5> kValues = {0x00, 0x01, 0x7F, 0x80, 0xFF};

PaxChunk BuildCompactChunk(uint8_t shape) {
  static std::array<PaxChunk, 16> cache;
  static std::array<bool, 16> built = {};
  const uint8_t idx = shape & 15;
  if (built[idx]) {
    return cache[idx];
  }

  constexpr int64_t kRows = 200;
  const bool ramp = (idx & 1) != 0;
  const bool two_valued = (idx & 2) != 0;
  const bool keep_nulls = (idx & 4) != 0;
  const bool dense_nulls = (idx & 8) != 0;
  const Schema sc = MakeStorageSchema();
  std::vector<Row> rows;
  rows.reserve(static_cast<size_t>(kRows));
  for (int64_t i = 0; i < kRows; ++i) {
    rows.push_back(Row{
        Value(i),
        Value(ramp ? i * 2 : (i * 3) + 1),
        Value("name-" + std::to_string(i % 61)),
        Value(12.5 + (static_cast<double>(i % 97) * 0.5)),
        Value(two_valued ? static_cast<int64_t>(i & 1)
                         : static_cast<int64_t>(i % 5)),
    });
  }
  if (keep_nulls) {
    for (int64_t i = 0; i < kRows; i += 7) {
      rows[static_cast<size_t>(i)][1] = Value();
    }
  }
  if (dense_nulls) {
    for (int64_t i = 100; i < kRows; i += 2) {
      rows[static_cast<size_t>(i)][2] = Value();
    }
  }
  cache[idx] = PaxChunk::BuildFromSortedRun(MakeRun(rows, sc), sc);
  built[idx] = true;
  return cache[idx];
}

void CheckCurrent(std::vector<char>& work, char orig, const Schema& sc,
                  int64_t key_probe, const char* what) {
  const Status verdict = PaxChunk::ValidateChunkBytes(work);
  const bool accepted = (verdict == Status::kSuccess);
  if (!accepted) {
    auto parsed = PaxChunk::Deserialize(work);
    if (parsed.HasValue()) {
      std::cerr << "walk rejected but parser accepted: " << what << "\n";
      std::abort();
    }
    return;
  }
  auto parsed = PaxChunk::Deserialize(work);
  if (!parsed.HasValue()) {
    std::cerr << "walk accepted but parser rejected: " << what << "\n";
    std::abort();
  }
  const PaxChunk& chunk = parsed.Value();
  const PaxChunk::Footer& f = chunk.GetFooter();
  uint64_t rows = 0;
  for (size_t i = 0; i < f.row_counts.size() && i < f.row_group_count; ++i) {
    rows += f.row_counts[i];
  }
  if (rows != chunk.TotalRowCount()) {
    std::cerr << "row count disagrees with row_counts[] (" << what << ")\n";
    std::abort();
  }
  size_t idx = 0;
  const Status pl = chunk.PointLookup(key_probe, sc, &idx);
  if (pl == Status::kSuccess && idx >= chunk.TotalRowCount()) {
    std::cerr << "PointLookup returned row " << idx << " of "
              << chunk.TotalRowCount() << " (" << what << ")\n";
    std::abort();
  }
  HammerChunk(chunk, sc, key_probe);
  (void)orig;
}

void SweepSection(std::vector<char>& work, size_t off, size_t len,
                  size_t stride, size_t start, const Schema& sc,
                  int64_t key_probe, int full_budget, const char* name) {
  if (len == 0) {
    return;
  }
  constexpr size_t kMaxSweepSites = 64;
  if (len > kMaxSweepSites * stride) {
    stride = (len + kMaxSweepSites - 1) / kMaxSweepSites;
  }
  for (size_t i = start; i < len && full_budget >= 0; i += stride) {
    for (uint8_t v : kValues) {
      const size_t at = off + i;
      const char orig = work[at];
      work[at] = static_cast<char>(v);
      ResignChunkCrc(work);
      std::array<char, 160> what{};
      if (std::snprintf(what.data(), what.size(), "%s[%zu]=%02x", name, i, v) <
          0) {
        std::abort();
      }
      if (full_budget > 0) {
        --full_budget;
        CheckCurrent(work, orig, sc, key_probe, what.data());
      } else {
        const Status verdict = PaxChunk::ValidateChunkBytes(work);
        if (verdict == Status::kSuccess) {
          auto parsed = PaxChunk::Deserialize(work);
          if (!parsed.HasValue()) {
            std::cerr << "walk accepted but parser rejected: " << what.data()
                      << "\n";
            std::abort();
          }
        }
      }
      work[at] = orig;
    }
  }
  ResignChunkCrc(work);
}

void PokeExtreme(std::vector<char>& bytes, size_t off, size_t width,
                 uint64_t value) {
  for (size_t k = 0; k < width; ++k) {
    bytes[off + width - 1 - k] = static_cast<char>((value >> (8 * k)) & 0xFF);
  }
}

}  // namespace

int FuzzFooterSectionsOneInput(const uint8_t* data, size_t size) {
  if (size < 12) {
    return 0;
  }
  const Schema sc = MakeStorageSchema();
  const auto shape = static_cast<uint8_t>(data[0] & 15);
  const PaxChunk good = BuildCompactChunk(shape);
  const std::vector<char>& base = good.Data();
  if (base.size() < 128) {
    return 0;
  }

  SectionMap map;
  if (!MapSections(good.GetFooter(), base, &map) || !map.mapped ||
      map.sections.empty()) {
    return 0;
  }

  const size_t sel = data[1] % map.sections.size();
  const Section& sec = map.sections[sel];
  if (sec.len == 0) {
    return 0;
  }
  const size_t stride = 1 + (data[2] % 7);
  const size_t start = data[3] % sec.len;
  const uint8_t mode = data[4] % 4;
  int64_t key_probe = 0;
  if (size >= 16) {
    std::memcpy(&key_probe, data + size - 8, 8);
  }

  const size_t off = sec.off;
  if (off + sec.len > base.size()) {
    return 0;
  }

  switch (mode) {
    case 0: {
      std::vector<char> work = base;
      SweepSection(work, off, sec.len, stride, start, sc, key_probe, 8,
                   sec.name);
      break;
    }
    case 1: {
      static constexpr std::array<uint64_t, 15> kExtremes = {
          0,
          1,
          2,
          0x7F,
          0x80,
          0xFF,
          0x7FFFULL,
          0x8000ULL,
          0xFFFFULL,
          0x7FFFFFFFULL,
          0x80000000ULL,
          0xFFFFFFFFULL,
          0x7FFFFFFFFFFFFFFFULL,
          0x8000000000000000ULL,
          0xFFFFFFFFFFFFFFFFULL};
      constexpr size_t kMaxSitesPerWidth = 4;
      for (size_t w : {size_t{1}, size_t{2}, size_t{4}, size_t{8}}) {
        const size_t sites = sec.len / w;
        const size_t fstride =
            sites > kMaxSitesPerWidth
                ? (sites + kMaxSitesPerWidth - 1) / kMaxSitesPerWidth
                : 1;
        for (size_t f = start % w; f + w <= sec.len; f += w * fstride) {
          for (uint64_t e : kExtremes) {
            std::vector<char> bytes = base;
            PokeExtreme(bytes, off + f, w, e);
            ResignChunkCrc(bytes);
            std::array<char, 160> what{};
            if (std::snprintf(what.data(), what.size(), "%s+%zu w=%zu e=%llx",
                              sec.name, f, w,
                              static_cast<unsigned long long>(e)) < 0) {
              std::abort();
            }
            CheckCurrent(bytes, 0, sc, key_probe, what.data());
          }
        }
      }
      break;
    }
    case 2: {
      if (sec.len != 1) {
        std::vector<char> bytes = base;
        bytes[off] = static_cast<char>(base[off] == 0 ? 1 : 0);
        ResignChunkCrc(bytes);
        CheckCurrent(bytes, 0, sc, key_probe, sec.name);
        break;
      }
      for (uint8_t v : {uint8_t{0}, uint8_t{1}, uint8_t{2}, uint8_t{0xFF}}) {
        std::vector<char> bytes = base;
        bytes[off] = static_cast<char>(v);
        ResignChunkCrc(bytes);
        std::array<char, 160> what{};
        if (std::snprintf(what.data(), what.size(), "%s flag=%02x", sec.name,
                          v) < 0) {
          std::abort();
        }
        CheckCurrent(bytes, 0, sc, key_probe, what.data());
      }
      break;
    }
    default: {
      const uint8_t n = 1 + (data[5] % 4);
      std::vector<char> bytes = base;
      for (uint8_t k = 0; k < n; ++k) {
        const size_t at =
            off + ((start + static_cast<size_t>(k) * 37) % sec.len);
        bytes[at] = static_cast<char>(kValues[(data[6 + k] + k) % 5]);
      }
      ResignChunkCrc(bytes);
      CheckCurrent(bytes, 0, sc, key_probe, sec.name);
      break;
    }
  }
  return 0;
}

}  // namespace tinylamb::fuzz

#ifndef TINYLAMB_FUZZ_NO_MAIN_SYMBOL
extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  return tinylamb::fuzz::FuzzFooterSectionsOneInput(data, size);
}
#endif
