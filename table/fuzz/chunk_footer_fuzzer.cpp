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

// Footer-corruption fuzz: build one VALID self-describing chunk wire image,
// then poison the FOOTER (not just the data region) and re-sign the CRC32C
// words so the structural walk, Deserialize, and every Footer consumer are
// actually reached.
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

#include "common/constants.hpp"
#include "table/fuzz/fuzz_chunk_common.hpp"
#include "table/fuzz/fuzz_helpers.hpp"
#include "table/pax_chunk/pax_chunk.hpp"
#include "type/schema.hpp"

namespace tinylamb::fuzz {
namespace {

struct ImageMap {
  size_t footer_off = 0;
  size_t footer_size = 0;
  size_t data_end = 0;
  size_t row_counts_off = 0;
  size_t col_offsets_off = 0;
  size_t encodings_off = 0;
  size_t params_off = 0;
  size_t ncell = 0;
  size_t rg = 0;
};

bool MapImage(const std::vector<char>& b, ImageMap* m) {
  if (!WireLayout(b, &m->footer_off, &m->footer_size)) return false;
  const size_t fo = m->footer_off;
  const uint32_t rg = LoadBE32(b, fo + 12);
  const auto cc = static_cast<uint16_t>(LoadBE16(b, fo + 16));
  if (rg == 0 || rg > 4096 || cc == 0 || cc > 4096) return false;
  if (30 + static_cast<size_t>(rg) * 2 > m->footer_size) return false;
  m->rg = rg;
  m->ncell = static_cast<size_t>(rg) * cc;
  m->row_counts_off = fo + 30;
  m->col_offsets_off = m->row_counts_off + static_cast<size_t>(rg) * 2;
  m->data_end = b.size() - m->footer_size - 4;
  if (m->col_offsets_off + m->ncell * 4 > fo + m->footer_size) return false;
  size_t pos = m->col_offsets_off + m->ncell * 4;
  const auto zm = static_cast<uint8_t>(b[fo + 21]);
  if (zm > 1) return false;
  if (zm != 0) pos += static_cast<size_t>(rg) * 16;
  m->encodings_off = pos;
  pos += m->ncell;
  m->params_off = pos;
  pos += m->ncell * 4;
  return pos <= fo + m->footer_size;
}

size_t ChooseSite(const ImageMap& m, const std::vector<char>& b, size_t hot_off,
                  size_t hot_len, uint32_t roll, bool allow_data) {
  const size_t footer_bytes = b.size() - m.footer_off;
  if (hot_len > 0 && (roll & 3) != 3) {
    return hot_off + (roll >> 2) % hot_len;
  }
  if (allow_data && (roll & 7) == 7) {
    return (roll >> 5) % (m.data_end > 0 ? m.data_end : 1);
  }
  if (footer_bytes == 0) return 0;
  return m.footer_off + (roll >> 5) % footer_bytes;
}

enum class Target : uint8_t {
  kColumnOffsets,
  kRowCounts,
  kEncodings,
  kParams,
  kZoneMap,
  kNullBitmap,
  kWholeFooter,
  kTargetCount,
};

}  // namespace

int FuzzChunkFooterOneInput(const uint8_t* data, size_t size) {
  if (size < 24) return 0;
  const Schema sc = MakeStorageSchema();
  const auto shape = static_cast<uint8_t>(data[0] & 15);
  const PaxChunk good = BuildStorageChunk(shape);

  static uint32_t round_trip_done = 0;
  if ((round_trip_done & (1U << shape)) == 0) {
    if (!DeserializeRoundTrips(good, sc)) {
      std::fprintf(stderr,
                   "Deserialize(chunk.Data()) is not lossless (shape %u)\n",
                   shape);
      std::abort();
    }
    round_trip_done |= 1U << shape;
  }
  std::vector<char> bytes = good.Data();
  if (bytes.size() < 64) return 0;

  ImageMap m;
  if (!MapImage(bytes, &m)) return 0;

  const auto t =
      static_cast<Target>(data[1] % static_cast<uint8_t>(Target::kTargetCount));
  const bool resign = (data[2] & 0x1) != 0;
  const auto op_mask = static_cast<uint8_t>((data[2] >> 1) & 0x3);
  const uint8_t n_sites = 1 + (data[3] % 8);

  size_t hot_off = 0;
  size_t hot_len = 0;
  switch (t) {
    case Target::kColumnOffsets:
      hot_off = m.col_offsets_off;
      hot_len = m.ncell * 4;
      break;
    case Target::kRowCounts:
      hot_off = m.row_counts_off;
      hot_len = m.rg * 2;
      break;
    case Target::kEncodings:
      hot_off = m.encodings_off;
      hot_len = m.ncell;
      break;
    case Target::kParams:
      hot_off = m.params_off;
      hot_len = m.ncell * 4;
      break;
    case Target::kZoneMap:
      hot_off = m.col_offsets_off;
      hot_len = 16;
      break;
    case Target::kNullBitmap:
      hot_off = m.footer_off + m.footer_size / 2;
      hot_len = m.footer_size - m.footer_size / 2;
      break;
    case Target::kWholeFooter:
    case Target::kTargetCount:
    default:
      break;
  }

  int64_t probe = 0;
  if (size >= 16) std::memcpy(&probe, data + size - 8, 8);

  const size_t trailer_lo = bytes.size() - 4;

  size_t p = 4;
  for (uint8_t k = 0; k < n_sites; ++k) {
    if (p + 4 > size - 8) break;
    const uint32_t roll = static_cast<uint32_t>(data[p]) |
                          (static_cast<uint32_t>(data[p + 1]) << 8) |
                          (static_cast<uint32_t>(data[p + 2]) << 16) |
                          (static_cast<uint32_t>(data[p + 3]) << 24);
    p += 4;
    const size_t site = ChooseSite(m, bytes, hot_off, hot_len, roll, true);
    ApplyMutation(bytes, site, trailer_lo, op_mask, k + 1, roll,
                  static_cast<uint8_t>(1 + ((roll >> 28) & 0x7)));
  }

  if (resign) ResignChunkCrc(bytes);

  const Status v1 = PaxChunk::ValidateChunkBytes(bytes);
  const Status v2 = PaxChunk::ValidateChunkBytes(bytes);
  if (v1 != v2) {
    std::fprintf(stderr, "nondeterministic ValidateChunkBytes verdict\n");
    std::abort();
  }
  if (v1 != Status::kSuccess) return 0;

  auto chunk_or = PaxChunk::Deserialize(bytes);
  if (!chunk_or.HasValue()) {
    std::fprintf(stderr,
                 "ValidateChunkBytes accepted but Deserialize rejected\n");
    std::abort();
  }
  const PaxChunk& chunk = chunk_or.Value();

  HammerChunk(chunk, sc, probe);

  PaxChunkScanIterator it(&chunk, sc);
  size_t seen = 0;
  for (; it.IsValid() && seen < 200; ++it, ++seen) {
    (void)it.TryValue();
    (void)it.ValueRef();
  }
  std::vector<slot_t> proj{1};
  PaxChunkScanIterator pit(&chunk, sc);
  pit.Project(proj);
  for (size_t i = 0; i < 50 && pit.IsValid(); ++pit, ++i) (void)pit.ValueRef();

  return 0;
}

}  // namespace tinylamb::fuzz

#ifndef TINYLAMB_FUZZ_NO_MAIN_SYMBOL
extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  return tinylamb::fuzz::FuzzChunkFooterOneInput(data, size);
}
#endif
