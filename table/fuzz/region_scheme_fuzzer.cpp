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

// Per-region corruption fuzz for the scheme families.
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

#include "common/constants.hpp"
#include "table/fuzz/fuzz_chunk_common.hpp"
#include "table/fuzz/fuzz_helpers.hpp"
#include "table/fuzz/fuzz_scheme_cases.hpp"
#include "table/pax_chunk/pax_chunk.hpp"
#include "type/schema.hpp"

namespace tinylamb::fuzz {
namespace {

constexpr const SchemeCase* const kCases = kSchemeCases;
constexpr size_t kCaseCount = kSchemeCaseCount;

PaxChunk BuildForced(const Schema& sc, const SchemeCase& c, bool* ok) {
  *ok = false;
  static int cache[kCaseCount];
  static bool cache_init = false;
  if (!cache_init) {
    for (size_t i = 0; i < kCaseCount; ++i) cache[i] = -1;
    cache_init = true;
  }
  const auto idx = static_cast<size_t>(&c - kCases);
  int& slot = cache[idx];
  const int first = slot >= 0 ? slot : 0;
  for (int s = first; s < static_cast<int>(c.nshapes); ++s) {
    setenv("TINYLAMB_FORCE_SCHEME", c.name, 1);
    PaxChunk chunk = PaxChunk::BuildFromSortedRun(
        MakeRun(MakeStorageRows(c.shapes[s]), sc), sc);
    unsetenv("TINYLAMB_FORCE_SCHEME");
    const auto& enc = chunk.GetFooter().encodings;
    if (!enc.empty() && enc[c.col] == static_cast<uint8_t>(c.expected)) {
      slot = s;
      *ok = true;
      return chunk;
    }
    if (slot < 0 && s == 0) slot = 0;
  }
  slot = static_cast<int>(c.nshapes);
  return PaxChunk{};
}

bool CellRange(const std::vector<char>& b, const PaxChunk::Footer& f,
               size_t cell, size_t* begin, size_t* end) {
  size_t fo = 0;
  size_t fs = 0;
  if (!WireLayout(b, &fo, &fs)) return false;
  if (cell >= f.column_offsets.size()) return false;
  const size_t data_end = b.size() - fs - 4;
  const size_t off = f.column_offsets[cell];
  const size_t succ = (cell + 1 < f.column_offsets.size())
                          ? f.column_offsets[cell + 1]
                          : data_end;
  if (off > succ || succ > data_end) return false;
  *begin = off;
  *end = succ;
  return true;
}

}  // namespace

int FuzzRegionSchemeOneInput(const uint8_t* data, size_t size) {
  if (size < 24) return 0;

  const SchemeCase& c = kCases[data[0] % kCaseCount];
  const size_t other_col = data[1] % 5;
  const bool resign = (data[2] & 0x1) != 0;
  const auto op_mask = static_cast<uint8_t>((data[2] >> 1) & 0x3);
  const uint8_t n_sites = 1 + (data[3] % 8);
  int64_t probe = 0;
  std::memcpy(&probe, data + size - 8, 8);

  const Schema sc = MakeStorageSchema();
  bool built = false;
  const PaxChunk good = BuildForced(sc, c, &built);
  if (!built) return 0;

  std::vector<char> bytes = good.Data();
  if (bytes.size() < 64) return 0;

  size_t tb = 0;
  size_t te = 0;
  const bool have_target =
      CellRange(bytes, good.GetFooter(), c.col, &tb, &te) && te > tb;
  size_t ob = 0;
  size_t oe = 0;
  const bool have_other =
      CellRange(bytes, good.GetFooter(), other_col, &ob, &oe) && oe > ob;

  const size_t trailer_lo = bytes.size() - 4;
  size_t p = 4;
  for (uint8_t k = 0; k < n_sites; ++k) {
    if (p + 4 > size - 8) break;
    const uint32_t roll = static_cast<uint32_t>(data[p]) |
                          (static_cast<uint32_t>(data[p + 1]) << 8) |
                          (static_cast<uint32_t>(data[p + 2]) << 16) |
                          (static_cast<uint32_t>(data[p + 3]) << 24);
    p += 4;
    size_t site = 0;
    if ((roll & 3) == 3 && have_other) {
      site = ob + (roll >> 2) % (oe - ob);
    } else if ((roll & 0x1F) == 0x1F) {
      size_t fo = 0;
      size_t fs = 0;
      if (!WireLayout(bytes, &fo, &fs)) break;
      site = (roll >> 5) % (fo > 0 ? fo : 1);
    } else if (have_target) {
      site = tb + (roll >> 2) % (te - tb);
    } else {
      site = (roll >> 2) % bytes.size();
    }
    ApplyMutation(bytes, site, trailer_lo, op_mask, k + 1, roll,
                  static_cast<uint8_t>(1 + ((roll >> 28) & 0x7)));
  }

  if (resign) ResignChunkCrc(bytes);

  const Status v1 = PaxChunk::ValidateChunkBytes(bytes);
  if (v1 != PaxChunk::ValidateChunkBytes(bytes)) {
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
  HammerChunk(chunk_or.Value(), sc, probe);
  return 0;
}

}  // namespace tinylamb::fuzz

#ifndef TINYLAMB_FUZZ_NO_MAIN_SYMBOL
extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  return tinylamb::fuzz::FuzzRegionSchemeOneInput(data, size);
}
#endif
