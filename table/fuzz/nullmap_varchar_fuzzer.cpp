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

// Null-bitmap and VARCHAR-region corruption fuzz.
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <optional>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "table/fuzz/fuzz_chunk_common.hpp"
#include "table/fuzz/fuzz_helpers.hpp"
#include "table/fuzz/fuzz_str_cases.hpp"
#include "table/pax_chunk/pax_chunk.hpp"
#include "type/schema.hpp"

namespace tinylamb::fuzz {
namespace {

bool TryShape(const Schema& sc, const StrCase& c, uint8_t shape,
              PaxChunk* out) {
  setenv("TINYLAMB_FORCE_SCHEME", c.name, 1);
  PaxChunk chunk =
      PaxChunk::BuildFromSortedRun(MakeRun(MakeStorageRows(shape), sc), sc);
  unsetenv("TINYLAMB_FORCE_SCHEME");
  const auto& enc = chunk.GetFooter().encodings;
  if (enc.size() > c.col && enc[c.col] == static_cast<uint8_t>(c.expected)) {
    *out = std::move(chunk);
    return true;
  }
  return false;
}

PaxChunk BuildStrForced(const Schema& sc, const StrCase& c, bool* ok) {
  *ok = false;
  static std::array<int, kStrCaseCount> chosen;
  static bool init = false;
  if (!init) {
    chosen.fill(-1);
    init = true;
  }
  const auto idx = static_cast<size_t>(&c - kStrCases);
  int& slot = chosen[idx];
  const int first = slot >= 0 ? slot : 0;
  for (int s = first; std::cmp_less(s, c.nshapes); ++s) {
    PaxChunk chunk;
    if (TryShape(sc, c, c.shapes[s], &chunk)) {
      slot = s;
      *ok = true;
      return chunk;
    }
    if (slot < 0 && s == 0) {
      slot = 0;
    }
  }
  slot = static_cast<int>(c.nshapes);
  return PaxChunk{};
}

bool NullBitmapRange(const std::vector<char>& b, size_t* begin, size_t* len) {
  size_t fo = 0;
  size_t fs = 0;
  if (!WireLayout(b, &fo, &fs)) {
    return false;
  }
  auto rd = [&](size_t off, int n) -> uint64_t {
    uint64_t v = 0;
    for (int i = 0; i < n; ++i) {
      v = (v << 8) |
          static_cast<unsigned char>(b[off + static_cast<size_t>(i)]);
    }
    return v;
  };
  const uint64_t rg = rd(fo + 12, 4);
  const uint64_t cc = rd(fo + 16, 2);
  if (rg == 0 || rg > 4096 || cc == 0 || cc > 4096) {
    return false;
  }
  const uint64_t ncell = rg * cc;
  size_t pos =
      30 + (static_cast<size_t>(rg) * 2) + (static_cast<size_t>(ncell) * 4);
  if (pos > fs) {
    return false;
  }
  if (rd(fo + 21, 1) != 0) {
    pos += static_cast<size_t>(rg) * 16;
  }
  pos += static_cast<size_t>(ncell) + (static_cast<size_t>(ncell) * 4);
  if (pos + 1 > fs) {
    return false;
  }
  auto flag = [&](size_t p) -> int {
    return static_cast<unsigned char>(b[fo + p]);
  };
  if (flag(pos) == 1) {
    pos += 1 + (static_cast<size_t>(cc) * 17);
  } else {
    ++pos;
  }
  if (pos < fs && flag(pos) == 1) {
    pos += 1 + (static_cast<size_t>(ncell) * 16);
  } else if (pos < fs) {
    ++pos;
  }
  if (pos < fs && flag(pos) == 1) {
    pos += 1 + (static_cast<size_t>(ncell) * 8);
  } else if (pos < fs) {
    ++pos;
  }
  if (pos < fs && flag(pos) == 1) {
    pos += 1 + (static_cast<size_t>(ncell) * 8);
  } else if (pos < fs) {
    ++pos;
  }
  if (pos < fs && flag(pos) == 1) {
    pos += 1 + (static_cast<size_t>(ncell) * 64);
  } else if (pos < fs) {
    ++pos;
  }
  if (pos >= fs) {
    return false;
  }
  if (flag(pos) == 1) {
    ++pos;
    if (pos >= fs) {
      return false;
    }
    const uint64_t log2 = rd(fo + pos, 1);
    if (log2 > 20) {
      return false;
    }
    pos += 1 + ((size_t{1} << log2) * 12);
  } else {
    ++pos;
  }
  if (pos + 1 > fs) {
    return false;
  }
  if (flag(pos) != 1) {
    return false;
  }
  ++pos;
  size_t bytes_len = 0;
  for (uint64_t g = 0; g < rg; ++g) {
    const uint64_t rc = rd(fo + 30 + (static_cast<size_t>(g) * 2), 2);
    bytes_len += static_cast<size_t>(cc) * ((rc + 7) / 8);
  }
  if (pos + bytes_len > fs) {
    return false;
  }
  *begin = fo + pos;
  *len = bytes_len;
  return true;
}

bool NullBitmapCell(const PaxChunk::Footer& f, uint32_t g, slot_t c,
                    const uint8_t** out, size_t* len) {
  if (!f.null_present || g >= f.row_group_count || c >= f.column_count) {
    return false;
  }
  const size_t cc = f.column_count;
  size_t off = 0;
  for (uint32_t gg = 0; gg < g; ++gg) {
    if (gg >= f.row_counts.size()) {
      return false;
    }
    off += cc * ((static_cast<size_t>(f.row_counts[gg]) + 7) / 8);
  }
  if (g >= f.row_counts.size()) {
    return false;
  }
  const size_t cell = (static_cast<size_t>(f.row_counts[g]) + 7) / 8;
  off += static_cast<size_t>(c) * cell;
  if (cell == 0 || off + cell > f.null_bitmaps.size()) {
    return false;
  }
  *out = f.null_bitmaps.data() + off;
  *len = cell;
  return true;
}

void RunNullAwareKernels(const PaxChunk& chunk, const PaxChunk::Footer& f,
                         int64_t lit) {
  const std::vector<char>& data = chunk.Data();
  size_t fo = 0;
  size_t fs = 0;
  if (!WireLayout(data, &fo, &fs)) {
    return;
  }
  const size_t data_end = data.size() - fs - 4;
  for (uint32_t g = 0; g < f.row_group_count; ++g) {
    for (slot_t c = 0; c < f.column_count; ++c) {
      const size_t cell = (static_cast<size_t>(g) * f.column_count) + c;
      if (cell >= f.encodings.size() || cell >= f.column_offsets.size()) {
        continue;
      }
      const uint8_t scheme = f.encodings[cell];
      if (scheme != static_cast<uint8_t>(PaxChunk::Scheme::kByteSlice) &&
          scheme != static_cast<uint8_t>(PaxChunk::Scheme::kPBitS)) {
        continue;
      }
      const size_t off = f.column_offsets[cell];
      const size_t succ = (cell + 1 < f.column_offsets.size())
                              ? f.column_offsets[cell + 1]
                              : data_end;
      if (off > succ || succ > data_end) {
        continue;
      }
      const char* region = data.data() + off;
      const char* region_end = data.data() + succ;
      if (g >= f.row_counts.size()) {
        continue;
      }
      const uint32_t n = f.row_counts[g];
      if (n == 0) {
        continue;
      }
      const uint8_t* bmp = nullptr;
      size_t bmp_len = 0;
      const bool have_bmp = NullBitmapCell(f, g, c, &bmp, &bmp_len);
      const uint32_t param = cell < f.params.size() ? f.params[cell] : 0;
      const bool byteslice =
          scheme == static_cast<uint8_t>(PaxChunk::Scheme::kByteSlice);
      std::vector<uint64_t> mask(((n + 63) / 64) + 2, 0);
      for (auto op : {PaxChunk::CompareOp::kEq, PaxChunk::CompareOp::kLe,
                      PaxChunk::CompareOp::kLt, PaxChunk::CompareOp::kGe,
                      PaxChunk::CompareOp::kGt}) {
        if (byteslice) {
          PaxChunk::ByteSliceCompareMask(region, n, op, lit, bmp, mask.data());
          (void)PaxChunk::ByteSliceFilteredSum(region, n, bmp, mask.data());
        } else {
          PaxChunk::PBitSCompareMask(region, param, n, region_end, op, lit, bmp,
                                     mask.data());
          (void)PaxChunk::PBitSFilteredSum(region, param, n, region_end, bmp,
                                           mask.data());
        }
      }
      if (byteslice && have_bmp) {
        for (auto op : {PaxChunk::CompareOp::kEq, PaxChunk::CompareOp::kLe,
                        PaxChunk::CompareOp::kLt, PaxChunk::CompareOp::kGe,
                        PaxChunk::CompareOp::kGt}) {
          (void)PaxChunk::ByteSliceNullAwareCount(
              region, PaxChunk::Scheme::kByteSlice, param, n, bmp, op, lit);
        }
        (void)bmp_len;
      }
      if (!byteslice && have_bmp) {
        for (auto op : {PaxChunk::CompareOp::kEq, PaxChunk::CompareOp::kLe,
                        PaxChunk::CompareOp::kLt, PaxChunk::CompareOp::kGe,
                        PaxChunk::CompareOp::kGt}) {
          (void)PaxChunk::PBitSNullAwareCount(region, param, n, region_end, bmp,
                                              op, lit);
        }
      }
    }
  }
}

}  // namespace

int FuzzNullmapVarcharOneInput(const uint8_t* data, size_t size) {
  if (size < 24) {
    return 0;
  }

  const StrCase& c = kStrCases[data[0] % kStrCaseCount];
  const bool resign = (data[2] & 0x1) != 0;
  const auto op_mask = static_cast<uint8_t>((data[2] >> 1) & 0x3);
  const uint8_t n_sites = 1 + (data[3] % 8);
  int64_t probe = 0;
  std::memcpy(&probe, data + size - 8, 8);

  const Schema sc = MakeStorageSchema();
  bool built = false;
  const PaxChunk good = BuildStrForced(sc, c, &built);
  if (!built) {
    return 0;
  }

  std::vector<char> bytes = good.Data();
  if (bytes.size() < 64) {
    return 0;
  }

  size_t vb = 0;
  size_t ve = 0;
  size_t fo = 0;
  size_t fs = 0;
  if (WireLayout(bytes, &fo, &fs) &&
      good.GetFooter().column_offsets.size() > static_cast<size_t>(c.col) + 1) {
    const size_t off =
        good.GetFooter().column_offsets[static_cast<size_t>(c.col)];
    const size_t succ =
        good.GetFooter().column_offsets[static_cast<size_t>(c.col) + 1];
    if (off <= succ && succ <= fo) {
      vb = off;
      ve = succ;
    }
  }
  size_t nb = 0;
  size_t nl = 0;
  const bool have_nulls = NullBitmapRange(bytes, &nb, &nl);
  const bool have_forced = ve > vb;

  const size_t trailer_lo = bytes.size() - 4;
  size_t p = 4;
  for (uint8_t k = 0; k < n_sites; ++k) {
    if (p + 4 > size - 8) {
      break;
    }
    const uint32_t roll = static_cast<uint32_t>(data[p]) |
                          (static_cast<uint32_t>(data[p + 1]) << 8) |
                          (static_cast<uint32_t>(data[p + 2]) << 16) |
                          (static_cast<uint32_t>(data[p + 3]) << 24);
    p += 4;
    size_t site = 0;
    if (have_nulls && (roll & 1) == 0) {
      site = nb + ((roll >> 1) % nl);
    } else if (have_forced && (roll & 1) == 1) {
      site = vb + ((roll >> 1) % (ve - vb));
    } else {
      site = (roll >> 3) % bytes.size();
    }
    ApplyMutation(bytes, site, trailer_lo, op_mask, k + 1, roll,
                  static_cast<uint8_t>(((roll & 0x1) != 0U) ? 0xFF : 0x00));
  }

  if (resign) {
    ResignChunkCrc(bytes);
  }

  const Status v1 = PaxChunk::ValidateChunkBytes(bytes);
  if (v1 != PaxChunk::ValidateChunkBytes(bytes)) {
    std::cerr << "nondeterministic ValidateChunkBytes verdict\n";
    std::abort();
  }
  if (v1 != Status::kSuccess) {
    return 0;
  }

  auto chunk_or = PaxChunk::Deserialize(bytes);
  if (!chunk_or.HasValue()) {
    std::cerr << "ValidateChunkBytes accepted but Deserialize rejected\n";
    std::abort();
  }
  const PaxChunk& chunk = chunk_or.Value();

  HammerChunk(chunk, sc, probe);
  RunNullAwareKernels(chunk, chunk.GetFooter(), probe);

  for (slot_t col = 0; col < sc.ColumnCount(); ++col) {
    (void)VarcharFilterCount(chunk, col, StrScanOp::kEq, "name-7", &sc);
    (void)VarcharFilterCount(chunk, col, StrScanOp::kLt, "name-30", &sc);
    (void)VarcharRangeCount(chunk, col, std::nullopt, "name-30", &sc);
    (void)VarcharRangeCount(chunk, col, "name-1", "name-40", &sc);
  }

  PaxChunkScanIterator it(&chunk, sc);
  size_t seen = 0;
  for (; it.IsValid() && seen < 400; ++it, ++seen) {
    (void)it.TryValue();
    (void)it.ValueRef();
  }
  std::vector<slot_t> proj{c.col, 0};
  PaxChunkScanIterator pit(&chunk, sc);
  pit.Project(proj);
  for (size_t i = 0; i < 100 && pit.IsValid(); ++pit, ++i) {
    (void)pit.ValueRef();
  }
  PaxChunkScanIterator sit(&chunk, sc);
  sit.Seek(chunk.TotalRowCount() / 2);
  for (size_t i = 0; i < 100 && sit.IsValid(); ++sit, ++i) {
    (void)sit.ValueRef();
  }

  return 0;
}

}  // namespace tinylamb::fuzz

#ifndef TINYLAMB_FUZZ_NO_MAIN_SYMBOL
extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  return tinylamb::fuzz::FuzzNullmapVarcharOneInput(data, size);
}
#endif
