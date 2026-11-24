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

// Encoder / validator AGREEMENT fuzzing: whatever EncodeColumnAdaptive emits
// must be a chunk ValidateChunkBytes and Deserialize accept, and reopen
// losslessly.
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/platform/process.hpp"
#include "common/constants.hpp"
#include "common/platform/endian.hpp"
#include "common/status_or.hpp"
#include "table/fuzz/fuzz_chunk_common.hpp"
#include "table/fuzz/fuzz_helpers.hpp"
#include "table/pax_chunk/pax_chunk.hpp"
#include "table/sorted_run/sorted_run.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::fuzz {
namespace {

constexpr std::array<const char*, 68> kForceNames = {
    "ARITH",     "SPARSE",      "ANSFOR",        "OPRLE",         "XORFORC",
    "GORILLAC",  "CHIMPC",      "PLA",           "INTERP",        "RLEIDX",
    "DELTAPFOR", "ELIASFANO",   "RLEBP",         "WAH",           "BITMAP",
    "SIMPLE8B",  "FREQ",        "RLEX",          "DICTX",         "ALP",
    "PBITS",     "BP128",       "QPP",           "RLE8",          "RLEDELTA",
    "WAVELETR",  "CHIMP",       "GORILLA",       "PATAS",         "RLE",
    "FORBP",     "DELTAFP",     "XORFOR",        "DELTABITWEAVE", "PFOR",
    "RLEDICT",   "DICT",        "OPDICT",        "STREAMVBYTE",   "D2PFOR",
    "HUFFDICT",  "BITWEAVE",    "WAVELET",       "DELTAFOR",      "BYTESLICE",
    "DDRICE",    "DELTAVARINT", "CHIMP128",      "ALPRDTANS",     "SIMDFOR",
    "DELTASIMD", "S4",          "TOPK",          "FSST",          "FRONTCODE",
    "PLAIN",     "CONST",       "SFOR",          "MOD",           "TILEDICT",
    "EXPFOR",    "SORTDICT",    "LANES",         "POSTINGS",      "TEMPLATE",
    "DSTREAM",   "FASTPFOR",    "DELTAFASTPFOR",
};
constexpr size_t kForceCount = kForceNames.size();

// T4270: save/restore the previous TINYLAMB_FORCE_SCHEME (ScopedEnvVar
// style) instead of blanket-unsetting, so nesting or an externally set
// scheme survives the guard. The harness is single-threaded by design
// (libFuzzer's -jobs/-workers use separate processes with isolated
// environments), so the process environment is deliberately global here;
// the getenv/setenv/unsetenv calls carry NOLINT(concurrency-mt-unsafe)
// annotations, matching the sibling fuzz harnesses.
class ForceGuard {
 public:
  explicit ForceGuard(const char* name) {
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    const char* prev = ::tinylamb::platform::GetEnv("TINYLAMB_FORCE_SCHEME");
    if (prev != nullptr) {
      had_prev_ = true;
      prev_ = prev;
    }
    if (name != nullptr) {
      ::tinylamb::platform::SetEnv("TINYLAMB_FORCE_SCHEME", name,
             1);  // NOLINT(concurrency-mt-unsafe)
    } else {
      ::tinylamb::platform::UnsetEnv("TINYLAMB_FORCE_SCHEME");  // NOLINT(concurrency-mt-unsafe)
    }
  }
  ~ForceGuard() {
    if (had_prev_) {
      ::tinylamb::platform::SetEnv("TINYLAMB_FORCE_SCHEME", prev_.c_str(),
             1);  // NOLINT(concurrency-mt-unsafe)
    } else {
      ::tinylamb::platform::UnsetEnv("TINYLAMB_FORCE_SCHEME");  // NOLINT(concurrency-mt-unsafe)
    }
  }
  ForceGuard(const ForceGuard&) = delete;
  ForceGuard& operator=(const ForceGuard&) = delete;
  ForceGuard(ForceGuard&&) = delete;
  ForceGuard& operator=(ForceGuard&&) = delete;

 private:
  bool had_prev_{false};
  std::string prev_;
};

std::vector<Row> MakeRows(const Schema& sc, uint8_t shape, uint32_t n,
                          uint8_t salt) {
  std::vector<Row> rows;
  rows.reserve(n);
  const bool narrow = (shape & 1) != 0;
  const bool two_valued = (shape & 2) != 0;
  const bool ramp = (shape & 4) != 0;
  const bool keep_nulls = (shape & 8) != 0;
  const bool const_col = (shape & 16) != 0;
  const bool skewed = (shape & 32) != 0;
  const bool tile_local = (shape & 64) != 0;
  for (uint32_t i = 0; i < n; ++i) {
    std::vector<Value> vals;
    vals.reserve(sc.ColumnCount());
    // Salt from data[3] keeps rows sorted while making later input bytes
    // observable to the fuzzer (they previously did nothing).
    vals.emplace_back((static_cast<int64_t>(i) << 8) | salt);
    for (slot_t c = 1; c < sc.ColumnCount(); ++c) {
      switch (sc.GetColumn(c).Type()) {
        case ValueType::kVarChar: {
          vals.emplace_back("v" + std::to_string(i % (2 + (i % 17))));
          break;
        }
        case ValueType::kDouble: {
          vals.emplace_back(FuzzDoubleWithSpecials(
              static_cast<size_t>(i),
              100.25 + (static_cast<double>(i % 13) * 0.5)));
          break;
        }
        case ValueType::kInt64:
        case ValueType::kDate: {
          if (tile_local) {
            constexpr uint32_t kLocalD = 8;
            const uint64_t tile = static_cast<uint64_t>(i) / 64U;
            const uint64_t slot =
                (static_cast<uint64_t>(i) * 2654435761ULL + tile * 40503ULL) %
                kLocalD;
            vals.emplace_back(INT64_C(1000000000000) +
                              (static_cast<int64_t>(tile) * INT64_C(1048576)) +
                              (static_cast<int64_t>(slot) * 7919));
          } else if (skewed) {
            constexpr int64_t kSkewD = 64;
            static const std::vector<double> weights = [] {
              std::vector<double> v(static_cast<size_t>(kSkewD), 0.0);
              double t = 0.0;
              for (int64_t k = 1; k <= kSkewD; ++k) {
                v[static_cast<size_t>(k - 1)] =
                    1.0 / std::pow(static_cast<double>(k), 1.5);
                t += v[static_cast<size_t>(k - 1)];
              }
              for (double& x : v) {
                x /= t;
              }
              return v;
            }();
            double acc = 0.0;
            const double target =
                static_cast<double>((static_cast<uint64_t>(i) * 2654435761ULL) %
                                    10000ULL) /
                10000.0;
            for (int64_t k = 0; k < kSkewD; ++k) {
              acc += weights[static_cast<size_t>(k)];
              if (acc >= target) {
                vals.emplace_back(INT64_C(1000000000000) + (k * 7919));
                break;
              }
            }
          } else if (const_col) {
            vals.emplace_back(int64_t{7});
          } else if (two_valued) {
            vals.emplace_back(static_cast<int64_t>(i & 1));
          } else if (narrow) {
            vals.emplace_back(static_cast<int64_t>(2000 + (i % 9)));
          } else if (ramp) {
            vals.emplace_back((static_cast<int64_t>(i) * 3) + 1);
          } else {
            vals.emplace_back((static_cast<int64_t>(i) * 7919) + 13);
          }
          break;
        }
        default:
          break;
      }
    }
    rows.emplace_back(std::move(vals));
  }
  if (keep_nulls) {
    for (uint32_t i = 1; i < n; i += 7) {
      if (rows[i].Count() > 1) {
        rows[i][1] = Value();
      }
    }
  }
  return rows;
}

}  // namespace

int FuzzEncodeAgreeOneInput(const uint8_t* data, size_t size) {
  if (size < 24) {
    return 0;
  }

  const size_t force_idx = data[0] % (kForceCount + 1);
  const bool adaptive = force_idx == kForceCount;
  const char* force = adaptive ? nullptr : kForceNames[force_idx];
  const auto shape = static_cast<uint8_t>(data[1] & 127);
  static constexpr std::array<uint32_t, 17> kCounts = {
      1,   2,   7,   8,   9,   63,   64,   65,  127,
      128, 129, 255, 256, 257, 1023, 1024, 1025};
  const uint32_t n = kCounts[data[2] % kCounts.size()];

  const Schema sc = MakeStorageSchema();
  const std::vector<Row> rows = MakeRows(sc, shape, n, data[3]);
  PaxChunk chunk;
  {
    ForceGuard guard(force);
    chunk = PaxChunk::BuildFromSortedRun(MakeRun(rows, sc), sc).MoveValue();
  }

  const Status v = PaxChunk::ValidateChunkBytes(chunk.Data());
  if (v != Status::kSuccess) {
    std::string schemes;
    for (unsigned char encoding : chunk.GetFooter().encodings) {
      schemes += " " + std::to_string(encoding);
    }
    std::cerr << "ENCODE/VALIDATE DISAGREEMENT: force="
              << (adaptive ? "(adaptive)" : force)
              << " shape=" << static_cast<unsigned>(shape) << " n=" << n
              << " validate=" << static_cast<int>(v.GetCode())
              << " schemes:" << schemes << "\n";
    std::abort();
  }

  const StatusOr<PaxChunk> reopened = PaxChunk::Deserialize(chunk.Data());
  if (!reopened.HasValue()) {
    std::cerr << "ENCODE/VALIDATE DISAGREEMENT: accepted but Deserialize "
                 "rejected (force="
              << (adaptive ? "(adaptive)" : force) << " n=" << n << ")\n";
    std::abort();
  }
  if (reopened.Value().TotalRowCount() != n) {
    std::cerr << "row count lost across a reopen: "
              << reopened.Value().TotalRowCount() << " != " << n << "\n";
    std::abort();
  }
  for (size_t i = 0; i < n; ++i) {
    for (slot_t c = 0; c < sc.ColumnCount(); ++c) {
      // Compare the freshly encoded chunk against the ORIGINAL rows: comparing
      // only the two decoded copies would let a systematic encoder/decoder
      // error pass the round-trip oracle.
      const StatusOr<Value> fresh = chunk.ReadColumnValue(i, c, sc);
      if (!fresh.HasValue() || fresh.Value() != rows[i][c]) {
        std::cerr << "ENCODE/SOURCE MISMATCH: row=" << i << " col=" << c
                  << " force=" << (adaptive ? "(adaptive)" : force) << "\n";
        std::abort();
      }
      const StatusOr<Value> a = chunk.ReadColumnValue(i, c, sc);
      const StatusOr<Value> b = reopened.Value().ReadColumnValue(i, c, sc);
      if (!a.HasValue() || !b.HasValue() || a.Value() != b.Value()) {
        std::cerr << "value mismatch across a reopen: row=" << i << " col=" << c
                  << " force=" << (adaptive ? "(adaptive)" : force) << "\n";
        std::abort();
      }
    }
  }
  return 0;
}

}  // namespace tinylamb::fuzz

#ifndef TINYLAMB_FUZZ_NO_MAIN_SYMBOL
extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  return tinylamb::fuzz::FuzzEncodeAgreeOneInput(data, size);
}
#endif
