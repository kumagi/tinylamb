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

#ifndef TINYLAMB_TABLE_FUZZ_STR_CASES_HPP
#define TINYLAMB_TABLE_FUZZ_STR_CASES_HPP

// The forced-scheme case table shared by nullmap_varchar_fuzzer.cpp and the
// seed generator.
#include <cstddef>
#include <cstdint>

#include "common/constants.hpp"
#include "table/pax_chunk/pax_chunk.hpp"

namespace tinylamb::fuzz {

// Each case forces one scheme and tells the harness which column of
// MakeStorageSchema() the force lands on.
struct StrCase {
  const char* name;
  PaxChunk::Scheme expected;
  slot_t col;
  uint8_t shapes[4];
  uint8_t nshapes;
};

// Every shape listed for a case must keep bit 2 set (the null-preserving bit
// of MakeStorageRows) so the chunk always carries a null-bitmap section.
inline constexpr StrCase kStrCases[] = {
    // ---- Varchar families (col 2) ----
    {"FSST", PaxChunk::Scheme::kFsst, 2, {4, 5, 12, 13}, 4},
    {"FRONTCODE", PaxChunk::Scheme::kFrontCode, 2, {4, 5, 12, 13}, 4},
    {"DICT", PaxChunk::Scheme::kDict, 2, {4, 5, 12, 13}, 4},
    {"OPDICT", PaxChunk::Scheme::kOpDict, 2, {4, 5, 12, 13}, 4},
    {"DICTX", PaxChunk::Scheme::kDictX, 2, {4, 5, 12, 13}, 4},
    {"PLAIN", PaxChunk::Scheme::kPlain, 2, {4, 5, 12, 13}, 4},
    // ---- Null-aware BSI / sparse families (col 1, the nullable int64) ----
    {"BYTESLICE", PaxChunk::Scheme::kByteSlice, 1, {4, 5, 12, 13}, 4},
    {"PBITS", PaxChunk::Scheme::kPBitS, 1, {4, 5, 12, 13}, 4},
    {"SPARSE", PaxChunk::Scheme::kSparse, 1, {4, 12, 5, 13}, 4},
    // ---- Composite / inner-region families ----
    {"ARITH", PaxChunk::Scheme::kArith, 0, {4, 5, 6, 7}, 4},
    {"PLA", PaxChunk::Scheme::kPla, 1, {4, 5, 12, 13}, 4},
    {"ANSFOR", PaxChunk::Scheme::kAnsFor, 1, {4, 5, 12, 13}, 4},
    {"OPRLE", PaxChunk::Scheme::kOpRle, 4, {4, 6, 12, 14}, 4},
    {"RLEX", PaxChunk::Scheme::kRleX, 4, {4, 6, 12, 14}, 4},
    {"FREQ", PaxChunk::Scheme::kFreq, 4, {6, 4, 14, 12}, 4},
    // ---- Checkpointed float bitstreams (col 3) ----
    {"XORFORC", PaxChunk::Scheme::kXorForC, 3, {4, 5, 6, 7}, 4},
    {"GORILLAC", PaxChunk::Scheme::kGorillaC, 3, {4, 5, 6, 7}, 4},
    {"CHIMPC", PaxChunk::Scheme::kChimpC, 3, {4, 5, 6, 7}, 4},
    {"ALP", PaxChunk::Scheme::kAlp, 3, {4, 5, 6, 7}, 4},
    // ---- Other newer int64 layouts ----
    {"SIMPLE8B", PaxChunk::Scheme::kSimple8b, 1, {4, 5, 12, 13}, 4},
    {"RLEIDX", PaxChunk::Scheme::kRleIdx, 0, {4, 5, 6, 7}, 4},
    {"INTERP", PaxChunk::Scheme::kInterp, 0, {4, 5, 6, 7}, 4},
    {"STREAMVBYTE", PaxChunk::Scheme::kStreamVByte, 1, {4, 5, 12, 13}, 4},
    {"D2PFOR", PaxChunk::Scheme::kD2Pfor, 0, {4, 5, 6, 7}, 4},
    {"HUFFDICT", PaxChunk::Scheme::kHuffDict, 4, {4, 6, 12, 14}, 4},
    {"SIMDFOR", PaxChunk::Scheme::kSimdFor, 1, {4, 5, 12, 13}, 4},
    {"DELTASIMD", PaxChunk::Scheme::kDeltaSimd, 0, {4, 5, 6, 7}, 4},
    {"S4", PaxChunk::Scheme::kS4, 0, {4, 5, 6, 7}, 4},
    {"TOPK", PaxChunk::Scheme::kTopk, 1, {20, 21, 28, 29}, 4},
    {"SFOR", PaxChunk::Scheme::kSfor, 0, {4, 5, 6, 7}, 4},
    {"MOD", PaxChunk::Scheme::kMod, 1, {36, 37, 44, 45}, 4},
    {"TILEDICT", PaxChunk::Scheme::kTileDict, 1, {68, 69, 76, 77}, 4},
    {"EXPFOR", PaxChunk::Scheme::kExpFor, 0, {4, 5, 6, 7}, 4},
    {"SORTDICT", PaxChunk::Scheme::kSortDict, 4, {4, 6, 12, 14}, 4},
    {"LANES", PaxChunk::Scheme::kLane, 0, {4, 5, 6, 7}, 4},
    {"POSTINGS", PaxChunk::Scheme::kPostings, 4, {4, 6, 12, 14}, 4},
    {"TEMPLATE", PaxChunk::Scheme::kTemplate, 2, {4, 5, 12, 13}, 4},
    {"DSTREAM", PaxChunk::Scheme::kDStream, 0, {4, 5, 6, 7}, 4},
};

inline constexpr size_t kStrCaseCount =
    sizeof(kStrCases) / sizeof(kStrCases[0]);

}  // namespace tinylamb::fuzz

#endif  // TINYLAMB_TABLE_FUZZ_STR_CASES_HPP
