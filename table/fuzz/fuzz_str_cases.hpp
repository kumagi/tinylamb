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
#include <array>
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
  std::array<uint8_t, 4> shapes{};
  uint8_t nshapes;
};

// Every shape listed for a case must keep bit 2 set (the null-preserving bit
// of MakeStorageRows) so the chunk always carries a null-bitmap section.
inline constexpr auto kStrCases = std::to_array<StrCase>({
    // ---- Varchar families (col 2) ----
    {.name = "FSST",
     .expected = PaxChunk::Scheme::kFsst,
     .col = 2,
     .shapes = {4, 5, 12, 13},
     .nshapes = 4},
    {.name = "FRONTCODE",
     .expected = PaxChunk::Scheme::kFrontCode,
     .col = 2,
     .shapes = {4, 5, 12, 13},
     .nshapes = 4},
    {.name = "DICT",
     .expected = PaxChunk::Scheme::kDict,
     .col = 2,
     .shapes = {4, 5, 12, 13},
     .nshapes = 4},
    {.name = "OPDICT",
     .expected = PaxChunk::Scheme::kOpDict,
     .col = 2,
     .shapes = {4, 5, 12, 13},
     .nshapes = 4},
    {.name = "DICTX",
     .expected = PaxChunk::Scheme::kDictX,
     .col = 2,
     .shapes = {4, 5, 12, 13},
     .nshapes = 4},
    {.name = "PLAIN",
     .expected = PaxChunk::Scheme::kPlain,
     .col = 2,
     .shapes = {4, 5, 12, 13},
     .nshapes = 4},
    // ---- Null-aware BSI / sparse families (col 1, the nullable int64) ----
    {.name = "BYTESLICE",
     .expected = PaxChunk::Scheme::kByteSlice,
     .col = 1,
     .shapes = {4, 5, 12, 13},
     .nshapes = 4},
    {.name = "PBITS",
     .expected = PaxChunk::Scheme::kPBitS,
     .col = 1,
     .shapes = {4, 5, 12, 13},
     .nshapes = 4},
    {.name = "SPARSE",
     .expected = PaxChunk::Scheme::kSparse,
     .col = 1,
     .shapes = {4, 12, 5, 13},
     .nshapes = 4},
    // ---- Composite / inner-region families ----
    {.name = "ARITH",
     .expected = PaxChunk::Scheme::kArith,
     .col = 0,
     .shapes = {4, 5, 6, 7},
     .nshapes = 4},
    {.name = "PLA",
     .expected = PaxChunk::Scheme::kPla,
     .col = 1,
     .shapes = {4, 5, 12, 13},
     .nshapes = 4},
    {.name = "ANSFOR",
     .expected = PaxChunk::Scheme::kAnsFor,
     .col = 1,
     .shapes = {4, 5, 12, 13},
     .nshapes = 4},
    {.name = "OPRLE",
     .expected = PaxChunk::Scheme::kOpRle,
     .col = 4,
     .shapes = {4, 6, 12, 14},
     .nshapes = 4},
    {.name = "RLEX",
     .expected = PaxChunk::Scheme::kRleX,
     .col = 4,
     .shapes = {4, 6, 12, 14},
     .nshapes = 4},
    {.name = "FREQ",
     .expected = PaxChunk::Scheme::kFreq,
     .col = 4,
     .shapes = {6, 4, 14, 12},
     .nshapes = 4},
    // ---- Checkpointed float bitstreams (col 3) ----
    {.name = "XORFORC",
     .expected = PaxChunk::Scheme::kXorForC,
     .col = 3,
     .shapes = {4, 5, 6, 7},
     .nshapes = 4},
    {.name = "GORILLAC",
     .expected = PaxChunk::Scheme::kGorillaC,
     .col = 3,
     .shapes = {4, 5, 6, 7},
     .nshapes = 4},
    {.name = "CHIMPC",
     .expected = PaxChunk::Scheme::kChimpC,
     .col = 3,
     .shapes = {4, 5, 6, 7},
     .nshapes = 4},
    {.name = "ALP",
     .expected = PaxChunk::Scheme::kAlp,
     .col = 3,
     .shapes = {4, 5, 6, 7},
     .nshapes = 4},
    // ---- Other newer int64 layouts ----
    {.name = "SIMPLE8B",
     .expected = PaxChunk::Scheme::kSimple8b,
     .col = 1,
     .shapes = {4, 5, 12, 13},
     .nshapes = 4},
    {.name = "RLEIDX",
     .expected = PaxChunk::Scheme::kRleIdx,
     .col = 0,
     .shapes = {4, 5, 6, 7},
     .nshapes = 4},
    {.name = "INTERP",
     .expected = PaxChunk::Scheme::kInterp,
     .col = 0,
     .shapes = {4, 5, 6, 7},
     .nshapes = 4},
    {.name = "STREAMVBYTE",
     .expected = PaxChunk::Scheme::kStreamVByte,
     .col = 1,
     .shapes = {4, 5, 12, 13},
     .nshapes = 4},
    {.name = "D2PFOR",
     .expected = PaxChunk::Scheme::kD2Pfor,
     .col = 0,
     .shapes = {4, 5, 6, 7},
     .nshapes = 4},
    {.name = "HUFFDICT",
     .expected = PaxChunk::Scheme::kHuffDict,
     .col = 4,
     .shapes = {4, 6, 12, 14},
     .nshapes = 4},
    {.name = "SIMDFOR",
     .expected = PaxChunk::Scheme::kSimdFor,
     .col = 1,
     .shapes = {4, 5, 12, 13},
     .nshapes = 4},
    {.name = "DELTASIMD",
     .expected = PaxChunk::Scheme::kDeltaSimd,
     .col = 0,
     .shapes = {4, 5, 6, 7},
     .nshapes = 4},
    {.name = "S4",
     .expected = PaxChunk::Scheme::kS4,
     .col = 0,
     .shapes = {4, 5, 6, 7},
     .nshapes = 4},
    {.name = "TOPK",
     .expected = PaxChunk::Scheme::kTopk,
     .col = 1,
     .shapes = {20, 21, 28, 29},
     .nshapes = 4},
    {.name = "SFOR",
     .expected = PaxChunk::Scheme::kSfor,
     .col = 0,
     .shapes = {4, 5, 6, 7},
     .nshapes = 4},
    {.name = "MOD",
     .expected = PaxChunk::Scheme::kMod,
     .col = 1,
     .shapes = {36, 37, 44, 45},
     .nshapes = 4},
    {.name = "TILEDICT",
     .expected = PaxChunk::Scheme::kTileDict,
     .col = 1,
     .shapes = {68, 69, 76, 77},
     .nshapes = 4},
    {.name = "EXPFOR",
     .expected = PaxChunk::Scheme::kExpFor,
     .col = 0,
     .shapes = {4, 5, 6, 7},
     .nshapes = 4},
    {.name = "SORTDICT",
     .expected = PaxChunk::Scheme::kSortDict,
     .col = 4,
     .shapes = {4, 6, 12, 14},
     .nshapes = 4},
    {.name = "LANES",
     .expected = PaxChunk::Scheme::kLane,
     .col = 0,
     .shapes = {4, 5, 6, 7},
     .nshapes = 4},
    {.name = "POSTINGS",
     .expected = PaxChunk::Scheme::kPostings,
     .col = 4,
     .shapes = {4, 6, 12, 14},
     .nshapes = 4},
    {.name = "TEMPLATE",
     .expected = PaxChunk::Scheme::kTemplate,
     .col = 2,
     .shapes = {4, 5, 12, 13},
     .nshapes = 4},
    {.name = "DSTREAM",
     .expected = PaxChunk::Scheme::kDStream,
     .col = 0,
     .shapes = {4, 5, 6, 7},
     .nshapes = 4},
});

inline constexpr size_t kStrCaseCount = kStrCases.size();

}  // namespace tinylamb::fuzz

#endif  // TINYLAMB_TABLE_FUZZ_STR_CASES_HPP
