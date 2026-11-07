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

#ifndef TINYLAMB_TABLE_FUZZ_SCHEME_CASES_HPP
#define TINYLAMB_TABLE_FUZZ_SCHEME_CASES_HPP

// The forced-scheme case table shared by region_scheme_fuzzer.cpp and the
// seed generator.
#include <cstddef>
#include <cstdint>

#include "common/constants.hpp"
#include "table/pax_chunk/pax_chunk.hpp"

namespace tinylamb::fuzz {

// Each case forces one scheme and tells the harness which column of
// MakeStorageSchema() the force lands on.
struct SchemeCase {
  const char* name;
  PaxChunk::Scheme expected;
  slot_t col;
  uint8_t shapes[4];
  uint8_t nshapes;
};

// Every scheme family whose region has a non-trivial internal layout (a header
// of counts/offsets/bitoffs, an exception list, a checkpoint table, an inner
// region, or a bitstream).
inline constexpr SchemeCase kSchemeCases[] = {
    // ---- 27..33: the newer families ----
    {"ARITH", PaxChunk::Scheme::kArith, 0, {0, 1, 2, 4}, 4},
    {"SPARSE", PaxChunk::Scheme::kSparse, 1, {4, 12, 5, 13}, 4},
    {"ANSFOR", PaxChunk::Scheme::kAnsFor, 1, {0, 1, 4, 8}, 4},
    {"OPRLE", PaxChunk::Scheme::kOpRle, 4, {0, 2, 4, 6}, 4},
    {"XORFORC", PaxChunk::Scheme::kXorForC, 3, {0, 1, 2, 4}, 4},
    {"GORILLAC", PaxChunk::Scheme::kGorillaC, 3, {0, 1, 2, 4}, 4},
    {"CHIMPC", PaxChunk::Scheme::kChimpC, 3, {0, 1, 2, 4}, 4},
    // ---- Post-33 composite / segmented / checkpointed int64 families ----
    {"PLA", PaxChunk::Scheme::kPla, 1, {4, 5, 12, 13}, 4},
    {"INTERP", PaxChunk::Scheme::kInterp, 0, {0, 1, 2, 4}, 4},
    {"RLEIDX", PaxChunk::Scheme::kRleIdx, 0, {0, 1, 2, 4}, 4},
    {"DELTAPFOR", PaxChunk::Scheme::kDeltaPfor, 0, {0, 1, 2, 4}, 4},
    {"ELIASFano", PaxChunk::Scheme::kEliasFano, 0, {0, 1, 2, 4}, 4},
    {"RLEBP", PaxChunk::Scheme::kRleBp, 4, {0, 2, 4, 6}, 4},
    {"WAH", PaxChunk::Scheme::kWah, 4, {2, 3, 6, 7}, 4},
    {"BITMAP", PaxChunk::Scheme::kBitmap, 4, {2, 3, 6, 7}, 4},
    {"SIMPLE8B", PaxChunk::Scheme::kSimple8b, 1, {0, 1, 2, 4}, 4},
    {"FREQ", PaxChunk::Scheme::kFreq, 4, {2, 0, 4, 6}, 4},
    {"RLEX", PaxChunk::Scheme::kRleX, 4, {0, 2, 4, 6}, 4},
    {"DICTX", PaxChunk::Scheme::kDictX, 4, {0, 2, 4, 6}, 4},
    {"ALP", PaxChunk::Scheme::kAlp, 3, {0, 1, 2, 4}, 4},
    {"PBITS", PaxChunk::Scheme::kPBitS, 1, {0, 1, 4, 5}, 4},
    {"BP128", PaxChunk::Scheme::kBp128, 1, {0, 1, 2, 4}, 4},
    {"QPP", PaxChunk::Scheme::kQpp, 0, {0, 1, 2, 4}, 4},
    {"RLE8", PaxChunk::Scheme::kRle8, 4, {0, 2, 4, 6}, 4},
    {"RLEDELTA", PaxChunk::Scheme::kRleDelta, 0, {0, 1, 2, 4}, 4},
    {"WAVELETR", PaxChunk::Scheme::kWaveletR, 4, {0, 2, 4, 6}, 4},
    // ---- Float bitstreams ----
    {"CHIMP", PaxChunk::Scheme::kChimp, 3, {0, 1, 2, 4}, 4},
    {"GORILLA", PaxChunk::Scheme::kGorilla, 3, {0, 1, 2, 4}, 4},
    {"PATAS", PaxChunk::Scheme::kPatas, 3, {0, 1, 2, 4}, 4},
    // ---- Early families ----
    {"RLE", PaxChunk::Scheme::kRle, 4, {0, 2, 4, 6}, 4},
    {"FORBP", PaxChunk::Scheme::kForBp, 1, {0, 1, 2, 4}, 4},
    {"DELTAFP", PaxChunk::Scheme::kDeltaFp, 0, {0, 1, 2, 4}, 4},
    {"XORFOR", PaxChunk::Scheme::kXorFor, 3, {0, 1, 2, 4}, 4},
    {"DELTABITWEAVE", PaxChunk::Scheme::kDeltaBitweave, 0, {0, 1, 2, 4}, 4},
    {"PFOR", PaxChunk::Scheme::kPfor, 1, {0, 1, 2, 4}, 4},
    {"RLEDICT", PaxChunk::Scheme::kRleDict, 4, {0, 2, 4, 6}, 4},
    {"DICT", PaxChunk::Scheme::kDict, 4, {0, 2, 4, 6}, 4},
    {"OPDICT", PaxChunk::Scheme::kOpDict, 4, {0, 2, 4, 6}, 4},
    {"STREAMVBYTE", PaxChunk::Scheme::kStreamVByte, 1, {0, 1, 2, 4}, 4},
    {"D2PFOR", PaxChunk::Scheme::kD2Pfor, 0, {0, 1, 2, 4}, 4},
    {"HUFFDICT", PaxChunk::Scheme::kHuffDict, 4, {0, 2, 4, 6}, 4},
    {"BITWEAVE", PaxChunk::Scheme::kBitWeave, 1, {0, 1, 2, 4}, 4},
    {"WAVELET", PaxChunk::Scheme::kWavelet, 4, {0, 2, 4, 6}, 4},
    {"DELTAFOR", PaxChunk::Scheme::kDeltaFor, 0, {0, 1, 2, 4}, 4},
    {"BYTESLICE", PaxChunk::Scheme::kByteSlice, 1, {0, 1, 2, 4}, 4},
    {"DDRICE", PaxChunk::Scheme::kDdRice, 0, {0, 1, 2, 4}, 4},
    {"DELTAVARINT", PaxChunk::Scheme::kDeltaVarint, 0, {0, 1, 2, 4}, 4},
    {"CHIMP128", PaxChunk::Scheme::kChimp128, 3, {0, 1, 2, 4}, 4},
    {"ALPRDTANS", PaxChunk::Scheme::kAlpRdTans, 3, {0, 1, 2, 4}, 4},
    {"SIMDFOR", PaxChunk::Scheme::kSimdFor, 1, {0, 1, 2, 4}, 4},
    {"DELTASIMD", PaxChunk::Scheme::kDeltaSimd, 0, {0, 1, 2, 4}, 4},
    {"S4", PaxChunk::Scheme::kS4, 0, {0, 1, 2, 4}, 4},
    {"TOPK", PaxChunk::Scheme::kTopk, 1, {16, 20, 24, 28}, 4},
    {"SFOR", PaxChunk::Scheme::kSfor, 0, {0, 1, 2, 4}, 4},
    {"MOD", PaxChunk::Scheme::kMod, 1, {32, 36, 40, 44}, 4},
    {"TILEDICT", PaxChunk::Scheme::kTileDict, 1, {64, 68, 72, 76}, 4},
    {"EXPFOR", PaxChunk::Scheme::kExpFor, 0, {0, 1, 2, 4}, 4},
    {"SORTDICT", PaxChunk::Scheme::kSortDict, 4, {0, 2, 4, 6}, 4},
    {"LANES", PaxChunk::Scheme::kLane, 0, {0, 1, 2, 4}, 4},
    {"POSTINGS", PaxChunk::Scheme::kPostings, 4, {0, 2, 4, 6}, 4},
    {"TEMPLATE", PaxChunk::Scheme::kTemplate, 2, {0, 4, 8, 12}, 4},
    {"DSTREAM", PaxChunk::Scheme::kDStream, 0, {0, 1, 2, 4}, 4},
    {"FASTPFOR", PaxChunk::Scheme::kFastPfor, 1, {0, 1, 2, 4}, 4},
    {"DELTAFASTPFOR", PaxChunk::Scheme::kDeltaFastPfor, 0, {0, 1, 2, 4}, 4},
};

inline constexpr size_t kSchemeCaseCount =
    sizeof(kSchemeCases) / sizeof(kSchemeCases[0]);

}  // namespace tinylamb::fuzz

#endif  // TINYLAMB_TABLE_FUZZ_SCHEME_CASES_HPP
