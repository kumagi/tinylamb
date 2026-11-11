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

#pragma once

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "table/pax_chunk/pax_chunk.hpp"

// Narrow adapter used only by the chunk-size experiment.  It deliberately
// invokes the same encoders and scalar lookup primitives as PaxChunk without
// serializing a footer.  The persisted footer stores row counts as uint16_t,
// so a footer-shaped benchmark cannot represent one 65,536-row cell; this
// adapter lets the experiment vary the codec block independently from the
// existing 1,024-row physical row group. Compressed schemes conservatively
// decline blocks above 65,535 rows unless their wider wire fields are proven:
// PLAIN and CONST accept larger numeric blocks, integer RLE also does, and
// varchar PLAIN is the sole large string-block path. Smaller per-codec limits
// still apply. A declined block is unsupported, never a zero-byte encoding.
namespace tinylamb::codec_benchmark {

enum class ValueKind : uint8_t { kInt64, kDoubleBits, kVarchar };

struct EncodedBlock {
  std::vector<char> bytes;
  PaxChunk::Scheme scheme{PaxChunk::Scheme::kPlain};
  uint32_t param{0};
  uint32_t row_count{0};
  ValueKind kind{ValueKind::kInt64};
};

std::optional<EncodedBlock> EncodeInt64Block(PaxChunk::Scheme scheme,
                                             std::span<const int64_t> values);

std::optional<EncodedBlock> EncodeDoubleBitsBlock(
    PaxChunk::Scheme scheme, std::span<const uint64_t> values);

std::optional<EncodedBlock> EncodeVarcharBlock(
    PaxChunk::Scheme scheme, std::span<const std::string> values);

int64_t DecodeInt64At(const EncodedBlock& block, uint32_t index);

// Decodes in logical order.  Cumulative codecs use a single-pass path so the
// no-sort scan baseline stays O(n), rather than repeatedly replaying a prefix.
void DecodeInt64All(const EncodedBlock& block, std::span<int64_t> output);

uint32_t LowerBoundInt64(const EncodedBlock& block, int64_t value);

// Evaluates low <= value < high directly on an order-preserving dictionary's
// packed rank codes.  When output_mask is non-empty, matching rows are ORed at
// row_offset + local_row; an empty span requests COUNT only.  This keeps the
// unsorted OpDict baseline in the compressed domain instead of materializing
// int64 values merely for the benchmark.
bool OpDictRangeFold(const EncodedBlock& block, int64_t low, int64_t high,
                     uint32_t row_offset, std::span<uint64_t> output_mask,
                     uint64_t* count_out);

std::string DecodeVarcharAt(const EncodedBlock& block, uint32_t index);

uint32_t LowerBoundVarchar(const EncodedBlock& block, std::string_view value);

// Sequential compressed-domain range fold for PLAIN, FSST, and Front-Code
// varchar blocks.  It reuses one decode buffer per block and optionally ORs
// matches into a segment-wide mask, avoiding O(n * restart_interval) random
// decoding for the unsorted baseline.
bool VarcharRangeFold(const EncodedBlock& block, std::string_view low,
                      std::string_view high, uint32_t row_offset,
                      std::span<uint64_t> output_mask, uint64_t* count_out);

}  // namespace tinylamb::codec_benchmark
