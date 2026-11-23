/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_DETAIL_OOC_CHUNK_CODEC_HPP
#define TINYLAMB_EXECUTOR_DETAIL_OOC_CHUNK_CODEC_HPP

#include <cstddef>
#include <string>
#include <string_view>

#include "common/spill/spill_store.hpp"
#include "common/status_or.hpp"
#include "executor/data_chunk.hpp"

namespace tinylamb {

// Estimates the in-memory footprint (bytes) of `chunk`, including column
// buffers, variable-length string/nested Value payloads, and row positions.
[[nodiscard]] size_t EstimateChunkMemoryBytes(const DataChunk& chunk) noexcept;

// Appends one encoded chunk to `dst`, preserving any existing prefix.
// The payload includes column types and flags, positions, nulls, collation,
// and column data. Fallback values also carry their individual tags.
[[nodiscard]] Status EncodeDataChunk(const DataChunk& chunk, std::string* dst);

// Decodes one DataChunk emitted by `EncodeDataChunk` from `src`. On success,
// replaces `chunk_out` and advances `src`. A malformed payload leaves `src`
// at the original position and clears `chunk_out`.
[[nodiscard]] Status DecodeDataChunk(std::string_view* src,
                                     DataChunk* chunk_out);

// Writes one or more framed DataChunks into `SpillSegmentWriter`:
//   [uint32_t frame_bytes][chunk_payload]
// Splits rows into contiguous ranges to fit the codec's row limit and any
// nonzero `max_frame_bytes` limit. A single oversized row can exceed the byte
// limit.
[[nodiscard]] Status WriteDataChunk(SpillSegmentWriter* writer,
                                    const DataChunk& chunk,
                                    std::string* scratch_buffer = nullptr,
                                    size_t max_frame_bytes = 0);

// Reads the next framed DataChunk from `SpillSegmentReader`.
// Returns `false` cleanly when `reader->Eof()` before reading the frame header.
[[nodiscard]] StatusOr<bool> ReadDataChunk(
    SpillSegmentReader* reader, DataChunk* chunk_out,
    std::string* scratch_buffer = nullptr);

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_DETAIL_OOC_CHUNK_CODEC_HPP
