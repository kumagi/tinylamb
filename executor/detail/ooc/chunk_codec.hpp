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

// Encodes `chunk` (columnar types, unsigned/collation/heterogeneous metadata,
// valid RowPositions, NULL bitmaps, and column payloads) onto `dst`.
[[nodiscard]] Status EncodeDataChunk(const DataChunk& chunk, std::string* dst);

// Decodes one DataChunk emitted by `EncodeDataChunk` from `src`, resetting
// `chunk_out` and advancing `src` past the consumed bytes.
[[nodiscard]] Status DecodeDataChunk(std::string_view* src,
                                     DataChunk* chunk_out);

// Writes one framed DataChunk into `SpillSegmentWriter`:
//   [uint32_t frame_bytes][chunk_payload]
// When `max_frame_bytes` > 0 and the encoded chunk exceeds it, the rows are
// split across multiple frames (each holding a row-contiguous sub-chunk) so a
// reader loop reassembles the same rows without ever materializing a frame
// much larger than the cap (only a single oversized row can still exceed it).
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
