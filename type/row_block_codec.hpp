/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_TYPE_ROW_BLOCK_CODEC_HPP
#define TINYLAMB_TYPE_ROW_BLOCK_CODEC_HPP

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "common/spill/spill_store.hpp"
#include "common/status_or.hpp"
#include "type/row.hpp"
#include "type/value.hpp"

namespace tinylamb {

// Estimates the in-memory footprint (bytes) of `val`, including heap-owned
// string/array payloads.
[[nodiscard]] size_t EstimateValueMemoryBytes(const Value& val) noexcept;

// Estimates the in-memory footprint (bytes) of `row`, including outer struct,
// `values_` vector storage, and nested Value payloads.
[[nodiscard]] size_t EstimateRowMemoryBytes(const Row& row) noexcept;

// Appends a self-describing, length-safe binary representation of `val` onto
// `dst` (supports arbitrary string lengths > 64 KiB,
// unsigned/interval/collation tags, NULLs, dates, doubles, and arrays).
[[nodiscard]] Status EncodeSpillValue(const Value& val, std::string* dst);

// Decodes one Value from `src`, advancing `src` past the consumed bytes.
[[nodiscard]] StatusOr<Value> DecodeSpillValue(std::string_view* src,
                                               int max_depth = 64);

// Appends one self-describing Row onto `dst` (does not depend on 16-bit
// VARCHAR prefixes or external Schema).
[[nodiscard]] Status EncodeSpillRow(const Row& row, std::string* dst);

// Decodes one Row from `src`, advancing `src` past the consumed bytes.
[[nodiscard]] StatusOr<Row> DecodeSpillRow(std::string_view* src);

// Encodes `rows` into a single framed block appended to `dst`:
//   [uint32_t row_count][uint32_t payload_bytes][row_0 ... row_k]
[[nodiscard]] Status EncodeRowBlock(std::span<const Row> rows,
                                    std::string* dst);

// Decodes one framed block emitted by `EncodeRowBlock` from `src`, advancing
// `src` past the block and appending decoded rows to `rows_out`.
[[nodiscard]] Status DecodeRowBlock(std::string_view* src,
                                    std::vector<Row>* rows_out);

// Streams a framed Row block into `SpillSegmentWriter` using at most one block
// buffer.
[[nodiscard]] Status WriteRowBlock(SpillSegmentWriter* writer,
                                   std::span<const Row> rows,
                                   std::string* scratch_buffer = nullptr);

// Reads the next framed Row block from `SpillSegmentReader`.
// Returns `true` if a block was read (appended/assigned into `rows_out`), or
// `false` cleanly on EOF when `reader->Eof()` is true before reading the block
// header.
[[nodiscard]] StatusOr<bool> ReadRowBlock(
    SpillSegmentReader* reader, std::vector<Row>* rows_out,
    std::string* scratch_buffer = nullptr);

}  // namespace tinylamb

#endif  // TINYLAMB_TYPE_ROW_BLOCK_CODEC_HPP
