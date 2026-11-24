/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/ooc/chunk_codec.hpp"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <new>
#include <stdexcept>
#include <vector>

#include "type/row_block_codec.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {
namespace {

template <typename T>
void AppendPod(T val, std::string* dst) {
  // append() writes the bytes once; resize() would zero-fill then memcpy.
  const char* bytes = reinterpret_cast<const char*>(&val);
  dst->append(bytes, sizeof(T));
}

template <typename T>
Status ReadPod(std::string_view* src, T* out) {
  if (src->size() < sizeof(T)) {
    return StatusError(StatusCode::kCorrupt, "truncated DataChunk spill frame");
  }
  src->copy(reinterpret_cast<char*>(out), sizeof(T));
  src->remove_prefix(sizeof(T));
  return Status::kSuccess;
}

constexpr uint8_t kFlagUnsigned = 1U << 0;
constexpr uint8_t kFlagHeterogeneous = 1U << 1;
constexpr uint8_t kFlagFallbackValues = 1U << 2;
constexpr uint8_t kFlagHasNulls = 1U << 3;
constexpr unsigned kCollationShift = 5;
constexpr uint8_t kCollationMask = 0x03U;
constexpr uint32_t kMaxFrameRows = 1U << 22;

}  // namespace

size_t EstimateChunkMemoryBytes(const DataChunk& chunk) noexcept {
  size_t total = sizeof(DataChunk) + (chunk.Size() * sizeof(RowPosition));
  for (size_t c = 0; c < chunk.ColumnCount(); ++c) {
    const ColumnVector& col = chunk.ColumnAt(c);
    total += sizeof(ColumnVector);
    total += col.NullBitmap().size() * sizeof(uint64_t);
    if (col.IsHeterogeneous() || col.Type() == ValueType::kArray) {
      for (size_t r = 0; r < col.Size(); ++r) {
        total += EstimateValueMemoryBytes(col.ValueAt(r));
      }
      continue;
    }
    switch (col.Type()) {
      case ValueType::kInt64:
      case ValueType::kDate:
        total += col.IntegerData().size() * sizeof(int64_t);
        break;
      case ValueType::kDouble:
        total += col.DoubleData().size() * sizeof(double);
        break;
      case ValueType::kVarChar:
        total += col.StringData().size() * sizeof(std::string);
        for (const std::string& s : col.StringData()) {
          total += s.size();
        }
        break;
      case ValueType::kNull:
      case ValueType::kArray:
        break;
    }
  }
  return total;
}

namespace {

Status ValidateChunkColumns(const DataChunk& chunk) {
  for (size_t i = 0; i < chunk.ColumnCount(); ++i) {
    const ColumnVector& column = chunk.ColumnAt(i);
    if (column.Size() != chunk.Size() ||
        (column.Type() == ValueType::kVarChar && !column.IsHeterogeneous() &&
         column.StringData().size() != chunk.Size())) {
      return StatusError(StatusCode::kInvalidArgument,
                         "inconsistent column width in DataChunk spill input");
    }
  }
  return Status::kSuccess;
}

Status EncodeDataChunkPayload(const DataChunk& chunk, std::string* dst) try {
  const auto row_count = static_cast<uint32_t>(chunk.Size());
  const auto col_count = static_cast<uint32_t>(chunk.ColumnCount());
  AppendPod(row_count, dst);
  AppendPod(col_count, dst);

  bool has_positions = false;
  for (uint32_t r = 0; r < row_count; ++r) {
    if (chunk.PositionAt(r).IsValid()) {
      has_positions = true;
      break;
    }
  }
  AppendPod(static_cast<uint8_t>(has_positions ? 1 : 0), dst);
  if (has_positions && row_count > 0) {
    const size_t pos_off = dst->size();
    dst->resize(pos_off +
                (static_cast<size_t>(row_count) * RowPosition::Size()));
    for (uint32_t r = 0; r < row_count; ++r) {
      const RowPosition& p = chunk.PositionAt(r);
      const std::string ser = p.Serialize();
      std::memcpy(dst->data() + pos_off +
                      (static_cast<size_t>(r) * RowPosition::Size()),
                  ser.data(), RowPosition::Size());
    }
  }

  for (uint32_t c = 0; c < col_count; ++c) {
    const ColumnVector& col = chunk.ColumnAt(c);
    const ValueType vt = col.Type();
    const Collation coll = col.ColumnCollation();
    const bool use_fallback =
        col.IsHeterogeneous() || vt == ValueType::kArray ||
        (vt == ValueType::kVarChar && coll != Collation::kNone);

    bool any_null = false;
    if (!use_fallback && !col.NullBitmap().empty()) {
      for (const uint64_t word : col.NullBitmap()) {
        if (word != 0) {
          any_null = true;
          break;
        }
      }
    }

    // NULL values carry no collation tag, so preserve the column collation
    // even when this frame contains no non-NULL values.
    auto flags = static_cast<uint8_t>(
        (static_cast<uint8_t>(coll) & kCollationMask) << kCollationShift);
    if (col.IsUnsigned()) {
      flags |= kFlagUnsigned;
    }
    if (col.IsHeterogeneous()) {
      flags |= kFlagHeterogeneous;
    }
    if (use_fallback) {
      flags |= kFlagFallbackValues;
    }
    if (any_null) {
      flags |= kFlagHasNulls;
    }
    AppendPod(static_cast<uint8_t>(vt), dst);
    AppendPod(flags, dst);

    if (row_count == 0) {
      continue;
    }
    if (use_fallback) {
      for (uint32_t r = 0; r < row_count; ++r) {
        RETURN_IF_FAIL(EncodeSpillValue(col.ValueAt(r), dst));
      }
      continue;
    }

    if (any_null) {
      const size_t nulls_off = dst->size();
      dst->resize(nulls_off + row_count);
      for (uint32_t r = 0; r < row_count; ++r) {
        (*dst)[nulls_off + r] = col.IsNull(r) ? 1 : 0;
      }
    }

    switch (vt) {
      case ValueType::kNull:
        break;
      case ValueType::kInt64:
      case ValueType::kDate: {
        const size_t bytes = static_cast<size_t>(row_count) * sizeof(int64_t);
        const size_t off = dst->size();
        dst->resize(off + bytes);
        if (col.IntegerData().size() >= row_count) {
          std::memcpy(dst->data() + off, col.IntegerData().data(), bytes);
        } else {
          std::memset(dst->data() + off, 0, bytes);
        }
        break;
      }
      case ValueType::kDouble: {
        const size_t bytes = static_cast<size_t>(row_count) * sizeof(double);
        const size_t off = dst->size();
        dst->resize(off + bytes);
        if (col.DoubleData().size() >= row_count) {
          std::memcpy(dst->data() + off, col.DoubleData().data(), bytes);
        } else {
          std::memset(dst->data() + off, 0, bytes);
        }
        break;
      }
      case ValueType::kVarChar: {
        const auto& strs = col.StringData();
        for (uint32_t r = 0; r < row_count; ++r) {
          if (any_null && col.IsNull(r)) {
            AppendPod(uint32_t{0}, dst);
            continue;
          }
          const std::string& s = strs[r];
          if (s.size() > std::numeric_limits<uint32_t>::max()) {
            return StatusError(StatusCode::kTooBigData,
                               "string exceeds 4GiB DataChunk spill limit");
          }
          const auto slen = static_cast<uint32_t>(s.size());
          AppendPod(slen, dst);
          if (slen > 0) {
            dst->append(s.data(), slen);
          }
        }
        break;
      }
      case ValueType::kArray:
        // Handled by `use_fallback`.
        break;
    }
  }
  return Status::kSuccess;
} catch (const std::length_error&) {
  return Status::kTooBigData;
} catch (const std::bad_alloc&) {
  return Status::kMemoryLimitExceeded;
}

}  // namespace

Status EncodeDataChunk(const DataChunk& chunk, std::string* dst) {
  if (dst == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "null dst in EncodeDataChunk");
  }
  if (chunk.Size() > kMaxFrameRows ||
      chunk.ColumnCount() > std::numeric_limits<uint32_t>::max()) {
    return StatusError(StatusCode::kTooBigData,
                       "DataChunk dimensions exceed frame limit");
  }
  RETURN_IF_FAIL(ValidateChunkColumns(chunk));
  const size_t original_size = dst->size();
  const Status encoded = EncodeDataChunkPayload(chunk, dst);
  if (!encoded.ok()) {
    dst->resize(original_size);
  }
  return encoded;
}

namespace {

Status DecodeDataChunkPayload(std::string_view* src, DataChunk* chunk_out) try {
  uint32_t row_count = 0;
  uint32_t col_count = 0;
  uint8_t has_positions = 0;
  RETURN_IF_FAIL(ReadPod(src, &row_count));
  RETURN_IF_FAIL(ReadPod(src, &col_count));
  RETURN_IF_FAIL(ReadPod(src, &has_positions));

  // Validate the in-frame dimensions against the remaining payload before
  // using them as allocation sizes: every column carries at least a 2-byte
  // (type, flags) header, and row_count is capped far above any legitimate
  // spill frame (frames are byte-capped by the staging/grace flush limits).
  // Without these checks a corrupted header drives multi-GiB resize /
  // Initialize calls whose std::bad_alloc / std::length_error escapes the
  // Status boundary (project invariant 1).
  constexpr size_t kColumnHeaderBytes = 2;
  if (col_count > src->size() / kColumnHeaderBytes) {
    return StatusError(StatusCode::kCorrupt,
                       "col_count exceeds DataChunk frame payload");
  }
  if (row_count > kMaxFrameRows) {
    return StatusError(StatusCode::kCorrupt,
                       "row_count exceeds DataChunk frame limit");
  }
  if (has_positions > 1) {
    return StatusError(StatusCode::kCorrupt,
                       "invalid RowPositions flag in DataChunk frame");
  }

  std::vector<RowPosition> positions;
  if (has_positions != 0 && row_count > 0) {
    const size_t pos_bytes =
        static_cast<size_t>(row_count) * RowPosition::Size();
    const size_t header_bytes =
        static_cast<size_t>(col_count) * kColumnHeaderBytes;
    if (src->size() - header_bytes < pos_bytes) {
      return StatusError(StatusCode::kCorrupt,
                         "truncated RowPositions in DataChunk frame");
    }
    positions.resize(row_count);
    for (uint32_t r = 0; r < row_count; ++r) {
      positions[r].Deserialize(src->data() +
                               (static_cast<size_t>(r) * RowPosition::Size()));
    }
    src->remove_prefix(pos_bytes);
  }

  chunk_out->Reset();
  std::vector<ValueType> initial_types(col_count, ValueType::kNull);
  // The per-column decoders check payload extents before growing their
  // buffers. Avoid reserving row-sized storage from the header alone.
  chunk_out->Initialize(initial_types, 0);

  std::vector<uint8_t> nulls_scratch;
  std::vector<int64_t> ints_scratch;
  std::vector<double> doubles_scratch;
  std::vector<std::string_view> sv_scratch;

  for (uint32_t c = 0; c < col_count; ++c) {
    uint8_t raw_type = 0;
    uint8_t flags = 0;
    RETURN_IF_FAIL(ReadPod(src, &raw_type));
    RETURN_IF_FAIL(ReadPod(src, &flags));
    if (raw_type > static_cast<uint8_t>(ValueType::kArray)) {
      return StatusError(StatusCode::kCorrupt,
                         "invalid ValueType in DataChunk frame");
    }
    const auto vt = static_cast<ValueType>(raw_type);
    const bool is_unsigned = (flags & kFlagUnsigned) != 0;
    const bool is_hetero = (flags & kFlagHeterogeneous) != 0;
    const bool use_fallback = (flags & kFlagFallbackValues) != 0;
    const bool has_nulls = (flags & kFlagHasNulls) != 0;

    ColumnVector& col = chunk_out->ColumnAt(c);
    ZoneMap* zm = chunk_out->MutableZoneMapAt(c);
    // The collation field is written for every frame (heterogeneous columns
    // carry per-value tags instead, but the bits must still be in range).
    const auto collation =
        static_cast<Collation>((flags >> kCollationShift) & kCollationMask);
    if (collation > Collation::kCaseInsensitive) {
      return StatusError(StatusCode::kCorrupt,
                         "invalid collation in DataChunk frame");
    }
    if (is_hetero) {
      col.SetAllowHeterogeneous(true);
    } else {
      // A frame must always rebind an empty column; a non-empty one means a
      // corrupt frame or a reused output chunk and cannot be decoded safely.
      if (!col.RebindEmptyType(vt, is_unsigned, collation)) {
        return StatusError(StatusCode::kCorrupt,
                           "non-empty column in DataChunk frame");
      }
    }

    if (row_count == 0) {
      continue;
    }

    if (use_fallback) {
      // Every fallback value consumes at least one tag byte.
      if (static_cast<size_t>(row_count) > src->size()) {
        return StatusError(StatusCode::kCorrupt,
                           "truncated fallback values in DataChunk frame");
      }
      for (uint32_t r = 0; r < row_count; ++r) {
        ASSIGN_OR_RETURN(Value, v, DecodeSpillValue(src));
        col.Append(v);
        zm->Add(v);
      }
      continue;
    }

    const uint8_t* nulls_ptr = nullptr;
    if (has_nulls) {
      if (src->size() < row_count) {
        return StatusError(StatusCode::kCorrupt,
                           "truncated null mask in DataChunk frame");
      }
      nulls_scratch.resize(row_count);
      std::memcpy(nulls_scratch.data(), src->data(), row_count);
      src->remove_prefix(row_count);
      nulls_ptr = nulls_scratch.data();
    }

    switch (vt) {
      case ValueType::kNull:
        col.AppendNullBatch(row_count, zm);
        break;
      case ValueType::kInt64:
      case ValueType::kDate: {
        const size_t bytes = static_cast<size_t>(row_count) * sizeof(int64_t);
        if (src->size() < bytes) {
          return StatusError(StatusCode::kCorrupt,
                             "truncated integer column in DataChunk frame");
        }
        ints_scratch.resize(row_count);
        std::memcpy(ints_scratch.data(), src->data(), bytes);
        src->remove_prefix(bytes);
        col.AppendIntBatch(vt, is_unsigned, ints_scratch.data(), nulls_ptr,
                           row_count, zm);
        break;
      }
      case ValueType::kDouble: {
        const size_t bytes = static_cast<size_t>(row_count) * sizeof(double);
        if (src->size() < bytes) {
          return StatusError(StatusCode::kCorrupt,
                             "truncated double column in DataChunk frame");
        }
        doubles_scratch.resize(row_count);
        std::memcpy(doubles_scratch.data(), src->data(), bytes);
        src->remove_prefix(bytes);
        col.AppendDoubleBatch(doubles_scratch.data(), nulls_ptr, row_count, zm);
        break;
      }
      case ValueType::kVarChar: {
        // Every cell carries at least a 4-byte length prefix (null cells
        // encode a zero length), so the scratch cannot exceed the payload.
        if (static_cast<size_t>(row_count) * 4 > src->size()) {
          return StatusError(StatusCode::kCorrupt,
                             "truncated varchar column in DataChunk frame");
        }
        sv_scratch.resize(row_count);
        for (uint32_t r = 0; r < row_count; ++r) {
          uint32_t slen = 0;
          RETURN_IF_FAIL(ReadPod(src, &slen));
          if (src->size() < slen) {
            return StatusError(StatusCode::kCorrupt,
                               "truncated varchar cell in DataChunk frame");
          }
          sv_scratch[r] = src->substr(0, slen);
          src->remove_prefix(slen);
        }
        col.AppendStringViewBatch(sv_scratch.data(), nulls_ptr, row_count, zm);
        break;
      }
      case ValueType::kArray:
        return StatusError(StatusCode::kCorrupt,
                           "unexpected non-fallback array column");
    }
  }

  chunk_out->CommitBatchAppend(row_count);
  if (has_positions != 0U) {
    for (uint32_t r = 0; r < row_count; ++r) {
      chunk_out->SetPositionAt(r, positions[r]);
    }
  }
  return Status::kSuccess;
} catch (const std::length_error&) {
  return Status::kTooBigData;
} catch (const std::bad_alloc&) {
  return Status::kMemoryLimitExceeded;
}

// Upper-bound estimate of the `EncodeDataChunk` bytes contributed by one row
// (per-row null-mask byte, fixed cell widths, varchar length prefixes,
// fallback Value payloads, and the position block).  Overestimating is safe:
// `WriteRowRange` re-splits on the actual encoded size.
size_t EstimateEncodedRowBytes(const DataChunk& chunk, size_t r) {
  size_t bytes = RowPosition::Size();
  for (size_t c = 0; c < chunk.ColumnCount(); ++c) {
    const ColumnVector& col = chunk.ColumnAt(c);
    if (col.IsHeterogeneous() || col.Type() == ValueType::kArray) {
      bytes += EstimateValueMemoryBytes(col.ValueAt(r)) + 16;
      continue;
    }
    bytes += 1;
    switch (col.Type()) {
      case ValueType::kInt64:
      case ValueType::kDate:
      case ValueType::kDouble:
        bytes += 8;
        break;
      case ValueType::kVarChar:
        bytes += 4 + (col.IsNull(r) ? 0 : col.StringData()[r].size());
        break;
      case ValueType::kNull:
      case ValueType::kArray:
        break;
    }
  }
  return bytes;
}

// Encodes rows [begin, end) of `chunk` and writes them as one frame; when the
// encoded payload exceeds `max_frame_bytes` the range is bisected
// recursively.  A single row that alone exceeds the cap is written as-is.
Status WriteRowRange(SpillSegmentWriter* writer, const DataChunk& chunk,
                     size_t begin, size_t end, size_t max_frame_bytes,
                     std::string* buf) {
  buf->clear();
  buf->resize(sizeof(uint32_t));
  const DataChunk* payload = nullptr;
  DataChunk sub;
  if (begin == 0 && end == chunk.Size()) {
    payload = &chunk;
  } else {
    for (size_t r = begin; r < end; ++r) {
      sub.AppendFrom(chunk, r);
    }
    payload = &sub;
  }
  RETURN_IF_FAIL(EncodeDataChunk(*payload, buf));
  const size_t payload_bytes = buf->size() - sizeof(uint32_t);
  if (max_frame_bytes != 0 && payload_bytes > max_frame_bytes &&
      end - begin > 1) {
    const size_t mid = begin + ((end - begin) / 2);
    RETURN_IF_FAIL(
        WriteRowRange(writer, chunk, begin, mid, max_frame_bytes, buf));
    return WriteRowRange(writer, chunk, mid, end, max_frame_bytes, buf);
  }
  if (payload_bytes > std::numeric_limits<uint32_t>::max()) {
    return StatusError(StatusCode::kTooBigData,
                       "DataChunk spill frame exceeds uint32_t");
  }
  const auto frame_len = static_cast<uint32_t>(payload_bytes);
  std::memcpy(buf->data(), &frame_len, sizeof(uint32_t));
  return writer->Write(buf->data(), buf->size());
}

}  // namespace

Status DecodeDataChunk(std::string_view* src, DataChunk* chunk_out) {
  if (src == nullptr || chunk_out == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "null argument in DecodeDataChunk");
  }
  std::string_view remaining = *src;
  Status decoded = DecodeDataChunkPayload(&remaining, chunk_out);
  if (!decoded.ok()) {
    *chunk_out = DataChunk{};
    return decoded;
  }
  *src = remaining;
  return Status::kSuccess;
}

// Spill frames carry only a length header; no magic/version. Spill files are
// process-local temporaries, so format drift cannot mix readers/writers across
// binaries; corruption relies on the payload decoder's own end checks.
Status WriteDataChunk(SpillSegmentWriter* writer, const DataChunk& chunk,
                      std::string* scratch_buffer, size_t max_frame_bytes) try {
  if (writer == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "null writer in WriteDataChunk");
  }
  RETURN_IF_FAIL(ValidateChunkColumns(chunk));
  std::string local;
  std::string* buf = (scratch_buffer != nullptr) ? scratch_buffer : &local;
  if (chunk.Size() <= kMaxFrameRows &&
      (max_frame_bytes == 0 || chunk.Size() <= 1)) {
    return WriteRowRange(writer, chunk, 0, chunk.Size(), max_frame_bytes, buf);
  }
  for (size_t begin = 0; begin < chunk.Size();) {
    const size_t row_limit =
        begin + std::min<size_t>(kMaxFrameRows, chunk.Size() - begin);
    size_t end = row_limit;
    if (max_frame_bytes != 0) {
      // Pack rows against the estimated size and split again if the encoded
      // size exceeds the cap. Always allow at least one row.
      end = begin;
      size_t acc = 0;
      const size_t row_budget =
          max_frame_bytes - std::min(max_frame_bytes, size_t{64});
      while (end < row_limit) {
        const size_t row_bytes = EstimateEncodedRowBytes(chunk, end);
        // Leave 64B headroom for the frame/column headers; `WriteRowRange`
        // re-verifies the encoded size and re-splits on any misestimate.
        if (end > begin && row_bytes > row_budget - std::min(acc, row_budget)) {
          break;
        }
        acc += row_bytes;
        ++end;
      }
    }
    RETURN_IF_FAIL(
        WriteRowRange(writer, chunk, begin, end, max_frame_bytes, buf));
    begin = end;
  }
  return Status::kSuccess;
} catch (const std::length_error&) {
  return Status::kTooBigData;
} catch (const std::bad_alloc&) {
  return Status::kMemoryLimitExceeded;
}

StatusOr<bool> ReadDataChunk(SpillSegmentReader* reader, DataChunk* chunk_out,
                             std::string* scratch_buffer) {
  if (reader == nullptr || chunk_out == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "null argument in ReadDataChunk");
  }
  RETURN_IF_FAIL(reader->Open());
  if (reader->Eof()) {
    return false;
  }
  uint32_t frame_bytes = 0;
  RETURN_IF_FAIL(reader->ReadPod(&frame_bytes));
  if (frame_bytes == 0) {
    // The writer always emits at least the 4-byte frame header plus an
    // encoded chunk, so a zero length means a corrupt spill image. Report it
    // here instead of letting DecodeDataChunk fail with a generic corrupt
    // error on an empty view.
    return StatusError(StatusCode::kCorrupt,
                       "invalid zero-length DataChunk spill frame");
  }
  std::string local;
  std::string* buf = (scratch_buffer != nullptr) ? scratch_buffer : &local;
  if (frame_bytes > reader->Remaining()) {
    return StatusError(StatusCode::kCorrupt, "truncated DataChunk spill frame");
  }
  try {
    buf->resize(frame_bytes);
  } catch (const std::length_error&) {
    return Status::kTooBigData;
  } catch (const std::bad_alloc&) {
    return Status::kMemoryLimitExceeded;
  }
  if (frame_bytes > 0) {
    RETURN_IF_FAIL(reader->ReadExact(buf->data(), frame_bytes));
  }
  std::string_view sv(*buf);
  RETURN_IF_FAIL(DecodeDataChunk(&sv, chunk_out));
  if (!sv.empty()) {
    *chunk_out = DataChunk{};
    return StatusError(StatusCode::kCorrupt,
                       "trailing unconsumed bytes in DataChunk spill frame");
  }
  return true;
}

}  // namespace tinylamb
