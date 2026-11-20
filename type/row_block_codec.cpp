/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "type/row_block_codec.hpp"

#include <array>
#include <cstring>
#include <limits>
#include <utility>

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
    return StatusError(StatusCode::kCorrupt, "truncated row block stream");
  }
  src->copy(reinterpret_cast<char*>(out), sizeof(T));
  src->remove_prefix(sizeof(T));
  return Status::kSuccess;
}

Status EncodeValueImpl(const Value& val, std::string* dst, int depth) {
  if (depth <= 0) {
    return StatusError(StatusCode::kTooBigData,
                       "value nesting depth exceeds spill codec limit");
  }
  const auto tag =
      static_cast<uint8_t>((static_cast<uint8_t>(val.type) & 0x07U) |
                           ((val.Collation() & 0x07U) << 3));
  AppendPod(tag, dst);
  switch (val.type) {
    case ValueType::kNull:
      return Status::kSuccess;
    case ValueType::kInt64:
    case ValueType::kDate:
      AppendPod(val.value.int_value, dst);
      return Status::kSuccess;
    case ValueType::kDouble:
      AppendPod(val.value.double_value, dst);
      return Status::kSuccess;
    case ValueType::kVarChar: {
      if (val.value.varchar_value.size() >
          std::numeric_limits<uint32_t>::max()) {
        return StatusError(StatusCode::kTooBigData,
                           "varchar value exceeds 4GiB spill limit");
      }
      const auto len = static_cast<uint32_t>(val.value.varchar_value.size());
      AppendPod(len, dst);
      if (len > 0) {
        dst->append(val.value.varchar_value);
      }
      return Status::kSuccess;
    }
    case ValueType::kArray: {
      const std::string_view sql_type = val.ArrayElementSqlType();
      if (sql_type.size() > std::numeric_limits<uint32_t>::max()) {
        return StatusError(StatusCode::kTooBigData,
                           "array SQL type exceeds 4GiB spill limit");
      }
      const auto type_len = static_cast<uint32_t>(sql_type.size());
      AppendPod(type_len, dst);
      if (type_len > 0) {
        dst->append(sql_type);
      }
      const auto& elems = val.ArrayElements();
      if (elems.size() > std::numeric_limits<uint32_t>::max()) {
        return StatusError(StatusCode::kTooBigData,
                           "array element count exceeds spill limit");
      }
      const auto count = static_cast<uint32_t>(elems.size());
      AppendPod(count, dst);
      for (const Value& elem : elems) {
        RETURN_IF_FAIL(EncodeValueImpl(elem, dst, depth - 1));
      }
      return Status::kSuccess;
    }
  }
  return StatusError(StatusCode::kUnknownType,
                     "unsupported ValueType in row spill codec");
}

Status DecodeRowBlockPayload(uint32_t row_count, std::string_view payload,
                             std::vector<Row>* rows_out) {
  // Every encoded row begins with a uint32_t column count, so row_count cannot
  // exceed payload.size() / sizeof(uint32_t). Validate before reserve() to
  // guard against OOM on corrupt headers.
  if (static_cast<uint64_t>(row_count) > payload.size() / sizeof(uint32_t)) {
    return StatusError(StatusCode::kCorrupt, "corrupt row count in row block");
  }
  rows_out->reserve(rows_out->size() + row_count);
  for (uint32_t i = 0; i < row_count; ++i) {
    ASSIGN_OR_RETURN(Row, row, DecodeSpillRow(&payload));
    rows_out->push_back(std::move(row));
  }
  if (!payload.empty()) {
    return StatusError(StatusCode::kCorrupt,
                       "trailing unconsumed bytes in row block");
  }
  return Status::kSuccess;
}

}  // namespace

size_t EstimateValueMemoryBytes(const Value& val) noexcept {
  size_t total = sizeof(Value);
  switch (val.type) {
    case ValueType::kNull:
    case ValueType::kInt64:
    case ValueType::kDate:
    case ValueType::kDouble:
      break;
    case ValueType::kVarChar:
      // Count the full VARCHAR payload conservatively (including short
      // strings that may live in SSO) so string-heavy skew never undercounts.
      total += val.value.varchar_value.size();
      break;
    case ValueType::kArray:
      total += val.ArrayElementSqlType().size();
      for (const Value& elem : val.ArrayElements()) {
        total += EstimateValueMemoryBytes(elem);
      }
      break;
  }
  return total;
}

size_t EstimateRowMemoryBytes(const Row& row) noexcept {
  size_t total = sizeof(Row);
  for (const Value& v : row.values_) {
    total += EstimateValueMemoryBytes(v);
  }
  return total;
}

Status EncodeSpillValue(const Value& val, std::string* dst) {
  if (dst == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "null dst in EncodeSpillValue");
  }
  return EncodeValueImpl(val, dst, 64);
}

StatusOr<Value> DecodeSpillValue(std::string_view* src, int max_depth) {
  if (src == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "null src in DecodeSpillValue");
  }
  if (max_depth <= 0) {
    return StatusError(StatusCode::kCorrupt,
                       "array nesting depth exceeded during spill decode");
  }
  uint8_t tag = 0;
  RETURN_IF_FAIL(ReadPod(src, &tag));
  const auto vt = static_cast<ValueType>(tag & 0x07U);
  const uint8_t collation = (tag >> 3) & 0x07U;
  if (collation > 4) {
    return StatusError(StatusCode::kCorrupt,
                       "corrupt collation tag in row spill stream");
  }
  switch (vt) {
    case ValueType::kNull:
      return Value();
    case ValueType::kInt64: {
      int64_t v = 0;
      RETURN_IF_FAIL(ReadPod(src, &v));
      Value out(v);
      out.SetCollation(collation);
      return out;
    }
    case ValueType::kDate: {
      int64_t v = 0;
      RETURN_IF_FAIL(ReadPod(src, &v));
      Value out = Value::DateFromDays(v);
      out.SetCollation(collation);
      return out;
    }
    case ValueType::kDouble: {
      double d = 0.0;
      RETURN_IF_FAIL(ReadPod(src, &d));
      Value out(d);
      out.SetCollation(collation);
      return out;
    }
    case ValueType::kVarChar: {
      uint32_t len = 0;
      RETURN_IF_FAIL(ReadPod(src, &len));
      if (src->size() < len) {
        return StatusError(StatusCode::kCorrupt,
                           "truncated varchar in row spill stream");
      }
      Value out;
      out.CopyFromStringView(src->substr(0, len));
      src->remove_prefix(len);
      out.SetCollation(collation);
      return out;
    }
    case ValueType::kArray: {
      uint32_t type_len = 0;
      RETURN_IF_FAIL(ReadPod(src, &type_len));
      if (src->size() < type_len) {
        return StatusError(StatusCode::kCorrupt,
                           "truncated array SQL type in row spill stream");
      }
      std::string sql_type(src->substr(0, type_len));
      src->remove_prefix(type_len);
      uint32_t count = 0;
      RETURN_IF_FAIL(ReadPod(src, &count));
      if (src->size() < count) {
        // Each element consumes at least 1 byte tag.
        return StatusError(StatusCode::kCorrupt,
                           "corrupt array count in row spill stream");
      }
      std::vector<Value> elems;
      elems.reserve(count);
      for (uint32_t i = 0; i < count; ++i) {
        ASSIGN_OR_RETURN(Value, elem, DecodeSpillValue(src, max_depth - 1));
        elems.push_back(std::move(elem));
      }
      Value out = Value::Array(std::move(elems), std::move(sql_type));
      out.SetCollation(collation);
      return out;
    }
  }
  return StatusError(StatusCode::kCorrupt,
                     "invalid ValueType in row spill stream");
}

Status EncodeSpillRow(const Row& row, std::string* dst) {
  if (dst == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "null dst in EncodeSpillRow");
  }
  if (row.values_.size() > std::numeric_limits<uint32_t>::max()) {
    return StatusError(StatusCode::kTooBigData,
                       "row column count exceeds spill limit");
  }
  AppendPod(static_cast<uint32_t>(row.values_.size()), dst);
  for (const Value& v : row.values_) {
    RETURN_IF_FAIL(EncodeSpillValue(v, dst));
  }
  return Status::kSuccess;
}

StatusOr<Row> DecodeSpillRow(std::string_view* src) {
  if (src == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "null src in DecodeSpillRow");
  }
  uint32_t col_count = 0;
  RETURN_IF_FAIL(ReadPod(src, &col_count));
  if (src->size() < col_count) {
    return StatusError(StatusCode::kCorrupt,
                       "corrupt column count in row spill stream");
  }
  std::vector<Value> vals;
  vals.reserve(col_count);
  for (uint32_t i = 0; i < col_count; ++i) {
    ASSIGN_OR_RETURN(Value, v, DecodeSpillValue(src));
    vals.push_back(std::move(v));
  }
  return Row(std::move(vals));
}

Status EncodeRowBlock(std::span<const Row> rows, std::string* dst) {
  if (dst == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "null dst in EncodeRowBlock");
  }
  if (rows.size() > std::numeric_limits<uint32_t>::max()) {
    return StatusError(StatusCode::kTooBigData,
                       "row block count exceeds uint32_t");
  }
  // [row_count, payload_bytes] only: no magic/version, as with OOC spill
  // frames (temporary storage, never shared across binary versions).
  const size_t header_off = dst->size();
  AppendPod(static_cast<uint32_t>(rows.size()), dst);
  AppendPod(uint32_t{0}, dst);  // placeholder for payload_bytes
  const size_t payload_start = dst->size();
  for (const Row& row : rows) {
    const Status status = EncodeSpillRow(row, dst);
    if (status != Status::kSuccess) {
      dst->resize(header_off);
      return status;
    }
  }
  const size_t payload_bytes = dst->size() - payload_start;
  if (payload_bytes > std::numeric_limits<uint32_t>::max()) {
    dst->resize(header_off);
    return StatusError(StatusCode::kTooBigData,
                       "row block payload exceeds uint32_t");
  }
  const auto pbytes = static_cast<uint32_t>(payload_bytes);
  std::memcpy(dst->data() + header_off + sizeof(uint32_t), &pbytes,
              sizeof(uint32_t));
  return Status::kSuccess;
}

Status DecodeRowBlock(std::string_view* src, std::vector<Row>* rows_out) {
  if (src == nullptr || rows_out == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "null argument in DecodeRowBlock");
  }
  uint32_t row_count = 0;
  uint32_t payload_bytes = 0;
  RETURN_IF_FAIL(ReadPod(src, &row_count));
  RETURN_IF_FAIL(ReadPod(src, &payload_bytes));
  if (src->size() < payload_bytes) {
    return StatusError(StatusCode::kCorrupt, "truncated row block payload");
  }
  std::string_view payload = src->substr(0, payload_bytes);
  src->remove_prefix(payload_bytes);
  return DecodeRowBlockPayload(row_count, payload, rows_out);
}

Status WriteRowBlock(SpillSegmentWriter* writer, std::span<const Row> rows,
                     std::string* scratch_buffer) {
  if (writer == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "null writer in WriteRowBlock");
  }
  std::string local;
  std::string* buf = (scratch_buffer != nullptr) ? scratch_buffer : &local;
  buf->clear();
  RETURN_IF_FAIL(EncodeRowBlock(rows, buf));
  return writer->Write(buf->data(), buf->size());
}

StatusOr<bool> ReadRowBlock(SpillSegmentReader* reader,
                            std::vector<Row>* rows_out,
                            std::string* scratch_buffer) {
  if (reader == nullptr || rows_out == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "null argument in ReadRowBlock");
  }
  RETURN_IF_FAIL(reader->Open());
  if (reader->Eof()) {
    return false;
  }
  std::array<uint32_t, 2> header = {0, 0};
  RETURN_IF_FAIL(reader->ReadExact(header.data(), sizeof(header)));
  const uint32_t row_count = header[0];
  const uint32_t payload_bytes = header[1];
  if (payload_bytes > reader->Remaining()) {
    return StatusError(StatusCode::kCorrupt, "truncated row block payload");
  }
  std::string local;
  std::string* buf = (scratch_buffer != nullptr) ? scratch_buffer : &local;
  buf->resize(payload_bytes);
  if (payload_bytes > 0) {
    RETURN_IF_FAIL(reader->ReadExact(buf->data(), payload_bytes));
  }
  RETURN_IF_FAIL(
      DecodeRowBlockPayload(row_count, std::string_view(*buf), rows_out));
  return true;
}

}  // namespace tinylamb
