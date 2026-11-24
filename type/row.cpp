/**
 * Copyright 2023 KUMAZAKI Hiroki
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

#include "type/row.hpp"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "common/exc_shim.hpp"
#include "common/serdes.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

size_t Row::Serialize(char* dst) const {
  const char* const original_offset = dst;
  const bool has_null = std::ranges::any_of(
      values_, [](const Value& value) { return value.IsNull(); });
  constexpr slot_t kNullBitmapFlag = slot_t{1} << 15;
  // Slot width is 16 bits and the top bit is the null-bitmap flag; a row with
  // >= 32768 columns would silently wrap the count and corrupt the image.
  // Programmer-error precondition (documented on Row::Serialize): callers
  // accepting user-built rows must run CheckSerializable() at the Status
  // boundary first. This CHECK is the invariant assertion for that contract,
  // not a recoverable error channel -- emitting a truncated image here would
  // corrupt the table, so there is no size_t-only fallback to return.
  CHECK_MSG(values_.size() < kNullBitmapFlag,
            "too many columns to serialize a row");
  const auto count = static_cast<slot_t>(values_.size());
  dst += SerializeSlot(dst, has_null ? count | kNullBitmapFlag : count);
  if (has_null) {
    const size_t bitmap_size = (values_.size() + 7) / 8;
    memset(dst, 0, bitmap_size);
    for (size_t i = 0; i < values_.size(); ++i) {
      if (values_[i].IsNull()) {
        dst[i / 8] |= static_cast<char>(1U << (i % 8));  // NOLINT(bugprone-narrowing-conversions): byte mask fits in the low byte.
      }
    }
    dst += bitmap_size;
  }
  for (const Value& value : values_) {
    if (!value.IsNull()) {
      dst += value.Serialize(dst);
    }
  }
  return static_cast<size_t>(dst - original_offset);
}

Status Row::CheckSerializable() const {
  if (values_.size() >= (slot_t{1} << 15)) {
    return StatusError(StatusCode::kTooBigData,
                       "too many columns to serialize a row");
  }
  for (const Value& value : values_) {
    RETURN_IF_FAIL(value.CheckSerializable());
  }
  return Status::kSuccess;
}

StatusOr<size_t> Row::TryDeserialize(const char* src, const Schema& sc) {
  const char* const original_offset = src;
  constexpr slot_t kNullBitmapFlag = slot_t{1} << 15;
  slot_t encoded_count = 0;
  src += DeserializeSlot(src, &encoded_count);
  const bool has_null = (encoded_count & kNullBitmapFlag) != 0;
  const auto count = static_cast<slot_t>(encoded_count & ~kNullBitmapFlag);
  // The stored image always carries every schema column; a forged count
  // would otherwise drive sc.GetColumn(i) out of bounds below.
  if (count != sc.ColumnCount()) {
    return StatusError(StatusCode::kCorrupt, "row image column count mismatch");
  }
  const char* bitmap = nullptr;
  if (has_null) {
    bitmap = src;
    src += (count + 7) / 8;
  }
  const Column* const columns = count != 0 ? &sc.GetColumn(0) : nullptr;
  Status error = Status::kSuccess;
  // Reuse surviving Value slots (see TryDeserializeProjected): a repeated
  // decode into the same Row keeps owned_data capacity alive across rows.
  if (values_.size() != count) {
    if (values_.size() < count) {
      values_.reserve(count);
    }
    values_.resize(count);
  }
  if (has_null) {
    for (slot_t i = 0; i < count; ++i) {
      if ((bitmap[i / 8] & static_cast<char>(1U << (i % 8))) != 0) {
        values_[i] = Value{};  // SQL NULL; clears the payload, keeps capacity.
        continue;
      }
      const size_t consumed =
          values_[i].DeserializeInto(src, columns[i].Type(), &error);
      if (error != Status::kSuccess) [[unlikely]] {
        return error;
      }
      src += consumed;
      if (columns[i].IsUnsigned() && values_[i].type == ValueType::kInt64) {
        values_[i].SetUnsigned();
      }
      if (columns[i].IsInterval() && !values_[i].IsNull()) {
        values_[i].SetInterval();
      }
    }
  } else {
    for (slot_t i = 0; i < count; ++i) {
      const size_t consumed =
          values_[i].DeserializeInto(src, columns[i].Type(), &error);
      if (error != Status::kSuccess) [[unlikely]] {
        return error;
      }
      src += consumed;
      if (columns[i].IsUnsigned() && values_[i].type == ValueType::kInt64) {
        values_[i].SetUnsigned();
      }
      if (columns[i].IsInterval() && !values_[i].IsNull()) {
        values_[i].SetInterval();
      }
    }
  }
  return static_cast<size_t>(src - original_offset);
}

StatusOr<size_t> Row::TryDeserializeProjected(
    const char* src, const Schema& sc, const std::vector<slot_t>& columns) {
  const size_t kept_count = columns.size();
  // Slot reuse requires strictly ascending, in-range storage columns.
  for (size_t k = 0; k < kept_count; ++k) {
    if (columns[k] >= sc.ColumnCount()) {
      return StatusError(StatusCode::kInvalidArgument,
                         "row projection column is out of range");
    }
    if (k != 0 && columns[k - 1] >= columns[k]) {
      return StatusError(StatusCode::kInvalidArgument,
                         "row projection must be strictly ascending");
    }
  }
  const char* const original_offset = src;
  constexpr slot_t kNullBitmapFlag = slot_t{1} << 15;
  slot_t encoded_count = 0;
  src += DeserializeSlot(src, &encoded_count);
  const bool has_null = (encoded_count & kNullBitmapFlag) != 0;
  const auto count = static_cast<slot_t>(encoded_count & ~kNullBitmapFlag);
  // Projected reads still walk the full stored image to skip unkept columns,
  // so the count must match the full schema here as well.
  if (count != sc.ColumnCount()) {
    return StatusError(StatusCode::kCorrupt, "row image column count mismatch");
  }
  const char* bitmap = nullptr;
  if (has_null) {
    bitmap = src;
    src += (count + 7) / 8;
  }

  const Column* const schema_columns = count != 0 ? &sc.GetColumn(0) : nullptr;
  const slot_t* const kept = kept_count != 0 ? columns.data() : nullptr;
  Status error = Status::kSuccess;
  // Reuse surviving Value slots instead of clear()+emplace_back(): the scan
  // iterator decodes row after row into the same Row, and a surviving slot
  // keeps its owned_data capacity, so VARCHAR decode stops paying one
  // malloc/free pair per variable column per row. Slot -> column mapping is
  // fixed per projection, so a reused slot only ever sees one column's type
  // and its collation flag stays consistent.
  if (values_.size() != kept_count) {
    if (values_.size() < kept_count) {
      values_.reserve(kept_count);
    }
    values_.resize(kept_count);
  }
  size_t projection = 0;
  size_t i = 0;
  // Two loops (bitmap / no-bitmap) keep the common no-NULL row free of the
  // per-column bitmap branch; `keep` is checked first since projections are
  // usually a small subset walked in order.
  for (; has_null && i < count; ++i) {
    const bool keep = projection < kept_count && kept[projection] == i;
    if ((bitmap[i / 8] & static_cast<char>(1U << (i % 8))) != 0) {
      if (keep) {
        // NULL slot; the reset clears the payload but keeps owned_data
        // capacity, so a later non-NULL row refills without allocating.
        values_[projection] = Value{};
        ++projection;
      }
      continue;
    }
    const ValueType type = schema_columns[i].Type();
    if (keep) {
      Value& value = values_[projection];
      if (const size_t fixed = FixedSerializedSize(type); fixed != 0)
          [[likely]] {
        uint64_t bits = 0;
        src += DeserializeU64(src, &bits);
        value.value.int_value = static_cast<int64_t>(bits);
        value.type = type;
        value.SetCollation(0);
        src += fixed - sizeof(int64_t);
      } else {
        const size_t consumed = value.DeserializeInto(src, type, &error);
        if (error != Status::kSuccess) [[unlikely]] {
          return error;
        }
        src += consumed;
      }
      if (schema_columns[i].IsUnsigned() && value.type == ValueType::kInt64) {
        value.SetUnsigned();
      }
      if (schema_columns[i].IsInterval() && !value.IsNull()) {
        value.SetInterval();
      }
      ++projection;
    } else {
      if (const size_t fixed = FixedSerializedSize(type); fixed != 0)
          [[likely]] {
        src += fixed;
      } else {
        const size_t skipped = Value::SkipSerializedInto(src, type, &error);
        if (error != Status::kSuccess) [[unlikely]] {
          return error;
        }
        src += skipped;
      }
    }
  }
  for (; i < count; ++i) {
    const bool keep = projection < kept_count && kept[projection] == i;
    const ValueType type = schema_columns[i].Type();
    if (keep) {
      Value& value = values_[projection];
      if (const size_t fixed = FixedSerializedSize(type); fixed != 0)
          [[likely]] {
        uint64_t bits = 0;
        src += DeserializeU64(src, &bits);
        value.value.int_value = static_cast<int64_t>(bits);
        value.type = type;
        value.SetCollation(0);
        src += fixed - sizeof(int64_t);
      } else {
        const size_t consumed = value.DeserializeInto(src, type, &error);
        if (error != Status::kSuccess) [[unlikely]] {
          return error;
        }
        src += consumed;
      }
      if (schema_columns[i].IsUnsigned() && value.type == ValueType::kInt64) {
        value.SetUnsigned();
      }
      if (schema_columns[i].IsInterval() && !value.IsNull()) {
        value.SetInterval();
      }
      ++projection;
    } else {
      if (const size_t fixed = FixedSerializedSize(type); fixed != 0)
          [[likely]] {
        src += fixed;
      } else {
        const size_t skipped = Value::SkipSerializedInto(src, type, &error);
        if (error != Status::kSuccess) [[unlikely]] {
          return error;
        }
        src += skipped;
      }
    }
  }
  return static_cast<size_t>(src - original_offset);
}

StatusOr<size_t> Row::TryDeserializePlanned(const char* src,
                                            const RowDecodePlan& plan) {
  const char* const original_offset = src;
  constexpr slot_t kNullBitmapFlag = slot_t{1} << 15;
  slot_t encoded_count = 0;
  src += DeserializeSlot(src, &encoded_count);
  const bool has_null = (encoded_count & kNullBitmapFlag) != 0;
  const auto count = static_cast<slot_t>(encoded_count & ~kNullBitmapFlag);
  // Same image contract as TryDeserializeProjected: projected reads still
  // walk the full stored image, so the count must match the whole schema.
  if (count != plan.stored_count) {
    return StatusError(StatusCode::kCorrupt, "row image column count mismatch");
  }
  const char* bitmap = nullptr;
  if (has_null) {
    bitmap = src;
    src += (count + 7) / 8;
  }
  const size_t kept_count = plan.kept_count;
  // Slot-reuse decode (see TryDeserializeProjected): a surviving slot keeps
  // its owned_data capacity across rows.
  if (values_.size() != kept_count) {
    if (values_.size() < kept_count) {
      values_.reserve(kept_count);
    }
    values_.resize(kept_count);
  }
  const ValueType* const types = plan.stored_types.data();
  const size_t* const slots = plan.stored_slot.data();
  Status error = Status::kSuccess;
  // Every stored column fixed-width and no unsigned tag: the per-cell
  // dispatch (FixedSerializedSize switch, Status-plumbed skip calls,
  // unsigned check) collapses to a constant 8-byte stride.  A row without a
  // null bitmap has every cell present, so the kept slots also sit at
  // constant payload offsets (independent loads instead of a cursor chain).
  if (plan.all_fixed && !plan.has_unsigned) {
    Value* const out = values_.data();
    if (!has_null) [[likely]] {
      if (kept_count == plan.stored_count) {
        for (slot_t i = 0; i < count; ++i) {
          uint64_t bits = 0;
          std::memcpy(&bits, src, sizeof(bits));
          src += sizeof(bits);
          out[i].value.int_value = static_cast<int64_t>(TINYLAMB_BSWAP64(bits));
          out[i].type = types[i];
          out[i].SetCollation(0);
        }
      } else {
        const char* const payload = src;
        for (slot_t i = 0; i < count; ++i) {
          const size_t slot = slots[i];
          if (slot == RowDecodePlan::kNoSlot) [[likely]] {
            continue;
          }
          uint64_t bits = 0;
          std::memcpy(&bits, payload + (i * sizeof(bits)), sizeof(bits));
          out[slot].value.int_value =
              static_cast<int64_t>(TINYLAMB_BSWAP64(bits));
          out[slot].type = types[i];
          out[slot].SetCollation(0);
        }
        src = payload + (static_cast<size_t>(count) * sizeof(uint64_t));
      }
      return static_cast<size_t>(src - original_offset);
    }
    // Rows carrying a bitmap: a NULL column contributes no payload, so the
    // cursor walk stays, but the FixedSerializedSize/skip dispatch still
    // collapses to the fixed stride.
    for (slot_t i = 0; i < count; ++i) {
      const size_t slot = slots[i];
      if ((bitmap[i >> 3] & static_cast<char>(1U << (i & 7))) != 0) {
        if (slot != RowDecodePlan::kNoSlot) {
          out[slot] = Value{};  // clears payload, keeps owned_data capacity
        }
        continue;
      }
      uint64_t bits = 0;
      std::memcpy(&bits, src, sizeof(bits));
      src += sizeof(bits);
      if (slot != RowDecodePlan::kNoSlot) {
        out[slot].value.int_value =
            static_cast<int64_t>(TINYLAMB_BSWAP64(bits));
        out[slot].type = types[i];
        out[slot].SetCollation(0);
      }
    }
    return static_cast<size_t>(src - original_offset);
  }
  size_t i = 0;
  for (; has_null && i < count; ++i) {
    const size_t slot = slots[i];
    if ((bitmap[i / 8] & static_cast<char>(1U << (i % 8))) != 0) {
      if (slot != RowDecodePlan::kNoSlot) {
        values_[slot] = Value{};  // clears payload, keeps owned_data capacity
      }
      continue;
    }
    if (slot == RowDecodePlan::kNoSlot) {
      if (const size_t fixed = FixedSerializedSize(types[i]); fixed != 0)
          [[likely]] {
        src += fixed;
      } else if (types[i] == ValueType::kVarChar) [[likely]] {
        // VARCHAR dominates the variable-width skip set; inline the
        // length-prefix walk instead of the Status-plumbed call.
        bin_size_t len = 0;
        std::memcpy(&len, src, sizeof(len));
        src += sizeof(len) + TINYLAMB_BSWAP16(len);
      } else {
        const size_t skipped = Value::SkipSerializedInto(src, types[i], &error);
        if (error != Status::kSuccess) [[unlikely]] {
          return error;
        }
        src += skipped;
      }
      continue;
    }
    Value& value = values_[slot];
    if (const size_t fixed = FixedSerializedSize(types[i]); fixed != 0)
        [[likely]] {
      uint64_t bits = 0;
      src += DeserializeU64(src, &bits);
      value.value.int_value = static_cast<int64_t>(bits);
      value.type = types[i];
      value.SetCollation(0);
      src += fixed - sizeof(int64_t);
    } else {
      const size_t consumed = value.DeserializeInto(src, types[i], &error);
      if (error != Status::kSuccess) [[unlikely]] {
        return error;
      }
      src += consumed;
    }
    if (slot != RowDecodePlan::kNoSlot && plan.slot_unsigned[slot] != 0 &&
        value.type == ValueType::kInt64) {
      value.SetUnsigned();
    }
    if (slot != RowDecodePlan::kNoSlot && plan.has_interval &&
        plan.slot_interval[slot] != 0 && !value.IsNull()) {
      value.SetInterval();
    }
  }
  for (; i < count; ++i) {
    const size_t slot = slots[i];
    if (slot == RowDecodePlan::kNoSlot) {
      if (const size_t fixed = FixedSerializedSize(types[i]); fixed != 0)
          [[likely]] {
        src += fixed;
      } else if (types[i] == ValueType::kVarChar) [[likely]] {
        // VARCHAR dominates the variable-width skip set; inline the
        // length-prefix walk instead of the Status-plumbed call.
        bin_size_t len = 0;
        std::memcpy(&len, src, sizeof(len));
        src += sizeof(len) + TINYLAMB_BSWAP16(len);
      } else {
        const size_t skipped = Value::SkipSerializedInto(src, types[i], &error);
        if (error != Status::kSuccess) [[unlikely]] {
          return error;
        }
        src += skipped;
      }
      continue;
    }
    Value& value = values_[slot];
    if (const size_t fixed = FixedSerializedSize(types[i]); fixed != 0)
        [[likely]] {
      uint64_t bits = 0;
      src += DeserializeU64(src, &bits);
      value.value.int_value = static_cast<int64_t>(bits);
      value.type = types[i];
      value.SetCollation(0);
      src += fixed - sizeof(int64_t);
    } else {
      const size_t consumed = value.DeserializeInto(src, types[i], &error);
      if (error != Status::kSuccess) [[unlikely]] {
        return error;
      }
      src += consumed;
    }
    if (slot != RowDecodePlan::kNoSlot && plan.slot_unsigned[slot] != 0 &&
        value.type == ValueType::kInt64) {
      value.SetUnsigned();
    }
    if (slot != RowDecodePlan::kNoSlot && plan.has_interval &&
        plan.slot_interval[slot] != 0 && !value.IsNull()) {
      value.SetInterval();
    }
  }
  return static_cast<size_t>(src - original_offset);
}

StatusOr<std::optional<int64_t>> Row::TryPeekInteger(const char* src,
                                                     const Schema& sc,
                                                     slot_t column) {
  constexpr slot_t kNullBitmapFlag = slot_t{1} << 15;
  slot_t encoded_count = 0;
  src += DeserializeSlot(src, &encoded_count);
  const bool has_null = (encoded_count & kNullBitmapFlag) != 0;
  const auto count = static_cast<slot_t>(encoded_count & ~kNullBitmapFlag);
  if (count != sc.ColumnCount()) {
    return Status::kCorrupt;
  }
  if (column >= count) {
    return Status::kInvalidArgument;
  }
  const char* bitmap = nullptr;
  if (has_null) {
    bitmap = src;
    src += (count + 7) / 8;
  }
  for (slot_t i = 0; i < count; ++i) {
    const bool is_null =
        has_null && (bitmap[i / 8] & static_cast<char>(1U << (i % 8))) != 0;
    if (i == column) {
      if (is_null) {
        return std::nullopt;
      }
      const ValueType type = sc.GetColumn(i).Type();
      if (type != ValueType::kInt64 && type != ValueType::kDate) {
        return std::nullopt;
      }
      // INT64/DATE images are fixed-width big-endian; a direct load answers
      // the peek without constructing a Value (row decode ran this per row).
      int64_t key = 0;
      DeserializeInteger(src, &key);
      // UINT64 images are stored as raw int64 bits. Peeking reinterprets
      // those bits as int64 so the value compares equal to the original
      // Value's int_value (RuntimeFilter matches the bit pattern); a 2^63+
      // value therefore wraps to a negative int64 here. int64 has no positive
      // representation for it, and no caller reads this as a magnitude. DATE
      // is a day-count domain and never takes part in unsigned key
      // comparison, so ignore a stray unsigned flag on a DATE column.
      if (type == ValueType::kInt64 && sc.GetColumn(i).IsUnsigned()) {
        return static_cast<int64_t>(static_cast<uint64_t>(key));
      }
      return key;
    }
    if (is_null) {
      continue;
    }
    if (const size_t fixed = FixedSerializedSize(sc.GetColumn(i).Type());
        fixed != 0) [[likely]] {
      src += fixed;
      continue;
    }
    Status skip_error = Status::kSuccess;
    const size_t skipped =
        Value::SkipSerializedInto(src, sc.GetColumn(i).Type(), &skip_error);
    if (skip_error != Status::kSuccess) [[unlikely]] {
      return skip_error;
    }
    src += skipped;
  }
  return std::nullopt;
}

size_t Row::Size() const {
  const bool has_null = std::ranges::any_of(
      values_, [](const Value& value) { return value.IsNull(); });
  size_t ret = sizeof(uint16_t);
  if (has_null) {
    ret += (values_.size() + 7) / 8;
  }
  for (const auto& v : values_) {
    if (!v.IsNull()) {
      ret += v.Size();
    }
  }
  return ret;
}

std::string Row::ToString() const {
  std::ostringstream output;
  output << *this;
  return output.str();
}

StatusOr<std::string> Row::TryEncodeMemcomparableFormat() const {
  std::vector<std::string> parts;
  parts.reserve(values_.size());
  size_t total = 0;
  for (const auto& v : values_) {
    ASSIGN_OR_RETURN(std::string, part, v.TryEncodeMemcomparableFormat());
    total += part.size();
    parts.push_back(std::move(part));
  }
  std::string encoded;
  encoded.reserve(total);
  for (const std::string& part : parts) {
    encoded += part;
  }
  return encoded;
}

std::string Row::EncodeKeyMemcomparableFormat(const Schema& sc) const {
  if (values_.empty()) {
    throw std::runtime_error("cannot encode key of empty row");
  }
  const auto idx = sc.KeyColumnIndex();
  if (idx >= values_.size()) {
    throw std::runtime_error("key column index out of range");
  }
  return values_[idx].EncodeMemcomparableFormat();
}

size_t Row::EncodeKeyMemcomparableFormatTo(const Schema& sc, char* dst) const {
  if (values_.empty()) {
    throw std::runtime_error("cannot encode key of empty row");
  }
  const auto idx = sc.KeyColumnIndex();
  if (idx >= values_.size()) {
    throw std::runtime_error("key column index out of range");
  }
  return values_[idx].EncodeMemcomparableFormatTo(dst);
}

namespace {

size_t PeekValueSerializedSize(  // NOLINT(misc-no-recursion)
    const char* src, size_t max_len, size_t need, ValueType vt,
    size_t depth = 0) {
  // Use the same limit (and the same >= boundary) as the other decode paths
  // in value.cpp; `> 32` would accept a depth-33 array that the real decoder
  // rejects as corrupt, so Peek and Deserialize would disagree.
  if (depth >= Value::kMaxArrayNestingDepth) {
    throw std::runtime_error("array nesting depth exceeded");
  }
  switch (vt) {
    case ValueType::kInt64:
    case ValueType::kDate:
    case ValueType::kDouble:
      if (max_len - need < sizeof(int64_t)) {
        throw std::runtime_error("row exceeds available bytes");
      }
      return need + sizeof(int64_t);
    case ValueType::kVarChar: {
      if (max_len - need < sizeof(bin_size_t)) {
        throw std::runtime_error("truncated varchar header");
      }
      bin_size_t len = 0;
      DeserializeSlot(src + need, &len);
      if (max_len - need < sizeof(bin_size_t) + len) {
        throw std::runtime_error("row exceeds available bytes");
      }
      return need + sizeof(bin_size_t) + len;
    }
    case ValueType::kArray: {
      if (max_len - need < sizeof(uint32_t) + sizeof(bin_size_t)) {
        throw std::runtime_error("truncated array header");
      }
      uint32_t elem_count = 0;
      need += DeserializeU32(src + need, &elem_count);
      // Match Value::DeserializeInto / SkipSerializedInto: without this cap a
      // corrupt image with many elements but enough trailing bytes would be
      // "valid" to Peek and then rejected by the real decoder.
      constexpr uint32_t kMaxEncodedArrayElements = 1U << 20;
      if (elem_count > kMaxEncodedArrayElements) {
        throw std::runtime_error("corrupt array element count");
      }
      bin_size_t type_len = 0;
      DeserializeSlot(src + need, &type_len);
      if (max_len - need < sizeof(bin_size_t) + type_len) {
        throw std::runtime_error("truncated array type header");
      }
      need += sizeof(bin_size_t) + type_len;
      if (elem_count > max_len - need) {
        throw std::runtime_error("truncated array element presence byte");
      }
      for (uint32_t i = 0; i < elem_count; ++i) {
        if (max_len - need < 1) {
          throw std::runtime_error("truncated array element presence byte");
        }
        const bool present = src[need++] != 0;
        if (!present) {
          continue;
        }
        if (max_len - need < 1) {
          throw std::runtime_error("truncated array element type byte");
        }
        const auto encoded_type = static_cast<uint8_t>(src[need++]);
        if ((encoded_type >> 3) > 4) {
          throw std::runtime_error("invalid array element tag");
        }
        const auto elem_vt = static_cast<ValueType>(encoded_type & 7);
        need = PeekValueSerializedSize(src, max_len, need, elem_vt, depth + 1);
      }
      return need;
    }
    default:
      throw std::runtime_error("null column cannot be deserialized");
  }
}

}  // namespace

size_t Row::PeekSerializedSize(const char* src, size_t max_len,
                               const Schema& sc) {
  if (max_len < sizeof(slot_t)) {
    throw std::runtime_error("truncated row header");
  }
  constexpr slot_t kNullBitmapFlag = slot_t{1} << 15;
  slot_t count_raw = 0;
  DeserializeSlot(src, &count_raw);
  const bool has_map = (count_raw & kNullBitmapFlag) != 0;
  const auto count = static_cast<slot_t>(count_raw & ~kNullBitmapFlag);
  if (count != sc.ColumnCount()) {
    throw std::runtime_error("column count mismatch");
  }
  size_t need = sizeof(count_raw);
  const size_t bmp_bytes = (static_cast<size_t>(count) + 7) / 8;
  if (has_map) {
    if (max_len - need < bmp_bytes) {
      throw std::runtime_error("truncated row nullmap");
    }
    need += bmp_bytes;
  }
  const char* bitmap = src + sizeof(count_raw);
  for (slot_t i = 0; i < count; ++i) {
    if (has_map && (bitmap[i / 8] & static_cast<char>(1U << (i % 8))) != 0) {
      continue;
    }
    const ValueType vt = sc.GetColumn(i).Type();
    need = PeekValueSerializedSize(src, max_len, need, vt);
  }
  return need;
}

size_t Row::DeserializeBounded(const char* src, size_t max_len,
                               const Schema& sc) {
  // Two passes are intentional: PeekSerializedSize validates every length
  // and nesting bound against `max_len` before any read, because
  // TryDeserialize is deliberately bounds-agnostic and trusts its input.
  // Folding the checks into the decode would let a corrupt image read past
  // the buffer; Peek's result is the validated decoded byte count we return.
  const size_t need = PeekSerializedSize(src, max_len, sc);
  const auto decoded = TryDeserialize(src, sc);
  if (!decoded.HasValue()) {
    throw std::runtime_error(decoded.GetStatus().GetMessage());
  }
  return need;
}

Status Row::TryDecodeMemcomparableFormat(std::string_view src) {
  values_.clear();
  while (!src.empty()) {
    Value v;
    // Self-delimiting memcomparable chunks: the callee consumes by offsets,
    // never as a C string.
    ASSIGN_OR_RETURN(size_t, advanced, v.TryDecodeMemcomparableFormat(src));
    src.remove_prefix(advanced);
    values_.push_back(v);
  }
  return Status::kSuccess;
}

StatusOr<Row> Row::TryExtract(const std::vector<slot_t>& elms) const {
  std::vector<Value> extracted;
  extracted.reserve(elms.size());
  for (size_t offset : elms) {
    if (values_.size() <= offset) {
      return StatusError(StatusCode::kInvalidArgument,
                         "row projection column is out of range");
    }
    // Value owns its variable-length payload, and TryExtract is const, so a
    // deep copy is required for the returned Row to outlive this one; there
    // is no reference-based projection in this representation.
    extracted.push_back(values_[offset]);
  }
  return Row(std::move(extracted));
}

Row Row::operator+(const Row& rhs) const {
  // Join output materialization calls this per emitted tuple; size up front so
  // the lhs copy does not grow-regrow (two allocations) per row.
  std::vector<Value> v;
  v.reserve(values_.size() + rhs.values_.size());
  v.insert(v.end(), values_.begin(), values_.end());
  v.insert(v.end(), rhs.values_.begin(), rhs.values_.end());
  return Row(std::move(v));
}

std::ostream& operator<<(std::ostream& o, const Row& r) {
  o << "[";
  for (size_t i = 0; i < r.values_.size(); ++i) {
    if (0 < i) {
      o << ", ";
    }
    o << r.values_[i];
  }
  o << "]";
  return o;
}

Encoder& operator<<(Encoder& e, const Row& r) {
  e << r.values_;
  return e;
}

Decoder& operator>>(Decoder& d, Row& r) {
  d >> r.values_;
  return d;
}
}  // namespace tinylamb

uint64_t std::hash<tinylamb::Row>::operator()(const tinylamb::Row& row) const {
  uint64_t ret = 0x9e3779b97f4a7c15ULL;
  for (const auto& v : row.values_) {
    ret = (ret << 7) | (ret >> 57);
    ret ^= std::hash<tinylamb::Value>()(v);
    ret *= 0x100000001b3ULL;
  }
  return ret;
}
