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
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "common/constants.hpp"
#include "common/decoder.hpp"
#include "common/encoder.hpp"
#include "common/serdes.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb {
Value& Row::operator[](size_t i) { return values_[i]; }

const Value& Row::operator[](size_t i) const { return values_[i]; }

size_t Row::Serialize(char* dst) const {
  const char* const original_offset = dst;
  const bool has_null =
      std::any_of(values_.begin(), values_.end(),
                  [](const Value& value) { return value.IsNull(); });
  constexpr slot_t kNullBitmapFlag = slot_t{1} << 15;
  const slot_t count = static_cast<slot_t>(values_.size());
  dst += SerializeSlot(dst, has_null ? count | kNullBitmapFlag : count);
  if (has_null) {
    const size_t bitmap_size = (values_.size() + 7) / 8;
    memset(dst, 0, bitmap_size);
    for (size_t i = 0; i < values_.size(); ++i) {
      if (values_[i].IsNull()) dst[i / 8] |= static_cast<char>(1U << (i % 8));
    }
    dst += bitmap_size;
  }
  for (const Value& value : values_) {
    if (!value.IsNull()) dst += value.Serialize(dst);
  }
  return dst - original_offset;
}

size_t Row::Deserialize(const char* src, const Schema& sc) {
  const char* const original_offset = src;
  constexpr slot_t kNullBitmapFlag = slot_t{1} << 15;
  slot_t encoded_count;
  src += DeserializeSlot(src, &encoded_count);
  const bool has_null = (encoded_count & kNullBitmapFlag) != 0;
  const slot_t count = encoded_count & ~kNullBitmapFlag;
  const char* bitmap = nullptr;
  if (has_null) {
    bitmap = src;
    src += (count + 7) / 8;
  }
  values_.clear();
  values_.reserve(count);
  for (slot_t i = 0; i < count; ++i) {
    const bool is_null =
        has_null && (bitmap[i / 8] & static_cast<char>(1U << (i % 8))) != 0;
    Value v;
    if (!is_null) src += v.Deserialize(src, sc.GetColumn(i).Type());
    values_.push_back(v);
  }
  return src - original_offset;
}

size_t Row::DeserializeProjected(const char* src, const Schema& sc,
                                 const std::vector<slot_t>& columns) {
  const char* const original_offset = src;
  constexpr slot_t kNullBitmapFlag = slot_t{1} << 15;
  slot_t encoded_count;
  src += DeserializeSlot(src, &encoded_count);
  const bool has_null = (encoded_count & kNullBitmapFlag) != 0;
  const slot_t count = encoded_count & ~kNullBitmapFlag;
  const char* bitmap = nullptr;
  if (has_null) {
    bitmap = src;
    src += (count + 7) / 8;
  }

  values_.clear();
  values_.reserve(columns.size());
  size_t projection = 0;
  for (slot_t i = 0; i < count; ++i) {
    const bool is_null =
        has_null && (bitmap[i / 8] & static_cast<char>(1U << (i % 8))) != 0;
    const bool keep =
        projection < columns.size() && columns[projection] == i;
    if (is_null) {
      if (keep) {
        values_.emplace_back();
        ++projection;
      }
      continue;
    }
    const ValueType type = sc.GetColumn(i).Type();
    if (keep) {
      Value value;
      src += value.Deserialize(src, type);
      values_.push_back(std::move(value));
      ++projection;
    } else {
      src += Value::SkipSerialized(src, type);
    }
  }
  return src - original_offset;
}

std::optional<int64_t> Row::TryPeekInteger(const char* src, const Schema& sc,
                                           slot_t column) {
  constexpr slot_t kNullBitmapFlag = slot_t{1} << 15;
  slot_t encoded_count;
  src += DeserializeSlot(src, &encoded_count);
  const bool has_null = (encoded_count & kNullBitmapFlag) != 0;
  const slot_t count = encoded_count & ~kNullBitmapFlag;
  if (column >= count) return std::nullopt;
  const char* bitmap = nullptr;
  if (has_null) {
    bitmap = src;
    src += (count + 7) / 8;
  }
  for (slot_t i = 0; i < count; ++i) {
    const bool is_null =
        has_null && (bitmap[i / 8] & static_cast<char>(1U << (i % 8))) != 0;
    if (i == column) {
      if (is_null) return std::nullopt;
      const ValueType type = sc.GetColumn(i).Type();
      if (type != ValueType::kInt64 && type != ValueType::kDate) {
        return std::nullopt;
      }
      Value value;
      value.Deserialize(src, type);
      return value.value.int_value;
    }
    if (is_null) continue;
    src += Value::SkipSerialized(src, sc.GetColumn(i).Type());
  }
  return std::nullopt;
}

size_t Row::Size() const {
  const bool has_null =
      std::any_of(values_.begin(), values_.end(),
                  [](const Value& value) { return value.IsNull(); });
  size_t ret = sizeof(uint16_t);
  if (has_null) ret += (values_.size() + 7) / 8;
  for (const auto& v : values_) {
    if (!v.IsNull()) ret += v.Size();
  }
  return ret;
}

std::string Row::EncodeMemcomparableFormat() const {
  std::stringstream ss;
  for (const auto& v : values_) {
    ss << v.EncodeMemcomparableFormat();
  }
  return ss.str();
}

void Row::DecodeMemcomparableFormat(std::string_view src) {
  values_.clear();
  while (!src.empty()) {
    Value v;
    size_t advanced = v.DecodeMemcomparableFormat(src.data());
    src.remove_prefix(advanced);
    values_.push_back(v);
  }
}

Row Row::Extract(const std::vector<slot_t>& elms) const {
  Row tmp;
  std::vector<Value> extracted;
  extracted.reserve(elms.size());
  for (size_t offset : elms) {
    if (values_.size() <= offset) {
      return {};
    }
    extracted.push_back(values_[offset]);
  }
  return Row(extracted);
}

Row Row::operator+(const Row& rhs) const {
  std::vector v(values_);
  v.reserve(v.size() + rhs.Size());
  for (const auto& r : rhs.values_) {
    v.push_back(r);
  }
  return Row(v);
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
  uint64_t ret = 0xcafe;
  for (const auto& v : row.values_) {
    ret += std::hash<tinylamb::Value>()(v);
  }
  return ret;
}
