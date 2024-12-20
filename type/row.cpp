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
  dst += SerializeSlot(dst, values_.size());
  for (const auto& v : values_) {
    dst += v.Serialize(dst);
  }
  return dst - original_offset;
}

size_t Row::Deserialize(const char* src, const Schema& sc) {
  const char* const original_offset = src;
  uint16_t count;
  src += DeserializeSlot(src, &count);
  values_.clear();
  values_.reserve(count);
  for (uint16_t i = 0; i < count; ++i) {
    Value v;
    src += v.Deserialize(src, sc.GetColumn(i).Type());
    values_.push_back(v);
  }
  return src - original_offset;
}

size_t Row::Size() const {
  size_t ret = sizeof(uint16_t);
  for (const auto& v : values_) {
    ret += v.Size();
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
