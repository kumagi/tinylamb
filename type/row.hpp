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

#ifndef TINYLAMB_ROW_HPP
#define TINYLAMB_ROW_HPP

#include <cstring>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>

#include "common/exc_shim.hpp"
#include "common/status_or.hpp"
#include "type/value.hpp"

namespace tinylamb {
class Schema;

struct Row {
  Row() = default;
  Row(std::initializer_list<Value> v) : values_(v) {}
  explicit Row(std::vector<Value> v) : values_(std::move(v)) {}
  Row(const Row&) = default;
  Row(Row&&) = default;
  Row& operator=(const Row&) = default;
  Row& operator=(Row&&) = default;
  ~Row() = default;

  Value& operator[](size_t i);
  const Value& operator[](size_t i) const;
  // Serialize assumes the row image fits the on-disk widths; callers taking
  // user-supplied rows must check CheckSerializable() first (VARCHAR values
  // are length-prefixed with 16 bits and the column count carries a flag
  // bit).
  [[nodiscard]] Status CheckSerializable() const;
  size_t Serialize(char* dst) const;
  [[nodiscard]] StatusOr<size_t> TryDeserialize(const char* src,
                                                const Schema& sc);
  [[nodiscard]] StatusOr<size_t> TryDeserializeProjected(
      const char* src, const Schema& sc, const std::vector<slot_t>& columns);
  // EXC-SHIM: deprecated throwing wrappers (common/exc_shim.hpp).
  size_t Deserialize(const char* src, const Schema& sc) {
    return ExcShimUnwrap(TryDeserialize(src, sc), "Row::Deserialize");
  }
  size_t DeserializeProjected(const char* src, const Schema& sc,
                              const std::vector<slot_t>& columns) {
    return ExcShimUnwrap(TryDeserializeProjected(src, sc, columns),
                         "Row::DeserializeProjected");
  }
  // Read a single INT64/DATE column without materializing other values.
  [[nodiscard]] static std::optional<int64_t> TryPeekInteger(const char* src,
                                                             const Schema& sc,
                                                             slot_t column);
  [[nodiscard]] size_t Size() const;
  [[nodiscard]] std::string ToString() const;
  [[nodiscard]] StatusOr<std::string> TryEncodeMemcomparableFormat() const;
  [[nodiscard]] Status TryDecodeMemcomparableFormat(std::string_view src);
  // EXC-SHIM: deprecated throwing wrappers (common/exc_shim.hpp).
  [[nodiscard]] std::string EncodeMemcomparableFormat() const {
    return ExcShimUnwrap(TryEncodeMemcomparableFormat(),
                         "Row::EncodeMemcomparableFormat");
  }
  void DecodeMemcomparableFormat(std::string_view src) {
    const Status status = TryDecodeMemcomparableFormat(src);
    if (status != Status::kSuccess) {
      detail::ExcShimThrow("Row::DecodeMemcomparableFormat", status);
    }
  }
  void Clear() { values_.clear(); }
  [[nodiscard]] bool IsValid() const { return !values_.empty(); }
  [[nodiscard]] Row Extract(const std::vector<slot_t>& elms) const;
  Row operator+(const Row& rhs) const;

  bool operator==(const Row& rhs) const = default;
  bool operator!=(const Row& rhs) const { return !operator==(rhs); }
  friend std::ostream& operator<<(std::ostream& o, const Row& r);
  friend Encoder& operator<<(Encoder& e, const Row& r);
  friend Decoder& operator>>(Decoder& d, Row& r);

  std::vector<Value> values_;
};

}  // namespace tinylamb

namespace std {
template <>
class hash<tinylamb::Row> {
 public:
  uint64_t operator()(const tinylamb::Row& row) const;
};

}  // namespace std

#endif  // TINYLAMB_ROW_HPP
