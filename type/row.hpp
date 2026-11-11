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

// Precomputed projected-decode plan: per stored column, its type and the
// output slot it lands in (or none).  Everything the decode loop needs, so a
// scan with a fixed projection stops re-deriving it per row (the ascending
// projection guard, schema column lookups, projection cursor).  Built once
// per scan iterator; malformed (non-ascending / out-of-range) projections
// have no plan and keep the legacy per-row path.
struct RowDecodePlan {
  static constexpr size_t kNoSlot = static_cast<size_t>(-1);
  slot_t stored_count{0};
  size_t kept_count{0};
  std::vector<ValueType> stored_types;  // per stored column
  std::vector<size_t> stored_slot;     // per stored column: kept slot / kNoSlot
  std::vector<uint8_t> slot_unsigned;  // per kept slot
  // Plan-shape flags the decode loop reads once per row instead of
  // re-deriving per cell.  all_fixed: every stored column has a fixed-width
  // image; has_unsigned: any kept slot applies the unsigned tag (the per-cell
  // check only runs when it can fire).
  bool all_fixed{false};
  bool has_unsigned{false};
};

struct Row {
  Row() = default;
  Row(std::initializer_list<Value> v) : values_(v) {}
  explicit Row(std::vector<Value> v) : values_(std::move(v)) {}
  Row(const Row&) = default;
  Row(Row&&) = default;
  Row& operator=(const Row&) = default;
  Row& operator=(Row&&) = default;
  ~Row() = default;

  // Hot path: cell access is on every scan/join/aggregate row loop, so it
  // stays inline in the header.
  Value& operator[](size_t i) { return values_[i]; }
  const Value& operator[](size_t i) const { return values_[i]; }
  // Number of cells (bounds checks want this, never the serialized byte
  // size returned by Size()).
  [[nodiscard]] size_t Count() const { return values_.size(); }
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
  // Projected decode against a precomputed RowDecodePlan (same image
  // contract and slot-reuse behavior as TryDeserializeProjected).
  [[nodiscard]] StatusOr<size_t> TryDeserializePlanned(
      const char* src, const RowDecodePlan& plan);
  // EXC-SHIM: deprecated throwing wrappers (common/exc_shim.hpp).
  size_t Deserialize(const char* src, const Schema& sc) {
    return ExcShimUnwrap(TryDeserialize(src, sc), "Row::Deserialize");
  }
  size_t DeserializeProjected(const char* src, const Schema& sc,
                              const std::vector<slot_t>& columns) {
    return ExcShimUnwrap(TryDeserializeProjected(src, sc, columns),
                         "Row::DeserializeProjected");
  }
  size_t DeserializePlanned(const char* src, const RowDecodePlan& plan) {
    return ExcShimUnwrap(TryDeserializePlanned(src, plan),
                         "Row::DeserializePlanned");
  }
  size_t DeserializeBounded(const char* src, size_t max_len, const Schema& sc);
  [[nodiscard]] static size_t PeekSerializedSize(const char* src,
                                                 size_t max_len,
                                                 const Schema& sc);
  // Read a single INT64/DATE column without materializing other values.
  // NULL/non-integer values return nullopt; malformed images return Status.
  // As with TryDeserialize, src must address a complete serialized row image.
  [[nodiscard]] static StatusOr<std::optional<int64_t>> TryPeekInteger(
      const char* src, const Schema& sc, slot_t column);
  // Serialized byte size of the row image (count + optional null bitmap +
  // payload).  Bounds checks over cell indexes want Count(), never this.
  [[nodiscard]] size_t Size() const;
  [[nodiscard]] std::string ToString() const;
  [[nodiscard]] StatusOr<std::string> TryEncodeMemcomparableFormat() const;
  [[nodiscard]] Status TryDecodeMemcomparableFormat(std::string_view src);
  // EXC-SHIM: deprecated throwing wrappers (common/exc_shim.hpp).
  [[nodiscard]] std::string EncodeMemcomparableFormat() const {
    return ExcShimUnwrap(TryEncodeMemcomparableFormat(),
                         "Row::EncodeMemcomparableFormat");
  }
  [[nodiscard]] std::string EncodeKeyMemcomparableFormat(
      const Schema& sc) const;
  size_t EncodeKeyMemcomparableFormatTo(const Schema& sc, char* dst) const;
  void DecodeMemcomparableFormat(std::string_view src) {
    const Status status = TryDecodeMemcomparableFormat(src);
    if (status != Status::kSuccess) {
      detail::ExcShimThrow("Row::DecodeMemcomparableFormat", status);
    }
  }
  void DecodeMemcomparableFormatBounded(std::string_view src) {
    DecodeMemcomparableFormat(src);
  }
  void Clear() { values_.clear(); }
  [[nodiscard]] bool IsValid() const { return !values_.empty(); }
  [[nodiscard]] StatusOr<Row> TryExtract(const std::vector<slot_t>& elms) const;
  // Legacy throwing wrapper. Storage/executor callers use TryExtract.
  [[nodiscard]] Row Extract(const std::vector<slot_t>& elms) const {
    return ExcShimUnwrap(TryExtract(elms), "Row::Extract");
  }
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
