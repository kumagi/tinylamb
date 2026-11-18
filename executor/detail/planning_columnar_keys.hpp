/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#pragma once

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "executor/detail/columnar_table.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"

namespace tinylamb::relational_detail {

// Pack the build's actual signed integer/date ranges into at most 63 bits.
// The sign bit remains an out-of-domain sentinel; no valid key can alias it.
// This is injective over the ranges, so duplicate chains keep exact equality.
struct PackedJoinRanges {
  std::array<int64_t, CompositeIntKey::kMaxParts> minima{};
  std::array<uint64_t, CompositeIntKey::kMaxParts> spans{};
  std::array<unsigned, CompositeIntKey::kMaxParts> shifts{};

  [[nodiscard]] int64_t Encode(const Row& row,
                               const std::vector<slot_t>& columns,
                               const std::vector<int64_t>& offsets) const;
};

std::optional<PackedJoinRanges> BuildPackedJoinRanges(
    const Relation& build, const std::vector<slot_t>& columns,
    const std::vector<int64_t>& offsets);

void AppendBigEndian(std::string* output, uint64_t value, size_t width);

void AppendMemcomparableInteger(int64_t input, uint8_t prefix, bool flip_sign,
                                std::string* output);

struct BorrowedJoinKey {
  const Row* row{nullptr};
  const std::vector<slot_t>* columns{nullptr};
  const std::vector<int64_t>* biases{nullptr};

  bool operator==(const BorrowedJoinKey& other) const;
};

inline uint64_t MixEstimateHash64(uint64_t x) noexcept {
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  return x ^ (x >> 31);
}

struct BorrowedJoinKeyHash {
  size_t operator()(const BorrowedJoinKey& key) const;
};

bool CanBorrowJoinKeys(const Relation& a, const Relation& b,
                       const std::vector<slot_t>& a_columns,
                       const std::vector<slot_t>& b_columns,
                       const std::vector<int64_t>& a_biases,
                       const std::vector<int64_t>& b_biases);

void AppendMemcomparableVarChar(std::string_view input, std::string* output);
void AppendMemcomparableDouble(double d, std::string* output);
void AppendMemcomparableValue(const Value& value, std::string* output);

std::string EncodeJoinKeyInternal(const Row& row,
                                  const std::vector<slot_t>& columns,
                                  bool null_safe, bool fold_zero,
                                  const std::vector<int64_t>* offsets);

std::string EncodeNullSafeJoinKey(const Row& row,
                                  const std::vector<slot_t>& columns);

// Same bytes as EncodeNullSafeJoinKey, written into a caller-owned scratch
// (cleared first, capacity reused) so hot join paths need not allocate a
// temporary std::string for every partition/lookup key.
void EncodeNullSafeJoinKeyInto(const Row& row,
                               const std::vector<slot_t>& columns,
                               std::string* out);

bool EligibleHashedEstimateSchema(const Schema& left_schema,
                                  const std::vector<slot_t>& left_columns,
                                  const Schema& right_schema,
                                  const std::vector<slot_t>& right_columns);

std::optional<int64_t> HashEstimateKeyTuple(
    const Row& row, const std::vector<slot_t>& columns) noexcept;

template <typename Fn>
void ForEachStridedColumnarPos(const ColumnarTable& tbl, size_t stride,
                               Fn&& fn) {
  size_t base = 0;
  size_t next = 0;
  for (const ColumnarChunk& chunk : tbl.chunks) {
    const size_t chunk_end = base + chunk.rows;
    while (next < chunk_end) {
      fn(chunk, next - base);
      next += stride;
    }
    base = chunk_end;
  }
}

bool ColumnarHasNullOrNaN(const ColumnarChunk& chunk,
                          std::span<const slot_t> columns, size_t r,
                          Value* scratch);

std::optional<int64_t> ColumnarHashEstimateKeyTuple(
    const ColumnarChunk& chunk, std::span<const slot_t> columns, size_t r,
    Value* scratch) noexcept;

CompositeIntKey ColumnarEncodeCompositeIntKey(
    const ColumnarChunk& chunk, std::span<const slot_t> columns, size_t r,
    const std::vector<int64_t>* offsets);

void ColumnarEncodeJoinKeyInternal(const ColumnarChunk& chunk,
                                   std::span<const slot_t> columns, size_t r,
                                   bool fold_zero,
                                   const std::vector<int64_t>* offsets,
                                   Value* scratch, std::string* output);

bool TryColumnarEncodeJoinKeyNonNull(const ColumnarChunk& chunk,
                                     std::span<const slot_t> columns, size_t r,
                                     bool fold_zero,
                                     const std::vector<int64_t>* offsets,
                                     Value* scratch, std::string* output);

}  // namespace tinylamb::relational_detail
