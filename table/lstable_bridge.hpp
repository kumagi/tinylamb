/**
 * Copyright 2026 KUMAZAKI Hiroki
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

#ifndef TINYLAMB_LSTABLE_BRIDGE_HPP
#define TINYLAMB_LSTABLE_BRIDGE_HPP

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/constants.hpp"
#include "common/platform/endian.hpp"
#include "common/platform/file_io.hpp"
#include "common/runtime_filter.hpp"
#include "page/row_position.hpp"
#include "table/fragment/fragment.hpp"
#include "table/full_scan_iterator.hpp"
#include "table/pax_chunk/pax_chunk.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

class Table;
class Transaction;

struct LSTableColumnDictionary {
  static constexpr uint8_t kNullCode = 0xFFU;
  static constexpr size_t kMaxEntries = 254;
  bool active{false};
  // Wide dictionaries keep active false: 8-bit kernels use 256-entry masks.
  uint8_t wide_code_bytes{0};
  // Wide codes may use their exact bit width instead of the logical
  // 2/4-byte lane width. NULL uses the dictionary entry count.
  uint8_t packed_code_bits{0};
  // True only after every distinct byte string has been checked against
  // Value equality. Interval-equivalent spellings otherwise remain peers.
  bool bytewise_equality{false};
  bool all_single_byte{false};
  ValueType value_type{ValueType::kVarChar};
  bool all_canonical_utc_timestamps{false};
  size_t packed_code_count{0};
  // Own the 8-bit stream once per column; page blocks borrow subspans.
  std::vector<uint8_t> narrow_codes;
  // Wide code streams follow physical block/row order. Page blocks keep
  // offsets into this owner. Packed streams are little-endian, with seven
  // zero padding bytes for a safe unaligned 64-bit read at the final row.
  std::vector<uint8_t> wide_codes;
  std::vector<std::string> entries;
  std::vector<int64_t> int_entries;
  std::vector<double> double_entries;
  std::vector<double> one_minus_double_entries;
  std::vector<double> one_plus_double_entries;
  // Zero-copy views for persistent memory-mapped `.tlcs` stores.
  std::span<const uint8_t> narrow_codes_view{};
  std::span<const uint8_t> wide_codes_view{};
  std::span<const uint64_t> flat_str_offsets{};
  std::span<const char> flat_str_bytes{};
  std::span<const int64_t> int_entries_view{};
  std::span<const double> double_entries_view{};
  std::span<const uint8_t> utc_minute_of_hour_view{};
  std::span<const uint32_t> utc_minute_bucket_view{};
  std::span<const uint32_t> utc_minute_exemplar_code_view{};
  std::array<uint8_t, 256> single_byte_chars{};

  [[nodiscard]] std::span<const uint8_t> NarrowCodes() const noexcept {
    return !narrow_codes_view.empty() ? narrow_codes_view
                                      : std::span<const uint8_t>(narrow_codes);
  }

  [[nodiscard]] std::span<const uint8_t> WideCodes() const noexcept {
    return !wide_codes_view.empty() ? wide_codes_view
                                    : std::span<const uint8_t>(wide_codes);
  }

  [[nodiscard]] size_t StringEntryCount() const noexcept {
    return !flat_str_offsets.empty() ? (flat_str_offsets.size() - 1)
                                     : entries.size();
  }

  [[nodiscard]] std::string_view StringEntryAt(size_t code) const noexcept {
    if (!flat_str_offsets.empty()) {
      if (code + 1 >= flat_str_offsets.size()) {
        return {};
      }
      const uint64_t b = flat_str_offsets[code];
      const uint64_t e = flat_str_offsets[code + 1];
      return {flat_str_bytes.data() + b, static_cast<size_t>(e - b)};
    }
    return code < entries.size() ? std::string_view(entries[code])
                                 : std::string_view{};
  }

  [[nodiscard]] std::span<const int64_t> IntEntries() const noexcept {
    return !int_entries_view.empty() ? int_entries_view
                                     : std::span<const int64_t>(int_entries);
  }

  [[nodiscard]] size_t IntEntryCount() const noexcept {
    return IntEntries().size();
  }

  [[nodiscard]] int64_t IntEntryAt(size_t code) const noexcept {
    return IntEntries()[code];
  }

  [[nodiscard]] std::span<const double> DoubleEntries() const noexcept {
    return !double_entries_view.empty()
               ? double_entries_view
               : std::span<const double>(double_entries);
  }

  [[nodiscard]] size_t DoubleEntryCount() const noexcept {
    return DoubleEntries().size();
  }

  [[nodiscard]] size_t DictionaryCodeCount() const noexcept {
    if (active) {
      return NarrowCodes().size();
    }
    const auto wc = WideCodes();
    return packed_code_bits != 0  ? packed_code_count
           : wide_code_bytes != 0 ? wc.size() / wide_code_bytes
                                  : 0;
  }

  [[nodiscard]] uint32_t DictionaryCodeAt(size_t row) const noexcept {
    if (row >= DictionaryCodeCount()) {
      return UINT32_MAX;
    }
    if (active) {
      return NarrowCodes()[row];
    }
    const auto wc = WideCodes();
    if (packed_code_bits != 0) {
      const size_t bit = row * packed_code_bits;
      uint64_t word = 0;
      std::memcpy(&word, wc.data() + (bit / 8), sizeof(word));
      return static_cast<uint32_t>((le64toh(word) >> (bit % 8)) &
                                   ((uint64_t{1} << packed_code_bits) - 1));
    }
    const auto* data = wc.data() + (row * wide_code_bytes);
    if (wide_code_bytes == 2) {
      uint16_t code = 0;
      std::memcpy(&code, data, sizeof(code));
      return code;
    }
    uint32_t code = 0;
    std::memcpy(&code, data, sizeof(code));
    return code;
  }
};

struct LSTableColumnBlock {
  ValueType type{ValueType::kNull};
  bool is_unsigned{false};
  bool has_null{false};
  bool has_min_max{false};
  bool has_double_min_max{false};
  bool has_nan{false};
  bool all_single_byte_varchar{false};
  uint8_t for_width{0};
  int64_t for_base{0};
  int64_t min_int{std::numeric_limits<int64_t>::max()};
  int64_t max_int{std::numeric_limits<int64_t>::min()};
  double min_double{std::numeric_limits<double>::infinity()};
  double max_double{-std::numeric_limits<double>::infinity()};
  const LSTableColumnDictionary* dict{nullptr};
  size_t wide_code_begin{0};
  size_t wide_code_count{0};
  std::array<uint64_t, 4> dict_mask{};
  std::span<const uint8_t> dict_codes;
  std::vector<uint16_t> for16_codes;
  std::span<const int64_t> int_cells;
  std::vector<uint8_t> varchar_bytes;
  std::vector<uint32_t> varchar_ends;
  std::vector<uint8_t> nulls;
  // Zero-copy views and per-granule synopses for `.tlcs` blocks.
  bool is_const_code{false};
  bool has_code_min_max{false};
  uint32_t const_code{UINT32_MAX};
  uint32_t min_code{UINT32_MAX};
  uint32_t max_code{0};
  uint32_t min_nonzero_code{UINT32_MAX};
  std::span<const uint8_t> for_bytes{};
  std::span<const uint8_t> nulls_view{};
  std::span<const uint8_t> varchar_bytes_view{};
  std::span<const uint32_t> varchar_ends_view{};
  std::span<const uint64_t> block_bloom{};

  [[nodiscard]] bool IsNull(size_t row) const noexcept {
    if (!has_null) {
      return false;
    }
    const std::span<const uint8_t> n =
        !nulls_view.empty() ? nulls_view : std::span<const uint8_t>(nulls);
    return row < n.size() && n[row] != 0;
  }

  [[nodiscard]] bool HasRawIntCells() const noexcept {
    return !int_cells.empty();
  }

  [[nodiscard]] int64_t IntAt(size_t row) const noexcept {
    if (!int_cells.empty()) {
      return int_cells[row];
    }
    switch (for_width) {
      case 0:
        return for_base;
      case 1:
        return static_cast<int64_t>(static_cast<uint64_t>(for_base) +
                                    for_bytes[row]);
      case 2: {
        uint16_t v = 0;
        std::memcpy(&v, for_bytes.data() + (row * 2), sizeof(v));
        return static_cast<int64_t>(static_cast<uint64_t>(for_base) + v);
      }
      case 4: {
        uint32_t v = 0;
        std::memcpy(&v, for_bytes.data() + (row * 4), sizeof(v));
        return static_cast<int64_t>(static_cast<uint64_t>(for_base) + v);
      }
      default:
        return 0;
    }
  }

  void MaterializeIntSlice(size_t r0, size_t count,
                           int64_t* __restrict dst) const noexcept {
    if (!int_cells.empty()) {
      std::memcpy(dst, int_cells.data() + r0, count * sizeof(int64_t));
      return;
    }
    const auto base_u = static_cast<uint64_t>(for_base);
    switch (for_width) {
      case 0:
        for (size_t i = 0; i < count; ++i) {
          dst[i] = for_base;
        }
        return;
      case 1: {
        const uint8_t* __restrict src = for_bytes.data() + r0;
        for (size_t i = 0; i < count; ++i) {
          dst[i] = static_cast<int64_t>(base_u + src[i]);
        }
        return;
      }
      case 2: {
        const auto* __restrict src =
            reinterpret_cast<const uint16_t*>(for_bytes.data()) + r0;
        for (size_t i = 0; i < count; ++i) {
          dst[i] = static_cast<int64_t>(base_u + src[i]);
        }
        return;
      }
      case 4: {
        const auto* __restrict src =
            reinterpret_cast<const uint32_t*>(for_bytes.data()) + r0;
        for (size_t i = 0; i < count; ++i) {
          dst[i] = static_cast<int64_t>(base_u + src[i]);
        }
        return;
      }
      default:
        std::memset(dst, 0, count * sizeof(int64_t));
        return;
    }
  }

  [[nodiscard]] bool BlockBloomMayContain(int64_t key) const noexcept {
    if (block_bloom.empty()) {
      return true;
    }
    const size_t mask = (block_bloom.size() * 64) - 1;
    auto z = static_cast<uint64_t>(key);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    z ^= z >> 31;
    const size_t bit0 = static_cast<size_t>(z) & mask;
    const size_t bit1 = static_cast<size_t>(z + (std::rotl(z, 32) | 1U)) & mask;
    return (block_bloom[bit0 >> 6] & (uint64_t{1} << (bit0 & 63))) != 0 &&
           (block_bloom[bit1 >> 6] & (uint64_t{1} << (bit1 & 63))) != 0;
  }

  [[nodiscard]] std::string_view VarcharAt(size_t row) const noexcept {
    if (dict != nullptr && dict->wide_code_bytes != 0) {
      const uint32_t code = is_const_code ? const_code : DictionaryCodeAt(row);
      return code < dict->StringEntryCount() ? dict->StringEntryAt(code)
                                             : std::string_view{};
    }
    if (dict != nullptr && dict->active) {
      if (is_const_code) {
        return const_code < dict->StringEntryCount()
                   ? dict->StringEntryAt(const_code)
                   : std::string_view{};
      }
      if (row >= dict_codes.size()) {
        return {};
      }
      const uint8_t code = dict_codes[row];
      if (code < dict->StringEntryCount()) {
        return dict->StringEntryAt(code);
      }
      return {};
    }
    const std::span<const uint32_t> ends =
        !varchar_ends_view.empty() ? varchar_ends_view
                                   : std::span<const uint32_t>(varchar_ends);
    const std::span<const uint8_t> bytes =
        !varchar_bytes_view.empty() ? varchar_bytes_view
                                    : std::span<const uint8_t>(varchar_bytes);
    if (row >= ends.size()) {
      return {};
    }
    const uint32_t begin = row == 0 ? 0U : ends[row - 1];
    const uint32_t end = ends[row];
    if (end < begin || end > bytes.size()) {
      return {};
    }
    return {reinterpret_cast<const char*>(bytes.data()) + begin, end - begin};
  }

  [[nodiscard]] uint32_t DictionaryCodeAt(size_t row) const noexcept {
    if (is_const_code) {
      return const_code;
    }
    if (dict == nullptr || row >= wide_code_count) {
      return UINT32_MAX;
    }
    return dict->DictionaryCodeAt(wide_code_begin + row);
  }
};

// Indexed by the original table column number, with storage only for loaded
// columns. The optional offset map belongs to the enclosing immutable store,
// just like dictionary pointers and numeric spans in LSTableColumnBlock.
class LSTableBlockColumns {
 public:
  void resize(size_t count) {
    offsets_ = {};
    values_.resize(count);
  }
  void Project(std::span<const size_t> offsets, size_t count) {
    offsets_ = offsets;
    values_.resize(count);
  }
  [[nodiscard]] size_t size() const {
    return offsets_.empty() ? values_.size() : offsets_.size();
  }
  LSTableColumnBlock& operator[](size_t column) {
    return offsets_.empty() ? values_[column] : values_.at(offsets_[column]);
  }
  const LSTableColumnBlock& operator[](size_t column) const {
    return offsets_.empty() ? values_[column] : values_.at(offsets_[column]);
  }
  [[nodiscard]] std::span<LSTableColumnBlock> Stored() { return values_; }
  [[nodiscard]] std::span<const LSTableColumnBlock> Stored() const {
    return values_;
  }

 private:
  std::span<const size_t> offsets_{};
  std::vector<LSTableColumnBlock> values_;
};

struct LSTablePageBlock {
  page_id_t page_id{0};
  lsn_t page_lsn{0};
  uint32_t row_count{0};
  bool has_any_null{false};
  bool identity_slots{false};
  std::vector<slot_t> slots;
  LSTableBlockColumns columns;

  [[nodiscard]] slot_t SlotAt(uint32_t row) const noexcept {
    return identity_slots ? static_cast<slot_t>(row) : slots[row];
  }

  [[nodiscard]] bool MatchesZoneMaps(
      const std::vector<IntegerPeekCompare>* peek_compares,
      const RuntimeFilterGroup* runtime_filter,
      std::optional<slot_t> runtime_filter_column) const noexcept;
};

struct LSTableStore {
  static constexpr uint64_t kBuildByteLimit = uint64_t{4} << 30;
  static constexpr uint32_t kInvalidBlockIndex =
      std::numeric_limits<uint32_t>::max();
  uint64_t layout_generation{0};
  page_id_t first_pid{0};
  txn_id_t uncommitted_owner_txn{0};
  uint64_t total_rows{0};
  std::vector<page_id_t> page_ids;
  std::vector<LSTablePageBlock> blocks;
  std::vector<uint32_t> flat_page_to_block;
  std::unordered_map<page_id_t, size_t> page_to_block;
  std::vector<LSTableColumnDictionary> dictionaries;
  // Numeric cells are owned once per column in physical row order. Page
  // blocks borrow slices only after construction has finished growing these
  // buffers. VARCHAR columns have empty entries here.
  std::vector<std::vector<int64_t>> integer_columns;
  // Empty means every column. Partial stores retain original column indexes
  // and are published only in the separate, single-entry projection cache.
  std::vector<uint8_t> loaded_columns;
  std::vector<size_t> column_offsets;
  [[nodiscard]] bool HasColumn(slot_t column) const {
    return loaded_columns.empty() ||
           (column < loaded_columns.size() && loaded_columns[column] != 0);
  }
  // Immutable column membership synopses, valid under the same generation
  // and snapshot checks as the encoded blocks. A positive probe is only a
  // candidate; it never substitutes for the row predicate.
  std::vector<std::shared_ptr<const BloomRuntimeFilter>> integer_membership;
  std::vector<uint8_t> column_is_monotonic;
  std::vector<uint8_t> column_is_unique_nonnull;
  std::vector<uint8_t> column_is_identity_1_to_n;
  std::vector<uint8_t> column_has_null;
  std::vector<int64_t> column_min_int;
  std::vector<int64_t> column_max_int;
  std::optional<Schema> lstable_schema;
  // The SQL schema had a UINT* column. lstable_schema re-creates columns as
  // plain INT64 (no unsigned flag), so ValidateFragment's unsigned refusal
  // cannot see it there; ExecuteFragment checks this flag instead.
  bool has_unsigned_column{false};
  std::shared_ptr<const PaxChunk> pax_chunk;
  bool is_persistent_tlcs{false};
  std::shared_ptr<const platform::MemoryMap> backing_mmap;
  std::string_view serialized_column_stats;

  [[nodiscard]] StatusOr<ExecResult> ExecuteFragment(
      const Fragment& fragment, Runtime runtime = Runtime::kNative) const;

  [[nodiscard]] const LSTablePageBlock* FindBlock(
      page_id_t pid) const noexcept {
    if (pid < flat_page_to_block.size()) {
      const uint32_t idx = flat_page_to_block[pid];
      return idx < blocks.size() ? &blocks[idx] : nullptr;
    }
    const auto it = page_to_block.find(pid);
    if (it == page_to_block.end() || it->second >= blocks.size()) {
      return nullptr;
    }
    return &blocks[it->second];
  }

  [[nodiscard]] static bool SupportsSchema(const Schema& schema) noexcept;

  [[nodiscard]] bool MayContainInteger(slot_t column, int64_t value) const {
    if (column >= integer_membership.size()) {
      return true;
    }
    const auto& membership = integer_membership[column];
    return !membership || !membership->Reject(value);
  }
};

[[nodiscard]] std::shared_ptr<const LSTableStore> BuildLSTableStore(
    Transaction& txn, const Table& table,
    const std::vector<slot_t>* projection = nullptr,
    uint64_t max_bytes = LSTableStore::kBuildByteLimit);

[[nodiscard]] std::shared_ptr<const LSTableStore> GetOrBuildLSTableStore(
    Transaction& txn, const Table& table, bool allow_lazy_build,
    const std::vector<slot_t>* projection = nullptr);

}  // namespace tinylamb

#endif  // TINYLAMB_LSTABLE_BRIDGE_HPP
