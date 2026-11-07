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
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/constants.hpp"
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
  bool all_single_byte{false};
  ValueType value_type{ValueType::kVarChar};
  std::vector<std::string> entries;
  std::vector<int64_t> int_entries;
  std::vector<double> double_entries;
  std::vector<double> one_minus_double_entries;
  std::vector<double> one_plus_double_entries;
  std::array<uint8_t, 256> single_byte_chars{};
};

struct LSTableColumnBlock {
  ValueType type{ValueType::kNull};
  bool is_unsigned{false};
  bool has_null{false};
  bool has_min_max{false};
  bool has_double_min_max{false};
  bool all_single_byte_varchar{false};
  uint8_t for_width{0};
  int64_t for_base{0};
  int64_t min_int{std::numeric_limits<int64_t>::max()};
  int64_t max_int{std::numeric_limits<int64_t>::min()};
  double min_double{std::numeric_limits<double>::infinity()};
  double max_double{-std::numeric_limits<double>::infinity()};
  const LSTableColumnDictionary* dict{nullptr};
  std::array<uint64_t, 4> dict_mask{};
  std::vector<uint8_t> dict_codes;
  std::vector<uint16_t> for16_codes;
  std::vector<int64_t> int_cells;
  std::vector<uint8_t> varchar_bytes;
  std::vector<uint32_t> varchar_ends;
  std::vector<uint8_t> nulls;

  [[nodiscard]] bool IsNull(size_t row) const noexcept {
    return has_null && row < nulls.size() && nulls[row] != 0;
  }

  [[nodiscard]] std::string_view VarcharAt(size_t row) const noexcept {
    if (dict != nullptr && dict->active && row < dict_codes.size()) {
      const uint8_t code = dict_codes[row];
      if (code < dict->entries.size()) {
        return dict->entries[code];
      }
      return {};
    }
    const uint32_t begin = row == 0 ? 0U : varchar_ends[row - 1];
    const uint32_t end = varchar_ends[row];
    return {reinterpret_cast<const char*>(varchar_bytes.data()) + begin,
            end - begin};
  }
};

struct LSTablePageBlock {
  page_id_t page_id{0};
  lsn_t page_lsn{0};
  uint32_t row_count{0};
  bool has_any_null{false};
  std::vector<slot_t> slots;
  std::vector<LSTableColumnBlock> columns;

  [[nodiscard]] bool MatchesZoneMaps(
      const std::vector<IntegerPeekCompare>* peek_compares,
      const RuntimeFilterGroup* runtime_filter,
      std::optional<slot_t> runtime_filter_column) const noexcept;
};

struct LSTableStore {
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
  std::vector<uint8_t> column_is_monotonic;
  std::vector<uint8_t> column_is_unique_nonnull;
  std::vector<uint8_t> column_is_identity_1_to_n;
  std::vector<uint8_t> column_has_null;
  std::vector<int64_t> column_min_int;
  std::vector<int64_t> column_max_int;
  std::optional<Schema> lstable_schema;
  std::shared_ptr<const PaxChunk> pax_chunk;

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
};

[[nodiscard]] std::shared_ptr<const LSTableStore> BuildLSTableStore(
    Transaction& txn, const Table& table);

[[nodiscard]] std::shared_ptr<const LSTableStore> GetOrBuildLSTableStore(
    Transaction& txn, const Table& table, bool allow_lazy_build);

}  // namespace tinylamb

#endif  // TINYLAMB_LSTABLE_BRIDGE_HPP
