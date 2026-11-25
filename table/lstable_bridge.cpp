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

#include "table/lstable_bridge.hpp"

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstring>
#include <exception>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/log_message.hpp"
#include "common/memory_budget.hpp"
#include "common/serdes.hpp"
#include "page/page.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "page/row_page.hpp"
#include "table/memtable/memtable.hpp"
#include "table/pax_chunk/pax_chunk.hpp"
#include "table/sorted_run/sorted_run.hpp"
#include "table/table.hpp"
#include "table/tlcs_file.hpp"
#include "transaction/transaction.hpp"
#include "type/collation.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/constraint.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

namespace {

// Storage is always big-endian (common/serdes.hpp applies the swap on every
// host). Conditional swapping here made big-endian builds decode every cell
// byte-reversed, silently corrupting values and zone maps.
#if defined(__GNUC__) || defined(__clang__)
#define TINYLAMB_LST_BSWAP64(x) __builtin_bswap64(x)
#else
inline uint64_t LstSwap64Portable(uint64_t v) {
  v = ((v >> 1) & 0x5555555555555555ULL) | ((v & 0x5555555555555555ULL) << 1);
  v = ((v >> 2) & 0x3333333333333333ULL) | ((v & 0x3333333333333333ULL) << 2);
  v = ((v >> 4) & 0x0F0F0F0F0F0F0F0FULL) | ((v & 0x0F0F0F0F0F0F0F0FULL) << 4);
  return ((v >> 8) & 0x00FF00FF00FF00FFULL) |
         ((v & 0x00FF00FF00FF00FFULL) << 8);
}
#define TINYLAMB_LST_BSWAP64(x) LstSwap64Portable(x)
#endif

std::optional<Schema> ConvertToLSTableSchema(const Schema& schema) {
  if (schema.ColumnCount() == 0) {
    return std::nullopt;
  }
  std::vector<Column> cols;
  cols.reserve(schema.ColumnCount() + 1);
  // Prepend a synthetic unique rowid primary key column so MemTable/SortedRun
  // preserves every duplicate row in physical order without key collisions.
  cols.emplace_back(ColumnName("__rowid"), ValueType::kInt64,
                    Constraint(Constraint::kPrimaryKey));
  for (slot_t i = 0; i < schema.ColumnCount(); ++i) {
    const Column& c = schema.GetColumn(i);
    ValueType vt = ValueType::kNull;
    switch (c.Type()) {
      case ValueType::kInt64:
      case ValueType::kDate:
        vt = ValueType::kInt64;
        break;
      case ValueType::kDouble:
        vt = ValueType::kDouble;
        break;
      case ValueType::kVarChar:
        vt = ValueType::kVarChar;
        break;
      default:
        return std::nullopt;
    }
    cols.emplace_back(ColumnName(std::string(c.Name().name)), vt);
  }
  return Schema(std::string(schema.Name()), std::move(cols));
}

bool DecodeRowImageIntoBlock(std::string_view image, slot_t slot_id,
                             const Schema& schema,
                             std::vector<std::vector<int64_t>>& integer_columns,
                             const std::vector<uint8_t>& loaded_columns,
                             LSTablePageBlock* block) {
  constexpr slot_t kNullBitmapFlag = slot_t{1} << 15;
  // Every DeserializeSlot below (the header count and both VARCHAR lengths)
  // is preceded by an explicit end-of-image bounds check; a short/forged
  // image returns false here rather than feeding decoded junk downstream.
  if (image.size() < sizeof(slot_t)) {
    return false;
  }
  const char* cursor = image.data();
  const char* const end = image.data() + image.size();
  slot_t encoded_count = 0;
  cursor += DeserializeSlot(cursor, &encoded_count);
  const bool has_null = (encoded_count & kNullBitmapFlag) != 0;
  const auto stored_count =
      static_cast<slot_t>(encoded_count & ~kNullBitmapFlag);
  if (stored_count != schema.ColumnCount()) {
    return false;
  }
  const char* bitmap = nullptr;
  if (has_null) {
    const size_t bitmap_bytes = (stored_count + 7) / 8;
    if (std::cmp_less(end - cursor, bitmap_bytes)) {
      return false;
    }
    bitmap = cursor;
    cursor += bitmap_bytes;
  }
  const uint32_t row_idx = block->row_count;
  for (slot_t i = 0; i < stored_count; ++i) {
    const bool is_null =
        has_null && (bitmap[i / 8] & static_cast<char>(1U << (i % 8))) != 0;
    if (!loaded_columns.empty() && loaded_columns[i] == 0) {
      if (!is_null) {
        size_t bytes = sizeof(uint64_t);
        if (schema.GetColumn(i).Type() == ValueType::kVarChar) {
          if (static_cast<size_t>(end - cursor) < sizeof(bin_size_t)) {
            return false;
          }
          bin_size_t length = 0;
          cursor += DeserializeSlot(cursor, &length);
          bytes = length;
        }
        if (std::cmp_less(end - cursor, bytes)) {
          return false;
        }
        cursor += bytes;
      }
      continue;
    }
    LSTableColumnBlock& col = block->columns[i];
    if (is_null) {
      if (!col.has_null) {
        col.has_null = true;
        col.nulls.resize(row_idx, 0);
      }
      col.nulls.push_back(1);
      block->has_any_null = true;
      if (col.type == ValueType::kVarChar) {
        col.varchar_ends.push_back(
            static_cast<uint32_t>(col.varchar_bytes.size()));
      } else {
        integer_columns[i].push_back(0);
      }
      continue;
    }
    if (col.has_null) {
      col.nulls.push_back(0);
    }
    switch (col.type) {
      case ValueType::kInt64:
      case ValueType::kDate:
      case ValueType::kDouble: {
        if (static_cast<size_t>(end - cursor) < sizeof(uint64_t)) {
          return false;
        }
        uint64_t bits = 0;
        std::memcpy(&bits, cursor, sizeof(bits));
        cursor += sizeof(bits);
        const auto val = static_cast<int64_t>(TINYLAMB_LST_BSWAP64(bits));
        integer_columns[i].push_back(val);
        if (col.type == ValueType::kInt64 || col.type == ValueType::kDate) {
          if (!col.has_min_max) {
            col.has_min_max = true;
            col.min_int = val;
            col.max_int = val;
          } else {
            col.min_int = std::min(col.min_int, val);
            col.max_int = std::max(col.max_int, val);
          }
        } else if (col.type == ValueType::kDouble) {
          double d = 0.0;
          std::memcpy(&d, &val, sizeof(d));
          col.has_nan |= std::isnan(d);
          if (!std::isnan(d)) {
            if (!col.has_double_min_max) {
              col.has_double_min_max = true;
              col.min_double = d;
              col.max_double = d;
            } else {
              col.min_double = std::min(col.min_double, d);
              col.max_double = std::max(col.max_double, d);
            }
          }
        }
        break;
      }
      case ValueType::kVarChar: {
        if (static_cast<size_t>(end - cursor) < sizeof(bin_size_t)) {
          return false;
        }
        bin_size_t length = 0;
        cursor += DeserializeSlot(cursor, &length);
        if (std::cmp_less(end - cursor, length)) {
          return false;
        }
        col.varchar_bytes.insert(col.varchar_bytes.end(), cursor,
                                 cursor + length);
        cursor += length;
        col.varchar_ends.push_back(
            static_cast<uint32_t>(col.varchar_bytes.size()));
        break;
      }
      default:
        return false;
    }
  }
  block->slots.push_back(slot_id);
  ++block->row_count;
  return true;
}

// Keep repeated, high-cardinality text shared across page blocks. This is a
// data encoding, built independently of queries, with codes sorted in byte
// order so future equality/range kernels can stay in the code domain.
void BuildWideStringDictionary(LSTableStore* store, slot_t column_index,
                               size_t* summary_budget) {
  auto& dictionary = store->dictionaries[column_index];
  std::unordered_map<std::string_view, uint32_t> codes;
  size_t dictionary_bytes = 0;
  size_t source_bytes = 0;
  for (const auto& block : store->blocks) {
    const auto& column = block.columns[column_index];
    source_bytes += column.varchar_bytes.size() +
                    (column.varchar_ends.size() * sizeof(uint32_t));
  }
  // Let the encoded payload, rather than an arbitrary cardinality, decide
  // whether this representation fits. The source already obeys the store cap.
  const size_t dictionary_limit = source_bytes;
  for (const auto& block : store->blocks) {
    const auto& column = block.columns[column_index];
    for (uint32_t r = 0; r < block.row_count; ++r) {
      if (column.IsNull(r)) {
        continue;
      }
      const auto value = column.VarcharAt(r);
      const auto [unused, inserted] = codes.try_emplace(value, 0);
      if (inserted) {
        dictionary_bytes += sizeof(std::string) + value.size();
        if (codes.size() >= UINT32_MAX || dictionary_bytes > dictionary_limit) {
          return;
        }
      }
    }
  }
  if (codes.empty()) {
    return;
  }
  const uint8_t width = codes.size() < UINT16_MAX ? 2 : 4;
  const auto bits = static_cast<uint8_t>(std::bit_width(codes.size()));
  const uint64_t stream_bytes = ((store->total_rows * bits + 7) / 8) + 7;
  const uint64_t encoded_bytes = dictionary_bytes + stream_bytes;
  // Estimate owned string metadata as well as payload and codes. Any
  // estimated saving is useful: a fixed extra percentage discards compact
  // code-domain execution when the dictionary grows, even though it still
  // occupies less space than the original strings and offsets.
  if (encoded_bytes >= source_bytes) {
    return;
  }
  dictionary.entries.reserve(codes.size());
  for (const auto& [value, unused] : codes) {
    dictionary.entries.emplace_back(value);
  }
  std::ranges::sort(dictionary.entries);
  dictionary.codes_follow_string_order =
      std::adjacent_find(dictionary.entries.begin(), dictionary.entries.end(),
                         [](const auto& a, const auto& b) {
                           return CompareVarCharForOrderBy(a, b, false) >= 0;
                         }) == dictionary.entries.end();
  // The encoding must preserve the original bytes even when Value treats
  // two spellings as equal. Record only a proof that codes can serve as
  // equality keys; no row counts or query results are retained here.
  struct Hash {
    const std::vector<std::string>* entries;
    size_t operator()(uint32_t code) const {
      Value value;
      value.type = ValueType::kVarChar;
      value.value.varchar_value = (*entries)[code];
      return std::hash<Value>()(value);
    }
  };
  struct Equal {
    const std::vector<std::string>* entries;
    bool operator()(uint32_t left, uint32_t right) const {
      Value lhs;
      lhs.type = ValueType::kVarChar;
      lhs.value.varchar_value = (*entries)[left];
      Value rhs;
      rhs.type = ValueType::kVarChar;
      rhs.value.varchar_value = (*entries)[right];
      return lhs == rhs;
    }
  };
  std::unordered_set<uint32_t, Hash, Equal> peers(0, Hash{&dictionary.entries},
                                                  Equal{&dictionary.entries});
  peers.reserve(dictionary.entries.size());
  dictionary.bytewise_equality = true;
  for (size_t i = 0; i < dictionary.entries.size(); ++i) {
    if (!peers.insert(static_cast<uint32_t>(i)).second) {
      dictionary.bytewise_equality = false;
      break;
    }
  }
  // The discovery views point into source blocks. Rebind all map keys to
  // owned dictionary strings before replacing any block's source bytes.
  codes.clear();
  for (size_t i = 0; i < dictionary.entries.size(); ++i) {
    codes.emplace(dictionary.entries[i], static_cast<uint32_t>(i));
  }
  dictionary.wide_code_bytes = width;
  dictionary.packed_code_bits = bits;
  dictionary.packed_code_count = static_cast<size_t>(store->total_rows);
  dictionary.wide_codes.resize(static_cast<size_t>(stream_bytes), 0);
  const uint64_t summary_bytes =
      (codes.size() + 1) *
      (sizeof(int64_t) + sizeof(uint64_t) + sizeof(uint32_t));
  const bool keep_summary =
      store->total_rows <= static_cast<uint64_t>(INT64_MAX) &&
      summary_bytes <= *summary_budget &&
      summary_bytes < source_bytes - encoded_bytes;
  if (keep_summary) {
    *summary_budget -= summary_bytes;
    dictionary.code_counts.resize(codes.size() + 1, 0);
    dictionary.code_first_row.resize(codes.size() + 1, UINT64_MAX);
    dictionary.encounter_codes.reserve(codes.size() + 1);
  }
  size_t row_offset = 0;
  for (auto& block : store->blocks) {
    auto& column = block.columns[column_index];
    for (uint32_t r = 0; r < block.row_count; ++r) {
      const uint32_t code = column.IsNull(r)
                                ? static_cast<uint32_t>(codes.size())
                                : codes.at(column.VarcharAt(r));
      if (keep_summary && dictionary.code_counts[code]++ == 0) {
        dictionary.code_first_row[code] = row_offset + r;
        dictionary.encounter_codes.push_back(code);
      }
      if (!column.IsNull(r)) {
        column.has_code_min_max = true;
        column.min_code = std::min(column.min_code, code);
        column.max_code = std::max(column.max_code, code);
      }
      const size_t bit = (row_offset + r) * bits;
      auto* output = dictionary.wide_codes.data() + (bit / 8);
      uint64_t word = 0;
      std::memcpy(&word, output, sizeof(word));
      word =
          htole64(le64toh(word) | (static_cast<uint64_t>(code) << (bit % 8)));
      std::memcpy(output, &word, sizeof(word));
    }
    column.wide_code_begin = row_offset;
    column.wide_code_count = block.row_count;
    row_offset += block.row_count;
    std::vector<uint8_t>().swap(column.varchar_bytes);
    std::vector<uint32_t>().swap(column.varchar_ends);
    column.all_single_byte_varchar = false;
    column.dict = &dictionary;
  }
}

// A compact code index lets grouping/distinct kernels avoid hashing raw
// integers on each query. Row kernels still need int_cells, so this is an
// additional representation with a per-store budget, not a memory saving.
// All discovery is complete before allocating a code stream. Its owner never
// resizes again, so every page subspan stays valid for the store's lifetime.
void AllocateNarrowCodes(LSTableStore& store, slot_t column) {
  auto& codes = store.dictionaries[column].narrow_codes;
  codes.resize(static_cast<size_t>(store.total_rows));
  size_t begin = 0;
  for (auto& block : store.blocks) {
    auto& encoded = block.columns[column];
    encoded.dict_codes =
        std::span<const uint8_t>(codes).subspan(begin, block.row_count);
    encoded.wide_code_begin = begin;
    encoded.wide_code_count = block.row_count;
    begin += block.row_count;
  }
}

// Prove uniqueness independently of the dictionary's compression budget.
// A bounded sample can disprove it cheaply; only a complete exact sort can
// establish it. The proof belongs to this immutable snapshot's generation.
bool ProveIntegerUniqueness(const LSTableStore& store, slot_t column) {
  constexpr size_t kMaxRows = (size_t{128} << 20) / sizeof(int64_t);
  if (store.total_rows == 0 || store.total_rows > kMaxRows) {
    return false;
  }
  std::vector<int64_t> values;
  values.reserve(std::min<size_t>(4096, store.total_rows));
  for (const auto& block : store.blocks) {
    const auto& cells = block.columns[column].int_cells;
    const size_t count = std::min<size_t>(4096 - values.size(), cells.size());
    values.insert(values.end(), cells.begin(),
                  cells.begin() + static_cast<std::ptrdiff_t>(count));
    if (values.size() == 4096) {
      break;
    }
  }
  std::ranges::sort(values);
  if (std::adjacent_find(values.begin(), values.end()) != values.end()) {
    return false;
  }
  values.clear();
  values.reserve(store.total_rows);
  for (const auto& block : store.blocks) {
    const auto& cells = block.columns[column].int_cells;
    if (cells.size() != block.row_count) {
      return false;
    }
    values.insert(values.end(), cells.begin(), cells.end());
  }
  if (values.size() != store.total_rows) {
    return false;
  }
  std::ranges::sort(values);
  return std::adjacent_find(values.begin(), values.end()) == values.end();
}

void BuildWideIntegerDictionary(LSTableStore* store, slot_t column_index,
                                ValueType type, size_t* remaining_bytes,
                                size_t* summary_budget) {
  std::unordered_map<int64_t, uint32_t> codes;
  for (const auto& block : store->blocks) {
    const auto& column = block.columns[column_index];
    for (size_t row = 0; row < block.row_count; ++row) {
      if (!column.IsNull(row)) {
        codes.try_emplace(column.int_cells[row], 0);
        if (codes.size() >= UINT32_MAX ||
            codes.size() > *remaining_bytes / sizeof(int64_t)) {
          return;
        }
      }
    }
  }
  if (codes.empty()) {
    return;
  }
  // The complete map is an exact proof even when dictionary encoding would
  // expand this column and is discarded below. NULLs were excluded above,
  // so one distinct value per physical row also proves absence of NULLs.
  if (codes.size() == store->total_rows) {
    store->column_is_unique_nonnull[column_index] = 1;
  }
  const uint8_t width = codes.size() < UINT16_MAX ? 2 : 4;
  const auto bits = static_cast<uint8_t>(std::bit_width(codes.size()));
  const uint64_t stream_bytes = ((store->total_rows * bits + 7) / 8) + 7;
  const uint64_t bytes = (codes.size() * sizeof(int64_t)) + stream_bytes;
  const uint64_t raw_bytes = store->total_rows * sizeof(int64_t);
  if (bytes > *remaining_bytes || bytes > raw_bytes - (raw_bytes / 5)) {
    return;
  }
  auto& dictionary = store->dictionaries[column_index];
  dictionary.value_type = type;
  dictionary.int_entries.reserve(codes.size());
  for (const auto& [value, unused] : codes) {
    dictionary.int_entries.push_back(value);
  }
  std::ranges::sort(dictionary.int_entries);
  for (size_t code = 0; code < dictionary.int_entries.size(); ++code) {
    codes[dictionary.int_entries[code]] = static_cast<uint32_t>(code);
  }
  dictionary.wide_code_bytes = width;
  dictionary.packed_code_bits = bits;
  dictionary.packed_code_count = static_cast<size_t>(store->total_rows);
  dictionary.wide_codes.resize(static_cast<size_t>(stream_bytes), 0);
  const uint64_t summary_bytes =
      (codes.size() + 1) *
      (sizeof(int64_t) + sizeof(uint64_t) + sizeof(uint32_t));
  const bool keep_summary =
      store->total_rows <= static_cast<uint64_t>(INT64_MAX) &&
      summary_bytes <= *summary_budget && summary_bytes < raw_bytes - bytes;
  if (keep_summary) {
    *summary_budget -= summary_bytes;
    dictionary.code_counts.resize(codes.size() + 1, 0);
    dictionary.code_first_row.resize(codes.size() + 1, UINT64_MAX);
    dictionary.encounter_codes.reserve(codes.size() + 1);
  }
  size_t row_offset = 0;
  for (auto& block : store->blocks) {
    auto& column = block.columns[column_index];
    for (size_t row = 0; row < block.row_count; ++row) {
      const uint32_t code = column.IsNull(row)
                                ? static_cast<uint32_t>(codes.size())
                                : codes.at(column.int_cells[row]);
      if (keep_summary && dictionary.code_counts[code]++ == 0) {
        dictionary.code_first_row[code] = row_offset + row;
        dictionary.encounter_codes.push_back(code);
      }
      const size_t bit = (row_offset + row) * bits;
      auto* output = dictionary.wide_codes.data() + (bit / 8);
      uint64_t word = 0;
      std::memcpy(&word, output, sizeof(word));
      word =
          htole64(le64toh(word) | (static_cast<uint64_t>(code) << (bit % 8)));
      std::memcpy(output, &word, sizeof(word));
    }
    column.dict = &dictionary;
    column.wide_code_begin = row_offset;
    column.wide_code_count = block.row_count;
    row_offset += block.row_count;
  }
  *remaining_bytes -= static_cast<size_t>(bytes);
}

}  // namespace

namespace {

uint64_t EffectiveBuildByteLimit(uint64_t requested_max_bytes) {
  if (requested_max_bytes != LSTableStore::kBuildByteLimit) {
    return requested_max_bytes;
  }
  const auto& budget = ProcessMemoryBudget::Global();
  const size_t pool_max =
      std::min(budget.ColumnarPoolLimit(),
               budget.EffectivePoolMaxBytes(MemoryPoolKind::kColumnar));
  const uint64_t pool_cap = std::max<uint64_t>(uint64_t{1} << 20, pool_max / 2);
  return std::min<uint64_t>(requested_max_bytes, pool_cap);
}

size_t EstimateDictionaryBytes(const LSTableColumnDictionary& dict) noexcept {
  size_t bytes = dict.narrow_codes.size() + dict.wide_codes.size() +
                 (dict.code_counts.size() * sizeof(int64_t)) +
                 (dict.code_first_row.size() * sizeof(uint64_t)) +
                 (dict.encounter_codes.size() * sizeof(uint32_t)) +
                 (dict.int_entries.size() * sizeof(int64_t)) +
                 (dict.double_entries.size() * sizeof(double)) +
                 (dict.one_minus_double_entries.size() * sizeof(double)) +
                 (dict.one_plus_double_entries.size() * sizeof(double));
  for (const std::string& entry : dict.entries) {
    bytes += sizeof(std::string) + entry.size();
  }
  return bytes;
}

size_t EstimateSecondaryStoreBytes(const LSTableStore& store,
                                   const Schema& schema) noexcept {
  size_t bytes = 0;
  for (const LSTableColumnDictionary& dict : store.dictionaries) {
    bytes += EstimateDictionaryBytes(dict);
  }
  for (const LSTablePageBlock& blk : store.blocks) {
    for (slot_t c = 0; c < schema.ColumnCount(); ++c) {
      if (store.HasColumn(c)) {
        bytes += blk.columns[c].for16_codes.size() * sizeof(uint16_t);
      }
    }
  }
  for (const auto& membership : store.integer_membership) {
    if (membership) {
      bytes += membership->Words().size() * sizeof(uint64_t);
    }
  }
  return bytes;
}

bool CanonicalizeProjection(const std::vector<slot_t>& projection,
                            slot_t col_count, std::vector<uint32_t>* out) {
  out->clear();
  out->reserve(projection.size());
  for (const slot_t c : projection) {
    if (c >= col_count) {
      return false;
    }
    out->push_back(static_cast<uint32_t>(c));
  }
  std::ranges::sort(*out);
  const auto dup = std::ranges::unique(*out);
  out->erase(dup.begin(), dup.end());
  return true;
}

LSTableColumnDictionary MakeDictionaryView(const LSTableColumnDictionary& src) {
  LSTableColumnDictionary out;
  out.active = src.active;
  out.wide_code_bytes = src.wide_code_bytes;
  out.packed_code_bits = src.packed_code_bits;
  out.bytewise_equality = src.bytewise_equality;
  out.codes_follow_string_order = src.codes_follow_string_order;
  out.all_single_byte = src.all_single_byte;
  out.value_type = src.value_type;
  out.all_canonical_utc_timestamps = src.all_canonical_utc_timestamps;
  out.packed_code_count = src.packed_code_count;
  out.single_byte_chars = src.single_byte_chars;

  out.narrow_codes_view = src.NarrowCodes();
  out.wide_codes_view = src.WideCodes();
  out.int_entries_view = src.IntEntries();
  out.double_entries_view = src.DoubleEntries();
  out.flat_str_offsets = src.flat_str_offsets;
  out.flat_str_bytes = src.flat_str_bytes;
  out.utc_minute_of_hour_view = src.utc_minute_of_hour_view;
  out.utc_minute_bucket_view = src.utc_minute_bucket_view;
  out.utc_minute_exemplar_code_view = src.utc_minute_exemplar_code_view;

  out.entries = src.entries;
  out.code_counts = src.code_counts;
  out.code_first_row = src.code_first_row;
  out.encounter_codes = src.encounter_codes;
  out.one_minus_double_entries = src.one_minus_double_entries;
  out.one_plus_double_entries = src.one_plus_double_entries;
  return out;
}

size_t TransferColumnarCharge(const std::shared_ptr<ProcessMemoryCharge>& from,
                              const std::shared_ptr<ProcessMemoryCharge>& to,
                              size_t requested_bytes) {
  if (requested_bytes == 0 || !from || !to) {
    return 0;
  }
  // TransferTo moves the charge in one noexcept step: it cannot throw or fail
  // after the source has been debited (a pool mismatch is rejected up front),
  // so the accounting can never lose the bytes between a Release and a
  // ReserveForced.
  return from->TransferTo(*to, requested_bytes);
}

void ExtractColumnsFromStore(
    LSTableStore& src, const Table& table, uint64_t generation,
    const std::vector<slot_t>& cols,
    std::shared_ptr<const LSTableBlockLayout>* layout_io,
    std::vector<std::shared_ptr<const LSTableColumnCacheEntry>>* cols_io) {
  const Schema& schema = table.GetSchema();
  const size_t nblocks = src.blocks.size();
  // first_pid/owner alone do not identify a layout: a rebuild at the same
  // generation can change the row count, and a stale generation would pair
  // old row counts / page LSNs with the freshly extracted columns.
  if (!*layout_io || (*layout_io)->first_pid != src.first_pid ||
      (*layout_io)->uncommitted_owner_txn != src.uncommitted_owner_txn ||
      (*layout_io)->generation != generation ||
      (*layout_io)->total_rows != src.total_rows) {
    auto layout = std::make_shared<LSTableBlockLayout>();
    layout->generation = generation;
    layout->first_pid = src.first_pid;
    layout->uncommitted_owner_txn = src.uncommitted_owner_txn;
    layout->total_rows = src.total_rows;
    layout->lstable_schema = src.lstable_schema;
    layout->has_unsigned_column = src.has_unsigned_column;
    layout->is_persistent_tlcs = src.is_persistent_tlcs;
    layout->pax_chunk = std::move(src.pax_chunk);
    layout->page_ids = std::move(src.page_ids);
    layout->flat_page_to_block = std::move(src.flat_page_to_block);
    layout->page_to_block = std::move(src.page_to_block);
    layout->block_row_counts.reserve(nblocks);
    layout->block_page_lsns.reserve(nblocks);
    layout->block_identity_slots.reserve(nblocks);
    bool any_non_identity = false;
    for (const LSTablePageBlock& blk : src.blocks) {
      if (!blk.identity_slots) {
        any_non_identity = true;
        break;
      }
    }
    if (any_non_identity) {
      layout->block_slots.resize(nblocks);
    }
    size_t layout_bytes =
        sizeof(LSTableBlockLayout) +
        (layout->page_ids.size() * sizeof(page_id_t)) +
        (layout->flat_page_to_block.size() * sizeof(uint32_t)) +
        (layout->page_to_block.size() * 32ULL) +
        (nblocks * (sizeof(uint32_t) + sizeof(lsn_t) + sizeof(uint8_t)));
    for (size_t bi = 0; bi < nblocks; ++bi) {
      LSTablePageBlock& blk = src.blocks[bi];
      layout->block_row_counts.push_back(blk.row_count);
      layout->block_page_lsns.push_back(blk.page_lsn);
      layout->block_identity_slots.push_back(blk.identity_slots ? 1 : 0);
      if (any_non_identity && !blk.identity_slots) {
        layout_bytes += blk.slots.size() * sizeof(slot_t);
        layout->block_slots[bi] = std::move(blk.slots);
      }
    }
    layout->memory_charge =
        std::make_shared<ProcessMemoryCharge>(MemoryPoolKind::kColumnar);
    TransferColumnarCharge(src.memory_charge, layout->memory_charge,
                           layout_bytes);
    *layout_io = std::move(layout);
    table.RefillLSTableBlockLayout(generation, *layout_io);
  }

  for (const slot_t c : cols) {
    if (c >= schema.ColumnCount() || !src.HasColumn(c)) {
      continue;
    }
    auto entry = std::make_shared<LSTableColumnCacheEntry>();
    entry->generation = generation;
    entry->col_index = c;
    entry->type = schema.GetColumn(c).Type();
    entry->is_unsigned = schema.GetColumn(c).IsUnsigned();
    entry->has_null =
        c < src.column_has_null.size() && src.column_has_null[c] != 0;
    entry->is_monotonic =
        c < src.column_is_monotonic.size() && src.column_is_monotonic[c] != 0;
    entry->is_unique_nonnull = c < src.column_is_unique_nonnull.size() &&
                               src.column_is_unique_nonnull[c] != 0;
    entry->is_identity_1_to_n = c < src.column_is_identity_1_to_n.size() &&
                                src.column_is_identity_1_to_n[c] != 0;
    if (c < src.column_min_int.size()) {
      entry->min_int = src.column_min_int[c];
    }
    if (c < src.column_max_int.size()) {
      entry->max_int = src.column_max_int[c];
    }
    if (c < src.integer_columns.size()) {
      entry->integer_column = std::move(src.integer_columns[c]);
    }
    if (c < src.dictionaries.size()) {
      entry->dictionary = std::move(src.dictionaries[c]);
    }
    if (c < src.integer_membership.size()) {
      entry->integer_membership = std::move(src.integer_membership[c]);
    }

    entry->blocks.resize(nblocks);
    size_t col_bytes =
        sizeof(LSTableColumnCacheEntry) +
        (nblocks * sizeof(LSTableColumnBlock)) +
        (entry->integer_column.size() * sizeof(int64_t)) +
        EstimateDictionaryBytes(entry->dictionary) +
        (entry->integer_membership
             ? entry->integer_membership->Words().size() * sizeof(uint64_t)
             : 0);
    size_t row_base = 0;
    const auto narrow_span = entry->dictionary.NarrowCodes();
    for (size_t bi = 0; bi < nblocks; ++bi) {
      const uint32_t rc = src.blocks[bi].row_count;
      LSTableColumnBlock& col = entry->blocks[bi];
      col = std::move(src.blocks[bi].columns[c]);
      if (col.dict != nullptr) {
        col.dict = &entry->dictionary;
      }
      if (entry->dictionary.active && !col.dict_codes.empty() &&
          row_base + rc <= narrow_span.size()) {
        col.dict_codes = narrow_span.subspan(row_base, rc);
      }
      if (!entry->integer_column.empty() &&
          row_base + rc <= entry->integer_column.size()) {
        col.int_cells = std::span<const int64_t>(entry->integer_column)
                            .subspan(row_base, rc);
      }
      if (!col.for16_codes.empty()) {
        // Reinterpreting object bytes as unsigned char is explicitly allowed
        // by the aliasing rules, and the byte length is scaled from uint16_t
        // to uint8_t, so no alignment/endian assumption is made here (the
        // same pattern is used for the other for16_codes views in this file).
        col.for_bytes = std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(col.for16_codes.data()),
            col.for16_codes.size() * sizeof(uint16_t));
      }
      col_bytes += col.nulls.size() + col.varchar_bytes.size() +
                   (col.varchar_ends.size() * sizeof(uint32_t)) +
                   (col.for16_codes.size() * sizeof(uint16_t));
      row_base += rc;
    }

    entry->estimated_bytes = col_bytes;
    entry->memory_charge =
        std::make_shared<ProcessMemoryCharge>(MemoryPoolKind::kColumnar);
    TransferColumnarCharge(src.memory_charge, entry->memory_charge, col_bytes);
    table.RefillCachedLSTableColumn(c, generation, col_bytes, entry);
    if (c < cols_io->size()) {
      (*cols_io)[c] = std::move(entry);
    }
  }
}

}  // namespace

bool LSTablePageBlock::MatchesZoneMaps(
    const std::vector<IntegerPeekCompare>* peek_compares,
    const RuntimeFilterGroup* runtime_filter,
    std::optional<slot_t> runtime_filter_column) const noexcept {
  if (row_count == 0) {
    return false;
  }
  if (peek_compares != nullptr) {
    for (const IntegerPeekCompare& cmp : *peek_compares) {
      if (cmp.column >= columns.size()) {
        continue;
      }
      const LSTableColumnBlock& col = columns[cmp.column];
      if (col.type != ValueType::kInt64 && col.type != ValueType::kDate) {
        return false;
      }
      // Signed min/max and bloom comparisons cannot prune an unsigned
      // column. Let the row-level predicate evaluate it in the right domain.
      if (col.is_unsigned) {
        continue;
      }
      if (!col.has_min_max) {
        return false;
      }
      switch (cmp.op) {
        case BinaryOperation::kEquals:
          if (cmp.constant < col.min_int || cmp.constant > col.max_int) {
            return false;
          }
          if (!col.BlockBloomMayContain(cmp.constant)) {
            return false;
          }
          break;
        case BinaryOperation::kNotEquals:
          if (col.min_int == cmp.constant && col.max_int == cmp.constant) {
            return false;
          }
          break;
        case BinaryOperation::kLessThan:
          if (col.min_int >= cmp.constant) {
            return false;
          }
          break;
        case BinaryOperation::kLessThanEquals:
          if (col.min_int > cmp.constant) {
            return false;
          }
          break;
        case BinaryOperation::kGreaterThan:
          if (col.max_int <= cmp.constant) {
            return false;
          }
          break;
        case BinaryOperation::kGreaterThanEquals:
          if (col.max_int < cmp.constant) {
            return false;
          }
          break;
        default:
          break;
      }
    }
  }
  if (runtime_filter != nullptr && runtime_filter_column.has_value() &&
      *runtime_filter_column < columns.size()) {
    if (runtime_filter->RejectsAll()) {
      return false;
    }
    const LSTableColumnBlock& col = columns[*runtime_filter_column];
    if (col.type == ValueType::kInt64 || col.type == ValueType::kDate) {
      if (!col.has_min_max) {
        return false;
      }
      if (runtime_filter->RejectRange(col.min_int, col.max_int)) {
        return false;
      }
    }
  }
  return true;
}

std::shared_ptr<const LSTableIntegerRuns> LSTableStore::IntegerRuns(
    slot_t column) const {
  try {
    std::scoped_lock lock(integer_runs_mutex_);
    if (integer_runs_ && integer_runs_->column == column) {
      return integer_runs_;
    }
    const auto keys = IntegerColumn(column);
    if (total_rows == 0 || total_rows > UINT32_MAX ||
        column >= column_is_monotonic.size() ||
        (column_is_monotonic[column] == 0U) ||
        column >= column_has_null.size() || (column_has_null[column] != 0U) ||
        keys.size() != total_rows) {
      return nullptr;
    }
    const uint64_t span = static_cast<uint64_t>(keys.back()) -
                          static_cast<uint64_t>(keys.front()) + 1;
    constexpr size_t max_bytes = size_t{256} << 20;
    if (span == 0 || span >= max_bytes / sizeof(uint32_t) ||
        span > total_rows * 4) {
      return nullptr;
    }
    auto index = std::make_shared<LSTableIntegerRuns>();
    index->memory_charge =
        std::make_shared<ProcessMemoryCharge>(MemoryPoolKind::kColumnar);
    if (!index->memory_charge->TryAddWithReclaim(
            ((span + 1) * sizeof(uint32_t)) + sizeof(LSTableIntegerRuns))) {
      return nullptr;
    }
    index->column = column;
    index->minimum = keys.front();
    index->offsets.resize(span + 1);
    size_t next = 0;
    int64_t previous = keys.front();
    uint32_t run_begin = 0;
    index->run_count = 1;
    for (uint32_t row = 0; row < total_rows; ++row) {
      if (keys[row] < previous) {
        return nullptr;
      }
      if (keys[row] != previous) {
        ++index->run_count;
        index->max_run_rows = std::max(index->max_run_rows, row - run_begin);
        run_begin = row;
      }
      previous = keys[row];
      const uint64_t slot = static_cast<uint64_t>(keys[row]) -
                            static_cast<uint64_t>(index->minimum);
      if (slot >= span) {
        return nullptr;
      }
      if (slot >= next) {
        std::fill(
            index->offsets.begin() + static_cast<std::ptrdiff_t>(next),
            index->offsets.begin() + static_cast<std::ptrdiff_t>(slot + 1),
            row);
        next = slot + 1;
      }
    }
    std::fill(index->offsets.begin() + static_cast<std::ptrdiff_t>(next),
              index->offsets.end(), static_cast<uint32_t>(total_rows));
    index->max_run_rows = std::max(
        index->max_run_rows, static_cast<uint32_t>(total_rows) - run_begin);
    integer_runs_ = std::move(index);
    return integer_runs_;
  } catch (...) {
    // Optional acceleration: allocation failure keeps the ordinary scan.
    return nullptr;
  }
}

std::shared_ptr<const LSTableIntegerRunBounds> LSTableStore::IntegerRunBounds(
    slot_t key_column, slot_t value_column) const {
  try {
    const auto runs = IntegerRuns(key_column);
    const auto cells = IntegerColumn(value_column);
    if (!runs || cells.size() != total_rows ||
        value_column >= column_has_null.size() ||
        (column_has_null[value_column] != 0U)) {
      return nullptr;
    }
    // IntegerRuns() took and released this same latch before returning; no
    // second mutex is held here, so there is no nested acquisition/ordering
    // hazard with the lock in IntegerRuns().
    std::scoped_lock lock(integer_runs_mutex_);
    if (integer_run_bounds_ && integer_run_bounds_->key_column == key_column &&
        integer_run_bounds_->value_column == value_column) {
      return integer_run_bounds_;
    }
    int64_t low = 0, high = 0;
    if (value_column < column_min_int.size() &&
        value_column < column_max_int.size() &&
        column_min_int[value_column] <= column_max_int[value_column]) {
      low = column_min_int[value_column];
      high = column_max_int[value_column];
    } else {
      const auto extrema = std::minmax_element(cells.begin(), cells.end());
      low = *extrema.first;
      high = *extrema.second;
    }
    if (static_cast<uint64_t>(high) - static_cast<uint64_t>(low) > UINT16_MAX) {
      return nullptr;
    }
    const size_t count = runs->offsets.size() - 1;
    constexpr size_t max_bytes = size_t{256} << 20;
    if (count > max_bytes / sizeof(uint32_t)) {
      return nullptr;
    }
    const size_t occupied = runs->run_count;
    const bool sparse = count > 4096 && occupied < count / 2;
    const size_t words = sparse ? (count + 63) / 64 : 0;
    const size_t packed_count = sparse ? occupied : count;
    const size_t bytes = (packed_count * sizeof(uint32_t)) +
                         (words * (sizeof(uint64_t) + sizeof(uint32_t)));
    auto bounds = std::make_shared<LSTableIntegerRunBounds>();
    bounds->memory_charge =
        std::make_shared<ProcessMemoryCharge>(MemoryPoolKind::kColumnar);
    // This optional synopsis must not evict the column image it accelerates
    // or other hot tables, only to rebuild them on every following query.
    bool coarse = false;
    if (!bounds->memory_charge->TryAdd(bytes + sizeof(*bounds))) {
      const size_t coarse_bytes =
          (packed_count * sizeof(uint16_t)) +
          (words * (sizeof(uint64_t) + sizeof(uint32_t)));
      if (!bounds->memory_charge->TryAdd(coarse_bytes + sizeof(*bounds))) {
        return nullptr;
      }
      coarse = true;
    }
    bounds->key_column = key_column;
    bounds->value_column = value_column;
    bounds->key_minimum = runs->minimum;
    bounds->value_minimum = low;
    bounds->value_maximum = high;
    if (coarse) {
      const uint64_t span =
          static_cast<uint64_t>(high) - static_cast<uint64_t>(low);
      bounds->coarse_shift =
          std::bit_width(span) > 8 ? std::bit_width(span) - 8 : 0;
      bounds->coarse.resize(packed_count);
    } else {
      bounds->packed.resize(packed_count);
    }
    bounds->key_bits.resize(words);
    bounds->key_prefix.resize(words);
    size_t packed_row = 0;
    for (size_t i = 0; i < count; ++i) {
      const uint32_t begin = runs->offsets[i], end = runs->offsets[i + 1];
      if (begin == end) {
        continue;
      }
      int64_t lo = cells[begin], hi = lo;
      for (size_t r = begin + 1; r < end; ++r) {
        lo = std::min(lo, cells[r]);
        hi = std::max(hi, cells[r]);
      }
      const auto delta_lo = static_cast<uint32_t>(static_cast<uint64_t>(lo) -
                                                  static_cast<uint64_t>(low));
      const auto delta_hi = static_cast<uint32_t>(static_cast<uint64_t>(hi) -
                                                  static_cast<uint64_t>(low));
      const size_t offset = sparse ? packed_row++ : i;
      if (sparse) {
        bounds->key_bits[i >> 6] |= uint64_t{1} << (i & 63);
      }
      if (coarse) {
        bounds->coarse[offset] = (delta_lo >> bounds->coarse_shift) |
                                 ((delta_hi >> bounds->coarse_shift) << 8);
      } else {
        bounds->packed[offset] = delta_lo | (delta_hi << 16);
      }
    }
    uint32_t prefix = 0;
    for (size_t word = 0; word < words; ++word) {
      bounds->key_prefix[word] = prefix;
      prefix += std::popcount(bounds->key_bits[word]);
    }
    integer_run_bounds_ = std::move(bounds);
    return integer_run_bounds_;
  } catch (...) {
    return nullptr;  // Optional synopsis: keep the ordinary row predicate.
  }
}

bool LSTableStore::SupportsSchema(const Schema& schema) noexcept {
  if (schema.ColumnCount() == 0) {
    return false;
  }
  for (slot_t i = 0; i < schema.ColumnCount(); ++i) {
    if (schema.GetColumn(i).IsInterval()) {
      return false;
    }
    const ValueType t = schema.GetColumn(i).Type();
    if (t != ValueType::kInt64 && t != ValueType::kDate &&
        t != ValueType::kDouble && t != ValueType::kVarChar) {
      return false;
    }
  }
  return true;
}

StatusOr<ExecResult> LSTableStore::ExecuteFragment(const Fragment& fragment,
                                                   Runtime runtime) const {
  // UINT64 has a different memcomparable order than the INT64 cells the
  // chunk stores; decline (as ValidateFragment would for an unsigned
  // schema) instead of comparing 2^63+1 as a negative number.
  if (!lstable_schema.has_value() || pax_chunk == nullptr ||
      pax_chunk->TotalRowCount() != total_rows || has_unsigned_column) {
    ExecResult rejected;
    rejected.status = Status::kNotImplemented;
    rejected.rejected = true;
    rejected.coverage = Coverage::kNone;
    return rejected;
  }
  ArtifactRef ref;
  ref.kind = ArtifactRef::Kind::kChunk;
  ref.chunk = pax_chunk.get();
  ref.generation = layout_generation;
  ref.schema = &(*lstable_schema);
  ArtifactSet artifacts{ref};
  return Execute(runtime, artifacts, fragment, *lstable_schema);
}

// NOLINTNEXTLINE(misc-use-anonymous-namespace): bridge helper is file-local.
static std::shared_ptr<const LSTableStore> BuildLSTableStoreImpl(
    Transaction& txn, const Table& table, const std::vector<slot_t>* projection,
    uint64_t max_bytes, bool cache_in_table,
    std::shared_ptr<LSTableStore>* mutable_out) {
  if (txn.IsFailed() || table.RowsRewrittenInPlace()) {
    return nullptr;
  }
  const uint64_t effective_max_bytes = EffectiveBuildByteLimit(max_bytes);
  const Schema& schema = table.GetSchema();
  if (!LSTableStore::SupportsSchema(schema)) {
    return nullptr;
  }
  const uint64_t generation = table.LayoutGeneration();
  if (generation == 0) {
    return nullptr;
  }
  std::vector<uint8_t> projection_mask;
  const auto& incarnation = table.IncarnationState();
  if (projection != nullptr && incarnation != nullptr) {
    projection_mask.resize(schema.ColumnCount());
    for (slot_t col : *projection) {
      if (col >= projection_mask.size()) {
        return nullptr;
      }
      projection_mask[col] = 1;
    }
    if (incarnation->ProjectionRejected(generation, effective_max_bytes,
                                        projection_mask)) {
      return nullptr;
    }
  }
  const auto remember_projection_limit = [&] {
    if (!projection_mask.empty() && txn.WriteSetEmpty() &&
        table.LayoutGeneration() == generation) {
      incarnation->RejectProjection(generation, effective_max_bytes,
                                    projection_mask);
    }
  };
  const std::shared_ptr<const TlcsFile> backing_tlcs =
      table.GetBackingTlcsStore();
  if (backing_tlcs != nullptr &&
      backing_tlcs->LayoutGeneration() == generation &&
      backing_tlcs->FirstPageId() == table.FirstPageId()) {
    if (projection == nullptr) {
      if (auto resident = backing_tlcs->ResidentStore(); resident != nullptr) {
        table.RefillLSTableStore(resident);
        return resident;
      }
      if (backing_tlcs->IsPaged()) {
        if (max_bytes == LSTableStore::kBuildByteLimit) {
          table.NoteLSTableCapacityRejected(generation);
        }
        return nullptr;
      }
    } else {
      bool capacity_rejected = false;
      if (auto projected = backing_tlcs->BuildProjectedResidentStore(
              schema, projection, effective_max_bytes, &capacity_rejected);
          projected != nullptr) {
        table.RefillProjectedLSTableStore(projected);
        return projected;
      }
      if (backing_tlcs->IsPaged()) {
        if (capacity_rejected) {
          remember_projection_limit();
        }
        return nullptr;
      }
    }
  }
  StatusOr<TableLease> lease = table.AcquireLease(txn.ID());
  if (!lease.HasValue()) {
    return nullptr;
  }

  txn.SweepCommittedVersionChains();

  auto store = std::make_shared<LSTableStore>();
  store->layout_generation = generation;
  store->first_pid = table.FirstPageId();
  store->integer_columns.resize(schema.ColumnCount());
  if (projection != nullptr) {
    store->loaded_columns.resize(schema.ColumnCount(), 0);
    for (const slot_t column : *projection) {
      if (column >= schema.ColumnCount()) {
        return nullptr;
      }
      store->loaded_columns[column] = 1;
    }
    store->column_offsets.resize(schema.ColumnCount(), SIZE_MAX);
    size_t offset = 0;
    for (slot_t c = 0; c < schema.ColumnCount(); ++c) {
      if (store->HasColumn(c)) {
        store->column_offsets[c] = offset++;
      }
    }
  }

  page_id_t page_id = table.FirstPageId();
  page_id_t max_page_id = 0;
  const bool any_version_chains = txn.HasAnyPageVersionChains();
  std::unordered_set<page_id_t> visited;
  uint64_t estimated_bytes = 0;
  auto mem_charge =
      std::make_shared<ProcessMemoryCharge>(MemoryPoolKind::kColumnar);

  constexpr size_t kStreamDictSlotCount = 512;
  constexpr size_t kStreamDictSlotMask = kStreamDictSlotCount - 1;
  struct StreamDictSlot {
    uint64_t hash{0};
    uint32_t entry_idx{0};
  };
  struct StreamVarCharDict {
    bool active{false};
    bool viable{false};
    bool all_blocks_single_byte{true};
    std::vector<std::string> entries;
    std::array<StreamDictSlot, kStreamDictSlotCount> slots{};
    std::vector<std::vector<uint8_t>> block_codes;
    size_t precharged_code_bytes{0};
  };
  auto stream_hash_sv = [](std::string_view sv) noexcept -> uint64_t {
    uint64_t h = 14695981039346656037ULL;
    for (const char ch : sv) {
      h ^= static_cast<unsigned char>(ch);
      h *= 1099511628211ULL;
    }
    return h == 0 ? 1ULL : h;
  };
  std::vector<StreamVarCharDict> stream_varchar_dicts(schema.ColumnCount());
  for (slot_t c = 0; c < schema.ColumnCount(); ++c) {
    if (store->HasColumn(c) &&
        schema.GetColumn(c).Type() == ValueType::kVarChar) {
      stream_varchar_dicts[c].active = true;
      stream_varchar_dicts[c].viable = true;
      stream_varchar_dicts[c].entries.reserve(
          LSTableColumnDictionary::kMaxEntries);
    }
  }

  while (page_id != 0) {
    if (!visited.insert(page_id).second) {
      return nullptr;
    }
    StatusOr<PageRef> page_or = txn.GetPageManager()->GetPage(page_id, true);
    if (!page_or.HasValue() || page_or.Value().IsNull() ||
        page_or.Value()->Type() != PageType::kRowPage) {
      return nullptr;
    }
    PageRef& page = page_or.Value();
    const bool has_chains =
        any_version_chains && txn.HasPageVersionChains(page_id);
    const page_id_t next_page = page->body.row_page.next_page_id_;
    if (next_page != 0) {
      const Status visible = txn.GetPageManager()->CheckTableTailPage(
          txn.ID(), store->first_pid, next_page);
      if (visible != Status::kSuccess) {
        return nullptr;
      }
    }

    LSTablePageBlock block;
    block.page_id = page_id;
    block.page_lsn = page->PageLSN();
    if (projection == nullptr) {
      block.columns.resize(schema.ColumnCount());
    } else {
      block.columns.Project(store->column_offsets,
                            static_cast<size_t>(std::ranges::count(
                                store->loaded_columns, uint8_t{1})));
    }
    const RowPage& row_page = page->body.row_page;
    const slot_t row_max = row_page.RowMax();
    block.slots.reserve(row_max);
    for (slot_t c = 0; c < schema.ColumnCount(); ++c) {
      if (!store->HasColumn(c)) {
        continue;
      }
      block.columns[c].type = schema.GetColumn(c).Type();
      block.columns[c].is_unsigned = schema.GetColumn(c).IsUnsigned();
      if (store->HasColumn(c) && block.columns[c].type == ValueType::kVarChar) {
        block.columns[c].varchar_ends.reserve(row_max);
      }
    }

    if (row_page.IsColumnarStub()) {
      if (backing_tlcs == nullptr) {
        return nullptr;
      }
      page_or.Value().PageUnlock();
      std::vector<std::string> stub_rows;
      if (backing_tlcs->DecodeBlockEncodedRowsByPid(
              page_id, schema, &stub_rows) != Status::kSuccess) {
        return nullptr;
      }
      for (size_t i = 0; i < stub_rows.size(); ++i) {
        const auto slot = static_cast<slot_t>(i);
        const std::string_view image(stub_rows[slot]);
        if (image.empty()) {
          continue;
        }
        if (!DecodeRowImageIntoBlock(image, slot, schema,
                                     store->integer_columns,
                                     store->loaded_columns, &block)) {
          return nullptr;
        }
      }
    } else if (!has_chains) {
      for (slot_t slot = 0; slot < row_max; ++slot) {
        const std::string_view image = row_page.GetRow(slot);
        if (image.empty()) {
          continue;
        }
        if (!DecodeRowImageIntoBlock(image, slot, schema,
                                     store->integer_columns,
                                     store->loaded_columns, &block)) {
          return nullptr;
        }
      }
    } else {
      store->uncommitted_owner_txn = txn.ID();
      for (slot_t slot = 0; slot < row_max; ++slot) {
        if (row_page.GetRow(slot).empty()) {
          continue;
        }
        StatusOr<std::string_view> visible = page->Read(txn, slot);
        if (visible.GetStatus() == Status::kNotExists) {
          continue;
        }
        if (!visible.HasValue() ||
            !DecodeRowImageIntoBlock(visible.Value(), slot, schema,
                                     store->integer_columns,
                                     store->loaded_columns, &block)) {
          return nullptr;
        }
      }
    }

    for (LSTableColumnBlock& col : block.columns.Stored()) {
      if (col.type == ValueType::kVarChar && !col.has_null &&
          block.row_count > 0 && col.varchar_bytes.size() == block.row_count &&
          col.varchar_ends.size() == block.row_count) {
        bool all_one = true;
        for (uint32_t r = 0; r < block.row_count; ++r) {
          if (col.varchar_ends[r] != r + 1) {
            all_one = false;
            break;
          }
        }
        col.all_single_byte_varchar = all_one;
      }
    }

    page_or.Value().PageUnlock();
    max_page_id = std::max(max_page_id, page_id);
    store->page_ids.push_back(page_id);
    store->total_rows += block.row_count;

    // Stream-compress low-cardinality VARCHAR columns (<= 254 distinct strings)
    // per block so transient raw string buffers never blow past
    // `effective_max_bytes` on multi-ten-million-row projections (e.g. TPC-H
    // SF=10 `l_shipinstruct` + `l_shipmode`). If any later block exceeds 254
    // distinct strings, earlier blocks are losslessly rehydrated back to
    // `varchar_bytes`/`varchar_ends` and charged before continuing.
    size_t extra_code_bytes_this_block = 0;
    for (slot_t c = 0; c < schema.ColumnCount(); ++c) {
      StreamVarCharDict& sd = stream_varchar_dicts[c];
      if (!sd.viable) {
        continue;
      }
      LSTableColumnBlock& col = block.columns[c];
      std::vector<uint8_t> codes(block.row_count,
                                 LSTableColumnDictionary::kNullCode);
      bool fits_dict = true;
      std::string_view prev_sv;
      uint8_t prev_code = 0;
      bool have_prev = false;
      for (uint32_t r = 0; r < block.row_count; ++r) {
        if (col.IsNull(r)) {
          sd.all_blocks_single_byte = false;
          continue;
        }
        const std::string_view sv = col.VarcharAt(r);
        if (have_prev && sv == prev_sv) {
          codes[r] = prev_code;
          continue;
        }
        const uint64_t h = stream_hash_sv(sv);
        size_t slot = static_cast<size_t>(h) & kStreamDictSlotMask;
        uint8_t found_code = 0;
        bool matched = false;
        while (sd.slots[slot].hash != 0) {
          const uint32_t idx = sd.slots[slot].entry_idx;
          if (sd.slots[slot].hash == h && sd.entries[idx] == sv) {
            found_code = static_cast<uint8_t>(idx);
            matched = true;
            break;
          }
          slot = (slot + 1) & kStreamDictSlotMask;
        }
        if (!matched) {
          if (sd.entries.size() >= LSTableColumnDictionary::kMaxEntries) {
            fits_dict = false;
            break;
          }
          found_code = static_cast<uint8_t>(sd.entries.size());
          sd.entries.emplace_back(sv);
          sd.slots[slot] = StreamDictSlot{.hash = h, .entry_idx = found_code};
        }
        codes[r] = found_code;
        prev_sv = sd.entries[found_code];
        prev_code = found_code;
        have_prev = true;
      }

      if (fits_dict) {
        if (block.row_count > 0 && !col.all_single_byte_varchar) {
          sd.all_blocks_single_byte = false;
          std::vector<uint8_t>().swap(col.varchar_bytes);
        }
        std::vector<uint32_t>().swap(col.varchar_ends);
        extra_code_bytes_this_block += codes.size();
        sd.precharged_code_bytes += codes.size();
        sd.block_codes.push_back(std::move(codes));
      } else {
        // Rehydrate previously compressed blocks `0..store->blocks.size()-1`.
        size_t restored_raw_bytes = 0;
        for (size_t bi = 0; bi < store->blocks.size(); ++bi) {
          LSTableColumnBlock& prev_col = store->blocks[bi].columns[c];
          const std::vector<uint8_t>& bc = sd.block_codes[bi];
          const uint32_t nrows = store->blocks[bi].row_count;
          prev_col.varchar_ends.resize(nrows);
          if (prev_col.all_single_byte_varchar) {
            for (uint32_t r = 0; r < nrows; ++r) {
              prev_col.varchar_ends[r] = r + 1;
            }
            restored_raw_bytes += nrows * sizeof(uint32_t);
          } else {
            size_t total_chars = 0;
            for (uint32_t r = 0; r < nrows; ++r) {
              if (bc[r] != LSTableColumnDictionary::kNullCode) {
                total_chars += sd.entries[bc[r]].size();
              }
            }
            prev_col.varchar_bytes.resize(total_chars);
            uint32_t cur_off = 0;
            for (uint32_t r = 0; r < nrows; ++r) {
              const uint8_t code = bc[r];
              if (code != LSTableColumnDictionary::kNullCode) {
                const std::string& s = sd.entries[code];
                if (!s.empty()) {
                  std::memcpy(prev_col.varchar_bytes.data() + cur_off, s.data(),
                              s.size());
                }
                cur_off += static_cast<uint32_t>(s.size());
              }
              prev_col.varchar_ends[r] = cur_off;
            }
            restored_raw_bytes +=
                total_chars + (static_cast<size_t>(nrows) * sizeof(uint32_t));
          }
        }
        if (sd.precharged_code_bytes > 0) {
          mem_charge->Release(sd.precharged_code_bytes);
          estimated_bytes -= sd.precharged_code_bytes;
          sd.precharged_code_bytes = 0;
        }
        estimated_bytes += restored_raw_bytes;
        if (estimated_bytes > effective_max_bytes ||
            !mem_charge->TryAddWithReclaim(restored_raw_bytes)) {
          if (estimated_bytes > effective_max_bytes) {
            remember_projection_limit();
          }
          if (projection == nullptr &&
              max_bytes == LSTableStore::kBuildByteLimit &&
              table.LayoutGeneration() == generation) {
            table.NoteLSTableCapacityRejected(generation);
          }
          return nullptr;
        }
        std::vector<std::vector<uint8_t>>().swap(sd.block_codes);
        std::vector<std::string>().swap(sd.entries);
        sd.viable = false;
      }
    }

    size_t block_bytes = extra_code_bytes_this_block;
    for (slot_t c = 0; c < schema.ColumnCount(); ++c) {
      if (!store->HasColumn(c)) {
        continue;
      }
      const LSTableColumnBlock& col = block.columns[c];
      block_bytes += ((col.type == ValueType::kVarChar ? 0 : block.row_count) *
                      sizeof(int64_t)) +
                     col.varchar_bytes.size() +
                     (col.varchar_ends.size() * sizeof(uint32_t)) +
                     col.nulls.size();
    }
    estimated_bytes += block_bytes;
    if (store->total_rows > static_cast<uint64_t>(INT64_MAX) ||
        estimated_bytes > effective_max_bytes ||
        !mem_charge->TryAddWithReclaim(block_bytes)) {
      if (estimated_bytes > effective_max_bytes) {
        remember_projection_limit();
      }
      // Only an unchanged full-store capacity failure suppresses later full
      // builds. Snapshot/IO failures must never become a capacity verdict.
      if (projection == nullptr && max_bytes == LSTableStore::kBuildByteLimit &&
          table.LayoutGeneration() == generation) {
        table.NoteLSTableCapacityRejected(generation);
      }
      return nullptr;
    }
    store->blocks.push_back(std::move(block));
    page_id = next_page;
  }

  store->memory_charge = std::move(mem_charge);

  if (table.LayoutGeneration() != generation || table.RowsRewrittenInPlace() ||
      txn.IsFailed()) {
    return nullptr;
  }

  // No page view is published while column vectors can still reallocate.
  // This also covers holes and MVCC-visible rows: only complete decoded rows
  // contribute one cell to each numeric column and to block.row_count.
  size_t row_base = 0;
  for (auto& block : store->blocks) {
    for (slot_t c = 0; c < schema.ColumnCount(); ++c) {
      if (store->HasColumn(c) && block.columns[c].type != ValueType::kVarChar) {
        const auto& cells = store->integer_columns[c];
        if (cells.size() != store->total_rows) {
          return nullptr;
        }
        block.columns[c].int_cells =
            std::span<const int64_t>(cells).subspan(row_base, block.row_count);
      }
    }
    row_base += block.row_count;
  }

  constexpr page_id_t kMaxFlatPageId = 16'777'216ULL;
  if (max_page_id < kMaxFlatPageId &&
      store->blocks.size() <= std::numeric_limits<uint32_t>::max() - 1) {
    const size_t flat_bytes =
        static_cast<size_t>(max_page_id + 1) * sizeof(uint32_t);
    if (store->memory_charge->TryAddWithReclaim(flat_bytes)) {
      store->flat_page_to_block.assign(static_cast<size_t>(max_page_id + 1),
                                       LSTableStore::kInvalidBlockIndex);
      for (size_t idx = 0; idx < store->page_ids.size(); ++idx) {
        store->flat_page_to_block[store->page_ids[idx]] =
            static_cast<uint32_t>(idx);
      }
    }
  }
  if (store->flat_page_to_block.empty() && !store->page_ids.empty()) {
    const size_t map_bytes = store->page_ids.size() * 32;
    if (!store->memory_charge->TryAddWithReclaim(map_bytes)) {
      if (projection == nullptr && max_bytes == LSTableStore::kBuildByteLimit &&
          table.LayoutGeneration() == generation) {
        table.NoteLSTableCapacityRejected(generation);
      }
      return nullptr;
    }
    store->page_to_block.reserve(store->page_ids.size());
    for (size_t idx = 0; idx < store->page_ids.size(); ++idx) {
      store->page_to_block.emplace(store->page_ids[idx], idx);
    }
  }

  const auto& col_budget = ProcessMemoryBudget::Global();
  const size_t col_pool_max =
      std::min(col_budget.ColumnarPoolLimit(),
               col_budget.EffectivePoolMaxBytes(MemoryPoolKind::kColumnar));
  const slot_t col_count = schema.ColumnCount();
  store->dictionaries.resize(col_count);
  size_t dictionary_summary_budget =
      std::min<size_t>(size_t{256} << 20, col_pool_max / 4);
  store->column_is_monotonic.assign(col_count, 0);
  store->column_is_unique_nonnull.assign(col_count, 0);
  store->column_is_identity_1_to_n.assign(col_count, 0);
  store->column_has_null.assign(col_count, 0);
  store->column_min_int.assign(col_count, std::numeric_limits<int64_t>::max());
  store->column_max_int.assign(col_count, std::numeric_limits<int64_t>::min());

  for (slot_t c = 0; c < col_count; ++c) {
    if (!store->HasColumn(c)) {
      continue;
    }
    const ValueType vt = schema.GetColumn(c).Type();
    bool any_null = false;
    for (const LSTablePageBlock& blk : store->blocks) {
      if (blk.row_count > 0 && blk.columns[c].has_null) {
        any_null = true;
        break;
      }
    }
    store->column_has_null[c] = any_null ? 1 : 0;

    if (vt == ValueType::kInt64 || vt == ValueType::kDate) {
      bool monotonic = !any_null && store->total_rows > 0;
      bool strict_monotonic = monotonic && !schema.GetColumn(c).IsUnsigned();
      bool identity_1_to_n = strict_monotonic && vt == ValueType::kInt64;
      bool have_prev = false;
      int64_t prev_val = 0;
      uint64_t expected_id = 1;
      int64_t c_min = std::numeric_limits<int64_t>::max();
      int64_t c_max = std::numeric_limits<int64_t>::min();
      for (const LSTablePageBlock& blk : store->blocks) {
        const LSTableColumnBlock& col = blk.columns[c];
        if (col.has_min_max) {
          c_min = std::min(c_min, col.min_int);
          c_max = std::max(c_max, col.max_int);
        }
        if (monotonic) {
          const int64_t* __restrict cells = col.int_cells.data();
          for (uint32_t r = 0; r < blk.row_count; ++r) {
            const int64_t v = cells[r];
            if (have_prev) {
              if (v < prev_val) {
                monotonic = false;
                strict_monotonic = false;
                identity_1_to_n = false;
                break;
              }
              if (v == prev_val) {
                strict_monotonic = false;
                identity_1_to_n = false;
              }
            }
            if (identity_1_to_n && std::cmp_not_equal(v, expected_id)) {
              identity_1_to_n = false;
            }
            ++expected_id;
            prev_val = v;
            have_prev = true;
          }
        }
      }
      if (!strict_monotonic && c == 0 && !any_null && store->total_rows > 0 &&
          vt == ValueType::kInt64 && !schema.GetColumn(c).IsUnsigned() &&
          c_min <= c_max) {
        const uint64_t span =
            static_cast<uint64_t>(c_max) - static_cast<uint64_t>(c_min);
        if (span <= 33'554'432ULL && span + 1 >= store->total_rows) {
          std::vector<uint64_t> seen_bits(
              static_cast<size_t>((span >> 6U) + 1U), 0ULL);
          const auto base_u = static_cast<uint64_t>(c_min);
          bool all_unique = true;
          for (const LSTablePageBlock& blk : store->blocks) {
            const int64_t* __restrict cells = blk.columns[c].int_cells.data();
            for (uint32_t r = 0; r < blk.row_count; ++r) {
              const uint64_t off = static_cast<uint64_t>(cells[r]) - base_u;
              const uint64_t mask = uint64_t{1} << (off & 63U);
              uint64_t& word = seen_bits[static_cast<size_t>(off >> 6U)];
              if ((word & mask) != 0) {
                all_unique = false;
                break;
              }
              word |= mask;
            }
            if (!all_unique) {
              break;
            }
          }
          strict_monotonic = all_unique;
        }
      }
      store->column_is_monotonic[c] = monotonic ? 1 : 0;
      store->column_is_unique_nonnull[c] = strict_monotonic ? 1 : 0;
      store->column_is_identity_1_to_n[c] = identity_1_to_n ? 1 : 0;
      store->column_min_int[c] = c_min;
      store->column_max_int[c] = c_max;
      if (!any_null && store->total_rows > 0 && c_min <= c_max) {
        const uint64_t span =
            static_cast<uint64_t>(c_max) - static_cast<uint64_t>(c_min);
        if (vt == ValueType::kInt64 && span <= 512ULL) {
          std::array<uint8_t, 513> seen{};
          size_t distinct_count = 0;
          const auto base_u = static_cast<uint64_t>(c_min);
          for (const LSTablePageBlock& blk : store->blocks) {
            const int64_t* __restrict cells = blk.columns[c].int_cells.data();
            for (uint32_t r = 0; r < blk.row_count; ++r) {
              const auto off =
                  static_cast<size_t>(static_cast<uint64_t>(cells[r]) - base_u);
              if (seen[off] == 0) {
                seen[off] = 1;
                ++distinct_count;
              }
            }
          }
          if (distinct_count > 0 &&
              distinct_count <= LSTableColumnDictionary::kMaxEntries) {
            LSTableColumnDictionary& dict = store->dictionaries[c];
            dict.active = true;
            dict.value_type = ValueType::kInt64;
            dict.int_entries.reserve(distinct_count);
            std::array<uint8_t, 513> off_to_code{};
            off_to_code.fill(LSTableColumnDictionary::kNullCode);
            for (size_t off = 0; off <= static_cast<size_t>(span); ++off) {
              if (seen[off] != 0) {
                const auto code = static_cast<uint8_t>(dict.int_entries.size());
                off_to_code[off] = code;
                dict.int_entries.push_back(static_cast<int64_t>(base_u + off));
              }
            }
            AllocateNarrowCodes(*store, c);
            for (LSTablePageBlock& blk : store->blocks) {
              LSTableColumnBlock& col = blk.columns[c];
              col.dict = &dict;
              col.dict_mask = {0ULL, 0ULL, 0ULL, 0ULL};
              const int64_t* __restrict cells = col.int_cells.data();
              uint8_t* __restrict codes =
                  dict.narrow_codes.data() + col.wide_code_begin;
              for (uint32_t r = 0; r < blk.row_count; ++r) {
                const uint8_t code = off_to_code[static_cast<size_t>(
                    static_cast<uint64_t>(cells[r]) - base_u)];
                codes[r] = code;
                col.dict_mask[code >> 6] |= uint64_t{1} << (code & 63);
              }
            }
          }
        }
        if (span <= 65535ULL) {
          const auto base_u = static_cast<uint64_t>(c_min);
          for (LSTablePageBlock& blk : store->blocks) {
            LSTableColumnBlock& col = blk.columns[c];
            col.for_base = c_min;
            col.for_width = 2;
            col.for16_codes.resize(blk.row_count);
            const int64_t* __restrict cells = col.int_cells.data();
            uint16_t* __restrict codes16 = col.for16_codes.data();
            for (uint32_t r = 0; r < blk.row_count; ++r) {
              codes16[r] = static_cast<uint16_t>(
                  static_cast<uint64_t>(cells[r]) - base_u);
            }
            col.for_bytes = std::span<const uint8_t>(
                reinterpret_cast<const uint8_t*>(col.for16_codes.data()),
                col.for16_codes.size() * sizeof(uint16_t));
          }
        }
      }
      continue;
    }

    if (vt == ValueType::kDouble && !any_null && store->total_rows > 0) {
      struct DoubleDictSlot {
        uint64_t hash{0};
        int64_t bits{0};
        double val{0.0};
        uint8_t code{0};
      };
      constexpr size_t kTableSize = 512;
      constexpr size_t kTableMask = kTableSize - 1;
      std::array<DoubleDictSlot, kTableSize> table_slots{};
      std::vector<double> distinct_vals;
      distinct_vals.reserve(LSTableColumnDictionary::kMaxEntries);
      bool fits_dictionary = true;
      int64_t prev_bits = 0;
      bool have_prev_bits = false;
      auto hash_bits = [](int64_t b) noexcept -> uint64_t {
        auto z = static_cast<uint64_t>(b);
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
        z ^= z >> 31;
        return z == 0 ? 1ULL : z;
      };
      for (const LSTablePageBlock& blk : store->blocks) {
        const LSTableColumnBlock& col = blk.columns[c];
        const int64_t* __restrict cells = col.int_cells.data();
        for (uint32_t r = 0; r < blk.row_count; ++r) {
          const int64_t b = cells[r];
          if (have_prev_bits && b == prev_bits) {
            continue;
          }
          double d = 0.0;
          std::memcpy(&d, &b, sizeof(d));
          if (std::isnan(d)) {
            fits_dictionary = false;
            break;
          }
          if (d == 0.0) {
            d = 0.0;  // Canonicalize -0.0 and +0.0
          }
          int64_t canon_bits = 0;
          std::memcpy(&canon_bits, &d, sizeof(canon_bits));
          prev_bits = b;
          have_prev_bits = true;
          const uint64_t h = hash_bits(canon_bits);
          size_t slot = static_cast<size_t>(h) & kTableMask;
          while (true) {
            if (table_slots[slot].hash == 0) {
              if (distinct_vals.size() >=
                  LSTableColumnDictionary::kMaxEntries) {
                fits_dictionary = false;
                break;
              }
              table_slots[slot] = DoubleDictSlot{
                  .hash = h,
                  .bits = canon_bits,
                  .val = d,
                  .code = static_cast<uint8_t>(distinct_vals.size())};
              distinct_vals.push_back(d);
              break;
            }
            if (table_slots[slot].bits == canon_bits) {
              break;
            }
            slot = (slot + 1) & kTableMask;
          }
          if (!fits_dictionary) {
            break;
          }
        }
        if (!fits_dictionary) {
          break;
        }
      }
      if (fits_dictionary && !distinct_vals.empty()) {
        std::ranges::sort(distinct_vals);
        LSTableColumnDictionary& dict = store->dictionaries[c];
        dict.active = true;
        dict.value_type = ValueType::kDouble;
        // Entries are canonicalized at build time (±0 unified, NaN rejected),
        // so code equality is value equality; TlcsFile::Finalize already
        // publishes this for persisted dictionaries (P680).
        dict.bytewise_equality = true;
        dict.double_entries = std::move(distinct_vals);
        dict.one_minus_double_entries.resize(dict.double_entries.size());
        dict.one_plus_double_entries.resize(dict.double_entries.size());
        table_slots.fill(DoubleDictSlot{});
        for (size_t idx = 0; idx < dict.double_entries.size(); ++idx) {
          const double d = dict.double_entries[idx];
          dict.one_minus_double_entries[idx] = 1.0 - d;
          dict.one_plus_double_entries[idx] = 1.0 + d;
          int64_t canon_bits = 0;
          std::memcpy(&canon_bits, &d, sizeof(canon_bits));
          const uint64_t h = hash_bits(canon_bits);
          size_t slot = static_cast<size_t>(h) & kTableMask;
          while (table_slots[slot].hash != 0) {
            slot = (slot + 1) & kTableMask;
          }
          table_slots[slot] = DoubleDictSlot{.hash = h,
                                             .bits = canon_bits,
                                             .val = d,
                                             .code = static_cast<uint8_t>(idx)};
        }
        AllocateNarrowCodes(*store, c);
        for (LSTablePageBlock& blk : store->blocks) {
          LSTableColumnBlock& col = blk.columns[c];
          col.dict = &dict;
          col.dict_mask = {0ULL, 0ULL, 0ULL, 0ULL};
          const int64_t* __restrict cells = col.int_cells.data();
          uint8_t* __restrict codes =
              dict.narrow_codes.data() + col.wide_code_begin;
          int64_t last_b = 0;
          uint8_t last_code = 0;
          bool have_last = false;
          for (uint32_t r = 0; r < blk.row_count; ++r) {
            const int64_t b = cells[r];
            uint8_t code = last_code;
            if (!have_last || b != last_b) {
              double d = 0.0;
              std::memcpy(&d, &b, sizeof(d));
              if (d == 0.0) {
                d = 0.0;
              }
              int64_t canon_bits = 0;
              std::memcpy(&canon_bits, &d, sizeof(canon_bits));
              const uint64_t h = hash_bits(canon_bits);
              size_t slot = static_cast<size_t>(h) & kTableMask;
              while (table_slots[slot].bits != canon_bits) {
                slot = (slot + 1) & kTableMask;
              }
              code = table_slots[slot].code;
              last_b = b;
              last_code = code;
              have_last = true;
            }
            codes[r] = code;
            col.has_code_min_max = true;
            col.min_code = std::min<uint32_t>(col.min_code, code);
            col.max_code = std::max<uint32_t>(col.max_code, code);
            col.dict_mask[code >> 6] |= uint64_t{1} << (code & 63);
          }
        }
      }
      continue;
    }

    if (vt == ValueType::kVarChar && store->total_rows > 0) {
      StreamVarCharDict& sd = stream_varchar_dicts[c];
      bool all_blocks_single_byte =
          !any_null && (!sd.active || !sd.viable || sd.all_blocks_single_byte);
      if (all_blocks_single_byte && (!sd.active || !sd.viable)) {
        for (const LSTablePageBlock& blk : store->blocks) {
          if (blk.row_count > 0 && !blk.columns[c].all_single_byte_varchar) {
            all_blocks_single_byte = false;
            break;
          }
        }
      }
      if (all_blocks_single_byte) {
        if (sd.precharged_code_bytes > 0) {
          store->memory_charge->Release(sd.precharged_code_bytes);
          sd.precharged_code_bytes = 0;
        }
        std::vector<std::vector<uint8_t>>().swap(sd.block_codes);
        std::vector<std::string>().swap(sd.entries);
        std::array<uint8_t, 256> seen{};
        size_t distinct_count = 0;
        for (const LSTablePageBlock& blk : store->blocks) {
          const uint8_t* __restrict bytes = blk.columns[c].varchar_bytes.data();
          for (uint32_t r = 0; r < blk.row_count; ++r) {
            const uint8_t b = bytes[r];
            if (seen[b] == 0) {
              seen[b] = 1;
              ++distinct_count;
            }
          }
        }
        if (distinct_count > 0 &&
            distinct_count <= LSTableColumnDictionary::kMaxEntries) {
          LSTableColumnDictionary& dict = store->dictionaries[c];
          dict.active = true;
          dict.all_single_byte = true;
          dict.entries.reserve(distinct_count);
          std::array<uint8_t, 256> byte_to_code{};
          byte_to_code.fill(LSTableColumnDictionary::kNullCode);
          for (size_t b = 0; b < 256; ++b) {
            if (seen[b] != 0) {
              const auto code = static_cast<uint8_t>(dict.entries.size());
              byte_to_code[b] = code;
              dict.single_byte_chars[code] = static_cast<uint8_t>(b);
              dict.entries.emplace_back(1, static_cast<char>(b));
            }
          }
          AllocateNarrowCodes(*store, c);
          for (LSTablePageBlock& blk : store->blocks) {
            LSTableColumnBlock& col = blk.columns[c];
            col.dict = &dict;
            col.dict_mask = {0ULL, 0ULL, 0ULL, 0ULL};
            const uint8_t* __restrict bytes = col.varchar_bytes.data();
            uint8_t* __restrict codes =
                dict.narrow_codes.data() + col.wide_code_begin;
            for (uint32_t r = 0; r < blk.row_count; ++r) {
              const uint8_t code = byte_to_code[bytes[r]];
              codes[r] = code;
              col.dict_mask[code >> 6] |= uint64_t{1} << (code & 63);
            }
            std::vector<uint32_t>().swap(col.varchar_ends);
          }
        }
        continue;
      }

      if (sd.active && sd.viable) {
        if (!sd.entries.empty()) {
          const size_t n_ent = sd.entries.size();
          std::vector<size_t> order(n_ent);
          for (size_t i = 0; i < n_ent; ++i) {
            order[i] = i;
          }
          std::ranges::sort(order, [&](size_t a, size_t b) {
            return sd.entries[a] < sd.entries[b];
          });
          std::array<uint8_t, 256> old_to_new{};
          old_to_new.fill(LSTableColumnDictionary::kNullCode);
          LSTableColumnDictionary& dict = store->dictionaries[c];
          dict.active = true;
          dict.entries.reserve(n_ent);
          dict.all_single_byte = !any_null;
          for (size_t new_idx = 0; new_idx < n_ent; ++new_idx) {
            const size_t old_idx = order[new_idx];
            old_to_new[old_idx] = static_cast<uint8_t>(new_idx);
            std::string& s = sd.entries[old_idx];
            if (s.size() != 1) {
              dict.all_single_byte = false;
            } else {
              dict.single_byte_chars[new_idx] = static_cast<uint8_t>(s[0]);
            }
            dict.entries.push_back(std::move(s));
          }
          dict.codes_follow_string_order =
              std::adjacent_find(dict.entries.begin(), dict.entries.end(),
                                 [](const auto& a, const auto& b) {
                                   return CompareVarCharForOrderBy(a, b,
                                                                   false) >= 0;
                                 }) == dict.entries.end();

          AllocateNarrowCodes(*store, c);
          for (size_t bi = 0; bi < store->blocks.size(); ++bi) {
            LSTablePageBlock& blk = store->blocks[bi];
            LSTableColumnBlock& col = blk.columns[c];
            col.dict_mask = {0ULL, 0ULL, 0ULL, 0ULL};
            uint8_t* __restrict codes =
                dict.narrow_codes.data() + col.wide_code_begin;
            const std::vector<uint8_t>& raw_codes = sd.block_codes[bi];
            for (uint32_t r = 0; r < blk.row_count; ++r) {
              const uint8_t mapped = old_to_new[raw_codes[r]];
              codes[r] = mapped;
              if (mapped != LSTableColumnDictionary::kNullCode) {
                col.dict_mask[mapped >> 6] |= uint64_t{1} << (mapped & 63);
              }
            }
            col.dict = &dict;
            if (!col.all_single_byte_varchar) {
              std::vector<uint8_t>().swap(col.varchar_bytes);
            }
            std::vector<uint32_t>().swap(col.varchar_ends);
          }
        }
        if (sd.precharged_code_bytes > 0) {
          store->memory_charge->Release(sd.precharged_code_bytes);
          sd.precharged_code_bytes = 0;
        }
        std::vector<std::vector<uint8_t>>().swap(sd.block_codes);
        std::vector<std::string>().swap(sd.entries);
        continue;
      }

      BuildWideStringDictionary(store.get(), c, &dictionary_summary_budget);
    }
  }

  size_t code_index_budget =
      std::min<size_t>(size_t{64} << 20, col_pool_max / 8);
  if (store->total_rows >= 8192) {
    for (slot_t c = 0; c < col_count; ++c) {
      if (!store->HasColumn(c) || store->column_has_null[c] != 0 ||
          store->column_is_unique_nonnull[c] != 0 ||
          schema.GetColumn(c).IsUnsigned()) {
        continue;
      }
      const auto type = schema.GetColumn(c).Type();
      if ((type == ValueType::kInt64 || type == ValueType::kDate) &&
          ProveIntegerUniqueness(*store, c)) {
        store->column_is_unique_nonnull[c] = 1;
      }
    }
    for (slot_t c = 0; c < col_count; ++c) {
      if (!store->HasColumn(c)) {
        continue;
      }
      const auto& column = schema.GetColumn(c);
      if ((column.Type() == ValueType::kInt64 ||
           column.Type() == ValueType::kDate) &&
          !column.IsUnsigned() && !store->dictionaries[c].active &&
          store->column_is_unique_nonnull[c] == 0) {
        BuildWideIntegerDictionary(store.get(), c, column.Type(),
                                   &code_index_budget,
                                   &dictionary_summary_budget);
      }
    }
  }

  // Large integer domains cannot be pruned by a small dictionary. Retain a
  // bounded Bloom synopsis of every non-NULL value so absent point probes
  // need not visit all page blocks. Bits are an upper-bound sizing estimate,
  // not optimizer statistics or a proof of cardinality.
  store->integer_membership.resize(col_count);
  if (store->total_rows >= 8192) {
    const size_t bit_count = std::min<size_t>(
        BloomRuntimeFilter::BitsForKeys(store->total_rows), size_t{1} << 24);
    for (slot_t c = 0; c < col_count; ++c) {
      if (!store->HasColumn(c)) {
        continue;
      }
      const auto& column = schema.GetColumn(c);
      if (column.Type() != ValueType::kInt64 || column.IsUnsigned() ||
          store->dictionaries[c].active ||
          // An all-NULL column stores the inverted sentinel
          // (min=INT64_MAX > max=INT64_MIN); skip it like a constant one.
          store->column_min_int[c] >= store->column_max_int[c]) {
        continue;
      }
      std::vector<uint64_t> bits(bit_count / 64);
      for (const auto& block : store->blocks) {
        const auto& input = block.columns[c];
        for (uint32_t r = 0; r < block.row_count; ++r) {
          if (input.IsNull(r)) {
            continue;
          }
          for (unsigned probe = 0; probe < BloomRuntimeFilter::kHashProbes;
               ++probe) {
            const size_t bit = static_cast<size_t>(BloomRuntimeFilter::Hash(
                                   input.int_cells[r], probe)) &
                               (bit_count - 1);
            bits[bit / 64] |= uint64_t{1} << (bit % 64);
          }
        }
      }
      store->integer_membership[c] = std::make_shared<const BloomRuntimeFilter>(
          std::move(bits), bit_count, store->total_rows);
    }
  }

  // Account all dictionary, summary, FOR-16, and integer Bloom filter buffers
  // under kColumnar. Under pressure, drop optional membership/summary metadata
  // first before rejecting the build.
  if (size_t extra_bytes = EstimateSecondaryStoreBytes(*store, schema);
      extra_bytes > 0 &&
      !store->memory_charge->TryAddWithReclaim(extra_bytes)) {
    std::vector<std::shared_ptr<const BloomRuntimeFilter>>(col_count).swap(
        store->integer_membership);
    for (LSTableColumnDictionary& dict : store->dictionaries) {
      std::vector<int64_t>().swap(dict.code_counts);
      std::vector<uint64_t>().swap(dict.code_first_row);
      std::vector<uint32_t>().swap(dict.encounter_codes);
    }
    extra_bytes = EstimateSecondaryStoreBytes(*store, schema);
    if (extra_bytes > 0 &&
        !store->memory_charge->TryAddWithReclaim(extra_bytes)) {
      // Wide-integer dictionaries are purely optional secondary encodings whose
      // backing int_cells remain intact; drop them under pool pressure rather
      // than discarding the already-charged base column blocks.
      for (slot_t c = 0; c < col_count; ++c) {
        LSTableColumnDictionary& dict = store->dictionaries[c];
        if (!dict.wide_codes.empty()) {
          dict = LSTableColumnDictionary{};
          for (LSTablePageBlock& blk : store->blocks) {
            LSTableColumnBlock& col = blk.columns[c];
            col.dict = nullptr;
            col.wide_code_begin = 0;
            col.wide_code_count = 0;
          }
        }
      }
      extra_bytes = EstimateSecondaryStoreBytes(*store, schema);
      if (extra_bytes > 0 &&
          !store->memory_charge->TryAddWithReclaim(extra_bytes)) {
        if (projection == nullptr &&
            max_bytes == LSTableStore::kBuildByteLimit &&
            table.LayoutGeneration() == generation) {
          table.NoteLSTableCapacityRejected(generation);
        }
        return nullptr;
      }
    }
  }

  // Also build a PaxChunk for up to one row group (1024 rows) so
  // native PaxChunk metadata / codecs are materialized on the store.
  try {
    std::optional<Schema> lst_schema = ConvertToLSTableSchema(schema);
    if (lst_schema.has_value() && projection == nullptr) {
      store->lstable_schema = *lst_schema;
      for (slot_t i = 0; i < schema.ColumnCount(); ++i) {
        store->has_unsigned_column =
            store->has_unsigned_column || schema.GetColumn(i).IsUnsigned();
      }
      MemTable mem(*lst_schema);
      int64_t row_id = 0;
      constexpr size_t kMaxPaxSampleRows = PaxChunk::kRowGroupSize;
      for (const LSTablePageBlock& blk : store->blocks) {
        if (std::cmp_greater_equal(row_id, kMaxPaxSampleRows)) {
          break;
        }
        for (uint32_t r = 0;
             r < blk.row_count && std::cmp_less(row_id, kMaxPaxSampleRows);
             ++r) {
          std::vector<Value> vals;
          vals.reserve(schema.ColumnCount() + 1);
          vals.emplace_back(row_id++);
          for (slot_t c = 0; c < schema.ColumnCount(); ++c) {
            const LSTableColumnBlock& col = blk.columns[c];
            if (col.IsNull(r)) {
              vals.emplace_back();
              continue;
            }
            switch (col.type) {
              case ValueType::kInt64:
              case ValueType::kDate:
                vals.emplace_back(col.int_cells[r]);
                break;
              case ValueType::kDouble: {
                double d = 0.0;
                std::memcpy(&d, &col.int_cells[r], sizeof(d));
                vals.emplace_back(d);
                break;
              }
              case ValueType::kVarChar:
                vals.emplace_back(std::string(col.VarcharAt(r)));
                break;
              default:
                vals.emplace_back();
                break;
            }
          }
          if (mem.Put(Row(std::move(vals))) != Status::kSuccess) {
            break;
          }
        }
      }
      if (row_id > 0) {
        std::vector<char> blob;
        std::vector<uint32_t> key_run;
        std::vector<uint32_t> row_run;
        std::vector<uint8_t> del_run;
        size_t gen = 0;
        if (mem.Sync(&blob, &key_run, &row_run, &gen, &del_run) ==
                Status::kSuccess &&
            !key_run.empty()) {
          std::vector<SortedRun::Entry> entries;
          entries.reserve(key_run.size());
          for (size_t i = 0; i < key_run.size(); ++i) {
            entries.emplace_back(key_run[i], row_run[i],
                                 i < del_run.size() && del_run[i] != 0);
          }
          SortedRun run(std::move(blob), std::move(entries), gen);
          auto chunk = PaxChunk::BuildFromSortedRun(run, *lst_schema, false);
          if (chunk.HasValue()) {
            store->pax_chunk =
                std::make_shared<const PaxChunk>(chunk.MoveValue());
          } else {
            LOG(WARN) << "PaxChunk sample construction skipped: "
                      << chunk.GetStatus();
          }
        }
      }
    }
  } catch (const std::exception& e) {
    LOG(WARN) << "PaxChunk sample construction skipped: " << e.what();
  } catch (...) {
    LOG(WARN) << "PaxChunk sample construction skipped: unknown exception";
  }

  if (cache_in_table) {
    if (projection != nullptr) {
      // Replacing the one cached projection bounds cache growth across
      // queries. Existing readers keep their immutable shared store until
      // completion.
      table.RefillProjectedLSTableStore(store);
    } else if (store->uncommitted_owner_txn == 0 ||
               table.GetLSTableStore() == nullptr) {
      table.RefillLSTableStore(store);
    }
  }
  if (mutable_out != nullptr) {
    *mutable_out = store;
  }
  return store;
}

std::shared_ptr<const LSTableStore> BuildLSTableStore(
    Transaction& txn, const Table& table, const std::vector<slot_t>* projection,
    uint64_t max_bytes) {
  return BuildLSTableStoreImpl(txn, table, projection, max_bytes,
                               /*cache_in_table=*/true,
                               /*mutable_out=*/nullptr);
}

namespace {

void CopyColumnBlockView(const LSTableColumnBlock& src,
                         const LSTableColumnDictionary* dst_dict,
                         LSTableColumnBlock* dst) noexcept {
  dst->type = src.type;
  dst->is_unsigned = src.is_unsigned;
  dst->has_null = src.has_null;
  dst->has_min_max = src.has_min_max;
  dst->has_double_min_max = src.has_double_min_max;
  dst->has_nan = src.has_nan;
  dst->all_single_byte_varchar = src.all_single_byte_varchar;
  dst->for_width = src.for_width;
  dst->for_base = src.for_base;
  dst->min_int = src.min_int;
  dst->max_int = src.max_int;
  dst->min_double = src.min_double;
  dst->max_double = src.max_double;
  dst->dict = dst_dict;
  dst->wide_code_begin = src.wide_code_begin;
  dst->wide_code_count = src.wide_code_count;
  dst->dict_mask = src.dict_mask;
  dst->dict_codes = src.dict_codes;
  dst->int_cells = src.int_cells;
  dst->is_const_code = src.is_const_code;
  dst->has_code_min_max = src.has_code_min_max;
  dst->const_code = src.const_code;
  dst->min_code = src.min_code;
  dst->max_code = src.max_code;
  dst->min_nonzero_code = src.min_nonzero_code;
  if (!src.for_bytes.empty()) {
    dst->for_bytes = src.for_bytes;
  } else if (!src.for16_codes.empty()) {
    dst->for_bytes = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(src.for16_codes.data()),
        src.for16_codes.size() * sizeof(uint16_t));
  } else {
    dst->for_bytes = {};
  }
  dst->nulls_view = src.Nulls();
  dst->varchar_bytes_view = src.VarcharBytes();
  dst->varchar_ends_view = src.VarcharEnds();
  dst->block_bloom = src.block_bloom;
}

}  // namespace

const LSTableColumnBlock* LSTableStore::ResolveBlockColumn(
    size_t bi, slot_t col,
    std::vector<std::unique_ptr<LSTableLazyDecodedBlock>>* transient_out)
    const {
  if (bi >= blocks.size() || !HasColumn(col)) {
    return nullptr;
  }
  if (!ColumnIsLazy(col)) {
    return &blocks[bi].columns[col];
  }
  const std::shared_ptr<const LSTableColumnCacheEntry>& entry =
      column_owners[col];
  if (!entry || !entry->lazy_state || !entry->lazy_state->lazy_blocks ||
      bi >= entry->blocks.size()) {
    return nullptr;
  }
  const LSTableLazyColumnState& lazy = *entry->lazy_state;
  if (const LSTableColumnBlock* cached =
          lazy.lazy_blocks[bi].load(std::memory_order_acquire);
      cached != nullptr) {
    return cached;
  }
  if (!lazy.backing_tlcs) {
    return nullptr;
  }
  std::unique_ptr<LSTableLazyDecodedBlock> decoded;
  if (!lazy.backing_tlcs->DecodeSingleColumnBlockForCache(
          static_cast<uint32_t>(bi), col, *entry, &decoded) ||
      decoded == nullptr) {
    return nullptr;
  }
  auto charge =
      std::make_shared<ProcessMemoryCharge>(MemoryPoolKind::kColumnar);
  const bool budgeted = decoded->charged_bytes == 0 ||
                        charge->TryAddWithReclaim(decoded->charged_bytes);
  if (budgeted) {
    decoded->memory_charge = std::move(charge);
    std::scoped_lock lk(
        lazy.lazy_stripes[bi & (LSTableLazyColumnState::kLazyStripeCount - 1)]);
    if (const LSTableColumnBlock* winner =
            lazy.lazy_blocks[bi].load(std::memory_order_relaxed);
        winner != nullptr) {
      return winner;
    }
    std::shared_ptr<const LSTableLazyDecodedBlock> owned = std::move(decoded);
    const size_t charged = owned->charged_bytes;
    const LSTableColumnBlock* ptr = &owned->block;
    lazy.lazy_owners[bi] = std::move(owned);
    if (lazy.lazy_bytes_used && charged > 0) {
      lazy.lazy_bytes_used->fetch_add(charged, std::memory_order_relaxed);
    }
    lazy.lazy_blocks[bi].store(ptr, std::memory_order_release);
    return ptr;
  }
  if (transient_out == nullptr) {
    return nullptr;
  }
  transient_out->push_back(std::move(decoded));
  return &transient_out->back()->block;
}

const LSTablePageBlock* LSTableStore::MaterializeBlockView(
    size_t bi, LazyBlockScratch* scratch,
    std::span<const slot_t> needed_cols) const {
  if (bi >= blocks.size()) {
    return nullptr;
  }
  if (!is_block_lazy) {
    return &blocks[bi];
  }
  if (scratch == nullptr) {
    return nullptr;
  }
  const LSTablePageBlock& src_b = blocks[bi];
  LSTablePageBlock& dst_b = scratch->block;
  dst_b.page_id = src_b.page_id;
  dst_b.page_lsn = src_b.page_lsn;
  dst_b.row_count = src_b.row_count;
  dst_b.has_any_null = src_b.has_any_null;
  dst_b.identity_slots = src_b.identity_slots;
  if (!src_b.identity_slots) {
    dst_b.slots = src_b.slots;
  }
  if (dst_b.columns.size() != src_b.columns.size() ||
      dst_b.columns.Stored().size() != src_b.columns.Stored().size()) {
    if (loaded_columns.empty()) {
      dst_b.columns.resize(src_b.columns.size());
    } else {
      size_t kept = 0;
      for (const uint8_t b : loaded_columns) {
        kept += (b != 0) ? 1 : 0;
      }
      dst_b.columns.Project(column_offsets, kept);
    }
  }
  auto materialize_col = [&](slot_t c) -> bool {
    // dst_b.columns may be projected: Contains() maps the original column
    // number through the offset table, so an unloaded column must be skipped
    // here instead of letting operator[]'s .at() throw across the Status
    // boundary.
    if (!HasColumn(c) || c >= src_b.columns.size() ||
        !dst_b.columns.Contains(c)) {
      return true;
    }
    if (ColumnIsLazy(c)) {
      const LSTableColumnBlock* resolved =
          ResolveBlockColumn(bi, c, &scratch->transient);
      if (resolved == nullptr) {
        return false;
      }
      CopyColumnBlockView(
          *resolved, resolved->dict != nullptr ? &dictionaries[c] : nullptr,
          &dst_b.columns[c]);
    } else {
      const LSTableColumnBlock& dense = src_b.columns[c];
      CopyColumnBlockView(dense, dense.dict, &dst_b.columns[c]);
    }
    return true;
  };
  if (needed_cols.empty()) {
    const size_t col_count = src_b.columns.size();
    for (size_t c = 0; c < col_count; ++c) {
      if (!materialize_col(static_cast<slot_t>(c))) {
        return nullptr;
      }
    }
  } else {
    for (const slot_t c : needed_cols) {
      if (!materialize_col(c)) {
        return nullptr;
      }
    }
  }
  return &dst_b;
}

std::shared_ptr<const LSTableStore> AssembleLSTableStoreView(
    uint64_t generation, size_t col_count,
    const std::shared_ptr<const LSTableBlockLayout>& layout,
    const std::vector<std::shared_ptr<const LSTableColumnCacheEntry>>& columns,
    const std::vector<slot_t>* projection) {
  (void)projection;
  if (!layout || columns.size() != col_count) {
    return nullptr;
  }
  auto store = std::make_shared<LSTableStore>();
  store->layout_generation = generation;
  store->first_pid = layout->first_pid;
  store->uncommitted_owner_txn = layout->uncommitted_owner_txn;
  store->total_rows = layout->total_rows;
  store->lstable_schema = layout->lstable_schema;
  store->has_unsigned_column = layout->has_unsigned_column;
  store->is_persistent_tlcs = layout->is_persistent_tlcs;
  store->pax_chunk = layout->pax_chunk;
  store->serialized_column_stats = layout->serialized_column_stats;
  store->backing_mmap = layout->backing_mmap;
  if (!store->backing_mmap) {
    for (const auto& entry : columns) {
      if (entry && entry->backing_mmap) {
        store->backing_mmap = entry->backing_mmap;
        break;
      }
    }
  }
  store->memory_charge =
      std::make_shared<ProcessMemoryCharge>(MemoryPoolKind::kColumnar);
  store->page_ids = layout->page_ids;
  store->flat_page_to_block = layout->flat_page_to_block;
  store->page_to_block = layout->page_to_block;
  store->layout_owner = layout;
  store->column_owners = columns;

  size_t kept = 0;
  for (size_t c = 0; c < col_count; ++c) {
    if (columns[c] != nullptr) {
      ++kept;
    }
  }
  if (kept < col_count) {
    store->loaded_columns.assign(col_count, 0);
    store->column_offsets.assign(col_count, SIZE_MAX);
    size_t off = 0;
    for (size_t c = 0; c < col_count; ++c) {
      if (columns[c] != nullptr) {
        store->loaded_columns[c] = 1;
        store->column_offsets[c] = off++;
      }
    }
  }

  store->dictionaries.resize(col_count);
  store->integer_columns.resize(col_count);
  store->integer_columns_view.resize(col_count);
  store->integer_membership.resize(col_count);
  store->column_has_null.assign(col_count, 0);
  store->column_is_monotonic.assign(col_count, 0);
  store->column_is_unique_nonnull.assign(col_count, 0);
  store->column_is_identity_1_to_n.assign(col_count, 0);
  store->column_min_int.assign(col_count, std::numeric_limits<int64_t>::max());
  store->column_max_int.assign(col_count, std::numeric_limits<int64_t>::min());

  const size_t nblocks = layout->block_row_counts.size();
  for (size_t c = 0; c < col_count; ++c) {
    const auto& entry = columns[c];
    if (!entry) {
      continue;
    }
    if (entry->blocks.size() != nblocks) {
      return nullptr;
    }
    store->column_has_null[c] = entry->has_null ? 1 : 0;
    store->column_is_monotonic[c] = entry->is_monotonic ? 1 : 0;
    store->column_is_unique_nonnull[c] = entry->is_unique_nonnull ? 1 : 0;
    store->column_is_identity_1_to_n[c] = entry->is_identity_1_to_n ? 1 : 0;
    store->column_min_int[c] = entry->min_int;
    store->column_max_int[c] = entry->max_int;
    store->dictionaries[c] = MakeDictionaryView(entry->dictionary);
    store->integer_columns_view[c] = entry->IntegerColumnSpan();
    store->integer_membership[c] = entry->integer_membership;
    if (entry->is_block_lazy) {
      store->is_block_lazy = true;
    }
  }

  store->blocks.resize(nblocks);
  for (size_t bi = 0; bi < nblocks; ++bi) {
    LSTablePageBlock& dst_b = store->blocks[bi];
    if (bi < layout->page_ids.size()) {
      dst_b.page_id = layout->page_ids[bi];
    }
    if (bi < layout->block_page_lsns.size()) {
      dst_b.page_lsn = layout->block_page_lsns[bi];
    }
    dst_b.row_count = layout->block_row_counts[bi];
    dst_b.identity_slots = bi < layout->block_identity_slots.size()
                               ? (layout->block_identity_slots[bi] != 0)
                               : true;
    if (!dst_b.identity_slots && bi < layout->block_slots.size()) {
      dst_b.slots = layout->block_slots[bi];
    }
    if (kept == col_count) {
      dst_b.columns.resize(col_count);
    } else {
      dst_b.columns.Project(store->column_offsets, kept);
    }
    for (size_t c = 0; c < col_count; ++c) {
      const auto& entry = columns[c];
      if (!entry) {
        continue;
      }
      const LSTableColumnBlock& src = entry->blocks[bi];
      LSTableColumnBlock& dst = dst_b.columns[c];
      CopyColumnBlockView(
          src, (src.dict != nullptr) ? &store->dictionaries[c] : nullptr, &dst);
      if (dst.has_null) {
        dst_b.has_any_null = true;
      }
    }
  }
  return store;
}

std::shared_ptr<const LSTableStore> GetOrBuildLSTableStore(
    Transaction& txn, const Table& table, bool allow_lazy_build,
    const std::vector<slot_t>* projection, bool allow_block_lazy) {
  if (txn.IsFailed() || table.RowsRewrittenInPlace()) {
    return nullptr;
  }
  const uint64_t generation = table.LayoutGeneration();
  if (generation == 0) {
    return nullptr;
  }
  std::shared_ptr<const LSTableStore> current = table.GetLSTableStore();
  if (current != nullptr && current->layout_generation == generation &&
      current->first_pid == table.FirstPageId() &&
      (allow_block_lazy || !current->is_block_lazy)) {
    if (current->uncommitted_owner_txn == 0 ||
        current->uncommitted_owner_txn == txn.ID()) {
      return current;
    }
    txn.SweepCommittedVersionChains();
    if (!txn.HasAnyPageVersionChains() &&
        (txn.IsReadOnly() || txn.WriteSetEmpty())) {
      if (auto rebuilt = BuildLSTableStore(txn, table);
          rebuilt != nullptr &&
          rebuilt->layout_generation == table.LayoutGeneration()) {
        return rebuilt;
      }
    }
    return nullptr;
  }
  // A cached physical image is only safe when this snapshot can do a physical
  // read: with registered MVCC version chains the txn's logical view differs
  // from any physical store, so every cached-store return below must pass
  // this gate (fresh builds are exempt — they are built under this txn).
  const auto physical_snapshot_safe = [&]() {
    txn.SweepCommittedVersionChains();
    return !txn.HasAnyPageVersionChains() &&
           (txn.IsReadOnly() || txn.WriteSetEmpty());
  };
  const Schema& schema = table.GetSchema();
  const slot_t col_count = schema.ColumnCount();
  std::vector<uint32_t> req_cols;
  if (projection != nullptr) {
    if (!CanonicalizeProjection(*projection, col_count, &req_cols)) {
      return nullptr;
    }
  } else {
    req_cols.resize(col_count);
    for (slot_t c = 0; c < col_count; ++c) {
      req_cols[c] = c;
    }
  }
  if (!req_cols.empty()) {
    if (auto cached_view = table.GetCachedLSTableView(generation, req_cols);
        cached_view != nullptr &&
        cached_view->layout_generation == generation &&
        cached_view->first_pid == table.FirstPageId() &&
        (allow_block_lazy || !cached_view->is_block_lazy) &&
        (cached_view->uncommitted_owner_txn == 0 ||
         cached_view->uncommitted_owner_txn == txn.ID())) {
      if (physical_snapshot_safe()) {
        return cached_view;
      }
    }
  }
  if (projection != nullptr) {
    const auto partial = table.GetProjectedLSTableStore();
    if (partial != nullptr && partial->layout_generation == generation &&
        partial->first_pid == table.FirstPageId() &&
        (allow_block_lazy || !partial->is_block_lazy) &&
        (partial->uncommitted_owner_txn == 0 ||
         partial->uncommitted_owner_txn == txn.ID()) &&
        std::ranges::all_of(req_cols, [&](uint32_t c) {
          return c < col_count && partial->HasColumn(static_cast<slot_t>(c));
        })) {
      if (physical_snapshot_safe()) {
        return partial;
      }
    }
  }
  if (!req_cols.empty() && LSTableStore::SupportsSchema(schema)) {
    auto cached_layout = table.GetLSTableBlockLayout(generation);
    if (cached_layout != nullptr &&
        cached_layout->first_pid == table.FirstPageId() &&
        (cached_layout->uncommitted_owner_txn == 0 ||
         cached_layout->uncommitted_owner_txn == txn.ID())) {
      std::vector<std::shared_ptr<const LSTableColumnCacheEntry>>
          cached_entries(col_count);
      bool all_cached = true;
      for (const uint32_t c32 : req_cols) {
        const auto c = static_cast<slot_t>(c32);
        auto entry = table.GetCachedLSTableColumn(c, generation);
        if (!entry || (!allow_block_lazy && entry->is_block_lazy)) {
          all_cached = false;
          break;
        }
        cached_entries[c] = std::move(entry);
      }
      if (all_cached && physical_snapshot_safe()) {
        if (auto view =
                AssembleLSTableStoreView(generation, col_count, cached_layout,
                                         cached_entries, projection)) {
          bool covers_request = true;
          for (const uint32_t c32 : req_cols) {
            if (!view->HasColumn(static_cast<slot_t>(c32))) {
              covers_request = false;
              break;
            }
          }
          if (covers_request) {
            table.RefillCachedLSTableView(
                generation, std::vector<uint32_t>(req_cols), view);
            return view;
          }
        }
      }
    }
  }
  // Reaching here means the cached store is stale (different generation or
  // first page). A stale non-empty store is exactly the one that must be
  // replaced; only a FRESH non-empty store may block a rebuild. Existing
  // readers keep their own shared_ptr snapshot while the replacement is built.
  if (!allow_lazy_build ||
      (current != nullptr && current->total_rows > 0 &&
       current->layout_generation == generation &&
       current->first_pid == table.FirstPageId()) ||
      (!txn.IsReadOnly() && !txn.WriteSetEmpty())) {
    return nullptr;
  }
  txn.SweepCommittedVersionChains();
  if (txn.HasAnyPageVersionChains()) {
    return nullptr;
  }

  const std::shared_ptr<const TlcsFile> backing_tlcs =
      table.GetBackingTlcsStore();
  const bool valid_tlcs = backing_tlcs != nullptr &&
                          backing_tlcs->LayoutGeneration() == generation &&
                          backing_tlcs->FirstPageId() == table.FirstPageId();
  if (valid_tlcs) {
    if (auto resident = backing_tlcs->ResidentStore();
        resident != nullptr && resident->layout_generation == generation &&
        resident->first_pid == table.FirstPageId()) {
      table.RefillLSTableStore(resident);
      return resident;
    }
    const bool use_block_lazy = allow_block_lazy &&
                                backing_tlcs->TotalRows() >= 65536 &&
                                backing_tlcs->HasColumnSynopses();
    if (!use_block_lazy && projection == nullptr &&
        table.LSTableCapacityRejected(generation)) {
      return nullptr;
    }
    if (!LSTableStore::SupportsSchema(schema)) {
      return nullptr;
    }
    if (!use_block_lazy && projection == nullptr) {
      std::vector<slot_t> all_slots;
      all_slots.reserve(req_cols.size());
      for (const uint32_t c32 : req_cols) {
        all_slots.push_back(static_cast<slot_t>(c32));
      }
      const uint64_t est_all =
          backing_tlcs->EstimateColumnsCacheBytes(schema, all_slots);
      const uint64_t eff_limit =
          EffectiveBuildByteLimit(LSTableStore::kBuildByteLimit);
      const auto& budget = ProcessMemoryBudget::Global();
      const size_t col_limit =
          std::min(budget.ColumnarPoolLimit(),
                   budget.EffectivePoolMaxBytes(MemoryPoolKind::kColumnar));
      if (est_all > eff_limit || (col_limit > 0 && est_all > col_limit)) {
        table.NoteLSTableCapacityRejected(generation);
        return nullptr;
      }
    }
    std::vector<std::shared_ptr<const LSTableColumnCacheEntry>> col_entries(
        col_count);
    std::vector<slot_t> missing_cols;
    std::vector<std::shared_ptr<const LSTableColumnCacheEntry>> existing_lazy;
    bool has_existing_lazy = false;
    for (const uint32_t c32 : req_cols) {
      const auto c = static_cast<slot_t>(c32);
      if (auto cached = table.GetCachedLSTableColumn(c, generation)) {
        if (use_block_lazy || !cached->is_block_lazy) {
          col_entries[c] = std::move(cached);
          continue;
        }
        if (table.LSTableColumnRejected(c, generation)) {
          return nullptr;
        }
        missing_cols.push_back(c);
        existing_lazy.push_back(std::move(cached));
        has_existing_lazy = true;
      } else {
        if (!use_block_lazy && table.LSTableColumnRejected(c, generation)) {
          return nullptr;
        }
        missing_cols.push_back(c);
        existing_lazy.push_back(nullptr);
      }
    }
    auto layout = table.GetLSTableBlockLayout(generation);
    if (!layout || layout->first_pid != table.FirstPageId()) {
      layout = backing_tlcs->BuildBlockLayout(schema, generation);
      if (!layout) {
        return nullptr;
      }
      table.RefillLSTableBlockLayout(generation, layout);
    }
    if (!missing_cols.empty()) {
      if (use_block_lazy) {
        auto built = backing_tlcs->BuildLazyColumnCacheEntries(
            schema, generation, missing_cols);
        bool all_built = built.size() == missing_cols.size();
        for (size_t i = 0; i < missing_cols.size(); ++i) {
          if (i < built.size() && built[i] != nullptr) {
            const slot_t c = missing_cols[i];
            table.RefillCachedLSTableColumn(
                c, generation, built[i]->estimated_bytes, built[i],
                built[i]->lazy_state ? built[i]->lazy_state->lazy_bytes_used
                                     : nullptr);
            col_entries[c] = std::move(built[i]);
          } else {
            all_built = false;
          }
        }
        if (!all_built) {
          return nullptr;
        }
      } else {
        const uint64_t eff_limit =
            EffectiveBuildByteLimit(LSTableStore::kBuildByteLimit);
        std::vector<slot_t> rejected_cols;
        auto built = backing_tlcs->BuildColumnCacheEntries(
            schema, generation, missing_cols, eff_limit, &rejected_cols,
            has_existing_lazy ? &existing_lazy : nullptr);
        for (const slot_t rc : rejected_cols) {
          // Only a column this caller actually requested may be marked as
          // rejected for the generation; an implementation change in the
          // builder must not permanently ban an unrelated column.
          if (std::ranges::find(missing_cols, rc) != missing_cols.end()) {
            table.NoteLSTableColumnRejected(rc, generation);
          }
        }
        bool all_built = built.size() == missing_cols.size();
        for (size_t i = 0; i < missing_cols.size(); ++i) {
          if (i < built.size() && built[i] != nullptr) {
            const slot_t c = missing_cols[i];
            table.RefillCachedLSTableColumn(
                c, generation, built[i]->estimated_bytes, built[i]);
            col_entries[c] = std::move(built[i]);
          } else {
            all_built = false;
          }
        }
        if (!all_built) {
          return nullptr;
        }
      }
    }
    auto view = AssembleLSTableStoreView(generation, col_count, layout,
                                         col_entries, projection);
    if (!view) {
      return nullptr;
    }
    table.RefillCachedLSTableView(generation, std::move(req_cols), view);
    return view;
  }

  if (!table.LSTableCapacityRejected(generation)) {
    uint64_t full_limit = LSTableStore::kBuildByteLimit;
    if (!valid_tlcs && projection != nullptr && !projection->empty() &&
        projection->size() * 2 < col_count && col_count >= 16) {
      const uint64_t eff =
          EffectiveBuildByteLimit(LSTableStore::kBuildByteLimit);
      full_limit =
          std::min(eff, std::max<uint64_t>(uint64_t{64} << 20, eff / 4));
    }
    if (auto full = BuildLSTableStore(txn, table, nullptr, full_limit);
        full != nullptr &&
        full->layout_generation == table.LayoutGeneration()) {
      return full;
    }
    if (full_limit != LSTableStore::kBuildByteLimit &&
        table.LayoutGeneration() == generation) {
      // A failure under the narrowed limit says nothing about its cause: an
      // I/O error, snapshot mismatch or transient memory shortage looks the
      // same as a genuine capacity failure here.  Retry at the full limit and
      // let BuildLSTableStore record a capacity rejection only when the full
      // build itself really runs out of budget.
      if (auto full = BuildLSTableStore(txn, table, nullptr,
                                        LSTableStore::kBuildByteLimit);
          full != nullptr &&
          full->layout_generation == table.LayoutGeneration()) {
        return full;
      }
    }
  }
  if (projection == nullptr || !table.LSTableCapacityRejected(generation)) {
    return nullptr;
  }
  if (!LSTableStore::SupportsSchema(schema)) {
    return nullptr;
  }
  std::vector<uint8_t> req_mask(col_count, 0);
  for (const uint32_t c : req_cols) {
    req_mask[c] = 1;
  }
  if (const auto& incarnation = table.IncarnationState();
      incarnation &&
      incarnation->ProjectionRejected(
          generation, EffectiveBuildByteLimit(LSTableStore::kBuildByteLimit),
          req_mask)) {
    return nullptr;
  }
  if (req_cols.size() >= col_count) {
    return nullptr;
  }

  std::vector<std::shared_ptr<const LSTableColumnCacheEntry>> col_entries(
      col_count);
  std::vector<slot_t> missing_cols;
  for (const uint32_t c32 : req_cols) {
    const auto c = static_cast<slot_t>(c32);
    if (auto cached = table.GetCachedLSTableColumn(c, generation)) {
      col_entries[c] = std::move(cached);
    } else {
      if (table.LSTableColumnRejected(c, generation)) {
        return nullptr;
      }
      missing_cols.push_back(c);
    }
  }
  auto layout = table.GetLSTableBlockLayout(generation);
  const bool need_layout = !layout || layout->first_pid != table.FirstPageId();
  if (!missing_cols.empty() || need_layout) {
    if (!missing_cols.empty() && layout != nullptr) {
      uint64_t est_missing =
          sizeof(LSTablePageBlock) * layout->block_row_counts.size();
      for (const slot_t mc : missing_cols) {
        const ValueType vt = schema.GetColumn(mc).Type();
        est_missing +=
            layout->total_rows * (vt == ValueType::kVarChar ? 16ULL : 12ULL);
      }
      const auto& budget = ProcessMemoryBudget::Global();
      const size_t col_limit =
          std::min(budget.ColumnarPoolLimit(),
                   budget.EffectivePoolMaxBytes(MemoryPoolKind::kColumnar));
      if (col_limit > 0 && budget.ColumnarUsed() + est_missing > col_limit) {
        // The global pool usage only decides *whether* to reclaim; the
        // eviction itself walks this table's own incarnation columnar cache,
        // so it cannot drop another table's hot column entries.
        static_cast<void>(table.EvictIdleColumnarHeadroom(
            (budget.ColumnarUsed() + est_missing) - col_limit));
      }
    }
    std::shared_ptr<LSTableStore> mutable_built;
    auto built = BuildLSTableStoreImpl(
        txn, table, &missing_cols, LSTableStore::kBuildByteLimit,
        /*cache_in_table=*/false, &mutable_built);
    if (built != nullptr && !mutable_built) {
      // A concurrent reader may have attached/returned a resident store.
      return built;
    }
    if (!mutable_built && req_cols.size() > missing_cols.size()) {
      static_cast<void>(table.EvictIdleColumnarHeadroom(size_t{256} << 20));
      built = BuildLSTableStoreImpl(txn, table, &missing_cols,
                                    LSTableStore::kBuildByteLimit,
                                    /*cache_in_table=*/false, &mutable_built);
      if (built != nullptr && !mutable_built) {
        return built;
      }
    }
    if (!mutable_built) {
      if (missing_cols.size() == 1 && table.LayoutGeneration() == generation) {
        table.NoteLSTableColumnRejected(missing_cols[0], generation);
      }
      return nullptr;
    }
    ExtractColumnsFromStore(*mutable_built, table, generation, missing_cols,
                            &layout, &col_entries);
    // ExtractColumnsFromStore skips requested columns the built store does not
    // carry.  Assembling that partial view would still succeed and cache it as
    // satisfying the whole projection, so refuse it here instead.
    for (const slot_t mc : missing_cols) {
      if (col_entries[mc] == nullptr) {
        return nullptr;
      }
    }
  }
  auto view = AssembleLSTableStoreView(generation, col_count, layout,
                                       col_entries, projection);
  if (!view) {
    return nullptr;
  }
  table.RefillCachedLSTableView(generation, std::move(req_cols), view);
  return view;
}

}  // namespace tinylamb
