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
#include "common/serdes.hpp"
#include "page/page.hpp"
#include "page/page_manager.hpp"
#include "page/page_ref.hpp"
#include "page/row_page.hpp"
#include "table/memtable/memtable.hpp"
#include "table/pax_chunk/pax_chunk.hpp"
#include "table/sorted_run/sorted_run.hpp"
#include "table/table.hpp"
#include "transaction/transaction.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/constraint.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

namespace {

#if defined(__BYTE_ORDER__) && (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
#define TINYLAMB_LST_BSWAP64(x) __builtin_bswap64(x)
#else
#define TINYLAMB_LST_BSWAP64(x) (x)
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
                             const Schema& schema, LSTablePageBlock* block) {
  constexpr slot_t kNullBitmapFlag = slot_t{1} << 15;
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
    LSTableColumnBlock& col = block->columns[i];
    const bool is_null =
        has_null && (bitmap[i / 8] & static_cast<char>(1U << (i % 8))) != 0;
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
        col.int_cells.push_back(0);
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
        col.int_cells.push_back(val);
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
      if (!col.has_min_max) {
        return false;
      }
      switch (cmp.op) {
        case BinaryOperation::kEquals:
          if (cmp.constant < col.min_int || cmp.constant > col.max_int) {
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

bool LSTableStore::SupportsSchema(const Schema& schema) noexcept {
  if (schema.ColumnCount() == 0) {
    return false;
  }
  for (slot_t i = 0; i < schema.ColumnCount(); ++i) {
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
  if (!lstable_schema.has_value() || pax_chunk == nullptr ||
      pax_chunk->TotalRowCount() != total_rows) {
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

std::shared_ptr<const LSTableStore> BuildLSTableStore(Transaction& txn,
                                                      const Table& table) {
  if (txn.IsFailed() || table.RowsRewrittenInPlace()) {
    return nullptr;
  }
  const Schema& schema = table.GetSchema();
  if (!LSTableStore::SupportsSchema(schema)) {
    return nullptr;
  }
  const uint64_t generation = table.LayoutGeneration();
  if (generation == 0) {
    return nullptr;
  }
  StatusOr<TableLease> lease = table.AcquireLease(txn.ID());
  if (!lease.HasValue()) {
    return nullptr;
  }

  txn.SweepCommittedVersionChains();

  auto store = std::make_shared<LSTableStore>();
  store->layout_generation = generation;
  store->first_pid = table.FirstPageId();

  page_id_t page_id = table.FirstPageId();
  page_id_t max_page_id = 0;
  const bool any_version_chains = txn.HasAnyPageVersionChains();
  std::unordered_set<page_id_t> visited;
  constexpr uint64_t kMaxLSTableRows = 16'777'216ULL;
  constexpr uint64_t kMaxLSTableBytes = 4ULL << 30;
  uint64_t estimated_bytes = 0;
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
    block.columns.resize(schema.ColumnCount());
    const RowPage& row_page = page->body.row_page;
    const slot_t row_max = row_page.RowMax();
    block.slots.reserve(row_max);
    for (slot_t c = 0; c < schema.ColumnCount(); ++c) {
      block.columns[c].type = schema.GetColumn(c).Type();
      block.columns[c].is_unsigned = schema.GetColumn(c).IsUnsigned();
      if (block.columns[c].type != ValueType::kVarChar) {
        block.columns[c].int_cells.reserve(row_max);
      } else {
        block.columns[c].varchar_ends.reserve(row_max);
      }
    }

    if (!has_chains) {
      for (slot_t slot = 0; slot < row_max; ++slot) {
        const std::string_view image = row_page.GetRow(slot);
        if (image.empty()) {
          continue;
        }
        if (!DecodeRowImageIntoBlock(image, slot, schema, &block)) {
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
            !DecodeRowImageIntoBlock(visible.Value(), slot, schema, &block)) {
          return nullptr;
        }
      }
    }

    for (LSTableColumnBlock& col : block.columns) {
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

    max_page_id = std::max(max_page_id, page_id);
    store->page_ids.push_back(page_id);
    store->total_rows += block.row_count;
    for (const LSTableColumnBlock& col : block.columns) {
      estimated_bytes +=
          (col.int_cells.size() * sizeof(int64_t)) + col.varchar_bytes.size() +
          (col.varchar_ends.size() * sizeof(uint32_t)) + col.nulls.size();
    }
    if (store->total_rows > kMaxLSTableRows ||
        estimated_bytes > kMaxLSTableBytes) {
      return nullptr;
    }
    store->blocks.push_back(std::move(block));
    page_id = next_page;
  }

  if (table.LayoutGeneration() != generation || table.RowsRewrittenInPlace() ||
      txn.IsFailed()) {
    return nullptr;
  }

  constexpr page_id_t kMaxFlatPageId = 16'777'216ULL;
  if (max_page_id < kMaxFlatPageId &&
      store->blocks.size() <= std::numeric_limits<uint32_t>::max() - 1) {
    store->flat_page_to_block.assign(static_cast<size_t>(max_page_id + 1),
                                     LSTableStore::kInvalidBlockIndex);
    for (size_t idx = 0; idx < store->page_ids.size(); ++idx) {
      store->flat_page_to_block[store->page_ids[idx]] =
          static_cast<uint32_t>(idx);
    }
  } else {
    store->page_to_block.reserve(store->page_ids.size());
    for (size_t idx = 0; idx < store->page_ids.size(); ++idx) {
      store->page_to_block.emplace(store->page_ids[idx], idx);
    }
  }

  const slot_t col_count = schema.ColumnCount();
  store->dictionaries.resize(col_count);
  store->column_is_monotonic.assign(col_count, 0);
  store->column_is_unique_nonnull.assign(col_count, 0);
  store->column_is_identity_1_to_n.assign(col_count, 0);
  store->column_has_null.assign(col_count, 0);
  store->column_min_int.assign(col_count, std::numeric_limits<int64_t>::max());
  store->column_max_int.assign(col_count, std::numeric_limits<int64_t>::min());

  auto hash_sv = [](std::string_view sv) noexcept -> uint64_t {
    uint64_t h = 14695981039346656037ULL;
    for (const unsigned char ch : sv) {
      h ^= ch;
      h *= 1099511628211ULL;
    }
    return h == 0 ? 1ULL : h;
  };

  for (slot_t c = 0; c < col_count; ++c) {
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
            for (LSTablePageBlock& blk : store->blocks) {
              LSTableColumnBlock& col = blk.columns[c];
              col.dict = &dict;
              col.dict_codes.resize(blk.row_count);
              col.dict_mask = {0ULL, 0ULL, 0ULL, 0ULL};
              const int64_t* __restrict cells = col.int_cells.data();
              uint8_t* __restrict codes = col.dict_codes.data();
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
        for (LSTablePageBlock& blk : store->blocks) {
          LSTableColumnBlock& col = blk.columns[c];
          col.dict = &dict;
          col.dict_codes.resize(blk.row_count);
          col.dict_mask = {0ULL, 0ULL, 0ULL, 0ULL};
          const int64_t* __restrict cells = col.int_cells.data();
          uint8_t* __restrict codes = col.dict_codes.data();
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
            col.dict_mask[code >> 6] |= uint64_t{1} << (code & 63);
          }
        }
      }
      continue;
    }

    if (vt == ValueType::kVarChar && store->total_rows > 0) {
      bool all_blocks_single_byte = !any_null;
      if (all_blocks_single_byte) {
        for (const LSTablePageBlock& blk : store->blocks) {
          if (blk.row_count > 0 && !blk.columns[c].all_single_byte_varchar) {
            all_blocks_single_byte = false;
            break;
          }
        }
      }
      if (all_blocks_single_byte) {
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
          for (LSTablePageBlock& blk : store->blocks) {
            LSTableColumnBlock& col = blk.columns[c];
            col.dict = &dict;
            col.dict_codes.resize(blk.row_count);
            col.dict_mask = {0ULL, 0ULL, 0ULL, 0ULL};
            const uint8_t* __restrict bytes = col.varchar_bytes.data();
            uint8_t* __restrict codes = col.dict_codes.data();
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

      // General low-cardinality VARCHAR dictionary discovery (up to 254 codes).
      struct DictSlot {
        uint64_t hash{0};
        const char* data{nullptr};
        uint32_t size{0};
        uint8_t code{0};
      };
      constexpr size_t kTableSize = 512;
      constexpr size_t kTableMask = kTableSize - 1;
      std::array<DictSlot, kTableSize> table_slots{};
      std::vector<std::string_view> distinct_views;
      distinct_views.reserve(LSTableColumnDictionary::kMaxEntries);
      bool fits_dictionary = true;
      std::string_view prev_sv;
      bool have_prev_sv = false;

      for (const LSTablePageBlock& blk : store->blocks) {
        const LSTableColumnBlock& col = blk.columns[c];
        for (uint32_t r = 0; r < blk.row_count; ++r) {
          if (col.IsNull(r)) {
            continue;
          }
          const std::string_view sv = col.VarcharAt(r);
          if (have_prev_sv && sv == prev_sv) {
            continue;
          }
          prev_sv = sv;
          have_prev_sv = true;
          const uint64_t h = hash_sv(sv);
          size_t slot = static_cast<size_t>(h) & kTableMask;
          while (true) {
            if (table_slots[slot].hash == 0) {
              if (distinct_views.size() >=
                  LSTableColumnDictionary::kMaxEntries) {
                fits_dictionary = false;
                break;
              }
              table_slots[slot] =
                  DictSlot{.hash = h,
                           .data = sv.data(),
                           .size = static_cast<uint32_t>(sv.size()),
                           .code = static_cast<uint8_t>(distinct_views.size())};
              distinct_views.push_back(sv);
              break;
            }
            if (table_slots[slot].hash == h &&
                table_slots[slot].size == sv.size() &&
                (sv.empty() || std::memcmp(table_slots[slot].data, sv.data(),
                                           sv.size()) == 0)) {
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

      if (fits_dictionary && !distinct_views.empty()) {
        LSTableColumnDictionary& dict = store->dictionaries[c];
        dict.active = true;
        dict.entries.reserve(distinct_views.size());
        for (const std::string_view sv : distinct_views) {
          dict.entries.emplace_back(sv);
        }
        std::ranges::sort(dict.entries);
        dict.all_single_byte = !any_null;
        table_slots.fill(DictSlot{});
        for (size_t idx = 0; idx < dict.entries.size(); ++idx) {
          const std::string& s = dict.entries[idx];
          if (s.size() != 1) {
            dict.all_single_byte = false;
          } else {
            dict.single_byte_chars[idx] = static_cast<uint8_t>(s[0]);
          }
          const std::string_view sv(s);
          const uint64_t h = hash_sv(sv);
          size_t slot = static_cast<size_t>(h) & kTableMask;
          while (table_slots[slot].hash != 0) {
            slot = (slot + 1) & kTableMask;
          }
          table_slots[slot] = DictSlot{.hash = h,
                                       .data = sv.data(),
                                       .size = static_cast<uint32_t>(sv.size()),
                                       .code = static_cast<uint8_t>(idx)};
        }

        for (LSTablePageBlock& blk : store->blocks) {
          LSTableColumnBlock& col = blk.columns[c];
          col.dict_codes.resize(blk.row_count);
          col.dict_mask = {0ULL, 0ULL, 0ULL, 0ULL};
          uint8_t* __restrict codes = col.dict_codes.data();
          std::string_view last_sv;
          uint8_t last_code = LSTableColumnDictionary::kNullCode;
          bool have_last = false;
          for (uint32_t r = 0; r < blk.row_count; ++r) {
            if (col.IsNull(r)) {
              codes[r] = LSTableColumnDictionary::kNullCode;
              continue;
            }
            const std::string_view sv = col.VarcharAt(r);
            uint8_t code = last_code;
            if (!have_last || sv != last_sv) {
              const uint64_t h = hash_sv(sv);
              size_t slot = static_cast<size_t>(h) & kTableMask;
              while (true) {
                if (table_slots[slot].hash == h &&
                    table_slots[slot].size == sv.size() &&
                    (sv.empty() || std::memcmp(table_slots[slot].data,
                                               sv.data(), sv.size()) == 0)) {
                  code = table_slots[slot].code;
                  break;
                }
                slot = (slot + 1) & kTableMask;
              }
              last_sv = std::string_view(dict.entries[code]);
              last_code = code;
              have_last = true;
            }
            codes[r] = code;
            col.dict_mask[code >> 6] |= uint64_t{1} << (code & 63);
          }
          col.dict = &dict;
          if (!col.all_single_byte_varchar) {
            std::vector<uint8_t>().swap(col.varchar_bytes);
          }
          std::vector<uint32_t>().swap(col.varchar_ends);
        }
      }
    }
  }

  // Also build a PaxChunk for up to one row group (1024 rows) so
  // native PaxChunk metadata / codecs are materialized on the store.
  try {
    std::optional<Schema> lst_schema = ConvertToLSTableSchema(schema);
    if (lst_schema.has_value()) {
      store->lstable_schema = *lst_schema;
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
          store->pax_chunk = std::make_shared<const PaxChunk>(
              PaxChunk::BuildFromSortedRun(run, *lst_schema, false));
        }
      }
    }
  } catch (const std::exception& e) {
    LOG(WARN) << "PaxChunk sample construction skipped: " << e.what();
  } catch (...) {
    LOG(WARN) << "PaxChunk sample construction skipped: unknown exception";
  }

  if (store->uncommitted_owner_txn == 0 || table.GetLSTableStore() == nullptr) {
    table.SetLSTableStore(store);
  }
  return store;
}

std::shared_ptr<const LSTableStore> GetOrBuildLSTableStore(
    Transaction& txn, const Table& table, bool allow_lazy_build) {
  if (txn.IsFailed() || table.RowsRewrittenInPlace()) {
    return nullptr;
  }
  const uint64_t generation = table.LayoutGeneration();
  if (generation == 0) {
    return nullptr;
  }
  std::shared_ptr<const LSTableStore> current = table.GetLSTableStore();
  if (current != nullptr && current->layout_generation == generation &&
      current->first_pid == table.FirstPageId()) {
    if (current->uncommitted_owner_txn == 0 ||
        current->uncommitted_owner_txn == txn.ID()) {
      return current;
    }
    txn.SweepCommittedVersionChains();
    if (!txn.HasAnyPageVersionChains() &&
        (txn.IsReadOnly() || txn.WriteSetEmpty())) {
      return BuildLSTableStore(txn, table);
    }
    return nullptr;
  }
  if (!allow_lazy_build || (current != nullptr && current->total_rows > 0) ||
      (!txn.IsReadOnly() && !txn.WriteSetEmpty())) {
    return nullptr;
  }
  txn.SweepCommittedVersionChains();
  if (txn.HasAnyPageVersionChains()) {
    return nullptr;
  }
  return BuildLSTableStore(txn, table);
}

}  // namespace tinylamb
