/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/exception_status.hpp"
#include "common/parallel_work.hpp"
#include "common/platform/range_selection.hpp"
#include "common/query_parallelism.hpp"
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/scan_tuple_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/query_memory.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/proto_text.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "index/index_scan_iterator.hpp"
#include "query/statement.hpp"
#include "table/full_scan_iterator.hpp"
#include "table/iterator.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/column_name.hpp"
#include "type/date.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

namespace {

// Tiny exact sets need no data-dependent bitset lookup. Comparing a small
// fixed batch lets the compiler vectorize the membership test; NULL checks
// and selection writes only visit matches.
uint32_t SelectTinyRuntimeKeys(const LSTableColumnBlock& col, uint32_t count,
                               const std::vector<int64_t>& keys,
                               const RuntimeFilterGroup& filter,
                               uint32_t* selection) {
  std::array<int64_t, 4> values{};
  size_t key_count = 0;
  // ExactKeys is an enclosure: another conjunct can still reject a key.
  for (int64_t key : keys) {
    if (!filter.Reject(key)) { values[key_count++] = key;
}
  }
  if (key_count == 0) { return 0;
}
  std::fill(values.begin() + key_count, values.end(), values[0]);
  const int64_t* cells = col.int_cells.data();
  uint32_t selected = 0;
  std::array<uint8_t, 64> hits{};
  for (uint32_t begin = 0; begin < count; begin += hits.size()) {
    const uint32_t n = std::min<uint32_t>(hits.size(), count - begin);
    for (uint32_t i = 0; i < n; ++i) {
      const int64_t v = cells[begin + i];
      hits[i] = static_cast<uint8_t>(v == values[0]) |
                static_cast<uint8_t>(v == values[1]) |
                static_cast<uint8_t>(v == values[2]) |
                static_cast<uint8_t>(v == values[3]);
    }
    for (uint32_t i = 0; i < n; ++i) {
      if ((hits[i] != 0U) && !col.IsNull(begin + i)) {
        selection[selected++] = begin + i;
      }
    }
  }
  return selected;
}

uint32_t SelectIntegerRange(const LSTableColumnBlock& col, uint32_t count,
                            int64_t lower, int64_t upper, uint32_t* selection) {
  uint32_t selected = platform::SelectInt64ClosedRange(
      col.int_cells.data(), count, lower, upper, selection);
  if (col.has_null) {
    uint32_t nonnull = 0;
    for (uint32_t i = 0; i < selected; ++i) {
      if (!col.IsNull(selection[i])) { selection[nonnull++] = selection[i];
}
}
    selected = nonnull;
  }
  return selected;
}

template <typename Left, typename Right>
bool ComparePositional(BinaryOperation op, const Left& left,
                       const Right& right) {
  switch (op) {
    case BinaryOperation::kEquals:
      return left == right;
    case BinaryOperation::kNotEquals:
      return left != right;
    case BinaryOperation::kLessThan:
      return left < right;
    case BinaryOperation::kLessThanEquals:
      return left <= right;
    case BinaryOperation::kGreaterThan:
      return left > right;
    case BinaryOperation::kGreaterThanEquals:
      return left >= right;
    default:
      return false;
  }
}

Value ReadLSTableCellValue(const LSTableColumnBlock& col, uint32_t r) {
  if (col.IsNull(r) || col.type == ValueType::kNull) {
    return {};
  }
  Value v;
  switch (col.type) {
    case ValueType::kInt64:
      v.SetInt64(col.IntAt(r));
      if (col.is_unsigned) {
        v.SetUnsigned();
      }
      return v;
    case ValueType::kDate:
      v.SetInt64(col.IntAt(r));
      v.type = ValueType::kDate;
      return v;
    case ValueType::kDouble:
      if (r < col.int_cells.size()) {
        v.SetDouble(std::bit_cast<double>(col.int_cells[r]));
      }
      return v;
    case ValueType::kVarChar:
      v.CopyFromStringView(col.VarcharAt(r));
      return v;
    default:
      return {};
  }
}

void AppendLSTableColumnSlice(const LSTableColumnBlock& blk_col,
                              const std::shared_ptr<const void>& owner,
                              size_t src_offset, const uint32_t* sel,
                              size_t count, ColumnarColumn* dst,
                              size_t prev_rows);

void AppendLSTableSubstringSlice(const LSTableColumnBlock& blk_col,
                                 const std::shared_ptr<const void>& owner,
                                 size_t src_offset, const uint32_t* sel,
                                 size_t count, int64_t start, int64_t length,
                                 bool has_length, ColumnarColumn* dst,
                                 size_t prev_rows) {
  if (count == 0) {
    return;
  }
  if (blk_col.type == ValueType::kNull) {
    if (prev_rows == 0 && dst->IsFlat() && !dst->is_interval &&
        (dst->type == ValueType::kNull || dst->type == ValueType::kVarChar)) {
      dst->SetConstantNull(ValueType::kVarChar, false, false, count);
      return;
    }
    dst->AppendConstant(Value(), count, nullptr, prev_rows);
    return;
  }
  if (blk_col.type != ValueType::kVarChar) {
    ColumnarColumn tmp;
    AppendLSTableColumnSlice(blk_col, owner, src_offset, sel, count, &tmp, 0);
    if (!dst->AppendSubstringFrom(tmp, 0, nullptr, count, start, length,
                                  has_length, prev_rows)) {
      dst->AppendConstant(Value(), count, nullptr, prev_rows);
    }
    return;
  }
  if (blk_col.dict != nullptr && blk_col.is_const_code && !blk_col.has_null &&
      blk_col.const_code < blk_col.dict->StringEntryCount()) {
    const std::string_view sliced =
        SliceUtf8SubstringView(blk_col.dict->StringEntryAt(blk_col.const_code),
                               start, length, has_length);
    if (prev_rows == 0 && dst->IsFlat()) {
      dst->SetConstantString(sliced, false, count, owner);
      return;
    }
    if (dst->encoding == ColumnarColumn::Encoding::kConstant &&
        !dst->const_is_null && dst->type == ValueType::kVarChar &&
        !dst->is_interval && dst->const_string == sliced) {
      dst->const_rows = std::max(dst->const_rows, prev_rows);
      dst->const_rows += count;
      return;
    }
  }
  if (dst->encoding != ColumnarColumn::Encoding::kFlat) {
    dst->MaterializeToFlat(prev_rows, nullptr);
  }
  if (dst->is_uniform && dst->type == ValueType::kNull) {
    dst->UpgradeNullTo(ValueType::kVarChar, false, false, prev_rows);
  }
  if (!dst->is_uniform || dst->type != ValueType::kVarChar ||
      dst->is_interval) {
    dst->DemoteToFallback(prev_rows);
    dst->fallback.reserve(dst->fallback.size() + count);
    for (size_t i = 0; i < count; ++i) {
      const uint32_t r =
          sel != nullptr ? sel[i] : static_cast<uint32_t>(src_offset + i);
      if (blk_col.has_null && blk_col.IsNull(r)) {
        dst->fallback.emplace_back();
      } else {
        dst->fallback.emplace_back(std::string(SliceUtf8SubstringView(
            blk_col.VarcharAt(r), start, length, has_length)));
      }
    }
    return;
  }
  if (blk_col.has_null && !dst->has_null) {
    dst->EnsureNullBitmap(prev_rows);
  }
  if (dst->has_null) {
    dst->nulls.resize(prev_rows + count, 0);
  }
  dst->strings.resize(prev_rows + count);
  std::string_view* __restrict dst_s = dst->strings.data() + prev_rows;
  if (blk_col.dict != nullptr && blk_col.dict->active &&
      !blk_col.is_const_code && !blk_col.dict_codes.empty() &&
      blk_col.dict->StringEntryCount() <= 4096) {
    const size_t entry_cnt = blk_col.dict->StringEntryCount();
    std::vector<std::string_view> dict_sliced(entry_cnt);
    for (size_t e = 0; e < entry_cnt; ++e) {
      dict_sliced[e] = SliceUtf8SubstringView(blk_col.dict->StringEntryAt(e),
                                              start, length, has_length);
    }
    const uint8_t* __restrict dc = blk_col.dict_codes.data();
    for (size_t i = 0; i < count; ++i) {
      const uint32_t r =
          sel != nullptr ? sel[i] : static_cast<uint32_t>(src_offset + i);
      const uint8_t c = dc[r];
      if ((blk_col.has_null && blk_col.IsNull(r)) || c >= entry_cnt ||
          c == LSTableColumnDictionary::kNullCode) {
        if (!dst->has_null) {
          dst->EnsureNullBitmap(prev_rows + count);
        }
        dst->nulls[prev_rows + i] = 1;
      } else {
        dst_s[i] = dict_sliced[c];
      }
    }
    return;
  }
  for (size_t i = 0; i < count; ++i) {
    const uint32_t r =
        sel != nullptr ? sel[i] : static_cast<uint32_t>(src_offset + i);
    if (blk_col.has_null && blk_col.IsNull(r)) {
      if (!dst->has_null) {
        dst->EnsureNullBitmap(prev_rows + count);
      }
      dst->nulls[prev_rows + i] = 1;
    } else {
      dst_s[i] = SliceUtf8SubstringView(blk_col.VarcharAt(r), start, length,
                                        has_length);
    }
  }
}

void AppendLSTableColumnSlice(const LSTableColumnBlock& blk_col,
                              const std::shared_ptr<const void>& owner,
                              size_t src_offset, const uint32_t* sel,
                              size_t count, ColumnarColumn* dst,
                              size_t prev_rows) {
  if (count == 0) {
    return;
  }
  if (blk_col.type == ValueType::kNull) {
    dst->AppendConstant(Value(), count, nullptr, prev_rows);
    return;
  }
  const uint8_t* src_nulls =
      !blk_col.nulls_view.empty()
          ? blk_col.nulls_view.data()
          : (blk_col.nulls.empty() ? nullptr : blk_col.nulls.data());

  // Preserve dictionary encoding zero-copy / code-only across scan blocks
  // so downstream filters, joins, and GroupBy can reuse the immutable
  // LSTableColumnDictionary directly without copying or hashing strings.
  if (blk_col.type == ValueType::kVarChar && blk_col.dict != nullptr &&
      dst->is_uniform && !dst->is_interval &&
      (dst->type == ValueType::kNull || dst->type == ValueType::kVarChar)) {
    const size_t entry_cnt = blk_col.dict->StringEntryCount();
    if (blk_col.is_const_code && !blk_col.has_null && prev_rows == 0 &&
        count == kColumnarChunkRows && blk_col.const_code < entry_cnt) {
      dst->SetConstantString(blk_col.dict->StringEntryAt(blk_col.const_code),
                             false, count, owner);
      return;
    }
    const bool dst_empty_or_same_dict =
        (prev_rows == 0 && dst->IsFlat()) ||
        (dst->encoding == ColumnarColumn::Encoding::kDictionary &&
         dst->dict == blk_col.dict);
    if (dst_empty_or_same_dict) {
      if (blk_col.dict->active && !blk_col.is_const_code &&
          !blk_col.dict_codes.empty()) {
        if (prev_rows == 0 && sel == nullptr &&
            src_offset + count <= blk_col.dict_codes.size() &&
            (!blk_col.has_null || src_nulls != nullptr)) {
          dst->SetBorrowedNarrowDict(blk_col.dict, owner, ValueType::kVarChar,
                                     false, false, blk_col.has_null,
                                     blk_col.dict_codes.data() + src_offset,
                                     (blk_col.has_null && src_nulls != nullptr)
                                         ? (src_nulls + src_offset)
                                         : nullptr,
                                     count);
          return;
        }
        if (dst->encoding == ColumnarColumn::Encoding::kDictionary &&
            dst->borrowed_dict_u8 != nullptr) {
          dst->MaterializeDictCodes(prev_rows);
        }
        if (prev_rows == 0) {
          dst->Init(ValueType::kVarChar, false, false, 0);
          dst->encoding = ColumnarColumn::Encoding::kDictionary;
          dst->dict = blk_col.dict;
        }
        if (dst->encoding_owner == nullptr) {
          dst->encoding_owner = owner;
        }
        dst->has_null = dst->has_null || blk_col.has_null;
        const size_t base = dst->dict_codes.size();
        if (prev_rows > 0 || (sel != nullptr && count < kColumnarChunkRows)) {
          if (dst->dict_codes.capacity() < kColumnarChunkRows) {
            dst->dict_codes.reserve(kColumnarChunkRows);
          }
        }
        dst->dict_codes.resize(base + count);
        uint32_t* __restrict out = dst->dict_codes.data() + base;
        const uint8_t* __restrict dc = blk_col.dict_codes.data();
        if (!blk_col.has_null) {
          if (sel == nullptr) {
            for (size_t i = 0; i < count; ++i) {
              const uint8_t c = dc[src_offset + i];
              out[i] = c < entry_cnt ? static_cast<uint32_t>(c)
                                     : ColumnarColumn::kNullDictCode;
            }
          } else {
            for (size_t i = 0; i < count; ++i) {
              const uint8_t c = dc[sel[i]];
              out[i] = c < entry_cnt ? static_cast<uint32_t>(c)
                                     : ColumnarColumn::kNullDictCode;
            }
          }
        } else {
          if (sel == nullptr) {
            for (size_t i = 0; i < count; ++i) {
              const size_t r = src_offset + i;
              const uint8_t c = dc[r];
              out[i] = (blk_col.IsNull(r) || c >= entry_cnt ||
                        c == LSTableColumnDictionary::kNullCode)
                           ? ColumnarColumn::kNullDictCode
                           : static_cast<uint32_t>(c);
            }
          } else {
            for (size_t i = 0; i < count; ++i) {
              const uint32_t r = sel[i];
              const uint8_t c = dc[r];
              out[i] = (blk_col.IsNull(r) || c >= entry_cnt ||
                        c == LSTableColumnDictionary::kNullCode)
                           ? ColumnarColumn::kNullDictCode
                           : static_cast<uint32_t>(c);
            }
          }
        }
        return;
      }
      if (blk_col.is_const_code || blk_col.dict->wide_code_bytes != 0) {
        if (dst->encoding == ColumnarColumn::Encoding::kDictionary &&
            dst->borrowed_dict_u8 != nullptr) {
          dst->MaterializeDictCodes(prev_rows);
        }
        if (prev_rows == 0) {
          dst->Init(ValueType::kVarChar, false, false, 0);
          dst->encoding = ColumnarColumn::Encoding::kDictionary;
          dst->dict = blk_col.dict;
        }
        if (dst->encoding_owner == nullptr) {
          dst->encoding_owner = owner;
        }
        dst->has_null = dst->has_null || blk_col.has_null;
        const size_t base = dst->dict_codes.size();
        if (prev_rows > 0 || (sel != nullptr && count < kColumnarChunkRows)) {
          if (dst->dict_codes.capacity() < kColumnarChunkRows) {
            dst->dict_codes.reserve(kColumnarChunkRows);
          }
        }
        dst->dict_codes.resize(base + count);
        uint32_t* __restrict out = dst->dict_codes.data() + base;
        if (!blk_col.has_null) {
          if (blk_col.is_const_code) {
            const uint32_t c = blk_col.const_code < entry_cnt
                                   ? blk_col.const_code
                                   : ColumnarColumn::kNullDictCode;
            for (size_t i = 0; i < count; ++i) {
              out[i] = c;
            }
          } else if (sel == nullptr) {
            for (size_t i = 0; i < count; ++i) {
              const uint32_t c = blk_col.DictionaryCodeAt(src_offset + i);
              out[i] = c < entry_cnt ? c : ColumnarColumn::kNullDictCode;
            }
          } else {
            for (size_t i = 0; i < count; ++i) {
              const uint32_t c = blk_col.DictionaryCodeAt(sel[i]);
              out[i] = c < entry_cnt ? c : ColumnarColumn::kNullDictCode;
            }
          }
        } else if (sel == nullptr) {
          for (size_t i = 0; i < count; ++i) {
            const size_t r = src_offset + i;
            const uint32_t c = blk_col.DictionaryCodeAt(r);
            out[i] = (blk_col.IsNull(r) || c >= entry_cnt)
                         ? ColumnarColumn::kNullDictCode
                         : c;
          }
        } else {
          for (size_t i = 0; i < count; ++i) {
            const uint32_t r = sel[i];
            const uint32_t c = blk_col.DictionaryCodeAt(r);
            out[i] = (blk_col.IsNull(r) || c >= entry_cnt)
                         ? ColumnarColumn::kNullDictCode
                         : c;
          }
        }
        return;
      }
    }
  }

  // Zero-copy Frame-of-Reference / constant / borrowed raw int slices.
  if ((blk_col.type == ValueType::kInt64 || blk_col.type == ValueType::kDate) &&
      dst->is_uniform && !dst->is_interval &&
      (dst->type == ValueType::kNull ||
       (dst->type == blk_col.type &&
        dst->is_unsigned == blk_col.is_unsigned))) {
    if (!blk_col.has_null && !blk_col.HasRawIntCells() &&
        blk_col.for_width == 0) {
      if (prev_rows == 0 && dst->IsFlat()) {
        dst->SetConstantInt(blk_col.type, blk_col.is_unsigned, blk_col.for_base,
                            count);
        return;
      }
      if (dst->encoding == ColumnarColumn::Encoding::kConstant &&
          !dst->const_is_null && dst->type == blk_col.type &&
          dst->is_unsigned == blk_col.is_unsigned &&
          dst->const_int == blk_col.for_base) {
        dst->const_rows = std::max(dst->const_rows, prev_rows);
        dst->const_rows += count;
        return;
      }
    }
    if (prev_rows == 0 && dst->IsFlat() && sel == nullptr &&
        (!blk_col.has_null || src_nulls != nullptr)) {
      if (!blk_col.HasRawIntCells() &&
          (blk_col.for_width == 1 || blk_col.for_width == 2 ||
           blk_col.for_width == 4) &&
          blk_col.for_bytes.size() >=
              (src_offset + count) * blk_col.for_width) {
        dst->SetBorrowedFrameOfReference(
            owner, blk_col.type, blk_col.is_unsigned, blk_col.for_width,
            blk_col.for_base,
            blk_col.for_bytes.data() + (src_offset * blk_col.for_width),
            blk_col.has_null,
            (blk_col.has_null && src_nulls != nullptr)
                ? (src_nulls + src_offset)
                : nullptr,
            count);
        return;
      }
      if (blk_col.HasRawIntCells() &&
          src_offset + count <= blk_col.int_cells.size()) {
        dst->SetBorrowedFrameOfReference(
            owner, blk_col.type, blk_col.is_unsigned, 8, 0,
            reinterpret_cast<const uint8_t*>(blk_col.int_cells.data() +
                                             src_offset),
            blk_col.has_null,
            (blk_col.has_null && src_nulls != nullptr)
                ? (src_nulls + src_offset)
                : nullptr,
            count);
        return;
      }
    }
  }

  if (dst->encoding != ColumnarColumn::Encoding::kFlat) {
    dst->MaterializeToFlat(prev_rows);
  }
  if (dst->is_uniform && dst->type == ValueType::kNull) {
    dst->UpgradeNullTo(blk_col.type, blk_col.is_unsigned, false, prev_rows);
  }
  const bool double_missing_cells =
      blk_col.type == ValueType::kDouble && blk_col.int_cells.empty();
  if (!dst->is_uniform || dst->type != blk_col.type ||
      dst->is_unsigned != blk_col.is_unsigned || dst->is_interval ||
      double_missing_cells) {
    dst->DemoteToFallback(prev_rows);
    dst->fallback.reserve(dst->fallback.size() + count);
    if (sel == nullptr) {
      for (size_t i = 0; i < count; ++i) {
        dst->fallback.push_back(ReadLSTableCellValue(
            blk_col, static_cast<uint32_t>(src_offset + i)));
      }
    } else {
      for (size_t i = 0; i < count; ++i) {
        dst->fallback.push_back(ReadLSTableCellValue(blk_col, sel[i]));
      }
    }
    return;
  }

  const bool accumulate_chunk =
      prev_rows > 0 || (sel != nullptr && count < kColumnarChunkRows);
  if (blk_col.has_null && src_nulls != nullptr) {
    if (accumulate_chunk && dst->nulls.capacity() < kColumnarChunkRows) {
      dst->nulls.reserve(kColumnarChunkRows);
    }
    dst->EnsureNullBitmap(prev_rows);
    const size_t nbase = dst->nulls.size();
    dst->nulls.resize(nbase + count);
    if (sel == nullptr) {
      std::memcpy(dst->nulls.data() + nbase, src_nulls + src_offset, count);
    } else {
      uint8_t* __restrict dn = dst->nulls.data() + nbase;
      for (size_t i = 0; i < count; ++i) {
        dn[i] = src_nulls[sel[i]];
      }
    }
  } else if (dst->has_null) {
    if (accumulate_chunk && dst->nulls.capacity() < kColumnarChunkRows) {
      dst->nulls.reserve(kColumnarChunkRows);
    }
    dst->nulls.resize(prev_rows + count, 0);
  }

  switch (blk_col.type) {
    case ValueType::kInt64:
    case ValueType::kDate: {
      const size_t base = dst->ints.size();
      if (accumulate_chunk && dst->ints.capacity() < kColumnarChunkRows) {
        dst->ints.reserve(kColumnarChunkRows);
      }
      dst->ints.resize(base + count);
      int64_t* __restrict di = dst->ints.data() + base;
      if (blk_col.HasRawIntCells()) {
        const int64_t* __restrict src = blk_col.int_cells.data();
        if (sel == nullptr) {
          std::memcpy(di, src + src_offset, count * sizeof(int64_t));
        } else {
          for (size_t i = 0; i < count; ++i) {
            di[i] = src[sel[i]];
          }
        }
      } else if (sel == nullptr) {
        blk_col.MaterializeIntSlice(src_offset, count, di);
      } else {
        const auto base_u = static_cast<uint64_t>(blk_col.for_base);
        switch (blk_col.for_width) {
          case 0:
            for (size_t i = 0; i < count; ++i) {
              di[i] = blk_col.for_base;
            }
            break;
          case 1: {
            const uint8_t* __restrict fb = blk_col.for_bytes.data();
            for (size_t i = 0; i < count; ++i) {
              di[i] = static_cast<int64_t>(base_u + fb[sel[i]]);
            }
            break;
          }
          case 2: {
            const uint8_t* __restrict fb = blk_col.for_bytes.data();
            for (size_t i = 0; i < count; ++i) {
              uint16_t v = 0;
              std::memcpy(&v, fb + (static_cast<size_t>(sel[i]) * 2),
                          sizeof(v));
              di[i] = static_cast<int64_t>(base_u + v);
            }
            break;
          }
          case 4: {
            const uint8_t* __restrict fb = blk_col.for_bytes.data();
            for (size_t i = 0; i < count; ++i) {
              uint32_t v = 0;
              std::memcpy(&v, fb + (static_cast<size_t>(sel[i]) * 4),
                          sizeof(v));
              di[i] = static_cast<int64_t>(base_u + v);
            }
            break;
          }
          default:
            std::memset(di, 0, count * sizeof(int64_t));
            break;
        }
      }
      break;
    }
    case ValueType::kDouble: {
      const size_t base = dst->doubles.size();
      if (accumulate_chunk && dst->doubles.capacity() < kColumnarChunkRows) {
        dst->doubles.reserve(kColumnarChunkRows);
      }
      dst->doubles.resize(base + count);
      double* __restrict dd = dst->doubles.data() + base;
      const int64_t* __restrict src = blk_col.int_cells.data();
      if (sel == nullptr) {
        std::memcpy(dd, src + src_offset, count * sizeof(double));
      } else {
        for (size_t i = 0; i < count; ++i) {
          dd[i] = std::bit_cast<double>(src[sel[i]]);
        }
      }
      break;
    }
    case ValueType::kVarChar: {
      const size_t base = dst->strings.size();
      dst->strings.resize(base + count);
      std::string_view* ds = dst->strings.data() + base;
      if (!blk_col.has_null) {
        if (sel == nullptr) {
          for (size_t i = 0; i < count; ++i) {
            ds[i] = blk_col.VarcharAt(src_offset + i);
          }
        } else {
          for (size_t i = 0; i < count; ++i) {
            ds[i] = blk_col.VarcharAt(sel[i]);
          }
        }
      } else {
        if (sel == nullptr) {
          for (size_t i = 0; i < count; ++i) {
            const size_t r = src_offset + i;
            if (!blk_col.IsNull(r)) {
              ds[i] = blk_col.VarcharAt(r);
            }
          }
        } else {
          for (size_t i = 0; i < count; ++i) {
            const uint32_t r = sel[i];
            if (!blk_col.IsNull(r)) {
              ds[i] = blk_col.VarcharAt(r);
            }
          }
        }
      }
      break;
    }
    default:
      break;
  }
}

struct ColumnarScanScratch {
  std::vector<std::vector<int64_t>> int_cols;
  std::vector<std::vector<std::string_view>> str_cols;
  std::vector<uint8_t> all_null_buf;
  std::vector<BytecodeColumnView> blk_views;
  std::vector<BytecodeColumnView> slice_views;
  Row fallback_row;
};

Schema RefineSchemaFromColumnarTable(const Schema& base_schema,
                                     const ColumnarTable& table,
                                     bool* all_uniform_non_null) {
  const size_t ncols = base_schema.ColumnCount();
  std::vector<Column> cols;
  cols.reserve(ncols);
  for (size_t c = 0; c < ncols; ++c) {
    cols.push_back(base_schema.GetColumn(c));
  }
  bool uniform_non_null = ncols > 0 && table.total_rows > 0;
  if (table.total_rows > 0) {
    for (size_t c = 0; c < ncols; ++c) {
      bool saw_non_null = false;
      bool uniform = true;
      ValueType vt = ValueType::kNull;
      bool is_unsigned = false;
      bool is_interval = false;
      for (const ColumnarChunk& chunk : table.chunks) {
        if (chunk.rows == 0 || c >= chunk.columns.size()) {
          continue;
        }
        const ColumnarColumn& col = chunk.columns[c];
        if (!col.is_uniform) {
          Value v;
          for (size_t r = 0; r < chunk.rows; ++r) {
            col.WriteValueInto(r, &v);
            if (v.IsNull()) {
              continue;
            }
            if (!saw_non_null) {
              saw_non_null = true;
              vt = v.type;
              is_unsigned = v.IsUnsigned();
              is_interval = v.IsInterval();
            } else if (v.type != vt || v.IsUnsigned() != is_unsigned ||
                       v.IsInterval() != is_interval) {
              uniform = false;
              break;
            }
          }
          if (!uniform) {
            break;
          }
          continue;
        }
        if (col.type == ValueType::kNull || !col.AnyNonNull(chunk.rows)) {
          continue;
        }
        if (!saw_non_null) {
          saw_non_null = true;
          vt = col.type;
          is_unsigned = col.is_unsigned;
          is_interval = col.is_interval;
        } else if (col.type != vt || col.is_unsigned != is_unsigned ||
                   col.is_interval != is_interval) {
          uniform = false;
          break;
        }
      }
      if (!uniform) {
        cols[c] = Column(cols[c].Name(), ValueType::kNull);
        uniform_non_null = false;
      } else if (saw_non_null) {
        cols[c].SetType(vt);
        cols[c].SetUnsigned(is_unsigned);
        cols[c].SetInterval(is_interval);
      } else {
        uniform_non_null = false;
      }
    }
  } else {
    uniform_non_null = false;
  }
  if (all_uniform_non_null != nullptr) {
    *all_uniform_non_null = uniform_non_null;
  }
  return {base_schema.Name(), std::move(cols)};
}

}  // namespace

bool TryParallelTableScan(
    TransactionContext& context, Table& table,
    const std::vector<slot_t>* projection,
    const RuntimeFilterGroup* runtime_filter, ValueType runtime_filter_type,
    std::optional<slot_t> full_key_column, bool filter_during_scan,
    const CompiledScanFilter* scan_filter, const Schema& result_schema,
    const Scope* outer, const CteMap& ctes, Relation* result, Status* error,
    const ScanTupleFilter* tuple_filter, bool columnar_output,
    const PushedScanProjection* pushed_projection,
    bool* pushed_projection_applied) {
  if (pushed_projection_applied != nullptr) {
    *pushed_projection_applied = false;
  }
  // A page-wide table can have many morsels but few rows. Starting sixteen
  // threads for each 10k-row outer/inner scan costs more than the selective
  // semi-join itself. Statistics choose scheduling only, never scan bounds.
  constexpr size_t kRowsPerScanWorker = 8192;
  size_t row_workers = 16;
  auto stats = context.GetStats(table.GetSchema().Name());
  if (stats.HasValue() && stats.Value()->Rows() > 0) {
    row_workers =
        std::max<size_t>(1, stats.Value()->Rows() / kRowsPerScanWorker);
  }
  std::shared_ptr<const LSTableStore> lstable_store;
  const RuntimeFilterGroup* active_lstable_rf = nullptr;
  {
    std::vector<slot_t> needed;
    if (projection != nullptr) {
      needed = *projection;
      if (full_key_column) {
        needed.push_back(*full_key_column);
      }
    }
    std::shared_ptr<const LSTableStore> store = GetOrBuildLSTableStore(
        context.txn_, table, true, projection != nullptr ? &needed : nullptr);
    if (store != nullptr &&
        store->layout_generation == table.LayoutGeneration() &&
        store->first_pid == table.FirstPageId()) {
      bool valid = true;
      if (store->uncommitted_owner_txn != 0) {
        valid = (store->uncommitted_owner_txn == context.txn_.ID());
      } else if (!context.txn_.IsReadOnly() && !context.txn_.WriteSetEmpty()) {
        valid = false;
      } else if (context.txn_.HasAnyPageVersionChains()) {
        for (const page_id_t pid : store->page_ids) {
          if (context.txn_.HasPageVersionChains(pid) ||
              store->FindBlock(pid) == nullptr) {
            valid = false;
            break;
          }
        }
      }
      if (valid) {
        lstable_store = std::move(store);
        if (runtime_filter != nullptr && !runtime_filter->empty() &&
            full_key_column.has_value() &&
            *full_key_column < table.GetSchema().ColumnCount()) {
          const Column& col = table.GetSchema().GetColumn(*full_key_column);
          if ((col.Type() == ValueType::kInt64 ||
               col.Type() == ValueType::kDate) &&
              !col.IsUnsigned() &&
              // kNull means the build side's type was never observed: only
              // a group that rejects unconditionally is sound on an
              // arbitrary INT64/DATE column (the AcceptsColumn contract).
              (runtime_filter_type == col.Type() ||
               (runtime_filter_type == ValueType::kNull &&
                runtime_filter->RejectsAll()))) {
            active_lstable_rf = runtime_filter;
          }
        }
      }
    }
  }

  // A proof over another scan image cannot justify pruning this scan.
  if (tuple_filter != nullptr && tuple_filter->store != lstable_store) {
    tuple_filter = nullptr;
  }

  std::vector<Table::ScanMorsel> morsels;
  constexpr size_t kMaxParallelScanWorkers = 16;
  size_t workers = 1;
  size_t lstable_blocks_per_morsel = 8;
  if (lstable_store != nullptr) {
    lstable_blocks_per_morsel =
        std::clamp<size_t>(lstable_store->blocks.size() / 64, 1, 8);
    const size_t block_morsels = std::max<size_t>(
        1, (lstable_store->blocks.size() + lstable_blocks_per_morsel - 1) /
               lstable_blocks_per_morsel);
    if (lstable_store->total_rows < 4096 || block_morsels < 2) {
      workers = 1;
    } else {
      const size_t lstable_row_workers =
          std::max<size_t>(2, lstable_store->total_rows / 4096);
      workers = std::min<size_t>(
          {kMaxParallelScanWorkers,
           QueryParallelWorkerLimit(std::thread::hardware_concurrency()),
           block_morsels, lstable_row_workers});
      workers = std::max<size_t>(1, workers);
    }
  } else {
    if (row_workers <= 1) {
      return false;
    }
    Status morsel_status = Status::kSuccess;
    morsels = table.BuildScanMorsels(context.txn_, 8, &morsel_status);
    if (morsel_status != Status::kSuccess) {
      if (error != nullptr) {
        *error = std::move(morsel_status);
      }
      return false;
    }
    workers = std::min<size_t>(
        {kMaxParallelScanWorkers,
         QueryParallelWorkerLimit(std::thread::hardware_concurrency()),
         std::max<size_t>(1, morsels.size()), row_workers});
    if (workers <= 1 || morsels.size() < 8) {
      return false;
    }
  }

  std::vector<Relation> shards(workers);
  // These counters are updated for every accepted/rejected row. Adjacent
  // size_t arrays make workers invalidate the same cache line continuously.
  struct alignas(64) ScanCounters {
    size_t seen{0};
    size_t out{0};
    size_t transfer_tested{0};
    size_t transfer_rejected{0};
  };
  std::vector<ScanCounters> shard_counts(workers);
  std::vector<uint8_t> worker_uniform_ok(workers,
                                         lstable_store != nullptr ? 1 : 0);
  // When every non-simple filter component compiled to context-free
  // bytecode, workers decide rows themselves; only rows whose program
  // raised are deferred for a contextual MatchScanFilter re-check.
  // Simple-only filters keep the original tight inline loop: the generic
  // worker evaluator adds measurable per-row overhead on multi-million-row
  // scans with nothing non-simple to evaluate.
  static const bool worker_filter_disabled =
      std::getenv("TINYLAMB_DISABLE_WORKER_FILTER") != nullptr;
  const bool has_post_simple_filter =
      filter_during_scan && scan_filter != nullptr &&
      (scan_filter->compile_decided_reject ||
       scan_filter->needs_unsigned_tagging || !scan_filter->prefix_in.empty() ||
       !scan_filter->prebuilt_membership.empty() ||
       !scan_filter->prebuilt_scalar_compare.empty() ||
       !scan_filter->residual.empty() ||
       !scan_filter->disjunctive_branches.empty());
  const WorkerScanFilterPrograms worker_programs =
      has_post_simple_filter && !worker_filter_disabled
          ? CompileWorkerFilter(*scan_filter, result_schema)
          : WorkerScanFilterPrograms{};
  std::vector<std::vector<Row>> deferred_rows(worker_programs.complete ? workers
                                                                       : 0);
  std::mutex error_mu;
  Status error_ex{Status::kSuccess};
  std::optional<std::vector<slot_t>> proj_opt;
  if (projection != nullptr) {
    proj_opt = *projection;
  }

  const auto integer_peeks =
      filter_during_scan && scan_filter != nullptr
          ? BuildIntegerPeeks(*scan_filter, projection, table.GetSchema())
          : std::vector<IntegerPeekCompare>{};
  if (lstable_store != nullptr &&
      std::ranges::any_of(integer_peeks, [&](const auto& compare) {
        return compare.op == BinaryOperation::kEquals &&
               !lstable_store->MayContainInteger(compare.column,
                                                 compare.constant);
      })) {
    result->schema = result_schema;
    *error = Status::kSuccess;
    return true;
  }

  struct ScanIntInterval {
    slot_t column;
    int64_t lo{std::numeric_limits<int64_t>::min()};
    int64_t hi{std::numeric_limits<int64_t>::max()};
    bool empty{false};
  };
  std::vector<ScanIntInterval> scan_intervals;
  for (const auto& peek : integer_peeks) {
    if (peek.op == BinaryOperation::kNotEquals) { continue;
}
    auto it = std::ranges::find(scan_intervals, peek.column,
                                &ScanIntInterval::column);
    if (it == scan_intervals.end()) {
      scan_intervals.push_back({peek.column});
      it = std::prev(scan_intervals.end());
    }
    const int64_t k = peek.constant;
    switch (peek.op) {
      case BinaryOperation::kEquals:
        it->lo = std::max(it->lo, k);
        it->hi = std::min(it->hi, k);
        break;
      case BinaryOperation::kGreaterThanEquals:
        it->lo = std::max(it->lo, k);
        break;
      case BinaryOperation::kLessThanEquals:
        it->hi = std::min(it->hi, k);
        break;
      case BinaryOperation::kGreaterThan:
        if (k == std::numeric_limits<int64_t>::max()) {
          it->empty = true;
        } else {
          it->lo = std::max(it->lo, k + 1);
}
        break;
      case BinaryOperation::kLessThan:
        if (k == std::numeric_limits<int64_t>::min()) {
          it->empty = true;
        } else {
          it->hi = std::min(it->hi, k - 1);
}
        break;
      default:
        break;
    }
    it->empty = it->empty || it->lo > it->hi;
  }

  struct LSTableDictColFilter {
    slot_t column{0};
    bool allow_null{false};
    std::array<uint64_t, 4> pass_mask{};
    std::array<uint8_t, 256> pass{};
  };
  struct LSTableWideDictFilter {
    slot_t column{0};
    bool allow_null{false};
    std::vector<uint8_t> pass;
  };
  struct DisjBranchFilter {
    std::vector<LSTableDictColFilter> narrow_cols;
    std::vector<LSTableWideDictFilter> wide_cols;
  };
  struct LSTableDoubleZonePredicate {
    slot_t column{0};
    BinaryOperation op{BinaryOperation::kEquals};
    double constant{0.0};
  };
  struct LSTableIntColCompare {
    slot_t left_col{0};
    slot_t right_col{0};
    BinaryOperation op{BinaryOperation::kEquals};
  };
  struct LSTableExtraRf {
    slot_t column{0};
    const RuntimeFilterGroup* group{nullptr};
  };
  std::vector<LSTableDictColFilter> lstable_dict_filters;
  std::vector<LSTableWideDictFilter> lstable_wide_filters;
  std::vector<DisjBranchFilter> lstable_disj_branches;
  std::vector<LSTableDoubleZonePredicate> lstable_double_zones;
  std::vector<LSTableIntColCompare> lstable_col_compares;
  std::vector<LSTableExtraRf> lstable_extra_rfs;
  std::vector<uint8_t> simple_covered_by_dict;
  std::vector<uint8_t> prefix_covered_by_dict;
  std::vector<uint8_t> prebuilt_membership_covered;
  bool disjunctive_covered_by_dict = false;

  const auto to_full_col = [&](slot_t proj_slot) -> slot_t {
    return projection != nullptr ? (*projection)[proj_slot] : proj_slot;
  };
  const auto eval_string_membership =
      [](std::string_view sv, const PrebuiltMembershipPredicate& pm) -> bool {
    const MembershipStringSet& strings = *pm.string_values;
    bool found = false;
    if (PlainStringComparable(sv)) {
      found = strings.contains(sv);
    } else {
      const Value probe{std::string(sv)};
      for (const std::string& member : strings) {
        StatusOr<Value> equal = TryBinary(BinaryOperation::kEquals, probe,
                                          Value(std::string(member)));
        if (equal.HasValue() && equal.Value().Truthy()) {
          found = true;
          break;
        }
      }
    }
    return found ? !pm.negated : (!pm.contains_null && pm.negated);
  };
  const auto eval_pm_null_pass =
      [](const PrebuiltMembershipPredicate& pm) noexcept -> bool {
    if (pm.string_values != nullptr) {
      return pm.string_values->empty() && !pm.contains_null && pm.negated;
    }
    if (pm.values != nullptr && pm.values->Empty() && !pm.contains_null) {
      return pm.negated;
    }
    return pm.existence && pm.negated;
  };
  const auto can_eval_pm_on_dict = [&](const PrebuiltMembershipPredicate& pm,
                                       const LSTableColumnDictionary& dict,
                                       const Column& col) noexcept -> bool {
    if (pm.runtime_filter != nullptr || pm.distinct_witness_minmax != nullptr ||
        pm.packed_pair_col2.has_value() || col.IsUnsigned() ||
        col.IsInterval()) {
      return false;
    }
    if (pm.string_values != nullptr) {
      return dict.value_type == ValueType::kVarChar;
    }
    if (pm.values == nullptr) {
      return false;
    }
    return (pm.domain_type == ValueType::kInt64 ||
            pm.domain_type == ValueType::kDate) &&
           dict.value_type == pm.domain_type;
  };
  const auto eval_pm_on_dict_code = [&](const PrebuiltMembershipPredicate& pm,
                                        const LSTableColumnDictionary& dict,
                                        size_t code) -> bool {
    if (pm.string_values != nullptr) {
      return eval_string_membership(dict.entries[code], pm);
    }
    const FlatIntSet& values = *pm.values;
    if (values.Empty() && !pm.contains_null) {
      return pm.negated;
    }
    const bool found = values.Contains(dict.int_entries[code]);
    return found ? !pm.negated : (!pm.contains_null && pm.negated);
  };
  const bool can_predecide_lstable =
      lstable_store != nullptr && filter_during_scan &&
      scan_filter != nullptr && !scan_filter->compile_decided_reject &&
      !scan_filter->needs_unsigned_tagging;
  // NULL tests depend only on the validated snapshot's null bitmap. Decide
  // them before allocating projected rows, including NOT (col IS NULL).
  const auto null_tests = can_predecide_lstable
                              ? BuildNullPeeks(*scan_filter, result_schema,
                                               projection, table.GetSchema())
                              : std::vector<NullPeekCompare>{};
  const bool residual_decided_by_null_tests =
      can_predecide_lstable &&
      null_tests.size() == scan_filter->residual.size();
  if (can_predecide_lstable) {
    simple_covered_by_dict.assign(scan_filter->simple.size(), 0);
    prefix_covered_by_dict.assign(scan_filter->prefix_in.size(), 0);
    prebuilt_membership_covered.assign(scan_filter->prebuilt_membership.size(),
                                       0);
    std::vector<int> col_to_df(table.GetSchema().ColumnCount(), -1);
    const auto dict_size_of =
        [](const LSTableColumnDictionary& dict) noexcept -> size_t {
      if (dict.value_type == ValueType::kInt64 ||
          dict.value_type == ValueType::kDate) {
        return dict.int_entries.size();
      }
      if (dict.value_type == ValueType::kDouble) {
        return dict.double_entries.size();
      }
      return dict.entries.size();
    };
    auto get_or_init_df = [&](slot_t full_col) -> LSTableDictColFilter& {
      if (col_to_df[full_col] < 0) {
        col_to_df[full_col] = static_cast<int>(lstable_dict_filters.size());
        LSTableDictColFilter df;
        df.column = full_col;
        df.allow_null = true;
        const auto& dict = lstable_store->dictionaries[full_col];
        const size_t ncodes = dict_size_of(dict);
        for (size_t code = 0; code < ncodes; ++code) {
          df.pass[code] = 1;
          df.pass_mask[code >> 6] |= uint64_t{1} << (code & 63);
        }
        lstable_dict_filters.push_back(df);
      }
      return lstable_dict_filters[static_cast<size_t>(col_to_df[full_col])];
    };
    const auto eval_pred_on_dict_code = [&](const SimpleComparePredicate& pred,
                                            const LSTableColumnDictionary& dict,
                                            size_t code,
                                            bool* supported) noexcept -> bool {
      *supported = false;
      if (pred.column_payload || pred.constant.IsNull()) {
        return false;
      }
      if (dict.value_type == ValueType::kVarChar &&
          pred.constant.type == ValueType::kVarChar &&
          !pred.constant.IsCaseInsensitive() &&
          (pred.op == BinaryOperation::kLike ||
           pred.op == BinaryOperation::kNotLike || pred.plain_string_payload)) {
        *supported = true;
        // `code` 0 doubles as the "is this predicate dictionary-decidable"
        // probe, so an empty dictionary (every row NULL, or a store built
        // under a tiny columnar budget) must not be indexed.
        if (code >= dict.entries.size()) {
          return false;
        }
        const std::string_view text = dict.entries[code];
        const std::string_view pat = pred.constant.value.varchar_value;
        if (pred.op == BinaryOperation::kLike ||
            pred.op == BinaryOperation::kNotLike) {
          const bool matched = FastLikeMatch(text, pat);
          return pred.op == BinaryOperation::kLike ? matched : !matched;
        }
        return ComparePositional(pred.op, text, pat);
      }
      if (dict.value_type == ValueType::kInt64 && pred.int_payload &&
          !pred.constant.IsUnsigned()) {
        *supported = true;
        if (code >= dict.int_entries.size()) {
          return false;
        }
        return ComparePositional(pred.op, dict.int_entries[code],
                                 pred.int_constant);
      }
      if (dict.value_type == ValueType::kDouble && pred.double_payload) {
        *supported = true;
        if (code >= dict.double_entries.size()) {
          return false;
        }
        return ComparePositional(pred.op, dict.double_entries[code],
                                 pred.double_constant);
      }
      return false;
    };
    for (size_t si = 0; si < scan_filter->simple.size(); ++si) {
      const SimpleComparePredicate& pred = scan_filter->simple[si];
      if (pred.column_payload) {
        const slot_t lfull = to_full_col(pred.column);
        const slot_t rfull = to_full_col(pred.right_column);
        if (lfull < table.GetSchema().ColumnCount() &&
            rfull < table.GetSchema().ColumnCount()) {
          const Column& lc = table.GetSchema().GetColumn(lfull);
          const Column& rc = table.GetSchema().GetColumn(rfull);
          if ((lc.Type() == ValueType::kInt64 ||
               lc.Type() == ValueType::kDate) &&
              (rc.Type() == ValueType::kInt64 ||
               rc.Type() == ValueType::kDate) &&
              !lc.IsUnsigned() && !rc.IsUnsigned()) {
            lstable_col_compares.push_back(LSTableIntColCompare{
                .left_col = lfull, .right_col = rfull, .op = pred.op});
            simple_covered_by_dict[si] = 1;
          }
        }
        continue;
      }
      if (pred.constant.IsNull()) {
        continue;
      }
      const slot_t full_col = to_full_col(pred.column);
      if (full_col >= table.GetSchema().ColumnCount()) {
        continue;
      }
      if (!pred.double_payload &&
          !table.GetSchema().GetColumn(full_col).IsUnsigned() &&
          (pred.constant.type == ValueType::kInt64 ||
           pred.constant.type == ValueType::kDate ||
           (pred.date_string_payload &&
            table.GetSchema().GetColumn(full_col).Type() ==
                ValueType::kDate))) {
        for (const IntegerPeekCompare& pk : integer_peeks) {
          if (pk.column == full_col && pk.op == pred.op &&
              pk.constant == pred.int_constant) {
            simple_covered_by_dict[si] = 1;
            break;
          }
        }
      }
      // Integer peeks are exact for this predicate and already run on raw
      // cells (including NULL rejection). Do not also decode packed dictionary
      // codes and test the same comparison for every surviving row.
      if (simple_covered_by_dict[si] != 0) {
        continue;
      }
      if (full_col < lstable_store->dictionaries.size() &&
          lstable_store->dictionaries[full_col].wide_code_bytes != 0) {
        const auto& dictionary = lstable_store->dictionaries[full_col];
        bool supported = false;
        (void)eval_pred_on_dict_code(pred, dictionary, 0, &supported);
        if (supported) {
          LSTableWideDictFilter filter;
          filter.column = full_col;
          filter.pass.resize(dict_size_of(dictionary));
          for (size_t code = 0; code < filter.pass.size(); ++code) {
            filter.pass[code] = static_cast<uint8_t>(
                eval_pred_on_dict_code(pred, dictionary, code, &supported));
          }
          lstable_wide_filters.push_back(std::move(filter));
          simple_covered_by_dict[si] = 1;
          continue;
        }
      }
      if (pred.double_payload &&
          table.GetSchema().GetColumn(full_col).Type() == ValueType::kDouble) {
        lstable_double_zones.push_back(
            LSTableDoubleZonePredicate{.column = full_col,
                                       .op = pred.op,
                                       .constant = pred.double_constant});
      }
      if (full_col < lstable_store->dictionaries.size() &&
          lstable_store->dictionaries[full_col].active &&
          !table.GetSchema().GetColumn(full_col).IsUnsigned()) {
        const auto& dict = lstable_store->dictionaries[full_col];
        bool supported = false;
        (void)eval_pred_on_dict_code(pred, dict, 0, &supported);
        if (supported) {
          LSTableDictColFilter& df = get_or_init_df(full_col);
          df.allow_null = false;
          df.pass_mask = {0ULL, 0ULL, 0ULL, 0ULL};
          const size_t ncodes = dict_size_of(dict);
          for (size_t code = 0; code < ncodes; ++code) {
            if (df.pass[code] == 0) {
              continue;
            }
            bool code_sup = false;
            if (!eval_pred_on_dict_code(pred, dict, code, &code_sup)) {
              df.pass[code] = 0;
            } else {
              df.pass_mask[code >> 6] |= uint64_t{1} << (code & 63);
            }
          }
          simple_covered_by_dict[si] = 1;
        }
      }
    }
    {
      for (size_t pi = 0; pi < scan_filter->prefix_in.size(); ++pi) {
        const PrefixInPredicate& pred = scan_filter->prefix_in[pi];
        const slot_t full_col = to_full_col(pred.column);
        if (full_col < lstable_store->dictionaries.size() &&
            lstable_store->dictionaries[full_col].active &&
            lstable_store->dictionaries[full_col].value_type ==
                ValueType::kVarChar) {
          LSTableDictColFilter& df = get_or_init_df(full_col);
          df.allow_null = false;
          const auto& dict = lstable_store->dictionaries[full_col];
          df.pass_mask = {0ULL, 0ULL, 0ULL, 0ULL};
          for (size_t code = 0; code < dict.entries.size(); ++code) {
            if (df.pass[code] == 0) {
              continue;
            }
            const std::string_view text = dict.entries[code];
            const bool ok = std::ranges::any_of(
                pred.prefixes,
                [text](const std::string& p) { return text.starts_with(p); });
            if (!ok) {
              df.pass[code] = 0;
            } else {
              df.pass_mask[code >> 6] |= uint64_t{1} << (code & 63);
            }
          }
          prefix_covered_by_dict[pi] = 1;
        }
      }
      if (!scan_filter->disjunctive_branches.empty()) {
        std::optional<slot_t> common_col;
        bool single_dict_col_or = true;
        for (const auto& br : scan_filter->disjunctive_branches) {
          if (!br.prefix_in.empty() || !br.prebuilt_membership.empty() ||
              !br.residual.empty() || br.simple.empty()) {
            single_dict_col_or = false;
            break;
          }
          for (const SimpleComparePredicate& sp : br.simple) {
            const slot_t fc = to_full_col(sp.column);
            if (!common_col.has_value()) {
              common_col = fc;
            } else if (*common_col != fc) {
              single_dict_col_or = false;
              break;
            }
            if (fc >= lstable_store->dictionaries.size() ||
                !lstable_store->dictionaries[fc].active ||
                table.GetSchema().GetColumn(fc).IsUnsigned()) {
              single_dict_col_or = false;
              break;
            }
            bool sup = false;
            (void)eval_pred_on_dict_code(sp, lstable_store->dictionaries[fc], 0,
                                         &sup);
            if (!sup) {
              single_dict_col_or = false;
              break;
            }
          }
          if (!single_dict_col_or) {
            break;
          }
        }
        if (single_dict_col_or && common_col.has_value()) {
          const slot_t fc = *common_col;
          LSTableDictColFilter& df = get_or_init_df(fc);
          df.allow_null = false;
          const auto& dict = lstable_store->dictionaries[fc];
          const size_t ncodes = dict_size_of(dict);
          df.pass_mask = {0ULL, 0ULL, 0ULL, 0ULL};
          for (size_t code = 0; code < ncodes; ++code) {
            if (df.pass[code] == 0) {
              continue;
            }
            bool any_br_ok = false;
            for (const auto& br : scan_filter->disjunctive_branches) {
              bool br_ok = true;
              for (const SimpleComparePredicate& sp : br.simple) {
                bool sup = false;
                if (!eval_pred_on_dict_code(sp, dict, code, &sup)) {
                  br_ok = false;
                  break;
                }
              }
              if (br_ok) {
                any_br_ok = true;
                break;
              }
            }
            if (!any_br_ok) {
              df.pass[code] = 0;
            } else {
              df.pass_mask[code >> 6] |= uint64_t{1} << (code & 63);
            }
          }
          disjunctive_covered_by_dict = true;
        } else {
          auto get_branch_narrow = [&](DisjBranchFilter& db,
                                       slot_t fc) -> LSTableDictColFilter& {
            for (LSTableDictColFilter& df : db.narrow_cols) {
              if (df.column == fc) {
                return df;
              }
            }
            LSTableDictColFilter df;
            df.column = fc;
            df.allow_null = true;
            const size_t ncodes = dict_size_of(lstable_store->dictionaries[fc]);
            for (size_t code = 0; code < ncodes; ++code) {
              df.pass[code] = 1;
              df.pass_mask[code >> 6] |= uint64_t{1} << (code & 63);
            }
            db.narrow_cols.push_back(df);
            return db.narrow_cols.back();
          };
          auto get_branch_wide = [&](DisjBranchFilter& db,
                                     slot_t fc) -> LSTableWideDictFilter& {
            for (LSTableWideDictFilter& wf : db.wide_cols) {
              if (wf.column == fc) {
                return wf;
              }
            }
            LSTableWideDictFilter wf;
            wf.column = fc;
            wf.allow_null = true;
            wf.pass.assign(dict_size_of(lstable_store->dictionaries[fc]), 1);
            db.wide_cols.push_back(std::move(wf));
            return db.wide_cols.back();
          };
          std::vector<DisjBranchFilter> built_branches;
          built_branches.reserve(scan_filter->disjunctive_branches.size());
          bool all_branches_dict_covered = true;
          for (const auto& br : scan_filter->disjunctive_branches) {
            if (!br.residual.empty() ||
                (br.simple.empty() && br.prefix_in.empty() &&
                 br.prebuilt_membership.empty())) {
              all_branches_dict_covered = false;
              break;
            }
            DisjBranchFilter db;
            auto apply_to_col = [&](slot_t fc, bool allow_null,
                                    const auto& pred_fn,
                                    bool supported) -> bool {
              if (!supported || fc >= table.GetSchema().ColumnCount() ||
                  table.GetSchema().GetColumn(fc).IsUnsigned() ||
                  fc >= lstable_store->dictionaries.size()) {
                return false;
              }
              const auto& dict = lstable_store->dictionaries[fc];
              if (dict.active) {
                LSTableDictColFilter& df = get_branch_narrow(db, fc);
                df.allow_null = df.allow_null && allow_null;
                df.pass_mask = {0ULL, 0ULL, 0ULL, 0ULL};
                const size_t ncodes = dict_size_of(dict);
                for (size_t code = 0; code < ncodes; ++code) {
                  if (df.pass[code] == 0) {
                    continue;
                  }
                  if (!pred_fn(dict, code)) {
                    df.pass[code] = 0;
                  } else {
                    df.pass_mask[code >> 6] |= uint64_t{1} << (code & 63);
                  }
                }
                return true;
              }
              if (dict.wide_code_bytes != 0) {
                LSTableWideDictFilter& wf = get_branch_wide(db, fc);
                wf.allow_null = wf.allow_null && allow_null;
                const size_t ncodes = wf.pass.size();
                for (size_t code = 0; code < ncodes; ++code) {
                  if (wf.pass[code] != 0 && !pred_fn(dict, code)) {
                    wf.pass[code] = 0;
                  }
                }
                return true;
              }
              return false;
            };
            bool br_ok = true;
            for (const SimpleComparePredicate& sp : br.simple) {
              const slot_t fc = to_full_col(sp.column);
              bool sup = false;
              if (fc < lstable_store->dictionaries.size()) {
                (void)eval_pred_on_dict_code(
                    sp, lstable_store->dictionaries[fc], 0, &sup);
              }
              if (!apply_to_col(
                      fc, false,
                      [&](const LSTableColumnDictionary& dict, size_t code) {
                        bool code_sup = false;
                        return eval_pred_on_dict_code(sp, dict, code,
                                                      &code_sup);
                      },
                      sup)) {
                br_ok = false;
                break;
              }
            }
            if (br_ok) {
              for (const PrefixInPredicate& pp : br.prefix_in) {
                const slot_t fc = to_full_col(pp.column);
                const bool sup = fc < lstable_store->dictionaries.size() &&
                                 lstable_store->dictionaries[fc].value_type ==
                                     ValueType::kVarChar;
                if (!apply_to_col(
                        fc, false,
                        [&](const LSTableColumnDictionary& dict, size_t code) {
                          const std::string_view text = dict.entries[code];
                          return std::ranges::any_of(
                              pp.prefixes, [text](const std::string& p) {
                                return text.starts_with(p);
                              });
                        },
                        sup)) {
                  br_ok = false;
                  break;
                }
              }
            }
            if (br_ok) {
              for (const PrebuiltMembershipPredicate& pm :
                   br.prebuilt_membership) {
                const slot_t fc = to_full_col(pm.column);
                const bool sup =
                    fc < lstable_store->dictionaries.size() &&
                    fc < table.GetSchema().ColumnCount() &&
                    can_eval_pm_on_dict(pm, lstable_store->dictionaries[fc],
                                        table.GetSchema().GetColumn(fc));
                if (!apply_to_col(
                        fc, eval_pm_null_pass(pm),
                        [&](const LSTableColumnDictionary& dict, size_t code) {
                          return eval_pm_on_dict_code(pm, dict, code);
                        },
                        sup)) {
                  br_ok = false;
                  break;
                }
              }
            }
            if (!br_ok) {
              all_branches_dict_covered = false;
              break;
            }
            built_branches.push_back(std::move(db));
          }
          if (all_branches_dict_covered && !built_branches.empty()) {
            lstable_disj_branches = std::move(built_branches);
            disjunctive_covered_by_dict = true;
          }
        }
      }
      for (size_t mi = 0; mi < scan_filter->prebuilt_membership.size(); ++mi) {
        const PrebuiltMembershipPredicate& pred =
            scan_filter->prebuilt_membership[mi];
        const slot_t full_col = to_full_col(pred.column);
        if (pred.runtime_filter != nullptr && !pred.negated) {
          if (full_col < table.GetSchema().ColumnCount()) {
            const Column& col = table.GetSchema().GetColumn(full_col);
            if (col.Type() == pred.domain_type &&
                (col.Type() == ValueType::kInt64 ||
                 col.Type() == ValueType::kDate) &&
                !col.IsUnsigned()) {
              lstable_extra_rfs.push_back(LSTableExtraRf{
                  .column = full_col, .group = pred.runtime_filter.get()});
              prebuilt_membership_covered[mi] = 1;
            }
          }
          continue;
        }
        if (full_col < table.GetSchema().ColumnCount() &&
            full_col < lstable_store->dictionaries.size()) {
          const Column& col = table.GetSchema().GetColumn(full_col);
          const auto& dict = lstable_store->dictionaries[full_col];
          if (can_eval_pm_on_dict(pred, dict, col)) {
            if (dict.active) {
              LSTableDictColFilter& df = get_or_init_df(full_col);
              df.allow_null = df.allow_null && eval_pm_null_pass(pred);
              df.pass_mask = {0ULL, 0ULL, 0ULL, 0ULL};
              const size_t ncodes = dict_size_of(dict);
              for (size_t code = 0; code < ncodes; ++code) {
                if (df.pass[code] == 0) {
                  continue;
                }
                if (!eval_pm_on_dict_code(pred, dict, code)) {
                  df.pass[code] = 0;
                } else {
                  df.pass_mask[code >> 6] |= uint64_t{1} << (code & 63);
                }
              }
              prebuilt_membership_covered[mi] = 1;
            } else if (dict.wide_code_bytes != 0) {
              LSTableWideDictFilter filter;
              filter.column = full_col;
              filter.allow_null = eval_pm_null_pass(pred);
              filter.pass.resize(dict_size_of(dict));
              for (size_t code = 0; code < filter.pass.size(); ++code) {
                filter.pass[code] = static_cast<uint8_t>(
                    eval_pm_on_dict_code(pred, dict, code));
              }
              lstable_wide_filters.push_back(std::move(filter));
              prebuilt_membership_covered[mi] = 1;
            }
          }
        }
      }
    }
  }
  const auto eval_simple_on_lstable =
      [&](const LSTablePageBlock& blk, uint32_t r,
          const SimpleComparePredicate& pred, bool* checked) -> bool {
    *checked = true;
    const slot_t full_col = to_full_col(pred.column);
    if (full_col >= blk.columns.size()) {
      *checked = false;
      return true;
    }
    const LSTableColumnBlock& col = blk.columns[full_col];
    if (col.IsNull(r)) {
      return false;
    }
    if (pred.column_payload) {
      const slot_t rfull_col = to_full_col(pred.right_column);
      if (rfull_col >= blk.columns.size()) {
        *checked = false;
        return true;
      }
      const LSTableColumnBlock& rcol = blk.columns[rfull_col];
      if (rcol.IsNull(r)) {
        return false;
      }
      if ((col.type == ValueType::kInt64 || col.type == ValueType::kDate) &&
          (rcol.type == ValueType::kInt64 || rcol.type == ValueType::kDate) &&
          !col.is_unsigned && !rcol.is_unsigned) {
        return ComparePositional(pred.op, col.int_cells[r], rcol.int_cells[r]);
      }
      if (col.type == ValueType::kDouble && rcol.type == ValueType::kDouble) {
        return ComparePositional(pred.op,
                                 std::bit_cast<double>(col.int_cells[r]),
                                 std::bit_cast<double>(rcol.int_cells[r]));
      }
      if (col.type == ValueType::kVarChar && rcol.type == ValueType::kVarChar) {
        return ComparePositional(pred.op, col.VarcharAt(r), rcol.VarcharAt(r));
      }
      *checked = false;
      return true;
    }
    if (pred.constant.IsNull()) {
      return false;
    }
    if (pred.date_string_payload && col.type == ValueType::kDate &&
        !col.is_unsigned) {
      return ComparePositional(pred.op, col.int_cells[r], pred.int_constant);
    }
    if (pred.int_payload &&
        (col.type == ValueType::kInt64 || col.type == ValueType::kDate) &&
        !col.is_unsigned && !pred.constant.IsUnsigned()) {
      return ComparePositional(pred.op, col.int_cells[r], pred.int_constant);
    }
    if (pred.double_payload && col.type == ValueType::kDouble) {
      return ComparePositional(pred.op, std::bit_cast<double>(col.int_cells[r]),
                               pred.double_constant);
    }
    if (col.type == ValueType::kVarChar &&
        pred.constant.type == ValueType::kVarChar &&
        !pred.constant.IsCaseInsensitive()) {
      const std::string_view text = col.VarcharAt(r);
      const std::string_view pat = pred.constant.value.varchar_value;
      if (pred.op == BinaryOperation::kLike ||
          pred.op == BinaryOperation::kNotLike) {
        const bool matched = FastLikeMatch(text, pat);
        return pred.op == BinaryOperation::kLike ? matched : !matched;
      }
      if (pred.plain_string_payload) {
        return ComparePositional(pred.op, text, pat);
      }
    }
    *checked = false;
    return true;
  };
  const auto eval_prebuilt_membership_on_lstable =
      [&](const LSTablePageBlock& blk, uint32_t r,
          const PrebuiltMembershipPredicate& pred) -> bool {
    if (pred.packed_pair_col2.has_value()) {
      if (pred.values == nullptr ||
          (pred.values->Empty() && !pred.contains_null)) {
        return pred.negated;
      }
      const slot_t fc0 = to_full_col(pred.column);
      const slot_t fc1 = to_full_col(*pred.packed_pair_col2);
      if (fc0 >= blk.columns.size() || fc1 >= blk.columns.size()) {
        return pred.negated;
      }
      const LSTableColumnBlock& c0 = blk.columns[fc0];
      const LSTableColumnBlock& c1 = blk.columns[fc1];
      if (c0.IsNull(r) || c1.IsNull(r) || c0.type != pred.domain_type ||
          c1.type != pred.packed_pair_type2 || c0.is_unsigned ||
          c1.is_unsigned) {
        return pred.negated;
      }
      const int64_t k0 = c0.int_cells[r];
      const int64_t k1 = c1.int_cells[r];
      if (!std::in_range<int32_t>(k0) || !std::in_range<int32_t>(k1)) {
        return pred.negated;
      }
      const uint64_t packed =
          (static_cast<uint64_t>(static_cast<uint32_t>(static_cast<int32_t>(k0)))
           << 32) |
          static_cast<uint32_t>(static_cast<int32_t>(k1));
      const bool found = pred.values->Contains(std::bit_cast<int64_t>(packed));
      return pred.negated ? !found : found;
    }
    if (pred.distinct_witness_minmax != nullptr) {
      const slot_t fk = to_full_col(pred.column);
      const slot_t fw = to_full_col(pred.witness_column);
      if (fk >= blk.columns.size() || fw >= blk.columns.size()) {
        return pred.negated;
      }
      const LSTableColumnBlock& ck = blk.columns[fk];
      const LSTableColumnBlock& cw = blk.columns[fw];
      if (ck.IsNull(r) || cw.IsNull(r) || ck.type != pred.domain_type ||
          cw.type != pred.witness_type || ck.is_unsigned || cw.is_unsigned) {
        return pred.negated;
      }
      const auto* mm = pred.distinct_witness_minmax->Find(ck.int_cells[r]);
      const int64_t ow = cw.int_cells[r];
      const bool found =
          (mm != nullptr) && (mm->first != ow || mm->second != ow);
      return pred.negated ? !found : found;
    }
    if (pred.runtime_filter != nullptr) {
      const slot_t full_col = to_full_col(pred.column);
      if (full_col >= blk.columns.size()) {
        return false;
      }
      const LSTableColumnBlock& col = blk.columns[full_col];
      return !col.IsNull(r) && col.type == pred.domain_type &&
             !pred.runtime_filter->Reject(col.int_cells[r]);
    }
    if (pred.string_values != nullptr) {
      if (pred.string_values->empty() && !pred.contains_null) {
        return pred.negated;
      }
      const slot_t full_col = to_full_col(pred.column);
      if (full_col >= blk.columns.size()) {
        return false;
      }
      const LSTableColumnBlock& col = blk.columns[full_col];
      if (col.IsNull(r) || col.type != ValueType::kVarChar) {
        return false;
      }
      return eval_string_membership(col.VarcharAt(r), pred);
    }
    const FlatIntSet& values = *pred.values;
    if (values.Empty() && !pred.contains_null) {
      return pred.negated;
    }
    const slot_t full_col = to_full_col(pred.column);
    if (full_col >= blk.columns.size()) {
      return false;
    }
    const LSTableColumnBlock& col = blk.columns[full_col];
    if (col.IsNull(r)) {
      return pred.existence && pred.negated;
    }
    if (col.type != pred.domain_type) {
      return false;
    }
    const bool found = values.Contains(col.int_cells[r]);
    return found ? !pred.negated : (!pred.contains_null && pred.negated);
  };
  const auto match_lstable_pre_materialize = [&](const LSTablePageBlock& blk,
                                                 uint32_t r,
                                                 bool* fully_decided) -> bool {
    *fully_decided = false;
    if (!filter_during_scan || scan_filter == nullptr) {
      *fully_decided = true;
      return true;
    }
    if (scan_filter->compile_decided_reject) {
      *fully_decided = true;
      return false;
    }
    if (!can_predecide_lstable) {
      return true;
    }
    bool all_simple_checked = true;
    for (size_t si = 0; si < scan_filter->simple.size(); ++si) {
      if (si < simple_covered_by_dict.size() &&
          simple_covered_by_dict[si] != 0) {
        continue;
      }
      bool checked = false;
      if (!eval_simple_on_lstable(blk, r, scan_filter->simple[si], &checked)) {
        return false;
      }
      if (!checked) {
        all_simple_checked = false;
      }
    }
    bool all_disjunctive_checked = scan_filter->disjunctive_branches.empty() ||
                                   disjunctive_covered_by_dict;
    {
      for (size_t pi = 0; pi < scan_filter->prefix_in.size(); ++pi) {
        if (pi < prefix_covered_by_dict.size() &&
            prefix_covered_by_dict[pi] != 0) {
          continue;
        }
        const PrefixInPredicate& pred = scan_filter->prefix_in[pi];
        const slot_t full_col = to_full_col(pred.column);
        if (full_col >= blk.columns.size()) {
          return false;
        }
        const LSTableColumnBlock& col = blk.columns[full_col];
        if (col.IsNull(r) || col.type != ValueType::kVarChar) {
          return false;
        }
        const std::string_view text = col.VarcharAt(r);
        if (!std::ranges::any_of(pred.prefixes, [text](const std::string& p) {
              return text.starts_with(p);
            })) {
          return false;
        }
      }
      if (!scan_filter->disjunctive_branches.empty() &&
          !disjunctive_covered_by_dict) {
        bool any_branch_possible = false;
        bool every_branch_complete = true;
        for (const auto& br : scan_filter->disjunctive_branches) {
          if (!br.residual.empty()) {
            every_branch_complete = false;
          }
          bool branch_ok = true;
          for (const SimpleComparePredicate& sp : br.simple) {
            bool checked = false;
            if (!eval_simple_on_lstable(blk, r, sp, &checked)) {
              branch_ok = false;
              break;
            }
            if (!checked) {
              every_branch_complete = false;
            }
          }
          if (branch_ok) {
            for (const PrefixInPredicate& pp : br.prefix_in) {
              const slot_t fc = to_full_col(pp.column);
              if (fc >= blk.columns.size()) {
                every_branch_complete = false;
                continue;
              }
              const LSTableColumnBlock& pcol = blk.columns[fc];
              if (pcol.IsNull(r) || pcol.type != ValueType::kVarChar) {
                branch_ok = false;
                break;
              }
              const std::string_view text = pcol.VarcharAt(r);
              if (!std::ranges::any_of(pp.prefixes,
                                       [text](const std::string& p) {
                                         return text.starts_with(p);
                                       })) {
                branch_ok = false;
                break;
              }
            }
          }
          if (branch_ok) {
            for (const PrebuiltMembershipPredicate& pm :
                 br.prebuilt_membership) {
              if (!eval_prebuilt_membership_on_lstable(blk, r, pm)) {
                branch_ok = false;
                break;
              }
            }
          }
          if (branch_ok) {
            any_branch_possible = true;
            if (every_branch_complete && br.residual.empty()) {
              break;
            }
          }
        }
        if (!any_branch_possible) {
          return false;
        }
        all_disjunctive_checked = every_branch_complete;
      }
      for (size_t mi = 0; mi < scan_filter->prebuilt_membership.size(); ++mi) {
        if (mi < prebuilt_membership_covered.size() &&
            prebuilt_membership_covered[mi] != 0) {
          continue;
        }
        if (!eval_prebuilt_membership_on_lstable(
                blk, r, scan_filter->prebuilt_membership[mi])) {
          return false;
        }
      }
    }
    *fully_decided =
        all_simple_checked &&
        (!has_post_simple_filter ||
         (worker_programs.complete &&
          scan_filter->prebuilt_scalar_compare.empty() &&
          all_disjunctive_checked && residual_decided_by_null_tests));
    return true;
  };
  const bool lstable_fast_loop_decides_all = [&]() {
    if (lstable_store == nullptr) {
      return false;
    }
    if (!filter_during_scan || scan_filter == nullptr) {
      return true;
    }
    if (scan_filter->compile_decided_reject ||
        scan_filter->needs_unsigned_tagging) {
      return false;
    }
    for (size_t si = 0; si < scan_filter->simple.size(); ++si) {
      if (si >= simple_covered_by_dict.size() ||
          simple_covered_by_dict[si] == 0) {
        return false;
      }
    }
    for (size_t pi = 0; pi < scan_filter->prefix_in.size(); ++pi) {
      if (pi >= prefix_covered_by_dict.size() ||
          prefix_covered_by_dict[pi] == 0) {
        return false;
      }
    }
    if (!scan_filter->disjunctive_branches.empty() &&
        !disjunctive_covered_by_dict) {
      return false;
    }
    for (size_t mi = 0; mi < scan_filter->prebuilt_membership.size(); ++mi) {
      if (mi >= prebuilt_membership_covered.size() ||
          prebuilt_membership_covered[mi] == 0) {
        return false;
      }
    }
    if (!has_post_simple_filter) {
      return true;
    }
    return worker_programs.complete &&
           scan_filter->prebuilt_scalar_compare.empty() &&
           residual_decided_by_null_tests;
  }();
  std::optional<slot_t> lstable_key_column = full_key_column;
  // Reuse physical key runs for sparse probes of sorted immutable columns.
  // Only candidate positions are indexed; remaining predicates still run below.
  std::shared_ptr<const LSTableIntegerRuns> runtime_key_runs;
  if (lstable_store && (active_lstable_rf != nullptr) && lstable_key_column &&
      active_lstable_rf->has_dense_bits()) {
    const uint64_t span = static_cast<uint64_t>(active_lstable_rf->Hi()) -
                          static_cast<uint64_t>(active_lstable_rf->Lo());
    if (static_cast<double>(active_lstable_rf->DistinctKeys()) <
        (static_cast<double>(span) + 1.0) * 0.25) {
      runtime_key_runs = lstable_store->IntegerRuns(*lstable_key_column);
    }
  }
  // A join may publish an unordered key first and an ordered key as a
  // secondary predicate. Prefer the ordered key's physical runs, retaining
  // the former primary filter as an ordinary conjunct.
  if (lstable_store && !runtime_key_runs) {
    for (auto it = lstable_extra_rfs.begin(); it != lstable_extra_rfs.end();
         ++it) {
      const auto* group = it->group;
      if ((group == nullptr) || !group->has_dense_bits()) { continue;
}
      const uint64_t span = static_cast<uint64_t>(group->Hi()) -
                            static_cast<uint64_t>(group->Lo());
      if (static_cast<double>(group->DistinctKeys()) >=
          (static_cast<double>(span) + 1.0) * 0.25) {
        continue;
}
      auto runs = lstable_store->IntegerRuns(it->column);
      if (!runs) { continue;
}
      const LSTableExtraRf chosen = *it;
      if ((active_lstable_rf != nullptr) && lstable_key_column) {
        *it = LSTableExtraRf{.column=*lstable_key_column, .group=active_lstable_rf};
      } else {
        lstable_extra_rfs.erase(it);
      }
      active_lstable_rf = chosen.group;
      lstable_key_column = chosen.column;
      runtime_key_runs = std::move(runs);
      break;
    }
  }
  std::shared_ptr<const LSTableIntegerRunBounds> runtime_run_bounds;
  const ScanIntInterval* bounded_interval = nullptr;
  if (runtime_key_runs) {
    for (const auto& interval : scan_intervals) {
      runtime_run_bounds =
          lstable_store->IntegerRunBounds(*lstable_key_column, interval.column);
      if (std::getenv("TINYLAMB_FUSION_DEBUG") != nullptr) {
        // NOLINTNEXTLINE(cert-err33-c) error intentionally ignored
        std::fprintf(stderr,
                     "[scan-bounds] key=%u value=%u rows=%zu null=%d min=%lld "
                     "max=%lld available=%d columnar=%zu total=%zu rss=%llu\n",
                     *lstable_key_column, interval.column,
                     lstable_store->integer_columns[interval.column].size(),
                     lstable_store->column_has_null[interval.column],
                     static_cast<long long>(
                         lstable_store->column_min_int[interval.column]),
                     static_cast<long long>(
                         lstable_store->column_max_int[interval.column]),
                     static_cast<int>(runtime_run_bounds != nullptr),
                     ProcessMemoryBudget::Global().ColumnarUsed(),
                     ProcessMemoryBudget::Global().TotalUsed(),
                     static_cast<unsigned long long>(
                         ProcessMemoryBudget::Global().PeakRssBytes()));
      }
      if (runtime_run_bounds) {
        bounded_interval = &interval;
        break;
      }
    }
  }
  uint64_t estimated_scan_rows = lstable_store != nullptr
                                     ? lstable_store->total_rows
                                     : table.BackingColumnarRowCount();
  if (runtime_key_runs && runtime_key_runs->max_run_rows != 0) {
    // Bound the output by the actual bitmap population and the largest
    // physical run. Unlike a selectivity guess, this cannot underestimate
    // the rows passing the primary filter, even with skew or duplicate keys.
    uint64_t accepted_keys = 0;
    for (size_t w = 0; w < active_lstable_rf->dense_bits_word_count(); ++w) {
      accepted_keys += std::popcount(active_lstable_rf->dense_bits_data()[w]);
}
    const uint64_t max_run = runtime_key_runs->max_run_rows;
    if (accepted_keys <= estimated_scan_rows / max_run) {
      estimated_scan_rows = accepted_keys * max_run;
}
  }
  const size_t est_cols = pushed_projection != nullptr
                              ? pushed_projection->output_schema.ColumnCount()
                              : result_schema.ColumnCount();
  const size_t est_row_bytes = std::max<size_t>(64, est_cols * size_t{24});
  const size_t est_in_memory_bytes =
      estimated_scan_rows > (std::numeric_limits<size_t>::max() / est_row_bytes)
          ? std::numeric_limits<size_t>::max()
          : static_cast<size_t>(estimated_scan_rows) * est_row_bytes;
  const bool estimated_fits_budget =
      estimated_scan_rows == 0 ||
      (QueryMemoryBudget::Global().Remaining() / 2) >= est_in_memory_bytes;
  const bool has_ample_memory_budget =
      Relation::HasAmpleMemoryBudget() && estimated_fits_budget;
  const bool fast_in_memory_append =
      lstable_fast_loop_decides_all && has_ample_memory_budget;
  const bool filter_unsigned_or_incomplete =
      filter_during_scan && scan_filter != nullptr &&
      (scan_filter->needs_unsigned_tagging ||
       (!worker_programs.complete && has_post_simple_filter));
  const bool use_columnar_sink =
      columnar_output && lstable_store != nullptr && has_ample_memory_budget &&
      result->rows.empty() && !result->HasSpill() &&
      result->columnar == nullptr && !filter_unsigned_or_incomplete;
  if (std::getenv("TINYLAMB_FUSION_DEBUG") != nullptr) {
    // NOLINTNEXTLINE(cert-err33-c) error intentionally ignored
    std::fprintf(
        stderr,
        "[scan] rows=%llu estimate=%llu col_requested=%d sink=%d "
        "rf_col=%d rf_keys=%zu runs=%d remaining=%zu width=%zu bounds=%d\n",
        static_cast<unsigned long long>(
            lstable_store ? lstable_store->total_rows : 0),
        static_cast<unsigned long long>(estimated_scan_rows), static_cast<int>(columnar_output),
        static_cast<int>(use_columnar_sink),
        lstable_key_column ? static_cast<int>(*lstable_key_column) : -1,
        (active_lstable_rf != nullptr) ? active_lstable_rf->DistinctKeys() : 0,
        static_cast<int>(runtime_key_runs != nullptr), QueryMemoryBudget::Global().Remaining(),
        est_cols, static_cast<int>(runtime_run_bounds != nullptr));
  }
  std::optional<CompiledColumnarProjection> pushed_plan;
  std::vector<uint8_t> pushed_needed_by_bytecode;
  bool pushed_needs_bytecode_views = false;
  if (use_columnar_sink && pushed_projection != nullptr) {
    CompiledColumnarProjection compiled = CompiledColumnarProjection::Compile(
        pushed_projection->expressions, result_schema,
        pushed_projection->output_schema);
    if (compiled.all_fast) {
      pushed_needed_by_bytecode.assign(result_schema.ColumnCount(), 0);
      for (const auto& item : compiled.items) {
        if (item.kind == CompiledColumnarProjection::Kind::kBytecode &&
            item.bytecode.has_value()) {
          pushed_needs_bytecode_views = true;
          for (const BytecodeInstruction& inst :
               item.bytecode->Instructions()) {
            if (inst.opcode == BytecodeOp::kLoadColumn &&
                inst.operand < pushed_needed_by_bytecode.size()) {
              pushed_needed_by_bytecode[inst.operand] = 1;
            }
          }
        }
      }
      pushed_plan = std::move(compiled);
    }
  }
  const bool apply_pushed_proj = pushed_plan.has_value();
  const Schema& columnar_schema =
      apply_pushed_proj ? pushed_plan->output_schema : result_schema;
  std::vector<ColumnarTable> worker_columnar(use_columnar_sink ? workers : 0);
  std::atomic<bool> abort_columnar_scan{false};
  const auto materialize_lstable_row =
      [&](const LSTablePageBlock& blk,
          const LSTableColumnBlock* const* proj_cols,
          const int64_t* const* proj_int_ptrs, const ValueType* proj_types,
          bool blk_fast_scalar, size_t col_count, uint32_t r, Row* out_row) {
        if (col_count == 3) {
          out_row->values_.reserve(4);
        }
        out_row->values_.resize(col_count);
        if (blk_fast_scalar) [[likely]] {
          Value* const dst_base = out_row->values_.data();
          for (size_t k = 0; k < col_count; ++k) {
            Value& dst = dst_base[k];
            const ValueType vt = proj_types[k];
            dst.type = vt;
            const int64_t raw = proj_int_ptrs[k][r];
            if (vt == ValueType::kDouble) {
              dst.value.double_value = std::bit_cast<double>(raw);
            } else {
              dst.value.int_value = raw;
            }
          }
          return;
        }
        for (size_t k = 0; k < col_count; ++k) {
          const LSTableColumnBlock& col =
              proj_cols != nullptr
                  ? *proj_cols[k]
                  : blk.columns[projection != nullptr ? (*projection)[k]
                                                      : static_cast<slot_t>(k)];
          Value& dst = out_row->values_[k];
          if (col.has_null && col.IsNull(r)) [[unlikely]] {
            if (dst.type != ValueType::kNull) {
              dst = Value();
            }
            continue;
          }
          switch (col.type) {
            case ValueType::kInt64:
              dst.type = ValueType::kInt64;
              dst.value.int_value = col.int_cells[r];
              if (col.is_unsigned) [[unlikely]] {
                dst.SetUnsigned();
              }
              break;
            case ValueType::kDate:
              dst.type = ValueType::kDate;
              dst.value.int_value = col.int_cells[r];
              break;
            case ValueType::kDouble:
              dst.type = ValueType::kDouble;
              dst.value.double_value = std::bit_cast<double>(col.int_cells[r]);
              break;
            case ValueType::kVarChar: {
              const std::string_view sv = col.VarcharAt(r);
              dst.type = ValueType::kVarChar;
              dst.owned_data.assign(sv);
              dst.value.varchar_value = dst.owned_data;
              break;
            }
            default:
              if (dst.type != ValueType::kNull) {
                dst = Value();
              }
              break;
          }
        }
      };
  const auto scan_worker = [&](size_t w) {
    try {
      auto& local = shards[w];
      if (lstable_store != nullptr) {
        const auto* peek_ptr = integer_peeks.empty() ? nullptr : &integer_peeks;
        const size_t total_blocks = lstable_store->blocks.size();
        (void)lstable_blocks_per_morsel;
        const size_t blk_begin = (total_blocks * w) / workers;
        const size_t blk_end = (total_blocks * (w + 1)) / workers;
        size_t init_res = std::clamp<size_t>(
            lstable_store->total_rows / (workers * 8), 4096, 65536);
        if (!use_columnar_sink && fast_in_memory_append &&
            scan_filter == nullptr && tuple_filter == nullptr &&
            active_lstable_rf == nullptr && lstable_extra_rfs.empty()) {
          size_t exact_worker_rows = 0;
          for (size_t bi = blk_begin; bi < blk_end; ++bi) {
            exact_worker_rows += lstable_store->blocks[bi].row_count;
          }
          init_res = exact_worker_rows;
        }
        if (!use_columnar_sink) {
          local.rows.reserve(init_res);
        }
        ColumnarChunk cur_chunk;
        bool cur_chunk_init = false;
        std::vector<uint32_t> sel_buf;
        std::vector<uint32_t> rf_candidates;
        ColumnarScanScratch scan_scratch;
        struct PendingSelBlock {
          const LSTablePageBlock* blk{nullptr};
          uint32_t src_offset{0};
          uint32_t sel_begin{0};
          uint32_t sel_end{0};
          uint32_t count{0};
        };
        std::vector<PendingSelBlock> pending_blocks;
        std::vector<uint32_t> pending_sel;
        size_t pending_rows = 0;
        std::vector<uint8_t> pending_null_scratch;
        if (use_columnar_sink) {
          sel_buf.reserve(kColumnarChunkRows);
          if (!apply_pushed_proj) {
            pending_blocks.reserve(128);
            pending_sel.reserve(kColumnarChunkRows);
            pending_null_scratch.resize(kColumnarChunkRows);
          }
          if (apply_pushed_proj && pushed_needs_bytecode_views) {
            const size_t ncols = result_schema.ColumnCount();
            scan_scratch.int_cols.resize(ncols);
            scan_scratch.str_cols.resize(ncols);
            scan_scratch.blk_views.resize(ncols);
          }
        }
        const auto flush_pending_batch = [&]() {
          if (pending_rows == 0) {
            return;
          }
          const size_t ncols = columnar_schema.ColumnCount();
          ColumnarChunk out_chunk;
          out_chunk.InitFromSchema(columnar_schema, 0);
          out_chunk.rows = pending_rows;
          if (pending_blocks.size() == 1 &&
              pending_blocks[0].sel_begin == pending_blocks[0].sel_end) {
            const PendingSelBlock& ps = pending_blocks[0];
            for (size_t k = 0; k < ncols; ++k) {
              const slot_t col_idx = projection != nullptr
                                         ? (*projection)[k]
                                         : static_cast<slot_t>(k);
              AppendLSTableColumnSlice(ps.blk->columns[col_idx], lstable_store,
                                       ps.src_offset, nullptr, ps.count,
                                       &out_chunk.columns[k], 0);
            }
            worker_columnar[w].AppendChunk(std::move(out_chunk));
            pending_blocks.clear();
            pending_sel.clear();
            pending_rows = 0;
            return;
          }
          const uint32_t* __restrict sel_all = pending_sel.data();
          uint8_t* __restrict null_buf = pending_null_scratch.data();
          const auto gather_block_nulls =
              [&](const LSTableColumnBlock& bc, const PendingSelBlock& ps,
                  size_t out_off, uint8_t* seen_null) {
                const uint32_t cnt = ps.count;
                const uint8_t* sn =
                    !bc.nulls_view.empty()
                        ? bc.nulls_view.data()
                        : (bc.nulls.empty() ? nullptr : bc.nulls.data());
                if (!bc.has_null) {
                  std::memset(null_buf + out_off, 0, cnt);
                  return;
                }
                if (sn == nullptr) {
                  for (uint32_t i = 0; i < cnt; ++i) {
                    const uint32_t r = ps.sel_begin == ps.sel_end
                                           ? (ps.src_offset + i)
                                           : sel_all[ps.sel_begin + i];
                    const uint8_t nv = bc.IsNull(r) ? 1 : 0;
                    null_buf[out_off + i] = nv;
                    *seen_null |= nv;
                  }
                  return;
                }
                if (ps.sel_begin == ps.sel_end) {
                  std::memcpy(null_buf + out_off, sn + ps.src_offset, cnt);
                  for (uint32_t i = 0; i < cnt; ++i) {
                    *seen_null |= null_buf[out_off + i];
                  }
                  return;
                }
                const uint32_t* __restrict s = sel_all + ps.sel_begin;
                for (uint32_t i = 0; i < cnt; ++i) {
                  const uint8_t nv = sn[s[i]];
                  null_buf[out_off + i] = nv;
                  *seen_null |= nv;
                }
              };
          for (size_t k = 0; k < ncols; ++k) {
            const slot_t col_idx = projection != nullptr
                                       ? (*projection)[k]
                                       : static_cast<slot_t>(k);
            const Column& col_meta = columnar_schema.GetColumn(k);
            ColumnarColumn& dst = out_chunk.columns[k];
            const ValueType vt = col_meta.Type();
            const bool expect_unsigned = col_meta.IsUnsigned();
            bool all_same_int =
                !col_meta.IsInterval() &&
                (vt == ValueType::kInt64 || vt == ValueType::kDate);
            bool all_raw_int = all_same_int;
            bool all_same_double =
                !col_meta.IsInterval() && vt == ValueType::kDouble;
            bool any_blk_has_null = false;
            for (const PendingSelBlock& ps : pending_blocks) {
              const LSTableColumnBlock& bc = ps.blk->columns[col_idx];
              if (bc.has_null) {
                any_blk_has_null = true;
              }
              if (bc.type != vt || bc.is_unsigned != expect_unsigned) {
                all_same_int = false;
                all_raw_int = false;
                all_same_double = false;
                break;
              }
              if (all_raw_int && !bc.HasRawIntCells()) {
                all_raw_int = false;
              }
              if (all_same_double && bc.int_cells.empty()) {
                all_same_double = false;
              }
            }
            if (all_same_int) {
              dst.Init(vt, expect_unsigned, false, 0);
              dst.ints.resize(pending_rows);
              int64_t* __restrict di = dst.ints.data();
              size_t out_off = 0;
              uint8_t seen_null = 0;
              if (all_raw_int) {
                for (const PendingSelBlock& ps : pending_blocks) {
                  const LSTableColumnBlock& bc = ps.blk->columns[col_idx];
                  const int64_t* __restrict src = bc.int_cells.data();
                  const uint32_t cnt = ps.count;
                  if (ps.sel_begin == ps.sel_end) {
                    std::memcpy(di + out_off, src + ps.src_offset,
                                cnt * sizeof(int64_t));
                  } else {
                    const uint32_t* __restrict s = sel_all + ps.sel_begin;
                    for (uint32_t i = 0; i < cnt; ++i) {
                      di[out_off + i] = src[s[i]];
                    }
                  }
                  if (any_blk_has_null) {
                    gather_block_nulls(bc, ps, out_off, &seen_null);
                  }
                  out_off += cnt;
                }
              } else {
                for (const PendingSelBlock& ps : pending_blocks) {
                  const LSTableColumnBlock& bc = ps.blk->columns[col_idx];
                  const uint32_t cnt = ps.count;
                  if (ps.sel_begin == ps.sel_end) {
                    bc.MaterializeIntSlice(ps.src_offset, cnt, di + out_off);
                  } else if (bc.HasRawIntCells()) {
                    const int64_t* __restrict src = bc.int_cells.data();
                    const uint32_t* __restrict s = sel_all + ps.sel_begin;
                    for (uint32_t i = 0; i < cnt; ++i) {
                      di[out_off + i] = src[s[i]];
                    }
                  } else {
                    const uint32_t* __restrict s = sel_all + ps.sel_begin;
                    const auto base_u = static_cast<uint64_t>(bc.for_base);
                    int64_t* __restrict dst_p = di + out_off;
                    switch (bc.for_width) {
                      case 0:
                        for (uint32_t i = 0; i < cnt; ++i) {
                          dst_p[i] = bc.for_base;
                        }
                        break;
                      case 1: {
                        const uint8_t* __restrict fb = bc.for_bytes.data();
                        for (uint32_t i = 0; i < cnt; ++i) {
                          dst_p[i] = static_cast<int64_t>(base_u + fb[s[i]]);
                        }
                        break;
                      }
                      case 2: {
                        const uint8_t* __restrict fb = bc.for_bytes.data();
                        for (uint32_t i = 0; i < cnt; ++i) {
                          uint16_t v = 0;
                          std::memcpy(&v, fb + (static_cast<size_t>(s[i]) * 2),
                                      sizeof(v));
                          dst_p[i] = static_cast<int64_t>(base_u + v);
                        }
                        break;
                      }
                      case 4: {
                        const uint8_t* __restrict fb = bc.for_bytes.data();
                        for (uint32_t i = 0; i < cnt; ++i) {
                          uint32_t v = 0;
                          std::memcpy(&v, fb + (static_cast<size_t>(s[i]) * 4),
                                      sizeof(v));
                          dst_p[i] = static_cast<int64_t>(base_u + v);
                        }
                        break;
                      }
                      default:
                        std::memset(dst_p, 0, cnt * sizeof(int64_t));
                        break;
                    }
                  }
                  if (any_blk_has_null) {
                    gather_block_nulls(bc, ps, out_off, &seen_null);
                  }
                  out_off += cnt;
                }
              }
              if (seen_null != 0) {
                dst.has_null = true;
                dst.nulls.assign(null_buf, null_buf + pending_rows);
              }
              continue;
            }
            if (all_same_double) {
              dst.Init(ValueType::kDouble, false, false, 0);
              dst.doubles.resize(pending_rows);
              double* __restrict dd = dst.doubles.data();
              size_t out_off = 0;
              uint8_t seen_null = 0;
              for (const PendingSelBlock& ps : pending_blocks) {
                const LSTableColumnBlock& bc = ps.blk->columns[col_idx];
                const int64_t* __restrict src = bc.int_cells.data();
                const uint32_t cnt = ps.count;
                if (ps.sel_begin == ps.sel_end) {
                  std::memcpy(dd + out_off, src + ps.src_offset,
                              cnt * sizeof(double));
                } else {
                  const uint32_t* __restrict s = sel_all + ps.sel_begin;
                  for (uint32_t i = 0; i < cnt; ++i) {
                    dd[out_off + i] = std::bit_cast<double>(src[s[i]]);
                  }
                }
                if (any_blk_has_null) {
                  gather_block_nulls(bc, ps, out_off, &seen_null);
                }
                out_off += cnt;
              }
              if (seen_null != 0) {
                dst.has_null = true;
                dst.nulls.assign(null_buf, null_buf + pending_rows);
              }
              continue;
            }
            size_t out_off = 0;
            for (const PendingSelBlock& ps : pending_blocks) {
              const LSTableColumnBlock& bc = ps.blk->columns[col_idx];
              const uint32_t* slice_sel = ps.sel_begin == ps.sel_end
                                              ? nullptr
                                              : (sel_all + ps.sel_begin);
              AppendLSTableColumnSlice(bc, lstable_store, ps.src_offset,
                                       slice_sel, ps.count, &dst, out_off);
              out_off += ps.count;
            }
          }
          worker_columnar[w].AppendChunk(std::move(out_chunk));
          pending_blocks.clear();
          pending_sel.clear();
          pending_rows = 0;
        };
        const auto flush_block_selection =
            [&](const LSTablePageBlock& blk,
                const LSTableColumnBlock* const* proj_cols, const uint32_t* sel,
                size_t count) -> bool {
          if (count == 0) {
            return true;
          }
          const auto resolve_blk_col =
              [&](size_t k) -> const LSTableColumnBlock& {
            return proj_cols != nullptr
                       ? *proj_cols[k]
                       : blk.columns[projection != nullptr
                                         ? (*projection)[k]
                                         : static_cast<slot_t>(k)];
          };
          const auto record_worker_error = [&](Status st) {
            abort_columnar_scan.store(true, std::memory_order_relaxed);
            std::scoped_lock lock(error_mu);
            if (error != nullptr && *error == Status::kSuccess) {
              *error = std::move(st);
            } else if (error == nullptr && error_ex == Status::kSuccess) {
              error_ex = std::move(st);
            }
          };
          if (!apply_pushed_proj) {
            size_t offset = 0;
            while (offset < count) {
              if (pending_rows == 0 && sel == nullptr &&
                  count - offset >= kColumnarChunkRows) {
                const size_t ncols = columnar_schema.ColumnCount();
                ColumnarChunk full_chunk;
                full_chunk.InitFromSchema(columnar_schema, 0);
                full_chunk.rows = kColumnarChunkRows;
                for (size_t k = 0; k < ncols; ++k) {
                  AppendLSTableColumnSlice(resolve_blk_col(k), lstable_store,
                                           offset, nullptr, kColumnarChunkRows,
                                           &full_chunk.columns[k], 0);
                }
                worker_columnar[w].AppendChunk(std::move(full_chunk));
                offset += kColumnarChunkRows;
                continue;
              }
              const size_t space = kColumnarChunkRows - pending_rows;
              const size_t n = std::min(space, count - offset);
              if (sel == nullptr) {
                pending_blocks.push_back(
                    PendingSelBlock{.blk = &blk,
                                    .src_offset = static_cast<uint32_t>(offset),
                                    .sel_begin = 0,
                                    .sel_end = 0,
                                    .count = static_cast<uint32_t>(n)});
              } else {
                const auto sb = static_cast<uint32_t>(pending_sel.size());
                pending_sel.insert(pending_sel.end(), sel + offset,
                                   sel + offset + n);
                const auto se = static_cast<uint32_t>(pending_sel.size());
                pending_blocks.push_back(
                    PendingSelBlock{.blk = &blk,
                                    .src_offset = 0,
                                    .sel_begin = sb,
                                    .sel_end = se,
                                    .count = static_cast<uint32_t>(n)});
              }
              pending_rows += n;
              offset += n;
              if (pending_rows == kColumnarChunkRows) {
                flush_pending_batch();
              }
            }
            return true;
          }

          bool views_prepared = false;
          const auto prepare_blk_views = [&]() {
            if (views_prepared || !pushed_needs_bytecode_views) {
              return;
            }
            views_prepared = true;
            const size_t ncols = result_schema.ColumnCount();
            for (size_t k = 0; k < ncols; ++k) {
              if (pushed_needed_by_bytecode[k] == 0) {
                continue;
              }
              const LSTableColumnBlock& col = resolve_blk_col(k);
              BytecodeColumnView& v = scan_scratch.blk_views[k];
              v = BytecodeColumnView{};
              v.is_uniform = true;
              v.has_null = col.has_null;
              v.is_unsigned = col.is_unsigned;
              const uint8_t* nptr =
                  !col.nulls_view.empty()
                      ? col.nulls_view.data()
                      : (col.nulls.empty() ? nullptr : col.nulls.data());
              v.nulls = nptr;
              if (col.type == ValueType::kNull ||
                  (col.type == ValueType::kDouble && col.int_cells.empty() &&
                   col.has_null)) {
                v.type = ValueType::kNull;
                v.has_null = true;
                if (v.nulls == nullptr) {
                  if (scan_scratch.all_null_buf.size() < blk.row_count) {
                    scan_scratch.all_null_buf.assign(blk.row_count, 1);
                  }
                  v.nulls = scan_scratch.all_null_buf.data();
                }
                continue;
              }
              if (col.type == ValueType::kInt64 ||
                  col.type == ValueType::kDate) {
                v.type = col.type;
                if (col.HasRawIntCells() &&
                    col.int_cells.size() >= blk.row_count) {
                  v.ints = col.int_cells.data();
                } else {
                  auto& vec = scan_scratch.int_cols[k];
                  vec.resize(blk.row_count);
                  col.MaterializeIntSlice(0, blk.row_count, vec.data());
                  v.ints = vec.data();
                }
                continue;
              }
              if (col.type == ValueType::kDouble) {
                v.type = ValueType::kDouble;
                if (col.int_cells.size() >= blk.row_count) {
                  v.raw_double_bits = col.int_cells.data();
                } else {
                  v.is_uniform = false;
                }
                continue;
              }
              if (col.type == ValueType::kVarChar) {
                v.type = ValueType::kVarChar;
                auto& svec = scan_scratch.str_cols[k];
                if (svec.size() < blk.row_count) {
                  svec.resize(blk.row_count);
                }
                if (sel == nullptr) {
                  for (uint32_t r = 0; r < blk.row_count; ++r) {
                    if (!col.has_null || !col.IsNull(r)) {
                      svec[r] = col.VarcharAt(r);
                    }
                  }
                } else {
                  for (size_t i = 0; i < count; ++i) {
                    const uint32_t r = sel[i];
                    if (!col.has_null || !col.IsNull(r)) {
                      svec[r] = col.VarcharAt(r);
                    }
                  }
                }
                v.strings = svec.data();
                continue;
              }
              v.is_uniform = false;
            }
          };

          size_t offset = 0;
          while (offset < count) {
            if (!cur_chunk_init) {
              cur_chunk.InitFromSchema(columnar_schema, 0);
              cur_chunk.keepalives.push_back(lstable_store);
              cur_chunk_init = true;
            }
            const size_t space = kColumnarChunkRows - cur_chunk.rows;
            const size_t n = std::min(space, count - offset);
            const size_t prev_rows = cur_chunk.rows;
            const uint32_t* slice_sel =
                sel != nullptr ? (sel + offset) : nullptr;
            std::span<const BytecodeColumnView> views_span;
            if (pushed_needs_bytecode_views) {
              prepare_blk_views();
              if (slice_sel == nullptr && offset > 0) {
                scan_scratch.slice_views = scan_scratch.blk_views;
                for (size_t ci = 0; ci < scan_scratch.slice_views.size();
                     ++ci) {
                  if (pushed_needed_by_bytecode[ci] == 0) {
                    continue;
                  }
                  BytecodeColumnView& sv = scan_scratch.slice_views[ci];
                  if (sv.nulls != nullptr) {
                    sv.nulls += offset;
                  }
                  if (sv.ints != nullptr) {
                    sv.ints += offset;
                  }
                  if (sv.doubles != nullptr) {
                    sv.doubles += offset;
                  }
                  if (sv.raw_double_bits != nullptr) {
                    sv.raw_double_bits += offset;
                  }
                  if (sv.strings != nullptr) {
                    sv.strings += offset;
                  }
                }
                views_span = scan_scratch.slice_views;
              } else {
                views_span = scan_scratch.blk_views;
              }
            }
            for (size_t k = 0; k < pushed_plan->items.size(); ++k) {
              const auto& item = pushed_plan->items[k];
              ColumnarColumn& dst_col = cur_chunk.columns[k];
              switch (item.kind) {
                case CompiledColumnarProjection::Kind::kPassThrough:
                  AppendLSTableColumnSlice(resolve_blk_col(item.src_col),
                                           lstable_store,
                                           slice_sel == nullptr ? offset : 0,
                                           slice_sel, n, &dst_col, prev_rows);
                  break;
                case CompiledColumnarProjection::Kind::kConstant: {
                  ChunkStringArena* arena =
                      (!item.constant.IsNull() &&
                       item.constant.type == ValueType::kVarChar)
                          ? cur_chunk.EnsureArena()
                          : nullptr;
                  dst_col.AppendConstant(item.constant, n, arena, prev_rows);
                  break;
                }
                case CompiledColumnarProjection::Kind::kSubstring:
                  AppendLSTableSubstringSlice(
                      resolve_blk_col(item.src_col), lstable_store,
                      slice_sel == nullptr ? offset : 0, slice_sel, n,
                      item.substr_start, item.substr_length,
                      item.substr_has_length, &dst_col, prev_rows);
                  break;
                case CompiledColumnarProjection::Kind::kBytecode: {
                  BytecodeColumnSink sink = dst_col.AsBytecodeSink(prev_rows);
                  Status eval_err{Status::kSuccess};
                  const auto st = item.bytecode->TryEvaluateColumnarBatch(
                      views_span, slice_sel, n, &sink, &eval_err);
                  if (st == BytecodeProgram::FastEval::kDone) {
                    dst_col.SyncFromBytecodeSink(
                        sink,
                        sink.may_borrow_constant_strings
                            ? cur_chunk.EnsureArena()
                            : nullptr,
                        n);
                  } else if (st == BytecodeProgram::FastEval::kError) {
                    record_worker_error(std::move(eval_err));
                    return false;
                  } else {
                    const size_t in_cols = result_schema.ColumnCount();
                    scan_scratch.fallback_row.values_.resize(in_cols);
                    for (size_t i = 0; i < n; ++i) {
                      const uint32_t r =
                          slice_sel != nullptr
                              ? slice_sel[i]
                              : static_cast<uint32_t>(offset + i);
                      for (size_t c = 0; c < in_cols; ++c) {
                        if (pushed_needed_by_bytecode[c] != 0) {
                          scan_scratch.fallback_row.values_[c] =
                              ReadLSTableCellValue(resolve_blk_col(c), r);
                        }
                      }
                      StatusOr<Value> val_or = item.bytecode->TryEvaluateRow(
                          scan_scratch.fallback_row);
                      if (!val_or.HasValue()) {
                        record_worker_error(val_or.GetStatus());
                        return false;
                      }
                      const Value& evaluated = val_or.Value();
                      ChunkStringArena* arena =
                          (!evaluated.IsNull() &&
                           evaluated.type == ValueType::kVarChar)
                              ? cur_chunk.EnsureArena()
                              : nullptr;
                      dst_col.AppendValue(evaluated, arena, prev_rows + i);
                    }
                  }
                  break;
                }
                case CompiledColumnarProjection::Kind::kNullCoalesce:
                case CompiledColumnarProjection::Kind::kFallback:
                  record_worker_error(
                      StatusError(StatusCode::kRuntimeError,
                                  "unexpected fallback item in scan"));
                  return false;
              }
            }
            cur_chunk.rows += n;
            offset += n;
            if (cur_chunk.rows == kColumnarChunkRows) {
              worker_columnar[w].AppendChunk(std::move(cur_chunk));
              cur_chunk = ColumnarChunk{};
              cur_chunk_init = false;
            }
          }
          return true;
        };
        {
          std::vector<const DisjBranchFilter*> active_disj_branches;
          active_disj_branches.reserve(lstable_disj_branches.size());
          uint64_t next_block_row = 0;
          if (runtime_key_runs) {
            for (size_t bi = 0; bi < blk_begin; ++bi) {
              next_block_row += lstable_store->blocks[bi].row_count;
}
          }
          for (size_t bi = blk_begin; bi < blk_end; ++bi) {
            const uint64_t block_row = next_block_row;
            next_block_row += lstable_store->blocks[bi].row_count;
            if (abort_columnar_scan.load(std::memory_order_relaxed)) {
              break;
            }
            const LSTablePageBlock* blk = &lstable_store->blocks[bi];
            if (blk->row_count == 0) {
              continue;
            }
            if (!blk->MatchesZoneMaps(peek_ptr, active_lstable_rf,
                                      lstable_key_column)) {
              continue;
            }
            bool zone_rejected = false;
            bool dict_all_pass = lstable_wide_filters.empty();
            for (const LSTableDictColFilter& df : lstable_dict_filters) {
              const LSTableColumnBlock& dcol = blk->columns[df.column];
              const bool non_null_any =
                  !dcol.dict_codes.empty() &&
                  (((dcol.dict_mask[0] & df.pass_mask[0]) |
                    (dcol.dict_mask[1] & df.pass_mask[1]) |
                    (dcol.dict_mask[2] & df.pass_mask[2]) |
                    (dcol.dict_mask[3] & df.pass_mask[3])) != 0);
              if (!non_null_any && !(dcol.has_null && df.allow_null)) {
                zone_rejected = true;
                break;
              }
              if ((dcol.has_null && !df.allow_null) ||
                  dcol.dict_codes.empty() ||
                  ((dcol.dict_mask[0] & ~df.pass_mask[0]) |
                   (dcol.dict_mask[1] & ~df.pass_mask[1]) |
                   (dcol.dict_mask[2] & ~df.pass_mask[2]) |
                   (dcol.dict_mask[3] & ~df.pass_mask[3])) != 0) {
                dict_all_pass = false;
              }
            }
            if (!zone_rejected && !lstable_disj_branches.empty()) {
              active_disj_branches.clear();
              bool disj_all_pass = false;
              for (const DisjBranchFilter& db : lstable_disj_branches) {
                bool possible = true;
                bool br_all_pass = db.wide_cols.empty();
                for (const LSTableDictColFilter& df : db.narrow_cols) {
                  const LSTableColumnBlock& dcol = blk->columns[df.column];
                  const bool non_null_any =
                      !dcol.dict_codes.empty() &&
                      (((dcol.dict_mask[0] & df.pass_mask[0]) |
                        (dcol.dict_mask[1] & df.pass_mask[1]) |
                        (dcol.dict_mask[2] & df.pass_mask[2]) |
                        (dcol.dict_mask[3] & df.pass_mask[3])) != 0);
                  if (!non_null_any && !(dcol.has_null && df.allow_null)) {
                    possible = false;
                    break;
                  }
                  if ((dcol.has_null && !df.allow_null) ||
                      dcol.dict_codes.empty() ||
                      ((dcol.dict_mask[0] & ~df.pass_mask[0]) |
                       (dcol.dict_mask[1] & ~df.pass_mask[1]) |
                       (dcol.dict_mask[2] & ~df.pass_mask[2]) |
                       (dcol.dict_mask[3] & ~df.pass_mask[3])) != 0) {
                    br_all_pass = false;
                  }
                }
                if (!possible) {
                  continue;
                }
                if (br_all_pass) {
                  disj_all_pass = true;
                  active_disj_branches.clear();
                  break;
                }
                active_disj_branches.push_back(&db);
              }
              if (!disj_all_pass) {
                dict_all_pass = false;
                if (active_disj_branches.empty()) {
                  zone_rejected = true;
                }
              }
            }
            if (zone_rejected) {
              continue;
            }
            bool peek_all_pass = true;
            if (peek_ptr != nullptr) {
              for (const IntegerPeekCompare& pred : *peek_ptr) {
                const LSTableColumnBlock& pcol = blk->columns[pred.column];
                if (pcol.has_null || !pcol.has_min_max) {
                  peek_all_pass = false;
                  break;
                }
                bool all_ok = false;
                switch (pred.op) {
                  case BinaryOperation::kEquals:
                    all_ok = (pcol.min_int == pred.constant &&
                              pcol.max_int == pred.constant);
                    break;
                  case BinaryOperation::kNotEquals:
                    all_ok = (pred.constant < pcol.min_int ||
                              pred.constant > pcol.max_int);
                    break;
                  case BinaryOperation::kLessThan:
                    all_ok = (pcol.max_int < pred.constant);
                    break;
                  case BinaryOperation::kLessThanEquals:
                    all_ok = (pcol.max_int <= pred.constant);
                    break;
                  case BinaryOperation::kGreaterThan:
                    all_ok = (pcol.min_int > pred.constant);
                    break;
                  case BinaryOperation::kGreaterThanEquals:
                    all_ok = (pcol.min_int >= pred.constant);
                    break;
                  default:
                    break;
                }
                if (!all_ok) {
                  peek_all_pass = false;
                  break;
                }
              }
            }
            for (const LSTableDoubleZonePredicate& dz : lstable_double_zones) {
              const LSTableColumnBlock& dcol = blk->columns[dz.column];
              if (dcol.has_nan || std::isnan(dz.constant)) {
                continue;
              }
              if (!dcol.has_double_min_max) {
                zone_rejected = true;
                break;
              }
              switch (dz.op) {
                case BinaryOperation::kEquals:
                  if (dz.constant < dcol.min_double ||
                      dz.constant > dcol.max_double) {
                    zone_rejected = true;
                  }
                  break;
                case BinaryOperation::kNotEquals:
                  if (dcol.min_double == dz.constant &&
                      dcol.max_double == dz.constant) {
                    zone_rejected = true;
                  }
                  break;
                case BinaryOperation::kLessThan:
                  if (dcol.min_double >= dz.constant) {
                    zone_rejected = true;
                  }
                  break;
                case BinaryOperation::kLessThanEquals:
                  if (dcol.min_double > dz.constant) {
                    zone_rejected = true;
                  }
                  break;
                case BinaryOperation::kGreaterThan:
                  if (dcol.max_double <= dz.constant) {
                    zone_rejected = true;
                  }
                  break;
                case BinaryOperation::kGreaterThanEquals:
                  if (dcol.max_double < dz.constant) {
                    zone_rejected = true;
                  }
                  break;
                default:
                  break;
              }
              if (zone_rejected) {
                break;
              }
            }
            if (zone_rejected) {
              continue;
            }
            bool col_cmp_all_pass = true;
            for (const LSTableIntColCompare& cc : lstable_col_compares) {
              const LSTableColumnBlock& lcol = blk->columns[cc.left_col];
              const LSTableColumnBlock& rcol = blk->columns[cc.right_col];
              if (!lcol.has_min_max || !rcol.has_min_max) {
                zone_rejected = true;
                break;
              }
              bool all_ok = false;
              switch (cc.op) {
                case BinaryOperation::kEquals:
                  if (lcol.max_int < rcol.min_int ||
                      lcol.min_int > rcol.max_int) {
                    zone_rejected = true;
                  }
                  all_ok = (lcol.min_int == lcol.max_int &&
                            rcol.min_int == rcol.max_int &&
                            lcol.min_int == rcol.min_int);
                  break;
                case BinaryOperation::kNotEquals:
                  if (lcol.min_int == lcol.max_int &&
                      rcol.min_int == rcol.max_int &&
                      lcol.min_int == rcol.min_int) {
                    zone_rejected = true;
                  }
                  all_ok = (lcol.max_int < rcol.min_int ||
                            lcol.min_int > rcol.max_int);
                  break;
                case BinaryOperation::kLessThan:
                  if (lcol.min_int >= rcol.max_int) {
                    zone_rejected = true;
                  }
                  all_ok = (lcol.max_int < rcol.min_int);
                  break;
                case BinaryOperation::kLessThanEquals:
                  if (lcol.min_int > rcol.max_int) {
                    zone_rejected = true;
                  }
                  all_ok = (lcol.max_int <= rcol.min_int);
                  break;
                case BinaryOperation::kGreaterThan:
                  if (lcol.max_int <= rcol.min_int) {
                    zone_rejected = true;
                  }
                  all_ok = (lcol.min_int > rcol.max_int);
                  break;
                case BinaryOperation::kGreaterThanEquals:
                  if (lcol.max_int < rcol.min_int) {
                    zone_rejected = true;
                  }
                  all_ok = (lcol.min_int >= rcol.max_int);
                  break;
                default:
                  break;
              }
              if (zone_rejected) {
                break;
              }
              if (!all_ok || lcol.has_null || rcol.has_null) {
                col_cmp_all_pass = false;
              }
            }
            if (zone_rejected) {
              continue;
            }
            if (can_predecide_lstable) {
              for (const PrebuiltMembershipPredicate& pred :
                   scan_filter->prebuilt_membership) {
                if (pred.runtime_filter != nullptr) {
                  const slot_t full_col = to_full_col(pred.column);
                  if (full_col < blk->columns.size()) {
                    const LSTableColumnBlock& mcol = blk->columns[full_col];
                    if (mcol.type == pred.domain_type &&
                        (mcol.type == ValueType::kInt64 ||
                         mcol.type == ValueType::kDate)) {
                      if (!mcol.has_min_max ||
                          pred.runtime_filter->RejectRange(mcol.min_int,
                                                           mcol.max_int)) {
                        zone_rejected = true;
                        break;
                      }
                    }
                  }
                  continue;
                }
                if (pred.negated || pred.values == nullptr ||
                    pred.values->Empty()) {
                  continue;
                }
                const slot_t full_col = to_full_col(pred.column);
                if (full_col >= blk->columns.size()) {
                  continue;
                }
                const LSTableColumnBlock& mcol = blk->columns[full_col];
                if (mcol.type == pred.domain_type &&
                    (mcol.type == ValueType::kInt64 ||
                     mcol.type == ValueType::kDate)) {
                  if (!mcol.has_min_max || !pred.values->ContainsAnyInRange(
                                               mcol.min_int, mcol.max_int)) {
                    zone_rejected = true;
                    break;
                  }
                }
              }
              if (zone_rejected) {
                continue;
              }
            }
            bool null_rejected = false;
            for (const NullPeekCompare& test : null_tests) {
              if (test.expect_null && !blk->columns[test.column].has_null) {
                null_rejected = true;
                break;
              }
            }
            if (null_rejected) {
              continue;
            }
            const size_t col_count = projection != nullptr
                                         ? projection->size()
                                         : blk->columns.size();
            constexpr size_t kMaxFastProjCols = 16;
            std::array<const LSTableColumnBlock*, kMaxFastProjCols>
                proj_cols_buf{};
            std::array<const int64_t*, kMaxFastProjCols> proj_int_ptrs_buf{};
            std::array<ValueType, kMaxFastProjCols> proj_types_buf{};
            const LSTableColumnBlock* const* proj_cols_ptr = nullptr;
            const int64_t* const* proj_int_ptrs = nullptr;
            const ValueType* proj_types_ptr = nullptr;
            bool blk_fast_scalar = false;
            if (col_count <= kMaxFastProjCols) {
              blk_fast_scalar = true;
              for (size_t k = 0; k < col_count; ++k) {
                const slot_t col_idx = projection != nullptr
                                           ? (*projection)[k]
                                           : static_cast<slot_t>(k);
                const LSTableColumnBlock* cblk = &blk->columns[col_idx];
                proj_cols_buf[k] = cblk;
                proj_types_buf[k] = cblk->type;
                if (cblk->has_null || cblk->is_unsigned ||
                    !cblk->HasRawIntCells() ||
                    (cblk->type != ValueType::kInt64 &&
                     cblk->type != ValueType::kDate &&
                     cblk->type != ValueType::kDouble)) {
                  blk_fast_scalar = false;
                } else {
                  proj_int_ptrs_buf[k] = cblk->int_cells.data();
                }
              }
              proj_cols_ptr = proj_cols_buf.data();
              if (blk_fast_scalar) {
                proj_int_ptrs = proj_int_ptrs_buf.data();
                proj_types_ptr = proj_types_buf.data();
              }
            }
            if (worker_uniform_ok[w] != 0) {
              if (col_count != result_schema.ColumnCount()) {
                worker_uniform_ok[w] = 0;
              } else {
                for (size_t k = 0; k < col_count; ++k) {
                  const slot_t col_idx = projection != nullptr
                                             ? (*projection)[k]
                                             : static_cast<slot_t>(k);
                  const LSTableColumnBlock& cblk = blk->columns[col_idx];
                  if (cblk.type != ValueType::kNull &&
                      cblk.type != result_schema.GetColumn(k).Type()) {
                    worker_uniform_ok[w] = 0;
                    break;
                  }
                }
              }
            }
            const LSTableColumnBlock* const rf_col =
                (active_lstable_rf != nullptr && lstable_key_column.has_value())
                    ? &blk->columns[*lstable_key_column]
                    : nullptr;
            const bool rf_has_dense =
                rf_col != nullptr && active_lstable_rf->has_dense_bits();
            const uint64_t* const rf_dense_bits =
                rf_has_dense ? active_lstable_rf->dense_bits_data() : nullptr;
            const size_t rf_dense_words =
                rf_has_dense ? active_lstable_rf->dense_bits_word_count() : 0;
            const int64_t rf_lo = rf_has_dense ? active_lstable_rf->Lo() : 0;
            const int64_t rf_hi = rf_has_dense ? active_lstable_rf->Hi() : 0;
            const bool rf_col_has_null = rf_col != nullptr && rf_col->has_null;
            const bool rf_col_monotonic =
                rf_has_dense && !rf_col_has_null &&
                *lstable_key_column <
                    lstable_store->column_is_monotonic.size() &&
                lstable_store->column_is_monotonic[*lstable_key_column] != 0;
            struct BlockExtraRf {
              const LSTableColumnBlock* col{nullptr};
              const uint64_t* dense_bits{nullptr};
              int64_t lo{0};
              int64_t hi{0};
              bool has_null{false};
              const RuntimeFilterGroup* group{nullptr};
            };
            constexpr size_t kMaxFastExtraRfs = 4;
            std::array<BlockExtraRf, kMaxFastExtraRfs> block_extra_rfs{};
            const size_t fast_extra_rf_count =
                std::min(lstable_extra_rfs.size(), kMaxFastExtraRfs);
            for (size_t ei = 0; ei < fast_extra_rf_count; ++ei) {
              const LSTableExtraRf& erf = lstable_extra_rfs[ei];
              const LSTableColumnBlock* ecol = &blk->columns[erf.column];
              const bool has_dense = erf.group->has_dense_bits();
              block_extra_rfs[ei] = BlockExtraRf{
                  .col = ecol,
                  .dense_bits =
                      has_dense ? erf.group->dense_bits_data() : nullptr,
                  .lo = has_dense ? erf.group->Lo() : 0,
                  .hi = has_dense ? erf.group->Hi() : 0,
                  .has_null = ecol->has_null,
                  .group = erf.group,
              };
            }
            const bool block_all_pass =
                tuple_filter == nullptr && rf_col == nullptr &&
                fast_extra_rf_count == 0 && dict_all_pass && peek_all_pass &&
                col_cmp_all_pass;
            if (fast_in_memory_append && block_all_pass && null_tests.empty()) {
              // Pattern C: Dense All-Pass Streaming (s_block = 100%).
              const uint32_t nrows = blk->row_count;
              if (use_columnar_sink) {
                if (!flush_block_selection(*blk, proj_cols_ptr, nullptr,
                                           nrows)) {
                  break;
                }
              } else {
                for (uint32_t r = 0; r < nrows; ++r) {
                  Row& out_row = local.rows.emplace_back();
                  materialize_lstable_row(*blk, proj_cols_ptr, proj_int_ptrs,
                                          proj_types_ptr, blk_fast_scalar,
                                          col_count, r, &out_row);
                }
              }
              shard_counts[w].seen += nrows;
              shard_counts[w].out += nrows;
              continue;
            }
            if (tuple_filter == nullptr && fast_in_memory_append &&
                null_tests.empty() && rf_col != nullptr && rf_has_dense &&
                rf_col->HasRawIntCells() && !rf_col_monotonic &&
                fast_extra_rf_count == 0 && dict_all_pass && peek_all_pass &&
                col_cmp_all_pass) {
              const uint32_t nrows = blk->row_count;
              const int64_t* const rf_cells = rf_col->int_cells.data();
              const auto span_u =
                  static_cast<uint64_t>(rf_hi) - static_cast<uint64_t>(rf_lo);
              const auto lo_u = static_cast<uint64_t>(rf_lo);
              uint32_t matched_in_blk = 0;
              if (use_columnar_sink) {
                sel_buf.resize(nrows);
                uint32_t* out_sel = sel_buf.data();
                size_t out_n = 0;
                const auto* keys = active_lstable_rf->ExactKeys();
                if (keys != nullptr && !keys->empty() && keys->size() <= 4) {
                  out_n = SelectTinyRuntimeKeys(*rf_col, nrows, *keys,
                                                *active_lstable_rf, out_sel);
                } else {
                  for (uint32_t r = 0; r < nrows; ++r) {
                    const uint64_t off =
                        static_cast<uint64_t>(rf_cells[r]) - lo_u;
                    const bool hit =
                        (!rf_col_has_null || !rf_col->IsNull(r)) &&
                        off <= span_u &&
                        ((rf_dense_bits[off >> 6] >> (off & 63)) & 1) != 0;
                    out_sel[out_n] = r;
                    out_n += static_cast<size_t>(hit);
                  }
                }
                sel_buf.resize(out_n);
                matched_in_blk = static_cast<uint32_t>(sel_buf.size());
                if (!sel_buf.empty() &&
                    !flush_block_selection(*blk, proj_cols_ptr, sel_buf.data(),
                                           sel_buf.size())) {
                  break;
                }
                sel_buf.clear();
              } else {
                for (uint32_t r = 0; r < nrows; ++r) {
                  const uint64_t off =
                      static_cast<uint64_t>(rf_cells[r]) - lo_u;
                  if ((!rf_col_has_null || !rf_col->IsNull(r)) &&
                      off <= span_u &&
                      (rf_dense_bits[off >> 6] & (uint64_t{1} << (off & 63))) !=
                          0) {
                    Row& out_row = local.rows.emplace_back();
                    materialize_lstable_row(*blk, proj_cols_ptr, proj_int_ptrs,
                                            proj_types_ptr, blk_fast_scalar,
                                            col_count, r, &out_row);
                    ++matched_in_blk;
                  }
                }
              }
              shard_counts[w].seen += matched_in_blk;
              shard_counts[w].out += matched_in_blk;
              continue;
            }
            if (use_columnar_sink) {
              sel_buf.clear();
            }
            // For a sparse runtime key set, first collect matches in a tight
            // loop. The general predicate dispatch then visits only survivors,
            // instead of testing block-level flags for every rejected row.
            const LSTableColumnBlock* candidate_col = nullptr;
            const RuntimeFilterGroup* candidate_filter = nullptr;
            double candidate_density = 0.25;
            if (rf_col_monotonic) {
              // The ordered primary filter already skips whole rejected key
              // ranges. A secondary prefilter disables that skipping, so only
              // choose it when its estimated density beats the primary.
              const uint64_t span =
                  static_cast<uint64_t>(rf_hi) - static_cast<uint64_t>(rf_lo);
              candidate_density = std::min(
                  candidate_density,
                  static_cast<double>(active_lstable_rf->DistinctKeys()) /
                      (static_cast<double>(span) + 1.0));
            }
            const auto consider_candidate =
                [&](const LSTableColumnBlock* col,
                    const RuntimeFilterGroup* filter) {
                  if (col == nullptr || filter == nullptr ||
                      !filter->has_dense_bits() || !col->HasRawIntCells() ||
                      !col->has_min_max) {
                    return;
}
                  const uint64_t span = static_cast<uint64_t>(col->max_int) -
                                        static_cast<uint64_t>(col->min_int);
                  if (span == 0) { return;
}
                  const double density =
                      static_cast<double>(filter->DistinctKeys()) /
                      static_cast<double>(span);
                  if (density <= candidate_density) {
                    candidate_col = col;
                    candidate_filter = filter;
                    candidate_density = density;
                  }
                };
            if (!rf_col_monotonic) {
              consider_candidate(rf_col, active_lstable_rf);
            }
            // A secondary join key can be much more selective than the first
            // (e.g. a few items inside a broad date range). Use that key before
            // entering the general predicate loop as well.
            for (size_t ei = 0; ei < fast_extra_rf_count; ++ei) {
              consider_candidate(block_extra_rfs[ei].col,
                                 block_extra_rfs[ei].group);
            }
            // Apply a selective signed/date range before dictionary and
            // column-to-column predicate dispatch. This remains only a
            // prefilter; the complete predicate still checks every survivor.
            const ScanIntInterval* candidate_range = nullptr;
            for (const auto& interval : scan_intervals) {
              const auto& col = blk->columns[interval.column];
              if (!col.HasRawIntCells() || !col.has_min_max) { continue;
}
              const int64_t lo = std::max(interval.lo, col.min_int);
              const int64_t hi = std::min(interval.hi, col.max_int);
              const double density =
                  interval.empty || lo > hi
                      ? 0.0
                      : (static_cast<double>(static_cast<uint64_t>(hi) -
                                             static_cast<uint64_t>(lo)) +
                         1.0) /
                            (static_cast<double>(
                                 static_cast<uint64_t>(col.max_int) -
                                 static_cast<uint64_t>(col.min_int)) +
                             1.0);
              if (density < candidate_density) {
                candidate_density = density;
                candidate_col = &col;
                candidate_filter = nullptr;
                candidate_range = &interval;
              }
            }
            bool rf_preselected = candidate_col == rf_col &&
                                  candidate_filter == active_lstable_rf &&
                                  candidate_col != nullptr;
            uint32_t candidate_count = blk->row_count;
            if (runtime_key_runs && rf_col_monotonic &&
                candidate_col == nullptr && rf_col->has_min_max) {
              candidate_col = rf_col;
              rf_preselected = true;
              rf_candidates.resize(blk->row_count);
              candidate_count = 0;
              // Apply one secondary membership test while enumerating the
              // indexed rows. The general predicate loop still rechecks all
              // survivors, but avoids dispatching over rejected candidates.
              const BlockExtraRf* indexed_prefilter = nullptr;
              double extra_density = 0.5;
              for (size_t ei = 0; ei < fast_extra_rf_count; ++ei) {
                const auto& extra = block_extra_rfs[ei];
                if (!extra.dense_bits || !extra.col->HasRawIntCells() ||
                    !extra.col->has_min_max) {
                  continue;
}
                const uint64_t span =
                    static_cast<uint64_t>(extra.col->max_int) -
                    static_cast<uint64_t>(extra.col->min_int);
                const double density =
                    static_cast<double>(extra.group->DistinctKeys()) /
                    (static_cast<double>(span) + 1.0);
                if (density < extra_density) {
                  indexed_prefilter = &extra;
                  extra_density = density;
                }
              }
              const int64_t first_key = std::max(rf_lo, rf_col->min_int);
              const int64_t last_key = std::min(rf_hi, rf_col->max_int);
              if (first_key <= last_key) {
                const uint64_t first = static_cast<uint64_t>(first_key) -
                                       static_cast<uint64_t>(rf_lo);
                const uint64_t last = static_cast<uint64_t>(last_key) -
                                      static_cast<uint64_t>(rf_lo);
                for (size_t word_idx = first >> 6; word_idx <= (last >> 6);
                     ++word_idx) {
                  uint64_t bits = rf_dense_bits[word_idx];
                  if (word_idx == (first >> 6)) {
                    bits &= ~uint64_t{0} << (first & 63);
}
                  if (word_idx == (last >> 6)) {
                    bits &= ~uint64_t{0} >> (63 - (last & 63));
}
                  while (bits != 0) {
                    const uint64_t off =
                        (uint64_t{word_idx} << 6) + std::countr_zero(bits);
                    const auto key = std::bit_cast<int64_t>(
                        static_cast<uint64_t>(rf_lo) + off);
                    if (runtime_run_bounds && (bounded_interval->empty ||
                                               !runtime_run_bounds->MayOverlap(
                                                   key, bounded_interval->lo,
                                                   bounded_interval->hi))) {
                      bits &= bits - 1;
                      continue;
                    }
                    const auto [begin, end] = runtime_key_runs->Find(key);
                    const uint64_t lo = std::max<uint64_t>(begin, block_row);
                    const uint64_t hi =
                        std::min<uint64_t>(end, block_row + blk->row_count);
                    if (indexed_prefilter) {
                      const auto& extra = *indexed_prefilter;
                      const auto base = static_cast<uint64_t>(extra.lo);
                      const uint64_t span =
                          static_cast<uint64_t>(extra.hi) - base;
                      for (uint64_t row = lo; row < hi; ++row) {
                        const auto local_row =
                            static_cast<uint32_t>(row - block_row);
                        const uint64_t extra_offset =
                            static_cast<uint64_t>(
                                extra.col->int_cells[local_row]) -
                            base;
                        if ((!extra.has_null ||
                             !extra.col->IsNull(local_row)) &&
                            extra_offset <= span &&
                            ((extra.dense_bits[extra_offset >> 6] >>
                              (extra_offset & 63)) &
                             1)) {
                          rf_candidates[candidate_count++] = local_row;
}
                      }
                    } else {
                      for (uint64_t row = lo; row < hi; ++row) {
                        rf_candidates[candidate_count++] =
                            static_cast<uint32_t>(row - block_row);
}
                    }
                    bits &= bits - 1;
                  }
                }
              }
            } else if (candidate_col != nullptr) {
              rf_candidates.resize(blk->row_count);
              if (candidate_range != nullptr) {
                candidate_count =
                    candidate_range->empty
                        ? 0
                        : SelectIntegerRange(*candidate_col, blk->row_count,
                                             candidate_range->lo,
                                             candidate_range->hi,
                                             rf_candidates.data());
              } else {
                const int64_t* cells = candidate_col->int_cells.data();
                const auto lo =
                    static_cast<uint64_t>(candidate_filter->Lo());
                const uint64_t span =
                    static_cast<uint64_t>(candidate_filter->Hi()) - lo;
                const uint64_t* bits = candidate_filter->dense_bits_data();
                candidate_count = 0;
                const auto* keys = candidate_filter->ExactKeys();
                if (keys != nullptr && !keys->empty() && keys->size() <= 4) {
                  candidate_count = SelectTinyRuntimeKeys(
                      *candidate_col, blk->row_count, *keys, *candidate_filter,
                      rf_candidates.data());
                } else {
                  for (uint32_t r = 0; r < blk->row_count; ++r) {
                    const uint64_t off = static_cast<uint64_t>(cells[r]) - lo;
                    const bool hit = !candidate_col->IsNull(r) && off <= span &&
                                     ((bits[off >> 6] >> (off & 63)) & 1);
                    rf_candidates[candidate_count] = r;
                    candidate_count += static_cast<uint32_t>(hit);
                  }
                }
              }
            }
            for (uint32_t scan_pos = 0; scan_pos < candidate_count;
                 ++scan_pos) {
              const uint32_t r =
                  candidate_col != nullptr ? rf_candidates[scan_pos] : scan_pos;
              bool null_matches = true;
              for (const NullPeekCompare& test : null_tests) {
                if (blk->columns[test.column].IsNull(r) != test.expect_null) {
                  null_matches = false;
                  break;
                }
              }
              if (!null_matches) {
                continue;
              }
              if (rf_col != nullptr && !rf_preselected) {
                if (rf_col_has_null && rf_col->IsNull(r)) [[unlikely]] {
                  continue;
                }
                const int64_t kv = rf_col->int_cells[r];
                if (rf_has_dense) {
                  bool accepted = false;
                  if (kv >= rf_lo && kv <= rf_hi) {
                    const uint64_t off = static_cast<uint64_t>(kv) -
                                         static_cast<uint64_t>(rf_lo);
                    accepted = (rf_dense_bits[off >> 6] &
                                (uint64_t{1} << (off & 63))) != 0;
                  }
                  if (!accepted) {
                    if (rf_col_monotonic && candidate_col == nullptr) {
                      if (kv > rf_hi) {
                        break;
                      }
                      int64_t next_valid_key = rf_lo;
                      if (kv >= rf_lo) {
                        const uint64_t next_off = static_cast<uint64_t>(kv) -
                                                  static_cast<uint64_t>(rf_lo) +
                                                  1ULL;
                        auto w_idx = static_cast<size_t>(next_off >> 6);
                        if (w_idx >= rf_dense_words) {
                          break;
                        }
                        uint64_t word =
                            rf_dense_bits[w_idx] & (~0ULL << (next_off & 63));
                        while (word == 0) {
                          if (++w_idx >= rf_dense_words) {
                            break;
                          }
                          word = rf_dense_bits[w_idx];
                        }
                        if (w_idx >= rf_dense_words) {
                          break;
                        }
                        const uint64_t found_off =
                            (static_cast<uint64_t>(w_idx) << 6) +
                            static_cast<uint64_t>(std::countr_zero(word));
                        next_valid_key =
                            rf_lo + static_cast<int64_t>(found_off);
                        if (next_valid_key > rf_hi) {
                          break;
                        }
                      }
                      if (rf_col->has_min_max &&
                          next_valid_key > rf_col->max_int) {
                        break;
                      }
                      const uint32_t row_count = blk->row_count;
                      const int64_t* const cells = rf_col->int_cells.data();
                      if (r + 1 < row_count && cells[r + 1] < next_valid_key) {
                        uint32_t lo = r + 1;
                        uint32_t step = 4;
                        while (lo + step < row_count &&
                               cells[lo + step] < next_valid_key) {
                          lo += step;
                          step <<= 1;
                        }
                        const uint32_t hi = std::min(row_count, lo + step + 1);
                        const int64_t* found = std::lower_bound(
                            cells + lo, cells + hi, next_valid_key);
                        scan_pos = static_cast<uint32_t>(found - cells) - 1;
                      }
                    }
                    continue;
                  }
                } else if (active_lstable_rf->Reject(kv)) {
                  continue;
                }
              }
              if (fast_extra_rf_count != 0) {
                bool extra_ok = true;
                for (size_t ei = 0; ei < fast_extra_rf_count; ++ei) {
                  const BlockExtraRf& erf = block_extra_rfs[ei];
                  if (erf.has_null && erf.col->IsNull(r)) [[unlikely]] {
                    extra_ok = false;
                    break;
                  }
                  const int64_t kv = erf.col->int_cells[r];
                  if (erf.dense_bits != nullptr) {
                    if (kv < erf.lo || kv > erf.hi) {
                      extra_ok = false;
                      break;
                    }
                    const uint64_t off = static_cast<uint64_t>(kv) -
                                         static_cast<uint64_t>(erf.lo);
                    if ((erf.dense_bits[off >> 6] &
                         (uint64_t{1} << (off & 63))) == 0) {
                      extra_ok = false;
                      break;
                    }
                  } else if (erf.group->Reject(kv)) {
                    extra_ok = false;
                    break;
                  }
                }
                if (extra_ok && lstable_extra_rfs.size() > kMaxFastExtraRfs)
                    [[unlikely]] {
                  for (size_t ei = kMaxFastExtraRfs;
                       ei < lstable_extra_rfs.size(); ++ei) {
                    const LSTableExtraRf& erf = lstable_extra_rfs[ei];
                    const LSTableColumnBlock& ecol = blk->columns[erf.column];
                    if ((ecol.has_null && ecol.IsNull(r)) ||
                        erf.group->Reject(ecol.int_cells[r])) {
                      extra_ok = false;
                      break;
                    }
                  }
                }
                if (!extra_ok) {
                  continue;
                }
              }
              if (!dict_all_pass) {
                bool dict_ok = true;
                for (const auto& filter : lstable_wide_filters) {
                  const auto& column = blk->columns[filter.column];
                  if (column.IsNull(r)) {
                    if (!filter.allow_null) {
                      dict_ok = false;
                      break;
                    }
                    continue;
                  }
                  const auto code = column.DictionaryCodeAt(r);
                  if (code >= filter.pass.size() || filter.pass[code] == 0) {
                    dict_ok = false;
                    break;
                  }
                }
                if (!dict_ok) {
                  continue;
                }
                for (const LSTableDictColFilter& df : lstable_dict_filters) {
                  const LSTableColumnBlock& dcol = blk->columns[df.column];
                  if (dcol.has_null && dcol.IsNull(r)) {
                    if (!df.allow_null) {
                      dict_ok = false;
                      break;
                    }
                  } else if (dcol.dict_codes.empty() ||
                             df.pass[dcol.dict_codes[r]] == 0) {
                    dict_ok = false;
                    break;
                  }
                }
                if (!dict_ok) {
                  continue;
                }
                if (!active_disj_branches.empty()) {
                  bool disj_ok = false;
                  for (const DisjBranchFilter* db : active_disj_branches) {
                    bool br_ok = true;
                    for (const LSTableDictColFilter& df : db->narrow_cols) {
                      const LSTableColumnBlock& dcol = blk->columns[df.column];
                      if (dcol.has_null && dcol.IsNull(r)) {
                        if (!df.allow_null) {
                          br_ok = false;
                          break;
                        }
                      } else if (dcol.dict_codes.empty() ||
                                 df.pass[dcol.dict_codes[r]] == 0) {
                        br_ok = false;
                        break;
                      }
                    }
                    if (!br_ok) {
                      continue;
                    }
                    for (const LSTableWideDictFilter& wf : db->wide_cols) {
                      const LSTableColumnBlock& wcol = blk->columns[wf.column];
                      if (wcol.IsNull(r)) {
                        if (!wf.allow_null) {
                          br_ok = false;
                          break;
                        }
                        continue;
                      }
                      const uint32_t code = wcol.DictionaryCodeAt(r);
                      if (code >= wf.pass.size() || wf.pass[code] == 0) {
                        br_ok = false;
                        break;
                      }
                    }
                    if (br_ok) {
                      disj_ok = true;
                      break;
                    }
                  }
                  if (!disj_ok) {
                    continue;
                  }
                }
              }
              if (!peek_all_pass && peek_ptr != nullptr) {
                bool peek_ok = true;
                for (const IntegerPeekCompare& pred : *peek_ptr) {
                  const LSTableColumnBlock& pcol = blk->columns[pred.column];
                  if ((pcol.has_null && pcol.IsNull(r)) ||
                      !ComparePositional(pred.op, pcol.int_cells[r],
                                         pred.constant)) {
                    peek_ok = false;
                    break;
                  }
                }
                if (!peek_ok) {
                  continue;
                }
              }
              if (!col_cmp_all_pass) {
                bool cc_ok = true;
                for (const LSTableIntColCompare& cc : lstable_col_compares) {
                  const LSTableColumnBlock& lcol = blk->columns[cc.left_col];
                  const LSTableColumnBlock& rcol = blk->columns[cc.right_col];
                  if ((lcol.has_null && lcol.IsNull(r)) ||
                      (rcol.has_null && rcol.IsNull(r)) ||
                      !ComparePositional(cc.op, lcol.int_cells[r],
                                         rcol.int_cells[r])) {
                    cc_ok = false;
                    break;
                  }
                }
                if (!cc_ok) {
                  continue;
                }
              }
              if (tuple_filter != nullptr) {
                ++shard_counts[w].transfer_tested;
                if (tuple_filter->Reject(*blk, r)) {
                  ++shard_counts[w].transfer_rejected;
                  continue;
                }
              }
              ++shard_counts[w].seen;
              if (fast_in_memory_append) {
                if (use_columnar_sink) {
                  sel_buf.push_back(r);
                } else {
                  Row& out_row = local.rows.emplace_back();
                  materialize_lstable_row(*blk, proj_cols_ptr, proj_int_ptrs,
                                          proj_types_ptr, blk_fast_scalar,
                                          col_count, r, &out_row);
                }
                ++shard_counts[w].out;
                continue;
              }
              bool fully_decided = lstable_fast_loop_decides_all;
              if (!fully_decided &&
                  !match_lstable_pre_materialize(*blk, r, &fully_decided)) {
                continue;
              }
              if (fully_decided && has_ample_memory_budget) {
                if (use_columnar_sink) {
                  sel_buf.push_back(r);
                } else {
                  Row& out_row = local.rows.emplace_back();
                  materialize_lstable_row(*blk, proj_cols_ptr, proj_int_ptrs,
                                          proj_types_ptr, blk_fast_scalar,
                                          col_count, r, &out_row);
                }
                ++shard_counts[w].out;
                continue;
              }
              Row row;
              materialize_lstable_row(*blk, proj_cols_ptr, proj_int_ptrs,
                                      proj_types_ptr, blk_fast_scalar,
                                      col_count, r, &row);
              bool matches = true;
              if (!fully_decided) {
                if (worker_programs.complete) {
                  switch (MatchScanFilterWorker(
                      row, result_schema, *scan_filter, worker_programs)) {
                    case 0:
                      matches = false;
                      break;
                    case 1:
                      break;
                    default:
                      deferred_rows[w].push_back(std::move(row));
                      continue;
                  }
                } else if (filter_during_scan && scan_filter &&
                           !scan_filter->needs_unsigned_tagging) {
                  for (const SimpleComparePredicate& pred :
                       scan_filter->simple) {
                    if (!MatchSimpleCompare(row, pred)) {
                      matches = false;
                      break;
                    }
                  }
                }
              }
              if (matches) {
                if (use_columnar_sink) {
                  sel_buf.push_back(r);
                } else {
                  local.AddRow(std::move(row));
                }
                ++shard_counts[w].out;
              }
            }
            if (use_columnar_sink && !sel_buf.empty()) {
              const bool flushed = flush_block_selection(
                  *blk, proj_cols_ptr, sel_buf.data(), sel_buf.size());
              sel_buf.clear();
              if (!flushed) {
                break;
              }
            }
          }
        }
        if (use_columnar_sink && !apply_pushed_proj) {
          flush_pending_batch();
        }
        if (use_columnar_sink && cur_chunk_init && cur_chunk.rows > 0) {
          worker_columnar[w].AppendChunk(std::move(cur_chunk));
          cur_chunk = ColumnarChunk{};
          cur_chunk_init = false;
        }
        if (!use_columnar_sink && has_ample_memory_budget) {
          local.peak_intermediate_rows =
              std::max(local.peak_intermediate_rows, local.rows.size());
        }
        return;
      }
      // Static rotation: worker w scans morsels w, w+W, w+2W, ... .
      // The atomic counter this replaced handed morsels to whichever
      // worker grabbed them, so shard contents — and therefore the
      // shard-order concatenation below — changed run to run.  With
      // fixed ownership the concatenated relation is a deterministic
      // function of the table state, which the FP aggregates downstream
      // (TPC-DS rank boundaries) read as exact bits.
      for (size_t mi = w; mi < morsels.size(); mi += workers) {
        Iterator iterator = table.BeginMorselScan(
            context.txn_, morsels[mi], proj_opt, nullptr, full_key_column,
            integer_peeks.empty() ? nullptr : &integer_peeks, runtime_filter,
            runtime_filter_type);
        while (iterator.IsValid()) {
          ++shard_counts[w].seen;
          bool matches = true;
          if (worker_programs.complete) {
            switch (MatchScanFilterWorker(*iterator, result_schema,
                                          *scan_filter, worker_programs)) {
              case 0:
                matches = false;
                break;
              case 1:
                break;
              default:
                deferred_rows[w].push_back(std::move(*iterator));
                matches = false;
                break;
            }
          } else if (filter_during_scan && scan_filter &&
                     !scan_filter->needs_unsigned_tagging) {
            // Worker threads only evaluate the simple
            // (constant-vs-column) predicates. Residual predicates may
            // contain query expressions whose evaluation touches the
            // shared TransactionContext and its explicit execution
            // runtime, neither of which is safe on worker threads.
            for (const SimpleComparePredicate& pred : scan_filter->simple) {
              if (!MatchSimpleCompare(*iterator, pred)) {
                matches = false;
                break;
              }
            }
          }
          if (matches) {
            // The row is fully consumed here: move it instead of paying
            // one string copy per cell (the next ++ re-decodes anyway).
            local.AddRow(std::move(*iterator));
            ++shard_counts[w].out;
          }
          ++iterator;
        }
        if (iterator.GetStatus() != Status::kSuccess) {
          std::scoped_lock lock(error_mu);
          if (error != nullptr && *error == Status::kSuccess) {
            *error = iterator.GetStatus();
          }
          break;
        }
      }
    } catch (...) {
      std::scoped_lock lock(error_mu);
      if (error_ex == Status::kSuccess) {
        error_ex =
            StatusFromCurrentException("parallel table scan worker failed");
      }
    }
  };
  const Status scan_status = RunParallelTasks(workers, scan_worker);
  if (!scan_status.ok()) {
    if (error != nullptr) {
      *error = scan_status;
    }
    return false;
  }
  if (error_ex != Status::kSuccess) {
    if (error != nullptr) {
      *error = error_ex;
    }
    return false;
  }
  // A spill that fails mid-append is a truncated result, not a clean one.
  // Every merge branch below must surface the worker-shard status; the sticky
  // Relation::spill_error_ is otherwise discarded by ResetContents() and the
  // caller sees a short but "successful" result.
  const auto record_error = [&](Status status) {
    if (error != nullptr && error->ok() && status != Status::kSuccess) {
      *error = std::move(status);
    }
  };
  if (use_columnar_sink) {
    auto merged = std::make_shared<ColumnarTable>();
    merged->AppendKeepalive(lstable_store);
    ColumnarChunk deferred_chunk;
    bool deferred_chunk_init = false;
    for (size_t w = 0; w < workers; ++w) {
      merged->AppendTable(std::move(worker_columnar[w]));
      if (w < deferred_rows.size() && !deferred_rows[w].empty()) {
        for (Row& row : deferred_rows[w]) {
          Status deferred_error{Status::kSuccess};
          if (MatchScanFilter(row, result_schema, *scan_filter, outer, context,
                              ctes, &deferred_error)) {
            if (!deferred_chunk_init) {
              deferred_chunk.InitFromSchema(columnar_schema, 64);
              deferred_chunk_init = true;
            }
            if (!apply_pushed_proj) {
              deferred_chunk.AppendRow(row);
            } else {
              Row proj_row;
              proj_row.values_.reserve(pushed_plan->items.size());
              for (const auto& item : pushed_plan->items) {
                switch (item.kind) {
                  case CompiledColumnarProjection::Kind::kPassThrough:
                    proj_row.values_.push_back(item.src_col < row.Count()
                                                   ? row[item.src_col]
                                                   : Value());
                    break;
                  case CompiledColumnarProjection::Kind::kConstant:
                    proj_row.values_.push_back(item.constant);
                    break;
                  case CompiledColumnarProjection::Kind::kSubstring: {
                    const Value raw = item.src_col < row.Count()
                                          ? row[item.src_col]
                                          : Value();
                    if (raw.IsNull()) {
                      proj_row.values_.emplace_back();
                    } else if (raw.type == ValueType::kVarChar &&
                               !raw.IsInterval()) {
                      proj_row.values_.emplace_back(
                          std::string(SliceUtf8SubstringView(
                              raw.value.varchar_value, item.substr_start,
                              item.substr_length, item.substr_has_length)));
                    } else {
                      record_error(
                          StatusError(StatusCode::kInvalidArgument,
                                      "SUBSTR argument type mismatch"));
                      proj_row.values_.emplace_back();
                    }
                    break;
                  }
                  case CompiledColumnarProjection::Kind::kBytecode: {
                    StatusOr<Value> v = item.bytecode->TryEvaluateRow(row);
                    if (!v.HasValue()) {
                      record_error(v.GetStatus());
                      proj_row.values_.emplace_back();
                    } else {
                      proj_row.values_.push_back(v.MoveValue());
                    }
                    break;
                  }
                  case CompiledColumnarProjection::Kind::kNullCoalesce:
                  case CompiledColumnarProjection::Kind::kFallback:
                    record_error(StatusError(
                        StatusCode::kRuntimeError,
                        "unexpected fallback item in deferred scan row"));
                    proj_row.values_.emplace_back();
                    break;
                }
              }
              deferred_chunk.AppendRow(proj_row);
            }
            if (deferred_chunk.rows == kColumnarChunkRows) {
              merged->AppendChunk(std::move(deferred_chunk));
              deferred_chunk = ColumnarChunk{};
              deferred_chunk_init = false;
            }
          }
          if (!deferred_error.ok()) {
            record_error(std::move(deferred_error));
          }
        }
        deferred_rows[w].clear();
      }
      record_error(shards[w].GetStatus());
      shards[w].ResetContents();
    }
    if (deferred_chunk_init && deferred_chunk.rows > 0) {
      merged->AppendChunk(std::move(deferred_chunk));
    }
    bool all_uniform_non_null = false;
    if (apply_pushed_proj || merged->total_rows > 0) {
      result->schema = RefineSchemaFromColumnarTable(columnar_schema, *merged,
                                                     &all_uniform_non_null);
    } else {
      result->schema = columnar_schema;
    }
    const bool ok_status = (error == nullptr || error->ok());
    result->SetColumnarTable(std::move(merged),
                             ok_status && all_uniform_non_null);
    if (apply_pushed_proj && pushed_projection_applied != nullptr) {
      *pushed_projection_applied = true;
    }
    if (context.execution_runtime() != nullptr) {
      for (size_t w = 0; w < workers; ++w) {
        context.execution_runtime()->transfer_scan_tested +=
            shard_counts[w].transfer_tested;
        context.execution_runtime()->transfer_scan_rejected +=
            shard_counts[w].transfer_rejected;
        context.execution_runtime()->scan_rows += shard_counts[w].seen;
        context.execution_runtime()->scan_values_available +=
            shard_counts[w].seen * table.GetSchema().ColumnCount();
        context.execution_runtime()->scan_values_decoded +=
            shard_counts[w].out * result_schema.ColumnCount();
        context.execution_runtime()->scan_output_rows += shard_counts[w].out;
      }
    }
    return true;
  }
  size_t total_in_memory = 0;
  for (size_t w = 0; w < workers; ++w) {
    if (!shards[w].HasSpill()) {
      total_in_memory += shards[w].rows.size();
    }
  }
  if (worker_programs.complete || !has_post_simple_filter) {
    bool can_parallel_merge = has_ample_memory_budget && workers > 1 &&
                              total_in_memory >= 16384 &&
                              result->rows.empty() && !result->HasSpill();
    if (can_parallel_merge) {
      for (size_t w = 0; w < workers; ++w) {
        if (shards[w].HasSpill() ||
            (w < deferred_rows.size() && !deferred_rows[w].empty())) {
          can_parallel_merge = false;
          break;
        }
      }
    }
    if (can_parallel_merge) {
      std::vector<size_t> shard_offsets(workers + 1, 0);
      for (size_t w = 0; w < workers; ++w) {
        shard_offsets[w + 1] = shard_offsets[w] + shards[w].rows.size();
      }
      result->rows.resize(total_in_memory);
      std::vector<size_t> transferred_charges(workers, 0);
      {
        const auto move_shard = [&](size_t w) {
          std::move(shards[w].rows.begin(), shards[w].rows.end(),
                    result->rows.begin() +
                        static_cast<std::ptrdiff_t>(shard_offsets[w]));
          // The bytes live in result now; hand over the budget charge
          // instead of losing it when the shard is destroyed.
          transferred_charges[w] = shards[w].charged_bytes_;
          shards[w].charged_bytes_ = 0;
          shards[w].rows.clear();
          shards[w].rows.shrink_to_fit();
        };
        const Status merge_status = RunParallelTasks(workers, move_shard);
        if (!merge_status.ok()) {
          record_error(merge_status);
          return false;
        }
      }
      for (size_t charge : transferred_charges) {
        result->charged_bytes_ += charge;
      }

      result->peak_intermediate_rows =
          std::max(result->peak_intermediate_rows, result->rows.size());
    } else {
      if (total_in_memory > 0 && (workers > 1 || !result->rows.empty())) {
        const size_t reserve_count =
            has_ample_memory_budget ? total_in_memory
                                    : std::min<size_t>(total_in_memory, 65536);
        result->rows.reserve(result->rows.size() + reserve_count);
      }
      // Workers decided every row except the deferred ones; move results and
      // re-check the deferred rows with the contextual evaluator.
      for (size_t w = 0; w < workers; ++w) {
        if (w < deferred_rows.size()) {
          for (Row& row : deferred_rows[w]) {
            Status deferred_error{Status::kSuccess};
            if (MatchScanFilter(row, result_schema, *scan_filter, outer,
                                context, ctes, &deferred_error)) {
              result->AddRow(std::move(row));
            }
            if (!deferred_error.ok() && error != nullptr && error->ok()) {
              *error = std::move(deferred_error);
            }
          }
          deferred_rows[w].clear();
        }
        record_error(shards[w].FinishSpill());
        if (!shards[w].HasSpill()) {
          result->MergeInMemoryShard(std::move(shards[w]));
        } else {
          record_error(result->AdoptSpilledShard(std::move(shards[w])));
        }
        record_error(shards[w].GetStatus());
        shards[w].ResetContents();
      }
    }
  } else if (has_post_simple_filter) {
    // Re-apply every component the workers did not decide. Prebuilt sets
    // and prefix tests have no residual AST, so copying only residual and
    // OR branches silently drops them (notably NOT IN in TPC-H Q16).
    CompiledScanFilter residual_only = *scan_filter;
    if (!scan_filter->needs_unsigned_tagging) {
      residual_only.simple.clear();
    }
    if (can_predecide_lstable) {
      residual_only.prefix_in.clear();
      residual_only.prebuilt_membership.clear();
      if (disjunctive_covered_by_dict) {
        residual_only.disjunctive_branches.clear();
      }
    }
    Relation candidates(context.execution_runtime());
    candidates.schema = result_schema;
    record_error(Relation::MergeShardsInto(shards, &candidates));
    record_error(candidates.FinishSpill());
    record_error(candidates.GetStatus());
    ScopedCorrelatedOuter correlated_outer(context, candidates);
    ColumnLookupCache lookup_cache;
    const bool moved = candidates.ForEachMutableRow([&](Row& row) {
      if (!MatchScanFilter(row, result_schema, residual_only, outer, context,
                           ctes, error, &lookup_cache)) {
        return;
      }
      result->AddRow(std::move(row));
    });
    if (!moved) {
      record_error(candidates.ForEachRow([&](const Row& row) {
        if (!MatchScanFilter(row, result_schema, residual_only, outer, context,
                             ctes, error, &lookup_cache)) {
          return;
        }
        result->AddRow(row);
      }));
    }
    candidates.ResetContents();
  } else {
    record_error(Relation::MergeShardsInto(shards, result));
  }
  for (size_t w = 0; w < workers; ++w) {
    if (context.execution_runtime() != nullptr) {
      context.execution_runtime()->transfer_scan_tested +=
          shard_counts[w].transfer_tested;
      context.execution_runtime()->transfer_scan_rejected +=
          shard_counts[w].transfer_rejected;
      context.execution_runtime()->scan_rows += shard_counts[w].seen;
      context.execution_runtime()->scan_values_available +=
          shard_counts[w].seen * table.GetSchema().ColumnCount();
      context.execution_runtime()->scan_values_decoded +=
          shard_counts[w].out * result_schema.ColumnCount();
      context.execution_runtime()->scan_output_rows += shard_counts[w].out;
    }
  }
  if (lstable_store != nullptr && !result->HasSpill() &&
      !result->rows.empty() && (error == nullptr || error->ok()) &&
      std::ranges::all_of(worker_uniform_ok,
                          [](uint8_t ok) { return ok != 0; })) {
    // Only set when every row produced in this scan matches result_schema.
    bool cols_non_null = result_schema.ColumnCount() > 0;
    for (size_t k = 0; k < result_schema.ColumnCount(); ++k) {
      if (result_schema.GetColumn(k).Type() == ValueType::kNull) {
        cols_non_null = false;
        break;
      }
    }
    if (cols_non_null) {
      result->uniform_cell_types = true;
    }
  }
  return true;
}

}  // namespace tinylamb::relational_detail
