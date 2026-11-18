/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/columnar_table.hpp"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <string_view>
#include <thread>
#include <vector>

#include "common/parallel_work.hpp"
#include "common/query_parallelism.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/function_call_expression.hpp"

namespace tinylamb::relational_detail {

namespace {

bool IdentifierEqualsIgnoreCase(std::string_view left, std::string_view right) {
  if (left.size() != right.size()) {
    return false;
  }
  for (size_t i = 0; i < left.size(); ++i) {
    const auto lhs = static_cast<unsigned char>(left[i]);
    const auto rhs = static_cast<unsigned char>(right[i]);
    if (lhs == rhs) {
      continue;
    }
    if (lhs >= 0x80 || rhs >= 0x80) {
      if (std::tolower(lhs) != std::tolower(rhs)) {
        return false;
      }
      continue;
    }
    const unsigned char folded_left =
        lhs >= 'A' && lhs <= 'Z' ? lhs + ('a' - 'A') : lhs;
    const unsigned char folded_right =
        rhs >= 'A' && rhs <= 'Z' ? rhs + ('a' - 'A') : rhs;
    if (folded_left != folded_right) {
      return false;
    }
  }
  return true;
}

}  // namespace

std::optional<size_t> LocalColumnOffset(const Schema& schema,
                                        const ColumnName& name) {
  std::optional<size_t> match;
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    const ColumnName& candidate = schema.GetColumn(i).Name();
    if (!IdentifierEqualsIgnoreCase(candidate.name, name.name)) {
      continue;
    }
    if (!name.schema.empty() &&
        !IdentifierEqualsIgnoreCase(candidate.schema, name.schema)) {
      continue;
    }
    if (match) {
      return std::nullopt;
    }
    match = i;
  }
  if (!match && name.schema.empty()) {
    for (size_t i = 0; i < schema.ColumnCount(); ++i) {
      const ColumnName& candidate = schema.GetColumn(i).Name();
      if (IdentifierEqualsIgnoreCase(candidate.schema, name.name)) {
        if (match) {
          return std::nullopt;
        }
        match = i;
      }
    }
  }
  return match;
}

std::optional<size_t> ResolveScalarColumnOffset(const Schema& schema,
                                                const ColumnName& name) {
  std::optional<size_t> match;
  for (size_t i = 0; i < schema.ColumnCount(); ++i) {
    const ColumnName& candidate = schema.GetColumn(i).Name();
    if (!IdentifierEqualsIgnoreCase(candidate.name, name.name)) {
      continue;
    }
    if (!name.schema.empty() &&
        !IdentifierEqualsIgnoreCase(candidate.schema, name.schema)) {
      continue;
    }
    if (match) {
      return std::nullopt;
    }
    match = i;
  }
  return match;
}

bool ColumnarColumn::AnyNonNull(size_t row_count) const noexcept {
  if (row_count == 0) {
    return false;
  }
  switch (encoding) {
    case Encoding::kFlat:
      if (!is_uniform) {
        const size_t limit = std::min(row_count, fallback.size());
        for (size_t r = 0; r < limit; ++r) {
          if (!fallback[r].IsNull()) {
            return true;
          }
        }
        return false;
      }
      if (type == ValueType::kNull) {
        return false;
      }
      if (!has_null) {
        return true;
      }
      for (size_t r = 0; r < std::min(row_count, nulls.size()); ++r) {
        if (nulls[r] == 0) {
          return true;
        }
      }
      return false;
    case Encoding::kConstant:
      return !const_is_null && type != ValueType::kNull;
    case Encoding::kDictionary: {
      if (!has_null) {
        return dict != nullptr && DictEntryCount() > 0;
      }
      const size_t n = borrowed_dict_u8 != nullptr
                           ? std::min(row_count, borrowed_count)
                           : std::min(row_count, dict_codes.size());
      const size_t entry_cnt = DictEntryCount();
      for (size_t r = 0; r < n; ++r) {
        const uint32_t code = DictCodeAt(r);
        if (code != kNullDictCode && code < entry_cnt) {
          return true;
        }
      }
      return false;
    }
    case Encoding::kFrameOfReference:
      if (!has_null || borrowed_nulls == nullptr) {
        return borrowed_count > 0;
      }
      for (size_t r = 0; r < std::min(row_count, borrowed_count); ++r) {
        if (borrowed_nulls[r] == 0) {
          return true;
        }
      }
      return false;
    case Encoding::kGathered: {
      if (gather_index == nullptr) {
        return false;
      }
      const size_t n = std::min(row_count, gather_index->src_rows.size());
      for (size_t r = 0; r < n; ++r) {
        const ColumnarColumn* leaf = nullptr;
        size_t lr = 0;
        if (ResolveLeaf(r, &leaf, &lr)) {
          return true;
        }
      }
      return false;
    }
  }
  return false;
}

void ColumnarColumn::MaterializeDictCodes(size_t prev_rows) {
  if (encoding != Encoding::kDictionary || borrowed_dict_u8 == nullptr) {
    return;
  }
  const size_t n = std::min(prev_rows, borrowed_count);
  dict_codes.resize(n);
  if (!has_null) {
    for (size_t i = 0; i < n; ++i) {
      dict_codes[i] = borrowed_dict_u8[i];
    }
  } else {
    const uint8_t* bn = borrowed_nulls;
    for (size_t i = 0; i < n; ++i) {
      const uint8_t c = borrowed_dict_u8[i];
      dict_codes[i] = ((bn != nullptr && bn[i] != 0) ||
                       c == LSTableColumnDictionary::kNullCode)
                          ? kNullDictCode
                          : static_cast<uint32_t>(c);
    }
  }
  borrowed_dict_u8 = nullptr;
  borrowed_nulls = nullptr;
  borrowed_count = 0;
}

void ColumnarColumn::MaterializeFlatInto(size_t row_count, ColumnarColumn* dst,
                                         ChunkStringArena* arena) const {
  const size_t n = row_count;
  dst->Init(type, is_unsigned, is_interval, n);
  if (encoding == Encoding::kFlat) {
    *dst = *this;
    return;
  }
  if (!is_uniform) {
    dst->is_uniform = false;
    dst->ints.clear();
    dst->doubles.clear();
    dst->strings.clear();
    dst->fallback.resize(n);
    for (size_t r = 0; r < n; ++r) {
      WriteValueInto(r, &dst->fallback[r]);
      EnsureFallbackValueOwned(dst->fallback[r]);
    }
    return;
  }
  if (encoding == Encoding::kConstant) {
    if (const_is_null || type == ValueType::kNull) {
      if (type != ValueType::kNull) {
        dst->has_null = true;
        dst->nulls.assign(n, 1);
        switch (type) {
          case ValueType::kInt64:
          case ValueType::kDate:
            dst->ints.assign(n, 0);
            break;
          case ValueType::kDouble:
            dst->doubles.assign(n, 0.0);
            break;
          case ValueType::kVarChar:
            dst->strings.assign(n, std::string_view{});
            break;
          default:
            break;
        }
      }
      return;
    }
    switch (type) {
      case ValueType::kInt64:
      case ValueType::kDate:
        dst->ints.assign(n, const_int);
        break;
      case ValueType::kDouble:
        dst->doubles.assign(n, const_double);
        break;
      case ValueType::kVarChar: {
        const std::string_view sv =
            arena != nullptr ? arena->Store(const_string) : const_string;
        dst->strings.assign(n, sv);
        break;
      }
      default:
        break;
    }
    return;
  }
  if (encoding == Encoding::kFrameOfReference) {
    if (has_null) {
      dst->has_null = true;
      dst->nulls.resize(n);
      if (borrowed_nulls != nullptr) {
        std::memcpy(dst->nulls.data(), borrowed_nulls, n);
      } else {
        std::memset(dst->nulls.data(), 0, n);
      }
    }
    dst->ints.resize(n);
    int64_t* __restrict di = dst->ints.data();
    switch (for_width) {
      case 0:
        for (size_t i = 0; i < n; ++i) {
          di[i] = for_base;
        }
        break;
      case 1: {
        const auto base_u = static_cast<uint64_t>(for_base);
        for (size_t i = 0; i < n; ++i) {
          di[i] = static_cast<int64_t>(base_u + for_bytes[i]);
        }
        break;
      }
      case 2: {
        const auto base_u = static_cast<uint64_t>(for_base);
        for (size_t i = 0; i < n; ++i) {
          uint16_t d = 0;
          std::memcpy(&d, for_bytes + (i * sizeof(d)), sizeof(d));
          di[i] = static_cast<int64_t>(base_u + d);
        }
        break;
      }
      case 4: {
        const auto base_u = static_cast<uint64_t>(for_base);
        for (size_t i = 0; i < n; ++i) {
          uint32_t d = 0;
          std::memcpy(&d, for_bytes + (i * sizeof(d)), sizeof(d));
          di[i] = static_cast<int64_t>(base_u + d);
        }
        break;
      }
      case 8:
        std::memcpy(di, for_bytes, n * sizeof(int64_t));
        break;
      default:
        std::memset(di, 0, n * sizeof(int64_t));
        break;
    }
    return;
  }
  if (encoding == Encoding::kDictionary) {
    if (has_null) {
      dst->has_null = true;
      dst->nulls.resize(n);
    }
    const size_t entry_cnt = DictEntryCount();
    switch (type) {
      case ValueType::kVarChar: {
        dst->strings.resize(n);
        for (size_t i = 0; i < n; ++i) {
          const uint32_t c = DictCodeAt(i);
          if (c == kNullDictCode || c >= entry_cnt) {
            if (has_null) {
              dst->nulls[i] = 1;
            }
            dst->strings[i] = {};
          } else {
            if (has_null) {
              dst->nulls[i] = 0;
            }
            const std::string_view sv = dict->StringEntryAt(c);
            dst->strings[i] = arena != nullptr ? arena->Store(sv) : sv;
          }
        }
        break;
      }
      case ValueType::kInt64:
      case ValueType::kDate: {
        dst->ints.resize(n);
        for (size_t i = 0; i < n; ++i) {
          const uint32_t c = DictCodeAt(i);
          if (c == kNullDictCode || c >= entry_cnt) {
            if (has_null) {
              dst->nulls[i] = 1;
            }
            dst->ints[i] = 0;
          } else {
            if (has_null) {
              dst->nulls[i] = 0;
            }
            dst->ints[i] = dict->IntEntryAt(c);
          }
        }
        break;
      }
      case ValueType::kDouble: {
        dst->doubles.resize(n);
        const auto entries = dict->DoubleEntries();
        for (size_t i = 0; i < n; ++i) {
          const uint32_t c = DictCodeAt(i);
          if (c == kNullDictCode || c >= entry_cnt) {
            if (has_null) {
              dst->nulls[i] = 1;
            }
            dst->doubles[i] = 0.0;
          } else {
            if (has_null) {
              dst->nulls[i] = 0;
            }
            dst->doubles[i] = entries[c];
          }
        }
        break;
      }
      default:
        break;
    }
    return;
  }
  if (encoding == Encoding::kGathered) {
    if (type == ValueType::kNull) {
      return;
    }
    if (has_null) {
      dst->has_null = true;
      dst->nulls.resize(n);
    }
    switch (type) {
      case ValueType::kInt64:
      case ValueType::kDate:
        dst->ints.resize(n);
        break;
      case ValueType::kDouble:
        dst->doubles.resize(n);
        break;
      case ValueType::kVarChar:
        dst->strings.resize(n);
        break;
      default:
        break;
    }
    if ((type == ValueType::kInt64 || type == ValueType::kDate ||
         type == ValueType::kDouble) &&
        n > 0 && gather_index != nullptr &&
        gather_index->source_table != nullptr &&
        gather_index->src_chunks.size() >= n &&
        gather_index->src_rows.size() >= n) {
      const bool want_double = (type == ValueType::kDouble);
      struct GTableSlot {
        const ColumnarTable* tbl{nullptr};
        uint16_t col{0};
        uint32_t meta_base{0};
        uint32_t num_chunks{0};
      };
      struct GChunkMeta {
        // 0=fallback, 1=flat, 2=for, 3=const, 4=null, 5=gathered
        uint8_t kind{0};
        uint8_t for_width{0};
        bool has_null{false};
        int32_t next_tbl{-1};
        uint64_t for_base{0};
        const int64_t* ints{nullptr};
        const double* doubles{nullptr};
        const uint8_t* nulls{nullptr};
        const uint8_t* for_bytes{nullptr};
        const ColumnarColumn* leaf{nullptr};
        int64_t const_int{0};
        double const_double{0.0};
        const uint32_t* next_sc{nullptr};
        const uint32_t* next_sr{nullptr};
        size_t next_n{0};
      };
      std::vector<GTableSlot> tables;
      std::vector<GChunkMeta> metas;
      tables.reserve(8);
      metas.reserve(32);
      bool cache_ok = true;
      auto intern_table = [&](const ColumnarTable* tbl,
                              size_t col_idx) -> int32_t {
        if (tbl == nullptr) {
          return -1;
        }
        const auto col_u16 = static_cast<uint16_t>(col_idx);
        for (size_t ti = 0; ti < tables.size(); ++ti) {
          if (tables[ti].tbl == tbl && tables[ti].col == col_u16) {
            return static_cast<int32_t>(ti);
          }
        }
        if (tables.size() >= 64) {
          cache_ok = false;
          return -1;
        }
        const auto ti = static_cast<int32_t>(tables.size());
        const auto nch = static_cast<uint32_t>(tbl->chunks.size());
        const auto base = static_cast<uint32_t>(metas.size());
        tables.push_back(GTableSlot{
            .tbl = tbl, .col = col_u16, .meta_base = base, .num_chunks = nch});
        metas.resize(base + nch);
        return ti;
      };
      intern_table(gather_index->source_table.get(), gather_col);
      for (size_t ti = 0; cache_ok && ti < tables.size(); ++ti) {
        const GTableSlot slot = tables[ti];
        for (uint32_t ci = 0; ci < slot.num_chunks; ++ci) {
          GChunkMeta meta;
          const auto& chunk_cols = slot.tbl->chunks[ci].columns;
          if (slot.col >= chunk_cols.size()) {
            meta.kind = 4;
            metas[slot.meta_base + ci] = meta;
            continue;
          }
          const ColumnarColumn& sc = chunk_cols[slot.col];
          meta.leaf = &sc;
          if (sc.encoding == Encoding::kGathered) {
            const ChunkGatherIndex* gi = sc.gather_index.get();
            if (gi == nullptr || gi->source_table == nullptr) {
              meta.kind = 4;
            } else {
              const int32_t next_ti =
                  intern_table(gi->source_table.get(), sc.gather_col);
              if (!cache_ok || next_ti < 0) {
                break;
              }
              meta.kind = 5;
              meta.next_tbl = next_ti;
              meta.next_sc = gi->src_chunks.data();
              meta.next_sr = gi->src_rows.data();
              meta.next_n =
                  std::min(gi->src_chunks.size(), gi->src_rows.size());
            }
          } else {
            meta.kind = 0;
            if (sc.is_uniform && sc.type == type &&
                sc.is_unsigned == is_unsigned &&
                sc.is_interval == is_interval) {
              if (sc.encoding == Encoding::kConstant) {
                if (sc.const_is_null || sc.type == ValueType::kNull) {
                  meta.kind = 4;
                } else {
                  meta.kind = 3;
                  meta.const_int = sc.const_int;
                  meta.const_double = sc.const_double;
                }
              } else if (sc.encoding == Encoding::kFlat) {
                if (sc.type == ValueType::kNull) {
                  meta.kind = 4;
                } else {
                  meta.kind = 1;
                  meta.ints = sc.ints.data();
                  meta.doubles = sc.doubles.data();
                  meta.has_null = sc.has_null && !sc.nulls.empty();
                  meta.nulls = meta.has_null ? sc.nulls.data() : nullptr;
                }
              } else if (!want_double &&
                         sc.encoding == Encoding::kFrameOfReference) {
                meta.kind = 2;
                meta.for_width = sc.for_width;
                meta.for_base = static_cast<uint64_t>(sc.for_base);
                meta.for_bytes = sc.for_bytes;
                meta.has_null = sc.has_null && sc.borrowed_nulls != nullptr;
                meta.nulls = meta.has_null ? sc.borrowed_nulls : nullptr;
              }
            }
          }
          metas[slot.meta_base + ci] = meta;
        }
      }
      if (cache_ok && !tables.empty()) {
        const uint32_t* __restrict root_sc = gather_index->src_chunks.data();
        const uint32_t* __restrict root_sr = gather_index->src_rows.data();
        const uint32_t root_nchunks = tables[0].num_chunks;
        const GTableSlot* __restrict tbls = tables.data();
        const GChunkMeta* __restrict mdata = metas.data();
        uint8_t* __restrict dn = has_null ? dst->nulls.data() : nullptr;
        int64_t* __restrict di = !want_double ? dst->ints.data() : nullptr;
        double* __restrict dd = want_double ? dst->doubles.data() : nullptr;
        bool all_flat_nonnull = !has_null && !gather_index->has_null &&
                                tables.size() == 1 && root_nchunks > 0;
        if (all_flat_nonnull) {
          for (uint32_t ci = 0; ci < root_nchunks; ++ci) {
            if (mdata[ci].kind != 1 || mdata[ci].has_null) {
              all_flat_nonnull = false;
              break;
            }
          }
        }
        if (all_flat_nonnull) {
          if (!want_double) {
            for (size_t r = 0; r < n; ++r) {
              const uint32_t sc = root_sc[r];
              const uint32_t lr = root_sr[r];
              if (sc < root_nchunks && lr != ChunkGatherIndex::kNullRow)
                  [[likely]] {
                di[r] = mdata[sc].ints[lr];
              }
            }
          } else {
            for (size_t r = 0; r < n; ++r) {
              const uint32_t sc = root_sc[r];
              const uint32_t lr = root_sr[r];
              if (sc < root_nchunks && lr != ChunkGatherIndex::kNullRow)
                  [[likely]] {
                dd[r] = mdata[sc].doubles[lr];
              }
            }
          }
          return;
        }
        for (size_t r = 0; r < n; ++r) {
          uint32_t sc = root_sc[r];
          uint32_t lr = root_sr[r];
          if (lr == ChunkGatherIndex::kNullRow || sc >= root_nchunks)
              [[unlikely]] {
            if (dn != nullptr) {
              dn[r] = 1;
            }
            continue;
          }
          const GChunkMeta* m = &mdata[sc];
          bool is_null_row = false;
          while (m->kind == 5) {
            if (lr >= m->next_n) [[unlikely]] {
              is_null_row = true;
              break;
            }
            sc = m->next_sc[lr];
            lr = m->next_sr[lr];
            if (lr == ChunkGatherIndex::kNullRow || m->next_tbl < 0)
                [[unlikely]] {
              is_null_row = true;
              break;
            }
            const GTableSlot& ts = tbls[m->next_tbl];
            if (sc >= ts.num_chunks) [[unlikely]] {
              is_null_row = true;
              break;
            }
            m = &mdata[ts.meta_base + sc];
          }
          if (is_null_row || m->kind == 4) [[unlikely]] {
            if (dn != nullptr) {
              dn[r] = 1;
            }
            continue;
          }
          if (m->kind == 1) {
            if (m->has_null && m->nulls[lr] != 0) [[unlikely]] {
              if (dn != nullptr) {
                dn[r] = 1;
              }
              continue;
            }
            if (dn != nullptr) {
              dn[r] = 0;
            }
            if (!want_double) {
              di[r] = m->ints[lr];
            } else {
              dd[r] = m->doubles[lr];
            }
          } else if (m->kind == 2) {
            if (m->has_null && m->nulls[lr] != 0) [[unlikely]] {
              if (dn != nullptr) {
                dn[r] = 1;
              }
              continue;
            }
            if (dn != nullptr) {
              dn[r] = 0;
            }
            int64_t val = 0;
            switch (m->for_width) {
              case 0:
                val = static_cast<int64_t>(m->for_base);
                break;
              case 1:
                val = static_cast<int64_t>(m->for_base + m->for_bytes[lr]);
                break;
              case 2: {
                uint16_t d = 0;
                std::memcpy(&d, m->for_bytes + (static_cast<size_t>(lr) * 2),
                            sizeof(d));
                val = static_cast<int64_t>(m->for_base + d);
                break;
              }
              case 4: {
                uint32_t d = 0;
                std::memcpy(&d, m->for_bytes + (static_cast<size_t>(lr) * 4),
                            sizeof(d));
                val = static_cast<int64_t>(m->for_base + d);
                break;
              }
              case 8:
                std::memcpy(&val, m->for_bytes + (static_cast<size_t>(lr) * 8),
                            sizeof(val));
                break;
              default:
                break;
            }
            di[r] = val;
          } else if (m->kind == 3) {
            if (dn != nullptr) {
              dn[r] = 0;
            }
            if (!want_double) {
              di[r] = m->const_int;
            } else {
              dd[r] = m->const_double;
            }
          } else {
            if (m->leaf == nullptr || m->leaf->NonGatheredIsNull(lr)) {
              if (dn != nullptr) {
                dn[r] = 1;
              }
              continue;
            }
            if (dn != nullptr) {
              dn[r] = 0;
            }
            if (!want_double) {
              di[r] = m->leaf->IntAt(lr);
            } else {
              dd[r] = m->leaf->DoubleAt(lr);
            }
          }
        }
        return;
      }
    }
    for (size_t r = 0; r < n; ++r) {
      const ColumnarColumn* leaf = nullptr;
      size_t lr = 0;
      if (!ResolveLeaf(r, &leaf, &lr)) {
        if (has_null) {
          dst->nulls[r] = 1;
        }
        continue;
      }
      if (has_null) {
        dst->nulls[r] = 0;
      }
      switch (type) {
        case ValueType::kInt64:
        case ValueType::kDate:
          dst->ints[r] = leaf->IntAt(lr);
          break;
        case ValueType::kDouble:
          dst->doubles[r] = leaf->DoubleAt(lr);
          break;
        case ValueType::kVarChar: {
          const std::string_view sv = leaf->StringAt(lr);
          dst->strings[r] = arena != nullptr ? arena->Store(sv) : sv;
          break;
        }
        default:
          break;
      }
    }
  }
}

void ColumnarColumn::MaterializeToFlat(size_t row_count,
                                       ChunkStringArena* arena) {
  if (encoding == Encoding::kFlat) {
    return;
  }
  std::shared_ptr<ChunkStringArena> created_arena;
  ChunkStringArena* effective_arena = arena;
  if (effective_arena == nullptr && type == ValueType::kVarChar) {
    created_arena = std::make_shared<ChunkStringArena>();
    effective_arena = created_arena.get();
  }
  ColumnarColumn flat;
  MaterializeFlatInto(row_count, &flat, effective_arena);
  if (created_arena != nullptr) {
    flat.local_arena = std::move(created_arena);
  }
  *this = std::move(flat);
}

BytecodeColumnView ColumnarColumn::ToBytecodeView(
    size_t row_count, ColumnarColumn* flat_scratch,
    ChunkStringArena* scratch_arena) const {
  if (encoding == Encoding::kFlat) {
    return AsBytecodeView();
  }
  MaterializeFlatInto(row_count, flat_scratch, scratch_arena);
  return flat_scratch->AsBytecodeView();
}

void ColumnarColumn::AppendConstant(const Value& v, size_t count,
                                    ChunkStringArena* arena, size_t prev_rows) {
  if (count == 0) {
    return;
  }
  const bool can_constant_scalar =
      v.IsNull() ||
      (v.type != ValueType::kArray &&
       (v.type == ValueType::kInt64     ? (v.Collation() == 0 || v.IsUnsigned())
        : v.type == ValueType::kVarChar ? (v.Collation() == 0 || v.IsInterval())
                                        : v.Collation() == 0));
  if (prev_rows == 0 && encoding == Encoding::kFlat && is_uniform &&
      can_constant_scalar) {
    if (v.IsNull()) {
      const bool preserve_typed_null = (type != ValueType::kNull);
      if (preserve_typed_null) {
        SetConstantNull(type, is_unsigned, is_interval, count);
      } else {
        SetConstantNull(ValueType::kNull, false, false, count);
      }
      return;
    }
    if (type == ValueType::kNull ||
        (v.type == type && v.IsUnsigned() == is_unsigned &&
         v.IsInterval() == is_interval)) {
      switch (v.type) {
        case ValueType::kInt64:
        case ValueType::kDate:
          SetConstantInt(v.type, v.IsUnsigned(), v.value.int_value, count);
          return;
        case ValueType::kDouble:
          SetConstantDouble(v.value.double_value, count);
          return;
        case ValueType::kVarChar: {
          const std::string_view stored =
              arena != nullptr ? arena->Store(v.value.varchar_value)
                               : v.value.varchar_value;
          std::shared_ptr<ChunkStringArena> own;
          if (arena == nullptr && !stored.empty()) {
            own = std::make_shared<ChunkStringArena>();
            SetConstantString(own->Store(stored), v.IsInterval(), count, own);
          } else {
            SetConstantString(stored, v.IsInterval(), count);
          }
          return;
        }
        default:
          break;
      }
    }
  }
  if (encoding == Encoding::kConstant) {
    if (v.IsNull() && (const_is_null || type == ValueType::kNull)) {
      const_rows = std::max(const_rows, prev_rows);
      const_rows += count;
      if (type != ValueType::kNull) {
        has_null = true;
      }
      return;
    }
    if (!v.IsNull() && !const_is_null && v.type == type &&
        v.IsUnsigned() == is_unsigned && v.IsInterval() == is_interval &&
        v.Collation() == 0) {
      const bool same = (type == ValueType::kInt64 || type == ValueType::kDate)
                            ? (v.value.int_value == const_int)
                        : (type == ValueType::kDouble)
                            ? (std::bit_cast<uint64_t>(v.value.double_value) ==
                               std::bit_cast<uint64_t>(const_double))
                        : (type == ValueType::kVarChar)
                            ? (v.value.varchar_value == const_string)
                            : false;
      if (same) {
        const_rows = std::max(const_rows, prev_rows);
        const_rows += count;
        return;
      }
    }
    MaterializeToFlat(prev_rows, arena);
  } else if (encoding != Encoding::kFlat) {
    MaterializeToFlat(prev_rows, arena);
  }
  if (!is_uniform) {
    fallback.resize(fallback.size() + count, v);
    return;
  }
  if (v.IsNull()) {
    if (type == ValueType::kNull) {
      return;
    }
    EnsureNullBitmap(prev_rows);
    nulls.resize(prev_rows + count, 1);
    switch (type) {
      case ValueType::kInt64:
      case ValueType::kDate:
        ints.resize(prev_rows + count, 0);
        break;
      case ValueType::kDouble:
        doubles.resize(prev_rows + count, 0.0);
        break;
      case ValueType::kVarChar:
        strings.resize(prev_rows + count);
        break;
      default:
        break;
    }
    return;
  }
  const bool type_compatible =
      v.type != ValueType::kArray && v.type == type &&
      ((v.type == ValueType::kInt64 &&
        ((is_unsigned && v.IsUnsigned()) ||
         (!is_unsigned && v.Collation() == 0))) ||
       (v.type == ValueType::kVarChar &&
        ((is_interval && v.IsInterval()) ||
         (!is_interval && v.Collation() == 0))) ||
       ((v.type == ValueType::kDate || v.type == ValueType::kDouble) &&
        v.Collation() == 0));
  if (!type_compatible) {
    if (type == ValueType::kNull && prev_rows == 0 &&
        v.type != ValueType::kArray &&
        (v.type == ValueType::kInt64     ? v.Collation() == 0 || v.IsUnsigned()
         : v.type == ValueType::kVarChar ? v.Collation() == 0 || v.IsInterval()
                                         : v.Collation() == 0)) {
      Init(v.type, v.IsUnsigned(), v.IsInterval(), count);
    } else {
      DemoteToFallback(prev_rows);
      fallback.resize(fallback.size() + count, v);
      return;
    }
  }
  if (has_null) {
    nulls.resize(prev_rows + count, 0);
  }
  switch (type) {
    case ValueType::kInt64:
    case ValueType::kDate:
      ints.resize(prev_rows + count, v.value.int_value);
      break;
    case ValueType::kDouble:
      doubles.resize(prev_rows + count, v.value.double_value);
      break;
    case ValueType::kVarChar: {
      ChunkStringArena* target_arena =
          arena != nullptr ? arena : EnsureLocalArena();
      const std::string_view stored =
          target_arena->Store(v.value.varchar_value);
      strings.resize(prev_rows + count, stored);
      break;
    }
    default:
      DemoteToFallback(prev_rows);
      fallback.resize(fallback.size() + count, v);
      break;
  }
}

void ColumnarColumn::AppendSliceFrom(const ColumnarColumn& src,
                                     const uint32_t* sel, size_t count,
                                     ChunkStringArena* arena,
                                     size_t prev_rows) {
  if (count == 0) {
    return;
  }
  // Fast zero-copy / encoded transfers when starting a new output chunk slice
  // or extending a compatible dictionary/constant column.
  if (prev_rows == 0 && encoding == Encoding::kFlat &&
      (type == ValueType::kNull ||
       (src.is_uniform && src.type == type && src.is_unsigned == is_unsigned &&
        src.is_interval == is_interval) ||
       (!is_uniform && !src.is_uniform))) {
    if (src.encoding == Encoding::kConstant) {
      if (src.const_is_null || src.type == ValueType::kNull) {
        SetConstantNull(
            type != ValueType::kNull ? type : src.type,
            type != ValueType::kNull ? is_unsigned : src.is_unsigned,
            type != ValueType::kNull ? is_interval : src.is_interval, count);
      } else if (src.type == ValueType::kVarChar) {
        const std::string_view stored = arena != nullptr
                                            ? arena->Store(src.const_string)
                                            : src.const_string;
        SetConstantString(stored, src.is_interval, count,
                          arena == nullptr ? src.encoding_owner : nullptr);
        if (arena == nullptr) {
          RetainOwnersFrom(src);
        }
      } else if (src.type == ValueType::kDouble) {
        SetConstantDouble(src.const_double, count);
      } else {
        SetConstantInt(src.type, src.is_unsigned, src.const_int, count);
      }
      return;
    }
    if (src.encoding == Encoding::kDictionary) {
      if (sel == nullptr && src.borrowed_dict_u8 != nullptr &&
          count <= src.borrowed_count) {
        SetBorrowedNarrowDict(src.dict, src.encoding_owner, src.type,
                              src.is_unsigned, src.is_interval, src.has_null,
                              src.borrowed_dict_u8, src.borrowed_nulls, count);
        for (const auto& owner : src.extra_owners) {
          RetainOwner(owner);
        }
        return;
      }
      Init(src.type, src.is_unsigned, src.is_interval, 0);
      encoding = Encoding::kDictionary;
      encoding_owner = src.encoding_owner;
      extra_owners = src.extra_owners;
      dict = src.dict;
      has_null = src.has_null;
      dict_codes.resize(count);
      if (sel == nullptr) {
        if (src.borrowed_dict_u8 != nullptr) {
          for (size_t i = 0; i < count; ++i) {
            dict_codes[i] = src.DictCodeAt(i);
          }
        } else {
          std::memcpy(dict_codes.data(), src.dict_codes.data(),
                      count * sizeof(uint32_t));
        }
      } else {
        for (size_t i = 0; i < count; ++i) {
          dict_codes[i] = src.DictCodeAt(sel[i]);
        }
      }
      return;
    }
    if (src.encoding == Encoding::kFrameOfReference && sel == nullptr &&
        count <= src.borrowed_count) {
      SetBorrowedFrameOfReference(src.encoding_owner, src.type, src.is_unsigned,
                                  src.for_width, src.for_base, src.for_bytes,
                                  src.has_null, src.borrowed_nulls, count);
      return;
    }
    if (src.encoding == Encoding::kGathered && sel == nullptr &&
        src.gather_index != nullptr &&
        count == src.gather_index->src_rows.size() &&
        count == src.gather_index->src_chunks.size()) {
      SetGathered(src.gather_index, src.gather_col, src.type, src.is_unsigned,
                  src.is_interval, src.is_uniform, src.has_null);
      return;
    }
  }
  if (encoding == Encoding::kDictionary &&
      src.encoding == Encoding::kDictionary && dict == src.dict &&
      type == src.type && is_unsigned == src.is_unsigned &&
      is_interval == src.is_interval) {
    if (borrowed_dict_u8 != nullptr) {
      MaterializeDictCodes(prev_rows);
    }
    RetainOwner(src.encoding_owner);
    for (const auto& owner : src.extra_owners) {
      RetainOwner(owner);
    }
    has_null = has_null || src.has_null;
    const size_t base = dict_codes.size();
    dict_codes.resize(base + count);
    if (sel == nullptr && src.borrowed_dict_u8 == nullptr) {
      std::memcpy(dict_codes.data() + base, src.dict_codes.data(),
                  count * sizeof(uint32_t));
    } else if (sel == nullptr) {
      for (size_t i = 0; i < count; ++i) {
        dict_codes[base + i] = src.DictCodeAt(i);
      }
    } else {
      for (size_t i = 0; i < count; ++i) {
        dict_codes[base + i] = src.DictCodeAt(sel[i]);
      }
    }
    return;
  }
  if (encoding != Encoding::kFlat) {
    MaterializeToFlat(prev_rows, arena);
  }
  if (src.encoding != Encoding::kFlat) {
    if (!is_uniform || !src.is_uniform || src.type != type ||
        src.is_unsigned != is_unsigned || src.is_interval != is_interval) {
      if (is_uniform && src.is_uniform && src.type == ValueType::kNull &&
          type != ValueType::kNull) {
        EnsureNullBitmap(prev_rows);
        nulls.resize(prev_rows + count, 1);
        switch (type) {
          case ValueType::kInt64:
          case ValueType::kDate:
            ints.resize(prev_rows + count, 0);
            break;
          case ValueType::kDouble:
            doubles.resize(prev_rows + count, 0.0);
            break;
          case ValueType::kVarChar:
            strings.resize(prev_rows + count);
            break;
          default:
            break;
        }
        return;
      }
      if (!UpgradeNullTo(src.type, src.is_unsigned, src.is_interval,
                         prev_rows)) {
        DemoteToFallback(prev_rows);
        fallback.reserve(fallback.size() + count);
        for (size_t i = 0; i < count; ++i) {
          const size_t r = sel != nullptr ? sel[i] : i;
          Value v = src.ValueAt(r);
          EnsureFallbackValueOwned(v);
          fallback.push_back(std::move(v));
        }
        return;
      }
    }
    if (type == ValueType::kNull) {
      return;
    }
    if (src.has_null && !has_null) {
      EnsureNullBitmap(prev_rows);
    }
    if (has_null) {
      nulls.resize(prev_rows + count);
    }
    switch (type) {
      case ValueType::kInt64:
      case ValueType::kDate: {
        ints.resize(prev_rows + count);
        int64_t* __restrict dst_i = ints.data() + prev_rows;
        uint8_t* __restrict dst_n =
            has_null ? nulls.data() + prev_rows : nullptr;
        for (size_t i = 0; i < count; ++i) {
          const size_t r = sel != nullptr ? sel[i] : i;
          const bool null = src.IsNull(r);
          if (dst_n != nullptr) {
            dst_n[i] = null ? 1 : 0;
          }
          dst_i[i] = null ? 0 : src.IntAt(r);
        }
        break;
      }
      case ValueType::kDouble: {
        doubles.resize(prev_rows + count);
        double* __restrict dst_d = doubles.data() + prev_rows;
        uint8_t* __restrict dst_n =
            has_null ? nulls.data() + prev_rows : nullptr;
        for (size_t i = 0; i < count; ++i) {
          const size_t r = sel != nullptr ? sel[i] : i;
          const bool null = src.IsNull(r);
          if (dst_n != nullptr) {
            dst_n[i] = null ? 1 : 0;
          }
          dst_d[i] = null ? 0.0 : src.DoubleAt(r);
        }
        break;
      }
      case ValueType::kVarChar: {
        strings.resize(prev_rows + count);
        std::string_view* __restrict dst_s = strings.data() + prev_rows;
        uint8_t* __restrict dst_n =
            has_null ? nulls.data() + prev_rows : nullptr;
        for (size_t i = 0; i < count; ++i) {
          const size_t r = sel != nullptr ? sel[i] : i;
          const bool null = src.IsNull(r);
          if (dst_n != nullptr) {
            dst_n[i] = null ? 1 : 0;
          }
          if (null) {
            dst_s[i] = {};
          } else {
            const std::string_view sv = src.StringAt(r);
            dst_s[i] = arena != nullptr ? arena->Store(sv) : sv;
          }
        }
        if (arena == nullptr) {
          RetainOwnersFrom(src);
        }
        break;
      }
      default:
        break;
    }
    return;
  }
  if (!is_uniform || !src.is_uniform || src.type != type ||
      src.is_unsigned != is_unsigned || src.is_interval != is_interval) {
    if (is_uniform && src.is_uniform && src.type == ValueType::kNull &&
        type != ValueType::kNull) {
      EnsureNullBitmap(prev_rows);
      nulls.resize(prev_rows + count, 1);
      switch (type) {
        case ValueType::kInt64:
        case ValueType::kDate:
          ints.resize(prev_rows + count, 0);
          break;
        case ValueType::kDouble:
          doubles.resize(prev_rows + count, 0.0);
          break;
        case ValueType::kVarChar:
          strings.resize(prev_rows + count);
          break;
        default:
          break;
      }
      return;
    }
    if (!UpgradeNullTo(src.type, src.is_unsigned, src.is_interval, prev_rows)) {
      DemoteToFallback(prev_rows);
      fallback.reserve(fallback.size() + count);
      for (size_t i = 0; i < count; ++i) {
        const size_t r = sel != nullptr ? sel[i] : i;
        Value v = src.ValueAt(r);
        EnsureFallbackValueOwned(v);
        fallback.push_back(std::move(v));
      }
      return;
    }
  }
  if (type == ValueType::kNull) {
    return;
  }
  if (src.has_null && !has_null) {
    EnsureNullBitmap(prev_rows);
  }
  if (has_null) {
    nulls.resize(prev_rows + count);
    uint8_t* __restrict dst_n = nulls.data() + prev_rows;
    if (!src.has_null) {
      std::memset(dst_n, 0, count);
    } else if (sel == nullptr) {
      std::memcpy(dst_n, src.nulls.data(), count);
    } else {
      const uint8_t* __restrict src_n = src.nulls.data();
      for (size_t i = 0; i < count; ++i) {
        dst_n[i] = src_n[sel[i]];
      }
    }
  }
  switch (type) {
    case ValueType::kInt64:
    case ValueType::kDate: {
      ints.resize(prev_rows + count);
      int64_t* __restrict dst = ints.data() + prev_rows;
      const int64_t* __restrict s = src.ints.data();
      if (sel == nullptr) {
        std::memcpy(dst, s, count * sizeof(int64_t));
      } else {
        for (size_t i = 0; i < count; ++i) {
          dst[i] = s[sel[i]];
        }
      }
      break;
    }
    case ValueType::kDouble: {
      doubles.resize(prev_rows + count);
      double* __restrict dst = doubles.data() + prev_rows;
      const double* __restrict s = src.doubles.data();
      if (sel == nullptr) {
        std::memcpy(dst, s, count * sizeof(double));
      } else {
        for (size_t i = 0; i < count; ++i) {
          dst[i] = s[sel[i]];
        }
      }
      break;
    }
    case ValueType::kVarChar: {
      strings.resize(prev_rows + count);
      std::string_view* __restrict dst = strings.data() + prev_rows;
      const std::string_view* __restrict s = src.strings.data();
      if (arena == nullptr) {
        if (sel == nullptr) {
          std::memcpy(dst, s, count * sizeof(std::string_view));
        } else {
          for (size_t i = 0; i < count; ++i) {
            dst[i] = s[sel[i]];
          }
        }
        RetainOwnersFrom(src);
      } else {
        const uint8_t* __restrict sn =
            src.has_null ? src.nulls.data() : nullptr;
        for (size_t i = 0; i < count; ++i) {
          const size_t r = sel != nullptr ? sel[i] : i;
          dst[i] = (sn != nullptr && sn[r] != 0) ? std::string_view{}
                                                 : arena->Store(s[r]);
        }
      }
      break;
    }
    default:
      break;
  }
}

namespace {

inline size_t Utf8CodepointStep(std::string_view s, size_t i) noexcept {
  const auto c = static_cast<unsigned char>(s[i]);
  if ((c & 0x80U) == 0U) {
    return 1;
  }
  if ((c & 0xE0U) == 0xC0U) {
    return (i + 1 < s.size()) ? 2 : 1;
  }
  if ((c & 0xF0U) == 0xE0U) {
    return (i + 2 < s.size()) ? 3 : (i + 1 < s.size() ? 2 : 1);
  }
  if ((c & 0xF8U) == 0xF0U) {
    return (i + 3 < s.size())   ? 4
           : (i + 2 < s.size()) ? 3
                                : (i + 1 < s.size() ? 2 : 1);
  }
  return 1;
}

}  // namespace

std::string_view SliceUtf8SubstringView(std::string_view s, int64_t start,
                                        int64_t length,
                                        bool has_length) noexcept {
  if (s.empty() || (has_length && length <= 0)) {
    return {};
  }
  size_t actual_start = 0;
  if (start > 0) {
    actual_start = static_cast<size_t>(start - 1);
  } else if (start < 0) {
    size_t total_cps = 0;
    for (size_t i = 0; i < s.size(); i += Utf8CodepointStep(s, i)) {
      ++total_cps;
    }
    const int64_t signed_start = static_cast<int64_t>(total_cps) + start;
    actual_start = signed_start > 0 ? static_cast<size_t>(signed_start) : 0;
  }
  if (actual_start >= s.size()) {
    // Since every code point takes >= 1 byte, total_cps <= s.size().
    return {};
  }
  if (actual_start == 0 && has_length) {
    const auto len_u = static_cast<size_t>(length);
    if (len_u <= s.size()) {
      unsigned char any_non_ascii = 0;
      for (size_t j = 0; j < len_u; ++j) {
        any_non_ascii |= static_cast<unsigned char>(s[j]);
      }
      if ((any_non_ascii & 0x80U) == 0U) {
        return s.substr(0, len_u);
      }
    }
  }
  size_t byte_start = 0;
  for (size_t cp = 0; cp < actual_start && byte_start < s.size(); ++cp) {
    byte_start += Utf8CodepointStep(s, byte_start);
  }
  if (byte_start >= s.size()) {
    return {};
  }
  if (!has_length) {
    return s.substr(byte_start);
  }
  const auto len_u = static_cast<size_t>(length);
  size_t byte_end = byte_start;
  for (size_t cp = 0; cp < len_u && byte_end < s.size(); ++cp) {
    byte_end += Utf8CodepointStep(s, byte_end);
  }
  return s.substr(byte_start, byte_end - byte_start);
}

bool ColumnarColumn::AppendSubstringFrom(const ColumnarColumn& src,
                                         size_t src_offset, const uint32_t* sel,
                                         size_t count, int64_t start,
                                         int64_t length, bool has_length,
                                         size_t prev_rows) {
  if (count == 0) {
    return true;
  }
  if (!src.is_uniform) {
    for (size_t i = 0; i < count; ++i) {
      const size_t r = sel != nullptr ? sel[i] : (src_offset + i);
      const Value v = src.ValueAt(r);
      if (!v.IsNull() && (v.type != ValueType::kVarChar || v.IsInterval())) {
        return false;
      }
    }
    if (encoding != Encoding::kFlat) {
      MaterializeToFlat(prev_rows, nullptr);
    }
    if (is_uniform && type == ValueType::kNull) {
      UpgradeNullTo(ValueType::kVarChar, false, false, prev_rows);
    }
    if (!is_uniform || type != ValueType::kVarChar || is_interval) {
      return false;
    }
    strings.resize(prev_rows + count);
    for (size_t i = 0; i < count; ++i) {
      const size_t r = sel != nullptr ? sel[i] : (src_offset + i);
      const Value v = src.ValueAt(r);
      if (v.IsNull()) {
        if (!has_null) {
          EnsureNullBitmap(prev_rows + count);
        }
        nulls[prev_rows + i] = 1;
      } else {
        if (has_null) {
          nulls[prev_rows + i] = 0;
        }
        strings[prev_rows + i] = SliceUtf8SubstringView(
            v.value.varchar_value, start, length, has_length);
      }
    }
    RetainOwnersFrom(src);
    return true;
  }
  if (src.type == ValueType::kNull) {
    if (prev_rows == 0 && encoding == Encoding::kFlat && !is_interval &&
        (type == ValueType::kNull || type == ValueType::kVarChar)) {
      SetConstantNull(ValueType::kVarChar, false, false, count);
      return true;
    }
    AppendConstant(Value(), count, nullptr, prev_rows);
    return true;
  }
  if (src.type != ValueType::kVarChar || src.is_interval) {
    return false;
  }
  if (src.encoding == Encoding::kConstant) {
    const std::string_view sliced =
        src.const_is_null ? std::string_view{}
                          : SliceUtf8SubstringView(src.const_string, start,
                                                   length, has_length);
    if (prev_rows == 0 && encoding == Encoding::kFlat &&
        (type == ValueType::kNull ||
         (type == ValueType::kVarChar && !is_interval))) {
      if (src.const_is_null) {
        SetConstantNull(ValueType::kVarChar, false, false, count);
      } else {
        SetConstantString(sliced, false, count, src.encoding_owner);
        RetainOwnersFrom(src);
      }
      return true;
    }
    if (encoding == Encoding::kConstant && type == ValueType::kVarChar &&
        !is_interval && !const_is_null && !src.const_is_null &&
        const_string == sliced) {
      const_rows = std::max(const_rows, prev_rows);
      const_rows += count;
      return true;
    }
    if (src.const_is_null) {
      AppendConstant(Value(), count, nullptr, prev_rows);
      return true;
    }
    if (encoding != Encoding::kFlat) {
      MaterializeToFlat(prev_rows, nullptr);
    }
    if (is_uniform && type == ValueType::kNull) {
      UpgradeNullTo(ValueType::kVarChar, false, false, prev_rows);
    }
    strings.resize(prev_rows + count, sliced);
    if (has_null) {
      nulls.resize(prev_rows + count, 0);
    }
    RetainOwnersFrom(src);
    return true;
  }
  if (encoding != Encoding::kFlat) {
    MaterializeToFlat(prev_rows, nullptr);
  }
  if (is_uniform && type == ValueType::kNull) {
    UpgradeNullTo(ValueType::kVarChar, false, false, prev_rows);
  }
  if (!is_uniform || type != ValueType::kVarChar || is_interval) {
    return false;
  }
  if (src.has_null && !has_null) {
    EnsureNullBitmap(prev_rows);
  }
  if (has_null) {
    nulls.resize(prev_rows + count, 0);
  }
  strings.resize(prev_rows + count);
  std::string_view* __restrict dst_s = strings.data() + prev_rows;
  if (src.encoding == Encoding::kDictionary && src.dict != nullptr &&
      src.dict->StringEntryCount() <= 4096) {
    const size_t entry_cnt = src.dict->StringEntryCount();
    std::vector<std::string_view> dict_sliced(entry_cnt);
    for (size_t e = 0; e < entry_cnt; ++e) {
      dict_sliced[e] = SliceUtf8SubstringView(src.dict->StringEntryAt(e), start,
                                              length, has_length);
    }
    for (size_t i = 0; i < count; ++i) {
      const size_t r = sel != nullptr ? sel[i] : (src_offset + i);
      const uint32_t code = src.DictCodeAt(r);
      if ((src.has_null && src.IsNull(r)) || code >= entry_cnt) {
        if (!has_null) {
          EnsureNullBitmap(prev_rows + count);
        }
        nulls[prev_rows + i] = 1;
      } else {
        dst_s[i] = dict_sliced[code];
      }
    }
    RetainOwnersFrom(src);
    return true;
  }
  if (src.encoding == Encoding::kFlat && !src.has_null && sel == nullptr &&
      src_offset + count <= src.strings.size()) {
    const std::string_view* __restrict s = src.strings.data() + src_offset;
    for (size_t i = 0; i < count; ++i) {
      dst_s[i] = SliceUtf8SubstringView(s[i], start, length, has_length);
    }
    RetainOwnersFrom(src);
    return true;
  }
  for (size_t i = 0; i < count; ++i) {
    const size_t r = sel != nullptr ? sel[i] : (src_offset + i);
    if (src.IsNull(r)) {
      if (!has_null) {
        EnsureNullBitmap(prev_rows + count);
      }
      nulls[prev_rows + i] = 1;
    } else {
      dst_s[i] =
          SliceUtf8SubstringView(src.StringAt(r), start, length, has_length);
    }
  }
  RetainOwnersFrom(src);
  return true;
}

CompiledColumnarProjection CompiledColumnarProjection::Compile(
    const std::vector<Expression>& expressions, const Schema& input_schema,
    Schema out_schema) {
  CompiledColumnarProjection plan;
  plan.output_schema = std::move(out_schema);
  plan.items.reserve(expressions.size());
  plan.all_fast = true;
  std::vector<uint8_t> col_used(input_schema.ColumnCount(), 0);
  const auto mark_col = [&](slot_t c) {
    if (c < col_used.size() && col_used[c] == 0) {
      col_used[c] = 1;
    }
  };
  for (const Expression& expr : expressions) {
    Item item;
    if (!expr) {
      plan.all_fast = false;
      plan.items.push_back(std::move(item));
      continue;
    }
    if (expr->Type() == TypeTag::kColumnValue) {
      const auto off = ResolveScalarColumnOffset(
          input_schema, expr->AsColumnValue().GetColumnName());
      if (off.has_value() && *off < input_schema.ColumnCount()) {
        item.kind = Kind::kPassThrough;
        item.src_col = static_cast<slot_t>(*off);
        mark_col(item.src_col);
        plan.items.push_back(std::move(item));
        continue;
      }
      plan.all_fast = false;
      plan.items.push_back(std::move(item));
      continue;
    }
    if (expr->Type() == TypeTag::kConstantValue) {
      item.kind = Kind::kConstant;
      item.constant = expr->AsConstantValue().GetValue();
      plan.items.push_back(std::move(item));
      continue;
    }
    if (expr->Type() == TypeTag::kFunctionCallExp) {
      const FunctionCallExpression& fn = expr->AsFunctionCallExpression();
      if ((fn.FuncName() == "substr" || fn.FuncName() == "substring") &&
          (fn.Args().size() == 2 || fn.Args().size() == 3)) {
        const Expression& a0 = fn.Args()[0];
        const Expression& a1 = fn.Args()[1];
        if (a0 && a0->Type() == TypeTag::kColumnValue && a1 &&
            a1->Type() == TypeTag::kConstantValue) {
          const auto off = ResolveScalarColumnOffset(
              input_schema, a0->AsColumnValue().GetColumnName());
          const Value& v1 = a1->AsConstantValue().GetValue();
          bool bounds_ok =
              off.has_value() && *off < input_schema.ColumnCount() &&
              input_schema.GetColumn(*off).Type() == ValueType::kVarChar &&
              !input_schema.GetColumn(*off).IsInterval() && !v1.IsNull() &&
              v1.type == ValueType::kInt64 && !v1.IsUnsigned();
          int64_t len = 0;
          const bool has_length = fn.Args().size() == 3;
          if (bounds_ok && has_length) {
            const Expression& a2 = fn.Args()[2];
            bounds_ok = a2 && a2->Type() == TypeTag::kConstantValue;
            if (bounds_ok) {
              const Value& v2 = a2->AsConstantValue().GetValue();
              bounds_ok = !v2.IsNull() && v2.type == ValueType::kInt64 &&
                          !v2.IsUnsigned() && v2.value.int_value >= 0;
              len = v2.value.int_value;
            }
          }
          if (bounds_ok) {
            item.kind = Kind::kSubstring;
            item.src_col = static_cast<slot_t>(*off);
            item.substr_start = v1.value.int_value;
            item.substr_length = len;
            item.substr_has_length = has_length;
            mark_col(item.src_col);
            plan.items.push_back(std::move(item));
            continue;
          }
        }
      }
    }
    if (BytecodeEnabled()) {
      std::optional<BytecodeProgram> compiled =
          BytecodeCompiler::CompileRowCached(expr, input_schema,
                                             BytecodeCompiler::Context::kValue);
      if (compiled.has_value() && compiled->FastInterpretable()) {
        item.kind = Kind::kBytecode;
        for (const BytecodeInstruction& inst : compiled->Instructions()) {
          if (inst.opcode == BytecodeOp::kLoadColumn) {
            mark_col(inst.operand);
          }
        }
        item.bytecode = std::move(*compiled);
        plan.items.push_back(std::move(item));
        continue;
      }
    }
    plan.all_fast = false;
    plan.items.push_back(std::move(item));
  }
  return plan;
}

StatusOr<std::optional<std::shared_ptr<ColumnarTable>>> TryProjectColumnarTable(
    const CompiledColumnarProjection& plan, const ColumnarTable& input) {
  if (!plan.all_fast) {
    return std::optional<std::shared_ptr<ColumnarTable>>(std::nullopt);
  }
  auto out = std::make_shared<ColumnarTable>();
  out->keepalives = input.keepalives;
  if (input.total_rows == 0 || input.chunks.empty()) {
    return std::optional<std::shared_ptr<ColumnarTable>>(std::move(out));
  }
  const size_t nchunks = input.chunks.size();
  const size_t nitems = plan.items.size();
  const size_t max_in_cols = [&]() {
    size_t m = 0;
    for (const ColumnarChunk& c : input.chunks) {
      m = std::max(m, c.columns.size());
    }
    return m;
  }();
  std::vector<uint8_t> bytecode_needed_col(max_in_cols, 0);
  for (const auto& item : plan.items) {
    if (item.kind == CompiledColumnarProjection::Kind::kBytecode &&
        item.bytecode.has_value()) {
      for (const BytecodeInstruction& inst : item.bytecode->Instructions()) {
        if (inst.opcode == BytecodeOp::kLoadColumn &&
            inst.operand < bytecode_needed_col.size()) {
          bytecode_needed_col[inst.operand] = 1;
        }
      }
    }
  }
  std::vector<ColumnarChunk> out_chunks(nchunks);
  std::atomic<bool> abort_flag{false};
  std::atomic<uint8_t> unsupported_flag{0};
  std::vector<Status> worker_errors;

  const auto eval_chunk = [&](size_t ci, Status* err_out) {
    if (abort_flag.load(std::memory_order_relaxed)) {
      return;
    }
    const ColumnarChunk& src = input.chunks[ci];
    if (src.rows == 0) {
      return;
    }
    ColumnarChunk& dst = out_chunks[ci];
    dst.InitFromSchema(plan.output_schema, 0);
    if (src.string_arena != nullptr) {
      dst.AppendKeepaliveUnique(src.string_arena);
    }
    for (const auto& k : src.keepalives) {
      dst.AppendKeepaliveUnique(k);
    }
    std::vector<BytecodeColumnView> views;
    std::vector<ColumnarColumn> view_scratches;
    for (size_t k = 0; k < nitems; ++k) {
      const auto& item = plan.items[k];
      ColumnarColumn& col = dst.columns[k];
      switch (item.kind) {
        case CompiledColumnarProjection::Kind::kPassThrough: {
          if (item.src_col < src.columns.size()) {
            const ColumnarColumn& sc = src.columns[item.src_col];
            dst.RetainColumnOwners(sc);
            col.AppendSliceFrom(sc, nullptr, src.rows, nullptr, 0);
          } else {
            col.AppendConstant(Value(), src.rows, nullptr, 0);
          }
          break;
        }
        case CompiledColumnarProjection::Kind::kConstant: {
          ChunkStringArena* arena = (!item.constant.IsNull() &&
                                     item.constant.type == ValueType::kVarChar)
                                        ? dst.EnsureArena()
                                        : nullptr;
          col.AppendConstant(item.constant, src.rows, arena, 0);
          break;
        }
        case CompiledColumnarProjection::Kind::kSubstring: {
          if (item.src_col >= src.columns.size()) {
            col.AppendConstant(Value(), src.rows, nullptr, 0);
            break;
          }
          const ColumnarColumn& sc = src.columns[item.src_col];
          dst.RetainColumnOwners(sc);
          if (!col.AppendSubstringFrom(sc, 0, nullptr, src.rows,
                                       item.substr_start, item.substr_length,
                                       item.substr_has_length, 0)) {
            unsupported_flag.store(1, std::memory_order_relaxed);
            abort_flag.store(true, std::memory_order_relaxed);
            return;
          }
          break;
        }
        case CompiledColumnarProjection::Kind::kBytecode: {
          if (views.empty()) {
            views.resize(src.columns.size());
            view_scratches.resize(src.columns.size());
            for (size_t c = 0; c < src.columns.size(); ++c) {
              if (c < bytecode_needed_col.size() &&
                  bytecode_needed_col[c] != 0) {
                const ColumnarColumn& sc = src.columns[c];
                dst.RetainColumnOwners(sc);
                views[c] =
                    sc.ToBytecodeView(src.rows, &view_scratches[c], nullptr);
              } else {
                views[c] = BytecodeColumnView{};
              }
            }
          }
          const Column& out_col_schema = plan.output_schema.GetColumn(k);
          col.Init(out_col_schema.Type(), out_col_schema.IsUnsigned(),
                   out_col_schema.IsInterval(), src.rows);
          BytecodeColumnSink sink = col.AsBytecodeSink(0);
          Status eval_err{Status::kSuccess};
          const auto st = item.bytecode->TryEvaluateColumnarBatch(
              views, nullptr, src.rows, &sink, &eval_err);
          if (st == BytecodeProgram::FastEval::kDone) {
            col.SyncFromBytecodeSink(
                sink,
                sink.may_borrow_constant_strings ? dst.EnsureArena() : nullptr,
                src.rows);
          } else if (st == BytecodeProgram::FastEval::kError) {
            *err_out = std::move(eval_err);
            abort_flag.store(true, std::memory_order_relaxed);
            return;
          } else {
            unsupported_flag.store(1, std::memory_order_relaxed);
            abort_flag.store(true, std::memory_order_relaxed);
            return;
          }
          break;
        }
        case CompiledColumnarProjection::Kind::kFallback:
          unsupported_flag.store(1, std::memory_order_relaxed);
          abort_flag.store(true, std::memory_order_relaxed);
          return;
      }
    }
    dst.rows = src.rows;
  };

  const size_t workers =
      input.total_rows >= 4096
          ? std::min<size_t>(
                {QueryParallelWorkerLimit(std::thread::hardware_concurrency()),
                 nchunks, input.total_rows / 2048})
          : 1;
  if (workers <= 1) {
    Status err{Status::kSuccess};
    for (size_t ci = 0; ci < nchunks; ++ci) {
      eval_chunk(ci, &err);
      if (!err.ok()) {
        return err;
      }
      if (unsupported_flag.load(std::memory_order_relaxed) != 0) {
        return std::optional<std::shared_ptr<ColumnarTable>>(std::nullopt);
      }
    }
  } else {
    worker_errors.assign(workers, Status::kSuccess);
    Status p_st = RunParallelTasks(workers, [&](size_t w) {
      const size_t lo = (nchunks * w) / workers;
      const size_t hi = (nchunks * (w + 1)) / workers;
      for (size_t ci = lo; ci < hi; ++ci) {
        eval_chunk(ci, &worker_errors[w]);
        if (abort_flag.load(std::memory_order_relaxed)) {
          break;
        }
      }
    });
    RETURN_IF_FAIL(p_st);
    for (const Status& st : worker_errors) {
      RETURN_IF_FAIL(st);
    }
    if (unsupported_flag.load(std::memory_order_relaxed) != 0) {
      return std::optional<std::shared_ptr<ColumnarTable>>(std::nullopt);
    }
  }
  for (ColumnarChunk& c : out_chunks) {
    out->AppendChunk(std::move(c));
  }
  return std::optional<std::shared_ptr<ColumnarTable>>(std::move(out));
}

std::shared_ptr<ColumnarTable> SliceColumnarTable(const ColumnarTable& input,
                                                  size_t begin, size_t count) {
  auto out = std::make_shared<ColumnarTable>();
  out->keepalives = input.keepalives;
  if (count == 0 || begin >= input.total_rows) {
    return out;
  }
  const size_t end = std::min(input.total_rows, begin + count);
  size_t cursor = 0;
  for (const ColumnarChunk& src : input.chunks) {
    const size_t c_begin = cursor;
    const size_t c_end = cursor + src.rows;
    cursor = c_end;
    if (c_end <= begin || c_begin >= end || src.rows == 0) {
      continue;
    }
    const size_t lo = c_begin < begin ? begin - c_begin : 0;
    const size_t hi = std::min(src.rows, end - c_begin);
    if (lo == 0 && hi == src.rows) {
      out->AppendChunk(ColumnarChunk(src));
      continue;
    }
    const size_t sub_count = hi - lo;
    ColumnarChunk sub;
    sub.rows = sub_count;
    sub.string_arena = src.string_arena;
    sub.keepalives = src.keepalives;
    sub.columns.resize(src.columns.size());
    std::vector<uint32_t> sel;
    std::vector<
        std::pair<const ChunkGatherIndex*, std::shared_ptr<ChunkGatherIndex>>>
        sliced_gathers;
    const auto slice_gather =
        [&](const std::shared_ptr<const ChunkGatherIndex>& gi)
        -> std::shared_ptr<ChunkGatherIndex> {
      for (const auto& entry : sliced_gathers) {
        if (entry.first == gi.get()) {
          return entry.second;
        }
      }
      auto sub_gi = std::make_shared<ChunkGatherIndex>();
      sub_gi->source_table = gi->source_table;
      sub_gi->src_chunks.assign(
          gi->src_chunks.begin() + static_cast<std::ptrdiff_t>(lo),
          gi->src_chunks.begin() + static_cast<std::ptrdiff_t>(hi));
      sub_gi->src_rows.assign(
          gi->src_rows.begin() + static_cast<std::ptrdiff_t>(lo),
          gi->src_rows.begin() + static_cast<std::ptrdiff_t>(hi));
      if (gi->has_null) {
        for (uint32_t sr : sub_gi->src_rows) {
          if (sr == ChunkGatherIndex::kNullRow) {
            sub_gi->has_null = true;
            break;
          }
        }
      }
      sliced_gathers.emplace_back(gi.get(), sub_gi);
      return sub_gi;
    };
    for (size_t c = 0; c < src.columns.size(); ++c) {
      const ColumnarColumn& scol = src.columns[c];
      if (scol.encoding == ColumnarColumn::Encoding::kConstant) {
        sub.columns[c].InitEmptyLike(scol, 0);
        sub.columns[c].AppendSliceFrom(scol, nullptr, sub_count, nullptr, 0);
        continue;
      }
      if (scol.encoding == ColumnarColumn::Encoding::kFrameOfReference &&
          hi <= scol.borrowed_count) {
        const size_t byte_stride =
            scol.for_width == 0 ? 0 : static_cast<size_t>(scol.for_width);
        sub.columns[c].SetBorrowedFrameOfReference(
            scol.encoding_owner, scol.type, scol.is_unsigned, scol.for_width,
            scol.for_base,
            scol.for_bytes != nullptr ? scol.for_bytes + (lo * byte_stride)
                                      : nullptr,
            scol.has_null,
            scol.borrowed_nulls != nullptr ? scol.borrowed_nulls + lo : nullptr,
            sub_count);
        continue;
      }
      if (scol.encoding == ColumnarColumn::Encoding::kDictionary &&
          scol.borrowed_dict_u8 != nullptr && hi <= scol.borrowed_count) {
        sub.columns[c].SetBorrowedNarrowDict(
            scol.dict, scol.encoding_owner, scol.type, scol.is_unsigned,
            scol.is_interval, scol.has_null, scol.borrowed_dict_u8 + lo,
            scol.borrowed_nulls != nullptr ? scol.borrowed_nulls + lo : nullptr,
            sub_count);
        continue;
      }
      if (scol.encoding == ColumnarColumn::Encoding::kGathered &&
          scol.gather_index != nullptr &&
          hi <= scol.gather_index->src_rows.size() &&
          hi <= scol.gather_index->src_chunks.size()) {
        sub.columns[c].SetGathered(
            slice_gather(scol.gather_index), scol.gather_col, scol.type,
            scol.is_unsigned, scol.is_interval, scol.is_uniform, scol.has_null);
        continue;
      }
      if (sel.empty()) {
        sel.resize(sub_count);
        for (size_t i = 0; i < sub_count; ++i) {
          sel[i] = static_cast<uint32_t>(lo + i);
        }
      }
      sub.columns[c].InitEmptyLike(scol, sub_count);
      sub.columns[c].AppendSliceFrom(scol, sel.data(), sub_count, nullptr, 0);
    }
    out->AppendChunk(std::move(sub));
  }
  return out;
}

Status MaterializeColumnarTableToRows(const ColumnarTable& table,
                                      const Schema& schema,
                                      std::vector<Row>* out_rows,
                                      bool* uniform_ok) {
  out_rows->clear();
  if (uniform_ok != nullptr) {
    *uniform_ok = false;
  }
  if (table.total_rows == 0 || table.chunks.empty()) {
    return Status::kSuccess;
  }
  out_rows->resize(table.total_rows);
  std::vector<size_t> chunk_offsets(table.chunks.size(), 0);
  size_t running = 0;
  bool all_uniform = true;
  const size_t schema_cols = schema.ColumnCount();
  for (size_t ci = 0; ci < table.chunks.size(); ++ci) {
    chunk_offsets[ci] = running;
    running += table.chunks[ci].rows;
    if (table.chunks[ci].columns.size() != schema_cols) {
      all_uniform = false;
    } else {
      for (size_t c = 0; c < schema_cols; ++c) {
        const ColumnarColumn& col = table.chunks[ci].columns[c];
        const Column& scol = schema.GetColumn(c);
        if (!col.is_uniform || col.type == ValueType::kNull ||
            col.type != scol.Type() || col.is_unsigned != scol.IsUnsigned() ||
            col.is_interval != scol.IsInterval()) {
          all_uniform = false;
        }
      }
    }
  }
  if (uniform_ok != nullptr) {
    *uniform_ok = all_uniform;
  }
  const size_t workers =
      table.total_rows >= 8192
          ? std::min<size_t>(
                {QueryParallelWorkerLimit(std::thread::hardware_concurrency()),
                 table.chunks.size(), table.total_rows / 4096})
          : 1;
  auto materialize_chunk = [&](size_t ci) {
    const ColumnarChunk& chunk = table.chunks[ci];
    Row* dst = out_rows->data() + chunk_offsets[ci];
    for (size_t r = 0; r < chunk.rows; ++r) {
      chunk.MaterializeRowInto(r, &dst[r]);
    }
  };
  if (workers <= 1) {
    for (size_t ci = 0; ci < table.chunks.size(); ++ci) {
      materialize_chunk(ci);
    }
    return Status::kSuccess;
  }
  const Status st = RunParallelTasks(workers, [&](size_t worker) {
    const size_t lo = (table.chunks.size() * worker) / workers;
    const size_t hi = (table.chunks.size() * (worker + 1)) / workers;
    for (size_t ci = lo; ci < hi; ++ci) {
      materialize_chunk(ci);
    }
  });
  if (st != Status::kSuccess) {
    out_rows->clear();
    if (uniform_ok != nullptr) {
      *uniform_ok = false;
    }
    return st;
  }
  return Status::kSuccess;
}

std::shared_ptr<ColumnarTable> ProjectColumnarTableBySlots(
    const ColumnarTable& input, std::span<const slot_t> slots,
    bool* all_uniform_out) {
  auto out = std::make_shared<ColumnarTable>();
  out->keepalives = input.keepalives;
  bool all_uniform = input.total_rows > 0;
  for (const ColumnarChunk& src : input.chunks) {
    if (src.rows == 0) {
      continue;
    }
    ColumnarChunk dst;
    dst.rows = src.rows;
    dst.string_arena = src.string_arena;
    dst.keepalives = src.keepalives;
    dst.columns.resize(slots.size());
    for (size_t k = 0; k < slots.size(); ++k) {
      const slot_t col_idx = slots[k];
      if (col_idx < src.columns.size()) {
        dst.columns[k] = src.columns[col_idx];
      } else {
        dst.columns[k].SetConstantNull(ValueType::kNull, false, false,
                                       src.rows);
      }
      if (!dst.columns[k].is_uniform) {
        all_uniform = false;
      }
    }
    out->AppendChunk(std::move(dst));
  }
  if (all_uniform_out != nullptr) {
    *all_uniform_out = all_uniform && out->total_rows > 0;
  }
  return out;
}

StatusOr<std::shared_ptr<ColumnarTable>> BuildColumnarTableFromRows(
    const Schema& schema, std::span<const Row> rows) {
  auto table = std::make_shared<ColumnarTable>();
  if (rows.empty()) {
    return table;
  }
  const size_t total = rows.size();
  const size_t num_chunks =
      (total + kColumnarChunkRows - 1) / kColumnarChunkRows;
  table->chunks.resize(num_chunks);

  auto fill_chunk = [&](size_t ci) {
    const size_t lo = ci * kColumnarChunkRows;
    const size_t hi = std::min(total, lo + kColumnarChunkRows);
    ColumnarChunk& chunk = table->chunks[ci];
    chunk.InitFromSchema(schema, hi - lo);
    for (size_t r = lo; r < hi; ++r) {
      chunk.AppendRow(rows[r]);
    }
  };

  const size_t workers =
      total >= 8192
          ? std::min<size_t>(
                {QueryParallelWorkerLimit(std::thread::hardware_concurrency()),
                 num_chunks, total / 4096})
          : 1;
  if (workers <= 1) {
    for (size_t ci = 0; ci < num_chunks; ++ci) {
      fill_chunk(ci);
    }
    table->total_rows = total;
    return table;
  }
  RETURN_IF_FAIL(RunParallelTasks(workers, [&](size_t worker) {
    const size_t lo = (num_chunks * worker) / workers;
    const size_t hi = (num_chunks * (worker + 1)) / workers;
    for (size_t ci = lo; ci < hi; ++ci) {
      fill_chunk(ci);
    }
  }));
  table->total_rows = total;
  return table;
}

}  // namespace tinylamb::relational_detail
