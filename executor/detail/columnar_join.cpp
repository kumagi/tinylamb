/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/columnar_join.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/parallel_work.hpp"
#include "common/platform/int128_conversion.hpp"
#include "common/platform/process.hpp"
#include "common/query_parallelism.hpp"
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/planning_columnar_keys.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "expression/binary_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/jit.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

namespace {

constexpr uint32_t kColumnarEmptyBucket = UINT32_MAX;

class FlatStringViewMap {
 public:
  struct Slot {
    uint64_t hash{0};
    const char* ptr{nullptr};
    uint32_t len{0};
    uint32_t head{kColumnarEmptyBucket};
  };

  static uint64_t HashSv(std::string_view sv) noexcept {
    uint64_t h = static_cast<uint64_t>(std::hash<std::string_view>{}(sv));
    h ^= MixEstimateHash64(static_cast<uint64_t>(sv.size()));
    return MixEstimateHash64(h);
  }

  void Reserve(size_t expected) {
    const size_t min_cap = std::max<size_t>(16, ((expected * 10) / 7) + 2);
    size_t cap = 16;
    while (cap < min_cap) {
      cap <<= 1;
    }
    if (cap > slots_.size()) {
      Rehash(cap);
    }
  }

  [[nodiscard]] const uint32_t* Find(std::string_view sv) const noexcept {
    return FindWithHash(sv, HashSv(sv));
  }

  [[nodiscard]] const uint32_t* FindWithHash(std::string_view sv,
                                             uint64_t h) const noexcept {
    if (slots_.empty()) {
      return nullptr;
    }
    size_t pos = static_cast<size_t>(h) & mask_;
    while (true) {
      const Slot& s = slots_[pos];
      if (s.head == kColumnarEmptyBucket) {
        return nullptr;
      }
      if (s.hash == h && s.len == sv.size() &&
          (s.len == 0 || std::memcmp(s.ptr, sv.data(), s.len) == 0)) {
        return &s.head;
      }
      pos = (pos + 1) & mask_;
    }
  }

  uint32_t* TryEmplaceWithHash(std::string_view sv, uint64_t h,
                               bool* inserted) {
    if ((size_ + 1) * 10 > slots_.size() * 7) {
      Rehash(std::max<size_t>(16, slots_.size() * 2));
    }
    size_t pos = static_cast<size_t>(h) & mask_;
    while (true) {
      Slot& s = slots_[pos];
      if (s.head == kColumnarEmptyBucket) {
        s.hash = h;
        s.ptr = sv.data();
        s.len = static_cast<uint32_t>(sv.size());
        s.head = 0;
        ++size_;
        if (inserted != nullptr) {
          *inserted = true;
        }
        return &s.head;
      }
      if (s.hash == h && s.len == sv.size() &&
          (s.len == 0 || std::memcmp(s.ptr, sv.data(), s.len) == 0)) {
        if (inserted != nullptr) {
          *inserted = false;
        }
        return &s.head;
      }
      pos = (pos + 1) & mask_;
    }
  }

  uint32_t* TryEmplace(std::string_view sv, bool* inserted) {
    return TryEmplaceWithHash(sv, HashSv(sv), inserted);
  }

 private:
  void Rehash(size_t new_cap) {
    std::vector<Slot> next(new_cap);
    const size_t new_mask = new_cap - 1;
    for (const Slot& old : slots_) {
      if (old.head == kColumnarEmptyBucket) {
        continue;
      }
      size_t pos = static_cast<size_t>(old.hash) & new_mask;
      while (next[pos].head != kColumnarEmptyBucket) {
        pos = (pos + 1) & new_mask;
      }
      next[pos] = old;
    }
    slots_ = std::move(next);
    mask_ = new_mask;
  }

  std::vector<Slot> slots_;
  size_t mask_{0};
  size_t size_{0};
};

class FlatCompositeIntMap {
 public:
  struct Slot {
    uint64_t hash{0};
    CompositeIntKey key{};
    uint32_t head{kColumnarEmptyBucket};
  };

  static uint64_t HashKey(const CompositeIntKey& k) noexcept {
    uint64_t h = MixEstimateHash64(k.count);
    for (uint8_t i = 0; i < k.count; ++i) {
      h ^= MixEstimateHash64(static_cast<uint64_t>(k.parts[i])) +
           0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
    }
    return h;
  }

  void Reserve(size_t expected) {
    const size_t min_cap = std::max<size_t>(16, ((expected * 10) / 7) + 2);
    size_t cap = 16;
    while (cap < min_cap) {
      cap <<= 1;
    }
    if (cap > slots_.size()) {
      Rehash(cap);
    }
  }

  [[nodiscard]] const uint32_t* Find(const CompositeIntKey& k) const noexcept {
    return FindWithHash(k, HashKey(k));
  }

  [[nodiscard]] const uint32_t* FindWithHash(const CompositeIntKey& k,
                                             uint64_t h) const noexcept {
    if (slots_.empty()) {
      return nullptr;
    }
    size_t pos = static_cast<size_t>(h) & mask_;
    while (true) {
      const Slot& s = slots_[pos];
      if (s.head == kColumnarEmptyBucket) {
        return nullptr;
      }
      if (s.hash == h && s.key == k) {
        return &s.head;
      }
      pos = (pos + 1) & mask_;
    }
  }

  uint32_t* TryEmplace(const CompositeIntKey& k, bool* inserted) {
    return TryEmplaceWithHash(k, HashKey(k), inserted);
  }

  uint32_t* TryEmplaceWithHash(const CompositeIntKey& k, uint64_t h,
                               bool* inserted) {
    if ((size_ + 1) * 10 > slots_.size() * 7) {
      Rehash(std::max<size_t>(16, slots_.size() * 2));
    }
    size_t pos = static_cast<size_t>(h) & mask_;
    while (true) {
      Slot& s = slots_[pos];
      if (s.head == kColumnarEmptyBucket) {
        s.hash = h;
        s.key = k;
        s.head = 0;
        ++size_;
        if (inserted != nullptr) {
          *inserted = true;
        }
        return &s.head;
      }
      if (s.hash == h && s.key == k) {
        if (inserted != nullptr) {
          *inserted = false;
        }
        return &s.head;
      }
      pos = (pos + 1) & mask_;
    }
  }

 private:
  void Rehash(size_t new_cap) {
    std::vector<Slot> next(new_cap);
    const size_t new_mask = new_cap - 1;
    for (const Slot& old : slots_) {
      if (old.head == kColumnarEmptyBucket) {
        continue;
      }
      size_t pos = static_cast<size_t>(old.hash) & new_mask;
      while (next[pos].head != kColumnarEmptyBucket) {
        pos = (pos + 1) & new_mask;
      }
      next[pos] = old;
    }
    slots_ = std::move(next);
    mask_ = new_mask;
  }

  std::vector<Slot> slots_;
  size_t mask_{0};
  size_t size_{0};
};

struct ColumnarColSummary {
  std::shared_ptr<const ColumnarTable> leaf_owner;
  size_t leaf_col{0};
  bool same_leaf_target{true};
  bool all_uniform{true};
  bool any_null{false};
  bool all_null{true};
  ValueType common_type{ValueType::kNull};
  bool common_unsigned{false};
  bool common_interval{false};
};

std::vector<ColumnarColSummary> SummarizeColumnarTable(
    const std::shared_ptr<const ColumnarTable>& tbl, const Schema& schema) {
  const size_t ncols = schema.ColumnCount();
  std::vector<ColumnarColSummary> out(ncols);
  for (size_t c = 0; c < ncols; ++c) {
    out[c].leaf_owner = tbl;
    out[c].leaf_col = c;
  }
  if (tbl == nullptr || tbl->chunks.empty()) {
    return out;
  }
  std::vector<bool> target_init(ncols, false);
  std::vector<bool> type_init(ncols, false);
  for (const ColumnarChunk& ch : tbl->chunks) {
    if (ch.rows == 0) {
      continue;
    }
    // T4593: projected ColumnarTables carry chunks narrower than the schema
    // (columnar_table.cpp resize(slots.size())); index only columns this
    // chunk actually provides, matching FinalizeColumnarJoinResult's guard.
    for (size_t c = 0; c < ncols && c < ch.columns.size(); ++c) {
      ColumnarColSummary& sum = out[c];
      const ColumnarColumn& col = ch.columns[c];
      std::shared_ptr<const ColumnarTable> cur_owner = tbl;
      size_t cur_col = c;
      if (col.encoding == ColumnarColumn::Encoding::kGathered &&
          col.gather_index != nullptr &&
          col.gather_index->source_table != nullptr) {
        cur_owner = col.gather_index->source_table;
        cur_col = col.gather_col;
      }
      if (!target_init[c]) {
        sum.leaf_owner = std::move(cur_owner);
        sum.leaf_col = cur_col;
        target_init[c] = true;
      } else if (sum.same_leaf_target &&
                 (sum.leaf_owner.get() != cur_owner.get() ||
                  sum.leaf_col != cur_col)) {
        sum.same_leaf_target = false;
        sum.leaf_owner = tbl;
        sum.leaf_col = c;
      }
      if (col.has_null || col.type == ValueType::kNull ||
          (col.encoding == ColumnarColumn::Encoding::kConstant &&
           col.const_is_null)) {
        sum.any_null = true;
      }
      if (!col.is_uniform) {
        sum.all_uniform = false;
        sum.all_null = false;
        continue;
      }
      if (col.type != ValueType::kNull) {
        sum.all_null = false;
        if (!type_init[c]) {
          sum.common_type = col.type;
          sum.common_unsigned = col.is_unsigned;
          sum.common_interval = col.is_interval;
          type_init[c] = true;
        } else if (sum.common_type != col.type ||
                   sum.common_unsigned != col.is_unsigned ||
                   sum.common_interval != col.is_interval) {
          sum.all_uniform = false;
        }
      }
    }
  }
  return out;
}

std::vector<size_t> ComputeColumnarBuildGroups(
    const ColumnarTable& tbl, const std::vector<ColumnarColSummary>& plan) {
  const size_t ncols = plan.size();
  std::vector<size_t> group_of(ncols, 0);
  std::vector<size_t> repr_cols;
  repr_cols.reserve(ncols);
  for (size_t c = 0; c < ncols; ++c) {
    if (!plan[c].same_leaf_target) {
      group_of[c] = repr_cols.size();
      repr_cols.push_back(c);
      continue;
    }
    size_t found = repr_cols.size();
    for (size_t g = 0; g < repr_cols.size(); ++g) {
      const size_t rc = repr_cols[g];
      if (!plan[rc].same_leaf_target ||
          plan[rc].leaf_owner.get() != plan[c].leaf_owner.get()) {
        continue;
      }
      bool same_gi = true;
      for (const ColumnarChunk& ch : tbl.chunks) {
        if (ch.rows == 0) {
          continue;
        }
        // T4593: narrower projected chunks simply carry no evidence for
        // these columns; skip them instead of indexing out of range.
        if (rc >= ch.columns.size() || c >= ch.columns.size()) {
          continue;
        }
        if (ch.columns[rc].gather_index.get() !=
            ch.columns[c].gather_index.get()) {
          same_gi = false;
          break;
        }
      }
      if (same_gi) {
        found = g;
        break;
      }
    }
    if (found == repr_cols.size()) {
      repr_cols.push_back(c);
    }
    group_of[c] = found;
  }
  return group_of;
}

Relation FinalizeColumnarJoinResult(
    TransactionContext& context, const Schema& schema,
    std::shared_ptr<const std::vector<std::string>> using_columns,
    const Relation& stats_seed, uint64_t comparisons,
    std::shared_ptr<ColumnarTable> table) {
  Relation result(context.execution_runtime());
  result.schema = schema;
  result.using_columns = std::move(using_columns);
  result.hash_joins = stats_seed.hash_joins;
  result.hybrid_hash_joins = stats_seed.hybrid_hash_joins;
  result.in_memory_hash_joins = stats_seed.in_memory_hash_joins;
  result.nested_loop_joins = stats_seed.nested_loop_joins;
  result.join_comparisons = stats_seed.join_comparisons + comparisons;
  const size_t out_rows = table != nullptr ? table->total_rows : 0;
  result.peak_intermediate_rows =
      std::max(stats_seed.peak_intermediate_rows, out_rows);
  bool all_uniform = true;
  if (table != nullptr) {
    for (const ColumnarChunk& ch : table->chunks) {
      if (ch.rows == 0) {
        continue;
      }
      for (size_t c = 0; c < ch.columns.size() && c < schema.ColumnCount();
           ++c) {
        const ColumnarColumn& col = ch.columns[c];
        const Column& scol = schema.GetColumn(c);
        if (!col.is_uniform ||
            (col.type != ValueType::kNull &&
             (col.type != scol.Type() || col.is_unsigned != scol.IsUnsigned() ||
              col.is_interval))) {
          all_uniform = false;
          break;
        }
      }
      if (!all_uniform) {
        break;
      }
    }
    result.SetColumnarTable(std::move(table), all_uniform);
  } else {
    result.SetColumnarTable(std::make_shared<ColumnarTable>(), true);
  }
  return result;
}

void AppendSideSingleRowToColumnarChunk(const ColumnarChunk& single_src,
                                        size_t row_count, const Schema& schema,
                                        ColumnarChunk* dst) {
  const size_t ncols = schema.ColumnCount();
  Value v;
  for (size_t c = 0; c < ncols; ++c) {
    const ColumnarColumn& src = single_src.columns[c];
    src.WriteValueInto(0, &v);
    ColumnarColumn out;
    const bool scalar_ok =
        !v.IsNull() && !v.IsCaseInsensitive() && v.type != ValueType::kArray &&
        (v.type == ValueType::kVarChar
             ? (v.Collation() == 0 || v.IsInterval())
             : (v.Collation() == 0 ||
                (v.type == ValueType::kInt64 && v.IsUnsigned())));
    if (v.IsNull()) {
      out.SetConstantNull(schema.GetColumn(c).Type(), false, false, row_count);
    } else if (scalar_ok) {
      out.Init(v.type, v.IsUnsigned(), v.IsInterval(), 0);
      out.encoding = ColumnarColumn::Encoding::kConstant;
      out.const_is_null = false;
      out.const_rows = row_count;
      if (v.type == ValueType::kInt64 || v.type == ValueType::kDate) {
        out.const_int = v.value.int_value;
      } else if (v.type == ValueType::kDouble) {
        out.const_double = v.value.double_value;
      } else if (v.type == ValueType::kVarChar) {
        out.const_string = dst->string_arena->Store(v.value.varchar_value);
      }
    } else {
      out.Init(ValueType::kNull, false, false, row_count);
      out.DemoteToFallback(row_count);
      out.fallback.assign(row_count, v);
      for (Value& cell : out.fallback) {
        if (cell.type == ValueType::kVarChar &&
            !cell.value.varchar_value.empty()) {
          cell.value.varchar_value =
              dst->string_arena->Store(cell.value.varchar_value);
          cell.owned_data.clear();
        }
      }
    }
    dst->columns.push_back(std::move(out));
  }
}

void AppendNullColumnsToChunk(size_t row_count, const Schema& schema,
                              ColumnarChunk* dst) {
  for (size_t c = 0; c < schema.ColumnCount(); ++c) {
    ColumnarColumn col;
    col.SetConstantNull(schema.GetColumn(c).Type(), false, false, row_count);
    dst->columns.push_back(std::move(col));
  }
}

void AppendAliasColumnsToChunk(const ColumnarChunk& src, ColumnarChunk* dst) {
  if (src.string_arena != nullptr) {
    dst->keepalives.push_back(src.string_arena);
  }
  for (const auto& ka : src.keepalives) {
    dst->keepalives.push_back(ka);
  }
  for (const ColumnarColumn& c : src.columns) {
    dst->columns.push_back(c);
  }
}

}  // namespace

StatusOr<std::optional<Relation>> TryColumnarCrossOrNullJoin(
    TransactionContext& context, Relation& left, Relation& right,
    bool want_left_nulls, bool want_right_nulls, const Schema& result_schema,
    const std::vector<Expression>& residual,
    std::shared_ptr<const std::vector<std::string>> using_columns,
    const Relation& stats_seed) {
  if (left.HasSpill() || right.HasSpill() ||
      (!left.HasColumnarOnly() && !right.HasColumnarOnly()) ||
      !residual.empty()) {
    return std::nullopt;
  }
  const size_t nl = left.TotalRows();
  const size_t nr = right.TotalRows();
  if (nl > 1 && nr > 1) {
    return std::nullopt;
  }
  if ((nl == 0 && !want_right_nulls) || (nr == 0 && !want_left_nulls) ||
      (nl == 0 && nr == 0)) {
    return FinalizeColumnarJoinResult(context, result_schema,
                                      std::move(using_columns), stats_seed, 0,
                                      std::make_shared<ColumnarTable>());
  }
  auto to_col =
      [](Relation& rel) -> StatusOr<std::shared_ptr<const ColumnarTable>> {
    if (rel.HasColumnarOnly()) {
      return rel.columnar;
    }
    ASSIGN_OR_RETURN(std::shared_ptr<ColumnarTable>, built,
                     BuildColumnarTableFromRows(rel.schema, rel.rows));
    return std::shared_ptr<const ColumnarTable>(std::move(built));
  };
  auto out = std::make_shared<ColumnarTable>();
  if (nl == 0) {
    ASSIGN_OR_RETURN(std::shared_ptr<const ColumnarTable>, rtbl, to_col(right));
    for (const ColumnarChunk& rc : rtbl->chunks) {
      if (rc.rows == 0) {
        continue;
      }
      ColumnarChunk dst;
      dst.rows = rc.rows;
      dst.string_arena = std::make_shared<ChunkStringArena>();
      AppendNullColumnsToChunk(rc.rows, left.schema, &dst);
      AppendAliasColumnsToChunk(rc, &dst);
      out->total_rows += dst.rows;
      out->chunks.push_back(std::move(dst));
    }
    return FinalizeColumnarJoinResult(context, result_schema,
                                      std::move(using_columns), stats_seed, 0,
                                      std::move(out));
  }
  if (nr == 0) {
    ASSIGN_OR_RETURN(std::shared_ptr<const ColumnarTable>, ltbl, to_col(left));
    for (const ColumnarChunk& lc : ltbl->chunks) {
      if (lc.rows == 0) {
        continue;
      }
      ColumnarChunk dst;
      dst.rows = lc.rows;
      dst.string_arena = std::make_shared<ChunkStringArena>();
      AppendAliasColumnsToChunk(lc, &dst);
      AppendNullColumnsToChunk(lc.rows, right.schema, &dst);
      out->total_rows += dst.rows;
      out->chunks.push_back(std::move(dst));
    }
    return FinalizeColumnarJoinResult(context, result_schema,
                                      std::move(using_columns), stats_seed, 0,
                                      std::move(out));
  }
  ASSIGN_OR_RETURN(std::shared_ptr<const ColumnarTable>, ltbl, to_col(left));
  ASSIGN_OR_RETURN(std::shared_ptr<const ColumnarTable>, rtbl, to_col(right));
  const auto find_first_nonempty =
      [](const ColumnarTable& tbl) -> const ColumnarChunk* {
    for (const ColumnarChunk& ch : tbl.chunks) {
      if (ch.rows > 0) {
        return &ch;
      }
    }
    return nullptr;
  };
  if (nl == 1) {
    const ColumnarChunk* single = find_first_nonempty(*ltbl);
    if (single == nullptr) {
      return std::nullopt;
    }
    for (const ColumnarChunk& rc : rtbl->chunks) {
      if (rc.rows == 0) {
        continue;
      }
      ColumnarChunk dst;
      dst.rows = rc.rows;
      dst.string_arena = std::make_shared<ChunkStringArena>();
      AppendSideSingleRowToColumnarChunk(*single, rc.rows, left.schema, &dst);
      AppendAliasColumnsToChunk(rc, &dst);
      out->total_rows += dst.rows;
      out->chunks.push_back(std::move(dst));
    }
    return FinalizeColumnarJoinResult(context, result_schema,
                                      std::move(using_columns), stats_seed, nr,
                                      std::move(out));
  }
  const ColumnarChunk* single = find_first_nonempty(*rtbl);
  if (single == nullptr) {
    return std::nullopt;
  }
  for (const ColumnarChunk& lc : ltbl->chunks) {
    if (lc.rows == 0) {
      continue;
    }
    ColumnarChunk dst;
    dst.rows = lc.rows;
    dst.string_arena = std::make_shared<ChunkStringArena>();
    AppendAliasColumnsToChunk(lc, &dst);
    AppendSideSingleRowToColumnarChunk(*single, lc.rows, right.schema, &dst);
    out->total_rows += dst.rows;
    out->chunks.push_back(std::move(dst));
  }
  return FinalizeColumnarJoinResult(context, result_schema,
                                    std::move(using_columns), stats_seed, nl,
                                    std::move(out));
}

struct SrcIntLeafCache {
  enum Kind : uint8_t {
    kOther = 0,
    kAllNull,
    kConst,
    kFlat,
    kFoR0,
    kFoR1,
    kFoR2,
    kFoR4,
    kFoR8,
  };
  struct ChunkDesc {
    Kind kind{kOther};
    int64_t base{0};
    const int64_t* ints{nullptr};
    const uint8_t* for_bytes{nullptr};
    const uint8_t* nulls{nullptr};
  };
  const ColumnarTable* tbl{nullptr};
  size_t col{0};
  bool any_null{false};
  bool any_other{false};
  std::vector<ChunkDesc> chunks;

  void Ensure(const ColumnarTable* src_tbl, size_t src_col) {
    if (tbl == src_tbl && col == src_col) {
      return;
    }
    tbl = src_tbl;
    col = src_col;
    any_null = false;
    any_other = false;
    chunks.resize(src_tbl->chunks.size());
    for (size_t c = 0; c < src_tbl->chunks.size(); ++c) {
      ChunkDesc& d = chunks[c];
      d = ChunkDesc{};
      const auto& cols = src_tbl->chunks[c].columns;
      if (src_col >= cols.size()) {
        d.kind = kOther;
        any_other = true;
        any_null = true;
        continue;
      }
      const ColumnarColumn& sc = cols[src_col];
      if (!sc.is_uniform || sc.is_unsigned || sc.is_interval ||
          (sc.type != ValueType::kInt64 && sc.type != ValueType::kDate &&
           sc.type != ValueType::kNull)) {
        d.kind = kOther;
        any_other = true;
        any_null = true;
        continue;
      }
      if (sc.type == ValueType::kNull ||
          (sc.encoding == ColumnarColumn::Encoding::kConstant &&
           sc.const_is_null)) {
        d.kind = kAllNull;
        any_null = true;
        continue;
      }
      switch (sc.encoding) {
        case ColumnarColumn::Encoding::kConstant:
          d.kind = kConst;
          d.base = sc.const_int;
          break;
        case ColumnarColumn::Encoding::kFlat:
          d.kind = kFlat;
          d.ints = sc.ints.data();
          if (sc.has_null && !sc.nulls.empty()) {
            d.nulls = sc.nulls.data();
            any_null = true;
          }
          break;
        case ColumnarColumn::Encoding::kFrameOfReference:
          switch (sc.for_width) {
            case 0:
              d.kind = kFoR0;
              break;
            case 1:
              d.kind = kFoR1;
              break;
            case 2:
              d.kind = kFoR2;
              break;
            case 4:
              d.kind = kFoR4;
              break;
            case 8:
              d.kind = kFoR8;
              break;
            default:
              d.kind = kOther;
              any_other = true;
              any_null = true;
              continue;
          }
          d.base = sc.for_base;
          d.for_bytes = sc.for_bytes;
          if (sc.has_null && sc.borrowed_nulls != nullptr) {
            d.nulls = sc.borrowed_nulls;
            any_null = true;
          }
          break;
        default:
          d.kind = kOther;
          any_other = true;
          any_null = true;
          break;
      }
    }
  }
};

namespace {

inline uint64_t DecodeIntTile64(const ColumnarColumn& col, uint32_t r0,
                                uint32_t cnt, SrcIntLeafCache* leaf_cache,
                                int64_t* __restrict vals) noexcept {
  const uint64_t full_mask =
      cnt == 64 ? ~uint64_t{0} : ((uint64_t{1} << cnt) - 1);
  if (!col.is_uniform || col.is_unsigned || col.is_interval) {
    uint64_t mask = 0;
    for (uint32_t i = 0; i < cnt; ++i) {
      if (col.TryReadNonNullInt(r0 + i, &vals[i])) {
        mask |= (uint64_t{1} << i);
      }
    }
    return mask;
  }
  if (col.type == ValueType::kNull) {
    return 0;
  }
  auto pack_nulls = [](const uint8_t* __restrict np, uint32_t cnt_n,
                       uint64_t fmask) noexcept -> uint64_t {
    if (np == nullptr) {
      return fmask;
    }
    uint64_t mask = 0;
    for (uint32_t i = 0; i < cnt_n; ++i) {
      mask |= static_cast<uint64_t>(np[i] == 0) << i;
    }
    return mask;
  };
  if (col.encoding == ColumnarColumn::Encoding::kFlat) {
    std::memcpy(vals, col.ints.data() + r0, static_cast<size_t>(cnt) * 8);
    const uint8_t* np = (col.has_null && !col.nulls.empty())
                            ? (col.nulls.data() + r0)
                            : nullptr;
    return pack_nulls(np, cnt, full_mask);
  }
  if (col.encoding == ColumnarColumn::Encoding::kFrameOfReference) {
    const auto base_u = static_cast<uint64_t>(col.for_base);
    const uint8_t* __restrict fb = col.for_bytes;
    bool ok_width = true;
    switch (col.for_width) {
      case 0:
        for (uint32_t i = 0; i < cnt; ++i) {
          vals[i] = col.for_base;
        }
        break;
      case 1: {
        const uint8_t* __restrict p = fb + r0;
        for (uint32_t i = 0; i < cnt; ++i) {
          vals[i] = static_cast<int64_t>(base_u + p[i]);
        }
        break;
      }
      case 2: {
        const uint8_t* __restrict p = fb + (static_cast<size_t>(r0) * 2);
        for (uint32_t i = 0; i < cnt; ++i) {
          uint16_t d = 0;
          std::memcpy(&d, p + (static_cast<size_t>(i) * 2), sizeof(d));
          vals[i] = static_cast<int64_t>(base_u + d);
        }
        break;
      }
      case 4: {
        const uint8_t* __restrict p = fb + (static_cast<size_t>(r0) * 4);
        for (uint32_t i = 0; i < cnt; ++i) {
          uint32_t d = 0;
          std::memcpy(&d, p + (static_cast<size_t>(i) * 4), sizeof(d));
          vals[i] = static_cast<int64_t>(base_u + d);
        }
        break;
      }
      case 8: {
        const uint8_t* __restrict p = fb + (static_cast<size_t>(r0) * 8);
        for (uint32_t i = 0; i < cnt; ++i) {
          uint64_t d = 0;
          std::memcpy(&d, p + (static_cast<size_t>(i) * 8), sizeof(d));
          vals[i] = static_cast<int64_t>(base_u + d);
        }
        break;
      }
      default:
        ok_width = false;
        break;
    }
    if (ok_width) {
      const uint8_t* np = (col.has_null && col.borrowed_nulls != nullptr)
                              ? (col.borrowed_nulls + r0)
                              : nullptr;
      return pack_nulls(np, cnt, full_mask);
    }
  }
  if (col.encoding == ColumnarColumn::Encoding::kGathered &&
      leaf_cache != nullptr && col.gather_index != nullptr &&
      col.gather_index->source_table != nullptr) {
    const ChunkGatherIndex* gi = col.gather_index.get();
    leaf_cache->Ensure(gi->source_table.get(), col.gather_col);
    const SrcIntLeafCache::ChunkDesc* __restrict descs =
        leaf_cache->chunks.data();
    const uint32_t* __restrict scs = gi->src_chunks.data() + r0;
    const uint32_t* __restrict srs = gi->src_rows.data() + r0;
    auto read_desc = [](const SrcIntLeafCache::ChunkDesc& d,
                        uint32_t sr) noexcept -> int64_t {
      switch (d.kind) {
        case SrcIntLeafCache::kConst:
        case SrcIntLeafCache::kFoR0:
          return d.base;
        case SrcIntLeafCache::kFlat:
          return d.ints[sr];
        case SrcIntLeafCache::kFoR1:
          return static_cast<int64_t>(static_cast<uint64_t>(d.base) +
                                      d.for_bytes[sr]);
        case SrcIntLeafCache::kFoR2: {
          uint16_t v = 0;
          std::memcpy(&v, d.for_bytes + (static_cast<size_t>(sr) * sizeof(v)),
                      sizeof(v));
          return static_cast<int64_t>(static_cast<uint64_t>(d.base) + v);
        }
        case SrcIntLeafCache::kFoR4: {
          uint32_t v = 0;
          std::memcpy(&v, d.for_bytes + (static_cast<size_t>(sr) * sizeof(v)),
                      sizeof(v));
          return static_cast<int64_t>(static_cast<uint64_t>(d.base) + v);
        }
        case SrcIntLeafCache::kFoR8: {
          uint64_t v = 0;
          std::memcpy(&v, d.for_bytes + (static_cast<size_t>(sr) * sizeof(v)),
                      sizeof(v));
          return static_cast<int64_t>(static_cast<uint64_t>(d.base) + v);
        }
        default:
          return 0;
      }
    };
    if (!gi->has_null && !leaf_cache->any_null && !leaf_cache->any_other) {
      for (uint32_t i = 0; i < cnt; ++i) {
        vals[i] = read_desc(descs[scs[i]], srs[i]);
      }
      return full_mask;
    }
    uint64_t mask = 0;
    for (uint32_t i = 0; i < cnt; ++i) {
      const uint32_t sr = srs[i];
      if (sr == ChunkGatherIndex::kNullRow) {
        continue;
      }
      const SrcIntLeafCache::ChunkDesc& d = descs[scs[i]];
      if (d.kind == SrcIntLeafCache::kAllNull) {
        continue;
      }
      if (d.kind == SrcIntLeafCache::kOther) {
        if (col.TryReadNonNullInt(r0 + i, &vals[i])) {
          mask |= (uint64_t{1} << i);
        }
        continue;
      }
      if (d.nulls != nullptr && d.nulls[sr] != 0) {
        continue;
      }
      vals[i] = read_desc(d, sr);
      mask |= (uint64_t{1} << i);
    }
    return mask;
  }
  uint64_t mask = 0;
  for (uint32_t i = 0; i < cnt; ++i) {
    if (col.TryReadNonNullInt(r0 + i, &vals[i])) {
      mask |= (uint64_t{1} << i);
    }
  }
  return mask;
}

bool ColumnIntBiasFitsNoOverflow(const ColumnarTable& tbl, slot_t col_idx,
                                 int64_t bias) noexcept {
  if (bias == 0) {
    return true;
  }
  const platform::Int128 kMin64 = std::numeric_limits<int64_t>::min();
  const platform::Int128 kMax64 = std::numeric_limits<int64_t>::max();
  const platform::Int128 lo128 = kMin64 - static_cast<platform::Int128>(bias);
  const platform::Int128 hi128 = kMax64 - static_cast<platform::Int128>(bias);
  const int64_t lo = lo128 < kMin64 ? std::numeric_limits<int64_t>::min()
                                    : static_cast<int64_t>(lo128);
  const int64_t hi = hi128 > kMax64 ? std::numeric_limits<int64_t>::max()
                                    : static_cast<int64_t>(hi128);
  for (const ColumnarChunk& ch : tbl.chunks) {
    if (ch.rows == 0) {
      continue;
    }
    const ColumnarColumn& c = ch.columns[col_idx];
    if (!c.is_uniform || c.is_unsigned || c.is_interval) {
      return false;
    }
    if (c.type == ValueType::kNull ||
        (c.encoding == ColumnarColumn::Encoding::kConstant &&
         c.const_is_null)) {
      continue;
    }
    if (c.type != ValueType::kInt64) {
      return false;
    }
    if (c.encoding == ColumnarColumn::Encoding::kConstant) {
      if (c.const_int < lo || c.const_int > hi) {
        return false;
      }
      continue;
    }
    if (c.encoding == ColumnarColumn::Encoding::kFlat) {
      if (!c.has_null) {
        const int64_t* vals = c.ints.data();
        bool bad = false;
        for (size_t r = 0; r < ch.rows; ++r) {
          bad |= (vals[r] < lo || vals[r] > hi);
        }
        if (bad) {
          return false;
        }
      } else {
        for (size_t r = 0; r < ch.rows; ++r) {
          if (c.IsNull(r)) {
            continue;
          }
          const int64_t v = c.ints[r];
          if (v < lo || v > hi) {
            return false;
          }
        }
      }
      continue;
    }
    if (c.encoding == ColumnarColumn::Encoding::kFrameOfReference &&
        !c.has_null && c.for_width <= 4) {
      const platform::Int128 max_delta =
          c.for_width == 0 ? 0
                           : ((platform::Int128{1}
                               << (static_cast<unsigned>(c.for_width) * 8U)) -
                              1);
      const auto min_val = static_cast<platform::Int128>(c.for_base);
      const platform::Int128 max_val = min_val + max_delta;
      if (min_val >= lo && max_val <= hi) {
        continue;
      }
    }
    for (size_t r = 0; r < ch.rows; ++r) {
      if (c.IsNull(r)) {
        continue;
      }
      const int64_t v = c.IntAt(r);
      if (v < lo || v > hi) {
        return false;
      }
    }
  }
  return true;
}

bool CanElideJoinResidualWithKeys(
    const Expression& predicate, const Schema& result_schema, size_t left_width,
    bool flip, const ColumnarTable& build_tbl, const ColumnarTable& probe_tbl,
    const std::vector<slot_t>& left_columns,
    const std::vector<slot_t>& right_columns,
    const std::vector<int64_t>& left_key_offsets,
    const std::vector<int64_t>& right_key_offsets,
    const std::vector<slot_t>& build_cols,
    const std::vector<slot_t>& probe_cols,
    const std::vector<int64_t>& build_biases,
    const std::vector<int64_t>& probe_biases,
    const std::vector<ColumnarColSummary>& build_plan,
    const std::vector<ColumnarColSummary>& probe_plan,
    std::vector<int8_t>* key_bias_safe) {
  if (!predicate || predicate->Type() != TypeTag::kBinaryExp) {
    return false;
  }
  const BinaryExpression& binary = predicate->AsBinaryExpression();
  if (binary.Op() != BinaryOperation::kEquals) {
    return false;
  }
  ColumnName l_name;
  ColumnName r_name;
  int64_t l_bias = 0;
  int64_t r_bias = 0;
  if (!ColumnWithConstantBias(binary.Left(), &l_name, &l_bias) ||
      !ColumnWithConstantBias(binary.Right(), &r_name, &r_bias)) {
    return false;
  }
  const int loff = result_schema.Offset(l_name);
  const int roff = result_schema.Offset(r_name);
  if (loff < 0 || roff < 0) {
    return false;
  }
  const bool bare_cols = binary.Left()->Type() == TypeTag::kColumnValue &&
                         binary.Right()->Type() == TypeTag::kColumnValue;
  const ColumnarTable& left_tbl = flip ? build_tbl : probe_tbl;
  const ColumnarTable& right_tbl = flip ? probe_tbl : build_tbl;
  for (size_t k = 0; k < left_columns.size(); ++k) {
    const int left_slot = static_cast<int>(left_columns[k]);
    const int right_slot = static_cast<int>(left_width + right_columns[k]);
    bool same_order = false;
    if (loff == left_slot && roff == right_slot) {
      same_order = true;
    } else if (loff == right_slot && roff == left_slot) {
      same_order = false;
    } else {
      continue;
    }
    const int64_t pred_left_bias = same_order ? l_bias : r_bias;
    const int64_t pred_right_bias = same_order ? r_bias : l_bias;
    if (static_cast<platform::Int128>(pred_left_bias) - pred_right_bias !=
        static_cast<platform::Int128>(left_key_offsets[k]) -
            right_key_offsets[k]) {
      continue;
    }
    const ColumnarColSummary& bp = build_plan[build_cols[k]];
    const ColumnarColSummary& pp = probe_plan[probe_cols[k]];
    if (bp.all_null || pp.all_null || !bp.all_uniform || !pp.all_uniform ||
        bp.common_unsigned || pp.common_unsigned || bp.common_interval ||
        pp.common_interval || bp.common_type != pp.common_type) {
      continue;
    }
    const ValueType vt = bp.common_type;
    if (vt == ValueType::kVarChar || vt == ValueType::kDate) {
      if (!bare_cols || build_biases[k] != 0 || probe_biases[k] != 0) {
        continue;
      }
      return true;
    }
    if (vt == ValueType::kInt64) {
      if ((*key_bias_safe)[k] < 0) {
        const bool ok = ColumnIntBiasFitsNoOverflow(build_tbl, build_cols[k],
                                                    build_biases[k]) &&
                        ColumnIntBiasFitsNoOverflow(probe_tbl, probe_cols[k],
                                                    probe_biases[k]);
        (*key_bias_safe)[k] = ok ? 1 : 0;
      }
      if ((*key_bias_safe)[k] == 1 &&
          ColumnIntBiasFitsNoOverflow(left_tbl, left_columns[k],
                                      pred_left_bias) &&
          ColumnIntBiasFitsNoOverflow(right_tbl, right_columns[k],
                                      pred_right_bias)) {
        return true;
      }
    }
  }
  return false;
}

}  // namespace

StatusOr<std::optional<Relation>> TryColumnarHashJoin(
    TransactionContext& context, Relation& left, Relation& right, bool flip,
    bool want_left_nulls, bool want_right_nulls, bool has_null_safe_key,
    bool has_using_residuals, const std::vector<slot_t>& left_columns,
    const std::vector<slot_t>& right_columns,
    const std::vector<int64_t>& left_key_offsets,
    const std::vector<int64_t>& right_key_offsets,
    const std::vector<Expression>& residual,
    const std::optional<std::vector<BytecodeProgram>>& compiled_residuals,
    std::shared_ptr<const std::vector<std::string>> using_columns,
    const Relation& stats_seed) {
  if (left.HasSpill() || right.HasSpill() || has_null_safe_key ||
      has_using_residuals ||
      (!left.HasColumnarOnly() && !right.HasColumnarOnly()) ||
      (!residual.empty() && !compiled_residuals.has_value())) {
    return std::optional<Relation>{std::nullopt};
  }
  const Schema result_schema = left.schema + right.schema;
  const size_t nl = left.TotalRows();
  const size_t nr = right.TotalRows();
  if ((nl == 0 && !want_right_nulls) || (nr == 0 && !want_left_nulls) ||
      (nl == 0 && nr == 0)) {
    return std::optional<Relation>{FinalizeColumnarJoinResult(
        context, result_schema, std::move(using_columns), stats_seed, 0,
        std::make_shared<ColumnarTable>())};
  }
  // Re-encoding a row-based side into ColumnarTable duplicates it for the
  // duration of the build. Converting the larger relation costs more than
  // materializing the smaller columnar side to rows on the ordinary join
  // path, so only convert a side that is not the bigger input.
  const bool left_row_based = !left.HasColumnarOnly();
  const bool right_row_based = !right.HasColumnarOnly();
  if ((left_row_based && !right_row_based && nl > nr) ||
      (!left_row_based && right_row_based && nr > nl)) {
    return std::optional<Relation>{std::nullopt};
  }
  auto to_col =
      [](Relation& rel) -> StatusOr<std::shared_ptr<const ColumnarTable>> {
    if (rel.HasColumnarOnly()) {
      return rel.columnar;
    }
    ASSIGN_OR_RETURN(std::shared_ptr<ColumnarTable>, built,
                     BuildColumnarTableFromRows(rel.schema, rel.rows));
    // The columnar copy fully replaces the buffered rows; drop them so the
    // input is not held twice for the rest of the join.
    rel.ResetContents();
    return std::shared_ptr<const ColumnarTable>(std::move(built));
  };
  ASSIGN_OR_RETURN(std::shared_ptr<const ColumnarTable>, left_tbl,
                   to_col(left));
  ASSIGN_OR_RETURN(std::shared_ptr<const ColumnarTable>, right_tbl,
                   to_col(right));

  const bool probe_outer_emit = flip ? want_right_nulls : want_left_nulls;
  const bool want_build_nulls = flip ? want_left_nulls : want_right_nulls;
  const auto& build_tbl = flip ? left_tbl : right_tbl;
  const auto& probe_tbl = flip ? right_tbl : left_tbl;
  const Schema& build_schema = flip ? left.schema : right.schema;
  const Schema& probe_schema = flip ? right.schema : left.schema;
  const std::vector<slot_t>& build_cols = flip ? left_columns : right_columns;
  const std::vector<slot_t>& probe_cols = flip ? right_columns : left_columns;
  const std::vector<int64_t>& build_biases =
      flip ? left_key_offsets : right_key_offsets;
  const std::vector<int64_t>& probe_biases =
      flip ? right_key_offsets : left_key_offsets;

  std::vector<ColumnarColSummary> left_plan =
      SummarizeColumnarTable(left_tbl, left.schema);
  std::vector<ColumnarColSummary> right_plan =
      SummarizeColumnarTable(right_tbl, right.schema);
  const auto& build_plan = flip ? left_plan : right_plan;
  const auto& probe_plan = flip ? right_plan : left_plan;
  const std::vector<size_t> build_groups =
      ComputeColumnarBuildGroups(*build_tbl, build_plan);
  const size_t num_build_groups =
      build_groups.empty() ? 0 : (*std::ranges::max_element(build_groups) + 1);
  std::vector<size_t> build_group_repr(num_build_groups, 0);
  for (size_t c = build_groups.size(); c-- > 0;) {
    build_group_repr[build_groups[c]] = c;
  }

  const size_t n_build = build_tbl->total_rows;
  const size_t n_probe = probe_tbl->total_rows;
  if (n_build == 0 || n_probe == 0) {
    auto out = std::make_shared<ColumnarTable>();
    const auto& keep_tbl = n_build == 0 ? probe_tbl : build_tbl;
    const bool keep_left = (n_build == 0) ? !flip : flip;
    const Schema& null_schema = keep_left ? right.schema : left.schema;
    for (const ColumnarChunk& ch : keep_tbl->chunks) {
      if (ch.rows == 0) {
        continue;
      }
      ColumnarChunk dst;
      dst.rows = ch.rows;
      dst.string_arena = std::make_shared<ChunkStringArena>();
      if (!keep_left) {
        AppendNullColumnsToChunk(ch.rows, null_schema, &dst);
      }
      AppendAliasColumnsToChunk(ch, &dst);
      if (keep_left) {
        AppendNullColumnsToChunk(ch.rows, null_schema, &dst);
      }
      out->total_rows += dst.rows;
      out->chunks.push_back(std::move(dst));
    }
    return std::optional<Relation>{FinalizeColumnarJoinResult(
        context, result_schema, std::move(using_columns), stats_seed, 0,
        std::move(out))};
  }

  bool any_all_null_key = false;
  bool all_int_keys = !build_cols.empty();
  bool single_str_key = build_cols.size() == 1;
  for (size_t k = 0; k < build_cols.size(); ++k) {
    const ColumnarColSummary& bp = build_plan[build_cols[k]];
    const ColumnarColSummary& pp = probe_plan[probe_cols[k]];
    if (bp.all_null || pp.all_null) {
      any_all_null_key = true;
    }
    const bool int_ok = bp.all_uniform && pp.all_uniform &&
                        !bp.common_unsigned && !pp.common_unsigned &&
                        bp.common_type == pp.common_type &&
                        (bp.common_type == ValueType::kInt64 ||
                         bp.common_type == ValueType::kDate);
    all_int_keys &= int_ok;
    const bool str_ok = bp.all_uniform && pp.all_uniform &&
                        !bp.common_interval && !pp.common_interval &&
                        bp.common_type == ValueType::kVarChar &&
                        pp.common_type == ValueType::kVarChar;
    single_str_key &= str_ok;
  }

  std::vector<slot_t> res_left_slots;
  std::vector<slot_t> res_right_slots;
  const size_t left_width = left.schema.ColumnCount();
  std::vector<const BytecodeProgram*> active_residuals;
  if (compiled_residuals && !compiled_residuals->empty()) {
    if (compiled_residuals->size() == residual.size()) {
      std::vector<int8_t> key_bias_safe(left_columns.size(), -1);
      active_residuals.reserve(compiled_residuals->size());
      for (size_t pi = 0; pi < residual.size(); ++pi) {
        if (CanElideJoinResidualWithKeys(
                residual[pi], result_schema, left_width, flip, *build_tbl,
                *probe_tbl, left_columns, right_columns, left_key_offsets,
                right_key_offsets, build_cols, probe_cols, build_biases,
                probe_biases, build_plan, probe_plan, &key_bias_safe)) {
          continue;
        }
        active_residuals.push_back(&(*compiled_residuals)[pi]);
      }
    } else {
      active_residuals.reserve(compiled_residuals->size());
      for (const BytecodeProgram& program : *compiled_residuals) {
        active_residuals.push_back(&program);
      }
    }
  }
  const bool has_active_residuals = !active_residuals.empty();
  if (has_active_residuals) {
    for (const BytecodeProgram* program : active_residuals) {
      for (const BytecodeInstruction& inst : program->Instructions()) {
        if (inst.opcode == BytecodeOp::kLoadColumn) {
          const size_t slot = inst.operand;
          if (slot < left_width) {
            res_left_slots.push_back(static_cast<slot_t>(slot));
          } else if (slot < left_width + right.schema.ColumnCount()) {
            res_right_slots.push_back(static_cast<slot_t>(slot - left_width));
          }
        }
      }
    }
    std::ranges::sort(res_left_slots);
    res_left_slots.erase(std::ranges::unique(res_left_slots).begin(),
                         res_left_slots.end());
    std::ranges::sort(res_right_slots);
    res_right_slots.erase(std::ranges::unique(res_right_slots).begin(),
                          res_right_slots.end());
  }
  const std::vector<slot_t>& res_probe_slots =
      flip ? res_right_slots : res_left_slots;
  const std::vector<slot_t>& res_build_slots =
      flip ? res_left_slots : res_right_slots;
  const size_t res_probe_off = flip ? left_width : 0;
  const size_t res_build_off = flip ? 0 : left_width;

  struct FastResidualScaledDoubleCmp {
    bool active{false};
    bool probe_is_lhs{true};
    slot_t probe_col{0};
    slot_t build_col{0};
    double scale{1.0};
    BinaryOperation op{BinaryOperation::kLessThanEquals};
  } fast_res_scaled_cmp;
  if (active_residuals.size() == 1 && compiled_residuals.has_value() &&
      compiled_residuals->size() == residual.size()) {
    size_t active_pi = 0;
    for (size_t pi = 0; pi < residual.size(); ++pi) {
      if (&(*compiled_residuals)[pi] == active_residuals[0]) {
        active_pi = pi;
        break;
      }
    }
    const Expression& r_expr = residual[active_pi];
    if (r_expr && r_expr->Type() == TypeTag::kBinaryExp) {
      const auto& bin = r_expr->AsBinaryExpression();
      const BinaryOperation bop = bin.Op();
      if (bop == BinaryOperation::kLessThan ||
          bop == BinaryOperation::kLessThanEquals ||
          bop == BinaryOperation::kGreaterThan ||
          bop == BinaryOperation::kGreaterThanEquals) {
        auto unwrap_double_cast = [](const Expression& e) -> Expression {
          if (e && e->Type() == TypeTag::kCastExp &&
              e->AsCastExpression().TargetValueType() == ValueType::kDouble &&
              e->AsCastExpression().Child() != nullptr) {
            return e->AsCastExpression().Child();
          }
          return e;
        };
        struct ColSideRef {
          bool on_probe{false};
          slot_t col{0};
        };
        auto extract_col = [&](const Expression& raw_e,
                               ColSideRef* out) -> bool {
          const Expression& e = unwrap_double_cast(raw_e);
          if (!e || e->Type() != TypeTag::kColumnValue) {
            return false;
          }
          const auto off = LocalColumnOffset(
              result_schema, e->AsColumnValue().GetColumnName());
          if (!off.has_value() || *off >= result_schema.ColumnCount()) {
            return false;
          }
          const Column& c = result_schema.GetColumn(*off);
          if (c.IsUnsigned() || c.Type() != ValueType::kDouble) {
            return false;
          }
          if (*off < left_width) {
            out->on_probe = !flip;
            out->col = *off;
          } else {
            out->on_probe = flip;
            out->col = *off - left_width;
          }
          return true;
        };
        auto extract_const = [&](const Expression& raw_e, double* out) -> bool {
          const Expression& e = unwrap_double_cast(raw_e);
          if (!e || e->Type() != TypeTag::kConstantValue) {
            return false;
          }
          const Value& v = e->AsConstantValue().GetValue();
          if (v.IsNull()) {
            return false;
          }
          if (v.type == ValueType::kDouble) {
            if (!std::isfinite(v.value.double_value)) {
              return false;
            }
            *out = v.value.double_value;
            return true;
          }
          if (v.type == ValueType::kInt64 && !v.IsUnsigned()) {
            *out = static_cast<double>(v.value.int_value);
            return true;
          }
          return false;
        };
        auto extract_scaled = [&](const Expression& raw_e, ColSideRef* col_out,
                                  double* scale_out) -> bool {
          const Expression& e = unwrap_double_cast(raw_e);
          if (!e || e->Type() != TypeTag::kBinaryExp ||
              e->AsBinaryExpression().Op() != BinaryOperation::kMultiply) {
            return false;
          }
          const auto& mb = e->AsBinaryExpression();
          if (extract_const(mb.Left(), scale_out) &&
              extract_col(mb.Right(), col_out)) {
            return true;
          }
          if (extract_col(mb.Left(), col_out) &&
              extract_const(mb.Right(), scale_out)) {
            return true;
          }
          return false;
        };
        auto flip_op = [](BinaryOperation o) -> BinaryOperation {
          switch (o) {
            case BinaryOperation::kLessThan:
              return BinaryOperation::kGreaterThan;
            case BinaryOperation::kLessThanEquals:
              return BinaryOperation::kGreaterThanEquals;
            case BinaryOperation::kGreaterThan:
              return BinaryOperation::kLessThan;
            case BinaryOperation::kGreaterThanEquals:
              return BinaryOperation::kLessThanEquals;
            default:
              return o;
          }
        };
        ColSideRef ca;
        ColSideRef cb;
        double sc = 1.0;
        bool matched = false;
        BinaryOperation cmp_op = bop;
        if (extract_col(bin.Left(), &ca) &&
            extract_scaled(bin.Right(), &cb, &sc) &&
            ca.on_probe != cb.on_probe) {
          matched = true;
          cmp_op = bop;
        } else if (extract_scaled(bin.Left(), &cb, &sc) &&
                   extract_col(bin.Right(), &ca) &&
                   ca.on_probe != cb.on_probe) {
          matched = true;
          cmp_op = flip_op(bop);
        }
        if (matched) {
          const slot_t pcol = ca.on_probe ? ca.col : cb.col;
          const slot_t bcol = ca.on_probe ? cb.col : ca.col;
          if (probe_plan[pcol].all_uniform && build_plan[bcol].all_uniform &&
              (probe_plan[pcol].all_null ||
               probe_plan[pcol].common_type == ValueType::kDouble) &&
              (build_plan[bcol].all_null ||
               build_plan[bcol].common_type == ValueType::kDouble)) {
            fast_res_scaled_cmp = {.active = true,
                                   .probe_is_lhs = ca.on_probe,
                                   .probe_col = pcol,
                                   .build_col = bcol,
                                   .scale = sc,
                                   .op = cmp_op};
          }
        }
      }
    }
  }

  std::vector<uint32_t> build_chunk_of(n_build);
  std::vector<uint32_t> build_row_of(n_build);
  std::vector<uint32_t> chunk_base_bid(build_tbl->chunks.size(), 0);
  {
    uint32_t bid_acc = 0;
    for (size_t bc = 0; bc < build_tbl->chunks.size(); ++bc) {
      chunk_base_bid[bc] = bid_acc;
      bid_acc += static_cast<uint32_t>(build_tbl->chunks[bc].rows);
    }
  }
  std::vector<uint32_t> next_in_bucket(n_build, kColumnarEmptyBucket);

  enum class KeyMode {
    kEmptyNull,
    kSingleInt64,
    kPacked2x32,
    kMultiInt64,
    kSingleVarChar,
    kEncoded
  };
  KeyMode mode = KeyMode::kEncoded;
  if (any_all_null_key) {
    mode = KeyMode::kEmptyNull;
  } else if (all_int_keys && build_cols.size() == 1) {
    mode = KeyMode::kSingleInt64;
  } else if (all_int_keys && build_cols.size() == 2 &&
             ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_PACKED2X32_HJ") ==
                 nullptr &&
             JitPacked2x32HashProbe::Compile(false, false, false, false)
                 .has_value()) {
    mode = KeyMode::kPacked2x32;
  } else if (all_int_keys && build_cols.size() <= CompositeIntKey::kMaxParts) {
    mode = KeyMode::kMultiInt64;
  } else if (single_str_key) {
    mode = KeyMode::kSingleVarChar;
  }

  struct ChunkSliceTask {
    uint32_t chunk_idx{0};
    uint32_t row_begin{0};
    uint32_t row_end{0};
  };
  static const bool probe_disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_PARALLEL_PROBE") !=
      nullptr;
  const size_t max_w =
      QueryParallelWorkerLimit(std::thread::hardware_concurrency());

  static constexpr size_t kColumnarBuildShards = 64;
  const size_t build_workers =
      (!probe_disabled && max_w > 1 && mode != KeyMode::kEmptyNull &&
       ((mode == KeyMode::kEncoded && n_build >= 4096) || n_build >= 16384))
          ? std::min<size_t>(
                {kParallelProbeMaxWorkers, max_w, (n_build + 2047) / 2048})
          : 1;
  const bool sharded = build_workers > 1;

  auto int_shard_of = [](int64_t k) noexcept -> size_t {
    return (MixEstimateHash64(static_cast<uint64_t>(k)) >> 32) &
           (kColumnarBuildShards - 1);
  };

  FlatIntKeyMap<uint32_t> int_map;
  FlatCompositeIntMap comp_map;
  FlatStringViewMap str_map;
  std::vector<FlatIntKeyMap<uint32_t>> int_shards;
  std::vector<FlatCompositeIntMap> comp_shards;
  std::vector<FlatStringViewMap> str_shards;
  ChunkStringArena encoded_build_arena;
  std::vector<ChunkStringArena> encoded_worker_arenas;

  static_assert(kColumnarEmptyBucket == std::numeric_limits<uint32_t>::max());
  std::vector<uint64_t> packed_ht_keys;
  std::vector<uint32_t> packed_ht_heads;
  std::vector<uint64_t> packed_bloom_words;
  uint64_t packed_ht_mask = 0;
  uint64_t packed_bloom_word_mask = 0;
  bool packed_has_bloom = false;
  bool packed_built_chunk_map = false;

  if (mode == KeyMode::kPacked2x32) {
    struct PackedBuildItem {
      uint64_t key;
      uint32_t bid;
    };
    const slot_t bcol0 = build_cols[0];
    const slot_t bcol1 = build_cols[1];
    const int64_t b0 = build_biases[0];
    const int64_t b1 = build_biases[1];
    std::atomic<bool> packed_overflow{false};
    auto extract_slice = [&](const ColumnarChunk& ch, uint32_t bc, uint32_t r0,
                             uint32_t r1, SrcIntLeafCache* lc,
                             std::vector<PackedBuildItem>* out) {
      const uint32_t base = chunk_base_bid[bc];
      for (uint32_t r = r0; r < r1; ++r) {
        build_chunk_of[base + r] = bc;
        build_row_of[base + r] = r;
      }
      if (packed_overflow.load(std::memory_order_relaxed)) {
        return;
      }
      const ColumnarColumn& c0 = ch.columns[bcol0];
      const ColumnarColumn& c1 = ch.columns[bcol1];
      if (c0.encoding == ColumnarColumn::Encoding::kFlat && c0.is_uniform &&
          (!c0.has_null || c0.nulls.empty()) && c0.type != ValueType::kNull &&
          c1.encoding == ColumnarColumn::Encoding::kFlat && c1.is_uniform &&
          (!c1.has_null || c1.nulls.empty()) && c1.type != ValueType::kNull) {
        const int64_t* p0 = c0.ints.data();
        const int64_t* p1 = c1.ints.data();
        for (uint32_t r = r0; r < r1; ++r) {
          const int64_t v0 = b0 != 0 ? AddJoinKeyBias(p0[r], b0) : p0[r];
          const int64_t v1 = b1 != 0 ? AddJoinKeyBias(p1[r], b1) : p1[r];
          if (static_cast<int64_t>(static_cast<int32_t>(v0)) != v0 ||
              static_cast<int64_t>(static_cast<int32_t>(v1)) != v1) {
            packed_overflow.store(true, std::memory_order_relaxed);
            return;
          }
          const uint64_t packed =
              (static_cast<uint64_t>(
                   static_cast<uint32_t>(static_cast<int32_t>(v0)))
               << 32) |
              static_cast<uint64_t>(
                  static_cast<uint32_t>(static_cast<int32_t>(v1)));
          out->push_back({packed, base + r});
        }
        return;
      }
      std::array<int64_t, 64> t0{};
      std::array<int64_t, 64> t1{};
      for (uint32_t p = r0; p < r1; p += 64) {
        const uint32_t sub = std::min<uint32_t>(64, r1 - p);
        uint64_t valid = DecodeIntTile64(c0, p, sub, lc, t0.data());
        valid &= DecodeIntTile64(c1, p, sub, lc, t1.data());
        while (valid != 0) {
          const auto i = static_cast<uint32_t>(std::countr_zero(valid));
          valid &= (valid - 1);
          const int64_t v0 = b0 != 0 ? AddJoinKeyBias(t0[i], b0) : t0[i];
          const int64_t v1 = b1 != 0 ? AddJoinKeyBias(t1[i], b1) : t1[i];
          if (static_cast<int64_t>(static_cast<int32_t>(v0)) != v0 ||
              static_cast<int64_t>(static_cast<int32_t>(v1)) != v1) {
            packed_overflow.store(true, std::memory_order_relaxed);
            return;
          }
          const uint64_t packed =
              (static_cast<uint64_t>(
                   static_cast<uint32_t>(static_cast<int32_t>(v0)))
               << 32) |
              static_cast<uint64_t>(
                  static_cast<uint32_t>(static_cast<int32_t>(v1)));
          out->push_back({packed, base + p + i});
        }
      }
    };
    const size_t extract_workers = sharded ? build_workers : 1;
    std::vector<std::vector<PackedBuildItem>> worker_items(extract_workers);
    if (!sharded) {
      SrcIntLeafCache lc;
      worker_items[0].reserve(n_build);
      for (uint32_t bc = 0; bc < build_tbl->chunks.size(); ++bc) {
        const ColumnarChunk& ch = build_tbl->chunks[bc];
        extract_slice(ch, bc, 0, static_cast<uint32_t>(ch.rows), &lc,
                      worker_items.data());
        if (packed_overflow.load(std::memory_order_relaxed)) {
          break;
        }
      }
    } else {
      std::vector<ChunkSliceTask> ext_tasks;
      constexpr uint32_t kExtSliceRows = 4096;
      for (uint32_t bc = 0; bc < build_tbl->chunks.size(); ++bc) {
        const auto nrows = static_cast<uint32_t>(build_tbl->chunks[bc].rows);
        for (uint32_t r0 = 0; r0 < nrows; r0 += kExtSliceRows) {
          ext_tasks.push_back(
              {.chunk_idx = bc,
               .row_begin = r0,
               .row_end = std::min<uint32_t>(nrows, r0 + kExtSliceRows)});
        }
      }
      std::atomic<size_t> next_ext{0};
      RETURN_IF_FAIL(RunParallelTasks(extract_workers, [&](size_t w) {
        SrcIntLeafCache lc;
        auto& out = worker_items[w];
        out.reserve((n_build / extract_workers) + 64);
        while (true) {
          const size_t tid = next_ext.fetch_add(1, std::memory_order_relaxed);
          if (tid >= ext_tasks.size()) {
            break;
          }
          const ChunkSliceTask& t = ext_tasks[tid];
          extract_slice(build_tbl->chunks[t.chunk_idx], t.chunk_idx,
                        t.row_begin, t.row_end, &lc, &out);
        }
      }));
    }
    if (packed_overflow.load(std::memory_order_relaxed)) {
      mode = KeyMode::kMultiInt64;
    } else {
      size_t valid_build_count = 0;
      for (const auto& wi : worker_items) {
        valid_build_count += wi.size();
      }
      if (valid_build_count == 0) {
        mode = KeyMode::kEmptyNull;
        packed_built_chunk_map = true;
      } else {
        const size_t ht_cap =
            std::bit_ceil(std::max<size_t>(16, valid_build_count * 2));
        packed_ht_mask = ht_cap - 1;
        packed_ht_keys.assign(ht_cap, 0ULL);
        packed_ht_heads.assign(ht_cap, kColumnarEmptyBucket);
        packed_has_bloom = (valid_build_count >= 16384);
        if (packed_has_bloom) {
          const size_t bloom_bits = std::clamp<size_t>(
              std::bit_ceil(valid_build_count * 8), 4096, size_t{1} << 25);
          packed_bloom_words.assign(bloom_bits >> 6, 0ULL);
          packed_bloom_word_mask = (bloom_bits >> 6) - 1;
        }
        if (!sharded || ht_cap < 1024 || extract_workers <= 1) {
          for (auto& wi : worker_items) {
            for (const PackedBuildItem& it : wi) {
              const uint64_t h = HashPacked2x32Key(it.key);
              if (packed_has_bloom) {
                InsertPacked2x32Bloom(h, packed_bloom_words.data(),
                                      packed_bloom_word_mask);
              }
              auto slot = static_cast<size_t>(h & packed_ht_mask);
              while (packed_ht_heads[slot] != kColumnarEmptyBucket &&
                     packed_ht_keys[slot] != it.key) {
                slot = (slot + 1) & packed_ht_mask;
              }
              if (packed_ht_heads[slot] == kColumnarEmptyBucket) {
                packed_ht_keys[slot] = it.key;
                next_in_bucket[it.bid] = kColumnarEmptyBucket;
              } else {
                next_in_bucket[it.bid] = packed_ht_heads[slot];
              }
              packed_ht_heads[slot] = it.bid;
            }
            std::vector<PackedBuildItem>().swap(wi);
          }
        } else {
          constexpr size_t kBuildParts = 64;
          const size_t range_len = ht_cap >> 6;
          const uint32_t range_shift =
              static_cast<uint32_t>(std::bit_width(packed_ht_mask)) - 6U;
          struct PartItem {
            uint64_t key;
            uint64_t h;
            uint32_t bid;
          };
          std::vector<std::array<std::vector<PartItem>, kBuildParts>>
              part_items(extract_workers);
          RETURN_IF_FAIL(RunParallelTasks(extract_workers, [&](size_t w) {
            auto& parts = part_items[w];
            for (const PackedBuildItem& it : worker_items[w]) {
              const uint64_t h = HashPacked2x32Key(it.key);
              const auto p =
                  static_cast<size_t>((h & packed_ht_mask) >> range_shift);
              parts[p].push_back({it.key, h, it.bid});
            }
            std::vector<PackedBuildItem>().swap(worker_items[w]);
          }));
          std::array<std::vector<PartItem>, kBuildParts> overflow_items{};
          std::atomic<size_t> next_p{0};
          RETURN_IF_FAIL(RunParallelTasks(extract_workers, [&](size_t) {
            while (true) {
              const size_t p = next_p.fetch_add(1, std::memory_order_relaxed);
              if (p >= kBuildParts) {
                break;
              }
              const size_t r_begin = p * range_len;
              const size_t r_end = r_begin + range_len;
              for (size_t w = 0; w < extract_workers; ++w) {
                auto& bucket = part_items[w][p];
                for (const PartItem& it : bucket) {
                  if (packed_has_bloom) {
                    const auto w0 = static_cast<size_t>((it.h >> 6U) &
                                                        packed_bloom_word_mask);
                    const auto w1 = static_cast<size_t>((it.h >> 30U) &
                                                        packed_bloom_word_mask);
                    const uint64_t m0 = uint64_t{1} << (it.h & 63U);
                    const uint64_t m1 = uint64_t{1} << ((it.h >> 24U) & 63U);
                    __atomic_fetch_or(&packed_bloom_words[w0], m0,
                                      __ATOMIC_RELAXED);
                    __atomic_fetch_or(&packed_bloom_words[w1], m1,
                                      __ATOMIC_RELAXED);
                  }
                  auto slot = static_cast<size_t>(it.h & packed_ht_mask);
                  while (slot < r_end &&
                         packed_ht_heads[slot] != kColumnarEmptyBucket &&
                         packed_ht_keys[slot] != it.key) {
                    ++slot;
                  }
                  if (slot == r_end) {
                    overflow_items[p].push_back(it);
                  } else if (packed_ht_heads[slot] == kColumnarEmptyBucket) {
                    packed_ht_keys[slot] = it.key;
                    next_in_bucket[it.bid] = kColumnarEmptyBucket;
                    packed_ht_heads[slot] = it.bid;
                  } else {
                    next_in_bucket[it.bid] = packed_ht_heads[slot];
                    packed_ht_heads[slot] = it.bid;
                  }
                }
                std::vector<PartItem>().swap(bucket);
              }
            }
          }));
          for (size_t p = 0; p < kBuildParts; ++p) {
            for (const PartItem& it : overflow_items[p]) {
              auto slot = static_cast<size_t>(it.h & packed_ht_mask);
              while (packed_ht_heads[slot] != kColumnarEmptyBucket &&
                     packed_ht_keys[slot] != it.key) {
                slot = (slot + 1) & packed_ht_mask;
              }
              if (packed_ht_heads[slot] == kColumnarEmptyBucket) {
                packed_ht_keys[slot] = it.key;
                next_in_bucket[it.bid] = kColumnarEmptyBucket;
              } else {
                next_in_bucket[it.bid] = packed_ht_heads[slot];
              }
              packed_ht_heads[slot] = it.bid;
            }
          }
        }
      }
    }
  }

  if (mode == KeyMode::kPacked2x32 ||
      (mode == KeyMode::kEmptyNull && packed_built_chunk_map)) {
  } else if (!sharded) {
    for (uint32_t bc = 0; bc < build_tbl->chunks.size(); ++bc) {
      const uint32_t base = chunk_base_bid[bc];
      const auto rows = static_cast<uint32_t>(build_tbl->chunks[bc].rows);
      for (uint32_t r = 0; r < rows; ++r) {
        build_chunk_of[base + r] = bc;
        build_row_of[base + r] = r;
      }
    }
    if (mode == KeyMode::kSingleInt64) {
      int_map.Reserve(n_build);
      const slot_t bcol_idx = build_cols[0];
      const int64_t bias = build_biases[0];
      SrcIntLeafCache build_leaf_cache;
      for (size_t bc = 0; bc < build_tbl->chunks.size(); ++bc) {
        const ColumnarChunk& ch = build_tbl->chunks[bc];
        const uint32_t base = chunk_base_bid[bc];
        const ColumnarColumn& col = ch.columns[bcol_idx];
        if (col.encoding == ColumnarColumn::Encoding::kConstant) {
          if (col.IsNull(0)) {
            continue;
          }
          const int64_t k =
              bias != 0 ? AddJoinKeyBias(col.IntAt(0), bias) : col.IntAt(0);
          for (size_t r = 0; r < ch.rows; ++r) {
            const uint32_t bid = base + static_cast<uint32_t>(r);
            bool ins = false;
            uint32_t* head = int_map.TryEmplace(k, &ins);
            next_in_bucket[bid] = ins ? kColumnarEmptyBucket : *head;
            *head = bid;
          }
          continue;
        }
        if (col.encoding == ColumnarColumn::Encoding::kFlat && col.is_uniform &&
            (!col.has_null || col.nulls.empty()) &&
            col.type != ValueType::kNull) {
          const int64_t* ptr = col.ints.data();
          for (size_t r = 0; r < ch.rows; ++r) {
            const int64_t k = bias != 0 ? AddJoinKeyBias(ptr[r], bias) : ptr[r];
            const uint32_t bid = base + static_cast<uint32_t>(r);
            bool ins = false;
            uint32_t* head = int_map.TryEmplace(k, &ins);
            next_in_bucket[bid] = ins ? kColumnarEmptyBucket : *head;
            *head = bid;
          }
          continue;
        }
        std::array<int64_t, 64> tile_vals{};
        const auto nrows = static_cast<uint32_t>(ch.rows);
        for (uint32_t r0 = 0; r0 < nrows; r0 += 64) {
          const uint32_t cnt = std::min<uint32_t>(64, nrows - r0);
          uint64_t valid = DecodeIntTile64(col, r0, cnt, &build_leaf_cache,
                                           tile_vals.data());
          while (valid != 0) {
            const auto i = static_cast<uint32_t>(std::countr_zero(valid));
            valid &= (valid - 1);
            const int64_t k =
                bias != 0 ? AddJoinKeyBias(tile_vals[i], bias) : tile_vals[i];
            const uint32_t bid = base + r0 + i;
            bool ins = false;
            uint32_t* head = int_map.TryEmplace(k, &ins);
            next_in_bucket[bid] = ins ? kColumnarEmptyBucket : *head;
            *head = bid;
          }
        }
      }
    } else if (mode == KeyMode::kMultiInt64) {
      comp_map.Reserve(n_build);
      for (size_t bc = 0; bc < build_tbl->chunks.size(); ++bc) {
        const ColumnarChunk& ch = build_tbl->chunks[bc];
        const uint32_t base = chunk_base_bid[bc];
        for (size_t r = 0; r < ch.rows; ++r) {
          bool has_null = false;
          CompositeIntKey k;
          k.count = static_cast<uint8_t>(build_cols.size());
          for (size_t idx = 0; idx < build_cols.size(); ++idx) {
            const ColumnarColumn& col = ch.columns[build_cols[idx]];
            if (col.IsNull(r)) {
              has_null = true;
              break;
            }
            const int64_t raw = col.IntAt(r);
            k.parts[idx] = build_biases[idx] != 0
                               ? AddJoinKeyBias(raw, build_biases[idx])
                               : raw;
          }
          if (has_null) {
            continue;
          }
          const uint32_t bid = base + static_cast<uint32_t>(r);
          bool ins = false;
          uint32_t* head = comp_map.TryEmplace(k, &ins);
          next_in_bucket[bid] = ins ? kColumnarEmptyBucket : *head;
          *head = bid;
        }
      }
    } else if (mode == KeyMode::kSingleVarChar) {
      str_map.Reserve(n_build);
      const slot_t bcol_idx = build_cols[0];
      std::vector<uint64_t> dict_hashes;
      for (size_t bc = 0; bc < build_tbl->chunks.size(); ++bc) {
        const ColumnarChunk& ch = build_tbl->chunks[bc];
        const uint32_t base = chunk_base_bid[bc];
        const ColumnarColumn& col = ch.columns[bcol_idx];
        if (col.encoding == ColumnarColumn::Encoding::kDictionary &&
            col.dict != nullptr) {
          const size_t nd = col.DictEntryCount();
          dict_hashes.resize(nd);
          for (size_t d = 0; d < nd; ++d) {
            dict_hashes[d] =
                FlatStringViewMap::HashSv(col.dict->StringEntryAt(d));
          }
          for (size_t r = 0; r < ch.rows; ++r) {
            const uint32_t code = col.DictCodeAt(r);
            if (col.has_null &&
                (code == ColumnarColumn::kNullDictCode || code >= nd)) {
              continue;
            }
            const uint32_t bid = base + static_cast<uint32_t>(r);
            bool ins = false;
            uint32_t* head = str_map.TryEmplaceWithHash(
                col.dict->StringEntryAt(code), dict_hashes[code], &ins);
            next_in_bucket[bid] = ins ? kColumnarEmptyBucket : *head;
            *head = bid;
          }
          continue;
        }
        if (col.encoding == ColumnarColumn::Encoding::kFlat && col.is_uniform &&
            !col.has_null) {
          const std::string_view* ptr = col.strings.data();
          for (size_t r = 0; r < ch.rows; ++r) {
            const uint32_t bid = base + static_cast<uint32_t>(r);
            bool ins = false;
            uint32_t* head = str_map.TryEmplace(ptr[r], &ins);
            next_in_bucket[bid] = ins ? kColumnarEmptyBucket : *head;
            *head = bid;
          }
          continue;
        }
        for (size_t r = 0; r < ch.rows; ++r) {
          if (col.IsNull(r)) {
            continue;
          }
          const uint32_t bid = base + static_cast<uint32_t>(r);
          bool ins = false;
          uint32_t* head = str_map.TryEmplace(col.StringAt(r), &ins);
          next_in_bucket[bid] = ins ? kColumnarEmptyBucket : *head;
          *head = bid;
        }
      }
    } else if (mode == KeyMode::kEncoded) {
      str_map.Reserve(n_build);
      Value scratch;
      std::string key_buf;
      for (size_t bc = 0; bc < build_tbl->chunks.size(); ++bc) {
        const ColumnarChunk& ch = build_tbl->chunks[bc];
        const uint32_t base = chunk_base_bid[bc];
        for (size_t r = 0; r < ch.rows; ++r) {
          if (!TryColumnarEncodeJoinKeyNonNull(
                  ch, build_cols, r, true, &build_biases, &scratch, &key_buf)) {
            continue;
          }
          const std::string_view stored = encoded_build_arena.Store(key_buf);
          const uint32_t bid = base + static_cast<uint32_t>(r);
          bool ins = false;
          uint32_t* head = str_map.TryEmplace(stored, &ins);
          next_in_bucket[bid] = ins ? kColumnarEmptyBucket : *head;
          *head = bid;
        }
      }
    }
  } else {
    std::vector<ChunkSliceTask> build_tasks;
    constexpr uint32_t kBuildSliceRows = 4096;
    for (uint32_t bc = 0; bc < build_tbl->chunks.size(); ++bc) {
      const auto nrows = static_cast<uint32_t>(build_tbl->chunks[bc].rows);
      for (uint32_t r0 = 0; r0 < nrows; r0 += kBuildSliceRows) {
        build_tasks.push_back(
            {.chunk_idx = bc,
             .row_begin = r0,
             .row_end = std::min<uint32_t>(nrows, r0 + kBuildSliceRows)});
      }
    }
    const size_t shard_mask = kColumnarBuildShards - 1;
    std::atomic<size_t> next_build_task{0};
    std::atomic<size_t> next_shard{0};

    if (mode == KeyMode::kSingleInt64) {
      struct IntItem {
        int64_t key;
        uint32_t bid;
      };
      int_shards.resize(kColumnarBuildShards);
      std::vector<std::vector<IntItem>> buckets(build_workers *
                                                kColumnarBuildShards);
      const slot_t bcol_idx = build_cols[0];
      const int64_t bias = build_biases[0];
      RETURN_IF_FAIL(RunParallelTasks(build_workers, [&](size_t w) {
        const size_t w_base = w * kColumnarBuildShards;
        SrcIntLeafCache build_leaf_cache;
        while (true) {
          const size_t tid =
              next_build_task.fetch_add(1, std::memory_order_relaxed);
          if (tid >= build_tasks.size()) {
            break;
          }
          const ChunkSliceTask task = build_tasks[tid];
          const uint32_t bc = task.chunk_idx;
          const ColumnarChunk& ch = build_tbl->chunks[bc];
          const uint32_t base = chunk_base_bid[bc];
          for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
            build_chunk_of[base + r] = bc;
            build_row_of[base + r] = r;
          }
          const ColumnarColumn& col = ch.columns[bcol_idx];
          if (col.encoding == ColumnarColumn::Encoding::kConstant) {
            if (col.IsNull(0)) {
              continue;
            }
            const int64_t k =
                bias != 0 ? AddJoinKeyBias(col.IntAt(0), bias) : col.IntAt(0);
            auto& dst = buckets[w_base + int_shard_of(k)];
            for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
              dst.push_back({k, base + r});
            }
            continue;
          }
          if (col.encoding == ColumnarColumn::Encoding::kFlat &&
              col.is_uniform && (!col.has_null || col.nulls.empty()) &&
              col.type != ValueType::kNull) {
            const int64_t* ptr = col.ints.data();
            for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
              const int64_t k =
                  bias != 0 ? AddJoinKeyBias(ptr[r], bias) : ptr[r];
              buckets[w_base + int_shard_of(k)].push_back({k, base + r});
            }
            continue;
          }
          std::array<int64_t, 64> tile_vals{};
          for (uint32_t r0 = task.row_begin; r0 < task.row_end; r0 += 64) {
            const uint32_t cnt = std::min<uint32_t>(64, task.row_end - r0);
            uint64_t valid = DecodeIntTile64(col, r0, cnt, &build_leaf_cache,
                                             tile_vals.data());
            while (valid != 0) {
              const auto i = static_cast<uint32_t>(std::countr_zero(valid));
              valid &= (valid - 1);
              const int64_t k =
                  bias != 0 ? AddJoinKeyBias(tile_vals[i], bias) : tile_vals[i];
              buckets[w_base + int_shard_of(k)].push_back({k, base + r0 + i});
            }
          }
        }
      }));
      RETURN_IF_FAIL(RunParallelTasks(build_workers, [&](size_t) {
        while (true) {
          const size_t s = next_shard.fetch_add(1, std::memory_order_relaxed);
          if (s >= kColumnarBuildShards) {
            break;
          }
          size_t total = 0;
          for (size_t w = 0; w < build_workers; ++w) {
            total += buckets[(w * kColumnarBuildShards) + s].size();
          }
          if (total == 0) {
            continue;
          }
          FlatIntKeyMap<uint32_t>& map = int_shards[s];
          map.Reserve(total);
          for (size_t w = 0; w < build_workers; ++w) {
            for (const IntItem& it : buckets[(w * kColumnarBuildShards) + s]) {
              bool ins = false;
              uint32_t* head = map.TryEmplace(it.key, &ins);
              next_in_bucket[it.bid] = ins ? kColumnarEmptyBucket : *head;
              *head = it.bid;
            }
          }
        }
      }));
    } else if (mode == KeyMode::kMultiInt64) {
      struct CompItem {
        uint64_t h;
        CompositeIntKey key;
        uint32_t bid;
      };
      comp_shards.resize(kColumnarBuildShards);
      std::vector<std::vector<CompItem>> buckets(build_workers *
                                                 kColumnarBuildShards);
      RETURN_IF_FAIL(RunParallelTasks(build_workers, [&](size_t w) {
        const size_t w_base = w * kColumnarBuildShards;
        while (true) {
          const size_t tid =
              next_build_task.fetch_add(1, std::memory_order_relaxed);
          if (tid >= build_tasks.size()) {
            break;
          }
          const ChunkSliceTask task = build_tasks[tid];
          const uint32_t bc = task.chunk_idx;
          const ColumnarChunk& ch = build_tbl->chunks[bc];
          const uint32_t base = chunk_base_bid[bc];
          for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
            build_chunk_of[base + r] = bc;
            build_row_of[base + r] = r;
          }
          for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
            bool has_null = false;
            CompositeIntKey k;
            k.count = static_cast<uint8_t>(build_cols.size());
            for (size_t idx = 0; idx < build_cols.size(); ++idx) {
              const ColumnarColumn& col = ch.columns[build_cols[idx]];
              if (col.IsNull(r)) {
                has_null = true;
                break;
              }
              const int64_t raw = col.IntAt(r);
              k.parts[idx] = build_biases[idx] != 0
                                 ? AddJoinKeyBias(raw, build_biases[idx])
                                 : raw;
            }
            if (has_null) {
              continue;
            }
            const uint64_t h = FlatCompositeIntMap::HashKey(k);
            buckets[w_base + ((h >> 32) & shard_mask)].push_back(
                {h, k, base + r});
          }
        }
      }));
      RETURN_IF_FAIL(RunParallelTasks(build_workers, [&](size_t) {
        while (true) {
          const size_t s = next_shard.fetch_add(1, std::memory_order_relaxed);
          if (s >= kColumnarBuildShards) {
            break;
          }
          size_t total = 0;
          for (size_t w = 0; w < build_workers; ++w) {
            total += buckets[(w * kColumnarBuildShards) + s].size();
          }
          if (total == 0) {
            continue;
          }
          FlatCompositeIntMap& map = comp_shards[s];
          map.Reserve(total);
          for (size_t w = 0; w < build_workers; ++w) {
            for (const CompItem& it : buckets[(w * kColumnarBuildShards) + s]) {
              bool ins = false;
              uint32_t* head = map.TryEmplaceWithHash(it.key, it.h, &ins);
              next_in_bucket[it.bid] = ins ? kColumnarEmptyBucket : *head;
              *head = it.bid;
            }
          }
        }
      }));
    } else {
      struct StrItem {
        uint64_t h;
        std::string_view sv;
        uint32_t bid;
      };
      str_shards.resize(kColumnarBuildShards);
      if (mode == KeyMode::kEncoded) {
        encoded_worker_arenas.resize(build_workers);
      }
      std::vector<std::vector<StrItem>> buckets(build_workers *
                                                kColumnarBuildShards);
      const slot_t bcol_idx =
          mode == KeyMode::kSingleVarChar ? build_cols[0] : 0;
      RETURN_IF_FAIL(RunParallelTasks(build_workers, [&](size_t w) {
        const size_t w_base = w * kColumnarBuildShards;
        std::vector<uint64_t> dict_hashes;
        Value scratch;
        std::string key_buf;
        while (true) {
          const size_t tid =
              next_build_task.fetch_add(1, std::memory_order_relaxed);
          if (tid >= build_tasks.size()) {
            break;
          }
          const ChunkSliceTask task = build_tasks[tid];
          const uint32_t bc = task.chunk_idx;
          const ColumnarChunk& ch = build_tbl->chunks[bc];
          const uint32_t base = chunk_base_bid[bc];
          for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
            build_chunk_of[base + r] = bc;
            build_row_of[base + r] = r;
          }
          if (mode == KeyMode::kSingleVarChar) {
            const ColumnarColumn& col = ch.columns[bcol_idx];
            if (col.encoding == ColumnarColumn::Encoding::kDictionary &&
                col.dict != nullptr) {
              const size_t nd = col.DictEntryCount();
              const bool prehash =
                  nd <= static_cast<size_t>(task.row_end - task.row_begin) * 2;
              if (prehash) {
                dict_hashes.resize(nd);
                for (size_t d = 0; d < nd; ++d) {
                  dict_hashes[d] =
                      FlatStringViewMap::HashSv(col.dict->StringEntryAt(d));
                }
              }
              for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
                const uint32_t code = col.DictCodeAt(r);
                if (col.has_null &&
                    (code == ColumnarColumn::kNullDictCode || code >= nd)) {
                  continue;
                }
                const std::string_view sv = col.dict->StringEntryAt(code);
                const uint64_t h =
                    prehash ? dict_hashes[code] : FlatStringViewMap::HashSv(sv);
                buckets[w_base + ((h >> 32) & shard_mask)].push_back(
                    {h, sv, base + r});
              }
              continue;
            }
            if (col.encoding == ColumnarColumn::Encoding::kFlat &&
                col.is_uniform && !col.has_null) {
              const std::string_view* ptr = col.strings.data();
              for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
                const uint64_t h = FlatStringViewMap::HashSv(ptr[r]);
                buckets[w_base + ((h >> 32) & shard_mask)].push_back(
                    {h, ptr[r], base + r});
              }
              continue;
            }
            for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
              if (col.IsNull(r)) {
                continue;
              }
              const std::string_view sv = col.StringAt(r);
              const uint64_t h = FlatStringViewMap::HashSv(sv);
              buckets[w_base + ((h >> 32) & shard_mask)].push_back(
                  {h, sv, base + r});
            }
          } else {
            ChunkStringArena& arena = encoded_worker_arenas[w];
            for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
              if (!TryColumnarEncodeJoinKeyNonNull(ch, build_cols, r, true,
                                                   &build_biases, &scratch,
                                                   &key_buf)) {
                continue;
              }
              const std::string_view stored = arena.Store(key_buf);
              const uint64_t h = FlatStringViewMap::HashSv(stored);
              buckets[w_base + ((h >> 32) & shard_mask)].push_back(
                  {h, stored, base + r});
            }
          }
        }
      }));
      RETURN_IF_FAIL(RunParallelTasks(build_workers, [&](size_t) {
        while (true) {
          const size_t s = next_shard.fetch_add(1, std::memory_order_relaxed);
          if (s >= kColumnarBuildShards) {
            break;
          }
          size_t total = 0;
          for (size_t w = 0; w < build_workers; ++w) {
            total += buckets[(w * kColumnarBuildShards) + s].size();
          }
          if (total == 0) {
            continue;
          }
          FlatStringViewMap& map = str_shards[s];
          map.Reserve(total);
          for (size_t w = 0; w < build_workers; ++w) {
            for (const StrItem& it : buckets[(w * kColumnarBuildShards) + s]) {
              bool ins = false;
              uint32_t* head = map.TryEmplaceWithHash(it.sv, it.h, &ins);
              next_in_bucket[it.bid] = ins ? kColumnarEmptyBucket : *head;
              *head = it.bid;
            }
          }
        }
      }));
    }
  }

  auto find_int = [&](int64_t k) noexcept -> const uint32_t* {
    return sharded ? int_shards[int_shard_of(k)].Find(k) : int_map.Find(k);
  };
  auto find_comp = [&](const CompositeIntKey& k) noexcept -> const uint32_t* {
    if (!sharded) {
      return comp_map.Find(k);
    }
    const uint64_t h = FlatCompositeIntMap::HashKey(k);
    return comp_shards[(h >> 32) & (kColumnarBuildShards - 1)].FindWithHash(k,
                                                                            h);
  };
  auto find_str = [&](std::string_view sv) noexcept -> const uint32_t* {
    if (!sharded) {
      return str_map.Find(sv);
    }
    const uint64_t h = FlatStringViewMap::HashSv(sv);
    return str_shards[(h >> 32) & (kColumnarBuildShards - 1)].FindWithHash(sv,
                                                                           h);
  };

  std::vector<ChunkSliceTask> tasks;
  constexpr uint32_t kProbeSliceRows = 8192;
  for (uint32_t pc = 0; pc < probe_tbl->chunks.size(); ++pc) {
    const auto nrows = static_cast<uint32_t>(probe_tbl->chunks[pc].rows);
    for (uint32_t r0 = 0; r0 < nrows; r0 += kProbeSliceRows) {
      tasks.push_back(
          {.chunk_idx = pc,
           .row_begin = r0,
           .row_end = std::min<uint32_t>(nrows, r0 + kProbeSliceRows)});
    }
  }

  const size_t workers =
      (!probe_disabled && max_w > 1 && n_probe >= 8192 && tasks.size() > 1)
          ? std::min<size_t>({kParallelProbeMaxWorkers, max_w, tasks.size()})
          : 1;

  std::vector<uint8_t> shared_build_matched(want_build_nulls ? n_build : 0, 0);
  struct WorkerOutput {
    std::vector<ColumnarChunk> chunks;
    uint64_t comparisons{0};
  };
  std::vector<WorkerOutput> worker_outputs(workers);
  std::atomic<size_t> next_task{0};
  std::atomic<bool> probe_failed{false};
  std::mutex error_mutex;
  Status first_error{Status::kSuccess};

  // fill_build_side / emit_unmatched_slice only gather groups referenced by
  // non-all-null build columns; mark them once so the per-batch loops below
  // skip building indexes nobody reads (P101).
  std::vector<uint8_t> build_group_used(num_build_groups, 0);
  for (size_t c = 0; c < build_schema.ColumnCount(); ++c) {
    if (!build_plan[c].all_null) {
      build_group_used[build_groups[c]] = 1;
    }
  }

  // The probe-side gather grouping depends only on the chunk's column
  // encodings, not on which rows matched. Derive it once per chunk here
  // (serially, before the workers start) instead of re-deriving it inside
  // emit_matched_batch for every 2048-row output batch (P101).
  struct ProbeEmitLayout {
    std::vector<size_t> group_of;
    std::vector<size_t> target_cols;
    std::vector<size_t> repr_cols;
    std::vector<const ChunkGatherIndex*> repr_sgi;
  };
  const auto gather_source_of =
      [](const ColumnarColumn& col) -> const ChunkGatherIndex* {
    return (col.encoding == ColumnarColumn::Encoding::kGathered &&
            col.gather_index != nullptr &&
            col.gather_index->source_table != nullptr)
               ? col.gather_index.get()
               : nullptr;
  };
  std::vector<ProbeEmitLayout> probe_layouts(probe_tbl->chunks.size());
  for (uint32_t pc = 0; pc < probe_tbl->chunks.size(); ++pc) {
    const ColumnarChunk& pchunk = probe_tbl->chunks[pc];
    ProbeEmitLayout& layout = probe_layouts[pc];
    const size_t n_probe_cols = probe_schema.ColumnCount();
    layout.group_of.resize(n_probe_cols, 0);
    layout.target_cols.resize(n_probe_cols, 0);
    for (size_t c = 0; c < n_probe_cols; ++c) {
      const ColumnarColumn& pcol = pchunk.columns[c];
      const ChunkGatherIndex* sgi = gather_source_of(pcol);
      const ColumnarTable* stbl =
          sgi != nullptr ? sgi->source_table.get() : probe_tbl.get();
      layout.target_cols[c] = sgi != nullptr ? pcol.gather_col : c;
      size_t g = 0;
      for (; g < layout.repr_cols.size(); ++g) {
        const ChunkGatherIndex* rgi = layout.repr_sgi[g];
        const ColumnarTable* rtbl =
            rgi != nullptr ? rgi->source_table.get() : probe_tbl.get();
        if (rgi == sgi && rtbl == stbl) {
          break;
        }
      }
      if (g == layout.repr_cols.size()) {
        layout.repr_cols.push_back(c);
        layout.repr_sgi.push_back(sgi);
      }
      layout.group_of[c] = g;
    }
  }

  auto emit_matched_batch = [&](uint32_t pc, std::span<const uint32_t> p_rows,
                                std::span<const uint32_t> b_ids,
                                std::vector<ColumnarChunk>* out_chunks) {
    const size_t n = p_rows.size();
    if (n == 0) {
      return;
    }
    const ColumnarChunk& pchunk = probe_tbl->chunks[pc];
    const size_t n_probe_cols = probe_schema.ColumnCount();
    const ProbeEmitLayout& layout = probe_layouts[pc];
    std::vector<std::shared_ptr<ChunkGatherIndex>> p_gis;
    p_gis.reserve(layout.repr_cols.size());
    for (size_t g = 0; g < layout.repr_cols.size(); ++g) {
      const ColumnarColumn& pcol = pchunk.columns[layout.repr_cols[g]];
      const ChunkGatherIndex* sgi = layout.repr_sgi[g];
      auto gi = std::make_shared<ChunkGatherIndex>();
      gi->source_table =
          sgi != nullptr ? pcol.gather_index->source_table : probe_tbl;
      gi->src_chunks.reserve(n);
      gi->src_rows.reserve(n);
      if (sgi != nullptr) {
        for (uint32_t pr : p_rows) {
          const uint32_t tc = sgi->src_chunks[pr];
          const uint32_t tr = sgi->src_rows[pr];
          gi->src_chunks.push_back(tc);
          gi->src_rows.push_back(tr);
          if (tr == ChunkGatherIndex::kNullRow) {
            gi->has_null = true;
          }
        }
      } else {
        gi->src_chunks.assign(n, pc);
        gi->src_rows.insert(gi->src_rows.end(), p_rows.begin(), p_rows.end());
      }
      p_gis.push_back(std::move(gi));
    }

    std::vector<std::shared_ptr<ChunkGatherIndex>> b_gis(num_build_groups);
    for (size_t g = 0; g < num_build_groups; ++g) {
      if (build_group_used[g] == 0) {
        continue;
      }
      const size_t rc = build_group_repr[g];
      const bool flat = build_plan[rc].same_leaf_target;
      auto gi = std::make_shared<ChunkGatherIndex>();
      gi->source_table = flat ? build_plan[rc].leaf_owner : build_tbl;
      gi->src_chunks.reserve(n);
      gi->src_rows.reserve(n);
      for (uint32_t bid : b_ids) {
        if (bid == kColumnarEmptyBucket) {
          gi->src_chunks.push_back(0);
          gi->src_rows.push_back(ChunkGatherIndex::kNullRow);
          gi->has_null = true;
        } else {
          const uint32_t bc = build_chunk_of[bid];
          const uint32_t br = build_row_of[bid];
          if (flat && build_tbl->chunks[bc].columns[rc].encoding ==
                          ColumnarColumn::Encoding::kGathered) {
            const ChunkGatherIndex* sgi =
                build_tbl->chunks[bc].columns[rc].gather_index.get();
            const uint32_t tc = sgi->src_chunks[br];
            const uint32_t tr = sgi->src_rows[br];
            gi->src_chunks.push_back(tc);
            gi->src_rows.push_back(tr);
            if (tr == ChunkGatherIndex::kNullRow) {
              gi->has_null = true;
            }
          } else {
            gi->src_chunks.push_back(bc);
            gi->src_rows.push_back(br);
          }
        }
      }
      b_gis[g] = std::move(gi);
    }

    ColumnarChunk out_ch;
    out_ch.rows = n;
    out_ch.columns.resize(result_schema.ColumnCount());
    auto fill_probe_side = [&](size_t dst_offset) {
      for (size_t c = 0; c < n_probe_cols; ++c) {
        const ColumnarColumn& src_col = pchunk.columns[c];
        const ValueType vt = src_col.type != ValueType::kNull
                                 ? src_col.type
                                 : probe_schema.GetColumn(c).Type();
        if (src_col.encoding == ColumnarColumn::Encoding::kConstant &&
            (src_col.const_is_null || src_col.type == ValueType::kNull)) {
          out_ch.columns[dst_offset + c].SetConstantNull(
              vt, src_col.is_unsigned, src_col.is_interval, n);
          continue;
        }
        const auto& gi = p_gis[layout.group_of[c]];
        out_ch.columns[dst_offset + c].SetGathered(
            gi, layout.target_cols[c], vt, src_col.is_unsigned,
            src_col.is_interval, src_col.is_uniform,
            src_col.has_null || src_col.type == ValueType::kNull ||
                gi->has_null);
      }
    };
    auto fill_build_side = [&](size_t dst_offset) {
      for (size_t c = 0; c < build_schema.ColumnCount(); ++c) {
        const ColumnarColSummary& bs = build_plan[c];
        const ValueType vt = bs.common_type != ValueType::kNull
                                 ? bs.common_type
                                 : build_schema.GetColumn(c).Type();
        if (bs.all_null) {
          out_ch.columns[dst_offset + c].SetConstantNull(vt, bs.common_unsigned,
                                                         bs.common_interval, n);
          continue;
        }
        const size_t tcol = bs.same_leaf_target ? bs.leaf_col : c;
        const auto& gi = b_gis[build_groups[c]];
        out_ch.columns[dst_offset + c].SetGathered(
            gi, tcol, vt, bs.common_unsigned, bs.common_interval,
            bs.all_uniform, bs.any_null || gi->has_null);
      }
    };
    if (flip) {
      fill_build_side(0);
      fill_probe_side(build_schema.ColumnCount());
    } else {
      fill_probe_side(0);
      fill_build_side(n_probe_cols);
    }
    out_chunks->push_back(std::move(out_ch));
  };

  auto worker_fn = [&](size_t wid) {
    WorkerOutput& wout = worker_outputs[wid];
    SrcIntLeafCache probe_leaf_cache;
    Row scratch_row;
    if (has_active_residuals && !fast_res_scaled_cmp.active) {
      scratch_row.values_.resize(result_schema.ColumnCount());
    }
    Value key_scratch;
    std::string key_buf;
    std::vector<uint32_t> dict_heads;
    std::vector<uint32_t> match_p_rows;
    std::vector<uint32_t> match_b_ids;
    match_p_rows.reserve(4096);
    match_b_ids.reserve(4096);
    std::array<std::optional<JitPacked2x32HashProbe>, 16> packed_probe_cache;
    std::vector<int64_t> packed_tile_k0;
    std::vector<int64_t> packed_tile_k1;
    std::vector<uint8_t> packed_tile_n0;
    std::vector<uint8_t> packed_tile_n1;
    std::vector<uint32_t> packed_tile_heads;
    if (mode == KeyMode::kPacked2x32) {
      packed_tile_k0.resize(4096);
      packed_tile_k1.resize(4096);
      packed_tile_n0.resize(4096);
      packed_tile_n1.resize(4096);
      packed_tile_heads.resize(4096);
    }

    while (true) {
      // Only the cancellation flag is shared without the error lock.
      if (probe_failed.load(std::memory_order_relaxed)) {
        return;
      }
      const size_t tid = next_task.fetch_add(1, std::memory_order_relaxed);
      if (tid >= tasks.size()) {
        break;
      }
      const ChunkSliceTask task = tasks[tid];
      const ColumnarChunk& pchunk = probe_tbl->chunks[task.chunk_idx];
      match_p_rows.clear();
      match_b_ids.clear();

      auto flush_matches = [&]() {
        if (match_p_rows.size() >= kColumnarChunkRows) {
          size_t off = 0;
          while (match_p_rows.size() - off >= kColumnarChunkRows) {
            emit_matched_batch(
                task.chunk_idx,
                std::span<const uint32_t>(match_p_rows.data() + off,
                                          kColumnarChunkRows),
                std::span<const uint32_t>(match_b_ids.data() + off,
                                          kColumnarChunkRows),
                &wout.chunks);
            off += kColumnarChunkRows;
          }
          if (off > 0) {
            match_p_rows.erase(
                match_p_rows.begin(),
                match_p_rows.begin() + static_cast<ptrdiff_t>(off));
            match_b_ids.erase(
                match_b_ids.begin(),
                match_b_ids.begin() + static_cast<ptrdiff_t>(off));
          }
        }
      };

      auto handle_candidates = [&](uint32_t pr, uint32_t head) {
        bool matched = false;
        if (head != kColumnarEmptyBucket) {
          if (fast_res_scaled_cmp.active) {
            double pd = 0.0;
            if (pchunk.columns[fast_res_scaled_cmp.probe_col]
                    .TryReadNonNullDouble(pr, &pd)) {
              const slot_t rbcol = fast_res_scaled_cmp.build_col;
              const double sc = fast_res_scaled_cmp.scale;
              const bool p_lhs = fast_res_scaled_cmp.probe_is_lhs;
              const BinaryOperation rop = fast_res_scaled_cmp.op;
              for (uint32_t bid = head; bid != kColumnarEmptyBucket;
                   bid = next_in_bucket[bid]) {
                ++wout.comparisons;
                const uint32_t bc = build_chunk_of[bid];
                const uint32_t br = build_row_of[bid];
                double bd = 0.0;
                if (!build_tbl->chunks[bc].columns[rbcol].TryReadNonNullDouble(
                        br, &bd)) {
                  continue;
                }
                const double lhs_v = p_lhs ? pd : bd;
                const double rhs_v = sc * (p_lhs ? bd : pd);
                bool accepted = false;
                switch (rop) {
                  case BinaryOperation::kLessThan:
                    accepted = (lhs_v < rhs_v);
                    break;
                  case BinaryOperation::kLessThanEquals:
                    accepted = (lhs_v <= rhs_v);
                    break;
                  case BinaryOperation::kGreaterThan:
                    accepted = (lhs_v > rhs_v);
                    break;
                  case BinaryOperation::kGreaterThanEquals:
                    accepted = (lhs_v >= rhs_v);
                    break;
                  default:
                    break;
                }
                if (!accepted) {
                  continue;
                }
                matched = true;
                if (want_build_nulls &&
                    __atomic_load_n(&shared_build_matched[bid],
                                    __ATOMIC_RELAXED) == 0) {
                  __atomic_store_n(&shared_build_matched[bid], uint8_t{1},
                                   __ATOMIC_RELAXED);
                }
                match_p_rows.push_back(pr);
                match_b_ids.push_back(bid);
                if (match_p_rows.size() >= kColumnarChunkRows) {
                  flush_matches();
                }
              }
            } else {
              for (uint32_t bid = head; bid != kColumnarEmptyBucket;
                   bid = next_in_bucket[bid]) {
                ++wout.comparisons;
              }
            }
          } else {
            bool loaded_probe_res = false;
            for (uint32_t bid = head; bid != kColumnarEmptyBucket;
                 bid = next_in_bucket[bid]) {
              ++wout.comparisons;
              if (has_active_residuals) {
                if (!loaded_probe_res) {
                  for (slot_t ps : res_probe_slots) {
                    pchunk.columns[ps].WriteValueInto(
                        pr, &scratch_row.values_[res_probe_off + ps]);
                  }
                  loaded_probe_res = true;
                }
                const uint32_t bc = build_chunk_of[bid];
                const uint32_t br = build_row_of[bid];
                const ColumnarChunk& bchunk = build_tbl->chunks[bc];
                for (slot_t bs : res_build_slots) {
                  bchunk.columns[bs].WriteValueInto(
                      br, &scratch_row.values_[res_build_off + bs]);
                }
                bool accepted = true;
                for (const BytecodeProgram* program : active_residuals) {
                  auto val = TryEvaluateRowAuto(*program, scratch_row);
                  if (!val.HasValue()) {
                    std::scoped_lock lock(error_mutex);
                    if (first_error.ok()) {
                      first_error = val.GetStatus();
                      probe_failed.store(true, std::memory_order_relaxed);
                    }
                    return false;
                  }
                  if (!Truthy(val.Value())) {
                    accepted = false;
                    break;
                  }
                }
                if (!accepted) {
                  continue;
                }
              }
              matched = true;
              if (want_build_nulls &&
                  __atomic_load_n(&shared_build_matched[bid],
                                  __ATOMIC_RELAXED) == 0) {
                __atomic_store_n(&shared_build_matched[bid], uint8_t{1},
                                 __ATOMIC_RELAXED);
              }
              match_p_rows.push_back(pr);
              match_b_ids.push_back(bid);
              if (match_p_rows.size() >= kColumnarChunkRows) {
                flush_matches();
              }
            }
          }
        }
        if (!matched && probe_outer_emit) {
          match_p_rows.push_back(pr);
          match_b_ids.push_back(kColumnarEmptyBucket);
          if (match_p_rows.size() >= kColumnarChunkRows) {
            flush_matches();
          }
        }
        return true;
      };

      if (mode == KeyMode::kEmptyNull) {
        if (probe_outer_emit) {
          for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
            if (!handle_candidates(r, kColumnarEmptyBucket)) {
              return;
            }
          }
        }
      } else if (mode == KeyMode::kSingleInt64) {
        const ColumnarColumn& pcol = pchunk.columns[probe_cols[0]];
        const int64_t bias = probe_biases[0];
        if (pcol.encoding == ColumnarColumn::Encoding::kConstant) {
          uint32_t head = kColumnarEmptyBucket;
          if (!pcol.IsNull(0)) {
            const int64_t k =
                bias != 0 ? AddJoinKeyBias(pcol.IntAt(0), bias) : pcol.IntAt(0);
            if (const uint32_t* found = find_int(k)) {
              head = *found;
            }
          }
          if (head != kColumnarEmptyBucket || probe_outer_emit) {
            for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
              if (!handle_candidates(r, head)) {
                return;
              }
            }
          }
        } else if (pcol.encoding == ColumnarColumn::Encoding::kDictionary &&
                   pcol.dict != nullptr && pcol.DictEntryCount() <= 65536) {
          const size_t nd = pcol.DictEntryCount();
          dict_heads.assign(nd, kColumnarEmptyBucket);
          bool any_hit = false;
          for (size_t d = 0; d < nd; ++d) {
            const int64_t raw = pcol.dict->IntEntryAt(d);
            const int64_t k = bias != 0 ? AddJoinKeyBias(raw, bias) : raw;
            if (const uint32_t* found = find_int(k)) {
              dict_heads[d] = *found;
              any_hit = true;
            }
          }
          if (any_hit || probe_outer_emit) {
            for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
              const uint32_t code = pcol.DictCodeAt(r);
              const uint32_t head =
                  (pcol.has_null &&
                   (code == ColumnarColumn::kNullDictCode || code >= nd))
                      ? kColumnarEmptyBucket
                      : dict_heads[code];
              if (!handle_candidates(r, head)) {
                return;
              }
            }
          }
        } else if (pcol.encoding == ColumnarColumn::Encoding::kFlat &&
                   pcol.is_uniform && (!pcol.has_null || pcol.nulls.empty()) &&
                   pcol.type != ValueType::kNull) {
          const int64_t* ptr = pcol.ints.data();
          for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
            const int64_t k = bias != 0 ? AddJoinKeyBias(ptr[r], bias) : ptr[r];
            const uint32_t* found = find_int(k);
            if (!handle_candidates(r, found ? *found : kColumnarEmptyBucket)) {
              return;
            }
          }
        } else {
          std::array<int64_t, 64> tile_vals{};
          for (uint32_t r0 = task.row_begin; r0 < task.row_end; r0 += 64) {
            const uint32_t cnt = std::min<uint32_t>(64, task.row_end - r0);
            const uint64_t valid = DecodeIntTile64(
                pcol, r0, cnt, &probe_leaf_cache, tile_vals.data());
            const uint64_t full =
                cnt == 64 ? ~uint64_t{0} : ((uint64_t{1} << cnt) - 1);
            if (valid == full) {
              for (uint32_t i = 0; i < cnt; ++i) {
                const int64_t k = bias != 0 ? AddJoinKeyBias(tile_vals[i], bias)
                                            : tile_vals[i];
                const uint32_t* found = find_int(k);
                if (!handle_candidates(r0 + i,
                                       found ? *found : kColumnarEmptyBucket)) {
                  return;
                }
              }
            } else {
              for (uint32_t i = 0; i < cnt; ++i) {
                uint32_t head = kColumnarEmptyBucket;
                if ((valid & (uint64_t{1} << i)) != 0) {
                  const int64_t k = bias != 0
                                        ? AddJoinKeyBias(tile_vals[i], bias)
                                        : tile_vals[i];
                  if (const uint32_t* found = find_int(k)) {
                    head = *found;
                  }
                }
                if (!handle_candidates(r0 + i, head)) {
                  return;
                }
              }
            }
          }
        }
      } else if (mode == KeyMode::kPacked2x32) {
        const ColumnarColumn& pcol0 = pchunk.columns[probe_cols[0]];
        const ColumnarColumn& pcol1 = pchunk.columns[probe_cols[1]];
        const int64_t pb0 = probe_biases[0];
        const int64_t pb1 = probe_biases[1];
        const bool has_bias = (pb0 != 0 || pb1 != 0);
        auto prep_key_col = [&](const ColumnarColumn& col, uint32_t r0,
                                uint32_t cnt, int64_t* dst_k, uint8_t* dst_n,
                                const int64_t** out_k,
                                const uint8_t** out_n) -> bool {
          if (col.encoding == ColumnarColumn::Encoding::kFlat &&
              col.is_uniform && col.type != ValueType::kNull &&
              col.ints.size() >= r0 + cnt) {
            *out_k = col.ints.data() + r0;
            if (col.has_null && col.nulls.size() >= r0 + cnt) {
              *out_n = col.nulls.data() + r0;
              return true;
            }
            *out_n = nullptr;
            return false;
          }
          bool any_null = false;
          for (uint32_t off = 0; off < cnt; off += 64) {
            const uint32_t sub = std::min<uint32_t>(64, cnt - off);
            const uint64_t valid = DecodeIntTile64(
                col, r0 + off, sub, &probe_leaf_cache, dst_k + off);
            const uint64_t full =
                sub == 64 ? ~uint64_t{0} : ((uint64_t{1} << sub) - 1);
            if (valid == full) {
              std::memset(dst_n + off, 0, sub);
            } else {
              any_null = true;
              for (uint32_t b = 0; b < sub; ++b) {
                dst_n[off + b] =
                    static_cast<uint8_t>((valid & (uint64_t{1} << b)) == 0);
              }
            }
          }
          *out_k = dst_k;
          *out_n = any_null ? dst_n : nullptr;
          return any_null;
        };
        for (uint32_t r0 = task.row_begin; r0 < task.row_end; r0 += 4096) {
          const uint32_t cnt = std::min<uint32_t>(4096, task.row_end - r0);
          const int64_t* in_k0 = nullptr;
          const uint8_t* in_n0 = nullptr;
          const int64_t* in_k1 = nullptr;
          const uint8_t* in_n1 = nullptr;
          const bool has_n0 =
              prep_key_col(pcol0, r0, cnt, packed_tile_k0.data(),
                           packed_tile_n0.data(), &in_k0, &in_n0);
          const bool has_n1 =
              prep_key_col(pcol1, r0, cnt, packed_tile_k1.data(),
                           packed_tile_n1.data(), &in_k1, &in_n1);
          const auto cache_idx = static_cast<size_t>(
              (has_n0 ? 1U : 0U) | (has_n1 ? 2U : 0U) |
              (packed_has_bloom ? 4U : 0U) | (has_bias ? 8U : 0U));
          if (!packed_probe_cache[cache_idx].has_value()) {
            packed_probe_cache[cache_idx] = JitPacked2x32HashProbe::Compile(
                has_n0, has_n1, packed_has_bloom, has_bias);
          }
          if (packed_probe_cache[cache_idx].has_value()) {
            packed_probe_cache[cache_idx]->Probe(
                cnt, in_k0, in_n0, in_k1, in_n1, pb0, pb1,
                packed_has_bloom ? packed_bloom_words.data() : nullptr,
                packed_bloom_word_mask, packed_ht_keys.data(),
                packed_ht_heads.data(), packed_ht_mask,
                packed_tile_heads.data());
          } else {
            for (uint32_t i = 0; i < cnt; ++i) {
              uint32_t head = kColumnarEmptyBucket;
              if ((!has_n0 || in_n0[i] == 0) && (!has_n1 || in_n1[i] == 0)) {
                const int64_t v0 = has_bias && pb0 != 0
                                       ? AddJoinKeyBias(in_k0[i], pb0)
                                       : in_k0[i];
                const int64_t v1 = has_bias && pb1 != 0
                                       ? AddJoinKeyBias(in_k1[i], pb1)
                                       : in_k1[i];
                if (static_cast<int64_t>(static_cast<int32_t>(v0)) == v0 &&
                    static_cast<int64_t>(static_cast<int32_t>(v1)) == v1) {
                  const uint64_t packed =
                      (static_cast<uint64_t>(
                           static_cast<uint32_t>(static_cast<int32_t>(v0)))
                       << 32) |
                      static_cast<uint64_t>(
                          static_cast<uint32_t>(static_cast<int32_t>(v1)));
                  const uint64_t h = HashPacked2x32Key(packed);
                  auto slot = static_cast<size_t>(h & packed_ht_mask);
                  while (packed_ht_heads[slot] != kColumnarEmptyBucket) {
                    if (packed_ht_keys[slot] == packed) {
                      head = packed_ht_heads[slot];
                      break;
                    }
                    slot = (slot + 1) & packed_ht_mask;
                  }
                }
              }
              packed_tile_heads[i] = head;
            }
          }
          if (!has_active_residuals && !want_build_nulls) {
            for (uint32_t i = 0; i < cnt; ++i) {
              const uint32_t head = packed_tile_heads[i];
              if (head != kColumnarEmptyBucket) {
                for (uint32_t bid = head; bid != kColumnarEmptyBucket;
                     bid = next_in_bucket[bid]) {
                  ++wout.comparisons;
                  match_p_rows.push_back(r0 + i);
                  match_b_ids.push_back(bid);
                  if (match_p_rows.size() >= kColumnarChunkRows) {
                    flush_matches();
                  }
                }
              } else if (probe_outer_emit) {
                match_p_rows.push_back(r0 + i);
                match_b_ids.push_back(kColumnarEmptyBucket);
                if (match_p_rows.size() >= kColumnarChunkRows) {
                  flush_matches();
                }
              }
            }
          } else {
            for (uint32_t i = 0; i < cnt; ++i) {
              if (!handle_candidates(r0 + i, packed_tile_heads[i])) {
                return;
              }
            }
          }
        }
      } else if (mode == KeyMode::kMultiInt64) {
        for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
          bool has_null = false;
          CompositeIntKey k;
          k.count = static_cast<uint8_t>(probe_cols.size());
          for (size_t idx = 0; idx < probe_cols.size(); ++idx) {
            const ColumnarColumn& col = pchunk.columns[probe_cols[idx]];
            int64_t raw = 0;
            if (!col.TryReadNonNullInt(r, &raw)) {
              has_null = true;
              break;
            }
            k.parts[idx] = probe_biases[idx] != 0
                               ? AddJoinKeyBias(raw, probe_biases[idx])
                               : raw;
          }
          uint32_t head = kColumnarEmptyBucket;
          if (!has_null) {
            if (const uint32_t* found = find_comp(k)) {
              head = *found;
            }
          }
          if (!handle_candidates(r, head)) {
            return;
          }
        }
      } else if (mode == KeyMode::kSingleVarChar) {
        const ColumnarColumn& pcol = pchunk.columns[probe_cols[0]];
        if (pcol.encoding == ColumnarColumn::Encoding::kConstant) {
          uint32_t head = kColumnarEmptyBucket;
          if (!pcol.IsNull(0)) {
            if (const uint32_t* found = find_str(pcol.StringAt(0))) {
              head = *found;
            }
          }
          if (head != kColumnarEmptyBucket || probe_outer_emit) {
            for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
              if (!handle_candidates(r, head)) {
                return;
              }
            }
          }
        } else if (pcol.encoding == ColumnarColumn::Encoding::kDictionary &&
                   pcol.dict != nullptr && pcol.DictEntryCount() <= 65536) {
          const size_t nd = pcol.DictEntryCount();
          dict_heads.assign(nd, kColumnarEmptyBucket);
          bool any_hit = false;
          for (size_t d = 0; d < nd; ++d) {
            if (const uint32_t* found = find_str(pcol.dict->StringEntryAt(d))) {
              dict_heads[d] = *found;
              any_hit = true;
            }
          }
          if (any_hit || probe_outer_emit) {
            for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
              const uint32_t code = pcol.DictCodeAt(r);
              const uint32_t head =
                  (pcol.has_null &&
                   (code == ColumnarColumn::kNullDictCode || code >= nd))
                      ? kColumnarEmptyBucket
                      : dict_heads[code];
              if (!handle_candidates(r, head)) {
                return;
              }
            }
          }
        } else if (pcol.encoding == ColumnarColumn::Encoding::kFlat &&
                   pcol.is_uniform && !pcol.has_null) {
          const std::string_view* ptr = pcol.strings.data();
          for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
            const uint32_t* found = find_str(ptr[r]);
            if (!handle_candidates(r, found ? *found : kColumnarEmptyBucket)) {
              return;
            }
          }
        } else {
          for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
            uint32_t head = kColumnarEmptyBucket;
            std::string_view sv;
            if (pcol.TryReadNonNullString(r, &sv)) {
              if (const uint32_t* found = find_str(sv)) {
                head = *found;
              }
            }
            if (!handle_candidates(r, head)) {
              return;
            }
          }
        }
      } else {
        for (uint32_t r = task.row_begin; r < task.row_end; ++r) {
          uint32_t head = kColumnarEmptyBucket;
          if (TryColumnarEncodeJoinKeyNonNull(pchunk, probe_cols, r, true,
                                              &probe_biases, &key_scratch,
                                              &key_buf)) {
            if (const uint32_t* found = find_str(key_buf)) {
              head = *found;
            }
          }
          if (!handle_candidates(r, head)) {
            return;
          }
        }
      }

      if (!match_p_rows.empty()) {
        emit_matched_batch(task.chunk_idx, match_p_rows, match_b_ids,
                           &wout.chunks);
      }
    }
  };

  if (workers > 1) {
    RETURN_IF_FAIL(RunParallelTasks(workers, worker_fn));
  } else {
    worker_fn(0);
  }
  RETURN_IF_FAIL(first_error);

  auto out_table = std::make_shared<ColumnarTable>();
  uint64_t total_comparisons = 0;
  for (WorkerOutput& wo : worker_outputs) {
    total_comparisons += wo.comparisons;
    for (ColumnarChunk& ch : wo.chunks) {
      if (ch.rows == 0) {
        continue;
      }
      out_table->total_rows += ch.rows;
      out_table->chunks.push_back(std::move(ch));
    }
  }

  if (want_build_nulls) {
    auto emit_unmatched_slice = [&](std::span<const uint32_t> batch_bids,
                                    std::vector<ColumnarChunk>* dst) {
      const size_t batch_n = batch_bids.size();
      if (batch_n == 0) {
        return;
      }
      std::vector<std::shared_ptr<ChunkGatherIndex>> b_gis(num_build_groups);
      for (size_t g = 0; g < num_build_groups; ++g) {
        if (build_group_used[g] == 0) {
          continue;
        }
        const size_t rc = build_group_repr[g];
        const bool flat = build_plan[rc].same_leaf_target;
        auto gi = std::make_shared<ChunkGatherIndex>();
        gi->source_table = flat ? build_plan[rc].leaf_owner : build_tbl;
        gi->src_chunks.reserve(batch_n);
        gi->src_rows.reserve(batch_n);
        for (uint32_t bid : batch_bids) {
          const uint32_t bc = build_chunk_of[bid];
          const uint32_t br = build_row_of[bid];
          if (flat && build_tbl->chunks[bc].columns[rc].encoding ==
                          ColumnarColumn::Encoding::kGathered) {
            const ChunkGatherIndex* sgi =
                build_tbl->chunks[bc].columns[rc].gather_index.get();
            const uint32_t tc = sgi->src_chunks[br];
            const uint32_t tr = sgi->src_rows[br];
            gi->src_chunks.push_back(tc);
            gi->src_rows.push_back(tr);
            if (tr == ChunkGatherIndex::kNullRow) {
              gi->has_null = true;
            }
          } else {
            gi->src_chunks.push_back(bc);
            gi->src_rows.push_back(br);
          }
        }
        b_gis[g] = std::move(gi);
      }
      ColumnarChunk uch;
      uch.rows = batch_n;
      if (flip) {
        uch.columns.resize(build_schema.ColumnCount());
        for (size_t c = 0; c < build_schema.ColumnCount(); ++c) {
          const ColumnarColSummary& bs = build_plan[c];
          const ValueType vt = bs.common_type != ValueType::kNull
                                   ? bs.common_type
                                   : build_schema.GetColumn(c).Type();
          if (bs.all_null) {
            uch.columns[c].SetConstantNull(vt, bs.common_unsigned,
                                           bs.common_interval, batch_n);
          } else {
            const size_t tcol = bs.same_leaf_target ? bs.leaf_col : c;
            const auto& gi = b_gis[build_groups[c]];
            uch.columns[c].SetGathered(gi, tcol, vt, bs.common_unsigned,
                                       bs.common_interval, bs.all_uniform,
                                       bs.any_null || gi->has_null);
          }
        }
        AppendNullColumnsToChunk(batch_n, probe_schema, &uch);
      } else {
        AppendNullColumnsToChunk(batch_n, probe_schema, &uch);
        const size_t base_col = probe_schema.ColumnCount();
        uch.columns.resize(base_col + build_schema.ColumnCount());
        for (size_t c = 0; c < build_schema.ColumnCount(); ++c) {
          const ColumnarColSummary& bs = build_plan[c];
          const ValueType vt = bs.common_type != ValueType::kNull
                                   ? bs.common_type
                                   : build_schema.GetColumn(c).Type();
          if (bs.all_null) {
            uch.columns[base_col + c].SetConstantNull(
                vt, bs.common_unsigned, bs.common_interval, batch_n);
          } else {
            const size_t tcol = bs.same_leaf_target ? bs.leaf_col : c;
            const auto& gi = b_gis[build_groups[c]];
            uch.columns[base_col + c].SetGathered(
                gi, tcol, vt, bs.common_unsigned, bs.common_interval,
                bs.all_uniform, bs.any_null || gi->has_null);
          }
        }
      }
      dst->push_back(std::move(uch));
    };

    const size_t u_workers = (!probe_disabled && max_w > 1 && n_build >= 16384)
                                 ? std::min<size_t>({kParallelProbeMaxWorkers,
                                                     max_w, n_build / 4096})
                                 : 1;
    if (u_workers > 1) {
      std::vector<std::vector<ColumnarChunk>> u_chunks(u_workers);
      RETURN_IF_FAIL(RunParallelTasks(u_workers, [&](size_t w) {
        const size_t lo = (n_build * w) / u_workers;
        const size_t hi = (n_build * (w + 1)) / u_workers;
        std::vector<uint32_t> batch;
        batch.reserve(kColumnarChunkRows);
        for (size_t i = lo; i < hi; ++i) {
          if (shared_build_matched[i] == 0) {
            batch.push_back(static_cast<uint32_t>(i));
            if (batch.size() == kColumnarChunkRows) {
              emit_unmatched_slice(batch, &u_chunks[w]);
              batch.clear();
            }
          }
        }
        if (!batch.empty()) {
          emit_unmatched_slice(batch, &u_chunks[w]);
        }
      }));
      for (auto& vec : u_chunks) {
        for (ColumnarChunk& ch : vec) {
          out_table->total_rows += ch.rows;
          out_table->chunks.push_back(std::move(ch));
        }
      }
    } else {
      std::vector<uint32_t> batch;
      batch.reserve(std::min<size_t>(kColumnarChunkRows, n_build));
      for (uint32_t i = 0; i < n_build; ++i) {
        if (shared_build_matched[i] == 0) {
          batch.push_back(i);
          if (batch.size() == kColumnarChunkRows) {
            emit_unmatched_slice(batch, &out_table->chunks);
            out_table->total_rows += kColumnarChunkRows;
            batch.clear();
          }
        }
      }
      if (!batch.empty()) {
        out_table->total_rows += batch.size();
        emit_unmatched_slice(batch, &out_table->chunks);
      }
    }
  }

  return std::optional<Relation>{FinalizeColumnarJoinResult(
      context, result_schema, std::move(using_columns), stats_seed,
      total_comparisons, std::move(out_table))};
}

}  // namespace tinylamb::relational_detail
