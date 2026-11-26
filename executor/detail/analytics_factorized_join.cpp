/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/analytics_factorized_join.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/parallel_work.hpp"
#include "common/platform/process.hpp"
#include "common/query_parallelism.hpp"
#include "common/status_or.hpp"
#include "common/string_util.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/fused_grouped_join.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/expression.hpp"
#include "expression/parameter_slot.hpp"
#include "type/collation.hpp"
#include "type/column_name.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {
namespace {

constexpr size_t kMaxCollapsedAggs = 16;
constexpr size_t kMaxDenseKeySpan = 65536;
constexpr uint32_t kAbsentSlot = std::numeric_limits<uint32_t>::max();

struct TransparentStringHash {
  using is_transparent = void;
  size_t operator()(std::string_view sv) const noexcept {
    return std::hash<std::string_view>{}(sv);
  }
};

using TransparentStringSlotMap =
    std::unordered_map<std::string, uint32_t, TransparentStringHash,
                       std::equal_to<>>;

enum class SimpleAggKind : uint8_t {
  kCountStar = 0,
  kCount,
  kSum,
  kAvg,
  kMin,
  kMax,
};

struct AggSlotState {
  uint64_t count{0};
  __int128 sum_i128{0};
  __int128 pos_i128{0};
  __int128 neg_i128{0};
  double sum_dbl{0.0};
  Value extremum;
  bool has_extremum{false};
  // 0 = none observed, 1 = signed int64, 2 = double
  uint8_t numeric_mode{0};
  bool invalid{false};
};

inline bool AddValueWithWeight(AggSlotState* st, SimpleAggKind kind,
                               const Value& val, uint64_t weight) noexcept {
  if (st->invalid || weight == 0) {
    return !st->invalid;
  }
  if (kind == SimpleAggKind::kCountStar) {
    st->count += weight;
    return true;
  }
  if (val.IsNull()) {
    return true;
  }
  if (kind == SimpleAggKind::kCount) {
    st->count += weight;
    return true;
  }
  if (kind == SimpleAggKind::kSum || kind == SimpleAggKind::kAvg) {
    if (val.type == ValueType::kInt64 && !val.IsUnsigned() &&
        !val.IsInterval()) {
      if (st->numeric_mode == 0) {
        st->numeric_mode = 1;
      } else if (st->numeric_mode != 1) {
        st->invalid = true;
        return false;
      }
      const __int128 prod = static_cast<__int128>(val.value.int_value) *
                            static_cast<__int128>(weight);
      st->sum_i128 += prod;
      if (prod > 0) {
        st->pos_i128 += prod;
      } else if (prod < 0) {
        st->neg_i128 += prod;
      }
      st->count += weight;
      return true;
    }
    if (val.type == ValueType::kDouble && !std::isnan(val.value.double_value) &&
        std::isfinite(val.value.double_value)) {
      if (st->numeric_mode == 0) {
        st->numeric_mode = 2;
      } else if (st->numeric_mode != 2) {
        st->invalid = true;
        return false;
      }
      st->sum_dbl += val.value.double_value * static_cast<double>(weight);
      if (!std::isfinite(st->sum_dbl)) {
        st->invalid = true;
        return false;
      }
      st->count += weight;
      return true;
    }
    st->invalid = true;
    return false;
  }
  // MIN / MAX (idempotent under weight >= 1).
  if (val.IsCaseInsensitive() || val.IsInterval() ||
      val.type == ValueType::kArray ||
      (val.type == ValueType::kDouble && std::isnan(val.value.double_value))) {
    st->invalid = true;
    return false;
  }
  if (!st->has_extremum) {
    st->extremum = val;
    st->has_extremum = true;
    return true;
  }
  if (st->extremum.type != val.type ||
      st->extremum.IsUnsigned() != val.IsUnsigned()) {
    st->invalid = true;
    return false;
  }
  if (kind == SimpleAggKind::kMin) {
    if (val < st->extremum) {
      st->extremum = val;
    }
  } else {
    if (val > st->extremum) {
      st->extremum = val;
    }
  }
  return true;
}

inline bool AddEntryCellWithWeight(AggSlotState* st, SimpleAggKind kind,
                                   const ColumnarChunk* chunk, const Row* row,
                                   size_t r, slot_t col,
                                   uint64_t weight) noexcept {
  if (st->invalid || weight == 0) {
    return !st->invalid;
  }
  if (kind == SimpleAggKind::kCountStar) {
    st->count += weight;
    return true;
  }
  if (row != nullptr) {
    return AddValueWithWeight(st, kind, (*row)[col], weight);
  }
  const ColumnarColumn& cc = chunk->columns[col];
  if ((kind == SimpleAggKind::kSum || kind == SimpleAggKind::kAvg) &&
      cc.is_uniform && !cc.is_unsigned && !cc.is_interval) {
    if (cc.type == ValueType::kInt64) {
      int64_t iv = 0;
      if (!cc.TryReadNonNullInt(r, &iv)) {
        return true;
      }
      if (st->numeric_mode == 0) {
        st->numeric_mode = 1;
      } else if (st->numeric_mode != 1) {
        st->invalid = true;
        return false;
      }
      const __int128 prod =
          static_cast<__int128>(iv) * static_cast<__int128>(weight);
      st->sum_i128 += prod;
      if (prod > 0) {
        st->pos_i128 += prod;
      } else if (prod < 0) {
        st->neg_i128 += prod;
      }
      st->count += weight;
      return true;
    }
    if (cc.type == ValueType::kDouble) {
      double dv = 0.0;
      if (!cc.TryReadNonNullDouble(r, &dv)) {
        return true;
      }
      if (std::isnan(dv) || !std::isfinite(dv)) {
        st->invalid = true;
        return false;
      }
      if (st->numeric_mode == 0) {
        st->numeric_mode = 2;
      } else if (st->numeric_mode != 2) {
        st->invalid = true;
        return false;
      }
      st->sum_dbl += dv * static_cast<double>(weight);
      if (!std::isfinite(st->sum_dbl)) {
        st->invalid = true;
        return false;
      }
      st->count += weight;
      return true;
    }
  }
  Value tmp;
  cc.WriteValueInto(r, &tmp);
  return AddValueWithWeight(st, kind, tmp, weight);
}

inline bool MergeAggSlot(AggSlotState* dst, const AggSlotState& src,
                         SimpleAggKind kind, uint64_t scale = 1) noexcept {
  if (dst->invalid || src.invalid) {
    dst->invalid = true;
    return false;
  }
  if (scale == 0) {
    return true;
  }
  if (kind == SimpleAggKind::kCountStar || kind == SimpleAggKind::kCount) {
    dst->count += src.count * scale;
    return true;
  }
  if (kind == SimpleAggKind::kSum || kind == SimpleAggKind::kAvg) {
    if (src.numeric_mode == 0) {
      return true;
    }
    if (dst->numeric_mode == 0) {
      dst->numeric_mode = src.numeric_mode;
    } else if (dst->numeric_mode != src.numeric_mode) {
      dst->invalid = true;
      return false;
    }
    if (src.numeric_mode == 1) {
      const auto sc = static_cast<__int128>(scale);
      dst->sum_i128 += src.sum_i128 * sc;
      dst->pos_i128 += src.pos_i128 * sc;
      dst->neg_i128 += src.neg_i128 * sc;
      dst->count += src.count * scale;
      return true;
    }
    dst->sum_dbl += src.sum_dbl * static_cast<double>(scale);
    if (!std::isfinite(dst->sum_dbl)) {
      dst->invalid = true;
      return false;
    }
    dst->count += src.count * scale;
    return true;
  }
  if (!src.has_extremum) {
    return true;
  }
  if (!dst->has_extremum) {
    dst->extremum = src.extremum;
    dst->has_extremum = true;
    return true;
  }
  if (dst->extremum.type != src.extremum.type ||
      dst->extremum.IsUnsigned() != src.extremum.IsUnsigned()) {
    dst->invalid = true;
    return false;
  }
  if (kind == SimpleAggKind::kMin) {
    if (src.extremum < dst->extremum) {
      dst->extremum = src.extremum;
    }
  } else {
    if (src.extremum > dst->extremum) {
      dst->extremum = src.extremum;
    }
  }
  return true;
}

// Lean proof:
// lean/TinylambOpt/ResearchProofs/SemijoinGuardedCheckedEagerSum.lean (sound)
// Conservative checked finish: rejects (returns false -> fall back to checked
// generic execution) if even the positive partial sum or negative partial sum
// exceeds signed INT64 bounds.
inline bool FinishAggSlot(const AggSlotState& st, SimpleAggKind kind,
                          Value* out) noexcept {
  if (st.invalid) {
    return false;
  }
  switch (kind) {
    case SimpleAggKind::kCountStar:
    case SimpleAggKind::kCount:
      *out = Value(static_cast<int64_t>(st.count));
      return true;
    case SimpleAggKind::kSum: {
      if (st.count == 0 || st.numeric_mode == 0) {
        *out = Value();
        return true;
      }
      if (st.numeric_mode == 1) {
        const __int128 max64 = std::numeric_limits<int64_t>::max();
        const __int128 min64 = std::numeric_limits<int64_t>::min();
        if (st.pos_i128 > max64 || st.neg_i128 < min64 || st.sum_i128 > max64 ||
            st.sum_i128 < min64) {
          return false;
        }
        *out = Value(static_cast<int64_t>(st.sum_i128));
        return true;
      }
      if (!std::isfinite(st.sum_dbl)) {
        return false;
      }
      *out = Value(st.sum_dbl);
      return true;
    }
    case SimpleAggKind::kAvg: {
      if (st.count == 0 || st.numeric_mode == 0) {
        *out = Value();
        return true;
      }
      if (st.numeric_mode == 1) {
        const __int128 max64 = std::numeric_limits<int64_t>::max();
        const __int128 min64 = std::numeric_limits<int64_t>::min();
        if (st.pos_i128 > max64 || st.neg_i128 < min64 || st.sum_i128 > max64 ||
            st.sum_i128 < min64) {
          return false;
        }
        *out = Value(static_cast<double>(static_cast<long double>(st.sum_i128) /
                                         static_cast<long double>(st.count)));
        return true;
      }
      if (!std::isfinite(st.sum_dbl)) {
        return false;
      }
      *out = Value(st.sum_dbl / static_cast<double>(st.count));
      return true;
    }
    case SimpleAggKind::kMin:
    case SimpleAggKind::kMax:
      *out = st.has_extremum ? st.extremum : Value();
      return true;
  }
  return false;
}

std::optional<SimpleAggKind> ClassifySimpleAgg(const AggregateExpression* agg,
                                               bool is_count_star) noexcept {
  if (is_count_star) {
    return SimpleAggKind::kCountStar;
  }
  if (agg == nullptr || agg->Distinct() || agg->WhereFilter() ||
      agg->Having() != AggregateHavingModifier::kNone ||
      !agg->InnerOrderBy().empty() || agg->SecondaryArg() ||
      !agg->ExtraArgs().empty() || !agg->TrailingArgs().empty() ||
      !agg->Child() || agg->NeedsGroupContext() || agg->HasInnerGroupBy()) {
    return std::nullopt;
  }
  switch (agg->GetType()) {
    case AggregationType::kCount:
      return SimpleAggKind::kCount;
    case AggregationType::kSum:
      return SimpleAggKind::kSum;
    case AggregationType::kAvg:
      return SimpleAggKind::kAvg;
    case AggregationType::kMin:
      return SimpleAggKind::kMin;
    case AggregationType::kMax:
      return SimpleAggKind::kMax;
    default:
      return std::nullopt;
  }
}

void RetainColumnarKeepalives(TransactionContext& context,
                              const Relation& rel) {
  if (rel.HasColumnar() && rel.columnar != nullptr &&
      context.execution_runtime() != nullptr) {
    context.execution_runtime()->retained_scan_stores.push_back(rel.columnar);
  }
}

void EnsureOwnedStringValues(Row* row) {
  for (Value& v : row->values_) {
    if (v.type == ValueType::kVarChar && v.owned_data.empty() &&
        !v.value.varchar_value.empty()) {
      v.owned_data.assign(v.value.varchar_value);
      v.value.varchar_value = v.owned_data;
    }
  }
}

inline bool ReadColValueOrRow(const ColumnarChunk* chunk, const Row* row,
                              size_t chunk_row, slot_t col,
                              Value* out) noexcept {
  if (row != nullptr) {
    *out = (*row)[col];
    return true;
  }
  chunk->columns[col].WriteValueInto(chunk_row, out);
  return true;
}

template <typename Fn>
bool ForEachRelationEntry(const Relation& rel, Fn&& fn) {
  if (rel.HasSpill()) {
    return false;
  }
  if (!rel.HasColumnarOnly()) {
    for (size_t i = 0; i < rel.rows.size(); ++i) {
      if (!fn(nullptr, &rel.rows[i], i)) {
        return false;
      }
    }
    return true;
  }
  for (const ColumnarChunk& chunk : rel.columnar->chunks) {
    for (size_t r = 0; r < chunk.rows; ++r) {
      if (!fn(&chunk, nullptr, r)) {
        return false;
      }
    }
  }
  return true;
}

template <typename Fn>
Status ParallelForEachRelationEntryRange(const Relation& rel, size_t workers,
                                         Fn&& worker_fn) {
  const size_t total = rel.TotalRows();
  if (total == 0) {
    return Status::kSuccess;
  }
  if (!rel.HasColumnarOnly()) {
    const auto run_w = [&](size_t w) {
      const size_t begin = (total * w) / workers;
      const size_t end = (total * (w + 1)) / workers;
      worker_fn(w, [&](auto&& entry_cb) {
        for (size_t ri = begin; ri < end; ++ri) {
          if (!entry_cb(nullptr, &rel.rows[ri], ri)) {
            return;
          }
        }
      });
    };
    if (workers <= 1) {
      run_w(0);
      return Status::kSuccess;
    }
    return RunParallelTasks(workers, run_w);
  }
  const auto& chunks = rel.columnar->chunks;
  const size_t chunk_count = chunks.size();
  const size_t actual_workers =
      std::min(workers, std::max<size_t>(1, chunk_count));
  const auto run_w = [&](size_t w) {
    const size_t c_begin = (chunk_count * w) / actual_workers;
    const size_t c_end = (chunk_count * (w + 1)) / actual_workers;
    worker_fn(w, [&](auto&& entry_cb) {
      for (size_t ci = c_begin; ci < c_end; ++ci) {
        const ColumnarChunk& chunk = chunks[ci];
        for (size_t r = 0; r < chunk.rows; ++r) {
          if (!entry_cb(&chunk, nullptr, r)) {
            return;
          }
        }
      }
    });
  };
  if (actual_workers <= 1) {
    run_w(0);
    return Status::kSuccess;
  }
  return RunParallelTasks(actual_workers, run_w);
}

inline bool ReadIntKeyFromEntry(const ColumnarChunk* chunk, const Row* row,
                                size_t r, slot_t col, ValueType expected_type,
                                int64_t* out, bool* invalid_cell) noexcept {
  if (row != nullptr) {
    const Value& v = (*row)[col];
    if (v.IsNull()) {
      return false;
    }
    if (v.type != expected_type || v.IsUnsigned() || v.IsInterval()) {
      *invalid_cell = true;
      return false;
    }
    *out = v.value.int_value;
    return true;
  }
  const ColumnarColumn& cc = chunk->columns[col];
  if (cc.is_uniform && !cc.is_unsigned && !cc.is_interval &&
      cc.type == expected_type) {
    return cc.TryReadNonNullInt(r, out);
  }
  Value tmp;
  cc.WriteValueInto(r, &tmp);
  if (tmp.IsNull()) {
    return false;
  }
  if (tmp.type != expected_type || tmp.IsUnsigned() || tmp.IsInterval()) {
    *invalid_cell = true;
    return false;
  }
  *out = tmp.value.int_value;
  return true;
}

struct DecodedIntColumnView {
  const int64_t* values{nullptr};
  const uint8_t* nulls{nullptr};
  bool all_null{false};
  bool is_const{false};
  int64_t const_val{0};
  bool ok{false};
};

inline DecodedIntColumnView DecodeIntColumnChunk(
    const ColumnarColumn& cc, size_t n_rows, ValueType expected_type,
    std::vector<int64_t>* val_scratch,
    std::vector<uint8_t>* null_scratch) noexcept {
  DecodedIntColumnView view;
  if (!cc.is_uniform || cc.is_unsigned || cc.is_interval ||
      cc.type != expected_type) {
    return view;
  }
  switch (cc.encoding) {
    case ColumnarColumn::Encoding::kFlat:
      if (cc.ints.size() < n_rows) {
        return view;
      }
      view.values = cc.ints.data();
      view.nulls = (cc.has_null && cc.nulls.size() >= n_rows) ? cc.nulls.data()
                                                              : nullptr;
      view.ok = true;
      return view;
    case ColumnarColumn::Encoding::kConstant:
      view.all_null = cc.const_is_null;
      view.is_const = true;
      view.const_val = cc.const_int;
      view.ok = true;
      return view;
    case ColumnarColumn::Encoding::kFrameOfReference: {
      const uint8_t* nulls = (cc.has_null && cc.borrowed_nulls != nullptr)
                                 ? cc.borrowed_nulls
                                 : nullptr;
      if (cc.for_width == 0) {
        if (nulls == nullptr) {
          view.is_const = true;
          view.const_val = cc.for_base;
          view.ok = true;
          return view;
        }
        val_scratch->assign(n_rows, cc.for_base);
        view.values = val_scratch->data();
        view.nulls = nulls;
        view.ok = true;
        return view;
      }
      if (cc.for_bytes == nullptr) {
        return view;
      }
      val_scratch->resize(n_rows);
      int64_t* dst = val_scratch->data();
      const auto base = static_cast<uint64_t>(cc.for_base);
      const uint8_t* src = cc.for_bytes;
      switch (cc.for_width) {
        case 1:
          for (size_t r = 0; r < n_rows; ++r) {
            dst[r] = static_cast<int64_t>(base + src[r]);
          }
          break;
        case 2:
          for (size_t r = 0; r < n_rows; ++r) {
            uint16_t v = 0;
            std::memcpy(&v, src + (r * sizeof(v)), sizeof(v));
            dst[r] = static_cast<int64_t>(base + v);
          }
          break;
        case 4:
          for (size_t r = 0; r < n_rows; ++r) {
            uint32_t v = 0;
            std::memcpy(&v, src + (r * sizeof(v)), sizeof(v));
            dst[r] = static_cast<int64_t>(base + v);
          }
          break;
        case 8:
          std::memcpy(dst, src, n_rows * sizeof(int64_t));
          break;
        default:
          return view;
      }
      view.values = dst;
      view.nulls = nulls;
      view.ok = true;
      return view;
    }
    case ColumnarColumn::Encoding::kDictionary: {
      if (cc.dict == nullptr) {
        return view;
      }
      val_scratch->resize(n_rows);
      int64_t* dst = val_scratch->data();
      if (!cc.has_null && cc.borrowed_dict_u8 != nullptr &&
          cc.borrowed_count >= n_rows) {
        const uint8_t* codes = cc.borrowed_dict_u8;
        for (size_t r = 0; r < n_rows; ++r) {
          dst[r] = cc.dict->IntEntryAt(codes[r]);
        }
        view.values = dst;
        view.ok = true;
        return view;
      }
      if (!cc.has_null && cc.dict_codes.size() >= n_rows) {
        const uint32_t* codes = cc.dict_codes.data();
        for (size_t r = 0; r < n_rows; ++r) {
          dst[r] = cc.dict->IntEntryAt(codes[r]);
        }
        view.values = dst;
        view.ok = true;
        return view;
      }
      null_scratch->assign(n_rows, 0);
      uint8_t* ndst = null_scratch->data();
      bool any_null = false;
      for (size_t r = 0; r < n_rows; ++r) {
        const uint32_t code = cc.CheckedDictCodeAt(r);
        if (code == ColumnarColumn::kNullDictCode) {
          ndst[r] = 1;
          any_null = true;
        } else {
          dst[r] = cc.dict->IntEntryAt(code);
        }
      }
      view.values = dst;
      view.nulls = any_null ? ndst : nullptr;
      view.ok = true;
      return view;
    }
    case ColumnarColumn::Encoding::kGathered:
    default:
      return view;
  }
}

inline bool ReadStringKeyFromEntry(const ColumnarChunk* chunk, const Row* row,
                                   size_t r, slot_t col, std::string_view* out,
                                   Value* owned_tmp,
                                   bool* invalid_cell) noexcept {
  if (row != nullptr) {
    const Value& v = (*row)[col];
    if (v.IsNull()) {
      return false;
    }
    if (v.type != ValueType::kVarChar || v.IsCaseInsensitive() ||
        v.IsInterval()) {
      *invalid_cell = true;
      return false;
    }
    *out = v.value.varchar_value;
    return true;
  }
  const ColumnarColumn& cc = chunk->columns[col];
  if (cc.is_uniform && !cc.is_unsigned && !cc.is_interval &&
      cc.type == ValueType::kVarChar && chunk->rows > 0) {
    // Verify fallback / collation tags only when not guaranteed binary uniform.
    if (cc.IsFlat() || cc.encoding == ColumnarColumn::Encoding::kDictionary ||
        cc.encoding == ColumnarColumn::Encoding::kConstant) {
      return cc.TryReadNonNullString(r, out);
    }
  }
  cc.WriteValueInto(r, owned_tmp);
  if (owned_tmp->IsNull()) {
    return false;
  }
  if (owned_tmp->type != ValueType::kVarChar ||
      owned_tmp->IsCaseInsensitive() || owned_tmp->IsInterval()) {
    *invalid_cell = true;
    return false;
  }
  *out = owned_tmp->value.varchar_value;
  return true;
}

inline bool ReadIntKeyCell(const Value& v, ValueType expected_type,
                           int64_t* out, bool* invalid_cell) noexcept {
  if (v.IsNull()) {
    return false;
  }
  if (v.type != expected_type || v.IsUnsigned() || v.IsInterval()) {
    *invalid_cell = true;
    return false;
  }
  *out = v.value.int_value;
  return true;
}

// Checks whether an expression (outside AggregateExpression subtrees) ONLY
// reads columns allowed by `allowed_pred(col_name)`.
bool AllNonAggColumnsAllowed(
    const Expression& expr,
    const std::function<bool(const ColumnName&)>& allowed_pred) {
  if (!expr) {
    return true;
  }
  if (expr->Type() == TypeTag::kQueryExp) {
    return false;
  }
  if (expr->Type() == TypeTag::kAggregateExp) {
    return true;
  }
  if (expr->Type() == TypeTag::kColumnValue) {
    const ColumnName& col = expr->AsColumnValue().GetColumnName();
    if (col.name == "*") {
      return false;
    }
    return allowed_pred(col);
  }
  return std::ranges::all_of(
      ExpressionChildren(expr), [&](const Expression& child) {
        return AllNonAggColumnsAllowed(child, allowed_pred);
      });
}

bool HasSingleEquiKeyCompatible(const Column& lc, const Column& rc) noexcept {
  if (lc.Type() != rc.Type() || lc.IsUnsigned() || rc.IsUnsigned() ||
      lc.IsInterval() || rc.IsInterval()) {
    return false;
  }
  if (lc.Type() == ValueType::kInt64 || lc.Type() == ValueType::kDate) {
    return true;
  }
  return lc.Type() == ValueType::kVarChar &&
         lc.GetTextDomain() == rc.GetTextDomain();
}

std::string EncodeKeyTuple(const std::vector<Value>& vals) {
  std::string out;
  for (const Value& v : vals) {
    if (v.IsNull()) {
      out.push_back('\x00');
      continue;
    }
    out.push_back(static_cast<char>(static_cast<uint8_t>(v.type) + 1));
    out.push_back(v.IsUnsigned() ? '\x01' : '\x00');
    switch (v.type) {
      case ValueType::kInt64:
      case ValueType::kDate: {
        const auto u = static_cast<uint64_t>(v.value.int_value);
        out.append(reinterpret_cast<const char*>(&u), sizeof(u));
        break;
      }
      case ValueType::kDouble: {
        double d = v.value.double_value;
        if (d == 0.0) {
          d = 0.0;  // normalize -0.0 == +0.0
        }
        uint64_t u = 0;
        std::memcpy(&u, &d, sizeof(u));
        out.append(reinterpret_cast<const char*>(&u), sizeof(u));
        break;
      }
      case ValueType::kVarChar: {
        const auto len = static_cast<uint32_t>(v.value.varchar_value.size());
        out.append(reinterpret_cast<const char*>(&len), sizeof(len));
        out.append(v.value.varchar_value);
        break;
      }
      default: {
        const std::string s = v.AsString();
        const auto len = static_cast<uint32_t>(s.size());
        out.append(reinterpret_cast<const char*>(&len), sizeof(len));
        out.append(s);
        break;
      }
    }
  }
  return out;
}

StatusOr<Relation> EmitGroupedProjectionRelation(
    TransactionContext& context, const Schema& rep_schema,
    const std::vector<Row>& rep_rows,
    const std::vector<std::vector<AggSlotState>>& group_aggs,
    const std::vector<SimpleAggKind>& agg_kinds,
    const std::optional<Row>& empty_scalar_rep_row,
    const GroupedJoinFusionSpec& fusion, size_t joins_delta,
    size_t comparisons_delta, const std::vector<const Relation*>& keep_rels) {
  const size_t ncols = fusion.select_items.size();
  std::vector<Column> out_cols;
  out_cols.reserve(ncols);
  for (size_t i = 0; i < ncols; ++i) {
    const std::string name =
        i < fusion.select_names.size() && !fusion.select_names[i].empty()
            ? fusion.select_names[i]
            : "col_" + std::to_string(i);
    out_cols.emplace_back(name, ValueType::kNull);
  }
  std::vector<ColumnTypeTracker> col_tracks(ncols);
  Relation out(context.execution_runtime());

  auto emit_one_group = [&](const Row& rep_row,
                            const std::vector<AggSlotState>* states,
                            bool* aborted) -> StatusOr<bool> {
    AggregateResultMap agg_results;
    agg_results.reserve(fusion.aggregates.size());
    for (size_t ai = 0; ai < fusion.aggregates.size(); ++ai) {
      Value v;
      if (states == nullptr) {
        // Empty input table on scalar aggregate: COUNT -> 0, others -> NULL.
        v = (agg_kinds[ai] == SimpleAggKind::kCountStar ||
             agg_kinds[ai] == SimpleAggKind::kCount)
                ? Value(int64_t{0})
                : Value();
      } else {
        if (!FinishAggSlot((*states)[ai], agg_kinds[ai], &v)) {
          *aborted = true;
          return false;
        }
      }
      agg_results.emplace(fusion.aggregates[ai], std::move(v));
    }
    Scope scope{.row = &rep_row, .schema = &rep_schema};
    if (fusion.having) {
      ASSIGN_OR_RETURN(Value, keep,
                       TryEvaluate(fusion.having, scope, &agg_results, context,
                                   /*ctes=*/{}));
      if (!Truthy(keep)) {
        return true;
      }
    }
    Row out_row;
    out_row.values_.reserve(ncols);
    for (size_t ci = 0; ci < ncols; ++ci) {
      ASSIGN_OR_RETURN(Value, cell,
                       TryEvaluate(fusion.select_items[ci], scope, &agg_results,
                                   context, /*ctes=*/{}));
      col_tracks[ci].Observe(cell);
      out_row.values_.push_back(std::move(cell));
    }
    EnsureOwnedStringValues(&out_row);
    out.AddRow(std::move(out_row));
    return true;
  };

  if (rep_rows.empty()) {
    if (fusion.group_keys.empty() && empty_scalar_rep_row.has_value()) {
      bool aborted = false;
      ASSIGN_OR_RETURN(
          bool, ok, emit_one_group(*empty_scalar_rep_row, nullptr, &aborted));
      (void)ok;
      if (aborted) {
        return StatusError(StatusCode::kInvalidArgument, "eager fallback");
      }
    }
  } else {
    for (size_t gi = 0; gi < rep_rows.size(); ++gi) {
      bool aborted = false;
      ASSIGN_OR_RETURN(bool, ok,
                       emit_one_group(rep_rows[gi], &group_aggs[gi], &aborted));
      (void)ok;
      if (aborted) {
        return StatusError(StatusCode::kInvalidArgument, "eager fallback");
      }
    }
  }
  const bool all_uniform = ColumnTypeTracker::FinalizeColumns(
      std::span<const std::vector<ColumnTypeTracker>>(&col_tracks, 1),
      out_cols);
  out.schema = Schema("", std::move(out_cols));
  RETURN_IF_FAIL(out.FinishSpill());
  RETURN_IF_FAIL(out.GetStatus());
  out.uniform_cell_types = !out.HasSpill() && all_uniform && !out.rows.empty();
  if (fusion.output_sink != nullptr && !out.HasSpill()) {
    const size_t total_out = out.rows.size();
    auto begun = fusion.output_sink->begin(out.schema, total_out, 1);
    RETURN_IF_FAIL(begun.GetStatus());
    if (begun.Value()) {
      fusion.output_sink->used = true;
      std::vector<Value> batch;
      batch.reserve(std::min(total_out, GroupedOutputSink::kBatchRows) * ncols);
      for (size_t first = 0; first < total_out;
           first += GroupedOutputSink::kBatchRows) {
        const size_t count =
            std::min(GroupedOutputSink::kBatchRows, total_out - first);
        batch.clear();
        for (size_t r = 0; r < count; ++r) {
          const Row& row = out.rows[first + r];
          for (size_t c = 0; c < ncols; ++c) {
            batch.push_back(row[c]);
          }
        }
        RETURN_IF_FAIL(fusion.output_sink->push(
            0, first, std::span<const Value>(batch.data(), count * ncols)));
      }
      out.rows.clear();
      out.uniform_cell_types = false;
    }
  }
  if (fusion.columnar_output && !out.HasSpill() && !out.rows.empty()) {
    ASSIGN_OR_RETURN(std::shared_ptr<ColumnarTable>, built,
                     BuildColumnarTableFromRows(out.schema, out.rows));
    out.SetColumnarTable(std::move(built), all_uniform);
  }
  for (const Relation* kr : keep_rels) {
    if (kr != nullptr) {
      RetainColumnarKeepalives(context, *kr);
    }
  }
  out.hash_joins = joins_delta;
  out.in_memory_hash_joins = joins_delta;
  out.join_comparisons = comparisons_delta;
  out.peak_intermediate_rows = out.TotalRows();
  if (context.execution_runtime() != nullptr) {
    ++context.execution_runtime()->grouped_join_fusions;
  }
  return out;
}

bool IsEagerFallbackStatus(const Status& st) {
  return st.GetCode() == StatusCode::kInvalidArgument &&
         st.GetMessage() == "eager fallback";
}

}  // namespace

StatusOr<std::optional<Relation>> TryCollapsedRightGroupedJoin(
    TransactionContext& context, const Relation& left, const Relation& right,
    const std::vector<EqualityKey>& equality_keys, JoinType join_type,
    const std::vector<Expression>& residuals,
    const std::vector<BytecodeProgram>& key_programs,
    const std::vector<BytecodeProgram>& child_programs, const Scope* outer,
    const CteMap& ctes, GroupedJoinFusionSpec* fusion) {
  static const bool disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_COLLAPSED_RIGHT_JOIN") !=
      nullptr;
  if (disabled || fusion == nullptr || !fusion->eligible || fusion->fired ||
      outer != nullptr || !ctes.empty() || !residuals.empty() ||
      left.HasSpill() || right.HasSpill() || left.using_columns != nullptr ||
      right.using_columns != nullptr || fusion->anti_unmatched_only ||
      fusion->rollup_count_distribution ||
      (join_type != JoinType::kInner && join_type != JoinType::kLeft) ||
      equality_keys.size() != 1 || fusion->aggregates.empty() ||
      fusion->aggregates.size() > kMaxCollapsedAggs) {
    return std::optional<Relation>{};
  }
  const EqualityKey& eq = equality_keys[0];
  if (eq.null_safe || eq.left_offset != 0 || eq.right_offset != 0 ||
      eq.left >= left.schema.ColumnCount() ||
      eq.right >= right.schema.ColumnCount()) {
    return std::optional<Relation>{};
  }
  const Schema combined_schema = left.schema + right.schema;
  const size_t left_width = left.schema.ColumnCount();
  const size_t right_width = right.schema.ColumnCount();
  if (!HasSingleEquiKeyCompatible(left.schema.GetColumn(eq.left),
                                  right.schema.GetColumn(eq.right))) {
    return std::optional<Relation>{};
  }
  const ValueType key_type = left.schema.GetColumn(eq.left).Type();

  // Verify which side owns GROUP BY keys.
  bool any_key_left = false;
  bool any_key_right = false;
  for (size_t k = 0; k < key_programs.size(); ++k) {
    if (!fusion->group_keys[k] || ContainsQuery(fusion->group_keys[k]) ||
        ContainsParameterSlot(fusion->group_keys[k]) ||
        !ExpressionCannotThrowForPlanner(fusion->group_keys[k])) {
      return std::optional<Relation>{};
    }
    std::vector<slot_t> loads;
    CollectLoadCells(key_programs[k], &loads);
    for (const slot_t c : loads) {
      if (c < left_width) {
        any_key_left = true;
      } else {
        any_key_right = true;
      }
    }
  }
  if (any_key_left && any_key_right) {
    return std::optional<Relation>{};
  }
  bool probe_is_left = true;
  if (any_key_right) {
    if (join_type != JoinType::kInner) {
      return std::optional<Relation>{};
    }
    probe_is_left = false;
  }

  const Relation& probe_rel = probe_is_left ? left : right;
  const Relation& build_rel = probe_is_left ? right : left;
  if (probe_rel.TotalRows() > 4000000 ||
      (probe_rel.TotalRows() > 1000000 && fusion->group_keys.size() > 1)) {
    return std::optional<Relation>{};
  }
  const auto probe_key_col =
      static_cast<slot_t>(probe_is_left ? eq.left : eq.right);
  const auto build_key_col =
      static_cast<slot_t>(probe_is_left ? eq.right : eq.left);

  // Classify aggregates and check side isolation.
  std::vector<SimpleAggKind> agg_kinds(fusion->aggregates.size());
  std::vector<bool> agg_on_probe(fusion->aggregates.size(), true);
  std::vector<bool> agg_direct_col(fusion->aggregates.size(), false);
  std::vector<slot_t> agg_local_col(fusion->aggregates.size(), 0);
  for (size_t ai = 0; ai < fusion->aggregates.size(); ++ai) {
    auto kind =
        ClassifySimpleAgg(fusion->aggregates[ai], fusion->count_star[ai]);
    if (!kind.has_value()) {
      return std::optional<Relation>{};
    }
    agg_kinds[ai] = *kind;
    if (fusion->count_star[ai]) {
      agg_on_probe[ai] = true;
      continue;
    }
    const Expression& child = fusion->aggregates[ai]->Child();
    if (ContainsQuery(child) || ContainsParameterSlot(child) ||
        !ExpressionCannotThrowForPlanner(child)) {
      return std::optional<Relation>{};
    }
    std::vector<slot_t> loads;
    CollectLoadCells(child_programs[ai], &loads);
    bool loads_left = false;
    bool loads_right = false;
    for (const slot_t c : loads) {
      if (c < left_width) {
        loads_left = true;
      } else {
        loads_right = true;
      }
    }
    if (loads_left && loads_right) {
      return std::optional<Relation>{};
    }
    const bool on_probe = probe_is_left ? !loads_right : !loads_left;
    agg_on_probe[ai] = on_probe;
    if (child->Type() == TypeTag::kColumnValue) {
      const auto off = LocalColumnOffset(
          combined_schema, child->AsColumnValue().GetColumnName());
      if (off.has_value()) {
        const size_t local = (static_cast<size_t>(*off) < left_width)
                                 ? static_cast<size_t>(*off)
                                 : (static_cast<size_t>(*off) - left_width);
        const bool expected_on_left = static_cast<size_t>(*off) < left_width;
        if (expected_on_left == (probe_is_left ? on_probe : !on_probe)) {
          agg_direct_col[ai] = true;
          agg_local_col[ai] = static_cast<slot_t>(local);
        }
      }
    }
  }

  // Verify HAVING and SELECT non-aggregate references only touch probe GROUP BY
  // columns (or the paired inner join key).
  std::unordered_set<std::string> grouped_col_names;
  for (const Expression& gk : fusion->group_keys) {
    if (gk && gk->Type() == TypeTag::kColumnValue) {
      const auto off = LocalColumnOffset(combined_schema,
                                         gk->AsColumnValue().GetColumnName());
      if (off.has_value()) {
        grouped_col_names.insert(AsciiToLower(
            combined_schema.GetColumn(static_cast<size_t>(*off)).Name().name));
      }
    } else if (!AllNonAggColumnsAllowed(
                   gk, [](const ColumnName&) { return false; })) {
      // Computed GROUP BY key: must match identically in SELECT/HAVING unless
      // only aggregates + constants remain outside GROUP BY.
    }
  }
  const auto col_allowed = [&](const ColumnName& col) -> bool {
    const auto off = LocalColumnOffset(combined_schema, col);
    if (!off.has_value()) {
      return false;
    }
    const auto co = static_cast<size_t>(*off);
    const bool on_left = co < left_width;
    if (on_left == probe_is_left) {
      return std::ranges::any_of(fusion->group_keys, [&](const Expression& gk) {
        if (!gk || gk->Type() != TypeTag::kColumnValue) {
          return false;
        }
        return LocalColumnOffset(combined_schema,
                                 gk->AsColumnValue().GetColumnName()) ==
               static_cast<int>(co);
      });
    }
    if (join_type == JoinType::kInner &&
        co == (on_left ? eq.left : left_width + eq.right)) {
      const size_t probe_eq_combined =
          probe_is_left ? eq.left : (left_width + eq.right);
      return std::ranges::any_of(fusion->group_keys, [&](const Expression& gk) {
        if (!gk || gk->Type() != TypeTag::kColumnValue) {
          return false;
        }
        return LocalColumnOffset(combined_schema,
                                 gk->AsColumnValue().GetColumnName()) ==
               static_cast<int>(probe_eq_combined);
      });
    }
    return false;
  };
  if (fusion->having && !AllNonAggColumnsAllowed(fusion->having, col_allowed)) {
    return std::optional<Relation>{};
  }
  for (const Expression& se : fusion->select_items) {
    // Allow exact GROUP BY expression structural equality too.
    bool matches_group_expr =
        std::ranges::any_of(fusion->group_keys, [&](const Expression& gk) {
          return gk && se && gk->ToString() == se->ToString();
        });
    if (!matches_group_expr && !AllNonAggColumnsAllowed(se, col_allowed)) {
      return std::optional<Relation>{};
    }
  }

  // Resolve GROUP BY key column descriptors for fast extraction on probe.
  std::vector<std::optional<slot_t>> direct_group_probe_cols(
      fusion->group_keys.size());
  for (size_t k = 0; k < fusion->group_keys.size(); ++k) {
    if (fusion->group_keys[k]->Type() == TypeTag::kColumnValue) {
      const auto off = LocalColumnOffset(
          probe_rel.schema,
          fusion->group_keys[k]->AsColumnValue().GetColumnName());
      if (off.has_value()) {
        direct_group_probe_cols[k] = static_cast<slot_t>(*off);
      }
    }
  }

  std::vector<slot_t> build_agg_indices;
  std::vector<slot_t> probe_agg_indices;
  for (size_t ai = 0; ai < agg_kinds.size(); ++ai) {
    if (agg_on_probe[ai]) {
      probe_agg_indices.push_back(static_cast<slot_t>(ai));
    } else {
      build_agg_indices.push_back(static_cast<slot_t>(ai));
    }
  }

  // Phase A: Collapse `build_rel` by `build_key_col`.
  const size_t n_build_aggs = build_agg_indices.size();
  std::vector<uint64_t> collapsed_cnt;
  std::vector<AggSlotState> collapsed_aggs;  // [kid * n_build_aggs + ba]

  const bool is_int_join_key =
      (key_type == ValueType::kInt64 || key_type == ValueType::kDate);
  bool use_dense_int = false;
  int64_t dense_min = 0;
  std::vector<uint32_t> dense_slots;
  FlatIntKeyMap<uint32_t> int_key_to_slot;
  bool use_pack7_str = (key_type == ValueType::kVarChar);
  std::vector<uint32_t> page_url_slots;
  std::unordered_map<uint64_t, uint32_t> pack7_key_to_slot;
  TransparentStringSlotMap str_key_to_slot;

  bool phase_a_done = false;
  const bool all_build_aggs_direct = std::ranges::all_of(
      build_agg_indices,
      [&](slot_t ai) { return fusion->count_star[ai] || agg_direct_col[ai]; });
  if (is_int_join_key && build_rel.TotalRows() > 0) {
    int64_t bmin = std::numeric_limits<int64_t>::max();
    int64_t bmax = std::numeric_limits<int64_t>::min();
    bool bad = false;
    const size_t build_workers = (build_rel.TotalRows() >= 32768)
                                     ? ParallelWorkerCount(std::max<size_t>(
                                           1, build_rel.TotalRows() / 16384))
                                     : 1;
    if (build_workers > 1) {
      std::vector<int64_t> w_min(build_workers,
                                 std::numeric_limits<int64_t>::max());
      std::vector<int64_t> w_max(build_workers,
                                 std::numeric_limits<int64_t>::min());
      std::vector<uint8_t> w_bad(build_workers, 0);
      RETURN_IF_FAIL(ParallelForEachRelationEntryRange(
          build_rel, build_workers, [&](size_t w, auto&& iter_fn) {
            int64_t lmin = std::numeric_limits<int64_t>::max();
            int64_t lmax = std::numeric_limits<int64_t>::min();
            bool lbad = false;
            iter_fn([&](const ColumnarChunk* chunk, const Row* r, size_t ri) {
              int64_t kv = 0;
              if (ReadIntKeyFromEntry(chunk, r, ri, build_key_col, key_type,
                                      &kv, &lbad)) {
                lmin = std::min(lmin, kv);
                lmax = std::max(lmax, kv);
              }
              return !lbad;
            });
            w_min[w] = lmin;
            w_max[w] = lmax;
            w_bad[w] = lbad ? 1U : 0U;
          }));
      for (size_t w = 0; w < build_workers; ++w) {
        if (w_bad[w] != 0U) {
          bad = true;
          break;
        }
        bmin = std::min(bmin, w_min[w]);
        bmax = std::max(bmax, w_max[w]);
      }
    } else {
      ForEachRelationEntry(
          build_rel, [&](const ColumnarChunk* chunk, const Row* r, size_t ri) {
            int64_t kv = 0;
            if (ReadIntKeyFromEntry(chunk, r, ri, build_key_col, key_type, &kv,
                                    &bad)) {
              bmin = std::min(bmin, kv);
              bmax = std::max(bmax, kv);
            }
            return !bad;
          });
    }
    if (bad) {
      return std::optional<Relation>{};
    }
    if (bmin <= bmax) {
      const uint64_t span =
          static_cast<uint64_t>(bmax) - static_cast<uint64_t>(bmin) + 1ULL;
      if (span > 0 && span <= kMaxDenseKeySpan) {
        use_dense_int = true;
        dense_min = bmin;
        dense_slots.assign(static_cast<size_t>(span), kAbsentSlot);
        if (build_workers > 1 && all_build_aggs_direct) {
          struct alignas(64) BuildLocal {
            std::vector<uint64_t> cnt;
            std::vector<AggSlotState> aggs;
            bool invalid{false};
          };
          std::vector<BuildLocal> blocals(build_workers);
          for (BuildLocal& bl : blocals) {
            bl.cnt.assign(static_cast<size_t>(span), 0);
            if (n_build_aggs > 0) {
              bl.aggs.resize(static_cast<size_t>(span) * n_build_aggs);
            }
          }
          RETURN_IF_FAIL(ParallelForEachRelationEntryRange(
              build_rel, build_workers, [&](size_t w, auto&& iter_fn) {
                BuildLocal& bl = blocals[w];
                iter_fn([&](const ColumnarChunk* chunk, const Row* r,
                            size_t ri) {
                  int64_t kv = 0;
                  if (!ReadIntKeyFromEntry(chunk, r, ri, build_key_col,
                                           key_type, &kv, &bl.invalid)) {
                    return !bl.invalid;
                  }
                  const auto off =
                      static_cast<size_t>(static_cast<uint64_t>(kv) -
                                          static_cast<uint64_t>(dense_min));
                  ++bl.cnt[off];
                  for (size_t ba = 0; ba < n_build_aggs; ++ba) {
                    const slot_t ai = build_agg_indices[ba];
                    if (!AddEntryCellWithWeight(
                            &bl.aggs[(off * n_build_aggs) + ba], agg_kinds[ai],
                            chunk, r, ri, agg_local_col[ai], 1)) {
                      bl.invalid = true;
                      return false;
                    }
                  }
                  return true;
                });
              }));
          for (const BuildLocal& bl : blocals) {
            if (bl.invalid) {
              return std::optional<Relation>{};
            }
          }
          for (size_t off = 0; off < static_cast<size_t>(span); ++off) {
            uint64_t total_c = 0;
            for (const BuildLocal& bl : blocals) {
              total_c += bl.cnt[off];
            }
            if (total_c == 0) {
              continue;
            }
            const auto sid = static_cast<uint32_t>(collapsed_cnt.size());
            dense_slots[off] = sid;
            collapsed_cnt.push_back(total_c);
            if (n_build_aggs > 0) {
              const size_t dst_base = static_cast<size_t>(sid) * n_build_aggs;
              collapsed_aggs.resize(dst_base + n_build_aggs);
              for (const BuildLocal& bl : blocals) {
                if (bl.cnt[off] == 0) {
                  continue;
                }
                const size_t src_base = off * n_build_aggs;
                for (size_t ba = 0; ba < n_build_aggs; ++ba) {
                  const slot_t ai = build_agg_indices[ba];
                  if (!MergeAggSlot(&collapsed_aggs[dst_base + ba],
                                    bl.aggs[src_base + ba], agg_kinds[ai], 1)) {
                    return std::optional<Relation>{};
                  }
                }
              }
            }
          }
          phase_a_done = true;
        }
      }
    }
  }

  std::vector<std::pair<std::string, uint32_t>> pack7_overflow_entries;
  auto get_or_alloc_slot = [&](uint32_t* slot_out, const int64_t* ikey,
                               std::string_view skey) -> bool {
    if (is_int_join_key) {
      if (use_dense_int) {
        const auto off = static_cast<size_t>(static_cast<uint64_t>(*ikey) -
                                             static_cast<uint64_t>(dense_min));
        uint32_t& ref = dense_slots[off];
        if (ref == kAbsentSlot) {
          ref = static_cast<uint32_t>(collapsed_cnt.size());
          collapsed_cnt.push_back(0);
          if (n_build_aggs > 0) {
            collapsed_aggs.resize(collapsed_cnt.size() * n_build_aggs);
          }
        }
        *slot_out = ref;
        return true;
      }
      if (const uint32_t* existing = int_key_to_slot.Find(*ikey)) {
        *slot_out = *existing;
        return true;
      }
      const auto sid = static_cast<uint32_t>(collapsed_cnt.size());
      collapsed_cnt.push_back(0);
      if (n_build_aggs > 0) {
        collapsed_aggs.resize(collapsed_cnt.size() * n_build_aggs);
      }
      *int_key_to_slot.TryEmplace(*ikey) = sid;
      *slot_out = sid;
      return true;
    }
    // Lean proof:
    // lean/TinylambOpt/ResearchProofs/Wave8BigBench.lean (UrlGrouping_sound)
    // Canonical "page-<N>" URLs map bijectively into a dense flat array,
    // disjoint from pack7/literal string maps.
    uint32_t page_id = 0;
    if (TryParseCanonicalPageUrl(skey, &page_id)) {
      if (page_id >= page_url_slots.size()) {
        page_url_slots.resize(static_cast<size_t>(page_id) + 1, kAbsentSlot);
      }
      uint32_t& ref = page_url_slots[page_id];
      if (ref == kAbsentSlot) {
        ref = static_cast<uint32_t>(collapsed_cnt.size());
        collapsed_cnt.push_back(0);
        if (n_build_aggs > 0) {
          collapsed_aggs.resize(collapsed_cnt.size() * n_build_aggs);
        }
      }
      *slot_out = ref;
      return true;
    }
    if (use_pack7_str) {
      if (auto p7 = PackShortString7(skey)) {
        auto [it, inserted] = pack7_key_to_slot.emplace(
            *p7, static_cast<uint32_t>(collapsed_cnt.size()));
        if (inserted) {
          collapsed_cnt.push_back(0);
          if (n_build_aggs > 0) {
            collapsed_aggs.resize(collapsed_cnt.size() * n_build_aggs);
          }
          if (n_build_aggs == 0) {
            // Keep key text only if we need to spill into str_key_to_slot
            // later.
          }
        }
        *slot_out = it->second;
        return true;
      }
      // First >7-byte string seen: migrate existing pack7 map to str map once.
      use_pack7_str = false;
      str_key_to_slot.reserve(pack7_key_to_slot.size() + 64);
      for (const auto& [word, sid] : pack7_key_to_slot) {
        const auto len = static_cast<size_t>(word & 0xFFU);
        std::string tmp(len, '\0');
        for (size_t i = 0; i < len; ++i) {
          tmp[i] = static_cast<char>((word >> ((7U - i) * 8U)) & 0xFFU);
        }
        str_key_to_slot.emplace(std::move(tmp), sid);
      }
      pack7_key_to_slot.clear();
    }
    auto it = str_key_to_slot.find(skey);
    if (it != str_key_to_slot.end()) {
      *slot_out = it->second;
      return true;
    }
    const auto sid = static_cast<uint32_t>(collapsed_cnt.size());
    collapsed_cnt.push_back(0);
    if (n_build_aggs > 0) {
      collapsed_aggs.resize(collapsed_cnt.size() * n_build_aggs);
    }
    str_key_to_slot.emplace(std::string(skey), sid);
    *slot_out = sid;
    return true;
  };

  Row build_eval_scratch(std::vector<Value>(left_width + right_width));
  Value build_key_owned_tmp;
  bool build_invalid = false;
  Status build_status = Status::kSuccess;
  if (!phase_a_done) {
    ForEachRelationEntry(build_rel, [&](const ColumnarChunk* chunk,
                                        const Row* r, size_t ri) {
      uint32_t sid = kAbsentSlot;
      if (is_int_join_key) {
        int64_t kv = 0;
        if (!ReadIntKeyFromEntry(chunk, r, ri, build_key_col, key_type, &kv,
                                 &build_invalid)) {
          return !build_invalid;
        }
        get_or_alloc_slot(&sid, &kv, {});
      } else {
        std::string_view sv;
        if (!ReadStringKeyFromEntry(chunk, r, ri, build_key_col, &sv,
                                    &build_key_owned_tmp, &build_invalid)) {
          return !build_invalid;
        }
        get_or_alloc_slot(&sid, nullptr, sv);
      }
      ++collapsed_cnt[sid];
      if (n_build_aggs > 0) {
        const size_t offset = probe_is_left ? left_width : 0;
        for (size_t ba = 0; ba < n_build_aggs; ++ba) {
          const slot_t ai = build_agg_indices[ba];
          if (agg_direct_col[ai]) {
            if (!AddEntryCellWithWeight(
                    &collapsed_aggs[(sid * n_build_aggs) + ba], agg_kinds[ai],
                    chunk, r, ri, agg_local_col[ai], 1)) {
              build_invalid = true;
              return false;
            }
          } else {
            if (r != nullptr) {
              for (size_t c = 0; c < r->Count(); ++c) {
                build_eval_scratch.values_[offset + c] = (*r)[c];
              }
            } else {
              for (size_t c = 0; c < build_rel.schema.ColumnCount(); ++c) {
                chunk->columns[c].WriteValueInto(
                    ri, &build_eval_scratch.values_[offset + c]);
              }
            }
            StatusOr<Value> eval_or =
                child_programs[ai].TryEvaluateRow(build_eval_scratch);
            if (!eval_or.HasValue()) {
              build_status = eval_or.GetStatus();
              build_invalid = true;
              return false;
            }
            Value val = eval_or.MoveValue();
            if (!AddValueWithWeight(&collapsed_aggs[(sid * n_build_aggs) + ba],
                                    agg_kinds[ai], val, 1)) {
              build_invalid = true;
              return false;
            }
          }
        }
      }
      return true;
    });
  }
  RETURN_IF_FAIL(build_status);
  if (build_invalid) {
    return std::optional<Relation>{};
  }

  // Empty build relation with INNER join: 0 matched rows.
  const bool build_is_empty = collapsed_cnt.empty();
  if (build_is_empty && join_type == JoinType::kInner) {
    std::optional<Row> empty_rep;
    if (fusion->group_keys.empty()) {
      empty_rep = Row(std::vector<Value>(left_width + right_width));
    }
    auto res_or = EmitGroupedProjectionRelation(
        context, combined_schema, {}, {}, agg_kinds, empty_rep, *fusion,
        left.hash_joins + right.hash_joins + 1,
        left.join_comparisons + right.join_comparisons, {&left, &right});
    if (!res_or.HasValue()) {
      if (!IsEagerFallbackStatus(res_or.GetStatus())) {
        return res_or.GetStatus();
      }
      return std::optional<Relation>{};
    }
    fusion->fired = true;
    return std::optional<Relation>(res_or.MoveValue());
  }

  // Lean proof:
  // lean/TinylambOpt/ResearchProofs/Wave8Ceb.lean (SparseOuterCorrection_sound)
  // For a LEFT JOIN whose right side contributes no aggregate payloads, every
  // probe row contributes max(1, mult(key)) times. When no right key has
  // multiplicity > 1, every probe row has weight 1 with zero hash lookups.
  bool left_outer_unit_weight = false;
  if (join_type == JoinType::kLeft && n_build_aggs == 0) {
    left_outer_unit_weight = true;
    for (const uint64_t cnt : collapsed_cnt) {
      if (cnt > 1) {
        left_outer_unit_weight = false;
        break;
      }
    }
  }

  auto lookup_build_slot = [&](const ColumnarChunk* chunk, const Row* probe_row,
                               size_t r_idx, Value* probe_key_tmp,
                               std::string_view* matched_str_out,
                               int64_t* matched_int_out, uint64_t* out_weight,
                               uint32_t* out_sid, bool* out_invalid) -> bool {
    if (left_outer_unit_weight) {
      return false;
    }
    if (is_int_join_key) {
      int64_t kv = 0;
      bool inv = false;
      if (!ReadIntKeyFromEntry(chunk, probe_row, r_idx, probe_key_col, key_type,
                               &kv, &inv)) {
        if (inv) {
          *out_invalid = true;
        }
        return false;
      }
      *matched_int_out = kv;
      if (use_dense_int) {
        const uint64_t off =
            static_cast<uint64_t>(kv) - static_cast<uint64_t>(dense_min);
        if (off >= dense_slots.size() || dense_slots[off] == kAbsentSlot) {
          return false;
        }
        *out_sid = dense_slots[off];
        *out_weight = collapsed_cnt[*out_sid];
        return true;
      }
      if (const uint32_t* found = int_key_to_slot.Find(kv)) {
        *out_sid = *found;
        *out_weight = collapsed_cnt[*out_sid];
        return true;
      }
      return false;
    }
    std::string_view sv;
    bool inv = false;
    if (!ReadStringKeyFromEntry(chunk, probe_row, r_idx, probe_key_col, &sv,
                                probe_key_tmp, &inv)) {
      if (inv) {
        *out_invalid = true;
      }
      return false;
    }
    *matched_str_out = sv;
    if (!page_url_slots.empty()) {
      uint32_t page_id = 0;
      if (TryParseCanonicalPageUrl(sv, &page_id)) {
        if (page_id >= page_url_slots.size() ||
            page_url_slots[page_id] == kAbsentSlot) {
          return false;
        }
        *out_sid = page_url_slots[page_id];
        *out_weight = collapsed_cnt[*out_sid];
        return true;
      }
    }
    if (use_pack7_str) {
      auto p7 = PackShortString7(sv);
      if (!p7.has_value()) {
        return false;
      }
      auto it = pack7_key_to_slot.find(*p7);
      if (it == pack7_key_to_slot.end()) {
        return false;
      }
      *out_sid = it->second;
      *out_weight = collapsed_cnt[*out_sid];
      return true;
    }
    auto it = str_key_to_slot.find(sv);
    if (it == str_key_to_slot.end()) {
      return false;
    }
    *out_sid = it->second;
    *out_weight = collapsed_cnt[*out_sid];
    return true;
  };

  // Phase B: Parallel Probe & Group Accumulation.
  const size_t total_probe = probe_rel.TotalRows();
  const size_t workers =
      ParallelWorkerCount(std::max<size_t>(1, total_probe / 16384));
  const size_t n_aggs = agg_kinds.size();
  const size_t n_gkeys = fusion->group_keys.size();

  const bool single_int_group_key =
      n_gkeys == 1 && direct_group_probe_cols[0].has_value() &&
      !probe_rel.schema.GetColumn(*direct_group_probe_cols[0]).IsUnsigned() &&
      (probe_rel.schema.GetColumn(*direct_group_probe_cols[0]).Type() ==
           ValueType::kInt64 ||
       probe_rel.schema.GetColumn(*direct_group_probe_cols[0]).Type() ==
           ValueType::kDate);
  const bool single_pack7_group_key =
      n_gkeys == 1 && direct_group_probe_cols[0].has_value() &&
      probe_rel.schema.GetColumn(*direct_group_probe_cols[0]).Type() ==
          ValueType::kVarChar;

  constexpr int64_t kDirectGroupKeyLimit = 16384;
  const slot_t single_gk_col = (single_int_group_key || single_pack7_group_key)
                                   ? *direct_group_probe_cols[0]
                                   : 0;
  const ValueType single_gk_type =
      single_int_group_key ? probe_rel.schema.GetColumn(single_gk_col).Type()
                           : ValueType::kNull;
  bool has_non_direct_probe_agg = false;
  for (const slot_t ai : probe_agg_indices) {
    if (!fusion->count_star[ai] && !agg_direct_col[ai]) {
      has_non_direct_probe_agg = true;
      break;
    }
  }

  struct WorkerLocal {
    bool has_scalar{false};
    std::vector<AggSlotState> scalar_aggs;
    std::vector<uint32_t> direct_gid;
    FlatIntKeyMap<uint32_t> int_map;
    std::unordered_map<uint64_t, uint32_t> pack7_map;
    TransparentStringSlotMap str_map;
    uint32_t null_group_idx{kAbsentSlot};
    std::vector<std::vector<Value>> group_keys;
    std::vector<AggSlotState> flat_group_aggs;
    std::vector<Value> null_or_scalar_key;
    uint64_t matched_comparisons{0};
    bool invalid{false};
    Status error{Status::kSuccess};
  };
  std::vector<WorkerLocal> locals(workers);
  for (WorkerLocal& wl : locals) {
    if (n_gkeys == 0) {
      wl.scalar_aggs.resize(n_aggs);
    } else if (single_int_group_key) {
      wl.direct_gid.assign(static_cast<size_t>(kDirectGroupKeyLimit),
                           kAbsentSlot);
    }
  }

  RETURN_IF_FAIL(ParallelForEachRelationEntryRange(
      probe_rel, workers, [&](size_t w, auto&& iter_fn) {
        WorkerLocal& wl = locals[w];
        Row combined_scratch(std::vector<Value>(left_width + right_width));
        std::vector<Value> gk_vals(n_gkeys);
        const size_t probe_offset = probe_is_left ? 0 : left_width;
        const size_t build_offset = probe_is_left ? left_width : 0;
        const size_t probe_width = probe_rel.schema.ColumnCount();
        Value probe_key_tmp;
        Value group_str_tmp;
        Value agg_val_tmp;

        iter_fn([&](const ColumnarChunk* chunk, const Row* probe_row,
                    size_t ri) -> bool {
          uint64_t weight = 1;
          uint32_t sid = kAbsentSlot;
          bool key_invalid = false;
          int64_t matched_int = 0;
          std::string_view matched_str;
          const bool matched = lookup_build_slot(
              chunk, probe_row, ri, &probe_key_tmp, &matched_str, &matched_int,
              &weight, &sid, &key_invalid);
          if (key_invalid) {
            wl.invalid = true;
            return false;
          }
          if (!matched) {
            if (join_type == JoinType::kInner) {
              return true;
            }
            weight = 1;
          } else {
            wl.matched_comparisons += weight;
          }

          // Locate target group accumulators.
          AggSlotState* dst_aggs = nullptr;
          if (n_gkeys == 0) {
            wl.has_scalar = true;
            dst_aggs = wl.scalar_aggs.data();
          } else {
            bool populated_combined = false;
            auto ensure_combined = [&]() {
              if (!populated_combined) {
                if (probe_row != nullptr) {
                  for (size_t c = 0; c < probe_row->Count(); ++c) {
                    combined_scratch.values_[probe_offset + c] =
                        (*probe_row)[c];
                  }
                } else {
                  for (size_t c = 0; c < probe_width; ++c) {
                    chunk->columns[c].WriteValueInto(
                        ri, &combined_scratch.values_[probe_offset + c]);
                  }
                }
                if (matched) {
                  combined_scratch.values_[build_offset + build_key_col] =
                      is_int_join_key ? (key_type == ValueType::kDate
                                             ? Value::DateFromDays(matched_int)
                                             : Value(matched_int))
                                      : Value(std::string(matched_str));
                }
                populated_combined = true;
              }
            };
            if (single_int_group_key) {
              int64_t iv = 0;
              bool ginv = false;
              if (!ReadIntKeyFromEntry(chunk, probe_row, ri, single_gk_col,
                                       single_gk_type, &iv, &ginv)) {
                if (ginv) {
                  wl.invalid = true;
                  return false;
                }
                if (wl.null_group_idx == kAbsentSlot) {
                  wl.null_group_idx =
                      static_cast<uint32_t>(wl.group_keys.size());
                  wl.group_keys.push_back({Value()});
                  wl.flat_group_aggs.resize(wl.group_keys.size() * n_aggs);
                }
                dst_aggs = wl.flat_group_aggs.data() +
                           (static_cast<size_t>(wl.null_group_idx) * n_aggs);
              } else {
                uint32_t gid = kAbsentSlot;
                if (iv >= 0 && iv < kDirectGroupKeyLimit) {
                  gid = wl.direct_gid[static_cast<size_t>(iv)];
                  if (gid == kAbsentSlot) {
                    gid = static_cast<uint32_t>(wl.group_keys.size());
                    wl.direct_gid[static_cast<size_t>(iv)] = gid;
                    wl.group_keys.push_back({single_gk_type == ValueType::kDate
                                                 ? Value::DateFromDays(iv)
                                                 : Value(iv)});
                    wl.flat_group_aggs.resize(wl.group_keys.size() * n_aggs);
                  }
                } else if (const uint32_t* found = wl.int_map.Find(iv)) {
                  gid = *found;
                } else {
                  gid = static_cast<uint32_t>(wl.group_keys.size());
                  *wl.int_map.TryEmplace(iv) = gid;
                  wl.group_keys.push_back({single_gk_type == ValueType::kDate
                                               ? Value::DateFromDays(iv)
                                               : Value(iv)});
                  wl.flat_group_aggs.resize(wl.group_keys.size() * n_aggs);
                }
                dst_aggs = wl.flat_group_aggs.data() +
                           (static_cast<size_t>(gid) * n_aggs);
              }
            } else if (single_pack7_group_key) {
              std::string_view sv;
              bool ginv = false;
              if (!ReadStringKeyFromEntry(chunk, probe_row, ri, single_gk_col,
                                          &sv, &group_str_tmp, &ginv)) {
                if (ginv) {
                  wl.invalid = true;
                  return false;
                }
                if (wl.null_group_idx == kAbsentSlot) {
                  wl.null_group_idx =
                      static_cast<uint32_t>(wl.group_keys.size());
                  wl.group_keys.push_back({Value()});
                  wl.flat_group_aggs.resize(wl.group_keys.size() * n_aggs);
                }
                dst_aggs = wl.flat_group_aggs.data() +
                           (static_cast<size_t>(wl.null_group_idx) * n_aggs);
              } else {
                uint32_t gid = kAbsentSlot;
                if (auto p7 = PackShortString7(sv)) {
                  auto [it, inserted] = wl.pack7_map.emplace(
                      *p7, static_cast<uint32_t>(wl.group_keys.size()));
                  if (inserted) {
                    Value owned{std::string(sv)};
                    wl.group_keys.push_back({std::move(owned)});
                    wl.flat_group_aggs.resize(wl.group_keys.size() * n_aggs);
                  }
                  gid = it->second;
                } else {
                  if (auto it = wl.str_map.find(sv); it != wl.str_map.end()) {
                    gid = it->second;
                  } else {
                    gid = static_cast<uint32_t>(wl.group_keys.size());
                    wl.str_map.emplace(std::string(sv), gid);
                    Value owned{std::string(sv)};
                    wl.group_keys.push_back({std::move(owned)});
                    wl.flat_group_aggs.resize(wl.group_keys.size() * n_aggs);
                  }
                }
                dst_aggs = wl.flat_group_aggs.data() +
                           (static_cast<size_t>(gid) * n_aggs);
              }
            } else {
              ensure_combined();
              for (size_t k = 0; k < n_gkeys; ++k) {
                if (direct_group_probe_cols[k].has_value()) {
                  ReadColValueOrRow(chunk, probe_row, ri,
                                    *direct_group_probe_cols[k], &gk_vals[k]);
                } else {
                  StatusOr<Value> kv_or =
                      key_programs[k].TryEvaluateRow(combined_scratch);
                  if (!kv_or.HasValue()) {
                    wl.error = kv_or.GetStatus();
                    return false;
                  }
                  gk_vals[k] = kv_or.MoveValue();
                }
                if (gk_vals[k].IsCaseInsensitive() || gk_vals[k].IsInterval()) {
                  wl.invalid = true;
                  return false;
                }
              }
              std::string enc = EncodeKeyTuple(gk_vals);
              auto [it, inserted] = wl.str_map.emplace(
                  std::move(enc), static_cast<uint32_t>(wl.group_keys.size()));
              if (inserted) {
                std::vector<Value> owned_keys = gk_vals;
                for (Value& v : owned_keys) {
                  if (v.type == ValueType::kVarChar && v.owned_data.empty() &&
                      !v.value.varchar_value.empty()) {
                    v.owned_data.assign(v.value.varchar_value);
                    v.value.varchar_value = v.owned_data;
                  }
                }
                wl.group_keys.push_back(std::move(owned_keys));
                wl.flat_group_aggs.resize(wl.group_keys.size() * n_aggs);
              }
              dst_aggs = wl.flat_group_aggs.data() +
                         (static_cast<size_t>(it->second) * n_aggs);
            }

            // For non-direct probe agg expressions, ensure combined_scratch.
            if (has_non_direct_probe_agg) {
              ensure_combined();
            }
          }

          // Accumulate probe aggregates weighted by `weight`.
          for (const slot_t ai : probe_agg_indices) {
            if (fusion->count_star[ai]) {
              dst_aggs[ai].count += weight;
              continue;
            }
            if (agg_direct_col[ai]) {
              if (!AddEntryCellWithWeight(&dst_aggs[ai], agg_kinds[ai], chunk,
                                          probe_row, ri, agg_local_col[ai],
                                          weight)) {
                wl.invalid = true;
                return false;
              }
            } else {
              if (probe_row != nullptr) {
                for (size_t c = 0; c < probe_row->Count(); ++c) {
                  combined_scratch.values_[probe_offset + c] = (*probe_row)[c];
                }
              } else {
                for (size_t c = 0; c < probe_width; ++c) {
                  chunk->columns[c].WriteValueInto(
                      ri, &combined_scratch.values_[probe_offset + c]);
                }
              }
              StatusOr<Value> av =
                  child_programs[ai].TryEvaluateRow(combined_scratch);
              if (!av.HasValue()) {
                wl.error = av.GetStatus();
                return false;
              }
              agg_val_tmp = av.MoveValue();
              if (!AddValueWithWeight(&dst_aggs[ai], agg_kinds[ai], agg_val_tmp,
                                      weight)) {
                wl.invalid = true;
                return false;
              }
            }
          }
          // Accumulate matched build-side aggregates (once per matched key
          // lookup).
          if (matched && n_build_aggs > 0) {
            const size_t base = sid * n_build_aggs;
            for (size_t ba = 0; ba < n_build_aggs; ++ba) {
              const slot_t ai = build_agg_indices[ba];
              if (!MergeAggSlot(&dst_aggs[ai], collapsed_aggs[base + ba],
                                agg_kinds[ai], 1)) {
                wl.invalid = true;
                return false;
              }
            }
          }
          return true;
        });
      }));

  uint64_t total_comparisons = 0;
  for (const WorkerLocal& wl : locals) {
    RETURN_IF_FAIL(wl.error);
    if (wl.invalid) {
      return std::optional<Relation>{};
    }
    total_comparisons += wl.matched_comparisons;
  }

  // Merge worker locals into final representative rows & agg vectors.
  std::vector<Row> final_rep_rows;
  std::vector<std::vector<AggSlotState>> final_group_aggs;
  const size_t probe_offset = probe_is_left ? 0 : left_width;
  const size_t build_offset = probe_is_left ? left_width : 0;

  if (n_gkeys == 0) {
    bool any_scalar = false;
    std::vector<AggSlotState> merged_scalar(n_aggs);
    for (const WorkerLocal& wl : locals) {
      if (!wl.has_scalar) {
        continue;
      }
      any_scalar = true;
      for (size_t ai = 0; ai < n_aggs; ++ai) {
        if (!MergeAggSlot(&merged_scalar[ai], wl.scalar_aggs[ai], agg_kinds[ai],
                          1)) {
          return std::optional<Relation>{};
        }
      }
    }
    if (any_scalar) {
      final_rep_rows.emplace_back(std::vector<Value>(left_width + right_width));
      final_group_aggs.push_back(std::move(merged_scalar));
    }
  } else {
    std::unordered_map<std::string, size_t> global_groups;
    for (const WorkerLocal& wl : locals) {
      for (size_t gi = 0; gi < wl.group_keys.size(); ++gi) {
        std::string enc = EncodeKeyTuple(wl.group_keys[gi]);
        auto [it, inserted] =
            global_groups.emplace(std::move(enc), final_rep_rows.size());
        if (inserted) {
          Row rep(std::vector<Value>(left_width + right_width));
          for (size_t k = 0; k < n_gkeys; ++k) {
            if (direct_group_probe_cols[k].has_value()) {
              rep.values_[probe_offset + *direct_group_probe_cols[k]] =
                  wl.group_keys[gi][k];
              if (join_type == JoinType::kInner &&
                  *direct_group_probe_cols[k] == probe_key_col) {
                rep.values_[build_offset + build_key_col] =
                    wl.group_keys[gi][k];
              }
            }
          }
          EnsureOwnedStringValues(&rep);
          final_rep_rows.push_back(std::move(rep));
          const size_t src_base = gi * n_aggs;
          final_group_aggs.emplace_back(
              wl.flat_group_aggs.begin() +
                  static_cast<std::ptrdiff_t>(src_base),
              wl.flat_group_aggs.begin() +
                  static_cast<std::ptrdiff_t>(src_base + n_aggs));
        } else {
          const size_t dst_idx = it->second;
          const size_t src_base = gi * n_aggs;
          for (size_t ai = 0; ai < n_aggs; ++ai) {
            if (!MergeAggSlot(&final_group_aggs[dst_idx][ai],
                              wl.flat_group_aggs[src_base + ai], agg_kinds[ai],
                              1)) {
              return std::optional<Relation>{};
            }
          }
        }
      }
    }
  }

  // Note: for computed GROUP BY expressions, evaluate against combined rep row
  // requires representative columns; we only populate direct_group_probe_cols,
  // so verify any non-direct GROUP BY expression isn't needed ungrouped in
  // SELECT/HAVING.
  for (size_t k = 0; k < n_gkeys; ++k) {
    if (!direct_group_probe_cols[k].has_value()) {
      return std::optional<Relation>{};
    }
  }

  std::optional<Row> empty_rep;
  if (n_gkeys == 0) {
    empty_rep = Row(std::vector<Value>(left_width + right_width));
  }
  auto result_or = EmitGroupedProjectionRelation(
      context, combined_schema, final_rep_rows, final_group_aggs, agg_kinds,
      empty_rep, *fusion, left.hash_joins + right.hash_joins + 1,
      left.join_comparisons + right.join_comparisons + total_comparisons,
      {&left, &right});
  if (!result_or.HasValue()) {
    if (!IsEagerFallbackStatus(result_or.GetStatus())) {
      return result_or.GetStatus();
    }
    return std::optional<Relation>{};
  }
  fusion->fired = true;
  return std::optional<Relation>(result_or.MoveValue());
}

// Lean proofs:
//   -
//   lean/TinylambOpt/ResearchProofs/BushyJoinAssociativityWithDeadKeyPrune.lean
//     (sound)
//   - lean/TinylambOpt/ResearchProofs/StarHubMultiwayWeightedProduct.lean
//     (sound)
//   - lean/TinylambOpt/ResearchProofs/Wave9AnalyticsRemaining.lean
//     (SelfJoinBridgeBipartiteWeights_sound,
//      FilteredSemiJoinCountBeforeGroup_sound)
StatusOr<std::optional<Relation>> TryFactorizedJoinTreeAggregate(
    TransactionContext& context, const std::vector<Relation>& relations,
    const std::vector<uint8_t>& absorbed,
    const std::vector<PredicateInfo>& predicates, const Scope* outer,
    const CteMap& ctes, GroupedJoinFusionSpec* fusion) {
  static const bool disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_FACTORIZED_JOIN_TREE") !=
      nullptr;
  if (disabled || fusion == nullptr || !fusion->eligible || fusion->fired ||
      outer != nullptr || !ctes.empty() || fusion->rollup_count_distribution ||
      fusion->anti_unmatched_only || fusion->aggregates.empty() ||
      fusion->aggregates.size() > kMaxCollapsedAggs) {
    return std::optional<Relation>{};
  }
  std::vector<size_t> active_rels;
  std::vector<int> rel_to_active(relations.size(), -1);
  for (size_t i = 0; i < relations.size(); ++i) {
    if (i < absorbed.size() && absorbed[i] == 0U) {
      if (relations[i].HasSpill() || relations[i].using_columns != nullptr ||
          relations[i].schema.ColumnCount() == 0) {
        return std::optional<Relation>{};
      }
      rel_to_active[i] = static_cast<int>(active_rels.size());
      active_rels.push_back(i);
    }
  }
  const size_t n_active = active_rels.size();
  if (n_active < 3 || n_active > 16) {
    return std::optional<Relation>{};
  }

  // Every active relation joins via pure column equality predicates ONLY.
  struct JoinEdge {
    size_t r0{0};
    slot_t c0{0};
    size_t r1{0};
    slot_t c1{0};
  };
  std::vector<JoinEdge> edges;
  std::vector<std::unordered_map<slot_t, size_t>> col_to_node(n_active);
  struct ColNode {
    size_t ar{0};
    slot_t col{0};
  };
  std::vector<ColNode> col_nodes;
  auto get_node = [&](size_t ar, slot_t col) -> size_t {
    auto it = col_to_node[ar].find(col);
    if (it != col_to_node[ar].end()) {
      return it->second;
    }
    const size_t id = col_nodes.size();
    col_nodes.push_back({ar, col});
    col_to_node[ar].emplace(col, id);
    return id;
  };

  for (const PredicateInfo& p : predicates) {
    if (!p.resolved || p.sources.size() < 2) {
      continue;
    }
    if (p.contains_query || p.sources.size() != 2 ||
        !IsColumnEqualityPredicate(p.expression) ||
        p.expression->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
      return std::optional<Relation>{};
    }
    auto sit = p.sources.begin();
    const size_t s0 = *sit++;
    const size_t s1 = *sit;
    if (s0 >= relations.size() || s1 >= relations.size() ||
        rel_to_active[s0] < 0 || rel_to_active[s1] < 0) {
      return std::optional<Relation>{};
    }
    const auto a0 = static_cast<size_t>(rel_to_active[s0]);
    const auto a1 = static_cast<size_t>(rel_to_active[s1]);
    const BinaryExpression& bin = p.expression->AsBinaryExpression();
    const ColumnName& lhs = bin.Left()->AsColumnValue().GetColumnName();
    const ColumnName& rhs = bin.Right()->AsColumnValue().GetColumnName();
    const auto l0 = LocalColumnOffset(relations[s0].schema, lhs);
    const auto r0 = LocalColumnOffset(relations[s0].schema, rhs);
    const auto l1 = LocalColumnOffset(relations[s1].schema, lhs);
    const auto r1 = LocalColumnOffset(relations[s1].schema, rhs);
    std::optional<slot_t> c0, c1;
    if (l0.has_value() && r1.has_value() && !r0.has_value() &&
        !l1.has_value()) {
      c0 = static_cast<slot_t>(*l0);
      c1 = static_cast<slot_t>(*r1);
    } else if (r0.has_value() && l1.has_value() && !l0.has_value() &&
               !r1.has_value()) {
      c0 = static_cast<slot_t>(*r0);
      c1 = static_cast<slot_t>(*l1);
    } else {
      return std::optional<Relation>{};
    }
    if (!HasSingleEquiKeyCompatible(relations[s0].schema.GetColumn(*c0),
                                    relations[s1].schema.GetColumn(*c1))) {
      return std::optional<Relation>{};
    }
    edges.push_back(JoinEdge{.r0 = a0, .c0 = *c0, .r1 = a1, .c1 = *c1});
  }
  if (edges.empty()) {
    return std::optional<Relation>{};
  }

  // Union-Find over column nodes to form Equivalence Classes (#12).
  std::vector<size_t> parent(col_nodes.size());
  for (const JoinEdge& e : edges) {
    get_node(e.r0, e.c0);
    get_node(e.r1, e.c1);
  }
  parent.resize(col_nodes.size());
  for (size_t i = 0; i < parent.size(); ++i) {
    parent[i] = i;
  }
  auto uf_find = [&](auto&& self, size_t x) -> size_t {
    return parent[x] == x ? x : (parent[x] = self(self, parent[x]));
  };
  for (const JoinEdge& e : edges) {
    const size_t n0 = get_node(e.r0, e.c0);
    const size_t n1 = get_node(e.r1, e.c1);
    const size_t rx = uf_find(uf_find, n0);
    const size_t ry = uf_find(uf_find, n1);
    if (rx != ry) {
      parent[ry] = rx;
    }
  }

  // Map equivalence-class roots to compact eq_class indices `0..E-1`.
  std::unordered_map<size_t, size_t> uf_to_eq;
  struct EqClassInfo {
    ValueType type{ValueType::kInt64};
    TextDomain text_domain{TextDomain::kUnspecified};
    // active_rel -> slot in that relation; reject if a relation puts two
    // distinct cols into the same eq class.
    std::vector<std::pair<size_t, slot_t>> members;
  };
  std::vector<EqClassInfo> eq_classes;
  for (size_t ni = 0; ni < col_nodes.size(); ++ni) {
    const size_t r = uf_find(uf_find, ni);
    auto [it, inserted] = uf_to_eq.emplace(r, eq_classes.size());
    if (inserted) {
      const Column& cdef =
          relations[active_rels[col_nodes[ni].ar]].schema.GetColumn(
              col_nodes[ni].col);
      eq_classes.push_back(EqClassInfo{.type = cdef.Type(),
                                       .text_domain = cdef.GetTextDomain(),
                                       .members = {}});
    }
    const size_t ec = it->second;
    const Column& cdef =
        relations[active_rels[col_nodes[ni].ar]].schema.GetColumn(
            col_nodes[ni].col);
    if (!HasSingleEquiKeyCompatible(
            cdef,
            relations[active_rels[eq_classes[ec].members.empty()
                                      ? col_nodes[ni].ar
                                      : eq_classes[ec].members[0].first]]
                .schema.GetColumn(eq_classes[ec].members.empty()
                                      ? col_nodes[ni].col
                                      : eq_classes[ec].members[0].second))) {
      return std::optional<Relation>{};
    }
    for (const auto& [existing_ar, _] : eq_classes[ec].members) {
      if (existing_ar == col_nodes[ni].ar) {
        return std::optional<Relation>{};  // intra-relation cycle in one eq
                                           // class
      }
    }
    eq_classes[ec].members.emplace_back(col_nodes[ni].ar, col_nodes[ni].col);
  }

  // Build bipartite graph (Relations `0..n_active-1`, EqClasses
  // `n_active..n_active+E-1`) and verify it is a single connected tree
  // (Berge-acyclic).
  const size_t n_eq = eq_classes.size();
  const size_t b_nodes = n_active + n_eq;
  size_t b_edge_count = 0;
  std::vector<std::vector<size_t>> b_adj(b_nodes);
  for (size_t ec = 0; ec < n_eq; ++ec) {
    for (const auto& [ar, _] : eq_classes[ec].members) {
      b_adj[ar].push_back(n_active + ec);
      b_adj[n_active + ec].push_back(ar);
      ++b_edge_count;
    }
  }
  if (b_edge_count != b_nodes - 1) {
    return std::optional<Relation>{};  // disconnected or cyclic
  }
  {
    std::vector<uint8_t> vis(b_nodes, 0);
    std::queue<size_t> q;
    vis[0] = 1;
    q.push(0);
    size_t visited = 0;
    while (!q.empty()) {
      const size_t u = q.front();
      q.pop();
      ++visited;
      for (const size_t v : b_adj[u]) {
        if (vis[v] == 0) {
          vis[v] = 1;
          q.push(v);
        }
      }
    }
    if (visited != b_nodes) {
      return std::optional<Relation>{};  // disconnected
    }
  }

  // Also ensure every active relation's join-key VARCHAR columns carry no
  // collation/interval.
  for (size_t ec = 0; ec < n_eq; ++ec) {
    if (eq_classes[ec].type != ValueType::kVarChar) {
      continue;
    }
    for (const auto& [ar, col] : eq_classes[ec].members) {
      const Relation& rel = relations[active_rels[ar]];
      bool bad = false;
      if (rel.HasColumnarOnly()) {
        for (const ColumnarChunk& ch : rel.columnar->chunks) {
          const ColumnarColumn& cc = ch.columns[col];
          if (cc.is_uniform && !cc.is_unsigned && !cc.is_interval &&
              cc.type == ValueType::kVarChar) {
            continue;
          }
          for (size_t ri = 0; ri < ch.rows; ++ri) {
            Value v;
            cc.WriteValueInto(ri, &v);
            if (!v.IsNull() && (v.IsCaseInsensitive() || v.IsInterval())) {
              bad = true;
              break;
            }
          }
          if (bad) {
            break;
          }
        }
      } else {
        for (const Row& row : rel.rows) {
          const Value& v = row[col];
          if (!v.IsNull() && (v.IsCaseInsensitive() || v.IsInterval())) {
            bad = true;
            break;
          }
        }
      }
      if (bad) {
        return std::optional<Relation>{};
      }
    }
  }

  // Collect all column references across GROUP BY, SELECT, HAVING, aggregates.
  // First verify SELECT & HAVING have no ungrouped non-agg columns outside
  // GROUP BY (using active-schema names), and every agg is a supported simple
  // column agg.
  Schema full_active_schema;
  std::vector<size_t> ar_schema_offset(n_active, 0);
  for (size_t ar = 0; ar < n_active; ++ar) {
    ar_schema_offset[ar] = full_active_schema.ColumnCount();
    full_active_schema = full_active_schema + relations[active_rels[ar]].schema;
  }
  auto resolve_to_ar_col =
      [&](const ColumnName& cn) -> std::optional<std::pair<size_t, slot_t>> {
    const auto off = LocalColumnOffset(full_active_schema, cn);
    if (!off.has_value()) {
      return std::nullopt;
    }
    for (size_t ar = n_active; ar-- > 0;) {
      if (static_cast<size_t>(*off) >= ar_schema_offset[ar]) {
        return std::make_pair(ar,
                              static_cast<slot_t>(static_cast<size_t>(*off) -
                                                  ar_schema_offset[ar]));
      }
    }
    return std::nullopt;
  };
  auto eq_class_of = [&](size_t ar, slot_t col) -> std::optional<size_t> {
    auto it = col_to_node[ar].find(col);
    if (it == col_to_node[ar].end()) {
      return std::nullopt;
    }
    return uf_to_eq[uf_find(uf_find, it->second)];
  };

  // Validate GROUP BY keys: must all be simple ColumnValues (or constant).
  struct GroupKeyRef {
    bool is_col{false};
    size_t orig_ar{0};
    slot_t orig_col{0};
    std::optional<size_t> eq_cls;
  };
  std::vector<GroupKeyRef> gk_refs(fusion->group_keys.size());
  for (size_t k = 0; k < fusion->group_keys.size(); ++k) {
    const Expression& gk = fusion->group_keys[k];
    if (!gk || ContainsQuery(gk) || ContainsParameterSlot(gk) ||
        !ExpressionCannotThrowForPlanner(gk)) {
      return std::optional<Relation>{};
    }
    if (gk->Type() != TypeTag::kColumnValue) {
      return std::optional<Relation>{};
    }
    auto resolved = resolve_to_ar_col(gk->AsColumnValue().GetColumnName());
    if (!resolved.has_value()) {
      return std::optional<Relation>{};
    }
    gk_refs[k] =
        GroupKeyRef{.is_col = true,
                    .orig_ar = resolved->first,
                    .orig_col = resolved->second,
                    .eq_cls = eq_class_of(resolved->first, resolved->second)};
  }

  // Classify aggregates.
  std::vector<SimpleAggKind> agg_kinds(fusion->aggregates.size());
  struct AggColRef {
    bool is_count_star{false};
    size_t ar{0};
    slot_t col{0};
    std::optional<size_t> eq_cls;
  };
  std::vector<AggColRef> agg_refs(fusion->aggregates.size());
  for (size_t ai = 0; ai < fusion->aggregates.size(); ++ai) {
    auto kind =
        ClassifySimpleAgg(fusion->aggregates[ai], fusion->count_star[ai]);
    if (!kind.has_value()) {
      return std::optional<Relation>{};
    }
    agg_kinds[ai] = *kind;
    if (fusion->count_star[ai]) {
      agg_refs[ai].is_count_star = true;
      continue;
    }
    const Expression& child = fusion->aggregates[ai]->Child();
    if (child->Type() != TypeTag::kColumnValue) {
      return std::optional<Relation>{};
    }
    auto resolved = resolve_to_ar_col(child->AsColumnValue().GetColumnName());
    if (!resolved.has_value()) {
      return std::optional<Relation>{};
    }
    agg_refs[ai] =
        AggColRef{.is_count_star = false,
                  .ar = resolved->first,
                  .col = resolved->second,
                  .eq_cls = eq_class_of(resolved->first, resolved->second)};
  }

  // Check SELECT and HAVING ungrouped columns: must all resolve to a GROUP BY
  // column or an equivalent column in the same equivalence class.
  const auto select_having_col_ok = [&](const ColumnName& cn) -> bool {
    auto res = resolve_to_ar_col(cn);
    if (!res.has_value()) {
      return false;
    }
    const auto ec = eq_class_of(res->first, res->second);
    return std::ranges::any_of(gk_refs, [&](const GroupKeyRef& g) {
      if (!g.is_col) {
        return false;
      }
      if (g.orig_ar == res->first && g.orig_col == res->second) {
        return true;
      }
      return ec.has_value() && g.eq_cls.has_value() && *ec == *g.eq_cls;
    });
  };
  if (fusion->having &&
      !AllNonAggColumnsAllowed(fusion->having, select_having_col_ok)) {
    return std::optional<Relation>{};
  }
  for (const Expression& se : fusion->select_items) {
    if (!AllNonAggColumnsAllowed(se, select_having_col_ok)) {
      return std::optional<Relation>{};
    }
  }

  // Pick best root active relation `R_root`:
  // Every GROUP BY key must map onto `R_root` (directly or via eq_class #12),
  // AND every aggregate must either map onto `R_root` or belong to a DIRECT
  // relation child of an eq-class attached to `R_root` (#11).
  // Among valid roots, pick the one with the SMALLEST TotalRows() (or fewest
  // value branches), e.g., `pl` (200 rows) in L1, `k` (2K) in J4, `u` (5K) in
  // C2!
  std::optional<size_t> chosen_root;
  size_t best_root_rows = std::numeric_limits<size_t>::max();
  for (size_t cand_root = 0; cand_root < n_active; ++cand_root) {
    auto eq_col_on_ar = [&](size_t ec, size_t ar) -> std::optional<slot_t> {
      for (const auto& [m_ar, m_col] : eq_classes[ec].members) {
        if (m_ar == ar) {
          return m_col;
        }
      }
      return std::nullopt;
    };
    bool ok = true;
    for (const GroupKeyRef& g : gk_refs) {
      if (g.orig_ar != cand_root &&
          (!g.eq_cls.has_value() || !eq_col_on_ar(*g.eq_cls, cand_root))) {
        ok = false;
        break;
      }
    }
    if (!ok) {
      continue;
    }

    // Identify direct neighbor relations of `cand_root` in the join tree
    // (those sharing any eq-class node adjacent to `cand_root`).
    std::unordered_set<size_t> direct_children;
    for (const size_t ec_node : b_adj[cand_root]) {
      const size_t ec = ec_node - n_active;
      for (const auto& [m_ar, _] : eq_classes[ec].members) {
        if (m_ar != cand_root) {
          direct_children.insert(m_ar);
        }
      }
    }
    for (const AggColRef& ar_ref : agg_refs) {
      if (ar_ref.is_count_star) {
        continue;
      }
      if (ar_ref.ar == cand_root) {
        continue;
      }
      if (ar_ref.eq_cls.has_value() &&
          eq_col_on_ar(*ar_ref.eq_cls, cand_root)) {
        continue;  // substituted onto cand_root!
      }
      if (!direct_children.contains(ar_ref.ar)) {
        ok = false;
        break;
      }
    }
    if (!ok) {
      continue;
    }
    const size_t cand_rows = relations[active_rels[cand_root]].TotalRows();
    if (cand_rows < best_root_rows) {
      best_root_rows = cand_rows;
      chosen_root = cand_root;
    }
  }
  if (!chosen_root.has_value()) {
    return std::optional<Relation>{};
  }
  const size_t root_ar = *chosen_root;

  // Map each GROUP BY key onto `root_ar` (using eq-class substitution #12!).
  std::vector<slot_t> root_group_cols(gk_refs.size());
  for (size_t k = 0; k < gk_refs.size(); ++k) {
    if (gk_refs[k].orig_ar == root_ar) {
      root_group_cols[k] = gk_refs[k].orig_col;
    } else {
      for (const auto& [m_ar, m_col] : eq_classes[*gk_refs[k].eq_cls].members) {
        if (m_ar == root_ar) {
          root_group_cols[k] = m_col;
          break;
        }
      }
    }
  }

  // Assign each aggregate as root-owned or direct-child-owned.
  std::vector<bool> agg_is_root(agg_refs.size(), true);
  std::vector<slot_t> agg_root_col(agg_refs.size(), 0);
  // direct child ar -> list of agg indices it owns.
  std::unordered_map<size_t, std::vector<size_t>> child_owned_aggs;
  for (size_t ai = 0; ai < agg_refs.size(); ++ai) {
    if (agg_refs[ai].is_count_star) {
      continue;
    }
    if (agg_refs[ai].ar == root_ar) {
      agg_root_col[ai] = agg_refs[ai].col;
      continue;
    }
    if (agg_refs[ai].eq_cls.has_value()) {
      bool mapped = false;
      for (const auto& [m_ar, m_col] :
           eq_classes[*agg_refs[ai].eq_cls].members) {
        if (m_ar == root_ar) {
          agg_root_col[ai] = m_col;
          mapped = true;
          break;
        }
      }
      if (mapped) {
        continue;
      }
    }
    agg_is_root[ai] = false;
    child_owned_aggs[agg_refs[ai].ar].push_back(ai);
  }

  // Build rooted bipartite tree from `root_ar`.
  // Relation node `ar` has parent eq-class `parent_eq[ar]` (`SIZE_MAX` for
  // root_ar), and child eq-classes -> child relation nodes `rel_children[ar]`:
  // Each child of `ar` is `{child_ar, parent_col_on_ar, child_join_col, ec}`.
  struct TreeChildLink {
    size_t child_ar{0};
    slot_t parent_col{0};
    slot_t child_col{0};
    size_t ec{0};
  };
  std::vector<std::vector<TreeChildLink>> tree_children(n_active);
  std::vector<size_t> post_order;
  post_order.reserve(n_active);
  {
    std::vector<int> b_parent(b_nodes, -1);
    std::vector<size_t> bfs;
    bfs.push_back(root_ar);
    b_parent[root_ar] = static_cast<int>(root_ar);
    for (size_t bi = 0; bi < bfs.size(); ++bi) {
      const size_t u = bfs[bi];
      for (const size_t v : b_adj[u]) {
        if (b_parent[v] >= 0) {
          continue;
        }
        b_parent[v] = static_cast<int>(u);
        bfs.push_back(v);
      }
    }
    for (size_t ar = 0; ar < n_active; ++ar) {
      if (ar == root_ar) {
        continue;
      }
      const auto ec_node = static_cast<size_t>(b_parent[ar]);
      const size_t ec = ec_node - n_active;
      const auto p_ar = static_cast<size_t>(b_parent[ec_node]);
      slot_t p_col = 0, c_col = 0;
      for (const auto& [m_ar, m_col] : eq_classes[ec].members) {
        if (m_ar == p_ar) {
          p_col = m_col;
        }
        if (m_ar == ar) {
          c_col = m_col;
        }
      }
      tree_children[p_ar].push_back(TreeChildLink{
          .child_ar = ar, .parent_col = p_col, .child_col = c_col, .ec = ec});
    }
    for (size_t i = bfs.size(); i-- > 0;) {
      if (bfs[i] < n_active) {
        post_order.push_back(bfs[i]);
      }
    }
  }

  // Summary storage per non-root relation `ar`: maps non-null parent-join key
  // -> (weight_sum, optionally value_aggs).
  struct DenseIntSumSlot {
    uint64_t row_count{0};
    uint64_t agg_count{0};
    __int128 sum_i128{0};
    __int128 pos_i128{0};
    __int128 neg_i128{0};
  };
  struct SubtreeSummary {
    ValueType key_type{ValueType::kInt64};
    bool is_int{true};
    bool use_dense{false};
    bool use_dense_counts_only{false};
    bool use_dense_int_agg{false};
    bool has_any_entry{false};
    int64_t dense_min{0};
    std::vector<uint32_t> dense_slots;
    std::vector<uint64_t> dense_counts;
    std::vector<DenseIntSumSlot> dense_int_aggs;
    FlatIntKeyMap<uint32_t> int_map;
    bool use_pack7{false};
    std::unordered_map<uint64_t, uint32_t> pack7_map;
    TransparentStringSlotMap str_map;
    std::vector<uint64_t> counts;
    std::vector<size_t> owned_aggs;        // global agg indices
    std::vector<AggSlotState> agg_states;  // [sid * owned_aggs.size() + k]

    [[gnu::always_inline]] void LookupInt(int64_t kv, bool* found,
                                          uint64_t* w_out,
                                          uint32_t* sid_out) const noexcept {
      *found = false;
      *w_out = 0;
      if (use_dense_counts_only) {
        const uint64_t off =
            static_cast<uint64_t>(kv) - static_cast<uint64_t>(dense_min);
        if (off < dense_counts.size()) {
          const uint64_t w = dense_counts[off];
          if (w > 0) {
            *sid_out = static_cast<uint32_t>(off);
            *w_out = w;
            *found = true;
          }
        }
        return;
      }
      if (use_dense_int_agg) {
        const uint64_t off =
            static_cast<uint64_t>(kv) - static_cast<uint64_t>(dense_min);
        if (off < dense_int_aggs.size()) {
          const uint64_t w = dense_int_aggs[off].row_count;
          if (w > 0) {
            *sid_out = static_cast<uint32_t>(off);
            *w_out = w;
            *found = true;
          }
        }
        return;
      }
      if (use_dense) {
        const uint64_t off =
            static_cast<uint64_t>(kv) - static_cast<uint64_t>(dense_min);
        if (off < dense_slots.size() && dense_slots[off] != kAbsentSlot) {
          *sid_out = dense_slots[off];
          *w_out = counts[*sid_out];
          *found = *w_out > 0;
        }
        return;
      }
      if (const uint32_t* s = int_map.Find(kv)) {
        *sid_out = *s;
        *w_out = counts[*sid_out];
        *found = *w_out > 0;
      }
    }

    void Lookup(const ColumnarChunk* chunk, const Row* p_row, size_t r_idx,
                slot_t p_col, Value* key_str_tmp, bool* found, uint64_t* w_out,
                uint32_t* sid_out, bool* bad) const noexcept {
      *found = false;
      *w_out = 0;
      if (is_int) {
        int64_t kv = 0;
        if (!ReadIntKeyFromEntry(chunk, p_row, r_idx, p_col, key_type, &kv,
                                 bad)) {
          return;
        }
        LookupInt(kv, found, w_out, sid_out);
        return;
      }
      std::string_view sv;
      if (!ReadStringKeyFromEntry(chunk, p_row, r_idx, p_col, &sv, key_str_tmp,
                                  bad)) {
        return;
      }
      if (use_pack7) {
        auto p7 = PackShortString7(sv);
        if (!p7) {
          return;
        }
        auto it = pack7_map.find(*p7);
        if (it != pack7_map.end()) {
          *sid_out = it->second;
          *w_out = counts[*sid_out];
          *found = *w_out > 0;
        }
        return;
      }
      auto it = str_map.find(sv);
      if (it != str_map.end()) {
        *sid_out = it->second;
        *w_out = counts[*sid_out];
        *found = *w_out > 0;
      }
    }
  };
  std::vector<SubtreeSummary> summaries(n_active);
  // Parent join column in `ar` when `ar != root_ar`.
  std::vector<slot_t> rel_parent_col(n_active, 0);
  std::vector<ValueType> rel_parent_key_type(n_active, ValueType::kInt64);
  for (size_t p_ar = 0; p_ar < n_active; ++p_ar) {
    for (const TreeChildLink& lnk : tree_children[p_ar]) {
      rel_parent_col[lnk.child_ar] = lnk.child_col;
      rel_parent_key_type[lnk.child_ar] = eq_classes[lnk.ec].type;
    }
  }

  // Group non-root active relations by tree depth (deepest first) so sibling
  // independent subtrees (e.g. both `posts` aliases in STATS-CEB C3 or
  // `comments`/`votes` in C1) build their SubtreeSummary in parallel.
  std::vector<size_t> ar_depth(n_active, 0);
  size_t max_depth = 0;
  {
    std::queue<size_t> dq;
    dq.push(root_ar);
    while (!dq.empty()) {
      const size_t u = dq.front();
      dq.pop();
      for (const TreeChildLink& lnk : tree_children[u]) {
        ar_depth[lnk.child_ar] = ar_depth[u] + 1;
        max_depth = std::max(max_depth, ar_depth[lnk.child_ar]);
        dq.push(lnk.child_ar);
      }
    }
  }
  std::vector<std::vector<size_t>> depth_levels(max_depth + 1);
  for (const size_t ar : post_order) {
    if (ar != root_ar) {
      depth_levels[ar_depth[ar]].push_back(ar);
    }
  }

  uint64_t total_comparisons = 0;

  // Build one non-root relation's SubtreeSummary (thread-safe across sibling
  // relations at the same tree depth).
  auto build_one_summary = [&](size_t ar, bool allow_inner_parallel,
                               uint64_t* comparisons_out) -> bool {
    const Relation& rel = relations[active_rels[ar]];
    SubtreeSummary& sum = summaries[ar];
    sum.key_type = rel_parent_key_type[ar];
    sum.is_int =
        (sum.key_type == ValueType::kInt64 || sum.key_type == ValueType::kDate);
    sum.use_pack7 = !sum.is_int;
    if (auto it = child_owned_aggs.find(ar); it != child_owned_aggs.end()) {
      sum.owned_aggs = it->second;
    }
    const size_t n_owned = sum.owned_aggs.size();
    const slot_t up_col = rel_parent_col[ar];
    const auto& children = tree_children[ar];

    uint64_t span = 0;
    if (sum.is_int && rel.TotalRows() > 0) {
      int64_t bmin = std::numeric_limits<int64_t>::max();
      int64_t bmax = std::numeric_limits<int64_t>::min();
      bool bad = false;
      if (rel.HasColumnarOnly()) {
        std::vector<int64_t> key_scratch;
        std::vector<uint8_t> null_scratch;
        for (const ColumnarChunk& chunk : rel.columnar->chunks) {
          if (chunk.rows == 0) {
            continue;
          }
          const DecodedIntColumnView kv_view =
              DecodeIntColumnChunk(chunk.columns[up_col], chunk.rows,
                                   sum.key_type, &key_scratch, &null_scratch);
          if (kv_view.ok) {
            if (kv_view.all_null) {
              continue;
            }
            if (kv_view.is_const) {
              bmin = std::min(bmin, kv_view.const_val);
              bmax = std::max(bmax, kv_view.const_val);
            } else if (kv_view.nulls == nullptr) {
              const int64_t* vals = kv_view.values;
              for (size_t ri = 0; ri < chunk.rows; ++ri) {
                const int64_t v = vals[ri];
                bmin = std::min(v, bmin);
                bmax = std::max(v, bmax);
              }
            } else {
              const int64_t* vals = kv_view.values;
              const uint8_t* nulls = kv_view.nulls;
              for (size_t ri = 0; ri < chunk.rows; ++ri) {
                if (nulls[ri] == 0) {
                  const int64_t v = vals[ri];
                  bmin = std::min(v, bmin);
                  bmax = std::max(v, bmax);
                }
              }
            }
          } else {
            for (size_t ri = 0; ri < chunk.rows; ++ri) {
              int64_t kv = 0;
              if (ReadIntKeyFromEntry(&chunk, nullptr, ri, up_col, sum.key_type,
                                      &kv, &bad)) {
                bmin = std::min(kv, bmin);
                bmax = std::max(kv, bmax);
              }
              if (bad) {
                return false;
              }
            }
          }
        }
      } else {
        ForEachRelationEntry(
            rel, [&](const ColumnarChunk* chunk, const Row* r, size_t ri) {
              int64_t kv = 0;
              if (ReadIntKeyFromEntry(chunk, r, ri, up_col, sum.key_type, &kv,
                                      &bad)) {
                bmin = std::min(bmin, kv);
                bmax = std::max(bmax, kv);
              }
              return !bad;
            });
      }
      if (bad) {
        return false;
      }
      if (bmin <= bmax) {
        span = static_cast<uint64_t>(bmax) - static_cast<uint64_t>(bmin) + 1ULL;
        const size_t max_dense_span = std::min<size_t>(
            4ULL * 1024ULL * 1024ULL,
            std::max<size_t>(kMaxDenseKeySpan, rel.TotalRows() * 4ULL));
        if (span > 0 && span <= max_dense_span) {
          sum.use_dense = true;
          sum.dense_min = bmin;
        }
      }
    }

    // Fast path 1: dense count-only leaf (no owned aggregates, no children).
    if (sum.use_dense && n_owned == 0 && children.empty() &&
        rel.HasColumnarOnly()) {
      sum.use_dense_counts_only = true;
      sum.dense_counts.assign(static_cast<size_t>(span), 0);
      uint64_t* dc = sum.dense_counts.data();
      const auto dmin = static_cast<uint64_t>(sum.dense_min);
      uint64_t local_cnt = 0;
      bool bad = false;
      std::vector<int64_t> key_scratch;
      std::vector<uint8_t> null_scratch;
      for (const ColumnarChunk& chunk : rel.columnar->chunks) {
        if (chunk.rows == 0) {
          continue;
        }
        const DecodedIntColumnView kv_view =
            DecodeIntColumnChunk(chunk.columns[up_col], chunk.rows,
                                 sum.key_type, &key_scratch, &null_scratch);
        if (kv_view.ok) {
          if (kv_view.all_null) {
            continue;
          }
          if (kv_view.is_const) {
            const auto off = static_cast<size_t>(
                static_cast<uint64_t>(kv_view.const_val) - dmin);
            dc[off] += chunk.rows;
            local_cnt += chunk.rows;
          } else if (kv_view.nulls == nullptr) {
            const int64_t* vals = kv_view.values;
            for (size_t ri = 0; ri < chunk.rows; ++ri) {
              ++dc[static_cast<size_t>(static_cast<uint64_t>(vals[ri]) - dmin)];
            }
            local_cnt += chunk.rows;
          } else {
            const int64_t* vals = kv_view.values;
            const uint8_t* nulls = kv_view.nulls;
            for (size_t ri = 0; ri < chunk.rows; ++ri) {
              if (nulls[ri] == 0) {
                ++dc[static_cast<size_t>(static_cast<uint64_t>(vals[ri]) -
                                         dmin)];
                ++local_cnt;
              }
            }
          }
        } else {
          for (size_t ri = 0; ri < chunk.rows; ++ri) {
            int64_t kv = 0;
            if (ReadIntKeyFromEntry(&chunk, nullptr, ri, up_col, sum.key_type,
                                    &kv, &bad)) {
              ++dc[static_cast<size_t>(static_cast<uint64_t>(kv) - dmin)];
              ++local_cnt;
            }
            if (bad) {
              return false;
            }
          }
        }
      }
      sum.has_any_entry = (local_cnt > 0);
      *comparisons_out = local_cnt;
      return true;
    }

    // Fast path 1b: dense count-only single-child chain node (no owned
    // aggregates, 1 INT64/DATE child, e.g. `comments` and `posts` in STATS-CEB
    // C2). Decodes both parent-join key and child-join key per chunk and folds
    // child weights directly into `dense_counts`.
    if (sum.use_dense && n_owned == 0 && children.size() == 1 &&
        summaries[children[0].child_ar].is_int && rel.HasColumnarOnly()) {
      const TreeChildLink& ch = children[0];
      const SubtreeSummary& ch_sum = summaries[ch.child_ar];
      sum.use_dense_counts_only = true;
      sum.dense_counts.assign(static_cast<size_t>(span), 0);
      const auto dmin = static_cast<uint64_t>(sum.dense_min);
      const auto& chunks = rel.columnar->chunks;
      const size_t nchunks = chunks.size();
      const size_t inner_workers =
          (allow_inner_parallel && rel.TotalRows() >= 65536 &&
           span <= 524288ULL && nchunks >= 2)
              ? std::min<size_t>(ParallelWorkerCount(std::min<size_t>(
                                     4, rel.TotalRows() / 32768)),
                                 nchunks)
              : 1;
      auto run_chain_range = [&](size_t c_begin, size_t c_end,
                                 uint64_t* __restrict__ dc,
                                 uint64_t* local_cnt_out) -> bool {
        uint64_t local_cnt = 0;
        bool bad = false;
        std::vector<int64_t> up_scratch;
        std::vector<uint8_t> up_null_scratch;
        std::vector<int64_t> ch_scratch;
        std::vector<uint8_t> ch_null_scratch;
        for (size_t chi = c_begin; chi < c_end; ++chi) {
          const ColumnarChunk& chunk = chunks[chi];
          if (chunk.rows == 0) {
            continue;
          }
          const DecodedIntColumnView up_view =
              DecodeIntColumnChunk(chunk.columns[up_col], chunk.rows,
                                   sum.key_type, &up_scratch, &up_null_scratch);
          const DecodedIntColumnView ch_view = DecodeIntColumnChunk(
              chunk.columns[ch.parent_col], chunk.rows, ch_sum.key_type,
              &ch_scratch, &ch_null_scratch);
          if (up_view.ok && ch_view.ok) {
            if (up_view.all_null || ch_view.all_null) {
              continue;
            }
            if (!up_view.is_const && up_view.nulls == nullptr &&
                !ch_view.is_const && ch_view.nulls == nullptr) {
              const int64_t* uvals = up_view.values;
              const int64_t* cvals = ch_view.values;
              for (size_t ri = 0; ri < chunk.rows; ++ri) {
                bool found = false;
                uint64_t cw = 0;
                uint32_t dummy_sid = 0;
                ch_sum.LookupInt(cvals[ri], &found, &cw, &dummy_sid);
                if (!found || cw == 0) {
                  continue;
                }
                const auto off = static_cast<size_t>(
                    static_cast<uint64_t>(uvals[ri]) - dmin);
                uint64_t next_cnt = 0;
                if (__builtin_add_overflow(dc[off], cw, &next_cnt))
                    [[unlikely]] {
                  return false;
                }
                dc[off] = next_cnt;
                ++local_cnt;
              }
            } else {
              for (size_t ri = 0; ri < chunk.rows; ++ri) {
                if ((up_view.nulls != nullptr && up_view.nulls[ri] != 0) ||
                    (ch_view.nulls != nullptr && ch_view.nulls[ri] != 0)) {
                  continue;
                }
                const int64_t ckv =
                    ch_view.is_const ? ch_view.const_val : ch_view.values[ri];
                bool found = false;
                uint64_t cw = 0;
                uint32_t dummy_sid = 0;
                ch_sum.LookupInt(ckv, &found, &cw, &dummy_sid);
                if (!found || cw == 0) {
                  continue;
                }
                const int64_t ukv =
                    up_view.is_const ? up_view.const_val : up_view.values[ri];
                const auto off =
                    static_cast<size_t>(static_cast<uint64_t>(ukv) - dmin);
                uint64_t next_cnt = 0;
                if (__builtin_add_overflow(dc[off], cw, &next_cnt))
                    [[unlikely]] {
                  return false;
                }
                dc[off] = next_cnt;
                ++local_cnt;
              }
            }
          } else {
            for (size_t ri = 0; ri < chunk.rows; ++ri) {
              int64_t ckv = 0;
              if (!ReadIntKeyFromEntry(&chunk, nullptr, ri, ch.parent_col,
                                       ch_sum.key_type, &ckv, &bad)) {
                if (bad) {
                  return false;
                }
                continue;
              }
              bool found = false;
              uint64_t cw = 0;
              uint32_t dummy_sid = 0;
              ch_sum.LookupInt(ckv, &found, &cw, &dummy_sid);
              if (!found || cw == 0) {
                continue;
              }
              int64_t ukv = 0;
              if (!ReadIntKeyFromEntry(&chunk, nullptr, ri, up_col,
                                       sum.key_type, &ukv, &bad)) {
                if (bad) {
                  return false;
                }
                continue;
              }
              const auto off =
                  static_cast<size_t>(static_cast<uint64_t>(ukv) - dmin);
              uint64_t next_cnt = 0;
              if (__builtin_add_overflow(dc[off], cw, &next_cnt)) [[unlikely]] {
                return false;
              }
              dc[off] = next_cnt;
              ++local_cnt;
            }
          }
        }
        *local_cnt_out = local_cnt;
        return true;
      };
      uint64_t total_local = 0;
      if (inner_workers <= 1) {
        if (!run_chain_range(0, nchunks, sum.dense_counts.data(),
                             &total_local)) {
          return false;
        }
      } else {
        std::vector<std::vector<uint64_t>> w_dc(inner_workers);
        std::vector<uint8_t> w_ok(inner_workers, 0);
        std::vector<uint64_t> w_cnt(inner_workers, 0);
        const Status pst = RunParallelTasks(inner_workers, [&](size_t w) {
          const size_t cb = (nchunks * w) / inner_workers;
          const size_t ce = (nchunks * (w + 1)) / inner_workers;
          uint64_t* target_dc = nullptr;
          if (w == 0) {
            target_dc = sum.dense_counts.data();
          } else {
            w_dc[w].assign(static_cast<size_t>(span), 0);
            target_dc = w_dc[w].data();
          }
          w_ok[w] = run_chain_range(cb, ce, target_dc, &w_cnt[w]) ? 1U : 0U;
        });
        if (!pst.ok()) {
          return false;
        }
        for (size_t w = 0; w < inner_workers; ++w) {
          if (w_ok[w] == 0U) {
            return false;
          }
          total_local += w_cnt[w];
        }
        uint64_t* __restrict__ dst = sum.dense_counts.data();
        const auto span_sz = static_cast<size_t>(span);
        for (size_t w = 1; w < inner_workers; ++w) {
          const uint64_t* __restrict__ src = w_dc[w].data();
          for (size_t i = 0; i < span_sz; ++i) {
            if (src[i] == 0) {
              continue;
            }
            uint64_t next_cnt = 0;
            if (__builtin_add_overflow(dst[i], src[i], &next_cnt))
                [[unlikely]] {
              return false;
            }
            dst[i] = next_cnt;
          }
        }
      }
      sum.has_any_entry = (total_local > 0);
      *comparisons_out = total_local;
      return true;
    }

    // Fast path 2: dense single INT64 SUM/AVG leaf (e.g. AVG(p.score) on
    // `posts p` in STATS-CEB C3). Stores (row_count, agg_count, sum/pos/neg) in
    // one cache-line-sized POD slot per key — zero indirect slot table, zero
    // Value destructor overhead.
    if (sum.use_dense && n_owned == 1 && children.empty() &&
        rel.HasColumnarOnly() && span <= 2ULL * 1024ULL * 1024ULL) {
      const size_t ai = sum.owned_aggs[0];
      if (agg_kinds[ai] == SimpleAggKind::kSum ||
          agg_kinds[ai] == SimpleAggKind::kAvg) {
        const slot_t agg_col = agg_refs[ai].col;
        bool all_chunks_int64 = true;
        for (const ColumnarChunk& chunk : rel.columnar->chunks) {
          if (chunk.rows == 0) {
            continue;
          }
          const ColumnarColumn& acc = chunk.columns[agg_col];
          if (!acc.is_uniform || acc.is_unsigned || acc.is_interval ||
              acc.type != ValueType::kInt64) {
            all_chunks_int64 = false;
            break;
          }
        }
        if (all_chunks_int64) {
          sum.use_dense_int_agg = true;
          sum.dense_int_aggs.resize(static_cast<size_t>(span));
          DenseIntSumSlot* slots = sum.dense_int_aggs.data();
          const auto dmin = static_cast<uint64_t>(sum.dense_min);
          uint64_t local_cnt = 0;
          bool bad = false;
          std::vector<int64_t> key_scratch;
          std::vector<uint8_t> key_null_scratch;
          std::vector<int64_t> agg_scratch;
          std::vector<uint8_t> agg_null_scratch;
          for (const ColumnarChunk& chunk : rel.columnar->chunks) {
            if (chunk.rows == 0) {
              continue;
            }
            const DecodedIntColumnView kv_view = DecodeIntColumnChunk(
                chunk.columns[up_col], chunk.rows, sum.key_type, &key_scratch,
                &key_null_scratch);
            const DecodedIntColumnView av_view = DecodeIntColumnChunk(
                chunk.columns[agg_col], chunk.rows, ValueType::kInt64,
                &agg_scratch, &agg_null_scratch);
            if (kv_view.ok && av_view.ok) {
              if (kv_view.all_null) {
                continue;
              }
              if (!kv_view.is_const && kv_view.nulls == nullptr &&
                  !av_view.is_const && av_view.nulls == nullptr) {
                const int64_t* kvals = kv_view.values;
                const int64_t* avals = av_view.values;
                for (size_t ri = 0; ri < chunk.rows; ++ri) {
                  DenseIntSumSlot& st = slots[static_cast<size_t>(
                      static_cast<uint64_t>(kvals[ri]) - dmin)];
                  ++st.row_count;
                  const int64_t v = avals[ri];
                  ++st.agg_count;
                  st.sum_i128 += v;
                  if (v > 0) {
                    st.pos_i128 += v;
                  } else if (v < 0) {
                    st.neg_i128 += v;
                  }
                }
                local_cnt += chunk.rows;
              } else {
                for (size_t ri = 0; ri < chunk.rows; ++ri) {
                  if (kv_view.nulls != nullptr && kv_view.nulls[ri] != 0) {
                    continue;
                  }
                  const int64_t kv =
                      kv_view.is_const ? kv_view.const_val : kv_view.values[ri];
                  DenseIntSumSlot& st = slots[static_cast<size_t>(
                      static_cast<uint64_t>(kv) - dmin)];
                  ++st.row_count;
                  ++local_cnt;
                  if (!av_view.all_null &&
                      (av_view.nulls == nullptr || av_view.nulls[ri] == 0)) {
                    const int64_t v = av_view.is_const ? av_view.const_val
                                                       : av_view.values[ri];
                    ++st.agg_count;
                    st.sum_i128 += v;
                    if (v > 0) {
                      st.pos_i128 += v;
                    } else if (v < 0) {
                      st.neg_i128 += v;
                    }
                  }
                }
              }
            } else {
              for (size_t ri = 0; ri < chunk.rows; ++ri) {
                int64_t kv = 0;
                if (!ReadIntKeyFromEntry(&chunk, nullptr, ri, up_col,
                                         sum.key_type, &kv, &bad)) {
                  if (bad) {
                    return false;
                  }
                  continue;
                }
                DenseIntSumSlot& st = slots[static_cast<size_t>(
                    static_cast<uint64_t>(kv) - dmin)];
                ++st.row_count;
                ++local_cnt;
                int64_t v = 0;
                if (chunk.columns[agg_col].TryReadNonNullInt(ri, &v)) {
                  ++st.agg_count;
                  st.sum_i128 += v;
                  if (v > 0) {
                    st.pos_i128 += v;
                  } else if (v < 0) {
                    st.neg_i128 += v;
                  }
                }
              }
            }
          }
          sum.has_any_entry = (local_cnt > 0);
          *comparisons_out = local_cnt;
          return true;
        }
      }
    }

    if (sum.use_dense) {
      sum.dense_slots.assign(static_cast<size_t>(span), kAbsentSlot);
      const size_t est =
          std::min<size_t>(static_cast<size_t>(span), rel.TotalRows());
      sum.counts.reserve(est);
      if (n_owned > 0) {
        sum.agg_states.reserve(est * n_owned);
      }
    }

    Value sum_key_tmp;
    auto alloc_sum_slot = [&](const ColumnarChunk* chunk, const Row* row,
                              size_t r_idx,
                              bool* bad) -> std::optional<uint32_t> {
      if (sum.is_int) {
        int64_t kv = 0;
        if (!ReadIntKeyFromEntry(chunk, row, r_idx, up_col, sum.key_type, &kv,
                                 bad)) {
          return std::nullopt;
        }
        if (sum.use_dense) {
          const auto off = static_cast<size_t>(
              static_cast<uint64_t>(kv) - static_cast<uint64_t>(sum.dense_min));
          uint32_t& ref = sum.dense_slots[off];
          if (ref == kAbsentSlot) {
            ref = static_cast<uint32_t>(sum.counts.size());
            sum.counts.push_back(0);
            if (n_owned > 0) {
              sum.agg_states.resize(sum.counts.size() * n_owned);
            }
          }
          return ref;
        }
        if (const uint32_t* existing = sum.int_map.Find(kv)) {
          return *existing;
        }
        const auto sid = static_cast<uint32_t>(sum.counts.size());
        sum.counts.push_back(0);
        if (n_owned > 0) {
          sum.agg_states.resize(sum.counts.size() * n_owned);
        }
        *sum.int_map.TryEmplace(kv) = sid;
        return sid;
      }
      std::string_view sv;
      if (!ReadStringKeyFromEntry(chunk, row, r_idx, up_col, &sv, &sum_key_tmp,
                                  bad)) {
        return std::nullopt;
      }
      if (sum.use_pack7) {
        if (auto p7 = PackShortString7(sv)) {
          auto [it, ins] = sum.pack7_map.emplace(
              *p7, static_cast<uint32_t>(sum.counts.size()));
          if (ins) {
            sum.counts.push_back(0);
            if (n_owned > 0) {
              sum.agg_states.resize(sum.counts.size() * n_owned);
            }
          }
          return it->second;
        }
        sum.use_pack7 = false;
        sum.str_map.reserve(sum.pack7_map.size() + 64);
        for (const auto& [word, sid] : sum.pack7_map) {
          const auto len = static_cast<size_t>(word & 0xFFU);
          std::string tmp(len, '\0');
          for (size_t i = 0; i < len; ++i) {
            tmp[i] = static_cast<char>((word >> ((7U - i) * 8U)) & 0xFFU);
          }
          sum.str_map.emplace(std::move(tmp), sid);
        }
        sum.pack7_map.clear();
      }
      auto it = sum.str_map.find(sv);
      if (it != sum.str_map.end()) {
        return it->second;
      }
      const auto sid = static_cast<uint32_t>(sum.counts.size());
      sum.counts.push_back(0);
      if (n_owned > 0) {
        sum.agg_states.resize(sum.counts.size() * n_owned);
      }
      sum.str_map.emplace(std::string(sv), sid);
      return sid;
    };

    bool invalid = false;
    uint64_t local_comparisons = 0;
    Value child_key_tmp;
    ForEachRelationEntry(rel, [&](const ColumnarChunk* chunk, const Row* r,
                                  size_t ri) {
      __int128 w = 1;
      for (const TreeChildLink& ch : children) {
        bool found = false;
        uint64_t cw = 0;
        uint32_t dummy_sid = 0;
        summaries[ch.child_ar].Lookup(chunk, r, ri, ch.parent_col,
                                      &child_key_tmp, &found, &cw, &dummy_sid,
                                      &invalid);
        if (invalid) {
          return false;
        }
        if (!found) {
          return true;  // semi-pruned by empty child
        }
        w *= static_cast<__int128>(cw);
        if (std::cmp_greater(w, std::numeric_limits<uint64_t>::max())) {
          invalid = true;
          return false;
        }
      }
      auto sid = alloc_sum_slot(chunk, r, ri, &invalid);
      if (invalid) {
        return false;
      }
      if (!sid.has_value()) {
        return true;
      }
      const auto wu = static_cast<uint64_t>(w);
      __int128 next_cnt =
          static_cast<__int128>(sum.counts[*sid]) + static_cast<__int128>(wu);
      if (std::cmp_greater(next_cnt, std::numeric_limits<uint64_t>::max())) {
        invalid = true;
        return false;
      }
      sum.counts[*sid] = static_cast<uint64_t>(next_cnt);
      ++local_comparisons;
      if (n_owned > 0) {
        for (size_t oi = 0; oi < n_owned; ++oi) {
          const size_t ai = sum.owned_aggs[oi];
          if (!AddEntryCellWithWeight(&sum.agg_states[(*sid * n_owned) + oi],
                                      agg_kinds[ai], chunk, r, ri,
                                      agg_refs[ai].col, wu)) {
            invalid = true;
            return false;
          }
        }
      }
      return true;
    });
    if (invalid) {
      return false;
    }
    sum.has_any_entry = !sum.counts.empty();
    *comparisons_out = local_comparisons;
    return true;
  };

  // Bottom-up level-parallel evaluation for all non-root relations (`#9` &
  // `#11`).
  for (size_t depth = max_depth; depth >= 1; --depth) {
    const auto& level = depth_levels[depth];
    if (level.empty()) {
      continue;
    }
    size_t level_rows = 0;
    for (const size_t ar : level) {
      level_rows += relations[active_rels[ar]].TotalRows();
    }
    std::vector<uint8_t> ok_flags(level.size(), 0);
    std::vector<uint64_t> comp_counts(level.size(), 0);
    if (level.size() > 1 && level_rows >= 16384) {
      RETURN_IF_FAIL(RunParallelTasks(level.size(), [&](size_t idx) {
        ok_flags[idx] =
            build_one_summary(level[idx], /*allow_inner_parallel=*/false,
                              &comp_counts[idx])
                ? 1U
                : 0U;
      }));
    } else {
      for (size_t idx = 0; idx < level.size(); ++idx) {
        ok_flags[idx] =
            build_one_summary(level[idx],
                              /*allow_inner_parallel=*/(level.size() == 1),
                              &comp_counts[idx])
                ? 1U
                : 0U;
      }
    }
    for (size_t idx = 0; idx < level.size(); ++idx) {
      if (ok_flags[idx] == 0U) {
        return std::optional<Relation>{};
      }
      total_comparisons += comp_counts[idx];
    }
    for (const size_t ar : level) {
      if (!summaries[ar].has_any_entry) {
        // Any empty subtree annihilates an inner join!
        std::optional<Row> empty_rep;
        if (fusion->group_keys.empty()) {
          empty_rep = Row(std::vector<Value>(full_active_schema.ColumnCount()));
        }
        std::vector<const Relation*> keep_rels;
        keep_rels.reserve(active_rels.size());
        for (const size_t idx : active_rels) {
          keep_rels.push_back(&relations[idx]);
        }
        auto res_or = EmitGroupedProjectionRelation(
            context, full_active_schema, {}, {}, agg_kinds, empty_rep, *fusion,
            n_active - 1, total_comparisons, keep_rels);
        if (!res_or.HasValue()) {
          if (!IsEagerFallbackStatus(res_or.GetStatus())) {
            return res_or.GetStatus();
          }
          return std::optional<Relation>{};
        }
        fusion->fired = true;
        return std::optional<Relation>(res_or.MoveValue());
      }
    }
  }

  // Root scan in parallel (`root_ar`).
  const Relation& root_rel = relations[active_rels[root_ar]];
  const auto& root_children = tree_children[root_ar];
  const size_t n_rc = root_children.size();
  const size_t total_root = root_rel.TotalRows();
  const size_t workers =
      ParallelWorkerCount(std::max<size_t>(1, total_root / 16384));
  const size_t n_aggs = agg_kinds.size();
  const size_t n_gkeys = root_group_cols.size();
  const ValueType gk0_type =
      n_gkeys == 1 ? root_rel.schema.GetColumn(root_group_cols[0]).Type()
                   : ValueType::kNull;
  const bool single_int_gk =
      (n_gkeys == 1 &&
       (gk0_type == ValueType::kInt64 || gk0_type == ValueType::kDate) &&
       !root_rel.schema.GetColumn(root_group_cols[0]).IsUnsigned());
  bool all_root_children_int = true;
  for (size_t ci = 0; ci < n_rc; ++ci) {
    if (!summaries[root_children[ci].child_ar].is_int) {
      all_root_children_int = false;
      break;
    }
  }
  bool any_root_non_count_star_agg = false;
  for (size_t ai = 0; ai < n_aggs; ++ai) {
    if (agg_kinds[ai] != SimpleAggKind::kCountStar && agg_is_root[ai]) {
      any_root_non_count_star_agg = true;
      break;
    }
  }

  struct RootWorkerLocal {
    bool has_scalar{false};
    std::vector<AggSlotState> scalar_aggs;
    FlatIntKeyMap<uint32_t> int_map;
    uint32_t null_group_slot{kAbsentSlot};
    std::vector<int64_t> int_group_keys;
    std::vector<uint8_t> int_group_is_null;
    std::unordered_map<std::string, uint32_t> str_map;
    std::vector<std::vector<Value>> group_keys;
    std::vector<std::vector<AggSlotState>> group_aggs;
    uint64_t comparisons{0};
    bool invalid{false};
  };
  std::vector<RootWorkerLocal> locals(workers);
  for (auto& wl : locals) {
    if (n_gkeys == 0) {
      wl.scalar_aggs.resize(n_aggs);
    }
  }

  const auto merge_child_owned_aggs =
      [&](std::vector<AggSlotState>* dst, const uint64_t* cw,
          const uint32_t* csid, uint64_t wt) noexcept -> bool {
    // Star-hub child value branches (#11): scale by W_other = W_total /
    // cw[ci].
    for (size_t ci = 0; ci < n_rc; ++ci) {
      const SubtreeSummary& ch_sum = summaries[root_children[ci].child_ar];
      if (ch_sum.owned_aggs.empty()) {
        continue;
      }
      const uint64_t w_other = wt / cw[ci];
      if (ch_sum.use_dense_int_agg) {
        const DenseIntSumSlot& ds = ch_sum.dense_int_aggs[csid[ci]];
        if (ds.agg_count > 0) {
          const size_t ai = ch_sum.owned_aggs[0];
          AggSlotState& target = (*dst)[ai];
          if (target.numeric_mode == 0) {
            target.numeric_mode = 1;
          } else if (target.numeric_mode != 1) {
            target.invalid = true;
            return false;
          }
          const auto sc = static_cast<__int128>(w_other);
          target.sum_i128 += ds.sum_i128 * sc;
          target.pos_i128 += ds.pos_i128 * sc;
          target.neg_i128 += ds.neg_i128 * sc;
          target.count += ds.agg_count * w_other;
        }
        continue;
      }
      const size_t n_owned = ch_sum.owned_aggs.size();
      const size_t base = csid[ci] * n_owned;
      for (size_t oi = 0; oi < n_owned; ++oi) {
        const size_t ai = ch_sum.owned_aggs[oi];
        if (!MergeAggSlot(&(*dst)[ai], ch_sum.agg_states[base + oi],
                          agg_kinds[ai], w_other)) {
          return false;
        }
      }
    }
    return true;
  };

  const auto get_single_int_group =
      [&](RootWorkerLocal& wl, bool is_null,
          int64_t kv) -> std::vector<AggSlotState>* {
    if (is_null) {
      if (wl.null_group_slot == kAbsentSlot) {
        wl.null_group_slot = static_cast<uint32_t>(wl.group_aggs.size());
        wl.int_group_keys.push_back(0);
        wl.int_group_is_null.push_back(1U);
        wl.group_aggs.emplace_back(n_aggs);
      }
      return &wl.group_aggs[wl.null_group_slot];
    }
    if (const uint32_t* found = wl.int_map.Find(kv)) {
      return &wl.group_aggs[*found];
    }
    const auto gid = static_cast<uint32_t>(wl.group_aggs.size());
    *wl.int_map.TryEmplace(kv) = gid;
    wl.int_group_keys.push_back(kv);
    wl.int_group_is_null.push_back(0U);
    wl.group_aggs.emplace_back(n_aggs);
    return &wl.group_aggs[gid];
  };

  if (root_rel.HasColumnarOnly() && all_root_children_int &&
      (n_gkeys == 0 || single_int_gk) && !any_root_non_count_star_agg) {
    // Fast columnar chunk-batched root probe: decodes each child join key and
    // optional single INT64/DATE group key once per chunk, avoiding per-row
    // ColumnarColumn::IntAt dispatch and EncodeKeyTuple string allocations.
    const auto& chunks = root_rel.columnar->chunks;
    const size_t chunk_count = chunks.size();
    const size_t actual_workers =
        std::min(workers, std::max<size_t>(1, chunk_count));
    const auto run_chunk_worker = [&](size_t w) {
      RootWorkerLocal& wl = locals[w];
      const size_t c_begin = (chunk_count * w) / actual_workers;
      const size_t c_end = (chunk_count * (w + 1)) / actual_workers;
      std::vector<uint64_t> cw(n_rc);
      std::vector<uint32_t> csid(n_rc);
      std::vector<std::vector<int64_t>> child_val_scratch(n_rc);
      std::vector<std::vector<uint8_t>> child_null_scratch(n_rc);
      std::vector<DecodedIntColumnView> child_views(n_rc);
      std::vector<int64_t> gk_val_scratch;
      std::vector<uint8_t> gk_null_scratch;
      Value child_key_tmp;
      for (size_t chi = c_begin; chi < c_end; ++chi) {
        const ColumnarChunk& chunk = chunks[chi];
        if (chunk.rows == 0) {
          continue;
        }
        bool all_decoded = true;
        bool any_all_null = false;
        for (size_t ci = 0; ci < n_rc; ++ci) {
          const SubtreeSummary& ch_sum = summaries[root_children[ci].child_ar];
          child_views[ci] = DecodeIntColumnChunk(
              chunk.columns[root_children[ci].parent_col], chunk.rows,
              ch_sum.key_type, &child_val_scratch[ci], &child_null_scratch[ci]);
          if (!child_views[ci].ok) {
            all_decoded = false;
            break;
          }
          if (child_views[ci].all_null) {
            any_all_null = true;
          }
        }
        if (all_decoded && any_all_null) {
          continue;
        }
        DecodedIntColumnView gk_view;
        if (all_decoded && single_int_gk) {
          gk_view = DecodeIntColumnChunk(chunk.columns[root_group_cols[0]],
                                         chunk.rows, gk0_type, &gk_val_scratch,
                                         &gk_null_scratch);
          if (!gk_view.ok) {
            all_decoded = false;
          }
        }
        if (all_decoded) {
          for (size_t ri = 0; ri < chunk.rows; ++ri) {
            __int128 w_total = 1;
            bool matched = true;
            for (size_t ci = 0; ci < n_rc; ++ci) {
              const DecodedIntColumnView& cv = child_views[ci];
              if (cv.nulls != nullptr && cv.nulls[ri] != 0) {
                matched = false;
                break;
              }
              const int64_t kv = cv.is_const ? cv.const_val : cv.values[ri];
              bool found = false;
              summaries[root_children[ci].child_ar].LookupInt(
                  kv, &found, &cw[ci], &csid[ci]);
              if (!found || cw[ci] == 0) {
                matched = false;
                break;
              }
              w_total *= static_cast<__int128>(cw[ci]);
              if (std::cmp_greater(w_total,
                                   std::numeric_limits<uint64_t>::max())) {
                wl.invalid = true;
                return;
              }
            }
            if (!matched) {
              continue;
            }
            const auto wt = static_cast<uint64_t>(w_total);
            wl.comparisons += 1;
            std::vector<AggSlotState>* dst = nullptr;
            if (n_gkeys == 0) {
              wl.has_scalar = true;
              dst = &wl.scalar_aggs;
            } else {
              const bool is_null =
                  gk_view.all_null ||
                  (gk_view.nulls != nullptr && gk_view.nulls[ri] != 0);
              const int64_t gk = is_null
                                     ? 0
                                     : (gk_view.is_const ? gk_view.const_val
                                                         : gk_view.values[ri]);
              dst = get_single_int_group(wl, is_null, gk);
            }
            for (size_t ai = 0; ai < n_aggs; ++ai) {
              if (agg_kinds[ai] == SimpleAggKind::kCountStar) {
                (*dst)[ai].count += wt;
              }
            }
            if (!merge_child_owned_aggs(dst, cw.data(), csid.data(), wt)) {
              wl.invalid = true;
              return;
            }
          }
        } else {
          for (size_t ri = 0; ri < chunk.rows; ++ri) {
            __int128 w_total = 1;
            bool matched = true;
            for (size_t ci = 0; ci < n_rc; ++ci) {
              bool found = false;
              summaries[root_children[ci].child_ar].Lookup(
                  &chunk, nullptr, ri, root_children[ci].parent_col,
                  &child_key_tmp, &found, &cw[ci], &csid[ci], &wl.invalid);
              if (wl.invalid) {
                return;
              }
              if (!found || cw[ci] == 0) {
                matched = false;
                break;
              }
              w_total *= static_cast<__int128>(cw[ci]);
              if (std::cmp_greater(w_total,
                                   std::numeric_limits<uint64_t>::max())) {
                wl.invalid = true;
                return;
              }
            }
            if (!matched) {
              continue;
            }
            const auto wt = static_cast<uint64_t>(w_total);
            wl.comparisons += 1;
            std::vector<AggSlotState>* dst = nullptr;
            if (n_gkeys == 0) {
              wl.has_scalar = true;
              dst = &wl.scalar_aggs;
            } else {
              int64_t gk = 0;
              const bool non_null =
                  ReadIntKeyFromEntry(&chunk, nullptr, ri, root_group_cols[0],
                                      gk0_type, &gk, &wl.invalid);
              if (wl.invalid) {
                return;
              }
              dst = get_single_int_group(wl, !non_null, gk);
            }
            for (size_t ai = 0; ai < n_aggs; ++ai) {
              if (agg_kinds[ai] == SimpleAggKind::kCountStar) {
                (*dst)[ai].count += wt;
              }
            }
            if (!merge_child_owned_aggs(dst, cw.data(), csid.data(), wt)) {
              wl.invalid = true;
              return;
            }
          }
        }
      }
    };
    if (actual_workers <= 1) {
      run_chunk_worker(0);
    } else {
      RETURN_IF_FAIL(RunParallelTasks(actual_workers, run_chunk_worker));
    }
  } else {
    RETURN_IF_FAIL(ParallelForEachRelationEntryRange(
        root_rel, workers, [&](size_t w, auto&& iter_fn) {
          RootWorkerLocal& wl = locals[w];
          std::vector<uint64_t> cw(n_rc);
          std::vector<uint32_t> csid(n_rc);
          std::vector<Value> gk_vals(n_gkeys);
          Value child_key_tmp;
          iter_fn([&](const ColumnarChunk* chunk, const Row* r,
                      size_t ri) -> bool {
            __int128 w_total = 1;
            for (size_t ci = 0; ci < n_rc; ++ci) {
              bool found = false;
              summaries[root_children[ci].child_ar].Lookup(
                  chunk, r, ri, root_children[ci].parent_col, &child_key_tmp,
                  &found, &cw[ci], &csid[ci], &wl.invalid);
              if (wl.invalid) return false;
              if (!found || cw[ci] == 0) return true;
              w_total *= static_cast<__int128>(cw[ci]);
              if (std::cmp_greater(w_total,
                                   std::numeric_limits<uint64_t>::max())) {
                wl.invalid = true;
                return false;
              }
            }
            const auto wt = static_cast<uint64_t>(w_total);
            wl.comparisons += 1;

            std::vector<AggSlotState>* dst = nullptr;
            if (n_gkeys == 0) {
              wl.has_scalar = true;
              dst = &wl.scalar_aggs;
            } else if (single_int_gk) {
              int64_t gk = 0;
              const bool non_null = ReadIntKeyFromEntry(
                  chunk, r, ri, root_group_cols[0], gk0_type, &gk, &wl.invalid);
              if (wl.invalid) {
                return false;
              }
              dst = get_single_int_group(wl, !non_null, gk);
            } else {
              for (size_t k = 0; k < n_gkeys; ++k) {
                ReadColValueOrRow(chunk, r, ri, root_group_cols[k],
                                  &gk_vals[k]);
                if (gk_vals[k].IsCaseInsensitive() || gk_vals[k].IsInterval()) {
                  wl.invalid = true;
                  return false;
                }
              }
              std::string enc = EncodeKeyTuple(gk_vals);
              auto [it, ins] = wl.str_map.emplace(
                  std::move(enc), static_cast<uint32_t>(wl.group_aggs.size()));
              if (ins) {
                std::vector<Value> owned = gk_vals;
                for (Value& v : owned) {
                  if (v.type == ValueType::kVarChar && v.owned_data.empty() &&
                      !v.value.varchar_value.empty()) {
                    v.owned_data.assign(v.value.varchar_value);
                    v.value.varchar_value = v.owned_data;
                  }
                }
                wl.group_keys.push_back(std::move(owned));
                wl.group_aggs.emplace_back(n_aggs);
              }
              dst = &wl.group_aggs[it->second];
            }

            for (size_t ai = 0; ai < n_aggs; ++ai) {
              if (agg_kinds[ai] == SimpleAggKind::kCountStar) {
                (*dst)[ai].count += wt;
              } else if (agg_is_root[ai]) {
                if (!AddEntryCellWithWeight(&(*dst)[ai], agg_kinds[ai], chunk,
                                            r, ri, agg_root_col[ai], wt)) {
                  wl.invalid = true;
                  return false;
                }
              }
            }
            if (!merge_child_owned_aggs(dst, cw.data(), csid.data(), wt)) {
              wl.invalid = true;
              return false;
            }
            return true;
          });
        }));
  }

  for (const auto& wl : locals) {
    if (wl.invalid) {
      return std::optional<Relation>{};
    }
    total_comparisons += wl.comparisons;
  }

  // Build representative rows of `full_active_schema` populating
  // root_group_cols AND any eq-class partner columns (so SELECT/HAVING can read
  // either `f.id` or `m.forum`).
  std::vector<Row> final_rep_rows;
  std::vector<std::vector<AggSlotState>> final_group_aggs;
  const auto populate_group_rep_row = [&](Row* rep,
                                          const std::vector<Value>& vals) {
    for (size_t k = 0; k < n_gkeys; ++k) {
      rep->values_[ar_schema_offset[root_ar] + root_group_cols[k]] = vals[k];
      rep->values_[ar_schema_offset[gk_refs[k].orig_ar] + gk_refs[k].orig_col] =
          vals[k];
      if (gk_refs[k].eq_cls.has_value()) {
        for (const auto& [m_ar, m_col] :
             eq_classes[*gk_refs[k].eq_cls].members) {
          rep->values_[ar_schema_offset[m_ar] + m_col] = vals[k];
        }
      }
    }
    EnsureOwnedStringValues(rep);
  };

  if (n_gkeys == 0) {
    bool any = false;
    std::vector<AggSlotState> merged(n_aggs);
    for (const auto& wl : locals) {
      if (!wl.has_scalar) {
        continue;
      }
      any = true;
      for (size_t ai = 0; ai < n_aggs; ++ai) {
        if (!MergeAggSlot(&merged[ai], wl.scalar_aggs[ai], agg_kinds[ai], 1)) {
          return std::optional<Relation>{};
        }
      }
    }
    if (any) {
      final_rep_rows.emplace_back(
          std::vector<Value>(full_active_schema.ColumnCount()));
      final_group_aggs.push_back(std::move(merged));
    }
  } else if (single_int_gk) {
    FlatIntKeyMap<size_t> global_int_groups;
    std::optional<size_t> global_null_idx;
    for (const auto& wl : locals) {
      for (size_t gi = 0; gi < wl.int_group_keys.size(); ++gi) {
        size_t dst_idx = 0;
        bool is_new = false;
        if (wl.int_group_is_null[gi] != 0U) {
          if (!global_null_idx.has_value()) {
            global_null_idx = final_rep_rows.size();
            dst_idx = *global_null_idx;
            is_new = true;
          } else {
            dst_idx = *global_null_idx;
          }
        } else {
          const int64_t kv = wl.int_group_keys[gi];
          if (const size_t* found = global_int_groups.Find(kv)) {
            dst_idx = *found;
          } else {
            dst_idx = final_rep_rows.size();
            *global_int_groups.TryEmplace(kv) = dst_idx;
            is_new = true;
          }
        }
        if (is_new) {
          Row rep(std::vector<Value>(full_active_schema.ColumnCount()));
          const Value v =
              wl.int_group_is_null[gi] != 0U
                  ? Value()
                  : (gk0_type == ValueType::kDate
                         ? Value::DateFromDays(wl.int_group_keys[gi])
                         : Value(wl.int_group_keys[gi]));
          populate_group_rep_row(&rep, {v});
          final_rep_rows.push_back(std::move(rep));
          final_group_aggs.push_back(wl.group_aggs[gi]);
        } else {
          for (size_t ai = 0; ai < n_aggs; ++ai) {
            if (!MergeAggSlot(&final_group_aggs[dst_idx][ai],
                              wl.group_aggs[gi][ai], agg_kinds[ai], 1)) {
              return std::optional<Relation>{};
            }
          }
        }
      }
    }
  } else {
    std::unordered_map<std::string, size_t> global_groups;
    for (const auto& wl : locals) {
      for (size_t gi = 0; gi < wl.group_keys.size(); ++gi) {
        std::string enc = EncodeKeyTuple(wl.group_keys[gi]);
        auto [it, ins] =
            global_groups.emplace(std::move(enc), final_rep_rows.size());
        if (ins) {
          Row rep(std::vector<Value>(full_active_schema.ColumnCount()));
          populate_group_rep_row(&rep, wl.group_keys[gi]);
          final_rep_rows.push_back(std::move(rep));
          final_group_aggs.push_back(wl.group_aggs[gi]);
        } else {
          const size_t dst_idx = it->second;
          for (size_t ai = 0; ai < n_aggs; ++ai) {
            if (!MergeAggSlot(&final_group_aggs[dst_idx][ai],
                              wl.group_aggs[gi][ai], agg_kinds[ai], 1)) {
              return std::optional<Relation>{};
            }
          }
        }
      }
    }
  }

  std::optional<Row> empty_rep;
  if (n_gkeys == 0) {
    empty_rep = Row(std::vector<Value>(full_active_schema.ColumnCount()));
  }
  std::vector<const Relation*> keep_rels;
  keep_rels.reserve(n_active);
  for (const size_t idx : active_rels) {
    keep_rels.push_back(&relations[idx]);
  }
  auto result_or = EmitGroupedProjectionRelation(
      context, full_active_schema, final_rep_rows, final_group_aggs, agg_kinds,
      empty_rep, *fusion, n_active - 1, total_comparisons, keep_rels);
  if (!result_or.HasValue()) {
    if (!IsEagerFallbackStatus(result_or.GetStatus())) {
      return result_or.GetStatus();
    }
    return std::optional<Relation>{};
  }
  fusion->fired = true;
  return std::optional<Relation>(result_or.MoveValue());
}

StatusOr<std::optional<Relation>> TrySmallDimStarDirectAggregate(
    TransactionContext& context, const std::vector<Relation>& relations,
    const std::vector<uint8_t>& absorbed,
    const std::vector<PredicateInfo>& predicates, const Scope* outer,
    const CteMap& ctes, GroupedJoinFusionSpec* fusion) {
  static const bool disabled =
      ::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_SMALL_DIM_DENSE_LOOKUP") !=
      nullptr;
  if (disabled || fusion == nullptr || !fusion->eligible || fusion->fired ||
      outer != nullptr || !ctes.empty() || fusion->rollup_count_distribution ||
      fusion->anti_unmatched_only || fusion->aggregates.empty() ||
      fusion->aggregates.size() > kMaxCollapsedAggs) {
    return std::optional<Relation>{};
  }
  std::vector<size_t> active_rels;
  std::vector<int> rel_to_active(relations.size(), -1);
  for (size_t i = 0; i < relations.size(); ++i) {
    if (i < absorbed.size() && absorbed[i] == 0U) {
      if (relations[i].HasSpill() || relations[i].using_columns != nullptr ||
          relations[i].schema.ColumnCount() == 0) {
        return std::optional<Relation>{};
      }
      rel_to_active[i] = static_cast<int>(active_rels.size());
      active_rels.push_back(i);
    }
  }
  const size_t n_active = active_rels.size();
  if (n_active < 2 || n_active > 5) {
    return std::optional<Relation>{};
  }

  // Pick fact candidate as the largest active relation.
  size_t fact_ar = 0;
  for (size_t ar = 1; ar < n_active; ++ar) {
    if (relations[active_rels[ar]].TotalRows() >
        relations[active_rels[fact_ar]].TotalRows()) {
      fact_ar = ar;
    }
  }
  const Relation& fact_rel = relations[active_rels[fact_ar]];
  if (fact_rel.TotalRows() < 4096) {
    return std::optional<Relation>{};
  }

  struct DimFlatIntCol {
    slot_t dim_col{0};
    std::vector<int64_t> vals;
    std::vector<uint8_t> is_null;
  };
  struct LeafFilter {
    size_t leaf_ar{0};
    slot_t dim_fk_col{0};
    slot_t leaf_pk_col{0};
    ValueType key_type{ValueType::kInt64};
    FlatIntKeyMap<uint8_t> pk_set;
  };
  struct DimLink {
    size_t dim_ar{0};
    slot_t fact_col{0};
    slot_t dim_pk_col{0};
    ValueType key_type{ValueType::kInt64};
    int64_t min_pk{0};
    uint64_t span{0};
    size_t matched_rows{0};
    bool existence_only{false};
    bool flat_int_aggs_only{false};
    std::vector<uint64_t> dense_exists_bits;
    std::vector<Row> dim_rows;
    std::vector<uint32_t> dense_row_idx;
    std::vector<DimFlatIntCol> flat_int_cols;
    std::vector<LeafFilter> leaf_filters;
  };
  struct RawEdge {
    size_t a0{0};
    slot_t c0{0};
    size_t a1{0};
    slot_t c1{0};
    ValueType key_type{ValueType::kInt64};
  };
  std::vector<RawEdge> raw_edges;
  for (const PredicateInfo& p : predicates) {
    if (!p.resolved || p.sources.size() < 2) {
      continue;
    }
    if (p.contains_query || p.sources.size() != 2 ||
        !IsColumnEqualityPredicate(p.expression) ||
        p.expression->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
      return std::optional<Relation>{};
    }
    auto sit = p.sources.begin();
    const size_t s0 = *sit++;
    const size_t s1 = *sit;
    if (s0 >= relations.size() || s1 >= relations.size() ||
        rel_to_active[s0] < 0 || rel_to_active[s1] < 0) {
      return std::optional<Relation>{};
    }
    const auto a0 = static_cast<size_t>(rel_to_active[s0]);
    const auto a1 = static_cast<size_t>(rel_to_active[s1]);
    const size_t r0_idx = active_rels[a0];
    const size_t r1_idx = active_rels[a1];

    const BinaryExpression& bin = p.expression->AsBinaryExpression();
    const ColumnName& lhs = bin.Left()->AsColumnValue().GetColumnName();
    const ColumnName& rhs = bin.Right()->AsColumnValue().GetColumnName();
    const auto l0 = LocalColumnOffset(relations[r0_idx].schema, lhs);
    const auto r0 = LocalColumnOffset(relations[r0_idx].schema, rhs);
    const auto l1 = LocalColumnOffset(relations[r1_idx].schema, lhs);
    const auto r1 = LocalColumnOffset(relations[r1_idx].schema, rhs);
    std::optional<slot_t> c0, c1;
    if (l0.has_value() && r1.has_value() && !r0.has_value() &&
        !l1.has_value()) {
      c0 = static_cast<slot_t>(*l0);
      c1 = static_cast<slot_t>(*r1);
    } else if (r0.has_value() && l1.has_value() && !l0.has_value() &&
               !r1.has_value()) {
      c0 = static_cast<slot_t>(*r0);
      c1 = static_cast<slot_t>(*l1);
    } else {
      return std::optional<Relation>{};
    }
    const Column& col0 = relations[r0_idx].schema.GetColumn(*c0);
    const Column& col1 = relations[r1_idx].schema.GetColumn(*c1);
    if (col0.Type() != col1.Type() || col0.IsUnsigned() || col1.IsUnsigned() ||
        (col0.Type() != ValueType::kInt64 && col0.Type() != ValueType::kDate)) {
      return std::optional<Relation>{};
    }
    raw_edges.push_back(
        {.a0 = a0, .c0 = *c0, .a1 = a1, .c1 = *c1, .key_type = col0.Type()});
  }

  std::vector<DimLink> dims;
  std::unordered_map<size_t, size_t> ar_to_dim_idx;
  for (const RawEdge& e : raw_edges) {
    if (e.a0 != fact_ar && e.a1 != fact_ar) {
      continue;
    }
    const size_t d_ar = (e.a0 == fact_ar) ? e.a1 : e.a0;
    const slot_t fc = (e.a0 == fact_ar) ? e.c0 : e.c1;
    const slot_t dc = (e.a0 == fact_ar) ? e.c1 : e.c0;
    auto it = ar_to_dim_idx.find(d_ar);
    if (it != ar_to_dim_idx.end()) {
      const DimLink& dlk = dims[it->second];
      if (dlk.fact_col == fc && dlk.dim_pk_col == dc) {
        continue;
      }
      return std::optional<Relation>{};  // multiple join edges to same dim
    }
    ar_to_dim_idx[d_ar] = dims.size();
    DimLink dlk;
    dlk.dim_ar = d_ar;
    dlk.fact_col = fc;
    dlk.dim_pk_col = dc;
    dlk.key_type = e.key_type;
    dims.push_back(std::move(dlk));
  }
  if (dims.empty()) {
    return std::optional<Relation>{};
  }

  // Attach 1-hop snowflake leaf dims (e.g. kind_type joined to title in JOB J3)
  // as exact PK-unique semijoin filters on their parent direct dim.
  std::unordered_set<size_t> leaf_ars;
  for (const RawEdge& e : raw_edges) {
    if (e.a0 == fact_ar || e.a1 == fact_ar) {
      continue;
    }
    const bool a0_is_dim = ar_to_dim_idx.contains(e.a0);
    const bool a1_is_dim = ar_to_dim_idx.contains(e.a1);
    if (a0_is_dim == a1_is_dim) {
      return std::optional<Relation>{};  // dim-dim edge or 2-hop leaf chain
    }
    const size_t d_ar = a0_is_dim ? e.a0 : e.a1;
    const size_t l_ar = a0_is_dim ? e.a1 : e.a0;
    const slot_t d_fk_col = a0_is_dim ? e.c0 : e.c1;
    const slot_t l_pk_col = a0_is_dim ? e.c1 : e.c0;
    if (!leaf_ars.insert(l_ar).second) {
      return std::optional<Relation>{};  // leaf has multiple edges
    }
    LeafFilter lf;
    lf.leaf_ar = l_ar;
    lf.dim_fk_col = d_fk_col;
    lf.leaf_pk_col = l_pk_col;
    lf.key_type = e.key_type;
    dims[ar_to_dim_idx[d_ar]].leaf_filters.push_back(std::move(lf));
  }
  for (size_t ar = 0; ar < n_active; ++ar) {
    if (ar != fact_ar && !ar_to_dim_idx.contains(ar) &&
        !leaf_ars.contains(ar)) {
      return std::optional<Relation>{};  // disconnected relation
    }
  }

  Schema full_active_schema;
  std::vector<size_t> ar_schema_offset(n_active, 0);
  for (size_t ar = 0; ar < n_active; ++ar) {
    ar_schema_offset[ar] = full_active_schema.ColumnCount();
    full_active_schema = full_active_schema + relations[active_rels[ar]].schema;
  }
  auto resolve_to_ar_col =
      [&](const ColumnName& cn) -> std::optional<std::pair<size_t, slot_t>> {
    const auto off = LocalColumnOffset(full_active_schema, cn);
    if (!off.has_value()) {
      return std::nullopt;
    }
    for (size_t ar = n_active; ar-- > 0;) {
      if (static_cast<size_t>(*off) >= ar_schema_offset[ar]) {
        return std::make_pair(ar,
                              static_cast<slot_t>(static_cast<size_t>(*off) -
                                                  ar_schema_offset[ar]));
      }
    }
    return std::nullopt;
  };

  struct ColSource {
    size_t ar{0};
    slot_t col{0};
  };
  std::vector<ColSource> gk_sources(fusion->group_keys.size());
  for (size_t k = 0; k < fusion->group_keys.size(); ++k) {
    if (!fusion->group_keys[k] ||
        fusion->group_keys[k]->Type() != TypeTag::kColumnValue) {
      return std::optional<Relation>{};
    }
    auto res = resolve_to_ar_col(
        fusion->group_keys[k]->AsColumnValue().GetColumnName());
    if (!res.has_value()) {
      return std::optional<Relation>{};
    }
    gk_sources[k] = {.ar = res->first, .col = res->second};
  }

  std::vector<SimpleAggKind> agg_kinds(fusion->aggregates.size());
  std::vector<ColSource> agg_sources(fusion->aggregates.size());
  for (size_t ai = 0; ai < fusion->aggregates.size(); ++ai) {
    auto kind =
        ClassifySimpleAgg(fusion->aggregates[ai], fusion->count_star[ai]);
    if (!kind.has_value()) {
      return std::optional<Relation>{};
    }
    agg_kinds[ai] = *kind;
    if (!fusion->count_star[ai]) {
      const Expression& ch = fusion->aggregates[ai]->Child();
      if (ch->Type() != TypeTag::kColumnValue) {
        return std::optional<Relation>{};
      }
      auto res = resolve_to_ar_col(ch->AsColumnValue().GetColumnName());
      if (!res.has_value()) {
        return std::optional<Relation>{};
      }
      agg_sources[ai] = {.ar = res->first, .col = res->second};
    }
  }

  const auto allowed_fn = [&](const ColumnName& cn) -> bool {
    auto res = resolve_to_ar_col(cn);
    if (!res.has_value()) {
      return false;
    }
    return std::ranges::any_of(gk_sources, [&](const ColSource& gs) {
      return gs.ar == res->first && gs.col == res->second;
    });
  };
  if (fusion->having && !AllNonAggColumnsAllowed(fusion->having, allowed_fn)) {
    return std::optional<Relation>{};
  }
  for (const Expression& se : fusion->select_items) {
    if (!AllNonAggColumnsAllowed(se, allowed_fn)) {
      return std::optional<Relation>{};
    }
  }

  const size_t n_aggs = agg_kinds.size();
  const size_t n_gkeys = gk_sources.size();
  const size_t n_dims = dims.size();
  constexpr uint64_t kMaxStarDenseKeySpan = 1ULL << 20U;

  // Leaf snowflake dims must be existence-only (no columns referenced by
  // GROUP BY or aggregates; SELECT/HAVING are already checked against
  // gk_sources by AllNonAggColumnsAllowed).
  if (!leaf_ars.empty()) {
    for (const ColSource& gs : gk_sources) {
      if (leaf_ars.contains(gs.ar)) {
        return std::optional<Relation>{};
      }
    }
    for (size_t ai = 0; ai < n_aggs; ++ai) {
      if (!fusion->count_star[ai] && leaf_ars.contains(agg_sources[ai].ar)) {
        return std::optional<Relation>{};
      }
    }
    for (DimLink& dlk : dims) {
      for (LeafFilter& lf : dlk.leaf_filters) {
        const Relation& lrel = relations[active_rels[lf.leaf_ar]];
        bool bad = false;
        bool dup = false;
        size_t non_null_pks = 0;
        ForEachRelationEntry(
            lrel, [&](const ColumnarChunk* chunk, const Row* r, size_t ri) {
              int64_t kv = 0;
              if (!ReadIntKeyFromEntry(chunk, r, ri, lf.leaf_pk_col,
                                       lf.key_type, &kv, &bad)) {
                return !bad;
              }
              if (lf.pk_set.Find(kv) != nullptr) {
                dup = true;
                return false;
              }
              *lf.pk_set.TryEmplace(kv) = 1U;
              ++non_null_pks;
              return true;
            });
        if (bad || dup || non_null_pks == 0) {
          return std::optional<Relation>{};
        }
      }
    }
  }

  // Lean proofs:
  // - lean/TinylambOpt/ResearchProofs/Wave8Job.lean (FixedGender_sound):
  //   discharge a sole constant-value dim GROUP BY key to a 1-group scalar
  //   fold.
  // - lean/TinylambOpt/ResearchProofs/RestartRaces/Goal_JobJ3_PersonPages.lean:
  //   store existence-only dimensions as 1-bit dense bitsets (32x smaller, L1).
  std::optional<Value> const_group_key;
  std::vector<int> agg_flat_col_idx(n_aggs, -1);

  for (size_t di = 0; di < n_dims; ++di) {
    DimLink& dlk = dims[di];
    const Relation& drel = relations[active_rels[dlk.dim_ar]];
    const bool is_const_gk_candidate =
        n_gkeys == 1 && gk_sources[0].ar == dlk.dim_ar;
    auto passes_leaf_filters = [&](const ColumnarChunk* chunk, const Row* r,
                                   size_t ri, bool* out_bad) -> bool {
      for (const LeafFilter& lf : dlk.leaf_filters) {
        int64_t fk = 0;
        if (!ReadIntKeyFromEntry(chunk, r, ri, lf.dim_fk_col, lf.key_type, &fk,
                                 out_bad)) {
          return false;
        }
        if (lf.pk_set.Find(fk) == nullptr) {
          return false;
        }
      }
      return true;
    };
    int64_t bmin = std::numeric_limits<int64_t>::max();
    int64_t bmax = std::numeric_limits<int64_t>::min();
    bool bad = false;
    bool saw_gk_row = false;
    bool all_same_gk = is_const_gk_candidate;
    Value first_gk_val;
    Value scratch_gk_val;
    ForEachRelationEntry(drel, [&](const ColumnarChunk* chunk, const Row* r,
                                   size_t ri) {
      if (!dlk.leaf_filters.empty() &&
          !passes_leaf_filters(chunk, r, ri, &bad)) {
        return !bad;
      }
      int64_t kv = 0;
      if (ReadIntKeyFromEntry(chunk, r, ri, dlk.dim_pk_col, dlk.key_type, &kv,
                              &bad)) {
        bmin = std::min(bmin, kv);
        bmax = std::max(bmax, kv);
      }
      if (bad) {
        return false;
      }
      if (all_same_gk) {
        ReadColValueOrRow(chunk, r, ri, gk_sources[0].col, &scratch_gk_val);
        if (scratch_gk_val.IsNull() || scratch_gk_val.IsCaseInsensitive() ||
            scratch_gk_val.IsInterval() ||
            (saw_gk_row && !(scratch_gk_val == first_gk_val))) {
          all_same_gk = false;
        } else if (!saw_gk_row) {
          first_gk_val = scratch_gk_val;
          if (first_gk_val.type == ValueType::kVarChar &&
              first_gk_val.owned_data.empty() &&
              !first_gk_val.value.varchar_value.empty()) {
            first_gk_val.owned_data.assign(first_gk_val.value.varchar_value);
            first_gk_val.value.varchar_value = first_gk_val.owned_data;
          }
          saw_gk_row = true;
        }
      }
      return true;
    });
    if (bad || bmin > bmax) {
      return std::optional<Relation>{};
    }
    const uint64_t span =
        static_cast<uint64_t>(bmax) - static_cast<uint64_t>(bmin) + 1ULL;
    if (span == 0 || span > kMaxStarDenseKeySpan) {
      return std::optional<Relation>{};
    }
    dlk.min_pk = bmin;
    dlk.span = span;
    if (is_const_gk_candidate && saw_gk_row && all_same_gk) {
      const_group_key = std::move(first_gk_val);
    }

    bool has_live_gk = false;
    for (size_t k = 0; k < n_gkeys; ++k) {
      if (gk_sources[k].ar == dlk.dim_ar &&
          (n_gkeys != 1 || !const_group_key.has_value())) {
        has_live_gk = true;
        break;
      }
    }
    bool has_any_agg = false;
    bool all_aggs_flat_int = true;
    std::vector<size_t> dim_agg_indices;
    for (size_t ai = 0; ai < n_aggs; ++ai) {
      if (!fusion->count_star[ai] && agg_sources[ai].ar == dlk.dim_ar) {
        has_any_agg = true;
        dim_agg_indices.push_back(ai);
        const Column& col = drel.schema.GetColumn(agg_sources[ai].col);
        if ((agg_kinds[ai] != SimpleAggKind::kSum &&
             agg_kinds[ai] != SimpleAggKind::kAvg &&
             agg_kinds[ai] != SimpleAggKind::kCount) ||
            col.Type() != ValueType::kInt64 || col.IsUnsigned() ||
            col.IsInterval()) {
          all_aggs_flat_int = false;
        }
      }
    }

    if (!has_live_gk && !has_any_agg) {
      dlk.existence_only = true;
      dlk.dense_exists_bits.assign(static_cast<size_t>((span + 63ULL) / 64ULL),
                                   0ULL);
      bool dup = false;
      ForEachRelationEntry(
          drel, [&](const ColumnarChunk* chunk, const Row* r, size_t ri) {
            if (!dlk.leaf_filters.empty() &&
                !passes_leaf_filters(chunk, r, ri, &bad)) {
              return !bad;
            }
            int64_t kv = 0;
            if (!ReadIntKeyFromEntry(chunk, r, ri, dlk.dim_pk_col, dlk.key_type,
                                     &kv, &bad)) {
              return !bad;
            }
            const auto off = static_cast<size_t>(
                static_cast<uint64_t>(kv) - static_cast<uint64_t>(dlk.min_pk));
            const uint64_t mask = 1ULL << (off & 63U);
            if ((dlk.dense_exists_bits[off >> 6U] & mask) != 0ULL) {
              dup = true;
              return false;
            }
            dlk.dense_exists_bits[off >> 6U] |= mask;
            ++dlk.matched_rows;
            return true;
          });
      if (bad || dup) {
        return std::optional<Relation>{};
      }
    } else if (!has_live_gk && has_any_agg && all_aggs_flat_int) {
      dlk.flat_int_aggs_only = true;
      dlk.dense_row_idx.assign(static_cast<size_t>(span), kAbsentSlot);
      for (const size_t ai : dim_agg_indices) {
        int existing = -1;
        for (size_t fi = 0; fi < dlk.flat_int_cols.size(); ++fi) {
          if (dlk.flat_int_cols[fi].dim_col == agg_sources[ai].col) {
            existing = static_cast<int>(fi);
            break;
          }
        }
        if (existing < 0) {
          existing = static_cast<int>(dlk.flat_int_cols.size());
          DimFlatIntCol fc;
          fc.dim_col = agg_sources[ai].col;
          fc.vals.reserve(drel.TotalRows());
          fc.is_null.reserve(drel.TotalRows());
          dlk.flat_int_cols.push_back(std::move(fc));
        }
        agg_flat_col_idx[ai] = existing;
      }
      bool dup = false;
      ForEachRelationEntry(
          drel, [&](const ColumnarChunk* chunk, const Row* r, size_t ri) {
            if (!dlk.leaf_filters.empty() &&
                !passes_leaf_filters(chunk, r, ri, &bad)) {
              return !bad;
            }
            int64_t kv = 0;
            if (!ReadIntKeyFromEntry(chunk, r, ri, dlk.dim_pk_col, dlk.key_type,
                                     &kv, &bad)) {
              return !bad;
            }
            const auto off = static_cast<size_t>(
                static_cast<uint64_t>(kv) - static_cast<uint64_t>(dlk.min_pk));
            if (dlk.dense_row_idx[off] != kAbsentSlot) {
              dup = true;
              return false;
            }
            const auto ridx = static_cast<uint32_t>(dlk.matched_rows++);
            dlk.dense_row_idx[off] = ridx;
            for (DimFlatIntCol& fc : dlk.flat_int_cols) {
              int64_t av = 0;
              const bool non_null = ReadIntKeyFromEntry(
                  chunk, r, ri, fc.dim_col, ValueType::kInt64, &av, &bad);
              if (bad) {
                return false;
              }
              fc.vals.push_back(non_null ? av : 0);
              fc.is_null.push_back(non_null ? 0U : 1U);
            }
            return true;
          });
      if (bad || dup) {
        return std::optional<Relation>{};
      }
    } else {
      dlk.dense_row_idx.assign(static_cast<size_t>(span), kAbsentSlot);
      bool dup = false;
      ForEachRelationEntry(drel, [&](const ColumnarChunk* chunk, const Row* r,
                                     size_t ri) {
        if (!dlk.leaf_filters.empty() &&
            !passes_leaf_filters(chunk, r, ri, &bad)) {
          return !bad;
        }
        Row row_copy;
        if (r != nullptr) {
          row_copy = *r;
        } else {
          row_copy.values_.resize(chunk->columns.size());
          for (size_t c = 0; c < chunk->columns.size(); ++c) {
            chunk->columns[c].WriteValueInto(ri, &row_copy.values_[c]);
          }
        }
        EnsureOwnedStringValues(&row_copy);
        int64_t kv = 0;
        if (ReadIntKeyCell(row_copy[dlk.dim_pk_col], dlk.key_type, &kv, &bad)) {
          const auto off = static_cast<size_t>(
              static_cast<uint64_t>(kv) - static_cast<uint64_t>(dlk.min_pk));
          if (dlk.dense_row_idx[off] != kAbsentSlot) {
            dup = true;
            return false;
          }
          dlk.dense_row_idx[off] = static_cast<uint32_t>(dlk.dim_rows.size());
          ++dlk.matched_rows;
        }
        dlk.dim_rows.push_back(std::move(row_copy));
        return !bad;
      });
      if (bad || dup) {
        return std::optional<Relation>{};
      }
    }
  }

  // Static selectivity + L1-resident existence-only dimension probe ordering
  // heuristic (note: Goal_JobJ3_ProbeSchedule.lean refuted greedy per-morsel
  // cache-oblivious scheduling, so we use static whole-dim selectivity rather
  // than per-morsel greedy switching).
  std::vector<size_t> probe_order(n_dims);
  for (size_t di = 0; di < n_dims; ++di) {
    probe_order[di] = di;
  }
  std::ranges::sort(probe_order, [&](size_t a, size_t b) {
    const double sel_a =
        static_cast<double>(dims[a].matched_rows) /
        static_cast<double>(std::max<uint64_t>(1, dims[a].span));
    const double sel_b =
        static_cast<double>(dims[b].matched_rows) /
        static_cast<double>(std::max<uint64_t>(1, dims[b].span));
    const double score_a = sel_a * (dims[a].existence_only ? 0.5 : 1.0);
    const double score_b = sel_b * (dims[b].existence_only ? 0.5 : 1.0);
    return score_a < score_b;
  });

  // Parallel scan of `fact_rel` with O(1) dense dim lookups.
  const size_t total_fact = fact_rel.TotalRows();
  const size_t workers =
      ParallelWorkerCount(std::max<size_t>(1, total_fact / 16384));

  struct WorkerLocal {
    bool has_scalar{false};
    std::vector<AggSlotState> scalar_aggs;
    std::unordered_map<std::string, uint32_t> str_map;
    std::vector<std::vector<Value>> group_keys;
    std::vector<std::vector<AggSlotState>> group_aggs;
    uint64_t comparisons{0};
    bool invalid{false};
  };
  std::vector<WorkerLocal> locals(workers);
  for (auto& wl : locals) {
    if (n_gkeys == 0 || const_group_key.has_value()) {
      wl.scalar_aggs.resize(n_aggs);
    }
  }

  RETURN_IF_FAIL(ParallelForEachRelationEntryRange(
      fact_rel, workers, [&](size_t w, auto&& iter_fn) {
        WorkerLocal& wl = locals[w];
        std::vector<uint32_t> dim_ridx(n_dims);
        std::vector<Value> gk_vals(n_gkeys);
        iter_fn([&](const ColumnarChunk* chunk, const Row* fr,
                    size_t ri) -> bool {
          for (const size_t di : probe_order) {
            const DimLink& dlk = dims[di];
            int64_t fk = 0;
            if (!ReadIntKeyFromEntry(chunk, fr, ri, dlk.fact_col, dlk.key_type,
                                     &fk, &wl.invalid)) {
              return !wl.invalid;
            }
            const uint64_t off =
                static_cast<uint64_t>(fk) - static_cast<uint64_t>(dlk.min_pk);
            if (off >= dlk.span) return true;
            if (dlk.existence_only) {
              if (((dlk.dense_exists_bits[off >> 6U] >> (off & 63U)) & 1ULL) ==
                  0ULL) {
                return true;
              }
            } else {
              const uint32_t ridx = dlk.dense_row_idx[off];
              if (ridx == kAbsentSlot) return true;
              dim_ridx[di] = ridx;
            }
          }
          wl.comparisons += 1;

          auto read_cell_into = [&](const ColSource& cs, Value* out) {
            if (cs.ar == fact_ar) {
              ReadColValueOrRow(chunk, fr, ri, cs.col, out);
              return;
            }
            const size_t di = ar_to_dim_idx[cs.ar];
            *out = dims[di].dim_rows[dim_ridx[di]][cs.col];
          };

          std::vector<AggSlotState>* dst = nullptr;
          if (n_gkeys == 0 || const_group_key.has_value()) {
            wl.has_scalar = true;
            dst = &wl.scalar_aggs;
          } else {
            for (size_t k = 0; k < n_gkeys; ++k) {
              read_cell_into(gk_sources[k], &gk_vals[k]);
              if (gk_vals[k].IsCaseInsensitive() || gk_vals[k].IsInterval()) {
                wl.invalid = true;
                return false;
              }
            }
            std::string enc = EncodeKeyTuple(gk_vals);
            auto [it, ins] = wl.str_map.emplace(
                std::move(enc), static_cast<uint32_t>(wl.group_aggs.size()));
            if (ins) {
              std::vector<Value> owned = gk_vals;
              for (Value& v : owned) {
                if (v.type == ValueType::kVarChar && v.owned_data.empty() &&
                    !v.value.varchar_value.empty()) {
                  v.owned_data.assign(v.value.varchar_value);
                  v.value.varchar_value = v.owned_data;
                }
              }
              wl.group_keys.push_back(std::move(owned));
              wl.group_aggs.emplace_back(n_aggs);
            }
            dst = &wl.group_aggs[it->second];
          }

          Value agg_tmp;
          for (size_t ai = 0; ai < n_aggs; ++ai) {
            if (fusion->count_star[ai]) {
              ++(*dst)[ai].count;
            } else if (agg_sources[ai].ar == fact_ar) {
              if (!AddEntryCellWithWeight(&(*dst)[ai], agg_kinds[ai], chunk, fr,
                                          ri, agg_sources[ai].col, 1)) {
                wl.invalid = true;
                return false;
              }
            } else if (agg_flat_col_idx[ai] >= 0) {
              const size_t di = ar_to_dim_idx[agg_sources[ai].ar];
              const DimFlatIntCol& fc =
                  dims[di]
                      .flat_int_cols[static_cast<size_t>(agg_flat_col_idx[ai])];
              const uint32_t ridx = dim_ridx[di];
              if (fc.is_null[ridx] == 0U) {
                AggSlotState& st = (*dst)[ai];
                if (agg_kinds[ai] == SimpleAggKind::kCount) {
                  ++st.count;
                } else {
                  if (st.numeric_mode == 0) {
                    st.numeric_mode = 1;
                  }
                  const int64_t iv = fc.vals[ridx];
                  st.sum_i128 += iv;
                  if (iv > 0) {
                    st.pos_i128 += iv;
                  } else if (iv < 0) {
                    st.neg_i128 += iv;
                  }
                  ++st.count;
                }
              }
            } else {
              read_cell_into(agg_sources[ai], &agg_tmp);
              if (!AddValueWithWeight(&(*dst)[ai], agg_kinds[ai], agg_tmp, 1)) {
                wl.invalid = true;
                return false;
              }
            }
          }
          return true;
        });
      }));

  uint64_t total_comparisons = 0;
  for (const auto& wl : locals) {
    if (wl.invalid) {
      return std::optional<Relation>{};
    }
    total_comparisons += wl.comparisons;
  }

  std::vector<Row> final_rep_rows;
  std::vector<std::vector<AggSlotState>> final_group_aggs;
  if (n_gkeys == 0 || const_group_key.has_value()) {
    bool any = false;
    std::vector<AggSlotState> merged(n_aggs);
    for (const auto& wl : locals) {
      if (!wl.has_scalar) {
        continue;
      }
      any = true;
      for (size_t ai = 0; ai < n_aggs; ++ai) {
        if (!MergeAggSlot(&merged[ai], wl.scalar_aggs[ai], agg_kinds[ai], 1)) {
          return std::optional<Relation>{};
        }
      }
    }
    if (any) {
      Row rep(std::vector<Value>(full_active_schema.ColumnCount()));
      if (const_group_key.has_value()) {
        rep.values_[ar_schema_offset[gk_sources[0].ar] + gk_sources[0].col] =
            *const_group_key;
        EnsureOwnedStringValues(&rep);
      }
      final_rep_rows.push_back(std::move(rep));
      final_group_aggs.push_back(std::move(merged));
    }
  } else {
    std::unordered_map<std::string, size_t> global_groups;
    for (const auto& wl : locals) {
      for (size_t gi = 0; gi < wl.group_keys.size(); ++gi) {
        std::string enc = EncodeKeyTuple(wl.group_keys[gi]);
        auto [it, ins] =
            global_groups.emplace(std::move(enc), final_rep_rows.size());
        if (ins) {
          Row rep(std::vector<Value>(full_active_schema.ColumnCount()));
          for (size_t k = 0; k < n_gkeys; ++k) {
            rep.values_[ar_schema_offset[gk_sources[k].ar] +
                        gk_sources[k].col] = wl.group_keys[gi][k];
          }
          EnsureOwnedStringValues(&rep);
          final_rep_rows.push_back(std::move(rep));
          final_group_aggs.push_back(wl.group_aggs[gi]);
        } else {
          const size_t dst_idx = it->second;
          for (size_t ai = 0; ai < n_aggs; ++ai) {
            if (!MergeAggSlot(&final_group_aggs[dst_idx][ai],
                              wl.group_aggs[gi][ai], agg_kinds[ai], 1)) {
              return std::optional<Relation>{};
            }
          }
        }
      }
    }
  }

  std::optional<Row> empty_rep;
  if (n_gkeys == 0) {
    empty_rep = Row(std::vector<Value>(full_active_schema.ColumnCount()));
  }
  std::vector<const Relation*> keep_rels;
  keep_rels.reserve(n_active);
  for (const size_t idx : active_rels) {
    keep_rels.push_back(&relations[idx]);
  }
  auto result_or = EmitGroupedProjectionRelation(
      context, full_active_schema, final_rep_rows, final_group_aggs, agg_kinds,
      empty_rep, *fusion, n_dims, total_comparisons, keep_rels);
  if (!result_or.HasValue()) {
    if (!IsEagerFallbackStatus(result_or.GetStatus())) {
      return result_or.GetStatus();
    }
    return std::optional<Relation>{};
  }
  fusion->fired = true;
  return std::optional<Relation>(result_or.MoveValue());
}

}  // namespace tinylamb::relational_detail
