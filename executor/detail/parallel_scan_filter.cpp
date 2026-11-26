/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <thread>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/exception_status.hpp"
#include "common/parallel_work.hpp"
#include "common/platform/process.hpp"
#include "common/query_parallelism.hpp"
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "expression/binary_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "table/iterator.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/table_statistics.hpp"
#include "type/date.hpp"
#include "type/schema.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

namespace {

bool BytecodeColumnTruthy(const ColumnarColumn& col, size_t r) {
  if (!col.is_uniform) {
    return col.ValueAt(r).Truthy();
  }
  if (col.IsNull(r)) {
    return false;
  }
  switch (col.type) {
    case ValueType::kInt64:
      return col.IntAt(r) != 0;
    case ValueType::kDouble:
      return col.DoubleAt(r) != 0.0;
    default:
      // Value::Truthy treats every other non-null type as true.
      return true;
  }
}

template <typename PredFn>
void NarrowSelection(size_t nrows, std::vector<uint32_t>* sel, bool* has_sel,
                     PredFn&& pred) {
  if (!*has_sel) {
    sel->clear();
    for (uint32_t r = 0; r < static_cast<uint32_t>(nrows); ++r) {
      if (pred(r)) {
        sel->push_back(r);
      }
    }
    *has_sel = true;
    return;
  }
  size_t out = 0;
  for (const uint32_t r : *sel) {
    if (pred(r)) {
      (*sel)[out++] = r;
    }
  }
  sel->resize(out);
}

bool ComputeColumnarUniformNonNull(const ColumnarTable& table) {
  if (table.total_rows == 0 || table.chunks.empty()) {
    return false;
  }
  size_t ncols = 0;
  for (const ColumnarChunk& chunk : table.chunks) {
    if (chunk.rows > 0) {
      ncols = chunk.columns.size();
      break;
    }
  }
  if (ncols == 0) {
    return false;
  }
  for (size_t c = 0; c < ncols; ++c) {
    bool saw_non_null = false;
    ValueType vt = ValueType::kNull;
    bool is_unsigned = false;
    bool is_interval = false;
    for (const ColumnarChunk& chunk : table.chunks) {
      if (chunk.rows == 0) {
        continue;
      }
      if (c >= chunk.columns.size()) {
        return false;
      }
      const ColumnarColumn& col = chunk.columns[c];
      if (!col.is_uniform) {
        return false;
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
        return false;
      }
    }
    if (!saw_non_null) {
      return false;
    }
  }
  return true;
}

bool TryFilterColumnarTable(TransactionContext& context,
                            std::shared_ptr<const ColumnarTable> input_table,
                            const CompiledScanFilter& filter,
                            const Schema& match_schema, const Scope* outer,
                            const CteMap& ctes,
                            const ScanRuntimeFilter* runtime_filter,
                            Relation* result, Status* error,
                            bool parallel_disabled) {
  const ColumnarTable& input = *input_table;
  if (filter.needs_unsigned_tagging) {
    return false;
  }
  if (filter.compile_decided_reject) {
    auto empty = std::make_shared<ColumnarTable>();
    empty->keepalives = input.keepalives;
    result->SetColumnarTable(std::move(empty), false);
    return true;
  }
  const bool has_rf = runtime_filter != nullptr && runtime_filter->enabled();
  const bool filter_empty =
      !has_rf && filter.simple.empty() && filter.prefix_in.empty() &&
      filter.prebuilt_membership.empty() &&
      filter.prebuilt_scalar_compare.empty() &&
      filter.disjunctive_branches.empty() && filter.residual.empty();
  if (filter_empty || input.total_rows == 0 || input.chunks.empty()) {
    // Pass-through: the columnar table is immutable, so share the source's
    // storage instead of deep-copying every chunk and recomputing from the
    // duplicate.
    const bool uniform = ComputeColumnarUniformNonNull(input);
    result->SetColumnarTable(std::move(input_table), uniform);
    return true;
  }
  struct FastScaledDoubleCmp {
    bool active{false};
    slot_t lhs_col{0};
    slot_t rhs_col{0};
    double scale{1.0};
    BinaryOperation op{BinaryOperation::kLessThanEquals};
  } fast_scaled_cmp;
  if (filter.residual.size() == 1 && filter.residual[0] != nullptr &&
      filter.residual[0]->Type() == TypeTag::kBinaryExp) {
    const auto& bin = filter.residual[0]->AsBinaryExpression();
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
      auto extract_col = [&](const Expression& raw_e, slot_t* out) -> bool {
        const Expression& e = unwrap_double_cast(raw_e);
        if (!e || e->Type() != TypeTag::kColumnValue) {
          return false;
        }
        const auto off =
            LocalColumnOffset(match_schema, e->AsColumnValue().GetColumnName());
        if (!off.has_value() ||
            static_cast<size_t>(*off) >= match_schema.ColumnCount() ||
            match_schema.GetColumn(static_cast<size_t>(*off)).Type() !=
                ValueType::kDouble) {
          return false;
        }
        *out = *off;
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
      auto extract_scaled = [&](const Expression& raw_e, slot_t* col_out,
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
      slot_t lcol = 0;
      slot_t rcol = 0;
      double sc = 1.0;
      if (extract_col(bin.Left(), &lcol) &&
          extract_scaled(bin.Right(), &rcol, &sc)) {
        fast_scaled_cmp = {.active = true,
                           .lhs_col = lcol,
                           .rhs_col = rcol,
                           .scale = sc,
                           .op = bop};
      } else if (extract_scaled(bin.Left(), &rcol, &sc) &&
                 extract_col(bin.Right(), &lcol)) {
        fast_scaled_cmp = {.active = true,
                           .lhs_col = lcol,
                           .rhs_col = rcol,
                           .scale = sc,
                           .op = flip_op(bop)};
      }
    }
  }

  // Fast path for `col_a op (col_b +/- const_int)` over DATE or signed INT64
  // columns (e.g. TPC-DS Q72 `d3.d_date > d1.d_date + 5`).
  // Lean proof: lean/TinylambOpt/VectorizedJoinAndPruning.lean
  //   (date_offset_cmp_filter_sound)
  struct FastDateOffsetCmp {
    bool active{false};
    bool is_date{false};
    slot_t lhs_col{0};
    slot_t rhs_col{0};
    int64_t delta{0};
    BinaryOperation op{BinaryOperation::kGreaterThan};
  } fast_date_cmp;
  if (!fast_scaled_cmp.active && filter.residual.size() == 1 &&
      filter.residual[0] != nullptr &&
      filter.residual[0]->Type() == TypeTag::kBinaryExp) {
    const auto& bin = filter.residual[0]->AsBinaryExpression();
    const BinaryOperation bop = bin.Op();
    if (bop == BinaryOperation::kLessThan ||
        bop == BinaryOperation::kLessThanEquals ||
        bop == BinaryOperation::kGreaterThan ||
        bop == BinaryOperation::kGreaterThanEquals ||
        bop == BinaryOperation::kEquals || bop == BinaryOperation::kNotEquals) {
      auto extract_id_col = [&](const Expression& e, slot_t* out,
                                ValueType* vt_out) -> bool {
        if (!e || e->Type() != TypeTag::kColumnValue) {
          return false;
        }
        const auto off =
            LocalColumnOffset(match_schema, e->AsColumnValue().GetColumnName());
        if (!off.has_value() ||
            static_cast<size_t>(*off) >= match_schema.ColumnCount()) {
          return false;
        }
        const Column& c = match_schema.GetColumn(static_cast<size_t>(*off));
        if (c.IsUnsigned() ||
            (c.Type() != ValueType::kDate && c.Type() != ValueType::kInt64)) {
          return false;
        }
        *out = *off;
        *vt_out = c.Type();
        return true;
      };
      auto extract_i64_const = [](const Expression& e, int64_t* out) -> bool {
        if (!e || e->Type() != TypeTag::kConstantValue) {
          return false;
        }
        const Value& v = e->AsConstantValue().GetValue();
        if (v.IsNull() || v.type != ValueType::kInt64 || v.IsUnsigned()) {
          return false;
        }
        *out = v.value.int_value;
        return true;
      };
      auto extract_shifted = [&](const Expression& e, slot_t* col_out,
                                 ValueType* vt_out,
                                 int64_t* delta_out) -> bool {
        if (!e || e->Type() != TypeTag::kBinaryExp) {
          return false;
        }
        const auto& ab = e->AsBinaryExpression();
        if (ab.Op() != BinaryOperation::kAdd &&
            ab.Op() != BinaryOperation::kSubtract) {
          return false;
        }
        int64_t k = 0;
        if (extract_id_col(ab.Left(), col_out, vt_out) &&
            extract_i64_const(ab.Right(), &k)) {
          if (ab.Op() == BinaryOperation::kSubtract) {
            if (k == std::numeric_limits<int64_t>::min()) {
              return false;
            }
            k = -k;
          }
          *delta_out = k;
          return true;
        }
        if (ab.Op() == BinaryOperation::kAdd &&
            extract_i64_const(ab.Left(), &k) &&
            extract_id_col(ab.Right(), col_out, vt_out)) {
          *delta_out = k;
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
      slot_t lcol = 0;
      slot_t rcol = 0;
      ValueType lvt = ValueType::kNull;
      ValueType rvt = ValueType::kNull;
      int64_t delta = 0;
      if (extract_id_col(bin.Left(), &lcol, &lvt) &&
          extract_shifted(bin.Right(), &rcol, &rvt, &delta) && lvt == rvt) {
        fast_date_cmp = {.active = true,
                         .is_date = (lvt == ValueType::kDate),
                         .lhs_col = lcol,
                         .rhs_col = rcol,
                         .delta = delta,
                         .op = bop};
      } else if (extract_shifted(bin.Left(), &rcol, &rvt, &delta) &&
                 extract_id_col(bin.Right(), &lcol, &lvt) && lvt == rvt) {
        fast_date_cmp = {.active = true,
                         .is_date = (lvt == ValueType::kDate),
                         .lhs_col = lcol,
                         .rhs_col = rcol,
                         .delta = delta,
                         .op = flip_op(bop)};
      }
    }
  }

  bool fast_residual_covers_all_chunks = false;
  if (fast_scaled_cmp.active) {
    fast_residual_covers_all_chunks = true;
    for (const ColumnarChunk& ch : input.chunks) {
      if (ch.rows == 0) {
        continue;
      }
      if (fast_scaled_cmp.lhs_col >= ch.columns.size() ||
          fast_scaled_cmp.rhs_col >= ch.columns.size()) {
        fast_residual_covers_all_chunks = false;
        break;
      }
      const auto& ca = ch.columns[fast_scaled_cmp.lhs_col];
      const auto& cb = ch.columns[fast_scaled_cmp.rhs_col];
      if (!ca.is_uniform || ca.type != ValueType::kDouble || !cb.is_uniform ||
          cb.type != ValueType::kDouble) {
        fast_residual_covers_all_chunks = false;
        break;
      }
    }
  } else if (fast_date_cmp.active) {
    fast_residual_covers_all_chunks = true;
    const ValueType want_vt =
        fast_date_cmp.is_date ? ValueType::kDate : ValueType::kInt64;
    for (const ColumnarChunk& ch : input.chunks) {
      if (ch.rows == 0) {
        continue;
      }
      if (fast_date_cmp.lhs_col >= ch.columns.size() ||
          fast_date_cmp.rhs_col >= ch.columns.size()) {
        fast_residual_covers_all_chunks = false;
        break;
      }
      const auto& ca = ch.columns[fast_date_cmp.lhs_col];
      const auto& cb = ch.columns[fast_date_cmp.rhs_col];
      if (!ca.is_uniform || ca.is_unsigned || ca.type != want_vt ||
          !cb.is_uniform || cb.is_unsigned || cb.type != want_vt) {
        fast_residual_covers_all_chunks = false;
        break;
      }
    }
  }

  WorkerScanFilterPrograms programs = CompileWorkerFilter(filter, match_schema);
  if (!programs.complete) {
    const bool has_branch_residual =
        std::ranges::any_of(filter.disjunctive_branches,
                            [](const auto& b) { return !b.residual.empty(); });
    if (fast_residual_covers_all_chunks && !has_branch_residual) {
      programs.complete = true;
    } else {
      return false;
    }
  }

  const size_t nchunks = input.chunks.size();
  const size_t max_workers =
      (!parallel_disabled && input.total_rows >= 16384 && nchunks >= 2)
          ? std::min<size_t>(
                {size_t{16},
                 QueryParallelWorkerLimit(std::thread::hardware_concurrency()),
                 nchunks, std::max<size_t>(1, input.total_rows / 4096)})
          : 1;
  const size_t workers = std::max<size_t>(1, max_workers);

  struct DeferredChunkRow {
    size_t preceding_rows;
    Row row;
  };
  struct DictFilterCacheEntry {
    const void* pred_ptr{nullptr};
    const LSTableColumnDictionary* dict{nullptr};
    std::vector<uint8_t> pass;
    bool supported{true};
  };
  // Bound the per-worker cache: chunk-dict pairs only ever add entries, and
  // lookups scan the vector linearly.  Restart the cache past the cap to
  // keep both memory and probe cost bounded.
  constexpr size_t kMaxDictFilterCacheEntries = 32;
  std::vector<ColumnarTable> worker_tables(workers);
  std::vector<std::vector<DeferredChunkRow>> deferred(workers);
  std::vector<std::vector<DictFilterCacheEntry>> worker_dict_caches(workers);
  std::atomic<size_t> key_rejected{0};
  std::atomic<size_t> key_null_rejected{0};

  const auto eval_rf_col = [&](const ColumnarChunk& chunk,
                               const ScanRuntimeFilter& rf, bool count_nulls,
                               std::vector<uint32_t>* sel, bool* has_sel,
                               size_t* rej, size_t* null_rej) {
    if (rf.column >= chunk.columns.size()) {
      return;
    }
    const ColumnarColumn& col = chunk.columns[rf.column];
    const size_t nrows = chunk.rows;
    if (col.is_uniform && col.type == ValueType::kNull) {
      if (rf.Reject(static_cast<int8_t>(ValueType::kNull), true, 0)) {
        const size_t n = *has_sel ? sel->size() : nrows;
        *rej += n;
        if (count_nulls) {
          *null_rej += n;
        }
        sel->clear();
        *has_sel = true;
      }
      return;
    }
    if (col.encoding == ColumnarColumn::Encoding::kConstant && col.is_uniform &&
        (col.type == ValueType::kInt64 || col.type == ValueType::kDate)) {
      if (rf.Reject(static_cast<int8_t>(col.type), col.const_is_null,
                    col.const_is_null ? 0 : col.const_int)) {
        const size_t n = *has_sel ? sel->size() : nrows;
        *rej += n;
        if (count_nulls && col.const_is_null) {
          *null_rej += n;
        }
        sel->clear();
        *has_sel = true;
      }
      return;
    }
    if (col.IsFlat() && col.is_uniform &&
        (col.type == ValueType::kInt64 || col.type == ValueType::kDate)) {
      const auto vt = static_cast<int8_t>(col.type);
      const bool has_null = col.has_null;
      const uint8_t* nulls = col.nulls.data();
      const int64_t* ints = col.ints.data();
      NarrowSelection(nrows, sel, has_sel, [&](uint32_t r) {
        const bool is_null = has_null && nulls[r] != 0;
        if (rf.Reject(vt, is_null, is_null ? 0 : ints[r])) {
          ++(*rej);
          if (count_nulls && is_null) {
            ++(*null_rej);
          }
          return false;
        }
        return true;
      });
      return;
    }
    if (col.is_uniform &&
        (col.type == ValueType::kInt64 || col.type == ValueType::kDate)) {
      const auto vt = static_cast<int8_t>(col.type);
      NarrowSelection(nrows, sel, has_sel, [&](uint32_t r) {
        const bool is_null = col.IsNull(r);
        if (rf.Reject(vt, is_null, is_null ? 0 : col.IntAt(r))) {
          ++(*rej);
          if (count_nulls && is_null) {
            ++(*null_rej);
          }
          return false;
        }
        return true;
      });
      return;
    }
    Value kv;
    NarrowSelection(nrows, sel, has_sel, [&](uint32_t r) {
      col.WriteValueInto(r, &kv);
      if (rf.Reject(static_cast<int8_t>(kv.type), kv.IsNull(),
                    kv.value.int_value)) {
        ++(*rej);
        if (count_nulls && kv.IsNull()) {
          ++(*null_rej);
        }
        return false;
      }
      return true;
    });
  };

  const auto apply_simple_pred = [&](size_t worker_id,
                                     const ColumnarChunk& chunk,
                                     const SimpleComparePredicate& pred,
                                     Row* scratch_row,
                                     std::vector<uint32_t>* sel,
                                     bool* has_sel) {
    const size_t nrows = chunk.rows;
    if (pred.column_payload || pred.column >= chunk.columns.size()) {
      if (pred.column_payload && pred.column < chunk.columns.size() &&
          pred.right_column < chunk.columns.size()) {
        const ColumnarColumn& lc = chunk.columns[pred.column];
        const ColumnarColumn& rc = chunk.columns[pred.right_column];
        if (lc.is_uniform && rc.is_uniform && !lc.is_unsigned &&
            !rc.is_unsigned && lc.type == rc.type && lc.IsFlat() &&
            rc.IsFlat()) {
          if ((lc.type == ValueType::kInt64 || lc.type == ValueType::kDate) &&
              lc.ints.size() == nrows && rc.ints.size() == nrows) {
            const int64_t* __restrict lv = lc.ints.data();
            const int64_t* __restrict rv = rc.ints.data();
            const bool any_null = lc.has_null || rc.has_null;
            const BinaryOperation op = pred.op;
            auto cmp_i = [&](int64_t a, int64_t b) noexcept -> bool {
              switch (op) {
                case BinaryOperation::kEquals:
                  return a == b;
                case BinaryOperation::kNotEquals:
                  return a != b;
                case BinaryOperation::kLessThan:
                  return a < b;
                case BinaryOperation::kLessThanEquals:
                  return a <= b;
                case BinaryOperation::kGreaterThan:
                  return a > b;
                case BinaryOperation::kGreaterThanEquals:
                  return a >= b;
                default:
                  return false;
              }
            };
            if (!any_null) {
              NarrowSelection(nrows, sel, has_sel,
                              [&](uint32_t r) { return cmp_i(lv[r], rv[r]); });
            } else {
              NarrowSelection(nrows, sel, has_sel, [&](uint32_t r) {
                return !lc.IsNull(r) && !rc.IsNull(r) && cmp_i(lv[r], rv[r]);
              });
            }
            return;
          }
          if (lc.type == ValueType::kDouble && lc.doubles.size() == nrows &&
              rc.doubles.size() == nrows) {
            const double* __restrict lv = lc.doubles.data();
            const double* __restrict rv = rc.doubles.data();
            const bool any_null = lc.has_null || rc.has_null;
            const BinaryOperation op = pred.op;
            auto cmp_d = [&](double a, double b) noexcept -> bool {
              switch (op) {
                case BinaryOperation::kEquals:
                  return a == b;
                case BinaryOperation::kNotEquals:
                  return a != b;
                case BinaryOperation::kLessThan:
                  return a < b;
                case BinaryOperation::kLessThanEquals:
                  return a <= b;
                case BinaryOperation::kGreaterThan:
                  return a > b;
                case BinaryOperation::kGreaterThanEquals:
                  return a >= b;
                default:
                  return false;
              }
            };
            if (!any_null) {
              NarrowSelection(nrows, sel, has_sel,
                              [&](uint32_t r) { return cmp_d(lv[r], rv[r]); });
            } else {
              NarrowSelection(nrows, sel, has_sel, [&](uint32_t r) {
                return !lc.IsNull(r) && !rc.IsNull(r) && cmp_d(lv[r], rv[r]);
              });
            }
            return;
          }
        }
      }
      const size_t need = std::max<size_t>(pred.column, pred.right_column) + 1;
      if (scratch_row->values_.size() < need) {
        scratch_row->values_.resize(need);
      }
      NarrowSelection(nrows, sel, has_sel, [&](uint32_t r) {
        if (pred.column < chunk.columns.size()) {
          chunk.columns[pred.column].WriteValueInto(
              r, &scratch_row->values_[pred.column]);
        }
        if (pred.column_payload && pred.right_column < chunk.columns.size()) {
          chunk.columns[pred.right_column].WriteValueInto(
              r, &scratch_row->values_[pred.right_column]);
        }
        return MatchSimpleCompare(*scratch_row, pred);
      });
      return;
    }
    if (pred.constant.IsNull()) {
      sel->clear();
      *has_sel = true;
      return;
    }
    const ColumnarColumn& col = chunk.columns[pred.column];
    if (col.is_uniform && col.type == ValueType::kNull) {
      sel->clear();
      *has_sel = true;
      return;
    }
    if (col.encoding == ColumnarColumn::Encoding::kConstant && col.is_uniform) {
      if (col.const_is_null) {
        sel->clear();
        *has_sel = true;
        return;
      }
      if (scratch_row->values_.size() <= pred.column) {
        scratch_row->values_.resize(pred.column + 1);
      }
      col.WriteValueInto(0, &scratch_row->values_[pred.column]);
      if (!MatchSimpleCompare(*scratch_row, pred)) {
        sel->clear();
        *has_sel = true;
      }
      return;
    }
    if (col.is_uniform && !col.is_unsigned &&
        (col.type == ValueType::kInt64 || col.type == ValueType::kDate) &&
        (pred.int_payload ||
         (col.type == ValueType::kDate && pred.date_string_payload))) {
      const int64_t rhs = pred.int_constant;
      const BinaryOperation op = pred.op;
      if (col.IsFlat()) {
        const bool has_null = col.has_null;
        const uint8_t* nulls = col.nulls.data();
        const int64_t* ints = col.ints.data();
        NarrowSelection(nrows, sel, has_sel, [&](uint32_t r) {
          if (has_null && nulls[r] != 0) {
            return false;
          }
          return ComparePositional(op, ints[r], rhs);
        });
      } else {
        NarrowSelection(nrows, sel, has_sel, [&](uint32_t r) {
          if (col.IsNull(r)) {
            return false;
          }
          return ComparePositional(op, col.IntAt(r), rhs);
        });
      }
      return;
    }
    if (col.is_uniform && col.type == ValueType::kDouble &&
        pred.double_payload) {
      const double rhs = pred.double_constant;
      const BinaryOperation op = pred.op;
      if (col.IsFlat()) {
        const bool has_null = col.has_null;
        const uint8_t* nulls = col.nulls.data();
        const double* doubles = col.doubles.data();
        NarrowSelection(nrows, sel, has_sel, [&](uint32_t r) {
          if (has_null && nulls[r] != 0) {
            return false;
          }
          return ComparePositional(op, doubles[r], rhs);
        });
      } else {
        NarrowSelection(nrows, sel, has_sel, [&](uint32_t r) {
          if (col.IsNull(r)) {
            return false;
          }
          return ComparePositional(op, col.DoubleAt(r), rhs);
        });
      }
      return;
    }
    if (col.is_uniform && !col.is_interval && col.type == ValueType::kVarChar &&
        pred.constant.type == ValueType::kVarChar &&
        !pred.constant.IsCaseInsensitive() &&
        (pred.plain_string_payload || pred.op == BinaryOperation::kLike ||
         pred.op == BinaryOperation::kNotLike)) {
      const std::string_view rhs = pred.constant.value.varchar_value;
      const BinaryOperation op = pred.op;
      if (col.encoding == ColumnarColumn::Encoding::kDictionary &&
          col.dict != nullptr) {
        auto& cache = worker_dict_caches[worker_id];
        const DictFilterCacheEntry* cached = nullptr;
        for (const auto& entry : cache) {
          if (entry.pred_ptr == &pred && entry.dict == col.dict) {
            cached = &entry;
            break;
          }
        }
        if (cached == nullptr) {
          DictFilterCacheEntry fresh;
          fresh.pred_ptr = &pred;
          fresh.dict = col.dict;
          const size_t ec = col.dict->StringEntryCount();
          // Every slot is written by the scan below; reserve avoids a
          // zero-fill that the overwrite would make redundant.
          fresh.pass.reserve(ec);
          for (size_t code = 0; code < ec; ++code) {
            const std::string_view lhs = col.dict->StringEntryAt(code);
            bool ok = false;
            if (op == BinaryOperation::kLike ||
                op == BinaryOperation::kNotLike) {
              const bool m = FastLikeMatch(lhs, rhs);
              ok = op == BinaryOperation::kLike ? m : !m;
            } else {
              ok = ComparePositional(op, lhs, rhs);
            }
            fresh.pass.push_back(ok ? 1 : 0);
          }
          if (cache.size() >= kMaxDictFilterCacheEntries) {
            cache.clear();
          }
          cache.push_back(std::move(fresh));
          cached = &cache.back();
        }
        const uint8_t* __restrict pass_arr = cached->pass.data();
        const size_t ec = cached->pass.size();
        NarrowSelection(nrows, sel, has_sel, [&](uint32_t r) {
          const uint32_t code = col.DictCodeAt(r);
          return code < ec && pass_arr[code] != 0;
        });
        return;
      }
      if (col.IsFlat()) {
        const bool has_null = col.has_null;
        const uint8_t* nulls = col.nulls.data();
        const std::string_view* strs = col.strings.data();
        NarrowSelection(nrows, sel, has_sel, [&](uint32_t r) {
          if (has_null && nulls[r] != 0) {
            return false;
          }
          const std::string_view lhs = strs[r];
          if (op == BinaryOperation::kLike || op == BinaryOperation::kNotLike) {
            const bool m = FastLikeMatch(lhs, rhs);
            return op == BinaryOperation::kLike ? m : !m;
          }
          return ComparePositional(op, lhs, rhs);
        });
      } else {
        NarrowSelection(nrows, sel, has_sel, [&](uint32_t r) {
          if (col.IsNull(r)) {
            return false;
          }
          const std::string_view lhs = col.StringAt(r);
          if (op == BinaryOperation::kLike || op == BinaryOperation::kNotLike) {
            const bool m = FastLikeMatch(lhs, rhs);
            return op == BinaryOperation::kLike ? m : !m;
          }
          return ComparePositional(op, lhs, rhs);
        });
      }
      return;
    }
    if (scratch_row->values_.size() <= pred.column) {
      scratch_row->values_.resize(pred.column + 1);
    }
    NarrowSelection(nrows, sel, has_sel, [&](uint32_t r) {
      col.WriteValueInto(r, &scratch_row->values_[pred.column]);
      return MatchSimpleCompare(*scratch_row, pred);
    });
  };

  const auto apply_prefix_in = [&](size_t worker_id, const ColumnarChunk& chunk,
                                   const PrefixInPredicate& pred,
                                   std::vector<uint32_t>* sel, bool* has_sel) {
    if (pred.column >= chunk.columns.size()) {
      sel->clear();
      *has_sel = true;
      return;
    }
    const ColumnarColumn& col = chunk.columns[pred.column];
    if (col.encoding == ColumnarColumn::Encoding::kConstant && col.is_uniform) {
      if (col.const_is_null || col.type != ValueType::kVarChar ||
          !pred.Matches(col.const_string)) {
        sel->clear();
        *has_sel = true;
      }
      return;
    }
    if (col.is_uniform && col.type == ValueType::kVarChar &&
        col.encoding == ColumnarColumn::Encoding::kDictionary &&
        col.dict != nullptr) {
      auto& cache = worker_dict_caches[worker_id];
      const DictFilterCacheEntry* cached = nullptr;
      for (const auto& entry : cache) {
        if (entry.pred_ptr == &pred && entry.dict == col.dict) {
          cached = &entry;
          break;
        }
      }
      if (cached == nullptr) {
        DictFilterCacheEntry fresh;
        fresh.pred_ptr = &pred;
        fresh.dict = col.dict;
        const size_t ec = col.dict->StringEntryCount();
        // The loop writes every slot; skip the zero-fill.
        fresh.pass.reserve(ec);
        for (size_t code = 0; code < ec; ++code) {
          const std::string_view sv = col.dict->StringEntryAt(code);
          fresh.pass.push_back(pred.Matches(sv) ? 1 : 0);
        }
        if (cache.size() >= kMaxDictFilterCacheEntries) {
          cache.clear();
        }
        cache.push_back(std::move(fresh));
        cached = &cache.back();
      }
      const uint8_t* __restrict pass_arr = cached->pass.data();
      const size_t ec = cached->pass.size();
      NarrowSelection(chunk.rows, sel, has_sel, [&](uint32_t r) {
        const uint32_t code = col.DictCodeAt(r);
        return code < ec && pass_arr[code] != 0;
      });
      return;
    }
    if (col.is_uniform && col.type == ValueType::kVarChar) {
      if (col.IsFlat()) {
        const bool has_null = col.has_null;
        const uint8_t* nulls = col.nulls.data();
        const std::string_view* strs = col.strings.data();
        NarrowSelection(chunk.rows, sel, has_sel, [&](uint32_t r) {
          if (has_null && nulls[r] != 0) {
            return false;
          }
          return pred.Matches(strs[r]);
        });
      } else {
        NarrowSelection(chunk.rows, sel, has_sel, [&](uint32_t r) {
          if (col.IsNull(r)) {
            return false;
          }
          return pred.Matches(col.StringAt(r));
        });
      }
      return;
    }
    if (col.is_uniform && col.type != ValueType::kVarChar) {
      sel->clear();
      *has_sel = true;
      return;
    }
    Value v;
    NarrowSelection(chunk.rows, sel, has_sel, [&](uint32_t r) {
      col.WriteValueInto(r, &v);
      if (v.IsNull() || v.type != ValueType::kVarChar) {
        return false;
      }
      return pred.Matches(v.value.varchar_value);
    });
  };

  const auto apply_prebuilt_membership =
      [&](size_t worker_id, const ColumnarChunk& chunk,
          const PrebuiltMembershipPredicate& pred, Row* scratch_row,
          std::vector<BytecodeColumnView>* views,
          std::vector<ColumnarColumn>* view_scratches,
          std::vector<uint8_t>* flat_view_ready, std::vector<uint32_t>* sel,
          bool* has_sel) {
        if (pred.column >= chunk.columns.size()) {
          sel->clear();
          *has_sel = true;
          return;
        }
        const ColumnarColumn& col = chunk.columns[pred.column];
        const auto resolve_flat_int = [&](bool* has_null_out,
                                          const uint8_t** nulls_out,
                                          const int64_t** ints_out) -> bool {
          if (col.IsFlat()) {
            *has_null_out = col.has_null && !col.nulls.empty();
            *nulls_out = col.nulls.data();
            *ints_out = col.ints.data();
            return true;
          }
          if (views != nullptr && view_scratches != nullptr &&
              flat_view_ready != nullptr &&
              pred.column < flat_view_ready->size()) {
            if ((*flat_view_ready)[pred.column] == 0) {
              (*views)[pred.column] = col.ToBytecodeView(
                  chunk.rows, &(*view_scratches)[pred.column], nullptr);
              (*flat_view_ready)[pred.column] = 1;
            }
            const BytecodeColumnView& v = (*views)[pred.column];
            if (v.is_uniform && v.ints != nullptr) {
              *has_null_out = v.has_null && v.nulls != nullptr;
              *nulls_out = v.nulls;
              *ints_out = v.ints;
              return true;
            }
          }
          return false;
        };
        if (col.is_uniform && pred.runtime_filter != nullptr &&
            col.type == pred.domain_type && !col.is_unsigned) {
          const RuntimeFilterGroup& rf = *pred.runtime_filter;
          if (rf.RejectsAll()) {
            sel->clear();
            *has_sel = true;
            return;
          }
          if (col.encoding == ColumnarColumn::Encoding::kConstant) {
            if (col.const_is_null || rf.Reject(col.const_int)) {
              sel->clear();
              *has_sel = true;
            }
            return;
          }
          bool has_null = false;
          const uint8_t* nulls = nullptr;
          const int64_t* ints = nullptr;
          if (resolve_flat_int(&has_null, &nulls, &ints)) {
            if (rf.has_dense_bits() && rf.has_bounds() && rf.Hi() >= rf.Lo()) {
              const auto rbase = static_cast<uint64_t>(rf.Lo());
              const uint64_t rspan_m1 = static_cast<uint64_t>(rf.Hi()) - rbase;
              if ((rspan_m1 >> 6) < rf.dense_bits_word_count()) {
                const uint64_t* __restrict dbits = rf.dense_bits_data();
                if (!has_null) {
                  NarrowSelection(chunk.rows, sel, has_sel, [&](uint32_t r) {
                    const uint64_t off = static_cast<uint64_t>(ints[r]) - rbase;
                    return off <= rspan_m1 &&
                           (dbits[off >> 6] & (uint64_t{1} << (off & 63))) != 0;
                  });
                } else {
                  NarrowSelection(chunk.rows, sel, has_sel, [&](uint32_t r) {
                    if (nulls[r] != 0) {
                      return false;
                    }
                    const uint64_t off = static_cast<uint64_t>(ints[r]) - rbase;
                    return off <= rspan_m1 &&
                           (dbits[off >> 6] & (uint64_t{1} << (off & 63))) != 0;
                  });
                }
                return;
              }
            }
            NarrowSelection(chunk.rows, sel, has_sel, [&](uint32_t r) {
              return (!has_null || nulls[r] == 0) && !rf.Reject(ints[r]);
            });
          } else {
            NarrowSelection(chunk.rows, sel, has_sel, [&](uint32_t r) {
              return !col.IsNull(r) && !rf.Reject(col.IntAt(r));
            });
          }
          return;
        }
        if (col.is_uniform && !col.is_interval &&
            col.type == ValueType::kVarChar && pred.string_values &&
            !pred.string_values->empty() &&
            col.encoding == ColumnarColumn::Encoding::kDictionary &&
            col.dict != nullptr) {
          auto& cache = worker_dict_caches[worker_id];
          const DictFilterCacheEntry* cached = nullptr;
          for (const auto& entry : cache) {
            if (entry.pred_ptr == &pred && entry.dict == col.dict) {
              cached = &entry;
              break;
            }
          }
          if (cached == nullptr) {
            DictFilterCacheEntry fresh;
            fresh.pred_ptr = &pred;
            fresh.dict = col.dict;
            const size_t ec = col.dict->StringEntryCount();
            // Slots are appended as they are decided; an early bail leaves
            // `supported` false and the table is never consulted.
            fresh.pass.reserve(ec);
            for (size_t code = 0; code < ec; ++code) {
              const std::string_view sv = col.dict->StringEntryAt(code);
              if (!PlainStringComparable(sv)) {
                fresh.supported = false;
                break;
              }
              const bool found = pred.string_values->contains(sv);
              const bool ok =
                  found ? !pred.negated : (!pred.contains_null && pred.negated);
              fresh.pass.push_back(ok ? 1 : 0);
            }
            if (cache.size() >= kMaxDictFilterCacheEntries) {
              cache.clear();
            }
            cache.push_back(std::move(fresh));
            cached = &cache.back();
          }
          if (cached->supported) {
            const uint8_t* __restrict pass_arr = cached->pass.data();
            const size_t ec = cached->pass.size();
            NarrowSelection(chunk.rows, sel, has_sel, [&](uint32_t r) {
              const uint32_t code = col.DictCodeAt(r);
              return code < ec && pass_arr[code] != 0;
            });
            return;
          }
        }
        if (pred.packed_pair_col2.has_value() && pred.values != nullptr) {
          const slot_t c2 = *pred.packed_pair_col2;
          if (c2 >= chunk.columns.size()) {
            if (!pred.negated) {
              sel->clear();
              *has_sel = true;
            }
            return;
          }
          if (pred.values->Empty() && !pred.contains_null) {
            if (!pred.negated) {
              sel->clear();
              *has_sel = true;
            }
            return;
          }
          const ColumnarColumn& col2 = chunk.columns[c2];
          if (col.is_uniform && !col.is_unsigned &&
              col.type == pred.domain_type && col2.is_uniform &&
              !col2.is_unsigned && col2.type == pred.packed_pair_type2) {
            const FlatIntSet& values = *pred.values;
            const bool neg = pred.negated;
            NarrowSelection(chunk.rows, sel, has_sel, [&](uint32_t r) {
              if (col.IsNull(r) || col2.IsNull(r)) {
                return neg;
              }
              const int64_t k0 = col.IntAt(r);
              const int64_t k1 = col2.IntAt(r);
              if (!std::in_range<int32_t>(k0) || !std::in_range<int32_t>(k1)) {
                return neg;
              }
              const uint64_t packed =
                  (static_cast<uint64_t>(
                       static_cast<uint32_t>(static_cast<int32_t>(k0)))
                   << 32) |
                  static_cast<uint32_t>(static_cast<int32_t>(k1));
              const bool found =
                  values.Contains(std::bit_cast<int64_t>(packed));
              return neg ? !found : found;
            });
            return;
          }
        }
        if (col.is_uniform && pred.runtime_filter == nullptr &&
            !pred.packed_pair_col2.has_value() &&
            pred.distinct_witness_minmax == nullptr && !pred.string_values &&
            pred.values != nullptr && col.type == pred.domain_type &&
            !col.is_unsigned && (!pred.values->Empty() || pred.contains_null)) {
          const FlatIntSet& values = *pred.values;
          const bool neg = pred.negated;
          const bool ex = pred.existence;
          const bool cnull = pred.contains_null;
          if (col.encoding == ColumnarColumn::Encoding::kConstant) {
            const bool ok =
                col.const_is_null
                    ? (ex && neg)
                    : (values.Contains(col.const_int) ? !neg : (!cnull && neg));
            if (!ok) {
              sel->clear();
              *has_sel = true;
            }
            return;
          }
          bool has_null = false;
          const uint8_t* nulls = nullptr;
          const int64_t* ints = nullptr;
          if (resolve_flat_int(&has_null, &nulls, &ints)) {
            NarrowSelection(chunk.rows, sel, has_sel, [&](uint32_t r) {
              if (has_null && nulls[r] != 0) {
                return ex && neg;
              }
              const bool found = values.Contains(ints[r]);
              return found ? !neg : (!cnull && neg);
            });
          } else {
            NarrowSelection(chunk.rows, sel, has_sel, [&](uint32_t r) {
              if (col.IsNull(r)) {
                return ex && neg;
              }
              const bool found = values.Contains(col.IntAt(r));
              return found ? !neg : (!cnull && neg);
            });
          }
          return;
        }
        slot_t max_col = pred.distinct_witness_minmax != nullptr
                             ? std::max(pred.column, pred.witness_column)
                             : pred.column;
        if (pred.packed_pair_col2.has_value()) {
          max_col = std::max(max_col, *pred.packed_pair_col2);
        }
        if (scratch_row->values_.size() <= max_col) {
          scratch_row->values_.resize(max_col + 1);
        }
        NarrowSelection(chunk.rows, sel, has_sel, [&](uint32_t r) {
          col.WriteValueInto(r, &scratch_row->values_[pred.column]);
          if (pred.distinct_witness_minmax != nullptr &&
              pred.witness_column < chunk.columns.size()) {
            chunk.columns[pred.witness_column].WriteValueInto(
                r, &scratch_row->values_[pred.witness_column]);
          }
          if (pred.packed_pair_col2.has_value() &&
              *pred.packed_pair_col2 < chunk.columns.size()) {
            chunk.columns[*pred.packed_pair_col2].WriteValueInto(
                r, &scratch_row->values_[*pred.packed_pair_col2]);
          }
          return MatchPrebuiltMembership(*scratch_row, pred);
        });
      };

  const auto eval_chunk_worker = [&](size_t w) {
    const size_t cbegin = (nchunks * w) / workers;
    const size_t cend = (nchunks * (w + 1)) / workers;
    ColumnarChunk cur_chunk;
    bool cur_chunk_init = false;
    std::vector<std::shared_ptr<ChunkGatherIndex>> cur_gathers;
    std::vector<uint32_t> sel;
    std::vector<uint32_t> disj_sel;
    std::vector<uint8_t> disj_pass;
    std::vector<BytecodeColumnView> views;
    std::vector<ColumnarColumn> view_scratches;
    std::vector<uint8_t> flat_view_ready;
    ColumnarColumn truthy_col;
    Row scratch_row;
    Row full_row;
    size_t local_accepted_rows = 0;
    size_t local_rej = 0;
    size_t local_null_rej = 0;

    const bool can_columnar_disjunction =
        !filter.disjunctive_branches.empty() &&
        std::ranges::all_of(filter.disjunctive_branches, [](const auto& b) {
          return b.residual.empty() &&
                 std::ranges::all_of(b.simple, [](const auto& sp) {
                   return !sp.column_payload;
                 });
        });

    std::vector<uint8_t> residual_needed_cols;
    if (!filter.residual.empty() && programs.residual.has_value()) {
      for (const BytecodeInstruction& inst :
           programs.residual->Instructions()) {
        if (inst.opcode == BytecodeOp::kLoadColumn) {
          if (residual_needed_cols.size() <= inst.operand) {
            residual_needed_cols.resize(inst.operand + 1, 0);
          }
          residual_needed_cols[inst.operand] = 1;
        }
      }
    }

    for (size_t ci = cbegin; ci < cend; ++ci) {
      const ColumnarChunk& chunk = input.chunks[ci];
      if (chunk.rows == 0) {
        continue;
      }
      const size_t nrows = chunk.rows;
      views.resize(chunk.columns.size());
      view_scratches.resize(chunk.columns.size());
      flat_view_ready.assign(chunk.columns.size(), 0);
      bool has_sel = false;
      sel.clear();
      const auto empty_now = [&]() { return has_sel && sel.empty(); };

      if (has_rf) {
        eval_rf_col(chunk, *runtime_filter, true, &sel, &has_sel, &local_rej,
                    &local_null_rej);
        if (empty_now()) {
          continue;
        }
        for (const ScanRuntimeFilter& ext : runtime_filter->extra) {
          if (!ext.enabled()) {
            continue;
          }
          eval_rf_col(chunk, ext, false, &sel, &has_sel, &local_rej,
                      &local_null_rej);
          if (empty_now()) {
            break;
          }
        }
        if (empty_now()) {
          continue;
        }
      }
      for (const SimpleComparePredicate& pred : filter.simple) {
        apply_simple_pred(w, chunk, pred, &scratch_row, &sel, &has_sel);
        if (empty_now()) {
          break;
        }
      }
      if (empty_now()) {
        continue;
      }
      for (const PrefixInPredicate& pred : filter.prefix_in) {
        apply_prefix_in(w, chunk, pred, &sel, &has_sel);
        if (empty_now()) {
          break;
        }
      }
      if (empty_now()) {
        continue;
      }
      for (const PrebuiltMembershipPredicate& pred :
           filter.prebuilt_membership) {
        apply_prebuilt_membership(w, chunk, pred, &scratch_row, &views,
                                  &view_scratches, &flat_view_ready, &sel,
                                  &has_sel);
        if (empty_now()) {
          break;
        }
      }
      if (empty_now()) {
        continue;
      }
      for (const PrebuiltScalarComparePredicate& pred :
           filter.prebuilt_scalar_compare) {
        const size_t need = std::max<size_t>(pred.column, pred.key_column) + 1;
        if (scratch_row.values_.size() < need) {
          scratch_row.values_.resize(need);
        }
        NarrowSelection(nrows, &sel, &has_sel, [&](uint32_t r) {
          if (pred.column < chunk.columns.size()) {
            chunk.columns[pred.column].WriteValueInto(
                r, &scratch_row.values_[pred.column]);
          }
          if (pred.key_column < chunk.columns.size()) {
            chunk.columns[pred.key_column].WriteValueInto(
                r, &scratch_row.values_[pred.key_column]);
          }
          return MatchPrebuiltScalarCompare(scratch_row, pred);
        });
        if (empty_now()) {
          break;
        }
      }
      if (empty_now()) {
        continue;
      }

      std::vector<uint32_t> chunk_deferred_rows;
      if (!filter.disjunctive_branches.empty()) {
        if (can_columnar_disjunction) {
          disj_pass.assign(nrows, 0);
          bool any_full_pass = false;
          for (size_t b = 0; b < filter.disjunctive_branches.size(); ++b) {
            const auto& branch = filter.disjunctive_branches[b];
            bool branch_has_sel = true;
            if (b == 0) {
              branch_has_sel = has_sel;
              if (has_sel) {
                disj_sel = sel;
              } else {
                disj_sel.clear();
              }
            } else {
              disj_sel.clear();
              if (has_sel) {
                for (uint32_t r : sel) {
                  if (disj_pass[r] == 0) {
                    disj_sel.push_back(r);
                  }
                }
              } else {
                for (uint32_t r = 0; r < static_cast<uint32_t>(nrows); ++r) {
                  if (disj_pass[r] == 0) {
                    disj_sel.push_back(r);
                  }
                }
              }
              if (disj_sel.empty()) {
                break;
              }
            }
            const auto branch_empty = [&]() {
              return branch_has_sel && disj_sel.empty();
            };
            for (const SimpleComparePredicate& sp : branch.simple) {
              apply_simple_pred(w, chunk, sp, &scratch_row, &disj_sel,
                                &branch_has_sel);
              if (branch_empty()) {
                break;
              }
            }
            if (!branch_empty()) {
              for (const PrefixInPredicate& pp : branch.prefix_in) {
                apply_prefix_in(w, chunk, pp, &disj_sel, &branch_has_sel);
                if (branch_empty()) {
                  break;
                }
              }
            }
            if (!branch_empty()) {
              for (const PrebuiltMembershipPredicate& pm :
                   branch.prebuilt_membership) {
                apply_prebuilt_membership(w, chunk, pm, &scratch_row, &views,
                                          &view_scratches, &flat_view_ready,
                                          &disj_sel, &branch_has_sel);
                if (branch_empty()) {
                  break;
                }
              }
            }
            if (!branch_has_sel) {
              any_full_pass = true;
              break;
            }
            for (uint32_t idx : disj_sel) {
              disj_pass[idx] = 1;
            }
          }
          if (!any_full_pass) {
            NarrowSelection(nrows, &sel, &has_sel,
                            [&](uint32_t r) { return disj_pass[r] != 0; });
          }
        } else {
          NarrowSelection(nrows, &sel, &has_sel, [&](uint32_t r) {
            bool any_branch_passed = false;
            for (size_t b = 0; b < filter.disjunctive_branches.size(); ++b) {
              const auto& branch = filter.disjunctive_branches[b];
              bool branch_ok = true;
              for (const SimpleComparePredicate& sp : branch.simple) {
                const size_t need =
                    std::max<size_t>(sp.column, sp.right_column) + 1;
                if (scratch_row.values_.size() < need) {
                  scratch_row.values_.resize(need);
                }
                if (sp.column < chunk.columns.size()) {
                  chunk.columns[sp.column].WriteValueInto(
                      r, &scratch_row.values_[sp.column]);
                }
                if (sp.column_payload &&
                    sp.right_column < chunk.columns.size()) {
                  chunk.columns[sp.right_column].WriteValueInto(
                      r, &scratch_row.values_[sp.right_column]);
                }
                if (!MatchSimpleCompare(scratch_row, sp)) {
                  branch_ok = false;
                  break;
                }
              }
              if (branch_ok) {
                for (const PrefixInPredicate& pp : branch.prefix_in) {
                  if (scratch_row.values_.size() <= pp.column) {
                    scratch_row.values_.resize(pp.column + 1);
                  }
                  if (pp.column < chunk.columns.size()) {
                    chunk.columns[pp.column].WriteValueInto(
                        r, &scratch_row.values_[pp.column]);
                  }
                  if (!MatchPrefixIn(scratch_row, pp)) {
                    branch_ok = false;
                    break;
                  }
                }
              }
              if (branch_ok) {
                for (const PrebuiltMembershipPredicate& pm :
                     branch.prebuilt_membership) {
                  slot_t max_col = pm.distinct_witness_minmax != nullptr
                                       ? std::max(pm.column, pm.witness_column)
                                       : pm.column;
                  if (pm.packed_pair_col2.has_value()) {
                    max_col = std::max(max_col, *pm.packed_pair_col2);
                  }
                  if (scratch_row.values_.size() <= max_col) {
                    scratch_row.values_.resize(max_col + 1);
                  }
                  if (pm.column < chunk.columns.size()) {
                    chunk.columns[pm.column].WriteValueInto(
                        r, &scratch_row.values_[pm.column]);
                  }
                  if (pm.distinct_witness_minmax != nullptr &&
                      pm.witness_column < chunk.columns.size()) {
                    chunk.columns[pm.witness_column].WriteValueInto(
                        r, &scratch_row.values_[pm.witness_column]);
                  }
                  if (pm.packed_pair_col2.has_value() &&
                      *pm.packed_pair_col2 < chunk.columns.size()) {
                    chunk.columns[*pm.packed_pair_col2].WriteValueInto(
                        r, &scratch_row.values_[*pm.packed_pair_col2]);
                  }
                  if (!MatchPrebuiltMembership(scratch_row, pm)) {
                    branch_ok = false;
                    break;
                  }
                }
              }
              if (branch_ok && !branch.residual.empty()) {
                chunk.MaterializeRowInto(r, &full_row);
                StatusOr<Value> decided =
                    TryEvaluateRowAuto(*programs.branch_residuals[b], full_row);
                if (!decided.HasValue()) {
                  chunk_deferred_rows.push_back(r);
                  return false;
                }
                if (!Truthy(decided.Value())) {
                  branch_ok = false;
                }
              }
              if (branch_ok) {
                any_branch_passed = true;
                break;
              }
            }
            return any_branch_passed;
          });
        }
      }

      if (!filter.residual.empty() && !empty_now()) {
        bool handled_fast_scaled = false;
        if (fast_scaled_cmp.active &&
            fast_scaled_cmp.lhs_col < chunk.columns.size() &&
            fast_scaled_cmp.rhs_col < chunk.columns.size()) {
          const ColumnarColumn& ca = chunk.columns[fast_scaled_cmp.lhs_col];
          const ColumnarColumn& cb = chunk.columns[fast_scaled_cmp.rhs_col];
          if (ca.encoding == ColumnarColumn::Encoding::kFlat && ca.is_uniform &&
              ca.type == ValueType::kDouble && ca.doubles.size() == nrows &&
              cb.encoding == ColumnarColumn::Encoding::kFlat && cb.is_uniform &&
              cb.type == ValueType::kDouble && cb.doubles.size() == nrows) {
            const double* __restrict av = ca.doubles.data();
            const double* __restrict bv = cb.doubles.data();
            const double sc = fast_scaled_cmp.scale;
            const bool no_null = !ca.has_null && !cb.has_null;
            auto cmp_val = [&](double a, double b) noexcept -> bool {
              const double rhs = sc * b;
              switch (fast_scaled_cmp.op) {
                case BinaryOperation::kLessThan:
                  return a < rhs;
                case BinaryOperation::kLessThanEquals:
                  return a <= rhs;
                case BinaryOperation::kGreaterThan:
                  return a > rhs;
                case BinaryOperation::kGreaterThanEquals:
                  return a >= rhs;
                default:
                  return false;
              }
            };
            if (!has_sel) {
              sel.clear();
              if (no_null) {
                for (uint32_t r = 0; r < static_cast<uint32_t>(nrows); ++r) {
                  if (cmp_val(av[r], bv[r])) {
                    sel.push_back(r);
                  }
                }
              } else {
                for (uint32_t r = 0; r < static_cast<uint32_t>(nrows); ++r) {
                  if (!ca.IsNull(r) && !cb.IsNull(r) && cmp_val(av[r], bv[r])) {
                    sel.push_back(r);
                  }
                }
              }
              has_sel = true;
            } else {
              size_t out_n = 0;
              if (no_null) {
                for (size_t i = 0; i < sel.size(); ++i) {
                  const uint32_t r = sel[i];
                  if (cmp_val(av[r], bv[r])) {
                    sel[out_n++] = r;
                  }
                }
              } else {
                for (size_t i = 0; i < sel.size(); ++i) {
                  const uint32_t r = sel[i];
                  if (!ca.IsNull(r) && !cb.IsNull(r) && cmp_val(av[r], bv[r])) {
                    sel[out_n++] = r;
                  }
                }
              }
              sel.resize(out_n);
            }
            handled_fast_scaled = true;
          }
        }
        if (!handled_fast_scaled && fast_date_cmp.active &&
            fast_date_cmp.lhs_col < chunk.columns.size() &&
            fast_date_cmp.rhs_col < chunk.columns.size()) {
          const ColumnarColumn& ca = chunk.columns[fast_date_cmp.lhs_col];
          const ColumnarColumn& cb = chunk.columns[fast_date_cmp.rhs_col];
          const ValueType want_vt =
              fast_date_cmp.is_date ? ValueType::kDate : ValueType::kInt64;
          if (ca.is_uniform && !ca.is_unsigned && ca.type == want_vt &&
              cb.is_uniform && !cb.is_unsigned && cb.type == want_vt) {
            const bool a_flat = ca.IsFlat() && ca.ints.size() == nrows;
            const bool b_flat = cb.IsFlat() && cb.ints.size() == nrows;
            const int64_t* __restrict av = a_flat ? ca.ints.data() : nullptr;
            const int64_t* __restrict bv = b_flat ? cb.ints.data() : nullptr;
            const int64_t delta = fast_date_cmp.delta;
            const bool is_date = fast_date_cmp.is_date;
            const bool no_null = !ca.has_null && !cb.has_null;
            const BinaryOperation op = fast_date_cmp.op;
            const bool has_bc_fallback = programs.residual.has_value();
            bool out_of_range = false;
            auto cmp_row = [&](uint32_t r) -> bool {
              if (!no_null && (ca.IsNull(r) || cb.IsNull(r))) {
                return false;
              }
              const int64_t a = a_flat ? av[r] : ca.IntAt(r);
              const int64_t b = b_flat ? bv[r] : cb.IntAt(r);
              int64_t rhs = 0;
              if (__builtin_add_overflow(b, delta, &rhs) ||
                  (is_date && (rhs < kDateMinDays || rhs > kDateMaxDays)))
                  [[unlikely]] {
                if (has_bc_fallback) {
                  out_of_range = true;
                } else {
                  chunk_deferred_rows.push_back(r);
                }
                return false;
              }
              switch (op) {
                case BinaryOperation::kLessThan:
                  return a < rhs;
                case BinaryOperation::kLessThanEquals:
                  return a <= rhs;
                case BinaryOperation::kGreaterThan:
                  return a > rhs;
                case BinaryOperation::kGreaterThanEquals:
                  return a >= rhs;
                case BinaryOperation::kEquals:
                  return a == rhs;
                case BinaryOperation::kNotEquals:
                  return a != rhs;
                default:
                  return false;
              }
            };
            std::vector<uint32_t> saved_sel;
            const bool saved_has_sel = has_sel;
            if (has_bc_fallback && has_sel) {
              saved_sel = sel;
            }
            if (!has_sel) {
              sel.clear();
              for (uint32_t r = 0; r < static_cast<uint32_t>(nrows); ++r) {
                if (cmp_row(r)) {
                  sel.push_back(r);
                }
              }
              has_sel = true;
            } else {
              size_t out_n = 0;
              for (size_t i = 0; i < sel.size(); ++i) {
                const uint32_t r = sel[i];
                if (cmp_row(r)) {
                  sel[out_n++] = r;
                }
              }
              sel.resize(out_n);
            }
            if (out_of_range) {
              sel = std::move(saved_sel);
              has_sel = saved_has_sel;
            } else {
              handled_fast_scaled = true;
            }
          }
        }
        if (!handled_fast_scaled) {
          const size_t active_count = has_sel ? sel.size() : nrows;
          for (size_t c = 0; c < chunk.columns.size(); ++c) {
            if (c < residual_needed_cols.size() &&
                residual_needed_cols[c] != 0) {
              if (flat_view_ready[c] == 0) {
                views[c] = chunk.columns[c].ToBytecodeView(
                    chunk.rows, &view_scratches[c], nullptr);
                flat_view_ready[c] = 1;
              }
            } else {
              views[c] = BytecodeColumnView{};
            }
          }
          truthy_col.Init(ValueType::kNull, false, false, active_count);
          BytecodeColumnSink sink = truthy_col.AsBytecodeSink(0);
          Status eval_err{Status::kSuccess};
          const auto st = programs.residual->TryEvaluateColumnarBatch(
              views, has_sel ? sel.data() : nullptr, active_count, &sink,
              &eval_err);
          if (st == BytecodeProgram::FastEval::kDone) {
            truthy_col.SyncFromBytecodeSink(sink, nullptr, active_count);
            if (!has_sel) {
              sel.clear();
              for (size_t i = 0; i < active_count; ++i) {
                if (BytecodeColumnTruthy(truthy_col, i)) {
                  sel.push_back(static_cast<uint32_t>(i));
                }
              }
              has_sel = true;
            } else {
              size_t out = 0;
              for (size_t i = 0; i < active_count; ++i) {
                if (BytecodeColumnTruthy(truthy_col, i)) {
                  sel[out++] = sel[i];
                }
              }
              sel.resize(out);
            }
          } else {
            NarrowSelection(nrows, &sel, &has_sel, [&](uint32_t r) {
              chunk.MaterializeRowInto(r, &full_row);
              StatusOr<Value> decided =
                  TryEvaluateRowAuto(*programs.residual, full_row);
              if (!decided.HasValue()) {
                chunk_deferred_rows.push_back(r);
                return false;
              }
              return Truthy(decided.Value());
            });
          }
        }
      }

      if (!chunk_deferred_rows.empty()) {
        std::sort(chunk_deferred_rows.begin(), chunk_deferred_rows.end());
        size_t def_idx = 0;
        const auto push_def = [&](Row&& r, size_t before_acc) {
          deferred[w].push_back(DeferredChunkRow{.preceding_rows = before_acc,
                                                 .row = std::move(r)});
        };
        const size_t acc_cnt = has_sel ? sel.size() : nrows;
        for (size_t i = 0; i < acc_cnt; ++i) {
          const uint32_t r = has_sel ? sel[i] : static_cast<uint32_t>(i);
          while (def_idx < chunk_deferred_rows.size() &&
                 chunk_deferred_rows[def_idx] < r) {
            chunk.MaterializeRowInto(chunk_deferred_rows[def_idx++], &full_row);
            push_def(std::move(full_row), local_accepted_rows + i);
            full_row = Row{};
          }
        }
        while (def_idx < chunk_deferred_rows.size()) {
          chunk.MaterializeRowInto(chunk_deferred_rows[def_idx++], &full_row);
          push_def(std::move(full_row), local_accepted_rows + acc_cnt);
          full_row = Row{};
        }
      }

      const size_t keep_count = has_sel ? sel.size() : nrows;
      if (keep_count == 0) {
        continue;
      }
      if (!has_sel && keep_count == nrows) {
        if (cur_chunk_init && cur_chunk.rows > 0) {
          worker_tables[w].AppendChunk(std::move(cur_chunk));
          cur_chunk = ColumnarChunk{};
          cur_chunk_init = false;
          cur_gathers.clear();
        }
        worker_tables[w].AppendChunk(ColumnarChunk(chunk));
        local_accepted_rows += keep_count;
        continue;
      }
      const size_t ncols = chunk.columns.size();
      const uint32_t* sel_ptr = has_sel ? sel.data() : nullptr;
      size_t offset = 0;
      const auto append_keepalive_unique =
          [&](const std::shared_ptr<const void>& k) {
            if (k == nullptr) {
              return;
            }
            for (const auto& existing : cur_chunk.keepalives) {
              if (existing.get() == k.get()) {
                return;
              }
            }
            cur_chunk.keepalives.push_back(k);
          };
      while (offset < keep_count) {
        if (!cur_chunk_init) {
          cur_chunk.InitFromSchema(match_schema, 0);
          cur_chunk_init = true;
          cur_gathers.clear();
        }
        const size_t space = kColumnarChunkRows - cur_chunk.rows;
        const size_t n = std::min(space, keep_count - offset);
        const size_t prev_rows = cur_chunk.rows;
        if (sel_ptr == nullptr && offset > 0) {
          sel.resize(keep_count);
          for (size_t i = 0; i < keep_count; ++i) {
            sel[i] = static_cast<uint32_t>(i);
          }
          sel_ptr = sel.data();
        }
        const uint32_t* slice_sel =
            sel_ptr != nullptr ? (sel_ptr + offset) : nullptr;
        std::vector<std::pair<const ChunkGatherIndex*,
                              std::shared_ptr<ChunkGatherIndex>>>
            slice_gi_map;
        const auto get_filtered_gather =
            [&](const ColumnarColumn& src_col,
                size_t col_idx) -> std::shared_ptr<ChunkGatherIndex> {
          const ChunkGatherIndex* src_gi = src_col.gather_index.get();
          for (const auto& entry : slice_gi_map) {
            if (entry.first == src_gi) {
              return entry.second;
            }
          }
          std::shared_ptr<ChunkGatherIndex> dst_gi;
          if (prev_rows == 0) {
            dst_gi = std::make_shared<ChunkGatherIndex>();
            dst_gi->source_table = src_gi->source_table;
            dst_gi->src_chunks.reserve(n);
            dst_gi->src_rows.reserve(n);
            cur_gathers.push_back(dst_gi);
          } else if (cur_chunk.columns[col_idx].encoding ==
                         ColumnarColumn::Encoding::kGathered &&
                     cur_chunk.columns[col_idx].gather_index != nullptr) {
            const ChunkGatherIndex* prev_gi_ptr =
                cur_chunk.columns[col_idx].gather_index.get();
            for (const auto& cg : cur_gathers) {
              if (cg.get() == prev_gi_ptr &&
                  cg->source_table == src_gi->source_table &&
                  cg->src_rows.size() == prev_rows) {
                dst_gi = cg;
                break;
              }
            }
          }
          if (dst_gi == nullptr) {
            return nullptr;
          }
          const size_t before = dst_gi->src_rows.size();
          dst_gi->src_chunks.resize(before + n);
          dst_gi->src_rows.resize(before + n);
          bool found_null = dst_gi->has_null;
          for (size_t i = 0; i < n; ++i) {
            const uint32_t r =
                slice_sel != nullptr ? slice_sel[i] : static_cast<uint32_t>(i);
            const uint32_t sr = src_gi->src_rows[r];
            dst_gi->src_chunks[before + i] = src_gi->src_chunks[r];
            dst_gi->src_rows[before + i] = sr;
            if (sr == ChunkGatherIndex::kNullRow) {
              found_null = true;
            }
          }
          dst_gi->has_null = found_null;
          slice_gi_map.emplace_back(src_gi, dst_gi);
          return dst_gi;
        };
        for (size_t c = 0; c < ncols; ++c) {
          const ColumnarColumn& sc = chunk.columns[c];
          if (sc.encoding == ColumnarColumn::Encoding::kGathered &&
              sc.gather_index != nullptr &&
              (prev_rows == 0 || cur_chunk.columns[c].encoding ==
                                     ColumnarColumn::Encoding::kGathered)) {
            if (auto dst_gi = get_filtered_gather(sc, c)) {
              cur_chunk.columns[c].SetGathered(
                  std::move(dst_gi), sc.gather_col, sc.type, sc.is_unsigned,
                  sc.is_interval, sc.is_uniform, sc.has_null);
              continue;
            }
          }
          cur_chunk.columns[c].AppendSliceFrom(sc, slice_sel, n, nullptr,
                                               prev_rows);
        }
        append_keepalive_unique(chunk.string_arena);
        for (const auto& k : chunk.keepalives) {
          append_keepalive_unique(k);
        }
        cur_chunk.rows += n;
        offset += n;
        if (cur_chunk.rows == kColumnarChunkRows) {
          worker_tables[w].AppendChunk(std::move(cur_chunk));
          cur_chunk = ColumnarChunk{};
          cur_chunk_init = false;
          cur_gathers.clear();
        }
      }
      local_accepted_rows += keep_count;
    }
    if (cur_chunk_init && cur_chunk.rows > 0) {
      worker_tables[w].AppendChunk(std::move(cur_chunk));
    }
    if (has_rf) {
      key_rejected.fetch_add(local_rej, std::memory_order_relaxed);
      key_null_rejected.fetch_add(local_null_rej, std::memory_order_relaxed);
    }
  };

  if (workers <= 1) {
    eval_chunk_worker(0);
  } else {
    const Status st = RunParallelTasks(workers, eval_chunk_worker);
    if (!st.ok()) {
      if (error != nullptr && error->ok()) {
        *error = st;
      }
      return true;
    }
  }

  auto out = std::make_shared<ColumnarTable>();
  out->keepalives = input.keepalives;
  const bool any_deferred =
      std::ranges::any_of(deferred, [](const auto& d) { return !d.empty(); });
  if (!any_deferred) {
    for (size_t w = 0; w < workers; ++w) {
      out->AppendTable(std::move(worker_tables[w]));
    }
  } else {
    const auto record_error = [&](Status status) {
      if (error != nullptr && error->ok() && status != Status::kSuccess) {
        *error = std::move(status);
      }
    };
    ColumnarChunk merged_chunk;
    bool merged_chunk_init = false;
    const auto emit_row = [&](const Row& r) {
      if (!merged_chunk_init) {
        merged_chunk.InitFromSchema(match_schema, kColumnarChunkRows);
        merged_chunk_init = true;
      }
      merged_chunk.AppendRow(r);
      if (merged_chunk.rows == kColumnarChunkRows) {
        out->AppendChunk(std::move(merged_chunk));
        merged_chunk = ColumnarChunk{};
        merged_chunk_init = false;
      }
    };
    for (size_t w = 0; w < workers; ++w) {
      if (deferred[w].empty()) {
        if (merged_chunk_init && merged_chunk.rows > 0) {
          out->AppendChunk(std::move(merged_chunk));
          merged_chunk = ColumnarChunk{};
          merged_chunk_init = false;
        }
        out->AppendTable(std::move(worker_tables[w]));
        continue;
      }
      size_t accepted = 0;
      size_t di = 0;
      const auto emit_def = [&] {
        while (di < deferred[w].size() &&
               deferred[w][di].preceding_rows == accepted) {
          Row& r = deferred[w][di++].row;
          Status def_err{Status::kSuccess};
          if (MatchScanFilter(r, match_schema, filter, outer, context, ctes,
                              &def_err)) {
            emit_row(r);
          }
          record_error(std::move(def_err));
        }
      };
      Row r_tmp;
      for (const ColumnarChunk& wc : worker_tables[w].chunks) {
        for (size_t r = 0; r < wc.rows; ++r) {
          emit_def();
          wc.MaterializeRowInto(r, &r_tmp);
          emit_row(r_tmp);
          ++accepted;
        }
      }
      emit_def();
      deferred[w].clear();
    }
    if (merged_chunk_init && merged_chunk.rows > 0) {
      out->AppendChunk(std::move(merged_chunk));
    }
  }
  const bool out_uniform = ComputeColumnarUniformNonNull(*out);
  result->SetColumnarTable(std::move(out), out_uniform);
  if (context.execution_runtime() != nullptr) {
    context.execution_runtime()->key_filter_rejected += key_rejected.load();
    context.execution_runtime()->key_filter_null_rejected +=
        key_null_rejected.load();
  }
  return true;
}

}  // namespace

bool TryParallelFilterRows(TransactionContext& context, const Relation& source,
                           const CompiledScanFilter& filter,
                           const Schema& match_schema, const Scope* outer,
                           const CteMap& ctes,
                           const ScanRuntimeFilter* runtime_filter,
                           Relation* result, Status* error,
                           Relation* movable_source) {
  constexpr size_t kMinParallelFilterRows = 16384;
  constexpr size_t kMaxParallelFilterWorkers = 16;
  static const bool parallel_filter_disabled =
      ::tinylamb::platform::GetEnv(
          "TINYLAMB_DISABLE_PARALLEL_RELATION_FILTER") != nullptr;
  if (source.HasSpill() ||
      (movable_source != nullptr && movable_source != &source)) {
    return false;
  }
  // Register the relation being filtered so a correlated-subquery build
  // triggered while evaluating deferred residuals (MatchScanFilter on the
  // consumer thread, or inside TryFilterColumnarTable) can prune its build
  // scan to this pipeline's outer key domain — the same contract the serial
  // path in relational.cpp establishes around its ForEachRow loop.
  ScopedCorrelatedOuter correlated_outer(context, source);
  if (source.HasColumnarOnly()) {
    return TryFilterColumnarTable(context, source.columnar, filter,
                                  match_schema, outer, ctes, runtime_filter,
                                  result, error, parallel_filter_disabled);
  }
  if (parallel_filter_disabled || source.rows.size() < kMinParallelFilterRows) {
    return false;
  }
  const WorkerScanFilterPrograms programs =
      CompileWorkerFilter(filter, match_schema);
  if (!programs.complete) {
    return false;
  }
  const size_t workers =
      std::min({kMaxParallelFilterWorkers,
                QueryParallelWorkerLimit(std::thread::hardware_concurrency()),
                source.rows.size() / 4096});
  if (workers <= 1) {
    return false;
  }
  std::vector<Relation> shards(workers);
  struct DeferredRow {
    size_t preceding_rows;
    Row row;
  };
  std::vector<std::vector<DeferredRow>> deferred(workers);
  std::atomic<size_t> key_rejected{0};
  std::atomic<size_t> key_null_rejected{0};
  std::mutex error_mu;
  Status error_ex{Status::kSuccess};
  const size_t chunk_rows = (source.rows.size() + workers - 1) / workers;
  {
    std::vector<std::jthread> threads;
    threads.reserve(workers);
    for (size_t w = 0; w < workers; ++w) {
      try {
        threads.emplace_back([&, w] {
          try {
            auto& local = shards[w];
            // Each worker owns one contiguous range. Merge ranges in source
            // order, including rows that need contextual evaluation.
            size_t accepted_rows = 0;
            const size_t begin = w * chunk_rows;
            const size_t end = std::min(source.rows.size(), begin + chunk_rows);
            for (size_t i = begin; i < end; ++i) {
              const Row& row = source.rows[i];
              if (runtime_filter != nullptr && runtime_filter->enabled()) {
                const Value& key = row[runtime_filter->column];
                if (runtime_filter->Reject(static_cast<int8_t>(key.type),
                                           key.IsNull(), key.value.int_value)) {
                  ++key_rejected;
                  if (key.IsNull()) {
                    ++key_null_rejected;
                  }
                  continue;
                }
              }
              switch (
                  MatchScanFilterWorker(row, match_schema, filter, programs)) {
                case 0:
                  break;
                case 1:
                  ++accepted_rows;
                  if (movable_source != nullptr) {
                    local.AddRow(std::move(movable_source->rows[i]));
                  } else {
                    local.AddRow(row);
                  }
                  break;
                default:
                  if (movable_source != nullptr) {
                    deferred[w].push_back(
                        {accepted_rows, std::move(movable_source->rows[i])});
                  } else {
                    deferred[w].push_back({accepted_rows, row});
                  }
                  break;
              }
            }
          } catch (...) {
            std::scoped_lock lock(error_mu);
            if (error_ex == Status::kSuccess) {
              error_ex =
                  StatusFromCurrentException("parallel filter worker failed");
            }
          }
        });
      } catch (...) {
        // Thread *creation* failed, not the worker body. Record it, or the
        // merge below drains empty shards and reports a clean result that is
        // silently missing every row the missing worker would have produced.
        std::scoped_lock lock(error_mu);
        if (error_ex == Status::kSuccess) {
          error_ex =
              StatusFromCurrentException("parallel filter worker failed");
        }
        break;
      }
    }
  }
  if (error_ex != Status::kSuccess) {
    if (error != nullptr && error->ok()) {
      *error = error_ex;
    }
    return true;
  }
  // Main-thread merge in worker order; deferred rows take the contextual
  // MatchScanFilter path so error semantics are bit-identical to serial.
  const auto record_error = [&](Status status) {
    if (error != nullptr && error->ok() && status != Status::kSuccess) {
      *error = std::move(status);
    }
  };
  size_t total_in_memory = 0;
  for (size_t w = 0; w < workers; ++w) {
    if (!shards[w].HasSpill()) {
      total_in_memory += shards[w].rows.size();
    }
  }
  if (total_in_memory > 0) {
    result->rows.reserve(result->rows.size() + total_in_memory);
  }
  for (size_t w = 0; w < workers; ++w) {
    record_error(shards[w].FinishSpill());
    if (deferred[w].empty() && !shards[w].HasSpill()) {
      result->MergeInMemoryShard(std::move(shards[w]));
    } else {
      size_t accepted_rows = 0;
      size_t deferred_index = 0;
      const auto emit_deferred = [&] {
        while (deferred_index < deferred[w].size() &&
               deferred[w][deferred_index].preceding_rows == accepted_rows) {
          Row& row = deferred[w][deferred_index++].row;
          Status deferred_error{Status::kSuccess};
          if (MatchScanFilter(row, match_schema, filter, outer, context, ctes,
                              &deferred_error)) {
            result->AddRow(std::move(row));
          }
          record_error(std::move(deferred_error));
        }
      };
      if (shards[w].HasSpill()) {
        // ForEachRow includes the append-only tail as well as resident rows.
        record_error(shards[w].ForEachRow([&](const Row& row) {
          emit_deferred();
          result->AddRow(row);
          ++accepted_rows;
        }));
      } else {
        for (Row& row : shards[w].rows) {
          emit_deferred();
          result->AddRow(std::move(row));
          ++accepted_rows;
        }
      }
      emit_deferred();
    }
    deferred[w].clear();
    record_error(shards[w].GetStatus());
    shards[w].ResetContents();
  }
  if (context.execution_runtime() != nullptr) {
    context.execution_runtime()->key_filter_rejected += key_rejected.load();
    context.execution_runtime()->key_filter_null_rejected +=
        key_null_rejected.load();
  }
  return true;
}

}  // namespace tinylamb::relational_detail
