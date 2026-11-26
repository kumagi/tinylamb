/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <functional>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/log_message.hpp"
#include "common/parallel_work.hpp"
#include "common/platform/int128_conversion.hpp"
#include "common/platform/process.hpp"
#include "common/platform/range_selection.hpp"
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/columnar_tlcs_fast_path.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/relation.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/subquery_runtime.hpp"
#include "executor/detail/typed_stream_aggregate.hpp"
#include "executor/detail/typed_stream_aggregate_internal.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/expression.hpp"
#include "expression/query_expression.hpp"
#include "table/full_scan_iterator.hpp"
#include "table/iterator.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "type/column.hpp"
#include "type/type.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

namespace {

struct DirectBlockCol {
  const LSTableColumnBlock* col{nullptr};
  const int64_t* cells{nullptr};
  bool has_null{false};
  std::vector<int64_t> scratch;

  void Bind(const LSTableColumnBlock& blk_col, uint32_t row_count) {
    col = &blk_col;
    has_null = blk_col.has_null;
    if (blk_col.type == ValueType::kVarChar) {
      cells = nullptr;
      return;
    }
    if (blk_col.HasRawIntCells()) {
      cells = blk_col.int_cells.data();
    } else {
      scratch.resize(row_count);
      blk_col.MaterializeIntSlice(0, row_count, scratch.data());
      cells = scratch.data();
    }
  }

  [[nodiscard]] bool IsNull(uint32_t r) const noexcept {
    return has_null && col->IsNull(r);
  }
  [[nodiscard]] int64_t Int(uint32_t r) const noexcept { return cells[r]; }
  [[nodiscard]] double Double(uint32_t r) const noexcept {
    return std::bit_cast<double>(cells[r]);
  }
  [[nodiscard]] std::string_view VarChar(uint32_t r) const noexcept {
    return col->VarcharAt(r);
  }
};

template <typename T>
inline bool CompareDirectScalar(BinaryOperation op, T a, T b) noexcept {
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
}

inline bool IsStandardCompareOp(BinaryOperation op) noexcept {
  return op == BinaryOperation::kEquals || op == BinaryOperation::kNotEquals ||
         op == BinaryOperation::kLessThan ||
         op == BinaryOperation::kLessThanEquals ||
         op == BinaryOperation::kGreaterThan ||
         op == BinaryOperation::kGreaterThanEquals;
}

enum class BlockFilterStatus : uint8_t {
  kAllFail = 0,
  kAllPass = 1,
  kMixed = 2,
};

struct CompiledComparePred {
  TypedFilterCompare cmp;
  slot_t full_left{0};
  slot_t full_right{0};
  bool is_int{false};
  bool is_double{false};
  bool is_string{false};
};

inline BlockFilterStatus CheckCompareBlockStatus(
    const CompiledComparePred& pred, const LSTablePageBlock& blk) noexcept {
  if (!pred.cmp.constant_payload || pred.is_string) {
    return BlockFilterStatus::kMixed;
  }
  const LSTableColumnBlock& col = blk.columns[pred.full_left];
  if (pred.is_double) {
    const double k = pred.cmp.double_constant;
    if (!col.has_double_min_max || col.has_nan || std::isnan(k)) {
      return BlockFilterStatus::kMixed;
    }
    const double lo = col.min_double;
    const double hi = col.max_double;
    bool all_fail = false;
    bool all_pass = false;
    switch (pred.cmp.op) {
      case BinaryOperation::kEquals:
        all_fail = (k < lo || k > hi);
        all_pass = (lo == k && hi == k);
        break;
      case BinaryOperation::kNotEquals:
        all_fail = (lo == k && hi == k);
        all_pass = (k < lo || k > hi);
        break;
      case BinaryOperation::kLessThan:
        all_fail = (lo >= k);
        all_pass = (hi < k);
        break;
      case BinaryOperation::kLessThanEquals:
        all_fail = (lo > k);
        all_pass = (hi <= k);
        break;
      case BinaryOperation::kGreaterThan:
        all_fail = (hi <= k);
        all_pass = (lo > k);
        break;
      case BinaryOperation::kGreaterThanEquals:
        all_fail = (hi < k);
        all_pass = (lo >= k);
        break;
      default:
        break;
    }
    if (all_fail) {
      return BlockFilterStatus::kAllFail;
    }
    if (all_pass && !col.has_null) {
      return BlockFilterStatus::kAllPass;
    }
    return BlockFilterStatus::kMixed;
  }
  const int64_t k = pred.cmp.int_constant;
  if (!col.has_min_max) {
    return BlockFilterStatus::kMixed;
  }
  const int64_t lo = col.min_int;
  const int64_t hi = col.max_int;
  bool all_fail = false;
  bool all_pass = false;
  switch (pred.cmp.op) {
    case BinaryOperation::kEquals:
      all_fail = (k < lo || k > hi || !col.BlockBloomMayContain(k));
      all_pass = (lo == k && hi == k);
      break;
    case BinaryOperation::kNotEquals:
      all_fail = (lo == k && hi == k);
      all_pass = (k < lo || k > hi);
      break;
    case BinaryOperation::kLessThan:
      all_fail = (lo >= k);
      all_pass = (hi < k);
      break;
    case BinaryOperation::kLessThanEquals:
      all_fail = (lo > k);
      all_pass = (hi <= k);
      break;
    case BinaryOperation::kGreaterThan:
      all_fail = (hi <= k);
      all_pass = (lo > k);
      break;
    case BinaryOperation::kGreaterThanEquals:
      all_fail = (hi < k);
      all_pass = (lo >= k);
      break;
    default:
      break;
  }
  if (all_fail) {
    return BlockFilterStatus::kAllFail;
  }
  if (all_pass && !col.has_null) {
    return BlockFilterStatus::kAllPass;
  }
  return BlockFilterStatus::kMixed;
}

inline bool EvalCompiledCompareRow(const CompiledComparePred& pred,
                                   const std::vector<DirectBlockCol>& cols,
                                   uint32_t r) noexcept {
  const DirectBlockCol& lcol = cols[pred.full_left];
  if (lcol.IsNull(r)) {
    return false;
  }
  if (pred.cmp.constant_payload) {
    if (pred.is_string) {
      return MatchTypedStringCompare(lcol.VarChar(r), pred.cmp);
    }
    if (pred.is_double) {
      return CompareDirectScalar(pred.cmp.op, lcol.Double(r),
                                 pred.cmp.double_constant);
    }
    return CompareDirectScalar(pred.cmp.op, lcol.Int(r), pred.cmp.int_constant);
  }
  const DirectBlockCol& rcol = cols[pred.full_right];
  if (rcol.IsNull(r)) {
    return false;
  }
  if (pred.is_string) {
    return CompareDirectScalar(pred.cmp.op, lcol.VarChar(r), rcol.VarChar(r));
  }
  if (pred.is_double) {
    return CompareDirectScalar(pred.cmp.op, lcol.Double(r), rcol.Double(r));
  }
  return CompareDirectScalar(pred.cmp.op, lcol.Int(r), rcol.Int(r));
}

struct CompiledLSTableBlockFilter {
  struct NullPred {
    slot_t full_col{0};
    bool is_null{true};
  };
  struct IntInterval {
    slot_t full_col{0};
    int64_t lo{std::numeric_limits<int64_t>::min()};
    int64_t hi{std::numeric_limits<int64_t>::max()};
    bool empty{false};
  };
  // Lean proof:
  // lean/TinylambOpt/ResearchProofs/AnalyticsWorkloads/HotKeyAndEpochAggregates.lean
  // (InRangeSynopsis_sound).
  struct IntInList {
    slot_t full_col{0};
    int64_t lo{0};
    int64_t hi{0};
    uint64_t mask{0};
    bool use_mask{false};
    std::vector<int64_t> values;
  };
  struct Branch {
    std::vector<CompiledComparePred> preds;
  };
  struct Disjunction {
    std::vector<Branch> branches;
  };

  std::vector<NullPred> null_preds;
  std::vector<IntInterval> int_intervals;
  std::vector<IntInList> int_inlists;
  std::vector<CompiledComparePred> stage1_preds;
  std::vector<Disjunction> disjunctions;
  std::vector<uint8_t> referenced_full_cols;
  bool always_false{false};

  static bool CompileCompare(const TypedFilterCompare& cmp,
                             const std::vector<slot_t>& proj_to_full,
                             const std::vector<ValueType>& proj_types,
                             const std::vector<uint8_t>& proj_unsigned,
                             const LSTableStore& store,
                             std::vector<uint8_t>* referenced,
                             CompiledComparePred* out) {
    if (cmp.left >= proj_types.size()) {
      return false;
    }
    const slot_t full_left = proj_to_full[cmp.left];
    if (full_left >= store.column_has_null.size()) {
      return false;
    }
    const ValueType ltype = proj_types[cmp.left];
    out->cmp = cmp;
    out->full_left = full_left;
    out->full_right = 0;
    out->is_int = false;
    out->is_double = false;
    out->is_string = false;
    if (cmp.constant_payload) {
      if (cmp.string_payload) {
        if (ltype != ValueType::kVarChar) {
          return false;
        }
        if (!IsStandardCompareOp(cmp.op) && cmp.op != BinaryOperation::kLike &&
            cmp.op != BinaryOperation::kNotLike) {
          return false;
        }
        out->is_string = true;
      } else if (cmp.is_double) {
        if (ltype != ValueType::kDouble || !IsStandardCompareOp(cmp.op)) {
          return false;
        }
        out->is_double = true;
      } else {
        if ((ltype != ValueType::kInt64 && ltype != ValueType::kDate) ||
            proj_unsigned[cmp.left] != 0 || !IsStandardCompareOp(cmp.op)) {
          return false;
        }
        out->is_int = true;
      }
      (*referenced)[full_left] = 1;
      return true;
    }
    if (cmp.right >= proj_types.size() || !IsStandardCompareOp(cmp.op)) {
      return false;
    }
    const slot_t full_right = proj_to_full[cmp.right];
    if (full_right >= store.column_has_null.size() ||
        ltype != proj_types[cmp.right]) {
      return false;
    }
    if (cmp.string_payload) {
      if (ltype != ValueType::kVarChar) {
        return false;
      }
      out->is_string = true;
    } else if (cmp.is_double) {
      if (ltype != ValueType::kDouble) {
        return false;
      }
      out->is_double = true;
    } else {
      if ((ltype != ValueType::kInt64 && ltype != ValueType::kDate) ||
          proj_unsigned[cmp.left] != 0 || proj_unsigned[cmp.right] != 0) {
        return false;
      }
      out->is_int = true;
    }
    out->full_right = full_right;
    (*referenced)[full_left] = 1;
    (*referenced)[full_right] = 1;
    return true;
  }

  bool Init(const TypedAggregationPlan& plan,
            const std::vector<slot_t>& proj_to_full,
            const std::vector<ValueType>& proj_types,
            const std::vector<uint8_t>& proj_unsigned,
            const LSTableStore& store) {
    referenced_full_cols.assign(store.column_has_null.size(), 0);
    std::vector<int> interval_idx(store.column_has_null.size(), -1);
    for (const TypedNullCompare& nf : plan.null_filters) {
      if (nf.column >= proj_types.size()) {
        return false;
      }
      const slot_t full_col = proj_to_full[nf.column];
      if (full_col >= store.column_has_null.size()) {
        return false;
      }
      null_preds.push_back(
          NullPred{.full_col = full_col, .is_null = nf.is_null});
      referenced_full_cols[full_col] = 1;
    }
    for (const TypedFilterCompare& cmp : plan.filter_compares) {
      CompiledComparePred pred;
      if (!CompileCompare(cmp, proj_to_full, proj_types, proj_unsigned, store,
                          &referenced_full_cols, &pred)) {
        return false;
      }
      if (pred.cmp.constant_payload && pred.is_int &&
          pred.cmp.op != BinaryOperation::kNotEquals) {
        int& slot = interval_idx[pred.full_left];
        if (slot < 0) {
          slot = static_cast<int>(int_intervals.size());
          int_intervals.push_back(IntInterval{.full_col = pred.full_left});
        }
        IntInterval& iv = int_intervals[static_cast<size_t>(slot)];
        const int64_t k = pred.cmp.int_constant;
        switch (pred.cmp.op) {
          case BinaryOperation::kEquals:
            iv.lo = std::max(iv.lo, k);
            iv.hi = std::min(iv.hi, k);
            break;
          case BinaryOperation::kGreaterThanEquals:
            iv.lo = std::max(iv.lo, k);
            break;
          case BinaryOperation::kGreaterThan:
            if (k == std::numeric_limits<int64_t>::max()) {
              iv.empty = true;
            } else {
              iv.lo = std::max(iv.lo, k + 1);
            }
            break;
          case BinaryOperation::kLessThanEquals:
            iv.hi = std::min(iv.hi, k);
            break;
          case BinaryOperation::kLessThan:
            if (k == std::numeric_limits<int64_t>::min()) {
              iv.empty = true;
            } else {
              iv.hi = std::min(iv.hi, k - 1);
            }
            break;
          default:
            break;
        }
        iv.empty = iv.empty || iv.lo > iv.hi;
        if (iv.empty) {
          always_false = true;
        }
      } else {
        stage1_preds.push_back(std::move(pred));
      }
    }
    for (const auto& raw_disj : plan.filter_disjunctions) {
      if (raw_disj.empty()) {
        always_false = true;
        continue;
      }
      Disjunction disj;
      for (const auto& raw_branch : raw_disj) {
        Branch branch;
        for (const TypedFilterCompare& cmp : raw_branch) {
          CompiledComparePred pred;
          if (!CompileCompare(cmp, proj_to_full, proj_types, proj_unsigned,
                              store, &referenced_full_cols, &pred)) {
            return false;
          }
          branch.preds.push_back(std::move(pred));
        }
        disj.branches.push_back(std::move(branch));
      }
      disjunctions.push_back(std::move(disj));
    }
    for (const TypedInListCompare& list : plan.inlist_compares) {
      if (list.is_string || list.is_double ||
          list.column >= proj_types.size() || proj_unsigned[list.column] != 0 ||
          (proj_types[list.column] != ValueType::kInt64 &&
           proj_types[list.column] != ValueType::kDate)) {
        return false;
      }
      const slot_t full_col = proj_to_full[list.column];
      if (full_col >= store.column_has_null.size()) {
        return false;
      }
      if (list.int_values.empty()) {
        always_false = true;
        continue;
      }
      IntInList compiled;
      compiled.full_col = full_col;
      compiled.values = list.int_values;
      compiled.lo = compiled.values[0];
      compiled.hi = compiled.values[0];
      for (const int64_t v : compiled.values) {
        compiled.lo = std::min(compiled.lo, v);
        compiled.hi = std::max(compiled.hi, v);
      }
      const uint64_t span = static_cast<uint64_t>(compiled.hi) -
                            static_cast<uint64_t>(compiled.lo);
      if (span < 64ULL) {
        compiled.use_mask = true;
        compiled.mask = 0;
        for (const int64_t v : compiled.values) {
          compiled.mask |=
              (uint64_t{1} << (static_cast<uint64_t>(v) -
                               static_cast<uint64_t>(compiled.lo)));
        }
      } else if (compiled.values.size() > 16) {
        return false;
      }
      int_inlists.push_back(std::move(compiled));
      referenced_full_cols[full_col] = 1;
    }
    return true;
  }

  struct Scratch {
    struct ActiveBranchSlice {
      uint32_t pred_begin{0};
      uint32_t pred_end{0};
      bool double_range{false};
      slot_t range_col{0};
      double lo{0};
      double hi{0};
    };
    struct ActiveDisjSlice {
      uint32_t br_begin{0};
      uint32_t br_end{0};
    };
    std::vector<DirectBlockCol> cols;
    std::vector<uint8_t> col_bound;
    std::vector<const NullPred*> active_nulls;
    std::vector<const IntInterval*> active_intervals;
    std::vector<const IntInList*> active_inlists;
    std::vector<const CompiledComparePred*> active_stage1;
    std::vector<const CompiledComparePred*> flat_disj_preds;
    std::vector<ActiveBranchSlice> flat_disj_branches;
    std::vector<ActiveDisjSlice> active_disjs;
  };

  static void InitScratch(size_t num_store_cols, Scratch* ws) {
    ws->cols.resize(num_store_cols);
    ws->col_bound.assign(num_store_cols, 0);
  }

  uint32_t FilterBlock(const LSTablePageBlock& blk,
                       const std::vector<uint8_t>& always_bind_cols,
                       Scratch* ws, bool* all_pass,
                       std::vector<uint32_t>* sel) const {
    if (always_false || blk.row_count == 0) {
      *all_pass = false;
      return 0;
    }
    ws->active_nulls.clear();
    for (const NullPred& nf : null_preds) {
      const LSTableColumnBlock& col = blk.columns[nf.full_col];
      if (!col.has_null) {
        if (nf.is_null) {
          *all_pass = false;
          return 0;
        }
        continue;
      }
      ws->active_nulls.push_back(&nf);
    }
    ws->active_intervals.clear();
    for (const IntInterval& iv : int_intervals) {
      const LSTableColumnBlock& col = blk.columns[iv.full_col];
      if (col.has_min_max) {
        if (col.max_int < iv.lo || col.min_int > iv.hi) {
          *all_pass = false;
          return 0;
        }
        if (iv.lo == iv.hi && !col.BlockBloomMayContain(iv.lo)) {
          *all_pass = false;
          return 0;
        }
        if (!col.has_null && col.min_int >= iv.lo && col.max_int <= iv.hi) {
          continue;
        }
      }
      ws->active_intervals.push_back(&iv);
    }
    ws->active_inlists.clear();
    for (const IntInList& il : int_inlists) {
      const LSTableColumnBlock& col = blk.columns[il.full_col];
      if (col.has_min_max) {
        if (col.max_int < il.lo || col.min_int > il.hi) {
          *all_pass = false;
          return 0;
        }
        if (il.values.size() <= 16) {
          bool any_may_match = false;
          for (const int64_t v : il.values) {
            if (v >= col.min_int && v <= col.max_int &&
                col.BlockBloomMayContain(v)) {
              any_may_match = true;
              break;
            }
          }
          if (!any_may_match) {
            *all_pass = false;
            return 0;
          }
        }
      }
      ws->active_inlists.push_back(&il);
    }
    ws->active_stage1.clear();
    for (const CompiledComparePred& pred : stage1_preds) {
      const BlockFilterStatus st = CheckCompareBlockStatus(pred, blk);
      if (st == BlockFilterStatus::kAllFail) {
        *all_pass = false;
        return 0;
      }
      if (st == BlockFilterStatus::kMixed) {
        ws->active_stage1.push_back(&pred);
      }
    }
    ws->flat_disj_preds.clear();
    ws->flat_disj_branches.clear();
    ws->active_disjs.clear();
    for (const Disjunction& disj : disjunctions) {
      bool disj_all_pass = false;
      const auto br_start =
          static_cast<uint32_t>(ws->flat_disj_branches.size());
      const auto pred_saved = static_cast<uint32_t>(ws->flat_disj_preds.size());
      for (const Branch& branch : disj.branches) {
        bool branch_fail = false;
        const auto p_begin = static_cast<uint32_t>(ws->flat_disj_preds.size());
        for (const CompiledComparePred& pred : branch.preds) {
          const BlockFilterStatus st = CheckCompareBlockStatus(pred, blk);
          if (st == BlockFilterStatus::kAllFail) {
            branch_fail = true;
            break;
          }
          if (st == BlockFilterStatus::kMixed) {
            ws->flat_disj_preds.push_back(&pred);
          }
        }
        if (branch_fail) {
          ws->flat_disj_preds.resize(p_begin);
          continue;
        }
        const auto p_end = static_cast<uint32_t>(ws->flat_disj_preds.size());
        if (p_begin == p_end) {
          ws->flat_disj_preds.resize(pred_saved);
          ws->flat_disj_branches.resize(br_start);
          disj_all_pass = true;
          break;
        }
        // NOLINTNEXTLINE(modernize-use-designated-initializers) positional init
        // is clear
        Scratch::ActiveBranchSlice slice{
            // NOLINT(modernize-use-designated-initializers)
            // NOLINT(modernize-use-designated-initializers)
            .pred_begin = p_begin,
            .pred_end =
                p_end};  // NOLINT(modernize-use-designated-initializers)
        // BETWEEN branches otherwise dispatch two generic comparisons and
        // load the same cell/null flag twice for each surviving row.
        if (p_end == p_begin + 2) {
          const auto* lower = ws->flat_disj_preds[p_begin];
          const auto* upper = ws->flat_disj_preds[p_begin + 1];
          if (lower->cmp.op == BinaryOperation::kLessThanEquals) {
            std::swap(lower, upper);
          }
          if (lower->is_double && upper->is_double &&
              lower->cmp.constant_payload && upper->cmp.constant_payload &&
              lower->full_left == upper->full_left &&
              lower->cmp.op == BinaryOperation::kGreaterThanEquals &&
              upper->cmp.op == BinaryOperation::kLessThanEquals) {
            slice.double_range = true;
            slice.range_col = lower->full_left;
            slice.lo = lower->cmp.double_constant;
            slice.hi = upper->cmp.double_constant;
          }
        }
        ws->flat_disj_branches.push_back(slice);
      }
      if (disj_all_pass) {
        continue;
      }
      const auto br_end = static_cast<uint32_t>(ws->flat_disj_branches.size());
      if (br_start == br_end) {
        *all_pass = false;
        return 0;
      }
      ws->active_disjs.push_back({br_start, br_end});
    }

    const uint32_t n = blk.row_count;
    const auto ensure_col_bound = [&](slot_t c) {
      if (ws->col_bound[c] == 0) {
        ws->cols[c].Bind(blk.columns[c], n);
        ws->col_bound[c] = 1;
      }
    };
    std::ranges::fill(ws->col_bound, 0);
    for (size_t c = 0; c < always_bind_cols.size(); ++c) {
      if (always_bind_cols[c] != 0) {
        ensure_col_bound(static_cast<slot_t>(c));
      }
    }
    if (ws->active_nulls.empty() && ws->active_intervals.empty() &&
        ws->active_inlists.empty() && ws->active_stage1.empty() &&
        ws->active_disjs.empty()) {
      *all_pass = true;
      return n;
    }

    for (const NullPred* nf : ws->active_nulls) {
      ensure_col_bound(nf->full_col);
    }
    for (const IntInterval* iv : ws->active_intervals) {
      ensure_col_bound(iv->full_col);
    }
    for (const IntInList* il : ws->active_inlists) {
      ensure_col_bound(il->full_col);
    }
    for (const CompiledComparePred* pred : ws->active_stage1) {
      ensure_col_bound(pred->full_left);
      if (!pred->cmp.constant_payload) {
        ensure_col_bound(pred->full_right);
      }
    }
    for (const CompiledComparePred* pred : ws->flat_disj_preds) {
      ensure_col_bound(pred->full_left);
      if (!pred->cmp.constant_payload) {
        ensure_col_bound(pred->full_right);
      }
    }

    *all_pass = false;
    sel->resize(n);
    uint32_t m = 0;
    bool initialized_sel = false;

    if (!ws->active_nulls.empty()) {
      const NullPred* nf0 = ws->active_nulls[0];
      const DirectBlockCol& col = ws->cols[nf0->full_col];
      const bool target_null = nf0->is_null;
      for (uint32_t r = 0; r < n; ++r) {
        if (col.IsNull(r) == target_null) {
          (*sel)[m++] = r;
        }
      }
      initialized_sel = true;
      for (size_t ni = 1; ni < ws->active_nulls.size() && m > 0; ++ni) {
        const NullPred* nf = ws->active_nulls[ni];
        const DirectBlockCol& ncol = ws->cols[nf->full_col];
        const bool tn = nf->is_null;
        uint32_t out_m = 0;
        for (uint32_t idx = 0; idx < m; ++idx) {
          const uint32_t r = (*sel)[idx];
          if (ncol.IsNull(r) == tn) {
            (*sel)[out_m++] = r;
          }
        }
        m = out_m;
      }
      if (m == 0) {
        return 0;
      }
    }

    for (const IntInList* il : ws->active_inlists) {
      const DirectBlockCol& col = ws->cols[il->full_col];
      const uint32_t count = initialized_sel ? m : n;
      uint32_t out_m = 0;
      if (il->use_mask) {
        const auto lo_u = static_cast<uint64_t>(il->lo);
        const uint64_t mask = il->mask;
        if (!initialized_sel) {
          if (!col.has_null && col.cells != nullptr) {
            const int64_t* __restrict cells = col.cells;
            for (uint32_t r = 0; r < count; ++r) {
              const uint64_t d = static_cast<uint64_t>(cells[r]) - lo_u;
              const uint32_t hit =
                  static_cast<uint32_t>(d < 64ULL) &
                  static_cast<uint32_t>(((mask >> (d & 63ULL)) & 1ULL) != 0);
              (*sel)[out_m] = r;
              out_m += hit;
            }
          } else {
            for (uint32_t r = 0; r < count; ++r) {
              if (col.IsNull(r)) {
                continue;
              }
              const uint64_t d = static_cast<uint64_t>(col.Int(r)) - lo_u;
              const uint32_t hit =
                  static_cast<uint32_t>(d < 64ULL) &
                  static_cast<uint32_t>(((mask >> (d & 63ULL)) & 1ULL) != 0);
              (*sel)[out_m] = r;
              out_m += hit;
            }
          }
        } else if (!col.has_null) {
          for (uint32_t idx = 0; idx < count; ++idx) {
            const uint32_t r = (*sel)[idx];
            const uint64_t d = static_cast<uint64_t>(col.Int(r)) - lo_u;
            const uint32_t hit =
                static_cast<uint32_t>(d < 64ULL) &
                static_cast<uint32_t>(((mask >> (d & 63ULL)) & 1ULL) != 0);
            (*sel)[out_m] = r;
            out_m += hit;
          }
        } else {
          for (uint32_t idx = 0; idx < count; ++idx) {
            const uint32_t r = (*sel)[idx];
            if (col.IsNull(r)) {
              continue;
            }
            const uint64_t d = static_cast<uint64_t>(col.Int(r)) - lo_u;
            const uint32_t hit =
                static_cast<uint32_t>(d < 64ULL) &
                static_cast<uint32_t>(((mask >> (d & 63ULL)) & 1ULL) != 0);
            (*sel)[out_m] = r;
            out_m += hit;
          }
        }
      } else {
        const auto match_val = [&](int64_t v) -> bool {
          if (v < il->lo || v > il->hi) {
            return false;
          }
          return std::ranges::any_of(
              il->values, [v](const int64_t item) { return v == item; });
        };
        for (uint32_t idx = 0; idx < count; ++idx) {
          const uint32_t r = initialized_sel ? (*sel)[idx] : idx;
          if ((!col.has_null || !col.IsNull(r)) && match_val(col.Int(r))) {
            (*sel)[out_m++] = r;
          }
        }
      }
      m = out_m;
      initialized_sel = true;
      if (m == 0) {
        return 0;
      }
    }

    for (const IntInterval* iv : ws->active_intervals) {
      const DirectBlockCol& col = ws->cols[iv->full_col];
      // Unsigned subtraction represents the inclusive signed interval even
      // when it crosses zero or touches either INT64 endpoint. Compact the
      // selection without a data-dependent branch for every input row.
      const auto lo = static_cast<uint64_t>(iv->lo);
      const uint64_t width = static_cast<uint64_t>(iv->hi) - lo;
      const uint32_t count = initialized_sel ? m : n;
      uint32_t out_m = 0;
      const auto select = [&]<bool selected, bool nullable>() {
        if constexpr (!selected) {
          out_m = platform::SelectInt64ClosedRange(col.cells, count, iv->lo,
                                                   iv->hi, sel->data());
          if constexpr (nullable) {
            uint32_t nonnull = 0;
            for (uint32_t i = 0; i < out_m; ++i) {
              const uint32_t row = (*sel)[i];
              if (!col.IsNull(row)) {
                (*sel)[nonnull++] = row;
              }
            }
            out_m = nonnull;
          }
          return;
        }
        for (uint32_t idx = 0; idx < count; ++idx) {
          const uint32_t r = selected ? (*sel)[idx] : idx;
          const bool in_range = static_cast<uint64_t>(col.Int(r)) - lo <= width;
          bool matches = in_range;
          if constexpr (nullable) {
            matches &= !col.IsNull(r);
          }
          (*sel)[out_m] = r;
          out_m += static_cast<uint32_t>(matches);
        }
      };
      if (initialized_sel) {
        if (col.has_null) {
          select.template operator()<true, true>();
        } else {
          select.template operator()<true, false>();
        }
      } else if (col.has_null) {
        select.template operator()<false, true>();
      } else {
        select.template operator()<false, false>();
      }
      m = out_m;
      initialized_sel = true;
      if (m == 0) {
        return 0;
      }
    }

    for (size_t predicate = 0; predicate < ws->active_stage1.size();
         ++predicate) {
      const CompiledComparePred* pred = ws->active_stage1[predicate];
      if (pred->is_double && pred->cmp.constant_payload) {
        const auto& column = ws->cols[pred->full_left];
        const double constant = pred->cmp.double_constant;
        const uint32_t count = initialized_sel ? m : n;
        const auto select_compare = [&](auto compare) {
          const auto run = [&]<bool selected, bool nullable>() {
            uint32_t out = 0;
            for (uint32_t i = 0; i < count; ++i) {
              const uint32_t row = selected ? (*sel)[i] : i;
              bool hit = compare(column.Double(row), constant);
              if constexpr (nullable) {
                hit &= !column.IsNull(row);
              }
              (*sel)[out] = row;
              out += static_cast<uint32_t>(hit);
            }
            m = out;
          };
          if (initialized_sel) {
            if (column.has_null) {
              run.template operator()<true, true>();
            } else {
              run.template operator()<true, false>();
            }
          } else {
            if (column.has_null) {
              run.template operator()<false, true>();
            } else {
              run.template operator()<false, false>();
            }
          }
        };
        const CompiledComparePred* paired =
            predicate + 1 < ws->active_stage1.size()
                ? ws->active_stage1[predicate + 1]
                : nullptr;
        if ((paired != nullptr) && paired->is_double &&
            paired->cmp.constant_payload &&
            paired->full_left == pred->full_left &&
            ((pred->cmp.op == BinaryOperation::kGreaterThanEquals &&
              paired->cmp.op == BinaryOperation::kLessThanEquals) ||
             (pred->cmp.op == BinaryOperation::kLessThanEquals &&
              paired->cmp.op == BinaryOperation::kGreaterThanEquals))) {
          const bool lower_first =
              pred->cmp.op == BinaryOperation::kGreaterThanEquals;
          const double lower =
              lower_first ? constant : paired->cmp.double_constant;
          const double upper =
              lower_first ? paired->cmp.double_constant : constant;
          if (initialized_sel && !column.has_null) {
            m = platform::CompactDoubleRange(column.cells, sel->data(), count,
                                             lower, upper, true, true);
          } else {
            select_compare([&](double value, double) {
              return (value >= lower) & (value <= upper);
            });
          }
          ++predicate;
        } else {
          switch (pred->cmp.op) {
            case BinaryOperation::kEquals:
              select_compare(std::equal_to<double>{});
              break;
            case BinaryOperation::kNotEquals:
              select_compare(std::not_equal_to<double>{});
              break;
            case BinaryOperation::kLessThan:
              if (initialized_sel && !column.has_null) {
                m = platform::CompactDoubleRange(
                    column.cells, sel->data(), count,
                    -std::numeric_limits<double>::infinity(), constant, true,
                    false);
              } else {
                select_compare(std::less<double>{});
              }
              break;
            case BinaryOperation::kLessThanEquals:
              select_compare(std::less_equal<double>{});
              break;
            case BinaryOperation::kGreaterThan:
              select_compare(std::greater<double>{});
              break;
            case BinaryOperation::kGreaterThanEquals:
              select_compare(std::greater_equal<double>{});
              break;
            default:
              return 0;
          }
        }
        initialized_sel = true;
        if (m == 0) {
          return 0;
        }
        continue;
      }
      if (!initialized_sel) {
        for (uint32_t r = 0; r < n; ++r) {
          if (EvalCompiledCompareRow(*pred, ws->cols, r)) {
            (*sel)[m++] = r;
          }
        }
        initialized_sel = true;
      } else {
        uint32_t out_m = 0;
        for (uint32_t idx = 0; idx < m; ++idx) {
          const uint32_t r = (*sel)[idx];
          if (EvalCompiledCompareRow(*pred, ws->cols, r)) {
            (*sel)[out_m++] = r;
          }
        }
        m = out_m;
      }
      if (m == 0) {
        return 0;
      }
    }

    for (const Scratch::ActiveDisjSlice& dslice : ws->active_disjs) {
      const auto eval_disj = [&](uint32_t r) noexcept -> bool {
        for (uint32_t bi = dslice.br_begin; bi < dslice.br_end; ++bi) {
          const Scratch::ActiveBranchSlice& br = ws->flat_disj_branches[bi];
          if (br.double_range) {
            const DirectBlockCol& col = ws->cols[br.range_col];
            const double v = col.Double(r);
            if (!col.IsNull(r) && v >= br.lo && v <= br.hi) {
              return true;
            }
            continue;
          }
          bool ok = true;
          for (uint32_t pi = br.pred_begin; pi < br.pred_end; ++pi) {
            if (!EvalCompiledCompareRow(*ws->flat_disj_preds[pi], ws->cols,
                                        r)) {
              ok = false;
              break;
            }
          }
          if (ok) {
            return true;
          }
        }
        return false;
      };
      if (!initialized_sel) {
        for (uint32_t r = 0; r < n; ++r) {
          if (eval_disj(r)) {
            (*sel)[m++] = r;
          }
        }
        initialized_sel = true;
      } else {
        uint32_t out_m = 0;
        for (uint32_t idx = 0; idx < m; ++idx) {
          const uint32_t r = (*sel)[idx];
          if (eval_disj(r)) {
            (*sel)[out_m++] = r;
          }
        }
        m = out_m;
      }
      if (m == 0) {
        return 0;
      }
    }
    return m;
  }
};

struct DirectAggBinding {
  SlimAggKind kind{SlimAggKind::kNone};
  bool is_count_star{false};
  std::optional<slot_t> full_col;
  ValueType col_type{ValueType::kNull};
  const TypedDoubleExprInput* double_expr{nullptr};
  const TypedIntExprInput* int_expr{nullptr};
};

inline bool CollectDirectAggBindings(
    const TypedAggregationPlan& plan, const LSTableStore& store,
    const std::vector<slot_t>& proj_to_full,
    const std::vector<ValueType>& proj_types,
    const std::vector<uint8_t>& proj_unsigned,
    std::vector<DirectAggBinding>* out_bindings,
    std::vector<uint8_t>* agg_bind_cols) {
  out_bindings->clear();
  out_bindings->reserve(plan.aggregates.size());
  agg_bind_cols->assign(store.column_has_null.size(), 0);
  for (size_t i = 0; i < plan.aggregates.size(); ++i) {
    const TypedAggregateInput& input = plan.aggregates[i];
    const SlimAggKind kind = plan.slim_kinds[i];
    if (input.child.has_value() || input.conditional.has_value() ||
        input.coalesce_double.has_value() || kind == SlimAggKind::kNone) {
      return false;
    }
    DirectAggBinding b;
    b.kind = kind;
    if (kind == SlimAggKind::kCount && IsCountStar(*input.aggregate)) {
      b.is_count_star = true;
      out_bindings->push_back(b);
      continue;
    }
    if (input.column.has_value()) {
      if (*input.column >= proj_types.size()) {
        return false;
      }
      const slot_t fc = proj_to_full[*input.column];
      if (fc >= store.column_has_null.size()) {
        return false;
      }
      const ValueType vt = proj_types[*input.column];
      const bool is_unsigned = proj_unsigned[*input.column] != 0;
      switch (kind) {
        case SlimAggKind::kCount:
          break;
        case SlimAggKind::kSumInt64:
        case SlimAggKind::kMinInt64:
        case SlimAggKind::kMaxInt64:
          if (vt != ValueType::kInt64 || is_unsigned) {
            return false;
          }
          break;
        case SlimAggKind::kMinDate:
        case SlimAggKind::kMaxDate:
          if (vt != ValueType::kDate) {
            return false;
          }
          break;
        case SlimAggKind::kSumDouble:
        case SlimAggKind::kMinDouble:
        case SlimAggKind::kMaxDouble:
        case SlimAggKind::kCountDistinctDouble:
          if (vt != ValueType::kDouble) {
            return false;
          }
          break;
        case SlimAggKind::kAvgDouble:
          if (vt != ValueType::kDouble &&
              (vt != ValueType::kInt64 || is_unsigned)) {
            return false;
          }
          break;
        case SlimAggKind::kCountDistinctInt64:
          if ((vt != ValueType::kInt64 && vt != ValueType::kDate) ||
              is_unsigned) {
            return false;
          }
          break;
        default:
          return false;
      }
      b.full_col = fc;
      b.col_type = vt;
      (*agg_bind_cols)[fc] = 1;
      out_bindings->push_back(b);
      continue;
    }
    if (input.double_expr.has_value()) {
      if (kind != SlimAggKind::kCount && kind != SlimAggKind::kSumDouble &&
          kind != SlimAggKind::kAvgDouble && kind != SlimAggKind::kMinDouble &&
          kind != SlimAggKind::kMaxDouble) {
        return false;
      }
      const TypedDoubleExprInput& de = *input.double_expr;
      const auto valid_dcol = [&](slot_t c) -> bool {
        if (c >= proj_types.size() || proj_types[c] != ValueType::kDouble) {
          return false;
        }
        const slot_t fc = proj_to_full[c];
        if (fc >= store.column_has_null.size()) {
          return false;
        }
        (*agg_bind_cols)[fc] = 1;
        return true;
      };
      switch (de.shape) {
        case TypedDoubleExprInput::Shape::kSingleDoubleCol:
          if (!valid_dcol(de.col0)) {
            return false;
          }
          break;
        case TypedDoubleExprInput::Shape::kMulTwoDoubleCols:
        case TypedDoubleExprInput::Shape::kMulConstMinusCol:
          if (!valid_dcol(de.col0) || !valid_dcol(de.col1)) {
            return false;
          }
          break;
        case TypedDoubleExprInput::Shape::kMulConstMinusColMulConstPlusCol:
          if (!valid_dcol(de.col0) || !valid_dcol(de.col1) ||
              !valid_dcol(de.col2)) {
            return false;
          }
          break;
        case TypedDoubleExprInput::Shape::kGeneric:
          for (const TypedDoubleExprOp& op : de.ops) {
            if (op.kind == TypedDoubleExprOp::Kind::kLoadDoubleCol) {
              if (!valid_dcol(op.column)) {
                return false;
              }
            } else if (op.kind ==
                       TypedDoubleExprOp::Kind::kLoadIntColAsDouble) {
              if (op.column >= proj_types.size() ||
                  proj_types[op.column] != ValueType::kInt64 ||
                  proj_unsigned[op.column] != 0) {
                return false;
              }
              const slot_t fc = proj_to_full[op.column];
              if (fc >= store.column_has_null.size()) {
                return false;
              }
              (*agg_bind_cols)[fc] = 1;
            }
          }
          break;
      }
      b.double_expr = &de;
      out_bindings->push_back(b);
      continue;
    }
    if (input.int_expr.has_value()) {
      if (kind != SlimAggKind::kCount && kind != SlimAggKind::kSumInt64 &&
          kind != SlimAggKind::kAvgDouble && kind != SlimAggKind::kMinInt64 &&
          kind != SlimAggKind::kMaxInt64) {
        return false;
      }
      const TypedIntExprInput& ie = *input.int_expr;
      for (const TypedIntExprOp& op : ie.ops) {
        if (op.kind == TypedIntExprOp::Kind::kLoadIntCol) {
          if (op.column >= proj_types.size() ||
              proj_types[op.column] != ValueType::kInt64 ||
              proj_unsigned[op.column] != 0) {
            return false;
          }
          const slot_t fc = proj_to_full[op.column];
          if (fc >= store.column_has_null.size()) {
            return false;
          }
          (*agg_bind_cols)[fc] = 1;
        }
      }
      b.int_expr = &ie;
      out_bindings->push_back(b);
      continue;
    }
    return false;
  }
  return true;
}

inline bool EvalDirectDoubleExpr(const TypedDoubleExprInput& expr,
                                 const std::vector<slot_t>& proj_to_full,
                                 const std::vector<DirectBlockCol>& cols,
                                 uint32_t r, double* out) noexcept {
  switch (expr.shape) {
    case TypedDoubleExprInput::Shape::kSingleDoubleCol: {
      const DirectBlockCol& c0 = cols[proj_to_full[expr.col0]];
      if (c0.IsNull(r)) {
        return false;
      }
      *out = c0.Double(r);
      return true;
    }
    case TypedDoubleExprInput::Shape::kMulTwoDoubleCols: {
      const DirectBlockCol& c0 = cols[proj_to_full[expr.col0]];
      const DirectBlockCol& c1 = cols[proj_to_full[expr.col1]];
      if (c0.IsNull(r) || c1.IsNull(r)) {
        return false;
      }
      *out = c0.Double(r) * c1.Double(r);
      return true;
    }
    case TypedDoubleExprInput::Shape::kMulConstMinusCol: {
      const DirectBlockCol& c0 = cols[proj_to_full[expr.col0]];
      const DirectBlockCol& c1 = cols[proj_to_full[expr.col1]];
      if (c0.IsNull(r) || c1.IsNull(r)) {
        return false;
      }
      *out = c0.Double(r) * (expr.k0 - c1.Double(r));
      return true;
    }
    case TypedDoubleExprInput::Shape::kMulConstMinusColMulConstPlusCol: {
      const DirectBlockCol& c0 = cols[proj_to_full[expr.col0]];
      const DirectBlockCol& c1 = cols[proj_to_full[expr.col1]];
      const DirectBlockCol& c2 = cols[proj_to_full[expr.col2]];
      if (c0.IsNull(r) || c1.IsNull(r) || c2.IsNull(r)) {
        return false;
      }
      *out = c0.Double(r) * (expr.k0 - c1.Double(r)) * (expr.k1 + c2.Double(r));
      return true;
    }
    case TypedDoubleExprInput::Shape::kGeneric:
      break;
  }
  std::array<double, 16> stack{};
  size_t sp = 0;
  // Producers cap ops at 16 entries so pushes can never exceed the stack;
  // keep that bound explicit here instead of relying on the upstream
  // compiler invariant.
  if (expr.ops.size() > stack.size()) {
    return false;
  }
  for (const TypedDoubleExprOp& op : expr.ops) {
    switch (op.kind) {
      case TypedDoubleExprOp::Kind::kLoadDoubleCol: {
        const DirectBlockCol& c = cols[proj_to_full[op.column]];
        if (c.IsNull(r)) {
          return false;
        }
        stack[sp++] = c.Double(r);
        break;
      }
      case TypedDoubleExprOp::Kind::kLoadIntColAsDouble: {
        const DirectBlockCol& c = cols[proj_to_full[op.column]];
        if (c.IsNull(r)) {
          return false;
        }
        stack[sp++] = static_cast<double>(c.Int(r));
        break;
      }
      case TypedDoubleExprOp::Kind::kConstant:
        stack[sp++] = op.constant;
        break;
      case TypedDoubleExprOp::Kind::kAdd:
        stack[sp - 2] += stack[sp - 1];
        --sp;
        break;
      case TypedDoubleExprOp::Kind::kSub:
        stack[sp - 2] -= stack[sp - 1];
        --sp;
        break;
      case TypedDoubleExprOp::Kind::kMul:
        stack[sp - 2] *= stack[sp - 1];
        --sp;
        break;
    }
  }
  *out = stack[0];
  return true;
}

inline bool EvalDirectIntExpr(const TypedIntExprInput& expr,
                              const std::vector<slot_t>& proj_to_full,
                              const std::vector<DirectBlockCol>& cols,
                              uint32_t r, int64_t* out, Status* err) {
  std::array<int64_t, 16> stack{};
  size_t sp = 0;
  // Producers cap ops at 16 entries so pushes can never exceed the stack;
  // keep that bound explicit here instead of relying on the upstream
  // compiler invariant.
  if (expr.ops.size() > stack.size()) {
    *err = StatusError(StatusCode::kInvalidArgument,
                       "typed INT expression operand stack overflow");
    return false;
  }
  for (const TypedIntExprOp& op : expr.ops) {
    switch (op.kind) {
      case TypedIntExprOp::Kind::kLoadIntCol: {
        const DirectBlockCol& c = cols[proj_to_full[op.column]];
        if (c.IsNull(r)) {
          return false;
        }
        stack[sp++] = c.Int(r);
        break;
      }
      case TypedIntExprOp::Kind::kConstant:
        stack[sp++] = op.constant;
        break;
      case TypedIntExprOp::Kind::kAdd: {
        int64_t v = 0;
        if (__builtin_add_overflow(stack[sp - 2], stack[sp - 1], &v)) {
          *err = StatusError(StatusCode::kInvalidArgument,
                             "integer overflow on '+'");
          return false;
        }
        stack[sp - 2] = v;
        --sp;
        break;
      }
      case TypedIntExprOp::Kind::kSub: {
        int64_t v = 0;
        if (__builtin_sub_overflow(stack[sp - 2], stack[sp - 1], &v)) {
          *err = StatusError(StatusCode::kInvalidArgument,
                             "integer overflow on '-'");
          return false;
        }
        stack[sp - 2] = v;
        --sp;
        break;
      }
      case TypedIntExprOp::Kind::kMul: {
        int64_t v = 0;
        if (__builtin_mul_overflow(stack[sp - 2], stack[sp - 1], &v)) {
          *err = StatusError(StatusCode::kInvalidArgument,
                             "integer overflow on '*'");
          return false;
        }
        stack[sp - 2] = v;
        --sp;
        break;
      }
    }
  }
  *out = stack[0];
  return true;
}

inline Value FinishDirectState(SlimAggKind kind, int64_t cnt, int64_t itot,
                               double dtot, uint8_t flags, size_t distinct_cnt,
                               Status* error) {
  switch (kind) {
    case SlimAggKind::kCount:
      return Value(cnt);
    case SlimAggKind::kSumInt64:
      return cnt == 0 ? Value() : Value(itot);
    case SlimAggKind::kSumDouble:
      if (cnt == 0) {
        return {};
      }
      if ((flags & 1) == 0 && std::isinf(dtot)) {
        *error = StatusError(StatusCode::kIsInfinity,
                             "SUM overflow: result is infinite");
        return {};
      }
      return Value(dtot);
    case SlimAggKind::kAvgDouble:
      return cnt == 0 ? Value() : Value(dtot / static_cast<double>(cnt));
    case SlimAggKind::kCountDistinctInt64:
    case SlimAggKind::kCountDistinctDouble:
      if (distinct_cnt >
          static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
        *error = StatusError(StatusCode::kIsInfinity,
                             "COUNT(DISTINCT) overflow: result exceeds INT64");
        return {};
      }
      return Value(static_cast<int64_t>(distinct_cnt));
    case SlimAggKind::kMinInt64:
    case SlimAggKind::kMaxInt64:
      return cnt == 0 ? Value() : Value(itot);
    case SlimAggKind::kMinDate:
    case SlimAggKind::kMaxDate:
      return cnt == 0 ? Value() : Value::DateFromDays(itot);
    case SlimAggKind::kMinDouble:
    case SlimAggKind::kMaxDouble:
      if (cnt == 0) {
        return {};
      }
      if ((flags & 1) != 0) {
        return Value(std::numeric_limits<double>::quiet_NaN());
      }
      return Value(dtot);
    default:
      *error = StatusError(StatusCode::kInvalidArgument,
                           "unsupported direct slim aggregate");
      return {};
  }
}

inline uint64_t MixScalarHash64(uint64_t z) noexcept {
  z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
  z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
  z ^= z >> 31;
  return z == 0 ? 1ULL : z;
}

inline uint64_t MixStringHash64(std::string_view sv) noexcept {
  return MixScalarHash64(std::hash<std::string_view>{}(sv));
}

StatusOr<std::optional<Relation>> BuildScalarDirectOutputChecked(
    const TypedAggregationPlan& plan, const SelectStatement& stmt,
    const Schema& qualified_schema, const Schema& table_schema,
    const std::vector<Value>& finished_values, size_t total_scanned,
    size_t total_matching, size_t total_updates, bool publish_lstable_marker,
    const std::string& fold_cache_key,
    std::chrono::steady_clock::time_point scan_begin,
    std::chrono::steady_clock::time_point merge_begin,
    ExecutionRuntime* runtime) {
  if (publish_lstable_marker && runtime != nullptr) {
    auto marker = std::make_shared<Relation>(runtime);
    marker->schema = qualified_schema;
    runtime->base_relations.emplace(fold_cache_key, std::move(marker));
  }

  std::vector<Value> row_values;
  row_values.reserve(plan.emit_items.size());
  for (const TypedEmitItem& item : plan.emit_items) {
    if (const auto* post_agg_ptr = item.post_agg.get()) {
      const auto& post_agg = *post_agg_ptr;
      std::vector<Value> post_agg_cells;
      post_agg_cells.reserve(post_agg.slots.size());
      bool types_match_schema = post_agg_ptr->program.has_value();
      for (size_t si = 0; si < post_agg.slots.size(); ++si) {
        const auto& slot = post_agg.slots[si];
        if (slot.is_key || slot.index >= finished_values.size()) {
          return std::optional<Relation>{};
        }
        Value arg = finished_values[slot.index];
        if (types_match_schema && !arg.IsNull()) {
          const ValueType expected = post_agg.eval_schema.GetColumn(si).Type();
          if (expected != ValueType::kNull && arg.type != expected) {
            types_match_schema = false;
          }
        }
        post_agg_cells.push_back(std::move(arg));
      }
      const Row eval_row(std::move(post_agg_cells));
      if (types_match_schema && post_agg_ptr->program.has_value()) {
        ASSIGN_OR_RETURN(Value, evaluated,
                         TryEvaluateRowAuto(*post_agg_ptr->program, eval_row));
        row_values.push_back(std::move(evaluated));
      } else {
        ASSIGN_OR_RETURN(
            Value, evaluated,
            post_agg.expr->TryEvaluate(eval_row, post_agg.eval_schema));
        row_values.push_back(std::move(evaluated));
      }
      continue;
    }
    row_values.push_back(item.constant ? *item.constant
                                       : finished_values[item.index]);
  }
  std::vector<Column> output_columns;
  output_columns.reserve(stmt.SelectList().size());
  for (size_t i = 0; i < stmt.SelectList().size(); ++i) {
    Column col(
        ProjectionName(stmt.SelectList()[i], i),
        row_values[i].IsNull()
            ? ValueTypeFromTypeTag(stmt.SelectList()[i]
                                       .expression->ResultType(qualified_schema)
                                       .GetType())
            : ValueTypeOf(row_values[i]));
    col.SetUnsigned(row_values[i].IsUnsigned());
    output_columns.push_back(std::move(col));
  }
  Relation output(runtime);
  output.AddRow(Row(std::move(row_values)));
  output.schema = Schema("", std::move(output_columns));

  if (runtime != nullptr) {
    const double scan_ms = ElapsedMs(scan_begin);
    runtime->scan_ms += scan_ms;
    runtime->filter_ms += scan_ms;
    runtime->scan_rows += total_scanned;
    runtime->scan_output_rows += total_matching;
    runtime->scan_values_decoded +=
        total_scanned * qualified_schema.ColumnCount();
    runtime->scan_values_available +=
        total_scanned * table_schema.ColumnCount();
    runtime->typed_parallel_aggregations += 1;
    runtime->aggregate_groups += 1;
    runtime->aggregate_input_rows += total_matching;
    runtime->aggregate_updates += total_updates;
    runtime->aggregate_ms += ElapsedMs(merge_begin);
  }
  return std::optional<Relation>(std::move(output));
}

Relation BuildScalarDirectOutput(
    const TypedAggregationPlan& plan, const SelectStatement& stmt,
    const Schema& qualified_schema, const Schema& table_schema,
    const std::vector<Value>& finished_values, size_t total_scanned,
    size_t total_matching, size_t total_updates, bool publish_lstable_marker,
    const std::string& fold_cache_key,
    std::chrono::steady_clock::time_point scan_begin,
    std::chrono::steady_clock::time_point merge_begin,
    ExecutionRuntime* runtime) {
  auto checked = BuildScalarDirectOutputChecked(
      plan, stmt, qualified_schema, table_schema, finished_values,
      total_scanned, total_matching, total_updates, publish_lstable_marker,
      fold_cache_key, scan_begin, merge_begin, runtime);
  // Every caller pre-checks that the fast-path plan has no post_agg emit item,
  // so Checked must return a successful StatusOr holding a Relation. An empty
  // optional means that plan invariant was broken; fail loudly at the boundary
  // instead of dereferencing it (which is UB and would be reported as success).
  CHECK_MSG(checked.HasValue(),
            "BuildScalarDirectOutput: direct output check failed");
  std::optional<Relation> output = checked.MoveValue();
  CHECK_MSG(output.has_value(),
            "BuildScalarDirectOutput: unsupported post_agg in fast-path plan");
  return std::move(*output);
}

// Two-pass sharded parallel scalar COUNT(DISTINCT col) over LSTableStore.
// Supports both numeric/DATE columns and zero-copy VarChar slices without
// allocating per-row Strings or doing single-threaded full-table map merges.
StatusOr<std::optional<Relation>> TryRunParallelScalarCountDistinct(
    const LSTableStore& store, const TypedAggregationPlan& plan,
    const SelectStatement& stmt, const Expression& having_expr,
    const std::vector<slot_t>& proj_to_full,
    const std::vector<ValueType>& proj_types,
    const std::vector<uint8_t>& proj_unsigned, const Schema& qualified_schema,
    const Schema& table_schema, bool direct_children,
    const RuntimeFilterGroup* stream_runtime_filter,
    const PrebuiltMembershipPredicate* stream_prebuilt_membership,
    size_t worker_count, ExecutionRuntime* runtime, bool publish_lstable_marker,
    const std::string& fold_cache_key,
    std::chrono::steady_clock::time_point scan_begin) {
  if (!direct_children || stream_runtime_filter != nullptr ||
      stream_prebuilt_membership != nullptr ||
      (!plan.filter_programs.empty() && !plan.only_null_filter_programs) ||
      !(stmt.GroupBy().empty() || stmt.IsGroupAll()) ||
      !plan.key_offsets.empty() || having_expr != nullptr || stmt.HasLimit() ||
      stmt.Distinct() || stmt.WithTies() || !plan.order_items.empty() ||
      !plan.dependent_key_programs.empty() || !plan.inlist_compares.empty() ||
      plan.aggregates.size() != 1 || plan.emit_items.size() != 1 ||
      store.total_rows < 1024) {
    return std::optional<Relation>{};
  }
  const TypedEmitItem& emit_item = plan.emit_items[0];
  if (emit_item.is_key || emit_item.dependent_key ||
      emit_item.post_agg != nullptr || emit_item.constant ||
      emit_item.index != 0) {
    return std::optional<Relation>{};
  }
  const TypedAggregateInput& input = plan.aggregates[0];
  if (input.aggregate == nullptr ||
      input.aggregate->GetType() != AggregationType::kCount ||
      !input.aggregate->Distinct() || input.aggregate->HasWhereFilter() ||
      input.aggregate->NeedsGroupContext() || input.child.has_value() ||
      input.conditional.has_value() || input.double_expr.has_value() ||
      input.coalesce_double.has_value() || input.int_expr.has_value() ||
      !input.column.has_value() || *input.column >= proj_types.size()) {
    return std::optional<Relation>{};
  }
  const slot_t proj_col = *input.column;
  const slot_t full_col = proj_to_full[proj_col];
  if (full_col >= store.column_has_null.size() ||
      proj_unsigned[proj_col] != 0) {
    return std::optional<Relation>{};
  }
  const ValueType vt = proj_types[proj_col];
  if (vt != ValueType::kInt64 && vt != ValueType::kDate &&
      vt != ValueType::kDouble && vt != ValueType::kVarChar) {
    return std::optional<Relation>{};
  }
  if (vt == ValueType::kVarChar &&
      (full_col >= table_schema.ColumnCount() ||
       table_schema.GetColumn(full_col).IsInterval())) {
    return std::optional<Relation>{};
  }

  CompiledLSTableBlockFilter filter;
  if (!filter.Init(plan, proj_to_full, proj_types, proj_unsigned, store)) {
    return std::optional<Relation>{};
  }
  if (filter.always_false) {
    const auto merge_begin = std::chrono::steady_clock::now();
    return std::optional<Relation>(BuildScalarDirectOutput(
        plan, stmt, qualified_schema, table_schema, {Value(int64_t{0})}, 0, 0,
        0, publish_lstable_marker, fold_cache_key, scan_begin, merge_begin,
        runtime));
  }

  const bool no_predicates =
      filter.null_preds.empty() && filter.int_intervals.empty() &&
      filter.int_inlists.empty() && filter.stage1_preds.empty() &&
      filter.disjunctions.empty();
  if (no_predicates && full_col < store.dictionaries.size()) {
    const LSTableColumnDictionary& dict = store.dictionaries[full_col];
    const size_t entry_count =
        vt == ValueType::kVarChar
            ? dict.StringEntryCount()
            : (vt == ValueType::kDouble ? dict.DoubleEntryCount()
                                        : dict.IntEntryCount());
    if (dict.active && dict.code_counts.size() == entry_count + 1 &&
        (vt != ValueType::kVarChar || dict.bytewise_equality)) {
      int64_t non_null_distinct = 0;
      for (size_t c = 0; c < entry_count; ++c) {
        if (dict.code_counts[c] > 0) {
          ++non_null_distinct;
        }
      }
      const auto merge_begin = std::chrono::steady_clock::now();
      return std::optional<Relation>(BuildScalarDirectOutput(
          plan, stmt, qualified_schema, table_schema,
          {Value(non_null_distinct)}, store.total_rows, store.total_rows,
          store.total_rows, publish_lstable_marker, fold_cache_key, scan_begin,
          merge_begin, runtime));
    }
  }

  std::vector<uint8_t> agg_bind_cols(store.column_has_null.size(), 0);
  agg_bind_cols[full_col] = 1;
  const size_t nblocks = store.blocks.size();
  const size_t workers_used =
      std::max<size_t>(1, std::min(worker_count, std::max<size_t>(1, nblocks)));
  constexpr size_t kShards = 64;
  constexpr size_t kShardMask = kShards - 1;

  std::atomic<bool> budget_exceeded{false};
  std::vector<QueryMemoryCharge> worker_charges(workers_used);
  std::vector<size_t> worker_scanned(workers_used, 0);
  std::vector<size_t> worker_matching(workers_used, 0);

  if (vt == ValueType::kVarChar) {
    struct StrSlice {
      const char* ptr{nullptr};
      uint32_t len{0};
      uint32_t h32{0};
    };
    struct StrCacheSlot {
      uint64_t h64{0};
      const char* ptr{nullptr};
      uint32_t len{0};
    };
    constexpr size_t kCacheSlots = 4096;
    constexpr size_t kCacheMask = kCacheSlots - 1;
    std::vector<std::array<std::vector<StrSlice>, kShards>> worker_shards(
        workers_used);
    std::vector<uint8_t> worker_saw_empty(workers_used, 0);

    RETURN_IF_FAIL(RunParallelTasks(workers_used, [&](size_t w) {
      if (budget_exceeded.load(std::memory_order_relaxed)) {
        return;
      }
      QueryMemoryCharge& qcharge = worker_charges[w];
      if (!qcharge.TryAdd(kCacheSlots * sizeof(StrCacheSlot))) {
        budget_exceeded.store(true, std::memory_order_relaxed);
        return;
      }
      std::vector<StrCacheSlot> cache(kCacheSlots);
      size_t charged_shard_bytes = 0;
      auto& shards = worker_shards[w];

      CompiledLSTableBlockFilter::Scratch ws;
      filter.InitScratch(store.column_has_null.size(), &ws);
      std::vector<uint32_t> sel;
      const size_t blk_begin = (nblocks * w) / workers_used;
      const size_t blk_end = (nblocks * (w + 1)) / workers_used;
      bool saw_empty = false;
      size_t scanned = 0;
      size_t matching = 0;

      auto sync_charge = [&]() -> bool {
        size_t cur_cap = 0;
        for (const auto& s : shards) {
          cur_cap += s.capacity() * sizeof(StrSlice);
        }
        if (cur_cap > charged_shard_bytes) {
          if (!qcharge.TryAdd(cur_cap - charged_shard_bytes)) {
            budget_exceeded.store(true, std::memory_order_relaxed);
            return false;
          }
          charged_shard_bytes = cur_cap;
        }
        return true;
      };

      auto push_sv = [&](std::string_view sv) -> bool {
        if (sv.empty()) {
          saw_empty = true;
          return true;
        }
        if (sv.size() > std::numeric_limits<uint32_t>::max()) {
          budget_exceeded.store(true, std::memory_order_relaxed);
          return false;
        }
        const uint64_t h64 = MixStringHash64(sv);
        StrCacheSlot& cs = cache[static_cast<size_t>(h64) & kCacheMask];
        if (cs.ptr != nullptr && cs.h64 == h64 && cs.len == sv.size() &&
            std::memcmp(cs.ptr, sv.data(), sv.size()) == 0) {
          return true;
        }
        cs = StrCacheSlot{h64, sv.data(), static_cast<uint32_t>(sv.size())};
        auto h32 = static_cast<uint32_t>((h64 >> 6) ^ (h64 >> 32));
        if (h32 == 0) {
          h32 = 1;
        }
        auto& shard_vec = shards[static_cast<size_t>(h64) & kShardMask];
        const size_t old_cap = shard_vec.capacity();
        shard_vec.push_back(
            StrSlice{sv.data(), static_cast<uint32_t>(sv.size()), h32});
        return shard_vec.capacity() == old_cap || sync_charge();
      };

      for (size_t bi = blk_begin; bi < blk_end; ++bi) {
        if (budget_exceeded.load(std::memory_order_relaxed)) {
          return;
        }
        const auto& blk = store.blocks[bi];
        scanned += blk.row_count;
        bool all_pass = false;
        const uint32_t m =
            filter.FilterBlock(blk, agg_bind_cols, &ws, &all_pass, &sel);
        if (m == 0) {
          continue;
        }
        matching += m;
        const DirectBlockCol& col = ws.cols[full_col];
        const uint32_t* sel_ptr = sel.data();
        if (!col.has_null) {
          if (all_pass) {
            for (uint32_t r = 0; r < m; ++r) {
              if (!push_sv(col.VarChar(r))) {
                return;
              }
            }
          } else {
            for (uint32_t idx = 0; idx < m; ++idx) {
              if (!push_sv(col.VarChar(sel_ptr[idx]))) {
                return;
              }
            }
          }
        } else {
          if (all_pass) {
            for (uint32_t r = 0; r < m; ++r) {
              if (!col.IsNull(r) && !push_sv(col.VarChar(r))) {
                return;
              }
            }
          } else {
            for (uint32_t idx = 0; idx < m; ++idx) {
              const uint32_t r = sel_ptr[idx];
              if (!col.IsNull(r) && !push_sv(col.VarChar(r))) {
                return;
              }
            }
          }
        }
      }
      worker_saw_empty[w] = saw_empty ? 1 : 0;
      worker_scanned[w] = scanned;
      worker_matching[w] = matching;
    }));
    if (budget_exceeded.load(std::memory_order_acquire)) {
      return std::optional<Relation>{};
    }

    const auto merge_begin = std::chrono::steady_clock::now();
    std::array<size_t, kShards> shard_distinct{};
    std::atomic<size_t> next_shard{0};
    RETURN_IF_FAIL(RunParallelTasks(workers_used, [&](size_t w) {
      std::vector<StrSlice> table;
      size_t charged_table_cap = 0;
      while (true) {
        if (budget_exceeded.load(std::memory_order_relaxed)) {
          return;
        }
        const size_t s = next_shard.fetch_add(1, std::memory_order_relaxed);
        if (s >= kShards) {
          return;
        }
        size_t total_in_shard = 0;
        for (size_t src = 0; src < workers_used; ++src) {
          total_in_shard += worker_shards[src][s].size();
        }
        if (total_in_shard == 0) {
          shard_distinct[s] = 0;
          continue;
        }
        size_t cap = 16;
        const size_t min_cap = total_in_shard * 2;
        while (cap < min_cap) {
          cap *= 2;
        }
        if (cap > charged_table_cap) {
          if (!worker_charges[w].TryAdd((cap - charged_table_cap) *
                                        sizeof(StrSlice))) {
            budget_exceeded.store(true, std::memory_order_relaxed);
            return;
          }
          table.resize(cap);
          charged_table_cap = cap;
        }
        std::fill_n(table.begin(), cap, StrSlice{});
        const size_t mask = cap - 1;
        size_t unique_count = 0;
        for (size_t src = 0; src < workers_used; ++src) {
          for (const StrSlice& item : worker_shards[src][s]) {
            size_t slot = static_cast<size_t>(MixScalarHash64(item.h32)) & mask;
            bool dup = false;
            while (table[slot].ptr != nullptr) {
              if (table[slot].h32 == item.h32 && table[slot].len == item.len &&
                  std::memcmp(table[slot].ptr, item.ptr, item.len) == 0) {
                dup = true;
                break;
              }
              slot = (slot + 1) & mask;
            }
            if (!dup) {
              table[slot] = item;
              ++unique_count;
            }
          }
          std::vector<StrSlice>().swap(worker_shards[src][s]);
        }
        shard_distinct[s] = unique_count;
      }
    }));
    if (budget_exceeded.load(std::memory_order_acquire)) {
      return std::optional<Relation>{};
    }

    int64_t total_distinct = 0;
    for (size_t s = 0; s < kShards; ++s) {
      total_distinct += static_cast<int64_t>(shard_distinct[s]);
    }
    for (uint8_t seen : worker_saw_empty) {
      if (seen != 0) {
        ++total_distinct;
        break;
      }
    }
    const size_t total_scanned = std::accumulate(
        worker_scanned.begin(), worker_scanned.end(), size_t{0});
    const size_t total_matching = std::accumulate(
        worker_matching.begin(), worker_matching.end(), size_t{0});
    return std::optional<Relation>(BuildScalarDirectOutput(
        plan, stmt, qualified_schema, table_schema, {Value(total_distinct)},
        total_scanned, total_matching, total_matching, publish_lstable_marker,
        fold_cache_key, scan_begin, merge_begin, runtime));
  }

  // Numeric (Int64 / Date / Double) 64-shard parallel COUNT(DISTINCT col).
  const bool is_dbl = (vt == ValueType::kDouble);
  struct IntCacheSlot {
    uint64_t h64{0};
    int64_t key{0};
  };
  constexpr size_t kIntCacheSlots = 16384;
  constexpr size_t kIntCacheMask = kIntCacheSlots - 1;
  std::vector<std::array<std::vector<int64_t>, kShards>> worker_shards(
      workers_used);

  RETURN_IF_FAIL(RunParallelTasks(workers_used, [&](size_t w) {
    if (budget_exceeded.load(std::memory_order_relaxed)) {
      return;
    }
    QueryMemoryCharge& qcharge = worker_charges[w];
    if (!qcharge.TryAdd(kIntCacheSlots * sizeof(IntCacheSlot))) {
      budget_exceeded.store(true, std::memory_order_relaxed);
      return;
    }
    std::vector<IntCacheSlot> cache(kIntCacheSlots);
    size_t charged_shard_bytes = 0;
    auto& shards = worker_shards[w];

    CompiledLSTableBlockFilter::Scratch ws;
    filter.InitScratch(store.column_has_null.size(), &ws);
    std::vector<uint32_t> sel;
    const size_t blk_begin = (nblocks * w) / workers_used;
    const size_t blk_end = (nblocks * (w + 1)) / workers_used;
    size_t scanned = 0;
    size_t matching = 0;

    auto sync_charge = [&]() -> bool {
      size_t cur_cap = 0;
      for (const auto& s : shards) {
        cur_cap += s.capacity() * sizeof(int64_t);
      }
      if (cur_cap > charged_shard_bytes) {
        if (!qcharge.TryAdd(cur_cap - charged_shard_bytes)) {
          budget_exceeded.store(true, std::memory_order_relaxed);
          return false;
        }
        charged_shard_bytes = cur_cap;
      }
      return true;
    };

    auto push_key = [&](int64_t key) -> bool {
      const uint64_t h64 = MixScalarHash64(static_cast<uint64_t>(key));
      IntCacheSlot& cs = cache[static_cast<size_t>(h64) & kIntCacheMask];
      if (cs.h64 == h64 && cs.key == key) {
        return true;
      }
      cs = IntCacheSlot{h64, key};
      auto& shard_vec = shards[static_cast<size_t>(h64) & kShardMask];
      const size_t old_cap = shard_vec.capacity();
      shard_vec.push_back(key);
      return shard_vec.capacity() == old_cap || sync_charge();
    };

    for (size_t bi = blk_begin; bi < blk_end; ++bi) {
      if (budget_exceeded.load(std::memory_order_relaxed)) {
        return;
      }
      const auto& blk = store.blocks[bi];
      scanned += blk.row_count;
      bool all_pass = false;
      const uint32_t m =
          filter.FilterBlock(blk, agg_bind_cols, &ws, &all_pass, &sel);
      if (m == 0) {
        continue;
      }
      matching += m;
      const DirectBlockCol& col = ws.cols[full_col];
      const uint32_t* sel_ptr = sel.data();
      if (!col.has_null) {
        if (all_pass) {
          for (uint32_t r = 0; r < m; ++r) {
            const int64_t k = is_dbl
                                  ? CanonicalDistinctDoubleBits(col.Double(r))
                                  : col.Int(r);
            if (!push_key(k)) {
              return;
            }
          }
        } else {
          for (uint32_t idx = 0; idx < m; ++idx) {
            const uint32_t r = sel_ptr[idx];
            const int64_t k = is_dbl
                                  ? CanonicalDistinctDoubleBits(col.Double(r))
                                  : col.Int(r);
            if (!push_key(k)) {
              return;
            }
          }
        }
      } else {
        if (all_pass) {
          for (uint32_t r = 0; r < m; ++r) {
            if (!col.IsNull(r)) {
              const int64_t k = is_dbl
                                    ? CanonicalDistinctDoubleBits(col.Double(r))
                                    : col.Int(r);
              if (!push_key(k)) {
                return;
              }
            }
          }
        } else {
          for (uint32_t idx = 0; idx < m; ++idx) {
            const uint32_t r = sel_ptr[idx];
            if (!col.IsNull(r)) {
              const int64_t k = is_dbl
                                    ? CanonicalDistinctDoubleBits(col.Double(r))
                                    : col.Int(r);
              if (!push_key(k)) {
                return;
              }
            }
          }
        }
      }
    }
    worker_scanned[w] = scanned;
    worker_matching[w] = matching;
  }));
  if (budget_exceeded.load(std::memory_order_acquire)) {
    return std::optional<Relation>{};
  }

  const auto merge_begin = std::chrono::steady_clock::now();
  std::array<size_t, kShards> shard_distinct{};
  std::atomic<size_t> next_shard{0};
  RETURN_IF_FAIL(RunParallelTasks(workers_used, [&](size_t w) {
    FlatIntKeyMap<uint8_t> dmap;
    size_t charged_cap = 0;
    while (true) {
      if (budget_exceeded.load(std::memory_order_relaxed)) {
        return;
      }
      const size_t s = next_shard.fetch_add(1, std::memory_order_relaxed);
      if (s >= kShards) {
        return;
      }
      size_t total_in_shard = 0;
      for (size_t src = 0; src < workers_used; ++src) {
        total_in_shard += worker_shards[src][s].size();
      }
      if (total_in_shard == 0) {
        shard_distinct[s] = 0;
        continue;
      }
      const size_t min_cap = (total_in_shard * 10 + 6) / 7;
      size_t next_cap = 16;
      while (next_cap < min_cap) {
        next_cap *= 2;
      }
      if (next_cap > charged_cap) {
        if (!worker_charges[w].TryAdd((next_cap - charged_cap) * 16)) {
          budget_exceeded.store(true, std::memory_order_relaxed);
          return;
        }
        charged_cap = next_cap;
      }
      dmap.Clear();
      dmap.Reserve(total_in_shard);
      for (size_t src = 0; src < workers_used; ++src) {
        for (const int64_t key : worker_shards[src][s]) {
          bool inserted = false;
          uint8_t* slot = dmap.TryEmplace(key, &inserted);
          if (inserted) {
            *slot = 1;
          }
        }
        std::vector<int64_t>().swap(worker_shards[src][s]);
      }
      shard_distinct[s] = dmap.Size();
    }
  }));
  if (budget_exceeded.load(std::memory_order_acquire)) {
    return std::optional<Relation>{};
  }

  int64_t total_distinct = 0;
  for (size_t s = 0; s < kShards; ++s) {
    total_distinct += static_cast<int64_t>(shard_distinct[s]);
  }
  const size_t total_scanned =
      std::accumulate(worker_scanned.begin(), worker_scanned.end(), size_t{0});
  const size_t total_matching = std::accumulate(
      worker_matching.begin(), worker_matching.end(), size_t{0});
  return std::optional<Relation>(BuildScalarDirectOutput(
      plan, stmt, qualified_schema, table_schema, {Value(total_distinct)},
      total_scanned, total_matching, total_matching, publish_lstable_marker,
      fold_cache_key, scan_begin, merge_begin, runtime));
}

// O(blocks) metadata fast path for unfiltered scalar MIN/MAX(Int64/Date)
// and COUNT(*) when every non-empty block carries valid min/max bounds.
std::optional<std::vector<Value>> TryScalarBlockMetaMinMax(
    const LSTableStore& store, const CompiledLSTableBlockFilter& filter,
    const std::vector<DirectAggBinding>& bindings) {
  if (filter.always_false || !filter.null_preds.empty() ||
      !filter.int_intervals.empty() || !filter.int_inlists.empty() ||
      !filter.stage1_preds.empty() || !filter.disjunctions.empty()) {
    return std::nullopt;
  }
  const size_t naggs = bindings.size();
  for (const DirectAggBinding& b : bindings) {
    if (b.is_count_star) {
      continue;
    }
    if (b.int_expr != nullptr || b.double_expr != nullptr ||
        !b.full_col.has_value() ||
        (b.kind != SlimAggKind::kMinInt64 && b.kind != SlimAggKind::kMaxInt64 &&
         b.kind != SlimAggKind::kMinDate && b.kind != SlimAggKind::kMaxDate)) {
      return std::nullopt;
    }
  }
  for (const LSTablePageBlock& blk : store.blocks) {
    if (blk.row_count == 0) {
      continue;
    }
    for (const DirectAggBinding& b : bindings) {
      if (b.is_count_star) {
        continue;
      }
      const std::optional<slot_t> full_col = b.full_col;
      if (!full_col.has_value() || !blk.columns[*full_col].has_min_max) {
        return std::nullopt;
      }
    }
  }
  std::vector<int64_t> counts(naggs, 0);
  std::vector<int64_t> itot(naggs, 0);
  for (const LSTablePageBlock& blk : store.blocks) {
    if (blk.row_count == 0) {
      continue;
    }
    for (size_t a = 0; a < naggs; ++a) {
      const DirectAggBinding& b = bindings[a];
      if (b.is_count_star) {
        counts[a] += static_cast<int64_t>(blk.row_count);
        continue;
      }
      const std::optional<slot_t> full_col = b.full_col;
      if (!full_col.has_value()) {
        return std::nullopt;
      }
      const LSTableColumnBlock& col = blk.columns[*full_col];
      const bool is_min =
          (b.kind == SlimAggKind::kMinInt64 || b.kind == SlimAggKind::kMinDate);
      const int64_t bv = is_min ? col.min_int : col.max_int;
      if (counts[a] == 0 || (is_min ? bv < itot[a] : itot[a] < bv)) {
        itot[a] = bv;
      }
      counts[a] += static_cast<int64_t>(blk.row_count);
    }
  }
  std::vector<Value> finished(naggs);
  for (size_t a = 0; a < naggs; ++a) {
    Status err = Status::kSuccess;
    finished[a] = FinishDirectState(bindings[a].kind, counts[a], itot[a], 0.0,
                                    0, 0, &err);
    if (!err.ok()) {
      return std::nullopt;
    }
  }
  return finished;
}

// Lean proof: lean/TinylambOpt/CountBoundsAndBlockAggregates.lean
//   (dict_exact_freq_filtered_count_sound, filtered_extremum_bound_unsound_bug)
// O(|D|) exact frequency fold for single-column (filtered) or multi-column
// (unfiltered marginal) dictionary-encoded scalar aggregates.
StatusOr<std::optional<std::vector<Value>>> TryScalarNarrowDictShortcut(
    const LSTableStore& store, const Schema& table_schema,
    const CompiledLSTableBlockFilter& filter,
    const std::vector<DirectAggBinding>& bindings, size_t* matching_rows_out) {
  if (filter.always_false || !filter.disjunctions.empty() ||
      !filter.int_inlists.empty()) {
    return std::optional<std::vector<Value>>{};
  }
  constexpr size_t kMaxShortcutDictEntries = 262'144;
  const bool unfiltered = filter.null_preds.empty() &&
                          filter.int_intervals.empty() &&
                          filter.stage1_preds.empty();
  if (unfiltered) {
    for (const DirectAggBinding& b : bindings) {
      if (b.is_count_star) {
        continue;
      }
      if (b.int_expr != nullptr || b.double_expr != nullptr ||
          !b.full_col.has_value()) {
        return std::optional<std::vector<Value>>{};
      }
      const slot_t c = *b.full_col;
      if (c >= store.dictionaries.size() || c >= table_schema.ColumnCount() ||
          table_schema.GetColumn(c).IsUnsigned()) {
        return std::optional<std::vector<Value>>{};
      }
      const LSTableColumnDictionary& dict = store.dictionaries[c];
      const ValueType vt = dict.value_type;
      if (!dict.active && dict.wide_code_bytes == 0) {
        return std::optional<std::vector<Value>>{};
      }
      if (vt != ValueType::kInt64 && vt != ValueType::kDate &&
          vt != ValueType::kVarChar) {
        return std::optional<std::vector<Value>>{};
      }
      if (vt == ValueType::kVarChar &&
          (!dict.bytewise_equality || table_schema.GetColumn(c).IsInterval() ||
           b.kind != SlimAggKind::kCount)) {
        return std::optional<std::vector<Value>>{};
      }
      const size_t ec = vt == ValueType::kVarChar ? dict.StringEntryCount()
                                                  : dict.IntEntryCount();
      if (ec > kMaxShortcutDictEntries || dict.code_counts.size() != ec + 1) {
        return std::optional<std::vector<Value>>{};
      }
      switch (b.kind) {
        case SlimAggKind::kCount:
        case SlimAggKind::kSumInt64:
        case SlimAggKind::kAvgDouble:
        case SlimAggKind::kCountDistinctInt64:
        case SlimAggKind::kMinInt64:
        case SlimAggKind::kMaxInt64:
        case SlimAggKind::kMinDate:
        case SlimAggKind::kMaxDate:
          break;
        default:
          return std::optional<std::vector<Value>>{};
      }
    }

    std::vector<Value> finished(bindings.size());
    for (size_t a = 0; a < bindings.size(); ++a) {
      const DirectAggBinding& b = bindings[a];
      if (b.is_count_star) {
        finished[a] = Value(static_cast<int64_t>(store.total_rows));
        continue;
      }
      const slot_t c = *b.full_col;
      const LSTableColumnDictionary& dict = store.dictionaries[c];
      const ValueType vt = dict.value_type;
      const size_t ec = vt == ValueType::kVarChar ? dict.StringEntryCount()
                                                  : dict.IntEntryCount();
      int64_t cnt_acc = 0;
      int64_t itot = 0;
      double dtot = 0.0;
      size_t distinct_cnt = 0;
      platform::Int128 sum_pos = 0;
      platform::Int128 sum_neg = 0;
      platform::Int128 sum_run = 0;
      // P596: exact Σ|value| when this binding is an AVG (see the arm).
      platform::Int128 dtot_abs = 0;
      for (size_t code = 0; code < ec; ++code) {
        const int64_t cnt = dict.code_counts[code];
        if (cnt <= 0) {
          continue;
        }
        if (vt == ValueType::kVarChar) {
          cnt_acc += cnt;
          continue;
        }
        const int64_t iv = dict.IntEntryAt(code);
        switch (b.kind) {
          case SlimAggKind::kCount:
            cnt_acc += cnt;
            break;
          case SlimAggKind::kSumInt64: {
            const platform::Int128 prod =
                static_cast<platform::Int128>(iv) * cnt;
            sum_run += prod;
            if (prod < 0) {
              sum_neg += prod;
            } else {
              sum_pos += prod;
            }
            cnt_acc += cnt;
            break;
          }
          case SlimAggKind::kAvgDouble: {
            // P596: the multiply re-associates the canonical row-ordered
            // double fold; only exact while Σ|v| <= 2^53 (checked below).
            const platform::Int128 prod =
                static_cast<platform::Int128>(iv) * cnt;
            dtot_abs += prod < 0 ? -prod : prod;
            dtot += static_cast<double>(iv) * static_cast<double>(cnt);
            cnt_acc += cnt;
            break;
          }
          case SlimAggKind::kCountDistinctInt64:
            ++distinct_cnt;
            break;
          case SlimAggKind::kMinInt64:
          case SlimAggKind::kMinDate:
            if (cnt_acc == 0 || iv < itot) {
              itot = iv;
            }
            cnt_acc += cnt;
            break;
          case SlimAggKind::kMaxInt64:
          case SlimAggKind::kMaxDate:
            if (cnt_acc == 0 || itot < iv) {
              itot = iv;
            }
            cnt_acc += cnt;
            break;
          default:
            return std::optional<std::vector<Value>>{};
        }
      }
      if (b.kind == SlimAggKind::kSumInt64) {
        if (sum_pos > std::numeric_limits<int64_t>::max() ||
            sum_neg < std::numeric_limits<int64_t>::min()) {
          return std::optional<std::vector<Value>>{};
        }
        itot = static_cast<int64_t>(sum_run);
      }
      if (b.kind == SlimAggKind::kAvgDouble &&
          dtot_abs > static_cast<platform::Int128>(INT64_C(1) << 53)) {
        return std::optional<std::vector<Value>>{};
      }
      Status err = Status::kSuccess;
      finished[a] =
          FinishDirectState(b.kind, cnt_acc, itot, dtot, 0, distinct_cnt, &err);
      if (!err.ok()) {
        return err;
      }
    }
    *matching_rows_out = store.total_rows;
    return std::optional<std::vector<Value>>(std::move(finished));
  }

  std::optional<slot_t> target_col;
  for (const auto& np : filter.null_preds) {
    if (target_col && *target_col != np.full_col) {
      return std::optional<std::vector<Value>>{};
    }
    target_col = np.full_col;
  }
  for (const auto& iv : filter.int_intervals) {
    if (target_col && *target_col != iv.full_col) {
      return std::optional<std::vector<Value>>{};
    }
    target_col = iv.full_col;
  }
  for (const auto& pred : filter.stage1_preds) {
    if (!pred.cmp.constant_payload) {
      return std::optional<std::vector<Value>>{};
    }
    if (target_col && *target_col != pred.full_left) {
      return std::optional<std::vector<Value>>{};
    }
    target_col = pred.full_left;
  }
  for (const DirectAggBinding& b : bindings) {
    if (b.is_count_star) {
      continue;
    }
    if (b.int_expr != nullptr || b.double_expr != nullptr ||
        !b.full_col.has_value()) {
      return std::optional<std::vector<Value>>{};
    }
    if (target_col && *target_col != *b.full_col) {
      return std::optional<std::vector<Value>>{};
    }
    target_col = b.full_col;
    switch (b.kind) {
      case SlimAggKind::kCount:
      case SlimAggKind::kSumInt64:
      case SlimAggKind::kAvgDouble:
      case SlimAggKind::kCountDistinctInt64:
      case SlimAggKind::kMinInt64:
      case SlimAggKind::kMaxInt64:
      case SlimAggKind::kMinDate:
      case SlimAggKind::kMaxDate:
        break;
      default:
        return std::optional<std::vector<Value>>{};
    }
  }
  if (!target_col.has_value() || *target_col >= store.dictionaries.size() ||
      *target_col >= table_schema.ColumnCount() ||
      table_schema.GetColumn(*target_col).IsUnsigned()) {
    return std::optional<std::vector<Value>>{};
  }
  const slot_t c = *target_col;
  const LSTableColumnDictionary& dict = store.dictionaries[c];
  const ValueType vt = dict.value_type;
  if (!dict.active && dict.wide_code_bytes == 0) {
    return std::optional<std::vector<Value>>{};
  }
  if (vt != ValueType::kInt64 && vt != ValueType::kDate &&
      vt != ValueType::kVarChar) {
    return std::optional<std::vector<Value>>{};
  }
  if (vt == ValueType::kVarChar &&
      (!dict.bytewise_equality || table_schema.GetColumn(c).IsInterval())) {
    return std::optional<std::vector<Value>>{};
  }
  const size_t entry_count = vt == ValueType::kVarChar ? dict.StringEntryCount()
                                                       : dict.IntEntryCount();
  if (dict.code_counts.size() != entry_count + 1) {
    return std::optional<std::vector<Value>>{};
  }
  if (entry_count > kMaxShortcutDictEntries) {
    return std::optional<std::vector<Value>>{};
  }

  const size_t naggs = bindings.size();
  std::vector<int64_t> counts(naggs, 0);
  std::vector<int64_t> itot(naggs, 0);
  std::vector<double> dtot(naggs, 0.0);
  std::vector<size_t> distinct_cnts(naggs, 0);
  std::vector<platform::Int128> sum_pos(naggs, 0);
  std::vector<platform::Int128> sum_neg(naggs, 0);
  std::vector<platform::Int128> sum_run(naggs, 0);
  // P596: exact Σ|value| per AVG binding (see the kAvgDouble arm below).
  std::vector<platform::Int128> dtot_abs(naggs, 0);
  size_t total_matching = 0;

  for (size_t code = 0; code <= entry_count; ++code) {
    const int64_t cnt = dict.code_counts[code];
    if (cnt <= 0) {
      continue;
    }
    const bool is_null = (code == entry_count);
    bool pass = true;
    for (const auto& np : filter.null_preds) {
      if (is_null != np.is_null) {
        pass = false;
        break;
      }
    }
    if (!pass) {
      continue;
    }
    if (is_null) {
      if (!filter.int_intervals.empty() || !filter.stage1_preds.empty()) {
        continue;
      }
      total_matching += static_cast<size_t>(cnt);
      for (size_t a = 0; a < naggs; ++a) {
        if (bindings[a].is_count_star) {
          counts[a] += cnt;
        }
      }
      continue;
    }

    if (vt == ValueType::kVarChar) {
      const std::string_view sv = dict.StringEntryAt(code);
      for (const auto& pred : filter.stage1_preds) {
        if (!MatchTypedStringCompare(sv, pred.cmp)) {
          pass = false;
          break;
        }
      }
      if (!pass) {
        continue;
      }
      total_matching += static_cast<size_t>(cnt);
      for (size_t a = 0; a < naggs; ++a) {
        if (bindings[a].is_count_star ||
            bindings[a].kind == SlimAggKind::kCount) {
          counts[a] += cnt;
        }
      }
      continue;
    }

    const int64_t iv = dict.IntEntryAt(code);
    for (const auto& iv_pred : filter.int_intervals) {
      if (iv < iv_pred.lo || iv > iv_pred.hi) {
        pass = false;
        break;
      }
    }
    if (!pass) {
      continue;
    }
    for (const auto& pred : filter.stage1_preds) {
      if (!CompareDirectScalar(pred.cmp.op, iv, pred.cmp.int_constant)) {
        pass = false;
        break;
      }
    }
    if (!pass) {
      continue;
    }
    total_matching += static_cast<size_t>(cnt);
    for (size_t a = 0; a < naggs; ++a) {
      const SlimAggKind kind = bindings[a].kind;
      if (bindings[a].is_count_star || kind == SlimAggKind::kCount) {
        counts[a] += cnt;
      } else if (kind == SlimAggKind::kSumInt64) {
        // Row-ordered evaluation raises "integer overflow in SUM" the moment
        // a *partial* sum leaves int64, and that outcome depends on scan
        // order. Every prefix sums a subset of the observed values, so it is
        // bounded by [sum_neg, sum_pos] regardless of order; this
        // order-independent shortcut may answer only when both extremes fit.
        const platform::Int128 prod = static_cast<platform::Int128>(iv) * cnt;
        sum_run[a] += prod;
        if (prod < 0) {
          sum_neg[a] += prod;
        } else {
          sum_pos[a] += prod;
        }
        counts[a] += cnt;
      } else if (kind == SlimAggKind::kAvgDouble) {
        // P596: the canonical AVG adds double(v) row-by-row in scan order;
        // the multiply here re-associates that accumulation, so the same SQL
        // differs by ULPs purely with plan shape.  While the exact integer
        // Σ|v| stays within 2^53 every intermediate is an exactly
        // representable double and any accumulation order is bit-identical;
        // beyond that defer to the row-ordered path (SUM arm's precedent).
        const platform::Int128 prod = static_cast<platform::Int128>(iv) * cnt;
        dtot_abs[a] += prod < 0 ? -prod : prod;
        dtot[a] += static_cast<double>(iv) * static_cast<double>(cnt);
        counts[a] += cnt;
      } else if (kind == SlimAggKind::kCountDistinctInt64) {
        ++distinct_cnts[a];
      } else if (kind == SlimAggKind::kMinInt64 ||
                 kind == SlimAggKind::kMinDate) {
        if (counts[a] == 0 || iv < itot[a]) {
          itot[a] = iv;
        }
        counts[a] += cnt;
      } else if (kind == SlimAggKind::kMaxInt64 ||
                 kind == SlimAggKind::kMaxDate) {
        if (counts[a] == 0 || itot[a] < iv) {
          itot[a] = iv;
        }
        counts[a] += cnt;
      }
    }
  }

  for (size_t a = 0; a < naggs; ++a) {
    if (bindings[a].kind != SlimAggKind::kSumInt64) {
      continue;
    }
    if (sum_pos[a] > std::numeric_limits<int64_t>::max() ||
        sum_neg[a] < std::numeric_limits<int64_t>::min()) {
      // A row-order partial sum could escape int64 somewhere; whether the
      // ordered evaluation errors depends on the unseen scan order, so defer
      // to it instead of diverging from ground truth.
      return std::optional<std::vector<Value>>{};
    }
    itot[a] = static_cast<int64_t>(sum_run[a]);
  }
  for (size_t a = 0; a < naggs; ++a) {
    if (bindings[a].kind == SlimAggKind::kAvgDouble &&
        dtot_abs[a] > static_cast<platform::Int128>(INT64_C(1) << 53)) {
      // Re-association could move the double sum; the row-ordered fold is
      // the ground truth (invariant 3), so let it answer instead.
      return std::optional<std::vector<Value>>{};
    }
  }
  std::vector<Value> finished(naggs);
  for (size_t a = 0; a < naggs; ++a) {
    Status err = Status::kSuccess;
    finished[a] = FinishDirectState(bindings[a].kind, counts[a], itot[a],
                                    dtot[a], 0, distinct_cnts[a], &err);
    if (!err.ok()) {
      return err;
    }
  }
  *matching_rows_out = total_matching;
  return std::optional<std::vector<Value>>(std::move(finished));
}

// Fuses multiple `SUM(col + K)` / `SUM(col - K)` / `SUM(col)` scalar aggs
// over identical base integer columns when overflow is provably impossible.
StatusOr<std::optional<Relation>> TryRunFusedLinearIntSums(
    const LSTableStore& store, const TypedAggregationPlan& plan,
    const SelectStatement& stmt, const CompiledLSTableBlockFilter& filter,
    const std::vector<DirectAggBinding>& bindings,
    const std::vector<slot_t>& proj_to_full,
    const std::vector<ValueType>& proj_types,
    const std::vector<uint8_t>& proj_unsigned, const Schema& qualified_schema,
    const Schema& table_schema, size_t worker_count, ExecutionRuntime* runtime,
    bool publish_lstable_marker, const std::string& fold_cache_key,
    std::chrono::steady_clock::time_point scan_begin) {
  if (bindings.size() < 2 || filter.always_false || store.total_rows == 0) {
    return std::optional<Relation>{};
  }
  struct Term {
    size_t agg_idx{0};
    slot_t full_col{0};
    int64_t delta{0};
  };
  struct ColGroup {
    slot_t full_col{0};
    int64_t min_delta{0};
    int64_t max_delta{0};
  };
  std::vector<Term> terms;
  terms.reserve(bindings.size());
  std::vector<ColGroup> groups;
  groups.reserve(bindings.size());
  std::vector<int> col_to_group(store.column_has_null.size(), -1);
  std::vector<uint8_t> agg_bind_cols(store.column_has_null.size(), 0);

  for (size_t a = 0; a < bindings.size(); ++a) {
    const DirectAggBinding& b = bindings[a];
    if (b.kind != SlimAggKind::kSumInt64 || b.double_expr != nullptr) {
      return std::optional<Relation>{};
    }
    slot_t full_col = 0;
    int64_t delta = 0;
    if (b.full_col.has_value() && b.int_expr == nullptr) {
      full_col = *b.full_col;
      delta = 0;
    } else if (b.int_expr != nullptr && b.int_expr->ops.size() == 3) {
      const auto& ops = b.int_expr->ops;
      slot_t proj_c = 0;
      if (ops[0].kind == TypedIntExprOp::Kind::kLoadIntCol &&
          ops[1].kind == TypedIntExprOp::Kind::kConstant &&
          ops[2].kind == TypedIntExprOp::Kind::kAdd) {
        proj_c = ops[0].column;
        delta = ops[1].constant;
      } else if (ops[0].kind == TypedIntExprOp::Kind::kConstant &&
                 ops[1].kind == TypedIntExprOp::Kind::kLoadIntCol &&
                 ops[2].kind == TypedIntExprOp::Kind::kAdd) {
        proj_c = ops[1].column;
        delta = ops[0].constant;
      } else if (ops[0].kind == TypedIntExprOp::Kind::kLoadIntCol &&
                 ops[1].kind == TypedIntExprOp::Kind::kConstant &&
                 ops[2].kind == TypedIntExprOp::Kind::kSub &&
                 ops[1].constant != std::numeric_limits<int64_t>::min()) {
        proj_c = ops[0].column;
        delta = -ops[1].constant;
      } else {
        return std::optional<Relation>{};
      }
      if (proj_c >= proj_types.size() ||
          proj_types[proj_c] != ValueType::kInt64 ||
          proj_unsigned[proj_c] != 0) {
        return std::optional<Relation>{};
      }
      full_col = proj_to_full[proj_c];
      if (full_col >= store.column_has_null.size()) {
        return std::optional<Relation>{};
      }
    } else {
      return std::optional<Relation>{};
    }
    agg_bind_cols[full_col] = 1;
    if (col_to_group[full_col] < 0) {
      col_to_group[full_col] = static_cast<int>(groups.size());
      groups.push_back(ColGroup{
          .full_col = full_col, .min_delta = delta, .max_delta = delta});
    } else {
      ColGroup& g = groups[static_cast<size_t>(col_to_group[full_col])];
      g.min_delta = std::min(g.min_delta, delta);
      g.max_delta = std::max(g.max_delta, delta);
    }
    terms.push_back(Term{.agg_idx = a, .full_col = full_col, .delta = delta});
  }
  if (groups.size() >= bindings.size()) {
    return std::optional<Relation>{};
  }

  for (const ColGroup& g : groups) {
    int64_t g_min = std::numeric_limits<int64_t>::max();
    int64_t g_max = std::numeric_limits<int64_t>::min();
    bool saw_block = false;
    for (const LSTablePageBlock& blk : store.blocks) {
      if (blk.row_count == 0) {
        continue;
      }
      const LSTableColumnBlock& col = blk.columns[g.full_col];
      if (!col.has_min_max) {
        return std::optional<Relation>{};
      }
      saw_block = true;
      g_min = std::min(g_min, col.min_int);
      g_max = std::max(g_max, col.max_int);
    }
    if (!saw_block) {
      return std::optional<Relation>{};
    }
    const platform::Int128 lo =
        static_cast<platform::Int128>(g_min) + g.min_delta;
    const platform::Int128 hi =
        static_cast<platform::Int128>(g_max) + g.max_delta;
    constexpr platform::Int128 kI64Min = std::numeric_limits<int64_t>::min();
    constexpr platform::Int128 kI64Max = std::numeric_limits<int64_t>::max();
    if (lo < kI64Min || hi > kI64Max) {
      return std::optional<Relation>{};
    }
    auto abs128 = [](platform::Int128 v) { return v < 0 ? -v : v; };
    const platform::Int128 max_abs =
        std::max({abs128(g_min), abs128(g_max), abs128(lo), abs128(hi),
                  abs128(static_cast<platform::Int128>(g_min) + g.max_delta),
                  abs128(static_cast<platform::Int128>(g_max) + g.min_delta)});
    if (max_abs > kI64Max / static_cast<platform::Int128>(store.total_rows)) {
      return std::optional<Relation>{};
    }
  }

  const size_t nblocks = store.blocks.size();
  const size_t ngroups = groups.size();
  const size_t workers_used =
      std::max<size_t>(1, std::min(worker_count, std::max<size_t>(1, nblocks)));
  struct alignas(64) GroupWorkerState {
    std::vector<int64_t> sums;
    std::vector<int64_t> counts;
    size_t scanned_rows{0};
    size_t matching_rows{0};
  };
  std::vector<GroupWorkerState> states(workers_used);
  for (auto& st : states) {
    st.sums.assign(ngroups, 0);
    st.counts.assign(ngroups, 0);
  }

  RETURN_IF_FAIL(RunParallelTasks(workers_used, [&](size_t w) {
    GroupWorkerState& st = states[w];
    CompiledLSTableBlockFilter::Scratch ws;
    filter.InitScratch(store.column_has_null.size(), &ws);
    std::vector<uint32_t> sel;
    const size_t blk_begin = (nblocks * w) / workers_used;
    const size_t blk_end = (nblocks * (w + 1)) / workers_used;
    for (size_t bi = blk_begin; bi < blk_end; ++bi) {
      const auto& blk = store.blocks[bi];
      st.scanned_rows += blk.row_count;
      bool all_pass = false;
      const uint32_t m =
          filter.FilterBlock(blk, agg_bind_cols, &ws, &all_pass, &sel);
      if (m == 0) {
        continue;
      }
      st.matching_rows += m;
      const uint32_t* sel_ptr = sel.data();
      for (size_t gi = 0; gi < ngroups; ++gi) {
        const DirectBlockCol& col = ws.cols[groups[gi].full_col];
        int64_t tot = 0;
        int64_t cnt = 0;
        if (!col.has_null) {
          if (all_pass) {
            const int64_t* __restrict cells = col.cells;
            for (uint32_t r = 0; r < m; ++r) {
              tot += cells[r];
            }
            cnt = static_cast<int64_t>(m);
          } else {
            const int64_t* __restrict cells = col.cells;
            for (uint32_t idx = 0; idx < m; ++idx) {
              tot += cells[sel_ptr[idx]];
            }
            cnt = static_cast<int64_t>(m);
          }
        } else if (all_pass) {
          for (uint32_t r = 0; r < m; ++r) {
            if (!col.IsNull(r)) {
              tot += col.Int(r);
              ++cnt;
            }
          }
        } else {
          for (uint32_t idx = 0; idx < m; ++idx) {
            const uint32_t r = sel_ptr[idx];
            if (!col.IsNull(r)) {
              tot += col.Int(r);
              ++cnt;
            }
          }
        }
        st.sums[gi] += tot;
        st.counts[gi] += cnt;
      }
    }
  }));

  const auto merge_begin = std::chrono::steady_clock::now();
  std::vector<int64_t> group_sums(ngroups, 0);
  std::vector<int64_t> group_cnts(ngroups, 0);
  size_t total_scanned = 0;
  size_t total_matching = 0;
  for (const auto& st : states) {
    total_scanned += st.scanned_rows;
    total_matching += st.matching_rows;
    for (size_t gi = 0; gi < ngroups; ++gi) {
      group_sums[gi] += st.sums[gi];
      group_cnts[gi] += st.counts[gi];
    }
  }
  std::vector<Value> finished_values(bindings.size());
  for (const Term& term : terms) {
    const auto gi = static_cast<size_t>(col_to_group[term.full_col]);
    const int64_t cnt = group_cnts[gi];
    if (cnt == 0) {
      finished_values[term.agg_idx] = Value();
    } else {
      const platform::Int128 adjusted =
          static_cast<platform::Int128>(group_sums[gi]) +
          (static_cast<platform::Int128>(term.delta) * cnt);
      finished_values[term.agg_idx] = Value(static_cast<int64_t>(adjusted));
    }
  }
  return std::optional<Relation>(BuildScalarDirectOutput(
      plan, stmt, qualified_schema, table_schema, finished_values,
      total_scanned, total_matching, total_matching * bindings.size(),
      publish_lstable_marker, fold_cache_key, scan_begin, merge_begin,
      runtime));
}

}  // namespace

StatusOr<std::optional<Relation>> TryRunDirectLSTableScalarFast(
    const LSTableStore& store, const TypedAggregationPlan& plan,
    const SelectStatement& stmt, const Expression& having_expr,
    const std::vector<slot_t>& proj_to_full,
    const std::vector<ValueType>& proj_types,
    const std::vector<uint8_t>& proj_unsigned, const Schema& qualified_schema,
    const Schema& table_schema, bool direct_children,
    const RuntimeFilterGroup* stream_runtime_filter,
    const PrebuiltMembershipPredicate* stream_prebuilt_membership,
    size_t worker_count, ExecutionRuntime* runtime, bool publish_lstable_marker,
    const std::string& fold_cache_key,
    std::chrono::steady_clock::time_point scan_begin) {
  ASSIGN_OR_RETURN(
      std::optional<Relation>, count_distinct_result,
      TryRunParallelScalarCountDistinct(
          store, plan, stmt, having_expr, proj_to_full, proj_types,
          proj_unsigned, qualified_schema, table_schema, direct_children,
          stream_runtime_filter, stream_prebuilt_membership, worker_count,
          runtime, publish_lstable_marker, fold_cache_key, scan_begin));
  if (count_distinct_result) {
    return count_distinct_result;
  }
  if (stream_runtime_filter != nullptr ||
      stream_prebuilt_membership != nullptr ||
      (!plan.filter_programs.empty() && !plan.only_null_filter_programs) ||
      !(stmt.GroupBy().empty() || stmt.IsGroupAll()) ||
      !plan.key_offsets.empty() || having_expr != nullptr || stmt.HasLimit() ||
      stmt.Distinct() || stmt.WithTies() || !plan.order_items.empty() ||
      !plan.dependent_key_programs.empty() || !plan.slim ||
      plan.aggregates.empty() || plan.aggregates.size() > 128) {
    return std::optional<Relation>{};
  }
  for (const TypedEmitItem& item : plan.emit_items) {
    if (item.dependent_key || item.is_key) {
      return std::optional<Relation>{};
    }
    if (item.post_agg != nullptr) {
      for (const auto& sl : item.post_agg->slots) {
        if (sl.is_key) {
          return std::optional<Relation>{};
        }
      }
    }
  }
  CompiledLSTableBlockFilter filter;
  if (!filter.Init(plan, proj_to_full, proj_types, proj_unsigned, store)) {
    return std::optional<Relation>{};
  }
  std::vector<DirectAggBinding> bindings;
  std::vector<uint8_t> agg_bind_cols;
  if (!CollectDirectAggBindings(plan, store, proj_to_full, proj_types,
                                proj_unsigned, &bindings, &agg_bind_cols)) {
    return std::optional<Relation>{};
  }

  if (auto meta_finished = TryScalarBlockMetaMinMax(store, filter, bindings)) {
    const auto merge_begin = std::chrono::steady_clock::now();
    return BuildScalarDirectOutputChecked(
        plan, stmt, qualified_schema, table_schema, *meta_finished,
        store.total_rows, store.total_rows, store.total_rows,
        publish_lstable_marker, fold_cache_key, scan_begin, merge_begin,
        runtime);
  }
  size_t dict_matching = 0;
  ASSIGN_OR_RETURN(std::optional<std::vector<Value>>, dict_finished,
                   TryScalarNarrowDictShortcut(store, table_schema, filter,
                                               bindings, &dict_matching));
  if (dict_finished) {
    const auto merge_begin = std::chrono::steady_clock::now();
    return BuildScalarDirectOutputChecked(
        plan, stmt, qualified_schema, table_schema, *dict_finished,
        store.total_rows, dict_matching, dict_matching, publish_lstable_marker,
        fold_cache_key, scan_begin, merge_begin, runtime);
  }
  ASSIGN_OR_RETURN(
      std::optional<Relation>, linear_sum_result,
      TryRunFusedLinearIntSums(
          store, plan, stmt, filter, bindings, proj_to_full, proj_types,
          proj_unsigned, qualified_schema, table_schema, worker_count, runtime,
          publish_lstable_marker, fold_cache_key, scan_begin));
  if (linear_sum_result) {
    return linear_sum_result;
  }

  // Resident numeric columns are contiguous across physical pages. Batch
  // them without repeating predicate setup for every small row-page slice.
  // Nullable/encoded/string columns retain the ordinary block path.
  constexpr size_t kFlatBatchRows = 16384;
  std::vector<uint8_t> flat_needed = filter.referenced_full_cols;
  for (size_t c = 0; c < agg_bind_cols.size(); ++c) {
    flat_needed[c] |= agg_bind_cols[c];
  }
  bool coalesce = store.total_rows >= 4096 && !store.blocks.empty();
  for (size_t c = 0; coalesce && c < flat_needed.size(); ++c) {
    // NOLINTNEXTLINE(readability-uppercase-literal-suffix) literal suffix style
    if (flat_needed[c] == 0u) {
      continue;
    }
    coalesce = store.IntegerColumn(c).size() == store.total_rows &&
               // NOLINTNEXTLINE(readability-uppercase-literal-suffix) literal
               // suffix style
               (store.column_has_null[c] ==
                0u) &&  // NOLINT(readability-uppercase-literal-suffix)
               (store.blocks.front().columns[c].type == ValueType::kInt64 ||
                store.blocks.front().columns[c].type == ValueType::kDate ||
                store.blocks.front().columns[c].type == ValueType::kDouble);
    // Preserve page-level range pruning for ordered predicate columns.
    // NOLINTNEXTLINE(readability-uppercase-literal-suffix) literal suffix style
    if ((filter.referenced_full_cols[c] != 0u) &&
        // NOLINTNEXTLINE(readability-uppercase-literal-suffix) literal suffix
        // style
        c < store.column_is_monotonic.size() &&
        (store.column_is_monotonic[c] !=
         0u)) {  // NOLINT(readability-uppercase-literal-suffix)
      coalesce = false;
    }
  }
  std::shared_ptr<QueryMemoryCharge> flat_scratch_charge;
  if (coalesce) {
    flat_scratch_charge = TryReserveQueryMemory(
        worker_count * (kFlatBatchRows * sizeof(uint32_t) +
                        flat_needed.size() * sizeof(LSTableColumnBlock)));
    coalesce = flat_scratch_charge != nullptr;
  }
  const size_t nblocks =
      coalesce ? (store.total_rows + kFlatBatchRows - 1) / kFlatBatchRows
               : store.blocks.size();
  const size_t naggs = bindings.size();
  const size_t workers_used =
      std::max<size_t>(1, std::min(worker_count, std::max<size_t>(1, nblocks)));
  struct alignas(64) ScalarWorkerState {
    std::vector<int64_t> counts;
    std::vector<int64_t> int_totals;
    std::vector<double> double_totals;
    std::vector<uint8_t> flags;
    std::vector<std::unique_ptr<FlatIntKeyMap<uint8_t>>> distinct_maps;
    size_t scanned_rows{0};
    size_t matching_rows{0};
    size_t updates{0};
    Status err{Status::kSuccess};
  };
  std::vector<ScalarWorkerState> states(workers_used);
  for (auto& st : states) {
    st.counts.assign(naggs, 0);
    st.int_totals.assign(naggs, 0);
    st.double_totals.assign(naggs, 0.0);
    st.flags.assign(naggs, 0);
    st.distinct_maps.resize(naggs);
  }

  RETURN_IF_FAIL(RunParallelTasks(workers_used, [&](size_t w) {
    ScalarWorkerState& st = states[w];
    CompiledLSTableBlockFilter::Scratch ws;
    filter.InitScratch(store.column_has_null.size(), &ws);
    std::vector<uint32_t> sel;
    const size_t blk_begin = (nblocks * w) / workers_used;
    const size_t blk_end = (nblocks * (w + 1)) / workers_used;

    LSTablePageBlock flat_block;
    if (coalesce) {
      flat_block.columns.resize(flat_needed.size());
      for (size_t c = 0; c < flat_needed.size(); ++c) {
        if (flat_needed[c])
          flat_block.columns[c].type = store.blocks.front().columns[c].type;
      }
    }
    for (size_t bi = blk_begin; bi < blk_end; ++bi) {
      if (coalesce) {
        const size_t begin = bi * kFlatBatchRows;
        flat_block.row_count = static_cast<uint32_t>(
            std::min<uint64_t>(kFlatBatchRows, store.total_rows - begin));
        for (size_t c = 0; c < flat_needed.size(); ++c) {
          if (flat_needed[c])
            flat_block.columns[c].int_cells =
                store.IntegerColumn(c).subspan(begin, flat_block.row_count);
        }
      }
      const auto& blk = coalesce ? flat_block : store.blocks[bi];
      st.scanned_rows += blk.row_count;
      bool all_pass = false;
      const uint32_t m =
          filter.FilterBlock(blk, agg_bind_cols, &ws, &all_pass, &sel);
      if (m == 0) {
        continue;
      }
      st.matching_rows += m;
      const uint32_t* sel_ptr = sel.data();

      for (size_t a = 0; a < naggs; ++a) {
        const DirectAggBinding& b = bindings[a];
        if (b.is_count_star) {
          st.counts[a] += static_cast<int64_t>(m);
          ++st.updates;
          continue;
        }
        if (b.full_col.has_value()) {
          const DirectBlockCol& col = ws.cols[*b.full_col];
          if (b.kind == SlimAggKind::kCount) {
            if (!col.has_null) {
              st.counts[a] += static_cast<int64_t>(m);
            } else {
              int64_t cnt = 0;
              if (all_pass) {
                for (uint32_t r = 0; r < m; ++r) {
                  cnt += static_cast<int64_t>(!col.IsNull(r));
                }
              } else {
                for (uint32_t idx = 0; idx < m; ++idx) {
                  cnt += static_cast<int64_t>(!col.IsNull(sel_ptr[idx]));
                }
              }
              st.counts[a] += cnt;
            }
            st.updates += m;
            continue;
          }
          if (b.kind == SlimAggKind::kSumDouble ||
              b.kind == SlimAggKind::kAvgDouble) {
            double sum = 0.0;
            int64_t cnt = 0;
            uint8_t saw_inf = 0;
            if (b.col_type == ValueType::kDouble) {
              if (!col.has_null) {
                if (all_pass) {
                  for (uint32_t r = 0; r < m; ++r) {
                    const double v = col.Double(r);
                    sum += v;
                    saw_inf |= static_cast<uint8_t>(std::isinf(v));
                  }
                } else {
                  for (uint32_t idx = 0; idx < m; ++idx) {
                    const double v = col.Double(sel_ptr[idx]);
                    sum += v;
                    saw_inf |= static_cast<uint8_t>(std::isinf(v));
                  }
                }
                cnt = static_cast<int64_t>(m);
              } else {
                if (all_pass) {
                  for (uint32_t r = 0; r < m; ++r) {
                    if (!col.IsNull(r)) {
                      const double v = col.Double(r);
                      sum += v;
                      saw_inf |= static_cast<uint8_t>(std::isinf(v));
                      ++cnt;
                    }
                  }
                } else {
                  for (uint32_t idx = 0; idx < m; ++idx) {
                    const uint32_t r = sel_ptr[idx];
                    if (!col.IsNull(r)) {
                      const double v = col.Double(r);
                      sum += v;
                      saw_inf |= static_cast<uint8_t>(std::isinf(v));
                      ++cnt;
                    }
                  }
                }
              }
            } else {
              if (!col.has_null) {
                if (all_pass) {
                  for (uint32_t r = 0; r < m; ++r) {
                    sum += static_cast<double>(col.Int(r));
                  }
                } else {
                  for (uint32_t idx = 0; idx < m; ++idx) {
                    sum += static_cast<double>(col.Int(sel_ptr[idx]));
                  }
                }
                cnt = static_cast<int64_t>(m);
              } else {
                if (all_pass) {
                  for (uint32_t r = 0; r < m; ++r) {
                    if (!col.IsNull(r)) {
                      sum += static_cast<double>(col.Int(r));
                      ++cnt;
                    }
                  }
                } else {
                  for (uint32_t idx = 0; idx < m; ++idx) {
                    const uint32_t r = sel_ptr[idx];
                    if (!col.IsNull(r)) {
                      sum += static_cast<double>(col.Int(r));
                      ++cnt;
                    }
                  }
                }
              }
            }
            st.double_totals[a] += sum;
            st.counts[a] += cnt;
            if (b.kind == SlimAggKind::kSumDouble) {
              st.flags[a] |= saw_inf;
            }
            st.updates += static_cast<size_t>(cnt);
            continue;
          }
          if (b.kind == SlimAggKind::kSumInt64) {
            int64_t tot = st.int_totals[a];
            int64_t cnt = 0;
            const auto add_int = [&](int64_t v) -> bool {
              int64_t next = 0;
              if (__builtin_add_overflow(tot, v, &next)) {
                st.err = StatusError(StatusCode::kIsInfinity,
                                     "integer overflow in SUM");
                return false;
              }
              tot = next;
              ++cnt;
              return true;
            };
            if (!col.has_null) {
              if (all_pass) {
                for (uint32_t r = 0; r < m; ++r) {
                  if (!add_int(col.Int(r))) {
                    return;
                  }
                }
              } else {
                for (uint32_t idx = 0; idx < m; ++idx) {
                  if (!add_int(col.Int(sel_ptr[idx]))) {
                    return;
                  }
                }
              }
            } else if (all_pass) {
              for (uint32_t r = 0; r < m; ++r) {
                if (!col.IsNull(r) && !add_int(col.Int(r))) {
                  return;
                }
              }
            } else {
              for (uint32_t idx = 0; idx < m; ++idx) {
                const uint32_t r = sel_ptr[idx];
                if (!col.IsNull(r) && !add_int(col.Int(r))) {
                  return;
                }
              }
            }
            st.int_totals[a] = tot;
            st.counts[a] += cnt;
            st.updates += static_cast<size_t>(cnt);
            continue;
          }
          if (b.kind == SlimAggKind::kCountDistinctInt64 ||
              b.kind == SlimAggKind::kCountDistinctDouble) {
            if (!st.distinct_maps[a]) {
              st.distinct_maps[a] = std::make_unique<FlatIntKeyMap<uint8_t>>();
              st.distinct_maps[a]->Reserve(std::min<size_t>(
                  16384, std::max<size_t>(256, store.total_rows / 64)));
            }
            auto& dmap = *st.distinct_maps[a];
            int64_t cnt = 0;
            const bool is_dbl = (b.kind == SlimAggKind::kCountDistinctDouble);
            const auto ins = [&](uint32_t r) {
              const int64_t raw =
                  is_dbl ? CanonicalDistinctDoubleBits(col.Double(r))
                         : col.Int(r);
              bool inserted = false;
              uint8_t* slot = dmap.TryEmplace(raw, &inserted);
              if (inserted) {
                *slot = 1;
              }
              ++cnt;
            };
            if (!col.has_null) {
              if (all_pass) {
                for (uint32_t r = 0; r < m; ++r) {
                  ins(r);
                }
              } else {
                for (uint32_t idx = 0; idx < m; ++idx) {
                  ins(sel_ptr[idx]);
                }
              }
            } else if (all_pass) {
              for (uint32_t r = 0; r < m; ++r) {
                if (!col.IsNull(r)) {
                  ins(r);
                }
              }
            } else {
              for (uint32_t idx = 0; idx < m; ++idx) {
                const uint32_t r = sel_ptr[idx];
                if (!col.IsNull(r)) {
                  ins(r);
                }
              }
            }
            st.updates += static_cast<size_t>(cnt);
            continue;
          }
          if (b.kind == SlimAggKind::kMinInt64 ||
              b.kind == SlimAggKind::kMaxInt64 ||
              b.kind == SlimAggKind::kMinDate ||
              b.kind == SlimAggKind::kMaxDate) {
            const bool is_min = (b.kind == SlimAggKind::kMinInt64 ||
                                 b.kind == SlimAggKind::kMinDate);
            // Lean proof:
            // lean/TinylambOpt/ResearchProofs/AnalyticalOperators/DictionarySlicesAndExtrema.lean
            //   (sound_DominatedColumn).
            // A block whose non-NULL range cannot improve the current extremum
            // is skipped regardless of which rows survived the filter.
            if (!coalesce && st.counts[a] > 0) {
              const LSTableColumnBlock& raw_col = blk.columns[*b.full_col];
              if (raw_col.has_min_max && !raw_col.is_unsigned) {
                if (is_min ? (raw_col.min_int >= st.int_totals[a])
                           : (raw_col.max_int <= st.int_totals[a])) {
                  continue;
                }
              }
            }
            int64_t best = st.int_totals[a];
            int64_t cnt = st.counts[a];
            int64_t added = 0;
            const auto upd = [&](int64_t v) {
              if (cnt == 0 || (is_min ? v < best : best < v)) {
                best = v;
              }
              ++cnt;
              ++added;
            };
            if (!col.has_null) {
              if (all_pass) {
                for (uint32_t r = 0; r < m; ++r) {
                  upd(col.Int(r));
                }
              } else {
                for (uint32_t idx = 0; idx < m; ++idx) {
                  upd(col.Int(sel_ptr[idx]));
                }
              }
            } else if (all_pass) {
              for (uint32_t r = 0; r < m; ++r) {
                if (!col.IsNull(r)) {
                  upd(col.Int(r));
                }
              }
            } else {
              for (uint32_t idx = 0; idx < m; ++idx) {
                const uint32_t r = sel_ptr[idx];
                if (!col.IsNull(r)) {
                  upd(col.Int(r));
                }
              }
            }
            st.int_totals[a] = best;
            st.counts[a] = cnt;
            st.updates += static_cast<size_t>(added);
            continue;
          }
          // kMinDouble / kMaxDouble
          const bool is_min = (b.kind == SlimAggKind::kMinDouble);
          // Lean proof:
          // lean/TinylambOpt/ResearchProofs/AnalyticalOperators/DictionarySlicesAndExtrema.lean
          //   (sound_DominatedColumn).
          if (!coalesce && st.counts[a] > 0 && (st.flags[a] & 1) == 0 &&
              !std::isnan(st.double_totals[a])) {
            const LSTableColumnBlock& raw_col = blk.columns[*b.full_col];
            if (raw_col.has_double_min_max && !raw_col.has_nan) {
              if (is_min ? (raw_col.min_double >= st.double_totals[a])
                         : (raw_col.max_double <= st.double_totals[a])) {
                continue;
              }
            }
          }
          double best = st.double_totals[a];
          int64_t cnt = st.counts[a];
          uint8_t flags = st.flags[a];
          int64_t added = 0;
          const auto upd = [&](double v) {
            if (std::isnan(v)) {
              flags |= 1;
            } else if (cnt == 0 || (is_min ? v < best : best < v)) {
              best = v;
            }
            ++cnt;
            ++added;
          };
          if (!col.has_null) {
            if (all_pass) {
              for (uint32_t r = 0; r < m; ++r) {
                upd(col.Double(r));
              }
            } else {
              for (uint32_t idx = 0; idx < m; ++idx) {
                upd(col.Double(sel_ptr[idx]));
              }
            }
          } else if (all_pass) {
            for (uint32_t r = 0; r < m; ++r) {
              if (!col.IsNull(r)) {
                upd(col.Double(r));
              }
            }
          } else {
            for (uint32_t idx = 0; idx < m; ++idx) {
              const uint32_t r = sel_ptr[idx];
              if (!col.IsNull(r)) {
                upd(col.Double(r));
              }
            }
          }
          st.double_totals[a] = best;
          st.counts[a] = cnt;
          st.flags[a] = flags;
          st.updates += static_cast<size_t>(added);
          continue;
        }

        if (b.double_expr != nullptr) {
          const TypedDoubleExprInput& de = *b.double_expr;
          double sum_or_best = st.double_totals[a];
          int64_t cnt = st.counts[a];
          uint8_t flags = st.flags[a];
          int64_t added = 0;
          // Recognized non-null arithmetic needs no per-row shape or NULL
          // dispatch. Preserve operation order and SUM's infinity tracking.
          if (b.kind == SlimAggKind::kSumDouble ||
              b.kind == SlimAggKind::kAvgDouble) {
            const auto fold = [&](auto evaluate) {
              const auto run = [&]<bool selected, bool track_infinity>() {
                bool infinite = false;
                for (uint32_t i = 0; i < m; ++i) {
                  const uint32_t row = selected ? sel_ptr[i] : i;
                  const double value = evaluate(row);
                  sum_or_best += value;
                  if constexpr (track_infinity) infinite |= std::isinf(value);
                }
                if (infinite) flags |= 1;
              };
              if (all_pass) {
                if (b.kind == SlimAggKind::kSumDouble)
                  run.template operator()<false, true>();
                else
                  run.template operator()<false, false>();
              } else {
                if (b.kind == SlimAggKind::kSumDouble)
                  run.template operator()<true, true>();
                else
                  run.template operator()<true, false>();
              }
            };
            bool folded = false;
            if (de.shape != TypedDoubleExprInput::Shape::kGeneric) {
              const auto& x = ws.cols[proj_to_full[de.col0]];
              if (!x.has_null &&
                  de.shape == TypedDoubleExprInput::Shape::kSingleDoubleCol) {
                fold([&](uint32_t row) { return x.Double(row); });
                folded = true;
              } else if (!x.has_null) {
                const auto& y = ws.cols[proj_to_full[de.col1]];
                if (!y.has_null &&
                    de.shape ==
                        TypedDoubleExprInput::Shape::kMulTwoDoubleCols) {
                  fold([&](uint32_t row) {
                    return x.Double(row) * y.Double(row);
                  });
                  folded = true;
                } else if (!y.has_null &&
                           de.shape ==
                               TypedDoubleExprInput::Shape::kMulConstMinusCol) {
                  fold([&](uint32_t row) {
                    return x.Double(row) * (de.k0 - y.Double(row));
                  });
                  folded = true;
                } else if (!y.has_null &&
                           de.shape == TypedDoubleExprInput::Shape::
                                           kMulConstMinusColMulConstPlusCol) {
                  const auto& z = ws.cols[proj_to_full[de.col2]];
                  if (!z.has_null) {
                    fold([&](uint32_t row) {
                      return x.Double(row) * (de.k0 - y.Double(row)) *
                             (de.k1 + z.Double(row));
                    });
                    folded = true;
                  }
                }
              }
            }
            if (folded) {
              st.double_totals[a] = sum_or_best;
              st.counts[a] = cnt + m;
              st.flags[a] = flags;
              st.updates += m;
              continue;
            }
          }
          const auto handle_val = [&](double v) {
            if (b.kind == SlimAggKind::kCount) {
              ++cnt;
              ++added;
              return;
            }
            if (b.kind == SlimAggKind::kSumDouble ||
                b.kind == SlimAggKind::kAvgDouble) {
              sum_or_best += v;
              if (b.kind == SlimAggKind::kSumDouble && std::isinf(v)) {
                flags |= 1;
              }
            } else {
              const bool is_min = (b.kind == SlimAggKind::kMinDouble);
              if (std::isnan(v)) {
                flags |= 1;
              } else if (cnt == 0 ||
                         (is_min ? v < sum_or_best : sum_or_best < v)) {
                sum_or_best = v;
              }
            }
            ++cnt;
            ++added;
          };
          if (all_pass) {
            for (uint32_t r = 0; r < m; ++r) {
              double v = 0.0;
              if (EvalDirectDoubleExpr(de, proj_to_full, ws.cols, r, &v)) {
                handle_val(v);
              }
            }
          } else {
            for (uint32_t idx = 0; idx < m; ++idx) {
              double v = 0.0;
              if (EvalDirectDoubleExpr(de, proj_to_full, ws.cols, sel_ptr[idx],
                                       &v)) {
                handle_val(v);
              }
            }
          }
          st.double_totals[a] = sum_or_best;
          st.counts[a] = cnt;
          st.flags[a] = flags;
          st.updates += static_cast<size_t>(added);
          continue;
        }

        const TypedIntExprInput& ie = *b.int_expr;
        int64_t cnt = st.counts[a];
        int64_t itot = st.int_totals[a];
        double dtot = st.double_totals[a];
        int64_t added = 0;
        const auto handle_ival = [&](int64_t v) -> bool {
          if (b.kind == SlimAggKind::kCount) {
            ++cnt;
          } else if (b.kind == SlimAggKind::kSumInt64) {
            int64_t next = 0;
            if (__builtin_add_overflow(itot, v, &next)) {
              st.err = StatusError(StatusCode::kIsInfinity,
                                   "integer overflow in SUM");
              return false;
            }
            itot = next;
            ++cnt;
          } else if (b.kind == SlimAggKind::kAvgDouble) {
            dtot += static_cast<double>(v);
            ++cnt;
          } else {
            const bool is_min = (b.kind == SlimAggKind::kMinInt64);
            if (cnt == 0 || (is_min ? v < itot : itot < v)) {
              itot = v;
            }
            ++cnt;
          }
          ++added;
          return true;
        };
        if (all_pass) {
          for (uint32_t r = 0; r < m; ++r) {
            int64_t v = 0;
            if (EvalDirectIntExpr(ie, proj_to_full, ws.cols, r, &v, &st.err)) {
              if (!handle_ival(v)) {
                return;
              }
            } else if (!st.err.ok()) {
              return;
            }
          }
        } else {
          for (uint32_t idx = 0; idx < m; ++idx) {
            int64_t v = 0;
            if (EvalDirectIntExpr(ie, proj_to_full, ws.cols, sel_ptr[idx], &v,
                                  &st.err)) {
              if (!handle_ival(v)) {
                return;
              }
            } else if (!st.err.ok()) {
              return;
            }
          }
        }
        st.counts[a] = cnt;
        st.int_totals[a] = itot;
        st.double_totals[a] = dtot;
        st.updates += static_cast<size_t>(added);
      }
    }
  }));

  for (const auto& st : states) {
    if (!st.err.ok()) {
      return st.err;
    }
  }
  const auto merge_begin = std::chrono::steady_clock::now();
  size_t total_scanned = 0;
  size_t total_matching = 0;
  size_t total_updates = 0;
  for (const auto& st : states) {
    total_scanned += st.scanned_rows;
    total_matching += st.matching_rows;
    total_updates += st.updates;
  }

  std::vector<Value> finished_values(naggs);
  for (size_t a = 0; a < naggs; ++a) {
    const SlimAggKind kind = bindings[a].kind;
    int64_t total_cnt = 0;
    int64_t total_int = 0;
    double total_dbl = 0.0;
    uint8_t total_flags = 0;
    size_t distinct_cnt = 0;
    if (IsSlimDistinctKind(kind)) {
      std::unique_ptr<FlatIntKeyMap<uint8_t>> merged_dmap;
      for (size_t w = 0; w < workers_used; ++w) {
        if (!states[w].distinct_maps[a]) {
          continue;
        }
        if (!merged_dmap) {
          merged_dmap = std::move(states[w].distinct_maps[a]);
        } else {
          states[w].distinct_maps[a]->ForEach([&](int64_t raw, uint8_t) {
            bool inserted = false;
            uint8_t* slot = merged_dmap->TryEmplace(raw, &inserted);
            if (inserted) {
              *slot = 1;
            }
          });
        }
      }
      distinct_cnt = merged_dmap ? merged_dmap->Size() : 0;
    } else if (IsSlimMinMaxKind(kind)) {
      const bool is_min =
          (kind == SlimAggKind::kMinInt64 || kind == SlimAggKind::kMinDate ||
           kind == SlimAggKind::kMinDouble);
      for (size_t w = 0; w < workers_used; ++w) {
        const int64_t wc = states[w].counts[a];
        if (wc == 0) {
          continue;
        }
        if (kind == SlimAggKind::kMinDouble ||
            kind == SlimAggKind::kMaxDouble) {
          total_flags |= states[w].flags[a];
          const double wv = states[w].double_totals[a];
          if ((states[w].flags[a] & 1) == 0 &&
              (total_cnt == 0 || (is_min ? wv < total_dbl : total_dbl < wv))) {
            total_dbl = wv;
          }
        } else {
          const int64_t wv = states[w].int_totals[a];
          if (total_cnt == 0 || (is_min ? wv < total_int : total_int < wv)) {
            total_int = wv;
          }
        }
        total_cnt += wc;
      }
    } else {
      for (size_t w = 0; w < workers_used; ++w) {
        const int64_t wc = states[w].counts[a];
        if (wc == 0) {
          continue;
        }
        if (kind == SlimAggKind::kSumInt64) {
          int64_t next = 0;
          if (__builtin_add_overflow(total_int, states[w].int_totals[a],
                                     &next)) {
            return StatusError(StatusCode::kIsInfinity,
                               "integer overflow in SUM");
          }
          total_int = next;
        } else if (kind == SlimAggKind::kSumDouble ||
                   kind == SlimAggKind::kAvgDouble) {
          total_dbl += states[w].double_totals[a];
          total_flags |= states[w].flags[a];
        }
        total_cnt += wc;
      }
    }
    Status finish_err = Status::kSuccess;
    finished_values[a] =
        FinishDirectState(kind, total_cnt, total_int, total_dbl, total_flags,
                          distinct_cnt, &finish_err);
    if (!finish_err.ok()) {
      return finish_err;
    }
  }
  return BuildScalarDirectOutputChecked(
      plan, stmt, qualified_schema, table_schema, finished_values,
      total_scanned, total_matching, total_updates, publish_lstable_marker,
      fold_cache_key, scan_begin, merge_begin, runtime);
}

StatusOr<std::optional<Relation>> TryRunDirectLSTableBoundedIntFast(
    const LSTableStore& store, const TypedAggregationPlan& plan,
    const SelectStatement& stmt, const Expression& having_expr,
    TransactionContext& context, const CteMap& ctes,
    const std::vector<slot_t>& proj_to_full,
    const std::vector<ValueType>& proj_types,
    const std::vector<uint8_t>& proj_unsigned, const Schema& qualified_schema,
    const Schema& table_schema, bool direct_children,
    const RuntimeFilterGroup* stream_runtime_filter,
    const PrebuiltMembershipPredicate* stream_prebuilt_membership,
    size_t worker_count, ExecutionRuntime* runtime, bool publish_lstable_marker,
    const std::string& fold_cache_key,
    std::chrono::steady_clock::time_point scan_begin) {
  const size_t nkeys = plan.key_offsets.size();
  // Lean proof:
  // lean/TinylambOpt/ResearchProofs/AnalyticsWorkloads/HotKeyAndEpochAggregates.lean
  // (CountTail_sound) Allow ORDER BY COUNT(*) / COUNT(col) DESC so 2-key
  // bounded aggregates (e.g. H2O H2) use the direct dense array + hybrid
  // bucket/tail ordering.
  bool count_tail_order = false;
  size_t count_tail_agg_idx = 0;
  if (plan.order_items.size() == 1 && plan.order_ascending.size() == 1 &&
      !plan.order_ascending[0] && having_expr == nullptr &&
      !plan.order_items[0].constant && !plan.order_items[0].is_key &&
      !plan.order_items[0].dependent_key &&
      plan.order_items[0].post_agg == nullptr &&
      plan.order_items[0].index < plan.slim_kinds.size() &&
      plan.slim_kinds[plan.order_items[0].index] == SlimAggKind::kCount) {
    count_tail_order = true;
    count_tail_agg_idx = plan.order_items[0].index;
  }
  if (stream_runtime_filter != nullptr ||
      stream_prebuilt_membership != nullptr ||
      (!plan.filter_programs.empty() && !plan.only_null_filter_programs) ||
      stmt.GroupBy().empty() || stmt.IsGroupAll() ||
      (nkeys != 1 && nkeys != 2) ||
      (nkeys == 2 && (::tinylamb::platform::GetEnv(
                          "TINYLAMB_DISABLE_TWO_KEY_DIRECT_AGG") != nullptr ||
                      having_expr != nullptr)) ||
      stmt.HasLimit() || stmt.Distinct() || stmt.WithTies() ||
      (!plan.order_items.empty() && !count_tail_order) ||
      !plan.dependent_key_programs.empty() || !plan.inlist_compares.empty() ||
      !plan.slim || plan.aggregates.empty() || plan.aggregates.size() > 4) {
    return std::optional<Relation>{};
  }
  const slot_t key_proj0 = plan.key_offsets[0];
  const ValueType key_type0 = proj_types[key_proj0];
  if ((key_type0 != ValueType::kInt64 && key_type0 != ValueType::kDate) ||
      proj_unsigned[key_proj0] != 0) {
    return std::optional<Relation>{};
  }
  const slot_t key_full0 = proj_to_full[key_proj0];
  if (key_full0 >= store.column_has_null.size() ||
      key_full0 >= store.column_min_int.size() ||
      (nkeys == 1 && key_full0 < store.column_is_monotonic.size() &&
       store.column_is_monotonic[key_full0] != 0 &&
       store.total_rows >= 65536)) {
    return std::optional<Relation>{};
  }
  const bool key_nullable0 = store.column_has_null[key_full0] != 0;
  const int64_t key_min0 = store.column_min_int[key_full0];
  const int64_t key_max0 = store.column_max_int[key_full0];
  size_t span0 = 0;
  if (key_min0 <= key_max0) {
    const uint64_t diff0 =
        static_cast<uint64_t>(key_max0) - static_cast<uint64_t>(key_min0);
    if (diff0 >= 262144ULL) {
      return std::optional<Relation>{};
    }
    span0 = static_cast<size_t>(diff0 + 1ULL);
  } else if (!key_nullable0) {
    return std::optional<Relation>{};
  }
  const size_t s0 = span0 + (key_nullable0 ? 1 : 0);

  slot_t key_proj1 = 0;
  ValueType key_type1 = ValueType::kNull;
  slot_t key_full1 = 0;
  bool key_nullable1 = false;
  int64_t key_min1 = 0;
  int64_t key_max1 = 0;
  size_t span1 = 0;
  size_t s1 = 1;
  if (nkeys == 2) {
    // Lean proof:
    // lean/TinylambOpt/ResearchProofs/BoundedMultiKeyDirectArrayAggregate.lean
    // (sound)
    key_proj1 = plan.key_offsets[1];
    key_type1 = proj_types[key_proj1];
    if ((key_type1 != ValueType::kInt64 && key_type1 != ValueType::kDate) ||
        proj_unsigned[key_proj1] != 0) {
      return std::optional<Relation>{};
    }
    key_full1 = proj_to_full[key_proj1];
    if (key_full1 >= store.column_has_null.size() ||
        key_full1 >= store.column_min_int.size()) {
      return std::optional<Relation>{};
    }
    key_nullable1 = store.column_has_null[key_full1] != 0;
    key_min1 = store.column_min_int[key_full1];
    key_max1 = store.column_max_int[key_full1];
    if (key_min1 <= key_max1) {
      const uint64_t diff1 =
          static_cast<uint64_t>(key_max1) - static_cast<uint64_t>(key_min1);
      if (diff1 >= 262144ULL) {
        return std::optional<Relation>{};
      }
      span1 = static_cast<size_t>(diff1 + 1ULL);
    } else if (!key_nullable1) {
      return std::optional<Relation>{};
    }
    s1 = span1 + (key_nullable1 ? 1 : 0);
    if (s0 == 0 || s1 == 0 || s1 > 262144 / s0) {
      return std::optional<Relation>{};
    }
  } else if (s0 == 0) {
    return std::optional<Relation>{};
  }
  const size_t total_slots = (nkeys == 2) ? (s0 * s1) : s0;

  for (const SlimAggKind k : plan.slim_kinds) {
    if (IsSlimDistinctKind(k)) {
      return std::optional<Relation>{};
    }
  }
  for (const TypedEmitItem& item : plan.emit_items) {
    if (item.post_agg != nullptr || item.dependent_key ||
        (item.is_key && item.index >= nkeys)) {
      return std::optional<Relation>{};
    }
  }
  std::vector<PositionalHaving> positional_having;
  if (having_expr != nullptr) {
    if (!TryCompilePositionalHavingConjuncts(having_expr, plan.aggregates,
                                             &positional_having)) {
      positional_having.clear();
    }
  }
  CompiledLSTableBlockFilter filter;
  if (!filter.Init(plan, proj_to_full, proj_types, proj_unsigned, store)) {
    return std::optional<Relation>{};
  }
  std::vector<DirectAggBinding> bindings;
  std::vector<uint8_t> always_bind_cols;
  if (!CollectDirectAggBindings(plan, store, proj_to_full, proj_types,
                                proj_unsigned, &bindings, &always_bind_cols)) {
    return std::optional<Relation>{};
  }
  always_bind_cols[key_full0] = 1;
  if (nkeys == 2) {
    always_bind_cols[key_full1] = 1;
  }

  // Defer to existing fast_dict_double_agg when no nullable columns, no
  // null_filters, no disjunctions, no min/max, no non-constant/double compares,
  // and at most 1 aggregate exist.
  bool needs_this_path =
      nkeys == 2 || key_nullable0 || !plan.filter_programs.empty() ||
      !plan.filter_disjunctions.empty() || bindings.size() > 1 ||
      (having_expr != nullptr && positional_having.empty()) || span0 > 65536U ||
      (store.total_rows >= 65536 && worker_count > 1 &&
       std::ranges::any_of(plan.slim_kinds, [](SlimAggKind k) {
         return k == SlimAggKind::kSumDouble || k == SlimAggKind::kAvgDouble;
       }));
  for (const DirectAggBinding& b : bindings) {
    if (!direct_children || b.int_expr != nullptr || b.double_expr != nullptr ||
        IsSlimMinMaxKind(b.kind)) {
      needs_this_path = true;
    }
    if (b.full_col.has_value() && store.column_has_null[*b.full_col] != 0) {
      needs_this_path = true;
    }
  }
  for (size_t c = 0; c < filter.referenced_full_cols.size(); ++c) {
    if (filter.referenced_full_cols[c] != 0 && store.column_has_null[c] != 0) {
      needs_this_path = true;
    }
  }
  for (const TypedFilterCompare& cmp : plan.filter_compares) {
    if (!cmp.constant_payload || cmp.is_double) {
      needs_this_path = true;
    }
  }
  if (!needs_this_path) {
    return std::optional<Relation>{};
  }

  const size_t nblocks = store.blocks.size();
  const size_t naggs = bindings.size();
  size_t workers_used =
      std::max<size_t>(1, std::min(worker_count, std::max<size_t>(1, nblocks)));
  if (nkeys == 2 && total_slots > 32768) {
    workers_used = std::max<size_t>(
        1, std::min<size_t>(workers_used, (size_t{4} * 262144) / total_slots));
  }
  // Lean proof:
  // lean/TinylambOpt/ResearchProofs/AnalyticsWorkloads/HotKeyAndEpochAggregates.lean
  // (EpochAverage_sound): reuse a pooled 16-byte (stamp, count, sum) dense
  // buffer across queries via an epoch bump without zeroing or allocating a
  // separate present/row_gidx array, recording each touched gid even when its
  // first qualifying payload is NULL.
  const bool use_compact_epoch =
      nkeys == 2 && naggs == 1 && having_expr == nullptr &&
      store.total_rows < std::numeric_limits<uint32_t>::max() &&
      bindings[0].int_expr == nullptr && bindings[0].double_expr == nullptr &&
      (bindings[0].is_count_star ||
       (bindings[0].kind == SlimAggKind::kAvgDouble &&
        bindings[0].full_col.has_value() &&
        (bindings[0].col_type == ValueType::kInt64 ||
         bindings[0].col_type == ValueType::kDouble)));

  struct alignas(16) DirectCompactEpochSlot {
    uint32_t stamp{0};
    uint32_t count{0};
    double sum{0.0};
  };
  struct DirectCompactEpochBuffer {
    std::vector<DirectCompactEpochSlot> slots;
    uint32_t epoch{0};
  };
  static std::mutex direct_epoch_pool_mu;
  static std::vector<std::unique_ptr<DirectCompactEpochBuffer>>
      direct_epoch_pool;

  struct alignas(64) BoundedWorkerState {
    std::vector<uint8_t> present;
    std::array<std::vector<int64_t>, 4> counts;
    std::array<std::vector<int64_t>, 4> int_totals;
    std::array<std::vector<double>, 4> double_totals;
    std::array<std::vector<uint8_t>, 4> flags;
    std::unique_ptr<DirectCompactEpochBuffer> epoch_buf;
    bool epoch_active{false};
    size_t scanned_rows{0};
    size_t matching_rows{0};
    size_t updates{0};
    Status err{Status::kSuccess};

    void EnsureAllocated(size_t s, const std::vector<DirectAggBinding>& bs) {
      if (!present.empty()) {
        return;
      }
      present.assign(s, 0);
      for (size_t a = 0; a < bs.size(); ++a) {
        counts[a].assign(s, 0);
        const SlimAggKind k = bs[a].kind;
        if (k == SlimAggKind::kSumInt64 || k == SlimAggKind::kMinInt64 ||
            k == SlimAggKind::kMaxInt64 || k == SlimAggKind::kMinDate ||
            k == SlimAggKind::kMaxDate) {
          int_totals[a].assign(s, 0);
        }
        if (k == SlimAggKind::kSumDouble || k == SlimAggKind::kAvgDouble ||
            k == SlimAggKind::kMinDouble || k == SlimAggKind::kMaxDouble) {
          double_totals[a].assign(s, 0.0);
        }
        if (k == SlimAggKind::kSumDouble || k == SlimAggKind::kMinDouble ||
            k == SlimAggKind::kMaxDouble) {
          flags[a].assign(s, 0);
        }
      }
    }
  };
  std::vector<BoundedWorkerState> states(workers_used);
  struct DirectEpochPoolReturnGuard {
    explicit DirectEpochPoolReturnGuard(std::vector<BoundedWorkerState>* st)
        : states_ptr(st) {}
    DirectEpochPoolReturnGuard(const DirectEpochPoolReturnGuard&) = delete;
    DirectEpochPoolReturnGuard& operator=(const DirectEpochPoolReturnGuard&) =
        delete;
    DirectEpochPoolReturnGuard(DirectEpochPoolReturnGuard&&) = delete;
    DirectEpochPoolReturnGuard& operator=(DirectEpochPoolReturnGuard&&) =
        delete;
    std::vector<BoundedWorkerState>* states_ptr{nullptr};
    ~DirectEpochPoolReturnGuard() {
      if (states_ptr == nullptr) {
        return;
      }
      std::scoped_lock lock(direct_epoch_pool_mu);
      for (auto& st : *states_ptr) {
        if (st.epoch_buf && direct_epoch_pool.size() < 16) {
          direct_epoch_pool.push_back(std::move(st.epoch_buf));
        }
      }
    }
  } direct_epoch_guard{use_compact_epoch ? &states : nullptr};
  if (use_compact_epoch) {
    std::scoped_lock lock(direct_epoch_pool_mu);
    for (size_t w = 0; w < workers_used; ++w) {
      if (!direct_epoch_pool.empty()) {
        states[w].epoch_buf = std::move(direct_epoch_pool.back());
        direct_epoch_pool.pop_back();
      } else {
        states[w].epoch_buf = std::make_unique<DirectCompactEpochBuffer>();
      }
    }
  }

  RETURN_IF_FAIL(RunParallelTasks(workers_used, [&](size_t w) {
    BoundedWorkerState& st = states[w];
    CompiledLSTableBlockFilter::Scratch ws;
    filter.InitScratch(store.column_has_null.size(), &ws);
    std::vector<uint32_t> sel;
    std::vector<uint32_t> row_gidx;
    const size_t blk_begin = (nblocks * w) / workers_used;
    const size_t blk_end = (nblocks * (w + 1)) / workers_used;

    for (size_t bi = blk_begin; bi < blk_end; ++bi) {
      const auto& blk = store.blocks[bi];
      st.scanned_rows += blk.row_count;
      bool all_pass = false;
      const uint32_t m =
          filter.FilterBlock(blk, always_bind_cols, &ws, &all_pass, &sel);
      if (m == 0) {
        continue;
      }
      if (use_compact_epoch) {
        if (!st.epoch_active) {
          DirectCompactEpochBuffer& eb = *st.epoch_buf;
          if (eb.slots.size() < total_slots ||
              eb.epoch == std::numeric_limits<uint32_t>::max()) {
            eb.slots.assign(total_slots, DirectCompactEpochSlot{});
            eb.epoch = 1;
          } else {
            ++eb.epoch;
          }
          st.epoch_active = true;
        }
        st.matching_rows += m;
        const uint32_t* sel_ptr = sel.data();
        const DirectBlockCol& kcol0 = ws.cols[key_full0];
        const DirectBlockCol& kcol1 = ws.cols[key_full1];
        const int64_t* __restrict__ keys0 = kcol0.cells;
        const int64_t* __restrict__ keys1 = kcol1.cells;
        DirectCompactEpochSlot* __restrict__ eslots =
            st.epoch_buf->slots.data();
        const uint32_t ep = st.epoch_buf->epoch;
        if (bindings[0].is_count_star) {
          if (!kcol0.has_null && !kcol1.has_null) {
            if (all_pass) {
              for (uint32_t r = 0; r < m; ++r) {
                const size_t gi =
                    ((static_cast<size_t>(static_cast<uint64_t>(keys0[r]) -
                                          static_cast<uint64_t>(key_min0)) *
                      s1) +
                     static_cast<size_t>(static_cast<uint64_t>(keys1[r]) -
                                         static_cast<uint64_t>(key_min1)));
                DirectCompactEpochSlot& slot = eslots[gi];
                if (slot.stamp != ep) {
                  slot.stamp = ep;
                  slot.count = 1;
                } else {
                  ++slot.count;
                }
              }
            } else {
              for (uint32_t idx = 0; idx < m; ++idx) {
                const uint32_t r = sel_ptr[idx];
                const size_t gi =
                    ((static_cast<size_t>(static_cast<uint64_t>(keys0[r]) -
                                          static_cast<uint64_t>(key_min0)) *
                      s1) +
                     static_cast<size_t>(static_cast<uint64_t>(keys1[r]) -
                                         static_cast<uint64_t>(key_min1)));
                DirectCompactEpochSlot& slot = eslots[gi];
                if (slot.stamp != ep) {
                  slot.stamp = ep;
                  slot.count = 1;
                } else {
                  ++slot.count;
                }
              }
            }
          } else {
            for (uint32_t idx = 0; idx < m; ++idx) {
              const uint32_t r = all_pass ? idx : sel_ptr[idx];
              const size_t i0 =
                  kcol0.IsNull(r)
                      ? span0
                      : static_cast<size_t>(static_cast<uint64_t>(keys0[r]) -
                                            static_cast<uint64_t>(key_min0));
              const size_t i1 =
                  kcol1.IsNull(r)
                      ? span1
                      : static_cast<size_t>(static_cast<uint64_t>(keys1[r]) -
                                            static_cast<uint64_t>(key_min1));
              DirectCompactEpochSlot& slot = eslots[(i0 * s1) + i1];
              if (slot.stamp != ep) {
                slot.stamp = ep;
                slot.count = 1;
              } else {
                ++slot.count;
              }
            }
          }
          st.updates += m;
          continue;
        }
        const DirectBlockCol& vcol = ws.cols[*bindings[0].full_col];
        const bool is_int_v = (bindings[0].col_type == ValueType::kInt64);
        if (!kcol0.has_null && !kcol1.has_null && !vcol.has_null) {
          if (is_int_v) {
            const int64_t* __restrict__ vals = vcol.cells;
            if (all_pass) {
              for (uint32_t r = 0; r < m; ++r) {
                const size_t gi =
                    ((static_cast<size_t>(static_cast<uint64_t>(keys0[r]) -
                                          static_cast<uint64_t>(key_min0)) *
                      s1) +
                     static_cast<size_t>(static_cast<uint64_t>(keys1[r]) -
                                         static_cast<uint64_t>(key_min1)));
                DirectCompactEpochSlot& slot = eslots[gi];
                if (slot.stamp != ep) {
                  slot.stamp = ep;
                  slot.count = 1;
                  slot.sum = static_cast<double>(vals[r]);
                } else {
                  ++slot.count;
                  slot.sum += static_cast<double>(vals[r]);
                }
              }
            } else {
              for (uint32_t idx = 0; idx < m; ++idx) {
                const uint32_t r = sel_ptr[idx];
                const size_t gi =
                    ((static_cast<size_t>(static_cast<uint64_t>(keys0[r]) -
                                          static_cast<uint64_t>(key_min0)) *
                      s1) +
                     static_cast<size_t>(static_cast<uint64_t>(keys1[r]) -
                                         static_cast<uint64_t>(key_min1)));
                DirectCompactEpochSlot& slot = eslots[gi];
                if (slot.stamp != ep) {
                  slot.stamp = ep;
                  slot.count = 1;
                  slot.sum = static_cast<double>(vals[r]);
                } else {
                  ++slot.count;
                  slot.sum += static_cast<double>(vals[r]);
                }
              }
            }
          } else {
            for (uint32_t idx = 0; idx < m; ++idx) {
              const uint32_t r = all_pass ? idx : sel_ptr[idx];
              const size_t gi =
                  ((static_cast<size_t>(static_cast<uint64_t>(keys0[r]) -
                                        static_cast<uint64_t>(key_min0)) *
                    s1) +
                   static_cast<size_t>(static_cast<uint64_t>(keys1[r]) -
                                       static_cast<uint64_t>(key_min1)));
              DirectCompactEpochSlot& slot = eslots[gi];
              if (slot.stamp != ep) {
                slot.stamp = ep;
                slot.count = 1;
                slot.sum = vcol.Double(r);
              } else {
                ++slot.count;
                slot.sum += vcol.Double(r);
              }
            }
          }
          st.updates += m;
        } else {
          size_t upd = 0;
          for (uint32_t idx = 0; idx < m; ++idx) {
            const uint32_t r = all_pass ? idx : sel_ptr[idx];
            const size_t i0 =
                kcol0.IsNull(r)
                    ? span0
                    : static_cast<size_t>(static_cast<uint64_t>(keys0[r]) -
                                          static_cast<uint64_t>(key_min0));
            const size_t i1 =
                kcol1.IsNull(r)
                    ? span1
                    : static_cast<size_t>(static_cast<uint64_t>(keys1[r]) -
                                          static_cast<uint64_t>(key_min1));
            DirectCompactEpochSlot& slot = eslots[(i0 * s1) + i1];
            if (slot.stamp != ep) {
              slot.stamp = ep;
              slot.count = 0;
              slot.sum = 0.0;
            }
            if (!vcol.IsNull(r)) {
              ++slot.count;
              slot.sum +=
                  is_int_v ? static_cast<double>(vcol.Int(r)) : vcol.Double(r);
              ++upd;
            }
          }
          st.updates += upd;
        }
        continue;
      }
      st.EnsureAllocated(total_slots, bindings);
      st.matching_rows += m;
      const uint32_t* sel_ptr = sel.data();
      uint8_t* __restrict__ pres = st.present.data();
      row_gidx.resize(m);
      uint32_t* __restrict__ gidx = row_gidx.data();
      if (nkeys == 2) {
        const DirectBlockCol& kcol0 = ws.cols[key_full0];
        const DirectBlockCol& kcol1 = ws.cols[key_full1];
        const int64_t* keys0 = kcol0.cells;
        const int64_t* keys1 = kcol1.cells;
        if (!kcol0.has_null && !kcol1.has_null) {
          if (all_pass) {
            for (uint32_t r = 0; r < m; ++r) {
              const auto gi =
                  static_cast<uint32_t>(((static_cast<uint64_t>(keys0[r]) -
                                          static_cast<uint64_t>(key_min0)) *
                                         s1) +
                                        (static_cast<uint64_t>(keys1[r]) -
                                         static_cast<uint64_t>(key_min1)));
              gidx[r] = gi;
              pres[gi] = 1;
            }
          } else {
            for (uint32_t idx = 0; idx < m; ++idx) {
              const uint32_t r = sel_ptr[idx];
              const auto gi =
                  static_cast<uint32_t>(((static_cast<uint64_t>(keys0[r]) -
                                          static_cast<uint64_t>(key_min0)) *
                                         s1) +
                                        (static_cast<uint64_t>(keys1[r]) -
                                         static_cast<uint64_t>(key_min1)));
              gidx[idx] = gi;
              pres[gi] = 1;
            }
          }
        } else {
          for (uint32_t idx = 0; idx < m; ++idx) {
            const uint32_t r = all_pass ? idx : sel_ptr[idx];
            const size_t i0 =
                kcol0.IsNull(r)
                    ? span0
                    : static_cast<size_t>(static_cast<uint64_t>(keys0[r]) -
                                          static_cast<uint64_t>(key_min0));
            const size_t i1 =
                kcol1.IsNull(r)
                    ? span1
                    : static_cast<size_t>(static_cast<uint64_t>(keys1[r]) -
                                          static_cast<uint64_t>(key_min1));
            const auto gi = static_cast<uint32_t>((i0 * s1) + i1);
            gidx[idx] = gi;
            pres[gi] = 1;
          }
        }
      } else {
        const DirectBlockCol& kcol = ws.cols[key_full0];
        const int64_t* keys = kcol.cells;
        if (!kcol.has_null) {
          if (all_pass) {
            for (uint32_t r = 0; r < m; ++r) {
              const auto gi =
                  static_cast<uint32_t>(static_cast<uint64_t>(keys[r]) -
                                        static_cast<uint64_t>(key_min0));
              gidx[r] = gi;
              pres[gi] = 1;
            }
          } else {
            for (uint32_t idx = 0; idx < m; ++idx) {
              const uint32_t r = sel_ptr[idx];
              const auto gi =
                  static_cast<uint32_t>(static_cast<uint64_t>(keys[r]) -
                                        static_cast<uint64_t>(key_min0));
              gidx[idx] = gi;
              pres[gi] = 1;
            }
          }
        } else {
          const auto null_gi = static_cast<uint32_t>(span0);
          if (all_pass) {
            for (uint32_t r = 0; r < m; ++r) {
              const uint32_t gi =
                  kcol.IsNull(r)
                      ? null_gi
                      : static_cast<uint32_t>(static_cast<uint64_t>(keys[r]) -
                                              static_cast<uint64_t>(key_min0));
              gidx[r] = gi;
              pres[gi] = 1;
            }
          } else {
            for (uint32_t idx = 0; idx < m; ++idx) {
              const uint32_t r = sel_ptr[idx];
              const uint32_t gi =
                  kcol.IsNull(r)
                      ? null_gi
                      : static_cast<uint32_t>(static_cast<uint64_t>(keys[r]) -
                                              static_cast<uint64_t>(key_min0));
              gidx[idx] = gi;
              pres[gi] = 1;
            }
          }
        }
      }

      for (size_t a = 0; a < naggs; ++a) {
        const DirectAggBinding& b = bindings[a];
        int64_t* __restrict__ cnts = st.counts[a].data();
        if (b.is_count_star) {
          for (uint32_t i = 0; i < m; ++i) {
            ++cnts[gidx[i]];
          }
          st.updates += m;
          continue;
        }
        if (b.full_col.has_value()) {
          const DirectBlockCol& col = ws.cols[*b.full_col];
          if (b.kind == SlimAggKind::kCount) {
            if (!col.has_null) {
              for (uint32_t i = 0; i < m; ++i) {
                ++cnts[gidx[i]];
              }
            } else if (all_pass) {
              for (uint32_t r = 0; r < m; ++r) {
                cnts[gidx[r]] += static_cast<int64_t>(!col.IsNull(r));
              }
            } else {
              for (uint32_t i = 0; i < m; ++i) {
                cnts[gidx[i]] += static_cast<int64_t>(!col.IsNull(sel_ptr[i]));
              }
            }
            st.updates += m;
            continue;
          }
          if (b.kind == SlimAggKind::kSumDouble ||
              b.kind == SlimAggKind::kAvgDouble) {
            double* __restrict__ dtot = st.double_totals[a].data();
            uint8_t* flg = (b.kind == SlimAggKind::kSumDouble)
                               ? st.flags[a].data()
                               : nullptr;
            size_t upd = 0;
            const auto add_d = [&](uint32_t gi, double v) {
              dtot[gi] += v;
              ++cnts[gi];
              if (flg != nullptr && std::isinf(v)) {
                flg[gi] |= 1;
              }
              ++upd;
            };
            if (b.col_type == ValueType::kDouble) {
              if (!col.has_null) {
                if (all_pass) {
                  for (uint32_t r = 0; r < m; ++r) {
                    add_d(gidx[r], col.Double(r));
                  }
                } else {
                  for (uint32_t i = 0; i < m; ++i) {
                    add_d(gidx[i], col.Double(sel_ptr[i]));
                  }
                }
              } else if (all_pass) {
                for (uint32_t r = 0; r < m; ++r) {
                  if (!col.IsNull(r)) {
                    add_d(gidx[r], col.Double(r));
                  }
                }
              } else {
                for (uint32_t i = 0; i < m; ++i) {
                  const uint32_t r = sel_ptr[i];
                  if (!col.IsNull(r)) {
                    add_d(gidx[i], col.Double(r));
                  }
                }
              }
            } else {
              if (!col.has_null) {
                if (all_pass) {
                  for (uint32_t r = 0; r < m; ++r) {
                    add_d(gidx[r], static_cast<double>(col.Int(r)));
                  }
                } else {
                  for (uint32_t i = 0; i < m; ++i) {
                    add_d(gidx[i], static_cast<double>(col.Int(sel_ptr[i])));
                  }
                }
              } else if (all_pass) {
                for (uint32_t r = 0; r < m; ++r) {
                  if (!col.IsNull(r)) {
                    add_d(gidx[r], static_cast<double>(col.Int(r)));
                  }
                }
              } else {
                for (uint32_t i = 0; i < m; ++i) {
                  const uint32_t r = sel_ptr[i];
                  if (!col.IsNull(r)) {
                    add_d(gidx[i], static_cast<double>(col.Int(r)));
                  }
                }
              }
            }
            st.updates += upd;
            continue;
          }
          if (b.kind == SlimAggKind::kSumInt64) {
            int64_t* __restrict__ itot = st.int_totals[a].data();
            size_t upd = 0;
            const auto add_i = [&](uint32_t gi, int64_t v) -> bool {
              int64_t next = 0;
              if (__builtin_add_overflow(itot[gi], v, &next)) {
                st.err = StatusError(StatusCode::kIsInfinity,
                                     "integer overflow in SUM");
                return false;
              }
              itot[gi] = next;
              ++cnts[gi];
              ++upd;
              return true;
            };
            if (!col.has_null) {
              if (all_pass) {
                for (uint32_t r = 0; r < m; ++r) {
                  if (!add_i(gidx[r], col.Int(r))) {
                    return;
                  }
                }
              } else {
                for (uint32_t i = 0; i < m; ++i) {
                  if (!add_i(gidx[i], col.Int(sel_ptr[i]))) {
                    return;
                  }
                }
              }
            } else if (all_pass) {
              for (uint32_t r = 0; r < m; ++r) {
                if (!col.IsNull(r) && !add_i(gidx[r], col.Int(r))) {
                  return;
                }
              }
            } else {
              for (uint32_t i = 0; i < m; ++i) {
                const uint32_t r = sel_ptr[i];
                if (!col.IsNull(r) && !add_i(gidx[i], col.Int(r))) {
                  return;
                }
              }
            }
            st.updates += upd;
            continue;
          }
          if (b.kind == SlimAggKind::kMinInt64 ||
              b.kind == SlimAggKind::kMaxInt64 ||
              b.kind == SlimAggKind::kMinDate ||
              b.kind == SlimAggKind::kMaxDate) {
            const bool is_min = (b.kind == SlimAggKind::kMinInt64 ||
                                 b.kind == SlimAggKind::kMinDate);
            int64_t* __restrict__ itot = st.int_totals[a].data();
            size_t upd = 0;
            const auto upd_i = [&](uint32_t gi, int64_t v) {
              if (cnts[gi] == 0 || (is_min ? v < itot[gi] : itot[gi] < v)) {
                itot[gi] = v;
              }
              ++cnts[gi];
              ++upd;
            };
            if (!col.has_null) {
              if (all_pass) {
                for (uint32_t r = 0; r < m; ++r) {
                  upd_i(gidx[r], col.Int(r));
                }
              } else {
                for (uint32_t i = 0; i < m; ++i) {
                  upd_i(gidx[i], col.Int(sel_ptr[i]));
                }
              }
            } else if (all_pass) {
              for (uint32_t r = 0; r < m; ++r) {
                if (!col.IsNull(r)) {
                  upd_i(gidx[r], col.Int(r));
                }
              }
            } else {
              for (uint32_t i = 0; i < m; ++i) {
                const uint32_t r = sel_ptr[i];
                if (!col.IsNull(r)) {
                  upd_i(gidx[i], col.Int(r));
                }
              }
            }
            st.updates += upd;
            continue;
          }
          const bool is_min = (b.kind == SlimAggKind::kMinDouble);
          double* __restrict__ dtot = st.double_totals[a].data();
          uint8_t* __restrict__ flg = st.flags[a].data();
          size_t upd = 0;
          const auto upd_d = [&](uint32_t gi, double v) {
            if (std::isnan(v)) {
              flg[gi] |= 1;
            } else if (cnts[gi] == 0 ||
                       (is_min ? v < dtot[gi] : dtot[gi] < v)) {
              dtot[gi] = v;
            }
            ++cnts[gi];
            ++upd;
          };
          if (!col.has_null) {
            if (all_pass) {
              for (uint32_t r = 0; r < m; ++r) {
                upd_d(gidx[r], col.Double(r));
              }
            } else {
              for (uint32_t i = 0; i < m; ++i) {
                upd_d(gidx[i], col.Double(sel_ptr[i]));
              }
            }
          } else if (all_pass) {
            for (uint32_t r = 0; r < m; ++r) {
              if (!col.IsNull(r)) {
                upd_d(gidx[r], col.Double(r));
              }
            }
          } else {
            for (uint32_t i = 0; i < m; ++i) {
              const uint32_t r = sel_ptr[i];
              if (!col.IsNull(r)) {
                upd_d(gidx[i], col.Double(r));
              }
            }
          }
          st.updates += upd;
          continue;
        }

        if (b.double_expr != nullptr) {
          const TypedDoubleExprInput& de = *b.double_expr;
          double* dtot = st.double_totals[a].data();
          uint8_t* flg = st.flags[a].data();
          size_t upd = 0;
          const auto handle_d = [&](uint32_t gi, double v) {
            if (b.kind == SlimAggKind::kCount) {
              ++cnts[gi];
            } else if (b.kind == SlimAggKind::kSumDouble ||
                       b.kind == SlimAggKind::kAvgDouble) {
              dtot[gi] += v;
              if (b.kind == SlimAggKind::kSumDouble && std::isinf(v)) {
                flg[gi] |= 1;
              }
              ++cnts[gi];
            } else {
              const bool is_min = (b.kind == SlimAggKind::kMinDouble);
              if (std::isnan(v)) {
                flg[gi] |= 1;
              } else if (cnts[gi] == 0 ||
                         (is_min ? v < dtot[gi] : dtot[gi] < v)) {
                dtot[gi] = v;
              }
              ++cnts[gi];
            }
            ++upd;
          };
          for (uint32_t i = 0; i < m; ++i) {
            const uint32_t r = all_pass ? i : sel_ptr[i];
            double v = 0.0;
            if (EvalDirectDoubleExpr(de, proj_to_full, ws.cols, r, &v)) {
              handle_d(gidx[i], v);
            }
          }
          st.updates += upd;
          continue;
        }

        const TypedIntExprInput& ie = *b.int_expr;
        int64_t* itot = st.int_totals[a].data();
        double* dtot = st.double_totals[a].data();
        size_t upd = 0;
        for (uint32_t i = 0; i < m; ++i) {
          const uint32_t r = all_pass ? i : sel_ptr[i];
          int64_t v = 0;
          if (EvalDirectIntExpr(ie, proj_to_full, ws.cols, r, &v, &st.err)) {
            const uint32_t gi = gidx[i];
            if (b.kind == SlimAggKind::kCount) {
              ++cnts[gi];
            } else if (b.kind == SlimAggKind::kSumInt64) {
              int64_t next = 0;
              if (__builtin_add_overflow(itot[gi], v, &next)) {
                st.err = StatusError(StatusCode::kIsInfinity,
                                     "integer overflow in SUM");
                return;
              }
              itot[gi] = next;
              ++cnts[gi];
            } else if (b.kind == SlimAggKind::kAvgDouble) {
              dtot[gi] += static_cast<double>(v);
              ++cnts[gi];
            } else {
              const bool is_min = (b.kind == SlimAggKind::kMinInt64);
              if (cnts[gi] == 0 || (is_min ? v < itot[gi] : itot[gi] < v)) {
                itot[gi] = v;
              }
              ++cnts[gi];
            }
            ++upd;
          } else if (!st.err.ok()) {
            return;
          }
        }
        st.updates += upd;
      }
    }
  }));

  for (const auto& st : states) {
    if (!st.err.ok()) {
      return st.err;
    }
  }
  if (publish_lstable_marker && runtime != nullptr) {
    auto marker = std::make_shared<Relation>(runtime);
    marker->schema = qualified_schema;
    runtime->base_relations.emplace(fold_cache_key, std::move(marker));
  }

  const auto merge_begin = std::chrono::steady_clock::now();
  size_t total_scanned = 0;
  size_t total_matching = 0;
  size_t total_updates = 0;
  std::vector<size_t> active_workers;
  active_workers.reserve(workers_used);
  for (size_t w = 0; w < workers_used; ++w) {
    total_scanned += states[w].scanned_rows;
    total_matching += states[w].matching_rows;
    total_updates += states[w].updates;
    if (use_compact_epoch ? states[w].epoch_active
                          : !states[w].present.empty()) {
      active_workers.push_back(w);
    }
  }

  const bool parallel_having =
      having_expr == nullptr || !positional_having.empty();
  const size_t emit_workers = (parallel_having && total_slots >= 4096 &&
                               workers_used > 1 && !active_workers.empty())
                                  ? workers_used
                                  : 1;
  // Lean-backed (SemijoinFiltersAndGroupThresholds.lean:
  // FrequencyUpperBoundHavingPrune_sound): when HAVING enforces `COUNT(...) >
  // T` or `COUNT(...) >= T` and no sibling aggregate can raise a SUM(INT64)
  // overflow error, check the group's exact count first before
  // merging/finishing remaining aggregates or decoding keys.
  std::optional<size_t> having_freq_agg;
  int64_t having_min_freq = 0;
  if (::tinylamb::platform::GetEnv("TINYLAMB_DISABLE_HAVING_FREQ_PRUNE") ==
          nullptr &&
      !positional_having.empty() && naggs > 1) {
    bool has_int_sum = false;
    for (size_t a = 0; a < naggs; ++a) {
      if (bindings[a].kind == SlimAggKind::kSumInt64) {
        has_int_sum = true;
        break;
      }
    }
    if (!has_int_sum) {
      for (const PositionalHaving& clause : positional_having) {
        if (clause.rhs_is_agg || !clause.has_constant ||
            clause.constant.IsNull() ||
            clause.constant.type != ValueType::kInt64 ||
            clause.agg_index >= naggs ||
            bindings[clause.agg_index].kind != SlimAggKind::kCount) {
          continue;
        }
        int64_t req = 0;
        if (clause.op == BinaryOperation::kGreaterThan) {
          if (clause.constant.value.int_value ==
              std::numeric_limits<int64_t>::max()) {
            continue;
          }
          req = clause.constant.value.int_value + 1;
        } else if (clause.op == BinaryOperation::kGreaterThanEquals) {
          req = clause.constant.value.int_value;
        } else {
          continue;
        }
        if (!having_freq_agg || req > having_min_freq) {
          having_freq_agg = clause.agg_index;
          having_min_freq = req;
        }
      }
    }
  }

  std::vector<std::vector<Row>> emit_rows(emit_workers);
  std::vector<std::vector<int64_t>> emit_order_counts(
      count_tail_order ? emit_workers : 0);
  std::vector<size_t> emit_groups(emit_workers, 0);
  std::vector<Status> emit_errors(emit_workers, Status::kSuccess);

  RETURN_IF_FAIL(RunParallelTasks(emit_workers, [&](size_t ew) {
    try {
      if (active_workers.empty()) {
        return;
      }
      const size_t idx_begin = (total_slots * ew) / emit_workers;
      const size_t idx_end = (total_slots * (ew + 1)) / emit_workers;
      std::vector<Value> finished(naggs);
      Row representative;
      AggregateResultMap aggregate_results;
      bool map_initialized = false;
      auto& out = emit_rows[ew];
      for (size_t idx = idx_begin; idx < idx_end; ++idx) {
        int64_t order_count_val = 0;
        if (use_compact_epoch) {
          bool any = false;
          int64_t total_cnt = 0;
          double total_dbl = 0.0;
          for (const size_t w : active_workers) {
            const DirectCompactEpochBuffer& eb = *states[w].epoch_buf;
            const DirectCompactEpochSlot& slot = eb.slots[idx];
            if (slot.stamp == eb.epoch) {
              any = true;
              total_cnt += static_cast<int64_t>(slot.count);
              total_dbl += slot.sum;
            }
          }
          if (!any) {
            continue;
          }
          ++emit_groups[ew];
          order_count_val = total_cnt;
          finished[0] = FinishDirectState(bindings[0].kind, total_cnt, 0,
                                          total_dbl, 0, 0, &emit_errors[ew]);
          if (!emit_errors[ew].ok()) {
            return;
          }
        } else {
          bool any = false;
          for (const size_t w : active_workers) {
            if (states[w].present[idx] != 0) {
              any = true;
              break;
            }
          }
          if (!any) {
            continue;
          }
          ++emit_groups[ew];
          if (having_freq_agg) {
            int64_t freq = 0;
            const size_t fa = *having_freq_agg;
            for (const size_t w : active_workers) {
              freq += states[w].counts[fa][idx];
            }
            if (freq < having_min_freq) {
              continue;
            }
          }
          for (size_t a = 0; a < naggs; ++a) {
            const SlimAggKind kind = bindings[a].kind;
            int64_t total_cnt = 0;
            int64_t total_int = 0;
            double total_dbl = 0.0;
            uint8_t total_flags = 0;
            if (IsSlimMinMaxKind(kind)) {
              const bool is_min = (kind == SlimAggKind::kMinInt64 ||
                                   kind == SlimAggKind::kMinDate ||
                                   kind == SlimAggKind::kMinDouble);
              for (const size_t w : active_workers) {
                const int64_t wc = states[w].counts[a][idx];
                if (wc == 0) {
                  continue;
                }
                if (kind == SlimAggKind::kMinDouble ||
                    kind == SlimAggKind::kMaxDouble) {
                  total_flags |= states[w].flags[a][idx];
                  const double wv = states[w].double_totals[a][idx];
                  if ((states[w].flags[a][idx] & 1) == 0 &&
                      (total_cnt == 0 ||
                       (is_min ? wv < total_dbl : total_dbl < wv))) {
                    total_dbl = wv;
                  }
                } else {
                  const int64_t wv = states[w].int_totals[a][idx];
                  if (total_cnt == 0 ||
                      (is_min ? wv < total_int : total_int < wv)) {
                    total_int = wv;
                  }
                }
                total_cnt += wc;
              }
            } else {
              for (const size_t w : active_workers) {
                const int64_t wc = states[w].counts[a][idx];
                if (wc == 0) {
                  continue;
                }
                if (kind == SlimAggKind::kSumInt64) {
                  int64_t next = 0;
                  if (__builtin_add_overflow(
                          total_int, states[w].int_totals[a][idx], &next)) {
                    emit_errors[ew] = StatusError(StatusCode::kIsInfinity,
                                                  "integer overflow in SUM");
                    return;
                  }
                  total_int = next;
                } else if (kind == SlimAggKind::kSumDouble ||
                           kind == SlimAggKind::kAvgDouble) {
                  total_dbl += states[w].double_totals[a][idx];
                  if (!states[w].flags[a].empty()) {
                    total_flags |= states[w].flags[a][idx];
                  }
                }
                total_cnt += wc;
              }
            }
            if (count_tail_order && a == count_tail_agg_idx) {
              order_count_val = total_cnt;
            }
            finished[a] =
                FinishDirectState(kind, total_cnt, total_int, total_dbl,
                                  total_flags, 0, &emit_errors[ew]);
            if (!emit_errors[ew].ok()) {
              return;
            }
          }
        }
        auto decode_key_slot = [](size_t kidx, bool nullable, size_t kspan,
                                  int64_t kmin, ValueType ktype) -> Value {
          if (nullable && kidx == kspan) {
            return Value();
          }
          const int64_t kv = kmin + static_cast<int64_t>(kidx);
          return ktype == ValueType::kDate ? Value::DateFromDays(kv)
                                           : Value(kv);
        };
        const size_t idx0 = (nkeys == 2) ? (idx / s1) : idx;
        const size_t idx1 = (nkeys == 2) ? (idx % s1) : 0;
        const Value key_val =
            decode_key_slot(idx0, key_nullable0, span0, key_min0, key_type0);
        const Value key_val1 = (nkeys == 2)
                                   ? decode_key_slot(idx1, key_nullable1, span1,
                                                     key_min1, key_type1)
                                   : Value();
        if (having_expr != nullptr) {
          if (!positional_having.empty()) {
            bool keep_group = true;
            for (const PositionalHaving& clause : positional_having) {
              const Value* right_ptr =
                  clause.rhs_is_agg ? &finished[clause.rhs_agg_index] : nullptr;
              keep_group =
                  EvalPositionalHavingValues(clause, finished[clause.agg_index],
                                             right_ptr, &emit_errors[ew]);
              if (!emit_errors[ew].ok()) {
                return;
              }
              if (!keep_group) {
                break;
              }
            }
            if (!keep_group) {
              continue;
            }
          } else {
            if (!map_initialized) {
              representative = Row(
                  std::vector<Value>(qualified_schema.ColumnCount(), Value()));
              aggregate_results.reserve(naggs);
              for (size_t a = 0; a < naggs; ++a) {
                aggregate_results.emplace(plan.aggregates[a].aggregate,
                                          finished[a]);
              }
              map_initialized = true;
            } else {
              for (size_t a = 0; a < naggs; ++a) {
                aggregate_results[plan.aggregates[a].aggregate] = finished[a];
              }
            }
            representative.values_[key_proj0] = key_val;
            Scope scope{.row = &representative,
                        .schema = &qualified_schema,
                        .outer = nullptr};
            scope.aggregates = &aggregate_results;
            StatusOr<Value> having_value = TryEvaluate(
                having_expr, scope, &aggregate_results, context, ctes);
            if (!having_value.HasValue()) {
              emit_errors[ew] = having_value.GetStatus();
              return;
            }
            if (!Truthy(having_value.MoveValue())) {
              continue;
            }
          }
        }
        std::vector<Value> row_values;
        row_values.reserve(plan.emit_items.size());
        for (const TypedEmitItem& item : plan.emit_items) {
          if (item.constant) {
            row_values.push_back(*item.constant);
          } else if (item.is_key) {
            row_values.push_back(item.index == 0 ? key_val : key_val1);
          } else {
            row_values.push_back(finished[item.index]);
          }
        }
        out.emplace_back(std::move(row_values));
        if (count_tail_order) {
          emit_order_counts[ew].push_back(order_count_val);
        }
      }
    } catch (const std::exception& error) {
      emit_errors[ew] = StatusError(StatusCode::kRuntimeError, error.what());
    } catch (...) {
      emit_errors[ew] =
          StatusError(StatusCode::kRuntimeError, "bounded int emit failed");
    }
  }));

  for (const Status& err : emit_errors) {
    if (!err.ok()) {
      return err;
    }
  }

  std::vector<Column> output_columns;
  output_columns.reserve(stmt.SelectList().size());
  for (size_t i = 0; i < stmt.SelectList().size(); ++i) {
    output_columns.emplace_back(
        ProjectionName(stmt.SelectList()[i], i),
        ValueTypeFromTypeTag(stmt.SelectList()[i]
                                 .expression->ResultType(qualified_schema)
                                 .GetType()));
  }
  Relation output(runtime);
  size_t total_out_rows = 0;
  size_t formed_groups = 0;
  for (size_t ew = 0; ew < emit_workers; ++ew) {
    total_out_rows += emit_rows[ew].size();
    formed_groups += emit_groups[ew];
  }
  output.rows.reserve(total_out_rows);
  if (count_tail_order && total_out_rows > 1) {
    // Lean proof:
    // lean/TinylambOpt/ResearchProofs/AnalyticsWorkloads/HotKeyAndEpochAggregates.lean
    // (CountTail_sound): bucket counts <= kCountTailThreshold directly in O(N)
    // and only sort the small head with count > kCountTailThreshold.
    constexpr int64_t kCountTailThreshold = 4096;
    std::array<size_t, static_cast<size_t>(kCountTailThreshold) + 1>
        bucket_counts{};
    struct HeadEntry {
      int64_t count{0};
      size_t ew{0};
      size_t pos{0};
    };
    std::vector<HeadEntry> head_entries;
    for (size_t ew = 0; ew < emit_workers; ++ew) {
      const auto& cnts = emit_order_counts[ew];
      for (size_t p = 0; p < cnts.size(); ++p) {
        const int64_t c = cnts[p];
        if (c >= 0 && c <= kCountTailThreshold) {
          ++bucket_counts[static_cast<size_t>(c)];
        } else {
          head_entries.push_back({.count = c, .ew = ew, .pos = p});
        }
      }
    }
    std::ranges::stable_sort(head_entries,
                             [](const HeadEntry& a, const HeadEntry& b) {
                               return a.count > b.count;
                             });
    std::array<size_t, static_cast<size_t>(kCountTailThreshold) + 1>
        bucket_offsets{};
    size_t cursor = head_entries.size();
    for (size_t b = static_cast<size_t>(kCountTailThreshold) + 1; b-- > 0;) {
      bucket_offsets[b] = cursor;
      cursor += bucket_counts[b];
    }
    std::vector<Row> ordered_rows(total_out_rows);
    for (size_t i = 0; i < head_entries.size(); ++i) {
      ordered_rows[i] =
          std::move(emit_rows[head_entries[i].ew][head_entries[i].pos]);
    }
    for (size_t ew = 0; ew < emit_workers; ++ew) {
      const auto& cnts = emit_order_counts[ew];
      auto& er = emit_rows[ew];
      for (size_t p = 0; p < cnts.size(); ++p) {
        const int64_t c = cnts[p];
        if (c >= 0 && c <= kCountTailThreshold) {
          const size_t dst = bucket_offsets[static_cast<size_t>(c)]++;
          ordered_rows[dst] = std::move(er[p]);
        }
      }
    }
    for (Row& r : ordered_rows) {
      output.AddRow(std::move(r));
    }
  } else {
    for (auto& er : emit_rows) {
      for (Row& r : er) {
        output.AddRow(std::move(r));
      }
    }
  }
  if (!output.rows.empty()) {
    for (size_t i = 0; i < output_columns.size(); ++i) {
      if (output.rows[0][i].IsNull()) {
        continue;
      }
      output_columns[i] =
          Column(output_columns[i].Name(), ValueTypeOf(output.rows[0][i]));
      output_columns[i].SetUnsigned(output.rows[0][i].IsUnsigned());
    }
  }
  output.schema = Schema("", std::move(output_columns));

  if (runtime != nullptr) {
    const double scan_ms = ElapsedMs(scan_begin);
    runtime->scan_ms += scan_ms;
    runtime->filter_ms += scan_ms;
    runtime->scan_rows += total_scanned;
    runtime->scan_output_rows += total_matching;
    runtime->scan_values_decoded +=
        total_scanned * qualified_schema.ColumnCount();
    runtime->scan_values_available +=
        total_scanned * table_schema.ColumnCount();
    runtime->typed_parallel_aggregations += 1;
    runtime->aggregate_groups += formed_groups;
    runtime->aggregate_input_rows += total_matching;
    runtime->aggregate_updates += total_updates;
    runtime->aggregate_ms += ElapsedMs(merge_begin);
  }
  return std::optional<Relation>(std::move(output));
}

}  // namespace tinylamb::relational_detail
