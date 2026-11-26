/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <span>
#include <string>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"
#include "executor/detail/columnar_tlcs_fast_path.hpp"
#include "executor/detail/columnar_tlcs_internal.hpp"

namespace tinylamb::relational_detail {
namespace {
// Lean proof: lean/TinylambOpt/TpchSf10VectorizedExec.lean
//   (filterColumnAtATime_eq_row_at_a_time, filter_skip_always_true,
//    range_fusion_filter_eq)
// Column-at-a-time conjunctive filter for non-null int64/date and double
// constant comparisons, with ZoneMap always-true elimination and same-column
// range fusion.
struct FastNumericPred {
  enum class Kind : uint8_t {
    kIntSingle = 0,
    kIntRange,
    kIntInMask64,
    kIntInList,
    kDoubleSingle,
    kDoubleRange,
  };
  Kind kind{Kind::kIntSingle};
  slot_t col{0};
  const int64_t* cells{nullptr};
  BinaryOperation op{BinaryOperation::kEquals};
  int64_t int_lo{0};
  int64_t int_hi{0};
  uint64_t in_mask{0};
  const int64_t* in_vals{nullptr};
  size_t in_count{0};
  double dbl_lo{0.0};
  double dbl_hi{0.0};
  bool lo_inclusive{true};
  bool hi_inclusive{true};
  uint8_t priority{2};
};

template <typename PredFn>
void ApplyFirstPred(uint32_t rc, std::vector<uint32_t>* cand_rows,
                    PredFn&& pred) noexcept {
  cand_rows->resize(rc);
  uint32_t* out = cand_rows->data();
  size_t k = 0;
  for (uint32_t r = 0; r < rc; ++r) {
    out[k] = r;
    k += static_cast<size_t>(pred(r));
  }
  cand_rows->resize(k);
}

template <typename PredFn>
void RefinePredInPlace(std::vector<uint32_t>* cand_rows,
                       PredFn&& pred) noexcept {
  uint32_t* data = cand_rows->data();
  const size_t n = cand_rows->size();
  size_t out = 0;
  for (size_t i = 0; i < n; ++i) {
    const uint32_t r = data[i];
    data[out] = r;
    out += static_cast<size_t>(pred(r));
  }
  cand_rows->resize(out);
}

void RunFastNumericPred(const FastNumericPred& p, uint32_t rc, bool is_first,
                        std::vector<uint32_t>* cand_rows) noexcept {
  const int64_t* cells = p.cells;
  auto dispatch = [&](auto&& fn) {
    if (is_first) {
      ApplyFirstPred(rc, cand_rows, fn);
    } else {
      RefinePredInPlace(cand_rows, fn);
    }
  };
  switch (p.kind) {
    case FastNumericPred::Kind::kIntSingle: {
      const int64_t c = p.int_lo;
      switch (p.op) {
        case BinaryOperation::kEquals:
          dispatch([cells, c](uint32_t r) noexcept { return cells[r] == c; });
          return;
        case BinaryOperation::kNotEquals:
          dispatch([cells, c](uint32_t r) noexcept { return cells[r] != c; });
          return;
        case BinaryOperation::kLessThan:
          dispatch([cells, c](uint32_t r) noexcept { return cells[r] < c; });
          return;
        case BinaryOperation::kLessThanEquals:
          dispatch([cells, c](uint32_t r) noexcept { return cells[r] <= c; });
          return;
        case BinaryOperation::kGreaterThan:
          dispatch([cells, c](uint32_t r) noexcept { return cells[r] > c; });
          return;
        case BinaryOperation::kGreaterThanEquals:
          dispatch([cells, c](uint32_t r) noexcept { return cells[r] >= c; });
          return;
        default:
          return;
      }
    }
    case FastNumericPred::Kind::kIntRange: {
      const int64_t lo = p.int_lo;
      const int64_t hi = p.int_hi;
      if (p.lo_inclusive && p.hi_inclusive) {
        dispatch([cells, lo, hi](uint32_t r) noexcept {
          const int64_t v = cells[r];
          return v >= lo && v <= hi;
        });
      } else if (p.lo_inclusive && !p.hi_inclusive) {
        dispatch([cells, lo, hi](uint32_t r) noexcept {
          const int64_t v = cells[r];
          return v >= lo && v < hi;
        });
      } else if (!p.lo_inclusive && p.hi_inclusive) {
        dispatch([cells, lo, hi](uint32_t r) noexcept {
          const int64_t v = cells[r];
          return v > lo && v <= hi;
        });
      } else {
        dispatch([cells, lo, hi](uint32_t r) noexcept {
          const int64_t v = cells[r];
          return v > lo && v < hi;
        });
      }
      return;
    }
    case FastNumericPred::Kind::kIntInMask64: {
      const auto lo = static_cast<uint64_t>(p.int_lo);
      const uint64_t span = static_cast<uint64_t>(p.int_hi) - lo;
      const uint64_t mask = p.in_mask;
      dispatch([cells, lo, span, mask](uint32_t r) noexcept {
        const uint64_t off = static_cast<uint64_t>(cells[r]) - lo;
        return off <= span && ((mask >> off) & 1ULL) != 0ULL;
      });
      return;
    }
    case FastNumericPred::Kind::kIntInList: {
      const int64_t* vals = p.in_vals;
      const size_t n = p.in_count;
      const int64_t lo = p.int_lo;
      const int64_t hi = p.int_hi;
      dispatch([cells, vals, n, lo, hi](uint32_t r) noexcept {
        const int64_t v = cells[r];
        if (v < lo || v > hi) {
          return false;
        }
        for (size_t i = 0; i < n; ++i) {
          if (vals[i] == v) {
            return true;
          }
        }
        return false;
      });
      return;
    }
    case FastNumericPred::Kind::kDoubleSingle: {
      const double c = p.dbl_lo;
      switch (p.op) {
        case BinaryOperation::kEquals:
          dispatch([cells, c](uint32_t r) noexcept {
            return std::bit_cast<double>(cells[r]) == c;
          });
          return;
        case BinaryOperation::kNotEquals:
          dispatch([cells, c](uint32_t r) noexcept {
            return std::bit_cast<double>(cells[r]) != c;
          });
          return;
        case BinaryOperation::kLessThan:
          dispatch([cells, c](uint32_t r) noexcept {
            return std::bit_cast<double>(cells[r]) < c;
          });
          return;
        case BinaryOperation::kLessThanEquals:
          dispatch([cells, c](uint32_t r) noexcept {
            return std::bit_cast<double>(cells[r]) <= c;
          });
          return;
        case BinaryOperation::kGreaterThan:
          dispatch([cells, c](uint32_t r) noexcept {
            return std::bit_cast<double>(cells[r]) > c;
          });
          return;
        case BinaryOperation::kGreaterThanEquals:
          dispatch([cells, c](uint32_t r) noexcept {
            return std::bit_cast<double>(cells[r]) >= c;
          });
          return;
        default:
          return;
      }
    }
    case FastNumericPred::Kind::kDoubleRange: {
      const double lo = p.dbl_lo;
      const double hi = p.dbl_hi;
      if (p.lo_inclusive && p.hi_inclusive) {
        dispatch([cells, lo, hi](uint32_t r) noexcept {
          const auto v = std::bit_cast<double>(cells[r]);
          return v >= lo && v <= hi;
        });
      } else if (p.lo_inclusive && !p.hi_inclusive) {
        dispatch([cells, lo, hi](uint32_t r) noexcept {
          const auto v = std::bit_cast<double>(cells[r]);
          return v >= lo && v < hi;
        });
      } else if (!p.lo_inclusive && p.hi_inclusive) {
        dispatch([cells, lo, hi](uint32_t r) noexcept {
          const auto v = std::bit_cast<double>(cells[r]);
          return v > lo && v <= hi;
        });
      } else {
        dispatch([cells, lo, hi](uint32_t r) noexcept {
          const auto v = std::bit_cast<double>(cells[r]);
          return v > lo && v < hi;
        });
      }
      return;
    }
  }
}

bool TryFilterBlockNumericColumnAtATime(
    const ColumnarPredicateSet& preds,
    const std::vector<const TlcsFile::DecodedColumnChunk*>& col_ptrs,
    uint32_t rc, std::vector<uint32_t>* cand_rows) {
  const size_t total_preds = preds.compares.size() + preds.inlists.size();
  if (!preds.nulls.empty() || !preds.derived_compares.empty() ||
      total_preds == 0 || total_preds > 16) {
    return false;
  }
  // Lean proof:
  // lean/TinylambOpt/ResearchProofs/Wave8H2o.lean (InRangeSynopsis_sound)
  // lean/TinylambOpt/ResearchProofs/Wave9ClickBenchHighCardAndFilter.lean
  //   (MultiKeyInRangeZoneBlockSkip_sound,
  //   BlockConjunctionZoneBitmapPrune_sound)
  for (const TypedInListCompare& in : preds.inlists) {
    if (in.is_string || in.is_double || in.int_values.empty() ||
        col_ptrs[in.column] == nullptr || col_ptrs[in.column]->has_null ||
        col_ptrs[in.column]->is_unsigned ||
        col_ptrs[in.column]->int_cells.size() < rc) {
      return false;
    }
  }
  for (const TypedFilterCompare& cmp : preds.compares) {
    if (!cmp.constant_payload || cmp.string_payload ||
        col_ptrs[cmp.left] == nullptr || col_ptrs[cmp.left]->has_null) {
      return false;
    }
    switch (cmp.op) {
      case BinaryOperation::kEquals:
      case BinaryOperation::kNotEquals:
      case BinaryOperation::kLessThan:
      case BinaryOperation::kLessThanEquals:
      case BinaryOperation::kGreaterThan:
      case BinaryOperation::kGreaterThanEquals:
        break;
      default:
        return false;
    }
  }

  std::array<FastNumericPred, 16> fps{};
  size_t nf = 0;
  // Lean proof:
  // lean/TinylambOpt/ResearchProofs/Wave8H2o.lean (InRangeSynopsis_sound)
  // lean/TinylambOpt/ResearchProofs/Wave9ClickBenchHighCardAndFilter.lean
  //   (MultiKeyInRangeZoneBlockSkip_sound,
  //   BlockConjunctionZoneBitmapPrune_sound)
  for (const TypedInListCompare& in : preds.inlists) {
    const auto& column = *col_ptrs[in.column];
    int64_t imin = in.int_values[0];
    int64_t imax = in.int_values[0];
    for (const int64_t v : in.int_values) {
      imin = std::min(v, imin);
      imax = std::max(v, imax);
    }
    if (column.has_min_max && !column.is_unsigned) {
      if (column.max_int < imin || column.min_int > imax) {
        cand_rows->clear();
        return true;
      }
      if (column.min_int == column.max_int) {
        bool hit = false;
        for (const int64_t v : in.int_values) {
          if (v == column.min_int) {
            hit = true;
            break;
          }
        }
        if (!hit) {
          cand_rows->clear();
          return true;
        }
        continue;
      }
    }
    const uint64_t span =
        static_cast<uint64_t>(imax) - static_cast<uint64_t>(imin);
    if (span < 64ULL) {
      uint64_t mask = 0;
      for (const int64_t v : in.int_values) {
        mask |= (uint64_t{1}
                 << (static_cast<uint64_t>(v) - static_cast<uint64_t>(imin)));
      }
      fps[nf++] = FastNumericPred{
          .kind = FastNumericPred::Kind::kIntInMask64,
          .col = in.column,
          .cells = column.int_cells.data(),
          .int_lo = imin,
          .int_hi = imax,
          .in_mask = mask,
          .priority = 0,
      };
    } else {
      fps[nf++] = FastNumericPred{
          .kind = FastNumericPred::Kind::kIntInList,
          .col = in.column,
          .cells = column.int_cells.data(),
          .int_lo = imin,
          .int_hi = imax,
          .in_vals = in.int_values.data(),
          .in_count = in.int_values.size(),
          .priority = 1,
      };
    }
  }
  for (const TypedFilterCompare& cmp : preds.compares) {
    const auto& column = *col_ptrs[cmp.left];
    if (!cmp.is_double) {
      if (column.has_min_max && !column.is_unsigned) {
        const int64_t lo = column.min_int;
        const int64_t hi = column.max_int;
        const int64_t c = cmp.int_constant;
        const bool always_true =
            (cmp.op == BinaryOperation::kEquals && lo == c && hi == c) ||
            (cmp.op == BinaryOperation::kNotEquals && (hi < c || c < lo)) ||
            (cmp.op == BinaryOperation::kLessThan && hi < c) ||
            (cmp.op == BinaryOperation::kLessThanEquals && hi <= c) ||
            (cmp.op == BinaryOperation::kGreaterThan && lo > c) ||
            (cmp.op == BinaryOperation::kGreaterThanEquals && lo >= c);
        if (always_true) {
          continue;
        }
        const bool always_false =
            (cmp.op == BinaryOperation::kEquals && (c < lo || c > hi)) ||
            (cmp.op == BinaryOperation::kNotEquals && lo == c && hi == c) ||
            (cmp.op == BinaryOperation::kLessThan && lo >= c) ||
            (cmp.op == BinaryOperation::kLessThanEquals && lo > c) ||
            (cmp.op == BinaryOperation::kGreaterThan && hi <= c) ||
            (cmp.op == BinaryOperation::kGreaterThanEquals && hi < c);
        if (always_false) {
          cand_rows->clear();
          return true;
        }
      }
      const uint8_t prio =
          (cmp.op == BinaryOperation::kEquals && cmp.int_constant != 0)
              ? 0
              : (cmp.op == BinaryOperation::kEquals ? 1 : 3);
      fps[nf++] = FastNumericPred{
          .kind = FastNumericPred::Kind::kIntSingle,
          .col = cmp.left,
          .cells = column.int_cells.data(),
          .op = cmp.op,
          .int_lo = cmp.int_constant,
          .priority = prio,
      };
    } else {
      const double c = cmp.double_constant;
      if (column.has_double_min_max && !column.has_nan &&
          std::isfinite(column.min_double) &&
          std::isfinite(column.max_double) && !std::isnan(c)) {
        const double lo = column.min_double;
        const double hi = column.max_double;
        const bool always_true =
            (cmp.op == BinaryOperation::kEquals && lo == c && hi == c) ||
            (cmp.op == BinaryOperation::kNotEquals && (hi < c || c < lo)) ||
            (cmp.op == BinaryOperation::kLessThan && hi < c) ||
            (cmp.op == BinaryOperation::kLessThanEquals && hi <= c) ||
            (cmp.op == BinaryOperation::kGreaterThan && lo > c) ||
            (cmp.op == BinaryOperation::kGreaterThanEquals && lo >= c);
        if (always_true) {
          continue;
        }
        const bool always_false =
            (cmp.op == BinaryOperation::kEquals && (c < lo || c > hi)) ||
            (cmp.op == BinaryOperation::kNotEquals && lo == c && hi == c) ||
            (cmp.op == BinaryOperation::kLessThan && lo >= c) ||
            (cmp.op == BinaryOperation::kLessThanEquals && lo > c) ||
            (cmp.op == BinaryOperation::kGreaterThan && hi <= c) ||
            (cmp.op == BinaryOperation::kGreaterThanEquals && hi < c);
        if (always_false) {
          cand_rows->clear();
          return true;
        }
      }
      const uint8_t prio = (cmp.op == BinaryOperation::kEquals) ? 1 : 4;
      fps[nf++] = FastNumericPred{
          .kind = FastNumericPred::Kind::kDoubleSingle,
          .col = cmp.left,
          .cells = column.int_cells.data(),
          .op = cmp.op,
          .dbl_lo = c,
          .priority = prio,
      };
    }
  }

  if (nf == 0) {
    cand_rows->resize(rc);
    for (uint32_t r = 0; r < rc; ++r) {
      (*cand_rows)[r] = r;
    }
    return true;
  }

  // Fuse same-column (lower, upper) pairs into a single range predicate.
  std::array<FastNumericPred, 16> fused{};
  std::array<bool, 16> consumed{};
  size_t nfused = 0;
  for (size_t i = 0; i < nf; ++i) {
    if (consumed[i]) {
      continue;
    }
    const FastNumericPred& a = fps[i];
    const bool a_is_lo = (a.op == BinaryOperation::kGreaterThan ||
                          a.op == BinaryOperation::kGreaterThanEquals);
    const bool a_is_hi = (a.op == BinaryOperation::kLessThan ||
                          a.op == BinaryOperation::kLessThanEquals);
    bool merged = false;
    if (a_is_lo || a_is_hi) {
      for (size_t j = i + 1; j < nf; ++j) {
        if (consumed[j] || fps[j].col != a.col || fps[j].kind != a.kind) {
          continue;
        }
        const FastNumericPred& b = fps[j];
        const bool b_is_lo = (b.op == BinaryOperation::kGreaterThan ||
                              b.op == BinaryOperation::kGreaterThanEquals);
        const bool b_is_hi = (b.op == BinaryOperation::kLessThan ||
                              b.op == BinaryOperation::kLessThanEquals);
        if ((a_is_lo && b_is_hi) || (a_is_hi && b_is_lo)) {
          const FastNumericPred& lo_p = a_is_lo ? a : b;
          const FastNumericPred& hi_p = a_is_hi ? a : b;
          FastNumericPred rp = lo_p;
          rp.kind = (a.kind == FastNumericPred::Kind::kIntSingle)
                        ? FastNumericPred::Kind::kIntRange
                        : FastNumericPred::Kind::kDoubleRange;
          rp.int_hi = hi_p.int_lo;
          rp.dbl_hi = hi_p.dbl_lo;
          rp.lo_inclusive = (lo_p.op == BinaryOperation::kGreaterThanEquals);
          rp.hi_inclusive = (hi_p.op == BinaryOperation::kLessThanEquals);
          rp.priority = (a.kind == FastNumericPred::Kind::kIntSingle) ? 2 : 3;
          fused[nfused++] = rp;
          consumed[i] = true;
          consumed[j] = true;
          merged = true;
          break;
        }
      }
    }
    if (!merged) {
      fused[nfused++] = a;
      consumed[i] = true;
    }
  }

  std::sort(fused.begin(), fused.begin() + nfused,
            [](const FastNumericPred& a, const FastNumericPred& b) {
              return a.priority < b.priority;
            });

  for (size_t i = 0; i < nfused; ++i) {
    RunFastNumericPred(fused[i], rc, i == 0, cand_rows);
    if (cand_rows->empty()) {
      return true;
    }
  }
  return true;
}

}  // namespace

StatusOr<bool> DecodeAndFilterBlock(
    const TlcsFile& tlcs, uint32_t bidx, const Schema& table_schema,
    const ColumnarPredicateSet& preds, const ColumnarStageSchedule& sched,
    BlockDecodeScratch* scratch,
    std::vector<const TlcsFile::DecodedColumnChunk*>* col_ptrs,
    std::vector<uint32_t>* cand_rows, uint64_t* local_scanned) {
  for (const slot_t c : sched.stage1a_cols) {
    (*col_ptrs)[c] = nullptr;
  }
  for (const slot_t c : sched.stage1b_cols) {
    (*col_ptrs)[c] = nullptr;
  }
  for (const slot_t c : sched.stage1c_cols) {
    (*col_ptrs)[c] = nullptr;
  }
  for (const slot_t c : sched.stage2_cols) {
    (*col_ptrs)[c] = nullptr;
  }
  for (const slot_t c : sched.cond_cols) {
    (*col_ptrs)[c] = nullptr;
  }
  cand_rows->clear();

  auto decode_cond_cols_if_needed = [&]() -> Status {
    if (sched.cond_cols.empty()) {
      return Status::kSuccess;
    }
    std::vector<slot_t> needed;
    needed.reserve(sched.cond_cols.size());
    for (size_t ci = 0; ci < sched.cond_cols.size(); ++ci) {
      bool any_hit = false;
      for (const uint32_t r : *cand_rows) {
        if (RowMatchesCaseConditions(sched.cond_guards[ci], *col_ptrs, r)) {
          any_hit = true;
          break;
        }
      }
      if (any_hit) {
        needed.push_back(sched.cond_cols[ci]);
      }
    }
    if (!needed.empty()) {
      RETURN_IF_FAIL(tlcs.DecodeBlockColumnsWithOptions(
          bidx, needed, table_schema, &scratch->blk_cond, true,
          &scratch->io_cond, {}));
      for (size_t i = 0; i < needed.size(); ++i) {
        (*col_ptrs)[needed[i]] = &scratch->blk_cond.columns[i];
      }
    }
    return Status::kSuccess;
  };

  if (!sched.stage1a_cols.empty() && !sched.stage1b_cols.empty()) {
    // Read cheap filter columns before the wider LIKE inputs.
    RETURN_IF_FAIL(tlcs.DecodeBlockColumnsWithOptions(
        bidx, sched.stage1b_cols, table_schema, &scratch->blk1b, true,
        &scratch->io1b, sched.stage1b_len_only));
    *local_scanned += scratch->blk1b.row_count;
    for (size_t i = 0; i < sched.stage1b_cols.size(); ++i) {
      (*col_ptrs)[sched.stage1b_cols[i]] = &scratch->blk1b.columns[i];
    }
    // For a raw length-only gate, prove an empty block cheaply. On other
    // blocks let the literal kernel generate candidates and evaluate the
    // length comparison on those rows. Lean: literal_before_length_gate.
    bool refine_literal = true;
    if (sched.stage1b_cols.size() == 1 && sched.stage1b_len_only[0] != 0U &&
        std::ranges::any_of(preds.contains_prechecks, [](const auto& check) {
          return check.kernel.has_value();
        })) {
      const slot_t slot = sched.stage1b_cols[0];
      const auto& chunk = scratch->blk1b.columns[0];
      if (!chunk.has_null && !chunk.dict_encoded &&
          chunk.varchar_ends.size() == scratch->blk1b.row_count &&
          !chunk.varchar_ends.empty() &&
          std::ranges::any_of(
              preds.compares, [slot](const TypedFilterCompare& cmp) {
                return cmp.left == slot && cmp.constant_payload &&
                       cmp.string_payload &&
                       cmp.string_match ==
                           TypedFilterCompare::StringMatchKind::kExact &&
                       cmp.string_constant.empty() &&
                       cmp.op == BinaryOperation::kNotEquals;
              })) {
        if (chunk.varchar_ends.back() == 0 &&
            std::ranges::all_of(chunk.varchar_ends,
                                [](uint32_t end) { return end == 0; })) {
          return false;
        }
        refine_literal = false;
      }
    }
    if (refine_literal) {
      for (uint32_t r = 0; r < scratch->blk1b.row_count; ++r) {
        if (RowMatchesColPtrs(preds, *col_ptrs, r)) {
          cand_rows->push_back(r);
        }
      }
      if (cand_rows->empty()) {
        return false;
      }
    }

    RETURN_IF_FAIL(tlcs.DecodeBlockColumnsWithOptions(
        bidx, sched.stage1a_cols, table_schema, &scratch->blk1a, true,
        &scratch->io1a, {}));
    for (size_t i = 0; i < sched.stage1a_cols.size(); ++i) {
      (*col_ptrs)[sched.stage1a_cols[i]] = &scratch->blk1a.columns[i];
    }
    if (!TryJitContainsRows(preds, *col_ptrs, scratch->blk1a.row_count,
                            refine_literal, cand_rows)) {
      for (const auto& pc : preds.contains_prechecks) {
        const auto* ch = (*col_ptrs)[pc.full_col];
        if (ch != nullptr && !BlockPassesContainsPrecheck(*ch, pc.needle)) {
          cand_rows->clear();
          return false;
        }
      }
      if (refine_literal) {
        std::erase_if(*cand_rows, [&](uint32_t r) {
          return !RowMatchesColPtrs(preds, *col_ptrs, r);
        });
      } else {
        for (uint32_t r = 0; r < scratch->blk1a.row_count; ++r) {
          if (RowMatchesColPtrs(preds, *col_ptrs, r)) {
            cand_rows->push_back(r);
          }
        }
      }
    }
    if (cand_rows->empty()) {
      return false;
    }

    if (!sched.stage1c_cols.empty()) {
      RETURN_IF_FAIL(tlcs.DecodeBlockColumnsWithOptions(
          bidx, sched.stage1c_cols, table_schema, &scratch->blk1c, true,
          &scratch->io1c, {}, false, *cand_rows));
      for (size_t i = 0; i < sched.stage1c_cols.size(); ++i) {
        (*col_ptrs)[sched.stage1c_cols[i]] = &scratch->blk1c.columns[i];
      }
      std::vector<uint32_t> filtered1c;
      filtered1c.reserve(cand_rows->size());
      for (const uint32_t r : *cand_rows) {
        if (RowMatchesColPtrs(preds, *col_ptrs, r)) {
          filtered1c.push_back(r);
        }
      }
      cand_rows->swap(filtered1c);
      if (cand_rows->empty()) {
        return false;
      }
    }

    if (!sched.stage2_cols.empty()) {
      RETURN_IF_FAIL(tlcs.DecodeBlockColumnsWithOptions(
          bidx, sched.stage2_cols, table_schema, &scratch->blk2, true,
          &scratch->io2, sched.stage2_len_only));
      for (size_t i = 0; i < sched.stage2_cols.size(); ++i) {
        (*col_ptrs)[sched.stage2_cols[i]] = &scratch->blk2.columns[i];
      }
    }
    RETURN_IF_FAIL(decode_cond_cols_if_needed());
    return true;
  }

  if (!sched.stage1a_cols.empty()) {
    RETURN_IF_FAIL(tlcs.DecodeBlockColumnsWithOptions(
        bidx, sched.stage1a_cols, table_schema, &scratch->blk1a, true,
        &scratch->io1a, {}));
    *local_scanned += scratch->blk1a.row_count;
    for (size_t i = 0; i < sched.stage1a_cols.size(); ++i) {
      (*col_ptrs)[sched.stage1a_cols[i]] = &scratch->blk1a.columns[i];
    }
    if (!TryJitContainsRows(preds, *col_ptrs, scratch->blk1a.row_count, false,
                            cand_rows)) {
      for (const auto& pc : preds.contains_prechecks) {
        const auto* ch = (*col_ptrs)[pc.full_col];
        if (ch != nullptr && !BlockPassesContainsPrecheck(*ch, pc.needle)) {
          return false;
        }
      }
      for (uint32_t r = 0; r < scratch->blk1a.row_count; ++r) {
        if (RowMatchesColPtrs(preds, *col_ptrs, r)) {
          cand_rows->push_back(r);
        }
      }
    }
    if (cand_rows->empty()) {
      return false;
    }

    if (!sched.stage1c_cols.empty()) {
      RETURN_IF_FAIL(tlcs.DecodeBlockColumnsWithOptions(
          bidx, sched.stage1c_cols, table_schema, &scratch->blk1c, true,
          &scratch->io1c, {}, false, *cand_rows));
      for (size_t i = 0; i < sched.stage1c_cols.size(); ++i) {
        (*col_ptrs)[sched.stage1c_cols[i]] = &scratch->blk1c.columns[i];
      }
      std::vector<uint32_t> filtered1c;
      filtered1c.reserve(cand_rows->size());
      for (const uint32_t r : *cand_rows) {
        if (RowMatchesColPtrs(preds, *col_ptrs, r)) {
          filtered1c.push_back(r);
        }
      }
      cand_rows->swap(filtered1c);
      if (cand_rows->empty()) {
        return false;
      }
    }

    if (!sched.stage1b_cols.empty()) {
      RETURN_IF_FAIL(tlcs.DecodeBlockColumnsWithOptions(
          bidx, sched.stage1b_cols, table_schema, &scratch->blk1b, true,
          &scratch->io1b, sched.stage1b_len_only));
      for (size_t i = 0; i < sched.stage1b_cols.size(); ++i) {
        (*col_ptrs)[sched.stage1b_cols[i]] = &scratch->blk1b.columns[i];
      }
      std::vector<uint32_t> filtered;
      filtered.reserve(cand_rows->size());
      for (const uint32_t r : *cand_rows) {
        if (RowMatchesColPtrs(preds, *col_ptrs, r)) {
          filtered.push_back(r);
        }
      }
      cand_rows->swap(filtered);
      if (cand_rows->empty()) {
        return false;
      }
    }
    if (!sched.stage2_cols.empty()) {
      RETURN_IF_FAIL(tlcs.DecodeBlockColumnsWithOptions(
          bidx, sched.stage2_cols, table_schema, &scratch->blk2, true,
          &scratch->io2, sched.stage2_len_only));
      for (size_t i = 0; i < sched.stage2_cols.size(); ++i) {
        (*col_ptrs)[sched.stage2_cols[i]] = &scratch->blk2.columns[i];
      }
    }
    RETURN_IF_FAIL(decode_cond_cols_if_needed());
    return true;
  }

  if (!sched.stage1b_cols.empty()) {
    RETURN_IF_FAIL(tlcs.DecodeBlockColumnsWithOptions(
        bidx, sched.stage1b_cols, table_schema, &scratch->blk1b, true,
        &scratch->io1b, sched.stage1b_len_only));
    const uint32_t rc = scratch->blk1b.row_count;
    *local_scanned += rc;
    for (size_t i = 0; i < sched.stage1b_cols.size(); ++i) {
      (*col_ptrs)[sched.stage1b_cols[i]] = &scratch->blk1b.columns[i];
    }
    if (preds.Empty() || BlockSatisfiesAllColumnarPredicates(preds, *col_ptrs)) {
      cand_rows->resize(rc);
      for (uint32_t r = 0; r < rc; ++r) {
        (*cand_rows)[r] = r;
      }
    } else if (preds.nulls.empty() && preds.inlists.empty() &&
               preds.derived_compares.empty() && preds.compares.size() == 1 &&
               preds.compares[0].constant_payload &&
               preds.compares[0].string_payload &&
               preds.compares[0].op == BinaryOperation::kNotEquals &&
               preds.compares[0].string_constant.empty() &&
               (*col_ptrs)[preds.compares[0].left] != nullptr &&
               !(*col_ptrs)[preds.compares[0].left]->has_null) {
      const auto* ch = (*col_ptrs)[preds.compares[0].left];
      cand_rows->reserve(rc);
      if (!ch->dict_encoded && ch->varchar_ends.size() >= rc) {
        const uint32_t* ends = ch->varchar_ends.data();
        uint32_t prev = 0;
        for (uint32_t r = 0; r < rc; ++r) {
          const uint32_t end = ends[r];
          if (end != prev) {
            cand_rows->push_back(r);
          }
          prev = end;
        }
      } else {
        for (uint32_t r = 0; r < rc; ++r) {
          if (ch->VarcharLength(r) != 0) {
            cand_rows->push_back(r);
          }
        }
      }
    } else {
      if (!TryFilterBlockNumericColumnAtATime(preds, *col_ptrs, rc,
                                              cand_rows)) {
        for (uint32_t r = 0; r < rc; ++r) {
          if (RowMatchesColPtrs(preds, *col_ptrs, r)) {
            cand_rows->push_back(r);
          }
        }
      }
    }
    if (cand_rows->empty()) {
      return false;
    }
    if (!sched.stage2_cols.empty()) {
      RETURN_IF_FAIL(tlcs.DecodeBlockColumnsWithOptions(
          bidx, sched.stage2_cols, table_schema, &scratch->blk2, true,
          &scratch->io2, sched.stage2_len_only));
      for (size_t i = 0; i < sched.stage2_cols.size(); ++i) {
        (*col_ptrs)[sched.stage2_cols[i]] = &scratch->blk2.columns[i];
      }
    }
    RETURN_IF_FAIL(decode_cond_cols_if_needed());
    return true;
  }

  ASSIGN_OR_RETURN(uint32_t, rc, tlcs.BlockRowCount(bidx));
  *local_scanned += rc;
  cand_rows->resize(rc);
  for (uint32_t r = 0; r < rc; ++r) {
    (*cand_rows)[r] = r;
  }
  if (cand_rows->empty()) {
    return false;
  }
  if (!sched.stage2_cols.empty()) {
    RETURN_IF_FAIL(tlcs.DecodeBlockColumnsWithOptions(
        bidx, sched.stage2_cols, table_schema, &scratch->blk2, true,
        &scratch->io2, sched.stage2_len_only));
    for (size_t i = 0; i < sched.stage2_cols.size(); ++i) {
      (*col_ptrs)[sched.stage2_cols[i]] = &scratch->blk2.columns[i];
    }
  }
  RETURN_IF_FAIL(decode_cond_cols_if_needed());
  return true;
}
}  // namespace tinylamb::relational_detail
