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
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/platform/process.hpp"
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/columnar_tlcs_fast_path.hpp"
#include "executor/detail/columnar_tlcs_internal.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/timestamp_trunc_kernel.hpp"
#include "expression/array_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/unary_expression.hpp"
#include "table/table_statistics.hpp"
#include "transaction/transaction.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

// Resolves a backing TLCS store only when the table and transaction have a
// completely clean read snapshot with zero uncommitted/in-place deltas.
std::shared_ptr<const TlcsFile> ResolveCleanBackingTlcs(
    const Table& table, TransactionContext& ctx) noexcept {
  if (ctx.txn_.IsFailed() || table.RowsRewrittenInPlace()) {
    return nullptr;
  }
  if (!ctx.txn_.IsReadOnly() && !ctx.txn_.WriteSetEmpty()) {
    return nullptr;
  }
  const uint64_t generation = table.LayoutGeneration();
  if (generation == 0) {
    return nullptr;
  }
  if (ctx.txn_.HasAnyPageVersionChains()) {
    ctx.txn_.SweepCommittedVersionChains();
    if (ctx.txn_.HasAnyPageVersionChains()) {
      return nullptr;
    }
  }
  std::shared_ptr<const TlcsFile> tlcs = table.GetBackingTlcsStore();
  if (tlcs == nullptr || tlcs->LayoutGeneration() != generation ||
      tlcs->FirstPageId() != table.FirstPageId()) {
    return nullptr;
  }
  return tlcs;
}

// ============================================================================
// 1. Predicate Compilation, TLCS Directory Pruning, and Row Filtering
// ============================================================================

void FinalizePredicateMetadata(const TlcsFile& tlcs, const Schema& table_schema,
                               ColumnarPredicateSet* set) {
  const size_t ncols = table_schema.ColumnCount();
  set->col_referenced.assign(ncols, 0);
  set->col_needs_varchar_bytes.assign(ncols, 0);
  set->col_dict_mask_covers_const.assign(ncols, 0);
  set->contains_prechecks.clear();
  set->dict_masks.clear();

  for (const TypedFilterCompare& cmp : set->compares) {
    if (cmp.left < ncols) {
      set->col_referenced[cmp.left] = 1;
      if (table_schema.GetColumn(cmp.left).Type() == ValueType::kVarChar) {
        // `col = ''` and `col <> ''` only need string lengths!
        const bool is_empty_len_check =
            cmp.constant_payload && cmp.string_payload &&
            cmp.string_match == TypedFilterCompare::StringMatchKind::kExact &&
            cmp.string_constant.empty() &&
            (cmp.op == BinaryOperation::kEquals ||
             cmp.op == BinaryOperation::kNotEquals);
        if (!is_empty_len_check) {
          set->col_needs_varchar_bytes[cmp.left] = 1;
        }
        if (cmp.constant_payload && cmp.string_payload &&
            cmp.op == BinaryOperation::kLike &&
            cmp.string_match ==
                TypedFilterCompare::StringMatchKind::kContains &&
            !cmp.string_constant.empty()) {
          set->contains_prechecks.push_back({
              .full_col = cmp.left,
              .needle = cmp.string_constant,
              .kernel = tlcs.BlockCount() >= 128
                            ? JitLiteralSearch::Compile(cmp.string_constant)
                            : std::nullopt,
          });
        }
      }
    }
    if (!cmp.constant_payload && cmp.right < ncols) {
      set->col_referenced[cmp.right] = 1;
      if (table_schema.GetColumn(cmp.right).Type() == ValueType::kVarChar) {
        set->col_needs_varchar_bytes[cmp.right] = 1;
      }
    }
  }
  for (const TypedInListCompare& in : set->inlists) {
    if (in.column < ncols) {
      set->col_referenced[in.column] = 1;
      if (table_schema.GetColumn(in.column).Type() == ValueType::kVarChar) {
        set->col_needs_varchar_bytes[in.column] = 1;
      }
    }
  }
  for (const TypedNullCompare& nf : set->nulls) {
    if (nf.column < ncols) {
      set->col_referenced[nf.column] = 1;
    }
  }
  for (const DerivedZoneCompare& dc : set->derived_compares) {
    if (dc.col_a < ncols) {
      set->col_referenced[dc.col_a] = 1;
    }
    if (dc.col_b < ncols) {
      set->col_referenced[dc.col_b] = 1;
    }
  }

  // Precompute valid 256-bit code masks for columns that use a complete narrow
  // dictionary in TLCS so directory pruning runs in a few bitwise ANDs.
  const auto& dict_kinds = tlcs.DictKinds();
  const auto& dicts = tlcs.Dicts();
  for (slot_t c = 0; c < ncols && c < dict_kinds.size() && c < dicts.size();
       ++c) {
    if (set->col_referenced[c] == 0U ||
        dict_kinds[c] != TlcsDictKind::kCompleteNarrow ||
        std::ranges::any_of(set->nulls, [c](const TypedNullCompare& filter) {
          return filter.column == c && filter.is_null;
        })) {
      // Directory masks represent non-NULL codes only. The NULL metadata
      // above already determines which blocks can satisfy IS NULL.
      continue;
    }
    const LSTableColumnDictionary& dict = dicts[c];
    const ValueType vt = table_schema.GetColumn(c).Type();
    size_t entry_count = 0;
    if (vt == ValueType::kVarChar) {
      entry_count = dict.StringEntryCount();
    } else if (vt == ValueType::kDouble) {
      entry_count = dict.DoubleEntryCount();
    } else {
      entry_count = dict.IntEntryCount();
    }
    if (entry_count == 0 ||
        entry_count > LSTableColumnDictionary::kMaxEntries) {
      continue;
    }
    DictFilterMask mask{.full_col = c};
    for (size_t code = 0; code < entry_count; ++code) {
      bool ok = true;
      for (const TypedFilterCompare& cmp : set->compares) {
        if (cmp.left != c || !cmp.constant_payload) {
          continue;
        }
        if (vt == ValueType::kVarChar && cmp.string_payload) {
          if (!MatchTypedStringCompare(dict.StringEntryAt(code), cmp)) {
            ok = false;
            break;
          }
        } else if (vt == ValueType::kDouble && cmp.is_double) {
          const double v = dict.DoubleEntries()[code];
          bool matched = false;
          switch (cmp.op) {
            case BinaryOperation::kEquals:
              matched = v == cmp.double_constant;
              break;
            case BinaryOperation::kNotEquals:
              matched = v != cmp.double_constant;
              break;
            case BinaryOperation::kLessThan:
              matched = v < cmp.double_constant;
              break;
            case BinaryOperation::kLessThanEquals:
              matched = v <= cmp.double_constant;
              break;
            case BinaryOperation::kGreaterThan:
              matched = v > cmp.double_constant;
              break;
            case BinaryOperation::kGreaterThanEquals:
              matched = v >= cmp.double_constant;
              break;
            default:
              matched = true;
              break;
          }
          if (!matched) {
            ok = false;
            break;
          }
        } else if ((vt == ValueType::kInt64 || vt == ValueType::kDate) &&
                   !cmp.is_double && !cmp.string_payload) {
          const int64_t v = dict.IntEntryAt(code);
          bool matched = false;
          switch (cmp.op) {
            case BinaryOperation::kEquals:
              matched = v == cmp.int_constant;
              break;
            case BinaryOperation::kNotEquals:
              matched = v != cmp.int_constant;
              break;
            case BinaryOperation::kLessThan:
              matched = v < cmp.int_constant;
              break;
            case BinaryOperation::kLessThanEquals:
              matched = v <= cmp.int_constant;
              break;
            case BinaryOperation::kGreaterThan:
              matched = v > cmp.int_constant;
              break;
            case BinaryOperation::kGreaterThanEquals:
              matched = v >= cmp.int_constant;
              break;
            default:
              matched = true;
              break;
          }
          if (!matched) {
            ok = false;
            break;
          }
        }
      }
      if (ok) {
        for (const TypedInListCompare& in : set->inlists) {
          if (in.column != c) {
            continue;
          }
          if (in.is_string && vt == ValueType::kVarChar) {
            if (!MatchTypedStringInList(dict.StringEntryAt(code), in)) {
              ok = false;
              break;
            }
          } else if (in.is_double && vt == ValueType::kDouble) {
            const double v = dict.DoubleEntries()[code];
            if (std::ranges::find(in.double_values, v) ==
                in.double_values.end()) {
              ok = false;
              break;
            }
          } else if (!in.is_string && !in.is_double &&
                     (vt == ValueType::kInt64 || vt == ValueType::kDate)) {
            const int64_t v = dict.IntEntryAt(code);
            if (std::ranges::find(in.int_values, v) == in.int_values.end()) {
              ok = false;
              break;
            }
          }
        }
      }
      if (ok) {
        mask.allowed[code / 64] |= (uint64_t{1} << (code % 64));
      }
    }
    set->dict_masks.push_back(mask);
    set->col_dict_mask_covers_const[c] = 1U;
  }
}

namespace {

Expression UnwrapBoundParameterSlots(  // NOLINT(misc-no-recursion)
    const Expression& expr) {
  if (!expr || !ContainsParameterSlot(expr)) {
    return expr;
  }
  switch (expr->Type()) {
    case TypeTag::kConstantValue: {
      const auto* slot = dynamic_cast<const ParameterSlot*>(expr.get());
      if (slot == nullptr) {
        return expr;
      }
      return ConstantValueExp(slot->Evaluate(Row(), Schema()));
    }
    case TypeTag::kBinaryExp: {
      const auto& b = expr->AsBinaryExpression();
      return BinaryExpressionExp(UnwrapBoundParameterSlots(b.Left()), b.Op(),
                                 UnwrapBoundParameterSlots(b.Right()));
    }
    case TypeTag::kUnaryExp: {
      const auto& u = expr->AsUnaryExpression();
      return UnaryExpressionExp(UnwrapBoundParameterSlots(u.Child()), u.Op());
    }
    case TypeTag::kCastExp: {
      const auto& c = expr->AsCastExpression();
      return CastExpressionExp(UnwrapBoundParameterSlots(c.Child()),
                               c.TargetTypeName(), c.ReturnNullOnError());
    }
    case TypeTag::kCaseExp: {
      const auto& cs = expr->AsCaseExpression();
      std::vector<std::pair<Expression, Expression>> whens;
      whens.reserve(cs.when_clauses_.size());
      for (const auto& w : cs.when_clauses_) {
        whens.emplace_back(UnwrapBoundParameterSlots(w.first),
                           UnwrapBoundParameterSlots(w.second));
      }
      return CaseExpressionExp(std::move(whens),
                               UnwrapBoundParameterSlots(cs.else_clause_));
    }
    case TypeTag::kInExp: {
      const auto& in = expr->AsInExpression();
      std::vector<Expression> list;
      list.reserve(in.list_.size());
      for (const Expression& item : in.list_) {
        list.push_back(UnwrapBoundParameterSlots(item));
      }
      return InExpressionExp(UnwrapBoundParameterSlots(in.child_),
                             std::move(list));
    }
    case TypeTag::kFunctionCallExp: {
      const auto& fn = expr->AsFunctionCallExpression();
      std::vector<Expression> args;
      args.reserve(fn.Args().size());
      for (const Expression& arg : fn.Args()) {
        args.push_back(UnwrapBoundParameterSlots(arg));
      }
      return FunctionCallExp(fn.FuncName(), std::move(args));
    }
    case TypeTag::kArrayExp: {
      const auto& arr = expr->AsArrayExpression();
      std::vector<Expression> elems;
      elems.reserve(arr.Elements().size());
      for (const Expression& el : arr.Elements()) {
        elems.push_back(UnwrapBoundParameterSlots(el));
      }
      return ArrayExpressionExp(std::move(elems), arr.ElementSqlType());
    }
    default:
      return expr;
  }
}

// Lean proof:
//   lean/TinylambOpt/ResearchProofs/StorageExecution/DerivedZonesA.lean
//     (Product, SaturatingLoss, Distance, BucketSum: SoundInterval, SkipRange)
//   lean/TinylambOpt/ResearchProofs/StorageExecution/DerivedZonesB.lean
//     (WrappedSum: SoundInterval, SkipRange)
struct Int128Interval {
  __int128 lo{0};
  __int128 hi{0};
};

Int128Interval ComputeDerivedIntInterval(DerivedZoneCompare::Kernel kernel,
                                         int64_t a_lo, int64_t a_hi,
                                         int64_t b_lo, int64_t b_hi) noexcept {
  const auto al = static_cast<__int128>(a_lo);
  const auto ah = static_cast<__int128>(a_hi);
  const auto bl = static_cast<__int128>(b_lo);
  const auto bh = static_cast<__int128>(b_hi);
  switch (kernel) {
    case DerivedZoneCompare::Kernel::kAdd:
      return {.lo = al + bl, .hi = ah + bh};
    case DerivedZoneCompare::Kernel::kSub:
      return {.lo = al - bh, .hi = ah - bl};
    case DerivedZoneCompare::Kernel::kMul: {
      const __int128 p0 = al * bl;
      const __int128 p1 = al * bh;
      const __int128 p2 = ah * bl;
      const __int128 p3 = ah * bh;
      return {.lo = std::min({p0, p1, p2, p3}),
              .hi = std::max({p0, p1, p2, p3})};
    }
    case DerivedZoneCompare::Kernel::kAbsDiff: {
      // Distance.SoundInterval: gapMin = max(0, max(a.lo - b.hi, b.lo - a.hi)),
      // gapMax = max(a.hi - b.lo, b.hi - a.lo).
      const __int128 d1 = al - bh;
      const __int128 d2 = bl - ah;
      const auto lo = std::max<__int128>({0, d1, d2});
      const __int128 hi = std::max(ah - bl, bh - al);
      return {.lo = lo, .hi = hi};
    }
    case DerivedZoneCompare::Kernel::kFloorDiv: {
      // FloorRatio.SoundInterval (a >= 0, b > 0): [a.lo / b.hi, a.hi / b.lo].
      return {.lo = al / bh, .hi = ah / bl};
    }
    case DerivedZoneCompare::Kernel::kBitXor: {
      // BitwiseXor.SoundInterval (a >= 0, b >= 0):
      //   w = max(width(a.lo ^ a.hi), width(b.lo ^ b.hi))
      //   prefix = (a.lo ^ b.lo) & ~((1 << w) - 1)
      //   interval = [prefix, prefix | ((1 << w) - 1)]
      const auto ua_lo = static_cast<uint64_t>(a_lo);
      const auto ua_hi = static_cast<uint64_t>(a_hi);
      const auto ub_lo = static_cast<uint64_t>(b_lo);
      const auto ub_hi = static_cast<uint64_t>(b_hi);
      const unsigned w = std::max(std::bit_width(ua_lo ^ ua_hi),
                                  std::bit_width(ub_lo ^ ub_hi));
      const uint64_t mask =
          (w >= 64U) ? ~uint64_t{0} : ((uint64_t{1} << w) - 1ULL);
      const uint64_t base = (ua_lo ^ ub_lo) & ~mask;
      return {.lo = static_cast<__int128>(base),
              .hi = static_cast<__int128>(base | mask)};
    }
    case DerivedZoneCompare::Kernel::kMin:
      // CappedPair.SoundInterval: LEAST(a, b) in [min(a.lo, b.lo), min(a.hi,
      // b.hi)].
      return {.lo = std::min(al, bl), .hi = std::min(ah, bh)};
    case DerivedZoneCompare::Kernel::kMax:
      // CappedPair.SoundInterval: GREATEST(a, b) in [max(a.lo, b.lo), max(a.hi,
      // b.hi)].
      return {.lo = std::max(al, bl), .hi = std::max(ah, bh)};
    case DerivedZoneCompare::Kernel::kSaturatingSub:
      // SaturatingLoss.SoundInterval: GREATEST(a - b, 0) in
      // [max(0, a.lo - b.hi), max(0, a.hi - b.lo)].
      return {.lo = std::max<__int128>(0, al - bh),
              .hi = std::max<__int128>(0, ah - bl)};
  }
  return {};
}

bool AllBlocksIntDerivedOverflowFree(const TlcsFile& tlcs, slot_t col_a,
                                     slot_t col_b,
                                     DerivedZoneCompare::Kernel kernel) {
  constexpr auto kMinSafe =
      static_cast<__int128>(std::numeric_limits<int64_t>::min()) + 1;
  constexpr auto kMaxSafe =
      static_cast<__int128>(std::numeric_limits<int64_t>::max());
  const uint32_t groups = tlcs.DirGroupCount();
  for (uint32_t g = 0; g < groups; ++g) {
    auto grp = tlcs.GetDirGroup(g);
    if (!grp.HasValue()) {
      return false;
    }
    for (const TlcsFile::BlockDirEntry& blk : grp.Value()->blocks) {
      if (col_a >= blk.columns.size() || col_b >= blk.columns.size()) {
        return false;
      }
      const TlcsFile::ColumnMeta& ma = blk.columns[col_a];
      const TlcsFile::ColumnMeta& mb = blk.columns[col_b];
      if (ma.encoding == TlcsColumnEncoding::kConstNull ||
          mb.encoding == TlcsColumnEncoding::kConstNull) {
        continue;
      }
      if (!ma.has_min_max || !mb.has_min_max || ma.is_unsigned ||
          mb.is_unsigned) {
        return false;
      }
      if (kernel == DerivedZoneCompare::Kernel::kFloorDiv &&
          (ma.min_int < 0 || mb.min_int <= 0)) {
        return false;
      }
      if (kernel == DerivedZoneCompare::Kernel::kBitXor &&
          (ma.min_int < 0 || mb.min_int < 0)) {
        return false;
      }
      // For ABS(a - b) and GREATEST(a - b, 0), intermediate `a - b` must not
      // overflow int64_t.
      if (kernel == DerivedZoneCompare::Kernel::kAbsDiff ||
          kernel == DerivedZoneCompare::Kernel::kSaturatingSub) {
        const Int128Interval sub_iv = ComputeDerivedIntInterval(
            DerivedZoneCompare::Kernel::kSub, ma.min_int, ma.max_int,
            mb.min_int, mb.max_int);
        if (sub_iv.lo < kMinSafe || sub_iv.hi > kMaxSafe) {
          return false;
        }
      }
      const Int128Interval iv = ComputeDerivedIntInterval(
          kernel, ma.min_int, ma.max_int, mb.min_int, mb.max_int);
      if (iv.lo < kMinSafe || iv.hi > kMaxSafe) {
        return false;
      }
    }
  }
  return true;
}

std::optional<DerivedZoneCompare> TryCompileDerivedZoneCompare(
    const Expression& predicate, const TlcsFile& tlcs,
    const Schema& scan_schema, std::span<const slot_t> proj_to_full,
    const Schema& table_schema) {
  if (!predicate || predicate->Type() != TypeTag::kBinaryExp) {
    return std::nullopt;
  }
  const auto& bin = predicate->AsBinaryExpression();
  BinaryOperation op = bin.Op();
  switch (op) {
    case BinaryOperation::kEquals:
    case BinaryOperation::kNotEquals:
    case BinaryOperation::kLessThan:
    case BinaryOperation::kLessThanEquals:
    case BinaryOperation::kGreaterThan:
    case BinaryOperation::kGreaterThanEquals:
      break;
    default:
      return std::nullopt;
  }

  auto flip_op = [](BinaryOperation cmp_op) noexcept -> BinaryOperation {
    switch (cmp_op) {
      case BinaryOperation::kLessThan:
        return BinaryOperation::kGreaterThan;
      case BinaryOperation::kLessThanEquals:
        return BinaryOperation::kGreaterThanEquals;
      case BinaryOperation::kGreaterThan:
        return BinaryOperation::kLessThan;
      case BinaryOperation::kGreaterThanEquals:
        return BinaryOperation::kLessThanEquals;
      default:
        return cmp_op;
    }
  };

  Expression expr_side;
  Expression const_side;
  if (bin.Right() && bin.Right()->Type() == TypeTag::kConstantValue) {
    expr_side = bin.Left();
    const_side = bin.Right();
  } else if (bin.Left() && bin.Left()->Type() == TypeTag::kConstantValue) {
    expr_side = bin.Right();
    const_side = bin.Left();
    op = flip_op(op);
  } else {
    return std::nullopt;
  }
  if (!expr_side || !const_side) {
    return std::nullopt;
  }
  const Value& cv = const_side->AsConstantValue().GetValue();
  if (cv.IsNull() || cv.IsUnsigned() ||
      (cv.type != ValueType::kInt64 && cv.type != ValueType::kDouble)) {
    return std::nullopt;
  }
  if (cv.type == ValueType::kDouble && !std::isfinite(cv.value.double_value)) {
    return std::nullopt;
  }

  DerivedZoneCompare::Kernel kernel = DerivedZoneCompare::Kernel::kAdd;
  Expression lhs_col_expr;
  Expression rhs_col_expr;
  if (expr_side->Type() == TypeTag::kBinaryExp) {
    const auto& arith = expr_side->AsBinaryExpression();
    switch (arith.Op()) {
      case BinaryOperation::kAdd:
        kernel = DerivedZoneCompare::Kernel::kAdd;
        break;
      case BinaryOperation::kSubtract:
        kernel = DerivedZoneCompare::Kernel::kSub;
        break;
      case BinaryOperation::kMultiply:
        kernel = DerivedZoneCompare::Kernel::kMul;
        break;
      default:
        return std::nullopt;
    }
    lhs_col_expr = arith.Left();
    rhs_col_expr = arith.Right();
  } else if (expr_side->Type() == TypeTag::kFunctionCallExp) {
    const auto& fn = expr_side->AsFunctionCallExpression();
    if (fn.FuncName() == "abs" && fn.Args().size() == 1 && fn.Args()[0] &&
        fn.Args()[0]->Type() == TypeTag::kBinaryExp) {
      const auto& sub = fn.Args()[0]->AsBinaryExpression();
      if (sub.Op() != BinaryOperation::kSubtract) {
        return std::nullopt;
      }
      kernel = DerivedZoneCompare::Kernel::kAbsDiff;
      lhs_col_expr = sub.Left();
      rhs_col_expr = sub.Right();
    } else if (fn.FuncName() == "div" && fn.Args().size() == 2 &&
               fn.Args()[0] && fn.Args()[1]) {
      kernel = DerivedZoneCompare::Kernel::kFloorDiv;
      lhs_col_expr = fn.Args()[0];
      rhs_col_expr = fn.Args()[1];
    } else if (fn.FuncName() == "__bit_xor" && fn.Args().size() == 2 &&
               fn.Args()[0] && fn.Args()[1]) {
      kernel = DerivedZoneCompare::Kernel::kBitXor;
      lhs_col_expr = fn.Args()[0];
      rhs_col_expr = fn.Args()[1];
    } else if (fn.FuncName() == "least" && fn.Args().size() == 2 &&
               fn.Args()[0] && fn.Args()[1]) {
      kernel = DerivedZoneCompare::Kernel::kMin;
      lhs_col_expr = fn.Args()[0];
      rhs_col_expr = fn.Args()[1];
    } else if (fn.FuncName() == "greatest" && fn.Args().size() == 2 &&
               fn.Args()[0] && fn.Args()[1]) {
      auto is_zero_const = [](const Expression& e) -> bool {
        if (!e || e->Type() != TypeTag::kConstantValue) {
          return false;
        }
        const Value& v = e->AsConstantValue().GetValue();
        if (v.IsNull() || v.IsUnsigned()) {
          return false;
        }
        return (v.type == ValueType::kInt64 && v.value.int_value == 0) ||
               (v.type == ValueType::kDouble && v.value.double_value == 0.0);
      };
      auto match_sub = [](const Expression& e, Expression* l,
                          Expression* r) -> bool {
        if (!e || e->Type() != TypeTag::kBinaryExp) {
          return false;
        }
        const auto& sub = e->AsBinaryExpression();
        if (sub.Op() != BinaryOperation::kSubtract) {
          return false;
        }
        *l = sub.Left();
        *r = sub.Right();
        return true;
      };
      if ((is_zero_const(fn.Args()[1]) &&
           match_sub(fn.Args()[0], &lhs_col_expr, &rhs_col_expr)) ||
          (is_zero_const(fn.Args()[0]) &&
           match_sub(fn.Args()[1], &lhs_col_expr, &rhs_col_expr))) {
        kernel = DerivedZoneCompare::Kernel::kSaturatingSub;
      } else {
        kernel = DerivedZoneCompare::Kernel::kMax;
        lhs_col_expr = fn.Args()[0];
        rhs_col_expr = fn.Args()[1];
      }
    } else {
      return std::nullopt;
    }
  } else {
    return std::nullopt;
  }

  if (!lhs_col_expr || !rhs_col_expr ||
      lhs_col_expr->Type() != TypeTag::kColumnValue ||
      rhs_col_expr->Type() != TypeTag::kColumnValue) {
    return std::nullopt;
  }
  const auto off_a = LocalColumnOffset(
      scan_schema, lhs_col_expr->AsColumnValue().GetColumnName());
  const auto off_b = LocalColumnOffset(
      scan_schema, rhs_col_expr->AsColumnValue().GetColumnName());
  if (!off_a || !off_b || *off_a >= scan_schema.ColumnCount() ||
      *off_b >= scan_schema.ColumnCount()) {
    return std::nullopt;
  }
  const slot_t full_a = MapSlot(static_cast<slot_t>(*off_a), proj_to_full);
  const slot_t full_b = MapSlot(static_cast<slot_t>(*off_b), proj_to_full);
  if (full_a >= table_schema.ColumnCount() ||
      full_b >= table_schema.ColumnCount()) {
    return std::nullopt;
  }
  const Column& col_a = table_schema.GetColumn(full_a);
  const Column& col_b = table_schema.GetColumn(full_b);
  if (col_a.IsUnsigned() || col_b.IsUnsigned() ||
      col_a.Type() != col_b.Type()) {
    return std::nullopt;
  }
  const bool is_double = (col_a.Type() == ValueType::kDouble);
  if (!is_double && col_a.Type() != ValueType::kInt64) {
    return std::nullopt;
  }
  if (is_double && (kernel == DerivedZoneCompare::Kernel::kFloorDiv ||
                    kernel == DerivedZoneCompare::Kernel::kBitXor)) {
    return std::nullopt;
  }
  if (!is_double && cv.type != ValueType::kInt64) {
    return std::nullopt;
  }
  if (!is_double &&
      !AllBlocksIntDerivedOverflowFree(tlcs, full_a, full_b, kernel)) {
    return std::nullopt;
  }
  DerivedZoneCompare out;
  out.col_a = full_a;
  out.col_b = full_b;
  out.op = op;
  out.kernel = kernel;
  out.is_double = is_double;
  if (is_double) {
    out.double_constant = cv.type == ValueType::kDouble
                              ? cv.value.double_value
                              : static_cast<double>(cv.value.int_value);
  } else {
    out.int_constant = cv.value.int_value;
  }
  return out;
}

// Howard Hinnant's exact civil-calendar conversions matching `Value::Date`
// (days since 1970-01-01).
int64_t CivilFloorDiv(int64_t a, int64_t b) noexcept {
  return (a / b) - (((a % b != 0) && ((a < 0) != (b < 0))) ? 1 : 0);
}

int64_t DaysFromCivilDate(int64_t y, int64_t m, int64_t d) noexcept {
  y -= (m <= 2) ? 1 : 0;
  const int64_t era = CivilFloorDiv(y, 400);
  const int64_t yoe = y - (era * 400);
  const int64_t doy = (((153 * (m + (m > 2 ? -3 : 9))) + 2) / 5) + d - 1;
  const int64_t doe = (yoe * 365) + (yoe / 4) - (yoe / 100) + doy;
  return (era * 146097) + doe - 719468;
}

void CivilDateFromDays(int64_t z, int64_t* y, int64_t* m, int64_t* d) noexcept {
  z += 719468;
  const int64_t era = CivilFloorDiv(z, 146097);
  const int64_t doe = z - (era * 146097);
  const int64_t yoe =
      (doe - (doe / 1460) + (doe / 36524) - (doe / 146096)) / 365;
  const int64_t doy = doe - ((365 * yoe) + (yoe / 4) - (yoe / 100));
  const int64_t mp = ((5 * doy) + 2) / 153;
  *d = doy - (((153 * mp) + 2) / 5) + 1;
  *m = mp < 10 ? mp + 3 : mp - 9;
  *y = yoe + (era * 400) + (*m <= 2 ? 1 : 0);
}

// Lean proof:
//   lean/TinylambOpt/ResearchProofs/LayerContracts/CalendarRanges_All.lean
//     (Month.PredicateRange, Quarter.PredicateRange, CivilYear.PredicateRange,
//      Month.CoverBlocks, Quarter.CoverBlocks, CivilYear.CoverBlocks)
// Rewrites `DATE_TRUNC(date_col, 'YEAR'|'QUARTER'|'MONTH') = DATE '...'` into
// two raw DATE comparisons `date_col >= lo AND date_col < hi` so TLCS ZoneMap
// block pruning and branchless filtering apply directly.
bool TryCompileCalendarTruncPredicate(const Expression& predicate,
                                      const Schema& scan_schema,
                                      std::span<const slot_t> proj_to_full,
                                      const Schema& table_schema,
                                      std::vector<TypedFilterCompare>* out) {
  if (!predicate || predicate->Type() != TypeTag::kBinaryExp) {
    return false;
  }
  const auto& bin = predicate->AsBinaryExpression();
  if (bin.Op() != BinaryOperation::kEquals) {
    return false;
  }
  const Expression* fn_expr = nullptr;
  const Expression* const_expr = nullptr;
  if (bin.Left() && bin.Left()->Type() == TypeTag::kFunctionCallExp &&
      bin.Right() && bin.Right()->Type() == TypeTag::kConstantValue) {
    fn_expr = &bin.Left();
    const_expr = &bin.Right();
  } else if (bin.Right() && bin.Right()->Type() == TypeTag::kFunctionCallExp &&
             bin.Left() && bin.Left()->Type() == TypeTag::kConstantValue) {
    fn_expr = &bin.Right();
    const_expr = &bin.Left();
  } else {
    return false;
  }
  const auto& fn = (*fn_expr)->AsFunctionCallExpression();
  if (fn.FuncName() != "date_trunc" || fn.Args().size() != 2 || !fn.Args()[0] ||
      !fn.Args()[1] || fn.Args()[0]->Type() != TypeTag::kColumnValue ||
      fn.Args()[1]->Type() != TypeTag::kConstantValue) {
    return false;
  }
  const Value& unit_val = fn.Args()[1]->AsConstantValue().GetValue();
  const Value& target_val = (*const_expr)->AsConstantValue().GetValue();
  if (unit_val.IsNull() || unit_val.type != ValueType::kVarChar ||
      target_val.IsNull() || target_val.type != ValueType::kDate ||
      target_val.IsUnsigned()) {
    return false;
  }
  const auto off = LocalColumnOffset(
      scan_schema, fn.Args()[0]->AsColumnValue().GetColumnName());
  if (!off || *off >= scan_schema.ColumnCount()) {
    return false;
  }
  const slot_t full_col = MapSlot(static_cast<slot_t>(*off), proj_to_full);
  if (full_col >= table_schema.ColumnCount()) {
    return false;
  }
  const Column& col = table_schema.GetColumn(full_col);
  if (col.Type() != ValueType::kDate || col.IsUnsigned()) {
    return false;
  }
  std::string unit(unit_val.AsStringView());
  for (char& ch : unit) {
    if (ch >= 'a' && ch <= 'z') {
      ch = static_cast<char>(ch - 'a' + 'A');
    }
  }
  const int64_t target_days = target_val.value.int_value;
  int64_t y = 0;
  int64_t m = 0;
  int64_t d = 0;
  CivilDateFromDays(target_days, &y, &m, &d);
  if (y < 1 || y > 9998) {
    return false;
  }
  bool valid_bucket_start = false;
  int64_t hi_days = 0;
  if (unit == "YEAR") {
    valid_bucket_start = (m == 1 && d == 1);
    hi_days = DaysFromCivilDate(y + 1, 1, 1);
  } else if (unit == "QUARTER") {
    valid_bucket_start = (d == 1 && (m == 1 || m == 4 || m == 7 || m == 10));
    hi_days = (m == 10) ? DaysFromCivilDate(y + 1, 1, 1)
                        : DaysFromCivilDate(y, m + 3, 1);
  } else if (unit == "MONTH") {
    valid_bucket_start = (d == 1);
    hi_days = (m == 12) ? DaysFromCivilDate(y + 1, 1, 1)
                        : DaysFromCivilDate(y, m + 1, 1);
  } else {
    return false;
  }
  if (!valid_bucket_start) {
    // Non-aligned date literal can never match DATE_TRUNC(col, unit).
    out->push_back({.left = full_col,
                    .op = BinaryOperation::kGreaterThanEquals,
                    .is_double = false,
                    .constant_payload = true,
                    .int_constant = 1});
    out->push_back({.left = full_col,
                    .op = BinaryOperation::kLessThan,
                    .is_double = false,
                    .constant_payload = true,
                    .int_constant = 0});
    return true;
  }
  out->push_back({.left = full_col,
                  .op = BinaryOperation::kGreaterThanEquals,
                  .is_double = false,
                  .constant_payload = true,
                  .int_constant = target_days});
  out->push_back({.left = full_col,
                  .op = BinaryOperation::kLessThan,
                  .is_double = false,
                  .constant_payload = true,
                  .int_constant = hi_days});
  return true;
}

// Lean proof:
//   lean/TinylambOpt/ResearchProofs/LayerContracts/SeekIntervals_All.lean
//     (DistanceBand.ExactSeek, DistanceBand.Multiplicity)
// Rewrites `ABS(col - center) <= radius` (or `<`) into two raw comparisons
// `col >= center - radius AND col <= center + radius` after verifying all TLCS
// blocks are overflow-free on `col - center`.
bool TryCompileDistanceBandPredicate(const Expression& predicate,
                                     const TlcsFile& tlcs,
                                     const Schema& scan_schema,
                                     std::span<const slot_t> proj_to_full,
                                     const Schema& table_schema,
                                     std::vector<TypedFilterCompare>* out) {
  if (!predicate || predicate->Type() != TypeTag::kBinaryExp) {
    return false;
  }
  const auto& bin = predicate->AsBinaryExpression();
  BinaryOperation op = bin.Op();
  const Expression* abs_expr = nullptr;
  const Expression* rad_expr = nullptr;
  if (bin.Left() && bin.Left()->Type() == TypeTag::kFunctionCallExp &&
      bin.Right() && bin.Right()->Type() == TypeTag::kConstantValue) {
    abs_expr = &bin.Left();
    rad_expr = &bin.Right();
  } else if (bin.Right() && bin.Right()->Type() == TypeTag::kFunctionCallExp &&
             bin.Left() && bin.Left()->Type() == TypeTag::kConstantValue) {
    abs_expr = &bin.Right();
    rad_expr = &bin.Left();
    if (op == BinaryOperation::kGreaterThanEquals) {
      op = BinaryOperation::kLessThanEquals;
    } else if (op == BinaryOperation::kGreaterThan) {
      op = BinaryOperation::kLessThan;
    } else {
      return false;
    }
  } else {
    return false;
  }
  if (op != BinaryOperation::kLessThanEquals &&
      op != BinaryOperation::kLessThan) {
    return false;
  }
  const auto& fn = (*abs_expr)->AsFunctionCallExpression();
  if (fn.FuncName() != "abs" || fn.Args().size() != 1 || !fn.Args()[0] ||
      fn.Args()[0]->Type() != TypeTag::kBinaryExp) {
    return false;
  }
  const auto& sub = fn.Args()[0]->AsBinaryExpression();
  if (sub.Op() != BinaryOperation::kSubtract || !sub.Left() || !sub.Right()) {
    return false;
  }
  const Expression* col_expr = nullptr;
  const Expression* ctr_expr = nullptr;
  if (sub.Left()->Type() == TypeTag::kColumnValue &&
      sub.Right()->Type() == TypeTag::kConstantValue) {
    col_expr = &sub.Left();
    ctr_expr = &sub.Right();
  } else if (sub.Right()->Type() == TypeTag::kColumnValue &&
             sub.Left()->Type() == TypeTag::kConstantValue) {
    col_expr = &sub.Right();
    ctr_expr = &sub.Left();
  } else {
    return false;
  }
  const Value& rad_v = (*rad_expr)->AsConstantValue().GetValue();
  const Value& ctr_v = (*ctr_expr)->AsConstantValue().GetValue();
  if (rad_v.IsNull() || rad_v.type != ValueType::kInt64 || rad_v.IsUnsigned() ||
      ctr_v.IsNull() || ctr_v.type != ValueType::kInt64 || ctr_v.IsUnsigned()) {
    return false;
  }
  const auto off = LocalColumnOffset(
      scan_schema, (*col_expr)->AsColumnValue().GetColumnName());
  if (!off || *off >= scan_schema.ColumnCount()) {
    return false;
  }
  const slot_t full_col = MapSlot(static_cast<slot_t>(*off), proj_to_full);
  if (full_col >= table_schema.ColumnCount()) {
    return false;
  }
  const Column& col = table_schema.GetColumn(full_col);
  if (col.Type() != ValueType::kInt64 || col.IsUnsigned()) {
    return false;
  }
  constexpr auto kMinSafe =
      static_cast<__int128>(std::numeric_limits<int64_t>::min()) + 1;
  constexpr auto kMaxSafe =
      static_cast<__int128>(std::numeric_limits<int64_t>::max());
  const auto center = static_cast<__int128>(ctr_v.value.int_value);
  const auto radius = static_cast<__int128>(rad_v.value.int_value);
  // Verify `col - center` cannot overflow int64_t on any TLCS block.
  const uint32_t groups = tlcs.DirGroupCount();
  for (uint32_t g = 0; g < groups; ++g) {
    auto grp = tlcs.GetDirGroup(g);
    if (!grp.HasValue()) {
      return false;
    }
    for (const TlcsFile::BlockDirEntry& blk : grp.Value()->blocks) {
      if (full_col >= blk.columns.size()) {
        return false;
      }
      const TlcsFile::ColumnMeta& m = blk.columns[full_col];
      if (m.encoding == TlcsColumnEncoding::kConstNull) {
        continue;
      }
      if (!m.has_min_max || m.is_unsigned) {
        return false;
      }
      const __int128 d_lo = static_cast<__int128>(m.min_int) - center;
      const __int128 d_hi = static_cast<__int128>(m.max_int) - center;
      if (d_lo < kMinSafe || d_lo > kMaxSafe || d_hi < kMinSafe ||
          d_hi > kMaxSafe) {
        return false;
      }
    }
  }
  const bool inc = (op == BinaryOperation::kLessThanEquals);
  if (radius < 0 || (radius == 0 && !inc)) {
    out->push_back({.left = full_col,
                    .op = BinaryOperation::kGreaterThanEquals,
                    .is_double = false,
                    .constant_payload = true,
                    .int_constant = 1});
    out->push_back({.left = full_col,
                    .op = BinaryOperation::kLessThan,
                    .is_double = false,
                    .constant_payload = true,
                    .int_constant = 0});
    return true;
  }
  const __int128 lo = center - radius;
  const __int128 hi = center + radius;
  if (lo < static_cast<__int128>(std::numeric_limits<int64_t>::min()) ||
      hi > kMaxSafe) {
    return false;
  }
  out->push_back({.left = full_col,
                  .op = inc ? BinaryOperation::kGreaterThanEquals
                            : BinaryOperation::kGreaterThan,
                  .is_double = false,
                  .constant_payload = true,
                  .int_constant = static_cast<int64_t>(lo)});
  out->push_back({.left = full_col,
                  .op = inc ? BinaryOperation::kLessThanEquals
                            : BinaryOperation::kLessThan,
                  .is_double = false,
                  .constant_payload = true,
                  .int_constant = static_cast<int64_t>(hi)});
  return true;
}

// Lean proof:
//   lean/TinylambOpt/ResearchProofs/LayerContracts/SeekIntervals_All.lean
//     (QuotientBand.ExactSeek, QuotientBand.Multiplicity)
// Rewrites `DIV(col, d) <op> q` (for nonnegative int64 `col` verified across
// all TLCS blocks and positive constant divisor `d > 0`) into raw range
// comparisons on `col` so TLCS ZoneMap pruning and SIMD scans apply directly.
bool TryCompileQuotientBandPredicate(const Expression& predicate,
                                     const TlcsFile& tlcs,
                                     const Schema& scan_schema,
                                     std::span<const slot_t> proj_to_full,
                                     const Schema& table_schema,
                                     std::vector<TypedFilterCompare>* out) {
  if (!predicate || predicate->Type() != TypeTag::kBinaryExp) {
    return false;
  }
  const auto& bin = predicate->AsBinaryExpression();
  BinaryOperation op = bin.Op();
  const Expression* div_expr = nullptr;
  const Expression* quot_expr = nullptr;
  if (bin.Left() && bin.Left()->Type() == TypeTag::kFunctionCallExp &&
      bin.Right() && bin.Right()->Type() == TypeTag::kConstantValue) {
    div_expr = &bin.Left();
    quot_expr = &bin.Right();
  } else if (bin.Right() && bin.Right()->Type() == TypeTag::kFunctionCallExp &&
             bin.Left() && bin.Left()->Type() == TypeTag::kConstantValue) {
    div_expr = &bin.Right();
    quot_expr = &bin.Left();
    if (op == BinaryOperation::kLessThan) {
      op = BinaryOperation::kGreaterThan;
    } else if (op == BinaryOperation::kLessThanEquals) {
      op = BinaryOperation::kGreaterThanEquals;
    } else if (op == BinaryOperation::kGreaterThan) {
      op = BinaryOperation::kLessThan;
    } else if (op == BinaryOperation::kGreaterThanEquals) {
      op = BinaryOperation::kLessThanEquals;
    }
  } else {
    return false;
  }
  if (op != BinaryOperation::kEquals && op != BinaryOperation::kLessThan &&
      op != BinaryOperation::kLessThanEquals &&
      op != BinaryOperation::kGreaterThan &&
      op != BinaryOperation::kGreaterThanEquals) {
    return false;
  }
  const auto& fn = (*div_expr)->AsFunctionCallExpression();
  if (fn.FuncName() != "div" || fn.Args().size() != 2 || !fn.Args()[0] ||
      !fn.Args()[1] || fn.Args()[0]->Type() != TypeTag::kColumnValue ||
      fn.Args()[1]->Type() != TypeTag::kConstantValue) {
    return false;
  }
  const Value& d_v = fn.Args()[1]->AsConstantValue().GetValue();
  const Value& q_v = (*quot_expr)->AsConstantValue().GetValue();
  if (d_v.IsNull() || d_v.type != ValueType::kInt64 || d_v.IsUnsigned() ||
      d_v.value.int_value <= 0 || q_v.IsNull() ||
      q_v.type != ValueType::kInt64 || q_v.IsUnsigned()) {
    return false;
  }
  const auto off = LocalColumnOffset(
      scan_schema, fn.Args()[0]->AsColumnValue().GetColumnName());
  if (!off || *off >= scan_schema.ColumnCount()) {
    return false;
  }
  const slot_t full_col = MapSlot(static_cast<slot_t>(*off), proj_to_full);
  if (full_col >= table_schema.ColumnCount()) {
    return false;
  }
  const Column& col = table_schema.GetColumn(full_col);
  if (col.Type() != ValueType::kInt64 || col.IsUnsigned()) {
    return false;
  }
  // Verify all non-null TLCS blocks have min_int >= 0 (Nat domain).
  const uint32_t groups = tlcs.DirGroupCount();
  for (uint32_t g = 0; g < groups; ++g) {
    auto grp = tlcs.GetDirGroup(g);
    if (!grp.HasValue()) {
      return false;
    }
    for (const TlcsFile::BlockDirEntry& blk : grp.Value()->blocks) {
      if (full_col >= blk.columns.size()) {
        return false;
      }
      const TlcsFile::ColumnMeta& m = blk.columns[full_col];
      if (m.encoding == TlcsColumnEncoding::kConstNull) {
        continue;
      }
      if (!m.has_min_max || m.is_unsigned || m.min_int < 0) {
        return false;
      }
    }
  }
  const auto d = static_cast<__int128>(d_v.value.int_value);
  const auto q = static_cast<__int128>(q_v.value.int_value);
  constexpr auto kMaxI64 =
      static_cast<__int128>(std::numeric_limits<int64_t>::max());
  auto emit_impossible = [&]() {
    out->push_back({.left = full_col,
                    .op = BinaryOperation::kGreaterThanEquals,
                    .is_double = false,
                    .constant_payload = true,
                    .int_constant = 1});
    out->push_back({.left = full_col,
                    .op = BinaryOperation::kLessThan,
                    .is_double = false,
                    .constant_payload = true,
                    .int_constant = 0});
  };
  if (op == BinaryOperation::kEquals) {
    if (q < 0) {
      emit_impossible();
      return true;
    }
    const __int128 lo = q * d;
    if (lo > kMaxI64) {
      emit_impossible();
      return true;
    }
    const __int128 hi_inc = ((q + 1) * d) - 1;
    out->push_back({.left = full_col,
                    .op = BinaryOperation::kGreaterThanEquals,
                    .is_double = false,
                    .constant_payload = true,
                    .int_constant = static_cast<int64_t>(lo)});
    if (hi_inc <= kMaxI64) {
      out->push_back({.left = full_col,
                      .op = BinaryOperation::kLessThanEquals,
                      .is_double = false,
                      .constant_payload = true,
                      .int_constant = static_cast<int64_t>(hi_inc)});
    }
    return true;
  }
  if (op == BinaryOperation::kGreaterThanEquals ||
      op == BinaryOperation::kGreaterThan) {
    const __int128 eff_q = (op == BinaryOperation::kGreaterThan) ? (q + 1) : q;
    if (eff_q <= 0) {
      // Every non-null x >= 0 satisfies DIV(x, d) >= 0; keep a non-null check
      // via `x >= 0`.
      out->push_back({.left = full_col,
                      .op = BinaryOperation::kGreaterThanEquals,
                      .is_double = false,
                      .constant_payload = true,
                      .int_constant = 0});
      return true;
    }
    const __int128 lo = eff_q * d;
    if (lo > kMaxI64) {
      emit_impossible();
      return true;
    }
    out->push_back({.left = full_col,
                    .op = BinaryOperation::kGreaterThanEquals,
                    .is_double = false,
                    .constant_payload = true,
                    .int_constant = static_cast<int64_t>(lo)});
    return true;
  }
  // kLessThan / kLessThanEquals
  const __int128 eff_q = (op == BinaryOperation::kLessThan) ? (q - 1) : q;
  if (eff_q < 0) {
    emit_impossible();
    return true;
  }
  const __int128 hi_inc = ((eff_q + 1) * d) - 1;
  if (hi_inc > kMaxI64) {
    out->push_back({.left = full_col,
                    .op = BinaryOperation::kGreaterThanEquals,
                    .is_double = false,
                    .constant_payload = true,
                    .int_constant = 0});
    return true;
  }
  out->push_back({.left = full_col,
                  .op = BinaryOperation::kLessThanEquals,
                  .is_double = false,
                  .constant_payload = true,
                  .int_constant = static_cast<int64_t>(hi_inc)});
  return true;
}

// Lean proof:
//   lean/TinylambOpt/ResearchProofs/LayerContracts/SeekIntervals_All.lean
//     (MinClamp.ExactSeek, MinClamp.EmptyAbove, MinClamp.PlateauStart)
// Rewrites `LEAST(col, a) <op> b` (`=`, `>`, `>=`) and `GREATEST(col, a) <op>
// b` (`=`, `<`, `<=`) into exact raw comparisons on `col`.
bool TryCompileMinClampPredicate(const Expression& predicate,
                                 const Schema& scan_schema,
                                 std::span<const slot_t> proj_to_full,
                                 const Schema& table_schema,
                                 std::vector<TypedFilterCompare>* out) {
  if (!predicate || predicate->Type() != TypeTag::kBinaryExp) {
    return false;
  }
  const auto& bin = predicate->AsBinaryExpression();
  BinaryOperation op = bin.Op();
  const Expression* fn_expr = nullptr;
  const Expression* rhs_expr = nullptr;
  if (bin.Left() && bin.Left()->Type() == TypeTag::kFunctionCallExp &&
      bin.Right() && bin.Right()->Type() == TypeTag::kConstantValue) {
    fn_expr = &bin.Left();
    rhs_expr = &bin.Right();
  } else if (bin.Right() && bin.Right()->Type() == TypeTag::kFunctionCallExp &&
             bin.Left() && bin.Left()->Type() == TypeTag::kConstantValue) {
    fn_expr = &bin.Right();
    rhs_expr = &bin.Left();
    if (op == BinaryOperation::kLessThan) {
      op = BinaryOperation::kGreaterThan;
    } else if (op == BinaryOperation::kLessThanEquals) {
      op = BinaryOperation::kGreaterThanEquals;
    } else if (op == BinaryOperation::kGreaterThan) {
      op = BinaryOperation::kLessThan;
    } else if (op == BinaryOperation::kGreaterThanEquals) {
      op = BinaryOperation::kLessThanEquals;
    }
  } else {
    return false;
  }
  const auto& fn = (*fn_expr)->AsFunctionCallExpression();
  const bool is_least = (fn.FuncName() == "least");
  const bool is_greatest = (fn.FuncName() == "greatest");
  if ((!is_least && !is_greatest) || fn.Args().size() != 2 || !fn.Args()[0] ||
      !fn.Args()[1]) {
    return false;
  }
  if (is_least && op != BinaryOperation::kEquals &&
      op != BinaryOperation::kGreaterThan &&
      op != BinaryOperation::kGreaterThanEquals) {
    return false;
  }
  if (is_greatest && op != BinaryOperation::kEquals &&
      op != BinaryOperation::kLessThan &&
      op != BinaryOperation::kLessThanEquals) {
    return false;
  }
  const Expression* col_expr = nullptr;
  const Expression* cap_expr = nullptr;
  if (fn.Args()[0]->Type() == TypeTag::kColumnValue &&
      fn.Args()[1]->Type() == TypeTag::kConstantValue) {
    col_expr = fn.Args().data();
    cap_expr = &fn.Args()[1];
  } else if (fn.Args()[1]->Type() == TypeTag::kColumnValue &&
             fn.Args()[0]->Type() == TypeTag::kConstantValue) {
    col_expr = &fn.Args()[1];
    cap_expr = fn.Args().data();
  } else {
    return false;
  }
  const auto off = LocalColumnOffset(
      scan_schema, (*col_expr)->AsColumnValue().GetColumnName());
  if (!off || *off >= scan_schema.ColumnCount()) {
    return false;
  }
  const slot_t full_col = MapSlot(static_cast<slot_t>(*off), proj_to_full);
  if (full_col >= table_schema.ColumnCount()) {
    return false;
  }
  const Column& col = table_schema.GetColumn(full_col);
  if (col.IsUnsigned() ||
      (col.Type() != ValueType::kInt64 && col.Type() != ValueType::kDouble)) {
    return false;
  }
  const Value& a_v = (*cap_expr)->AsConstantValue().GetValue();
  const Value& b_v = (*rhs_expr)->AsConstantValue().GetValue();
  if (a_v.IsNull() || b_v.IsNull() || a_v.IsUnsigned() || b_v.IsUnsigned()) {
    return false;
  }
  const bool is_double = (col.Type() == ValueType::kDouble);
  if (!is_double) {
    if (a_v.type != ValueType::kInt64 || b_v.type != ValueType::kInt64) {
      return false;
    }
    const int64_t a = a_v.value.int_value;
    const int64_t b = b_v.value.int_value;
    auto emit_impossible = [&]() {
      out->push_back({.left = full_col,
                      .op = BinaryOperation::kGreaterThan,
                      .is_double = false,
                      .constant_payload = true,
                      .int_constant = a});
      out->push_back({.left = full_col,
                      .op = BinaryOperation::kLessThan,
                      .is_double = false,
                      .constant_payload = true,
                      .int_constant = a});
    };
    if (is_least) {
      if (op == BinaryOperation::kEquals) {
        if (b > a) {
          emit_impossible();
        } else if (b == a) {
          out->push_back({.left = full_col,
                          .op = BinaryOperation::kGreaterThanEquals,
                          .is_double = false,
                          .constant_payload = true,
                          .int_constant = a});
        } else {
          out->push_back({.left = full_col,
                          .op = BinaryOperation::kEquals,
                          .is_double = false,
                          .constant_payload = true,
                          .int_constant = b});
        }
      } else if (op == BinaryOperation::kGreaterThan) {
        if (b >= a) {
          emit_impossible();
        } else {
          out->push_back({.left = full_col,
                          .op = BinaryOperation::kGreaterThan,
                          .is_double = false,
                          .constant_payload = true,
                          .int_constant = b});
        }
      } else {  // kGreaterThanEquals
        if (b > a) {
          emit_impossible();
        } else {
          out->push_back({.left = full_col,
                          .op = BinaryOperation::kGreaterThanEquals,
                          .is_double = false,
                          .constant_payload = true,
                          .int_constant = b});
        }
      }
      return true;
    }
    // is_greatest
    if (op == BinaryOperation::kEquals) {
      if (b < a) {
        emit_impossible();
      } else if (b == a) {
        out->push_back({.left = full_col,
                        .op = BinaryOperation::kLessThanEquals,
                        .is_double = false,
                        .constant_payload = true,
                        .int_constant = a});
      } else {
        out->push_back({.left = full_col,
                        .op = BinaryOperation::kEquals,
                        .is_double = false,
                        .constant_payload = true,
                        .int_constant = b});
      }
    } else if (op == BinaryOperation::kLessThan) {
      if (b <= a) {
        emit_impossible();
      } else {
        out->push_back({.left = full_col,
                        .op = BinaryOperation::kLessThan,
                        .is_double = false,
                        .constant_payload = true,
                        .int_constant = b});
      }
    } else {  // kLessThanEquals
      if (b < a) {
        emit_impossible();
      } else {
        out->push_back({.left = full_col,
                        .op = BinaryOperation::kLessThanEquals,
                        .is_double = false,
                        .constant_payload = true,
                        .int_constant = b});
      }
    }
    return true;
  }
  auto to_dbl = [](const Value& v, double* d) -> bool {
    if (v.type == ValueType::kDouble) {
      if (!std::isfinite(v.value.double_value)) {
        return false;
      }
      *d = v.value.double_value;
      return true;
    }
    if (v.type == ValueType::kInt64) {
      *d = static_cast<double>(v.value.int_value);
      return true;
    }
    return false;
  };
  double a = 0.0;
  double b = 0.0;
  if (!to_dbl(a_v, &a) || !to_dbl(b_v, &b)) {
    return false;
  }
  auto emit_impossible = [&]() {
    out->push_back({.left = full_col,
                    .op = BinaryOperation::kGreaterThan,
                    .is_double = true,
                    .constant_payload = true,
                    .double_constant = a});
    out->push_back({.left = full_col,
                    .op = BinaryOperation::kLessThan,
                    .is_double = true,
                    .constant_payload = true,
                    .double_constant = a});
  };
  if (is_least) {
    if (op == BinaryOperation::kEquals) {
      if (b > a) {
        emit_impossible();
      } else if (b == a) {
        out->push_back({.left = full_col,
                        .op = BinaryOperation::kGreaterThanEquals,
                        .is_double = true,
                        .constant_payload = true,
                        .double_constant = a});
      } else {
        out->push_back({.left = full_col,
                        .op = BinaryOperation::kEquals,
                        .is_double = true,
                        .constant_payload = true,
                        .double_constant = b});
      }
    } else if (op == BinaryOperation::kGreaterThan) {
      if (b >= a) {
        emit_impossible();
      } else {
        out->push_back({.left = full_col,
                        .op = BinaryOperation::kGreaterThan,
                        .is_double = true,
                        .constant_payload = true,
                        .double_constant = b});
      }
    } else {
      if (b > a) {
        emit_impossible();
      } else {
        out->push_back({.left = full_col,
                        .op = BinaryOperation::kGreaterThanEquals,
                        .is_double = true,
                        .constant_payload = true,
                        .double_constant = b});
      }
    }
    return true;
  }
  if (op == BinaryOperation::kEquals) {
    if (b < a) {
      emit_impossible();
    } else if (b == a) {
      out->push_back({.left = full_col,
                      .op = BinaryOperation::kLessThanEquals,
                      .is_double = true,
                      .constant_payload = true,
                      .double_constant = a});
    } else {
      out->push_back({.left = full_col,
                      .op = BinaryOperation::kEquals,
                      .is_double = true,
                      .constant_payload = true,
                      .double_constant = b});
    }
  } else if (op == BinaryOperation::kLessThan) {
    if (b <= a) {
      emit_impossible();
    } else {
      out->push_back({.left = full_col,
                      .op = BinaryOperation::kLessThan,
                      .is_double = true,
                      .constant_payload = true,
                      .double_constant = b});
    }
  } else {
    if (b < a) {
      emit_impossible();
    } else {
      out->push_back({.left = full_col,
                      .op = BinaryOperation::kLessThanEquals,
                      .is_double = true,
                      .constant_payload = true,
                      .double_constant = b});
    }
  }
  return true;
}

}  // namespace

bool CompilePredicatesFromExpressions(
    const std::vector<Expression>& predicates,
    const std::vector<IntegerPeekCompare>& full_peeks, const TlcsFile& tlcs,
    const Schema& scan_schema, std::span<const slot_t> proj_to_full,
    const Schema& table_schema, ColumnarPredicateSet* out,
    bool allow_parameter_slots) {
  out->compares.clear();
  out->inlists.clear();
  out->nulls.clear();
  out->derived_compares.clear();

  static const bool dbg_pred =
      ::tinylamb::platform::GetEnv("TINYLAMB_DEBUG_TLCS") != nullptr;
  for (const Expression& raw_predicate : predicates) {
    if (!raw_predicate) {
      continue;
    }
    if (ContainsQuery(raw_predicate) || ContainsVolatileCall(raw_predicate) ||
        (!allow_parameter_slots && ContainsParameterSlot(raw_predicate))) {
      if (dbg_pred) {
        (void)std::fprintf(stderr,
                           "[TLCS-PRED-BAIL] query/param/volatile: %s\n",
                           raw_predicate->ToString().c_str());
      }
      return false;
    }
    const Expression predicate = allow_parameter_slots
                                     ? UnwrapBoundParameterSlots(raw_predicate)
                                     : raw_predicate;
    if (predicate->Type() == TypeTag::kInExp) {
      const InExpression& membership = predicate->AsInExpression();
      if (membership.list_.empty() || membership.list_.size() > 64 ||
          !membership.child_ ||
          membership.child_->Type() != TypeTag::kColumnValue) {
        return false;
      }
      const auto offset = LocalColumnOffset(
          scan_schema, membership.child_->AsColumnValue().GetColumnName());
      if (!offset || *offset >= scan_schema.ColumnCount()) {
        return false;
      }
      const Column& col = scan_schema.GetColumn(static_cast<slot_t>(*offset));
      if (col.IsUnsigned()) {
        return false;
      }
      TypedInListCompare compiled;
      compiled.column = MapSlot(static_cast<slot_t>(*offset), proj_to_full);
      const ValueType vt = col.Type();
      compiled.is_string = (vt == ValueType::kVarChar);
      compiled.is_double = (vt == ValueType::kDouble);
      if (!compiled.is_string && !compiled.is_double &&
          vt != ValueType::kInt64 && vt != ValueType::kDate) {
        return false;
      }
      for (const Expression& item : membership.list_) {
        if (!item || ContainsVolatileCall(item) ||
            (!allow_parameter_slots && ContainsParameterSlot(item)) ||
            ContainsQuery(item) || !item->TouchedColumns().empty()) {
          return false;
        }
        auto folded = item->TryEvaluate(Row(), Schema());
        if (!folded.HasValue()) {
          return false;
        }
        const Value& v = folded.Value();
        if (v.IsNull() || v.IsUnsigned() ||
            (!compiled.is_string && v.type != vt)) {
          return false;
        }
        if (compiled.is_string) {
          if (v.type != ValueType::kVarChar || v.IsCaseInsensitive() ||
              !PlainStringComparable(v.value.varchar_value)) {
            return false;
          }
          compiled.strings.emplace_back(v.value.varchar_value);
        } else if (compiled.is_double) {
          compiled.double_values.push_back(v.value.double_value);
        } else {
          compiled.int_values.push_back(v.value.int_value);
        }
      }
      out->inlists.push_back(std::move(compiled));
      continue;
    }

    auto cmp = TryCompileTypedFilterCompare(predicate, scan_schema, true);
    if (cmp.has_value()) {
      cmp->left = MapSlot(cmp->left, proj_to_full);
      if (!cmp->constant_payload) {
        cmp->right = MapSlot(cmp->right, proj_to_full);
      }
      out->compares.push_back(std::move(*cmp));
      continue;
    }

    auto derived = TryCompileDerivedZoneCompare(predicate, tlcs, scan_schema,
                                                proj_to_full, table_schema);
    if (derived.has_value()) {
      out->derived_compares.push_back(*derived);
      continue;
    }

    if (TryCompileCalendarTruncPredicate(predicate, scan_schema, proj_to_full,
                                         table_schema, &out->compares)) {
      continue;
    }

    if (TryCompileDistanceBandPredicate(predicate, tlcs, scan_schema,
                                        proj_to_full, table_schema,
                                        &out->compares)) {
      continue;
    }

    if (TryCompileQuotientBandPredicate(predicate, tlcs, scan_schema,
                                        proj_to_full, table_schema,
                                        &out->compares)) {
      continue;
    }

    if (TryCompileMinClampPredicate(predicate, scan_schema, proj_to_full,
                                    table_schema, &out->compares)) {
      continue;
    }

    // Handle optional NOT*(IS NULL / IS NOT NULL).
    Expression inner = predicate;
    bool negated = false;
    while (inner && inner->Type() == TypeTag::kUnaryExp &&
           inner->AsUnaryExpression().Op() == UnaryOperation::kNot) {
      negated = !negated;
      inner = inner->AsUnaryExpression().Child();
    }
    if (inner && inner->Type() == TypeTag::kUnaryExp) {
      const auto& unary = inner->AsUnaryExpression();
      if ((unary.Op() == UnaryOperation::kIsNull ||
           unary.Op() == UnaryOperation::kIsNotNull) &&
          unary.Child() && unary.Child()->Type() == TypeTag::kColumnValue) {
        const auto offset = LocalColumnOffset(
            scan_schema, unary.Child()->AsColumnValue().GetColumnName());
        if (offset && *offset < scan_schema.ColumnCount()) {
          out->nulls.push_back(
              {.column = MapSlot(static_cast<slot_t>(*offset), proj_to_full),
               .is_null = (unary.Op() == UnaryOperation::kIsNull) != negated});
          continue;
        }
      }
    }
    if (dbg_pred) {
      std::string extra;
      // The static_cast<int> values below are small enum tags (operator and
      // type enums) used only in this debug string, not size/offset values;
      // converting them to int cannot narrow away any information.
      if (predicate->Type() == TypeTag::kBinaryExp) {
        const auto& b = predicate->AsBinaryExpression();
        extra = " lop=" + std::to_string(static_cast<int>(b.Op())) +
                " lt=" + std::to_string(static_cast<int>(b.Left()->Type())) +
                " rt=" + std::to_string(static_cast<int>(b.Right()->Type()));
        if (b.Left()->Type() == TypeTag::kColumnValue) {
          const auto& cn = b.Left()->AsColumnValue().GetColumnName();
          const auto off = LocalColumnOffset(scan_schema, cn);
          extra += " lcol=(" + cn.schema + "." + cn.name + ")" +
                   " off=" + (off ? std::to_string(*off) : "none");
          if (off && *off < scan_schema.ColumnCount()) {
            const Column& col =
                scan_schema.GetColumn(static_cast<slot_t>(*off));
            extra += " ctype=" + std::to_string(static_cast<int>(col.Type())) +
                     " uns=" + std::to_string(col.IsUnsigned() ? 1 : 0);
          }
        }
        if (b.Right()->Type() == TypeTag::kConstantValue) {
          const Value& rv = b.Right()->AsConstantValue().GetValue();
          extra += " rvt=" + std::to_string(static_cast<int>(rv.type)) +
                   " runs=" + std::to_string(rv.IsUnsigned() ? 1 : 0) +
                   " rci=" + std::to_string(rv.IsCaseInsensitive() ? 1 : 0);
        }
      }
      (void)std::fprintf(stderr, "[TLCS-PRED-BAIL] cols=%u%s\n",
                         static_cast<unsigned>(scan_schema.ColumnCount()),
                         extra.c_str());
    }
    return false;
  }

  for (const IntegerPeekCompare& peek : full_peeks) {
    if (peek.column >= table_schema.ColumnCount()) {
      return false;
    }
    const Column& col = table_schema.GetColumn(peek.column);
    if (col.IsUnsigned() ||
        (col.Type() != ValueType::kInt64 && col.Type() != ValueType::kDate)) {
      return false;
    }
    out->compares.push_back({.left = peek.column,
                             .op = peek.op,
                             .is_double = false,
                             .constant_payload = true,
                             .int_constant = peek.constant});
  }

  FinalizePredicateMetadata(tlcs, table_schema, out);
  return true;
}

namespace {}  // namespace

bool CompilePredicatesFromTypedPlan(const TypedAggregationPlan& plan,
                                    const TlcsFile& tlcs,
                                    std::span<const slot_t> proj_to_full,
                                    const Schema& table_schema,
                                    ColumnarPredicateSet* out) {
  if (!plan.filter_disjunctions.empty()) {
    return false;
  }
  if (!plan.filter_programs.empty() && !plan.only_null_filter_programs) {
    return false;
  }
  out->compares.reserve(plan.filter_compares.size());
  for (TypedFilterCompare cmp : plan.filter_compares) {
    cmp.left = MapSlot(cmp.left, proj_to_full);
    if (!cmp.constant_payload) {
      cmp.right = MapSlot(cmp.right, proj_to_full);
    }
    out->compares.push_back(std::move(cmp));
  }
  out->inlists.reserve(plan.inlist_compares.size());
  for (TypedInListCompare in : plan.inlist_compares) {
    in.column = MapSlot(in.column, proj_to_full);
    out->inlists.push_back(std::move(in));
  }
  out->nulls.reserve(plan.null_filters.size());
  for (TypedNullCompare nf : plan.null_filters) {
    nf.column = MapSlot(nf.column, proj_to_full);
    out->nulls.push_back(nf);
  }
  FinalizePredicateMetadata(tlcs, table_schema, out);
  return true;
}

namespace {

// Conservative sound block-level pruning against TlcsFile::BlockDirEntry.
bool MayTlcsBlockMatch(const TlcsFile::BlockDirEntry& blk,
                       const ColumnarPredicateSet& preds) noexcept {
  for (const TypedNullCompare& nf : preds.nulls) {
    if (nf.column >= blk.columns.size()) {
      return false;
    }
    const TlcsFile::ColumnMeta& meta = blk.columns[nf.column];
    if (nf.is_null && !meta.has_null &&
        meta.encoding != TlcsColumnEncoding::kConstNull) {
      return false;
    }
    if (!nf.is_null && meta.encoding == TlcsColumnEncoding::kConstNull) {
      return false;
    }
  }

  for (const DictFilterMask& dm : preds.dict_masks) {
    if (dm.full_col >= blk.columns.size()) {
      return false;
    }
    const TlcsFile::ColumnMeta& meta = blk.columns[dm.full_col];
    if (meta.encoding == TlcsColumnEncoding::kConstNull) {
      return false;
    }
    if (meta.encoding == TlcsColumnEncoding::kNarrowDict) {
      uint64_t any = 0;
      for (size_t w = 0; w < 4; ++w) {
        any |= (meta.dict_mask[w] & dm.allowed[w]);
      }
      if (any == 0) {
        return false;
      }
    }
  }

  // Lean proof:
  // lean/TinylambOpt/ResearchProofs/QueryRewrites/BlockPruningAndBoundedGrouping.lean
  //   (BlockConjunctionZoneBitmapPrune_sound)
  // lean/TinylambOpt/ResearchProofs/StorageExecution/DerivedZonesA.lean
  //   (Product, SaturatingLoss, Distance, BucketSum: SoundInterval, SkipRange)
  for (const TypedFilterCompare& cmp : preds.compares) {
    if (cmp.left >= blk.columns.size()) {
      continue;
    }
    if (!cmp.constant_payload) {
      if (cmp.right >= blk.columns.size()) {
        continue;
      }
      const TlcsFile::ColumnMeta& lm = blk.columns[cmp.left];
      const TlcsFile::ColumnMeta& rm = blk.columns[cmp.right];
      if (lm.encoding == TlcsColumnEncoding::kConstNull ||
          rm.encoding == TlcsColumnEncoding::kConstNull) {
        return false;
      }
      if (cmp.is_double) {
        if (lm.has_double_min_max && !lm.has_nan && !lm.is_unsigned &&
            rm.has_double_min_max && !rm.has_nan && !rm.is_unsigned) {
          switch (cmp.op) {
            case BinaryOperation::kEquals:
              if (lm.max_double < rm.min_double ||
                  rm.max_double < lm.min_double) {
                return false;
              }
              break;
            case BinaryOperation::kNotEquals:
              if (lm.min_double == lm.max_double &&
                  rm.min_double == rm.max_double &&
                  lm.min_double == rm.min_double) {
                return false;
              }
              break;
            case BinaryOperation::kLessThan:
              if (lm.min_double >= rm.max_double) {
                return false;
              }
              break;
            case BinaryOperation::kLessThanEquals:
              if (lm.min_double > rm.max_double) {
                return false;
              }
              break;
            case BinaryOperation::kGreaterThan:
              if (lm.max_double <= rm.min_double) {
                return false;
              }
              break;
            case BinaryOperation::kGreaterThanEquals:
              if (lm.max_double < rm.min_double) {
                return false;
              }
              break;
            default:
              break;
          }
        }
      } else if (!cmp.string_payload && lm.has_min_max && !lm.is_unsigned &&
                 rm.has_min_max && !rm.is_unsigned) {
        switch (cmp.op) {
          case BinaryOperation::kEquals:
            if (lm.max_int < rm.min_int || rm.max_int < lm.min_int) {
              return false;
            }
            break;
          case BinaryOperation::kNotEquals:
            if (lm.min_int == lm.max_int && rm.min_int == rm.max_int &&
                lm.min_int == rm.min_int) {
              return false;
            }
            break;
          case BinaryOperation::kLessThan:
            if (lm.min_int >= rm.max_int) {
              return false;
            }
            break;
          case BinaryOperation::kLessThanEquals:
            if (lm.min_int > rm.max_int) {
              return false;
            }
            break;
          case BinaryOperation::kGreaterThan:
            if (lm.max_int <= rm.min_int) {
              return false;
            }
            break;
          case BinaryOperation::kGreaterThanEquals:
            if (lm.max_int < rm.min_int) {
              return false;
            }
            break;
          default:
            break;
        }
      }
      continue;
    }
    const TlcsFile::ColumnMeta& meta = blk.columns[cmp.left];
    if (meta.encoding == TlcsColumnEncoding::kConstNull) {
      return false;
    }
    if (cmp.string_payload) {
      if (meta.type == ValueType::kVarChar &&
          meta.encoding == TlcsColumnEncoding::kVarChar) {
        if (cmp.op == BinaryOperation::kEquals &&
            cmp.string_match == TypedFilterCompare::StringMatchKind::kExact &&
            !cmp.string_constant.empty() &&
            meta.data_size < cmp.string_constant.size()) {
          return false;
        }
        if (cmp.op == BinaryOperation::kNotEquals &&
            cmp.string_match == TypedFilterCompare::StringMatchKind::kExact &&
            cmp.string_constant.empty() && meta.data_size == 0) {
          return false;
        }
        if (cmp.op == BinaryOperation::kLike && !cmp.string_constant.empty() &&
            meta.data_size < cmp.string_constant.size()) {
          return false;
        }
      }
      continue;
    }
    if (cmp.is_double) {
      if (!meta.has_double_min_max || meta.has_nan || meta.is_unsigned) {
        continue;
      }
      const double c = cmp.double_constant;
      switch (cmp.op) {
        case BinaryOperation::kEquals:
          if (c < meta.min_double || c > meta.max_double) {
            return false;
          }
          break;
        case BinaryOperation::kNotEquals:
          if (meta.min_double == meta.max_double && meta.min_double == c) {
            return false;
          }
          break;
        case BinaryOperation::kLessThan:
          if (meta.min_double >= c) {
            return false;
          }
          break;
        case BinaryOperation::kLessThanEquals:
          if (meta.min_double > c) {
            return false;
          }
          break;
        case BinaryOperation::kGreaterThan:
          if (meta.max_double <= c) {
            return false;
          }
          break;
        case BinaryOperation::kGreaterThanEquals:
          if (meta.max_double < c) {
            return false;
          }
          break;
        default:
          break;
      }
      continue;
    }
    if (!meta.has_min_max || meta.is_unsigned) {
      continue;
    }
    const int64_t c = cmp.int_constant;
    switch (cmp.op) {
      case BinaryOperation::kEquals:
        if (c < meta.min_int || c > meta.max_int) {
          return false;
        }
        break;
      case BinaryOperation::kNotEquals:
        if (meta.min_int == meta.max_int && meta.min_int == c) {
          return false;
        }
        break;
      case BinaryOperation::kLessThan:
        if (meta.min_int >= c) {
          return false;
        }
        break;
      case BinaryOperation::kLessThanEquals:
        if (meta.min_int > c) {
          return false;
        }
        break;
      case BinaryOperation::kGreaterThan:
        if (meta.max_int <= c) {
          return false;
        }
        break;
      case BinaryOperation::kGreaterThanEquals:
        if (meta.max_int < c) {
          return false;
        }
        break;
      default:
        break;
    }
  }

  // Lean proof:
  // lean/TinylambOpt/ResearchProofs/QueryRewrites/JoinMultiplicityAndZoneFilters.lean
  //   (MultiKeyInRangeZoneBlockSkip_sound)
  for (const TypedInListCompare& in : preds.inlists) {
    if (in.column >= blk.columns.size()) {
      return false;
    }
    const TlcsFile::ColumnMeta& meta = blk.columns[in.column];
    if (meta.encoding == TlcsColumnEncoding::kConstNull) {
      return false;
    }
    if (!in.is_string && !in.is_double && meta.has_min_max &&
        !meta.is_unsigned) {
      bool any_in_range = false;
      for (const int64_t v : in.int_values) {
        if (v >= meta.min_int && v <= meta.max_int) {
          any_in_range = true;
          break;
        }
      }
      if (!any_in_range) {
        return false;
      }
    }
    if (in.is_double && meta.has_double_min_max && !meta.has_nan &&
        !meta.is_unsigned) {
      bool any_in_range = false;
      for (const double v : in.double_values) {
        if (v >= meta.min_double && v <= meta.max_double) {
          any_in_range = true;
          break;
        }
      }
      if (!any_in_range) {
        return false;
      }
    }
  }

  // Lean proof:
  //   lean/TinylambOpt/ResearchProofs/StorageExecution/DerivedZonesA.lean
  //     (Product, SaturatingLoss, Distance, BucketSum: SoundInterval,
  //     SkipRange)
  //   lean/TinylambOpt/ResearchProofs/StorageExecution/DerivedZonesB.lean
  //     (WrappedSum: SoundInterval, SkipRange)
  for (const DerivedZoneCompare& dc : preds.derived_compares) {
    if (dc.col_a >= blk.columns.size() || dc.col_b >= blk.columns.size()) {
      return false;
    }
    const TlcsFile::ColumnMeta& ma = blk.columns[dc.col_a];
    const TlcsFile::ColumnMeta& mb = blk.columns[dc.col_b];
    if (ma.encoding == TlcsColumnEncoding::kConstNull ||
        mb.encoding == TlcsColumnEncoding::kConstNull) {
      return false;
    }
    if (dc.is_double) {
      if (!ma.has_double_min_max || ma.has_nan || ma.is_unsigned ||
          !mb.has_double_min_max || mb.has_nan || mb.is_unsigned) {
        continue;
      }
      double lo = 0.0;
      double hi = 0.0;
      switch (dc.kernel) {
        case DerivedZoneCompare::Kernel::kAdd:
          lo = ma.min_double + mb.min_double;
          hi = ma.max_double + mb.max_double;
          break;
        case DerivedZoneCompare::Kernel::kSub:
          lo = ma.min_double - mb.max_double;
          hi = ma.max_double - mb.min_double;
          break;
        case DerivedZoneCompare::Kernel::kMul: {
          const double p0 = ma.min_double * mb.min_double;
          const double p1 = ma.min_double * mb.max_double;
          const double p2 = ma.max_double * mb.min_double;
          const double p3 = ma.max_double * mb.max_double;
          lo = std::min({p0, p1, p2, p3});
          hi = std::max({p0, p1, p2, p3});
          break;
        }
        case DerivedZoneCompare::Kernel::kAbsDiff:
          lo = std::max({0.0, ma.min_double - mb.max_double,
                         mb.min_double - ma.max_double});
          hi = std::max(ma.max_double - mb.min_double,
                        mb.max_double - ma.min_double);
          break;
        case DerivedZoneCompare::Kernel::kFloorDiv:
        case DerivedZoneCompare::Kernel::kBitXor:
          continue;
        case DerivedZoneCompare::Kernel::kMin:
          lo = std::min(ma.min_double, mb.min_double);
          hi = std::min(ma.max_double, mb.max_double);
          break;
        case DerivedZoneCompare::Kernel::kMax:
          lo = std::max(ma.min_double, mb.min_double);
          hi = std::max(ma.max_double, mb.max_double);
          break;
        case DerivedZoneCompare::Kernel::kSaturatingSub:
          lo = std::max(0.0, ma.min_double - mb.max_double);
          hi = std::max(0.0, ma.max_double - mb.min_double);
          break;
      }
      if (!std::isfinite(lo) || !std::isfinite(hi)) {
        continue;
      }
      const double c = dc.double_constant;
      switch (dc.op) {
        case BinaryOperation::kEquals:
          if (c < lo || c > hi) {
            return false;
          }
          break;
        case BinaryOperation::kNotEquals:
          if (lo == hi && lo == c) {
            return false;
          }
          break;
        case BinaryOperation::kLessThan:
          if (lo >= c) {
            return false;
          }
          break;
        case BinaryOperation::kLessThanEquals:
          if (lo > c) {
            return false;
          }
          break;
        case BinaryOperation::kGreaterThan:
          if (hi <= c) {
            return false;
          }
          break;
        case BinaryOperation::kGreaterThanEquals:
          if (hi < c) {
            return false;
          }
          break;
        default:
          break;
      }
      continue;
    }
    if (!ma.has_min_max || ma.is_unsigned || !mb.has_min_max ||
        mb.is_unsigned) {
      continue;
    }
    const Int128Interval iv = ComputeDerivedIntInterval(
        dc.kernel, ma.min_int, ma.max_int, mb.min_int, mb.max_int);
    const auto c = static_cast<__int128>(dc.int_constant);
    switch (dc.op) {
      case BinaryOperation::kEquals:
        if (c < iv.lo || c > iv.hi) {
          return false;
        }
        break;
      case BinaryOperation::kNotEquals:
        if (iv.lo == iv.hi && iv.lo == c) {
          return false;
        }
        break;
      case BinaryOperation::kLessThan:
        if (iv.lo >= c) {
          return false;
        }
        break;
      case BinaryOperation::kLessThanEquals:
        if (iv.lo > c) {
          return false;
        }
        break;
      case BinaryOperation::kGreaterThan:
        if (iv.hi <= c) {
          return false;
        }
        break;
      case BinaryOperation::kGreaterThanEquals:
        if (iv.hi < c) {
          return false;
        }
        break;
      default:
        break;
    }
  }
  return true;
}

}  // namespace

StatusOr<std::vector<uint32_t>> SelectCandidateBlocks(
    const TlcsFile& tlcs, const ColumnarPredicateSet& preds) {
  std::vector<uint32_t> candidates;
  const uint32_t groups = tlcs.DirGroupCount();
  const uint32_t group_size = tlcs.DirGroupSize();
  candidates.reserve(tlcs.BlockCount());
  for (uint32_t g = 0; g < groups; ++g) {
    ASSIGN_OR_RETURN(std::shared_ptr<const TlcsFile::DirGroup>, grp,
                     tlcs.GetDirGroup(g));
    const uint32_t base_idx = g * group_size;
    for (uint32_t b = 0; b < grp->blocks.size(); ++b) {
      if (preds.Empty() || MayTlcsBlockMatch(grp->blocks[b], preds)) {
        candidates.push_back(base_idx + b);
      }
    }
  }
  return candidates;
}

// Lean proof:
//   lean/TinylambOpt/ResearchProofs/StorageExecution/DerivedZonesA.lean
//     (Product, SaturatingLoss, Distance, BucketSum: TakeAll)
//   lean/TinylambOpt/ResearchProofs/StorageExecution/DerivedZonesB.lean
//     (WrappedSum: TakeAll)
bool BlockSatisfiesAllColumnarPredicates(
    const ColumnarPredicateSet& preds,
    const std::vector<const TlcsFile::DecodedColumnChunk*>& col_ptrs) noexcept {
  if (!preds.inlists.empty()) {
    return false;
  }
  for (const TypedNullCompare& nf : preds.nulls) {
    if (nf.column >= col_ptrs.size() || col_ptrs[nf.column] == nullptr) {
      return false;
    }
    const auto& ch = *col_ptrs[nf.column];
    if (nf.is_null || ch.has_null) {
      return false;
    }
  }
  for (const TypedFilterCompare& cmp : preds.compares) {
    if (cmp.string_payload || cmp.left >= col_ptrs.size() ||
        col_ptrs[cmp.left] == nullptr) {
      return false;
    }
    const auto& lc = *col_ptrs[cmp.left];
    if (lc.has_null || lc.is_unsigned) {
      return false;
    }
    if (!cmp.constant_payload) {
      if (cmp.right >= col_ptrs.size() || col_ptrs[cmp.right] == nullptr) {
        return false;
      }
      const auto& rc = *col_ptrs[cmp.right];
      if (rc.has_null || rc.is_unsigned) {
        return false;
      }
      if (cmp.is_double) {
        if (!lc.has_double_min_max || lc.has_nan || !rc.has_double_min_max ||
            rc.has_nan) {
          return false;
        }
        const bool ok =
            (cmp.op == BinaryOperation::kEquals &&
             lc.min_double == lc.max_double && rc.min_double == rc.max_double &&
             lc.min_double == rc.min_double) ||
            (cmp.op == BinaryOperation::kNotEquals &&
             (lc.max_double < rc.min_double ||
              rc.max_double < lc.min_double)) ||
            (cmp.op == BinaryOperation::kLessThan &&
             lc.max_double < rc.min_double) ||
            (cmp.op == BinaryOperation::kLessThanEquals &&
             lc.max_double <= rc.min_double) ||
            (cmp.op == BinaryOperation::kGreaterThan &&
             lc.min_double > rc.max_double) ||
            (cmp.op == BinaryOperation::kGreaterThanEquals &&
             lc.min_double >= rc.max_double);
        if (!ok) {
          return false;
        }
      } else {
        if (!lc.has_min_max || !rc.has_min_max) {
          return false;
        }
        const bool ok =
            (cmp.op == BinaryOperation::kEquals && lc.min_int == lc.max_int &&
             rc.min_int == rc.max_int && lc.min_int == rc.min_int) ||
            (cmp.op == BinaryOperation::kNotEquals &&
             (lc.max_int < rc.min_int || rc.max_int < lc.min_int)) ||
            (cmp.op == BinaryOperation::kLessThan && lc.max_int < rc.min_int) ||
            (cmp.op == BinaryOperation::kLessThanEquals &&
             lc.max_int <= rc.min_int) ||
            (cmp.op == BinaryOperation::kGreaterThan &&
             lc.min_int > rc.max_int) ||
            (cmp.op == BinaryOperation::kGreaterThanEquals &&
             lc.min_int >= rc.max_int);
        if (!ok) {
          return false;
        }
      }
      continue;
    }
    if (cmp.is_double) {
      if (!lc.has_double_min_max || lc.has_nan) {
        return false;
      }
      const double c = cmp.double_constant;
      const bool ok =
          (cmp.op == BinaryOperation::kEquals && lc.min_double == c &&
           lc.max_double == c) ||
          (cmp.op == BinaryOperation::kNotEquals &&
           (lc.max_double < c || c < lc.min_double)) ||
          (cmp.op == BinaryOperation::kLessThan && lc.max_double < c) ||
          (cmp.op == BinaryOperation::kLessThanEquals && lc.max_double <= c) ||
          (cmp.op == BinaryOperation::kGreaterThan && lc.min_double > c) ||
          (cmp.op == BinaryOperation::kGreaterThanEquals && lc.min_double >= c);
      if (!ok) {
        return false;
      }
    } else {
      if (!lc.has_min_max) {
        return false;
      }
      const int64_t c = cmp.int_constant;
      const bool ok =
          (cmp.op == BinaryOperation::kEquals && lc.min_int == c &&
           lc.max_int == c) ||
          (cmp.op == BinaryOperation::kNotEquals &&
           (lc.max_int < c || c < lc.min_int)) ||
          (cmp.op == BinaryOperation::kLessThan && lc.max_int < c) ||
          (cmp.op == BinaryOperation::kLessThanEquals && lc.max_int <= c) ||
          (cmp.op == BinaryOperation::kGreaterThan && lc.min_int > c) ||
          (cmp.op == BinaryOperation::kGreaterThanEquals && lc.min_int >= c);
      if (!ok) {
        return false;
      }
    }
  }
  for (const DerivedZoneCompare& dc : preds.derived_compares) {
    if (dc.col_a >= col_ptrs.size() || dc.col_b >= col_ptrs.size() ||
        col_ptrs[dc.col_a] == nullptr || col_ptrs[dc.col_b] == nullptr) {
      return false;
    }
    const auto& ca = *col_ptrs[dc.col_a];
    const auto& cb = *col_ptrs[dc.col_b];
    if (ca.has_null || cb.has_null || ca.is_unsigned || cb.is_unsigned) {
      return false;
    }
    if (dc.is_double) {
      if (!ca.has_double_min_max || ca.has_nan || !cb.has_double_min_max ||
          cb.has_nan) {
        return false;
      }
      double lo = 0.0;
      double hi = 0.0;
      switch (dc.kernel) {
        case DerivedZoneCompare::Kernel::kAdd:
          lo = ca.min_double + cb.min_double;
          hi = ca.max_double + cb.max_double;
          break;
        case DerivedZoneCompare::Kernel::kSub:
          lo = ca.min_double - cb.max_double;
          hi = ca.max_double - cb.min_double;
          break;
        case DerivedZoneCompare::Kernel::kMul: {
          const double p0 = ca.min_double * cb.min_double;
          const double p1 = ca.min_double * cb.max_double;
          const double p2 = ca.max_double * cb.min_double;
          const double p3 = ca.max_double * cb.max_double;
          lo = std::min({p0, p1, p2, p3});
          hi = std::max({p0, p1, p2, p3});
          break;
        }
        case DerivedZoneCompare::Kernel::kAbsDiff:
          lo = std::max({0.0, ca.min_double - cb.max_double,
                         cb.min_double - ca.max_double});
          hi = std::max(ca.max_double - cb.min_double,
                        cb.max_double - ca.min_double);
          break;
        case DerivedZoneCompare::Kernel::kFloorDiv:
        case DerivedZoneCompare::Kernel::kBitXor:
          return false;
        case DerivedZoneCompare::Kernel::kMin:
          lo = std::min(ca.min_double, cb.min_double);
          hi = std::min(ca.max_double, cb.max_double);
          break;
        case DerivedZoneCompare::Kernel::kMax:
          lo = std::max(ca.min_double, cb.min_double);
          hi = std::max(ca.max_double, cb.max_double);
          break;
        case DerivedZoneCompare::Kernel::kSaturatingSub:
          lo = std::max(0.0, ca.min_double - cb.max_double);
          hi = std::max(0.0, ca.max_double - cb.min_double);
          break;
      }
      if (!std::isfinite(lo) || !std::isfinite(hi)) {
        return false;
      }
      const double c = dc.double_constant;
      const bool ok =
          (dc.op == BinaryOperation::kEquals && lo == c && hi == c) ||
          (dc.op == BinaryOperation::kNotEquals && (hi < c || c < lo)) ||
          (dc.op == BinaryOperation::kLessThan && hi < c) ||
          (dc.op == BinaryOperation::kLessThanEquals && hi <= c) ||
          (dc.op == BinaryOperation::kGreaterThan && lo > c) ||
          (dc.op == BinaryOperation::kGreaterThanEquals && lo >= c);
      if (!ok) {
        return false;
      }
    } else {
      if (!ca.has_min_max || !cb.has_min_max) {
        return false;
      }
      const Int128Interval iv = ComputeDerivedIntInterval(
          dc.kernel, ca.min_int, ca.max_int, cb.min_int, cb.max_int);
      const auto c = static_cast<__int128>(dc.int_constant);
      const bool ok =
          (dc.op == BinaryOperation::kEquals && iv.lo == c && iv.hi == c) ||
          (dc.op == BinaryOperation::kNotEquals && (iv.hi < c || c < iv.lo)) ||
          (dc.op == BinaryOperation::kLessThan && iv.hi < c) ||
          (dc.op == BinaryOperation::kLessThanEquals && iv.hi <= c) ||
          (dc.op == BinaryOperation::kGreaterThan && iv.lo > c) ||
          (dc.op == BinaryOperation::kGreaterThanEquals && iv.lo >= c);
      if (!ok) {
        return false;
      }
    }
  }
  return true;
}

// Evaluates predicates against an array of per-table-column chunks (null entry
// means that column has not been decoded yet and its conjuncts are deferred).
bool RowMatchesColPtrs(
    const ColumnarPredicateSet& preds,
    const std::vector<const TlcsFile::DecodedColumnChunk*>& col_ptrs,
    uint32_t row) noexcept {
  for (const TypedNullCompare& nf : preds.nulls) {
    const auto* ch = col_ptrs[nf.column];
    if (ch == nullptr) {
      continue;
    }
    if (ch->IsNull(row) != nf.is_null) {
      return false;
    }
  }
  // Lean proof:
  //   lean/TinylambOpt/ResearchProofs/StorageExecution/GrammarScansA.lean
  //     (ContainsSlash.DictionaryMask)
  //   lean/TinylambOpt/ResearchProofs/StorageExecution/BitLayoutsA.lean
  //     (RowMajor.DictionaryProbe)
  // O(1) dictionary-code bitmask precheck: evaluates all constant compares and
  // IN-lists on narrow-dictionary columns via a single bit test before touching
  // string payloads.
  for (const DictFilterMask& dm : preds.dict_masks) {
    const auto* ch = col_ptrs[dm.full_col];
    if (ch == nullptr || !ch->dict_encoded) {
      continue;
    }
    if (ch->IsNull(row)) {
      return false;
    }
    const uint8_t code = ch->dict_codes[row];
    if (((dm.allowed[code >> 6U] >> (code & 63U)) & 1ULL) == 0ULL) {
      return false;
    }
  }
  // Lean proof:
  // lean/TinylambOpt/ResearchProofs/QueryRewrites/BlockPruningAndBoundedGrouping.lean
  //   (BlockConjunctionZoneBitmapPrune_sound)
  for (const TypedFilterCompare& cmp : preds.compares) {
    const auto* lcol = col_ptrs[cmp.left];
    if (lcol == nullptr) {
      continue;
    }
    if (cmp.constant_payload && lcol->dict_encoded &&
        cmp.left < preds.col_dict_mask_covers_const.size() &&
        preds.col_dict_mask_covers_const[cmp.left] != 0U) {
      // Already verified exact match via preds.dict_masks above.
      continue;
    }
    if (lcol->IsNull(row)) {
      return false;
    }
    if (!cmp.constant_payload) {
      const auto* rcol = col_ptrs[cmp.right];
      if (rcol == nullptr) {
        continue;
      }
      if (rcol->IsNull(row)) {
        return false;
      }
      if (cmp.is_double) {
        const auto lv = std::bit_cast<double>(lcol->int_cells[row]);
        const auto rv = std::bit_cast<double>(rcol->int_cells[row]);
        bool matched = false;
        switch (cmp.op) {
          case BinaryOperation::kEquals:
            matched = lv == rv;
            break;
          case BinaryOperation::kNotEquals:
            matched = lv != rv;
            break;
          case BinaryOperation::kLessThan:
            matched = lv < rv;
            break;
          case BinaryOperation::kLessThanEquals:
            matched = lv <= rv;
            break;
          case BinaryOperation::kGreaterThan:
            matched = lv > rv;
            break;
          case BinaryOperation::kGreaterThanEquals:
            matched = lv >= rv;
            break;
          default:
            break;
        }
        if (!matched) {
          return false;
        }
      } else {
        const int64_t lv = lcol->int_cells[row];
        const int64_t rv = rcol->int_cells[row];
        bool matched = false;
        switch (cmp.op) {
          case BinaryOperation::kEquals:
            matched = lv == rv;
            break;
          case BinaryOperation::kNotEquals:
            matched = lv != rv;
            break;
          case BinaryOperation::kLessThan:
            matched = lv < rv;
            break;
          case BinaryOperation::kLessThanEquals:
            matched = lv <= rv;
            break;
          case BinaryOperation::kGreaterThan:
            matched = lv > rv;
            break;
          case BinaryOperation::kGreaterThanEquals:
            matched = lv >= rv;
            break;
          default:
            break;
        }
        if (!matched) {
          return false;
        }
      }
      continue;
    }
    if (cmp.string_payload) {
      // Lean proof:
      // lean/TinylambOpt/ResearchProofs/QueryRewrites/BlockPruningAndBoundedGrouping.lean
      //   (NonEmptyStringFilterBeforeDictDecode_sound)
      if (cmp.string_match == TypedFilterCompare::StringMatchKind::kExact &&
          cmp.string_constant.empty() &&
          (cmp.op == BinaryOperation::kEquals ||
           cmp.op == BinaryOperation::kNotEquals)) {
        const bool non_empty = lcol->VarcharLength(row) != 0;
        const bool want_non_empty = (cmp.op == BinaryOperation::kNotEquals);
        if (non_empty != want_non_empty) {
          return false;
        }
        continue;
      }
      if (!MatchTypedStringCompare(lcol->VarcharAt(row), cmp)) {
        return false;
      }
      continue;
    }
    if (cmp.is_double) {
      const auto lv = std::bit_cast<double>(lcol->int_cells[row]);
      bool matched = false;
      switch (cmp.op) {
        case BinaryOperation::kEquals:
          matched = lv == cmp.double_constant;
          break;
        case BinaryOperation::kNotEquals:
          matched = lv != cmp.double_constant;
          break;
        case BinaryOperation::kLessThan:
          matched = lv < cmp.double_constant;
          break;
        case BinaryOperation::kLessThanEquals:
          matched = lv <= cmp.double_constant;
          break;
        case BinaryOperation::kGreaterThan:
          matched = lv > cmp.double_constant;
          break;
        case BinaryOperation::kGreaterThanEquals:
          matched = lv >= cmp.double_constant;
          break;
        default:
          break;
      }
      if (!matched) {
        return false;
      }
    } else {
      const int64_t lv = lcol->int_cells[row];
      bool matched = false;
      switch (cmp.op) {
        case BinaryOperation::kEquals:
          matched = lv == cmp.int_constant;
          break;
        case BinaryOperation::kNotEquals:
          matched = lv != cmp.int_constant;
          break;
        case BinaryOperation::kLessThan:
          matched = lv < cmp.int_constant;
          break;
        case BinaryOperation::kLessThanEquals:
          matched = lv <= cmp.int_constant;
          break;
        case BinaryOperation::kGreaterThan:
          matched = lv > cmp.int_constant;
          break;
        case BinaryOperation::kGreaterThanEquals:
          matched = lv >= cmp.int_constant;
          break;
        default:
          break;
      }
      if (!matched) {
        return false;
      }
    }
  }
  if (!std::ranges::all_of(preds.inlists, [&](const TypedInListCompare& in) {
        const auto* col = col_ptrs[in.column];
        if (col == nullptr) {
          return true;
        }
        if (col->dict_encoded &&
            in.column < preds.col_dict_mask_covers_const.size() &&
            preds.col_dict_mask_covers_const[in.column] != 0U) {
          return true;
        }
        if (col->IsNull(row)) {
          return false;
        }
        if (in.is_string) {
          if (!MatchTypedStringInList(col->VarcharAt(row), in)) {
            return false;
          }
        } else if (in.is_double) {
          const auto lv = std::bit_cast<double>(col->int_cells[row]);
          if (std::ranges::find(in.double_values, lv) ==
              in.double_values.end()) {
            return false;
          }
        } else {
          const int64_t lv = col->int_cells[row];
          if (std::ranges::find(in.int_values, lv) == in.int_values.end()) {
            return false;
          }
        }
        return true;
      })) {
    return false;
  }
  for (const DerivedZoneCompare& dc : preds.derived_compares) {
    const auto* ca = col_ptrs[dc.col_a];
    const auto* cb = col_ptrs[dc.col_b];
    if (ca == nullptr || cb == nullptr) {
      continue;
    }
    if (ca->IsNull(row) || cb->IsNull(row)) {
      return false;
    }
    if (dc.is_double) {
      const auto a = std::bit_cast<double>(ca->int_cells[row]);
      const auto b = std::bit_cast<double>(cb->int_cells[row]);
      double val = 0.0;
      switch (dc.kernel) {
        case DerivedZoneCompare::Kernel::kAdd:
          val = a + b;
          break;
        case DerivedZoneCompare::Kernel::kSub:
          val = a - b;
          break;
        case DerivedZoneCompare::Kernel::kMul:
          val = a * b;
          break;
        case DerivedZoneCompare::Kernel::kAbsDiff:
          val = std::abs(a - b);
          break;
        case DerivedZoneCompare::Kernel::kFloorDiv:
        case DerivedZoneCompare::Kernel::kBitXor:
          return false;
        case DerivedZoneCompare::Kernel::kMin:
          val = (std::isnan(a) || std::isnan(b))
                    ? std::numeric_limits<double>::quiet_NaN()
                    : std::min(a, b);
          break;
        case DerivedZoneCompare::Kernel::kMax:
          val = (std::isnan(a) || std::isnan(b))
                    ? std::numeric_limits<double>::quiet_NaN()
                    : std::max(a, b);
          break;
        case DerivedZoneCompare::Kernel::kSaturatingSub:
          val = (std::isnan(a) || std::isnan(b))
                    ? std::numeric_limits<double>::quiet_NaN()
                    : std::max(0.0, a - b);
          break;
      }
      bool matched = false;
      switch (dc.op) {
        case BinaryOperation::kEquals:
          matched = val == dc.double_constant;
          break;
        case BinaryOperation::kNotEquals:
          matched = val != dc.double_constant;
          break;
        case BinaryOperation::kLessThan:
          matched = val < dc.double_constant;
          break;
        case BinaryOperation::kLessThanEquals:
          matched = val <= dc.double_constant;
          break;
        case BinaryOperation::kGreaterThan:
          matched = val > dc.double_constant;
          break;
        case BinaryOperation::kGreaterThanEquals:
          matched = val >= dc.double_constant;
          break;
        default:
          break;
      }
      if (!matched) {
        return false;
      }
    } else {
      const int64_t a = ca->int_cells[row];
      const int64_t b = cb->int_cells[row];
      int64_t val = 0;
      switch (dc.kernel) {
        case DerivedZoneCompare::Kernel::kAdd:
          val = a + b;
          break;
        case DerivedZoneCompare::Kernel::kSub:
          val = a - b;
          break;
        case DerivedZoneCompare::Kernel::kMul:
          val = a * b;
          break;
        case DerivedZoneCompare::Kernel::kAbsDiff:
          val = std::abs(a - b);
          break;
        case DerivedZoneCompare::Kernel::kFloorDiv:
          val = a / b;
          break;
        case DerivedZoneCompare::Kernel::kBitXor:
          val = static_cast<int64_t>(static_cast<uint64_t>(a) ^
                                     static_cast<uint64_t>(b));
          break;
        case DerivedZoneCompare::Kernel::kMin:
          val = std::min(a, b);
          break;
        case DerivedZoneCompare::Kernel::kMax:
          val = std::max(a, b);
          break;
        case DerivedZoneCompare::Kernel::kSaturatingSub:
          val = std::max<int64_t>(0, a - b);
          break;
      }
      bool matched = false;
      switch (dc.op) {
        case BinaryOperation::kEquals:
          matched = val == dc.int_constant;
          break;
        case BinaryOperation::kNotEquals:
          matched = val != dc.int_constant;
          break;
        case BinaryOperation::kLessThan:
          matched = val < dc.int_constant;
          break;
        case BinaryOperation::kLessThanEquals:
          matched = val <= dc.int_constant;
          break;
        case BinaryOperation::kGreaterThan:
          matched = val > dc.int_constant;
          break;
        case BinaryOperation::kGreaterThanEquals:
          matched = val >= dc.int_constant;
          break;
        default:
          break;
      }
      if (!matched) {
        return false;
      }
    }
  }
  return true;
}

// Lean proofs:
//   lean/TinylambOpt/PhysicalJitFusion.lean
//     (first_hit_decides_row, refine_candidates_sound)
//   lean/TinylambOpt/ResearchProofs/AnalyticalOperators/FrequencyBoundsAndOrderedFetch.lean
//     (FrontCodedContains_sound)
//   lean/TinylambOpt/ResearchProofs/ClickstreamLayout/BoundaryAwareSubstringSearch.lean
//     (ConcatSearch.Goal)
bool TryJitContainsRows(
    const ColumnarPredicateSet& preds,
    const std::vector<const TlcsFile::DecodedColumnChunk*>& col_ptrs,
    uint32_t row_count, bool refine, std::vector<uint32_t>* rows) {
  // One positive literal is enough to generate the candidate list. The
  // regular evaluator handles the remaining conjuncts on surviving rows.
  for (const auto& precheck : preds.contains_prechecks) {
    const auto* chunk = col_ptrs[precheck.full_col];
    if (chunk == nullptr || row_count == 0 || precheck.needle.empty()) {
      continue;
    }
    if (chunk->dict_encoded && chunk->dict != nullptr &&
        chunk->dict_codes.size() == row_count) {
      // FrontCodedContains_sound: evaluate `LIKE '%needle%'` once per
      // dictionary entry using adjacent common prefixes, then filter rows via
      // a 1-byte code table lookup.
      const size_t n_entries =
          std::min<size_t>(chunk->dict->StringEntryCount(), 256);
      const std::string_view needle = precheck.needle;
      const size_t needle_len = needle.size();
      std::array<uint8_t, 256> code_match{};
      bool any_match = false;
      std::string_view prev;
      size_t prev_hit_end = 0;
      for (size_t c = 0; c < n_entries; ++c) {
        const std::string_view cur = chunk->dict->StringEntryAt(c);
        const size_t min_len = std::min(prev.size(), cur.size());
        size_t lcp = 0;
        while (lcp < min_len && prev[lcp] == cur[lcp]) {
          ++lcp;
        }
        if (prev_hit_end > 0 && prev_hit_end <= lcp) {
          code_match[c] = 1;
          any_match = true;
          prev = cur;
          continue;
        }
        const size_t start =
            (lcp + 1 >= needle_len) ? (lcp + 1 - needle_len) : 0;
        size_t pos = std::string_view::npos;
        if (start + needle_len <= cur.size()) {
          if (precheck.kernel) {
            const auto tail = std::span<const uint8_t>(
                reinterpret_cast<const uint8_t*>(cur.data() + start),
                cur.size() - start);
            const size_t rel = precheck.kernel->Find(tail);
            if (rel < tail.size()) {
              pos = start + rel;
            }
          } else {
            pos = cur.find(needle, start);
          }
        }
        if (pos != std::string_view::npos) {
          code_match[c] = 1;
          any_match = true;
          prev_hit_end = pos + needle_len;
        } else {
          prev_hit_end = 0;
        }
        prev = cur;
      }
      if (!any_match) {
        rows->clear();
        return true;
      }
      const bool only_literal = preds.compares.size() == 1 &&
                                preds.nulls.empty() && preds.inlists.empty() &&
                                preds.derived_compares.empty();
      const uint8_t* __restrict codes = chunk->dict_codes.data();
      if (refine) {
        size_t write = 0;
        for (const uint32_t row : *rows) {
          if (!chunk->IsNull(row) && code_match[codes[row]] != 0 &&
              (only_literal || RowMatchesColPtrs(preds, col_ptrs, row))) {
            (*rows)[write++] = row;
          }
        }
        rows->resize(write);
      } else {
        rows->clear();
        for (uint32_t row = 0; row < row_count; ++row) {
          if (!chunk->IsNull(row) && code_match[codes[row]] != 0 &&
              (only_literal || RowMatchesColPtrs(preds, col_ptrs, row))) {
            rows->push_back(row);
          }
        }
      }
      return true;
    }
    if (chunk->dict_encoded || chunk->varchar_ends.size() != row_count ||
        chunk->varchar_ends.back() != chunk->varchar_bytes.size()) {
      continue;
    }
    const std::span<const uint8_t> bytes(chunk->varchar_bytes);
    const std::string_view needle = precheck.needle;
    const size_t needle_len = needle.size();
    auto find_from = [&](size_t start) noexcept -> size_t {
      if (start + needle_len > bytes.size()) {
        return bytes.size();
      }
      if (precheck.kernel) {
        const size_t rel = precheck.kernel->Find(bytes.subspan(start));
        return (rel < bytes.size() - start) ? (start + rel) : bytes.size();
      }
#ifdef __GLIBC__
      const void* hit = ::memmem(bytes.data() + start, bytes.size() - start,
                                 needle.data(), needle_len);
      if (hit == nullptr) {
        return bytes.size();
      }
      return static_cast<size_t>(static_cast<const uint8_t*>(hit) -
                                 bytes.data());
#else
      const std::string_view hay(
          reinterpret_cast<const char*>(bytes.data() + start),
          bytes.size() - start);
      const size_t rel = hay.find(needle);
      return (rel == std::string_view::npos) ? bytes.size() : (start + rel);
#endif
    };
    size_t match = find_from(0);
    if (match == bytes.size()) {
      rows->clear();
      return true;
    }
    // A TLCS decoder can supply malformed offsets. Keep the ordinary per-row
    // bounds behavior for those chunks and search only monotone offsets.
    const auto& ends = chunk->varchar_ends;
    if (!std::ranges::is_sorted(ends)) {
      continue;
    }
    // ClickstreamLayout.Execution.ConcatSearch: search the concatenated byte
    // stream once, resolve each hit's row via monotonic upper_bound on `ends`,
    // reject cross-row matches (`needle_len <= end - match`), and jump to
    // `end` so each matching row is emitted at most once in row order.
    if (!refine) {
      rows->clear();
    }
    size_t read = 0;
    size_t write = 0;
    const bool only_literal = preds.compares.size() == 1 &&
                              preds.nulls.empty() && preds.inlists.empty() &&
                              preds.derived_compares.empty();
    auto row_end = ends.begin();
    while (match < bytes.size()) {
      row_end = std::upper_bound(row_end, ends.end(), match);
      if (row_end == ends.end()) {
        break;
      }
      const auto row = static_cast<uint32_t>(row_end - ends.begin());
      const size_t end = *row_end;
      if (needle_len <= end - match && !chunk->IsNull(row)) {
        bool emit = true;
        if (refine) {
          while (read < rows->size() && (*rows)[read] < row) {
            ++read;
          }
          if (read == rows->size()) {
            break;
          }
          if ((*rows)[read] != row) {
            emit = false;
          } else {
            ++read;
          }
        }
        if (emit && (only_literal || RowMatchesColPtrs(preds, col_ptrs, row))) {
          if (refine) {
            (*rows)[write++] = row;
          } else {
            rows->push_back(row);
          }
        }
      }
      // If the earliest hit crosses the boundary, every later start within
      // this row crosses it too. In either case the row has been decided.
      match = find_from(end);
    }
    if (refine) {
      rows->resize(write);
    }
    return true;
  }
  return false;
}

// Fast block-level byte search for `LIKE '%needle%'` filters prior to row-level
// boundary scanning or decoding other columns.
bool BlockPassesContainsPrecheck(const TlcsFile::DecodedColumnChunk& chunk,
                                 std::string_view needle) noexcept {
  if (chunk.dict_encoded) {
    // Directory mask check already handled narrow-dict filtering if complete;
    // row-level checks still apply per dict entry.
    return true;
  }
  if (chunk.varchar_bytes.size() < needle.size() || needle.empty()) {
    return needle.empty() && !chunk.varchar_bytes.empty();
  }
#ifdef __GLIBC__
  return ::memmem(chunk.varchar_bytes.data(), chunk.varchar_bytes.size(),
                  needle.data(), needle.size()) != nullptr;
#else
  const std::string_view all(
      reinterpret_cast<const char*>(chunk.varchar_bytes.data()),
      chunk.varchar_bytes.size());
  return all.find(needle) != std::string_view::npos;
#endif
}

Value ValueFromChunkRow(const TlcsFile::DecodedColumnChunk& chunk,
                        uint32_t row) {
  if (chunk.IsNull(row)) {
    return {};
  }
  switch (chunk.type) {
    case ValueType::kInt64: {
      Value v(chunk.int_cells[row]);
      if (chunk.is_unsigned) {
        v.SetUnsigned();
      }
      return v;
    }
    case ValueType::kDate:
      return Value::DateFromDays(chunk.int_cells[row]);
    case ValueType::kDouble:
      return Value(std::bit_cast<double>(chunk.int_cells[row]));
    case ValueType::kVarChar:
      return Value(std::string(chunk.VarcharAt(row)));
    default:
      return {};
  }
}

// ============================================================================
// 3. Shared TLCS Columnar Agg Primitives & Streaming/Pruned Engine
// ============================================================================

}  // namespace tinylamb::relational_detail
