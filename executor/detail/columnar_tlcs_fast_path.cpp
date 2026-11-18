/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/detail/columnar_tlcs_fast_path.hpp"

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
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/memory_budget.hpp"
#include "common/parallel_work.hpp"
#include "common/query_parallelism.hpp"
#include "common/regex_guard.hpp"
#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/columnar_table.hpp"
#include "executor/detail/columnar_tlcs_internal.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/flat_int_key_map.hpp"
#include "executor/detail/planning_heuristics.hpp"
#include "executor/detail/scan_filter.hpp"
#include "executor/detail/timestamp_trunc_kernel.hpp"
#include "executor/detail/typed_stream_aggregate_internal.hpp"
#include "executor/detail/typed_stream_aggregate_partitioned.hpp"
#include "executor/query_memory.hpp"
#include "expression/aggregate_expression.hpp"
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
#include "type/collation.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb::relational_detail {

namespace {

inline double MsSince(std::chrono::steady_clock::time_point begin) noexcept {
  return ColumnarMsSince(begin);
}

slot_t MapSlot(size_t slot, std::span<const slot_t> proj_to_full) noexcept {
  return proj_to_full.empty() ? static_cast<slot_t>(slot) : proj_to_full[slot];
}

}  // namespace

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
  std::shared_ptr<const TlcsFile> tlcs = table.GetBackingTLCSStore();
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
          set->contains_prechecks.push_back(
              {.full_col = cmp.left, .needle = cmp.string_constant});
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

  // Precompute valid 256-bit code masks for columns that use a complete narrow
  // dictionary in TLCS so directory pruning runs in a few bitwise ANDs.
  const auto& dict_kinds = tlcs.DictKinds();
  const auto& dicts = tlcs.Dicts();
  for (slot_t c = 0; c < ncols && c < dict_kinds.size() && c < dicts.size();
       ++c) {
    if (set->col_referenced[c] == 0U ||
        dict_kinds[c] != TlcsDictKind::kCompleteNarrow) {
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
        for (const TypedNullCompare& nf : set->nulls) {
          if (nf.column == c && nf.is_null) {
            ok = false;
            break;
          }
        }
      }
      if (ok) {
        mask.allowed[code / 64] |= (uint64_t{1} << (code % 64));
      }
    }
    set->dict_masks.push_back(mask);
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

  static const bool dbg_pred = std::getenv("TINYLAMB_DEBUG_TLCS") != nullptr;
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
                         scan_schema.ColumnCount(), extra.c_str());
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

namespace {

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

  for (const TypedFilterCompare& cmp : preds.compares) {
    if (!cmp.constant_payload || cmp.left >= blk.columns.size()) {
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
  for (const TypedFilterCompare& cmp : preds.compares) {
    const auto* lcol = col_ptrs[cmp.left];
    if (lcol == nullptr) {
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
  return std::ranges::all_of(preds.inlists, [&](const TypedInListCompare& in) {
    const auto* col = col_ptrs[in.column];
    if (col == nullptr) {
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
      if (std::ranges::find(in.double_values, lv) == in.double_values.end()) {
        return false;
      }
    } else {
      const int64_t lv = col->int_cells[row];
      if (std::ranges::find(in.int_values, lv) == in.int_values.end()) {
        return false;
      }
    }
    return true;
  });
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

Status FoldRowIntoAggState(
    const ColAggSpec& spec,
    const std::vector<const TlcsFile::DecodedColumnChunk*>& col_ptrs,
    uint32_t row, RuntimeAggState& st, PagedStringArena& arena) {
  if (spec.kind == ColAggKind::kCountStar) {
    ++st.count;
    st.has_value = true;
    return Status::kSuccess;
  }
  const TlcsFile::DecodedColumnChunk& chunk = *col_ptrs[spec.full_col];
  if (chunk.IsNull(row)) {
    return Status::kSuccess;
  }
  switch (spec.kind) {
    case ColAggKind::kCountStar:
      break;
    case ColAggKind::kCountCol:
    case ColAggKind::kCountByteLen:
      ++st.count;
      st.has_value = true;
      break;
    case ColAggKind::kSumInt64: {
      int64_t next = 0;
      if (__builtin_add_overflow(st.int_val, chunk.int_cells[row], &next)) {
        return StatusError(StatusCode::kIsInfinity, "integer overflow on SUM");
      }
      st.int_val = next;
      st.has_value = true;
      break;
    }
    case ColAggKind::kSumByteLen:
      st.int_val += static_cast<int64_t>(chunk.VarcharLength(row));
      st.has_value = true;
      break;
    case ColAggKind::kSumDouble:
      st.double_val += std::bit_cast<double>(chunk.int_cells[row]);
      st.has_value = true;
      break;
    case ColAggKind::kAvgDouble:
      st.double_val += chunk.type == ValueType::kDouble
                           ? std::bit_cast<double>(chunk.int_cells[row])
                           : static_cast<double>(chunk.int_cells[row]);
      ++st.count;
      st.has_value = true;
      break;
    case ColAggKind::kAvgByteLen:
      st.double_val += static_cast<double>(chunk.VarcharLength(row));
      ++st.count;
      st.has_value = true;
      break;
    case ColAggKind::kMinInt64:
    case ColAggKind::kMinDate: {
      const int64_t v = chunk.int_cells[row];
      if (!st.has_value || v < st.int_val) {
        st.int_val = v;
        st.has_value = true;
      }
      break;
    }
    case ColAggKind::kMaxInt64:
    case ColAggKind::kMaxDate: {
      const int64_t v = chunk.int_cells[row];
      if (!st.has_value || v > st.int_val) {
        st.int_val = v;
        st.has_value = true;
      }
      break;
    }
    case ColAggKind::kMinDouble: {
      const auto v = std::bit_cast<double>(chunk.int_cells[row]);
      if (std::isnan(v)) {
        st.saw_nan = true;
      }
      if (!st.has_value ||
          CompareForOrderBy(Value(v), Value(st.double_val)) < 0) {
        st.double_val = v;
        st.has_value = true;
      }
      break;
    }
    case ColAggKind::kMaxDouble: {
      const auto v = std::bit_cast<double>(chunk.int_cells[row]);
      if (std::isnan(v)) {
        st.saw_nan = true;
      }
      if (!st.has_value ||
          CompareForOrderBy(Value(v), Value(st.double_val)) > 0) {
        st.double_val = v;
        st.has_value = true;
      }
      break;
    }
    case ColAggKind::kMinVarChar: {
      // Lean proof: lean/TinylambOpt/ClickBenchDiscovery.lean
      // (order_preserving_dict_min_code_sound, unsorted_dict_min_code_bug)
      if (chunk.dict_encoded && chunk.dict != nullptr &&
          row < chunk.dict_codes.size()) {
        const uint8_t code = chunk.dict_codes[row];
        const size_t nd = chunk.dict->StringEntryCount();
        if (code < nd) {
          if (!st.has_value) {
            bool byte_sorted = chunk.dict->codes_follow_string_order;
            if (byte_sorted) {
              for (size_t k = 1; k < nd; ++k) {
                if (!(chunk.dict->StringEntryAt(k - 1) <
                      chunk.dict->StringEntryAt(k))) {
                  byte_sorted = false;
                  break;
                }
              }
            }
            const std::string_view sv = chunk.dict->StringEntryAt(code);
            st.dict_ptr = chunk.dict;
            st.dict_code = code;
            st.dict_byte_sorted = byte_sorted;
            st.str_ptr = sv.data();
            st.str_len = static_cast<uint32_t>(sv.size());
            st.has_value = true;
            break;
          }
          if (st.dict_ptr == chunk.dict) {
            if (code == st.dict_code) {
              break;
            }
            if (st.dict_byte_sorted) {
              if (code < st.dict_code) {
                const std::string_view sv = chunk.dict->StringEntryAt(code);
                st.dict_code = code;
                st.str_ptr = sv.data();
                st.str_len = static_cast<uint32_t>(sv.size());
              }
              break;
            }
            const std::string_view sv = chunk.dict->StringEntryAt(code);
            if (sv < std::string_view(st.str_ptr, st.str_len)) {
              st.dict_code = code;
              st.str_ptr = sv.data();
              st.str_len = static_cast<uint32_t>(sv.size());
            }
            break;
          }
        }
      }
      const std::string_view sv = chunk.VarcharAt(row);
      if (!st.has_value || sv < std::string_view(st.str_ptr, st.str_len)) {
        ASSIGN_OR_RETURN(std::string_view, interned, arena.Intern(sv));
        st.dict_ptr = nullptr;
        st.str_ptr = interned.data();
        st.str_len = static_cast<uint32_t>(interned.size());
        st.has_value = true;
      }
      break;
    }
    case ColAggKind::kMaxVarChar: {
      if (chunk.dict_encoded && chunk.dict != nullptr &&
          row < chunk.dict_codes.size()) {
        const uint8_t code = chunk.dict_codes[row];
        const size_t nd = chunk.dict->StringEntryCount();
        if (code < nd) {
          if (!st.has_value) {
            bool byte_sorted = chunk.dict->codes_follow_string_order;
            if (byte_sorted) {
              for (size_t k = 1; k < nd; ++k) {
                if (!(chunk.dict->StringEntryAt(k - 1) <
                      chunk.dict->StringEntryAt(k))) {
                  byte_sorted = false;
                  break;
                }
              }
            }
            const std::string_view sv = chunk.dict->StringEntryAt(code);
            st.dict_ptr = chunk.dict;
            st.dict_code = code;
            st.dict_byte_sorted = byte_sorted;
            st.str_ptr = sv.data();
            st.str_len = static_cast<uint32_t>(sv.size());
            st.has_value = true;
            break;
          }
          if (st.dict_ptr == chunk.dict) {
            if (code == st.dict_code) {
              break;
            }
            if (st.dict_byte_sorted) {
              if (code > st.dict_code) {
                const std::string_view sv = chunk.dict->StringEntryAt(code);
                st.dict_code = code;
                st.str_ptr = sv.data();
                st.str_len = static_cast<uint32_t>(sv.size());
              }
              break;
            }
            const std::string_view sv = chunk.dict->StringEntryAt(code);
            if (sv > std::string_view(st.str_ptr, st.str_len)) {
              st.dict_code = code;
              st.str_ptr = sv.data();
              st.str_len = static_cast<uint32_t>(sv.size());
            }
            break;
          }
        }
      }
      const std::string_view sv = chunk.VarcharAt(row);
      if (!st.has_value || sv > std::string_view(st.str_ptr, st.str_len)) {
        ASSIGN_OR_RETURN(std::string_view, interned, arena.Intern(sv));
        st.dict_ptr = nullptr;
        st.str_ptr = interned.data();
        st.str_len = static_cast<uint32_t>(interned.size());
        st.has_value = true;
      }
      break;
    }
    case ColAggKind::kMinByteLen: {
      const auto v = static_cast<int64_t>(chunk.VarcharLength(row));
      if (!st.has_value || v < st.int_val) {
        st.int_val = v;
        st.has_value = true;
      }
      break;
    }
    case ColAggKind::kMaxByteLen: {
      const auto v = static_cast<int64_t>(chunk.VarcharLength(row));
      if (!st.has_value || v > st.int_val) {
        st.int_val = v;
        st.has_value = true;
      }
      break;
    }
    case ColAggKind::kCountDistinctInt64:
    case ColAggKind::kCountDistinctDate:
    case ColAggKind::kCountDistinctDouble: {
      int64_t raw = chunk.int_cells[row];
      if (spec.kind == ColAggKind::kCountDistinctDouble) {
        const auto d = std::bit_cast<double>(raw);
        if (d == 0.0) {
          raw = 0;
        } else if (std::isnan(d)) {
          raw = int64_t{0x7ff8000000000000LL};
        }
      }
      if (!st.distinct_ints) {
        st.distinct_ints = std::make_unique<FlatIntKeyMap<uint8_t>>();
      }
      st.distinct_ints->TryEmplace(raw);
      st.has_value = true;
      break;
    }
    case ColAggKind::kCountDistinctVarChar: {
      if (!st.distinct_strs) {
        st.distinct_strs =
            std::make_unique<std::unordered_set<std::string_view>>();
      }
      const std::string_view sv = chunk.VarcharAt(row);
      if (!st.distinct_strs->contains(sv)) {
        // Intern only when the value is first seen: one allocation per
        // distinct string instead of one per row.
        ASSIGN_OR_RETURN(std::string_view, interned, arena.Intern(sv));
        st.distinct_strs->insert(interned);
      }
      st.has_value = true;
      break;
    }
  }
  return Status::kSuccess;
}

Status MergeAggState(const ColAggSpec& spec, RuntimeAggState& dst,
                     RuntimeAggState& src, PagedStringArena& arena) {
  if (!src.has_value) {
    return Status::kSuccess;
  }
  switch (spec.kind) {
    case ColAggKind::kCountStar:
    case ColAggKind::kCountCol:
    case ColAggKind::kCountByteLen:
      dst.count += src.count;
      dst.has_value = true;
      break;
    case ColAggKind::kSumInt64: {
      int64_t next = 0;
      if (__builtin_add_overflow(dst.int_val, src.int_val, &next)) {
        return StatusError(StatusCode::kIsInfinity, "integer overflow on SUM");
      }
      dst.int_val = next;
      dst.has_value = true;
      break;
    }
    case ColAggKind::kSumByteLen:
      dst.int_val += src.int_val;
      dst.has_value = true;
      break;
    case ColAggKind::kSumDouble:
      dst.double_val += src.double_val;
      dst.has_value = true;
      break;
    case ColAggKind::kAvgDouble:
    case ColAggKind::kAvgByteLen:
      dst.double_val += src.double_val;
      dst.count += src.count;
      dst.has_value = true;
      break;
    case ColAggKind::kMinInt64:
    case ColAggKind::kMinDate:
    case ColAggKind::kMinByteLen:
      if (!dst.has_value || src.int_val < dst.int_val) {
        dst.int_val = src.int_val;
        dst.has_value = true;
      }
      break;
    case ColAggKind::kMaxInt64:
    case ColAggKind::kMaxDate:
    case ColAggKind::kMaxByteLen:
      if (!dst.has_value || src.int_val > dst.int_val) {
        dst.int_val = src.int_val;
        dst.has_value = true;
      }
      break;
    case ColAggKind::kMinDouble:
      dst.saw_nan = dst.saw_nan || src.saw_nan;
      if (!dst.has_value ||
          CompareForOrderBy(Value(src.double_val), Value(dst.double_val)) < 0) {
        dst.double_val = src.double_val;
        dst.has_value = true;
      }
      break;
    case ColAggKind::kMaxDouble:
      dst.saw_nan = dst.saw_nan || src.saw_nan;
      if (!dst.has_value ||
          CompareForOrderBy(Value(src.double_val), Value(dst.double_val)) > 0) {
        dst.double_val = src.double_val;
        dst.has_value = true;
      }
      break;
    case ColAggKind::kMinVarChar: {
      if (dst.has_value && dst.dict_ptr != nullptr &&
          dst.dict_ptr == src.dict_ptr) {
        if (src.dict_code == dst.dict_code) {
          break;
        }
        if (dst.dict_byte_sorted) {
          if (src.dict_code < dst.dict_code) {
            dst.dict_code = src.dict_code;
            dst.str_ptr = src.str_ptr;
            dst.str_len = src.str_len;
          }
          break;
        }
      }
      const std::string_view sv(src.str_ptr, src.str_len);
      if (!dst.has_value || sv < std::string_view(dst.str_ptr, dst.str_len)) {
        if (src.dict_ptr != nullptr) {
          dst.dict_ptr = src.dict_ptr;
          dst.dict_code = src.dict_code;
          dst.dict_byte_sorted = src.dict_byte_sorted;
          dst.str_ptr = src.str_ptr;
          dst.str_len = src.str_len;
        } else {
          ASSIGN_OR_RETURN(std::string_view, interned, arena.Intern(sv));
          dst.dict_ptr = nullptr;
          dst.str_ptr = interned.data();
          dst.str_len = static_cast<uint32_t>(interned.size());
        }
        dst.has_value = true;
      }
      break;
    }
    case ColAggKind::kMaxVarChar: {
      if (dst.has_value && dst.dict_ptr != nullptr &&
          dst.dict_ptr == src.dict_ptr) {
        if (src.dict_code == dst.dict_code) {
          break;
        }
        if (dst.dict_byte_sorted) {
          if (src.dict_code > dst.dict_code) {
            dst.dict_code = src.dict_code;
            dst.str_ptr = src.str_ptr;
            dst.str_len = src.str_len;
          }
          break;
        }
      }
      const std::string_view sv(src.str_ptr, src.str_len);
      if (!dst.has_value || sv > std::string_view(dst.str_ptr, dst.str_len)) {
        if (src.dict_ptr != nullptr) {
          dst.dict_ptr = src.dict_ptr;
          dst.dict_code = src.dict_code;
          dst.dict_byte_sorted = src.dict_byte_sorted;
          dst.str_ptr = src.str_ptr;
          dst.str_len = src.str_len;
        } else {
          ASSIGN_OR_RETURN(std::string_view, interned, arena.Intern(sv));
          dst.dict_ptr = nullptr;
          dst.str_ptr = interned.data();
          dst.str_len = static_cast<uint32_t>(interned.size());
        }
        dst.has_value = true;
      }
      break;
    }
    case ColAggKind::kCountDistinctInt64:
    case ColAggKind::kCountDistinctDate:
    case ColAggKind::kCountDistinctDouble:
      if (src.distinct_ints != nullptr) {
        if (!dst.distinct_ints) {
          dst.distinct_ints = std::move(src.distinct_ints);
        } else {
          src.distinct_ints->ForEach(
              [&](int64_t k, uint8_t&) { dst.distinct_ints->TryEmplace(k); });
        }
        dst.has_value = true;
      }
      break;
    case ColAggKind::kCountDistinctVarChar:
      if (src.distinct_strs != nullptr) {
        if (!dst.distinct_strs) {
          dst.distinct_strs = std::move(src.distinct_strs);
        } else {
          for (const std::string_view s : *src.distinct_strs) {
            ASSIGN_OR_RETURN(std::string_view, interned, arena.Intern(s));
            dst.distinct_strs->insert(interned);
          }
        }
        dst.has_value = true;
      }
      break;
  }
  return Status::kSuccess;
}

Status WorkerGroupTable::Reserve(size_t expected_groups) {
  const size_t need_slots =
      std::max<size_t>(1024, std::bit_ceil(expected_groups * 2));
  if (slots_.size() >= need_slots) {
    return Status::kSuccess;
  }
  return Rehash(need_slots);
}

StatusOr<uint32_t> WorkerGroupTable::FindOrInsert(
    const RuntimeKeyCell* probe_keys, uint64_t hash, bool borrow_strings) {
  const size_t nkeys = key_specs_->size();
  if ((num_groups_ + 1) * 10 >= slots_.size() * 7) {
    const size_t next_cap = slots_.empty() ? size_t{1024} : slots_.size() * 2;
    RETURN_IF_FAIL(Rehash(next_cap));
  }
  const size_t mask = slots_.size() - 1;
  size_t pos = static_cast<size_t>(hash) & mask;
  while (true) {
    Slot& s = slots_[pos];
    if (s.group_plus_one == 0) {
      const auto gid = static_cast<uint32_t>(num_groups_++);
      const size_t kbase = keys_.size();
      keys_.resize(kbase + nkeys);
      for (size_t i = 0; i < nkeys; ++i) {
        keys_[kbase + i] = probe_keys[i];
        const auto kk = (*key_specs_)[i].kind;
        if (!borrow_strings && !probe_keys[i].is_null &&
            (kk == ColKeyKind::kColVarChar ||
             kk == ColKeyKind::kCaseWhenVarChar ||
             kk == ColKeyKind::kRegexReplaceVarChar)) {
          ASSIGN_OR_RETURN(std::string_view, interned,
                           arena_.Intern(probe_keys[i].StrView()));
          keys_[kbase + i].str_ptr = interned.data();
          keys_[kbase + i].str_len = static_cast<uint32_t>(interned.size());
        }
      }
      aggs_.resize(aggs_.size() + agg_specs_->size());
      group_hashes_.push_back(hash);
      s.hash = hash;
      s.group_plus_one = gid + 1;
      return gid;
    }
    if (s.hash == hash) {
      const uint32_t existing = s.group_plus_one - 1;
      if (GroupKeySliceHasher::Equals(
              &keys_[static_cast<size_t>(existing) * nkeys], probe_keys,
              *key_specs_)) {
        return existing;
      }
    }
    pos = (pos + 1) & mask;
  }
}

Status WorkerGroupTable::MergeFromGroup(const RuntimeKeyCell* src_keys,
                                        uint64_t hash,
                                        RuntimeAggState* src_aggs,
                                        bool borrow_strings) {
  ASSIGN_OR_RETURN(uint32_t, gid, FindOrInsert(src_keys, hash, borrow_strings));
  const size_t abase = static_cast<size_t>(gid) * agg_specs_->size();
  for (size_t a = 0; a < agg_specs_->size(); ++a) {
    RETURN_IF_FAIL(
        MergeAggState((*agg_specs_)[a], aggs_[abase + a], src_aggs[a], arena_));
  }
  return Status::kSuccess;
}

Status WorkerGroupTable::Rehash(size_t new_cap) {
  const size_t new_bytes = new_cap * sizeof(Slot);
  const size_t old_bytes = slots_.size() * sizeof(Slot);
  if (charge_ != nullptr && new_bytes > old_bytes &&
      !charge_->TryAdd(new_bytes - old_bytes)) {
    return StatusError(StatusCode::kMemoryLimitExceeded,
                       "columnar hash table exceeded budget");
  }
  std::vector<Slot> next(new_cap);
  const size_t mask = new_cap - 1;
  for (const Slot& old : slots_) {
    if (old.group_plus_one == 0) {
      continue;
    }
    size_t pos = static_cast<size_t>(old.hash) & mask;
    while (next[pos].group_plus_one != 0) {
      pos = (pos + 1) & mask;
    }
    next[pos] = old;
  }
  slots_.swap(next);
  return Status::kSuccess;
}

namespace {

// Specialized single-int64/date worker group table using FlatIntKeyMap so
// integer-key grouped scans (Q28/Q43) pay a single 16-byte probe instead of
// multi-key slice hashing + indirect key array chase.
class SingleIntWorkerTable {
 public:
  explicit SingleIntWorkerTable(size_t agg_count, QueryMemoryCharge* charge,
                                bool count_star_only = false)
      : agg_count_(agg_count),
        count_star_only_(count_star_only),
        charge_(charge),
        arena_(charge) {}

  StatusOr<uint32_t> FindOrInsertNonNull(int64_t key) {
    bool inserted = false;
    uint32_t* slot = map_.TryEmplace(key, &inserted);
    if (!inserted) {
      return *slot;
    }
    const size_t gbytes =
        (count_star_only_ ? sizeof(int64_t)
                          : agg_count_ * sizeof(RuntimeAggState)) +
        sizeof(int64_t) + sizeof(uint64_t) + 32;
    if (keys_.size() >= reserved_cap_) {
      const size_t next_cap =
          std::max<size_t>(256, reserved_cap_ == 0 ? 256 : reserved_cap_ * 2);
      if (charge_ != nullptr &&
          !charge_->TryAdd((next_cap - reserved_cap_) * gbytes)) {
        return StatusError(StatusCode::kMemoryLimitExceeded,
                           "columnar single-int hash table exceeded budget");
      }
      reserved_cap_ = next_cap;
      keys_.reserve(next_cap);
      hashes_.reserve(next_cap);
      if (count_star_only_) {
        counts_only_.reserve(next_cap);
      } else {
        aggs_.reserve(next_cap * agg_count_);
      }
    }
    const auto gid = static_cast<uint32_t>(keys_.size());
    *slot = gid;
    keys_.push_back(key);
    uint64_t x = static_cast<uint64_t>(key) + 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    hashes_.push_back(x ^ (x >> 31));
    if (count_star_only_) {
      counts_only_.push_back(0);
    } else {
      aggs_.resize(aggs_.size() + agg_count_);
    }
    return gid;
  }

  StatusOr<RuntimeAggState*> FindOrInsertNullAggs() {
    if (!has_null_) {
      if (charge_ != nullptr &&
          !charge_->TryAdd((agg_count_ * sizeof(RuntimeAggState)) + 16)) {
        return StatusError(StatusCode::kMemoryLimitExceeded,
                           "columnar single-int null group exceeded budget");
      }
      has_null_ = true;
      null_aggs_.resize(agg_count_);
    }
    return null_aggs_.data();
  }

  void IncrementNullCount(int64_t delta = 1) {
    if (!has_null_) {
      has_null_ = true;
      null_count_only_ = 0;
    }
    null_count_only_ += delta;
  }

  void Reserve(size_t n) {
    if (n <= reserved_cap_) {
      return;
    }
    const size_t gbytes =
        (count_star_only_ ? sizeof(int64_t)
                          : agg_count_ * sizeof(RuntimeAggState)) +
        sizeof(int64_t) + sizeof(uint64_t) + 32;
    if (charge_ != nullptr && !charge_->TryAdd((n - reserved_cap_) * gbytes)) {
      return;
    }
    reserved_cap_ = n;
    keys_.reserve(n);
    hashes_.reserve(n);
    if (count_star_only_) {
      counts_only_.reserve(n);
    } else {
      aggs_.reserve(n * agg_count_);
    }
  }

  [[nodiscard]] size_t TotalGroups() const noexcept {
    return keys_.size() + (has_null_ ? 1 : 0);
  }
  [[nodiscard]] size_t NonNullCount() const noexcept { return keys_.size(); }
  [[nodiscard]] bool HasNullGroup() const noexcept { return has_null_; }
  [[nodiscard]] int64_t KeyAt(size_t gid) const noexcept { return keys_[gid]; }
  [[nodiscard]] uint64_t HashAt(size_t gid) const noexcept {
    return hashes_[gid];
  }
  [[nodiscard]] RuntimeAggState* AggsAt(size_t gid) noexcept {
    return &aggs_[gid * agg_count_];
  }
  [[nodiscard]] RuntimeAggState* NullAggs() noexcept {
    return null_aggs_.data();
  }
  [[nodiscard]] int64_t& CountAt(size_t gid) noexcept {
    return counts_only_[gid];
  }
  [[nodiscard]] int64_t CountAt(size_t gid) const noexcept {
    return counts_only_[gid];
  }
  [[nodiscard]] int64_t NullCount() const noexcept {
    return count_star_only_ ? null_count_only_ : null_aggs_[0].count;
  }
  [[nodiscard]] bool IsCountStarOnly() const noexcept {
    return count_star_only_;
  }
  static constexpr uint64_t kNullHash = 0x243f6a8885a308d3ULL;
  [[nodiscard]] PagedStringArena& Arena() noexcept { return arena_; }

 private:
  size_t agg_count_{0};
  bool count_star_only_{false};
  size_t reserved_cap_{0};
  QueryMemoryCharge* charge_{nullptr};
  PagedStringArena arena_;
  FlatIntKeyMap<uint32_t> map_;
  std::vector<int64_t> keys_;
  std::vector<uint64_t> hashes_;
  std::vector<RuntimeAggState> aggs_;
  std::vector<int64_t> counts_only_;
  bool has_null_{false};
  std::vector<RuntimeAggState> null_aggs_;
  int64_t null_count_only_{0};
};

}  // namespace

Value FinishAggState(const ColAggSpec& spec, const RuntimeAggState& st) {
  switch (spec.kind) {
    case ColAggKind::kCountStar:
    case ColAggKind::kCountCol:
    case ColAggKind::kCountByteLen:
      return Value(st.count);
    case ColAggKind::kSumInt64:
    case ColAggKind::kSumByteLen:
      return st.has_value ? Value(st.int_val) : Value();
    case ColAggKind::kSumDouble:
      return st.has_value ? Value(st.double_val) : Value();
    case ColAggKind::kAvgDouble:
    case ColAggKind::kAvgByteLen:
      return st.count == 0
                 ? Value()
                 : Value(st.double_val / static_cast<double>(st.count));
    case ColAggKind::kMinInt64:
    case ColAggKind::kMaxInt64:
    case ColAggKind::kMinByteLen:
    case ColAggKind::kMaxByteLen:
      return st.has_value ? Value(st.int_val) : Value();
    case ColAggKind::kMinDate:
    case ColAggKind::kMaxDate:
      return st.has_value ? Value::DateFromDays(st.int_val) : Value();
    case ColAggKind::kMinDouble:
    case ColAggKind::kMaxDouble:
      // GoogleSQL: any NaN in the group makes MIN/MAX NaN.
      if (!st.has_value) {
        return {};
      }
      return Value(st.saw_nan ? std::numeric_limits<double>::quiet_NaN()
                              : st.double_val);
    case ColAggKind::kMinVarChar:
    case ColAggKind::kMaxVarChar:
      return st.has_value ? Value(std::string(st.str_ptr, st.str_len))
                          : Value();
    case ColAggKind::kCountDistinctInt64:
    case ColAggKind::kCountDistinctDate:
    case ColAggKind::kCountDistinctDouble:
      return Value(static_cast<int64_t>(
          st.distinct_ints != nullptr ? st.distinct_ints->Size() : 0));
    case ColAggKind::kCountDistinctVarChar:
      return Value(static_cast<int64_t>(
          st.distinct_strs != nullptr ? st.distinct_strs->size() : 0));
  }
  return {};
}

namespace {

bool TryClassifyAggregate(const AggregateExpression* agg,
                          const Schema& qualified_schema,
                          std::span<const slot_t> proj_to_full,
                          const Schema& table_schema, ColAggSpec* out) {
  if (agg == nullptr || agg->WhereFilter() ||
      agg->Having() != AggregateHavingModifier::kNone ||
      agg->HavingCondition() != nullptr || !agg->InnerOrderBy().empty() ||
      agg->InnerLimit().has_value() || agg->SecondaryArg() ||
      !agg->TrailingArgs().empty()) {
    return false;
  }
  out->expr = agg;
  out->needs_bytes = false;
  out->needs_lengths_only = false;

  const AggregationType at = agg->GetType();
  const bool distinct = agg->Distinct();
  const Expression& child = agg->Child();

  // COUNT(*) or COUNT(constant)
  if (at == AggregationType::kCount && !distinct) {
    if (!child ||
        (child->Type() == TypeTag::kConstantValue &&
         !child->AsConstantValue().GetValue().IsNull()) ||
        (child->Type() == TypeTag::kColumnValue &&
         child->AsColumnValue().GetColumnName().name == "*")) {
      out->kind = ColAggKind::kCountStar;
      return true;
    }
  }
  if (!child) {
    return false;
  }

  // Recognize optional `CAST(BYTE_LENGTH(col) AS FLOAT64)` or
  // `BYTE_LENGTH(col)`.
  Expression inner = child;
  if (inner->Type() == TypeTag::kCastExp &&
      inner->AsCastExpression().TargetValueType() == ValueType::kDouble) {
    inner = inner->AsCastExpression().Child();
  }
  if (!distinct && inner && inner->Type() == TypeTag::kFunctionCallExp) {
    const auto& fn = inner->AsFunctionCallExpression();
    if (IdentifierEquals(fn.FuncName(), "byte_length") &&
        fn.Args().size() == 1 && fn.Args()[0] &&
        fn.Args()[0]->Type() == TypeTag::kColumnValue) {
      const auto slot = LocalColumnOffset(
          qualified_schema, fn.Args()[0]->AsColumnValue().GetColumnName());
      if (slot && *slot < qualified_schema.ColumnCount() &&
          qualified_schema.GetColumn(*slot).Type() == ValueType::kVarChar) {
        out->full_col = MapSlot(*slot, proj_to_full);
        out->needs_lengths_only = true;
        switch (at) {
          case AggregationType::kCount:
            out->kind = ColAggKind::kCountByteLen;
            return true;
          case AggregationType::kSum:
            out->kind = ColAggKind::kSumByteLen;
            return true;
          case AggregationType::kAvg:
            out->kind = ColAggKind::kAvgByteLen;
            return true;
          case AggregationType::kMin:
            out->kind = ColAggKind::kMinByteLen;
            return true;
          case AggregationType::kMax:
            out->kind = ColAggKind::kMaxByteLen;
            return true;
          default:
            return false;
        }
      }
    }
  }

  // Unwrap identity or numeric widening cast to FLOAT64 for SUM/AVG(CAST(int AS
  // FLOAT64)).
  Expression leaf = child;
  if (at == AggregationType::kAvg && leaf->Type() == TypeTag::kCastExp &&
      leaf->AsCastExpression().TargetValueType() == ValueType::kDouble) {
    leaf = leaf->AsCastExpression().Child();
  }
  if (!leaf || leaf->Type() != TypeTag::kColumnValue) {
    return false;
  }
  const auto slot = LocalColumnOffset(qualified_schema,
                                      leaf->AsColumnValue().GetColumnName());
  if (!slot || *slot >= qualified_schema.ColumnCount()) {
    return false;
  }
  const Column& col = qualified_schema.GetColumn(*slot);
  if (col.IsUnsigned() || col.IsInterval()) {
    return false;
  }
  out->full_col = MapSlot(*slot, proj_to_full);
  const ValueType vt = table_schema.GetColumn(out->full_col).Type();

  if (distinct) {
    if (at != AggregationType::kCount) {
      return false;
    }
    if (vt == ValueType::kInt64) {
      out->kind = ColAggKind::kCountDistinctInt64;
      return true;
    }
    if (vt == ValueType::kDate) {
      out->kind = ColAggKind::kCountDistinctDate;
      return true;
    }
    if (vt == ValueType::kDouble) {
      out->kind = ColAggKind::kCountDistinctDouble;
      return true;
    }
    if (vt == ValueType::kVarChar) {
      out->kind = ColAggKind::kCountDistinctVarChar;
      out->needs_bytes = true;
      return true;
    }
    return false;
  }

  switch (at) {
    case AggregationType::kCount:
      out->kind = ColAggKind::kCountCol;
      out->needs_lengths_only = (vt == ValueType::kVarChar);
      return true;
    case AggregationType::kSum:
      if (vt == ValueType::kInt64) {
        out->kind = ColAggKind::kSumInt64;
        return true;
      }
      if (vt == ValueType::kDouble) {
        out->kind = ColAggKind::kSumDouble;
        return true;
      }
      return false;
    case AggregationType::kAvg:
      if (vt == ValueType::kInt64 || vt == ValueType::kDouble) {
        out->kind = ColAggKind::kAvgDouble;
        return true;
      }
      return false;
    case AggregationType::kMin:
      if (vt == ValueType::kInt64) {
        out->kind = ColAggKind::kMinInt64;
        return true;
      }
      if (vt == ValueType::kDate) {
        out->kind = ColAggKind::kMinDate;
        return true;
      }
      if (vt == ValueType::kDouble) {
        out->kind = ColAggKind::kMinDouble;
        return true;
      }
      if (vt == ValueType::kVarChar) {
        out->kind = ColAggKind::kMinVarChar;
        out->needs_bytes = true;
        return true;
      }
      return false;
    case AggregationType::kMax:
      if (vt == ValueType::kInt64) {
        out->kind = ColAggKind::kMaxInt64;
        return true;
      }
      if (vt == ValueType::kDate) {
        out->kind = ColAggKind::kMaxDate;
        return true;
      }
      if (vt == ValueType::kDouble) {
        out->kind = ColAggKind::kMaxDouble;
        return true;
      }
      if (vt == ValueType::kVarChar) {
        out->kind = ColAggKind::kMaxVarChar;
        out->needs_bytes = true;
        return true;
      }
      return false;
    default:
      return false;
  }
}

bool TryClassifyGroupKey(const Expression& expr, const Schema& qualified_schema,
                         std::span<const slot_t> proj_to_full,
                         const Schema& table_schema, ColGroupKeySpec* out) {
  if (!expr || ContainsParameterSlot(expr) || ContainsQuery(expr) ||
      ContainsVolatileCall(expr)) {
    return false;
  }
  out->original_expr_str = expr->ToString();

  if (expr->Type() == TypeTag::kConstantValue) {
    const Value& v = expr->AsConstantValue().GetValue();
    if (!v.IsNull() && !v.IsUnsigned() && v.type == ValueType::kInt64) {
      out->kind = ColKeyKind::kConstInt64;
      out->const_int = v.value.int_value;
      return true;
    }
    return false;
  }

  if (expr->Type() == TypeTag::kColumnValue) {
    const auto slot = LocalColumnOffset(qualified_schema,
                                        expr->AsColumnValue().GetColumnName());
    if (!slot || *slot >= qualified_schema.ColumnCount()) {
      return false;
    }
    const Column& col = qualified_schema.GetColumn(*slot);
    if (col.IsUnsigned() || col.IsInterval()) {
      return false;
    }
    out->full_col = MapSlot(*slot, proj_to_full);
    switch (table_schema.GetColumn(out->full_col).Type()) {
      case ValueType::kInt64:
        out->kind = ColKeyKind::kColInt64;
        return true;
      case ValueType::kDate:
        out->kind = ColKeyKind::kColDate;
        return true;
      case ValueType::kDouble:
        out->kind = ColKeyKind::kColDouble;
        return true;
      case ValueType::kVarChar:
        out->kind = ColKeyKind::kColVarChar;
        return true;
      default:
        return false;
    }
  }

  if (expr->Type() == TypeTag::kBinaryExp) {
    const auto& bin = expr->AsBinaryExpression();
    if (bin.Op() == BinaryOperation::kAdd ||
        bin.Op() == BinaryOperation::kSubtract) {
      Expression col_e;
      Expression const_e;
      bool negate = false;
      if (bin.Left() && bin.Left()->Type() == TypeTag::kColumnValue &&
          bin.Right() && bin.Right()->Type() == TypeTag::kConstantValue) {
        col_e = bin.Left();
        const_e = bin.Right();
        negate = (bin.Op() == BinaryOperation::kSubtract);
      } else if (bin.Op() == BinaryOperation::kAdd && bin.Left() &&
                 bin.Left()->Type() == TypeTag::kConstantValue && bin.Right() &&
                 bin.Right()->Type() == TypeTag::kColumnValue) {
        col_e = bin.Right();
        const_e = bin.Left();
      }
      if (col_e && const_e) {
        const Value& cv = const_e->AsConstantValue().GetValue();
        if (!cv.IsNull() && !cv.IsUnsigned() && cv.type == ValueType::kInt64 &&
            (!negate ||
             cv.value.int_value != std::numeric_limits<int64_t>::min())) {
          const auto slot = LocalColumnOffset(
              qualified_schema, col_e->AsColumnValue().GetColumnName());
          if (slot && *slot < qualified_schema.ColumnCount()) {
            const Column& col = qualified_schema.GetColumn(*slot);
            if (!col.IsUnsigned() && !col.IsInterval()) {
              out->full_col = MapSlot(*slot, proj_to_full);
              if (table_schema.GetColumn(out->full_col).Type() ==
                  ValueType::kInt64) {
                out->kind = ColKeyKind::kColInt64Delta;
                out->const_int =
                    negate ? -cv.value.int_value : cv.value.int_value;
                return true;
              }
            }
          }
        }
      }
    }
  }

  if (expr->Type() == TypeTag::kFunctionCallExp) {
    const auto& call = expr->AsFunctionCallExpression();
    if (const auto unit = CompileUtcTimestampTrunc(call)) {
      if (call.Args()[0] && call.Args()[0]->Type() == TypeTag::kColumnValue) {
        const auto slot = LocalColumnOffset(
            qualified_schema, call.Args()[0]->AsColumnValue().GetColumnName());
        if (slot && *slot < qualified_schema.ColumnCount() &&
            qualified_schema.GetColumn(*slot).Type() == ValueType::kVarChar) {
          out->kind = ColKeyKind::kUtcTimestampTrunc;
          out->full_col = MapSlot(*slot, proj_to_full);
          out->trunc_unit = *unit;
          return true;
        }
      }
      return false;
    }
    if (const auto unit = CompileUtcTimestampExtract(call)) {
      if (call.Args()[0] && call.Args()[0]->Type() == TypeTag::kColumnValue) {
        const auto slot = LocalColumnOffset(
            qualified_schema, call.Args()[0]->AsColumnValue().GetColumnName());
        if (slot && *slot < qualified_schema.ColumnCount() &&
            qualified_schema.GetColumn(*slot).Type() == ValueType::kVarChar) {
          out->kind = ColKeyKind::kUtcTimestampExtract;
          out->full_col = MapSlot(*slot, proj_to_full);
          out->trunc_unit = *unit;
          return true;
        }
      }
      return false;
    }
    if (IdentifierEquals(call.FuncName(), "regexp_replace") &&
        call.Args().size() == 3 && call.Args()[0] &&
        call.Args()[0]->Type() == TypeTag::kColumnValue && call.Args()[1] &&
        call.Args()[1]->Type() == TypeTag::kConstantValue && call.Args()[2] &&
        call.Args()[2]->Type() == TypeTag::kConstantValue) {
      const auto slot = LocalColumnOffset(
          qualified_schema, call.Args()[0]->AsColumnValue().GetColumnName());
      const Value& pat = call.Args()[1]->AsConstantValue().GetValue();
      const Value& rep = call.Args()[2]->AsConstantValue().GetValue();
      if (slot && *slot < qualified_schema.ColumnCount() &&
          qualified_schema.GetColumn(*slot).Type() == ValueType::kVarChar &&
          pat.type == ValueType::kVarChar && !pat.IsCaseInsensitive() &&
          rep.type == ValueType::kVarChar && !rep.IsCaseInsensitive()) {
        std::string scratch;
        auto validate = ReplaceCheckedRegexView(
            "", pat.value.varchar_value, rep.value.varchar_value, &scratch);
        if (!validate.HasValue()) {
          return false;
        }
        out->kind = ColKeyKind::kRegexReplaceVarChar;
        out->full_col = MapSlot(*slot, proj_to_full);
        out->regex_pattern = std::string(pat.value.varchar_value);
        out->regex_replacement = std::string(rep.value.varchar_value);
        return true;
      }
    }
    return false;
  }

  if (expr->Type() == TypeTag::kCaseExp) {
    const auto& ce = expr->AsCaseExpression();
    if (ce.when_clauses_.size() != 1 || !ce.when_clauses_[0].first ||
        !ce.when_clauses_[0].second || !ce.else_clause_ ||
        ce.else_clause_->Type() != TypeTag::kConstantValue ||
        ce.when_clauses_[0].second->Type() != TypeTag::kColumnValue) {
      return false;
    }
    const Value& else_v = ce.else_clause_->AsConstantValue().GetValue();
    if (else_v.IsCaseInsensitive() ||
        (!else_v.IsNull() && (else_v.type != ValueType::kVarChar ||
                              !PlainStringComparable(else_v.AsStringView())))) {
      return false;
    }
    const auto slot = LocalColumnOffset(
        qualified_schema,
        ce.when_clauses_[0].second->AsColumnValue().GetColumnName());
    if (!slot || *slot >= qualified_schema.ColumnCount() ||
        qualified_schema.GetColumn(*slot).Type() != ValueType::kVarChar) {
      return false;
    }
    std::vector<TypedFilterCompare> conds;
    for (const Expression& conj : SplitConjuncts(ce.when_clauses_[0].first)) {
      auto cmp = TryCompileTypedFilterCompare(conj, qualified_schema, true);
      if (!cmp || !cmp->constant_payload || cmp->is_double ||
          cmp->string_payload) {
        return false;
      }
      cmp->left = MapSlot(cmp->left, proj_to_full);
      conds.push_back(std::move(*cmp));
    }
    out->kind = ColKeyKind::kCaseWhenVarChar;
    out->full_col = MapSlot(*slot, proj_to_full);
    out->case_conditions = std::move(conds);
    out->case_else_is_null = else_v.IsNull();
    if (!else_v.IsNull()) {
      out->case_else_str = std::string(else_v.AsStringView());
    }
    return true;
  }
  return false;
}

}  // namespace

StatusOr<Value> MaterializeKeyCellValue(const ColGroupKeySpec& spec,
                                        const RuntimeKeyCell& cell) {
  if (cell.is_null) {
    return Value();
  }
  switch (spec.kind) {
    case ColKeyKind::kColInt64:
    case ColKeyKind::kConstInt64:
    case ColKeyKind::kColInt64Delta:
    case ColKeyKind::kUtcTimestampExtract:
      return Value(cell.int_val);
    case ColKeyKind::kColDate:
      return Value::DateFromDays(cell.int_val);
    case ColKeyKind::kColDouble:
      return Value(cell.double_val);
    case ColKeyKind::kUtcTimestampTrunc:
      return FormatUtcTimestampTruncKey(cell.int_val);
    case ColKeyKind::kColVarChar:
    case ColKeyKind::kCaseWhenVarChar:
    case ColKeyKind::kRegexReplaceVarChar:
      return Value(std::string(cell.str_ptr, cell.str_len));
  }
  return Value();
}

StatusOr<Value> EvaluateBindingValue(
    const OutputSlotBinding& b, const std::vector<ColGroupKeySpec>& key_specs,
    const RuntimeKeyCell* keys, const std::vector<Value>& finished_aggs,
    std::vector<Value>* cached_key_values) {
  switch (b.kind) {
    case OutputSlotBinding::Kind::kConstValue:
      return b.constant;
    case OutputSlotBinding::Kind::kAgg:
      return finished_aggs[b.index];
    case OutputSlotBinding::Kind::kKey: {
      Value& cached = (*cached_key_values)[b.index];
      if (!cached.IsNull() || keys[b.index].is_null) {
        if (keys[b.index].is_null) {
          return Value();
        }
        return cached;
      }
      ASSIGN_OR_RETURN(
          Value, kv,
          MaterializeKeyCellValue(key_specs[b.index], keys[b.index]));
      cached = std::move(kv);
      return cached;
    }
    case OutputSlotBinding::Kind::kPostAgg: {
      Row arg_row;
      arg_row.values_.reserve(b.post_agg->slots.size());
      for (const auto& s : b.post_agg->slots) {
        if (s.is_key) {
          ASSIGN_OR_RETURN(
              Value, kv,
              MaterializeKeyCellValue(key_specs[s.index], keys[s.index]));
          arg_row.values_.push_back(std::move(kv));
        } else {
          arg_row.values_.push_back(finished_aggs[s.index]);
        }
      }
      if (b.post_agg->program.has_value()) {
        return b.post_agg->program->EvaluateRow(arg_row);
      }
      return b.post_agg->expr->TryEvaluate(arg_row, b.post_agg->eval_schema);
    }
  }
  return Value();
}

StatusOr<bool> EvalPositionalHavingOnStates(
    const std::vector<PositionalHaving>& clauses,
    const std::vector<ColAggSpec>& agg_specs, const RuntimeAggState* ast,
    std::vector<Value>* finished_scratch) {
  for (const PositionalHaving& clause : clauses) {
    if (!clause.rhs_is_agg && clause.has_constant &&
        clause.constant.type == ValueType::kInt64 &&
        (agg_specs[clause.agg_index].kind == ColAggKind::kCountStar ||
         agg_specs[clause.agg_index].kind == ColAggKind::kCountCol ||
         agg_specs[clause.agg_index].kind == ColAggKind::kCountByteLen)) {
      const int64_t lv = ast[clause.agg_index].count;
      const int64_t rv = clause.constant.value.int_value;
      bool ok = true;
      switch (clause.op) {
        case BinaryOperation::kEquals:
          ok = lv == rv;
          break;
        case BinaryOperation::kNotEquals:
          ok = lv != rv;
          break;
        case BinaryOperation::kLessThan:
          ok = lv < rv;
          break;
        case BinaryOperation::kLessThanEquals:
          ok = lv <= rv;
          break;
        case BinaryOperation::kGreaterThan:
          ok = lv > rv;
          break;
        case BinaryOperation::kGreaterThanEquals:
          ok = lv >= rv;
          break;
        default:
          ok = true;
          break;
      }
      if (!ok) {
        return false;
      }
      continue;
    }
    (*finished_scratch)[clause.agg_index] =
        FinishAggState(agg_specs[clause.agg_index], ast[clause.agg_index]);
    if (clause.rhs_is_agg) {
      (*finished_scratch)[clause.rhs_agg_index] = FinishAggState(
          agg_specs[clause.rhs_agg_index], ast[clause.rhs_agg_index]);
    }
    const Value* rhs = clause.rhs_is_agg
                           ? &(*finished_scratch)[clause.rhs_agg_index]
                           : nullptr;
    Status err = Status::kSuccess;
    if (!EvalPositionalHavingValues(
            clause, (*finished_scratch)[clause.agg_index], rhs, &err)) {
      return false;
    }
    RETURN_IF_FAIL(err);
  }
  return true;
}

ColumnarStageSchedule BuildColumnarStageSchedule(
    const Schema& table_schema, const ColumnarPredicateSet& preds,
    const std::vector<ColGroupKeySpec>& key_specs,
    const std::vector<ColAggSpec>& agg_specs) {
  const size_t ncols = table_schema.ColumnCount();
  std::vector<uint8_t> in_stage1a(ncols, 0);
  ColumnarStageSchedule sched;
  for (const auto& pc : preds.contains_prechecks) {
    if (in_stage1a[pc.full_col] == 0U) {
      in_stage1a[pc.full_col] = 1;
      sched.stage1a_cols.push_back(pc.full_col);
    }
  }
  std::vector<uint8_t> in_stage1b(ncols, 0);
  std::vector<uint8_t> s1b_bytes(ncols, 0);
  for (size_t c = 0; c < ncols; ++c) {
    if (c < preds.col_referenced.size() && preds.col_referenced[c] != 0U &&
        in_stage1a[c] == 0U) {
      in_stage1b[c] = 1;
      if (c < preds.col_needs_varchar_bytes.size() &&
          preds.col_needs_varchar_bytes[c] != 0U) {
        s1b_bytes[c] = 1;
      }
    }
  }
  std::vector<uint8_t> payload_needed(ncols, 0);
  std::vector<uint8_t> payload_bytes(ncols, 0);
  std::vector<uint32_t> non_case_refs(ncols, 0);
  std::vector<uint32_t> case_target_refs(ncols, 0);
  std::vector<size_t> case_key_owner(ncols, 0);
  for (size_t c = 0; c < ncols; ++c) {
    if (in_stage1a[c] != 0U || in_stage1b[c] != 0U) {
      ++non_case_refs[c];
    }
  }
  for (size_t ki = 0; ki < key_specs.size(); ++ki) {
    const auto& ks = key_specs[ki];
    if (ks.kind == ColKeyKind::kConstInt64) {
      continue;
    }
    payload_needed[ks.full_col] = 1;
    if (table_schema.GetColumn(ks.full_col).Type() == ValueType::kVarChar) {
      payload_bytes[ks.full_col] = 1;
    }
    if (ks.kind == ColKeyKind::kCaseWhenVarChar &&
        !ks.case_conditions.empty()) {
      ++case_target_refs[ks.full_col];
      case_key_owner[ks.full_col] = ki;
    } else {
      ++non_case_refs[ks.full_col];
    }
    for (const auto& cc : ks.case_conditions) {
      payload_needed[cc.left] = 1;
      ++non_case_refs[cc.left];
      if (!cc.constant_payload) {
        payload_needed[cc.right] = 1;
        ++non_case_refs[cc.right];
      }
    }
  }
  for (const auto& as : agg_specs) {
    if (as.kind == ColAggKind::kCountStar) {
      continue;
    }
    payload_needed[as.full_col] = 1;
    ++non_case_refs[as.full_col];
    if (as.needs_bytes) {
      payload_bytes[as.full_col] = 1;
    }
  }
  // Lean proof: lean/TinylambOpt/ClickBenchDiscovery.lean
  // (conditional_key_disjoint_partition_sound): isolate VARCHAR columns
  // referenced exclusively by a single `CASE WHEN` target (e.g. `Referer` in
  // ClickBench Q40) into `cond_cols` so blocks where 0 surviving rows satisfy
  // the guard never read or decompress that column.
  std::vector<uint8_t> is_cond_col(ncols, 0);
  for (size_t c = 0; c < ncols; ++c) {
    if (case_target_refs[c] == 1 && non_case_refs[c] == 0 &&
        table_schema.GetColumn(c).Type() == ValueType::kVarChar) {
      is_cond_col[c] = 1;
      sched.cond_cols.push_back(static_cast<slot_t>(c));
      sched.cond_guards.push_back(key_specs[case_key_owner[c]].case_conditions);
    }
  }
  const bool has_stage1_filters = !sched.stage1a_cols.empty() || !preds.Empty();
  if (!has_stage1_filters) {
    for (size_t c = 0; c < ncols; ++c) {
      if (is_cond_col[c] != 0U) {
        continue;
      }
      if (in_stage1b[c] != 0U || payload_needed[c] != 0U) {
        sched.stage1b_cols.push_back(static_cast<slot_t>(c));
        const bool need_b = s1b_bytes[c] != 0U || payload_bytes[c] != 0U;
        sched.stage1b_len_only.push_back(need_b ? 0 : 1);
      }
    }
  } else {
    for (size_t c = 0; c < ncols; ++c) {
      if (in_stage1b[c] != 0U) {
        if (!sched.stage1a_cols.empty() && s1b_bytes[c] != 0U) {
          sched.stage1c_cols.push_back(static_cast<slot_t>(c));
        } else {
          sched.stage1b_cols.push_back(static_cast<slot_t>(c));
          sched.stage1b_len_only.push_back(
              (s1b_bytes[c] != 0U || payload_bytes[c] != 0U) ? 0 : 1);
        }
      }
    }
    for (size_t c = 0; c < ncols; ++c) {
      if (in_stage1a[c] == 0U && in_stage1b[c] == 0U && is_cond_col[c] == 0U &&
          payload_needed[c] != 0U) {
        sched.stage2_cols.push_back(static_cast<slot_t>(c));
        sched.stage2_len_only.push_back(payload_bytes[c] != 0U ? 0 : 1);
      }
    }
  }
  sched.col_in_payload = std::move(payload_needed);
  return sched;
}

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
    // Decode cheap integer / length-only filter columns (`stage1b_cols`, e.g.
    // `SearchPhrase <> ''` in ClickBench Q22/Q23) FIRST so blocks with 0
    // survivors never read or decompress wide `LIKE` columns (`Title`, `URL`)!
    RETURN_IF_FAIL(tlcs.DecodeBlockColumnsWithOptions(
        bidx, sched.stage1b_cols, table_schema, &scratch->blk1b, true,
        &scratch->io1b, sched.stage1b_len_only));
    *local_scanned += scratch->blk1b.row_count;
    for (size_t i = 0; i < sched.stage1b_cols.size(); ++i) {
      (*col_ptrs)[sched.stage1b_cols[i]] = &scratch->blk1b.columns[i];
    }
    for (uint32_t r = 0; r < scratch->blk1b.row_count; ++r) {
      if (RowMatchesColPtrs(preds, *col_ptrs, r)) {
        cand_rows->push_back(r);
      }
    }
    if (cand_rows->empty()) {
      return false;
    }

    RETURN_IF_FAIL(tlcs.DecodeBlockColumnsWithOptions(
        bidx, sched.stage1a_cols, table_schema, &scratch->blk1a, true,
        &scratch->io1a, {}));
    for (size_t i = 0; i < sched.stage1a_cols.size(); ++i) {
      (*col_ptrs)[sched.stage1a_cols[i]] = &scratch->blk1a.columns[i];
    }
    for (const auto& pc : preds.contains_prechecks) {
      const auto* ch = (*col_ptrs)[pc.full_col];
      if (ch != nullptr && !BlockPassesContainsPrecheck(*ch, pc.needle)) {
        cand_rows->clear();
        return false;
      }
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

    if (!sched.stage1c_cols.empty()) {
      RETURN_IF_FAIL(tlcs.DecodeBlockColumnsWithOptions(
          bidx, sched.stage1c_cols, table_schema, &scratch->blk1c, true,
          &scratch->io1c, {}));
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
    if (cand_rows->empty()) {
      return false;
    }

    if (!sched.stage1c_cols.empty()) {
      RETURN_IF_FAIL(tlcs.DecodeBlockColumnsWithOptions(
          bidx, sched.stage1c_cols, table_schema, &scratch->blk1c, true,
          &scratch->io1c, {}));
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
    if (preds.Empty()) {
      cand_rows->resize(rc);
      for (uint32_t r = 0; r < rc; ++r) {
        (*cand_rows)[r] = r;
      }
    } else if (preds.nulls.empty() && preds.inlists.empty() &&
               preds.compares.size() == 1 &&
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
      bool flat_int_ok = preds.nulls.empty() && preds.inlists.empty() &&
                         !preds.compares.empty() && preds.compares.size() <= 16;
      if (flat_int_ok) {
        for (const TypedFilterCompare& cmp : preds.compares) {
          if (!cmp.constant_payload || cmp.string_payload || cmp.is_double ||
              (*col_ptrs)[cmp.left] == nullptr ||
              (*col_ptrs)[cmp.left]->has_null) {
            flat_int_ok = false;
            break;
          }
        }
      }
      if (flat_int_ok) {
        struct FastIntPred {
          const int64_t* cells;
          BinaryOperation op;
          int64_t c;
          uint8_t priority;
        };
        std::array<FastIntPred, 16> fps{};
        const size_t nf = preds.compares.size();
        for (size_t i = 0; i < nf; ++i) {
          const auto& cmp = preds.compares[i];
          const uint8_t prio =
              (cmp.op == BinaryOperation::kEquals && cmp.int_constant != 0)
                  ? 0
                  : (cmp.op == BinaryOperation::kEquals ? 1 : 2);
          fps[i] = {.cells = (*col_ptrs)[cmp.left]->int_cells.data(),
                    .op = cmp.op,
                    .c = cmp.int_constant,
                    .priority = prio};
        }
        std::sort(fps.begin(), fps.begin() + nf,
                  [](const FastIntPred& a, const FastIntPred& b) {
                    return a.priority < b.priority;
                  });
        for (uint32_t r = 0; r < rc; ++r) {
          bool ok = true;
          for (size_t i = 0; i < nf; ++i) {
            const int64_t lv = fps[i].cells[r];
            const int64_t rv = fps[i].c;
            switch (fps[i].op) {
              case BinaryOperation::kEquals:
                ok = (lv == rv);
                break;
              case BinaryOperation::kNotEquals:
                ok = (lv != rv);
                break;
              case BinaryOperation::kLessThan:
                ok = (lv < rv);
                break;
              case BinaryOperation::kLessThanEquals:
                ok = (lv <= rv);
                break;
              case BinaryOperation::kGreaterThan:
                ok = (lv > rv);
                break;
              case BinaryOperation::kGreaterThanEquals:
                ok = (lv >= rv);
                break;
              default:
                ok = RowMatchesColPtrs(preds, *col_ptrs, r);
                i = nf;
                break;
            }
            if (!ok) {
              break;
            }
          }
          if (ok) {
            cand_rows->push_back(r);
          }
        }
      } else {
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

StatusOr<bool> ValidateDeltaKeysNoOverflow(
    const TlcsFile& tlcs, const std::vector<uint32_t>& candidate_blocks,
    const std::vector<ColGroupKeySpec>& key_specs) {
  std::vector<std::pair<slot_t, int64_t>> checks;
  for (const auto& ks : key_specs) {
    if (ks.kind == ColKeyKind::kColInt64Delta) {
      checks.emplace_back(ks.full_col, ks.const_int);
    }
  }
  if (checks.empty()) {
    return true;
  }
  const uint32_t dg_count = tlcs.DirGroupCount();
  const uint32_t dg_size = tlcs.DirGroupSize();
  std::vector<std::shared_ptr<const TlcsFile::DirGroup>> dir_groups(dg_count);
  for (uint32_t g = 0; g < dg_count; ++g) {
    ASSIGN_OR_RETURN(std::shared_ptr<const TlcsFile::DirGroup>, grp,
                     tlcs.GetDirGroup(g));
    dir_groups[g] = std::move(grp);
  }
  for (const uint32_t bidx : candidate_blocks) {
    const auto& blk = dir_groups[bidx / dg_size]->blocks[bidx % dg_size];
    for (const auto& [col, delta] : checks) {
      const auto& meta = blk.columns[col];
      if (meta.encoding == TlcsColumnEncoding::kConstNull) {
        continue;
      }
      if (!meta.has_min_max) {
        return false;
      }
      int64_t tmp = 0;
      if (__builtin_add_overflow(meta.min_int, delta, &tmp) ||
          __builtin_add_overflow(meta.max_int, delta, &tmp)) {
        return false;
      }
    }
  }
  return true;
}

// Lean proof: lean/TinylambOpt/ClickBenchDiscovery.lean
// (group_by_functional_expr_reduction_sound, group_by_overflow_expr_drop_bug)
StatusOr<KeyProjectionMap> BuildKeyProjectionMap(
    const TlcsFile& tlcs, const std::vector<uint32_t>& candidate_blocks,
    const std::vector<ColGroupKeySpec>& key_specs) {
  KeyProjectionMap km;
  km.full_to_phys.assign(key_specs.size(), -1);
  km.delta_base_phys.assign(key_specs.size(), -1);

  bool any_delta = false;
  for (const auto& ks : key_specs) {
    if (ks.kind == ColKeyKind::kColInt64Delta) {
      any_delta = true;
      break;
    }
  }
  bool delta_safe = false;
  if (any_delta) {
    ASSIGN_OR_RETURN(
        bool, ok,
        ValidateDeltaKeysNoOverflow(tlcs, candidate_blocks, key_specs));
    delta_safe = ok;
  }

  std::vector<std::pair<slot_t, int32_t>> int64_bases;
  for (size_t i = 0; i < key_specs.size(); ++i) {
    const auto& ks = key_specs[i];
    if (ks.kind == ColKeyKind::kConstInt64) {
      continue;
    }
    if (ks.kind == ColKeyKind::kColInt64) {
      const auto p = static_cast<int32_t>(km.phys_specs.size());
      km.phys_specs.push_back(ks);
      km.full_to_phys[i] = p;
      int64_bases.emplace_back(ks.full_col, p);
    }
  }
  if (int64_bases.empty() || !delta_safe) {
    km.phys_specs.clear();
    std::fill(km.full_to_phys.begin(), km.full_to_phys.end(), -1);
    for (size_t i = 0; i < key_specs.size(); ++i) {
      if (key_specs[i].kind == ColKeyKind::kConstInt64) {
        continue;
      }
      km.full_to_phys[i] = static_cast<int32_t>(km.phys_specs.size());
      km.phys_specs.push_back(key_specs[i]);
    }
    return km;
  }

  for (size_t i = 0; i < key_specs.size(); ++i) {
    const auto& ks = key_specs[i];
    if (ks.kind == ColKeyKind::kConstInt64 || km.full_to_phys[i] >= 0) {
      continue;
    }
    if (ks.kind == ColKeyKind::kColInt64Delta) {
      int32_t base_p = -1;
      for (const auto& [col, p] : int64_bases) {
        if (col == ks.full_col) {
          base_p = p;
          break;
        }
      }
      if (base_p >= 0) {
        km.delta_base_phys[i] = base_p;
        continue;
      }
    }
    km.full_to_phys[i] = static_cast<int32_t>(km.phys_specs.size());
    km.phys_specs.push_back(ks);
  }
  return km;
}

namespace {

bool BindExpressionToOutputSlot(const Expression& expr,
                                const std::vector<ColGroupKeySpec>& key_specs,
                                const std::vector<ColAggSpec>& agg_specs,
                                const std::vector<NamedExpression>* select_list,
                                OutputSlotBinding* out) {
  if (!expr) {
    return false;
  }
  if (expr->Type() == TypeTag::kConstantValue) {
    out->kind = OutputSlotBinding::Kind::kConstValue;
    out->constant = expr->AsConstantValue().GetValue();
    return true;
  }
  if (expr->Type() == TypeTag::kAggregateExp) {
    const AggregateExpression* ptr = &expr->AsAggregateExpression();
    for (size_t i = 0; i < agg_specs.size(); ++i) {
      if (agg_specs[i].expr == ptr) {
        out->kind = OutputSlotBinding::Kind::kAgg;
        out->index = i;
        return true;
      }
    }
    const std::string s = expr->ToString();
    for (size_t i = 0; i < agg_specs.size(); ++i) {
      if (agg_specs[i].expr->ToString() == s) {
        out->kind = OutputSlotBinding::Kind::kAgg;
        out->index = i;
        return true;
      }
    }
    return false;
  }
  const std::string expr_str = expr->ToString();
  for (size_t i = 0; i < key_specs.size(); ++i) {
    if (key_specs[i].original_expr_str == expr_str) {
      out->kind = OutputSlotBinding::Kind::kKey;
      out->index = i;
      return true;
    }
  }
  if (select_list != nullptr && expr->Type() == TypeTag::kColumnValue) {
    const ColumnName& cn = expr->AsColumnValue().GetColumnName();
    if (cn.schema.empty()) {
      for (const NamedExpression& se : *select_list) {
        if (!se.name.empty() && IdentifierEquals(se.name, cn.name)) {
          return BindExpressionToOutputSlot(se.expression, key_specs, agg_specs,
                                            nullptr, out);
        }
      }
    }
  }
  return false;
}

}  // namespace

StatusOr<std::optional<Relation>> TryRunColumnarTlcsAggregate(
    TransactionContext& context, const SelectStatement& stmt,
    const Expression& having_expr, const Table& table,
    const std::vector<slot_t>& projection, const Schema& qualified_schema,
    const Schema& table_schema, const TypedAggregationPlan* typed_plan,
    const std::vector<const AggregateExpression*>& aggregate_expressions,
    const std::vector<Expression>& scan_predicates,
    const LSTableStore* resident_store_hint, ExecutionRuntime* runtime,
    bool publish_lstable_marker, const std::string& fold_cache_key,
    std::chrono::steady_clock::time_point scan_begin) {
  static const bool dbg = std::getenv("TINYLAMB_DEBUG_TLCS") != nullptr;
  auto dbg_bail = [&](const char* why) -> std::optional<Relation> {
    if (dbg) {
      (void)std::fprintf(stderr, "[TLCS-AGG-BAIL] %s (typed=%d, ngroups=%zu)\n",
                         why, typed_plan != nullptr ? 1 : 0,
                         stmt.GroupBy().size());
    }
    return std::optional<Relation>{};
  };
  if (std::getenv("TINYLAMB_DISABLE_COLUMNAR_TLCS_AGG") != nullptr) {
    return dbg_bail("disabled by env");
  }
  if (stmt.Distinct() || stmt.HasDistinctOn() || stmt.Qualify() ||
      stmt.WithTies() || stmt.ForUpdate() || stmt.SelectList().empty() ||
      (stmt.HasLimit() &&
       stmt.Offset() > std::numeric_limits<size_t>::max() - stmt.Limit()) ||
      ProcessMemoryBudget::Global().AlwaysSpillEnabled()) {
    return dbg_bail("stmt flags/spill gate");
  }
  if (having_expr &&
      (ContainsQuery(having_expr) || ContainsParameterSlot(having_expr) ||
       ContainsVolatileCall(having_expr))) {
    return dbg_bail("having query/param/volatile");
  }

  std::shared_ptr<const TlcsFile> tlcs =
      ResolveCleanBackingTlcs(table, context);
  if (tlcs == nullptr || tlcs->BlockCount() == 0) {
    return dbg_bail("no clean tlcs");
  }

  // Compile predicates into full table slots.
  ColumnarPredicateSet preds;
  if (typed_plan != nullptr) {
    if (!CompilePredicatesFromTypedPlan(*typed_plan, *tlcs, projection,
                                        table_schema, &preds)) {
      return dbg_bail("typed_plan predicates unsupported");
    }
    if (resident_store_hint != nullptr && !preds.contains_prechecks.empty() &&
        preds.compares.size() >= 3) {
      return dbg_bail("resident uncompressed store faster on multi-LIKE");
    }
  } else {
    if (!CompilePredicatesFromExpressions(scan_predicates, {}, *tlcs,
                                          qualified_schema, projection,
                                          table_schema, &preds)) {
      return dbg_bail("scan_predicates unsupported");
    }
  }

  // Classify aggregates.
  std::vector<ColAggSpec> agg_specs;
  agg_specs.reserve(aggregate_expressions.size());
  bool any_scalar_distinct = false;
  for (const AggregateExpression* agg : aggregate_expressions) {
    ColAggSpec spec;
    if (!TryClassifyAggregate(agg, qualified_schema, projection, table_schema,
                              &spec)) {
      if (dbg && agg != nullptr) {
        (void)std::fprintf(stderr, "[TLCS-AGG-BAIL] agg=%s\n",
                           agg->ToString().c_str());
      }
      return std::optional<Relation>{};
    }
    if (spec.kind == ColAggKind::kCountDistinctInt64 ||
        spec.kind == ColAggKind::kCountDistinctDate ||
        spec.kind == ColAggKind::kCountDistinctDouble ||
        spec.kind == ColAggKind::kCountDistinctVarChar) {
      any_scalar_distinct = true;
    }
    agg_specs.push_back(spec);
  }

  // Classify group keys.
  const bool is_group_all = stmt.IsGroupAll() || stmt.GroupBy().empty();
  std::vector<ColGroupKeySpec> key_specs;
  bool any_regex_key = false;
  bool any_computed_key = false;
  if (!is_group_all) {
    key_specs.reserve(stmt.GroupBy().size());
    for (const Expression& k : stmt.GroupBy()) {
      ColGroupKeySpec ks;
      if (!TryClassifyGroupKey(k, qualified_schema, projection, table_schema,
                               &ks)) {
        if (dbg && k) {
          (void)std::fprintf(stderr, "[TLCS-AGG-BAIL] group_key=%s\n",
                             k->ToString().c_str());
        }
        return std::optional<Relation>{};
      }
      if (ks.kind != ColKeyKind::kColInt64 && ks.kind != ColKeyKind::kColDate &&
          ks.kind != ColKeyKind::kColDouble &&
          ks.kind != ColKeyKind::kColVarChar) {
        any_computed_key = true;
        if (ks.kind == ColKeyKind::kRegexReplaceVarChar) {
          any_regex_key = true;
        }
      }
      key_specs.push_back(std::move(ks));
    }
    if (key_specs.size() > 8) {
      return dbg_bail(">8 group keys");
    }
  }

  // Scalar COUNT(DISTINCT) over large tables is already handled optimally by
  // TryRunParallelScalarCountDistinct / sharded path.
  if (is_group_all && any_scalar_distinct) {
    return dbg_bail("scalar count(distinct)");
  }

  // Compile PositionalHaving.
  std::vector<PositionalHaving> positional_having;
  if (having_expr != nullptr) {
    std::vector<TypedAggregateInput> agg_inputs(agg_specs.size());
    for (size_t i = 0; i < agg_specs.size(); ++i) {
      agg_inputs[i].aggregate = agg_specs[i].expr;
    }
    if (!TryCompilePositionalHavingConjuncts(having_expr, agg_inputs,
                                             &positional_having)) {
      return dbg_bail("having unsupported");
    }
  }

  // Bind output items and ORDER BY items.
  std::vector<OutputSlotBinding> emit_bindings;
  // Bind output items directly against `key_specs` (`stmt.GroupBy()`) and
  // `agg_specs`, falling back to `typed_plan->emit_items[i].post_agg` only when
  // `typed_plan` key indices align 1-to-1 (no dropped constants/dependent
  // keys).
  const bool typed_keys_aligned =
      typed_plan != nullptr && typed_plan->dependent_key_programs.empty() &&
      typed_plan->key_offsets.size() == key_specs.size() &&
      typed_plan->emit_items.size() == stmt.SelectList().size();
  emit_bindings.resize(stmt.SelectList().size());
  for (size_t i = 0; i < stmt.SelectList().size(); ++i) {
    if (BindExpressionToOutputSlot(stmt.SelectList()[i].expression, key_specs,
                                   agg_specs, nullptr, &emit_bindings[i])) {
      continue;
    }
    if (typed_keys_aligned && typed_plan->emit_items[i].post_agg != nullptr) {
      emit_bindings[i].kind = OutputSlotBinding::Kind::kPostAgg;
      emit_bindings[i].post_agg = typed_plan->emit_items[i].post_agg;
      continue;
    }
    if (dbg && stmt.SelectList()[i].expression) {
      (void)std::fprintf(stderr, "[TLCS-AGG-BAIL] select_bind=%s\n",
                         stmt.SelectList()[i].expression->ToString().c_str());
    }
    return std::optional<Relation>{};
  }

  std::vector<ColumnarOrderBinding> order_bindings;
  if (!stmt.OrderBy().empty()) {
    const bool typed_order_aligned =
        typed_keys_aligned &&
        typed_plan->order_items.size() == stmt.OrderBy().size();
    order_bindings.reserve(stmt.OrderBy().size());
    for (size_t i = 0; i < stmt.OrderBy().size(); ++i) {
      const auto& term = stmt.OrderBy()[i];
      OutputSlotBinding ob;
      struct LocalSortKeySpec {
        bool ascending{true};
        std::optional<bool> nulls_first{std::nullopt};
        Collation collation{Collation::kNone};
      } ks{.ascending = term.ascending,
           .nulls_first = term.nulls_first,
           .collation = CollationOfExpression(term.expression)};
      if (!BindExpressionToOutputSlot(term.expression, key_specs, agg_specs,
                                      &stmt.SelectList(), &ob)) {
        if (typed_order_aligned &&
            typed_plan->order_items[i].post_agg != nullptr) {
          ob.kind = OutputSlotBinding::Kind::kPostAgg;
          ob.post_agg = typed_plan->order_items[i].post_agg;
        } else {
          if (dbg && term.expression) {
            (void)std::fprintf(stderr, "[TLCS-AGG-BAIL] order_bind=%s\n",
                               term.expression->ToString().c_str());
          }
          return std::optional<Relation>{};
        }
      }
      order_bindings.push_back(
          {.binding = std::move(ob), .order = MakeSortKeyOrder(ks)});
    }
  }

  // Prune directory blocks.
  const auto dir_begin = std::chrono::steady_clock::now();
  ASSIGN_OR_RETURN(std::vector<uint32_t>, candidate_blocks,
                   SelectCandidateBlocks(*tlcs, preds));
  const double dir_ms = MsSince(dir_begin);

  // Optional Stage 1 -> Stage 2 block survivor memo for CB-3: avoids
  // re-decoding non-matching blocks and re-evaluating expensive `LIKE` / regex
  // predicates in Stage 2 when Stage 1 already filtered the exact same blocks.
  struct Stage1BlockFilterMemo {
    std::vector<uint8_t> block_has_match;
    std::vector<std::vector<uint16_t>> block_rows;
    std::atomic<size_t> cached_rows_total{0};
    bool complete{false};
  };

  // Helper that executes a classified `(key_specs, agg_specs, having, order,
  // emit)` plan over `candidate_blocks` using either the partitioned sharded
  // Wave-B2 engine or the resident worker engine.
  auto run_classified_plan =
      [&](const std::vector<ColAggSpec>& plan_agg_specs,
          const std::vector<PositionalHaving>& plan_having,
          const std::vector<OutputSlotBinding>& plan_emit,
          const std::vector<ColumnarOrderBinding>& plan_order,
          const std::vector<NamedExpression>& plan_select_names,
          ExecutionRuntime* plan_runtime, bool plan_publish_marker,
          Stage1BlockFilterMemo* filter_memo_out =
              nullptr) -> StatusOr<std::optional<Relation>> {
    bool plan_any_byte_len = false;
    bool plan_any_distinct = false;
    for (const ColAggSpec& spec : plan_agg_specs) {
      if (spec.kind == ColAggKind::kCountByteLen ||
          spec.kind == ColAggKind::kSumByteLen ||
          spec.kind == ColAggKind::kAvgByteLen ||
          spec.kind == ColAggKind::kMinByteLen ||
          spec.kind == ColAggKind::kMaxByteLen) {
        plan_any_byte_len = true;
      }
      if (spec.kind == ColAggKind::kCountDistinctInt64 ||
          spec.kind == ColAggKind::kCountDistinctDate ||
          spec.kind == ColAggKind::kCountDistinctDouble ||
          spec.kind == ColAggKind::kCountDistinctVarChar) {
        plan_any_distinct = true;
      }
    }

    // Empty pruned candidate set -> quick clean result!
    if (candidate_blocks.empty()) {
      Relation empty_out(plan_runtime);
      std::vector<Column> out_cols;
      out_cols.reserve(plan_emit.size());
      for (size_t i = 0; i < plan_emit.size(); ++i) {
        ValueType vt = ValueType::kInt64;
        std::string cname = "col_" + std::to_string(i);
        if (i < plan_select_names.size()) {
          cname = ProjectionName(plan_select_names[i], i);
          if (plan_select_names[i].expression) {
            auto rt =
                plan_select_names[i].expression->ResultType(qualified_schema);
            vt = rt.GetType() == TypeTag::kDouble    ? ValueType::kDouble
                 : rt.GetType() == TypeTag::kDate    ? ValueType::kDate
                 : rt.GetType() == TypeTag::kVarChar ? ValueType::kVarChar
                                                     : ValueType::kInt64;
          }
        }
        out_cols.emplace_back(std::move(cname), vt);
      }
      if (is_group_all) {
        std::vector<Value> finished_aggs(plan_agg_specs.size());
        for (size_t a = 0; a < plan_agg_specs.size(); ++a) {
          RuntimeAggState empty_st;
          finished_aggs[a] = FinishAggState(plan_agg_specs[a], empty_st);
        }
        bool passes_having = true;
        for (const PositionalHaving& clause : plan_having) {
          const Value* rhs = clause.rhs_is_agg
                                 ? &finished_aggs[clause.rhs_agg_index]
                                 : nullptr;
          Status err = Status::kSuccess;
          if (!EvalPositionalHavingValues(
                  clause, finished_aggs[clause.agg_index], rhs, &err)) {
            passes_having = false;
            break;
          }
          RETURN_IF_FAIL(err);
        }
        if (passes_having) {
          std::vector<Value> row_vals;
          row_vals.reserve(plan_emit.size());
          std::vector<Value> empty_keys;
          for (const auto& eb : plan_emit) {
            ASSIGN_OR_RETURN(Value, v,
                             EvaluateBindingValue(eb, key_specs, nullptr,
                                                  finished_aggs, &empty_keys));
            row_vals.push_back(std::move(v));
          }
          for (size_t i = 0; i < row_vals.size(); ++i) {
            if (!row_vals[i].IsNull()) {
              out_cols[i] =
                  Column(out_cols[i].Name(), ValueTypeOf(row_vals[i]));
              out_cols[i].SetUnsigned(row_vals[i].IsUnsigned());
            }
          }
          empty_out.AddRow(Row(std::move(row_vals)));
        }
      }
      empty_out.schema = Schema("", std::move(out_cols));
      return std::optional<Relation>(std::move(empty_out));
    }

    // Lean proof: lean/TinylambOpt/ClickBenchDiscovery.lean
    // (timestampTrunc_monotone, contiguous_prefix_preserves_complete_buckets,
    // periodic_extract_breaks_monotone_runs_bug)
    // CB-6: Monotone `TIMESTAMP_TRUNC` sorted-block early cutoff (ClickBench
    // Q43). When grouping by a single `TIMESTAMP_TRUNC(col, unit)` key,
    // ordering by that same key (`Collation::kNone`), and every candidate
    // block's `VarcharBlockBound` certifies canonical space-separated UTC
    // strings (`19` or `22` bytes) without NULLs, `TIMESTAMP_TRUNC` is strictly
    // monotone non-decreasing with respect to byte order. Sorting blocks by
    // `blk_min_trunc` (`ASC`) or `blk_max_trunc` (`DESC`) lets us stop reading
    // remaining blocks the instant `Offset + Limit` distinct buckets are
    // sealed!
    if (!is_group_all && key_specs.size() == 1 &&
        key_specs[0].kind == ColKeyKind::kUtcTimestampTrunc &&
        plan_having.empty() && !plan_any_distinct &&
        plan_agg_specs.size() == 1 &&
        plan_agg_specs[0].kind == ColAggKind::kCountStar && stmt.HasLimit() &&
        plan_order.size() == 1 &&
        plan_order[0].binding.kind == OutputSlotBinding::Kind::kKey &&
        plan_order[0].binding.index == 0 &&
        plan_order[0].order.KeyCollation() != Collation::kCaseInsensitive &&
        candidate_blocks.size() >= 4 &&
        std::all_of(plan_emit.begin(), plan_emit.end(),
                    [](const OutputSlotBinding& b) {
                      return b.kind == OutputSlotBinding::Kind::kKey ||
                             b.kind == OutputSlotBinding::Kind::kAgg ||
                             b.kind == OutputSlotBinding::Kind::kConstValue;
                    })) {
      const size_t cap =
          stmt.Offset() <= std::numeric_limits<size_t>::max() - stmt.Limit()
              ? stmt.Offset() + stmt.Limit()
              : std::numeric_limits<size_t>::max();
      // Serial sorted-block cutoff wins when `cap` buckets seal within the
      // first few blocks (`cap <= candidate_blocks * 16` — e.g. ~32 distinct
      // minutes per 32K-row block); when `cap` spans a large fraction of the
      // candidate blocks (`OFFSET 1000` over only 14 blocks), parallel multi-
      // worker scanning finishes faster.
      const bool cutoff_worthwhile =
          cap <= 64 || cap <= candidate_blocks.size() * 16;
      const slot_t ts_col = key_specs[0].full_col;
      std::shared_ptr<const std::vector<TlcsFile::VarcharBlockBound>> vbounds =
          cutoff_worthwhile ? tlcs->GetCachedVarcharBlockBounds(ts_col)
                            : nullptr;
      if (cutoff_worthwhile && vbounds == nullptr &&
          (tlcs->DictionaryKind(ts_col) == TlcsDictKind::kCompleteNarrow ||
           candidate_blocks.size() > 32 ||
           (preds.Empty() && candidate_blocks.size() >= 4 &&
            cap < candidate_blocks.size() * 64))) {
        auto vb_or = tlcs->GetOrComputeVarcharBlockBounds(ts_col, table_schema);
        if (vb_or.HasValue()) {
          vbounds = vb_or.MoveValue();
        }
      }
      if (vbounds != nullptr && cap > 0 && cap <= 32768) {
        struct BoundedBlock {
          uint32_t bidx{0};
          int64_t min_t{0};
          int64_t max_t{0};
        };
        std::vector<BoundedBlock> bblocks;
        bblocks.reserve(candidate_blocks.size());
        bool bounds_ok = true;
        for (const uint32_t bidx : candidate_blocks) {
          if (bidx >= vbounds->size()) {
            bounds_ok = false;
            break;
          }
          const auto& vb = (*vbounds)[bidx];
          if (vb.is_const_null) {
            if (vb.has_null) {
              bounds_ok = false;
              break;
            }
            continue;
          }
          if (vb.has_null || !vb.has_min_max || !vb.all_canonical_utc_ts ||
              vb.min_truncated || vb.max_truncated ||
              (vb.min_len != 19 && vb.min_len != 22) ||
              (vb.max_len != 19 && vb.max_len != 22)) {
            bounds_ok = false;
            break;
          }
          const auto min_k = TryUtcTimestampTruncKey(
              std::string_view(vb.min_prefix.data(), vb.min_len),
              key_specs[0].trunc_unit);
          const auto max_k = TryUtcTimestampTruncKey(
              std::string_view(vb.max_prefix.data(), vb.max_len),
              key_specs[0].trunc_unit);
          if (!min_k.has_value() || !max_k.has_value() || *min_k > *max_k) {
            bounds_ok = false;
            break;
          }
          bblocks.push_back({.bidx = bidx, .min_t = *min_k, .max_t = *max_k});
        }
        if (bounds_ok && !bblocks.empty()) {
          const bool asc = plan_order[0].order.Ascending();
          std::stable_sort(
              bblocks.begin(), bblocks.end(),
              [asc](const BoundedBlock& a, const BoundedBlock& b) noexcept {
                return asc ? (a.min_t < b.min_t) : (a.max_t > b.max_t);
              });
          const ColumnarStageSchedule cut_sched = BuildColumnarStageSchedule(
              table_schema, preds, key_specs, plan_agg_specs);
          QueryMemoryCharge cut_charge;
          SingleIntWorkerTable itable(1, &cut_charge,
                                      /*count_star_only=*/true);
          BlockDecodeScratch cut_scratch;
          TimestampPrefixCache ts_cache;
          std::vector<const TlcsFile::DecodedColumnChunk*> col_ptrs(
              table_schema.ColumnCount(), nullptr);
          std::vector<uint32_t> cand_rows;
          uint64_t scanned = 0;
          uint64_t matching = 0;

          auto heap_cmp = [asc](int64_t a, int64_t b) noexcept {
            return asc ? (a < b) : (a > b);
          };
          std::vector<int64_t> best_keys;
          best_keys.reserve(cap + 1);
          bool cut_ok = true;
          size_t blocks_visited = 0;

          for (const BoundedBlock& bb : bblocks) {
            if (best_keys.size() == cap) {
              const int64_t cutoff = best_keys.front();
              if (asc ? (bb.min_t > cutoff) : (bb.max_t < cutoff)) {
                break;
              }
            }
            ++blocks_visited;
            auto dec_or = DecodeAndFilterBlock(*tlcs, bb.bidx, table_schema,
                                               preds, cut_sched, &cut_scratch,
                                               &col_ptrs, &cand_rows, &scanned);
            if (!dec_or.HasValue()) {
              cut_ok = false;
              break;
            }
            if (!dec_or.Value()) {
              continue;
            }
            matching += cand_rows.size();
            const auto* kch = col_ptrs[ts_col];
            bool has_last = false;
            int64_t last_k = 0;
            uint32_t last_gid = 0;
            for (const uint32_t r : cand_rows) {
              const auto packed =
                  ts_cache.Trunc(kch->VarcharAt(r), key_specs[0].trunc_unit);
              if (!packed.has_value()) {
                cut_ok = false;
                break;
              }
              const int64_t kval = *packed;
              if (best_keys.size() == cap &&
                  (asc ? (kval > best_keys.front())
                       : (kval < best_keys.front()))) {
                continue;
              }
              uint32_t gid = 0;
              if (has_last && kval == last_k) {
                gid = last_gid;
              } else {
                const uint32_t prev_nn = itable.NonNullCount();
                auto gid_or = itable.FindOrInsertNonNull(kval);
                if (!gid_or.HasValue()) {
                  cut_ok = false;
                  break;
                }
                gid = gid_or.Value();
                if (itable.NonNullCount() > prev_nn) {
                  if (best_keys.size() < cap) {
                    best_keys.push_back(kval);
                    std::push_heap(best_keys.begin(), best_keys.end(),
                                   heap_cmp);
                  } else if (heap_cmp(kval, best_keys.front())) {
                    std::pop_heap(best_keys.begin(), best_keys.end(), heap_cmp);
                    best_keys.back() = kval;
                    std::push_heap(best_keys.begin(), best_keys.end(),
                                   heap_cmp);
                  }
                }
                last_k = kval;
                last_gid = gid;
                has_last = true;
              }
              ++itable.CountAt(gid);
            }
            if (!cut_ok) {
              break;
            }
          }

          if (cut_ok) {
            std::sort(best_keys.begin(), best_keys.end(), heap_cmp);
            if (plan_publish_marker && plan_runtime != nullptr) {
              auto marker = std::make_shared<Relation>(plan_runtime);
              marker->schema = qualified_schema;
              plan_runtime->base_relations.emplace(fold_cache_key,
                                                   std::move(marker));
            }
            Relation output(plan_runtime);
            output.rows.reserve(best_keys.size());
            std::vector<Column> out_cols;
            out_cols.reserve(plan_emit.size());
            for (size_t i = 0; i < plan_emit.size(); ++i) {
              std::string cname = i < plan_select_names.size()
                                      ? ProjectionName(plan_select_names[i], i)
                                      : ("col_" + std::to_string(i));
              out_cols.emplace_back(std::move(cname), ValueType::kInt64);
            }
            std::vector<uint8_t> type_refined(out_cols.size(), 0);
            for (const int64_t kv : best_keys) {
              ASSIGN_OR_RETURN(uint32_t, gid, itable.FindOrInsertNonNull(kv));
              const int64_t cnt = itable.CountAt(gid);
              std::vector<Value> row_vals;
              row_vals.reserve(plan_emit.size());
              for (size_t i = 0; i < plan_emit.size(); ++i) {
                Value v;
                if (plan_emit[i].kind == OutputSlotBinding::Kind::kConstValue) {
                  v = plan_emit[i].constant;
                } else if (plan_emit[i].kind == OutputSlotBinding::Kind::kAgg) {
                  v = Value(cnt);
                } else {
                  ASSIGN_OR_RETURN(Value, ts_v, FormatUtcTimestampTruncKey(kv));
                  v = std::move(ts_v);
                }
                if (type_refined[i] == 0U && !v.IsNull()) {
                  out_cols[i] = Column(out_cols[i].Name(), ValueTypeOf(v));
                  out_cols[i].SetUnsigned(v.IsUnsigned());
                  type_refined[i] = 1;
                }
                row_vals.push_back(std::move(v));
              }
              output.AddRow(Row(std::move(row_vals)));
            }
            output.schema = Schema("", std::move(out_cols));
            if (dbg) {
              (void)std::fprintf(stderr,
                                 "[TLCS-AGG-OK] mono-trunc-cutoff "
                                 "visited=%zu/%zu rows=%zu/%zu\n",
                                 blocks_visited, bblocks.size(), scanned,
                                 matching);
            }
            if (plan_runtime != nullptr) {
              const double scan_ms = MsSince(scan_begin);
              plan_runtime->scan_ms += scan_ms;
              plan_runtime->filter_ms += scan_ms;
              plan_runtime->scan_rows += scanned;
              plan_runtime->scan_output_rows += matching;
              plan_runtime->typed_parallel_aggregations += 1;
              plan_runtime->aggregate_groups += itable.TotalGroups();
              plan_runtime->aggregate_input_rows += matching;
            }
            return std::optional<Relation>(std::move(output));
          }
        }
      }
    }

    const double survive_ratio =
        static_cast<double>(candidate_blocks.size()) /
        static_cast<double>(std::max<uint32_t>(1, tlcs->BlockCount()));
    const bool grouped_admitted =
        is_group_all || survive_ratio < 0.60 ||
        !preds.contains_prechecks.empty() || plan_any_byte_len ||
        any_regex_key ||
        (resident_store_hint == nullptr && candidate_blocks.size() <= 512);
    const bool computed_unadmitted = !is_group_all && any_computed_key &&
                                     !any_regex_key && survive_ratio >= 0.60 &&
                                     candidate_blocks.size() > 512;
    const bool high_key_multi = !is_group_all && key_specs.size() >= 4 &&
                                !any_regex_key && !plan_any_distinct &&
                                candidate_blocks.size() >= 8;
    const bool force_partitioned = (!is_group_all && plan_any_distinct) ||
                                   !grouped_admitted || computed_unadmitted ||
                                   high_key_multi;
    if (force_partitioned) {
      PartitionedColumnarAggRequest part_req{
          .stmt = stmt,
          .tlcs = tlcs,
          .qualified_schema = qualified_schema,
          .table_schema = table_schema,
          .preds = preds,
          .candidate_blocks = candidate_blocks,
          .key_specs = key_specs,
          .agg_specs = plan_agg_specs,
          .positional_having = plan_having,
          .emit_bindings = plan_emit,
          .order_bindings = plan_order,
          .runtime = plan_runtime,
          .publish_lstable_marker = plan_publish_marker,
          .fold_cache_key = fold_cache_key,
          .scan_begin = scan_begin,
          .dir_ms = dir_ms,
          .any_scalar_distinct = plan_any_distinct,
      };
      return TryRunPartitionedColumnarTlcsAggregate(part_req);
    }

    const size_t ncols = table_schema.ColumnCount();
    const ColumnarStageSchedule sched = BuildColumnarStageSchedule(
        table_schema, preds, key_specs, plan_agg_specs);

    const bool has_checked_int_sum =
        std::any_of(plan_agg_specs.begin(), plan_agg_specs.end(),
                    [](const ColAggSpec& spec) {
                      return spec.kind == ColAggKind::kSumInt64;
                    });
    const size_t workers =
        has_checked_int_sum
            ? size_t{1}
            : std::max<size_t>(
                  1, std::min<size_t>({QueryParallelWorkerLimit(
                                           std::thread::hardware_concurrency()),
                                       candidate_blocks.size(), size_t{16}}));

    bool packed_int32_pair = false;
    if (!is_group_all && key_specs.size() == 2 && !plan_any_distinct &&
        (key_specs[0].kind == ColKeyKind::kColInt64 ||
         key_specs[0].kind == ColKeyKind::kColDate) &&
        (key_specs[1].kind == ColKeyKind::kColInt64 ||
         key_specs[1].kind == ColKeyKind::kColDate)) {
      const uint32_t dg_count = tlcs->DirGroupCount();
      const uint32_t dg_size = tlcs->DirGroupSize();
      std::vector<std::shared_ptr<const TlcsFile::DirGroup>> dir_groups(
          dg_count);
      for (uint32_t g = 0; g < dg_count; ++g) {
        ASSIGN_OR_RETURN(std::shared_ptr<const TlcsFile::DirGroup>, grp,
                         tlcs->GetDirGroup(g));
        dir_groups[g] = std::move(grp);
      }
      bool pair_ok = true;
      const slot_t c0 = key_specs[0].full_col;
      const slot_t c1 = key_specs[1].full_col;
      for (const uint32_t bidx : candidate_blocks) {
        const auto& blk = dir_groups[bidx / dg_size]->blocks[bidx % dg_size];
        const auto& m0 = blk.columns[c0];
        const auto& m1 = blk.columns[c1];
        if (m0.has_null || m1.has_null || !m0.has_min_max || !m1.has_min_max ||
            m0.min_int < std::numeric_limits<int32_t>::min() ||
            m0.max_int > std::numeric_limits<int32_t>::max() ||
            m1.min_int < std::numeric_limits<int32_t>::min() ||
            m1.max_int > std::numeric_limits<int32_t>::max()) {
          pair_ok = false;
          break;
        }
      }
      packed_int32_pair = pair_ok;
    }

    const bool single_int_key =
        packed_int32_pair ||
        (!is_group_all && key_specs.size() == 1 && !plan_any_distinct &&
         (key_specs[0].kind == ColKeyKind::kColInt64 ||
          key_specs[0].kind == ColKeyKind::kColDate ||
          key_specs[0].kind == ColKeyKind::kUtcTimestampTrunc ||
          key_specs[0].kind == ColKeyKind::kUtcTimestampExtract));

    std::vector<QueryMemoryCharge> worker_charges(workers);
    if (filter_memo_out != nullptr && !preds.Empty()) {
      filter_memo_out->block_has_match.assign(tlcs->BlockCount(), 0);
      filter_memo_out->block_rows.resize(tlcs->BlockCount());
      filter_memo_out->cached_rows_total.store(0, std::memory_order_relaxed);
      filter_memo_out->complete = false;
    }
    std::vector<std::unique_ptr<PagedStringArena>> scalar_arenas;
    std::vector<std::unique_ptr<WorkerGroupTable>> worker_tables;
    std::vector<std::unique_ptr<SingleIntWorkerTable>> single_int_tables;
    std::vector<std::vector<RuntimeAggState>> scalar_worker_aggs;
    const bool single_int_count_star =
        single_int_key && plan_agg_specs.size() == 1 &&
        plan_agg_specs[0].kind == ColAggKind::kCountStar;
    if (is_group_all) {
      scalar_worker_aggs.resize(workers);
      scalar_arenas.reserve(workers);
      for (size_t w = 0; w < workers; ++w) {
        scalar_worker_aggs[w].resize(plan_agg_specs.size());
        scalar_arenas.push_back(
            std::make_unique<PagedStringArena>(&worker_charges[w]));
      }
    } else if (single_int_key) {
      single_int_tables.reserve(workers);
      for (size_t w = 0; w < workers; ++w) {
        single_int_tables.push_back(std::make_unique<SingleIntWorkerTable>(
            plan_agg_specs.size(), &worker_charges[w], single_int_count_star));
      }
    } else {
      worker_tables.reserve(workers);
      for (size_t w = 0; w < workers; ++w) {
        worker_tables.push_back(std::make_unique<WorkerGroupTable>(
            &key_specs, &plan_agg_specs, &worker_charges[w]));
      }
    }
    std::vector<Status> worker_status(workers, Status::kSuccess);
    std::atomic<size_t> next_idx{0};
    std::atomic<uint64_t> total_scanned{0};
    std::atomic<uint64_t> total_matching{0};

    auto worker_fn = [&](size_t tid) {
      BlockDecodeScratch decode_scratch;
      TimestampPrefixCache ts_cache;
      RegexReplaceCache regex_cache;

      std::vector<uint32_t> cand_rows;
      std::vector<RuntimeKeyCell> probe_keys(key_specs.size());
      std::string regex_scratch;
      uint64_t local_scanned = 0;
      uint64_t local_matching = 0;
      std::vector<const TlcsFile::DecodedColumnChunk*> col_ptrs(ncols, nullptr);

      while (true) {
        const size_t bi = next_idx.fetch_add(1, std::memory_order_relaxed);
        if (bi >= candidate_blocks.size()) {
          break;
        }
        const uint32_t bidx = candidate_blocks[bi];
        auto dec_or = DecodeAndFilterBlock(*tlcs, bidx, table_schema, preds,
                                           sched, &decode_scratch, &col_ptrs,
                                           &cand_rows, &local_scanned);
        if (!dec_or.HasValue()) {
          worker_status[tid] = dec_or.GetStatus();
          return;
        }
        if (!dec_or.Value()) {
          continue;
        }

        if (filter_memo_out != nullptr &&
            bidx < filter_memo_out->block_has_match.size()) {
          filter_memo_out->block_has_match[bidx] = 1;
          if (!cand_rows.empty() && cand_rows.size() <= 4096 &&
              cand_rows.back() <= std::numeric_limits<uint16_t>::max()) {
            const size_t prev = filter_memo_out->cached_rows_total.fetch_add(
                cand_rows.size(), std::memory_order_relaxed);
            if (prev + cand_rows.size() <= 262144) {
              auto& saved = filter_memo_out->block_rows[bidx];
              saved.resize(cand_rows.size());
              for (size_t i = 0; i < cand_rows.size(); ++i) {
                saved[i] = static_cast<uint16_t>(cand_rows[i]);
              }
            }
          }
        }

        local_matching += cand_rows.size();

        if (is_group_all) {
          auto& sa = scalar_worker_aggs[tid];
          if (plan_agg_specs.size() == 1 &&
              plan_agg_specs[0].kind == ColAggKind::kCountStar) {
            sa[0].count += static_cast<int64_t>(cand_rows.size());
            sa[0].has_value = true;
            continue;
          }
          for (const uint32_t r : cand_rows) {
            for (size_t a = 0; a < plan_agg_specs.size(); ++a) {
              Status fold_st = FoldRowIntoAggState(
                  plan_agg_specs[a], col_ptrs, r, sa[a], *scalar_arenas[tid]);
              if (fold_st != Status::kSuccess) {
                worker_status[tid] = std::move(fold_st);
                return;
              }
            }
          }
          continue;
        }

        if (packed_int32_pair) {
          SingleIntWorkerTable& itable = *single_int_tables[tid];
          const auto* kch0 = col_ptrs[key_specs[0].full_col];
          const auto* kch1 = col_ptrs[key_specs[1].full_col];
          if (kch0->has_null || kch1->has_null) {
            worker_status[tid] = StatusError(StatusCode::kInvalidArgument,
                                             "unexpected null in packed int32");
            return;
          }
          bool has_last = false;
          int64_t last_key = 0;
          uint32_t last_gid = 0;
          for (const uint32_t r : cand_rows) {
            const int64_t pkey =
                (static_cast<int64_t>(static_cast<int32_t>(kch0->int_cells[r]))
                 << 32) |
                static_cast<int64_t>(static_cast<uint32_t>(
                    static_cast<int32_t>(kch1->int_cells[r])));
            uint32_t gid = 0;
            if (has_last && pkey == last_key) {
              gid = last_gid;
            } else {
              auto gid_or = itable.FindOrInsertNonNull(pkey);
              if (!gid_or.HasValue()) {
                worker_status[tid] = gid_or.GetStatus();
                return;
              }
              gid = gid_or.Value();
              last_key = pkey;
              last_gid = gid;
              has_last = true;
            }
            if (single_int_count_star) {
              ++itable.CountAt(gid);
            } else {
              RuntimeAggState* st_arr = itable.AggsAt(gid);
              for (size_t a = 0; a < plan_agg_specs.size(); ++a) {
                Status fold_st = FoldRowIntoAggState(
                    plan_agg_specs[a], col_ptrs, r, st_arr[a], itable.Arena());
                if (fold_st != Status::kSuccess) {
                  worker_status[tid] = std::move(fold_st);
                  return;
                }
              }
            }
          }
          continue;
        }

        if (single_int_key) {
          SingleIntWorkerTable& itable = *single_int_tables[tid];
          const ColGroupKeySpec& ks0 = key_specs[0];
          const auto* kch = col_ptrs[ks0.full_col];
          const bool k_no_null = !kch->has_null;
          const bool direct_int_col = (ks0.kind == ColKeyKind::kColInt64 ||
                                       ks0.kind == ColKeyKind::kColDate);
          const bool q28_fast =
              direct_int_col && plan_agg_specs.size() == 2 &&
              plan_agg_specs[0].kind == ColAggKind::kAvgByteLen &&
              plan_agg_specs[1].kind == ColAggKind::kCountStar;
          const auto* b0 =
              q28_fast ? col_ptrs[plan_agg_specs[0].full_col] : nullptr;
          const bool b0_no_null = q28_fast && !b0->has_null;
          std::array<int64_t, 256> dict_ts_vals{};
          size_t dict_ts_nd = 0;
          bool dict_ts_fast = false;
          if (!direct_int_col && kch->dict_encoded && kch->dict != nullptr &&
              kch->dict->StringEntryCount() <= 256 &&
              kch->dict_codes.size() >= cand_rows.back() + 1) {
            dict_ts_fast = true;
            const size_t nd = kch->dict->StringEntryCount();
            for (size_t d = 0; d < nd; ++d) {
              const auto sv = kch->dict->StringEntryAt(d);
              const auto packed = (ks0.kind == ColKeyKind::kUtcTimestampTrunc)
                                      ? ts_cache.Trunc(sv, ks0.trunc_unit)
                                      : ts_cache.Extract(sv, ks0.trunc_unit);
              if (!packed.has_value()) {
                dict_ts_fast = false;
                break;
              }
              dict_ts_vals[d] = *packed;
            }
            dict_ts_nd = nd;
          }
          bool has_last = false;
          int64_t last_key = 0;
          uint32_t last_gid = 0;

          for (const uint32_t r : cand_rows) {
            if (!k_no_null && kch->IsNull(r)) {
              if (single_int_count_star) {
                itable.IncrementNullCount();
                continue;
              }
              auto n_or = itable.FindOrInsertNullAggs();
              if (!n_or.HasValue()) {
                worker_status[tid] = n_or.GetStatus();
                return;
              }
              RuntimeAggState* st_arr = n_or.Value();
              for (size_t a = 0; a < plan_agg_specs.size(); ++a) {
                Status fold_st = FoldRowIntoAggState(
                    plan_agg_specs[a], col_ptrs, r, st_arr[a], itable.Arena());
                if (fold_st != Status::kSuccess) {
                  worker_status[tid] = std::move(fold_st);
                  return;
                }
              }
              continue;
            }
            int64_t kval = 0;
            if (direct_int_col) {
              kval = kch->int_cells[r];
            } else if (dict_ts_fast && kch->dict_codes[r] < dict_ts_nd) {
              kval = dict_ts_vals[kch->dict_codes[r]];
            } else if (ks0.kind == ColKeyKind::kUtcTimestampTrunc) {
              const auto packed =
                  ts_cache.Trunc(kch->VarcharAt(r), ks0.trunc_unit);
              if (!packed.has_value()) {
                worker_status[tid] =
                    StatusError(StatusCode::kInvalidArgument,
                                "timestamp key fell back to generic path");
                return;
              }
              kval = *packed;
            } else {
              const auto packed =
                  ts_cache.Extract(kch->VarcharAt(r), ks0.trunc_unit);
              if (!packed.has_value()) {
                worker_status[tid] =
                    StatusError(StatusCode::kInvalidArgument,
                                "timestamp key fell back to generic path");
                return;
              }
              kval = *packed;
            }
            uint32_t gid = 0;
            if (has_last && kval == last_key) {
              gid = last_gid;
            } else {
              auto gid_or = itable.FindOrInsertNonNull(kval);
              if (!gid_or.HasValue()) {
                worker_status[tid] = gid_or.GetStatus();
                return;
              }
              gid = gid_or.Value();
              last_key = kval;
              last_gid = gid;
              has_last = true;
            }
            if (single_int_count_star) {
              ++itable.CountAt(gid);
            } else if (q28_fast) {
              if (b0_no_null || !b0->IsNull(r)) {
                RuntimeAggState* st_arr = itable.AggsAt(gid);
                st_arr[0].double_val +=
                    static_cast<double>(b0->VarcharLength(r));
                ++st_arr[0].count;
                st_arr[0].has_value = true;
                ++st_arr[1].count;
                st_arr[1].has_value = true;
              }
            } else {
              RuntimeAggState* st_arr = itable.AggsAt(gid);
              for (size_t a = 0; a < plan_agg_specs.size(); ++a) {
                Status fold_st = FoldRowIntoAggState(
                    plan_agg_specs[a], col_ptrs, r, st_arr[a], itable.Arena());
                if (fold_st != Status::kSuccess) {
                  worker_status[tid] = std::move(fold_st);
                  return;
                }
              }
            }
          }
          continue;
        }

        WorkerGroupTable& wtable = *worker_tables[tid];
        for (const uint32_t r : cand_rows) {
          Status key_st = Status::kSuccess;
          for (size_t k = 0; k < key_specs.size(); ++k) {
            if (!ExtractSingleKeyCell(key_specs[k], col_ptrs, r, &probe_keys[k],
                                      &regex_scratch, &ts_cache, &key_st,
                                      &regex_cache)) {
              worker_status[tid] = std::move(key_st);
              return;
            }
          }
          const uint64_t khash =
              GroupKeySliceHasher::HashTyped(probe_keys.data(), key_specs);
          auto gid_or = wtable.FindOrInsert(probe_keys.data(), khash);
          if (!gid_or.HasValue()) {
            worker_status[tid] = gid_or.GetStatus();
            return;
          }
          RuntimeAggState* st_arr = wtable.AggsAt(gid_or.Value());
          for (size_t a = 0; a < plan_agg_specs.size(); ++a) {
            Status fold_st = FoldRowIntoAggState(plan_agg_specs[a], col_ptrs, r,
                                                 st_arr[a], wtable.Arena());
            if (fold_st != Status::kSuccess) {
              worker_status[tid] = std::move(fold_st);
              return;
            }
          }
        }
      }
      total_scanned.fetch_add(local_scanned, std::memory_order_relaxed);
      total_matching.fetch_add(local_matching, std::memory_order_relaxed);
    };

    if (workers == 1) {
      worker_fn(0);
    } else {
      RunParallelTasks(workers, worker_fn);
    }
    for (const Status& st : worker_status) {
      if (st == StatusCode::kInvalidArgument ||
          st == StatusCode::kMemoryLimitExceeded) {
        return std::optional<Relation>{};
      }
      RETURN_IF_FAIL(st);
    }
    if (filter_memo_out != nullptr &&
        !filter_memo_out->block_has_match.empty()) {
      filter_memo_out->complete = true;
    }

    const auto merge_begin = std::chrono::steady_clock::now();

    if (is_group_all) {
      QueryMemoryCharge scalar_merge_charge;
      PagedStringArena scalar_merge_arena(&scalar_merge_charge);
      std::vector<RuntimeAggState> merged(plan_agg_specs.size());
      for (size_t w = 0; w < workers; ++w) {
        for (size_t a = 0; a < plan_agg_specs.size(); ++a) {
          Status mst =
              MergeAggState(plan_agg_specs[a], merged[a],
                            scalar_worker_aggs[w][a], scalar_merge_arena);
          if (mst == StatusCode::kMemoryLimitExceeded) {
            return std::optional<Relation>{};
          }
          RETURN_IF_FAIL(mst);
        }
      }
      std::vector<Value> finished_aggs(plan_agg_specs.size());
      for (size_t a = 0; a < plan_agg_specs.size(); ++a) {
        finished_aggs[a] = FinishAggState(plan_agg_specs[a], merged[a]);
      }
      bool passes = true;
      for (const PositionalHaving& clause : plan_having) {
        const Value* rhs =
            clause.rhs_is_agg ? &finished_aggs[clause.rhs_agg_index] : nullptr;
        Status err = Status::kSuccess;
        if (!EvalPositionalHavingValues(clause, finished_aggs[clause.agg_index],
                                        rhs, &err)) {
          passes = false;
          break;
        }
        RETURN_IF_FAIL(err);
      }
      if (plan_publish_marker && plan_runtime != nullptr) {
        auto marker = std::make_shared<Relation>(plan_runtime);
        marker->schema = qualified_schema;
        plan_runtime->base_relations.emplace(fold_cache_key, std::move(marker));
      }
      Relation out(plan_runtime);
      std::vector<Column> out_cols;
      out_cols.reserve(plan_emit.size());
      if (passes) {
        std::vector<Value> row_vals;
        row_vals.reserve(plan_emit.size());
        std::vector<Value> empty_keys;
        for (size_t i = 0; i < plan_emit.size(); ++i) {
          ASSIGN_OR_RETURN(
              Value, v,
              EvaluateBindingValue(plan_emit[i], key_specs, nullptr,
                                   finished_aggs, &empty_keys));
          std::string cname = i < plan_select_names.size()
                                  ? ProjectionName(plan_select_names[i], i)
                                  : ("col_" + std::to_string(i));
          Column c(std::move(cname), ValueTypeOf(v));
          c.SetUnsigned(v.IsUnsigned());
          out_cols.push_back(std::move(c));
          row_vals.push_back(std::move(v));
        }
        out.AddRow(Row(std::move(row_vals)));
      } else {
        for (size_t i = 0; i < plan_emit.size(); ++i) {
          std::string cname = i < plan_select_names.size()
                                  ? ProjectionName(plan_select_names[i], i)
                                  : ("col_" + std::to_string(i));
          out_cols.emplace_back(std::move(cname), ValueType::kInt64);
        }
      }
      out.schema = Schema("", std::move(out_cols));
      if (plan_runtime != nullptr) {
        const double scan_ms = MsSince(scan_begin);
        plan_runtime->scan_ms += scan_ms;
        plan_runtime->filter_ms += scan_ms;
        plan_runtime->scan_rows += total_scanned.load();
        plan_runtime->scan_output_rows += total_matching.load();
        plan_runtime->aggregate_groups += passes ? 1 : 0;
        plan_runtime->aggregate_input_rows += total_matching.load();
        plan_runtime->aggregate_ms += MsSince(merge_begin);
      }
      return std::optional<Relation>(std::move(out));
    }

    size_t total_worker_groups = 0;
    if (single_int_key) {
      for (const auto& it : single_int_tables) {
        total_worker_groups += it->TotalGroups();
      }
    } else {
      for (const auto& wt : worker_tables) {
        total_worker_groups += wt->NumGroups();
      }
    }

    if (single_int_key && total_worker_groups <= 32768 && plan_having.empty() &&
        !plan_any_distinct && plan_agg_specs.size() == 1 &&
        plan_agg_specs[0].kind == ColAggKind::kCountStar &&
        plan_order.size() == 1 &&
        plan_order[0].order.KeyCollation() != Collation::kCaseInsensitive &&
        (plan_order[0].binding.kind == OutputSlotBinding::Kind::kAgg ||
         (plan_order[0].binding.kind == OutputSlotBinding::Kind::kKey &&
          (key_specs[plan_order[0].binding.index].kind ==
               ColKeyKind::kColInt64 ||
           key_specs[plan_order[0].binding.index].kind ==
               ColKeyKind::kColDate ||
           key_specs[plan_order[0].binding.index].kind ==
               ColKeyKind::kUtcTimestampExtract ||
           key_specs[plan_order[0].binding.index].kind ==
               ColKeyKind::kUtcTimestampTrunc))) &&
        std::all_of(plan_emit.begin(), plan_emit.end(),
                    [](const OutputSlotBinding& b) {
                      return b.kind == OutputSlotBinding::Kind::kKey ||
                             b.kind == OutputSlotBinding::Kind::kAgg ||
                             b.kind == OutputSlotBinding::Kind::kConstValue;
                    })) {
      SingleIntWorkerTable& merged = *single_int_tables[0];
      merged.Reserve(std::min<size_t>(total_worker_groups, 32768));
      for (size_t w = 1; w < workers; ++w) {
        SingleIntWorkerTable& src = *single_int_tables[w];
        for (uint32_t g = 0; g < src.NonNullCount(); ++g) {
          ASSIGN_OR_RETURN(uint32_t, dst_gid,
                           merged.FindOrInsertNonNull(src.KeyAt(g)));
          merged.CountAt(dst_gid) += src.CountAt(g);
        }
        if (src.HasNullGroup()) {
          merged.IncrementNullCount(src.NullCount());
        }
      }
      const OutputSlotBinding& ob0 = plan_order[0].binding;
      const SortKeyOrder& ord0 = plan_order[0].order;
      const bool order_is_agg = (ob0.kind == OutputSlotBinding::Kind::kAgg);
      const size_t n_nn = merged.NonNullCount();
      struct DirectSortItem {
        bool is_null{false};
        int64_t val{0};
        uint32_t gid{0};
      };
      std::vector<DirectSortItem> items;
      items.reserve(n_nn + (merged.HasNullGroup() ? 1 : 0));
      for (uint32_t g = 0; g < n_nn; ++g) {
        int64_t sv = 0;
        if (order_is_agg) {
          sv = merged.CountAt(g);
        } else if (packed_int32_pair) {
          const int64_t pk = merged.KeyAt(g);
          sv = (ob0.index == 0)
                   ? static_cast<int64_t>(static_cast<int32_t>(pk >> 32))
                   : static_cast<int64_t>(
                         static_cast<int32_t>(static_cast<uint32_t>(pk)));
        } else {
          sv = merged.KeyAt(g);
        }
        items.push_back({.is_null = false, .val = sv, .gid = g});
      }
      if (merged.HasNullGroup()) {
        items.push_back({.is_null = !order_is_agg,
                         .val = order_is_agg ? merged.NullCount() : 0,
                         .gid = UINT32_MAX});
      }
      size_t emit_count = items.size();
      if (stmt.HasLimit()) {
        const size_t cap =
            stmt.Offset() <= std::numeric_limits<size_t>::max() - stmt.Limit()
                ? stmt.Offset() + stmt.Limit()
                : std::numeric_limits<size_t>::max();
        emit_count = std::min(emit_count, cap);
      }
      auto dcmp = [&](const DirectSortItem& a,
                      const DirectSortItem& b) noexcept {
        if (a.is_null || b.is_null) {
          if (a.is_null != b.is_null) {
            return a.is_null ? ord0.NullsFirst() : !ord0.NullsFirst();
          }
          return a.gid < b.gid;
        }
        if (a.val != b.val) {
          return ord0.Ascending() ? (a.val < b.val) : (a.val > b.val);
        }
        return a.gid < b.gid;
      };
      if (!items.empty()) {
        if (emit_count < items.size() && emit_count * 2 < items.size()) {
          std::partial_sort(
              items.begin(),
              items.begin() + static_cast<std::ptrdiff_t>(emit_count),
              items.end(), dcmp);
        } else {
          std::sort(items.begin(), items.end(), dcmp);
        }
      }
      if (plan_publish_marker && plan_runtime != nullptr) {
        auto marker = std::make_shared<Relation>(plan_runtime);
        marker->schema = qualified_schema;
        plan_runtime->base_relations.emplace(fold_cache_key, std::move(marker));
      }
      Relation output(plan_runtime);
      output.rows.reserve(emit_count);
      std::vector<Column> out_cols;
      out_cols.reserve(plan_emit.size());
      for (size_t i = 0; i < plan_emit.size(); ++i) {
        ValueType vt = ValueType::kInt64;
        std::string cname = "col_" + std::to_string(i);
        if (i < plan_select_names.size()) {
          cname = ProjectionName(plan_select_names[i], i);
          if (plan_select_names[i].expression) {
            auto rt =
                plan_select_names[i].expression->ResultType(qualified_schema);
            vt = rt.GetType() == TypeTag::kDouble    ? ValueType::kDouble
                 : rt.GetType() == TypeTag::kDate    ? ValueType::kDate
                 : rt.GetType() == TypeTag::kVarChar ? ValueType::kVarChar
                                                     : ValueType::kInt64;
          }
        }
        out_cols.emplace_back(std::move(cname), vt);
      }
      std::vector<uint8_t> type_refined(out_cols.size(), 0);
      const size_t approx_row_bytes = plan_emit.size() * sizeof(Value);
      const size_t flush_gran = Relation::ChargeFlushGranularity();
      for (size_t r = 0; r < emit_count; ++r) {
        const uint32_t g = items[r].gid;
        const int64_t cnt =
            (g == UINT32_MAX) ? merged.NullCount() : merged.CountAt(g);
        std::vector<Value> row_vals;
        row_vals.reserve(plan_emit.size());
        for (size_t i = 0; i < plan_emit.size(); ++i) {
          const OutputSlotBinding& eb = plan_emit[i];
          Value v;
          if (eb.kind == OutputSlotBinding::Kind::kConstValue) {
            v = eb.constant;
          } else if (eb.kind == OutputSlotBinding::Kind::kAgg) {
            v = Value(cnt);
          } else if (g == UINT32_MAX) {
            v = Value();
          } else {
            const int64_t kv =
                packed_int32_pair
                    ? ((eb.index == 0)
                           ? static_cast<int64_t>(
                                 static_cast<int32_t>(merged.KeyAt(g) >> 32))
                           : static_cast<int64_t>(static_cast<int32_t>(
                                 static_cast<uint32_t>(merged.KeyAt(g)))))
                    : merged.KeyAt(g);
            const ColGroupKeySpec& ks = key_specs[eb.index];
            if (ks.kind == ColKeyKind::kColDate) {
              v = Value::DateFromDays(kv);
            } else if (ks.kind == ColKeyKind::kUtcTimestampTrunc) {
              ASSIGN_OR_RETURN(Value, ts_v, FormatUtcTimestampTruncKey(kv));
              v = std::move(ts_v);
            } else {
              v = Value(kv);
            }
          }
          if (type_refined[i] == 0U && !v.IsNull()) {
            out_cols[i] = Column(out_cols[i].Name(), ValueTypeOf(v));
            out_cols[i].SetUnsigned(v.IsUnsigned());
            type_refined[i] = 1;
          }
          row_vals.push_back(std::move(v));
        }
        output.AddRowWithKnownBytes(Row(std::move(row_vals)), approx_row_bytes,
                                    flush_gran);
      }
      output.schema = Schema("", std::move(out_cols));
      if (plan_runtime != nullptr) {
        const double scan_ms = MsSince(scan_begin);
        plan_runtime->scan_ms += scan_ms;
        plan_runtime->filter_ms += scan_ms;
        plan_runtime->scan_rows += total_scanned.load();
        plan_runtime->scan_output_rows += total_matching.load();
        plan_runtime->typed_parallel_aggregations += 1;
        plan_runtime->aggregate_groups += merged.TotalGroups();
        plan_runtime->aggregate_input_rows += total_matching.load();
        plan_runtime->aggregate_ms += MsSince(merge_begin);
      }
      return std::optional<Relation>(std::move(output));
    }

    const size_t merge_shards =
        (workers > 1 && total_worker_groups > 4096)
            ? (total_worker_groups > 65536
                   ? 64
                   : (total_worker_groups > 32768 ? 16 : 8))
            : 1;
    const uint64_t shard_mask = merge_shards - 1;

    struct ShardRef {
      uint32_t worker_id{0};
      uint32_t gid{0};
    };
    std::vector<std::vector<ShardRef>> shard_refs(merge_shards);
    for (size_t s = 0; s < merge_shards; ++s) {
      shard_refs[s].reserve((total_worker_groups / merge_shards) + 16);
    }
    if (single_int_key) {
      for (uint32_t w = 0; w < workers; ++w) {
        const auto& it = *single_int_tables[w];
        for (uint32_t g = 0; g < it.NonNullCount(); ++g) {
          const size_t s =
              merge_shards == 1
                  ? 0
                  : (static_cast<size_t>(it.HashAt(g)) & shard_mask);
          shard_refs[s].push_back({.worker_id = w, .gid = g});
        }
        if (it.HasNullGroup()) {
          const size_t s =
              merge_shards == 1
                  ? 0
                  : (static_cast<size_t>(SingleIntWorkerTable::kNullHash) &
                     shard_mask);
          shard_refs[s].push_back({.worker_id = w, .gid = UINT32_MAX});
        }
      }
    } else {
      for (uint32_t w = 0; w < workers; ++w) {
        const auto& wt = *worker_tables[w];
        for (uint32_t g = 0; g < wt.NumGroups(); ++g) {
          const size_t s =
              merge_shards == 1
                  ? 0
                  : (static_cast<size_t>(wt.HashAt(g)) & shard_mask);
          shard_refs[s].push_back({.worker_id = w, .gid = g});
        }
      }
    }

    enum class FastSingleOrderKind : uint8_t { kNone, kInt, kDouble };
    FastSingleOrderKind fast_order_kind = FastSingleOrderKind::kNone;
    if (plan_order.size() == 1 &&
        plan_order[0].order.KeyCollation() != Collation::kCaseInsensitive) {
      const OutputSlotBinding& b0 = plan_order[0].binding;
      if (b0.kind == OutputSlotBinding::Kind::kAgg) {
        switch (plan_agg_specs[b0.index].kind) {
          case ColAggKind::kCountStar:
          case ColAggKind::kCountCol:
          case ColAggKind::kCountByteLen:
          case ColAggKind::kSumInt64:
          case ColAggKind::kSumByteLen:
          case ColAggKind::kMinInt64:
          case ColAggKind::kMaxInt64:
          case ColAggKind::kMinByteLen:
          case ColAggKind::kMaxByteLen:
          case ColAggKind::kMinDate:
          case ColAggKind::kMaxDate:
            fast_order_kind = FastSingleOrderKind::kInt;
            break;
          case ColAggKind::kAvgDouble:
          case ColAggKind::kAvgByteLen:
          case ColAggKind::kSumDouble:
          case ColAggKind::kMinDouble:
          case ColAggKind::kMaxDouble:
            fast_order_kind = FastSingleOrderKind::kDouble;
            break;
          default:
            break;
        }
      } else if (b0.kind == OutputSlotBinding::Kind::kKey) {
        switch (key_specs[b0.index].kind) {
          case ColKeyKind::kColInt64:
          case ColKeyKind::kConstInt64:
          case ColKeyKind::kColDate:
          case ColKeyKind::kUtcTimestampExtract:
          case ColKeyKind::kUtcTimestampTrunc:
            fast_order_kind = FastSingleOrderKind::kInt;
            break;
          case ColKeyKind::kColDouble:
            fast_order_kind = FastSingleOrderKind::kDouble;
            break;
          default:
            break;
        }
      }
    }

    struct FinalGroupRow {
      uint32_t shard{0};
      uint32_t gid{0};
      bool num_null{false};
      int64_t num_int{0};
      double num_double{0.0};
      uint32_t order_offset{0};
    };
    std::vector<std::vector<Value>> shard_order_keys(merge_shards);
    auto cmp_rows = [&](const FinalGroupRow& a, const FinalGroupRow& b) {
      if (fast_order_kind != FastSingleOrderKind::kNone) {
        const SortKeyOrder& ord0 = plan_order[0].order;
        if (a.num_null || b.num_null) {
          if (a.num_null != b.num_null) {
            return a.num_null ? ord0.NullsFirst() : !ord0.NullsFirst();
          }
        } else if (fast_order_kind == FastSingleOrderKind::kInt) {
          if (a.num_int != b.num_int) {
            return ord0.Ascending() ? (a.num_int < b.num_int)
                                    : (a.num_int > b.num_int);
          }
        } else {
          const bool na = std::isnan(a.num_double);
          const bool nb = std::isnan(b.num_double);
          if (na != nb) {
            return ord0.Ascending() ? na : !na;
          }
          if (!na && a.num_double != b.num_double) {
            return ord0.Ascending() ? (a.num_double < b.num_double)
                                    : (a.num_double > b.num_double);
          }
        }
        return std::tie(a.shard, a.gid) < std::tie(b.shard, b.gid);
      }
      for (size_t k = 0; k < plan_order.size(); ++k) {
        const int c = plan_order[k].order.Compare(
            shard_order_keys[a.shard][a.order_offset + k],
            shard_order_keys[b.shard][b.order_offset + k]);
        if (c != 0) {
          return c < 0;
        }
      }
      return std::tie(a.shard, a.gid) < std::tie(b.shard, b.gid);
    };
    auto cmp_probe_row = [&](const std::vector<Value>& scratch,
                             const FinalGroupRow& probe,
                             const FinalGroupRow& row) {
      const std::vector<Value>& row_keys = shard_order_keys[row.shard];
      for (size_t k = 0; k < plan_order.size(); ++k) {
        const int c = plan_order[k].order.Compare(
            scratch[k], row_keys[row.order_offset + k]);
        if (c != 0) {
          return c < 0;
        }
      }
      return std::tie(probe.shard, probe.gid) < std::tie(row.shard, row.gid);
    };

    const bool bounded_topk = stmt.HasLimit() && !plan_order.empty();
    const size_t topk_cap =
        stmt.HasLimit() ? (stmt.Offset() <= std::numeric_limits<size_t>::max() -
                                                stmt.Limit()
                               ? stmt.Offset() + stmt.Limit()
                               : std::numeric_limits<size_t>::max())
                        : 0;

    std::vector<uint8_t> needed_agg_before_emit(plan_agg_specs.size(), 0);
    for (const PositionalHaving& clause : plan_having) {
      const bool fast_int_count =
          !clause.rhs_is_agg && clause.has_constant &&
          clause.constant.type == ValueType::kInt64 &&
          (plan_agg_specs[clause.agg_index].kind == ColAggKind::kCountStar ||
           plan_agg_specs[clause.agg_index].kind == ColAggKind::kCountCol ||
           plan_agg_specs[clause.agg_index].kind == ColAggKind::kCountByteLen);
      if (!fast_int_count) {
        needed_agg_before_emit[clause.agg_index] = 1;
        if (clause.rhs_is_agg) {
          needed_agg_before_emit[clause.rhs_agg_index] = 1;
        }
      }
    }
    if (fast_order_kind == FastSingleOrderKind::kNone) {
      for (const auto& ob : plan_order) {
        if (ob.binding.kind == OutputSlotBinding::Kind::kAgg) {
          needed_agg_before_emit[ob.binding.index] = 1;
        } else if (ob.binding.kind == OutputSlotBinding::Kind::kPostAgg) {
          std::fill(needed_agg_before_emit.begin(),
                    needed_agg_before_emit.end(), 1);
        }
      }
    }

    std::vector<QueryMemoryCharge> merge_charges(merge_shards);
    std::vector<std::unique_ptr<WorkerGroupTable>> merged_shards(merge_shards);
    std::vector<Status> merge_status(merge_shards, Status::kSuccess);
    std::vector<std::vector<FinalGroupRow>> shard_survivors(merge_shards);
    std::vector<Status> shard_extract_status(merge_shards, Status::kSuccess);

    auto merge_and_extract_fn = [&](size_t s) {
      const bool bypass_merge =
          (!single_int_key && merge_shards == 1 && workers == 1);
      if (!bypass_merge) {
        if (shard_refs[s].empty()) {
          return;
        }
        auto mt = std::make_unique<WorkerGroupTable>(
            &key_specs, &plan_agg_specs, &merge_charges[s]);
        Status st = mt->Reserve(shard_refs[s].size());
        if (st != Status::kSuccess) {
          merge_status[s] = std::move(st);
          return;
        }
        std::array<RuntimeKeyCell, 2> unpack_cells{};
        RuntimeAggState count_only_state;
        auto unpack_or_single = [&](int64_t packed,
                                    const RuntimeKeyCell** out_ptr) {
          if (packed_int32_pair) {
            unpack_cells[0] =
                RuntimeKeyCell{.int_val = static_cast<int64_t>(
                                   static_cast<int32_t>(packed >> 32))};
            unpack_cells[1] = RuntimeKeyCell{
                .int_val = static_cast<int64_t>(
                    static_cast<int32_t>(static_cast<uint32_t>(packed)))};
            *out_ptr = unpack_cells.data();
          } else {
            unpack_cells[0] = RuntimeKeyCell{.int_val = packed};
            *out_ptr = unpack_cells.data();
          }
        };
        for (const ShardRef& ref : shard_refs[s]) {
          Status mst = Status::kSuccess;
          if (single_int_key) {
            SingleIntWorkerTable& src = *single_int_tables[ref.worker_id];
            if (ref.gid == UINT32_MAX) {
              unpack_cells[0] = RuntimeKeyCell{.is_null = true};
              if (single_int_count_star) {
                count_only_state.count = src.NullCount();
                count_only_state.has_value = true;
                mst = mt->MergeFromGroup(unpack_cells.data(),
                                         SingleIntWorkerTable::kNullHash,
                                         &count_only_state, true);
              } else {
                mst = mt->MergeFromGroup(unpack_cells.data(),
                                         SingleIntWorkerTable::kNullHash,
                                         src.NullAggs(),
                                         /*borrow_strings=*/true);
              }
            } else {
              const RuntimeKeyCell* kptr = nullptr;
              unpack_or_single(src.KeyAt(ref.gid), &kptr);
              const uint64_t kh =
                  packed_int32_pair
                      ? GroupKeySliceHasher::HashTyped(kptr, key_specs)
                      : src.HashAt(ref.gid);
              if (single_int_count_star) {
                count_only_state.count = src.CountAt(ref.gid);
                count_only_state.has_value = true;
                mst = mt->MergeFromGroup(kptr, kh, &count_only_state, true);
              } else {
                mst = mt->MergeFromGroup(kptr, kh, src.AggsAt(ref.gid),
                                         /*borrow_strings=*/true);
              }
            }
          } else {
            WorkerGroupTable& src = *worker_tables[ref.worker_id];
            mst = mt->MergeFromGroup(src.KeysAt(ref.gid), src.HashAt(ref.gid),
                                     src.AggsAt(ref.gid),
                                     /*borrow_strings=*/true);
          }
          if (mst != Status::kSuccess) {
            merge_status[s] = std::move(mst);
            return;
          }
        }
        merged_shards[s] = std::move(mt);
      }
      if (!merged_shards[s]) {
        return;
      }

      WorkerGroupTable& mt = *merged_shards[s];
      auto& out_vec = shard_survivors[s];
      std::vector<Value> finished_scratch(plan_agg_specs.size());
      std::vector<Value> cached_keys(key_specs.size());
      std::vector<Value> order_scratch;
      if (fast_order_kind == FastSingleOrderKind::kNone &&
          !plan_order.empty()) {
        order_scratch.resize(plan_order.size());
      }
      const size_t ng = mt.NumGroups();
      const bool use_shard_heap = bounded_topk && topk_cap > 0 && ng > topk_cap;
      if (use_shard_heap) {
        out_vec.reserve(topk_cap + 1);
      } else if (plan_having.empty()) {
        out_vec.reserve(ng);
      }

      for (uint32_t g = 0; g < ng; ++g) {
        const RuntimeAggState* ast = mt.AggsAt(g);
        auto having_or = EvalPositionalHavingOnStates(
            plan_having, plan_agg_specs, ast, &finished_scratch);
        if (!having_or.HasValue()) {
          shard_extract_status[s] = having_or.GetStatus();
          return;
        }
        if (!having_or.Value()) {
          continue;
        }

        if (fast_order_kind != FastSingleOrderKind::kNone) {
          FinalGroupRow probe{.shard = static_cast<uint32_t>(s), .gid = g};
          const OutputSlotBinding& b0 = plan_order[0].binding;
          if (b0.kind == OutputSlotBinding::Kind::kAgg) {
            const RuntimeAggState& st = ast[b0.index];
            switch (plan_agg_specs[b0.index].kind) {
              case ColAggKind::kCountStar:
              case ColAggKind::kCountCol:
              case ColAggKind::kCountByteLen:
                probe.num_int = st.count;
                break;
              case ColAggKind::kSumInt64:
              case ColAggKind::kSumByteLen:
              case ColAggKind::kMinInt64:
              case ColAggKind::kMaxInt64:
              case ColAggKind::kMinByteLen:
              case ColAggKind::kMaxByteLen:
              case ColAggKind::kMinDate:
              case ColAggKind::kMaxDate:
                probe.num_null = !st.has_value;
                probe.num_int = st.int_val;
                break;
              case ColAggKind::kAvgDouble:
              case ColAggKind::kAvgByteLen:
                probe.num_null = (st.count == 0);
                probe.num_double =
                    st.count == 0
                        ? 0.0
                        : (st.double_val / static_cast<double>(st.count));
                break;
              case ColAggKind::kSumDouble:
                probe.num_null = !st.has_value;
                probe.num_double = st.double_val;
                break;
              case ColAggKind::kMinDouble:
              case ColAggKind::kMaxDouble:
                probe.num_null = !st.has_value;
                probe.num_double =
                    st.saw_nan ? std::numeric_limits<double>::quiet_NaN()
                               : st.double_val;
                break;
              default:
                break;
            }
          } else {
            const RuntimeKeyCell& kc = mt.KeysAt(g)[b0.index];
            probe.num_null = kc.is_null;
            if (fast_order_kind == FastSingleOrderKind::kInt) {
              probe.num_int = kc.int_val;
            } else {
              probe.num_double = kc.double_val;
            }
          }
          if (bounded_topk && topk_cap == 0) {
            continue;
          }
          if (use_shard_heap) {
            if (out_vec.size() < topk_cap) {
              out_vec.push_back(std::move(probe));
              std::push_heap(out_vec.begin(), out_vec.end(), cmp_rows);
            } else if (cmp_rows(probe, out_vec.front())) {
              std::pop_heap(out_vec.begin(), out_vec.end(), cmp_rows);
              out_vec.back() = probe;
              std::push_heap(out_vec.begin(), out_vec.end(), cmp_rows);
            }
          } else {
            out_vec.push_back(std::move(probe));
          }
        } else if (!plan_order.empty()) {
          for (size_t a = 0; a < plan_agg_specs.size(); ++a) {
            if (needed_agg_before_emit[a]) {
              finished_scratch[a] = FinishAggState(plan_agg_specs[a], ast[a]);
            }
          }
          std::fill(cached_keys.begin(), cached_keys.end(), Value());
          for (size_t k = 0; k < plan_order.size(); ++k) {
            auto kv_or = EvaluateBindingValue(plan_order[k].binding, key_specs,
                                              mt.KeysAt(g), finished_scratch,
                                              &cached_keys);
            if (!kv_or.HasValue()) {
              shard_extract_status[s] = kv_or.GetStatus();
              return;
            }
            order_scratch[k] = std::move(kv_or.Value());
          }
          if (bounded_topk && topk_cap == 0) {
            continue;
          }
          std::vector<Value>& arena = shard_order_keys[s];
          if (use_shard_heap) {
            FinalGroupRow probe{.shard = static_cast<uint32_t>(s), .gid = g};
            if (out_vec.size() < topk_cap) {
              const auto offset = static_cast<uint32_t>(arena.size());
              for (size_t k = 0; k < plan_order.size(); ++k) {
                arena.push_back(std::move(order_scratch[k]));
              }
              probe.order_offset = offset;
              out_vec.push_back(probe);
              std::push_heap(out_vec.begin(), out_vec.end(), cmp_rows);
            } else if (cmp_probe_row(order_scratch, probe, out_vec.front())) {
              std::pop_heap(out_vec.begin(), out_vec.end(), cmp_rows);
              FinalGroupRow& evicted = out_vec.back();
              for (size_t k = 0; k < plan_order.size(); ++k) {
                arena[evicted.order_offset + k] = std::move(order_scratch[k]);
              }
              evicted.gid = g;
              std::push_heap(out_vec.begin(), out_vec.end(), cmp_rows);
            }
            continue;
          }
          const auto offset = static_cast<uint32_t>(arena.size());
          for (size_t k = 0; k < plan_order.size(); ++k) {
            arena.push_back(std::move(order_scratch[k]));
          }
          out_vec.push_back({.shard = static_cast<uint32_t>(s),
                             .gid = g,
                             .order_offset = offset});
        } else {
          out_vec.push_back({.shard = static_cast<uint32_t>(s), .gid = g});
          if (stmt.HasLimit() && out_vec.size() >= topk_cap) {
            break;
          }
        }
      }
    };

    if (!single_int_key && merge_shards == 1 && workers == 1) {
      merged_shards[0] = std::move(worker_tables[0]);
      merge_and_extract_fn(0);
    } else if (merge_shards == 1) {
      merge_and_extract_fn(0);
    } else {
      const size_t mworkers = std::min(workers, merge_shards);
      std::atomic<size_t> next_s{0};
      RunParallelTasks(mworkers, [&](size_t) {
        while (true) {
          const size_t s = next_s.fetch_add(1, std::memory_order_relaxed);
          if (s >= merge_shards) {
            break;
          }
          merge_and_extract_fn(s);
        }
      });
    }
    for (const Status& st : merge_status) {
      if (st == StatusCode::kMemoryLimitExceeded) {
        return std::optional<Relation>{};
      }
      RETURN_IF_FAIL(st);
    }
    for (const Status& st : shard_extract_status) {
      RETURN_IF_FAIL(st);
    }

    size_t total_merged_groups = 0;
    for (size_t s = 0; s < merge_shards; ++s) {
      if (merged_shards[s]) {
        total_merged_groups += merged_shards[s]->NumGroups();
      }
    }

    size_t total_survivors = 0;
    for (const auto& sv : shard_survivors) {
      total_survivors += sv.size();
    }
    std::vector<FinalGroupRow> survivors;
    survivors.reserve(total_survivors);
    for (auto& sv : shard_survivors) {
      for (auto& item : sv) {
        survivors.push_back(std::move(item));
      }
    }

    size_t emit_count = survivors.size();
    if (stmt.HasLimit()) {
      emit_count = std::min(emit_count, topk_cap);
    }
    if (!plan_order.empty() && !survivors.empty()) {
      if (emit_count < survivors.size() && emit_count * 2 < survivors.size()) {
        std::partial_sort(
            survivors.begin(),
            survivors.begin() + static_cast<std::ptrdiff_t>(emit_count),
            survivors.end(), cmp_rows);
      } else {
        std::sort(survivors.begin(), survivors.end(), cmp_rows);
      }
    }

    if (plan_publish_marker && plan_runtime != nullptr) {
      auto marker = std::make_shared<Relation>(plan_runtime);
      marker->schema = qualified_schema;
      plan_runtime->base_relations.emplace(fold_cache_key, std::move(marker));
    }

    Relation output(plan_runtime);
    output.rows.reserve(emit_count);
    std::vector<Column> out_cols;
    out_cols.reserve(plan_emit.size());
    for (size_t i = 0; i < plan_emit.size(); ++i) {
      ValueType vt = ValueType::kInt64;
      std::string cname = "col_" + std::to_string(i);
      if (i < plan_select_names.size()) {
        cname = ProjectionName(plan_select_names[i], i);
        if (plan_select_names[i].expression) {
          auto rt =
              plan_select_names[i].expression->ResultType(qualified_schema);
          vt = rt.GetType() == TypeTag::kDouble    ? ValueType::kDouble
               : rt.GetType() == TypeTag::kDate    ? ValueType::kDate
               : rt.GetType() == TypeTag::kVarChar ? ValueType::kVarChar
                                                   : ValueType::kInt64;
        }
      }
      out_cols.emplace_back(std::move(cname), vt);
    }
    std::vector<uint8_t> type_refined(out_cols.size(), 0);
    std::vector<Value> full_finished(plan_agg_specs.size());
    std::vector<Value> cached_keys(key_specs.size());
    for (size_t r = 0; r < emit_count; ++r) {
      const FinalGroupRow& fg = survivors[r];
      WorkerGroupTable& mt = *merged_shards[fg.shard];
      const RuntimeAggState* ast = mt.AggsAt(fg.gid);
      for (size_t a = 0; a < plan_agg_specs.size(); ++a) {
        full_finished[a] = FinishAggState(plan_agg_specs[a], ast[a]);
      }
      std::vector<Value> row_vals;
      row_vals.reserve(plan_emit.size());
      std::fill(cached_keys.begin(), cached_keys.end(), Value());
      for (size_t i = 0; i < plan_emit.size(); ++i) {
        ASSIGN_OR_RETURN(
            Value, v,
            EvaluateBindingValue(plan_emit[i], key_specs, mt.KeysAt(fg.gid),
                                 full_finished, &cached_keys));
        if (type_refined[i] == 0U && !v.IsNull()) {
          out_cols[i] = Column(out_cols[i].Name(), ValueTypeOf(v));
          out_cols[i].SetUnsigned(v.IsUnsigned());
          type_refined[i] = 1;
        }
        row_vals.push_back(std::move(v));
      }
      output.AddRow(Row(std::move(row_vals)));
    }
    output.schema = Schema("", std::move(out_cols));

    if (dbg) {
      (void)std::fprintf(stderr,
                         "[TLCS-AGG-OK] grouped dir=%.2fms scan=%.2fms "
                         "merge=%.2fms cands=%zu/%u rows=%zu/%zu groups=%zu\n",
                         dir_ms, MsSince(scan_begin), MsSince(merge_begin),
                         candidate_blocks.size(), tlcs->BlockCount(),
                         total_scanned.load(), total_matching.load(),
                         total_merged_groups);
    }
    if (plan_runtime != nullptr) {
      const double scan_ms = MsSince(scan_begin);
      plan_runtime->scan_ms += scan_ms;
      plan_runtime->filter_ms += scan_ms;
      plan_runtime->scan_rows += total_scanned.load();
      plan_runtime->scan_output_rows += total_matching.load();
      plan_runtime->typed_parallel_aggregations += 1;
      plan_runtime->aggregate_groups += total_merged_groups;
      plan_runtime->aggregate_input_rows += total_matching.load();
      plan_runtime->aggregate_ms += MsSince(merge_begin);
    }
    return std::optional<Relation>(std::move(output));
  };

  // Lean proof: lean/TinylambOpt/ClickBenchDiscovery.lean
  // (two_stage_topn_late_agg_materialization_sound,
  // two_stage_late_agg_sql_in_null_group_bug)
  // CB-3: Two-Stage Top-N Late Aggregate Materialization (ClickBench Q10, Q22,
  // Q23, Q29). When HAVING and ORDER BY ... LIMIT K depend only on lightweight
  // ranking aggregates (`agg1`, e.g. `COUNT(*)`, `AVG(BYTE_LENGTH(...))`),
  // defer expensive payload aggregates (`MIN/MAX(VARCHAR)`, `COUNT(DISTINCT)`,
  // and provably non-overflowing unreferenced aggregates) to a second pass over
  // ONLY the `W <= K` winning group keys using NULL-safe key membership!
  const size_t outer_topk_cap =
      stmt.HasLimit()
          ? (stmt.Offset() <= std::numeric_limits<size_t>::max() - stmt.Limit()
                 ? stmt.Offset() + stmt.Limit()
                 : std::numeric_limits<size_t>::max())
          : 0;
  if (!is_group_all && stmt.HasLimit() && !order_bindings.empty() &&
      outer_topk_cap > 0 && outer_topk_cap <= 256 &&
      !candidate_blocks.empty() && !agg_specs.empty()) {
    auto sum_int64_provably_no_overflow =
        [&](slot_t full_col) -> StatusOr<bool> {
      const uint32_t dg_count = tlcs->DirGroupCount();
      const uint32_t dg_size = tlcs->DirGroupSize();
      std::vector<std::shared_ptr<const TlcsFile::DirGroup>> dir_groups(
          dg_count);
      for (uint32_t g = 0; g < dg_count; ++g) {
        ASSIGN_OR_RETURN(std::shared_ptr<const TlcsFile::DirGroup>, grp,
                         tlcs->GetDirGroup(g));
        dir_groups[g] = std::move(grp);
      }
      __int128 max_pos_sum = 0;
      __int128 min_neg_sum = 0;
      for (const uint32_t bidx : candidate_blocks) {
        const auto& blk = dir_groups[bidx / dg_size]->blocks[bidx % dg_size];
        const auto& meta = blk.columns[full_col];
        if (meta.encoding == TlcsColumnEncoding::kConstNull ||
            blk.row_count == 0) {
          continue;
        }
        if (!meta.has_min_max) {
          return false;
        }
        if (meta.max_int > 0) {
          max_pos_sum += static_cast<__int128>(meta.max_int) *
                         static_cast<__int128>(blk.row_count);
          if (max_pos_sum >
              static_cast<__int128>(std::numeric_limits<int64_t>::max())) {
            return false;
          }
        }
        if (meta.min_int < 0) {
          min_neg_sum += static_cast<__int128>(meta.min_int) *
                         static_cast<__int128>(blk.row_count);
          if (min_neg_sum <
              static_cast<__int128>(std::numeric_limits<int64_t>::min())) {
            return false;
          }
        }
      }
      return true;
    };

    std::vector<uint8_t> is_early(agg_specs.size(), 0);
    for (const PositionalHaving& h : positional_having) {
      is_early[h.agg_index] = 1;
      if (h.rhs_is_agg) {
        is_early[h.rhs_agg_index] = 1;
      }
    }
    for (const ColumnarOrderBinding& ob : order_bindings) {
      if (ob.binding.kind == OutputSlotBinding::Kind::kAgg) {
        is_early[ob.binding.index] = 1;
      } else if (ob.binding.kind == OutputSlotBinding::Kind::kPostAgg &&
                 ob.binding.post_agg != nullptr) {
        for (const auto& s : ob.binding.post_agg->slots) {
          if (!s.is_key && s.index < is_early.size()) {
            is_early[s.index] = 1;
          }
        }
      }
    }
    for (size_t a = 0; a < agg_specs.size(); ++a) {
      if (is_early[a] == 0U && agg_specs[a].kind == ColAggKind::kSumInt64) {
        ASSIGN_OR_RETURN(bool, safe_no_ovf,
                         sum_int64_provably_no_overflow(agg_specs[a].full_col));
        if (!safe_no_ovf) {
          is_early[a] = 1;
        }
      }
    }

    auto is_heavy_agg = [](ColAggKind k) noexcept {
      return k == ColAggKind::kMinVarChar || k == ColAggKind::kMaxVarChar ||
             k == ColAggKind::kCountDistinctInt64 ||
             k == ColAggKind::kCountDistinctDate ||
             k == ColAggKind::kCountDistinctDouble ||
             k == ColAggKind::kCountDistinctVarChar;
    };
    bool early_has_heavy = false;
    bool late_has_heavy = false;
    std::vector<size_t> orig_to_early(agg_specs.size(), SIZE_MAX);
    std::vector<ColAggSpec> early_aggs;
    std::vector<size_t> late_orig_idxs;
    std::vector<ColAggSpec> late_aggs;
    for (size_t a = 0; a < agg_specs.size(); ++a) {
      if (is_early[a] != 0U) {
        if (is_heavy_agg(agg_specs[a].kind)) {
          early_has_heavy = true;
        }
        orig_to_early[a] = early_aggs.size();
        early_aggs.push_back(agg_specs[a]);
      } else {
        if (is_heavy_agg(agg_specs[a].kind)) {
          late_has_heavy = true;
        }
        late_orig_idxs.push_back(a);
        late_aggs.push_back(agg_specs[a]);
      }
    }
    // Ensure Stage 1 has at least one aggregate (`COUNT(*)` if ORDER BY is key)
    // and Stage 2 skips at least one heavy `MIN/MAX(VARCHAR)` or
    // `COUNT(DISTINCT)` aggregate.
    if (!early_has_heavy && late_has_heavy && !early_aggs.empty()) {
      std::vector<PositionalHaving> s1_having = positional_having;
      for (PositionalHaving& h : s1_having) {
        h.agg_index = orig_to_early[h.agg_index];
        if (h.rhs_is_agg) {
          h.rhs_agg_index = orig_to_early[h.rhs_agg_index];
        }
      }
      std::vector<ColumnarOrderBinding> s1_order = order_bindings;
      bool s1_order_ok = true;
      for (ColumnarOrderBinding& ob : s1_order) {
        if (ob.binding.kind == OutputSlotBinding::Kind::kAgg) {
          ob.binding.index = orig_to_early[ob.binding.index];
        } else if (ob.binding.kind == OutputSlotBinding::Kind::kPostAgg) {
          // Clone PostAggExpr with remapped aggregate indices.
          auto cloned = std::make_shared<PostAggExpr>(*ob.binding.post_agg);
          for (auto& sl : cloned->slots) {
            if (!sl.is_key) {
              if (sl.index >= orig_to_early.size() ||
                  orig_to_early[sl.index] == SIZE_MAX) {
                s1_order_ok = false;
                break;
              }
              sl.index = orig_to_early[sl.index];
            }
          }
          ob.binding.post_agg = std::move(cloned);
        }
      }
      if (s1_order_ok) {
        std::vector<OutputSlotBinding> s1_emit;
        s1_emit.reserve(key_specs.size() + early_aggs.size());
        for (size_t k = 0; k < key_specs.size(); ++k) {
          OutputSlotBinding kb;
          kb.kind = OutputSlotBinding::Kind::kKey;
          kb.index = k;
          s1_emit.push_back(std::move(kb));
        }
        for (size_t e = 0; e < early_aggs.size(); ++e) {
          OutputSlotBinding ab;
          ab.kind = OutputSlotBinding::Kind::kAgg;
          ab.index = e;
          s1_emit.push_back(std::move(ab));
        }
        Stage1BlockFilterMemo s1_memo;
        ASSIGN_OR_RETURN(
            std::optional<Relation>, s1_res,
            run_classified_plan(early_aggs, s1_having, s1_emit, s1_order, {},
                                /*plan_runtime=*/nullptr,
                                /*plan_publish_marker=*/false, &s1_memo));
        if (s1_res.has_value()) {
          Relation& s1_rel = *s1_res;
          const size_t n_win = s1_rel.rows.size();
          if (n_win == 0) {
            if (publish_lstable_marker && runtime != nullptr) {
              auto marker = std::make_shared<Relation>(runtime);
              marker->schema = qualified_schema;
              runtime->base_relations.emplace(fold_cache_key,
                                              std::move(marker));
            }
            Relation empty_out(runtime);
            std::vector<Column> out_cols;
            out_cols.reserve(stmt.SelectList().size());
            for (size_t i = 0; i < stmt.SelectList().size(); ++i) {
              ValueType vt = ValueType::kInt64;
              if (stmt.SelectList()[i].expression) {
                auto rt = stmt.SelectList()[i].expression->ResultType(
                    qualified_schema);
                vt = rt.GetType() == TypeTag::kDouble    ? ValueType::kDouble
                     : rt.GetType() == TypeTag::kDate    ? ValueType::kDate
                     : rt.GetType() == TypeTag::kVarChar ? ValueType::kVarChar
                                                         : ValueType::kInt64;
              }
              out_cols.emplace_back(ProjectionName(stmt.SelectList()[i], i),
                                    vt);
            }
            empty_out.schema = Schema("", std::move(out_cols));
            return std::optional<Relation>(std::move(empty_out));
          }

          ASSIGN_OR_RETURN(
              KeyProjectionMap, km,
              BuildKeyProjectionMap(*tlcs, candidate_blocks, key_specs));
          const size_t n_phys = km.phys_specs.size();
          std::vector<size_t> phys_to_full(n_phys, 0);
          for (size_t fk = 0; fk < key_specs.size(); ++fk) {
            if (km.full_to_phys[fk] >= 0) {
              phys_to_full[static_cast<size_t>(km.full_to_phys[fk])] = fk;
            }
          }

          // Build NULL-safe (`IS NOT DISTINCT FROM`) open-addressed lookup
          // table over the `n_win <= 256` winning physical key tuples!
          std::vector<RuntimeKeyCell> win_phys_keys(n_win * n_phys);
          std::vector<RuntimeKeyCell> win_full_keys(n_win * key_specs.size());
          bool key_conv_ok = true;
          auto value_to_cell = [](const ColGroupKeySpec& ks, const Value& v,
                                  RuntimeKeyCell* out) -> bool {
            *out = RuntimeKeyCell{};
            if (v.IsNull()) {
              out->is_null = true;
              return true;
            }
            switch (ks.kind) {
              case ColKeyKind::kColInt64:
              case ColKeyKind::kConstInt64:
              case ColKeyKind::kColInt64Delta:
              case ColKeyKind::kUtcTimestampExtract:
                out->int_val = v.value.int_value;
                return true;
              case ColKeyKind::kColDate:
                out->int_val = v.DateDays();
                return true;
              case ColKeyKind::kColDouble:
                out->double_val = v.value.double_value;
                return true;
              case ColKeyKind::kUtcTimestampTrunc: {
                const auto pk =
                    TryUtcTimestampTruncKey(v.AsStringView(), ks.trunc_unit);
                if (!pk.has_value()) {
                  return false;
                }
                out->int_val = *pk;
                return true;
              }
              case ColKeyKind::kColVarChar:
              case ColKeyKind::kCaseWhenVarChar:
              case ColKeyKind::kRegexReplaceVarChar: {
                const std::string_view sv = v.AsStringView();
                out->str_ptr = sv.data();
                out->str_len = static_cast<uint32_t>(sv.size());
                return true;
              }
            }
            return false;
          };

          for (size_t w = 0; w < n_win && key_conv_ok; ++w) {
            for (size_t fk = 0; fk < key_specs.size(); ++fk) {
              if (!value_to_cell(key_specs[fk], s1_rel.rows[w][fk],
                                 &win_full_keys[(w * key_specs.size()) + fk])) {
                key_conv_ok = false;
                break;
              }
            }
            for (size_t p = 0; p < n_phys; ++p) {
              win_phys_keys[(w * n_phys) + p] =
                  win_full_keys[(w * key_specs.size()) + phys_to_full[p]];
            }
          }

          if (key_conv_ok) {
            const size_t ht_cap =
                std::max<size_t>(16, std::bit_ceil(n_win * 4));
            const size_t ht_mask = ht_cap - 1;
            struct WinSlot {
              uint64_t hash{0};
              uint32_t win_plus_one{0};
            };
            std::vector<WinSlot> win_ht(ht_cap);
            bool win_p0_has_null = false;
            int64_t win_p0_min = std::numeric_limits<int64_t>::max();
            int64_t win_p0_max = std::numeric_limits<int64_t>::min();
            const bool p0_int_prunable =
                n_phys >= 1 &&
                (km.phys_specs[0].kind == ColKeyKind::kColInt64 ||
                 km.phys_specs[0].kind == ColKeyKind::kColDate);

            for (uint32_t w = 0; w < n_win; ++w) {
              const RuntimeKeyCell* pk = &win_phys_keys[w * n_phys];
              if (p0_int_prunable) {
                if (pk[0].is_null) {
                  win_p0_has_null = true;
                } else {
                  win_p0_min = std::min(win_p0_min, pk[0].int_val);
                  win_p0_max = std::max(win_p0_max, pk[0].int_val);
                }
              }
              const uint64_t h =
                  GroupKeySliceHasher::HashTyped(pk, km.phys_specs);
              size_t pos = static_cast<size_t>(h) & ht_mask;
              while (win_ht[pos].win_plus_one != 0) {
                pos = (pos + 1) & ht_mask;
              }
              win_ht[pos] = {.hash = h, .win_plus_one = w + 1};
            }

            // Prune `candidate_blocks` for Pass 2 using `km.phys_specs[0]`
            // integer zone maps when applicable (e.g. `RegionID` in Q10!).
            std::vector<uint32_t> p2_blocks;
            if (p0_int_prunable && !win_p0_has_null &&
                win_p0_min <= win_p0_max) {
              const uint32_t dg_count = tlcs->DirGroupCount();
              const uint32_t dg_size = tlcs->DirGroupSize();
              std::vector<std::shared_ptr<const TlcsFile::DirGroup>> dir_groups(
                  dg_count);
              for (uint32_t g = 0; g < dg_count; ++g) {
                ASSIGN_OR_RETURN(std::shared_ptr<const TlcsFile::DirGroup>, grp,
                                 tlcs->GetDirGroup(g));
                dir_groups[g] = std::move(grp);
              }
              const slot_t p0_col = km.phys_specs[0].full_col;
              p2_blocks.reserve(candidate_blocks.size());
              for (const uint32_t bidx : candidate_blocks) {
                const auto& blk =
                    dir_groups[bidx / dg_size]->blocks[bidx % dg_size];
                const auto& m = blk.columns[p0_col];
                if (m.encoding == TlcsColumnEncoding::kConstNull) {
                  continue;
                }
                if (m.has_min_max &&
                    (m.max_int < win_p0_min || m.min_int > win_p0_max)) {
                  continue;
                }
                p2_blocks.push_back(bidx);
              }
            } else {
              p2_blocks = candidate_blocks;
            }

            if (s1_memo.complete) {
              std::vector<uint32_t> filtered_p2;
              filtered_p2.reserve(p2_blocks.size());
              for (const uint32_t bidx : p2_blocks) {
                if (bidx < s1_memo.block_has_match.size() &&
                    s1_memo.block_has_match[bidx] != 0U) {
                  filtered_p2.push_back(bidx);
                }
              }
              p2_blocks = std::move(filtered_p2);
            }

            const size_t n_late = late_aggs.size();
            const ColumnarStageSchedule p2_key_sched =
                BuildColumnarStageSchedule(table_schema, preds, km.phys_specs,
                                           {});
            // Predicate-free schedule used when Stage 1 cached exact surviving
            // `uint16_t` row offsets for a block (`s1_memo.block_rows[bidx]`).
            std::vector<slot_t> p2_memo_key_cols;
            std::vector<slot_t> p2_memo_late_cols;
            std::vector<uint8_t> p2_memo_late_len_only;
            {
              const ColumnarPredicateSet empty_preds;
              const ColumnarStageSchedule memo_ks = BuildColumnarStageSchedule(
                  table_schema, empty_preds, km.phys_specs, {});
              std::vector<uint8_t> in_memo_key(table_schema.ColumnCount(), 0);
              auto add_memo_key = [&](slot_t c) {
                if (in_memo_key[c] == 0U) {
                  in_memo_key[c] = 1;
                  p2_memo_key_cols.push_back(c);
                }
              };
              for (const slot_t c : memo_ks.stage1a_cols) { add_memo_key(c);
}
              for (const slot_t c : memo_ks.stage1b_cols) { add_memo_key(c);
}
              for (const slot_t c : memo_ks.stage2_cols) { add_memo_key(c);
}
              for (const slot_t c : memo_ks.cond_cols) { add_memo_key(c);
}
              std::vector<uint8_t> memo_late_seen(table_schema.ColumnCount(),
                                                  0);
              std::vector<uint8_t> memo_late_bytes(table_schema.ColumnCount(),
                                                   0);
              for (const ColAggSpec& ls : late_aggs) {
                if (ls.kind == ColAggKind::kCountStar) { continue;
}
                memo_late_seen[ls.full_col] = 1;
                if (ls.needs_bytes) { memo_late_bytes[ls.full_col] = 1;
}
              }
              for (size_t c = 0; c < table_schema.ColumnCount(); ++c) {
                if (memo_late_seen[c] != 0U && in_memo_key[c] == 0U) {
                  p2_memo_late_cols.push_back(static_cast<slot_t>(c));
                  p2_memo_late_len_only.push_back(memo_late_bytes[c] != 0U ? 0
                                                                           : 1);
                }
              }
            }
            const size_t ncols = table_schema.ColumnCount();
            std::vector<uint8_t> in_key_sched(ncols, 0);
            for (const slot_t c : p2_key_sched.stage1a_cols) {
              in_key_sched[c] = 1;
            }
            for (const slot_t c : p2_key_sched.stage1b_cols) {
              in_key_sched[c] = 1;
            }
            for (const slot_t c : p2_key_sched.stage1c_cols) {
              in_key_sched[c] = 1;
            }
            for (const slot_t c : p2_key_sched.stage2_cols) {
              in_key_sched[c] = 1;
            }
            for (const slot_t c : p2_key_sched.cond_cols) {
              in_key_sched[c] = 1;
            }

            // Check which already-scheduled columns still need full VARCHAR
            // bytes for `late_aggs` (`kMinVarChar` on `URL` in Q22/Q23 where
            // `URL` was already in `stage1a_cols` with full bytes!).
            std::vector<slot_t> p2_extra_cols;
            std::vector<uint8_t> p2_extra_len_only;
            std::vector<uint8_t> extra_seen(ncols, 0);
            std::vector<uint8_t> extra_bytes(ncols, 0);
            for (const ColAggSpec& ls : late_aggs) {
              if (ls.kind == ColAggKind::kCountStar) {
                continue;
              }
              extra_seen[ls.full_col] = 1;
              if (ls.needs_bytes) {
                extra_bytes[ls.full_col] = 1;
              }
            }
            ColumnarStageSchedule p2_sched = p2_key_sched;
            for (size_t i = 0; i < p2_sched.stage1b_cols.size(); ++i) {
              if (extra_bytes[p2_sched.stage1b_cols[i]] != 0U) {
                p2_sched.stage1b_len_only[i] = 0;
              }
            }
            for (size_t i = 0; i < p2_sched.stage2_cols.size(); ++i) {
              if (extra_bytes[p2_sched.stage2_cols[i]] != 0U) {
                p2_sched.stage2_len_only[i] = 0;
              }
            }
            for (size_t c = 0; c < ncols; ++c) {
              if (extra_seen[c] != 0U && in_key_sched[c] == 0U) {
                p2_extra_cols.push_back(static_cast<slot_t>(c));
                p2_extra_len_only.push_back(extra_bytes[c] != 0U ? 0 : 1);
              }
            }

            // Separate integer `COUNT(DISTINCT)` late aggs for sharded
            // deduplication (handles skewed top-10 regions in ClickBench Q10
            // without building multi-million-entry maps per worker!).
            std::vector<size_t> int_dist_late_idxs;
            for (size_t la = 0; la < n_late; ++la) {
              if (late_aggs[la].kind == ColAggKind::kCountDistinctInt64 ||
                  late_aggs[la].kind == ColAggKind::kCountDistinctDate ||
                  late_aggs[la].kind == ColAggKind::kCountDistinctDouble) {
                int_dist_late_idxs.push_back(la);
              }
            }
            const bool sharded_int_distinct =
                int_dist_late_idxs.size() == 1 && p2_blocks.size() > 16;
            const size_t int_dist_la =
                sharded_int_distinct ? int_dist_late_idxs[0] : SIZE_MAX;

            const size_t p2_workers = std::max<size_t>(
                1, std::min<size_t>({QueryParallelWorkerLimit(
                                         std::thread::hardware_concurrency()),
                                     std::max<size_t>(1, p2_blocks.size()),
                                     size_t{16}}));
            static constexpr size_t kDistShards = 64;
            struct WinDistPair {
              uint16_t win_id{0};
              int64_t dv{0};
            };
            std::vector<std::vector<std::vector<WinDistPair>>> dist_shards(
                sharded_int_distinct ? kDistShards : 0);
            for (auto& ds : dist_shards) {
              ds.resize(p2_workers);
            }

            std::vector<QueryMemoryCharge> p2_charges(p2_workers);
            std::vector<std::unique_ptr<PagedStringArena>> p2_arenas;
            p2_arenas.reserve(p2_workers);
            std::vector<std::vector<RuntimeAggState>> p2_worker_aggs(
                p2_workers);
            for (size_t w = 0; w < p2_workers; ++w) {
              p2_arenas.push_back(
                  std::make_unique<PagedStringArena>(&p2_charges[w]));
              p2_worker_aggs[w].resize(n_win * n_late);
            }
            std::vector<Status> p2_status(p2_workers, Status::kSuccess);
            std::atomic<size_t> next_p2{0};
            const bool single_direct_p0 =
                n_phys == 1 &&
                (km.phys_specs[0].kind == ColKeyKind::kColInt64 ||
                 km.phys_specs[0].kind == ColKeyKind::kColDate);

            auto p2_worker_fn = [&](size_t tid) {
              BlockDecodeScratch scratch;
              TlcsFile::DecodedBlock blk_extra;
              std::vector<uint8_t> io_extra;
              TimestampPrefixCache ts_cache;
              RegexReplaceCache regex_cache;
              std::string regex_scratch;
              std::vector<const TlcsFile::DecodedColumnChunk*> col_ptrs(
                  ncols, nullptr);
              std::vector<uint32_t> cand_rows;
              std::vector<RuntimeKeyCell> probe_keys(n_phys);
              std::vector<std::pair<uint32_t, uint32_t>> matched_wins;
              uint64_t local_scanned = 0;

              // 4096-slot L1 session dedup filter for sharded int distinct.
              static constexpr size_t kL1DistSlots = 4096;
              struct L1DistEntry {
                uint16_t win_plus_one{0};
                int64_t dv{0};
              };
              std::vector<L1DistEntry> l1_dist(
                  sharded_int_distinct ? kL1DistSlots : 0);

              while (true) {
                const size_t bi =
                    next_p2.fetch_add(1, std::memory_order_relaxed);
                if (bi >= p2_blocks.size()) {
                  break;
                }
                const uint32_t bidx = p2_blocks[bi];
                const bool use_memo_rows = s1_memo.complete &&
                                           bidx < s1_memo.block_rows.size() &&
                                           !s1_memo.block_rows[bidx].empty();
                if (use_memo_rows) {
                  std::fill(col_ptrs.begin(), col_ptrs.end(), nullptr);
                  if (!p2_memo_key_cols.empty()) {
                    Status mst = tlcs->DecodeBlockColumnsWithOptions(
                        bidx, p2_memo_key_cols, table_schema, &scratch.blk1a,
                        true, &scratch.io1a, {});
                    if (mst != Status::kSuccess) {
                      p2_status[tid] = std::move(mst);
                      return;
                    }
                    for (size_t i = 0; i < p2_memo_key_cols.size(); ++i) {
                      col_ptrs[p2_memo_key_cols[i]] = &scratch.blk1a.columns[i];
                    }
                  }
                  const auto& saved = s1_memo.block_rows[bidx];
                  cand_rows.resize(saved.size());
                  for (size_t i = 0; i < saved.size(); ++i) {
                    cand_rows[i] = saved[i];
                  }
                } else {
                  auto dec_or = DecodeAndFilterBlock(
                      *tlcs, bidx, table_schema, preds, p2_sched, &scratch,
                      &col_ptrs, &cand_rows, &local_scanned);
                  if (!dec_or.HasValue()) {
                    p2_status[tid] = dec_or.GetStatus();
                    return;
                  }
                  if (!dec_or.Value()) {
                    continue;
                  }
                }

                matched_wins.clear();
                if (single_direct_p0) {
                  const auto* kch = col_ptrs[km.phys_specs[0].full_col];
                  const bool no_null = !kch->has_null;
                  for (const uint32_t r : cand_rows) {
                    const bool is_null = !no_null && kch->IsNull(r);
                    if (!is_null && !win_p0_has_null &&
                        (kch->int_cells[r] < win_p0_min ||
                         kch->int_cells[r] > win_p0_max)) {
                      continue;
                    }
                    probe_keys[0] =
                        is_null ? RuntimeKeyCell{.is_null = true}
                                : RuntimeKeyCell{.int_val = kch->int_cells[r]};
                    const uint64_t h = GroupKeySliceHasher::HashTyped(
                        probe_keys.data(), km.phys_specs);
                    size_t pos = static_cast<size_t>(h) & ht_mask;
                    while (win_ht[pos].win_plus_one != 0) {
                      const uint32_t w = win_ht[pos].win_plus_one - 1;
                      if (win_ht[pos].hash == h &&
                          GroupKeySliceHasher::Equals(
                              &win_phys_keys[w * n_phys], probe_keys.data(),
                              km.phys_specs)) {
                        matched_wins.emplace_back(r, w);
                        break;
                      }
                      pos = (pos + 1) & ht_mask;
                    }
                  }
                } else {
                  for (const uint32_t r : cand_rows) {
                    Status kst = Status::kSuccess;
                    for (size_t p = 0; p < n_phys; ++p) {
                      if (!ExtractSingleKeyCell(
                              km.phys_specs[p], col_ptrs, r, &probe_keys[p],
                              &regex_scratch, &ts_cache, &kst, &regex_cache)) {
                        p2_status[tid] = std::move(kst);
                        return;
                      }
                    }
                    const uint64_t h = GroupKeySliceHasher::HashTyped(
                        probe_keys.data(), km.phys_specs);
                    size_t pos = static_cast<size_t>(h) & ht_mask;
                    while (win_ht[pos].win_plus_one != 0) {
                      const uint32_t w = win_ht[pos].win_plus_one - 1;
                      if (win_ht[pos].hash == h &&
                          GroupKeySliceHasher::Equals(
                              &win_phys_keys[w * n_phys], probe_keys.data(),
                              km.phys_specs)) {
                        matched_wins.emplace_back(r, w);
                        break;
                      }
                      pos = (pos + 1) & ht_mask;
                    }
                  }
                }

                if (matched_wins.empty()) {
                  continue;
                }

                const auto& need_late_cols =
                    use_memo_rows ? p2_memo_late_cols : p2_extra_cols;
                const auto& need_late_len =
                    use_memo_rows ? p2_memo_late_len_only : p2_extra_len_only;
                if (!need_late_cols.empty()) {
                  Status est = tlcs->DecodeBlockColumnsWithOptions(
                      bidx, need_late_cols, table_schema, &blk_extra, true,
                      &io_extra, need_late_len);
                  if (est != Status::kSuccess) {
                    p2_status[tid] = std::move(est);
                    return;
                  }
                  for (size_t i = 0; i < need_late_cols.size(); ++i) {
                    col_ptrs[need_late_cols[i]] = &blk_extra.columns[i];
                  }
                }

                auto& my_aggs = p2_worker_aggs[tid];
                for (const auto& [r, w] : matched_wins) {
                  RuntimeAggState* base_st = &my_aggs[w * n_late];
                  for (size_t la = 0; la < n_late; ++la) {
                    if (la == int_dist_la) {
                      const ColAggSpec& ds = late_aggs[la];
                      const auto* dch = col_ptrs[ds.full_col];
                      if (dch->IsNull(r)) {
                        continue;
                      }
                      int64_t dv = dch->int_cells[r];
                      if (ds.kind == ColAggKind::kCountDistinctDouble) {
                        const auto d = std::bit_cast<double>(dv);
                        if (d == 0.0) {
                          dv = 0;
                        } else if (std::isnan(d)) {
                          dv = int64_t{0x7ff8000000000000LL};
                        }
                      }
                      const uint64_t dh = MixHash64(
                          static_cast<uint64_t>(dv) ^
                          (static_cast<uint64_t>(w) * 0x9e3779b97f4a7c15ULL));
                      L1DistEntry& le =
                          l1_dist[static_cast<size_t>(dh) & (kL1DistSlots - 1)];
                      if (le.win_plus_one == w + 1 && le.dv == dv) {
                        continue;
                      }
                      le = {.win_plus_one = static_cast<uint16_t>(w + 1),
                            .dv = dv};
                      const size_t s =
                          static_cast<size_t>(dh) & (kDistShards - 1);
                      auto& vec = dist_shards[s][tid];
                      if (vec.size() == vec.capacity()) {
                        const size_t grow =
                            std::max<size_t>(256, vec.capacity()) *
                            sizeof(WinDistPair);
                        if (!p2_charges[tid].TryAdd(grow)) {
                          p2_status[tid] = StatusError(
                              StatusCode::kMemoryLimitExceeded,
                              "two-stage distinct shard exceeded budget");
                          return;
                        }
                        vec.reserve(vec.capacity() +
                                    std::max<size_t>(256, vec.capacity()));
                      }
                      vec.push_back(
                          {.win_id = static_cast<uint16_t>(w), .dv = dv});
                      continue;
                    }
                    Status fst =
                        FoldRowIntoAggState(late_aggs[la], col_ptrs, r,
                                            base_st[la], *p2_arenas[tid]);
                    if (fst != Status::kSuccess) {
                      p2_status[tid] = std::move(fst);
                      return;
                    }
                  }
                }
              }
            };

            if (p2_workers == 1) {
              p2_worker_fn(0);
            } else {
              RunParallelTasks(p2_workers, p2_worker_fn);
            }
            bool p2_mem_exceeded = false;
            for (const Status& st : p2_status) {
              if (st == StatusCode::kInvalidArgument ||
                  st == StatusCode::kMemoryLimitExceeded) {
                p2_mem_exceeded = true;
                break;
              }
              RETURN_IF_FAIL(st);
            }

            if (!p2_mem_exceeded) {
              QueryMemoryCharge merge_charge;
              PagedStringArena merge_arena(&merge_charge);
              std::vector<RuntimeAggState> merged_late(n_win * n_late);
              bool merge_mem_exceeded = false;
              for (size_t w_tid = 0; w_tid < p2_workers && !merge_mem_exceeded;
                   ++w_tid) {
                for (size_t w = 0; w < n_win; ++w) {
                  for (size_t la = 0; la < n_late; ++la) {
                    if (la == int_dist_la) {
                      continue;
                    }
                    Status mst = MergeAggState(
                        late_aggs[la], merged_late[(w * n_late) + la],
                        p2_worker_aggs[w_tid][(w * n_late) + la], merge_arena);
                    if (mst == StatusCode::kMemoryLimitExceeded) {
                      merge_mem_exceeded = true;
                      break;
                    }
                    RETURN_IF_FAIL(mst);
                  }
                }
              }

              std::vector<uint64_t> sharded_dist_counts(n_win, 0);
              if (!merge_mem_exceeded && sharded_int_distinct) {
                const size_t dworkers = std::min(p2_workers, kDistShards);
                std::vector<std::vector<uint64_t>> thread_dcounts(
                    dworkers, std::vector<uint64_t>(n_win, 0));
                std::vector<Status> dstatus(dworkers, Status::kSuccess);
                std::atomic<size_t> next_ds{0};
                RunParallelTasks(dworkers, [&](size_t dtid) {
                  QueryMemoryCharge set_charge;
                  FlatIntKeyMap<uint8_t> dset;
                  while (true) {
                    const size_t s =
                        next_ds.fetch_add(1, std::memory_order_relaxed);
                    if (s >= kDistShards) {
                      break;
                    }
                    size_t total_pairs = 0;
                    for (size_t w = 0; w < p2_workers; ++w) {
                      total_pairs += dist_shards[s][w].size();
                    }
                    if (total_pairs == 0) {
                      continue;
                    }
                    dset.Clear();
                    for (size_t w = 0; w < p2_workers; ++w) {
                      for (const WinDistPair& pr : dist_shards[s][w]) {
                        // Pack `(win_id <= 255)` + 64-bit `dv` via secondary
                        // per-win bitmask inside `FlatIntKeyMap<uint8_t>` when
                        // `n_win <= 8`, or hash-mixed `(win_id, dv)` when `dv`
                        // fits 56 bits; use exact `unordered_set` fallback
                        // when 64-bit full width with `n_win > 1` is needed.
                        if (n_win <= 8) {
                          uint8_t* ptr = dset.TryEmplace(pr.dv);
                          const auto bit =
                              static_cast<uint8_t>(1U << pr.win_id);
                          if ((*ptr & bit) == 0U) {
                            *ptr |= bit;
                            ++thread_dcounts[dtid][pr.win_id];
                          }
                        }
                      }
                    }
                    if (n_win > 8) {
                      struct PairKey {
                        uint16_t w;
                        int64_t v;
                        bool operator==(const PairKey& o) const noexcept {
                          return w == o.w && v == o.v;
                        }
                      };
                      struct PairHash {
                        size_t operator()(const PairKey& k) const noexcept {
                          return static_cast<size_t>(
                              MixHash64(static_cast<uint64_t>(k.v) ^
                                        (static_cast<uint64_t>(k.w) *
                                         0x9e3779b97f4a7c15ULL)));
                        }
                      };
                      std::unordered_set<PairKey, PairHash> exact_set;
                      exact_set.reserve(total_pairs);
                      for (size_t w = 0; w < p2_workers; ++w) {
                        for (const WinDistPair& pr : dist_shards[s][w]) {
                          if (exact_set.insert({pr.win_id, pr.dv}).second) {
                            ++thread_dcounts[dtid][pr.win_id];
                          }
                        }
                      }
                    }
                    dist_shards[s].clear();
                  }
                });
                for (size_t dtid = 0; dtid < dworkers; ++dtid) {
                  for (size_t w = 0; w < n_win; ++w) {
                    sharded_dist_counts[w] += thread_dcounts[dtid][w];
                  }
                }
              }

              if (!merge_mem_exceeded) {
                if (publish_lstable_marker && runtime != nullptr) {
                  auto marker = std::make_shared<Relation>(runtime);
                  marker->schema = qualified_schema;
                  runtime->base_relations.emplace(fold_cache_key,
                                                  std::move(marker));
                }
                Relation out(runtime);
                out.rows.reserve(n_win);
                std::vector<Column> out_cols;
                out_cols.reserve(stmt.SelectList().size());
                for (size_t i = 0; i < stmt.SelectList().size(); ++i) {
                  ValueType vt = ValueType::kInt64;
                  if (stmt.SelectList()[i].expression) {
                    auto rt = stmt.SelectList()[i].expression->ResultType(
                        qualified_schema);
                    vt = rt.GetType() == TypeTag::kDouble ? ValueType::kDouble
                         : rt.GetType() == TypeTag::kDate ? ValueType::kDate
                         : rt.GetType() == TypeTag::kVarChar
                             ? ValueType::kVarChar
                             : ValueType::kInt64;
                  }
                  out_cols.emplace_back(ProjectionName(stmt.SelectList()[i], i),
                                        vt);
                }
                std::vector<uint8_t> type_refined(out_cols.size(), 0);
                std::vector<Value> full_finished(agg_specs.size());
                std::vector<Value> cached_keys(key_specs.size());
                for (size_t w = 0; w < n_win; ++w) {
                  for (size_t a = 0; a < agg_specs.size(); ++a) {
                    if (orig_to_early[a] != SIZE_MAX) {
                      full_finished[a] =
                          s1_rel.rows[w][key_specs.size() + orig_to_early[a]];
                    }
                  }
                  for (size_t la = 0; la < n_late; ++la) {
                    const size_t orig_a = late_orig_idxs[la];
                    if (la == int_dist_la) {
                      full_finished[orig_a] =
                          Value(static_cast<int64_t>(sharded_dist_counts[w]));
                    } else {
                      full_finished[orig_a] = FinishAggState(
                          late_aggs[la], merged_late[(w * n_late) + la]);
                    }
                  }
                  std::fill(cached_keys.begin(), cached_keys.end(), Value());
                  for (size_t fk = 0; fk < key_specs.size(); ++fk) {
                    cached_keys[fk] = s1_rel.rows[w][fk];
                  }
                  std::vector<Value> row_vals;
                  row_vals.reserve(emit_bindings.size());
                  for (size_t i = 0; i < emit_bindings.size(); ++i) {
                    ASSIGN_OR_RETURN(Value, v,
                                     EvaluateBindingValue(
                                         emit_bindings[i], key_specs,
                                         &win_full_keys[w * key_specs.size()],
                                         full_finished, &cached_keys));
                    if (type_refined[i] == 0U && !v.IsNull()) {
                      out_cols[i] = Column(out_cols[i].Name(), ValueTypeOf(v));
                      out_cols[i].SetUnsigned(v.IsUnsigned());
                      type_refined[i] = 1;
                    }
                    row_vals.push_back(std::move(v));
                  }
                  out.AddRow(Row(std::move(row_vals)));
                }
                out.schema = Schema("", std::move(out_cols));
                if (dbg) {
                  (void)std::fprintf(stderr,
                                     "[TLCS-AGG-OK] two-stage-topn winners=%zu "
                                     "p2_blocks=%zu/%zu "
                                     "total=%.2fms\n",
                                     n_win, p2_blocks.size(),
                                     candidate_blocks.size(),
                                     MsSince(scan_begin));
                }
                if (runtime != nullptr) {
                  runtime->typed_parallel_aggregations += 1;
                  runtime->aggregate_groups += n_win;
                }
                return std::optional<Relation>(std::move(out));
              }
            }
          }
        }
      }
    }
  }

  return run_classified_plan(agg_specs, positional_having, emit_bindings,
                             order_bindings, stmt.SelectList(), runtime,
                             publish_lstable_marker);
}

}  // namespace tinylamb::relational_detail
