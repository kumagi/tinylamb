/**
 * Copyright 2026 KUMAZAKI Hiroki
 * Licensed under the Apache License, Version 2.0.
 */

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/status_or.hpp"
#include "database/transaction_context.hpp"
#include "executor/detail/expression_eval.hpp"
#include "executor/detail/window_eval.hpp"
#include "expression/aggregate_expression.hpp"
#include "expression/binary_expression.hpp"
#include "expression/bytecode.hpp"
#include "expression/cast_expression.hpp"
#include "expression/column_value.hpp"
#include "expression/constant_value.hpp"
#include "expression/expression.hpp"
#include "expression/function_call_expression.hpp"
#include "expression/named_expression.hpp"
#include "expression/parameter_slot.hpp"
#include "expression/query_expression.hpp"
#include "expression/rewrite.hpp"
#include "expression/unary_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "query/select_rewrites.hpp"
#include "query/statement.hpp"
#include "table/lstable_bridge.hpp"
#include "table/table.hpp"
#include "table/tlcs_file.hpp"
#include "type/collation.hpp"
#include "type/column.hpp"
#include "type/column_name.hpp"
#include "type/constraint.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"
#include "type/value.hpp"
#include "type/value_type.hpp"

namespace tinylamb {

namespace {

bool IdentifierEqualsCI(std::string_view left, std::string_view right) {
  return left.size() == right.size() &&
         std::equal(left.begin(), left.end(), right.begin(),
                    [](char lhs, char rhs) {
                      return std::tolower(static_cast<unsigned char>(lhs)) ==
                             std::tolower(static_cast<unsigned char>(rhs));
                    });
}

std::string AsciiLower(std::string_view s) {
  std::string out(s);
  for (char& ch : out) {
    ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
  }
  return out;
}

std::vector<Expression> SplitAndConjuncts(const Expression& expr) {
  std::vector<Expression> out;
  if (!expr) {
    return out;
  }
  std::vector<Expression> stack{expr};
  while (!stack.empty()) {
    Expression cur = std::move(stack.back());
    stack.pop_back();
    if (!cur) {
      continue;
    }
    if (cur->Type() == TypeTag::kBinaryExp &&
        cur->AsBinaryExpression().Op() == BinaryOperation::kAnd) {
      stack.push_back(cur->AsBinaryExpression().Right());
      stack.push_back(cur->AsBinaryExpression().Left());
    } else {
      out.push_back(std::move(cur));
    }
  }
  return out;
}

std::vector<Expression> SplitOrDisjuncts(const Expression& expr) {
  std::vector<Expression> out;
  if (!expr) {
    return out;
  }
  std::vector<Expression> stack{expr};
  while (!stack.empty()) {
    Expression cur = std::move(stack.back());
    stack.pop_back();
    if (!cur) {
      continue;
    }
    if (cur->Type() == TypeTag::kBinaryExp &&
        cur->AsBinaryExpression().Op() == BinaryOperation::kOr) {
      stack.push_back(cur->AsBinaryExpression().Right());
      stack.push_back(cur->AsBinaryExpression().Left());
    } else {
      out.push_back(std::move(cur));
    }
  }
  return out;
}

Expression CombineAndConjuncts(const std::vector<Expression>& conjuncts) {
  Expression acc = nullptr;
  for (const Expression& c : conjuncts) {
    if (!c) {
      continue;
    }
    acc = acc ? BinaryExpressionExp(acc, BinaryOperation::kAnd, c) : c;
  }
  return acc;
}

Expression CombineOrDisjuncts(const std::vector<Expression>& disjuncts) {
  Expression acc = nullptr;
  for (const Expression& d : disjuncts) {
    if (!d) {
      continue;
    }
    acc = acc ? BinaryExpressionExp(acc, BinaryOperation::kOr, d) : d;
  }
  return acc;
}

bool ExprHasParamOrSubqueryOrVolatile(  // NOLINT(misc-no-recursion)
    const Expression& expr) {
  if (!expr) {
    return false;
  }
  if (expr->Type() == TypeTag::kQueryExp ||
      expr->Type() == TypeTag::kWindowFunctionExp ||
      dynamic_cast<const ParameterSlot*>(expr.get()) != nullptr) {
    return true;
  }
  if (expr->Type() == TypeTag::kFunctionCallExp) {
    // Use the single source of truth in expression/rewrite.hpp. A local name
    // list drifted from GetFunctionVolatility() and let stable functions such
    // as CURRENT_DATETIME (and volatile NEWID) through, which plan-time
    // folding then froze into a cacheable Value.
    if (GetFunctionVolatility(expr->AsFunctionCallExpression().FuncName()) !=
        Volatility::kImmutable) {
      return true;
    }
  }
  return std::ranges::any_of(ExpressionChildren(expr),
                             ExprHasParamOrSubqueryOrVolatile);
}

bool ExprContainsAggregateNode(  // NOLINT(misc-no-recursion)
    const Expression& expr) {
  if (!expr) {
    return false;
  }
  if (expr->Type() == TypeTag::kAggregateExp) {
    return true;
  }
  if (expr->Type() == TypeTag::kQueryExp) {
    return false;
  }
  return std::ranges::any_of(ExpressionChildren(expr),
                             ExprContainsAggregateNode);
}

bool ExprAstEquals(  // NOLINT(misc-no-recursion)
    const Expression& a, const Expression& b) {
  if (a == b) {
    return true;
  }
  if (!a || !b || a->Type() != b->Type()) {
    return false;
  }
  if (a->Type() == TypeTag::kColumnValue) {
    const ColumnName& ca = a->AsColumnValue().GetColumnName();
    const ColumnName& cb = b->AsColumnValue().GetColumnName();
    return IdentifierEqualsCI(ca.schema, cb.schema) &&
           IdentifierEqualsCI(ca.name, cb.name);
  }
  if (a->Type() == TypeTag::kConstantValue) {
    if (dynamic_cast<const ParameterSlot*>(a.get()) != nullptr ||
        dynamic_cast<const ParameterSlot*>(b.get()) != nullptr) {
      return false;
    }
    const Value& va = a->AsConstantValue().GetValue();
    const Value& vb = b->AsConstantValue().GetValue();
    if (va.IsNull() || vb.IsNull()) {
      return va.IsNull() && vb.IsNull() && va.type == vb.type;
    }
    return va.type == vb.type && va.IsUnsigned() == vb.IsUnsigned() &&
           va.Collation() == vb.Collation() && va == vb;
  }
  if (a->ToString() != b->ToString()) {
    return false;
  }
  const auto ca = ExpressionChildren(a);
  const auto cb = ExpressionChildren(b);
  if (ca.size() != cb.size()) {
    return false;
  }
  for (size_t i = 0; i < ca.size(); ++i) {
    if (!ExprAstEquals(ca[i], cb[i])) {
      return false;
    }
  }
  return true;
}

bool SelectListHasStarItem(const SelectStatement& stmt) {
  return std::ranges::any_of(
      stmt.SelectList(), [](const NamedExpression& item) {
        if (item.star_modifiers != nullptr) {
          return true;
        }
        if (!item.expression) {
          return false;
        }
        if (item.expression->Type() == TypeTag::kColumnValue &&
            item.expression->AsColumnValue().GetColumnName().name == "*") {
          return true;
        }
        if (item.expression->Type() == TypeTag::kFunctionCallExp) {
          const auto& fn = item.expression->AsFunctionCallExpression();
          if (fn.FuncName() == "__value_table_value" ||
              fn.FuncName() == "__proto_new") {
            return true;
          }
        }
        return false;
      });
}

struct ResolvedBaseRel {
  size_t rel_idx{0};
  std::string table_name;
  std::string qualifier;
  bool has_explicit_alias{false};
  std::shared_ptr<Table> table;
  Schema schema;
};

struct ResolvedColRef {
  size_t rel_idx{0};
  slot_t slot{0};
  bool operator==(const ResolvedColRef& o) const noexcept {
    return rel_idx == o.rel_idx && slot == o.slot;
  }
};

bool ResolvePlainBaseSources(const SelectStatement& stmt,
                             const std::unordered_set<std::string>& cte_names,
                             TransactionContext& ctx,
                             std::vector<ResolvedBaseRel>* out) {
  out->clear();
  if (stmt.Sources().empty()) {
    return false;
  }
  std::unordered_set<std::string> seen_quals;
  seen_quals.reserve(stmt.Sources().size());
  out->reserve(stmt.Sources().size());
  for (size_t i = 0; i < stmt.Sources().size(); ++i) {
    const SelectSource& src = stmt.Sources()[i];
    if (src.query != nullptr || src.unnest != nullptr || src.is_lateral ||
        src.table.empty() || src.sample.has_value() ||
        !src.using_columns.empty() || src.from_nested_join) {
      return false;
    }
    if (cte_names.contains(AsciiLower(src.table)) ||
        stmt.WithQueries().contains(src.table)) {
      return false;
    }
    StatusOr<std::shared_ptr<Table>> tbl = ctx.GetTable(src.table);
    if (!tbl.HasValue() || tbl.Value() == nullptr) {
      return false;
    }
    const std::string qual = src.alias.empty() ? src.table : src.alias;
    if (!seen_quals.insert(AsciiLower(qual)).second) {
      return false;
    }
    out->push_back(ResolvedBaseRel{
        .rel_idx = i,
        .table_name = src.table,
        .qualifier = qual,
        .has_explicit_alias =
            !src.alias.empty() && !IdentifierEqualsCI(src.alias, src.table),
        .table = tbl.Value(),
        .schema = tbl.Value()->GetSchema(),
    });
  }
  return true;
}

std::optional<slot_t> FindColInSchema(const Schema& schema,
                                      std::string_view col_name) {
  std::optional<slot_t> found;
  for (slot_t c = 0; c < schema.ColumnCount(); ++c) {
    if (IdentifierEqualsCI(schema.GetColumn(c).Name().name, col_name)) {
      if (found.has_value()) {
        return std::nullopt;
      }
      found = c;
    }
  }
  return found;
}

std::optional<ResolvedColRef> ResolveColRef(
    const ColumnName& cn, const std::vector<ResolvedBaseRel>& rels) {
  if (cn.name.empty() || cn.name == "*") {
    return std::nullopt;
  }
  if (!cn.schema.empty()) {
    std::optional<ResolvedColRef> match;
    for (const ResolvedBaseRel& r : rels) {
      const bool qual_match = IdentifierEqualsCI(r.qualifier, cn.schema) ||
                              (!r.has_explicit_alias &&
                               IdentifierEqualsCI(r.table_name, cn.schema));
      if (!qual_match) {
        continue;
      }
      const auto slot = FindColInSchema(r.schema, cn.name);
      if (!slot.has_value() || match.has_value()) {
        return std::nullopt;
      }
      match = ResolvedColRef{.rel_idx = r.rel_idx, .slot = *slot};
    }
    return match;
  }
  std::optional<ResolvedColRef> match;
  for (const ResolvedBaseRel& r : rels) {
    const auto slot = FindColInSchema(r.schema, cn.name);
    if (!slot.has_value()) {
      continue;
    }
    if (match.has_value()) {
      return std::nullopt;
    }
    match = ResolvedColRef{.rel_idx = r.rel_idx, .slot = *slot};
  }
  return match;
}

bool CollectResolvedExprRelations(
    const Expression& expr, const std::vector<ResolvedBaseRel>& rels,
    std::unordered_set<size_t>* touched_rels,
    std::vector<ResolvedColRef>* touched_cols = nullptr) {
  if (!expr) {
    return true;
  }
  for (const ColumnName& cn : expr->TouchedColumns()) {
    const auto res = ResolveColRef(cn, rels);
    if (!res.has_value()) {
      return false;
    }
    touched_rels->insert(res->rel_idx);
    if (touched_cols != nullptr) {
      touched_cols->push_back(*res);
    }
  }
  return true;
}

bool ComparableEquijoinColumns(const Column& a, const Column& b) noexcept {
  return !a.IsInterval() && !b.IsInterval() && a.Type() == b.Type() &&
         a.IsUnsigned() == b.IsUnsigned() &&
         (a.Type() == ValueType::kInt64 || a.Type() == ValueType::kDate ||
          a.Type() == ValueType::kDouble || a.Type() == ValueType::kVarChar);
}

// Certifies that `col_idx` on `table` is strictly UNIQUE and NOT NULL, either
// via catalog schema constraints (`PRIMARY KEY` or `NOT NULL` + unique index)
// or via an immutable clean `.tlcs` / `LSTableStore` snapshot generation proof.
// Enforces Trap 2a (`nullable_unique_left_elides_groupby_bug`): any NULL value
// in the column unconditionally disqualifies uniqueness certification.
bool IsColumnCertifiedUniqueNotNull(const Table& table, slot_t col_idx,
                                    TransactionContext& ctx) {
  const Schema& schema = table.GetSchema();
  if (col_idx >= schema.ColumnCount()) {
    return false;
  }
  const Column& col = schema.GetColumn(col_idx);
  if (col.IsInterval()) {
    return false;
  }
  const Constraint::ConstraintType ctype = col.GetConstraint().ctype;
  if (ctype == Constraint::kPrimaryKey) {
    return true;
  }
  if (ctype == Constraint::kNotNull) {
    for (size_t i = 0; i < table.IndexCount(); ++i) {
      const Index& idx = table.GetIndex(i);
      if (idx.IsUnique() && idx.sc_.key_.size() == 1 &&
          idx.sc_.key_[0] == col_idx) {
        return true;
      }
    }
  }

  // Snapshot-level proof on clean immutable storage (bound to LayoutGeneration;
  // callers clear compiled-plan caching whenever a TPC-H rewrite fires).
  if (ctx.txn_.IsFailed() || table.RowsRewrittenInPlace()) {
    return false;
  }
  if (!ctx.txn_.IsReadOnly() && !ctx.txn_.WriteSetEmpty()) {
    return false;
  }
  // LayoutGeneration() and the LS store built below describe the *current*
  // table state. A read-write transaction with an empty write set can still
  // hold a snapshot older than a committed row mutation; that snapshot may
  // see historical rows the current-state proof never examined, so its
  // uniqueness cannot be certified from this generation.
  if (!ctx.txn_.IsReadOnly() && ctx.txn_.RequiresHistoricalRead()) {
    return false;
  }
  const uint64_t generation = table.LayoutGeneration();
  if (generation == 0) {
    return false;
  }
  if (ctx.txn_.HasAnyPageVersionChains()) {
    ctx.txn_.SweepCommittedVersionChains();
    if (ctx.txn_.HasAnyPageVersionChains()) {
      return false;
    }
  }
  const auto backing_tlcs = table.GetBackingTlcsStore();
  const bool clean_txn = ctx.txn_.IsReadOnly() || ctx.txn_.WriteSetEmpty();
  if (clean_txn && !table.RowsRewrittenInPlace() && backing_tlcs != nullptr &&
      backing_tlcs->LayoutGeneration() == generation &&
      backing_tlcs->FirstPageId() == table.FirstPageId() &&
      backing_tlcs->HasColumnSynopses() &&
      col_idx < backing_tlcs->ColumnSynopses().size()) {
    const auto& syn = backing_tlcs->ColumnSynopses()[col_idx];
    return !syn.has_null && syn.is_unique_nonnull;
  }
  // Avoid lazily scanning huge paged fact tables (> 4M rows) solely to probe
  // uniqueness; dimensions (`customer` 1.5M, `part` 2M, `supplier` 100K,
  // `nation` 25, `region` 5) are resident or small.
  const uint64_t backing_rows = table.BackingColumnarRowCount();
  if (backing_rows > 4'000'000ULL && table.GetLSTableStore() == nullptr) {
    if (backing_tlcs != nullptr && backing_tlcs->IsPaged()) {
      return false;
    }
  }
  const std::vector<slot_t> proj_single{col_idx};
  std::shared_ptr<const LSTableStore> store = GetOrBuildLSTableStore(
      ctx.txn_, table, /*allow_lazy_build=*/true, &proj_single);
  if (store == nullptr || store->layout_generation != generation ||
      store->first_pid != table.FirstPageId() || !store->HasColumn(col_idx) ||
      col_idx >= store->column_is_unique_nonnull.size() ||
      col_idx >= store->column_has_null.size()) {
    return false;
  }
  if (store->uncommitted_owner_txn != 0 &&
      store->uncommitted_owner_txn != ctx.txn_.ID()) {
    return false;
  }
  return store->column_is_unique_nonnull[col_idx] != 0 &&
         store->column_has_null[col_idx] == 0;
}

void CollectCteNamesInScope(const SelectStatement& stmt,
                            std::unordered_set<std::string>* cte_names) {
  for (const auto& [name, body] : stmt.WithQueries()) {
    (void)body;
    cte_names->insert(AsciiLower(name));
  }
}

// ============================================================================
// TPCH-1: Disjunctive Common-Conjunct Factoring, Per-Table Envelopes, and
//         Numeric/Date Bounding-Hull Range Synthesis
// Lean proof: lean/TinylambOpt/JoinAndAggregatePushdown.lean
//   - kleene_common_conjunct_factor_or_sound
//   - disjunctive_join_envelope_pushdown_sound
//   - interval_hull_of_disjuncts_sound
//   - envelope_missing_branch_pred_drops_rows_bug (Trap 1 guard)
// ============================================================================

struct ScalarBoundVal {
  ValueType type{ValueType::kNull};
  int64_t int_val{0};
  double dbl_val{0.0};
  bool inclusive{true};
};

bool EvalConstantBoundExpr(const Expression& expr, ValueType col_type,
                           ScalarBoundVal* out) {
  if (!expr || ExprHasParamOrSubqueryOrVolatile(expr) ||
      ExprContainsAggregateNode(expr) || !expr->TouchedColumns().empty()) {
    return false;
  }
  StatusOr<Value> val_or = expr->TryEvaluate(Row(), Schema());
  if (!val_or.HasValue() || val_or.Value().IsNull() ||
      val_or.Value().IsUnsigned()) {
    return false;
  }
  const Value& v = val_or.Value();
  if (col_type == ValueType::kDate) {
    if (v.type != ValueType::kDate) {
      return false;
    }
    out->type = ValueType::kDate;
    out->int_val = v.DateDays();
    return true;
  }
  if (col_type == ValueType::kInt64) {
    if (v.type == ValueType::kInt64) {
      out->type = ValueType::kInt64;
      out->int_val = v.value.int_value;
      return true;
    }
    return false;
  }
  if (col_type == ValueType::kDouble) {
    if (v.type == ValueType::kDouble) {
      if (std::isnan(v.value.double_value)) {
        return false;
      }
      out->type = ValueType::kDouble;
      out->dbl_val = v.value.double_value;
      return true;
    }
    if (v.type == ValueType::kInt64) {
      out->type = ValueType::kDouble;
      out->dbl_val = static_cast<double>(v.value.int_value);
      return true;
    }
  }
  return false;
}

int CompareBoundValues(const ScalarBoundVal& a,
                       const ScalarBoundVal& b) noexcept {
  if (a.type == ValueType::kDouble || b.type == ValueType::kDouble) {
    return a.dbl_val < b.dbl_val ? -1 : (a.dbl_val > b.dbl_val ? 1 : 0);
  }
  return a.int_val < b.int_val ? -1 : (a.int_val > b.int_val ? 1 : 0);
}

Expression MakeBoundConstantExpr(const ScalarBoundVal& b) {
  if (b.type == ValueType::kDate) {
    return ConstantValueExp(Value::DateFromDays(b.int_val));
  }
  if (b.type == ValueType::kInt64) {
    return ConstantValueExp(Value(b.int_val));
  }
  return ConstantValueExp(Value(b.dbl_val));
}

// Lean proof: lean/TinylambOpt/JoinAndAggregatePushdown.lean
//   (interval_hull_of_disjuncts_sound)
void AppendDisjunctiveIntervalHulls(
    const std::vector<std::vector<Expression>>& branch_conjs,
    const std::vector<ResolvedBaseRel>& rels,
    std::vector<Expression>* extra_conjuncts) {
  if (branch_conjs.size() < 2 || rels.empty()) {
    return;
  }
  struct ColInterval {
    bool has_lo{false};
    bool has_hi{false};
    ScalarBoundVal lo;
    ScalarBoundVal hi;
  };
  // Collect candidate columns bounded in branch 0.
  std::vector<ResolvedColRef> candidates;
  candidates.reserve(branch_conjs[0].size());
  auto extract_col_cmp = [&](const Expression& c, ResolvedColRef* ref_out,
                             BinaryOperation* op_out,
                             Expression* rhs_out) -> bool {
    if (!c || c->Type() != TypeTag::kBinaryExp) {
      return false;
    }
    const auto& bin = c->AsBinaryExpression();
    BinaryOperation op = bin.Op();
    Expression lhs = bin.Left();
    Expression rhs = bin.Right();
    if (lhs && lhs->Type() == TypeTag::kColumnValue) {
      const auto r = ResolveColRef(lhs->AsColumnValue().GetColumnName(), rels);
      if (!r.has_value()) {
        return false;
      }
      *ref_out = *r;
      *op_out = op;
      *rhs_out = rhs;
      return true;
    }
    if (rhs && rhs->Type() == TypeTag::kColumnValue) {
      const auto r = ResolveColRef(rhs->AsColumnValue().GetColumnName(), rels);
      if (!r.has_value()) {
        return false;
      }
      switch (op) {
        case BinaryOperation::kEquals:
          break;
        case BinaryOperation::kLessThan:
          op = BinaryOperation::kGreaterThan;
          break;
        case BinaryOperation::kLessThanEquals:
          op = BinaryOperation::kGreaterThanEquals;
          break;
        case BinaryOperation::kGreaterThan:
          op = BinaryOperation::kLessThan;
          break;
        case BinaryOperation::kGreaterThanEquals:
          op = BinaryOperation::kLessThanEquals;
          break;
        default:
          return false;
      }
      *ref_out = *r;
      *op_out = op;
      *rhs_out = lhs;
      return true;
    }
    return false;
  };

  for (const Expression& c : branch_conjs[0]) {
    ResolvedColRef ref;
    BinaryOperation op = BinaryOperation::kEquals;
    Expression rhs;
    if (!extract_col_cmp(c, &ref, &op, &rhs)) {
      continue;
    }
    const Column& col = rels[ref.rel_idx].schema.GetColumn(ref.slot);
    if (col.IsUnsigned() || col.IsInterval() ||
        (col.Type() != ValueType::kInt64 && col.Type() != ValueType::kDouble &&
         col.Type() != ValueType::kDate)) {
      continue;
    }
    if (std::ranges::find(candidates, ref) == candidates.end()) {
      candidates.push_back(ref);
    }
  }

  for (const ResolvedColRef& cand : candidates) {
    const Column& col = rels[cand.rel_idx].schema.GetColumn(cand.slot);
    bool all_have_lo = true;
    bool all_have_hi = true;
    ScalarBoundVal global_lo;
    ScalarBoundVal global_hi;
    bool first_lo = true;
    bool first_hi = true;
    bool any_distinct_lo = false;
    bool any_distinct_hi = false;

    for (const auto& bcs : branch_conjs) {
      ColInterval bi;
      for (const Expression& c : bcs) {
        ResolvedColRef ref;
        BinaryOperation op = BinaryOperation::kEquals;
        Expression rhs;
        if (!extract_col_cmp(c, &ref, &op, &rhs) || !(ref == cand)) {
          continue;
        }
        ScalarBoundVal v;
        if (!EvalConstantBoundExpr(rhs, col.Type(), &v)) {
          continue;
        }
        if (op == BinaryOperation::kEquals ||
            op == BinaryOperation::kGreaterThanEquals ||
            op == BinaryOperation::kGreaterThan) {
          v.inclusive = (op != BinaryOperation::kGreaterThan);
          if (!bi.has_lo || CompareBoundValues(v, bi.lo) > 0 ||
              (CompareBoundValues(v, bi.lo) == 0 && !v.inclusive)) {
            bi.lo = v;
            bi.has_lo = true;
          }
        }
        if (op == BinaryOperation::kEquals ||
            op == BinaryOperation::kLessThanEquals ||
            op == BinaryOperation::kLessThan) {
          v.inclusive = (op != BinaryOperation::kLessThan);
          if (!bi.has_hi || CompareBoundValues(v, bi.hi) < 0 ||
              (CompareBoundValues(v, bi.hi) == 0 && !v.inclusive)) {
            bi.hi = v;
            bi.has_hi = true;
          }
        }
      }
      if (!bi.has_lo) {
        all_have_lo = false;
      } else if (first_lo) {
        global_lo = bi.lo;
        first_lo = false;
      } else {
        const int cmp = CompareBoundValues(bi.lo, global_lo);
        if (cmp != 0) {
          any_distinct_lo = true;
        }
        if (cmp < 0) {
          global_lo = bi.lo;
        } else if (cmp == 0) {
          // The hull is an over-approximation of the OR of the branches.
          // Its lower endpoint is inclusive as soon as ANY branch attaining
          // that endpoint includes it; a sibling exclusive branch does not
          // make the value absent from the union, so never clear the flag.
          global_lo.inclusive = global_lo.inclusive || bi.lo.inclusive;
        }
      }
      if (!bi.has_hi) {
        all_have_hi = false;
      } else if (first_hi) {
        global_hi = bi.hi;
        first_hi = false;
      } else {
        const int cmp = CompareBoundValues(bi.hi, global_hi);
        if (cmp != 0) {
          any_distinct_hi = true;
        }
        if (cmp > 0) {
          global_hi = bi.hi;
        } else if (cmp == 0) {
          // Symmetric to the lower endpoint: any branch attaining the hull's
          // upper endpoint inclusively keeps it inclusive for the union.
          global_hi.inclusive = global_hi.inclusive || bi.hi.inclusive;
        }
      }
    }

    const ColumnName qcol(rels[cand.rel_idx].qualifier, col.Name().name);
    if (all_have_lo && (any_distinct_lo || all_have_hi)) {
      extra_conjuncts->push_back(BinaryExpressionExp(
          ColumnValueExp(qcol),
          global_lo.inclusive ? BinaryOperation::kGreaterThanEquals
                              : BinaryOperation::kGreaterThan,
          MakeBoundConstantExpr(global_lo)));
    }
    if (all_have_hi && (any_distinct_hi || all_have_lo)) {
      extra_conjuncts->push_back(BinaryExpressionExp(
          ColumnValueExp(qcol),
          global_hi.inclusive ? BinaryOperation::kLessThanEquals
                              : BinaryOperation::kLessThan,
          MakeBoundConstantExpr(global_hi)));
    }
  }
}

bool FactorAndEnvelopeOneSelect(SelectStatement* stmt,
                                const std::unordered_set<std::string>& ctes,
                                TransactionContext& ctx) {
  if (stmt == nullptr || !stmt->WhereClause()) {
    return false;
  }
  const bool any_right_or_full =
      std::ranges::any_of(stmt->Sources(), [](const SelectSource& s) {
        return s.join_type == JoinType::kRight ||
               s.join_type == JoinType::kFull;
      });
  if (any_right_or_full) {
    return false;
  }
  std::vector<ResolvedBaseRel> rels;
  const bool all_base_rels = ResolvePlainBaseSources(*stmt, ctes, ctx, &rels);
  size_t preserved_limit = 0;
  if (all_base_rels) {
    while (preserved_limit < stmt->Sources().size()) {
      if (preserved_limit > 0 &&
          stmt->Sources()[preserved_limit].join_type != JoinType::kInner &&
          stmt->Sources()[preserved_limit].join_type != JoinType::kCross) {
        break;
      }
      ++preserved_limit;
    }
  }

  const std::vector<Expression> orig_conjuncts =
      SplitAndConjuncts(stmt->WhereClause());
  std::unordered_set<std::string> existing_reprs;
  existing_reprs.reserve(orig_conjuncts.size() * 2);
  for (const Expression& c : orig_conjuncts) {
    if (c) {
      existing_reprs.insert(c->ToString());
    }
  }

  std::vector<Expression> new_conjuncts;
  new_conjuncts.reserve(orig_conjuncts.size() + 8);
  bool changed = false;

  auto push_unique_conj = [&](Expression e) {
    if (!e) {
      return;
    }
    const std::string repr = e->ToString();
    if (existing_reprs.insert(repr).second) {
      new_conjuncts.push_back(std::move(e));
      changed = true;
    }
  };

  Schema combined_schema;
  if (all_base_rels) {
    std::vector<Column> combined_cols;
    for (const ResolvedBaseRel& rel : rels) {
      for (size_t ci = 0; ci < rel.schema.ColumnCount(); ++ci) {
        Column col = rel.schema.GetColumn(ci);
        col.Name() = ColumnName(rel.qualifier, col.Name().name);
        combined_cols.push_back(std::move(col));
      }
    }
    combined_schema = Schema("", std::move(combined_cols));
  }

  for (const Expression& conj : orig_conjuncts) {
    const std::vector<Expression> branches = SplitOrDisjuncts(conj);
    const bool error_free_for_factor =
        all_base_rels
            ? AuditIsErrorFreePredicateForOrdering(conj, combined_schema)
            : ExpressionCannotThrowForPlanner(conj);
    if (branches.size() < 2 || ExprHasParamOrSubqueryOrVolatile(conj) ||
        ExprContainsAggregateNode(conj) || !error_free_for_factor) {
      new_conjuncts.push_back(conj);
      continue;
    }

    std::vector<std::vector<Expression>> branch_conjs(branches.size());
    for (size_t i = 0; i < branches.size(); ++i) {
      branch_conjs[i] = SplitAndConjuncts(branches[i]);
    }

    // (1a) Common Kleene conjunct factoring across all OR branches.
    std::vector<Expression> hoisted;
    hoisted.reserve(branch_conjs[0].size());
    std::vector<uint8_t> b0_common(branch_conjs[0].size(), 0);
    for (size_t c0 = 0; c0 < branch_conjs[0].size(); ++c0) {
      const Expression& cand = branch_conjs[0][c0];
      bool in_all = true;
      for (size_t bi = 1; bi < branch_conjs.size(); ++bi) {
        const bool found =
            std::ranges::any_of(branch_conjs[bi], [&](const Expression& other) {
              return ExprAstEquals(cand, other);
            });
        if (!found) {
          in_all = false;
          break;
        }
      }
      if (in_all) {
        b0_common[c0] = 1;
        // Deduplicate within `hoisted` itself.
        if (std::ranges::none_of(hoisted, [&](const Expression& h) {
              return ExprAstEquals(cand, h);
            })) {
          hoisted.push_back(cand);
        }
      }
    }

    bool any_branch_became_true = false;
    if (!hoisted.empty()) {
      changed = true;
      for (const Expression& h : hoisted) {
        existing_reprs.insert(h->ToString());
        new_conjuncts.push_back(h);
      }
      for (auto& bcs : branch_conjs) {
        std::vector<Expression> residual;
        residual.reserve(bcs.size());
        for (const Expression& item : bcs) {
          const bool was_hoisted = std::ranges::any_of(
              hoisted,
              [&](const Expression& h) { return ExprAstEquals(item, h); });
          if (!was_hoisted) {
            residual.push_back(item);
          }
        }
        if (residual.empty()) {
          any_branch_became_true = true;
        }
        bcs = std::move(residual);
      }
    }

    if (any_branch_became_true) {
      // `C AND (TRUE OR R2 ...)` == `C` in Kleene 3-valued logic whenever C is
      // evaluated in a boolean-TRUE WHERE filter context!
      continue;
    }

    // Rebuild the residual OR (still kept for exactness of cross-branch
    // correlations!).
    std::vector<Expression> rebuilt_branches;
    rebuilt_branches.reserve(branch_conjs.size());
    for (const auto& bcs : branch_conjs) {
      Expression b_expr = CombineAndConjuncts(bcs);
      if (std::ranges::none_of(rebuilt_branches, [&](const Expression& prev) {
            return ExprAstEquals(prev, b_expr);
          })) {
        rebuilt_branches.push_back(std::move(b_expr));
      }
    }
    Expression residual_or = CombineOrDisjuncts(rebuilt_branches);
    new_conjuncts.push_back(residual_or);
    if (residual_or) {
      existing_reprs.insert(residual_or->ToString());
    }

    if (!all_base_rels || rebuilt_branches.size() < 2) {
      continue;
    }

    // Re-split deduplicated residual branches for envelope & hull synthesis.
    std::vector<std::vector<Expression>> res_branch_conjs(
        rebuilt_branches.size());
    std::unordered_set<size_t> or_touched_rels;
    bool all_cols_resolved = true;
    for (size_t bi = 0; bi < rebuilt_branches.size(); ++bi) {
      res_branch_conjs[bi] = SplitAndConjuncts(rebuilt_branches[bi]);
      if (!CollectResolvedExprRelations(rebuilt_branches[bi], rels,
                                        &or_touched_rels)) {
        all_cols_resolved = false;
      }
    }

    // (1b) Single-table disjunctive envelope synthesis (strictly guarding
    // Trap 1 `envelope_missing_branch_pred_drops_rows_bug`: EVERY branch must
    // contain >= 1 predicate local to relation `r`).
    if (all_cols_resolved && or_touched_rels.size() >= 2) {
      for (size_t r = 0; r < preserved_limit; ++r) {
        if (!or_touched_rels.contains(r)) {
          continue;
        }
        std::vector<Expression> env_disjuncts;
        env_disjuncts.reserve(res_branch_conjs.size());
        std::vector<std::vector<Expression>> env_branch_conjs;
        env_branch_conjs.reserve(res_branch_conjs.size());
        bool every_branch_constrains_r = true;
        for (const auto& bcs : res_branch_conjs) {
          std::vector<Expression> r_only;
          r_only.reserve(bcs.size());
          for (const Expression& bc : bcs) {
            std::unordered_set<size_t> bc_rels;
            if (CollectResolvedExprRelations(bc, rels, &bc_rels) &&
                bc_rels.size() == 1 && bc_rels.contains(r)) {
              r_only.push_back(bc);
            }
          }
          if (r_only.empty()) {
            every_branch_constrains_r = false;
            break;
          }
          Expression r_and = CombineAndConjuncts(r_only);
          if (std::ranges::none_of(env_disjuncts, [&](const Expression& prev) {
                return ExprAstEquals(prev, r_and);
              })) {
            env_disjuncts.push_back(std::move(r_and));
          }
          env_branch_conjs.push_back(std::move(r_only));
        }
        if (every_branch_constrains_r && !env_disjuncts.empty()) {
          push_unique_conj(CombineOrDisjuncts(env_disjuncts));
        }
      }
    }

    // (1c) Numeric / DATE bounding-hull synthesis across branches.
    if (all_cols_resolved) {
      std::vector<Expression> hulls;
      AppendDisjunctiveIntervalHulls(res_branch_conjs, rels, &hulls);
      for (Expression& h : hulls) {
        push_unique_conj(std::move(h));
      }
    }
  }

  if (changed) {
    stmt->SetWhereClause(CombineAndConjuncts(new_conjuncts));
  }
  return changed;
}

bool FactorAndEnvelopeRecursive(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt, std::unordered_set<std::string> ctes,
    TransactionContext& ctx) {
  if (stmt == nullptr) {
    return false;
  }
  CollectCteNamesInScope(*stmt, &ctes);
  bool changed = false;
  for (const auto& [name, body] : stmt->WithQueries()) {
    if (!stmt->IsRecursiveWith(name) && body != nullptr) {
      changed |= FactorAndEnvelopeRecursive(body.get(), ctes, ctx);
    }
  }
  for (const SelectSource& src : stmt->Sources()) {
    if (src.query != nullptr && !src.is_lateral) {
      changed |= FactorAndEnvelopeRecursive(src.query.get(), ctes, ctx);
    }
  }
  changed |= FactorAndEnvelopeOneSelect(stmt, ctes, ctx);
  return changed;
}

// ============================================================================
// TPCH-2: Left-Join Right Pre-Aggregation and Unique-Left GROUP BY Elision
// Lean proofs:
//   - lean/TinylambOpt/JoinAndAggregatePushdown.lean
//     - left_join_right_preagg_and_unique_elide_groupby_sound
//     - nullable_unique_left_elides_groupby_bug (Trap 2a guard)
//     - left_join_count_nonstrict_expr_bug (Trap 2b guard)
//   - lean/TinylambOpt/ResearchProofs/UniqueKeyLeftJoinElisionAndMark.lean
//     (sound)
//   -
//   lean/TinylambOpt/ResearchProofs/LeftJoinPreservedToNullableFilterTransfer.lean
//     (sound)
// ============================================================================

bool PreaggregateLeftJoinOneSelect(SelectStatement* stmt,
                                   const std::unordered_set<std::string>& ctes,
                                   TransactionContext& ctx) {
  if (stmt == nullptr || stmt->Sources().size() != 2 || stmt->Distinct() ||
      stmt->HasDistinctOn() || stmt->IsGroupAll() ||
      stmt->GroupBy().size() != 1 || stmt->Having() != nullptr ||
      stmt->Qualify() != nullptr || stmt->WithTies() || stmt->ForUpdate() ||
      !stmt->UnionAll().empty() || stmt->GetSetOperationTree() != nullptr ||
      relational_detail::HasWindowFunctions(*stmt) ||
      SelectListHasStarItem(*stmt)) {
    return false;
  }
  const SelectSource& src_l = stmt->Sources()[0];
  const SelectSource& src_r = stmt->Sources()[1];
  if ((src_l.join_type != JoinType::kCross &&
       src_l.join_type != JoinType::kInner) ||
      src_l.join_condition != nullptr || src_r.join_type != JoinType::kLeft ||
      src_r.join_condition == nullptr) {
    return false;
  }
  std::vector<ResolvedBaseRel> rels;
  if (!ResolvePlainBaseSources(*stmt, ctes, ctx, &rels) || rels.size() != 2) {
    return false;
  }
  if (!stmt->GroupBy()[0] ||
      stmt->GroupBy()[0]->Type() != TypeTag::kColumnValue) {
    return false;
  }
  const auto gb_ref =
      ResolveColRef(stmt->GroupBy()[0]->AsColumnValue().GetColumnName(), rels);
  if (!gb_ref.has_value() || gb_ref->rel_idx != 0) {
    return false;
  }
  const slot_t slot_key_l = gb_ref->slot;
  const Column& col_key_l = rels[0].schema.GetColumn(slot_key_l);
  if (col_key_l.IsInterval()) {
    return false;
  }

  // WHERE (if present) must touch ONLY left relation 0.
  if (stmt->WhereClause()) {
    if (ExprHasParamOrSubqueryOrVolatile(stmt->WhereClause()) ||
        ExprContainsAggregateNode(stmt->WhereClause())) {
      return false;
    }
    std::unordered_set<size_t> where_rels;
    if (!CollectResolvedExprRelations(stmt->WhereClause(), rels, &where_rels) ||
        where_rels.contains(1)) {
      return false;
    }
  }

  // Inspect ON condition: must be `keyL = keyR` + R-only deterministic filters.
  if (ExprHasParamOrSubqueryOrVolatile(src_r.join_condition) ||
      ExprContainsAggregateNode(src_r.join_condition)) {
    return false;
  }
  std::optional<slot_t> slot_key_r;
  std::vector<Expression> on_r_preds;
  for (const Expression& c : SplitAndConjuncts(src_r.join_condition)) {
    std::unordered_set<size_t> crels;
    if (!CollectResolvedExprRelations(c, rels, &crels) || crels.empty()) {
      return false;
    }
    if (crels.size() == 1 && crels.contains(1)) {
      on_r_preds.push_back(c);
      continue;
    }
    if (crels.size() == 2 && c->Type() == TypeTag::kBinaryExp &&
        c->AsBinaryExpression().Op() == BinaryOperation::kEquals &&
        c->AsBinaryExpression().Left()->Type() == TypeTag::kColumnValue &&
        c->AsBinaryExpression().Right()->Type() == TypeTag::kColumnValue &&
        !slot_key_r.has_value()) {
      const auto lr = ResolveColRef(
          c->AsBinaryExpression().Left()->AsColumnValue().GetColumnName(),
          rels);
      const auto rr = ResolveColRef(
          c->AsBinaryExpression().Right()->AsColumnValue().GetColumnName(),
          rels);
      if (!lr.has_value() || !rr.has_value()) {
        return false;
      }
      ResolvedColRef l_side = *lr;
      ResolvedColRef r_side = *rr;
      if (l_side.rel_idx == 1 && r_side.rel_idx == 0) {
        std::swap(l_side, r_side);
      }
      if (l_side.rel_idx != 0 || l_side.slot != slot_key_l ||
          r_side.rel_idx != 1) {
        return false;
      }
      if (!ComparableEquijoinColumns(col_key_l,
                                     rels[1].schema.GetColumn(r_side.slot))) {
        return false;
      }
      slot_key_r = r_side.slot;
      continue;
    }
    return false;
  }
  if (!slot_key_r.has_value()) {
    return false;
  }

  // Validate SELECT list and ORDER BY:
  // Trap 2b (`left_join_count_nonstrict_expr_bug`): EVERY aggregate must be a
  // null-strict `COUNT(r_plain_col)` where `r_plain_col` is a plain column of R
  // (never `COUNT(*)`, `COUNT(1)`, or `COUNT(COALESCE(...))`). Outside those
  // aggregates, only `keyL` (or SELECT aliases in ORDER BY) may appear.
  std::unordered_set<std::string> select_aliases;
  select_aliases.reserve(stmt->SelectList().size());
  for (const NamedExpression& item : stmt->SelectList()) {
    if (!item.name.empty()) {
      select_aliases.insert(AsciiLower(item.name));
    }
  }
  std::vector<slot_t> counted_r_slots;
  const std::function<bool(const Expression&, bool)> validate_expr =
      [&](const Expression& e, bool allow_select_alias) -> bool {
    if (!e) {
      return true;
    }
    if (e->Type() == TypeTag::kQueryExp ||
        e->Type() == TypeTag::kWindowFunctionExp ||
        dynamic_cast<const ParameterSlot*>(e.get()) != nullptr) {
      return false;
    }
    if (e->Type() == TypeTag::kAggregateExp) {
      const auto& agg = e->AsAggregateExpression();
      if (agg.GetType() != AggregationType::kCount || agg.Distinct() ||
          agg.HasWhereFilter() || agg.NeedsGroupContext() || !agg.Child() ||
          agg.Child()->Type() != TypeTag::kColumnValue) {
        return false;
      }
      const ColumnName& cn = agg.Child()->AsColumnValue().GetColumnName();
      if (cn.name == "*") {
        return false;
      }
      const auto rref = ResolveColRef(cn, rels);
      if (!rref.has_value() || rref->rel_idx != 1) {
        return false;
      }
      if (std::ranges::find(counted_r_slots, rref->slot) ==
          counted_r_slots.end()) {
        counted_r_slots.push_back(rref->slot);
      }
      return true;
    }
    if (e->Type() == TypeTag::kColumnValue) {
      const ColumnName& cn = e->AsColumnValue().GetColumnName();
      if (allow_select_alias && cn.schema.empty() &&
          select_aliases.contains(AsciiLower(cn.name))) {
        return true;
      }
      const auto ref = ResolveColRef(cn, rels);
      return ref.has_value() && ref->rel_idx == 0 && ref->slot == slot_key_l;
    }
    return std::ranges::all_of(ExpressionChildren(e),
                               [&](const Expression& ch) {
                                 return validate_expr(ch, allow_select_alias);
                               });
  };

  for (const NamedExpression& item : stmt->SelectList()) {
    if (ExprHasParamOrSubqueryOrVolatile(item.expression) ||
        !validate_expr(item.expression, false)) {
      return false;
    }
  }
  for (const SelectStatement::OrderByTerm& ob : stmt->OrderBy()) {
    if (ExprHasParamOrSubqueryOrVolatile(ob.expression) ||
        !validate_expr(ob.expression, true)) {
      return false;
    }
  }
  if (counted_r_slots.empty()) {
    return false;
  }

  // Qualify R-local predicates explicitly to `rels[1].qualifier` so the derived
  // subquery is self-contained.
  const std::function<Expression(const Expression&)> qualify_r_expr =
      [&](const Expression& e) -> Expression {
    if (!e) {
      return e;
    }
    if (e->Type() == TypeTag::kColumnValue) {
      const auto ref = ResolveColRef(e->AsColumnValue().GetColumnName(), rels);
      if (ref.has_value() && ref->rel_idx == 1) {
        return ColumnValueExp(
            ColumnName(rels[1].qualifier,
                       rels[1].schema.GetColumn(ref->slot).Name().name));
      }
      return e;
    }
    std::vector<Expression> children;
    const auto orig = ExpressionChildren(e);
    children.reserve(orig.size());
    for (const Expression& ch : orig) {
      children.push_back(qualify_r_expr(ch));
    }
    return WithExpressionChildren(e, std::move(children));
  };

  const ColumnName r_key_col(rels[1].qualifier,
                             rels[1].schema.GetColumn(*slot_key_r).Name().name);
  std::vector<Expression> r_where_conjs;
  r_where_conjs.reserve(on_r_preds.size() + 1);
  for (const Expression& p : on_r_preds) {
    r_where_conjs.push_back(qualify_r_expr(p));
  }
  r_where_conjs.push_back(UnaryExpressionExp(ColumnValueExp(r_key_col),
                                             UnaryOperation::kIsNotNull));

  std::vector<NamedExpression> r_select;
  r_select.reserve(1 + counted_r_slots.size());
  r_select.emplace_back("__rj_key", ColumnValueExp(r_key_col));
  for (size_t k = 0; k < counted_r_slots.size(); ++k) {
    const ColumnName rc(
        rels[1].qualifier,
        rels[1].schema.GetColumn(counted_r_slots[k]).Name().name);
    r_select.emplace_back("__rj_cnt_" + std::to_string(k),
                          AggregateExpressionExp(AggregationType::kCount,
                                                 ColumnValueExp(rc), false));
  }

  auto r_sub = std::make_shared<SelectStatement>();
  SelectSource inner_r_src = src_r;
  inner_r_src.join_type = JoinType::kCross;
  inner_r_src.join_condition = nullptr;
  r_sub->SetSources({std::move(inner_r_src)});
  r_sub->SetWhereClause(CombineAndConjuncts(r_where_conjs));
  r_sub->SetGroupBy({ColumnValueExp(r_key_col)});
  r_sub->SetSelectList(std::move(r_select));

  // Trap 2a (`nullable_unique_left_elides_groupby_bug`): elide outer GROUP BY
  // ONLY when `keyL` is certified UNIQUE + NOT NULL (and left has no WHERE?
  // Wait: filtering a UNIQUE + NOT NULL column by `where_L` STILL leaves a
  // subset of unique non-null keys!).
  const bool elide_group_by =
      IsColumnCertifiedUniqueNotNull(*rels[0].table, slot_key_l, ctx);

  const std::string rj_alias = "__tpch_lj_agg";
  const ColumnName l_key_col(rels[0].qualifier, col_key_l.Name().name);

  const std::function<Expression(const Expression&, bool)> rewrite_outer_expr =
      [&](const Expression& e, bool allow_select_alias) -> Expression {
    if (!e) {
      return e;
    }
    if (e->Type() == TypeTag::kAggregateExp) {
      const auto& agg = e->AsAggregateExpression();
      const auto rref =
          ResolveColRef(agg.Child()->AsColumnValue().GetColumnName(), rels);
      const auto it = std::ranges::find(counted_r_slots, rref->slot);
      const auto k =
          static_cast<size_t>(std::distance(counted_r_slots.begin(), it));
      const Expression cnt_col =
          ColumnValueExp(ColumnName(rj_alias, "__rj_cnt_" + std::to_string(k)));
      if (elide_group_by) {
        return FunctionCallExp("coalesce",
                               {cnt_col, ConstantValueExp(Value(int64_t{0}))});
      }
      return CastExpressionExp(
          FunctionCallExp(
              "coalesce",
              {AggregateExpressionExp(AggregationType::kSum, cnt_col, false),
               ConstantValueExp(Value(int64_t{0}))}),
          "INT64", false);
    }
    if (e->Type() == TypeTag::kColumnValue) {
      const ColumnName& cn = e->AsColumnValue().GetColumnName();
      if (allow_select_alias && cn.schema.empty() &&
          select_aliases.contains(AsciiLower(cn.name))) {
        return e;
      }
      return ColumnValueExp(l_key_col);
    }
    std::vector<Expression> children;
    const auto orig = ExpressionChildren(e);
    children.reserve(orig.size());
    for (const Expression& ch : orig) {
      children.push_back(rewrite_outer_expr(ch, allow_select_alias));
    }
    return WithExpressionChildren(e, std::move(children));
  };

  std::vector<NamedExpression> new_select;
  new_select.reserve(stmt->SelectList().size());
  for (const auto& old_item : stmt->SelectList()) {
    const std::string out_name =
        old_item.name.empty() ? old_item.expression->ToString() : old_item.name;
    new_select.emplace_back(out_name,
                            rewrite_outer_expr(old_item.expression, false));
  }
  std::vector<SelectStatement::OrderByTerm> new_order = stmt->OrderBy();
  for (auto& term : new_order) {
    term.expression = rewrite_outer_expr(term.expression, true);
  }

  SelectSource new_l_src = src_l;
  if (new_l_src.alias.empty()) {
    new_l_src.alias = new_l_src.table;
  }
  SelectSource new_r_src(
      "", rj_alias, std::move(r_sub), JoinType::kLeft,
      BinaryExpressionExp(ColumnValueExp(l_key_col), BinaryOperation::kEquals,
                          ColumnValueExp(ColumnName(rj_alias, "__rj_key"))));
  stmt->SetSources({std::move(new_l_src), std::move(new_r_src)});
  stmt->SetSelectList(std::move(new_select));
  stmt->SetOrderBy(std::move(new_order));
  if (elide_group_by) {
    stmt->SetGroupBy({});
  } else {
    stmt->SetGroupBy({ColumnValueExp(l_key_col)});
  }
  return true;
}

bool PreaggregateLeftJoinRecursive(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt, std::unordered_set<std::string> ctes,
    TransactionContext& ctx) {
  if (stmt == nullptr) {
    return false;
  }
  CollectCteNamesInScope(*stmt, &ctes);
  bool changed = false;
  for (const auto& [name, body] : stmt->WithQueries()) {
    if (!stmt->IsRecursiveWith(name) && body != nullptr) {
      changed |= PreaggregateLeftJoinRecursive(body.get(), ctes, ctx);
    }
  }
  for (const SelectSource& src : stmt->Sources()) {
    if (src.query != nullptr && !src.is_lateral) {
      changed |= PreaggregateLeftJoinRecursive(src.query.get(), ctes, ctx);
    }
  }
  changed |= PreaggregateLeftJoinOneSelect(stmt, ctes, ctx);
  return changed;
}

// ============================================================================
// TPCH-3: Self-Semi-Join HAVING Aggregate Absorption
// Lean proof: lean/TinylambOpt/JoinAndAggregatePushdown.lean
//   - self_semijoin_having_agg_absorption_sound
//   - self_semijoin_mismatched_where_bug (Trap 3 guard)
// ============================================================================

Expression CanonicalizeSingleTableExpr(  // NOLINT(misc-no-recursion)
    const Expression& expr, const std::vector<ResolvedBaseRel>& rels,
    size_t expected_rel, const std::string& target_qual, bool* ok) {
  if (!expr) {
    return expr;
  }
  if (expr->Type() == TypeTag::kColumnValue) {
    const ColumnName& cn = expr->AsColumnValue().GetColumnName();
    if (cn.name == "*") {
      return expr;
    }
    const auto ref = ResolveColRef(cn, rels);
    if (!ref.has_value() || ref->rel_idx != expected_rel) {
      *ok = false;
      return expr;
    }
    return ColumnValueExp(
        ColumnName(target_qual,
                   rels[expected_rel].schema.GetColumn(ref->slot).Name().name));
  }
  if (expr->Type() == TypeTag::kQueryExp ||
      expr->Type() == TypeTag::kWindowFunctionExp ||
      dynamic_cast<const ParameterSlot*>(expr.get()) != nullptr) {
    *ok = false;
    return expr;
  }
  std::vector<Expression> children;
  const auto orig = ExpressionChildren(expr);
  children.reserve(orig.size());
  for (const Expression& ch : orig) {
    children.push_back(
        CanonicalizeSingleTableExpr(ch, rels, expected_rel, target_qual, ok));
    if (!*ok) {
      return expr;
    }
  }
  return WithExpressionChildren(expr, std::move(children));
}

bool AbsorbSelfSemijoinOneSelect(SelectStatement* stmt,
                                 const std::unordered_set<std::string>& ctes,
                                 TransactionContext& ctx) {
  if (stmt == nullptr || stmt->Sources().size() < 2 || stmt->Distinct() ||
      stmt->HasDistinctOn() || stmt->IsGroupAll() || stmt->GroupBy().empty() ||
      stmt->Qualify() != nullptr || stmt->WithTies() || stmt->ForUpdate() ||
      !stmt->UnionAll().empty() || stmt->GetSetOperationTree() != nullptr ||
      relational_detail::HasWindowFunctions(*stmt) ||
      SelectListHasStarItem(*stmt)) {
    return false;
  }
  for (const SelectSource& s : stmt->Sources()) {
    if (s.join_type != JoinType::kCross && s.join_type != JoinType::kInner) {
      return false;
    }
  }
  std::vector<ResolvedBaseRel> rels;
  if (!ResolvePlainBaseSources(*stmt, ctes, ctx, &rels)) {
    return false;
  }

  std::vector<Expression> all_conjs = SplitAndConjuncts(stmt->WhereClause());
  all_conjs.reserve(all_conjs.size() + stmt->Sources().size());
  for (const SelectSource& s : stmt->Sources()) {
    if (s.join_condition) {
      for (Expression& jc : SplitAndConjuncts(s.join_condition)) {
        all_conjs.push_back(std::move(jc));
      }
    }
  }
  if (all_conjs.empty()) {
    return false;
  }

  for (size_t in_idx = 0; in_idx < all_conjs.size(); ++in_idx) {
    const Expression& in_conj = all_conjs[in_idx];
    if (!in_conj || in_conj->Type() != TypeTag::kQueryExp) {
      continue;
    }
    const QueryExpression& qexp = in_conj->AsQueryExpression();
    if (qexp.Exists() || qexp.Negated() || qexp.ArrayResult() ||
        qexp.Mode() != QuantifierMode::kIn || !qexp.Test() ||
        qexp.Test()->Type() != TypeTag::kColumnValue ||
        qexp.Query() == nullptr) {
      continue;
    }
    const auto in_lhs_ref =
        ResolveColRef(qexp.Test()->AsColumnValue().GetColumnName(), rels);
    if (!in_lhs_ref.has_value()) {
      continue;
    }

    const SelectStatement& sub = *qexp.Query();
    if (sub.Sources().size() != 1 || !sub.WithQueries().empty() ||
        !sub.UnionAll().empty() || sub.GetSetOperationTree() != nullptr ||
        sub.Distinct() || sub.HasDistinctOn() || sub.IsGroupAll() ||
        sub.GroupBy().size() != 1 || sub.Having() == nullptr ||
        sub.Qualify() != nullptr || sub.HasLimit() || sub.Offset() != 0 ||
        sub.SelectList().size() != 1 ||
        ExprHasParamOrSubqueryOrVolatile(sub.Having()) ||
        (sub.WhereClause() &&
         (ExprHasParamOrSubqueryOrVolatile(sub.WhereClause()) ||
          ExprContainsAggregateNode(sub.WhereClause())))) {
      continue;
    }
    std::vector<ResolvedBaseRel> sub_rels;
    if (!ResolvePlainBaseSources(sub, ctes, ctx, &sub_rels) ||
        sub_rels.size() != 1) {
      continue;
    }
    if (!sub.GroupBy()[0] ||
        sub.GroupBy()[0]->Type() != TypeTag::kColumnValue ||
        !sub.SelectList()[0].expression ||
        sub.SelectList()[0].expression->Type() != TypeTag::kColumnValue) {
      continue;
    }
    const auto sub_gb_ref = ResolveColRef(
        sub.GroupBy()[0]->AsColumnValue().GetColumnName(), sub_rels);
    const auto sub_sel_ref = ResolveColRef(
        sub.SelectList()[0].expression->AsColumnValue().GetColumnName(),
        sub_rels);
    if (!sub_gb_ref.has_value() || !sub_sel_ref.has_value() ||
        sub_gb_ref->slot != sub_sel_ref->slot) {
      continue;
    }
    const slot_t key_slot = sub_gb_ref->slot;
    const Column& sub_key_col = sub_rels[0].schema.GetColumn(key_slot);
    if (sub_key_col.IsInterval() || (sub_key_col.Type() != ValueType::kInt64 &&
                                     sub_key_col.Type() != ValueType::kDate)) {
      continue;
    }

    // Canonicalize `sub.WhereClause()` conjuncts to bare column names for Trap
    // 3
    //  comparison (`self_semijoin_mismatched_where_bug`).
    bool sub_where_ok = true;
    std::vector<std::string> sub_canon_where;
    for (const Expression& sc : SplitAndConjuncts(sub.WhereClause())) {
      const Expression ce =
          CanonicalizeSingleTableExpr(sc, sub_rels, 0, "", &sub_where_ok);
      if (!sub_where_ok) {
        break;
      }
      sub_canon_where.push_back(ce->ToString());
    }
    if (!sub_where_ok) {
      continue;
    }
    std::ranges::sort(sub_canon_where);
    sub_canon_where.erase(std::ranges::unique(sub_canon_where).begin(),
                          sub_canon_where.end());

    // Search for a matching outer source `t_idx` over the same base table `T`.
    for (size_t t_idx = 0; t_idx < rels.size(); ++t_idx) {
      if (!IdentifierEqualsCI(rels[t_idx].table_name, sub_rels[0].table_name)) {
        continue;
      }
      // Inspect all other conjuncts in `all_conjs`: find `outer_col = T.key`,
      // `t_idx`-local predicates, and reject if any other cross-relation
      // conjunct touches `t_idx`.
      std::optional<size_t> eq_conj_idx;
      std::optional<ResolvedColRef> outer_partner_ref;
      std::vector<size_t> outer_t_local_idxs;
      bool conj_shape_ok = true;

      for (size_t ci = 0; ci < all_conjs.size(); ++ci) {
        if (ci == in_idx) {
          continue;
        }
        const Expression& c = all_conjs[ci];
        std::unordered_set<size_t> crels;
        if (ExprHasParamOrSubqueryOrVolatile(c) ||
            !CollectResolvedExprRelations(c, rels, &crels)) {
          // Unresolved / subquery conjunct: safe only if it shares zero table
          // qualifiers/columns with `t_idx`, which we cannot certify if
          // `CollectResolvedExprRelations` failed; check raw TouchedColumns.
          bool touches_t = false;
          for (const ColumnName& cn : c->TouchedColumns()) {
            const auto r = ResolveColRef(cn, rels);
            if (!r.has_value() || r->rel_idx == t_idx) {
              touches_t = true;
              break;
            }
          }
          if (touches_t) {
            conj_shape_ok = false;
            break;
          }
          continue;
        }
        if (!crels.contains(t_idx)) {
          continue;
        }
        if (crels.size() == 1) {
          outer_t_local_idxs.push_back(ci);
          continue;
        }
        // This rewrite is deliberately single-key: the subquery gate above
        // requires `sub.GroupBy().size() == 1`, and the multiplicity-collapse
        // proof only covers that one key. A second `t_idx = ...` equijoin is
        // therefore not "discarded" -- it makes `conj_shape_ok` false below so
        // the whole rewrite is skipped and the original query stays intact.
        if (crels.size() == 2 && c->Type() == TypeTag::kBinaryExp &&
            c->AsBinaryExpression().Op() == BinaryOperation::kEquals &&
            c->AsBinaryExpression().Left()->Type() == TypeTag::kColumnValue &&
            c->AsBinaryExpression().Right()->Type() == TypeTag::kColumnValue &&
            !eq_conj_idx.has_value()) {
          const auto lr = ResolveColRef(
              c->AsBinaryExpression().Left()->AsColumnValue().GetColumnName(),
              rels);
          const auto rr = ResolveColRef(
              c->AsBinaryExpression().Right()->AsColumnValue().GetColumnName(),
              rels);
          if (!lr.has_value() || !rr.has_value()) {
            conj_shape_ok = false;
            break;
          }
          ResolvedColRef t_side = *lr;
          ResolvedColRef o_side = *rr;
          if (t_side.rel_idx != t_idx) {
            std::swap(t_side, o_side);
          }
          if (t_side.rel_idx == t_idx && t_side.slot == key_slot &&
              o_side.rel_idx != t_idx &&
              ComparableEquijoinColumns(
                  sub_key_col,
                  rels[o_side.rel_idx].schema.GetColumn(o_side.slot))) {
            eq_conj_idx = ci;
            outer_partner_ref = o_side;
            continue;
          }
        }
        conj_shape_ok = false;
        break;
      }
      if (!conj_shape_ok || !eq_conj_idx.has_value() ||
          !outer_partner_ref.has_value()) {
        continue;
      }

      // `in_lhs_ref` must be either `outer_partner_ref` or `(t_idx, key_slot)`.
      const ResolvedColRef t_key_ref{.rel_idx = t_idx, .slot = key_slot};
      if (!(*in_lhs_ref == *outer_partner_ref || *in_lhs_ref == t_key_ref)) {
        continue;
      }

      // Trap 3 (`self_semijoin_mismatched_where_bug`): outer T-local WHERE
      // conjuncts and subquery WHERE conjuncts must be equivalent!
      bool outer_where_ok = true;
      std::vector<std::string> outer_canon_where;
      outer_canon_where.reserve(outer_t_local_idxs.size());
      for (const size_t idx_w : outer_t_local_idxs) {
        const Expression ce = CanonicalizeSingleTableExpr(
            all_conjs[idx_w], rels, t_idx, "", &outer_where_ok);
        if (!outer_where_ok) {
          break;
        }
        outer_canon_where.push_back(ce->ToString());
      }
      if (!outer_where_ok) {
        continue;
      }
      std::ranges::sort(outer_canon_where);
      outer_canon_where.erase(std::ranges::unique(outer_canon_where).begin(),
                              outer_canon_where.end());
      if (outer_canon_where != sub_canon_where) {
        continue;
      }

      // Check outer GROUP BY: must contain `outer_partner_ref` or `t_key_ref`,
      // and NO other column of `t_idx`.
      bool gb_has_key = false;
      bool gb_ok = true;
      for (const Expression& g : stmt->GroupBy()) {
        std::unordered_set<size_t> grels;
        std::vector<ResolvedColRef> gcols;
        if (!g || ExprHasParamOrSubqueryOrVolatile(g) ||
            !CollectResolvedExprRelations(g, rels, &grels, &gcols)) {
          gb_ok = false;
          break;
        }
        for (const ResolvedColRef& gc : gcols) {
          if (gc.rel_idx == t_idx && gc.slot != key_slot) {
            gb_ok = false;
            break;
          }
        }
        if (g->Type() == TypeTag::kColumnValue) {
          const auto gr =
              ResolveColRef(g->AsColumnValue().GetColumnName(), rels);
          if (gr.has_value() &&
              (*gr == *outer_partner_ref || *gr == t_key_ref)) {
            gb_has_key = true;
          }
        }
      }
      if (!gb_ok || !gb_has_key) {
        continue;
      }

      // Collect outer SELECT aliases for ORDER BY resolution.
      std::unordered_set<std::string> select_aliases;
      for (const NamedExpression& item : stmt->SelectList()) {
        if (!item.name.empty()) {
          select_aliases.insert(AsciiLower(item.name));
        }
      }

      // Validate SELECT, HAVING, ORDER BY:
      // Every AggregateExpression in the outer query must be `SUM(expr_T)`
      // (non-DISTINCT, unfiltered) where `expr_T` touches ONLY `t_idx` (so row
      // multiplicity collapse of `t_idx` from |G_T| to 1 preserves every
      // aggregate value!). Outside those `SUM(expr_T)`s, `t_idx` may only be
      // referenced via `t_key_ref`.
      const std::string sub_qual = sub_rels[0].qualifier;
      std::vector<Expression> absorbed_sum_children;
      std::vector<std::string> absorbed_sum_reprs;
      absorbed_sum_children.reserve(stmt->SelectList().size());
      absorbed_sum_reprs.reserve(stmt->SelectList().size());
      bool clauses_ok = true;

      const std::function<void(const Expression&, bool)> scan_clause =
          [&](const Expression& e, bool allow_alias) {
            if (!e || !clauses_ok) {
              return;
            }
            if (e->Type() == TypeTag::kQueryExp ||
                e->Type() == TypeTag::kWindowFunctionExp ||
                dynamic_cast<const ParameterSlot*>(e.get()) != nullptr) {
              clauses_ok = false;
              return;
            }
            if (e->Type() == TypeTag::kAggregateExp) {
              const auto& agg = e->AsAggregateExpression();
              if (agg.GetType() != AggregationType::kSum || agg.Distinct() ||
                  agg.HasWhereFilter() || agg.NeedsGroupContext() ||
                  !agg.Child() || ExprContainsAggregateNode(agg.Child()) ||
                  ExprHasParamOrSubqueryOrVolatile(agg.Child())) {
                clauses_ok = false;
                return;
              }
              bool child_ok = true;
              Expression sub_child = CanonicalizeSingleTableExpr(
                  agg.Child(), rels, t_idx, sub_qual, &child_ok);
              if (!child_ok || sub_child->TouchedColumns().empty()) {
                clauses_ok = false;
                return;
              }
              const std::string repr = sub_child->ToString();
              if (std::ranges::find(absorbed_sum_reprs, repr) ==
                  absorbed_sum_reprs.end()) {
                absorbed_sum_reprs.push_back(repr);
                absorbed_sum_children.push_back(std::move(sub_child));
              }
              return;
            }
            if (e->Type() == TypeTag::kColumnValue) {
              const ColumnName& cn = e->AsColumnValue().GetColumnName();
              if (allow_alias && cn.schema.empty() &&
                  select_aliases.contains(AsciiLower(cn.name))) {
                return;
              }
              const auto ref = ResolveColRef(cn, rels);
              if (!ref.has_value() ||
                  (ref->rel_idx == t_idx && ref->slot != key_slot)) {
                clauses_ok = false;
              }
              return;
            }
            for (const Expression& ch : ExpressionChildren(e)) {
              scan_clause(ch, allow_alias);
              if (!clauses_ok) {
                return;
              }
            }
          };

      for (const NamedExpression& item : stmt->SelectList()) {
        scan_clause(item.expression, false);
      }
      scan_clause(stmt->Having(), false);
      for (const SelectStatement::OrderByTerm& ob : stmt->OrderBy()) {
        scan_clause(ob.expression, true);
      }
      if (!clauses_ok || absorbed_sum_children.empty()) {
        continue;
      }

      // Build the single absorbed grouped derived subquery `__tpch_self_abs`!
      const std::string abs_alias = "__tpch_self_abs";
      const ColumnName sub_key_cn(sub_qual, sub_key_col.Name().name);
      bool having_canon_ok = true;
      Expression sub_having = CanonicalizeSingleTableExpr(
          sub.Having(), sub_rels, 0, sub_qual, &having_canon_ok);
      if (!having_canon_ok) {
        continue;
      }
      std::vector<Expression> sub_where_vec;
      if (sub.WhereClause()) {
        bool w_ok = true;
        Expression sw = CanonicalizeSingleTableExpr(sub.WhereClause(), sub_rels,
                                                    0, sub_qual, &w_ok);
        if (!w_ok) {
          continue;
        }
        sub_where_vec.push_back(std::move(sw));
      }
      sub_where_vec.push_back(UnaryExpressionExp(ColumnValueExp(sub_key_cn),
                                                 UnaryOperation::kIsNotNull));

      std::vector<NamedExpression> abs_select;
      abs_select.reserve(1 + absorbed_sum_children.size());
      abs_select.emplace_back("__abs_key", ColumnValueExp(sub_key_cn));
      for (size_t k = 0; k < absorbed_sum_children.size(); ++k) {
        abs_select.emplace_back(
            "__abs_sum_" + std::to_string(k),
            AggregateExpressionExp(AggregationType::kSum,
                                   absorbed_sum_children[k], false));
      }

      auto abs_stmt = std::make_shared<SelectStatement>();
      SelectSource inner_sub_src = sub.Sources()[0];
      if (inner_sub_src.alias.empty()) {
        inner_sub_src.alias = inner_sub_src.table;
      }
      inner_sub_src.join_type = JoinType::kCross;
      inner_sub_src.join_condition = nullptr;
      abs_stmt->SetSources({std::move(inner_sub_src)});
      abs_stmt->SetWhereClause(CombineAndConjuncts(sub_where_vec));
      abs_stmt->SetGroupBy({ColumnValueExp(sub_key_cn)});
      abs_stmt->SetHaving(std::move(sub_having));
      abs_stmt->SetSelectList(std::move(abs_select));

      const ColumnName abs_key_ref(abs_alias, "__abs_key");
      const ColumnName partner_cn(rels[outer_partner_ref->rel_idx].qualifier,
                                  rels[outer_partner_ref->rel_idx]
                                      .schema.GetColumn(outer_partner_ref->slot)
                                      .Name()
                                      .name);

      // Rewrite outer expressions: `(t_idx, key_slot)` ->
      // `__tpch_self_abs.__abs_key`, and `SUM(expr_T_k)` ->
      // `SUM(__tpch_self_abs.__abs_sum_k)`.
      const std::function<Expression(const Expression&, bool)> rewrite_expr =
          [&](const Expression& e, bool allow_alias) -> Expression {
        if (!e) {
          return e;
        }
        if (e->Type() == TypeTag::kAggregateExp) {
          const auto& agg = e->AsAggregateExpression();
          bool dummy_ok = true;
          const Expression canon_ch = CanonicalizeSingleTableExpr(
              agg.Child(), rels, t_idx, sub_qual, &dummy_ok);
          const auto it =
              std::ranges::find(absorbed_sum_reprs, canon_ch->ToString());
          const auto k = static_cast<size_t>(
              std::distance(absorbed_sum_reprs.begin(), it));
          return AggregateExpressionExp(
              AggregationType::kSum,
              ColumnValueExp(
                  ColumnName(abs_alias, "__abs_sum_" + std::to_string(k))),
              false);
        }
        if (e->Type() == TypeTag::kColumnValue) {
          const ColumnName& cn = e->AsColumnValue().GetColumnName();
          if (allow_alias && cn.schema.empty() &&
              select_aliases.contains(AsciiLower(cn.name))) {
            return e;
          }
          const auto ref = ResolveColRef(cn, rels);
          if (ref.has_value() && *ref == t_key_ref) {
            return ColumnValueExp(abs_key_ref);
          }
          return e;
        }
        std::vector<Expression> children;
        const auto orig = ExpressionChildren(e);
        children.reserve(orig.size());
        for (const Expression& ch : orig) {
          children.push_back(rewrite_expr(ch, allow_alias));
        }
        return WithExpressionChildren(e, std::move(children));
      };

      std::vector<SelectSource> new_sources = stmt->Sources();
      for (size_t s = 0; s < new_sources.size(); ++s) {
        if (s == t_idx) {
          new_sources[s] = SelectSource("", abs_alias, std::move(abs_stmt),
                                        JoinType::kCross, nullptr);
        } else {
          if (new_sources[s].alias.empty()) {
            new_sources[s].alias = new_sources[s].table;
          }
          new_sources[s].join_type = JoinType::kCross;
          new_sources[s].join_condition = nullptr;
        }
      }

      std::vector<Expression> new_where;
      new_where.reserve(all_conjs.size());
      for (size_t ci = 0; ci < all_conjs.size(); ++ci) {
        if (ci == in_idx || std::ranges::find(outer_t_local_idxs, ci) !=
                                outer_t_local_idxs.end()) {
          continue;
        }
        if (ci == *eq_conj_idx) {
          new_where.push_back(BinaryExpressionExp(ColumnValueExp(partner_cn),
                                                  BinaryOperation::kEquals,
                                                  ColumnValueExp(abs_key_ref)));
          continue;
        }
        new_where.push_back(all_conjs[ci]);
      }

      std::vector<Expression> new_gb;
      new_gb.reserve(stmt->GroupBy().size());
      for (const Expression& g : stmt->GroupBy()) {
        new_gb.push_back(rewrite_expr(g, false));
      }

      std::vector<NamedExpression> new_sel;
      new_sel.reserve(stmt->SelectList().size());
      for (const NamedExpression& item : stmt->SelectList()) {
        const std::string out_name =
            item.name.empty() ? item.expression->ToString() : item.name;
        new_sel.emplace_back(out_name, rewrite_expr(item.expression, false));
      }

      Expression new_having = rewrite_expr(stmt->Having(), false);
      std::vector<SelectStatement::OrderByTerm> new_order = stmt->OrderBy();
      for (auto& term : new_order) {
        term.expression = rewrite_expr(term.expression, true);
      }

      stmt->SetSources(std::move(new_sources));
      stmt->SetWhereClause(CombineAndConjuncts(new_where));
      stmt->SetGroupBy(std::move(new_gb));
      stmt->SetHaving(std::move(new_having));
      stmt->SetSelectList(std::move(new_sel));
      stmt->SetOrderBy(std::move(new_order));
      return true;
    }
  }
  return false;
}

bool AbsorbSelfSemijoinRecursive(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt, std::unordered_set<std::string> ctes,
    TransactionContext& ctx) {
  if (stmt == nullptr) {
    return false;
  }
  CollectCteNamesInScope(*stmt, &ctes);
  bool changed = false;
  for (const auto& [name, body] : stmt->WithQueries()) {
    if (!stmt->IsRecursiveWith(name) && body != nullptr) {
      changed |= AbsorbSelfSemijoinRecursive(body.get(), ctes, ctx);
    }
  }
  for (const SelectSource& src : stmt->Sources()) {
    if (src.query != nullptr && !src.is_lateral) {
      changed |= AbsorbSelfSemijoinRecursive(src.query.get(), ctes, ctx);
    }
  }
  changed |= AbsorbSelfSemijoinOneSelect(stmt, ctes, ctx);
  return changed;
}

// ============================================================================
// TPCH-4a: CTE Self-MAX / Self-MIN Subquery -> Non-Null Top-1 WITH TIES
// Lean proof: lean/TinylambOpt/JoinAndAggregatePushdown.lean
//   - cte_argmax_subquery_to_nonnull_max_ties_sound
//   - argmax_with_ties_without_is_not_null_bug (Trap 4a guard)
// ============================================================================

size_t CountTableReferencesInExpr(  // NOLINT(misc-no-recursion)
    const Expression& expr, std::string_view target_name,
    std::unordered_set<const SelectStatement*>* visited_defs);

size_t CountTableReferencesInSelect(  // NOLINT(misc-no-recursion)
    const SelectStatement& stmt, std::string_view target_name,
    bool include_with_defs,
    std::unordered_set<const SelectStatement*>* visited_defs = nullptr) {
  size_t count = 0;
  std::unordered_set<const SelectStatement*> local_visited;
  if (visited_defs == nullptr) {
    visited_defs = &local_visited;
  }
  // Any nested SelectStatement can be the same shared_ptr reached through
  // several paths (WITH definition, inlined derived source, UNION branch or
  // query expression). Count each definition once regardless of the path.
  auto walk_nested = [&](const SelectStatement* nested) -> size_t {
    if (nested == nullptr || !visited_defs->insert(nested).second) {
      return 0;
    }
    return CountTableReferencesInSelect(*nested, target_name, true,
                                        visited_defs);
  };
  if (include_with_defs) {
    for (const auto& [wname, wbody] : stmt.WithQueries()) {
      (void)wname;
      // A CTE body is a single definition. Without this guard a nested
      // definition reachable from several references is walked (and its own
      // internal references counted) once per path, breaking the "exactly
      // two references" check in RewriteCteArgmax.
      count += walk_nested(wbody.get());
    }
  }
  for (const SelectSource& src : stmt.Sources()) {
    if (!src.table.empty() && IdentifierEqualsCI(src.table, target_name)) {
      ++count;
    }
    count += walk_nested(src.query.get());
    count += CountTableReferencesInExpr(src.join_condition, target_name,
                                        visited_defs);
    count += CountTableReferencesInExpr(src.unnest, target_name, visited_defs);
  }
  count +=
      CountTableReferencesInExpr(stmt.WhereClause(), target_name, visited_defs);
  count += CountTableReferencesInExpr(stmt.Having(), target_name, visited_defs);
  count +=
      CountTableReferencesInExpr(stmt.Qualify(), target_name, visited_defs);
  for (const NamedExpression& item : stmt.SelectList()) {
    count +=
        CountTableReferencesInExpr(item.expression, target_name, visited_defs);
  }
  for (const Expression& g : stmt.GroupBy()) {
    count += CountTableReferencesInExpr(g, target_name, visited_defs);
  }
  for (const auto& ob : stmt.OrderBy()) {
    count +=
        CountTableReferencesInExpr(ob.expression, target_name, visited_defs);
  }
  for (const auto& br : stmt.UnionAll()) {
    count += walk_nested(br.get());
  }
  return count;
}

size_t CountTableReferencesInExpr(  // NOLINT(misc-no-recursion)
    const Expression& expr, std::string_view target_name,
    std::unordered_set<const SelectStatement*>* visited_defs) {
  if (!expr) {
    return 0;
  }
  size_t count = 0;
  if (expr->Type() == TypeTag::kQueryExp) {
    const auto& q = expr->AsQueryExpression();
    if (q.Query() != nullptr && visited_defs->insert(q.Query().get()).second) {
      count += CountTableReferencesInSelect(*q.Query(), target_name, true,
                                            visited_defs);
    }
    count += CountTableReferencesInExpr(q.Test(), target_name, visited_defs);
    return count;
  }
  for (const Expression& ch : ExpressionChildren(expr)) {
    count += CountTableReferencesInExpr(ch, target_name, visited_defs);
  }
  return count;
}

std::string OutputColumnNameAt(const SelectStatement& stmt, size_t idx) {
  const NamedExpression& item = stmt.SelectList()[idx];
  if (!item.name.empty()) {
    return item.name;
  }
  if (item.expression && item.expression->Type() == TypeTag::kColumnValue) {
    return item.expression->AsColumnValue().GetColumnName().name;
  }
  return {};
}

// ============================================================================
// TPCH-5b: Transitive Equijoin Closure Synthesis
// Lean proof: lean/TinylambOpt/JoinAndAggregatePushdown.lean
//   - equijoin_transitive_closure_sound
// ============================================================================

bool SynthesizeTransitiveEquijoinsOneSelect(
    SelectStatement* stmt, const std::unordered_set<std::string>& ctes,
    TransactionContext& ctx) {
  if (stmt == nullptr || stmt->Sources().size() < 3 || !stmt->WhereClause()) {
    return false;
  }
  for (const SelectSource& s : stmt->Sources()) {
    if (s.join_type != JoinType::kCross && s.join_type != JoinType::kInner) {
      return false;
    }
  }
  std::vector<ResolvedBaseRel> rels;
  if (!ResolvePlainBaseSources(*stmt, ctes, ctx, &rels)) {
    return false;
  }

  std::vector<Expression> conjs = SplitAndConjuncts(stmt->WhereClause());
  std::vector<ResolvedColRef> nodes;
  std::vector<size_t> parent;
  nodes.reserve(conjs.size() * 2);
  parent.reserve(conjs.size() * 2);
  auto get_node = [&](const ResolvedColRef& r) -> size_t {
    for (size_t i = 0; i < nodes.size(); ++i) {
      if (nodes[i] == r) {
        return i;
      }
    }
    const size_t id = nodes.size();
    nodes.push_back(r);
    parent.push_back(id);
    return id;
  };
  auto find_root = [&](size_t x) {
    while (parent[x] != x) {
      parent[x] = parent[parent[x]];
      x = parent[x];
    }
    return x;
  };

  struct PairHash {
    size_t operator()(const std::pair<size_t, size_t>& p) const noexcept {
      // Mix both halves with the standard boost-style combination; a plain
      // `first << 16` loses the high bits of large node ids and piles many
      // distinct pairs into the same bucket on large joins.
      const size_t a = std::hash<size_t>{}(p.first);
      const size_t b = std::hash<size_t>{}(p.second);
      return a ^ (b + static_cast<size_t>(0x9e3779b97f4a7c15ULL) + (a << 6) +
                  (a >> 2));
    }
  };
  std::unordered_set<std::pair<size_t, size_t>, PairHash> existing_pairs;

  auto ingest_eq = [&](const Expression& c) {
    if (!c || c->Type() != TypeTag::kBinaryExp ||
        c->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
      return;
    }
    const auto& bin = c->AsBinaryExpression();
    if (!bin.Left() || bin.Left()->Type() != TypeTag::kColumnValue ||
        !bin.Right() || bin.Right()->Type() != TypeTag::kColumnValue) {
      return;
    }
    const auto lr =
        ResolveColRef(bin.Left()->AsColumnValue().GetColumnName(), rels);
    const auto rr =
        ResolveColRef(bin.Right()->AsColumnValue().GetColumnName(), rels);
    if (!lr.has_value() || !rr.has_value() || lr->rel_idx == rr->rel_idx) {
      return;
    }
    const Column& ca = rels[lr->rel_idx].schema.GetColumn(lr->slot);
    const Column& cb = rels[rr->rel_idx].schema.GetColumn(rr->slot);
    if (!ComparableEquijoinColumns(ca, cb)) {
      return;
    }
    const size_t na = get_node(*lr);
    const size_t nb = get_node(*rr);
    existing_pairs.insert({std::min(na, nb), std::max(na, nb)});
    const size_t ra = find_root(na);
    const size_t rb = find_root(nb);
    if (ra != rb) {
      parent[rb] = ra;
    }
  };

  for (const Expression& c : conjs) {
    ingest_eq(c);
  }
  for (const SelectSource& s : stmt->Sources()) {
    for (const Expression& jc : SplitAndConjuncts(s.join_condition)) {
      ingest_eq(jc);
    }
  }
  if (nodes.size() < 3) {
    return false;
  }

  std::unordered_map<size_t, std::vector<size_t>> classes;
  for (size_t i = 0; i < nodes.size(); ++i) {
    classes[find_root(i)].push_back(i);
  }

  bool added = false;
  for (auto& [root, members] : classes) {
    (void)root;
    if (members.size() < 3 || members.size() > 8) {
      continue;
    }
    for (size_t i = 0; i < members.size(); ++i) {
      for (size_t j = i + 1; j < members.size(); ++j) {
        const size_t na = members[i];
        const size_t nb = members[j];
        if (nodes[na].rel_idx == nodes[nb].rel_idx) {
          continue;
        }
        const auto key = std::make_pair(std::min(na, nb), std::max(na, nb));
        if (!existing_pairs.insert(key).second) {
          continue;
        }
        const ColumnName lhs_cn(rels[nodes[na].rel_idx].qualifier,
                                rels[nodes[na].rel_idx]
                                    .schema.GetColumn(nodes[na].slot)
                                    .Name()
                                    .name);
        const ColumnName rhs_cn(rels[nodes[nb].rel_idx].qualifier,
                                rels[nodes[nb].rel_idx]
                                    .schema.GetColumn(nodes[nb].slot)
                                    .Name()
                                    .name);
        conjs.push_back(BinaryExpressionExp(ColumnValueExp(lhs_cn),
                                            BinaryOperation::kEquals,
                                            ColumnValueExp(rhs_cn)));
        added = true;
      }
    }
  }

  if (added) {
    stmt->SetWhereClause(CombineAndConjuncts(conjs));
  }
  return added;
}

bool SynthesizeTransitiveEquijoinsRecursive(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt, std::unordered_set<std::string> ctes,
    TransactionContext& ctx) {
  if (stmt == nullptr) {
    return false;
  }
  CollectCteNamesInScope(*stmt, &ctes);
  bool changed = false;
  for (const auto& [name, body] : stmt->WithQueries()) {
    if (!stmt->IsRecursiveWith(name) && body != nullptr) {
      changed |= SynthesizeTransitiveEquijoinsRecursive(body.get(), ctes, ctx);
    }
  }
  for (const SelectSource& src : stmt->Sources()) {
    if (src.query != nullptr && !src.is_lateral) {
      changed |=
          SynthesizeTransitiveEquijoinsRecursive(src.query.get(), ctes, ctx);
    }
  }
  changed |= SynthesizeTransitiveEquijoinsOneSelect(stmt, ctes, ctx);
  return changed;
}

// ============================================================================
// TPCH-5a: Late Unique-Dimension Wide Attribute Materialization After Top-N
// Lean proof: lean/TinylambOpt/JoinAndAggregatePushdown.lean
//   - topn_agg_late_unique_dim_attribute_join_sound
//   - late_dim_join_without_key_semijoin_steals_limit_bug (Trap 5 guard)
// ============================================================================

bool DeferUniqueDimAfterTopNOneSelect(
    SelectStatement* stmt, const std::unordered_set<std::string>& ctes,
    TransactionContext& ctx) {
  if (stmt == nullptr || stmt->Sources().size() < 2 || stmt->Distinct() ||
      stmt->HasDistinctOn() || stmt->IsGroupAll() ||
      stmt->GroupBy().size() < 2 || stmt->Qualify() != nullptr ||
      !stmt->HasLimit() || stmt->WithTies() || stmt->Limit() == 0 ||
      stmt->Limit() > 1024 || stmt->Offset() > 1024 ||
      stmt->Offset() + stmt->Limit() > 1024 || stmt->OrderBy().empty() ||
      stmt->ForUpdate() || !stmt->UnionAll().empty() ||
      stmt->GetSetOperationTree() != nullptr ||
      relational_detail::HasWindowFunctions(*stmt) ||
      SelectListHasStarItem(*stmt)) {
    return false;
  }
  for (const SelectSource& s : stmt->Sources()) {
    if (s.join_type != JoinType::kCross && s.join_type != JoinType::kInner) {
      return false;
    }
  }
  std::vector<ResolvedBaseRel> rels;
  if (!ResolvePlainBaseSources(*stmt, ctes, ctx, &rels)) {
    return false;
  }

  // Every GROUP BY item must be a plain resolved column.
  std::vector<ResolvedColRef> gb_cols;
  gb_cols.reserve(stmt->GroupBy().size());
  for (const Expression& g : stmt->GroupBy()) {
    if (!g || g->Type() != TypeTag::kColumnValue) {
      return false;
    }
    const auto r = ResolveColRef(g->AsColumnValue().GetColumnName(), rels);
    if (!r.has_value()) {
      return false;
    }
    gb_cols.push_back(*r);
  }

  // Identify direct unique-keyed sources in GROUP BY: `r` has `(r, pk_slot)` in
  // `gb_cols` with `IsColumnCertifiedUniqueNotNull`.
  // A relation may expose several certified UNIQUE NOT NULL columns in the
  // GROUP BY; keep them all so a later column can still serve as the deferred
  // lookup key when an earlier one is pinned by a predicate/ORDER BY.
  std::unordered_map<size_t, std::vector<slot_t>> unique_rel_pk;
  for (const ResolvedColRef& gr : gb_cols) {
    const Column& c = rels[gr.rel_idx].schema.GetColumn(gr.slot);
    if ((c.Type() == ValueType::kInt64 || c.Type() == ValueType::kDate) &&
        !c.IsInterval() &&
        IsColumnCertifiedUniqueNotNull(*rels[gr.rel_idx].table, gr.slot, ctx)) {
      auto& slots = unique_rel_pk[gr.rel_idx];
      if (std::ranges::find(slots, gr.slot) == slots.end()) {
        slots.push_back(gr.slot);
      }
    }
  }
  if (unique_rel_pk.empty()) {
    return false;
  }

  // Also detect 1-hop FD unique dimensions `r2` joined to a unique source `r`
  // via top-level WHERE / inner ON `r.fk = r2.pk2` where `pk2` is certified
  // UNIQUE + NOT NULL on `r2` (e.g. `customer.c_nationkey = nation.n_nationkey`
  // in TPC-H Q10!).
  std::vector<Expression> all_conjs = SplitAndConjuncts(stmt->WhereClause());
  all_conjs.reserve(all_conjs.size() + stmt->Sources().size());
  for (const SelectSource& s : stmt->Sources()) {
    for (Expression& jc : SplitAndConjuncts(s.join_condition)) {
      all_conjs.push_back(std::move(jc));
    }
  }
  for (const Expression& c : all_conjs) {
    if (!c || c->Type() != TypeTag::kBinaryExp ||
        c->AsBinaryExpression().Op() != BinaryOperation::kEquals ||
        !c->AsBinaryExpression().Left() ||
        c->AsBinaryExpression().Left()->Type() != TypeTag::kColumnValue ||
        !c->AsBinaryExpression().Right() ||
        c->AsBinaryExpression().Right()->Type() != TypeTag::kColumnValue) {
      continue;
    }
    const auto lr = ResolveColRef(
        c->AsBinaryExpression().Left()->AsColumnValue().GetColumnName(), rels);
    const auto rr = ResolveColRef(
        c->AsBinaryExpression().Right()->AsColumnValue().GetColumnName(), rels);
    if (!lr.has_value() || !rr.has_value() || lr->rel_idx == rr->rel_idx) {
      continue;
    }
    auto try_fd_hop = [&](const ResolvedColRef& from,
                          const ResolvedColRef& to) {
      if (!unique_rel_pk.contains(from.rel_idx) ||
          unique_rel_pk.contains(to.rel_idx)) {
        return;
      }
      const Column& ca = rels[from.rel_idx].schema.GetColumn(from.slot);
      const Column& cb = rels[to.rel_idx].schema.GetColumn(to.slot);
      if ((cb.Type() == ValueType::kInt64 || cb.Type() == ValueType::kDate) &&
          ComparableEquijoinColumns(ca, cb) &&
          IsColumnCertifiedUniqueNotNull(*rels[to.rel_idx].table, to.slot,
                                         ctx)) {
        auto& slots = unique_rel_pk[to.rel_idx];
        if (std::ranges::find(slots, to.slot) == slots.end()) {
          slots.push_back(to.slot);
        }
      }
    };
    try_fd_hop(*lr, *rr);
    try_fd_hop(*rr, *lr);
  }

  // Collect every column referenced in WHERE, inner ON, HAVING, aggregate
  // arguments, or ORDER BY (resolving SELECT aliases in ORDER BY).
  std::vector<ResolvedColRef> pinned_cols;
  auto pin_expr_cols = [&](const Expression& e) -> bool {
    if (!e) {
      return true;
    }
    std::unordered_set<size_t> dummy;
    return CollectResolvedExprRelations(e, rels, &dummy, &pinned_cols);
  };
  if (!pin_expr_cols(stmt->WhereClause()) || !pin_expr_cols(stmt->Having())) {
    return false;
  }
  for (const SelectSource& s : stmt->Sources()) {
    if (!pin_expr_cols(s.join_condition)) {
      return false;
    }
  }

  const std::function<bool(const Expression&)> pin_agg_children =
      [&](const Expression& e) -> bool {
    if (!e) {
      return true;
    }
    if (e->Type() == TypeTag::kQueryExp) {
      return false;
    }
    if (e->Type() == TypeTag::kAggregateExp) {
      return pin_expr_cols(e);
    }
    return std::ranges::all_of(ExpressionChildren(e), pin_agg_children);
  };
  for (const NamedExpression& item : stmt->SelectList()) {
    if (ExprHasParamOrSubqueryOrVolatile(item.expression) ||
        !pin_agg_children(item.expression)) {
      return false;
    }
  }
  for (const auto& ob : stmt->OrderBy()) {
    if (ExprHasParamOrSubqueryOrVolatile(ob.expression)) {
      return false;
    }
    // Expand SELECT-list alias if `ob.expression` is a bare alias reference.
    if (ob.expression && ob.expression->Type() == TypeTag::kColumnValue) {
      const ColumnName& cn = ob.expression->AsColumnValue().GetColumnName();
      if (cn.schema.empty()) {
        bool matched_alias = false;
        for (const NamedExpression& item : stmt->SelectList()) {
          if (!item.name.empty() && IdentifierEqualsCI(item.name, cn.name)) {
            if (!pin_expr_cols(item.expression)) {
              return false;
            }
            matched_alias = true;
            break;
          }
        }
        if (matched_alias) {
          continue;
        }
      }
    }
    if (!pin_expr_cols(ob.expression)) {
      return false;
    }
  }

  // Also pin any non-PK column that appears inside a composite SELECT
  // expression (so deferred columns ONLY appear as plain top-level SELECT items
  // or are omitted from SELECT!).
  for (const NamedExpression& item : stmt->SelectList()) {
    if (!item.expression || item.expression->Type() == TypeTag::kColumnValue) {
      continue;
    }
    const std::function<void(const Expression&)> pin_non_agg_cols =
        [&](const Expression& e) {
          if (!e || e->Type() == TypeTag::kAggregateExp) {
            return;
          }
          if (e->Type() == TypeTag::kColumnValue) {
            if (const auto r =
                    ResolveColRef(e->AsColumnValue().GetColumnName(), rels)) {
              pinned_cols.push_back(*r);
            }
            return;
          }
          for (const Expression& ch : ExpressionChildren(e)) {
            pin_non_agg_cols(ch);
          }
        };
    pin_non_agg_cols(item.expression);
  }

  auto is_pinned = [&](const ResolvedColRef& rc) {
    return std::ranges::find(pinned_cols, rc) != pinned_cols.end();
  };

  std::vector<ResolvedColRef> deferred_cols;
  deferred_cols.reserve(gb_cols.size());
  std::vector<ResolvedColRef> retained_gb_cols;
  retained_gb_cols.reserve(gb_cols.size());
  std::unordered_set<size_t> deferred_rels;
  deferred_rels.reserve(gb_cols.size());
  size_t deferred_varchar_count = 0;

  for (const ResolvedColRef& gr : gb_cols) {
    const auto pk_it = unique_rel_pk.find(gr.rel_idx);
    if (pk_it != unique_rel_pk.end() &&
        std::ranges::find(pk_it->second.begin(), pk_it->second.end(),
                          gr.slot) == pk_it->second.end() &&
        !is_pinned(gr)) {
      deferred_cols.push_back(gr);
      deferred_rels.insert(gr.rel_idx);
      if (rels[gr.rel_idx].schema.GetColumn(gr.slot).Type() ==
          ValueType::kVarChar) {
        ++deferred_varchar_count;
      }
    } else if (std::ranges::find(retained_gb_cols, gr) ==
               retained_gb_cols.end()) {
      retained_gb_cols.push_back(gr);
    }
  }

  // Worthwhile gate: require at least 1 deferred VARCHAR attribute (or >= 2
  // deferred attributes) so we never add a post-Top-N lookup join for a trivial
  // 1-int attribute.
  if (deferred_cols.empty() ||
      (deferred_varchar_count == 0 && deferred_cols.size() < 2)) {
    return false;
  }

  // Pick a certified unique key for a deferred relation, preferring one that
  // is already retained (so no extra key column needs projecting).
  auto pick_retained_pk = [&](size_t d_rel) -> std::optional<slot_t> {
    const auto it = unique_rel_pk.find(d_rel);
    if (it == unique_rel_pk.end() || it->second.empty()) {
      return std::nullopt;
    }
    for (const slot_t s : it->second) {
      const ResolvedColRef ref{.rel_idx = d_rel, .slot = s};
      if (std::ranges::find(retained_gb_cols, ref) != retained_gb_cols.end()) {
        return s;
      }
    }
    return it->second.front();
  };

  // Ensure every `deferred_rels` member's `pk_slot` is present in
  // `retained_gb_cols` so the outer query can join the K winners directly to
  // each deferred dimension by its certified PK!
  for (const size_t d_rel : deferred_rels) {
    const auto pk_slot = pick_retained_pk(d_rel);
    if (!pk_slot.has_value()) {
      return false;
    }
    const ResolvedColRef pk_ref{.rel_idx = d_rel, .slot = *pk_slot};
    if (std::ranges::find(retained_gb_cols, pk_ref) == retained_gb_cols.end()) {
      retained_gb_cols.push_back(pk_ref);
    }
  }
  if (retained_gb_cols.empty()) {
    return false;
  }

  // Verify every SELECT item is either a deferred column, a retained GROUP BY
  // column, or an expression over aggregates + retained GROUP BY columns.
  auto is_deferred = [&](const ResolvedColRef& rc) {
    return std::ranges::find(deferred_cols, rc) != deferred_cols.end();
  };
  auto is_retained_gb = [&](const ResolvedColRef& rc) {
    return std::ranges::find(retained_gb_cols, rc) != retained_gb_cols.end();
  };
  const std::function<bool(const Expression&)> expr_uses_only_core =
      [&](const Expression& e) -> bool {
    if (!e) {
      return true;
    }
    if (e->Type() == TypeTag::kAggregateExp) {
      return true;
    }
    if (e->Type() == TypeTag::kColumnValue) {
      const auto r = ResolveColRef(e->AsColumnValue().GetColumnName(), rels);
      return r.has_value() && is_retained_gb(*r);
    }
    return std::ranges::all_of(ExpressionChildren(e), expr_uses_only_core);
  };
  for (const NamedExpression& item : stmt->SelectList()) {
    if (!item.expression) {
      return false;
    }
    if (item.expression->Type() == TypeTag::kColumnValue) {
      const auto r =
          ResolveColRef(item.expression->AsColumnValue().GetColumnName(), rels);
      if (!r.has_value() || (!is_deferred(*r) && !is_retained_gb(*r))) {
        return false;
      }
      continue;
    }
    if (!expr_uses_only_core(item.expression)) {
      return false;
    }
  }

  // Build `__tpch_topn_core`:
  // Trap 5 (`late_dim_join_without_key_semijoin_steals_limit_bug`): keep ALL
  // original `Sources()` and `WhereClause()` inside `__tpch_topn_core` so
  // foreign-key semi-join filtering and dimension predicates execute BEFORE
  // `LIMIT (Offset + Limit)`!
  const std::string core_alias = "__tpch_topn_core";
  std::vector<Expression> core_gb;
  std::vector<NamedExpression> core_select;
  core_gb.reserve(retained_gb_cols.size());
  core_select.reserve(retained_gb_cols.size() + stmt->SelectList().size() +
                      stmt->OrderBy().size());

  auto key_out_name = [](size_t k_idx) {
    return "__topn_k" + std::to_string(k_idx);
  };
  for (size_t k = 0; k < retained_gb_cols.size(); ++k) {
    const ResolvedColRef& kr = retained_gb_cols[k];
    const ColumnName kcn(
        rels[kr.rel_idx].qualifier,
        rels[kr.rel_idx].schema.GetColumn(kr.slot).Name().name);
    core_gb.push_back(ColumnValueExp(kcn));
    core_select.emplace_back(key_out_name(k), ColumnValueExp(kcn));
  }

  // Map non-deferred SELECT items into `core_select`.
  std::vector<std::string> select_core_col(stmt->SelectList().size());
  std::unordered_map<std::string, std::string> alias_to_core_col;
  size_t agg_out_counter = 0;
  for (size_t i = 0; i < stmt->SelectList().size(); ++i) {
    const NamedExpression& item = stmt->SelectList()[i];
    if (item.expression->Type() == TypeTag::kColumnValue) {
      const auto r =
          ResolveColRef(item.expression->AsColumnValue().GetColumnName(), rels);
      if (!r.has_value()) {
        return false;
      }
      if (is_deferred(*r)) {
        continue;
      }
      const auto kit = std::ranges::find(retained_gb_cols, *r);
      const auto k_idx =
          static_cast<size_t>(std::distance(retained_gb_cols.begin(), kit));
      select_core_col[i] = key_out_name(k_idx);
      if (!item.name.empty()) {
        alias_to_core_col.emplace(AsciiLower(item.name), select_core_col[i]);
      }
      continue;
    }
    const std::string cname = "__topn_v" + std::to_string(agg_out_counter++);
    core_select.emplace_back(cname, item.expression);
    select_core_col[i] = cname;
    if (!item.name.empty()) {
      alias_to_core_col.emplace(AsciiLower(item.name), cname);
    }
  }

  // Map ORDER BY terms for both `core` and outer `stmt`.
  std::vector<SelectStatement::OrderByTerm> core_order;
  std::vector<SelectStatement::OrderByTerm> outer_order;
  core_order.reserve(stmt->OrderBy().size());
  outer_order.reserve(stmt->OrderBy().size());

  for (const auto& term : stmt->OrderBy()) {
    // Ordinal constant ORDER BY `1`, `2`, ...
    if (term.expression->Type() == TypeTag::kConstantValue) {
      const Value& cv = term.expression->AsConstantValue().GetValue();
      if (cv.type == ValueType::kInt64 && !cv.IsNull() &&
          cv.value.int_value >= 1 &&
          static_cast<size_t>(cv.value.int_value) <=
              stmt->SelectList().size()) {
        const auto ord_idx = static_cast<size_t>(cv.value.int_value - 1);
        if (select_core_col[ord_idx].empty()) {
          return false;
        }
        core_order.push_back(SelectStatement::OrderByTerm{
            .expression =
                ColumnValueExp(ColumnName("", select_core_col[ord_idx])),
            .ascending = term.ascending,
            .nulls_first = term.nulls_first});
        outer_order.push_back(SelectStatement::OrderByTerm{
            .expression = ColumnValueExp(
                ColumnName(core_alias, select_core_col[ord_idx])),
            .ascending = term.ascending,
            .nulls_first = term.nulls_first});
        continue;
      }
    }
    if (term.expression->Type() == TypeTag::kColumnValue) {
      const ColumnName& cn = term.expression->AsColumnValue().GetColumnName();
      if (cn.schema.empty()) {
        const auto ait = alias_to_core_col.find(AsciiLower(cn.name));
        if (ait != alias_to_core_col.end()) {
          core_order.push_back(SelectStatement::OrderByTerm{
              .expression = ColumnValueExp(ColumnName("", ait->second)),
              .ascending = term.ascending,
              .nulls_first = term.nulls_first});
          outer_order.push_back(SelectStatement::OrderByTerm{
              .expression = ColumnValueExp(ColumnName(core_alias, ait->second)),
              .ascending = term.ascending,
              .nulls_first = term.nulls_first});
          continue;
        }
      }
      const auto r = ResolveColRef(cn, rels);
      if (r.has_value() && is_retained_gb(*r)) {
        const auto kit = std::ranges::find(retained_gb_cols, *r);
        const auto k_idx =
            static_cast<size_t>(std::distance(retained_gb_cols.begin(), kit));
        const std::string kname = key_out_name(k_idx);
        core_order.push_back(SelectStatement::OrderByTerm{
            .expression = ColumnValueExp(ColumnName("", kname)),
            .ascending = term.ascending,
            .nulls_first = term.nulls_first});
        outer_order.push_back(SelectStatement::OrderByTerm{
            .expression = ColumnValueExp(ColumnName(core_alias, kname)),
            .ascending = term.ascending,
            .nulls_first = term.nulls_first});
        continue;
      }
      return false;
    }
    if (!expr_uses_only_core(term.expression)) {
      return false;
    }
    const std::string oname = "__topn_ord_" + std::to_string(agg_out_counter++);
    core_select.emplace_back(oname, term.expression);
    core_order.push_back(SelectStatement::OrderByTerm{
        .expression = ColumnValueExp(ColumnName("", oname)),
        .ascending = term.ascending,
        .nulls_first = term.nulls_first});
    outer_order.push_back(SelectStatement::OrderByTerm{
        .expression = ColumnValueExp(ColumnName(core_alias, oname)),
        .ascending = term.ascending,
        .nulls_first = term.nulls_first});
  }

  auto core_stmt = std::make_shared<SelectStatement>();
  std::vector<SelectSource> core_sources = stmt->Sources();
  for (SelectSource& cs : core_sources) {
    if (cs.alias.empty()) {
      cs.alias = cs.table;
    }
  }
  core_stmt->SetSources(std::move(core_sources));
  core_stmt->SetWhereClause(stmt->WhereClause());
  core_stmt->SetGroupBy(std::move(core_gb));
  core_stmt->SetHaving(stmt->Having());
  core_stmt->SetSelectList(std::move(core_select));
  core_stmt->SetOrderBy(std::move(core_order));
  core_stmt->SetOffset(0);
  core_stmt->SetLimit(stmt->Offset() + stmt->Limit());

  // Build outer post-Top-K attribute lookup query over the <= K winners.
  std::vector<SelectSource> outer_sources;
  outer_sources.emplace_back("", core_alias, std::move(core_stmt),
                             JoinType::kCross, nullptr);

  std::vector<size_t> sorted_def_rels(deferred_rels.begin(),
                                      deferred_rels.end());
  std::ranges::sort(sorted_def_rels);
  std::unordered_map<size_t, std::string> late_dim_alias;
  std::vector<Expression> outer_where;
  outer_where.reserve(sorted_def_rels.size());

  for (const size_t d_rel : sorted_def_rels) {
    const std::string dalias = "__tpch_late_d" + std::to_string(d_rel);
    late_dim_alias.emplace(d_rel, dalias);
    outer_sources.emplace_back(rels[d_rel].table_name, dalias, nullptr,
                               JoinType::kCross, nullptr);
    const auto pk_slot = pick_retained_pk(d_rel);
    if (!pk_slot.has_value()) {
      return false;
    }
    const ResolvedColRef pk_ref{.rel_idx = d_rel, .slot = *pk_slot};
    const auto kit = std::ranges::find(retained_gb_cols, pk_ref);
    const auto k_idx =
        static_cast<size_t>(std::distance(retained_gb_cols.begin(), kit));
    outer_where.push_back(BinaryExpressionExp(
        ColumnValueExp(ColumnName(core_alias, key_out_name(k_idx))),
        BinaryOperation::kEquals,
        ColumnValueExp(ColumnName(
            dalias, rels[d_rel].schema.GetColumn(*pk_slot).Name().name))));
  }

  std::vector<NamedExpression> outer_select;
  outer_select.reserve(stmt->SelectList().size());
  for (size_t i = 0; i < stmt->SelectList().size(); ++i) {
    const NamedExpression& old_item = stmt->SelectList()[i];
    if (!old_item.expression) {
      return false;
    }
    const std::string out_name =
        old_item.name.empty() ? old_item.expression->ToString() : old_item.name;
    if (!select_core_col[i].empty()) {
      outer_select.emplace_back(
          out_name, ColumnValueExp(ColumnName(core_alias, select_core_col[i])));
    } else {
      // An empty core slot means this item was skipped as a deferred plain
      // column. Never call AsColumnValue() on an unchecked expression: a
      // computed scalar (or a missing expression) must abort the rewrite.
      if (old_item.expression->Type() != TypeTag::kColumnValue) {
        return false;
      }
      const auto r = ResolveColRef(
          old_item.expression->AsColumnValue().GetColumnName(), rels);
      if (!r.has_value() || !is_deferred(*r)) {
        return false;
      }
      const auto alias = late_dim_alias.find(r->rel_idx);
      if (alias == late_dim_alias.end()) {
        return false;
      }
      outer_select.emplace_back(
          out_name,
          ColumnValueExp(ColumnName(
              alias->second,
              rels[r->rel_idx].schema.GetColumn(r->slot).Name().name)));
    }
  }

  const size_t saved_offset = stmt->Offset();
  const size_t saved_limit = stmt->Limit();
  stmt->SetSources(std::move(outer_sources));
  stmt->SetWhereClause(CombineAndConjuncts(outer_where));
  stmt->SetGroupBy({});
  stmt->SetHaving(nullptr);
  stmt->SetSelectList(std::move(outer_select));
  stmt->SetOrderBy(std::move(outer_order));
  stmt->SetOffset(saved_offset);
  stmt->SetLimit(saved_limit);
  return true;
}

// ============================================================================
// TPCH-6a: Correlated Strict Comparison Against SUM/AVG/MIN/MAX -> INNER JOIN
// Lean proof: lean/TinylambOpt/JoinAndAggregatePushdown.lean
//   - correlated_gt_sum_decorrelate_to_inner_join_sound
//   - correlated_gt_count_demotes_to_inner_join_bug (Trap 6a Kim's COUNT guard)
// ============================================================================

bool IsStrictNullRejectingCompareOp(BinaryOperation op) noexcept {
  switch (op) {
    case BinaryOperation::kEquals:
    case BinaryOperation::kNotEquals:
    case BinaryOperation::kLessThan:
    case BinaryOperation::kLessThanEquals:
    case BinaryOperation::kGreaterThan:
    case BinaryOperation::kGreaterThanEquals:
      return true;
    default:
      return false;
  }
}

// Trap 6a (`correlated_gt_count_demotes_to_inner_join_bug`): ONLY admit
// empty-group-NULL aggregates (`SUM`, `AVG`, `MIN`, `MAX`), optionally scaled
// by a non-null constant (`c * AGG(x)` or `AGG(x) * c`). Strictly reject
// `COUNT`, `COALESCE`, `IFNULL`, `CASE`, or additive offsets? Note even
// `c + SUM(x)` is NULL on empty group, whereas `COUNT` / `COALESCE` returns 0!
// Restricting to bare `SUM`/`AVG`/`MIN`/`MAX` or `const * AGG` / `AGG * const`
// is maximally clean and provably NULL on empty group.
bool ExtractNullOnEmptyCorrelatedAggregate(const Expression& select_expr,
                                           const AggregateExpression** agg_out,
                                           Expression* scale_const_out,
                                           bool* const_on_left_out) {
  *agg_out = nullptr;
  *scale_const_out = nullptr;
  *const_on_left_out = true;
  if (!select_expr) {
    return false;
  }
  auto check_agg = [](const Expression& e) -> const AggregateExpression* {
    if (!e || e->Type() != TypeTag::kAggregateExp) {
      return nullptr;
    }
    const auto& agg = e->AsAggregateExpression();
    if (agg.Distinct() || agg.HasWhereFilter() || agg.NeedsGroupContext() ||
        !agg.Child()) {
      return nullptr;
    }
    switch (agg.GetType()) {
      case AggregationType::kSum:
      case AggregationType::kAvg:
      case AggregationType::kMin:
      case AggregationType::kMax:
        return &agg;
      default:
        return nullptr;
    }
  };
  if (const auto* direct = check_agg(select_expr)) {
    *agg_out = direct;
    return true;
  }
  if (select_expr->Type() == TypeTag::kBinaryExp &&
      select_expr->AsBinaryExpression().Op() == BinaryOperation::kMultiply) {
    const auto& bin = select_expr->AsBinaryExpression();
    if (bin.Left() && bin.Left()->Type() == TypeTag::kConstantValue &&
        dynamic_cast<const ParameterSlot*>(bin.Left().get()) == nullptr &&
        !bin.Left()->AsConstantValue().GetValue().IsNull()) {
      if (const auto* ragg = check_agg(bin.Right())) {
        *agg_out = ragg;
        *scale_const_out = bin.Left();
        *const_on_left_out = true;
        return true;
      }
    }
    if (bin.Right() && bin.Right()->Type() == TypeTag::kConstantValue &&
        dynamic_cast<const ParameterSlot*>(bin.Right().get()) == nullptr &&
        !bin.Right()->AsConstantValue().GetValue().IsNull()) {
      if (const auto* lagg = check_agg(bin.Left())) {
        *agg_out = lagg;
        *scale_const_out = bin.Right();
        *const_on_left_out = false;
        return true;
      }
    }
  }
  return false;
}

void CollectCorrSumAliasesInUse(  // NOLINT(misc-no-recursion)
    const SelectStatement& stmt, std::unordered_set<std::string>* used) {
  for (const SelectSource& src : stmt.Sources()) {
    if (!src.alias.empty()) {
      used->insert(AsciiLower(src.alias));
    }
    if (!src.table.empty()) {
      used->insert(AsciiLower(src.table));
    }
    if (src.query != nullptr) {
      CollectCorrSumAliasesInUse(*src.query, used);
    }
  }
  for (const auto& [name, body] : stmt.WithQueries()) {
    used->insert(AsciiLower(name));
    if (body != nullptr) {
      CollectCorrSumAliasesInUse(*body, used);
    }
  }
  for (const NamedExpression& item : stmt.SelectList()) {
    if (!item.name.empty()) {
      used->insert(AsciiLower(item.name));
    }
  }
}

bool DecorrelateCorrelatedSumOneSelect(
    SelectStatement* stmt, const std::unordered_set<std::string>& ctes,
    TransactionContext& ctx, size_t* alias_seq,
    std::unordered_set<std::string>* used_aliases) {
  if (stmt == nullptr || stmt->Sources().empty() || !stmt->WhereClause() ||
      stmt->ForUpdate()) {
    return false;
  }
  for (const SelectSource& s : stmt->Sources()) {
    if (s.join_type != JoinType::kCross && s.join_type != JoinType::kInner) {
      return false;
    }
  }
  std::vector<ResolvedBaseRel> outer_rels;
  {
    std::unordered_set<std::string> seen_quals;
    outer_rels.reserve(stmt->Sources().size());
    for (size_t i = 0; i < stmt->Sources().size(); ++i) {
      const SelectSource& src = stmt->Sources()[i];
      if (src.unnest != nullptr || src.is_lateral || src.sample.has_value() ||
          !src.using_columns.empty() || src.from_nested_join ||
          src.join_condition != nullptr) {
        return false;
      }
      if (src.query != nullptr) {
        if (src.alias.empty() || src.query->SelectList().empty() ||
            SelectListHasStarItem(*src.query) ||
            !src.query->UnionAll().empty() ||
            src.query->GetSetOperationTree() != nullptr) {
          return false;
        }
        if (!seen_quals.insert(AsciiLower(src.alias)).second) {
          return false;
        }
        std::vector<ResolvedBaseRel> sub_rels;
        const bool sub_has_rels =
            ResolvePlainBaseSources(*src.query, ctes, ctx, &sub_rels);
        std::vector<Column> d_cols;
        d_cols.reserve(src.query->SelectList().size());
        std::unordered_set<std::string> seen_dcols;
        for (size_t si = 0; si < src.query->SelectList().size(); ++si) {
          const std::string cname = OutputColumnNameAt(*src.query, si);
          if (cname.empty() || !seen_dcols.insert(AsciiLower(cname)).second) {
            return false;
          }
          Column dcol(cname, ValueType::kNull);
          const Expression& sexpr = src.query->SelectList()[si].expression;
          if (sub_has_rels && sexpr && sexpr->Type() == TypeTag::kColumnValue) {
            if (const auto sref = ResolveColRef(
                    sexpr->AsColumnValue().GetColumnName(), sub_rels)) {
              const Column& bc =
                  sub_rels[sref->rel_idx].schema.GetColumn(sref->slot);
              dcol = Column(cname, bc.Type());
              dcol.SetUnsigned(bc.IsUnsigned());
              dcol.SetInterval(bc.IsInterval());
            }
          }
          d_cols.push_back(std::move(dcol));
        }
        outer_rels.push_back(ResolvedBaseRel{
            .rel_idx = i,
            .table_name = "",
            .qualifier = src.alias,
            .has_explicit_alias = true,
            .table = nullptr,
            .schema = Schema(src.alias, std::move(d_cols)),
        });
        continue;
      }
      if (src.table.empty() || ctes.contains(AsciiLower(src.table)) ||
          stmt->WithQueries().contains(src.table)) {
        return false;
      }
      StatusOr<std::shared_ptr<Table>> tbl = ctx.GetTable(src.table);
      if (!tbl.HasValue() || tbl.Value() == nullptr) {
        return false;
      }
      const std::string qual = src.alias.empty() ? src.table : src.alias;
      if (!seen_quals.insert(AsciiLower(qual)).second) {
        return false;
      }
      outer_rels.push_back(ResolvedBaseRel{
          .rel_idx = i,
          .table_name = src.table,
          .qualifier = qual,
          .has_explicit_alias =
              !src.alias.empty() && !IdentifierEqualsCI(src.alias, src.table),
          .table = tbl.Value(),
          .schema = tbl.Value()->GetSchema(),
      });
    }
  }

  // Qualify any bare column references in existing clauses BEFORE adding a new
  // derived source `__tpch_corr_sum_N` so nothing becomes ambiguous.
  std::vector<Expression> conjs = SplitAndConjuncts(stmt->WhereClause());
  std::vector<SelectSource> new_sources = stmt->Sources();
  for (SelectSource& s : new_sources) {
    if (s.query == nullptr && s.alias.empty()) {
      s.alias = s.table;
    }
  }
  std::vector<Expression> out_conjs;
  out_conjs.reserve(conjs.size() + 4);
  bool changed = false;

  for (const Expression& conj : conjs) {
    if (!conj || conj->Type() != TypeTag::kBinaryExp ||
        !IsStrictNullRejectingCompareOp(conj->AsBinaryExpression().Op())) {
      out_conjs.push_back(conj);
      continue;
    }
    const auto& cmp = conj->AsBinaryExpression();
    // Lean proof: lean/TinylambOpt/PredicateAndRelationalLowering.lean
    // (correlated_scalar_agg_decorrelation_sound)
    auto extract_outer_scaled_q =
        [](const Expression& side, const QueryExpression** q_out,
           Expression* oscale_out, bool* oconst_left_out) -> bool {
      if (!side) {
        return false;
      }
      if (side->Type() == TypeTag::kQueryExp) {
        *q_out = &side->AsQueryExpression();
        *oscale_out = nullptr;
        *oconst_left_out = true;
        return true;
      }
      if (side->Type() == TypeTag::kBinaryExp &&
          side->AsBinaryExpression().Op() == BinaryOperation::kMultiply) {
        const Expression& l = side->AsBinaryExpression().Left();
        const Expression& r = side->AsBinaryExpression().Right();
        auto is_nonnull_num_const = [](const Expression& c) {
          if (!c || c->Type() != TypeTag::kConstantValue) {
            return false;
          }
          const Value& v = c->AsConstantValue().GetValue();
          return !v.IsNull() &&
                 (v.type == ValueType::kInt64 || v.type == ValueType::kDouble);
        };
        if (is_nonnull_num_const(l) && r && r->Type() == TypeTag::kQueryExp) {
          *q_out = &r->AsQueryExpression();
          *oscale_out = l;
          *oconst_left_out = true;
          return true;
        }
        if (l && l->Type() == TypeTag::kQueryExp && is_nonnull_num_const(r)) {
          *q_out = &l->AsQueryExpression();
          *oscale_out = r;
          *oconst_left_out = false;
          return true;
        }
      }
      return false;
    };
    const QueryExpression* rhs_q = nullptr;
    Expression rhs_oscale;
    bool rhs_oconst_left = true;
    const QueryExpression* lhs_q = nullptr;
    Expression lhs_oscale;
    bool lhs_oconst_left = true;
    const bool rhs_is_q = extract_outer_scaled_q(cmp.Right(), &rhs_q,
                                                 &rhs_oscale, &rhs_oconst_left);
    const bool lhs_is_q = extract_outer_scaled_q(cmp.Left(), &lhs_q,
                                                 &lhs_oscale, &lhs_oconst_left);
    if (rhs_is_q == lhs_is_q) {
      out_conjs.push_back(conj);
      continue;
    }
    const Expression& outer_side = rhs_is_q ? cmp.Left() : cmp.Right();
    const QueryExpression& qexp = *(rhs_is_q ? rhs_q : lhs_q);
    const Expression& outer_scale = rhs_is_q ? rhs_oscale : lhs_oscale;
    const bool outer_const_left = rhs_is_q ? rhs_oconst_left : lhs_oconst_left;
    if (qexp.Exists() || qexp.Negated() || qexp.Test() != nullptr ||
        qexp.ArrayResult() || qexp.Query() == nullptr ||
        ExprHasParamOrSubqueryOrVolatile(outer_side) ||
        ExprContainsAggregateNode(outer_side)) {
      out_conjs.push_back(conj);
      continue;
    }
    std::unordered_set<size_t> outer_side_rels;
    if (!CollectResolvedExprRelations(outer_side, outer_rels,
                                      &outer_side_rels)) {
      out_conjs.push_back(conj);
      continue;
    }

    const SelectStatement& sub = *qexp.Query();
    if (sub.Sources().size() != 1 || !sub.WithQueries().empty() ||
        !sub.UnionAll().empty() || sub.GetSetOperationTree() != nullptr ||
        sub.Distinct() || sub.HasDistinctOn() || !sub.GroupBy().empty() ||
        sub.Having() != nullptr || sub.Qualify() != nullptr || sub.HasLimit() ||
        sub.Offset() != 0 || sub.SelectList().size() != 1 ||
        !sub.WhereClause()) {
      out_conjs.push_back(conj);
      continue;
    }
    std::vector<ResolvedBaseRel> inner_rels;
    if (!ResolvePlainBaseSources(sub, ctes, ctx, &inner_rels) ||
        inner_rels.size() != 1) {
      out_conjs.push_back(conj);
      continue;
    }
    // Avoid collision when inner unqualified table name equals an outer alias:
    // resolve each column explicitly against `inner_rels` first (SQL scoping:
    // inner table shadows outer unless outer-qualified!).
    const AggregateExpression* agg_ptr = nullptr;
    Expression scale_const;
    bool const_on_left = true;
    if (!ExtractNullOnEmptyCorrelatedAggregate(sub.SelectList()[0].expression,
                                               &agg_ptr, &scale_const,
                                               &const_on_left) ||
        ExprHasParamOrSubqueryOrVolatile(agg_ptr->Child()) ||
        ExprContainsAggregateNode(agg_ptr->Child())) {
      out_conjs.push_back(conj);
      continue;
    }

    auto resolve_scoped = [&](const ColumnName& cn, bool* is_inner_out,
                              ResolvedColRef* ref_out) -> bool {
      if (cn.name.empty() || cn.name == "*") {
        return false;
      }
      if (!cn.schema.empty()) {
        if (const auto ir = ResolveColRef(cn, inner_rels)) {
          // Ensure outer doesn't ALSO have same qualifier unless inner owns it
          // by SQL inner-first scoping.
          *is_inner_out = true;
          *ref_out = *ir;
          return true;
        }
        if (const auto or_ref = ResolveColRef(cn, outer_rels)) {
          *is_inner_out = false;
          *ref_out = *or_ref;
          return true;
        }
        return false;
      }
      // Unqualified name: SQL binds to inner relation FIRST if inner schema
      // contains `cn.name`, otherwise to outer scope!
      if (const auto islot = FindColInSchema(inner_rels[0].schema, cn.name)) {
        *is_inner_out = true;
        *ref_out = ResolvedColRef{.rel_idx = 0, .slot = *islot};
        return true;
      }
      if (const auto or_ref = ResolveColRef(cn, outer_rels)) {
        *is_inner_out = false;
        *ref_out = *or_ref;
        return true;
      }
      return false;
    };

    // `agg_ptr->Child()` must touch ONLY `inner_rels[0]`.
    bool agg_child_ok = true;
    const std::string inner_qual = inner_rels[0].qualifier;
    const std::function<Expression(const Expression&)> qualify_inner_only =
        [&](const Expression& e) -> Expression {
      if (!e || !agg_child_ok) {
        return e;
      }
      if (e->Type() == TypeTag::kColumnValue) {
        bool is_in = false;
        ResolvedColRef rr;
        if (!resolve_scoped(e->AsColumnValue().GetColumnName(), &is_in, &rr) ||
            !is_in) {
          agg_child_ok = false;
          return e;
        }
        return ColumnValueExp(ColumnName(
            inner_qual, inner_rels[0].schema.GetColumn(rr.slot).Name().name));
      }
      std::vector<Expression> chs;
      const auto orig = ExpressionChildren(e);
      chs.reserve(orig.size());
      for (const Expression& ch : orig) {
        chs.push_back(qualify_inner_only(ch));
        if (!agg_child_ok) {
          return e;
        }
      }
      return WithExpressionChildren(e, std::move(chs));
    };
    Expression qualified_agg_child = qualify_inner_only(agg_ptr->Child());
    if (!agg_child_ok || qualified_agg_child->TouchedColumns().empty()) {
      out_conjs.push_back(conj);
      continue;
    }

    // Partition `sub.WhereClause()` into correlation equalities
    // `inner_col = outer_col` and inner-only predicates.
    struct CorrPair {
      slot_t inner_slot{0};
      ResolvedColRef outer_ref;
    };
    std::vector<CorrPair> corr_pairs;
    std::vector<Expression> inner_local_preds;
    bool sub_where_valid = true;

    for (const Expression& sc : SplitAndConjuncts(sub.WhereClause())) {
      if (ExprHasParamOrSubqueryOrVolatile(sc) ||
          ExprContainsAggregateNode(sc)) {
        sub_where_valid = false;
        break;
      }
      bool touches_in = false;
      bool touches_out = false;
      for (const ColumnName& cn : sc->TouchedColumns()) {
        bool is_in = false;
        ResolvedColRef rr;
        if (!resolve_scoped(cn, &is_in, &rr)) {
          sub_where_valid = false;
          break;
        }
        if (is_in) {
          touches_in = true;
        } else {
          touches_out = true;
        }
      }
      if (!sub_where_valid) {
        break;
      }
      if (touches_in && !touches_out) {
        agg_child_ok = true;
        Expression qp = qualify_inner_only(sc);
        if (!agg_child_ok) {
          sub_where_valid = false;
          break;
        }
        inner_local_preds.push_back(std::move(qp));
        continue;
      }
      if (touches_in && touches_out && sc->Type() == TypeTag::kBinaryExp &&
          sc->AsBinaryExpression().Op() == BinaryOperation::kEquals &&
          sc->AsBinaryExpression().Left()->Type() == TypeTag::kColumnValue &&
          sc->AsBinaryExpression().Right()->Type() == TypeTag::kColumnValue) {
        bool l_in = false;
        bool r_in = false;
        ResolvedColRef lr;
        ResolvedColRef rr;
        if (!resolve_scoped(sc->AsBinaryExpression()
                                .Left()
                                ->AsColumnValue()
                                .GetColumnName(),
                            &l_in, &lr) ||
            !resolve_scoped(sc->AsBinaryExpression()
                                .Right()
                                ->AsColumnValue()
                                .GetColumnName(),
                            &r_in, &rr) ||
            l_in == r_in) {
          sub_where_valid = false;
          break;
        }
        const ResolvedColRef in_side = l_in ? lr : rr;
        const ResolvedColRef out_side = l_in ? rr : lr;
        const Column& ic = inner_rels[0].schema.GetColumn(in_side.slot);
        const Column& oc =
            outer_rels[out_side.rel_idx].schema.GetColumn(out_side.slot);
        if (!ComparableEquijoinColumns(ic, oc)) {
          sub_where_valid = false;
          break;
        }
        corr_pairs.push_back(
            CorrPair{.inner_slot = in_side.slot, .outer_ref = out_side});
        continue;
      }
      sub_where_valid = false;
      break;
    }

    if (!sub_where_valid || corr_pairs.empty()) {
      out_conjs.push_back(conj);
      continue;
    }

    // Build deduplicated inner GROUP BY keys and derived source! The counter
    // is shared across the whole statement tree, but a user-supplied alias can
    // still spell the same name; skip those so the minted alias is unique in
    // every scope it can be referenced from.
    std::string cs_alias;
    do {
      cs_alias = "__tpch_corr_sum_" + std::to_string((*alias_seq)++);
    } while (used_aliases->contains(AsciiLower(cs_alias)));
    used_aliases->insert(AsciiLower(cs_alias));
    std::vector<slot_t> unique_inner_keys;
    unique_inner_keys.reserve(corr_pairs.size());
    for (const CorrPair& cp : corr_pairs) {
      if (std::ranges::find(unique_inner_keys, cp.inner_slot) ==
          unique_inner_keys.end()) {
        unique_inner_keys.push_back(cp.inner_slot);
      }
    }

    std::vector<Expression> cs_gb;
    std::vector<NamedExpression> cs_select;
    cs_gb.reserve(unique_inner_keys.size());
    cs_select.reserve(unique_inner_keys.size() + 1);
    for (size_t k = 0; k < unique_inner_keys.size(); ++k) {
      const ColumnName icn(
          inner_qual,
          inner_rels[0].schema.GetColumn(unique_inner_keys[k]).Name().name);
      cs_gb.push_back(ColumnValueExp(icn));
      cs_select.emplace_back("__cs_k" + std::to_string(k), ColumnValueExp(icn));
      inner_local_preds.push_back(
          UnaryExpressionExp(ColumnValueExp(icn), UnaryOperation::kIsNotNull));
    }
    cs_select.emplace_back(
        "__cs_agg",
        AggregateExpressionExp(agg_ptr->GetType(),
                               std::move(qualified_agg_child), false));

    auto cs_stmt = std::make_shared<SelectStatement>();
    SelectSource inner_src = sub.Sources()[0];
    if (inner_src.alias.empty()) {
      inner_src.alias = inner_src.table;
    }
    inner_src.join_type = JoinType::kCross;
    inner_src.join_condition = nullptr;
    cs_stmt->SetSources({std::move(inner_src)});
    cs_stmt->SetWhereClause(CombineAndConjuncts(inner_local_preds));
    cs_stmt->SetGroupBy(std::move(cs_gb));
    cs_stmt->SetSelectList(std::move(cs_select));

    new_sources.emplace_back("", cs_alias, std::move(cs_stmt), JoinType::kCross,
                             nullptr);

    for (const CorrPair& cp : corr_pairs) {
      const auto kit = std::ranges::find(unique_inner_keys, cp.inner_slot);
      const auto k_idx =
          static_cast<size_t>(std::distance(unique_inner_keys.begin(), kit));
      const ColumnName ocn(outer_rels[cp.outer_ref.rel_idx].qualifier,
                           outer_rels[cp.outer_ref.rel_idx]
                               .schema.GetColumn(cp.outer_ref.slot)
                               .Name()
                               .name);
      out_conjs.push_back(BinaryExpressionExp(
          ColumnValueExp(ocn), BinaryOperation::kEquals,
          ColumnValueExp(
              ColumnName(cs_alias, "__cs_k" + std::to_string(k_idx)))));
    }

    Expression agg_val_ref = ColumnValueExp(ColumnName(cs_alias, "__cs_agg"));
    if (scale_const != nullptr) {
      agg_val_ref =
          const_on_left
              ? BinaryExpressionExp(scale_const, BinaryOperation::kMultiply,
                                    agg_val_ref)
              : BinaryExpressionExp(agg_val_ref, BinaryOperation::kMultiply,
                                    scale_const);
    }
    if (outer_scale != nullptr) {
      agg_val_ref =
          outer_const_left
              ? BinaryExpressionExp(outer_scale, BinaryOperation::kMultiply,
                                    agg_val_ref)
              : BinaryExpressionExp(agg_val_ref, BinaryOperation::kMultiply,
                                    outer_scale);
    }
    out_conjs.push_back(
        rhs_is_q ? BinaryExpressionExp(outer_side, cmp.Op(), agg_val_ref)
                 : BinaryExpressionExp(agg_val_ref, cmp.Op(), outer_side));
    changed = true;
  }

  if (changed) {
    stmt->SetSources(std::move(new_sources));
    stmt->SetWhereClause(CombineAndConjuncts(out_conjs));
  }
  return changed;
}

bool DecorrelateCorrelatedSumRecursive(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt, std::unordered_set<std::string> ctes,
    TransactionContext& ctx, size_t* alias_seq,
    std::unordered_set<std::string>* used_aliases) {
  if (stmt == nullptr) {
    return false;
  }
  CollectCteNamesInScope(*stmt, &ctes);
  bool changed = false;
  for (const auto& [name, body] : stmt->WithQueries()) {
    if (!stmt->IsRecursiveWith(name) && body != nullptr) {
      changed |= DecorrelateCorrelatedSumRecursive(body.get(), ctes, ctx,
                                                   alias_seq, used_aliases);
    }
  }
  for (const SelectSource& src : stmt->Sources()) {
    if (src.query != nullptr && !src.is_lateral) {
      changed |= DecorrelateCorrelatedSumRecursive(src.query.get(), ctes, ctx,
                                                   alias_seq, used_aliases);
    }
  }
  changed |= DecorrelateCorrelatedSumOneSelect(stmt, ctes, ctx, alias_seq,
                                               used_aliases);
  return changed;
}

}  // namespace

// Lean proof: lean/TinylambOpt/JoinAndAggregatePushdown.lean
//   (kleene_common_conjunct_factor_or_sound,
//    disjunctive_join_envelope_pushdown_sound,
//    interval_hull_of_disjuncts_sound,
//    envelope_missing_branch_pred_drops_rows_bug)
bool FactorAndEnvelopeDisjunctiveWhere(SelectStatement* select,
                                       TransactionContext& ctx) {
  return FactorAndEnvelopeRecursive(select, {}, ctx);
}

// Lean proof: lean/TinylambOpt/JoinAndAggregatePushdown.lean
//   (left_join_right_preagg_and_unique_elide_groupby_sound,
//    nullable_unique_left_elides_groupby_bug,
//    left_join_count_nonstrict_expr_bug)
bool PreaggregateLeftJoinRightCounts(SelectStatement* select,
                                     TransactionContext& ctx) {
  return PreaggregateLeftJoinRecursive(select, {}, ctx);
}

// Lean proof: lean/TinylambOpt/JoinAndAggregatePushdown.lean
//   (self_semijoin_having_agg_absorption_sound,
//    self_semijoin_mismatched_where_bug)
bool AbsorbSelfSemijoinHavingAggregate(SelectStatement* select,
                                       TransactionContext& ctx) {
  return AbsorbSelfSemijoinRecursive(select, {}, ctx);
}

// Lean proof: lean/TinylambOpt/JoinAndAggregatePushdown.lean
//   (cte_argmax_subquery_to_nonnull_max_ties_sound,
//    argmax_with_ties_without_is_not_null_bug)
bool RewriteCteArgmaxSubqueryToTopTies(SelectStatement* outer,
                                       TransactionContext& ctx) {
  if (outer == nullptr || outer->WithQueries().empty() ||
      !outer->WhereClause() || outer->Sources().empty() ||
      !outer->UnionAll().empty() || outer->GetSetOperationTree() != nullptr) {
    return false;
  }
  for (const SelectSource& s : outer->Sources()) {
    if (s.join_type != JoinType::kCross && s.join_type != JoinType::kInner) {
      return false;
    }
  }

  bool any_rewritten = false;
  for (const std::string& cte_name : outer->WithQueryOrder()) {
    if (outer->IsRecursiveWith(cte_name)) {
      continue;
    }
    const auto cte_it = outer->WithQueries().find(cte_name);
    if (cte_it == outer->WithQueries().end() || cte_it->second == nullptr) {
      continue;
    }
    std::shared_ptr<SelectStatement> cte_body = cte_it->second;
    if (cte_body->HasLimit() || cte_body->Offset() != 0 ||
        !cte_body->UnionAll().empty() ||
        cte_body->GetSetOperationTree() != nullptr ||
        cte_body->SelectList().empty() || SelectListHasStarItem(*cte_body)) {
      continue;
    }
    // Verify unique non-empty output names on `cte_body`.
    std::vector<std::string> cte_cols;
    cte_cols.reserve(cte_body->SelectList().size());
    std::unordered_set<std::string> seen_cols;
    bool cols_ok = true;
    for (size_t i = 0; i < cte_body->SelectList().size(); ++i) {
      const std::string cname = OutputColumnNameAt(*cte_body, i);
      if (cname.empty() || !seen_cols.insert(AsciiLower(cname)).second) {
        cols_ok = false;
        break;
      }
      cte_cols.push_back(cname);
    }
    if (!cols_ok) {
      continue;
    }

    // `cte_name` must be referenced EXACTLY twice across the entire statement!
    if (CountTableReferencesInSelect(*outer, cte_name, true) != 2) {
      continue;
    }

    // Reference #1: a single plain source in `outer->Sources()`.
    std::optional<size_t> outer_cte_src_idx;
    for (size_t i = 0; i < outer->Sources().size(); ++i) {
      const SelectSource& src = outer->Sources()[i];
      if (IdentifierEqualsCI(src.table, cte_name)) {
        if (src.query != nullptr || src.unnest != nullptr || src.is_lateral ||
            src.sample.has_value() || !src.using_columns.empty() ||
            src.from_nested_join || outer_cte_src_idx.has_value()) {
          outer_cte_src_idx.reset();
          break;
        }
        outer_cte_src_idx = i;
      }
    }
    if (!outer_cte_src_idx.has_value()) {
      continue;
    }
    const SelectSource& outer_cte_src = outer->Sources()[*outer_cte_src_idx];
    const std::string outer_cte_qual =
        outer_cte_src.alias.empty() ? outer_cte_src.table : outer_cte_src.alias;

    // Check whether an unqualified column name could collide with any other
    // source in `outer->Sources()`.
    auto unqualified_conflicts_other_source =
        [&](std::string_view col_name) -> bool {
      for (size_t i = 0; i < outer->Sources().size(); ++i) {
        if (i == *outer_cte_src_idx) {
          continue;
        }
        const SelectSource& osrc = outer->Sources()[i];
        if (osrc.unnest != nullptr || osrc.is_lateral) {
          return true;
        }
        if (osrc.query != nullptr) {
          if (SelectListHasStarItem(*osrc.query)) {
            return true;
          }
          for (size_t s = 0; s < osrc.query->SelectList().size(); ++s) {
            if (IdentifierEqualsCI(OutputColumnNameAt(*osrc.query, s),
                                   col_name)) {
              return true;
            }
          }
          continue;
        }
        if (const auto w_it = outer->WithQueries().find(osrc.table);
            w_it != outer->WithQueries().end() && w_it->second != nullptr) {
          if (SelectListHasStarItem(*w_it->second)) {
            return true;
          }
          for (size_t s = 0; s < w_it->second->SelectList().size(); ++s) {
            if (IdentifierEqualsCI(OutputColumnNameAt(*w_it->second, s),
                                   col_name)) {
              return true;
            }
          }
          continue;
        }
        StatusOr<std::shared_ptr<Table>> tbl = ctx.GetTable(osrc.table);
        if (!tbl.HasValue() || tbl.Value() == nullptr ||
            FindColInSchema(tbl.Value()->GetSchema(), col_name).has_value()) {
          return true;
        }
      }
      return false;
    };

    // Reference #2: top-level WHERE conjunct `c_col = (SELECT MAX/MIN(c_col)
    // FROM cte_name)`.
    std::vector<Expression> conjs = SplitAndConjuncts(outer->WhereClause());
    std::optional<size_t> matched_conj_idx;
    std::string matched_val_col;
    AggregationType matched_agg_type = AggregationType::kMax;

    for (size_t ci = 0; ci < conjs.size(); ++ci) {
      const Expression& c = conjs[ci];
      if (!c || c->Type() != TypeTag::kBinaryExp ||
          c->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
        continue;
      }
      const auto& bin = c->AsBinaryExpression();
      const Expression* col_side = nullptr;
      const Expression* sub_side = nullptr;
      if (bin.Left() && bin.Left()->Type() == TypeTag::kColumnValue &&
          bin.Right() && bin.Right()->Type() == TypeTag::kQueryExp) {
        col_side = &bin.Left();
        sub_side = &bin.Right();
      } else if (bin.Right() && bin.Right()->Type() == TypeTag::kColumnValue &&
                 bin.Left() && bin.Left()->Type() == TypeTag::kQueryExp) {
        col_side = &bin.Right();
        sub_side = &bin.Left();
      } else {
        continue;
      }

      const ColumnName& outer_cn = (*col_side)->AsColumnValue().GetColumnName();
      if (!outer_cn.schema.empty()) {
        const bool qual_ok =
            IdentifierEqualsCI(outer_cn.schema, outer_cte_qual) ||
            (outer_cte_src.alias.empty() &&
             IdentifierEqualsCI(outer_cn.schema, outer_cte_src.table));
        if (!qual_ok) {
          continue;
        }
      } else if (unqualified_conflicts_other_source(outer_cn.name)) {
        continue;
      }
      const auto& qexp = (*sub_side)->AsQueryExpression();
      if (qexp.Exists() || qexp.Negated() || qexp.Test() != nullptr ||
          qexp.ArrayResult() || qexp.Query() == nullptr) {
        continue;
      }
      const SelectStatement& msub = *qexp.Query();
      if (msub.Sources().size() != 1 || !msub.WithQueries().empty() ||
          !msub.UnionAll().empty() || msub.GetSetOperationTree() != nullptr ||
          msub.WhereClause() != nullptr || !msub.GroupBy().empty() ||
          msub.Having() != nullptr || msub.Qualify() != nullptr ||
          msub.HasLimit() || msub.Offset() != 0 ||
          msub.SelectList().size() != 1) {
        continue;
      }
      const SelectSource& msrc = msub.Sources()[0];
      if (!IdentifierEqualsCI(msrc.table, cte_name) || msrc.query != nullptr ||
          msrc.unnest != nullptr || msrc.sample.has_value()) {
        continue;
      }
      const std::string mqual = msrc.alias.empty() ? msrc.table : msrc.alias;
      const Expression& mexpr = msub.SelectList()[0].expression;
      if (!mexpr || mexpr->Type() != TypeTag::kAggregateExp) {
        continue;
      }
      const auto& magg = mexpr->AsAggregateExpression();
      if ((magg.GetType() != AggregationType::kMax &&
           magg.GetType() != AggregationType::kMin) ||
          magg.Distinct() || magg.HasWhereFilter() ||
          magg.NeedsGroupContext() || !magg.Child() ||
          magg.Child()->Type() != TypeTag::kColumnValue) {
        continue;
      }
      const ColumnName& sub_cn = magg.Child()->AsColumnValue().GetColumnName();
      if (!sub_cn.schema.empty() && !IdentifierEqualsCI(sub_cn.schema, mqual)) {
        continue;
      }
      if (!IdentifierEqualsCI(outer_cn.name, sub_cn.name)) {
        continue;
      }
      auto col_it = std::ranges::find_if(cte_cols, [&](const std::string& n) {
        return IdentifierEqualsCI(n, outer_cn.name);
      });
      if (col_it == cte_cols.end()) {
        continue;
      }
      // Guard against a missing expression at the StatusOr boundary. Computed
      // scalars are allowed: the wrapper below orders by the *projected*
      // `base.<val_col>` (not by the original formula), so a formula that
      // reads input columns hidden by its alias cannot change WITH TIES
      // ordering. Rejecting every non-aggregate/non-column item disabled the
      // rewrite for the NULL/NaN trap cases altogether.
      const auto& value_expr =
          cte_body->SelectList()[col_it - cte_cols.begin()].expression;
      if (!value_expr) {
        continue;
      }
      matched_conj_idx = ci;
      matched_val_col = *col_it;
      matched_agg_type = magg.GetType();
      break;
    }

    if (!matched_conj_idx.has_value()) {
      continue;
    }

    // Trap 4a (`argmax_with_ties_without_is_not_null_bug`) + GoogleSQL NaN
    // poisoning guard:
    // 1. Filter `WHERE val_col IS NOT NULL` before `FETCH FIRST 1 ROWS WITH
    // TIES`
    //    so an all-NULL CTE emits 0 rows (matching `NULL = NULL` -> UNKNOWN).
    // 2. Sort `(CASE WHEN val_col = val_col THEN 0 ELSE 1 END) DESC` first so
    //    any IEEE-754 `NaN` (which poisons GoogleSQL `MAX`/`MIN` to `NaN` even
    //    though `CompareForOrderBy` orders `NaN < -Inf`) wins the Top-1 tie
    //    slot, and replace the outer conjunct with `val_col = val_col` so `NaN`
    //    is rejected (`NaN = NaN` -> FALSE -> 0 rows, exact match!).
    auto inner_copy = std::make_shared<SelectStatement>(*cte_body);
    SelectStatement wrapped;
    const std::string base_alias = "__tpch_argmax_base";
    wrapped.SetSources({SelectSource("", base_alias, std::move(inner_copy),
                                     JoinType::kCross, nullptr)});
    std::vector<NamedExpression> wrap_sel;
    wrap_sel.reserve(cte_cols.size());
    for (const std::string& cname : cte_cols) {
      wrap_sel.emplace_back(cname,
                            ColumnValueExp(ColumnName(base_alias, cname)));
    }
    wrapped.SetSelectList(std::move(wrap_sel));
    const Expression base_val_expr =
        ColumnValueExp(ColumnName(base_alias, matched_val_col));
    wrapped.SetWhereClause(
        UnaryExpressionExp(base_val_expr, UnaryOperation::kIsNotNull));
    const Expression is_nan_rank = CaseExpressionExp(
        {{BinaryExpressionExp(base_val_expr, BinaryOperation::kEquals,
                              base_val_expr),
          ConstantValueExp(Value(int64_t{0}))}},
        ConstantValueExp(Value(int64_t{1})));
    wrapped.SetOrderBy(
        {SelectStatement::OrderByTerm{.expression = is_nan_rank,
                                      .ascending = false,
                                      .nulls_first = false},
         SelectStatement::OrderByTerm{
             .expression = base_val_expr,
             .ascending = (matched_agg_type == AggregationType::kMin),
             .nulls_first = false}});
    wrapped.SetOffset(0);
    wrapped.SetLimit(1);
    wrapped.SetWithTies(true);

    *cte_body = std::move(wrapped);
    const Expression outer_val_expr =
        ColumnValueExp(ColumnName(outer_cte_qual, matched_val_col));
    conjs[*matched_conj_idx] = BinaryExpressionExp(
        outer_val_expr, BinaryOperation::kEquals, outer_val_expr);
    outer->SetWhereClause(CombineAndConjuncts(conjs));
    any_rewritten = true;
  }

  return any_rewritten;
}

// Lean proof: lean/TinylambOpt/JoinAndAggregatePushdown.lean
//   (equijoin_transitive_closure_sound)
bool SynthesizeTransitiveEquijoins(SelectStatement* select,
                                   TransactionContext& ctx) {
  return SynthesizeTransitiveEquijoinsRecursive(select, {}, ctx);
}

// Lean proof: lean/TinylambOpt/JoinAndAggregatePushdown.lean
//   (topn_agg_late_unique_dim_attribute_join_sound,
//    late_dim_join_without_key_semijoin_steals_limit_bug)
bool DeferUniqueDimensionAttributesAfterTopN(SelectStatement* select,
                                             TransactionContext& ctx) {
  if (select == nullptr) {
    return false;
  }
  std::unordered_set<std::string> ctes;
  CollectCteNamesInScope(*select, &ctes);
  return DeferUniqueDimAfterTopNOneSelect(select, ctes, ctx);
}

// Lean proof: lean/TinylambOpt/JoinAndAggregatePushdown.lean
//   (correlated_gt_sum_decorrelate_to_inner_join_sound,
//    correlated_gt_count_demotes_to_inner_join_bug)
bool DecorrelateCorrelatedSumComparisons(SelectStatement* select,
                                         TransactionContext& ctx) {
  size_t alias_seq = 0;
  std::unordered_set<std::string> used_aliases;
  if (select != nullptr) {
    CollectCorrSumAliasesInUse(*select, &used_aliases);
  }
  return DecorrelateCorrelatedSumRecursive(select, {}, ctx, &alias_seq,
                                           &used_aliases);
}

namespace {

struct CertifiedInt64ColBounds {
  int64_t min_val{0};
  int64_t max_val{0};
  uint64_t row_upper_bound{0};
  bool non_empty_bounds{false};
};

std::optional<CertifiedInt64ColBounds> QueryCertifiedInt64Bounds(
    TransactionContext& ctx, const Table& table, slot_t col_idx) {
  const Schema& schema = table.GetSchema();
  if (col_idx >= schema.ColumnCount()) {
    return std::nullopt;
  }
  const Column& col = schema.GetColumn(col_idx);
  if (col.Type() != ValueType::kInt64 || col.IsUnsigned() || col.IsInterval()) {
    return std::nullopt;
  }
  if (ctx.txn_.IsFailed() || table.RowsRewrittenInPlace()) {
    return std::nullopt;
  }
  if (!ctx.txn_.IsReadOnly() && !ctx.txn_.WriteSetEmpty()) {
    return std::nullopt;
  }
  const uint64_t generation = table.LayoutGeneration();
  if (generation == 0) {
    return std::nullopt;
  }
  if (ctx.txn_.HasAnyPageVersionChains()) {
    ctx.txn_.SweepCommittedVersionChains();
    if (ctx.txn_.HasAnyPageVersionChains()) {
      return std::nullopt;
    }
  }

  // 1. Check backing `.tlcs` directory metadata directly (O(blocks), zero data
  //    decompression).
  if (const auto tlcs = table.GetBackingTlcsStore();
      tlcs != nullptr && tlcs->LayoutGeneration() == generation &&
      tlcs->FirstPageId() == table.FirstPageId()) {
    const uint32_t dg_count = tlcs->DirGroupCount();
    CertifiedInt64ColBounds out;
    out.min_val = std::numeric_limits<int64_t>::max();
    out.max_val = std::numeric_limits<int64_t>::min();
    for (uint32_t g = 0; g < dg_count; ++g) {
      auto grp_or = tlcs->GetDirGroup(g);
      if (!grp_or.HasValue()) {
        return std::nullopt;
      }
      for (const auto& blk : grp_or.Value()->blocks) {
        // An all-NULL block contributes no value to SUM(col) and no row to
        // COUNT(col); counting it only inflates the overflow bound. Blocks
        // without metadata for this column stay counted (conservative).
        if (col_idx < blk.columns.size() &&
            blk.columns[col_idx].encoding == TlcsColumnEncoding::kConstNull) {
          continue;
        }
        out.row_upper_bound += blk.row_count;
        if (blk.row_count == 0 || col_idx >= blk.columns.size()) {
          continue;
        }
        const auto& meta = blk.columns[col_idx];
        if (!meta.has_min_max) {
          return std::nullopt;
        }
        out.min_val = std::min(out.min_val, meta.min_int);
        out.max_val = std::max(out.max_val, meta.max_int);
        out.non_empty_bounds = true;
      }
    }
    if (!out.non_empty_bounds) {
      out.min_val = 0;
      out.max_val = 0;
    }
    return out;
  }

  // 2. Check resident / lazily built single-column `LSTableStore` on PAX
  // tables.
  const std::vector<slot_t> single_proj{col_idx};
  std::shared_ptr<const LSTableStore> store = GetOrBuildLSTableStore(
      ctx.txn_, table, /*allow_lazy_build=*/true, &single_proj);
  if (store == nullptr || store->layout_generation != generation ||
      store->first_pid != table.FirstPageId() || !store->HasColumn(col_idx) ||
      col_idx >= store->column_min_int.size() ||
      col_idx >= store->column_max_int.size()) {
    return std::nullopt;
  }
  if (store->uncommitted_owner_txn != 0 &&
      store->uncommitted_owner_txn != ctx.txn_.ID()) {
    return std::nullopt;
  }
  CertifiedInt64ColBounds out;
  out.row_upper_bound = store->total_rows;
  if (store->column_min_int[col_idx] <= store->column_max_int[col_idx]) {
    out.min_val = store->column_min_int[col_idx];
    out.max_val = store->column_max_int[col_idx];
    out.non_empty_bounds = true;
  }
  return out;
}

bool MatchAffineSumArg(const Expression& child,
                       const std::vector<ResolvedBaseRel>& rels,
                       slot_t* slot_out, Expression* col_expr_out,
                       int64_t* delta_out) {
  if (!child || child->Type() != TypeTag::kBinaryExp) {
    return false;
  }
  const auto& bin = child->AsBinaryExpression();
  if (bin.Op() != BinaryOperation::kAdd &&
      bin.Op() != BinaryOperation::kSubtract) {
    return false;
  }
  Expression col_side;
  Expression const_side;
  bool negate_const = false;
  if (bin.Left() && bin.Left()->Type() == TypeTag::kColumnValue &&
      bin.Right() && bin.Right()->Type() == TypeTag::kConstantValue) {
    col_side = bin.Left();
    const_side = bin.Right();
    negate_const = (bin.Op() == BinaryOperation::kSubtract);
  } else if (bin.Op() == BinaryOperation::kAdd && bin.Left() &&
             bin.Left()->Type() == TypeTag::kConstantValue && bin.Right() &&
             bin.Right()->Type() == TypeTag::kColumnValue) {
    const_side = bin.Left();
    col_side = bin.Right();
  } else {
    return false;
  }
  const Value& cv = const_side->AsConstantValue().GetValue();
  if (cv.IsNull() || cv.IsUnsigned() || cv.type != ValueType::kInt64) {
    return false;
  }
  const int64_t raw_c = cv.value.int_value;
  if (negate_const && raw_c == std::numeric_limits<int64_t>::min()) {
    return false;
  }
  const auto ref =
      ResolveColRef(col_side->AsColumnValue().GetColumnName(), rels);
  if (!ref.has_value() || ref->rel_idx != 0) {
    return false;
  }
  const Column& col = rels[0].schema.GetColumn(ref->slot);
  if (col.Type() != ValueType::kInt64 || col.IsUnsigned() || col.IsInterval()) {
    return false;
  }
  *slot_out = ref->slot;
  *col_expr_out = col_side;
  *delta_out = negate_const ? -raw_c : raw_c;
  return true;
}

bool RewriteAffineLinearSumsOneSelect(
    SelectStatement* stmt, const std::unordered_set<std::string>& ctes,
    TransactionContext& ctx) {
  if (stmt == nullptr || stmt->Distinct() || stmt->HasDistinctOn() ||
      stmt->WithTies() || stmt->Qualify() != nullptr ||
      !stmt->UnionAll().empty() || stmt->GetSetOperationTree() != nullptr ||
      stmt->Sources().size() != 1) {
    return false;
  }
  std::vector<ResolvedBaseRel> rels;
  if (!ResolvePlainBaseSources(*stmt, ctes, ctx, &rels) || rels.size() != 1 ||
      rels[0].table == nullptr) {
    return false;
  }

  std::unordered_map<slot_t, std::vector<int64_t>> col_deltas;
  const std::function<void(const Expression&)> collect_aggs =
      [&](const Expression& e) {
        if (!e) {
          return;
        }
        if (e->Type() == TypeTag::kAggregateExp) {
          const auto& agg = e->AsAggregateExpression();
          if (agg.GetType() == AggregationType::kSum && !agg.Distinct() &&
              !agg.IgnoreNulls() && !agg.HasWhereFilter() &&
              !agg.NeedsGroupContext() && !agg.HasInnerGroupBy()) {
            slot_t sl = 0;
            Expression col_e;
            int64_t delta = 0;
            if (MatchAffineSumArg(agg.Child(), rels, &sl, &col_e, &delta)) {
              col_deltas[sl].push_back(delta);
            } else if (agg.Child() &&
                       agg.Child()->Type() == TypeTag::kColumnValue) {
              const auto ref = ResolveColRef(
                  agg.Child()->AsColumnValue().GetColumnName(), rels);
              if (ref.has_value() && ref->rel_idx == 0) {
                const Column& col = rels[0].schema.GetColumn(ref->slot);
                if (col.Type() == ValueType::kInt64 && !col.IsUnsigned() &&
                    !col.IsInterval()) {
                  col_deltas[ref->slot].push_back(0);
                }
              }
            }
          }
          return;
        }
        if (e->Type() == TypeTag::kWindowFunctionExp ||
            e->Type() == TypeTag::kQueryExp) {
          return;
        }
        for (const Expression& ch : ExpressionChildren(e)) {
          collect_aggs(ch);
        }
      };

  for (const NamedExpression& se : stmt->SelectList()) {
    collect_aggs(se.expression);
  }
  collect_aggs(stmt->Having());
  for (const auto& ob : stmt->OrderBy()) {
    collect_aggs(ob.expression);
  }
  if (col_deltas.empty()) {
    return false;
  }

  std::unordered_set<slot_t> certified_slots;
  for (const auto& [slot, deltas] : col_deltas) {
    const auto bounds = QueryCertifiedInt64Bounds(ctx, *rels[0].table, slot);
    if (!bounds.has_value()) {
      continue;
    }
    const auto i64_min =
        static_cast<__int128>(std::numeric_limits<int64_t>::min());
    const auto i64_max =
        static_cast<__int128>(std::numeric_limits<int64_t>::max());
    const auto n = static_cast<__int128>(bounds->row_upper_bound);
    const auto lo = static_cast<__int128>(bounds->min_val);
    const auto hi = static_cast<__int128>(bounds->max_val);
    const __int128 min_neg_sum =
        (bounds->non_empty_bounds && lo < 0) ? (n * lo) : 0;
    const __int128 max_pos_sum =
        (bounds->non_empty_bounds && hi > 0) ? (n * hi) : 0;
    if (min_neg_sum < i64_min || max_pos_sum > i64_max) {
      continue;
    }
    bool all_ok = true;
    for (const int64_t d64 : deltas) {
      const auto d = static_cast<__int128>(d64);
      if (bounds->non_empty_bounds && (lo + d < i64_min || lo + d > i64_max ||
                                       hi + d < i64_min || hi + d > i64_max)) {
        all_ok = false;
        break;
      }
      const __int128 max_shift = d * n;
      if (max_shift < i64_min || max_shift > i64_max) {
        all_ok = false;
        break;
      }
      // SUM(col + d) aggregates at most `n` non-NULL rows, each shifted into
      // [lo+d, hi+d]. Compute the extremes of the *shifted* range directly:
      // adding the sign-split unshifted sums to d*n assumed a fixed sign for
      // n*hi and could both over- and under-estimate the true overflow range
      // when d is negative (or positive) and rows span zero.
      const __int128 shifted_lo = lo + d;
      const __int128 shifted_hi = hi + d;
      // `shifted_hi` can be as large as 2^64 and `n` as large as 2^64, so
      // their product may exceed signed __int128. Decide overflow by
      // division before multiplying instead of multiplying then comparing.
      bool extremes_ok = true;
      __int128 worst_lo = 0;
      __int128 worst_hi = 0;
      if (bounds->non_empty_bounds && n > 0) {
        if (shifted_lo < 0) {
          if (shifted_lo < i64_min / n) {
            extremes_ok = false;
          } else {
            worst_lo = n * shifted_lo;
          }
        }
        if (shifted_hi > 0) {
          if (shifted_hi > i64_max / n) {
            extremes_ok = false;
          } else {
            worst_hi = n * shifted_hi;
          }
        }
      }
      if (!extremes_ok || worst_lo < i64_min || worst_hi > i64_max) {
        all_ok = false;
        break;
      }
    }
    if (all_ok) {
      certified_slots.insert(slot);
    }
  }
  if (certified_slots.empty()) {
    return false;
  }

  // Lean proof: lean/TinylambOpt/CountBoundsAndBlockAggregates.lean
  //   (affine_sum_no_overflow_fusion_sound,
  //   affine_sum_row_overflow_unsound_bug)
  // Share canonical `SUM(col)` / `COUNT(col)` AggregateExpression instances per
  // slot so `CollectAggregates` deduplicates 90+ shifted sums (ClickBench Q30)
  // into a single `(SUM(col), COUNT(col))` pair (< 128 typed aggregates).
  std::unordered_map<slot_t, std::pair<Expression, Expression>> canon_slot_aggs;
  auto get_canon_aggs =
      [&](slot_t sl,
          const Expression& col_e) -> const std::pair<Expression, Expression>& {
    auto it = canon_slot_aggs.find(sl);
    if (it == canon_slot_aggs.end()) {
      it = canon_slot_aggs
               .emplace(
                   sl,
                   std::pair{
                       AggregateExpressionExp(AggregationType::kSum, col_e),
                       AggregateExpressionExp(AggregationType::kCount, col_e)})
               .first;
    }
    return it->second;
  };

  bool any_rewritten = false;
  const std::function<Expression(const Expression&)> rewrite_expr =
      [&](const Expression& e) -> Expression {
    if (!e) {
      return e;
    }
    if (e->Type() == TypeTag::kAggregateExp) {
      const auto& agg = e->AsAggregateExpression();
      if (agg.GetType() == AggregationType::kSum && !agg.Distinct() &&
          !agg.IgnoreNulls() && !agg.HasWhereFilter() &&
          !agg.NeedsGroupContext() && !agg.HasInnerGroupBy()) {
        slot_t sl = 0;
        Expression col_e;
        int64_t delta = 0;
        if (MatchAffineSumArg(agg.Child(), rels, &sl, &col_e, &delta) &&
            certified_slots.contains(sl)) {
          any_rewritten = true;
          const auto& [sum_col, cnt_col] = get_canon_aggs(sl, col_e);
          if (delta == 0) {
            return sum_col;
          }
          Expression shift_term =
              BinaryExpressionExp(ConstantValueExp(Value(delta)),
                                  BinaryOperation::kMultiply, cnt_col);
          return BinaryExpressionExp(sum_col, BinaryOperation::kAdd,
                                     shift_term);
        }
        if (agg.Child() && agg.Child()->Type() == TypeTag::kColumnValue) {
          const auto ref =
              ResolveColRef(agg.Child()->AsColumnValue().GetColumnName(), rels);
          if (ref.has_value() && ref->rel_idx == 0 &&
              certified_slots.contains(ref->slot)) {
            const auto& [sum_col, cnt_col] =
                get_canon_aggs(ref->slot, agg.Child());
            (void)cnt_col;
            return sum_col;
          }
        }
      }
      return e;
    }
    if (e->Type() == TypeTag::kWindowFunctionExp ||
        e->Type() == TypeTag::kQueryExp) {
      return e;
    }
    const auto orig = ExpressionChildren(e);
    if (orig.empty()) {
      return e;
    }
    std::vector<Expression> next;
    next.reserve(orig.size());
    bool ch_changed = false;
    for (const Expression& ch : orig) {
      Expression nr = rewrite_expr(ch);
      if (nr.get() != ch.get()) {
        ch_changed = true;
      }
      next.push_back(std::move(nr));
    }
    return ch_changed ? WithExpressionChildren(e, std::move(next)) : e;
  };

  std::vector<NamedExpression> new_select = stmt->SelectList();
  for (NamedExpression& se : new_select) {
    if (!se.expression) {
      continue;
    }
    Expression nr = rewrite_expr(se.expression);
    if (nr.get() != se.expression.get()) {
      if (se.name.empty()) {
        se.name = se.expression->ToString();
      }
      se.expression = std::move(nr);
    }
  }
  Expression new_having =
      stmt->Having() ? rewrite_expr(stmt->Having()) : nullptr;
  auto new_order = stmt->OrderBy();
  for (auto& ob : new_order) {
    if (ob.expression) {
      ob.expression = rewrite_expr(ob.expression);
    }
  }
  if (!any_rewritten) {
    return false;
  }
  stmt->SetSelectList(std::move(new_select));
  stmt->SetHaving(std::move(new_having));
  stmt->SetOrderBy(std::move(new_order));
  return true;
}

bool RewriteAffineLinearSumsRecursive(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt, std::unordered_set<std::string> ctes,
    TransactionContext& ctx) {
  if (stmt == nullptr) {
    return false;
  }
  CollectCteNamesInScope(*stmt, &ctes);
  bool changed = false;
  for (const auto& [name, body] : stmt->WithQueries()) {
    if (!stmt->IsRecursiveWith(name) && body != nullptr) {
      changed |= RewriteAffineLinearSumsRecursive(body.get(), ctes, ctx);
    }
  }
  for (const SelectSource& src : stmt->Sources()) {
    if (src.query != nullptr && !src.is_lateral) {
      changed |= RewriteAffineLinearSumsRecursive(src.query.get(), ctes, ctx);
    }
  }
  changed |= RewriteAffineLinearSumsOneSelect(stmt, ctes, ctx);
  return changed;
}

}  // namespace

// Lean proof: lean/TinylambOpt/CountBoundsAndBlockAggregates.lean
//   (affine_sum_homomorphism_sound, affine_sum_coalesce_empty_bug,
//    affine_sum_overflow_masked_bug, affine_sum_no_overflow_fusion_sound)
bool RewriteAffineLinearSums(SelectStatement* select, TransactionContext& ctx) {
  return RewriteAffineLinearSumsRecursive(select, {}, ctx);
}

namespace {

// Bottom-up Kleene 3VL common-conjunct factoring over OR trees:
// `(A AND B) OR (A AND C) -> A AND (B OR C)` and `(A AND B) OR A -> A`.
// Lean proof: lean/TinylambOpt/PredicateAndRelationalLowering.lean
//   (kleene_and_or_distrib_left_sound)
Expression FactorCommonConjunctsInOrTree(  // NOLINT(misc-no-recursion)
    const Expression& expr, size_t depth = 0) {
  if (!expr || depth > 64 || ExprHasParamOrSubqueryOrVolatile(expr) ||
      ExprContainsAggregateNode(expr) || expr->Type() != TypeTag::kBinaryExp) {
    return expr;
  }
  const BinaryOperation op = expr->AsBinaryExpression().Op();
  if (op == BinaryOperation::kAnd) {
    std::vector<Expression> parts = SplitAndConjuncts(expr);
    bool any_changed = false;
    for (Expression& p : parts) {
      Expression np = FactorCommonConjunctsInOrTree(p, depth + 1);
      if (np.get() != p.get()) {
        any_changed = true;
        p = std::move(np);
      }
    }
    return any_changed ? CombineAndConjuncts(parts) : expr;
  }
  if (op != BinaryOperation::kOr) {
    return expr;
  }

  std::vector<Expression> branches = SplitOrDisjuncts(expr);
  if (branches.size() < 2) {
    return expr;
  }
  bool child_changed = false;
  for (Expression& b : branches) {
    Expression nb = FactorCommonConjunctsInOrTree(b, depth + 1);
    if (nb.get() != b.get()) {
      child_changed = true;
      b = std::move(nb);
    }
  }

  std::vector<std::vector<Expression>> branch_conjs(branches.size());
  for (size_t i = 0; i < branches.size(); ++i) {
    branch_conjs[i] = SplitAndConjuncts(branches[i]);
  }

  std::vector<Expression> hoisted;
  hoisted.reserve(branch_conjs[0].size());
  for (const Expression& cand : branch_conjs[0]) {
    bool in_all = true;
    for (size_t bi = 1; bi < branch_conjs.size(); ++bi) {
      const bool found = std::ranges::any_of(
          branch_conjs[bi],
          [&](const Expression& other) { return ExprAstEquals(cand, other); });
      if (!found) {
        in_all = false;
        break;
      }
    }
    if (in_all && std::ranges::none_of(hoisted, [&](const Expression& prev) {
          return ExprAstEquals(cand, prev);
        })) {
      hoisted.push_back(cand);
    }
  }
  if (hoisted.empty()) {
    return child_changed ? CombineOrDisjuncts(branches) : expr;
  }

  bool any_empty_residual = false;
  std::vector<Expression> rebuilt_branches;
  rebuilt_branches.reserve(branch_conjs.size());
  for (const auto& bcs : branch_conjs) {
    std::vector<Expression> residual;
    residual.reserve(bcs.size());
    for (const Expression& item : bcs) {
      const bool was_hoisted = std::ranges::any_of(
          hoisted, [&](const Expression& h) { return ExprAstEquals(item, h); });
      if (!was_hoisted) {
        residual.push_back(item);
      }
    }
    if (residual.empty()) {
      any_empty_residual = true;
      break;
    }
    Expression rb = CombineAndConjuncts(residual);
    if (std::ranges::none_of(rebuilt_branches, [&](const Expression& prev) {
          return ExprAstEquals(prev, rb);
        })) {
      rebuilt_branches.push_back(std::move(rb));
    }
  }

  if (any_empty_residual) {
    // `(A AND B) OR A == A` in Kleene 3-valued logic across all 9 truth states.
    return CombineAndConjuncts(hoisted);
  }
  hoisted.push_back(CombineOrDisjuncts(rebuilt_branches));
  return CombineAndConjuncts(hoisted);
}

bool RewriteCorrelatedPositiveCountOneSelect(
    SelectStatement* stmt, const std::unordered_set<std::string>& ctes,
    TransactionContext& ctx) {
  if (stmt == nullptr || !stmt->WhereClause() || stmt->Sources().empty() ||
      !stmt->UnionAll().empty() || stmt->GetSetOperationTree() != nullptr ||
      stmt->ForUpdate()) {
    return false;
  }
  std::vector<ResolvedBaseRel> outer_rels;
  if (!ResolvePlainBaseSources(*stmt, ctes, ctx, &outer_rels) ||
      outer_rels.empty()) {
    return false;
  }

  auto match_positive_count_cmp = [](const Expression& conj,
                                     const QueryExpression** q_out) -> bool {
    if (!conj || conj->Type() != TypeTag::kBinaryExp) {
      return false;
    }
    const auto& bin = conj->AsBinaryExpression();
    const BinaryOperation op = bin.Op();
    const Expression& lhs = bin.Left();
    const Expression& rhs = bin.Right();
    auto is_int_const = [](const Expression& e, int64_t* val_out) -> bool {
      if (!e || e->Type() != TypeTag::kConstantValue ||
          dynamic_cast<const ParameterSlot*>(e.get()) != nullptr) {
        return false;
      }
      const Value& v = e->AsConstantValue().GetValue();
      if (v.IsNull() || v.IsUnsigned() || v.type != ValueType::kInt64) {
        return false;
      }
      *val_out = v.value.int_value;
      return true;
    };
    int64_t k = 0;
    if (lhs && lhs->Type() == TypeTag::kQueryExp && is_int_const(rhs, &k)) {
      if ((op == BinaryOperation::kGreaterThan && k == 0) ||
          (op == BinaryOperation::kGreaterThanEquals && k == 1)) {
        *q_out = &lhs->AsQueryExpression();
        return true;
      }
    }
    if (rhs && rhs->Type() == TypeTag::kQueryExp && is_int_const(lhs, &k)) {
      if ((op == BinaryOperation::kLessThan && k == 0) ||
          (op == BinaryOperation::kLessThanEquals && k == 1)) {
        *q_out = &rhs->AsQueryExpression();
        return true;
      }
    }
    return false;
  };

  const std::vector<Expression> conjs = SplitAndConjuncts(stmt->WhereClause());
  std::vector<Expression> out_conjs;
  out_conjs.reserve(conjs.size());
  bool changed = false;

  for (const Expression& conj : conjs) {
    const QueryExpression* qptr = nullptr;
    if (!match_positive_count_cmp(conj, &qptr) || qptr == nullptr ||
        qptr->Exists() || qptr->Negated() || qptr->Test() != nullptr ||
        qptr->ArrayResult() || qptr->Query() == nullptr) {
      out_conjs.push_back(conj);
      continue;
    }

    const SelectStatement& sub = *qptr->Query();
    if (sub.Sources().size() != 1 || sub.Sources()[0].sample.has_value() ||
        !sub.WithQueries().empty() || !sub.UnionAll().empty() ||
        sub.GetSetOperationTree() != nullptr || sub.Distinct() ||
        sub.HasDistinctOn() || !sub.GroupBy().empty() ||
        sub.Having() != nullptr || sub.Qualify() != nullptr || sub.HasLimit() ||
        sub.Offset() != 0 || sub.SelectList().size() != 1 ||
        !sub.WhereClause() || sub.ForUpdate()) {
      out_conjs.push_back(conj);
      continue;
    }

    const Expression& sel_expr = sub.SelectList()[0].expression;
    if (!sel_expr || sel_expr->Type() != TypeTag::kAggregateExp) {
      out_conjs.push_back(conj);
      continue;
    }
    const auto& agg = sel_expr->AsAggregateExpression();
    if (agg.GetType() != AggregationType::kCount || agg.Distinct() ||
        agg.IgnoreNulls() || agg.HasWhereFilter() || agg.NeedsGroupContext() ||
        agg.HasInnerGroupBy()) {
      out_conjs.push_back(conj);
      continue;
    }

    std::vector<ResolvedBaseRel> inner_rels;
    if (!ResolvePlainBaseSources(sub, ctes, ctx, &inner_rels) ||
        inner_rels.size() != 1) {
      out_conjs.push_back(conj);
      continue;
    }

    auto resolve_scoped = [&](const ColumnName& cn, bool* is_inner_out,
                              ResolvedColRef* ref_out) -> bool {
      if (cn.name.empty() || cn.name == "*") {
        return false;
      }
      if (!cn.schema.empty()) {
        if (const auto ir = ResolveColRef(cn, inner_rels)) {
          *is_inner_out = true;
          *ref_out = *ir;
          return true;
        }
        if (const auto or_ref = ResolveColRef(cn, outer_rels)) {
          *is_inner_out = false;
          *ref_out = *or_ref;
          return true;
        }
        return false;
      }
      if (const auto islot = FindColInSchema(inner_rels[0].schema, cn.name)) {
        *is_inner_out = true;
        *ref_out = ResolvedColRef{.rel_idx = 0, .slot = *islot};
        return true;
      }
      if (const auto or_ref = ResolveColRef(cn, outer_rels)) {
        *is_inner_out = false;
        *ref_out = *or_ref;
        return true;
      }
      return false;
    };

    // Validate `COUNT(*)` / `COUNT(non_null_const)` / `COUNT(inner_corr_col)`.
    bool count_star_or_const = false;
    std::optional<slot_t> counted_inner_slot;
    if (!agg.Child() ||
        (agg.Child()->Type() == TypeTag::kColumnValue &&
         agg.Child()->AsColumnValue().GetColumnName().schema.empty() &&
         agg.Child()->AsColumnValue().GetColumnName().name == "*") ||
        (agg.Child()->Type() == TypeTag::kConstantValue &&
         dynamic_cast<const ParameterSlot*>(agg.Child().get()) == nullptr &&
         !agg.Child()->AsConstantValue().GetValue().IsNull())) {
      count_star_or_const = true;
    } else if (agg.Child()->Type() == TypeTag::kColumnValue) {
      bool is_in = false;
      ResolvedColRef rr;
      if (resolve_scoped(agg.Child()->AsColumnValue().GetColumnName(), &is_in,
                         &rr) &&
          is_in) {
        counted_inner_slot = rr.slot;
      }
    }
    if (!count_star_or_const && !counted_inner_slot.has_value()) {
      out_conjs.push_back(conj);
      continue;
    }

    const std::string& inner_qual = inner_rels[0].qualifier;
    bool qualify_ok = true;
    const std::function<Expression(const Expression&, bool)>
        rewrite_inner_cols =
            [&](const Expression& e, bool with_qualifier) -> Expression {
      if (!e || !qualify_ok) {
        return e;
      }
      if (e->Type() == TypeTag::kColumnValue) {
        bool is_in = false;
        ResolvedColRef rr;
        if (!resolve_scoped(e->AsColumnValue().GetColumnName(), &is_in, &rr) ||
            !is_in) {
          qualify_ok = false;
          return e;
        }
        const std::string& cname =
            inner_rels[0].schema.GetColumn(rr.slot).Name().name;
        return ColumnValueExp(with_qualifier ? ColumnName(inner_qual, cname)
                                             : ColumnName(cname));
      }
      const auto orig = ExpressionChildren(e);
      if (orig.empty()) {
        return e;
      }
      std::vector<Expression> next;
      next.reserve(orig.size());
      for (const Expression& ch : orig) {
        next.push_back(rewrite_inner_cols(ch, with_qualifier));
        if (!qualify_ok) {
          return e;
        }
      }
      return WithExpressionChildren(e, std::move(next));
    };

    const Expression factored_where =
        FactorCommonConjunctsInOrTree(sub.WhereClause());
    struct CorrEq {
      slot_t inner_slot{0};
      ResolvedColRef outer_ref;
    };
    std::vector<CorrEq> corr_eqs;
    std::vector<Expression> inner_local_preds;
    bool sub_ok = true;

    for (const Expression& sc : SplitAndConjuncts(factored_where)) {
      if (!sc || ExprHasParamOrSubqueryOrVolatile(sc) ||
          ExprContainsAggregateNode(sc)) {
        sub_ok = false;
        break;
      }
      bool touches_in = false;
      bool touches_out = false;
      for (const ColumnName& cn : sc->TouchedColumns()) {
        bool is_in = false;
        ResolvedColRef rr;
        if (!resolve_scoped(cn, &is_in, &rr)) {
          sub_ok = false;
          break;
        }
        if (is_in) {
          touches_in = true;
        } else {
          touches_out = true;
        }
      }
      if (!sub_ok) {
        break;
      }
      if (!touches_out) {
        qualify_ok = true;
        Expression unqual = rewrite_inner_cols(sc, /*with_qualifier=*/false);
        if (!qualify_ok || !SafeToReduceEvaluationCountWithSchema(
                               unqual, inner_rels[0].schema)) {
          sub_ok = false;
          break;
        }
        qualify_ok = true;
        Expression qual = rewrite_inner_cols(sc, /*with_qualifier=*/true);
        if (!qualify_ok) {
          sub_ok = false;
          break;
        }
        inner_local_preds.push_back(std::move(qual));
        continue;
      }
      if (touches_in && touches_out && sc->Type() == TypeTag::kBinaryExp &&
          sc->AsBinaryExpression().Op() == BinaryOperation::kEquals &&
          sc->AsBinaryExpression().Left()->Type() == TypeTag::kColumnValue &&
          sc->AsBinaryExpression().Right()->Type() == TypeTag::kColumnValue) {
        bool l_in = false;
        bool r_in = false;
        ResolvedColRef lr;
        ResolvedColRef rr;
        if (!resolve_scoped(sc->AsBinaryExpression()
                                .Left()
                                ->AsColumnValue()
                                .GetColumnName(),
                            &l_in, &lr) ||
            !resolve_scoped(sc->AsBinaryExpression()
                                .Right()
                                ->AsColumnValue()
                                .GetColumnName(),
                            &r_in, &rr) ||
            l_in == r_in) {
          sub_ok = false;
          break;
        }
        const ResolvedColRef in_side = l_in ? lr : rr;
        const ResolvedColRef out_side = l_in ? rr : lr;
        const Column& ic = inner_rels[0].schema.GetColumn(in_side.slot);
        const Column& oc =
            outer_rels[out_side.rel_idx].schema.GetColumn(out_side.slot);
        if (!ComparableEquijoinColumns(ic, oc)) {
          sub_ok = false;
          break;
        }
        if (std::ranges::none_of(corr_eqs, [&](const CorrEq& prev) {
              return prev.inner_slot == in_side.slot &&
                     prev.outer_ref == out_side;
            })) {
          corr_eqs.push_back(
              CorrEq{.inner_slot = in_side.slot, .outer_ref = out_side});
        }
        continue;
      }
      sub_ok = false;
      break;
    }

    if (!sub_ok || corr_eqs.size() != 1) {
      out_conjs.push_back(conj);
      continue;
    }
    if (counted_inner_slot.has_value() &&
        *counted_inner_slot != corr_eqs[0].inner_slot) {
      out_conjs.push_back(conj);
      continue;
    }

    const ColumnName inner_key_cn(
        inner_qual,
        inner_rels[0].schema.GetColumn(corr_eqs[0].inner_slot).Name().name);
    const ColumnName outer_key_cn(
        outer_rels[corr_eqs[0].outer_ref.rel_idx].qualifier,
        outer_rels[corr_eqs[0].outer_ref.rel_idx]
            .schema.GetColumn(corr_eqs[0].outer_ref.slot)
            .Name()
            .name);
    inner_local_preds.push_back(UnaryExpressionExp(ColumnValueExp(inner_key_cn),
                                                   UnaryOperation::kIsNotNull));

    auto in_sub = std::make_shared<SelectStatement>();
    SelectSource inner_src = sub.Sources()[0];
    if (inner_src.alias.empty()) {
      inner_src.alias = inner_qual;
    }
    inner_src.join_type = JoinType::kCross;
    inner_src.join_condition = nullptr;
    in_sub->SetSources({std::move(inner_src)});
    in_sub->SetWhereClause(CombineAndConjuncts(inner_local_preds));
    in_sub->SetSelectList({NamedExpression("", ColumnValueExp(inner_key_cn))});

    out_conjs.push_back(
        QueryExpressionExp(std::move(in_sub), ColumnValueExp(outer_key_cn),
                           /*exists=*/false, /*negated=*/false));
    changed = true;
  }

  if (changed) {
    stmt->SetWhereClause(CombineAndConjuncts(out_conjs));
  }
  return changed;
}

bool RewriteCorrelatedPositiveCountRecursive(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt, std::unordered_set<std::string> ctes,
    TransactionContext& ctx) {
  if (stmt == nullptr) {
    return false;
  }
  // Names already visible from enclosing scopes, captured before this level's
  // WITH names merge in. A nested WITH may reuse one of them: the body's
  // self-named reference then still resolves to the outer CTE, so the name
  // must stay in scope (same rule as CollectStatementTableRefs' shadowed
  // set).
  const std::unordered_set<std::string> outer_ctes = ctes;
  CollectCteNamesInScope(*stmt, &ctes);
  bool changed = false;
  for (const auto& [name, body] : stmt->WithQueries()) {
    // A non-recursive CTE is not visible inside its own definition: its
    // self-named reference resolves to the enclosing WITH or to a catalog
    // table. Do not leak the outer name into the body's scope, or a real
    // table with that name is misclassified as a CTE reference.
    if (body == nullptr || stmt->IsRecursiveWith(name)) {
      continue;
    }
    auto body_ctes = ctes;
    if (!outer_ctes.contains(AsciiLower(name))) {
      body_ctes.erase(AsciiLower(name));
    }
    changed |= RewriteCorrelatedPositiveCountRecursive(
        body.get(), std::move(body_ctes), ctx);
  }
  for (const SelectSource& src : stmt->Sources()) {
    if (src.query != nullptr && !src.is_lateral) {
      changed |=
          RewriteCorrelatedPositiveCountRecursive(src.query.get(), ctes, ctx);
    }
  }
  for (const auto& br : stmt->UnionAll()) {
    if (br != nullptr) {
      changed |= RewriteCorrelatedPositiveCountRecursive(br.get(), ctes, ctx);
    }
  }
  if (const auto& tree = stmt->GetSetOperationTree(); tree != nullptr) {
    if (tree->first != nullptr) {
      changed |=
          RewriteCorrelatedPositiveCountRecursive(tree->first.get(), ctes, ctx);
    }
    for (const auto& br : tree->branches) {
      if (br != nullptr) {
        changed |= RewriteCorrelatedPositiveCountRecursive(br.get(), ctes, ctx);
      }
    }
  }
  changed |= RewriteCorrelatedPositiveCountOneSelect(stmt, ctes, ctx);
  return changed;
}

}  // namespace

// Lean proof: lean/TinylambOpt/PredicateAndRelationalLowering.lean
//   (kleene_and_or_distrib_left_sound,
//    correlated_count_gt_zero_to_in_subquery_sound,
//    trap_correlated_count_eq_zero_not_in_null_outer_unsound)
bool RewriteCorrelatedPositiveCountToInSubquery(SelectStatement* select,
                                                TransactionContext& ctx) {
  return RewriteCorrelatedPositiveCountRecursive(select, {}, ctx);
}

namespace {

bool IsCountDistinctSimpleCol(const Expression& expr,
                              const std::vector<ResolvedBaseRel>& bases,
                              Expression* out_arg) {
  if (!expr || expr->Type() != TypeTag::kAggregateExp) {
    return false;
  }
  const auto& agg = expr->AsAggregateExpression();
  if (agg.GetType() != AggregationType::kCount || !agg.Distinct() ||
      agg.HasWhereFilter() || agg.NeedsGroupContext() || !agg.Child() ||
      agg.Child()->Type() != TypeTag::kColumnValue) {
    return false;
  }
  const ColumnName& cn = agg.Child()->AsColumnValue().GetColumnName();
  if (cn.name == "*") {
    return false;
  }
  const auto resolved = ResolveColRef(cn, bases);
  if (!resolved.has_value()) {
    return false;
  }
  const Column& col = bases[resolved->rel_idx].schema.GetColumn(resolved->slot);
  // Exclude DOUBLE (NaN breaks total order) and collated VARCHAR.
  if (col.Type() != ValueType::kInt64 && col.Type() != ValueType::kDate) {
    return false;
  }
  *out_arg = agg.Child();
  return true;
}

bool AsInt64ConstLiteral(const Expression& expr, int64_t* out) {
  if (!expr || expr->Type() != TypeTag::kConstantValue ||
      dynamic_cast<const ParameterSlot*>(expr.get()) != nullptr) {
    return false;
  }
  const Value& v = expr->AsConstantValue().GetValue();
  if (v.IsNull() || v.type != ValueType::kInt64 || v.IsUnsigned()) {
    return false;
  }
  *out = v.value.int_value;
  return true;
}

// Lean proof: lean/TinylambOpt/VectorizedJoinAndPruning.lean
//   (count_distinct_gt_one_iff_min_lt_max,
//    count_distinct_eq_one_iff_min_eq_max)
bool RewriteHavingCountDistinctOneSelect(
    SelectStatement* stmt, const std::unordered_set<std::string>& ctes,
    TransactionContext& ctx) {
  if (stmt == nullptr || stmt->Having() == nullptr || stmt->GroupBy().empty() ||
      stmt->Sources().empty()) {
    return false;
  }
  std::vector<ResolvedBaseRel> bases;
  if (!ResolvePlainBaseSources(*stmt, ctes, ctx, &bases)) {
    return false;
  }
  std::vector<Expression> conjs = SplitAndConjuncts(stmt->Having());
  bool changed = false;
  for (Expression& conj : conjs) {
    if (!conj || conj->Type() != TypeTag::kBinaryExp) {
      continue;
    }
    const auto& bin = conj->AsBinaryExpression();
    Expression arg;
    int64_t c = 0;
    BinaryOperation op = bin.Op();
    bool matched = false;
    if (IsCountDistinctSimpleCol(bin.Left(), bases, &arg) &&
        AsInt64ConstLiteral(bin.Right(), &c)) {
      matched = true;
    } else if (IsCountDistinctSimpleCol(bin.Right(), bases, &arg) &&
               AsInt64ConstLiteral(bin.Left(), &c)) {
      // Flip comparison so COUNT(DISTINCT x) is on the left.
      switch (op) {
        case BinaryOperation::kEquals:
          break;
        case BinaryOperation::kLessThan:
          op = BinaryOperation::kGreaterThan;
          break;
        case BinaryOperation::kLessThanEquals:
          op = BinaryOperation::kGreaterThanEquals;
          break;
        case BinaryOperation::kGreaterThan:
          op = BinaryOperation::kLessThan;
          break;
        case BinaryOperation::kGreaterThanEquals:
          op = BinaryOperation::kLessThanEquals;
          break;
        default:
          continue;
      }
      matched = true;
    }
    if (!matched) {
      continue;
    }
    const bool is_gt_one =
        (op == BinaryOperation::kGreaterThan && c == 1) ||
        (op == BinaryOperation::kGreaterThanEquals && c == 2);
    const bool is_eq_one = (op == BinaryOperation::kEquals && c == 1);
    if (!is_gt_one && !is_eq_one) {
      continue;
    }
    Expression min_e =
        AggregateExpressionExp(AggregationType::kMin, arg, false);
    Expression max_e =
        AggregateExpressionExp(AggregationType::kMax, arg, false);
    conj = BinaryExpressionExp(
        std::move(min_e),
        is_gt_one ? BinaryOperation::kLessThan : BinaryOperation::kEquals,
        std::move(max_e));
    changed = true;
  }
  if (changed) {
    stmt->SetHaving(CombineAndConjuncts(conjs));
  }
  return changed;
}

bool RewriteHavingCountDistinctRecursive(  // NOLINT(misc-no-recursion)
    SelectStatement* stmt, std::unordered_set<std::string> ctes,
    TransactionContext& ctx) {
  if (stmt == nullptr) {
    return false;
  }
  // Same shadowing rule as RewriteCorrelatedPositiveCountRecursive: keep the
  // name when an enclosing scope already provides it.
  const std::unordered_set<std::string> outer_ctes = ctes;
  CollectCteNamesInScope(*stmt, &ctes);
  bool changed = false;
  for (const auto& [name, body] : stmt->WithQueries()) {
    // A non-recursive CTE is not visible inside its own definition; only a
    // WITH RECURSIVE work table may refer to its own name.
    if (body == nullptr || stmt->IsRecursiveWith(name)) {
      continue;
    }
    auto body_ctes = ctes;
    if (!outer_ctes.contains(AsciiLower(name))) {
      body_ctes.erase(AsciiLower(name));
    }
    changed |= RewriteHavingCountDistinctRecursive(body.get(),
                                                   std::move(body_ctes), ctx);
  }
  for (const SelectSource& src : stmt->Sources()) {
    if (src.query != nullptr && !src.is_lateral) {
      changed |=
          RewriteHavingCountDistinctRecursive(src.query.get(), ctes, ctx);
    }
  }
  for (const auto& br : stmt->UnionAll()) {
    if (br != nullptr) {
      changed |= RewriteHavingCountDistinctRecursive(br.get(), ctes, ctx);
    }
  }
  if (const auto& tree = stmt->GetSetOperationTree(); tree != nullptr) {
    if (tree->first != nullptr) {
      changed |=
          RewriteHavingCountDistinctRecursive(tree->first.get(), ctes, ctx);
    }
    for (const auto& br : tree->branches) {
      if (br != nullptr) {
        changed |= RewriteHavingCountDistinctRecursive(br.get(), ctes, ctx);
      }
    }
  }
  changed |= RewriteHavingCountDistinctOneSelect(stmt, ctes, ctx);
  return changed;
}

}  // namespace

// Lean proof: lean/TinylambOpt/VectorizedJoinAndPruning.lean
//   (count_distinct_gt_one_iff_min_lt_max,
//    count_distinct_eq_one_iff_min_eq_max)
bool RewriteHavingCountDistinctToMinMax(SelectStatement* select,
                                        TransactionContext& ctx) {
  return RewriteHavingCountDistinctRecursive(select, {}, ctx);
}

namespace {

bool SamePlainBaseSourceList(const SelectStatement& a,
                             const SelectStatement& b) {
  if (a.Sources().size() != b.Sources().size() || a.Sources().empty()) {
    return false;
  }
  for (size_t i = 0; i < a.Sources().size(); ++i) {
    const SelectSource& sa = a.Sources()[i];
    const SelectSource& sb = b.Sources()[i];
    if (sa.query != nullptr || sb.query != nullptr || sa.unnest != nullptr ||
        sb.unnest != nullptr || sa.is_lateral || sb.is_lateral ||
        sa.table.empty() || sb.table.empty() || sa.sample.has_value() ||
        sb.sample.has_value() || !sa.using_columns.empty() ||
        !sb.using_columns.empty() || sa.from_nested_join ||
        sb.from_nested_join) {
      return false;
    }
    if ((sa.join_type != JoinType::kCross &&
         sa.join_type != JoinType::kInner) ||
        (sb.join_type != JoinType::kCross &&
         sb.join_type != JoinType::kInner)) {
      return false;
    }
    if (!IdentifierEqualsCI(sa.table, sb.table) ||
        !IdentifierEqualsCI(sa.alias, sb.alias)) {
      return false;
    }
    if ((sa.join_condition == nullptr) != (sb.join_condition == nullptr)) {
      return false;
    }
    if (sa.join_condition != nullptr &&
        !ExprAstEquals(sa.join_condition, sb.join_condition)) {
      return false;
    }
  }
  return true;
}

bool IsSimpleNonDistinctSum(const Expression& expr, Expression* out_child) {
  if (!expr || expr->Type() != TypeTag::kAggregateExp) {
    return false;
  }
  const auto& agg = expr->AsAggregateExpression();
  if (agg.GetType() != AggregationType::kSum || agg.Distinct() ||
      agg.HasWhereFilter() || agg.NeedsGroupContext() || !agg.Child()) {
    return false;
  }
  *out_child = agg.Child();
  return true;
}

// Replaces every `SUM(val)` (matching `target_val`) inside `expr` with
// `replacement`, rejecting any other AggregateExpression or ColumnValue.
bool ReplaceMatchingSumInScalarSubquery(  // NOLINT(misc-no-recursion)
    const Expression& expr, const Expression& target_val,
    const Expression& replacement, Expression* out, size_t* hit_count) {
  if (!expr) {
    return false;
  }
  switch (expr->Type()) {
    case TypeTag::kConstantValue:
      if (dynamic_cast<const ParameterSlot*>(expr.get()) != nullptr) {
        return false;
      }
      *out = expr;
      return true;
    case TypeTag::kAggregateExp: {
      Expression child;
      if (!IsSimpleNonDistinctSum(expr, &child) ||
          !ExprAstEquals(child, target_val)) {
        return false;
      }
      *out = replacement;
      ++(*hit_count);
      return true;
    }
    case TypeTag::kBinaryExp: {
      const auto& b = expr->AsBinaryExpression();
      Expression nl, nr;
      if (!ReplaceMatchingSumInScalarSubquery(b.Left(), target_val, replacement,
                                              &nl, hit_count) ||
          !ReplaceMatchingSumInScalarSubquery(b.Right(), target_val,
                                              replacement, &nr, hit_count)) {
        return false;
      }
      *out = BinaryExpressionExp(std::move(nl), b.Op(), std::move(nr));
      return true;
    }
    case TypeTag::kUnaryExp: {
      const auto& u = expr->AsUnaryExpression();
      Expression nc;
      if (!ReplaceMatchingSumInScalarSubquery(u.Child(), target_val,
                                              replacement, &nc, hit_count)) {
        return false;
      }
      *out = UnaryExpressionExp(std::move(nc), u.Op());
      return true;
    }
    default:
      return false;
  }
}

// Rewrites expressions in outer HAVING by replacing `SUM(target_val)` with
// `grp_sum_col` and the matched scalar subquery with `new_subquery`.
bool RewriteFubiniHavingExpr(  // NOLINT(misc-no-recursion)
    const Expression& expr, const Expression& target_val,
    const Expression& grp_sum_col, const SelectStatement* target_sub,
    const Expression& new_subquery, Expression* out, bool* sub_replaced) {
  if (!expr) {
    return false;
  }
  switch (expr->Type()) {
    case TypeTag::kConstantValue:
      *out = expr;
      return true;
    case TypeTag::kAggregateExp: {
      Expression child;
      if (!IsSimpleNonDistinctSum(expr, &child) ||
          !ExprAstEquals(child, target_val)) {
        return false;
      }
      *out = grp_sum_col;
      return true;
    }
    case TypeTag::kQueryExp: {
      const auto& sq = expr->AsQueryExpression();
      if (!sq.Exists() && !sq.Test() && sq.Query().get() == target_sub) {
        *out = new_subquery;
        *sub_replaced = true;
        return true;
      }
      return false;
    }
    case TypeTag::kBinaryExp: {
      const auto& b = expr->AsBinaryExpression();
      Expression nl, nr;
      if (!RewriteFubiniHavingExpr(b.Left(), target_val, grp_sum_col,
                                   target_sub, new_subquery, &nl,
                                   sub_replaced) ||
          !RewriteFubiniHavingExpr(b.Right(), target_val, grp_sum_col,
                                   target_sub, new_subquery, &nr,
                                   sub_replaced)) {
        return false;
      }
      *out = BinaryExpressionExp(std::move(nl), b.Op(), std::move(nr));
      return true;
    }
    case TypeTag::kUnaryExp: {
      const auto& u = expr->AsUnaryExpression();
      Expression nc;
      if (!RewriteFubiniHavingExpr(u.Child(), target_val, grp_sum_col,
                                   target_sub, new_subquery, &nc,
                                   sub_replaced)) {
        return false;
      }
      *out = UnaryExpressionExp(std::move(nc), u.Op());
      return true;
    }
    default:
      return false;
  }
}

void FindFirstScalarSubquery(  // NOLINT(misc-no-recursion)
    const Expression& expr, const SelectStatement** out) {
  if (!expr || *out != nullptr) {
    return;
  }
  switch (expr->Type()) {
    case TypeTag::kQueryExp: {
      const auto& sq = expr->AsQueryExpression();
      if (!sq.Exists() && !sq.Test() && sq.Query() != nullptr) {
        *out = sq.Query().get();
      }
      return;
    }
    case TypeTag::kBinaryExp:
      FindFirstScalarSubquery(expr->AsBinaryExpression().Left(), out);
      FindFirstScalarSubquery(expr->AsBinaryExpression().Right(), out);
      return;
    case TypeTag::kUnaryExp:
      FindFirstScalarSubquery(expr->AsUnaryExpression().Child(), out);
      return;
    default:
      return;
  }
}

// Rewrites ColumnValue references using a name map (for self-join CTE).
bool RemapColumnsInExpr(  // NOLINT(misc-no-recursion)
    const Expression& expr, const std::vector<ResolvedBaseRel>& rels,
    const std::string& cte_alias,
    const std::vector<std::pair<ResolvedColRef, std::string>>& col_map,
    Expression* out) {
  if (!expr) {
    *out = nullptr;
    return true;
  }
  switch (expr->Type()) {
    case TypeTag::kConstantValue:
      *out = expr;
      return true;
    case TypeTag::kColumnValue: {
      const auto ref =
          ResolveColRef(expr->AsColumnValue().GetColumnName(), rels);
      if (!ref.has_value()) {
        return false;
      }
      return std::ranges::any_of(col_map, [&](const auto& entry) {
        const auto& [k, mapped] = entry;
        if (k == *ref) {
          *out = ColumnValueExp(ColumnName(cte_alias, mapped));
          return true;
        }
        return false;
      });
    }
    case TypeTag::kBinaryExp: {
      const auto& b = expr->AsBinaryExpression();
      Expression nl, nr;
      if (!RemapColumnsInExpr(b.Left(), rels, cte_alias, col_map, &nl) ||
          !RemapColumnsInExpr(b.Right(), rels, cte_alias, col_map, &nr)) {
        return false;
      }
      *out = BinaryExpressionExp(std::move(nl), b.Op(), std::move(nr));
      return true;
    }
    case TypeTag::kUnaryExp: {
      const auto& u = expr->AsUnaryExpression();
      Expression nc;
      if (!RemapColumnsInExpr(u.Child(), rels, cte_alias, col_map, &nc)) {
        return false;
      }
      *out = UnaryExpressionExp(std::move(nc), u.Op());
      return true;
    }
    case TypeTag::kAggregateExp: {
      const auto& a = expr->AsAggregateExpression();
      if (a.HasWhereFilter() || a.NeedsGroupContext()) {
        return false;
      }
      Expression nc;
      if (a.Child() != nullptr &&
          !RemapColumnsInExpr(a.Child(), rels, cte_alias, col_map, &nc)) {
        return false;
      }
      *out = AggregateExpressionExp(a.GetType(), std::move(nc), a.Distinct());
      return true;
    }
    default:
      return false;
  }
}

void CollectResolvedColsInExpr(  // NOLINT(misc-no-recursion)
    const Expression& expr, const std::vector<ResolvedBaseRel>& rels,
    std::vector<ResolvedColRef>* out, bool* ok) {
  if (!expr || !*ok) {
    return;
  }
  switch (expr->Type()) {
    case TypeTag::kConstantValue:
      return;
    case TypeTag::kColumnValue: {
      const auto ref =
          ResolveColRef(expr->AsColumnValue().GetColumnName(), rels);
      if (!ref.has_value()) {
        *ok = false;
        return;
      }
      if (std::ranges::find(*out, *ref) == out->end()) {
        out->push_back(*ref);
      }
      return;
    }
    case TypeTag::kBinaryExp:
      CollectResolvedColsInExpr(expr->AsBinaryExpression().Left(), rels, out,
                                ok);
      CollectResolvedColsInExpr(expr->AsBinaryExpression().Right(), rels, out,
                                ok);
      return;
    case TypeTag::kUnaryExp:
      CollectResolvedColsInExpr(expr->AsUnaryExpression().Child(), rels, out,
                                ok);
      return;
    case TypeTag::kAggregateExp: {
      const auto& a = expr->AsAggregateExpression();
      if (a.HasWhereFilter() || a.NeedsGroupContext()) {
        *ok = false;
        return;
      }
      CollectResolvedColsInExpr(a.Child(), rels, out, ok);
      return;
    }
    default:
      *ok = false;
      return;
  }
}

bool RewriteSubqueryAggArgToCteCol(  // NOLINT(misc-no-recursion)
    const Expression& expr, const std::string& sub_alias,
    const std::string& sub_table, const std::string& cte_agg_col_name,
    slot_t* out_fact_slot, const Schema& fact_schema, Expression* out,
    size_t* agg_hits) {
  if (!expr) {
    return false;
  }
  switch (expr->Type()) {
    case TypeTag::kConstantValue:
      if (dynamic_cast<const ParameterSlot*>(expr.get()) != nullptr) {
        return false;
      }
      *out = expr;
      return true;
    case TypeTag::kAggregateExp: {
      const auto& a = expr->AsAggregateExpression();
      if ((a.GetType() != AggregationType::kAvg &&
           a.GetType() != AggregationType::kSum &&
           a.GetType() != AggregationType::kMin &&
           a.GetType() != AggregationType::kMax) ||
          a.Distinct() || a.HasWhereFilter() || a.NeedsGroupContext() ||
          !a.Child() || a.Child()->Type() != TypeTag::kColumnValue) {
        return false;
      }
      const ColumnName& cn = a.Child()->AsColumnValue().GetColumnName();
      if (!cn.schema.empty() && !IdentifierEqualsCI(cn.schema, sub_alias) &&
          !IdentifierEqualsCI(cn.schema, sub_table)) {
        return false;
      }
      const auto slot = FindColInSchema(fact_schema, cn.name);
      if (!slot.has_value()) {
        return false;
      }
      if (*agg_hits > 0 && *out_fact_slot != *slot) {
        return false;
      }
      *out_fact_slot = *slot;
      ++(*agg_hits);
      *out = AggregateExpressionExp(
          a.GetType(), ColumnValueExp(ColumnName(cte_agg_col_name)), false);
      return true;
    }
    case TypeTag::kBinaryExp: {
      const auto& b = expr->AsBinaryExpression();
      Expression nl, nr;
      if (!RewriteSubqueryAggArgToCteCol(b.Left(), sub_alias, sub_table,
                                         cte_agg_col_name, out_fact_slot,
                                         fact_schema, &nl, agg_hits) ||
          !RewriteSubqueryAggArgToCteCol(b.Right(), sub_alias, sub_table,
                                         cte_agg_col_name, out_fact_slot,
                                         fact_schema, &nr, agg_hits)) {
        return false;
      }
      *out = BinaryExpressionExp(std::move(nl), b.Op(), std::move(nr));
      return true;
    }
    case TypeTag::kUnaryExp: {
      const auto& u = expr->AsUnaryExpression();
      Expression nc;
      if (!RewriteSubqueryAggArgToCteCol(u.Child(), sub_alias, sub_table,
                                         cte_agg_col_name, out_fact_slot,
                                         fact_schema, &nc, agg_hits)) {
        return false;
      }
      *out = UnaryExpressionExp(std::move(nc), u.Op());
      return true;
    }
    default:
      return false;
  }
}

}  // namespace

// Lean proof: lean/TinylambOpt/JoinAndAggregatePushdown.lean
//   (grouped_having_global_sum_fubini_sound,
//    grouped_having_distinct_sum_fubini_bug)
bool RewriteHavingGlobalSumFubini(SelectStatement* stmt,
                                  TransactionContext& ctx) {
  if (stmt == nullptr || stmt->Having() == nullptr || stmt->GroupBy().empty() ||
      !stmt->WithQueries().empty() || stmt->Distinct() || stmt->HasLimit() ||
      stmt->Offset() != 0 || !stmt->UnionAll().empty() ||
      stmt->GetSetOperationTree() != nullptr || stmt->SelectList().empty() ||
      SelectListHasStarItem(*stmt)) {
    return false;
  }
  std::vector<ResolvedBaseRel> rels;
  if (!ResolvePlainBaseSources(*stmt, {}, ctx, &rels)) {
    return false;
  }
  const SelectStatement* sub = nullptr;
  FindFirstScalarSubquery(stmt->Having(), &sub);
  if (sub == nullptr || !sub->GroupBy().empty() || sub->Having() != nullptr ||
      sub->Distinct() || sub->HasLimit() || sub->Offset() != 0 ||
      !sub->WithQueries().empty() || !sub->UnionAll().empty() ||
      sub->GetSetOperationTree() != nullptr || sub->SelectList().size() != 1) {
    return false;
  }
  if (!SamePlainBaseSourceList(*stmt, *sub)) {
    return false;
  }
  if ((stmt->WhereClause() == nullptr) != (sub->WhereClause() == nullptr)) {
    return false;
  }
  if (stmt->WhereClause() != nullptr &&
      !ExprAstEquals(stmt->WhereClause(), sub->WhereClause())) {
    return false;
  }
  // If HAVING is already a direct `SUM(e) <cmp> (SELECT SUM(e) * factor ...)`
  // (where SUM(e) is in SELECT), executor/relational.cpp's single-pass
  // post_having.is_self_sum_factor computes the grand total and filters
  // groups inline without materializing a CTE.
  if (stmt->Having()->Type() == TypeTag::kBinaryExp) {
    const auto& hbin = stmt->Having()->AsBinaryExpression();
    if (IsComparison(hbin.Op()) && hbin.Left() && hbin.Right() &&
        hbin.Left()->Type() == TypeTag::kAggregateExp &&
        hbin.Right()->Type() == TypeTag::kQueryExp &&
        hbin.Right()->AsQueryExpression().Query().get() == sub) {
      const Expression& se = sub->SelectList()[0].expression;
      if (se && se->Type() == TypeTag::kBinaryExp &&
          se->AsBinaryExpression().Op() == BinaryOperation::kMultiply) {
        const auto& mbin = se->AsBinaryExpression();
        const bool is_sum_times_const =
            (mbin.Left() && mbin.Right() &&
             ((ExprAstEquals(mbin.Left(), hbin.Left()) &&
               mbin.Right()->Type() == TypeTag::kConstantValue) ||
              (ExprAstEquals(mbin.Right(), hbin.Left()) &&
               mbin.Left()->Type() == TypeTag::kConstantValue)));
        if (is_sum_times_const) {
          for (const auto& si : stmt->SelectList()) {
            if (si.expression && ExprAstEquals(si.expression, hbin.Left())) {
              return false;
            }
          }
        }
      }
    }
  }
  // Find a non-DISTINCT, non-filtered SUM(val) in stmt->SelectList() that also
  // appears in sub->SelectList()[0].
  // Generated CTE/output names must not collide with user aliases, table
  // names or output columns, or the outer rewrite binds to the wrong object.
  std::unordered_set<std::string> fubini_used_names;
  for (const SelectSource& s : stmt->Sources()) {
    if (!s.alias.empty()) {
      fubini_used_names.insert(AsciiLower(s.alias));
    }
    if (!s.table.empty()) {
      fubini_used_names.insert(AsciiLower(s.table));
    }
  }
  for (size_t k = 0; k < stmt->SelectList().size(); ++k) {
    const std::string nm = OutputColumnNameAt(*stmt, k);
    if (!nm.empty()) {
      fubini_used_names.insert(AsciiLower(nm));
    }
  }
  auto fubini_fresh = [&](const std::string& base) {
    if (fubini_used_names.insert(AsciiLower(base)).second) {
      return base;
    }
    for (size_t seq = 1;; ++seq) {
      const std::string cand = base + "_" + std::to_string(seq);
      if (fubini_used_names.insert(AsciiLower(cand)).second) {
        return cand;
      }
    }
  };
  const std::string cte_name = fubini_fresh("__tpch_fubini_grp");
  const std::string total_name = fubini_fresh("__fubini_total");
  for (size_t i = 0; i < stmt->SelectList().size(); ++i) {
    Expression sum_val;
    if (!IsSimpleNonDistinctSum(stmt->SelectList()[i].expression, &sum_val)) {
      continue;
    }
    const std::string sum_col_name = OutputColumnNameAt(*stmt, i);
    if (sum_col_name.empty()) {
      continue;
    }
    // Check all select list items have unique non-empty output names.
    std::vector<std::string> out_names;
    out_names.reserve(stmt->SelectList().size());
    std::unordered_set<std::string> seen;
    bool names_ok = true;
    for (size_t j = 0; j < stmt->SelectList().size(); ++j) {
      const std::string nm = OutputColumnNameAt(*stmt, j);
      if (nm.empty() || !seen.insert(AsciiLower(nm)).second) {
        names_ok = false;
        break;
      }
      out_names.push_back(nm);
    }
    if (!names_ok) {
      return false;
    }
    // Verify sub->SelectList()[0] is a pure function of SUM(sum_val).
    Expression rewritten_sub_expr;
    size_t hits = 0;
    const Expression cte_sum_agg = AggregateExpressionExp(
        AggregationType::kSum,
        ColumnValueExp(ColumnName(cte_name, sum_col_name)), false);
    if (!ReplaceMatchingSumInScalarSubquery(sub->SelectList()[0].expression,
                                            sum_val, cte_sum_agg,
                                            &rewritten_sub_expr, &hits) ||
        hits == 0) {
      continue;
    }
    auto new_sub_stmt = std::make_shared<SelectStatement>();
    new_sub_stmt->SetSources(
        {SelectSource(cte_name, "", nullptr, JoinType::kCross, nullptr)});
    new_sub_stmt->SetSelectList(
        {NamedExpression(total_name, std::move(rewritten_sub_expr))});
    const Expression new_sub_expr = std::make_shared<QueryExpression>(
        std::move(new_sub_stmt), nullptr, false, false);

    Expression new_where;
    bool sub_replaced = false;
    const Expression grp_col_ref =
        ColumnValueExp(ColumnName(cte_name, sum_col_name));
    if (!RewriteFubiniHavingExpr(stmt->Having(), sum_val, grp_col_ref, sub,
                                 new_sub_expr, &new_where, &sub_replaced) ||
        !sub_replaced) {
      continue;
    }

    // Build CTE body = original grouped query without HAVING and ORDER BY.
    auto cte_body = std::make_shared<SelectStatement>(*stmt);
    cte_body->SetHaving(nullptr);
    cte_body->SetOrderBy({});

    SelectStatement outer;
    outer.AddWithQuery(cte_name, std::move(cte_body));
    outer.SetSources(
        {SelectSource(cte_name, "", nullptr, JoinType::kCross, nullptr)});
    std::vector<NamedExpression> outer_sel;
    outer_sel.reserve(out_names.size());
    for (const std::string& nm : out_names) {
      outer_sel.emplace_back(nm, ColumnValueExp(ColumnName(cte_name, nm)));
    }
    outer.SetSelectList(std::move(outer_sel));
    outer.SetWhereClause(std::move(new_where));
    outer.SetOrderBy(stmt->OrderBy());
    *stmt = std::move(outer);
    return true;
  }
  return false;
}

// Lean proof: lean/TinylambOpt/VectorizedJoinAndPruning.lean
//   (self_join_unique_dim_cte_decorrelate_sound)
bool RewriteSelfJoinUniqueDimCorrelatedAgg(SelectStatement* stmt,
                                           TransactionContext& ctx) {
  if (stmt == nullptr || stmt->Sources().size() != 2 ||
      !stmt->WithQueries().empty() || stmt->WhereClause() == nullptr ||
      !stmt->GroupBy().empty() || stmt->Having() != nullptr ||
      stmt->Distinct() || stmt->HasLimit() || stmt->Offset() != 0 ||
      !stmt->UnionAll().empty() || stmt->GetSetOperationTree() != nullptr ||
      stmt->SelectList().empty() || SelectListHasStarItem(*stmt)) {
    return false;
  }
  for (const SelectSource& s : stmt->Sources()) {
    if (s.join_type != JoinType::kCross && s.join_type != JoinType::kInner) {
      return false;
    }
    if (s.join_condition != nullptr) {
      return false;
    }
  }
  std::vector<ResolvedBaseRel> rels;
  if (!ResolvePlainBaseSources(*stmt, {}, ctx, &rels) || rels.size() != 2) {
    return false;
  }
  const std::vector<Expression> conjs = SplitAndConjuncts(stmt->WhereClause());

  // Locate the single correlated scalar subquery comparison conjunct.
  std::optional<size_t> sub_conj_idx;
  for (size_t ci = 0; ci < conjs.size(); ++ci) {
    if (!conjs[ci] || conjs[ci]->Type() != TypeTag::kBinaryExp) {
      continue;
    }
    const auto& bin = conjs[ci]->AsBinaryExpression();
    const bool left_sq = bin.Left() && bin.Left()->Type() == TypeTag::kQueryExp;
    const bool right_sq =
        bin.Right() && bin.Right()->Type() == TypeTag::kQueryExp;
    if (left_sq != right_sq) {
      if (sub_conj_idx.has_value()) {
        return false;
      }
      sub_conj_idx = ci;
    }
  }
  if (!sub_conj_idx.has_value()) {
    return false;
  }

  const auto& sub_bin = conjs[*sub_conj_idx]->AsBinaryExpression();
  if (!IsStrictNullRejectingCompareOp(sub_bin.Op())) {
    return false;
  }
  const bool sq_on_right = sub_bin.Right()->Type() == TypeTag::kQueryExp;
  const Expression& lhs_expr = sq_on_right ? sub_bin.Left() : sub_bin.Right();
  const auto& sq_node =
      (sq_on_right ? sub_bin.Right() : sub_bin.Left())->AsQueryExpression();
  if (sq_node.Exists() || sq_node.Test() || sq_node.Query() == nullptr) {
    return false;
  }
  const SelectStatement& sub = *sq_node.Query();
  if (sub.Sources().size() != 1 || sub.Sources()[0].query != nullptr ||
      sub.Sources()[0].table.empty() || !sub.GroupBy().empty() ||
      sub.Having() != nullptr || sub.Distinct() || sub.HasLimit() ||
      sub.Offset() != 0 || !sub.WithQueries().empty() ||
      !sub.UnionAll().empty() || sub.GetSetOperationTree() != nullptr ||
      sub.SelectList().size() != 1 || sub.WhereClause() == nullptr) {
    return false;
  }

  // Determine which outer relation is the fact table `F` (same table as sub)
  // and which is the unique dimension `D`.
  size_t fact_idx = 2;
  for (size_t i = 0; i < 2; ++i) {
    if (IdentifierEqualsCI(rels[i].table_name, sub.Sources()[0].table)) {
      fact_idx = i;
      break;
    }
  }
  if (fact_idx >= 2) {
    return false;
  }
  const size_t dim_idx = 1 - fact_idx;

  // Verify outer equijoin F.fk = D.pk and D.pk is certified UNIQUE NOT NULL,
  // and no other predicates touch F (all other conjuncts touch only D).
  std::optional<slot_t> fact_fk_slot;
  std::optional<slot_t> dim_pk_slot;
  std::vector<Expression> base_where_conjs;
  for (size_t ci = 0; ci < conjs.size(); ++ci) {
    if (ci == *sub_conj_idx) {
      continue;
    }
    const Expression& c = conjs[ci];
    std::unordered_set<size_t> touched;
    if (!CollectResolvedExprRelations(c, rels, &touched)) {
      return false;
    }
    if (touched.size() == 1 && touched.contains(dim_idx)) {
      base_where_conjs.push_back(c);
      continue;
    }
    if (touched.size() == 2 && c->Type() == TypeTag::kBinaryExp &&
        c->AsBinaryExpression().Op() == BinaryOperation::kEquals &&
        c->AsBinaryExpression().Left()->Type() == TypeTag::kColumnValue &&
        c->AsBinaryExpression().Right()->Type() == TypeTag::kColumnValue) {
      const auto r1 = ResolveColRef(
          c->AsBinaryExpression().Left()->AsColumnValue().GetColumnName(),
          rels);
      const auto r2 = ResolveColRef(
          c->AsBinaryExpression().Right()->AsColumnValue().GetColumnName(),
          rels);
      if (!r1.has_value() || !r2.has_value()) {
        return false;
      }
      if (r1->rel_idx == fact_idx && r2->rel_idx == dim_idx) {
        fact_fk_slot = r1->slot;
        dim_pk_slot = r2->slot;
        base_where_conjs.push_back(c);
        continue;
      }
      if (r2->rel_idx == fact_idx && r1->rel_idx == dim_idx) {
        fact_fk_slot = r2->slot;
        dim_pk_slot = r1->slot;
        base_where_conjs.push_back(c);
        continue;
      }
    }
    // Any other predicate touching F disqualifies (would filter B vs F2).
    return false;
  }
  if (!fact_fk_slot.has_value() || !dim_pk_slot.has_value()) {
    return false;
  }
  if (!IsColumnCertifiedUniqueNotNull(*rels[dim_idx].table, *dim_pk_slot,
                                      ctx)) {
    return false;
  }

  // Verify subquery WHERE is ONLY `F2.fk = D.pk` (matching fact_fk_slot &
  // dim_pk_slot).
  const std::vector<Expression> sub_conjs =
      SplitAndConjuncts(sub.WhereClause());
  if (sub_conjs.size() != 1 || !sub_conjs[0] ||
      sub_conjs[0]->Type() != TypeTag::kBinaryExp ||
      sub_conjs[0]->AsBinaryExpression().Op() != BinaryOperation::kEquals ||
      sub_conjs[0]->AsBinaryExpression().Left()->Type() !=
          TypeTag::kColumnValue ||
      sub_conjs[0]->AsBinaryExpression().Right()->Type() !=
          TypeTag::kColumnValue) {
    return false;
  }
  const ColumnName& s_l = sub_conjs[0]
                              ->AsBinaryExpression()
                              .Left()
                              ->AsColumnValue()
                              .GetColumnName();
  const ColumnName& s_r = sub_conjs[0]
                              ->AsBinaryExpression()
                              .Right()
                              ->AsColumnValue()
                              .GetColumnName();
  const std::string sub_alias = sub.Sources()[0].alias.empty()
                                    ? sub.Sources()[0].table
                                    : sub.Sources()[0].alias;
  auto matches_sub_fk = [&](const ColumnName& cn) {
    if (!cn.schema.empty() && !IdentifierEqualsCI(cn.schema, sub_alias) &&
        !IdentifierEqualsCI(cn.schema, sub.Sources()[0].table)) {
      return false;
    }
    const auto s = FindColInSchema(rels[fact_idx].schema, cn.name);
    return s.has_value() && *s == *fact_fk_slot;
  };
  auto matches_outer_pk = [&](const ColumnName& cn) {
    const auto r = ResolveColRef(cn, rels);
    return r.has_value() && r->rel_idx == dim_idx && r->slot == *dim_pk_slot;
  };
  if (!((matches_sub_fk(s_l) && matches_outer_pk(s_r)) ||
        (matches_sub_fk(s_r) && matches_outer_pk(s_l)))) {
    return false;
  }

  // All generated names live in the same namespace as user aliases, table
  // names and column names; mint collision-free spellings up front.
  std::unordered_set<std::string> self_used_names;
  for (const SelectSource& s : stmt->Sources()) {
    if (!s.alias.empty()) {
      self_used_names.insert(AsciiLower(s.alias));
    }
    if (!s.table.empty()) {
      self_used_names.insert(AsciiLower(s.table));
    }
  }
  for (const SelectSource& s : sub.Sources()) {
    if (!s.alias.empty()) {
      self_used_names.insert(AsciiLower(s.alias));
    }
    if (!s.table.empty()) {
      self_used_names.insert(AsciiLower(s.table));
    }
  }
  for (const auto& item : stmt->SelectList()) {
    if (!item.name.empty()) {
      self_used_names.insert(AsciiLower(item.name));
    }
  }
  for (const auto& item : sub.SelectList()) {
    if (!item.name.empty()) {
      self_used_names.insert(AsciiLower(item.name));
    }
  }
  for (const auto& r : rels) {
    for (size_t c = 0; c < r.schema.ColumnCount(); ++c) {
      self_used_names.insert(AsciiLower(r.schema.GetColumn(c).Name().name));
    }
  }
  auto self_fresh = [&](const std::string& base) {
    if (self_used_names.insert(AsciiLower(base)).second) {
      return base;
    }
    for (size_t seq = 1;; ++seq) {
      const std::string cand = base + "_" + std::to_string(seq);
      if (self_used_names.insert(AsciiLower(cand)).second) {
        return cand;
      }
    }
  };

  // Rewrite subquery SELECT expression to read from CTE column `__c_agg`.
  slot_t agg_fact_slot = 0;
  size_t agg_hits = 0;
  Expression rewritten_agg_expr;
  const std::string cte_agg_col = self_fresh("__self_agg_arg");
  if (!RewriteSubqueryAggArgToCteCol(sub.SelectList()[0].expression, sub_alias,
                                     sub.Sources()[0].table, cte_agg_col,
                                     &agg_fact_slot, rels[fact_idx].schema,
                                     &rewritten_agg_expr, &agg_hits) ||
      agg_hits == 0) {
    return false;
  }

  // Collect all outer columns needed in the CTE.
  std::vector<ResolvedColRef> needed_cols;
  needed_cols.reserve(stmt->SelectList().size() + 2);
  needed_cols.push_back(
      ResolvedColRef{.rel_idx = fact_idx, .slot = *fact_fk_slot});
  const ResolvedColRef agg_ref{.rel_idx = fact_idx, .slot = agg_fact_slot};
  if (std::ranges::find(needed_cols, agg_ref) == needed_cols.end()) {
    needed_cols.push_back(agg_ref);
  }
  bool cols_ok = true;
  CollectResolvedColsInExpr(lhs_expr, rels, &needed_cols, &cols_ok);
  for (const NamedExpression& item : stmt->SelectList()) {
    CollectResolvedColsInExpr(item.expression, rels, &needed_cols, &cols_ok);
  }
  if (!cols_ok) {
    return false;
  }

  // Build CTE projection & column name map.
  const std::string cte_name = self_fresh("__tpch_self_base");
  const std::string base_alias = self_fresh("__b");
  const std::string grp_alias = self_fresh("__g");
  const std::string fk_col_name = self_fresh("__self_fk");
  const std::string agg_out_name = self_fresh("__self_agg_val");

  std::vector<std::pair<ResolvedColRef, std::string>> col_map;
  std::vector<NamedExpression> cte_sel;
  for (size_t i = 0; i < needed_cols.size(); ++i) {
    const ResolvedColRef& ref = needed_cols[i];
    std::string mapped;
    if (ref.rel_idx == fact_idx && ref.slot == *fact_fk_slot) {
      mapped = fk_col_name;
    } else if (ref.rel_idx == fact_idx && ref.slot == agg_fact_slot) {
      mapped = cte_agg_col;
    } else {
      mapped = self_fresh("__self_c" + std::to_string(i));
    }
    col_map.emplace_back(ref, mapped);
    const std::string orig_col =
        rels[ref.rel_idx].schema.GetColumn(ref.slot).Name().name;
    cte_sel.emplace_back(mapped, ColumnValueExp(ColumnName(
                                     rels[ref.rel_idx].qualifier, orig_col)));
  }
  // If fact_fk_slot == agg_fact_slot, add a second alias for cte_agg_col.
  if (*fact_fk_slot == agg_fact_slot) {
    const std::string orig_col =
        rels[fact_idx].schema.GetColumn(*fact_fk_slot).Name().name;
    cte_sel.emplace_back(cte_agg_col, ColumnValueExp(ColumnName(
                                          rels[fact_idx].qualifier, orig_col)));
  }

  auto cte_body = std::make_shared<SelectStatement>();
  cte_body->SetSources(stmt->Sources());
  cte_body->SetSelectList(std::move(cte_sel));
  cte_body->SetWhereClause(CombineAndConjuncts(base_where_conjs));

  // Build grouped derived table over CTE.
  auto grp_query = std::make_shared<SelectStatement>();
  grp_query->SetSources(
      {SelectSource(cte_name, "", nullptr, JoinType::kCross, nullptr)});
  grp_query->SetSelectList(
      {NamedExpression(fk_col_name, ColumnValueExp(ColumnName(fk_col_name))),
       NamedExpression(agg_out_name, std::move(rewritten_agg_expr))});
  grp_query->SetGroupBy({ColumnValueExp(ColumnName(fk_col_name))});

  // Remap outer lhs_expr & SELECT list to `__b.<mapped>`.
  Expression new_lhs;
  if (!RemapColumnsInExpr(lhs_expr, rels, base_alias, col_map, &new_lhs)) {
    return false;
  }
  std::vector<NamedExpression> new_outer_sel;
  new_outer_sel.reserve(stmt->SelectList().size());
  for (const NamedExpression& item : stmt->SelectList()) {
    Expression ne;
    if (!RemapColumnsInExpr(item.expression, rels, base_alias, col_map, &ne)) {
      return false;
    }
    new_outer_sel.emplace_back(item.name, std::move(ne));
  }

  const Expression grp_val_ref =
      ColumnValueExp(ColumnName(grp_alias, agg_out_name));
  const Expression cmp_expr =
      sq_on_right
          ? BinaryExpressionExp(std::move(new_lhs), sub_bin.Op(), grp_val_ref)
          : BinaryExpressionExp(grp_val_ref, sub_bin.Op(), std::move(new_lhs));
  const Expression join_eq =
      BinaryExpressionExp(ColumnValueExp(ColumnName(base_alias, fk_col_name)),
                          BinaryOperation::kEquals,
                          ColumnValueExp(ColumnName(grp_alias, fk_col_name)));

  SelectStatement outer;
  outer.AddWithQuery(cte_name, std::move(cte_body));
  outer.SetSources(
      {SelectSource(cte_name, base_alias, nullptr, JoinType::kCross, nullptr),
       SelectSource("", grp_alias, std::move(grp_query), JoinType::kInner,
                    join_eq)});
  outer.SetSelectList(std::move(new_outer_sel));
  outer.SetWhereClause(cmp_expr);
  outer.SetOrderBy(stmt->OrderBy());
  *stmt = std::move(outer);
  return true;
}

// Lean proof: lean/TinylambOpt/VectorizedJoinAndPruning.lean
//   (semijoin_domain_pushdown_in_subquery_sound)
bool RewritePushOuterSemiJoinDomainIntoInSubquery(SelectStatement* stmt,
                                                  TransactionContext& ctx) {
  if (stmt == nullptr || stmt->WhereClause() == nullptr ||
      !stmt->WithQueries().empty() || !stmt->GroupBy().empty() ||
      stmt->Having() != nullptr || stmt->Distinct() || stmt->HasLimit() ||
      stmt->Offset() != 0 || !stmt->UnionAll().empty() ||
      stmt->GetSetOperationTree() != nullptr || stmt->SelectList().empty() ||
      SelectListHasStarItem(*stmt)) {
    return false;
  }
  std::vector<ResolvedBaseRel> rels;
  if (!ResolvePlainBaseSources(*stmt, {}, ctx, &rels)) {
    return false;
  }
  std::vector<Expression> conjs = SplitAndConjuncts(stmt->WhereClause());
  if (conjs.size() < 2) {
    return false;
  }
  size_t in_idx = conjs.size();
  for (size_t i = 0; i < conjs.size(); ++i) {
    const Expression& c = conjs[i];
    if (!c || c->Type() != TypeTag::kQueryExp) {
      continue;
    }
    const auto& qe = c->AsQueryExpression();
    if (!qe.Test() || qe.Exists() || qe.Negated() || qe.ArrayResult() ||
        qe.Mode() != QuantifierMode::kIn || qe.Query() == nullptr) {
      continue;
    }
    const SelectStatement& sub = *qe.Query();
    if (sub.SelectList().size() != 1 || !sub.SelectList()[0].expression ||
        sub.SelectList()[0].expression->Type() != TypeTag::kColumnValue ||
        !sub.GroupBy().empty() || sub.Having() != nullptr || sub.HasLimit() ||
        sub.Offset() != 0 || !sub.WithQueries().empty() ||
        !sub.UnionAll().empty() || sub.GetSetOperationTree() != nullptr) {
      continue;
    }
    const bool heavy_sub =
        sub.Sources().size() >= 2 ||
        (sub.WhereClause() != nullptr &&
         ExprHasParamOrSubqueryOrVolatile(sub.WhereClause()));
    if (!heavy_sub) {
      continue;
    }
    if (in_idx != conjs.size()) {
      return false;
    }
    in_idx = i;
  }
  if (in_idx == conjs.size()) {
    return false;
  }

  std::vector<Expression> other_conjs;
  other_conjs.reserve(conjs.size() - 1);
  for (size_t i = 0; i < conjs.size(); ++i) {
    if (i == in_idx) {
      continue;
    }
    if (!conjs[i] || ExprHasParamOrSubqueryOrVolatile(conjs[i])) {
      return false;
    }
    other_conjs.push_back(conjs[i]);
  }
  if (other_conjs.empty()) {
    return false;
  }

  const auto& qe = conjs[in_idx]->AsQueryExpression();
  if (qe.Test()->Type() != TypeTag::kColumnValue) {
    return false;
  }
  const auto key_ref =
      ResolveColRef(qe.Test()->AsColumnValue().GetColumnName(), rels);
  if (!key_ref.has_value()) {
    return false;
  }

  std::vector<ResolvedColRef> needed_cols;
  needed_cols.push_back(*key_ref);
  bool cols_ok = true;
  for (const NamedExpression& item : stmt->SelectList()) {
    CollectResolvedColsInExpr(item.expression, rels, &needed_cols, &cols_ok);
  }
  std::unordered_set<std::string> select_aliases;
  for (size_t i = 0; i < stmt->SelectList().size(); ++i) {
    const std::string out_nm = OutputColumnNameAt(*stmt, i);
    if (!out_nm.empty()) {
      select_aliases.insert(AsciiLower(out_nm));
    }
  }
  for (const auto& term : stmt->OrderBy()) {
    if (!term.expression) {
      return false;
    }
    if (term.expression->Type() == TypeTag::kColumnValue) {
      const ColumnName& cn = term.expression->AsColumnValue().GetColumnName();
      if (cn.schema.empty() && select_aliases.contains(AsciiLower(cn.name))) {
        continue;
      }
    }
    CollectResolvedColsInExpr(term.expression, rels, &needed_cols, &cols_ok);
  }
  if (!cols_ok) {
    return false;
  }

  // The injected Domain CTE, alias and column names share the user query's
  // namespace; generate non-colliding spellings.
  std::unordered_set<std::string> dom_used_names;
  for (const auto& r : rels) {
    dom_used_names.insert(AsciiLower(r.qualifier));
    dom_used_names.insert(AsciiLower(r.table_name));
    for (size_t c = 0; c < r.schema.ColumnCount(); ++c) {
      dom_used_names.insert(AsciiLower(r.schema.GetColumn(c).Name().name));
    }
  }
  for (const auto& n : select_aliases) {
    dom_used_names.insert(n);
  }
  auto dom_fresh = [&](const std::string& base) {
    if (dom_used_names.insert(AsciiLower(base)).second) {
      return base;
    }
    for (size_t seq = 1;; ++seq) {
      const std::string cand = base + "_" + std::to_string(seq);
      if (dom_used_names.insert(AsciiLower(cand)).second) {
        return cand;
      }
    }
  };
  const std::string cte_name = dom_fresh("__tpch_outer_dom");
  const std::string dom_alias = dom_fresh("__d");
  const std::string dom_key_col = dom_fresh("__dom_key");

  std::vector<std::pair<ResolvedColRef, std::string>> col_map;
  std::vector<NamedExpression> cte_sel;
  for (size_t i = 0; i < needed_cols.size(); ++i) {
    const ResolvedColRef& ref = needed_cols[i];
    const std::string mapped = (ref == *key_ref)
                                   ? dom_key_col
                                   : dom_fresh("__dom_c" + std::to_string(i));
    col_map.emplace_back(ref, mapped);
    const std::string orig_col =
        rels[ref.rel_idx].schema.GetColumn(ref.slot).Name().name;
    cte_sel.emplace_back(mapped, ColumnValueExp(ColumnName(
                                     rels[ref.rel_idx].qualifier, orig_col)));
  }

  auto cte_body = std::make_shared<SelectStatement>();
  cte_body->SetSources(stmt->Sources());
  cte_body->SetSelectList(std::move(cte_sel));
  cte_body->SetWhereClause(CombineAndConjuncts(other_conjs));

  auto make_dom_in = [&](Expression target_col) -> Expression {
    auto dom_q = std::make_shared<SelectStatement>();
    dom_q->SetSources(
        {SelectSource(cte_name, "", nullptr, JoinType::kCross, nullptr)});
    dom_q->SetSelectList(
        {NamedExpression("", ColumnValueExp(ColumnName(dom_key_col)))});
    return QueryExpressionExp(std::move(dom_q), std::move(target_col),
                              /*exists=*/false, /*negated=*/false);
  };

  auto new_sub = std::make_shared<SelectStatement>(*qe.Query());
  const Expression r_key_expr = new_sub->SelectList()[0].expression;

  std::vector<Expression> sub_conjs;
  if (new_sub->WhereClause() != nullptr) {
    sub_conjs = SplitAndConjuncts(new_sub->WhereClause());
  }
  sub_conjs.push_back(make_dom_in(r_key_expr));
  new_sub->SetWhereClause(CombineAndConjuncts(sub_conjs));

  std::vector<NamedExpression> new_outer_sel;
  new_outer_sel.reserve(stmt->SelectList().size());
  for (size_t i = 0; i < stmt->SelectList().size(); ++i) {
    const NamedExpression& item = stmt->SelectList()[i];
    Expression ne;
    if (!RemapColumnsInExpr(item.expression, rels, dom_alias, col_map, &ne)) {
      return false;
    }
    std::string out_alias = item.name;
    if (out_alias.empty()) {
      out_alias = OutputColumnNameAt(*stmt, i);
    }
    new_outer_sel.emplace_back(std::move(out_alias), std::move(ne));
  }
  std::vector<SelectStatement::OrderByTerm> new_order_by;
  new_order_by.reserve(stmt->OrderBy().size());
  for (const auto& term : stmt->OrderBy()) {
    if (term.expression->Type() == TypeTag::kColumnValue) {
      const ColumnName& cn = term.expression->AsColumnValue().GetColumnName();
      if (cn.schema.empty() && select_aliases.contains(AsciiLower(cn.name))) {
        new_order_by.push_back(term);
        continue;
      }
    }
    Expression ne;
    if (!RemapColumnsInExpr(term.expression, rels, dom_alias, col_map, &ne)) {
      return false;
    }
    SelectStatement::OrderByTerm nt = term;
    nt.expression = std::move(ne);
    new_order_by.push_back(std::move(nt));
  }

  SelectStatement outer;
  outer.AddWithQuery(cte_name, std::move(cte_body));
  outer.SetSources(
      {SelectSource(cte_name, dom_alias, nullptr, JoinType::kCross, nullptr)});
  outer.SetSelectList(std::move(new_outer_sel));
  outer.SetWhereClause(QueryExpressionExp(
      std::move(new_sub), ColumnValueExp(ColumnName(dom_alias, dom_key_col)),
      /*exists=*/false, /*negated=*/false));
  outer.SetOrderBy(std::move(new_order_by));
  *stmt = std::move(outer);
  return true;
}

// ============================================================================
// TPC-DS Q78: Disjunctive Left-Join Key-Domain Intersection Pushdown into
// Grouped CTEs
// Lean proof: lean/TinylambOpt/VectorizedJoinAndPruning.lean
//   (disjunctive_left_join_semijoin_pushdown_sound)
// ============================================================================

bool RewriteDisjunctiveLeftJoinCteKeyDomain(SelectStatement* stmt,
                                            TransactionContext& ctx) {
  if (stmt == nullptr || stmt->WithQueries().size() < 3 ||
      stmt->Sources().size() != 3 || !stmt->WhereClause() ||
      !stmt->UnionAll().empty() || stmt->GetSetOperationTree() != nullptr ||
      stmt->ForUpdate()) {
    return false;
  }
  const std::string active_cte_name = "__lj_active_keys";
  for (const auto& [name, _] : stmt->WithQueries()) {
    if (stmt->IsRecursiveWith(name) ||
        IdentifierEqualsCI(name, active_cte_name)) {
      return false;
    }
  }
  const SelectSource& src_l = stmt->Sources()[0];
  const SelectSource& src_r1 = stmt->Sources()[1];
  const SelectSource& src_r2 = stmt->Sources()[2];
  if (src_l.query != nullptr || src_r1.query != nullptr ||
      src_r2.query != nullptr || src_l.unnest != nullptr ||
      src_r1.unnest != nullptr || src_r2.unnest != nullptr ||
      src_l.sample.has_value() || src_r1.sample.has_value() ||
      src_r2.sample.has_value() || src_r1.join_type != JoinType::kLeft ||
      src_r2.join_type != JoinType::kLeft || !src_r1.join_condition ||
      !src_r2.join_condition || !src_r1.using_columns.empty() ||
      !src_r2.using_columns.empty()) {
    return false;
  }
  auto find_cte_key = [&](const std::string& tbl) -> std::string {
    for (const auto& [k, v] : stmt->WithQueries()) {
      if (v != nullptr && IdentifierEqualsCI(k, tbl)) {
        return k;
      }
    }
    return {};
  };
  const std::string cte_l_name = find_cte_key(src_l.table);
  const std::string cte_r1_name = find_cte_key(src_r1.table);
  const std::string cte_r2_name = find_cte_key(src_r2.table);
  if (cte_l_name.empty() || cte_r1_name.empty() || cte_r2_name.empty() ||
      IdentifierEqualsCI(cte_l_name, cte_r1_name) ||
      IdentifierEqualsCI(cte_l_name, cte_r2_name) ||
      IdentifierEqualsCI(cte_r1_name, cte_r2_name)) {
    return false;
  }
  auto cte_used_in = [&](const SelectStatement& s, const std::string& target) {
    return std::ranges::any_of(s.Sources(), [&](const SelectSource& src) {
      return src.query == nullptr && IdentifierEqualsCI(src.table, target);
    });
  };
  for (const auto& [name, body] : stmt->WithQueries()) {
    if (body == nullptr) {
      continue;
    }
    if (cte_used_in(*body, cte_l_name) || cte_used_in(*body, cte_r1_name) ||
        cte_used_in(*body, cte_r2_name)) {
      return false;
    }
  }
  const std::shared_ptr<SelectStatement>& cte_l =
      stmt->WithQueries().at(cte_l_name);
  const std::shared_ptr<SelectStatement>& cte_r1 =
      stmt->WithQueries().at(cte_r1_name);
  const std::shared_ptr<SelectStatement>& cte_r2 =
      stmt->WithQueries().at(cte_r2_name);
  const std::string qual_l = src_l.alias.empty() ? src_l.table : src_l.alias;
  const std::string qual_r1 =
      src_r1.alias.empty() ? src_r1.table : src_r1.alias;
  const std::string qual_r2 =
      src_r2.alias.empty() ? src_r2.table : src_r2.alias;

  auto collect_cte_out_cols =
      [](const SelectStatement& cte,
         std::unordered_map<std::string, size_t>* out) -> bool {
    if (cte.SelectList().empty() || cte.GroupBy().empty() ||
        cte.Having() != nullptr || cte.Distinct() || cte.HasDistinctOn() ||
        cte.HasLimit() || cte.Offset() != 0 || !cte.UnionAll().empty() ||
        cte.GetSetOperationTree() != nullptr || cte.Sources().size() < 2 ||
        SelectListHasStarItem(cte)) {
      return false;
    }
    for (size_t i = 0; i < cte.SelectList().size(); ++i) {
      const std::string cname = OutputColumnNameAt(cte, i);
      if (cname.empty() || !out->emplace(AsciiLower(cname), i).second) {
        return false;
      }
    }
    return true;
  };
  std::unordered_map<std::string, size_t> out_l, out_r1, out_r2;
  if (!collect_cte_out_cols(*cte_l, &out_l) ||
      !collect_cte_out_cols(*cte_r1, &out_r1) ||
      !collect_cte_out_cols(*cte_r2, &out_r2)) {
    return false;
  }

  auto resolve_outer_col = [&](const ColumnName& cn, int* rel_out,
                               size_t* slot_out) -> bool {
    const std::string lc = AsciiLower(cn.name);
    if (!cn.schema.empty()) {
      if (IdentifierEqualsCI(cn.schema, qual_l) && out_l.contains(lc)) {
        *rel_out = 0;
        *slot_out = out_l.at(lc);
        return true;
      }
      if (IdentifierEqualsCI(cn.schema, qual_r1) && out_r1.contains(lc)) {
        *rel_out = 1;
        *slot_out = out_r1.at(lc);
        return true;
      }
      if (IdentifierEqualsCI(cn.schema, qual_r2) && out_r2.contains(lc)) {
        *rel_out = 2;
        *slot_out = out_r2.at(lc);
        return true;
      }
      return false;
    }
    int hits = 0;
    if (out_l.contains(lc)) {
      *rel_out = 0;
      *slot_out = out_l.at(lc);
      ++hits;
    }
    if (out_r1.contains(lc)) {
      *rel_out = 1;
      *slot_out = out_r1.at(lc);
      ++hits;
    }
    if (out_r2.contains(lc)) {
      *rel_out = 2;
      *slot_out = out_r2.at(lc);
      ++hits;
    }
    return hits == 1;
  };

  auto is_false_when_rel_null = [&](const Expression& e, int target_rel) {
    if (!e) {
      return false;
    }
    if (e->Type() == TypeTag::kUnaryExp) {
      const auto& u = e->AsUnaryExpression();
      if (u.Op() == UnaryOperation::kIsNotNull && u.Child() &&
          u.Child()->Type() == TypeTag::kColumnValue) {
        int r = -1;
        size_t s = 0;
        return resolve_outer_col(u.Child()->AsColumnValue().GetColumnName(), &r,
                                 &s) &&
               r == target_rel;
      }
      return false;
    }
    if (e->Type() != TypeTag::kBinaryExp) {
      return false;
    }
    const auto& b = e->AsBinaryExpression();
    if (b.Op() != BinaryOperation::kGreaterThan || !b.Left() || !b.Right() ||
        b.Right()->Type() != TypeTag::kConstantValue) {
      return false;
    }
    const Value& rhs = b.Right()->AsConstantValue().GetValue();
    if (rhs.IsNull()) {
      return false;
    }
    const bool rhs_nonneg =
        (rhs.type == ValueType::kInt64 && rhs.value.int_value >= 0) ||
        (rhs.type == ValueType::kDouble && rhs.value.double_value >= 0.0);
    if (!rhs_nonneg) {
      return false;
    }
    if (b.Left()->Type() == TypeTag::kColumnValue) {
      int r = -1;
      size_t s = 0;
      return resolve_outer_col(b.Left()->AsColumnValue().GetColumnName(), &r,
                               &s) &&
             r == target_rel;
    }
    if (b.Left()->Type() == TypeTag::kFunctionCallExp) {
      const auto& fn = b.Left()->AsFunctionCallExpression();
      if ((IdentifierEqualsCI(fn.FuncName(), "coalesce") ||
           IdentifierEqualsCI(fn.FuncName(), "ifnull")) &&
          fn.Args().size() == 2 && fn.Args()[0] &&
          fn.Args()[0]->Type() == TypeTag::kColumnValue && fn.Args()[1] &&
          fn.Args()[1]->Type() == TypeTag::kConstantValue) {
        const Value& def = fn.Args()[1]->AsConstantValue().GetValue();
        if (def.type != ValueType::kInt64 && def.type != ValueType::kDouble) {
          return false;
        }
        const auto def_le_rhs =
            TryEvaluateBinary(BinaryOperation::kLessThanEquals, def, rhs);
        int r = -1;
        size_t s = 0;
        return def_le_rhs.HasValue() && def_le_rhs.Value().Truthy() &&
               resolve_outer_col(fn.Args()[0]->AsColumnValue().GetColumnName(),
                                 &r, &s) &&
               r == target_rel;
      }
    }
    return false;
  };

  const std::vector<Expression> outer_conjs =
      SplitAndConjuncts(stmt->WhereClause());
  bool found_disj_null_reject = false;
  std::unordered_map<size_t, Expression> const_l_filters;
  for (const Expression& c : outer_conjs) {
    if (!c || c->Type() != TypeTag::kBinaryExp) {
      continue;
    }
    const auto& b = c->AsBinaryExpression();
    if (b.Op() == BinaryOperation::kOr) {
      if ((is_false_when_rel_null(b.Left(), 1) &&
           is_false_when_rel_null(b.Right(), 2)) ||
          (is_false_when_rel_null(b.Left(), 2) &&
           is_false_when_rel_null(b.Right(), 1))) {
        found_disj_null_reject = true;
      }
    } else if (b.Op() == BinaryOperation::kEquals && b.Left() && b.Right()) {
      auto check_eq = [&](const Expression& col_e, const Expression& val_e) {
        if (col_e->Type() == TypeTag::kColumnValue &&
            val_e->Type() == TypeTag::kConstantValue &&
            dynamic_cast<const ParameterSlot*>(val_e.get()) == nullptr &&
            !val_e->AsConstantValue().GetValue().IsNull()) {
          int r = -1;
          size_t s = 0;
          if (resolve_outer_col(col_e->AsColumnValue().GetColumnName(), &r,
                                &s) &&
              r == 0) {
            const_l_filters.emplace(s, val_e);
          }
        }
      };
      check_eq(b.Left(), b.Right());
      check_eq(b.Right(), b.Left());
    }
  }
  if (!found_disj_null_reject) {
    return false;
  }

  auto extract_join_eqs = [&](const Expression& cond, int target_r,
                              std::unordered_map<size_t, size_t>* l_to_r) {
    const std::vector<Expression> eqs = SplitAndConjuncts(cond);
    if (eqs.empty()) {
      return false;
    }
    for (const Expression& e : eqs) {
      if (!e || e->Type() != TypeTag::kBinaryExp ||
          e->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
        return false;
      }
      const auto& b = e->AsBinaryExpression();
      if (!b.Left() || !b.Right() ||
          b.Left()->Type() != TypeTag::kColumnValue ||
          b.Right()->Type() != TypeTag::kColumnValue) {
        return false;
      }
      int r1 = -1, r2 = -1;
      size_t s1 = 0, s2 = 0;
      if (!resolve_outer_col(b.Left()->AsColumnValue().GetColumnName(), &r1,
                             &s1) ||
          !resolve_outer_col(b.Right()->AsColumnValue().GetColumnName(), &r2,
                             &s2)) {
        return false;
      }
      if (r1 == 0 && r2 == target_r) {
        (*l_to_r)[s1] = s2;
      } else if (r2 == 0 && r1 == target_r) {
        (*l_to_r)[s2] = s1;
      } else {
        return false;
      }
    }
    return true;
  };
  std::unordered_map<size_t, size_t> l_to_r1, l_to_r2;
  if (!extract_join_eqs(src_r1.join_condition, 1, &l_to_r1) ||
      !extract_join_eqs(src_r2.join_condition, 2, &l_to_r2) ||
      l_to_r1.size() != l_to_r2.size()) {
    return false;
  }
  std::vector<size_t> non_const_l_slots;
  std::vector<size_t> const_l_slots;
  for (const auto& [ls, _] : l_to_r1) {
    if (!l_to_r2.contains(ls)) {
      return false;
    }
    if (const_l_filters.contains(ls)) {
      const_l_slots.push_back(ls);
    } else {
      non_const_l_slots.push_back(ls);
    }
  }
  std::ranges::sort(non_const_l_slots);
  std::ranges::sort(const_l_slots);
  if (non_const_l_slots.size() != 2) {
    return false;
  }

  struct CteFactInfo {
    std::string fact_table;
    std::string fact_alias;
    std::vector<ColumnName> key_cols;
    std::vector<SelectSource> inner_dim_sources;
    std::vector<Expression> dim_const_preds;
    uint64_t fact_rows{0};
  };
  auto analyze_cte = [&](const SelectStatement& cte,
                         const std::vector<size_t>& non_const_slots,
                         const std::vector<size_t>& const_slots,
                         CteFactInfo* info) -> bool {
    const SelectSource& fsrc = cte.Sources()[0];
    if (fsrc.query != nullptr || fsrc.unnest != nullptr ||
        fsrc.sample.has_value() || fsrc.table.empty()) {
      return false;
    }
    auto tbl = ctx.GetTable(fsrc.table);
    if (!tbl.HasValue() || tbl.Value() == nullptr) {
      return false;
    }
    auto st = ctx.GetStats(fsrc.table);
    info->fact_rows = st.HasValue() ? st.Value()->Rows() : 0;
    const Schema& fschema = tbl.Value()->GetSchema();
    info->fact_table = fsrc.table;
    info->fact_alias = fsrc.alias.empty() ? fsrc.table : fsrc.alias;

    auto expr_in_gb = [&](const Expression& e) {
      if (!e || e->Type() != TypeTag::kColumnValue) {
        return false;
      }
      const ColumnName& cn = e->AsColumnValue().GetColumnName();
      return std::ranges::any_of(cte.GroupBy(), [&](const Expression& g) {
        if (g && g->Type() == TypeTag::kColumnValue) {
          const ColumnName& gn = g->AsColumnValue().GetColumnName();
          return IdentifierEqualsCI(cn.name, gn.name) &&
                 (cn.schema.empty() || gn.schema.empty() ||
                  IdentifierEqualsCI(cn.schema, gn.schema));
        }
        return false;
      });
    };
    for (const size_t sl : non_const_slots) {
      const Expression& se = cte.SelectList()[sl].expression;
      if (!se || se->Type() != TypeTag::kColumnValue || !expr_in_gb(se)) {
        return false;
      }
      const ColumnName& cn = se->AsColumnValue().GetColumnName();
      if (!FindColInSchema(fschema, cn.name).has_value()) {
        return false;
      }
      info->key_cols.emplace_back(info->fact_alias, cn.name);
    }
    for (size_t i = 1; i < cte.Sources().size(); ++i) {
      const SelectSource& s = cte.Sources()[i];
      if (s.join_type == JoinType::kInner && s.query == nullptr &&
          s.unnest == nullptr && !s.sample.has_value()) {
        info->inner_dim_sources.push_back(s);
      }
    }
    for (size_t i = 0; i < const_slots.size(); ++i) {
      const size_t sl = const_slots[i];
      const Expression& se = cte.SelectList()[sl].expression;
      if (!se || se->Type() != TypeTag::kColumnValue || !expr_in_gb(se)) {
        return false;
      }
      info->dim_const_preds.push_back(BinaryExpressionExp(
          se, BinaryOperation::kEquals, const_l_filters.at(const_l_slots[i])));
    }
    return true;
  };

  std::vector<size_t> nc_r1, nc_r2, c_r1, c_r2;
  for (const size_t ls : non_const_l_slots) {
    nc_r1.push_back(l_to_r1.at(ls));
    nc_r2.push_back(l_to_r2.at(ls));
  }
  for (const size_t ls : const_l_slots) {
    c_r1.push_back(l_to_r1.at(ls));
    c_r2.push_back(l_to_r2.at(ls));
  }
  CteFactInfo info_l, info_r1, info_r2;
  if (!analyze_cte(*cte_l, non_const_l_slots, const_l_slots, &info_l) ||
      !analyze_cte(*cte_r1, nc_r1, c_r1, &info_r1) ||
      !analyze_cte(*cte_r2, nc_r2, c_r2, &info_r2)) {
    return false;
  }
  if (info_l.fact_rows < 100000 && info_r1.fact_rows < 100000 &&
      info_r2.fact_rows < 100000) {
    return false;
  }

  auto make_key_sub = [&](const CteFactInfo& inf) {
    auto sub = std::make_shared<SelectStatement>();
    std::vector<SelectSource> srcs;
    srcs.emplace_back(inf.fact_table, inf.fact_alias, nullptr, JoinType::kCross,
                      nullptr);
    for (const SelectSource& ds : inf.inner_dim_sources) {
      srcs.push_back(ds);
    }
    sub->SetSources(std::move(srcs));
    sub->SetSelectList({
        NamedExpression("k0", ColumnValueExp(inf.key_cols[0])),
        NamedExpression("k1", ColumnValueExp(inf.key_cols[1])),
    });
    if (!inf.dim_const_preds.empty()) {
      sub->SetWhereClause(CombineAndConjuncts(inf.dim_const_preds));
    }
    return sub;
  };

  auto s_sub = make_key_sub(info_l);
  auto o_sub = make_key_sub(info_r1);
  o_sub->AddUnionAll(make_key_sub(info_r2));

  auto active_cte = std::make_shared<SelectStatement>();
  active_cte->SetDistinct(true);
  active_cte->SetSelectList({
      NamedExpression("__lj_k0", ColumnValueExp(ColumnName("__lj_s", "k0"))),
      NamedExpression("__lj_k1", ColumnValueExp(ColumnName("__lj_s", "k1"))),
  });
  active_cte->SetSources({
      SelectSource("", "__lj_s", std::move(s_sub), JoinType::kCross, nullptr),
      SelectSource(
          "", "__lj_o", std::move(o_sub), JoinType::kInner,
          BinaryExpressionExp(
              BinaryExpressionExp(ColumnValueExp(ColumnName("__lj_s", "k0")),
                                  BinaryOperation::kEquals,
                                  ColumnValueExp(ColumnName("__lj_o", "k0"))),
              BinaryOperation::kAnd,
              BinaryExpressionExp(ColumnValueExp(ColumnName("__lj_s", "k1")),
                                  BinaryOperation::kEquals,
                                  ColumnValueExp(ColumnName("__lj_o", "k1"))))),
  });

  auto rewrite_cte_body = [&](SelectStatement* body, const CteFactInfo& inf) {
    std::vector<SelectSource> new_srcs;
    new_srcs.reserve(body->Sources().size() + 1);
    new_srcs.emplace_back(active_cte_name, active_cte_name, nullptr,
                          JoinType::kCross, nullptr);
    SelectSource fact_src = body->Sources()[0];
    fact_src.join_type = JoinType::kInner;
    fact_src.join_condition = BinaryExpressionExp(
        BinaryExpressionExp(
            ColumnValueExp(inf.key_cols[0]), BinaryOperation::kEquals,
            ColumnValueExp(ColumnName(active_cte_name, "__lj_k0"))),
        BinaryOperation::kAnd,
        BinaryExpressionExp(
            ColumnValueExp(inf.key_cols[1]), BinaryOperation::kEquals,
            ColumnValueExp(ColumnName(active_cte_name, "__lj_k1"))));
    new_srcs.push_back(std::move(fact_src));
    for (size_t i = 1; i < body->Sources().size(); ++i) {
      new_srcs.push_back(body->Sources()[i]);
    }
    body->SetSources(std::move(new_srcs));
    if (!inf.dim_const_preds.empty()) {
      std::vector<Expression> w = SplitAndConjuncts(body->WhereClause());
      for (const Expression& cp : inf.dim_const_preds) {
        w.push_back(cp);
      }
      body->SetWhereClause(CombineAndConjuncts(w));
    }
  };
  rewrite_cte_body(cte_l.get(), info_l);
  rewrite_cte_body(cte_r1.get(), info_r1);
  rewrite_cte_body(cte_r2.get(), info_r2);

  std::vector<std::pair<std::string, std::shared_ptr<SelectStatement>>> saved;
  saved.reserve(stmt->WithQueryOrder().size());
  for (const std::string& wname : stmt->WithQueryOrder()) {
    auto it = stmt->WithQueries().find(wname);
    if (it != stmt->WithQueries().end()) {
      saved.emplace_back(wname, it->second);
    }
  }
  stmt->ClearWithQueries();
  stmt->AddWithQuery(active_cte_name, std::move(active_cte));
  for (auto& [wname, wbody] : saved) {
    stmt->AddWithQuery(std::move(wname), std::move(wbody));
  }
  return true;
}

// Lean proof: lean/TinylambOpt/VectorizedJoinAndPruning.lean
//   (join_union_all_distribute_sound)
// Distributes inner/cross joins and WHERE predicates over a UNION ALL derived
// table of plain base-table projections:
//   SELECT [DISTINCT] proj FROM (B1 UNION ALL B2 ...) AS u, R1, ..., Rm WHERE P
//   ==>
//   SELECT [DISTINCT] proj FROM (
//     SELECT proj FROM B1, R1, ..., Rm WHERE P
//     UNION ALL
//     SELECT proj FROM B2, R1, ..., Rm WHERE P
//   ) AS __du_u
// Avoids materializing huge UNION ALL fact tables (e.g. TPC-DS Q54's 21.6M-row
// catalog_sales UNION ALL web_sales) before selective dimension filters.
bool RewriteDistributeJoinOverUnionAll(SelectStatement* stmt,
                                       TransactionContext& ctx) {
  if (stmt == nullptr) {
    return false;
  }
  bool changed = false;
  for (const std::string& wname : stmt->WithQueryOrder()) {
    auto it = stmt->WithQueries().find(wname);
    if (it != stmt->WithQueries().end() && it->second != nullptr) {
      if (RewriteDistributeJoinOverUnionAll(it->second.get(), ctx)) {
        changed = true;
      }
    }
  }
  for (const auto& br : stmt->UnionAll()) {
    if (br != nullptr && RewriteDistributeJoinOverUnionAll(br.get(), ctx)) {
      changed = true;
    }
  }
  for (const auto& src : stmt->Sources()) {
    if (src.query != nullptr &&
        RewriteDistributeJoinOverUnionAll(src.query.get(), ctx)) {
      changed = true;
    }
  }
  if (stmt->Sources().size() < 2 || !stmt->GroupBy().empty() ||
      stmt->Having() != nullptr || stmt->Qualify() != nullptr ||
      stmt->HasLimit() || stmt->Offset() != 0 || !stmt->OrderBy().empty() ||
      !stmt->UnionAll().empty() || stmt->GetSetOperationTree() != nullptr ||
      stmt->AsStruct() || stmt->ForUpdate() || stmt->SelectList().empty()) {
    return changed;
  }
  auto has_agg_or_win = [&](const Expression& e) -> bool {
    bool found = false;
    std::function<void(const Expression&)> walk = [&](const Expression& node) {
      if (!node || found) {
        return;
      }
      if (node->Type() == TypeTag::kAggregateExp ||
          node->Type() == TypeTag::kWindowFunctionExp ||
          node->Type() == TypeTag::kQueryExp) {
        found = true;
        return;
      }
      for (const Expression& ch : ExpressionChildren(node)) {
        walk(ch);
      }
    };
    walk(e);
    return found;
  };
  for (const NamedExpression& item : stmt->SelectList()) {
    if (!item.expression || has_agg_or_win(item.expression)) {
      return changed;
    }
    if (item.expression->Type() == TypeTag::kColumnValue &&
        item.expression->AsColumnValue().GetColumnName().name == "*") {
      return changed;
    }
  }
  if (stmt->WhereClause() != nullptr && has_agg_or_win(stmt->WhereClause())) {
    return changed;
  }
  for (const SelectSource& src : stmt->Sources()) {
    if ((src.join_type != JoinType::kCross &&
         src.join_type != JoinType::kInner) ||
        src.unnest || src.sample || !src.using_columns.empty() ||
        (src.join_condition != nullptr && has_agg_or_win(src.join_condition))) {
      return changed;
    }
  }
  // Find a UNION ALL derived table source with 2..4 simple base-table branches
  // (supporting both flat UnionAll() and SetOperationTree representations).
  size_t u_idx = stmt->Sources().size();
  std::vector<const SelectStatement*> branches;
  for (size_t i = 0; i < stmt->Sources().size(); ++i) {
    const SelectSource& src = stmt->Sources()[i];
    if (src.query == nullptr || src.query->UnionByName() ||
        src.query->UnionDistinct() || !src.query->WithQueries().empty() ||
        src.query->HasLimit() || src.query->Offset() != 0 ||
        !src.query->OrderBy().empty()) {
      continue;
    }
    bool match_ok = true;
    for (const SetOperationMatch& m : src.query->Matches()) {
      if (m.by_name || m.corresponding || !m.columns.empty()) {
        match_ok = false;
        break;
      }
    }
    if (!match_ok) {
      continue;
    }
    std::vector<const SelectStatement*> cand;
    if (const auto& tree = src.query->GetSetOperationTree(); tree != nullptr) {
      if (tree->first == nullptr || tree->branches.empty() ||
          tree->branches.size() > 3 ||
          tree->kinds.size() != tree->branches.size()) {
        continue;
      }
      bool all_ua = true;
      for (SetOperationKind k : tree->kinds) {
        if (k != SetOperationKind::kUnionAll) {
          all_ua = false;
          break;
        }
      }
      if (!all_ua) {
        continue;
      }
      cand.push_back(tree->first.get());
      for (const auto& br : tree->branches) {
        if (br == nullptr) {
          all_ua = false;
          break;
        }
        cand.push_back(br.get());
      }
      if (!all_ua) {
        continue;
      }
    } else if (!src.query->UnionAll().empty() &&
               src.query->UnionAll().size() <= 3) {
      const auto& kinds = src.query->SetOperationKinds();
      bool all_ua = true;
      for (size_t k = 0; k < src.query->UnionAll().size(); ++k) {
        const SetOperationKind kind =
            k < kinds.size() ? kinds[k] : SetOperationKind::kUnionAll;
        if (kind != SetOperationKind::kUnionAll ||
            src.query->UnionAll()[k] == nullptr) {
          all_ua = false;
          break;
        }
      }
      if (!all_ua) {
        continue;
      }
      cand.push_back(src.query.get());
      for (const auto& br : src.query->UnionAll()) {
        cand.push_back(br.get());
      }
    } else {
      continue;
    }
    if (cand.size() >= 2) {
      u_idx = i;
      branches = std::move(cand);
      break;
    }
  }
  if (u_idx >= stmt->Sources().size() || branches.size() < 2) {
    return changed;
  }
  const SelectSource& u_src = stmt->Sources()[u_idx];
  const std::string u_alias = u_src.alias;
  if (u_alias.empty()) {
    return changed;
  }
  const SelectStatement& head = *branches[0];
  const size_t n_cols = head.SelectList().size();
  if (n_cols == 0) {
    return changed;
  }
  std::vector<std::string> u_out_names;
  u_out_names.reserve(n_cols);
  std::unordered_map<std::string, size_t> u_out_index;
  for (size_t c = 0; c < n_cols; ++c) {
    const NamedExpression& ne = head.SelectList()[c];
    std::string nm = ne.name;
    if (nm.empty() && ne.expression != nullptr &&
        ne.expression->Type() == TypeTag::kColumnValue) {
      nm = ne.expression->AsColumnValue().GetColumnName().name;
    }
    if (nm.empty() || nm == "*") {
      return changed;
    }
    std::string low = AsciiLower(nm);
    if (!u_out_index.emplace(low, c).second) {
      return changed;
    }
    u_out_names.push_back(std::move(low));
  }
  struct BranchInfo {
    std::string table;
    std::string orig_alias;
    std::vector<std::string> col_names;
    Expression where_clause;
  };
  std::vector<BranchInfo> b_infos;
  b_infos.reserve(branches.size());
  for (const SelectStatement* br : branches) {
    if (br->Sources().size() != 1 || br->Sources()[0].query != nullptr ||
        br->Sources()[0].unnest || br->Sources()[0].sample ||
        !br->GroupBy().empty() || br->Having() != nullptr || br->Distinct() ||
        br->HasLimit() || br->Offset() != 0 || !br->OrderBy().empty() ||
        !br->WithQueries().empty() || br->Qualify() != nullptr ||
        br->SelectList().size() != n_cols) {
      return changed;
    }
    const SelectSource& bsrc = br->Sources()[0];
    if (bsrc.table.empty()) {
      return changed;
    }
    StatusOr<std::shared_ptr<Table>> tbl = ctx.GetTable(bsrc.table);
    if (!tbl.HasValue()) {
      return changed;
    }
    const Schema& bschema = tbl.Value()->GetSchema();
    BranchInfo info;
    info.table = bsrc.table;
    info.orig_alias = bsrc.alias.empty() ? bsrc.table : bsrc.alias;
    info.where_clause = br->WhereClause();
    info.col_names.reserve(n_cols);
    for (size_t c = 0; c < n_cols; ++c) {
      const Expression& e = br->SelectList()[c].expression;
      if (!e || e->Type() != TypeTag::kColumnValue) {
        return changed;
      }
      const ColumnName& cn = e->AsColumnValue().GetColumnName();
      if (cn.name.empty() || cn.name == "*") {
        return changed;
      }
      if (!cn.schema.empty() &&
          AsciiLower(cn.schema) != AsciiLower(info.orig_alias) &&
          AsciiLower(cn.schema) != AsciiLower(info.table)) {
        return changed;
      }
      bool exists = false;
      for (size_t sc = 0; sc < bschema.ColumnCount(); ++sc) {
        if (AsciiLower(bschema.GetColumn(sc).Name().name) ==
            AsciiLower(cn.name)) {
          exists = true;
          break;
        }
      }
      if (!exists) {
        return changed;
      }
      info.col_names.push_back(cn.name);
    }
    b_infos.push_back(std::move(info));
  }
  std::unordered_set<std::string> other_cols;
  for (size_t i = 0; i < stmt->Sources().size(); ++i) {
    if (i == u_idx) {
      continue;
    }
    const SelectSource& osrc = stmt->Sources()[i];
    if (osrc.query == nullptr && !osrc.table.empty()) {
      StatusOr<std::shared_ptr<Table>> ot = ctx.GetTable(osrc.table);
      if (ot.HasValue()) {
        const Schema& os = ot.Value()->GetSchema();
        for (size_t sc = 0; sc < os.ColumnCount(); ++sc) {
          other_cols.insert(AsciiLower(os.GetColumn(sc).Name().name));
        }
      }
    } else if (osrc.query != nullptr) {
      // A derived source's output names are just as visible as base columns;
      // otherwise unqualified references get rebound into the UNION branch.
      for (const NamedExpression& ne : osrc.query->SelectList()) {
        std::string nm = ne.name;
        if (nm.empty() && ne.expression &&
            ne.expression->Type() == TypeTag::kColumnValue) {
          nm = ne.expression->AsColumnValue().GetColumnName().name;
        }
        if (!nm.empty() && nm != "*") {
          other_cols.insert(AsciiLower(nm));
        }
      }
    }
  }
  // The generated branch/wrapper aliases must not shadow any source already
  // visible in the statement, or the rebind below captures the wrong columns.
  std::unordered_set<std::string> used_aliases;
  for (const SelectSource& s : stmt->Sources()) {
    if (!s.alias.empty()) {
      used_aliases.insert(AsciiLower(s.alias));
    }
    if (!s.table.empty()) {
      used_aliases.insert(AsciiLower(s.table));
    }
  }
  if (used_aliases.contains("__du_union")) {
    return changed;
  }
  for (size_t bi = 0; bi < b_infos.size(); ++bi) {
    if (used_aliases.contains(AsciiLower("__du_b" + std::to_string(bi)))) {
      return changed;
    }
  }
  const std::string u_alias_low = AsciiLower(u_alias);
  bool rewrite_ok = true;
  auto map_expr = [](const Expression& root, auto&& fn) -> Expression {
    std::function<Expression(const Expression&)> rec =
        [&](const Expression& node) -> Expression {
      if (!node) {
        return nullptr;
      }
      std::vector<Expression> children = ExpressionChildren(node);
      bool child_changed = false;
      for (Expression& ch : children) {
        Expression next = rec(ch);
        if (next.get() != ch.get()) {
          child_changed = true;
          ch = std::move(next);
        }
      }
      Expression rebuilt =
          child_changed ? WithExpressionChildren(node, std::move(children))
                        : node;
      return fn(rebuilt);
    };
    return rec(root);
  };
  auto rewrite_expr_for_branch = [&](const Expression& expr,
                                     const std::string& br_alias,
                                     const BranchInfo& bi) -> Expression {
    return map_expr(expr, [&](const Expression& node) -> Expression {
      if (node->Type() == TypeTag::kColumnValue) {
        const ColumnName& cn = node->AsColumnValue().GetColumnName();
        const std::string tlow = AsciiLower(cn.schema);
        const std::string nlow = AsciiLower(cn.name);
        if (tlow == u_alias_low) {
          auto it = u_out_index.find(nlow);
          if (it == u_out_index.end()) {
            rewrite_ok = false;
            return node;
          }
          return ColumnValueExp(ColumnName(br_alias, bi.col_names[it->second]));
        }
        if (tlow.empty()) {
          auto it = u_out_index.find(nlow);
          if (it != u_out_index.end()) {
            if (other_cols.contains(nlow)) {
              rewrite_ok = false;
              return node;
            }
            return ColumnValueExp(
                ColumnName(br_alias, bi.col_names[it->second]));
          }
        }
      }
      return node;
    });
  };
  auto rewrite_branch_where = [&](const Expression& expr,
                                  const std::string& br_alias,
                                  const BranchInfo& bi) -> Expression {
    const std::string oalow = AsciiLower(bi.orig_alias);
    const std::string tlow_b = AsciiLower(bi.table);
    return map_expr(expr, [&](const Expression& node) -> Expression {
      if (node->Type() == TypeTag::kColumnValue) {
        const ColumnName& cn = node->AsColumnValue().GetColumnName();
        const std::string tl = AsciiLower(cn.schema);
        if (tl.empty() || tl == oalow || tl == tlow_b) {
          return ColumnValueExp(ColumnName(br_alias, cn.name));
        }
      }
      return node;
    });
  };

  std::vector<std::shared_ptr<SelectStatement>> built_branches;
  built_branches.reserve(b_infos.size());
  for (size_t bi = 0; bi < b_infos.size(); ++bi) {
    const BranchInfo& info = b_infos[bi];
    const std::string br_alias = "__du_b" + std::to_string(bi);
    auto br_stmt = std::make_shared<SelectStatement>();
    std::vector<NamedExpression> br_select;
    br_select.reserve(stmt->SelectList().size());
    for (size_t si = 0; si < stmt->SelectList().size(); ++si) {
      const NamedExpression& item = stmt->SelectList()[si];
      std::string alias = item.name;
      if (alias.empty() && item.expression->Type() == TypeTag::kColumnValue) {
        alias = item.expression->AsColumnValue().GetColumnName().name;
      }
      if (alias.empty()) {
        alias = "__du_c" + std::to_string(si);
      }
      br_select.emplace_back(
          alias, rewrite_expr_for_branch(item.expression, br_alias, info));
    }
    br_stmt->SetSelectList(std::move(br_select));
    std::vector<SelectSource> br_sources;
    br_sources.reserve(stmt->Sources().size());
    std::vector<Expression> extra_preds;
    for (size_t i = 0; i < stmt->Sources().size(); ++i) {
      if (i == u_idx) {
        br_sources.emplace_back(info.table, br_alias, nullptr, JoinType::kCross,
                                nullptr);
        if (stmt->Sources()[i].join_condition != nullptr) {
          extra_preds.push_back(rewrite_expr_for_branch(
              stmt->Sources()[i].join_condition, br_alias, info));
        }
      } else {
        SelectSource copy = stmt->Sources()[i];
        if (copy.join_condition != nullptr) {
          copy.join_condition =
              rewrite_expr_for_branch(copy.join_condition, br_alias, info);
        }
        br_sources.push_back(std::move(copy));
      }
    }
    br_stmt->SetSources(std::move(br_sources));
    std::vector<Expression> where_parts;
    if (stmt->WhereClause() != nullptr) {
      where_parts.push_back(
          rewrite_expr_for_branch(stmt->WhereClause(), br_alias, info));
    }
    if (info.where_clause != nullptr) {
      where_parts.push_back(
          rewrite_branch_where(info.where_clause, br_alias, info));
    }
    for (auto& ep : extra_preds) {
      where_parts.push_back(std::move(ep));
    }
    if (!where_parts.empty()) {
      br_stmt->SetWhereClause(CombineAndConjuncts(where_parts));
    }
    if (!rewrite_ok) {
      return changed;
    }
    built_branches.push_back(std::move(br_stmt));
  }

  auto union_sub = built_branches[0];
  for (size_t bi = 1; bi < built_branches.size(); ++bi) {
    union_sub->AddUnionAll(built_branches[bi]);
  }
  const std::string wrap_alias = "__du_union";
  std::vector<NamedExpression> outer_select;
  outer_select.reserve(stmt->SelectList().size());
  for (size_t si = 0; si < stmt->SelectList().size(); ++si) {
    const std::string& col_nm = built_branches[0]->SelectList()[si].name;
    const std::string& orig_alias = stmt->SelectList()[si].name;
    outer_select.emplace_back(orig_alias.empty() ? col_nm : orig_alias,
                              ColumnValueExp(ColumnName(wrap_alias, col_nm)));
  }
  stmt->SetSelectList(std::move(outer_select));
  stmt->SetSources({SelectSource("", wrap_alias, std::move(union_sub),
                                 JoinType::kCross, nullptr)});
  stmt->SetWhereClause(nullptr);
  return true;
}

// Lean proof: lean/TinylambOpt/VectorizedJoinAndPruning.lean
//   (grouped_sum_union_all_distribute_sound)
// Distributes grouped SUM/MIN/MAX aggregation over a pure UNION ALL CTE or
// derived table of plain base-table projections:
//   SELECT g..., SUM(e1), ... FROM u, R1, ... WHERE P GROUP BY g...
//   ==>
//   SELECT __dg_u.g..., SUM(__dg_u.a1), ... FROM (
//     SELECT g..., SUM(e1[B0]) AS a1, ... FROM B0, R1, ... WHERE P[B0] GROUP BY
//     g... UNION ALL SELECT g..., SUM(e1[B1]) AS a1, ... FROM B1, R1, ... WHERE
//     P[B1] GROUP BY g...
//   ) AS __dg_u GROUP BY __dg_u.g...
// Eliminates materializing multi-million-row UNION ALL fact tables (e.g.
// TPC-DS Q02 `wscs` 8.76M rows) and lets each branch hit deferred fused
// grouped-join directly on its base table.
bool RewriteDistributeGroupedSumOverUnionAll(SelectStatement* root,
                                             TransactionContext& ctx) {
  if (root == nullptr) {
    return false;
  }
  auto extract_union_branches =
      [](const SelectStatement* q,
         std::vector<const SelectStatement*>* out) -> bool {
    out->clear();
    if (q == nullptr || q->UnionByName() || q->UnionDistinct() ||
        !q->WithQueries().empty() || q->HasLimit() || q->Offset() != 0 ||
        !q->OrderBy().empty()) {
      return false;
    }
    // Unwrap trivial pass-through wrapper `SELECT c1, c2 FROM (B1 UNION ALL B2)
    // sq`.
    if (q->UnionAll().empty() && q->GetSetOperationTree() == nullptr) {
      if (!q->GroupBy().empty() || q->Having() != nullptr || q->Distinct() ||
          q->WhereClause() != nullptr || q->Qualify() != nullptr ||
          q->Sources().size() != 1 || q->Sources()[0].query == nullptr) {
        return false;
      }
      const SelectStatement* inner = q->Sources()[0].query.get();
      std::vector<const SelectStatement*> inner_brs;
      if (inner == nullptr || inner->UnionByName() || inner->UnionDistinct() ||
          !inner->WithQueries().empty() || inner->HasLimit() ||
          inner->Offset() != 0 || !inner->OrderBy().empty()) {
        return false;
      }
      if (const auto& tree = inner->GetSetOperationTree(); tree != nullptr) {
        if (tree->first == nullptr || tree->branches.empty() ||
            tree->branches.size() > 3 ||
            tree->kinds.size() != tree->branches.size()) {
          return false;
        }
        for (SetOperationKind k : tree->kinds) {
          if (k != SetOperationKind::kUnionAll) {
            return false;
          }
        }
        inner_brs.push_back(tree->first.get());
        for (const auto& b : tree->branches) {
          if (!b) {
            return false;
          }
          inner_brs.push_back(b.get());
        }
      } else if (!inner->UnionAll().empty() && inner->UnionAll().size() <= 3) {
        const auto& kinds = inner->SetOperationKinds();
        for (size_t k = 0; k < inner->UnionAll().size(); ++k) {
          const SetOperationKind kind =
              k < kinds.size() ? kinds[k] : SetOperationKind::kUnionAll;
          if (kind != SetOperationKind::kUnionAll || !inner->UnionAll()[k]) {
            return false;
          }
        }
        inner_brs.push_back(inner);
        for (const auto& b : inner->UnionAll()) {
          inner_brs.push_back(b.get());
        }
      } else {
        return false;
      }
      if (inner_brs.size() < 2 ||
          inner_brs[0]->SelectList().size() != q->SelectList().size()) {
        return false;
      }
      for (size_t c = 0; c < q->SelectList().size(); ++c) {
        const NamedExpression& oe = q->SelectList()[c];
        const NamedExpression& ie = inner_brs[0]->SelectList()[c];
        if (!oe.expression || oe.expression->Type() != TypeTag::kColumnValue) {
          return false;
        }
        std::string in_nm = ie.name;
        if (in_nm.empty() && ie.expression &&
            ie.expression->Type() == TypeTag::kColumnValue) {
          in_nm = ie.expression->AsColumnValue().GetColumnName().name;
        }
        std::string out_nm =
            oe.name.empty()
                ? oe.expression->AsColumnValue().GetColumnName().name
                : oe.name;
        if (in_nm.empty() ||
            AsciiLower(oe.expression->AsColumnValue().GetColumnName().name) !=
                AsciiLower(in_nm) ||
            AsciiLower(out_nm) != AsciiLower(in_nm)) {
          return false;
        }
      }
      *out = std::move(inner_brs);
      return true;
    }
    for (const SetOperationMatch& m : q->Matches()) {
      if (m.by_name || m.corresponding || !m.columns.empty()) {
        return false;
      }
    }
    if (const auto& tree = q->GetSetOperationTree(); tree != nullptr) {
      if (tree->first == nullptr || tree->branches.empty() ||
          tree->branches.size() > 3 ||
          tree->kinds.size() != tree->branches.size()) {
        return false;
      }
      for (SetOperationKind k : tree->kinds) {
        if (k != SetOperationKind::kUnionAll) {
          return false;
        }
      }
      out->push_back(tree->first.get());
      for (const auto& b : tree->branches) {
        if (!b) {
          return false;
        }
        out->push_back(b.get());
      }
      return out->size() >= 2;
    }
    if (!q->UnionAll().empty() && q->UnionAll().size() <= 3) {
      const auto& kinds = q->SetOperationKinds();
      for (size_t k = 0; k < q->UnionAll().size(); ++k) {
        const SetOperationKind kind =
            k < kinds.size() ? kinds[k] : SetOperationKind::kUnionAll;
        if (kind != SetOperationKind::kUnionAll || !q->UnionAll()[k]) {
          return false;
        }
      }
      out->push_back(q);
      for (const auto& b : q->UnionAll()) {
        out->push_back(b.get());
      }
      return out->size() >= 2;
    }
    return false;
  };

  auto map_expr = [](const Expression& root_e, auto&& fn) -> Expression {
    std::function<Expression(const Expression&)> rec =
        [&](const Expression& node) -> Expression {
      if (!node) {
        return nullptr;
      }
      std::vector<Expression> children = ExpressionChildren(node);
      bool ch_changed = false;
      for (Expression& ch : children) {
        Expression next = rec(ch);
        if (next.get() != ch.get()) {
          ch_changed = true;
          ch = std::move(next);
        }
      }
      Expression rebuilt =
          ch_changed ? WithExpressionChildren(node, std::move(children)) : node;
      return fn(rebuilt);
    };
    return rec(root_e);
  };

  auto has_disallowed_inner = [](const Expression& e) -> bool {
    bool bad = false;
    std::function<void(const Expression&)> walk = [&](const Expression& node) {
      if (!node || bad) {
        return;
      }
      if (node->Type() == TypeTag::kAggregateExp ||
          node->Type() == TypeTag::kWindowFunctionExp ||
          node->Type() == TypeTag::kQueryExp) {
        bad = true;
        return;
      }
      for (const Expression& ch : ExpressionChildren(node)) {
        walk(ch);
      }
    };
    walk(e);
    return bad;
  };

  std::unordered_set<std::string> consumed_ctes;
  std::function<bool(
      SelectStatement*,
      const std::unordered_map<std::string, const SelectStatement*>&)>
      rewrite_one =
          [&](SelectStatement* stmt,
              const std::unordered_map<std::string, const SelectStatement*>&
                  cte_env) -> bool {
    if (stmt == nullptr) {
      return false;
    }
    bool changed = false;
    auto local_env = cte_env;
    for (const std::string& wname : stmt->WithQueryOrder()) {
      auto it = stmt->WithQueries().find(wname);
      if (it != stmt->WithQueries().end() && it->second != nullptr) {
        if (rewrite_one(it->second.get(), local_env)) {
          changed = true;
        }
        local_env[AsciiLower(wname)] = it->second.get();
      }
    }
    for (const auto& br : stmt->UnionAll()) {
      if (br != nullptr && rewrite_one(br.get(), local_env)) {
        changed = true;
      }
    }
    for (const auto& src : stmt->Sources()) {
      if (src.query != nullptr && rewrite_one(src.query.get(), local_env)) {
        changed = true;
      }
    }
    if (stmt->Sources().size() < 2 || stmt->GroupBy().empty() ||
        stmt->Having() != nullptr || stmt->Qualify() != nullptr ||
        stmt->Distinct() || stmt->HasLimit() || stmt->Offset() != 0 ||
        !stmt->OrderBy().empty() || !stmt->UnionAll().empty() ||
        stmt->GetSetOperationTree() != nullptr || stmt->AsStruct() ||
        stmt->ForUpdate() || stmt->SelectList().empty()) {
      return changed;
    }
    if (stmt->WhereClause() != nullptr &&
        has_disallowed_inner(stmt->WhereClause())) {
      return changed;
    }
    for (const SelectSource& src : stmt->Sources()) {
      if ((src.join_type != JoinType::kCross &&
           src.join_type != JoinType::kInner) ||
          src.unnest || src.sample || !src.using_columns.empty() ||
          (src.join_condition != nullptr &&
           has_disallowed_inner(src.join_condition))) {
        return changed;
      }
    }
    // Check that every SELECT item is either a plain group-key column or a
    // top-level non-DISTINCT SUM/MIN/MAX(expr) with no nested aggs/windows.
    std::vector<ColumnName> group_cols;
    group_cols.reserve(stmt->GroupBy().size());
    for (const Expression& ge : stmt->GroupBy()) {
      if (!ge || ge->Type() != TypeTag::kColumnValue) {
        return changed;
      }
      group_cols.push_back(ge->AsColumnValue().GetColumnName());
    }
    struct OutItemSpec {
      bool is_agg{false};
      size_t group_idx{0};
      AggregationType agg_type{AggregationType::kSum};
      Expression agg_child;
      std::string out_name;
    };
    std::vector<OutItemSpec> out_specs;
    out_specs.reserve(stmt->SelectList().size());
    bool any_agg = false;
    for (size_t si = 0; si < stmt->SelectList().size(); ++si) {
      const NamedExpression& ne = stmt->SelectList()[si];
      if (!ne.expression) {
        return changed;
      }
      if (ne.expression->Type() == TypeTag::kAggregateExp) {
        const auto& agg = ne.expression->AsAggregateExpression();
        if (agg.Distinct() ||
            (agg.GetType() != AggregationType::kSum &&
             agg.GetType() != AggregationType::kMin &&
             agg.GetType() != AggregationType::kMax) ||
            !agg.Child() || has_disallowed_inner(agg.Child())) {
          return changed;
        }
        any_agg = true;
        out_specs.push_back(OutItemSpec{
            .is_agg = true,
            .group_idx = 0,
            .agg_type = agg.GetType(),
            .agg_child = agg.Child(),
            .out_name =
                ne.name.empty() ? ("__dg_a" + std::to_string(si)) : ne.name});
      } else if (ne.expression->Type() == TypeTag::kColumnValue) {
        const ColumnName& cn = ne.expression->AsColumnValue().GetColumnName();
        size_t gidx = group_cols.size();
        for (size_t gi = 0; gi < group_cols.size(); ++gi) {
          if (AsciiLower(group_cols[gi].name) == AsciiLower(cn.name) &&
              (group_cols[gi].schema.empty() || cn.schema.empty() ||
               AsciiLower(group_cols[gi].schema) == AsciiLower(cn.schema))) {
            gidx = gi;
            break;
          }
        }
        if (gidx >= group_cols.size()) {
          return changed;
        }
        out_specs.push_back(
            OutItemSpec{.is_agg = false,
                        .group_idx = gidx,
                        .agg_type = AggregationType::kSum,
                        .agg_child = nullptr,
                        .out_name = ne.name.empty() ? cn.name : ne.name});
      } else {
        return changed;
      }
    }
    if (!any_agg) {
      return changed;
    }

    // Locate a UNION ALL source (inline derived table or CTE).
    size_t u_idx = stmt->Sources().size();
    std::vector<const SelectStatement*> branches;
    for (size_t i = 0; i < stmt->Sources().size(); ++i) {
      const SelectSource& src = stmt->Sources()[i];
      const SelectStatement* candidate = nullptr;
      if (src.query != nullptr) {
        candidate = src.query.get();
      } else if (!src.table.empty()) {
        auto it = local_env.find(AsciiLower(src.table));
        if (it != local_env.end()) {
          candidate = it->second;
        }
      }
      if (candidate != nullptr &&
          extract_union_branches(candidate, &branches)) {
        u_idx = i;
        break;
      }
    }
    if (u_idx >= stmt->Sources().size() || branches.size() < 2) {
      return changed;
    }
    const SelectSource& u_src = stmt->Sources()[u_idx];
    const std::string u_alias = u_src.alias.empty() ? u_src.table : u_src.alias;
    if (u_alias.empty()) {
      return changed;
    }
    const SelectStatement& head = *branches[0];
    const size_t n_cols = head.SelectList().size();
    if (n_cols == 0) {
      return changed;
    }
    std::unordered_map<std::string, size_t> u_out_index;
    for (size_t c = 0; c < n_cols; ++c) {
      const NamedExpression& ne = head.SelectList()[c];
      std::string nm = ne.name;
      if (nm.empty() && ne.expression &&
          ne.expression->Type() == TypeTag::kColumnValue) {
        nm = ne.expression->AsColumnValue().GetColumnName().name;
      }
      if (nm.empty() || nm == "*") {
        return changed;
      }
      if (!u_out_index.emplace(AsciiLower(nm), c).second) {
        return changed;
      }
    }
    struct BranchInfo {
      std::string table;
      std::string orig_alias;
      std::vector<std::string> col_names;
      Expression where_clause;
    };
    std::vector<BranchInfo> b_infos;
    b_infos.reserve(branches.size());
    for (const SelectStatement* br : branches) {
      if (br->Sources().size() != 1 || br->Sources()[0].query != nullptr ||
          br->Sources()[0].unnest || br->Sources()[0].sample ||
          !br->GroupBy().empty() || br->Having() != nullptr || br->Distinct() ||
          br->HasLimit() || br->Offset() != 0 || !br->OrderBy().empty() ||
          !br->WithQueries().empty() || br->Qualify() != nullptr ||
          br->SelectList().size() != n_cols) {
        return changed;
      }
      const SelectSource& bsrc = br->Sources()[0];
      if (bsrc.table.empty()) {
        return changed;
      }
      StatusOr<std::shared_ptr<Table>> tbl = ctx.GetTable(bsrc.table);
      if (!tbl.HasValue()) {
        return changed;
      }
      const Schema& bschema = tbl.Value()->GetSchema();
      BranchInfo info;
      info.table = bsrc.table;
      info.orig_alias = bsrc.alias.empty() ? bsrc.table : bsrc.alias;
      info.where_clause = br->WhereClause();
      info.col_names.reserve(n_cols);
      for (size_t c = 0; c < n_cols; ++c) {
        const Expression& e = br->SelectList()[c].expression;
        if (!e || e->Type() != TypeTag::kColumnValue) {
          return changed;
        }
        const ColumnName& cn = e->AsColumnValue().GetColumnName();
        if (cn.name.empty() || cn.name == "*") {
          return changed;
        }
        if (!cn.schema.empty() &&
            AsciiLower(cn.schema) != AsciiLower(info.orig_alias) &&
            AsciiLower(cn.schema) != AsciiLower(info.table)) {
          return changed;
        }
        bool exists = false;
        for (size_t sc = 0; sc < bschema.ColumnCount(); ++sc) {
          if (AsciiLower(bschema.GetColumn(sc).Name().name) ==
              AsciiLower(cn.name)) {
            exists = true;
            break;
          }
        }
        if (!exists) {
          return changed;
        }
        info.col_names.push_back(cn.name);
      }
      b_infos.push_back(std::move(info));
    }
    std::unordered_set<std::string> other_cols;
    for (size_t i = 0; i < stmt->Sources().size(); ++i) {
      if (i == u_idx) {
        continue;
      }
      const SelectSource& osrc = stmt->Sources()[i];
      if (osrc.query == nullptr && !osrc.table.empty()) {
        StatusOr<std::shared_ptr<Table>> ot = ctx.GetTable(osrc.table);
        if (ot.HasValue()) {
          const Schema& os = ot.Value()->GetSchema();
          for (size_t sc = 0; sc < os.ColumnCount(); ++sc) {
            other_cols.insert(AsciiLower(os.GetColumn(sc).Name().name));
          }
        }
      } else if (osrc.query != nullptr) {
        for (const NamedExpression& ne : osrc.query->SelectList()) {
          std::string nm = ne.name;
          if (nm.empty() && ne.expression &&
              ne.expression->Type() == TypeTag::kColumnValue) {
            nm = ne.expression->AsColumnValue().GetColumnName().name;
          }
          if (!nm.empty() && nm != "*") {
            other_cols.insert(AsciiLower(nm));
          }
        }
      }
    }
    // Never shadow existing source aliases with the generated branch and
    // wrapper names; the rebind below relies on them being unambiguous.
    std::unordered_set<std::string> used_aliases;
    for (const SelectSource& s : stmt->Sources()) {
      if (!s.alias.empty()) {
        used_aliases.insert(AsciiLower(s.alias));
      }
      if (!s.table.empty()) {
        used_aliases.insert(AsciiLower(s.table));
      }
    }
    if (used_aliases.contains("__dg_union")) {
      return changed;
    }
    for (size_t bi = 0; bi < b_infos.size(); ++bi) {
      if (used_aliases.contains(AsciiLower("__dg_b" + std::to_string(bi)))) {
        return changed;
      }
    }
    const std::string u_alias_low = AsciiLower(u_alias);
    bool rewrite_ok = true;
    auto rewrite_expr_for_branch = [&](const Expression& expr,
                                       const std::string& br_alias,
                                       const BranchInfo& bi) -> Expression {
      return map_expr(expr, [&](const Expression& node) -> Expression {
        if (node->Type() == TypeTag::kColumnValue) {
          const ColumnName& cn = node->AsColumnValue().GetColumnName();
          const std::string tlow = AsciiLower(cn.schema);
          const std::string nlow = AsciiLower(cn.name);
          if (tlow == u_alias_low) {
            auto it = u_out_index.find(nlow);
            if (it == u_out_index.end()) {
              rewrite_ok = false;
              return node;
            }
            return ColumnValueExp(
                ColumnName(br_alias, bi.col_names[it->second]));
          }
          if (tlow.empty()) {
            auto it = u_out_index.find(nlow);
            if (it != u_out_index.end()) {
              if (other_cols.contains(nlow)) {
                rewrite_ok = false;
                return node;
              }
              return ColumnValueExp(
                  ColumnName(br_alias, bi.col_names[it->second]));
            }
          }
        }
        return node;
      });
    };
    auto rewrite_branch_where = [&](const Expression& expr,
                                    const std::string& br_alias,
                                    const BranchInfo& bi) -> Expression {
      const std::string oalow = AsciiLower(bi.orig_alias);
      const std::string tlow_b = AsciiLower(bi.table);
      return map_expr(expr, [&](const Expression& node) -> Expression {
        if (node->Type() == TypeTag::kColumnValue) {
          const ColumnName& cn = node->AsColumnValue().GetColumnName();
          const std::string tl = AsciiLower(cn.schema);
          if (tl.empty() || tl == oalow || tl == tlow_b) {
            return ColumnValueExp(ColumnName(br_alias, cn.name));
          }
        }
        return node;
      });
    };

    std::vector<std::shared_ptr<SelectStatement>> built_branches;
    built_branches.reserve(b_infos.size());
    for (size_t bi = 0; bi < b_infos.size(); ++bi) {
      const BranchInfo& info = b_infos[bi];
      const std::string br_alias = "__dg_b" + std::to_string(bi);
      auto br_stmt = std::make_shared<SelectStatement>();
      std::vector<NamedExpression> br_select;
      std::vector<Expression> br_group_by;
      br_group_by.reserve(group_cols.size());
      for (size_t gi = 0; gi < group_cols.size(); ++gi) {
        Expression ge = rewrite_expr_for_branch(ColumnValueExp(group_cols[gi]),
                                                br_alias, info);
        br_select.emplace_back("__dg_k" + std::to_string(gi), ge);
        br_group_by.push_back(std::move(ge));
      }
      for (size_t si = 0; si < out_specs.size(); ++si) {
        if (!out_specs[si].is_agg) {
          continue;
        }
        Expression child =
            rewrite_expr_for_branch(out_specs[si].agg_child, br_alias, info);
        br_select.emplace_back(
            "__dg_a" + std::to_string(si),
            std::make_shared<AggregateExpression>(out_specs[si].agg_type,
                                                  std::move(child), false));
      }
      br_stmt->SetSelectList(std::move(br_select));
      br_stmt->SetGroupBy(std::move(br_group_by));
      std::vector<SelectSource> br_sources;
      br_sources.reserve(stmt->Sources().size());
      std::vector<Expression> extra_preds;
      for (size_t i = 0; i < stmt->Sources().size(); ++i) {
        if (i == u_idx) {
          br_sources.emplace_back(info.table, br_alias, nullptr,
                                  JoinType::kCross, nullptr);
          if (stmt->Sources()[i].join_condition != nullptr) {
            extra_preds.push_back(rewrite_expr_for_branch(
                stmt->Sources()[i].join_condition, br_alias, info));
          }
        } else {
          SelectSource copy = stmt->Sources()[i];
          if (copy.join_condition != nullptr) {
            copy.join_condition =
                rewrite_expr_for_branch(copy.join_condition, br_alias, info);
          }
          br_sources.push_back(std::move(copy));
        }
      }
      br_stmt->SetSources(std::move(br_sources));
      std::vector<Expression> where_parts;
      if (stmt->WhereClause() != nullptr) {
        where_parts.push_back(
            rewrite_expr_for_branch(stmt->WhereClause(), br_alias, info));
      }
      if (info.where_clause != nullptr) {
        where_parts.push_back(
            rewrite_branch_where(info.where_clause, br_alias, info));
      }
      for (auto& ep : extra_preds) {
        where_parts.push_back(std::move(ep));
      }
      if (!where_parts.empty()) {
        br_stmt->SetWhereClause(CombineAndConjuncts(where_parts));
      }
      if (!rewrite_ok) {
        return changed;
      }
      built_branches.push_back(std::move(br_stmt));
    }

    auto union_sub = built_branches[0];
    for (size_t bi = 1; bi < built_branches.size(); ++bi) {
      union_sub->AddUnionAll(built_branches[bi]);
    }
    const std::string wrap_alias = "__dg_union";
    std::vector<NamedExpression> outer_select;
    outer_select.reserve(out_specs.size());
    for (size_t si = 0; si < out_specs.size(); ++si) {
      if (!out_specs[si].is_agg) {
        outer_select.emplace_back(
            out_specs[si].out_name,
            ColumnValueExp(ColumnName(
                wrap_alias,
                "__dg_k" + std::to_string(out_specs[si].group_idx))));
      } else {
        outer_select.emplace_back(
            out_specs[si].out_name,
            std::make_shared<AggregateExpression>(
                out_specs[si].agg_type,
                ColumnValueExp(
                    ColumnName(wrap_alias, "__dg_a" + std::to_string(si))),
                false));
      }
    }
    std::vector<Expression> outer_group_by;
    outer_group_by.reserve(group_cols.size());
    for (size_t gi = 0; gi < group_cols.size(); ++gi) {
      outer_group_by.push_back(ColumnValueExp(
          ColumnName(wrap_alias, "__dg_k" + std::to_string(gi))));
    }
    if (u_src.query == nullptr && !u_src.table.empty() &&
        std::ranges::any_of(root->WithQueries(), [&](const auto& wq) {
          return IdentifierEqualsCI(wq.first, u_src.table);
        })) {
      consumed_ctes.insert(AsciiLower(u_src.table));
    }
    stmt->SetSelectList(std::move(outer_select));
    stmt->SetSources({SelectSource("", wrap_alias, std::move(union_sub),
                                   JoinType::kCross, nullptr)});
    stmt->SetWhereClause(nullptr);
    stmt->SetGroupBy(std::move(outer_group_by));
    return true;
  };

  std::unordered_map<std::string, const SelectStatement*> empty_env;
  const bool did_rewrite = rewrite_one(root, empty_env);
  if (did_rewrite && !consumed_ctes.empty() && !root->WithQueries().empty()) {
    std::unordered_set<std::string> live_refs;
    std::function<void(const Expression&)> walk_expr =
        [&](const Expression& e) {
          if (!e) {
            return;
          }
          if (e->Type() == TypeTag::kQueryExp) {
            const auto& qe = e->AsQueryExpression();
            if (qe.Query() != nullptr) {
              // Conservative: mark all consumed_ctes as live if any subquery
              // exists in an expression we don't recursively inspect deeply.
              for (const std::string& c : consumed_ctes) {
                live_refs.insert(c);
              }
            }
          }
          for (const Expression& ch : ExpressionChildren(e)) {
            walk_expr(ch);
          }
        };
    std::function<void(const SelectStatement*)> walk_stmt =
        [&](const SelectStatement* s) {
          if (s == nullptr) {
            return;
          }
          for (const SelectSource& src : s->Sources()) {
            if (src.query != nullptr) {
              walk_stmt(src.query.get());
            } else if (!src.table.empty()) {
              live_refs.insert(AsciiLower(src.table));
            }
            walk_expr(src.join_condition);
          }
          for (const auto& br : s->UnionAll()) {
            walk_stmt(br.get());
          }
          if (const auto& tree = s->GetSetOperationTree(); tree != nullptr) {
            walk_stmt(tree->first.get());
            for (const auto& br : tree->branches) {
              walk_stmt(br.get());
            }
          }
          for (const NamedExpression& ne : s->SelectList()) {
            walk_expr(ne.expression);
          }
          walk_expr(s->WhereClause());
          walk_expr(s->Having());
          walk_expr(s->Qualify());
          for (const Expression& expression : s->GroupBy()) {
            walk_expr(expression);
          }
          for (const auto& term : s->OrderBy()) {
            walk_expr(term.expression);
          }
          for (const Expression& expression : s->DistinctOn()) {
            walk_expr(expression);
          }
        };
    walk_stmt(root);
    // Inline CTE bodies may still be referenced from elsewhere (e.g. a
    // consumed CTE A that references another consumed CTE B). Walk every
    // body so transitive references are recorded as live; keeping an extra
    // unused CTE definition is harmless, dropping a needed one is not.
    for (const auto& [wname, wbody] : root->WithQueries()) {
      (void)wname;
      walk_stmt(wbody.get());
    }
    std::vector<std::string> to_remove;
    for (const auto& [wname, wbody] : root->WithQueries()) {
      (void)wbody;
      const std::string low = AsciiLower(wname);
      if (consumed_ctes.contains(low) && !live_refs.contains(low) &&
          !root->IsRecursiveWith(wname)) {
        to_remove.push_back(wname);
      }
    }
    for (const std::string& wname : to_remove) {
      root->RemoveWithQuery(wname);
    }
  }
  return did_rewrite;
}

bool PushTransitiveGroupedSubqueryDimSemiFilter(SelectStatement* root,
                                                TransactionContext& ctx) {
  if (root == nullptr) {
    return false;
  }
  auto has_bad_node = [](const Expression& e) -> bool {
    bool bad = false;
    std::function<void(const Expression&)> walk = [&](const Expression& node) {
      if (!node || bad) {
        return;
      }
      if (node->Type() == TypeTag::kQueryExp ||
          node->Type() == TypeTag::kAggregateExp ||
          node->Type() == TypeTag::kWindowFunctionExp) {
        bad = true;
        return;
      }
      for (const Expression& ch : ExpressionChildren(node)) {
        walk(ch);
      }
    };
    walk(e);
    return bad;
  };
  auto map_expr = [](const Expression& root_e, auto&& fn) -> Expression {
    std::function<Expression(const Expression&)> rec =
        [&](const Expression& node) -> Expression {
      if (!node) {
        return nullptr;
      }
      std::vector<Expression> children = ExpressionChildren(node);
      bool ch_changed = false;
      for (Expression& ch : children) {
        Expression next = rec(ch);
        if (next.get() != ch.get()) {
          ch_changed = true;
          ch = std::move(next);
        }
      }
      Expression rebuilt =
          ch_changed ? WithExpressionChildren(node, std::move(children)) : node;
      return fn(rebuilt);
    };
    return rec(root_e);
  };

  // Count CTE references across `root` so we only restrict single-use CTEs
  // here (shared CTEs are handled by PushSharedGroupedCteKeyRestrictions).
  std::unordered_map<std::string, size_t> cte_ref_counts;
  {
    std::function<void(const SelectStatement*)> count_refs =
        [&](const SelectStatement* s) {
          if (s == nullptr) {
            return;
          }
          for (const auto& [wn, wb] : s->WithQueries()) {
            (void)wn;
            count_refs(wb.get());
          }
          for (const SelectSource& src : s->Sources()) {
            if (src.query != nullptr) {
              count_refs(src.query.get());
            } else if (!src.table.empty()) {
              ++cte_ref_counts[AsciiLower(src.table)];
            }
          }
          for (const auto& br : s->UnionAll()) {
            count_refs(br.get());
          }
          if (const auto& tree = s->GetSetOperationTree(); tree != nullptr) {
            count_refs(tree->first.get());
            for (const auto& br : tree->branches) {
              count_refs(br.get());
            }
          }
        };
    count_refs(root);
  }

  std::function<bool(SelectStatement*,
                     const std::unordered_map<std::string, SelectStatement*>&)>
      process_stmt =
          [&](SelectStatement* stmt,
              const std::unordered_map<std::string, SelectStatement*>& cte_env)
      -> bool {
    if (stmt == nullptr) {
      return false;
    }
    bool changed = false;
    auto local_env = cte_env;
    for (const std::string& wn : stmt->WithQueryOrder()) {
      auto it = stmt->WithQueries().find(wn);
      if (it != stmt->WithQueries().end() && it->second != nullptr) {
        if (process_stmt(it->second.get(), local_env)) {
          changed = true;
        }
        if (!stmt->IsRecursiveWith(wn)) {
          local_env[AsciiLower(wn)] = it->second.get();
        }
      }
    }
    for (const auto& br : stmt->UnionAll()) {
      if (br != nullptr && process_stmt(br.get(), local_env)) {
        changed = true;
      }
    }
    for (const auto& src : stmt->Sources()) {
      if (src.query != nullptr && process_stmt(src.query.get(), local_env)) {
        changed = true;
      }
    }
    if (stmt->Sources().size() < 2) {
      return changed;
    }
    for (const SelectSource& src : stmt->Sources()) {
      if ((src.join_type != JoinType::kCross &&
           src.join_type != JoinType::kInner) ||
          src.unnest || src.is_lateral || src.sample || src.from_nested_join ||
          !src.using_columns.empty()) {
        return changed;
      }
    }

    const size_t nsrc = stmt->Sources().size();
    struct SrcMeta {
      std::string alias_low;
      std::string table_low;
      std::shared_ptr<Table> base_table;
      SelectStatement* grouped_target{nullptr};
      SelectStatement* union_target{nullptr};
      std::unordered_map<std::string, std::string> cols;  // low -> canon
    };
    std::vector<SrcMeta> metas(nsrc);
    for (size_t i = 0; i < nsrc; ++i) {
      const SelectSource& src = stmt->Sources()[i];
      metas[i].alias_low =
          AsciiLower(src.alias.empty() ? src.table : src.alias);
      metas[i].table_low = AsciiLower(src.table);
      if (src.query != nullptr) {
        SelectStatement* q = src.query.get();
        if (!q->GroupBy().empty() && !q->Distinct() && !q->HasDistinctOn() &&
            !q->HasLimit() && q->Offset() == 0 && q->UnionAll().empty() &&
            q->GetSetOperationTree() == nullptr && q->WithQueries().empty()) {
          metas[i].grouped_target = q;
        } else if ((!q->UnionAll().empty() ||
                    q->GetSetOperationTree() != nullptr) &&
                   !q->HasLimit() && q->Offset() == 0 &&
                   q->WithQueries().empty()) {
          metas[i].union_target = q;
        }
        for (const NamedExpression& ne : q->SelectList()) {
          std::string nm = ne.name;
          if (nm.empty() && ne.expression &&
              ne.expression->Type() == TypeTag::kColumnValue) {
            nm = ne.expression->AsColumnValue().GetColumnName().name;
          }
          if (!nm.empty() && nm != "*") {
            metas[i].cols[AsciiLower(nm)] = nm;
          }
        }
      } else if (!src.table.empty()) {
        auto cit = local_env.find(metas[i].table_low);
        if (cit != local_env.end() && cit->second != nullptr) {
          SelectStatement* q = cit->second;
          if (cte_ref_counts[metas[i].table_low] == 1 &&
              !q->GroupBy().empty() && !q->Distinct() && !q->HasDistinctOn() &&
              !q->HasLimit() && q->Offset() == 0 && q->UnionAll().empty() &&
              q->GetSetOperationTree() == nullptr && q->WithQueries().empty()) {
            metas[i].grouped_target = q;
          }
          for (const NamedExpression& ne : q->SelectList()) {
            std::string nm = ne.name;
            if (nm.empty() && ne.expression &&
                ne.expression->Type() == TypeTag::kColumnValue) {
              nm = ne.expression->AsColumnValue().GetColumnName().name;
            }
            if (!nm.empty() && nm != "*") {
              metas[i].cols[AsciiLower(nm)] = nm;
            }
          }
        } else {
          StatusOr<std::shared_ptr<Table>> t = ctx.GetTable(src.table);
          if (t.HasValue() && t.Value() != nullptr) {
            metas[i].base_table = t.Value();
            const Schema& sc = t.Value()->GetSchema();
            for (size_t c = 0; c < sc.ColumnCount(); ++c) {
              const std::string& cname = sc.GetColumn(c).Name().name;
              metas[i].cols[AsciiLower(cname)] = cname;
            }
          }
        }
      }
    }

    // Generated subquery aliases must not collide with an existing source
    // alias or table name in this scope.
    auto fresh_src_alias = [&](const std::string& base) {
      auto in_use = [&](const std::string& low) {
        return std::ranges::any_of(metas, [&](const auto& meta) {
          return meta.alias_low == low || meta.table_low == low;
        });
      };
      if (!in_use(AsciiLower(base))) {
        return base;
      }
      for (size_t seq = 1;; ++seq) {
        const std::string cand = base + "_" + std::to_string(seq);
        if (!in_use(AsciiLower(cand))) {
          return cand;
        }
      }
    };

    auto resolve_col = [&](const ColumnName& cn, size_t* src_out,
                           std::string* col_low_out) -> bool {
      const std::string nlow = AsciiLower(cn.name);
      if (nlow.empty() || nlow == "*") {
        return false;
      }
      const std::string slow = AsciiLower(cn.schema);
      size_t hits = 0;
      size_t hit_idx = 0;
      for (size_t i = 0; i < nsrc; ++i) {
        if (!slow.empty() && slow != metas[i].alias_low &&
            slow != metas[i].table_low) {
          continue;
        }
        if (metas[i].cols.contains(nlow)) {
          hit_idx = i;
          ++hits;
        }
      }
      if (hits != 1) {
        return false;
      }
      *src_out = hit_idx;
      *col_low_out = nlow;
      return true;
    };

    std::vector<Expression> conjs = SplitAndConjuncts(stmt->WhereClause());
    for (const SelectSource& src : stmt->Sources()) {
      if (src.join_condition != nullptr) {
        for (Expression& jc : SplitAndConjuncts(src.join_condition)) {
          conjs.push_back(std::move(jc));
        }
      }
    }

    // Union-Find over `(src_idx, col_low)` equality keys.
    std::unordered_map<std::string, size_t> node_id;
    std::vector<std::pair<size_t, std::string>> node_info;
    std::vector<size_t> uf_parent;
    auto get_node = [&](size_t sidx, const std::string& clow) -> size_t {
      const std::string key = std::to_string(sidx) + ":" + clow;
      auto [it, ins] = node_id.emplace(key, node_info.size());
      if (ins) {
        node_info.emplace_back(sidx, clow);
        uf_parent.push_back(it->second);
      }
      return it->second;
    };
    auto uf_find = [&](size_t x) -> size_t {
      while (uf_parent[x] != x) {
        uf_parent[x] = uf_parent[uf_parent[x]];
        x = uf_parent[x];
      }
      return x;
    };
    auto uf_union = [&](size_t a, size_t b) {
      a = uf_find(a);
      b = uf_find(b);
      if (a != b) {
        uf_parent[a] = b;
      }
    };

    std::vector<std::vector<Expression>> dim_local_preds(nsrc);
    for (const Expression& c : conjs) {
      if (!c || has_bad_node(c)) {
        continue;
      }
      if (c->Type() == TypeTag::kBinaryExp &&
          c->AsBinaryExpression().Op() == BinaryOperation::kEquals) {
        const Expression& l = c->AsBinaryExpression().Left();
        const Expression& r = c->AsBinaryExpression().Right();
        if (l && r && l->Type() == TypeTag::kColumnValue &&
            r->Type() == TypeTag::kColumnValue) {
          size_t ls = 0;
          size_t rs = 0;
          std::string lc;
          std::string rc;
          if (resolve_col(l->AsColumnValue().GetColumnName(), &ls, &lc) &&
              resolve_col(r->AsColumnValue().GetColumnName(), &rs, &rc)) {
            uf_union(get_node(ls, lc), get_node(rs, rc));
            continue;
          }
        }
      }
      // Single-source local predicate?
      const auto touched = c->TouchedColumns();
      if (touched.empty()) {
        continue;
      }
      size_t only_src = nsrc;
      bool single_ok = true;
      for (const ColumnName& cn : touched) {
        size_t sidx = 0;
        std::string clow;
        if (!resolve_col(cn, &sidx, &clow)) {
          single_ok = false;
          break;
        }
        if (only_src == nsrc) {
          only_src = sidx;
        } else if (only_src != sidx) {
          single_ok = false;
          break;
        }
      }
      if (single_ok && only_src < nsrc &&
          metas[only_src].base_table != nullptr &&
          metas[only_src].base_table->BackingColumnarRowCount() <= 250000) {
        dim_local_preds[only_src].push_back(c);
      }
    }

    // Try pushing a selective dimension semi-filter into each grouped target.
    for (size_t g_idx = 0; g_idx < nsrc; ++g_idx) {
      SelectStatement* g = metas[g_idx].grouped_target;
      if (g == nullptr) {
        continue;
      }
      bool g_sources_ok = !g->Sources().empty();
      bool g_has_large_table = false;
      std::vector<std::shared_ptr<Table>> g_tables(g->Sources().size());
      for (size_t gi = 0; gi < g->Sources().size(); ++gi) {
        const SelectSource& gs = g->Sources()[gi];
        if ((gs.join_type != JoinType::kCross &&
             gs.join_type != JoinType::kInner) ||
            gs.unnest || gs.is_lateral || gs.sample || gs.from_nested_join ||
            !gs.using_columns.empty()) {
          g_sources_ok = false;
          break;
        }
        if (gs.query == nullptr && !gs.table.empty()) {
          StatusOr<std::shared_ptr<Table>> gt = ctx.GetTable(gs.table);
          if (gt.HasValue() && gt.Value() != nullptr) {
            g_tables[gi] = gt.Value();
            if (gt.Value()->BackingColumnarRowCount() >= 200000) {
              g_has_large_table = true;
            }
          }
        }
      }
      if (!g_sources_ok || !g_has_large_table) {
        continue;
      }

      // Map GROUP BY column names inside `g` to their output names.
      for (const Expression& ge : g->GroupBy()) {
        if (!ge || ge->Type() != TypeTag::kColumnValue) {
          continue;
        }
        const ColumnName& gk_cn = ge->AsColumnValue().GetColumnName();
        const std::string gk_low = AsciiLower(gk_cn.name);
        std::string g_out_low;
        for (const NamedExpression& ne : g->SelectList()) {
          if (ne.expression && ne.expression->Type() == TypeTag::kColumnValue) {
            const ColumnName& scn =
                ne.expression->AsColumnValue().GetColumnName();
            if (AsciiLower(scn.name) == gk_low &&
                (gk_cn.schema.empty() || scn.schema.empty() ||
                 AsciiLower(gk_cn.schema) == AsciiLower(scn.schema))) {
              g_out_low = AsciiLower(ne.name.empty() ? scn.name : ne.name);
              break;
            }
          }
        }
        if (g_out_low.empty()) {
          continue;
        }
        const std::string g_node_key = std::to_string(g_idx) + ":" + g_out_low;
        auto nit = node_id.find(g_node_key);
        if (nit == node_id.end()) {
          continue;
        }
        const size_t root_id = uf_find(nit->second);

        // Find a selective dimension table in the same equivalence class.
        size_t best_d = nsrc;
        std::string best_d_col;
        for (size_t nid = 0; nid < node_info.size(); ++nid) {
          if (uf_find(nid) != root_id) {
            continue;
          }
          const size_t cand_s = node_info[nid].first;
          if (cand_s == g_idx || metas[cand_s].base_table == nullptr ||
              dim_local_preds[cand_s].empty()) {
            continue;
          }
          const std::string& clow = node_info[nid].second;
          auto col_it = metas[cand_s].cols.find(clow);
          if (col_it == metas[cand_s].cols.end()) {
            continue;
          }
          const Schema& dsc = metas[cand_s].base_table->GetSchema();
          const int doff = dsc.Offset(ColumnName("", col_it->second));
          if (doff < 0) {
            continue;
          }
          const Column& dc = dsc.GetColumn(static_cast<size_t>(doff));
          if (dc.IsUnsigned() || (dc.Type() != ValueType::kInt64 &&
                                  dc.Type() != ValueType::kDate)) {
            continue;
          }
          best_d = cand_s;
          best_d_col = col_it->second;
          break;
        }
        if (best_d >= nsrc) {
          continue;
        }

        // Collect all inner columns of `g` equal to `gk_cn` (including
        // equi-join partners inside `g->WhereClause()`).
        auto resolve_g_col = [&](const ColumnName& cn,
                                 ColumnName* out) -> bool {
          const std::string nl = AsciiLower(cn.name);
          const std::string sl = AsciiLower(cn.schema);
          size_t hits = 0;
          ColumnName found;
          for (size_t gi = 0; gi < g->Sources().size(); ++gi) {
            if (g_tables[gi] == nullptr) {
              continue;
            }
            const SelectSource& gs = g->Sources()[gi];
            const std::string gal = gs.alias.empty() ? gs.table : gs.alias;
            if (!sl.empty() && sl != AsciiLower(gal) &&
                sl != AsciiLower(gs.table)) {
              continue;
            }
            const Schema& gsc = g_tables[gi]->GetSchema();
            for (size_t c = 0; c < gsc.ColumnCount(); ++c) {
              if (AsciiLower(gsc.GetColumn(c).Name().name) == nl) {
                found = ColumnName(gal, gsc.GetColumn(c).Name().name);
                ++hits;
                break;
              }
            }
          }
          if (hits != 1) {
            return false;
          }
          *out = std::move(found);
          return true;
        };

        ColumnName root_g_col;
        if (!resolve_g_col(gk_cn, &root_g_col)) {
          continue;
        }
        std::vector<ColumnName> target_g_cols = {root_g_col};
        std::vector<Expression> g_conjs = SplitAndConjuncts(g->WhereClause());
        for (const SelectSource& gs : g->Sources()) {
          if (gs.join_condition != nullptr) {
            for (Expression& jc : SplitAndConjuncts(gs.join_condition)) {
              g_conjs.push_back(std::move(jc));
            }
          }
        }
        // Skip if `g` already has an IN-subquery on `root_g_col`.
        bool already_filtered = false;
        for (const Expression& gc : g_conjs) {
          if (gc && gc->Type() == TypeTag::kQueryExp) {
            const auto& qe = gc->AsQueryExpression();
            if (!qe.Exists() && !qe.Negated() && qe.Test() &&
                qe.Test()->Type() == TypeTag::kColumnValue) {
              ColumnName tcn;
              if (resolve_g_col(qe.Test()->AsColumnValue().GetColumnName(),
                                &tcn) &&
                  AsciiLower(tcn.schema) == AsciiLower(root_g_col.schema) &&
                  AsciiLower(tcn.name) == AsciiLower(root_g_col.name)) {
                already_filtered = true;
                break;
              }
            }
          }
          if (gc && gc->Type() == TypeTag::kBinaryExp &&
              gc->AsBinaryExpression().Op() == BinaryOperation::kEquals) {
            const Expression& l = gc->AsBinaryExpression().Left();
            const Expression& r = gc->AsBinaryExpression().Right();
            if (l && r && l->Type() == TypeTag::kColumnValue &&
                r->Type() == TypeTag::kColumnValue) {
              ColumnName lcn;
              ColumnName rcn;
              if (resolve_g_col(l->AsColumnValue().GetColumnName(), &lcn) &&
                  resolve_g_col(r->AsColumnValue().GetColumnName(), &rcn)) {
                auto same_col = [](const ColumnName& a, const ColumnName& b) {
                  return AsciiLower(a.schema) == AsciiLower(b.schema) &&
                         AsciiLower(a.name) == AsciiLower(b.name);
                };
                if (same_col(lcn, root_g_col) && !same_col(rcn, root_g_col)) {
                  target_g_cols.push_back(rcn);
                } else if (same_col(rcn, root_g_col) &&
                           !same_col(lcn, root_g_col)) {
                  target_g_cols.push_back(lcn);
                }
              }
            }
          }
        }
        if (already_filtered) {
          continue;
        }

        const std::string sub_alias = fresh_src_alias("__tsf_d");
        std::vector<Expression> sub_preds;
        sub_preds.reserve(dim_local_preds[best_d].size());
        for (const Expression& dp : dim_local_preds[best_d]) {
          sub_preds.push_back(map_expr(dp, [&](const Expression& node) {
            if (node->Type() == TypeTag::kColumnValue) {
              const ColumnName& cn = node->AsColumnValue().GetColumnName();
              return ColumnValueExp(ColumnName(sub_alias, cn.name));
            }
            return node;
          }));
        }
        std::vector<Expression> new_g_where;
        if (g->WhereClause() != nullptr) {
          new_g_where.push_back(g->WhereClause());
        }
        for (const ColumnName& tcol : target_g_cols) {
          auto in_sub = std::make_shared<SelectStatement>();
          in_sub->SetSources(
              {SelectSource(stmt->Sources()[best_d].table, sub_alias, nullptr,
                            JoinType::kCross, nullptr)});
          in_sub->SetWhereClause(CombineAndConjuncts(sub_preds));
          in_sub->SetSelectList({NamedExpression(
              "", ColumnValueExp(ColumnName(sub_alias, best_d_col)))});
          new_g_where.push_back(QueryExpressionExp(
              std::move(in_sub), ColumnValueExp(tcol), /*exists=*/false,
              /*negated=*/false));
        }
        g->SetWhereClause(CombineAndConjuncts(new_g_where));
        changed = true;
      }
    }

    // Also push selective dimension semi-filters into non-grouped UNION ALL /
    // UNION DISTINCT derived tables (e.g. TPC-DS Q71: item.i_manager_id = 1
    // and time_dim.t_meal_time IN (...) pushed into 3-way UNION ALL).
    // Lean proof: lean/TinylambOpt/VectorizedJoinAndPruning.lean
    //   (union_semi_filter_pushdown_sound)
    for (size_t u_idx = 0; u_idx < nsrc; ++u_idx) {
      SelectStatement* u = metas[u_idx].union_target;
      if (u == nullptr) {
        continue;
      }
      std::vector<SelectStatement*> leaves;
      auto collect_union_leaves = [&](auto& self, SelectStatement* s) -> bool {
        if (s == nullptr) {
          return false;
        }
        if (const auto& tree = s->GetSetOperationTree(); tree != nullptr) {
          for (SetOperationKind k : tree->kinds) {
            if (k != SetOperationKind::kUnionAll &&
                k != SetOperationKind::kUnion) {
              return false;
            }
          }
          if (!self(self, tree->first.get())) {
            return false;
          }
          for (const auto& br : tree->branches) {
            if (!self(self, br.get())) {
              return false;
            }
          }
          return true;
        }
        if (s->UnionAll().empty()) {
          leaves.push_back(s);
          return true;
        }
        for (auto k : s->SetOperationKinds()) {
          if (k != SetOperationKind::kUnionAll &&
              k != SetOperationKind::kUnion) {
            return false;
          }
        }
        leaves.push_back(s);
        for (const auto& br : s->UnionAll()) {
          if (!self(self, br.get())) {
            return false;
          }
        }
        return true;
      };
      if (!collect_union_leaves(collect_union_leaves, u) || leaves.size() < 2) {
        continue;
      }
      const size_t n_cols = leaves[0]->SelectList().size();
      bool all_leaves_ok = (n_cols > 0);
      bool any_large_table = false;
      std::vector<std::vector<std::shared_ptr<Table>>> leaf_tables(
          leaves.size());
      for (size_t li = 0; li < leaves.size() && all_leaves_ok; ++li) {
        SelectStatement* br = leaves[li];
        if (!br->GroupBy().empty() || br->IsGroupAll() || br->Having() ||
            br->HasLimit() || br->Offset() != 0 || !br->OrderBy().empty() ||
            br->Qualify() || br->WithTies() || !br->WithQueries().empty() ||
            br->SelectList().size() != n_cols || br->Sources().empty()) {
          all_leaves_ok = false;
          break;
        }
        leaf_tables[li].resize(br->Sources().size());
        for (size_t si = 0; si < br->Sources().size(); ++si) {
          const SelectSource& bs = br->Sources()[si];
          if ((bs.join_type != JoinType::kCross &&
               bs.join_type != JoinType::kInner) ||
              bs.query != nullptr || bs.table.empty() || bs.unnest ||
              bs.is_lateral || bs.sample || bs.from_nested_join) {
            all_leaves_ok = false;
            break;
          }
          StatusOr<std::shared_ptr<Table>> bt = ctx.GetTable(bs.table);
          if (!bt.HasValue() || bt.Value() == nullptr) {
            all_leaves_ok = false;
            break;
          }
          leaf_tables[li][si] = bt.Value();
          if (bt.Value()->BackingColumnarRowCount() >= 200000) {
            any_large_table = true;
          }
        }
      }
      if (!all_leaves_ok || !any_large_table) {
        continue;
      }

      for (size_t ci = 0; ci < n_cols; ++ci) {
        const NamedExpression& ne0 = leaves[0]->SelectList()[ci];
        std::string out_nm = ne0.name;
        if (out_nm.empty() && ne0.expression &&
            ne0.expression->Type() == TypeTag::kColumnValue) {
          out_nm = ne0.expression->AsColumnValue().GetColumnName().name;
        }
        if (out_nm.empty() || out_nm == "*") {
          continue;
        }
        const std::string out_low = AsciiLower(out_nm);
        const std::string u_node_key = std::to_string(u_idx) + ":" + out_low;
        auto nit = node_id.find(u_node_key);
        if (nit == node_id.end()) {
          continue;
        }
        const size_t root_id = uf_find(nit->second);

        size_t best_d = nsrc;
        std::string best_d_col;
        for (size_t nid = 0; nid < node_info.size(); ++nid) {
          if (uf_find(nid) != root_id) {
            continue;
          }
          const size_t cand_s = node_info[nid].first;
          if (cand_s == u_idx || metas[cand_s].base_table == nullptr ||
              dim_local_preds[cand_s].empty()) {
            continue;
          }
          const std::string& clow = node_info[nid].second;
          auto col_it = metas[cand_s].cols.find(clow);
          if (col_it == metas[cand_s].cols.end()) {
            continue;
          }
          const Schema& dsc = metas[cand_s].base_table->GetSchema();
          const int doff = dsc.Offset(ColumnName("", col_it->second));
          if (doff < 0) {
            continue;
          }
          const Column& dc = dsc.GetColumn(static_cast<size_t>(doff));
          if (dc.IsUnsigned() || (dc.Type() != ValueType::kInt64 &&
                                  dc.Type() != ValueType::kDate)) {
            continue;
          }
          best_d = cand_s;
          best_d_col = col_it->second;
          break;
        }
        if (best_d >= nsrc) {
          continue;
        }

        std::vector<ColumnName> leaf_cols;
        leaf_cols.reserve(leaves.size());
        bool cols_ok = true;
        for (size_t li = 0; li < leaves.size(); ++li) {
          const Expression& bexpr = leaves[li]->SelectList()[ci].expression;
          if (!bexpr || bexpr->Type() != TypeTag::kColumnValue) {
            cols_ok = false;
            break;
          }
          const ColumnName& bcn = bexpr->AsColumnValue().GetColumnName();
          const std::string nl = AsciiLower(bcn.name);
          const std::string sl = AsciiLower(bcn.schema);
          size_t hits = 0;
          ColumnName resolved;
          for (size_t si = 0; si < leaves[li]->Sources().size(); ++si) {
            const SelectSource& bs = leaves[li]->Sources()[si];
            const std::string bal = bs.alias.empty() ? bs.table : bs.alias;
            if (!sl.empty() && sl != AsciiLower(bal) &&
                sl != AsciiLower(bs.table)) {
              continue;
            }
            const Schema& bsc = leaf_tables[li][si]->GetSchema();
            for (size_t k = 0; k < bsc.ColumnCount(); ++k) {
              if (AsciiLower(bsc.GetColumn(k).Name().name) == nl) {
                resolved = ColumnName(bal, bsc.GetColumn(k).Name().name);
                ++hits;
                break;
              }
            }
          }
          if (hits != 1) {
            cols_ok = false;
            break;
          }
          // Skip if leaf already has an IN-subquery on this column.
          for (const Expression& lc :
               SplitAndConjuncts(leaves[li]->WhereClause())) {
            if (lc && lc->Type() == TypeTag::kQueryExp) {
              const auto& qe = lc->AsQueryExpression();
              if (!qe.Exists() && !qe.Negated() && qe.Test() &&
                  qe.Test()->Type() == TypeTag::kColumnValue) {
                const ColumnName& tcn =
                    qe.Test()->AsColumnValue().GetColumnName();
                if (AsciiLower(tcn.name) == AsciiLower(resolved.name) &&
                    (tcn.schema.empty() ||
                     AsciiLower(tcn.schema) == AsciiLower(resolved.schema))) {
                  cols_ok = false;
                  break;
                }
              }
            }
          }
          if (!cols_ok) {
            break;
          }
          leaf_cols.push_back(std::move(resolved));
        }
        if (!cols_ok) {
          continue;
        }

        const std::string sub_alias = fresh_src_alias("__tsf_ud");
        std::vector<Expression> sub_preds;
        sub_preds.reserve(dim_local_preds[best_d].size());
        for (const Expression& dp : dim_local_preds[best_d]) {
          sub_preds.push_back(map_expr(dp, [&](const Expression& node) {
            if (node->Type() == TypeTag::kColumnValue) {
              const ColumnName& cn = node->AsColumnValue().GetColumnName();
              return ColumnValueExp(ColumnName(sub_alias, cn.name));
            }
            return node;
          }));
        }
        for (size_t li = 0; li < leaves.size(); ++li) {
          auto in_sub = std::make_shared<SelectStatement>();
          in_sub->SetSources(
              {SelectSource(stmt->Sources()[best_d].table, sub_alias, nullptr,
                            JoinType::kCross, nullptr)});
          in_sub->SetWhereClause(CombineAndConjuncts(sub_preds));
          in_sub->SetSelectList({NamedExpression(
              "", ColumnValueExp(ColumnName(sub_alias, best_d_col)))});
          Expression in_pred = QueryExpressionExp(
              std::move(in_sub), ColumnValueExp(leaf_cols[li]),
              /*exists=*/false, /*negated=*/false);
          leaves[li]->SetWhereClause(
              leaves[li]->WhereClause()
                  ? BinaryExpressionExp(leaves[li]->WhereClause(),
                                        BinaryOperation::kAnd,
                                        std::move(in_pred))
                  : std::move(in_pred));
        }
        // The IN predicate was added to every leaf individually, so the
        // container must keep its own WHERE (normally empty). Copying the
        // first leaf's WHERE here would apply that branch's own predicates
        // to the whole UNION ALL and drop rows from the other branches.
        changed = true;
      }
    }
    return changed;
  };

  std::unordered_map<std::string, SelectStatement*> empty_env;
  return process_stmt(root, empty_env);
}

// Rewrites a single-pass self-correlated scalar AVG/SUM/MIN/MAX comparison
// (e.g. TPC-DS Q32 / Q92:
//   SELECT SUM(f.v) FROM F, D_item, D_date
//   WHERE D_item.pk = F.fk AND D_date.pk = F.date_fk AND ...
//     AND f.v > (SELECT 1.3 * AVG(f2.v) FROM F AS f2, D_date AS d2
//                WHERE f2.fk = D_item.pk AND d2.pk = f2.date_fk AND ...))
// into a single-pass window derived table `AVG(f.v) OVER (PARTITION BY F.fk)`
// when `D_item.pk` is certified unique non-null and all non-correlated
// predicates on `{F, D_date}` match identically between outer and subquery.
// Lean proof: lean/TinylambOpt/VectorizedJoinAndPruning.lean
//   (correlated_self_avg_to_window_sound)
bool RewriteCorrelatedSelfAvgToWindow(SelectStatement* root,
                                      TransactionContext& ctx) {
  if (root == nullptr) {
    return false;
  }
  auto has_bad_node = [](const Expression& root_expr) -> bool {
    bool bad = false;
    std::function<void(const Expression&)> walk = [&](const Expression& e) {
      if (!e || bad) {
        return;
      }
      if (e->Type() == TypeTag::kWindowFunctionExp ||
          e->Type() == TypeTag::kQueryExp) {
        bad = true;
        return;
      }
      if (e->Type() == TypeTag::kConstantValue &&
          dynamic_cast<const ParameterSlot*>(e.get()) != nullptr) {
        bad = true;
        return;
      }
      if (e->Type() == TypeTag::kFunctionCallExp &&
          GetFunctionVolatility(e->AsFunctionCallExpression().FuncName()) !=
              Volatility::kImmutable) {
        bad = true;
        return;
      }
      for (const Expression& ch : ExpressionChildren(e)) {
        walk(ch);
      }
    };
    walk(root_expr);
    return bad;
  };
  auto map_expr = [](const Expression& root_expr,
                     const std::function<Expression(const Expression&)>& fn) {
    std::function<Expression(const Expression&)> rec =
        [&](const Expression& e) -> Expression {
      if (!e) {
        return e;
      }
      Expression mapped = fn(e);
      if (mapped.get() != e.get()) {
        return mapped;
      }
      const std::vector<Expression> ch = ExpressionChildren(e);
      if (ch.empty()) {
        return e;
      }
      std::vector<Expression> next;
      next.reserve(ch.size());
      bool diff = false;
      for (const Expression& c : ch) {
        Expression mc = rec(c);
        if (mc.get() != c.get()) {
          diff = true;
        }
        next.push_back(std::move(mc));
      }
      return diff ? WithExpressionChildren(e, std::move(next)) : e;
    };
    return rec(root_expr);
  };

  if (root->Sources().size() < 2 || !root->GroupBy().empty() ||
      root->Distinct() || root->HasDistinctOn() || root->Having() ||
      root->Qualify() || !root->UnionAll().empty() ||
      root->GetSetOperationTree() != nullptr || !root->WithQueries().empty() ||
      root->ForUpdate() || root->WhereClause() == nullptr) {
    return false;
  }
  const size_t nsrc = root->Sources().size();
  std::vector<std::shared_ptr<Table>> outer_tables(nsrc);
  std::vector<std::string> outer_alias_low(nsrc);
  std::vector<std::string> outer_table_low(nsrc);
  for (size_t i = 0; i < nsrc; ++i) {
    const SelectSource& src = root->Sources()[i];
    if ((src.join_type != JoinType::kCross &&
         src.join_type != JoinType::kInner) ||
        src.query != nullptr || src.table.empty() || src.unnest ||
        src.is_lateral || src.sample || src.from_nested_join ||
        !src.using_columns.empty() || src.join_condition != nullptr) {
      return false;
    }
    StatusOr<std::shared_ptr<Table>> t = ctx.GetTable(src.table);
    if (!t.HasValue() || t.Value() == nullptr) {
      return false;
    }
    outer_tables[i] = t.Value();
    outer_alias_low[i] = AsciiLower(src.alias.empty() ? src.table : src.alias);
    outer_table_low[i] = AsciiLower(src.table);
  }
  auto resolve_in = [&](const std::vector<std::shared_ptr<Table>>& tbls,
                        const std::vector<std::string>& alows,
                        const std::vector<std::string>& tlows,
                        const ColumnName& cn, size_t* src_out,
                        std::string* col_canon_out) -> bool {
    const std::string nl = AsciiLower(cn.name);
    if (nl.empty() || nl == "*") {
      return false;
    }
    const std::string sl = AsciiLower(cn.schema);
    size_t hits = 0;
    size_t hit_i = 0;
    std::string hit_c;
    for (size_t i = 0; i < tbls.size(); ++i) {
      if (!sl.empty() && sl != alows[i] && sl != tlows[i]) {
        continue;
      }
      const Schema& sc = tbls[i]->GetSchema();
      for (size_t c = 0; c < sc.ColumnCount(); ++c) {
        if (AsciiLower(sc.GetColumn(c).Name().name) == nl) {
          hit_i = i;
          hit_c = sc.GetColumn(c).Name().name;
          ++hits;
          break;
        }
      }
    }
    if (hits != 1) {
      return false;
    }
    *src_out = hit_i;
    *col_canon_out = std::move(hit_c);
    return true;
  };

  std::vector<Expression> outer_conjs = SplitAndConjuncts(root->WhereClause());
  // Find exactly one candidate subquery comparison conjunct.
  for (size_t cj_idx = 0; cj_idx < outer_conjs.size(); ++cj_idx) {
    const Expression& cj = outer_conjs[cj_idx];
    if (!cj || cj->Type() != TypeTag::kBinaryExp) {
      continue;
    }
    const auto& cmp = cj->AsBinaryExpression();
    if (!IsStrictNullRejectingCompareOp(cmp.Op()) || !cmp.Left() ||
        !cmp.Right()) {
      continue;
    }
    bool sub_on_right = true;
    Expression lhs_expr;
    const QueryExpression* qexp = nullptr;
    if (cmp.Left()->Type() == TypeTag::kColumnValue &&
        cmp.Right()->Type() == TypeTag::kQueryExp) {
      lhs_expr = cmp.Left();
      qexp = &cmp.Right()->AsQueryExpression();
      sub_on_right = true;
    } else if (cmp.Right()->Type() == TypeTag::kColumnValue &&
               cmp.Left()->Type() == TypeTag::kQueryExp) {
      lhs_expr = cmp.Right();
      qexp = &cmp.Left()->AsQueryExpression();
      sub_on_right = false;
    } else {
      continue;
    }
    if (qexp->Exists() || qexp->Test() != nullptr || qexp->ArrayResult() ||
        qexp->Query() == nullptr) {
      continue;
    }
    const SelectStatement& sub = *qexp->Query();
    if (sub.Sources().empty() || sub.Sources().size() >= nsrc ||
        !sub.GroupBy().empty() || sub.IsGroupAll() || sub.Having() ||
        sub.Qualify() || sub.Distinct() || sub.HasDistinctOn() ||
        sub.HasLimit() || sub.Offset() != 0 || !sub.UnionAll().empty() ||
        sub.GetSetOperationTree() != nullptr || !sub.WithQueries().empty() ||
        sub.SelectList().size() != 1 || sub.WhereClause() == nullptr) {
      continue;
    }
    const AggregateExpression* agg = nullptr;
    Expression scale_const;
    bool const_on_left = true;
    if (!ExtractNullOnEmptyCorrelatedAggregate(sub.SelectList()[0].expression,
                                               &agg, &scale_const,
                                               &const_on_left) ||
        agg == nullptr || !agg->Child() ||
        agg->Child()->Type() != TypeTag::kColumnValue) {
      continue;
    }
    std::string win_fn_name;
    switch (agg->GetType()) {
      case AggregationType::kAvg:
        win_fn_name = "AVG";
        break;
      case AggregationType::kSum:
        win_fn_name = "SUM";
        break;
      case AggregationType::kMin:
        win_fn_name = "MIN";
        break;
      case AggregationType::kMax:
        win_fn_name = "MAX";
        break;
      default:
        continue;
    }

    // Map each subquery source 1-to-1 to an outer source with same base table.
    const size_t nsub = sub.Sources().size();
    std::vector<std::shared_ptr<Table>> sub_tables(nsub);
    std::vector<std::string> sub_alias_low(nsub);
    std::vector<std::string> sub_table_low(nsub);
    std::vector<size_t> sub_to_outer(nsub, nsrc);
    std::vector<bool> outer_in_sub(nsrc, false);
    bool map_ok = true;
    for (size_t si = 0; si < nsub; ++si) {
      const SelectSource& ss = sub.Sources()[si];
      if ((ss.join_type != JoinType::kCross &&
           ss.join_type != JoinType::kInner) ||
          ss.query != nullptr || ss.table.empty() || ss.unnest ||
          ss.is_lateral || ss.sample || ss.from_nested_join ||
          !ss.using_columns.empty() || ss.join_condition != nullptr) {
        map_ok = false;
        break;
      }
      StatusOr<std::shared_ptr<Table>> t = ctx.GetTable(ss.table);
      if (!t.HasValue() || t.Value() == nullptr) {
        map_ok = false;
        break;
      }
      sub_tables[si] = t.Value();
      sub_alias_low[si] = AsciiLower(ss.alias.empty() ? ss.table : ss.alias);
      sub_table_low[si] = AsciiLower(ss.table);
      size_t match_cnt = 0;
      size_t match_oi = nsrc;
      for (size_t oi = 0; oi < nsrc; ++oi) {
        if (!outer_in_sub[oi] && outer_table_low[oi] == sub_table_low[si]) {
          match_oi = oi;
          ++match_cnt;
        }
      }
      if (match_cnt != 1) {
        map_ok = false;
        break;
      }
      sub_to_outer[si] = match_oi;
      outer_in_sub[match_oi] = true;
    }
    if (!map_ok) {
      continue;
    }

    // Require exactly one outer-only source (e.g. `item`) for clarity & safety.
    size_t dim_oi = nsrc;
    size_t outer_only_cnt = 0;
    for (size_t oi = 0; oi < nsrc; ++oi) {
      if (!outer_in_sub[oi]) {
        dim_oi = oi;
        ++outer_only_cnt;
      }
    }
    if (outer_only_cnt != 1) {
      continue;
    }

    // Resolve agg column in sub.
    size_t agg_sub_si = 0;
    std::string agg_col_canon;
    if (!resolve_in(sub_tables, sub_alias_low, sub_table_low,
                    agg->Child()->AsColumnValue().GetColumnName(), &agg_sub_si,
                    &agg_col_canon)) {
      continue;
    }
    const size_t agg_outer_oi = sub_to_outer[agg_sub_si];

    // Canonicalize an expression over sub or outer into string form using
    // `(outer_src_idx, col_low)` so predicates can be compared identically.
    auto canon_expr = [&](const Expression& e, bool is_sub,
                          bool* ok_out) -> std::string {
      *ok_out = true;
      Expression rewritten = map_expr(e, [&](const Expression& node) {
        if (node->Type() == TypeTag::kColumnValue) {
          const ColumnName& cn = node->AsColumnValue().GetColumnName();
          size_t si = 0;
          std::string cc;
          if (is_sub) {
            if (!resolve_in(sub_tables, sub_alias_low, sub_table_low, cn, &si,
                            &cc)) {
              *ok_out = false;
              return node;
            }
            return ColumnValueExp(ColumnName(
                "__o" + std::to_string(sub_to_outer[si]), AsciiLower(cc)));
          }
          if (!resolve_in(outer_tables, outer_alias_low, outer_table_low, cn,
                          &si, &cc)) {
            *ok_out = false;
            return node;
          }
          return ColumnValueExp(
              ColumnName("__o" + std::to_string(si), AsciiLower(cc)));
        }
        return node;
      });
      if (!*ok_out) {
        return {};
      }
      // Normalize commutative equality `a = b` vs `b = a`.
      if (rewritten->Type() == TypeTag::kBinaryExp &&
          rewritten->AsBinaryExpression().Op() == BinaryOperation::kEquals) {
        const std::string ls =
            rewritten->AsBinaryExpression().Left()->ToString();
        const std::string rs =
            rewritten->AsBinaryExpression().Right()->ToString();
        std::string canonical = ls <= rs ? ls : rs;
        canonical += "=";
        canonical += ls <= rs ? rs : ls;
        return canonical;
      }
      return rewritten->ToString();
    };

    // Inspect subquery conjuncts: expect 1 correlation equality `sub.fk =
    // dim.pk` and all other conjuncts purely local to `sub`.
    std::vector<Expression> sub_conjs = SplitAndConjuncts(sub.WhereClause());
    bool corr_found = false;
    size_t corr_fk_outer_oi = nsrc;
    std::string corr_fk_col;
    std::string corr_dim_pk_col;
    std::unordered_set<std::string> sub_canon_preds;
    bool sub_ok = true;
    for (const Expression& sc : sub_conjs) {
      if (!sc || has_bad_node(sc)) {
        sub_ok = false;
        break;
      }
      bool ok_local = false;
      std::string cs = canon_expr(sc, /*is_sub=*/true, &ok_local);
      if (ok_local) {
        sub_canon_preds.insert(std::move(cs));
        continue;
      }
      // Must be the single correlation equality `sub_col = dim_pk_col`.
      if (corr_found || sc->Type() != TypeTag::kBinaryExp ||
          sc->AsBinaryExpression().Op() != BinaryOperation::kEquals) {
        sub_ok = false;
        break;
      }
      const Expression& l = sc->AsBinaryExpression().Left();
      const Expression& r = sc->AsBinaryExpression().Right();
      if (!l || !r || l->Type() != TypeTag::kColumnValue ||
          r->Type() != TypeTag::kColumnValue) {
        sub_ok = false;
        break;
      }
      size_t ls = 0;
      size_t rs = 0;
      std::string lc;
      std::string rc;
      const bool l_in_sub =
          resolve_in(sub_tables, sub_alias_low, sub_table_low,
                     l->AsColumnValue().GetColumnName(), &ls, &lc);
      const bool r_in_sub =
          resolve_in(sub_tables, sub_alias_low, sub_table_low,
                     r->AsColumnValue().GetColumnName(), &rs, &rc);
      if (l_in_sub == r_in_sub) {
        sub_ok = false;
        break;
      }
      size_t outer_s = 0;
      std::string outer_c;
      const ColumnName& o_cn = l_in_sub ? r->AsColumnValue().GetColumnName()
                                        : l->AsColumnValue().GetColumnName();
      if (!resolve_in(outer_tables, outer_alias_low, outer_table_low, o_cn,
                      &outer_s, &outer_c) ||
          outer_s != dim_oi) {
        sub_ok = false;
        break;
      }
      corr_found = true;
      corr_fk_outer_oi = sub_to_outer[l_in_sub ? ls : rs];
      corr_fk_col = l_in_sub ? lc : rc;
      corr_dim_pk_col = outer_c;
    }
    if (!sub_ok || !corr_found) {
      continue;
    }

    // Certify `dim_oi.corr_dim_pk_col` is unique non-null so joining `dim_oi`
    // never duplicates any fact row.
    const int pk_off = outer_tables[dim_oi]->GetSchema().Offset(
        ColumnName("", corr_dim_pk_col));
    if (pk_off < 0 ||
        !IsColumnCertifiedUniqueNotNull(*outer_tables[dim_oi],
                                        static_cast<slot_t>(pk_off), ctx)) {
      continue;
    }

    // Verify outer conjuncts: every `sub_canon_preds` must be present in outer,
    // the join `corr_fk_outer_oi.corr_fk_col = dim_oi.corr_dim_pk_col` must be
    // present in outer, and any remaining outer conjunct must touch ONLY
    // dim_oi.
    std::string expected_dim_join_canon;
    {
      const std::string a = "__o" + std::to_string(corr_fk_outer_oi) + "." +
                            AsciiLower(corr_fk_col);
      const std::string b =
          "__o" + std::to_string(dim_oi) + "." + AsciiLower(corr_dim_pk_col);
      expected_dim_join_canon = a <= b ? a : b;
      expected_dim_join_canon += "=";
      expected_dim_join_canon += a <= b ? b : a;
    }
    std::unordered_set<std::string> matched_sub_preds;
    bool found_dim_join = false;
    bool outer_ok = true;
    for (size_t oi_c = 0; oi_c < outer_conjs.size(); ++oi_c) {
      if (oi_c == cj_idx) {
        continue;
      }
      const Expression& oc = outer_conjs[oi_c];
      if (!oc || has_bad_node(oc)) {
        outer_ok = false;
        break;
      }
      bool ok_c = false;
      const std::string cs = canon_expr(oc, /*is_sub=*/false, &ok_c);
      if (!ok_c) {
        outer_ok = false;
        break;
      }
      if (cs == expected_dim_join_canon) {
        found_dim_join = true;
        continue;
      }
      if (sub_canon_preds.contains(cs)) {
        matched_sub_preds.insert(cs);
        continue;
      }
      // Must touch ONLY `dim_oi`.
      bool only_dim = true;
      for (const ColumnName& cn : oc->TouchedColumns()) {
        size_t s = 0;
        std::string cc;
        if (!resolve_in(outer_tables, outer_alias_low, outer_table_low, cn, &s,
                        &cc) ||
            s != dim_oi) {
          only_dim = false;
          break;
        }
      }
      if (!only_dim) {
        outer_ok = false;
        break;
      }
    }
    if (!outer_ok || !found_dim_join ||
        matched_sub_preds.size() != sub_canon_preds.size()) {
      continue;
    }

    // Generated internal names must not collide with user aliases, CTE
    // names or output columns in this scope, otherwise the rewritten
    // column references silently bind to the user's objects.
    std::unordered_set<std::string> used_names;
    for (const SelectSource& s : root->Sources()) {
      if (!s.alias.empty()) {
        used_names.insert(AsciiLower(s.alias));
      }
      if (!s.table.empty()) {
        used_names.insert(AsciiLower(s.table));
      }
    }
    for (const auto& [wn, wb] : root->WithQueries()) {
      (void)wb;
      used_names.insert(AsciiLower(wn));
    }
    for (const NamedExpression& ne : root->SelectList()) {
      if (!ne.name.empty()) {
        used_names.insert(AsciiLower(ne.name));
      }
    }
    auto fresh_name = [&](const std::string& base) {
      if (used_names.insert(AsciiLower(base)).second) {
        return base;
      }
      for (size_t seq = 1;; ++seq) {
        const std::string cand = base + "_" + std::to_string(seq);
        if (used_names.insert(AsciiLower(cand)).second) {
          return cand;
        }
      }
    };

    // Collect all columns referenced by outer SelectList, OrderBy, and
    // lhs_expr.
    std::unordered_map<std::string, std::string> col_to_alias;
    std::vector<NamedExpression> inner_select;
    bool sel_ok = true;
    auto register_col = [&](const ColumnName& cn) -> std::string {
      size_t s = 0;
      std::string cc;
      if (!resolve_in(outer_tables, outer_alias_low, outer_table_low, cn, &s,
                      &cc)) {
        sel_ok = false;
        return {};
      }
      const std::string key = std::to_string(s) + ":" + AsciiLower(cc);
      auto [it, ins] = col_to_alias.emplace(
          key, fresh_name("__csa_c" + std::to_string(inner_select.size())));
      if (ins) {
        const SelectSource& ss = root->Sources()[s];
        const std::string qual = ss.alias.empty() ? ss.table : ss.alias;
        inner_select.emplace_back(it->second,
                                  ColumnValueExp(ColumnName(qual, cc)));
      }
      return it->second;
    };
    const std::string sub_wrap_alias = fresh_name("__csa_sub");
    auto rewrite_outer_expr = [&](const Expression& e) -> Expression {
      if (!e) {
        return e;
      }
      if (has_bad_node(e)) {
        sel_ok = false;
        return e;
      }
      return map_expr(e, [&](const Expression& node) -> Expression {
        if (node->Type() == TypeTag::kColumnValue) {
          const std::string al =
              register_col(node->AsColumnValue().GetColumnName());
          if (!sel_ok) {
            return node;
          }
          return ColumnValueExp(ColumnName(sub_wrap_alias, al));
        }
        return node;
      });
    };
    const std::string lhs_alias =
        register_col(lhs_expr->AsColumnValue().GetColumnName());
    std::vector<NamedExpression> new_select;
    new_select.reserve(root->SelectList().size());
    for (const NamedExpression& ne : root->SelectList()) {
      new_select.emplace_back(ne.name, rewrite_outer_expr(ne.expression));
    }
    auto new_order = root->OrderBy();
    for (auto& ot : new_order) {
      ot.expression = rewrite_outer_expr(ot.expression);
    }
    if (!sel_ok) {
      continue;
    }

    const SelectSource& agg_src = root->Sources()[agg_outer_oi];
    const std::string agg_qual =
        agg_src.alias.empty() ? agg_src.table : agg_src.alias;
    const SelectSource& fk_src = root->Sources()[corr_fk_outer_oi];
    const std::string fk_qual =
        fk_src.alias.empty() ? fk_src.table : fk_src.alias;
    const std::string win_alias = fresh_name("__csa_w");
    inner_select.emplace_back(
        win_alias, WindowFunctionCallExp(
                       std::move(win_fn_name),
                       {ColumnValueExp(ColumnName(agg_qual, agg_col_canon))},
                       {ColumnValueExp(ColumnName(fk_qual, corr_fk_col))}, {}));

    std::vector<Expression> inner_where_conjs;
    inner_where_conjs.reserve(outer_conjs.size() - 1);
    for (size_t i = 0; i < outer_conjs.size(); ++i) {
      if (i != cj_idx) {
        inner_where_conjs.push_back(outer_conjs[i]);
      }
    }
    auto inner_stmt = std::make_shared<SelectStatement>();
    inner_stmt->SetSources(root->Sources());
    inner_stmt->SetWhereClause(CombineAndConjuncts(inner_where_conjs));
    inner_stmt->SetSelectList(std::move(inner_select));

    Expression win_col = ColumnValueExp(ColumnName(sub_wrap_alias, win_alias));
    Expression rhs_val =
        scale_const
            ? (const_on_left
                   ? BinaryExpressionExp(scale_const,
                                         BinaryOperation::kMultiply, win_col)
                   : BinaryExpressionExp(win_col, BinaryOperation::kMultiply,
                                         scale_const))
            : win_col;
    Expression lhs_col = ColumnValueExp(ColumnName(sub_wrap_alias, lhs_alias));
    Expression new_where =
        sub_on_right ? BinaryExpressionExp(lhs_col, cmp.Op(), rhs_val)
                     : BinaryExpressionExp(rhs_val, cmp.Op(), lhs_col);

    root->SetSources({SelectSource("", sub_wrap_alias, std::move(inner_stmt),
                                   JoinType::kCross, nullptr)});
    root->SetWhereClause(std::move(new_where));
    root->SetSelectList(std::move(new_select));
    if (!new_order.empty()) {
      root->SetOrderBy(std::move(new_order));
    }
    return true;
  }
  return false;
}

// Merges a cross-join of two ungrouped scalar-aggregate derived tables over
// the same base join into a single-pass conditional-aggregate derived table.
//   Case 1 (identical base sources, differing local predicates, e.g. TPC-DS
//   Q90):
//     FROM (SELECT AGG FROM B WHERE C AND P1) A, (SELECT AGG FROM B WHERE C AND
//     P2) B
//     -> FROM (SELECT COND_AGG(P1), COND_AGG(P2) FROM B WHERE C AND (P1 OR P2))
//     M
//   Case 2 (one side adds a single certified-unique-not-null PK table P, e.g.
//   Q61):
//     FROM (SELECT AGG FROM B, P WHERE C AND fk = P.pk AND Cp) A,
//          (SELECT AGG FROM B WHERE C) B
//     -> FROM (SELECT COND_AGG(__m_dim.__m_pk IS NOT NULL), AGG
//              FROM B LEFT OUTER JOIN (SELECT P.pk AS __m_pk FROM P WHERE Cp)
//              __m_dim ON fk = __m_dim.__m_pk WHERE C) M
// Lean proof: lean/TinylambOpt/VectorizedJoinAndPruning.lean
//   (scalar_agg_cross_join_merge_case1_sound,
//    scalar_agg_cross_join_merge_case2_sound)
bool RewriteScalarAggCrossJoinMerge(SelectStatement* root,
                                    TransactionContext& ctx) {
  if (root == nullptr || root->Sources().size() < 2 ||
      root->Sources().size() > 16 || root->WhereClause() != nullptr ||
      !root->GroupBy().empty() || root->Having() != nullptr ||
      root->Distinct() || root->HasDistinctOn() ||
      !root->WithQueries().empty() || !root->UnionAll().empty() ||
      root->GetSetOperationTree() != nullptr || root->Qualify() != nullptr ||
      relational_detail::HasWindowFunctions(*root)) {
    return false;
  }
  const size_t n_subs = root->Sources().size();
  for (const SelectSource& s : root->Sources()) {
    if (!s.query || s.unnest || s.sample || s.join_condition ||
        (s.join_type != JoinType::kCross && s.join_type != JoinType::kInner)) {
      return false;
    }
  }
  auto is_plain_scalar_agg = [](const SelectStatement& st) -> bool {
    if (st.Sources().empty() || !st.GroupBy().empty() ||
        st.Having() != nullptr || st.Distinct() || st.HasDistinctOn() ||
        st.HasLimit() || st.Offset() != 0 || !st.OrderBy().empty() ||
        !st.WithQueries().empty() || !st.UnionAll().empty() ||
        st.GetSetOperationTree() != nullptr || st.Qualify() != nullptr ||
        relational_detail::HasWindowFunctions(st) || st.SelectList().empty()) {
      return false;
    }
    for (const auto& src : st.Sources()) {
      if (src.query || src.unnest || src.sample || src.join_condition ||
          (src.join_type != JoinType::kCross &&
           src.join_type != JoinType::kInner)) {
        return false;
      }
    }
    return std::ranges::all_of(st.SelectList(), [&](const auto& item) {
      if (item.name.empty() || !item.expression ||
          item.expression->Type() != TypeTag::kAggregateExp) {
        return false;
      }
      const auto& agg = item.expression->AsAggregateExpression();
      if (agg.HasWhereFilter()) {
        return false;
      }
      const AggregationType at = agg.GetType();
      return !agg.Distinct() &&
             (at == AggregationType::kCount || at == AggregationType::kSum ||
              at == AggregationType::kAvg);
    });
  };
  std::unordered_set<std::string> out_names;
  std::unordered_set<std::string> outer_aliases;
  for (const SelectSource& s : root->Sources()) {
    if (!is_plain_scalar_agg(*s.query)) {
      return false;
    }
    outer_aliases.insert(AsciiLower(s.alias.empty() ? s.table : s.alias));
    for (const auto& it : s.query->SelectList()) {
      if (!out_names.insert(AsciiLower(it.name)).second) {
        return false;
      }
    }
  }
  // Two derived sources can share an alias while wrapping different tables;
  // comparing only table/alias would call them identical and let a textually
  // equal predicate (e.g. `a.k = 1`) be treated as a common conjunct even
  // though `a` resolves to different relations in the two subqueries.
  auto derived_key = [](const SelectStatement& q) -> std::string {
    std::string k;
    for (const auto& s : q.Sources()) {
      k += s.table + "|" + s.alias + ";";
    }
    for (const auto& ne : q.SelectList()) {
      k += ne.name + "=" + (ne.expression ? ne.expression->ToString() : "") +
           ";";
    }
    k += q.WhereClause() ? q.WhereClause()->ToString() : "";
    return k;
  };
  auto src_key = [&](const SelectSource& src) -> std::string {
    std::string k =
        src.table + "#" + (src.alias.empty() ? src.table : src.alias);
    if (src.query != nullptr) {
      k += "#Q" + derived_key(*src.query);
    }
    return k;
  };
  auto expr_safe_pred = [](const Expression& root_e) -> bool {
    if (!root_e) {
      return false;
    }
    bool ok = true;
    std::function<void(const Expression&)> walk = [&](const Expression& node) {
      if (!ok || !node) {
        return;
      }
      if (node->Type() == TypeTag::kQueryExp ||
          node->Type() == TypeTag::kAggregateExp ||
          node->Type() == TypeTag::kWindowFunctionExp) {
        ok = false;
        return;
      }
      if (node->Type() == TypeTag::kConstantValue &&
          dynamic_cast<const ParameterSlot*>(node.get()) != nullptr) {
        ok = false;
        return;
      }
      if (node->Type() == TypeTag::kFunctionCallExp &&
          GetFunctionVolatility(node->AsFunctionCallExpression().FuncName()) !=
              Volatility::kImmutable) {
        ok = false;
        return;
      }
      for (const Expression& ch : ExpressionChildren(node)) {
        walk(ch);
      }
    };
    walk(root_e);
    return ok;
  };
  auto map_expr = [](const Expression& root_e,
                     const std::function<Expression(const Expression&)>& fn) {
    std::function<Expression(const Expression&)> rec =
        [&](const Expression& e) -> Expression {
      if (!e) {
        return e;
      }
      Expression mapped = fn(e);
      if (mapped.get() != e.get()) {
        return mapped;
      }
      const std::vector<Expression> ch = ExpressionChildren(e);
      if (ch.empty()) {
        return e;
      }
      std::vector<Expression> next;
      next.reserve(ch.size());
      bool diff = false;
      for (const Expression& c : ch) {
        Expression nc = rec(c);
        if (nc.get() != c.get()) {
          diff = true;
        }
        next.push_back(std::move(nc));
      }
      return diff ? WithExpressionChildren(e, std::move(next)) : e;
    };
    return rec(root_e);
  };
  auto make_cond_agg = [](const AggregateExpression& agg,
                          const Expression& cond) -> Expression {
    if (agg.GetType() == AggregationType::kCount) {
      const bool is_star =
          !agg.Child() ||
          (agg.Child()->Type() == TypeTag::kColumnValue &&
           agg.Child()->AsColumnValue().GetColumnName().name == "*");
      Expression then_val =
          is_star ? ConstantValueExp(Value(int64_t{1})) : agg.Child();
      Expression c =
          CaseExpressionExp({{cond, then_val}}, ConstantValueExp(Value()));
      return AggregateExpressionExp(AggregationType::kCount, std::move(c),
                                    agg.Distinct());
    }
    Expression c =
        CaseExpressionExp({{cond, agg.Child()}}, ConstantValueExp(Value()));
    return AggregateExpressionExp(agg.GetType(), std::move(c), false);
  };
  // A fixed internal alias can shadow a user alias or output column, which
  // would make rebind_outer capture the wrong references.
  std::unordered_set<std::string> used_names;
  used_names.insert(outer_aliases.begin(), outer_aliases.end());
  used_names.insert(out_names.begin(), out_names.end());
  // CTE names share the identifier namespace: a user CTE named
  // __merged_scalar_agg would be shadowed by the generated alias and
  // rebind_outer() would capture the wrong references.
  for (const auto& [cte_name, cte_body] : root->WithQueries()) {
    (void)cte_body;
    used_names.insert(AsciiLower(cte_name));
  }
  std::string merged_alias = "__merged_scalar_agg";
  for (size_t seq = 1; used_names.contains(AsciiLower(merged_alias)); ++seq) {
    merged_alias = "__merged_scalar_agg_" + std::to_string(seq);
  }
  auto finish_rewrite = [&](std::shared_ptr<SelectStatement> merged_stmt) {
    auto rebind_outer = [&](const Expression& e) -> Expression {
      return map_expr(e, [&](const Expression& node) -> Expression {
        if (node && node->Type() == TypeTag::kColumnValue) {
          const ColumnName& cn = node->AsColumnValue().GetColumnName();
          if (cn.name == "*") {
            return node;
          }
          if (outer_aliases.contains(AsciiLower(cn.schema)) ||
              (cn.schema.empty() && out_names.contains(AsciiLower(cn.name)))) {
            return ColumnValueExp(ColumnName(merged_alias, cn.name));
          }
        }
        return node;
      });
    };
    std::vector<NamedExpression> new_sel;
    new_sel.reserve(root->SelectList().size());
    for (const auto& item : root->SelectList()) {
      new_sel.emplace_back(item.name, rebind_outer(item.expression));
    }
    auto new_ord = root->OrderBy();
    for (auto& oi : new_ord) {
      oi.expression = rebind_outer(oi.expression);
    }
    root->SetSources({SelectSource("", merged_alias, std::move(merged_stmt),
                                   JoinType::kCross, nullptr)});
    root->SetSelectList(std::move(new_sel));
    if (!new_ord.empty()) {
      root->SetOrderBy(std::move(new_ord));
    }
    return true;
  };

  // Build source key sets for all N subqueries.
  std::vector<std::unordered_set<std::string>> sks(n_subs);
  for (size_t i = 0; i < n_subs; ++i) {
    for (const auto& src : root->Sources()[i].query->Sources()) {
      if (!sks[i].insert(src_key(src)).second) {
        return false;
      }
    }
  }

  // Case 1: all N subqueries share identical base sources with differing local
  // WHERE conjuncts (e.g. TPC-DS Q90 [N=2], Q28 [N=6], Q88 [N=8]).
  bool all_same_sources = true;
  for (size_t i = 1; i < n_subs; ++i) {
    if (sks[i] != sks[0]) {
      all_same_sources = false;
      break;
    }
  }
  if (all_same_sources) {
    std::vector<std::vector<Expression>> conjs(n_subs);
    std::vector<std::unordered_set<std::string>> c_strs(n_subs);
    for (size_t i = 0; i < n_subs; ++i) {
      conjs[i] = SplitConjuncts(root->Sources()[i].query->WhereClause());
      for (const auto& e : conjs[i]) {
        c_strs[i].insert(e->ToString());
      }
    }
    std::vector<Expression> common;
    std::unordered_set<std::string> common_strs;
    for (const auto& e : conjs[0]) {
      const std::string s = e->ToString();
      bool in_all = true;
      for (size_t i = 1; i < n_subs; ++i) {
        if (!c_strs[i].contains(s)) {
          in_all = false;
          break;
        }
      }
      if (in_all && common_strs.insert(s).second) {
        common.push_back(e);
      }
    }
    auto has_or_node = [](const Expression& root_e) -> bool {
      bool found = false;
      std::function<void(const Expression&)> walk = [&](const Expression& n) {
        if (found || !n) {
          return;
        }
        if (n->Type() == TypeTag::kBinaryExp &&
            n->AsBinaryExpression().Op() == BinaryOperation::kOr) {
          found = true;
          return;
        }
        for (const Expression& ch : ExpressionChildren(n)) {
          walk(ch);
        }
      };
      walk(root_e);
      return found;
    };
    std::vector<Expression> branch_preds;
    branch_preds.reserve(n_subs);
    for (size_t i = 0; i < n_subs; ++i) {
      std::vector<Expression> diff;
      for (const auto& e : conjs[i]) {
        if (!common_strs.contains(e->ToString())) {
          if (!expr_safe_pred(e) || has_or_node(e)) {
            return false;
          }
          diff.push_back(e);
        }
      }
      if (diff.empty()) {
        return false;
      }
      branch_preds.push_back(CombineAndConjuncts(diff));
    }
    Expression disj = branch_preds[0];
    for (size_t i = 1; i < n_subs; ++i) {
      disj = BinaryExpressionExp(std::move(disj), BinaryOperation::kOr,
                                 branch_preds[i]);
    }
    common.push_back(std::move(disj));

    std::vector<NamedExpression> m_sel;
    size_t m_sel_total = 0;
    for (size_t i = 0; i < n_subs; ++i) {
      m_sel_total += root->Sources()[i].query->SelectList().size();
    }
    m_sel.reserve(m_sel_total);
    for (size_t i = 0; i < n_subs; ++i) {
      for (const auto& item : root->Sources()[i].query->SelectList()) {
        m_sel.emplace_back(
            item.name, make_cond_agg(item.expression->AsAggregateExpression(),
                                     branch_preds[i]));
      }
    }
    auto merged = std::make_shared<SelectStatement>();
    merged->SetSources(root->Sources()[0].query->Sources());
    merged->SetWhereClause(CombineAndConjuncts(common));
    merged->SetSelectList(std::move(m_sel));
    return finish_rewrite(std::move(merged));
  }

  // Case 2 (N == 2): one side has exactly one extra certified-unique-not-null
  // PK base table P (e.g. TPC-DS Q61: promotional_sales adds `promotion`).
  if (n_subs != 2) {
    return false;
  }
  const SelectStatement& sub0 = *root->Sources()[0].query;
  const SelectStatement& sub1 = *root->Sources()[1].query;
  const SelectStatement* big_sub = nullptr;
  const SelectStatement* small_sub = nullptr;
  const std::unordered_set<std::string>* small_sk = nullptr;
  if (sub0.Sources().size() == sub1.Sources().size() + 1) {
    big_sub = &sub0;
    small_sub = &sub1;
    small_sk = &sks[1];
  } else if (sub1.Sources().size() == sub0.Sources().size() + 1) {
    big_sub = &sub1;
    small_sub = &sub0;
    small_sk = sks.data();
  } else {
    return false;
  }
  const SelectSource* extra_src = nullptr;
  for (const auto& src : big_sub->Sources()) {
    if (!small_sk->contains(src_key(src))) {
      if (extra_src != nullptr) {
        return false;
      }
      extra_src = &src;
    }
  }
  if (extra_src == nullptr) {
    return false;
  }
  auto extra_tbl_or = ctx.GetTable(extra_src->table);
  if (!extra_tbl_or.HasValue()) {
    return false;
  }
  const Schema& extra_schema = extra_tbl_or.Value()->GetSchema();
  const std::string extra_qual =
      extra_src->alias.empty() ? extra_src->table : extra_src->alias;
  auto col_belongs_to_extra = [&](const ColumnName& cn) -> bool {
    if (!cn.schema.empty() && cn.schema != extra_qual &&
        cn.schema != extra_src->table) {
      return false;
    }
    return extra_schema.Offset(ColumnName("", cn.name)) >= 0;
  };
  // Verify big_sub's aggregate expressions do not reference extra_src.
  for (const auto& item : big_sub->SelectList()) {
    for (const ColumnName& cn : item.expression->TouchedColumns()) {
      if (col_belongs_to_extra(cn)) {
        return false;
      }
    }
  }
  std::vector<Expression> c_big = SplitConjuncts(big_sub->WhereClause());
  std::vector<Expression> c_small = SplitConjuncts(small_sub->WhereClause());
  std::unordered_set<std::string> big_strs;
  std::unordered_set<std::string> small_strs;
  for (const auto& e : c_big) {
    big_strs.insert(e->ToString());
  }
  for (const auto& e : c_small) {
    small_strs.insert(e->ToString());
  }
  // Every conjunct of small_sub must appear in big_sub.
  for (const auto& e : c_small) {
    if (!big_strs.contains(e->ToString())) {
      return false;
    }
  }
  Expression fk_expr;
  std::string pk_col_name;
  std::vector<Expression> extra_local_preds;
  for (const auto& e : c_big) {
    if (small_strs.contains(e->ToString())) {
      continue;
    }
    if (!expr_safe_pred(e)) {
      return false;
    }
    // Check if this is `fk = P.pk`.
    if (e->Type() == TypeTag::kBinaryExp &&
        e->AsBinaryExpression().Op() == BinaryOperation::kEquals &&
        e->AsBinaryExpression().Left() && e->AsBinaryExpression().Right() &&
        e->AsBinaryExpression().Left()->Type() == TypeTag::kColumnValue &&
        e->AsBinaryExpression().Right()->Type() == TypeTag::kColumnValue) {
      const ColumnName& ln =
          e->AsBinaryExpression().Left()->AsColumnValue().GetColumnName();
      const ColumnName& rn =
          e->AsBinaryExpression().Right()->AsColumnValue().GetColumnName();
      const bool l_ext = col_belongs_to_extra(ln);
      const bool r_ext = col_belongs_to_extra(rn);
      if (l_ext != r_ext) {
        if (fk_expr != nullptr) {
          return false;
        }
        fk_expr = l_ext ? e->AsBinaryExpression().Right()
                        : e->AsBinaryExpression().Left();
        pk_col_name = l_ext ? ln.name : rn.name;
        continue;
      }
    }
    // Otherwise every touched column must belong strictly to extra_src.
    const auto touched = e->TouchedColumns();
    if (touched.empty()) {
      return false;
    }
    for (const ColumnName& cn : touched) {
      if (!col_belongs_to_extra(cn)) {
        return false;
      }
    }
    extra_local_preds.push_back(e);
  }
  const int pk_off = pk_col_name.empty()
                         ? -1
                         : extra_schema.Offset(ColumnName("", pk_col_name));
  if (!fk_expr || pk_off < 0 ||
      !IsColumnCertifiedUniqueNotNull(*extra_tbl_or.Value(),
                                      static_cast<slot_t>(pk_off), ctx)) {
    return false;
  }
  // The injected LEFT JOIN source and its output column must not shadow any
  // alias or output name already present in the merged statement.
  std::unordered_set<std::string> case2_used_names;
  for (const SelectSource& s : small_sub->Sources()) {
    if (!s.alias.empty()) {
      case2_used_names.insert(AsciiLower(s.alias));
    }
    if (!s.table.empty()) {
      case2_used_names.insert(AsciiLower(s.table));
    }
  }
  for (const auto& item : small_sub->SelectList()) {
    if (!item.name.empty()) {
      case2_used_names.insert(AsciiLower(item.name));
    }
  }
  for (const auto& item : big_sub->SelectList()) {
    if (!item.name.empty()) {
      case2_used_names.insert(AsciiLower(item.name));
    }
  }
  auto fresh_m_name = [&](const std::string& base) {
    if (case2_used_names.insert(AsciiLower(base)).second) {
      return base;
    }
    for (size_t seq = 1;; ++seq) {
      const std::string cand = base + "_" + std::to_string(seq);
      if (case2_used_names.insert(AsciiLower(cand)).second) {
        return cand;
      }
    }
  };
  const std::string dim_sub_alias = fresh_m_name("__m_dim");
  const std::string dim_pk_alias = fresh_m_name("__m_pk");
  auto dim_sub = std::make_shared<SelectStatement>();
  dim_sub->SetSources({SelectSource(extra_src->table, extra_src->alias, nullptr,
                                    JoinType::kCross, nullptr)});
  if (!extra_local_preds.empty()) {
    dim_sub->SetWhereClause(CombineAndConjuncts(extra_local_preds));
  }
  dim_sub->SetSelectList({NamedExpression(
      dim_pk_alias, ColumnValueExp(ColumnName(extra_qual, pk_col_name)))});

  Expression pk_ref = ColumnValueExp(ColumnName(dim_sub_alias, dim_pk_alias));
  Expression join_on =
      BinaryExpressionExp(fk_expr, BinaryOperation::kEquals, pk_ref);
  Expression matched_cond =
      UnaryExpressionExp(pk_ref, UnaryOperation::kIsNotNull);

  std::vector<SelectSource> m_sources = small_sub->Sources();
  m_sources.emplace_back("", dim_sub_alias, std::move(dim_sub), JoinType::kLeft,
                         std::move(join_on));

  std::vector<NamedExpression> m_sel;
  m_sel.reserve(small_sub->SelectList().size() + big_sub->SelectList().size());
  for (const auto& item : small_sub->SelectList()) {
    m_sel.push_back(item);
  }
  for (const auto& item : big_sub->SelectList()) {
    const auto& agg = item.expression->AsAggregateExpression();
    if (agg.GetType() == AggregationType::kCount && !agg.Child()) {
      m_sel.emplace_back(
          item.name,
          AggregateExpressionExp(AggregationType::kCount, pk_ref, false));
    } else {
      m_sel.emplace_back(item.name, make_cond_agg(agg, matched_cond));
    }
  }
  auto merged = std::make_shared<SelectStatement>();
  merged->SetSources(std::move(m_sources));
  if (small_sub->WhereClause()) {
    merged->SetWhereClause(small_sub->WhereClause());
  }
  merged->SetSelectList(std::move(m_sel));
  return finish_rewrite(std::move(merged));
}

}  // namespace tinylamb
