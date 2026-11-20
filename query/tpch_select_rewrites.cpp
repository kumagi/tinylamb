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
    const std::string fn =
        AsciiLower(expr->AsFunctionCallExpression().FuncName());
    if (fn == "rand" || fn == "random" || fn == "uuid" ||
        fn == "generate_uuid" || fn == "now" || fn == "current_timestamp" ||
        fn == "current_date" || fn == "current_time" ||
        fn == "clock_timestamp") {
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
  // Avoid lazily scanning huge paged fact tables (> 4M rows) solely to probe
  // uniqueness; dimensions (`customer` 1.5M, `part` 2M, `supplier` 100K,
  // `nation` 25, `region` 5) are resident or small.
  const uint64_t backing_rows = table.BackingColumnarRowCount();
  if (backing_rows > 4'000'000ULL && table.GetLSTableStore() == nullptr) {
    const auto tlcs = table.GetBackingTlcsStore();
    if (tlcs != nullptr && tlcs->IsPaged()) {
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
// Lean proof: lean/TinylambOpt/TpchSf10Discovery.lean
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

// Lean proof: lean/TinylambOpt/TpchSf10Discovery.lean
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
        } else if (cmp == 0 && bi.lo.inclusive) {
          global_lo.inclusive = true;
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
        } else if (cmp == 0 && bi.hi.inclusive) {
          global_hi.inclusive = true;
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

  for (const Expression& conj : orig_conjuncts) {
    const std::vector<Expression> branches = SplitOrDisjuncts(conj);
    if (branches.size() < 2 || ExprHasParamOrSubqueryOrVolatile(conj) ||
        ExprContainsAggregateNode(conj)) {
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
// Lean proof: lean/TinylambOpt/TpchSf10Discovery.lean
//   - left_join_right_preagg_and_unique_elide_groupby_sound
//   - nullable_unique_left_elides_groupby_bug (Trap 2a guard)
//   - left_join_count_nonstrict_expr_bug (Trap 2b guard)
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
    return WithExpressionChildren(e, children);
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
    return WithExpressionChildren(e, children);
  };

  std::vector<NamedExpression> new_select;
  new_select.reserve(stmt->SelectList().size());
  for (const auto & old_item : stmt->SelectList()) {
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
// Lean proof: lean/TinylambOpt/TpchSf10Discovery.lean
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
  return WithExpressionChildren(expr, children);
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
        return WithExpressionChildren(e, children);
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
// Lean proof: lean/TinylambOpt/TpchSf10Discovery.lean
//   - cte_argmax_subquery_to_nonnull_max_ties_sound
//   - argmax_with_ties_without_is_not_null_bug (Trap 4a guard)
// ============================================================================

size_t CountTableReferencesInExpr(  // NOLINT(misc-no-recursion)
    const Expression& expr, std::string_view target_name);

size_t CountTableReferencesInSelect(  // NOLINT(misc-no-recursion)
    const SelectStatement& stmt, std::string_view target_name,
    bool include_with_defs) {
  size_t count = 0;
  if (include_with_defs) {
    for (const auto& [wname, wbody] : stmt.WithQueries()) {
      (void)wname;
      if (wbody != nullptr) {
        count += CountTableReferencesInSelect(*wbody, target_name, true);
      }
    }
  }
  for (const SelectSource& src : stmt.Sources()) {
    if (!src.table.empty() && IdentifierEqualsCI(src.table, target_name)) {
      ++count;
    }
    if (src.query != nullptr) {
      count += CountTableReferencesInSelect(*src.query, target_name, true);
    }
    count += CountTableReferencesInExpr(src.join_condition, target_name);
    count += CountTableReferencesInExpr(src.unnest, target_name);
  }
  count += CountTableReferencesInExpr(stmt.WhereClause(), target_name);
  count += CountTableReferencesInExpr(stmt.Having(), target_name);
  count += CountTableReferencesInExpr(stmt.Qualify(), target_name);
  for (const NamedExpression& item : stmt.SelectList()) {
    count += CountTableReferencesInExpr(item.expression, target_name);
  }
  for (const Expression& g : stmt.GroupBy()) {
    count += CountTableReferencesInExpr(g, target_name);
  }
  for (const auto& ob : stmt.OrderBy()) {
    count += CountTableReferencesInExpr(ob.expression, target_name);
  }
  for (const auto& br : stmt.UnionAll()) {
    if (br != nullptr) {
      count += CountTableReferencesInSelect(*br, target_name, true);
    }
  }
  return count;
}

size_t CountTableReferencesInExpr(  // NOLINT(misc-no-recursion)
    const Expression& expr, std::string_view target_name) {
  if (!expr) {
    return 0;
  }
  size_t count = 0;
  if (expr->Type() == TypeTag::kQueryExp) {
    const auto& q = expr->AsQueryExpression();
    if (q.Query() != nullptr) {
      count += CountTableReferencesInSelect(*q.Query(), target_name, true);
    }
    count += CountTableReferencesInExpr(q.Test(), target_name);
    return count;
  }
  for (const Expression& ch : ExpressionChildren(expr)) {
    count += CountTableReferencesInExpr(ch, target_name);
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
// Lean proof: lean/TinylambOpt/TpchSf10Discovery.lean
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
      return (p.first << 16) ^ p.second;
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
// Lean proof: lean/TinylambOpt/TpchSf10Discovery.lean
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
  std::unordered_map<size_t, slot_t> unique_rel_pk;
  for (const ResolvedColRef& gr : gb_cols) {
    if (unique_rel_pk.contains(gr.rel_idx)) {
      continue;
    }
    const Column& c = rels[gr.rel_idx].schema.GetColumn(gr.slot);
    if ((c.Type() == ValueType::kInt64 || c.Type() == ValueType::kDate) &&
        !c.IsInterval() &&
        IsColumnCertifiedUniqueNotNull(*rels[gr.rel_idx].table, gr.slot, ctx)) {
      unique_rel_pk.emplace(gr.rel_idx, gr.slot);
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
        unique_rel_pk.emplace(to.rel_idx, to.slot);
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
    if (pk_it != unique_rel_pk.end() && gr.slot != pk_it->second &&
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

  // Ensure every `deferred_rels` member's `pk_slot` is present in
  // `retained_gb_cols` so the outer query can join the K winners directly to
  // each deferred dimension by its certified PK!
  for (const size_t d_rel : deferred_rels) {
    const ResolvedColRef pk_ref{.rel_idx = d_rel, .slot = unique_rel_pk[d_rel]};
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
    const slot_t pk_slot = unique_rel_pk[d_rel];
    const ResolvedColRef pk_ref{.rel_idx = d_rel, .slot = pk_slot};
    const auto kit = std::ranges::find(retained_gb_cols, pk_ref);
    const auto k_idx =
        static_cast<size_t>(std::distance(retained_gb_cols.begin(), kit));
    outer_where.push_back(BinaryExpressionExp(
        ColumnValueExp(ColumnName(core_alias, key_out_name(k_idx))),
        BinaryOperation::kEquals,
        ColumnValueExp(ColumnName(
            dalias, rels[d_rel].schema.GetColumn(pk_slot).Name().name))));
  }

  std::vector<NamedExpression> outer_select;
  outer_select.reserve(stmt->SelectList().size());
  for (size_t i = 0; i < stmt->SelectList().size(); ++i) {
    const NamedExpression& old_item = stmt->SelectList()[i];
    const std::string out_name =
        old_item.name.empty() ? old_item.expression->ToString() : old_item.name;
    if (!select_core_col[i].empty()) {
      outer_select.emplace_back(
          out_name, ColumnValueExp(ColumnName(core_alias, select_core_col[i])));
    } else {
      const auto r = ResolveColRef(
          old_item.expression->AsColumnValue().GetColumnName(), rels);
      if (!r.has_value()) {
        return false;
      }
      outer_select.emplace_back(
          out_name,
          ColumnValueExp(ColumnName(
              late_dim_alias.at(r->rel_idx),
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
// Lean proof: lean/TinylambOpt/TpchSf10Discovery.lean
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

bool DecorrelateCorrelatedSumOneSelect(
    SelectStatement* stmt, const std::unordered_set<std::string>& ctes,
    TransactionContext& ctx, size_t* alias_seq) {
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
    // Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
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
      return WithExpressionChildren(e, chs);
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

    // Build deduplicated inner GROUP BY keys and derived source!
    const std::string cs_alias =
        "__tpch_corr_sum_" + std::to_string((*alias_seq)++);
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
    TransactionContext& ctx, size_t* alias_seq) {
  if (stmt == nullptr) {
    return false;
  }
  CollectCteNamesInScope(*stmt, &ctes);
  bool changed = false;
  for (const auto& [name, body] : stmt->WithQueries()) {
    if (!stmt->IsRecursiveWith(name) && body != nullptr) {
      changed |=
          DecorrelateCorrelatedSumRecursive(body.get(), ctes, ctx, alias_seq);
    }
  }
  for (const SelectSource& src : stmt->Sources()) {
    if (src.query != nullptr && !src.is_lateral) {
      changed |= DecorrelateCorrelatedSumRecursive(src.query.get(), ctes, ctx,
                                                   alias_seq);
    }
  }
  changed |= DecorrelateCorrelatedSumOneSelect(stmt, ctes, ctx, alias_seq);
  return changed;
}

}  // namespace

// Lean proof: lean/TinylambOpt/TpchSf10Discovery.lean
//   (kleene_common_conjunct_factor_or_sound,
//    disjunctive_join_envelope_pushdown_sound,
//    interval_hull_of_disjuncts_sound,
//    envelope_missing_branch_pred_drops_rows_bug)
bool FactorAndEnvelopeDisjunctiveWhere(SelectStatement* select,
                                       TransactionContext& ctx) {
  return FactorAndEnvelopeRecursive(select, {}, ctx);
}

// Lean proof: lean/TinylambOpt/TpchSf10Discovery.lean
//   (left_join_right_preagg_and_unique_elide_groupby_sound,
//    nullable_unique_left_elides_groupby_bug,
//    left_join_count_nonstrict_expr_bug)
bool PreaggregateLeftJoinRightCounts(SelectStatement* select,
                                     TransactionContext& ctx) {
  return PreaggregateLeftJoinRecursive(select, {}, ctx);
}

// Lean proof: lean/TinylambOpt/TpchSf10Discovery.lean
//   (self_semijoin_having_agg_absorption_sound,
//    self_semijoin_mismatched_where_bug)
bool AbsorbSelfSemijoinHavingAggregate(SelectStatement* select,
                                       TransactionContext& ctx) {
  return AbsorbSelfSemijoinRecursive(select, {}, ctx);
}

// Lean proof: lean/TinylambOpt/TpchSf10Discovery.lean
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
      // A computed scalar can reference input columns hidden by its alias.
      // WITH TIES evaluates ordering expressions against projected rows.
      const auto& value_expr = cte_body->SelectList()[col_it - cte_cols.begin()].expression;
      if (value_expr->Type() != TypeTag::kAggregateExp &&
          value_expr->Type() != TypeTag::kColumnValue) {
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

// Lean proof: lean/TinylambOpt/TpchSf10Discovery.lean
//   (equijoin_transitive_closure_sound)
bool SynthesizeTransitiveEquijoins(SelectStatement* select,
                                   TransactionContext& ctx) {
  return SynthesizeTransitiveEquijoinsRecursive(select, {}, ctx);
}

// Lean proof: lean/TinylambOpt/TpchSf10Discovery.lean
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

// Lean proof: lean/TinylambOpt/TpchSf10Discovery.lean
//   (correlated_gt_sum_decorrelate_to_inner_join_sound,
//    correlated_gt_count_demotes_to_inner_join_bug)
bool DecorrelateCorrelatedSumComparisons(SelectStatement* select,
                                         TransactionContext& ctx) {
  size_t alias_seq = 0;
  return DecorrelateCorrelatedSumRecursive(select, {}, ctx, &alias_seq);
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
        out.row_upper_bound += blk.row_count;
        if (blk.row_count == 0 || col_idx >= blk.columns.size()) {
          continue;
        }
        const auto& meta = blk.columns[col_idx];
        if (meta.encoding == TlcsColumnEncoding::kConstNull) {
          continue;
        }
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
      const __int128 worst_lo = min_neg_sum + std::min<__int128>(0, max_shift);
      const __int128 worst_hi = max_pos_sum + std::max<__int128>(0, max_shift);
      if (worst_lo < i64_min || worst_hi > i64_max) {
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

  // Lean proof: lean/TinylambOpt/ClickBenchRound2.lean
  //   (affine_sum_no_overflow_fusion_sound, affine_sum_row_overflow_unsound_bug)
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
               .emplace(sl,
                        std::pair{AggregateExpressionExp(AggregationType::kSum,
                                                         col_e),
                                  AggregateExpressionExp(
                                      AggregationType::kCount, col_e)})
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
    return ch_changed ? WithExpressionChildren(e, next) : e;
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

// Lean proof: lean/TinylambOpt/ClickBenchRound2.lean
//   (affine_sum_homomorphism_sound, affine_sum_coalesce_empty_bug,
//    affine_sum_overflow_masked_bug, affine_sum_no_overflow_fusion_sound)
bool RewriteAffineLinearSums(SelectStatement* select, TransactionContext& ctx) {
  return RewriteAffineLinearSumsRecursive(select, {}, ctx);
}

namespace {

// Bottom-up Kleene 3VL common-conjunct factoring over OR trees:
// `(A AND B) OR (A AND C) -> A AND (B OR C)` and `(A AND B) OR A -> A`.
// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
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
      const bool found =
          std::ranges::any_of(branch_conjs[bi], [&](const Expression& other) {
            return ExprAstEquals(cand, other);
          });
      if (!found) {
        in_all = false;
        break;
      }
    }
    if (in_all &&
        std::ranges::none_of(hoisted, [&](const Expression& prev) {
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

  auto match_positive_count_cmp =
      [](const Expression& conj, const QueryExpression** q_out) -> bool {
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
        return ColumnValueExp(
            with_qualifier ? ColumnName(inner_qual, cname) : ColumnName(cname));
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
      return WithExpressionChildren(e, next);
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
        if (!qualify_ok ||
            !SafeToReduceEvaluationCountWithSchema(unqual,
                                                   inner_rels[0].schema)) {
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
        if (!resolve_scoped(
                sc->AsBinaryExpression().Left()->AsColumnValue().GetColumnName(),
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
    inner_local_preds.push_back(UnaryExpressionExp(
        ColumnValueExp(inner_key_cn), UnaryOperation::kIsNotNull));

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

    out_conjs.push_back(QueryExpressionExp(
        std::move(in_sub), ColumnValueExp(outer_key_cn),
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
  CollectCteNamesInScope(*stmt, &ctes);
  bool changed = false;
  for (const auto& [name, body] : stmt->WithQueries()) {
    if (!stmt->IsRecursiveWith(name) && body != nullptr) {
      changed |=
          RewriteCorrelatedPositiveCountRecursive(body.get(), ctes, ctx);
    }
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
        changed |=
            RewriteCorrelatedPositiveCountRecursive(br.get(), ctes, ctx);
      }
    }
  }
  changed |= RewriteCorrelatedPositiveCountOneSelect(stmt, ctes, ctx);
  return changed;
}

}  // namespace

// Lean proof: lean/TinylambOpt/TpcdsExecCodegen.lean
//   (kleene_and_or_distrib_left_sound,
//    correlated_count_gt_zero_to_in_subquery_sound,
//    trap_correlated_count_eq_zero_not_in_null_outer_unsound)
bool RewriteCorrelatedPositiveCountToInSubquery(SelectStatement* select,
                                                TransactionContext& ctx) {
  return RewriteCorrelatedPositiveCountRecursive(select, {}, ctx);
}

}  // namespace tinylamb
